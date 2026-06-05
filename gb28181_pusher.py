#!/usr/bin/env python3
"""gb28181_pusher_multi — 支持多路视频的GB28181推流器

从config文件夹读取多个配置文件，每个配置文件对应一路视频推流

使用方法
────────────

python3 gb28181_pusher_multi.py

配置文件目录结构:
config/
  ├── camera1.json
  ├── camera2.json
  └── camera3.json

每个JSON配置文件格式示例:
{
    "server_ip": "192.168.1.100",
    "server_port": 5060,
    "server_id": "11009000000000000000",
    "domain": "1100900000",
    "agent_id": "300000000010000000001",
    "agent_password": "000000",
    "channel_id": "340000000000000000001",
    "source": "rtsp://admin:admin@192.168.111.222/h264/ch1/main/av_stream",
    "udp": false,
    "local_ip": null,
    "verbose": true,
    "reconnect_interval": 5,
    "max_reconnect_attempts": 0,
    "connection_timeout": 10,
    "manufacturer": "StrawberryInno",
    "devicename": "Camera1"
}
"""
from __future__ import annotations
import os
import glob

# 设置GStreamer插件路径
os.environ['GST_PLUGIN_PATH'] = './gst-gb28181sink/build'

import argparse
import hashlib
import http.server
import logging
import random
import re
import shlex
import signal
import socket
import subprocess
import threading
import time
import json
from contextlib import AbstractContextManager
from typing import Callable, List, Optional, Tuple, Dict, Any
from pathlib import Path
from urllib.parse import parse_qs


LOGGER = logging.getLogger("gb28181")

###############################################################################
# ─────────────────────────────── Helper utilities ─────────────────────────────── #
###############################################################################

def md5_hex(text: str) -> str:
    """Return ``MD5(text).hexdigest()`` using explicit *UTF‑8* encoding."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def find_local_ip(dst: str) -> str:
    """Return the source IPv4 address the kernel would use to reach *dst*."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect((dst, 80))
        return sock.getsockname()[0]
    finally:
        sock.close()


_RTSP_URL_RE = re.compile(r"^rtsp://([^/:]+)(?::(\d+))?", re.IGNORECASE)


def _parse_rtsp_url(url: str) -> Tuple[str, int]:
    """Extract ``(host, port)`` from an *rtsp://* URL. Defaults to port 554."""
    m = _RTSP_URL_RE.match(url)
    if not m:
        raise ValueError(f"Not a valid RTSP URL: {url}")
    host = m.group(1)
    port = int(m.group(2)) if m.group(2) else 554
    return host, port


def _tcp_probe(host: str, port: int, timeout: float) -> bool:
    """Return True if a TCP connection to ``host:port`` succeeds within *timeout*."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, OSError):
        return False


def check_rtsp_source(source: str, timeout: int = 5) -> bool:
    """Probe whether *source* is reachable and decodable.

    Strategy:
      1. If *source* is a non-RTSP URL (e.g. ``"test"``, ``file://``, local
         pipeline test source) the function returns True — there is nothing
         useful we can probe in advance.
      2. Try a lightweight ``gst-launch-1.0`` probe pipeline that decodes
         a single frame.  This catches authentication errors, codec
         mismatches, and stream-not-found errors in one shot.
      3. Fall back to a plain TCP connect if ``gst-launch-1.0`` is missing
         from the system — this only proves the server is reachable, not
         that the stream is valid.

    Returns True if the stream looks healthy, False otherwise.
    """
    if not source or not source.lower().startswith("rtsp://"):
        return True  # nothing to probe (test source, file://, etc.)

    host, port = _parse_rtsp_url(source)

    # Strategy 2: gst-launch probe pipeline (decodes one frame)
    gst_pipeline = (
        f"gst-launch-1.0 -e rtspsrc location={shlex.quote(source)} "
        f"num-buffers=1 timeout={int(max(1, timeout)) * 1000000} ! "
        f"fakesink"
    )
    try:
        result = subprocess.run(
            gst_pipeline,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=timeout + 2,  # a little slack over the gst-level timeout
        )
        if result.returncode == 0:
            return True
    except (subprocess.TimeoutExpired, FileNotFoundError):
        # gst-launch missing or hung — fall through to TCP probe
        pass
    except Exception:
        pass

    # Strategy 3: TCP fallback (network reachability only)
    return _tcp_probe(host, port, timeout=float(timeout))


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from JSON file"""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file {config_path} not found")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # Set default values for optional parameters
    defaults = {
        "server_port": 8116,
        "domain": "4101050000",
        "udp": False,
        "local_ip": None,
        "verbose": False,
        "reconnect_interval": 5,
        "max_reconnect_attempts": 0,
        "connection_timeout": 10,
        "source": "test",
        "manufacturer": "gzhaibaogd",
        "devicename": "Superdock",
        "rtsp_precheck": True,
        "rtsp_precheck_timeout": 5,
    }
    
    # Merge config with defaults
    for key, default_value in defaults.items():
        if key not in config:
            config[key] = default_value
    
    # Validate required parameters
    required_params = ["server_ip", "server_id", "agent_id", "agent_password", "channel_id"]
    missing_params = [param for param in required_params if not config.get(param)]
    if missing_params:
        raise ValueError(f"Missing required parameters in {config_path}: {', '.join(missing_params)}")

    return config


# ##############################################################################
# ────────────────────── Web CRUD helper utilities ─────────────────────────────
# ##############################################################################
#
# These functions are used by the embedded web admin to validate, persist and
# safely resolve channel config file paths.  Keeping them as module-level
# functions makes them easy to unit-test without spinning up the HTTP server.

# Field names that the web CRUD form accepts and what we expect from each.
_CRUD_FIELDS: Dict[str, type] = {
    "server_ip": str,
    "server_id": str,
    "agent_id": str,
    "agent_password": str,
    "channel_id": str,
    "source": str,
}

# Optional fields with type coercion callbacks.  None == str.
_CRUD_OPTIONAL_FIELDS: Dict[str, Any] = {
    "server_port": int,
    "domain": str,
    "udp": lambda v: _parse_bool(v, default=False),
    "local_ip": str,
    "verbose": lambda v: _parse_bool(v, default=False),
    "reconnect_interval": int,
    "max_reconnect_attempts": int,
    "connection_timeout": int,
    "manufacturer": str,
    "devicename": str,
    "rtsp_precheck": lambda v: _parse_bool(v, default=True),
    "rtsp_precheck_timeout": int,
}

_CONFIG_FILE_RE = re.compile(r"^[A-Za-z0-9_\-]+\.json$")


def _parse_bool(value: Any, default: bool = False) -> bool:
    """Lenient bool parser used by the web CRUD form.

    Accepts ``"true"`` / ``"false"`` (case-insensitive), ``"1"`` / ``"0"``,
    ``"yes"`` / ``"no"``.  Falls back to *default* on anything else.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    s = str(value).strip().lower()
    if s in ("true", "1", "yes", "y", "on"):
        return True
    if s in ("false", "0", "no", "n", "off", ""):
        return False
    return default


def _build_config_from_form(form: Dict[str, str]) -> Dict[str, Any]:
    """Coerce a flat ``{field: str_value}`` mapping into a typed config dict.

    The form is the raw ``urllib.parse.parse_qs`` result (or any mapping of
    string values).  Required fields missing or empty raise :class:`ValueError`;
    unknown fields are silently dropped.  The returned dict is filled in with
    the same defaults as :func:`load_config` so that downstream code
    (``_build_pusher``) can always read every key without ``KeyError``.
    """
    out: Dict[str, Any] = {}
    for key, _typ in _CRUD_FIELDS.items():
        v = (form.get(key) or "").strip()
        if not v:
            raise ValueError(f"Missing required field: {key}")
        out[key] = v

    for key, caster in _CRUD_OPTIONAL_FIELDS.items():
        raw = form.get(key)
        if raw is None or str(raw).strip() == "":
            # Apply sensible default matching load_config() defaults.
            out[key] = _CRUD_FIELD_DEFAULTS.get(key)
            continue
        try:
            out[key] = caster(raw) if callable(caster) else caster(raw)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Invalid value for {key!r}: {raw!r} ({e})") from None

    # Belt-and-braces: also apply load_config's defaults for any key neither
    # required nor optional.  Keeps the dict shape identical to what
    # load_config() would return.
    for k, v in _CRUD_FIELD_DEFAULTS.items():
        out.setdefault(k, v)
    return out


# Same defaults used by load_config() — kept in sync.
_CRUD_FIELD_DEFAULTS: Dict[str, Any] = {
    "server_port": 8116,
    "domain": "4101050000",
    "udp": False,
    "local_ip": None,
    "verbose": False,
    "reconnect_interval": 5,
    "max_reconnect_attempts": 0,
    "connection_timeout": 10,
    "source": "test",
    "manufacturer": "gzhaibaogd",
    "devicename": "Superdock",
    "rtsp_precheck": True,
    "rtsp_precheck_timeout": 5,
}


def _safe_config_path(config_dir: str, config_file: str) -> str:
    """Resolve ``config_dir/config_file`` and reject any path escaping *config_dir*.

    Raises :class:`ValueError` if *config_file* contains ``..``, an absolute
    path, or anything outside the allowed ``[A-Za-z0-9_-].json`` pattern.
    """
    if not config_file or not _CONFIG_FILE_RE.match(config_file):
        raise ValueError(
            f"Invalid config_file name: {config_file!r} "
            "(must match [A-Za-z0-9_-]+\\.json)"
        )
    base = os.path.realpath(config_dir)
    candidate = os.path.realpath(os.path.join(base, config_file))
    if not candidate.startswith(base + os.sep) and candidate != base:
        raise ValueError(f"config_file escapes config_dir: {config_file!r}")
    return candidate


def _save_config_atomic(path: str, config: Dict[str, Any]) -> None:
    """Write *config* to *path* atomically via a sibling ``.tmp`` + ``os.replace``.

    The file is created with mode 0o600 so credentials aren't world-readable.
    """
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fchmod(f.fileno(), 0o600)
    os.replace(tmp_path, path)


# ##############################################################################


def load_all_configs(config_dir: str = "config") -> Tuple[List[Dict[str, Any]], List[str]]:
    """从指定目录加载所有JSON配置文件。

    Returns ``(configs, errors)``:
      * ``configs`` — 成功加载的配置列表（已注入 ``_config_file`` 元字段）
      * ``errors``  — 每个错误一条简短描述（按发现顺序）

    Raises ``FileNotFoundError`` only when the directory itself is missing
    or contains no ``*.json`` files.
    """
    if not os.path.exists(config_dir):
        raise FileNotFoundError(f"Config directory '{config_dir}' not found")

    config_files = glob.glob(os.path.join(config_dir, "*.json"))

    if not config_files:
        raise FileNotFoundError(f"No JSON config files found in '{config_dir}'")

    configs: List[Dict[str, Any]] = []
    errors: List[str] = []
    for config_file in sorted(config_files):
        try:
            config = load_config(config_file)
            config['_config_file'] = os.path.basename(config_file)
            configs.append(config)
            LOGGER.info(f"Loaded config: {config_file}")
        except Exception as e:
            msg = f"Failed to load {os.path.basename(config_file)}: {e}"
            LOGGER.error(msg)
            errors.append(msg)

    return configs, errors


###############################################################################
# ────────────────────────────────── Core class API ────────────────────────────────── #
###############################################################################

class GB28181Pusher(AbstractContextManager):
    """A tiny GB28181 device implementation that answers *INVITE* and pushes a
    synthetic test stream (via *gst‑launch‑1.0*) to the requested RTP/PS port.
    """

    HB_GAP: int = 60
    REG_TRIES: int = 5
    RECV_TIMEOUT: int = 5
    _PT_PRIORITY: Tuple[Tuple[int, str], ...] = ((96, "PS"), (98, "H264"))

    def __init__(
        self,
        *,
        server_ip: str,
        server_port: int = 5060,
        server_id: str,
        domain: Optional[str] = None,
        agent_id: str,
        agent_password: str,
        channel_id: str,
        source: str = "test",
        use_udp_signalling: bool = False,
        local_ip: Optional[str] = None,
        verbose: bool = False,
        reconnect_interval: int = 5,
        max_reconnect_attempts: int = 0,
        connection_timeout: int = 10,
        manufacturer: str = "StrawberryInno",
        devicename: str = "Superdock",
        instance_name: str = "default",
        rtsp_precheck: bool = True,
        rtsp_precheck_timeout: int = 5,
    ) -> None:
        self.server_ip: str = server_ip
        self.server_port: int = server_port
        self.server_id: str = server_id
        self.domain: str = domain or server_id[:10]
        self.agent_id: str = agent_id
        self.source: str = source
        self.agent_password: str = agent_password
        self.channel_id: str = channel_id
        self.use_udp_signalling: bool = use_udp_signalling
        self.local_ip: str = local_ip or find_local_ip(server_ip)
        self.verbose: bool = verbose
        self.manufacturer: str = manufacturer
        self.devicename: str = devicename
        self.instance_name: str = instance_name
        self.reconnect_interval: int = reconnect_interval
        self.max_reconnect_attempts: int = max_reconnect_attempts
        self.connection_timeout: int = connection_timeout
        self.rtsp_precheck: bool = rtsp_precheck
        self.rtsp_precheck_timeout: int = rtsp_precheck_timeout

        # 为每个实例创建独立的logger
        self.logger = logging.getLogger(f"gb28181.{instance_name}")

        self._sock: socket.socket | None = None
        self._send: Callable[[bytes], None]
        self._recv: Callable[[], str]
        self._connected: bool = False
        self._shutdown_requested: bool = False
        self._push_thread: Optional[threading.Thread] = None
        self._push_stop_evt: Optional[threading.Event] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_stop_evt: Optional[threading.Event] = None

    def run_forever(self) -> None:
        """Open signalling socket, REGISTER to the platform, then handle requests forever."""
        reconnect_count = 0

        while not self._shutdown_requested:
            try:
                self._connect_and_register()
                self._connected = True
                reconnect_count = 0

                self.logger.info("Ready — waiting for INVITE/SUBSCRIBE")
                self._event_loop()

            except KeyboardInterrupt:
                self.logger.info("Interrupted by user — exiting …")
                break
            except Exception as exc:
                self._connected = False
                self._stop_heartbeat()

                if self._shutdown_requested:
                    break

                if self.max_reconnect_attempts > 0 and reconnect_count >= self.max_reconnect_attempts:
                    self.logger.error("Max reconnection attempts (%d) reached. Giving up.", self.max_reconnect_attempts)
                    break

                reconnect_count += 1
                self.logger.warning("Connection lost: %s. Attempting reconnection %d in %d seconds...",
                             exc, reconnect_count, self.reconnect_interval)

                if self._sock:
                    try:
                        self._sock.close()
                    except:
                        pass
                    self._sock = None

                time.sleep(self.reconnect_interval)

    def _connect_and_register(self) -> None:
        """Connect to server and complete registration process."""
        self._precheck_rtsp()
        self._open_signalling_socket()
        self._register()
        self._start_heartbeat()

    def _precheck_rtsp(self) -> None:
        """Verify the RTSP source is reachable before opening the SIP socket.

        Skipped when the source is not an ``rtsp://`` URL (e.g. ``"test"``) or
        when the user has disabled the precheck via ``rtsp_precheck=False``.
        Raises :class:`RuntimeError` on failure so the surrounding
        :meth:`run_forever` loop can apply its existing reconnect / backoff
        policy.
        """
        if not self.rtsp_precheck:
            self.logger.info("RTSP precheck disabled by config — skipping.")
            return
        if not self.source or not self.source.lower().startswith("rtsp://"):
            self.logger.debug("Source is not an RTSP URL — precheck not applicable.")
            return

        self.logger.info("Pre-checking RTSP source: %s (timeout=%ds) …",
                         self.source, self.rtsp_precheck_timeout)
        if check_rtsp_source(self.source, timeout=self.rtsp_precheck_timeout):
            self.logger.info("RTSP source OK.")
            return

        msg = (
            f"RTSP source unreachable or undecodable: {self.source} "
            f"(timeout={self.rtsp_precheck_timeout}s). "
            "Aborting connection attempt."
        )
        self.logger.error(msg)
        raise RuntimeError(msg)

    def __enter__(self):
        self.run_forever()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._shutdown()
        return False

    def _digest_response(
        self,
        nonce: str,
        realm: str,
        method: str,
        uri: str,
        qop: Optional[str],
    ) -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
        """Return ``(response, nc, cnonce, qop_used)`` for a *Digest* challenge."""
        a1 = md5_hex(f"{self.agent_id}:{realm}:{self.agent_password}")
        a2 = md5_hex(f"{method}:{uri}")
        if qop:
            nc = "00000001"
            cnonce = f"{random.randint(0, 0xFFFFFF):06x}"
            resp = md5_hex(f"{a1}:{nonce}:{nc}:{cnonce}:{qop}:{a2}")
            return resp, nc, cnonce, qop
        resp = md5_hex(f"{a1}:{nonce}:{a2}")
        return resp, None, None, None

    @staticmethod
    def _sip(start_line: str, headers: List[str], body: str = "") -> bytes:
        headers.append(f"Content-Length: {len(body)}")
        return (start_line + "\r\n" + "\r\n".join(headers) + "\r\n\r\n" + body).encode()

    def _build_register(self, cseq: int, auth_header: Optional[str] = None) -> bytes:
        via_branch = f"z9hG4bK{time.time_ns()}"
        hdrs = [
            f"Via: SIP/2.0/{'UDP' if self.use_udp_signalling else 'TCP'} {self.local_ip}:{self._local_port};branch={via_branch}",
            f"From: <sip:{self.agent_id}@{self.domain}>;tag=reg",
            f"To: <sip:{self.agent_id}@{self.domain}>",
            f"Call-ID: {self.agent_id}",
            f"CSeq: {cseq} REGISTER",
            f"Contact: <sip:{self.agent_id}@{self.local_ip}:{self._local_port}>;+sip.instance=\"<urn:uuid:{self.agent_id}>\"",
            "Max-Forwards: 70",
            "User-Agent: skygb28181",
            "Expires: 3600",
        ]
        if auth_header:
            hdrs.append(f"Authorization: {auth_header}")
        return self._sip(f"REGISTER sip:{self.domain} SIP/2.0", hdrs)

    def _build_message(self, xml_body: str, cseq: int, suffix: str) -> bytes:
        hdrs = [
            f"Via: SIP/2.0/{'UDP' if self.use_udp_signalling else 'TCP'} {self.local_ip}:{self._local_port};branch=z9hG4bK{time.time_ns()}",
            f"From: <sip:{self.agent_id}@{self.domain}>;tag=resp",
            f"To: <sip:{self.server_id}@{self.domain}>",
            f"Call-ID: {self.agent_id}{suffix}",
            f"CSeq: {cseq} MESSAGE",
            "Content-Type: Application/MANSCDP+xml",
            "Max-Forwards: 70",
            "User-Agent: skygb28181",
        ]
        return self._sip(f"MESSAGE sip:{self.server_id}@{self.domain} SIP/2.0", hdrs, xml_body)

    def _ok200(self, req: str) -> bytes:
        via = re.search(r"Via:(.*)", req).group(1).strip()
        fr = re.search(r"From:(.*)", req).group(1).strip()
        to = re.search(r"To:(.*)", req).group(1).strip()
        call = re.search(r"Call-ID:(.*)", req).group(1).strip()
        cseq = re.search(r"CSeq:(.*)", req).group(1).strip()
        hdrs = [
            f"Via:{via}",
            f"From:{fr}",
            f"To:{to}",
            f"Call-ID:{call}",
            f"CSeq:{cseq}",
            f"Contact: <sip:{self.agent_id}@{self.local_ip}:{self._local_port}>",
            "User-Agent: skygb28181",
        ]
        return self._sip("SIP/2.0 200 OK", hdrs)

    def _invite_ok(
        self,
        invite_msg: str,
        dst_ip: str,
        dst_port: int,
        pt: int,
        is_tcp: bool,
        codec: str,
        ssrc_dec: Optional[int],
    ) -> bytes:
        """Craft 200 OK with SDP that mirrors platform's *c= / m=* lines."""
        via = re.search(r"Via:(.*)", invite_msg).group(1).strip()
        fr = re.search(r"From:(.*)", invite_msg).group(1).strip()
        to = re.search(r"To:(.*)", invite_msg).group(1).strip()
        if "tag=" not in to:
            to += ";tag=ok"
        call = re.search(r"Call-ID:(.*)", invite_msg).group(1).strip()
        cseq = re.search(r"CSeq:(.*)", invite_msg).group(1).strip()

        sdp_lines = [
            "v=0",
            f"o={self.agent_id} 0 0 IN IP4 {self.local_ip}",
            "s=Play",
            f"c=IN IP4 {dst_ip}",
            "t=0 0",
            f"m=video {dst_port} {'TCP/RTP/AVP' if is_tcp else 'RTP/AVP'} {pt}",
            "a=sendonly",
            f"a=rtpmap:{pt} {codec}/90000",
            "a=filesize:0",
        ]
        if is_tcp:
            sdp_lines.insert(6, "a=setup:active")
            sdp_lines.insert(7, "a=connection:new")
        if ssrc_dec is not None:
            sdp_lines.append(f"y={ssrc_dec:010d}")

        sdp_body = "\r\n".join(sdp_lines) + "\r\n"
        hdrs = [
            f"Via:{via}",
            f"From:{fr}",
            f"To:{to}",
            f"Call-ID:{call}",
            f"CSeq:{cseq}",
            f"Contact: <sip:{self.agent_id}@{self.local_ip}:{self._local_port}>",
            "Content-Type: application/sdp",
            "User-Agent: skygb28181",
        ]
        return self._sip("SIP/2.0 200 OK", hdrs, sdp_body)

    def _sub_ok(self, req: str) -> bytes:
        via = re.search(r"Via:(.*)", req).group(1).strip()
        fr = re.search(r"From:(.*)", req).group(1).strip()
        to = re.search(r"To:(.*)", req).group(1).strip()
        if "tag=" not in to:
            to += f";tag={random.randint(1,1<<31)}"
        call = re.search(r"Call-ID:(.*)", req).group(1).strip()
        cseq = re.search(r"CSeq:(.*)", req).group(1).strip()
        ev_id = re.search(r"Event:\s*Catalog;id=(\d+)", req).group(1)
        sn = re.search(r"<SN>(\d+)</SN>", req).group(1)

        body = (
            f"<?xml version='1.0' encoding='GB2312'?><Response><CmdType>Catalog</CmdType>"
            f"<SN>{sn}</SN><DeviceID>{self.agent_id}</DeviceID><Result>OK</Result></Response>"
        )
        hdrs = [
            f"Via:{via}",
            f"From:{fr}",
            f"To:{to}",
            f"Call-ID:{call}",
            f"CSeq:{cseq}",
            f"Contact:<sip:{self.agent_id}@{self.local_ip}:{self._local_port}>",
            "Expires: 600",
            "Content-Type: Application/MANSCDP+xml",
            f"Event: Catalog;id={ev_id}",
            "User-Agent: skygb28181",
        ]
        return self._sip("SIP/2.0 200 OK", hdrs, body)

    def _open_signalling_socket(self) -> None:
        if self.use_udp_signalling:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind((self.local_ip, 0))
            sock.settimeout(self.RECV_TIMEOUT)
            self.logger.info("UDP signalling %s → %s:%d", sock.getsockname(), self.server_ip, self.server_port)
            self._send = self._wrap_send(lambda d: sock.sendto(d, (self.server_ip, self.server_port)), "UDP→")
            self._recv = self._wrap_recv(lambda: sock.recvfrom(65535)[0].decode(), "UDP←")
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.connection_timeout)
            try:
                sock.connect((self.server_ip, self.server_port))
                self.logger.info("TCP signalling %s → %s:%d", sock.getsockname(), self.server_ip, self.server_port)
            except socket.timeout:
                sock.close()
                raise ConnectionError(f"Connection timeout after {self.connection_timeout} seconds")
            except Exception as e:
                sock.close()
                raise ConnectionError(f"Failed to connect: {e}")

            sock.settimeout(None)
            self._send = self._wrap_send(sock.sendall, "TCP→")
            self._recv = self._wrap_recv(lambda: self._recv_tcp(sock), "TCP←")
        self._sock = sock
        self._local_port = sock.getsockname()[1]

    def _wrap_send(self, fn: Callable[[bytes], None], label: str) -> Callable[[bytes], None]:
        def _inner(data: bytes):
            if self.verbose:
                self.logger.debug("%s\n%s", label, data.decode(errors="ignore"))
            fn(data)
        return _inner

    def _wrap_recv(self, fn: Callable[[], str], label: str) -> Callable[[], str]:
        def _inner() -> str:
            data = fn()
            if self.verbose:
                self.logger.debug("%s\n%s", label, data)
            return data
        return _inner

    def _register(self) -> None:
        cseq = 1
        for attempt in range(self.REG_TRIES):
            self._send(self._build_register(cseq))
            try:
                rsp = self._recv()
            except socket.timeout:
                self.logger.warning("REGISTER timeout (%d/%d)", attempt+1, self.REG_TRIES)
                continue
            if rsp.startswith("SIP/2.0 401"):
                nonce = re.search(r'nonce="([^"]+)"', rsp).group(1)
                realm = re.search(r'realm="([^"]+)"', rsp).group(1)
                qop_m = re.search(r'qop\s*=\s*"?([a-zA-Z0-9\-]+)', rsp)
                qop = qop_m.group(1) if qop_m else None
                resp, nc, cnonce, qop_used = self._digest_response(nonce, realm, "REGISTER", f"sip:{self.domain}", qop)
                cseq += 1
                if qop_used:
                    auth_hdr = (
                        f'Digest username="{self.agent_id}", realm="{realm}", nonce="{nonce}",'
                        f' uri="sip:{self.domain}", response="{resp}", algorithm=MD5,'
                        f' qop={qop_used}, nc={nc}, cnonce="{cnonce}"'
                    )
                else:
                    auth_hdr = (
                        f'Digest username="{self.agent_id}", realm="{realm}", nonce="{nonce}",'
                        f' uri="sip:{self.domain}", response="{resp}", algorithm=MD5'
                    )
                self._send(self._build_register(cseq, auth_hdr))
                rsp = self._recv()
            if rsp.startswith("SIP/2.0 200"):
                self.logger.info("REGISTER success")
                break
        else:
            raise RuntimeError("Failed to REGISTER after %d attempts" % self.REG_TRIES)

        info_xml = (
            f"<?xml version='1.0' encoding='GB2312'?><Response>"
            f"<CmdType>DeviceInfo</CmdType><SN>1</SN><DeviceID>{self.agent_id}</DeviceID>"
            f"<DeviceName>{self.devicename}</DeviceName><Manufacturer>{self.manufacturer}</Manufacturer>"
            f"<Model>test</Model><Firmware>1.0</Firmware><Result>OK</Result></Response>"
        )
        cat_xml = lambda sn: (
            f"<?xml version='1.0' encoding='GB2312'?><Response><CmdType>Catalog</CmdType><SN>{sn}</SN>"
            f"<DeviceID>{self.agent_id}</DeviceID><SumNum>1</SumNum><DeviceList><Item>"
            f"<DeviceID>{self.channel_id}</DeviceID><Name>ch1</Name><Manufacturer>{self.manufacturer}</Manufacturer>"
            f"<Model>v1</Model><Status>ON</Status></Item></DeviceList></Response>"
        )
        keep_xml = (
            f"<?xml version='1.0' encoding='GB2312'?><Notify><CmdType>Keepalive</CmdType><SN>1</SN>"
            f"<DeviceID>{self.agent_id}</DeviceID><Status>OK</Status></Notify>"
        )
        self._send(self._build_message(keep_xml, 1, "keep"))
        self._send(self._build_message(info_xml, 2, "info"))
        self._send(self._build_message(cat_xml(1), 3, "cat"))

    def _start_heartbeat(self):
        self._stop_heartbeat()
        self._heartbeat_stop_evt = threading.Event()

        def _hb():
            seq = 10
            keep_xml = (
                f"<?xml version='1.0' encoding='GB2312'?><Notify><CmdType>Keepalive</CmdType><SN>{{}}</SN>"
                f"<DeviceID>{self.agent_id}</DeviceID><Status>OK</Status></Notify>"
            )
            while not self._heartbeat_stop_evt.is_set():
                if self._heartbeat_stop_evt.wait(self.HB_GAP):
                    break
                try:
                    if self._connected and self._send:
                        self._send(self._build_message(keep_xml.format(seq), seq, "k"))
                        seq += 1
                except Exception as e:
                    self.logger.warning("Heartbeat send failed: %s", e)
                    break

        self._heartbeat_thread = threading.Thread(target=_hb, daemon=True)
        self._heartbeat_thread.start()

    def _stop_heartbeat(self):
        if self._heartbeat_stop_evt:
            self._heartbeat_stop_evt.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=1)
        self._heartbeat_thread = None
        self._heartbeat_stop_evt = None

    def _event_loop(self):
        while not self._shutdown_requested and self._connected:
            try:
                pkt = self._recv()
            except socket.timeout:
                continue
            except ConnectionError as exc:
                self.logger.error("Connection error in event loop: %s", exc)
                raise
            except Exception as exc:
                self.logger.exception("Receive error — leaving main loop: %s", exc)
                raise

            if pkt.startswith("INVITE"):
                self._handle_invite(pkt)
            elif pkt.startswith("BYE"):
                self._send(self._ok200(pkt))
                self._stop_push()
                self.logger.info("Session closed — waiting for next INVITE …")
            elif pkt.startswith("MESSAGE"):
                self._handle_message(pkt)
            elif pkt.startswith("SUBSCRIBE"):
                self._send(self._sub_ok(pkt))

    def _handle_invite(self, invite_msg: str):
        via = re.search(r"Via:(.*)", invite_msg).group(1).strip()
        fr = re.search(r"From:(.*)", invite_msg).group(1).strip()
        to = re.search(r"To:(.*)", invite_msg).group(1).strip()
        call = re.search(r"Call-ID:(.*)", invite_msg).group(1).strip()
        cseq = re.search(r"CSeq:(.*)", invite_msg).group(1).strip()
        self._send(self._sip("SIP/2.0 100 Trying", [f"Via:{via}", f"From:{fr}", f"To:{to}", f"Call-ID:{call}", f"CSeq:{cseq}"]))

        try:
            dst_ip, dst_port, pt, is_tcp, ssrc_dec, codec = self._parse_invite(invite_msg)
        except ValueError as exc:
            self.logger.warning("Could not parse SDP in INVITE: %s — ignored", exc)
            return

        self._send(self._invite_ok(invite_msg, dst_ip, dst_port, pt, is_tcp, codec, ssrc_dec))
        _ = self._recv()

        self._start_push(dst_ip, dst_port, is_tcp, codec, pt, ssrc_dec)

    def _handle_message(self, msg: str):
        self._send(self._ok200(msg))
        if "<Query>" in msg:
            cmd = re.search(r"<CmdType>(.+?)</CmdType>", msg).group(1)
            sn = re.search(r"<SN>(\d+)</SN>", msg).group(1)
            if cmd == "Catalog":
                cat_xml = (
                    f"<?xml version='1.0' encoding='GB2312'?><Response><CmdType>Catalog</CmdType>"
                    f"<SN>{sn}</SN><DeviceID>{self.agent_id}</DeviceID><SumNum>1</SumNum><DeviceList><Item>"
                    f"<DeviceID>{self.channel_id}</DeviceID><Name>ch1</Name><Manufacturer>{self.manufacturer}</Manufacturer>"
                    f"<Model>v1</Model><Status>ON</Status></Item></DeviceList></Response>"
                )
                self._send(self._build_message(cat_xml, 99, "catR"))
            elif cmd == "DeviceInfo":
                info_xml = (
                    f"<?xml version='1.0' encoding='GB2312'?><Response>"
                    f"<CmdType>DeviceInfo</CmdType><SN>{sn}</SN><DeviceID>{self.agent_id}</DeviceID>"
                    f"<DeviceName>{self.devicename}</DeviceName><Manufacturer>{self.manufacturer}</Manufacturer>"
                    f"<Model>test</Model><Firmware>1.0</Firmware><Result>OK</Result></Response>"
                )
                self._send(self._build_message(info_xml, 98, "infoR"))

    def _start_push(
        self,
        dst_ip: str,
        dst_port: int,
        use_tcp: bool,
        codec: str,
        pt: int,
        ssrc_dec: Optional[int],
    ) -> None:
        self._stop_push()
        self._push_stop_evt = threading.Event()
        self._push_thread = threading.Thread(
            target=self._gst_loop,
            args=(dst_ip, dst_port, use_tcp, codec, pt, ssrc_dec, self._push_stop_evt),
            daemon=True,
        )
        self._push_thread.start()

    def _gst_loop(
            self,
            dst_ip: str,
            dst_port: int,
            use_tcp: bool,
            codec: str,
            pt: int,
            ssrc_dec: Optional[int],
            stop_evt: threading.Event
        ) -> None:
        gst_cmd = self._make_gst_cmd(dst_ip, dst_port, use_tcp, codec, pt, ssrc_dec)
        self.logger.info("GStreamer cmd: %s", shlex.join(gst_cmd))
        proc = subprocess.Popen(gst_cmd)
        try:
            while not stop_evt.is_set():
                time.sleep(1)
        finally:
            proc.terminate()
            proc.wait()
            self.logger.info("GStreamer exited")

    def _source_elements(self) -> List[str]:
        uri = self.source
        if uri == "test":
            return ["videotestsrc", "is-live=true",
                    "!", "video/x-raw,width=640,height=480,framerate=25/1",
                    "!", "x264enc", "key-int-max=50", "tune=zerolatency", "bitrate=500"]
        if uri.startswith("rtsp://"):
            return ["rtspsrc", f"location={uri}", "latency=0", "!", "rtph264depay"]
        if uri.startswith("udp://"):
            loc = uri[6:]
            host, _, port = loc.partition(":")
            elements = ["udpsrc", f"port={port}"]
            if host:
                elements += [f"multicast-group={host}"]
            elements += ["!", "application/x-rtp", "!", "rtph264depay"]
            return elements
        if uri.startswith("file://"):
            path = uri[7:]
            return ["filesrc", f"location={path}", "!", "qtdemux", "name=demux", "demux.video_0"]
        if uri.startswith("v4l2://"):
            dev = uri[7:]
            return ["v4l2src", f"device={dev}"]
        raise ValueError(f"Unsupported --source URI: {uri}")

    def _make_gst_cmd(
            self,
            dst_ip: str,
            dst_port: int,
            use_tcp: bool,
            codec: str,
            pt: int,
            ssrc_dec: Optional[int]
    ) -> List[str]:
        protocol = "tcp" if use_tcp else "udp"
        ssrc_opt: List[str] = [] if ssrc_dec is None else [f"ssrc=0x{ssrc_dec:08x}"]
        src_chain = self._source_elements()

        if codec == "PS":
            pay_chain = [
                "!", "h264parse", "!", "mpegpsmux",
                "!", "gb28181sink", f"protocol={protocol}", f"host={dst_ip}", f"port={dst_port}", f"pt={pt}", *ssrc_opt,
            ]
        else:
            pay_chain = [
                "!", "videoconvert", "!", "x264enc", "key-int-max=50", "tune=zerolatency", "bitrate=800",
                "!", "rtph264pay", "config-interval=-1", f"pt={pt}",
                "!", "gb28181sink", f"protocol={protocol}", f"host={dst_ip}", f"port={dst_port}", *ssrc_opt,
            ]
        return ["gst-launch-1.0", "-q", *src_chain, *pay_chain]

    def _stop_push(self):
        if self._push_stop_evt is not None:
            self._push_stop_evt.set()
            self._push_thread.join()
            self._push_thread = None
            self._push_stop_evt = None

    @staticmethod
    def _recv_tcp(sock: socket.socket) -> str:
        """Read exactly one SIP message from *sock* (TCP-framed with CRLFCRLF)."""
        buf = b""
        while True:
            if b"\r\n\r\n" in buf:
                hdr_bin, rest = buf.split(b"\r\n\r\n", 1)
                break
            if b"\n\n" in buf:
                hdr_bin, rest = buf.split(b"\n\n", 1)
                break
            chunk = sock.recv(8192)
            if not chunk:
                raise ConnectionError("TCP closed before header complete")
            buf += chunk
        hdr = hdr_bin.decode(errors="ignore")

        m = re.search(r"Content-Length\s*:\s*(\d+)", hdr, re.I)
        need = int(m.group(1)) if m else 0
        body = rest
        while len(body) < need:
            chunk = sock.recv(need - len(body))
            if not chunk:
                raise ConnectionError("TCP closed before body complete")
            body += chunk

        return hdr + "\r\n\r\n" + body[:need].decode(errors="ignore")

    def _parse_invite(self, msg: str) -> Tuple[str, int, int, bool, Optional[int], str]:
        """Return (*dst_ip*, *dst_port*, *pt*, *is_tcp*, *ssrc*, *codec*)."""
        if "\r\n\r\n" in msg:
            body = msg.split("\r\n\r\n", 1)[1]
        elif "\n\n" in msg:
            body = msg.split("\n\n", 1)[1]
        else:
            raise ValueError("SDP not found in INVITE")

        dst_ip: Optional[str] = None
        dst_port: Optional[int] = None
        is_tcp = False
        cand_list: List[int] = []
        pt_map: dict[int, str] = {}
        ssrc_dec: Optional[int] = None

        for line in body.splitlines():
            l = line.strip()
            if l.startswith("c=IN IP4"):
                dst_ip = l.split()[2]
            elif l.startswith("m=video"):
                sp = l.split()
                dst_port = int(sp[1])
                is_tcp = sp[2].upper().startswith("TCP")
                cand_list = [int(x) for x in sp[3:]]
            elif l.lower().startswith("a=rtpmap:"):
                n, enc = l.split()[0][9:], l.split()[1].split("/")[0]
                pt_map[int(n)] = enc.upper()
            elif l.startswith("y="):
                try:
                    ssrc_dec = int(l[2:])
                except ValueError:
                    pass

        if not cand_list:
            raise ValueError("m=video line not found")

        for want_pt, want_codec in self._PT_PRIORITY:
            if want_pt in cand_list and pt_map.get(want_pt) == want_codec:
                return dst_ip, dst_port, want_pt, is_tcp, ssrc_dec, want_codec
        pt = cand_list[0]
        return dst_ip, dst_port, pt, is_tcp, ssrc_dec, pt_map.get(pt, "H264")

    def _shutdown(self):
        self._shutdown_requested = True
        self._connected = False
        self._stop_heartbeat()
        self._stop_push()
        if self._sock:
            self._sock.close()
            self._sock = None
        self.logger.info("Shutdown complete")


###############################################################################
# ──────────────────────────────── Multi-instance Manager ──────────────────────────────── #
###############################################################################

class MultiPusherManager:
    """管理多个GB28181推流实例，支持配置热重载和单实例重启。"""

    # Used to identify a config on disk across reloads.
    @staticmethod
    def _config_fingerprint(config: Dict[str, Any]) -> str:
        """Return a stable hash of the *mutable* config fields.

        ``_config_file`` and other bookkeeping keys are excluded.
        """
        keys = ("server_ip", "server_port", "server_id", "domain",
                "agent_id", "agent_password", "channel_id", "source",
                "udp", "local_ip", "verbose", "reconnect_interval",
                "max_reconnect_attempts", "connection_timeout",
                "manufacturer", "devicename",
                "rtsp_precheck", "rtsp_precheck_timeout")
        payload = {k: config.get(k) for k in keys}
        return json.dumps(payload, sort_keys=True, default=str)

    def __init__(self, configs: List[Dict[str, Any]], config_dir: str = "config"):
        self.configs = configs
        self._config_dir = config_dir  # used by reload() to re-scan disk
        self.pushers: List[GB28181Pusher] = []
        self.threads: List[threading.Thread] = []
        # channel_id -> (pusher, thread, fingerprint) — used by reload()
        self._index: Dict[str, Tuple[GB28181Pusher, threading.Thread, str]] = {}
        # Lock guarding structural changes (add/remove pusher).
        self._lock = threading.Lock()

    def _build_pusher(self, config: Dict[str, Any], instance_name: str) -> GB28181Pusher:
        return GB28181Pusher(
            server_ip=config["server_ip"],
            server_port=config["server_port"],
            server_id=config["server_id"],
            domain=config["domain"],
            agent_id=config["agent_id"],
            agent_password=config["agent_password"],
            channel_id=config["channel_id"],
            source=config["source"],
            use_udp_signalling=config["udp"],
            local_ip=config["local_ip"],
            verbose=config["verbose"],
            reconnect_interval=config["reconnect_interval"],
            max_reconnect_attempts=config["max_reconnect_attempts"],
            connection_timeout=config["connection_timeout"],
            manufacturer=config["manufacturer"],
            devicename=config["devicename"],
            instance_name=instance_name,
            rtsp_precheck=config["rtsp_precheck"],
            rtsp_precheck_timeout=config["rtsp_precheck_timeout"],
        )

    @staticmethod
    def _channel_id_of(config: Dict[str, Any]) -> str:
        """The stable identity used to track a pusher across reloads."""
        return str(config.get("channel_id") or config.get("_config_file") or "")

    def _start_one(self, config: Dict[str, Any], instance_name: str) -> Tuple[GB28181Pusher, threading.Thread]:
        pusher = self._build_pusher(config, instance_name)
        thread = threading.Thread(
            target=pusher.run_forever,
            name=f"Pusher-{instance_name}",
            daemon=False,
        )
        thread.start()
        return pusher, thread

    def start_all(self):
        """启动所有推流实例"""
        for i, config in enumerate(self.configs):
            instance_name = config.get('_config_file', f'instance_{i}')
            try:
                pusher, thread = self._start_one(config, instance_name)
            except Exception as e:
                LOGGER.error(f"Failed to start {instance_name}: {e}")
                continue
            with self._lock:
                self.pushers.append(pusher)
                self.threads.append(thread)
                self._index[self._channel_id_of(config)] = (
                    pusher, thread, self._config_fingerprint(config)
                )
            LOGGER.info(f"Started pusher instance: {instance_name}")
            time.sleep(0.5)  # 稍微延迟启动，避免同时连接

    def _stop_one(self, pusher: GB28181Pusher, thread: threading.Thread,
                  join_timeout: float = 5.0) -> None:
        """Request shutdown on *pusher* and wait up to *join_timeout* seconds."""
        try:
            pusher._shutdown()
        except Exception as e:
            LOGGER.error(f"Error requesting shutdown for {pusher.instance_name}: {e}")
        thread.join(timeout=join_timeout)
        if thread.is_alive():
            LOGGER.warning(
                "Pusher thread %s did not exit within %.1fs — leaking (daemon=False).",
                thread.name, join_timeout,
            )

    def wait_all(self):
        """等待所有线程结束"""
        try:
            for thread in self.threads:
                thread.join()
        except KeyboardInterrupt:
            LOGGER.info("Received interrupt signal, shutting down all pushers...")
            self.shutdown_all()

    def shutdown_all(self):
        """关闭所有推流实例"""
        with self._lock:
            items = list(self._index.values())
        for pusher, thread, _fp in items:
            self._stop_one(pusher, thread)
        with self._lock:
            self.pushers.clear()
            self.threads.clear()
            self._index.clear()

    # ------------------------------------------------------------------
    # Hot reload & per-instance control (used by SIGHUP and the web UI)
    # ------------------------------------------------------------------

    def reload(self) -> Dict[str, Any]:
        """Diff current config dir against running instances and reconcile.

        Returns a summary dict ``{added, updated, removed, errors}`` for
        logging and the web API.  Individual config errors do not abort the
        whole reload — they are reported under ``errors``.
        """
        config_dir = self._config_dir or "config"
        try:
            new_configs, scan_errors = load_all_configs(config_dir)
        except Exception as e:
            LOGGER.error("Reload aborted — cannot scan %s: %s", config_dir, e)
            return {"added": [], "updated": [], "removed": [], "errors": [str(e)]}

        summary: Dict[str, Any] = {"added": [], "updated": [], "removed": [], "errors": list(scan_errors)}

        # Index new configs by channel_id (preserve insertion order for "added")
        new_index: Dict[str, Dict[str, Any]] = {}
        for cfg in new_configs:
            cid = self._channel_id_of(cfg)
            if not cid:
                msg = f"Config {cfg.get('_config_file')} has no channel_id — skipped"
                LOGGER.error(msg)
                summary["errors"].append(msg)
                continue
            if cid in new_index:
                msg = f"Duplicate channel_id {cid} — skipping {cfg.get('_config_file')}"
                LOGGER.error(msg)
                summary["errors"].append(msg)
                continue
            new_index[cid] = cfg

        with self._lock:
            current = dict(self._index)

        # ---- 1. Remove instances whose config is gone or whose file was deleted
        for cid, (pusher, thread, _fp) in current.items():
            if cid not in new_index:
                self._stop_one(pusher, thread)
                with self._lock:
                    self._index.pop(cid, None)
                    if pusher in self.pushers:
                        self.pushers.remove(pusher)
                    if thread in self.threads:
                        self.threads.remove(thread)
                LOGGER.info("Reload: removed %s (channel_id=%s)", pusher.instance_name, cid)
                summary["removed"].append(pusher.instance_name)

        # ---- 2. Add new / update changed
        for cid, cfg in new_index.items():
            new_fp = self._config_fingerprint(cfg)
            instance_name = cfg.get("_config_file", cid)
            if cid not in current:
                # Brand new instance
                try:
                    pusher, thread = self._start_one(cfg, instance_name)
                except Exception as e:
                    msg = f"Failed to start {instance_name}: {e}"
                    LOGGER.error(msg)
                    summary["errors"].append(msg)
                    continue
                with self._lock:
                    self.pushers.append(pusher)
                    self.threads.append(thread)
                    self._index[cid] = (pusher, thread, new_fp)
                LOGGER.info("Reload: added %s (channel_id=%s)", instance_name, cid)
                summary["added"].append(instance_name)
            else:
                old_pusher, old_thread, old_fp = current[cid]
                if old_fp != new_fp:
                    self._stop_one(old_pusher, old_thread)
                    try:
                        pusher, thread = self._start_one(cfg, instance_name)
                    except Exception as e:
                        msg = f"Failed to restart {instance_name}: {e}"
                        LOGGER.error(msg)
                        summary["errors"].append(msg)
                        with self._lock:
                            self._index.pop(cid, None)
                            if old_pusher in self.pushers:
                                self.pushers.remove(old_pusher)
                            if old_thread in self.threads:
                                self.threads.remove(old_thread)
                        continue
                    with self._lock:
                        self.pushers.remove(old_pusher)
                        self.threads.remove(old_thread)
                        self.pushers.append(pusher)
                        self.threads.append(thread)
                        self._index[cid] = (pusher, thread, new_fp)
                    LOGGER.info("Reload: updated %s (channel_id=%s)", instance_name, cid)
                    summary["updated"].append(instance_name)
                # else: fingerprint matches, leave running instance alone

        return summary

    def restart_instance(self, channel_id: str) -> bool:
        """Stop and re-create a single instance.  Returns False if not found."""
        with self._lock:
            entry = self._index.get(channel_id)
        if entry is None:
            LOGGER.warning("restart_instance: channel_id=%s not found", channel_id)
            return False
        pusher, thread, _fp = entry
        instance_name = pusher.instance_name

        # Find the current config to rebuild from.
        config = next((c for c in self.configs if self._channel_id_of(c) == channel_id), None)
        if config is None:
            # Config was removed from disk — just stop the instance.
            self._stop_one(pusher, thread)
            with self._lock:
                self._index.pop(channel_id, None)
                if pusher in self.pushers:
                    self.pushers.remove(pusher)
                if thread in self.threads:
                    self.threads.remove(thread)
            LOGGER.info("restart_instance: removed orphan %s (channel_id=%s)",
                        instance_name, channel_id)
            return True

        self._stop_one(pusher, thread)
        try:
            new_pusher, new_thread = self._start_one(config, instance_name)
        except Exception as e:
            LOGGER.error("restart_instance: failed to start %s: %s", instance_name, e)
            with self._lock:
                self._index.pop(channel_id, None)
                if pusher in self.pushers:
                    self.pushers.remove(pusher)
                if thread in self.threads:
                    self.threads.remove(thread)
            return False
        with self._lock:
            self.pushers.remove(pusher)
            self.threads.remove(thread)
            self.pushers.append(new_pusher)
            self.threads.append(new_thread)
            self._index[channel_id] = (new_pusher, new_thread, self._config_fingerprint(config))
        LOGGER.info("restart_instance: restarted %s (channel_id=%s)", instance_name, channel_id)
        return True

    def get_status(self) -> List[Dict[str, Any]]:
        """Snapshot of every running pusher for the web UI."""
        with self._lock:
            items = list(self._index.items())
        out = []
        for cid, (pusher, thread, _fp) in items:
            uptime = 0.0
            if thread.is_alive():
                # threading.Thread has no start_time; we approximate from process.
                pass
            out.append({
                "channel_id": cid,
                "instance_name": pusher.instance_name,
                "server_ip": pusher.server_ip,
                "server_id": pusher.server_id,
                "agent_id": pusher.agent_id,
                "source": pusher.source,
                "registered": bool(getattr(pusher, "_connected", False)),
                "thread_alive": thread.is_alive(),
            })
        return out

    # ----- Web CRUD: add / update / delete a single channel -----------------

    def get_channel(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """Return ``{channel_id, config_file, config}`` for *channel_id* or ``None``.

        *config* is the full on-disk dict with the internal ``_config_file`` key
        included so the web UI can show / preserve the file name on round-trip.
        """
        with self._lock:
            entry = self._index.get(channel_id)
        # Prefer the _config_file recorded in self.configs (more reliable than
        # the instance name, which has the ".json" stripped).
        config = None
        config_file = None
        for c in self.configs:
            if self._channel_id_of(c) == channel_id:
                config = dict(c)
                config_file = c.get("_config_file") or (f"{entry[0].instance_name}.json" if entry else None)
                break
        if config is None and entry is not None:
            # Orphaned instance whose config was deleted from disk — fall back
            # to the live instance's instance_name and try the file directly.
            config_file = f"{entry[0].instance_name}.json"
            path = _safe_config_path(self._config_dir, config_file)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    config = json.load(f)
                    config["_config_file"] = config_file
        if config is None:
            return None
        return {"channel_id": channel_id, "config_file": config_file, "config": config}

    def add_channel(self, config: Dict[str, Any], config_file: str) -> Dict[str, Any]:
        """Create a new channel: write the JSON file then start a pusher.

        *config* is the typed config dict (from :func:`_build_config_from_form`).
        *config_file* is the desired filename inside ``self._config_dir``.

        Returns a summary dict ``{channel_id, config_file, started}``.  Raises
        :class:`FileExistsError` if *config_file* already exists, and
        :class:`ValueError` on validation failures.
        """
        if not self._config_dir:
            raise ValueError("config_dir is not set")
        path = _safe_config_path(self._config_dir, config_file)
        if os.path.exists(path):
            raise FileExistsError(f"config_file already exists: {config_file}")
        # Inject _config_file so reload() / start_all() pick it up correctly.
        config_with_meta = dict(config)
        config_with_meta["_config_file"] = config_file
        _save_config_atomic(path, config_with_meta)
        instance_name = config_file[:-len(".json")]
        try:
            pusher, thread = self._start_one(config_with_meta, instance_name)
        except Exception as e:
            # Roll back the file so the disk and live state don't diverge.
            try:
                os.remove(path)
            except OSError:
                pass
            raise
        with self._lock:
            self.configs.append(config_with_meta)
            self.pushers.append(pusher)
            self.threads.append(thread)
            self._index[self._channel_id_of(config_with_meta)] = (
                pusher, thread, self._config_fingerprint(config_with_meta)
            )
        LOGGER.info("add_channel: created %s (channel_id=%s)",
                    instance_name, self._channel_id_of(config_with_meta))
        return {
            "channel_id": self._channel_id_of(config_with_meta),
            "config_file": config_file,
            "started": True,
        }

    def update_channel(self, channel_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Replace the config of an existing channel and restart it.

        The new *config* is written to the same file the original was loaded
        from (looked up via the running instance's ``instance_name``), and the
        instance is stopped and re-created.  Returns
        ``{channel_id, config_file, updated, restarted}``.
        """
        with self._lock:
            entry = self._index.get(channel_id)
        if entry is None:
            raise KeyError(f"channel_id not found: {channel_id}")
        pusher, thread, _fp = entry
        instance_name = pusher.instance_name
        config_file = f"{instance_name}.json"
        path = _safe_config_path(self._config_dir, config_file)
        config_with_meta = dict(config)
        config_with_meta["_config_file"] = config_file
        _save_config_atomic(path, config_with_meta)
        # Replace in self.configs (matched by old config_file)
        with self._lock:
            for i, c in enumerate(self.configs):
                if c.get("_config_file") == config_file:
                    self.configs[i] = config_with_meta
                    break
            else:
                self.configs.append(config_with_meta)
        self._stop_one(pusher, thread)
        try:
            new_pusher, new_thread = self._start_one(config_with_meta, instance_name)
        except Exception as e:
            LOGGER.error("update_channel: failed to start %s: %s", instance_name, e)
            with self._lock:
                self._index.pop(channel_id, None)
                if pusher in self.pushers:
                    self.pushers.remove(pusher)
                if thread in self.threads:
                    self.threads.remove(thread)
            raise
        with self._lock:
            self.pushers.remove(pusher)
            self.threads.remove(thread)
            self.pushers.append(new_pusher)
            self.threads.append(new_thread)
            self._index[channel_id] = (new_pusher, new_thread,
                                       self._config_fingerprint(config_with_meta))
        LOGGER.info("update_channel: updated %s (channel_id=%s)", instance_name, channel_id)
        return {
            "channel_id": channel_id,
            "config_file": config_file,
            "updated": True,
            "restarted": True,
        }

    def delete_channel(self, channel_id: str) -> Dict[str, Any]:
        """Hard-delete a channel: stop the instance and remove its JSON file.

        If the file is already gone (orphaned instance) we still stop the
        instance and report success.
        """
        with self._lock:
            entry = self._index.get(channel_id)
        if entry is None:
            raise KeyError(f"channel_id not found: {channel_id}")
        pusher, thread, _fp = entry
        instance_name = pusher.instance_name
        config_file = f"{instance_name}.json"
        self._stop_one(pusher, thread)
        with self._lock:
            self._index.pop(channel_id, None)
            if pusher in self.pushers:
                self.pushers.remove(pusher)
            if thread in self.threads:
                self.threads.remove(thread)
            self.configs = [c for c in self.configs
                            if c.get("_config_file") != config_file]
        # Best-effort file removal — don't fail the call if the file is gone.
        try:
            path = _safe_config_path(self._config_dir, config_file)
            if os.path.exists(path):
                os.remove(path)
        except ValueError as e:
            LOGGER.warning("delete_channel: %s", e)
        except OSError as e:
            LOGGER.warning("delete_channel: failed to remove %s: %s", config_file, e)
        LOGGER.info("delete_channel: removed %s (channel_id=%s)", instance_name, channel_id)
        return {
            "channel_id": channel_id,
            "config_file": config_file,
            "deleted": True,
        }


###############################################################################
# ──────────────────────────────── Web Admin UI ──────────────────────────────── #
###############################################################################

_WEB_INDEX_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>skygb28181 管理后台</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.5 -apple-system, BlinkMacSystemFont, "PingFang SC",
         "Hiragino Sans GB", "Microsoft YaHei", "微软雅黑", "Segoe UI",
         system-ui, sans-serif;
         background: #0e1419; color: #d6dde3; }
  header { padding: 14px 20px; background: #131c25; border-bottom: 1px solid #1f2a36;
           display: flex; align-items: center; gap: 16px; }
  header h1 { font-size: 18px; margin: 0; color: #7ed1ff; }
  header .stats { color: #8a99ad; font-size: 13px; }
  header .stats b { color: #d6dde3; }
  .actions { margin-left: auto; display: flex; gap: 8px; }
  button { background: #1d2a38; color: #d6dde3; border: 1px solid #2a3a4d;
           padding: 6px 12px; border-radius: 4px; cursor: pointer; font-size: 13px;
           font-family: inherit; }
  button:hover { background: #243446; }
  button.primary { background: #1f4f7a; border-color: #2974ad; }
  button.primary:hover { background: #255d8e; }
  button.danger { background: #5a1f1f; border-color: #8a2929; }
  button.danger:hover { background: #6b2424; }
  main { padding: 16px 20px; }
  .empty { text-align: center; padding: 60px; color: #6a7886; }
  .grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fill, minmax(360px, 1fr)); }
  .card { background: #131c25; border: 1px solid #1f2a36; border-radius: 6px;
          padding: 12px 14px; }
  .card .row { display: flex; justify-content: space-between; align-items: center;
               margin-bottom: 6px; }
  .card .name { font-weight: 600; color: #e0e6ed; word-break: break-all; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 10px;
           font-size: 11px; font-weight: 600; }
  .badge.ok { background: #0f3d24; color: #6ee79c; }
  .badge.warn { background: #4a3a0e; color: #f0c060; }
  .badge.err { background: #4a1010; color: #f08080; }
  .meta { color: #8a99ad; font-size: 12px; }
  .meta div { margin: 2px 0; word-break: break-all; }
  .meta b { color: #aab4c1; font-weight: 500; }
  .card .row.actions-row { margin-top: 10px; padding-top: 10px;
                           border-top: 1px solid #1f2a36; gap: 6px; }
  .card .row.actions-row button { flex: 1; }
  .toast { position: fixed; bottom: 16px; right: 16px; background: #1d2a38;
           border: 1px solid #2a3a4d; padding: 10px 16px; border-radius: 4px;
           opacity: 0; transition: opacity .3s; z-index: 100; }
  .toast.show { opacity: 1; }
  .toast.err { border-color: #8a2929; }
  code { background: #0a0f15; padding: 1px 5px; border-radius: 3px; font-size: 12px;
         font-family: "SF Mono", Menlo, Consolas, "微软雅黑", monospace; }

  /* 弹窗 */
  .modal-bg { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.6);
              align-items: center; justify-content: center; z-index: 50; }
  .modal-bg.show { display: flex; }
  .modal { background: #131c25; border: 1px solid #2a3a4d; border-radius: 8px;
           padding: 20px; width: 560px; max-width: 95vw; max-height: 90vh;
           overflow-y: auto; }
  .modal h2 { margin: 0 0 12px; font-size: 16px; color: #7ed1ff; }
  .modal .grid-form { display: grid; grid-template-columns: 130px 1fr; gap: 8px 12px;
                      align-items: center; }
  .modal .grid-form label { color: #8a99ad; font-size: 12px; text-align: right; }
  .modal .grid-form input, .modal .grid-form select {
    background: #0a0f15; border: 1px solid #2a3a4d; color: #d6dde3;
    padding: 6px 8px; border-radius: 4px; font-size: 13px; width: 100%;
    font-family: inherit; }
  .modal .grid-form input:focus { outline: none; border-color: #2974ad; }
  .modal .row-buttons { display: flex; gap: 8px; justify-content: flex-end;
                        margin-top: 16px; }
  .modal .err { color: #f08080; font-size: 12px; margin-top: 8px; min-height: 14px; }
  .pw-wrap { position: relative; }
  .pw-wrap .toggle { position: absolute; right: 6px; top: 50%; transform: translateY(-50%);
                     background: transparent; border: 0; color: #7ed1ff; cursor: pointer;
                     font-size: 11px; padding: 2px 6px; }
  .small { color: #6a7886; font-size: 11px; }
</style>
</head>
<body>
<header>
  <h1>skygb28181 推流管理</h1>
  <div class="stats" id="stats">加载中…</div>
  <div class="actions">
    <button class="primary" onclick="openAdd()">+ 新增通道</button>
    <button onclick="reloadAll()">↻ 重载配置</button>
  </div>
</header>
<main>
  <div id="container"><div class="empty">加载中…</div></div>
</main>
<div class="toast" id="toast"></div>

<!-- 通道新增/编辑 弹窗 -->
<div class="modal-bg" id="modal">
  <div class="modal">
    <h2 id="modalTitle">新增通道</h2>
    <form id="chForm" class="grid-form" onsubmit="return submitForm(event)">
      <label for="f-config_file">配置文件名 *</label>
      <input id="f-config_file" name="config_file" required pattern="[A-Za-z0-9_\\-]+\\.json"
             placeholder="camera_lobby.json">
      <label></label>
      <div class="small">保存到 <code>config/</code> 目录下,每个通道一个 JSON 文件。</div>

      <label for="f-channel_id">通道 ID *</label>
      <input id="f-channel_id" name="channel_id" required
             placeholder="340000000000000000001">

      <label for="f-server_ip">国标服务器 IP *</label>
      <input id="f-server_ip" name="server_ip" required value="101.226.23.126">

      <label for="f-server_port">服务器端口</label>
      <input id="f-server_port" name="server_port" type="number" value="8116">

      <label for="f-server_id">服务器 ID *</label>
      <input id="f-server_id" name="server_id" required value="41010500002000000001">

      <label for="f-domain">域</label>
      <input id="f-domain" name="domain" value="4101050000">

      <label for="f-agent_id">设备 ID *</label>
      <input id="f-agent_id" name="agent_id" required
             placeholder="340000000000000000001">

      <label for="f-agent_password">设备密码 *</label>
      <div class="pw-wrap">
        <input id="f-agent_password" name="agent_password" type="password" required value="12345678">
        <button type="button" class="toggle" onclick="togglePw('f-agent_password', this)">显示</button>
      </div>

      <label for="f-source">RTSP 源 *</label>
      <input id="f-source" name="source" required
             placeholder="rtsp://用户名:密码@192.168.1.122/h264/ch1/main/av_stream">

      <label for="f-udp">UDP 信令</label>
      <select id="f-udp" name="udp">
        <option value="false">关闭 (TCP)</option><option value="true">启用 (UDP)</option>
      </select>

      <label for="f-local_ip">本机 IP</label>
      <input id="f-local_ip" name="local_ip" placeholder="(自动检测)">

      <label for="f-manufacturer">厂商</label>
      <input id="f-manufacturer" name="manufacturer" value="gzhaibaogd">

      <label for="f-devicename">设备名</label>
      <input id="f-devicename" name="devicename" value="Superdock">

      <label for="f-verbose">详细日志</label>
      <select id="f-verbose" name="verbose">
        <option value="false">关闭</option><option value="true">开启</option>
      </select>

      <label for="f-reconnect_interval">重连间隔 (秒)</label>
      <input id="f-reconnect_interval" name="reconnect_interval" type="number" value="5">

      <label for="f-max_reconnect_attempts">最大重连次数</label>
      <input id="f-max_reconnect_attempts" name="max_reconnect_attempts" type="number" value="0">

      <label for="f-connection_timeout">连接超时 (秒)</label>
      <input id="f-connection_timeout" name="connection_timeout" type="number" value="10">

      <label for="f-rtsp_precheck">RTSP 预检</label>
      <select id="f-rtsp_precheck" name="rtsp_precheck">
        <option value="true">开启</option><option value="false">关闭</option>
      </select>

      <label for="f-rtsp_precheck_timeout">预检超时 (秒)</label>
      <input id="f-rtsp_precheck_timeout" name="rtsp_precheck_timeout" type="number" value="5">

      <label></label>
      <div id="formErr" class="err"></div>

      <label></label>
      <div class="row-buttons">
        <button type="button" onclick="closeModal()">取消</button>
        <button type="submit" class="primary" id="submitBtn">保存</button>
      </div>
    </form>
  </div>
</div>

<script>
let channels = [];
let editingCid = null;  // null 表示新增

async function fetchStatus() {
  try {
    const r = await fetch('/api/channels');
    const j = await r.json();
    channels = j.channels || [];
    render();
  } catch (e) { toast('加载失败: ' + e, true); }
}

const STATE_LABEL = { ok: '已注册', warn: '未注册', err: '已停止' };

function render() {
  const total = channels.length;
  const ok = channels.filter(c => c.registered && c.thread_alive).length;
  const bad = total - ok;
  document.getElementById('stats').innerHTML =
    `共 <b>${total}</b> 个通道 · <b style="color:#6ee79c">${ok}</b> 已注册 · ` +
    `<b style="color:${bad ? '#f08080' : '#8a99ad'}">${bad}</b> 异常 · ` +
    `每 5 秒自动刷新`;

  const c = document.getElementById('container');
  if (!total) { c.innerHTML = '<div class="empty">暂无通道。点击"+ 新增通道"开始添加。</div>'; return; }
  c.className = 'grid';
  c.innerHTML = channels.map(ch => {
    const state = !ch.thread_alive ? 'err'
                : ch.registered ? 'ok' : 'warn';
    const stateText = STATE_LABEL[state];
    return `
      <div class="card">
        <div class="row">
          <div class="name">${esc(ch.instance_name)}</div>
          <span class="badge ${state}">${stateText}</span>
        </div>
        <div class="meta">
          <div><b>配置文件</b> <code>${esc(ch.config_file || '(缺失)')}</code></div>
          <div><b>通道 ID</b> <code>${esc(ch.channel_id)}</code></div>
          <div><b>国标服务器</b> ${esc(ch.server_ip)} (id ${esc(ch.server_id)})</div>
          <div><b>设备 ID</b> <code>${esc(ch.agent_id)}</code></div>
          <div><b>RTSP 源</b> <code>${esc(ch.source)}</code></div>
          <div><b>运行状态</b> ${ch.thread_alive ? '运行中' : '已停止'}</div>
        </div>
        <div class="row actions-row">
          <button onclick="restart('${esc(ch.channel_id)}')">↻ 重启</button>
          <button onclick="openEdit('${esc(ch.channel_id)}')">✎ 编辑</button>
          <button class="danger" onclick="del('${esc(ch.channel_id)}', '${esc(ch.instance_name)}')">✕ 删除</button>
        </div>
      </div>`;
  }).join('');
}

function esc(s) { return String(s ?? '').replace(/[&<>"']/g, c =>
  ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]); }

function toast(msg, err) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast show' + (err ? ' err' : '');
  setTimeout(() => t.className = 'toast', 2500);
}

function togglePw(id, btn) {
  const el = document.getElementById(id);
  if (el.type === 'password') { el.type = 'text'; btn.textContent = '隐藏'; }
  else { el.type = 'password'; btn.textContent = '显示'; }
}

async function reloadAll() {
  const r = await fetch('/api/reload', {method: 'POST'});
  const j = await r.json();
  toast(`重载: 新增 ${j.added.length} · 更新 ${j.updated.length} · 删除 ${j.removed.length}` +
        (j.errors.length ? ` (${j.errors.length} 个错误)` : ''), j.errors.length);
  fetchStatus();
}

async function restart(cid) {
  if (!confirm(`确定要重启通道 ${cid} 吗?`)) return;
  const r = await fetch('/api/restart/' + encodeURIComponent(cid), {method: 'POST'});
  const j = await r.json();
  toast(j.ok ? '重启成功' : '重启失败: ' + (j.error || '未知错误'), !j.ok);
  fetchStatus();
}

function openAdd() {
  editingCid = null;
  document.getElementById('modalTitle').textContent = '新增通道';
  const f = document.getElementById('chForm');
  f.reset();
  // 默认值
  document.getElementById('f-udp').value = 'false';
  document.getElementById('f-verbose').value = 'false';
  document.getElementById('f-rtsp_precheck').value = 'true';
  document.getElementById('f-config_file').disabled = false;
  document.getElementById('f-config_file').readOnly = false;
  document.getElementById('f-config_file').required = true;
  document.getElementById('formErr').textContent = '';
  document.getElementById('submitBtn').textContent = '创建';
  document.getElementById('modal').classList.add('show');
  document.getElementById('f-config_file').focus();
}

async function openEdit(cid) {
  const r = await fetch('/api/channels/' + encodeURIComponent(cid));
  if (!r.ok) { toast('加载通道失败: HTTP ' + r.status, true); return; }
  const j = await r.json();
  const cfg = j.config || {};
  editingCid = cid;
  document.getElementById('modalTitle').textContent = '编辑通道 ' + cid;
  const set = (id, v) => { const el = document.getElementById(id); el.value = v ?? ''; };
  set('f-config_file', j.config_file);
  set('f-channel_id', cfg.channel_id);
  set('f-server_ip', cfg.server_ip);
  set('f-server_port', cfg.server_port);
  set('f-server_id', cfg.server_id);
  set('f-domain', cfg.domain);
  set('f-agent_id', cfg.agent_id);
  set('f-agent_password', cfg.agent_password);
  set('f-source', cfg.source);
  set('f-udp', String(cfg.udp));
  set('f-local_ip', cfg.local_ip);
  set('f-manufacturer', cfg.manufacturer);
  set('f-devicename', cfg.devicename);
  set('f-verbose', String(cfg.verbose));
  set('f-reconnect_interval', cfg.reconnect_interval);
  set('f-max_reconnect_attempts', cfg.max_reconnect_attempts);
  set('f-connection_timeout', cfg.connection_timeout);
  set('f-rtsp_precheck', String(cfg.rtsp_precheck));
  set('f-rtsp_precheck_timeout', cfg.rtsp_precheck_timeout);
  // 编辑模式下配置文件名只读 (不能就地改名)
  const cf = document.getElementById('f-config_file');
  cf.readOnly = true;
  cf.required = false;
  document.getElementById('formErr').textContent = '';
  document.getElementById('submitBtn').textContent = '保存';
  document.getElementById('modal').classList.add('show');
}

function closeModal() {
  document.getElementById('modal').classList.remove('show');
  editingCid = null;
}

async function submitForm(ev) {
  ev.preventDefault();
  const f = document.getElementById('chForm');
  const body = new URLSearchParams(new FormData(f));
  document.getElementById('formErr').textContent = '';
  document.getElementById('submitBtn').disabled = true;
  try {
    let url, method;
    if (editingCid) { url = '/api/channels/' + encodeURIComponent(editingCid); method = 'PUT'; }
    else { url = '/api/channels'; method = 'POST'; }
    const r = await fetch(url, { method, body });
    const j = await r.json();
    if (!r.ok || !j.ok) {
      document.getElementById('formErr').textContent = j.error || ('HTTP ' + r.status);
      return;
    }
    toast(editingCid ? '通道已更新' : '通道已创建');
    closeModal();
    fetchStatus();
  } catch (e) {
    document.getElementById('formErr').textContent = String(e);
  } finally {
    document.getElementById('submitBtn').disabled = false;
  }
}

async function del(cid, name) {
  if (!confirm(`确定要删除通道 ${name} (${cid}) 吗?\n\n这将停止推流进程并删除 config/${name}.json 文件,操作不可撤销!`)) return;
  const r = await fetch('/api/channels/' + encodeURIComponent(cid), {method: 'DELETE'});
  const j = await r.json();
  if (!r.ok || !j.ok) { toast('删除失败: ' + (j.error || r.status), true); return; }
  toast('已删除 ' + name);
  fetchStatus();
}

fetchStatus();
setInterval(fetchStatus, 5000);
</script>
</body>
</html>
"""


class _WebHandler(http.server.BaseHTTPRequestHandler):
    """Minimal JSON API + embedded HTML for the admin UI."""

    manager: "MultiPusherManager" = None  # injected before serve_forever()

    # Silence the default per-request access log — we already log to LOGGER.
    def log_message(self, format, *args):  # noqa: A002
        return

    def _send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    # ------------------------------------------------------------------
    # Form / JSON body helpers
    # ------------------------------------------------------------------
    def _read_form(self) -> Dict[str, str]:
        """Decode an ``application/x-www-form-urlencoded`` body into a flat dict.

        Repeated keys are collapsed to the last value, which is fine for the
        CRUD form where every field appears once.  Empty / missing body
        returns an empty dict.
        """
        length = int(self.headers.get("Content-Length", 0) or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            parsed = parse_qs(raw.decode("utf-8"), keep_blank_values=True)
        except UnicodeDecodeError:
            return {}
        # Collapse: last value wins for each key.
        return {k: v[-1] for k, v in parsed.items()}

    # ------------------------------------------------------------------
    # Route dispatch
    # ------------------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html"):
            self._send_html(_WEB_INDEX_HTML)
            return
        if path == "/api/status":
            try:
                self._send_json(self.manager.get_status())
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/api/channels":
            try:
                # Reuse get_status() output but enrich with config_file for the UI.
                items = []
                for s in self.manager.get_status():
                    cid = s["channel_id"]
                    info = self.manager.get_channel(cid)
                    s["config_file"] = info["config_file"] if info else None
                    s["has_config"] = info is not None
                    items.append(s)
                self._send_json({"channels": items})
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path.startswith("/api/channels/"):
            cid = path[len("/api/channels/"):]
            if not cid:
                self._send_json({"ok": False, "error": "missing channel_id"}, 400)
                return
            try:
                info = self.manager.get_channel(cid)
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, 500)
                return
            if info is None:
                self._send_json({"ok": False, "error": "channel_id not found"}, 404)
                return
            self._send_json({"ok": True, **info})
            return
        self.send_error(404, "Not Found")

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path == "/api/reload":
            try:
                summary = self.manager.reload()
                self._send_json(summary)
            except Exception as e:
                self._send_json({"added": [], "updated": [], "removed": [],
                                 "errors": [str(e)]}, 500)
            return
        if path.startswith("/api/restart/"):
            cid = path[len("/api/restart/"):]
            if not cid:
                self._send_json({"ok": False, "error": "missing channel_id"}, 400)
                return
            try:
                ok = self.manager.restart_instance(cid)
                if ok:
                    self._send_json({"ok": True})
                else:
                    self._send_json({"ok": False, "error": "channel_id not found"}, 404)
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, 500)
            return
        if path == "/api/channels":
            self._handle_create_channel()
            return
        self.send_error(404, "Not Found")

    def do_PUT(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if not path.startswith("/api/channels/"):
            self.send_error(404, "Not Found")
            return
        cid = path[len("/api/channels/"):]
        if not cid:
            self._send_json({"ok": False, "error": "missing channel_id"}, 400)
            return
        self._handle_update_channel(cid)

    def do_DELETE(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if not path.startswith("/api/channels/"):
            self.send_error(404, "Not Found")
            return
        cid = path[len("/api/channels/"):]
        if not cid:
            self._send_json({"ok": False, "error": "missing channel_id"}, 400)
            return
        try:
            result = self.manager.delete_channel(cid)
            self._send_json({"ok": True, **result})
        except KeyError as e:
            self._send_json({"ok": False, "error": str(e)}, 404)
        except Exception as e:
            self._send_json({"ok": False, "error": str(e)}, 500)

    # ------------------------------------------------------------------
    # POST /api/channels — create new channel
    # ------------------------------------------------------------------
    def _handle_create_channel(self) -> None:
        form = self._read_form()
        config_file = (form.get("config_file") or "").strip()
        if not config_file:
            self._send_json(
                {"ok": False, "error": "Missing config_file"}, 400)
            return
        try:
            config = _build_config_from_form(form)
        except ValueError as e:
            self._send_json({"ok": False, "error": str(e)}, 400)
            return
        try:
            result = self.manager.add_channel(config, config_file)
        except FileExistsError as e:
            self._send_json({"ok": False, "error": str(e)}, 409)
        except ValueError as e:
            self._send_json({"ok": False, "error": str(e)}, 400)
        except Exception as e:
            self._send_json({"ok": False, "error": str(e)}, 500)
        else:
            self._send_json({"ok": True, **result})

    # ------------------------------------------------------------------
    # PUT /api/channels/<cid> — full-replace an existing channel
    # ------------------------------------------------------------------
    def _handle_update_channel(self, channel_id: str) -> None:
        form = self._read_form()
        # Drop config_file if present — cannot be changed on update.
        form.pop("config_file", None)
        try:
            config = _build_config_from_form(form)
        except ValueError as e:
            self._send_json({"ok": False, "error": str(e)}, 400)
            return
        try:
            result = self.manager.update_channel(channel_id, config)
        except KeyError as e:
            self._send_json({"ok": False, "error": str(e)}, 404)
        except ValueError as e:
            self._send_json({"ok": False, "error": str(e)}, 400)
        except Exception as e:
            self._send_json({"ok": False, "error": str(e)}, 500)
        else:
            self._send_json({"ok": True, **result})


class WebAdminServer:
    """Thin wrapper around ``http.server.ThreadingHTTPServer``."""

    def __init__(self, manager: "MultiPusherManager", host: str, port: int):
        self.manager = manager
        self.host = host
        self.port = port
        self._server: Optional[http.server.ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        # Inject the manager reference into the handler class *before* serving.
        _WebHandler.manager = self.manager
        self._server = http.server.ThreadingHTTPServer((self.host, self.port), _WebHandler)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="WebAdmin",
            daemon=True,
        )
        self._thread.start()
        LOGGER.info("Web admin UI available at http://%s:%d/", self.host, self.port)

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=3)


###############################################################################
# ──────────────────────────────── CLI convenience ──────────────────────────────── #
###############################################################################

def main(config_dir: str = "config",
         web_host: str = "127.0.0.1",
         web_port: int = 8080,
         disable_web: bool = False) -> None:
    """Main function - load all configs and run multiple pushers."""
    # Set up SIGHUP handler for config hot-reload (POSIX only).
    reload_requested = threading.Event()

    def _sighup_handler(signum, frame):
        LOGGER.info("SIGHUP received — scheduling config reload")
        reload_requested.set()

    try:
        signal.signal(signal.SIGHUP, _sighup_handler)
    except (AttributeError, ValueError):
        # signal.SIGHUP doesn't exist on Windows, or we're not in the main thread.
        LOGGER.warning("SIGHUP not available on this platform — hot reload via SIGHUP disabled")

    try:
        # 加载所有配置文件
        configs, load_errors = load_all_configs(config_dir)

        if not configs:
            LOGGER.error("No valid configuration files found")
            return

        for err in load_errors:
            LOGGER.warning("Skipped invalid config: %s", err)

        LOGGER.info(f"Found {len(configs)} configuration files")

        # 创建多实例管理器
        manager = MultiPusherManager(configs, config_dir=config_dir)

        # 启动 Web 管理界面（如未禁用）
        web: Optional[WebAdminServer] = None
        if not disable_web:
            web = WebAdminServer(manager, host=web_host, port=web_port)
            web.start()

        # 启动所有推流实例
        manager.start_all()

        # 主循环：等待 + 处理 SIGHUP 重载 + 优雅退出
        try:
            while True:
                # Periodic reload check (in case SIGHUP wakes us up)
                if reload_requested.is_set():
                    reload_requested.clear()
                    summary = manager.reload()
                    LOGGER.info("Hot-reload summary: +%d ~%d -%d (errors=%d)",
                                len(summary["added"]), len(summary["updated"]),
                                len(summary["removed"]), len(summary["errors"]))
                # Block on a short sleep so we can be interrupted promptly.
                # Using Event.wait makes it responsive to SIGHUP without polling.
                reload_requested.wait(timeout=1.0)
        except KeyboardInterrupt:
            LOGGER.info("Received interrupt signal, shutting down all pushers...")
        finally:
            if web is not None:
                web.stop()
            manager.shutdown_all()

    except Exception as e:
        LOGGER.error(f"Error: {e}")
        raise


if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        format="[%(asctime)s] [%(name)s] %(levelname)s — %(message)s",
        datefmt="%H:%M:%S",
        level=logging.INFO,
    )

    # 命令行参数解析
    ap = argparse.ArgumentParser(description="多路GB28181推流器 - 从config目录读取配置文件")
    ap.add_argument(
        "--config-dir",
        default="config",
        help="配置文件目录路径 (默认: ./config)"
    )
    ap.add_argument(
        "--web-host",
        default="0.0.0.0",
        help="Web 管理界面绑定地址 (默认: 127.0.0.1，设为 0.0.0.0 允许外部访问)"
    )
    ap.add_argument(
        "--web-port",
        type=int,
        default=8080,
        help="Web 管理界面端口 (默认: 8080)"
    )
    ap.add_argument(
        "--disable-web",
        action="store_true",
        help="禁用 Web 管理界面"
    )
    args = ap.parse_args()

    main(args.config_dir, args.web_host, args.web_port, args.disable_web)