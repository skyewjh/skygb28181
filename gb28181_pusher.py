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
os.environ['GST_PLUGIN_PATH'] = '/home/lyra/sbgb28181/gst-gb28181sink/build'

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
        "server_port": 5060,
        "domain": None,
        "udp": False,
        "local_ip": None,
        "verbose": False,
        "reconnect_interval": 5,
        "max_reconnect_attempts": 0,
        "connection_timeout": 10,
        "source": "test",
        "manufacturer": "StrawberryInno",
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
            "User-Agent: sbgb28181",
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
            "User-Agent: sbgb28181",
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
            "User-Agent: sbgb28181",
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
            "User-Agent: sbgb28181",
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
            "User-Agent: sbgb28181",
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


###############################################################################
# ──────────────────────────────── Web Admin UI ──────────────────────────────── #
###############################################################################

_WEB_INDEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>sbgb28181 Admin</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.4 -apple-system, "Segoe UI", system-ui, sans-serif;
         background: #0e1419; color: #d6dde3; }
  header { padding: 14px 20px; background: #131c25; border-bottom: 1px solid #1f2a36;
           display: flex; align-items: center; gap: 16px; }
  header h1 { font-size: 18px; margin: 0; color: #7ed1ff; }
  header .stats { color: #8a99ad; font-size: 13px; }
  header .stats b { color: #d6dde3; }
  .actions { margin-left: auto; display: flex; gap: 8px; }
  button { background: #1d2a38; color: #d6dde3; border: 1px solid #2a3a4d;
           padding: 6px 12px; border-radius: 4px; cursor: pointer; font-size: 13px; }
  button:hover { background: #243446; }
  button.primary { background: #1f4f7a; border-color: #2974ad; }
  button.primary:hover { background: #255d8e; }
  button.danger { background: #5a1f1f; border-color: #8a2929; }
  button.danger:hover { background: #6b2424; }
  main { padding: 16px 20px; }
  .empty { text-align: center; padding: 60px; color: #6a7886; }
  .grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fill, minmax(340px, 1fr)); }
  .card { background: #131c25; border: 1px solid #1f2a36; border-radius: 6px;
          padding: 12px 14px; }
  .card .row { display: flex; justify-content: space-between; align-items: center;
               margin-bottom: 6px; }
  .card .name { font-weight: 600; color: #e0e6ed; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 10px;
           font-size: 11px; font-weight: 600; text-transform: uppercase; }
  .badge.ok { background: #0f3d24; color: #6ee79c; }
  .badge.warn { background: #4a3a0e; color: #f0c060; }
  .badge.err { background: #4a1010; color: #f08080; }
  .meta { color: #8a99ad; font-size: 12px; }
  .meta div { margin: 2px 0; word-break: break-all; }
  .meta b { color: #aab4c1; font-weight: 500; }
  .card .row.actions-row { margin-top: 10px; padding-top: 10px; border-top: 1px solid #1f2a36; }
  .toast { position: fixed; bottom: 16px; right: 16px; background: #1d2a38;
           border: 1px solid #2a3a4d; padding: 10px 16px; border-radius: 4px;
           opacity: 0; transition: opacity .3s; }
  .toast.show { opacity: 1; }
  .toast.err { border-color: #8a2929; }
  code { background: #0a0f15; padding: 1px 5px; border-radius: 3px; font-size: 12px; }
</style>
</head>
<body>
<header>
  <h1>sbgb28181 Admin</h1>
  <div class="stats" id="stats">loading…</div>
  <div class="actions">
    <button class="primary" onclick="reloadAll()">↻ Reload Config</button>
  </div>
</header>
<main>
  <div id="container"><div class="empty">Loading…</div></div>
</main>
<div class="toast" id="toast"></div>
<script>
let instances = [];

async function fetchStatus() {
  const r = await fetch('/api/status');
  instances = await r.json();
  render();
}

function render() {
  const total = instances.length;
  const ok = instances.filter(i => i.registered && i.thread_alive).length;
  const bad = total - ok;
  document.getElementById('stats').innerHTML =
    `<b>${total}</b> instances · <b style="color:#6ee79c">${ok}</b> registered · ` +
    `<b style="color:${bad ? '#f08080' : '#8a99ad'}">${bad}</b> unhealthy · ` +
    `auto-refresh 5s`;

  const c = document.getElementById('container');
  if (!total) { c.innerHTML = '<div class="empty">No instances running.</div>'; return; }
  c.className = 'grid';
  c.innerHTML = instances.map(i => {
    const state = !i.thread_alive ? 'err'
                : i.registered ? 'ok' : 'warn';
    const stateText = !i.thread_alive ? 'dead'
                    : i.registered ? 'registered' : 'unregistered';
    return `
      <div class="card">
        <div class="row">
          <div class="name">${esc(i.instance_name)}</div>
          <span class="badge ${state}">${stateText}</span>
        </div>
        <div class="meta">
          <div><b>channel_id</b> <code>${esc(i.channel_id)}</code></div>
          <div><b>server</b> ${esc(i.server_ip)} (id ${esc(i.server_id)})</div>
          <div><b>agent_id</b> <code>${esc(i.agent_id)}</code></div>
          <div><b>source</b> <code>${esc(i.source)}</code></div>
          <div><b>thread</b> ${i.thread_alive ? 'alive' : 'dead'}</div>
        </div>
        <div class="row actions-row">
          <button onclick="restart('${esc(i.channel_id)}')">↻ Restart</button>
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

async function reloadAll() {
  const r = await fetch('/api/reload', {method: 'POST'});
  const j = await r.json();
  toast(`Reload: +${j.added.length} ~${j.updated.length} -${j.removed.length}` +
        (j.errors.length ? ` (${j.errors.length} errors)` : ''), j.errors.length);
  fetchStatus();
}

async function restart(channelId) {
  if (!confirm(`Restart instance ${channelId}?`)) return;
  const r = await fetch('/api/restart/' + encodeURIComponent(channelId), {method: 'POST'});
  const j = await r.json();
  toast(j.ok ? 'Restarted' : 'Failed: ' + (j.error || 'unknown'), !j.ok);
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

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html"):
            self._send_html(_WEB_INDEX_HTML)
        elif path == "/api/status":
            try:
                self._send_json(self.manager.get_status())
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
        else:
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
        elif path.startswith("/api/restart/"):
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
        else:
            self.send_error(404, "Not Found")


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
        default="127.0.0.1",
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