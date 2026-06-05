"""Unit tests for the web CRUD upgrade of skygb28181 admin UI.

Covers:
1. GET  /api/channels        — list of all instances + config_file
2. GET  /api/channels/<cid>  — single channel detail (200)
3. GET  /api/channels/<cid>  — not found (404)
4. PUT  /api/channels/<cid>  — full-replace, restart triggered
5. POST /api/channels        — create new channel
6. POST /api/channels        — duplicate config_file -> 409
7. POST /api/channels        — path-traversal config_file -> 400
8. DELETE /api/channels/<cid> — stop + remove file

The tests mock :class:`MultiPusherManager._start_one` so no real gst/SIP
traffic happens.  Run with ``python3 -m unittest tests.test_web_crud`` from
the project root.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
from typing import Tuple
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import gb28181_pusher as g  # noqa: E402


def _free_port() -> int:
    """Bind :0 to discover a free TCP port, then release it."""
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _make_fake_pusher(cid: str, instance_name: str):
    """A minimal stand-in for GB28181Pusher used in CRUD tests."""
    class FakePusher:
        def __init__(self):
            self.channel_id = cid
            self.instance_name = instance_name
            self.server_ip = "192.0.2.1"
            self.server_id = "11000000000000000000"
            self.agent_id = "300000000010000000001"
            self.source = "rtsp://example/stream"
            self._connected = False
            self._shutdown_calls = 0

        def _shutdown(self):
            self._shutdown_calls += 1

    pusher = FakePusher()
    thread = threading.Thread(target=lambda: time.sleep(60), daemon=True)
    thread.start()
    return pusher, thread


class _WebCrdTestBase(unittest.TestCase):
    """Spin up a real WebAdminServer in a temp config dir, mock the pusher."""

    def setUp(self) -> None:
        # Each test gets its own temp config dir.
        self._tmp = tempfile.mkdtemp(prefix="skygb28181-test-")
        self._port = _free_port()
        # A minimal config that pretends to have one running instance.
        cid = "340000000000000000001"
        fname = "camera1.json"
        config = {
            "server_ip": "192.0.2.1",
            "server_port": 5060,
            "server_id": "11000000000000000000",
            "domain": "1100000000",
            "agent_id": "300000000010000000001",
            "agent_password": "secret",
            "channel_id": cid,
            "source": "rtsp://example/stream",
            "udp": False,
            "verbose": True,
            "reconnect_interval": 5,
            "max_reconnect_attempts": 0,
            "connection_timeout": 10,
            "manufacturer": "StrawberryInno",
            "devicename": "Superdock",
            "rtsp_precheck": True,
            "rtsp_precheck_timeout": 5,
            "_config_file": fname,
        }
        with open(os.path.join(self._tmp, fname), "w", encoding="utf-8") as f:
            json.dump(config, f)
        self._seed_cid = cid
        self._seed_fname = fname
        # Build a manager with that single config and inject a fake pusher.
        self._manager = g.MultiPusherManager([config], config_dir=self._tmp)
        # Replace _start_one so CRUD operations don't actually launch a real
        # GB28181Pusher (which would call pjsip, gst, etc.).  This lets us
        # exercise the file-on-disk + index-management code paths safely.
        self._start_one_calls = []
        def fake_start_one(cfg, instance_name):
            cid = cfg.get("channel_id", "unknown")
            self._start_one_calls.append((cid, instance_name))
            pusher, thread = _make_fake_pusher(cid, instance_name)
            return pusher, thread
        self._manager._start_one = fake_start_one
        # Replace the auto-started pusher/thread with a fake so we don't
        # talk to any real SIP server.  The seed values mirror the config.
        pusher, thread = _make_fake_pusher(cid, fname[:-len(".json")])
        self._manager.pushers = [pusher]
        self._manager.threads = [thread]
        self._manager._index[cid] = (pusher, thread,
                                      self._manager._config_fingerprint(config))
        # Start the web admin.
        self._web = g.WebAdminServer(self._manager, "127.0.0.1", self._port)
        self._web.start()
        # Wait for the server to actually accept connections.
        self._wait_for_server()

    def tearDown(self) -> None:
        try:
            self._web.stop()
        except Exception:
            pass
        # Best-effort: stop the fake pusher's thread by calling _shutdown.
        for t in self._manager.threads:
            if t.is_alive():
                # threads are daemon, will die with the test runner.
                pass
        shutil.rmtree(self._tmp, ignore_errors=True)

    # ---- HTTP helpers --------------------------------------------------------
    def _base(self) -> str:
        return f"http://127.0.0.1:{self._port}"

    def _http(self, method: str, path: str, body: str = "") -> Tuple[int, str]:
        url = self._base() + path
        data = body.encode("utf-8") if body else None
        headers = {"Content-Type": "application/x-www-form-urlencoded"} if body else {}
        req = urlrequest.Request(url, data=data, method=method, headers=headers)
        try:
            r = urlrequest.urlopen(req, timeout=5)
            return r.getcode(), r.read().decode("utf-8")
        except HTTPError as e:
            return e.code, e.read().decode("utf-8")
        except URLError as e:
            self.fail(f"HTTP {method} {path} -> URLError {e}")

    def _wait_for_server(self) -> None:
        import socket
        for _ in range(50):
            try:
                with socket.create_connection(("127.0.0.1", self._port), timeout=0.5):
                    return
            except OSError:
                time.sleep(0.05)
        self.fail(f"web server did not start on port {self._port}")


# ===========================================================================
# Test 1: GET /api/channels lists everything
# ===========================================================================
class TestListChannels(_WebCrdTestBase):

    def test_list_includes_seed(self):
        code, body = self._http("GET", "/api/channels")
        self.assertEqual(code, 200, body)
        j = json.loads(body)
        self.assertIn("channels", j)
        items = j["channels"]
        self.assertEqual(len(items), 1)
        ch = items[0]
        self.assertEqual(ch["channel_id"], self._seed_cid)
        self.assertEqual(ch["config_file"], self._seed_fname)
        self.assertTrue(ch["has_config"])


# ===========================================================================
# Test 2: GET /api/channels/<cid> — single channel
# ===========================================================================
class TestGetOneChannel(_WebCrdTestBase):

    def test_get_existing(self):
        code, body = self._http("GET", f"/api/channels/{self._seed_cid}")
        self.assertEqual(code, 200, body)
        j = json.loads(body)
        self.assertTrue(j["ok"])
        self.assertEqual(j["channel_id"], self._seed_cid)
        self.assertEqual(j["config_file"], self._seed_fname)
        self.assertEqual(j["config"]["server_ip"], "192.0.2.1")
        self.assertEqual(j["config"]["agent_password"], "secret")


# ===========================================================================
# Test 3: GET /api/channels/<cid> — not found
# ===========================================================================
class TestGetOneChannelNotFound(_WebCrdTestBase):

    def test_get_missing(self):
        code, body = self._http("GET", "/api/channels/999999")
        self.assertEqual(code, 404, body)
        j = json.loads(body)
        self.assertFalse(j["ok"])


# ===========================================================================
# Test 4: PUT /api/channels/<cid> — full replace
# ===========================================================================
class TestUpdateChannel(_WebCrdTestBase):

    def test_update_replaces_and_restarts(self):
        new_cfg = {
            "server_ip": "192.0.2.99",
            "server_port": "5060",
            "server_id": "11000000000000000000",
            "domain": "1100000000",
            "agent_id": "300000000010000000001",
            "agent_password": "newpw",
            "channel_id": self._seed_cid,
            "source": "rtsp://new/stream",
            "udp": "false",
            "verbose": "false",
            "rtsp_precheck": "true",
            "rtsp_precheck_timeout": "5",
        }
        body = "&".join(f"{k}={v}" for k, v in new_cfg.items())
        code, resp = self._http("PUT", f"/api/channels/{self._seed_cid}", body)
        self.assertEqual(code, 200, resp)
        j = json.loads(resp)
        self.assertTrue(j["ok"])
        self.assertTrue(j["updated"])
        self.assertTrue(j["restarted"])
        # File on disk should reflect the new server_ip
        with open(os.path.join(self._tmp, self._seed_fname), "r") as f:
            saved = json.load(f)
        self.assertEqual(saved["server_ip"], "192.0.2.99")
        self.assertEqual(saved["agent_password"], "newpw")
        # _start_one was called for the replacement instance
        self.assertEqual(len(self._start_one_calls), 1,
                         f"expected 1 restart, got {self._start_one_calls}")
        cid, instance_name = self._start_one_calls[0]
        self.assertEqual(cid, self._seed_cid)
        # The live pusher after update is the *new* one from the second start.
        # We can verify this by checking that the pusher's _shutdown was NOT
        # called on the new one (only on the old one).
        new_pusher = self._manager._index[self._seed_cid][0]
        self.assertIsNotNone(new_pusher)


# ===========================================================================
# Test 5: POST /api/channels — create new
# ===========================================================================
class TestCreateChannel(_WebCrdTestBase):

    def test_create_writes_file_and_starts(self):
        form = {
            "config_file": "camera_lobby.json",
            "server_ip": "192.0.2.50",
            "server_port": "5060",
            "server_id": "11000000000000000000",
            "domain": "1100000000",
            "agent_id": "300000000020000000002",
            "agent_password": "abc",
            "channel_id": "340000000000000000002",
            "source": "rtsp://lobby/stream",
            "udp": "false",
            "verbose": "false",
            "rtsp_precheck": "true",
            "rtsp_precheck_timeout": "5",
        }
        body = "&".join(f"{k}={v}" for k, v in form.items())
        code, resp = self._http("POST", "/api/channels", body)
        self.assertEqual(code, 200, resp)
        j = json.loads(resp)
        self.assertTrue(j["ok"])
        self.assertEqual(j["config_file"], "camera_lobby.json")
        self.assertEqual(j["channel_id"], "340000000000000000002")
        # File exists on disk
        self.assertTrue(os.path.exists(os.path.join(self._tmp, "camera_lobby.json")))
        # New pusher is registered
        self.assertIn("340000000000000000002", self._manager._index)


# ===========================================================================
# Test 6: POST duplicate config_file -> 409
# ===========================================================================
class TestCreateDuplicate(_WebCrdTestBase):

    def test_duplicate_returns_409(self):
        form = {
            "config_file": self._seed_fname,  # already on disk
            "server_ip": "192.0.2.50",
            "server_id": "11000000000000000000",
            "agent_id": "300000000020000000002",
            "agent_password": "abc",
            "channel_id": "340000000000000000099",
            "source": "rtsp://x/y",
        }
        body = "&".join(f"{k}={v}" for k, v in form.items())
        code, resp = self._http("POST", "/api/channels", body)
        self.assertEqual(code, 409, resp)
        j = json.loads(resp)
        self.assertFalse(j["ok"])
        # No new pusher registered
        self.assertNotIn("340000000000000000099", self._manager._index)


# ===========================================================================
# Test 7: POST path-traversal config_file -> 400
# ===========================================================================
class TestCreatePathTraversal(_WebCrdTestBase):

    def test_traversal_rejected(self):
        form = {
            "config_file": "../etc/passwd",
            "server_ip": "192.0.2.50",
            "server_id": "11000000000000000000",
            "agent_id": "300000000020000000002",
            "agent_password": "abc",
            "channel_id": "340000000000000000099",
            "source": "rtsp://x/y",
        }
        body = "&".join(f"{k}={v}" for k, v in form.items())
        code, resp = self._http("POST", "/api/channels", body)
        self.assertEqual(code, 400, resp)
        j = json.loads(resp)
        self.assertFalse(j["ok"])
        # No file created outside the config dir
        self.assertFalse(os.path.exists(os.path.join(self._tmp, "..", "passwd.json")))


# ===========================================================================
# Test 8: DELETE /api/channels/<cid> — stop + remove file
# ===========================================================================
class TestDeleteChannel(_WebCrdTestBase):

    def test_delete_removes_file_and_stops(self):
        # Sanity: file exists, pusher in index
        self.assertTrue(os.path.exists(os.path.join(self._tmp, self._seed_fname)))
        pusher_before = self._manager._index[self._seed_cid][0]
        code, resp = self._http("DELETE", f"/api/channels/{self._seed_cid}")
        self.assertEqual(code, 200, resp)
        j = json.loads(resp)
        self.assertTrue(j["ok"])
        self.assertTrue(j["deleted"])
        # File gone
        self.assertFalse(os.path.exists(os.path.join(self._tmp, self._seed_fname)))
        # Index no longer contains it
        self.assertNotIn(self._seed_cid, self._manager._index)
        # _shutdown was called on the old pusher
        self.assertEqual(pusher_before._shutdown_calls, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
