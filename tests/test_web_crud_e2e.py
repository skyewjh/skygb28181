"""End-to-end smoke test against the live web server (not unit-test fake).

Boots a real WebAdminServer, makes real HTTP calls, and cleans up.  Used
to verify the wiring is correct after the unit tests pass.
"""
import json
import os
import shutil
import socket
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import gb28181_pusher as g  # noqa


def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close()
    return p


def http(method, url, data=None, expect_json=True):
    body = urllib.parse.urlencode(data).encode() if isinstance(data, dict) else data
    headers = {"Content-Type": "application/x-www-form-urlencoded"} if body else {}
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        r = urllib.request.urlopen(req, timeout=5)
        return r.getcode(), (json.loads(r.read()) if expect_json else r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def make_manager(config_dir):
    # Write a valid config for "camera1" pointing at a fake server.
    fname = "camera1.json"
    config = {
        "server_ip": "192.0.2.1", "server_port": 5060,
        "server_id": "11000000000000000000", "domain": "1100000000",
        "agent_id": "300000000010000000001", "agent_password": "secret",
        "channel_id": "340000000000000000001",
        "source": "rtsp://example/stream", "udp": False, "verbose": False,
        "manufacturer": "StrawberryInno", "devicename": "Superdock",
        "_config_file": fname,
    }
    with open(os.path.join(config_dir, fname), "w", encoding="utf-8") as f:
        json.dump(config, f)
    mgr = g.MultiPusherManager([config], config_dir=config_dir)
    # Replace _start_one with a no-op stub so no real pusher runs, then
    # manually seed the index the way start_all() would.
    def no_start(cfg, name):
        class P:
            channel_id = cfg.get("channel_id", "")
            instance_name = name
            server_ip = cfg.get("server_ip", "")
            server_id = cfg.get("server_id", "")
            agent_id = cfg.get("agent_id", "")
            source = cfg.get("source", "")
            _connected = False
            def _shutdown(self): pass
        t = threading.Thread(target=lambda: time.sleep(60), daemon=True)
        t.start()
        return P(), t
    mgr._start_one = no_start
    # Seed: simulate start_all() so /api/channels has something to show.
    pusher, thread = no_start(config, fname[:-len(".json")])
    mgr.pushers.append(pusher)
    mgr.threads.append(thread)
    mgr._index["340000000000000000001"] = (pusher, thread, mgr._config_fingerprint(config))
    return mgr, fname


def main():
    tmp = tempfile.mkdtemp(prefix="skygb28181-e2e-")
    port = free_port()
    mgr, fname = make_manager(tmp)
    web = g.WebAdminServer(mgr, "127.0.0.1", port)
    web.start()
    base = f"http://127.0.0.1:{port}"
    print(f"--- e2e server up on {base}, config_dir={tmp} ---")

    # 1) GET /api/channels
    code, j = http("GET", f"{base}/api/channels")
    print(f"[1] GET /api/channels -> {code}, {len(j['channels'])} channels")
    assert code == 200 and len(j["channels"]) == 1

    # 2) GET /api/channels/<cid>
    cid = "340000000000000000001"
    code, j = http("GET", f"{base}/api/channels/{cid}")
    print(f"[2] GET /api/channels/{cid} -> {code}, config_file={j['config_file']}")
    assert code == 200 and j["config_file"] == fname

    # 3) POST /api/channels — create camera_lobby.json
    new = {
        "config_file": "camera_lobby.json",
        "server_ip": "192.0.2.2", "server_port": "5060",
        "server_id": "11000000000000000000", "domain": "1100000000",
        "agent_id": "300000000020000000002", "agent_password": "abc",
        "channel_id": "340000000000000000002",
        "source": "rtsp://lobby/stream", "udp": "false",
    }
    code, j = http("POST", f"{base}/api/channels", new)
    print(f"[3] POST /api/channels -> {code}, {j}")
    assert code == 200 and j["ok"] and j["started"]
    assert os.path.exists(os.path.join(tmp, "camera_lobby.json"))

    # 4) POST duplicate -> 409
    code, j = http("POST", f"{base}/api/channels", new)
    print(f"[4] POST duplicate -> {code}, {j}")
    assert code == 409

    # 5) POST path traversal -> 400
    bad = dict(new); bad["config_file"] = "../escape.json"
    bad["channel_id"] = "340000000000000000099"
    code, j = http("POST", f"{base}/api/channels", bad)
    print(f"[5] POST ../escape -> {code}, {j}")
    assert code == 400

    # 6) PUT update existing
    upd = dict(new); upd["server_ip"] = "192.0.2.99"; del upd["config_file"]
    code, j = http("PUT", f"{base}/api/channels/{cid}", upd)
    print(f"[6] PUT update {cid} -> {code}, {j}")
    assert code == 200 and j["ok"] and j["updated"]
    with open(os.path.join(tmp, fname)) as f:
        assert json.load(f)["server_ip"] == "192.0.2.99"

    # 7) DELETE the new lobby channel
    code, j = http("DELETE", f"{base}/api/channels/340000000000000000002")
    print(f"[7] DELETE lobby -> {code}, {j}")
    assert code == 200 and j["ok"] and j["deleted"]
    assert not os.path.exists(os.path.join(tmp, "camera_lobby.json"))

    # 8) HTML page renders
    code, body = http("GET", f"{base}/", expect_json=False)
    print(f"[8] GET / -> {code}, html len={len(body)}")
    assert code == 200 and "新增通道" in body

    print("\n*** ALL E2E ASSERTIONS PASSED ***")

    web.stop()
    shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
