#!/usr/bin/env python3
"""tree-mem watchdog — keeps the V1 tree-memory MCP server (127.0.0.1:4310) alive.

Runs as a Hermes cron job in no-agent mode: stdout is delivered verbatim, and
EMPTY stdout sends nothing (the watchdog pattern). So this script is silent while
healthy and prints one line only when it had to (re)start the server.

Why it is container-local: config.yaml points `tree-mem` at http://127.0.0.1:4310/mcp,
and 127.0.0.1 from inside the hermes container is the container's own loopback. So the
server must live in this namespace — it dies with every container restart, and this
watchdog brings it back on the next tick. It also covers plain crashes.

Liveness is a real MCP `initialize` POST, not a bare TCP connect: a wedged uvicorn
still accepts sockets. Any 2xx counts as up.

Args are fixed here on purpose (paper trail). DATA_DIR points at the LIVE
tree-mem store and the flag is --root: the server was rewritten to read the
live store, hot-reload it, and cap the view at 128 KB.
"""

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

PYTHON = "/opt/hermes/.venv/bin/python3"
SERVER = "<hermes home>/scripts/tree-mem-mcp-server.py"
DATA_DIR = "<hermes home>/tree-mem/20260826_170355_abad4b7a-v2"
HOST, PORT = "127.0.0.1", 4310
LOG = "<hermes home>/logs/tree-mem-server.log"
STATE = "<hermes home>/tree-mem/watchdog_state.json"
LOCK = "<hermes home>/tree-mem/watchdog.lock"
READY_TIMEOUT = 25.0        # seconds to wait for a freshly started server
FAILURE_BACKOFF = 600.0     # after this many consecutive failures, retry at most this often
FAILURE_THRESHOLD = 3

PROBE_BODY = json.dumps({
    "jsonrpc": "2.0", "id": 1, "method": "initialize",
    "params": {"protocolVersion": "2025-06-18", "capabilities": {},
               "clientInfo": {"name": "tree-mem-watchdog", "version": "1.0"}},
}).encode()


def probe(timeout: float = 5.0) -> bool:
    """True when the MCP server answers an initialize handshake."""
    req = urllib.request.Request(
        f"http://{HOST}:{PORT}/mcp", data=PROBE_BODY, method="POST",
        headers={"Content-Type": "application/json",
                 "Accept": "application/json, text/event-stream"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return 200 <= r.status < 300
    except urllib.error.HTTPError as e:
        return 200 <= e.code < 300
    except (urllib.error.URLError, socket.timeout, OSError):
        return False


def load_state() -> dict:
    try:
        with open(STATE) as fh:
            return json.load(fh)
    except Exception:
        return {"failures": 0, "last_attempt": 0.0}


def save_state(state: dict) -> None:
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    tmp = STATE + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(state, fh)
    os.replace(tmp, STATE)


def start_server() -> int | None:
    """Launch detached (new session: survives the cron tick's process group)."""
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    log = open(LOG, "a", buffering=1)
    log.write(f"\n--- watchdog start {time.strftime('%Y-%m-%dT%H:%M:%S%z')} ---\n")
    try:
        p = subprocess.Popen(
            [PYTHON, SERVER, "--root", DATA_DIR, "--port", str(PORT)],
            stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
            start_new_session=True, cwd="/opt/data")
        return p.pid
    except Exception as e:
        log.write(f"watchdog: failed to launch: {e}\n")
        return None


def main() -> int:
    if probe():
        save_state({"failures": 0, "last_attempt": 0.0})
        return 0                                    # healthy: say nothing

    # Serialise concurrent ticks (belt and braces — the port check is the real lock).
    os.makedirs(os.path.dirname(LOCK), exist_ok=True)
    import fcntl
    with open(LOCK, "w") as lk:
        try:
            fcntl.flock(lk, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return 0                                # another tick is starting it
        if probe():                                 # re-check under the lock
            return 0

        state = load_state()
        now = time.time()
        state["failures"] = state.get("failures", 0) + 1
        if (state["failures"] > FAILURE_THRESHOLD
                and now - state.get("last_attempt", 0.0) < FAILURE_BACKOFF):
            save_state(state)                       # crash-looping: stay quiet, back off
            return 0
        state["last_attempt"] = now
        save_state(state)

        pid = start_server()
        if pid is None:
            print("⚠️ tree-mem MCP is down and could not be started — see "
                  f"{LOG}", flush=True)
            return 0

        deadline = time.time() + READY_TIMEOUT
        while time.time() < deadline:
            time.sleep(1.0)
            if probe(timeout=2.0):
                state["failures"] = 0
                state["last_attempt"] = 0.0
                save_state(state)
                print(f"🔧 tree-mem MCP was down — restarted (pid {pid}), handshake OK "
                      f"on {HOST}:{PORT}. The toolset picks it up on the next session; "
                      f"run `hermes mcp test tree-mem` to confirm.", flush=True)
                return 0

    print(f"⚠️ tree-mem MCP did not come up within {int(READY_TIMEOUT)}s of start "
          f"(pid {pid}) — see {LOG}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
