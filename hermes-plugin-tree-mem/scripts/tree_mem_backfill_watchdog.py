#!/usr/bin/env python3
"""
tree_mem_backfill_watchdog.py — keeps the Phase-2a tree-mem BACKFILL daemon alive.

Distinct from tree_mem_watchdog.py (that one guards the V1 MCP server on :4310).
This one guards the runtime that ingests state.db → main/ + tree/ jsonl.

Liveness rule (strict, learned on 2026-10-21): the daemon is DEAD unless
(a) a serve process exists AND (b) the serve log grew within WATCH_MINUTES.
Process existence alone is not proof — on 2026-10-08 the daemon was launched as
a child of a tool kernel and died with it; for 13 days the monitor cron reported
"serve is still alive" while nothing was running. The monitor's check was wrong;
this one watches the artifact the work writes.

On death: clear a stale owner.lock, relaunch DETACHED (start_new_session=True, so
it survives this process, the cron tick's process group, and session ends), then
re-verify. Prints one line always (this is not the silent MCP watchdog).

Exit 0 = healthy or successfully relaunched. Exit 1 = relaunch failed.
"""
import glob
import json
import os
import subprocess
from pathlib import Path
import sys
import time

_ROOT = os.environ.get("TREE_MEM_ROOT", "").strip() or os.path.join(
    os.path.expanduser("~"), ".hermes", "tree-mem")
ROOT = os.path.join(_ROOT, SESSION) if SESSION else _ROOT
SESSION = os.environ.get("TREE_MEM_SESSION", "")
WATCH_MINUTES = 5
LOG = f"{ROOT}/logs/serve.log"
PY = os.environ.get("TREE_MEM_PYTHON", sys.executable)
SCRIPT = os.environ.get("TREE_MEM_SCRIPT", "") or str(Path(__file__).resolve().parent / "tree-mem.py")


def leaves_built() -> int:
    n = 0
    for f in glob.glob(f"{ROOT}/tree/*.jsonl"):
        try:
            for line in open(f):
                if json.loads(line).get("l") == 0:
                    n += 1
        except Exception:
            pass
    return n


def process_alive() -> bool:
    out = subprocess.run(["pgrep", "-fc", "tree-mem.py serve"],
                         capture_output=True, text=True).stdout.strip()
    try:
        return int(out) > 0
    except ValueError:
        return False


def log_age_min() -> float:
    try:
        return (time.time() - os.path.getmtime(LOG)) / 60.0
    except OSError:
        return 999.0


def relaunch() -> int | None:
    lock = f"{ROOT}/owner.lock"
    if os.path.exists(lock):
        try:
            os.remove(lock)
        except OSError:
            pass
    with open(f"{ROOT}/logs/stdout.log", "ab") as out:
        p = subprocess.Popen(
            [PY, SCRIPT, "serve", "--session", SESSION, "--root", ROOT,
             "--workers", "8", "--interval", "2.0"],
            cwd="/opt/data", stdout=out, stderr=subprocess.STDOUT,
            start_new_session=True, stdin=subprocess.DEVNULL)
    return p.pid


def main() -> int:
    alive = process_alive()
    age = log_age_min()
    leaves = leaves_built()
    if alive and age <= WATCH_MINUTES:
        print(f"OK serve alive, log_age={age:.1f}m, leaves={leaves}")
        return 0
    reason = "no process" if not alive else f"log stale {age:.1f}m"
    print(f"DEAD ({reason}), leaves={leaves} -> relaunching")
    pid = relaunch()
    time.sleep(12)
    ok = process_alive() and log_age_min() <= WATCH_MINUTES
    print(f"RELAUNCH {'ok' if ok else 'FAILED'} (pid {pid}): "
          f"alive={process_alive()} log_age={log_age_min():.1f}m leaves={leaves_built()}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
