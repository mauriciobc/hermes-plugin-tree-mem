#!/usr/bin/env python3
"""
tree_mem_backfill_watchdog.py — keeps the Phase-2a tree-mem BACKFILL daemon alive.

Distinct from tree_mem_watchdog.py (that one guards the V1 MCP server on :4310).
This one guards the runtime that ingests state.db → main/ + tree/ jsonl.

Liveness rule (strict, learned on 2026-10-21): the daemon is DEAD unless
(a) a serve process exists AND (b) the TREE artifact grew within WATCH_MINUTES.

The artifact is `tree/*.jsonl` — the file the work actually writes. `serve.log`
records only starts, markers and errors, so it goes quiet during healthy
steady-state work; watching it reported DEAD on a daemon that was adding
60+ leaves/minute. The event log is kept for DIAGNOSIS only.

Fixes applied 2026-10-22 (see tree-mem skill -> runtime-operations.md):
 1. Liveness watches `tree/*.jsonl`, not `logs/serve.log`.
 2. Process count matches argv EXACTLY. `pgrep -f "tree-mem.py serve"` also
    matches the querying shell, so the count was inflated by a phantom.
 3. The relaunch does NOT delete a live `owner.lock`. The lock is the V4
    single-writer guard; the old code removed it unconditionally, which
    admitted a SECOND writer to the same store. Two writers = the same
    merge node id written with different content = the tree lies about
    coverage. Clear the lock only when the pid inside it is dead.
 4. The relaunch passes an EXPLICIT --db and a HERMES_HOME-aware env.
    Without HERMES_HOME the runtime resolved its state.db to
    $HOME/.hermes/state.db (which does not exist on that install) and
    died on startup with
    `sqlite3.OperationalError: unable to open database file`.

On death: clear a stale owner.lock, relaunch DETACHED (start_new_session=True,
so it survives this process, the cron tick's process group, and session ends),
then re-verify. Prints one line always (this is not the silent MCP watchdog).

Exit 0 = healthy or successfully relaunched. Exit 1 = relaunch failed.
"""
import glob
import json
import os
import subprocess
import sys
import time

def _hermes_home():
    """Hermes' own home resolver (HERMES_HOME may point anywhere).

    Guessing a path breaks on any install where the home is set to
    something else, so resolve through the framework's own resolver.
    """
    try:
        from hermes_constants import get_hermes_home
        return str(get_hermes_home())
    except Exception:
        return os.environ.get("HERMES_HOME", "").strip() or os.path.join(
            os.path.expanduser("~"), ".hermes")


def _default(*parts):
    return os.path.join(_hermes_home(), *parts)


# Every path is overridable; the defaults resolve through Hermes' own home.
ROOT = os.environ.get("TREE_MEM_STORE", "").strip() or _default(
    "tree-mem", "20260826_170355_abad4b7a-v2")
SESSION = os.environ.get("TREE_MEM_SESSION", "").strip() or "20260826_170355_abad4b7a"
WATCH_MINUTES = int(os.environ.get("TREE_MEM_WATCH_MINUTES", "5"))
LOG = f"{ROOT}/logs/serve.log"
# The runtime lives next to this script inside the plugin; fall back to the
# live copy under the data home.
PY = sys.executable or "/opt/hermes/.venv/bin/python3"
SCRIPT = os.environ.get("TREE_MEM_SCRIPT", "").strip() or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "tree-mem.py")
if not os.path.exists(SCRIPT):
    SCRIPT = _default("scripts", "tree-mem.py")
LOCK = f"{ROOT}/owner.lock"


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
    """True when a real serve daemon exists — matched on argv, not pgrep -f.

    `pgrep -f "tree-mem.py serve"` also matches the shell running this very
    query, so the count was inflated and liveness was asserted by a phantom.
    A serve daemon has argv[1] ending in tree-mem.py and argv[2] == "serve".
    """
    for d in glob.glob("/proc/[0-9]*"):
        try:
            with open(f"{d}/cmdline", "rb") as fh:
                argv = fh.read().split(b"\0")
        except OSError:
            continue
        argv = [a.decode("utf-8", "replace") for a in argv if a]
        if len(argv) >= 3 and argv[1].endswith("tree-mem.py") and argv[2] == "serve":
            return True
    return False


def lock_pid() -> int | None:
    """The pid recorded in owner.lock, if the lock file is readable."""
    try:
        with open(LOCK) as fh:
            return int(fh.read().strip())
    except (OSError, ValueError):
        return None


def pid_alive(pid: int | None) -> bool:
    """True only for a RUNNING process.

    A ZOMBIE still has /proc/<pid> — its parent has not reaped it — so a bare
    directory check says "alive" for a process that is already dead. A zombie
    holds no lock and serves no work, so it must count as DEAD or the stale
    owner.lock is never cleared and every relaunch dies with
    "FATAL: owner.lock exists — another writer owns this store".
    """
    if pid is None:
        return False
    try:
        with open(f"/proc/{pid}/stat") as fh:
            fields = fh.read().rsplit(")", 1)[1].split()
        state = fields[0]
    except (OSError, IndexError):
        return False
    return state != "Z"


def tree_age_min() -> float:
    """Age of the newest tree artifact — the file the work actually writes."""
    newest = 0.0
    for f in glob.glob(f"{ROOT}/tree/*.jsonl"):
        try:
            newest = max(newest, os.path.getmtime(f))
        except OSError:
            pass
    if not newest:
        return 999.0
    return (time.time() - newest) / 60.0


def log_age_min() -> float:
    """Event-log age — DIAGNOSIS ONLY. Goes quiet during healthy work."""
    try:
        return (time.time() - os.path.getmtime(LOG)) / 60.0
    except OSError:
        return 999.0


def state_db() -> str:
    """Resolve the Hermes state.db the way Hermes itself does.

    The runtime's own resolver needs HERMES_HOME in the environment; without
    it the fallback is $HOME/.hermes/state.db, which does not exist on this
    install and made the daemon die on startup. Resolve it here and pass it
    explicitly so the relaunch does not depend on the env it inherits.
    """
    env_home = os.environ.get("HERMES_HOME", "").strip()
    try:
        from hermes_constants import get_hermes_home
        home = str(get_hermes_home())
    except Exception:
        home = env_home or os.path.join(os.path.expanduser("~"), ".hermes")
    db = os.path.join(home, "state.db")
    if not os.path.exists(db) and env_home:
        db = os.path.join(env_home, "state.db")
    return db


def relaunch() -> int | None:
    """Start a detached daemon. Only clears the lock when its pid is dead."""
    pid = lock_pid()
    if pid is not None and not pid_alive(pid):
        try:
            os.remove(LOCK)
        except OSError:
            pass
    db = state_db()
    env = dict(os.environ)
    env["HERMES_HOME"] = os.path.dirname(db)
    with open(f"{ROOT}/logs/stdout.log", "ab") as out:
        p = subprocess.Popen(
            [PY, SCRIPT, "serve", "--session", SESSION, "--root", ROOT,
             "--db", db, "--workers", "8", "--interval", "2.0"],
            cwd="/opt/data", stdout=out, stderr=subprocess.STDOUT, env=env,
            start_new_session=True, stdin=subprocess.DEVNULL)
    return p.pid


def main() -> int:
    alive = process_alive()
    age = tree_age_min()
    leaves = leaves_built()
    if alive and age <= WATCH_MINUTES:
        print(f"OK serve alive, log_age={log_age_min():.1f}m, leaves={leaves}")
        return 0
    reason = "no process" if not alive else f"tree stale {age:.1f}m"
    print(f"DEAD ({reason}), leaves={leaves} -> relaunching")
    pid = relaunch()
    time.sleep(12)
    ok = process_alive() and tree_age_min() <= WATCH_MINUTES
    print(f"RELAUNCH {'ok' if ok else 'FAILED'} (pid {pid}): "
          f"alive={process_alive()} log_age={log_age_min():.1f}m leaves={leaves_built()}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
