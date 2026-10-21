#!/usr/bin/env python3
"""Monitor for the tree-mem backfill cron: emits a deterministic state line.
Output identical between ticks = cron skips the agent run (no noise).

Liveness (fixed 2026-10-21): the daemon is judged by the ARTIFACT it writes,
not by a process count. On 2026-10-08 the serve daemon was launched as a child
of a tool kernel and died with it; the old monitor kept reporting "serve is
still alive" for 13 days because it read a stale/irrelevant process signal.
Now: a serve process must exist AND the tree log must have grown within
STALE_MINUTES. Both are printed so the cron agent never has to guess.
"""
import glob
import json
import os
import subprocess
import time

S = "<hermes home>/tree-mem/20260826_170355_abad4b7a-v2"
STALE_MINUTES = 8
TOTAL = 5020

leaves = 0
merges = 0
leaf_bytes = 0
newest_tree_mtime = 0.0
for f in glob.glob(f"{S}/tree/*.jsonl"):
    try:
        mt = os.path.getmtime(f)
        if mt > newest_tree_mtime:
            newest_tree_mtime = mt
    except OSError:
        pass
    for line in open(f):
        try:
            d = json.loads(line)
        except Exception:
            continue
        if d["l"] == 0:
            leaves += 1
            leaf_bytes += d["size"]
        else:
            merges += 1
counter = 0
div = False
try:
    sg = json.load(open(f"{S}/build_signals.json"))
    counter = sg.get("counter", 0)
    div = sg.get("divergence", False)
except Exception:
    pass
try:
    log_size = os.path.getsize(f"{S}/logs/serve.log")
except OSError:
    log_size = 0
try:
    procs = int(subprocess.run(["pgrep", "-fc", "tree-mem.py serve"],
                                capture_output=True, text=True).stdout.strip() or 0)
except ValueError:
    procs = 0
tree_age_min = (time.time() - newest_tree_mtime) / 60.0 if newest_tree_mtime else 999.0
alive = procs > 0 and tree_age_min <= STALE_MINUTES
remaining = max(TOTAL + 30 - leaves, 0)
done = " DONE" if leaves >= TOTAL + 30 else ""
print(f"leaves={leaves} merges={merges} counter={counter} leaf_bytes={leaf_bytes} "
      f"log_size={log_size} procs={procs} tree_age_min={tree_age_min:.1f} "
      f"alive={alive} remaining~{remaining} divergence={div}{done}")
