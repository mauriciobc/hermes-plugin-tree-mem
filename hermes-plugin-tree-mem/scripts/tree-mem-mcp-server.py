#!/usr/bin/env python3
"""
tree-mem-mcp-server.py — the tree-memory tool surface for Hermes.

StreamableHTTP MCP server over the LIVE tree-mem store (Phase 2a runtime):
  tree_view()            → the graded view, id+n|line format
  tree_zoom(id, n)       → open line id+n into its two children (n=1: whole message)
  tree_date(id)          → date of message id

WHY THIS EXISTS (next to the plugin)
    The `tree-mem` plugin implements the MemoryProvider contract and is the
    eventual home of tree memory — but it is deliberately INACTIVE until the
    KPI gate passes (that flip is Phase 2b live injection). Until then the
    tree must still be reachable, and the honest way to reach it is as TOOLS:
    a new session can call tree_view/tree_zoom/tree_date on demand without
    anything being injected into its context.

    The proposal's rule is the same one this implements: "zoom is the only
    allowed mechanism to navigate the tree" — and "the latest word in the view
    is the truth".

STORE LAYOUT (written by scripts/tree-mem.py)
    <root>/main/YYYY-MM-DD.jsonl   units:   {i, kind, text, size, date}
    <root>/tree/YYYY-MM-DD.jsonl   nodes:   {l, i, text, size}
    <root>/view.json               view:    [[l, i], ...]
    <root>/build_signals.json      counter: newest ingested message id

HOT RELOAD
    The store is being built in the background, so the view changes under us.
    The server re-reads when the underlying files change (mtime check), so a
    long-lived server always serves the current tree. No restart needed.

UN-SUMMARIZED TAIL
    The proposal (§5): "A message not summarized yet shows as
    '(not summarized yet: zoom it)'." The view must not silently omit the
    tail — the model has to be able to tell "never happened" from "not
    compressed yet", and zoom must reach it.

Run:
  python3 tree-mem-mcp-server.py --root <hermes home>/tree-mem/<session> --port 4310
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime

try:
    from mcp.server.mcpserver import MCPServer   # mcp >= 2.0
except ImportError:
    try:
        from mcp.server.fastmcp import FastMCP as MCPServer   # mcp 1.x
    except ImportError:
        sys.exit("mcp package missing")

# view: [(l, i)] oldest first
STATE = {"units": {}, "nodes": {}, "view": [], "counter": 0, "offsets": {}}
STAMP = {"key": None, "at": 0.0}
ROOT = ""            # the live store dir; set from argv at startup

RELOAD_EVERY = 15.0       # seconds between store checks (the store is append-only)
PLACEHOLDER = "(not summarized yet: zoom it)"

# The view is DESIGNED to be 64-128 KB (proposal §3). While the merge
# scheduler is behind, the raw view can be >1 MB. Serving that whole would
# blow the caller's context — so cap it the same way the plugin does: keep
# the NEWEST lines (recent ones are fine-grained, old ones coarse) and say
# plainly that the view is truncated.
VIEW_HI = 128_000
VIEW_LO = 64_000


def _store_key(root):
    """A cheap fingerprint of the store's files, to detect changes.

    Only mtime + size, and only for the files that matter. The JSONL files
    are append-only, so their SIZE alone tells us they grew.
    """
    parts = []
    for sub in ("main", "tree"):
        d = os.path.join(root, sub)
        if not os.path.isdir(d):
            continue
        for fn in sorted(os.listdir(d)):
            p = os.path.join(d, fn)
            try:
                st = os.stat(p)
                parts.append(f"{sub}/{fn}:{st.st_mtime_ns}:{st.st_size}")
            except OSError:
                pass
    for extra in ("view.json", "build_signals.json"):
        p = os.path.join(root, extra)
        try:
            st = os.stat(p)
            parts.append(f"{extra}:{st.st_mtime_ns}:{st.st_size}")
        except OSError:
            pass
    return "|".join(parts)


def _trim(u):
    """Keep only what the tools return. The raw unit carries tool_calls,
    timestamps and other bulk the tools never expose — storing it all cost
    ~100 MB of RSS on a 7 GB box that was already swapping."""
    return {
        "i": u.get("i"),
        "kind": u.get("kind"),
        "text": u.get("text", ""),
        "date": u.get("date"),
        "size": u.get("size"),
    }


def _read_new(path, offsets, sink):
    """Parse only the bytes appended to *path* since the last read.

    The store appends; it does not rewrite. So each file keeps a byte offset
    and we parse just the new tail. A full re-parse of ~9k JSONL lines on
    every reload tick was the CPU burn — this is O(new bytes) instead.
    """
    try:
        size = os.path.getsize(path)
    except OSError:
        return
    start = offsets.get(path, 0)
    if size < start:            # truncated/rotated: start over
        start = 0
    if size == start:
        return
    try:
        with open(path, "rb") as fh:
            fh.seek(start)
            blob = fh.read()
    except OSError:
        return
    offsets[path] = start + len(blob)
    for line in blob.decode("utf-8", "replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            sink(json.loads(line))
        except Exception:
            continue


def load(root, full=False):
    """Read the live store. Units and nodes are keyed for O(1) lookup.

    full=True re-reads everything from scratch (used at startup and if a
    file shrank); otherwise only newly appended bytes are parsed.
    """
    units = STATE["units"]
    nodes = STATE["nodes"]
    offsets = STATE["offsets"]
    if full:
        units.clear(); nodes.clear(); offsets.clear()
    mdir = os.path.join(root, "main")
    if os.path.isdir(mdir):
        for fn in sorted(os.listdir(mdir)):
            if fn.endswith(".jsonl"):
                _read_new(os.path.join(mdir, fn), offsets,
                          lambda u: units.__setitem__(int(u["i"]), _trim(u)))
    tdir = os.path.join(root, "tree")
    if os.path.isdir(tdir):
        for fn in sorted(os.listdir(tdir)):
            if fn.endswith(".jsonl"):
                _read_new(os.path.join(tdir, fn), offsets,
                          lambda n: nodes.__setitem__((int(n["l"]), int(n["i"])),
                                                     {"text": n.get("text", "")}))
    view = []
    try:
        with open(os.path.join(root, "view.json"), encoding="utf-8") as fh:
            raw = json.load(fh)
        for e in raw:
            view.append((int(e["l"]), int(e["i"])))
    except Exception:
        pass
    counter = 0
    try:
        with open(os.path.join(root, "build_signals.json"), encoding="utf-8") as fh:
            counter = int(json.load(fh).get("counter", 0))
    except Exception:
        pass
    if not counter and units:
        counter = max(units) + 1
    STATE.update(units=units, nodes=nodes, view=view, counter=counter)


def ensure_fresh(root):
    """Reload the store when it changed (the backfill is still running)."""
    now = time.time()
    if now - STAMP["at"] < RELOAD_EVERY:
        return
    STAMP["at"] = now
    key = _store_key(root)
    if key != STAMP["key"]:
        load(root)          # incremental: only newly appended bytes
        STAMP["key"] = key


def fmt_node(l, i):
    node = STATE["nodes"].get((l, i))
    if not node:
        return f"{i * 2 ** l}+{2 ** l}|{PLACEHOLDER}"
    return f"{i * 2 ** l}+{2 ** l}|{node['text']}"


def view_lines():
    """The graded view, plus the un-summarized tail as placeholders.

    Capped at VIEW_HI bytes, newest first kept, with an explicit note when
    the view was truncated — the model must never mistake a capped view for
    the whole conversation.
    """
    lines = []
    covered_to = 0
    for l, i in STATE["view"]:
        node = STATE["nodes"].get((l, i))
        if node is None:
            continue
        lines.append(f"{i * 2 ** l}+{2 ** l}|{node['text']}")
        covered_to = max(covered_to, (i + 1) * (2 ** l))
    counter = STATE["counter"]
    if counter > covered_to:
        for mid in range(covered_to, counter):
            lines.append(f"{mid}+1|{PLACEHOLDER}")

    total = sum(len(l.encode("utf-8")) + 1 for l in lines)
    if total > VIEW_HI:
        kept, used = [], 0
        for line in reversed(lines):
            b = len(line.encode("utf-8")) + 1
            if used + b > VIEW_HI:
                break
            kept.append(line)
            used += b
        kept.reverse()
        dropped = len(lines) - len(kept)
        kept.insert(0, f"[view truncated: {dropped} older lines omitted to stay "
                       f"within the {VIEW_HI // 1000} KB budget; the merge "
                       f"scheduler is behind. Zoom to reach older material.]")
        lines = kept
    return lines


mcp = MCPServer("tree-mem")


@mcp.tool()
def tree_view() -> str:
    """The whole-chat graded summary view of the tree-memory layer.
    Lines are id+n|text covering n messages from id; n=1 means one message.
    Use tree_zoom to open a line into what it was made from. This is the
    agent's memory of past conversations — its latest word on a thing is the
    truth. Do not grep or search memories manually: zoom is the mechanism."""
    ensure_fresh(ROOT)
    return "\n".join(view_lines())


@mcp.tool()
def tree_zoom(id: int, n: int) -> str:
    """Open view line id+n into its two children (or the message if n=1).
    Requires: n is a power of 2 and id is a multiple of n.
    Zoom until you have it whole before you act, guess or ask."""
    ensure_fresh(ROOT)
    if n < 1 or (n & (n - 1)) != 0:
        return "ERROR: n must be a power of 2"
    if id % n != 0:
        return "ERROR: id must be a multiple of n"
    if n == 1:
        u = STATE["units"].get(id)
        if u is None:
            return f"ERROR: no message id {id}"
        return json.dumps(u, ensure_ascii=False)[:4000]
    l = n.bit_length() - 1          # level of the parent
    i = id // n
    child_l, child_i = l - 1, i * 2
    return fmt_node(child_l, child_i) + "\n" + fmt_node(child_l, child_i + 1)


@mcp.tool()
def tree_date(id: int) -> str:
    """The date and time of tree-memory message id."""
    ensure_fresh(ROOT)
    u = STATE["units"].get(id)
    if u is None:
        return f"ERROR: no message id {id}"
    return str(u.get("date"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True,
                    help="tree-mem store dir (contains main/, tree/, view.json)")
    ap.add_argument("--port", type=int, default=4310)
    args = ap.parse_args()
    globals()["ROOT"] = args.root
    load(ROOT, full=True)
    STAMP["key"] = _store_key(ROOT)
    print(f"tree-mem MCP: root={ROOT} units={len(STATE['units'])} "
          f"nodes={len(STATE['nodes'])} view={len(STATE['view'])} "
          f"counter={STATE['counter']}", flush=True)
    app = mcp.streamable_http_app()
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=args.port, http="h11",
                proxy_headers=True, log_level="warning")
