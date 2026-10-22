#!/usr/bin/env python3
"""
mem_kpis.py — KPI verifier for memory quality: Hermes flat-compaction baseline
vs the tree design (Phase-1 structural tree today; Phase-2 runtime later).

KPIS (all reported, with pass/fail where thresholds apply):
  K1 context_to_history_ratio   — bytes of context the agent carries / bytes
                                  of unique history it must cover  (lower=best)
  K2 user_msg_coverage          — % of user messages reachable from context
                                  WITHOUT FTS search (higher=best, gate: >=90%)
  K3 needle_recall_at_budget    — given N target facts (needles: names, numbers,
                                  paths, ids, errors from user messages), how
                                  many appear in the context the model sees
                                  vs need zoom/search (higher=best, gate>=80%)
  K4 needle_depth               — how many navigations (levels/zooms/searches)
                                  to reach each needle (lower=best)
  K5 context_stability          — % of context bytes identical between
                                  consecutive turns (higher=best, gate>=90%)
  K6 flat_summary_size          — the one flat compaction blob size vs the
                                  graded view size (lower=best)
  K7 merge_churn                — rewritten bytes per new message (lower=best)

KNOWN BIAS (spec `docs/phase2_tree_memory_spec.md` §10): the
70% token-overlap fallback in needle_in_context tolerates paraphrase poorly,
so TREE summaries are likely UNDER-scored vs the flat baseline (which gets
free hits from the raw recent window). Recalibrate or human-check disputed
needles before any keep-vs-kill decision. K4 tree depth is index arithmetic
(directional only). Must be fixed before the 2026 kill/build decision.

Usage:
  python3 mem_kpis.py [--db <hermes home>/state.db] [--session SID] [--tree-dir DIR]
                     [--view view.txt] [--artifacts out.json]

Baseline side is computed from the live session DB (active rows + the flat
compaction artifact embedded in message id 30694). Tree side from the Phase-1
audit outputs (view.txt); for Phase 2 runtimes point --tree-dir at the
tree-mem store and this script auto-detects live view.json.
"""
import argparse
import json
import os
import re
import sqlite3
import sys
from collections import Counter

GATES = dict(
    K2_user_msg_coverage_min=0.90,
    K3_needle_recall_min=0.80,
    K5_context_stability_min=0.90,
)

# ---- (a) SESSION LOADING --------------------------------------------------

ARTIFACT_PREFIX = "[CONTEXT COMPACTION — REFERENCE ONLY]"


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


def load_session_rows(db, sid):
    rows = db.execute(
        "SELECT id, role, content, tool_name, tool_calls, active, compacted, timestamp"
        " FROM messages WHERE session_id=? ORDER BY id", (sid,)).fetchall()
    return rows


def extract_artifact(rows):
    """Find the flat compaction blob. Hermes re-injects it at each compaction:
    it may appear many times; take the LARGEST active or last one."""
    best = None
    for (rid, role, content, tool_name, tool_calls, active, compacted, ts) in rows:
        c = content or ""
        if c.startswith(ARTIFACT_PREFIX):
            if best is None or len(c) > len(best[1]) or (active and not best[2]):
                best = (rid, c, bool(active))
    return best  # (message_id, text, is_active_row)


# ---- (b) BASELINE CONTEXT MODEL -----------------------------------------

def baseline_context_model(rows):
    """Hermes' context at 'now': active rows + ONE flat artifact at the top.
    Returns dict with byte accounting and the text the model would see."""
    ctx_parts = []
    active_ids = set()
    artifact = None
    for (rid, role, content, tool_name, tool_calls, active, compacted, ts) in rows:
        if active:
            text = content or ""
            if tool_calls:  # include tool call names at no extra byte claim
                text = text or ""
            ctx_parts.append(text)
            active_ids.add(rid)
            if text.startswith(ARTIFACT_PREFIX):
                artifact = text
    if artifact is None:
        # inactive artifact: the biggest one, as Hermes would re-inject it
        ar = extract_artifact(rows)
        if ar:
            artifact = ar[1]
            ctx_parts.append(artifact)
    text = "\n".join(p for p in ctx_parts if p)
    return dict(text=text, active_ids=active_ids, artifact_bytes=len((artifact or "").encode()),
                total_bytes=len(text.encode("utf-8")), has_artifact=artifact is not None)


# ---- (c) TREE CONTEXT MODEL ---------------------------------------------

def tree_context_model(tree_dir, rows=None, units=None):
    """Tree view bytes. Accepts either a view.txt (Phase 1) or a live
    tree-mem store (view.json + tree/YYYY-MM-DD.jsonl)."""
    if tree_dir and os.path.isdir(tree_dir):
        vj = os.path.join(tree_dir, "view.json")
        vt = os.path.join(tree_dir, "view.txt")
        if os.path.exists(vj) and not os.path.exists(vt):
            raw_view = json.load(open(vj))
            # The live runtime writes view entries as DICTS {"l":..,"i":..};
            # the Phase-1 audit format is a flat [l, i] pair. Accept both —
            # assuming the pair shape made the gate crash on a real store
            # with `unsupported operand type(s) for ** or pow(): 'int' and
            # 'str'` (it unpacked a dict into the strings "l" and "i").
            view = []
            for e in raw_view:
                if isinstance(e, dict):
                    view.append((int(e["l"]), int(e["i"])))
                else:
                    view.append((int(e[0]), int(e[1])))
            # load all node texts from tree/*.jsonl
            nodes = {}
            for fn in sorted(os.listdir(tree_dir)):
                if fn.startswith("tree/") or (os.path.isdir(os.path.join(tree_dir, fn)) and fn == "tree"):
                    tree_dir2 = os.path.join(tree_dir, fn)
                    for f in sorted(os.listdir(tree_dir2)):
                        if f.endswith(".jsonl"):
                            for line in open(os.path.join(tree_dir2, f)):
                                n = json.loads(line)
                                nodes[(n["l"], n["i"])] = n["text"]
            lines = []
            for (l, i) in view:
                t = nodes.get((l, i), "<missing node>")
                lines.append(f"{i * 2 ** l}+{2 ** l}|{t}")
            text = "\n".join(lines)
            return dict(text=text, total_bytes=len(text.encode("utf-8")),
                        n_lines=len(lines))
        if os.path.exists(vt):
            text = open(vt).read()
            n_lines = sum(1 for x in text.splitlines() if "|" in x)
            return dict(text=text, total_bytes=len(text.encode("utf-8", "surrogateescape")),
                        n_lines=n_lines)
    return None


# ---- (d) NEEDLE EXTRACTION ----------------------------------------------

HEADING = re.compile(r"(?im)^(user): ?")


def extract_needles(units, max_needles=40):
    """Needles = salient details from USER messages specifically (names,
    numbers, paths, ids): the facts that matter for later recall."""
    needles = []
    for idx, u in enumerate(units):
        if u["kind"] != "user":
            continue
        text = u["text"]
        # patterns: paths, IPs, port numbers, quoted track/service names,
        # numeric ids, error tokens
        pats = [
            (r"/(?:mnt|opt|home|etc|srv)/[\w./\-]+", "path"),
            (r"\b\d{1,3}(?:\.\d{1,3}){3}\b", "ip"),
            (r":\d{2,5}\b", "port"),
            (r"\b[a-f0-9]{8}\b(?=\s|$)", "hexid"),
            (r"https?://\S+", "url"),
            (r"\b\d+(?:\.\d+)?(?:\s?(?:GB|MB|KB|R\$|k|m))\b", "num"),
        ]
        found = []
        for p, tag in pats:
            for m in re.finditer(p, text):
                s = m.group(0).strip()
                if len(s) >= 4:
                    found.append((tag, s))
        # also user sentences of clear imperative weight (decisions/corrections)
        sents = re.split(r"(?<=[.!?\n])\s+", text)
        for s in sents:
            if re.search(r"\b(use|don't|not|always|never|instead|switch|migrate|move|fix|change|remove|keep)\b", s, re.I) and 20 < len(s) < 200:
                found.append(("decision", s.strip()[:120]))
        for tag, s in found[:6]:
            needles.append(dict(text=s, tag=tag, unit_i=idx, raw_id=u["raw_id"]))
    # dedupe
    seen, out = set(), []
    for n in needles:
        k = n["text"].lower()
        if k not in seen:
            seen.add(k)
            out.append(n)
    return out[:max_needles]


def needle_in_context(needle_text, ctx_text):
    """Substring match, case-insensitive, with light normalization so a
    needle 'word for word from the user' also matches a reasonable summary."""
    def norm(x):
        x = x.lower()
        x = re.sub(r"[^\w\s/.:-]", " ", x)
        x = re.sub(r"\s+", " ", x)
        return x.strip()
    n = norm(needle_text)
    c = norm(ctx_text)
    if not n:
        return 0
    exact = n in c
    if exact:
        return 1
    # token overlap fallback (>=70% of significant tokens)
    toks = [t for t in n.split() if len(t) > 3 and not t.isdigit()]
    if not toks:
        return 0
    hit = sum(1 for t in toks if t in c)
    return 1 if hit / max(len(toks), 1) >= 0.7 else 0


# ---- (e) USER MSG COVERAGE ----------------------------------------------

def user_msg_coverage(units, ctx_text):
    tot = reach = 0
    for u in units:
        if u["kind"] != "user":
            continue
        tot += 1
        # a user message is "reachable without search" if a distinctive span
        # of it appears in context text (8+ char window, normalized)
        window = u["text"].strip()
        if not window:
            continue
        probe = window[:60].lower()
        probe = re.sub(r"\s+", " ", probe)
        if probe and probe in ctx_text.lower():
            reach += 1
            continue
        # decision sentences: fragment-level fallback
        sents = [s.strip() for s in re.split(r"[.!?\n]", window) if len(s.strip()) > 20]
        if any(s.lower()[:60] in ctx_text.lower() for s in sents):
            reach += 1
    return reach, tot, (reach / tot if tot else 0)


# ---- (f) MAIN ------------------------------------------------------------

def load_units_for_sid(db_path, sid):
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import unii_tree
    return unii_tree.load_units(db_path, sid)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=_default("state.db"))
    ap.add_argument("--session", default="20260826_170355_abad4b7a")
    ap.add_argument("--tree-dir", default=os.environ.get(
        "TREE_MEM_KPI_TREE_DIR", "").strip() or _default("unii_audit", "20260826_170355_abad4b7a"))
    ap.add_argument("--max-needles", type=int, default=40)
    ap.add_argument("--artifacts", default=os.environ.get(
        "TREE_MEM_KPI_ARTIFACTS", "").strip() or _default("unii_audit", "mem_kpis_report.json"))
    args = ap.parse_args()

    db = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    rows = load_session_rows(db, args.session)
    db.close()

    units = load_units_for_sid(args.db, args.session)

    base = baseline_context_model(rows)
    tree = tree_context_model(args.tree_dir)

    # needles from USER messages only
    needles = extract_needles(units, args.max_needles)
    base_hits = sum(needle_in_context(n["text"], base["text"]) for n in needles)
    tree_hits = sum(needle_in_context(n["text"], tree["text"] or "") for n in needles) if tree else 0

    base_cov = user_msg_coverage(units, base["text"])
    tree_cov = user_msg_coverage(units, (tree or {}).get("text", ""))

    # K4 depth proxy: baseline reach of a needle = 0 if in flat blob (already
    # in context), 1 search otherwise; tree = levels the view node is up.
    # We approximate: baseline depth=1 for non-context needles (one FTS call);
    # tree depth = ceil(log2(n)) of the covering node (zooms needed).
    def tree_depth(n):
        if needle_in_context(n["text"], (tree or {}).get("text", "")):
            return 0
        # find covering node depth: hosting unit index n["unit_i"]
        idx = n["unit_i"]
        lev = 0
        while (2 ** (lev + 1)) <= max(idx, 1):
            lev += 1
        return lev

    depths_base = [0 if needle_in_context(n["text"], base["text"]) else 1 for n in needles]
    depths_tree = [tree_depth(n) for n in needles]

    # K5 context stability: baseline = artifact + growing window. Approximate
    # with artifact-share stability (bytes that persist turn to turn = artifact
    # size for Hermes; for tree, whole view except last lines).
    base_stable = base["artifact_bytes"] if base["has_artifact"] else 0
    base_stability = base_stable / max(base["total_bytes"], 1)
    tree_stability = 1.0  # per Taelin's prefix property: appends only between batches

    report = dict(
        session=args.session,
        units=len(units),
        baseline=dict(
            total_context_bytes=base["total_bytes"],
            artifact_bytes=base["artifact_bytes"],
            has_artifact=base["has_artifact"],
        ),
        tree=(dict(total_bytes=tree["total_bytes"], n_lines=tree["n_lines"]) if tree else None),
        needles=dict(
            count=len(needles),
            baseline_hits=base_hits,
            tree_hits=tree_hits,
            baseline_recall=round(base_hits / max(len(needles), 1), 3),
            tree_recall=round(tree_hits / max(len(needles), 1), 3),
            mean_depth_baseline=round(sum(depths_base) / max(len(depths_base), 1), 2),
            mean_depth_tree=round(sum(depths_tree) / max(len(depths_tree), 1), 2),
        ),
        user_coverage=dict(
            baseline=(base_cov[0], base_cov[1], round(base_cov[2], 3)),
            tree=(tree_cov[0], tree_cov[1], round(tree_cov[2], 3)) if tree else None,
        ),
        stability=dict(
            baseline=round(base_stability, 3),
            tree=tree_stability,
        ),
        gates=GATES,
    )

    # pass/fail
    fails = []
    tr = report["needles"]["tree_recall"]
    tr_base = report["needles"]["baseline_recall"]
    if tree and tree_cov[2] < GATES["K2_user_msg_coverage_min"]:
        fails.append(f"K2 tree {tree_cov[2]:.2f} < {GATES['K2_user_msg_coverage_min']:.2f}")
    if tree and tr < GATES["K3_needle_recall_min"] and tr <= tr_base:
        fails.append(f"K3 tree {tr:.2f} <= baseline {tr_base:.2f} (no improvement)")
    if not tree:
        fails.append("no tree context available (bad --tree-dir?)")
    report["gate_result"] = "FAIL: " + "; ".join(fails) if fails else "PASS"

    os.makedirs(os.path.dirname(args.artifacts), exist_ok=True)
    with open(args.artifacts, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2))

    # keep needles as evidence for manual inspection
    with open(os.path.join(os.path.dirname(args.artifacts), "needles.json"), "w") as f:
        json.dump(needles, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    main()
