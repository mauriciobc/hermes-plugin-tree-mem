#!/usr/bin/env python3
"""
tree-mem.py — Phase 2a shadow runtime for the tree-memory spec
(<hermes home>/docs/phase2_tree_memory_spec.md, approved 2026-10-08).

v0a: observes a Hermes session in state.db and maintains its own store:
  main/YYYY-MM-DD.jsonl   verbatim unit log {i,kind,raw_id,text,size,date}
  tree/YYYY-MM-DD.jsonl   built nodes {l,i,text,size}
  view.json               current view as [{l,i}] (never rebuilt from log)
  build_signals.json      owner/divergence/ingest state (V3/V4)
  logs/serve.log

Summarizer workers (≤8, ≥interval spacing) call the Nous API
(upstage/solar-mini4:free per spec §11-V2). Merge context = built view lines
before the node, 24 KB cap (spec §5.3). Merges batch at view ≥128 KB → ≤64 KB
(most-due pairs, spec §3.2). All byte rules enforced in code, never in model.

Run:  python3 tree-mem.py serve --session SID [--db <hermes home>/state.db]
                                   [--root <hermes home>/tree-mem/SID]
                                   [--interval 1.0] [--workers 8]
"""
import argparse
import datetime
import json
import os
import sys
import threading
import time
import urllib.request
import urllib.error

DB_RO = "file:{db}?mode=ro"
LEAF_CAP = 512
VIEW_HI = 128_000
VIEW_LO = 64_000
MERGE_CTX_CAP = 24_000
NOUS_BASE = os.environ.get("TREE_MEM_SUMMARIZER_BASE", "https://inference-api.nousresearch.com/v1")
NOUS_MODEL = os.environ.get("TREE_MEM_SUMMARIZER_MODEL", "upstage/solar-mini4:free")
def _hermes_home():
    """Hermes' own home resolver (HERMES_HOME may point anywhere)."""
    h = os.environ.get("HERMES_HOME", "").strip()
    if h:
        return h
    try:
        from hermes_constants import get_hermes_home
        return str(get_hermes_home())
    except Exception:
        return os.path.join(os.path.expanduser("~"), ".hermes")


AUTH_JSON = os.environ.get("TREE_MEM_AUTH_JSON",
                           os.path.join(_hermes_home(), "auth.json"))
NOTE_PREFIX = "[CONTEXT COMPACTION"

log_lock = threading.Lock()


def log(msg):
    with log_lock:
        line = f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} {msg}"
        print(line, flush=True)
        try:
            with open(LOG_PATH, "a") as f:
                f.write(line + "\n")
        except Exception:
            pass


LOG_PATH = "/dev/null"   # set in serve()


def iso(ts):
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).isoformat()


def one_line(s):
    return " ".join((s or "").split())


# ---------------- unit grouping (mirror of unii_tree.load_units) ----------

LONG_TEXT_CHARS = 30_000   # PROPOSAL §1: long text is split into rows, never cut


def split_long_text(text, size=LONG_TEXT_CHARS):
    """Split a long text into several rows (PROPOSAL §1). Short text is one row."""
    t = text or ""
    if len(t) <= size:
        return [t]
    parts = [t[k:k + size] for k in range(0, len(t), size)]
    return parts


TOOL_CLIP_CHARS = 30_000   # PROPOSAL §1: head+tail clip for tool output


def clip_head_tail(text, cap):
    """Keep the head and the tail of a long text, up to `cap` chars in all."""
    t = text or ""
    if len(t) <= cap:
        return t
    head = cap // 2
    tail = cap - head
    return t[:head] + "\n\n[...clipped...]\n\n" + t[-tail:]


def group_rows(rows):
    """rows: list of (id, role, content, tool_name, tool_calls, ts, active)
    ordered by id. Returns logical units: user|unii|tool|note."""
    units = []
    pending = {}
    for (rid, role, content, tool_name, tool_calls, ts, active) in rows:
        text = content or ""
        if role == "assistant" and tool_calls:
            try:
                calls = json.loads(tool_calls) if isinstance(tool_calls, str) else tool_calls
            except Exception:
                calls = []
            names = []
            for c in (calls if isinstance(calls, list) else []):
                nm = (c.get("function") or {}).get("name") or c.get("name") or "?"
                names.append(nm)
            units.append(dict(kind="unii", raw_id=rid, active=active, ts=ts,
                              text=f"[calls: {', '.join(names) or '-'}]"))
        elif role == "assistant":
            units.append(dict(kind="unii", raw_id=rid, active=active, ts=ts, text=text))
        elif role == "user":
            kind = "note" if text.startswith(NOTE_PREFIX) else "user"
            # PROPOSAL §1: long text is never cut — it is logged as several
            # messages in a row. Each row becomes its own leaf.
            for part in split_long_text(text):
                units.append(dict(kind=kind, raw_id=rid, active=active, ts=ts, text=part))
        elif role == "tool":
            units.append(dict(kind="tool", raw_id=rid, active=active, ts=ts,
                              tool=tool_name or "?", text=text))
    # merge assistant-calls with following echo
    merged = []
    k = 0
    while k < len(units):
        u = units[k]
        if (u["kind"] == "unii" and u["text"].startswith("[calls:")
                and k + 1 < len(units) and units[k + 1]["kind"] == "tool"):
            e = units[k + 1]
            merged.append(dict(kind="tool", raw_id=u["raw_id"],
                               active=(u["active"] and e["active"]) if isinstance(u["active"], int) else True,
                               ts=u["ts"], tool=e["tool"], text=e["text"]))
            k += 2
            continue
        merged.append(u)
        k += 1
    return merged


# ---------------- store ----------------------------------------------------

class Store:
    def __init__(self, root):
        self.root = root
        self.main_dir = os.path.join(root, "main")
        self.tree_dir = os.path.join(root, "tree")
        self.log_dir = os.path.join(root, "logs")
        for d in (self.main_dir, self.tree_dir, self.log_dir):
            os.makedirs(d, exist_ok=True)
        os.chmod(root, 0o750)
        self.lock = threading.RLock()
        self.counter = 0            # unit index i
        self.last_ingest_id = 0     # max messages.id ingested
        self.divergence = False
        self.units_meta = {}        # i -> {"kind","date","size"}
        self.nodes = {}             # (l,i) -> {"text","size","built":bool}
        self.view = []              # ordered [{"l","i"}]
        self._load()

    # --- persistence -------------------------------------------------------
    def _day_file(self, d, date):
        return os.path.join(d, date + ".jsonl")

    def _append_fsync(self, path, obj):
        line = json.dumps(obj, ensure_ascii=False) + "\n"
        with open(path, "a") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.chmod(path, 0o640)
        except OSError:
            pass

    def _load(self):
        sp = os.path.join(self.root, "build_signals.json")
        if os.path.exists(sp):
            s = json.load(open(sp))
            self.counter = s.get("counter", 0)
            self.last_ingest_id = s.get("last_ingest_id", 0)
            self.divergence = s.get("divergence", False)
            self.units_meta = s.get("units_meta", {})
        # nodes from tree/*.jsonl
        for fn in sorted(os.listdir(self.tree_dir)):
            if fn.endswith(".jsonl"):
                for line in open(os.path.join(self.tree_dir, fn)):
                    try:
                        n = json.loads(line)
                    except Exception:
                        continue
                    self.nodes[(n["l"], n["i"])] = {"text": n["text"], "size": n["size"], "built": True}
        vp = os.path.join(self.root, "view.json")
        if os.path.exists(vp):
            try:
                self.view = json.load(open(vp))
            except Exception:
                backup = vp + ".corrupt-" + str(time.time())
                os.rename(vp, backup)
                with open(os.path.join(self.log_dir, "view_corrupt.json"), "w") as f:
                    f.write(backup)
                log(f"FATAL-ISH: view.json corrupt, moved to {backup}; starting empty view (spec forbids silent rebuild — recorded)")
                self.view = []
        # pending (unbuilt) leaves recover from main log state
        if self.counter > 0 and not self.units_meta:
            for fn in sorted(os.listdir(self.main_dir)):
                if fn.endswith(".jsonl"):
                    for line in open(os.path.join(self.main_dir, fn)):
                        try:
                            u = json.loads(line)
                        except Exception:
                            continue
                        self.units_meta[u["i"]] = {"kind": u["kind"], "date": u["date"], "size": u["size"]}

    def _atomic_write(self, path, payload):
        """Write JSON atomically, safely under concurrency.

        The old code used a FIXED "<path>.tmp" name: two workers writing the
        same file raced on it, and one rename could publish a half-written
        file (or the loser got FileNotFoundError because the winner had
        already renamed the temp away). A unique temp name plus os.replace
        makes concurrent writes safe: the last writer wins with a COMPLETE
        file, and no reader ever sees a partial one.
        """
        tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
        try:
            with open(tmp, "w") as f:
                json.dump(payload, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)      # atomic replace, even on POSIX
            os.chmod(path, 0o640)
        finally:
            if os.path.exists(tmp):
                try:
                    os.unlink(tmp)
                except OSError:
                    pass

    def save_signals(self):
        sp = os.path.join(self.root, "build_signals.json")
        with self.lock:
            self._atomic_write(sp, dict(
                counter=self.counter, last_ingest_id=self.last_ingest_id,
                divergence=self.divergence, units_meta=self.units_meta,
                updated=datetime.datetime.now(datetime.timezone.utc).isoformat()))

    def save_view(self):
        vp = os.path.join(self.root, "view.json")
        with self.lock:
            self._atomic_write(vp, list(self.view))

    # --- appends -----------------------------------------------------------
    def append_units(self, units):
        """Append units to main log; returns list of new leaf tasks."""
        tasks = []
        with self.lock:
            for u in units:
                i = self.counter
                date = iso(u["ts"])[:10]
                if u["kind"] == "tool":
                    # PROPOSAL §1: a tool's output is clipped to its head and
                    # tail, 30,000 characters in all.
                    text = f"tool {u.get('tool', '?')}: {clip_head_tail(u['text'], TOOL_CLIP_CHARS)}"
                else:
                    text = u["text"]
                size = len(text.encode("utf-8"))
                self._append_fsync(self._day_file(self.main_dir, date),
                                   dict(i=i, kind=u["kind"], raw_id=u["raw_id"],
                                        text=text, size=size, date=iso(u["ts"])))
                self.units_meta[i] = dict(kind=u["kind"], date=iso(u["ts"]), size=size)
                self.last_ingest_id = max(self.last_ingest_id, u["raw_id"])
                self.counter += 1
                tasks.append((i, u["kind"], text))
            self.save_signals()
        return tasks

    def append_marker(self, text, ts=None):
        with self.lock:
            i = self.counter
            ts = ts or time.time()
            date = iso(ts)[:10]
            self._append_fsync(self._day_file(self.main_dir, date),
                               dict(i=i, kind="work", raw_id=-1, text=text,
                                    size=len(text.encode("utf-8")), date=iso(ts)))
            self.units_meta[i] = dict(kind="work", date=iso(ts), size=0)
            self.counter += 1
            # A FORK marker is INFORMATIONAL (we keep shadowing the old
            # session) — it is not divergence. Only a REWIND/gap is.
            # Markers are prefixed "MARKER: ..." — check the real content.
            if "REWIND" in text or "GAP" in text:
                self.divergence = True
            self.save_signals()
        log(f"MARKER: {text}")

    def node_day(self, ts):
        return iso(ts)[:10]

    def write_node(self, l, i, text, ts=None):
        with self.lock:
            size = len(text.encode("utf-8"))
            self.nodes[(l, i)] = {"text": text, "size": size, "built": True}
            self._append_fsync(self._day_file(self.tree_dir, date=self.node_day(ts or time.time())),
                               dict(l=l, i=i, text=text, size=size))
            if l == 0:
                # leaf built → enters the view at its position (append-only view)
                entry = {"l": 0, "i": i}
                if entry not in self.view:
                    self.view.append(entry)
                    self.view.sort(key=lambda e: (e["i"] * (2 ** e["l"])))
                    self.save_view()
            self.save_signals()

    def view_bytes(self):
        with self.lock:
            return sum(self.nodes[(e["l"], e["i"])]["size"]
                       for e in self.view if (e["l"], e["i"]) in self.nodes)

    def unit_text(self, i):
        """Fetch unit i's text from main logs (for queue reconstruction)."""
        with self.lock:
            for fn in sorted(os.listdir(self.main_dir)):
                if not fn.endswith(".jsonl"):
                    continue
                for line in open(os.path.join(self.main_dir, fn)):
                    try:
                        u = json.loads(line)
                    except Exception:
                        continue
                    if u["i"] == i:
                        return u["text"]
        return None

    def due_candidates(self):
        """Mergeable pairs (both in view, both built), most-due first."""
        T = self.counter
        vset = {(e["l"], e["i"]) for e in self.view}
        out = []
        for (l, i) in list(vset):
            if l == 0 and T < 2:
                continue
            # PROPOSAL §2: node(l,i) = merge(node(l-1,2i), node(l-1,2i+1)).
            # Siblings at level l are (l,2k) and (l,2k+1) — i must be EVEN.
            # Pairing (l,i)+(l,i+1) for odd i produces a parent that claims a
            # message range its children do not cover (coverage corruption).
            if i % 2 != 0:
                continue
            sib = (l, i + 1)
            if sib not in vset:
                continue
            parent = (l + 1, i // 2)
            if self.nodes.get(parent, {}).get("built"):
                continue
            if (l, i) not in self.nodes or sib not in self.nodes:
                continue
            # due = (T - last) / 2^l   (PROPOSAL §3.2: measure from the pair's
            # LAST message, never its first — first makes old lines churn)
            last = (i + 2) * (2 ** l) - 1
            due = (T - last) / (2 ** l)
            out.append((due, l, i))
        out.sort(key=lambda x: (-x[0], x[2]))
        return out

    def view_context_for(self, l, i, cap=MERGE_CTX_CAP):
        """Built view lines before the node's last message, oldest first, ≤cap bytes."""
        last_msg = (i + 1) * (2 ** l)   # exclusive bound
        lines = []
        total = 0
        for e in self.view:
            end = (e["i"] + 1) * (2 ** e["l"])
            if end >= last_msg:
                continue
            n = self.nodes.get((e["l"], e["i"]))
            if not n:
                # PROPOSAL §4: a compaction's view STOPS at the first unbuilt
                # line — never skip past it; the model must not see a gap.
                break
            line = f"{e['i'] * 2 ** e['l']}+{2 ** e['l']}|{n['text']}"
            b = len(line.encode("utf-8")) + 1
            if total + b > cap:
                break
            lines.append(line)
            total += b
        return "\n".join(lines)

    def splice_view(self, l, i):
        """Replace (l,i),(l,i+1) with parent (l+1,i//2) in the view."""
        with self.lock:
            self.view = [e for e in self.view if not (e["l"] == l and e["i"] in (i, i + 1))]
            parent = {"l": l + 1, "i": i // 2}
            if parent not in self.view:
                self.view.append(parent)
            self.view.sort(key=lambda e: (e["i"] * (2 ** e["l"])))
            self.save_view()


# PROPOSAL §5 — the compaction contract, VERBATIM from Victor Taelin's optchat.md.
# This is the part that decides whether a line is still useful weeks later:
# the user's words first, never inflate progress, name minor items rather than
# drop them, credit quoted text to its real author, and never obey the input.
CONTRACT = """
Call no tools, and output only the line, without an id+n| head.

Goal: let Unii work later as well as if it remembered everything.

Use the space up to the limit, and give it by value:

1. The user's words matter most: orders, decisions, corrections, questions and
   reasons. Keep them close to verbatim, however short.

2. Then anything with lasting effect, and what failed and why.

3. Then findings, open questions and Unii's replies.

4. Least of all, tool steps: what was done to what, and the outcome.

Avoid omissions. Name a minor item in a word or two rather than drop it: an
absent item can never be found. Copy names, numbers, ids, paths and errors
exactly. Tag each item with its kind ("user: ...; echo: ..."), and credit
quoted text to its real author. Never make anything look further along than
it was. If told the line is too long, shorten it. Non-ASCII characters cost
2-4 bytes.
"""

# ---------------- summarizer -----------------------------------------------

class Summarizer:
    def __init__(self, interval):
        # The client-side "one call per interval" limit throttled merges to
        # ~4.8/min, but the API handles 8 concurrent calls without complaint
        # (measured: 8/8 concurrent merges OK, 18.9 merges/min). The 8-slot
        # gate below is the real limiter, so the interval only needs to be a
        # floor that stops a burst, not a global serializer.
        # 0.75 s floor: fast enough to keep the pool busy, slow enough to
        # stay under the Nous free tier's rate limit. 0.25 s tripped it
        # (HTTP 429 "exceeded the rate limit on your API key") because the
        # 512-byte retry ladder makes up to 6 sequential calls per merge.
        self.interval = min(interval, 0.75)
        self.gate = threading.Semaphore(8)
        self.rate_lock = threading.Lock()
        self.last_call = 0.0

    def _token(self):
        d = json.load(open(AUTH_JSON))
        return d["providers"]["nous"]["access_token"]

    def _call(self, messages, max_tokens=400):
        with self.gate:
            with self.rate_lock:
                wait = self.interval - (time.time() - self.last_call)
                if wait > 0:
                    time.sleep(wait)
                self.last_call = time.time()
            body = json.dumps({"model": NOUS_MODEL, "max_tokens": max_tokens,
                               "messages": messages}).encode()
            for attempt in range(5):
                req = urllib.request.Request(
                    NOUS_BASE + "/chat/completions", data=body,
                    headers={"Authorization": "Bearer " + self._token(),
                             "Content-Type": "application/json"})
                try:
                    r = urllib.request.urlopen(req, timeout=90)
                    return json.loads(r.read())["choices"][0]["message"]["content"]
                except urllib.error.HTTPError as e:
                    if e.code == 429:
                        # Rate limited. Back off and retry — this is a
                        # "not now", not a failure. The old code gave up and
                        # the merge queue stalled on "failed after retries".
                        time.sleep(5 + attempt * 10)
                        continue
                    if e.code in (401, 403) and attempt == 0:
                        continue   # token may have rotated; re-read auth.json
                    if e.code == 429:
                        time.sleep(5 + attempt * 10)
                        continue
                    raise
                except Exception:
                    if attempt == 0:
                        time.sleep(4)
                        continue
                    raise
        raise RuntimeError("summarizer call failed after retries")

    def compress_leaf(self, kind, text):
        """PROPOSAL §4 — compaction of ONE message into one line of <=512 bytes.

        The task is verbatim from the proposal; the contract block below is the
        proposal's own compaction contract, verbatim. Both matter: the contract
        is what makes a line usable weeks later ("the user's words matter most",
        "never make anything look further along than it was").
        """
        ruler = "-" * 512
        messages = [
            {"role": "user", "content":
                f"Compaction: compress message into one line of at most 512 bytes "
                f"(about 70 words), the length of this ruler:\n{ruler}\n"
                f"<input>\n{kind}: {text}\n</input>\n" + CONTRACT},
        ]
        out = self._call(messages)
        if out is None:
            out = self._call(messages + [{"role": "assistant", "content": ""},
                              {"role": "user",
                               "content": "Your previous reply was empty. Output ONLY the summary line, "
                                          "no reasoning."}])
        return self.enforce_with_retries(messages, out)

    def merge_pair(self, a_text, b_text, context):
        """PROPOSAL §4 — merge of two adjacent lines into one line of <=512 bytes.

        <chat> is CONTEXT ONLY: "use it to understand <input> and resolve its
        references, never to add what <input> lacks."
        """
        ruler = "-" * 512
        messages = [
            {"role": "user", "content":
                f"<chat> may hold their messages, in more detail: take details of "
                f"them from there too.\n{context}\n</chat>\n"
                f"Compaction: merge lines (adjacent) into one line of at most 512 bytes "
                f"(about 70 words), the length of this ruler:\n{ruler}\n"
                f"<input>\n{a_text}\n{b_text}\n</input>\n" + CONTRACT},
        ]
        out = self._call(messages, max_tokens=900)
        if out is None:
            out = self._call(messages + [{"role": "assistant", "content": ""},
                              {"role": "user",
                               "content": "Output ONLY the merged line, no reasoning."}], max_tokens=900)
        return self.enforce_with_retries(messages, out)

    def enforce(self, text):
        """PROPOSAL §4 "The size": models can't count bytes, so use the ruler,
        the cut, retries. We keep the shortest of up to 5 attempts; a final hard
        clamp is the last resort (truncating the tail can cut the user's words,
        so it is never the first choice)."""
        t = (text or "").strip()
        b = t.encode("utf-8")
        return one_line(b[:LEAF_CAP].decode("utf-8", "ignore"))

    def enforce_with_retries(self, messages, text):
        """PROPOSAL §4 retry protocol: try at most 5 times, keep the shortest
        line; ask for a cut when the reply is over the limit."""
        best = self.enforce(text)
        best_len = len(best.encode("utf-8"))
        for _ in range(4):
            if best_len <= LEAF_CAP:
                break
            cut = best.encode("utf-8")[:LEAF_CAP].decode("utf-8", "ignore")
            ask = (f"Too long: your line is {len(text.encode('utf-8'))} bytes, over the "
                   f"512-byte limit. Write the whole line again for the same <input>, "
                   f"cutting just enough of the least valuable items to fit before this cut:\n"
                   f"{cut}| <- LIMIT")
            out = self._call(messages + [{"role": "assistant", "content": text or ""},
                                          {"role": "user", "content": ask}])
            if not out:
                break
            cand = self.enforce(out)
            cand_len = len(cand.encode("utf-8"))
            if cand_len < best_len:
                best, best_len = cand, cand_len
        return best


# ---------------- worker pool ----------------------------------------------

class Runtime:
    def __init__(self, store, summ, workers=8):
        self.store = store
        self.summ = summ
        self.wq = []
        self.wq_lock = threading.Condition()
        self.failed_nodes = {}
        self.running = True
        self.threads = [threading.Thread(target=self.worker, daemon=True)
                        for _ in range(max(1, workers))]

    def push_task(self, task):
        with self.wq_lock:
            self.wq.append(task)
            self.wq_lock.notify()

    def worker(self):
        while self.running:
            with self.wq_lock:
                while not self.wq and self.running:
                    self.wq_lock.wait(timeout=5)
                if not self.running:
                    return
                task = self.wq.pop(0)
            try:
                self.do_task(task)
            except Exception as e:
                kind, args = task[0], task
                fail = self.failed_nodes.get((task[0], id(task)), 0) + 1
                self.failed_nodes[(task[0], id(task))] = fail
                log(f"WORKER-ERR {task[0]}: {type(e).__name__} {str(e)[:120]} (fail #{fail})")
                if fail <= 15:
                    time.sleep(min(30, 2 * fail))
                    self.push_task(task)

    def do_task(self, task):
        t = task[0]
        if t == "batch":
            # Batch-merge until the view is back inside its budget. This task
            # was being pushed by maybe_batch_merge() but had NO handler, so it
            # fell through the if/elif chain and did nothing.
            try:
                self.batch_merge()
            finally:
                # The guard MUST be cleared here. It was only cleared on the
                # success path, so one failed batch wedged the whole
                # scheduler: _batch_running stayed True and the heartbeat
                # never queued another merge.
                self._batch_running = False
            return
        if t == "leaf":
            _, i, kind, text = task
            if (0, i) in self.store.nodes:
                return
            # PROPOSAL §2: "A source that fits in 512 bytes is its own node, with
            # no model call." 67% of our units qualify — skipping the API for
            # them is a 3,397-call saving and keeps their wording verbatim.
            raw = text.encode("utf-8")
            if len(raw) <= LEAF_CAP:
                line = text.strip()
            else:
                line = self.summ.compress_leaf(kind, text)
            size = len(line.encode("utf-8"))
            if size == 0:
                # An EMPTY source (assistant messages with no content — tool-only
                # turns, aborted replies) has no summary to write. Retrying it
                # forever starved the merge queue: the releaf task kept
                # re-queueing while batch_merge never ran, so the view stayed
                # at leaf level and never reached its 64-128 KB budget.
                # A zero-content turn IS a fact ("nothing was said here"), so
                # record it as such and let the tree move on.
                self.store.write_node(0, i, f"{kind}: (empty message — no content)")
                self.maybe_batch_merge()
                return
            self.store.write_node(0, i, f"{kind}: {line}" if not line.startswith("user:") else line)
            self.maybe_batch_merge()
        elif t == "merge":
            _, l, i = task
            a = self.store.nodes.get((l, i))
            b = self.store.nodes.get((l, i + 1))
            if not a or not b:
                return
            if (l + 1, i // 2) in self.store.nodes:
                return
            ctx = self.store.view_context_for(l, i)
            line = self.summ.merge_pair(a["text"], b["text"], ctx)
            if len(line.encode("utf-8")) == 0:
                raise ValueError("empty merge line")
            self.store.write_node(l + 1, i // 2, line)
            self.store.splice_view(l, i)
            self.maybe_batch_merge()
        elif t == "releaf":
            _, i = task
            line_text = self.store.unit_text(i)
            if line_text is None:
                return
            kind = self.store.units_meta[str(i)]["kind"]
            self.do_task(("leaf", i, kind, line_text))

    def schedule_merge_if_needed(self):
        """Kick the batch-merge when the view is over budget and the workers
        are otherwise idle.

        The batch used to start ONLY as a side effect of finishing a leaf
        task. Once the leaves ran out, nothing triggered it, so the view sat
        at 1.3 MB with 2,300+ mergeable pairs and never converged. The poll
        loop is the heartbeat, so it is what asks for the merge.
        """
        if self.store.view_bytes() <= VIEW_HI:
            return
        with self.wq_lock:
            # Count the batch tasks already queued instead of trusting a
            # sticky boolean — a boolean wedged the scheduler when a single
            # task failed.
            queued = sum(1 for t in self.wq if t[0] == "batch")
            if queued >= len(self.threads):
                return          # every worker already has a batch
            self._batch_running = True
            # One batch task per worker. The chain used to be serial — one
            # task did one merge and re-queued itself — so only ONE merge was
            # ever in flight while 7 workers sat idle (measured: 9 merges/min
            # with 8 workers, vs 18.9/min when 8 run concurrently).
            for _ in range(len(self.threads)):
                self.wq.append(("batch",))
            self.wq_lock.notify()

    def maybe_batch_merge(self):
        """Schedule batch-merge work when the view is over budget.

        The batch is a QUEUE TASK, not inline work: doing it inline pinned one
        worker to the whole job. The queue lets all workers share it.
        """
        if self.store.view_bytes() <= VIEW_HI:
            return
        if getattr(self, "_batch_running", False):
            return
        self._batch_running = True
        with self.wq_lock:
            if len(self.wq) < 64:
                self.push_task(("batch",))

    def batch_merge(self):
        """Merge pairs until the view is back inside VIEW_LO.

        This used to loop here, which pinned ONE worker thread to the whole
        job (each merge is a ~6 s model call) while the other workers idled —
        2,373 pairs at 6 s each is ~4 hours on one thread. Instead: merge one
        pair per call and re-queue the batch, so every worker shares the load.
        """
        if self.store.view_bytes() <= VIEW_LO:
            return
        due = self.store.due_candidates()
        if not due:
            return
        _, l, i = due[0]
        self.do_task(("merge", l, i))
        # Keep the pool fed. Handing back ONE task per completed merge starved
        # it: a worker that finished a 3 s merge put back a single task, so
        # the queue drained to a trickle (measured 9.3 merges/min with 6
        # workers, versus 18.9-48/min when the same number run concurrently).
        # Every worker needs a task waiting for it.
        with self.wq_lock:
            if len(self.wq) < len(self.threads) * 2:
                for _ in range(len(self.threads)):
                    self.wq.append(("batch",))
                self.wq_lock.notify()

    def start(self):
        for t in self.threads:
            t.start()


# ---------------- poll loop -------------------------------------------------

import sqlite3


def _default_state_db():
    """Resolve the Hermes state.db the way Hermes itself does.

    Guessing "~/.hermes" is wrong on any install where HERMES_HOME points
    elsewhere (this instance: /opt/data). Hermes' own resolver is
    authoritative — use it, and fall back to the env var.
    """
    try:
        from hermes_constants import get_hermes_home
        return str(get_hermes_home() / "state.db")
    except Exception:
        home = os.environ.get("HERMES_HOME", "").strip() or os.path.join(
            os.path.expanduser("~"), ".hermes")
        return os.path.join(home, "state.db")


def fetch_rows(db_path, sid, min_id):
    db = sqlite3.connect(DB_RO.format(db=db_path), uri=True)
    try:
        return db.execute(
            "SELECT id, role, content, tool_name, tool_calls, timestamp, active"
            " FROM messages WHERE session_id=? AND id>? ORDER BY id", (sid, min_id)).fetchall()
    finally:
        db.close()


def fetch_chat_id(db_path, sid):
    db = sqlite3.connect(DB_RO.format(db=db_path), uri=True)
    try:
        r = db.execute("SELECT chat_id FROM sessions WHERE id=?", (sid,)).fetchone()
        return r[0] if r else None
    finally:
        db.close()


def check_fork(db_path, sid, chat_id):
    """V3(2): newer session on the same chat → FORK marker (keep shadowing old)."""
    if chat_id is None:
        return None
    db = sqlite3.connect(DB_RO.format(db=db_path), uri=True)
    try:
        r = db.execute(
            "SELECT id, started_at FROM sessions WHERE chat_id=? AND source='telegram' AND id<>?"
            " ORDER BY started_at DESC LIMIT 1", (chat_id, sid)).fetchone()
        return r[0] if r else None
    finally:
        db.close()


def owner_claim(root):
    """V4: single-writer lockfile."""
    p = os.path.join(root, "owner.lock")
    try:
        fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        return True
    except FileExistsError:
        return False


def serve(args):
    global LOG_PATH
    default_root = os.environ.get("TREE_MEM_ROOT", "").strip() or os.path.join(_hermes_home(), "tree-mem")
    root = args.root or os.path.join(default_root, args.session)
    os.makedirs(root, exist_ok=True)
    LOG_PATH = os.path.join(root, "logs", "serve.log")
    if not owner_claim(root):
        log("FATAL: owner.lock exists — another writer owns this store (V4). Exiting.")
        sys.exit(3)
    store = Store(root)
    summ = Summarizer(args.interval)
    rt = Runtime(store, summ, args.workers)
    rt.start()
    log(f"serve start: session={args.session} root={root} "
        f"counter={store.counter} last_ingest_id={store.last_ingest_id} "
        f"nodes={len(store.nodes)} view={len(store.view)}")
    # Durable-queue reconstruction: every ingested unit without a leaf node gets re-queued.
    # (Node writes are the queue's persistence; in-memory queue loss is recoverable.)
    pending = [i for i in range(store.counter)
               if str(i) in store.units_meta and (0, i) not in store.nodes
               and store.units_meta[str(i)].get("kind") != "work"]
    for i in pending:
        rt.push_task(("releaf", i))
    log(f"requeued {len(pending)} outstanding leaves")
    chat_id = fetch_chat_id(args.db, args.session)
    log(f"chat_id={chat_id}")
    fork_announced = None
    while True:
        try:
            # The heartbeat: if the view is over budget, make sure the
            # batch-merge is queued. Without this the merge only ever ran as
            # a side effect of a leaf task, and stopped the moment the leaves
            # were done.
            rt.schedule_merge_if_needed()
            rows = fetch_rows(args.db, args.session, store.last_ingest_id)
            # V3(3): rewind/gap detection — any new row with id ≤ a seen id plus a gap
            if rows:
                ids = [r[0] for r in rows]
                min_new = min(ids)
                # Rewind = new rows whose ids are entirely BELOW what we ingested.
                # (Reject partial-overlap as routine; only full regression is a rewind.)
                if store.last_ingest_id and all(i <= store.last_ingest_id for i in ids):
                    store.append_marker(f"REWIND DETECTED at {min_new} (last={store.last_ingest_id})")
                    # advance ingest pointer past these to avoid looping
                    store.last_ingest_id = max(store.last_ingest_id, max(ids))
                    store.save_signals()
                    time.sleep(60)
                    continue
                units = group_rows(rows)
                tasks = store.append_units(units)
                for t in tasks:
                    rt.push_task(("leaf",) + t)
                log(f"ingested {len(units)} units (counter={store.counter})")
            if (fork_announced is None) or (time.time() % 600 < 10):
                newer = check_fork(args.db, args.session, chat_id)
                if newer and newer != fork_announced:
                    store.append_marker(f"FORK {args.session} -> {newer} (shadowing original)")
                    fork_announced = newer
            time.sleep(args.poll)
        except KeyboardInterrupt:
            log("stop requested")
            break
        except Exception as e:
            log(f"POLL-ERR: {type(e).__name__} {str(e)[:150]}")
            time.sleep(10)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["serve"])
    ap.add_argument("--session", required=True)
    ap.add_argument("--db", default=os.environ.get("TREE_MEM_DB", _default_state_db()))
    ap.add_argument("--root", default=None)
    ap.add_argument("--poll", type=float, default=10.0)
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()
    serve(args)


if __name__ == "__main__":
    main()
