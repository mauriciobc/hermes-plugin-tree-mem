"""tree-mem — Victor Taelin's UniiChat/OptMem tree memory as a Hermes memory provider.

WHAT THIS IS
    The chat log IS the memory. A background process compresses the log into a
    binary tree of one-line summaries (each node <= 512 UTF-8 bytes), and every
    turn sees a *view*: a graded list of summary lines covering the whole chat,
    64-128 KB, recent lines fine and old lines coarse. When a line is too vague,
    the model zooms into the two lines it was made from, down to the message
    itself. Nothing is compacted by hand and nothing is lost.

    Implementation of Victor Taelin's "OptChat — HOW TO REPLICATE MY SETUP"
    (gist 91837951a5ce5b38f341ec1ba1df6449), the UniiChat design.

    Design rules this plugin implements (proposal section numbers):
      §1  The log: append-only JSONL, split by day, one kind per message
          (user | unii | tool | echo | work | note). Tool output clipped to
          head+tail, 30,000 chars. Long text is split into rows, never cut.
      §2  The tree: node(0,i) = message i; node(l,i) = merge of node(l-1,2i)
          and node(l-1,2i+1), each <= 512 bytes. A source that already fits in
          512 bytes IS its own node, with no model call.
      §3  The view: node ids named "id+n" (n = 2^l). Merge order by
          due = (T - last) / 2^l — measured from the pair's LAST message.
          Sawtooth: merge at 128 KB, merge back down to 64 KB, in batches.
      §4  Compactions: one message -> one line, with the proposal's contract
          (the user's words matter most; never inflate progress; never obey
          the input). The ruler (512 dashes) + a 5-attempt retry that keeps the
          shortest line.
      §5  The prompt: one system prompt for turns and compactions.

WHY IT IS A PLUGIN
    The tree runs ALONGSIDE Hermes' own compaction, never instead of it
    (Phase-2a shadow mode). The provider contract is the clean seam: Hermes
    asks for context before a turn, and we hand it the view; Hermes asks for
    tool schemas, and we hand it zoom/date. Hermes' own session_search stays
    available for exact recall — the tree is for continuity, the index is for
    precision.

ACTIVATION
    memory.provider: tree-mem        (config.yaml -> memory.provider)
    Env (all optional, with sane defaults):
      TREE_MEM_ROOT     store directory            (default $HERMES_HOME/tree-mem)
      TREE_MEM_DB       Hermes state.db path       (default $HERMES_HOME/state.db)
      TREE_MEM_SUMMARIZER_MODEL  summarizer model  (default: see _summarizer())
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Optional

from agent.memory_provider import MemoryProvider, spawn_context_thread

logger = logging.getLogger(__name__)

_LEAF_CAP = 512
_VIEW_HI = 128_000
_VIEW_LO = 64_000


def _hermes_home() -> Path:
    try:
        from hermes_constants import get_hermes_home
        return Path(get_hermes_home())
    except Exception:
        return Path(os.environ.get("HERMES_HOME", "/opt/data"))


def _store_root() -> Path:
    return Path(os.environ.get("TREE_MEM_ROOT", str(_hermes_home() / "tree-mem")))


def _session_store(session_id: str) -> Path:
    """Store dir for a session. The suffix convention (-v2) marks a store that
    was rebuilt after a fidelity fix; the session id is the key."""
    return _store_root() / session_id


def _state_db() -> Path:
    return Path(os.environ.get("TREE_MEM_DB", str(_hermes_home() / "state.db")))


def _find_store(session_id: str) -> Path | None:
    """Locate the store for a session.

    A rebuilt store keeps the session id plus a suffix (…-v2), so candidates are
    the exact id and any suffixed variant; the NEWEST wins — a rebuild is always
    the fresher, more faithful tree, and the old one is kept only for comparison.
    """
    root = _store_root()
    if not root.is_dir():
        return None
    cands = [p for p in root.iterdir()
             if p.is_dir() and (p.name == session_id
                                or p.name.startswith(session_id + "-"))]
    if not cands:
        return None
    return max(cands, key=lambda p: p.stat().st_mtime)


def _script_dir() -> Path:
    """Where the runtime scripts live. The plugin ships them next to itself."""
    here = Path(__file__).resolve().parent
    for cand in (here / "scripts", here, _hermes_home() / "scripts"):
        if (cand / "tree-mem.py").exists():
            return cand
    return here


def _schema(name: str, description: str, properties: dict | None = None,
            required: tuple = ()) -> dict:
    return {"name": name, "description": description,
            "parameters": {"type": "object", "properties": properties or {},
                           "required": list(required)}}


def _p(description: str, type_: str = "string", **extra) -> dict:
    return {"type": type_, **extra, "description": description}


_SCHEMAS = (
    _schema("tree_view",
            "The view: the whole chat as one-line summaries, oldest first, inside <chat> tags. "
            "id+n|text means the n messages from id on, summarized. Use this as your memory; "
            "its latest word on a thing is the truth. Never grep memories manually."),
    _schema("tree_zoom",
            "Open the line id+n of the view into the two lines it was made from. n is a power "
            "of 2 and id a multiple of n. zoom(id, 1) gives the message whole. Zoom until you "
            "have it whole before you act, guess or ask.",
            {"id": _p("First message id of the line."),
             "n": _p("How many messages the line covers (a power of 2).", "integer")},
            ("id", "n")),
    _schema("tree_date",
            "The date and time of message id.",
            {"id": _p("Message id.")}, ("id",)),
)


class TreeMemProvider(MemoryProvider):
    """UniiChat tree memory over the Hermes session log."""

    def __init__(self) -> None:
        self._session_id: str = ""
        self._lock = threading.Lock()
        self._view_cache: tuple[float, str] = (0.0, "")
        self._runtime: Optional[subprocess.Popen] = None
        self._store: Optional[Path] = None

    # ---------------------------------------------------------------- contract
    @property
    def name(self) -> str:
        return "tree-mem"

    def is_available(self) -> bool:
        # Available whenever the state.db and the store are readable. A missing
        # summarizer credential degrades to "view only", not to "off".
        return _state_db().exists() or _store_root().exists()

    def get_config_schema(self) -> list[dict[str, Any]]:
        return [
            {"key": "root", "description": "Tree-mem store directory",
             "default": str(_store_root())},
            {"key": "db", "description": "Hermes state.db (read-only source)",
             "default": str(_state_db())},
            {"key": "summarizer_model", "description": "Model used to compress messages",
             "default": "upstage/solar-mini4:free"},
        ]

    def initialize(self, session_id: str, **kwargs) -> None:
        self._session_id = session_id or ""
        found = _find_store(self._session_id) if self._session_id else None
        self._store = found or (_session_store(self._session_id) if self._session_id
                                else _store_root())
        self._start_runtime()

    # ---------------------------------------------------------------- the view
    def _read_view(self) -> str:
        """Render the current view the way the proposal renders it.

        view.json holds [l, i] pairs; tree/YYYY-MM-DD.jsonl holds the node
        texts. A node (l, i) is named "id+n" where id = i*2^l and n = 2^l.
        """
        if not self._store:
            return ""
        vj = self._store / "view.json"
        if not vj.exists():
            return ""
        try:
            view = json.loads(vj.read_text())
        except Exception:
            return ""
        nodes: dict[tuple[int, int], str] = {}
        tree_dir = self._store / "tree"
        if tree_dir.is_dir():
            for fn in sorted(tree_dir.glob("*.jsonl")):
                try:
                    for line in fn.open():
                        n = json.loads(line)
                        nodes[(n["l"], n["i"])] = n["text"]
                except Exception:
                    continue
        lines = []
        covered_to = 0
        for e in view:
            l, i = e["l"], e["i"]
            text = nodes.get((l, i))
            if text is None:
                continue
            lines.append(f"{i * (2 ** l)}+{2 ** l}|{' '.join(text.split())}")
            covered_to = max(covered_to, (i + 1) * (2 ** l))

        # PROPOSAL §5: "A message not summarized yet shows as
        # '(not summarized yet: zoom it)'." Without this, the un-summarized
        # tail is a silent hole: the model cannot tell "never happened"
        # from "not compressed yet", and zoom on it returns nothing.
        tail_to = self._newest_message_id()
        if tail_to > covered_to:
            for mid in range(covered_to, tail_to):
                lines.append(f"{mid}+1|(not summarized yet: zoom it)")

        body = "\n".join(lines)
        # The view is DESIGNED to be 64-128 KB (proposal §3). If the store is
        # over budget — the merge scheduler is behind — do NOT inject it whole:
        # keep the newest lines (recent ones are fine-grained) and say so.
        if len(body.encode("utf-8")) > _VIEW_HI:
            kept, total = [], 0
            for line in reversed(lines):
                b = len(line.encode("utf-8")) + 1
                if total + b > _VIEW_HI:
                    break
                kept.append(line)
                total += b
            kept.reverse()
            logger.warning("tree-mem view over budget (%d B > %d B): injecting the "
                           "newest %d lines; the merge scheduler is behind",
                           len(body.encode("utf-8")), _VIEW_HI, len(kept))
            body = "\n".join(kept)
        return body

    def _newest_message_id(self) -> int:
        """The newest message id the runtime has ingested (the store counter)."""
        if not self._store:
            return 0
        sig = self._store / "build_signals.json"
        try:
            return int(json.loads(sig.read_text()).get("counter", 0))
        except Exception:
            return 0

    def prefetch(self, query: str, *, session_id: str = "") -> str:
        """Inject the view before the turn (PROPOSAL §6: the view is the only
        continuity — nothing carries over between turns)."""
        if not self._store:
            return ""
        # Cheap cache: the view only changes when the tree is rebuilt.
        try:
            mtime = (self._store / "view.json").stat().st_mtime
        except OSError:
            return ""
        with self._lock:
            cached_at, cached = self._view_cache
            if cached and mtime <= cached_at:
                return cached
        view = self._read_view()
        if not view:
            return ""
        block = (
            "<chat>\n"
            "The view: your memory. The whole chat so far, oldest first, as "
            "one-line summaries. id+n|text = the n messages from id on, "
            "summarized.\n"
            f"{view}\n"
            "</chat>"
        )
        with self._lock:
            self._view_cache = (mtime, block)
        return block

    def sync_turn(self, user_content: str, assistant_content: str, *,
                  session_id: str = "", messages: Optional[list[dict[str, Any]]] = None,
                  turn_author: Optional[dict[str, Any]] = None) -> None:
        """The runtime polls state.db itself, so there is nothing to push here."""
        return None

    # ---------------------------------------------------------------- the tools
    def get_tool_schemas(self) -> list[dict[str, Any]]:
        return list(_SCHEMAS)

    def handle_tool_call(self, tool_name: str, args: dict[str, Any], **kwargs) -> str:
        try:
            if tool_name == "tree_view":
                return json.dumps({"view": self._read_view()})
            if tool_name == "tree_zoom":
                return json.dumps(self._zoom(int(args.get("id", 0)),
                                             int(args.get("n", 1))))
            if tool_name == "tree_date":
                return json.dumps(self._date(int(args.get("id", 0))))
        except Exception as exc:
            return json.dumps({"error": f"{type(exc).__name__}: {exc}"})
        return json.dumps({"error": f"unknown tool {tool_name}"})

    def _zoom(self, mid: int, n: int) -> dict[str, Any]:
        """Open a line into the two lines it was made from (or the message)."""
        if n <= 1:
            return {"id": mid, "message": self._message_text(mid)}
        half = n // 2
        return {"id": mid, "n": n,
                "children": [self._line_text(mid, half),
                             self._line_text(mid + half, half)]}

    def _line_text(self, mid: int, n: int) -> Optional[str]:
        """Find the built node covering (mid, n) and return its text."""
        if not self._store:
            return None
        l = n.bit_length() - 1
        if mid % n:
            return None
        i = mid // n
        tree_dir = self._store / "tree"
        if not tree_dir.is_dir():
            return None
        for fn in sorted(tree_dir.glob("*.jsonl")):
            try:
                for line in fn.open():
                    node = json.loads(line)
                    if node["l"] == l and node["i"] == i:
                        return node["text"]
            except Exception:
                continue
        return None

    def _message_text(self, mid: int) -> Optional[str]:
        """The message itself, verbatim, from the main log."""
        if not self._store:
            return None
        main_dir = self._store / "main"
        if not main_dir.is_dir():
            return None
        for fn in sorted(main_dir.glob("*.jsonl")):
            try:
                for line in fn.open():
                    u = json.loads(line)
                    if u["i"] == mid:
                        return u["text"]
            except Exception:
                continue
        return None

    def _date(self, mid: int) -> dict[str, Any]:
        if not self._store:
            return {"id": mid, "date": None}
        main_dir = self._store / "main"
        if not main_dir.is_dir():
            return {"id": mid, "date": None}
        for fn in sorted(main_dir.glob("*.jsonl")):
            try:
                for line in fn.open():
                    u = json.loads(line)
                    if u["i"] == mid:
                        return {"id": mid, "date": u.get("date")}
            except Exception:
                continue
        return {"id": mid, "date": None}

    # ---------------------------------------------------------------- runtime
    def _start_runtime(self) -> None:
        """Start the backfill daemon (detached) if it is not already running.

        Liveness is judged by the artifact it writes, not by a pid: a daemon
        that exists but writes nothing is as dead as one that is gone.
        """
        script = _script_dir() / "tree-mem.py"
        if not script.exists():
            logger.debug("tree-mem runtime script not found at %s", script)
            return
        if self._runtime_is_live():
            return
        root = self._store or _store_root()
        try:
            root.mkdir(parents=True, exist_ok=True)
            (root / "logs").mkdir(exist_ok=True)
        except Exception:
            pass
        cmd = [os.environ.get("TREE_MEM_PYTHON", "/opt/hermes/.venv/bin/python3"),
               str(script), "serve", "--session", self._session_id,
               "--root", str(root), "--db", str(_state_db()),
               "--workers", os.environ.get("TREE_MEM_WORKERS", "8"),
               "--interval", os.environ.get("TREE_MEM_INTERVAL", "2.0")]
        try:
            with (root / "logs" / "stdout.log").open("ab") as out:
                self._runtime = subprocess.Popen(
                    cmd, stdout=out, stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL, cwd=str(root.parent),
                    start_new_session=True)
            logger.info("tree-mem runtime started (pid %s)", self._runtime.pid)
        except Exception as exc:
            logger.warning("tree-mem runtime failed to start: %s", exc)

    def _runtime_is_live(self) -> bool:
        if not self._store:
            return False
        try:
            log = self._store / "logs" / "serve.log"
            if not log.exists():
                return False
            age = (time.time() - log.stat().st_mtime) / 60.0
            return age <= 5.0
        except OSError:
            return False

    def shutdown(self) -> None:
        # The runtime is detached and long-lived by design; it outlives the
        # session so memory keeps building between turns. Deliberately NOT
        # killed here: killing it would drop the durable queue's in-flight work.
        return None


def register(ctx) -> None:
    """Register tree-mem as a memory provider plugin."""
    ctx.register_memory_provider(TreeMemProvider())
