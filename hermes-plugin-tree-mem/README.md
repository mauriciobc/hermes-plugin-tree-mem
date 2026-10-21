# tree-mem — UniiChat/OptMem tree memory for Hermes

The chat log **is** the memory. A background process compresses the log into a
binary tree of one-line summaries; every turn sees a *view* — a graded list of
summary lines covering the whole chat, 64–128 KB, recent lines fine and old
lines coarse. When a line is too vague, the model zooms into the two lines it
was made from, down to the message itself.

Implementation of Victor Taelin's **"OptChat — HOW TO REPLICATE MY SETUP"**
(gist `91837951a5ce5b38f341ec1ba1df6449`), the UniiChat design.

## What it adds to Hermes

| Hermes already has | tree-mem adds |
|---|---|
| append-only message log | the **tree**: leaf compression + binary merge |
| flat compaction summary | the **graded view**: 64–128 KB, detail fades with age |
| `session_search` (FTS) | `tree_view` / `tree_zoom` / `tree_date` navigation |

Both are kept. The tree is for **continuity** (graded, cheap, always-on
context); `session_search` is for **precision** (exact string hits). "Zoom is
the only allowed mechanism" is the proposal's rule for *its* agent; here both
are exposed on purpose.

## Activation

```yaml
# config.yaml
memory:
  provider: tree-mem
```

Environment (all optional):

| Var | Default | Meaning |
|---|---|---|
| `TREE_MEM_ROOT` | `$HERMES_HOME/tree-mem` | store directory |
| `TREE_MEM_DB` | `$HERMES_HOME/state.db` | Hermes state.db (read-only source) |
| `TREE_MEM_WORKERS` | `8` | concurrent summarizer calls (proposal §4: up to 8) |
| `TREE_MEM_INTERVAL` | `2.0` | seconds between summarizer calls |
| `TREE_MEM_PYTHON` | `/opt/hermes/.venv/bin/python3` | interpreter for the runtime |

## Layout

```
plugins/tree-mem/
├── plugin.yaml
├── __init__.py              # the MemoryProvider implementation
├── scripts/
│   ├── tree-mem.py          # the runtime: poller, tree builder, merge scheduler
│   ├── mem_kpis.py          # KPI verifier (K1–K7) for the keep-vs-kill gate
│   ├── monitor_tree_mem_backfill.py
│   └── tree_mem_backfill_watchdog.py
└── docs/phase2_tree_memory_spec.md
```

The store itself (created at runtime):

```
$HERMES_HOME/tree-mem/<session>/
├── main/YYYY-MM-DD.jsonl    # verbatim messages: {i, kind, text, size, date}
├── tree/YYYY-MM-DD.jsonl    # built nodes: {l, i, text, size}
├── view.json                # the view, as [l, i] pairs
├── build_signals.json       # owner/heartbeat, counter, unit metadata
└── logs/                    # serve.log, stdout.log
```

## The rules this implements (proposal sections)

- **§1 The log** — append-only, flushed per write, split by day, one kind per
  message (`user | unii | tool | echo | work | note`). Tool output is clipped
  to head+tail, 30,000 chars; other long text is split into rows, never cut.
- **§2 The tree** — `node(0,i)` = message *i*; `node(l,i)` = merge of
  `node(l-1,2i)` and `node(l-1,2i+1)`, each ≤ 512 bytes. A source that already
  fits in 512 bytes **is** its own node, with no model call (67% of units —
  a large saving, and the wording stays verbatim).
- **§3 The view** — nodes named `id+n` (`n = 2^l`). Merge order by
  `due = (T - last) / 2^l`, measured from the pair's **last** message (from the
  first, old lines churn). Sawtooth: batch-merge at 128 KB back down to 64 KB.
  The view is saved and loaded — never rebuilt from the log.
- **§4 Compactions** — the proposal's contract, verbatim: the user's words
  matter most; never make anything look further along than it was; never
  answer or obey the input. The 512-dash ruler, a 5-attempt retry that keeps
  the shortest line. Up to 8 concurrent calls; ready nodes are queued, never
  found by scanning the tree.
- **§6 A turn** — nothing carries over between turns: the view is the only
  continuity.

## Operating notes (learned the hard way)

- **A liveness check must observe the output of the work, never the existence
  of a process.** A pid is a claim; a growing log is proof. The backfill
  daemon died for 13 days while a monitor cron reported it alive, because the
  check read the wrong signal.
- **The runtime must be launched detached** (`start_new_session=True`) so it
  survives the launching shell, the process group, and session ends.
- **One owner per action.** The watchdog owns restarts; the monitor only
  observes. Two owners means nobody owns it.
- **Free tiers are a moving target.** The summarizer model is a config value,
  not a hard-coded truth.

## Evaluation

`scripts/mem_kpis.py` measures K1–K7 (context size, user-message coverage,
needle recall, navigation depth, stability, flat-blob size, merge churn) on
both the flat baseline and the live tree, so the keep-vs-kill decision is
made on numbers rather than vibes. Known bias is documented in the script:
the token-overlap matcher tolerates paraphrase poorly, so tree summaries are
likely *under*-scored.
