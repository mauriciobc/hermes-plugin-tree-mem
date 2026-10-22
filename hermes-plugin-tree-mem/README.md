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

## Operating notes (learned the hard way)

- **An empty source is a fact, not an error.** Assistant turns with no content
  have no summary; retrying them forever starved the merge queue and left the
  view at 1.3 MB. They are recorded as `(empty message — no content)`.
- **The merge scheduler needs a heartbeat.** Batch-merge used to start only as
  a side effect of finishing a leaf, so it never ran once the leaves were done.
  The poll loop now schedules it.
- **Batch-merge is a queue task, not inline work.** Done inline it pinned one
  worker to the whole job; as a queue task all 8 workers share the load.
- **The client-side rate limit protects the free tier.** 0.25 s between calls
  tripped the provider's HTTP 429 — and the 512-byte retry ladder makes up to
  6 sequential calls per merge. The floor is 0.75 s.
- **429 is "not now", not failure.** It is retried with backoff; treating it
  as a failure stalled the queue.
- **Views are capped, never whole.** Over budget, the newest lines are kept
  and the truncation is stated in the output.

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
│   ├── tree-mem-mcp-server.py  # the tools as a standalone MCP server
│   ├── mem_kpis.py          # KPI verifier (K1–K7) for the keep-vs-kill gate
│   ├── monitor_tree_mem_backfill.py
│   └── tree_mem_backfill_watchdog.py
└── docs/phase2_tree_memory_spec.md
```

## Two ways to reach the tree

The plugin is the eventual home of tree memory, but it is deliberately
**inactive** (`memory.provider: ''`) until the KPI gate passes — that flip is
Phase 2b live injection. Until then the tree is still reachable, as **tools**:

```
hermes plugins install tree-mem        # the provider (inert until the gate)
python3 scripts/tree-mem-mcp-server.py --root $HERMES_HOME/tree-mem/<session> --port 4310
```

and register it:

```yaml
mcp_servers:
  tree-mem:
    url: http://127.0.0.1:4310/mcp
    transport: streamable
    timeout: 30
    enabled: true
```

A fresh session then sees `tree_view` / `tree_zoom` / `tree_date` and can
reach the whole history on demand — nothing injected, nothing paid per turn.
The server hot-reloads as the tree is built (it parses only newly appended
bytes) and caps the view at 128 KB, keeping the newest lines and saying so.

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

## Upstream compliance

Verified against the live upstream sources on 2026-10-22 (SHA-matched
against the local copies, so the comparison is against the current code, not
a paraphrase):

- `github.com/VictorTaelin/OptMem` — the `memo` tool. Upstream OptMem has
  **no model call**: the agent writes each summary and hands it back
  (`memo nap lo-hi "<your line>"`). The model is the agent.
- `github.com/jonaslsaa/pi-optchat` — the closest runnable implementation:
  `src/prompts.ts` (the COMPACT contract and the compaction task templates),
  `src/memory.ts` (the tree, the view, the merge scheduling), and
  `src/compactor.ts` (the model call).

This plugin implements pi-optchat's *mechanics*, driven by a background
daemon instead of by the agent in the loop.

| upstream | here |
|---|---|
| `NODE = 512` | `LEAF_CAP = 512` |
| `SOURCE = 100_000` | `MERGE_SOURCE_CAP = 100_000` |
| `CLIPPED = '(not summarized: zoom it)'` | `Store.clipped_source()` |
| the contract is the **system** prompt | `{"role": "system", "content": CONTRACT}` |
| leaf task: `Compaction: compress message {i} into one line...` | same |
| merge task: `Compaction: merge lines {a} and {b}, adjacent...` | same |
| the view is framed `'<chat>\n' + lines + '\n</chat>'` | `VIEW_OPEN` |
| the merge context ends at `end(parent)` — the node being built | same |
| a source ≤ `NODE` needs **no** model call | same |
| the reply's `id+n|` head is **stripped** | `Runtime._strip_id_head()` |
| a line must also **shrink** the source it replaces | `enforce_with_retries` |
| `flat()` turns newlines into spaces | `flat()` |
| `due = (T - last) / 2^l` | same |

Two upstream behaviours we had to add late, both found by re-reading the
source rather than by testing: upstream **strips** a copied `id+n|` head
instead of rejecting the reply, and it keeps asking until the line is
**smaller than the source it replaces** ("a merge of two short lines can
come back nearly as big as both").

## Operating notes (learned the hard way)

- **A liveness check must observe the output of the work, never the existence
  of a process.** A pid is a claim; a growing log is proof. The backfill
  daemon died for 13 days while a monitor cron reported it alive, because the
  check read the wrong signal.
- **The runtime must be launched detached** (`start_new_session=True`) so it
  survives the launching shell, the process group, and session ends.
- **One owner per action.** The watchdog owns restarts; the monitor only
  observes. Two owners means nobody owns it.
- **The summarizer uses FREE models only.** The primary and every fallback
  are Nous `:free` models on ONE API key. A paid route is reachable only
  with `TREE_MEM_ALLOW_PAID_FALLBACK=1` set explicitly — it is off by
  default. The free tier's rate limit is **per API KEY**, not per model, so
  rotating models does not dodge a 429; back the whole pool off instead.
  Never probe that key while the daemon is running: the probe steals the
  daemon's budget.
- **Free tiers are a moving target.** The summarizer model is a config value,
  not a hard-coded truth.

## Evaluation

`scripts/mem_kpis.py` measures K1–K7 (context size, user-message coverage,
needle recall, navigation depth, stability, flat-blob size, merge churn) on
both the flat baseline and the live tree, so the keep-vs-kill decision is
made on numbers rather than vibes. Known bias is documented in the script:
the token-overlap matcher tolerates paraphrase poorly, so tree summaries are
likely *under*-scored.
