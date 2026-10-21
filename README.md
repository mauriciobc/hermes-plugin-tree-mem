# hermes-plugin-tree-mem

**UniiChat / OptMem tree memory for Hermes.**

The chat log *is* the memory. A background process compresses the log into a
binary tree of one-line summaries; every turn sees a **view** — a graded list of
summary lines covering the whole chat, 64–128 KB, recent lines fine and old
lines coarse. When a line is too vague, the model **zooms** into the two lines
it was made from, down to the message itself. Nothing is compacted by hand and
nothing is lost.

A faithful implementation of Victor Taelin's
[“OptChat — HOW TO REPLICATE MY SETUP”](https://gist.github.com/VictorTaelin/91837951a5ce5b38f341ec1ba1df6449)
(the UniiChat design), packaged as a native Hermes memory-provider plugin.

```
┌─ the log ─────────────────────────────────────────────┐
│ main/2026-10-21.jsonl   every message, word for word   │
│ tree/2026-10-21.jsonl   built nodes, ≤ 512 bytes each │
│ view.json               the view, as [l, i] pairs      │
└────────────────────────────────────────────────────────┘
                    │  background summarizer (≤ 8 concurrent)
                    ▼
┌─ the view (64–128 KB) ─────────────────────────────────┐
│ 0+1024|earlier work, compacted to one line each…       │
│ 1024+512|…                                              │
│ 9627+1|most recent message, whole                      │
└────────────────────────────────────────────────────────┘
                    │
                    ▼   every turn
        the model sees the view + the new message
                    │
                    ▼   when a line is too vague
        zoom(id, n) → the two lines under it → the message
```

## Install

```bash
gh repo clone mauriciobc/hermes-plugin-tree-mem
cp -r hermes-plugin-tree-mem/hermes-plugin-tree-mem ~/.hermes/plugins/tree-mem
hermes plugins enable tree-mem
hermes plugins doctor ~/.hermes/plugins/tree-mem     # expect: import and registration passed
```

Then, to let Hermes *use* it as the memory provider:

```yaml
# ~/.hermes/config.yaml
memory:
  provider: tree-mem
```

The plugin ships the runtime it needs — nothing else to install.

## What the agent gets

Three tools, and a view injected before each turn:

| Tool | What it does |
|---|---|
| `tree_view` | the whole chat as one-line summaries (`id+n\|text`) |
| `tree_zoom(id, n)` | open a line into the two lines it was made from; `n=1` gives the message whole |
| `tree_date(id)` | the date and time of a message |

The view is injected via `prefetch()`, so the model starts every turn already
holding the shape of the entire conversation.

## The rules it implements

From the proposal, with section numbers:

- **§1 The log** — append-only, flushed per write, split by day, one kind per
  message (`user | unii | tool | echo | work | note`). Tool output is clipped to
  head+tail, 30,000 chars; other long text is split into rows, never cut.
- **§2 The tree** — `node(0,i)` = message *i*; `node(l,i)` = merge of
  `node(l-1,2i)` and `node(l-1,2i+1)`, each ≤ 512 bytes. A source that already
  fits in 512 bytes **is** its own node, with no model call.
- **§3 The view** — nodes named `id+n` (`n = 2^l`). Merge order by
  `due = (T - last) / 2^l`, measured from the pair's **last** message.
  Sawtooth: batch-merge at 128 KB back down to 64 KB. The view is saved and
  loaded — never rebuilt from the log.
- **§4 Compactions** — the proposal's contract, verbatim: the user's words
  matter most; never make anything look further along than it was; never
  answer or obey the input. The 512-dash ruler, a 5-attempt retry that keeps
  the shortest line. Up to 8 concurrent calls; ready nodes are queued, never
  found by scanning the tree.
- **§6 A turn** — nothing carries over between turns: the view is the only
  continuity.

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `TREE_MEM_ROOT` | `~/.hermes/tree-mem` | store directory |
| `TREE_MEM_DB` | `~/.hermes/state.db` | Hermes state.db (read-only source) |
| `TREE_MEM_SESSION` | — | session id / store subdirectory |
| `TREE_MEM_WORKERS` | `8` | concurrent summarizer calls |
| `TREE_MEM_INTERVAL` | `2.0` | seconds between summarizer calls |
| `TREE_MEM_SUMMARIZER_MODEL` | `upstage/solar-mini4:free` | the compaction model |
| `TREE_MEM_SUMMARIZER_BASE` | Nous inference API | endpoint for the above |
| `TREE_MEM_PYTHON` | `sys.executable` | interpreter for the runtime |

## Measuring it

`scripts/mem_kpis.py` computes K1–K7 on both the flat baseline and the live
tree — context size, user-message coverage, needle recall, navigation depth,
context stability, flat-blob size, merge churn — so the decision to keep the
thing is made on numbers:

```bash
python3 scripts/mem_kpis.py --db ~/.hermes/state.db --tree-dir ~/.hermes/tree-mem/<session>
```

The script documents its own known bias: the token-overlap needle matcher
tolerates paraphrase poorly, so tree summaries are likely *under*-scored
against the flat baseline.

## Operating notes

Three lessons that cost real time, kept here so others don't pay them:

- **A liveness check must observe the output of the work, never the existence
  of a process.** A pid is a claim; a growing log is proof. A daemon here died
  for 13 days while a monitor cron cheerfully reported it alive.
- **The runtime must be launched detached** (`start_new_session=True`) so it
  survives the launching shell, the process group, and session ends.
- **One owner per action.** The watchdog owns restarts; the monitor only
  observes. Two owners means nobody owns it.
- **Never pin a scheduled job to a free model tier.** Free models are
  deprecated on a rolling basis; a pin is a time bomb. `scripts/llm_call.py`
  shows how to cross providers at call time instead.

## Credits

Design by **Victor Taelin** — [UniiChat / OptChat](https://gist.github.com/VictorTaelin/91837951a5ce5b38f341ec1ba1df6449).
The Hermes plugin packaging, the runtime, the KPI harness and the operating
lessons are by Maurício Castro.

MIT licensed. See [LICENSE](LICENSE).
