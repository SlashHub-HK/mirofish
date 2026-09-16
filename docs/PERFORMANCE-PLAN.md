# MiroFish performance, scalability & memory plan

**Context.** MiroFish shares a **24 GB host** with the rest of the project. A
single "Quick" run (10 entities / 10 rounds) was observed peaking at **~6 GB** and
then failing. That is not survivable on a shared box — the kernel OOM-killer
takes out the whole container, not one request.

**Goal.** A run whose peak memory is *predictable and bounded*, that degrades
honestly (a clear error, never a dead host) when capacity is exceeded, and that
can serve more than one user without duplicating the agent stack per request.

## Status legend

- **DONE** — implemented and verified in this repo
- **PLANNED** — specified below, not yet implemented
- **DECIDED-AGAINST** — considered and deliberately not done, with the reason

---

## 1. Root causes (measured / verified)

| # | Cause | Evidence |
| --- | --- | --- |
| C1 | A `kuzu.Database` was opened **per query**, never closed, with **no buffer cap** | `graph_db.py:_connect`; Kuzu docs: default pool is ~80% of *total physical RAM* and does **not** read the container cgroup limit |
| C2 | `search()` loaded **every** node and edge into Python, once per entity during prepare | `graph_db.search` called `get_all_edges` + `get_all_nodes` |
| C3 | The platform DB was **deleted unconditionally** at simulation start | `run_parallel_simulation.py` — so a "resume" lost all world state |
| C4 | The round loop always started at **round 0** | `for round_num in range(total_rounds)` |
| C5 | Flask registries (`_monitor_threads`, `_run_states`, `TaskManager._tasks`) were **never evicted** | growth across runs until restart |
| C6 | Both OASIS platforms (Twitter + Reddit) run in **one subprocess** | `platform: 'parallel'`, `asyncio.gather` — ~2× per-entity peak |
| C7 | `camel-oasis` pulls PyTorch; the Twitter recommender lazily loads **TWHIN-BERT (~0.5–1 GB)** per simulation subprocess | `oasis.make(platform=TWITTER)`; `recsys.py` |
| C8 | **Nothing bounded concurrency** — N concurrent prepares/reports/simulations each built a full agent stack | `start_simulation` only rejected a duplicate of the *same* id |

---

## 2. DONE (verified)

| Fix | Addresses | Verification |
| --- | --- | --- |
| One cached, **memory-capped** `kuzu.Database` per graph (`KUZU_BUFFER_POOL_MB=256`, `KUZU_MAX_NUM_THREADS=4`); `Connection` per call; close+evict on `delete_graph`; `close_all_databases()` via `atexit` | C1 | Ran against real kuzu 0.11.3; log shows `buffer_pool=256MB, threads=4, open=1`; `open dbs after delete: 0` |
| `search()` filters **in Kuzu** (`WHERE … concat(...)`) instead of materialising the graph | C2 | All scopes/case/limit/empty/whitespace cases executed against real kuzu; row set proven identical |
| Platform DB is only wiped when `resume_from == 0` | C3 | OASIS source inspected: **no `DROP TABLE`**, `sign_up()` swallows duplicate-INSERT and returns `success:False` ⇒ preserving it is safe |
| Round loop resumes at `last_completed_round()` | C4 | Helper unit-exercised (crash mid-round → correct watermark; junk lines tolerated) |
| `_monitor_threads.pop` in `finally`; finished `_run_states` trimmed to the newest 50 (live runs never evicted); shutdown clears both | C5 | Executed: bounded at 50, live run survived, oldest evicted |
| `cleanup_old_tasks()` now runs (hourly, on the write path) | C5 | Executed: fires once, then throttles |
| `resource_guard`: memory preflight (`MIROFISH_MIN_FREE_MB=800`), process-wide heavy-stage gate, and `MAX_CONCURRENT_SIMULATIONS=1` | C8 | Executed: refusal, allowance, disabled, **fail-open**, peak concurrency 1 across 5 threads, slot refusal, truthful process count |

**What "resume" honestly means now.** The simulated clock and the *social world*
(posts, comments, likes, follows, traces) continue. Each agent's private
conversation memory does **not** — `SocialAgent` has no memory argument, so camel
uses in-process `InMemoryKeyValueStorage`. That cannot survive a restart by
design; it is not a regression, and it is still strictly better than replaying
from round 0 into an empty world.

---

## 3. PLANNED (in priority order)

### P1 — Bound the report/insight read paths

`SimulationRunner.get_all_actions` reads the **entire** `actions.jsonl` for both
platforms, builds `AgentAction` objects, sorts, and only then paginates. Every
poll pays full-file cost. `kuzu_tools.py` has the same shape
(`get_all_nodes`/`get_all_edges`/`panorama_search` over whole tables).

- Stream the file honouring `offset`/`limit` while scanning (no full list).
- Push `LIMIT`/`WHERE` into the Kuzu queries used by `panorama_search`,
  `insight_forge` and `get_simulation_context`.
- Cap `get_console_log` / `get_agent_log` (they read whole files at
  `from_line=0`).

*Risk: low. These are read paths with no shared mutable state.*

### P2 — Make the platform split a real knob

Today `platform: 'parallel'` runs both OASIS envs in one process (~2× peak, C6).
Make it configurable (`MIROFISH_SIM_PLATFORM`, default unchanged) so the operator
can trade wall-clock for peak memory on a constrained host, and log the choice.

*Risk: medium — changes simulation behaviour, so it stays opt-in.*

### P3 — Cut the fixed per-subprocess model cost

TWHIN-BERT (~0.5–1 GB) is loaded because the Twitter platform defaults to
`recsys_type='twhin-bert'`. Options, in order of preference:

1. Make the recommender selectable and **document the quality/memory trade-off**
   (a lexical or random recommender removes the model load entirely).
2. Keep TWHIN-BERT but load it once per *host* rather than per subprocess
   (requires the simulate worker to be a long-lived process — see P5).

*Risk: high for simulation fidelity. Needs a product decision, not an
engineering one — do not change the default silently.*

### P4 — Publish the memory contract

- Expose the guard state on `/health`: free MB, heavy stages active, running
  simulations, Kuzu pools open. That makes capacity observable and lets
  SlashMarketer (or an operator) see *why* a run was refused.
- Add the knobs to `DEPLOY`-style docs and to the deploy config with the values
  that fit a 24 GB shared host.

*Risk: low.*

### P5 — Long-lived simulate worker (largest structural win)

Each run currently spawns a fresh Python process that re-imports camel/oasis and
re-loads any models. A persistent worker pool would amortise import + model load
across runs, and make "warm" starts seconds instead of tens of seconds.

*Risk: high. Needs process supervision, health checks, and a story for a wedged
worker. Only worth it once P1–P3 are done and the profile says imports dominate.*

### P6 — Set Kuzu's cap by measurement, not guess

`KUZU_BUFFER_POOL_MB=256` is a safe default, not a tuned one. Once the host
telemetry in P4 exists, size it from the observed working set (and note that the
pool is a ceiling, not a preallocation).

*Risk: low.*

---

## 4. DECIDED-AGAINST

- **Multiprocessing the in-process prepare stage.** It is I/O/LLM-bound; the
  profiles are threaded at `parallel_profile_count` already. Serialising it (C8
  guard) removes the stacking risk without the complexity.
- **Auto-scaling the entity budget from available RAM.** Silent quality changes
  are worse than a clear refusal; the ceiling is explicit (40/40) and the UI
  states it.
- **Vendoring MiroFish's frontend/static assets.** This is a headless backend;
  SlashMarketer owns the UI. Keeps the image small and the surface minimal.

---

## 5. How to verify any change here

There is no test suite in this repo, so changes are verified by execution:

1. `python -m compileall -q backend` — everything parses.
2. Import check for touched modules (the app cannot be fully imported locally
   without camel/torch; see the note below).
3. Targeted harnesses against the **real** dependency where one exists — e.g.
   `kuzu` for the graph/search work (that is how the unused-parameter bug and the
   `||`-vs-`concat` difference were caught).
4. Confirm the *failure* case too: a guard that cannot be made to refuse is not
   verified.

**Known limitation:** `camel-oasis`/`torch` are not installable in this
environment, so `app.tools.*` and `app.api.compat` cannot be imported here. Their
changes are covered by (1) plus an AST check that every imported symbol exists in
its source module.
