# MiroFish (in-repo deployment)

The **simulation engine behind Verify (System 2)**. SlashMarketer drives the A/B
simulation pipeline (`graph → env → simulate → report`) by calling this service
over HTTP; it is not imported by `apps/api`.

## Provenance & licence — READ THIS BEFORE EDITING

- Upstream: [`666ghj/MiroFish`](https://github.com/666ghj/MiroFish) — **AGPL-3.0**.
- This copy is the `main` branch of [`SlashHub-HK/mirofish`](https://github.com/SlashHub-HK/mirofish)
  (a fork carrying the SlashMarketer adapter layer in `backend/app/api/compat.py`),
  plus the local patches listed below.
- `LICENSE` is the upstream AGPL-3.0 text and **must stay in place**.

> **The AGPL boundary.** Upstream is AGPL-3.0, which attaches network-use source
> obligations to derived works. `docs/MIROFISH-INTEGRATION.md` previously stated
> MiroFish is "proxied, never vendored or linked". It now lives in this repo at
> the operator's request, so two rules keep that boundary intact:
>
> 1. **Never import this package from `apps/api`.** The only interface is the
>    HTTP contract below. `apps/api/app/mirofish.py` is the single client.
> 2. **Deploy it as its own service** (`railway.toml` / `Dockerfile` here). It is
>    a sibling app, not a library.
>
> This is an engineering guardrail, not legal advice — treat "does SlashMarketer
> become a derivative work?" as an open question for whoever owns licensing.

## Layout

```
apps/mirofish/
├── Dockerfile            # backend-only image (no Node/Vue/static build)
├── railway.toml          # deploy config; health check on /health
├── LICENSE               # upstream AGPL-3.0 — keep
└── backend/
    ├── requirements.txt
    ├── run.py            # waitress entrypoint, binds PORT
    ├── app/
    │   ├── api/compat.py # ← the SlashMarketer adapter (the integration surface)
    │   └── services/     # graph_db, simulation_runner, oasis_profile_generator, …
    └── scripts/          # run_parallel_simulation.py (the simulate worker)
```

## The contract SlashMarketer speaks

`apps/api/app/mirofish.py` (thin whitelisted proxy) + `apps/api/app/experiments.py`
(orchestration). Full list in [`docs/MIROFISH-INTEGRATION.md`](../../docs/MIROFISH-INTEGRATION.md).

```
POST /api/projects                        create (title, description, seed_text, target_entities)
POST /api/projects/expand                 one shared A/B world
POST /api/projects/{pid}/graph            ontology + graph
POST /api/projects/{pid}/simulation/prepare
POST /api/projects/{pid}/simulation/run   { rounds?, force? }
GET  /api/projects/{pid}/simulation/run/status
POST /api/projects/{pid}/report/generate
GET  /api/projects/{pid}/env-status
POST /api/projects/{pid}/interview | /report/chat
GET  /api/projects/{pid}/insight/{resource}
```

## Local patches on top of the fork

Each is a deliberate behaviour/memory fix; keep the comments that explain them.

| File                                                              | Patch                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ----------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `app/services/graph_db.py`                                        | **Kuzu handles are cached and memory-capped.** A `kuzu.Database` was constructed per query and never closed; Kuzu sizes each instance's buffer pool to ~80% of _total physical RAM_ and does not read the container's cgroup limit, so a 40-entity run opened ~100 pools entitled to ~19 GB each on a 24 GB host. Now one `Database` per graph via `_open_database()`, `KUZU_BUFFER_POOL_MB` (256) / `KUZU_MAX_NUM_THREADS` (4), a fresh `Connection` per call, and `_close_database()` on `delete_graph`. |
| `app/services/graph_db.py`                                        | **`search()` filters in Kuzu.** It used to load _every_ node and edge into Python (`get_all_edges` + `get_all_nodes`) and score them; prepare searches once per entity. Now a Cypher `WHERE … concat(...)` narrows the rows first. Row set is identical (a non-matching row scores 0 and can never reach `limit`).                                                                                                                                                                                         |
| `scripts/action_logger.py`                                        | `last_completed_round()` — reads the highest finished round from `actions.jsonl`, streaming so a long log is never loaded whole.                                                                                                                                                                                                                                                                                                                                                                           |
| `scripts/run_parallel_simulation.py`                              | **Resumes instead of replaying.** The round loop now starts at `last_completed_round(...)`; `round_num` drives the simulated clock and agent wake schedule, so restarting at 0 both wasted work and re-simulated the same hours.                                                                                                                                                                                                                                                                           |
| `app/config.py`                                                   | `KUZU_BUFFER_POOL_MB` / `KUZU_MAX_NUM_THREADS` knobs (see above).                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `app/__init__.py`                                                 | `atexit` → `close_all_databases()` so a restart releases pools instead of briefly holding two full sets.                                                                                                                                                                                                                                                                                                                                                                                                   |
| `app/services/simulation_runner.py`                               | **Registry eviction.** `_monitor_threads` was never popped (every run leaked a Thread); `_run_states` grew one entry per run forever. Now the thread entry is dropped in the monitor's `finally`, finished run states are trimmed to the newest 50 (live runs are never evicted — they are what the API polls), and shutdown clears both.                                                                                                                                                                  |
| `app/core/task_manager.py`                                        | `cleanup_old_tasks()` existed but was **never called**, so every completed task's result and JSON file were retained indefinitely. Now pruned opportunistically on the write path, at most hourly.                                                                                                                                                                                                                                                                                                         |
| `app/core/resource_guard.py` (new)                                | **Capacity controls.** A memory preflight (`MIROFISH_MIN_FREE_MB`, default 800) refuses heavy work when the host is short rather than letting the OOM-killer take the container; a process-wide gate (`MAX_CONCURRENT_HEAVY_STAGES`, default 1) serialises prepare/report; `MAX_CONCURRENT_SIMULATIONS` (default 1) caps concurrently running sims. Fails **open** when free memory is unknown.                                                                                                            |
| `app/tools/run_simulation.py`                                     | Applies the memory preflight + simulation-slot cap before spawning a worker.                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `app/tools/prepare_simulation.py`, `app/tools/generate_report.py` | Run under the heavy-stage gate with the memory preflight.                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `app/api/compat.py`                                               | Maps a capacity refusal to **503** (not 502) — SlashMarketer treats 503 as transient, so it backs off and retries instead of failing the arm.                                                                                                                                                                                                                                                                                                                                                              |
| `backend/scripts/run_parallel_simulation.py`                      | **Resume is now real:** the platform DB is only wiped when starting fresh (`resume_from == 0`). Previously it was deleted unconditionally, so a resumed run kept its round counter but lost the entire social world.                                                                                                                                                                                                                                                                                       |

### `force` semantics (important)

`POST /simulation/run { force: true }` calls `cleanup_simulation_logs()`, which
**deletes `twitter_simulation.db`, `reddit_simulation.db`, `actions.jsonl`,
`run_state.json` and `env_status.json`** — i.e. a restart from round 0 that loses
all agent memory. SlashMarketer therefore sends `force` **only** for an explicit
user re-simulate, never for a self-heal.

## Running locally

```bash
cd apps/mirofish/backend
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python run.py            # PORT / FLASK_PORT, default 5001
curl localhost:5001/health
```

Point SlashMarketer at it with `MIROFISH_BASE_URL=http://localhost:5001`.

## Capacity controls (env)

| Var                           | Default | Effect                                                         |
| ----------------------------- | ------- | -------------------------------------------------------------- |
| `MIROFISH_MIN_FREE_MB`        | 800     | Refuse heavy stages below this much free memory (`0` disables) |
| `MAX_CONCURRENT_HEAVY_STAGES` | 1       | Prepare/report stages allowed at once, per process             |
| `MAX_CONCURRENT_SIMULATIONS`  | 1       | Simulation subprocesses allowed at once                        |
| `KUZU_BUFFER_POOL_MB`         | 256     | Kuzu buffer pool ceiling **per graph**                         |
| `KUZU_MAX_NUM_THREADS`        | 4       | Kuzu threads per graph                                         |

> `/proc/meminfo` is Linux-only. On macOS (local dev) the memory preflight is
> **inactive** and logs a warning once — it fails open rather than guessing.

## Remaining work

See [`docs/PERFORMANCE-PLAN.md`](docs/PERFORMANCE-PLAN.md) for the prioritised
plan (P1–P6), the verified root causes, and what was deliberately not done.

## Known remaining work (audited, not yet done)

- `camel-oasis` pulls `sentence-transformers`/PyTorch and the Twitter recommender
  lazily loads `Twitter/twhin-bert-base` (~0.5–1 GB) per simulation subprocess.
  Making the recommender selectable is a **product-quality decision**, not an
  engineering one — it is deliberately left alone.
- `/run-status/detail` still returns `all_actions` (the full history) and calls
  `get_all_actions` four times. SlashMarketer does not read it — only upstream's
  own UI does — so it is lower priority, but it is a hazard if ever polled.
- Several `kuzu_tools.py` paths still materialise whole tables (`get_all_*`).
- No retention/GC for old simulation directories or graphs on the volume.
