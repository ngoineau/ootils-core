# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Target-architecture notes may appear below. Verify runtime reality before treating any capability as shipped.

## Project

`ootils-core` — a graph-based supply chain decision engine. FastAPI REST API on top of a Python kernel that models supply chains as typed nodes + edges, persisted in PostgreSQL 16. Core capabilities: incremental propagation, shortage detection, MRP explosion, scenario branching (copy-on-write), RCCP, a ghost/virtual-supply engine, and a data quality (DQ) pipeline.

## Commands

```bash
# Install (dev)
pip install -e ".[dev]"

# Run the API locally (Postgres must be reachable via DATABASE_URL)
export DATABASE_URL=postgresql:///ootils_dev
export OOTILS_API_TOKEN=dev-token          # server REFUSES to start without this
uvicorn ootils_core.api.app:app --reload

# Run everything in Docker (Postgres + API)
cp .env.example .env                        # fill in real values first
docker-compose up -d

# Tests (unit; CI excludes integration/ and smoke/)
python -m pytest tests/ -q --tb=short --ignore=tests/integration --ignore=tests/smoke

# Single test file / test / marker
python -m pytest tests/test_propagator.py -q
python -m pytest tests/test_propagator.py::test_name -q
python -m pytest -m "smoke" -q             # markers: slow, smoke, critical, requires_db

# Integration tests (need a live Postgres; not run in CI)
python -m pytest tests/integration -q

# Lint (CI runs ruff on src/ only)
ruff check src/

# Seed demo data / export OpenAPI
python scripts/seed_demo_data.py
python scripts/export_openapi.py
```

## Architecture

### Request lifecycle
HTTP request → Bearer-token auth (`api/auth.py`) → router in `api/routers/<domain>.py` → engine call → `db/connection.py` yields a `psycopg` connection (autocommit-on-success, rollback-on-exception, `dict_row` factory). One router per capability domain; routers are thin and delegate to the engine.

### Engine layout (`src/ootils_core/engine/`)
- `kernel/graph/` — `store` (CRUD over nodes/edges), `traversal` (topological + subgraph expansion), `dirty` (dirty-flag manager).
- `kernel/calc/` — `projection` (ProjectionKernel), `calendar`.
- `kernel/shortage/`, `kernel/explanation/`, `kernel/allocation/`, `kernel/temporal/` — specialized kernels.
- `orchestration/propagator.py` — `PropagationEngine.process_event()` is the main entry point: acquires advisory lock → expands dirty subgraph → topo sort → compute → persist → cascade. Pairs with `orchestration/calc_run.py` which tracks run status.
- `scenario/manager.py` — copy-on-write scenario branching.
- `dq/`, `ghost/`, `mrp/` — capability modules; the `dq/agent/` subtree is an LLM-driven remediation agent.

### Storage
- PostgreSQL 16 via `psycopg[binary]` 3.x — **not** SQLite (ADR-005 proposes SQLite but the project has moved past the proof stage).
- UUID PKs, `TIMESTAMPTZ` UTC, **no JSONB**. Typed columns, 21 numbered SQL migrations under `src/ootils_core/db/migrations/`.
- Migrations auto-apply on `OotilsDB()` construction (i.e. at API startup), serialized by a PG advisory lock (`_LOCK_KEY = 8_037_421_901`), tracked in `schema_migrations`. A migration that fails with an "already exists"-family error is recorded as applied rather than re-run — so new migrations must be idempotent in that sense.

### Auth
`api/auth.py` validates `OOTILS_API_TOKEN` at **import time** and raises `RuntimeError` if unset. Token comparison uses `hmac.compare_digest`. Don't add an "optional auth" path.

### Propagation model (ADR-003)
Event-driven, incremental, deterministic. An event marks a subgraph dirty; compute happens in topo order; unchanged nodes stop the cascade (they do not propagate further). Every change is attributable. Determinism is a hard constraint — **no randomness in the core engine**.

## Conventions that aren't obvious from the code

- **Tests run against real Postgres, no mocks.** Point `DATABASE_URL` at a throwaway DB.
- `tests/legacy/` is intentionally excluded via `tests/conftest.py:collect_ignore_glob` — targets the pre-graph architecture, do not re-enable.
- `/v1/ingest/*` has a 10 MB request-body cap enforced by `IngestPayloadSizeLimitMiddleware` in `api/app.py`.
- The generic exception handler in `api/app.py` deliberately hides exception strings from clients (logs them instead) to avoid leaking DSNs / stack traces. Don't "improve" it by echoing `str(exc)` to the response.
- Principles from `CONTRIBUTING.md` that the code enforces: API-first (no UI features in V1), explainability (every calculation traceable — see `kernel/explanation/`), fail-loudly over silent wrong answers.

## Documentation worth reading before non-trivial changes

- `docs/ADR-001-graph-model.md` — node/edge taxonomy.
- `docs/ADR-003-incremental-propagation.md` — dirty-flag + topo algorithm.
- `docs/ADR-004-explainability.md` — causal trace model.
- `docs/node-dictionary.md`, `docs/edge-dictionary.md` — canonical type reference.
- `docs/SCALABILITY.md` — current system is demo-scale (2–50 items); production scaling path documented here.
