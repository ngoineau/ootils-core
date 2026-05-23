# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Target-architecture notes may appear below. Verify runtime reality before treating any capability as shipped.

## Project

`ootils-core` — a graph-based supply chain decision engine. FastAPI REST API on top of a Python kernel that models supply chains as typed nodes + edges, persisted in PostgreSQL 16. Core capabilities: incremental propagation, shortage detection, MRP explosion, scenario branching (deep-copy fork — historically labelled "copy-on-write"; see [REVIEW-2026-05 R10](docs/REVIEW-2026-05.md)), RCCP, a ghost/virtual-supply engine, and a data quality (DQ) pipeline.

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
- `scenario/manager.py` — scenario forking via deep-copy (the original CoW vocabulary doesn't match the implementation; see REVIEW-2026-05 R10).
- `dq/`, `ghost/`, `mrp/` — capability modules; the `dq/agent/` subtree is an LLM-driven remediation agent.

### Storage
- PostgreSQL 16 via `psycopg[binary]` 3.x — **not** SQLite (ADR-005 proposes SQLite but the project has moved past the proof stage).
- UUID PKs, `TIMESTAMPTZ` UTC, **no JSONB** for business data. The "JSONB carve-out" pattern: diagnostic / forensic payloads with unbounded shape are the only acceptable JSONB sites, and each must carry a top-of-file comment block explaining the rationale (see `db/migrations/012_dq_agent.sql`, `021_mrp_lot_sizing_params.sql`, `031_demo_runs.sql`). Today's carve-out list: `dq_agent_runs.summary`, `mrp_runs.errors`, `mrp_runs.warnings`, `demo_runs.artifact`. Every other column uses typed columns. 32 numbered SQL migrations under `src/ootils_core/db/migrations/`.
- Migrations auto-apply on `OotilsDB()` construction (i.e. at API startup), serialized by a PG advisory lock (`_LOCK_KEY = 8_037_421_901`), tracked in `schema_migrations`. A migration that fails with an "already exists"-family error is recorded as applied rather than re-run — so new migrations must be idempotent in that sense.
- `events` are conceptually insert-only for the **payload**, but mutable for **bookkeeping metadata** (`processed` flag, `updated_at`). Sites that update `processed = TRUE` in the orchestration layer (`engine/orchestration/propagator.py`, `engine/orchestration/calc_run.py`) are by design — they advance the event lifecycle without rewriting the event. ADR-005 D2's "insert-only" applies to the payload, not the flag.

### Auth
`api/auth.py` validates `OOTILS_API_TOKEN` at **import time** and raises `RuntimeError` if unset. Token comparison uses `hmac.compare_digest`. Don't add an "optional auth" path.

### Propagation model (ADR-003)
Event-driven, incremental, deterministic. An event marks a subgraph dirty; compute happens in topo order; unchanged nodes stop the cascade (they do not propagate further). Every change is attributable. Determinism is a hard constraint — **no randomness in the core engine**.

## Conventions that aren't obvious from the code

- **Tests run against real Postgres, no mocks.** Point `DATABASE_URL` at a throwaway DB. Pure-Python helper functions (and Pydantic-validation 422 boundary tests) live in `tests/test_*.py` and don't need a DB. DB-touching tests live in `tests/integration/test_*_integration.py` and use the `conn` / `seeded_db` fixtures.
- `tests/legacy/` is intentionally excluded via `tests/conftest.py:collect_ignore_glob` — targets the pre-graph architecture, do not re-enable.
- `print()` is forbidden in production code paths — use the module `logger`. The exception is documentation: example `print()` calls **inside docstrings** (showing how a caller would use the returned value) are fine. See `src/ootils_core/forecasting/engine.py:80-81` for the canonical case.
- `/v1/ingest/*` has a 10 MB request-body cap enforced by `IngestPayloadSizeLimitMiddleware` in `api/app.py`.
- The generic exception handler in `api/app.py` deliberately hides exception strings from clients (logs them instead) to avoid leaking DSNs / stack traces. Don't "improve" it by echoing `str(exc)` to the response. **Typed domain exception carve-out**: a few routers do `raise HTTPException(detail=str(e))` where `e` is a *named domain exception* (`DiffError`, `ApprovalError`, `RejectionError` in `api/routers/staging.py:243,325,390`). These exceptions are raised by our own code at `staging/diff.py`, `staging/approve.py`, `staging/reject.py` with hand-authored messages that contain only UUIDs and status enums (no DSN, no DB error, no path) — the messages are part of the API contract and clients need them to act (e.g. "batch in terminal status `'imported'`; /diff only meaningful in 'pending'/'validated'"). The carve-out is limited to those three sites; any new `detail=str(e)` outside the named-domain-exception pattern is a real leak risk.
- Principles from `CONTRIBUTING.md` that the code enforces: API-first (no UI features in V1), explainability (every calculation traceable — see `kernel/explanation/`), fail-loudly over silent wrong answers.

## Documentation worth reading before non-trivial changes

- `docs/ADR-001-graph-model.md` — node/edge taxonomy.
- `docs/ADR-003-incremental-propagation.md` — dirty-flag + topo algorithm.
- `docs/ADR-004-explainability.md` — causal trace model.
- `docs/node-dictionary.md`, `docs/edge-dictionary.md` — canonical type reference.
- `docs/SCALABILITY.md` — current system is demo-scale (2–50 items); production scaling path documented here.
