# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Target-architecture notes may appear below. Verify runtime reality before treating any capability as shipped.

## Project

`ootils-core` — a graph-based supply chain decision engine. FastAPI REST API on top of a Python kernel that models supply chains as typed nodes + edges, persisted in PostgreSQL 16. Core capabilities: incremental propagation, shortage detection, MRP explosion, scenario branching (deep-copy fork — historically labelled "copy-on-write"; see [REVIEW-2026-05 R10](docs/REVIEW-2026-05.md)), RCCP, a ghost/virtual-supply engine, and a data quality (DQ) pipeline.

## North Star — Ootils is an agent-piloted supply-chain operating substrate

**This is the single most important framing.** Ootils is not an APS with an AI bolt-on. It is the **deterministic substrate** on top of which a fleet of agents (watchers, scenario workers, governance, orchestrators) continuously monitors, diagnoses, simulates, ranks, and drafts actions. Humans supervise exceptions and irreversible decisions; agents absorb the rest. Reference: `docs/STRATEGY-autonomous-supply-chain-operations.md`.

Every design and implementation decision must be evaluated against this lens:

- **Forkable / scenario-first** — every state-changing capability must work inside a scenario fork so agents can test counter-factuals without touching baseline. No feature is "agent-ready" if it only works on baseline.
- **Deterministic core, stochastic edge** — LLMs/agents never own core calculations. Engine is deterministic; agents propose, govern, approve. Reproducibility is non-negotiable.
- **Queryable from a scenario** — every read path (`GetNode`, `QueryShortages`, etc.) must accept a `scenario_id`. Agents read from forks, not just baseline.
- **Streamable** — agents subscribe to deltas (`StreamChanges`), they don't poll. New capabilities should emit change events.
- **Explainable** — every calculation must be traceable. Recommendations without evidence are rejected by governance agents.
- **Auditable** — every write (by agent or human) is logged with input, output, scenario_id, calc_run_id, policy result. Audit is a feature, not telemetry.
- **Confidence-aware** — outputs that agents consume (forecasts, anomalies, recommendations) must carry a confidence score and a data-freshness flag. Stale data or low DQ blocks autonomous actions.
- **Decision Ladder L0-L4** (cf. strategy doc §5) — every action is classified by reversibility/risk. L0-L2 may be autonomous; L3-L4 require human approval.
- **Budgeted / kill-switchable** — agent-facing endpoints must support idempotency, per-agent scopes, rate limits, and global kill switches.

**Anti-patterns to refuse** (even if requested):
- A module that only works on baseline (not forkable).
- A read endpoint without `scenario_id` parameter.
- A write that bypasses the recommendation/approval state machine for L3+ actions.
- A forecast / score / metric without a confidence or freshness signal.
- An LLM call inside a deterministic calculation path.
- A new feature without StreamChanges emission, without audit log, without explanation trace.

**Wedge V1** : "Autonomous shortage control tower with scenario-backed recommendations." Every near-term feature is judged by whether it advances this wedge.

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
- `dq/`, `ghost/` — capability modules under `engine/`; the `dq/agent/` subtree is an LLM-driven remediation agent.
- **Forecast confidence is REAL, not decorative (ADR-023):** `pyramide/confidence.py` is the single pure/deterministic composer (backtest WAPE x history depth x `demand_history.ingested_at` freshness; missing component → prudent 0.25 default, traced `components` + `stale` — never an optimistic 1.0). The freshness SLA is a request PARAMETER (pilot default 7 days), never a business constant; a Pyramide run on provably stale demand carries `pyramide_runs.stale_demand=TRUE` (migration 056) + exactly one `dq_findings` `STALE_DEMAND` row. Score thresholds belong to consumers (Decision Ladder), not to the module.
- **Moirai is licence-excluded:** `FM_MOIRAI` (Salesforce Moirai, cc-by-nc-4.0) is commercially excluded (decision locked 2026-05-31) — removed from the Pyramide application enums (`pyramide/models.py`, `engines.py`; API returns the standard unknown-method 422) but tolerated in DB CHECKs for historical rows (migration 057). `FM_CHRONOS` stays supported (real backend since PR-B2, see below).
- **Foundation-model axis B is LIVE (ADR-024):** `pyramide/foundation.py` wraps real Chronos-2 (lazy import, one pipeline/process, batch-only seeded inference, honest revision seal in `pyramide_runs.model_revision` — migration 059); routing is opt-in in `HierarchicalRunner`, FM native quantiles are refused in `confidence_interval_*` (conformal-only columns), and the `[foundation]` extra (torch CPU + chronos) is OPTIONAL — never required by the core, real-weights tests run only in the opt-in `test-foundation` CI job (PR label `foundation` / dispatch).
- **MRP exists in TWO implementations — know which one you're touching:**
  1. The consolidated **math core** `src/ootils_core/engine/mrp/core.py` (DB-free calc) + `loader.py` (SELECT-only load) — single source of MRP truth: planning-data load, forecast consumption, the LLC level-by-level time-phased cascade with lot sizing + lead-time offsetting, pegging. `scripts/mrp_core.py` is now a **re-export shim** over this package (ADR-020 step 3; guarded by `tests/test_mrp_shim_compat.py`), still the import point for the CLIs (`scripts/mrp_*.py`) and the watcher agents (`scripts/agent_*_watcher.py`). The whole path is **read-only** (cascade is in-memory Python) and **scenario-parameterized** (`load_planning_data(conn, horizon_days, scenario=...)`). Perf harness: `scripts/bench_mrp.py`.
  2. `src/ootils_core/engine/mrp/` — the **APICS MRP engine** package (`mrp_apics_engine`, `forecast_consumer`, `gross_to_net`, `lot_sizing`, `llc_calculator`, `time_fences`, `graph_integration`) that backs the API routers `mrp.py` (`POST /v1/mrp/run`) and `mrp_apics.py`. This one **writes** nodes/edges into the graph.
  The two are not unified — confirm the call path before changing MRP behaviour.
- **Scenario-backed watchers (#340):** `agent_shortage_watcher` and `agent_material_watcher` validate their recos by counter-factual — ONE fork `what-if-<agent>-<ts>` per run (in-process /v1/simulate path via `ootils_core/tools/agent_tools.py:simulate_overrides`), shortage delta stamped per reco in `evidence.simulation`, fork archived at end of run (TTL, never DELETE). Simulable subset = EXPEDITE with an existing future firm receipt to advance; ORDER_NOW/ORDER_RUSH carry the explicit not-simulated marker; failed fork propagation ⇒ reco emitted with `NEEDS_DATA_REVIEW` and no delta. Decision levels come from `scripts/agent_governance.py:decision_level(action)` (new-order drafts=L1, EXPEDITE=L2) — never hardcode 'L1'. lot_policy/eando/dq stay baseline-only (their actions aren't node overrides; gated on the scenario parameter overlay, #347). Harness: `scripts/agent_simulation.py`.
- **TWO shortage truths — each canonical on its own axis (ADR-021):** the canonical shortage **math** is `mrp_core` (`engine/mrp/core.py`: correct forecast consumption, `first_shortage`); the canonical **persistence/query system** is the `shortages` table (deterministic UUIDs, $-valued `severity_score` via the `cost_of` precedence, ADR-004 causal chain, `/v1/issues`), owned exclusively by `ShortageDetector`. Watchers NEVER write into `shortages` (read-only by design — they emit governed L1 DRAFT recommendations instead). CI guard: `tests/integration/test_shortage_truth_consistency_integration.py` asserts items(watchers) ⊆ items(kernel) on the seeded dataset.

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
