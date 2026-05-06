# Phase 1 QA Test Report — QA-001

Date: 2026-05-05
Scope: Forecasting, MPS, ATP/CTP, CRP, DRP model tests, Phase 1 integration/performance tests.

## Verdict

QA-001 is now satisfied for the Phase 1 core gate and has a deterministic DB-backed REST E2E proof.

Caveat: local workspace has no PostgreSQL, so the new integration test skips locally unless `DATABASE_URL` points to a test DB. It was executed successfully on VM 201 against a dedicated `ootils_test_phase1` database, not production.

## Evidence

### Phase 1 focused suite

Command:

```bash
.venv/bin/python -m pytest \
  tests/test_forecast_models.py \
  tests/test_forecasting_algorithms.py \
  tests/test_forecasting_api.py \
  tests/test_mps_aggregate_demand.py \
  tests/test_mps_capacity_check.py \
  tests/test_mps_models.py \
  tests/test_mps_promote_to_mrp.py \
  tests/test_atp_engine.py \
  tests/test_atp_api.py \
  tests/test_crp_models.py \
  tests/test_crp_engine.py \
  tests/test_crp_integration.py \
  tests/test_phase1_integration.py \
  tests/test_performance.py \
  -q --tb=short
```

Result: `314 passed, 1 skipped, 11 warnings in 4.46s`.

### Repo suite excluding DB-backed integration/smoke

Command:

```bash
OOTILS_API_TOKEN=test-token .venv/bin/python -m pytest tests/ -q --tb=short --ignore=tests/integration --ignore=tests/smoke
```

Result after fixes: `1532 passed, 20 skipped, 34 warnings in 31.10s`.

### E2E-001 DB-backed REST proof

Artifact: `tests/integration/test_phase1_e2e.py`

Flow covered:

1. Seed deterministic item/location/history/routing/work-center data in PostgreSQL.
2. `POST /v1/demand/forecast/generate`
3. `POST /v1/mps/aggregate-demand`
4. SQL approve MPS node for deterministic setup.
5. `POST /v1/mps/{mps_id}/promote-to-mrp`
6. `POST /v1/crp/calculate`
7. `POST /v1/atp/check`

VM 201 command, using dedicated test DB:

```bash
cd ~/ootils-core
set -a && . ./.env && set +a
docker compose exec -T postgres dropdb -U "$POSTGRES_USER" --if-exists ootils_test_phase1
docker compose exec -T postgres createdb -U "$POSTGRES_USER" ootils_test_phase1
DATABASE_URL=postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:5432/ootils_test_phase1 \
OOTILS_API_TOKEN=test-token \
python3 -m pytest tests/integration/test_phase1_e2e.py -q --tb=short
```

Result: `1 passed, 1 warning in 4.99s`.

Local focused regression after E2E fixes:

```bash
OOTILS_API_TOKEN=test-token .venv/bin/python -m pytest \
  tests/test_atp_engine.py \
  tests/test_crp_engine.py \
  tests/test_crp_integration.py \
  tests/test_forecasting_api.py \
  tests/test_mps_aggregate_demand.py \
  tests/test_mps_promote_to_mrp.py \
  tests/test_performance.py \
  tests/integration/test_phase1_e2e.py \
  -q --tb=short
```

Result: `132 passed, 2 skipped, 7 warnings in 2.76s`.

## Fixes applied during audit

1. `tests/test_performance.py`
   - Fixed CRP multi-work-center performance fixture.
   - Root cause: mocked operation used a different `routing_id` than the mocked routing, so the engine correctly produced zero load profiles.
   - Fix: reuse the same `routing_id` for routing and operation.

2. `tests/test_crp_integration.py`
   - Removed persistent mutation of `OOTILS_API_TOKEN` inside `test_crp_router_registered_in_app`.
   - Root cause: one test set `os.environ["OOTILS_API_TOKEN"] = "test-token-for-testing"` and did not restore it, causing later API tests to return 401 depending on test order.

3. `tests/test_mrp_unified.py` → `tests/integration/test_mrp_unified.py`
   - Moved DB-backed unified MRP endpoint tests under `tests/integration/`.
   - Root cause: root test suite runs with `--ignore=tests/integration --ignore=tests/smoke`; this file requires a real PostgreSQL connection and seeded demo data, so it should not run in the non-integration gate.

4. `tests/integration/test_phase1_e2e.py`
   - Added deterministic live DB/API proof for Forecast → MPS → promoted planned supply → CRP → ATP.

5. Migration hardening
   - Made `025_forecast_consumption_log.sql` a legacy no-op because `024_mrp_apics_schema_fixes.sql` already creates the APICS forecast consumption table.
   - Removed invalid PostgreSQL subquery `CHECK` constraints from Phase 1 migrations where needed.
   - Added `030_phase1_e2e_operational_tables.sql` for operational tables consumed by ATP/CRP/MPS (`planned_supply`, `on_hand_supply`, `customer_order_demand`) plus `forecast_values.active`.
   - Removed invalid `calendars` FK from CRP migration because no `calendars` table exists; existing operational calendar model is `operational_calendars`.

6. Runtime/API compatibility fixes
   - Included Forecasting/MPS/ATP/CRP routers in app wiring.
   - Fixed forecast historical demand query to use `CustomerOrderDemand` instead of invalid `CustomerOrder` node type.
   - Fixed MPS promotion insert target: `planned_supply` not `planned_supplies`.
   - Fixed CRP operation fetch table: `routing_operations` not `operations`.
   - Made ATP and CRP engines compatible with psycopg `dict_row` results from real API dependencies while preserving tuple/mock behavior.

## Acceptance criteria status

- Unit tests for Forecasting/MPS/ATP/CRP: PASS locally.
- Fixtures: PRESENT (`tests/fixtures/forecast_data.py`).
- Performance tests: PASS locally for tested targets.
- Test report: PRESENT (`docs/test-report-phase1.md`).
- CI integration: PRESENT. `.github/workflows/ci.yml` now has a dedicated `integration` job with PostgreSQL service running `tests/integration/test_phase1_e2e.py` after the normal test job.
- True E2E Forecast → MPS → MRP/CRP → ATP: PASS on VM 201 test DB via `tests/integration/test_phase1_e2e.py`.

## Recommendation

Mark QA-001 / E2E-001 as implemented and proven.

Next action: unpark Phase 1 feature branches in order: `feat/mps`, `feat/forecasting`, `feat/atp`.
