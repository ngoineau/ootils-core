"""
tests/integration/test_outcome_integration.py — DB-backed tests for the
recommendation-outcome backbone (chantier #393 A3-PR2, ADR-030) against a real
Postgres. Migration 069 (recommendation_outcomes) is applied by the
``migrated_db`` fixture exactly as production applies it (OotilsDB startup). No
mocks — CLAUDE.md.

Two surfaces are under test:

  * The engine (``engine/outcome/evaluator.py``): ``evaluate_and_persist`` — the
    single DB-touching orchestrator. Read-only on recommendations / shortages /
    inventory_snapshots (ADR-021: we READ shortages, we NEVER write it); the sole
    idempotent writer of recommendation_outcomes. Uses the function-scoped
    ``conn`` fixture directly.
  * The HTTP surface (``api/routers/outcomes.py``): GET /v1/recommendations/{id}/
    outcome (read), GET /v1/outcomes/summary (read, the five KPIs), POST
    /v1/outcomes/evaluate (ingest + kill switch). TestClient with ``get_db``
    overridden onto the test DB + minted tokens (the #392 pattern of
    test_agent_floor_integration.py / test_snapshot_integration.py).

Locked contracts:
  1. evaluate_and_persist: an APPROVED reco whose predicted shortage did NOT
     materialise (snapshot present, no active shortage at the coordinate) ->
     exactly one AVOIDED outcome persisted with the predicted-$ credited.
  2. Idempotent: a second pass for the SAME observation day UPDATEs the verdict
     in place (one row per (reco, as_of)), never duplicates.
  3. Read-only on shortages/recos/snapshots (ADR-021): the row counts of those
     three tables are byte-unchanged across an evaluation pass; only
     recommendation_outcomes grows.
  4. GET /{id}/outcome: 404 without an outcome; requires read; returns the verdict.
  5. GET /outcomes/summary: the five KPIs; NULL vs 0 honest (empty scenario ->
     every KPI None + basis 0; a seeded scenario -> real numbers); KPI 3 reads
     fva_wape (NULL fva_wape ignored); the from/to window; requires read;
     scenario isolation.
  6. POST /outcomes/evaluate: requires ingest; kill switch
     OOTILS_OUTCOMES_ENABLED=0 -> 503 (no write).

Every test seeds its own uuid4-suffixed master data and cleans the coordinates it
created (or relies on the module teardown that DROPs every public table). Dates
are anchored on the DB-side CURRENT_DATE. No wall-clock timing assertions. When a
test reads a snapshot/outcome back in SQL it accesses columns BY NAME (dict_row) —
never by positional unpacking (the DRP dict_row pitfall).

FIXED SOURCE BUG (formerly documented here as a known defect, now resolved):
``evaluator.py::_load_recommendations`` no longer SELECTs a non-existent
``location_id`` column — the loader was fixed to select only columns that exist
on ``recommendations`` (item_id, no location_id; only source/dest_location_id on
TRANSFER recos), matching a reco with no location item-wise via the observed
shortages' (item_id, None) pooled fallback. ``evaluate_and_persist`` runs cleanly
against a real DB again.

POST-REVIEW RE-DERIVATION (6 review fixes applied to evaluator.py/outcomes.py —
this file was re-derived against the new rules, not just patched):
  * Fix #1 (ratio bands, MATERIALIZED-first): none of THIS file's seeded verdicts
    change, because every seed here uses predicted_deficit_qty=100 (well above
    the old absolute-floor crossover of 20) — the pure-ratio bands agree with the
    prior absolute-floor-blended bands at that scale. The pure golden coverage of
    the fix itself lives in tests/test_outcome_evaluator.py.
  * Fix #2 (pooled-worst, order-independent): NEW coverage added below
    (``TestEvaluateAndPersist.test_pooled_worst_across_two_locations_is_max_severity``)
    — an item with a MILD shortage at the alphabetically-FIRST location and a
    SEVERE one at the second must resolve the (item, None) pooled key to the
    SEVERE (max deficit_qty) observation, never "whichever SQL saw first".
  * Fix #3 (KPI 5 rejects a negative unit_cost): NEW coverage added below
    (``TestOutcomesSummary.test_kpi5_rejects_negative_evidence_unit_cost_falls_back``).
  * Fix #4 (from/to windows ONLY 1/2/5): the existing window test is extended to
    assert KPI 3 (avg_fva_wape) is ALSO unaffected by the window (KPI 4 was
    already asserted) — both are scenario-scope-lifetime by construction.
"""
from __future__ import annotations

import json
import os
from datetime import date, timedelta
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from ootils_core.engine.outcome import evaluate_and_persist

from .conftest import requires_db

pytestmark = requires_db

# Seeded by migration 002 (is_baseline=TRUE).
BASELINE = UUID("00000000-0000-0000-0000-000000000001")
LEGACY_TOKEN = "integration-test-token"


# ---------------------------------------------------------------------------
# Direct-SQL seed helpers (function-scoped ``conn``). Every INSERT is
# parameterized; JSONB evidence is passed as a json.dumps string.
# ---------------------------------------------------------------------------


def _seed_item(conn, name: str = "outcome-item") -> UUID:
    return conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s) RETURNING item_id",
        (uuid4(), name),
    ).fetchone()["item_id"]


def _seed_location(conn, name: str = "outcome-loc") -> UUID:
    return conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s) RETURNING location_id",
        (uuid4(), name),
    ).fetchone()["location_id"]


def _seed_agent_run(conn, scenario_id: UUID, agent_name: str = "shortage_watcher") -> UUID:
    return conn.execute(
        "INSERT INTO agent_runs (agent_run_id, agent_name, scenario_id, status) "
        "VALUES (%s, %s, %s, 'COMPLETED') RETURNING agent_run_id",
        (uuid4(), agent_name, scenario_id),
    ).fetchone()["agent_run_id"]


def _seed_reco(
    conn,
    *,
    scenario_id: UUID,
    item_id: UUID,
    status: str = "APPROVED",
    shortage_date: date,
    deficit_qty: float = 100,
    recommended_qty: float = 120,
    estimated_cost: float | None = 4800,
    unit_cost: float | None = 3.0,
    action: str = "EXPEDITE",
    agent_name: str = "shortage_watcher",
) -> UUID:
    """One `recommendations` row (migration 039). The predicted shortage is
    shortage_date/deficit_qty; evidence carries the unit_cost $ basis (when
    unit_cost is not None)."""
    run_id = _seed_agent_run(conn, scenario_id, agent_name)
    evidence = json.dumps({"unit_cost": unit_cost}) if unit_cost is not None else None
    return conn.execute(
        """
        INSERT INTO recommendations (
            recommendation_id, agent_name, agent_run_id, scenario_id,
            item_id, item_external_id, shortage_date,
            deficit_qty, recommended_qty, estimated_cost, currency,
            action, status, confidence, evidence
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, 'EUR',
            %s, %s, 'HIGH', %s
        )
        RETURNING recommendation_id
        """,
        (
            uuid4(), agent_name, run_id, scenario_id,
            item_id, f"EXT-{uuid4().hex[:8]}", shortage_date,
            deficit_qty, recommended_qty, estimated_cost,
            action, status, evidence,
        ),
    ).fetchone()["recommendation_id"]


def _seed_calc_run(conn, scenario_id: UUID) -> UUID:
    return conn.execute(
        "INSERT INTO calc_runs (calc_run_id, scenario_id, status) "
        "VALUES (%s, %s, 'completed') RETURNING calc_run_id",
        (uuid4(), scenario_id),
    ).fetchone()["calc_run_id"]


def _seed_pi_node(conn, scenario_id: UUID, item_id: UUID, location_id: UUID) -> UUID:
    return conn.execute(
        """
        INSERT INTO nodes (node_id, node_type, scenario_id, item_id, location_id,
                           time_grain, time_ref, active)
        VALUES (%s, 'ProjectedInventory', %s, %s, %s, 'exact_date', CURRENT_DATE, TRUE)
        RETURNING node_id
        """,
        (uuid4(), scenario_id, item_id, location_id),
    ).fetchone()["node_id"]


def _seed_shortage(
    conn,
    *,
    scenario_id: UUID,
    item_id: UUID,
    location_id: UUID,
    shortage_date: date,
    shortage_qty: float,
    severity_score: float,
    status: str = "active",
) -> UUID:
    """A canonical `shortages` row (migration 005). Needs a pi_node + calc_run."""
    pi_node = _seed_pi_node(conn, scenario_id, item_id, location_id)
    calc_run = _seed_calc_run(conn, scenario_id)
    return conn.execute(
        """
        INSERT INTO shortages (shortage_id, scenario_id, pi_node_id, item_id,
            location_id, shortage_date, shortage_qty, severity_score, calc_run_id, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING shortage_id
        """,
        (
            uuid4(), scenario_id, pi_node, item_id, location_id,
            shortage_date, shortage_qty, severity_score, calc_run, status,
        ),
    ).fetchone()["shortage_id"]


def _seed_snapshot(
    conn,
    *,
    scenario_id: UUID,
    item_id: UUID,
    location_id: UUID,
    as_of_date: date,
    on_hand_qty: float = 10,
    source: str = "cli",
) -> UUID:
    return conn.execute(
        """
        INSERT INTO inventory_snapshots (snapshot_id, scenario_id, item_id,
            location_id, as_of_date, on_hand_qty, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING snapshot_id
        """,
        (uuid4(), scenario_id, item_id, location_id, as_of_date, on_hand_qty, source),
    ).fetchone()["snapshot_id"]


def _seed_pyramide_accuracy(
    conn,
    *,
    scenario_id: UUID,
    item_id: UUID,
    location_id: UUID,
    fva_wape: float | None,
    wape: float | None = 0.2,
    horizon: int | None = None,
) -> UUID:
    """A `pyramide_accuracy_metrics` aggregate row (horizon NULL by default) with
    fva_wape (migration 068). Needs a forecast + pyramide_run. Returns run_id."""
    forecast_id = conn.execute(
        """
        INSERT INTO forecasts (forecast_id, item_id, location_id, scenario_id,
            horizon_start, horizon_end, granularity, method)
        VALUES (%s, %s, %s, %s, CURRENT_DATE, CURRENT_DATE + 30, 'daily', 'MA')
        RETURNING forecast_id
        """,
        (uuid4(), item_id, location_id, scenario_id),
    ).fetchone()["forecast_id"]
    run_id = conn.execute(
        """
        INSERT INTO pyramide_runs (run_id, forecast_id, item_id, location_id,
            scenario_id, horizon_start, horizon_end, granularity, method,
            source_history_count)
        VALUES (%s, %s, %s, %s, %s, CURRENT_DATE, CURRENT_DATE + 30, 'daily', 'MA', 52)
        RETURNING run_id
        """,
        (uuid4(), forecast_id, item_id, location_id, scenario_id),
    ).fetchone()["run_id"]
    conn.execute(
        """
        INSERT INTO pyramide_accuracy_metrics (metric_id, run_id, horizon, wape,
            fva_wape, n_cutoffs, n_observations)
        VALUES (%s, %s, %s, %s, %s, 5, 20)
        """,
        (uuid4(), run_id, horizon, wape, fva_wape),
    )
    return run_id


def _count(conn, table: str, scenario_id: UUID) -> int:
    # Static table name (test-controlled literal, never caller data), parameterized value.
    return conn.execute(
        f"SELECT COUNT(*) AS n FROM {table} WHERE scenario_id = %s",  # noqa: S608 — literal table
        (scenario_id,),
    ).fetchone()["n"]


def _outcomes_for(conn, reco_id: UUID) -> list[dict]:
    return conn.execute(
        "SELECT * FROM recommendation_outcomes WHERE recommendation_id = %s "
        "ORDER BY evaluated_as_of DESC",
        (reco_id,),
    ).fetchall()


# ===========================================================================
# 1. evaluate_and_persist — engine, function-scoped conn
# ===========================================================================


class TestEvaluateAndPersist:
    def test_approved_avoided_persists_one_outcome_with_dollars(self, conn):
        """An APPROVED reco whose predicted shortage did NOT materialise (a
        snapshot exists at the coordinate but there is NO active shortage there)
        -> one AVOIDED outcome, predicted-$ credited = 100 * 3.0 = 300."""
        item = _seed_item(conn)
        loc = _seed_location(conn)
        today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
        reco_id = _seed_reco(
            conn, scenario_id=BASELINE, item_id=item,
            shortage_date=today + timedelta(days=20), deficit_qty=100, unit_cost=3.0,
        )
        # Observation snapshot at the coordinate, but NO active shortage seeded ->
        # observed deficit resolves to 0 -> AVOIDED.
        _seed_snapshot(conn, scenario_id=BASELINE, item_id=item, location_id=loc, as_of_date=today)
        conn.commit()

        metrics = evaluate_and_persist(conn, str(BASELINE))
        conn.commit()

        rows = _outcomes_for(conn, reco_id)
        assert len(rows) == 1
        row = rows[0]
        assert row["evaluation_status"] == "AVOIDED"
        assert row["observed_deficit_qty"] == 0
        assert row["avoided_severity_usd"] == 300
        assert row["predicted_deficit_qty"] == 100
        assert row["evaluated_as_of"] == today
        assert row["snapshot_id"] is not None, "AVOIDED carries the observation snapshot pointer"
        assert metrics["by_status"]["AVOIDED"] >= 1
        assert metrics["with_avoided_usd"] >= 1

    def test_materialized_when_shortage_persists(self, conn):
        """An APPROVED reco with a snapshot AND an active shortage at the
        coordinate whose observed deficit ~ the prediction -> MATERIALIZED,
        avoided a genuine 0 (not None). Predicted 100, observed 95 (ratio 0.95 >=
        0.90 floor)."""
        item = _seed_item(conn)
        loc = _seed_location(conn)
        today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
        reco_id = _seed_reco(
            conn, scenario_id=BASELINE, item_id=item,
            shortage_date=today + timedelta(days=20), deficit_qty=100, unit_cost=3.0,
        )
        _seed_snapshot(conn, scenario_id=BASELINE, item_id=item, location_id=loc, as_of_date=today)
        _seed_shortage(
            conn, scenario_id=BASELINE, item_id=item, location_id=loc,
            shortage_date=today, shortage_qty=95, severity_score=285,
        )
        conn.commit()

        evaluate_and_persist(conn, str(BASELINE))
        conn.commit()

        row = _outcomes_for(conn, reco_id)[0]
        assert row["evaluation_status"] == "MATERIALIZED"
        assert row["observed_deficit_qty"] == 95
        assert row["avoided_severity_usd"] == 0, "MATERIALIZED credits a hard 0, not NULL"

    def test_indeterminate_when_no_snapshot(self, conn):
        """An APPROVED reco with NO snapshot at its coordinate -> INDETERMINATE;
        observed/avoided both NULL, snapshot_id NULL."""
        item = _seed_item(conn)
        today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
        reco_id = _seed_reco(
            conn, scenario_id=BASELINE, item_id=item,
            shortage_date=today + timedelta(days=20), deficit_qty=100,
        )
        conn.commit()

        evaluate_and_persist(conn, str(BASELINE))
        conn.commit()

        row = _outcomes_for(conn, reco_id)[0]
        assert row["evaluation_status"] == "INDETERMINATE"
        assert row["observed_deficit_qty"] is None
        assert row["avoided_severity_usd"] is None
        assert row["snapshot_id"] is None

    def test_not_applicable_when_reco_never_acted(self, conn):
        """A DRAFT reco (never acted) with an observed shortage -> NOT_APPLICABLE:
        observed deficit recorded (cost-of-inaction signal), avoided NULL."""
        item = _seed_item(conn)
        loc = _seed_location(conn)
        today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
        reco_id = _seed_reco(
            conn, scenario_id=BASELINE, item_id=item, status="DRAFT",
            shortage_date=today + timedelta(days=20), deficit_qty=100, unit_cost=3.0,
        )
        _seed_shortage(
            conn, scenario_id=BASELINE, item_id=item, location_id=loc,
            shortage_date=today, shortage_qty=80, severity_score=240,
        )
        conn.commit()

        evaluate_and_persist(conn, str(BASELINE))
        conn.commit()

        row = _outcomes_for(conn, reco_id)[0]
        assert row["evaluation_status"] == "NOT_APPLICABLE"
        assert row["observed_deficit_qty"] == 80
        assert row["avoided_severity_usd"] is None

    def test_idempotent_same_day_updates_not_duplicates(self, conn):
        """Two evaluation passes for the SAME observation day -> one row per
        (reco, evaluated_as_of), the second UPDATEs in place. Prove the update
        path: change the reco's evidence unit cost between passes and assert the
        avoided-$ is recomputed on the SAME single row."""
        item = _seed_item(conn)
        loc = _seed_location(conn)
        today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
        reco_id = _seed_reco(
            conn, scenario_id=BASELINE, item_id=item,
            shortage_date=today + timedelta(days=20), deficit_qty=100, unit_cost=3.0,
        )
        _seed_snapshot(conn, scenario_id=BASELINE, item_id=item, location_id=loc, as_of_date=today)
        conn.commit()

        evaluate_and_persist(conn, str(BASELINE))
        conn.commit()
        first = _outcomes_for(conn, reco_id)
        assert len(first) == 1 and first[0]["avoided_severity_usd"] == 300

        # Bump the unit cost to 5.0 -> predicted_$ becomes 100 * 5 = 500.
        conn.execute(
            "UPDATE recommendations SET evidence = %s WHERE recommendation_id = %s",
            (json.dumps({"unit_cost": 5.0}), reco_id),
        )
        conn.commit()

        evaluate_and_persist(conn, str(BASELINE))
        conn.commit()
        second = _outcomes_for(conn, reco_id)
        assert len(second) == 1, "same (reco, day) must not duplicate — ON CONFLICT DO UPDATE"
        assert second[0]["avoided_severity_usd"] == 500, "verdict $ recomputed in place"
        assert second[0]["outcome_id"] == first[0]["outcome_id"], "same row, updated"

    def test_read_only_on_shortages_recos_snapshots(self, conn):
        """ADR-021 read-only discipline: an evaluation pass leaves the row counts
        of shortages / recommendations / inventory_snapshots byte-unchanged; only
        recommendation_outcomes grows."""
        item = _seed_item(conn)
        loc = _seed_location(conn)
        today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
        _seed_reco(
            conn, scenario_id=BASELINE, item_id=item,
            shortage_date=today + timedelta(days=20), deficit_qty=100,
        )
        _seed_snapshot(conn, scenario_id=BASELINE, item_id=item, location_id=loc, as_of_date=today)
        _seed_shortage(
            conn, scenario_id=BASELINE, item_id=item, location_id=loc,
            shortage_date=today, shortage_qty=40, severity_score=120,
        )
        conn.commit()

        before = {
            t: _count(conn, t, BASELINE)
            for t in ("shortages", "recommendations", "inventory_snapshots")
        }
        outcomes_before = conn.execute(
            "SELECT COUNT(*) AS n FROM recommendation_outcomes"
        ).fetchone()["n"]

        evaluate_and_persist(conn, str(BASELINE))
        conn.commit()

        after = {
            t: _count(conn, t, BASELINE)
            for t in ("shortages", "recommendations", "inventory_snapshots")
        }
        outcomes_after = conn.execute(
            "SELECT COUNT(*) AS n FROM recommendation_outcomes"
        ).fetchone()["n"]

        assert after == before, "shortages/recos/snapshots must be untouched (ADR-021 read-only)"
        assert outcomes_after > outcomes_before, "only recommendation_outcomes grows"

    def test_pooled_worst_across_two_locations_is_max_severity(self, conn):
        """Fix #2 golden — the (item, None) pooled key resolves to the
        MAX-severity observation across the item's locations, INDEPENDENT of SQL
        row order (which orders by (item_id, location_id), i.e. by location
        UUID first).

        Two locations, deliberately ordered so the alphabetically-FIRST
        location_id carries the MILD shortage (severity 1, qty 2) and the
        SECOND carries the SEVERE one (severity 100, qty 95) — reproducing
        exactly the case that a naive "first SQL row wins" pooling would get
        backwards (it would pick the mild one just because its UUID sorts
        first).

        The reco carries no location (a procurement-style reco), so it resolves
        via the pooled fallback. Predicted 100, unit_cost 3.0 -> predicted_$=300.
        If pooling picked the mild shortage (qty 2): ratio 2/100=0.02 <= 0.05 ->
        would be AVOIDED — the wrong, overstated verdict. Picking the correct
        max-severity (qty 95): ratio 95/100=0.95 >= 0.90 -> MATERIALIZED,
        avoided=0 — the honest verdict this fix guarantees.
        """
        item = _seed_item(conn)
        loc_first = UUID("00000000-0000-0000-0000-0000000000a1")
        loc_second = UUID("00000000-0000-0000-0000-0000000000b2")
        assert str(loc_first) < str(loc_second), "test setup: loc_first must sort before loc_second"
        conn.execute(
            "INSERT INTO locations (location_id, name) VALUES (%s, 'pool-a'), (%s, 'pool-b')",
            (loc_first, loc_second),
        )
        today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]

        # Mild shortage at the FIRST-sorting location: low severity, low qty.
        _seed_shortage(
            conn, scenario_id=BASELINE, item_id=item, location_id=loc_first,
            shortage_date=today, shortage_qty=2, severity_score=1,
        )
        # Severe shortage at the SECOND-sorting location: high severity, high qty.
        _seed_shortage(
            conn, scenario_id=BASELINE, item_id=item, location_id=loc_second,
            shortage_date=today, shortage_qty=95, severity_score=100,
        )
        # A snapshot anchor so the reco is classifiable (not INDETERMINATE) —
        # item-pooled, no location on the reco itself.
        _seed_snapshot(conn, scenario_id=BASELINE, item_id=item, location_id=loc_first, as_of_date=today)

        reco_id = _seed_reco(
            conn, scenario_id=BASELINE, item_id=item,
            shortage_date=today + timedelta(days=20), deficit_qty=100, unit_cost=3.0,
        )
        conn.commit()

        evaluate_and_persist(conn, str(BASELINE))
        conn.commit()

        row = _outcomes_for(conn, reco_id)[0]
        assert row["evaluation_status"] == "MATERIALIZED", (
            "pooling must pick the max-severity (95-qty) observation, not the "
            "alphabetically-first (2-qty) one"
        )
        assert row["observed_deficit_qty"] == 95
        assert row["avoided_severity_usd"] == 0

    def test_explicit_as_of_scopes_the_observation_window(self, conn):
        """A shortage dated in the FUTURE relative to a past as_of is NOT observed
        (the loader filters shortage_date <= as_of). An APPROVED reco with a
        snapshot on that past day + only a future shortage -> AVOIDED at the past
        as_of (nothing observed yet)."""
        item = _seed_item(conn)
        loc = _seed_location(conn)
        today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
        past = today - timedelta(days=10)
        reco_id = _seed_reco(
            conn, scenario_id=BASELINE, item_id=item,
            shortage_date=today + timedelta(days=20), deficit_qty=100, unit_cost=3.0,
        )
        _seed_snapshot(conn, scenario_id=BASELINE, item_id=item, location_id=loc, as_of_date=past)
        # A shortage that only materialises TODAY (after the `past` as_of).
        _seed_shortage(
            conn, scenario_id=BASELINE, item_id=item, location_id=loc,
            shortage_date=today, shortage_qty=90, severity_score=270,
        )
        conn.commit()

        evaluate_and_persist(conn, str(BASELINE), evaluated_as_of=past)
        conn.commit()

        row = _outcomes_for(conn, reco_id)[0]
        assert row["evaluated_as_of"] == past
        assert row["evaluation_status"] == "AVOIDED", "future shortage invisible at the past as_of"
        assert row["avoided_severity_usd"] == 300


# ===========================================================================
# HTTP surface — app fixtures (the #392 / snapshot pattern)
# ===========================================================================


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """TestClient with get_db overridden onto the test DB (mirrors
    test_snapshot_integration.py)."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = LEGACY_TOKEN

    from fastapi.testclient import TestClient

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB

    app = create_app()

    def override_db():
        db = OotilsDB(migrated_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def _clear_token_cache():
    """Clear the in-process minted-token cache around every test so a seed in one
    test never leaks a cached auth decision into another."""
    import ootils_core.api.auth as auth

    auth._token_cache.clear()
    yield
    auth._token_cache.clear()


def _db_conn(dsn):
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _mint_token(dsn, *, actor_kind: str, scopes: list[str]) -> str:
    from ootils_core.api.auth import hash_token, token_prefix

    clear = f"ootk_{actor_kind}_{uuid4().hex}"
    token_id = uuid4()
    with _db_conn(dsn) as c:
        c.execute(
            """
            INSERT INTO api_tokens (
                token_id, name, actor_kind, token_hash, token_prefix, scopes
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                token_id,
                f"outcome-{actor_kind}-{token_id}",
                actor_kind,
                hash_token(clear),
                token_prefix(clear),
                scopes,
            ),
        )
    return clear


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def new_scenario(migrated_db):
    """A fresh non-baseline scenario so each summary test is isolated from every
    other test's recos/outcomes (the summary is scenario-scoped). Cleans the
    coordinates it created afterwards."""
    sid = uuid4()
    with _db_conn(migrated_db) as c:
        c.execute(
            "INSERT INTO scenarios (scenario_id, name, is_baseline, status) "
            "VALUES (%s, %s, FALSE, 'active')",
            (sid, f"outcome-scn-{sid}"),
        )
    yield sid
    with _db_conn(migrated_db) as c:
        # Order matters for FKs: outcomes -> recos -> agent_runs; accuracy ->
        # runs -> forecasts; shortages -> nodes/calc_runs.
        c.execute(
            "DELETE FROM recommendation_outcomes WHERE recommendation_id IN "
            "(SELECT recommendation_id FROM recommendations WHERE scenario_id = %s)",
            (sid,),
        )
        c.execute("DELETE FROM recommendations WHERE scenario_id = %s", (sid,))
        c.execute("DELETE FROM agent_runs WHERE scenario_id = %s", (sid,))
        c.execute(
            "DELETE FROM pyramide_accuracy_metrics WHERE run_id IN "
            "(SELECT run_id FROM pyramide_runs WHERE scenario_id = %s)",
            (sid,),
        )
        c.execute("DELETE FROM pyramide_runs WHERE scenario_id = %s", (sid,))
        c.execute("DELETE FROM forecasts WHERE scenario_id = %s", (sid,))
        c.execute("DELETE FROM shortages WHERE scenario_id = %s", (sid,))
        c.execute("DELETE FROM inventory_snapshots WHERE scenario_id = %s", (sid,))
        c.execute(
            "DELETE FROM nodes WHERE scenario_id = %s AND node_type = 'ProjectedInventory'",
            (sid,),
        )
        c.execute("DELETE FROM calc_runs WHERE scenario_id = %s", (sid,))
        c.execute("DELETE FROM scenarios WHERE scenario_id = %s", (sid,))


def _seed_full_scenario(migrated_db, sid: UUID) -> dict:
    """Seed a scenario with a rich, KPI-exercising mix and RUN the evaluator once
    so recommendation_outcomes is populated. Returns handles the tests assert on.

    Composition (all coordinates distinct items so matching is unambiguous):
      * reco_avoided  — APPROVED, deficit 100, unit_cost 3.0, snapshot present, NO
        shortage -> AVOIDED, avoided_$ = 300.
      * reco_material — APPROVED, deficit 100, unit_cost 3.0, snapshot + active
        shortage 95 -> MATERIALIZED, avoided_$ = 0.
      * reco_draft    — DRAFT, deficit 100, unit_cost 2.0, active shortage 80,
        snapshot present -> NOT_APPLICABLE, observed 80 (cost of inaction =
        80 * 2.0 = 160).
    KPI expectations (derived by hand):
      KPI1 pct_shortages_avoided = AVOIDED / (AVOIDED+MATERIALIZED+PARTIAL) over
           ACTED recos = 1 / 2 = 0.5 (the DRAFT NOT_APPLICABLE is not acted).
      KPI2 avoided_severity_usd_total = 300 + 0 = 300.
      KPI4 reco_approval_rate = APPROVED / total = 2 / 3.
      KPI5 cost_of_inaction_usd = 80 * 2.0 = 160.
    KPI3 (avg_fva_wape) is seeded separately per test.
    """
    with _db_conn(migrated_db) as conn:
        today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]

        item_a = _seed_item(conn, "kpi-avoided")
        item_m = _seed_item(conn, "kpi-material")
        item_d = _seed_item(conn, "kpi-draft")
        loc = _seed_location(conn)

        reco_avoided = _seed_reco(
            conn, scenario_id=sid, item_id=item_a, status="APPROVED",
            shortage_date=today + timedelta(days=20), deficit_qty=100, unit_cost=3.0,
        )
        _seed_snapshot(conn, scenario_id=sid, item_id=item_a, location_id=loc, as_of_date=today)

        reco_material = _seed_reco(
            conn, scenario_id=sid, item_id=item_m, status="APPROVED",
            shortage_date=today + timedelta(days=20), deficit_qty=100, unit_cost=3.0,
        )
        _seed_snapshot(conn, scenario_id=sid, item_id=item_m, location_id=loc, as_of_date=today)
        _seed_shortage(
            conn, scenario_id=sid, item_id=item_m, location_id=loc,
            shortage_date=today, shortage_qty=95, severity_score=285,
        )

        reco_draft = _seed_reco(
            conn, scenario_id=sid, item_id=item_d, status="DRAFT",
            shortage_date=today + timedelta(days=20), deficit_qty=100, unit_cost=2.0,
        )
        _seed_snapshot(conn, scenario_id=sid, item_id=item_d, location_id=loc, as_of_date=today)
        _seed_shortage(
            conn, scenario_id=sid, item_id=item_d, location_id=loc,
            shortage_date=today, shortage_qty=80, severity_score=160,
        )

        evaluate_and_persist(conn, str(sid))
        conn.commit()

        return {
            "today": today,
            "loc": loc,
            "item_a": item_a,
            "item_m": item_m,
            "item_d": item_d,
            "reco_avoided": reco_avoided,
            "reco_material": reco_material,
            "reco_draft": reco_draft,
        }


# ===========================================================================
# 2. GET /v1/recommendations/{id}/outcome
# ===========================================================================


class TestGetRecommendationOutcome:
    def test_404_without_outcome(self, api_client, new_scenario, migrated_db):
        """A reco with no evaluated outcome -> 404 (not an empty 200)."""
        with _db_conn(migrated_db) as conn:
            item = _seed_item(conn)
            today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
            reco_id = _seed_reco(
                conn, scenario_id=new_scenario, item_id=item,
                shortage_date=today + timedelta(days=20),
            )
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["read"])
        resp = api_client.get(f"/v1/recommendations/{reco_id}/outcome", headers=_bearer(clear))
        assert resp.status_code == 404, resp.text

    def test_403_without_read_scope(self, api_client, new_scenario, migrated_db):
        with _db_conn(migrated_db) as conn:
            item = _seed_item(conn)
            today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
            reco_id = _seed_reco(
                conn, scenario_id=new_scenario, item_id=item,
                shortage_date=today + timedelta(days=20),
            )
        clear = _mint_token(migrated_db, actor_kind="agent", scopes=["recommend:draft"])
        resp = api_client.get(f"/v1/recommendations/{reco_id}/outcome", headers=_bearer(clear))
        assert resp.status_code == 403, resp.text
        assert "read" in resp.json()["detail"].lower()

    def test_returns_the_avoided_verdict(self, api_client, new_scenario, migrated_db):
        """A seeded+evaluated AVOIDED reco -> 200 with the verdict and the
        NULL-honest $ figures (avoided 300.0, observed 0.0)."""
        handles = _seed_full_scenario(migrated_db, new_scenario)
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["read"])

        resp = api_client.get(
            f"/v1/recommendations/{handles['reco_avoided']}/outcome", headers=_bearer(clear)
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["recommendation_id"] == str(handles["reco_avoided"])
        assert body["evaluation_status"] == "AVOIDED"
        assert body["observed_deficit_qty"] == 0.0
        assert body["avoided_severity_usd"] == 300.0
        assert body["predicted_deficit_qty"] == 100.0
        assert body["snapshot_id"] is not None

    def test_returns_not_applicable_null_avoided(self, api_client, new_scenario, migrated_db):
        """The DRAFT reco's verdict: NOT_APPLICABLE with avoided_severity_usd null
        (NULL-honest, not 0) and observed_deficit_qty 80.0 (cost of inaction)."""
        handles = _seed_full_scenario(migrated_db, new_scenario)
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["read"])

        resp = api_client.get(
            f"/v1/recommendations/{handles['reco_draft']}/outcome", headers=_bearer(clear)
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["evaluation_status"] == "NOT_APPLICABLE"
        assert body["avoided_severity_usd"] is None
        assert body["observed_deficit_qty"] == 80.0


# ===========================================================================
# 3. GET /v1/outcomes/summary — the five KPIs, NULL/0-honest
# ===========================================================================


class TestOutcomesSummary:
    def test_403_without_read_scope(self, api_client, new_scenario, migrated_db):
        clear = _mint_token(migrated_db, actor_kind="agent", scopes=["recommend:draft"])
        resp = api_client.get(
            "/v1/outcomes/summary",
            params={"scenario_id": str(new_scenario)},
            headers=_bearer(clear),
        )
        assert resp.status_code == 403, resp.text
        assert "read" in resp.json()["detail"].lower()

    def test_empty_scenario_every_kpi_null_and_basis_zero(
        self, api_client, new_scenario, migrated_db
    ):
        """NULL vs 0 honesty: an empty scenario (no recos, no outcomes, no
        accuracy) -> every KPI None and every *_basis / *_count 0. A None KPI is
        'no data to compute', DISTINCT from a real 0."""
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["read"])
        resp = api_client.get(
            "/v1/outcomes/summary",
            params={"scenario_id": str(new_scenario)},
            headers=_bearer(clear),
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["scenario_id"] == str(new_scenario)
        assert body["pct_shortages_avoided"] is None
        assert body["avoided_basis_count"] == 0
        assert body["avoided_severity_usd_total"] is None
        assert body["avg_fva_wape"] is None
        assert body["fva_basis_count"] == 0
        assert body["reco_approval_rate"] is None
        assert body["reco_total_count"] == 0
        assert body["cost_of_inaction_usd"] is None

    def test_seeded_scenario_five_kpis(self, api_client, new_scenario, migrated_db):
        """The five KPIs over the rich seed (derivations in _seed_full_scenario):
          KPI1 pct_shortages_avoided = 1/2 = 0.5
          KPI2 avoided_severity_usd_total = 300
          KPI4 reco_approval_rate = 2/3
          KPI5 cost_of_inaction_usd = 80 * 2.0 = 160
        Plus a fva_wape aggregate row of 0.15 -> KPI3 avg_fva_wape = 0.15.
        """
        handles = _seed_full_scenario(migrated_db, new_scenario)
        with _db_conn(migrated_db) as conn:
            _seed_pyramide_accuracy(
                conn, scenario_id=new_scenario, item_id=handles["item_a"],
                location_id=handles["loc"], fva_wape=0.15,
            )
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["read"])

        resp = api_client.get(
            "/v1/outcomes/summary",
            params={"scenario_id": str(new_scenario)},
            headers=_bearer(clear),
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        # KPI1: AVOIDED(1) / (AVOIDED+MATERIALIZED+PARTIAL over acted = 2) = 0.5.
        assert body["pct_shortages_avoided"] == pytest.approx(0.5)
        assert body["avoided_basis_count"] == 2
        # KPI2: 300 avoided + 0 materialized = 300.
        assert body["avoided_severity_usd_total"] == pytest.approx(300.0)
        # KPI4: 2 approved / 3 total.
        assert body["reco_approval_rate"] == pytest.approx(2 / 3)
        assert body["reco_total_count"] == 3
        # KPI5: DRAFT NOT_APPLICABLE observed 80 * unit_cost 2.0 = 160.
        assert body["cost_of_inaction_usd"] == pytest.approx(160.0)
        # KPI3: single aggregate fva_wape row = 0.15.
        assert body["avg_fva_wape"] == pytest.approx(0.15)
        assert body["fva_basis_count"] == 1

    def test_kpi3_ignores_null_fva_wape_and_horizon_rows(
        self, api_client, new_scenario, migrated_db
    ):
        """KPI3 NULL-honesty: only NON-NULL aggregate (horizon IS NULL) fva_wape
        rows are averaged. Seed three metric rows:
          * aggregate fva_wape = 0.20   (counted)
          * aggregate fva_wape = 0.40   (counted)  -> avg = 0.30, basis = 2
          * aggregate fva_wape = NULL    (ignored — not a masked 0)
          * per-horizon (horizon=1) fva_wape = 0.99 (ignored — not aggregate)
        """
        with _db_conn(migrated_db) as conn:
            item1 = _seed_item(conn, "fva-1")
            item2 = _seed_item(conn, "fva-2")
            item3 = _seed_item(conn, "fva-null")
            item4 = _seed_item(conn, "fva-horizon")
            loc = _seed_location(conn)
            _seed_pyramide_accuracy(
                conn, scenario_id=new_scenario, item_id=item1, location_id=loc, fva_wape=0.20
            )
            _seed_pyramide_accuracy(
                conn, scenario_id=new_scenario, item_id=item2, location_id=loc, fva_wape=0.40
            )
            _seed_pyramide_accuracy(
                conn, scenario_id=new_scenario, item_id=item3, location_id=loc, fva_wape=None
            )
            # A per-horizon row (horizon=1) with a big fva_wape that MUST be excluded.
            _seed_pyramide_accuracy(
                conn, scenario_id=new_scenario, item_id=item4, location_id=loc,
                fva_wape=0.99, horizon=1,
            )
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["read"])

        resp = api_client.get(
            "/v1/outcomes/summary",
            params={"scenario_id": str(new_scenario)},
            headers=_bearer(clear),
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["fva_basis_count"] == 2, "only the two non-NULL aggregate rows count"
        assert body["avg_fva_wape"] == pytest.approx(0.30), "(0.20 + 0.40) / 2, NULL & horizon ignored"

    def test_window_from_to_filters_observation_date(
        self, api_client, new_scenario, migrated_db
    ):
        """The from/to window filters KPIs 1/2/5 on evaluated_as_of ONLY (fix
        #4). The seed evaluates on CURRENT_DATE; a window entirely in the past
        excludes it (avoided basis 0, KPIs 1/2 None), while a window covering
        today includes it. KPI 3 (avg_fva_wape) and KPI 4 (reco_approval_rate)
        are scenario-scope-LIFETIME by construction and must NOT move between
        the two windows — asserted explicitly on both responses, not just one.
        """
        handles = _seed_full_scenario(migrated_db, new_scenario)
        today = handles["today"]
        with _db_conn(migrated_db) as conn:
            _seed_pyramide_accuracy(
                conn, scenario_id=new_scenario, item_id=handles["item_a"],
                location_id=handles["loc"], fva_wape=0.15,
            )
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["read"])

        # A past-only window: no outcomes in range -> outcome KPIs (1/2/5) empty.
        past_to = (today - timedelta(days=1)).isoformat()
        resp = api_client.get(
            "/v1/outcomes/summary",
            params={"scenario_id": str(new_scenario), "from": "2000-01-01", "to": past_to},
            headers=_bearer(clear),
        )
        assert resp.status_code == 200, resp.text
        past = resp.json()
        assert past["avoided_basis_count"] == 0, "no outcome falls in the past-only window"
        assert past["pct_shortages_avoided"] is None
        assert past["avoided_severity_usd_total"] is None
        # KPI3/4 are NOT window-filtered (fix #4: no evaluated_as_of of their
        # own) — unaffected by a window that excludes every outcome.
        assert past["reco_total_count"] == 3
        assert past["reco_approval_rate"] == pytest.approx(2 / 3)
        assert past["avg_fva_wape"] == pytest.approx(0.15)
        assert past["fva_basis_count"] == 1

        # A window covering today includes the KPI1/2/5 outcomes.
        resp = api_client.get(
            "/v1/outcomes/summary",
            params={
                "scenario_id": str(new_scenario),
                "from": today.isoformat(),
                "to": today.isoformat(),
            },
            headers=_bearer(clear),
        )
        assert resp.status_code == 200, resp.text
        now = resp.json()
        assert now["avoided_basis_count"] == 2
        assert now["avoided_severity_usd_total"] == pytest.approx(300.0)
        # KPI3/4 are IDENTICAL across both windows — proof they are lifetime,
        # not silently re-scoped by the same from/to that moved KPI1/2 above.
        assert now["reco_total_count"] == past["reco_total_count"]
        assert now["reco_approval_rate"] == pytest.approx(past["reco_approval_rate"])
        assert now["avg_fva_wape"] == pytest.approx(past["avg_fva_wape"])
        assert now["fva_basis_count"] == past["fva_basis_count"]

    def test_kpi5_rejects_negative_evidence_unit_cost_falls_back(
        self, api_client, new_scenario, migrated_db
    ):
        """Fix #3 golden — KPI 5's SQL unit-cost CASE mirrors the evaluator's
        Python ``uc > 0`` guard EXACTLY: a negative evidence unit_cost (-5) is
        REJECTED (the regex ``^-?[0-9]+(\\.[0-9]+)?$`` matches the literal
        '-5', but the ``> 0`` predicate then excludes it), so the fallback
        estimated_cost/recommended_qty (400/100 = 4.0) is used instead — never a
        negative cost of inaction.

        DRAFT reco (never acted), evidence unit_cost=-5, estimated_cost=400,
        recommended_qty=100 -> fallback unit_cost=4.0. An active shortage of
        qty=40 at the coordinate -> NOT_APPLICABLE outcome, observed_deficit_qty
        =40. cost_of_inaction_usd = 40 * 4.0 = 160 — NEVER 40 * -5 = -200.
        """
        with _db_conn(migrated_db) as conn:
            item = _seed_item(conn, "kpi5-neg")
            loc = _seed_location(conn)
            today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
            conn.execute(
                """
                INSERT INTO recommendations (
                    recommendation_id, agent_name, agent_run_id, scenario_id,
                    item_id, item_external_id, shortage_date,
                    deficit_qty, recommended_qty, estimated_cost, currency,
                    action, status, confidence, evidence
                ) VALUES (
                    %s, 'shortage_watcher', %s, %s,
                    %s, %s, %s,
                    100, 100, 400, 'EUR',
                    'EXPEDITE', 'DRAFT', 'HIGH', %s
                )
                """,
                (
                    uuid4(),
                    _seed_agent_run(conn, new_scenario),
                    new_scenario,
                    item, f"EXT-{uuid4().hex[:8]}", today + timedelta(days=20),
                    json.dumps({"unit_cost": -5}),
                ),
            )
            _seed_shortage(
                conn, scenario_id=new_scenario, item_id=item, location_id=loc,
                shortage_date=today, shortage_qty=40, severity_score=120,
            )
            evaluate_and_persist(conn, str(new_scenario))
            conn.commit()

        clear = _mint_token(migrated_db, actor_kind="service", scopes=["read"])
        resp = api_client.get(
            "/v1/outcomes/summary",
            params={"scenario_id": str(new_scenario)},
            headers=_bearer(clear),
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["cost_of_inaction_usd"] == pytest.approx(160.0), (
            "negative evidence unit_cost must be rejected, fallback (400/100=4.0) used"
        )
        assert body["cost_of_inaction_usd"] != pytest.approx(-200.0)

    def test_scenario_isolation(self, api_client, new_scenario, migrated_db):
        """The summary is scenario-scoped: a DIFFERENT empty scenario sees none of
        new_scenario's seeded outcomes (its KPIs stay empty)."""
        _seed_full_scenario(migrated_db, new_scenario)
        other = uuid4()
        with _db_conn(migrated_db) as c:
            c.execute(
                "INSERT INTO scenarios (scenario_id, name, is_baseline, status) "
                "VALUES (%s, %s, FALSE, 'active')",
                (other, f"outcome-other-{other}"),
            )
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["read"])
        try:
            resp = api_client.get(
                "/v1/outcomes/summary",
                params={"scenario_id": str(other)},
                headers=_bearer(clear),
            )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["reco_total_count"] == 0, "another scenario sees none of the seeded recos"
            assert body["avoided_basis_count"] == 0
            assert body["avg_fva_wape"] is None
        finally:
            with _db_conn(migrated_db) as c:
                c.execute("DELETE FROM scenarios WHERE scenario_id = %s", (other,))


# ===========================================================================
# 4. POST /v1/outcomes/evaluate — governed on-demand pass
# ===========================================================================


class TestEvaluatePost:
    def test_evaluate_requires_ingest_scope(self, api_client, new_scenario, migrated_db):
        """A write of persistent verdicts requires `ingest` — a read-only token is
        blocked on the scope floor."""
        clear = _mint_token(migrated_db, actor_kind="agent", scopes=["read"])
        resp = api_client.post(
            "/v1/outcomes/evaluate",
            params={"scenario_id": str(new_scenario)},
            json={},
            headers=_bearer(clear),
        )
        assert resp.status_code == 403, resp.text
        assert "ingest" in resp.json()["detail"].lower()

    def test_evaluate_401_without_token(self, api_client, new_scenario):
        resp = api_client.post(
            "/v1/outcomes/evaluate",
            params={"scenario_id": str(new_scenario)},
            json={},
        )
        assert resp.status_code == 401, resp.text

    def test_evaluate_201_persists_and_returns_summary(
        self, api_client, new_scenario, migrated_db
    ):
        """POST with ingest -> 201, persists the verdicts, returns the metrics.
        Seed an AVOIDED-shaped reco, evaluate via the endpoint, assert the row
        landed and the response echoes the by_status count."""
        with _db_conn(migrated_db) as conn:
            item = _seed_item(conn)
            loc = _seed_location(conn)
            today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
            reco_id = _seed_reco(
                conn, scenario_id=new_scenario, item_id=item, status="APPROVED",
                shortage_date=today + timedelta(days=20), deficit_qty=100, unit_cost=3.0,
            )
            _seed_snapshot(
                conn, scenario_id=new_scenario, item_id=item, location_id=loc, as_of_date=today
            )
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["ingest"])

        resp = api_client.post(
            "/v1/outcomes/evaluate",
            params={"scenario_id": str(new_scenario)},
            json={},
            headers=_bearer(clear),
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["scenario_id"] == str(new_scenario)
        assert body["evaluated"] >= 1
        assert body["upserted"] >= 1
        assert body["by_status"]["AVOIDED"] >= 1

        with _db_conn(migrated_db) as conn:
            row = conn.execute(
                "SELECT evaluation_status, avoided_severity_usd FROM recommendation_outcomes "
                "WHERE recommendation_id = %s",
                (reco_id,),
            ).fetchone()
        assert row is not None
        assert row["evaluation_status"] == "AVOIDED"
        assert row["avoided_severity_usd"] == 300

    def test_evaluate_503_when_kill_switch_off_writes_nothing(
        self, api_client, new_scenario, migrated_db
    ):
        """OOTILS_OUTCOMES_ENABLED falsy -> 503 on the evaluate verb (checked
        after auth/scope but before the DB), and NO outcome row is written."""
        with _db_conn(migrated_db) as conn:
            item = _seed_item(conn)
            loc = _seed_location(conn)
            today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
            reco_id = _seed_reco(
                conn, scenario_id=new_scenario, item_id=item, status="APPROVED",
                shortage_date=today + timedelta(days=20), deficit_qty=100,
            )
            _seed_snapshot(
                conn, scenario_id=new_scenario, item_id=item, location_id=loc, as_of_date=today
            )
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["ingest"])

        prev = os.environ.get("OOTILS_OUTCOMES_ENABLED")
        os.environ["OOTILS_OUTCOMES_ENABLED"] = "0"
        try:
            resp = api_client.post(
                "/v1/outcomes/evaluate",
                params={"scenario_id": str(new_scenario)},
                json={},
                headers=_bearer(clear),
            )
        finally:
            if prev is None:
                del os.environ["OOTILS_OUTCOMES_ENABLED"]
            else:
                os.environ["OOTILS_OUTCOMES_ENABLED"] = prev
        assert resp.status_code == 503, resp.text

        with _db_conn(migrated_db) as conn:
            n = conn.execute(
                "SELECT COUNT(*) AS n FROM recommendation_outcomes WHERE recommendation_id = %s",
                (reco_id,),
            ).fetchone()["n"]
        assert n == 0, "a disabled evaluator must not have written any outcome row"

    def test_evaluate_honours_explicit_as_of(self, api_client, new_scenario, migrated_db):
        """A body-supplied as_of anchors the verdict's evaluated_as_of and is
        echoed in the response."""
        with _db_conn(migrated_db) as conn:
            item = _seed_item(conn)
            loc = _seed_location(conn)
            today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
            past = today - timedelta(days=5)
            reco_id = _seed_reco(
                conn, scenario_id=new_scenario, item_id=item, status="APPROVED",
                shortage_date=today + timedelta(days=20), deficit_qty=100, unit_cost=3.0,
            )
            _seed_snapshot(
                conn, scenario_id=new_scenario, item_id=item, location_id=loc, as_of_date=past
            )
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["ingest"])

        resp = api_client.post(
            "/v1/outcomes/evaluate",
            params={"scenario_id": str(new_scenario)},
            json={"as_of": past.isoformat()},
            headers=_bearer(clear),
        )
        assert resp.status_code == 201, resp.text
        assert resp.json()["evaluated_as_of"] == past.isoformat()

        with _db_conn(migrated_db) as conn:
            row = conn.execute(
                "SELECT evaluated_as_of FROM recommendation_outcomes WHERE recommendation_id = %s",
                (reco_id,),
            ).fetchone()
        assert str(row["evaluated_as_of"]) == past.isoformat()
