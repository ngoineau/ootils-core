"""
tests/integration/test_forecast_watcher_integration.py — chantier DEM-1 PR-2.

DB-backed coverage of the GOVERNED forecast-drift emitter
(scripts/agent_forecast_watcher.py) against a real Postgres, no mocks. The
watcher is a thin orchestrator over pure helpers (classify_drift /
relative_bias / drift_recommendation_id, covered by tests/test_forecast_watcher.py);
this file asserts the pieces the pure tests cannot: the real DISTINCT-ON read of
the latest baseline Pyramide run joined to its aggregate accuracy row (horizon
NULL) + the mean-forecast bias scale, the idempotent upsert (deterministic uuid5
+ ON CONFLICT: live-DRAFT no-op / EXPIRED-tombstone reactivation, DO UPDATE
scoped to status='EXPIRED'), the stale-DRAFT expiration (the #346 supersede
pattern), the None-honest DB paths (mean_forecast NULL, mase NULL), the free
AN-1 recommendation_created event (migration 071,
forecast_drift_recommendations in emit._RECO_TABLES), the --dry-run no-write,
and the baseline-only exit-2 guard — end to end on a seeded accuracy dataset.

The sibling of tests/integration/test_reschedule_watcher_integration.py (#346)
and test_transfer_watcher_integration.py (#395) — same governed-watcher
invariants, applied to the demand echelon.

The ten cases (one test each):
  1. Emission        — a degraded-accuracy series (mase 2.0) -> ONE DRAFT in
                       forecast_drift_recommendations with the right drift_kind,
                       plus EXACTLY ONE recommendation_created event keyed by the
                       run's agent_run_id (events.new_text).
  2. Stability       — re-running on the IDENTICAL accuracy state inserts ZERO
                       new rows and ZERO new events (deterministic uuid5; a live
                       identical DRAFT is a no-op). THE idempotence invariant.
  3. Under threshold — a below-threshold series emits 0 recos / 0 events; AND a
                       prior DRAFT of this agent whose drift no longer fires
                       flips to EXPIRED (the supersede pattern).
  4. Kind change     — a series that goes MASE_DEGRADED -> BOTH (bias added
                       between runs) yields a new BOTH DRAFT and EXPIRES the old
                       MASE_DEGRADED one (a change of drift_kind mints a new id).
  5. None-honest     — bias present but NO forecast_values (mean_forecast NULL)
                       => NO BIAS_SUSTAINED; mase NULL + high bias + values
                       present => BIAS_SUSTAINED alone (mase column NULL).
  6. Dry-run         — --dry-run writes nothing: 0 agent_runs, 0 recos, 0 events.
  7. Baseline-only   — --scenario <fork> exits 2 with no write of any kind.
  8. Reactivation    — THE tombstone regression (adversarial-review MAJEUR): a
                       recurring SAME-kind drift re-derives the SAME uuid5, which
                       used to hit an EXPIRED row's ON CONFLICT DO NOTHING and
                       stay dead forever (invisible recurring drift, 0 event).
                       The fixed upsert's DO UPDATE ... WHERE status='EXPIRED'
                       arm must flip the tombstone back to DRAFT — agent_run_id
                       re-stamped to the CURRENT run, measures/evidence
                       refreshed — and RE-EMIT recommendation_created (the
                       re-stamped agent_run_id makes the AN-1 count non-zero).
                       Metrics: recommendations_reactivated=1, inserted=0.
  9. Oscillation     — MASE -> BOTH -> MASE: the BOTH run expires the MASE DRAFT
                       and inserts a genuinely new BOTH DRAFT; the return-to-MASE
                       run REACTIVATES the MASE tombstone and expires the BOTH
                       DRAFT. At every step exactly ONE live DRAFT and exactly
                       ONE event for that run.
 10. Human statuses  — a REJECTED row is NEVER reactivated by a recurring drift:
                       the DO UPDATE arm is scoped to status='EXPIRED' only —
                       REVIEWED/APPROVED/REJECTED/APPLIED belong to the #341
                       human state machine and stay untouched (row frozen,
                       agent_run_id unchanged, 0 event).

Determinism: every date is anchored on the DB-side CURRENT_DATE (never Python
now()), like the sibling watcher seeds. Each test resets the Pyramide + agent +
event tables (FK-ordered: the new forecast_drift_recommendations table CASCADEs
before items/scenarios, per the FK RESTRICT lesson) so the idempotence /
expiration assertions never bleed across the module-scoped DB.
"""
from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from .conftest import requires_db

# Import seam: agent_forecast_watcher + mrp_core (shim) + agent_governance live
# under scripts/ (outside the package), exactly as the sibling watcher tests.
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import agent_forecast_watcher  # noqa: E402
import mrp_core as core  # noqa: E402
from agent_governance import decision_level  # noqa: E402

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

pytestmark = [requires_db, pytest.mark.smoke]

BASELINE = core.BASELINE
AGENT = "forecast_watcher"


# ---------------------------------------------------------------------------
# Drive + read helpers.
# ---------------------------------------------------------------------------
def _run(dsn, scenario=None, dry_run=False):
    """Drive the watcher in-process (main(argv) -> int), like the fleet smoke.
    --allow-dev because guard_db refuses a bare ootils_dev otherwise (the sibling
    watcher smokes pass the same flag)."""
    argv = ["--dsn", dsn, "--allow-dev"]
    if scenario is not None:
        argv += ["--scenario", str(scenario)]
    if dry_run:
        argv += ["--dry-run"]
    return agent_forecast_watcher.main(argv)


def _drafts(dsn, scenario=BASELINE):
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        return conn.execute(
            "SELECT * FROM forecast_drift_recommendations "
            "WHERE agent_name=%s AND scenario_id=%s AND status='DRAFT' "
            "ORDER BY drift_kind",
            (AGENT, str(scenario)),
        ).fetchall()


def _count_recos(dsn, scenario=BASELINE):
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        return conn.execute(
            "SELECT COUNT(*) AS n FROM forecast_drift_recommendations "
            "WHERE agent_name=%s AND scenario_id=%s",
            (AGENT, str(scenario)),
        ).fetchone()["n"]


def _count_agent_runs(dsn):
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        return conn.execute(
            "SELECT COUNT(*) AS n FROM agent_runs WHERE agent_name=%s", (AGENT,)
        ).fetchone()["n"]


def _latest_completed_run(dsn):
    """The most recent COMPLETED agent_runs row for this agent (id + metrics)."""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        return conn.execute(
            "SELECT agent_run_id, metrics FROM agent_runs "
            "WHERE agent_name=%s AND status='COMPLETED' "
            "ORDER BY started_at DESC LIMIT 1",
            (AGENT,),
        ).fetchone()


def _reco_events_for_run(dsn, run_id):
    """recommendation_created events keyed by this run's agent_run_id — the AN-1
    keyset (emit stores str(agent_run_id) in events.new_text)."""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        return conn.execute(
            "SELECT * FROM events WHERE event_type='recommendation_created' "
            "AND new_text=%s",
            (str(run_id),),
        ).fetchall()


def _count_reco_events(dsn, scenario=BASELINE):
    """Every recommendation_created event this agent emitted on the scenario
    (old_text carries the agent name, ADR-027 run-granularity)."""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        return conn.execute(
            "SELECT COUNT(*) AS n FROM events "
            "WHERE event_type='recommendation_created' "
            "AND old_text=%s AND scenario_id=%s",
            (AGENT, str(scenario)),
        ).fetchone()["n"]


# ---------------------------------------------------------------------------
# Seed helpers. Each test gets a FUNCTION-scoped, freshly-truncated Pyramide +
# agent + event surface so idempotence / expiration assertions never bleed.
# ---------------------------------------------------------------------------
def _reset(conn):
    """Wipe the per-test Pyramide/accuracy/forecast surface + agent artifacts +
    fleet events. Keeps the migrated schema (and the migration-002 baseline
    scenario) intact; only clears what this file seeds. The NEW
    forecast_drift_recommendations table is listed FIRST so it is emptied before
    items/scenarios it FK-references (the FK RESTRICT lesson); CASCADE + RESTART
    IDENTITY sweeps the rest. Forks created by prior tests are harmless (they own
    no Pyramide run) so scenarios is deliberately NOT truncated (would drop
    baseline)."""
    conn.execute(
        "TRUNCATE forecast_drift_recommendations, pyramide_accuracy_metrics, "
        "pyramide_snapshots, pyramide_runs, forecast_values, forecasts, "
        "agent_runs, events, items, locations RESTART IDENTITY CASCADE"
    )


def _seed_series(
    conn,
    *,
    item_ext,
    loc_ext,
    mase,
    bias,
    scenario=BASELINE,
    mean_forecast=Decimal("100"),
    with_values=True,
    n_cutoffs=10,
    n_observations=50,
    wape=None,
    smape=None,
    granularity="weekly",
    status="generated",
):
    """Seed one baseline Pyramide run for a fresh (item, location) + its aggregate
    accuracy row (horizon NULL) + optionally forecast_values whose mean equals
    ``mean_forecast`` (the |bias|/mean bias scale the watcher reads via its
    LATERAL join). Returns (item_id, location_id, run_id).

    Column facts verified against migrations: forecasts/forecast_values/
    pyramide_runs all take method 'MA' (SEASONAL is excluded from the
    pyramide_runs CHECK, migration 038); granularity in daily/weekly/monthly;
    pyramide_runs.status default 'generated' (only 'failed' is skipped by the
    watcher read); source_history_count NOT NULL >= 0; pyramide_accuracy_metrics
    n_cutoffs / n_observations NOT NULL >= 0, all metric columns nullable
    (None-honest)."""
    item_id = conn.execute(
        "INSERT INTO items (external_id, name, item_type, standard_cost, cost_currency) "
        "VALUES (%s, %s, 'finished_good', 40.0, 'EUR') RETURNING item_id",
        (item_ext, item_ext),
    ).fetchone()["item_id"]
    loc_id = conn.execute(
        "INSERT INTO locations (name, location_type, external_id) "
        "VALUES (%s, 'dc', %s) RETURNING location_id",
        (loc_ext, loc_ext),
    ).fetchone()["location_id"]
    forecast_id = conn.execute(
        "INSERT INTO forecasts (item_id, location_id, scenario_id, horizon_start, "
        " horizon_end, granularity, method) "
        "VALUES (%s, %s, %s, CURRENT_DATE, CURRENT_DATE + 30, %s, 'MA') "
        "RETURNING forecast_id",
        (item_id, loc_id, str(scenario), granularity),
    ).fetchone()["forecast_id"]
    if with_values:
        # Three equal buckets so AVG(quantity) == mean_forecast exactly (the
        # deterministic bias scale). quantity >= 0 (migration-026 CHECK).
        for d in range(3):
            conn.execute(
                "INSERT INTO forecast_values (forecast_id, forecast_date, quantity, method) "
                "VALUES (%s, CURRENT_DATE + %s, %s, 'MA')",
                (forecast_id, d, mean_forecast),
            )
    run_id = conn.execute(
        "INSERT INTO pyramide_runs (forecast_id, item_id, location_id, scenario_id, "
        " horizon_start, horizon_end, granularity, method, source_history_count, status) "
        "VALUES (%s, %s, %s, %s, CURRENT_DATE, CURRENT_DATE + 30, %s, 'MA', %s, %s) "
        "RETURNING run_id",
        (forecast_id, item_id, loc_id, str(scenario), granularity, n_observations, status),
    ).fetchone()["run_id"]
    conn.execute(
        "INSERT INTO pyramide_accuracy_metrics (run_id, horizon, mase, wape, smape, "
        " bias, coverage, n_cutoffs, n_observations) "
        "VALUES (%s, NULL, %s, %s, %s, %s, NULL, %s, %s)",
        (run_id, mase, wape, smape, bias, n_cutoffs, n_observations),
    )
    return item_id, loc_id, run_id


def _set_accuracy(conn, run_id, *, mase=None, bias=None):
    """Mutate the aggregate (horizon NULL) accuracy row of an existing run
    in-place (used to shift a series' drift_kind between two watcher runs)."""
    conn.execute(
        "UPDATE pyramide_accuracy_metrics SET mase=%s, bias=%s "
        "WHERE run_id=%s AND horizon IS NULL",
        (mase, bias, run_id),
    )


# ===========================================================================
# 1. Emission — a degraded series -> one DRAFT + EXACTLY one AN-1 event.
# ===========================================================================
def test_degraded_series_emits_draft_and_one_event(migrated_db):
    """A latest baseline run whose aggregate MASE is 2.0 (> 1.3) with a low bias
    ratio (10/100 = 0.1 < 0.3) drifts as MASE_DEGRADED -> a single DRAFT in
    forecast_drift_recommendations carrying the typed measures + pyramide_run_id,
    AND exactly ONE recommendation_created event keyed to the run's agent_run_id
    (events.new_text), new_quantity=1 (forecast_drift_recommendations is in
    emit._RECO_TABLES -> the AN-1 event is free)."""
    dsn = migrated_db
    suffix = uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _reset(conn)
        item_id, loc_id, run_id = _seed_series(
            conn, item_ext=f"ITM-{suffix}", loc_ext=f"DC-{suffix}",
            mase=Decimal("2.0"), bias=Decimal("10"),
        )

    assert _run(dsn) == 0
    rows = _drafts(dsn)
    assert len(rows) == 1, "exactly one drift DRAFT expected"
    r = rows[0]
    assert r["action"] == "FORECAST_DRIFT"
    assert r["decision_level"] == decision_level("FORECAST_DRIFT") == "L1"
    assert r["drift_kind"] == "MASE_DEGRADED"
    assert r["status"] == "DRAFT"
    assert str(r["item_id"]) == str(item_id)
    assert str(r["location_id"]) == str(loc_id)
    assert str(r["pyramide_run_id"]) == str(run_id)
    assert r["cadence"] == "weekly"
    assert r["mase"] == Decimal("2.0")
    assert r["bias"] == Decimal("10")
    # tracking_ratio = |bias|/mean_forecast = 10/100 = 0.1, below the 0.3 bias
    # threshold => bias did NOT co-fire (pure MASE_DEGRADED).
    assert r["tracking_ratio"] is not None and r["tracking_ratio"] < Decimal("0.3")
    assert r["threshold_mase"] == Decimal("1.3")
    assert r["threshold_bias_ratio"] == Decimal("0.3")
    assert r["confidence"] is None
    assert r["evidence"] is not None and r["evidence"]["drift_kind"] == "MASE_DEGRADED"

    # EXACTLY ONE AN-1 event, keyed by the run's agent_run_id (the keyset).
    run = _latest_completed_run(dsn)
    events = _reco_events_for_run(dsn, run["agent_run_id"])
    assert len(events) == 1, "exactly one recommendation_created event for the run"
    ev = events[0]
    assert ev["old_text"] == AGENT           # agent name
    assert str(ev["scenario_id"]) == str(BASELINE)
    assert ev["new_quantity"] == 1           # one reco created in the run
    # The run metrics self-report the single insert (auditable).
    assert run["metrics"]["recommendations_inserted"] == 1
    assert run["metrics"]["drift_detected"] == 1


# ===========================================================================
# 2. Stability — re-run on the identical state inserts ZERO rows / ZERO events.
# ===========================================================================
def test_rerun_identical_state_inserts_zero_rows_and_events(migrated_db):
    """The deterministic-uuid5 idempotence invariant. After a first run, re-running
    on the UNCHANGED accuracy state re-derives the SAME recommendation_id; the
    upsert's WHERE status='EXPIRED' guard is false for a live DRAFT, so run 2 is
    a strict no-op — zero new rows AND (count-gated emission) zero new events,
    with the metrics self-reporting the no-op (reactivated=0 too)."""
    dsn = migrated_db
    suffix = uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _reset(conn)
        _seed_series(
            conn, item_ext=f"ITM-{suffix}", loc_ext=f"DC-{suffix}",
            mase=Decimal("2.0"), bias=Decimal("10"),
        )

    assert _run(dsn) == 0
    recos_1 = _count_recos(dsn)
    events_1 = _count_reco_events(dsn)
    assert recos_1 == 1
    assert events_1 == 1
    drafts_1 = {r["recommendation_id"] for r in _drafts(dsn)}

    # --- Second run on the identical state: no new rows, no new events.
    assert _run(dsn) == 0
    assert _count_recos(dsn) == recos_1, "stability broken: a new drift row on an unchanged state"
    assert _count_reco_events(dsn) == events_1, "stability broken: a new event on an unchanged state"
    assert {r["recommendation_id"] for r in _drafts(dsn)} == drafts_1, "the same DRAFT must stay live"

    run2 = _latest_completed_run(dsn)
    m = run2["metrics"]
    assert m["recommendations_inserted"] == 0
    assert m.get("recommendations_reactivated", 0) == 0, (
        "a live identical DRAFT is a no-op, never a reactivation"
    )
    assert m["recommendations_idempotent_noop"] == m["recommendations_affirmed"] == 1


# ===========================================================================
# 3. Under threshold — 0 reco / 0 event, and a prior DRAFT is EXPIRED.
# ===========================================================================
def test_under_thresholds_emits_nothing_and_expires_prior_draft(migrated_db):
    """A series with mase 1.0 (<= 1.3) and a low bias drifts as nothing -> 0 new
    recos, 0 events. AND a prior DRAFT of THIS agent on this scenario, whose drift
    no longer fires, must flip to EXPIRED (the supersede pattern: a run that
    affirms no id expires every live DRAFT of the agent+scenario)."""
    dsn = migrated_db
    suffix = uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _reset(conn)
        item_id, loc_id, _ = _seed_series(
            conn, item_ext=f"ITM-{suffix}", loc_ext=f"DC-{suffix}",
            mase=Decimal("1.0"), bias=Decimal("5"),
        )
        # A prior DRAFT of this agent whose drift is now resolved. Valid FKs: a
        # fresh agent_runs row + the seeded item/location.
        prior_run = conn.execute(
            "INSERT INTO agent_runs (agent_name, scenario_id, status) "
            "VALUES (%s, %s, 'COMPLETED') RETURNING agent_run_id",
            (AGENT, str(BASELINE)),
        ).fetchone()["agent_run_id"]
        prior_id = uuid4()
        conn.execute(
            "INSERT INTO forecast_drift_recommendations "
            "(recommendation_id, agent_name, agent_run_id, scenario_id, item_id, "
            " location_id, action, decision_level, drift_kind, status) "
            "VALUES (%s, %s, %s, %s, %s, %s, 'FORECAST_DRIFT', 'L1', 'MASE_DEGRADED', 'DRAFT')",
            (prior_id, AGENT, prior_run, str(BASELINE), item_id, loc_id),
        )

    assert _run(dsn) == 0

    # No live DRAFT and no new event (the run affirmed nothing, inserted nothing).
    assert _drafts(dsn) == []
    assert _count_reco_events(dsn) == 0, "an empty run must announce nothing"

    # The prior DRAFT is now EXPIRED (superseded by an empty state).
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        prior = conn.execute(
            "SELECT status FROM forecast_drift_recommendations WHERE recommendation_id=%s",
            (prior_id,),
        ).fetchone()
    assert prior["status"] == "EXPIRED", "a resolved prior DRAFT must flip to EXPIRED"


# ===========================================================================
# 4. Kind change — MASE_DEGRADED -> BOTH mints a new DRAFT, expires the old kind.
# ===========================================================================
def test_drift_kind_change_supersedes_prior_kind(migrated_db):
    """A change of drift_kind is a genuinely NEW recommendation (uuid5 keyed on
    scenario/item/location/drift_kind). Run 1 (mase 2.0, low bias) drafts
    MASE_DEGRADED. After adding a high bias (50/100 = 0.5 > 0.3) to the SAME run's
    accuracy row, run 2 classifies BOTH -> a new BOTH DRAFT is inserted and the
    stale MASE_DEGRADED DRAFT is EXPIRED."""
    dsn = migrated_db
    suffix = uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _reset(conn)
        _item_id, _loc_id, run_id = _seed_series(
            conn, item_ext=f"ITM-{suffix}", loc_ext=f"DC-{suffix}",
            mase=Decimal("2.0"), bias=Decimal("10"),
        )

    assert _run(dsn) == 0
    run1 = _drafts(dsn)
    assert len(run1) == 1 and run1[0]["drift_kind"] == "MASE_DEGRADED"
    mase_only_id = run1[0]["recommendation_id"]

    # Add a sustained bias to the SAME run's aggregate row: 50/100 = 0.5 -> BOTH.
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _set_accuracy(conn, run_id, mase=Decimal("2.0"), bias=Decimal("50"))

    assert _run(dsn) == 0
    live = _drafts(dsn)
    assert len(live) == 1, "exactly one live DRAFT after the kind change"
    assert live[0]["drift_kind"] == "BOTH"
    assert live[0]["recommendation_id"] != mase_only_id, "BOTH must mint a new id"
    assert live[0]["tracking_ratio"] > Decimal("0.3")

    # The prior MASE_DEGRADED DRAFT is now EXPIRED (superseded by the new kind).
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        old = conn.execute(
            "SELECT status FROM forecast_drift_recommendations WHERE recommendation_id=%s",
            (mase_only_id,),
        ).fetchone()
    assert old["status"] == "EXPIRED", "the superseded MASE_DEGRADED DRAFT must EXPIRE"


# ===========================================================================
# 5. None-honest DB — mean_forecast NULL blocks bias; mase NULL keeps bias alone.
# ===========================================================================
def test_none_honest_bias_scale_and_mase(migrated_db):
    """Two series in ONE baseline run assert both None-honest paths:
      A) high bias but NO forecast_values (mean_forecast NULL) => the bias cannot
         be normalized => NO BIAS_SUSTAINED (and mase NULL => no drift at all);
      B) mase NULL + high bias + values present (mean 100) => BIAS_SUSTAINED
         ALONE, with the mase column persisted NULL (never a masked 0)."""
    dsn = migrated_db
    suffix = uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _reset(conn)
        # A: bias present, but no scale to normalize it -> ignored.
        item_a, _, _ = _seed_series(
            conn, item_ext=f"ITM-A-{suffix}", loc_ext=f"DC-A-{suffix}",
            mase=None, bias=Decimal("50"), with_values=False,
        )
        # B: no mase, high bias, real scale -> BIAS_SUSTAINED only.
        item_b, _, _ = _seed_series(
            conn, item_ext=f"ITM-B-{suffix}", loc_ext=f"DC-B-{suffix}",
            mase=None, bias=Decimal("50"), with_values=True,
        )

    assert _run(dsn) == 0
    rows = _drafts(dsn)
    assert len(rows) == 1, "only series B drifts (A has no bias scale, no mase)"
    r = rows[0]
    assert str(r["item_id"]) == str(item_b)
    assert str(r["item_id"]) != str(item_a)
    assert r["drift_kind"] == "BIAS_SUSTAINED"
    assert r["mase"] is None, "None-honest: a NULL mase is persisted NULL, never 0"
    assert r["bias"] == Decimal("50")
    assert r["tracking_ratio"] > Decimal("0.3")


# ===========================================================================
# 6. Dry-run — writes nothing at all.
# ===========================================================================
def test_dry_run_writes_nothing(migrated_db):
    """--dry-run classifies but never opens a governed run: 0 agent_runs, 0
    recommendations, 0 events — even though the seeded series really does drift."""
    dsn = migrated_db
    suffix = uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _reset(conn)
        _seed_series(
            conn, item_ext=f"ITM-{suffix}", loc_ext=f"DC-{suffix}",
            mase=Decimal("2.0"), bias=Decimal("10"),
        )

    assert _run(dsn, dry_run=True) == 0
    assert _count_agent_runs(dsn) == 0, "dry-run must not open a governed run"
    assert _count_recos(dsn) == 0, "dry-run must not write any recommendation"
    assert _count_reco_events(dsn) == 0, "dry-run must not emit any event"

    # And a real run afterwards DOES write (proves the dry-run seed truly drifts).
    assert _run(dsn) == 0
    assert _count_recos(dsn) == 1


# ===========================================================================
# 7. Baseline-only — a fork scenario is refused with exit 2, no write.
# ===========================================================================
def test_fork_scenario_refused_exit_2_no_write(migrated_db):
    """The watcher is BASELINE-ONLY in V1 (drift is the REAL observed accuracy;
    a fork is simulated, ADR-030 rationale). --scenario <fork> exits 2 BEFORE any
    connection/governed run — 0 agent_runs, 0 recos, 0 events on both scenarios."""
    dsn = migrated_db
    suffix = uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _reset(conn)
        fork_id = conn.execute(
            "INSERT INTO scenarios (name, is_baseline, status) "
            "VALUES (%s, FALSE, 'active') RETURNING scenario_id",
            (f"fdr-fork-{suffix}",),
        ).fetchone()["scenario_id"]
        # A degraded series on the fork: the guard must reject BEFORE it is read.
        _seed_series(
            conn, item_ext=f"ITM-{suffix}", loc_ext=f"DC-{suffix}",
            mase=Decimal("2.0"), bias=Decimal("50"), scenario=fork_id,
        )

    assert _run(dsn, scenario=fork_id) == 2, "a fork must be refused with exit code 2"
    assert _count_agent_runs(dsn) == 0, "the refused run must open no governed run"
    assert _count_recos(dsn, scenario=fork_id) == 0
    assert _count_recos(dsn, scenario=BASELINE) == 0
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        n_events = conn.execute(
            "SELECT COUNT(*) AS n FROM events WHERE event_type='recommendation_created'"
        ).fetchone()["n"]
    assert n_events == 0, "the refused run must emit no event"


# ===========================================================================
# 8. Reactivation — an EXPIRED tombstone whose SAME-kind drift recurs comes back
#    to life (the adversarial-review MAJEUR regression).
# ===========================================================================
def test_expired_tombstone_reactivates_on_recurring_drift(migrated_db):
    """drift -> resolved -> SAME-kind re-drift. The uuid5 is keyed on
    (scenario, item, location, drift_kind), so the recurrence re-derives the SAME
    recommendation_id; a plain DO NOTHING would leave the EXPIRED tombstone dead
    forever — the recurring drift invisible, zero event. The fixed upsert
    (DO UPDATE ... WHERE status='EXPIRED') must instead flip the SAME row back to
    DRAFT with the agent_run_id re-stamped to the CURRENT run (which makes the
    AN-1 count-by-agent_run_id non-zero -> the event is re-emitted), the measures
    and evidence refreshed to the new drift, and the run metrics reporting
    recommendations_reactivated=1 / recommendations_inserted=0."""
    dsn = migrated_db
    suffix = uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _reset(conn)
        _item_id, _loc_id, run_id = _seed_series(
            conn, item_ext=f"ITM-{suffix}", loc_ext=f"DC-{suffix}",
            mase=Decimal("2.0"), bias=Decimal("10"),
        )

    # --- Run 1: the drift fires -> one DRAFT + one event.
    assert _run(dsn) == 0
    run1_drafts = _drafts(dsn)
    assert len(run1_drafts) == 1 and run1_drafts[0]["drift_kind"] == "MASE_DEGRADED"
    reco_id = run1_drafts[0]["recommendation_id"]
    run1_agent_run_id = run1_drafts[0]["agent_run_id"]
    assert _count_reco_events(dsn) == 1

    # --- Resolve the drift (mase back under threshold) -> run 2 expires it.
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _set_accuracy(conn, run_id, mase=Decimal("1.0"), bias=Decimal("10"))
    assert _run(dsn) == 0
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        tomb = conn.execute(
            "SELECT status FROM forecast_drift_recommendations WHERE recommendation_id=%s",
            (reco_id,),
        ).fetchone()
    assert tomb["status"] == "EXPIRED", "the resolved DRAFT must first become a tombstone"
    assert _count_reco_events(dsn) == 1, "the expiring run must emit no event"
    run2 = _latest_completed_run(dsn)
    assert _reco_events_for_run(dsn, run2["agent_run_id"]) == []

    # --- The SAME kind re-drifts (mase 2.5) -> run 3 must REACTIVATE the row.
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _set_accuracy(conn, run_id, mase=Decimal("2.5"), bias=Decimal("10"))
    assert _run(dsn) == 0

    live = _drafts(dsn)
    assert len(live) == 1, "the recurring drift must be visible as a live DRAFT again"
    r = live[0]
    assert r["recommendation_id"] == reco_id, (
        "reactivation must resurrect the SAME row (same uuid5), never mint a duplicate"
    )
    assert r["status"] == "DRAFT"
    run3 = _latest_completed_run(dsn)
    assert str(r["agent_run_id"]) == str(run3["agent_run_id"]), (
        "the reactivated row must be re-stamped to the CURRENT run"
    )
    assert str(r["agent_run_id"]) != str(run1_agent_run_id)
    assert r["mase"] == Decimal("2.5"), "the measures must be refreshed to the new drift"
    assert r["evidence"]["mase"] == 2.5, "the evidence trail must be refreshed too"
    assert _count_recos(dsn) == 1, "still exactly one row — reactivated, not duplicated"

    # The recurrence is ANNOUNCED: exactly one new event, keyed to run 3.
    run3_events = _reco_events_for_run(dsn, run3["agent_run_id"])
    assert len(run3_events) == 1, "a reactivated drift must re-emit recommendation_created"
    assert run3_events[0]["new_quantity"] == 1
    assert _count_reco_events(dsn) == 2  # run 1 + run 3 (never run 2)

    # Metrics contract of the fix.
    m = run3["metrics"]
    assert m["recommendations_reactivated"] == 1
    assert m["recommendations_inserted"] == 0


# ===========================================================================
# 9. Oscillation — MASE -> BOTH -> MASE: expire/insert then reactivate/expire.
# ===========================================================================
def test_oscillation_mase_both_mase_keeps_one_live_draft_per_step(migrated_db):
    """A series oscillating between drift kinds. Step 2 (BOTH) expires the MASE
    DRAFT and inserts a genuinely new BOTH row (a kind change mints a new uuid5);
    step 3 (back to MASE) REACTIVATES the MASE tombstone and expires the BOTH
    DRAFT. At every step: exactly ONE live DRAFT, exactly ONE event for that run,
    and only ever TWO physical rows (one per kind — recycled, never duplicated)."""
    dsn = migrated_db
    suffix = uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _reset(conn)
        _item_id, _loc_id, run_id = _seed_series(
            conn, item_ext=f"ITM-{suffix}", loc_ext=f"DC-{suffix}",
            mase=Decimal("2.0"), bias=Decimal("10"),
        )

    # --- Step 1: MASE_DEGRADED (bias ratio 0.1 < 0.3).
    assert _run(dsn) == 0
    step1 = _drafts(dsn)
    assert [r["drift_kind"] for r in step1] == ["MASE_DEGRADED"]
    mase_id = step1[0]["recommendation_id"]
    run1 = _latest_completed_run(dsn)
    assert len(_reco_events_for_run(dsn, run1["agent_run_id"])) == 1

    # --- Step 2: bias climbs (50/100 = 0.5) -> BOTH. New id; MASE expires.
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _set_accuracy(conn, run_id, mase=Decimal("2.0"), bias=Decimal("50"))
    assert _run(dsn) == 0
    step2 = _drafts(dsn)
    assert [r["drift_kind"] for r in step2] == ["BOTH"], "exactly one live DRAFT (BOTH)"
    both_id = step2[0]["recommendation_id"]
    assert both_id != mase_id
    run2 = _latest_completed_run(dsn)
    run2_events = _reco_events_for_run(dsn, run2["agent_run_id"])
    assert len(run2_events) == 1 and run2_events[0]["new_quantity"] == 1
    assert run2["metrics"]["recommendations_inserted"] == 1
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        old_mase = conn.execute(
            "SELECT status FROM forecast_drift_recommendations WHERE recommendation_id=%s",
            (mase_id,),
        ).fetchone()
    assert old_mase["status"] == "EXPIRED"

    # --- Step 3: bias falls back (10/100 = 0.1) -> MASE again. The MASE
    # tombstone reactivates; the BOTH DRAFT expires.
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _set_accuracy(conn, run_id, mase=Decimal("2.0"), bias=Decimal("10"))
    assert _run(dsn) == 0
    step3 = _drafts(dsn)
    assert [r["drift_kind"] for r in step3] == ["MASE_DEGRADED"], (
        "exactly one live DRAFT (the reactivated MASE)"
    )
    assert step3[0]["recommendation_id"] == mase_id, "the MASE tombstone is recycled"
    run3 = _latest_completed_run(dsn)
    assert str(step3[0]["agent_run_id"]) == str(run3["agent_run_id"])
    run3_events = _reco_events_for_run(dsn, run3["agent_run_id"])
    assert len(run3_events) == 1 and run3_events[0]["new_quantity"] == 1
    assert run3["metrics"]["recommendations_reactivated"] == 1
    assert run3["metrics"]["recommendations_inserted"] == 0
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        both_row = conn.execute(
            "SELECT status FROM forecast_drift_recommendations WHERE recommendation_id=%s",
            (both_id,),
        ).fetchone()
    assert both_row["status"] == "EXPIRED", "the BOTH DRAFT must expire on the way back"

    # Only ever two physical rows — one per kind, recycled across the oscillation.
    assert _count_recos(dsn) == 2
    # One event per emitting run: steps 1, 2 and 3 all announced.
    assert _count_reco_events(dsn) == 3


# ===========================================================================
# 10. Human statuses are sacred — a REJECTED row is never reactivated.
# ===========================================================================
def test_rejected_row_is_never_reactivated(migrated_db):
    """The reactivation arm is scoped to status='EXPIRED' ONLY. A row a human
    REJECTED through the #341 state machine stays REJECTED even when the same
    drift kind fires again: frozen measures, unchanged agent_run_id, no duplicate
    row, no event — the human verdict outranks the watcher."""
    dsn = migrated_db
    suffix = uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _reset(conn)
        _item_id, _loc_id, run_id = _seed_series(
            conn, item_ext=f"ITM-{suffix}", loc_ext=f"DC-{suffix}",
            mase=Decimal("2.0"), bias=Decimal("10"),
        )

    assert _run(dsn) == 0
    run1_drafts = _drafts(dsn)
    assert len(run1_drafts) == 1
    reco_id = run1_drafts[0]["recommendation_id"]
    run1_agent_run_id = run1_drafts[0]["agent_run_id"]

    # A human rejects the reco (the #341 state machine's terminal human verdict).
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        conn.execute(
            "UPDATE forecast_drift_recommendations SET status='REJECTED', updated_at=now() "
            "WHERE recommendation_id=%s",
            (reco_id,),
        )
        # The SAME kind keeps drifting (even worse): the watcher re-derives the
        # same uuid5 on the next run.
        _set_accuracy(conn, run_id, mase=Decimal("2.5"), bias=Decimal("10"))

    assert _run(dsn) == 0

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT status, mase, agent_run_id FROM forecast_drift_recommendations "
            "WHERE recommendation_id=%s",
            (reco_id,),
        ).fetchone()
    assert row["status"] == "REJECTED", "a human REJECTED verdict must never be reactivated"
    assert row["mase"] == Decimal("2.0"), "the frozen row's measures must not be refreshed"
    assert str(row["agent_run_id"]) == str(run1_agent_run_id), "agent_run_id must not be re-stamped"
    assert _count_recos(dsn) == 1, "no duplicate row may be minted around the REJECTED one"
    assert _drafts(dsn) == [], "no live DRAFT: the REJECTED verdict stands"

    # Nothing was inserted or reactivated -> the run announces nothing.
    run2 = _latest_completed_run(dsn)
    assert _reco_events_for_run(dsn, run2["agent_run_id"]) == []
    assert _count_reco_events(dsn) == 1  # only run 1's original event
    m = run2["metrics"]
    assert m["recommendations_reactivated"] == 0
    assert m["recommendations_inserted"] == 0
