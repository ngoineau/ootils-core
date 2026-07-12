"""
test_emit_stream_event.py — pure unit tests for the fleet-emission helper
(chantier AN-1, #401; src/ootils_core/engine/events/emit.py).

No DB: a recording fake connection captures the SQL + params emit_stream_event
would execute, so we assert the typed-column MAPPING and the fail-loudly
VALIDATION without a live Postgres. The event_type CHECK / stream_seq behaviour
against a real DB is left to the integration suite (see the module docstring
handoff at the bottom of the AN-1 change set).
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.events.emit import (
    FLEET_EVENT_TYPES,
    emit_recommendation_created_for_run,
    emit_stream_event,
)


class _RecordingConn:
    """Minimal psycopg-connection stand-in.

    Records every (sql, params) passed to execute; returns a cursor-like object
    whose fetchone() yields the next queued row. Enough to exercise the pure
    logic of emit_stream_event / emit_recommendation_created_for_run without a DB.
    """

    def __init__(self, fetch_results: list | None = None) -> None:
        self.calls: list[tuple[str, tuple]] = []
        self._fetch_results = list(fetch_results or [])

    def execute(self, sql: str, params: tuple):  # noqa: ANN201 - test double
        self.calls.append((sql, params))
        row = self._fetch_results.pop(0) if self._fetch_results else None

        class _Cur:
            def __init__(self, r):
                self._r = r

            def fetchone(self):
                return self._r

        return _Cur(row)


def _params_of(conn: _RecordingConn, index: int = -1) -> tuple:
    return conn.calls[index][1]


# ---------------------------------------------------------------------------
# Validation — fail loudly
# ---------------------------------------------------------------------------


def test_unknown_event_type_raises_before_any_execute() -> None:
    conn = _RecordingConn()
    with pytest.raises(ValueError, match="unknown fleet event_type"):
        emit_stream_event(conn, "not_a_type", uuid4())
    assert conn.calls == []  # no INSERT attempted


def test_invalid_source_raises_before_any_execute() -> None:
    conn = _RecordingConn()
    with pytest.raises(ValueError, match="invalid source"):
        emit_stream_event(conn, "calc_run_finished", uuid4(), source="bogus")
    assert conn.calls == []


@pytest.mark.parametrize("event_type", sorted(FLEET_EVENT_TYPES))
def test_every_fleet_type_is_accepted(event_type: str) -> None:
    conn = _RecordingConn()
    eid = emit_stream_event(conn, event_type, uuid4())
    assert isinstance(eid, UUID)
    assert len(conn.calls) == 1
    assert conn.calls[0][1][1] == event_type  # event_type is 2nd bound param


# ---------------------------------------------------------------------------
# Param mapping — the typed-column contract (migration 071 header)
# ---------------------------------------------------------------------------


def test_calc_run_finished_column_mapping() -> None:
    conn = _RecordingConn()
    scen = uuid4()
    run_id = uuid4()
    emit_stream_event(
        conn,
        "calc_run_finished",
        scen,
        field_changed="completed",
        new_text=str(run_id),
        new_quantity=42,
    )
    # INSERT column order: event_id, event_type, scenario_id, trigger_node_id,
    # field_changed, new_date, new_quantity, old_text, new_text, source, created_at
    p = _params_of(conn)
    assert p[1] == "calc_run_finished"
    assert p[2] == scen
    assert p[3] is None  # trigger_node_id
    assert p[4] == "completed"  # field_changed
    assert p[5] is None  # new_date
    assert p[6] == 42  # new_quantity
    assert p[7] is None  # old_text
    assert p[8] == str(run_id)  # new_text
    assert p[9] == "engine"  # source default


def test_snapshot_captured_carries_new_date() -> None:
    conn = _RecordingConn()
    d = date(2026, 7, 7)
    emit_stream_event(
        conn,
        "snapshot_captured",
        uuid4(),
        field_changed="snapshot_captured",
        new_date=d,
        new_quantity=Decimal("17"),
        source="api",
    )
    p = _params_of(conn)
    assert p[4] == "snapshot_captured"
    assert p[5] == d  # new_date
    assert p[6] == Decimal("17")  # new_quantity
    assert p[9] == "api"  # source override


def test_recommendation_created_action_and_agent_mapping() -> None:
    conn = _RecordingConn()
    node = uuid4()
    run_id = uuid4()
    emit_stream_event(
        conn,
        "recommendation_created",
        uuid4(),
        trigger_node_id=node,
        field_changed="EXPEDITE",
        old_text="shortage_watcher",
        new_text=str(run_id),
        new_quantity=3,
    )
    p = _params_of(conn)
    assert p[3] == node  # trigger_node_id
    assert p[4] == "EXPEDITE"  # field_changed = action
    assert p[7] == "shortage_watcher"  # old_text = agent
    assert p[8] == str(run_id)  # new_text = run id
    assert p[6] == 3  # new_quantity = count


def test_processed_flag_is_true_in_insert_sql() -> None:
    # Fleet notifications are terminal (nothing to compute) — processed=TRUE so
    # they are never swept into a calc run's event coalescing.
    conn = _RecordingConn()
    emit_stream_event(conn, "outcome_evaluated", uuid4(), new_quantity=1)
    sql = conn.calls[0][0]
    assert "processed" in sql
    assert "TRUE" in sql


# ---------------------------------------------------------------------------
# emit_recommendation_created_for_run — count-gated emission
# ---------------------------------------------------------------------------


def test_reco_created_emits_when_count_positive() -> None:
    # Three COUNT queries (recommendations, parameter_recommendations,
    # forecast_drift_recommendations) then the INSERT. Counts sum to 5 -> emit
    # with new_quantity=5.
    conn = _RecordingConn(fetch_results=[{"n": 2}, {"n": 3}, {"n": 0}])
    run_id = uuid4()
    scen = uuid4()
    eid = emit_recommendation_created_for_run(conn, run_id, scen, "shortage_watcher")
    assert isinstance(eid, UUID)
    # last call is the INSERT; new_quantity (index 6) == 5
    insert_params = conn.calls[-1][1]
    assert insert_params[1] == "recommendation_created"
    assert insert_params[6] == 5
    assert insert_params[7] == "shortage_watcher"  # old_text = agent
    assert insert_params[8] == str(run_id)  # new_text = run id


def test_reco_created_no_emit_when_zero() -> None:
    conn = _RecordingConn(fetch_results=[{"n": 0}, {"n": 0}, {"n": 0}])
    eid = emit_recommendation_created_for_run(conn, uuid4(), uuid4(), "dq_watcher")
    assert eid is None
    # Only the three COUNT queries ran (recommendations,
    # parameter_recommendations, forecast_drift_recommendations), no INSERT.
    assert len(conn.calls) == 3
    assert all("COUNT(*)" in sql for sql, _ in conn.calls)


def test_reco_created_tolerates_tuple_rows() -> None:
    # A watcher's psycopg.connect() default is tuple_row — COUNT comes back as a
    # 1-tuple, not a dict. The helper must handle both.
    conn = _RecordingConn(fetch_results=[(4,), (0,)])
    eid = emit_recommendation_created_for_run(conn, uuid4(), uuid4(), "material_watcher")
    assert isinstance(eid, UUID)
    assert conn.calls[-1][1][6] == 4  # new_quantity


def test_fleet_types_are_a_subset_of_router_valid_types() -> None:
    """Anti-drift guard, DB-FREE — runs in the unit CI job.

    The integration variant of this check sits in
    test_fleet_events_integration.py behind the module-level requires_db
    marker (so it never runs in the unit job); a silent divergence between
    FLEET_EVENT_TYPES (emit helper) and VALID_EVENT_TYPES (POST /v1/events
    validation) would otherwise ship unnoticed. The SQL CHECK itself
    (migration 071) is locked by the integration test against real Postgres.
    """
    from ootils_core.api.routers.events import VALID_EVENT_TYPES
    from ootils_core.engine.events.emit import FLEET_EVENT_TYPES

    missing = set(FLEET_EVENT_TYPES) - set(VALID_EVENT_TYPES)
    assert not missing, (
        f"FLEET_EVENT_TYPES not accepted by the router validation: {missing}"
    )


def test_fleet_types_present_in_migration_check() -> None:
    """Static cross-check against the migration files themselves (DB-free):
    every fleet type must appear verbatim in ONE of the migrations that widens
    events.event_type's CHECK constraint for a fleet-emission type, so the
    Python emitter and the SQL CHECK cannot drift without a red unit test.

    Migration 071 (#401 AN-1) introduced the first 5 fleet types; migration
    076 (PURGE-1) added the 6th (purge_executed) on top. A type is expected
    to appear in EXACTLY ONE of these (the migration that introduced it),
    but this check only needs "present in at least one" — the CHECK
    constraint itself is always the FULL cumulative list (each widening
    migration reproduces every earlier type verbatim), so a type missing
    from every widening migration this test scans is the real drift signal.
    """
    from pathlib import Path

    from ootils_core.engine.events.emit import FLEET_EVENT_TYPES

    migrations_dir = Path(__file__).parent.parent / "src/ootils_core/db/migrations"
    sql = "\n".join(
        (migrations_dir / name).read_text(encoding="utf-8")
        for name in ("071_events_fleet_types.sql", "076_maintenance_purge.sql")
    )
    missing = [t for t in FLEET_EVENT_TYPES if f"'{t}'" not in sql]
    assert not missing, f"types absent from migrations 071/076 CHECK: {missing}"
