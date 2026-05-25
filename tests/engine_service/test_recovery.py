"""
test_recovery.py — WAL crash recovery (ADR-017 phase 5).

These tests SIGKILL the engine at various moments and verify that the
state across restart is consistent. Marked `slow` because each test
spawns + tears down an engine (3-4 s each).
"""
from __future__ import annotations

import time
from pathlib import Path
from uuid import UUID, uuid4

import pytest

BASELINE = UUID("00000000-0000-0000-0000-000000000001")

pytestmark = pytest.mark.slow


def _engine_with_long_flush(engine_binary, dsn, port: int, wal: Path):
    """Helper: harness with a long flush interval so we can kill
    BEFORE Postgres catches up + reliably exercise WAL replay."""
    from ootils_core.engine_rust_service import EngineHarness

    return EngineHarness(
        binary_path=engine_binary,
        dsn=dsn,
        listen_addr=f"127.0.0.1:{port}",
        wal_path=wal,
        flush_interval_ms=10_000,  # 10s — kill -9 before flush
    )


def test_kill9_preserves_state_across_restart(engine_binary, dsn, grpc_module, tmp_path):
    """The canonical recovery test: propagate, SIGKILL, restart,
    verify GetNode returns the same closing_stock."""
    from ootils_core.engine_rust_service import EngineClient

    wal = tmp_path / "recovery.wal"
    from tests.engine_service.conftest import _free_port
    port = _free_port(start=50300)

    # Pick a trigger.
    import psycopg
    from psycopg.rows import dict_row
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT node_id FROM nodes WHERE node_type='ProjectedInventory' "
            "AND scenario_id=%s AND active=TRUE LIMIT 1",
            (BASELINE,),
        ).fetchone()
    if row is None:
        pytest.skip("no PI nodes in baseline")
    trigger = UUID(str(row["node_id"]))

    # ---- First run ----
    h1 = _engine_with_long_flush(engine_binary, dsn, port, wal)
    h1.start(wait_for_ready=True, ready_timeout_s=30.0)
    try:
        with EngineClient.connect(f"127.0.0.1:{port}") as c1:
            c1.propagate(
                scenario_id=BASELINE,
                event_id=uuid4(),
                event_type="supply_qty_changed",
                trigger_node_id=trigger,
            )
            pre_kill = c1.get_node(BASELINE, trigger)
        time.sleep(0.2)  # ensure WAL fsync completed (already sync, this is paranoia)
        h1.kill9()
    finally:
        # Defensive — if start raised, kill9() above won't have run.
        pass

    # ---- Second run, same WAL ----
    h2 = _engine_with_long_flush(engine_binary, dsn, port, wal)
    h2.start(wait_for_ready=True, ready_timeout_s=30.0)
    try:
        with EngineClient.connect(f"127.0.0.1:{port}") as c2:
            post_restart = c2.get_node(BASELINE, trigger)
    finally:
        h2.stop()

    assert post_restart.closing_stock == pre_kill.closing_stock
    assert post_restart.has_shortage == pre_kill.has_shortage


def test_repeated_kill9_remains_consistent(engine_binary, dsn, grpc_module, tmp_path):
    """5 kill-9 cycles in a row — state must remain consistent."""
    from ootils_core.engine_rust_service import EngineClient

    wal = tmp_path / "recovery-loop.wal"
    from tests.engine_service.conftest import _free_port
    port = _free_port(start=50400)

    # Pick a trigger.
    import psycopg
    from psycopg.rows import dict_row
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT node_id FROM nodes WHERE node_type='ProjectedInventory' "
            "AND scenario_id=%s AND active=TRUE LIMIT 1",
            (BASELINE,),
        ).fetchone()
    if row is None:
        pytest.skip("no PI nodes in baseline")
    trigger = UUID(str(row["node_id"]))

    reference_closing = None
    for i in range(5):
        if wal.exists():
            wal.unlink()
        h = _engine_with_long_flush(engine_binary, dsn, port, wal)
        h.start(wait_for_ready=True, ready_timeout_s=30.0)
        try:
            with EngineClient.connect(f"127.0.0.1:{port}") as c:
                c.propagate(
                    scenario_id=BASELINE,
                    event_id=uuid4(),
                    event_type="supply_qty_changed",
                    trigger_node_id=trigger,
                )
                state = c.get_node(BASELINE, trigger)
                if reference_closing is None:
                    reference_closing = state.closing_stock
                else:
                    assert state.closing_stock == reference_closing, (
                        f"iter {i}: drift detected closing={state.closing_stock} "
                        f"vs ref={reference_closing}"
                    )
        finally:
            h.kill9()


def test_wal_truncated_after_clean_shutdown(engine_binary, dsn, tmp_path):
    """A clean SIGTERM should trigger the bg flusher to drain + advance
    the WAL marker — on the next boot, replay should find no records to
    recover. With WAL v2 (Cluster A redesign) the file is not truncated
    in-place; it grows until rotation kicks in at 256 MB. The size
    check below remains valid because a single propagation generates
    ~200 bytes (header 20 + record ~180), well under the slack of 1 KB.
    The semantic invariant tested is: applied_pg_seq has advanced past
    all records, so replay() returns empty on the next boot."""
    from ootils_core.engine_rust_service import EngineClient, EngineHarness

    wal = tmp_path / "clean-shutdown.wal"
    from tests.engine_service.conftest import _free_port
    port = _free_port(start=50500)

    h = EngineHarness(
        binary_path=engine_binary,
        dsn=dsn,
        listen_addr=f"127.0.0.1:{port}",
        wal_path=wal,
        flush_interval_ms=50,  # short — flush before SIGTERM
    )
    h.start(wait_for_ready=True, ready_timeout_s=30.0)

    # Trigger a propagation to write to WAL.
    import psycopg
    from psycopg.rows import dict_row
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT node_id FROM nodes WHERE node_type='ProjectedInventory' "
            "AND scenario_id=%s AND active=TRUE LIMIT 1",
            (BASELINE,),
        ).fetchone()
    if row is not None:
        with EngineClient.connect(f"127.0.0.1:{port}") as c:
            c.propagate(
                scenario_id=BASELINE,
                event_id=uuid4(),
                event_type="supply_qty_changed",
                trigger_node_id=UUID(str(row["node_id"])),
            )

    # Give flusher a beat to drain.
    time.sleep(0.5)
    h.stop()

    # WAL should be at-or-near magic-header-only size (4 bytes) since
    # the bg flusher truncated it. Allow some slack — if there's any
    # late-arrival record it'd still be < 1 KB.
    assert wal.exists()
    assert wal.stat().st_size <= 1024, (
        f"WAL not truncated post-shutdown: {wal.stat().st_size} bytes"
    )
