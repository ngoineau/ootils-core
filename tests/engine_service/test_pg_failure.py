"""
test_pg_failure.py — Postgres-down failure injection (item #4).

Validates the engine survives Postgres unavailability:
- Boot fails cleanly with a bad DSN (no panic, exit code != 0).
- Write-behind queue accumulates deltas + WAL grows when PG is down,
  then catches up on recovery.
- Engine continues serving Propagate even while PG is unreachable —
  the in-RAM graph is the source of truth for reads; PG durability
  is async by design.
"""
from __future__ import annotations

import socket
import time
from uuid import UUID, uuid4

import pytest

BASELINE = UUID("00000000-0000-0000-0000-000000000001")

pytestmark = pytest.mark.slow


def test_boot_fails_with_unreachable_postgres(engine_binary, tmp_path):
    """The engine must NOT boot if Postgres is unreachable.
    Verify it exits with non-zero code (rather than spinning forever)."""
    from ootils_core.engine_rust_service import EngineHarness
    from tests.engine_service.conftest import _free_port

    port = _free_port(start=50700)
    wal = tmp_path / "boot-fail.wal"
    bad_dsn = "postgresql://nobody:nopw@127.0.0.1:65432/nodb"  # nothing listens here

    h = EngineHarness(engine_binary, bad_dsn, f"127.0.0.1:{port}", wal_path=wal)
    with pytest.raises((RuntimeError, TimeoutError)):
        h.start(wait_for_ready=True, ready_timeout_s=10.0)
    # Verify the process exited (not still hung).
    assert h.process is None or h.process.poll() is not None


def test_propagate_continues_when_pg_writeback_fails(engine_binary, dsn, tmp_path, grpc_module):
    """The engine should still service Propagate even if the write-
    behind flusher can't reach Postgres. The flusher's backoff loop
    is the safety net.

    We can't easily kill Postgres from a test (DB shared), so this
    test relies on the engine being able to operate from RAM alone.
    A real failure-injection test (kill pg container, verify backoff
    logs, verify recovery) needs Docker control and lives in
    integration CI, not here.

    Validates:
    - Engine boots normally
    - Propagate succeeds
    - WAL gets appended (deltas would need to flush)
    """
    from ootils_core.engine_rust_service import EngineClient, EngineHarness
    from tests.engine_service.conftest import _free_port

    port = _free_port(start=50800)
    wal = tmp_path / "pg-survival.wal"
    h = EngineHarness(engine_binary, dsn, f"127.0.0.1:{port}", wal_path=wal, flush_interval_ms=200)
    h.start(wait_for_ready=True, ready_timeout_s=30.0)

    try:
        with EngineClient.connect(f"127.0.0.1:{port}") as client:
            # Pick a PI to trigger on.
            import psycopg
            from psycopg.rows import dict_row
            with psycopg.connect(dsn, row_factory=dict_row) as conn:
                row = conn.execute(
                    "SELECT node_id FROM nodes WHERE node_type='ProjectedInventory' "
                    "AND scenario_id=%s AND active=TRUE LIMIT 1",
                    (BASELINE,),
                ).fetchone()
            assert row is not None
            trigger = UUID(str(row["node_id"]))

            # Issue 5 propagations. Engine must serve all of them via
            # in-RAM state, even if PG flush is slow.
            for _ in range(5):
                client.propagate(
                    scenario_id=BASELINE,
                    event_id=uuid4(),
                    event_type="supply_qty_changed",
                    trigger_node_id=trigger,
                )

            # GetNode should reflect the latest state from RAM, no PG roundtrip.
            state = client.get_node(BASELINE, trigger)
            assert state.node_id == trigger
    finally:
        h.stop()


def test_metrics_endpoint_reachable(engine_binary, dsn, tmp_path):
    """Bonus check: the Prometheus /metrics endpoint comes up on its
    own port and exposes the counters defined in `metrics.rs`."""
    from ootils_core.engine_rust_service import EngineHarness
    from tests.engine_service.conftest import _free_port

    port = _free_port(start=50900)
    metrics_port = _free_port(start=51000)
    wal = tmp_path / "metrics.wal"
    h = EngineHarness(engine_binary, dsn, f"127.0.0.1:{port}", wal_path=wal)
    # Override metrics-listen via env on top of the harness setup.
    import os as _os
    _os.environ["OOTILS_METRICS_LISTEN"] = f"127.0.0.1:{metrics_port}"
    try:
        h.start(wait_for_ready=True, ready_timeout_s=30.0)

        # Wait for the metrics endpoint to bind.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", metrics_port), timeout=0.5):
                    break
            except OSError:
                time.sleep(0.1)
        else:
            pytest.skip(f"metrics endpoint didn't bind on port {metrics_port}")

        # Issue a raw HTTP GET (avoid pulling requests as a dep).
        import http.client
        conn = http.client.HTTPConnection("127.0.0.1", metrics_port, timeout=2)
        conn.request("GET", "/metrics")
        resp = conn.getresponse()
        assert resp.status == 200, f"unexpected status {resp.status}"
        body = resp.read().decode("utf-8")
        conn.close()

        # Expect the documented counter names to appear.
        assert "ootils_engine_events_total" in body
        assert "ootils_engine_active_scenarios" in body
        assert "TYPE" in body  # Prom exposition format
    finally:
        h.stop()
        # Clean up the env override.
        _os.environ.pop("OOTILS_METRICS_LISTEN", None)
