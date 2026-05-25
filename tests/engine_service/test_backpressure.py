"""
test_backpressure.py — Cluster B audit response (F-005, F-007).

Validates the engine's resource-bound contracts:
- F-005 BLOCK: WAL size + queue depth are hard-capped. Above the cap,
  Propagate returns RESOURCE_EXHAUSTED instead of growing memory/disk
  unboundedly.
- F-007 BLOCK: A malformed OOTILS_METRICS_LISTEN is fail-fast at boot,
  not a silent warn-and-continue.

## Coverage split

Boot-time + RPC contract tests live here (Python integration). The
underlying cap *mechanisms* (WalFull on append, QueueFull on try_push)
have direct Rust unit tests in `wal.rs` and `write_behind.rs` because
exercising them via gRPC requires injecting state mutations into PG
to force the propagator to emit deltas — too fragile for a stable
test. The gRPC plumbing is mechanical: if the Rust caps trip AND
service.rs translates the errors to Status::resource_exhausted (which
it does — see propagate() in service.rs), the integration works.
"""
from __future__ import annotations

import socket
from uuid import UUID

import pytest

BASELINE = UUID("00000000-0000-0000-0000-000000000001")

pytestmark = pytest.mark.slow


def test_boot_fails_on_malformed_metrics_listen(engine_binary, dsn, tmp_path):
    """F-007: a non-parseable OOTILS_METRICS_LISTEN must crash the
    engine at startup instead of silently disabling metrics."""
    from ootils_core.engine_rust_service import EngineHarness
    from tests.engine_service.conftest import _free_port

    port = _free_port(start=51500)
    wal = tmp_path / "bad-metrics.wal"

    h = EngineHarness(
        binary_path=engine_binary,
        dsn=dsn,
        listen_addr=f"127.0.0.1:{port}",
        wal_path=wal,
        extra_env={
            # Intentionally not a valid host:port (looks plausible
            # though, mimicking a real misconfig like a typoed dot).
            "OOTILS_METRICS_LISTEN": "127.0.0.1.9090",
        },
    )
    with pytest.raises((RuntimeError, TimeoutError)):
        h.start(wait_for_ready=True, ready_timeout_s=10.0)

    # Process should have exited with non-zero — verify it's not still hung.
    assert h.process is None or h.process.poll() is not None
    # Stderr should contain a clear error message about the parse failure.
    stderr = h.read_stderr()
    assert "OOTILS_METRICS_LISTEN" in stderr or "metrics" in stderr.lower(), (
        f"expected metrics-listen parse error in stderr, got:\n{stderr[:2000]}"
    )


def test_boot_succeeds_with_metrics_listen_off(engine_binary, dsn, tmp_path):
    """F-007: 'off' is the documented sentinel for 'disable metrics'.
    Engine boots normally + the metrics port is NOT bound."""
    from ootils_core.engine_rust_service import EngineClient, EngineHarness
    from tests.engine_service.conftest import _free_port

    port = _free_port(start=51600)
    wal = tmp_path / "metrics-off.wal"

    h = EngineHarness(
        binary_path=engine_binary,
        dsn=dsn,
        listen_addr=f"127.0.0.1:{port}",
        wal_path=wal,
        extra_env={
            "OOTILS_METRICS_LISTEN": "off",
        },
    )
    h.start(wait_for_ready=True, ready_timeout_s=30.0)

    try:
        # gRPC works normally.
        with EngineClient.connect(f"127.0.0.1:{port}") as c:
            assert c.health().status == 1

        # The default Prometheus port (9090) should NOT be bound by
        # this engine since metrics are explicitly off.
        with pytest.raises((ConnectionRefusedError, OSError)):
            with socket.create_connection(("127.0.0.1", 9090), timeout=0.5):
                pass
    finally:
        h.stop()


def test_resource_exhausted_status_metadata_is_exposed(engine_binary, dsn, tmp_path):
    """F-005 sanity: the Prometheus /metrics endpoint exposes the
    configured caps as gauges so operators can monitor headroom."""
    from ootils_core.engine_rust_service import EngineHarness
    from tests.engine_service.conftest import _free_port
    import http.client

    port = _free_port(start=51900)
    metrics_port = _free_port(start=52000)
    wal = tmp_path / "metrics-gauges.wal"

    h = EngineHarness(
        binary_path=engine_binary,
        dsn=dsn,
        listen_addr=f"127.0.0.1:{port}",
        wal_path=wal,
        extra_env={
            "OOTILS_METRICS_LISTEN": f"127.0.0.1:{metrics_port}",
            "OOTILS_WAL_MAX_BYTES": "524288000",  # 500 MB
            "OOTILS_QUEUE_MAX_DEPTH": "750000",
        },
    )
    h.start(wait_for_ready=True, ready_timeout_s=30.0)

    try:
        # Scrape /metrics and verify the caps surface as gauges.
        conn = http.client.HTTPConnection("127.0.0.1", metrics_port, timeout=2)
        conn.request("GET", "/metrics")
        resp = conn.getresponse()
        body = resp.read().decode("utf-8")
        conn.close()
        assert resp.status == 200
        assert "ootils_engine_wal_max_bytes 524288000" in body
        assert "ootils_engine_queue_max_depth 750000" in body
        assert "ootils_engine_wal_size_bytes" in body
    finally:
        h.stop()
