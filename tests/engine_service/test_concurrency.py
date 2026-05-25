"""
test_concurrency.py — multi-client + parallel-call behavior.

The engine uses Arc<RwLock<Graph>> + DashMap for scenarios — these
tests validate the lock contract holds under real concurrent traffic.

Marked `slow` because they spin multiple threads + measure latency
distributions.
"""
from __future__ import annotations

import statistics
import threading
import time
from uuid import UUID, uuid4

import pytest

BASELINE = UUID("00000000-0000-0000-0000-000000000001")

pytestmark = pytest.mark.slow


def test_parallel_reads_during_propagation(engine_session, pick_pi_node):
    """Multiple GetNode reads should not block, even while Propagate
    is running on the baseline. Validates the F-009 lock split:
    plan_compute holds a READ lock (concurrent readers OK), apply
    holds the WRITE lock only briefly.

    F-035 fix: baseline the reader throughput WITHOUT propagation
    first, then assert that with-propagation throughput stays at
    least 50% of baseline. Old assertion (read_count > 100) didn't
    test the failure mode (severe lock contention would still pass
    if reads were sub-ms)."""
    _, client = engine_session
    trigger, _, _ = pick_pi_node()

    def run_readers(stop_event, errors, n_threads=4) -> int:
        read_count = 0
        read_lock = threading.Lock()

        def reader():
            nonlocal read_count
            try:
                while not stop_event.is_set():
                    client.get_node(BASELINE, trigger)
                    with read_lock:
                        read_count += 1
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=reader, daemon=True) for _ in range(n_threads)]
        for t in threads:
            t.start()
        return threads, lambda: read_count

    # ---- Baseline pass: 4 readers, no propagation, for 1.5 s ----
    stop_base = threading.Event()
    base_errors: list[Exception] = []
    base_threads, base_count_fn = run_readers(stop_base, base_errors)
    time.sleep(1.5)
    stop_base.set()
    for t in base_threads:
        t.join(timeout=2.0)
    assert not base_errors, f"baseline readers errored: {base_errors[:3]}"
    baseline_throughput = base_count_fn() / 1.5
    assert baseline_throughput > 100, (
        f"baseline throughput suspiciously low: {baseline_throughput:.0f} reads/s — "
        "test environment issue"
    )

    # ---- Contention pass: 4 readers + hammered Propagate for 3 s ----
    stop_c = threading.Event()
    c_errors: list[Exception] = []
    c_threads, c_count_fn = run_readers(stop_c, c_errors)

    deadline = time.perf_counter() + 3.0
    propagations = 0
    while time.perf_counter() < deadline:
        client.propagate(
            scenario_id=BASELINE,
            event_id=uuid4(),
            event_type="supply_qty_changed",
            trigger_node_id=trigger,
        )
        propagations += 1

    stop_c.set()
    for t in c_threads:
        t.join(timeout=2.0)
    assert not c_errors, f"concurrent reads errored: {c_errors[:3]}"
    contention_throughput = c_count_fn() / 3.0

    # F-009 contract: contention throughput must stay at least 50%
    # of baseline. A naive "write-lock-during-compute" implementation
    # would drop this to near 0 because every propagation blocks all
    # reads for ms. With the F-009 read/write split, reads see only
    # the brief write lock during apply (~µs).
    ratio = contention_throughput / baseline_throughput
    assert ratio >= 0.50, (
        f"reads heavily blocked during propagation: baseline={baseline_throughput:.0f}/s, "
        f"contention={contention_throughput:.0f}/s, ratio={ratio:.2f} (need ≥ 0.50). "
        "F-009 lock split regression?"
    )
    assert propagations > 10, f"only {propagations} propagations completed"


def test_concurrent_fork_calls(engine_binary, dsn, grpc_module, tmp_path):
    """Multiple ForkScenario calls in parallel — DashMap is supposed
    to handle concurrent inserts. Validates no deadlock + all forks
    listed at the end."""
    from ootils_core.engine_rust_service import EngineClient, EngineHarness

    wal = tmp_path / "concurrent-fork.wal"
    from tests.engine_service.conftest import _free_port
    port = _free_port(start=50600)
    h = EngineHarness(engine_binary, dsn, f"127.0.0.1:{port}", wal_path=wal)
    h.start(wait_for_ready=True, ready_timeout_s=30.0)

    try:
        n_forks = 10
        results: list[str] = []
        errors: list[Exception] = []
        threads: list[threading.Thread] = []

        def fork_one(i: int):
            try:
                c = EngineClient.connect(f"127.0.0.1:{port}")
                info = c.fork_scenario(BASELINE, name=f"concurrent-{i}")
                results.append(info.name)
                c.close()
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        for i in range(n_forks):
            t = threading.Thread(target=fork_one, args=(i,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join(timeout=10.0)

        assert not errors, f"concurrent forks errored: {errors[:3]}"
        assert len(results) == n_forks, f"expected {n_forks} forks, got {len(results)}"

        # Verify all forks are listed.
        with EngineClient.connect(f"127.0.0.1:{port}") as c:
            sl = c.list_scenarios()
            names = {s.name for s in sl.scenarios}
            for i in range(n_forks):
                assert f"concurrent-{i}" in names, f"missing fork {i}"
    finally:
        h.stop()


def test_burst_propagations_no_failures(engine_session, pick_pi_node):
    """Tight burst of 500 sequential propagations from one client
    should produce zero failures + bounded latency."""
    _, client = engine_session
    trigger, _, _ = pick_pi_node()

    latencies: list[float] = []
    failures = 0
    for _ in range(500):
        t0 = time.perf_counter()
        try:
            client.propagate(
                scenario_id=BASELINE,
                event_id=uuid4(),
                event_type="supply_qty_changed",
                trigger_node_id=trigger,
            )
            latencies.append((time.perf_counter() - t0) * 1000)
        except Exception:
            failures += 1

    assert failures == 0, f"{failures} failures in 500-event burst"
    assert len(latencies) == 500

    p95 = sorted(latencies)[int(0.95 * len(latencies))]
    # P2.1.a: ArcSwap baseline → ~25-30ms clone-on-write per propag.
    # Once P2.1.b lands per-scenario propagation, user-facing
    # propagations bypass this cost (overlay write only). Baseline
    # propagation as exercised by this test is rare in production
    # (Q3: max hourly).
    assert p95 < 80.0, f"p95 too high: {p95:.2f} ms"
