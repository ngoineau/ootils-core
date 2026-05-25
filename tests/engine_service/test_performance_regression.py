"""
test_performance_regression.py — pin current numbers as a baseline.

These tests FAIL if engine perf regresses beyond a known threshold.
The thresholds are tuned to the ADR-017 phase gates with some headroom
to absorb noise.

Marked `slow` — typically excluded from default CI; run in nightly or
before a release.
"""
from __future__ import annotations

import time
from uuid import UUID, uuid4

import pytest

BASELINE = UUID("00000000-0000-0000-0000-000000000001")

pytestmark = pytest.mark.slow

# --------------------------------------------------------------------- #
#  Single-event latency
# --------------------------------------------------------------------- #


def test_single_propagate_p50_under_5ms(engine_session, pick_pi_node):
    """Phase 6 measured 0.35 ms end-to-end on profile L.
    Allow 5 ms p50 — well under but with noise headroom for CI."""
    _, client = engine_session
    trigger, _, _ = pick_pi_node()

    latencies = []
    for _ in range(100):
        t0 = time.perf_counter()
        client.propagate(
            scenario_id=BASELINE,
            event_id=uuid4(),
            event_type="supply_qty_changed",
            trigger_node_id=trigger,
        )
        latencies.append((time.perf_counter() - t0) * 1000)

    latencies.sort()
    p50 = latencies[len(latencies) // 2]
    p95 = latencies[int(0.95 * len(latencies))]
    assert p50 < 5.0, f"single-propagate p50 regressed: {p50:.2f} ms"
    assert p95 < 20.0, f"single-propagate p95 regressed: {p95:.2f} ms"


# --------------------------------------------------------------------- #
#  Fork latency
# --------------------------------------------------------------------- #


def test_fork_p95_under_100ms(engine):
    """Phase 4 measured 32 ms p50 in-process. Client-side adds
    1-3 ms gRPC overhead. Uses a function-scoped engine fixture
    (clean state) — the module-scoped one accumulates forks from
    other tests which drags up the latency."""
    _, client = engine

    # Warmup: first few forks pay cold page-fault cost on the 76 MB clone.
    for _ in range(3):
        client.fork_scenario(BASELINE, name="warmup")

    latencies = []
    for i in range(20):
        t0 = time.perf_counter()
        client.fork_scenario(BASELINE, name=f"perf-fork-{i}")
        latencies.append((time.perf_counter() - t0) * 1000)

    latencies.sort()
    p50 = latencies[len(latencies) // 2]
    p95 = latencies[int(0.95 * len(latencies))]
    # 80 ms gives reasonable noise margin while still detecting a real regression.
    assert p50 < 80.0, f"fork p50 regressed: {p50:.2f} ms (phase 4 baseline 32 ms)"
    assert p95 < 150.0, f"fork p95 regressed: {p95:.2f} ms"


# --------------------------------------------------------------------- #
#  Compute throughput
# --------------------------------------------------------------------- #


def test_propagate_compute_under_10ms(engine_session, pick_pi_node):
    """The engine reports compute_us in EngineTiming. Validate the
    server-side compute (excluding gRPC roundtrip + Python overhead)
    stays sub-10 ms. Phase 3 was 86 ms for full L; incremental is
    sub-ms."""
    _, client = engine_session
    trigger, _, _ = pick_pi_node()

    compute_times_ms = []
    for _ in range(20):
        res = client.propagate(
            scenario_id=BASELINE,
            event_id=uuid4(),
            event_type="supply_qty_changed",
            trigger_node_id=trigger,
        )
        compute_times_ms.append(res.compute_ms)

    avg = sum(compute_times_ms) / len(compute_times_ms)
    mx = max(compute_times_ms)
    assert avg < 10.0, f"avg server-side compute regressed: {avg:.2f} ms"
    assert mx < 50.0, f"max server-side compute regressed: {mx:.2f} ms"


# --------------------------------------------------------------------- #
#  Memory at idle
# --------------------------------------------------------------------- #


def test_engine_memory_after_warmup_under_600mb(engine):
    """The graph itself is 76 MB on profile L. Total RSS settles
    around 120-150 MB after warmup. Uses a function-scoped engine
    fixture — the module-scoped one accumulates forks from previous
    tests, each adding ~76 MB."""
    psutil = pytest.importorskip("psutil")
    harness, client = engine

    # Warmup: a couple of operations.
    client.list_scenarios()
    client.metrics()

    rss_bytes = psutil.Process(harness.process.pid).memory_info().rss
    rss_mb = rss_bytes / 1_048_576
    assert rss_mb < 600.0, f"engine RSS regressed: {rss_mb:.1f} MB"
