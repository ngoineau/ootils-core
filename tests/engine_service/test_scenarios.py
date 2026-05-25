"""
test_scenarios.py — Fork / Merge / Isolation behavior of the COW scenario
manager (ADR-017 phase 4).
"""
from __future__ import annotations

import time
from uuid import UUID

import pytest

BASELINE = UUID("00000000-0000-0000-0000-000000000001")


def test_list_scenarios_includes_baseline(engine_session):
    _, client = engine_session
    sl = client.list_scenarios()
    assert len(sl.scenarios) >= 1
    names = [s.name for s in sl.scenarios]
    assert "baseline" in names


def test_fork_scenario_returns_info(engine_session):
    _, client = engine_session
    info = client.fork_scenario(BASELINE, name="test-fork-info")
    assert info.name == "test-fork-info"
    assert info.id  # non-empty UUID string
    assert info.overlay_size == 0
    # snapshot Arc<Graph> memory is ~76 MB on profile L
    assert info.memory_bytes > 10 * 1024 * 1024


def test_fork_is_fast(engine_session):
    """Fork latency gate from ADR-017 phase 4 was < 50 ms.
    Validate it from the client side too — gRPC roundtrip included."""
    _, client = engine_session
    t0 = time.perf_counter()
    info = client.fork_scenario(BASELINE, name="test-fork-perf")
    elapsed_ms = (time.perf_counter() - t0) * 1000
    # Add some buffer for first-call warm-up and the gRPC roundtrip
    # (the engine internal is ~32 ms p50, network adds 1-3 ms).
    assert elapsed_ms < 200, f"fork too slow from client side: {elapsed_ms:.1f} ms"


def test_multiple_forks_listed(engine_session):
    _, client = engine_session
    names_before = {s.name for s in client.list_scenarios().scenarios}
    new_names = []
    for i in range(5):
        info = client.fork_scenario(BASELINE, name=f"multi-fork-{i}")
        new_names.append(info.name)
    names_after = {s.name for s in client.list_scenarios().scenarios}
    # All 5 new forks should appear, baseline still there.
    assert "baseline" in names_after
    for n in new_names:
        assert n in names_after, f"missing fork {n} in list"


def test_fork_default_name_is_generated(engine_session):
    _, client = engine_session
    info = client.fork_scenario(BASELINE, name="")
    # Default name is "fork-XXXXXXXX" (8-char UUID prefix)
    assert info.name.startswith("fork-")
    assert len(info.name) > len("fork-")
