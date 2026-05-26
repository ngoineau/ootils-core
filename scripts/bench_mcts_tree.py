"""
bench_mcts_tree.py — measure deep fork-from-scenario chains.

An MCTS agent explores by branching: root → branch_1 → branch_1.1 →
branch_1.1.1 → ... Each level inherits the parent's overlay.
This bench measures:
  - Latency per fork at increasing depth
  - Cumulative overlay size growth
  - Memory cost of a deep tree

Usage:
    python scripts/bench_mcts_tree.py --depth 20 --propagations-per-level 5
"""
from __future__ import annotations

import argparse
import logging
import os
import statistics
import sys
import time
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from ootils_core.engine_rust_service import EngineClient, EngineHarness  # noqa: E402
from ootils_core._grpc import engine_pb2  # noqa: E402

BASELINE = UUID("00000000-0000-0000-0000-000000000001")


def find_binary() -> Path:
    base = ROOT / "rust" / "target" / "release"
    for name in ("ootils-engine.exe", "ootils-engine"):
        p = base / name
        if p.exists():
            return p
    raise FileNotFoundError(f"missing engine binary in {base}")


def fetch_triggers(dsn: str, n: int = 200) -> list[UUID]:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT node_id FROM nodes WHERE node_type='ProjectedInventory' "
            "AND scenario_id=%s AND active=TRUE AND bucket_sequence=0 "
            "ORDER BY node_id LIMIT %s",
            (BASELINE, n),
        ).fetchall()
    return [UUID(str(r["node_id"])) for r in rows]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--listen", default="127.0.0.1:51080")
    p.add_argument("--depth", type=int, default=20)
    p.add_argument("--propagations-per-level", type=int, default=5)
    args = p.parse_args()
    if not args.dsn:
        return 1

    binary = find_binary()
    wal = Path(os.environ.get("TEMP", "/tmp")) / f"bench-mcts-{os.getpid()}.wal"
    if wal.exists():
        wal.unlink()
    triggers = fetch_triggers(args.dsn)

    harness = EngineHarness(
        binary_path=binary,
        dsn=args.dsn,
        listen_addr=args.listen,
        wal_path=wal,
        flush_interval_ms=100,
        extra_env={"OOTILS_SCENARIO_TTL_SEC": "0"},
    )
    harness.start(wait_for_ready=True, ready_timeout_s=30.0)

    fork_times_us = []
    propag_times_ms = []
    overlay_sizes = []

    try:
        import psutil
        engine_pid = harness.process.pid
        rss_start = psutil.Process(engine_pid).memory_info().rss / 1_048_576

        with EngineClient.connect(args.listen) as client:
            # Root fork from baseline.
            t0 = time.perf_counter_ns()
            root = client._stub.ForkScenario(
                engine_pb2.ForkRequest(name="mcts-root"),
                timeout=5.0,
            )
            fork_times_us.append((time.perf_counter_ns() - t0) / 1000.0)
            current_sid = UUID(root.id)

            for depth in range(args.depth):
                # Propagate N events on current node
                events = [
                    engine_pb2.BatchEvent(
                        event_id=str(uuid4()),
                        event_type="supply_qty_changed",
                        trigger_node_id=str(triggers[(depth * 3 + i) % len(triggers)]),
                        payload=b"",
                    )
                    for i in range(args.propagations_per_level)
                ]
                t = time.perf_counter()
                client._stub.PropagateBatch(
                    engine_pb2.PropagateBatchRequest(
                        scenario_id=str(current_sid),
                        events=events,
                    ),
                    timeout=10.0,
                )
                propag_times_ms.append((time.perf_counter() - t) * 1000.0)

                # Get scenario info to measure overlay size
                sl = client.list_scenarios()
                this_scenario = next(
                    (s for s in sl.scenarios if UUID(s.id) == current_sid),
                    None,
                )
                if this_scenario:
                    overlay_sizes.append(this_scenario.overlay_size)

                # Fork the next level
                t = time.perf_counter_ns()
                child = client._stub.ForkScenario(
                    engine_pb2.ForkRequest(
                        parent_scenario_id=str(current_sid),
                        name=f"mcts-d{depth+1}",
                    ),
                    timeout=5.0,
                )
                fork_times_us.append((time.perf_counter_ns() - t) / 1000.0)
                current_sid = UUID(child.id)

        rss_end = psutil.Process(engine_pid).memory_info().rss / 1_048_576

        print("\n" + "=" * 70)
        print(f"MCTS TREE BENCH — depth {args.depth}, {args.propagations_per_level} propags/level")
        print("=" * 70)
        print(f"\n[FORK latencies]  (ArcSwap snapshot + DashMap overlay clone)")
        for i, ft in enumerate(fork_times_us):
            if i in (0, 1, 5, 10, args.depth // 2, args.depth - 1, args.depth):
                marker = "root" if i == 0 else f"depth {i}"
                print(f"  {marker:>12}: {ft:>9.1f} µs")
        print(f"\n  p50 across depths: {statistics.median(fork_times_us):.1f} µs")
        print(f"  max               : {max(fork_times_us):.1f} µs")

        print(f"\n[PROPAGATE BATCH] {args.propagations_per_level} events / level")
        print(f"  p50: {statistics.median(propag_times_ms):.2f} ms")
        print(f"  max: {max(propag_times_ms):.2f} ms")

        if overlay_sizes:
            print(f"\n[OVERLAY GROWTH] entries per scenario as the tree deepens")
            for i in (0, args.depth // 4, args.depth // 2, 3 * args.depth // 4, args.depth - 1):
                if i < len(overlay_sizes):
                    print(f"  depth {i:>3}: {overlay_sizes[i]:>5} overlay entries")

        print(f"\n[MEMORY]")
        print(f"  RSS start: {rss_start:.0f} MB")
        print(f"  RSS end  : {rss_end:.0f} MB")
        print(f"  Drift    : {rss_end - rss_start:+.0f} MB for {args.depth} levels deep")
        print(f"  Per level: {(rss_end - rss_start) / max(args.depth, 1):+.2f} MB/level")
        print("=" * 70)

    finally:
        harness.stop()
        try:
            wal.unlink()
        except OSError:
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
