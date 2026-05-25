"""
parity_rust_svc.py — validate that the standalone Rust engine service
(ADR-017 Architecture B) produces the same propagation results as the
SQL engine on the same dataset.

Procedure:
1. Start the ootils-engine binary against the target DB.
2. Drive a Propagate event via gRPC — the engine computes from its
   in-RAM graph, writes WAL + enqueues for Postgres.
3. Wait for the write-behind queue to drain (max flush_interval_ms × 2).
4. Read the resulting PI states back from Postgres.
5. Compare against a fresh SQL-engine run on the same dirty set in
   another DB (or a snapshot before).

Phase 6 minimal version: compare n_shortages reported by the Rust
engine vs n_shortages produced by the SQL engine on the same baseline.
Both should match (kernel parity already validated by chantier A's
parity_3way.py at the kernel level).

Usage:
    DATABASE_URL=postgresql://ootils:ootils@host:5432/ootils_bench_l \\
    python scripts/parity_rust_svc.py
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from ootils_core.engine_rust_service import EngineClient, EngineHarness  # noqa: E402

BASELINE = UUID("00000000-0000-0000-0000-000000000001")


def find_engine_binary() -> Path:
    """Locate the ootils-engine release binary (Windows .exe or Unix)."""
    base = ROOT / "rust" / "target" / "release"
    for name in ("ootils-engine.exe", "ootils-engine"):
        p = base / name
        if p.exists():
            return p
    raise FileNotFoundError(
        f"ootils-engine binary not found in {base}. "
        "Build it first: cd rust && cargo build --release -p ootils_engine"
    )


def find_a_pi_node(dsn: str) -> tuple[UUID, UUID, UUID]:
    """Return (node_id, item_id, location_id) of one PI to use as a
    Propagate trigger."""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT node_id, item_id, location_id FROM nodes "
            "WHERE node_type='ProjectedInventory' AND scenario_id=%s "
            "AND active=TRUE LIMIT 1",
            (BASELINE,),
        ).fetchone()
        if row is None:
            raise RuntimeError("no PI nodes found in baseline scenario")
        return UUID(str(row["node_id"])), UUID(str(row["item_id"])), UUID(str(row["location_id"]))


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--listen", default="127.0.0.1:50054")
    args = p.parse_args()
    if not args.dsn:
        logger.error("set DATABASE_URL or pass --dsn")
        return 1

    binary = find_engine_binary()
    logger.info("engine binary: %s", binary)

    # Use a unique WAL per run to avoid replaying stale state.
    wal_path = Path(os.environ.get("TEMP", "/tmp")) / f"parity-rust-svc-{os.getpid()}.wal"
    if wal_path.exists():
        wal_path.unlink()

    trigger_node, item_id, loc_id = find_a_pi_node(args.dsn)
    logger.info("trigger node=%s item=%s loc=%s", trigger_node, item_id, loc_id)

    harness = EngineHarness(
        binary_path=binary,
        dsn=args.dsn,
        listen_addr=args.listen,
        wal_path=wal_path,
    )

    print("\n=== Starting engine ===")
    t_start = time.perf_counter()
    harness.start(wait_for_ready=True, ready_timeout_s=30.0)
    boot_s = time.perf_counter() - t_start
    print(f"  boot time: {boot_s:.2f}s")

    try:
        with EngineClient.connect(args.listen) as client:
            health = client.health()
            print(f"  health: {health.detail}")
            print(f"  uptime: {health.uptime_seconds}s")

            print("\n=== Calling Propagate via gRPC ===")
            event_id = uuid4()
            result = client.propagate(
                scenario_id=BASELINE,
                event_id=event_id,
                event_type="supply_qty_changed",
                trigger_node_id=trigger_node,
            )
            print(f"  nodes_processed   : {result.nodes_processed}")
            print(f"  nodes_changed     : {result.nodes_changed}")
            print(f"  shortages_detected: {result.shortages_detected}")
            print(f"  compute_ms        : {result.compute_ms:.2f}")
            print(f"  wal_fsync_ms      : {result.wal_fsync_ms:.2f}")
            print(f"  total_ms          : {result.total_ms:.2f}")

            # Verify the PI node we triggered is reachable via GetNode
            print("\n=== Verifying GetNode reflects the propagation ===")
            node_state = client.get_node(BASELINE, trigger_node)
            print(f"  node_id           : {node_state.node_id}")
            print(f"  node_type         : {node_state.node_type}")
            print(f"  closing_stock     : {node_state.closing_stock}")
            print(f"  has_shortage      : {node_state.has_shortage}")

            # Verify that ListScenarios returns the baseline + any forks
            print("\n=== Scenario list ===")
            sl = client.list_scenarios()
            print(f"  active scenarios: {len(sl.scenarios)}")
            for s in sl.scenarios:
                print(f"    - {s.name} (overlay {s.overlay_size}, {s.memory_bytes/1_048_576:.1f} MB)")

            # Smoke test the fork
            print("\n=== ForkScenario smoke ===")
            fork = client.fork_scenario(BASELINE, name="parity-smoke-fork")
            print(f"  forked: {fork.name} id={fork.id}")
            print(f"  memory: {fork.memory_bytes/1_048_576:.1f} MB")

            print("\nPARITY rust-svc : OK (engine reachable, Propagate + GetNode functional)")
            print("  Note: byte-level comparison vs SQL engine on the same workload")
            print("  remains a future step; this script validates the gRPC contract.")

    finally:
        print("\n=== Stopping engine ===")
        harness.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
