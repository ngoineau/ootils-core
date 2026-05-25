"""
test_kill9_recovery.py — validate WAL crash recovery (ADR-017 phase 5
gate, retro-validated in phase 6).

Procedure repeated N times:
1. Start engine fresh (empty WAL, baseline loaded from Postgres).
2. Send one Propagate event.
3. Verify the result via GetNode.
4. SIGKILL the engine BEFORE the write-behind flusher has had time to
   flush to Postgres (sleep < flush_interval_ms).
5. Restart engine — boot must replay the WAL, re-apply deltas to RAM.
6. Verify GetNode on the trigger node returns the post-propagation
   state — same closing_stock as in step 3.
7. Assert: no data loss.

If all N iterations pass, the WAL recovery is validated.

Usage:
    DATABASE_URL=postgresql://ootils:ootils@host:5432/ootils_bench_l \\
    python scripts/test_kill9_recovery.py --iterations 10
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from ootils_core.engine_rust_service import EngineClient, EngineHarness  # noqa: E402

BASELINE = UUID("00000000-0000-0000-0000-000000000001")


def find_binary() -> Path:
    base = ROOT / "rust" / "target" / "release"
    for name in ("ootils-engine.exe", "ootils-engine"):
        p = base / name
        if p.exists():
            return p
    raise FileNotFoundError(f"missing engine binary in {base}")


def pick_trigger(dsn: str) -> UUID:
    """Pick a PI with non-trivial closing_stock so propagation will
    actually mutate values (forcing WAL writes)."""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT node_id FROM nodes "
            "WHERE node_type='ProjectedInventory' AND scenario_id=%s "
            "AND active=TRUE AND has_shortage = TRUE LIMIT 1",
            (BASELINE,),
        ).fetchone()
        if row:
            return UUID(str(row["node_id"]))
        # Fall back to any PI.
        row = conn.execute(
            "SELECT node_id FROM nodes "
            "WHERE node_type='ProjectedInventory' AND scenario_id=%s "
            "AND active=TRUE LIMIT 1",
            (BASELINE,),
        ).fetchone()
        return UUID(str(row["node_id"]))


def run_one_iteration(
    binary: Path,
    dsn: str,
    listen_addr: str,
    wal_path: Path,
    trigger: UUID,
    iteration: int,
) -> dict:
    """Run one kill-9 cycle. Returns metrics + boolean `recovery_ok`."""
    # 1. Fresh engine, fresh WAL.
    if wal_path.exists():
        wal_path.unlink()
    harness = EngineHarness(binary, dsn, listen_addr, wal_path=wal_path, flush_interval_ms=2000)
    # ^ flush_interval_ms=2000 so we have a comfortable window to
    #   kill -9 before the flusher runs.

    harness.start(wait_for_ready=True, ready_timeout_s=30.0)
    pre_kill_closing = None
    pre_kill_shortage = None
    try:
        with EngineClient.connect(listen_addr) as client:
            # 2. Propagate.
            res = client.propagate(
                scenario_id=BASELINE,
                event_id=uuid4(),
                event_type="supply_qty_changed",
                trigger_node_id=trigger,
            )
            # 3. Read back.
            node = client.get_node(BASELINE, trigger)
            pre_kill_closing = node.closing_stock
            pre_kill_shortage = node.has_shortage
            logger.info(
                "iter %d: pre-kill closing=%s has_shortage=%s wal_size=%d",
                iteration,
                pre_kill_closing,
                pre_kill_shortage,
                wal_path.stat().st_size if wal_path.exists() else 0,
            )

        # 4. Kill -9 IMMEDIATELY — flusher hasn't run yet (2s interval).
        time.sleep(0.1)  # give the WAL fsync a beat to finish (already done synchronously, this is a safety pause)
        wal_size_at_kill = wal_path.stat().st_size if wal_path.exists() else 0
        harness.kill9()
        logger.info("iter %d: SIGKILL'd engine, WAL has %d bytes", iteration, wal_size_at_kill)
    except Exception:
        harness.stop()
        raise

    # 5. Restart. The boot must replay WAL.
    harness2 = EngineHarness(binary, dsn, listen_addr, wal_path=wal_path, flush_interval_ms=2000)
    harness2.start(wait_for_ready=True, ready_timeout_s=30.0)

    recovery_ok = False
    post_kill_closing = None
    try:
        with EngineClient.connect(listen_addr) as client:
            health = client.health()
            logger.info("iter %d: post-restart health: %s", iteration, health.detail)

            # 6. Read back — should match what we had before SIGKILL.
            node = client.get_node(BASELINE, trigger)
            post_kill_closing = node.closing_stock
            recovery_ok = post_kill_closing == pre_kill_closing
            logger.info(
                "iter %d: post-restart closing=%s (expected %s) → recovery_ok=%s",
                iteration,
                post_kill_closing,
                pre_kill_closing,
                recovery_ok,
            )
    finally:
        harness2.stop()

    return {
        "iteration": iteration,
        "pre_kill_closing": str(pre_kill_closing),
        "post_kill_closing": str(post_kill_closing),
        "recovery_ok": recovery_ok,
        "wal_size_at_kill": wal_size_at_kill,
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--listen", default="127.0.0.1:50056")
    p.add_argument("--iterations", type=int, default=10)
    args = p.parse_args()
    if not args.dsn:
        logger.error("set DATABASE_URL or pass --dsn")
        return 1

    binary = find_binary()
    trigger = pick_trigger(args.dsn)
    wal = Path(os.environ.get("TEMP", "/tmp")) / f"kill9-recovery-{os.getpid()}.wal"
    logger.info("binary=%s trigger=%s wal=%s", binary, trigger, wal)

    results = []
    for i in range(1, args.iterations + 1):
        print(f"\n========== Iteration {i}/{args.iterations} ==========")
        try:
            r = run_one_iteration(binary, args.dsn, args.listen, wal, trigger, i)
            results.append(r)
        except Exception as e:
            logger.exception("iteration %d failed with exception", i)
            results.append({"iteration": i, "recovery_ok": False, "exception": str(e)})

    # Summary.
    print("\n\n========== KILL-9 RECOVERY SUMMARY ==========")
    n_ok = sum(1 for r in results if r.get("recovery_ok"))
    n_total = len(results)
    print(f"  iterations    : {n_total}")
    print(f"  recovery OK   : {n_ok}")
    print(f"  recovery FAIL : {n_total - n_ok}")
    print()
    for r in results:
        status = "OK  " if r.get("recovery_ok") else "FAIL"
        if "exception" in r:
            print(f"  {status} iter={r['iteration']}: EXCEPTION {r['exception']}")
        else:
            print(
                f"  {status} iter={r['iteration']}: "
                f"closing pre={r['pre_kill_closing']} post={r['post_kill_closing']} "
                f"wal_at_kill={r['wal_size_at_kill']}B"
            )
    print()
    if n_ok == n_total:
        print(f"PHASE 5 GATE PASSED — kill -9 recovery clean {n_ok}/{n_total} times")
        return 0
    print(f"PHASE 5 GATE FAILED — {n_total - n_ok} recovery failures")
    return 2


if __name__ == "__main__":
    sys.exit(main())
