"""
chaos_test_baseline.py — hammer the BASELINE path (CoW + WAL + PG).

Phase 2.1.a's clone-on-write design makes baseline propagation more
expensive (~30 ms per call) than the old in-place mutation (~5 ms).
The trade-off is that scenarios fork in O(1). This script measures
exactly where the baseline path saturates so we can document the
real envelope for operators.

Procedure:
- N workers, all hammering BASELINE (no scenarios).
- Engine handles the clone-on-write serially via propagation_lock.
- Measures actual sustained qps, latency, RSS, queue depth.
- Triggers RESOURCE_EXHAUSTED if WAL/queue caps are reached.
"""
from __future__ import annotations

import argparse
import logging
import os
import statistics
import sys
import threading
import time
from dataclasses import dataclass, field
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


@dataclass
class Result:
    latencies_ms: list[float] = field(default_factory=list)
    failures: int = 0
    failures_exhausted: int = 0
    rss_samples: list[tuple[float, float]] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)


def find_binary() -> Path:
    base = ROOT / "rust" / "target" / "release"
    for name in ("ootils-engine.exe", "ootils-engine"):
        p = base / name
        if p.exists():
            return p
    raise FileNotFoundError(f"missing engine binary in {base}")


def fetch_triggers(dsn: str, n: int = 100) -> list[UUID]:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT node_id FROM nodes WHERE node_type='ProjectedInventory' "
            "AND scenario_id=%s AND active=TRUE AND bucket_sequence=0 "
            "ORDER BY node_id LIMIT %s",
            (BASELINE, n),
        ).fetchall()
    return [UUID(str(r["node_id"])) for r in rows]


def worker(client_addr: str, triggers: list[UUID], stop: threading.Event, result: Result, idx: int):
    try:
        with EngineClient.connect(client_addr) as client:
            import grpc
            n = 0
            while not stop.is_set():
                trg = triggers[(idx * 7 + n) % len(triggers)]
                t0 = time.perf_counter()
                try:
                    client.propagate(
                        scenario_id=BASELINE,
                        event_id=uuid4(),
                        event_type="supply_qty_changed",
                        trigger_node_id=trg,
                        timeout=10.0,
                    )
                    elapsed = (time.perf_counter() - t0) * 1000.0
                    with result.lock:
                        result.latencies_ms.append(elapsed)
                        if len(result.latencies_ms) > 100_000:
                            result.latencies_ms = result.latencies_ms[-50_000:]
                except grpc.RpcError as exc:
                    with result.lock:
                        if exc.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                            result.failures_exhausted += 1
                        else:
                            result.failures += 1
                n += 1
    except Exception as e:  # noqa: BLE001
        logger.warning("worker %d crashed: %s", idx, e)


def memory_sampler(harness, result, stop):
    try:
        import psutil
    except ImportError:
        return
    start = time.perf_counter()
    while not stop.is_set():
        if harness.process is None or harness.process.poll() is not None:
            break
        try:
            rss = psutil.Process(harness.process.pid).memory_info().rss / 1_048_576
            with result.lock:
                result.rss_samples.append((time.perf_counter() - start, rss))
        except Exception:
            pass
        stop.wait(timeout=1.0)


def percentiles(samples):
    if not samples:
        return {"n": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0, "mean": 0.0}
    s = sorted(samples)
    n = len(s)
    return {
        "n": n,
        "p50": s[n // 2],
        "p95": s[int(0.95 * n)],
        "p99": s[int(0.99 * n)],
        "max": s[-1],
        "mean": statistics.mean(s),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--listen", default="127.0.0.1:51065")
    p.add_argument("--workers", type=int, default=10)
    p.add_argument("--duration-s", type=int, default=30)
    p.add_argument("--wal-max-mb", type=int, default=512)
    p.add_argument("--queue-max", type=int, default=500_000)
    args = p.parse_args()
    if not args.dsn:
        return 1

    binary = find_binary()
    wal = Path(os.environ.get("TEMP", "/tmp")) / f"chaos-base-{os.getpid()}.wal"
    if wal.exists():
        wal.unlink()

    triggers = fetch_triggers(args.dsn, n=100)
    logger.info("loaded %d trigger PIs", len(triggers))

    harness = EngineHarness(
        binary_path=binary,
        dsn=args.dsn,
        listen_addr=args.listen,
        wal_path=wal,
        flush_interval_ms=100,
        extra_env={
            "OOTILS_WAL_MAX_BYTES": str(args.wal_max_mb * 1024 * 1024),
            "OOTILS_QUEUE_MAX_DEPTH": str(args.queue_max),
        },
    )
    harness.start(wait_for_ready=True, ready_timeout_s=30.0)

    result = Result()
    stop = threading.Event()
    threads = []
    try:
        for i in range(args.workers):
            t = threading.Thread(target=worker, args=(args.listen, triggers, stop, result, i), daemon=True)
            threads.append(t); t.start()
        sampler = threading.Thread(target=memory_sampler, args=(harness, result, stop), daemon=True)
        threads.append(sampler); sampler.start()
        time.sleep(args.duration_s)
        stop.set()
        for t in threads:
            t.join(timeout=5.0)
    finally:
        harness.stop()
        try:
            wal.unlink()
        except OSError:
            pass

    pct = percentiles(result.latencies_ms)
    qps = pct["n"] / args.duration_s
    total_attempted = pct["n"] + result.failures + result.failures_exhausted
    print("\n" + "=" * 70)
    print(f"BASELINE CHAOS — {args.workers} workers / {args.duration_s}s")
    print(f"  WAL cap: {args.wal_max_mb} MB, Queue cap: {args.queue_max}")
    print("=" * 70)
    print(f"  Attempts: {total_attempted}  OK: {pct['n']}  Failed: {result.failures}  Exhausted: {result.failures_exhausted}")
    print(f"  QPS achieved: {qps:.0f}")
    print(f"  Latency ms: p50={pct['p50']:.2f}  p95={pct['p95']:.2f}  p99={pct['p99']:.2f}  max={pct['max']:.2f}")
    if result.rss_samples:
        rss_start = result.rss_samples[0][1]
        rss_end = result.rss_samples[-1][1]
        rss_max = max(r for _, r in result.rss_samples)
        print(f"  RSS: start={rss_start:.0f} MB  end={rss_end:.0f} MB  max={rss_max:.0f} MB  drift={rss_end - rss_start:+.0f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
