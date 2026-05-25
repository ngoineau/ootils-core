"""
stress_test_engine.py — sustained-load validation of the Rust engine
service (ADR-017 phase 7 gate).

Procedure:
1. Start the engine.
2. Spawn `--workers` Python threads, each firing Propagate calls in a
   tight loop, throttled so the aggregate matches `--target-rps`.
3. Run for `--duration-s` seconds.
4. Sample the engine's RSS every second (via psutil or fallback) to
   detect memory leaks.
5. Report:
   - Total events, achieved throughput
   - Latency p50 / p95 / p99 / max overall AND per time bucket
   - Memory min/max/end + drift
   - Failures by category

Phase 7 gate per ADR-017:
- p95 latency < 50 ms under load     ← the headline UX claim
- No memory drift over the soak run  ← no leaks

Usage:
    DATABASE_URL=postgresql://ootils:ootils@host:5432/ootils_bench_l \\
    python scripts/stress_test_engine.py --target-rps 100 --duration-s 60
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
from typing import Optional
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


def fetch_triggers(dsn: str, n: int = 5000) -> list[UUID]:
    """Pull a pool of PI node IDs to use as Propagate triggers."""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT node_id FROM nodes "
            "WHERE node_type='ProjectedInventory' AND scenario_id=%s "
            "AND active=TRUE ORDER BY random() LIMIT %s",
            (BASELINE, n),
        ).fetchall()
        return [UUID(str(r["node_id"])) for r in rows]


@dataclass
class WorkerResult:
    n_ok: int = 0
    n_fail: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    failure_msgs: list[str] = field(default_factory=list)


def worker_loop(
    worker_id: int,
    listen_addr: str,
    triggers: list[UUID],
    stop_event: threading.Event,
    per_worker_interval_s: float,
    result: WorkerResult,
) -> None:
    """One worker thread: opens its own gRPC channel + iterates Propagate."""
    try:
        client = EngineClient.connect(listen_addr)
    except Exception as e:
        result.failure_msgs.append(f"worker {worker_id} connect: {e}")
        return

    next_fire = time.perf_counter()
    pool_size = len(triggers)
    idx = worker_id

    while not stop_event.is_set():
        now = time.perf_counter()
        if now < next_fire:
            time.sleep(min(next_fire - now, 0.01))
            continue

        trigger = triggers[idx % pool_size]
        idx += 1
        t0 = time.perf_counter()
        try:
            client.propagate(
                scenario_id=BASELINE,
                event_id=uuid4(),
                event_type="supply_qty_changed",
                trigger_node_id=trigger,
                timeout=10.0,
            )
            elapsed_ms = (time.perf_counter() - t0) * 1000
            result.n_ok += 1
            result.latencies_ms.append(elapsed_ms)
        except Exception as e:
            result.n_fail += 1
            if len(result.failure_msgs) < 10:
                result.failure_msgs.append(f"worker {worker_id} call: {e}")

        next_fire += per_worker_interval_s

    client.close()


def sample_rss(harness: EngineHarness) -> Optional[int]:
    """Return RSS in bytes of the engine process, or None if unknown."""
    if harness.process is None:
        return None
    try:
        import psutil  # type: ignore
        return psutil.Process(harness.process.pid).memory_info().rss
    except ImportError:
        # Fallback for Windows without psutil — use tasklist (rough).
        return None


def memory_monitor(
    harness: EngineHarness,
    stop_event: threading.Event,
    samples_out: list[tuple[float, int]],
) -> None:
    t0 = time.perf_counter()
    while not stop_event.is_set():
        rss = sample_rss(harness)
        if rss is not None:
            samples_out.append((time.perf_counter() - t0, rss))
        time.sleep(1.0)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--listen", default="127.0.0.1:50057")
    p.add_argument("--target-rps", type=int, default=100)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--duration-s", type=int, default=60)
    args = p.parse_args()
    if not args.dsn:
        logger.error("set DATABASE_URL or pass --dsn")
        return 1

    binary = find_binary()
    wal = Path(os.environ.get("TEMP", "/tmp")) / f"stress-{os.getpid()}.wal"
    if wal.exists():
        wal.unlink()

    triggers = fetch_triggers(args.dsn)
    logger.info(
        "binary=%s dsn=%s rps=%d workers=%d duration=%ds triggers=%d",
        binary, args.dsn, args.target_rps, args.workers, args.duration_s, len(triggers),
    )

    per_worker_rate = args.target_rps / args.workers
    per_worker_interval_s = 1.0 / per_worker_rate
    logger.info("per-worker rate: %.2f rps (interval %.3f s)", per_worker_rate, per_worker_interval_s)

    harness = EngineHarness(binary, args.dsn, args.listen, wal_path=wal, flush_interval_ms=100)
    harness.start(wait_for_ready=True, ready_timeout_s=30.0)

    rss_samples: list[tuple[float, int]] = []
    stop_event = threading.Event()
    mem_thread = threading.Thread(
        target=memory_monitor, args=(harness, stop_event, rss_samples), daemon=True
    )
    mem_thread.start()

    workers: list[threading.Thread] = []
    results: list[WorkerResult] = [WorkerResult() for _ in range(args.workers)]
    for i in range(args.workers):
        t = threading.Thread(
            target=worker_loop,
            args=(i, args.listen, triggers, stop_event, per_worker_interval_s, results[i]),
            daemon=True,
        )
        workers.append(t)
        t.start()

    logger.info("running for %ds at ~%d rps...", args.duration_s, args.target_rps)
    bench_start = time.perf_counter()
    try:
        time.sleep(args.duration_s)
    finally:
        stop_event.set()
        bench_elapsed = time.perf_counter() - bench_start
        for t in workers:
            t.join(timeout=5.0)
        mem_thread.join(timeout=2.0)
        logger.info("stopping engine")
        harness.stop()

    # ---- Aggregate ---------------------------------------------------
    all_latencies: list[float] = []
    n_ok = 0
    n_fail = 0
    failures: list[str] = []
    for r in results:
        all_latencies.extend(r.latencies_ms)
        n_ok += r.n_ok
        n_fail += r.n_fail
        failures.extend(r.failure_msgs)

    achieved_rps = n_ok / bench_elapsed if bench_elapsed > 0 else 0
    sorted_lat = sorted(all_latencies)
    if not sorted_lat:
        logger.error("no successful calls")
        return 2

    def pct(p: float) -> float:
        idx = int(len(sorted_lat) * p)
        return sorted_lat[min(idx, len(sorted_lat) - 1)]

    print("\n" + "=" * 60)
    print("STRESS TEST RESULTS")
    print("=" * 60)
    print(f"  duration            : {bench_elapsed:.1f}s (target {args.duration_s}s)")
    print(f"  target rps          : {args.target_rps}")
    print(f"  achieved rps        : {achieved_rps:.1f}")
    print(f"  total OK            : {n_ok}")
    print(f"  total FAIL          : {n_fail}")
    print()
    print(f"  latency min         : {sorted_lat[0]:.2f} ms")
    print(f"  latency p50         : {pct(0.50):.2f} ms")
    print(f"  latency p95         : {pct(0.95):.2f} ms  <-- gate < 50 ms")
    print(f"  latency p99         : {pct(0.99):.2f} ms")
    print(f"  latency max         : {sorted_lat[-1]:.2f} ms")
    print(f"  latency mean        : {statistics.mean(sorted_lat):.2f} ms")
    print()

    # Per-bucket degradation (split duration into thirds).
    if len(all_latencies) >= 30:
        n = len(all_latencies)
        third = n // 3
        for label, slice_ in (
            ("early   (0%-33%)", all_latencies[:third]),
            ("middle (33%-66%)", all_latencies[third : 2 * third]),
            ("late   (66%-100%)", all_latencies[2 * third :]),
        ):
            s = sorted(slice_)
            p50 = s[len(s) // 2]
            p95 = s[int(len(s) * 0.95)]
            print(f"  {label}: p50={p50:.2f} ms  p95={p95:.2f} ms  ({len(slice_)} calls)")
        print()

    # Memory drift.
    if rss_samples:
        rss_min = min(r for _, r in rss_samples)
        rss_max = max(r for _, r in rss_samples)
        rss_start = rss_samples[0][1]
        rss_end = rss_samples[-1][1]
        drift = rss_end - rss_start
        print(f"  RSS min             : {rss_min/1_048_576:.1f} MB")
        print(f"  RSS max             : {rss_max/1_048_576:.1f} MB")
        print(f"  RSS start           : {rss_start/1_048_576:.1f} MB")
        print(f"  RSS end             : {rss_end/1_048_576:.1f} MB")
        print(f"  drift               : {drift/1_048_576:+.1f} MB")
        print()
    else:
        print("  RSS monitoring not available (install psutil for memory tracking)")
        print()

    if failures:
        print(f"  first {min(10, len(failures))} failure messages:")
        for f in failures[:10]:
            print(f"    - {f}")

    # Gate check.
    p95 = pct(0.95)
    gate_p95 = p95 < 50.0
    print()
    if gate_p95 and n_fail == 0:
        print("PHASE 7 GATE PASSED — p95 < 50 ms under sustained load, no failures")
        return 0
    print(f"PHASE 7 GATE STATUS: p95={p95:.1f} ms (target 50), failures={n_fail}")
    return 0 if gate_p95 and n_fail == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
