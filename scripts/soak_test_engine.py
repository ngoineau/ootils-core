"""
soak_test_engine.py — long-duration stability validation (item #5).

Runs the engine under sustained light load for `--duration-h` hours
(default 24h). Samples latency + RSS every minute. Fails if either:
- p95 latency drifts upward beyond `--latency-drift-threshold` (default 50%)
- RSS grows beyond `--rss-cap-mb` (default 2 GB)
- any propagation fails

Intended for nightly CI or pre-release validation, not the regular
test loop. Designed to be killable via SIGINT at any point with
partial results printed.

Usage (example, runs 1h with 50 rps):
    DATABASE_URL=postgresql://ootils:ootils@host:5432/ootils_bench_l \\
    python scripts/soak_test_engine.py --target-rps 50 --duration-h 1
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import statistics
import sys
import threading
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


def fetch_triggers(dsn: str, n: int = 5000) -> list[UUID]:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT node_id FROM nodes "
            "WHERE node_type='ProjectedInventory' AND scenario_id=%s "
            "AND active=TRUE ORDER BY random() LIMIT %s",
            (BASELINE, n),
        ).fetchall()
        return [UUID(str(r["node_id"])) for r in rows]


class SoakResult:
    def __init__(self):
        self.latencies: list[float] = []
        self.failures: int = 0
        self.bucket_p95: list[tuple[float, float]] = []  # (elapsed_h, p95_ms)
        self.bucket_rss: list[tuple[float, float]] = []  # (elapsed_h, rss_mb)
        self.lock = threading.Lock()


def worker(client: EngineClient, triggers: list[UUID], stop: threading.Event, result: SoakResult, interval_s: float) -> None:
    next_fire = time.perf_counter()
    idx = 0
    while not stop.is_set():
        now = time.perf_counter()
        if now < next_fire:
            time.sleep(min(next_fire - now, 0.1))
            continue
        t0 = time.perf_counter()
        try:
            client.propagate(
                scenario_id=BASELINE,
                event_id=uuid4(),
                event_type="supply_qty_changed",
                trigger_node_id=triggers[idx % len(triggers)],
                timeout=10.0,
            )
            elapsed_ms = (time.perf_counter() - t0) * 1000
            with result.lock:
                result.latencies.append(elapsed_ms)
                # Cap buffer so we don't OOM the test driver on long soak.
                if len(result.latencies) > 100_000:
                    result.latencies = result.latencies[-50_000:]
        except Exception:
            with result.lock:
                result.failures += 1
        idx += 1
        next_fire += interval_s


def sampler(harness: EngineHarness, result: SoakResult, stop: threading.Event, sample_interval_s: float = 60.0) -> None:
    import psutil

    start = time.perf_counter()
    while not stop.is_set():
        time.sleep(min(sample_interval_s, 5.0))
        if stop.is_set():
            break
        with result.lock:
            window = result.latencies[-1000:] if result.latencies else []
        if window:
            window_sorted = sorted(window)
            p95 = window_sorted[int(0.95 * len(window_sorted))]
        else:
            p95 = 0.0
        try:
            rss_mb = psutil.Process(harness.process.pid).memory_info().rss / 1_048_576
        except Exception:
            rss_mb = 0.0
        elapsed_h = (time.perf_counter() - start) / 3600.0
        with result.lock:
            result.bucket_p95.append((elapsed_h, p95))
            result.bucket_rss.append((elapsed_h, rss_mb))
        logger.info(
            "soak sample: t=%.3fh p95=%.2fms rss=%.1fMB calls=%d fail=%d",
            elapsed_h, p95, rss_mb,
            len(result.latencies), result.failures,
        )


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--listen", default="127.0.0.1:50060")
    p.add_argument("--target-rps", type=int, default=50)
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--duration-h", type=float, default=24.0)
    p.add_argument("--sample-interval-s", type=int, default=60)
    p.add_argument("--latency-drift-threshold", type=float, default=0.50, help="p95 drift fail threshold (0.5 = 50%)")
    p.add_argument("--rss-cap-mb", type=float, default=2048.0)
    args = p.parse_args()
    if not args.dsn:
        logger.error("set DATABASE_URL or pass --dsn")
        return 1

    binary = find_binary()
    wal = Path(os.environ.get("TEMP", "/tmp")) / f"soak-{os.getpid()}.wal"
    if wal.exists():
        wal.unlink()

    triggers = fetch_triggers(args.dsn)
    logger.info(
        "soak: rps=%d workers=%d duration=%.1fh triggers=%d",
        args.target_rps, args.workers, args.duration_h, len(triggers),
    )

    harness = EngineHarness(binary, args.dsn, args.listen, wal_path=wal, flush_interval_ms=100)
    harness.start(wait_for_ready=True, ready_timeout_s=30.0)

    per_worker_rate = args.target_rps / args.workers
    interval_s = 1.0 / per_worker_rate

    result = SoakResult()
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda s, f: stop.set())

    threads = []
    for _ in range(args.workers):
        c = EngineClient.connect(args.listen)
        t = threading.Thread(target=worker, args=(c, triggers, stop, result, interval_s), daemon=True)
        threads.append((t, c))
        t.start()

    sampler_t = threading.Thread(target=sampler, args=(harness, result, stop, args.sample_interval_s), daemon=True)
    sampler_t.start()

    duration_s = args.duration_h * 3600.0
    start_t = time.perf_counter()
    try:
        while not stop.is_set():
            if time.perf_counter() - start_t >= duration_s:
                break
            time.sleep(1.0)
    finally:
        stop.set()
        for t, _ in threads:
            t.join(timeout=2.0)
        for _, c in threads:
            c.close()
        sampler_t.join(timeout=5.0)
        harness.stop()

    # ---- Final report ------------------------------------------------
    print("\n" + "=" * 60)
    print("SOAK TEST RESULTS")
    print("=" * 60)
    print(f"  duration       : {(time.perf_counter() - start_t)/3600.0:.3f}h")
    print(f"  target rps     : {args.target_rps}")
    print(f"  total calls    : {len(result.latencies)}")
    print(f"  total failures : {result.failures}")

    if not result.latencies:
        print("  no successful calls — fail")
        return 2

    overall = sorted(result.latencies)
    print(f"  overall p50    : {overall[len(overall)//2]:.2f} ms")
    print(f"  overall p95    : {overall[int(0.95*len(overall))]:.2f} ms")
    print(f"  overall p99    : {overall[int(0.99*len(overall))]:.2f} ms")
    print(f"  overall mean   : {statistics.mean(overall):.2f} ms")

    # Drift between first 10% and last 10% of buckets.
    if len(result.bucket_p95) >= 6:
        early = result.bucket_p95[: max(1, len(result.bucket_p95) // 10)]
        late = result.bucket_p95[-max(1, len(result.bucket_p95) // 10):]
        early_p95 = statistics.mean([p for _, p in early])
        late_p95 = statistics.mean([p for _, p in late])
        drift = (late_p95 - early_p95) / max(early_p95, 0.01)
        print(f"  latency drift  : {drift*100:+.1f}% (early p95 {early_p95:.2f} → late p95 {late_p95:.2f})")
    else:
        drift = 0.0

    if result.bucket_rss:
        rss_start = result.bucket_rss[0][1]
        rss_end = result.bucket_rss[-1][1]
        rss_max = max(r for _, r in result.bucket_rss)
        print(f"  RSS start      : {rss_start:.1f} MB")
        print(f"  RSS end        : {rss_end:.1f} MB")
        print(f"  RSS max        : {rss_max:.1f} MB")
        print(f"  RSS drift      : {rss_end - rss_start:+.1f} MB")
    else:
        rss_max = 0.0

    # Gate checks.
    passed = True
    if result.failures > 0:
        print(f"  FAIL: {result.failures} failures during soak")
        passed = False
    if drift > args.latency_drift_threshold:
        print(f"  FAIL: latency drift {drift*100:.1f}% > threshold {args.latency_drift_threshold*100:.0f}%")
        passed = False
    if rss_max > args.rss_cap_mb:
        print(f"  FAIL: RSS max {rss_max:.1f} > cap {args.rss_cap_mb}")
        passed = False

    print()
    if passed:
        print(f"SOAK TEST PASSED — engine stable over {args.duration_h:.1f}h")
        return 0
    print(f"SOAK TEST FAILED — see above")
    return 2


if __name__ == "__main__":
    sys.exit(main())
