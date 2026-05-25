"""
chaos_test_multi_user.py — push Architecture B Phase 2.1 to its limits.

Combines multiple stress vectors that the existing stress_test_engine.py
doesn't exercise:

  1. **Massive multi-tenant**: N users (default 200) each forking their
     own scenario and propagating concurrently.
  2. **Sustained burst**: each worker fires propagations as fast as the
     engine accepts (no rate throttling — pure backpressure test).
  3. **Mixed read/write workload**: some workers propagate, others
     hammer GetNode + ListScenarios to stress the F-009 lock split.
  4. **Memory accounting**: sample RSS every second, report drift.
  5. **Fork storm**: optionally pre-fork all scenarios up front
     (cold cache) vs lazily during load (steady-state mix).
  6. **Cap behavior**: optionally set tight WAL/queue caps to verify
     RESOURCE_EXHAUSTED surfaces cleanly without crashing.

The script's job is NOT to pass/fail (the existing pytest covers
contracts). Its job is to MEASURE what happens at the limits + give
operators a feel for the engine's behavior envelope.

Usage:
    DATABASE_URL=postgresql://ootils:ootils@host:5432/ootils_bench_l \\
    python scripts/chaos_test_multi_user.py \\
        --n-users 200 \\
        --duration-s 60 \\
        --read-workers 20 \\
        --propag-workers 100
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


@dataclass
class ChaosResult:
    propag_latencies_ms: list[float] = field(default_factory=list)
    propag_failures: int = 0
    read_latencies_ms: list[float] = field(default_factory=list)
    read_failures: int = 0
    list_latencies_ms: list[float] = field(default_factory=list)
    list_failures: int = 0
    rss_samples_mb: list[tuple[float, float]] = field(default_factory=list)  # (t, mb)
    fork_latencies_us: list[float] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)


def find_binary() -> Path:
    base = ROOT / "rust" / "target" / "release"
    for name in ("ootils-engine.exe", "ootils-engine"):
        p = base / name
        if p.exists():
            return p
    raise FileNotFoundError(f"missing engine binary in {base}")


def fetch_triggers(dsn: str, n: int = 1000) -> list[UUID]:
    """Pick diverse trigger PIs across the baseline so different
    scenarios touch different parts of the graph."""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT node_id FROM nodes WHERE node_type='ProjectedInventory' "
            "AND scenario_id=%s AND active=TRUE AND bucket_sequence=0 "
            "ORDER BY node_id LIMIT %s",
            (BASELINE, n),
        ).fetchall()
    return [UUID(str(r["node_id"])) for r in rows]


def fork_users(client: EngineClient, n_users: int, result: ChaosResult) -> list[UUID]:
    """Pre-fork N user scenarios. Each fork should be sub-µs with ArcSwap."""
    logger.info("forking %d user scenarios...", n_users)
    scenarios = []
    for i in range(n_users):
        t0 = time.perf_counter_ns()
        info = client.fork_scenario(BASELINE, name=f"chaos-user-{i:04d}")
        elapsed_us = (time.perf_counter_ns() - t0) / 1000.0
        with result.lock:
            result.fork_latencies_us.append(elapsed_us)
        scenarios.append(UUID(info.id))
        if (i + 1) % 50 == 0:
            logger.info("  forked %d / %d (last fork %.1f µs)", i + 1, n_users, elapsed_us)
    return scenarios


def propag_worker(
    client_addr: str,
    scenarios: list[UUID],
    triggers: list[UUID],
    stop: threading.Event,
    result: ChaosResult,
    worker_idx: int,
):
    """Hammers propagations on a random user scenario with a random trigger.
    No throttling — fires as fast as the engine accepts."""
    try:
        with EngineClient.connect(client_addr) as client:
            n = 0
            while not stop.is_set():
                # Rotate through scenarios + triggers so different cores
                # of the propagator are exercised.
                sid = scenarios[(worker_idx + n) % len(scenarios)]
                trg = triggers[(worker_idx * 7 + n * 3) % len(triggers)]
                t0 = time.perf_counter()
                try:
                    client.propagate(
                        scenario_id=sid,
                        event_id=uuid4(),
                        event_type="supply_qty_changed",
                        trigger_node_id=trg,
                        timeout=10.0,
                    )
                    elapsed_ms = (time.perf_counter() - t0) * 1000.0
                    with result.lock:
                        result.propag_latencies_ms.append(elapsed_ms)
                        # Cap memory — keep last 100K samples max.
                        if len(result.propag_latencies_ms) > 100_000:
                            result.propag_latencies_ms = result.propag_latencies_ms[-50_000:]
                except Exception:
                    with result.lock:
                        result.propag_failures += 1
                n += 1
    except Exception as e:  # noqa: BLE001
        logger.warning("propag_worker %d crashed: %s", worker_idx, e)


def read_worker(
    client_addr: str,
    scenarios: list[UUID],
    triggers: list[UUID],
    stop: threading.Event,
    result: ChaosResult,
    worker_idx: int,
):
    """Hammers GetNode + ListScenarios concurrently with propagations.
    Stresses the F-009 read-lock-plan path."""
    try:
        with EngineClient.connect(client_addr) as client:
            n = 0
            while not stop.is_set():
                if n % 5 == 0:
                    # 1/5 calls: list scenarios (different lock pattern)
                    t0 = time.perf_counter()
                    try:
                        client.list_scenarios()
                        elapsed_ms = (time.perf_counter() - t0) * 1000.0
                        with result.lock:
                            result.list_latencies_ms.append(elapsed_ms)
                    except Exception:
                        with result.lock:
                            result.list_failures += 1
                else:
                    # 4/5 calls: GetNode on baseline
                    trg = triggers[(worker_idx * 11 + n) % len(triggers)]
                    t0 = time.perf_counter()
                    try:
                        client.get_node(BASELINE, trg)
                        elapsed_ms = (time.perf_counter() - t0) * 1000.0
                        with result.lock:
                            result.read_latencies_ms.append(elapsed_ms)
                            if len(result.read_latencies_ms) > 100_000:
                                result.read_latencies_ms = result.read_latencies_ms[-50_000:]
                    except Exception:
                        with result.lock:
                            result.read_failures += 1
                n += 1
    except Exception as e:  # noqa: BLE001
        logger.warning("read_worker %d crashed: %s", worker_idx, e)


def memory_sampler(
    harness: EngineHarness,
    result: ChaosResult,
    stop: threading.Event,
):
    """Samples RSS every second."""
    try:
        import psutil
    except ImportError:
        logger.warning("psutil not installed — skipping memory sampling")
        return
    start = time.perf_counter()
    while not stop.is_set():
        if harness.process is None or harness.process.poll() is not None:
            logger.error("engine died during chaos run!")
            break
        try:
            rss = psutil.Process(harness.process.pid).memory_info().rss / 1_048_576
            elapsed = time.perf_counter() - start
            with result.lock:
                result.rss_samples_mb.append((elapsed, rss))
        except Exception:
            pass
        stop.wait(timeout=1.0)


def percentiles(samples: list[float]) -> dict:
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


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--listen", default="127.0.0.1:51060")
    p.add_argument("--n-users", type=int, default=200, help="number of forked scenarios")
    p.add_argument("--propag-workers", type=int, default=50, help="threads firing propagations")
    p.add_argument("--read-workers", type=int, default=20, help="threads firing GetNode/ListScenarios")
    p.add_argument("--duration-s", type=int, default=60, help="how long to sustain the load")
    p.add_argument("--wal-max-mb", type=int, default=512, help="WAL size cap (test backpressure)")
    p.add_argument("--queue-max", type=int, default=500_000, help="queue depth cap")
    p.add_argument("--scenario-ttl-sec", type=int, default=0, help="scenario TTL (0 = disabled for stress)")
    args = p.parse_args()
    if not args.dsn:
        logger.error("set DATABASE_URL or pass --dsn")
        return 1

    binary = find_binary()
    wal = Path(os.environ.get("TEMP", "/tmp")) / f"chaos-{os.getpid()}.wal"
    if wal.exists():
        wal.unlink()

    triggers = fetch_triggers(args.dsn, n=1000)
    logger.info("loaded %d trigger PIs", len(triggers))
    if not triggers:
        logger.error("no triggers — empty baseline?")
        return 1

    logger.info(
        "spawning engine: listen=%s wal_max_mb=%d queue_max=%d ttl=%ds",
        args.listen, args.wal_max_mb, args.queue_max, args.scenario_ttl_sec,
    )
    harness = EngineHarness(
        binary_path=binary,
        dsn=args.dsn,
        listen_addr=args.listen,
        wal_path=wal,
        flush_interval_ms=100,
        extra_env={
            "OOTILS_WAL_MAX_BYTES": str(args.wal_max_mb * 1024 * 1024),
            "OOTILS_QUEUE_MAX_DEPTH": str(args.queue_max),
            "OOTILS_SCENARIO_TTL_SEC": str(args.scenario_ttl_sec),
        },
    )
    harness.start(wait_for_ready=True, ready_timeout_s=30.0)

    result = ChaosResult()
    stop = threading.Event()
    threads = []

    try:
        # ---- Phase 1: fork N users (cold start) ----
        with EngineClient.connect(args.listen) as setup_client:
            t_fork_start = time.perf_counter()
            scenarios = fork_users(setup_client, args.n_users, result)
            fork_total_s = time.perf_counter() - t_fork_start
            logger.info(
                "all %d users forked in %.2fs (%.0f forks/s)",
                len(scenarios), fork_total_s, len(scenarios) / fork_total_s,
            )

        # ---- Phase 2: sustained load ----
        logger.info(
            "starting load: %d propag workers + %d read workers for %ds",
            args.propag_workers, args.read_workers, args.duration_s,
        )
        for i in range(args.propag_workers):
            t = threading.Thread(
                target=propag_worker,
                args=(args.listen, scenarios, triggers, stop, result, i),
                daemon=True,
            )
            threads.append(t)
            t.start()
        for i in range(args.read_workers):
            t = threading.Thread(
                target=read_worker,
                args=(args.listen, scenarios, triggers, stop, result, i),
                daemon=True,
            )
            threads.append(t)
            t.start()

        # Memory sampler
        sampler = threading.Thread(target=memory_sampler, args=(harness, result, stop), daemon=True)
        threads.append(sampler)
        sampler.start()

        # Run for duration_s, then signal stop
        time.sleep(args.duration_s)
        logger.info("duration elapsed, signaling workers to stop")
        stop.set()
        for t in threads:
            t.join(timeout=5.0)

    finally:
        harness.stop()
        try:
            wal.unlink()
        except OSError:
            pass

    # ---- Report ----
    print("\n" + "=" * 70)
    print(f"CHAOS RESULTS — {args.n_users} users, {args.duration_s}s sustained")
    print("=" * 70)

    fork_p = percentiles(result.fork_latencies_us)
    print(f"\n[FORKS] total = {fork_p['n']}")
    print(f"  µs : p50={fork_p['p50']:.1f}  p95={fork_p['p95']:.1f}  p99={fork_p['p99']:.1f}  max={fork_p['max']:.1f}  mean={fork_p['mean']:.1f}")

    prop_p = percentiles(result.propag_latencies_ms)
    qps = prop_p["n"] / args.duration_s if args.duration_s > 0 else 0
    print(f"\n[PROPAGATIONS] total = {prop_p['n']}  failures = {result.propag_failures}  qps = {qps:.0f}")
    print(f"  ms : p50={prop_p['p50']:.2f}  p95={prop_p['p95']:.2f}  p99={prop_p['p99']:.2f}  max={prop_p['max']:.2f}")

    read_p = percentiles(result.read_latencies_ms)
    rqps = read_p["n"] / args.duration_s if args.duration_s > 0 else 0
    print(f"\n[GETNODE READS] total = {read_p['n']}  failures = {result.read_failures}  qps = {rqps:.0f}")
    print(f"  ms : p50={read_p['p50']:.3f}  p95={read_p['p95']:.3f}  p99={read_p['p99']:.3f}  max={read_p['max']:.3f}")

    list_p = percentiles(result.list_latencies_ms)
    print(f"\n[LIST_SCENARIOS] total = {list_p['n']}  failures = {result.list_failures}")
    print(f"  ms : p50={list_p['p50']:.2f}  p95={list_p['p95']:.2f}  p99={list_p['p99']:.2f}  max={list_p['max']:.2f}")

    if result.rss_samples_mb:
        rss_start = result.rss_samples_mb[0][1]
        rss_end = result.rss_samples_mb[-1][1]
        rss_max = max(r for _, r in result.rss_samples_mb)
        print(f"\n[MEMORY] start={rss_start:.0f} MB  end={rss_end:.0f} MB  max={rss_max:.0f} MB  drift={rss_end - rss_start:+.0f} MB")

    # Headline assertion
    print("\n" + "-" * 70)
    print("VERDICT:")
    if prop_p["n"] == 0:
        print("  ❌ ZERO propagations succeeded — engine likely unreachable")
        return 2
    if result.propag_failures > prop_p["n"] * 0.01:
        print(f"  [WARN]  propagation failure rate > 1%: {result.propag_failures}/{prop_p['n'] + result.propag_failures}")
    else:
        print(f"  [OK] propagation failure rate OK: {result.propag_failures} fails / {prop_p['n']} OK")
    if prop_p["p95"] > 100.0:
        print(f"  [WARN]  propagation p95 > 100ms: {prop_p['p95']:.1f} ms")
    else:
        print(f"  [OK] propagation p95 = {prop_p['p95']:.2f} ms (target < 100)")
    if read_p["p95"] > 50.0:
        print(f"  [WARN]  read p95 > 50ms (lock contention?): {read_p['p95']:.2f} ms")
    else:
        print(f"  [OK] read p95 = {read_p['p95']:.3f} ms (F-009 lock split holds)")
    if result.rss_samples_mb:
        drift = result.rss_samples_mb[-1][1] - result.rss_samples_mb[0][1]
        if drift > 200:
            print(f"  [WARN]  RSS drift > 200 MB: +{drift:.0f} MB (potential leak)")
        else:
            print(f"  [OK] RSS drift bounded: +{drift:.0f} MB")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
