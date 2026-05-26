"""
bench_agent_workload.py — measures Architecture B for the agent-first
workload (P3.1-P3.5).

Simulates a typical agent reasoning loop:
  1. Fork a sandbox (P2.1.a O(1) — ArcSwap)
  2. PropagateBatch N events in one RPC (P3.2 — avoids N×RTT)
  3. GetNode the result (P3.1 — overlay-aware)
  4. Fork-from-scenario to explore a branch (P3.5 — MCTS)
  5. PropagateBatch on the child branch
  6. Compare outcomes (mental diff)

Reports:
  - Per-step latency
  - End-to-end "agent decision cycle" time
  - Throughput per agent

Usage:
    DATABASE_URL=postgresql://ootils:ootils@host:5432/ootils_bench_l \\
    python scripts/bench_agent_workload.py --n-agents 10 --cycles-per-agent 50
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
from ootils_core._grpc import engine_pb2  # noqa: E402

BASELINE = UUID("00000000-0000-0000-0000-000000000001")


@dataclass
class AgentResult:
    fork_us: list[float] = field(default_factory=list)
    propag_batch_ms: list[float] = field(default_factory=list)
    propag_batch_count: list[int] = field(default_factory=list)
    read_us: list[float] = field(default_factory=list)
    fork_branch_us: list[float] = field(default_factory=list)
    cycle_total_ms: list[float] = field(default_factory=list)
    failures: int = 0
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


def agent_loop(
    client_addr: str,
    triggers: list[UUID],
    cycles: int,
    batch_size: int,
    result: AgentResult,
    agent_idx: int,
):
    """One agent's reasoning loop: fork, batch propagate, read, branch, propagate, repeat."""
    try:
        with EngineClient.connect(client_addr) as client:
            for cycle in range(cycles):
                cycle_t0 = time.perf_counter()

                # 1. Fork sandbox (P2.1.a)
                t = time.perf_counter_ns()
                req_fork = engine_pb2.ForkRequest(
                    parent_scenario_id="",
                    name=f"agent-{agent_idx}-cycle-{cycle}",
                )
                info = client._stub.ForkScenario(req_fork, timeout=5.0)
                sid = UUID(info.id)
                with result.lock:
                    result.fork_us.append((time.perf_counter_ns() - t) / 1000.0)

                # 2. PropagateBatch (P3.2)
                events = [
                    engine_pb2.BatchEvent(
                        event_id=str(uuid4()),
                        event_type="supply_qty_changed",
                        trigger_node_id=str(triggers[(agent_idx * 7 + cycle * 3 + i) % len(triggers)]),
                        payload=b"",
                    )
                    for i in range(batch_size)
                ]
                t = time.perf_counter()
                req_batch = engine_pb2.PropagateBatchRequest(
                    scenario_id=str(sid),
                    events=events,
                )
                batch_resp = client._stub.PropagateBatch(req_batch, timeout=10.0)
                batch_ms = (time.perf_counter() - t) * 1000.0
                with result.lock:
                    result.propag_batch_ms.append(batch_ms)
                    result.propag_batch_count.append(len(batch_resp.results))
                if batch_resp.failed_at_index != -1:
                    with result.lock:
                        result.failures += 1
                    continue

                # 3. Read overlay-aware (P3.1)
                t = time.perf_counter_ns()
                node = client.get_node(sid, events[0].trigger_node_id and triggers[(agent_idx * 7 + cycle * 3) % len(triggers)] or triggers[0])
                with result.lock:
                    result.read_us.append((time.perf_counter_ns() - t) / 1000.0)

                # 4. Fork-from-scenario branch (P3.5)
                t = time.perf_counter_ns()
                req_branch = engine_pb2.ForkRequest(
                    parent_scenario_id=str(sid),
                    name=f"agent-{agent_idx}-cycle-{cycle}-branch",
                )
                branch_info = client._stub.ForkScenario(req_branch, timeout=5.0)
                branch_sid = UUID(branch_info.id)
                with result.lock:
                    result.fork_branch_us.append((time.perf_counter_ns() - t) / 1000.0)

                # 5. Propagate on branch (small batch)
                branch_events = [
                    engine_pb2.BatchEvent(
                        event_id=str(uuid4()),
                        event_type="supply_qty_changed",
                        trigger_node_id=str(triggers[(agent_idx + i) % len(triggers)]),
                        payload=b"",
                    )
                    for i in range(3)
                ]
                req_branch_batch = engine_pb2.PropagateBatchRequest(
                    scenario_id=str(branch_sid),
                    events=branch_events,
                )
                client._stub.PropagateBatch(req_branch_batch, timeout=10.0)

                # 6. Cleanup (DeleteScenario for both)
                client._stub.DeleteScenario(
                    engine_pb2.DeleteRequest(scenario_id=str(branch_sid)),
                    timeout=5.0,
                )
                client._stub.DeleteScenario(
                    engine_pb2.DeleteRequest(scenario_id=str(sid)),
                    timeout=5.0,
                )

                cycle_ms = (time.perf_counter() - cycle_t0) * 1000.0
                with result.lock:
                    result.cycle_total_ms.append(cycle_ms)

    except Exception as e:  # noqa: BLE001
        logger.warning("agent %d crashed: %s", agent_idx, e)


def percentiles(samples):
    if not samples:
        return {"n": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "mean": 0.0}
    s = sorted(samples)
    n = len(s)
    return {
        "n": n,
        "p50": s[n // 2],
        "p95": s[int(0.95 * n)],
        "p99": s[int(0.99 * n)],
        "mean": statistics.mean(s),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--listen", default="127.0.0.1:51070")
    p.add_argument("--n-agents", type=int, default=10)
    p.add_argument("--cycles-per-agent", type=int, default=20)
    p.add_argument("--batch-size", type=int, default=10, help="events per PropagateBatch RPC")
    args = p.parse_args()
    if not args.dsn:
        return 1

    binary = find_binary()
    wal = Path(os.environ.get("TEMP", "/tmp")) / f"bench-agent-{os.getpid()}.wal"
    if wal.exists():
        wal.unlink()

    triggers = fetch_triggers(args.dsn, n=100)
    if not triggers:
        return 1

    logger.info(
        "agent bench: %d agents × %d cycles × %d events/batch",
        args.n_agents, args.cycles_per_agent, args.batch_size,
    )
    harness = EngineHarness(
        binary_path=binary,
        dsn=args.dsn,
        listen_addr=args.listen,
        wal_path=wal,
        flush_interval_ms=100,
        extra_env={"OOTILS_SCENARIO_TTL_SEC": "0"},
    )
    harness.start(wait_for_ready=True, ready_timeout_s=30.0)

    result = AgentResult()
    threads = []
    t0_global = time.perf_counter()
    try:
        for i in range(args.n_agents):
            t = threading.Thread(
                target=agent_loop,
                args=(args.listen, triggers, args.cycles_per_agent, args.batch_size, result, i),
                daemon=True,
            )
            threads.append(t)
            t.start()
        for t in threads:
            t.join(timeout=600.0)
    finally:
        wall_s = time.perf_counter() - t0_global
        harness.stop()
        try:
            wal.unlink()
        except OSError:
            pass

    fork_p = percentiles(result.fork_us)
    batch_p = percentiles(result.propag_batch_ms)
    read_p = percentiles(result.read_us)
    branch_p = percentiles(result.fork_branch_us)
    cycle_p = percentiles(result.cycle_total_ms)

    total_events = sum(result.propag_batch_count) + sum(result.propag_batch_count) // (args.batch_size // 3 + 1)
    total_cycles = cycle_p["n"]
    events_per_sec = sum(result.propag_batch_count) / wall_s if wall_s > 0 else 0
    cycles_per_sec = total_cycles / wall_s if wall_s > 0 else 0
    cycles_per_agent_per_sec = cycles_per_sec / args.n_agents if args.n_agents > 0 else 0

    print("\n" + "=" * 70)
    print(f"AGENT WORKLOAD BENCH — {args.n_agents} agents × {args.cycles_per_agent} cycles")
    print(f"  Wall time: {wall_s:.1f}s | Total cycles: {total_cycles} | Failures: {result.failures}")
    print("=" * 70)
    print(f"\n[FORK sandbox] (P2.1.a ArcSwap)")
    print(f"  µs: p50={fork_p['p50']:.1f}  p95={fork_p['p95']:.1f}  p99={fork_p['p99']:.1f}")
    print(f"\n[PROPAGATE BATCH] {args.batch_size} events / batch (P3.2)")
    print(f"  ms: p50={batch_p['p50']:.2f}  p95={batch_p['p95']:.2f}  p99={batch_p['p99']:.2f}")
    avg_per_event = batch_p["p50"] / args.batch_size
    print(f"  avg per event: {avg_per_event:.3f} ms")
    print(f"\n[GETNODE overlay-aware] (P3.1)")
    print(f"  µs: p50={read_p['p50']:.1f}  p95={read_p['p95']:.1f}  p99={read_p['p99']:.1f}")
    print(f"\n[FORK from scenario] MCTS branch (P3.5)")
    print(f"  µs: p50={branch_p['p50']:.1f}  p95={branch_p['p95']:.1f}  p99={branch_p['p99']:.1f}")
    print(f"\n[FULL AGENT CYCLE] fork + batch + read + branch + batch + cleanup")
    print(f"  ms: p50={cycle_p['p50']:.2f}  p95={cycle_p['p95']:.2f}  p99={cycle_p['p99']:.2f}")
    print(f"\n[THROUGHPUT]")
    print(f"  Events/sec total: {events_per_sec:.0f}")
    print(f"  Decision cycles/sec total: {cycles_per_sec:.1f}")
    print(f"  Decision cycles/sec/agent: {cycles_per_agent_per_sec:.1f}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
