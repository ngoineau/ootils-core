"""
bench_python.py — Python kernel baseline on the JSONL dataset.

Reads ./data/buckets.jsonl, parses inputs ONCE outside the bench loop
(so we measure compute, not JSON parsing), then runs `compute_pi_node`
N times over the dataset.

Reports throughput PI/s, latency p50/p95, total elapsed.
"""
from __future__ import annotations

import json
import os
import statistics
import sys
import time
from datetime import date
from decimal import Decimal

# Import the actual kernel — the one Rust will be compared against.
SRC = os.path.join(os.path.dirname(__file__), "..", "..", "src")
sys.path.insert(0, os.path.abspath(SRC))
from ootils_core.engine.kernel.calc.projection import ProjectionKernel  # noqa: E402

DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "buckets.jsonl")
N_ITERATIONS = 1  # number of full passes over the dataset


def _parse_date(s: str) -> date:
    y, m, d = s.split("-")
    return date(int(y), int(m), int(d))


def main() -> int:
    if not os.path.exists(DATA_PATH):
        print(f"Missing {DATA_PATH} — run extract_data.py first", file=sys.stderr)
        return 2

    # ---- Load + pre-parse (NOT measured) ------------------------------
    print(f"Loading {DATA_PATH}...", file=sys.stderr)
    records: list[dict] = []
    with open(DATA_PATH) as f:
        for line in f:
            r = json.loads(line)
            records.append({
                "opening_stock": Decimal(r["opening_stock"]),
                "bucket_start": _parse_date(r["bucket_start"]),
                "bucket_end": _parse_date(r["bucket_end"]),
                "supplies": [(_parse_date(d), Decimal(q)) for d, q in r["supplies"]],
                "demands": [(_parse_date(d), Decimal(q)) for d, q in r["demands"]],
            })
    n = len(records)
    total_supplies = sum(len(r["supplies"]) for r in records)
    total_demands = sum(len(r["demands"]) for r in records)
    print(
        f"  → {n} buckets, {total_supplies} supplies, {total_demands} demands",
        file=sys.stderr,
    )

    kernel = ProjectionKernel()

    # ---- Bench ---------------------------------------------------------
    print(f"Running {N_ITERATIONS} pass(es) over the dataset...", file=sys.stderr)
    per_call_ns: list[int] = []
    t0 = time.perf_counter_ns()
    for _ in range(N_ITERATIONS):
        for r in records:
            tc0 = time.perf_counter_ns()
            kernel.compute_pi_node(
                opening_stock=r["opening_stock"],
                supply_events=r["supplies"],
                demand_events=r["demands"],
                bucket_start=r["bucket_start"],
                bucket_end=r["bucket_end"],
            )
            per_call_ns.append(time.perf_counter_ns() - tc0)
    total_ns = time.perf_counter_ns() - t0

    # ---- Stats ---------------------------------------------------------
    total_s = total_ns / 1e9
    total_calls = n * N_ITERATIONS
    throughput = total_calls / total_s if total_s > 0 else 0

    srt = sorted(per_call_ns)
    p50_us = srt[len(srt) // 2] / 1000
    p95_us = srt[int(0.95 * len(srt))] / 1000
    mean_us = statistics.mean(per_call_ns) / 1000

    print()
    print("=== Python kernel bench ===")
    print(f"  Buckets         : {n}")
    print(f"  Iterations      : {N_ITERATIONS}")
    print(f"  Total calls     : {total_calls}")
    print(f"  Total time      : {total_s:.3f} s")
    print(f"  Throughput      : {throughput:.0f} PI/s")
    print()
    print(f"  Latency / call  : p50={p50_us:.2f} µs  mean={mean_us:.2f} µs  p95={p95_us:.2f} µs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
