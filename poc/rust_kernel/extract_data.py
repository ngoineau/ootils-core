"""
extract_data.py — Dump realistic PI bucket inputs from profile L to JSON.

Pulls 10,000 random ProjectedInventory nodes from `ootils_bench_l`,
resolves their incoming supply and demand edges, and serializes
(opening_stock, bucket_start, bucket_end, supplies, demands) per PI
as one JSON record per line (JSONL).

This is the *fair input* both Python and Rust benches consume — same
data, same shape, no parsing cost embedded in the bench loop.
"""
from __future__ import annotations

import json
import os
import sys
from decimal import Decimal
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

BASELINE = UUID("00000000-0000-0000-0000-000000000001")
N_BUCKETS = 10_000
OUT_PATH = os.path.join(os.path.dirname(__file__), "data", "buckets.jsonl")
DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://ootils:ootils@192.168.1.176:5432/ootils_bench_l",
)


def _decimal_to_str(x) -> str:
    """Keep full Decimal precision via string round-trip."""
    if x is None:
        return "0"
    return str(Decimal(str(x)))


def main() -> int:
    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        # Pick N random PI buckets with non-trivial supplies or demands
        # (so the bench measures real compute, not empty loops).
        print(f"Picking {N_BUCKETS} PI buckets from profile L...", file=sys.stderr)
        pi_rows = conn.execute(
            """
            SELECT n.node_id, n.opening_stock, n.time_span_start, n.time_span_end
            FROM nodes n
            WHERE n.node_type = 'ProjectedInventory'
              AND n.scenario_id = %s
              AND n.active = TRUE
              AND n.time_span_start IS NOT NULL
            ORDER BY random()
            LIMIT %s
            """,
            (BASELINE, N_BUCKETS),
        ).fetchall()

        if not pi_rows:
            print("No PI rows — DB empty?", file=sys.stderr)
            return 2

        # Batch-fetch all incoming supply + demand events for those PIs.
        node_ids = [r["node_id"] for r in pi_rows]

        print("Fetching incoming supplies...", file=sys.stderr)
        supplies = conn.execute(
            """
            SELECT e.to_node_id AS pi_node_id, s.time_ref AS event_date, s.quantity
            FROM edges e
            JOIN nodes s ON s.node_id = e.from_node_id
            WHERE e.to_node_id = ANY(%s)
              AND e.edge_type = 'replenishes'
              AND e.scenario_id = %s
              AND e.active = TRUE
              AND s.active = TRUE
              AND s.node_type IN (
                  'PurchaseOrderSupply','WorkOrderSupply','TransferSupply','PlannedSupply'
              )
            """,
            (node_ids, BASELINE),
        ).fetchall()

        print("Fetching incoming demands...", file=sys.stderr)
        demands = conn.execute(
            """
            SELECT
                e.to_node_id AS pi_node_id,
                COALESCE(d.time_ref, d.time_span_start) AS event_date,
                d.quantity
            FROM edges e
            JOIN nodes d ON d.node_id = e.from_node_id
            WHERE e.to_node_id = ANY(%s)
              AND e.edge_type = 'consumes'
              AND e.scenario_id = %s
              AND e.active = TRUE
              AND d.active = TRUE
              AND d.node_type IN (
                  'ForecastDemand','CustomerOrderDemand','DependentDemand','TransferDemand'
              )
            """,
            (node_ids, BASELINE),
        ).fetchall()

        # Group supplies/demands by PI node id
        sup_by_pi: dict[str, list[tuple[str, str]]] = {}
        for r in supplies:
            key = str(r["pi_node_id"])
            sup_by_pi.setdefault(key, []).append(
                (str(r["event_date"]), _decimal_to_str(r["quantity"]))
            )
        dem_by_pi: dict[str, list[tuple[str, str]]] = {}
        for r in demands:
            key = str(r["pi_node_id"])
            if r["event_date"] is None:
                continue
            dem_by_pi.setdefault(key, []).append(
                (str(r["event_date"]), _decimal_to_str(r["quantity"]))
            )

        # Stats
        total_sup = sum(len(v) for v in sup_by_pi.values())
        total_dem = sum(len(v) for v in dem_by_pi.values())
        pi_with_events = len(set(sup_by_pi) | set(dem_by_pi))
        print(
            f"  → {N_BUCKETS} PIs, {pi_with_events} with events, "
            f"{total_sup} supplies, {total_dem} demands",
            file=sys.stderr,
        )

        # Write JSONL
        with open(OUT_PATH, "w") as f:
            for r in pi_rows:
                key = str(r["node_id"])
                rec = {
                    "node_id": key,
                    "opening_stock": _decimal_to_str(r["opening_stock"]),
                    "bucket_start": str(r["time_span_start"]),
                    "bucket_end": str(r["time_span_end"]),
                    "supplies": sup_by_pi.get(key, []),
                    "demands": dem_by_pi.get(key, []),
                }
                f.write(json.dumps(rec) + "\n")

        print(f"Wrote {OUT_PATH}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
