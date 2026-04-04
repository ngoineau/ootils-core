"""
bridge.py — TemporalBridge: presentation layer, read-only.

Aggregates and disaggregates PI nodes at query time.
Never writes to the database — this is a pure read/transform layer.

Architecture (ADR-002d):
  - Sits above the computation layer
  - Does NOT participate in dirty-flag propagation
  - Does NOT apply contribution rules (that's the kernel's job)
  - Safe to call at any time without side effects
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

import psycopg

from ootils_core.engine.kernel.graph.store import GraphStore
from ootils_core.models import Node, NodeTypeTemporalPolicy

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Value objects returned by the Bridge
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AggregatedBucket:
    """
    A coarser-grain view of one or more PI nodes produced by the Bridge.
    Marked as approximated when the source data is coarser than the
    requested target grain (disaggregation case).
    """
    time_grain: str               # 'day' | 'week' | 'month'
    time_span_start: date         # inclusive
    time_span_end: date           # exclusive
    opening_stock: Decimal
    inflows: Decimal
    outflows: Decimal
    closing_stock: Decimal
    has_shortage: bool
    shortage_qty: Decimal
    source_node_ids: list[UUID]   # originating PI node IDs
    approximated: bool = False    # True when disaggregated from a coarser bucket


# ---------------------------------------------------------------------------
# Grain helpers
# ---------------------------------------------------------------------------

_GRAIN_ORDER: dict[str, int] = {
    "day": 0,
    "week": 1,
    "month": 2,
    "quarter": 3,
}


def _grain_rank(grain: str) -> int:
    """Higher rank = coarser grain."""
    try:
        return _GRAIN_ORDER[grain]
    except KeyError:
        raise ValueError(f"Unknown grain: {grain!r}. Valid values: {list(_GRAIN_ORDER)}")


def _week_start(d: date) -> date:
    """Return the Monday that starts the ISO week containing date d."""
    return d - timedelta(days=d.weekday())


def _month_start(d: date) -> date:
    """Return the first day of the month containing date d."""
    return date(d.year, d.month, 1)


def _bucket_key_for_grain(d: date, grain: str) -> date:
    """Return the canonical bucket reference date for a given grain."""
    if grain == "day":
        return d
    if grain == "week":
        return _week_start(d)
    if grain == "month":
        return _month_start(d)
    raise ValueError(f"Unsupported grain for bucketing: {grain!r}")


def _bucket_end_for_grain(bucket_start: date, grain: str) -> date:
    """Return the exclusive end date for a bucket starting at bucket_start."""
    if grain == "day":
        return bucket_start + timedelta(days=1)
    if grain == "week":
        return bucket_start + timedelta(weeks=1)
    if grain == "month":
        # Next month
        year = bucket_start.year + (bucket_start.month // 12)
        month = (bucket_start.month % 12) + 1
        return date(year, month, 1)
    raise ValueError(f"Unsupported grain for bucket end: {grain!r}")


# ---------------------------------------------------------------------------
# TemporalBridge
# ---------------------------------------------------------------------------


class TemporalBridge:
    """
    Read-only presentation layer — aggregates and disaggregates PI nodes.

    All methods accept a psycopg Connection with dict_row factory.
    NEVER writes to the database.

    GraphStore is instantiated once per connection in _load_series_nodes
    via the provided connection — this avoids storing a long-lived store
    and keeps the Bridge stateless across calls.
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_policy(
        self,
        node_type: str,
        db: psycopg.Connection,
    ) -> NodeTypeTemporalPolicy:
        """
        Load the NodeTypeTemporalPolicy for a node type from node_type_policies.

        Raises KeyError if no policy found for the given node_type.
        """
        row = db.execute(
            """
            SELECT policy_id, node_type,
                   zone1_grain, zone1_end_days,
                   zone2_grain, zone2_end_days,
                   zone3_grain,
                   week_start_dow,
                   active,
                   created_at, updated_at
            FROM node_type_policies
            WHERE node_type = %s AND active = TRUE
            """,
            (node_type,),
        ).fetchone()

        if row is None:
            raise KeyError(f"No active temporal policy found for node_type={node_type!r}")

        return NodeTypeTemporalPolicy(
            policy_id=UUID(str(row["policy_id"])),
            node_type=row["node_type"],
            zone1_grain=row["zone1_grain"],
            zone1_end_days=row["zone1_end_days"],
            zone2_grain=row["zone2_grain"],
            zone2_end_days=row["zone2_end_days"],
            zone3_grain=row["zone3_grain"],
            week_start_dow=row["week_start_dow"],
            active=bool(row["active"]),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )

    def aggregate(
        self,
        series_id: UUID,
        target_grain: str,
        db: psycopg.Connection,
    ) -> list[AggregatedBucket]:
        """
        Aggregate the PI nodes in a projection series to a coarser grain.

        Typical use: daily → weekly, daily → monthly, weekly → monthly.
        Fine-grain nodes that already match or are coarser than target_grain
        are grouped into target_grain buckets.

        Returns a list of AggregatedBucket ordered by time_span_start ASC.

        Rules:
          - closing_stock of a coarser bucket = closing_stock of its last fine-grain child
          - opening_stock = opening_stock of first child
          - inflows / outflows = sum of all children
          - has_shortage = any child has_shortage
          - shortage_qty = sum of children shortage_qty
          - approximated = False (aggregation is exact)
        """
        _grain_rank(target_grain)  # validate early

        nodes = self._load_series_nodes(series_id, db)
        if not nodes:
            logger.debug("aggregate: no nodes found for series_id=%s", series_id)
            return []

        # Group nodes by their target-grain bucket key
        buckets: dict[date, list[Node]] = {}
        for node in nodes:
            span_start = node.time_span_start
            if span_start is None:
                logger.warning(
                    "aggregate: node %s has no time_span_start — skipping", node.node_id
                )
                continue
            key = _bucket_key_for_grain(span_start, target_grain)
            buckets.setdefault(key, []).append(node)

        result: list[AggregatedBucket] = []
        for bucket_start in sorted(buckets):
            children = sorted(
                buckets[bucket_start],
                key=lambda n: (n.bucket_sequence or 0, str(n.node_id)),
            )
            bucket_end = _bucket_end_for_grain(bucket_start, target_grain)

            opening = children[0].opening_stock or Decimal("0")
            inflows = sum((n.inflows or Decimal("0") for n in children), Decimal("0"))
            outflows = sum((n.outflows or Decimal("0") for n in children), Decimal("0"))
            closing = children[-1].closing_stock or Decimal("0")
            has_shortage = any(n.has_shortage for n in children)
            shortage_qty = sum(
                (n.shortage_qty or Decimal("0") for n in children), Decimal("0")
            )
            source_ids = [n.node_id for n in children]

            result.append(
                AggregatedBucket(
                    time_grain=target_grain,
                    time_span_start=bucket_start,
                    time_span_end=bucket_end,
                    opening_stock=opening,
                    inflows=inflows,
                    outflows=outflows,
                    closing_stock=closing,
                    has_shortage=has_shortage,
                    shortage_qty=shortage_qty,
                    source_node_ids=source_ids,
                    approximated=False,
                )
            )

        logger.debug(
            "aggregate: series=%s target_grain=%s → %d buckets",
            series_id, target_grain, len(result),
        )
        return result

    def disaggregate(
        self,
        series_id: UUID,
        source_grain: str,
        target_grain: str,
        db: psycopg.Connection,
        distribution: str = "FLAT",
    ) -> list[AggregatedBucket]:
        """
        Disaggregate coarse-grain PI nodes into finer target_grain buckets.

        Typical use: monthly → weekly, monthly → daily, weekly → daily.
        Each source bucket is split into target_grain sub-buckets.

        Distribution modes:
          - FLAT (default): quantity is split evenly across sub-buckets.
                            Fractional remainders are added to the last sub-bucket.

        Returns a list of AggregatedBucket with approximated=True.
        All returned buckets are marked approximated since the sub-bucket
        values are computed by distribution, not by actual computation.

        Raises ValueError if target_grain is not finer than source_grain.
        """
        if _grain_rank(target_grain) >= _grain_rank(source_grain):
            raise ValueError(
                f"disaggregate: target_grain={target_grain!r} must be finer than "
                f"source_grain={source_grain!r}"
            )
        if distribution != "FLAT":
            raise NotImplementedError(
                f"Distribution mode {distribution!r} is not implemented. Only 'FLAT' is supported."
            )

        nodes = self._load_series_nodes(series_id, db)
        if not nodes:
            logger.debug("disaggregate: no nodes found for series_id=%s", series_id)
            return []

        result: list[AggregatedBucket] = []

        for node in nodes:
            if node.time_grain != source_grain:
                # Skip nodes at a different grain (e.g., daily nodes when disaggregating monthly)
                continue
            if node.time_span_start is None or node.time_span_end is None:
                logger.warning(
                    "disaggregate: node %s missing time_span_start/end — skipping",
                    node.node_id,
                )
                continue

            sub_buckets = self._enumerate_sub_buckets(
                node.time_span_start, node.time_span_end, target_grain
            )
            n_sub = len(sub_buckets)
            if n_sub == 0:
                continue

            inflows_total = node.inflows or Decimal("0")
            outflows_total = node.outflows or Decimal("0")

            # FLAT distribution: integer division + remainder on last bucket
            inflow_per_bucket, inflow_rem = divmod(inflows_total, Decimal(str(n_sub)))
            outflow_per_bucket, outflow_rem = divmod(outflows_total, Decimal(str(n_sub)))

            # Disaggregated sub-buckets share the source opening_stock logic:
            # - first sub-bucket carries the source opening_stock
            # - each subsequent opening = previous closing
            running_stock = node.opening_stock or Decimal("0")

            for idx, (sub_start, sub_end) in enumerate(sub_buckets):
                is_last = idx == n_sub - 1
                sub_inflows = inflow_per_bucket + (inflow_rem if is_last else Decimal("0"))
                sub_outflows = outflow_per_bucket + (outflow_rem if is_last else Decimal("0"))
                sub_opening = running_stock
                sub_closing = sub_opening + sub_inflows - sub_outflows
                sub_shortage = sub_closing < Decimal("0")
                sub_shortage_qty = abs(sub_closing) if sub_shortage else Decimal("0")

                result.append(
                    AggregatedBucket(
                        time_grain=target_grain,
                        time_span_start=sub_start,
                        time_span_end=sub_end,
                        opening_stock=sub_opening,
                        inflows=sub_inflows,
                        outflows=sub_outflows,
                        closing_stock=sub_closing,
                        has_shortage=sub_shortage,
                        shortage_qty=sub_shortage_qty,
                        source_node_ids=[node.node_id],
                        approximated=True,
                    )
                )
                running_stock = sub_closing

        result.sort(key=lambda b: b.time_span_start)
        logger.debug(
            "disaggregate: series=%s %s→%s → %d sub-buckets",
            series_id, source_grain, target_grain, len(result),
        )
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_series_nodes(
        self,
        series_id: UUID,
        db: psycopg.Connection,
    ) -> list[Node]:
        """Load all active PI nodes for a series, ordered by bucket_sequence."""
        store = GraphStore(db)
        return store.get_nodes_by_series(series_id)

    def _enumerate_sub_buckets(
        self,
        span_start: date,
        span_end: date,
        target_grain: str,
    ) -> list[tuple[date, date]]:
        """
        Enumerate all target_grain sub-buckets that fall within [span_start, span_end).

        Returns a list of (sub_start, sub_end) tuples.
        The last sub_end is clamped to span_end to handle partial months/weeks.
        """
        sub_buckets: list[tuple[date, date]] = []
        current = _bucket_key_for_grain(span_start, target_grain)

        while current < span_end:
            sub_end = _bucket_end_for_grain(current, target_grain)
            # Clamp to the source span boundary
            effective_end = min(sub_end, span_end)
            sub_buckets.append((current, effective_end))
            current = sub_end  # advance by full target-grain bucket

        return sub_buckets
