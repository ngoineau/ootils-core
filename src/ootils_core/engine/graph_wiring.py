"""
graph_wiring.py — shared graph-wiring helpers (projection-series bootstrap +
supply/demand -> ProjectedInventory edge wiring), extracted from
``api/routers/ingest.py`` (DESC-1 PR-B, #477).

WHY THIS MODULE EXISTS: every writer that materializes a supply/demand node
onto (item, location, scenario) needs the SAME two steps — (1) make sure a
``projection_series`` + its 90 daily ``ProjectedInventory`` PI buckets (plus
their ``feeds_forward`` chain) exist for that coordinate, and (2) wire the
new node to the PI bucket(s) it overlaps via a ``replenishes``/``consumes``
edge. Until DESC-1, ``api/routers/ingest.py`` was the ONLY caller and owned
both functions directly. The demand-descent run
(``engine/descent/run.py``) needs the IDENTICAL wiring for its derived
per-DC demand nodes — copy-pasting the logic would create a second writer of
the exact same graph-structural invariant (PI bucket existence, edge
idempotence) that could silently drift from the ingest path over time. This
module is therefore the SINGLE implementation; ``api/routers/ingest.py``
re-exports both names unchanged (``from ootils_core.engine.graph_wiring
import ensure_projection_series as _ensure_projection_series`` etc.) so
every existing caller/test keeps working byte-for-byte — behaviour is
IDENTICAL, only the file that owns the code moved.

Not part of ``engine/kernel/`` (the "only GraphStore touches the DB" rule is
scoped to that subpackage — see ``engine/kernel/graph/store.py``'s own
docstring). This module sits alongside ``engine/mrp/loader.py``,
``engine/drp/loader.py``, ``engine/snapshot/capture.py`` — ordinary engine
modules that own their own SQL outside the kernel boundary.

Both functions never commit — the caller (``get_db`` in a router, or the
descent run's own caller) owns the transaction.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional
from uuid import UUID, uuid4

from ootils_core.db.types import DictRowConnection

logger = logging.getLogger(__name__)


def ensure_projection_series(
    db: DictRowConnection,
    item_id: UUID,
    location_id: UUID,
    scenario_id: UUID,
) -> bool:
    """
    Ensure a ProjectionSeries + PI bucket nodes exist for (item, location, scenario).
    Creates them if missing. Returns True if created, False if already existed.

    Also chains the 90 buckets with 'feeds_forward' edges (bucket[i] -> bucket[i+1],
    weight_ratio=1.0), mirroring migration 019's backfill. Without this chain,
    GraphTraversal.expand_dirty_subgraph's downstream BFS (traversal.py) has no
    edge to walk past the single PI bucket a supply/demand node is wired to, so
    incremental propagation (POST /v1/events) only ever recomputes that one
    bucket and never cascades the closing_stock change to the rest of the
    horizon — silently stale projections outside a full recompute (found via
    the 2026-07-17 ingest lifecycle retraction test, which is the first test
    to drive the incremental path in isolation; every other caller in this
    file's test suite masks the gap with a full recompute, which recomputes
    every PI node directly and never needs the edges). This bug predates and
    is independent of the retraction fix: it affects every ProjectionSeries
    created through this ingest path (never through the demo seed, which
    migration 019 already backfilled).
    """
    existing = db.execute(
        """
        SELECT series_id FROM projection_series
        WHERE item_id = %s AND location_id = %s AND scenario_id = %s
        """,
        (item_id, location_id, scenario_id),
    ).fetchone()

    if existing:
        return False

    series_id = uuid4()
    today = date.today()
    horizon_start = today
    horizon_end = today + timedelta(days=90)

    db.execute(
        """
        INSERT INTO projection_series (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, now(), now())
        ON CONFLICT (item_id, location_id, scenario_id) DO NOTHING
        """,
        (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end),
    )

    row = db.execute(
        "SELECT series_id FROM projection_series WHERE item_id = %s AND location_id = %s AND scenario_id = %s",
        (item_id, location_id, scenario_id),
    ).fetchone()
    actual_series_id = UUID(str(row["series_id"])) if row else series_id

    bucket_node_ids: list[UUID] = []
    bucket_spans: list[tuple[date, date]] = []
    for i in range(90):
        day_start = today + timedelta(days=i)
        day_end = day_start + timedelta(days=1)
        node_id = uuid4()
        bucket_node_ids.append(node_id)
        bucket_spans.append((day_start, day_end))
        db.execute(
            """
            INSERT INTO nodes (
                node_id, node_type, scenario_id, item_id, location_id,
                time_grain, time_span_start, time_span_end, time_ref,
                projection_series_id, bucket_sequence,
                opening_stock, inflows, outflows, closing_stock,
                has_shortage, shortage_qty, is_dirty, active,
                created_at, updated_at
            ) VALUES (
                %s, 'ProjectedInventory', %s, %s, %s,
                'day', %s, %s, %s,
                %s, %s,
                0, 0, 0, 0,
                FALSE, 0, TRUE, TRUE,
                now(), now()
            )
            ON CONFLICT DO NOTHING
            """,
            (
                node_id, scenario_id, item_id, location_id,
                day_start, day_end, day_start,
                actual_series_id, i,
            ),
        )

    # Chain consecutive buckets: PI[i].closing_stock -> PI[i+1].opening_stock
    # (same contract as migration 019's backfill INSERT).
    for i in range(len(bucket_node_ids) - 1):
        from_id = bucket_node_ids[i]
        to_id = bucket_node_ids[i + 1]
        # Mirrors migration 019's backfill exactly: effective_start =
        # n1.time_span_start, effective_end = n2.time_span_end.
        effective_start, _ = bucket_spans[i]
        _, effective_end = bucket_spans[i + 1]
        db.execute(
            """
            INSERT INTO edges (
                edge_id, edge_type, from_node_id, to_node_id, scenario_id,
                priority, weight_ratio, effective_start, effective_end,
                active, created_at
            ) VALUES (
                %s, 'feeds_forward', %s, %s, %s,
                0, 1.0, %s, %s,
                TRUE, now()
            )
            ON CONFLICT DO NOTHING
            """,
            (uuid4(), from_id, to_id, scenario_id, effective_start, effective_end),
        )

    logger.info(
        "ensure_projection_series: created series + 90 PI buckets + 89 feeds_forward edges for item=%s loc=%s",
        item_id, location_id,
    )
    return True


def wire_node_to_pi(
    db: DictRowConnection,
    node_id: UUID,
    node_type: str,
    item_id: UUID,
    location_id: UUID,
    scenario_id: UUID,
    time_ref: date,
    time_span_start: Optional[date] = None,
    time_span_end: Optional[date] = None,
) -> int:
    """
    Connect a supply/demand node to the matching PI bucket(s) via edge(s).

    Point-in-time nodes (the default — `time_span_start`/`time_span_end`
    both None, the only mode used by supplies and exact_date/timeless
    demand) wire to AT MOST ONE PI bucket: the daily bucket containing
    `time_ref`. A periodic forecast (`time_span_start`/`time_span_end` both
    set — day/week/month time_grain) wires to EVERY daily PI bucket it
    overlaps `[time_span_start, time_span_end)`, so the proration CASE
    already computed in propagator_sql.py/propagator.py has more than one
    bucket to distribute across (replacing the historical single-day
    lumping of a periodic forecast).

    Idempotent per (node_id, edge_type, scenario_id): the full desired PI
    target set is (re)computed on every call. An edge already pointing at a
    desired target is left untouched (no PK churn against
    `inflows_agg`/`outflows_agg`, which SUM(quantity) once PER MATCHING
    EDGE — see the historical duplicate-edge bug fixed 2026-07-16). Edges
    pointing outside the desired set (stale target, or a duplicate to an
    already-kept target) are either retargeted onto a still-missing target
    (oldest first, preserving edge identity) or deleted. `edges` carries no
    unique constraint (only the `edge_id` PK, always a fresh uuid4() here),
    hence this retarget/delete scheme instead of `ON CONFLICT`.

    Returns the number of edges created or retargeted (0 if no PI bucket
    overlaps, or every target was already correctly wired).
    """
    if node_type in ("PurchaseOrderSupply", "WorkOrderSupply", "PlannedSupply", "TransferSupply", "OnHandSupply"):
        edge_type = "replenishes"
    elif node_type in ("ForecastDemand", "CustomerOrderDemand"):
        edge_type = "consumes"
    else:
        return 0

    if time_span_start is not None and time_span_end is not None:
        pi_rows = db.execute(
            """
            SELECT node_id FROM nodes
            WHERE node_type = 'ProjectedInventory'
              AND item_id = %s
              AND location_id = %s
              AND scenario_id = %s
              AND active = TRUE
              AND time_span_start < %s
              AND time_span_end > %s
            ORDER BY time_span_start ASC
            """,
            (item_id, location_id, scenario_id, time_span_end, time_span_start),
        ).fetchall()
    else:
        pi_rows = db.execute(
            """
            SELECT node_id FROM nodes
            WHERE node_type = 'ProjectedInventory'
              AND item_id = %s
              AND location_id = %s
              AND scenario_id = %s
              AND active = TRUE
              AND time_span_start <= %s
              AND time_span_end > %s
            ORDER BY time_span_start ASC
            LIMIT 1
            """,
            (item_id, location_id, scenario_id, time_ref, time_ref),
        ).fetchall()

    if not pi_rows:
        logger.debug(
            "wire_node_to_pi: no PI bucket found for item=%s loc=%s date=%s span=(%s,%s)",
            item_id, location_id, time_ref, time_span_start, time_span_end,
        )
        return 0

    # Order preserved from the query (time_span_start ASC); node_id is a PK
    # so no duplicates can appear across rows.
    target_ids: list[UUID] = [row["node_id"] for row in pi_rows]
    target_id_set = set(target_ids)

    existing_edges = db.execute(
        """
        SELECT edge_id, to_node_id FROM edges
        WHERE from_node_id = %s AND edge_type = %s AND scenario_id = %s
        ORDER BY created_at ASC
        """,
        (node_id, edge_type, scenario_id),
    ).fetchall()

    kept_by_target: dict[UUID, UUID] = {}
    reusable_edge_ids: list[UUID] = []  # oldest first — not (yet) pointing at a desired target
    duplicate_edge_ids: list[UUID] = []  # extra edge(s) already pointing at a kept target

    for e in existing_edges:
        target = e["to_node_id"]
        edge_id = e["edge_id"]
        if target in target_id_set and target not in kept_by_target:
            kept_by_target[target] = edge_id
        elif target in target_id_set:
            duplicate_edge_ids.append(edge_id)
        else:
            reusable_edge_ids.append(edge_id)

    wired = 0
    reuse_idx = 0
    for target in target_ids:
        if target in kept_by_target:
            continue  # already correctly wired — nothing to create or retarget
        if reuse_idx < len(reusable_edge_ids):
            # Retarget the oldest available stale edge instead of leaving it
            # dangling and inserting a parallel one.
            edge_id = reusable_edge_ids[reuse_idx]
            reuse_idx += 1
            db.execute(
                "UPDATE edges SET to_node_id = %s, active = TRUE WHERE edge_id = %s",
                (target, edge_id),
            )
        else:
            db.execute(
                """
                INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active, created_at)
                VALUES (%s, %s, %s, %s, %s, TRUE, now())
                """,
                (uuid4(), edge_type, node_id, target, scenario_id),
            )
        wired += 1

    stale_ids = reusable_edge_ids[reuse_idx:] + duplicate_edge_ids
    if stale_ids:
        db.execute("DELETE FROM edges WHERE edge_id = ANY(%s)", (stale_ids,))
        logger.info(
            "wire_node_to_pi: removed %d duplicate/stale %s edge(s) from node=%s",
            len(stale_ids), edge_type, node_id,
        )

    logger.debug(
        "wire_node_to_pi: wired node=%s (%s) -> %d PI bucket(s) via %s",
        node_id, node_type, len(target_ids), edge_type,
    )
    return wired
