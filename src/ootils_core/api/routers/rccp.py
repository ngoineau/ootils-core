"""
GET /v1/rccp/{resource_external_id} — Rough-Cut Capacity Planning endpoint.

Agrège la charge (load) des nœuds WorkOrderSupply / PlannedSupply connectés
à une Resource via l'edge `consumes_resource`, et la compare à la capacité
disponible de la resource (capacity_per_day × jours ouvrés du bucket).

Query params:
  from_date   : date début (YYYY-MM-DD), défaut = today
  to_date     : date fin (YYYY-MM-DD), défaut = from_date + 12 semaines
  grain       : day | week | month (défaut : week)

Réponse:
  {
    resource: { resource_id, external_id, name, resource_type, capacity_per_day, capacity_unit },
    buckets: [
      {
        period,           # date début du bucket (ISO 8601)
        period_end,       # date fin du bucket (inclusive)
        load,             # charge agrégée (somme des quantités des nœuds supply)
        capacity,         # capacité disponible du bucket
        utilization_pct,  # load / capacity * 100
        overloaded        # utilization_pct > 100
      }
    ]
  }
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/rccp", tags=["rccp"])

VALID_GRAINS = {"day", "week", "month"}


# ─────────────────────────────────────────────────────────────
# Response models
# ─────────────────────────────────────────────────────────────

class ResourceInfo(BaseModel):
    resource_id: str
    external_id: str
    name: str
    resource_type: str
    capacity_per_day: float
    capacity_unit: str
    location_id: Optional[str] = None


class RCCPBucket(BaseModel):
    period: date           # début du bucket
    period_end: date       # fin du bucket (inclusive)
    load: float            # charge agrégée
    capacity: float        # capacité disponible
    utilization_pct: float # load / capacity × 100
    overloaded: bool       # utilization_pct > 100


class RCCPResponse(BaseModel):
    resource: ResourceInfo
    buckets: list[RCCPBucket]


# ─────────────────────────────────────────────────────────────
# Helpers — bucketing
# ─────────────────────────────────────────────────────────────

def _bucket_start(d: date, grain: str) -> date:
    """Return the start of the bucket containing date d."""
    if grain == "day":
        return d
    elif grain == "week":
        # Monday of the week
        return d - timedelta(days=d.weekday())
    else:  # month
        return d.replace(day=1)


def _bucket_end(start: date, grain: str) -> date:
    """Return the last day (inclusive) of the bucket starting at start."""
    if grain == "day":
        return start
    elif grain == "week":
        return start + timedelta(days=6)
    else:  # month
        # Last day of the month
        if start.month == 12:
            return start.replace(day=31)
        return start.replace(month=start.month + 1, day=1) - timedelta(days=1)


def _next_bucket_start(start: date, grain: str) -> date:
    """Return the start of the next bucket."""
    if grain == "day":
        return start + timedelta(days=1)
    elif grain == "week":
        return start + timedelta(weeks=1)
    else:  # month
        if start.month == 12:
            return start.replace(year=start.year + 1, month=1)
        return start.replace(month=start.month + 1)


def _generate_buckets(from_date: date, to_date: date, grain: str) -> list[tuple[date, date]]:
    """
    Generate list of (bucket_start, bucket_end) pairs covering [from_date, to_date].
    """
    buckets = []
    current = _bucket_start(from_date, grain)
    while current <= to_date:
        end = _bucket_end(current, grain)
        if end > to_date:
            end = to_date
        buckets.append((current, end))
        current = _next_bucket_start(current, grain)
    return buckets


def _count_working_days(
    db: psycopg.Connection,
    location_id: Optional[str],
    start: date,
    end: date,
) -> int:
    """
    Count working days between start and end (inclusive).
    Uses operational_calendars if location_id is provided; else 5-day week fallback.
    """
    if location_id:
        row = db.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM operational_calendars
            WHERE location_id = %s::UUID
              AND calendar_date BETWEEN %s AND %s
              AND is_working_day = TRUE
            """,
            (location_id, start, end),
        ).fetchone()
        if row and row["cnt"] > 0:
            return int(row["cnt"])

    # Fallback: count Mon–Fri in [start, end]
    count = 0
    d = start
    while d <= end:
        if d.weekday() < 5:  # 0=Mon … 4=Fri
            count += 1
        d += timedelta(days=1)
    return count


# ─────────────────────────────────────────────────────────────
# GET /v1/rccp/{resource_external_id}
# ─────────────────────────────────────────────────────────────

@router.get(
    "/{resource_external_id}",
    response_model=RCCPResponse,
    summary="RCCP — Rough-Cut Capacity Planning",
    description=(
        "Agrège la charge des nœuds WorkOrderSupply / PlannedSupply connectés à la resource "
        "et la compare à la capacité disponible par bucket (day / week / month)."
    ),
)
async def get_rccp(
    resource_external_id: str,
    from_date: date = Query(default=None, description="Début de l'horizon (YYYY-MM-DD). Défaut : today."),
    to_date: date = Query(default=None, description="Fin de l'horizon (YYYY-MM-DD). Défaut : from_date + 84 jours."),
    grain: str = Query(default="week", description="Granularité : day | week | month."),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> RCCPResponse:
    """RCCP endpoint — charge vs capacité par bucket."""

    # Validate grain
    if grain not in VALID_GRAINS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"grain '{grain}' invalid; valid: {sorted(VALID_GRAINS)}",
        )

    # Default dates
    today = date.today()
    if from_date is None:
        from_date = today
    if to_date is None:
        to_date = from_date + timedelta(weeks=12)

    if to_date < from_date:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="to_date must be >= from_date",
        )

    # Fetch resource
    resource_row = db.execute(
        """
        SELECT resource_id, external_id, name, resource_type,
               capacity_per_day, capacity_unit, location_id
        FROM resources
        WHERE external_id = %s
        """,
        (resource_external_id,),
    ).fetchone()

    if resource_row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Resource '{resource_external_id}' not found",
        )

    resource_id = str(resource_row["resource_id"])
    location_id = str(resource_row["location_id"]) if resource_row["location_id"] else None
    capacity_per_day = float(resource_row["capacity_per_day"])

    resource_info = ResourceInfo(
        resource_id=resource_id,
        external_id=resource_row["external_id"],
        name=resource_row["name"],
        resource_type=resource_row["resource_type"],
        capacity_per_day=capacity_per_day,
        capacity_unit=resource_row["capacity_unit"],
        location_id=location_id,
    )

    # Fetch load: aggregate quantities from supply nodes connected to this resource
    # via edges of type 'consumes_resource' where the edge goes FROM a supply node TO the resource
    # The resource is stored in the `resources` table (not `nodes`), so we need to
    # join on the external_id stored in nodes or use a dedicated approach.
    #
    # Architecture: supply nodes (WorkOrderSupply, PlannedSupply) in `nodes` table
    # connect to the resource via `edges` where:
    #   - from_node_id = supply node_id
    #   - to_node_id = resource's node_id (if Resource is in the nodes table)
    #   - OR we use a dedicated approach using resource_id directly in edges
    #
    # Since Resource is a first-class entity (not in nodes table), we store resource_id
    # in edges.metadata or use the resource_id via a join approach.
    # Simpler: query edges joining from supply node, using a resource node if it exists,
    # or query via a dedicated consumes_resource junction.
    #
    # For ADR-010 V1: Resources are entities in the `resources` table.
    # Edges `consumes_resource` use nodes table entries for supply nodes,
    # and for the resource side we store the resource_id in edge metadata.
    # However, to stay consistent with the existing edge model (from_node_id → to_node_id),
    # we'll query: find supply nodes that reference this resource via edges.
    # The resource link is stored using resource_id in a to_resource_id column or via
    # a special node_type = 'Resource' in the nodes table.
    #
    # V1 Decision: We use a separate `consumes_resource` table OR
    # we look for nodes of type 'WorkOrderSupply'/'PlannedSupply' that have
    # edges to a special node representing the resource.
    #
    # Simplest consistent approach: store resource_id in nodes.resource_id for
    # WorkOrderSupply/PlannedSupply nodes, OR use the edge table with to_node_id pointing
    # to a Resource node in the nodes table.
    #
    # For V1, we'll use the edges table with a resource_node approach:
    # find all supply nodes connected to this resource (by resource_id stored in
    # a resource_node that maps to this resource).

    # Query: load = SUM of quantities for supply nodes connected to this resource
    # within the date range, aggregated by time_ref (supply date)
    load_rows = db.execute(
        """
        SELECT
            n.time_ref,
            COALESCE(n.quantity, 0) AS quantity
        FROM nodes n
        JOIN edges e ON e.from_node_id = n.node_id
        JOIN nodes rn ON rn.node_id = e.to_node_id
        WHERE n.node_type IN ('WorkOrderSupply', 'PlannedSupply')
          AND e.edge_type = 'consumes_resource'
          AND e.active = TRUE
          AND n.active = TRUE
          AND rn.node_type = 'Resource'
          AND rn.external_id = %s
          AND n.time_ref BETWEEN %s AND %s
        """,
        (resource_external_id, from_date, to_date),
    ).fetchall()

    # Build load by date
    load_by_date: dict[date, float] = {}
    for row in load_rows:
        d = row["time_ref"]
        if d is not None:
            load_by_date[d] = load_by_date.get(d, 0.0) + float(row["quantity"])

    # Fetch capacity overrides for the period
    override_rows = db.execute(
        """
        SELECT override_date, capacity
        FROM resource_capacity_overrides
        WHERE resource_id = %s::UUID
          AND override_date BETWEEN %s AND %s
        """,
        (resource_id, from_date, to_date),
    ).fetchall()
    capacity_overrides: dict[date, float] = {
        row["override_date"]: float(row["capacity"])
        for row in override_rows
    }

    # Generate buckets and compute RCCP
    buckets = []
    for bucket_start, bucket_end in _generate_buckets(from_date, to_date, grain):
        # Aggregate load for this bucket
        bucket_load = 0.0
        d = bucket_start
        while d <= bucket_end:
            bucket_load += load_by_date.get(d, 0.0)
            d += timedelta(days=1)

        # Capacity: sum per-day capacity within the bucket
        bucket_capacity = 0.0
        d = bucket_start
        while d <= bucket_end:
            if d in capacity_overrides:
                bucket_capacity += capacity_overrides[d]
            elif d.weekday() < 5:  # working day (Mon–Fri fallback)
                # Check operational_calendars
                cal_row = None
                if location_id:
                    cal_row = db.execute(
                        """
                        SELECT is_working_day, capacity_factor
                        FROM operational_calendars
                        WHERE location_id = %s::UUID AND calendar_date = %s
                        """,
                        (location_id, d),
                    ).fetchone()

                if cal_row is not None:
                    if cal_row["is_working_day"]:
                        bucket_capacity += capacity_per_day * float(cal_row["capacity_factor"] or 1.0)
                else:
                    # No calendar entry — use Mon–Fri heuristic
                    if d.weekday() < 5:
                        bucket_capacity += capacity_per_day
            d += timedelta(days=1)

        utilization_pct = (bucket_load / bucket_capacity * 100.0) if bucket_capacity > 0 else 0.0
        overloaded = utilization_pct > 100.0

        buckets.append(
            RCCPBucket(
                period=bucket_start,
                period_end=bucket_end,
                load=round(bucket_load, 4),
                capacity=round(bucket_capacity, 4),
                utilization_pct=round(utilization_pct, 2),
                overloaded=overloaded,
            )
        )

    logger.info(
        "rccp.get resource=%s from=%s to=%s grain=%s buckets=%d",
        resource_external_id, from_date, to_date, grain, len(buckets),
    )

    return RCCPResponse(resource=resource_info, buckets=buckets)
