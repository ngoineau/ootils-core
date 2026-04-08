"""
capacity_aggregate.py — Ghost capacity_aggregate engine.

Aggregates supply load (WorkOrderSupply + PlannedSupply) from all member items,
compares against the linked resource's capacity_per_day.

Alert emitted: capacity_overload
  when aggregated load > resource capacity for a given day.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import psycopg


def run_capacity_aggregate(
    db: psycopg.Connection,
    ghost_id: str,
    scenario_id: str,
    from_date: date,
    to_date: date,
) -> dict[str, Any]:
    """
    Evaluate a capacity_aggregate ghost over [from_date, to_date].

    Returns:
      {
        "ghost_id": ...,
        "ghost_type": "capacity_aggregate",
        "alerts": [{"type": "capacity_overload", "date": ..., "load": ..., "capacity": ..., "slack": ...}],
        "summary": {
          "from_date": ..., "to_date": ...,
          "resource_id": ...,
          "capacity_per_day": ...,
          "periods": [{"date": ..., "load_total": ..., "capacity": ..., "slack": ..., "overloaded": bool, "member_breakdown": [...]}]
        }
      }
    """
    # Load ghost
    ghost = db.execute(
        "SELECT ghost_id, ghost_type, resource_id FROM ghost_nodes WHERE ghost_id = %s",
        (ghost_id,),
    ).fetchone()
    if ghost is None:
        raise ValueError(f"Ghost {ghost_id} not found")
    if ghost["ghost_type"] != "capacity_aggregate":
        raise ValueError(f"Ghost {ghost_id} is not a capacity_aggregate ghost")

    # Load resource capacity
    resource_id = ghost["resource_id"]
    capacity_per_day = _get_resource_capacity(db, str(resource_id)) if resource_id else 0.0

    # Load members
    members = db.execute(
        "SELECT item_id FROM ghost_members WHERE ghost_id = %s AND role = 'member'",
        (ghost_id,),
    ).fetchall()

    if not members:
        raise ValueError(f"Ghost {ghost_id} has no members")

    item_ids = [str(m["item_id"]) for m in members]

    alerts = []
    periods = []

    # Day-by-day load aggregation
    current = from_date
    while current <= to_date:
        member_loads = []
        for item_id in item_ids:
            load = _get_supply_load(db, item_id, scenario_id, current)
            member_loads.append({"item_id": item_id, "load": load})

        load_total = sum(ml["load"] for ml in member_loads)
        slack = capacity_per_day - load_total
        overloaded = slack < 0

        period = {
            "date": current.isoformat(),
            "load_total": round(load_total, 4),
            "capacity": capacity_per_day,
            "slack": round(slack, 4),
            "overloaded": overloaded,
            "member_breakdown": member_loads,
        }
        periods.append(period)

        if overloaded:
            alerts.append({
                "type": "capacity_overload",
                "date": current.isoformat(),
                "load": round(load_total, 4),
                "capacity": capacity_per_day,
                "slack": round(slack, 4),
            })

        current += timedelta(days=1)

    return {
        "ghost_id": ghost_id,
        "ghost_type": "capacity_aggregate",
        "alerts": alerts,
        "summary": {
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
            "resource_id": str(resource_id) if resource_id else None,
            "capacity_per_day": capacity_per_day,
            "periods": periods,
        },
    }


def _get_resource_capacity(db: psycopg.Connection, resource_id: str) -> float:
    """Return capacity_per_day for a resource."""
    row = db.execute(
        "SELECT capacity_per_day FROM resources WHERE resource_id = %s",
        (resource_id,),
    ).fetchone()
    return float(row["capacity_per_day"]) if row else 0.0


def _get_supply_load(
    db: psycopg.Connection,
    item_id: str,
    scenario_id: str,
    ref_date: date,
) -> float:
    """
    Sum quantity of WorkOrderSupply + PlannedSupply nodes for an item/scenario on a specific date.
    """
    row = db.execute(
        """
        SELECT COALESCE(SUM(quantity), 0) AS load_qty
        FROM nodes
        WHERE node_type IN ('WorkOrderSupply', 'PlannedSupply')
          AND item_id = %s
          AND scenario_id = %s
          AND active = TRUE
          AND time_ref = %s
        """,
        (item_id, scenario_id, ref_date),
    ).fetchone()
    return float(row["load_qty"]) if row else 0.0
