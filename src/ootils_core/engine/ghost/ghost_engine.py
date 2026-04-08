"""
ghost_engine.py — Ghost dispatcher.

run_ghost(db, ghost_id, scenario_id, from_date, to_date)
  → dispatches to phase_transition or capacity_aggregate engine based on ghost_type.
"""
from __future__ import annotations

from datetime import date
from typing import Any

import psycopg

from .phase_transition import run_phase_transition
from .capacity_aggregate import run_capacity_aggregate


def run_ghost(
    db: psycopg.Connection,
    ghost_id: str,
    scenario_id: str,
    from_date: date,
    to_date: date,
) -> dict[str, Any]:
    """
    Dispatcher: load ghost_type and delegate to the appropriate engine.

    Raises:
      ValueError if ghost not found or ghost_type unknown.
    """
    row = db.execute(
        "SELECT ghost_type FROM ghost_nodes WHERE ghost_id = %s",
        (ghost_id,),
    ).fetchone()

    if row is None:
        raise ValueError(f"Ghost {ghost_id} not found")

    ghost_type = row["ghost_type"]

    if ghost_type == "phase_transition":
        return run_phase_transition(db, ghost_id, scenario_id, from_date, to_date)
    elif ghost_type == "capacity_aggregate":
        return run_capacity_aggregate(db, ghost_id, scenario_id, from_date, to_date)
    else:
        raise ValueError(f"Unknown ghost_type: {ghost_type}")
