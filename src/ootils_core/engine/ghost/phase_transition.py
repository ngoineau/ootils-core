"""
phase_transition.py — Ghost phase_transition engine.

Computes weight_A(t) / weight_B(t) dynamically based on the transition curve
and checks supply coherence between outgoing and incoming members.

Supported curves:
  linear  — linear interpolation between weight_at_start and weight_at_end
  step    — stays at weight_at_start until transition_end_date, then weight_at_end
  sigmoid — Hermite smoothstep (3t² - 2t³)

Alert emitted: transition_inconsistency
  when |ProjectedInventory(A) + ProjectedInventory(B) - baseline| > 10% of baseline
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import psycopg


# ---------------------------------------------------------------------------
# Weight calculation (O(1) per date — ADR-010 D3)
# ---------------------------------------------------------------------------

def compute_weight(
    t: date,
    transition_start_date: date | None,
    transition_end_date: date | None,
    transition_curve: str,
    weight_at_start: float,
    weight_at_end: float,
) -> float:
    """
    Return weight for a member at date t given transition parameters.

    Outside the transition window:
      t <= start → weight_at_start
      t >= end   → weight_at_end
    """
    if transition_start_date is None or transition_end_date is None:
        return weight_at_start

    if t <= transition_start_date:
        return weight_at_start
    if t >= transition_end_date:
        return weight_at_end

    # Inside the transition window
    total_days = (transition_end_date - transition_start_date).days
    if total_days <= 0:
        return weight_at_end

    ratio = (t - transition_start_date).days / total_days  # [0, 1)

    if transition_curve == "linear":
        return weight_at_start + ratio * (weight_at_end - weight_at_start)
    elif transition_curve == "step":
        # Stay at weight_at_start until end_date, then flip
        return weight_at_start
    elif transition_curve == "sigmoid":
        # Hermite smoothstep: 3r² - 2r³
        smooth = 3 * ratio**2 - 2 * ratio**3
        return weight_at_start + smooth * (weight_at_end - weight_at_start)
    else:
        # fallback: linear
        return weight_at_start + ratio * (weight_at_end - weight_at_start)


# ---------------------------------------------------------------------------
# Phase transition engine
# ---------------------------------------------------------------------------

INCONSISTENCY_THRESHOLD = 0.10  # 10% deviation triggers alert


def run_phase_transition(
    db: psycopg.Connection,
    ghost_id: str,
    scenario_id: str,
    from_date: date,
    to_date: date,
) -> dict[str, Any]:
    """
    Evaluate a phase_transition ghost over [from_date, to_date].

    Returns:
      {
        "ghost_id": ...,
        "ghost_type": "phase_transition",
        "alerts": [{"type": "transition_inconsistency", "date": ..., "delta": ..., "delta_pct": ...}],
        "summary": {
          "from_date": ..., "to_date": ...,
          "outgoing_item_id": ..., "incoming_item_id": ...,
          "transition_curve": ...,
          "weight_samples": [{"date": ..., "weight_outgoing": ..., "weight_incoming": ...}]
        }
      }
    """
    # Load ghost
    ghost = db.execute(
        "SELECT ghost_id, ghost_type, scenario_id FROM ghost_nodes WHERE ghost_id = %s",
        (ghost_id,),
    ).fetchone()
    if ghost is None:
        raise ValueError(f"Ghost {ghost_id} not found")
    if ghost["ghost_type"] != "phase_transition":
        raise ValueError(f"Ghost {ghost_id} is not a phase_transition ghost")

    # Load members
    members = db.execute(
        """
        SELECT member_id, item_id, role,
               transition_start_date, transition_end_date,
               transition_curve, weight_at_start, weight_at_end
        FROM ghost_members
        WHERE ghost_id = %s
        ORDER BY role
        """,
        (ghost_id,),
    ).fetchall()

    outgoing = next((m for m in members if m["role"] == "outgoing"), None)
    incoming = next((m for m in members if m["role"] == "incoming"), None)

    if outgoing is None or incoming is None:
        raise ValueError(f"Ghost {ghost_id} missing outgoing or incoming member")

    alerts = []
    weight_samples = []

    # Iterate day by day
    current = from_date
    while current <= to_date:
        w_out = compute_weight(
            current,
            outgoing["transition_start_date"],
            outgoing["transition_end_date"],
            outgoing["transition_curve"],
            float(outgoing["weight_at_start"]),
            float(outgoing["weight_at_end"]),
        )
        w_in = 1.0 - w_out

        weight_samples.append({
            "date": current.isoformat(),
            "weight_outgoing": round(w_out, 4),
            "weight_incoming": round(w_in, 4),
        })

        # Check projected inventory coherence
        proj_a = _get_projected_inventory(db, str(outgoing["item_id"]), scenario_id, current)
        proj_b = _get_projected_inventory(db, str(incoming["item_id"]), scenario_id, current)

        if proj_a is not None and proj_b is not None:
            observed = proj_a + proj_b
            # Baseline: proj_a / w_out gives the "full" volume A would have had alone
            if w_out > 0:
                baseline = proj_a / w_out
            elif proj_b > 0:
                baseline = proj_b
            else:
                baseline = 0.0

            if baseline > 0:
                delta = observed - baseline
                delta_pct = abs(delta) / baseline
                if delta_pct > INCONSISTENCY_THRESHOLD:
                    alerts.append({
                        "type": "transition_inconsistency",
                        "date": current.isoformat(),
                        "projected_a": proj_a,
                        "projected_b": proj_b,
                        "observed": observed,
                        "baseline": round(baseline, 4),
                        "delta": round(delta, 4),
                        "delta_pct": round(delta_pct, 4),
                    })

        current += timedelta(days=1)

    return {
        "ghost_id": ghost_id,
        "ghost_type": "phase_transition",
        "alerts": alerts,
        "summary": {
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
            "outgoing_item_id": str(outgoing["item_id"]),
            "incoming_item_id": str(incoming["item_id"]),
            "transition_curve": outgoing["transition_curve"],
            "weight_samples": weight_samples,
        },
    }


def _get_projected_inventory(
    db: psycopg.Connection,
    item_id: str,
    scenario_id: str,
    ref_date: date,
) -> float | None:
    """
    Get the most recent ProjectedInventory node quantity for an item/scenario on or before ref_date.
    Returns None if no projection found.
    """
    row = db.execute(
        """
        SELECT quantity FROM nodes
        WHERE node_type = 'ProjectedInventory'
          AND item_id = %s
          AND scenario_id = %s
          AND active = TRUE
          AND time_ref <= %s
        ORDER BY time_ref DESC
        LIMIT 1
        """,
        (item_id, scenario_id, ref_date),
    ).fetchone()
    return float(row["quantity"]) if row else None
