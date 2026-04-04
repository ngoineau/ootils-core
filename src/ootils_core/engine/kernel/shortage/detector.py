"""
detector.py — ShortageDetector for Sprint M4.

Detects shortages from ProjectedInventory nodes (closing_stock < 0),
scores their severity, persists them, and manages lifecycle (resolve stale).

This module is the exclusive owner of the `shortages` table and may use
direct SQL on it.  All other graph data access goes through GraphStore.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

from ootils_core.models import Node, ShortageRecord

logger = logging.getLogger(__name__)

# Unit cost proxy for PoC — will be replaced with actual item cost in future milestones.
_UNIT_COST_PROXY = Decimal("1")


class ShortageDetector:
    """
    Detects, scores, persists, and resolves inventory shortages.

    Owns the `shortages` table — uses direct SQL.
    """

    # ------------------------------------------------------------------
    # Detection
    # ------------------------------------------------------------------

    def detect(
        self,
        pi_node: Node,
        calc_run_id: UUID,
        scenario_id: UUID,
        db,
    ) -> Optional[ShortageRecord]:
        """
        Inspect a PI node and return a ShortageRecord if closing_stock < 0.

        Returns None if no shortage exists.

        severity_score = shortage_qty × days_in_bucket × unit_cost_proxy
        days_in_bucket = (time_span_end - time_span_start).days if available, else 1
        """
        closing = pi_node.closing_stock
        if closing is None or closing >= Decimal("0"):
            return None

        shortage_qty = abs(closing)

        # Bucket duration
        if pi_node.time_span_start is not None and pi_node.time_span_end is not None:
            days_in_bucket = (pi_node.time_span_end - pi_node.time_span_start).days
            if days_in_bucket <= 0:
                days_in_bucket = 1
        else:
            days_in_bucket = 1

        severity_score = shortage_qty * Decimal(str(days_in_bucket)) * _UNIT_COST_PROXY

        shortage_date = pi_node.time_span_start or pi_node.time_ref

        record = ShortageRecord(
            shortage_id=uuid4(),
            scenario_id=scenario_id,
            pi_node_id=pi_node.node_id,
            item_id=pi_node.item_id,
            location_id=pi_node.location_id,
            shortage_date=shortage_date,
            shortage_qty=shortage_qty,
            severity_score=severity_score,
            explanation_id=None,  # linked post-build by ExplanationBuilder if needed
            calc_run_id=calc_run_id,
            status="active",
        )

        logger.debug(
            "Shortage detected on node %s — qty=%s severity=%s",
            pi_node.node_id,
            shortage_qty,
            severity_score,
        )
        return record

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def persist(self, shortage: ShortageRecord, db) -> None:
        """
        Upsert a ShortageRecord into the `shortages` table.
        ON CONFLICT (pi_node_id, calc_run_id) → update all mutable fields.
        """
        now = datetime.now(timezone.utc)
        db.execute(
            """
            INSERT INTO shortages (
                shortage_id,
                scenario_id,
                pi_node_id,
                item_id,
                location_id,
                shortage_date,
                shortage_qty,
                severity_score,
                explanation_id,
                calc_run_id,
                status,
                created_at,
                updated_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (pi_node_id, calc_run_id) DO UPDATE SET
                shortage_qty    = EXCLUDED.shortage_qty,
                severity_score  = EXCLUDED.severity_score,
                shortage_date   = EXCLUDED.shortage_date,
                explanation_id  = EXCLUDED.explanation_id,
                status          = EXCLUDED.status,
                updated_at      = EXCLUDED.updated_at
            """,
            (
                shortage.shortage_id,
                shortage.scenario_id,
                shortage.pi_node_id,
                shortage.item_id,
                shortage.location_id,
                shortage.shortage_date,
                shortage.shortage_qty,
                shortage.severity_score,
                shortage.explanation_id,
                shortage.calc_run_id,
                shortage.status,
                shortage.created_at,
                now,
            ),
        )
        shortage.updated_at = now
        logger.debug("Shortage persisted: %s", shortage.shortage_id)

    # ------------------------------------------------------------------
    # Lifecycle management
    # ------------------------------------------------------------------

    def resolve_stale(
        self,
        scenario_id: UUID,
        calc_run_id: UUID,
        db,
    ) -> int:
        """
        Mark as 'resolved' all active shortages for this scenario that were
        NOT generated (or refreshed) in the current calc_run_id.

        Returns the count of rows updated.
        """
        now = datetime.now(timezone.utc)
        result = db.execute(
            """
            UPDATE shortages
            SET status     = 'resolved',
                updated_at = %s
            WHERE scenario_id  = %s
              AND status        = 'active'
              AND calc_run_id  != %s
            """,
            (now, scenario_id, calc_run_id),
        )
        count = result.rowcount if hasattr(result, "rowcount") else 0
        logger.info(
            "resolve_stale: %d shortages resolved for scenario %s (calc_run %s)",
            count,
            scenario_id,
            calc_run_id,
        )
        return count

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_active_shortages(
        self,
        scenario_id: UUID,
        db,
    ) -> list[ShortageRecord]:
        """
        Return all active shortages for a scenario, sorted by severity_score DESC.
        """
        rows = db.execute(
            """
            SELECT
                shortage_id,
                scenario_id,
                pi_node_id,
                item_id,
                location_id,
                shortage_date,
                shortage_qty,
                severity_score,
                explanation_id,
                calc_run_id,
                status,
                created_at,
                updated_at
            FROM shortages
            WHERE scenario_id = %s
              AND status = 'active'
            ORDER BY severity_score DESC
            """,
            (scenario_id,),
        ).fetchall()

        return [_row_to_shortage(r) for r in rows]


# ------------------------------------------------------------------
# Row → domain model helper
# ------------------------------------------------------------------


def _row_to_shortage(row) -> ShortageRecord:
    """Convert a DB row (dict or dict-like) to a ShortageRecord."""
    return ShortageRecord(
        shortage_id=UUID(str(row["shortage_id"])),
        scenario_id=UUID(str(row["scenario_id"])),
        pi_node_id=UUID(str(row["pi_node_id"])),
        item_id=UUID(str(row["item_id"])) if row.get("item_id") else None,
        location_id=UUID(str(row["location_id"])) if row.get("location_id") else None,
        shortage_date=row["shortage_date"],
        shortage_qty=Decimal(str(row["shortage_qty"])),
        severity_score=Decimal(str(row["severity_score"])),
        explanation_id=UUID(str(row["explanation_id"])) if row.get("explanation_id") else None,
        calc_run_id=UUID(str(row["calc_run_id"])),
        status=row["status"],
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )
