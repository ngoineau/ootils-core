"""
impact_scorer.py — Catégorie 4 : impact supply chain.

Pour chaque issue (L1/L2/stat/temporal) sur un item :
  1. Traverser le graph pour trouver les shortages actifs liés à l'item
  2. Remonter la BOM pour trouver les produits finis impactés
  3. Calculer impact_score = severity_weight × (1 + log(1 + active_shortages_count))
  4. Enrichir chaque issue avec impact_score, affected_items, active_shortages_count
"""
from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from uuid import UUID

import psycopg

from .stat_rules import AgentIssue

logger = logging.getLogger(__name__)

# Severity weights
SEVERITY_WEIGHTS = {
    "error": 3.0,
    "warning": 1.5,
    "info": 0.5,
}


@dataclass
class IssueImpact:
    issue_id: UUID
    impact_score: float
    affected_items: list[str]
    active_shortages_count: int


def _get_item_ids_for_issue(
    db: psycopg.Connection,
    batch_id: UUID,
    row_id: UUID | None,
) -> list[str]:
    """
    Extract item external_ids relevant to this issue.
    Looks at the row's content for item_external_id fields.
    """
    if row_id is None:
        # Batch-level issue — look at all items in the batch
        rows = db.execute(
            "SELECT raw_content FROM ingest_rows WHERE batch_id = %s LIMIT 100",
            (batch_id,),
        ).fetchall()
        items = []
        for r in rows:
            try:
                content = json.loads(r["raw_content"]) if isinstance(r["raw_content"], str) else r["raw_content"]
                for field in ("item_external_id", "external_id"):
                    val = content.get(field)
                    if val:
                        items.append(str(val))
            except Exception:
                pass
        return list(set(items))[:20]  # cap

    row = db.execute(
        "SELECT raw_content FROM ingest_rows WHERE row_id = %s",
        (row_id,),
    ).fetchone()
    if not row:
        return []
    try:
        content = json.loads(row["raw_content"]) if isinstance(row["raw_content"], str) else row["raw_content"]
        for field in ("item_external_id", "external_id"):
            val = content.get(field)
            if val:
                return [str(val)]
    except Exception:
        pass
    return []


def _get_active_shortages_for_items(
    db: psycopg.Connection,
    item_external_ids: list[str],
) -> tuple[int, list[str]]:
    """
    Count active shortages for a list of item external_ids.
    Returns (count, list_of_affected_external_ids).
    """
    if not item_external_ids:
        return 0, []

    rows = db.execute(
        """
        SELECT i.external_id, COUNT(s.shortage_id) AS shortage_count
        FROM items i
        JOIN shortages s ON s.item_id = i.item_id
        WHERE i.external_id = ANY(%s)
          AND s.status = 'active'
        GROUP BY i.external_id
        """,
        (item_external_ids,),
    ).fetchall()

    total = sum(r["shortage_count"] for r in rows)
    affected = [r["external_id"] for r in rows if r["shortage_count"] > 0]
    return total, affected


def _get_finished_goods_via_bom(
    db: psycopg.Connection,
    item_external_ids: list[str],
) -> list[str]:
    """
    Traverse BOM upward to find finished goods impacted by component items.
    Uses bom_component edges: component → parent.
    """
    if not item_external_ids:
        return []

    # Get item_ids from external_ids
    rows = db.execute(
        "SELECT item_id, external_id FROM items WHERE external_id = ANY(%s)",
        (item_external_ids,),
    ).fetchall()
    component_ids = {UUID(str(r["item_id"])) for r in rows}

    if not component_ids:
        return []

    visited: set[UUID] = set(component_ids)
    queue: list[UUID] = list(component_ids)
    finished_goods: set[str] = set()

    # BOM is stored as edges: from=component → to=parent or from=parent → to=component
    # The 'bom_component' edge type connects parent to child: parent -[bom_component]-> child
    # So to go UP, we need edges where to_node_id = item_node_id
    # GraphStore stores items as nodes via node_type='Item'
    # We'll use the bom table directly if available
    while queue:
        batch_component_ids = queue[:50]
        queue = queue[50:]

        # Try bom table first (from migration 008)
        bom_rows = db.execute(
            """
            SELECT DISTINCT b.parent_item_id, i.external_id
            FROM bom_components b
            JOIN items i ON i.item_id = b.parent_item_id
            WHERE b.component_item_id = ANY(%s)
            """,
            (list(batch_component_ids),),
        ).fetchall()

        for r in bom_rows:
            parent_id = UUID(str(r["parent_item_id"]))
            if parent_id not in visited:
                visited.add(parent_id)
                queue.append(parent_id)
                finished_goods.add(r["external_id"])

    return list(finished_goods)


def score_issues(
    db: psycopg.Connection,
    batch_id: UUID,
    issues: list[AgentIssue],
) -> list[AgentIssue]:
    """
    Enrich each issue with impact_score, affected_items, active_shortages_count.
    Modifies issues in place and returns them.
    """
    for issue in issues:
        item_ext_ids = _get_item_ids_for_issue(db, batch_id, issue.row_id)

        shortages_count, items_with_shortages = _get_active_shortages_for_items(
            db, item_ext_ids
        )

        # Also look at finished goods impacted via BOM
        fg_items = _get_finished_goods_via_bom(db, item_ext_ids)
        if fg_items:
            fg_shortages, fg_with_shortages = _get_active_shortages_for_items(
                db, fg_items
            )
            shortages_count += fg_shortages
            items_with_shortages = list(set(items_with_shortages + fg_with_shortages))

        severity_weight = SEVERITY_WEIGHTS.get(issue.severity, 1.0)
        impact_score = severity_weight * (1.0 + math.log(1.0 + shortages_count))

        issue.impact_score = round(impact_score, 4)
        issue.affected_items = items_with_shortages
        issue.active_shortages_count = shortages_count

    return issues
