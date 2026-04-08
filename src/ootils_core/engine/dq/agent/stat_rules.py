"""
stat_rules.py — Catégorie 1 : anomalies statistiques.

Rules:
  STAT_LEAD_TIME_SPIKE  : lead_time_days dévie de >3σ vs historique item
  STAT_FORECAST_SPIKE   : forecast_qty > moyenne historique × 10
  STAT_PRICE_OUTLIER    : unit_price hors [Q1 - 1.5×IQR, Q3 + 1.5×IQR]
  STAT_SAFETY_STOCK_ZERO: safety_stock = 0 sur item avec shortages actifs
  STAT_NEGATIVE_ONHAND  : on_hand_qty < 0
"""
from __future__ import annotations

import json
import logging
import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import psycopg

logger = logging.getLogger(__name__)

# Number of historical batches to load for comparison
HISTORY_WINDOW = 10

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"


@dataclass
class AgentIssue:
    """An issue raised by the DQ Agent (stat, temporal, or impact)."""
    issue_id: UUID
    batch_id: UUID
    row_id: UUID | None
    row_number: int | None
    dq_level: int  # 3 = stat, 4 = impact
    rule_code: str
    severity: str
    field_name: str | None
    raw_value: str | None
    message: str
    # Impact fields (populated by impact_scorer)
    impact_score: float | None = None
    affected_items: list[str] = field(default_factory=list)
    active_shortages_count: int = 0
    llm_explanation: str | None = None
    llm_suggestion: str | None = None


def _load_history(
    db: psycopg.Connection,
    entity_type: str,
    current_batch_id: UUID,
    window: int = HISTORY_WINDOW,
) -> list[dict]:
    """
    Load raw_content of rows from the N most recent completed batches
    of the same entity_type (excluding the current batch).
    Returns a flat list of content dicts.
    """
    rows = db.execute(
        """
        SELECT ir.raw_content
        FROM ingest_rows ir
        JOIN ingest_batches ib ON ib.batch_id = ir.batch_id
        WHERE ib.entity_type = %s
          AND ib.batch_id != %s
          AND ib.dq_status IN ('validated', 'rejected')
        ORDER BY ib.created_at DESC
        LIMIT %s
        """,
        (entity_type, current_batch_id, window * 100),  # rough cap
    ).fetchall()

    result = []
    for r in rows:
        try:
            content = json.loads(r["raw_content"]) if isinstance(r["raw_content"], str) else r["raw_content"]
            if isinstance(content, dict):
                result.append(content)
        except Exception:
            pass
    return result


def _load_current_rows(
    db: psycopg.Connection,
    batch_id: UUID,
) -> list[tuple[UUID, int, dict]]:
    """Load (row_id, row_number, content) for all rows in the current batch."""
    rows = db.execute(
        """
        SELECT row_id, row_number, raw_content
        FROM ingest_rows
        WHERE batch_id = %s
        ORDER BY row_number
        """,
        (batch_id,),
    ).fetchall()

    result = []
    for r in rows:
        try:
            content = json.loads(r["raw_content"]) if isinstance(r["raw_content"], str) else r["raw_content"]
            if isinstance(content, dict):
                result.append((r["row_id"], r["row_number"], content))
        except Exception:
            pass
    return result


def _get_entity_type(db: psycopg.Connection, batch_id: UUID) -> str:
    row = db.execute(
        "SELECT entity_type FROM ingest_batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    return row["entity_type"] if row else ""


# ──────────────────────────────────────────────────────────────
# STAT_LEAD_TIME_SPIKE
# ──────────────────────────────────────────────────────────────

def _check_lead_time_spike(
    batch_id: UUID,
    current_rows: list[tuple[UUID, int, dict]],
    history: list[dict],
) -> list[AgentIssue]:
    """Z-score > 3σ on lead_time_days vs historical values per item."""
    issues: list[AgentIssue] = []

    # Build historical lead times per item
    hist_by_item: dict[str, list[float]] = {}
    for content in history:
        item_ext = content.get("item_external_id") or content.get("external_id")
        lt = content.get("lead_time_days")
        if item_ext and lt is not None:
            try:
                hist_by_item.setdefault(item_ext, []).append(float(lt))
            except (TypeError, ValueError):
                pass

    for row_id, row_number, content in current_rows:
        item_ext = content.get("item_external_id") or content.get("external_id")
        lt_raw = content.get("lead_time_days")
        if item_ext is None or lt_raw is None:
            continue
        try:
            lt = float(lt_raw)
        except (TypeError, ValueError):
            continue

        hist = hist_by_item.get(item_ext, [])
        if len(hist) < 3:
            continue  # not enough data

        mean = statistics.mean(hist)
        stdev = statistics.stdev(hist)
        if stdev == 0:
            continue

        z_score = abs(lt - mean) / stdev
        if z_score > 3.0:
            issues.append(AgentIssue(
                issue_id=uuid4(),
                batch_id=batch_id,
                row_id=row_id,
                row_number=row_number,
                dq_level=3,
                rule_code="STAT_LEAD_TIME_SPIKE",
                severity=SEVERITY_ERROR,
                field_name="lead_time_days",
                raw_value=str(lt_raw),
                message=(
                    f"lead_time_days={lt} dévie de {z_score:.1f}σ vs historique item "
                    f"(μ={mean:.1f}, σ={stdev:.1f})"
                ),
            ))

    return issues


# ──────────────────────────────────────────────────────────────
# STAT_FORECAST_SPIKE
# ──────────────────────────────────────────────────────────────

def _check_forecast_spike(
    batch_id: UUID,
    current_rows: list[tuple[UUID, int, dict]],
    history: list[dict],
) -> list[AgentIssue]:
    """forecast qty > moyenne historique × 10 par item."""
    issues: list[AgentIssue] = []

    # Historical forecast quantities per item
    hist_by_item: dict[str, list[float]] = {}
    for content in history:
        item_ext = content.get("item_external_id")
        qty_raw = content.get("quantity")
        if item_ext and qty_raw is not None:
            try:
                hist_by_item.setdefault(item_ext, []).append(float(qty_raw))
            except (TypeError, ValueError):
                pass

    for row_id, row_number, content in current_rows:
        item_ext = content.get("item_external_id")
        qty_raw = content.get("quantity")
        if item_ext is None or qty_raw is None:
            continue
        try:
            qty = float(qty_raw)
        except (TypeError, ValueError):
            continue

        hist = hist_by_item.get(item_ext, [])
        if len(hist) < 2:
            continue

        mean = statistics.mean(hist)
        if mean <= 0:
            continue

        if qty > mean * 10:
            issues.append(AgentIssue(
                issue_id=uuid4(),
                batch_id=batch_id,
                row_id=row_id,
                row_number=row_number,
                dq_level=3,
                rule_code="STAT_FORECAST_SPIKE",
                severity=SEVERITY_WARNING,
                field_name="quantity",
                raw_value=str(qty_raw),
                message=(
                    f"forecast_qty={qty} est {qty/mean:.1f}× la moyenne historique "
                    f"item {item_ext} (μ={mean:.1f})"
                ),
            ))

    return issues


# ──────────────────────────────────────────────────────────────
# STAT_PRICE_OUTLIER
# ──────────────────────────────────────────────────────────────

def _check_price_outlier(
    batch_id: UUID,
    current_rows: list[tuple[UUID, int, dict]],
    history: list[dict],
) -> list[AgentIssue]:
    """unit_price hors [Q1 - 1.5×IQR, Q3 + 1.5×IQR]."""
    issues: list[AgentIssue] = []

    hist_prices: dict[str, list[float]] = {}
    for content in history:
        item_ext = content.get("item_external_id") or content.get("external_id")
        price = content.get("unit_price")
        if item_ext and price is not None:
            try:
                hist_prices.setdefault(item_ext, []).append(float(price))
            except (TypeError, ValueError):
                pass

    for row_id, row_number, content in current_rows:
        item_ext = content.get("item_external_id") or content.get("external_id")
        price_raw = content.get("unit_price")
        if item_ext is None or price_raw is None:
            continue
        try:
            price = float(price_raw)
        except (TypeError, ValueError):
            continue

        hist = sorted(hist_prices.get(item_ext, []))
        if len(hist) < 4:
            continue

        n = len(hist)
        q1 = hist[n // 4]
        q3 = hist[(3 * n) // 4]
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        if price < lower or price > upper:
            issues.append(AgentIssue(
                issue_id=uuid4(),
                batch_id=batch_id,
                row_id=row_id,
                row_number=row_number,
                dq_level=3,
                rule_code="STAT_PRICE_OUTLIER",
                severity=SEVERITY_WARNING,
                field_name="unit_price",
                raw_value=str(price_raw),
                message=(
                    f"unit_price={price} hors plage attendue [{lower:.2f}, {upper:.2f}] "
                    f"(Q1={q1:.2f}, Q3={q3:.2f})"
                ),
            ))

    return issues


# ──────────────────────────────────────────────────────────────
# STAT_SAFETY_STOCK_ZERO
# ──────────────────────────────────────────────────────────────

def _check_safety_stock_zero(
    db: psycopg.Connection,
    batch_id: UUID,
    current_rows: list[tuple[UUID, int, dict]],
) -> list[AgentIssue]:
    """safety_stock_qty = 0 sur item avec shortages actifs."""
    issues: list[AgentIssue] = []

    # Get items with active shortages
    shortage_rows = db.execute(
        """
        SELECT DISTINCT i.external_id
        FROM shortages s
        JOIN items i ON i.item_id = s.item_id
        WHERE s.status = 'active'
        """,
    ).fetchall()
    items_with_shortages = {r["external_id"] for r in shortage_rows}

    if not items_with_shortages:
        return issues

    for row_id, row_number, content in current_rows:
        safety_raw = content.get("safety_stock_qty")
        item_ext = content.get("item_external_id") or content.get("external_id")

        if safety_raw is None or item_ext is None:
            continue

        try:
            safety = float(safety_raw)
        except (TypeError, ValueError):
            continue

        if safety == 0 and item_ext in items_with_shortages:
            issues.append(AgentIssue(
                issue_id=uuid4(),
                batch_id=batch_id,
                row_id=row_id,
                row_number=row_number,
                dq_level=3,
                rule_code="STAT_SAFETY_STOCK_ZERO",
                severity=SEVERITY_WARNING,
                field_name="safety_stock_qty",
                raw_value=str(safety_raw),
                message=(
                    f"safety_stock_qty=0 pour l'item {item_ext} "
                    f"qui a des shortages actifs"
                ),
            ))

    return issues


# ──────────────────────────────────────────────────────────────
# STAT_NEGATIVE_ONHAND
# ──────────────────────────────────────────────────────────────

def _check_negative_onhand(
    batch_id: UUID,
    current_rows: list[tuple[UUID, int, dict]],
    entity_type: str,
) -> list[AgentIssue]:
    """on_hand_qty < 0 (L1 allows quantity=0 to pass; this checks for negative stock)."""
    issues: list[AgentIssue] = []

    if entity_type not in ("on_hand",):
        return issues

    for row_id, row_number, content in current_rows:
        qty_raw = content.get("quantity")
        if qty_raw is None:
            continue
        try:
            qty = float(qty_raw)
        except (TypeError, ValueError):
            continue

        if qty < 0:
            item_ext = content.get("item_external_id", "?")
            issues.append(AgentIssue(
                issue_id=uuid4(),
                batch_id=batch_id,
                row_id=row_id,
                row_number=row_number,
                dq_level=3,
                rule_code="STAT_NEGATIVE_ONHAND",
                severity=SEVERITY_ERROR,
                field_name="quantity",
                raw_value=str(qty_raw),
                message=(
                    f"on_hand_qty={qty} est négatif pour l'item {item_ext}"
                ),
            ))

    return issues


# ──────────────────────────────────────────────────────────────
# Main entry point
# ──────────────────────────────────────────────────────────────

def run_stat_rules(
    db: psycopg.Connection,
    batch_id: UUID,
) -> list[AgentIssue]:
    """Run all stat rules for a batch. Returns list of AgentIssue."""
    entity_type = _get_entity_type(db, batch_id)
    current_rows = _load_current_rows(db, batch_id)
    history = _load_history(db, entity_type, batch_id)

    issues: list[AgentIssue] = []

    # STAT_LEAD_TIME_SPIKE — only for supplier_items
    if entity_type in ("supplier_items",):
        issues.extend(_check_lead_time_spike(batch_id, current_rows, history))

    # STAT_FORECAST_SPIKE — only for forecast_demand / forecasts
    if entity_type in ("forecast_demand", "forecasts"):
        issues.extend(_check_forecast_spike(batch_id, current_rows, history))

    # STAT_PRICE_OUTLIER — only for entities with unit_price field
    if entity_type in ("purchase_orders", "supplier_items"):
        issues.extend(_check_price_outlier(batch_id, current_rows, history))

    # STAT_SAFETY_STOCK_ZERO — items with safety_stock_qty
    if entity_type in ("items",):
        issues.extend(_check_safety_stock_zero(db, batch_id, current_rows))

    # STAT_NEGATIVE_ONHAND — on_hand batches
    issues.extend(_check_negative_onhand(batch_id, current_rows, entity_type))

    logger.info(
        "stat_rules batch_id=%s entity=%s issues=%d",
        batch_id, entity_type, len(issues),
    )
    return issues
