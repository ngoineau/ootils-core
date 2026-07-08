"""
agent_forecast_watcher.py — Forecast Watcher (DEM-1), the first DEMAND-side
exception watcher of the fleet.

Reads the LATEST baseline Pyramide run per (item, location) and its aggregate
rolling-origin backtest metrics (pyramide_accuracy_metrics, horizon IS NULL),
then emits governed L1 DRAFT FORECAST_DRIFT recommendations when a series has
degraded — the demand analogue of the shortage/material control towers, drafting
a re-forecast/review a planner disposes.

DRIFT DETECTION (None-honest, ADR-023-adjacent):
  * MASE_DEGRADED : backtest MASE > --mase-threshold (default 1.3 => worse than
    1.3x the seasonal naive).
  * BIAS_SUSTAINED : |relative bias| > --bias-ratio-threshold (default 0.3). The
    persisted accuracy aggregate row carries NO demand scale (only mase/wape/
    smape are ratios; `bias` is the single absolute-demand quantity), so the
    relative bias is |bias| / mean(forecast quantity) — the run's own mean
    forecast at the SAME granularity as `bias` (a standard "forecast bias %"
    KPI). Single alias-free join to forecast_values; a true mean-ACTUAL
    normalization would require persisting MAE / mean-actual in
    pyramide_accuracy_metrics (migration change, out of DEM-1 PR-2 scope).
  * BOTH when both fire.
  None-honest: a NULL metric neither triggers NOR blocks the other; both NULL
  (or no mean forecast to normalize the bias) => the series is IGNORED. Absence
  of data is the freshness gate's job (ADR-023), never the drift watcher's — a
  missing metric is not a drift.

EMISSION: governed DRAFT rows in forecast_drift_recommendations via governed_run.
The recommendation_id is a DETERMINISTIC uuid5 over
(scenario, item, location, drift_kind) — NOT the run — so a re-run on an
unchanged, still-DRAFT drift re-derives the SAME id and upserts to a genuine
no-op (zero new rows, zero stream events). A drift that RECURS after having
been resolved (its prior DRAFT was EXPIRED by a previous run) re-derives that
SAME tombstoned id and is REACTIVATED to DRAFT with fresh metrics and a
re-stamped agent_run_id — a pure ON CONFLICT DO NOTHING would leave the
tombstone EXPIRED forever and the recurrence would be silently invisible.
Statuses a human has set (REVIEWED/APPROVED/REJECTED/APPLIED) are NEVER
touched by the upsert, even if the same drift recurs. Prior DRAFTs of THIS
agent/scenario whose drift no longer fires are EXPIRED (the reschedule-watcher
#346 idempotence pattern, stronger than blanket supersede-then-reinsert). The
fleet recommendation_created event (AN-1, #401) is emitted for free by
governed_run: forecast_drift_recommendations is in emit._RECO_TABLES, so a run
that inserts or reactivates >=1 row announces itself on GET /v1/stream; an
unchanged re-run writes 0 and announces nothing.

North Star: deterministic core (no LLM in the drift decision), DRAFT only (never
applies; decision level from agent_governance.decision_level — FORECAST_DRIFT is
L1, a reversible re-forecast/review proposal), auditable (agent_runs),
explainable (evidence = the metrics + windows + thresholds), idempotent
(deterministic id + ON CONFLICT).

BASELINE-ONLY (V1): drift is measured against the baseline forecast because it
is the REAL observed operational accuracy — a fork is simulated, not observed
(same rationale as the ADR-030 outcome machine). Passing a non-baseline scenario
is refused.

Usage:
    DATABASE_URL=postgresql://... python scripts/agent_forecast_watcher.py \
        [--mase-threshold 1.3] [--bias-ratio-threshold 0.3] [--top 15] [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import uuid
from collections import defaultdict
from decimal import Decimal
from typing import Any, Optional, Sequence

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

import mrp_core as core
from agent_governance import decision_level, governed_run

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("forecast_watcher")
AGENT_NAME = "forecast_watcher"

# Same fixed deterministic namespace the reschedule/transfer emitters use
# (ADR-003 kernel _ids namespace). Re-declared locally so this thin CLI carries
# no engine dependency; the name string is drift-specific so ids never collide
# across watchers.
_RECO_NAMESPACE = uuid.UUID("89e1e24e-42d7-5c31-87c7-c64e50e24131")

# Kept in sync with the forecast_drift_recommendations.drift_kind CHECK
# (migration 072): the Python half of the shared vocabulary.
VALID_DRIFT_KINDS: frozenset[str] = frozenset(
    {"MASE_DEGRADED", "BIAS_SUSTAINED", "BOTH"}
)

_COLUMNS: tuple[str, ...] = (
    "recommendation_id",
    "agent_name",
    "agent_run_id",
    "scenario_id",
    "item_id",
    "location_id",
    "pyramide_run_id",
    "action",
    "decision_level",
    "drift_kind",
    "cadence",
    "mase",
    "bias",
    "tracking_ratio",
    "threshold_mase",
    "threshold_bias_ratio",
    "status",
    "confidence",
    "evidence",
)


# ---------------------------------------------------------------------------
# Pure helpers (no DB) — unit-tested in isolation.
# ---------------------------------------------------------------------------
def drift_recommendation_id(
    scenario_id: object,
    item_id: object,
    location_id: object,
    drift_kind: str,
) -> uuid.UUID:
    """Stable uuid5 identity of a forecast-drift recommendation (idempotence key).

    Deterministic over (scenario, item, location, drift_kind) — NOT the run: a
    re-run on the SAME drift re-derives the SAME id, and the upsert (_upsert)
    resolves to one of four outcomes:
      * no existing row -> INSERT (a genuinely new DRAFT).
      * a still-live DRAFT with the same id -> no-op (idempotent re-affirm).
      * a prior row with this id was EXPIRED (drift resolved, then recurred) ->
        REACTIVATED to DRAFT with fresh metrics (the tombstone-reactivation
        fix — a pure ON CONFLICT DO NOTHING would leave it EXPIRED forever and
        the recurrence would be invisible).
      * a prior row with this id carries a human-set status (REVIEWED/APPROVED/
        REJECTED/APPLIED) -> never touched, even if the drift recurs.
    A change of drift_kind (e.g. MASE_DEGRADED -> BOTH) mints a genuinely new id
    (a new DRAFT), and the superseded prior kind is EXPIRED. location_id NULL
    (item-level grain) renders as the literal 'None'.
    """
    name = "|".join(
        [
            "forecast_drift_reco",
            str(scenario_id),
            str(item_id),
            str(location_id) if location_id is not None else "None",
            drift_kind,
        ]
    )
    return uuid.uuid5(_RECO_NAMESPACE, name)


def relative_bias(
    bias: Optional[Decimal], mean_forecast: Optional[Decimal]
) -> Optional[Decimal]:
    """|bias| as a fraction of the mean forecast volume (the bias-ratio scale).

    None-honest: a NULL bias, a NULL mean forecast, or a non-positive mean
    forecast (no scale to normalize against) => None, which cannot trigger the
    BIAS_SUSTAINED condition. Never fabricates a scale.
    """
    if bias is None or mean_forecast is None or mean_forecast <= 0:
        return None
    return abs(bias) / mean_forecast


def classify_drift(
    mase: Optional[Decimal],
    bias_ratio: Optional[Decimal],
    mase_threshold: Decimal,
    bias_ratio_threshold: Decimal,
) -> Optional[str]:
    """Pure drift classifier. Returns a drift_kind or None (no drift).

    None-honest: a NULL metric neither triggers nor blocks the other; both
    absent => None (the series is ignored — a missing metric is not a drift).
    """
    mase_degraded = mase is not None and mase > mase_threshold
    bias_sustained = bias_ratio is not None and bias_ratio > bias_ratio_threshold
    if mase_degraded and bias_sustained:
        return "BOTH"
    if mase_degraded:
        return "MASE_DEGRADED"
    if bias_sustained:
        return "BIAS_SUSTAINED"
    return None


# ---------------------------------------------------------------------------
# DB helpers — explicit dict_row cursors (row-factory agnostic; the CLI opens a
# tuple_row connection, the integration harness a dict_row one — access by name
# works under both).
# ---------------------------------------------------------------------------
def _fetch_series(conn: psycopg.Connection, scenario: object) -> list[dict[str, Any]]:
    """Latest non-failed run per (item, location) for a scenario + its aggregate
    backtest metrics (horizon NULL) + the run's mean forecast (bias scale).

    Mirrors the canonical latest-aggregate read (repository.fetch_latest_
    aggregate_wape): JOIN pyramide_accuracy_metrics on the horizon-NULL row,
    ORDER BY the run then the metric recency. DISTINCT ON collapses to one row
    per series. Runs are 'generated' at persist time and only later 'committed'
    (repository.persist_run / _commit_snapshot_to_demand_nodes), so both count —
    only 'failed' is excluded.
    """
    cur = conn.cursor(row_factory=dict_row)
    return cur.execute(
        """
        SELECT DISTINCT ON (r.item_id, r.location_id)
            r.run_id,
            r.item_id,
            r.location_id,
            i.external_id      AS item_external_id,
            loc.external_id    AS location_external_id,
            r.granularity,
            r.created_at,
            m.mase,
            m.bias,
            m.wape,
            m.smape,
            m.n_cutoffs,
            m.n_observations,
            fv.mean_forecast
        FROM pyramide_runs r
        JOIN pyramide_accuracy_metrics m
             ON m.run_id = r.run_id AND m.horizon IS NULL
        JOIN items i           ON i.item_id = r.item_id
        JOIN locations loc     ON loc.location_id = r.location_id
        LEFT JOIN LATERAL (
            SELECT AVG(fvv.quantity) AS mean_forecast
            FROM forecast_values fvv
            WHERE fvv.forecast_id = r.forecast_id
        ) fv ON TRUE
        WHERE r.scenario_id = %(scenario)s
          AND r.status <> 'failed'
        ORDER BY r.item_id, r.location_id, r.created_at DESC, m.created_at DESC
        """,
        {"scenario": scenario},
    ).fetchall()


def _upsert(
    conn: psycopg.Connection, rows: Sequence[Sequence[Any]]
) -> tuple[list, list, list]:
    """Idempotent insert with EXPIRED-tombstone reactivation.

    ON CONFLICT (recommendation_id) DO UPDATE ... WHERE status = 'EXPIRED': a
    brand-new drift id INSERTs; a still-live DRAFT with the same id hits the
    WHERE guard and is a genuine no-op (idempotent re-affirm, no RETURNING row);
    a prior row that was EXPIRED (drift resolved, then recurred with the SAME
    kind) is REACTIVATED to DRAFT with fresh metrics and a re-stamped
    agent_run_id — the fix for the tombstone bug where a pure DO NOTHING left a
    recurring drift invisible forever. A human-set status (REVIEWED/APPROVED/
    REJECTED/APPLIED) also fails the WHERE guard and is never touched — a
    REJECTED verdict stays rejected even if the same drift recurs (assumed).
    Deliberate corollary: a LIVE DRAFT's stored metrics are frozen at emission
    even if the magnitude worsens (same kind = same id = no-op) — refreshing
    them silently would be the "silent mutation" the reschedule pattern forbids,
    and re-emitting an event per oscillation would spam the fleet; the current
    magnitude is always one query away via pyramide_run_id.

    Returns (inserted_ids, reactivated_ids, affirmed_ids): inserted_ids are rows
    genuinely INSERTed this run (xmax = 0 — see below); reactivated_ids are
    EXPIRED rows flipped back to DRAFT; affirmed_ids is EVERY id attempted — the
    caller uses it to NOT expire still-valid prior DRAFTs. SQL composed via
    psycopg.sql (no f-strings).
    """
    if not rows:
        return [], [], []
    col_ids = sql.SQL(", ").join(sql.Identifier(c) for c in _COLUMNS)
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in _COLUMNS)
    query = sql.SQL(
        "INSERT INTO forecast_drift_recommendations ({cols}) VALUES ({vals}) "
        "ON CONFLICT (recommendation_id) DO UPDATE SET "
        "status = 'DRAFT', updated_at = now(), "
        "agent_run_id = EXCLUDED.agent_run_id, "
        "pyramide_run_id = EXCLUDED.pyramide_run_id, "
        "cadence = EXCLUDED.cadence, "
        "mase = EXCLUDED.mase, "
        "bias = EXCLUDED.bias, "
        "tracking_ratio = EXCLUDED.tracking_ratio, "
        "threshold_mase = EXCLUDED.threshold_mase, "
        "threshold_bias_ratio = EXCLUDED.threshold_bias_ratio, "
        "evidence = EXCLUDED.evidence "
        "WHERE forecast_drift_recommendations.status = 'EXPIRED' "
        # xmax = 0 is Postgres' only way to tell an INSERT from an UPDATE inside
        # a single upsert's RETURNING clause (no INSERTED/UPDATED marker exists).
        "RETURNING recommendation_id, (xmax = 0) AS was_insert"
    ).format(cols=col_ids, vals=placeholders)
    inserted_ids: list = []
    reactivated_ids: list = []
    cur = conn.cursor(row_factory=dict_row)
    for r in rows:
        got = cur.execute(query, r).fetchone()
        if got is not None:
            if got["was_insert"]:
                inserted_ids.append(got["recommendation_id"])
            else:
                reactivated_ids.append(got["recommendation_id"])
    affirmed = [r[0] for r in rows]
    return inserted_ids, reactivated_ids, affirmed


def _expire_stale_drafts(
    conn: psycopg.Connection, scenario: object, keep_ids: Sequence[object]
) -> int:
    """EXPIRE this agent/scenario's prior DRAFTs whose drift no longer fires.

    A DRAFT NOT in keep_ids (the ids the current run affirmed) means the drift it
    flagged is resolved — mark it EXPIRED so the queue reflects reality. Affirmed
    ids are left untouched (their identity was just re-affirmed by the idempotent
    upsert). Scoped to this agent + scenario so it never touches another agent's
    or another fork's rows.
    """
    cur = conn.cursor(row_factory=dict_row)
    if keep_ids:
        cur.execute(
            "UPDATE forecast_drift_recommendations SET status='EXPIRED', updated_at=now() "
            "WHERE agent_name=%s AND scenario_id=%s AND status='DRAFT' "
            "AND NOT (recommendation_id = ANY(%s))",
            (AGENT_NAME, scenario, list(keep_ids)),
        )
    else:
        cur.execute(
            "UPDATE forecast_drift_recommendations SET status='EXPIRED', updated_at=now() "
            "WHERE agent_name=%s AND scenario_id=%s AND status='DRAFT'",
            (AGENT_NAME, scenario),
        )
    return cur.rowcount


def _evidence(s: dict[str, Any], drift_kind: str, bias_ratio: Optional[Decimal],
              mase_threshold: Decimal, bias_ratio_threshold: Decimal) -> dict[str, Any]:
    """Forensic JSONB trail (migration-072 carve-out): the metrics that produced
    the verdict, the backtest window counts, and the thresholds crossed."""

    def _f(v: Any) -> Optional[float]:
        return None if v is None else float(v)

    return {
        "drift_kind": drift_kind,
        "mase": _f(s["mase"]),
        "bias": _f(s["bias"]),
        "wape": _f(s["wape"]),
        "smape": _f(s["smape"]),
        "bias_ratio": _f(bias_ratio),
        "mean_forecast": _f(s["mean_forecast"]),
        "n_cutoffs": s["n_cutoffs"],
        "n_observations": s["n_observations"],
        "granularity": s["granularity"],
        "thresholds": {
            "mase": float(mase_threshold),
            "bias_ratio": float(bias_ratio_threshold),
        },
        "pyramide_run_id": str(s["run_id"]),
        "item_external_id": s["item_external_id"],
        "location_external_id": s["location_external_id"],
        "rule": "latest baseline pyramide_accuracy_metrics aggregate row "
                "(horizon NULL); MASE_DEGRADED when mase > threshold, "
                "BIAS_SUSTAINED when |bias|/mean_forecast > bias_ratio_threshold "
                "(relative bias normalized by mean forecast at run granularity — "
                "the accuracy row carries no demand scale), BOTH when both.",
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Forecast Watcher (DEM-1) — governed DRAFT FORECAST_DRIFT recommendations."
    )
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--scenario", default=core.BASELINE,
                   help="scenario_id to run on (V1: baseline only)")
    p.add_argument("--mase-threshold", type=float, default=1.3,
                   help="MASE above this => MASE_DEGRADED (default 1.3)")
    p.add_argument("--bias-ratio-threshold", type=float, default=0.3,
                   help="|bias|/mean_forecast above this => BIAS_SUSTAINED (default 0.3)")
    p.add_argument("--top", type=int, default=15)
    p.add_argument("--dry-run", action="store_true",
                   help="log the drift verdicts without writing any row")
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2

    scenario = args.scenario
    if scenario != core.BASELINE:
        logger.error(
            "Forecast Watcher is BASELINE-ONLY in V1 (got scenario=%s). Forecast "
            "drift is the REAL observed operational accuracy of the baseline "
            "forecast; a fork is simulated, not observed (ADR-030 rationale).",
            scenario,
        )
        return 2

    db = core.guard_db(args.dsn, args.allow_dev)
    mase_threshold = Decimal(str(args.mase_threshold))
    bias_ratio_threshold = Decimal(str(args.bias_ratio_threshold))
    logger.info(
        "Forecast Watcher (DEM-1) running on DB=%s scenario=baseline "
        "mase>%.3f bias_ratio>%.3f%s",
        db, float(mase_threshold), float(bias_ratio_threshold),
        " [DRY-RUN]" if args.dry_run else "",
    )
    t0 = time.perf_counter()

    with psycopg.connect(args.dsn) as conn:
        series = _fetch_series(conn, scenario)

        candidates: list[dict[str, Any]] = []
        by_kind: defaultdict[str, int] = defaultdict(int)
        for s in series:
            bias_ratio = relative_bias(s["bias"], s["mean_forecast"])
            drift_kind = classify_drift(
                s["mase"], bias_ratio, mase_threshold, bias_ratio_threshold
            )
            if drift_kind is None:
                continue
            candidates.append({**s, "bias_ratio": bias_ratio, "drift_kind": drift_kind})
            by_kind[drift_kind] += 1

        if args.dry_run:
            logger.info(
                "DRY-RUN: %d series evaluated, %d drift verdict(s) (no write). By kind: %s",
                len(series), len(candidates), dict(by_kind),
            )
            _log_top(candidates, args.top)
            return 0

        with governed_run(conn, AGENT_NAME, scenario, t0=t0) as run:
            rows: list[tuple] = []
            for c in candidates:
                rid = drift_recommendation_id(
                    scenario, c["item_id"], c["location_id"], c["drift_kind"]
                )
                evidence = _evidence(
                    c, c["drift_kind"], c["bias_ratio"], mase_threshold, bias_ratio_threshold
                )
                rows.append((
                    rid, AGENT_NAME, run.run_id, scenario,
                    c["item_id"], c["location_id"], c["run_id"],
                    "FORECAST_DRIFT", decision_level("FORECAST_DRIFT"), c["drift_kind"],
                    c["granularity"], c["mase"], c["bias"], c["bias_ratio"],
                    mase_threshold, bias_ratio_threshold,
                    "DRAFT", None, Jsonb(evidence),
                ))

            inserted_ids, reactivated_ids, affirmed = _upsert(conn, rows)
            expired = _expire_stale_drafts(conn, scenario, affirmed)
            metrics = {
                "series_evaluated": len(series),
                "drift_detected": len(candidates),
                "recommendations_affirmed": len(affirmed),
                "recommendations_inserted": len(inserted_ids),
                "recommendations_reactivated": len(reactivated_ids),
                "recommendations_idempotent_noop": (
                    len(affirmed) - len(inserted_ids) - len(reactivated_ids)
                ),
                "expired_stale_drafts": expired,
                "by_drift_kind": dict(by_kind),
                "thresholds": {
                    "mase": float(mase_threshold),
                    "bias_ratio": float(bias_ratio_threshold),
                },
            }
            run.set_metrics(metrics)

    elapsed = round(time.perf_counter() - t0, 2)
    logger.info("=" * 92)
    logger.info("FORECAST WATCHER — run %s COMPLETED in %.2fs", str(run.run_id)[:8], elapsed)
    logger.info("  Series evaluated                    : %d", metrics["series_evaluated"])
    logger.info("  Drift detected                      : %d  %s",
                metrics["drift_detected"], metrics["by_drift_kind"])
    logger.info(
        "  Recommendations affirmed (DRAFT)    : %d  (new: %d, reactivated: %d, "
        "idempotent no-op: %d)",
        metrics["recommendations_affirmed"], metrics["recommendations_inserted"],
        metrics["recommendations_reactivated"], metrics["recommendations_idempotent_noop"],
    )
    logger.info("  Prior DRAFTs expired (drift gone)   : %d", metrics["expired_stale_drafts"])
    logger.info("=" * 92)
    _log_top(candidates, args.top)
    return 0


def _log_top(candidates: Sequence[dict[str, Any]], top: int) -> None:
    display = sorted(
        candidates,
        key=lambda c: (c["mase"] if c["mase"] is not None else Decimal(0)),
        reverse=True,
    )
    logger.info("TOP %d drift verdicts (worst MASE first):", top)
    logger.info("  %-14s %-8s %-14s %8s %10s", "item", "loc", "drift_kind", "mase", "bias_ratio")
    for c in display[:top]:
        mase_s = f"{float(c['mase']):.3f}" if c["mase"] is not None else "—"
        br_s = f"{float(c['bias_ratio']):.3f}" if c["bias_ratio"] is not None else "—"
        logger.info(
            "  %-14s %-8s %-14s %8s %10s",
            str(c["item_external_id"])[:14], str(c["location_external_id"])[:8],
            c["drift_kind"], mase_s, br_s,
        )
    logger.info("=" * 92)


if __name__ == "__main__":
    sys.exit(main())
