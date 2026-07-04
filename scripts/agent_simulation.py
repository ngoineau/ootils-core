"""
agent_simulation.py — scenario-backed counter-factual harness for the watcher
fleet (chantier #340, wedge V1 « scenario-backed recommendations »).

ONE scenario fork per watcher RUN (named ``what-if-<agent>-<run timestamp>``),
NOT one per recommendation: the watcher applies the overrides of all its
simulable candidates in that single fork (in-process /v1/simulate path via
``ootils_core.tools.agent_tools.simulate_overrides``), reads the aggregate
shortage delta, attributes each candidate its per-item share, then ARCHIVES
the scenario (TTL pattern — ``scenarios.status='archived'``, never DELETE).

Honest simulable subset (what a node override CAN express):
  EXPEDITE      — there is an existing FUTURE firm receipt (PO/WO/transfer
                  node) for the item landing AFTER the need date; the
                  counter-factual advances its ``time_ref`` to the need date.
                  SIMULABLE.
  ORDER_NOW /
  ORDER_RUSH    — the action drafts a NEW order; no node exists to override
                  and node creation is not an override. NOT simulable (marked
                  ``not_simulated_reason``; gated on ghost-supply injection).
  EXPEDITE with no such receipt — nothing to advance. NOT simulable.

Failure contract (fail-loudly, #339 vocabulary): if the fork's propagation
fails, simulable candidates are STILL emitted but demoted to
confidence=NEEDS_DATA_REVIEW and carry NO delta — never a fabricated one.
See :func:`effective_confidence`.

Scope note: shortage_watcher and material_watcher are scenario-backed via
:func:`simulate_run` (node overrides — EXPEDITE advances an existing firm
receipt). lot_policy is ALSO scenario-backed, via the sibling
:func:`simulate_param_run` (chantier #347 PR4): its actions map onto
whitelisted planning-param overlay fields (``lot_size_rule`` /
``min_order_qty`` / ``order_multiple_qty``, see ``ALLOWED_PARAM_FIELDS``),
applied through ``simulate_param_overrides`` instead of
``simulate_overrides``, but sharing the same one-fork-per-run /
archive-at-end harness shape. eando / dq stay baseline-only: their actions
are disposition changes, not expressible as either a node override or a
planning-param overlay field.
"""
from __future__ import annotations

import datetime as _dt
import logging
from collections import Counter, defaultdict

import psycopg
from psycopg.rows import dict_row

import mrp_core as core
from ootils_core.tools.agent_tools import (
    archive_scenario,
    simulate_overrides,
    simulate_param_overrides,
)

logger = logging.getLogger(__name__)

NOT_SIMULABLE_NEW_ORDER = (
    "action drafts a NEW order — not expressible as a node override "
    "(no existing supply node to advance); simulation gated on ghost-supply injection"
)
NOT_SIMULABLE_NO_RECEIPT = (
    "no existing future firm receipt lands after the need date for this item — "
    "nothing to advance by override"
)
NOT_SIMULABLE_NON_PARAMETRIC = (
    "action does not map onto a whitelisted planning-param overlay field "
    "(ALLOWED_PARAM_FIELDS) — nothing to simulate by parameter override"
)


# ---------------------------------------------------------------------------
# Pure helpers (unit-testable without a DB)
# ---------------------------------------------------------------------------

def build_expedite_override(receipts_by_item: dict, item, need_date: _dt.date):
    """Pure: pick the EARLIEST firm receipt strictly AFTER need_date and build
    the override that advances its ``time_ref`` to the need date.

    receipts_by_item: {item_id: [(time_ref: date, node_id), ...] sorted asc}.
    Returns the override dict (node_id/field_name/new_value + the receipt's
    original date for the evidence trail), or None when nothing is advanceable.
    """
    for tref, node_id in receipts_by_item.get(item, ()):
        if tref > need_date:
            return {
                "node_id": str(node_id),
                "field_name": "time_ref",
                "new_value": need_date.isoformat(),
                "receipt_time_ref": tref.isoformat(),
            }
    return None


def effective_confidence(base_confidence: str, simulated: bool, propagation_status) -> str:
    """Pure fail-loudly rule (#340): a candidate whose counter-factual was
    attempted but whose delta could NOT be computed (fork propagation failed
    or skipped) is emitted anyway but demoted to NEEDS_DATA_REVIEW — never
    with a fabricated delta. Non-simulated candidates keep their base
    confidence (they never claimed a counter-factual).
    """
    if simulated and propagation_status != "ok":
        return "NEEDS_DATA_REVIEW"
    return base_confidence


def simulation_evidence(summary: dict, result: dict) -> dict:
    """Evidence block (#340 contract) for one candidate: simulation_scenario_id
    + per-item shortage delta, OR the documented not-simulated marker."""
    ev = {
        "simulation_scenario_id": summary["scenario_id"],
        "simulated": result["simulated"],
        "propagation_status": summary["propagation_status"] if result["simulated"] else None,
        "delta": result["delta"],
    }
    if result["simulated"]:
        # Auditable marker, not just a docstring: this delta is the per-item
        # share of ONE fork shared by the whole watcher run (cost decision —
        # one deep-copy per run, not per reco). Cross-item interactions
        # between overrides of the same run may be over/under-attributed.
        ev["delta_attribution"] = "per_item_share_of_shared_run_fork"
    if result.get("override"):
        ev["override"] = result["override"]
    if result.get("reason"):
        ev["not_simulated_reason"] = result["reason"]
    return ev


# ---------------------------------------------------------------------------
# DB-facing harness
# ---------------------------------------------------------------------------

def load_future_receipts(conn, scenario: str = core.BASELINE) -> dict:
    """{item_id: [(time_ref, node_id), ...] sorted asc} of ACTIVE firm receipts
    (PO/WO/transfer nodes) dated today or later in the given scenario — the
    override candidates for EXPEDITE counter-factuals."""
    out = defaultdict(list)
    cur = conn.cursor()
    for node_id, item, tref in cur.execute(
        "SELECT node_id, item_id, time_ref FROM nodes "
        "WHERE scenario_id=%(b)s AND active AND node_type=ANY(%(t)s) "
        "AND time_ref IS NOT NULL AND time_ref >= CURRENT_DATE "
        "ORDER BY item_id, time_ref",
        {"b": scenario, "t": core.FIRM_RECEIPT_TYPES},
    ).fetchall():
        out[item].append((tref, node_id))
    return dict(out)


def simulate_run(dsn: str, agent_name: str, candidates: list, receipts_by_item: dict):
    """Run the ONE-fork-per-run counter-factual for a watcher run.

    candidates: list of {"item": uuid, "action": str, "need_date": date}.

    Returns (summary, results):
      summary — {"scenario_id", "scenario_name", "propagation_status",
                 "delta_computed", "aggregate", "archived",
                 "simulated_candidates", "non_simulated_candidates"}.
                 aggregate = {"new_shortages", "resolved_shortages",
                 "net_change"} or None. No fork is created when no candidate
                 is simulable (scenario_id stays None).
      results — one dict per candidate (index-aligned):
                 {"simulated": bool, "reason": str|None,
                  "delta": {"new_shortages", "resolved_shortages",
                            "net_change"} | None,
                  "override": {...}|None}.
    """
    results = []
    overrides, sim_idx = [], []
    for i, c in enumerate(candidates):
        if c["action"] != "EXPEDITE":
            results.append({"simulated": False, "reason": NOT_SIMULABLE_NEW_ORDER,
                            "delta": None, "override": None})
            continue
        ov = build_expedite_override(receipts_by_item, c["item"], c["need_date"])
        if ov is None:
            results.append({"simulated": False, "reason": NOT_SIMULABLE_NO_RECEIPT,
                            "delta": None, "override": None})
            continue
        results.append({"simulated": True, "reason": None, "delta": None, "override": ov})
        overrides.append({k: ov[k] for k in ("node_id", "field_name", "new_value")})
        sim_idx.append(i)

    summary = {
        "scenario_id": None, "scenario_name": None,
        "propagation_status": None, "delta_computed": False,
        "aggregate": None, "archived": False,
        "simulated_candidates": len(sim_idx),
        "non_simulated_candidates": len(candidates) - len(sim_idx),
    }
    if not overrides:
        return summary, results

    ts = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    name = f"what-if-{agent_name}-{ts}"
    summary["scenario_name"] = name
    try:
        # Dedicated dict_row connection: simulate_overrides owns its own
        # transaction/commits and the engine kernels need dict rows — never
        # reuse the watcher's governed-run connection.
        with psycopg.connect(dsn, row_factory=dict_row) as simconn:
            try:
                sim = simulate_overrides(
                    simconn, overrides,
                    scenario_name=name, base_scenario_id=core.BASELINE,
                )
                summary["scenario_id"] = sim["scenario_id"]
                summary["propagation_status"] = sim["propagation_status"]
                summary["delta_computed"] = sim["delta_computed"]
                if sim["delta_computed"]:
                    delta = sim["delta"]
                    new_by = Counter(str(e["item_id"]) for e in delta["new_shortages"] if e["item_id"])
                    res_by = Counter(str(e["item_id"]) for e in delta["resolved_shortages"] if e["item_id"])
                    summary["aggregate"] = {
                        "new_shortages": len(delta["new_shortages"]),
                        "resolved_shortages": len(delta["resolved_shortages"]),
                        "net_change": delta["net_shortage_change"],
                    }
                    # Attribute each simulable candidate its per-item share of
                    # the aggregate delta.
                    for i in sim_idx:
                        it = str(candidates[i]["item"])
                        n_new, n_res = new_by.get(it, 0), res_by.get(it, 0)
                        results[i]["delta"] = {
                            "new_shortages": n_new,
                            "resolved_shortages": n_res,
                            "net_change": n_new - n_res,
                        }
            finally:
                # ALWAYS retire the run's fork, even on a failed recompute —
                # TTL pattern (status='archived'), never DELETE.
                if summary["scenario_id"]:
                    archive_scenario(simconn, summary["scenario_id"])
                    summary["archived"] = True
    except Exception:
        # Fail-loudly: the run continues, simulable candidates are demoted to
        # NEEDS_DATA_REVIEW by effective_confidence() — no fabricated delta.
        logger.exception(
            "agent=%s: scenario simulation failed — simulable candidates will be "
            "emitted as NEEDS_DATA_REVIEW without delta", agent_name,
        )
        if summary["propagation_status"] is None:
            summary["propagation_status"] = "failed"
    return summary, results


def simulate_param_run(dsn: str, agent_name: str, candidates: list, applied_by: str):
    """Run the ONE-fork-per-run counter-factual for a planning-param overlay
    watcher run (chantier #347 PR4 — sibling of :func:`simulate_run`).

    candidates: list of {"item": uuid, "simulable": bool,
                 "param_override": {"item_id", "location_id", "field_name",
                 "value"} | list[{...}] | None, "reason": str | None}. A list
                 covers a proposal that maps onto MORE than one whitelisted
                 field at once (e.g. SET_LOT_RULE:POQ needs BOTH
                 lot_size_rule and lot_size_poq_periods to be meaningful —
                 see agent_lot_policy_watcher.build_param_override); every
                 override in the list is applied inside the SAME candidate's
                 fork contribution, and its per-item delta share is still
                 attributed as ONE candidate. The CALLER (the watcher)
                 decides simulability and builds the whitelisted
                 param_override(s) — this harness only owns the
                 fork/propagate/archive lifecycle, exactly like simulate_run
                 owns it for node overrides.

    Returns (summary, results) — SAME shape as simulate_run:
      summary — {"scenario_id", "scenario_name", "propagation_status",
                 "delta_computed", "aggregate", "archived",
                 "simulated_candidates", "non_simulated_candidates"}.
                 aggregate = {"new_shortages", "resolved_shortages",
                 "net_change"} or None. No fork is created when no candidate
                 is simulable (scenario_id stays None).
      results — one dict per candidate (index-aligned):
                 {"simulated": bool, "reason": str|None,
                  "delta": {"new_shortages", "resolved_shortages",
                            "net_change"} | None,
                  "override": {...}|list[{...}]|None} (same shape the
                  candidate supplied — dict or list).
    """
    results = []
    overrides, sim_idx = [], []
    for i, c in enumerate(candidates):
        if not c.get("simulable") or not c.get("param_override"):
            results.append({
                "simulated": False,
                "reason": c.get("reason") or NOT_SIMULABLE_NON_PARAMETRIC,
                "delta": None, "override": None,
            })
            continue
        ov = c["param_override"]
        ov_list = ov if isinstance(ov, list) else [ov]
        results.append({"simulated": True, "reason": None, "delta": None, "override": ov})
        overrides.extend({k: o[k] for k in ("item_id", "location_id", "field_name", "value")} for o in ov_list)
        sim_idx.append(i)

    summary = {
        "scenario_id": None, "scenario_name": None,
        "propagation_status": None, "delta_computed": False,
        "aggregate": None, "archived": False,
        "simulated_candidates": len(sim_idx),
        "non_simulated_candidates": len(candidates) - len(sim_idx),
    }
    if not overrides:
        return summary, results

    ts = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    name = f"what-if-{agent_name}-{ts}"
    summary["scenario_name"] = name
    try:
        # Dedicated dict_row connection: simulate_param_overrides owns its own
        # transaction/commits and the engine kernels need dict rows — never
        # reuse the watcher's governed-run connection.
        with psycopg.connect(dsn, row_factory=dict_row) as simconn:
            try:
                sim = simulate_param_overrides(
                    simconn, overrides,
                    scenario_name=name, base_scenario_id=core.BASELINE,
                    applied_by=applied_by,
                )
                summary["scenario_id"] = sim["scenario_id"]
                summary["propagation_status"] = sim["propagation_status"]
                summary["delta_computed"] = sim["delta_computed"]
                if sim["delta_computed"]:
                    delta = sim["delta"]
                    new_by = Counter(str(e["item_id"]) for e in delta["new_shortages"] if e["item_id"])
                    res_by = Counter(str(e["item_id"]) for e in delta["resolved_shortages"] if e["item_id"])
                    summary["aggregate"] = {
                        "new_shortages": len(delta["new_shortages"]),
                        "resolved_shortages": len(delta["resolved_shortages"]),
                        "net_change": delta["net_shortage_change"],
                    }
                    # Attribute each simulable candidate its per-item share of
                    # the aggregate delta.
                    for i in sim_idx:
                        it = str(candidates[i]["item"])
                        n_new, n_res = new_by.get(it, 0), res_by.get(it, 0)
                        results[i]["delta"] = {
                            "new_shortages": n_new,
                            "resolved_shortages": n_res,
                            "net_change": n_new - n_res,
                        }
            finally:
                # ALWAYS retire the run's fork, even on a failed recompute —
                # TTL pattern (status='archived'), never DELETE.
                if summary["scenario_id"]:
                    archive_scenario(simconn, summary["scenario_id"])
                    summary["archived"] = True
    except Exception:
        # Fail-loudly: the run continues, simulable candidates are demoted to
        # NEEDS_DATA_REVIEW by effective_confidence() — no fabricated delta.
        logger.exception(
            "agent=%s: param-overlay scenario simulation failed — simulable "
            "candidates will be emitted as NEEDS_DATA_REVIEW without delta", agent_name,
        )
        if summary["propagation_status"] is None:
            summary["propagation_status"] = "failed"
    return summary, results
