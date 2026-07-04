"""
tests/test_param_overrides_api.py — pure unit tests for the agent + REST
surface of the scenario planning-param overlay (chantier #347 PR4).

No DB required. Covers:
  - agent_lot_policy_watcher.build_param_override (pure change_type -> whitelisted
    override(s) mapping, including the POQ two-field split and the fail-loudly
    unknown change_type).
  - api.routers.param_overrides.ParamOverrideIn Pydantic validation (bad UUID,
    empty field_name/value/applied_by).
  - the kill-switch helpers _param_overlay_enabled / require_param_overlay_enabled
    against truthy/falsy env (monkeypatched os.environ).
  - agent_simulation.simulate_param_run's DB-free early return path (no simulable
    candidate -> NOT_SIMULABLE_NON_PARAMETRIC, no fork created).

DB round-trip coverage (endpoint 201/GET/DELETE, isolation, simulate delta,
fork archival) lives in
tests/integration/test_param_overlay_agent_integration.py.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from ootils_core.api.routers.param_overrides import (
    ParamOverrideIn,
    _param_overlay_enabled,
    require_param_overlay_enabled,
)
from ootils_core.engine.scenario.param_overlay import ALLOWED_PARAM_FIELDS

# ---------------------------------------------------------------------------
# Import seam: the watcher + simulation harness live under scripts/ (outside
# the package) and do a bare "import mrp_core" / "import agent_simulation" —
# same pattern as tests/integration/test_agent_fleet_smoke.py.
# ---------------------------------------------------------------------------
_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import agent_simulation  # noqa: E402  (after sys.path mutation, by design)
from agent_lot_policy_watcher import _CHANGE_TYPE_FIELD, build_param_override  # noqa: E402

VALID_UUID = "11111111-1111-1111-1111-111111111111"


# ---------------------------------------------------------------------------
# build_param_override — pure change_type -> whitelisted overlay field mapping
# ---------------------------------------------------------------------------


def test_build_param_override_renegotiate_moq_maps_to_min_order_qty():
    """RENEGOTIATE_MOQ -> a single min_order_qty override (whitelisted)."""
    overrides = build_param_override(VALID_UUID, "RENEGOTIATE_MOQ", "50")
    assert overrides == [
        {"item_id": VALID_UUID, "location_id": None,
         "field_name": "min_order_qty", "value": "50"}
    ]
    assert overrides[0]["field_name"] in ALLOWED_PARAM_FIELDS


def test_build_param_override_review_multiple_maps_to_order_multiple_qty():
    """REVIEW_MULTIPLE -> a single order_multiple_qty override (whitelisted)."""
    overrides = build_param_override(VALID_UUID, "REVIEW_MULTIPLE", "12")
    assert overrides == [
        {"item_id": VALID_UUID, "location_id": None,
         "field_name": "order_multiple_qty", "value": "12"}
    ]
    assert overrides[0]["field_name"] in ALLOWED_PARAM_FIELDS


def test_build_param_override_set_lot_rule_non_poq_maps_to_lot_size_rule():
    """SET_LOT_RULE with a plain rule -> a single lot_size_rule override."""
    overrides = build_param_override(VALID_UUID, "SET_LOT_RULE", "EOQ")
    assert overrides == [
        {"item_id": VALID_UUID, "location_id": None,
         "field_name": "lot_size_rule", "value": "EOQ"}
    ]
    assert overrides[0]["field_name"] in ALLOWED_PARAM_FIELDS


def test_build_param_override_set_lot_rule_poq_splits_into_two_whitelisted_fields():
    """SET_LOT_RULE:POQ:<n> yields TWO overrides (lot_size_rule + poq periods)
    with the period count parsed off the ':' suffix — both whitelisted."""
    overrides = build_param_override(VALID_UUID, "SET_LOT_RULE", "POQ:4")
    assert overrides == [
        {"item_id": VALID_UUID, "location_id": None,
         "field_name": "lot_size_rule", "value": "POQ"},
        {"item_id": VALID_UUID, "location_id": None,
         "field_name": "lot_size_poq_periods", "value": "4"},
    ]
    for ov in overrides:
        assert ov["field_name"] in ALLOWED_PARAM_FIELDS


def test_build_param_override_rejects_unknown_change_type():
    """An unknown change_type is a fail-loudly ValueError, never a silent default."""
    with pytest.raises(ValueError, match="unknown lot_policy change_type"):
        build_param_override(VALID_UUID, "NOT_A_CHANGE_TYPE", "1")


def test_change_type_field_map_targets_are_all_whitelisted():
    """Every field _CHANGE_TYPE_FIELD maps onto is a real overlay whitelist key
    — a drift here would silently emit a non-appliable override."""
    for field in _CHANGE_TYPE_FIELD.values():
        assert field in ALLOWED_PARAM_FIELDS


# ---------------------------------------------------------------------------
# ParamOverrideIn — Pydantic request-body validation (422 boundary)
# ---------------------------------------------------------------------------


def test_param_override_in_accepts_valid_body():
    """A well-formed body validates and coerces item_id to a UUID."""
    body = ParamOverrideIn(
        item_id=VALID_UUID, field_name="safety_stock_qty",
        value="42", applied_by="agent:test",
    )
    assert str(body.item_id) == VALID_UUID
    assert body.location_id is None


def test_param_override_in_rejects_invalid_uuid():
    """A non-UUID item_id is a validation error (422 at the API boundary)."""
    with pytest.raises(ValidationError):
        ParamOverrideIn(
            item_id="not-a-uuid", field_name="safety_stock_qty",
            value="42", applied_by="agent:test",
        )


def test_param_override_in_rejects_empty_field_name():
    """field_name has min_length=1 — the empty string is refused."""
    with pytest.raises(ValidationError):
        ParamOverrideIn(
            item_id=VALID_UUID, field_name="", value="42", applied_by="agent",
        )


def test_param_override_in_rejects_empty_value():
    """value has min_length=1 — the empty string is refused."""
    with pytest.raises(ValidationError):
        ParamOverrideIn(
            item_id=VALID_UUID, field_name="safety_stock_qty", value="",
            applied_by="agent",
        )


def test_param_override_in_rejects_empty_applied_by():
    """applied_by has min_length=1 — no anonymous overrides at the API boundary."""
    with pytest.raises(ValidationError):
        ParamOverrideIn(
            item_id=VALID_UUID, field_name="safety_stock_qty", value="42",
            applied_by="",
        )


# ---------------------------------------------------------------------------
# Kill switch — OOTILS_PARAM_OVERLAY_ENABLED (default ON, falsy -> 503)
# ---------------------------------------------------------------------------


def test_param_overlay_enabled_defaults_on_when_env_absent(monkeypatch):
    """Absent env var -> enabled (default ON): require_ passes without raising."""
    monkeypatch.delenv("OOTILS_PARAM_OVERLAY_ENABLED", raising=False)
    assert _param_overlay_enabled() is True
    require_param_overlay_enabled()  # no raise


@pytest.mark.parametrize("truthy", ["1", "true", "yes", "on", "TRUE", " On "])
def test_param_overlay_enabled_truthy_values(monkeypatch, truthy):
    """Every documented truthy spelling (case/space-insensitive) keeps it ON."""
    monkeypatch.setenv("OOTILS_PARAM_OVERLAY_ENABLED", truthy)
    assert _param_overlay_enabled() is True
    require_param_overlay_enabled()  # no raise


@pytest.mark.parametrize("falsy", ["0", "false", "no", "off", "", "disabled"])
def test_param_overlay_enabled_falsy_values_disable(monkeypatch, falsy):
    """Any non-truthy value disables the overlay -> _enabled() is False."""
    monkeypatch.setenv("OOTILS_PARAM_OVERLAY_ENABLED", falsy)
    assert _param_overlay_enabled() is False


def test_require_param_overlay_enabled_raises_503_when_disabled(monkeypatch):
    """A disabled overlay short-circuits the endpoint dependency with a 503."""
    monkeypatch.setenv("OOTILS_PARAM_OVERLAY_ENABLED", "0")
    with pytest.raises(HTTPException) as exc_info:
        require_param_overlay_enabled()
    assert exc_info.value.status_code == 503


# ---------------------------------------------------------------------------
# simulate_param_run — DB-free early return: no simulable candidate means no
# fork is ever created (scenario_id stays None) and the non-parametric reason
# is surfaced. Proves the harness never touches the DB for a run with nothing
# to simulate.
# ---------------------------------------------------------------------------


def test_simulate_param_run_no_simulable_candidate_is_db_free_noop():
    """A candidate flagged non-simulable (no param_override) short-circuits
    before any DB connection: no fork, NOT_SIMULABLE reason carried through."""
    candidates = [{"item": VALID_UUID, "simulable": False,
                   "param_override": None, "reason": None}]
    # dsn is deliberately bogus — the early return must not reach psycopg.connect.
    summary, results = agent_simulation.simulate_param_run(
        "postgresql://unreachable/nodb", "lot_policy_watcher", candidates,
        applied_by="agent:test",
    )
    assert summary["scenario_id"] is None
    assert summary["archived"] is False
    assert summary["simulated_candidates"] == 0
    assert summary["non_simulated_candidates"] == 1
    assert results[0]["simulated"] is False
    assert results[0]["reason"] == agent_simulation.NOT_SIMULABLE_NON_PARAMETRIC
    assert results[0]["delta"] is None
