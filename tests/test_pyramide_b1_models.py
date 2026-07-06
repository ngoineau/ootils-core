"""
Pure (DB-free) tests for the #394 B1 request/response contract additions on
the Pyramide router:

  * ``PyramideValueOut`` — the conformal bounds ``confidence_lower`` /
    ``confidence_upper`` are exposed additively: present when known, honestly
    None when not (NEVER 0 by default), and NOT part of the required set (an
    old serialized payload without them still validates);
  * ``HierarchicalRunRequest`` — ``recon_method`` defaults to 'middleout',
    only accepts the two methods HierarchicalRunner supports
    ('middleout' / 'mintrace_wls_shrink'), and rejects anything else at the
    Pydantic layer (before any DB work);
  * ``_hierarchical_series_out`` — the pure aggregate|leaf mapping is
    faithful without a database (fed a tiny stand-in series object).

These mirror the boundary-validation style of tests/test_pyramide_api.py
(422 before the DB) but target the schema objects directly, so no FastAPI app
or connection is constructed.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from ootils_core.api.routers.pyramide import (
    HierarchicalRunRequest,
    HierarchicalSeriesOut,
    PyramideValueOut,
    _hierarchical_series_out,
)


# ---------------------------------------------------------------------------
# PyramideValueOut — the conformal-bounds carve-out (additive, None-honest)
# ---------------------------------------------------------------------------


class TestPyramideValueOutBounds:
    def test_bounds_present_are_preserved(self):
        value = PyramideValueOut(
            value_id=uuid4(),
            forecast_date=date(2026, 7, 10),
            quantity=Decimal("100"),
            method="MA",
            confidence_lower=Decimal("92.5"),
            confidence_upper=Decimal("108.5"),
        )
        assert value.confidence_lower == Decimal("92.5")
        assert value.confidence_upper == Decimal("108.5")

    def test_bounds_default_to_none_never_zero(self):
        """A value built without bounds carries None on BOTH sides — the
        honest 'no calibration' signal — never a masked 0."""
        value = PyramideValueOut(
            value_id=uuid4(),
            forecast_date=date(2026, 7, 10),
            quantity=Decimal("100"),
            method="MA",
        )
        assert value.confidence_lower is None
        assert value.confidence_upper is None
        # Guard against a "0 default" regression that would look like a
        # collapsed interval instead of an absent one.
        assert value.confidence_lower != Decimal("0")
        assert value.confidence_upper != Decimal("0")

    def test_explicit_none_bounds_round_trip(self):
        value = PyramideValueOut(
            value_id=uuid4(),
            forecast_date=date(2026, 7, 10),
            quantity=Decimal("100"),
            method="MA",
            confidence_lower=None,
            confidence_upper=None,
        )
        assert value.confidence_lower is None
        assert value.confidence_upper is None

    def test_bounds_are_not_required_fields(self):
        """Additive contract: the two bound fields must NOT be required, so a
        payload minted before #394 (no bounds keys) still validates."""
        required = {
            name
            for name, field in PyramideValueOut.model_fields.items()
            if field.is_required()
        }
        assert "confidence_lower" not in required
        assert "confidence_upper" not in required
        # The pre-#394 field set is still sufficient on its own.
        legacy_payload = {
            "value_id": str(uuid4()),
            "forecast_date": "2026-07-10",
            "quantity": "100",
            "method": "MA",
        }
        value = PyramideValueOut.model_validate(legacy_payload)
        assert value.confidence_lower is None
        assert value.confidence_upper is None

    def test_serialization_emits_bound_keys(self):
        """Both keys must appear in the serialized payload (as null when
        absent) so a client can always read them — the whole point of B1."""
        value = PyramideValueOut(
            value_id=uuid4(),
            forecast_date=date(2026, 7, 10),
            quantity=Decimal("100"),
            method="MA",
        )
        dumped = value.model_dump()
        assert "confidence_lower" in dumped
        assert "confidence_upper" in dumped
        assert dumped["confidence_lower"] is None
        assert dumped["confidence_upper"] is None

    def test_serialization_preserves_present_bounds(self):
        value = PyramideValueOut(
            value_id=uuid4(),
            forecast_date=date(2026, 7, 10),
            quantity=Decimal("100"),
            method="MA",
            confidence_lower=Decimal("92.5"),
            confidence_upper=Decimal("108.5"),
        )
        dumped = value.model_dump(mode="json")
        assert Decimal(dumped["confidence_lower"]) == Decimal("92.5")
        assert Decimal(dumped["confidence_upper"]) == Decimal("108.5")


# ---------------------------------------------------------------------------
# HierarchicalRunRequest — recon_method contract
# ---------------------------------------------------------------------------


class TestHierarchicalRunRequestReconMethod:
    def _base_payload(self) -> dict:
        return {
            "hierarchy_id": "prod-default",
            "block_code": "FAM-1",
            "leaf_location_id": "DC-CENTRAL",
        }

    def test_recon_method_defaults_to_middleout(self):
        body = HierarchicalRunRequest(**self._base_payload())
        assert body.recon_method == "middleout"

    def test_recon_method_accepts_middleout(self):
        body = HierarchicalRunRequest(
            **self._base_payload(), recon_method="middleout"
        )
        assert body.recon_method == "middleout"

    def test_recon_method_accepts_mintrace_wls_shrink(self):
        body = HierarchicalRunRequest(
            **self._base_payload(), recon_method="mintrace_wls_shrink"
        )
        assert body.recon_method == "mintrace_wls_shrink"

    def test_recon_method_rejects_none_value(self):
        """'none' is valid ONLY on the leaf POST /runs endpoint; the
        hierarchical endpoint always reconciles, so 'none' is out of pattern."""
        with pytest.raises(ValidationError):
            HierarchicalRunRequest(**self._base_payload(), recon_method="none")

    def test_recon_method_rejects_unknown_value(self):
        with pytest.raises(ValidationError):
            HierarchicalRunRequest(**self._base_payload(), recon_method="banana")

    def test_recon_method_error_targets_the_field(self):
        with pytest.raises(ValidationError) as excinfo:
            HierarchicalRunRequest(**self._base_payload(), recon_method="banana")
        locs = {tuple(err["loc"]) for err in excinfo.value.errors()}
        assert ("recon_method",) in locs

    def test_method_error_helper_lists_supported_methods(self):
        """The _method_error helper (used by the handler for a 422 detail) is
        about the forecast method catalogue, not recon_method."""
        message = HierarchicalRunRequest._method_error()
        assert "method must be one of" in message
        assert "SEASONAL" in message

    def test_defaults_match_the_documented_contract(self):
        body = HierarchicalRunRequest(**self._base_payload())
        assert body.horizon_days == 90
        assert body.granularity == "daily"
        assert body.method == "AUTO_SELECT"
        assert body.model_strategy == "stat"
        assert body.block_level is None
        assert body.recon_level is None
        assert body.lookback_days == 365


# ---------------------------------------------------------------------------
# _hierarchical_series_out — pure aggregate|leaf mapping
# ---------------------------------------------------------------------------


class _StubSeries:
    """Minimal stand-in for HierarchicalPersistedSeries — the mapping only
    reads these six attributes, so no DB / dataclass import is needed."""

    def __init__(self, kind, key, level, run_id, forecast_id, snapshot_id):
        self.kind = kind
        self.key = key
        self.level = level
        self.run_id = run_id
        self.forecast_id = forecast_id
        self.snapshot_id = snapshot_id


class TestHierarchicalSeriesOutMapping:
    def test_leaf_series_maps_faithfully(self):
        run_id, forecast_id, snapshot_id = uuid4(), uuid4(), uuid4()
        item_key = str(uuid4())
        out = _hierarchical_series_out(
            _StubSeries(
                kind="leaf",
                key=item_key,
                level=None,
                run_id=run_id,
                forecast_id=forecast_id,
                snapshot_id=snapshot_id,
            )
        )
        assert isinstance(out, HierarchicalSeriesOut)
        assert out.kind == "leaf"
        assert out.key == item_key
        assert out.level is None
        assert out.run_id == run_id
        assert out.forecast_id == forecast_id
        assert out.snapshot_id == snapshot_id

    def test_aggregate_series_maps_faithfully(self):
        run_id, forecast_id = uuid4(), uuid4()
        out = _hierarchical_series_out(
            _StubSeries(
                kind="aggregate",
                key="FAM-1",
                level="family",
                run_id=run_id,
                forecast_id=forecast_id,
                snapshot_id=None,
            )
        )
        assert out.kind == "aggregate"
        assert out.key == "FAM-1"
        assert out.level == "family"
        assert out.run_id == run_id
        assert out.forecast_id == forecast_id
        # Aggregates carry no snapshot (leaf-only snapshot contract).
        assert out.snapshot_id is None
