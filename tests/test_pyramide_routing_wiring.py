"""
Unit tests for the DEM-1 head/tail routing WIRING (PR-1): the DB-side feature
builders (build_series_features / build_routing_decisions) and the opt-in
auto_route plumbing on both Pyramide endpoints.

DB-free by construction: the feature builders are exercised against a tiny fake
connection that serves canned rows, build_routing_decisions against a
hand-built summing block, and the endpoints against the no-DB TestClient with
every repository/runner seam monkeypatched. The routing MATH itself lives in
tests/test_pyramide_routing.py (routing.py is pure); here we only assert the
wiring: features arithmetic, series->decision mapping, method-override policy,
and that provenance reaches persist. Integration (routed_* non-NULL on the
seeded dataset) is the test-writer's job.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import status
from fastapi.testclient import TestClient

os.environ["OOTILS_API_TOKEN"] = "test-token"

from ootils_core.api.app import create_app
from ootils_core.api.dependencies import get_db
from ootils_core.api.routers import pyramide as pyramide_router
from ootils_core.pyramide import repository as repo
from ootils_core.pyramide.hierarchy import summing as summing_mod
from ootils_core.pyramide.hierarchy.summing import (
    AGGREGATE,
    LEAF,
    SeriesRef,
    SummingBlock,
)
from ootils_core.pyramide.routing import RoutingDecision, SeriesFeatures

D = Decimal
AUTH = {"Authorization": "Bearer test-token"}


# ---------------------------------------------------------------------------
# Fake connection for the feature builders
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FeatureConn:
    """Routes execute() to canned rows by inspecting the SQL: demand_history
    daily series vs item_asp annual value. Records every SQL seen so the
    site-filter presence can be asserted."""

    def __init__(self, *, daily=(), annual=None):
        self._daily = list(daily)
        self._annual = annual
        self.sql_seen: list[str] = []

    def execute(self, sql, params=None):
        self.sql_seen.append(sql)
        if "FROM item_asp" in sql:
            return _FakeCursor([self._annual] if self._annual is not None else [])
        if "FROM demand_history" in sql:
            return _FakeCursor(self._daily)
        raise AssertionError(f"unexpected query: {sql[:80]}")


# ---------------------------------------------------------------------------
# build_series_features
# ---------------------------------------------------------------------------


def test_build_series_features_zero_ratio_and_span():
    # 4 booking days over a 10-day span (Jan1..Jan10) -> zero_ratio 6/10.
    daily = [
        {"demand_date": date(2025, 1, 1), "total_qty": 5},
        {"demand_date": date(2025, 1, 3), "total_qty": 2},
        {"demand_date": date(2025, 1, 7), "total_qty": 1},
        {"demand_date": date(2025, 1, 10), "total_qty": 4},
    ]
    conn = _FeatureConn(daily=daily, annual={"annual_value": D("150000")})
    feats = repo.build_series_features(conn, uuid4(), None)

    assert feats.history_depth_days == 10
    assert feats.zero_ratio == D(6) / D(10)
    assert feats.annual_value == D("150000")
    assert feats.lifecycle is None
    assert feats.has_twin is False
    assert feats.aggregate_signal_ok is False


def test_build_series_features_annual_value_fallback_none():
    daily = [{"demand_date": date(2025, 1, 1), "total_qty": 3}]
    # No item_asp row at all -> None (documented fallback), never a masked 0.
    conn_no_row = _FeatureConn(daily=daily, annual=None)
    assert repo.build_series_features(conn_no_row, uuid4(), None).annual_value is None
    # Row exists but value_12m NULL -> still None (SUM over NULLs is NULL).
    conn_null = _FeatureConn(daily=daily, annual={"annual_value": None})
    assert repo.build_series_features(conn_null, uuid4(), None).annual_value is None


def test_build_series_features_empty_history_is_cold_start_shaped():
    conn = _FeatureConn(daily=[], annual={"annual_value": D("9999")})
    feats = repo.build_series_features(conn, uuid4(), None)

    assert feats.history_depth_days == 0
    assert feats.zero_ratio == D(0)
    assert feats.seasonal_strength is None
    assert feats.annual_value == D("9999")


def test_build_series_features_site_filter_only_with_location():
    conn_sited = _FeatureConn(daily=[], annual=None)
    repo.build_series_features(conn_sited, uuid4(), uuid4())
    dh_sql = next(s for s in conn_sited.sql_seen if "FROM demand_history" in s)
    # _warehouse_codes_subquery() is embedded (alias-aware) only when a site
    # filter intervenes.
    assert "location_aliases" in dh_sql

    conn_all_sites = _FeatureConn(daily=[], annual=None)
    repo.build_series_features(conn_all_sites, uuid4(), None)
    dh_sql_all = next(s for s in conn_all_sites.sql_seen if "FROM demand_history" in s)
    assert "location_aliases" not in dh_sql_all


def test_build_series_features_aggregate_signal_ok_passthrough():
    conn = _FeatureConn(daily=[], annual=None)
    feats = repo.build_series_features(conn, uuid4(), None, aggregate_signal_ok=True)
    assert feats.aggregate_signal_ok is True


# ---------------------------------------------------------------------------
# build_routing_decisions
# ---------------------------------------------------------------------------


def _one_block(item_key: str) -> SummingBlock:
    return SummingBlock(
        hierarchy_id="H",
        block_code="ROOT",
        block_level="family",
        series=(
            SeriesRef(kind=AGGREGATE, key="ROOT", level="family"),
            SeriesRef(kind=LEAF, key=item_key, leaf_code="ROOT"),
        ),
        leaves=(item_key,),
        rows=((0,), (0,)),
    )


def test_build_routing_decisions_maps_every_series(monkeypatch):
    item_key = str(uuid4())
    monkeypatch.setattr(
        summing_mod, "load_summing_blocks", lambda db, **k: [_one_block(item_key)]
    )

    node_feats = SeriesFeatures(
        history_depth_days=800,
        zero_ratio=D("0.1"),
        abc_class="A",
        seasonal_strength=D("0.4"),
    )
    leaf_call: dict[str, object] = {}

    def fake_node(db, hid, code, *, aggregate_signal_ok=False):
        return node_feats

    def fake_leaf(db, iid, loc, *, aggregate_signal_ok=False):
        leaf_call["aggregate_signal_ok"] = aggregate_signal_ok
        leaf_call["location_id"] = loc
        return SeriesFeatures(
            history_depth_days=30,
            zero_ratio=D("0.1"),
            abc_class="C",
            aggregate_signal_ok=aggregate_signal_ok,
        )

    monkeypatch.setattr(repo, "_build_node_features", fake_node)
    monkeypatch.setattr(repo, "build_series_features", fake_leaf)

    decisions = repo.build_routing_decisions(
        _FeatureConn(), hierarchy_id="H", block_code="ROOT"
    )

    assert set(decisions) == {"ROOT", item_key}
    for decision in decisions.values():
        assert isinstance(decision, RoutingDecision)
        assert decision.method
        assert decision.reason  # non-empty auditable sentence
    # Leaves route site-agnostic (location None) with a signal-bearing parent.
    assert leaf_call["aggregate_signal_ok"] is True
    assert leaf_call["location_id"] is None


def test_build_routing_decisions_block_not_found_raises(monkeypatch):
    monkeypatch.setattr(summing_mod, "load_summing_blocks", lambda db, **k: [])
    with pytest.raises(ValueError):
        repo.build_routing_decisions(
            _FeatureConn(), hierarchy_id="H", block_code="MISSING"
        )


# ---------------------------------------------------------------------------
# Endpoint wiring — shared harness
# ---------------------------------------------------------------------------


def _client() -> TestClient:
    app = create_app()

    def _override_db():
        yield object()

    app.dependency_overrides[get_db] = _override_db
    return TestClient(app, raise_server_exceptions=False)


def _fake_summary(**over) -> SimpleNamespace:
    base = dict(
        run_id=uuid4(),
        snapshot_id=uuid4(),
        forecast_id=uuid4(),
        status="generated",
        item_id=uuid4(),
        location_id=uuid4(),
        hierarchy_id=None,
        level=None,
        node_code=None,
        scenario_id=uuid4(),
        horizon_start=date(2025, 1, 1),
        horizon_end=date(2025, 3, 31),
        granularity="daily",
        method="AUTO_SELECT",
        model_strategy="stat",
        recon_method="none",
        random_seed=0,
        code_version="local",
        selected_model="ETS",
        engine_backend="internal",
        source_history_count=10,
        value_count=3,
        total_quantity=D("30"),
        deterministic_artifact="forecast_values",
        created_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        committed_at=None,
        stale_demand=False,
        routed_method=None,
        routed_level=None,
        routing_reason=None,
    )
    base.update(over)
    return SimpleNamespace(**base)


_HEAD_FEATS = SeriesFeatures(
    history_depth_days=800,
    zero_ratio=D("0.05"),
    abc_class="A",
    seasonal_strength=D("0.4"),
)  # routes to SEASONAL, leaf


def _wire_single_series(monkeypatch) -> dict:
    """Stub every seam of create_pyramide_run; return capture dict."""
    captured: dict[str, object] = {"features_called": 0}

    class _FakeRunner:
        def run(self, config, history):
            captured["config"] = config
            return SimpleNamespace()

    def fake_features(db, item_id, location_id, *, aggregate_signal_ok=False):
        captured["features_called"] = int(captured["features_called"]) + 1
        return _HEAD_FEATS

    def fake_persist(db, result, *, stale_demand=False, routing=None, history=None):
        captured["routing"] = routing
        return SimpleNamespace(
            run_id=uuid4(), snapshot_id=uuid4(), forecast_id=uuid4()
        )

    monkeypatch.setattr(pyramide_router, "resolve_item_uuid", lambda db, x: uuid4())
    monkeypatch.setattr(pyramide_router, "resolve_location_uuid", lambda db, x: uuid4())
    monkeypatch.setattr(
        pyramide_router,
        "get_historical_demand",
        lambda *, db, item_id, location_id, lookback_days, scenario_id: [D("1")],
    )
    monkeypatch.setattr(pyramide_router, "build_series_features", fake_features)
    monkeypatch.setattr(
        pyramide_router,
        "get_demand_freshness",
        lambda db, item_id: SimpleNamespace(ingest_age_days=1),
    )
    monkeypatch.setattr(pyramide_router, "persist_run", fake_persist)
    monkeypatch.setattr(
        pyramide_router, "fetch_run_summary", lambda db, run_id: _fake_summary()
    )
    monkeypatch.setattr(pyramide_router, "PyramideRunner", _FakeRunner)
    return captured


def _post_run(client: TestClient, **body) -> "TestClient.__class__":
    payload = {"item_id": "ITEM-1", "location_id": "LOC-1"}
    payload.update(body)
    return client.post("/v1/forecast/runs", json=payload, headers=AUTH)


# ---------------------------------------------------------------------------
# Single-series endpoint
# ---------------------------------------------------------------------------


def test_single_series_auto_route_off_is_byte_identical(monkeypatch):
    captured = _wire_single_series(monkeypatch)
    resp = _post_run(_client())  # auto_route defaults False

    assert resp.status_code == status.HTTP_201_CREATED
    assert captured["features_called"] == 0  # router never computes features
    assert captured["routing"] is None  # nothing handed to persist_run
    assert captured["config"].method == "AUTO_SELECT"  # method untouched


def test_single_series_auto_route_passes_routing_to_persist(monkeypatch):
    captured = _wire_single_series(monkeypatch)
    resp = _post_run(_client(), auto_route=True)

    assert resp.status_code == status.HTTP_201_CREATED
    assert captured["features_called"] == 1
    routing = captured["routing"]
    assert isinstance(routing, RoutingDecision)
    assert routing.level == "leaf"  # single-series is always leaf
    assert routing.reason


def test_single_series_auto_route_overrides_only_auto_select(monkeypatch):
    captured = _wire_single_series(monkeypatch)
    resp = _post_run(_client(), auto_route=True)  # method defaults AUTO_SELECT

    assert resp.status_code == status.HTTP_201_CREATED
    routing = captured["routing"]
    # An unopinionated AUTO_SELECT request runs the routed method.
    assert captured["config"].method == routing.method
    assert routing.method != "AUTO_SELECT"


def test_single_series_auto_route_never_overwrites_explicit_method(monkeypatch):
    captured = _wire_single_series(monkeypatch)
    resp = _post_run(_client(), auto_route=True, method="ENSEMBLE_STAT")

    assert resp.status_code == status.HTTP_201_CREATED
    # Explicit method survives; the routed recommendation is still recorded.
    assert captured["config"].method == "ENSEMBLE_STAT"
    assert isinstance(captured["routing"], RoutingDecision)
    assert captured["routing"].method != "ENSEMBLE_STAT"


# ---------------------------------------------------------------------------
# Hierarchical endpoint
# ---------------------------------------------------------------------------


def _wire_hierarchical(monkeypatch, canned) -> dict:
    captured: dict[str, object] = {"build_calls": 0}

    class _FakeHRunner:
        def run(self, db, config):
            captured["config"] = config
            return SimpleNamespace(
                hierarchy_id=config.hierarchy_id,
                block_code=config.block_code,
                block_level="family",
                recon_level="family",
                recon_method="middleout",
                horizon_start=config.horizon_start,
                horizon_end=config.horizon_end,
                granularity=config.granularity,
                method=config.method,
                persisted=(),
                warnings=(),
            )

    def fake_build(db, *, hierarchy_id, block_code, block_level=None, thresholds=None):
        captured["build_calls"] = int(captured["build_calls"]) + 1
        return canned

    monkeypatch.setattr(pyramide_router, "resolve_location_uuid", lambda db, x: uuid4())
    monkeypatch.setattr(pyramide_router, "build_routing_decisions", fake_build)
    monkeypatch.setattr(pyramide_router, "HierarchicalRunner", _FakeHRunner)
    return captured


def _post_hier(client: TestClient, **body) -> "TestClient.__class__":
    payload = {
        "hierarchy_id": "H",
        "block_code": "ROOT",
        "leaf_location_id": "LOC-1",
    }
    payload.update(body)
    return client.post("/v1/forecast/hierarchical-runs", json=payload, headers=AUTH)


def test_hierarchical_auto_route_off_injects_empty_decisions(monkeypatch):
    captured = _wire_hierarchical(monkeypatch, canned={"IGNORED": None})
    resp = _post_hier(_client())  # auto_route defaults False

    assert resp.status_code == status.HTTP_201_CREATED
    assert captured["build_calls"] == 0
    assert dict(captured["config"].routing_decisions) == {}


def test_hierarchical_auto_route_injects_built_decisions(monkeypatch):
    canned = {
        "ROOT": RoutingDecision(
            method="SEASONAL", level="aggregate", reason="node head"
        )
    }
    captured = _wire_hierarchical(monkeypatch, canned=canned)
    resp = _post_hier(_client(), auto_route=True)

    assert resp.status_code == status.HTTP_201_CREATED
    assert captured["build_calls"] == 1
    assert dict(captured["config"].routing_decisions) == canned
