"""
test_scenario_compare.py — DB-free unit tests for SC-1 (GET /v1/scenarios/compare).

Two surfaces, no Postgres anywhere in this file:

  * The pure functions of ``engine/scenario/compare.py`` (parse / bounds /
    reference resolution / KPI coercions / stale / deltas / comparable) —
    deterministic math, called directly.
  * The router boundary of ``api/routers/scenarios.py`` (422 bounds, 422
    malformed/unknown id with a hand-authored message and no psycopg/DSN leak,
    401 auth-first, 503 kill switch checked before any DB touch, route
    registration BEFORE ``/{scenario_id}``) — TestClient with a MagicMock DB
    injected through ``get_db`` (the ``tests/test_atp_api.py`` /
    ``tests/test_m5_scenarios.py`` DB-free precedents; the CLAUDE.md "no mocks"
    rule is about DB-touching tests, which live in
    tests/integration/test_scenario_compare_integration.py — the real-SQL twin
    of every mocked payload assertion below).

Locked contracts (architect points 1-4):
  - fill_rate None-honest: zero/NULL total demand -> None with basis_count 0,
    NEVER a masked 1.0; a REAL 1.0 (demand > 0, zero stockouts) stays legal.
  - stock_value None-honest: no computed PI coordinate -> None, never 0.0;
    unpriced coordinates contribute 0 and are counted in unpriced_count.
  - deltas are entry - reference (signed), reference = baseline if requested
    else the FIRST id passed.
  - comparable = every entry computable AND stale is literally False (the
    ``stale=None`` of a non-computable entry must not pass a ``not stale``).
  - stale = merge-after-calc OR latest own calc_run 'completed_stale'; no
    merge ever -> False.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.scenario.compare import (
    BASELINE_SCENARIO_ID,
    COST_PRECEDENCE,
    MAX_SCENARIO_IDS,
    MIN_SCENARIO_IDS,
    ScenarioCompareEntry,
    ScenarioCompareError,
    ScenarioKpis,
    compute_comparable,
    compute_deltas,
    compute_fill_rate,
    compute_shortage_kpis,
    compute_stale,
    compute_stock_value,
    parse_scenario_ids,
    resolve_reference_scenario_id,
    validate_id_count,
)

# create_app() validates OOTILS_API_TOKEN at construction (_expected_token) —
# set it before the app import chain (same pattern as tests/test_atp_api.py).
os.environ.setdefault("OOTILS_API_TOKEN", "test-token")

from fastapi.testclient import TestClient  # noqa: E402

from ootils_core.api.app import create_app  # noqa: E402
from ootils_core.api.dependencies import get_db  # noqa: E402

_TOKEN = os.environ["OOTILS_API_TOKEN"]
AUTH = {"Authorization": f"Bearer {_TOKEN}"}

T0 = datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _kpis(
    *,
    shortage_count: int = 0,
    below_safety_stock_count: int = 0,
    shortage_severity_usd: float = 0.0,
    stock_value_usd: Optional[float] = None,
    stock_value_basis_count: int = 0,
    stock_value_unpriced_count: int = 0,
    fill_rate_est: Optional[float] = None,
    fill_rate_basis_count: int = 0,
) -> ScenarioKpis:
    return ScenarioKpis(
        shortage_count=shortage_count,
        below_safety_stock_count=below_safety_stock_count,
        shortage_severity_usd=shortage_severity_usd,
        stock_value_usd=stock_value_usd,
        stock_value_basis_count=stock_value_basis_count,
        stock_value_unpriced_count=stock_value_unpriced_count,
        fill_rate_est=fill_rate_est,
        fill_rate_basis_count=fill_rate_basis_count,
    )


def _entry(
    *,
    computable: bool = True,
    stale: Optional[bool] = False,
    kpis: Optional[ScenarioKpis] = None,
) -> ScenarioCompareEntry:
    return ScenarioCompareEntry(
        scenario_id=uuid4(),
        name="fork",
        status="active",
        parent_scenario_id=BASELINE_SCENARIO_ID,
        calc_run_id=uuid4() if computable else None,
        computed_at=T0 if computable else None,
        stale=stale,
        computable=computable,
        note=None if computable else "No completed calc_run found",
        kpis=kpis if computable else None,
    )


# ===========================================================================
# parse_scenario_ids
# ===========================================================================


class TestParseScenarioIds:
    def test_parses_comma_separated_uuids_preserving_order(self):
        a, b, c = uuid4(), uuid4(), uuid4()
        assert parse_scenario_ids(f"{a},{b},{c}") == [a, b, c]

    def test_strips_whitespace_around_tokens(self):
        a, b = uuid4(), uuid4()
        assert parse_scenario_ids(f"  {a} , {b}  ") == [a, b]

    def test_preserves_duplicates_and_does_not_bound_check(self):
        """Dedup and count bounds belong to validate_id_count, not the parser —
        a single id or a duplicate pair must parse cleanly."""
        a = uuid4()
        assert parse_scenario_ids(str(a)) == [a]
        assert parse_scenario_ids(f"{a},{a}") == [a, a]

    def test_empty_token_raises_hand_authored_error(self):
        a = uuid4()
        with pytest.raises(ScenarioCompareError) as exc:
            parse_scenario_ids(f"{a},,{uuid4()}")
        assert "empty scenario id" in exc.value.detail

    def test_trailing_comma_is_an_empty_token(self):
        with pytest.raises(ScenarioCompareError):
            parse_scenario_ids(f"{uuid4()},")

    def test_empty_string_is_an_empty_token(self):
        with pytest.raises(ScenarioCompareError):
            parse_scenario_ids("")

    def test_malformed_token_error_names_the_exact_token(self):
        """Contract point 4: the 422 names the offending id — never a generic
        'invalid input'."""
        with pytest.raises(ScenarioCompareError) as exc:
            parse_scenario_ids(f"{uuid4()},not-a-uuid")
        assert "'not-a-uuid'" in exc.value.detail

    def test_error_detail_attribute_is_the_message(self):
        err = ScenarioCompareError("hand-authored")
        assert err.detail == "hand-authored"
        assert str(err) == "hand-authored"


# ===========================================================================
# validate_id_count
# ===========================================================================


class TestValidateIdCount:
    def test_bounds_are_2_and_5(self):
        assert MIN_SCENARIO_IDS == 2
        assert MAX_SCENARIO_IDS == 5

    @pytest.mark.parametrize("n", [2, 3, 5])
    def test_accepts_in_range(self, n):
        validate_id_count([uuid4() for _ in range(n)])  # must not raise

    @pytest.mark.parametrize("n", [0, 1, 6])
    def test_rejects_out_of_range_naming_the_count(self, n):
        with pytest.raises(ScenarioCompareError) as exc:
            validate_id_count([uuid4() for _ in range(n)])
        assert "between 2 and 5" in exc.value.detail
        assert f"got {n}" in exc.value.detail


# ===========================================================================
# resolve_reference_scenario_id
# ===========================================================================


class TestResolveReference:
    def test_first_id_when_baseline_absent(self):
        a, b = uuid4(), uuid4()
        assert resolve_reference_scenario_id([a, b]) == a

    def test_baseline_wins_even_when_passed_last(self):
        a, b = uuid4(), uuid4()
        assert (
            resolve_reference_scenario_id([a, b, BASELINE_SCENARIO_ID])
            == BASELINE_SCENARIO_ID
        )

    def test_baseline_sentinel_matches_migration_002_seed(self):
        assert BASELINE_SCENARIO_ID == UUID("00000000-0000-0000-0000-000000000001")


# ===========================================================================
# compute_shortage_kpis — 0-honest type-coercion boundary
# ===========================================================================


class TestComputeShortageKpis:
    def test_coerces_decimal_severity_to_float(self):
        count, below, usd = compute_shortage_kpis(3, 2, Decimal("123.45"))
        assert (count, below) == (3, 2)
        assert isinstance(usd, float) and usd == pytest.approx(123.45)

    def test_zero_shortages_is_a_real_zero_not_none(self):
        """0-honest, not None-honest: a healthy scenario really has 0 shortages."""
        assert compute_shortage_kpis(0, 0, Decimal("0")) == (0, 0, 0.0)


# ===========================================================================
# compute_stock_value — None-honest, per-bucket average, unpriced_count
# ===========================================================================


class TestComputeStockValue:
    def test_averages_total_over_bucket_count(self):
        value, basis, unpriced = compute_stock_value(Decimal("120"), 3, 3, 0)
        assert value == pytest.approx(40.0)
        assert (basis, unpriced) == (3, 0)

    def test_no_computed_coordinate_is_none_never_zero(self):
        """An un-propagated fork (0 PI coordinates with a computed
        closing_stock) -> None, never a masked $0."""
        assert compute_stock_value(None, 0, 0, 0) == (None, 0, 0)

    def test_zero_bucket_count_is_defensively_none(self):
        assert compute_stock_value(Decimal("50"), 0, 5, 2) == (None, 5, 2)

    def test_all_unpriced_basis_is_an_honest_zero_dollars(self):
        """Coordinates EXIST but every unit_cost resolved NULL: the $ figure is
        a real 0 (they contribute 0), surfaced by unpriced_count == basis —
        distinct from the no-data None above."""
        value, basis, unpriced = compute_stock_value(Decimal("0"), 1, 2, 2)
        assert value == 0.0
        assert basis == unpriced == 2

    def test_none_total_with_existing_basis_falls_back_to_zero(self):
        value, basis, unpriced = compute_stock_value(None, 2, 3, 3)
        assert value == 0.0
        assert (basis, unpriced) == (3, 3)

    def test_unpriced_count_passes_through_unchanged(self):
        _, basis, unpriced = compute_stock_value(Decimal("10"), 1, 4, 1)
        assert (basis, unpriced) == (4, 1)


# ===========================================================================
# compute_fill_rate — None-honest, never a masked 1.0
# ===========================================================================


class TestComputeFillRate:
    def test_typical_fill_rate(self):
        rate, basis = compute_fill_rate(Decimal("100"), Decimal("40"), 3)
        assert rate == pytest.approx(0.6)
        assert basis == 3

    def test_zero_demand_is_none_with_basis_zero_never_one(self):
        """THE contract point 1c assertion: demande=0 -> None + basis_count=0,
        JAMAIS 1.0."""
        rate, basis = compute_fill_rate(Decimal("0"), Decimal("0"), 0)
        assert rate is None and rate != 1.0
        assert basis == 0

    def test_null_demand_is_none(self):
        assert compute_fill_rate(None, None, 0) == (None, 0)

    def test_none_branch_forces_basis_to_zero_even_on_malformed_input(self):
        """The basis_count is derived-0 in the None branch regardless of what
        the caller passed — the None trigger and its basis can never diverge."""
        rate, basis = compute_fill_rate(Decimal("0"), Decimal("5"), 4)
        assert rate is None
        assert basis == 0

    def test_negative_demand_is_defensively_none(self):
        assert compute_fill_rate(Decimal("-10"), Decimal("0"), 1) == (None, 0)

    def test_real_one_point_zero_is_legal_when_demand_exists(self):
        """A genuine perfect fill (demand > 0, zero stockouts) IS 1.0 — the
        None-honesty is only about the zero-demand denominator."""
        rate, basis = compute_fill_rate(Decimal("100"), Decimal("0"), 3)
        assert rate == pytest.approx(1.0)
        assert basis == 3, "basis > 0 distinguishes a real 1.0 from a masked one"

    def test_null_stockout_counts_as_zero(self):
        rate, basis = compute_fill_rate(Decimal("100"), None, 3)
        assert rate == pytest.approx(1.0)
        assert basis == 3

    def test_stockout_exceeding_demand_goes_negative_unclamped(self):
        rate, _ = compute_fill_rate(Decimal("100"), Decimal("150"), 2)
        assert rate == pytest.approx(-0.5), "honest overshoot, never clamped to 0"


# ===========================================================================
# compute_stale — merge-after-calc OR own latest run 'completed_stale'
# ===========================================================================


class TestComputeStale:
    def test_no_merge_ever_is_false(self):
        """Contract point 2: aucun merge -> stale=false."""
        assert compute_stale(None, T0, "completed") is False

    def test_merge_before_calc_is_false(self):
        assert compute_stale(T0 - timedelta(hours=1), T0, "completed") is False

    def test_merge_after_calc_is_true(self):
        assert compute_stale(T0 + timedelta(hours=1), T0, "completed") is True

    def test_merge_without_completed_at_is_false(self):
        """A merge exists but the KPI run has no completed_at — the comparison
        cannot honestly trip, both operands are required."""
        assert compute_stale(T0, None, "completed") is False

    def test_completed_stale_status_alone_is_true(self):
        """The OR-branch: no merge event at all, but the fork's OWN latest
        calc_run (any status, the second independent query) is
        'completed_stale'."""
        assert compute_stale(None, T0, "completed_stale") is True

    def test_other_statuses_do_not_trip(self):
        for status in ("completed", "running", "failed", "pending", None):
            assert compute_stale(None, T0, status) is False

    def test_both_triggers_is_true(self):
        assert compute_stale(T0 + timedelta(hours=1), T0, "completed_stale") is True


# ===========================================================================
# compute_deltas — entry - reference, signed, None-propagating
# ===========================================================================

KPI_A = _kpis(
    shortage_count=5,
    below_safety_stock_count=2,
    shortage_severity_usd=450.0,
    stock_value_usd=40.0,
    stock_value_basis_count=3,
    fill_rate_est=0.6,
    fill_rate_basis_count=3,
)
KPI_B = _kpis(
    shortage_count=4,
    below_safety_stock_count=1,
    shortage_severity_usd=30.0,
    stock_value_usd=100.0,
    stock_value_basis_count=3,
    fill_rate_est=1.0,
    fill_rate_basis_count=3,
)


class TestComputeDeltas:
    def test_signed_deltas_are_entry_minus_reference(self):
        d = compute_deltas(KPI_B, KPI_A)
        assert d is not None
        assert d.shortage_count_delta == -1
        assert d.severity_usd_delta == pytest.approx(-420.0)
        assert d.stock_value_usd_delta == pytest.approx(60.0)
        assert d.fill_rate_delta == pytest.approx(0.4)

    def test_signs_flip_when_operands_swap(self):
        d = compute_deltas(KPI_A, KPI_B)
        assert d is not None
        assert d.shortage_count_delta == 1
        assert d.severity_usd_delta == pytest.approx(420.0)
        assert d.stock_value_usd_delta == pytest.approx(-60.0)
        assert d.fill_rate_delta == pytest.approx(-0.4)

    def test_reference_vs_itself_is_all_zeros(self):
        d = compute_deltas(KPI_A, KPI_A)
        assert d is not None
        assert d.shortage_count_delta == 0
        assert d.severity_usd_delta == 0.0
        assert d.stock_value_usd_delta == 0.0
        assert d.fill_rate_delta == 0.0

    def test_none_when_entry_kpis_missing(self):
        assert compute_deltas(None, KPI_A) is None

    def test_none_when_reference_kpis_missing(self):
        assert compute_deltas(KPI_B, None) is None

    def test_none_stock_value_on_one_side_nulls_only_that_delta(self):
        entry = _kpis(shortage_count=1, shortage_severity_usd=10.0,
                      stock_value_usd=None, fill_rate_est=0.9)
        d = compute_deltas(entry, KPI_A)
        assert d is not None
        assert d.stock_value_usd_delta is None
        assert d.shortage_count_delta == -4
        assert d.fill_rate_delta == pytest.approx(0.3)

    def test_none_fill_rate_on_reference_side_nulls_only_that_delta(self):
        ref = _kpis(shortage_count=1, stock_value_usd=10.0, fill_rate_est=None)
        d = compute_deltas(KPI_B, ref)
        assert d is not None
        assert d.fill_rate_delta is None
        assert d.stock_value_usd_delta == pytest.approx(90.0)


# ===========================================================================
# compute_comparable — all(computable AND stale is False)
# ===========================================================================


class TestComputeComparable:
    def test_all_computable_and_fresh_is_true(self):
        assert compute_comparable([_entry(), _entry()]) is True

    def test_one_stale_entry_breaks_comparability(self):
        assert compute_comparable([_entry(), _entry(stale=True)]) is False

    def test_non_computable_entry_breaks_comparability_the_none_trap(self):
        """A non-computable entry carries stale=None; ``not None`` is truthy, so
        a naive all(not e.stale) would wrongly say comparable — the literal
        ``stale is False`` requirement must catch it."""
        entries = [_entry(), _entry(computable=False, stale=None)]
        assert all(not e.stale for e in entries) is True  # the trap, documented
        assert compute_comparable(entries) is False

    def test_computable_with_stale_none_is_not_comparable(self):
        assert compute_comparable([_entry(), _entry(stale=None)]) is False


# ===========================================================================
# COST_PRECEDENCE — the citation the top-level payload must carry
# ===========================================================================


def test_cost_precedence_cites_the_mirrored_source_and_null_honesty():
    assert "propagator_sql.py:262-274" in COST_PRECEDENCE
    assert "standard_cost" in COST_PRECEDENCE
    assert "NULL-honest" in COST_PRECEDENCE


# ===========================================================================
# Router boundary — TestClient, MagicMock DB (never touched on 4xx paths)
# ===========================================================================


def _make_client(db_mock: MagicMock) -> TestClient:
    app = create_app()

    def override_db():
        yield db_mock

    app.dependency_overrides[get_db] = override_db
    # No context manager: lifespan (and any real DB bootstrap) must not run.
    return TestClient(app)


def _cursor(*, fetchone=None, fetchall=None):
    cur = MagicMock()
    cur.fetchone.return_value = fetchone
    cur.fetchall.return_value = fetchall if fetchall is not None else []
    return cur


def _scenario_row(scenario_id, name="fork", status="active", parent=BASELINE_SCENARIO_ID):
    return {
        "scenario_id": scenario_id,
        "name": name,
        "status": status,
        "is_baseline": False,
        "parent_scenario_id": parent,
    }


class TestCompareEndpointValidation:
    def test_unauthenticated_is_401(self):
        db = MagicMock()
        client = _make_client(db)
        resp = client.get(f"/v1/scenarios/compare?ids={uuid4()},{uuid4()}")
        assert resp.status_code == 401
        db.execute.assert_not_called()

    def test_single_id_is_422_naming_bounds_without_db(self):
        db = MagicMock()
        client = _make_client(db)
        resp = client.get(f"/v1/scenarios/compare?ids={uuid4()}", headers=AUTH)
        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert isinstance(detail, str), "hand-authored message, not a Pydantic list"
        assert "between 2 and 5" in detail and "got 1" in detail
        db.execute.assert_not_called()

    def test_six_ids_is_422(self):
        db = MagicMock()
        client = _make_client(db)
        ids = ",".join(str(uuid4()) for _ in range(6))
        resp = client.get(f"/v1/scenarios/compare?ids={ids}", headers=AUTH)
        assert resp.status_code == 422
        assert "got 6" in resp.json()["detail"]
        db.execute.assert_not_called()

    def test_malformed_id_is_422_naming_the_token_no_leak(self):
        """Also the route-ordering proof: if /{scenario_id} had captured the
        request, the 422 would be Starlette's uuid_parsing LIST for the PATH
        param — not our hand-authored STRING naming the query token."""
        db = MagicMock()
        client = _make_client(db)
        resp = client.get(
            f"/v1/scenarios/compare?ids={uuid4()},not-a-uuid", headers=AUTH
        )
        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert isinstance(detail, str)
        assert "'not-a-uuid'" in detail
        assert "psycopg" not in resp.text.lower()
        assert "dsn" not in resp.text.lower()
        db.execute.assert_not_called()

    def test_empty_ids_param_is_422(self):
        db = MagicMock()
        client = _make_client(db)
        resp = client.get("/v1/scenarios/compare?ids=", headers=AUTH)
        assert resp.status_code == 422
        assert "empty scenario id" in resp.json()["detail"]

    def test_missing_ids_param_is_fastapi_422_on_the_query_param(self):
        """Route ordering, second proof: the validation error is about the
        REQUIRED QUERY param 'ids' — not a uuid_parsing failure on a
        {scenario_id} PATH param (which is what a shadowed route would give)."""
        db = MagicMock()
        client = _make_client(db)
        resp = client.get("/v1/scenarios/compare", headers=AUTH)
        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert isinstance(detail, list)
        assert detail[0]["loc"] == ["query", "ids"]

    def test_unknown_id_is_422_naming_the_id(self):
        """Existence is the one validation that needs the DB: the mock returns
        a row for A only -> the request fails naming the missing id."""
        a, missing = uuid4(), uuid4()
        db = MagicMock()
        db.execute.return_value = _cursor(fetchall=[_scenario_row(a)])
        client = _make_client(db)
        resp = client.get(f"/v1/scenarios/compare?ids={a},{missing}", headers=AUTH)
        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert str(missing) in detail
        assert "psycopg" not in resp.text.lower()


class TestCompareEndpointKillSwitch:
    @pytest.mark.parametrize("value", ["0", "false", "off", "no"])
    def test_falsy_switch_is_503_before_any_db_touch(self, value, monkeypatch):
        monkeypatch.setenv("OOTILS_SCENARIO_COMPARE_ENABLED", value)
        db = MagicMock()
        client = _make_client(db)
        resp = client.get(
            f"/v1/scenarios/compare?ids={uuid4()},{uuid4()}", headers=AUTH
        )
        assert resp.status_code == 503
        assert "OOTILS_SCENARIO_COMPARE_ENABLED" in resp.json()["detail"]
        db.execute.assert_not_called()

    def test_default_is_on(self, monkeypatch):
        """No env var at all -> the gate is open (the request proceeds to
        validation and fails 422 on bounds, NOT 503)."""
        monkeypatch.delenv("OOTILS_SCENARIO_COMPARE_ENABLED", raising=False)
        client = _make_client(MagicMock())
        resp = client.get(f"/v1/scenarios/compare?ids={uuid4()}", headers=AUTH)
        assert resp.status_code == 422

    def test_auth_still_outranks_the_switch(self, monkeypatch):
        """Kill switch off + no credentials -> 401, not 503: an unauthenticated
        caller must not be able to probe the switch state."""
        monkeypatch.setenv("OOTILS_SCENARIO_COMPARE_ENABLED", "0")
        client = _make_client(MagicMock())
        resp = client.get(f"/v1/scenarios/compare?ids={uuid4()},{uuid4()}")
        assert resp.status_code == 401


# ===========================================================================
# Router full path — mocked cursor sequence (payload wiring; the real-SQL
# equivalents live in tests/integration/test_scenario_compare_integration.py)
# ===========================================================================


def _computable_cursors(
    calc_run_id,
    *,
    completed_at=T0,
    latest_status="completed",
    latest_merge_at=None,
    shortage_row=None,
    stock_row=None,
    fill_row=None,
):
    """The exact 7-query sequence _build_entry issues for one computable
    scenario, in code order: _latest_calc_run, completed_at, latest status
    (any), latest baseline merge, shortage KPIs, stock-value KPIs, fill-rate
    denominator."""
    return [
        _cursor(fetchone={"calc_run_id": calc_run_id}),
        _cursor(fetchone={"completed_at": completed_at}),
        _cursor(fetchone={"status": latest_status}),
        _cursor(fetchone={"latest_merge_at": latest_merge_at}),
        _cursor(fetchone=shortage_row or {
            "shortage_count": 0,
            "below_safety_stock_count": 0,
            "shortage_severity_usd": Decimal("0"),
            "stockout_qty_total": Decimal("0"),
        }),
        _cursor(fetchone=stock_row or {
            "coordinate_count": 0,
            "unpriced_count": 0,
            "bucket_count": 0,
            "total_value": None,
        }),
        _cursor(fetchone=fill_row or {
            "outflows_total": Decimal("0"),
            "demand_bucket_count": 0,
        }),
    ]


def test_full_payload_two_computable_scenarios_signed_deltas():
    """End-to-end through the router with a scripted cursor sequence: entries in
    request order, KPIs converted, deltas = entry - reference (reference = the
    FIRST id, baseline absent), comparable True, cost_precedence cited."""
    id_a, id_b = uuid4(), uuid4()
    run_a, run_b = uuid4(), uuid4()

    db = MagicMock()
    db.execute.side_effect = [
        _cursor(fetchall=[_scenario_row(id_a, name="Fork A"),
                          _scenario_row(id_b, name="Fork B")]),
        # Entry A: 1 stockout + 1 below_safety, $450, stock 120/3, demand 100.
        *_computable_cursors(
            run_a,
            shortage_row={
                "shortage_count": 1,
                "below_safety_stock_count": 1,
                "shortage_severity_usd": Decimal("450"),
                "stockout_qty_total": Decimal("40"),
            },
            stock_row={
                "coordinate_count": 3,
                "unpriced_count": 0,
                "bucket_count": 3,
                "total_value": Decimal("120"),
            },
            fill_row={"outflows_total": Decimal("100"), "demand_bucket_count": 3},
        ),
        # Entry B: healthier — 0 stockouts, $30, stock 300/3, same demand.
        *_computable_cursors(
            run_b,
            shortage_row={
                "shortage_count": 0,
                "below_safety_stock_count": 1,
                "shortage_severity_usd": Decimal("30"),
                "stockout_qty_total": Decimal("0"),
            },
            stock_row={
                "coordinate_count": 3,
                "unpriced_count": 0,
                "bucket_count": 3,
                "total_value": Decimal("300"),
            },
            fill_row={"outflows_total": Decimal("100"), "demand_bucket_count": 3},
        ),
    ]

    client = _make_client(db)
    resp = client.get(f"/v1/scenarios/compare?ids={id_a},{id_b}", headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert [e["scenario_id"] for e in body["entries"]] == [str(id_a), str(id_b)]
    assert body["reference_scenario_id"] == str(id_a)
    assert body["comparable"] is True
    assert "propagator_sql.py:262-274" in body["cost_precedence"]

    entry_a, entry_b = body["entries"]
    assert entry_a["computable"] is True
    assert entry_a["stale"] is False
    assert entry_a["calc_run_id"] == str(run_a)
    assert entry_a["computed_at"] is not None
    assert entry_a["parent_scenario_id"] == str(BASELINE_SCENARIO_ID)
    assert entry_a["kpis"]["shortage_count"] == 1
    assert entry_a["kpis"]["below_safety_stock_count"] == 1
    assert entry_a["kpis"]["shortage_severity_usd"] == pytest.approx(450.0)
    assert entry_a["kpis"]["stock_value_usd"] == pytest.approx(40.0)
    assert entry_a["kpis"]["fill_rate_est"] == pytest.approx(0.6)
    assert entry_a["kpis"]["fill_rate_basis_count"] == 3

    assert entry_b["kpis"]["shortage_count"] == 0
    assert entry_b["kpis"]["stock_value_usd"] == pytest.approx(100.0)
    assert entry_b["kpis"]["fill_rate_est"] == pytest.approx(1.0)

    # Reference vs itself: hard zeros. B vs A: signed improvements.
    assert entry_a["deltas"] == {
        "shortage_count_delta": 0,
        "severity_usd_delta": 0.0,
        "stock_value_usd_delta": 0.0,
        "fill_rate_delta": 0.0,
    }
    db_ = entry_b["deltas"]
    assert db_["shortage_count_delta"] == -1
    assert db_["severity_usd_delta"] == pytest.approx(-420.0)
    assert db_["stock_value_usd_delta"] == pytest.approx(60.0)
    assert db_["fill_rate_delta"] == pytest.approx(0.4)


def test_full_payload_non_computable_entry_kpis_null_computable_false():
    """A scenario with no completed calc_run (ValueError from
    _latest_calc_run, caught PER scenario): its entry is present with
    kpis/deltas/stale/calc_run_id all null, computable=false and a note — the
    other entry is untouched, comparable=false."""
    id_a, id_b = uuid4(), uuid4()
    run_a = uuid4()

    db = MagicMock()
    db.execute.side_effect = [
        _cursor(fetchall=[_scenario_row(id_a), _scenario_row(id_b, name="empty")]),
        *_computable_cursors(run_a),
        # Entry B: _latest_calc_run finds nothing -> ValueError -> early return.
        _cursor(fetchone=None),
    ]

    client = _make_client(db)
    resp = client.get(f"/v1/scenarios/compare?ids={id_a},{id_b}", headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["comparable"] is False, "a non-computable entry blocks comparability"
    entry_a, entry_b = body["entries"]

    assert entry_a["computable"] is True
    assert entry_a["deltas"] is not None  # reference A still deltas vs itself

    assert entry_b["computable"] is False
    assert entry_b["kpis"] is None
    assert entry_b["deltas"] is None
    assert entry_b["stale"] is None
    assert entry_b["calc_run_id"] is None
    assert entry_b["computed_at"] is None
    assert "No completed calc_run" in entry_b["note"]
    assert str(id_b) in entry_b["note"]
