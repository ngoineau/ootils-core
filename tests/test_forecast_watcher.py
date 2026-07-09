"""
test_forecast_watcher.py — pure unit tests for the DEM-1 Forecast Watcher
(scripts/agent_forecast_watcher.py).

No DB: the drift classifier, the relative-bias normalizer and the deterministic
uuid5 are pure functions; _upsert / _expire_stale_drafts are exercised against a
canned fake connection so the idempotent-upsert and supersede logic is asserted
in isolation. The end-to-end run (real accuracy metrics -> DRAFT rows + one
recommendation_created event; re-run -> zero new rows/events) is left to the
integration suite.
"""
from __future__ import annotations

import sys
import uuid
from decimal import Decimal
from pathlib import Path

# Import seam: agent_forecast_watcher lives under scripts/ (outside the package).
_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import agent_forecast_watcher as afw  # noqa: E402

_MASE_T = Decimal("1.3")
_BIAS_T = Decimal("0.3")


# ---------------------------------------------------------------------------
# classify_drift — the drift verdict
# ---------------------------------------------------------------------------
def test_mase_only_degraded() -> None:
    assert afw.classify_drift(Decimal("1.6"), Decimal("0.1"), _MASE_T, _BIAS_T) == "MASE_DEGRADED"


def test_bias_only_sustained() -> None:
    assert afw.classify_drift(Decimal("0.9"), Decimal("0.5"), _MASE_T, _BIAS_T) == "BIAS_SUSTAINED"


def test_both_drift() -> None:
    assert afw.classify_drift(Decimal("2.0"), Decimal("0.5"), _MASE_T, _BIAS_T) == "BOTH"


def test_under_both_thresholds_is_none() -> None:
    assert afw.classify_drift(Decimal("1.0"), Decimal("0.1"), _MASE_T, _BIAS_T) is None


def test_exact_threshold_does_not_trigger() -> None:
    # strict > : a metric equal to its threshold is NOT a drift.
    assert afw.classify_drift(_MASE_T, _BIAS_T, _MASE_T, _BIAS_T) is None


# None-honest: a NULL metric neither triggers nor blocks the other.
def test_none_mase_does_not_block_bias() -> None:
    assert afw.classify_drift(None, Decimal("0.5"), _MASE_T, _BIAS_T) == "BIAS_SUSTAINED"


def test_none_bias_does_not_block_mase() -> None:
    assert afw.classify_drift(Decimal("1.6"), None, _MASE_T, _BIAS_T) == "MASE_DEGRADED"


def test_none_mase_and_low_bias_is_none() -> None:
    assert afw.classify_drift(None, Decimal("0.1"), _MASE_T, _BIAS_T) is None


def test_both_metrics_none_series_ignored() -> None:
    assert afw.classify_drift(None, None, _MASE_T, _BIAS_T) is None


# ---------------------------------------------------------------------------
# relative_bias — the bias-ratio scale (None-honest)
# ---------------------------------------------------------------------------
def test_relative_bias_computes_fraction() -> None:
    assert afw.relative_bias(Decimal("30"), Decimal("100")) == Decimal("0.3")


def test_relative_bias_uses_absolute_value() -> None:
    # A negative (under-forecast) bias is just as much a sustained drift.
    assert afw.relative_bias(Decimal("-40"), Decimal("100")) == Decimal("0.4")


def test_relative_bias_none_bias() -> None:
    assert afw.relative_bias(None, Decimal("100")) is None


def test_relative_bias_none_scale() -> None:
    assert afw.relative_bias(Decimal("30"), None) is None


def test_relative_bias_zero_scale_is_none() -> None:
    # no demand scale => cannot normalize => None (never a division by zero).
    assert afw.relative_bias(Decimal("30"), Decimal("0")) is None


def test_relative_bias_negative_scale_is_none() -> None:
    assert afw.relative_bias(Decimal("30"), Decimal("-5")) is None


# ---------------------------------------------------------------------------
# drift_recommendation_id — deterministic uuid5 idempotence key
# ---------------------------------------------------------------------------
_SCEN = "00000000-0000-0000-0000-000000000001"
_ITEM = uuid.UUID("11111111-1111-1111-1111-111111111111")
_LOC = uuid.UUID("22222222-2222-2222-2222-222222222222")


def test_uuid5_is_deterministic() -> None:
    a = afw.drift_recommendation_id(_SCEN, _ITEM, _LOC, "MASE_DEGRADED")
    b = afw.drift_recommendation_id(_SCEN, _ITEM, _LOC, "MASE_DEGRADED")
    assert a == b
    assert isinstance(a, uuid.UUID)


def test_uuid5_changes_with_drift_kind() -> None:
    a = afw.drift_recommendation_id(_SCEN, _ITEM, _LOC, "MASE_DEGRADED")
    b = afw.drift_recommendation_id(_SCEN, _ITEM, _LOC, "BOTH")
    assert a != b


def test_uuid5_changes_with_series() -> None:
    other_item = uuid.UUID("33333333-3333-3333-3333-333333333333")
    a = afw.drift_recommendation_id(_SCEN, _ITEM, _LOC, "MASE_DEGRADED")
    b = afw.drift_recommendation_id(_SCEN, other_item, _LOC, "MASE_DEGRADED")
    assert a != b


def test_uuid5_location_none_is_stable() -> None:
    a = afw.drift_recommendation_id(_SCEN, _ITEM, None, "MASE_DEGRADED")
    b = afw.drift_recommendation_id(_SCEN, _ITEM, None, "MASE_DEGRADED")
    assert a == b
    # a None-grain id must differ from a located one.
    assert a != afw.drift_recommendation_id(_SCEN, _ITEM, _LOC, "MASE_DEGRADED")


# ---------------------------------------------------------------------------
# _upsert / _expire_stale_drafts — idempotent-insert-with-tombstone-reactivation
# + supersede against a fake connection (mock-conn pattern of the watcher
# tests). _upsert distinguishes insert vs reactivation via the fake cursor's
# "was_insert" key (stands in for Postgres' `xmax = 0` marker).
# ---------------------------------------------------------------------------
class _FakeCursor:
    def __init__(self, fetch_results=None, rowcount: int = 0) -> None:
        self._fetch = list(fetch_results or [])
        self.rowcount = rowcount
        self.executed: list = []

    def execute(self, query, params=None):  # noqa: ANN001, ANN201 - test double
        self.executed.append((query, params))
        return self

    def fetchone(self):  # noqa: ANN201
        return self._fetch.pop(0) if self._fetch else None


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self, row_factory=None):  # noqa: ANN001, ANN201
        return self._cursor


def _row(rid: uuid.UUID) -> tuple:
    # Only [0] (recommendation_id) is read by _upsert; the rest are filler.
    return (rid,) + ("x",) * (len(afw._COLUMNS) - 1)


def test_upsert_returns_inserted_and_affirmed() -> None:
    id1, id2, id3 = (uuid.uuid4() for _ in range(3))
    # row 2 hits the WHERE guard (still-live DRAFT, unchanged) => no RETURNING row.
    cur = _FakeCursor(fetch_results=[
        {"recommendation_id": id1, "was_insert": True},
        None,
        {"recommendation_id": id3, "was_insert": True},
    ])
    inserted, reactivated, affirmed = afw._upsert(
        _FakeConn(cur), [_row(id1), _row(id2), _row(id3)]
    )
    assert inserted == [id1, id3]                 # only the genuine inserts
    assert reactivated == []
    assert affirmed == [id1, id2, id3]            # every id attempted
    assert len(cur.executed) == 3                 # one execute per row


def test_upsert_reactivates_expired_tombstone() -> None:
    # A recurring drift hits the SAME id, which was previously EXPIRED: the
    # WHERE guard passes, the UPDATE branch fires => xmax != 0 => was_insert
    # False. This is the tombstone-reactivation fix (a prior pure DO NOTHING
    # would have left the row EXPIRED forever, and the recurrence invisible).
    id1 = uuid.uuid4()
    cur = _FakeCursor(fetch_results=[{"recommendation_id": id1, "was_insert": False}])
    inserted, reactivated, affirmed = afw._upsert(_FakeConn(cur), [_row(id1)])
    assert inserted == []
    assert reactivated == [id1]
    assert affirmed == [id1]


def test_upsert_empty_is_noop() -> None:
    cur = _FakeCursor()
    inserted, reactivated, affirmed = afw._upsert(_FakeConn(cur), [])
    assert inserted == []
    assert reactivated == []
    assert affirmed == []
    assert cur.executed == []


def test_upsert_sql_reactivates_only_expired_never_human_statuses() -> None:
    # No DB needed: psycopg.sql.Composed renders without a live connection
    # (identifier quoting/placeholder logic is connection-independent for
    # ASCII identifiers). Asserts the WHERE guard + the xmax marker are wired
    # as specified — not a DO NOTHING, not an unconditional DO UPDATE.
    id1 = uuid.uuid4()
    cur = _FakeCursor(fetch_results=[{"recommendation_id": id1, "was_insert": True}])
    afw._upsert(_FakeConn(cur), [_row(id1)])
    query, _params = cur.executed[0]
    rendered = query.as_string(None)
    assert "ON CONFLICT (recommendation_id) DO UPDATE SET" in rendered
    assert "DO NOTHING" not in rendered
    assert "WHERE forecast_drift_recommendations.status = 'EXPIRED'" in rendered
    assert "(xmax = 0) AS was_insert" in rendered


def test_expire_with_keep_ids_scopes_the_update() -> None:
    keep = [uuid.uuid4(), uuid.uuid4()]
    cur = _FakeCursor(rowcount=3)
    n = afw._expire_stale_drafts(_FakeConn(cur), _SCEN, keep)
    assert n == 3
    query, params = cur.executed[0]
    # keep_ids branch binds (agent_name, scenario, keep_ids) and uses NOT ANY.
    assert "NOT (recommendation_id = ANY(%s))" in query
    assert params == (afw.AGENT_NAME, _SCEN, keep)


def test_expire_without_keep_ids_expires_all_agent_drafts() -> None:
    cur = _FakeCursor(rowcount=5)
    n = afw._expire_stale_drafts(_FakeConn(cur), _SCEN, [])
    assert n == 5
    query, params = cur.executed[0]
    assert "NOT (" not in query
    assert params == (afw.AGENT_NAME, _SCEN)
