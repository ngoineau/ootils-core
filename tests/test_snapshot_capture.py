"""
Pure (DB-free) unit tests for ootils_core.engine.snapshot.capture (chantier
#393 A3-PR1, ADR-030).

Scope: everything in the capture module that is testable WITHOUT a live
connection. ``capture_snapshot`` and the write path of ``persist_snapshot``
need a real Postgres and live in tests/integration/test_snapshot_integration.py
instead — here we lock only the pure surface:

  * ``VALID_SOURCES`` is the frozen {cli, api, cron} set kept in sync with the
    migration-067 source CHECK.
  * ``persist_snapshot`` validates ``source`` BEFORE it touches the connection
    (an invalid channel fails loudly as a ValueError, not an opaque DB CHECK
    violation) and short-circuits an empty batch to 0 WITHOUT any SQL — both
    proven with a connection stand-in that raises on any attribute access, so
    the test genuinely asserts "no DB work happened".
  * ``SnapshotRow`` is a frozen dataclass carrying the raw-UUID coordinates and
    the NULL-honest shortage pair.

No mocks of the engine and no DB — CLAUDE.md: pure helpers live in tests/test_*.
"""
from __future__ import annotations

import dataclasses
import datetime as _dt
from decimal import Decimal
from uuid import uuid4

import pytest

from ootils_core.engine.snapshot import (
    VALID_SOURCES,
    SnapshotRow,
    persist_snapshot,
)


# ---------------------------------------------------------------------------
# A connection stand-in that fails on ANY use.
#
# persist_snapshot's two pure paths (invalid source, empty batch) must both
# return/raise WITHOUT touching the connection. Passing this object as `conn`
# turns "the code touched the DB" into an AttributeError the test would catch,
# so a passing test is positive evidence that no SQL was attempted.
# ---------------------------------------------------------------------------
class _ExplodingConn:
    def __getattr__(self, name: str):  # noqa: ANN401 - test stand-in
        raise AssertionError(
            f"persist_snapshot touched the connection ({name!r}) on a path that "
            "must be DB-free"
        )


# ---------------------------------------------------------------------------
# VALID_SOURCES
# ---------------------------------------------------------------------------


class TestValidSources:
    def test_exact_membership_matches_migration_067_check(self):
        # The migration 067 CHECK is source IN ('cli', 'api', 'cron').
        assert VALID_SOURCES == frozenset({"cli", "api", "cron"})

    def test_is_a_frozenset_immutable(self):
        assert isinstance(VALID_SOURCES, frozenset)
        with pytest.raises(AttributeError):
            VALID_SOURCES.add("rogue")  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# persist_snapshot — pure paths (source validation + empty short-circuit)
# ---------------------------------------------------------------------------


class TestPersistSnapshotPurePaths:
    def test_invalid_source_raises_valueerror_before_touching_conn(self):
        """A bad channel fails loudly as ValueError — and BEFORE any SQL. The
        exploding conn proves the validation short-circuits ahead of the DB."""
        with pytest.raises(ValueError, match="invalid snapshot source"):
            persist_snapshot(_ExplodingConn(), [], "rogue")

    def test_invalid_source_message_lists_allowed_sorted(self):
        with pytest.raises(ValueError) as exc:
            persist_snapshot(_ExplodingConn(), [], "SQL")
        msg = str(exc.value)
        # The message names the offending value and the sorted allowed set.
        assert "'SQL'" in msg
        assert "['api', 'cli', 'cron']" in msg

    def test_empty_rows_returns_zero_without_touching_conn(self):
        """No rows => 0 written and NO SQL — even with a valid source. The
        exploding conn proves the empty-batch fast path never hits the DB."""
        assert persist_snapshot(_ExplodingConn(), [], "cli") == 0

    @pytest.mark.parametrize("source", ["cli", "api", "cron"])
    def test_each_valid_source_accepted_on_empty_batch(self, source):
        """Every whitelisted channel passes validation; with an empty batch the
        call returns 0 without touching the connection (so no DB needed to prove
        the source is accepted)."""
        assert persist_snapshot(_ExplodingConn(), [], source) == 0

    def test_source_validation_precedes_empty_check(self):
        """Ordering guarantee: an invalid source with an empty batch still
        raises (validation is first), rather than silently returning 0."""
        with pytest.raises(ValueError):
            persist_snapshot(_ExplodingConn(), [], "")


# ---------------------------------------------------------------------------
# SnapshotRow — frozen dataclass, raw-UUID coordinates, NULL-honest shortage
# ---------------------------------------------------------------------------


def _row(**overrides) -> SnapshotRow:
    base = dict(
        scenario_id=uuid4(),
        item_id=uuid4(),
        location_id=uuid4(),
        as_of_date=_dt.date(2026, 7, 6),
        on_hand_qty=Decimal("12.5"),
        first_shortage_date=None,
        shortage_severity_usd=None,
        source="cli",
    )
    base.update(overrides)
    return SnapshotRow(**base)


class TestSnapshotRow:
    def test_is_frozen(self):
        row = _row()
        with pytest.raises(dataclasses.FrozenInstanceError):
            row.on_hand_qty = Decimal("99")  # type: ignore[misc]

    def test_fields_round_trip(self):
        sid, iid, lid = uuid4(), uuid4(), uuid4()
        as_of = _dt.date(2026, 1, 2)
        row = _row(
            scenario_id=sid,
            item_id=iid,
            location_id=lid,
            as_of_date=as_of,
            on_hand_qty=Decimal("7.000000"),
        )
        assert row.scenario_id == sid
        assert row.item_id == iid
        assert row.location_id == lid
        assert row.as_of_date == as_of
        assert row.on_hand_qty == Decimal("7.000000")
        assert row.source == "cli"

    def test_on_hand_qty_is_decimal_no_float_drift(self):
        # Decimal preserves scale where a float would drift — the whole point of
        # NUMERIC(18,6) round-tripping.
        row = _row(on_hand_qty=Decimal("0.1") + Decimal("0.2"))
        assert row.on_hand_qty == Decimal("0.3")

    def test_shortage_pair_null_honest_both_none_in_pr1(self):
        """The PR1 contract: the two shortage columns are None together (no
        projected shortage / not calculable at this grain)."""
        row = _row()
        assert row.first_shortage_date is None
        assert row.shortage_severity_usd is None

    def test_shortage_pair_can_be_set_together(self):
        """The dataclass itself does not forbid a both-set pair (a later PR fills
        both from the same projection); it only holds the two fields."""
        d = _dt.date(2026, 9, 1)
        row = _row(first_shortage_date=d, shortage_severity_usd=Decimal("4800"))
        assert row.first_shortage_date == d
        assert row.shortage_severity_usd == Decimal("4800")

    def test_equality_by_value(self):
        sid, iid, lid = uuid4(), uuid4(), uuid4()
        kw = dict(scenario_id=sid, item_id=iid, location_id=lid)
        assert _row(**kw) == _row(**kw)
