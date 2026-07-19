"""
tests/test_reconciliation_matcher.py — PURE unit tests (no DB) for the heuristic
reconciliation MATCHER (``engine.reconciliation.matcher.match_candidates``,
ADR-042 decision 4, PR-5b). The DB half (``run_reconciliation``: stamp
``fulfilled_at``/``fulfilled_erp_id``, the append-only ``reconciliation_runs``
row, the ``reconciliation_completed`` event) lives in
``tests/integration/test_reconciliation_integration.py``.

CONTRACT UNDER TEST — the REAL matcher (matcher.py is the authority; these tests
were re-derived against it, correcting an earlier draft written to a supposed
partition-invariant contract that the implementation contradicts). The matcher
pairs an inbound ERP purchase order with an already-exported, not-yet-reconciled
BASELINE recommendation on a DETERMINISTIC heuristic — never an ``ootils_ref``
round-trip (none exists in the pilot ERP). ``_is_plausible(reco, po)`` is TRUE
iff ALL of:

    * same ``item_external_id``
    * ``po.created_at`` STRICTLY AFTER ``reco.exported_at`` (an upserted
      pre-existing PO keeps its older created_at and is correctly excluded;
      equality is EXCLUDED — strictly ``>``)
    * ``|po.quantity - reco.quantity| <= reco.quantity * 5%``   (QTY, inclusive)
    * ``|po.delivery_date - reco.need_date| <= 7 days``          (DATE, inclusive)
    * supplier equality — ENFORCED ONLY WHEN BOTH SIDES CARRY A SUPPLIER (KNOWN
      GAP 1: the PO node persists no supplier, so a real V1 ``InboundPO`` always
      has ``supplier_external_id=None`` and this criterion never constrains
      today; the pure core keeps the comparison, forward-compatible)
    * for a ``requires_dest_location`` reco only (the TRANSFER family, KNOWN
      GAP 2), ``po.location_external_id == reco.dest_location_external_id``

CLASSIFICATION (the REAL MatchResult, four INDEPENDENT list buckets that NEED
NOT partition the input — migration 086 header: "matched + ambiguous +
unmatched need not equal recos_candidates by construction — ambiguity can be
counted from either side"):
    * ``matched`` — ``list[tuple[recommendation_id, po_external_id]]``: a reco
      with EXACTLY ONE plausible PO whose own plausible-reco set is exactly
      {that reco}. Safe to stamp.
    * ``ambiguous_reco_ids`` — recos with >= 2 plausible POs (never stamped).
    * ``ambiguous_po_ids`` — POs with >= 2 plausible recos (never stamped).
    * ``unmatched_reco_ids`` — recos with ZERO plausible PO.

THE LIMBO CASE IS DELIBERATE AND TESTED EXPLICITLY (``TestLimbo``): a reco whose
SINGLE plausible PO is itself contested by another reco (that PO has >= 2
plausible recos) is in NONE of the four buckets — not stamped, not counted,
simply reconsidered next run — while the contested PO surfaces in
``ambiguous_po_ids``. This is why the buckets do NOT partition and why there is
NO ``matched + ambiguous + unmatched == candidates`` invariant to assert.

RUN-LEVEL TALLY (``_run_counts`` below) is derived EXACTLY as
``run_reconciliation`` derives it for the ``reconciliation_runs`` row:
``candidates = len(recos)``, ``matched = len(result.matched)``,
``ambiguous = len(ambiguous_reco_ids) + len(ambiguous_po_ids)`` (BOTH sides),
``unmatched = len(unmatched_reco_ids)``.

+/-5% qty and +/-7d date are MODULE KNOBS (🎯 pilot-adjustable) — exercised
here through boundary BEHAVIOUR (exactly +5% in, +5.01% out; exactly +7d in,
+8d out), NOT by importing the constants, so a rename can never silently weaken
the boundary check.

No DB required — this file imports the matcher directly (a hard failure, never a
silent skip, if the pure core drifts) and needs no ``DATABASE_URL``.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

from ootils_core.engine.reconciliation import matcher

UTC = timezone.utc
EXPORTED_AT = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
PO_CREATED = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)  # strictly after EXPORTED_AT
NEED_DATE = date(2026, 8, 1)


# ─────────────────────────────────────────────────────────────
# Deterministic input builders — matching defaults so a lone reco + lone PO is
# an EXACT match; each test overrides exactly the one axis it probes. These two
# helpers are the single realignment point if a field is renamed.
# ─────────────────────────────────────────────────────────────
def _reco(n: int = 1, **overrides) -> matcher.RecoCandidate:
    base = dict(
        recommendation_id=UUID(int=n),
        item_external_id="IT-1",
        supplier_external_id=None,
        dest_location_external_id=None,
        requires_dest_location=False,
        quantity=Decimal("100"),
        need_date=NEED_DATE,
        exported_at=EXPORTED_AT,
    )
    base.update(overrides)
    return matcher.RecoCandidate(**base)


def _po(po: str = "PO-1", **overrides) -> matcher.InboundPO:
    base = dict(
        po_external_id=po,
        item_external_id="IT-1",
        location_external_id=None,
        supplier_external_id=None,  # KNOWN GAP 1 — the real V1 PO node carries none
        quantity=Decimal("100"),
        delivery_date=NEED_DATE,
        created_at=PO_CREATED,
    )
    base.update(overrides)
    return matcher.InboundPO(**base)


def _matched(result) -> set[tuple[UUID, str]]:
    """The stamped pairs as a set — ``result.matched`` is already
    ``list[tuple[UUID, str]]`` (no pair object with attributes)."""
    return set(result.matched)


def _run_counts(recos, result) -> tuple[int, int, int, int]:
    """(candidates, matched, ambiguous, unmatched) derived EXACTLY as
    ``run_reconciliation`` builds the ``reconciliation_runs`` row — ambiguous
    counts BOTH sides (reco-side + PO-side), never a candidate-centric
    partition (migration 086 header)."""
    return (
        len(recos),
        len(result.matched),
        len(result.ambiguous_reco_ids) + len(result.ambiguous_po_ids),
        len(result.unmatched_reco_ids),
    )


# ─────────────────────────────────────────────────────────────
# 1. Exact univocal match + empty inputs
# ─────────────────────────────────────────────────────────────
class TestExactMatch:
    def test_single_reco_single_po_exact(self):
        recos = (_reco(1),)
        result = matcher.match_candidates(recos, (_po("PO-77"),))
        assert _matched(result) == {(UUID(int=1), "PO-77")}
        assert result.ambiguous_reco_ids == []
        assert result.ambiguous_po_ids == []
        assert result.unmatched_reco_ids == []
        assert _run_counts(recos, result) == (1, 1, 0, 0)

    def test_empty_inputs_are_a_clean_zero_run(self):
        recos: tuple = ()
        result = matcher.match_candidates(recos, ())
        assert result.matched == []
        assert result.ambiguous_reco_ids == []
        assert result.ambiguous_po_ids == []
        assert result.unmatched_reco_ids == []
        assert _run_counts(recos, result) == (0, 0, 0, 0)


# ─────────────────────────────────────────────────────────────
# 2. Quantity tolerance — exactly +/-5% in, +5.01% out
# ─────────────────────────────────────────────────────────────
class TestQtyTolerance:
    def test_plus_5pct_exact_is_in(self):
        # reco qty 100, PO qty 105 -> |Δ| = 5 == 100 * 5% -> inclusive IN
        recos = (_reco(1),)
        result = matcher.match_candidates(recos, (_po(quantity=Decimal("105")),))
        assert _matched(result) == {(UUID(int=1), "PO-1")}
        assert _run_counts(recos, result) == (1, 1, 0, 0)

    def test_minus_5pct_exact_is_in(self):
        recos = (_reco(1),)
        result = matcher.match_candidates(recos, (_po(quantity=Decimal("95")),))
        assert _matched(result) == {(UUID(int=1), "PO-1")}
        assert _run_counts(recos, result) == (1, 1, 0, 0)

    def test_plus_5_01pct_is_out_unmatched(self):
        # PO qty 105.01 -> |Δ| = 5.01 > 5 -> OUT, the reco is unmatched
        recos = (_reco(1),)
        result = matcher.match_candidates(recos, (_po(quantity=Decimal("105.01")),))
        assert result.matched == []
        assert result.unmatched_reco_ids == [UUID(int=1)]
        assert _run_counts(recos, result) == (1, 0, 0, 1)


# ─────────────────────────────────────────────────────────────
# 3. Date window — exactly +/-7d in, +8d out
# ─────────────────────────────────────────────────────────────
class TestDateWindow:
    def test_plus_7d_exact_is_in(self):
        recos = (_reco(1),)
        result = matcher.match_candidates(
            recos, (_po(delivery_date=NEED_DATE + timedelta(days=7)),)
        )
        assert _matched(result) == {(UUID(int=1), "PO-1")}
        assert _run_counts(recos, result) == (1, 1, 0, 0)

    def test_minus_7d_exact_is_in(self):
        recos = (_reco(1),)
        result = matcher.match_candidates(
            recos, (_po(delivery_date=NEED_DATE - timedelta(days=7)),)
        )
        assert _matched(result) == {(UUID(int=1), "PO-1")}
        assert _run_counts(recos, result) == (1, 1, 0, 0)

    def test_plus_8d_is_out_unmatched(self):
        recos = (_reco(1),)
        result = matcher.match_candidates(
            recos, (_po(delivery_date=NEED_DATE + timedelta(days=8)),)
        )
        assert result.matched == []
        assert result.unmatched_reco_ids == [UUID(int=1)]
        assert _run_counts(recos, result) == (1, 0, 0, 1)


# ─────────────────────────────────────────────────────────────
# 4. The PO must POST-DATE the export (created_at > exported_at, STRICT)
# ─────────────────────────────────────────────────────────────
class TestPoMustPostdateExport:
    def test_po_created_after_export_matches(self):
        recos = (_reco(1),)
        result = matcher.match_candidates(
            recos, (_po(created_at=EXPORTED_AT + timedelta(seconds=1)),)
        )
        assert _matched(result) == {(UUID(int=1), "PO-1")}
        assert _run_counts(recos, result) == (1, 1, 0, 0)

    def test_po_created_before_export_is_unmatched(self):
        recos = (_reco(1),)
        result = matcher.match_candidates(
            recos, (_po(created_at=EXPORTED_AT - timedelta(days=1)),)
        )
        assert result.matched == []
        assert result.unmatched_reco_ids == [UUID(int=1)]
        assert _run_counts(recos, result) == (1, 0, 0, 1)

    def test_po_created_exactly_at_export_is_unmatched(self):
        # strictly AFTER: created_at > exported_at, NOT >= (equality EXCLUDED,
        # matcher.py ``_is_plausible``: ``if not (po.created_at > reco.exported_at)``).
        recos = (_reco(1),)
        result = matcher.match_candidates(recos, (_po(created_at=EXPORTED_AT),))
        assert result.matched == []
        assert result.unmatched_reco_ids == [UUID(int=1)]
        assert _run_counts(recos, result) == (1, 0, 0, 1)


# ─────────────────────────────────────────────────────────────
# 5. Supplier — enforced ONLY when BOTH sides carry one
# ─────────────────────────────────────────────────────────────
class TestSupplier:
    def test_supplier_mismatch_is_unmatched(self):
        # both sides present + different -> the gate fires -> not plausible.
        recos = (_reco(1, supplier_external_id="SUP-A"),)
        result = matcher.match_candidates(
            recos, (_po(supplier_external_id="SUP-B"),)
        )
        assert result.matched == []
        assert result.unmatched_reco_ids == [UUID(int=1)]
        assert _run_counts(recos, result) == (1, 0, 0, 1)

    def test_supplier_match_matches(self):
        recos = (_reco(1, supplier_external_id="SUP-A"),)
        result = matcher.match_candidates(
            recos, (_po(supplier_external_id="SUP-A"),)
        )
        assert _matched(result) == {(UUID(int=1), "PO-1")}
        assert _run_counts(recos, result) == (1, 1, 0, 0)

    def test_reco_without_supplier_ignores_po_supplier(self):
        # reco side None -> the gate never fires, whatever the PO carries.
        recos = (_reco(1, supplier_external_id=None),)
        result = matcher.match_candidates(
            recos, (_po(supplier_external_id="SUP-Z"),)
        )
        assert _matched(result) == {(UUID(int=1), "PO-1")}
        assert _run_counts(recos, result) == (1, 1, 0, 0)

    def test_po_none_supplier_never_constrains_the_real_v1_case(self):
        # KNOWN GAP 1 — the real V1 InboundPO ALWAYS has supplier_external_id
        # None (the ingest validates but never persists it). Even a reco that
        # DOES carry a supplier still matches, because the PO side is None so
        # the gate never fires. This is the shape every real reconciliation
        # takes today.
        recos = (_reco(1, supplier_external_id="SUP-A"),)
        result = matcher.match_candidates(
            recos, (_po(supplier_external_id=None),)
        )
        assert _matched(result) == {(UUID(int=1), "PO-1")}
        assert _run_counts(recos, result) == (1, 1, 0, 0)


# ─────────────────────────────────────────────────────────────
# 6. Ambiguity STAMPS NOTHING, and counts honestly from either side
# ─────────────────────────────────────────────────────────────
class TestAmbiguityStampsNothing:
    def test_two_pos_one_reco_counts_reco_side(self):
        # one reco with >= 2 plausible POs -> the RECO is ambiguous
        # (ambiguous_reco_ids); neither PO is contested (each is plausible for
        # that one reco only), so ambiguous_po_ids stays empty. Nothing stamped.
        recos = (_reco(1),)
        result = matcher.match_candidates(recos, (_po("PO-A"), _po("PO-B")))
        assert result.matched == []
        assert result.ambiguous_reco_ids == [UUID(int=1)]
        assert result.ambiguous_po_ids == []
        assert result.unmatched_reco_ids == []
        assert _run_counts(recos, result) == (1, 0, 1, 0)

    def test_two_recos_one_po_counts_po_side_both_recos_in_limbo(self):
        # one PO plausible for two recos -> the PO is contested
        # (ambiguous_po_ids). Each reco has EXACTLY ONE plausible PO but it is
        # contested, so BOTH recos fall into LIMBO — in NONE of the reco
        # buckets (not matched, not ambiguous_reco, not unmatched). This is the
        # PO-side count, NOT a candidate-centric ambiguous=2.
        recos = (_reco(1), _reco(2))
        result = matcher.match_candidates(recos, (_po("PO-X"),))
        assert result.matched == []
        assert result.ambiguous_reco_ids == []
        assert result.ambiguous_po_ids == ["PO-X"]
        assert result.unmatched_reco_ids == []
        # Both recos are in limbo — surfaced in no reco-side bucket.
        for rid in (UUID(int=1), UUID(int=2)):
            assert rid not in result.ambiguous_reco_ids
            assert rid not in result.unmatched_reco_ids
            assert rid not in {pair[0] for pair in result.matched}
        assert _run_counts(recos, result) == (2, 0, 1, 0)

    def test_ambiguous_reco_does_not_poison_a_clean_one(self):
        # c1 <-> p1 clean; c2 contested by p2a/p2b -> only c1 is stamped.
        recos = (
            _reco(1, item_external_id="IT-CLEAN"),
            _reco(2, item_external_id="IT-AMBI"),
        )
        pos = (
            _po("PO-CLEAN", item_external_id="IT-CLEAN"),
            _po("PO-AMBI-A", item_external_id="IT-AMBI"),
            _po("PO-AMBI-B", item_external_id="IT-AMBI"),
        )
        result = matcher.match_candidates(recos, pos)
        assert _matched(result) == {(UUID(int=1), "PO-CLEAN")}
        assert result.ambiguous_reco_ids == [UUID(int=2)]
        assert result.ambiguous_po_ids == []
        assert result.unmatched_reco_ids == []
        assert _run_counts(recos, result) == (2, 1, 1, 0)


# ─────────────────────────────────────────────────────────────
# 7. LIMBO — a reco whose sole plausible PO is contested is stamped NOR counted
# ─────────────────────────────────────────────────────────────
class TestLimbo:
    def test_reco_in_limbo_when_its_sole_po_is_contested(self):
        # Asymmetric limbo (the richest case, migration 086 header + MatchResult
        # docstring): c1 has TWO plausible POs (pA, pShared) -> c1 is a genuine
        # ambiguous_reco; c2 has EXACTLY ONE plausible PO (pShared) but pShared
        # is contested (plausible for both c1 and c2) -> c2 is in LIMBO, in NONE
        # of the four buckets. pShared surfaces in ambiguous_po_ids.
        #
        # Differentiation uses the supplier gate: pA carries SUP-A so it is
        # plausible for c1 (SUP-A) only — c2 (SUP-B) is excluded (both present,
        # different). pShared carries no supplier so the gate never fires and it
        # is plausible for BOTH c1 and c2 (KNOWN GAP 1 shape).
        c1 = _reco(1, item_external_id="IT-X", supplier_external_id="SUP-A")
        c2 = _reco(2, item_external_id="IT-X", supplier_external_id="SUP-B")
        p_a = _po("PO-A", item_external_id="IT-X", supplier_external_id="SUP-A")
        p_shared = _po("PO-SHARED", item_external_id="IT-X", supplier_external_id=None)
        recos = (c1, c2)
        result = matcher.match_candidates(recos, (p_a, p_shared))

        assert result.matched == []
        assert result.ambiguous_reco_ids == [UUID(int=1)]  # c1: two plausible POs
        assert result.ambiguous_po_ids == ["PO-SHARED"]     # contested PO
        assert result.unmatched_reco_ids == []

        # c2 is in LIMBO — present in NONE of the four buckets.
        assert UUID(int=2) not in result.ambiguous_reco_ids
        assert UUID(int=2) not in result.unmatched_reco_ids
        assert UUID(int=2) not in {pair[0] for pair in result.matched}

        # Run-level tally counts BOTH ambiguity sides (c1 reco-side + pShared
        # PO-side) — the buckets do NOT partition the 2 input recos.
        assert _run_counts(recos, result) == (2, 0, 2, 0)


# ─────────────────────────────────────────────────────────────
# 8. TRANSFER requires the dest location; a po_draft ignores it
# ─────────────────────────────────────────────────────────────
class TestTransferRequiresLocation:
    def test_transfer_matches_when_dest_location_matches(self):
        recos = (
            _reco(1, requires_dest_location=True, dest_location_external_id="DC-1"),
        )
        result = matcher.match_candidates(
            recos, (_po("PO-T", location_external_id="DC-1"),)
        )
        assert _matched(result) == {(UUID(int=1), "PO-T")}
        assert _run_counts(recos, result) == (1, 1, 0, 0)

    def test_transfer_unmatched_when_location_differs(self):
        recos = (
            _reco(1, requires_dest_location=True, dest_location_external_id="DC-1"),
        )
        result = matcher.match_candidates(
            recos, (_po("PO-T", location_external_id="DC-2"),)
        )
        assert result.matched == []
        assert result.unmatched_reco_ids == [UUID(int=1)]
        assert _run_counts(recos, result) == (1, 0, 0, 1)

    def test_po_draft_ignores_location(self):
        # requires_dest_location=False (a po_draft / reschedule) — the PO's
        # location is irrelevant to the match (recommendations has no generic
        # site column, ADR-042 PR-5a gap; only TRANSFER carries dest).
        recos = (_reco(1, requires_dest_location=False),)
        result = matcher.match_candidates(
            recos, (_po("PO-D", location_external_id="DC-WHATEVER"),)
        )
        assert _matched(result) == {(UUID(int=1), "PO-D")}
        assert _run_counts(recos, result) == (1, 1, 0, 0)


# ─────────────────────────────────────────────────────────────
# 9. Determinism — permuting BOTH input lists yields the IDENTICAL result
# ─────────────────────────────────────────────────────────────
class TestDeterminism:
    def _mixed(self):
        """A shape that exercises ALL FOUR buckets + limbo at once:
          * c1 (IT-CLEAN)  <-> p1              -> matched
          * c2 (IT-AMBI)   <-> p2a, p2b        -> ambiguous_reco
          * c3 (IT-LONELY) <-> (none)          -> unmatched
          * c5, c6 (IT-SHARE) both <-> pShared -> pShared ambiguous_po; c5/c6 limbo
        """
        c1 = _reco(1, item_external_id="IT-CLEAN")
        c2 = _reco(2, item_external_id="IT-AMBI")
        c3 = _reco(3, item_external_id="IT-LONELY")
        c5 = _reco(5, item_external_id="IT-SHARE")
        c6 = _reco(6, item_external_id="IT-SHARE")
        p1 = _po("PO-CLEAN", item_external_id="IT-CLEAN")
        p2a = _po("PO-AMBI-A", item_external_id="IT-AMBI")
        p2b = _po("PO-AMBI-B", item_external_id="IT-AMBI")
        p_shared = _po("PO-SHARE", item_external_id="IT-SHARE")
        return [c1, c2, c3, c5, c6], [p1, p2a, p2b, p_shared]

    def test_permutation_of_inputs_same_result(self):
        cands, pos = self._mixed()
        r1 = matcher.match_candidates(tuple(cands), tuple(pos))
        r2 = matcher.match_candidates(
            tuple(reversed(cands)), tuple(reversed(pos))
        )
        # Internal sorting (recos by str(uuid), pos by po_external_id) makes the
        # four output lists IDENTICAL, not merely set-equal, across permutations.
        assert r1.matched == r2.matched
        assert r1.ambiguous_reco_ids == r2.ambiguous_reco_ids
        assert r1.ambiguous_po_ids == r2.ambiguous_po_ids
        assert r1.unmatched_reco_ids == r2.unmatched_reco_ids

        # …and the shape is the expected all-buckets result.
        assert _matched(r1) == {(UUID(int=1), "PO-CLEAN")}
        assert r1.ambiguous_reco_ids == [UUID(int=2)]
        assert r1.ambiguous_po_ids == ["PO-SHARE"]
        assert r1.unmatched_reco_ids == [UUID(int=3)]
        # c5 and c6 are in limbo — in no reco-side bucket.
        for rid in (UUID(int=5), UUID(int=6)):
            assert rid not in r1.ambiguous_reco_ids
            assert rid not in r1.unmatched_reco_ids
            assert rid not in {pair[0] for pair in r1.matched}
        # candidates=5, matched=1, ambiguous=1 reco + 1 po = 2, unmatched=1.
        assert _run_counts(cands, r1) == (5, 1, 2, 1)
