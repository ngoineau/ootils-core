"""
Golden-master for the DRP (distribution) engine (engine/drp/core.py).

Same discipline as tests/test_mrp_core_golden.py: a tiny hand-computed dataset
whose expected outputs are derived STEP BY STEP in the comments, so any future
change that deviates from the documented arithmetic fails CI instead of silently
shifting a distribution plan. Every number below is derived by hand from the
documented semantics of core.py (module + function docstrings) BEFORE running —
none is copied back from an execution. If the engine ever disagrees with a
derivation here, the derivation is the contract and the divergence is a bug to
investigate, not a golden to "fix".

core.py's functions are pure (they operate on plain dict/list inputs, not the
DB), so this runs with no database — just the engine math.

Planning key throughout = (item, location) tuples; quantities are floats;
buckets are 0-indexed weekly integers.
"""
from __future__ import annotations

import pytest

from ootils_core.engine.drp import core
from ootils_core.engine.drp.core import TransferLink, TransferSignal, _fair_share_round, _sort_key

# A generous horizon so no case is truncated by the bucket ceiling; every
# derivation below reasons about the FIRST safety-breaking bucket, well inside.
H = 12


# ───────────────────────── projected_deficits ─────────────────────────


def test_deficit_simple_below_safety():
    """Case 1 — the simplest deficit.
    (A, EAST): on_hand=10, safety=5, demand 8 @bucket 2.
    Projection walk (pa starts at on_hand=10):
      bucket 0: pa -= 0  -> 10  (>= safety 5, no trigger)
      bucket 1: pa -= 0  -> 10  (>= 5)
      bucket 2: pa -= 8  -> 2   (< safety 5) -> TRIGGER
    deficit = safety - pa = 5 - 2 = 3.0, at deficit_bucket 2.
    """
    out = core.projected_deficits(
        demand_by_loc={("A", "EAST"): {2: 8.0}},
        on_hand_by_loc={("A", "EAST"): 10.0},
        safety_by_loc={("A", "EAST"): 5.0},
        horizon_buckets=H,
    )
    assert out == {("A", "EAST"): (2, 3.0)}


def test_deficit_triggers_at_safety_threshold_not_stockout():
    """Case 2 — the trigger is the SAFETY threshold, NOT stockout.
    (A, EAST): on_hand=30, safety=20, demand 15 @bucket 2 then 20 @bucket 4.
      bucket 2: pa = 30 - 15 = 15  -> 15 < safety 20 -> TRIGGER (still POSITIVE,
                nowhere near stockout — this is the whole point of the case).
    deficit = 20 - 15 = 5.0 at bucket 2. The later bucket-4 demand is never
    reached because the first breach already returned.
    """
    out = core.projected_deficits(
        demand_by_loc={("A", "EAST"): {2: 15.0, 4: 20.0}},
        on_hand_by_loc={("A", "EAST"): 30.0},
        safety_by_loc={("A", "EAST"): 20.0},
        horizon_buckets=H,
    )
    assert out == {("A", "EAST"): (2, 5.0)}
    # Explicit: the balance at trigger (15) is strictly positive — safety, not
    # stockout, fired. deficit_qty (5) restores safety (15 + 5 == 20).
    deficit_bucket, deficit_qty = out[("A", "EAST")]
    assert deficit_bucket == 2
    assert deficit_qty == pytest.approx(5.0)


def test_deficit_zero_safety_is_first_negative_bucket():
    """Case 3 — safety=0 reduces to the first NEGATIVE (stockout) bucket.
    (A, EAST): on_hand=10, safety=0, demand 4 @bucket 1 then 9 @bucket 2.
      bucket 1: pa = 10 - 4 = 6   -> 6 < 0? no.
      bucket 2: pa = 6  - 9 = -3  -> -3 < 0 -> TRIGGER (first negative).
    deficit = safety - pa = 0 - (-3) = 3.0 at bucket 2.
    (safety absent from the map => default 0.0 in the projection.)
    """
    out = core.projected_deficits(
        demand_by_loc={("A", "EAST"): {1: 4.0, 2: 9.0}},
        on_hand_by_loc={("A", "EAST"): 10.0},
        safety_by_loc={},
        horizon_buckets=H,
    )
    assert out == {("A", "EAST"): (2, 3.0)}


def test_deficit_absent_when_never_below_safety():
    """Case 4 — a coordinate that never breaks safety is ABSENT from the dict.
    (A, EAST): on_hand=100, safety=5, demand 10 @bucket 2.
      lowest projection over the horizon = 100 - 10 = 90, always >= safety 5.
    No breach => no key. The dict is empty (this is the ONLY coordinate).
    """
    out = core.projected_deficits(
        demand_by_loc={("A", "EAST"): {2: 10.0}},
        on_hand_by_loc={("A", "EAST"): 100.0},
        safety_by_loc={("A", "EAST"): 5.0},
        horizon_buckets=H,
    )
    assert out == {}
    assert ("A", "EAST") not in out


# ───────────────────────── excess_by_location ─────────────────────────


def test_excess_floor_zero_when_exactly_demand_plus_safety():
    """Case 5 — excess is floored at 0: a source holding EXACTLY demand+safety
    yields NO key (excess must be strictly > 0 to appear).
    (A, WEST): on_hand=25, safety=5, demand total = 20 (8 @b0 + 12 @b3).
      excess = 25 - (20 + 5) = 0 -> floored, NOT strictly positive -> no key.
    """
    out = core.excess_by_location(
        demand_by_loc={("A", "WEST"): {0: 8.0, 3: 12.0}},
        on_hand_by_loc={("A", "WEST"): 25.0},
        safety_by_loc={("A", "WEST"): 5.0},
        horizon_buckets=H,
    )
    assert out == {}


def test_excess_positive_uses_horizon_total_demand():
    """Companion to case 5/6 — the excess formula, made explicit.
    (A, WEST): on_hand=100, safety=5, demand total 20 (all @bucket 0).
      excess = 100 - (20 total demand + 5 safety) = 75.0.
    A location with stock but with NO entry in on_hand_by_loc can never carry
    excess — only coordinates present in on_hand_by_loc are scanned.
    """
    out = core.excess_by_location(
        demand_by_loc={("A", "WEST"): {0: 20.0}},
        on_hand_by_loc={("A", "WEST"): 100.0},
        safety_by_loc={("A", "WEST"): 5.0},
        horizon_buckets=H,
    )
    assert out == {("A", "WEST"): 75.0}


# ───────────────────────── transfer_signals ─────────────────────────


def test_transfer_nominal_single_signal():
    """Case 6 — the nominal transfer (the dev's smoke case).
    WEST (source): on_hand=100, safety=5, demand 20 @bucket 0
        -> excess = 100 - (20 + 5) = 75.
    EAST (dest):   on_hand=10, safety=5, demand 8 @bucket 2
        -> projection bucket 2 = 10 - 8 = 2 < 5 -> deficit (bucket 2, qty 3).
    Link WEST->EAST, lead_buckets=2, min_qty=1 (default), max_qty=None, prio=1.
      qty          = min(remaining deficit 3, source excess 75) = 3
                     (max_qty None => no cap); 3 >= min_qty 1 -> emit.
      ship_bucket  = max(0, deficit_bucket 2 - lead 2) = 0
      arrival      = ship 0 + lead 2 = 2 (lands exactly at the deficit bucket).
      source_excess_before = 75 (the excess seen BEFORE this draw).
    Exactly ONE signal.
    """
    signals = core.transfer_signals(
        demand_by_loc={("A", "WEST"): {0: 20.0}, ("A", "EAST"): {2: 8.0}},
        on_hand_by_loc={("A", "WEST"): 100.0, ("A", "EAST"): 10.0},
        safety_by_loc={("A", "WEST"): 5.0, ("A", "EAST"): 5.0},
        links=[TransferLink("WEST", "EAST", 2, 1.0, None, 1)],
        horizon_buckets=H,
    )
    # #395 PR2a: a single (source, dest) draw is fair-share degenerate (one
    # destination -> ratio 1.0), so fair_share_qty == the full residual == qty
    # (no rounding: transfer_multiple defaults to 1.0) -> rounding_remnant 0.
    assert signals == [
        TransferSignal(
            item="A",
            source_location="WEST",
            dest_location="EAST",
            qty=3.0,
            ship_bucket=0,
            arrival_bucket=2,
            deficit_bucket=2,
            deficit_qty=3.0,
            source_excess_before=75.0,
            fair_share_qty=3.0,
            rounding_remnant=0.0,
        )
    ]


def test_min_shipment_blocks_single_link_no_signal():
    """Case 7a — the minimum-shipment rule blocks a lane below its minimum.
    EAST deficit (bucket 2, qty 3) exactly as case 6.
    Link WEST->EAST has min_qty=10 > the needed 3:
      qty = min(3, excess 75) = 3; 3 < min_qty 10 -> emit NOTHING on this link.
    No other link exists -> the deficit is left uncovered -> ZERO signals.
    (Honest: we neither ship 3 below the lane minimum nor inflate to 10 units
    the destination does not need.)
    """
    signals = core.transfer_signals(
        demand_by_loc={("A", "WEST"): {0: 20.0}, ("A", "EAST"): {2: 8.0}},
        on_hand_by_loc={("A", "WEST"): 100.0, ("A", "EAST"): 10.0},
        safety_by_loc={("A", "WEST"): 5.0, ("A", "EAST"): 5.0},
        links=[TransferLink("WEST", "EAST", 2, 10.0, None, 1)],
        horizon_buckets=H,
    )
    assert signals == []


def test_min_shipment_falls_through_to_secondary_link():
    """Case 7b — a deficit blocked at the priority link's minimum FALLS THROUGH
    to the next-priority link, which serves it.
    EAST deficit (bucket 2, qty 3).
    Two candidate links into EAST, both sources holding excess:
      - WEST->EAST : priority 1, min_qty=10  (BLOCKS: 3 < 10)
      - NORTH->EAST: priority 2, min_qty=1   (serves)
    Sources' excess:
      WEST : on_hand 100, dem 20, ss 5 -> 75.
      NORTH: on_hand  50, dem 10, ss 5 -> 35.
    Greedy over EAST's candidates in (priority, source) order = [WEST(1), NORTH(2)]:
      WEST link: qty = min(3, 75) = 3 < min_qty 10 -> skip (remaining stays 3).
      NORTH link: qty = min(3, 35) = 3 >= min_qty 1 -> EMIT.
        ship = max(0, 2 - lead 2) = 0, arrival = 0 + 2 = 2.
        source_excess_before = 35 (NORTH's excess, untouched by the skipped WEST).
    Exactly ONE signal, sourced from NORTH.
    """
    signals = core.transfer_signals(
        demand_by_loc={
            ("A", "WEST"): {0: 20.0},
            ("A", "NORTH"): {0: 10.0},
            ("A", "EAST"): {2: 8.0},
        },
        on_hand_by_loc={("A", "WEST"): 100.0, ("A", "NORTH"): 50.0, ("A", "EAST"): 10.0},
        safety_by_loc={("A", "WEST"): 5.0, ("A", "NORTH"): 5.0, ("A", "EAST"): 5.0},
        links=[
            TransferLink("WEST", "EAST", 2, 10.0, None, 1),
            TransferLink("NORTH", "EAST", 2, 1.0, None, 2),
        ],
        horizon_buckets=H,
    )
    assert len(signals) == 1
    sig = signals[0]
    assert sig.source_location == "NORTH"
    assert sig.qty == pytest.approx(3.0)
    assert sig.ship_bucket == 0
    assert sig.arrival_bucket == 2
    assert sig.deficit_bucket == 2
    assert sig.source_excess_before == pytest.approx(35.0)


def test_shared_source_excess_decremented_across_destinations():
    """Case 8 — one source's excess is a single running counter shared greedily
    across destinations; the SECOND destination sees it decremented.
    Source WEST: on_hand=100, safety=0, no demand -> excess = 100.
    Two destinations, each with its own deficit, both linked to WEST only:
      DEST_A (label 'EAST'):  on_hand 0, ss 0, demand 30 @bucket 1
          -> bucket 1: pa = 0 - 30 = -30 < 0 -> deficit (bucket 1, qty 30).
      DEST_B (label 'SOUTH'): on_hand 0, ss 0, demand 40 @bucket 1
          -> deficit (bucket 1, qty 40).
    Destinations are processed in sorted (item, dest) order = EAST, then SOUTH.
      EAST : qty = min(30, excess 100) = 30 -> emit, source_excess_before = 100.
             WEST excess now 100 - 30 = 70.
      SOUTH: qty = min(40, excess 70) = 40 -> emit, source_excess_before = 70
             (DECREMENTED — the EAST draw already consumed 30).
    Total transferred 30 + 40 = 70 <= initial excess 100 (never over-drawn).
    Links have lead 0, so ship = deficit_bucket = 1, arrival = 1.

    #395 PR2a: EAST and SOUTH are each the ONLY destination WEST serves at
    priority level 1 in their own draw (they are processed as two SEPARATE
    (item, dest) coordinates, each pulling on WEST at its own turn — not two
    destinations split simultaneously at one level, since WEST has no OTHER
    destination competing for its excess in EITHER draw). Fair-share therefore
    degenerates to ratio 1.0 each time: fair_share_qty == qty == the full
    deficit on both signals (no rounding at the default transfer_multiple=1.0)
    -> rounding_remnant 0.0 on both. Quantities/source_excess_before UNCHANGED
    from the pre-fair-share greedy (verified by direct execution against this
    revised core, not merely carried over).
    """
    signals = core.transfer_signals(
        demand_by_loc={("A", "EAST"): {1: 30.0}, ("A", "SOUTH"): {1: 40.0}},
        on_hand_by_loc={("A", "WEST"): 100.0, ("A", "EAST"): 0.0, ("A", "SOUTH"): 0.0},
        safety_by_loc={},
        links=[
            TransferLink("WEST", "EAST", 0, 1.0, None, 1),
            TransferLink("WEST", "SOUTH", 0, 1.0, None, 1),
        ],
        horizon_buckets=H,
    )
    by_dest = {s.dest_location: s for s in signals}
    assert by_dest["EAST"].source_excess_before == pytest.approx(100.0)
    assert by_dest["EAST"].qty == pytest.approx(30.0)
    assert by_dest["EAST"].fair_share_qty == pytest.approx(30.0)
    assert by_dest["EAST"].rounding_remnant == pytest.approx(0.0)
    assert by_dest["SOUTH"].source_excess_before == pytest.approx(70.0)
    assert by_dest["SOUTH"].qty == pytest.approx(40.0)
    assert by_dest["SOUTH"].fair_share_qty == pytest.approx(40.0)
    assert by_dest["SOUTH"].rounding_remnant == pytest.approx(0.0)
    # Conservation: total drawn from WEST <= its initial excess.
    assert by_dest["EAST"].qty + by_dest["SOUTH"].qty <= 100.0


def test_max_qty_caps_partial_coverage():
    """Case 9 — max_qty caps a single transfer (honest partial coverage).
    EAST deficit: on_hand 0, ss 0, demand 10 @bucket 2 -> deficit (bucket 2, 10).
    Source WEST excess 100 (on_hand 100, no demand, ss 0). Link max_qty=4:
      qty = min(remaining 10, excess 100) = 10, then capped by max_qty -> 4.
      4 >= min_qty 1 -> emit qty=4 (covers 4 of the 10; the rest stays open,
      and no further link exists to cover it).
    ship = max(0, 2 - lead 1) = 1, arrival = 1 + 1 = 2.
    """
    signals = core.transfer_signals(
        demand_by_loc={("A", "EAST"): {2: 10.0}},
        on_hand_by_loc={("A", "WEST"): 100.0, ("A", "EAST"): 0.0},
        safety_by_loc={},
        links=[TransferLink("WEST", "EAST", 1, 1.0, 4.0, 1)],
        horizon_buckets=H,
    )
    assert len(signals) == 1
    assert signals[0].qty == pytest.approx(4.0)
    assert signals[0].deficit_qty == pytest.approx(10.0)  # full deficit recorded
    assert signals[0].ship_bucket == 1
    assert signals[0].arrival_bucket == 2


def test_no_excess_anywhere_yields_no_signal():
    """Case 10 — a source with no distributable excess never funds a transfer
    (never starve the source to feed a peer).
    EAST deficit: on_hand 0, ss 0, demand 10 @bucket 2 -> deficit (bucket 2, 10).
    Source WEST holds on_hand 20 but its OWN demand is 20 @bucket 0
        -> excess = 20 - (20 + 0) = 0 -> no excess key.
    Candidate link WEST->EAST finds avail=0 -> continue -> ZERO signals.
    """
    signals = core.transfer_signals(
        demand_by_loc={("A", "WEST"): {0: 20.0}, ("A", "EAST"): {2: 10.0}},
        on_hand_by_loc={("A", "WEST"): 20.0, ("A", "EAST"): 0.0},
        safety_by_loc={},
        links=[TransferLink("WEST", "EAST", 1, 1.0, None, 1)],
        horizon_buckets=H,
    )
    assert signals == []


def test_ship_bucket_floors_at_zero_arrival_after_deficit():
    """Case 11 — when the deficit is nearer than the transit lead time, ship
    floors at 0 and arrival lands AFTER the deficit; the signal is STILL emitted.
    EAST deficit: on_hand 0, ss 0, demand 5 @bucket 1 -> deficit (bucket 1, 5).
    Link WEST->EAST lead_buckets=3 (> deficit_bucket 1):
      ship    = max(0, 1 - 3) = 0
      arrival = 0 + 3 = 3, which is > deficit_bucket 1 -> "covered late".
      qty = min(5, excess 100) = 5 >= min 1 -> emit (NOT dropped).
    Source WEST excess 100 (on_hand 100, no demand).
    """
    signals = core.transfer_signals(
        demand_by_loc={("A", "EAST"): {1: 5.0}},
        on_hand_by_loc={("A", "WEST"): 100.0, ("A", "EAST"): 0.0},
        safety_by_loc={},
        links=[TransferLink("WEST", "EAST", 3, 1.0, None, 1)],
        horizon_buckets=H,
    )
    assert len(signals) == 1
    sig = signals[0]
    assert sig.ship_bucket == 0
    assert sig.arrival_bucket == 3
    assert sig.deficit_bucket == 1
    assert sig.arrival_bucket > sig.deficit_bucket  # late, but present
    assert sig.qty == pytest.approx(5.0)


def test_determinism_insertion_order_independent_and_output_sorted():
    """Case 12 — determinism + the output sort key (item, dest, deficit_bucket,
    priority, source).

    THREE signals engineered so a naive dict-order emission would NOT match the
    documented sort, then asserted byte-identical under two DIFFERENT input
    insertion orders.

    Setup (single item 'A'):
      Destinations & deficits (all on_hand 0, ss 0):
        DEPOT: demand 5 @bucket 0  -> deficit (bucket 0, 5)
        EAST : demand 10 @bucket 2 -> deficit (bucket 2, 10)
        SOUTH: demand 20 @bucket 1 -> deficit (bucket 1, 20)
      Sources (both plenty of excess, no own demand, ss 0):
        WEST : on_hand 100 -> excess 100
        NORTH: on_hand 100 -> excess 100
      Links (all lead 0, min 1, uncapped):
        DEPOT <- WEST  priority 1
        EAST  <- WEST  priority 1
        EAST  <- NORTH priority 2   (redundant on EAST — WEST already covers 10)
        SOUTH <- NORTH priority 1

    #395 F7 review fix (kept, re-verified against the #395 PR2a fair-share
    rewrite): the REAL service order is NOT insertion/label order. Under the
    CURRENT source-first, priority-stratified engine (module §SCOPE), all four
    lanes sit at priority levels {1, 2}; only level 1 matters here (WEST->DEPOT
    p1, WEST->EAST p1, NORTH->SOUTH p1 all fire at level 1; NORTH->EAST p2
    never fires — see below). Within level 1, sources are ordered
    (priority_min, source_location): NORTH (priority_min 1) sorts before WEST
    (priority_min 1) since "NORTH" < "WEST" — but WEST and NORTH share NO
    destination (WEST only feeds DEPOT/EAST, NORTH only feeds SOUTH), so their
    relative order has ZERO effect on any number below; the ONLY ordering that
    actually determines source_excess_before is WEST's OWN destinations, sorted
    alphabetically (active_dests = sorted(...)): DEPOT < EAST, so DEPOT draws
    on WEST'S excess BEFORE EAST does.
      DEPOT (fed by WEST only): fair-share is degenerate for WEST at this level
           on its first-processed destination (DEPOT is alone in WEST's
           dest-loop turn since EAST comes after alphabetically) -> WEST's
           excess snapshot 100 -> signal WEST->DEPOT, qty=5,
           source_excess_before=100 (WEST untouched so far). WEST's live excess
           drops 100 -> 95.
      EAST (fed by WEST at p1, ALSO by NORTH at p2): at level 1, WEST processes
           EAST next (after DEPOT) -> signal WEST->EAST, qty=10,
           source_excess_before=95 (WEST AFTER the DEPOT draw, NOT 100). WEST's
           excess drops 95 -> 85. At level 2, NORTH->EAST would be considered,
           but EAST's residual deficit is already 0 (WEST fully covered it at
           level 1) -> NO second EAST signal (greedy-between-levels: a level-1
           lane's coverage shrinks the residual before level 2 is even reached).
      SOUTH (fed by NORTH only, p1): NORTH's excess snapshot is its full 100 —
           untouched by anything before it (DEPOT/EAST only ever drew on WEST,
           a completely separate source coordinate) -> signal NORTH->SOUTH,
           qty=20, source_excess_before=100.
    Each draw above is itself fair-share-degenerate (exactly one destination
    active per source at that level/turn -> ratio 1.0), so fair_share_qty ==
    qty == the full deficit on every signal; no rounding at the default
    transfer_multiple=1.0 -> rounding_remnant 0.0 throughout. Quantities and
    source_excess_before are UNCHANGED from the pre-fair-share greedy engine —
    re-verified by direct execution against this revised core, not merely
    carried over from the old docstring.

    Expected emitted signals, already in the documented output sort order
    (item, dest, deficit_bucket, priority, source) since dest is alphabetical:
        ('A', 'DEPOT', 0, 1, 'WEST',  qty=5,  source_excess_before=100)
        ('A', 'EAST',  2, 1, 'WEST',  qty=10, source_excess_before=95)
        ('A', 'SOUTH', 1, 1, 'NORTH', qty=20, source_excess_before=100)
    'DEPOT' < 'EAST' < 'SOUTH' alphabetically -> that IS the output order.
    """
    demand = {
        ("A", "EAST"): {2: 10.0},
        ("A", "SOUTH"): {1: 20.0},
        ("A", "DEPOT"): {0: 5.0},
    }
    on_hand = {
        ("A", "WEST"): 100.0,
        ("A", "NORTH"): 100.0,
        ("A", "EAST"): 0.0,
        ("A", "SOUTH"): 0.0,
        ("A", "DEPOT"): 0.0,
    }
    links = [
        TransferLink("WEST", "EAST", 0, 1.0, None, 1),
        TransferLink("NORTH", "EAST", 0, 1.0, None, 2),
        TransferLink("NORTH", "SOUTH", 0, 1.0, None, 1),
        TransferLink("WEST", "DEPOT", 0, 1.0, None, 1),
    ]

    first = core.transfer_signals(demand, on_hand, {}, links, H)

    # Rebuild every input dict/list in a DIFFERENT insertion order — a correct,
    # order-independent engine must return a byte-identical list.
    demand_shuffled = {
        ("A", "DEPOT"): {0: 5.0},
        ("A", "SOUTH"): {1: 20.0},
        ("A", "EAST"): {2: 10.0},
    }
    on_hand_shuffled = {
        ("A", "DEPOT"): 0.0,
        ("A", "SOUTH"): 0.0,
        ("A", "EAST"): 0.0,
        ("A", "NORTH"): 100.0,
        ("A", "WEST"): 100.0,
    }
    links_shuffled = [
        TransferLink("WEST", "DEPOT", 0, 1.0, None, 1),
        TransferLink("NORTH", "SOUTH", 0, 1.0, None, 1),
        TransferLink("NORTH", "EAST", 0, 1.0, None, 2),
        TransferLink("WEST", "EAST", 0, 1.0, None, 1),
    ]
    second = core.transfer_signals(demand_shuffled, on_hand_shuffled, {}, links_shuffled, H)

    assert first == second, "output must be independent of input insertion order"

    # The documented total order AND the source_excess_before evidence on every
    # signal — this is the F7 fix: the previous version of this test asserted
    # only (dest, bucket, qty, source) and so never caught that its own
    # docstring had the service order (and therefore the WEST excess trail)
    # backwards. WEST's excess is 100 for DEPOT (first draw), then 95 for EAST
    # (AFTER DEPOT's draw of 5) — never 90, which would only be true if EAST
    # were served before DEPOT. NORTH is untouched by either WEST draw, so
    # SOUTH sees NORTH's full initial 100.
    assert [
        (s.dest_location, s.deficit_bucket, s.qty, s.source_location, s.source_excess_before)
        for s in first
    ] == [
        ("DEPOT", 0, 5.0, "WEST", 100.0),
        ("EAST", 2, 10.0, "WEST", 95.0),
        ("SOUTH", 1, 20.0, "NORTH", 100.0),
    ]
    # #395 PR2a: every draw above is fair-share-degenerate (one active
    # destination per source at its turn) -> fair_share_qty == qty on all
    # three, no rounding at the default multiple -> rounding_remnant 0.0.
    assert [(s.fair_share_qty, s.rounding_remnant) for s in first] == [
        (5.0, 0.0),
        (10.0, 0.0),
        (20.0, 0.0),
    ]


# ─────────────── #395 review fixes: excess window, item scoping, dedup ───────────────


def test_excess_window_bounded_to_horizon_buckets():
    """#395 F1 — the excess window is IDENTICAL to projected_deficits': demand
    at or beyond horizon_buckets is invisible to BOTH functions, not just the
    deficit side.
    (A, WEST): on_hand=100, safety=0, demand {bucket 50: 50.0}, horizon_buckets=12.
      excess_by_location only sums `t < horizon_buckets` (t=50 is NOT < 12) ->
      total_demand = 0.0 (the out-of-window 50 units are never summed).
      excess = 100 - (0 + 0) = 100.0 — NOT 100 - 50 = 50, which is what a
      time-unbounded sum would have produced.
    Companion check: projected_deficits' walk is `range(horizon_buckets)`
    (0..11), so it never reaches bucket 50 either — WEST itself carries no
    deficit (it is a pure source in this setup, consistent either way).
    """
    demand = {("A", "WEST"): {50: 50.0}}
    on_hand = {("A", "WEST"): 100.0}
    excess = core.excess_by_location(demand, on_hand, {}, horizon_buckets=H)
    assert excess == {("A", "WEST"): 100.0}
    deficits = core.projected_deficits(demand, on_hand, {}, horizon_buckets=H)
    assert ("A", "WEST") not in deficits


def test_excess_window_end_to_end_full_transfer():
    """#395 F1 end-to-end: the windowed excess (100.0, see above) funds a
    same-item deficit INSIDE the horizon in full.
    WEST: on_hand=100, safety=0, demand {50: 50.0} (out of window) -> excess 100.0.
    EAST: on_hand=0, safety=0, demand 80 @bucket 3 (inside window)
        -> bucket 3: pa = 0 - 80 = -80 < 0 -> deficit (bucket 3, 80.0).
    Link WEST->EAST, lead=0, min=1, uncapped:
      qty = min(80, 100) = 80.0 (the full deficit, entirely funded).
      ship = max(0, 3-0) = 3, arrival = 3+0 = 3.
    """
    demand = {("A", "WEST"): {50: 50.0}, ("A", "EAST"): {3: 80.0}}
    on_hand = {("A", "WEST"): 100.0, ("A", "EAST"): 0.0}
    links = [TransferLink("WEST", "EAST", 0, 1.0, None, 1)]
    signals = core.transfer_signals(demand, on_hand, {}, links, horizon_buckets=H)
    # #395 PR2a: single (source, dest) draw -> fair-share degenerate (ratio
    # 1.0), fair_share_qty == qty == the full residual; no rounding at the
    # default transfer_multiple=1.0 -> rounding_remnant 0.
    assert signals == [
        TransferSignal(
            item="A",
            source_location="WEST",
            dest_location="EAST",
            qty=80.0,
            ship_bucket=3,
            arrival_bucket=3,
            deficit_bucket=3,
            deficit_qty=80.0,
            source_excess_before=100.0,
            fair_share_qty=80.0,
            rounding_remnant=0.0,
        )
    ]


def test_item_specific_lane_never_serves_a_different_item():
    """#395 F2 — item scoping: a lane with item="A" set is usable ONLY by item
    A; item B's SAME-shaped deficit at the SAME destination, with excess on the
    SAME source, gets ZERO signals because the only candidate link is scoped
    away from it.
    WEST: excess 100.0 for BOTH item A (on_hand 100, no demand, ss 0) and item B
        (on_hand 100, no demand, ss 0) — so lack of excess is never the reason B
        goes unserved.
    EAST: deficit (bucket 1, 20.0) for BOTH A and B (demand 20 @bucket 1 each,
        on_hand 0, ss 0).
    ONE link WEST->EAST with item="A" (lead 0, min 1, uncapped, prio 1):
      _resolve_candidate_links(dest_links, "B") filters `link.item is None or
      link.item == item` -> "A" is neither None nor == "B" -> EXCLUDED ->
      candidates for B = [] -> deficit is left uncovered, NO signal for B.
      For item A the same link passes the filter (item == "A") -> ONE signal,
      qty = min(20, 100) = 20.
    """
    demand = {("A", "EAST"): {1: 20.0}, ("B", "EAST"): {1: 20.0}}
    on_hand = {
        ("A", "WEST"): 100.0, ("B", "WEST"): 100.0,
        ("A", "EAST"): 0.0, ("B", "EAST"): 0.0,
    }
    links = [TransferLink("WEST", "EAST", 0, 1.0, None, 1, item="A")]
    signals = core.transfer_signals(demand, on_hand, {}, links, horizon_buckets=H)
    assert len(signals) == 1, "item B must get ZERO signals — the only link is scoped to A"
    assert signals[0].item == "A"
    assert signals[0].qty == pytest.approx(20.0)


def test_most_specific_wins_single_signal_not_double_capacity():
    """#395 F3 — specificity-refinement: a generic lane and an item-specific
    lane on the SAME (source, dest) physical pair are NOT two independent lanes
    with summed capacity — the specific lane REPLACES every generic lane on
    that SAME pair for this item (the ONLY case _resolve_candidate_links ever
    excludes a lane; see its docstring), so exactly ONE signal is emitted,
    never two (30 then 50) that would double the pair's real physical
    capacity. Contrast with F6 below: this dedup applies ACROSS specificity
    levels only — two lanes of the SAME specificity on a pair (e.g. two
    generic duplicates) are never evicted this way; they are both kept as
    candidates instead (F6's scenario).
    EAST: deficit (bucket 2, 80.0) for item A (demand 80 @bucket 2, on_hand 0, ss 0).
    WEST: excess 100.0 for item A (on_hand 100, no demand, ss 0).
    TWO links on the SAME pair (WEST, EAST), same priority 1:
      generic       max_qty=30,  item=None
      item-specific max_qty=100, item="A"
    _resolve_candidate_links("A"): both pass the item filter (generic always
    passes; specific matches "A"); the pair (WEST, EAST) has at least one
    item-specific link -> ONLY the item="A" link (max_qty=100) survives as a
    candidate for this pair, REGARDLESS of which link was inserted first
    (asserted both ways below) — the generic one is discarded outright, not
    summed alongside it.
      qty = min(remaining 80, excess 100) = 80, capped by max_qty 100 -> stays 80.
    ONE signal, qty=80 — never two signals (e.g. 30 via generic + 50 via
    specific) that would imply 130 units of capacity on one physical lane.
    """
    demand = {("A", "EAST"): {2: 80.0}}
    on_hand = {("A", "WEST"): 100.0, ("A", "EAST"): 0.0}

    generic = TransferLink("WEST", "EAST", 0, 1.0, 30.0, 1, item=None)
    specific = TransferLink("WEST", "EAST", 0, 1.0, 100.0, 1, item="A")

    for links in ([generic, specific], [specific, generic]):
        signals = core.transfer_signals(demand, on_hand, {}, links, horizon_buckets=H)
        assert len(signals) == 1, "generic + specific on the same pair must yield ONE signal"
        assert signals[0].qty == pytest.approx(80.0)
        assert signals[0].source_excess_before == pytest.approx(100.0)


def test_different_pairs_with_different_item_scoping_are_independent():
    """#395 F4 — pair independence: an item-specific lane on (WEST, EAST) and a
    generic lane on (SOUTH, EAST) are two DIFFERENT physical pairs, so the
    specificity-refinement dedup (which only compares candidates on the SAME
    pair) never touches them — each pair resolves on its own.
    Item A: deficit (bucket 1, 10.0) at EAST; excess 100.0 at WEST (own item-A
        coordinate). Link WEST->EAST scoped item="A" serves it.
    Item B: deficit (bucket 1, 15.0) at EAST; excess 100.0 at SOUTH (own item-B
        coordinate) — NOTE item B has NO excess at WEST at all (no on_hand
        entry for (B, WEST)), so B could only ever be served via SOUTH.
    Link SOUTH->EAST is GENERIC (item=None) -> eligible for B (and would be
    eligible for A too, but A is already fully served by the more specific
    WEST link before SOUTH is even considered for A's deficit).
    Result: TWO independent signals — A via WEST->EAST, B via SOUTH->EAST.
    """
    demand = {("A", "EAST"): {1: 10.0}, ("B", "EAST"): {1: 15.0}}
    on_hand = {
        ("A", "WEST"): 100.0,
        ("B", "SOUTH"): 100.0,
        ("A", "EAST"): 0.0,
        ("B", "EAST"): 0.0,
    }
    links = [
        TransferLink("WEST", "EAST", 0, 1.0, None, 1, item="A"),
        TransferLink("SOUTH", "EAST", 0, 1.0, None, 1, item=None),
    ]
    signals = core.transfer_signals(demand, on_hand, {}, links, horizon_buckets=H)
    by_item = {s.item: s for s in signals}
    assert set(by_item) == {"A", "B"}
    assert by_item["A"].source_location == "WEST"
    assert by_item["A"].qty == pytest.approx(10.0)
    assert by_item["B"].source_location == "SOUTH"
    assert by_item["B"].qty == pytest.approx(15.0)


def test_duplicate_generic_lanes_same_pair_deterministic_link_ref_tiebreak():
    """#395 F6 — determinism for two lanes on the SAME (source, dest) pair at
    the SAME priority, both GENERIC (item=None): _resolve_candidate_links'
    dedup is a SPECIFICITY-REFINEMENT relation ACROSS specificity levels only
    (specific replaces generic on the SAME pair, per F3 above) — it NEVER
    compares two links of EQUAL specificity against each other to evict one.
    Two generic duplicates on the same pair are therefore BOTH kept as
    candidates (see the function's own docstring: "Same-specificity duplicates
    are ... ALL kept as candidates and served SEQUENTIALLY"), and determinism
    comes from the stable sort (priority ASC, source_location, link_ref) that
    decides which one is drained FIRST, not from evicting either.

    Setup: EAST deficit (bucket 2, 100.0); WEST excess 100.0. Two links WEST->
    EAST, same priority 1: link_ref="1" (max_qty=30), link_ref="2" (max_qty=None).
      Both pass item scoping (generic, item=None, eligible for any item).
      Both survive _resolve_candidate_links (same specificity -> no eviction),
      sorted (priority 1, source "WEST", link_ref) -> "1" before "2" (same
      priority, same source, "1" < "2" lexically) REGARDLESS of the order the
      two links were constructed/passed in.
      Greedy drain, in that fixed order:
        link_ref "1" (max_qty=30): qty = min(remaining 100, excess 100) = 100,
          capped by max_qty 30 -> 30. 30 >= min_qty 1 -> EMIT.
          source_excess_before = 100 (untouched so far).
          excess[WEST] -> 100 - 30 = 70. remaining_deficit -> 100 - 30 = 70.
        link_ref "2" (max_qty=None): qty = min(remaining 70, excess 70) = 70
          (no cap). 70 >= min_qty 1 -> EMIT.
          source_excess_before = 70 (AFTER link "1"'s draw, not 100).
          excess[WEST] -> 0. remaining_deficit -> 0.
      TWO signals, TOTAL 30 + 70 = 100 == the full deficit (fully covered by
      the two duplicate lanes together, never double-counted since excess is
      one shared running counter across both draws).
      ship = max(0, 2-0) = 2, arrival = 2 for both (same lead_buckets=0).
    Determinism asserted both ways: insertion order [ref1, ref2] and
    [ref2, ref1] give the IDENTICAL two-signal list.
    """
    demand = {("A", "EAST"): {2: 100.0}}
    on_hand = {("A", "WEST"): 100.0, ("A", "EAST"): 0.0}

    ref1 = TransferLink("WEST", "EAST", 0, 1.0, 30.0, 1, item=None, link_ref="1")
    ref2 = TransferLink("WEST", "EAST", 0, 1.0, None, 1, item=None, link_ref="2")

    # #395 PR2a: ONE (item, dest) coordinate served by TWO lanes from the SAME
    # source at the SAME level -> fair-share degenerate (single destination,
    # ratio 1.0), so dest_part == the full residual (100) for the whole level;
    # the two lanes then split it SEQUENTIALLY (sorted by link_ref) exactly as
    # the old greedy did. Lane "1": raw_part=min(100,100)=100,
    # fair_share_qty=min(100,100)=100 THEN capped by max_qty=30 -> 30 (the
    # capping happens on fair_share_qty too, not just qty); qty=floor(30/1)*1=30
    # (no rounding, mult=1.0) -> rounding_remnant=30-30=0. Lane "2":
    # dest_part now 100-30=70, raw_part=min(70,70)=70, fair_share_qty=min(70,70)
    # =70 (uncapped), qty=70 -> rounding_remnant=0.
    expected = [
        TransferSignal(
            item="A",
            source_location="WEST",
            dest_location="EAST",
            qty=30.0,
            ship_bucket=2,
            arrival_bucket=2,
            deficit_bucket=2,
            deficit_qty=100.0,
            source_excess_before=100.0,
            fair_share_qty=30.0,
            rounding_remnant=0.0,
        ),
        TransferSignal(
            item="A",
            source_location="WEST",
            dest_location="EAST",
            qty=70.0,
            ship_bucket=2,
            arrival_bucket=2,
            deficit_bucket=2,
            deficit_qty=100.0,
            source_excess_before=70.0,
            fair_share_qty=70.0,
            rounding_remnant=0.0,
        ),
    ]

    signals_order1 = core.transfer_signals(demand, on_hand, {}, [ref1, ref2], horizon_buckets=H)
    signals_order2 = core.transfer_signals(demand, on_hand, {}, [ref2, ref1], horizon_buckets=H)

    assert signals_order1 == expected
    assert signals_order2 == expected
    assert sum(s.qty for s in signals_order1) == pytest.approx(100.0), (
        "the two duplicate lanes together cover the full deficit exactly once"
    )
    assert signals_order1 == signals_order2, "dedup outcome must not depend on link insertion order"


def test_empty_inputs_yield_empty_signal_list():
    """Case 13 — no coordinates, no links -> [] (not None, no error)."""
    assert core.transfer_signals({}, {}, {}, [], H) == []
    # Links present but zero demand/on-hand is still empty.
    assert core.transfer_signals(
        {}, {}, {}, [TransferLink("WEST", "EAST", 1, 1.0, None, 1)], H
    ) == []


def test_conservation_combined_multi_source_multi_dest():
    """Case 14 — conservation invariants on a combined multi-source/-dest run:
      (i)  per destination: sum of qty transferred IN <= that dest's deficit_qty.
      (ii) per source: sum of qty drawn OUT <= that source's INITIAL excess.

    Setup (item 'A'):
      Sources (no own demand, ss 0):
        WEST : on_hand 50  -> excess 50
        NORTH: on_hand 30  -> excess 30
      Destinations (on_hand 0, ss 0):
        EAST : demand 60 @bucket 2 -> deficit (bucket 2, 60)
        SOUTH: demand 10 @bucket 3 -> deficit (bucket 3, 10)
      Links (lead 0, min 1, uncapped):
        EAST  <- WEST  priority 1
        EAST  <- NORTH priority 2
        SOUTH <- NORTH priority 1

    #395 PR2a re-derivation (source-first, priority-stratified — module
    §SCOPE): level 1 first. Active level-1 sources = WEST (priority_min 1,
    serves EAST) and NORTH (priority_min 1, via NORTH->SOUTH; NORTH->EAST is
    priority 2, not active yet), ordered (priority_min, source_location) ->
    "NORTH" < "WEST" -> NORTH processed first:
      NORTH @ level 1: only active dest is SOUTH (residual 10) -> fair-share
          degenerate (ratio 1.0) -> dest_part=min(30, 10)=10 -> qty=10.
          NORTH excess 30 -> 20; SOUTH residual 10 -> 0.
      WEST @ level 1: only active dest is EAST (residual 60) -> degenerate ->
          dest_part=min(50, 60)=50 -> qty=50. WEST excess 50 -> 0; EAST
          residual 60 -> 10.
    Level 2: NORTH->EAST becomes active. EAST's residual is now 10 (WEST
    already covered 50 of the 60 at level 1 — greedy BETWEEN levels).
      NORTH @ level 2: only active dest is EAST (residual 10) -> degenerate ->
          dest_part=min(NORTH's remaining excess 20, 10)=10 -> qty=10. NORTH
          excess 20 -> 10; EAST residual 10 -> 0 (fully covered, == its
          deficit, bound (i) tight).
    Every draw above is itself fair-share-degenerate (one active destination
    per source at its turn), so fair_share_qty == qty and rounding_remnant ==
    0.0 on all three signals (default transfer_multiple=1.0, no rounding).
    Draws: WEST out = 50 (== initial 50, bound (ii) tight);
           NORTH out = 10 + 10 = 20 (<= initial 30).
    Quantities/source_excess_before UNCHANGED from the pre-fair-share greedy —
    re-verified by direct execution against this revised core (the mechanism
    that produces them changed; the numbers did not, because WEST and NORTH's
    processing order never actually competes for the SAME destination at the
    SAME level in this dataset).
    """
    demand = {("A", "EAST"): {2: 60.0}, ("A", "SOUTH"): {3: 10.0}}
    on_hand = {
        ("A", "WEST"): 50.0,
        ("A", "NORTH"): 30.0,
        ("A", "EAST"): 0.0,
        ("A", "SOUTH"): 0.0,
    }
    links = [
        TransferLink("WEST", "EAST", 0, 1.0, None, 1),
        TransferLink("NORTH", "EAST", 0, 1.0, None, 2),
        TransferLink("NORTH", "SOUTH", 0, 1.0, None, 1),
    ]
    signals = core.transfer_signals(demand, on_hand, {}, links, H)

    # Independently recompute the initial excess and the deficits to bound against.
    initial_excess = core.excess_by_location(demand, on_hand, {}, H)
    deficits = core.projected_deficits(demand, on_hand, {}, H)

    in_by_dest: dict[tuple[str, str], float] = {}
    out_by_source: dict[tuple[str, str], float] = {}
    for s in signals:
        in_by_dest[(s.item, s.dest_location)] = (
            in_by_dest.get((s.item, s.dest_location), 0.0) + s.qty
        )
        out_by_source[(s.item, s.source_location)] = (
            out_by_source.get((s.item, s.source_location), 0.0) + s.qty
        )

    # (i) never transfer more into a destination than it is short.
    for coord, received in in_by_dest.items():
        assert received <= deficits[coord][1] + 1e-9, f"{coord} over-served"

    # (ii) never draw more out of a source than its initial distributable excess.
    for coord, drawn in out_by_source.items():
        assert drawn <= initial_excess[coord] + 1e-9, f"{coord} over-drawn"

    # Tight-bound spot checks from the derivation above.
    assert in_by_dest[("A", "EAST")] == pytest.approx(60.0)
    assert out_by_source[("A", "WEST")] == pytest.approx(50.0)
    assert out_by_source[("A", "NORTH")] == pytest.approx(20.0)

    # #395 PR2a: per-signal evidence, indexed by (source, dest) since this is a
    # 3-signal, 2-source, 2-dest run (unlike the aggregate bounds above, which
    # do not distinguish the two draws into EAST). Every draw is fair-share-
    # degenerate here (see docstring), so fair_share_qty == qty and
    # rounding_remnant == 0.0 throughout, with the announced
    # source_excess_before values reconfirmed per signal.
    by_source_dest = {(s.source_location, s.dest_location): s for s in signals}
    assert len(by_source_dest) == 3, "WEST->EAST, NORTH->EAST, NORTH->SOUTH — three distinct signals"
    west_east = by_source_dest[("WEST", "EAST")]
    assert west_east.qty == pytest.approx(50.0)
    assert west_east.source_excess_before == pytest.approx(50.0)
    assert west_east.fair_share_qty == pytest.approx(50.0)
    assert west_east.rounding_remnant == pytest.approx(0.0)
    north_east = by_source_dest[("NORTH", "EAST")]
    assert north_east.qty == pytest.approx(10.0)
    assert north_east.source_excess_before == pytest.approx(20.0)
    assert north_east.fair_share_qty == pytest.approx(10.0)
    assert north_east.rounding_remnant == pytest.approx(0.0)
    north_south = by_source_dest[("NORTH", "SOUTH")]
    assert north_south.qty == pytest.approx(10.0)
    assert north_south.source_excess_before == pytest.approx(30.0)
    assert north_south.fair_share_qty == pytest.approx(10.0)
    assert north_south.rounding_remnant == pytest.approx(0.0)


# ───────────────── #395 PR2a: fair-share, logistics rounding, priority switch ─────────────────


def test_fair_share_splits_scarce_source_proportionally_anti_famine():
    """PR2a-1 — fair-share under scarcity: ONE source, insufficient excess to
    cover TWO same-level destinations in full, split PROPORTIONALLY to their
    residual deficits — never first-come-first-served (which would starve the
    second destination to zero).
    Source WEST: on_hand=50, safety=0, no own demand -> excess = 50.
    EAST : demand 30 @bucket 1 -> deficit (bucket 1, 30).
    SOUTH: demand 70 @bucket 1 -> deficit (bucket 1, 70).
    Both linked to WEST only, same priority 1 (one level, both destinations
    active together): total_residual = 30 + 70 = 100.
      part(EAST)  = avail_snapshot(50) * (30/100) = 15.0
      part(SOUTH) = avail_snapshot(50) * (70/100) = 35.0
    Both parts fully fit their own residual (15<=30, 35<=70) and the lane's
    default transfer_multiple=1.0 makes qty == the ideal part exactly (no
    rounding) -> qty(EAST)=15.0, qty(SOUTH)=35.0. 15 + 35 = 50 == the full
    source excess (fully distributed, none wasted). BOTH parts are strictly
    > 0 — the anti-famine property: neither destination is starved to zero
    just because the OTHER one asked for more.
    source_excess_before is the LIVE counter at each individual draw, not the
    frozen snapshot: EAST draws first (processed order: sorted active_dests ->
    "EAST" < "SOUTH") against the untouched live counter (50); SOUTH then
    draws against the live counter AFTER EAST's draw (50 - 15 = 35), even
    though its IDEAL portion was computed from the ORIGINAL 50 snapshot.
    fair_share_qty == qty on both signals (no cap, no rounding) ->
    rounding_remnant 0.0 on both.
    """
    demand = {("A", "EAST"): {1: 30.0}, ("A", "SOUTH"): {1: 70.0}}
    on_hand = {("A", "WEST"): 50.0, ("A", "EAST"): 0.0, ("A", "SOUTH"): 0.0}
    links = [
        TransferLink("WEST", "EAST", 0, 1.0, None, 1),
        TransferLink("WEST", "SOUTH", 0, 1.0, None, 1),
    ]
    signals = core.transfer_signals(demand, on_hand, {}, links, horizon_buckets=H)
    by_dest = {s.dest_location: s for s in signals}

    assert by_dest["EAST"].qty == pytest.approx(15.0)
    assert by_dest["EAST"].fair_share_qty == pytest.approx(15.0)
    assert by_dest["EAST"].rounding_remnant == pytest.approx(0.0)
    assert by_dest["EAST"].source_excess_before == pytest.approx(50.0)

    assert by_dest["SOUTH"].qty == pytest.approx(35.0)
    assert by_dest["SOUTH"].fair_share_qty == pytest.approx(35.0)
    assert by_dest["SOUTH"].rounding_remnant == pytest.approx(0.0)
    assert by_dest["SOUTH"].source_excess_before == pytest.approx(35.0)

    assert by_dest["EAST"].qty > 0 and by_dest["SOUTH"].qty > 0, (
        "anti-famine: neither destination is starved to zero"
    )
    assert by_dest["EAST"].qty + by_dest["SOUTH"].qty == pytest.approx(50.0)


def test_logistics_rounding_down_with_remnant_two_destinations():
    """PR2a-2 — DOWN-rounding to transfer_multiple=10, with the shaved remnant
    recorded (not lost, but not re-servable here since each residual left over
    is itself sub-multiple).
    Source WEST: on_hand=100, safety=0, no own demand -> excess = 100.
    EAST : demand 33 @bucket 1 -> deficit (bucket 1, 33).
    SOUTH: demand 67 @bucket 1 -> deficit (bucket 1, 67).
    Both linked to WEST, same priority 1, BOTH lanes transfer_multiple=10.
    total_residual = 33 + 67 = 100 (conveniently the full excess, so the IDEAL
    fair-share parts equal the deficits exactly, isolating the rounding effect
    from the scarcity effect tested above):
      ideal(EAST)  = 100 * (33/100) = 33.0 -> fair_share_qty = 33.0 (== raw_part,
                     uncapped, avail plentiful).
                     qty = floor(33/10)*10 = 30.0 -> rounding_remnant = 33-30 = 3.0.
      ideal(SOUTH) = 100 * (67/100) = 67.0 -> fair_share_qty = 67.0.
                     qty = floor(67/10)*10 = 60.0 -> rounding_remnant = 67-60 = 7.0.
    Total qty OUT = 30 + 60 = 90.0 <= the initial excess 100 (never over-drawn).
    The down-round remnant (10 units total: 3 + 7) stays in WEST's live excess
    counter (100 - 30 - 60 = 10) but is NOT re-offered to either destination in
    THIS run: EAST's own remaining residual after its draw is 33-30=3 (< the
    10-unit multiple) and SOUTH's is 67-60=7 (< 10) — both sub-multiple, so
    _fair_share_round would floor either to 0 on any further pass; there is no
    third lane/destination in this dataset for the leftover 10 to fund.
    source_excess_before is the LIVE counter at each draw: EAST draws first
    (processed order "EAST" < "SOUTH") against the untouched 100; SOUTH draws
    against the live counter AFTER EAST's draw (100 - 30 = 70).
    """
    demand = {("A", "EAST"): {1: 33.0}, ("A", "SOUTH"): {1: 67.0}}
    on_hand = {("A", "WEST"): 100.0, ("A", "EAST"): 0.0, ("A", "SOUTH"): 0.0}
    links = [
        TransferLink("WEST", "EAST", 0, 1.0, None, 1, transfer_multiple=10.0),
        TransferLink("WEST", "SOUTH", 0, 1.0, None, 1, transfer_multiple=10.0),
    ]
    signals = core.transfer_signals(demand, on_hand, {}, links, horizon_buckets=H)
    by_dest = {s.dest_location: s for s in signals}

    assert by_dest["EAST"].qty == pytest.approx(30.0)
    assert by_dest["EAST"].fair_share_qty == pytest.approx(33.0)
    assert by_dest["EAST"].rounding_remnant == pytest.approx(3.0)
    assert by_dest["EAST"].source_excess_before == pytest.approx(100.0)

    assert by_dest["SOUTH"].qty == pytest.approx(60.0)
    assert by_dest["SOUTH"].fair_share_qty == pytest.approx(67.0)
    assert by_dest["SOUTH"].rounding_remnant == pytest.approx(7.0)
    assert by_dest["SOUTH"].source_excess_before == pytest.approx(70.0)

    total_out = by_dest["EAST"].qty + by_dest["SOUTH"].qty
    assert total_out == pytest.approx(90.0)
    assert total_out <= 100.0


def test_degenerate_residual_below_multiple_yields_no_micro_transfer():
    """PR2a-3 — a residual deficit smaller than one transfer_multiple rounds
    DOWN to exactly 0 -> NO signal at all (no micro-transfer below one case).
    EAST: demand 7 @bucket 1 -> deficit (bucket 1, 7.0).
    WEST: on_hand=100, safety=0, no own demand -> excess=100 (plentiful — this
        case isolates the rounding floor, not a scarcity effect).
    Link WEST->EAST, transfer_multiple=10.0 (single destination -> fair-share
    degenerate, dest_part == the full residual == 7.0):
      raw_part = 7.0; qty_brute = min(7.0, avail=100.0) = 7.0;
      qty = floor(7.0 / 10.0) * 10.0 = floor(0.7) * 10.0 = 0 * 10.0 = 0.0.
    0.0 <= 0 in _fair_share_round -> returns 0.0 -> transfer_signals treats it
    exactly like a below-minimum lane: emit NOTHING, no signal at all (not a
    zero-qty signal — the deficit is left uncovered, honest, since this is
    also the ONLY lane for EAST).
    """
    demand = {("A", "EAST"): {1: 7.0}}
    on_hand = {("A", "WEST"): 100.0, ("A", "EAST"): 0.0}
    links = [TransferLink("WEST", "EAST", 0, 1.0, None, 1, transfer_multiple=10.0)]
    signals = core.transfer_signals(demand, on_hand, {}, links, horizon_buckets=H)
    assert signals == []


def test_priority_level_fully_covers_before_next_priority_activates():
    """PR2a-4 — priority x fair-share interaction: a p1 lane's coverage of a
    destination's residual is applied IN FULL before any p2 lane (a DIFFERENT
    source) is even considered — the greedy-BETWEEN-levels discipline, on top
    of fair-share-WITHIN a level.
    EAST: demand 100 @bucket 1 -> deficit (bucket 1, 100.0).
    WEST : on_hand=40,  safety=0, no own demand -> excess=40.  Link WEST->EAST
        priority=1 (level 1).
    NORTH: on_hand=200, safety=0, no own demand -> excess=200. Link NORTH->EAST
        priority=2 (level 2) — a DIFFERENT source, so no shared excess counter
        with WEST.
    Level 1 (only WEST active — NORTH's lane is priority 2, not yet active):
        WEST is EAST's ONLY active source at this level -> fair-share
        degenerate (ratio 1.0) -> dest_part = min(40, 100) = 40 -> qty = 40.0
        (default transfer_multiple=1.0, no rounding). EAST's residual drops
        100 -> 60 BEFORE level 2 is even reached.
    Level 2 (NORTH activates): EAST's residual is now 60 (NOT the original
        100) -> dest_part = min(200, 60) = 60 -> qty = 60.0.
    Exactly TWO signals, in that priority order: WEST covers 40 first (full
    capacity, source_excess_before=40 — WEST is untouched by anything else),
    THEN NORTH covers the REMAINING 60 (source_excess_before=200 — NORTH is
    ALSO untouched before its own draw, since WEST and NORTH never share an
    excess counter). Total 40 + 60 = 100 == the full deficit.
    """
    demand = {("A", "EAST"): {1: 100.0}}
    on_hand = {("A", "WEST"): 40.0, ("A", "NORTH"): 200.0, ("A", "EAST"): 0.0}
    links = [
        TransferLink("WEST", "EAST", 0, 1.0, None, 1),
        TransferLink("NORTH", "EAST", 0, 1.0, None, 2),
    ]
    signals = core.transfer_signals(demand, on_hand, {}, links, horizon_buckets=H)
    assert len(signals) == 2
    by_source = {s.source_location: s for s in signals}

    assert by_source["WEST"].qty == pytest.approx(40.0)
    assert by_source["WEST"].fair_share_qty == pytest.approx(40.0)
    assert by_source["WEST"].rounding_remnant == pytest.approx(0.0)
    assert by_source["WEST"].source_excess_before == pytest.approx(40.0)

    assert by_source["NORTH"].qty == pytest.approx(60.0)
    assert by_source["NORTH"].fair_share_qty == pytest.approx(60.0)
    assert by_source["NORTH"].rounding_remnant == pytest.approx(0.0)
    assert by_source["NORTH"].source_excess_before == pytest.approx(200.0)

    assert by_source["WEST"].qty + by_source["NORTH"].qty == pytest.approx(100.0)


def test_priority_switch_off_flattens_into_one_fair_share_level(monkeypatch):
    """PR2a-5 — _FAIR_SHARE_RESPECTS_PRIORITY: the SAME dataset with the switch
    True (default) vs False gives DIFFERENT allocations whenever distinct
    priority levels exist, because False collapses every lane into ONE virtual
    fair-share level regardless of priority.
    Source WEST: on_hand=60, safety=0, no own demand -> excess=60.
    EAST : demand 100 @bucket 1 -> deficit (bucket 1, 100.0). Link WEST->EAST
        priority=1.
    SOUTH: demand 100 @bucket 1 -> deficit (bucket 1, 100.0). Link WEST->SOUTH
        priority=2 — SAME source WEST, DIFFERENT priority than the EAST lane.

    Switch True (default): level 1 has ONLY the WEST->EAST lane active (WEST->
    SOUTH is priority 2). WEST's level-1 lanes group -> ONLY EAST is an active
    destination at level 1 -> fair-share degenerate (ratio 1.0) ->
    dest_part=min(60,100)=60 -> qty(EAST)=60.0. At level 2, WEST's excess is
    already 60-60=0 -> avail_snapshot<=0 -> NO signal for SOUTH at all.
    Result: EAST=60.0, SOUTH gets ZERO.

    Switch False: every lane collapses to ONE level [0] regardless of real
    priority -> WEST's level_lanes now include BOTH EAST and SOUTH
    simultaneously -> active_dests=[EAST, SOUTH], total_residual=100+100=200:
      ideal(EAST)  = 60 * (100/200) = 30.0 -> qty(EAST)=30.0
      ideal(SOUTH) = 60 * (100/200) = 30.0 -> qty(SOUTH)=30.0
    Result: EAST=30.0, SOUTH=30.0 — BOTH destinations served, unlike True where
    SOUTH was shut out entirely by EAST's higher-priority full claim.
    30+30=60 == the full WEST excess either way (never over- or under-drawn
    relative to what WEST actually holds); what differs is WHO gets it.

    Constant restored via monkeypatch (auto-teardown) so this test cannot leak
    into any other test's module state.
    """
    demand = {("A", "EAST"): {1: 100.0}, ("A", "SOUTH"): {1: 100.0}}
    on_hand = {("A", "WEST"): 60.0, ("A", "EAST"): 0.0, ("A", "SOUTH"): 0.0}
    links = [
        TransferLink("WEST", "EAST", 0, 1.0, None, 1),
        TransferLink("WEST", "SOUTH", 0, 1.0, None, 2),
    ]

    assert core._FAIR_SHARE_RESPECTS_PRIORITY is True, "module default must be True"
    signals_respecting = core.transfer_signals(demand, on_hand, {}, links, horizon_buckets=H)
    by_dest_respecting = {s.dest_location: s for s in signals_respecting}
    assert set(by_dest_respecting) == {"EAST"}, "SOUTH gets shut out entirely when priority is respected"
    assert by_dest_respecting["EAST"].qty == pytest.approx(60.0)

    monkeypatch.setattr(core, "_FAIR_SHARE_RESPECTS_PRIORITY", False)
    signals_flat = core.transfer_signals(demand, on_hand, {}, links, horizon_buckets=H)
    by_dest_flat = {s.dest_location: s for s in signals_flat}
    assert set(by_dest_flat) == {"EAST", "SOUTH"}, "both destinations served once priority is ignored"
    assert by_dest_flat["EAST"].qty == pytest.approx(30.0)
    assert by_dest_flat["SOUTH"].qty == pytest.approx(30.0)

    # Contrast, explicit: the SAME dataset produces DIFFERENT allocations.
    assert by_dest_respecting["EAST"].qty != by_dest_flat["EAST"].qty


def test_fair_share_round_unit_cases():
    """PR2a-6 — _fair_share_round, the pure helper, exercised directly for
    every documented rule (identity, floor, avail bound, max_qty bound, both
    degenerate-to-zero cases).
    """
    # Identity at mult=1.0: no rounding at all, qty_brute passes through whole.
    assert _fair_share_round(47.0, 1.0, 1.0, None, 100.0) == pytest.approx(47.0)

    # Floor to a whole multiple: floor(47/10)*10 = floor(4.7)*10 = 4*10 = 40.
    assert _fair_share_round(47.0, 10.0, 1.0, None, 100.0) == pytest.approx(40.0)

    # Bounded by avail: qty_brute = min(200, 30) = 30, floor(30/1)*1 = 30 (avail
    # is the binding constraint, not raw_part).
    assert _fair_share_round(200.0, 1.0, 1.0, None, 30.0) == pytest.approx(30.0)

    # Bounded by max_qty: qty_brute = min(200, 100, 15) = 15, floor(15/1)*1=15
    # (max_qty is the binding constraint here, tighter than avail).
    assert _fair_share_round(200.0, 1.0, 1.0, 15.0, 100.0) == pytest.approx(15.0)

    # Degenerate: residual (7) smaller than one multiple (10) -> floor is 0.
    assert _fair_share_round(7.0, 10.0, 1.0, None, 100.0) == pytest.approx(0.0)

    # Below the lane minimum: qty_brute=50, floor(50/1)*1=50, but 50 < min_qty
    # (60) -> the minimum-shipment rule zeroes it out entirely.
    assert _fair_share_round(50.0, 1.0, 60.0, None, 100.0) == pytest.approx(0.0)


def test_transferlink_six_arg_backward_compat_no_rounding():
    """PR2a-7 — a TransferLink built with the ORIGINAL 6 positional arguments
    (no transfer_multiple) defaults to 1.0, which is a no-op multiple: a
    deficit that is NOT itself a multiple of anything (7.0, deliberately
    arbitrary) transfers EXACTLY, undiminished by any rounding — the
    pre-PR2a continuous-transfer behaviour is preserved byte-for-byte for
    every call site that never learned about the new field.
    EAST: demand 7.0 @bucket 1 -> deficit (bucket 1, 7.0).
    WEST: on_hand=100, safety=0, no own demand -> excess=100 (plentiful).
    Single (source, dest) pair -> fair-share degenerate (ratio 1.0) ->
    raw_part == fair_share_qty == 7.0; qty = floor(7.0/1.0)*1.0 = 7.0 exactly
    (NOT floored to some smaller value — mult=1.0 is truly a no-op).
    """
    link = TransferLink("WEST", "EAST", 0, 1.0, None, 1)
    assert link.transfer_multiple == pytest.approx(1.0)

    demand = {("A", "EAST"): {1: 7.0}}
    on_hand = {("A", "WEST"): 100.0, ("A", "EAST"): 0.0}
    signals = core.transfer_signals(demand, on_hand, {}, [link], horizon_buckets=H)
    assert len(signals) == 1
    sig = signals[0]
    assert sig.qty == pytest.approx(7.0)
    assert sig.fair_share_qty == pytest.approx(7.0)
    assert sig.rounding_remnant == pytest.approx(0.0)


# ───────────────── #395 PR2a review fix #2: remnant sweep ─────────────────
# ───────────────── #395 PR2a review fix #1: total-order _sort_key ─────────


def test_sweep_proven_case_one_destination_gets_the_pallet_never_zero_zero():
    """Sweep-1 — THE proven strand case from the module docstring's worked
    example: excess=12, transfer_multiple=12, TWO same-level destinations both
    short 20 -> the proportional split floors BOTH to 0 (0.5 pallet each), so
    the sweep MUST kick in and ship the one full pallet to somebody — never
    leave it stranded as 0/0.
    Source WEST: on_hand=12, safety=0, no own demand -> excess=12.
    D1: demand 20 @bucket 1 -> deficit (bucket 1, 20.0). D2: demand 20 @bucket 1
        -> deficit (bucket 1, 20.0). Both linked to WEST only, mult=12, same
        priority 1 (one level, both destinations active together).
    Proportional pass: total_residual = 20+20 = 40.
      ideal(D1) = 12 * (20/40) = 6.0 -> dest_part=min(6,20)=6.
        raw_part=6, qty_brute=min(6,12)=6, floor(6/12)*12=0 -> qty=0 -> no
        signal, dest_part unchanged (6), residual/excess UNTOUCHED.
      ideal(D2) = 6.0 likewise -> floors to 0 -> no signal.
    excess is STILL 12 after the proportional pass (nothing drawn), residual
    D1=20, D2=20 (both untouched).
    SWEEP: candidates = destinations with residual>0, sorted by
    (-residual, dest_location) -> D1 and D2 tie on residual (20==20) ->
    tie-break alphabetical -> "D1" < "D2" -> D1 chosen FIRST.
      raw_part = residual(D1) = 20 (the FULL remaining need, not a share).
      qty_brute = min(20, avail=12) = 12, floor(12/12)*12 = 12 -> qty=12.
      fair_share_qty = min(raw_part=20, avail=12) = 12 (no max_qty to cap
      further). Signal D1 qty=12. excess drops 12 -> 0.
    Sweep loop again: avail=0 -> break immediately. D2 receives NOTHING (not
    even a zero-qty signal — simply absent). Conservation: total transferred
    (12) <= the initial excess (12), exactly exhausted, never over-drawn.
    """
    demand = {("A", "D1"): {1: 20.0}, ("A", "D2"): {1: 20.0}}
    on_hand = {("A", "WEST"): 12.0, ("A", "D1"): 0.0, ("A", "D2"): 0.0}
    links = [
        TransferLink("WEST", "D1", 0, 1.0, None, 1, transfer_multiple=12.0),
        TransferLink("WEST", "D2", 0, 1.0, None, 1, transfer_multiple=12.0),
    ]
    signals = core.transfer_signals(demand, on_hand, {}, links, horizon_buckets=H)

    assert len(signals) == 1, "exactly ONE destination gets the pallet — never 0/0, never both partially"
    sig = signals[0]
    assert sig.dest_location == "D1", "equal residuals -> alphabetical tie-break picks D1 over D2"
    assert sig.qty == pytest.approx(12.0)
    assert sig.fair_share_qty == pytest.approx(12.0)
    assert sig.rounding_remnant == pytest.approx(0.0)
    assert sig.source_excess_before == pytest.approx(12.0)
    assert sig.qty <= 12.0, "conservation: never transfer more than the source's initial excess"


def test_sweep_asymmetric_serves_most_needy_destination():
    """Sweep-2 — asymmetric residuals: the sweep picks the MOST NEEDY
    destination (largest remaining residual), not merely the alphabetically
    first one — this case has UNEQUAL residuals so the tie-break never enters.
    Source WEST: on_hand=10, safety=0, no own demand -> excess=10.
    A: demand 30 @bucket 1 -> deficit (bucket 1, 30.0) (the MORE needy one).
    B: demand 10 @bucket 1 -> deficit (bucket 1, 10.0).
    Both linked to WEST, mult=10, same priority 1.
    Proportional pass: total_residual = 30+10 = 40.
      ideal(A) = 10 * (30/40) = 7.5 -> qty_brute=min(7.5,10)=7.5,
        floor(7.5/10)*10=0 -> no signal.
      ideal(B) = 10 * (10/40) = 2.5 -> floors to 0 likewise -> no signal.
    excess still 10, residual A=30, residual B=10 (both untouched).
    SWEEP: residuals are UNEQUAL (30 != 10) -> A (30, most needy) sorts
    strictly before B (10) by -residual, no tie-break needed.
      raw_part = residual(A) = 30 (full need). qty_brute=min(30,10)=10,
      floor(10/10)*10=10 -> qty=10. Signal A qty=10. excess drops 10 -> 0.
    Sweep loop again: avail=0 -> break. B receives NOTHING even though it was
    a "smaller", potentially easier-to-satisfy need — "most needy first" means
    largest residual wins the scarce pallet, not smallest.
    Total transferred = 10 == the full source excess (exhausted, conserved).
    """
    demand = {("A", "A"): {1: 30.0}, ("A", "B"): {1: 10.0}}
    on_hand = {("A", "WEST"): 10.0, ("A", "A"): 0.0, ("A", "B"): 0.0}
    links = [
        TransferLink("WEST", "A", 0, 1.0, None, 1, transfer_multiple=10.0),
        TransferLink("WEST", "B", 0, 1.0, None, 1, transfer_multiple=10.0),
    ]
    signals = core.transfer_signals(demand, on_hand, {}, links, horizon_buckets=H)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.dest_location == "A", "most-needy (residual 30) wins the pallet over B (residual 10)"
    assert sig.qty == pytest.approx(10.0)
    assert sig.fair_share_qty == pytest.approx(10.0)
    assert sig.rounding_remnant == pytest.approx(0.0)
    assert sig.qty == pytest.approx(10.0)  # == the full source excess, exhausted


def test_sweep_tie_break_equal_residual_dest_location_alphabetical():
    """Sweep-3 — isolates the tie-break rule itself: TWO destinations with
    EXACTLY equal residual and ONLY one pallet available. Destination labels
    deliberately chosen ("ALPHA"/"BETA") to make the alphabetical rule
    unambiguous (not merely "the first one written in the test").
    Source WEST: on_hand=5, safety=0, no own demand -> excess=5.
    ALPHA: demand 15 @bucket 1 -> deficit (bucket 1, 15.0).
    BETA : demand 15 @bucket 1 -> deficit (bucket 1, 15.0). EQUAL to ALPHA's.
    Both linked to WEST, mult=5, same priority 1.
    Proportional pass: total_residual=30, ideal(ALPHA)=ideal(BETA)=5*(15/30)=2.5
    each -> qty_brute=min(2.5,5)=2.5, floor(2.5/5)*5=0 -> both floor to 0, no
    signal from either. excess still 5, both residuals untouched at 15.
    SWEEP: residuals tie EXACTLY (15==15) -> tie-break is dest_location
    ascending -> "ALPHA" < "BETA" -> ALPHA is chosen (documented 🎯-adjustable
    business default: "most needy first, ties broken by dest_location").
      raw_part=residual(ALPHA)=15, qty_brute=min(15,5)=5, floor(5/5)*5=5 ->
      qty=5. excess drops 5 -> 0. BETA receives NOTHING (sweep breaks
      immediately on the next iteration: avail<=0).
    """
    demand = {("A", "ALPHA"): {1: 15.0}, ("A", "BETA"): {1: 15.0}}
    on_hand = {("A", "WEST"): 5.0, ("A", "ALPHA"): 0.0, ("A", "BETA"): 0.0}
    links = [
        TransferLink("WEST", "ALPHA", 0, 1.0, None, 1, transfer_multiple=5.0),
        TransferLink("WEST", "BETA", 0, 1.0, None, 1, transfer_multiple=5.0),
    ]
    signals = core.transfer_signals(demand, on_hand, {}, links, horizon_buckets=H)

    assert len(signals) == 1
    assert signals[0].dest_location == "ALPHA", (
        "equal residuals (15 == 15) -> dest_location alphabetical tie-break, "
        "ALPHA sorts before BETA"
    )
    assert signals[0].qty == pytest.approx(5.0)


def test_sweep_max_qty_tracked_per_lane_instance_not_by_value():
    """Sweep-4 — max_qty is tracked PER LANE OBJECT (id(link)), not by value:
    two lane INSTANCES that are equal on every field (same source, dest,
    priority, max_qty, min_qty, transfer_multiple) are still two INDEPENDENT
    physical capacities — the sweep (and the proportional pass before it) must
    never let one instance's consumption count against the other's remaining
    max_qty. Constructing a case where this actually changes the OUTPUT (not
    just an internal counter): if the two instances' max_qty were wrongly
    shared, only 10 units total could ever ship (one pallet); tracked
    correctly, 20 units ship (two independent 10-unit pallets).
    Source WEST: on_hand=25, safety=0, no own demand -> excess=25.
    EAST: demand 100 @bucket 1 -> deficit (bucket 1, 100.0). TWO DISTINCT
        TransferLink objects (constructed separately, not aliased) into EAST
        from WEST, both with priority=1, max_qty=10, min_qty=1,
        transfer_multiple=10 (identical field values, but id(lane1) !=
        id(lane2) as Python objects).
    Single destination -> fair-share degenerate (ratio 1.0): dest_part =
    min(avail_snapshot=25, residual=100) = 25.
      Lane 1 (processed first — both lanes compare EQUAL under _sort_key
        since every field matches, so Python's stable sort preserves
        construction/list order): raw_part=min(25,100)=25. remaining_max
        (lane1) = 10 - 0 = 10 (nothing shipped on it yet).
        qty_brute=min(25,25,10)=10, floor(10/10)*10=10 -> qty=10.
        fair_share_qty=min(25,25)=25 THEN capped by remaining_max(10)->10.
        excess drops 25->15. dest_part drops 25->15.
        shipped_by_link[id(lane1)] = 10.
      Lane 2: avail=15, dest_part=15>0. raw_part=min(15, residual=90)=15.
        remaining_max(lane2) = 10 - shipped_by_link.get(id(lane2),0)=10-0=10
        (lane2's OWN counter, untouched by lane1's shipment — the point of
        this test). qty_brute=min(15,15,10)=10, floor(10/10)*10=10 -> qty=10.
        excess drops 15->5. dest_part drops 15->5.
    Both lanes exhaust their OWN 10-unit cap -> the proportional pass alone
    ships 10+10=20 total (2 independent pallets), confirming per-instance
    tracking. (The subsequent sweep pass finds excess=5, both lanes'
    remaining_max now 0 each -> qty_brute capped to 0 on both -> nothing
    further ships; not asserted in detail here since the proportional pass
    already demonstrates the property.)
    """
    demand = {("A", "EAST"): {1: 100.0}}
    on_hand = {("A", "WEST"): 25.0, ("A", "EAST"): 0.0}
    lane1 = TransferLink("WEST", "EAST", 0, 1.0, 10.0, 1, transfer_multiple=10.0, link_ref="lane1")
    lane2 = TransferLink("WEST", "EAST", 0, 1.0, 10.0, 1, transfer_multiple=10.0, link_ref="lane2")
    assert lane1 is not lane2, "two DISTINCT instances, not the same object aliased twice"

    signals = core.transfer_signals(demand, on_hand, {}, [lane1, lane2], horizon_buckets=H)

    assert len(signals) == 2, "each lane instance ships its OWN 10-unit cap -> 20 total, not 10"
    assert sum(s.qty for s in signals) == pytest.approx(20.0)
    for sig in signals:
        assert sig.qty == pytest.approx(10.0)
        assert sig.fair_share_qty == pytest.approx(10.0)
        assert sig.rounding_remnant == pytest.approx(0.0)


def test_sweep_below_min_qty_falls_through_to_next_lane():
    """Sweep-5 — the sweep respects the minimum-shipment rule exactly like the
    proportional pass: a lane whose sweep-rounded qty is below its min_qty
    emits NOTHING and falls through to the next lane at the SAME destination.
    Source WEST: on_hand=12, safety=0, no own demand -> excess=12.
    EAST : demand 20 @bucket 1 -> deficit (bucket 1, 20.0). TWO lanes into
        EAST: lien1 (link_ref="1", min_qty=20 — WILL block), lien2
        (link_ref="2", min_qty=1 — will serve), both mult=12, priority=1.
    SOUTH: demand 20 @bucket 1 -> deficit (bucket 1, 20.0). ONE lane
        (link_ref="3", min_qty=1, mult=12, priority=1) — only present to
        force the proportional split to scarcity (so BOTH lanes floor to 0
        and the sweep is what actually resolves EAST, not the proportional
        pass).
    Proportional pass: total_residual=20+20=40, ideal(EAST)=12*(20/40)=6 ->
      dest_part=min(6,20)=6. Lien1: raw_part=6, qty_brute=min(6,12)=6,
      floor(6/12)*12=0 -> qty=0 (floors to 0 BEFORE min_qty is even checked)
      -> no signal, dest_part unchanged. Lien2: raw_part=6 still, qty_brute=6,
      floor=0 -> qty=0 -> no signal either. ideal(SOUTH)=6 likewise floors to
      0. excess STILL 12, both residuals untouched (EAST=20, SOUTH=20).
    SWEEP: residuals tie (20==20) -> alphabetical -> "EAST" < "SOUTH" -> EAST
    chosen. raw_part=residual(EAST)=20 (full need).
      Lien1 first (link_ref "1" < "2"): qty_brute=min(20,12)=12,
        floor(12/12)*12=12. 12 < min_qty(20) -> qty=0 -> BLOCKED, fall
        through (avail still 12, nothing decremented).
      Lien2: qty_brute=min(20,12)=12, floor=12. 12 >= min_qty(1) -> qty=12 ->
        EMIT via lien2. excess drops 12 -> 0.
    Exactly ONE signal: EAST via lien2, qty=12. SOUTH receives NOTHING (excess
    exhausted before the sweep's next iteration reaches it).
    """
    demand = {("A", "EAST"): {1: 20.0}, ("A", "SOUTH"): {1: 20.0}}
    on_hand = {("A", "WEST"): 12.0, ("A", "EAST"): 0.0, ("A", "SOUTH"): 0.0}
    lien1 = TransferLink("WEST", "EAST", 0, 20.0, None, 1, transfer_multiple=12.0, link_ref="1")
    lien2 = TransferLink("WEST", "EAST", 0, 1.0, None, 1, transfer_multiple=12.0, link_ref="2")
    south_link = TransferLink("WEST", "SOUTH", 0, 1.0, None, 1, transfer_multiple=12.0, link_ref="3")
    links = [lien1, lien2, south_link]

    signals = core.transfer_signals(demand, on_hand, {}, links, horizon_buckets=H)

    assert len(signals) == 1, "EAST served via the fall-through lane, SOUTH gets nothing"
    sig = signals[0]
    assert sig.dest_location == "EAST"
    assert sig.qty == pytest.approx(12.0)
    assert sig.fair_share_qty == pytest.approx(12.0)
    assert sig.rounding_remnant == pytest.approx(0.0)


def test_total_order_determinism_capped_lane_before_uncapped():
    """Determinism-1 (#395 PR2a review fix #1) — two GENERIC lanes on the SAME
    (source, dest, priority), BOTH with link_ref="" (the default — the
    collision _sort_key exists to resolve), differing ONLY by max_qty (30 vs
    None): the OUTPUT must be IDENTICAL regardless of link insertion order, and
    the CAPPED lane (30) must serve BEFORE the UNCAPPED one (None -> +inf).
    Before the #1 fix, link_ref alone was not a total order when both lanes
    left it at the "" default: [a, b] insertion produced 2 signals (30 then
    70, the intended behaviour) but [b, a] produced only 1 signal of 100 (a's
    cap silently NEVER APPLIED because it happened to sort second by
    incidental list order) — the exact bug this test locks shut.
    Source WEST: on_hand=100, safety=0, no own demand -> excess=100.
    EAST: demand 100 @bucket 1 -> deficit (bucket 1, 100.0). Lane A: max_qty=
        30. Lane B: max_qty=None (uncapped). Both priority=1, min_qty=1,
        transfer_multiple=1.0 (default), link_ref="" (default) — every field
        equal EXCEPT max_qty.
    _sort_key(A) = (1, "WEST", "", 30.0, 1.0, 1.0);
    _sort_key(B) = (1, "WEST", "", inf, 1.0, 1.0) -> A < B (30.0 < inf)
    REGARDLESS of which was constructed/passed first.
    Single destination -> fair-share degenerate (ratio 1.0): dest_part=
    min(100,100)=100.
      A (sorted first): raw_part=min(100,100)=100, remaining_max=30,
        qty_brute=min(100,100,30)=30, floor(30/1)*1=30 -> qty=30.
        fair_share_qty=min(100,100)=100 THEN capped by max_qty(30)->30.
        excess drops 100->70. dest_part drops 100->70.
      B: raw_part=min(70,70)=70 (residual EAST now 70), remaining_max=None
        (uncapped), qty_brute=min(70,70)=70, floor=70 -> qty=70.
        fair_share_qty=70. excess drops 70->0.
    TWO signals TOTAL: A=30 THEN B=70 (that exact order, in BOTH insertion
    permutations) -> 30+70=100 == the full deficit, fully covered.
    """
    demand = {("A", "EAST"): {1: 100.0}}
    on_hand = {("A", "WEST"): 100.0, ("A", "EAST"): 0.0}
    lane_a = TransferLink("WEST", "EAST", 0, 1.0, 30.0, 1)
    lane_b = TransferLink("WEST", "EAST", 0, 1.0, None, 1)

    signals_ab = core.transfer_signals(demand, on_hand, {}, [lane_a, lane_b], horizon_buckets=H)
    signals_ba = core.transfer_signals(demand, on_hand, {}, [lane_b, lane_a], horizon_buckets=H)

    assert signals_ab == signals_ba, "output must be independent of link insertion order"
    assert [s.qty for s in signals_ab] == [30.0, 70.0], (
        "the capped lane (30) must be served BEFORE the uncapped one (None -> +inf), "
        "in BOTH insertion orders — before the fix, [b, a] silently dropped the cap"
    )
    assert sum(s.qty for s in signals_ab) == pytest.approx(100.0)


def test_sort_key_unit_cases():
    """Determinism-2 (#395 PR2a review fix #1) — _sort_key, the pure helper,
    exercised directly:
      * max_qty=None maps to +inf (an uncapped lane sorts AFTER every capped
        one at the same priority/source/link_ref).
      * a TOTAL order across all 6 fields: two lanes differing in ANY field
        (here: min_qty) produce DIFFERENT keys, ordered by that field.
      * two lanes equal on EVERY field (even two SEPARATE objects) produce
        EQUAL keys — they are truly interchangeable by this key's own
        definition (the caller's own stable sort then decides between them,
        which is fine since they are identical lanes).
    """
    uncapped = TransferLink("WEST", "EAST", 0, 1.0, None, 1)
    capped = TransferLink("WEST", "EAST", 0, 1.0, 30.0, 1)
    key_uncapped = _sort_key(uncapped)
    key_capped = _sort_key(capped)
    assert key_uncapped == (1, "WEST", "", float("inf"), 1.0, 1.0)
    assert key_capped == (1, "WEST", "", 30.0, 1.0, 1.0)
    assert key_capped < key_uncapped, "capped (finite max_qty) sorts BEFORE uncapped (+inf)"

    # Total order across ALL 6 fields: differ only on min_qty -> different key,
    # ordered by that field (the field the two lanes differ on).
    low_min = TransferLink("WEST", "EAST", 0, 1.0, None, 1)
    high_min = TransferLink("WEST", "EAST", 0, 5.0, None, 1)
    assert _sort_key(low_min) != _sort_key(high_min)
    assert _sort_key(low_min) < _sort_key(high_min)

    # Two DISTINCT objects, every field equal -> EQUAL keys (interchangeable).
    twin_a = TransferLink("WEST", "EAST", 0, 1.0, 30.0, 1, item="X", link_ref="r", transfer_multiple=2.0)
    twin_b = TransferLink("WEST", "EAST", 0, 1.0, 30.0, 1, item="X", link_ref="r", transfer_multiple=2.0)
    assert twin_a is not twin_b
    assert _sort_key(twin_a) == _sort_key(twin_b)


def test_mult_one_multi_dest_scarcity_unaffected_by_sweep():
    """Non-regression — the exact 15/35 fair-share-under-scarcity golden from
    the previous PR2a round (test_fair_share_splits_scarce_source_
    proportionally_anti_famine), re-asserted standalone to prove the sweep
    review fixes NEVER perturb the mult=1.0 (no-rounding) case: the
    proportional pass already covers every residual down to whole units when
    transfer_multiple=1.0, so there is never an undrawn whole multiple left
    for the sweep to find — the sweep loop must run and exit immediately,
    finding nothing to do, on every mult=1 dataset.
    Source WEST: on_hand=50, safety=0, no own demand -> excess=50.
    EAST : demand 30 @bucket 1 -> deficit (bucket 1, 30.0).
    SOUTH: demand 70 @bucket 1 -> deficit (bucket 1, 70.0).
    Both linked to WEST, DEFAULT transfer_multiple=1.0, same priority 1.
    total_residual=100: ideal(EAST)=50*(30/100)=15.0 (raw_part==fair_share_qty,
    no cap, mult=1.0 -> qty=floor(15/1)*1=15 exactly, no rounding at all) ->
    qty(EAST)=15.0, remnant 0.0. ideal(SOUTH)=50*(70/100)=35.0 -> qty(SOUTH)=
    35.0 exactly, remnant 0.0. 15+35=50 == the full excess (fully distributed
    by the proportional pass alone; the sweep finds excess already at 0 and
    exits on its first iteration without shipping anything).
    """
    demand = {("A", "EAST"): {1: 30.0}, ("A", "SOUTH"): {1: 70.0}}
    on_hand = {("A", "WEST"): 50.0, ("A", "EAST"): 0.0, ("A", "SOUTH"): 0.0}
    links = [
        TransferLink("WEST", "EAST", 0, 1.0, None, 1),
        TransferLink("WEST", "SOUTH", 0, 1.0, None, 1),
    ]
    signals = core.transfer_signals(demand, on_hand, {}, links, horizon_buckets=H)
    by_dest = {s.dest_location: s for s in signals}

    assert len(signals) == 2, "the sweep never adds a THIRD signal on a mult=1.0 dataset"
    assert by_dest["EAST"].qty == pytest.approx(15.0)
    assert by_dest["EAST"].rounding_remnant == pytest.approx(0.0)
    assert by_dest["SOUTH"].qty == pytest.approx(35.0)
    assert by_dest["SOUTH"].rounding_remnant == pytest.approx(0.0)
    assert by_dest["EAST"].qty + by_dest["SOUTH"].qty == pytest.approx(50.0)
