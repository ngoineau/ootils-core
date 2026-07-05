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
from ootils_core.engine.drp.core import TransferLink, TransferSignal

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
    assert by_dest["SOUTH"].source_excess_before == pytest.approx(70.0)
    assert by_dest["SOUTH"].qty == pytest.approx(40.0)
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

    #395 F7 review fix: the REAL service order is NOT insertion/label order — it
    is `sorted(deficits)` on the (item, dest) key, i.e. plain alphabetical dest
    order: DEPOT < EAST < SOUTH. So DEPOT is served FIRST, not EAST — the
    previous docstring here had this backwards (it asserted "WEST excess after
    EAST draw = 90" as if EAST went first) and the test PASSED anyway only
    because it never checked source_excess_before. Corrected order below, with
    source_excess_before now asserted on all three signals so a future
    regression to the wrong order fails loudly instead of passing silently.

    Per-destination greedy resolution, in the REAL processed order:
      DEPOT (deficit 5, processed 1st): WEST(p1) covers all 5 -> signal
           WEST->DEPOT, source_excess_before=100 (WEST untouched so far).
           WEST excess drops 100 -> 95.
      EAST (deficit 10, processed 2nd): WEST(p1) covers all 10 -> signal
           WEST->EAST, source_excess_before=95 (WEST AFTER the DEPOT draw, NOT
           100). WEST excess drops 95 -> 85. NORTH(p2) then sees
           remaining_deficit 0 -> break, NO second EAST signal.
      SOUTH (deficit 20, processed 3rd): NORTH(p1) covers 20 -> signal
           NORTH->SOUTH, source_excess_before=100 (NORTH untouched by anything
           before it — DEPOT/EAST only ever drew on WEST).

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

    Greedy (dest order EAST then SOUTH):
      EAST (60): WEST(p1) gives min(60, 50)=50 (WEST excess -> 0);
                 NORTH(p2) gives min(10, 30)=10 (NORTH excess -> 20).
                 EAST covered 60 total == its deficit (bound (i) tight).
      SOUTH (10): NORTH(p1) gives min(10, 20)=10 (NORTH excess -> 10).
    Draws: WEST out = 50 (== initial 50, bound (ii) tight);
           NORTH out = 10 + 10 = 20 (<= initial 30).
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
