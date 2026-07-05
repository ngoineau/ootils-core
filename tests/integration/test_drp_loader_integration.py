"""
tests/integration/test_drp_loader_integration.py — DB-backed tests for
ootils_core.engine.drp.loader.load_drp_data against real Postgres (#395 PR1).

The loader is SELECT-only and strictly (item, location)-keyed and strictly
scenario-scoped (no baseline fallback — a fork sees ONLY the nodes carrying its
own scenario_id). These tests lock:
  1. per-(item, location) grouping is NOT pooled across locations,
  2. lead_buckets = ceil(transit_lead_time_days / 7) off the real column,
  3. the #347 forkability guarantee: a safety-stock override in a fork is
     visible to the DRP load of that fork and invisible to baseline,
  4. scenario-scoping of on-hand: a node seeded only on a fork is absent from
     the baseline load,
  5. distribution_links min/max/priority surface into TransferLink (max NULL
     -> None).

Fixture + seed style mirrors tests/integration/test_param_overlay_integration.py
(function-scoped `conn`, direct-SQL seeding via _seed_item / _seed_location /
_seed_planning_params / _seed_scenario) and the explicit-scenario_id node
seeding of tests/integration/test_param_overlay_propagation_integration.py (the
loader is scenario-scoped, so every node carries an explicit scenario_id — no
ScenarioManager deep-copy needed). No mocks — CLAUDE.md.
"""
from __future__ import annotations

from datetime import date, timedelta
from uuid import UUID, uuid4

from ootils_core.engine.drp.loader import load_drp_data
from ootils_core.engine.scenario.param_overlay import set_param_override

from .conftest import requires_db

pytestmark = requires_db

# Seeded by migration 002 (is_baseline=TRUE) — the only baseline scenario.
BASELINE = UUID("00000000-0000-0000-0000-000000000001")


# ---------------------------------------------------------------------------
# Seed helpers (calqued on test_param_overlay_integration.py)
# ---------------------------------------------------------------------------


def _seed_item(conn, name: str = "drp-item") -> UUID:
    return conn.execute(
        "INSERT INTO items (item_id, external_id, name) VALUES (%s, %s, %s) RETURNING item_id",
        (uuid4(), f"{name}-{uuid4()}", name),
    ).fetchone()["item_id"]


def _seed_location(conn, name: str = "drp-loc") -> UUID:
    return conn.execute(
        "INSERT INTO locations (location_id, external_id, name) "
        "VALUES (%s, %s, %s) RETURNING location_id",
        (uuid4(), f"{name}-{uuid4()}", name),
    ).fetchone()["location_id"]


def _seed_scenario(conn, name: str = "drp-fork") -> UUID:
    """A non-baseline scenario (a fork). The DRP loader is scenario-scoped by
    node.scenario_id; the override overlay is keyed by this scenario_id too, so
    a plain fork row (no ScenarioManager deep-copy) is all we need."""
    return conn.execute(
        "INSERT INTO scenarios (scenario_id, name, is_baseline, status) "
        "VALUES (%s, %s, FALSE, 'active') RETURNING scenario_id",
        (uuid4(), f"{name}-{uuid4()}"),
    ).fetchone()["scenario_id"]


def _seed_planning_params(conn, item_id, location_id, **overrides) -> None:
    """One CURRENT (effective_to NULL) item_planning_params row. Default
    safety_stock_qty=0 so a fork override to a large value is an unambiguous
    signal in the forkability test."""
    defaults = dict(
        lead_time_sourcing_days=14,
        lead_time_manufacturing_days=0,
        lead_time_transit_days=0,
        safety_stock_qty=0,
        min_order_qty=None,
        lot_size_rule="LOTFORLOT",
    )
    defaults.update(overrides)
    conn.execute(
        """
        INSERT INTO item_planning_params (
            item_id, location_id, effective_from, effective_to,
            lead_time_sourcing_days, lead_time_manufacturing_days, lead_time_transit_days,
            safety_stock_qty, min_order_qty, lot_size_rule
        ) VALUES (
            %(item_id)s, %(location_id)s, %(effective_from)s, NULL,
            %(lead_time_sourcing_days)s, %(lead_time_manufacturing_days)s, %(lead_time_transit_days)s,
            %(safety_stock_qty)s, %(min_order_qty)s, %(lot_size_rule)s
        )
        """,
        {"item_id": item_id, "location_id": location_id, "effective_from": date.today(), **defaults},
    )


def _seed_on_hand(conn, *, scenario_id, item_id, location_id, qty) -> UUID:
    """An OnHandSupply node explicitly scoped to `scenario_id` (the DRP loader
    is strictly scenario-scoped — see the #349/#347 seeding pattern)."""
    node_id = uuid4()
    conn.execute(
        """
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            quantity, time_grain, time_ref, active
        ) VALUES (%s, 'OnHandSupply', %s, %s, %s, %s, 'exact_date', CURRENT_DATE, TRUE)
        """,
        (node_id, scenario_id, item_id, location_id, qty),
    )
    return node_id


def _seed_customer_order(conn, *, scenario_id, item_id, location_id, qty, days_out) -> UUID:
    """A CustomerOrderDemand node dated CURRENT_DATE + days_out, scenario-scoped.
    Customer orders map straight to bucket((tref)) = days_out // 7 (no
    _spread_period proration), which keeps the bucket arithmetic exact and
    hand-checkable in these tests."""
    node_id = uuid4()
    conn.execute(
        """
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            quantity, time_grain, time_ref, active
        ) VALUES (
            %s, 'CustomerOrderDemand', %s, %s, %s, %s, 'exact_date',
            CURRENT_DATE + %s, TRUE
        )
        """,
        (node_id, scenario_id, item_id, location_id, qty, timedelta(days=days_out)),
    )
    return node_id


def _seed_link(
    conn,
    *,
    upstream_location_id,
    downstream_location_id,
    transit_lead_time_days,
    minimum_shipment_qty=1,
    maximum_shipment_qty=None,
    priority=100,
    active=True,
) -> UUID:
    """A distribution_links row (migration 029). Columns/defaults read straight
    off the real schema: minimum_shipment_qty NOT NULL DEFAULT 1, priority NOT
    NULL DEFAULT 100 (CHECK >= 1), maximum_shipment_qty NULLABLE."""
    return conn.execute(
        """
        INSERT INTO distribution_links (
            distribution_link_id, upstream_location_id, downstream_location_id,
            transit_lead_time_days, minimum_shipment_qty, maximum_shipment_qty,
            priority, active
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING distribution_link_id
        """,
        (
            uuid4(),
            upstream_location_id,
            downstream_location_id,
            transit_lead_time_days,
            minimum_shipment_qty,
            maximum_shipment_qty,
            priority,
            active,
        ),
    ).fetchone()["distribution_link_id"]


# ---------------------------------------------------------------------------
# 1. Per-(item, location) grouping is NOT pooled across locations
# ---------------------------------------------------------------------------


def test_demand_keyed_per_location_not_pooled(conn):
    """One item, two locations, different demands in different buckets: the
    loader yields TWO distinct (item, location) demand keys, each with its own
    bucket — proving the DRP echelon keys per-site rather than pooling to item
    level (the ONE structural difference from the MRP loader)."""
    item_id = _seed_item(conn)
    east = _seed_location(conn, "drp-east")
    west = _seed_location(conn, "drp-west")
    _seed_planning_params(conn, item_id, east)
    _seed_planning_params(conn, item_id, west)

    item_ext = conn.execute(
        "SELECT external_id FROM items WHERE item_id = %s", (item_id,)
    ).fetchone()["external_id"]
    east_ext = conn.execute(
        "SELECT external_id FROM locations WHERE location_id = %s", (east,)
    ).fetchone()["external_id"]
    west_ext = conn.execute(
        "SELECT external_id FROM locations WHERE location_id = %s", (west,)
    ).fetchone()["external_id"]

    # EAST: 8 units at CURRENT_DATE+15 -> bucket 15//7 = 2.
    _seed_customer_order(conn, scenario_id=BASELINE, item_id=item_id, location_id=east, qty=8, days_out=15)
    # WEST: 20 units at CURRENT_DATE+3 -> bucket 3//7 = 0.
    _seed_customer_order(conn, scenario_id=BASELINE, item_id=item_id, location_id=west, qty=20, days_out=3)
    conn.commit()

    d = load_drp_data(conn, horizon_days=180, scenario=str(BASELINE))

    assert (item_ext, east_ext) in d.demand_by_loc
    assert (item_ext, west_ext) in d.demand_by_loc
    assert d.demand_by_loc[(item_ext, east_ext)] == {2: 8.0}
    assert d.demand_by_loc[(item_ext, west_ext)] == {0: 20.0}
    # Not pooled: the two coordinates are separate keys, never summed to one.
    assert (item_ext, east_ext) != (item_ext, west_ext)


# ---------------------------------------------------------------------------
# 2. lead_buckets = ceil(transit_lead_time_days / 7)
# ---------------------------------------------------------------------------


def test_lead_buckets_is_ceil_of_transit_days_over_seven(conn):
    """The real formula in loader.py is math.ceil(transit_lead_time_days / 7):
    14d -> 2, 10d -> 2 (ceil(1.43)), 7d -> 1. Three links, three sources into
    one dest, asserted by source location."""
    item_id = _seed_item(conn)
    dest = _seed_location(conn, "drp-dest")
    src14 = _seed_location(conn, "drp-src14")
    src10 = _seed_location(conn, "drp-src10")
    src7 = _seed_location(conn, "drp-src7")
    _seed_planning_params(conn, item_id, dest)

    _seed_link(conn, upstream_location_id=src14, downstream_location_id=dest, transit_lead_time_days=14)
    _seed_link(conn, upstream_location_id=src10, downstream_location_id=dest, transit_lead_time_days=10)
    _seed_link(conn, upstream_location_id=src7, downstream_location_id=dest, transit_lead_time_days=7)
    conn.commit()

    d = load_drp_data(conn, horizon_days=180, scenario=str(BASELINE))

    ext = {}
    for loc in (dest, src14, src10, src7):
        ext[loc] = conn.execute(
            "SELECT external_id FROM locations WHERE location_id = %s", (loc,)
        ).fetchone()["external_id"]

    by_source = {lk.source_location: lk for lk in d.links}
    assert by_source[ext[src14]].lead_buckets == 2, "14 / 7 = 2.0 -> ceil 2"
    assert by_source[ext[src10]].lead_buckets == 2, "10 / 7 = 1.43 -> ceil 2"
    assert by_source[ext[src7]].lead_buckets == 1, "7 / 7 = 1.0 -> ceil 1"


# ---------------------------------------------------------------------------
# 3. Forkability (#347) — THE critical DRP guarantee
# ---------------------------------------------------------------------------


def test_safety_stock_override_forkable_and_baseline_unchanged(conn):
    """THE forkability test. Base safety_stock_qty=0 for (item, location). An
    override to 999 inside a fork must be visible in that fork's DRP load
    (safety_by_loc[(item, loc)] == 999) AND baseline must still resolve to 0.
    This is the guarantee that "DRP is forkable" (#347): an agent tests a
    per-site safety-stock counter-factual without forking master data."""
    item_id = _seed_item(conn)
    location_id = _seed_location(conn)
    _seed_planning_params(conn, item_id, location_id, safety_stock_qty=0)
    fork = _seed_scenario(conn)
    set_param_override(
        conn, fork, item_id, "safety_stock_qty", "999", "drp-test",
        location_id=location_id,
    )
    conn.commit()

    item_ext = conn.execute(
        "SELECT external_id FROM items WHERE item_id = %s", (item_id,)
    ).fetchone()["external_id"]
    loc_ext = conn.execute(
        "SELECT external_id FROM locations WHERE location_id = %s", (location_id,)
    ).fetchone()["external_id"]

    forked = load_drp_data(conn, horizon_days=180, scenario=str(fork))
    baseline = load_drp_data(conn, horizon_days=180, scenario=str(BASELINE))

    assert forked.safety_by_loc[(item_ext, loc_ext)] == 999.0, "fork reads its SS override"
    assert baseline.safety_by_loc[(item_ext, loc_ext)] == 0.0, "baseline stays at base SS"


# ---------------------------------------------------------------------------
# 4. Scenario-scoping of on-hand — fork-only node invisible to baseline
# ---------------------------------------------------------------------------


def test_on_hand_seeded_on_fork_only_absent_from_baseline(conn):
    """The loader is strictly scenario-scoped (WHERE n.scenario_id = %(b)s, no
    baseline fallback). An OnHandSupply seeded ONLY on the fork is visible in
    the fork's load and ABSENT from baseline's."""
    item_id = _seed_item(conn)
    location_id = _seed_location(conn)
    _seed_planning_params(conn, item_id, location_id)
    fork = _seed_scenario(conn)
    _seed_on_hand(conn, scenario_id=fork, item_id=item_id, location_id=location_id, qty=42)
    conn.commit()

    item_ext = conn.execute(
        "SELECT external_id FROM items WHERE item_id = %s", (item_id,)
    ).fetchone()["external_id"]
    loc_ext = conn.execute(
        "SELECT external_id FROM locations WHERE location_id = %s", (location_id,)
    ).fetchone()["external_id"]

    forked = load_drp_data(conn, horizon_days=180, scenario=str(fork))
    baseline = load_drp_data(conn, horizon_days=180, scenario=str(BASELINE))

    assert forked.on_hand_by_loc[(item_ext, loc_ext)] == 42.0
    assert (item_ext, loc_ext) not in baseline.on_hand_by_loc


# ---------------------------------------------------------------------------
# 5. Link min/max/priority surface into TransferLink (max NULL -> None)
# ---------------------------------------------------------------------------


def test_link_min_max_priority_surface_into_transferlink(conn):
    """distribution_links minimum_shipment_qty / maximum_shipment_qty /
    priority land on TransferLink; a NULL maximum_shipment_qty becomes
    max_qty=None (uncapped)."""
    item_id = _seed_item(conn)
    dest = _seed_location(conn, "drp-dest")
    src_capped = _seed_location(conn, "drp-src-capped")
    src_uncapped = _seed_location(conn, "drp-src-uncapped")
    _seed_planning_params(conn, item_id, dest)

    _seed_link(
        conn, upstream_location_id=src_capped, downstream_location_id=dest,
        transit_lead_time_days=7, minimum_shipment_qty=5, maximum_shipment_qty=50, priority=1,
    )
    _seed_link(
        conn, upstream_location_id=src_uncapped, downstream_location_id=dest,
        transit_lead_time_days=7, minimum_shipment_qty=2, maximum_shipment_qty=None, priority=3,
    )
    conn.commit()

    d = load_drp_data(conn, horizon_days=180, scenario=str(BASELINE))

    capped_ext = conn.execute(
        "SELECT external_id FROM locations WHERE location_id = %s", (src_capped,)
    ).fetchone()["external_id"]
    uncapped_ext = conn.execute(
        "SELECT external_id FROM locations WHERE location_id = %s", (src_uncapped,)
    ).fetchone()["external_id"]

    by_source = {lk.source_location: lk for lk in d.links}

    capped = by_source[capped_ext]
    assert capped.min_qty == 5.0
    assert capped.max_qty == 50.0
    assert capped.priority == 1

    uncapped = by_source[uncapped_ext]
    assert uncapped.min_qty == 2.0
    assert uncapped.max_qty is None, "NULL maximum_shipment_qty -> None (uncapped)"
    assert uncapped.priority == 3


def test_inactive_link_excluded(conn):
    """Belt on the WHERE dl.active filter: an inactive link never surfaces as a
    TransferLink."""
    item_id = _seed_item(conn)
    dest = _seed_location(conn, "drp-dest")
    src = _seed_location(conn, "drp-src")
    _seed_planning_params(conn, item_id, dest)

    _seed_link(
        conn, upstream_location_id=src, downstream_location_id=dest,
        transit_lead_time_days=7, active=False,
    )
    conn.commit()

    src_ext = conn.execute(
        "SELECT external_id FROM locations WHERE location_id = %s", (src,)
    ).fetchone()["external_id"]

    d = load_drp_data(conn, horizon_days=180, scenario=str(BASELINE))
    assert src_ext not in {lk.source_location for lk in d.links}


# ---------------------------------------------------------------------------
# #395 review fixes: fallback key, CO clip, item-specific lanes, ORDER BY
# ---------------------------------------------------------------------------


def _seed_item_no_external_id(conn, name: str = "drp-item-null") -> UUID:
    """An item row with external_id explicitly NULL. The UNIQUE constraint on
    items.external_id (migration 007) is a plain UNIQUE, not a NOT NULL — SQL
    NULLs are never equal to each other under UNIQUE, so any number of rows may
    carry a NULL external_id without collision. This is what the loader's F4
    fallback-key fix (COALESCE(external_id, item_id::text)) exists to handle."""
    return conn.execute(
        "INSERT INTO items (item_id, external_id, name) VALUES (%s, NULL, %s) "
        "RETURNING item_id",
        (uuid4(), name),
    ).fetchone()["item_id"]


def _seed_location_no_external_id(conn, name: str = "drp-loc-null") -> UUID:
    """A location row with external_id explicitly NULL — same NULL-tolerant
    UNIQUE rationale as _seed_item_no_external_id."""
    return conn.execute(
        "INSERT INTO locations (location_id, external_id, name) VALUES (%s, NULL, %s) "
        "RETURNING location_id",
        (uuid4(), name),
    ).fetchone()["location_id"]


def test_fallback_key_used_when_external_id_is_null(conn):
    """#395 F4, THE critical case: an item AND a location seeded with NO
    external_id (NULL) must NOT collapse onto a shared "None" coordinate — the
    loader keys them by COALESCE(external_id, <uuid>::text), i.e. the row's own
    stringified UUID as a stable fallback. We seed one on-hand node for this
    (item, location) pair with NO external_id anywhere and confirm the loader's
    on_hand_by_loc key is the (item_id, location_id) UUIDs stringified — never
    the literal strings "None"/"None" that a naive f"{external_id}" key would
    have produced for every un-backfilled row alike."""
    item_id = _seed_item_no_external_id(conn)
    location_id = _seed_location_no_external_id(conn)
    _seed_planning_params(conn, item_id, location_id)
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_id, location_id=location_id, qty=7)
    conn.commit()

    d = load_drp_data(conn, horizon_days=180, scenario=str(BASELINE))

    expected_key = (str(item_id), str(location_id))
    assert expected_key in d.on_hand_by_loc, (
        "the (item, location) coordinate must key by the stringified UUIDs when "
        "external_id is NULL, not by a literal 'None' placeholder"
    )
    assert d.on_hand_by_loc[expected_key] == 7.0
    assert ("None", "None") not in d.on_hand_by_loc


def test_fallback_key_two_items_without_external_id_on_same_location_stay_distinct(conn):
    """#395 F4: TWO items, BOTH with a NULL external_id, sharing the SAME
    location (which DOES carry an external_id) — a naive f"{None}" key would
    collapse both items' on-hand onto ONE coordinate ("None", loc_ext) and sum
    their quantities together. The UUID fallback keeps them on two DISTINCT
    coordinates, each with its own quantity."""
    item_a = _seed_item_no_external_id(conn, "drp-item-null-a")
    item_b = _seed_item_no_external_id(conn, "drp-item-null-b")
    location_id = _seed_location(conn, "drp-shared-loc")  # has a real external_id
    _seed_planning_params(conn, item_a, location_id)
    _seed_planning_params(conn, item_b, location_id)
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_a, location_id=location_id, qty=11)
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_b, location_id=location_id, qty=22)
    conn.commit()

    loc_ext = conn.execute(
        "SELECT external_id FROM locations WHERE location_id = %s", (location_id,)
    ).fetchone()["external_id"]

    d = load_drp_data(conn, horizon_days=180, scenario=str(BASELINE))

    key_a = (str(item_a), loc_ext)
    key_b = (str(item_b), loc_ext)
    assert key_a != key_b, "two NULL-external_id items must never resolve to the same coordinate"
    assert d.on_hand_by_loc[key_a] == 11.0
    assert d.on_hand_by_loc[key_b] == 22.0
    # Neither quantity is silently summed onto a shared "None" coordinate.
    assert d.on_hand_by_loc[key_a] + d.on_hand_by_loc[key_b] == 33.0


def test_fallback_key_item_with_and_without_external_id_coexist(conn):
    """#395 F4: one item WITH a real external_id and one item WITHOUT (NULL),
    same location, coexist without collision — the business-key item resolves
    to its readable external_id, the NULL one falls back to its UUID, and
    neither shadows the other."""
    item_named = _seed_item(conn, "drp-item-named")
    item_null = _seed_item_no_external_id(conn, "drp-item-anon")
    location_id = _seed_location(conn, "drp-coexist-loc")
    _seed_planning_params(conn, item_named, location_id)
    _seed_planning_params(conn, item_null, location_id)
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_named, location_id=location_id, qty=5)
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_null, location_id=location_id, qty=9)
    conn.commit()

    item_named_ext = conn.execute(
        "SELECT external_id FROM items WHERE item_id = %s", (item_named,)
    ).fetchone()["external_id"]
    loc_ext = conn.execute(
        "SELECT external_id FROM locations WHERE location_id = %s", (location_id,)
    ).fetchone()["external_id"]

    d = load_drp_data(conn, horizon_days=180, scenario=str(BASELINE))

    assert d.on_hand_by_loc[(item_named_ext, loc_ext)] == 5.0
    assert d.on_hand_by_loc[(str(item_null), loc_ext)] == 9.0


def test_customer_order_beyond_horizon_end_is_clipped(conn):
    """#395 F1 (loader side): a CustomerOrderDemand dated horizon_start + 400
    days, loaded with horizon_days=180 (horizon_end = start + 180), must be
    ABSENT from demand_by_loc entirely — clipped by the upper bound
    `horizon_start <= tref <= horizon_end`, symmetric with the forecast side's
    _spread_period confinement. Before the fix only the lower bound was
    enforced and this order would have landed in a bucket far past n_buckets,
    invisible to the core's windowed excess/deficit but still lingering in the
    raw dict."""
    item_id = _seed_item(conn)
    location_id = _seed_location(conn)
    _seed_planning_params(conn, item_id, location_id)
    _seed_customer_order(
        conn, scenario_id=BASELINE, item_id=item_id, location_id=location_id,
        qty=99, days_out=400,
    )
    conn.commit()

    item_ext = conn.execute(
        "SELECT external_id FROM items WHERE item_id = %s", (item_id,)
    ).fetchone()["external_id"]
    loc_ext = conn.execute(
        "SELECT external_id FROM locations WHERE location_id = %s", (location_id,)
    ).fetchone()["external_id"]

    d = load_drp_data(conn, horizon_days=180, scenario=str(BASELINE))
    assert (item_ext, loc_ext) not in d.demand_by_loc, (
        "a customer order 400 days out on a 180-day horizon must be clipped, "
        "not silently carried into demand_by_loc"
    )


def test_link_item_id_resolves_to_item_key_null_stays_none(conn):
    """#395 F2/F3 (loader side): distribution_links.item_id, when set, resolves
    onto TransferLink.item as the SAME (item, location) key convention used
    everywhere else in the loader; when NULL, TransferLink.item is None (a
    generic lane) — the LEFT JOIN items must NOT drop the link row just
    because item_id is NULL."""
    item_id = _seed_item(conn, "drp-lane-item")
    src = _seed_location(conn, "drp-lane-src")
    generic_src = _seed_location(conn, "drp-lane-generic-src")
    dest = _seed_location(conn, "drp-lane-dest")
    _seed_planning_params(conn, item_id, dest)

    specific_link_id = conn.execute(
        """
        INSERT INTO distribution_links (
            distribution_link_id, upstream_location_id, downstream_location_id,
            item_id, transit_lead_time_days, priority
        ) VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING distribution_link_id
        """,
        (uuid4(), src, dest, item_id, 7, 1),
    ).fetchone()["distribution_link_id"]
    generic_link_id = conn.execute(
        """
        INSERT INTO distribution_links (
            distribution_link_id, upstream_location_id, downstream_location_id,
            item_id, transit_lead_time_days, priority
        ) VALUES (%s, %s, %s, NULL, %s, %s)
        RETURNING distribution_link_id
        """,
        (uuid4(), generic_src, dest, 7, 2),
    ).fetchone()["distribution_link_id"]
    conn.commit()

    item_ext = conn.execute(
        "SELECT external_id FROM items WHERE item_id = %s", (item_id,)
    ).fetchone()["external_id"]

    d = load_drp_data(conn, horizon_days=180, scenario=str(BASELINE))
    links_by_ref = {lk.link_ref: lk for lk in d.links}

    specific = links_by_ref[str(specific_link_id)]
    generic = links_by_ref[str(generic_link_id)]
    assert specific.item == item_ext, "item_id set -> TransferLink.item resolves to its key"
    assert generic.item is None, (
        "item_id NULL -> TransferLink.item is None (generic); the LEFT JOIN "
        "must not drop the row"
    )


def test_links_order_by_is_stable_across_calls(conn):
    """#395 F6 (loader side): two links on the SAME (priority, source, dest)
    triple, differing only by distribution_link_id, must come back in the SAME
    relative order on every call — the loader's ORDER BY (priority, source,
    dest, distribution_link_id) makes that order deterministic (the smaller
    UUID sorts first), never physical scan order."""
    item_id = _seed_item(conn)
    src = _seed_location(conn, "drp-dup-src")
    dest = _seed_location(conn, "drp-dup-dest")
    _seed_planning_params(conn, item_id, dest)

    link_a = _seed_link(
        conn, upstream_location_id=src, downstream_location_id=dest,
        transit_lead_time_days=7, priority=5,
    )
    link_b = _seed_link(
        conn, upstream_location_id=src, downstream_location_id=dest,
        transit_lead_time_days=7, priority=5,
    )
    conn.commit()

    expected_ref_order = sorted([str(link_a), str(link_b)])

    d1 = load_drp_data(conn, horizon_days=180, scenario=str(BASELINE))
    d2 = load_drp_data(conn, horizon_days=180, scenario=str(BASELINE))

    refs1 = [lk.link_ref for lk in d1.links if lk.link_ref in {str(link_a), str(link_b)}]
    refs2 = [lk.link_ref for lk in d2.links if lk.link_ref in {str(link_a), str(link_b)}]

    assert refs1 == expected_ref_order, "ORDER BY ... distribution_link_id sorts the smaller UUID first"
    assert refs1 == refs2, "the relative order must be stable across two separate calls"
