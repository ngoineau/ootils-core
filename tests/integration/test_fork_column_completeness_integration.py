"""tests/integration/test_fork_column_completeness_integration.py — fork column
completeness guard (fix for the fork-loses-is_firm incident).

DB-backed coverage of ScenarioManager.create_scenario's deep-copy INSERT
(engine/scenario/manager.py:_copy_nodes) against a real Postgres, no mocks.
Two layers, per the column-coverage spec in the _copy_nodes docstring:

  (a) Targeted FPO regression — a baseline PlannedSupply with is_firm=TRUE
      must keep is_firm=TRUE on its scenario copy; and a RELEASE whose
      parent_node_id points at a RECEIPT of the same scenario must, after the
      fork, point at the NEW node_id of the copied receipt (the `pm` self-join
      remap on _node_map), never at the old cross-scenario node_id.

  (b) Generic anti-regression guard — introspect information_schema.columns
      for `nodes`, subtract an EXPLICIT, justified exclusion list, and compare
      every remaining column strictly between the baseline node and its copy.
      A future `nodes` column that the fork INSERT forgets, OR that is not
      added to the documented exclusions here, MUST fail this test. The
      false-green trap is closed twice: a compare-set column missing from the
      seed fails loudly, and a compare-set column NULL on BOTH sides fails
      loudly ("column not covered by the seed") instead of passing as
      NULL == NULL.

Determinism: fixed absolute dates — no engine runs here, only the copy.
dict_row throughout — columns accessed by NAME. The `conn` fixture rolls the
whole test back, so each test starts from a clean baseline.
"""
from __future__ import annotations

import datetime as _dt
from decimal import Decimal
from uuid import UUID, uuid4

from .conftest import requires_db

from ootils_core.engine.scenario.manager import ScenarioManager

pytestmark = requires_db

# Baseline sentinel UUID (seeded by migration 002).
BASELINE = UUID("00000000-0000-0000-0000-000000000001")

_D0 = _dt.date(2026, 7, 1)  # fixed anchor — the copy is date-agnostic


# ---------------------------------------------------------------------------
# Seed helpers (mirror the _node()/_seed_common() conventions of
# test_fpo_lifecycle_integration.py, extended with the MRP identity columns).
# ---------------------------------------------------------------------------


def _seed_common(conn):
    """One location + one item, enough for FK-valid nodes (no engine runs)."""
    loc_id = conn.execute(
        "INSERT INTO locations (name, location_type, external_id) "
        "VALUES (%s, %s, %s) RETURNING location_id",
        ("Fork Guard Plant", "plant", "LOC-FORK-GUARD"),
    ).fetchone()["location_id"]
    item_id = conn.execute(
        "INSERT INTO items (external_id, name, item_type, standard_cost, cost_currency) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING item_id",
        ("ITM-FORK-GUARD", "Fork Guard Item", "component", 40.0, "EUR"),
    ).fetchone()["item_id"]
    return loc_id, item_id


def _node(
    conn,
    ntype,
    scenario,
    item_id,
    loc_id,
    when,
    qty,
    *,
    is_firm=False,
    external_id=None,
    planned_order_type=None,
    parent_node_id=None,
):
    """Insert one active node at an exact date; returns its node_id."""
    return conn.execute(
        "INSERT INTO nodes (node_type, scenario_id, item_id, location_id, quantity, "
        " time_grain, time_ref, active, is_firm, external_id, planned_order_type, "
        " parent_node_id) "
        "VALUES (%s, %s, %s, %s, %s, 'exact_date', %s, TRUE, %s, %s, %s, %s) "
        "RETURNING node_id",
        (
            ntype,
            str(scenario),
            item_id,
            loc_id,
            qty,
            when,
            is_firm,
            external_id,
            planned_order_type,
            parent_node_id,
        ),
    ).fetchone()["node_id"]


def _fork(conn, name):
    """Fork the baseline; returns the new scenario_id."""
    return ScenarioManager().create_scenario(name, BASELINE, conn).scenario_id


def _copy_of(conn, scenario_id, external_id):
    """Fetch THE copied node of a fork by its (copied-verbatim) external_id."""
    rows = conn.execute(
        "SELECT * FROM nodes WHERE scenario_id = %s AND external_id = %s",
        (str(scenario_id), external_id),
    ).fetchall()
    assert len(rows) == 1, (
        f"expected exactly one copy of external_id={external_id!r} in the fork, "
        f"got {len(rows)}"
    )
    return rows[0]


# ---------------------------------------------------------------------------
# (a) Targeted FPO regression — is_firm survives the fork.
# ---------------------------------------------------------------------------


@requires_db
def test_fork_preserves_is_firm_on_planned_supply(conn):
    """A baseline PlannedSupply with is_firm=TRUE must keep is_firm=TRUE on its
    scenario copy — the exact fork-loses-is_firm regression (migration 061
    column silently omitted from the _copy_nodes INSERT)."""
    loc_id, item_id = _seed_common(conn)
    src_id = _node(
        conn, "PlannedSupply", BASELINE, item_id, loc_id, _D0, 100,
        is_firm=True, external_id="EXT-FPO-FIRM",
    )

    fork_id = _fork(conn, "fork-isfirm-regression")

    copy = _copy_of(conn, fork_id, "EXT-FPO-FIRM")
    assert copy["node_id"] != src_id, "the copy must carry a fresh node_id"
    assert copy["is_firm"] is True, (
        "fork dropped is_firm: a firmed PlannedSupply (FPO) forked into a "
        "scenario must stay firm, or every counter-factual silently un-firms it"
    )


@requires_db
def test_fork_remaps_release_parent_to_copied_receipt(conn):
    """A RELEASE whose parent_node_id points at a RECEIPT of the same scenario
    must, after the fork, point at the NEW node_id of the copied receipt (the
    `pm` self-join remap on _node_map) — never at the old baseline node_id,
    which would be a dangling cross-scenario reference."""
    loc_id, item_id = _seed_common(conn)
    receipt_id = _node(
        conn, "PlannedSupply", BASELINE, item_id, loc_id, _D0, 100,
        planned_order_type="RECEIPT", external_id="EXT-RCPT",
    )
    _node(
        conn, "PlannedSupply", BASELINE, item_id, loc_id,
        _D0 - _dt.timedelta(days=14), 100,
        planned_order_type="RELEASE", external_id="EXT-RLSE",
        parent_node_id=receipt_id,
    )

    fork_id = _fork(conn, "fork-parent-remap")

    copied_receipt = _copy_of(conn, fork_id, "EXT-RCPT")
    copied_release = _copy_of(conn, fork_id, "EXT-RLSE")
    assert copied_release["parent_node_id"] is not None, (
        "fork dropped parent_node_id: the copied RELEASE lost its RECEIPT link"
    )
    assert copied_release["parent_node_id"] != receipt_id, (
        "fork copied parent_node_id verbatim: the copied RELEASE points at the "
        "OLD baseline receipt (cross-scenario dangling reference), not its own "
        "copied sibling"
    )
    assert copied_release["parent_node_id"] == copied_receipt["node_id"], (
        "fork mis-remapped parent_node_id: the copied RELEASE must point at "
        "the copied RECEIPT's new node_id"
    )


# ---------------------------------------------------------------------------
# (b) Generic anti-regression guard — every `nodes` column is either copied
#     verbatim (and proven so) or explicitly excluded with a justification.
# ---------------------------------------------------------------------------

# Columns EXPLICITLY excluded from the verbatim-copy comparison. Each entry
# carries its justification — an undocumented exclusion is a test failure.
# Keep in lockstep with the column-coverage block in the _copy_nodes docstring
# (src/ootils_core/engine/scenario/manager.py).
EXCLUDED_COLUMNS: dict[str, str] = {
    # -- new identity, minted per copy -------------------------------------
    "node_id": "fresh UUID per copy (gen_random_uuid via _node_map)",
    "scenario_id": "the whole point of the fork — set to the target scenario",
    "created_at": "stamped NOW() at copy time",
    "updated_at": "stamped NOW() at copy time",
    # -- remapped references (not verbatim by design) -----------------------
    "projection_series_id": "remapped via _series_map to the fork's own series",
    "parent_node_id": (
        "remapped via the pm self-join on _node_map — covered by "
        "test_fork_remaps_release_parent_to_copied_receipt above"
    ),
    # -- deliberately reset to NULL on the copy (provenance / anti-replay,
    #    see the _copy_nodes docstring) — verified IS NULL below ------------
    "mrp_run_id": "reset to NULL: the copy was not produced by that MRP run",
    "last_calc_seq": "reset to NULL: rust-svc anti-replay guard is per-node_id",
    "last_calc_run_id": "reset to NULL: that calc_run ran against the SOURCE scenario",
    # -- deliberately reset to a fixed engine state — verified below --------
    "is_dirty": "reset to FALSE after copy (fork starts clean)",
    "active": "set to TRUE (only active nodes are copied)",
}

# Columns the copy must have RESET rather than copied — each is seeded with a
# non-trivial value on the baseline node so the reset is actually proven
# (copying a NULL would pass vacuously).
_RESET_TO_NULL = ("mrp_run_id", "last_calc_seq", "last_calc_run_id")

# Non-trivial, non-NULL seed value for every column of the verbatim-compare
# set. If a future migration adds a `nodes` column, it must be seeded here
# (and copied by _copy_nodes) or added to EXCLUDED_COLUMNS with a
# justification — anything else fails the test below.
def _seed_values(item_id, loc_id):
    return {
        "node_type": "PlannedSupply",
        "item_id": item_id,
        "location_id": loc_id,
        "quantity": Decimal("123.45"),
        "qty_uom": "CASE",
        "time_grain": "exact_date",
        "time_ref": _D0,
        "time_span_start": _D0,
        "time_span_end": _D0 + _dt.timedelta(days=7),
        "bucket_sequence": 7,
        "opening_stock": Decimal("11.5"),
        "inflows": Decimal("22.25"),
        "outflows": Decimal("33.75"),
        "closing_stock": Decimal("44.125"),
        "has_shortage": True,
        "shortage_qty": Decimal("9.5"),
        "has_exact_date_inputs": True,
        "has_week_inputs": True,
        "has_month_inputs": True,
        "external_id": "EXT-FORK-COVERAGE",
        "is_firm": True,
        "planned_order_type": "RECEIPT",
    }


@requires_db
def test_fork_copies_every_node_column_or_documents_exclusion(conn):
    """The core guard: information_schema.columns(nodes) minus the documented
    exclusions must be copied verbatim by the fork, column by column, strictly.
    A compare-set column that is NULL on both sides fails explicitly (the
    false-green trap), as does a compare-set column missing from the seed."""
    loc_id, item_id = _seed_common(conn)

    # A real calc_runs row so the baseline node can carry a non-NULL
    # last_calc_run_id (FK) — proving the copy RESETS it, not that it copied
    # a NULL.
    calc_run_id = conn.execute(
        "INSERT INTO calc_runs (scenario_id, status) "
        "VALUES (%s, 'completed') RETURNING calc_run_id",
        (str(BASELINE),),
    ).fetchone()["calc_run_id"]

    seed = _seed_values(item_id, loc_id)
    reset_seed = {
        "scenario_id": str(BASELINE),
        "is_dirty": True,        # copy must come out FALSE
        "active": True,          # only active nodes are copied; copy stays TRUE
        "mrp_run_id": uuid4(),   # copy must come out NULL
        "last_calc_seq": 42,     # copy must come out NULL
        "last_calc_run_id": calc_run_id,  # copy must come out NULL
    }
    insert_cols = {**seed, **reset_seed}
    src_id = conn.execute(
        f"INSERT INTO nodes ({', '.join(insert_cols)}) "
        f"VALUES ({', '.join(['%s'] * len(insert_cols))}) RETURNING node_id",
        list(insert_cols.values()),
    ).fetchone()["node_id"]

    # ---- dynamic column set --------------------------------------------
    all_columns = [
        r["column_name"]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'nodes' "
            "ORDER BY ordinal_position"
        ).fetchall()
    ]
    assert all_columns, "introspection returned no columns for `nodes`"

    stale_exclusions = set(EXCLUDED_COLUMNS) - set(all_columns)
    assert not stale_exclusions, (
        f"stale exclusion(s) {sorted(stale_exclusions)}: documented as excluded "
        "but no longer a `nodes` column — prune EXCLUDED_COLUMNS"
    )

    # THE compare set: everything not explicitly excluded, built dynamically
    # so a future migration-added column lands here automatically.
    compare_set = [c for c in all_columns if c not in EXCLUDED_COLUMNS]

    unseeded = set(compare_set) - set(seed)
    assert not unseeded, (
        f"column(s) {sorted(unseeded)} are in the verbatim-compare set but not "
        "covered by the seed: a new `nodes` column must either get a "
        "non-trivial seed value here (and be copied by _copy_nodes) or be "
        "added to EXCLUDED_COLUMNS with a written justification"
    )

    # ---- fork ------------------------------------------------------------
    fork_id = _fork(conn, "fork-column-completeness")

    baseline_row = conn.execute(
        "SELECT * FROM nodes WHERE node_id = %s", (src_id,)
    ).fetchone()
    copy_row = _copy_of(conn, fork_id, "EXT-FORK-COVERAGE")

    # ---- verbatim comparison, column by column, strictly ------------------
    mismatches = []
    for col in compare_set:
        src_val, dst_val = baseline_row[col], copy_row[col]
        if src_val is None and dst_val is None:
            # The false-green trap: NULL == NULL proves nothing about the
            # copy. The seed above is supposed to make this unreachable.
            mismatches.append(
                f"{col}: NULL on both sides — column not covered by the seed "
                "(false green); give it a non-trivial value in _seed_values"
            )
        elif src_val != dst_val:
            mismatches.append(
                f"{col}: baseline={src_val!r} but copy={dst_val!r} — the fork "
                "INSERT in _copy_nodes drops or mangles this column"
            )
    assert not mismatches, (
        "fork column completeness violated:\n  - " + "\n  - ".join(mismatches)
    )

    # ---- deliberate resets: proven, not assumed ---------------------------
    for col in _RESET_TO_NULL:
        assert baseline_row[col] is not None, (
            f"seed error: baseline {col} must be non-NULL so the reset is proven"
        )
        assert copy_row[col] is None, (
            f"{col} must be reset to NULL on the copy (see _copy_nodes "
            f"docstring), got {copy_row[col]!r}"
        )
    assert baseline_row["is_dirty"] is True, (
        "seed error: baseline is_dirty must be TRUE so the reset is proven"
    )
    assert copy_row["is_dirty"] is False, "copy must come out is_dirty=FALSE"
    assert copy_row["active"] is True, "copy must come out active=TRUE"
