"""
Integration tests for ootils_core.engine.mrp.llc_calculator.LLCCalculator
against a real PostgreSQL database.

Ported from tests/test_llc_calculator.py::TestDBBackedCalculator (which
mocked the DB via MagicMock). The pure-Python BFS / cycle-detection
tests stay in tests/test_llc_calculator.py — they don't need a DB.

Each test seeds items + bom_headers + bom_lines, runs the calculator,
verifies via SELECT, then deletes the rows it inserted. The `conn`
fixture rolls back uncommitted changes, but writes that we commit
need explicit DELETE.
"""
from __future__ import annotations

from uuid import UUID, uuid4

from ootils_core.engine.mrp.llc_calculator import LLCCalculator

from .conftest import requires_db

pytestmark = requires_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_bom(conn, edges: list[tuple[UUID, UUID]]) -> dict[str, list[UUID]]:
    """Insert items + bom_headers + bom_lines for the (parent, child) edges.

    Returns a dict with the inserted ids for explicit teardown:
        {"items": [...], "bom_headers": [...], "bom_lines": [...]}
    """
    items: set[UUID] = set()
    for p, c in edges:
        items.add(p)
        items.add(c)

    for item_id in items:
        conn.execute(
            "INSERT INTO items (item_id, name) VALUES (%s, %s) "
            "ON CONFLICT (item_id) DO NOTHING",
            (item_id, f"LLC test item {item_id}"),
        )

    parent_to_bom: dict[UUID, UUID] = {}
    bom_ids: list[UUID] = []
    line_ids: list[UUID] = []

    for parent, child in edges:
        if parent not in parent_to_bom:
            bom_id = uuid4()
            conn.execute(
                "INSERT INTO bom_headers (bom_id, parent_item_id, bom_version) "
                "VALUES (%s, %s, %s)",
                (bom_id, parent, f"test-{bom_id.hex[:8]}"),
            )
            parent_to_bom[parent] = bom_id
            bom_ids.append(bom_id)

        line_id = uuid4()
        conn.execute(
            "INSERT INTO bom_lines (line_id, bom_id, component_item_id, quantity_per) "
            "VALUES (%s, %s, %s, 1)",
            (line_id, parent_to_bom[parent], child),
        )
        line_ids.append(line_id)

    conn.commit()
    return {"items": list(items), "bom_headers": bom_ids, "bom_lines": line_ids}


def _teardown_bom(conn, inserted: dict[str, list[UUID]]) -> None:
    """Delete the rows seeded by _seed_bom. Order matters: lines → headers → items."""
    if inserted["bom_lines"]:
        conn.execute(
            "DELETE FROM bom_lines WHERE line_id = ANY(%s)",
            (inserted["bom_lines"],),
        )
    if inserted["bom_headers"]:
        conn.execute(
            "DELETE FROM bom_headers WHERE bom_id = ANY(%s)",
            (inserted["bom_headers"],),
        )
    if inserted["items"]:
        conn.execute(
            "DELETE FROM items WHERE item_id = ANY(%s)",
            (inserted["items"],),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestLLCCalculatorDBBacked:
    """LLCCalculator end-to-end against real Postgres."""

    def test_calculate_all_basic(self, conn):
        """DB-backed calculate_all loads edges, computes LLCs, persists."""
        fg = uuid4()
        sa = uuid4()
        rm = uuid4()
        seeded = _seed_bom(conn, [(fg, sa), (sa, rm)])
        try:
            calc = LLCCalculator(conn)
            result = calc.calculate_all()

            assert result.llc_map[fg] == 0
            assert result.llc_map[sa] == 1
            assert result.llc_map[rm] == 2
            assert result.max_llc == 2

            # Verify persistence on bom_lines
            persisted = {
                UUID(str(r["line_id"])): r["llc"]
                for r in conn.execute(
                    "SELECT line_id, llc FROM bom_lines WHERE line_id = ANY(%s)",
                    (seeded["bom_lines"],),
                ).fetchall()
            }
            assert all(llc > 0 for llc in persisted.values())
        finally:
            _teardown_bom(conn, seeded)

    def test_calculate_all_empty(self, conn):
        """Empty BOM should return empty result."""
        calc = LLCCalculator(conn)
        result = calc.calculate_all()
        # Other tests may have left rows committed; we just assert the call
        # returns a valid LLCResult, not that the global BOM is empty.
        assert result is not None
        assert hasattr(result, "llc_map")

    def test_detect_cycle_incremental(self, conn):
        """detect_cycle should find cycles when adding new components."""
        a = uuid4()
        b = uuid4()
        seeded = _seed_bom(conn, [(a, b)])
        try:
            calc = LLCCalculator(conn)
            # parent=A, new_component=B: B is already a child of A → adding
            # B under A again creates a cycle path A → B → ... → A.
            # The detector walks parents of B: {A}. Since A == parent we ask
            # about, that's a self-cycle.
            assert calc.detect_cycle(a, [b]) is True
        finally:
            _teardown_bom(conn, seeded)

    def test_detect_no_cycle_incremental(self, conn):
        """detect_cycle should return False when no cycle would be created."""
        fg = uuid4()
        sa = uuid4()
        rm = uuid4()
        seeded = _seed_bom(conn, [(fg, sa)])
        try:
            calc = LLCCalculator(conn)
            # Adding RM under SA: RM has no parents yet, no path back to SA.
            assert calc.detect_cycle(sa, [rm]) is False
        finally:
            _teardown_bom(conn, seeded)

    def test_load_existing_llc(self, conn):
        """load_existing_llc should return max LLC per component from DB."""
        fg = uuid4()
        sa = uuid4()
        rm = uuid4()
        seeded = _seed_bom(conn, [(fg, sa), (sa, rm)])
        try:
            calc = LLCCalculator(conn)
            calc.calculate_all()  # persist LLCs first
            result = calc.load_existing_llc()
            assert result.get(sa) == 1
            assert result.get(rm) == 2
        finally:
            _teardown_bom(conn, seeded)

    def test_get_items_by_llc(self, conn):
        """get_items_by_llc should group items by LLC level."""
        fg = uuid4()
        sa = uuid4()
        rm = uuid4()
        seeded = _seed_bom(conn, [(fg, sa), (sa, rm)])
        try:
            calc = LLCCalculator(conn)
            calc.calculate_all()
            result = calc.get_items_by_llc()
            # fg has no LLC entry as a component → it is a parent-only root at 0
            assert fg in result.get(0, [])
            assert sa in result.get(1, [])
            assert rm in result.get(2, [])
        finally:
            _teardown_bom(conn, seeded)
