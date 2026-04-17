"""
Low-Level Code (LLC) Calculator for APICS-compliant MRP.

LLC determines processing order in multi-level BOM:
  - LLC 0 = finished goods (top level, processed first by MRP)
  - LLC N = deepest components (processed last)

Algorithm: BFS from roots (items that are parents but never children),
tracking max depth per item. An item appearing at multiple BOM depths
gets the *maximum* depth — this ensures its gross requirements include
all possible dependent-demand paths.

Cycle detection: any BOM cycle (A → B → … → A) is detected and
reported as a ``CycleDetectedError`` with the cycle path.

Performance target: 10 000 items in < 50 ms (pure Python).
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from uuid import UUID

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────────────────────

class CycleDetectedError(Exception):
    """Raised when a cycle is found in the BOM graph.

    Attributes:
        cycle: ordered list of item_ids forming the cycle.
    """

    def __init__(self, cycle: List[UUID]):
        self.cycle = cycle
        path = " → ".join(str(i) for i in cycle)
        super().__init__(f"BOM cycle detected: {path}")


# ─────────────────────────────────────────────────────────────
# Pure-Python core (no DB dependency)
# ─────────────────────────────────────────────────────────────

@dataclass
class LLCResult:
    """Result of an LLC calculation run."""
    llc_map: Dict[UUID, int]
    """item_id → llc for every item in the BOM graph."""

    max_llc: int
    """Highest LLC value in the result."""

    items_by_llc: Dict[int, List[UUID]]
    """llc_level → [item_ids] for MRP level-by-level processing."""

    elapsed_ms: float
    """Wall-clock time for the calculation."""

    item_count: int
    """Total number of distinct items processed."""

    edge_count: int
    """Total number of active BOM edges."""


def compute_llc_pure(
    edges: List[Tuple[UUID, UUID]],
    standalone_items: Optional[List[UUID]] = None,
) -> LLCResult:
    """
    Pure-Python LLC computation — no DB required.

    Args:
        edges: list of (parent_id, child_id) tuples representing
               active BOM relationships.
        standalone_items: items that exist but have no BOM edges
                          (e.g. finished goods with no components).
                          They default to LLC 0.

    Returns:
        LLCResult with the full mapping.

    Raises:
        CycleDetectedError: if a cycle is found in the BOM.

    Performance: O(V + E) BFS + O(V + E) cycle check.
    """
    start = time.monotonic()

    # ── Build adjacency list ────────────────────────────────
    children_map: Dict[UUID, List[UUID]] = defaultdict(list)
    all_parents: Set[UUID] = set()
    all_children: Set[UUID] = set()

    for parent_id, child_id in edges:
        children_map[parent_id].append(child_id)
        all_parents.add(parent_id)
        all_children.add(child_id)

    all_items = all_parents | all_children
    if standalone_items:
        all_items.update(standalone_items)

    edge_count = len(edges)

    # ── Cycle detection via DFS ─────────────────────────────
    # Detect cycles *before* BFS so we fail fast.
    # We use a colour map: WHITE=unvisited, GRAY=in-progress, BLACK=done.
    WHITE, GRAY, BLACK = 0, 1, 2
    colour: Dict[UUID, int] = {item: WHITE for item in all_items}
    parent_of: Dict[UUID, Optional[UUID]] = {}  # for cycle path reconstruction

    def _dfs(start_node: UUID) -> None:
        stack: List[Tuple[UUID, int]] = [(start_node, 0)]
        # (node, child_index) — iterative DFS with explicit backtracking
        while stack:
            node, ci = stack[-1]
            kids = children_map.get(node, [])

            if colour[node] == WHITE:
                colour[node] = GRAY

            # Advance through children
            while ci < len(kids):
                child = kids[ci]
                stack[-1] = (node, ci + 1)

                if colour.get(child, WHITE) == GRAY:
                    # ── Cycle found! Reconstruct path ─────────
                    cycle = [child]
                    cur = node
                    while cur != child:
                        cycle.append(cur)
                        cur = parent_of.get(cur)
                        if cur is None:
                            break
                    cycle.append(child)
                    cycle.reverse()
                    raise CycleDetectedError(cycle)

                if colour.get(child, WHITE) == WHITE:
                    parent_of[child] = node
                    stack.append((child, 0))
                    break  # descend
                ci = stack[-1][1]
            else:
                # All children processed
                colour[node] = BLACK
                stack.pop()

    for item in all_items:
        if colour[item] == WHITE:
            _dfs(item)

    # ── BFS from roots → max depth per item ─────────────────
    # Roots: items that are parents but never children (top-level FG).
    roots = all_parents - all_children

    # Standalone items (no edges at all) are also roots at LLC 0.
    if standalone_items:
        roots.update(set(standalone_items) - all_children)

    max_depth: Dict[UUID, int] = {}
    queue: deque[Tuple[UUID, int]] = deque()

    for root in roots:
        max_depth[root] = 0
        queue.append((root, 0))

    while queue:
        item_id, depth = queue.popleft()
        for child_id in children_map.get(item_id, []):
            child_depth = depth + 1
            if child_id not in max_depth or child_depth > max_depth[child_id]:
                max_depth[child_id] = child_depth
                queue.append((child_id, child_depth))

    # ── Items that are children-only (no root path) get depth from BFS ─
    # These should not exist after cycle check, but safety fallback:
    for item in all_children:
        if item not in max_depth:
            max_depth[item] = 0
            logger.warning("llc_calculator: item %s has no root path, defaulting to LLC 0", item)

    # ── Group by LLC ────────────────────────────────────────
    items_by_llc: Dict[int, List[UUID]] = defaultdict(list)
    for item_id, llc in max_depth.items():
        items_by_llc[llc].append(item_id)

    elapsed = (time.monotonic() - start) * 1000

    return LLCResult(
        llc_map=dict(max_depth),
        max_llc=max(max_depth.values()) if max_depth else 0,
        items_by_llc=dict(items_by_llc),
        elapsed_ms=elapsed,
        item_count=len(max_depth),
        edge_count=edge_count,
    )


# ─────────────────────────────────────────────────────────────
# DB-backed calculator
# ─────────────────────────────────────────────────────────────

class LLCCalculator:
    """Calculate and manage Low-Level Codes for BOM items.

    Delegates the core algorithm to ``compute_llc_pure`` and handles
    DB loading / persistence.

    Usage::

        calc = LLCCalculator(db_conn)
        result = calc.calculate_all()        # compute + persist
        llc_map = calc.load_existing_llc()   # read from DB
        by_level = calc.get_items_by_llc()    # grouped for MRP
    """

    def __init__(self, db):
        self.db = db
        # Note: psycopg is required for DB operations but imported
        # at module level in the production environment. For testing
        # with mocks, we don't validate the db type here.

    # ── Main entry: compute + persist ───────────────────────

    def calculate_all(self) -> LLCResult:
        """
        Recalculate LLCs for all items with BOM relationships.
        Uses BFS from roots (items that are parents but never children).

        Persists LLC values to ``bom_lines.llc`` column.

        Returns:
            LLCResult with full mapping and timing.

        Raises:
            CycleDetectedError: if a cycle exists in the BOM.
        """
        # 1. Load all active BOM edges
        rows = self.db.execute("""
            SELECT bh.parent_item_id, bl.component_item_id, bl.line_id
            FROM bom_headers bh
            JOIN bom_lines bl ON bl.bom_id = bh.bom_id
            WHERE bh.status = 'active' AND bl.active = true
        """).fetchall()

        if not rows:
            logger.info("llc_calculator: No active BOM edges found")
            return LLCResult(
                llc_map={}, max_llc=0, items_by_llc={},
                elapsed_ms=0, item_count=0, edge_count=0,
            )

        # 2. Convert to pure-Python edges (batch UUID conversion)
        edges: List[Tuple[UUID, UUID]] = []
        line_ids: List[Tuple[UUID, UUID]] = []  # (component_item_id, line_id)

        for row in rows:
            parent_id = UUID(str(row["parent_item_id"]))
            child_id = UUID(str(row["component_item_id"]))
            line_id = UUID(str(row["line_id"]))
            edges.append((parent_id, child_id))
            line_ids.append((child_id, line_id))

        # 3. Pure computation (cycle check + BFS)
        result = compute_llc_pure(edges)

        # 4. Persist LLC values back to bom_lines
        updates: List[Tuple[int, UUID]] = []
        for child_id, line_id in line_ids:
            llc = result.llc_map.get(child_id, 0)
            updates.append((llc, line_id))

        if updates:
            with self.db.cursor() as cur:
                cur.executemany(
                    "UPDATE bom_lines SET llc = %s WHERE line_id = %s",
                    updates,
                )
            logger.info(
                "llc_calculator: Updated %d BOM lines with LLC values in %.1f ms",
                len(updates), result.elapsed_ms,
            )

        # 5. Also update items table with max LLC for quick reference
        self._update_items_llc(result.llc_map)

        return result

    # ── Load existing LLCs from DB ──────────────────────────

    def load_existing_llc(self) -> Dict[UUID, int]:
        """
        Load existing LLC values from bom_lines.
        Returns the max(llc) per component_item_id.
        """
        rows = self.db.execute("""
            SELECT component_item_id, MAX(llc) AS llc
            FROM bom_lines
            WHERE active = true
            GROUP BY component_item_id
        """).fetchall()

        result = {}
        for row in rows:
            item_id = UUID(str(row["component_item_id"]))
            llc = int(row["llc"]) if row["llc"] is not None else 0
            result[item_id] = llc
        return result

    # ── Group items by LLC level for MRP processing ─────────

    def get_items_by_llc(
        self, location_id: Optional[UUID] = None
    ) -> Dict[int, List[UUID]]:
        """
        Group items by their LLC level.

        Items with no BOM (pure finished goods) are at LLC 0.
        Items that are components get their LLC from bom_lines.

        Args:
            location_id: optional filter on item_planning_params.location_id

        Returns:
            Dict mapping llc_level → [item_ids]
        """
        location_filter = ""
        location_join = ""
        params: list = []

        if location_id:
            location_join = """
                JOIN item_planning_params ipp
                    ON ipp.item_id = bl.component_item_id
                    AND ipp.location_id = %s
                    AND (ipp.effective_to IS NULL OR ipp.effective_to = '9999-12-31'::DATE)
            """
            params.append(location_id)

        # Items with LLC from BOM
        query = f"""
            SELECT DISTINCT bl.component_item_id AS item_id, MAX(bl.llc) AS llc
            FROM bom_lines bl
            JOIN bom_headers bh ON bl.bom_id = bh.bom_id
            {location_join}
            WHERE bh.status = 'active' AND bl.active = true
            GROUP BY bl.component_item_id
        """
        rows = self.db.execute(query, params).fetchall()

        # Items that are parents but not children → LLC 0
        parent_query = """
            SELECT DISTINCT bh.parent_item_id
            FROM bom_headers bh
            WHERE bh.status = 'active'
              AND bh.parent_item_id NOT IN (
                  SELECT DISTINCT bl.component_item_id
                  FROM bom_lines bl
                  JOIN bom_headers bh2 ON bl.bom_id = bh2.bom_id
                  WHERE bh2.status = 'active' AND bl.active = true
              )
        """
        parent_rows = self.db.execute(parent_query).fetchall()

        result: Dict[int, List[UUID]] = defaultdict(list)

        for row in rows:
            item_id = UUID(str(row["item_id"]))
            llc = int(row["llc"]) if row["llc"] is not None else 0
            result[llc].append(item_id)

        for row in parent_rows:
            item_id = UUID(str(row["parent_item_id"]))
            result[0].append(item_id)

        return dict(result)

    # ── Incremental cycle detection ─────────────────────────

    def detect_cycle(
        self, parent_item_id: UUID, new_component_ids: List[UUID]
    ) -> bool:
        """
        Detect if adding new_component_ids under parent_item_id would create a cycle.
        DFS from new components checking if we can reach parent_item_id.

        Returns:
            True if a cycle would be created, False otherwise.
        """
        # Load existing BOM relationships
        rows = self.db.execute("""
            SELECT parent_item_id, component_item_id
            FROM bom_headers bh
            JOIN bom_lines bl ON bl.bom_id = bh.bom_id
            WHERE bh.status = 'active' AND bl.active = true
        """).fetchall()

        # Build child → parents map (reverse direction for upward traversal)
        child_to_parents: Dict[UUID, Set[UUID]] = defaultdict(set)
        for row in rows:
            parent = UUID(str(row["parent_item_id"]))
            child = UUID(str(row["component_item_id"]))
            child_to_parents[child].add(parent)

        # Check: can we reach parent_item_id from any of the new components?
        visited: Set[UUID] = set()
        stack = list(new_component_ids)

        while stack:
            node = stack.pop()
            if node == parent_item_id:
                return True  # Cycle detected
            if node in visited:
                continue
            visited.add(node)
            for parent in child_to_parents.get(node, set()):
                if parent not in visited:
                    stack.append(parent)

        return False

    # ── Private helpers ─────────────────────────────────────

    def _update_items_llc(self, llc_map: Dict[UUID, int]) -> None:
        """Update items table with max LLC for each item."""
        if not llc_map:
            return

        # Build (llc, item_id) tuples for batch update
        updates = [(llc, item_id) for item_id, llc in llc_map.items()]

        with self.db.cursor() as cur:
            cur.executemany(
                "UPDATE items SET llc = %s WHERE item_id = %s",
                updates,
            )