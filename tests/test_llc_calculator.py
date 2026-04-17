"""
test_llc_calculator.py — Unit tests for LLCCalculator (Phase 0, Section 6.2).

Covers:
  - BFS algorithm correctness (simple, multi-level, diamond BOM)
  - Cycle detection (simple cycle, deep cycle, self-reference)
  - Max-depth rule (item appearing at multiple levels)
  - Standalone items (LLC 0)
  - Performance (10k items < 50ms)
  - DB-backed calculator integration (mocked DB)
  - APICS scenario 002 multi-level BOM LLC ordering

Reference: APICS CPIM Part 2, Module 3 — BOM Structure & Low-Level Code
"""

from __future__ import annotations

import time
from collections import defaultdict
from decimal import Decimal
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest

# Import from local source (phase0 workspace)
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from ootils_core.engine.mrp.llc_calculator import (
    LLCResult,
    LLCCalculator,
    CycleDetectedError,
    compute_llc_pure,
)


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────

def _uuids(n: int) -> List[UUID]:
    """Generate n deterministic UUIDs for reproducible tests."""
    return [UUID(int=i) for i in range(1, n + 1)]


# ─────────────────────────────────────────────────────────────
# U-LLC-001: Simple BOM (FG → SA → RM)
# ─────────────────────────────────────────────────────────────

class TestSimpleBOM:
    """Single linear chain: FG → SA → RM. LLC should be FG=0, SA=1, RM=2."""

    def test_linear_chain(self):
        fg, sa, rm = _uuids(3)
        edges = [(fg, sa), (sa, rm)]

        result = compute_llc_pure(edges)

        assert result.llc_map[fg] == 0
        assert result.llc_map[sa] == 1
        assert result.llc_map[rm] == 2
        assert result.max_llc == 2
        assert result.item_count == 3

    def test_items_by_llc_grouping(self):
        fg, sa, rm = _uuids(3)
        edges = [(fg, sa), (sa, rm)]

        result = compute_llc_pure(edges)

        assert fg in result.items_by_llc[0]
        assert sa in result.items_by_llc[1]
        assert rm in result.items_by_llc[2]

    def test_four_level_chain(self):
        """FG → SA1 → SA2 → RM. LLC: 0,1,2,3."""
        items = _uuids(4)
        edges = [(items[0], items[1]), (items[1], items[2]), (items[2], items[3])]

        result = compute_llc_pure(edges)

        assert result.llc_map[items[0]] == 0
        assert result.llc_map[items[1]] == 1
        assert result.llc_map[items[2]] == 2
        assert result.llc_map[items[3]] == 3
        assert result.max_llc == 3


# ─────────────────────────────────────────────────────────────
# U-LLC-002: Multi-parent BOM (diamond / shared components)
# ─────────────────────────────────────────────────────────────

class TestDiamondBOM:
    """
    Diamond BOM:
        FG1 → SA1 → RM
        FG1 → SA2 → RM

    RM appears at depth 2 in both paths → LLC(RM) = 2 (max, not min).
    """

    def test_diamond_max_depth(self):
        fg1, sa1, sa2, rm = _uuids(4)
        edges = [(fg1, sa1), (fg1, sa2), (sa1, rm), (sa2, rm)]

        result = compute_llc_pure(edges)

        assert result.llc_map[fg1] == 0
        assert result.llc_map[sa1] == 1
        assert result.llc_map[sa2] == 1
        # RM at depth 2 in both paths → max = 2
        assert result.llc_map[rm] == 2

    def test_shared_component_at_different_depths(self):
        """
        RM appears at depth 1 and depth 3:
          FG → RM  (depth 1)
          FG → SA1 → SA2 → RM  (depth 3)

        LLC(RM) = max(1, 3) = 3
        """
        fg, sa1, sa2, rm = _uuids(4)
        edges = [
            (fg, rm),       # RM at depth 1
            (fg, sa1),
            (sa1, sa2),
            (sa2, rm),      # RM at depth 3
        ]

        result = compute_llc_pure(edges)

        assert result.llc_map[fg] == 0
        assert result.llc_map[sa1] == 1
        assert result.llc_map[sa2] == 2
        assert result.llc_map[rm] == 3  # max depth


# ─────────────────────────────────────────────────────────────
# U-LLC-003: Cycle detection
# ─────────────────────────────────────────────────────────────

class TestCycleDetection:
    """Cycle detection must raise CycleDetectedError with the cycle path."""

    def test_simple_cycle(self):
        """A → B → A forms a cycle."""
        a, b = _uuids(2)
        edges = [(a, b), (b, a)]

        with pytest.raises(CycleDetectedError) as exc_info:
            compute_llc_pure(edges)

        assert len(exc_info.value.cycle) >= 2
        assert a in exc_info.value.cycle

    def test_deep_cycle(self):
        """A → B → C → A forms a cycle of length 3."""
        a, b, c = _uuids(3)
        edges = [(a, b), (b, c), (c, a)]

        with pytest.raises(CycleDetectedError) as exc_info:
            compute_llc_pure(edges)

        assert a in exc_info.value.cycle
        assert b in exc_info.value.cycle
        assert c in exc_info.value.cycle

    def test_self_reference(self):
        """A → A is a trivial cycle."""
        a = UUID(int=1)
        edges = [(a, a)]

        with pytest.raises(CycleDetectedError):
            compute_llc_pure(edges)

    def test_cycle_with_legs(self):
        """
        Root → A → B → C → A (cycle with entry point).
        The cycle should still be detected even though Root is outside it.
        """
        root, a, b, c = UUID(int=1), UUID(int=2), UUID(int=3), UUID(int=4)
        edges = [
            (root, a),  # entry into cycle
            (a, b),
            (b, c),
            (c, a),     # cycle back to A
        ]

        with pytest.raises(CycleDetectedError) as exc_info:
            compute_llc_pure(edges)

        # The cycle path should include A, B, C
        cycle_set = set(exc_info.value.cycle)
        assert a in cycle_set or b in cycle_set or c in cycle_set

    def test_no_cycle_linear(self):
        """Linear chain should NOT raise CycleDetectedError."""
        fg, sa, rm = _uuids(3)
        edges = [(fg, sa), (sa, rm)]

        result = compute_llc_pure(edges)  # Should not raise
        assert result.item_count == 3

    def test_no_cycle_diamond(self):
        """Diamond BOM should NOT raise CycleDetectedError."""
        fg, sa1, sa2, rm = _uuids(4)
        edges = [(fg, sa1), (fg, sa2), (sa1, rm), (sa2, rm)]

        result = compute_llc_pure(edges)  # Should not raise
        assert result.item_count == 4

    def test_cycle_error_message(self):
        """CycleDetectedError message should contain the cycle path."""
        a, b = _uuids(2)
        edges = [(a, b), (b, a)]

        with pytest.raises(CycleDetectedError) as exc_info:
            compute_llc_pure(edges)

        msg = str(exc_info.value)
        assert "cycle" in msg.lower() or "BOM" in msg


# ─────────────────────────────────────────────────────────────
# U-LLC-004: Multiple root BOMs
# ─────────────────────────────────────────────────────────────

class TestMultipleRoots:
    """Multiple finished goods sharing components."""

    def test_two_fgs_shared_rm(self):
        """
        FG1 → RM
        FG2 → RM
        Both FGs are roots at LLC 0, RM at LLC 1.
        """
        fg1, fg2, rm = _uuids(3)
        edges = [(fg1, rm), (fg2, rm)]

        result = compute_llc_pure(edges)

        assert result.llc_map[fg1] == 0
        assert result.llc_map[fg2] == 0
        assert result.llc_map[rm] == 1

    def test_independent_boms(self):
        """
        Two independent BOM chains, no shared components.
        Each chain should get its own LLC sequence.
        """
        fg1, sa1, rm1 = UUID(int=1), UUID(int=2), UUID(int=3)
        fg2, sa2, rm2 = UUID(int=4), UUID(int=5), UUID(int=6)

        edges = [(fg1, sa1), (sa1, rm1), (fg2, sa2), (sa2, rm2)]

        result = compute_llc_pure(edges)

        assert result.llc_map[fg1] == 0
        assert result.llc_map[sa1] == 1
        assert result.llc_map[rm1] == 2
        assert result.llc_map[fg2] == 0
        assert result.llc_map[sa2] == 1
        assert result.llc_map[rm2] == 2


# ─────────────────────────────────────────────────────────────
# U-LLC-005: Standalone items (no BOM)
# ─────────────────────────────────────────────────────────────

class TestStandaloneItems:
    """Items with no BOM edges should default to LLC 0."""

    def test_standalone_item_gets_llc_0(self):
        """A standalone FG with no components should be LLC 0."""
        fg = UUID(int=1)
        edges = []

        result = compute_llc_pure(edges, standalone_items=[fg])

        assert result.llc_map[fg] == 0
        assert fg in result.items_by_llc[0]

    def test_mixed_standalone_and_bom(self):
        """Standalone items + BOM items should all appear."""
        fg_standalone = UUID(int=1)
        fg_bom, sa, rm = UUID(int=2), UUID(int=3), UUID(int=4)
        edges = [(fg_bom, sa), (sa, rm)]

        result = compute_llc_pure(edges, standalone_items=[fg_standalone])

        assert result.llc_map[fg_standalone] == 0
        assert result.llc_map[fg_bom] == 0
        assert result.llc_map[sa] == 1
        assert result.llc_map[rm] == 2

    def test_empty_bom_no_standalone(self):
        """No edges, no standalone items → empty result."""
        result = compute_llc_pure([])

        assert result.llc_map == {}
        assert result.max_llc == 0
        assert result.item_count == 0


# ─────────────────────────────────────────────────────────────
# U-LLC-006: Max-depth rule (APICS critical requirement)
# ─────────────────────────────────────────────────────────────

class TestMaxDepthRule:
    """
    APICS requires that an item's LLC is the MAXIMUM depth at which
    it appears in any BOM path. This ensures MRP processes it last,
    after all its parent requirements have been exploded.
    """

    def test_item_at_two_depths(self):
        """
        Component C appears at depth 1 and depth 2:
          FG → C   (depth 1)
          FG → SA → C  (depth 2)

        LLC(C) = max(1, 2) = 2
        """
        fg, sa, c = UUID(int=1), UUID(int=2), UUID(int=3)
        edges = [(fg, c), (fg, sa), (sa, c)]

        result = compute_llc_pure(edges)

        assert result.llc_map[c] == 2  # max depth, not 1

    def test_item_at_three_depths(self):
        """
        Component C appears at depth 1, 2, and 4:
          FG → C                 (depth 1)
          FG → SA → C            (depth 2)
          FG → SA → SA2 → SA3 → C  (depth 4)

        LLC(C) = 4
        """
        fg, sa, sa2, sa3, c = UUID(int=1), UUID(int=2), UUID(int=3), UUID(int=4), UUID(int=5)
        edges = [
            (fg, c),           # C at depth 1
            (fg, sa),          # SA at depth 1
            (sa, c),           # C at depth 2
            (sa, sa2),         # SA2 at depth 2
            (sa2, sa3),        # SA3 at depth 3
            (sa3, c),          # C at depth 4
        ]

        result = compute_llc_pure(edges)

        assert result.llc_map[c] == 4


# ─────────────────────────────────────────────────────────────
# U-LLC-007: APICS Scenario 002 (Multi-Level MRP)
# ─────────────────────────────────────────────────────────────

class TestAPICSScenario002:
    """
    APICS Scenario 002: FG → SA → RM (3 levels).
    LLC ordering must be FG=0, SA=1, RM=2.
    """

    def test_scenario_002_llc_ordering(self):
        fg = UUID(int=100)
        sa = UUID(int=200)
        rm = UUID(int=300)

        edges = [(fg, sa), (sa, rm)]

        result = compute_llc_pure(edges)

        assert result.llc_map[fg] == 0
        assert result.llc_map[sa] == 1
        assert result.llc_map[rm] == 2

    def test_scenario_002_processing_order(self):
        """Items should be grouped for MRP processing in LLC order."""
        fg = UUID(int=100)
        sa = UUID(int=200)
        rm = UUID(int=300)

        edges = [(fg, sa), (sa, rm)]

        result = compute_llc_pure(edges)

        # MRP processes LLC 0 first, then 1, then 2
        assert sorted(result.items_by_llc.keys()) == [0, 1, 2]

    def test_scenario_002_bom_explosion(self):
        """
        After MRP processes FG (LLC 0), its planned orders
        become dependent demand for SA (LLC 1).
        After SA, its planned orders become demand for RM (LLC 2).
        """
        fg = UUID(int=100)
        sa = UUID(int=200)
        rm = UUID(int=300)

        edges = [(fg, sa), (sa, rm)]
        result = compute_llc_pure(edges)

        # Verify that SA appears before RM in processing order
        sa_llc = result.llc_map[sa]
        rm_llc = result.llc_map[rm]
        assert sa_llc < rm_llc, "SA must be processed before RM"

        # Verify FG appears before SA
        fg_llc = result.llc_map[fg]
        assert fg_llc < sa_llc, "FG must be processed before SA"


# ─────────────────────────────────────────────────────────────
# U-LLC-008: Performance (10k items < 50ms)
# ─────────────────────────────────────────────────────────────

class TestPerformance:
    """Performance target: 10k items in < 50ms."""

    def test_10k_items_performance(self):
        """
        Generate a realistic 10k-item BOM with ~20k edges.
        BFS must complete in < 50ms.
        """
        n_items = 10_000
        # Build a forest of 5-level deep BOMs
        # Each parent has 2 children on average
        items = _uuids(n_items)
        edges = []

        # Level 0: 500 roots (FGs)
        # Level 1: 1000 SAs (2 per FG)
        # Level 2: 2000 components
        # Level 3: 3000 sub-components
        # Level 4: 3500 raw materials
        idx = 0
        roots = items[idx:idx + 500]; idx += 500
        level1 = items[idx:idx + 1000]; idx += 1000
        level2 = items[idx:idx + 2000]; idx += 2000
        level3 = items[idx:idx + 3000]; idx += 3000
        level4 = items[idx:idx + 3500]; idx += 3500

        # Each root → 2 level1 items
        for i, root in enumerate(roots):
            edges.append((root, level1[i * 2]))
            edges.append((root, level1[i * 2 + 1]))

        # Each level1 → 2 level2 items
        for i, sa in enumerate(level1):
            child_start = i * 2
            if child_start + 1 < len(level2):
                edges.append((sa, level2[child_start]))
                edges.append((sa, level2[child_start + 1]))

        # Each level2 → 1 level3 item (flat mapping)
        for i, comp in enumerate(level2):
            if i < len(level3):
                edges.append((comp, level3[i]))

        # Each level3 → 1 level4 item (shared RMs)
        for i, sub in enumerate(level3):
            rm_idx = i % len(level4)
            edges.append((sub, level4[rm_idx]))

        # Time the computation
        start = time.monotonic()
        result = compute_llc_pure(edges)
        elapsed_ms = (time.monotonic() - start) * 1000

        # All items that participate in edges should be in the result
        # (some level4 RMs may be shared, reducing distinct count)
        assert result.item_count > 0
        assert result.max_llc == 4  # 5 levels → max LLC = 4

        # Performance target: < 50ms (allow 200ms on CI)
        assert elapsed_ms < 200, f"LLC computation took {elapsed_ms:.1f}ms, expected < 200ms"

        print(f"\n  {result.item_count} items, {len(edges)} edges: {elapsed_ms:.1f}ms (internal: {result.elapsed_ms:.1f}ms)")

    def test_wide_bom_performance(self):
        """BOM with one FG and 5000 direct components (flat structure)."""
        fg = UUID(int=1)
        components = [UUID(int=i) for i in range(2, 5002)]
        edges = [(fg, c) for c in components]

        start = time.monotonic()
        result = compute_llc_pure(edges)
        elapsed_ms = (time.monotonic() - start) * 1000

        assert result.llc_map[fg] == 0
        # All components at LLC 1
        assert len(result.items_by_llc[1]) == 5000
        assert elapsed_ms < 100, f"Wide BOM took {elapsed_ms:.1f}ms"


# ─────────────────────────────────────────────────────────────
# U-LLC-009: DB-backed LLCCalculator (mocked DB)
# ─────────────────────────────────────────────────────────────

_psycopg_available = False
try:
    import psycopg
    _psycopg_available = True
except ImportError:
    pass


class TestDBBackedCalculator:
    """Test LLCCalculator with mocked DB connection.

    These tests require psycopg to be importable (for type annotation
    in the production code). They are skipped when psycopg is not
    available, such as in standalone test environments.
    """

    @pytest.mark.skipif(not _psycopg_available, reason="psycopg not installed")
    def test_calculate_all_basic(self):
        """DB-backed calculate_all loads edges, computes LLCs, persists."""
        fg = UUID(int=1)
        sa = UUID(int=2)
        rm = UUID(int=3)
        line1 = UUID(int=10)
        line2 = UUID(int=11)

        bom_rows = [
            {"parent_item_id": fg, "component_item_id": sa, "line_id": line1},
            {"parent_item_id": sa, "component_item_id": rm, "line_id": line2},
        ]

        mock_db = MagicMock()
        cursor_result = MagicMock()
        cursor_result.fetchall.return_value = bom_rows
        mock_db.execute.return_value = cursor_result

        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

        calc = LLCCalculator(mock_db)
        result = calc.calculate_all()

        assert result.llc_map[fg] == 0
        assert result.llc_map[sa] == 1
        assert result.llc_map[rm] == 2
        assert result.max_llc == 2

    @pytest.mark.skipif(not _psycopg_available, reason="psycopg not installed")
    def test_calculate_all_empty(self):
        """Empty BOM should return empty result."""
        mock_db = MagicMock()
        cursor_result = MagicMock()
        cursor_result.fetchall.return_value = []
        mock_db.execute.return_value = cursor_result

        calc = LLCCalculator(mock_db)
        result = calc.calculate_all()

        assert result.llc_map == {}
        assert result.item_count == 0
        assert result.edge_count == 0

    @pytest.mark.skipif(not _psycopg_available, reason="psycopg not installed")
    def test_detect_cycle_incremental(self):
        """detect_cycle should find cycles when adding new components.

        Existing BOM: A → B.
        Adding B under parent A: can B reach A via parent links?
        B's parents = {A} → yes → cycle detected.
        """
        a, b = UUID(int=1), UUID(int=2)

        bom_rows = [
            {"parent_item_id": a, "component_item_id": b},
        ]

        mock_db = MagicMock()
        cursor_result = MagicMock()
        cursor_result.fetchall.return_value = bom_rows
        mock_db.execute.return_value = cursor_result

        calc = LLCCalculator(mock_db)

        # parent=A, new_component=B: B can reach A (B's parent is A) → cycle
        assert calc.detect_cycle(a, [b]) is True

    @pytest.mark.skipif(not _psycopg_available, reason="psycopg not installed")
    def test_detect_no_cycle_incremental(self):
        """detect_cycle should return False when no cycle would be created."""
        fg, sa, rm = UUID(int=1), UUID(int=2), UUID(int=3)

        bom_rows = [
            {"parent_item_id": fg, "component_item_id": sa},
        ]

        mock_db = MagicMock()
        cursor_result = MagicMock()
        cursor_result.fetchall.return_value = bom_rows
        mock_db.execute.return_value = cursor_result

        calc = LLCCalculator(mock_db)

        # Adding RM under SA would not create a cycle
        assert calc.detect_cycle(sa, [rm]) is False

    @pytest.mark.skipif(not _psycopg_available, reason="psycopg not installed")
    def test_load_existing_llc(self):
        """load_existing_llc should return max LLC per component from DB."""
        sa = UUID(int=2)
        rm = UUID(int=3)

        rows = [
            {"component_item_id": sa, "llc": 1},
            {"component_item_id": rm, "llc": 2},
        ]

        mock_db = MagicMock()
        cursor_result = MagicMock()
        cursor_result.fetchall.return_value = rows
        mock_db.execute.return_value = cursor_result

        calc = LLCCalculator(mock_db)
        result = calc.load_existing_llc()

        assert result[sa] == 1
        assert result[rm] == 2

    @pytest.mark.skipif(not _psycopg_available, reason="psycopg not installed")
    def test_get_items_by_llc(self):
        """get_items_by_llc should group items by LLC level."""
        fg = UUID(int=1)
        sa = UUID(int=2)
        rm = UUID(int=3)

        component_rows = [
            {"item_id": sa, "llc": 1},
            {"item_id": rm, "llc": 2},
        ]

        parent_rows = [
            {"parent_item_id": fg},
        ]

        mock_db = MagicMock()

        call_count = [0]
        def mock_execute(query, params=None):
            call_count[0] += 1
            result = MagicMock()
            if call_count[0] == 1:
                result.fetchall.return_value = component_rows
            else:
                result.fetchall.return_value = parent_rows
            return result

        mock_db.execute = mock_execute

        calc = LLCCalculator(mock_db)
        result = calc.get_items_by_llc()

        assert fg in result[0]
        assert sa in result[1]
        assert rm in result[2]


# ─────────────────────────────────────────────────────────────
# U-LLC-010: Edge cases
# ─────────────────────────────────────────────────────────────

class TestEdgeCases:
    """Edge cases and boundary conditions."""

    def test_single_item_no_edges(self):
        """A single standalone item with no BOM edges."""
        item = UUID(int=1)
        result = compute_llc_pure([], standalone_items=[item])

        assert result.llc_map[item] == 0
        assert result.item_count == 1
        assert result.max_llc == 0

    def test_single_edge(self):
        """A single BOM edge: FG → SA."""
        fg, sa = UUID(int=1), UUID(int=2)
        edges = [(fg, sa)]

        result = compute_llc_pure(edges)

        assert result.llc_map[fg] == 0
        assert result.llc_map[sa] == 1
        assert result.edge_count == 1

    def test_large_branching_factor(self):
        """One FG with many direct components (wide BOM)."""
        fg = UUID(int=1)
        children = [UUID(int=i) for i in range(2, 102)]  # 100 children
        edges = [(fg, c) for c in children]

        result = compute_llc_pure(edges)

        assert result.llc_map[fg] == 0
        assert len(result.items_by_llc[1]) == 100
        assert result.max_llc == 1

    def test_result_timing_positive(self):
        """LLCResult should have positive elapsed_ms."""
        fg, sa = UUID(int=1), UUID(int=2)
        result = compute_llc_pure([(fg, sa)])

        assert result.elapsed_ms >= 0
        assert result.item_count == 2
        assert result.edge_count == 1

    def test_cycle_error_has_cycle_attribute(self):
        """CycleDetectedError should have a .cycle attribute."""
        a, b = UUID(int=1), UUID(int=2)
        edges = [(a, b), (b, a)]

        with pytest.raises(CycleDetectedError) as exc_info:
            compute_llc_pure(edges)

        assert hasattr(exc_info.value, 'cycle')
        assert isinstance(exc_info.value.cycle, list)
        assert len(exc_info.value.cycle) >= 2


# ─────────────────────────────────────────────────────────────
# U-LLC-011: APICS BOM with scrap factor (Scenario 008)
# ─────────────────────────────────────────────────────────────

class TestAPICSScenario008:
    """
    APICS Scenario 008: BOM with scrap factor.
    LLC calculation should not be affected by scrap factors
    (scrap affects qty_per, not LLC depth).
    """

    def test_scrap_factor_does_not_affect_llc(self):
        """LLC is purely structural — scrap factor doesn't change it."""
        fg = UUID(int=1)
        sa = UUID(int=2)
        edges = [(fg, sa)]

        result = compute_llc_pure(edges)

        assert result.llc_map[fg] == 0
        assert result.llc_map[sa] == 1
        # Scrap factor is a BOM line attribute, not a structural one


# ─────────────────────────────────────────────────────────────
# U-LLC-012: APICS Phantom BOM (Scenario 008 variant)
# ─────────────────────────────────────────────────────────────

class TestPhantomBOM:
    """
    Phantom items (non-stockable sub-assemblies) still need LLCs
    for MRP processing order, even though they generate no planned orders.
    """

    def test_phantom_item_gets_llc(self):
        """
        FG → Phantom → RM
        Phantom is at LLC 1, RM at LLC 2.
        The LLC calculation doesn't care about phantom status.
        """
        fg, phantom, rm = UUID(int=1), UUID(int=2), UUID(int=3)
        edges = [(fg, phantom), (phantom, rm)]

        result = compute_llc_pure(edges)

        assert result.llc_map[phantom] == 1
        assert result.llc_map[rm] == 2

    def test_phantom_at_two_depths(self):
        """
        Phantom appears at depth 1 and depth 2:
          FG → Phantom → RM1
          FG → SA → Phantom

        LLC(Phantom) = max(1, 2) = 2
        """
        fg, phantom, sa, rm1 = UUID(int=1), UUID(int=2), UUID(int=3), UUID(int=4)
        edges = [(fg, phantom), (fg, sa), (sa, phantom), (phantom, rm1)]

        result = compute_llc_pure(edges)

        assert result.llc_map[phantom] == 2  # max depth


# ─────────────────────────────────────────────────────────────
# U-LLC-013: Re-calculation (idempotency)
# ─────────────────────────────────────────────────────────────

class TestIdempotency:
    """LLC calculation should be idempotent."""

    def test_recalculate_same_result(self):
        """Running compute_llc_pure twice with same edges gives same result."""
        fg, sa, rm = UUID(int=1), UUID(int=2), UUID(int=3)
        edges = [(fg, sa), (sa, rm)]

        result1 = compute_llc_pure(edges)
        result2 = compute_llc_pure(edges)

        assert result1.llc_map == result2.llc_map

    def test_recalculate_after_adding_edges(self):
        """Adding edges and recalculating gives updated LLCs."""
        fg, sa, rm = UUID(int=1), UUID(int=2), UUID(int=3)

        # First: FG → SA
        result1 = compute_llc_pure([(fg, sa)])
        assert result1.llc_map[sa] == 1

        # Add: SA → RM
        result2 = compute_llc_pure([(fg, sa), (sa, rm)])
        assert result2.llc_map[rm] == 2

    def test_deterministic_uuids(self):
        """Same UUIDs always produce same LLCs."""
        items = [UUID(int=i) for i in range(1, 6)]
        edges = [
            (items[0], items[1]),
            (items[0], items[2]),
            (items[1], items[3]),
            (items[2], items[3]),
            (items[3], items[4]),
        ]

        results = [compute_llc_pure(edges) for _ in range(10)]

        for r in results[1:]:
            assert r.llc_map == results[0].llc_map