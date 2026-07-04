"""
Pure unit tests (no DB) for the sparse summing-matrix builder
(src/ootils_core/pyramide/hierarchy/summing.py) — Pyramide axis A.

Golden fixture — hierarchy 'h-test', levels = (family, product),
2 families x 3 items each, drawn out:

    FAM-A (family)                      FAM-B (family)
    ├── PRD-A1 (product)                ├── PRD-B1 (product)
    │     ├── item-a1                   │     └── item-b1
    │     └── item-a2                   └── PRD-B2 (product)
    └── PRD-A2 (product)                      ├── item-b2
          └── item-a3                         └── item-b3

Expected S for block FAM-A (columns = [item-a1, item-a2, item-a3],
ordered by (leaf_code, item); rows = aggregates in (level, code) order,
then leaf identities), written by hand:

                 a1  a2  a3
    FAM-A      [  1   1   1 ]     -> row (0, 1, 2)
    PRD-A1     [  1   1   0 ]     -> row (0, 1)
    PRD-A2     [  0   0   1 ]     -> row (2,)
    item-a1    [  1   0   0 ]     -> row (0,)
    item-a2    [  0   1   0 ]     -> row (1,)
    item-a3    [  0   0   1 ]     -> row (2,)

Expected S for block FAM-B (columns = [item-b1, item-b2, item-b3]):

                 b1  b2  b3
    FAM-B      [  1   1   1 ]     -> row (0, 1, 2)
    PRD-B1     [  1   0   0 ]     -> row (0,)
    PRD-B2     [  0   1   1 ]     -> row (1, 2)
    item-b1    [  1   0   0 ]     -> row (0,)
    item-b2    [  0   1   0 ]     -> row (1,)
    item-b3    [  0   0   1 ]     -> row (2,)
"""
from __future__ import annotations

import random
from decimal import Decimal

import pytest

from ootils_core.pyramide.hierarchy import (
    AGGREGATE,
    LEAF,
    HierarchyNodeRow,
    build_summing_blocks,
)

H = "h-test"
LEVELS = ("family", "product")

NODES = [
    HierarchyNodeRow(code="FAM-A", level="family", parent_code=None),
    HierarchyNodeRow(code="PRD-A1", level="product", parent_code="FAM-A"),
    HierarchyNodeRow(code="PRD-A2", level="product", parent_code="FAM-A"),
    HierarchyNodeRow(code="FAM-B", level="family", parent_code=None),
    HierarchyNodeRow(code="PRD-B1", level="product", parent_code="FAM-B"),
    HierarchyNodeRow(code="PRD-B2", level="product", parent_code="FAM-B"),
]

MEMBERSHIPS = [
    ("item-a1", "PRD-A1"),
    ("item-a2", "PRD-A1"),
    ("item-a3", "PRD-A2"),
    ("item-b1", "PRD-B1"),
    ("item-b2", "PRD-B2"),
    ("item-b3", "PRD-B2"),
]


def _blocks(**kwargs):
    return build_summing_blocks(
        hierarchy_id=H, levels=LEVELS, nodes=NODES, memberships=MEMBERSHIPS,
        **kwargs,
    )


class TestGoldenS:
    def test_two_blocks_sorted_by_code(self):
        blocks = _blocks()
        assert [b.block_code for b in blocks] == ["FAM-A", "FAM-B"]
        assert all(b.hierarchy_id == H for b in blocks)
        assert all(b.block_level == "family" for b in blocks)

    def test_block_a_matches_hand_written_s(self):
        block_a = _blocks()[0]
        assert block_a.leaves == ("item-a1", "item-a2", "item-a3")
        assert [(s.kind, s.key) for s in block_a.series] == [
            (AGGREGATE, "FAM-A"),
            (AGGREGATE, "PRD-A1"),
            (AGGREGATE, "PRD-A2"),
            (LEAF, "item-a1"),
            (LEAF, "item-a2"),
            (LEAF, "item-a3"),
        ]
        assert block_a.rows == (
            (0, 1, 2),   # FAM-A
            (0, 1),      # PRD-A1
            (2,),        # PRD-A2
            (0,),        # item-a1
            (1,),        # item-a2
            (2,),        # item-a3
        )
        # aggregate rows carry their level; leaf rows their leaf_code
        assert block_a.series[0].level == "family"
        assert block_a.series[1].level == "product"
        assert block_a.series[3].level is None
        assert block_a.series[3].leaf_code == "PRD-A1"
        assert block_a.series[5].leaf_code == "PRD-A2"

    def test_block_b_matches_hand_written_s(self):
        block_b = _blocks()[1]
        assert block_b.leaves == ("item-b1", "item-b2", "item-b3")
        assert block_b.rows == (
            (0, 1, 2),   # FAM-B
            (0,),        # PRD-B1
            (1, 2),      # PRD-B2
            (0,),        # item-b1
            (1,),        # item-b2
            (2,),        # item-b3
        )

    def test_y_equals_s_times_b(self):
        """y = S . b, checked against hand-computed values."""
        block_a = _blocks()[0]
        b = [Decimal("3"), Decimal("5"), Decimal("7")]
        y = block_a.multiply(b)
        assert y == [
            Decimal("15"),  # FAM-A  = 3 + 5 + 7
            Decimal("8"),   # PRD-A1 = 3 + 5
            Decimal("7"),   # PRD-A2 = 7
            Decimal("3"),   # item-a1
            Decimal("5"),   # item-a2
            Decimal("7"),   # item-a3
        ]

    def test_multiply_rejects_wrong_base_length(self):
        block_a = _blocks()[0]
        with pytest.raises(ValueError, match="leaf columns"):
            block_a.multiply([Decimal("1"), Decimal("2")])


class TestDeterminism:
    def test_input_order_does_not_change_output(self):
        """Same rows, shuffled — byte-identical blocks (explicit sorts)."""
        reference = _blocks()
        rng = random.Random(42)
        for _ in range(5):
            nodes = list(NODES)
            memberships = list(MEMBERSHIPS)
            rng.shuffle(nodes)
            rng.shuffle(memberships)
            shuffled = build_summing_blocks(
                hierarchy_id=H, levels=LEVELS,
                nodes=nodes, memberships=memberships,
            )
            assert shuffled == reference


class TestBlockLevelParameter:
    def test_blocks_at_product_level(self):
        """Generic cut level: one block per product node."""
        blocks = _blocks(block_level="product")
        assert [b.block_code for b in blocks] == [
            "PRD-A1", "PRD-A2", "PRD-B1", "PRD-B2",
        ]
        prd_a1 = blocks[0]
        assert prd_a1.block_level == "product"
        assert prd_a1.leaves == ("item-a1", "item-a2")
        # aggregate row for the block root, then leaf identities
        assert prd_a1.rows == ((0, 1), (0,), (1,))

    def test_unknown_block_level_fails_loudly(self):
        with pytest.raises(ValueError, match="block_level"):
            _blocks(block_level="galaxy")


class TestFailLoudly:
    def test_unknown_node_level_raises(self):
        nodes = NODES + [
            HierarchyNodeRow(code="X-1", level="galaxy", parent_code=None)
        ]
        with pytest.raises(ValueError, match="galaxy"):
            build_summing_blocks(
                hierarchy_id=H, levels=LEVELS, nodes=nodes,
                memberships=MEMBERSHIPS,
            )

    def test_duplicate_node_code_raises(self):
        nodes = NODES + [
            HierarchyNodeRow(code="FAM-A", level="family", parent_code=None)
        ]
        with pytest.raises(ValueError, match="duplicate"):
            build_summing_blocks(
                hierarchy_id=H, levels=LEVELS, nodes=nodes,
                memberships=MEMBERSHIPS,
            )

    def test_unknown_parent_code_raises(self):
        """A node whose parent_code targets a nonexistent code must raise
        (it would otherwise silently drop out of every block)."""
        nodes = NODES + [
            HierarchyNodeRow(code="PRD-X1", level="product",
                             parent_code="FAM-GHOST")
        ]
        with pytest.raises(ValueError, match="PRD-X1.*FAM-GHOST"):
            build_summing_blocks(
                hierarchy_id=H, levels=LEVELS, nodes=nodes,
                memberships=MEMBERSHIPS,
            )

    def test_membership_on_unknown_node_raises(self):
        memberships = MEMBERSHIPS + [("item-x", "PRD-Z9")]
        with pytest.raises(ValueError, match="PRD-Z9"):
            build_summing_blocks(
                hierarchy_id=H, levels=LEVELS, nodes=NODES,
                memberships=memberships,
            )

    def test_parent_cycle_raises(self):
        nodes = [
            HierarchyNodeRow(code="FAM-C", level="family", parent_code=None),
            HierarchyNodeRow(code="P-1", level="product", parent_code="P-2"),
            HierarchyNodeRow(code="P-2", level="product", parent_code="P-1"),
        ]
        # The cycle is unreachable from FAM-C, so cut blocks at 'product'
        # to walk into it.
        with pytest.raises(ValueError, match="cycle"):
            build_summing_blocks(
                hierarchy_id=H, levels=LEVELS, nodes=nodes, memberships=[],
                block_level="product",
            )

    def test_empty_levels_raises(self):
        with pytest.raises(ValueError, match="levels"):
            build_summing_blocks(
                hierarchy_id=H, levels=[], nodes=NODES, memberships=MEMBERSHIPS,
            )


class TestEdgeShapes:
    def test_node_without_items_gets_empty_row(self):
        """An aggregate node with no attached items sums nothing (row = ())
        and multiply() yields 0 for it — never an error."""
        nodes = NODES + [
            HierarchyNodeRow(code="PRD-A3", level="product", parent_code="FAM-A")
        ]
        block_a = build_summing_blocks(
            hierarchy_id=H, levels=LEVELS, nodes=nodes, memberships=MEMBERSHIPS,
        )[0]
        idx = [s.key for s in block_a.series].index("PRD-A3")
        assert block_a.rows[idx] == ()
        y = block_a.multiply([Decimal("3"), Decimal("5"), Decimal("7")])
        assert y[idx] == Decimal("0")

    def test_items_on_intermediate_node_are_summed_too(self):
        """Membership may point at ANY node (generic hierarchies): items
        attached to an intermediate node count in that node and above."""
        memberships = MEMBERSHIPS + [("item-a0", "FAM-A")]
        block_a = build_summing_blocks(
            hierarchy_id=H, levels=LEVELS, nodes=NODES, memberships=memberships,
        )[0]
        # columns ordered by (leaf_code, item): FAM-A < PRD-A1 < PRD-A2
        assert block_a.leaves == ("item-a0", "item-a1", "item-a2", "item-a3")
        fam_row = block_a.rows[[s.key for s in block_a.series].index("FAM-A")]
        assert fam_row == (0, 1, 2, 3)
        prd_a1_row = block_a.rows[[s.key for s in block_a.series].index("PRD-A1")]
        assert prd_a1_row == (1, 2)
