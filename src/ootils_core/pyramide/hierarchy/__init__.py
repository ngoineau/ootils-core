"""
Hierarchy layer of Pyramide (axis A) — sparse summing-matrix (S)
construction over the generic hierarchy registry (migration 047:
hierarchy / hierarchy_node / item_hierarchy).

Blocks are cut at a configurable level of a configurable domain's
hierarchy (default: one block per root node of the domain's default
hierarchy), so reconciliation always works on small independent
matrices instead of one full-hierarchy S.
"""

from .summing import (
    AGGREGATE,
    LEAF,
    HierarchyNodeRow,
    SeriesRef,
    SummingBlock,
    build_summing_blocks,
    load_summing_blocks,
    resolve_default_hierarchy_id,
)

__all__ = [
    "AGGREGATE",
    "LEAF",
    "HierarchyNodeRow",
    "SeriesRef",
    "SummingBlock",
    "build_summing_blocks",
    "load_summing_blocks",
    "resolve_default_hierarchy_id",
]
