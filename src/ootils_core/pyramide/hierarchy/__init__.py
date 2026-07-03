"""
Hierarchy layer of Pyramide (axis A) — sparse summing-matrix (S)
construction over the generic hierarchy registry (migration 047:
hierarchy / hierarchy_node / item_hierarchy), reconciliation
(middle-out deterministic core, MinT-shrink optional edge) and the
block-level orchestration runner.

Blocks are cut at a configurable level of a configurable domain's
hierarchy (default: one block per root node of the domain's default
hierarchy), so reconciliation always works on small independent
matrices instead of one full-hierarchy S.
"""

from .reconcile import (
    MINT_MIN_INSAMPLE,
    RECON_MIDDLEOUT,
    RECON_MINT_SHRINK,
    SUPPORTED_RECON_METHODS,
    LeafShare,
    MintInputs,
    ReconciledBlock,
    ReconciliationError,
    ReconciliationUnavailable,
    middle_out,
    mint_shrink,
    reconcile,
)
from .runner import (
    HierarchicalPersistedSeries,
    HierarchicalRunConfig,
    HierarchicalRunner,
    HierarchicalRunResult,
)
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
    "MINT_MIN_INSAMPLE",
    "RECON_MIDDLEOUT",
    "RECON_MINT_SHRINK",
    "SUPPORTED_RECON_METHODS",
    "HierarchicalPersistedSeries",
    "HierarchicalRunConfig",
    "HierarchicalRunner",
    "HierarchicalRunResult",
    "HierarchyNodeRow",
    "LeafShare",
    "MintInputs",
    "ReconciledBlock",
    "ReconciliationError",
    "ReconciliationUnavailable",
    "SeriesRef",
    "SummingBlock",
    "build_summing_blocks",
    "load_summing_blocks",
    "middle_out",
    "mint_shrink",
    "reconcile",
    "resolve_default_hierarchy_id",
]
