"""
Sparse summing-matrix (S) construction for hierarchical forecast
reconciliation (Pyramide axis A).

For a hierarchy block, the summing matrix S encodes ``y = S @ b``: every
series (aggregate node at any level, plus the leaf series themselves) is
a sum of leaf series. Reconciliation methods (bottom-up, middle-out,
MinT, ...) all consume S; this module only *builds* it.

Blocks
------
S is never built for the whole hierarchy at once (scale wall — see
docs/DESIGN-pyramide-forecasting.md §3): the hierarchy is cut into
independent BLOCKS, one per node of a configurable ``block_level``
(default: the ROOT level, ``hierarchy.levels[0]``). Each block covers
the node's whole subtree and the items attached to it.

Sparse representation
---------------------
scipy is not a project dependency (pyproject: core deps are fastapi /
psycopg / pydantic; the optional ``forecast`` extra has pandas but no
scipy), so S uses a pure-Python sparse row encoding: every row is the
sorted tuple of leaf column indices it sums (all coefficients of a
summing matrix are 0/1). Per-block sizes make this comfortable.

Determinism
-----------
Every ordering is explicit (sorted node codes, sorted (leaf_code,
item_id) for leaves, hierarchy level order for aggregate rows), so the
same inputs always produce byte-identical blocks, whatever the input
row order. No randomness.

Genericity
----------
Nothing here is domain-specific: hierarchies come from the registry
tables of migration 047 (``hierarchy`` / ``hierarchy_node`` /
``item_hierarchy``), with the domain and block level as parameters.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Sequence

import psycopg

logger = logging.getLogger(__name__)

# SeriesRef.kind values
AGGREGATE = "aggregate"
LEAF = "leaf"


@dataclass(frozen=True)
class HierarchyNodeRow:
    """One row of ``hierarchy_node`` (pure-Python mirror)."""
    code: str
    level: str
    parent_code: str | None


@dataclass(frozen=True)
class SeriesRef:
    """
    One row of a block's summing matrix.

    kind='aggregate' → key is a hierarchy_node.code, level its level name.
    kind='leaf'      → key is the item identifier (stringified item_id),
                       level is None, leaf_code the node the item hangs from.
    """
    kind: str
    key: str
    level: str | None = None
    leaf_code: str | None = None


@dataclass(frozen=True)
class SummingBlock:
    """
    The sparse summing matrix S of one hierarchy block.

    ``series`` are the rows of S (aggregates first — hierarchy level
    order, then code order — then the leaf identity rows). ``leaves``
    are the columns (item identifiers, ordered by (leaf_code, item)).
    ``rows[i]`` is the sorted tuple of column indices summed by series
    ``series[i]`` — i.e. S[i, j] = 1 iff j in rows[i].
    """
    hierarchy_id: str
    block_code: str
    block_level: str
    series: tuple[SeriesRef, ...]
    leaves: tuple[str, ...]
    rows: tuple[tuple[int, ...], ...]

    def multiply(self, base: Sequence) -> list:
        """
        Compute ``y = S @ base``: one output value per series, from one
        input value per leaf column (any additive type: Decimal, int,
        float). Empty rows (aggregate node with no items) yield 0.
        """
        if len(base) != len(self.leaves):
            raise ValueError(
                f"base vector has {len(base)} entries, block "
                f"'{self.block_code}' has {len(self.leaves)} leaf columns"
            )
        return [sum((base[j] for j in row), Decimal(0) if _is_decimal(base) else 0)
                for row in self.rows]


def _is_decimal(base: Sequence) -> bool:
    return bool(base) and isinstance(base[0], Decimal)


# ---------------------------------------------------------------------------
# Pure construction (unit-testable without a database)
# ---------------------------------------------------------------------------


def build_summing_blocks(
    hierarchy_id: str,
    levels: Sequence[str],
    nodes: Iterable[HierarchyNodeRow],
    memberships: Iterable[tuple[str, str]],
    block_level: str | None = None,
) -> list[SummingBlock]:
    """
    Build one sparse S per node of ``block_level`` (default: the root
    level ``levels[0]``).

    Args:
        hierarchy_id: registry id (traceability only — no lookup here).
        levels:       ordered level names, root → leaf (hierarchy.levels).
        nodes:        hierarchy_node rows of this hierarchy.
        memberships:  (item_key, leaf_code) pairs — item_hierarchy rows
                      of this hierarchy; item_key is any stable string
                      identifier (stringified item_id UUID from the DB).
        block_level:  level name whose nodes each get their own block.

    Returns blocks sorted by block_code. Fails loudly on: unknown
    block_level, node level not in ``levels``, duplicate node code,
    a parent_code pointing at an unknown node, parent cycles, or a
    membership pointing at an unknown node.
    """
    levels = list(levels)
    if not levels:
        raise ValueError(f"hierarchy '{hierarchy_id}' declares no levels")
    if block_level is None:
        block_level = levels[0]
    if block_level not in levels:
        raise ValueError(
            f"block_level '{block_level}' is not a level of hierarchy "
            f"'{hierarchy_id}' (levels: {levels})"
        )
    level_rank = {name: i for i, name in enumerate(levels)}

    by_code: dict[str, HierarchyNodeRow] = {}
    for node in nodes:
        if node.code in by_code:
            raise ValueError(
                f"duplicate node code '{node.code}' in hierarchy '{hierarchy_id}'"
            )
        if node.level not in level_rank:
            raise ValueError(
                f"node '{node.code}' has level '{node.level}' which is not "
                f"declared by hierarchy '{hierarchy_id}' (levels: {levels})"
            )
        by_code[node.code] = node

    children: dict[str, list[str]] = {}
    for node in by_code.values():
        if node.parent_code is not None:
            if node.parent_code not in by_code:
                # An orphan node is unreachable from every block root and
                # would silently vanish from all blocks (its demand with
                # it) — same fail-loudly rule as orphan memberships.
                raise ValueError(
                    f"node '{node.code}' has parent_code "
                    f"'{node.parent_code}' which does not exist in "
                    f"hierarchy '{hierarchy_id}'"
                )
            children.setdefault(node.parent_code, []).append(node.code)
    for kids in children.values():
        kids.sort()

    # item columns attached directly to each node (via leaf membership)
    items_by_node: dict[str, list[str]] = {}
    seen_items: set[str] = set()
    for item_key, leaf_code in memberships:
        if leaf_code not in by_code:
            raise ValueError(
                f"item '{item_key}' is attached to unknown node "
                f"'{leaf_code}' in hierarchy '{hierarchy_id}'"
            )
        if item_key in seen_items:
            # item_hierarchy's PK (item_id, hierarchy_id) makes this
            # impossible from the DB path; guard the pure path anyway
            # (a duplicated column would silently double-count demand).
            raise ValueError(
                f"item '{item_key}' appears twice in hierarchy "
                f"'{hierarchy_id}' memberships"
            )
        seen_items.add(item_key)
        items_by_node.setdefault(leaf_code, []).append(item_key)
    for item_keys in items_by_node.values():
        item_keys.sort()

    block_roots = sorted(
        code for code, node in by_code.items() if node.level == block_level
    )

    blocks: list[SummingBlock] = []
    for root in block_roots:
        blocks.append(
            _build_block(
                hierarchy_id, root, block_level, by_code, children,
                items_by_node, level_rank,
            )
        )
    return blocks


def _subtree_codes(
    root: str,
    children: dict[str, list[str]],
    hierarchy_id: str,
) -> list[str]:
    """Deterministic DFS of the subtree; fails loudly on parent cycles."""
    seen: set[str] = set()
    order: list[str] = []
    stack = [root]
    while stack:
        code = stack.pop()
        if code in seen:
            raise ValueError(
                f"parent_code cycle detected at node '{code}' in "
                f"hierarchy '{hierarchy_id}'"
            )
        seen.add(code)
        order.append(code)
        # push reversed so children pop in sorted order
        stack.extend(reversed(children.get(code, [])))
    return order


def _build_block(
    hierarchy_id: str,
    root: str,
    block_level: str,
    by_code: dict[str, HierarchyNodeRow],
    children: dict[str, list[str]],
    items_by_node: dict[str, list[str]],
    level_rank: dict[str, int],
) -> SummingBlock:
    subtree = _subtree_codes(root, children, hierarchy_id)
    subtree_set = set(subtree)

    # Columns: leaf items of the subtree, ordered by (leaf_code, item).
    leaf_pairs = sorted(
        (leaf_code, item_key)
        for leaf_code in subtree_set & items_by_node.keys()
        for item_key in items_by_node[leaf_code]
    )
    leaves = tuple(item_key for _, item_key in leaf_pairs)
    col_of = {item_key: j for j, (_, item_key) in enumerate(leaf_pairs)}
    leaf_code_of = {item_key: leaf_code for leaf_code, item_key in leaf_pairs}

    # Columns summed by each node = its own items + its children's, bottom-up.
    cols_by_node: dict[str, frozenset[int]] = {}
    for code in reversed(subtree):  # DFS order reversed → children first
        cols: set[int] = {
            col_of[item_key] for item_key in items_by_node.get(code, [])
        }
        for child in children.get(code, []):
            cols |= cols_by_node[child]
        cols_by_node[code] = frozenset(cols)

    # Rows: aggregates in (level rank, code) order, then leaf identities.
    aggregate_codes = sorted(
        subtree, key=lambda c: (level_rank[by_code[c].level], c)
    )
    series: list[SeriesRef] = []
    rows: list[tuple[int, ...]] = []
    for code in aggregate_codes:
        series.append(
            SeriesRef(kind=AGGREGATE, key=code, level=by_code[code].level)
        )
        rows.append(tuple(sorted(cols_by_node[code])))
    for j, item_key in enumerate(leaves):
        series.append(
            SeriesRef(kind=LEAF, key=item_key, leaf_code=leaf_code_of[item_key])
        )
        rows.append((j,))

    return SummingBlock(
        hierarchy_id=hierarchy_id,
        block_code=root,
        block_level=block_level,
        series=tuple(series),
        leaves=leaves,
        rows=tuple(rows),
    )


# ---------------------------------------------------------------------------
# Database loaders (thin wrappers over the pure builder)
# ---------------------------------------------------------------------------


def resolve_default_hierarchy_id(db: psycopg.Connection, domain: str) -> str:
    """
    The is_default hierarchy of a domain (migration 047 registry).
    Fails loudly if the domain has zero or several defaults — the
    application layer owns is_default uniqueness, so ambiguity is a
    data-quality error, not something to resolve silently.
    """
    rows = db.execute(
        """
        SELECT hierarchy_id FROM hierarchy
        WHERE domain = %s AND is_default
        ORDER BY hierarchy_id ASC
        """,
        (domain,),
    ).fetchall()
    if not rows:
        raise ValueError(f"no default hierarchy registered for domain '{domain}'")
    if len(rows) > 1:
        ids = [r["hierarchy_id"] for r in rows]
        raise ValueError(
            f"domain '{domain}' has {len(rows)} default hierarchies "
            f"({ids}); expected exactly one"
        )
    return rows[0]["hierarchy_id"]


def load_summing_blocks(
    db: psycopg.Connection,
    hierarchy_id: str | None = None,
    domain: str | None = None,
    block_level: str | None = None,
) -> list[SummingBlock]:
    """
    Load hierarchy_node + item_hierarchy (migration 047) and build the
    per-block sparse summing matrices.

    Pass either ``hierarchy_id`` explicitly, or ``domain`` to use the
    domain's default hierarchy. ``block_level`` defaults to the root
    level of the hierarchy (one block per root node).
    """
    if hierarchy_id is None:
        if domain is None:
            raise ValueError("provide hierarchy_id or domain")
        hierarchy_id = resolve_default_hierarchy_id(db, domain)

    hierarchy_row = db.execute(
        "SELECT levels FROM hierarchy WHERE hierarchy_id = %s",
        (hierarchy_id,),
    ).fetchone()
    if hierarchy_row is None:
        raise ValueError(f"hierarchy '{hierarchy_id}' not found")

    node_rows = db.execute(
        """
        SELECT code, level, parent_code
        FROM hierarchy_node
        WHERE hierarchy_id = %s
        ORDER BY code ASC
        """,
        (hierarchy_id,),
    ).fetchall()
    membership_rows = db.execute(
        """
        SELECT item_id, leaf_code
        FROM item_hierarchy
        WHERE hierarchy_id = %s
        ORDER BY leaf_code ASC, item_id ASC
        """,
        (hierarchy_id,),
    ).fetchall()

    return build_summing_blocks(
        hierarchy_id=hierarchy_id,
        levels=hierarchy_row["levels"],
        nodes=[
            HierarchyNodeRow(
                code=r["code"], level=r["level"], parent_code=r["parent_code"]
            )
            for r in node_rows
        ],
        memberships=[(str(r["item_id"]), r["leaf_code"]) for r in membership_rows],
        block_level=block_level,
    )
