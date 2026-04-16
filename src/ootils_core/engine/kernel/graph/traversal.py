"""
traversal.py — Topological sort and subgraph expansion over the planning graph.

Uses Python's graphlib.TopologicalSorter (stdlib, Python 3.9+).
Node IDs are used as tiebreakers for deterministic sort order.
"""
from __future__ import annotations

import collections
import graphlib
from datetime import date
from uuid import UUID

import psycopg

from ootils_core.models import EngineStartupError
from ootils_core.engine.kernel.graph.store import GraphStore


class GraphTraversal:
    """
    Topological sort and subgraph expansion over the planning graph.
    Uses Python graphlib.TopologicalSorter for cycle detection and ordering.

    All traversal is done at the application layer (not in SQL) to enable
    clean separation from the kernel's DB concerns.
    """

    def __init__(self, store: GraphStore) -> None:
        self._store = store

    def topological_sort(
        self,
        node_ids: set[UUID],
        scenario_id: UUID,
    ) -> list[UUID]:
        """
        Return a topological ordering of the given node_ids.

        Only edges between nodes within node_ids are considered
        (induced subgraph). Nodes with no relevant edges are included
        last (leaf ordering), sorted by node_id for determinism.

        Raises graphlib.CycleError if a cycle is detected in the subgraph.
        """
        # Build adjacency map for the induced subgraph via a single batch fetch
        # instead of N per-node queries (fixes N+1 performance issue).
        all_edges = self._store.get_all_edges(scenario_id)
        predecessors: dict[UUID, set[UUID]] = {n: set() for n in node_ids}
        for edge in all_edges:
            if edge.to_node_id in node_ids and edge.from_node_id in node_ids:
                predecessors[edge.to_node_id].add(edge.from_node_id)

        # Add implicit ordering: PI buckets in the same projection_series must be
        # processed in bucket_sequence order (feeds_forward edges may not exist).
        # We inject synthetic predecessor relationships between consecutive PI buckets.
        pi_nodes = {nid: self._store.get_node(nid, scenario_id) for nid in node_ids}
        from collections import defaultdict
        series_buckets: dict = defaultdict(list)
        for nid, nd in pi_nodes.items():
            if nd and nd.node_type == "ProjectedInventory" and nd.projection_series_id and nd.bucket_sequence is not None:
                series_buckets[nd.projection_series_id].append((nd.bucket_sequence, nid))
        for series_id, seq_list in series_buckets.items():
            seq_list.sort(key=lambda x: x[0])
            for i in range(1, len(seq_list)):
                prev_nid = seq_list[i - 1][1]
                curr_nid = seq_list[i][1]
                if curr_nid in predecessors:
                    predecessors[curr_nid].add(prev_nid)
                else:
                    predecessors[curr_nid] = {prev_nid}

        # Use graphlib.TopologicalSorter
        # TopologicalSorter takes {node: predecessors} mapping
        sorter = graphlib.TopologicalSorter(predecessors)

        # Collect ordered levels for deterministic tiebreaking
        # graphlib yields independent nodes in each "level" — we sort those by node_id
        result: list[UUID] = []
        sorter.prepare()
        while sorter.is_active():
            # Get all nodes that are ready (all predecessors done)
            ready = sorted(sorter.get_ready(), key=lambda n: str(n))
            for node_id in ready:
                result.append(node_id)
                sorter.done(node_id)

        return result

    def expand_dirty_subgraph(
        self,
        trigger_node_id: UUID,
        scenario_id: UUID,
        time_window: tuple[date, date],
    ) -> set[UUID]:
        """
        Starting from trigger_node_id, expand the set of nodes that must be
        recomputed due to a change.

        Expansion follows outbound edges (downstream cascade) within the
        given time_window [start, end). For PI nodes, only nodes whose
        time_span_start falls within the window are included.

        Returns the full set of affected node IDs (including trigger_node_id).
        """
        affected: set[UUID] = set()
        queue: collections.deque[UUID] = collections.deque([trigger_node_id])
        window_start, window_end = time_window

        while queue:
            current_id = queue.popleft()
            if current_id in affected:
                continue

            # Load the node to check its time_span
            node = self._store.get_node(current_id, scenario_id)
            if node is None:
                continue

            # Apply time_window filter for PI nodes
            if node.node_type == "ProjectedInventory" and node.time_span_start is not None:
                if node.time_span_start < window_start or node.time_span_start >= window_end:
                    continue

            affected.add(current_id)

            # Follow outbound edges downstream
            edges = self._store.get_edges_from(current_id, scenario_id)
            for edge in edges:
                if edge.to_node_id not in affected:
                    queue.append(edge.to_node_id)

        return affected

    def startup_cycle_check(self, scenario_id: UUID) -> None:
        """
        Assert the graph has no cycles by attempting a full topological sort.
        Called once during engine startup before any computation begins.

        Raises EngineStartupError if a cycle is detected.
        """
        # Load all active nodes for this scenario
        all_nodes = self._store.get_all_nodes(scenario_id)
        all_node_ids = {n.node_id for n in all_nodes}

        try:
            self.topological_sort(all_node_ids, scenario_id)
        except graphlib.CycleError as e:
            raise EngineStartupError(
                f"Cycle detected in planning graph for scenario {scenario_id}: {e}"
            ) from e
