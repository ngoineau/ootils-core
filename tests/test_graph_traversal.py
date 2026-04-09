"""Unit tests for GraphTraversal — topological sort and dirty subgraph expansion."""
from __future__ import annotations

import graphlib
from datetime import date
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from ootils_core.engine.kernel.graph.traversal import GraphTraversal
from ootils_core.models import Edge, Node


def make_edge(from_id, to_id, scenario_id, edge_type="replenishes"):
    return Edge(
        edge_id=uuid4(),
        edge_type=edge_type,
        from_node_id=from_id,
        to_node_id=to_id,
        scenario_id=scenario_id,
    )


def make_node(node_id, node_type="ProjectedInventory", scenario_id=None, time_span_start=None):
    return Node(
        node_id=node_id,
        node_type=node_type,
        scenario_id=scenario_id or uuid4(),
        time_span_start=time_span_start,
    )


class TestTopologicalSort:
    def test_empty_set_returns_empty(self):
        store = MagicMock()
        store.get_edges_to.return_value = []
        traversal = GraphTraversal(store)
        result = traversal.topological_sort(set(), uuid4())
        assert result == []

    def test_single_node_no_edges(self):
        store = MagicMock()
        store.get_edges_to.return_value = []
        traversal = GraphTraversal(store)
        n = uuid4()
        scenario = uuid4()
        result = traversal.topological_sort({n}, scenario)
        assert result == [n]

    def test_linear_chain_ordered_correctly(self):
        """A → B → C should sort as [A, B, C]."""
        store = MagicMock()
        scenario = uuid4()
        a, b, c = uuid4(), uuid4(), uuid4()

        # get_edges_to(node, scenario) returns edges pointing TO that node
        def get_edges_to(node_id, sid):
            if node_id == b:
                return [make_edge(a, b, scenario)]
            if node_id == c:
                return [make_edge(b, c, scenario)]
            return []

        store.get_edges_to.side_effect = get_edges_to
        traversal = GraphTraversal(store)
        result = traversal.topological_sort({a, b, c}, scenario)
        assert result.index(a) < result.index(b)
        assert result.index(b) < result.index(c)

    def test_edges_outside_node_set_ignored(self):
        """Edges from nodes not in node_ids should be ignored."""
        store = MagicMock()
        scenario = uuid4()
        a, b, outside = uuid4(), uuid4(), uuid4()

        def get_edges_to(node_id, sid):
            if node_id == a:
                # outside → a: should be ignored (outside not in node_ids)
                return [make_edge(outside, a, scenario)]
            if node_id == b:
                return [make_edge(a, b, scenario)]
            return []

        store.get_edges_to.side_effect = get_edges_to
        traversal = GraphTraversal(store)
        result = traversal.topological_sort({a, b}, scenario)
        # a has no predecessors within {a, b}, so it comes first
        assert result.index(a) < result.index(b)
        assert outside not in result

    def test_cycle_raises(self):
        """A cycle between nodes should raise graphlib.CycleError."""
        store = MagicMock()
        scenario = uuid4()
        a, b = uuid4(), uuid4()

        def get_edges_to(node_id, sid):
            if node_id == b:
                return [make_edge(a, b, scenario)]
            if node_id == a:
                return [make_edge(b, a, scenario)]
            return []

        store.get_edges_to.side_effect = get_edges_to
        traversal = GraphTraversal(store)
        with pytest.raises(graphlib.CycleError):
            traversal.topological_sort({a, b}, scenario)

    def test_uses_per_node_query(self):
        """topological_sort should call get_edges_to once per node."""
        store = MagicMock()
        store.get_edges_to.return_value = []
        traversal = GraphTraversal(store)
        nodes = {uuid4(), uuid4(), uuid4()}
        traversal.topological_sort(nodes, uuid4())
        # Called once per node in node_ids
        assert store.get_edges_to.call_count == len(nodes)

    def test_parallel_nodes_both_present(self):
        """Two independent nodes should both appear in the result."""
        store = MagicMock()
        store.get_edges_to.return_value = []
        traversal = GraphTraversal(store)
        a, b = uuid4(), uuid4()
        result = traversal.topological_sort({a, b}, uuid4())
        assert a in result
        assert b in result
        assert len(result) == 2


class TestExpandDirtySubgraph:
    def test_single_node_no_edges(self):
        """A node with no outbound edges should be in the result."""
        scenario = uuid4()
        n = uuid4()
        node = make_node(n, node_type="OnHandSupply", scenario_id=scenario)
        store = MagicMock()
        store.get_node.return_value = node
        store.get_edges_from.return_value = []
        traversal = GraphTraversal(store)
        result = traversal.expand_dirty_subgraph(n, scenario, (date(2025, 1, 1), date(2025, 12, 31)))
        assert n in result

    def test_follows_outbound_edges(self):
        """Expansion should follow outbound edges to downstream nodes."""
        scenario = uuid4()
        a, b = uuid4(), uuid4()
        node_a = make_node(a, node_type="OnHandSupply", scenario_id=scenario)
        node_b = make_node(b, node_type="OnHandSupply", scenario_id=scenario)
        edge_ab = make_edge(a, b, scenario)
        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: node_a if nid == a else node_b
        store.get_edges_from.side_effect = lambda nid, sid: [edge_ab] if nid == a else []
        traversal = GraphTraversal(store)
        result = traversal.expand_dirty_subgraph(a, scenario, (date(2025, 1, 1), date(2025, 12, 31)))
        assert a in result and b in result

    def test_pi_node_outside_window_excluded(self):
        """PI nodes whose time_span_start falls outside the window are excluded."""
        scenario = uuid4()
        a, b = uuid4(), uuid4()
        node_a = make_node(a, node_type="OnHandSupply", scenario_id=scenario)
        # b is a PI node with time_span_start outside the window
        node_b = make_node(
            b,
            node_type="ProjectedInventory",
            scenario_id=scenario,
            time_span_start=date(2026, 6, 1),  # outside window [2025-01-01, 2025-12-31)
        )
        edge_ab = make_edge(a, b, scenario)
        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: node_a if nid == a else node_b
        store.get_edges_from.side_effect = lambda nid, sid: [edge_ab] if nid == a else []
        traversal = GraphTraversal(store)
        result = traversal.expand_dirty_subgraph(a, scenario, (date(2025, 1, 1), date(2025, 12, 31)))
        assert a in result
        assert b not in result

    def test_pi_node_inside_window_included(self):
        """PI nodes with time_span_start inside the window are included."""
        scenario = uuid4()
        a, b = uuid4(), uuid4()
        node_a = make_node(a, node_type="OnHandSupply", scenario_id=scenario)
        node_b = make_node(
            b,
            node_type="ProjectedInventory",
            scenario_id=scenario,
            time_span_start=date(2025, 6, 1),  # inside window
        )
        edge_ab = make_edge(a, b, scenario)
        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: node_a if nid == a else node_b
        store.get_edges_from.side_effect = lambda nid, sid: [edge_ab] if nid == a else []
        traversal = GraphTraversal(store)
        result = traversal.expand_dirty_subgraph(a, scenario, (date(2025, 1, 1), date(2025, 12, 31)))
        assert a in result
        assert b in result

    def test_visited_nodes_not_revisited(self):
        """Already-visited nodes should not be processed again (cycle guard)."""
        scenario = uuid4()
        a, b = uuid4(), uuid4()
        node_a = make_node(a, node_type="OnHandSupply", scenario_id=scenario)
        node_b = make_node(b, node_type="OnHandSupply", scenario_id=scenario)
        # Edge back from b to a — would cause infinite loop without visited guard
        edge_ab = make_edge(a, b, scenario)
        edge_ba = make_edge(b, a, scenario)
        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: node_a if nid == a else node_b
        store.get_edges_from.side_effect = lambda nid, sid: [edge_ab] if nid == a else [edge_ba]
        traversal = GraphTraversal(store)
        # Should terminate (not loop forever) and include both nodes
        result = traversal.expand_dirty_subgraph(a, scenario, (date(2025, 1, 1), date(2025, 12, 31)))
        assert a in result
        assert b in result
