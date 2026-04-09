"""
ootils-core: AI-native supply chain planning engine.

This package provides a graph-based kernel for supply chain planning,
designed to be driven by AI agents and human planners alike.

Quick start (agent tools)::

    from ootils_core.tools import get_active_issues, simulate_override, trigger_recalculation

    # Get active shortages for the baseline scenario
    issues = get_active_issues(db)

    # Simulate an override
    result = simulate_override(db, node_id="...", field="qty", value=100)
"""

__version__ = "0.1.0"
__all__: list = []

