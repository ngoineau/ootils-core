# tests/conftest.py
# Exclude legacy tests directory — these target the pre-graph-architecture API
# (InventoryState, SupplyChainDecisionEngine) removed in Sprint 1.
collect_ignore_glob = ["legacy/*.py"]
