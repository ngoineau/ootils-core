"""Transactional generators — on_hand, purchase_orders, work_orders, transfers.

Each generator writes node rows of the appropriate node_type to the `nodes`
table under the baseline scenario. Volumes track planning_params entries.
"""
