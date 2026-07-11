"""
External-interface contracts (INT-1, ADR-037).

``contracts.py`` is the Python half of the feed-contract registry: strict
pydantic parsing of ``config/feed-contracts/*.yaml``, a versioned/idempotent
loader into the ``feed_contracts`` table (migration 073), and the
``get_active_contract`` reader. PR1 is registry-only — nothing here reads a
contract at ingest time yet (the daily-run runtime lands in PR2/PR3).
"""
from ootils_core.interfaces.contracts import (
    ContractError,
    FeedContract,
    FeedContractSpec,
    LoadOutcome,
    get_active_contract,
    load_contract_dir,
    parse_contract_file,
    upsert_contract,
)

__all__ = [
    "ContractError",
    "FeedContract",
    "FeedContractSpec",
    "LoadOutcome",
    "get_active_contract",
    "load_contract_dir",
    "parse_contract_file",
    "upsert_contract",
]
