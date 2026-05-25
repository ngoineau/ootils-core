"""
constants.py — single source of truth for cross-module constants.

# F-057 (audit closure)

Hardcoded constants like the baseline scenario UUID used to be
duplicated in 6+ places (`BASELINE = UUID(...)`, `_BASELINE_ID = ...`,
`BASELINE_SCENARIO_ID = ...`). A future migration to per-tenant
baselines would have required touching every site. New code should
import from here; existing call-sites can migrate opportunistically
when they're touched for unrelated reasons.

The corresponding Rust constant lives in
`rust/ootils_engine/src/loader.rs::BASELINE_SCENARIO_ID` (and
`rust/ootils_kernel/...` if the kernel ever needs it). Both must
stay in sync.
"""
from __future__ import annotations

from uuid import UUID

#: The baseline scenario UUID. Every node/edge in the seed data is
#: tagged with this scenario_id. Forks create new scenarios with
#: distinct UUIDs that overlay on top of baseline.
BASELINE_SCENARIO_ID: UUID = UUID("00000000-0000-0000-0000-000000000001")

# Backward-compat alias for code that imports `BASELINE` directly.
BASELINE = BASELINE_SCENARIO_ID
