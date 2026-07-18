"""
policy.py — shortage-detection safety_scope policy (ADR-021 amendment,
DESC-1 PR-C).

Pilot arbitration (2026-07-18, ADR-043 §3 / DESC-1 §3): safety stock is
judged at the NATIONAL level, not per site — it is a risk-pooling cushion,
mutualised across every centre. Per-site `safety_stock_qty` values loaded
into `item_planning_params` are DISPATCH/EXECUTION artefacts (what a centre
is provisioned to carry day-to-day), not a shortage-detection threshold in
their own right: a local dip below a centre's safety stock is expected and
self-corrects via DRP transfer, not a signal to act on in isolation. The
national cushion already exists — `engine/mrp/loader.py` sums
`safety_stock_qty` per item across every location (Truth B) — this module
does not touch that axis at all.

`safety_scope()` is the SINGLE resolution point for this policy, consulted
by both detection engines:
  - SQL: `propagator_sql.SHORTAGES_SQL`'s `pi_with_ss` CTE (and, by
    reusing that same SQL string unchanged, the Rust in-process engine's
    two shortage-detection call sites in `propagator_rust.py`).
  - Python: `ShortageDetector.detect_with_params` (mirrors the SQL CTE),
    resolved once per calc_run by `PropagationEngine._propagate` and
    threaded down alongside the existing `is_stocking` gate.

'national' — per-site DETECTION only fires on a physical stockout
(`closing_stock < 0`); the per-site `below_safety_stock` branch never
fires, in either engine.
'per_site' — the pre-DESC-1 behaviour, byte-for-byte: `below_safety_stock`
still fires per (item, location) exactly as before this amendment.

An unset `OOTILS_SAFETY_SCOPE` resolves to the pilot's arbitrated default
('national') — a documented decision, not a silent fallback. An UNKNOWN
value is never coerced or ignored: it raises `ValueError` so a calc run
fails loudly, at the very start, rather than running under an ambiguous
policy.
"""
from __future__ import annotations

import os
from typing import Literal, get_args

SafetyScope = Literal["national", "per_site"]

VALID_SAFETY_SCOPES: tuple[SafetyScope, ...] = get_args(SafetyScope)

_ENV_VAR = "OOTILS_SAFETY_SCOPE"
_DEFAULT_SCOPE: SafetyScope = "national"


def safety_scope() -> SafetyScope:
    """
    Resolve the shortage-detection safety-stock scope from `OOTILS_SAFETY_SCOPE`.

    Defaults to 'national' (the pilot's 2026-07-18 arbitration) when unset.
    Raises `ValueError` on any other value — never a silent fallback.
    """
    raw = os.environ.get(_ENV_VAR, _DEFAULT_SCOPE)
    if raw not in VALID_SAFETY_SCOPES:
        raise ValueError(
            f"Invalid {_ENV_VAR}={raw!r}; expected one of {VALID_SAFETY_SCOPES}"
        )
    return raw


def is_national_scope() -> bool:
    """
    True iff `safety_scope()` resolves to 'national'.

    Convenience for call sites that only need a boolean — e.g. the SQL
    engine's `%(safety_scope_national)s` query parameter
    (`propagator_sql.shortage_params`). Fails loudly exactly like
    `safety_scope()` on a misconfigured environment.
    """
    return safety_scope() == "national"
