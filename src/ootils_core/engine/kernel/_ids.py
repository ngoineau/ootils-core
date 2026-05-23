"""
Deterministic UUID generation for the kernel (ADR-003).

ADR-003 makes determinism a hard constraint: "no randomness in the core
engine". The kernel must produce the same UUIDs given the same input
state, so re-running the same calc_run over the same scenario yields
identical shortage_ids, edge_ids, explanation_ids etc. — making diff /
replay / audit comparisons possible.

UUIDs that depend on a per-execution identity (calc_run_id, idempotency_key)
remain stable across re-executions of *that same* run; the run identity
itself is supplied by the orchestration layer outside the kernel.

Usage:
    from ootils_core.engine.kernel._ids import deterministic_uuid

    shortage_id = deterministic_uuid(
        "shortage", scenario_id, calc_run_id, pi_node.node_id,
    )
"""
from __future__ import annotations

import uuid

# Fixed kernel namespace. Derived from uuid.uuid5(NAMESPACE_DNS, "kernel.ootils-core")
# and hard-coded so the value is stable across Python releases.
KERNEL_NAMESPACE = uuid.UUID("89e1e24e-42d7-5c31-87c7-c64e50e24131")


def deterministic_uuid(kind: str, *parts: object) -> uuid.UUID:
    """Stable uuid5 derived from a kind tag + identifying parts.

    Same inputs → same UUID. The `kind` tag prefixes the hash so different
    entity types using the same identifying parts (e.g. (scenario, node))
    do not collide.

    Parts are stringified via str(); UUIDs render as their canonical
    hyphenated form, dates use ISO 8601, ints render in base 10.
    """
    name = kind + "|" + "|".join(str(p) for p in parts)
    return uuid.uuid5(KERNEL_NAMESPACE, name)
