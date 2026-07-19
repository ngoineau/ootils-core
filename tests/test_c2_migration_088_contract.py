"""
tests/test_c2_migration_088_contract.py — C2 (moteur d'exception, chantier 2)
PURE contract guard for migration 088 (value-ledger event types + decision-basis
stamps). No DB, no package import — parses the migration .sql files on disk, the
same discipline as tests/test_purge_whitelist_guard.py so it runs in EVERY CI
job, not only the ones with a Postgres.

This file is the VOLET 3 anchor for doctrine C2 §4 — the migration trap:

    "Si C2 touche le CHECK events.event_type (088), il DOIT repartir de la liste
     complète de 086 (24 types, incluant reconciliation_completed) + les
     nouveaux types C2 — jamais de la liste de main (085) sous peine de faire
     sauter reconciliation_completed du CHECK à l'application séquentielle
     086->088."

Two families of tests live here, on purpose:

  * GREEN NOW — validate the parser and the architect's reuse/new classification
    against the schema that already exists on this branch (085). These prove the
    test infrastructure itself is correct, independent of whether volets 1-2 have
    landed migration 088 yet.

  * RED UNTIL VOLET 1/2 — the actual 088 proofs (file exists, CHECK widened FROM
    086 not 085, decision-basis columns added, defensively idempotent). They fail
    with a clear message until the migration is written by the parallel volets,
    then turn green — the doctrine's "les preuves qui comptent".
"""
from __future__ import annotations

import re
from pathlib import Path

# Derived from __file__ (not an ``ootils_core`` import) so this guard has ZERO
# runtime dependency — it parses text files and needs neither a DB nor an
# importable package, exactly like the migration-080 backfill assertions in
# tests/integration/test_ingest_retraction_integration.py.
_MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[1]
    / "src" / "ootils_core" / "db" / "migrations"
)

# ---------------------------------------------------------------------------
# The authoritative event-type sets (doctrine C2 §2/§4 + architect plan §A/§B).
# ---------------------------------------------------------------------------

# The COMPLETE 086 list (24 types) — reconstructed from the architect's
# reference copy of migration 086 (PR-5b, not yet merged on this branch). This
# is the list 088 MUST restart from; ``reconciliation_completed`` is the 24th
# type and the one that silently disappears if 088 restarts from 085 instead.
EXPECTED_086_TYPES: frozenset[str] = frozenset({
    # migrations 002 + 006 + 051 + 062 + 071 + 076 + 079 + 084 + 085
    "supply_date_changed", "supply_qty_changed",
    "demand_qty_changed", "onhand_updated",
    "policy_changed", "structure_changed",
    "scenario_created", "calc_triggered",
    "ingestion_complete", "po_date_changed",
    "test_event", "scenario_merge",
    "recommendation_transition",
    "node_firm_changed",
    "recommendation_created", "shortage_detected",
    "calc_run_finished", "snapshot_captured",
    "outcome_evaluated",
    "purge_executed",
    "daily_run_completed",
    "demand_descended",
    "export_executed",
    # migration 086 (ADR-042 decision 4, PR-5b) — the type the §4 trap drops
    "reconciliation_completed",
})

# The 4 NET-NEW value-ledger event types introduced by C2 (architect plan §A).
C2_NEW_TYPES: frozenset[str] = frozenset({
    "supply_status_changed",
    "supply_uom_changed",
    "demand_date_changed",
    "demand_status_changed",
})

# The 4 types C2 REUSES (already at the CHECK since migration 002) — the plan's
# "réutilisé" column. Filling their old/new ledger columns at ingest does not
# widen the CHECK.
C2_REUSED_TYPES: frozenset[str] = frozenset({
    "onhand_updated",
    "supply_qty_changed",
    "supply_date_changed",
    "demand_qty_changed",
})

# The complete post-088 CHECK set the doctrine mandates (086 ∪ new C2).
EXPECTED_088_TYPES: frozenset[str] = EXPECTED_086_TYPES | C2_NEW_TYPES

# Decision-basis columns migration 088 must add (doctrine C2 §3).
CALC_RUN_BASIS_COLUMNS = ("anchor_date", "engine_flavor", "code_version")
RECO_BASIS_COLUMNS = ("anchor_date", "stream_seq_hwm")


# ---------------------------------------------------------------------------
# Parsing helpers (no DB) — mirror test_purge_whitelist_guard.py's approach.
# ---------------------------------------------------------------------------

def _strip_line_comments(text: str) -> str:
    """Drop everything from '--' to end-of-line, per line — so a commented-out
    type name or a `-- migration 085 ...` header inside a CHECK body never
    leaks into the extracted set."""
    out = []
    for line in text.split("\n"):
        idx = line.find("--")
        out.append(line if idx == -1 else line[:idx])
    return "\n".join(out)


def _event_type_check_types(sql_text: str) -> frozenset[str]:
    """Return the set of single-quoted event types inside the sole
    ``event_type IN ( ... )`` list of a migration (the events CHECK). Works for
    both the CREATE-TABLE-inline form (002) and the ALTER-ADD-CONSTRAINT form
    (085/086/088). Returns empty if the migration has no such list."""
    stripped = _strip_line_comments(sql_text)
    m = re.search(r"event_type\s+IN\s*\(", stripped, re.IGNORECASE)
    if not m:
        return frozenset()
    # Capture up to the paren matching the '(' that opened the IN list.
    depth = 0
    body_start = m.end()          # first char after '('
    i = m.end() - 1               # position of the '('
    while i < len(stripped):
        c = stripped[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                body = stripped[body_start:i]
                return frozenset(re.findall(r"'([^']+)'", body))
        i += 1
    return frozenset()


def _migration(glob_pat: str) -> Path | None:
    hits = sorted(_MIGRATIONS_DIR.glob(glob_pat))
    return hits[0] if hits else None


def _migration_text(glob_pat: str) -> str | None:
    path = _migration(glob_pat)
    return path.read_text(encoding="utf-8") if path is not None else None


# ===========================================================================
# GREEN NOW — parser self-validation + reuse/new classification against 085.
# These do NOT depend on migration 088 existing; they lock the test infra.
# ===========================================================================

def test_parser_extracts_the_085_event_check():
    """Self-validation: the extractor pulls the real 23-type CHECK out of the
    migration that currently tops the tree (085). If this breaks, every 088
    assertion below is untrustworthy."""
    text = _migration_text("085_*.sql")
    assert text is not None, "migration 085 must exist on this branch"
    types = _event_type_check_types(text)
    assert "export_executed" in types                 # 085's own addition
    assert {"supply_qty_changed", "ingestion_complete", "onhand_updated"} <= types
    assert "reconciliation_completed" not in types     # only 086 adds it
    assert C2_NEW_TYPES.isdisjoint(types)              # C2's new types aren't here yet


def test_reference_086_list_is_085_plus_reconciliation_completed():
    """Cross-check the hardcoded EXPECTED_086_TYPES against the live 085 file:
    086 adds EXACTLY one type (reconciliation_completed) to 085. Catches a
    fat-fingered reference list before it can mask a real 088 regression."""
    assert len(EXPECTED_086_TYPES) == 24
    assert "reconciliation_completed" in EXPECTED_086_TYPES
    types_085 = _event_type_check_types(_migration_text("085_*.sql"))
    assert types_085 == EXPECTED_086_TYPES - {"reconciliation_completed"}, (
        "the hardcoded 086 reference set is not exactly 085 ∪ "
        "{reconciliation_completed} — reconcile it before trusting the 088 guard"
    )


def test_c2_reused_types_are_already_in_the_schema():
    """The plan's 'réutilisé' column: these 4 types must already be at the
    CHECK (002-era), so filling their ledger columns needs no widening."""
    types_085 = _event_type_check_types(_migration_text("085_*.sql"))
    assert C2_REUSED_TYPES <= types_085


def test_c2_new_types_are_genuinely_new():
    """The plan's 'NEW' column: these 4 must NOT already exist (else a name
    collision / no-op widening). Disjoint from the current CHECK and from the
    reused set."""
    types_085 = _event_type_check_types(_migration_text("085_*.sql"))
    assert C2_NEW_TYPES.isdisjoint(types_085)
    assert C2_NEW_TYPES.isdisjoint(C2_REUSED_TYPES)
    assert len(C2_NEW_TYPES) == 4


# ===========================================================================
# RED UNTIL VOLET 1/2 — the migration-088 proofs proper.
# ===========================================================================

def test_088_migration_file_exists():
    assert _migration("088_*.sql") is not None, (
        "migration 088 (C2 value-ledger + decision-basis) is not present yet — "
        "delivered by volets 1/2; this proof stays red until it lands"
    )


def test_088_check_is_widened_from_086_not_085():
    """Doctrine C2 §4 — THE migration trap. 088's events CHECK must be a
    superset of the FULL 086 list (so nothing already accepted is dropped) plus
    the 4 new C2 types. A superset assertion (not equality) stays correct even
    if a concurrent 087 legitimately adds its own types before the rebase."""
    text = _migration_text("088_*.sql")
    assert text is not None, "migration 088 not present yet (volets 1/2)"
    types = _event_type_check_types(text)
    assert types, "migration 088 does not widen events.event_type at all"

    dropped = EXPECTED_086_TYPES - types
    assert not dropped, (
        f"migration 088 DROPS type(s) already accepted at 086: {sorted(dropped)} "
        "— it restarted from 085 (or earlier) instead of the full 086 list "
        "(doctrine C2 §4)"
    )
    missing_new = C2_NEW_TYPES - types
    assert not missing_new, (
        f"migration 088 does not add the C2 value-ledger type(s): {sorted(missing_new)}"
    )


def test_088_keeps_reconciliation_completed_in_the_check():
    """The single most likely §4 casualty, called out on its own for a loud,
    unambiguous failure message."""
    text = _migration_text("088_*.sql")
    assert text is not None, "migration 088 not present yet (volets 1/2)"
    types = _event_type_check_types(text)
    assert "reconciliation_completed" in types, (
        "'reconciliation_completed' (added by migration 086) is missing from "
        "088's CHECK — sequential application 086->088 would REVOKE it. 088 "
        "must restart from the complete 086 list (doctrine C2 §4)"
    )


def test_088_adds_calc_run_decision_basis_columns():
    """Doctrine C2 §3 — calc_runs gains anchor_date/engine_flavor/code_version,
    each additively (ADD COLUMN IF NOT EXISTS)."""
    text = _migration_text("088_*.sql")
    assert text is not None, "migration 088 not present yet (volets 1/2)"
    stripped = _strip_line_comments(text)
    for col in CALC_RUN_BASIS_COLUMNS:
        assert re.search(
            rf"ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+{col}\b", stripped, re.IGNORECASE
        ), f"migration 088 does not add calc_runs.{col} (ADD COLUMN IF NOT EXISTS)"


def test_088_adds_recommendation_decision_basis_columns():
    """Doctrine C2 §3 — recommendations gains anchor_date + stream_seq_hwm."""
    text = _migration_text("088_*.sql")
    assert text is not None, "migration 088 not present yet (volets 1/2)"
    stripped = _strip_line_comments(text)
    for col in RECO_BASIS_COLUMNS:
        assert re.search(
            rf"ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+{col}\b", stripped, re.IGNORECASE
        ), f"migration 088 does not add recommendations.{col} (ADD COLUMN IF NOT EXISTS)"


def test_088_is_defensively_idempotent():
    """Doctrine C2 §6 — the runner wraps each file in ONE transaction and does
    NOT swallow 'already exists', so every statement must be replay-safe
    (pattern from migration 063). CHECK swap via DROP CONSTRAINT IF EXISTS;
    every added column via ADD COLUMN IF NOT EXISTS."""
    text = _migration_text("088_*.sql")
    assert text is not None, "migration 088 not present yet (volets 1/2)"
    stripped = _strip_line_comments(text)

    assert re.search(
        r"DROP\s+CONSTRAINT\s+IF\s+EXISTS\s+events_event_type_check",
        stripped, re.IGNORECASE,
    ), "088 must DROP CONSTRAINT IF EXISTS events_event_type_check before re-adding it"

    # No bare 'ADD COLUMN <name>' without the IF NOT EXISTS guard.
    bare = re.findall(r"ADD\s+COLUMN\s+(?!IF\s+NOT\s+EXISTS)", stripped, re.IGNORECASE)
    assert not bare, "088 has an ADD COLUMN without IF NOT EXISTS — not replay-safe"


def test_088_widens_only_the_events_check_no_business_jsonb():
    """Doctrine C2 §6 invariant — no business JSONB is introduced by this
    migration (its whole surface is typed columns + a CHECK)."""
    text = _migration_text("088_*.sql")
    assert text is not None, "migration 088 not present yet (volets 1/2)"
    stripped = _strip_line_comments(text).upper()
    assert "JSONB" not in stripped, "migration 088 must not introduce JSONB (C2 §6)"
