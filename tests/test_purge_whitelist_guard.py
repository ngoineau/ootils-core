"""
test_purge_whitelist_guard.py — CI guard: every scenario-scoped table in the
schema is either in PURGE_WHITELIST or explicitly exempted
(PURGE_EXEMPT_TABLES), both in engine/maintenance/purge.py (PURGE-1,
migration 076).

Pure/no-DB (CLAUDE.md unit-test convention): re-derives the set of
scenario-scoped tables by PARSING the migration .sql files on disk (no
`conn` fixture, no DATABASE_URL needed) — the same signal this repo's own
FK-retention guard (tests/integration/test_scenario_fk_retention.py) checks
at the live-DB level via pg_constraint, made usable without Postgres so it
runs in every CI job, not just the ones with a database.

WHAT COUNTS AS "SCENARIO-SCOPED": a table whose CREATE TABLE body declares a
literal ``scenario_id`` column (DIRECT scope — the overwhelming majority of
cases), PLUS two tables verified BY HAND to be scenario-scoped INDIRECTLY
(ADR-004's explanation chain, which carries no scenario_id column of its
own): ``explanations`` (scoped via calc_run_id -> calc_runs.scenario_id) and
``causal_steps`` (scoped via explanation_id -> explanations -> calc_runs). A
regex over ``scenario_id`` alone cannot discover these two on its own — they
are listed explicitly below, with the identical rationale documented in
``engine/maintenance/purge.py``'s module docstring (whitelist entries 1 + 6).

A migration's ``CREATE TABLE IF NOT EXISTS`` on an ALREADY-tracked table name
is treated as the real-Postgres no-op it is (the first creation since the
last DROP is authoritative); a ``DROP TABLE`` resets tracking so a later
``CREATE`` for the same name starts fresh. This is what correctly EXCLUDES
``zone_transition_runs`` from the discovered set: migration 002 created it
WITH a scenario_id column, but migration 003 DROPped and recreated it
WITHOUT one (see ``test_zone_transition_runs_is_not_scenario_scoped`` below)
— it is a global job-tracking table today, not per-scenario data, and
appears in neither ``PURGE_WHITELIST`` nor ``PURGE_EXEMPT_TABLES``.
"""
from __future__ import annotations

import re
from pathlib import Path

from ootils_core.db.connection import MIGRATIONS_DIR
from ootils_core.engine.maintenance.purge import PURGE_EXEMPT_TABLES, PURGE_WHITELIST

# Verified by hand against migration 004 (ADR-004): neither table carries a
# literal scenario_id column, but both are scenario-scoped transitively — see
# engine/maintenance/purge.py's module docstring (whitelist entries 1 + 6).
_INDIRECT_SCENARIO_SCOPED_TABLES: frozenset[str] = frozenset({"explanations", "causal_steps"})

_CREATE_OR_DROP_TABLE_RE = re.compile(
    r'CREATE TABLE\s+(?:IF NOT EXISTS\s+)?"?(\w+)"?\s*\(|'
    r'DROP TABLE\s+(?:IF EXISTS\s+)?"?(\w+)"?'
)


def _strip_line_comments(text: str) -> str:
    """Drop everything from '--' to end-of-line, per line — reduces false
    positives from a commented-out scenario_id mention or table name."""
    lines = []
    for line in text.split("\n"):
        idx = line.find("--")
        lines.append(line if idx == -1 else line[:idx])
    return "\n".join(lines)


def _table_body(text: str, open_paren_pos: int) -> str:
    """The substring between a CREATE TABLE's opening '(' and its matching
    closing ')', by depth counting. Adequate for this repo's migrations: no
    string literal inside a CREATE TABLE body contains an unbalanced paren."""
    depth = 0
    i = open_paren_pos
    body_start = open_paren_pos + 1
    while i < len(text):
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return text[body_start:i]
        i += 1
    raise AssertionError(f"unbalanced parens scanning CREATE TABLE from position {open_paren_pos}")


def _discover_scenario_scoped_tables() -> set[str]:
    """Parse every migration .sql file, in filename (= applied) order,
    tracking which tables currently exist and whether their CREATE TABLE
    body mentions scenario_id — respecting DROP TABLE resets and the
    real-Postgres semantics of ``CREATE TABLE IF NOT EXISTS`` on a table
    that already exists (a no-op; the first creation since the last DROP is
    authoritative)."""
    table_has_scenario: dict[str, bool] = {}

    for path in sorted(Path(MIGRATIONS_DIR).glob("*.sql")):
        text = _strip_line_comments(path.read_text(encoding="utf-8"))
        for match in _CREATE_OR_DROP_TABLE_RE.finditer(text):
            create_name, drop_name = match.group(1), match.group(2)
            if create_name is not None:
                if create_name in table_has_scenario:
                    continue  # real-Postgres no-op — first creation wins
                body = _table_body(text, match.end() - 1)
                table_has_scenario[create_name] = bool(re.search(r"\bscenario_id\b", body))
            elif drop_name is not None:
                table_has_scenario.pop(drop_name, None)

    discovered = {name for name, has_scenario in table_has_scenario.items() if has_scenario}
    return discovered | set(_INDIRECT_SCENARIO_SCOPED_TABLES)


def test_every_scenario_scoped_table_is_whitelisted_or_exempted():
    discovered = _discover_scenario_scoped_tables()
    classified = set(PURGE_WHITELIST) | set(PURGE_EXEMPT_TABLES)

    unclassified = discovered - classified
    assert not unclassified, (
        "The following scenario-scoped table(s) are neither in "
        "PURGE_WHITELIST nor PURGE_EXEMPT_TABLES (engine/maintenance/"
        "purge.py) — a migration added scenario_id to a table this guard "
        "has never classified. Add it to PURGE_WHITELIST (with its FK-safe "
        "position) or to PURGE_EXEMPT_TABLES (with an explicit "
        f"justification): {sorted(unclassified)}"
    )


def test_whitelist_and_exemptions_do_not_overlap():
    overlap = set(PURGE_WHITELIST) & set(PURGE_EXEMPT_TABLES)
    assert not overlap, (
        f"table(s) listed in BOTH PURGE_WHITELIST and PURGE_EXEMPT_TABLES: {sorted(overlap)}"
    )


def test_whitelist_has_no_duplicate_entries():
    assert len(PURGE_WHITELIST) == len(set(PURGE_WHITELIST))


def test_every_exemption_has_a_real_justification():
    for table, reason in PURGE_EXEMPT_TABLES.items():
        assert isinstance(reason, str) and len(reason.strip()) >= 20, (
            f"PURGE_EXEMPT_TABLES[{table!r}] has no real justification comment"
        )


def test_no_stale_exemptions():
    """An exemption entry for a table that no longer even exists in the
    schema (renamed/dropped) is dead documentation — catch it so
    PURGE_EXEMPT_TABLES stays an accurate map of the LIVE schema."""
    discovered = _discover_scenario_scoped_tables()
    stale = set(PURGE_EXEMPT_TABLES) - discovered
    assert not stale, f"PURGE_EXEMPT_TABLES references table(s) no longer scenario-scoped: {sorted(stale)}"


def test_zone_transition_runs_is_not_scenario_scoped():
    """Locks in the migration-003 finding: zone_transition_runs was
    scenario-scoped in migration 002 but migration 003 DROPped and
    recreated it WITHOUT scenario_id (a global job-tracking table, not
    per-scenario data) — it must never silently reappear in either list."""
    discovered = _discover_scenario_scoped_tables()
    assert "zone_transition_runs" not in discovered
    assert "zone_transition_runs" not in PURGE_WHITELIST
    assert "zone_transition_runs" not in PURGE_EXEMPT_TABLES
