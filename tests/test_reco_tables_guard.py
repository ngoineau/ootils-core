"""
test_reco_tables_guard.py — CI guard (chantier « moteur d'exception »,
CHANTIER 1): every governed-RECOMMENDATION table in the schema is either
declared in ``engine/events/emit.py:_RECO_TABLES`` or EXPLICITLY exempted
here (``_RECO_EXEMPT_TABLES``, with a real justification).

WHY THIS MATTERS — the CLAUDE.md invariant, verbatim: "Any new governed-reco
table MUST be added to ``_RECO_TABLES``, else its watcher's recos are
invisible to the stream." ``emit_recommendation_created_for_run``
(engine/events/emit.py) COUNTs new rows across ``_RECO_TABLES`` by
``agent_run_id`` and emits ONE ``recommendation_created`` event into the
``events`` table iff ≥1 row was created. A governed recommendation written to
a table the helper does not know about produces NO event — so a fleet
subscriber on ``GET /v1/stream`` (North Star "Streamable") never sees it. This
guard fails the build the moment a migration adds such a table without wiring
it in, so the omission is caught at review time, not in production silence.

Pure/no-DB (CLAUDE.md unit-test convention) and a deliberate structural twin
of tests/test_purge_whitelist_guard.py: it re-derives the set of
governed-recommendation tables by PARSING the migration .sql files on disk
(no ``conn`` fixture, no DATABASE_URL) so it runs in every CI job, not only
the ones with a Postgres.

HEURISTIC — the most robust available, and the reason it is a UNION of two
independent signals (documented as the brief requested):
  1. NAME: the table name ends in ``recommendations`` — the repo's consistent
     naming convention for a governed-reco artifact table (``recommendations``,
     ``parameter_recommendations``, ``forecast_drift_recommendations``,
     ``eando_recommendations``). Catches a correctly-named future table even
     if its columns drift.
  2. COLUMN SIGNATURE: the CREATE TABLE body declares a ``decision_level``
     column. On the live schema this column appears in EXACTLY the four
     governed-reco tables above and nowhere else (verified: migrations
     039/041/045/072) — it is the Decision-Ladder stamp that, by North Star
     doctrine, every governed recommendation carries. Catches a MIS-NAMED
     future governed-reco table (one that does not follow the naming
     convention) that a name-only heuristic would miss.
A table matching EITHER signal is a "governed-reco candidate" and must be
classified. ``decision_level`` was chosen over ``agent_run_id`` as the column
signal precisely because it is tight: ``agent_run_id`` is carried by many
non-reco tables (agent_runs, dq_findings, …) and would flood the candidate
set with false positives, whereas ``decision_level`` isolates exactly the
governed-recommendation family. Known limitation (honest): a governed-reco
table that neither follows the naming convention NOR carries a
``decision_level`` column would escape both signals — but that shape violates
two established repo conventions at once and is expected to be caught in
review regardless.

CLASSIFICATION of the current candidate set:
  * recommendations, parameter_recommendations,
    forecast_drift_recommendations  -> in _RECO_TABLES (stream-emitted).
  * eando_recommendations            -> EXEMPT (disposition changes, not the
    supply/param/forecast ACTION recos the stream announces — see the exempt
    entry's justification and engine/events/emit.py's module docstring).

Migration-parsing semantics (identical to the purge guard): line comments are
stripped first (a commented-out ``decision_level`` mention or table name never
counts); ``CREATE TABLE IF NOT EXISTS`` on an already-tracked name is the
real-Postgres no-op it is (first creation since the last DROP wins); a
``DROP TABLE`` resets tracking.
"""
from __future__ import annotations

import re
from pathlib import Path

from ootils_core.db.connection import MIGRATIONS_DIR
from ootils_core.engine.events.emit import _RECO_TABLES

# Governed-reco tables that match the discovery heuristic but are DELIBERATELY
# NOT stream-emitted as ``recommendation_created``. Each entry needs a real
# justification (≥20 chars, enforced below) — the exact "explicit exemption,
# never a silent weakening" posture of PURGE_EXEMPT_TABLES. Kept here rather
# than in engine/events/emit.py because it has no RUNTIME use (emit.py only
# COUNTs the include-list _RECO_TABLES); it is purely a guard-side registry of
# the deliberate exclusions emit.py's docstring already explains in prose.
_RECO_EXEMPT_TABLES: dict[str, str] = {
    "eando_recommendations": (
        "Excess & Obsolete DISPOSITION recommendations (migration 045): "
        "governed L1 DRAFTs, but disposition changes (STOP_BUY / SCRAP / "
        "HOLD / REDEPLOY …), NOT the supply / planning-param / forecast-drift "
        "ACTION recommendations the stream announces as recommendation_created. "
        "Deliberately excluded from _RECO_TABLES per engine/events/emit.py's "
        "module docstring — baseline-only by nature, outside the #340/#347 "
        "scenario-backed scope; counting them would mislabel a disposition "
        "scan as an action-recommendation run."
    ),
}

_CREATE_OR_DROP_TABLE_RE = re.compile(
    r'CREATE TABLE\s+(?:IF NOT EXISTS\s+)?"?(\w+)"?\s*\(|'
    r'DROP TABLE\s+(?:IF EXISTS\s+)?"?(\w+)"?'
)


def _strip_line_comments(text: str) -> str:
    """Drop everything from '--' to end-of-line, per line — so a commented-out
    ``decision_level`` reference or table name never registers as a match
    (migrations 061/066 mention decision_level ONLY in comments)."""
    lines = []
    for line in text.split("\n"):
        idx = line.find("--")
        lines.append(line if idx == -1 else line[:idx])
    return "\n".join(lines)


def _table_body(text: str, open_paren_pos: int) -> str:
    """Substring between a CREATE TABLE's opening '(' and its matching ')',
    by depth counting (adequate for this repo's migrations — no unbalanced
    paren inside a string literal in a CREATE TABLE body)."""
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
    raise AssertionError(
        f"unbalanced parens scanning CREATE TABLE from position {open_paren_pos}"
    )


def _is_reco_candidate(table_name: str, body: str) -> bool:
    """UNION heuristic (see module docstring): name ends in 'recommendations'
    OR the CREATE TABLE body declares a decision_level column."""
    by_name = table_name.endswith("recommendations")
    by_column = re.search(r"\bdecision_level\b", body) is not None
    return by_name or by_column


def _discover_reco_candidate_tables() -> set[str]:
    """Parse every migration .sql in filename (= applied) order, tracking
    which tables currently exist and whether each is a governed-reco
    candidate — respecting DROP resets and CREATE-IF-NOT-EXISTS no-ops."""
    table_is_candidate: dict[str, bool] = {}

    for path in sorted(Path(MIGRATIONS_DIR).glob("*.sql")):
        text = _strip_line_comments(path.read_text(encoding="utf-8"))
        for match in _CREATE_OR_DROP_TABLE_RE.finditer(text):
            create_name, drop_name = match.group(1), match.group(2)
            if create_name is not None:
                if create_name in table_is_candidate:
                    continue  # real-Postgres no-op — first creation wins
                body = _table_body(text, match.end() - 1)
                table_is_candidate[create_name] = _is_reco_candidate(create_name, body)
            elif drop_name is not None:
                table_is_candidate.pop(drop_name, None)

    return {name for name, is_candidate in table_is_candidate.items() if is_candidate}


def test_every_governed_reco_table_is_included_or_exempted():
    discovered = _discover_reco_candidate_tables()
    classified = set(_RECO_TABLES) | set(_RECO_EXEMPT_TABLES)

    unclassified = discovered - classified
    assert not unclassified, (
        "The following governed-recommendation table(s) are neither in "
        "engine/events/emit.py:_RECO_TABLES nor in this guard's "
        "_RECO_EXEMPT_TABLES: "
        f"{sorted(unclassified)}. CLAUDE.md invariant — a governed-reco table "
        "MISSING from _RECO_TABLES is INVISIBLE to GET /v1/stream: "
        "emit_recommendation_created_for_run COUNTs new rows only across "
        "_RECO_TABLES, so a reco written elsewhere emits no "
        "recommendation_created event and no fleet subscriber ever sees it. "
        "Add the table to _RECO_TABLES (if its recos SHOULD reach the stream) "
        "or to _RECO_EXEMPT_TABLES (with a real justification, if it is a "
        "disposition/finding table deliberately kept off the stream)."
    )


def test_include_list_and_exemptions_do_not_overlap():
    overlap = set(_RECO_TABLES) & set(_RECO_EXEMPT_TABLES)
    assert not overlap, (
        f"table(s) in BOTH _RECO_TABLES and _RECO_EXEMPT_TABLES: {sorted(overlap)}"
    )


def test_reco_tables_has_no_duplicate_entries():
    assert len(_RECO_TABLES) == len(set(_RECO_TABLES))


def test_every_exemption_has_a_real_justification():
    for table, reason in _RECO_EXEMPT_TABLES.items():
        assert isinstance(reason, str) and len(reason.strip()) >= 20, (
            f"_RECO_EXEMPT_TABLES[{table!r}] has no real justification comment"
        )


def test_no_stale_exemptions():
    """An exemption for a table that is no longer even a governed-reco
    candidate (renamed / dropped / lost its decision_level column) is dead
    documentation — catch it so _RECO_EXEMPT_TABLES stays an accurate map of
    the LIVE schema."""
    discovered = _discover_reco_candidate_tables()
    stale = set(_RECO_EXEMPT_TABLES) - discovered
    assert not stale, (
        f"_RECO_EXEMPT_TABLES references table(s) no longer discovered as "
        f"governed-reco candidates: {sorted(stale)}"
    )


def test_reco_tables_members_are_discovered_candidates():
    """Every _RECO_TABLES entry must itself be a real, discovered governed-reco
    table — guards against a typo/rename in _RECO_TABLES that would make
    emit_recommendation_created_for_run COUNT against a non-existent table."""
    discovered = _discover_reco_candidate_tables()
    missing = set(_RECO_TABLES) - discovered
    assert not missing, (
        f"_RECO_TABLES lists table(s) not found as governed-reco candidates in "
        f"the migrations on disk (typo or dropped table?): {sorted(missing)}"
    )


def test_eando_recommendations_is_excluded_by_design():
    """Locks in the deliberate exclusion (emit.py docstring): eando_recommendations
    IS a governed-reco candidate (name + decision_level) but is a DISPOSITION
    table, never stream-emitted as recommendation_created — it must stay out of
    _RECO_TABLES and in the exempt registry."""
    discovered = _discover_reco_candidate_tables()
    assert "eando_recommendations" in discovered
    assert "eando_recommendations" not in _RECO_TABLES
    assert "eando_recommendations" in _RECO_EXEMPT_TABLES
