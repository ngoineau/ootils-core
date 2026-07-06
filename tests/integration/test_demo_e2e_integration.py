"""
tests/integration/test_demo_e2e_integration.py — #408, THE invariant test.

Makes the executable wedge runbook (scripts/demo_e2e.py) a CI invariant: on the
CI-seeded base, a full ``demo_e2e.main([...])`` run must exit 0 with no FAIL in
its scoreboard, prove the cryptographic governance gate, archive (never delete)
its what-if fork, draft the XFER-01 DC-ATL -> DC-LAX transfer, capture snapshots
and evaluate outcomes, and NEVER print the DSN. A second run must stay exit 0 and
add no duplicates (idempotence). Requires a live PostgreSQL — skips otherwise.

WATCHERS MODE — deliberately ``--skip-watchers``:
  The demo's step 5 (the #392 governance gate) needs a baseline DRAFT
  recommendation to govern. With ``--skip-watchers``, step 4 (DRP) supplies that
  DRAFT: on the seeded base seed_drp() plants the XFER-01 lane, and
  ``POST /v1/drp/run`` drafts one governed TRANSFER (status='DRAFT', baseline,
  action='TRANSFER') into the ``recommendations`` table — which is exactly what
  step 5's ``_pick_draft_reco`` (newest baseline DRAFT) then approves. This is
  the path the runbook itself documents (DEMO-RUNBOOK.md §4: "step 5 then governs
  a DRAFT from the DRP step instead"). Verified against the script: step5 selects
  ``WHERE status='DRAFT' AND scenario_id=<baseline> ORDER BY created_at DESC``,
  and the DRP emitter writes exactly such a row. Running the watcher would add
  its own recos + a fork and slow the run, without changing what steps 4/5/6
  prove — so it is skipped, and steps 3 & 9 report SKIP (never FAIL), keeping the
  scoreboard failure-free.

Premises verified in the script + seed before writing (not assumed):
  * mask_dsn/scoreboard never emit the DSN (assert dsn not in captured stdout).
  * seed_drp plants XFER-01 with recommended_qty rounding to 180 (its own
    docstring + engine.drp.core parameters).
  * DELETE /v1/scenarios/{id} sets status='archived' (scenarios.py:151), never
    a row delete — the fork must survive as 'archived'.
  * recommendation_transitions carries actor_kind ('human'/'agent'), migration
    040; the human approval stamps actor_kind='human'.
  * api_tokens: name / actor_kind / scopes(TEXT[]) columns, migration 064; the
    demo mints DEMO-E2E-agent (['read','recommend:draft']) and DEMO-E2E-human
    (['read','ingest','recommend:draft','recommend:approve']).
  * inventory_snapshots / recommendation_outcomes exist (migrations 067/069).

The seed is driven exactly as scripts/seed_demo_data.py's __main__ does
(seed + seed_enrichment + seed_bom + seed_calendars + seed_drp), via subprocess,
mirroring tests/integration/test_seed.py::_run_seed.
"""
from __future__ import annotations

import contextlib
import io
import os
import subprocess
import sys
from pathlib import Path

import pytest

from .conftest import TEST_DB_URL, requires_db

# demo_e2e.py + the seed live under scripts/ (outside the installed package).
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

_SEED_SCRIPT = _SCRIPTS_DIR / "seed_demo_data.py"

BASELINE = "00000000-0000-0000-0000-000000000001"

# Module-scoped: the full run is not cheap (forecast + DRP + simulate fork +
# snapshots + outcomes). smoke marks it like the sibling seed-and-run batteries
# (test_scenario_backed_watchers_integration.py) — NOT slow (no timing asserts).
pytestmark = [requires_db, pytest.mark.smoke]


def _run_seed() -> subprocess.CompletedProcess:
    """Run the full seed (scripts/seed_demo_data.py __main__) against the test
    DB — seed + enrichment + bom + calendars + drp, exactly as production."""
    env = {**os.environ, "DATABASE_URL": TEST_DB_URL}
    return subprocess.run(
        [sys.executable, str(_SEED_SCRIPT)],
        capture_output=True,
        text=True,
        env=env,
    )


def _connect():
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(TEST_DB_URL, row_factory=dict_row, autocommit=True)


def _count(sql: str, params: tuple = ()) -> int:
    # Only hand params to psycopg when there ARE params: execute(sql, ())
    # switches psycopg into placeholder-validation mode, where a literal '%'
    # (e.g. a LIKE pattern) raises ProgrammingError. execute(sql) does not.
    with _connect() as conn:
        cur = conn.execute(sql, params) if params else conn.execute(sql)
        return int(cur.fetchone()["n"])


def _run_demo(argv_extra: list[str]) -> tuple[int, str]:
    """Call demo_e2e.main in-process with the seeded DSN, capturing stdout.

    Imported lazily (after OOTILS_API_TOKEN is set) so the module's deferred
    ootils_core imports resolve against a valid environment. stderr is left
    alone — main() only writes there on the pre-flight guard branches, which
    this integration run never hits (valid token + ootils target)."""
    import demo_e2e

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = demo_e2e.main(["--dsn", TEST_DB_URL, *argv_extra])
    return rc, buf.getvalue()


@pytest.fixture(scope="module")
def seeded_demo_db(migrated_db):
    """Seed the CI demo dataset once, and guarantee OOTILS_API_TOKEN is set (the
    app refuses to boot without it, and demo_e2e.main hard-exits 2 if it is
    absent). Yields the seeded DSN."""
    os.environ.setdefault("OOTILS_API_TOKEN", "integration-test-token")

    result = _run_seed()
    assert result.returncode == 0, (
        f"seed failed (rc={result.returncode}):\n"
        f"stdout: {result.stdout[-2000:]}\nstderr: {result.stderr[-2000:]}"
    )
    return migrated_db


# ===========================================================================
# 1. Full run — exit 0, no FAIL, DSN never printed
# ===========================================================================


@pytest.fixture(scope="module")
def first_run(seeded_demo_db):
    """Run the demo once (module-scoped) so the exit-code, no-FAIL, no-DSN, and
    all post-run DB assertions share ONE run instead of re-running per test."""
    rc, out = _run_demo(["--skip-watchers"])
    return rc, out


def test_full_run_exits_zero(first_run):
    rc, out = first_run
    assert rc == 0, f"demo_e2e.main exited {rc}\n{out[-3000:]}"


def test_full_run_scoreboard_has_no_fail(first_run):
    _rc, out = first_run
    # The scoreboard prints one "[FAIL] step N" line per failed step; the TOTAL
    # line reports "... / N fail". Neither may indicate a failure.
    assert "[FAIL]" not in out, f"a step FAILED:\n{out[-3000:]}"
    assert "0 fail" in out, f"scoreboard shows a non-zero fail count:\n{out[-1500:]}"


def test_full_run_never_prints_the_dsn(first_run):
    _rc, out = first_run
    # The single hardest guarantee: the raw DSN (credentials/host/port/query)
    # appears NOWHERE in the operator-facing output.
    assert TEST_DB_URL not in out, "the DSN leaked into the demo stdout"


def test_full_run_reports_the_expected_steps(first_run):
    _rc, out = first_run
    # Sanity that the run actually executed the wedge (not an early abort): the
    # gate, DRP, proof, and stream step headers are present.
    for marker in (
        "STEP 0",
        "Governance gate",
        "DRP transfer",
        "Proof machine",
        "StreamChanges",
    ):
        assert marker in out, f"missing step marker {marker!r} in run output"


# ===========================================================================
# 2. Post-run DB verifications
# ===========================================================================


def test_gate_human_approval_recorded(first_run):
    # The gate's human REVIEWED->APPROVED landed: the governed reco is APPROVED
    # and its approving transition is stamped actor_kind='human' (the token wins,
    # never the body). A rejected agent transition to APPROVED must NOT exist.
    with _connect() as conn:
        approved = conn.execute(
            """
            SELECT r.recommendation_id
            FROM recommendations r
            WHERE r.status = 'APPROVED' AND r.scenario_id = %s
            """,
            (BASELINE,),
        ).fetchall()
        assert approved, "no APPROVED recommendation after the gate step"

        # At least one APPROVED reco has a human-stamped APPROVED transition.
        human_approvals = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM recommendation_transitions
            WHERE to_status = 'APPROVED' AND actor_kind = 'human'
            """
        ).fetchone()["n"]
        assert int(human_approvals) >= 1, "no human-actor APPROVED transition"

        # The agent's attempt to APPROVE was refused (403) — it must have written
        # NO APPROVED transition under actor_kind='agent'.
        agent_approvals = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM recommendation_transitions
            WHERE to_status = 'APPROVED' AND actor_kind = 'agent'
            """
        ).fetchone()["n"]
        assert int(agent_approvals) == 0, (
            "an agent-actor APPROVED transition exists — the human gate leaked"
        )


def test_whatif_fork_is_archived_never_deleted(first_run):
    # The what-if fork must survive as status='archived' (TTL pattern), not be
    # DELETEd. It is named demo-e2e-whatif-<ts>.
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT status FROM scenarios
            WHERE name LIKE 'demo-e2e-whatif-%'
            """
        ).fetchall()
    assert rows, "the what-if fork row is gone (it must be archived, not deleted)"
    assert all(r["status"] == "archived" for r in rows), (
        f"a what-if fork is not archived: {[r['status'] for r in rows]}"
    )


def test_drp_emitted_the_xfer01_transfer(first_run):
    # seed_drp plants XFER-01 DC-ATL -> DC-LAX; the DRP step drafts exactly one
    # governed TRANSFER of 180 units (fair-share 185 DOWN-rounded to mult 10).
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT recommended_qty
            FROM recommendations
            WHERE action = 'TRANSFER'
              AND item_external_id = 'XFER-01'
              AND scenario_id = %s
            """,
            (BASELINE,),
        ).fetchall()
    assert rows, "no XFER-01 TRANSFER recommendation emitted by DRP"
    qtys = {float(r["recommended_qty"]) for r in rows}
    assert 180.0 in qtys, f"expected an XFER-01 transfer of 180, got {qtys}"


def test_snapshots_and_outcomes_present(first_run):
    # The proof machine ran: inventory_snapshots captured (source='api', the demo
    # uses the API capturer) and recommendation_outcomes evaluated.
    snaps = _count("SELECT COUNT(*) AS n FROM inventory_snapshots")
    assert snaps >= 1, "no inventory_snapshots after the proof step"
    outcomes = _count("SELECT COUNT(*) AS n FROM recommendation_outcomes")
    assert outcomes >= 1, "no recommendation_outcomes after the proof step"


def test_demo_tokens_exist_with_expected_scopes(first_run):
    # The two governed tokens were minted with the right kind + scopes.
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT name, actor_kind, scopes
            FROM api_tokens
            WHERE name LIKE 'DEMO-E2E-%' AND revoked_at IS NULL
            """
        ).fetchall()
    by_name = {r["name"]: r for r in rows}
    assert "DEMO-E2E-agent" in by_name, "agent demo token missing"
    assert "DEMO-E2E-human" in by_name, "human demo token missing"

    agent = by_name["DEMO-E2E-agent"]
    assert agent["actor_kind"] == "agent"
    assert set(agent["scopes"]) == {"read", "recommend:draft"}

    human = by_name["DEMO-E2E-human"]
    assert human["actor_kind"] == "human"
    assert set(human["scopes"]) == {
        "read",
        "ingest",
        "recommend:draft",
        "recommend:approve",
    }


# ===========================================================================
# 3. Idempotence — second run stays 0, no significant duplication
# ===========================================================================


def test_second_run_is_idempotent(first_run):
    # Snapshot the meaningful, deterministic-keyed populations after run 1…
    def _snapshot() -> dict[str, int]:
        return {
            "transfer_recos": _count(
                "SELECT COUNT(*) AS n FROM recommendations "
                "WHERE action = 'TRANSFER' AND item_external_id = 'XFER-01'"
            ),
            # Snapshots upsert per (scenario, item, location, as_of_date); a
            # same-day re-run must not add rows for today's as_of_date.
            "snapshots_today": _count(
                "SELECT COUNT(*) AS n FROM inventory_snapshots "
                "WHERE as_of_date = CURRENT_DATE"
            ),
            # Tokens are found+reused by name (the demo mints a fresh secret but
            # the DEMO-E2E-<role> NAME set stays exactly two live rows only if the
            # prior run's rows are not revoked — the demo does not revoke them, so
            # a re-run adds at most the freshly minted pair; assert the NAME set
            # size, not the row count, stays 2 distinct names).
            "distinct_token_names": _count(
                "SELECT COUNT(DISTINCT name) AS n FROM api_tokens "
                "WHERE name LIKE %s",
                ("DEMO-E2E-%",),
            ),
        }

    before = _snapshot()

    rc2, out2 = _run_demo(["--skip-watchers"])
    assert rc2 == 0, f"second demo run exited {rc2}\n{out2[-3000:]}"
    assert "[FAIL]" not in out2, f"second run had a FAIL:\n{out2[-3000:]}"
    assert TEST_DB_URL not in out2, "the DSN leaked into the second run stdout"

    after = _snapshot()

    # uuid5-keyed TRANSFER recos: identical plan re-run adds no new row.
    assert after["transfer_recos"] == before["transfer_recos"], (
        f"TRANSFER recos duplicated: {before['transfer_recos']} -> "
        f"{after['transfer_recos']}"
    )
    # Snapshots upsert on the same as_of_date: today's count is stable.
    assert after["snapshots_today"] == before["snapshots_today"], (
        f"snapshots for today duplicated: {before['snapshots_today']} -> "
        f"{after['snapshots_today']}"
    )
    # The DEMO-E2E name set stays exactly the two roles (found by name, not
    # proliferating new names on a re-run).
    assert after["distinct_token_names"] == before["distinct_token_names"] == 2, (
        "the DEMO-E2E token NAME set is not the stable two roles: "
        f"{before['distinct_token_names']} -> {after['distinct_token_names']}"
    )


# ===========================================================================
# 4. --out artefact — parseable, DSN-free, summary consistent with exit 0
# ===========================================================================


def test_out_artefact_is_written_dsn_free_and_consistent(seeded_demo_db, tmp_path):
    import json

    out_path = tmp_path / "scoreboard.json"
    rc, stdout = _run_demo(["--skip-watchers", "--out", str(out_path)])
    assert rc == 0, f"run with --out exited {rc}\n{stdout[-2000:]}"

    assert out_path.exists(), "--out did not write the scoreboard artefact"
    raw = out_path.read_text(encoding="utf-8")
    assert TEST_DB_URL not in raw, "the DSN leaked into the scoreboard artefact"

    payload = json.loads(raw)  # parseable
    # db field is the masked name only.
    assert payload["db"].startswith("db=")
    assert TEST_DB_URL not in payload["db"]
    # Summary consistent with a clean run: exit 0 <=> zero fails.
    assert payload["summary"]["fail"] == 0
    assert payload["summary"]["pass"] >= 1
    # Steps present and well-shaped.
    assert isinstance(payload["steps"], list) and payload["steps"]
    assert {"number", "title", "status", "detail", "data"} <= set(payload["steps"][0])
