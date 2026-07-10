"""
tests/integration/test_scenario_compare_integration.py — chantier SC-1.

DB-backed coverage of ``GET /v1/scenarios/compare`` against a real Postgres,
no mocks — the real-SQL twin of tests/test_scenario_compare.py (which covers
the pure math and the mocked router boundary). This file asserts the pieces
the DB-free tests cannot:

  * the MANDATORY ``calc_run_id = _latest_calc_run(:id)`` scoping of the
    ``shortages`` aggregate (an older completed run's rows must NOT stack) and
    the ``status='active'`` filter (a 'resolved' row is invisible);
  * the mirrored unit-cost LATERAL (propagator_sql.py:262-274 minus the ',1'):
    preferred-supplier unit_cost beats a cheaper non-preferred row, a
    unit_cost<=0 supplier row is skipped, items.standard_cost is the fallback,
    and a fully unpriced item resolves NULL -> unpriced_count++ contributing $0;
  * GREATEST(closing_stock,0) on real PI rows and the per-bucket average;
  * fill_rate from real ``nodes.outflows`` vs the latest run's stockout qty —
    including the None-honest zero-demand fork (never a masked 1.0);
  * the two REAL stale triggers: a baseline 'scenario_merge' event (the exact
    row shape ScenarioManager.promote() emits — event_type/scenario_id/source
    validated by the migration-002 CHECKs) created AFTER the fork's KPI run,
    and a fork whose OWN latest calc_run (any status — the second, independent
    query) is 'completed_stale';
  * per-scenario ValueError containment (a fork with only a 'running' run gets
    a computable=false entry naming itself; the request survives);
  * whole-request 422 for an unknown (but well-formed) id, message naming the
    id, no psycopg/DSN leak;
  * the kill switch (OOTILS_SCENARIO_COMPARE_ENABLED=0 -> 503) on the real app;
  * baseline admitted as an ordinary entry AND winning the delta reference even
    when passed last; an archived fork included with its status;
  * read purity: a 200 compare writes ZERO ``events`` rows (contract point 4 —
    "read pur, AUCUN event/audit").

Honesty note on the stale seed: the merge test INSERTs the ``events`` row
directly with the exact column shape ``ScenarioManager.promote()`` emits
(event_type='scenario_merge', scenario_id=BASELINE, source='engine' — verified
against manager.py's promote step 4 and the migration-002 CHECK lists) rather
than driving a full promote; a real promote needs a divergent node overlay and
would test the promote machinery, not the compare read path. The row shape IS
the real contract the reader (_fetch_latest_merge_event_at) depends on.

Determinism: timestamps are anchored on the DB-side now() with explicit
make_interval offsets (never Python now()), like the sibling watcher seeds.
Each test _reset()s the graph/calc surface FK-ordered (shortages before
nodes/calc_runs before items/locations); the migration-002 baseline scenario
row is NEVER truncated; fork rows accumulated across tests are harmless (every
test queries only the fork ids it just created, and ``events`` — the one
baseline-global read — is wiped by every _reset).
"""
from __future__ import annotations

import os
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from .conftest import requires_db

pytestmark = requires_db

BASELINE = "00000000-0000-0000-0000-000000000001"

# The auth module reads OOTILS_API_TOKEN from the environment per request
# (auth.py:_expected_token) — set it before create_app() (which fails loudly
# without it), same pattern as tests/integration/test_api_db.py.
os.environ.setdefault("OOTILS_API_TOKEN", "integration-test-token")
_TOKEN = os.environ["OOTILS_API_TOKEN"]
AUTH = {"Authorization": f"Bearer {_TOKEN}"}


# ---------------------------------------------------------------------------
# App client wired to the real test DB.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def client(migrated_db):
    """Module-scoped TestClient over the migrated test DB. get_db is overridden
    with a direct dict_row connection (which also disables the api_request_log
    middleware — app.py:_should_audit_request skips overridden-get_db apps, so
    the read-purity assertion can focus on the typed ``events`` table). No
    lifespan: the compare path needs no startup recovery/pool sizing."""
    os.environ["DATABASE_URL"] = migrated_db

    from fastapi.testclient import TestClient

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db

    app = create_app()

    def override_db():
        with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
            yield conn
            conn.rollback()  # GET is read-pure; never persist anything from it

    app.dependency_overrides[get_db] = override_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture()
def db(migrated_db):
    """Function-scoped autocommit seed connection, reset at the start of every
    test (FK order: shortages -> nodes -> calc_runs; supplier_items ->
    suppliers/items; CASCADE sweeps dependents). The baseline scenario row is
    deliberately NOT truncated (migration-002 seed); fork rows from prior tests
    are harmless — see the module docstring."""
    with psycopg.connect(migrated_db, autocommit=True, row_factory=dict_row) as conn:
        conn.execute(
            "TRUNCATE shortages, nodes, calc_runs, events, "
            "supplier_items, suppliers, items, locations "
            "RESTART IDENTITY CASCADE"
        )
        yield conn


# ---------------------------------------------------------------------------
# Seed helpers — every column validated against the real migrations:
# scenarios.status CHECK (002), calc_runs.status CHECK (002), nodes.node_type/
# time_grain CHECKs (002), shortages.severity_class CHECK (017) + the
# (pi_node_id, calc_run_id) unique index (005), supplier_items.lead_time_days
# NOT NULL > 0 (007), items.standard_cost nullable (042).
# ---------------------------------------------------------------------------


def _fork(conn, name, *, status="active"):
    return str(
        conn.execute(
            "INSERT INTO scenarios (name, is_baseline, status, parent_scenario_id) "
            "VALUES (%s, FALSE, %s, %s) RETURNING scenario_id",
            (name, status, BASELINE),
        ).fetchone()["scenario_id"]
    )


def _calc_run(conn, scenario_id, *, status="completed", completed_min_ago=0):
    """One calc_run whose completed_at is DB-side now() minus N minutes (never
    Python now() — the sibling watcher determinism rule). A non-terminal
    status ('running'/'pending') gets a NULL completed_at, like the real
    CalcRunManager state machine would leave it."""
    if status in ("completed", "completed_stale"):
        row = conn.execute(
            "INSERT INTO calc_runs (scenario_id, status, started_at, completed_at) "
            "VALUES (%s, %s, now() - make_interval(mins => %s + 1), "
            "        now() - make_interval(mins => %s)) "
            "RETURNING calc_run_id",
            (scenario_id, status, completed_min_ago, completed_min_ago),
        ).fetchone()
    else:
        row = conn.execute(
            "INSERT INTO calc_runs (scenario_id, status, started_at, completed_at) "
            "VALUES (%s, %s, now() - make_interval(mins => %s + 1), NULL) "
            "RETURNING calc_run_id",
            (scenario_id, status, completed_min_ago),
        ).fetchone()
    return str(row["calc_run_id"])


def _item(conn, name, *, standard_cost=None):
    return str(
        conn.execute(
            "INSERT INTO items (name, item_type, standard_cost) "
            "VALUES (%s, 'finished_good', %s) RETURNING item_id",
            (name, standard_cost),
        ).fetchone()["item_id"]
    )


def _location(conn, name):
    return str(
        conn.execute(
            "INSERT INTO locations (name, location_type) "
            "VALUES (%s, 'dc') RETURNING location_id",
            (name,),
        ).fetchone()["location_id"]
    )


def _supplier_link(conn, item_id, *, unit_cost, is_preferred=False):
    supplier_id = conn.execute(
        "INSERT INTO suppliers (name) VALUES (%s) RETURNING supplier_id",
        (f"SUP-{uuid4().hex[:8]}",),
    ).fetchone()["supplier_id"]
    conn.execute(
        "INSERT INTO supplier_items (supplier_id, item_id, lead_time_days, "
        " unit_cost, is_preferred) VALUES (%s, %s, 7, %s, %s)",
        (supplier_id, item_id, unit_cost, is_preferred),
    )


def _pi(conn, scenario_id, item_id, loc_id, bucket, *, closing, outflows):
    """One ProjectedInventory coordinate. closing/outflows may be None (an
    un-computed node) — the compare SQL must skip NULL closing_stock rows."""
    return str(
        conn.execute(
            "INSERT INTO nodes (node_type, scenario_id, item_id, location_id, "
            " time_grain, time_span_start, time_span_end, bucket_sequence, "
            " closing_stock, outflows, active) "
            "VALUES ('ProjectedInventory', %s, %s, %s, 'day', "
            "        CURRENT_DATE + %s, CURRENT_DATE + %s + 1, %s, %s, %s, TRUE) "
            "RETURNING node_id",
            (scenario_id, item_id, loc_id, bucket, bucket, bucket, closing, outflows),
        ).fetchone()["node_id"]
    )


def _shortage(
    conn,
    scenario_id,
    pi_node_id,
    calc_run_id,
    *,
    qty,
    severity,
    severity_class="stockout",
    status="active",
):
    conn.execute(
        "INSERT INTO shortages (scenario_id, pi_node_id, shortage_date, "
        " shortage_qty, severity_score, severity_class, calc_run_id, status) "
        "VALUES (%s, %s, CURRENT_DATE, %s, %s, %s, %s, %s)",
        (scenario_id, pi_node_id, qty, severity, severity_class, calc_run_id, status),
    )


def _merge_event(conn, *, minutes_ago):
    """The EXACT row shape ScenarioManager.promote() step 4 emits on the
    baseline (see the module docstring's honesty note)."""
    conn.execute(
        "INSERT INTO events (event_type, scenario_id, old_text, new_text, "
        " processed, source, created_at) "
        "VALUES ('scenario_merge', %s, %s, 'promoted', FALSE, 'engine', "
        "        now() - make_interval(mins => %s))",
        (BASELINE, str(uuid4()), minutes_ago),
    )


def _events_count(conn):
    return conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"]


def _compare(client, ids, **kw):
    return client.get(
        "/v1/scenarios/compare", params={"ids": ",".join(str(i) for i in ids)},
        headers=AUTH, **kw,
    )


# ===========================================================================
# 1. Two forks with different shortages — ranked in $ side by side, signed
#    deltas, calc_run scoping, 'resolved' exclusion, read purity.
# ===========================================================================


def test_two_forks_ranked_in_dollars_with_signed_deltas(client, db):
    """The flagship case. Fork WORSE (passed first -> the delta reference):
    2 active stockouts ($400 + $100) + 1 below_safety ($50) on its LATEST
    completed run, an OLDER completed run carrying a $99999 shortage that must
    NOT stack (the MANDATORY calc_run_id filter), stock (0+0+30)*$10 avg over
    3 buckets = $100, fill 1 - 40/100 = 0.6. Fork BETTER: 1 stockout ($50), a
    'resolved' $77777 row that must stay invisible, stock (40+50+60)*$4/3 =
    $200, fill 0.95. Entries come back in request order, severity ranked worse
    > better, deltas = entry - reference, comparable=true (no merge event ever
    -> stale=false per contract point 2), and the GET writes ZERO events rows."""
    loc = _location(db, "DC-1")

    worse = _fork(db, "worse")
    item_w = _item(db, "ITM-W", standard_cost=Decimal("10"))
    old_run = _calc_run(db, worse, completed_min_ago=60 * 24)  # yesterday
    run_w = _calc_run(db, worse, completed_min_ago=0)  # the KPI-bearing run
    w_b0 = _pi(db, worse, item_w, loc, 0, closing=Decimal("-25"), outflows=Decimal("50"))
    w_b1 = _pi(db, worse, item_w, loc, 1, closing=Decimal("-15"), outflows=Decimal("30"))
    w_b2 = _pi(db, worse, item_w, loc, 2, closing=Decimal("30"), outflows=Decimal("20"))
    _shortage(db, worse, w_b0, run_w, qty=Decimal("25"), severity=Decimal("400"))
    _shortage(db, worse, w_b1, run_w, qty=Decimal("15"), severity=Decimal("100"))
    _shortage(db, worse, w_b2, run_w, qty=Decimal("5"), severity=Decimal("50"),
              severity_class="below_safety_stock")
    # The stacking trap: same PI, OLDER completed run, huge $ — must be invisible.
    _shortage(db, worse, w_b0, old_run, qty=Decimal("999"), severity=Decimal("99999"))

    better = _fork(db, "better")
    item_b = _item(db, "ITM-B", standard_cost=Decimal("4"))
    run_b = _calc_run(db, better, completed_min_ago=0)
    b_b0 = _pi(db, better, item_b, loc, 0, closing=Decimal("40"), outflows=Decimal("50"))
    b_b1 = _pi(db, better, item_b, loc, 1, closing=Decimal("50"), outflows=Decimal("30"))
    _pi(db, better, item_b, loc, 2, closing=Decimal("60"), outflows=Decimal("20"))
    _shortage(db, better, b_b0, run_b, qty=Decimal("5"), severity=Decimal("50"))
    # The status trap: an active-run row a human resolved — must be invisible.
    _shortage(db, better, b_b1, run_b, qty=Decimal("70"), severity=Decimal("77777"),
              status="resolved")

    events_before = _events_count(db)
    resp = _compare(client, [worse, better])
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # Envelope: request order, reference = first id, comparable, citation.
    assert [e["scenario_id"] for e in body["entries"]] == [worse, better]
    assert body["reference_scenario_id"] == worse
    assert body["comparable"] is True
    assert "propagator_sql.py:262-274" in body["cost_precedence"]

    e_w, e_b = body["entries"]

    # WORSE — the latest run only: 2 stockouts, 1 below-safety, $550 (never
    # $99999-stacked), stock (GREATEST(-25,0)+GREATEST(-15,0)+30)*10/3 = 100.
    assert e_w["computable"] is True and e_w["stale"] is False
    assert e_w["calc_run_id"] == run_w
    assert e_w["computed_at"] is not None
    assert e_w["parent_scenario_id"] == BASELINE
    k_w = e_w["kpis"]
    assert k_w["shortage_count"] == 2
    assert k_w["below_safety_stock_count"] == 1
    assert k_w["shortage_severity_usd"] == pytest.approx(550.0)
    assert k_w["stock_value_usd"] == pytest.approx(100.0)
    assert k_w["stock_value_basis_count"] == 3
    assert k_w["stock_value_unpriced_count"] == 0
    assert k_w["fill_rate_est"] == pytest.approx(0.6)
    assert k_w["fill_rate_basis_count"] == 3

    # BETTER — the resolved $77777 row is invisible.
    k_b = e_b["kpis"]
    assert k_b["shortage_count"] == 1
    assert k_b["below_safety_stock_count"] == 0
    assert k_b["shortage_severity_usd"] == pytest.approx(50.0)
    assert k_b["stock_value_usd"] == pytest.approx(200.0)
    assert k_b["fill_rate_est"] == pytest.approx(0.95)

    # Ranked in $: the reference really is the more severe scenario.
    assert k_w["shortage_severity_usd"] > k_b["shortage_severity_usd"]

    # Signed deltas: reference vs itself = hard zeros; BETTER = improvement.
    assert e_w["deltas"] == {
        "shortage_count_delta": 0,
        "severity_usd_delta": 0.0,
        "stock_value_usd_delta": 0.0,
        "fill_rate_delta": 0.0,
    }
    d_b = e_b["deltas"]
    assert d_b["shortage_count_delta"] == -1
    assert d_b["severity_usd_delta"] == pytest.approx(-500.0)
    assert d_b["stock_value_usd_delta"] == pytest.approx(100.0)
    assert d_b["fill_rate_delta"] == pytest.approx(0.35)

    # Read purity (contract point 4): the GET wrote ZERO events rows.
    assert _events_count(db) == events_before, "a compare must never write events"


# ===========================================================================
# 2. The mirrored unit-cost LATERAL, on real supplier_items rows.
# ===========================================================================


def test_unit_cost_precedence_mirrors_shortages_sql(client, db):
    """The precedence the unit tests cannot reach: preferred supplier beats a
    CHEAPER non-preferred row (ORDER BY is_preferred DESC first, unit_cost ASC
    second); a unit_cost=0 row is excluded (unit_cost > 0) so the item falls
    back to items.standard_cost. Item P: preferred $8 vs non-preferred $3 ->
    $8. Item Z: one $0 supplier row + standard_cost $6 -> $6. One shared
    bucket, closing 10 each: stock_value = 10*8 + 10*6 = $140 over 1 bucket."""
    loc = _location(db, "DC-1")
    fork = _fork(db, "precedence")
    _calc_run(db, fork)

    item_p = _item(db, "ITM-P", standard_cost=Decimal("5"))
    _supplier_link(db, item_p, unit_cost=Decimal("8"), is_preferred=True)
    _supplier_link(db, item_p, unit_cost=Decimal("3"), is_preferred=False)
    item_z = _item(db, "ITM-Z", standard_cost=Decimal("6"))
    _supplier_link(db, item_z, unit_cost=Decimal("0"), is_preferred=True)

    _pi(db, fork, item_p, loc, 0, closing=Decimal("10"), outflows=None)
    _pi(db, fork, item_z, loc, 0, closing=Decimal("10"), outflows=None)

    other = _fork(db, "sibling")
    _calc_run(db, other)

    resp = _compare(client, [fork, other])
    assert resp.status_code == 200, resp.text
    k = resp.json()["entries"][0]["kpis"]
    assert k["stock_value_usd"] == pytest.approx(140.0), (
        "preferred $8 (not the cheaper $3) + standard-cost fallback $6 "
        "(the $0 supplier row must be skipped)"
    )
    assert k["stock_value_basis_count"] == 2
    assert k["stock_value_unpriced_count"] == 0


# ===========================================================================
# 3. unpriced_count — standard_cost NULL and no supplier row.
# ===========================================================================


def test_unpriced_item_counts_and_contributes_zero(client, db):
    """An item with NEITHER a priced supplier row NOR a standard_cost resolves
    unit_cost NULL (no ',1' fallback): counted in unpriced_count, contributes
    $0 — the priced item's $40 stands alone, never inflated or masked."""
    loc = _location(db, "DC-1")
    fork = _fork(db, "unpriced")
    _calc_run(db, fork)
    priced = _item(db, "ITM-OK", standard_cost=Decimal("4"))
    unpriced = _item(db, "ITM-NOCOST", standard_cost=None)
    _pi(db, fork, priced, loc, 0, closing=Decimal("10"), outflows=None)
    _pi(db, fork, unpriced, loc, 0, closing=Decimal("7"), outflows=None)

    other = _fork(db, "sibling")
    _calc_run(db, other)

    resp = _compare(client, [fork, other])
    assert resp.status_code == 200, resp.text
    k = resp.json()["entries"][0]["kpis"]
    assert k["stock_value_usd"] == pytest.approx(40.0), "unpriced contributes $0"
    assert k["stock_value_basis_count"] == 2, "the unpriced coordinate still counts"
    assert k["stock_value_unpriced_count"] == 1


# ===========================================================================
# 4. fill_rate None-honest — zero demand on real PI rows.
# ===========================================================================


def test_fill_rate_none_when_zero_demand(client, db):
    """A fork whose PI rows carry outflows 0/NULL has NO demand denominator:
    fill_rate_est must be null with basis_count 0 — never a masked 1.0 —
    while stock_value stays real (the two None triggers are independent)."""
    loc = _location(db, "DC-1")
    fork = _fork(db, "no-demand")
    _calc_run(db, fork)
    item = _item(db, "ITM-ND", standard_cost=Decimal("2"))
    _pi(db, fork, item, loc, 0, closing=Decimal("10"), outflows=Decimal("0"))
    _pi(db, fork, item, loc, 1, closing=Decimal("20"), outflows=None)

    other = _fork(db, "sibling")
    _calc_run(db, other)

    resp = _compare(client, [fork, other])
    assert resp.status_code == 200, resp.text
    k = resp.json()["entries"][0]["kpis"]
    assert k["fill_rate_est"] is None, "zero demand must be None, NEVER 1.0"
    assert k["fill_rate_basis_count"] == 0
    assert k["stock_value_usd"] == pytest.approx(30.0)  # (10*$2 + 20*$2) / 2 buckets


# ===========================================================================
# 5. Unknown id — whole-request 422 naming the id, no leak.
# ===========================================================================


def test_unknown_id_fails_whole_request_422(client, db):
    """A well-formed UUID with no scenarios row fails the WHOLE request (422),
    the hand-authored detail names the exact id, and nothing low-level leaks."""
    fork = _fork(db, "real")
    _calc_run(db, fork)
    ghost = str(uuid4())

    resp = _compare(client, [fork, ghost])
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert isinstance(detail, str), "hand-authored message, not a Pydantic list"
    assert ghost in detail
    assert "psycopg" not in resp.text.lower()
    assert "dsn" not in resp.text.lower()


# ===========================================================================
# 6. No completed calc_run — computable=false entry, request survives.
# ===========================================================================


def test_fork_without_completed_calc_run_is_computable_false(client, db):
    """A fork whose only calc_run is 'running' (never 'completed') trips
    _latest_calc_run's ValueError — caught PER scenario: its entry is present
    with kpis/deltas/stale/calc_run_id all null, computable=false and a note
    naming it; the healthy fork is untouched; comparable=false."""
    loc = _location(db, "DC-1")
    healthy = _fork(db, "healthy")
    item = _item(db, "ITM-H", standard_cost=Decimal("3"))
    _calc_run(db, healthy)
    _pi(db, healthy, item, loc, 0, closing=Decimal("10"), outflows=Decimal("5"))

    empty = _fork(db, "never-computed")
    _calc_run(db, empty, status="running")  # exists, but never completed

    resp = _compare(client, [healthy, empty])
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["comparable"] is False

    e_h, e_e = body["entries"]
    assert e_h["computable"] is True
    assert e_h["kpis"] is not None
    assert e_h["deltas"] is not None  # reference vs itself

    assert e_e["computable"] is False
    assert e_e["kpis"] is None
    assert e_e["deltas"] is None
    assert e_e["stale"] is None
    assert e_e["calc_run_id"] is None
    assert e_e["computed_at"] is None
    assert e_e["note"] is not None and empty in e_e["note"]


# ===========================================================================
# 7. Stale — a baseline merge posterior to the fork's calc.
# ===========================================================================


def test_stale_after_baseline_merge_breaks_comparability(client, db):
    """Contract point 2. A 'scenario_merge' event on the baseline (the exact
    row shape promote() emits) between the two forks' calc completions: the
    fork computed BEFORE the merge is stale=true, the fork computed AFTER
    stays stale=false, and one stale entry makes comparable=false."""
    old_fork = _fork(db, "computed-before-merge")
    _calc_run(db, old_fork, completed_min_ago=120)
    fresh_fork = _fork(db, "computed-after-merge")
    _calc_run(db, fresh_fork, completed_min_ago=0)

    _merge_event(db, minutes_ago=60)  # after old_fork's calc, before fresh_fork's

    resp = _compare(client, [old_fork, fresh_fork])
    assert resp.status_code == 200, resp.text
    body = resp.json()

    e_old, e_fresh = body["entries"]
    assert e_old["stale"] is True, "merge AFTER this fork's calc -> stale"
    assert e_old["computable"] is True, "stale is a freshness flag, not a failure"
    assert e_old["kpis"] is not None, "a stale entry still carries its numbers"
    assert e_fresh["stale"] is False, "calc AFTER the merge -> fresh"
    assert body["comparable"] is False, "one stale entry breaks comparability"


def test_stale_via_completed_stale_status_or_branch(client, db):
    """The OR-branch (module-docstring 'IMPORTANT' note): the KPI-bearing run
    is always literally 'completed', so this trigger needs the SECOND,
    any-status query — a fork whose LATEST run is 'completed_stale' is stale
    even with ZERO merge events, while its KPIs still come from the older
    'completed' run."""
    fork = _fork(db, "stale-status")
    kpi_run = _calc_run(db, fork, status="completed", completed_min_ago=60)
    _calc_run(db, fork, status="completed_stale", completed_min_ago=0)

    fresh = _fork(db, "fresh")
    _calc_run(db, fresh, completed_min_ago=0)

    assert _events_count(db) == 0, "seed sanity: no merge event anywhere"

    resp = _compare(client, [fork, fresh])
    assert resp.status_code == 200, resp.text
    body = resp.json()
    e_stale, e_fresh = body["entries"]
    assert e_stale["stale"] is True
    assert e_stale["calc_run_id"] == kpi_run, (
        "KPIs must come from the completed run, staleness from the newer "
        "completed_stale one"
    )
    assert e_fresh["stale"] is False
    assert body["comparable"] is False


# ===========================================================================
# 8. Kill switch — 503 on the real app.
# ===========================================================================


def test_kill_switch_off_is_503(client, db, monkeypatch):
    """OOTILS_SCENARIO_COMPARE_ENABLED=0 -> 503 naming the switch, checked
    after auth but before any compare work (both forks are real and
    computable — the 503 is the switch, not a data failure)."""
    fork_a = _fork(db, "a")
    _calc_run(db, fork_a)
    fork_b = _fork(db, "b")
    _calc_run(db, fork_b)

    monkeypatch.setenv("OOTILS_SCENARIO_COMPARE_ENABLED", "0")
    resp = _compare(client, [fork_a, fork_b])
    assert resp.status_code == 503
    assert "OOTILS_SCENARIO_COMPARE_ENABLED" in resp.json()["detail"]

    monkeypatch.delenv("OOTILS_SCENARIO_COMPARE_ENABLED")
    assert _compare(client, [fork_a, fork_b]).status_code == 200, "default is ON"


# ===========================================================================
# 9. Baseline admitted (and wins the reference), archived fork included.
# ===========================================================================


def test_baseline_reference_and_archived_fork_admitted(client, db):
    """Contract point 4: the baseline is an ordinary entry AND the delta
    reference even when passed LAST; an archived fork is included with its
    status (no status filtering). Baseline: a completed run, zero shortages
    (0-honest), no PI (stock/fill None-honest). Archived fork: 1 stockout.
    Deltas (archived - baseline): count +1, severity +$120, stock/fill None
    (the baseline side is None)."""
    loc = _location(db, "DC-1")
    baseline_run = _calc_run(db, BASELINE, completed_min_ago=0)

    archived = _fork(db, "what-if-archived", status="archived")
    item = _item(db, "ITM-A", standard_cost=Decimal("5"))
    run_a = _calc_run(db, archived)
    pi = _pi(db, archived, item, loc, 0, closing=Decimal("-8"), outflows=Decimal("10"))
    _shortage(db, archived, pi, run_a, qty=Decimal("8"), severity=Decimal("120"))

    resp = _compare(client, [archived, BASELINE])
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["reference_scenario_id"] == BASELINE, "baseline wins even passed last"
    e_arch, e_base = body["entries"]  # entries stay in REQUEST order

    assert e_base["scenario_id"] == BASELINE
    assert e_base["name"] == "Baseline"
    assert e_base["parent_scenario_id"] is None
    assert e_base["computable"] is True
    assert e_base["calc_run_id"] == baseline_run
    assert e_base["kpis"]["shortage_count"] == 0, "0-honest healthy baseline"
    assert e_base["kpis"]["stock_value_usd"] is None, "no PI -> None, never $0"
    assert e_base["kpis"]["fill_rate_est"] is None
    assert e_base["deltas"] == {
        "shortage_count_delta": 0,
        "severity_usd_delta": 0.0,
        "stock_value_usd_delta": None,
        "fill_rate_delta": None,
    }

    assert e_arch["status"] == "archived", "archived forks are included, with status"
    assert e_arch["computable"] is True
    assert e_arch["deltas"]["shortage_count_delta"] == 1
    assert e_arch["deltas"]["severity_usd_delta"] == pytest.approx(120.0)
    assert e_arch["deltas"]["stock_value_usd_delta"] is None, (
        "None on the baseline side propagates — a delta needs both operands"
    )
    assert e_arch["deltas"]["fill_rate_delta"] is None
    assert body["comparable"] is True, "archived+stale-free+computable is comparable"
