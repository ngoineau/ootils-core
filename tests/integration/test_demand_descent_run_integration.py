"""
tests/integration/test_demand_descent_run_integration.py — the demand-descent
run end to end (DESC-1 PR-B, ADR-043) against a real PostgreSQL — no mocks
(CLAUDE.md). The pure residual/quantize maths of ``engine/descent/run.py``
is unit-tested DB-free in tests/test_descent_run.py.

The six contract axes, driven through the REAL surface (POST
/v1/demand/descend on a module-scoped TestClient, pattern of
test_daily_orchestrator_integration.py):

  1. CAS NOMINAL — one virtual channel (``locations.is_stocking = FALSE``),
     two DCs, baseline ``demand_split_pct`` 60/40 for ITEM-A, one
     ForecastDemand 100 + one CustomerOrderDemand 50 on the channel (seeded
     through the real /v1/ingest/* endpoints): the run materializes the 4
     derived per-DC nodes (60/40 and 30/20), deactivates the split national
     sources, writes 4 ledger lines (``scenario_id`` NULL = baseline run,
     ``pct_applied`` frozen), preserves mass EXACTLY per source
     (SUM(qty_derived) == qty_source), emits EXACTLY ONE
     ``demand_descended`` event and traces a ``calc_runs`` row. The derived
     nodes are WIRED: every one carries active ``consumes`` edge(s) to
     ProjectedInventory buckets of ITS OWN DC (verified in SQL — 1 bucket
     for the exact_date CO, 7 for the week-span FD, per
     ``graph_wiring.wire_node_to_pi``'s span contract).
  2. IDEMPOTENCE — a second POST is an honest no-op: zero new nodes, zero
     new ledger lines, zero new events; the already-inactive sources are
     simply not considered again (only ITEM-B's still-national node is).
  3. ITEM SANS PARTS — ITEM-B carries demand on the channel but no
     ``demand_split_pct`` row: its node STAYS ACTIVE and national, the item
     is listed in ``items_without_shares`` (and in the event's ``old_text``)
     — ADR-043 fail-loudly, the demand is never invented onto a DC.
  4. FORKABILITE — a fork scenario (minimal INSERT: ``scenarios`` row + its
     own national FD node, per the sanctioned "scenario_id direct" path —
     the deep-copy fork API would copy baseline's already-descended state,
     which is a different test) with DIFFERENT split rows (80/20,
     ``scenario_id = fork``): the fork's descent applies 80/20 and the
     BASELINE stays byte-intact (node count, derived quantities, split
     percentages, ITEM-B's national node — all unchanged).
  5. KILL SWITCH — without ``OOTILS_DESCENT_ENABLED`` the endpoint answers
     503 before touching the DB; ``dry_run=true`` answers 200 with the
     computed counts but writes NOTHING (verified from a FRESH connection:
     no nodes at the DCs, no ledger rows, no event, no calc_run, sources
     still active).
  6. CONSERVATION DE MASSE POOLEE — the item-level SUM of ACTIVE demand
     (derived + remaining national) is IDENTICAL before and after the run:
     the pooled truth never moves, only its carrying nodes change
     (ADR-021/ADR-043 convergence).

ORDERING: the classes are deliberately sequential within the module (kill
switch / dry-run first — write-free — then nominal, then the tests that
read the nominal run's residue). Same module-lifecycle style as
test_daily_orchestrator_integration.py; the repo runs pytest in definition
order (no random-order plugin).

Isolation (pattern of test_daily_orchestrator_integration.py): referential
seeds under a unique PREFIX via the real API, neutralized by DEACTIVATION in
a module finalizer (items obsoleted, nodes deactivated, eligibility revoked,
the channel's is_stocking restored, fork archived) — never a DELETE cascade.
``demand_split_pct`` has no soft-delete flag and is left inert, keyed to the
obsoleted PREFIX items (same call as test_demand_descent_schema_integration).
The module-scoped ``migrated_db`` teardown drops the schema as the backstop.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from .conftest import requires_db

pytestmark = requires_db

TOKEN = "integration-test-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}

PREFIX = f"DESC-{uuid4().hex[:8]}"

BASELINE = UUID("00000000-0000-0000-0000-000000000001")

# Demand anchors safely inside the 90-day PI horizon graph_wiring creates.
TODAY = date.today()
FD_BUCKET = TODAY + timedelta(days=7)   # week grain → span [J+7, J+14)
CO_DATE = TODAY + timedelta(days=14)    # exact_date → single bucket

FD_QTY = Decimal("100.000000")
CO_QTY = Decimal("50.000000")
FD_B_QTY = Decimal("70.000000")

_DEMAND_TYPES = ("ForecastDemand", "CustomerOrderDemand")


def _ext(base: str) -> str:
    return f"{PREFIX}-{base}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _descend(client, *, dry_run: bool = False, scenario_id: UUID | None = None):
    url = "/v1/demand/descend"
    if scenario_id is not None:
        url = f"{url}?scenario_id={scenario_id}"
    return client.post(url, json={"dry_run": dry_run}, headers=AUTH)


def _descent_events(c, scenario_id: UUID) -> list[dict]:
    return c.execute(
        "SELECT field_changed, new_text, new_quantity, old_text, source, scenario_id "
        "FROM events WHERE event_type = 'demand_descended' AND scenario_id = %s "
        "ORDER BY created_at",
        (scenario_id,),
    ).fetchall()


def _ledger_rows(c, descent_run_id: UUID) -> list[dict]:
    return c.execute(
        "SELECT scenario_id, source_node_id, derived_node_id, item_id, "
        "dc_location_id, pct_applied, qty_source, qty_derived "
        "FROM demand_descent_lines WHERE descent_run_id = %s "
        "ORDER BY source_node_id, dc_location_id",
        (descent_run_id,),
    ).fetchall()


def _active_demand_by_item(c, scenario_id: UUID, item_ids: list[UUID]) -> dict[UUID, Decimal]:
    """Item-level pooled ACTIVE demand truth (derived + national alike)."""
    rows = c.execute(
        "SELECT item_id, SUM(quantity) AS total FROM nodes "
        "WHERE scenario_id = %s AND active = TRUE AND node_type = ANY(%s) "
        "AND item_id = ANY(%s) GROUP BY item_id",
        (scenario_id, list(_DEMAND_TYPES), item_ids),
    ).fetchall()
    return {row["item_id"]: row["total"] for row in rows}


def _count(c, sql: str, params: tuple = ()) -> int:
    return c.execute(sql, params).fetchone()["n"]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB (same
    pattern as test_daily_orchestrator_integration.py). The descent kill
    switch is read PER REQUEST (demand.py:_descent_enabled), so enabling it
    here governs every test except the ones that monkeypatch it off."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = TOKEN
    os.environ["OOTILS_DESCENT_ENABLED"] = "1"

    from fastapi.testclient import TestClient
    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB

    app = create_app()

    def override_db():
        db = OotilsDB(migrated_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()
    os.environ.pop("OOTILS_DESCENT_ENABLED", None)


@pytest.fixture(scope="module")
def seed(api_client, request, migrated_db):
    """One virtual channel (is_stocking=FALSE) + two DCs + two items, demand
    seeded through the REAL ingest endpoints (which wire the channel's own
    PI series exactly like production); split shares (60/40, ITEM-A only)
    and eligibility seeded by direct INSERT — migration 083's tables have no
    ingest surface yet (PR-F is the TSV flux). Neutralized by DEACTIVATION,
    never a DELETE."""
    items = [
        {"external_id": _ext("ITEM-A"), "name": "Descent item A",
         "item_type": "finished_good", "uom": "EA", "status": "active"},
        {"external_id": _ext("ITEM-B"), "name": "Descent item B (no shares)",
         "item_type": "finished_good", "uom": "EA", "status": "active"},
    ]
    resp = api_client.post("/v1/ingest/items", json={"items": items}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    locations = [
        {"external_id": _ext("NAT"), "name": "Descent national channel"},
        {"external_id": _ext("DC1"), "name": "Descent DC 1"},
        {"external_id": _ext("DC2"), "name": "Descent DC 2"},
    ]
    resp = api_client.post(
        "/v1/ingest/locations", json={"locations": locations}, headers=AUTH
    )
    assert resp.status_code == 200, resp.text

    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        ids = {
            r["external_id"]: r["item_id"]
            for r in c.execute(
                "SELECT external_id, item_id FROM items WHERE external_id LIKE %s",
                (PREFIX + "%",),
            ).fetchall()
        }
        locs = {
            r["external_id"]: r["location_id"]
            for r in c.execute(
                "SELECT external_id, location_id FROM locations WHERE external_id LIKE %s",
                (PREFIX + "%",),
            ).fetchall()
        }
        item_a, item_b = ids[_ext("ITEM-A")], ids[_ext("ITEM-B")]
        channel, dc1, dc2 = locs[_ext("NAT")], locs[_ext("DC1")], locs[_ext("DC2")]

        # The channel is a VIRTUAL demand-only location (migration 081) —
        # the descent's national-demand scope key.
        c.execute(
            "UPDATE locations SET is_stocking = FALSE WHERE location_id = %s",
            (channel,),
        )
        # Eligibility: explicit TRUE rows for ITEM-A at both DCs (an absent
        # pair is NOT eligible — run.py's eligibility gate). ITEM-B: none.
        for dc in (dc1, dc2):
            c.execute(
                "INSERT INTO item_dc_eligibility (item_id, dc_location_id, eligible, source) "
                "VALUES (%s, %s, TRUE, 'manual')",
                (item_a, dc),
            )
        # Baseline split 60/40 (scenario_id NULL = baseline, migration 083).
        for dc, pct in ((dc1, Decimal("0.6")), (dc2, Decimal("0.4"))):
            c.execute(
                "INSERT INTO demand_split_pct (scenario_id, item_id, dc_location_id, pct, method) "
                "VALUES (NULL, %s, %s, %s, 'manual')",
                (item_a, dc, pct),
            )
        c.commit()

    # National demand through the REAL ingest endpoints (baseline-only by
    # construction, which is exactly where the baseline runs read).
    resp = api_client.post(
        "/v1/ingest/forecast-demand",
        json={"forecasts": [
            {"item_external_id": _ext("ITEM-A"), "location_external_id": _ext("NAT"),
             "quantity": float(FD_QTY), "bucket_date": FD_BUCKET.isoformat(),
             "time_grain": "week"},
            {"item_external_id": _ext("ITEM-B"), "location_external_id": _ext("NAT"),
             "quantity": float(FD_B_QTY), "bucket_date": FD_BUCKET.isoformat(),
             "time_grain": "week"},
        ]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    resp = api_client.post(
        "/v1/ingest/customer-orders",
        json={"customer_orders": [
            {"external_id": _ext("CO-1"), "item_external_id": _ext("ITEM-A"),
             "location_external_id": _ext("NAT"), "quantity": float(CO_QTY),
             "requested_delivery_date": CO_DATE.isoformat(), "status": "open"},
        ]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text

    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        src = {
            (r["node_type"], r["item_id"]): r["node_id"]
            for r in c.execute(
                "SELECT node_id, node_type, item_id FROM nodes "
                "WHERE location_id = %s AND node_type = ANY(%s)",
                (channel, list(_DEMAND_TYPES)),
            ).fetchall()
        }

    def _neutralize():
        with psycopg.connect(migrated_db, autocommit=True) as c:
            c.execute(
                "UPDATE nodes SET active = FALSE WHERE item_id = ANY(%s)",
                ([item_a, item_b],),
            )
            c.execute(
                "UPDATE items SET status = 'obsolete' WHERE external_id LIKE %s",
                (PREFIX + "%",),
            )
            c.execute(
                "UPDATE item_dc_eligibility SET eligible = FALSE WHERE item_id = ANY(%s)",
                ([item_a, item_b],),
            )
            c.execute(
                "UPDATE locations SET is_stocking = TRUE WHERE location_id = %s",
                (channel,),
            )
            c.execute(
                "UPDATE scenarios SET status = 'archived' WHERE name LIKE %s",
                (PREFIX + "%",),
            )

    request.addfinalizer(_neutralize)
    return {
        "item_a": item_a, "item_b": item_b,
        "channel": channel, "dc1": dc1, "dc2": dc2,
        "fd_a": src[("ForecastDemand", item_a)],
        "fd_b": src[("ForecastDemand", item_b)],
        "co_a": src[("CustomerOrderDemand", item_a)],
    }


# ---------------------------------------------------------------------------
# 5a. Kill switch (write-free — runs before any descent has executed)
# ---------------------------------------------------------------------------


class TestKillSwitch:
    def test_disabled_switch_answers_503(self, api_client, seed, monkeypatch):
        """Without OOTILS_DESCENT_ENABLED the endpoint is a 503 — evaluated
        per request AFTER auth, BEFORE the DB dependency."""
        monkeypatch.delenv("OOTILS_DESCENT_ENABLED", raising=False)
        resp = _descend(api_client)
        assert resp.status_code == 503
        assert "OOTILS_DESCENT_ENABLED" in resp.json()["detail"]

    def test_falsy_switch_answers_503(self, api_client, seed, monkeypatch):
        monkeypatch.setenv("OOTILS_DESCENT_ENABLED", "0")
        assert _descend(api_client).status_code == 503

    def test_unknown_scenario_is_422(self, api_client, seed):
        """DescentError (unknown scenario) surfaces as the typed-domain 422
        carve-out — hand-authored message, UUID only."""
        ghost = uuid4()
        resp = _descend(api_client, scenario_id=ghost)
        assert resp.status_code == 422
        assert str(ghost) in resp.json()["detail"]


# ---------------------------------------------------------------------------
# 5b. dry_run=true — full computation, ZERO writes (before the nominal run)
# ---------------------------------------------------------------------------


class TestDryRun:
    def test_dry_run_computes_counts_but_writes_nothing(
        self, api_client, seed, migrated_db
    ):
        with psycopg.connect(migrated_db, row_factory=dict_row) as c:
            calc_runs_before = _count(c, "SELECT COUNT(*) AS n FROM calc_runs")
            events_before = _count(c, "SELECT COUNT(*) AS n FROM events")

        resp = _descend(api_client, dry_run=True)
        assert resp.status_code == 200
        body = resp.json()
        assert body["dry_run"] is True
        assert body["descent_run_id"] is None
        assert body["event_id"] is None
        # The plan WAS computed: all 3 national nodes considered, ITEM-B
        # honestly reported share-less.
        assert body["source_nodes_considered"] == 3
        assert body["source_nodes_deactivated"] == 0
        assert body["derived_nodes_created"] == 0
        assert body["lines_written"] == 0
        assert body["items_without_shares"] == [str(seed["item_b"])]
        assert body["recompute_triggered"] is False

        # ZERO writes — asserted from a FRESH connection.
        with psycopg.connect(migrated_db, row_factory=dict_row) as fresh:
            assert _count(
                fresh,
                "SELECT COUNT(*) AS n FROM nodes WHERE location_id = ANY(%s)",
                ([seed["dc1"], seed["dc2"]],),
            ) == 0  # no derived demand, no PI series at the DCs either
            assert _count(fresh, "SELECT COUNT(*) AS n FROM demand_descent_lines") == 0
            assert _descent_events(fresh, BASELINE) == []
            assert _count(fresh, "SELECT COUNT(*) AS n FROM calc_runs") == calc_runs_before
            assert _count(fresh, "SELECT COUNT(*) AS n FROM events") == events_before
            actives = fresh.execute(
                "SELECT node_id FROM nodes WHERE location_id = %s "
                "AND node_type = ANY(%s) AND active = TRUE",
                (seed["channel"], list(_DEMAND_TYPES)),
            ).fetchall()
            assert {r["node_id"] for r in actives} == {
                seed["fd_a"], seed["fd_b"], seed["co_a"]
            }


# ---------------------------------------------------------------------------
# 1 + 6. Nominal run — materialization, ledger, event, wiring, mass
# ---------------------------------------------------------------------------


class TestNominalDescent:
    def test_nominal_descend_splits_wires_ledgers_and_preserves_mass(
        self, api_client, seed, conn
    ):
        item_a, item_b = seed["item_a"], seed["item_b"]
        dc1, dc2 = seed["dc1"], seed["dc2"]

        # (6) Pooled truth BEFORE — item-level active demand.
        mass_before = _active_demand_by_item(conn, BASELINE, [item_a, item_b])
        assert mass_before[item_a] == FD_QTY + CO_QTY
        assert mass_before[item_b] == FD_B_QTY

        resp = _descend(api_client)
        assert resp.status_code == 200
        body = resp.json()
        assert body["dry_run"] is False
        assert body["scenario_id"] == str(BASELINE)
        assert body["source_nodes_considered"] == 3
        assert body["source_nodes_deactivated"] == 2      # FD-A + CO-A
        assert body["derived_nodes_created"] == 4          # 2 sources x 2 DCs
        assert body["lines_written"] == 4
        assert body["items_without_shares"] == [str(item_b)]
        assert body["descent_run_id"] is not None
        assert body["event_id"] is not None
        assert body["recompute_triggered"] is False
        run_id = UUID(body["descent_run_id"])

        # calc_run trace.
        run = conn.execute(
            "SELECT scenario_id, status, is_full_recompute FROM calc_runs "
            "WHERE calc_run_id = %s",
            (run_id,),
        ).fetchone()
        assert run is not None
        assert run["scenario_id"] == BASELINE
        assert run["status"] == "completed"
        assert run["is_full_recompute"] is False

        # The 4 derived nodes: 60/40 from the FD-100, 30/20 from the CO-50,
        # source node_type preserved, at the right DCs, active.
        derived = conn.execute(
            "SELECT n.node_type, n.location_id, n.quantity, n.active, n.scenario_id "
            "FROM demand_descent_lines l JOIN nodes n ON n.node_id = l.derived_node_id "
            "WHERE l.descent_run_id = %s ORDER BY n.node_type, n.quantity DESC",
            (run_id,),
        ).fetchall()
        assert [
            (r["node_type"], r["location_id"], r["quantity"]) for r in derived
        ] == [
            ("CustomerOrderDemand", dc1, Decimal("30.000000")),
            ("CustomerOrderDemand", dc2, Decimal("20.000000")),
            ("ForecastDemand", dc1, Decimal("60.000000")),
            ("ForecastDemand", dc2, Decimal("40.000000")),
        ]
        assert all(r["active"] and r["scenario_id"] == BASELINE for r in derived)

        # Split sources deactivated (anti-double-count); ITEM-B untouched.
        states = {
            r["node_id"]: r["active"]
            for r in conn.execute(
                "SELECT node_id, active FROM nodes WHERE node_id = ANY(%s)",
                ([seed["fd_a"], seed["co_a"], seed["fd_b"]],),
            ).fetchall()
        }
        assert states[seed["fd_a"]] is False
        assert states[seed["co_a"]] is False
        assert states[seed["fd_b"]] is True

        # Ledger: 4 lines, baseline runs stamp scenario_id NULL, pct frozen,
        # and mass conservation EXACT per source node.
        lines = _ledger_rows(conn, run_id)
        assert len(lines) == 4
        assert all(line["scenario_id"] is None for line in lines)
        assert all(line["item_id"] == item_a for line in lines)
        by_src_dc = {(line["source_node_id"], line["dc_location_id"]): line for line in lines}
        assert by_src_dc[(seed["fd_a"], dc1)]["pct_applied"] == Decimal("0.6")
        assert by_src_dc[(seed["fd_a"], dc2)]["pct_applied"] == Decimal("0.4")
        assert by_src_dc[(seed["co_a"], dc1)]["qty_derived"] == Decimal("30.000000")
        assert by_src_dc[(seed["co_a"], dc2)]["qty_derived"] == Decimal("20.000000")
        for src_node, qty in ((seed["fd_a"], FD_QTY), (seed["co_a"], CO_QTY)):
            src_lines = [ln for ln in lines if ln["source_node_id"] == src_node]
            assert all(ln["qty_source"] == qty for ln in src_lines)
            assert sum(ln["qty_derived"] for ln in src_lines) == qty

        # EXACTLY ONE demand_descended event, run-granular, None-honest.
        events = _descent_events(conn, BASELINE)
        assert len(events) == 1
        assert events[0]["field_changed"] == "demand_descended"
        assert events[0]["new_text"] == str(run_id)
        assert events[0]["new_quantity"] == 4
        assert events[0]["old_text"] == str(item_b)  # the share-less item
        assert events[0]["source"] == "api"

        # WIRING (in SQL): every derived node consumes PI buckets of ITS
        # own DC, same item, same scenario — zero mis-wired edges.
        assert _count(
            conn,
            "SELECT COUNT(*) AS n FROM demand_descent_lines l "
            "JOIN edges e ON e.from_node_id = l.derived_node_id AND e.edge_type = 'consumes' "
            "JOIN nodes pi ON pi.node_id = e.to_node_id "
            "WHERE l.descent_run_id = %s AND (pi.node_type <> 'ProjectedInventory' "
            "  OR pi.location_id <> l.dc_location_id OR pi.item_id <> l.item_id "
            "  OR pi.scenario_id <> %s)",
            (run_id, BASELINE),
        ) == 0
        edge_counts = conn.execute(
            "SELECT n.node_type, COUNT(e.edge_id) AS n_edges "
            "FROM demand_descent_lines l "
            "JOIN nodes n ON n.node_id = l.derived_node_id "
            "JOIN edges e ON e.from_node_id = n.node_id "
            "  AND e.edge_type = 'consumes' AND e.active "
            "WHERE l.descent_run_id = %s GROUP BY l.derived_node_id, n.node_type",
            (run_id,),
        ).fetchall()
        assert len(edge_counts) == 4  # every derived node is wired
        for r in edge_counts:
            if r["node_type"] == "CustomerOrderDemand":
                assert r["n_edges"] == 1   # exact_date → single bucket
            else:
                assert r["n_edges"] == 7   # week span [J+7, J+14) → 7 buckets

        # (6) Pooled truth AFTER == BEFORE, to the last unit.
        mass_after = _active_demand_by_item(conn, BASELINE, [item_a, item_b])
        assert mass_after == mass_before


# ---------------------------------------------------------------------------
# 2. Idempotence — a re-POST is an honest no-op
# ---------------------------------------------------------------------------


class TestIdempotence:
    def test_second_descend_is_a_clean_no_op(self, api_client, seed, conn):
        nodes_before = _count(conn, "SELECT COUNT(*) AS n FROM nodes")
        lines_before = _count(conn, "SELECT COUNT(*) AS n FROM demand_descent_lines")
        events_before = _count(conn, "SELECT COUNT(*) AS n FROM events")
        calc_runs_before = _count(conn, "SELECT COUNT(*) AS n FROM calc_runs")

        resp = _descend(api_client)
        assert resp.status_code == 200
        body = resp.json()
        # Only ITEM-B's still-national node is considered; the split sources
        # are inactive and out of scope — honest zeros everywhere else.
        assert body["source_nodes_considered"] == 1
        assert body["source_nodes_deactivated"] == 0
        assert body["derived_nodes_created"] == 0
        assert body["lines_written"] == 0
        assert body["descent_run_id"] is None
        assert body["event_id"] is None
        assert body["items_without_shares"] == [str(seed["item_b"])]

        # Zero new rows anywhere — the DB did not move.
        assert _count(conn, "SELECT COUNT(*) AS n FROM nodes") == nodes_before
        assert _count(conn, "SELECT COUNT(*) AS n FROM demand_descent_lines") == lines_before
        assert _count(conn, "SELECT COUNT(*) AS n FROM events") == events_before
        assert _count(conn, "SELECT COUNT(*) AS n FROM calc_runs") == calc_runs_before
        assert len(_descent_events(conn, BASELINE)) == 1


# ---------------------------------------------------------------------------
# 3. Item without shares — stays national, stays active, is listed
# ---------------------------------------------------------------------------


class TestItemWithoutShares:
    def test_item_b_stayed_national_and_active(self, seed, conn):
        node = conn.execute(
            "SELECT active, location_id, quantity FROM nodes WHERE node_id = %s",
            (seed["fd_b"],),
        ).fetchone()
        assert node["active"] is True
        assert node["location_id"] == seed["channel"]
        assert node["quantity"] == FD_B_QTY
        # Never split: no ledger line, no derived node at any DC.
        assert _count(
            conn,
            "SELECT COUNT(*) AS n FROM demand_descent_lines WHERE item_id = %s",
            (seed["item_b"],),
        ) == 0
        assert _count(
            conn,
            "SELECT COUNT(*) AS n FROM nodes WHERE item_id = %s AND location_id = ANY(%s)",
            (seed["item_b"], [seed["dc1"], seed["dc2"]]),
        ) == 0


# ---------------------------------------------------------------------------
# 4. Forkability — fork shares win, baseline stays intact
# ---------------------------------------------------------------------------


class TestForkability:
    def test_fork_descends_with_its_own_shares_baseline_untouched(
        self, api_client, seed, migrated_db, conn
    ):
        item_a, dc1, dc2 = seed["item_a"], seed["dc1"], seed["dc2"]
        fork = uuid4()
        fork_fd = uuid4()
        with psycopg.connect(migrated_db, row_factory=dict_row) as c:
            c.execute(
                "INSERT INTO scenarios (scenario_id, name, parent_scenario_id, status) "
                "VALUES (%s, %s, %s, 'active')",
                (fork, f"{PREFIX}-fork", BASELINE),
            )
            # Minimal copy: the fork's own national FD node on the channel
            # (mirrors ingest's ForecastDemand INSERT shape).
            c.execute(
                "INSERT INTO nodes (node_id, node_type, scenario_id, item_id, location_id, "
                "quantity, time_grain, time_ref, time_span_start, time_span_end, is_dirty, active) "
                "VALUES (%s, 'ForecastDemand', %s, %s, %s, %s, 'week', %s, %s, %s, TRUE, TRUE)",
                (fork_fd, fork, item_a, seed["channel"], FD_QTY,
                 FD_BUCKET, FD_BUCKET, FD_BUCKET + timedelta(days=7)),
            )
            # DIFFERENT split for the fork: 80/20 (scenario-scoped rows win
            # over the baseline 60/40 in run.py's DISTINCT ON resolution).
            for dc, pct in ((dc1, Decimal("0.8")), (dc2, Decimal("0.2"))):
                c.execute(
                    "INSERT INTO demand_split_pct (scenario_id, item_id, dc_location_id, pct, method) "
                    "VALUES (%s, %s, %s, %s, 'manual')",
                    (fork, item_a, dc, pct),
                )
            c.commit()

        # Baseline snapshot BEFORE the fork run.
        baseline_nodes_before = _count(
            conn, "SELECT COUNT(*) AS n FROM nodes WHERE scenario_id = %s", (BASELINE,)
        )
        baseline_mass_before = _active_demand_by_item(
            conn, BASELINE, [item_a, seed["item_b"]]
        )
        baseline_pcts_before = [
            r["pct"] for r in conn.execute(
                "SELECT pct FROM demand_split_pct WHERE scenario_id IS NULL "
                "AND item_id = %s ORDER BY pct",
                (item_a,),
            ).fetchall()
        ]
        assert baseline_pcts_before == [Decimal("0.4"), Decimal("0.6")]

        resp = _descend(api_client, scenario_id=fork)
        assert resp.status_code == 200
        body = resp.json()
        assert body["scenario_id"] == str(fork)
        assert body["source_nodes_considered"] == 1
        assert body["source_nodes_deactivated"] == 1
        assert body["derived_nodes_created"] == 2
        assert body["lines_written"] == 2
        assert body["items_without_shares"] == []
        fork_run_id = UUID(body["descent_run_id"])

        # The fork's derived nodes carry the FORK's 80/20 — not baseline's.
        fork_derived = conn.execute(
            "SELECT n.location_id, n.quantity, n.scenario_id "
            "FROM demand_descent_lines l JOIN nodes n ON n.node_id = l.derived_node_id "
            "WHERE l.descent_run_id = %s ORDER BY n.quantity DESC",
            (fork_run_id,),
        ).fetchall()
        assert [(r["location_id"], r["quantity"]) for r in fork_derived] == [
            (dc1, Decimal("80.000000")),
            (dc2, Decimal("20.000000")),
        ]
        assert all(r["scenario_id"] == fork for r in fork_derived)

        # Fork ledger is fork-stamped (never NULL) and mass-conserving.
        fork_lines = _ledger_rows(conn, fork_run_id)
        assert len(fork_lines) == 2
        assert all(line["scenario_id"] == fork for line in fork_lines)
        assert {line["pct_applied"] for line in fork_lines} == {
            Decimal("0.8"), Decimal("0.2")
        }
        assert sum(line["qty_derived"] for line in fork_lines) == FD_QTY

        # The fork's own event, scoped to the fork; its source deactivated;
        # its derived nodes wired to FORK-scenario PI buckets.
        fork_events = _descent_events(conn, fork)
        assert len(fork_events) == 1
        assert fork_events[0]["new_text"] == str(fork_run_id)
        assert conn.execute(
            "SELECT active FROM nodes WHERE node_id = %s", (fork_fd,)
        ).fetchone()["active"] is False
        assert _count(
            conn,
            "SELECT COUNT(*) AS n FROM demand_descent_lines l "
            "JOIN edges e ON e.from_node_id = l.derived_node_id "
            "  AND e.edge_type = 'consumes' AND e.active "
            "JOIN nodes pi ON pi.node_id = e.to_node_id "
            "WHERE l.descent_run_id = %s AND pi.node_type = 'ProjectedInventory' "
            "  AND pi.scenario_id = %s AND pi.location_id = l.dc_location_id",
            (fork_run_id, fork),
        ) >= 2  # both derived nodes wired inside the fork

        # BASELINE INTACT: no node added/changed, split rows unchanged,
        # pooled truth unchanged, exactly the one baseline event from the
        # nominal run.
        assert _count(
            conn, "SELECT COUNT(*) AS n FROM nodes WHERE scenario_id = %s", (BASELINE,)
        ) == baseline_nodes_before
        assert _active_demand_by_item(
            conn, BASELINE, [item_a, seed["item_b"]]
        ) == baseline_mass_before
        assert [
            r["pct"] for r in conn.execute(
                "SELECT pct FROM demand_split_pct WHERE scenario_id IS NULL "
                "AND item_id = %s ORDER BY pct",
                (item_a,),
            ).fetchall()
        ] == baseline_pcts_before
        baseline_derived = conn.execute(
            "SELECT n.quantity FROM demand_descent_lines l "
            "JOIN nodes n ON n.node_id = l.derived_node_id "
            "WHERE l.scenario_id IS NULL ORDER BY n.quantity",
            (),
        ).fetchall()
        assert [r["quantity"] for r in baseline_derived] == [
            Decimal("20.000000"), Decimal("30.000000"),
            Decimal("40.000000"), Decimal("60.000000"),
        ]
        assert len(_descent_events(conn, BASELINE)) == 1
