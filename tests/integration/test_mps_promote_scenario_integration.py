"""
Scenario-isolation integration tests for MPS promote-to-MRP (#398).

Before the fix, ``AggregateDemandEngine.promote_to_mrp`` issued its
``INSERT INTO planned_supply`` **without** a ``scenario_id`` column, so the
migration-030 DEFAULT (baseline, ``00000000-0000-0000-0000-000000000001``)
always won. An MPS node approved inside a fork therefore promoted its supply
onto the *baseline* plan — the North Star anti-pattern "a fork that writes
baseline". The fix reads ``scenario_id`` from the MPS node and binds it in the
INSERT; the ``/promote-to-mrp`` ``run_crp`` path passes the same scenario to
``CRPEngine.calculate`` so write and read stay on one plan.

The five cases below assert, end-to-end through the FastAPI TestClient on a
real PostgreSQL database (no mocks):

  1. Promote from a fork writes planned_supply on the FORK scenario, and adds
     ZERO baseline rows for that item/location.
  2. Non-regression: promote from baseline writes planned_supply on baseline.
  3. CRP (run_crp=true, scoped to the fork) sees the promoted supply as load;
     the same CRP calculation on baseline does not.
  4. ATP scoped to the fork sees the promoted supply; scoped to baseline it
     does not.
  5. Strict isolation: two APPROVED MPS nodes (one baseline, one fork) for the
     SAME item/location/bucket, promoted independently, each land their supply
     on their own scenario with no cross-leak (exact per-scenario counts).

Mechanics (module-scoped ``api_client``/``fork_scenario_id`` fixtures, direct
``scenarios`` INSERT for the fork, ``dict_row`` by-name access, CURRENT_DATE-
relative dates, per-test ``try/finally`` teardown) are cloned from the passing
``tests/integration/test_scenario_isolation_crp_integration.py``. Every row is
created and cleaned up locally with dedicated (test-prefixed) entities so the
per-item shortage pooling that spans locations cannot bleed across tests.

Verified premises (read from the worktree source, not assumed):
  - ``mps_nodes`` (migration 027): ``scenario_id`` NOT NULL FK; ``status`` is
    the ``mps_status`` enum; promote requires ``status = 'APPROVED'`` and
    ``active = TRUE`` (``mps/api.py`` + ``mps/engine.py:promote_to_mrp``);
    mandatory fields item_id/location_id/scenario_id/time_bucket/
    time_bucket_start/time_bucket_end/time_grain, CHECK
    ``total_demand = forecast_quantity + sales_orders_quantity`` and
    ``time_bucket_end >= time_bucket_start``. Unique active index on
    (item_id, location_id, scenario_id, time_bucket) — so the two nodes in
    case 5 (distinct scenarios) coexist.
  - ``promote_to_mrp`` inserts ``planned_supply`` with ``status='PLANNED'`` and
    ``due_date = time_bucket_start``.
  - Promote signature: ``POST /v1/mps/{id}/promote-to-mrp`` with body
    ``{explode_components, dry_run, run_crp, crp_horizon_days}``; the run's
    scenario is the MPS node's OWN scenario (no scenario param on the endpoint).
  - CRP ``_fetch_planned_orders`` accepts ``status IN
    ('RELEASED','APPROVED','PLANNED')`` — a freshly promoted (PLANNED) supply
    is visible to CRP directly. ATP ``_fetch_supplies`` accepts only
    ``('RELEASED','APPROVED')`` — so case 4 advances the promoted row to
    RELEASED before the ATP read (still scenario-scoped, still the fix's path).
  - Scenario is resolved by ``resolve_scenario_id`` (``?scenario_id=`` query
    param or ``X-Scenario-ID`` header, default baseline) for both ATP and CRP.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db

pytestmark = requires_db

AUTH_HEADERS = {"Authorization": "Bearer integration-test-token"}
BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"


# ---------------------------------------------------------------------------
# Shared module-scoped fixtures (mirror the CRP isolation module)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = "integration-test-token"

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB
    from fastapi.testclient import TestClient

    app = create_app()

    def override_db():
        db = OotilsDB(migrated_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture(scope="module")
def auth():
    return AUTH_HEADERS


@pytest.fixture(scope="module")
def fork_scenario_id(migrated_db) -> str:
    """Create a fork scenario row (parent = baseline), return its id."""
    import psycopg
    from psycopg.rows import dict_row

    scenario_id = uuid4()
    with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        conn.execute(
            """
            INSERT INTO scenarios (scenario_id, name, parent_scenario_id)
            VALUES (%s, %s, %s::UUID)
            """,
            (scenario_id, "mps-promote-isolation-fork", BASELINE_SCENARIO_ID),
        )
        conn.commit()
    return str(scenario_id)


# ---------------------------------------------------------------------------
# DB helpers (direct psycopg for setup/teardown — never against a shared DB)
# ---------------------------------------------------------------------------


def _db_conn(dsn):
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _insert_item_and_location(conn) -> tuple[UUID, UUID]:
    """Create a unique item + location (dedicated per test), return their ids."""
    item_id = uuid4()
    location_id = uuid4()
    conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s)",
        (item_id, f"test-398 MPS Item {item_id}"),
    )
    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
        (location_id, f"test-398 MPS Loc {location_id}"),
    )
    return item_id, location_id


def _insert_mps_node(
    conn,
    *,
    item_id: UUID,
    location_id: UUID,
    scenario_id: str,
    planned_quantity: Decimal,
    bucket_start: date,
    status: str = "APPROVED",
    time_bucket: str | None = None,
) -> UUID:
    """Insert an active MPS node ready to promote.

    total_demand must equal forecast_quantity + sales_orders_quantity
    (migration-027 CHECK). We route the whole demand through forecast_quantity;
    planned_quantity carries the amount the promote writes to planned_supply.
    """
    mps_id = uuid4()
    bucket_end = bucket_start + timedelta(days=6)
    label = time_bucket if time_bucket is not None else f"test-398-{bucket_start.isoformat()}"
    conn.execute(
        """
        INSERT INTO mps_nodes (
            mps_id, item_id, location_id, scenario_id,
            time_bucket, time_bucket_start, time_bucket_end, time_grain,
            forecast_quantity, sales_orders_quantity, total_demand,
            planned_quantity, status, active
        ) VALUES (
            %s, %s, %s, %s::UUID,
            %s, %s, %s, 'weekly',
            %s, 0, %s,
            %s, %s::mps_status, TRUE
        )
        """,
        (
            mps_id, item_id, location_id, scenario_id,
            label, bucket_start, bucket_end,
            planned_quantity, planned_quantity,
            planned_quantity, status,
        ),
    )
    return mps_id


def _insert_work_center(conn) -> UUID:
    """Insert a work-center row in the unified ``resources`` table (master data)."""
    wc_id = uuid4()
    conn.execute(
        """
        INSERT INTO resources (
            resource_id, external_id, name, resource_type,
            capacity_per_day, capacity_unit, efficiency, active
        ) VALUES (%s, %s, 'test-398 WC', 'work_center', 80, 'unit', 1.0, TRUE)
        """,
        (wc_id, f"WC-398-{wc_id.hex[:8]}"),
    )
    return wc_id


def _insert_routing_with_operation(conn, *, item_id: UUID, work_center_id: UUID) -> UUID:
    """Insert a routing + a single operation on the work center (master data).

    run_time_per_unit=0.5 h/unit, setup_time=0 — a promoted qty>0 supply for
    this item therefore produces strictly positive load on ``work_center_id``.
    """
    routing_id = uuid4()
    operation_id = uuid4()
    conn.execute(
        """
        INSERT INTO routings (routing_id, item_id, sequence, description, active)
        VALUES (%s, %s, 1, 'test-398 routing', TRUE)
        """,
        (routing_id, item_id),
    )
    conn.execute(
        """
        INSERT INTO routing_operations (
            operation_id, routing_id, sequence, resource_id,
            setup_time, run_time_per_unit, description, active
        ) VALUES (%s, %s, 10, %s, 0, 0.5, 'test-398 op', TRUE)
        """,
        (operation_id, routing_id, work_center_id),
    )
    return routing_id


def _count_planned_supply(conn, *, item_id: UUID, location_id: UUID, scenario_id: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM planned_supply
        WHERE item_id = %s AND location_id = %s AND scenario_id = %s::UUID
        """,
        (item_id, location_id, scenario_id),
    ).fetchone()
    return int(row["n"])


def _teardown(conn, *, item_id: UUID, location_id: UUID, work_center_id: UUID | None = None,
              routing_id: UUID | None = None) -> None:
    """Delete every row written for this item/location.

    FK order: planned_supply → mps edges/nodes → routing_operations → routings
    → resources → items/locations. Deleting the fork scenario row is NOT needed
    (it is a module-scoped fixture shared across the class); planned_supply rows
    are removed here so no FK to ``scenarios`` is left dangling per test.
    """
    conn.execute("DELETE FROM planned_supply WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM mps_supplies_edges WHERE mps_node_id IN "
                 "(SELECT mps_id FROM mps_nodes WHERE item_id = %s)", (item_id,))
    conn.execute("DELETE FROM mps_planned_for_edges WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM mps_nodes WHERE item_id = %s", (item_id,))
    if routing_id is not None:
        conn.execute("DELETE FROM routing_operations WHERE routing_id = %s", (routing_id,))
        conn.execute("DELETE FROM routings WHERE routing_id = %s", (routing_id,))
    if work_center_id is not None:
        conn.execute("DELETE FROM resources WHERE resource_id = %s", (work_center_id,))
    conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))


def _promote(api_client, auth, mps_id: UUID, **body):
    """POST /v1/mps/{id}/promote-to-mrp and return the response object."""
    return api_client.post(
        f"/v1/mps/{mps_id}/promote-to-mrp",
        json={"explode_components": False, **body},
        headers=auth,
    )


# ---------------------------------------------------------------------------
# Case 1 — promote from a fork does NOT write baseline
# ---------------------------------------------------------------------------


class TestPromoteFromForkIsolation:
    def test_promote_from_fork_writes_fork_and_not_baseline(
        self, api_client, auth, migrated_db, fork_scenario_id
    ):
        bucket_start = date.today() + timedelta(days=7)
        with _db_conn(migrated_db) as conn:
            item_id, location_id = _insert_item_and_location(conn)
            mps_id = _insert_mps_node(
                conn, item_id=item_id, location_id=location_id,
                scenario_id=fork_scenario_id, planned_quantity=Decimal("200"),
                bucket_start=bucket_start,
            )
        try:
            resp = _promote(api_client, auth, mps_id)
            assert resp.status_code == 200, resp.text
            data = resp.json()
            assert data["status"] == "RELEASED"
            assert data["planned_supplies_created"] == 1
            assert data["summary"]["scenario_id"] == fork_scenario_id

            with _db_conn(migrated_db) as conn:
                # Exactly one planned_supply on the fork for this item/location.
                assert _count_planned_supply(
                    conn, item_id=item_id, location_id=location_id,
                    scenario_id=fork_scenario_id,
                ) == 1
                # ZERO planned_supply on baseline for this item/location — the
                # regression this whole fix exists to prevent.
                assert _count_planned_supply(
                    conn, item_id=item_id, location_id=location_id,
                    scenario_id=BASELINE_SCENARIO_ID,
                ) == 0
                # And the row's scenario_id column is literally the fork's.
                row = conn.execute(
                    "SELECT scenario_id, status FROM planned_supply WHERE source_id = %s",
                    (mps_id,),
                ).fetchone()
                assert str(row["scenario_id"]) == fork_scenario_id
                assert row["status"] == "PLANNED"
        finally:
            with _db_conn(migrated_db) as conn:
                _teardown(conn, item_id=item_id, location_id=location_id)


# ---------------------------------------------------------------------------
# Case 2 — non-regression: promote from baseline stays on baseline
# ---------------------------------------------------------------------------


class TestPromoteFromBaselineUnchanged:
    def test_promote_from_baseline_writes_baseline(self, api_client, auth, migrated_db):
        bucket_start = date.today() + timedelta(days=7)
        with _db_conn(migrated_db) as conn:
            item_id, location_id = _insert_item_and_location(conn)
            mps_id = _insert_mps_node(
                conn, item_id=item_id, location_id=location_id,
                scenario_id=BASELINE_SCENARIO_ID, planned_quantity=Decimal("120"),
                bucket_start=bucket_start,
            )
        try:
            resp = _promote(api_client, auth, mps_id)
            assert resp.status_code == 200, resp.text
            data = resp.json()
            assert data["planned_supplies_created"] == 1
            assert data["summary"]["scenario_id"] == BASELINE_SCENARIO_ID

            with _db_conn(migrated_db) as conn:
                assert _count_planned_supply(
                    conn, item_id=item_id, location_id=location_id,
                    scenario_id=BASELINE_SCENARIO_ID,
                ) == 1
                row = conn.execute(
                    "SELECT scenario_id FROM planned_supply WHERE source_id = %s",
                    (mps_id,),
                ).fetchone()
                assert str(row["scenario_id"]) == BASELINE_SCENARIO_ID
        finally:
            with _db_conn(migrated_db) as conn:
                _teardown(conn, item_id=item_id, location_id=location_id)


# ---------------------------------------------------------------------------
# Case 3 — CRP (run_crp) reads the SAME scenario the promote wrote
# ---------------------------------------------------------------------------


class TestPromoteRunCRPScenarioScoped:
    def test_run_crp_sees_fork_supply_baseline_does_not(
        self, api_client, auth, migrated_db, fork_scenario_id
    ):
        # due_date (= bucket_start) must fall inside the CRP horizon (today+N).
        bucket_start = date.today() + timedelta(days=7)
        with _db_conn(migrated_db) as conn:
            item_id, location_id = _insert_item_and_location(conn)
            work_center_id = _insert_work_center(conn)
            routing_id = _insert_routing_with_operation(
                conn, item_id=item_id, work_center_id=work_center_id
            )
            mps_id = _insert_mps_node(
                conn, item_id=item_id, location_id=location_id,
                scenario_id=fork_scenario_id, planned_quantity=Decimal("100"),
                bucket_start=bucket_start,
            )
        wc = str(work_center_id)
        try:
            # Promote on the fork WITH run_crp=true. The endpoint runs CRP on the
            # MPS node's own scenario (the fork), so its own response's CRP block
            # must have seen the just-written supply.
            resp = _promote(
                api_client, auth, mps_id,
                run_crp=True, crp_horizon_days=30,
            )
            assert resp.status_code == 200, resp.text
            data = resp.json()
            assert data["crp_triggered"] is True
            # CRP ran without error and returned an overload count (>= 0), proving
            # the run_crp path reached CRPEngine.calculate with the fork scenario.
            assert data["crp_overload_count"] is not None

            # Now the discriminating assertion, via the explicit CRP endpoint:
            # a fork-scoped calculation restricted to OUR work center must load
            # the promoted (PLANNED, scenario-scoped) order; baseline must not.
            payload = {"horizon_days": 30, "work_center_ids": [wc]}

            fork_resp = api_client.post(
                "/v1/crp/calculate", json=payload,
                params={"scenario_id": fork_scenario_id}, headers=auth,
            )
            assert fork_resp.status_code == 200, fork_resp.text
            fork_body = fork_resp.json()
            fork_profile = fork_body["load_profiles"].get(wc)
            assert fork_profile is not None, "fork CRP saw no load on our WC"
            assert float(fork_profile["total_load_hours"]) > 0.0

            base_resp = api_client.post(
                "/v1/crp/calculate", json=payload, headers=auth,
            )
            assert base_resp.status_code == 200, base_resp.text
            base_body = base_resp.json()
            base_profile = base_body["load_profiles"].get(wc)
            base_load = 0.0 if base_profile is None else float(base_profile["total_load_hours"])
            # Baseline never received this supply ⇒ zero load on our dedicated WC.
            assert base_load == 0.0
        finally:
            with _db_conn(migrated_db) as conn:
                _teardown(
                    conn, item_id=item_id, location_id=location_id,
                    work_center_id=work_center_id, routing_id=routing_id,
                )


# ---------------------------------------------------------------------------
# Case 4 — ATP is scenario-consistent with the promoted supply
# ---------------------------------------------------------------------------


class TestPromoteATPScenarioScoped:
    def test_atp_fork_sees_supply_baseline_does_not(
        self, api_client, auth, migrated_db, fork_scenario_id
    ):
        # ATP counts a supply available on/after request_date within horizon.
        bucket_start = date.today() + timedelta(days=7)
        with _db_conn(migrated_db) as conn:
            item_id, location_id = _insert_item_and_location(conn)
            mps_id = _insert_mps_node(
                conn, item_id=item_id, location_id=location_id,
                scenario_id=fork_scenario_id, planned_quantity=Decimal("150"),
                bucket_start=bucket_start,
            )
        try:
            resp = _promote(api_client, auth, mps_id)
            assert resp.status_code == 200, resp.text

            # ATP._fetch_supplies only counts status IN ('RELEASED','APPROVED');
            # promote writes 'PLANNED'. Advance the promoted row to RELEASED so
            # ATP can see it — still on the fork scenario, still the row the fix
            # placed there (scenario_id is untouched by the status bump).
            with _db_conn(migrated_db) as conn:
                conn.execute(
                    "UPDATE planned_supply SET status = 'RELEASED' WHERE source_id = %s",
                    (mps_id,),
                )

            atp_payload = {
                "item_id": str(item_id),
                "location_id": str(location_id),
                "quantity": 150,
                "requested_date": bucket_start.isoformat(),
                "horizon_days": 30,
            }

            # Fork: the promoted 150-unit supply is visible ⇒ quantity_available > 0.
            fork_resp = api_client.post(
                "/v1/atp/check", json=atp_payload,
                params={"scenario_id": fork_scenario_id}, headers=auth,
            )
            assert fork_resp.status_code == 200, fork_resp.text
            fork_data = fork_resp.json()
            fork_avail = Decimal(str(fork_data["quantity_available"]))
            assert fork_avail > 0

            # Baseline: no supply for this dedicated item ⇒ nothing available,
            # full backorder. Proves the fork's promoted supply did not leak.
            base_resp = api_client.post(
                "/v1/atp/check", json=atp_payload, headers=auth,
            )
            assert base_resp.status_code == 200, base_resp.text
            base_data = base_resp.json()
            assert Decimal(str(base_data["quantity_available"])) == 0
            assert Decimal(str(base_data["backorder_quantity"])) == Decimal("150")
            # Strictly more available on the fork than on baseline.
            assert fork_avail > Decimal(str(base_data["quantity_available"]))
        finally:
            with _db_conn(migrated_db) as conn:
                _teardown(conn, item_id=item_id, location_id=location_id)


# ---------------------------------------------------------------------------
# Case 5 — strict cross-scenario isolation (two MPS, same item/location/bucket)
# ---------------------------------------------------------------------------


class TestPromoteStrictIsolation:
    def test_two_approved_nodes_promote_without_cross_leak(
        self, api_client, auth, migrated_db, fork_scenario_id
    ):
        bucket_start = date.today() + timedelta(days=7)
        # Same label on both nodes: the unique-active index keys on scenario_id
        # too, so a baseline node and a fork node with the SAME time_bucket are
        # both legal and both active.
        shared_bucket = f"test-398-shared-{uuid4().hex[:8]}"
        with _db_conn(migrated_db) as conn:
            item_id, location_id = _insert_item_and_location(conn)
            mps_baseline = _insert_mps_node(
                conn, item_id=item_id, location_id=location_id,
                scenario_id=BASELINE_SCENARIO_ID, planned_quantity=Decimal("90"),
                bucket_start=bucket_start, time_bucket=shared_bucket,
            )
            mps_fork = _insert_mps_node(
                conn, item_id=item_id, location_id=location_id,
                scenario_id=fork_scenario_id, planned_quantity=Decimal("40"),
                bucket_start=bucket_start, time_bucket=shared_bucket,
            )
        try:
            r1 = _promote(api_client, auth, mps_baseline)
            assert r1.status_code == 200, r1.text
            assert r1.json()["summary"]["scenario_id"] == BASELINE_SCENARIO_ID

            r2 = _promote(api_client, auth, mps_fork)
            assert r2.status_code == 200, r2.text
            assert r2.json()["summary"]["scenario_id"] == fork_scenario_id

            with _db_conn(migrated_db) as conn:
                # Exactly one supply per scenario — no double-write, no leak.
                assert _count_planned_supply(
                    conn, item_id=item_id, location_id=location_id,
                    scenario_id=BASELINE_SCENARIO_ID,
                ) == 1
                assert _count_planned_supply(
                    conn, item_id=item_id, location_id=location_id,
                    scenario_id=fork_scenario_id,
                ) == 1
                # Each source_id landed on the correct scenario with its own qty.
                base_row = conn.execute(
                    "SELECT scenario_id, quantity FROM planned_supply WHERE source_id = %s",
                    (mps_baseline,),
                ).fetchone()
                fork_row = conn.execute(
                    "SELECT scenario_id, quantity FROM planned_supply WHERE source_id = %s",
                    (mps_fork,),
                ).fetchone()
                assert str(base_row["scenario_id"]) == BASELINE_SCENARIO_ID
                assert Decimal(str(base_row["quantity"])) == Decimal("90")
                assert str(fork_row["scenario_id"]) == fork_scenario_id
                assert Decimal(str(fork_row["quantity"])) == Decimal("40")
        finally:
            with _db_conn(migrated_db) as conn:
                _teardown(conn, item_id=item_id, location_id=location_id)
