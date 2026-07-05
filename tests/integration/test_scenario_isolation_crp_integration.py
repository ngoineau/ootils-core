"""
Scenario-isolation integration test for CRP (#396).

Before the fix, ``CRPEngine._fetch_planned_orders`` fell back to an
*unfiltered* ``planned_supply`` query whenever ``scenario_id`` was ``None``
(and the ``/v1/crp/*`` endpoints never resolved a scenario at all). A fork that
wrote planned supply therefore contaminated the baseline CRP load, and vice-
versa. CRP now always scenario-scopes its planned-order read and every
``/v1/crp/*`` endpoint resolves ``scenario_id`` (query param or
``X-Scenario-ID`` header, default baseline) — matching ATP (#…) and RCCP (#338).

This test seeds a baseline dataset plus a fork scenario with an *extra*
planned supply and asserts:

  (a) the baseline response counts / loads only the baseline order (the fork's
      order is invisible);
  (b) the response with ``?scenario_id=<fork>`` (or ``X-Scenario-ID`` header)
      reflects the fork order only;
  (c) reading the fork does not contaminate a subsequent baseline read.

Mechanics (fixtures, seeding shape, dict_row-by-name access, CURRENT_DATE-
relative dates, per-test teardown) are copied from
``tests/integration/test_scenario_isolation_atp_rccp_integration.py`` — a
passing pattern. Runs against a real PostgreSQL database (no mocks) on a fresh
migrated DB; every row is created and cleaned up locally.
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
# Shared module-scoped fixtures (mirror the ATP/RCCP isolation module)
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
            (scenario_id, "crp-isolation-fork", BASELINE_SCENARIO_ID),
        )
        conn.commit()
    return str(scenario_id)


# ---------------------------------------------------------------------------
# Seeding helpers
# ---------------------------------------------------------------------------


def _insert_item_and_location(conn) -> tuple[UUID, UUID]:
    """Create a unique item + location, return their ids."""
    item_id = uuid4()
    location_id = uuid4()
    conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s)",
        (item_id, f"CRP Iso Item {item_id}"),
    )
    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
        (location_id, f"CRP Iso Loc {location_id}"),
    )
    return item_id, location_id


def _insert_work_center(conn) -> UUID:
    """Insert a work-center-flavoured row in the unified ``resources`` table.

    Master data — NOT scenario-scoped (shared by baseline and the fork).
    """
    wc_id = uuid4()
    conn.execute(
        """
        INSERT INTO resources (
            resource_id, external_id, name, resource_type,
            capacity_per_day, capacity_unit, efficiency, active
        ) VALUES (%s, %s, 'CRP Iso WC', 'work_center', 80, 'unit', 1.0, TRUE)
        """,
        (wc_id, f"WC-CRP-ISO-{wc_id.hex[:8]}"),
    )
    return wc_id


def _insert_routing_with_operation(conn, *, item_id: UUID, work_center_id: UUID) -> tuple[UUID, UUID]:
    """Insert a routing + a single operation on the work center (master data)."""
    routing_id = uuid4()
    operation_id = uuid4()
    conn.execute(
        """
        INSERT INTO routings (routing_id, item_id, sequence, description, active)
        VALUES (%s, %s, 1, 'CRP Iso routing', TRUE)
        """,
        (routing_id, item_id),
    )
    conn.execute(
        """
        INSERT INTO routing_operations (
            operation_id, routing_id, sequence, resource_id,
            setup_time, run_time_per_unit, description, active
        ) VALUES (%s, %s, 10, %s, 0, 0.5, 'CRP Iso op', TRUE)
        """,
        (operation_id, routing_id, work_center_id),
    )
    return routing_id, operation_id


def _seed_planned_supply(
    conn, *, item_id, location_id, quantity, due_date, scenario_id=BASELINE_SCENARIO_ID
) -> UUID:
    ps_id = uuid4()
    conn.execute(
        """
        INSERT INTO planned_supply
            (planned_supply_id, item_id, location_id, scenario_id, quantity, due_date, status, priority)
        VALUES (%s, %s, %s, %s::UUID, %s, %s, 'RELEASED', 0)
        """,
        (ps_id, item_id, location_id, scenario_id, quantity, due_date),
    )
    return ps_id


def _teardown_crp_rows(conn, *, item_id, location_id, work_center_id, routing_id) -> None:
    """Delete every CRP row written for this item/location/work center."""
    conn.execute("DELETE FROM routing_operations WHERE routing_id = %s", (routing_id,))
    conn.execute("DELETE FROM routings WHERE routing_id = %s", (routing_id,))
    conn.execute("DELETE FROM planned_supply WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM resources WHERE resource_id = %s", (work_center_id,))
    conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))


def _stable(body: dict) -> dict:
    """Strip per-call noise so two identical calculations compare equal.

    ``calculation_id`` is a fresh uuid4 on every call and ``calculation_time_ms``
    is timing — both differ run-to-run even for an identical order set.
    """
    return {
        k: v for k, v in body.items()
        if k not in ("calculation_time_ms", "calculation_id")
    }


def _total_load(body: dict, work_center_id: str) -> float:
    """Sum the load across the buckets of one work center's profile (0.0 if absent)."""
    profile = body["load_profiles"].get(work_center_id)
    if profile is None:
        return 0.0
    return float(profile["total_load_hours"])


# ---------------------------------------------------------------------------
# CRP — baseline vs fork isolation
# ---------------------------------------------------------------------------


class TestCRPScenarioIsolation:
    """POST /v1/crp/calculate planned-order aggregation must be scenario-scoped."""

    def _seed(self, migrated_db, fork_scenario_id):
        """Baseline: WC + routing + PlannedSupply 100. Fork: +PlannedSupply 40 (fork-only).

        Both orders are due inside the CRP horizon (CURRENT_DATE-relative). The
        work center, routing and operation are shared master data — the ONLY
        scenario-scoped row is ``planned_supply``.
        """
        import psycopg
        from psycopg.rows import dict_row

        due_date = date.today() + timedelta(days=7)
        with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
            item_id, location_id = _insert_item_and_location(conn)
            work_center_id = _insert_work_center(conn)
            routing_id, _op_id = _insert_routing_with_operation(
                conn, item_id=item_id, work_center_id=work_center_id
            )
            # Baseline order (must NOT leak into the fork answer)
            _seed_planned_supply(
                conn, item_id=item_id, location_id=location_id,
                quantity=Decimal("100"), due_date=due_date,
            )
            # Fork-only order (must NOT leak into the baseline answer)
            _seed_planned_supply(
                conn, item_id=item_id, location_id=location_id,
                quantity=Decimal("40"), due_date=due_date,
                scenario_id=fork_scenario_id,
            )
            conn.commit()
        return item_id, location_id, work_center_id, routing_id

    def test_crp_baseline_ignores_fork_and_fork_sees_own_rows(
        self, api_client, auth, migrated_db, fork_scenario_id
    ):
        import psycopg
        from psycopg.rows import dict_row

        item_id, location_id, work_center_id, routing_id = self._seed(
            migrated_db, fork_scenario_id
        )
        wc = str(work_center_id)
        payload = {"horizon_days": 30, "work_center_ids": [wc]}
        try:
            # (a) Baseline (no scenario param): only the baseline order (qty 100)
            # is counted; the fork's qty-40 order must be invisible.
            resp = api_client.post("/v1/crp/calculate", json=payload, headers=auth)
            assert resp.status_code == 200, resp.text
            baseline = resp.json()
            assert baseline["planned_orders_count"] == 1
            baseline_load = _total_load(baseline, wc)
            assert baseline_load > 0.0

            # (b) Fork: only the fork order (qty 40) is counted; the baseline
            # qty-100 order must be invisible. Different qty ⇒ different load.
            resp = api_client.post(
                "/v1/crp/calculate", json=payload,
                params={"scenario_id": fork_scenario_id}, headers=auth,
            )
            assert resp.status_code == 200, resp.text
            fork = resp.json()
            assert fork["planned_orders_count"] == 1
            fork_load = _total_load(fork, wc)
            assert fork_load > 0.0
            # The fork order is smaller (40 < 100) ⇒ strictly less load. This
            # proves the two scenarios are computed from disjoint order sets.
            assert fork_load < baseline_load

            # (c) X-Scenario-ID header is equivalent to the query param
            # (calculation_time_ms is timing noise — excluded from comparison).
            resp = api_client.post(
                "/v1/crp/calculate", json=payload,
                headers={**auth, "X-Scenario-ID": fork_scenario_id},
            )
            assert resp.status_code == 200, resp.text
            assert _stable(resp.json()) == _stable(fork)

            # (d) Baseline answer is unchanged after fork reads (no contamination).
            resp = api_client.post("/v1/crp/calculate", json=payload, headers=auth)
            assert resp.status_code == 200, resp.text
            assert _stable(resp.json()) == _stable(baseline)
        finally:
            with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
                _teardown_crp_rows(
                    conn, item_id=item_id, location_id=location_id,
                    work_center_id=work_center_id, routing_id=routing_id,
                )
                conn.commit()

    def test_crp_overloads_endpoint_is_scenario_scoped(
        self, api_client, auth, migrated_db, fork_scenario_id
    ):
        """GET /v1/crp/overloads must count overloads from its own scenario only.

        A bare 200-on-both-sides assertion has no teeth: FastAPI ignores an
        unknown query param, and the ``_seed()`` loads (50 h baseline / 20 h
        fork, capacity 80 h/day) never cross the capacity ceiling, so both
        sides report zero overloads whether or not the scenario filter is
        applied. This test forces an overload on the baseline side only, and
        asserts the *counts* — a revert of the CRP scenario-scoping fix (i.e.
        the fork read falling back to seeing the unfiltered baseline order)
        would make the fork side see the overload too, and the assertion
        below would fail.

        Arithmetic, verified against CRPEngine.calculate() (read the code,
        not assumed):
          - capacity_per_day=80, efficiency=1.0 => effective capacity 80 h/day.
          - Operation: setup_time=0, run_time_per_unit=0.5.
          - A SINGLE large order does NOT work here: CRPEngine spreads any one
            order's total_hours evenly across days_needed=ceil(total_hours /
            80) days (engine.py calculate(), the "Distribute load across the
            days" block) — hours_per_day = total_hours / days_needed is, by
            that construction, always <= capacity (e.g. qty=1000 => 500 h =>
            days_needed=ceil(500/80)=7 => ~71.43 h/day < 80 h/day => NO
            overload, however large the single order's quantity).
          - Overload therefore requires several orders whose per-order load
            lands on the SAME day. Each extra order below has
            quantity=100 => total_hours=50 h => days_needed=ceil(50/80)=1
            (single-day, no spreading ambiguity), due on the same due_date as
            the existing baseline order (also 50 h, from ``_seed()``).
            10 extra orders + the existing one: 11 x 50 h = 550 h on that one
            day versus an 80 h/day ceiling => excess ~470 h (~6.9x capacity) —
            a massive, arithmetic-detail-insensitive margin.
          - The fork side is untouched: its lone 40-qty order (20 h) stays
            far under 80 h/day => 0 overloads, both before and after the
            baseline-side seeding above.
        """
        import psycopg
        from psycopg.rows import dict_row

        item_id, location_id, work_center_id, routing_id = self._seed(
            migrated_db, fork_scenario_id
        )
        wc = str(work_center_id)
        due_date = date.today() + timedelta(days=7)  # matches _seed()'s due_date
        try:
            with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
                for _ in range(10):
                    _seed_planned_supply(
                        conn, item_id=item_id, location_id=location_id,
                        quantity=Decimal("100"), due_date=due_date,
                    )
                conn.commit()

            params_base = {"horizon_days": 30, "work_center_ids": wc}

            # Baseline: 11 orders (550 h) on one day vs. 80 h/day capacity —
            # at least one overload must be reported.
            resp = api_client.get("/v1/crp/overloads", params=params_base, headers=auth)
            assert resp.status_code == 200, resp.text
            baseline_overloads = resp.json()["overloads"]
            assert len(baseline_overloads) >= 1
            assert any(o["work_center_id"] == wc for o in baseline_overloads)

            # Fork: only its own 20 h order — no overload. Pre-fix (unfiltered
            # planned_supply fallback), the fork read would also see the
            # baseline's 550 h order and report >= 1 overload here.
            resp = api_client.get(
                "/v1/crp/overloads",
                params={**params_base, "scenario_id": fork_scenario_id},
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            fork_overloads = resp.json()["overloads"]
            assert len(fork_overloads) == 0
        finally:
            with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
                # _teardown_crp_rows deletes planned_supply by item_id, which
                # covers the 10 extra orders seeded above (same item_id).
                _teardown_crp_rows(
                    conn, item_id=item_id, location_id=location_id,
                    work_center_id=work_center_id, routing_id=routing_id,
                )
                conn.commit()

    def test_crp_invalid_scenario_id_is_422(self, api_client, auth):
        resp = api_client.post(
            "/v1/crp/calculate",
            json={"horizon_days": 30},
            params={"scenario_id": "not-a-uuid"},
            headers=auth,
        )
        assert resp.status_code == 422
