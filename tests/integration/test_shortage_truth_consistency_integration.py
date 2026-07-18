"""
Consistency guard between the TWO shortage truths (#343, ADR-021).

Ootils computes "is this item short?" in two places:

- **Truth A — kernel** : the propagation engine recomputes ProjectedInventory
  buckets and persists rows in the ``shortages`` table
  (``ShortageDetector`` / ``SHORTAGES_SQL``). It flags EVERY breached bucket,
  per (item, location) DAILY bucket, in BOTH severity classes
  (``stockout`` = closing < 0 and ``below_safety_stock`` = closing < safety).
  This is the canonical persistence/query system (deterministic UUIDs,
  $-valued severity since #342, causal chain ADR-004, ``/v1/issues``).
- **Truth B — watchers** : ``ootils_core.engine.mrp`` (``loader.py`` +
  ``core.py``) — the canonical MRP math (ADR-020). ``first_shortage`` is a
  read-only in-memory projection on CONSUMED demand that reports only the
  FIRST weekly bucket where the item-pooled projected balance drops below
  safety stock, and only for items carrying independent demand.

Contract asserted here: **INCLUSION, not equality — items(B) ⊆ items(A)**.

Why inclusion is the right contract (documented tolerance):

- A is strictly broader in *event* scope: it reports every breached bucket
  and both severity classes; B stops at the first below-safety bucket per
  item. So at item level A must see everything B sees.
- Equality can NOT hold structurally, in either direction:
  * A may legitimately contain items absent from B — items with no
    independent demand on the horizon (B only projects items with consumed
    demand), or per-location breaches masked by B's item-level pooling.
  * A now NETS customer orders against forecast just like B does
    (2026-07-17 PR modélisation, ADR-021 convergence: PROPAGATE_SQL's
    ``GREATEST(fc_out, co_out) + dep_out`` /
    ``propagator.py:_recompute_pi_node``'s ``max(fc, co) + dep`` mirror
    ``core.py:338``'s ``v = max(o, f)`` — A is aligned on B, core.py
    untouched). The inclusion SURVIVES the netting because the grains
    differ in A's favour: A nets at the FINE grain (per daily bucket, per
    location) while B nets on the POOLED item-level weekly series, and
    Σ max(f_i, o_i) ≥ max(Σ f_i, Σ o_i) — the fine-grain netted demand
    over any window is always ≥ the pooled netted demand, which keeps A's
    projected stock at or below B's. B additionally applies its demand
    time fence (+ consumption window), NOT replicated in A — that only
    further reduces B's demand, same direction: A stays structurally ≥ B.

Possible sources of FALSE NEGATIVES if this inclusion ever breaks (i.e. an
item short for B but absent from A) — each is a real bug, not tolerance:

- horizon drift: B is pinned to HORIZON_DAYS = 90 to match the seeded PI
  horizon (scripts/seed_demo_data.py builds 90 daily buckets). A wider B
  horizon would let B see breaches beyond A's projection and void the
  contract — do not widen it without widening the seed.
- demand-wiring drift: B reads demand nodes directly by (scenario, type);
  A only sees demand wired to PI buckets via ``consumes`` edges. A demand
  node inserted without its edge is invisible to A but visible to B — the
  exact "two truths diverge" failure this test exists to catch.
- safety-stock source drift: both read ``item_planning_params``, but A
  resolves per (item, location) with an item-level fallback while B SUMs
  per item. A param row change that splits those views shows up here.

Any divergence fails loudly with the item-level diff (external ids + B's
first-shortage details).

Fixture pattern mirrors tests/integration/test_m6_api_integration.py
(module-scoped seed via scripts/seed_demo_data.py); the Truth-A propagation
trigger mirrors tests/integration/test_seed.py::
test_16b_seed_full_recompute_rebuilds_shortages (POST /v1/calc/run with
full_recompute=True, then read the ``shortages`` table).
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from uuid import UUID

import pytest

from .conftest import requires_db, TEST_DB_URL

pytestmark = requires_db

SEED_SCRIPT = Path(__file__).parents[2] / "scripts" / "seed_demo_data.py"
AUTH_HEADERS = {"Authorization": "Bearer integration-test-token"}
BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")

# Pinned to the seeded ProjectedInventory horizon (seed_demo_data.py: 90 daily
# buckets per series). Note: mrp_core buckets weekly with ceil(90/7)+1 = 14
# weeks, so B actually scans up to day ~98 — the ~8-day overhang beyond A's
# projection is harmless on this seed (both shortages sit at bucket 0) but is
# why the contract is inclusion, not equality. Widening B's horizon further
# beyond A's projection horizon voids the contract.
HORIZON_DAYS = 90


def _run_seed():
    env = {**os.environ, "DATABASE_URL": TEST_DB_URL}
    return subprocess.run(
        [sys.executable, str(SEED_SCRIPT)],
        capture_output=True, text=True, env=env,
    )


# ---------------------------------------------------------------------------
# Shared module-scoped fixtures (mirror tests/integration/test_m6_api_integration.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def seeded_db(migrated_db):
    """Module-scoped: migrated DB with seed data loaded once."""
    result = _run_seed()
    if result.returncode != 0:
        pytest.skip(f"Seed failed: {result.stderr[:500]}")
    return migrated_db


@pytest.fixture(scope="module")
def api_client(seeded_db):
    """Module-scoped FastAPI TestClient bound to the real test DB."""
    os.environ["DATABASE_URL"] = seeded_db
    os.environ["OOTILS_API_TOKEN"] = "integration-test-token"

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB
    from fastapi.testclient import TestClient

    app = create_app()

    def override_db():
        db = OotilsDB(seeded_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Truth fixtures — each computed once per module
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def watcher_truth(seeded_db):
    """Truth B: mrp_core read-only projection — (first_shortage map, PlanningData).

    Same call path as the watcher fleet (scripts/agent_shortage_watcher.py):
    load_planning_data → consume_demand → first_shortage. Read-only, in-memory.
    Plain tuple-row connection: the loader indexes rows positionally.
    """
    import psycopg

    from ootils_core.engine.mrp.core import consume_demand, first_shortage
    from ootils_core.engine.mrp.loader import load_planning_data

    with psycopg.connect(seeded_db) as conn:
        data = load_planning_data(conn, horizon_days=HORIZON_DAYS)
    gross = consume_demand(data)
    return first_shortage(data, gross), data


@pytest.fixture(scope="module")
def kernel_truth(api_client, seeded_db):
    """Truth A: full baseline propagation, then the active ``shortages`` rows.

    POST /v1/calc/run full_recompute=True marks every baseline PI node dirty,
    recomputes them and persists shortages. The seed pre-inserts 'active'
    shortage rows for exactly the items Truth B flags (PUMP-01/VALVE-02)
    under a placeholder calc_run — relying on resolve_stale to retire them
    would mask a broken kernel detection if resolve_stale itself failed
    silently (its exception is swallowed with a warning in the propagator).
    So, like test_seed.py::test_16b, we DELETE the pre-seeded rows up front
    and additionally assert every row we read back belongs to THIS calc run.
    """
    import psycopg
    from psycopg.rows import dict_row
    from uuid import UUID as _UUID

    from ootils_core.engine.kernel.shortage.detector import ShortageDetector

    with psycopg.connect(seeded_db) as conn:
        conn.execute(
            "DELETE FROM shortages WHERE scenario_id = %s",
            (BASELINE_SCENARIO_ID,),
        )
        conn.commit()

    resp = api_client.post(
        "/v1/calc/run",
        json={"full_recompute": True},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200, f"Calc run failed: {resp.text}"
    calc_data = resp.json()
    assert calc_data["status"] == "completed", calc_data
    assert calc_data["nodes_recalculated"] > 0, calc_data
    run_id = _UUID(str(calc_data["calc_run_id"]))

    detector = ShortageDetector()
    with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
        shortages = detector.get_active_shortages(BASELINE_SCENARIO_ID, conn)

    # Self-verifying claim: everything active was produced by THIS run —
    # no seed leftovers, no stale rows from an earlier module.
    foreign = [s for s in shortages if s.calc_run_id != run_id]
    assert not foreign, (
        f"{len(foreign)} active shortage row(s) do not belong to the "
        f"triggering calc run {run_id} — stale/seed rows are polluting Truth A"
    )
    return shortages


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_seed_produces_shortages_in_both_truths(watcher_truth, kernel_truth):
    """Anti-false-green guard (pattern of the #332 parity guard): an empty
    truth on the seeded dataset means the harness is broken, never 'no
    shortage'. The seed is built to be short on both items:

    - PUMP-01: on-hand 30 < safety 50, week-0 consumed demand 105 → below
      safety at bucket 0.
    - VALVE-02: on-hand 45, safety 30, week-0 consumed demand 56 → balance
      -11 < 30 at bucket 0.
    """
    first, data = watcher_truth
    assert first, (
        "Truth B (mrp_core.first_shortage) found no shortage on the seeded "
        "dataset — seed or loader is broken, refusing a vacuous inclusion."
    )
    b_names = {data.names.get(i, str(i)) for i in first}
    assert {"PUMP-01", "VALVE-02"} <= b_names, (
        f"Truth B missed a seeded shortage item: got {sorted(b_names)}"
    )

    assert kernel_truth, (
        "Truth A (shortages table after full recompute) is empty on the "
        "seeded dataset — propagation or detection is broken."
    )


def test_watcher_shortage_items_subset_of_kernel_shortages(watcher_truth, kernel_truth):
    """items(B) ⊆ items(A): every item the watcher fleet calls short must
    exist in the kernel's ``shortages`` table (see module docstring for why
    inclusion — not equality — is the contract). Any divergence is a loud
    failure carrying the item diff.
    """
    first, data = watcher_truth
    b_items = set(first.keys())
    a_items = {s.item_id for s in kernel_truth if s.item_id is not None}

    missing = b_items - a_items
    if missing:
        diff = {
            data.names.get(item, str(item)): {
                "first_shortage_bucket": first[item]["bucket"],
                "date": str(first[item]["date"]),
                "deficit": round(first[item]["deficit"], 2),
                "balance": round(first[item]["balance"], 2),
            }
            for item in sorted(missing, key=lambda i: data.names.get(i, str(i)))
        }
        pytest.fail(
            "Shortage truths diverged: items short for the watchers "
            "(mrp_core.first_shortage) but ABSENT from the kernel shortages "
            f"table after a full baseline recompute: {diff}. "
            f"Truth A items: {sorted(data.names.get(i, str(i)) for i in a_items)}. "
            "Either a demand node is not wired to its PI bucket (kernel blind "
            "spot) or the horizons/params drifted — see ADR-021."
        )
