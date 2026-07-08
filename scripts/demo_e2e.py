#!/usr/bin/env python3
"""
demo_e2e.py — the executable wedge runbook (#408).

Drives the whole "autonomous shortage control tower with scenario-backed
recommendations" wedge end to end, in-process, against a real PostgreSQL
database, printing a narrative an operator can read out loud in front of a
prospect. It is the executable twin of docs/DEMO-RUNBOOK.md.

It assembles EXISTING product surfaces only — zero new mechanics: the same
FastAPI routers a client would call over HTTP (mounted on an in-process
TestClient bound to the target DSN), plus the same in-process agent entry
points the fleet uses (agent_shortage_watcher.main). Nothing here computes
supply-chain truth of its own; every number comes from the product.

Runs on TWO databases:
  * the PILOTE base (36k+ items, real demand_history) — the live demo, and
  * the CI-seeded base (scripts/seed_demo_data.py: VALVE-02 shortage,
    XFER-01 DC-ATL -> DC-LAX transfer opportunity) — the automated check.
Each step self-detects what it can do and reports PASS / SKIP(reason) / FAIL.

NON-DESTRUCTIVE, ABSOLUTE: no DELETE / TRUNCATE / destructive UPDATE, ever.
Only the product's own normal writes happen — mint two demo tokens, ensure one
distribution link, emit DRAFT recommendations, capture snapshots, evaluate
outcomes, fork + archive one what-if scenario. Every discovery query is a
read-only, parameterized SELECT. The DSN is NEVER printed (masked in every log
line and in the scoreboard artefact).

Idempotent: re-running duplicates nothing meaningful — tokens are reused by
name, the demo link upserts ON CONFLICT, recommendations are uuid5-keyed,
snapshots upsert, outcomes upsert, the fork is archived (never deleted).

print() is intentional here (this is a narrative CLI, the scripts/ house
pattern), not a logging violation.

Usage:
    OOTILS_API_TOKEN=... python scripts/demo_e2e.py --dsn postgresql://.../ootils_pilote_test
    OOTILS_API_TOKEN=... python scripts/demo_e2e.py --dsn ... --skip-watchers --bench
    OOTILS_API_TOKEN=... python scripts/demo_e2e.py --dsn ... --out scoreboard.json --show-tokens
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import subprocess
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional
from uuid import UUID

# The watchers (and their mrp_core / agent_simulation deps) do a bare
# ``import mrp_core``; scripts/ must be on sys.path for the in-process watcher
# call in step 3. This file lives IN scripts/, but make the dependency explicit
# so the module is importable from a test harness that adds src/ but not
# scripts/ to the path.
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"

# Token names are STABLE so a re-run reuses the same api_tokens rows instead of
# minting new ones (idempotence by name). Prefixed DEMO-E2E- so an operator can
# find and revoke everything this script created with one query.
_AGENT_TOKEN_NAME = "DEMO-E2E-agent"
_HUMAN_TOKEN_NAME = "DEMO-E2E-human"
# AN-2 (#392, PR2a) scope enforcement is now live end-to-end, so these grants
# must cover every call each token makes:
#   * AGENT — reads (step1/step8) + drafts (step4 DRP run is recommend:draft;
#     step5 DRAFT->REVIEWED is recommend:draft). It deliberately does NOT hold
#     recommend:approve, so step5's REVIEWED->APPROVED is refused (403) — the
#     whole point of the governance gate demo. No calc:run: no agent step
#     launches a run through the API (the watchers in step3 are DB-direct).
#   * HUMAN — the full operator superset (all scopes except `admin`): the
#     forecast run (step2) is calc:run; simulate / param-overrides / archive
#     (step7) and scenario delete are scenario:write; snapshots + outcomes
#     evaluate (step6) are ingest; the L3 approval (step5) is recommend:approve.
_AGENT_SCOPES = ["read", "recommend:draft"]
_HUMAN_SCOPES = [
    "read",
    "ingest",
    "calc:run",
    "graph:write",
    "scenario:write",
    "recommend:draft",
    "recommend:approve",
]

# The dedicated demo distribution link (step 4) is tagged so it is
# identifiable and idempotent. Name only — the row itself is keyed on its own
# deterministic id.
_DEMO_LINK_TAG = "DEMO-E2E"


# ---------------------------------------------------------------------------
# Presentation helpers (narrative CLI — print() is intentional)
# ---------------------------------------------------------------------------

PASS = "PASS"
SKIP = "SKIP"
FAIL = "FAIL"

_WIDTH = 92


@dataclass
class StepResult:
    """One demo step's verdict, plus a compact machine-readable payload."""

    number: int
    title: str
    status: str
    detail: str = ""
    data: dict[str, Any] = field(default_factory=dict)


def mask_dsn(dsn: str) -> str:
    """Return a display-safe DSN: the database NAME only, credentials/host
    stripped. The full DSN is NEVER printed or written to the artefact.

    ``postgresql://user:pass@host:5432/ootils_pilote_test?sslmode=require``
    -> ``db=ootils_pilote_test``. A bare name (no slashes) maps to itself.
    """
    tail = dsn.rstrip("/").split("/")[-1]
    name = tail.split("?")[0].strip()
    return f"db={name}" if name else "db=<unknown>"


def _banner(text: str) -> None:
    print("=" * _WIDTH)
    print(text)
    print("=" * _WIDTH)


def _step_header(number: int, title: str) -> None:
    print()
    print("-" * _WIDTH)
    print(f"STEP {number} — {title}")
    print("-" * _WIDTH)


def _verdict_line(result: StepResult) -> str:
    tag = {PASS: "[PASS]", SKIP: "[SKIP]", FAIL: "[FAIL]"}[result.status]
    detail = f"  {result.detail}" if result.detail else ""
    return f"{tag} step {result.number} ({result.title}){detail}"


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------


@dataclass
class DemoContext:
    """Everything the steps share. ``client`` is the in-process TestClient
    bound to the target DSN; ``dsn`` is kept only to hand to the in-process
    watcher and bench (never printed)."""

    dsn: str
    client: Any  # fastapi.testclient.TestClient
    verbose: bool
    skip_watchers: bool
    run_bench: bool
    show_tokens: bool
    # What-if base scenario for step 7 (default baseline; a fork enables the
    # pilot fork-on-fork counter-factual — #414). The literal 'baseline' is a
    # legal value the simulate router resolves to the sentinel UUID.
    whatif_base_scenario: str = BASELINE_SCENARIO_ID
    # Populated as steps run.
    agent_token: Optional[str] = None
    human_token: Optional[str] = None
    forecast_item: Optional[str] = None       # external_id
    forecast_location: Optional[str] = None    # external_id
    forecast_run_id: Optional[str] = None
    drp_item: Optional[str] = None
    drp_scenario_recos: list[str] = field(default_factory=list)
    draft_reco_for_gate: Optional[str] = None
    fork_scenario_id: Optional[str] = None

    def _auth(self, token: Optional[str]) -> dict[str, str]:
        if token is None:
            raise RuntimeError("token requested before it was minted")
        return {"Authorization": f"Bearer {token}"}

    @property
    def agent_auth(self) -> dict[str, str]:
        return self._auth(self.agent_token)

    @property
    def human_auth(self) -> dict[str, str]:
        return self._auth(self.human_token)


# ---------------------------------------------------------------------------
# Direct DB access (read-only discovery + token minting) — its own short-lived
# connections, autocommit, dict_row. Never reuses the app pool.
# ---------------------------------------------------------------------------


def _connect(dsn: str):
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _schema_version(dsn: str) -> Optional[str]:
    """MAX(version) applied — the latest migration filename, or None on a
    fresh/uninitialised DB."""
    with _connect(dsn) as conn:
        row = conn.execute(
            "SELECT MAX(version) AS v FROM schema_migrations"
        ).fetchone()
    return row["v"] if row and row["v"] else None


# ---------------------------------------------------------------------------
# App bootstrap
# ---------------------------------------------------------------------------


def build_client(dsn: str) -> Any:
    """Create the FastAPI app and bind get_db to the target DSN via an
    override (the phase1 / integration-fixture pattern). Migrations were
    already applied by the OotilsDB(dsn) in step 0 before this is called."""
    from fastapi.testclient import TestClient

    # NOTE: the get_db override below only covers ROUTE handlers. The auth
    # layer's minted-token lookup needs DATABASE_URL in the env, which main()
    # sets BEFORE the first ootils_core import (see the comment there) — by
    # the time this function runs it is too late to set it.
    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB

    app = create_app()
    bound_db = OotilsDB(dsn)

    def _override_db():
        with bound_db.conn() as conn:
            yield conn

    app.dependency_overrides[get_db] = _override_db
    return TestClient(app)


# ===========================================================================
# STEP 0 — boot + migration catch-up + read-only inventory
# ===========================================================================


def step0_boot(dsn: str) -> tuple[StepResult, Any]:
    """Apply pending migrations (OotilsDB construction), report the schema
    version before/after, and take a read-only inventory. Returns the step
    result AND the built client (step 0 is the only one that constructs it).

    This is the ONE step whose FAIL aborts the run: without a booted app and a
    reachable DB, nothing downstream is meaningful.
    """
    from ootils_core.db.connection import OotilsDB

    version_before = _schema_version(dsn)
    # Constructing OotilsDB applies every pending migration under its advisory
    # lock (the pilote base sits at 060 and catches up 061-069 here).
    OotilsDB(dsn)
    version_after = _schema_version(dsn)

    client = build_client(dsn)

    inv = _inventory(dsn)
    print(f"  DB                                 : {mask_dsn(dsn)}")
    print(f"  Schema version (before -> after)   : {version_before} -> {version_after}")
    print(f"  Items                              : {inv['items']:,}")
    print(f"  Locations                          : {inv['locations']:,}")
    print(f"  Locations with on-hand             : {inv['locations_with_onhand']:,}")
    print(f"  demand_history rows                : {inv['demand_history']:,}")
    print(f"  Recommendations (all statuses)     : {inv['recommendations']:,}")
    print(f"  Active shortages                   : {inv['active_shortages']:,}")

    caught_up = (
        [] if version_before == version_after else ["migrations applied at boot"]
    )
    detail = (
        f"booted, schema {version_after}"
        + (f" ({caught_up[0]})" if caught_up else " (already current)")
    )
    return (
        StepResult(
            number=0,
            title="Boot & migration catch-up",
            status=PASS,
            detail=detail,
            data={
                "version_before": version_before,
                "version_after": version_after,
                "inventory": inv,
            },
        ),
        client,
    )


def _inventory(dsn: str) -> dict[str, int]:
    """Read-only headline counts. Parameterized where it takes params; all
    static-column aggregates otherwise."""
    with _connect(dsn) as conn:
        items = conn.execute("SELECT COUNT(*) AS n FROM items").fetchone()["n"]
        locations = conn.execute(
            "SELECT COUNT(*) AS n FROM locations"
        ).fetchone()["n"]
        loc_onhand = conn.execute(
            """
            SELECT COUNT(DISTINCT location_id) AS n
            FROM nodes
            WHERE node_type = 'OnHandSupply' AND active AND quantity > 0
            """
        ).fetchone()["n"]
        demand_history = conn.execute(
            "SELECT COUNT(*) AS n FROM demand_history"
        ).fetchone()["n"]
        recos = conn.execute(
            "SELECT COUNT(*) AS n FROM recommendations"
        ).fetchone()["n"]
        shortages = conn.execute(
            "SELECT COUNT(*) AS n FROM shortages WHERE status = 'active'"
        ).fetchone()["n"]
    return {
        "items": int(items),
        "locations": int(locations),
        "locations_with_onhand": int(loc_onhand),
        "demand_history": int(demand_history),
        "recommendations": int(recos),
        "active_shortages": int(shortages),
    }


# ===========================================================================
# STEP 1 — mint two governed tokens (idempotent by name)
# ===========================================================================


def _find_token_id_by_name(dsn: str, name: str) -> Optional[str]:
    with _connect(dsn) as conn:
        row = conn.execute(
            "SELECT token_id FROM api_tokens WHERE name = %s "
            "AND revoked_at IS NULL ORDER BY created_at DESC LIMIT 1",
            (name,),
        ).fetchone()
    return str(row["token_id"]) if row else None


def _mint_token(
    dsn: str, *, name: str, actor_kind: str, scopes: list[str]
) -> tuple[str, str]:
    """Mint one live api_tokens row via the shared helper; return (cleartext,
    token_id).

    Delegates to ``ootils_core.api.token_service.mint_token`` — the SINGLE place
    that knows how a token is generated (256-bit os.urandom), hashed (SHA-256)
    and persisted (#392 AN-2 PR2b). The cleartext exists ONLY in this process;
    the DB stores its hash + prefix. The ``with _connect(dsn)`` block commits on
    exit (psycopg3 connection context manager), so the row is durable before we
    return. Note the argument order swap: mint_token returns (token_id, clear),
    this demo helper keeps its historical (clear, token_id) contract."""
    from ootils_core.api.token_service import mint_token

    with _connect(dsn) as conn:
        token_id, clear = mint_token(
            conn, name=name, actor_kind=actor_kind, scopes=scopes
        )
    return clear, str(token_id)


def _ensure_token(
    dsn: str, *, name: str, actor_kind: str, scopes: list[str]
) -> tuple[str, str, bool]:
    """Return (cleartext, prefix, reused). If a live token of this name
    exists we CANNOT recover its cleartext (only the hash is stored), so we
    mint a FRESH one and let the operator revoke the stale row out-of-band.
    Minting a new secret is the only honest option; the name still makes the
    set findable. ``reused`` is True only when no fresh mint was needed
    (i.e. it never is here — kept for API symmetry / future caching)."""
    existing = _find_token_id_by_name(dsn, name)
    clear, _token_id = _mint_token(
        dsn, name=name, actor_kind=actor_kind, scopes=scopes
    )
    from ootils_core.api.auth import token_prefix

    return clear, token_prefix(clear), existing is not None


def step1_tokens(ctx: DemoContext) -> StepResult:
    """Mint (or refresh) the two DEMO-E2E tokens: an AGENT (read +
    recommend:draft) and a HUMAN (read + ingest + recommend:draft +
    recommend:approve). The agent token cannot cross the L3 human gate — that
    asymmetry is the whole point of step 5. Cleartext is never printed unless
    --show-tokens (operator escape hatch); the non-secret prefix always is."""
    agent_clear, agent_prefix, agent_reused = _ensure_token(
        ctx.dsn, name=_AGENT_TOKEN_NAME, actor_kind="agent", scopes=_AGENT_SCOPES
    )
    human_clear, human_prefix, human_reused = _ensure_token(
        ctx.dsn, name=_HUMAN_TOKEN_NAME, actor_kind="human", scopes=_HUMAN_SCOPES
    )
    ctx.agent_token = agent_clear
    ctx.human_token = human_clear

    print(f"  Agent token   : name={_AGENT_TOKEN_NAME} prefix={agent_prefix} "
          f"scopes={_AGENT_SCOPES}")
    print(f"  Human token   : name={_HUMAN_TOKEN_NAME} prefix={human_prefix} "
          f"scopes={_HUMAN_SCOPES}")
    if ctx.show_tokens:
        print(f"  [--show-tokens] agent={agent_clear}")
        print(f"  [--show-tokens] human={human_clear}")
    else:
        print("  (cleartext hidden; pass --show-tokens to reveal for a manual demo)")

    # Prove the freshly-minted tokens authenticate through the real auth path.
    import ootils_core.api.auth as auth

    auth._token_cache.clear()  # so the just-inserted rows are looked up, not a stale miss
    probe = ctx.client.get("/v1/recommendations?limit=1", headers=ctx.agent_auth)
    if probe.status_code != 200:
        raise RuntimeError(
            f"minted agent token failed auth probe: {probe.status_code} {probe.text}"
        )

    prior = "refreshed 1+ prior row(s)" if (agent_reused or human_reused) else "freshly minted"
    return StepResult(
        number=1,
        title="Governed tokens",
        status=PASS,
        detail=f"agent+human tokens live ({prior}), agent auth probe 200",
        data={"agent_prefix": agent_prefix, "human_prefix": human_prefix},
    )


# ===========================================================================
# STEP 2 — forecast + FVA
# ===========================================================================


def _discover_forecast_series(dsn: str) -> Optional[tuple[str, str, int]]:
    """Find the (item_external_id, location_external_id, n_rows) with the most
    booking-based demand_history rows — the richest series to forecast.

    Mirrors pyramide.repository's booking predicates (stream='regular',
    inter-entity excluded, booked_date present, strict past). Resolves
    dh.warehouse_id to a site via external_id ∪ location_aliases (ADR-031 —
    the run-5 pilot execution proved the bare external_id equality misses
    every aliased ERP code; reverse resolution code→site, DISTINCT on aliases
    because one code may legitimately exist under several source_systems for
    the SAME site and must not fan out the row counts). Read-only. Returns
    None if no exploitable series exists."""
    with _connect(dsn) as conn:
        row = conn.execute(
            """
            SELECT i.external_id AS item_ext,
                   l.external_id AS loc_ext,
                   COUNT(*) AS n
            FROM demand_history dh
            JOIN items i ON i.item_id = dh.item_id
            LEFT JOIN locations ld ON ld.external_id = dh.warehouse_id
            LEFT JOIN (SELECT DISTINCT alias, location_id
                       FROM location_aliases) la ON la.alias = dh.warehouse_id
            JOIN locations l
              ON l.location_id = COALESCE(ld.location_id, la.location_id)
            WHERE dh.stream = 'regular'
              AND (dh.fulfillment IS NULL OR dh.fulfillment <> 'inter_entity')
              AND dh.booked_date IS NOT NULL
              AND dh.booked_date < CURRENT_DATE
            GROUP BY i.external_id, l.external_id
            ORDER BY n DESC, i.external_id, l.external_id
            LIMIT 1
            """
        ).fetchone()
    if row is None or int(row["n"]) == 0:
        return None
    return str(row["item_ext"]), str(row["loc_ext"]), int(row["n"])


def step2_forecast(ctx: DemoContext) -> StepResult:
    """Discover the richest booking-based series, run a Pyramide forecast on it
    (AUTO_SELECT, generous lookback), read the result back, and print WAPE /
    MASE / the REAL FVA (naive_wape - wape) / whether conformal CI bounds are
    present. SKIP honestly if no series is exploitable."""
    series = _discover_forecast_series(ctx.dsn)
    if series is None:
        # Distinguish "empty demand_history" from the pilot-data onboarding gap
        # observed on ootils_pilote_test: demand_history.warehouse_id carries
        # ERP numeric DC codes ('87', '286', ...) that were never mapped to
        # locations.external_id (alpha codes 'DAL', 'CAN', ...) — so the
        # migration-047 join yields zero rows even over millions of bookings.
        # Mapping those codes is a data-onboarding decision for the pilot
        # (🎯 see DEMO-RUNBOOK caveats), not something this demo fabricates.
        with _connect(ctx.dsn) as conn:
            diag = conn.execute(
                """
                SELECT COUNT(*) AS total,
                       COUNT(DISTINCT dh.warehouse_id) AS warehouses,
                       COUNT(DISTINCT dh.warehouse_id)
                         FILTER (WHERE l.location_id IS NOT NULL
                                    OR la.location_id IS NOT NULL) AS mapped
                FROM demand_history dh
                LEFT JOIN locations l ON l.external_id = dh.warehouse_id
                LEFT JOIN (SELECT DISTINCT alias, location_id
                           FROM location_aliases) la ON la.alias = dh.warehouse_id
                """
            ).fetchone()
        if int(diag["total"]) > 0 and int(diag["mapped"]) == 0:
            detail = (
                f"demand_history has {int(diag['total']):,} rows but NONE of its "
                f"{int(diag['warehouses'])} warehouse_id codes resolve to a site "
                "(external_id or alias, ADR-031) — pilot data-onboarding gap "
                "(load the code mapping via POST /v1/ingest/locations aliases)"
            )
        else:
            detail = (
                f"no exploitable booking-based series (mapped warehouse codes: "
                f"{int(diag['mapped'])}/{int(diag['warehouses'])}) — mapped rows "
                "exist but none matches the booking predicates"
            )
        return StepResult(
            number=2, title="Forecast + FVA", status=SKIP, detail=detail,
        )
    item_ext, loc_ext, n_rows = series
    ctx.forecast_item, ctx.forecast_location = item_ext, loc_ext
    print(f"  Richest series : item={item_ext} location={loc_ext} "
          f"({n_rows:,} booking rows)")

    # AUTO_SELECT + a generous horizon/lookback: works on both the pilote base
    # (deep history) and the CI-seeded base (90 days of PUMP-01@DC-ATL).
    create = ctx.client.post(
        "/v1/forecast/runs",
        headers=ctx.human_auth,
        json={
            "item_id": item_ext,
            "location_id": loc_ext,
            "horizon_days": 90,
            "granularity": "weekly",
            "method": "AUTO_SELECT",
        },
    )
    if create.status_code != 201:
        # 422 "historical demand required" is possible if the richest series is
        # below the engine's minimum — treat as an honest SKIP, not a FAIL.
        if create.status_code == 422:
            return StepResult(
                number=2,
                title="Forecast + FVA",
                status=SKIP,
                detail=f"forecast engine declined the series (422): {create.text[:120]}",
            )
        raise RuntimeError(
            f"POST /v1/forecast/runs -> {create.status_code} {create.text}"
        )
    run_id = create.json()["run_id"]
    ctx.forecast_run_id = run_id

    # GET /runs/{id} carries the accuracy metrics (aggregate row first); GET
    # /result carries the per-bucket values with the conformal CI bounds.
    meta = ctx.client.get(f"/v1/forecast/runs/{run_id}", headers=ctx.human_auth)
    if meta.status_code != 200:
        raise RuntimeError(f"GET run meta -> {meta.status_code} {meta.text}")
    result = ctx.client.get(
        f"/v1/forecast/runs/{run_id}/result", headers=ctx.human_auth
    )
    if result.status_code != 200:
        raise RuntimeError(f"GET run result -> {result.status_code} {result.text}")

    body = meta.json()
    values = result.json()["values"]
    agg = _aggregate_accuracy(body.get("accuracy_metrics") or [])
    has_ci = any(
        v.get("confidence_lower") is not None and v.get("confidence_upper") is not None
        for v in values
    )
    selected = body.get("selected_model")
    stale = body.get("stale_demand")

    wape = _fmt_num(agg.get("wape"))
    mase = _fmt_num(agg.get("mase"))
    fva = _fmt_num(agg.get("fva_wape"))
    print(f"  Selected model : {selected}   stale_demand={stale}")
    print(f"  WAPE           : {wape}")
    print(f"  MASE           : {mase}")
    print(f"  FVA (WAPE)     : {fva}   (naive_wape - wape; positive = beats seasonal-naive)")
    print(f"  Conformal CI   : {'present' if has_ci else 'absent (no honest calibration)'}")
    print(f"  Forecast buckets: {len(values)}")

    return StepResult(
        number=2,
        title="Forecast + FVA",
        status=PASS,
        detail=(
            f"run {run_id[:8]} model={selected} WAPE={wape} FVA={fva} "
            f"CI={'yes' if has_ci else 'no'}"
        ),
        data={
            "run_id": run_id,
            "item": item_ext,
            "location": loc_ext,
            "selected_model": selected,
            "wape": agg.get("wape"),
            "mase": agg.get("mase"),
            "fva_wape": agg.get("fva_wape"),
            "has_ci": has_ci,
            "buckets": len(values),
        },
    )


def _aggregate_accuracy(metrics: list[dict[str, Any]]) -> dict[str, Any]:
    """The aggregate (all-horizons) accuracy row is the one with horizon
    None/absent. FVA lives only on that row."""
    for m in metrics:
        if m.get("horizon") is None:
            return m
    return {}


# ===========================================================================
# STEP 3 — governed watchers
# ===========================================================================


def step3_watchers(ctx: DemoContext) -> StepResult:
    """Run the shortage watcher in-process (agent_shortage_watcher.main),
    scenario-backed (#340: it forks ONE what-if per run and archives it),
    then read back the DRAFT recommendations it emitted and their decision
    levels. SKIP when --skip-watchers (the watcher over 36k items takes
    minutes on the pilote base)."""
    if ctx.skip_watchers:
        return StepResult(
            number=3,
            title="Governed watchers",
            status=SKIP,
            detail="--skip-watchers (watcher over a large item base takes minutes)",
        )

    import agent_shortage_watcher

    before = _reco_count(ctx.dsn)
    rc = agent_shortage_watcher.main(["--dsn", ctx.dsn])
    if rc != 0:
        raise RuntimeError(f"agent_shortage_watcher.main returned {rc}")
    after = _reco_count(ctx.dsn)

    levels = _draft_reco_levels(ctx.dsn)
    total_draft = sum(levels.values())
    print(f"  Recommendations before -> after   : {before:,} -> {after:,}")
    print(f"  DRAFT recommendations (by level)   : {dict(sorted(levels.items()))}")
    print(f"  Total DRAFT                        : {total_draft:,}")

    return StepResult(
        number=3,
        title="Governed watchers",
        status=PASS,
        detail=f"shortage watcher emitted {total_draft} DRAFT reco(s), by level {dict(levels)}",
        data={"reco_before": before, "reco_after": after, "by_level": levels},
    )


def _reco_count(dsn: str) -> int:
    with _connect(dsn) as conn:
        return int(
            conn.execute("SELECT COUNT(*) AS n FROM recommendations").fetchone()["n"]
        )


def _draft_reco_levels(dsn: str) -> dict[str, int]:
    with _connect(dsn) as conn:
        rows = conn.execute(
            """
            SELECT decision_level, COUNT(*) AS n
            FROM recommendations
            WHERE status = 'DRAFT'
            GROUP BY decision_level
            """
        ).fetchall()
    return {str(r["decision_level"]): int(r["n"]) for r in rows}


# ===========================================================================
# STEP 4 — DRP inter-site transfer
# ===========================================================================


def _discover_drp_pair(dsn: str) -> Optional[dict[str, Any]]:
    """Find a real (item, source_location, dest_location) where an existing
    active distribution_link already connects a location holding strong on-hand
    to one that does not. Prefer an existing link (additive, zero new master
    data). Returns None if no link+on-hand pair exists.

    Read-only, parameterized by node types only."""
    with _connect(dsn) as conn:
        row = conn.execute(
            """
            SELECT dl.item_id,
                   i.external_id AS item_ext,
                   up.external_id AS src_ext,
                   dn.external_id AS dst_ext,
                   src_oh.qty AS src_onhand
            FROM distribution_links dl
            JOIN items i ON i.item_id = dl.item_id
            JOIN locations up ON up.location_id = dl.upstream_location_id
            JOIN locations dn ON dn.location_id = dl.downstream_location_id
            JOIN LATERAL (
                SELECT COALESCE(SUM(quantity), 0) AS qty
                FROM nodes
                WHERE node_type = 'OnHandSupply' AND active
                  AND item_id = dl.item_id
                  AND location_id = dl.upstream_location_id
            ) src_oh ON TRUE
            WHERE dl.active AND dl.item_id IS NOT NULL
              AND src_oh.qty > 0
            ORDER BY src_oh.qty DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        return None
    return {
        "item_id": str(row["item_id"]),
        "item_ext": str(row["item_ext"]),
        "src_ext": str(row["src_ext"]),
        "dst_ext": str(row["dst_ext"]),
        "src_onhand": float(row["src_onhand"]),
    }


def step4_drp(ctx: DemoContext) -> StepResult:
    """Run DRP (POST /v1/drp/run) and report the governed TRANSFER DRAFTs it
    emitted (fair-share + logistic rounding visible in each reco's evidence).
    On the CI-seeded base, XFER-01 DC-ATL -> DC-LAX already exists (seed_drp)
    and DRP drafts the 180-unit transfer out of the box. If no link exists at
    all, this SKIPs — the demo never fabricates master data beyond an existing
    link (the spec allows creating ONE link, but only when a real excess/
    deficit pair is found; discovery already requires a link, so a missing one
    means there is genuinely no transfer to show)."""
    pair = _discover_drp_pair(ctx.dsn)
    if pair is not None:
        ctx.drp_item = pair["item_ext"]
        print(f"  Transfer lane  : item={pair['item_ext']} "
              f"{pair['src_ext']} -> {pair['dst_ext']} "
              f"(source on-hand {pair['src_onhand']:,.0f})")
    else:
        print("  No distribution_link with a stocked source found — running DRP "
              "over the whole plan anyway (baseline).")

    run = ctx.client.post(
        "/v1/drp/run",
        headers=ctx.agent_auth,  # requires recommend:draft — the agent token has it
        json={"horizon_days": 180},
    )
    if run.status_code != 200:
        raise RuntimeError(f"POST /v1/drp/run -> {run.status_code} {run.text}")
    body = run.json()
    emitted = int(body["recommendations_emitted"])
    noop = int(body["recommendations_idempotent_noop"])
    signals = int(body["signals"])
    print(f"  Signals        : {signals}")
    print(f"  Transfers emitted (new)            : {emitted}")
    print(f"  Idempotent no-op (already drafted) : {noop}")
    print(f"  Decision level : {body['decision_level']}")

    # Show the fair-share/rounding evidence on the freshest transfer DRAFT.
    sample = _sample_transfer_reco(ctx.dsn)
    if sample is not None:
        print(f"  Sample transfer: item={sample['item_external_id']} "
              f"qty={sample['recommended_qty']} "
              f"({sample['evidence_note']})")

    # emitted + noop together prove the lane resolved to a transfer even on a
    # re-run (idempotence): a PASS requires at least one transfer signal.
    if signals == 0 and pair is not None:
        return StepResult(
            number=4,
            title="DRP transfer",
            status=FAIL,
            detail="a stocked transfer lane exists but DRP produced 0 signals",
            data=body,
        )
    return StepResult(
        number=4,
        title="DRP transfer",
        status=PASS,
        detail=f"{emitted} new + {noop} idempotent transfer DRAFT(s), {signals} signal(s)",
        data={**body, "lane": pair},
    )


def _sample_transfer_reco(dsn: str) -> Optional[dict[str, Any]]:
    """Newest TRANSFER DRAFT with a one-line fair-share/rounding note pulled
    from its evidence (read-only)."""
    with _connect(dsn) as conn:
        row = conn.execute(
            """
            SELECT item_external_id, recommended_qty, evidence
            FROM recommendations
            WHERE action = 'TRANSFER' AND status = 'DRAFT'
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        return None
    evidence = row["evidence"] or {}
    # evidence shape is emitter-defined; surface a couple of telling keys if
    # present, else a compact repr — never assume a fixed schema.
    keys = ("fair_share_qty", "rounded_qty", "transfer_multiple", "deficit_qty")
    picked = {k: evidence[k] for k in keys if isinstance(evidence, dict) and k in evidence}
    note = ", ".join(f"{k}={v}" for k, v in picked.items()) or "see evidence trail"
    return {
        "item_external_id": row["item_external_id"],
        "recommended_qty": float(row["recommended_qty"]),
        "evidence_note": note,
    }


# ===========================================================================
# STEP 5 — cryptographic governance gate (#392)
# ===========================================================================


def _pick_draft_reco(dsn: str) -> Optional[dict[str, Any]]:
    """Newest baseline DRAFT recommendation — the subject of the gate demo.
    Prefer one this demo's runs just created (any DRAFT works)."""
    with _connect(dsn) as conn:
        row = conn.execute(
            """
            SELECT recommendation_id, item_external_id, action, decision_level
            FROM recommendations
            WHERE status = 'DRAFT' AND scenario_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (BASELINE_SCENARIO_ID,),
        ).fetchone()
    if row is None:
        return None
    return {
        "recommendation_id": str(row["recommendation_id"]),
        "item_external_id": row["item_external_id"],
        "action": row["action"],
        "decision_level": row["decision_level"],
    }


def step5_gate(ctx: DemoContext) -> StepResult:
    """THE #392 demonstration. Take a DRAFT reco, move it DRAFT -> REVIEWED
    (the agent may), then:
      * the AGENT token attempts REVIEWED -> APPROVED  => 403 (human-only gate)
      * the HUMAN token performs REVIEWED -> APPROVED  => 200
    The actor_kind that decides the gate comes from the TOKEN, never the body —
    an agent cannot self-declare 'human'. SKIP if no DRAFT reco exists (steps 3
    and 4 were both skipped/empty)."""
    reco = _pick_draft_reco(ctx.dsn)
    if reco is None:
        return StepResult(
            number=5,
            title="Governance gate",
            status=SKIP,
            detail="no DRAFT recommendation to govern (steps 3 & 4 produced none)",
        )
    reco_id = reco["recommendation_id"]
    ctx.draft_reco_for_gate = reco_id
    print(f"  Subject reco   : {reco_id[:8]} item={reco['item_external_id']} "
          f"action={reco['action']} level={reco['decision_level']}")

    # DRAFT -> REVIEWED with the AGENT token (agents may review; needs
    # recommend:draft, which the agent token holds).
    reviewed = ctx.client.post(
        f"/v1/recommendations/{reco_id}/transition",
        headers=ctx.agent_auth,
        json={"to_status": "REVIEWED", "actor": "demo-agent", "actor_kind": "agent"},
    )
    if reviewed.status_code != 200:
        raise RuntimeError(
            f"agent DRAFT->REVIEWED expected 200, got {reviewed.status_code} {reviewed.text}"
        )
    print("  Agent DRAFT -> REVIEWED            : 200 (agents may review)")

    # AGENT attempts the L3 approval — must be refused by the human gate. The
    # body deliberately LIES actor_kind='human' to prove the token wins.
    agent_approve = ctx.client.post(
        f"/v1/recommendations/{reco_id}/transition",
        headers=ctx.agent_auth,
        json={"to_status": "APPROVED", "actor": "demo-agent", "actor_kind": "human"},
    )
    if agent_approve.status_code != 403:
        raise RuntimeError(
            f"agent REVIEWED->APPROVED expected 403, got "
            f"{agent_approve.status_code} {agent_approve.text}"
        )
    gate_detail = agent_approve.json().get("detail", "")
    print("  Agent REVIEWED -> APPROVED         : 403 (blocked)")
    print(f"    gate detail: {gate_detail}")

    # HUMAN performs the approval — passes both the scope floor and the gate.
    human_approve = ctx.client.post(
        f"/v1/recommendations/{reco_id}/transition",
        headers=ctx.human_auth,
        json={"to_status": "APPROVED", "actor": "demo-human", "actor_kind": "human"},
    )
    if human_approve.status_code != 200:
        raise RuntimeError(
            f"human REVIEWED->APPROVED expected 200, got "
            f"{human_approve.status_code} {human_approve.text}"
        )
    print("  Human REVIEWED -> APPROVED         : 200 (approved)")

    return StepResult(
        number=5,
        title="Governance gate",
        status=PASS,
        detail="agent APPROVE 403 (token wins over lying body); human APPROVE 200",
        data={"recommendation_id": reco_id, "gate_detail": gate_detail},
    )


# ===========================================================================
# STEP 6 — the proof machine (snapshots -> outcomes -> KPIs)
# ===========================================================================


def step6_proof(ctx: DemoContext) -> StepResult:
    """Close the value-proof loop: capture inventory snapshots (human token,
    ingest scope), evaluate recommendation outcomes, then read the five proof
    KPIs. Every KPI is NULL-honest — 'n/a (no data)' for NULL, never a
    misleading 0."""
    cap = ctx.client.post(
        "/v1/snapshots", headers=ctx.human_auth, json={}
    )
    if cap.status_code != 201:
        raise RuntimeError(f"POST /v1/snapshots -> {cap.status_code} {cap.text}")
    captured = int(cap.json()["snapshots_captured"])
    as_of = cap.json()["as_of_date"]
    print(f"  Snapshots      : captured {captured:,} coordinate(s) as_of {as_of}")

    ev = ctx.client.post(
        "/v1/outcomes/evaluate", headers=ctx.human_auth, json={}
    )
    if ev.status_code != 201:
        raise RuntimeError(f"POST /v1/outcomes/evaluate -> {ev.status_code} {ev.text}")
    ev_body = ev.json()
    print(f"  Outcomes eval  : evaluated {ev_body['evaluated']} "
          f"upserted {ev_body['upserted']} by_status {ev_body['by_status']}")

    summ = ctx.client.get("/v1/outcomes/summary", headers=ctx.human_auth)
    if summ.status_code != 200:
        raise RuntimeError(f"GET /v1/outcomes/summary -> {summ.status_code} {summ.text}")
    k = summ.json()

    print("  Proof KPIs (NULL-honest):")
    print(f"    1. pct_shortages_avoided     : {_kpi(k['pct_shortages_avoided'])}"
          f"   basis={k['avoided_basis_count']}")
    print(f"    2. avoided_severity_usd_total: {_kpi(k['avoided_severity_usd_total'])}")
    print(f"    3. avg_fva_wape (real FVA)   : {_kpi(k['avg_fva_wape'])}"
          f"   basis={k['fva_basis_count']}")
    print(f"    4. reco_approval_rate        : {_kpi(k['reco_approval_rate'])}"
          f"   total_recos={k['reco_total_count']}")
    print(f"    5. cost_of_inaction_usd      : {_kpi(k['cost_of_inaction_usd'])}")

    return StepResult(
        number=6,
        title="Proof machine",
        status=PASS,
        detail=(
            f"snapshots={captured} evaluated={ev_body['evaluated']} "
            f"approval_rate={_kpi(k['reco_approval_rate'])}"
        ),
        data={
            "snapshots_captured": captured,
            "outcomes": ev_body,
            "kpis": k,
        },
    )


def _kpi(value: Any) -> str:
    """NULL-honest KPI rendering: None -> 'n/a (no data)', never a fake 0."""
    if value is None:
        return "n/a (no data)"
    if isinstance(value, float):
        return f"{value:,.4f}".rstrip("0").rstrip(".")
    return str(value)


# ===========================================================================
# STEP 7 — forkable what-if (scenario + param overlay + honest delta)
# ===========================================================================


def _discover_shortage_coord(dsn: str, base_scenario_id: str) -> Optional[dict[str, Any]]:
    """A real (item, location) that currently has an active shortage in the
    given base scenario — the coordinate whose safety stock we relax in a fork.
    Read-only."""
    with _connect(dsn) as conn:
        row = conn.execute(
            """
            SELECT s.item_id, i.external_id AS item_ext,
                   s.location_id, l.external_id AS loc_ext
            FROM shortages s
            JOIN items i ON i.item_id = s.item_id
            JOIN locations l ON l.location_id = s.location_id
            WHERE s.status = 'active' AND s.scenario_id = %s
            ORDER BY s.severity_score DESC NULLS LAST
            LIMIT 1
            """,
            (base_scenario_id,),
        ).fetchone()
    if row is None:
        return None
    return {
        "item_id": str(row["item_id"]),
        "item_ext": str(row["item_ext"]),
        "location_id": str(row["location_id"]) if row["location_id"] else None,
        "loc_ext": str(row["loc_ext"]) if row["loc_ext"] else None,
    }


def _discover_any_pi_coord(dsn: str, base_scenario_id: str) -> Optional[dict[str, Any]]:
    """Fallback what-if coordinate when the base has NO active shortage: any
    (item, location) that carries a ProjectedInventory node in the base
    scenario (so the fork's node-override recompute has something to bite on).
    Deterministic pick (most PI buckets first). Read-only. Same return shape as
    _discover_shortage_coord."""
    with _connect(dsn) as conn:
        row = conn.execute(
            """
            SELECT n.item_id, i.external_id AS item_ext,
                   n.location_id, l.external_id AS loc_ext,
                   COUNT(*) AS n_buckets
            FROM nodes n
            JOIN items i ON i.item_id = n.item_id
            JOIN locations l ON l.location_id = n.location_id
            WHERE n.node_type = 'ProjectedInventory'
              AND n.scenario_id = %s AND n.active
              AND n.item_id IS NOT NULL AND n.location_id IS NOT NULL
            GROUP BY n.item_id, i.external_id, n.location_id, l.external_id
            ORDER BY n_buckets DESC, i.external_id, l.external_id
            LIMIT 1
            """,
            (base_scenario_id,),
        ).fetchone()
    if row is None:
        return None
    return {
        "item_id": str(row["item_id"]),
        "item_ext": str(row["item_ext"]),
        "location_id": str(row["location_id"]),
        "loc_ext": str(row["loc_ext"]),
    }


def _pick_pi_node_for_fork(
    dsn: str, base_scenario_id: str, item_id: str, location_id: Optional[str]
) -> Optional[str]:
    """A ProjectedInventory node id (in the base scenario) to hang a simulate
    override on — the node override path that /v1/simulate recomputes a shortage
    delta from. We nudge its opening_stock upward, a whitelisted simulate field,
    to prove the fork recomputes an HONEST delta (new/resolved)."""
    conds = ["node_type = 'ProjectedInventory'", "active", "scenario_id = %s", "item_id = %s"]
    params: list[Any] = [base_scenario_id, item_id]
    if location_id is not None:
        conds.append("location_id = %s")
        params.append(location_id)
    where = " AND ".join(conds)
    with _connect(dsn) as conn:
        row = conn.execute(
            f"SELECT node_id FROM nodes WHERE {where} "  # noqa: S608 — static columns, params bound
            "ORDER BY bucket_sequence LIMIT 1",
            params,
        ).fetchone()
    return str(row["node_id"]) if row else None


def step7_whatif(ctx: DemoContext) -> StepResult:
    """Forkable counter-factual, honest delta. Demonstrate BOTH forkable
    surfaces on ONE fork:
      (a) POST /v1/simulate — fork baseline, apply a node override on a PI
          node at a shortage coordinate, recompute, return the HONEST shortage
          delta (new/resolved, delta_computed flag). This is the chiffered
          what-if.
      (b) POST /v1/scenarios/{fork}/param-overrides — attach a safety_stock_qty
          overlay override (#347) to that same fork on the shortage item, then
          list it back: proof the 15 planning-param fields are forkable via
          REST (L0, never promoted onto baseline).
    Then ARCHIVE the fork (status='archived', never DELETE).

    Coordinate: the worst active shortage when the base has one; otherwise
    fall back to any ProjectedInventory coordinate and tighten (opening_stock
    to 0) instead of relax. SKIP only when the base has no PI node at all.

    Base scenario: ctx.whatif_base_scenario (default baseline). A non-baseline
    fork here is the pilot fork-on-fork path (#414): discovery, node pick, and
    the /v1/simulate base_scenario_id all read that fork, so the counter-factual
    is a fork OF the pilot fork — never touching baseline."""
    base = ctx.whatif_base_scenario
    is_baseline_base = base == BASELINE_SCENARIO_ID
    if not is_baseline_base:
        print(f"  What-if base   : {base} (fork-on-fork; baseline untouched)")
    coord = _discover_shortage_coord(ctx.dsn, base)
    relax = True  # shortage coord -> RELAX stock (resolve); fallback -> TIGHTEN
    if coord is None:
        # No active shortage on the base (e.g. no recent calc run). Fall back to
        # ANY ProjectedInventory coordinate and run the counter-factual in the
        # OTHER direction: opening_stock=0 ("what if we had less stock?") — an
        # equally honest fork whose delta shows NEW shortages instead of
        # resolved ones. Only SKIP when the base has no PI node at all to fork.
        coord = _discover_any_pi_coord(ctx.dsn, base)
        relax = False
        if coord is None:
            return StepResult(
                number=7,
                title="Forkable what-if",
                status=SKIP,
                detail=(
                    "no active-shortage coordinate AND no ProjectedInventory "
                    f"node on base scenario {base[:8]} to fork"
                ),
            )
        print("  No active shortage on the base — falling back to a PI "
              "coordinate, tightening instead of relaxing")
    print(f"  What-if coord  : item={coord['item_ext']} "
          f"location={coord['loc_ext']} "
          f"(direction: {'relax stock' if relax else 'tighten stock to 0'})")

    pi_node = _pick_pi_node_for_fork(ctx.dsn, base, coord["item_id"], coord["location_id"])
    fork_name = f"demo-e2e-whatif-{_dt.datetime.now(_dt.timezone.utc):%Y%m%dT%H%M%SZ}"

    overrides = []
    if pi_node is not None:
        # Shortage coordinate: relax the opening balance so the recompute
        # resolves shortages. Fallback coordinate: tighten it to zero so the
        # recompute provokes shortages. Either way the delta is honest.
        new_value = "100000" if relax else "0"
        overrides.append(
            {"node_id": pi_node, "field_name": "opening_stock", "new_value": new_value}
        )

    sim = ctx.client.post(
        "/v1/simulate",
        headers=ctx.human_auth,
        json={
            "scenario_name": fork_name,
            "base_scenario_id": base,
            "overrides": overrides,
        },
    )
    if sim.status_code != 201:
        raise RuntimeError(f"POST /v1/simulate -> {sim.status_code} {sim.text}")
    sim_body = sim.json()
    fork_id = sim_body["scenario_id"]
    ctx.fork_scenario_id = fork_id
    delta = sim_body.get("delta", {})
    n_new = len(delta.get("new_shortages", []))
    n_resolved = len(delta.get("resolved_shortages", []))
    print(f"  Fork           : {fork_id[:8]} name={fork_name}")
    print(f"  Propagation    : status={sim_body['propagation_status']} "
          f"delta_computed={sim_body['delta_computed']}")
    print(f"  Honest delta   : new_shortages={n_new} resolved_shortages={n_resolved} "
          f"net={delta.get('net_shortage_change')}")

    # (b) Attach a planning-param overlay override to the SAME fork and read it
    # back — proof the #347 whitelist is forkable via REST. safety_stock_qty is
    # a whitelisted field; the coordinate is the shortage item (+ location when
    # it carries one).
    overlay_ok = False
    overlay_note = "skipped (no location on the coordinate)"
    if coord["location_id"] is not None:
        po_body: dict[str, Any] = {
            "item_id": coord["item_id"],
            "location_id": coord["location_id"],
            "field_name": "safety_stock_qty",
            "value": "0",
            "applied_by": "demo-e2e",
        }
        po = ctx.client.post(
            f"/v1/scenarios/{fork_id}/param-overrides",
            headers=ctx.human_auth,
            json=po_body,
        )
        if po.status_code == 201:
            lst = ctx.client.get(
                f"/v1/scenarios/{fork_id}/param-overrides", headers=ctx.human_auth
            )
            n_over = lst.json().get("total", 0) if lst.status_code == 200 else 0
            overlay_ok = True
            overlay_note = f"safety_stock_qty overlay set, {n_over} override(s) on fork"
        else:
            # A 422 here (e.g. item has no planning-params row) is an honest,
            # non-fatal outcome — the fork + simulate delta already proved
            # forkability; report it without failing the step.
            overlay_note = f"overlay refused ({po.status_code}): {po.text[:100]}"
    print(f"  Param overlay  : {overlay_note}")

    # Archive the fork — TTL pattern, never DELETE. DELETE /v1/scenarios/{id}
    # sets status='archived' (see scenarios.py), the product's own archival.
    arch = ctx.client.delete(f"/v1/scenarios/{fork_id}", headers=ctx.human_auth)
    archived = arch.status_code == 204
    print(f"  Fork archived  : {'yes (status=archived)' if archived else f'no ({arch.status_code})'}")

    return StepResult(
        number=7,
        title="Forkable what-if",
        status=PASS,
        detail=(
            f"fork {fork_id[:8]} delta new={n_new}/resolved={n_resolved}"
            # 0/0 with a failed recompute is NOT "no change" — say so loudly.
            + ("" if sim_body["delta_computed"] else " (DELTA NOT COMPUTED)")
            + f", overlay={'set' if overlay_ok else 'n/a'}, archived={archived}"
            # Only mention the base when it is NOT baseline, to keep the CI-seeded
            # run's line identical to before this change.
            + ("" if is_baseline_base else f", base={base[:8]}")
        ),
        data={
            "fork_id": fork_id,
            "base_scenario_id": base,
            "propagation_status": sim_body["propagation_status"],
            "delta_computed": sim_body["delta_computed"],
            "new_shortages": n_new,
            "resolved_shortages": n_resolved,
            "overlay_set": overlay_ok,
            "archived": archived,
        },
    )


# ===========================================================================
# STEP 8 — StreamChanges (bounded replay by cursor)
# ===========================================================================


def step8_stream(ctx: DemoContext) -> StepResult:
    """Bounded, replayable read of everything this demo just did:
    GET /v1/stream?once=true&cursor=0 drains the baseline event history once
    and closes. This is the "for AI" proof — every state change the demo made
    is replayable by cursor, agents subscribe instead of polling. Parses the
    SSE frames to count events and report the last stream_seq."""
    resp = ctx.client.get(
        "/v1/stream?once=true&cursor=0", headers=ctx.agent_auth
    )
    if resp.status_code != 200:
        raise RuntimeError(f"GET /v1/stream -> {resp.status_code} {resp.text}")
    if not resp.headers.get("content-type", "").startswith("text/event-stream"):
        raise RuntimeError(
            f"stream content-type not SSE: {resp.headers.get('content-type')}"
        )

    n_events, last_seq, types = _parse_sse(resp.text)
    print(f"  Events replayed (cursor=0)         : {n_events:,}")
    print(f"  Last stream_seq                    : {last_seq}")
    if types:
        top = ", ".join(f"{t}={c}" for t, c in sorted(types.items())[:6])
        print(f"  Event types (top)                  : {top}")

    return StepResult(
        number=8,
        title="StreamChanges",
        status=PASS,
        detail=f"{n_events} event(s) replayable by cursor, last stream_seq={last_seq}",
        data={"events": n_events, "last_stream_seq": last_seq, "types": types},
    )


def _parse_sse(text: str) -> tuple[int, Optional[int], dict[str, int]]:
    """Count SSE frames carrying an ``id:`` line (data frames, not pings),
    return (count, last_id, {event_type: count}). Pure text parse — no schema
    assumption beyond the SSE ``id:``/``event:`` line grammar."""
    count = 0
    last_id: Optional[int] = None
    types: dict[str, int] = {}
    cur_id: Optional[int] = None
    cur_type: Optional[str] = None
    for line in text.splitlines():
        if line.startswith("id:"):
            raw = line[3:].strip()
            cur_id = int(raw) if raw.isdigit() else None
        elif line.startswith("event:"):
            cur_type = line[6:].strip()
        elif line == "":
            if cur_id is not None:
                count += 1
                last_id = cur_id
                if cur_type is not None:
                    types[cur_type] = types.get(cur_type, 0) + 1
            cur_id, cur_type = None, None
    return count, last_id, types


# ===========================================================================
# STEP 9 — (optional) MRP bench, read-only, subprocess
# ===========================================================================


def step9_bench(ctx: DemoContext) -> StepResult:
    """Optional (--bench): run scripts/bench_mrp.py in a subprocess (its own
    READ ONLY transaction) and echo the per-phase timings. Kept a subprocess so
    its READ ONLY transaction guard and process isolation are exactly as the
    perf harness intends — the demo never imports the bench into its own
    process."""
    if not ctx.run_bench:
        return StepResult(
            number=9,
            title="MRP bench",
            status=SKIP,
            detail="--bench not set",
        )
    bench_path = _SCRIPTS_DIR / "bench_mrp.py"
    proc = subprocess.run(
        [sys.executable, str(bench_path), "--dsn", ctx.dsn, "--repeats", "1"],
        capture_output=True,
        text=True,
        timeout=600,
    )
    # bench_mrp prints the DB NAME only (not the DSN) — safe to echo verbatim.
    for line in proc.stdout.splitlines():
        print(f"  | {line}")
    if proc.returncode != 0:
        return StepResult(
            number=9,
            title="MRP bench",
            status=FAIL,
            detail=f"bench_mrp exited {proc.returncode}: {proc.stderr.strip()[:160]}",
        )
    return StepResult(
        number=9,
        title="MRP bench",
        status=PASS,
        detail="MRP cascade benched (read-only, 1 repeat)",
        data={"stdout_tail": proc.stdout.splitlines()[-6:]},
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _run_step(
    fn: Callable[[DemoContext], StepResult], ctx: DemoContext
) -> StepResult:
    """Wrap one step: any exception becomes a FAIL with a compact message
    (full traceback only under --verbose). A FAIL never aborts the run
    (except step 0, handled specially in main)."""
    try:
        return fn(ctx)
    except Exception as exc:  # noqa: BLE001 — a step must never crash the runbook
        if ctx.verbose:
            traceback.print_exc()
        # Guard against a DSN leaking through an exception message.
        msg = str(exc).replace(ctx.dsn, mask_dsn(ctx.dsn))
        # fn.__name__ like "step3_watchers" -> number/title recovered best-effort.
        return StepResult(
            number=_step_number(fn),
            title=_step_title(fn),
            status=FAIL,
            detail=msg[:200],
        )


_STEP_TITLES = {
    "step1_tokens": (1, "Governed tokens"),
    "step2_forecast": (2, "Forecast + FVA"),
    "step3_watchers": (3, "Governed watchers"),
    "step4_drp": (4, "DRP transfer"),
    "step5_gate": (5, "Governance gate"),
    "step6_proof": (6, "Proof machine"),
    "step7_whatif": (7, "Forkable what-if"),
    "step8_stream": (8, "StreamChanges"),
    "step9_bench": (9, "MRP bench"),
}


def _step_number(fn: Callable[..., Any]) -> int:
    return _STEP_TITLES.get(fn.__name__, (0, ""))[0]


def _step_title(fn: Callable[..., Any]) -> str:
    return _STEP_TITLES.get(fn.__name__, (0, fn.__name__))[1]


def _fmt_num(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):,.4f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return str(value)


def _scoreboard(results: list[StepResult]) -> tuple[int, int, int]:
    passed = sum(1 for r in results if r.status == PASS)
    skipped = sum(1 for r in results if r.status == SKIP)
    failed = sum(1 for r in results if r.status == FAIL)
    return passed, skipped, failed


def _write_artefact(path: Path, dsn: str, results: list[StepResult]) -> None:
    """Persist a scoreboard JSON. The DSN is NEVER written — only mask_dsn."""
    passed, skipped, failed = _scoreboard(results)
    payload = {
        "db": mask_dsn(dsn),
        "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "summary": {"pass": passed, "skip": skipped, "fail": failed},
        "steps": [
            {
                "number": r.number,
                "title": r.title,
                "status": r.status,
                "detail": r.detail,
                "data": r.data,
            }
            for r in results
        ],
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _parse_args(argv: Optional[list[str]]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Executable wedge runbook (#408): forecast+FVA, governed watchers, "
            "DRP, cryptographic governance gate, deterministic proof KPIs, "
            "forkable what-if, cursor-replayable stream. Non-destructive, "
            "idempotent, DSN never printed."
        )
    )
    p.add_argument("--dsn", required=True, help="PostgreSQL DSN (never printed).")
    p.add_argument(
        "--allow-dev",
        action="store_true",
        help="Permit an 'ootils_dev' target (semi-prod guard, mirrors the CLIs).",
    )
    p.add_argument(
        "--bench",
        action="store_true",
        help="Also run the MRP perf bench (read-only subprocess).",
    )
    p.add_argument(
        "--skip-watchers",
        action="store_true",
        help="Skip the shortage watcher (minutes on a large item base).",
    )
    p.add_argument("--out", default=None, help="Write a scoreboard JSON to this path.")
    p.add_argument(
        "--whatif-base-scenario",
        default="baseline",
        help=(
            "Base scenario for step 7's what-if fork (UUID or 'baseline', "
            "default 'baseline'). A fork here runs the pilot fork-on-fork "
            "counter-factual; baseline stays untouched (#414)."
        ),
    )
    p.add_argument(
        "--show-tokens",
        action="store_true",
        help="Print the minted token cleartext (operator escape hatch).",
    )
    p.add_argument("--verbose", action="store_true", help="Full tracebacks on FAIL.")
    return p.parse_args(argv)


def _guard_target(dsn: str, allow_dev: bool) -> Optional[str]:
    """Mirror the CLI 'ootils*' guard so the demo can never point at a
    non-ootils database, and refuse the semi-prod ootils_dev without an
    explicit --allow-dev. Returns an error message, or None when OK."""
    name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not name.startswith("ootils"):
        return f"REFUSED: target {mask_dsn(dsn)} does not start with 'ootils'."
    if name == "ootils_dev" and not allow_dev:
        return "REFUSED: ootils_dev is semi-prod; pass --allow-dev to target it."
    return None


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)

    if not os.environ.get("OOTILS_API_TOKEN"):
        print(
            "ERROR: OOTILS_API_TOKEN must be set in the environment "
            "(the app refuses to start without it).",
            file=sys.stderr,
        )
        return 2

    guard_error = _guard_target(args.dsn, args.allow_dev)
    if guard_error is not None:
        print(f"ERROR: {guard_error}", file=sys.stderr)
        return 2

    # MUST happen before the FIRST ootils_core import (step 0 imports OotilsDB):
    # db/connection.py freezes DEFAULT_DATABASE_URL at module import, and the
    # auth layer's minted-token lookup (resolve_principal -> _get_ootils_db)
    # builds a SINGLETON OotilsDB() from that frozen default — it never goes
    # through the get_db override. Without this, every minted-token request
    # 503s "Authentication backend unavailable" against a default localhost DB
    # (the exact failure of the first pilot run).
    os.environ["DATABASE_URL"] = args.dsn

    # Resolve the what-if base: accept the literal 'baseline' (or a bad UUID)
    # as the sentinel, otherwise the caller's fork UUID. The simulate router
    # also tolerates 'baseline'/bad UUIDs, but resolving here keeps the ctx
    # value a canonical UUID string for the discovery SELECTs.
    raw_base = (args.whatif_base_scenario or "baseline").strip()
    if raw_base.lower() == "baseline":
        whatif_base = BASELINE_SCENARIO_ID
    else:
        try:
            whatif_base = str(UUID(raw_base))
        except ValueError:
            print(
                f"ERROR: --whatif-base-scenario {raw_base!r} is neither 'baseline' "
                "nor a valid UUID.",
                file=sys.stderr,
            )
            return 2

    _banner("OOTILS WEDGE RUNBOOK (#408) — autonomous shortage control tower")
    print(f"  Target : {mask_dsn(args.dsn)}")
    print(f"  Mode   : skip_watchers={args.skip_watchers} bench={args.bench}")
    if whatif_base != BASELINE_SCENARIO_ID:
        print(f"  What-if base scenario (step 7): {whatif_base}")

    results: list[StepResult] = []

    # Step 0 is special: its FAIL aborts (nothing downstream is meaningful).
    _step_header(0, "Boot & migration catch-up")
    try:
        step0, client = step0_boot(args.dsn)
    except Exception as exc:  # noqa: BLE001
        if args.verbose:
            traceback.print_exc()
        msg = str(exc).replace(args.dsn, mask_dsn(args.dsn))
        print(_verdict_line(StepResult(0, "Boot & migration catch-up", FAIL, msg[:200])))
        print("\nABORT: cannot boot the app / reach the DB — no further steps run.")
        return 1
    results.append(step0)
    print(_verdict_line(step0))

    ctx = DemoContext(
        dsn=args.dsn,
        client=client,
        verbose=args.verbose,
        skip_watchers=args.skip_watchers,
        run_bench=args.bench,
        show_tokens=args.show_tokens,
        whatif_base_scenario=whatif_base,
    )

    steps: list[Callable[[DemoContext], StepResult]] = [
        step1_tokens,
        step2_forecast,
        step3_watchers,
        step4_drp,
        step5_gate,
        step6_proof,
        step7_whatif,
        step8_stream,
        step9_bench,
    ]

    try:
        for fn in steps:
            _step_header(_step_number(fn), _step_title(fn))
            result = _run_step(fn, ctx)
            results.append(result)
            print(_verdict_line(result))
    finally:
        client.close()

    # Scoreboard
    passed, skipped, failed = _scoreboard(results)
    print()
    _banner("SCOREBOARD")
    for r in results:
        print(f"  {_verdict_line(r)}")
    print("-" * _WIDTH)
    print(f"  TOTAL: {passed} pass / {skipped} skip / {failed} fail   [{mask_dsn(args.dsn)}]")
    _banner("END")

    if args.out is not None:
        out_path = Path(args.out)
        _write_artefact(out_path, args.dsn, results)
        print(f"Scoreboard written to {out_path}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
