# Runbook — first graph propagation on the pilot base (#414)

> Status: operator runbook (#414, deliverable C).
> Audience: the orchestrator running the FIRST ProjectedInventory propagation on
> the pilot base (36 635 items, ~211 K raw nodes, **0** ProjectedInventory nodes).
> Companion scripts: `scripts/bootstrap_pi.py`, `scripts/demo_e2e.py`.

## Why this runbook exists

The propagation engine only ever *recalculates* ProjectedInventory (PI) nodes; it
never materialises them. `scripts/bootstrap_pi.py` is what materialises them (one
`projection_series` + one PI node per day, plus the supply/demand wiring edges,
per active `(item, location)` pair). Bootstrap has **never run on the pilot base**,
so there is nothing for the engine to propagate.

A baseline big-bang is **refused**: 36 K items × their sub-trees × a long horizon
would materialise 13–20 M PI nodes — permanent debt the propagator then carries on
every run. The chosen path is **scenario-first**:

1. Fork a dedicated scenario.
2. Bootstrap a **coherent BOM subset** over a **short horizon** *inside the fork*.
3. Propagate the fork.
4. Read the what-if surface from the fork.
5. Archive the fork.

The `bootstrap_pi.py` **2 000 000-node volumetric guard** is the anti-big-bang
rampart: it refuses `pairs × horizon` above the ceiling unless `--force`.

## Prerequisites

```bash
# A reachable PostgreSQL 16 pilot DB whose name starts with "ootils".
export DATABASE_URL='postgresql://…/ootils_pilote_test'

# The token the API itself needs to boot (any strong secret).
export OOTILS_API_TOKEN='choose-a-strong-secret'

# Source tree on the path (this milestone runs from the worktree).
export PYTHONPATH="$PWD/src"

# API base URL for the HTTP steps below.
export OOTILS_URL='http://127.0.0.1:8000'
export AUTH="Authorization: Bearer $OOTILS_API_TOKEN"
```

The API must be running (`uvicorn ootils_core.api.app:app`), or point `$OOTILS_URL`
at the pilot API. Migrations auto-apply at API startup.

The **DSN is never printed** by these tools — only the database name.

---

## Step 1 — Create the fork

There is **no** `POST /v1/scenarios` create route. The only REST path that
persists a new scenario is `POST /v1/simulate`, which forks a base scenario (here
baseline) via a deep-copy and returns the new `scenario_id`. Pass **no overrides**
so the fork is a faithful copy of baseline (which today has 0 PI nodes — so this
fork is cheap: it copies the raw supply/demand nodes only).

```bash
TS=$(date -u +%Y%m%dT%H%M%SZ)
curl -sS -X POST "$OOTILS_URL/v1/simulate" -H "$AUTH" -H 'Content-Type: application/json' \
  -d "{\"scenario_name\": \"pilot-propagation-$TS\", \"base_scenario_id\": \"baseline\", \"overrides\": []}"
```

Expected (abridged): HTTP 201, and a body carrying the new fork id. With zero PI
nodes on baseline the recompute is a no-op, so `propagation_status` is `skipped`
— that is expected and fine here (bootstrap in step 2 is what creates the PI
nodes to propagate).

```json
{
  "scenario_id": "1b9d…-fork-uuid",
  "scenario_name": "pilot-propagation-20260706T101500Z",
  "status": "created",
  "override_count": 0,
  "base_scenario_id": "00000000-0000-0000-0000-000000000001",
  "propagation_status": "skipped",
  "delta_computed": false
}
```

Capture the fork id:

```bash
export FORK='1b9d…-fork-uuid'   # the scenario_id returned above
```

---

## Step 2 — Bootstrap the PI graph inside the fork

Materialise PI nodes + edges **scoped to the fork**, on the 300 highest-demand
FINISHED items plus their full BOM sub-tree, over a 120-day horizon.

- **Finished item** = an item that is never a component of any active BOM line
  (LLC 0: BOM roots and standalone items).
- **Ranking** = booking demand (`demand_history.ordered_quantity`, `stream='regular'`)
  over the last 365 days, summed **per item** (the pilot's `warehouse_id` codes are
  not reliably mapped to `locations`, so demand is aggregated item-level).
- **BOM closure** = every transitive component of the seed items is pulled into
  scope, so a parent is never projected without the components its dependent
  demand needs.

```bash
python scripts/bootstrap_pi.py \
  --dsn "$DATABASE_URL" \
  --scenario "$FORK" \
  --sample-finished 300 \
  --horizon-days 120
```

Rough sizing: ~300 finished items, expanded by their BOM sub-trees to some
hundreds/low-thousands of items, × the number of active `(item, location)` pairs,
× 120 days. This stays well under the 2 M ceiling. If it does not, the script
**refuses** with a clear message — narrow the subset or shorten the horizon
(`--force` overrides the guard but accepts the debt knowingly).

Expected: an INFO summary, then a single machine-readable metrics line on stdout
(marker `BOOTSTRAP_METRICS:`). Capture it for the record:

```
BOOTSTRAP_METRICS: {"scenario_id": "1b9d…", "subset_mode": "sample_finished",
 "seed_items": 300, "scope_items_after_bom_closure": 812,
 "pairs_in_scope": 2450, "total_pairs_with_activity": 2450,
 "projected_nodes_estimate": 294000, "volumetric_ceiling": 2000000,
 "forced": false, "horizon_days": 120, "horizon_start": "2026-07-06",
 "horizon_end": "2026-11-03", "projection_series_created": 2450,
 "pi_nodes_created": 294000, "feeds_forward_edges": 291550,
 "supply_edges": 5100, "demand_edges": 8300, "edges_created": 304950,
 "total_rows": 601400, "scenario_nodes_before": 40120,
 "scenario_nodes_after": 334120,
 "timings_s": {"0_subset_s": 0.4, "1_identify_pairs_s": 1.1,
   "2_create_series_s": 0.6, "3_create_pi_nodes_s": 12.3,
   "4_feeds_forward_s": 6.8, "5_supply_edges_s": 2.1,
   "6_demand_edges_s": 3.4}, "wall_total_s": 27.2}
```

The numbers above are **illustrative** — the real values are what you record. The
guard fired (or not) is visible via `projected_nodes_estimate` vs
`volumetric_ceiling`; `forced=false` confirms the guard was respected.

---

## Step 3 — Trigger the propagation on the fork

The real, non-destructive, scenario-scoped **full-graph** propagation trigger is
`POST /v1/calc/run` with `full_recompute: true`, targeting the fork via the
`scenario_id` query parameter (or the `X-Scenario-ID` header). It marks every PI
node in the fork dirty and runs one full recompute (deterministic — ADR-003, not a
decision, so `require_auth` only).

```bash
curl -sS -X POST "$OOTILS_URL/v1/calc/run?scenario_id=$FORK" -H "$AUTH" \
  -H 'Content-Type: application/json' -d '{"full_recompute": true}'
```

Expected (abridged): HTTP 200, `status: "completed"`, and the node counts.

```json
{
  "calc_run_id": "…",
  "scenario_id": "1b9d…-fork-uuid",
  "status": "completed",
  "nodes_recalculated": 294000,
  "nodes_unchanged": 0,
  "message": "Full recompute: 294000 nodes recalculated"
}
```

A `status: "locked"` response means another calc run holds the per-scenario
advisory lock — wait and retry. `nodes_recalculated` should match
`pi_nodes_created` from step 2 (every PI node was marked dirty).

---

## Step 4 — Record the metrics

Take the propagation throughput for `docs/SCALABILITY.md`. From step 3's response
you have `nodes_recalculated`; the wall time is in the API log line
(`calc_run … completed`), or measure the curl round-trip:

```bash
/usr/bin/time -p curl -sS -X POST "$OOTILS_URL/v1/calc/run?scenario_id=$FORK" \
  -H "$AUTH" -H 'Content-Type: application/json' -d '{"full_recompute": true}' >/dev/null
```

Record, for `docs/SCALABILITY.md` § "Measured perf landscape":

- fork id, subset mode, `scope_items_after_bom_closure`, `pairs_in_scope`,
  `horizon_days`, `pi_nodes_created` (from `BOOTSTRAP_METRICS`),
- `nodes_recalculated` and wall seconds → **nps** (nodes/second),
- how many `shortages` the fork now holds:

```bash
psql "$DATABASE_URL" -c \
  "SELECT COUNT(*) FROM shortages WHERE scenario_id = '$FORK' AND status = 'active';"
```

This is the first real pilot-scale propagation data point — it belongs next to the
synthetic `scripts/bench_propagation.py` figures in SCALABILITY.md, clearly
labelled as pilot-subset (not baseline, not synthetic).

---

## Step 5 — Verify the what-if surface reads from the fork

Run the wedge runbook with step 7 pointed at the fork. Every other step is
unchanged; step 7 forks the FORK (fork-on-fork), applies a node override on a PI
node **in the fork**, recomputes, and reports an honest shortage delta — proving
the pilot fork is queryable and forkable end to end without touching baseline.

```bash
OOTILS_API_TOKEN="$OOTILS_API_TOKEN" python scripts/demo_e2e.py \
  --dsn "$DATABASE_URL" \
  --skip-watchers \
  --whatif-base-scenario "$FORK"
```

Expected: step 7 prints the base fork it used and a `[PASS]`:

```
STEP 7 — Forkable what-if
  What-if base   : 1b9d…-fork-uuid (fork-on-fork; baseline untouched)
  What-if coord  : item=… location=… (direction: relax stock)
  Fork           : …  name=demo-e2e-whatif-…
  Propagation    : status=ok delta_computed=True
  Honest delta   : new_shortages=… resolved_shortages=… net=…
  Fork archived  : yes (status=archived)
[PASS] step 7 (Forkable what-if)  fork … delta new=…/resolved=…, …, base=1b9d…
```

`--skip-watchers` keeps the run fast (the shortage watcher over a large item base
takes minutes). If step 7 SKIPs with "no ProjectedInventory node on base scenario
… to fork", step 2/3 did not populate the fork — re-check them.

---

## Step 6 — Archive the pilot fork

`DELETE /v1/scenarios/{id}` sets `status = 'archived'` (TTL pattern — never a hard
delete). The demo's own step 7 already archives its short-lived child fork; this
archives the pilot fork itself once the metrics are recorded.

```bash
curl -sS -o /dev/null -w '%{http_code}\n' \
  -X DELETE "$OOTILS_URL/v1/scenarios/$FORK" -H "$AUTH"
```

Expected: `204`. Confirm:

```bash
psql "$DATABASE_URL" -c \
  "SELECT status FROM scenarios WHERE scenario_id = '$FORK';"   -- archived
```

The fork's nodes/edges/shortages stay in the DB under the archived scenario (an
audit trail), out of the baseline working set. Baseline was never touched at any
step.
