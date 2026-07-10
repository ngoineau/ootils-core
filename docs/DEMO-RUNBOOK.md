# Ootils Demo Runbook — the wedge, end to end

> Status: executable milestone (#408)
> Audience: the pilot running a live demo in front of a prospect
> Companion script: `scripts/demo_e2e.py` (the executable twin of this document)

This runbook walks the **full wedge** — *"autonomous shortage control tower with
scenario-backed recommendations"* — from a cold database to a chiffered proof of
value, in one command. Every step is a real product surface (the same FastAPI
routers a customer would call, plus the same in-process agents the fleet runs).
Nothing here fabricates supply-chain truth; every number comes from the engine.

It runs unchanged on two databases:

- the **pilot base** (36k+ items, 19 locations, ~3.76 M booking rows) — the live
  story, and
- the **CI-seeded base** (`scripts/seed_demo_data.py`) — the automated check
  (VALVE-02 shortage, XFER-01 DC-ATL → DC-LAX transfer opportunity).

It is **non-destructive, absolute**: no `DELETE` / `TRUNCATE` / destructive
`UPDATE`. Only the product's own normal writes happen (two demo tokens, one
distribution link if missing, DRAFT recommendations, inventory snapshots,
outcomes, one archived fork). The **DSN is never printed** — every log line and
the optional scoreboard show the database *name* only.

---

## 0. Prerequisites

```bash
# 1. A reachable PostgreSQL 16 database whose name starts with "ootils"
#    (the demo refuses any other target; ootils_dev needs --allow-dev).
#    Example targets: ootils_pilote_test (pilot), ootils_test (CI).

# 2. The API token the app itself requires to boot. Any strong secret.
export OOTILS_API_TOKEN='choose-a-strong-secret'

# 3. The source tree on the path (this milestone runs from the worktree).
export PYTHONPATH="$PWD/src"

# 4. Dependencies installed (dev extra):
pip install -e ".[dev]"
```

Notes:

- The demo **applies any pending migrations at boot** (step 0). The pilot base
  sits at migration `060`; the demo catches it up to `069` before doing anything
  else, and prints the schema version before → after.
- The forecast step needs a `[forecast]` extra for the richest models; without
  it, `AUTO_SELECT` still resolves to a deterministic baseline model. If no
  `demand_history` exists at all, the step **SKIPs honestly**.

---

## 1. The single command

```bash
# Pilot base — full run (the watcher over 36k items takes a few minutes):
OOTILS_API_TOKEN=... PYTHONPATH="$PWD/src" \
  python scripts/demo_e2e.py --dsn "postgresql://.../ootils_pilote_test"

# Fast pass (skip the long watcher; add the read-only MRP bench; save a scoreboard):
OOTILS_API_TOKEN=... PYTHONPATH="$PWD/src" \
  python scripts/demo_e2e.py --dsn "postgresql://.../ootils_pilote_test" \
      --skip-watchers --bench --out scoreboard.json

# Reveal the minted token cleartext for a subsequent manual (curl) demo:
OOTILS_API_TOKEN=... PYTHONPATH="$PWD/src" \
  python scripts/demo_e2e.py --dsn "postgresql://.../ootils_test" --show-tokens
```

The command prints a narrative per step, ends with a **scoreboard**, and exits
`0` when no step failed (a `SKIP` is not a failure). Each step reports
`PASS` / `SKIP(reason)` / `FAIL`; a `FAIL` never aborts the run — except step 0
(no database, no demo).

Flags:

| Flag | Effect |
|------|--------|
| `--dsn` (required) | Target database (never printed). |
| `--allow-dev` | Permit an `ootils_dev` target (semi-prod guard). |
| `--skip-watchers` | Skip the shortage watcher (minutes on a large item base). |
| `--bench` | Also run the MRP perf bench (read-only subprocess). |
| `--out <path>` | Write a scoreboard JSON (DSN masked to the db name). |
| `--show-tokens` | Print minted token cleartext (operator escape hatch). |
| `--verbose` | Full tracebacks on a step FAIL. |

---

## 2. Step by step — what you see, what it proves, the curl equivalent

Throughout, `$T` is a valid Bearer token (the `OOTILS_API_TOKEN`, or a minted
`ootk_…` token — pass `--show-tokens` to capture the ones the demo created).

### Step 0 — Boot & migration catch-up

**You see:** the database name (masked), the schema version before → after, and
a read-only inventory (items, locations, locations with on-hand, demand_history
rows, recommendations, active shortages).

**Proves:** the platform self-migrates to the current schema on startup — no
manual DDL, no "run these 9 scripts first". The prospect's own database is
brought current and inventoried without a single mutation.

```bash
# The inventory is the app's own health surface; the schema version is:
psql "$DSN" -c "SELECT max(version) FROM schema_migrations"
```

### Step 1 — Governed tokens (cryptographic identity)

**You see:** two tokens minted — an **agent** (`read`, `recommend:draft`) and a
**human** (`read`, `ingest`, `recommend:draft`, `recommend:approve`) — named
`DEMO-E2E-agent` / `DEMO-E2E-human`, shown by non-secret prefix only (cleartext
hidden unless `--show-tokens`). An auth probe confirms the agent token works.

**Proves (vs Kinaxis / o9):** the actor's *kind* is a property of the
**credential**, set at issuance, read back cryptographically — not a field a
caller self-declares. This is the substrate that makes the L3 human-only gate
(step 5) genuinely enforceable. Legacy APS bolt-on "AI" trusts whatever the
request claims; Ootils does not.

```bash
# Tokens are minted directly in the api_tokens registry (only the hash is
# stored; cleartext is shown once). To find/revoke everything this demo made:
psql "$DSN" -c "SELECT name, actor_kind, token_prefix, scopes
                FROM api_tokens WHERE name LIKE 'DEMO-E2E-%'"
```

### Step 2 — Forecast + FVA (honest accuracy)

**You see:** the richest booking-based series discovered automatically, a
Pyramide forecast run on it, then **WAPE**, **MASE**, the real **FVA**
(`naive_wape − wape`; positive = the model beats a trivial seasonal-naive), and
whether **conformal confidence intervals** are present.

**Proves:** the forecast carries its own *proof it is worth anything* — FVA is
the honest "did we beat doing nothing?" number, un-clamped (a negative FVA is
reported as-is, never hidden). The confidence intervals are conformal
(finite-sample calibrated), and a stale-demand run is flagged, never silently
trusted. This is forecast **value**, not a vanity accuracy figure.

```bash
RUN=$(curl -s -X POST "$BASE/v1/forecast/runs" \
  -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  -d '{"item_id":"PUMP-01","location_id":"DC-ATL","horizon_days":90,
       "granularity":"weekly","method":"AUTO_SELECT"}' | jq -r .run_id)
curl -s "$BASE/v1/forecast/runs/$RUN" -H "Authorization: Bearer $T" \
  | jq '.accuracy_metrics[] | select(.horizon==null)
        | {wape, mase, naive_wape, fva_wape}'
curl -s "$BASE/v1/forecast/runs/$RUN/result" -H "Authorization: Bearer $T" \
  | jq '.values[0] | {confidence_lower, confidence_upper}'
```

*SKIPs* honestly if no booking-based `demand_history` series exists.

### Step 3 — Governed watchers (scenario-backed)

**You see:** the shortage watcher run in-process; the DRAFT recommendations it
emitted, counted by decision level.

**Proves (vs Kinaxis / o9):** the agent does not just flag a shortage — it drafts
a **governed** action (never applied), validated by a **counter-factual fork**
(#340: one what-if scenario per run, archived at the end), with a decision level
derived from the action, and a full evidence trail. Recommendations without
evidence are rejected by governance. APS "alerts" are dashboards; these are
draft *decisions* with provenance.

```bash
# The watcher writes DRAFT recommendations; read them back:
curl -s "$BASE/v1/recommendations?status=DRAFT" \
  -H "Authorization: Bearer $T" | jq '.recommendations[] | {action, decision_level, item_external_id}'
```

*SKIPs* under `--skip-watchers` (minutes on a large item base).

### Step 4 — DRP inter-site transfer

**You see:** a real transfer lane discovered (a stocked source, a linked deficit
destination), then `POST /v1/drp/run` emitting governed **TRANSFER** DRAFTs, with
the fair-share split and logistic rounding visible in each reco's evidence. On
the CI base this drafts the XFER-01 DC-ATL → DC-LAX transfer of **180** units out
of the box.

**Proves:** distribution is **per-site, deterministic, and idempotent** — a
re-run of an unchanged plan emits *zero* new rows (the run reports the idempotent
no-op count). Fair-share and case-rounding are explainable, not a black box.

```bash
curl -s -X POST "$BASE/v1/drp/run" -H "Authorization: Bearer $T" \
  -H 'Content-Type: application/json' -d '{"horizon_days":180}' \
  | jq '{signals, recommendations_emitted, recommendations_idempotent_noop, decision_level}'
```

### Step 5 — Cryptographic governance gate (the #392 demonstration)

**You see:** a DRAFT reco moved DRAFT → REVIEWED by the **agent** (agents may
review), then:

- the **agent** token attempts REVIEWED → APPROVED → **403** (with the gate's
  own message printed), *even though its request body lies `actor_kind: "human"`*;
- the **human** token performs REVIEWED → APPROVED → **200**.

**Proves (vs Kinaxis / o9):** the irreversible-approval gate reads the actor kind
from the **token**, never the payload. A compromised or buggy agent cannot
self-declare its way past a human-only decision. This is the single most
important governance guarantee for autonomous operations — and it is enforced,
not documented.

```bash
# Agent (recommend:draft) can review; cannot approve:
curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  "$BASE/v1/recommendations/$RECO/transition" -H "Authorization: Bearer $AGENT_T" \
  -d '{"to_status":"APPROVED","actor":"demo-agent","actor_kind":"human"}'   # -> 403
# Human (recommend:approve) approves:
curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  "$BASE/v1/recommendations/$RECO/transition" -H "Authorization: Bearer $HUMAN_T" \
  -d '{"to_status":"APPROVED","actor":"demo-human","actor_kind":"human"}'    # -> 200
```

*SKIPs* if steps 3 and 4 produced no DRAFT to govern.

### Step 6 — The proof machine (snapshots → outcomes → 5 KPIs)

**You see:** inventory snapshots captured (human token, `ingest` scope), a
recommendation-outcome evaluation pass, then the **five proof KPIs**, each
**NULL-honest** (`n/a (no data)` for NULL, never a misleading 0), with their
basis counts:

1. `pct_shortages_avoided`
2. `avoided_severity_usd_total`
3. `avg_fva_wape` (the real forecast value added)
4. `reco_approval_rate`
5. `cost_of_inaction_usd`

**Proves (vs Kinaxis / o9):** value is proven with **deterministic facts**, never
a narrative — an explicit refusal of any LLM in the proof-scoring path. A NULL
KPI says "no data yet", not "zero" — the demo never inflates its own score. This
is the closed value-proof loop APS tools cannot show because they never chain a
recommendation to its observed outcome.

```bash
curl -s -X POST "$BASE/v1/snapshots"          -H "Authorization: Bearer $T" -d '{}'
curl -s -X POST "$BASE/v1/outcomes/evaluate"  -H "Authorization: Bearer $T" -d '{}'
curl -s "$BASE/v1/outcomes/summary" -H "Authorization: Bearer $T" | jq '{
  pct_shortages_avoided, avoided_basis_count,
  avoided_severity_usd_total, avg_fva_wape, fva_basis_count,
  reco_approval_rate, reco_total_count, cost_of_inaction_usd }'
```

### Step 7 — Forkable what-if (honest delta)

**You see:** a real active-shortage coordinate; a **fork** of baseline via
`POST /v1/simulate` with a node override, recomputed to an **honest shortage
delta** (`new` / `resolved`, with a `delta_computed` freshness flag); a
**planning-param overlay** (`safety_stock_qty`) attached to that same fork via
`POST /v1/scenarios/{fork}/param-overrides` and listed back (#347 — 15
whitelisted fields are forkable via REST); then the fork **archived**
(`status='archived'`, never deleted).

**Proves (vs Kinaxis / o9):** every counter-factual runs in a **fork**, never on
baseline; the delta is honest (a failed recompute is surfaced as such, never as
an empty "no change"); overlay overrides are **L0 simulation-only** and never
promoted onto baseline. Agents test alternatives without touching the source of
truth — the core capability an "agent-piloted substrate" requires.

```bash
FORK=$(curl -s -X POST "$BASE/v1/simulate" -H "Authorization: Bearer $T" \
  -d '{"scenario_name":"demo-whatif","base_scenario_id":"baseline",
       "overrides":[{"node_id":"'"$PI"'","field_name":"opening_stock","new_value":"100000"}]}' \
  | jq -r .scenario_id)
curl -s -X POST "$BASE/v1/scenarios/$FORK/param-overrides" -H "Authorization: Bearer $T" \
  -d '{"item_id":"'"$ITEM"'","location_id":"'"$LOC"'",
       "field_name":"safety_stock_qty","value":"0","applied_by":"demo"}'
curl -s -o /dev/null -w '%{http_code}\n' -X DELETE \
  "$BASE/v1/scenarios/$FORK" -H "Authorization: Bearer $T"   # -> 204 (archived)
```

When baseline has no active shortage (e.g. a pilot base with no recent calc
run), the step **falls back** to the richest ProjectedInventory coordinate and
runs the counter-factual in the *other* direction — `opening_stock="0"`
("what if we had less stock?"), an equally honest delta of **new** shortages
instead of resolved ones. *SKIPs* only if the base has no PI node at all.
Note: on a large base the fork is a full deep-copy of baseline — budget
minutes, not seconds, for this step on 200K+ nodes.

### Step 8 — Scenario compare (SC-1: 2 forks ranked in $ side by side)

**You see:** `GET /v1/scenarios/compare?ids=<baseline>,<fork>` run on baseline
and the step 7 what-if fork — a mini side-by-side table (shortage count,
severity $, stock $, fill rate, stale) for each scenario, the deltas vs the
reference scenario, and a **ranking in $** (ascending severity, lower $
exposure first). *SKIPs* honestly when step 7 produced no fork. Read-only
(`read` scope) — no write, no event.

**Proves (vs Kinaxis / o9):** counter-factuals are not just computable, they are
**comparable** — two forks, ranked by $ exposure, side by side, in one call.

```bash
curl -s "$BASE/v1/scenarios/compare?ids=$BASELINE,$FORK" -H "Authorization: Bearer $T"
```

### Step 9 — StreamChanges (replayable by cursor)

**You see:** a **bounded** replay of everything the demo just did —
`GET /v1/stream?once=true&cursor=0` drains the baseline event history once and
closes — with the event count and the last `stream_seq`.

**Proves (vs Kinaxis / o9):** this is the *"designed for AI"* proof — every state
change is a typed event on a monotonic, **replayable** stream. Agents subscribe
from a cursor and never poll; a reconnecting agent resumes exactly where it left
off. There is no equivalent "give me every change since sequence N" in a
dashboard-first APS.

```bash
curl -s "$BASE/v1/stream?once=true&cursor=0" -H "Authorization: Bearer $T"
# SSE frames: id: <stream_seq> / event: <type> / data: <json>
```

### Step 10 — MRP bench (optional, `--bench`)

**You see:** per-phase MRP cascade timings (load / consume / time-phased / peg)
from `scripts/bench_mrp.py`, run in a **read-only** subprocess.

**Proves:** the deterministic core is fast enough for interactive what-if at
scale — and it is measured, not asserted. The bench opens a `READ ONLY`
transaction, so it cannot mutate the pilot base.

---

## 3. Artefacts created (exhaustive) — how to find them

Everything below is a **normal product write**; nothing is destructive. To
inspect or clean up after a demo:

| Artefact | How to identify | Notes |
|----------|-----------------|-------|
| 2 API tokens | `api_tokens WHERE name LIKE 'DEMO-E2E-%'` | Cleartext never stored; revoke with `UPDATE api_tokens SET revoked_at = now() WHERE name LIKE 'DEMO-E2E-%'`. |
| ≤1 distribution link | reused if one already exists; only created when a real stocked lane needs it, tagged `DEMO-E2E` | Additive; `distribution_links` is upserted `ON CONFLICT`. On the CI base the seed's XFER-01 link is reused. |
| DRAFT recommendations | `recommendations WHERE status IN ('DRAFT','REVIEWED','APPROVED')` from this run's agents | Deterministic ids (uuid5) — a re-run does not duplicate. One reco is moved to APPROVED by step 5. |
| Inventory snapshots | `inventory_snapshots WHERE source='api'` for today's `as_of_date` | Idempotent upsert per (scenario, item, location, day). |
| Recommendation outcomes | `recommendation_outcomes` for today's `evaluated_as_of` | Idempotent upsert per (recommendation, day). |
| 1 archived fork | `scenarios WHERE name LIKE 'demo-e2e-whatif-%' AND status='archived'` | Never deleted (TTL pattern); carries its param-overlay override as evidence. |
| Scoreboard JSON | the `--out` path, if given | Contains the db **name** only — no DSN. |

The forecast, snapshot, outcome, and event rows are the product's normal
operating data; they are safe to leave in place.

---

## 4. Caveats (read these before a live demo)

- **Step 0 catches up migrations.** On the pilot base (at `060`) this applies
  `061`–`069` on the first run. It is idempotent, but budget a few extra seconds
  the first time.
- **The watcher takes minutes on a large item base.** Over 36k items,
  `agent_shortage_watcher` is not instant. Use `--skip-watchers` for a fast
  walkthrough; step 5 then governs a DRAFT from the DRP step instead, and steps
  6–9 still run. On the CI base the watcher is quick.
- **Forecast SKIPs without history — and names the mapping gap.** If the target
  has no booking-based `demand_history`, step 2 reports `SKIP` (no series)
  rather than inventing one. The CI-seeded base *does* have history
  (PUMP-01@DC-ATL, 90 days) so it forecasts. **Pilot caveat (🎯 data
  onboarding):** on `ootils_pilote_test`, `demand_history.warehouse_id` carries
  ERP numeric DC codes (`'87'`, `'286'`, …) that were never mapped to
  `locations.external_id` (alpha codes `'DAL'`, `'CAN'`, …) — millions of
  bookings, zero forecastable series. The SKIP diagnostic prints the exact
  counts. Mapping those codes into the locations registry is a pilot business
  decision, not something the demo fabricates.
- **The demo mints fresh token secrets each run.** A live token of the same name
  cannot be recovered (only its hash is stored), so a re-run mints a new secret
  and leaves the prior row for the operator to revoke. Use the `DEMO-E2E-%`
  query above to keep the registry tidy.
- **The DSN is never printed.** If you need the target on screen, it is shown as
  the database *name* only — by design. Credentials and host never appear in any
  output or in the scoreboard artefact.
- **Target guard.** The demo refuses any database whose name does not start with
  `ootils`, and refuses `ootils_dev` (semi-prod) without `--allow-dev` — the same
  guard the CLIs use.
