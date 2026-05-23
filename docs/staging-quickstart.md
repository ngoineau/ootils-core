# Staging pipeline — quickstart

This guide walks an integrator through the four-step workflow that pushes data from an external system (ERP, WMS, MES, planning tool, Excel-from-ops) into the Ootils canonical tables, with full audit and a safety net before anything irreversible happens.

> **Architecture reference**: see [ADR-013](ADR-013-external-interfaces.md) for the design rationale and [ADR-009](ADR-009-import-pipeline.md) for the DQ pipeline.

## Lifecycle in one diagram

```
file               POST /upload         run_dq()              GET /diff
─────  ───────────────────────────►  ─────────►   ─────────────────────►
                 status=pending       L1-L4 →     preview impact +
                                     validated   20% deletion guard
                                          │
                                          │  POST /approve  →  status=imported
                                          │                    canonical tables updated
                                          │                    transform_runs audit row
                                          │
                                          └──  POST /reject   →  status=rejected
                                                                 reason persisted
```

## Step 1 — Upload the file

Supported formats: `TSV` (recommended), `CSV`, `XLSX`, `JSON`. Auto-detect on extension + content sniff. Max 50 MB.

```bash
curl -X POST "${OOTILS_API_URL}/v1/staging/upload" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}" \
  -F "file=@items_sap_2026w22.tsv" \
  -F "entity_type=items" \
  -F "source_system=SAP-EU" \
  -F "notes=weekly master refresh"

# Response 202:
# {
#   "upload_id": "...",
#   "batch_id": "abc12345-...",
#   "status": "pending",
#   "rows_inserted": 4827,
#   "format": "tsv",
#   "encoding": "utf-8",
#   "headers": ["external_id", "name", "item_type", "uom", "status"],
#   "sha256": "...",
#   "file_size_bytes": 312456
# }
```

The batch lands in status `pending` — no DQ run yet, no canonical write.

**Supported entity types**: `items`, `locations`, `suppliers`, `supplier_items`, `purchase_orders`, `customer_orders`, `forecasts`, `work_orders`, `transfers`, `on_hand`. See [`docs/staging-templates/`](staging-templates/) for the per-entity column specs.

## Step 2 — Run the DQ pipeline

Today this is a Python-side call. A future worker / event-driven runner will do it automatically after `/upload`.

```python
from ootils_core.engine.dq.engine import run_dq
import psycopg

with psycopg.connect(OOTILS_DSN) as conn:
    result = run_dq(conn, batch_id)
    print(result.batch_dq_status)  # 'validated' or 'rejected'
    for issue in result.issues:
        print(f"  {issue.severity}: {issue.rule_code} on row {issue.row_number}")
```

What runs:
- **L1 structural** — types, mandatory fields, max lengths
- **L2 referential** — FK presence in `items` / `locations` / `suppliers`
- **L3 business** — 15 rule families (status enum, lead_time>0, MOQ≤MAX, date ranges, ...)
- **L4 cross-batch** — duplicate external_id, inter-batch collision, supplier inactive, orphan items

If any `severity='error'` issue fires, `dq_status='rejected'` and the batch can't be approved. Look at `data_quality_issues` to see which rows broke and why.

If only warnings, `dq_status='validated'` — proceed.

> The DQ engine flips `dq_status` on the batch row. **Promoting `status` from `pending` to `validated` is currently a separate UPDATE** (an orchestrator concern, e.g. a worker that owns the DQ run). For ad-hoc testing, do it manually:
>
> ```sql
> UPDATE ingest_batches SET status='validated' WHERE batch_id='abc12345-...';
> ```

## Step 3 — Review the diff (the safety net)

```bash
curl "${OOTILS_API_URL}/v1/staging/batches/${BATCH_ID}/diff" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}"

# {
#   "supported": true,
#   "counts": {
#     "in_batch": 4827,
#     "in_canonical_for_source": 4901,
#     "will_insert": 23,
#     "will_update": 156,
#     "will_noop": 4648,
#     "will_soft_delete": 74
#   },
#   "samples": {
#     "will_insert":      ["SAP-001", ...],  // up to 10
#     "will_update":      ["SAP-042", ...],  // up to 10
#     "will_soft_delete": ["SAP-OBS-001", ...]
#   },
#   "deletion_guard": {
#     "ratio": 0.0151,        // 1.5%
#     "threshold": 0.20,      // 20%
#     "exceeds_threshold": false
#   }
# }
```

**`exceeds_threshold=true` is the canary**. If you see this, the batch would soft-delete more than 20% of the source's existing footprint — almost always a truncated export or a scope-reduction accident. Inspect `will_soft_delete` samples and the upstream export before approving with `force`.

## Step 4a — Approve

```bash
curl -X POST "${OOTILS_API_URL}/v1/staging/batches/${BATCH_ID}/approve" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "approved_by": "ops@example.com",
    "notes": "Weekly SAP master refresh, validated with planning",
    "force": false
  }'

# 200:
# {
#   "run_id": "...",
#   "counts": {
#     "rows_inserted": 23,
#     "rows_updated": 156,
#     "rows_soft_deleted": 74,
#     "rows_noop": 4648
#   },
#   "forced_approval": false,
#   "duration_seconds": 0.124
# }
```

If `deletion_guard.exceeds_threshold` was true and you got `400 Bad Request`, retry with `"force": true` AND include a clear justification in `notes`. The `forced_approval` flag is stored in `staging.transform_runs.forced_approval` for audit forever.

What happens behind the scenes (single SAVEPOINT, all-or-nothing):
- UPSERT each batch row into the canonical table (insert if new, update if existing)
- Upsert the corresponding `external_references` mapping
- Soft-delete missing rows: for `items`, set `status='obsolete'`; for `suppliers`, `status='inactive'`; for `locations`, only remove the mapping (no status column)
- Mark batch `status='imported'`, set `imported_at`
- Create the `staging.transform_runs` audit row

If anything fails: rollback, batch stays `validated`, audit row marked `failed` with error_message. Operator can retry after fixing the underlying issue.

## Step 4b — Reject

```bash
curl -X POST "${OOTILS_API_URL}/v1/staging/batches/${BATCH_ID}/reject" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "rejected_by": "ops@example.com",
    "reason": "/diff showed unexpected 60% deletion ratio — source export was truncated"
  }'
```

Permanent decision — `ingest_rows` stay around for forensics, but the batch cannot be revived. To retry, re-upload as a new batch.

## Quick observability

```sql
-- List all open batches (not yet imported / rejected)
SELECT batch_id, entity_type, source_system, status, dq_status,
       submitted_at, total_rows
FROM ingest_batches
WHERE status NOT IN ('imported', 'rejected')
ORDER BY submitted_at DESC;

-- See what changed in canonical from a given import
SELECT run_id, batch_id, status, approved_by,
       rows_inserted, rows_updated, rows_soft_deleted, rows_noop,
       forced_approval, completed_at
FROM staging.transform_runs
ORDER BY started_at DESC LIMIT 20;

-- DQ issues for a specific batch
SELECT row_number, dq_level, rule_code, severity, field_name, message
FROM data_quality_issues
WHERE batch_id = 'abc12345-...'
ORDER BY row_number, dq_level;
```

## Common patterns by integrator

| Source | Cadence | Typical entity types | Template start |
|--------|---------|----------------------|----------------|
| SAP / Oracle ERP nightly export | daily | items, suppliers, supplier_items, customer_orders, purchase_orders | items.tsv |
| MES / shop-floor system | hourly | work_orders | work_orders.tsv |
| WMS / inventory snapshot | daily | on_hand, transfers | on_hand.tsv |
| Demand planning tool | weekly | forecasts | forecasts.tsv |
| Manual Excel from ops | ad-hoc | various | the relevant template |

## Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| 400 `unknown entity_type` | typo in form field — see catalogue in [staging-templates/README.md](staging-templates/README.md) |
| 400 `parse error: invalid JSON` / `header line contains an empty column` | file structure issue — open in a viewer to check |
| `cp1252 (non-UTF-8)` in response `encoding` | source exports legacy encoding — works but ask for UTF-8 in the long run |
| 400 `deletion ratio exceeds threshold` | scope reduction — investigate before passing `force=true` |
| DQ `L2_UNKNOWN_REF` errors | dependency entity (item / location / supplier) hasn't been imported yet — push that first |
| DQ `L3_INVALID_*_STATUS` | source's status values don't match the canonical enum — see template for valid values |
| `L4_DUPLICATE_EXTERNAL_ID` | export produced the same external_id twice — fix at source, re-upload |

## See also

- [ADR-013](ADR-013-external-interfaces.md) — design decisions for the staging contract
- [ADR-009](ADR-009-import-pipeline.md) — the 2-step staging architecture
- [staging-templates/](staging-templates/) — per-entity file format contracts
