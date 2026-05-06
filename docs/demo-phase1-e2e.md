# Phase 1 E2E Demo — Forecast → MPS → MRP → CRP → ATP

This demo proves the user-facing Phase 1 planning chain through real FastAPI routers and PostgreSQL:

1. Seed deterministic finished-good, plant, historical demand, work-center, routing, and operation data.
2. Generate statistical demand forecast: `POST /v1/demand/forecast/generate`.
3. Aggregate forecast into MPS: `POST /v1/mps/aggregate-demand`.
4. Approve the MPS node for deterministic demo setup, then promote to MRP planned supply: `POST /v1/mps/{mps_id}/promote-to-mrp`.
5. Calculate CRP load from released planned supply: `POST /v1/crp/calculate`.
6. Check ATP for a customer quantity/date request: `POST /v1/atp/check`.

The approval step is currently SQL-driven because an approval workflow endpoint is outside the current scope. All planning calculations still go through the real API routers.

## Run on VM 201 with disposable DB

```bash
cd ~/ootils-core
set -a && . ./.env && set +a

docker compose exec -T postgres dropdb -U "$POSTGRES_USER" --if-exists ootils_demo_phase1
docker compose exec -T postgres createdb -U "$POSTGRES_USER" ootils_demo_phase1

DATABASE_URL="postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:5432/ootils_demo_phase1" \
OOTILS_API_TOKEN="$OOTILS_API_TOKEN" \
python3 scripts/demo_phase1_e2e.py
```

Use `--json` for compact machine-readable output.

## Expected output shape

```json
{
  "status": "ok",
  "item_external_id": "DEMO-FG-...",
  "location_external_id": "DEMO-PLANT-...",
  "forecast": {
    "buckets": 21,
    "total_quantity": "...",
    "method": "MA"
  },
  "mps": {
    "mps_nodes_created": 3,
    "total_demand": "...",
    "first_mps_id": "..."
  },
  "mrp_promotion": {
    "status": "RELEASED",
    "planned_supplies_created": 1
  },
  "crp": {
    "planned_orders_count": 1,
    "work_centers_count": 1,
    "load_profiles": 1
  },
  "atp": {
    "requested_quantity": "5",
    "quantity_available": "...",
    "buckets": 30
  }
}
```

## Demo positioning

This is the first clean product proof for Phase 1: Ootils can take historical demand, forecast it, turn it into a master schedule, release planned supply, compute capacity load, and answer whether a customer request can be promised.
