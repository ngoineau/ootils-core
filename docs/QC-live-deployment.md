# QC-live-deployment.md — Ootils V1 Live Deployment QC Checklist

**Date:** 2026-04-05  
**Reviewer:** QC/DevOps (Claw subagent)  
**Branch:** `live/v1-bootstrap` — `github.com/ngoineau/ootils-core`  
**Target:** Proxmox VM 201, Debian 12, Docker stack (PostgreSQL + FastAPI)  
**Status:** ⛔ BLOCKED — 4 BLOCKERs must be resolved before go-live

---

## 🔴 BLOCKERS SUMMARY (Resolve Before Starting)

| # | BLOCKER | Impact | Fix |
|---|---------|--------|-----|
| B1 | `python -m ootils_core.db.migrate` **does not exist** | Step 7 will fail with `ModuleNotFoundError` | Remove step 7; migrations auto-run at API startup via `OotilsDB.__init__()` |
| B2 | `001_initial_schema.sql` contains SQLite-only syntax (`PRAGMA`, `strftime`, `INSERT OR IGNORE`) | Fresh PG migration crashes; `connection.py` error handler only swallows `"already exists"` → re-raises | Fix 001 to be a true no-op (it's labeled deprecated; replace with `SELECT 1;` only) |
| B3 | `docker-compose.yml` pins `postgres:15-alpine` — plan says PostgreSQL 16 | Version mismatch between declared architecture and actual deployment | Update compose file to `postgres:16-alpine` OR explicitly decide 15 is acceptable and update documentation |
| B4 | Seed script `seed_demo_data.py` **not idempotent on re-run** | Each run generates new UUIDs → `ON CONFLICT DO NOTHING` on item_id/location_id won't fire → duplicate rows accumulate → `/v1/issues` returns multiplied results | Add `SELECT` lookups before INSERT or add `UNIQUE` constraints on `items.name`, `locations.name` |

---

## PRE-FLIGHT CHECKS (Before Touching Proxmox)

### Infrastructure
- [ ] **IP free:** Confirm `192.168.x.y` (target static IP) is not in ARP table — `ping -c1 <target-ip>` returns no response
- [ ] **VM ID free:** `qm list` on Proxmox host — VM 201 does not exist
- [ ] **Proxmox disk space:** `df -h /var/lib/vz` ≥ 40 GiB free (32 GiB disk + cloud image + overhead)
- [ ] **SSH key on Proxmox host:** `ls ~/.ssh/id_*.pub` returns a key — copy path for cloud-init
- [ ] **Network reachability:** Proxmox host can reach `github.com` — `curl -s https://github.com > /dev/null && echo OK`
- [ ] **Debian cloud image not already cached:** `ls /var/lib/vz/template/iso/ | grep debian-12` — note path if exists to skip re-download
- [ ] **Port 8000 not in use** on target IP (confirm nothing else is bound)
- [ ] **Port 5432 exposure decision:** Confirm whether Postgres port 5432 is exposed to LAN or localhost-only (security decision)

### Repository
- [ ] **Branch exists on remote:** `git ls-remote https://github.com/ngoineau/ootils-core refs/heads/live/v1-bootstrap` returns a commit hash
- [ ] **Known BLOCKERs resolved:** Confirm B1–B4 above are fixed in branch before deployment

---

## STEP-BY-STEP VALIDATION

---

### Step 1 — Download Debian 12 Cloud Image

**Command:**
```bash
wget https://cloud.debian.org/images/cloud/bookworm/latest/debian-12-genericcloud-amd64.qcow2 \
  -O /var/lib/vz/template/iso/debian-12-genericcloud-amd64.qcow2
```

**Success criterion:**
- [ ] File exists and is ≥ 300 MB: `ls -lh /var/lib/vz/template/iso/debian-12-genericcloud-amd64.qcow2`
- [ ] SHA256 matches Debian published checksum (download `SHA256SUMS` from same URL path and verify)
- [ ] File is a valid QCOW2: `qemu-img info <path>` returns `file format: qcow2`

**Failure recovery:**
- Re-download; use `--continue` flag to resume partial download
- If mirror is slow: use a closer Debian mirror or CDN URL

---

### Step 2 — Create VM 201

**Command (minimal example):**
```bash
qm create 201 --memory 6144 --cores 2 --name ootils-v1 \
  --net0 virtio,bridge=vmbr0 \
  --scsihw virtio-scsi-pci \
  --scsi0 local-lvm:0,import-from=/var/lib/vz/template/iso/debian-12-genericcloud-amd64.qcow2 \
  --ide2 local-lvm:cloudinit \
  --boot c --bootdisk scsi0 \
  --agent enabled=1
```

**Then resize disk:**
```bash
qm disk resize 201 scsi0 32G
```

**Success criterion:**
- [ ] `qm list` shows VM 201 with status `stopped`
- [ ] `qm config 201` shows correct memory (6144), cores (2), disk configured
- [ ] Disk shows 32G: `qm config 201 | grep scsi0`

**Failure recovery:**
- If `qm create` fails: `qm destroy 201 --purge` and retry
- If disk resize fails: check available PVE storage — `pvesm status`

---

### Step 3 — Cloud-Init Configuration

**Command:**
```bash
qm set 201 \
  --ciuser debian \
  --sshkeys ~/.ssh/id_ed25519.pub \
  --ipconfig0 ip=<STATIC_IP>/24,gw=<GATEWAY> \
  --nameserver 1.1.1.1 \
  --cipassword ""
```

**Success criterion:**
- [ ] `qm config 201 | grep -E "ciuser|ipconfig|sshkeys"` shows correct values
- [ ] `qm config 201 | grep cipassword` — should show no password or blank

**Failure recovery:**
- Re-run `qm set` with corrected parameters; cloud-init config is stateless before first boot

---

### Step 4 — Start VM, Install qemu-guest-agent + Docker

**Start:**
```bash
qm start 201
```

**Wait for IP assignment (60–90s), then:**
```bash
ssh debian@<STATIC_IP>
```

**Install:**
```bash
sudo apt-get update
sudo apt-get install -y qemu-guest-agent
sudo systemctl enable qemu-guest-agent --now

# Docker (official method)
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker debian
# Log out and back in for group to apply
```

**Success criterion:**
- [ ] `qm agent 201 ping` from Proxmox host returns success
- [ ] `qm guest cmd 201 get-host-name` returns hostname
- [ ] `docker --version` on VM — Docker 24+ expected
- [ ] `docker compose version` — Compose v2 (plugin, not standalone)
- [ ] `docker run --rm hello-world` completes successfully
- [ ] `free -h` shows ~5.8 GiB available (sanity check RAM)
- [ ] `df -h /` shows ~30 GiB on root (disk resize applied)

**Failure recovery:**
- VM won't start: check Proxmox task log (`qm showcmd 201`)
- SSH timeout: verify cloud-init applied (`qm cloudinit dump 201`)
- Docker install fails: check APT proxy/firewall, try manual package install

---

### Step 5 — Clone Repo, Checkout `live/v1-bootstrap`

**Command:**
```bash
git clone https://github.com/ngoineau/ootils-core.git
cd ootils-core
git checkout live/v1-bootstrap
git log --oneline -3
```

**Success criterion:**
- [ ] Clone completes without error
- [ ] `git branch` shows `live/v1-bootstrap` checked out
- [ ] `git log --oneline -1` matches the expected commit hash (note it pre-deployment)
- [ ] `ls docker-compose.yml Dockerfile pyproject.toml` — all 3 present

**Failure recovery:**
- Auth error: use HTTPS with token or configure SSH key for GitHub
- Wrong branch: `git fetch origin && git checkout live/v1-bootstrap`

---

### Step 6 — `docker compose up --build -d`

**Command:**
```bash
docker compose up --build -d
```

**Watch logs immediately after:**
```bash
docker compose logs -f --tail=50
```

**Success criterion:**
- [ ] Build completes without error — look for `Successfully built <hash>` or `naming to docker.io/library/ootils-core-api`
- [ ] Both containers running: `docker compose ps` shows `postgres` and `api` both in `Up` / `running` state
- [ ] Postgres healthcheck passes: `docker compose ps` shows `healthy` for postgres (not `starting`)
- [ ] API startup log shows: `Application startup complete` and `Uvicorn running on http://0.0.0.0:8000`
- [ ] Migration log output visible: look for SQL execution in API logs (migrations apply at `OotilsDB.__init__()` — first request or startup)
- [ ] No `FATAL` or `ERROR` in postgres logs: `docker compose logs postgres | grep -i error`

**Failure recovery:**

| Failure | Recovery |
|---------|----------|
| Build fails (pip install) | Check network from VM; `docker compose build --no-cache` |
| Postgres unhealthy | `docker compose logs postgres` — check disk space, port conflict |
| API crashes immediately | `docker compose logs api` — look for `ImportError`, `ConnectionRefused` |
| Port 8000 already in use | `ss -tlnp | grep 8000` — kill conflicting process |
| API starts before PG ready | Should not occur — `depends_on: condition: service_healthy` is configured ✅ |

**Docker readiness note:** `docker-compose.yml` correctly uses `depends_on: postgres: condition: service_healthy` with a 10-retry healthcheck. The API will wait for Postgres to be ready. **No startup race condition.** ✅

---

### Step 7 — Apply Migrations

> ⚠️ **BLOCKER B1:** `python -m ootils_core.db.migrate` **does not exist as a runnable module.** There is no `__main__.py` in `ootils_core/db/`.

**Actual migration behavior:**
Migrations are applied automatically at API startup via `OotilsDB._apply_migrations()` in `connection.py`. They run when the first `OotilsDB()` instance is created (triggered by any API request or the `get_db` dependency).

**Correct validation command (replaces Step 7):**
```bash
# Force a migration pass by hitting health endpoint (triggers OotilsDB init)
curl -s http://<VM_IP>:8000/health

# Verify tables in Postgres
docker compose exec postgres psql -U ootils -d ootils_dev -c "\dt"
```

**Success criterion:**
- [ ] `\dt` output includes: `items`, `locations`, `nodes`, `edges`, `scenarios`, `calc_runs`, `shortages`, `explanations`, `zone_transition_runs`, `scenario_overrides`, `scenario_diffs`, `events`, `projection_series`
- [ ] No missing tables from the expected schema
- [ ] API logs show no migration errors

**Migration safety assessment:**
- Migrations use `IF NOT EXISTS` throughout (002–006) → **idempotent for DDL** ✅
- `connection.py` error handler swallows `"already exists"` errors → re-runs safe ✅
- **BLOCKER B2:** `001_initial_schema.sql` contains `PRAGMA journal_mode = WAL` — this crashes on fresh PostgreSQL. The handler does NOT swallow syntax errors → exception propagates → API fails to start on first deployment.
- No migration version table (no Alembic/Flyway) → no rollback tracking; ordering is purely filename-based

**Mid-migration failure:** If API crashes mid-migration on a fresh DB, some DDL may have been applied (autocommit mode). Recovery:
```bash
# Nuclear option — drop and recreate database
docker compose exec postgres psql -U ootils -c "DROP DATABASE ootils_dev;"
docker compose exec postgres psql -U ootils -c "CREATE DATABASE ootils_dev;"
# Then restart API
docker compose restart api
```

---

### Step 8 — Seed Data

**Command:**
```bash
docker compose exec api python scripts/seed_demo_data.py
```

**Success criterion:**
- [ ] Output ends with `✅ Seed complete.`
- [ ] All 6 sub-steps show `✓`:
  - `✓ Items: PUMP-01 (...), VALVE-02 (...)`
  - `✓ Locations: DC-ATL (...), DC-LAX (...)`
  - `✓ Projection series created`
  - `→ Creating PI nodes (90 days × 2 series)...`
  - `✓ Supply and demand nodes created`
  - `✓ Trigger event inserted`
- [ ] Verify in DB:
```bash
docker compose exec postgres psql -U ootils -d ootils_dev -c \
  "SELECT COUNT(*) FROM nodes; SELECT COUNT(*) FROM shortages; SELECT COUNT(*) FROM edges;"
```
Expected: `nodes` ≈ 182 (90+90+2+1 supply/demand), `shortages` ≈ 13 (8 pump + 5 valve), `edges` ≥ 2

**Seed idempotency assessment — ⚠️ WARNING:**
- Script uses `uuid4()` on every run → new PKs every time
- `ON CONFLICT DO NOTHING` on `items`, `locations`, `projection_series` fires only if there's a UNIQUE constraint conflict on PK (item_id)
- Since item_id is a new UUID each run, there is **no conflict** → duplicate rows will be inserted on re-run
- **Impact:** Second run doubles nodes, shortages, edges → `/v1/issues` returns duplicated results
- **Mitigation:** Run seed exactly once; if re-run needed, truncate first:
```bash
docker compose exec postgres psql -U ootils -d ootils_dev -c \
  "TRUNCATE shortages, edges, nodes, projection_series, locations, items, calc_runs, events CASCADE;"
```

---

### Step 9 — Validate

#### 9a. Health Check

```bash
curl -s http://<VM_IP>:8000/health | python3 -m json.tool
```

**Expected response:**
```json
{
  "status": "ok",
  "version": "1.0.0"
}
```

**Success criterion:**
- [ ] HTTP 200
- [ ] `status` == `"ok"`
- [ ] `version` == `"1.0.0"`

**Is `curl /health` sufficient?** **No.** `/health` is a shallow check — it only confirms the API process is alive. It does NOT verify:
- Database connectivity
- Schema integrity
- Engine correctness

**Recommended enhanced health check:**
```bash
# Test DB connectivity via a protected endpoint
curl -s -H "Authorization: Bearer dev-token" \
  "http://<VM_IP>:8000/v1/issues?severity=all" | python3 -m json.tool
```

If DB is down, this returns 500. If it returns issues, DB is up and queries work.

#### 9b. Issues Endpoint

```bash
curl -s -H "Authorization: Bearer dev-token" \
  "http://<VM_IP>:8000/v1/issues?severity=all" | python3 -m json.tool
```

**Expected response structure:**
```json
{
  "issues": [
    {
      "node_id": "<uuid>",
      "item_id": "<uuid>",
      "location_id": "<uuid>",
      "shortage_qty": "3",
      "severity_score": "3",
      "severity": "low",
      "shortage_date": "2026-04-22",
      "explanation_id": null,
      "explanation_url": null
    }
    // ... more issues
  ],
  "total": 13,
  "as_of": "2026-04-05T..."
}
```

**Success criterion:**
- [ ] HTTP 200 (not 401, not 500)
- [ ] `total` ≥ 10 (expect ~13 — 8 PUMP-01 shortages + 5 VALVE-02 shortages)
- [ ] Issues span two items (confirm by checking distinct `item_id` values)
- [ ] Issues span two locations (confirm by checking distinct `location_id` values)
- [ ] `severity` values present: mix of `"low"` and `"medium"` expected
- [ ] `shortage_date` values fall within `today + 10` to `today + 24` range

**Is returning data sufficient proof?** **Partially.** It confirms:
- DB is up ✅
- Schema is correct ✅
- Seed ran ✅
- Shortage detection query works ✅

It does NOT confirm:
- Engine propagation ran (shortages were seeded directly, not computed)
- Causal graph is coherent
- Edge wiring between PO and PI nodes is correct

**Recommended additional validation:**
```bash
# Check explain endpoint works for one shortage
NODE_ID=$(curl -s -H "Authorization: Bearer dev-token" \
  "http://<VM_IP>:8000/v1/issues" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['issues'][0]['node_id'])")

curl -s -H "Authorization: Bearer dev-token" \
  "http://<VM_IP>:8000/v1/explain?node_id=${NODE_ID}" | python3 -m json.tool

# Check graph endpoint
curl -s -H "Authorization: Bearer dev-token" \
  "http://<VM_IP>:8000/v1/graph" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'nodes={len(d[\"nodes\"])}, edges={len(d[\"edges\"])}')"
```

---

## MONITORING AFTER VALIDATION

### Immediate (during validation session)
- [ ] `docker compose logs api --tail=100` — no ERROR-level log lines
- [ ] `docker compose logs postgres --tail=50` — no FATAL lines
- [ ] `docker stats --no-stream` — API < 500 MiB RAM, Postgres < 200 MiB RAM
- [ ] `docker compose exec postgres psql -U ootils -d ootils_dev -c "SELECT * FROM calc_runs ORDER BY started_at DESC LIMIT 5;"` — check no stuck `running` runs

### Ongoing (if left running)
- Container restarts: `docker compose ps` — `Restarts` column should be 0
- Log tailing: `docker compose logs -f api 2>&1 | grep -i "error\|fatal\|exception"` in background
- Disk usage: `df -h /` — Postgres data volume should not grow unexpectedly
- No built-in alerting for V1 — **manual monitoring only**

### Engine correctness signal
The planning engine (propagation) is **not triggered automatically** after seed. Shortages were seeded directly (not computed). To verify the engine runs:

```bash
# POST an event to trigger propagation
curl -s -X POST -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  "http://<VM_IP>:8000/v1/events" \
  -d '{"event_type": "ingestion_complete", "scenario_id": "00000000-0000-0000-0000-000000000001", "source": "manual_test"}'

# Check a new calc_run was created
docker compose exec postgres psql -U ootils -d ootils_dev -c \
  "SELECT calc_run_id, status, started_at, completed_at FROM calc_runs ORDER BY started_at DESC LIMIT 3;"
```

> ⚠️ Known issue (B3 from QC-V1-COMPLETE): `shortage_detector.resolve_stale()` is never called in the propagator — stale shortages may persist after engine runs.

---

## CLEANUP / POST-VALIDATION DECISION

### Option A — Leave Running (Recommended for iteration)
- Leave docker stack up
- Take a Proxmox VM snapshot for rollback baseline:
```bash
qm snapshot 201 post-seed-validated --description "Ootils V1 seed validated $(date -u +%Y-%m-%dT%H:%M:%SZ)"
```
- Note snapshot name for rollback: `qm rollback 201 post-seed-validated`

### Option B — Snapshot and Pause
```bash
# On VM: clean shutdown
docker compose down
sudo poweroff

# On Proxmox host:
qm snapshot 201 v1-validated-clean --description "Clean state post validation"
# VM is stopped — costs no CPU, minimal disk (only delta from base image)
```

### Option C — Destroy
```bash
# Nuclear — full reset
docker compose down -v  # destroys postgres_data volume
qm stop 201
qm destroy 201 --purge
```
Use only if starting completely fresh.

### What NOT to do
- Do not leave `OOTILS_API_TOKEN=dev-token` accessible from the public internet
- Do not expose port 5432 to LAN without firewall rules
- Do not run `docker compose up --build` again without truncating the DB first (seed idempotency issue — B4)

---

## COMPLETE VALIDATION COMMAND SEQUENCE

```bash
# From your workstation (replace VM_IP)
VM_IP="<your-vm-ip>"
TOKEN="dev-token"

# 1. Health
curl -sf http://${VM_IP}:8000/health && echo "✅ Health OK"

# 2. Issues
ISSUES=$(curl -sf -H "Authorization: Bearer ${TOKEN}" \
  "http://${VM_IP}:8000/v1/issues?severity=all")
echo $ISSUES | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'✅ Issues: {d[\"total\"]} found')"

# 3. Graph sanity
GRAPH=$(curl -sf -H "Authorization: Bearer ${TOKEN}" \
  "http://${VM_IP}:8000/v1/graph")
echo $GRAPH | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'✅ Graph: {len(d[\"nodes\"])} nodes, {len(d[\"edges\"])} edges')"

# 4. Trigger engine run
curl -sf -X POST -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  "http://${VM_IP}:8000/v1/events" \
  -d '{"event_type":"ingestion_complete","scenario_id":"00000000-0000-0000-0000-000000000001","source":"qc_validation"}' \
  && echo "✅ Event posted"

# 5. Docs accessible (no auth)
curl -sf http://${VM_IP}:8000/docs | grep -q "Ootils" && echo "✅ Swagger UI OK"
```

---

## RISK REGISTER

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| Migration 001 SQLite crash on fresh PG | **HIGH** | BLOCKER — API won't start | Fix 001 to SELECT 1 only |
| `python -m ootils_core.db.migrate` missing | **CERTAIN** | Step 7 fails | Remove step 7 from plan |
| Seed re-run doubles data | **MEDIUM** | Corrupted test results | Truncate before re-seed |
| PG 15 vs PG 16 version mismatch | **LOW** | Behavioral differences in JSON, partitioning | Pin compose to 16 |
| API token `dev-token` in production | **HIGH** | Security exposure | Change token before any external access |
| No migration rollback path | **MEDIUM** | Manual DB recovery needed | Document DROP/CREATE procedure |
| Engine shortages seeded not computed | **LOW** | Engine not actually validated | Post ingestion_complete event manually |

---

*Generated by QC review of `live/v1-bootstrap` source — 2026-04-05*
