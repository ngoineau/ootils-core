# ootils-core

**Graph-based supply chain planning engine — PostgreSQL + FastAPI + Python.**

`ootils-core` is a supply chain decision engine that models a network of nodes and edges (items, locations, PI nodes, suppliers, resources) and runs incremental propagation, shortage detection, MRP explosion, and scenario analysis on that graph. It exposes a REST API for integration with AI agents, dashboards, and planning tools.

---

## Architecture

```
┌─────────────────────────────────────────────┐
│  FastAPI REST API (Bearer token auth)        │
│  /v1/graph  /v1/projection  /v1/scenarios    │
│  /v1/ingest  /v1/dq  /v1/bom  /v1/rccp      │
│  /v1/ghosts  /v1/explain  /v1/simulate  ...  │
└──────────────────────┬──────────────────────┘
                       │
┌──────────────────────▼──────────────────────┐
│  Python kernel                               │
│  ├── Graph traversal + incremental           │
│  │   propagation (dirty-flag pattern)        │
│  ├── Shortage detection + severity scoring   │
│  ├── MRP explosion (BOM + lead times)        │
│  ├── Scenario branching (copy-on-write)      │
│  ├── RCCP (rough-cut capacity planning)      │
│  ├── Ghost engine (virtual supply nodes)     │
│  └── DQ agent (data quality pipeline)       │
└──────────────────────┬──────────────────────┘
                       │
┌──────────────────────▼──────────────────────┐
│  PostgreSQL 16                               │
│  Typed schema — SQL migrations               │
│  UUID PKs, TIMESTAMPTZ UTC, JSONB only for   │
│  diagnostic or staging payloads              │
└─────────────────────────────────────────────┘
```

---

## Requirements

| Dependency | Version |
|-----------|---------|
| Python | 3.11+ |
| PostgreSQL | 16 |
| Docker + Docker Compose | Latest |

CI validates Python 3.11. The default Docker image runs Python 3.12.

---

## Quick start

### 1. Clone and configure

```bash
git clone https://github.com/ngoineau/ootils-core.git
cd ootils-core
cp .env.example .env
# Edit .env — set POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, OOTILS_API_TOKEN
```

### 2. Start services

```bash
docker compose up -d
```

This starts PostgreSQL 16 and the FastAPI server on port 8000. Migrations run automatically on first boot.

### 3. Verify

```bash
curl http://localhost:8000/health
# {"status": "ok"}

curl http://localhost:8000/docs
# Opens Swagger UI
```

### 4. Load demo data (optional)

```bash
docker compose exec api python scripts/seed_demo_data.py
```

### 5. First API call

```bash
# List all graph nodes
curl -H "Authorization: Bearer <your-token>" http://localhost:8000/v1/graph/nodes

# Run a propagation
curl -X POST -H "Authorization: Bearer <your-token>" \
  -H "Content-Type: application/json" \
  -d '{"scenario_id": "<uuid>"}' \
  http://localhost:8000/v1/graph/propagate
```

---

## Key capabilities

| Capability | Endpoints | Description |
|-----------|-----------|-------------|
| **Graph model** | `/v1/graph/*`, `/v1/nodes/*` | Nodes (PI, supply, demand, resource), edges (replenishes, consumes, feeds_forward, etc.) |
| **Projection engine** | `/v1/projection/*` | Time-series projections with elastic time (daily → weekly → monthly) |
| **Shortage detection** | `/v1/issues/*` | Post-propagation shortage scoring and explanation |
| **Explainability** | `/v1/explain/*` | Causal step traces for shortage root causes |
| **Scenario management** | `/v1/scenarios/*` | Branch, fork, archive scenarios; baseline tracking |
| **Simulation** | `/v1/simulate` | What-if simulation with scenario overrides |
| **MRP** | `/v1/bom/*` | BOM management + MRP explosion |
| **Ingest pipeline** | `/v1/ingest/*` | Batch import of static and dynamic master data |
| **Data quality** | `/v1/dq/*` | L1/L2 DQ checks, issue tracking, agent-driven remediation |
| **RCCP** | `/v1/rccp/*` | Rough-cut capacity planning against resource constraints |
| **Ghost engine** | `/v1/ghosts/*` | Virtual supply nodes for unconstrained planning |
| **Calendars** | `/v1/calendars/*` | Planning calendars for zone transitions |
| **Calc runs** | `/v1/calc/*` | Propagation job tracking and status |
| **Planning params** | `/v1/items/planning-params` | Item-level planning parameters (SS, ROP, EOQ) |
| **Events** | `/v1/events/*` | Supply chain event queue |

---

## API documentation

Interactive Swagger UI: **http://localhost:8000/docs**

ReDoc: **http://localhost:8000/redoc**

Static OpenAPI spec: `docs/openapi.json`

Authentication: `Authorization: Bearer <OOTILS_API_TOKEN>`

---

## Development

### Install

```bash
pip install -e ".[dev]"
```

### Run tests

```bash
python3 -m pytest tests/ -q
```

Tests use a real PostgreSQL test database (via `DATABASE_URL` env var). No mocks for DB-backed integration tests.

### Run locally (without Docker)

```bash
export DATABASE_URL=postgresql:///ootils_dev
export OOTILS_API_TOKEN=dev-token
uvicorn ootils_core.api.app:app --reload
```

Migrations apply automatically on startup.

### Project structure

```
src/ootils_core/
├── api/
│   ├── app.py              # FastAPI application factory
│   └── routers/            # One router per capability domain
├── db/
│   ├── connection.py       # PostgreSQL connection + migration runner
│   └── migrations/         # Sequential SQL migrations
├── engine/
│   ├── propagator.py       # Incremental graph propagation
│   ├── shortage/           # Shortage detection + severity scoring
│   ├── scenario/           # Scenario branching + copy-on-write
│   ├── dq/                 # Data quality pipeline + DQ agent
│   └── ghosts/             # Ghost node engine
└── models/                 # Pydantic request/response schemas
```

---

## Scalability

Current system is validated at demo scale (2–50 items). For production deployment:

- **500 items (SMB):** Batch propagation queries required. See `docs/SCALABILITY.md`.
- **5,000+ items:** Architectural investment in in-memory propagation + table partitioning. See `docs/SCALABILITY.md`.

---

## Documentation

| Doc | Purpose |
|-----|---------|
| `docs/SCALABILITY.md` | Volume projections, breaking points, fix roadmap |
| `docs/INFRA-RUNBOOK.md` | Deployment, backup, scenario cleanup procedures |
| `docs/ADR-*.md` | Architecture decision records |
| `docs/SPEC-*.md` | Feature specifications |
| `ROADMAP.md` | Product roadmap |
| `CONTRIBUTING.md` | Contribution guidelines |

---

## License

See `LICENSE`.
