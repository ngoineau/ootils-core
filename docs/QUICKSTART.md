# Quickstart

Get `ootils-core` running locally, hit the API, and run a propagation in **under 5 minutes**.

This guide is opinionated: it assumes Docker, picks a default token, and skips configuration details that don't matter for "first run". For the full setup, read [`../README.md`](../README.md) and [`INFRA-RUNBOOK.md`](INFRA-RUNBOOK.md).

---

## Prerequisites

- Docker + Docker Compose
- `curl` (or `httpie`, or any HTTP client)
- About 5 minutes

You do **not** need a local PostgreSQL — compose brings one up.

---

## 1. Clone and configure

```bash
git clone https://github.com/ngoineau/ootils-core.git
cd ootils-core
cp .env.example .env
```

Edit `.env` and set at least:

```bash
POSTGRES_USER=ootils
POSTGRES_PASSWORD=ootils
POSTGRES_DB=ootils_dev
OOTILS_API_TOKEN=dev-token   # any non-empty string for local dev
```

> **About the API token.** The API fails to start if `OOTILS_API_TOKEN` is unset (see [`SECURITY.md`](../SECURITY.md)). For local dev anything works; for staging/prod, use a strong random secret and rotate it. The CI suite uses `test-token-ci`; existing tests hard-code `dev-token` as fallback.

---

## 2. Start services

```bash
docker compose up -d
```

This builds the API image, starts PostgreSQL 16, applies all 32 migrations on first boot, and exposes the API on port `8000`.

Check health:

```bash
curl http://localhost:8000/health
# {"status":"ok","version":"1.0.0"}
```

> Health is the only unauthenticated endpoint.

---

## 3. (Optional) Load the demo dataset

```bash
docker compose exec api python scripts/seed_demo_data.py
```

This populates two items, two locations, and supply/demand nodes that produce two known shortages (PUMP-01 @ DC-ATL, VALVE-02 @ DC-LAX). Useful for verifying the agent surface end-to-end.

---

## 4. First authenticated call

```bash
TOKEN="dev-token"   # whatever you put in .env

curl -H "Authorization: Bearer $TOKEN" \
     http://localhost:8000/v1/graph/nodes | jq '.[0:3]'
```

Or list active shortages (after seeding):

```bash
curl -H "Authorization: Bearer $TOKEN" \
     "http://localhost:8000/v1/issues?scenario_id=00000000-0000-0000-0000-000000000001" | jq
```

---

## 5. (Optional) Interactive API docs

By default Swagger UI and ReDoc are **disabled** in production. To enable them for local dev:

```bash
echo "OOTILS_ENABLE_API_DOCS=1" >> .env
docker compose up -d --force-recreate api
```

Then open:
- Swagger UI: <http://localhost:8000/docs>
- ReDoc:      <http://localhost:8000/redoc>

The CSP middleware automatically relaxes its rules on `/docs` and `/redoc` so Swagger's inline scripts and the CDN-hosted assets load correctly.

---

## 6. Trigger a propagation (the agent path)

The shortest path from "data in" to "shortages computed" mirrors what an LLM agent does via `tools/agent_tools.py`:

```bash
# 1. Trigger a full recompute for the baseline scenario
curl -H "Authorization: Bearer $TOKEN" \
     -X POST \
     "http://localhost:8000/v1/calc/run?scenario_id=00000000-0000-0000-0000-000000000001"

# 2. Fetch the shortages that resulted
curl -H "Authorization: Bearer $TOKEN" \
     "http://localhost:8000/v1/issues?scenario_id=00000000-0000-0000-0000-000000000001"

# 3. Explain a specific shortage
curl -H "Authorization: Bearer $TOKEN" \
     "http://localhost:8000/v1/explain/<shortage_id>"
```

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `RuntimeError: OOTILS_API_TOKEN environment variable is not set.` | Set `OOTILS_API_TOKEN=<anything>` in `.env`; the API fails closed. |
| `401 Unauthorized` on `/v1/*` calls | Check the `Authorization: Bearer <token>` header matches the value in `.env`. The server reads it fresh on every request. |
| `connection refused` on port 5432 | `docker compose ps` — confirm `postgres` is healthy. Migrations apply via `OotilsDB()` constructor in `api/dependencies.py`. |
| Swagger UI shows a blank page | `OOTILS_ENABLE_API_DOCS=1` must be set; rebuild the API container after changing `.env`. |
| Health passes but `/v1/*` hangs in tests | Tests forgot `app.dependency_overrides[get_db]`. The audit log in `_log_api_request` tries to open a DB connection on `/health`; overriding `get_db` short-circuits it. |

---

## Next steps

- Read [`../README.md`](../README.md) for the capability surface and the architecture diagram.
- Read [`INDEX.md`](INDEX.md) — categorised map of the `docs/` tree.
- Read [`ADR-003-incremental-propagation.md`](ADR-003-incremental-propagation.md) and [`ADR-004-explainability.md`](ADR-004-explainability.md) — these are the two ADRs you need to understand the engine.
- Run `make help` (or read the [`Makefile`](../Makefile)) for canonical dev commands.
