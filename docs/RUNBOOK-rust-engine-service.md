# Runbook — Rust engine service (ADR-017 Architecture B)

This is the operational guide for the standalone Rust engine service
delivered in phases 1-8 of [ADR-017](ADR-017-architecture-b-rust-engine-service.md).

If you only want **performance benefits without the architectural shift**,
stick with `OOTILS_ENGINE=sql` (default) or `OOTILS_ENGINE=rust` (Architecture A,
in-process). The service path is opt-in.

## Why this service exists

The Postgres-backed engines (SQL, Rust-in-process) are capped at
~2.4× SQL on bulk and slightly slower than SQL on incremental events.
The bottleneck is Postgres I/O on the propagation hot path.

The standalone Rust engine moves the state into RAM and writes back to
Postgres asynchronously. Measured on profile L (10 K SKU, 227 K PI nodes):

| Mode | SQL engine | Rust service (this) | Speedup |
|---|---|---|---|
| Single event propagation | 95 ms | **0.35 ms** | **271×** |
| Full propagation (all PIs dirty) | 36.9 s | **86 ms** | **430×** |
| Scenario fork | 5-15 s | **32 ms** | **150-470×** |
| Sustained 100 events/sec | n/a | p95 **2.36 ms** | — |
| Sustained 5000 events/sec | n/a | p95 **1.37 ms** | — |

Reproducible via `scripts/stress_test_engine.py`.

## Architecture summary

```
┌──────────────────┐ gRPC ┌────────────────────────────────────┐
│ FastAPI Python   │─────→│ ootils-engine (Rust service)        │
│ OOTILS_ENGINE=   │      │ - in-RAM graph (76 MB on profile L) │
│   rust-svc       │      │ - rayon parallel propagator         │
└──────────────────┘      │ - WAL (local file, fsync per event) │
                          │ - write-behind to Postgres (100ms)  │
                          └────────────────┬────────────────────┘
                                           ▼
                                    ┌──────────────┐
                                    │ PostgreSQL   │
                                    │ (durable     │
                                    │  source of   │
                                    │  truth)      │
                                    └──────────────┘
```

## Components

- `ootils-engine` binary (Rust). Built from `rust/ootils_engine/`.
- `ootils-kernel` PyO3 module (Architecture A). Still useful; not required
  for the service path.
- `RustServicePropagationEngine` (Python). Lives in
  `src/ootils_core/engine/orchestration/propagator_rust_svc.py`.
  Dispatched via `OOTILS_ENGINE=rust-svc`.
- gRPC contract: `rust/ootils_proto/proto/engine.proto`.

## Configuration (environment variables)

### Python (FastAPI) side

| Var | Default | Purpose |
|---|---|---|
| `OOTILS_ENGINE` | `sql` | Set to `rust-svc` to dispatch to the service |
| `OOTILS_ENGINE_ADDR` | `127.0.0.1:50051` | gRPC endpoint of the service |
| `DATABASE_URL` | — | Postgres DSN (shared with the service for read paths) |

### Rust service side

| Var | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | — | Postgres DSN (bootstrap load + write-behind) |
| `OOTILS_ENGINE_LISTEN` | `127.0.0.1:50051` | gRPC bind address |
| `OOTILS_WAL_PATH` | `./ootils-engine.wal` | Local WAL file. **Mount a persistent volume here.** |
| `OOTILS_FLUSH_INTERVAL_MS` | `100` | Write-behind flush cadence (Postgres lag = this) |
| `RUST_LOG` | `info,ootils_engine=debug` | tracing-subscriber env filter |

## Deployment

### Option A — Single docker-compose (dev/staging)

Add to your existing `docker-compose.yml`:

```yaml
services:
  ootils-engine:
    build:
      context: .
      dockerfile: Dockerfile.engine
    image: ootils-engine:0.1.0
    environment:
      DATABASE_URL: "postgresql://ootils:ootils@db:5432/ootils"
      OOTILS_ENGINE_LISTEN: "0.0.0.0:50051"
      OOTILS_WAL_PATH: "/var/lib/ootils-engine/wal.bin"
      RUST_LOG: "info"
    ports:
      - "50051:50051"
    volumes:
      - ootils-wal:/var/lib/ootils-engine
    depends_on:
      - db
    restart: unless-stopped

  api:
    # ... existing FastAPI service ...
    environment:
      OOTILS_ENGINE: "rust-svc"
      OOTILS_ENGINE_ADDR: "ootils-engine:50051"
      DATABASE_URL: "postgresql://ootils:ootils@db:5432/ootils"
    depends_on:
      - ootils-engine

volumes:
  ootils-wal:
```

### Option B — Kubernetes (production)

Run the engine as a **StatefulSet** (1 replica per tenant, named pod
slots for WAL persistence) alongside the FastAPI Deployment. Sketch:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ootils-engine
spec:
  serviceName: ootils-engine
  replicas: 1
  template:
    spec:
      containers:
        - name: engine
          image: ootils-engine:0.1.0
          ports:
            - containerPort: 50051
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef: { name: ootils-db, key: dsn }
            - name: OOTILS_ENGINE_LISTEN
              value: "0.0.0.0:50051"
            - name: OOTILS_WAL_PATH
              value: "/var/lib/ootils-engine/wal.bin"
          volumeMounts:
            - name: wal
              mountPath: /var/lib/ootils-engine
          readinessProbe:
            tcpSocket: { port: 50051 }
            initialDelaySeconds: 5
            periodSeconds: 2
          livenessProbe:
            tcpSocket: { port: 50051 }
            periodSeconds: 10
  volumeClaimTemplates:
    - metadata: { name: wal }
      spec:
        accessModes: [ReadWriteOnce]
        resources:
          requests:
            storage: 10Gi
```

Multi-tenant: one StatefulSet per tenant, isolated WAL volumes,
separate Postgres credentials. Phase-8 ships single-tenant; multi-
tenant is a future ADR.

## Rollout plan

Recommended canary sequence:

1. **Phase 8a — Validation environment**.
   Deploy the engine in staging. Run `scripts/stress_test_engine.py
   --target-rps 100 --duration-s 600` (10-minute soak). Confirm
   p95 < 50 ms and RSS drift ≤ 0.

2. **Phase 8b — Read-only canary** (optional).
   Deploy the engine alongside the SQL engine. Keep
   `OOTILS_ENGINE=sql` but run a background job that periodically
   fires `GetNode` against the service for the same node IDs the
   SQL engine just wrote — compare for drift. If 0 drift for 24h,
   move to 8c.

3. **Phase 8c — 1% production traffic**.
   Switch `OOTILS_ENGINE=rust-svc` for a small slice (single tenant
   or single endpoint). Monitor :
   - p95 latency on `/v1/events` from the API
   - `ootils-engine` RSS (Prometheus or `docker stats`)
   - WAL file size (`stat /var/lib/ootils-engine/wal.bin`)
   - Postgres write-behind queue depth (engine logs)
   - Error rate on `/v1/events`

4. **Phase 8d — 10% then 100%**.
   Ramp up over 1-2 weeks. Keep `OOTILS_ENGINE=sql` reachable as
   rollback (just flip the env var, no code change).

## Health checks

### Liveness — TCP

```bash
nc -z -w2 ootils-engine 50051 && echo "alive"
```

The gRPC port accepts TCP only after `verify_postgres` + initial
baseline load succeed. If the port is open, the engine is healthy.

### Readiness — gRPC `Health` RPC

From Python:

```python
from ootils_core.engine_rust_service import EngineClient
client = EngineClient.connect("ootils-engine:50051")
health = client.health()
assert health.status == 1  # SERVING
print(health.detail)  # e.g. "phase 2: baseline loaded (330434 nodes, gen 5)"
```

Use this readiness probe in k8s instead of pure TCP: it confirms the
in-RAM graph is actually populated, not just the port bound.

## Observability

Phase 8 ships logs only. Tracing/metrics integration (OTLP, Prometheus)
is follow-up work — out of scope but the engine already emits
`tracing` events that can be exported by wiring `tracing-opentelemetry`
in a future PR.

Useful log signals (grep on engine stdout):

| Pattern | Meaning |
|---|---|
| `baseline ready in RAM` | Boot complete, ready to serve |
| `write-behind flusher started` | Background Postgres flush task up |
| `WAL truncated after flush` | Successful Postgres flush, WAL reclaimed |
| `flusher recovered, resetting backoff` | PG was unreachable, recovered |
| `WriteBehindQueue: flush to Postgres FAILED, backing off` | PG outage in progress |
| `scenario forked from baseline` | Fork operation, with `clone_ms` |

## Rollback

Setting `OOTILS_ENGINE=sql` (or removing the env var) reverts FastAPI
to the SQL engine. **The Rust service can be stopped without notice**
once Python has stopped sending it traffic — its in-RAM state is
disposable (Postgres is the source of truth).

If the engine **crashes mid-flush**, on restart it replays the WAL,
re-applies the deltas to its in-RAM graph, and re-enqueues them for
Postgres. No data loss as long as the WAL volume is intact.

If the **WAL volume is lost** (catastrophic disk failure), Postgres
contains the state up to the last successful flush — which may lag
by up to `OOTILS_FLUSH_INTERVAL_MS` (100ms default). Anything
acked by the engine within that 100ms window may not be in Postgres.

In practice: keep the WAL on a redundant volume (RAID, k8s PV with
replication). For stricter durability, lower `OOTILS_FLUSH_INTERVAL_MS`
at the cost of more frequent Postgres writes.

## Known limitations (phase 8 ship)

- **No TLS** on the gRPC channel. The engine assumes a trusted
  network (k8s pod-to-pod, VPC peering). Public exposure requires
  TLS in front (envoy, nginx) or follow-up TLS support in tonic.
- **No auth** on gRPC. Same trust assumption.
- **No multi-tenant isolation**. One process serves one tenant
  (or all if you don't isolate). Per-tenant processes recommended
  in k8s.
- **Eventual consistency** for non-gRPC reads. If Python reads
  Postgres directly (e.g. dashboards), data lags by
  `OOTILS_FLUSH_INTERVAL_MS`. Read via `GetNode` RPC for
  read-your-writes consistency.
- **Per-scenario propagation** not yet exposed. Propagate always
  targets the baseline. Fork-then-propagate-on-fork is a phase-9
  task.
