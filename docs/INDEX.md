# `docs/` index

Navigation map for `docs/`. Group by purpose, not by date. Read top-to-bottom for onboarding, jump by section for reference.

Resolves R9 of [REVIEW-2026-05.md](REVIEW-2026-05.md).

---

## Start here

- [`QUICKSTART.md`](QUICKSTART.md) — Clone → run → first API call in 5 minutes.
- [`staging-quickstart.md`](staging-quickstart.md) — Push data from an external system (ERP / WMS / MES / Excel) end-to-end in 4 steps.
- [`staging-templates/`](staging-templates/) — Per-entity file-format contracts (one `.md` + one `.tsv` per entity).
- [`../README.md`](../README.md) — Full capability surface and architecture diagram.
- [`../CLAUDE.md`](../CLAUDE.md) — Context for Claude Code sessions: conventions, commands, architecture map.
- [`../ROADMAP.md`](../ROADMAP.md) — V1 milestones.
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — How to contribute.
- [`STRATEGY.md`](STRATEGY.md) — Product strategy and positioning.
- [`STRATEGY-autonomous-supply-chain-operations.md`](STRATEGY-autonomous-supply-chain-operations.md) — Agent-fleet operating model and proof plan for autonomous supply-chain operations.

## Architecture decisions (ADRs)

The current-state ADRs to read first:

- [`ADR-001-graph-model.md`](ADR-001-graph-model.md) — The graph model: nodes, edges, scenarios.
- [`ADR-003-incremental-propagation.md`](ADR-003-incremental-propagation.md) — Deterministic incremental propagation.
- [`ADR-004-explainability.md`](ADR-004-explainability.md) — Causal step traces for shortage roots.
- [`ADR-011-scenario-retention.md`](ADR-011-scenario-retention.md) — FK retention policy; soft-delete only. Its "Reste à faire" follow-up (archived-scenario cleanup) is closed by [ADR-039](ADR-039-scenario-archive-cleanup.md).
- [`ADR-012-scenario-fork-bulk.md`](ADR-012-scenario-fork-bulk.md) — Bulk `INSERT…SELECT` scenario fork (27.5× faster). Lazy CoW deferred to ADR-013.

Elastic time (sprint of iteration, ADR-002 is the final version to read):

- [`ADR-002d-elastic-time-final.md`](ADR-002d-elastic-time-final.md) — **Authoritative.**
- `ADR-002-elastic-time.md` / `ADR-002b…d-elastic-time-*.md` — Historical iterations.

Operational concerns:

- [`ADR-005-storage-layer.md`](ADR-005-storage-layer.md) — Storage. **Marked superseded** — kept for history, runtime is Postgres via psycopg3.
- [`ADR-006-blockers-resolution.md`](ADR-006-blockers-resolution.md), [`ADR-007-showstoppers-resolution.md`](ADR-007-showstoppers-resolution.md), [`ADR-008-agent-operability-fixes.md`](ADR-008-agent-operability-fixes.md) — Punctual decisions during sprint hardening.
- [`ADR-009-import-pipeline.md`](ADR-009-import-pipeline.md) — Ingest pipeline shape (2-step staging + DQ).
- [`ADR-010-ghosts-tags.md`](ADR-010-ghosts-tags.md) — Ghost nodes and tags.
- [`ADR-013-external-interfaces.md`](ADR-013-external-interfaces.md) — File formats (TSV/CSV/XLSX/JSON), full-reload semantics, mandatory approval. Complements ADR-009. D4 (mandatory human approval) is **partially superseded by [ADR-037](ADR-037-daily-run-and-governed-ingest.md)** for the governed daily-run case — D4 still stands for any ad-hoc upload outside a governed daily run.

### Full ADR register (001 → 037, chronological)

Every ADR under `docs/`, numbered. The curated "read first" lists above are the entry points; this is the complete map.

- [`ADR-001-graph-model.md`](ADR-001-graph-model.md) — Graph-based domain model: the node/edge taxonomy the whole engine is built on.
- [`ADR-002-elastic-time.md`](ADR-002-elastic-time.md) — Object-local ("elastic") time model — initial proposal.
- [`ADR-002b-elastic-time-design.md`](ADR-002b-elastic-time-design.md) — Elastic time, complete design iteration.
- [`ADR-002c-elastic-time-layered.md`](ADR-002c-elastic-time-layered.md) — Elastic time, layered-grain model iteration.
- [`ADR-002d-elastic-time-final.md`](ADR-002d-elastic-time-final.md) — Elastic time, **final/authoritative** decision.
- [`ADR-003-incremental-propagation.md`](ADR-003-incremental-propagation.md) — Deterministic incremental propagation (dirty-flag + topo order).
- [`ADR-004-explainability.md`](ADR-004-explainability.md) — Native explainability: root-cause causal chains.
- [`ADR-005-storage-layer.md`](ADR-005-storage-layer.md) — Storage layer and data model (**superseded** — runtime is Postgres/psycopg3).
- [`ADR-006-blockers-resolution.md`](ADR-006-blockers-resolution.md) — QC blockers resolution during hardening.
- [`ADR-007-showstoppers-resolution.md`](ADR-007-showstoppers-resolution.md) — Conceptual showstoppers resolution.
- [`ADR-008-agent-operability-fixes.md`](ADR-008-agent-operability-fixes.md) — Agent operability fixes.
- [`ADR-009-import-pipeline.md`](ADR-009-import-pipeline.md) — Import pipeline architecture: staging + DQ + core.
- [`ADR-010-ghosts-tags.md`](ADR-010-ghosts-tags.md) — Ghosts and tags: virtual supply + ad-hoc groupings.
- [`ADR-011-scenario-retention.md`](ADR-011-scenario-retention.md) — Scenario retention policy: FK `RESTRICT`, soft-delete only.
- [`ADR-012-scenario-fork-bulk.md`](ADR-012-scenario-fork-bulk.md) — Scenario fork via bulk `INSERT…SELECT` (27.5× faster).
- [`ADR-013-external-interfaces.md`](ADR-013-external-interfaces.md) — External interfaces: file formats, full reload, mandatory approval.
- [`ADR-014-resources-units-scd2.md`](ADR-014-resources-units-scd2.md) — Merge resources/work_centers, typed capacity units, transparent SCD2.
- [`ADR-015-rust-readiness.md`](ADR-015-rust-readiness.md) — Rust readiness: prepare kernel portability without porting yet.
- [`ADR-016-rust-engine-foundation.md`](ADR-016-rust-engine-foundation.md) — Rust engine foundation (Architecture A, tight scope).
- [`ADR-017-architecture-b-rust-engine-service.md`](ADR-017-architecture-b-rust-engine-service.md) — Architecture B: Rust in-memory engine service.
- [`ADR-018-per-scenario-propagation.md`](ADR-018-per-scenario-propagation.md) — Per-scenario propagation (engine RPC extension).
- [`ADR-019-demand-model-pyramide.md`](ADR-019-demand-model-pyramide.md) — Unified demand model (Pyramide): booking / shipping / backlog.
- [`ADR-020-mrp-consolidation.md`](ADR-020-mrp-consolidation.md) — Consolidate the two MRP engines into one source of truth.
- [`ADR-021-shortage-truth.md`](ADR-021-shortage-truth.md) — Single shortage truth: `mrp_core` math + the `shortages` table system.
- [`ADR-022-pyramide-reconciliation.md`](ADR-022-pyramide-reconciliation.md) — Hierarchical reconciliation: deterministic middle-out core, MinT-shrink at the edge.
- [`ADR-023-forecast-confidence.md`](ADR-023-forecast-confidence.md) — Forecast confidence score: deterministic accuracy × depth × freshness composition.
- [`ADR-024-foundation-model-routing.md`](ADR-024-foundation-model-routing.md) — Foundation model + head/tail routing: real Chronos-2, sealed provenance, native quantiles refused.
- [`ADR-025-scenario-param-overlay.md`](ADR-025-scenario-param-overlay.md) — Scenario-scoped planning-param overlay: single resolver, never promoted.
- [`ADR-026-reschedule-fpo.md`](ADR-026-reschedule-fpo.md) — Reschedule messages for open orders + Firm Planned Orders.
- [`ADR-027-streamchanges-sse.md`](ADR-027-streamchanges-sse.md) — StreamChanges: replayable SSE stream over `events`.
- [`ADR-028-drp-fair-share-rounding.md`](ADR-028-drp-fair-share-rounding.md) — DRP proportional fair-share + logistic down-rounding.
- [`ADR-029-agent-enterprise-floor.md`](ADR-029-agent-enterprise-floor.md) — Agent enterprise floor: cryptographic actor identity, per-agent tokens, scopes, kill switch.
- [`ADR-030-proof-machine.md`](ADR-030-proof-machine.md) — The proof machine: inventory snapshots, FVA, reco → outcome chaining.
- [`ADR-031-location-aliases.md`](ADR-031-location-aliases.md) — Location aliases: multi-code resolution of one warehouse.
- [`ADR-032-scope-grid-and-budgets.md`](ADR-032-scope-grid-and-budgets.md) — Scope grid, per-token budgets, credential lifecycle, `/metrics`.
- [`ADR-033-demand-routing-and-drift.md`](ADR-033-demand-routing-and-drift.md) — Head/tail demand routing wired + first demand-side watcher (forecast drift).
- [`ADR-034-scenario-compare.md`](ADR-034-scenario-compare.md) — Scenario compare (SC-1): read-only KPI comparison (shortages, stock value, fill rate) across 2-5 scenarios; stale computed with no new schema.
- [`ADR-035-buy-program-segmentation.md`](ADR-035-buy-program-segmentation.md) — Buy-program segmentation (DEM-2 PR1): read-only, zero-migration ΔFVA proof — new dense per-program reader, single-source `buy_program_bucket()` taxonomy (honest `UNKNOWN` bucket), reuses `compute_fva` unchanged.
- [`ADR-036-human-window.md`](ADR-036-human-window.md) — Human window (EXP-1 PR1): server-rendered `GET /ui` shell + `GET /v1/whoami`, read-only client over the existing API, no cookie/session, kill switch default OFF.
- [`ADR-037-daily-run-and-governed-ingest.md`](ADR-037-daily-run-and-governed-ingest.md) — Daily run & governed ingest (INT-1 PR1): versioned `feed_contracts` registry (migration 073) + pilot-editable YAML under `config/feed-contracts/`; supersedes [ADR-013](ADR-013-external-interfaces.md) D4 for the daily-run case (governed option (a): auto-approve iff DQ green AND all guards green, red guard on a blocking feed escalates via the L3 webhook). PR1 is registry-only — no runtime read yet (daily_runs + guard evaluation land in PR2/PR3, REST surface in PR4).
- [`ADR-039-scenario-archive-cleanup.md`](ADR-039-scenario-archive-cleanup.md) — Fork purge + shortage retention (PURGE-1, migration 076): closes the ADR-011 follow-up — TTL-driven deletion of an archived scenario's child rows (never the `scenarios` row, tombstoned via `purged_at`) through a CI-guarded FK-safe whitelist, plus a separate bounded GC of long-resolved `shortages`. Dry-run-by-default CLI + read-only `GET /v1/maintenance/purge-preview`; no HTTP apply endpoint in V1. Amends [ADR-005](ADR-005-storage-layer.md) (events insert-only carve-out) and [ADR-021](ADR-021-shortage-truth.md) (delegated shortage-retention GC).

## Feature specs (SPEC-*)

Read the SPEC matching the feature you are touching. SPECs are written before or during implementation; some have drifted from code — when in doubt, the code is authoritative.

- [`SPEC-INTERFACES.md`](SPEC-INTERFACES.md) — Inbound/outbound interfaces.
- [`SPEC-IMPORT-STATIC.md`](SPEC-IMPORT-STATIC.md) — Static (master) data import.
- [`SPEC-IMPORT-DYNAMIC.md`](SPEC-IMPORT-DYNAMIC.md) — Dynamic (transactional) data import.
- [`SPEC-INTEGRATION-STRATEGY.md`](SPEC-INTEGRATION-STRATEGY.md) — How import streams compose.
- [`SPEC-VALIDATION-HARNESS.md`](SPEC-VALIDATION-HARNESS.md) — Validation harness for inbound data.
- [`SPEC-STATIC-DATA-UI.md`](SPEC-STATIC-DATA-UI.md) — UI for static data review.
- [`SPEC-DQ-AGENT.md`](SPEC-DQ-AGENT.md) — Data Quality LLM agent.
- [`SPEC-HIERARCHIES.md`](SPEC-HIERARCHIES.md) — Hierarchy model (FR + EN).
- [`SPEC-GHOSTS-TAGS.md`](SPEC-GHOSTS-TAGS.md) — Ghost engine spec.

## API & data dictionaries

- [`api-spec.md`](api-spec.md) / [`openapi.json`](openapi.json) — REST surface.
- [`node-dictionary.md`](node-dictionary.md) — All `node_type` values and meaning.
- [`edge-dictionary.md`](edge-dictionary.md) — All `edge_type` values.

## Operations

- [`INFRA-RUNBOOK.md`](INFRA-RUNBOOK.md) — Deployment, backup, ops procedures.
- [`INFRA-vm-spec-validated.md`](INFRA-vm-spec-validated.md) — VM spec for the live deployment.
- [`SECURITY-vm-hardening.md`](SECURITY-vm-hardening.md) — Hardening checklist.
- [`SCALABILITY.md`](SCALABILITY.md) — Volume projections, known breaking points, fix roadmap.

## User-facing

- [`MANUEL-UTILISATEUR-DRAFT.md`](MANUEL-UTILISATEUR-DRAFT.md) — User manual (draft, FR).

## Reviews & retrospectives

The most recent first:

- [`REVIEW-2026-05.md`](REVIEW-2026-05.md) — May 2026 architecture review (10 findings, R1 + R4 resolved).
- [`REVIEW-BRANCHES-2026-04-07.md`](REVIEW-BRANCHES-2026-04-07.md) — Cross-branch state review.
- [`REVIEW-IMPORT-ARCHITECTURE.md`](REVIEW-IMPORT-ARCHITECTURE.md), [`REVIEW-IMPORT-DATA-ENGINEERING.md`](REVIEW-IMPORT-DATA-ENGINEERING.md), [`REVIEW-IMPORT-SC-EXPERT.md`](REVIEW-IMPORT-SC-EXPERT.md) — Import pipeline triple review.
- [`REVIEW-agent-operability.md`](REVIEW-agent-operability.md) — Agent operability deep-dive.
- [`REVIEW-conceptual-validation.md`](REVIEW-conceptual-validation.md) — Conceptual model audit.
- [`REVIEW-market-gtm.md`](REVIEW-market-gtm.md) — Market positioning.
- [`REVIEW-qc-validation.md`](REVIEW-qc-validation.md) — QC validation.

## Quality / proof artifacts

- [`QC-V1-COMPLETE.md`](QC-V1-COMPLETE.md) — V1 QC.
- [`QC-SPRINT1-REVIEW.md`](QC-SPRINT1-REVIEW.md) — Sprint 1 QC.
- [`QC-code-quality-review.md`](QC-code-quality-review.md) — Code quality pass.
- [`QC-live-deployment.md`](QC-live-deployment.md) — Live deployment QC.
- [`PROOF-OF-ARCHITECTURE-V1.md`](PROOF-OF-ARCHITECTURE-V1.md) — Proof-of-architecture artifact.
- [`PROPOSAL-engine-execution-model.md`](PROPOSAL-engine-execution-model.md) — Execution model proposal.

## Demos & milestones

- [`demo-phase1-e2e.md`](demo-phase1-e2e.md) — Phase 1 end-to-end demo notes.
- [`test-report-phase1.md`](test-report-phase1.md) — Phase 1 test report.
- [`DEMO-M7-ARCHITECTURE-VALIDATION.md`](DEMO-M7-ARCHITECTURE-VALIDATION.md), [`DEMO-M7-RESULTS.md`](DEMO-M7-RESULTS.md) — M7 demo.

## Specific topics

- [`EXPERT-dirty-flags-and-scenarios.md`](EXPERT-dirty-flags-and-scenarios.md) — Expert note on dirty flags + scenarios interaction.
- [`BACKLOG-calendar-architecture.md`](BACKLOG-calendar-architecture.md), [`CALENDAR-INTEGRATION-POINTS.md`](CALENDAR-INTEGRATION-POINTS.md) — Calendar model design.
- [`mrp-unification-tech-note.md`](mrp-unification-tech-note.md) — MRP endpoint unification (APICS mode).
- [`BIBLIOGRAPHY.md`](BIBLIOGRAPHY.md) — References used during design.

---

## Maintenance

When you add a doc, add it to the section that matches its purpose. When you supersede an ADR, mark the old one `Superseded by ADR-XXX` in its front matter and move it to a sub-bullet of its replacement here.
