# `docs/` index

Navigation map for `docs/`. Group by purpose, not by date. Read top-to-bottom for onboarding, jump by section for reference.

Resolves R9 of [REVIEW-2026-05.md](REVIEW-2026-05.md).

---

## Start here

- [`QUICKSTART.md`](QUICKSTART.md) — Clone → run → first API call in 5 minutes.
- [`../README.md`](../README.md) — Full capability surface and architecture diagram.
- [`../CLAUDE.md`](../CLAUDE.md) — Context for Claude Code sessions: conventions, commands, architecture map.
- [`../ROADMAP.md`](../ROADMAP.md) — V1 milestones.
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — How to contribute.
- [`STRATEGY.md`](STRATEGY.md) — Product strategy and positioning.

## Architecture decisions (ADRs)

The current-state ADRs to read first:

- [`ADR-001-graph-model.md`](ADR-001-graph-model.md) — The graph model: nodes, edges, scenarios.
- [`ADR-003-incremental-propagation.md`](ADR-003-incremental-propagation.md) — Deterministic incremental propagation.
- [`ADR-004-explainability.md`](ADR-004-explainability.md) — Causal step traces for shortage roots.
- [`ADR-011-scenario-retention.md`](ADR-011-scenario-retention.md) — FK retention policy; soft-delete only.

Elastic time (sprint of iteration, ADR-002 is the final version to read):

- [`ADR-002d-elastic-time-final.md`](ADR-002d-elastic-time-final.md) — **Authoritative.**
- `ADR-002-elastic-time.md` / `ADR-002b…d-elastic-time-*.md` — Historical iterations.

Operational concerns:

- [`ADR-005-storage-layer.md`](ADR-005-storage-layer.md) — Storage. **Marked superseded** — kept for history, runtime is Postgres via psycopg3.
- [`ADR-006-blockers-resolution.md`](ADR-006-blockers-resolution.md), [`ADR-007-showstoppers-resolution.md`](ADR-007-showstoppers-resolution.md), [`ADR-008-agent-operability-fixes.md`](ADR-008-agent-operability-fixes.md) — Punctual decisions during sprint hardening.
- [`ADR-009-import-pipeline.md`](ADR-009-import-pipeline.md) — Ingest pipeline shape.
- [`ADR-010-ghosts-tags.md`](ADR-010-ghosts-tags.md) — Ghost nodes and tags.

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
