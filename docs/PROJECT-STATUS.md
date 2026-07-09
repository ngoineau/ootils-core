# PROJECT STATUS — ootils-core

> **Document de contrôle.** Propriété de l'agent `ootils-pilote` (chef de projet).

**Dernière mise à jour : 2026-07-08 (H1 exécuté 5/5 — voir ROADMAP-AGENTS §4).**

---

## Ce fichier n'est plus la source de vérité

Le statut vivant du projet ne vit plus ici. Il vit à deux endroits, tenus à jour au fil de l'eau :

- **Épique [#397](https://github.com/ngoineau/ootils-core/issues/397)** — commentaires datés, cases cochées au fil des merges (vagues A/B/C).
- **[`docs/ROADMAP-AGENTS-2026-H2.md`](ROADMAP-AGENTS-2026-H2.md)** — document de passation : séquencement H0→H4, critères d'acceptation par chantier, règles de vol, registre 🎯 pilote.

Ce fichier était gelé au 2026-05-30 (il nommait encore « Pyramide » comme chantier actif, depuis livré). Le figer en pointeur évite une seconde source de vérité qui dérive.

---

## État factuel au 2026-07-08 (à revérifier en live)

- **`main` = `69237d0`.** 72 migrations SQL (`ls src/ootils_core/db/migrations/*.sql`), registre ADR 001→033 (36 fichiers via `ls docs/ADR-*.md`, variantes 002b/c/d incluses).
- **#408 (démo E2E) livrée/fermée** (7 PASS / 3 SKIP / 0 FAIL le 06/07) ; véhicule = `scripts/demo_e2e.py` + `docs/DEMO-RUNBOOK.md`.
- **#414 « allumer la base pilote »** : 3 lanes sur 4 mergées (A alias #416/ADR-031, D fix #398 #417, C bootstrap propagation #418) ; lane B = données 🎯 attendues.
- **H1 exécuté 5/5** (ROADMAP-AGENTS §4) : AN-1 émission d'events + flotte en subscribe (#401, PR #430, réalise la promesse « Streamable » posée par ADR-027) ; AN-2 scopes bout-en-bout + budgets par token (#392, PRs #434/#435, `docs/ADR-032-scope-grid-and-budgets.md`) ; SUP-1 une seule maths MRP, délégation APICS→cœur, parité vert dur CI (#423, PR #432, `docs/ADR-020-mrp-consolidation.md` PAS 4) ; DEM-1 routage tête/traîne câblé + premier watcher DEMANDE `agent_forecast_watcher` (PRs #438/#439, `docs/ADR-033-demand-routing-and-drift.md`, migration 072) ; PROD-QW paquet quick-wins prod (restore prouvé #192, pool durci, pip-audit/CodeQL, cov gate, `/v1/audit`, webhook L3 — PR #437).
- **🎯 pilote consignés par DEM-1 (registre détaillé : `docs/ADR-033-demand-routing-and-drift.md` §Réglages 🎯 résiduels — ne pas dupliquer ici)** : seuils de dérive par défaut `--mase-threshold 1.3` / `--bias-ratio-threshold 0.3` (réglables CLI) ; `tracking_ratio` normalisé par `mean_forecast` (pas `mean_actual` — `pyramide_accuracy_metrics` ne porte pas de mean-actual/MAE, migration future si le pilote tranche) ; un DRAFT `FORECAST_DRIFT` vivant (même `drift_kind`) est gelé par construction de la clé d'upsert — pas de ré-annonce/ré-stampage de métriques tant que la dérive ne change pas de nature, même si sa magnitude empire.
- **Livré depuis la photo de mai** : DRP fair-share (ADR-028), SSE `/v1/stream` (ADR-027), machine à preuve (ADR-030), overlay param scénarisé (ADR-025), reschedule + FPO (ADR-026), tokens par agent + scopes (migration 064, ADR-029), alias de site (ADR-031).
- **Prochain** : H2 (« la démo qui gagne » — SC-1 comparaison scénarios, DEM-2 avantage causal v1, SOP-1 Shipping Plan, EXP-1 surface humaine 🎯, MOAT-1 — voir ROADMAP-AGENTS §5).
