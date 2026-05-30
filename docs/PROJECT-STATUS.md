# PROJECT STATUS — ootils-core

> **Document de contrôle vivant.** Propriété de l'agent `ootils-pilote` (chef de projet).
> Source unique de vérité pour : statut, priorités, chantier actif, backlog, risques.
> À relire au début de chaque session, à mettre à jour à la fin. Les chiffres se
> revérifient en live (audit) — ce doc est le cadre, pas la mesure.

**Dernière mise à jour : 2026-05-30 (hygiène repo + bascule chantier smoke-fleet)**

---

## 0. Cap

**North Star** : substrat opérationnel déterministe piloté par une flotte d'agents.
**Wedge V1** : *Autonomous shortage control tower with scenario-backed recommendations.*
**Règle de pilotage** : WIP = 1. Un chantier fini (mergé + CI verte + doc à jour) avant le suivant.

---

## 1. Chantier ACTIF

> **Smoke-test de régression CI de la fleet d'agents (5 watchers).**
> Cadré (voir mini-plan ci-dessous). Pas encore lancé en exécution.
>
> **But** : chaque watcher s'instancie sur un jeu miniature seedé et produit un
> run gouverné valide (agent_runs COMPLETED + recommandation DRAFT/L1 avec
> evidence + confidence), sans planter, de façon idempotente. Objectif =
> verrou anti-régression silencieuse, pas couverture exhaustive métier.
>
> **Constat de cadrage** : `tests/engine_service/test_agent_workflow.py` couvre
> les primitives gRPC du moteur (fork, propagate-batch, heartbeat, get_node
> scenario-aware) — PAS les 5 watchers Python (`scripts/agent_*_watcher.py`),
> qui n'ont aujourd'hui **aucun test**. Le smoke-test est donc un NOUVEAU
> fichier (`tests/integration/test_agent_fleet_smoke.py`), pas une extension.
>
> **Écart North Star noté** (hors périmètre smoke, à traiter ensuite) : les 5
> watchers tournent sur `core.BASELINE` en dur — pas paramétrés par
> `scenario_id` en entrée. Anti-pattern « only works on baseline ». Le smoke
> verrouille le comportement actuel ; le passage scenario-first est un chantier
> distinct (lié à P2.2 overlays).

---

## 2. Livré et stable (sur `main`)

- **Ingestion V1** — 11 entités TSV canoniques, `bulk_ingest.py` (~9 000 rows/s, idempotent), `daily_load.py` (1 commande : load → LLC → cost_rollup → validate). Import quotidien **~123 s** (migration 046 a corrigé l'index manquant `nodes(parent_node_id)`).
- **Moteur MRP** — `mrp_core.py` source unique (pure, DB-free, golden-master en CI). Proration, consommation forecast, lot sizing complet, LLC, lead-times, time fences, pegging, cost-aware sourcing, dedup multi-location. Projection virtuelle (window function).
- **Outils** — `mrp_grid` (grille MPS mensuelle), `mrp_value` (valorisation $), `mrp_eando` (E&O), `mrp_projected_stock`, `shortage_scan`.
- **Fleet d'agents** (5, tous L1 DRAFT gouvernés) — shortage_watcher, material_watcher, lot_policy_watcher, dq_watcher, eando_watcher. Gouvernance : `recommendations` + `agent_runs`, state machine DRAFT→approbation.
- **Infra** — FastAPI + PostgreSQL 16, moteur Rust gRPC (ADR-017, non câblé au batch), CI verte (ruff + pytest + integration), 46 migrations, 18 ADR.
- **DB pilote** (`ootils_pilote_test`) — chargée et propre (nodes purgés 3,1 GB → 66 MB le 30/05). 36 635 items, BOM LLC max 7, couverture coût 25 %.

PRs #301→#312 mergées (sessions 27-30 mai).

---

## 3. Risques actifs

| Risque | Impact | Porteur |
|---|---|---|
| Couverture coût 25 % (9 023/36 635 items) | Valorisation sous-estimée | Données source (ERP) |
| Zéro test de la fleet d'agents (5 watchers sans aucun test) | Régression silencieuse | **EN COURS — chantier actif** (test-writer) |
| ~55 branches remote stale (squash-mergées) | Bruit | Purge batch à faire (hors GO ciblé) |
| mypy non-bloquant rouge | Dette qualité | Progressif, non urgent |

---

## 4. Backlog priorisé

### Candidats prochain chantier (P0)
1. **Unifier la vérité de demande + merge** — point d'entrée unique forecast + CO, vue réconciliée. *(Pré-sélectionné comme prochain.)*
2. ~~**Hygiène repo**~~ — **FAIT** (PR #314) : gitignore zips clients + `poc/target/` + `README.txt` ; commit `.codex/` + `AGENTS.md` ; commit sources PoC Rust ; `docs/SPEC-INTERFACES-INBOUND-V1.md` + `scripts/_run_full_scope.sh`. Dependabot : #299 (rust) fermée, #298 (cache) + #300 (pandas) rebasées. Reste : purge batch des branches remote ; merge #314.
3. **Smoke test CI de la fleet d'agents** — **PROMU CHANTIER ACTIF** (voir §1).

### Backlog technique (P1)
- `P2.2.a/b/c` — Scenario overlays (PG schema + save/load + merge).
- `LANES-LATER` — ingest distribution_links + transportation_lanes.
- `CUST-V1.1` — table customers + FK sur customer_orders.
- Revue #8 — primitive partagée governed-agent (DRY les 5 agents).
- Revue #10 — primitive unifiée de projection.

### WIP docs en décantation (P2)
- `docs/WIP-demand-module-design-session.md` (module Demand, D1-D8).
- `docs/AGENT-FLEET-CATALOG.md` (86 agents, 26 wedge V1, A1-A10).
- `docs/WIP-inbound-interfaces-spec.md` (14 interfaces, I1-I10).

---

## 5. Garde-fous opérationnels

- DB pilote sur VM 192.168.1.176, container `ootils-core-postgres-1`, DB `ootils_pilote_test`.
- **Toujours `SET statement_timeout` côté serveur** pour toute requête mutante — `timeout` host laisse des zombies qui tiennent des locks.
- `docker restart ootils-core-api-1` pour tuer des python détachés (pas de `pkill`/`ps` dans le conteneur).
- Une seule commande `docker exec` en avant-plan, jamais de boucle de polling.
- Commits/push = décision humaine. Agents = L1 DRAFT, jamais d'application directe à l'ERP.
