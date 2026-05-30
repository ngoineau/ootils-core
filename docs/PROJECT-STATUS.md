# PROJECT STATUS — ootils-core

> **Document de contrôle vivant.** Propriété de l'agent `ootils-pilote` (chef de projet).
> Source unique de vérité pour : statut, priorités, chantier actif, backlog, risques.
> À relire au début de chaque session, à mettre à jour à la fin. Les chiffres se
> revérifient en live (audit) — ce doc est le cadre, pas la mesure.

**Dernière mise à jour : 2026-05-30 (smoke-fleet mergé #315 ; cadrage chantier demande)**

---

## 0. Cap

**North Star** : substrat opérationnel déterministe piloté par une flotte d'agents.
**Wedge V1** : *Autonomous shortage control tower with scenario-backed recommendations.*
**Règle de pilotage** : WIP = 1. Un chantier fini (mergé + CI verte + doc à jour) avant le suivant.

---

## 1. Chantier ACTIF

> **Unifier la vérité de demande** — point d'entrée unique forecast + CO, vue
> réconciliée scenario-aware. **CADRÉ** (architecte, 2026-05-30). **En attente
> d'arbitrage humain** sur 3 décisions ouvertes avant lancement implémentation.
>
> **Constat de cadrage** : la consommation de demande est correcte dans
> `mrp_core` mais **dupliquée en 3 implémentations divergentes** —
> (1) `scripts/mrp_core.py` (`load_planning_data` + `consume_demand`, vérité
> réelle des watchers, mais `scenario=BASELINE` en dur) ; (2)
> `src/ootils_core/engine/mrp/forecast_consumer.py` (2ᵉ impl APICS, doctrine
> anti-double-comptage divergente) ; (3) `api/routers/forecasting.py:_get_historical_demand`
> — **le bug vivant** : `SUM(quantity) WHERE node_type IN ('ForecastDemand','CustomerOrderDemand')`
> → double-compte prévision + actuels, sans `scenario_id`.
>
> **Périmètre V1 (proposé)** : promouvoir UNE primitive scenario-aware comme
> source unique ; exposer `GET /v1/demand/reconciled?scenario_id=…` (vue
> CO/forecast/consumed/strategy/freshness, déjà calculée en CLI par
> `mrp_demand_query.py`) ; retirer le double-comptage du router. **HORS scope** :
> `demand_history`, Pyramide, hiérarchies/réconciliation, dimensions
> channel/region (= le WIP D1-D6, autre chantier).
>
> **Décisions ouvertes (arbitrage requis)** :
> 1. **Quelle est LA primitive ?** `mrp_core.consume_demand` (scripts, vérité
>    réelle) vs `engine/mrp/forecast_consumer` (src, APICS). Reco architecte :
>    promouvoir mrp_core dans `src/`, déprécier forecast_consumer.
> 2. **Scope du read endpoint V1** : vue réconciliée forward seule, ou avec
>    freshness/confidence par série dès V1 ?
> 3. **`_get_historical_demand`** : corriger maintenant (quick win) ou geler
>    jusqu'au chantier `demand_history` ?
>
> Détail complet du plan : cadrage architecte (session 2026-05-30).

---

## 2. Livré et stable (sur `main`)

- **Ingestion V1** — 11 entités TSV canoniques, `bulk_ingest.py` (~9 000 rows/s, idempotent), `daily_load.py` (1 commande : load → LLC → cost_rollup → validate). Import quotidien **~123 s** (migration 046 a corrigé l'index manquant `nodes(parent_node_id)`).
- **Moteur MRP** — `mrp_core.py` source unique (pure, DB-free, golden-master en CI). Proration, consommation forecast, lot sizing complet, LLC, lead-times, time fences, pegging, cost-aware sourcing, dedup multi-location. Projection virtuelle (window function).
- **Outils** — `mrp_grid` (grille MPS mensuelle), `mrp_value` (valorisation $), `mrp_eando` (E&O), `mrp_projected_stock`, `shortage_scan`.
- **Fleet d'agents** (5, tous L1 DRAFT gouvernés) — shortage_watcher, material_watcher, lot_policy_watcher, dq_watcher, eando_watcher. Gouvernance : `recommendations` + `agent_runs`, state machine DRAFT→approbation. **Smoke-test de régression CI** (`tests/integration/test_agent_fleet_smoke.py`, PR #315) : contrat universel run+idempotence ×5 + assertions ciblées shortage/material/dq/eando.
- **Infra** — FastAPI + PostgreSQL 16, moteur Rust gRPC (ADR-017, non câblé au batch), CI verte (ruff + pytest + integration), 46 migrations, 18 ADR.
- **DB pilote** (`ootils_pilote_test`) — chargée et propre (nodes purgés 3,1 GB → 66 MB le 30/05). 36 635 items, BOM LLC max 7, couverture coût 25 %.

PRs #301→#312 mergées (sessions 27-30 mai).

---

## 3. Risques actifs

| Risque | Impact | Porteur |
|---|---|---|
| Couverture coût 25 % (9 023/36 635 items) | Valorisation sous-estimée | Données source (ERP) |
| ~~Zéro test de la fleet d'agents~~ | ~~Régression silencieuse~~ | **CLOS** — smoke-test mergé (PR #315, CI verte) |
| ~55 branches remote stale (squash-mergées) | Bruit | Purge batch à faire (hors GO ciblé) |
| mypy non-bloquant rouge | Dette qualité | Progressif, non urgent |

---

## 4. Backlog priorisé

### Candidats prochain chantier (P0)
1. **Unifier la vérité de demande** — **PROMU CHANTIER ACTIF** (voir §1, cadré, en attente d'arbitrage).
2. ~~**Hygiène repo**~~ — **FAIT** (PR #314 mergé). Reste : purge batch des branches remote stale.
3. ~~**Smoke test CI de la fleet d'agents**~~ — **FAIT** (PR #315 mergé, CI verte).

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
