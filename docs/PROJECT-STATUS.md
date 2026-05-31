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

> **Module de demande (Pyramide)** — la « vérité de demande » s'est révélée être
> la **porte d'entrée du module Pyramide** (gestion avancée de la demande), pas
> un petit fix isolé. **Décisions D1-D8 + topologie de planif + 2 briques métier :
> TRANCHÉES** (session 2026-05-30). Formalisées dans **[ADR-019](ADR-019-demand-model-pyramide.md)**.
>
> **Cœur des décisions** (détail → ADR-019) :
> - On **prévoit le booking** (jamais le shipping). 3 séries : booking / shipping /
>   backlog. Le **shipping plan** (commandes + forecast netté) est la tête du MRP.
> - Prévision **granulaire & automatique** (Pyramide) au niveau Gen_Fam/Group ×
>   zone climatique, middle-out ; dimensions canal/région/client/type de commande.
> - Historique en PG (`demand_history`, hors graphe RAM) ; faits booking **et**
>   shipping ingérés de l'ERP ; backlog calculé.
> - **Topologie push** : MRP central (safety pooled) → DRP répartit vers les DC ;
>   la prévision granulaire remonte (MRP) et redescend (DRP).
> - Mesures **units + valeur** (ASP = glissant 12 mois, hors warranty $0).
> - Calendrier **S&OP** (March/June/Early Buy, phases saison) éditable par année,
>   variable ; la prévision s'y aligne.
> - **Deux modèles servis par la même prévision** : manufacturing (MRP) et
>   distribution pure (DRP seul — ex. client à 325 DC). Mutualiser le stock en
>   central + déployer via DRP = réduction de stock chiffrable (proposition de
>   valeur face aux setups décentralisés).
>
> **Constat technique sous-jacent** : 3 implémentations divergentes de la
> consommation de demande ; **aucune notion d'« actuals »** ; `_get_historical_demand`
> double-compte + lit `time_span_start` **NULL en prod** → prévision non
> fonctionnelle en production aujourd'hui.
>
> **Bug bloquant identifié (à corriger dans le chantier)** :
> `api/routers/forecasting.py:_get_historical_demand`.
>
> **Prochaine étape** : cadrer la **1ʳᵉ brique concrète** = table `demand_history`
> + import de l'extract réel (bookings + shippings + valeur), **dès l'arrivée de la
> donnée (~31/05-01/06)**. Specs `Gen_*` attendues du métier. Aucun code avant ce cadrage.

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
