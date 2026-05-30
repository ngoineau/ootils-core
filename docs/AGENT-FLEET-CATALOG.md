# Agent Fleet Catalog — Ootils

**Date** : 2026-05-25
**Statut** : Brouillon V0. Première liste exhaustive proposée pour validation.
**Cadre** : `CLAUDE.md` § North Star + `docs/STRATEGY-autonomous-supply-chain-operations.md` §4.

> Objectif : recenser tous les agents nécessaires pour faire fonctionner Ootils
> comme **outil de planification piloté par agents**. Le paper stratégique
> donne les grandes familles (§4) ; ce catalogue les détaille un par un et en
> ajoute des nouveaux que le périmètre fonctionnel d'Ootils (MRP, RCCP, ghost,
> calendar, DQ, staging, Pyramide…) impose.

---

## 0. Légende

- **Type** :
  - W = Watcher (continu, event-driven, read-heavy)
  - S = Scenario (génère/teste corrections dans des forks)
  - G = Governance (vérifie politique / impact / sécurité avant présentation)
  - O = Orchestrator (traffic control)
  - R = Reporting / Audit (synthèse post-fait)
- **L** = Decision Ladder level (L0 read-only, L1 draft reco, L2 internal low-risk write, L3 planning state mutation, L4 external execution)
- **Phase** :
  - V1 = wedge « Autonomous Shortage Control Tower »
  - V2 = extension après preuve V1
  - V3 = ambition long terme
- **Domaine Ootils** : module / capacité concerné

---

## 1. Watchers — observation continue (event-driven)

| # | Nom | L | Phase | Domaine Ootils | Scope / signaux observés |
|---|---|---|---|---|---|
| W01 | **Shortage Watcher** | L0 | V1 | `kernel/shortage` | Top shortages baseline, nouveaux shortages, shortages résolus, drift sévérité |
| W02 | **Service Risk Watcher** | L0 | V1 | `customer_order_demand` + projection | Commandes à risque, violations promised-date, OTIF prévisionnel |
| W03 | **Supply Watcher** | L0 | V1 | `purchase_orders`, `supplier_*` | PO en retard, suppliers faibles, confirmations manquantes, drift lead-time |
| W04 | **Inventory Watcher** | L0 | V2 | `nodes` (OnHand, ProjectedInventory) | Excess, obsolète, stock négatif, stranded, vieillissement, slow-mover |
| W05 | **Capacity Watcher** | L0 | V2 | `kernel/rccp` | Ressources surchargées, bottlenecks CRP/RCCP, drift utilisation |
| W06 | **Import Watcher** | L0 | V1 | `staging/`, `import_*` | Feeds en retard, row-count drift, schéma drift, duplicates, partial loads, propagation non déclenchée |
| W07 | **Data Quality Watcher** | L0 | V1 | `dq/` | Master data manquant, lead-times suspects, UoM incohérents, BOM cycles, paramètres unsafe |
| W08 | **Parameter Watcher** | L0 | V2 | `kernel/calc`, MOQ/SS/lot | Drift safety stock, MOQ/lot/lead-time anormaux vs historique |
| W09 | **Demand Anomaly Watcher** ★ | L0 | V1 | Pyramide + `demand_history` | Spike, drop, changepoint, drift saisonnalité, outliers par série |
| W10 | **Forecast Accuracy Watcher** ★ | L0 | V2 | Pyramide + backtest | MAPE/WAPE/bias drift, séries où la méthode AUTO_SELECT échoue, dégradation continue |
| W11 | **BOM Health Watcher** ★ | L0 | V2 | `kernel/mrp` | Cycles, composants inactifs, BOM stale, substitutions non maintenues |
| W12 | **Calendar Coherence Watcher** ★ | L0 | V2 | `kernel/calendar` | Calendriers invalides, dates fournisseur sur jour fermé, drift planning vs réel |
| W13 | **Lead Time Watcher** ★ | L0 | V2 | Histo PO vs paramètre | Écart lead-time réel vs paramétré, drift par supplier/SKU |
| W14 | **Ghost Watcher** ★ | L0 | V2 | `engine/ghost` | Suppliers virtuels surcouvrant des shortages réels, ghosts orphelins |
| W15 | **Scenario Hygiene Watcher** ★ | L0 | V2 | `scenario/`, Rust overlays | Scénarios stagnants, overlays qui grossissent, TTL non rafraîchi, scenarios morts |
| W16 | **Promo / Event Watcher** ★ | L0 | V3 | future `demand_events` | Promos à venir non répercutées dans forecast, événements terminés à nettoyer |
| W17 | **Allocation Watcher** ★ | L0 | V3 | `kernel/allocation` | Allocations injustes, customers premium sous-servis, allocations stales |
| W18 | **WAL / Engine Health Watcher** ★ | L0 | V1 | Rust engine | WAL size drift, write-behind lag, queue depth, propagate p95 drift |

★ = ajouts par rapport au paper stratégique §4.1

---

## 2. Scenario Workers — proposent et testent des corrections

| # | Nom | L | Phase | Domaine | Action testée |
|---|---|---|---|---|---|
| S01 | **Expedite Agent** | L1 | V1 | PO / supplier | Avancer date PO, alternate supplier, split PO |
| S02 | **Reallocation Agent** | L1 | V1 | Inter-site | Déplacer stock entre sites, transferts inter-DC |
| S03 | **Substitution Agent** | L1 | V2 | BOM / item | Composant alternatif, variant BOM, item de substitution |
| S04 | **Capacity Shift Agent** | L1 | V2 | RCCP | Overtime, work-center alternatif, re-séquencement |
| S05 | **Lot Size Agent** | L1 | V2 | Paramètres | MOQ, lot fixe, période de regroupement |
| S06 | **Demand Shaping Agent** ★ | L1 | V2 | Allocation client | Allocation alternative, promise-date négociée, priorisation |
| S07 | **Safety Stock Agent** ★ | L1 | V2 | Paramètres SS | Recalibration SS par segment service / volatility |
| S08 | **Forecast Override Agent** ★ | L1 | V2 | Pyramide | Tester un consensus demand plan ajusté (manual override + reco) |
| S09 | **Supplier Switch Agent** ★ | L1 | V3 | Sourcing | Switch supplier préféré sur SKU à risque, dual sourcing |
| S10 | **Promotion Impact Agent** ★ | L1 | V3 | Demand + supply | Tester un plan supply en réponse à promo planifiée |
| S11 | **New Product Intro Agent** ★ | L1 | V3 | NPI | Tester rampe-up, pre-build, transition phase-in/phase-out |
| S12 | **End-of-Life Agent** ★ | L1 | V3 | Phase-out | Tester run-down stock, dernier achat, transfert |
| S13 | **What-If Macro Agent** ★ | L1 | V3 | Top-down | Tester scénario macro (récession demande -15%, supplier KO) |

★ = ajouts par rapport au paper stratégique §4.2

---

## 3. Governance — vérifient avant présentation / application

| # | Nom | L | Phase | Domaine | Vérifie |
|---|---|---|---|---|---|
| G01 | **Policy Agent** | L0 | V1 | Policy engine | Action autorisée par politique / scope / rôle |
| G02 | **Finance Agent** | L0 | V2 | Finance | Impact working capital, marge, cash, écart vs budget |
| G03 | **Customer Agent** | L0 | V1 | Customer | Impact service / customer key account, SLA contractuel |
| G04 | **Supplier Agent** | L0 | V2 | Supplier | Faisabilité supplier, risque commercial, MOQ |
| G05 | **Audit Agent** | L0 | V1 | Audit | Trace évidence, fraîcheur data, qualité explication |
| G06 | **DQ Confidence Gate** ★ | L0 | V1 | DQ + causal path | Recommandation bloquée si DQ critique sur causal path |
| G07 | **Import Freshness Gate** ★ | L0 | V1 | Import health | Recommandation bloquée si feed source stale > SLA |
| G08 | **Forecast Confidence Gate** ★ | L0 | V2 | Pyramide accuracy | Recommandation labellisée DRAFT_LOW_CONFIDENCE si MAPE > seuil |
| G09 | **Conflict Resolver** ★ | L0 | V2 | Cross-agent | Si 2 scenario agents proposent actions opposées sur même SKU, arbitre ou escale |
| G10 | **Budget Enforcer** ★ | L0 | V1 | Runtime | Coupe agent si dépassement tokens / wall-clock / scenario count |

★ = ajouts par rapport au paper stratégique §4.3

---

## 4. Orchestrator(s)

| # | Nom | L | Phase | Responsabilité |
|---|---|---|---|---|
| O01 | **Orchestrator** | L0/L1 | V1 | Dispatcher issues → agents, dédupe investigations, stop runaway loops, consolide recommandations, escalade |
| O02 | **Domain Orchestrator (Shortage)** ★ | L0/L1 | V2 | Orchestrator spécialisé wedge shortage — séquence Watch → Diagnose → Scenario × N → Govern → Recommend |
| O03 | **Domain Orchestrator (Inventory)** ★ | L0/L1 | V3 | Idem pour campagne réduction inventory |
| O04 | **S&OP Cycle Orchestrator** ★ | L0/L1 | V3 | Orchestre un cycle S&OP mensuel : consensus demand → supply review → exec review |

★ = ajouts par rapport au paper stratégique §4.4

---

## 5. Reporting / Audit (agents périodiques, non event-driven)

| # | Nom | L | Phase | Sortie |
|---|---|---|---|---|
| R01 | **Daily Briefing Agent** ★ | L0 | V1 | Synthèse matinale : top shortages, recos en attente, anomalies overnight |
| R02 | **Recommendation Outcome Tracker** ★ | L0 | V2 | Mesure accepted/rejected, false positive rate, impact business réel post-action |
| R03 | **Agent Fleet Performance Auditor** ★ | L0 | V2 | Mesure perf de la flotte elle-même : agents qui produisent du bruit, agents inactifs, drift policy |
| R04 | **KPI Attribution Agent** ★ | L0 | V3 | Attribue les variations KPI (service, working capital) aux actions agent vs causes externes |
| R05 | **S&OP Reporter** ★ | L0 | V3 | Compile pack S&OP : demand plan, supply plan, gaps, recos, capacity outlook |

★ = ajouts par rapport au paper stratégique

---

## 5bis. Pilotage des interfaces (domaine transverse) ★

> Famille **dédiée** : Ootils est piloté à travers ses interfaces (ERP, WMS, MES,
> TMS, EDI, API REST, UI, portails fournisseur/client, MCP tools…). Le paper
> stratégique ne traite que l'**Import Watcher** (§7.1). En réalité **piloter
> les interfaces = un domaine complet** : santé contractuelle, schema drift,
> idempotency, replay, throttling, mapping, traduction, latence, versioning.

### 5bis.a Watchers interfaces

| # | Nom | L | Phase | Scope |
|---|---|---|---|---|
| I-W01 | **Interface Health Watcher** | L0 | V1 | Disponibilité endpoints (in/out), erreurs HTTP, latence p50/p95, retry rate, circuit breakers |
| I-W02 | **Schema Drift Watcher** | L0 | V2 | Schémas inbound (CSV, JSON, EDI) qui changent silencieusement vs contrat |
| I-W03 | **Idempotency Conflict Watcher** | L0 | V2 | Mêmes `source_ref` avec payloads divergents, replays détectés |
| I-W04 | **Mapping Drift Watcher** | L0 | V2 | Codes source (centre coût, item code, location code) inconnus dans le mapping → enrichissement nécessaire |
| I-W05 | **Outbound Backlog Watcher** | L0 | V2 | Files d'export ERP en retard (recommandations approuvées non poussées) |
| I-W06 | **API Consumer Watcher** | L0 | V2 | Patterns suspects côté consommateur API (rate-limit hit, scopes refusés, agents qui sortent du budget) |
| I-W07 | **UI Workflow Watcher** | L0 | V3 | Planners bloqués sur même écran > N min, abandons formulaire, taux rejet recommandation par UI |

### 5bis.b Scenario interfaces

| # | Nom | L | Phase | Action |
|---|---|---|---|---|
| I-S01 | **Replay Agent** | L2 | V2 | Re-rejouer un batch import après correction mapping/schéma, en idempotent |
| I-S02 | **Mapping Repair Agent** | L1 | V2 | Propose un mapping pour codes inconnus en se basant sur fuzzy + historique |
| I-S03 | **Schema Migration Agent** | L1 | V3 | Détecte un schema drift → propose mise à jour contrat + tests rétro-compatibilité |
| I-S04 | **Backfill Agent** | L1 | V2 | Comble un trou de données histo (gap detection) en rappatriant via interface |

### 5bis.c Governance interfaces

| # | Nom | L | Phase | Vérifie |
|---|---|---|---|---|
| I-G01 | **Contract Compliance Gate** | L0 | V2 | Tout payload inbound conforme au contrat versionné publié |
| I-G02 | **Outbound Policy Gate** | L0 | V1 | Aucun export vers ERP sans approbation humaine valide (L4 non-négociable) |
| I-G03 | **Scope Enforcer** | L0 | V1 | Tool call MCP respecte le scope agent (read-only / scenario-only / baseline) |

**Total Pilotage Interfaces** : 14 agents.

---

## 5ter. Pilotage des paramètres (domaine transverse) ★

> Famille **dédiée** : les paramètres planning (safety stock, MOQ, lot sizing,
> lead times, ABC/XYZ, calendriers, allocation rules, reorder points,
> coverage targets…) sont la **manette principale** d'un système de planif.
> Ils dérivent en permanence avec la réalité (lead time supplier qui glisse,
> volatility demande qui change, MOQ contrat renégocié…). Sans pilotage agent,
> ces paramètres pourrissent et le système prend de mauvaises décisions
> confiantes.
>
> Note : actions L3 (mutation paramètre baseline) → **toujours human approval**
> dans la V1. Les agents proposent, calculent l'impact, justifient. L'humain
> tranche.

### 5ter.a Watchers paramètres

| # | Nom | L | Phase | Scope |
|---|---|---|---|---|
| P-W01 | **Lead Time Drift Watcher** | L0 | V1 | Lead time réel (PO histo) vs paramétré, par supplier/item, drift statistique |
| P-W02 | **Safety Stock Adequacy Watcher** | L0 | V2 | SS actuel vs niveau de service réel observé, sur/sous-couverture |
| P-W03 | **MOQ / Lot Size Watcher** | L0 | V2 | MOQ/lot fixe qui crée du sur-stock chronique ou des shortages chroniques |
| P-W04 | **ABC/XYZ Drift Watcher** | L0 | V2 | Items qui ont changé de classe (B→A, X→Z) sans relabel + ajustement politique |
| P-W05 | **Coverage Target Watcher** | L0 | V2 | Cibles de couverture vs réalisé, drift par segment |
| P-W06 | **Calendar Drift Watcher** | L0 | V2 | Jours ouvrés réels (livraisons effectives) vs calendar paramétré |
| P-W07 | **Allocation Rule Watcher** | L0 | V3 | Règles d'allocation qui produisent des résultats hors politique (key account sous-servi récurrent) |
| P-W08 | **Reorder Point Watcher** | L0 | V2 | ROP qui se déclenche trop tôt / trop tard vs cycle réel |
| P-W09 | **Parameter Coherence Watcher** | L0 | V1 | Cohérence inter-paramètres (SS < cycle stock pour A-item, MOQ > 6 mois demande, lead time > horizon forecast…) |
| P-W10 | **Parameter Change Audit Watcher** | L0 | V1 | Qui a changé quoi quand, fréquence anormale de changements, changements hors gouvernance |

### 5ter.b Scenario paramètres

| # | Nom | L | Phase | Action proposée |
|---|---|---|---|---|
| P-S01 | **Safety Stock Recalibration Agent** | L1 | V2 | Recalcul SS par segment service × volatility demande × lead-time variability |
| P-S02 | **Lead Time Recalibration Agent** | L1 | V1 | Recalcul lead-time paramétré à partir d'histo PO (median, p90, distribution) |
| P-S03 | **MOQ Negotiation Agent** | L1 | V3 | Identifie MOQ où renégociation supplier aurait gros impact, prépare cas business |
| P-S04 | **Lot Sizing Optimizer** | L1 | V2 | Tester EOQ, Wagner-Whitin, dynamic lot sizing sur famille d'items |
| P-S05 | **ABC/XYZ Reclassification Agent** | L1 | V2 | Propose nouvelle classification + politiques associées (SS, ROP, cycle counting) |
| P-S06 | **Coverage Target Optimizer** | L1 | V3 | Propose cibles couverture par segment item × location × customer pour atteindre OTIF cible |
| P-S07 | **Calendar Tuner** | L1 | V2 | Propose ajustement calendar (jours fériés, fermetures, capacité réduite saisonnière) |
| P-S08 | **Parameter Bundle Agent** ★★ | L1 | V2 | Quand on touche un paramètre, propose le **bundle** cohérent (changer lead-time → recalcul SS + ROP automatique pour rester cohérent) |

### 5ter.c Governance paramètres

| # | Nom | L | Phase | Vérifie |
|---|---|---|---|---|
| P-G01 | **Parameter Change Policy Gate** | L0 | V1 | Changement paramètre conforme à la politique (qui peut changer quoi, ampleur max, fréquence min) |
| P-G02 | **Parameter Impact Estimator** | L0 | V2 | Avant approbation : impact estimé du changement sur inventory, service, cost, capacity (via scenario fork) |
| P-G03 | **Parameter Approval Workflow** | L0 | V1 | State machine : `proposed → reviewed → approved → applied → outcome_tracked` |
| P-G04 | **Parameter Rollback Agent** | L1 | V2 | Si paramètre changé produit outcome négatif vs prédiction → propose rollback |

**Total Pilotage Paramètres** : 14 agents.

---

## 6. Récap volumétrique

| Type | Coeur §§1-5 | Interfaces §5bis | Paramètres §5ter | Total | Dont V1 |
|---|---|---|---|---|---|
| Watchers | 18 | 7 | 10 | **35** | 12 |
| Scenario Workers | 13 | 4 | 8 | **25** | 3 |
| Governance | 10 | 3 | 4 | **17** | 9 |
| Orchestrator | 4 | – | – | **4** | 1 |
| Reporting | 5 | – | – | **5** | 1 |
| **Total** | **50** | **14** | **22** | **86** | **26** |

→ On dépasse largement les « 50 agents » du paper stratégique. C'est normal :
le paper sous-traitait deux domaines transverses (interfaces, paramètres) qui
sont en réalité de **vraies familles**. La cible « 50 » devient une cible
**runtime simultanée** (agents actifs concurremment), pas une cible
catalogue. Le catalogue est un **registry** ; le scheduler instancie seulement
ce qui est utile à un moment donné.

### Cible runtime simultanée vs catalogue

- **Catalogue** = 86 personas définies (limite « ce qu'Ootils sait faire »)
- **Runtime nominal** = ~50 agents actifs en régime stable (V3)
- **Runtime burst** = jusqu'à +30 scenario workers temporaires sur événement
- **Runtime V1** = 17-26 agents

### Flotte V1 minimale (wedge Shortage Control Tower)

```
Watchers coeur (7) :
  Shortage, Service Risk, Supply, Import, DQ, Demand Anomaly, WAL/Engine Health
Watchers interfaces (1) :
  Interface Health
Watchers paramètres (4) :
  Lead Time Drift, Parameter Coherence, Parameter Change Audit, (+ 1 placeholder)

Scenario coeur (2) :
  Expedite, Reallocation
Scenario paramètres (1) :
  Lead Time Recalibration

Governance coeur (6) :
  Policy, Customer, Audit, DQ Confidence Gate, Import Freshness Gate, Budget Enforcer
Governance interfaces (2) :
  Outbound Policy Gate, Scope Enforcer
Governance paramètres (1) :
  Parameter Change Policy Gate
  + Parameter Approval Workflow

Orchestrator (1) :
  Generalist Orchestrator
Reporting (1) :
  Daily Briefing
```

**26 agents pour le wedge V1.** Ratio cohérent avec §8 du paper (« 10-15 watchers + 10-20 scenario + 5-10 governance + 1-3 orchestrators »).

Note : Pilotage interfaces et paramètres sont **co-fondateurs du wedge**, pas du
nice-to-have. Sans Interface Health Watcher + Outbound Policy Gate, aucune
recommandation ne peut sortir vers ERP en sécurité. Sans Lead Time Drift
Watcher + Parameter Coherence Watcher, les recommandations expedite seront
fausses parce que basées sur des lead-times périmés.

---

## 7. Décisions ouvertes à trancher

| # | Question | Recommandation |
|---|---|---|
| **A1** | Le catalogue est-il complet pour V1 ? | Manque-t-il un agent évident pour le wedge shortage ? |
| **A2** | Les 17 V1 sont-ils tous nécessaires d'emblée, ou peut-on découper en V1.0 / V1.1 ? | V1.0 = 10 (Shortage, Service Risk, Supply, Import, DQ, Expedite, Policy, Customer, Audit, Orchestrator) ; V1.1 = + Demand Anomaly, Reallocation, gates, Budget Enforcer, Daily Briefing, WAL/Engine Health |
| **A3** | Chaque agent vit dans son propre process / thread ? Ou flotte = N "personas" servies par un même runtime ? | Runtime partagé (économie tokens + cache MCP tools), persona = config + scope. Cf. §8 « 50 agents ≠ 50 LLM loops indépendants » |
| **A4** | Backend LLM par agent (Claude / GPT / local) configurable ? | Oui, par persona. Watchers = small/fast model ; Scenario/Governance = large model |
| **A5** | Quelle est l'unité de coût à budgéter ? Tokens, RPC engine, scénarios créés ? | Les trois : `budget = {tokens, scenarios_created, wall_clock_s, write_actions}` par agent et par cycle business |
| **A6** | Granularité audit log : 1 ligne par tool call ou 1 ligne par décision agent ? | Les deux niveaux. Tool call log (technique) + decision log (métier avec evidence + recommendation_id) |
| **A7** | Où vit le registry agents ? Table PG `agents` ? Config YAML ? | Table PG `agents` + YAML import au boot pour init |
| **A8** | Kill switch : global, par type, par agent, par scope ? | Tous les 4 niveaux. Plus per-tenant en V3 |
| **A9** | Stream input : un seul bus d'événements ou un par domaine ? | Bus unique avec topics (`shortage.detected`, `import.batch.completed`, `demand.anomaly.flagged`, etc.). Agents s'abonnent par topic |
| **A10** | Tests : comment simuler 50 agents en CI ? | Mock LLM responses + dataset synthétique + assertions sur état attendu. Cf. §10.2 du paper |

---

## 8. Architecture runtime — esquisse à creuser

```
┌─────────────────────────────────────────────────────────────┐
│                  Agent Runtime (Phase B)                    │
│                                                             │
│  ┌──────────┐   ┌──────────┐   ┌──────────────┐            │
│  │ Registry │   │  Budget  │   │ Kill switch  │            │
│  │   (PG)   │   │ enforcer │   │              │            │
│  └────┬─────┘   └────┬─────┘   └──────┬───────┘            │
│       │              │                 │                    │
│  ┌────▼──────────────▼─────────────────▼──────────┐        │
│  │           Agent Scheduler / Dispatcher          │        │
│  └────┬────────────────────────────────────────────┘        │
│       │ spawn / route                                       │
│  ┌────▼────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ Watcher A   │  │ Scenario B   │  │ Governance C │       │
│  │ (persona)   │  │ (persona)    │  │ (persona)    │       │
│  └────┬────────┘  └─────┬────────┘  └─────┬────────┘       │
│       │ tool calls (MCP)                                    │
└───────┼─────────────────┼──────────────────┼────────────────┘
        │                 │                  │
        ▼                 ▼                  ▼
┌────────────────────────────────────────────────────┐
│         MCP Tool Server (curated surface)          │
│   query_shortages, get_node, fork_scenario,        │
│   propagate, query_demand_history, ...             │
└──────────┬─────────────────────────────────────────┘
           │
           ▼
┌──────────────────┐     ┌──────────────────┐
│  Rust engine     │     │  PostgreSQL      │
│  (gRPC)          │     │  (history + DQ)  │
└──────────────────┘     └──────────────────┘
           │
           ▼
   StreamChanges → agents abonnés
```

---

## 9. Pour la décantation

- Valider / amender le catalogue (§§1-5)
- Trancher A1-A10
- Identifier les tools MCP minimum à exposer pour la flotte V1.0
- Décider le phasage V1.0 / V1.1 / V2 / V3
- Décider si on documente chaque agent dans un fichier `agents/<name>.md` (persona + scope + tools + budget) — recommandé
