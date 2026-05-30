# WIP — Session design module Demand (Pyramide × Ootils)

**Date** : 2026-05-25
**Statut** : Brouillon, à décanter. Aucune décision tranchée, aucune ligne de code écrite.
**Contexte** : Suite intégration proposée par agent ChatGPT du module `pyramide/`. L'utilisateur a poussé pour creuser avant d'intégrer afin d'éviter le « classique module de demande complètement déconnecté du module de planif ».

> **Lecture obligatoire avant d'avancer** : `docs/STRATEGY-autonomous-supply-chain-operations.md`
> (position paper ChatGPT, 516 lignes). Le module Demand n'est pas un module de
> prévision classique — il doit servir un **outil de planification piloté 100% par
> des agents**. Voir §0bis ci-dessous pour les implications directes sur le design Demand.

---

## 0bis. Ancrage stratégique — pourquoi le module Demand existe

Le position paper `STRATEGY-autonomous-supply-chain-operations.md` cadre Ootils
comme un **substrat opérationnel pour une supply chain pilotée par une flotte
d'agents**, pas comme un APS classique avec un module forecast à côté. Les
citations clés du paper qui contraignent le design Demand :

> « One planner supervises 10x more scope because Ootils agents absorb
> monitoring, diagnosis, simulation, and action preparation. »

> « The engine should decide what changed. Agents should decide what to do
> about it. »

> « Deterministic engine. LLMs do not own core calculations. »

> « No recommendation if the relevant source feed is stale beyond SLA. »

### Conséquences directes pour le module Demand

| Exigence stratégie | Implication design Demand |
|---|---|
| **Demand Shaping Agent** (§4.2) teste allocation / promise-date alternatives | Pyramide doit être **forkable** par scénario — un agent doit pouvoir tester une demande counter-factuelle sans toucher la baseline. Cf. **D7**. |
| **Demand Watcher implicite** (anomalies demande, §4.1) | `demand_history` doit alimenter un détecteur d'anomalies (spike, drop, drift) → agent watcher. Pas de Pyramide muet. |
| **Data Quality Watcher** (§7.2) : outliers demande, UoM incohérentes | `demand_history` doit porter UoM explicite, source_system traçable, `attrs` pour features DQ. |
| **Import Watcher** (§7.1) : freshness SLA par source/entity | Chaque source de `demand_history` (ERP, POS, WMS) doit avoir un SLA. Pas de forecast si feed stale au-delà SLA. |
| **Recommendations bloquées si DQ critique sur causal path** (§7.3) | Pyramide doit produire un **score de confiance** par série forecastée (DQ + accuracy backtest), pas juste des valeurs nues. |
| **L0-L4 Decision Ladder** (§5) | Un override de demande = L3 (planning state mutation) → human approval. Un what-if demande dans un scénario = L1/L2 (draft / scenario create) → autonome. |
| **Audit trail complet** (§13 non-négociables) | Chaque PyramideRun doit logger : input data window, method, dimensions, accuracy, qui (agent ou humain), quand, scenario_id. La table `pyramide_runs` existante le supporte déjà partiellement. |
| **Explainability mandatory** (§13) | Pyramide doit pouvoir répondre « pourquoi cette prévision ? » → décomposition (tendance + saisonnalité + promo + bruit), feature importance pour ML methods. |
| **Bench 25K SKU + 50 agents** (§9-§10) | Le design `demand_history` doit tenir l'échelle preuve : 25K SKU × 5 ans × granularité jour × dimensions = volume PG à valider en bench. |
| **StreamChanges pour agents** (§8.1) | Une mise à jour de `demand_history` doit pouvoir déclencher un re-forecast ciblé + notification agent abonné. Pas de batch nocturne. |

### Phasage Demand aligné sur la roadmap stratégique

| Phase paper | Livrable Demand correspondant |
|---|---|
| Phase A — Engine Proof | `demand_history` + Pyramide alimenté par histo séparé. Bench scale. |
| Phase B — Agent Tool Surface | Tools MCP : `query_demand_history`, `run_pyramide_forecast`, `compare_forecast_scenarios`, `flag_demand_anomaly`. Idempotency keys. |
| Phase C — First Agent Fleet | **Demand Shaping Agent** + **Demand Anomaly Watcher**. |
| Phase D — Human Control Room | Vue comparaison forecast baseline vs scénario, approval workflow override demande. |
| Phase E — Controlled Execution | Export consensus demand plan vers ERP (S&OP). |

### Wedge produit et Demand (§6)

Le wedge V1 est **« Autonomous shortage control tower »**. La demande est en
amont des shortages : sans demande fiable, pas de shortage credible. Donc le
module Demand est **fondation du wedge**, pas un ajout latéral.

**Risque à éviter** : livrer Pyramide comme un module forecast standalone
(ChatGPT-style « forecasting service ») au lieu de l'intégrer comme une couche
queryable, forkable, agent-aware du substrat planning.

---

## 0. Citations utilisateur — fil rouge de la session

> « tu ajuste tout et tu intègre a ootils, on garde le nom **pyramide** pour la gestion de la demande. Tu me présente avant d'intégrer — je veux une intégration parfaite pour éviter le classique module de demande complètement déconnecté du module de planif. »

> « avant de répondre a tes questions, le forecast est basé sur des données historiques, il faut que l'on creuse le point non? »

> « comme peux t on avoir de la demande passé par définition dans une base qui se projéte sur le futur.... »

> « si je met 5 ans de données historique je me retrouve avec 5x365xnombre de sku de nodes.... c'est délirant »

> « demande par type/channel/region/ etc... »

> « creuse la concurrence »

> « C'est chaud le module Demand il faut que je dorme dessus — tu enregistre TOUTE cette conversation il faut que je décante »

---

## 1. Diagnostic — ce qui cloche dans Ootils aujourd'hui

### 1.1 Mélange passé/futur dans `nodes`

La table `nodes` est un fourre-tout :
- `ForecastDemand` — peut avoir des `time_span_start` dans le passé ET le futur
- `CustomerOrderDemand` — statuts `DRAFT` / `CONFIRMED` / `RELEASED` / `CANCELLED` ; **pas de `SHIPPED`**
- `ProjectedInventory` — forward-looking
- ~50 autres types

Conséquence : le « passé » est de facto mélangé avec la projection.

### 1.2 Loader Rust — pas de filtre temporel

`rust/ootils_engine/src/loader.rs:155-180` :

```sql
SELECT ... FROM nodes WHERE scenario_id = $1 AND active = TRUE
```

Pas de `WHERE time_span_start >= ...` → le moteur charge tout (passé + futur) en RAM. Implique que la « full timeline » est déjà inadvertedly implémentée.

### 1.3 ForecastingEngine pioche dans `nodes` pour l'historique

`src/ootils_core/api/routers/forecasting.py:_get_historical_demand` :

```sql
SELECT time_span_start, SUM(quantity)
FROM nodes
WHERE node_type IN ('ForecastDemand', 'CustomerOrderDemand')
  AND time_span_start >= (CURRENT_DATE - INTERVAL '90 days')
GROUP BY time_span_start
```

**Bug conceptuel** : on somme prévision + actuels comme s'ils étaient interchangeables. Le forecast doit apprendre sur des FAITS, pas sur ses propres prédictions passées.

### 1.4 Maths fatales — RAM si on garde l'histo dans `nodes`

| SKU | Sites | Horizon | Bucketing | Nodes total | RAM @ ~200B/node |
|---|---|---|---|---|---|
| 10 000 | 50 | 5 ans | quotidien | 912 M | **~136 GB** |
| 10 000 | 50 | 5 ans | hebdo | 130 M | ~20 GB |
| 10 000 | 50 | 5 ans | mensuel | 30 M | ~5 GB |

Confirmé par l'utilisateur : **délirant**. L'histo ne peut pas vivre dans le graphe RAM.

### 1.5 Absence de dimensions demande

Aucune colonne ni table pour :
- channel (B2B / B2C / e-com / retail)
- region
- customer_segment
- order_type (standard / promo / projet / sample)
- source_system

Or l'utilisateur pointe : **la demande a intrinsèquement ces dimensions**.

---

## 2. Recherche concurrence — comment font les leaders

### 2.1 SAP IBP

- **Master Data Types** (MDT) : `S6PRODUCT`, `S6LOCATION`, `S6CUSTOMER`, `S6LOCATIONPRODUCT`
- **Planning Levels** : combinaison attributs × bucket temporel
- **Key Figures séparés** :
  - Forward : `Sales Forecast`, `Marketing Forecast`, `Consensus Demand Plan`, `Projected Inventory`
  - Historique : `Sales Order History`, `Shipment History`, `POS History`
- **Hiérarchies** : top-down, bottom-up, middle-out + désagrégation proportionnelle

### 2.2 o9 Solutions

- Graphe de connaissances : Dimensions + Hiérarchies + Attributs
- Réconciliation automatique entre niveaux
- Actuals vs Plans **toujours séparés**, mêmes dimensions
- Clustering attribute-based pour méthodes statistiques

### 2.3 Kinaxis Maestro

- « Single data model » côté marketing, peu de détails publics
- Continuous planning sur graphe
- Bottom-up dominant + override planner

### 2.4 Blue Yonder

- Multi-dimensionnel global : région × segment × canal × produit
- Top-down + middle-out

### 2.5 Réconciliation hiérarchique — méthodes

| Méthode | Avantage | Inconvénient |
|---|---|---|
| **Top-down** | Stable, cohérent | Biaisé sur les feuilles |
| **Bottom-up** | Précis sur feuilles | Bruit qui s'accumule en haut |
| **Middle-out** | Compromis | Choix du niveau pivot subjectif |
| **MinT (Minimum Trace)** | Mathématiquement optimal | Lourd, matrices covariance |

### 2.6 Convergence industrie

**Tous les leaders séparent** :
1. Référentiel master data dimensionnel
2. **Table de faits historiques** (sales orders, shipments) avec ces dimensions
3. Planning graph forward-looking qui consomme les agrégats

NetSuite distinguishe Orders vs Actual Sales. APS systems séparent shipments/forecasts en champs distincts.

---

## 3. Design proposé — aligné industrie

### 3.1 Séparer faits historiques du graphe planif

```sql
CREATE TABLE demand_history (
  history_id         UUID PRIMARY KEY,
  -- Clés métier
  item_id            UUID NOT NULL,
  location_id        UUID NOT NULL,
  occurred_at        DATE NOT NULL,
  -- Quoi
  quantity           NUMERIC(18,4) NOT NULL,
  uom                TEXT NOT NULL,
  event_type         TEXT NOT NULL,  -- 'SALES_ORDER' | 'SHIPMENT' | 'INVOICE' | 'POS'
  -- Dimensions demande (pattern SAP IBP MDT)
  channel            TEXT,           -- 'B2B' | 'B2C' | 'E-COM' | 'RETAIL'
  region             TEXT,
  customer_segment   TEXT,           -- 'KEY_ACCOUNT' | 'SMB' | ...
  customer_id        UUID,
  order_type         TEXT,           -- 'STANDARD' | 'PROMO' | 'PROJECT' | 'SAMPLE'
  source_system      TEXT NOT NULL,  -- 'ERP_SAP' | 'POS_X' | 'MANUAL'
  -- Extension libre sans migration
  attrs              JSONB NOT NULL DEFAULT '{}'::jsonb,
  -- Audit
  ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_ref         TEXT  -- N° commande externe pour idempotence
);

CREATE INDEX ix_dh_item_loc_date    ON demand_history (item_id, location_id, occurred_at);
CREATE INDEX ix_dh_date             ON demand_history (occurred_at);
CREATE INDEX ix_dh_channel_region   ON demand_history (channel, region, occurred_at);
CREATE INDEX ix_dh_attrs            ON demand_history USING gin (attrs);
```

**Conséquences** :
- Histo en PG, pas en RAM Rust → de 136 GB à quelques GB indexés
- Pyramide tire via `SELECT SUM(quantity) FROM demand_history WHERE ... GROUP BY bucket`
- `nodes` reste forward-only, loader Rust ajoute `WHERE time_span_start >= CURRENT_DATE - INTERVAL '30 days'`

### 3.2 Hiérarchies génériques

```sql
CREATE TABLE dimension_hierarchy (
  dimension      TEXT NOT NULL,   -- 'item' | 'location' | 'customer'
  member_id      TEXT NOT NULL,
  parent_id      TEXT,
  level_name     TEXT NOT NULL,   -- 'SKU' | 'FAMILY' | 'CATEGORY' | 'DC' | 'REGION'
  level_depth    INT NOT NULL,
  PRIMARY KEY (dimension, member_id)
);
```

Permet réconciliation sans profondeur codée en dur.

### 3.3 Pyramide — config étendue

```
PyramideRunConfig:
  ├─ granularity:        DAILY | WEEKLY | MONTHLY
  ├─ horizon:            int
  ├─ method:             AUTO_SELECT | EXP_SMOOTHING | CROSTON | ML_LGBM | ...
  ├─ aggregation_level:  {item_id, location_id, channel?, region?, segment?}
  ├─ history_window:     int (mois)
  ├─ history_filter:     EventType[]   -- typiquement [SHIPMENT]
  └─ reconciliation:     BOTTOM_UP | TOP_DOWN | MIDDLE_OUT | MIN_T
```

Pyramide émet des `forecasts` (table existante 026), matérialisés en `ForecastDemand` nodes via le pont déjà fait par l'agent ChatGPT dans `pyramide/repository.py`.

### 3.4 Flux complet aligné industrie

```
┌─────────────────┐    ingestion       ┌──────────────────┐
│ ERP / POS / WMS │ ───────────────►   │ demand_history   │ (PG, dimensionnel)
└─────────────────┘                    └─────────┬────────┘
                                                 │ SELECT GROUP BY
                                                 ▼
                                       ┌──────────────────┐
                                       │ Pyramide runner  │ (AUTO_SELECT, MinT)
                                       └─────────┬────────┘
                                                 │ write
                                                 ▼
                                       ┌──────────────────┐
                                       │ forecasts +      │ (PG, header/lines)
                                       │ forecast_values  │
                                       └─────────┬────────┘
                                                 │ materialize
                                                 ▼
                                       ┌──────────────────┐
                                       │ nodes            │ (PG, forward-only)
                                       │  ForecastDemand  │
                                       └─────────┬────────┘
                                                 │ load forward window
                                                 ▼
                                       ┌──────────────────┐
                                       │ Rust engine RAM  │ (ArcSwap, propagation)
                                       └──────────────────┘
```

---

## 4. Décisions à trancher — D1 à D8

| # | Question | Recommandation | Alternative |
|---|---|---|---|
| **D1** | Filtrage historique | `event_type IN ('SHIPMENT')` par défaut, configurable par run | actuals_only mixte |
| **D2** | Dimensions MVP | `channel`, `region`, `customer_segment` colonnes ; reste `attrs` JSONB | Tout JSONB |
| **D3** | Hiérarchies | Table `dimension_hierarchy` générique | Colonnes en dur item_family/location_region |
| **D4** | Réconciliation MVP | **Bottom-up** par défaut + middle-out optionnel ; MinT en P2 | MinT d'emblée |
| **D5** | Window RAM Rust | `WHERE time_span_start >= CURRENT_DATE - INTERVAL '30 days'` | Garder full timeline |
| **D6** | Sources MVP | Dataset generator synthétique + ingestion CSV | Connecteur ERP réel |
| **D7** | Counter-factual demande | Overlay scenario surcharge `ForecastDemand` nodes (pattern existant) | Mécanisme « what-if forecast » dédié |
| **D8** | Statut SHIPPED manquant | Ajouter `SHIPPED` + trigger push vers `demand_history` | Ne traiter que ingestions externes |

---

## 5. Plan d'intégration (si validation D1-D8)

1. Migration `039_demand_history.sql` — table faits + hiérarchies + index
2. Migration `040_customer_order_shipped.sql` — statut SHIPPED + trigger d'extraction
3. Refactor `_get_historical_demand` — lire `demand_history`
4. Pyramide runner — ajouter `aggregation_level` + `history_filter` à `PyramideRunConfig`
5. Module `pyramide/reconciliation.py` — bottom-up + middle-out
6. Loader Rust — fenêtre forward-only configurable (default 30j)
7. Tests E2E — dataset histo synthétique → Pyramide → matérialisation → propagation → vérif cohérence
8. Bench — vérifier enveloppe RAM Rust avec window 30j

**Aucune ligne de code tant que D1-D8 ne sont pas tranchés.**

---

## 6. Points ouverts pour la décantation

- **MDT vs colonnes en dur** : si on prévoit que les dimensions vont bouger (nouveaux canaux, nouveaux segments), `attrs` JSONB couvre. Mais perf indexation moins fine que colonnes.
- **Niveau d'agrégation Pyramide** : faut-il toujours forecaster au SKU×site×canal×région, puis désagréger ? Ou laisser l'utilisateur choisir le niveau de forecast et la stratégie de désagrégation ?
- **Promotions / événements** : ajouter une table `demand_events` (promo, lancement, fin de vie) qui module Pyramide ? Ou rester sur features ML pures ?
- **Cohérence ATP/CTP** : la demande counter-factual via overlay doit-elle se propager à l'allocation client ? Couplage avec ADR-018.
- **Backtest / accuracy** : prévoir dès le MVP un mécanisme de backtest (MAPE, WAPE, bias) qui consomme `demand_history` ?
- **Multi-currency / multi-uom** : ignoré dans le design ci-dessus, à clarifier.
- **Permissions / multi-tenant** : `demand_history.source_system` suffit-il ou faut-il un `tenant_id` explicite ?

### Points ouverts liés à la dimension agent-first (cf. §0bis)

- **Confidence score forecast** : structure data (par série ? par bucket ? par horizon ?), seuils de blocage agent.
- **Tools MCP Demand à exposer** : liste précise et signatures pour Phase B.
- **Demand Anomaly Watcher** : règles de détection (z-score ? changepoint ? ML dédié ?) — versus heuristiques simples au début.
- **StreamChanges pour Demand** : faut-il un canal dédié `demand.history.appended` / `demand.forecast.published` distinct des changes graphe ?
- **Idempotency `source_ref`** : politique précise (replace si même ref ? interdire ? versionner ?).
- **SLA freshness par source** : où stocker (table `import_sources` ?), qui les définit, qui les modifie.
- **Counter-factual demande + scenario fork** : la demande dans un scénario peut-elle diverger de la demande baseline sans rejouer tout Pyramide ? Overlay sur `ForecastDemand` nodes uniquement, ou besoin d'un overlay sur `forecasts` aussi ?

---

## 7. État codé actuel (rappel pour reprise)

- Module `src/ootils_core/pyramide/` posé par agent ChatGPT (models, engines, runner, repository)
- Migration `038_pyramide.sql` posée (pyramide_runs, pyramide_snapshots, pyramide_snapshot_demand_nodes)
- FK vers `forecasts(forecast_id)` (migration 026)
- **Aucune intégration faite côté graphe / loader / Rust engine**
- **Tâches P2.2.a / P2.2.b / P2.2.c (PG overlays) toujours pending**

---

## 8. Pour la reprise

Quand tu reprends :
1. Relire §1 (diagnostic) et §4 (D1-D8) — c'est le cœur
2. Trancher D1-D8 (ou demander des arbitrages complémentaires)
3. Décider si on bloque P2.2.a-c en attendant ou si on parallélise
4. M'envoyer les choix, j'enchaîne sur le plan §5
