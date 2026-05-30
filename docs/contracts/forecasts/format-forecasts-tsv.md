# Format de fichier — `forecasts.tsv`

> Fichier des **prévisions de demande** par article × site × bucket temporel.
> Complète les `customer_orders.tsv` au-delà de l'horizon ferme (les forecasts couvrent l'horizon où la demande n'est pas encore matérialisée en commandes).
> Endpoint cible : `POST /v1/ingest/forecast-demand`.

---

## 1. Caractéristiques du fichier

| Propriété | Valeur |
|---|---|
| **Nom** | `forecasts.tsv` (exact) |
| **Format** | TSV |
| **Encodage** | UTF-8 (BOM toléré) |
| **Délimiteur** | tabulation (`\t`) |
| **Header** | ligne 1 obligatoire |
| **Endpoint** | `POST /v1/ingest/forecast-demand` |

---

## 2. Nature de la donnée

Une ligne = **une prévision de demande agrégée sur un bucket temporel** :
> « Sur la `bucket_date` (début de période), on prévoit `quantity` unités de `item_external_id` à `location_external_id`, sur une fenêtre de `time_grain` (jour/semaine/mois). »

C'est ce qui complète la projection forward côté **demande** au-delà des commandes clients fermes.

### Quand utiliser forecasts vs customer_orders

```
                Horizon
   J ───────────────────────────────────►
   │                                      │
   │  Commandes fermes (CO)              │
   │  ──────────                          │
   │           Mix CO + forecast           │
   │           (consommation forecast par CO)
   │                       ──────────────  │
   │                       Forecast seul    │
   │                                       │
```

- Sur l'horizon court (0-4 semaines) : surtout des CO
- Sur l'horizon moyen (1-3 mois) : mix CO + forecast (le forecast est « consommé » par les CO au fur et à mesure)
- Sur l'horizon long (3+ mois) : surtout du forecast

→ Stratégie de consommation pilotée par `item_planning_params.forecast_consumption_strategy` (`consume_forward`, `consume_backward`, `max_only`, `consume_both`).

---

## 3. Colonnes

**Ordre figé par le contrat canonique** : `item_external_id`, `location_external_id`, `quantity`, `bucket_date`, `time_grain`, `source`.

| # | Colonne | Obligatoire | Type | Domaine | Description |
|---|---|---|---|---|---|
| 1 | `item_external_id` | **oui** | texte | FK `items` | Article prévu. |
| 2 | `location_external_id` | **oui** | texte | FK `locations` | Site de consommation prévu. |
| 3 | `quantity` | **oui** | décimal | ≥ 0 (zéro autorisé) | Quantité prévue sur le bucket. |
| 4 | `bucket_date` | **oui** | date | ISO `YYYY-MM-DD` | Date de **début** du bucket. |
| 5 | `time_grain` | non | enum | `day` \| `week` \| `month` \| `exact_date` \| `timeless` | Voir §4. Défaut : `week`. |
| 6 | `source` | non | enum | `statistical` \| `consensus` \| `manual` \| `ml` | Origine du forecast. Défaut : `statistical`. |

---

## 4. Les 5 `time_grain`

| Valeur | Sémantique | Fenêtre couverte |
|---|---|---|
| `exact_date` | La quantité est attendue **pile à cette date** | 1 jour (la date elle-même) |
| `day` | Prévision quotidienne | `bucket_date` → `bucket_date` |
| `week` | Prévision hebdomadaire (défaut) | `bucket_date` → `bucket_date + 6 jours` |
| `month` | Prévision mensuelle | `bucket_date` → fin du mois |
| `timeless` | Sans contrainte temporelle (rare — catalogue, pièces de rechange) | – |

→ Cohérence : ne pas mélanger plusieurs `time_grain` pour le même couple (item × location) sur des plages qui se chevauchent. C'est techniquement possible mais incohérent métier (le DQ Watcher peut alerter).

---

## ⚠️ Limitation V1.0 — pro-rata temporel (monthly → weekly → daily)

**Si tu ingères un forecast `time_grain = month`, Ootils ne le ventile PAS automatiquement** en buckets plus fins (jours / semaines). Le bucket mensuel est stocké tel quel.

### Comportement actuel par étape

| Étape | Ce qui se passe |
|---|---|
| Ingestion `month` | Stocké comme un seul node `ForecastDemand` avec `time_grain = "month"`, `time_span_start = 1er du mois`, `time_span_end = 1er du mois suivant`. Quantité intacte. |
| Lecture en grain plus fin | Possible via `TemporalBridge.disaggregate()` (read-only) — mode `FLAT` uniquement : `Q / N` réparti uniformément, reste sur le dernier bucket. À appeler **explicitement** par l'appelant. |
| Forecast consumer (MRP) | Si on lui passe un `(2026-06-01, 130)` brut, **il met 130 dans LA semaine du 1er juin** (incorrect pour un forecast mensuel). Disaggrégation à faire **avant**. |

### Stratégies de ventilation NON supportées en V1.0

- ❌ Pondération **jours ouvrés** vs weekends (`130 / 22 jours ouvrés` au lieu de `/30 jours`)
- ❌ Pondération **profile saisonnier** (courbe semaine 1 vs semaine 4)
- ❌ Pondération **historique** (basé sur la distribution observée)

Seul `FLAT` est dispo en V1.0.

### Workaround recommandé : pré-ventilation côté client

**Le plus sûr** : tu génères tes forecasts en mensuel comme tu veux (S&OP, Pyramide, ERP, Excel), puis **avant de produire `forecasts.tsv`, tu fais le pro-rata toi-même** :

```
# Au lieu de pousser :
FG-APU-100	DC-LILLE	130	2026-06-01	month	consensus

# Tu produis :
FG-APU-100	DC-LILLE	32.5	2026-06-01	week	consensus
FG-APU-100	DC-LILLE	32.5	2026-06-08	week	consensus
FG-APU-100	DC-LILLE	32.5	2026-06-15	week	consensus
FG-APU-100	DC-LILLE	32.5	2026-06-22	week	consensus
```

Avantage : tu maîtrises **exactement** la ventilation (FLAT, jours ouvrés, saisonnier, peu importe). Pas de surprise dans la chaîne d'appel.

### Hybride : mensuel pour le S&OP + hebdo pour la planif

Si tu veux **aussi** garder le forecast mensuel pour le reporting macro, pousse les **deux** en parallèle (`source` distingue) :

```
FG-APU-100	DC-LILLE	130	2026-06-01	month	consensus       ← pour S&OP / reporting
FG-APU-100	DC-LILLE	32.5	2026-06-01	week	consensus    ← pour MRP
FG-APU-100	DC-LILLE	32.5	2026-06-08	week	consensus
... etc.
```

C'est ce que font les vrais APS quand ils ont un module S&OP au-dessus du MRP.

### Roadmap V1.1 / V2

- Disaggregation automatique au moment de la lecture (sans appel explicite à `TemporalBridge`)
- Modes de ventilation `WORKING_DAYS`, `SEASONAL_PROFILE`, `HISTORICAL` en plus de `FLAT`
- Module Demand V2 (Pyramide) génèrera directement du forecast au grain cible (jour / semaine) → cette limitation devient un non-problème pour les forecasts internes

---

## 5. Les 4 sources

| Source | Sémantique | Qui produit ? |
|---|---|---|
| `statistical` | Issue d'un modèle statistique (MA, ETS, Croston, ARIMA, etc.) | Pyramide quand connectée, ou ERP/APS externe |
| `consensus` | Plan validé S&OP, fruit d'un arbitrage humain | Réunion S&OP, après consensus |
| `manual` | Saisi à la main par un planner | Override individuel |
| `ml` | Issue d'un modèle ML (LightGBM, foundation models, etc.) | Pyramide ML ou modèle externe |

→ Plusieurs sources peuvent coexister pour le même couple item × location × bucket. **L'engine choisit** selon une politique (typiquement : `consensus` > `manual` > `ml` > `statistical`).

---

## 6. Exemples

### 6.1 Forecast hebdomadaire (cas typique)

```
item_external_id	location_external_id	quantity	bucket_date	time_grain	source
FG-APU-100	DC-LILLE	25	2026-06-01	week	consensus
FG-APU-100	DC-LILLE	28	2026-06-08	week	consensus
FG-APU-100	DC-LILLE	30	2026-06-15	week	consensus
FG-APU-100	DC-LILLE	32	2026-06-22	week	consensus
```

→ 4 semaines de prévision pour AquaPump 100 à Lille (25 → 32 unités / semaine, demande croissante).

### 6.2 Forecast multi-sites + multi-articles

```
item_external_id	location_external_id	quantity	bucket_date	time_grain	source
FG-APU-100	DC-LILLE	25	2026-06-01	week	consensus
FG-APU-100	CVL-RETAIL-NORD	80	2026-06-01	week	consensus
FG-APU-200	DC-LILLE	8	2026-06-01	week	consensus
FG-APU-100	DC-LILLE	28	2026-06-08	week	consensus
FG-APU-100	CVL-RETAIL-NORD	85	2026-06-08	week	consensus
FG-APU-200	DC-LILLE	10	2026-06-08	week	consensus
```

### 6.3 Forecast mensuel (horizon long)

```
item_external_id	location_external_id	quantity	bucket_date	time_grain	source
FG-APU-100	DC-LILLE	120	2026-07-01	month	consensus
FG-APU-100	DC-LILLE	135	2026-08-01	month	consensus
FG-APU-100	DC-LILLE	140	2026-09-01	month	consensus
```

### 6.4 Mix de sources (consensus prime, statistical en backup)

```
item_external_id	location_external_id	quantity	bucket_date	time_grain	source
FG-APU-100	DC-LILLE	25	2026-06-01	week	consensus
FG-APU-100	DC-LILLE	22	2026-06-01	week	statistical
```

→ Pour le même bucket, le consensus (25) primera sur la statistique (22) selon la politique de l'engine.

### 6.5 Cas invalides

```
item_external_id	location_external_id	quantity	bucket_date	time_grain	source
ITEM-X	DC-LILLE	25	2026-06-01	week	consensus            ← item inconnu → 422
FG-APU-100	LOC-X	25	2026-06-01	week	consensus          ← location inconnue → 422
FG-APU-100	DC-LILLE	-5	2026-06-01	week	consensus       ← quantity négative → 422
FG-APU-100	DC-LILLE	25	01-06-2026	week	consensus      ← date au mauvais format → 422
FG-APU-100	DC-LILLE	25	2026-06-01	yearly	consensus     ← time_grain inconnu → 422
FG-APU-100	DC-LILLE	25	2026-06-01	week	planner       ← source inconnue → 422
```

---

## 7. Bonnes pratiques bucket_date selon time_grain

| time_grain | bucket_date doit être... |
|---|---|
| `day` ou `exact_date` | n'importe quelle date |
| `week` | un lundi (convention ISO 8601) — facilite l'agrégation |
| `month` | le 1er du mois |

Non obligatoire techniquement en V1 mais fortement recommandé pour la cohérence multi-source.

---

## 8. Comportement à l'ingestion

### 8.1 Identification

Clé business : couple (`item_external_id`, `location_external_id`, `bucket_date`, `time_grain`, `source`).

- **Existe en base** → UPDATE de la quantité
- **N'existe pas** → INSERT (nouveau node ForecastDemand)

### 8.2 Pousser un nouveau forecast écrase l'ancien

Si tu ré-ingères un fichier avec les mêmes clés mais des quantités différentes, **les anciennes valeurs sont remplacées**. C'est voulu : un forecast est une projection qui se met à jour à chaque cycle de prévision.

Pour conserver l'historique des forecasts (pour mesurer l'accuracy), c'est l'engine qui le fait via des snapshots et le `forecast_consumption_log` (migration 025).

### 8.3 Validation FK

`item_external_id` et `location_external_id` doivent exister. Toute FK manquante → batch entier rejeté.

---

## 9. Pipeline

```
data/inbox/forecasts.tsv
        │
        ▼
   parse TSV
        │
        ▼
   validation (2 FK + quantity ≥ 0 + date ISO + time_grain enum + source enum)
        │
        ├─ FK manquante / type invalide ──► 422 → data/rejected/
        │
        ▼ OK
   POST /v1/ingest/forecast-demand
        │
        ▼
   upsert dans `nodes` (ForecastDemand)
        │
        ▼
   data/processed/forecasts_YYYYMMDD_HHMMSS.tsv + .report.json
```

---

## 10. Ordre de chargement

```
1.  items.tsv               ← ✅
2.  locations.tsv           ← ✅
3.  suppliers.tsv           ← ✅
4.  supplier_items.tsv      ← ✅
5.  item_planning_params.tsv ← ✅
6.  on_hand.tsv             ← ✅
7.  purchase_orders.tsv     ← ✅
8.  customer_orders.tsv     ← ✅
9.  forecasts.tsv           ← ICI (2 FK : items + locations)
10. transfers.tsv           ← après
11. bom_*.tsv               ← après
```

---

## 11. Lien avec Pyramide et `demand_history` (futur)

**Aujourd'hui (V1)** : tu pousses des forecasts externes (ERP / Excel / pre-computed ML).

**Demain (V2 Pyramide)** : un orchestrateur lance Pyramide, qui lit l'historique de demande, calcule un forecast, et écrit dans cette même table. Ton fichier `forecasts.tsv` deviendra optionnel — un cas « override manuel » du forecast Pyramide.

Voir `docs/WIP-demand-module-design-session.md` §3 pour le design cible.

---

## 12. Limitations V1.0

| Manque | V1.1 envisagé |
|---|---|
| Pas de `channel`, `region`, `customer_segment`, `order_type` | V1.1 — dimensions hiérarchiques pour désagrégation |
| Pas de `confidence_score` | V1.1 — score de fiabilité du forecast (consommé par Forecast Confidence Gate) |
| Pas de `forecast_version` | V1.1 — pour conserver l'historique des révisions forecast |
| Pas de `published_at` | V1.1 — date de production du forecast |
| Pas de `model_name` (qui statistical → quelle méthode ?) | V1.1 — AUTO_SELECT vs Croston vs EOQ... |

---

## 13. Validation rapide

```bash
python scripts/ingest_file.py data/inbox/forecasts.tsv --dry-run
```
