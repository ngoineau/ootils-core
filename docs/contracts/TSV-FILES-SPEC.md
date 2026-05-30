# Spécifications techniques — fichiers `.tsv` d'ingestion Ootils

> Référence consolidée pour les **11 entités** ingestibles par fichier dans Ootils V1.
> Document destiné aux équipes Data côté client / intégrateur.
> Pour les détails par entité, voir les docs individuelles dans `docs/contracts/<entité>/`.

---

## 0. Table des matières

| # | Entité | Nom de fichier | Endpoint | Section |
|---|---|---|---|---|
| 1 | Articles / SKU | `items.tsv` | `POST /v1/ingest/items` | [§2.1](#21-itemstsv) |
| 2 | Sites / lieux | `locations.tsv` | `POST /v1/ingest/locations` | [§2.2](#22-locationstsv) |
| 3 | Fournisseurs | `suppliers.tsv` | `POST /v1/ingest/suppliers` | [§2.3](#23-supplierstsv) |
| 4 | Conditions appro | `supplier_items.tsv` | `POST /v1/ingest/supplier-items` | [§2.4](#24-supplier_itemstsv) |
| 5 | Paramètres planning | `item_planning_params.tsv` | `POST /v1/ingest/planning-params` | [§2.5](#25-item_planning_paramstsv) |
| 6 | Stock disponible | `on_hand.tsv` | `POST /v1/ingest/on-hand` | [§2.6](#26-on_handtsv) |
| 7 | Commandes achats | `purchase_orders.tsv` | `POST /v1/ingest/purchase-orders` | [§2.7](#27-purchase_orderstsv) |
| 8 | Commandes clients | `customer_orders.tsv` | `POST /v1/ingest/customer-orders` | [§2.8](#28-customer_orderstsv) |
| 9 | Prévisions | `forecasts.tsv` | `POST /v1/ingest/forecast-demand` | [§2.9](#29-forecaststsv) |
| 10 | Transferts inter-sites | `transfers.tsv` | `POST /v1/ingest/transfers` | [§2.10](#210-transferstsv) |
| 11 | BOM (bundle 2 fichiers) | `bom_header.tsv` + `bom_components.tsv` | `POST /v1/ingest/bom` | [§2.11](#211-bom_headertsv--bom_componentstsv) |

---

## 1. Conventions communes à tous les fichiers

### 1.1 Format

| Propriété | Valeur |
|---|---|
| Format | TSV — Tab-Separated Values |
| Encodage | UTF-8 (BOM toléré) |
| Délimiteur | tabulation `\t` |
| Fin de ligne | LF (`\n`) ou CRLF (`\r\n`) acceptées |
| Encadrement | aucun guillemet — la tabulation suffit comme séparateur |
| Header | ligne 1 obligatoire avec noms exacts des colonnes |
| Lignes vides | autorisées (ignorées) |
| Lignes de commentaire | non supportées |
| Taille max par fichier | 10 MB |
| Lignes max par fichier | ~50 000 |

### 1.2 Types de données

| Type | Format attendu | Exemple |
|---|---|---|
| texte | UTF-8, max selon colonne | `FG-APU-100` |
| entier | base 10, sans séparateur de milliers | `42` |
| décimal | point décimal `.`, **pas** virgule | `1.25` ; `45.00` |
| booléen | insensible casse : `true`/`1`/`yes`/`y`/`t` = vrai ; `false`/`0`/`no`/`n`/`f`/vide = faux | `true` |
| date | ISO 8601 `YYYY-MM-DD` | `2026-06-15` |
| enum | valeur exacte de la liste (insensible casse non garantie — respecter la casse documentée) | `confirmed` |

### 1.3 Comportement à l'ingestion

| Aspect | Règle |
|---|---|
| Upsert | Existence en base → UPDATE. Sinon → INSERT. Clé business par entité (voir §2). |
| All-or-nothing | Une seule ligne invalide → **tout le batch rejeté** (HTTP 422), aucune écriture. |
| Cellule vide colonne optionnelle | Valeur par défaut serveur appliquée. |
| Cellule vide colonne obligatoire | Ligne invalide → batch rejeté. |
| Dry-run | Flag `--dry-run` du script ou `dry_run: true` dans le body — valide tout sans écrire. |
| Idempotency | Header HTTP `Idempotency-Key` — même clé + même payload = replay possible. |

### 1.4 Validation FK

Toute colonne marquée `FK <table>` doit référencer un `external_id` qui existe **déjà en base** (ou dans le même fichier pour les hiérarchies internes). Toute FK manquante → batch rejeté.

### 1.5 Ordre de chargement

```
1.  items.tsv                  (master data — racine)
2.  locations.tsv              (master data — racine)
3.  suppliers.tsv              (master data — racine)
4.  supplier_items.tsv         (FK : items, suppliers)
5.  item_planning_params.tsv   (FK : items, locations, suppliers)
6.  on_hand.tsv                (FK : items, locations)
7.  purchase_orders.tsv        (FK : items, locations, suppliers)
8.  customer_orders.tsv        (FK : items, locations)
9.  forecasts.tsv              (FK : items, locations)
10. transfers.tsv              (FK : items, locations × 2)
11. bom_header.tsv + bom_components.tsv  (FK : items × N — bundle 2 fichiers, N appels API)
```

---

## 2. Spécification par entité

---

### 2.1 `items.tsv`

**Clé business** : `external_id`.
**Endpoint** : `POST /v1/ingest/items`.
**Doc détaillée** : `docs/contracts/items/format-items-tsv.md`.

| # | Colonne | Obligatoire | Type | Domaine / contrainte | Description |
|---|---|---|---|---|---|
| 1 | `external_id` | **oui** | texte (≤128) | non-vide, unique dans le fichier | Code SKU ERP. Clé d'upsert. |
| 2 | `name` | **oui** | texte (≤512) | non-vide | Libellé article. |
| 3 | `item_type` | non | enum | `finished_good` \| `component` \| `raw_material` \| `semi_finished` | Défaut : `finished_good`. |
| 4 | `uom` | non | texte (≤16) | code UoM | Défaut : `EA`. |
| 5 | `status` | non | enum | `active` \| `obsolete` \| `phase_out` | Défaut : `active`. |

**Header exact** :
```
external_id	name	item_type	uom	status
```

**Exemple** :
```
external_id	name	item_type	uom	status
FG-APU-100	AquaPump 100	finished_good	EA	active
SUB-HOUSING-100	Housing Assembly 100	semi_finished	EA	active
COMP-MOTOR-24V	24V DC Motor	component	EA	active
RAW-STEEL-50	Acier brut 50kg	raw_material	KG	active
```

---

### 2.2 `locations.tsv`

**Clé business** : `external_id`.
**Endpoint** : `POST /v1/ingest/locations`.
**Doc détaillée** : `docs/contracts/locations/format-locations-tsv.md`.

| # | Colonne | Obligatoire | Type | Domaine / contrainte | Description |
|---|---|---|---|---|---|
| 1 | `external_id` | **oui** | texte (≤128) | non-vide, unique dans le fichier | Code site business. |
| 2 | `name` | **oui** | texte (≤512) | non-vide | Libellé site. |
| 3 | `location_type` | non | enum | `plant` \| `dc` \| `warehouse` \| `supplier_virtual` \| `customer_virtual` | Défaut : `dc`. |
| 4 | `country` | non | texte | ISO 3166-1 alpha-2 (`FR`, `DE`...) | Pays. |
| 5 | `timezone` | non | texte | IANA (`Europe/Paris`) | Fuseau horaire. |
| 6 | `parent_external_id` | non | texte | `external_id` d'une autre location (même fichier ou en base) | Hiérarchie réseau. |

**Header exact** :
```
external_id	name	location_type	country	timezone	parent_external_id
```

**Exemple** :
```
external_id	name	location_type	country	timezone	parent_external_id
PLANT-LYON	Lyon Plant	plant	FR	Europe/Paris	
WH-PARIS-01	Paris Warehouse	warehouse	FR	Europe/Paris	PLANT-LYON
DC-LILLE	Lille DC	dc	FR	Europe/Paris	WH-PARIS-01
SVL-ACME	ACME Supplier	supplier_virtual	IT	Europe/Rome	
CVL-RETAIL-NORD	Retail Nord	customer_virtual	FR	Europe/Paris	
```

**Types de location** :
- `plant` — site qui produit
- `warehouse` — entrepôt interne sans vente directe
- `dc` — Distribution Center, stocke + expédie au client
- `supplier_virtual` — nœud représentant un fournisseur dans le graphe
- `customer_virtual` — nœud représentant un client / canal

---

### 2.3 `suppliers.tsv`

**Clé business** : `external_id`.
**Endpoint** : `POST /v1/ingest/suppliers`.
**Doc détaillée** : `docs/contracts/suppliers/format-suppliers-tsv.md`.

| # | Colonne | Obligatoire | Type | Domaine / contrainte | Description |
|---|---|---|---|---|---|
| 1 | `external_id` | **oui** | texte (≤128) | non-vide, unique dans le fichier | Code fournisseur ERP. |
| 2 | `name` | **oui** | texte (≤512) | non-vide | Raison sociale. |
| 3 | `country` | non | texte | ISO 3166-1 alpha-2 | Pays du fournisseur. |
| 4 | `status` | non | enum | `active` \| `inactive` \| `blocked` | Défaut : `active`. |
| 5 | `lead_time_days` | non | entier | > 0 si fourni | Lead time de référence (niveau 1). |
| 6 | `reliability_score` | non | décimal | dans `[0.0, 1.0]` | Score fiabilité. 1.0 = parfait. |

**Header exact** :
```
external_id	name	country	status	lead_time_days	reliability_score
```

**Exemple** :
```
external_id	name	country	status	lead_time_days	reliability_score
SUP-MOTOR-01	MotorWorks Europe	DE	active	10	0.96
SUP-MECH-01	Precision Mechanics SAS	FR	active	7	0.94
SUP-STEEL-DE	SteelCo Germany	DE	active	21	0.88
SUP-LEGACY-01	OldVendor Co	IT	blocked	45	0.42
```

---

### 2.4 `supplier_items.tsv`

**Clé business** : couple (`supplier_external_id`, `item_external_id`).
**Endpoint** : `POST /v1/ingest/supplier-items`.
**Doc détaillée** : `docs/contracts/supplier_items/format-supplier-items-tsv.md`.

| # | Colonne | Obligatoire | Type | Domaine / contrainte | Description |
|---|---|---|---|---|---|
| 1 | `supplier_external_id` | **oui** | texte | FK `suppliers` | |
| 2 | `item_external_id` | **oui** | texte | FK `items` | |
| 3 | `lead_time_days` | **oui** | entier | > 0 | Lead time par paire (niveau 2 — override le niveau 1). |
| 4 | `currency` | non | texte | code ISO 4217 (`EUR`, `USD`...) | Défaut : `EUR`. |
| 5 | `moq` | non | décimal | > 0 si fourni | Minimum Order Quantity. |
| 6 | `unit_cost` | non | décimal | ≥ 0 si fourni | Prix unitaire. |
| 7 | `is_preferred` | non | booléen | `true` \| `false` | Fournisseur préféré pour cet article. Défaut : `false`. |

**Header exact** :
```
supplier_external_id	item_external_id	lead_time_days	currency	moq	unit_cost	is_preferred
```

**Exemple** :
```
supplier_external_id	item_external_id	lead_time_days	currency	moq	unit_cost	is_preferred
SUP-MOTOR-01	COMP-MOTOR-24V	10	EUR	20	45.00	true
SUP-MECH-01	COMP-IMPELLER-100	7	EUR	30	8.50	true
SUP-BOLT-FR	COMP-IMPELLER-100	5	EUR	100	7.20	false
SUP-STEEL-DE	RAW-STEEL-50	21	EUR	1000	1.20	true
```

(Multi-sourcing : 2 lignes pour `COMP-IMPELLER-100` — MECH préféré, BOLT en backup.)

---

### 2.5 `item_planning_params.tsv`

**Clé business** : couple (`item_external_id`, `location_external_id`).
**Endpoint** : `POST /v1/ingest/planning-params`.
**Comportement** : **SCD2 transparent** — push partiel autorisé, cellule vide = « ne touche pas ».
**Doc détaillée** : `docs/contracts/item_planning_params/format-item-planning-params-tsv.md`.

#### 2.5.1 Champs requis (2)

| # | Colonne | Type | Description |
|---|---|---|---|
| 1 | `item_external_id` | texte (FK `items`) | |
| 2 | `location_external_id` | texte (FK `locations`) | |

#### 2.5.2 Lead times (3)

| # | Colonne | Type | Contrainte | Description |
|---|---|---|---|---|
| 3 | `lead_time_sourcing_days` | entier | ≥ 0 | Délai commande → expédition fournisseur. |
| 4 | `lead_time_manufacturing_days` | entier | ≥ 0 | Délai de transformation interne. |
| 5 | `lead_time_transit_days` | entier | ≥ 0 | Délai de transport (limitation : simple nombre, sans mode/carrier — voir LANES-LATER). |

#### 2.5.3 Safety stock (2)

| # | Colonne | Type | Contrainte | Description |
|---|---|---|---|---|
| 6 | `safety_stock_qty` | décimal | ≥ 0 | SS en unités. |
| 7 | `safety_stock_days` | décimal | ≥ 0 | SS en jours de couverture. |

#### 2.5.4 Reorder / lot sizing (4)

| # | Colonne | Type | Contrainte | Description |
|---|---|---|---|---|
| 8 | `reorder_point_qty` | décimal | ≥ 0 | Point de commande. |
| 9 | `min_order_qty` | décimal | > 0 | MOQ. |
| 10 | `max_order_qty` | décimal | > 0 | Quantité maximum par commande. |
| 11 | `order_multiple` | décimal | > 0 | Multiple de commande indivisible. |

#### 2.5.5 Politique planning (4)

| # | Colonne | Type | Domaine | Description |
|---|---|---|---|---|
| 12 | `lot_size_rule` | enum | `LOTFORLOT` \| `FIXED_QTY` \| `EOQ` \| `POQ` \| `MIN_MAX` \| `MULTIPLE` | Défaut : `LOTFORLOT`. |
| 13 | `planning_horizon_days` | entier | > 0 | Défaut : 90. |
| 14 | `is_make` | booléen | `true` (fabriqué) \| `false` (acheté) | Défaut : `false`. |
| 15 | `preferred_supplier_external_id` | texte | FK `suppliers` | Override `supplier_items.is_preferred`. |

#### 2.5.6 Extensions APICS V1.1 (7, optionnelles)

| # | Colonne | Type | Contrainte |
|---|---|---|---|
| 16 | `economic_order_qty` | décimal | > 0 |
| 17 | `lot_size_poq_periods` | entier | > 0 |
| 18 | `order_multiple_qty` | décimal | > 0 |
| 19 | `frozen_time_fence_days` | entier | ≥ 0 |
| 20 | `slashed_time_fence_days` | entier | > 0 |
| 21 | `forecast_consumption_strategy` | enum | `max_only` \| `consume_forward` \| `consume_backward` \| `consume_both` |
| 22 | `consumption_window_days` | entier | > 0 |

**Header complet** :
```
item_external_id	location_external_id	lead_time_sourcing_days	lead_time_manufacturing_days	lead_time_transit_days	safety_stock_qty	safety_stock_days	reorder_point_qty	min_order_qty	max_order_qty	order_multiple	lot_size_rule	planning_horizon_days	is_make	preferred_supplier_external_id
```

(Colonnes 16-22 ajoutables si besoin.)

**Exemple** (colonnes V1, sans extensions APICS) :
```
item_external_id	location_external_id	lead_time_sourcing_days	lead_time_manufacturing_days	lead_time_transit_days	safety_stock_qty	reorder_point_qty	min_order_qty	order_multiple	lot_size_rule	planning_horizon_days	is_make	preferred_supplier_external_id
COMP-MOTOR-24V	PLANT-LYON	7	0	3	50	100	20	1	LOTFORLOT	90	false	SUP-MOTOR-01
SUB-HOUSING-100	PLANT-LYON	0	5	0	20	40	1	1	LOTFORLOT	90	true	
RAW-STEEL-50	PLANT-LYON	21			500		1000	100	EOQ	120	false	SUP-STEEL-DE
```

(Push partiel illustré : `RAW-STEEL-50` laisse `lead_time_manufacturing_days`, `lead_time_transit_days`, `reorder_point_qty` vides → ces champs ne sont **pas touchés** en base. Demande le SS uniquement, lot rule EOQ, sourcing chez SteelCo.)

**⚠️ Particularité SCD2** : ce fichier autorise le **push partiel**. Si tu ne mets que `safety_stock_qty`, les autres valeurs en base sont préservées. Re-push du même état = NOOP. Changement → l'API ferme l'ancienne ligne (effective_to = hier) et insère la nouvelle (effective_from = today).

---

### 2.6 `on_hand.tsv`

**Clé business** : couple (`item_external_id`, `location_external_id`).
**Endpoint** : `POST /v1/ingest/on-hand`.
**Nature** : **SNAPSHOT** à un instant T (pas DELTA — chaque push remplace).
**Doc détaillée** : `docs/contracts/on_hand/format-on-hand-tsv.md`.

| # | Colonne | Obligatoire | Type | Domaine / contrainte | Description |
|---|---|---|---|---|---|
| 1 | `item_external_id` | **oui** | texte | FK `items` | |
| 2 | `location_external_id` | **oui** | texte | FK `locations` | |
| 3 | `quantity` | **oui** | décimal | ≥ 0 (zéro valide = stock à zéro explicite) | Quantité disponible. |
| 4 | `uom` | non | texte | code UoM | Défaut : `EA`. |
| 5 | `as_of_date` | **oui** | date | ISO `YYYY-MM-DD` | Date de la photo. |
| 6 | `lot_number` | non (**ignoré V1.0**) | texte | – | Réservé V1.1. Présence tolérée, contenu non consommé. |

**Header exact** :
```
item_external_id	location_external_id	quantity	uom	as_of_date
```
(`lot_number` accepté mais ignoré.)

**Exemple** :
```
item_external_id	location_external_id	quantity	uom	as_of_date
FG-APU-100	DC-LILLE	15	EA	2026-05-26
SUB-HOUSING-100	PLANT-LYON	20	EA	2026-05-26
COMP-MOTOR-24V	PLANT-LYON	60	EA	2026-05-26
COMP-IMPELLER-100	DC-LILLE	0	EA	2026-05-26
RAW-STEEL-50	PLANT-LYON	1200	KG	2026-05-26
```

(Ligne `0` explicite pour `COMP-IMPELLER-100 @ DC-LILLE` = trigger shortage.)

---

### 2.7 `purchase_orders.tsv`

**Clé business** : `external_id` (numéro PO ERP).
**Endpoint** : `POST /v1/ingest/purchase-orders`.
**Doc détaillée** : `docs/contracts/purchase_orders/format-purchase-orders-tsv.md`.

| # | Colonne | Obligatoire | Type | Domaine / contrainte | Description |
|---|---|---|---|---|---|
| 1 | `external_id` | **oui** | texte | non-vide, unique | Numéro de PO ERP. |
| 2 | `item_external_id` | **oui** | texte | FK `items` | Article commandé. |
| 3 | `location_external_id` | **oui** | texte | FK `locations` | Site **récepteur**. |
| 4 | `supplier_external_id` | **oui** | texte | FK `suppliers` | Fournisseur livreur. |
| 5 | `quantity` | **oui** | décimal | > 0 | Quantité commandée. |
| 6 | `uom` | non | texte | code UoM | Défaut : `EA`. |
| 7 | `expected_delivery_date` | **oui** | date | ISO | Date de réception prévue. |
| 8 | `status` | non | enum | `draft` \| `confirmed` \| `in_transit` \| `received` \| `cancelled` | Défaut : `confirmed`. |

**Header exact** :
```
external_id	item_external_id	location_external_id	supplier_external_id	quantity	uom	expected_delivery_date	status
```

**Exemple** :
```
external_id	item_external_id	location_external_id	supplier_external_id	quantity	uom	expected_delivery_date	status
PO-2026-001	COMP-MOTOR-24V	PLANT-LYON	SUP-MOTOR-01	50	EA	2026-06-05	confirmed
PO-2026-003	RAW-STEEL-50	PLANT-LYON	SUP-STEEL-DE	2000	KG	2026-06-16	confirmed
PO-2026-004	COMP-MOTOR-24V	PLANT-LYON	SUP-MOTOR-01	30	EA	2026-05-29	in_transit
PO-2026-005	COMP-IMPELLER-100	PLANT-LYON	SUP-BOLT-FR	100	EA	2026-05-30	draft
```

**Impact projection par statut** :
| Statut | Compte dans la projection |
|---|---|
| `draft` | non |
| `confirmed` | **oui** (entrée prévue) |
| `in_transit` | **oui** (entrée prévue) |
| `received` | non (déjà dans `on_hand`) |
| `cancelled` | non |

---

### 2.8 `customer_orders.tsv`

**Clé business** : `external_id` (numéro commande client).
**Endpoint** : `POST /v1/ingest/customer-orders`.
**Doc détaillée** : `docs/contracts/customer_orders/format-customer-orders-tsv.md`.

> ⚠️ **Limitation V1.0 importante** : pas d'identifiant client individuel
> (`customer_external_id`, `channel`, `region`, `segment` absents). Le « client »
> est modélisé via une `customer_virtual` location utilisée comme `location_external_id`.
> **Impact** : le Service Risk Watcher et le Customer Agent du wedge V1 ne
> peuvent pas prioriser finement par key account ; Pyramide ne peut pas
> désagréger par dimension client. **Doit être levée en V1.1 (prérequis du
> module Demand V2)** — voir `docs/WIP-demand-module-design-session.md` §3.1.

| # | Colonne | Obligatoire | Type | Domaine / contrainte | Description |
|---|---|---|---|---|---|
| 1 | `external_id` | **oui** | texte | non-vide, unique | Numéro commande client ERP. |
| 2 | `item_external_id` | **oui** | texte | FK `items` | Article commandé. |
| 3 | `location_external_id` | **oui** | texte | FK `locations` | Site **expéditeur** (DC, WH, ou `customer_virtual`). |
| 4 | `quantity` | **oui** | décimal | > 0 | Quantité commandée. |
| 5 | `requested_delivery_date` | **oui** | date | ISO | Date demandée par le client. |
| 6 | `status` | non | enum | `open` \| `confirmed` \| `shipped` \| `delivered` \| `cancelled` | Défaut : `open`. |

**Header exact** :
```
external_id	item_external_id	location_external_id	quantity	requested_delivery_date	status
```

**Exemple** :
```
external_id	item_external_id	location_external_id	quantity	requested_delivery_date	status
CO-2026-001	FG-APU-100	DC-LILLE	30	2026-06-03	confirmed
CO-2026-002	FG-APU-100	DC-LILLE	15	2026-06-10	open
CO-2026-004	FG-APU-100	CVL-RETAIL-NORD	100	2026-06-08	confirmed
CO-2026-006	FG-APU-100	DC-LILLE	8	2026-05-25	shipped
```

(Note : `CVL-RETAIL-NORD` est une `customer_virtual` location utilisée comme proxy client — voir warning §2.8 ci-dessus.)

**Impact projection par statut** :
| Statut | Compte dans la demande future |
|---|---|
| `open` | **oui** (demande molle) |
| `confirmed` | **oui** (demande ferme) |
| `shipped` | non (sortie déjà effectuée) |
| `delivered` | non |
| `cancelled` | non |

---

### 2.9 `forecasts.tsv`

**Clé business** : couple (`item_external_id`, `location_external_id`, `bucket_date`, `time_grain`, `source`).
**Endpoint** : `POST /v1/ingest/forecast-demand`.
**Doc détaillée** : `docs/contracts/forecasts/format-forecasts-tsv.md`.

> ⚠️ **Limitation V1.0 — pro-rata `month` → `week`/`day` non automatique** :
> un forecast `time_grain = month` n'est PAS ventilé en sous-buckets au stockage.
> La disaggregation existe (`TemporalBridge.disaggregate`, mode `FLAT` seul)
> mais doit être appelée explicitement par le client. **Reco V1.0** : générer le
> pro-rata côté client et ne pousser que `time_grain = week` (ou `day`) dans
> Ootils. Voir doc détaillée §4bis pour le détail.

| # | Colonne | Obligatoire | Type | Domaine / contrainte | Description |
|---|---|---|---|---|---|
| 1 | `item_external_id` | **oui** | texte | FK `items` | |
| 2 | `location_external_id` | **oui** | texte | FK `locations` | |
| 3 | `quantity` | **oui** | décimal | ≥ 0 (zéro accepté = forecast nul explicite) | Quantité prévue sur le bucket. |
| 4 | `bucket_date` | **oui** | date | ISO | Date de **début** du bucket. |
| 5 | `time_grain` | non | enum | `day` \| `week` \| `month` \| `exact_date` \| `timeless` | Défaut : `week`. |
| 6 | `source` | non | enum | `statistical` \| `consensus` \| `manual` \| `ml` | Défaut : `statistical`. |

**Header exact** :
```
item_external_id	location_external_id	quantity	bucket_date	time_grain	source
```

**Exemple** :
```
item_external_id	location_external_id	quantity	bucket_date	time_grain	source
FG-APU-100	DC-LILLE	25	2026-06-01	week	consensus
FG-APU-100	DC-LILLE	28	2026-06-08	week	consensus
FG-APU-100	DC-LILLE	30	2026-06-15	week	consensus
FG-APU-100	CVL-RETAIL-NORD	80	2026-06-01	week	consensus
FG-APU-200	DC-LILLE	8	2026-06-01	week	consensus
```

(Reco : pré-ventiler côté client et pousser uniquement `week` ou `day` — voir warning §2.9 ci-dessus.)

**Conventions `bucket_date` selon `time_grain`** :
- `week` → lundi (ISO 8601)
- `month` → 1er du mois
- `day` / `exact_date` → date quelconque

---

### 2.10 `transfers.tsv`

**Clé business** : `external_id` (numéro STO).
**Endpoint** : `POST /v1/ingest/transfers`.
**Doc détaillée** : `docs/contracts/transfers/format-transfers-tsv.md`.

| # | Colonne | Obligatoire | Type | Domaine / contrainte | Description |
|---|---|---|---|---|---|
| 1 | `external_id` | **oui** | texte | non-vide, unique | Numéro de Stock Transfer Order. |
| 2 | `item_external_id` | **oui** | texte | FK `items` | Article transféré. |
| 3 | `from_location_external_id` | **oui** | texte | FK `locations`, ≠ `to_location_external_id` | Site origine. |
| 4 | `to_location_external_id` | **oui** | texte | FK `locations`, ≠ `from_location_external_id` | Site destination. |
| 5 | `quantity` | **oui** | décimal | > 0 | Quantité transférée. |
| 6 | `expected_delivery_date` | **oui** | date | ISO | Date d'arrivée prévue à destination. |
| 7 | `status` | non | enum | `planned` \| `in_transit` \| `delivered` \| `cancelled` | Défaut : `planned`. |

**Header exact** :
```
external_id	item_external_id	from_location_external_id	to_location_external_id	quantity	expected_delivery_date	status
```

**Exemple** :
```
external_id	item_external_id	from_location_external_id	to_location_external_id	quantity	expected_delivery_date	status
TR-2026-001	FG-APU-100	PLANT-LYON	WH-PARIS-01	50	2026-06-02	in_transit
TR-2026-002	FG-APU-100	WH-PARIS-01	DC-LILLE	20	2026-06-04	planned
TR-2026-004	FG-APU-200	PLANT-LYON	DC-LILLE	10	2026-06-06	planned
TR-2026-005	SUB-HOUSING-100	PLANT-LYON	WH-PARIS-01	5	2026-05-26	delivered
```

(Flux logique : Lyon → Paris → Lille.)

---

### 2.11 `bom_header.tsv` + `bom_components.tsv`

**Bundle particulier** : 2 fichiers fusionnés en N appels API (un par BOM).
**Endpoint** : `POST /v1/ingest/bom` (appelé 1× par BOM).
**Clé business** : couple (`parent_external_id`, `bom_version`).
**Doc détaillée** : `docs/contracts/bom/format-bom-tsv.md`.

#### 2.11.1 `bom_header.tsv`

| # | Colonne | Obligatoire | Type | Domaine / contrainte | Description |
|---|---|---|---|---|---|
| 1 | `parent_external_id` | **oui** | texte | FK `items` | Article parent (fabriqué). |
| 2 | `bom_version` | non | texte | – | Version BOM. Défaut : `1.0`. |
| 3 | `effective_from` | non | date | ISO | Date d'effet. Défaut : aujourd'hui. |

**Header exact** :
```
parent_external_id	bom_version	effective_from
```

**Exemple** :
```
parent_external_id	bom_version	effective_from
FG-APU-100	1.0	2026-04-01
FG-APU-200	1.0	2026-04-01
SUB-HOUSING-100	1.0	2026-04-01
```

#### 2.11.2 `bom_components.tsv`

| # | Colonne | Obligatoire | Type | Domaine / contrainte | Description |
|---|---|---|---|---|---|
| 1 | `parent_external_id` | **oui** | texte | doit exister dans `bom_header.tsv` | Référence header. |
| 2 | `bom_version` | **oui** | texte | doit exister dans `bom_header.tsv` | Référence header. |
| 3 | `component_external_id` | **oui** | texte | FK `items` | Composant. |
| 4 | `quantity_per` | **oui** | décimal | > 0 | Quantité de composant par unité de parent. |
| 5 | `uom` | non | texte | code UoM | Défaut : `EA`. |
| 6 | `scrap_factor` | non | décimal | `[0.0, 1.0[` | Taux de rebut. Défaut : `0.0`. |

**Header exact** :
```
parent_external_id	bom_version	component_external_id	quantity_per	uom	scrap_factor
```

**Exemple** (couplé avec le header ci-dessus) :
```
parent_external_id	bom_version	component_external_id	quantity_per	uom	scrap_factor
FG-APU-100	1.0	SUB-HOUSING-100	1	EA	0.00
FG-APU-100	1.0	COMP-MOTOR-24V	1	EA	0.00
FG-APU-100	1.0	COMP-IMPELLER-100	1	EA	0.02
FG-APU-200	1.0	SUB-HOUSING-100	1	EA	0.00
FG-APU-200	1.0	COMP-MOTOR-24V	2	EA	0.00
FG-APU-200	1.0	COMP-IMPELLER-100	1	EA	0.02
SUB-HOUSING-100	1.0	RAW-STEEL-50	2	KG	0.05
```

Lecture :
- `FG-APU-100` = 1 housing + 1 motor + 1 impeller (2 % rebut sur impeller)
- `FG-APU-200` = idem mais **2 motors** (version High Power)
- `SUB-HOUSING-100` est lui-même fabriqué à partir de 2 kg d'acier (5 % rebut)
- 3 BOMs → **3 appels API** émis par le script

#### 2.11.3 Règles particulières

- Le script s'invoque **uniquement** avec `bom_header.tsv` — il charge automatiquement `bom_components.tsv` à côté.
- Lancer `bom_components.tsv` seul est **refusé** (pas de métadonnées).
- Chaque (parent × version) de `bom_components.tsv` **doit** exister dans `bom_header.tsv` (sinon batch rejeté avant tout appel API).
- Un header sans composants → batch rejeté (un BOM vide n'a pas de sens).
- Plusieurs BOMs dans le même bundle → **N appels API séparés**. Si l'un échoue, les autres déjà envoyés restent écrits (pas d'atomicité globale en V1.0 — sujet à arbitrer V1.1).

---

## 3. Codes d'erreur et diagnostic

### 3.1 Codes HTTP

| Code | Sémantique |
|---|---|
| `200 OK` | Batch accepté. Détails dans `summary` et `results`. |
| `400 Bad Request` | Idempotency-Key malformée. |
| `401 Unauthorized` | Token Bearer invalide. |
| `409 Conflict` | Idempotency-Key réutilisée avec payload différent. |
| `413 Payload Too Large` | Body > 10 MB. |
| `422 Unprocessable Entity` | Validation échouée (FK, type, enum, contraintes business). **Rien n'est persisté.** |
| `429 Too Many Requests` | (V1.1) rate limit. |
| `500 Internal Server Error` | Bug serveur — consulter les logs. |

### 3.2 Structure de réponse standard

```json
{
  "status": "ok|dry_run|partial",
  "summary": { "total": 1234, "inserted": 1200, "updated": 30, "errors": 4 },
  "results": [
    { "external_id": "SKU-001", "item_id": "uuid", "action": "inserted" }
  ],
  "batch_id": "uuid",
  "dq_status": "passed|warning|failed"
}
```

### 3.3 Structure d'erreur 422

```json
{
  "detail": [
    { "external_id": "SKU-001", "row": 0, "errors": ["..."] },
    { "external_id": "SKU-002", "row": 3, "errors": ["..."] }
  ]
}
```

---

## 4. Workflow type — dépôt fichier

```
1. Tu déposes le fichier dans data/inbox/<entité>.tsv
2. Tu lances :   python scripts/ingest_file.py data/inbox/<entité>.tsv [--dry-run]
3. Le script :
     - parse le TSV
     - construit le payload JSON
     - appelle l'API en process (FastAPI TestClient — pas besoin de serveur HTTP)
     - si OK    → data/processed/<entité>_YYYYMMDD_HHMMSS.tsv  + .report.json
       si erreur → data/rejected/<entité>_YYYYMMDD_HHMMSS.tsv   + .report.json
```

Pré-requis env vars :
- `DATABASE_URL` : DSN PostgreSQL
- `OOTILS_API_TOKEN` : token Bearer (obligatoire au boot)

---

## 5. Limitations V1.0 — synthèse

| Sujet | Limitation V1.0 | À venir |
|---|---|---|
| **Identifiant client absent** | **Pas de table `customers`, pas de `customer_external_id` sur CO. Customer = `customer_virtual` location (workaround).** | **V1.1 — prérequis du module Demand V2 (task `CUST-V1.1`)** |
| **Pro-rata forecast monthly → weekly/daily** | **Pas automatique au stockage. Seul mode `FLAT` à la lecture (TemporalBridge). Forecast consumer NE bucketise PAS un `month` proprement.** | **V1.1 (task `PRORATA-V1.1`) + résolu via Pyramide V2** |
| Transit multi-mode | `lead_time_transit_days` = simple nombre | `distribution_links` + `transportation_lanes` (task LANES-LATER) |
| Lot tracking | Colonne `lot_number` dans on_hand ignorée | V1.1 |
| Customer dimensions | Pas de `channel`/`region`/`segment` sur CO et forecasts | V1.1 (cf. ligne ci-dessus) |
| OTIF tracking | Pas de `actual_delivery_date` sur PO/CO | V1.1 |
| Coûts / devise | Pas de `unit_cost`/`currency`/`total_value` sur PO/CO/transfers | V1.1 |
| Forecast confidence | Pas de `confidence_score` sur forecasts | V1.1 |
| Forecast version | Pas d'historique des révisions | V1.1 |
| BOM par site | BOM globale, pas par site de production | V1.1 |
| BOM substituts | Pas de `substitute_group` | V1.1 |
| Headers contrat | Pas de `X-Contract-Version` ni `X-Source-System` | V1.1 |
| Multi-source mapping | Pas de table `external_code_mapping` | V1.1 |

---

## 6. Templates vides à copier

Disponibles dans `data/templates/` :

```
items.template.tsv
locations.template.tsv
suppliers.template.tsv
supplier_items.template.tsv
item_planning_params.template.tsv
on_hand.template.tsv
purchase_orders.template.tsv
customer_orders.template.tsv
forecasts.template.tsv
transfers.template.tsv
bom_header.template.tsv
bom_components.template.tsv
```

Chaque template contient **uniquement la ligne header** — copier puis remplir.

---

## 7. Cas couramment rencontrés

| Symptôme | Cause probable | Solution |
|---|---|---|
| `422 — item_external_id 'X' not found in DB` | `items.tsv` pas chargé avant l'entité qui le référence | Respecter §1.5 ordre de chargement |
| `422 — lead_time_days must be > 0` | Cellule vide ou 0 sur colonne contrainte `> 0` | Mettre une valeur valide |
| `422 — status 'pending' invalid` | Valeur hors enum | Voir colonne `Domaine` du tableau |
| `parse error: line 3: column count 4 != header count 5` | Tabulation manquante ou en trop | Vérifier le délimiteur (TSV, pas CSV) |
| Le script déplace le fichier mais base vide | Mode `--dry-run` actif | Relancer sans `--dry-run` |
| `bom_components.tsv cannot be ingested alone` | Tentative de lancer le components seul | Lancer `bom_header.tsv` à la place |
| `from_location and to_location must differ` | Transfer de A vers A | Corriger la ligne |
