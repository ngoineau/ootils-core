# Format de fichier — `purchase_orders.tsv`

> Fichier des **commandes d'achat en cours** (open POs) — articles commandés à un fournisseur, qui arrivent à un site donné à une date prévue.
> Entité transactionnelle qui alimente le calcul des projections de stock (`ProjectedInventory`).
> Endpoint cible : `POST /v1/ingest/purchase-orders`.

---

## 1. Caractéristiques du fichier

| Propriété | Valeur |
|---|---|
| **Nom** | `purchase_orders.tsv` (exact) |
| **Format** | TSV |
| **Encodage** | UTF-8 (BOM toléré) |
| **Délimiteur** | tabulation (`\t`) |
| **Header** | ligne 1 obligatoire |
| **Endpoint** | `POST /v1/ingest/purchase-orders` |

---

## 2. Nature de la donnée

Une ligne = **une ligne de commande d'achat** :
> « On a commandé `quantity` unités de `item_external_id` chez `supplier_external_id`, livrable à `location_external_id` pour le `expected_delivery_date`. Statut actuel : `status`. »

C'est ce qui alimente la projection forward : les POs `confirmed` ou `in_transit` créent des **entrées de stock prévues** à leur date d'arrivée.

Note V1 :
- Pas de notion de ligne PO multiple (1 fichier ligne = 1 PO mono-ligne)
- Pour modéliser un PO multi-ligne, créer un `external_id` distinct par ligne (ex : `PO-2026-001-L1`, `PO-2026-001-L2`)

---

## 3. Colonnes

**Ordre figé par le contrat canonique** : `external_id`, `item_external_id`, `location_external_id`, `supplier_external_id`, `quantity`, `uom`, `expected_delivery_date`, `status`.

| # | Colonne | Obligatoire | Type | Domaine | Description |
|---|---|---|---|---|---|
| 1 | `external_id` | **oui** | texte | non-vide, unique dans le fichier | Numéro de PO ERP. Clé d'upsert. |
| 2 | `item_external_id` | **oui** | texte | FK `items` | Article commandé. |
| 3 | `location_external_id` | **oui** | texte | FK `locations` | Site qui **reçoit** la marchandise (plant, WH, DC). |
| 4 | `supplier_external_id` | **oui** | texte | FK `suppliers` | Fournisseur qui livre. |
| 5 | `quantity` | **oui** | décimal | > 0 | Quantité commandée. |
| 6 | `uom` | non | texte | code UoM | Défaut : `EA`. |
| 7 | `expected_delivery_date` | **oui** | date | ISO `YYYY-MM-DD` | Date de réception prévue. |
| 8 | `status` | non | enum | voir §4 | Défaut : `confirmed`. |

---

## 4. Les 5 statuts de PO

| Statut | Sémantique | Impact projection |
|---|---|---|
| `draft` | Brouillon, pas encore validé | **Ignoré** par la projection (rien de garanti) |
| `confirmed` | PO validée par les deux parties | Inclus dans la projection à `expected_delivery_date` |
| `in_transit` | Marchandise en route, expédiée par le fournisseur | Inclus, idem |
| `received` | Marchandise reçue (intégrée au on_hand) | **N'incrémente plus la projection** — déjà comptée dans `on_hand` |
| `cancelled` | Commande annulée | **Ignoré** |

Conséquence : on pousse **toutes** les POs même les `draft` / `received` / `cancelled`, c'est l'engine qui décide quoi prendre en compte selon le statut.

---

## 5. Cohérence avec les lead times

Le `expected_delivery_date` devrait être **cohérent** avec :
- la date de création du PO (non présente dans ce fichier V1)
- le `supplier_items.lead_time_days` ou `item_planning_params.lead_time_*`

Le **Lead Time Drift Watcher** (P-W01) compare ces dates au lead time paramétré pour détecter les dérives :
- PO créée le 2026-05-01 avec `expected_delivery_date = 2026-05-26` → lead time observé 25j
- `supplier_items.lead_time_days = 10` → drift de +15j → alerte

→ V1 : le drift est calculé sur l'historique des PO. La date de création n'est pas dans `purchase_orders.tsv` (limitation à corriger en V1.1).

---

## 6. Exemples

### 6.1 Exemple typique (5 POs ouvertes)

```
external_id	item_external_id	location_external_id	supplier_external_id	quantity	uom	expected_delivery_date	status
PO-2026-001	COMP-MOTOR-24V	PLANT-LYON	SUP-MOTOR-01	50	EA	2026-06-05	confirmed
PO-2026-002	COMP-IMPELLER-100	PLANT-LYON	SUP-MECH-01	80	EA	2026-06-02	confirmed
PO-2026-003	RAW-STEEL-50	PLANT-LYON	SUP-STEEL-DE	2000	KG	2026-06-16	confirmed
PO-2026-004	COMP-MOTOR-24V	PLANT-LYON	SUP-MOTOR-01	30	EA	2026-05-29	in_transit
PO-2026-005	COMP-IMPELLER-100	PLANT-LYON	SUP-BOLT-FR	100	EA	2026-05-30	draft
```

Lecture :
- PO-001 : 50 motors arrivent à Lyon le 05/06
- PO-002 : 80 impellers arrivent à Lyon le 02/06
- PO-003 : 2000 kg d'acier arrivent à Lyon le 16/06 (lead time 21j attendu pour SUP-STEEL-DE)
- PO-004 : 30 motors **en transit** — devrait arriver le 29/05
- PO-005 : encore `draft` — pas comptée dans la projection tant que pas confirmée

### 6.2 PO multi-ligne (1 commande, 3 articles)

```
external_id	item_external_id	location_external_id	supplier_external_id	quantity	uom	expected_delivery_date	status
PO-2026-100-L1	COMP-MOTOR-24V	PLANT-LYON	SUP-MOTOR-01	20	EA	2026-06-10	confirmed
PO-2026-100-L2	COMP-IMPELLER-100	PLANT-LYON	SUP-MECH-01	40	EA	2026-06-10	confirmed
PO-2026-100-L3	SUB-HOUSING-100	PLANT-LYON	SUP-MECH-01	15	EA	2026-06-10	confirmed
```

→ Convention : suffixer le numéro PO ERP avec `-L1`, `-L2`, `-L3` pour chaque ligne. Aligne avec la pratique SAP / Oracle / Sage.

### 6.3 PO reçue (intégrée au on_hand mais conservée pour traçabilité)

```
external_id	item_external_id	location_external_id	supplier_external_id	quantity	uom	expected_delivery_date	status
PO-2026-099	COMP-MOTOR-24V	PLANT-LYON	SUP-MOTOR-01	50	EA	2026-05-20	received
```

→ Cette PO **n'incrémente plus** la projection (déjà dans `on_hand`) mais reste consultable pour audit / OTIF supplier.

### 6.4 Cas invalides

```
external_id	item_external_id	location_external_id	supplier_external_id	quantity	uom	expected_delivery_date	status
	COMP-MOTOR-24V	PLANT-LYON	SUP-MOTOR-01	50	EA	2026-06-05	confirmed                ← external_id vide → 422
PO-X	ITEM-UNKNOWN	PLANT-LYON	SUP-MOTOR-01	50	EA	2026-06-05	confirmed                ← item inconnu → 422
PO-X	COMP-MOTOR-24V	LOC-XYZ	SUP-MOTOR-01	50	EA	2026-06-05	confirmed                ← location inconnue → 422
PO-X	COMP-MOTOR-24V	PLANT-LYON	SUP-UNKNOWN	50	EA	2026-06-05	confirmed                ← supplier inconnu → 422
PO-X	COMP-MOTOR-24V	PLANT-LYON	SUP-MOTOR-01	0	EA	2026-06-05	confirmed                ← quantity = 0 (doit être > 0) → 422
PO-X	COMP-MOTOR-24V	PLANT-LYON	SUP-MOTOR-01	50	EA	05-06-2026	confirmed              ← date au mauvais format → 422
PO-X	COMP-MOTOR-24V	PLANT-LYON	SUP-MOTOR-01	50	EA	2026-06-05	pending                 ← statut 'pending' inconnu → 422
```

---

## 7. Comportement à l'ingestion

### 7.1 Identification

Clé business : `external_id` (le numéro de PO).
- **Existe en base** → UPDATE des 7 autres champs (utile pour les changements de date, de quantité, ou de statut)
- **N'existe pas** → INSERT (nouveau node PurchaseOrder)

### 7.2 Cycle typique d'une PO

```
Jour J        : INSERT avec status='draft'
Jour J+1      : UPDATE avec status='confirmed'
Jour J+lead-1 : UPDATE avec status='in_transit'
Jour J+lead   : UPDATE avec status='received' (parallèlement, on_hand est mis à jour)
```

Chaque mise à jour utilise le même `external_id` → l'engine voit l'évolution.

### 7.3 Validation FK

Toutes les 3 FK (item, location, supplier) sont résolues en batch **avant** toute écriture. Si une seule FK est introuvable → tout le batch rejeté (422).

---

## 8. Pipeline

```
data/inbox/purchase_orders.tsv
        │
        ▼
   parse TSV
        │
        ▼
   validation (3 FK + types + status enum + date ISO + quantity > 0)
        │
        ├─ FK manquante / type invalide ──► 422 → data/rejected/
        │
        ▼ OK
   POST /v1/ingest/purchase-orders
        │
        ▼
   upsert dans `nodes` (PurchaseOrder)
        │
        ▼
   data/processed/purchase_orders_YYYYMMDD_HHMMSS.tsv + .report.json
```

---

## 9. Ordre de chargement

```
1.  items.tsv               ← ✅
2.  locations.tsv           ← ✅
3.  suppliers.tsv           ← ✅
4.  supplier_items.tsv      ← ✅
5.  item_planning_params.tsv ← ✅
6.  on_hand.tsv             ← ✅
7.  purchase_orders.tsv     ← ICI (3 FK : items + locations + suppliers)
8.  customer_orders.tsv     ← après
9.  forecasts.tsv           ← après
10. transfers.tsv           ← après
11. bom_*.tsv               ← après
```

---

## 10. Limitations connues V1.0

| Manque | V1.1 envisagé |
|---|---|
| Pas de `order_date` (date de création PO) | À ajouter — utile pour Lead Time Drift Watcher |
| Pas de `confirmation_date`, `supplier_ack_date` | À ajouter pour suivre OTIF supplier |
| Pas de `unit_cost`, `currency`, `total_value` | À ajouter pour Finance Agent (G02) |
| Pas de `tracking_number`, `carrier` | À ajouter quand transportation_lanes sera consommé |
| Pas de `incoterm` (FOB, CIF, DDP...) | Useful for cross-border, V2 |
| Pas de `received_quantity` (vs ordered_quantity) | Pour gérer les livraisons partielles, V1.1 |

---

## 11. Validation rapide

```bash
python scripts/ingest_file.py data/inbox/purchase_orders.tsv --dry-run
```
