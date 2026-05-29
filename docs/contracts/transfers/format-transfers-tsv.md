# Format de fichier — `transfers.tsv`

> Fichier des **transferts de stock inter-sites** (Stock Transfer Orders / STO).
> Mouvement entre deux locations internes du réseau : une location envoie, une autre reçoit.
> Endpoint cible : `POST /v1/ingest/transfers`.

---

## 1. Caractéristiques du fichier

| Propriété | Valeur |
|---|---|
| **Nom** | `transfers.tsv` (exact) |
| **Format** | TSV |
| **Encodage** | UTF-8 (BOM toléré) |
| **Délimiteur** | tabulation (`\t`) |
| **Header** | ligne 1 obligatoire |
| **Endpoint** | `POST /v1/ingest/transfers` |

---

## 2. Nature de la donnée

Une ligne = **un transfert de stock entre 2 sites** :
> « On déplace `quantity` unités de `item_external_id` depuis `from_location_external_id` vers `to_location_external_id`, arrivée prévue le `expected_delivery_date`. Statut actuel : `status`. »

Vu côté graphe Ootils :
- **Sortie** prévue sur `from_location` (consommation)
- **Entrée** prévue sur `to_location` (approvisionnement)
- Le node TransferSupply est rattaché au **PI de la destination** (cf. docstring endpoint : « The node is wired to the PI of the **destination** »)

Comparaison avec PO :
- Un PO = entrée venant de l'extérieur (fournisseur → site interne)
- Un transfer = redistribution interne (site interne → site interne)

---

## 3. Colonnes

**Ordre figé par le contrat canonique** : `external_id`, `item_external_id`, `from_location_external_id`, `to_location_external_id`, `quantity`, `expected_delivery_date`, `status`.

| # | Colonne | Obligatoire | Type | Domaine | Description |
|---|---|---|---|---|---|
| 1 | `external_id` | **oui** | texte | non-vide, unique dans le fichier | Numéro de STO (Stock Transfer Order). Clé d'upsert. |
| 2 | `item_external_id` | **oui** | texte | FK `items` | Article transféré. |
| 3 | `from_location_external_id` | **oui** | texte | FK `locations` | Site **origine** (qui expédie). |
| 4 | `to_location_external_id` | **oui** | texte | FK `locations` | Site **destination** (qui reçoit). Doit être différent de `from_location_external_id`. |
| 5 | `quantity` | **oui** | décimal | > 0 | Quantité transférée. |
| 6 | `expected_delivery_date` | **oui** | date | ISO `YYYY-MM-DD` | Date d'arrivée prévue à destination. |
| 7 | `status` | non | enum | voir §4 | Défaut : `planned`. |

---

## 4. Les 4 statuts de transfer

| Statut | Sémantique | Impact projection |
|---|---|---|
| `planned` | Transfert planifié, pas encore expédié | **Inclus** comme entrée prévue à destination + sortie prévue à origine |
| `in_transit` | Marchandise expédiée, en cours de transport | Idem, **plus de retour arrière possible** côté origine (le stock est déjà sorti) |
| `delivered` | Reçu à destination (intégré au on_hand destination) | **N'incrémente plus la projection** — déjà comptée dans `on_hand` destination |
| `cancelled` | Transfert annulé | **Ignoré** |

---

## 5. Cohérence métier

### 5.1 Contraintes d'usage

- `from_location` doit avoir le stock disponible au moment de l'expédition (sinon shortage à l'origine)
- Le transit time effectif (entre `from` et `to`) devrait être cohérent avec `distribution_links.transit_lead_time_days` (cf. task LANES-LATER) si défini
- Un transfert d'un `customer_virtual` vers ailleurs n'a pas de sens (les customers reçoivent, ils n'expédient pas dans le réseau interne)
- Un transfert d'un `supplier_virtual` vers ailleurs est rare (modélisé plutôt comme PO)

### 5.2 Direction logique typique

```
PLANT-LYON ──► WH-PARIS-01 ──► DC-LILLE ──► CVL-RETAIL-NORD
   (sortie usine)  (transferts internes)    (livraison client virtuelle)
```

Les transferts couvrent les 2 flèches du milieu.

### 5.3 Lien avec `distribution_links` (task LANES-LATER)

Aujourd'hui : un transfer pose un transit time **directement dans `expected_delivery_date`**, sans référencer de canal officiel.

Quand `distribution_links` sera consommé : on pourra valider qu'un transfer respecte les canaux **autorisés** (= les `distribution_links` actifs entre origine et destination), avec leur `shipment_days` et `minimum_shipment_qty`.

---

## 6. Exemples

### 6.1 Exemple minimal (1 transfer)

```
external_id	item_external_id	from_location_external_id	to_location_external_id	quantity	expected_delivery_date	status
TR-2026-001	FG-APU-100	WH-PARIS-01	DC-LILLE	20	2026-06-03	planned
```

### 6.2 Exemple réseau (plusieurs flux)

```
external_id	item_external_id	from_location_external_id	to_location_external_id	quantity	expected_delivery_date	status
TR-2026-001	FG-APU-100	PLANT-LYON	WH-PARIS-01	50	2026-06-02	in_transit
TR-2026-002	FG-APU-100	WH-PARIS-01	DC-LILLE	20	2026-06-04	planned
TR-2026-003	FG-APU-100	WH-PARIS-01	DC-LILLE	30	2026-06-11	planned
TR-2026-004	FG-APU-200	PLANT-LYON	DC-LILLE	10	2026-06-06	planned
TR-2026-005	SUB-HOUSING-100	PLANT-LYON	WH-PARIS-01	5	2026-05-26	delivered
```

Lecture :
- TR-001 : 50 AquaPump 100 partent de Lyon vers Paris (en transit)
- TR-002, TR-003 : 50 unités au total répartis sur 2 dates de Paris vers Lille
- TR-004 : 10 AquaPump 200 direct de Lyon à Lille
- TR-005 : déjà livré le 26/05, n'impacte plus la projection

### 6.3 Cas invalides

```
external_id	item_external_id	from_location_external_id	to_location_external_id	quantity	expected_delivery_date	status
TR-X	FG-APU-100	PLANT-LYON	PLANT-LYON	20	2026-06-03	planned       ← from == to → 422
TR-X	FG-APU-100	LOC-X	DC-LILLE	20	2026-06-03	planned             ← from inconnu → 422
TR-X	FG-APU-100	PLANT-LYON	LOC-X	20	2026-06-03	planned         ← to inconnu → 422
TR-X	ITEM-X	PLANT-LYON	DC-LILLE	20	2026-06-03	planned             ← item inconnu → 422
TR-X	FG-APU-100	PLANT-LYON	DC-LILLE	0	2026-06-03	planned         ← quantity = 0 → 422
TR-X	FG-APU-100	PLANT-LYON	DC-LILLE	20	03-06-2026	planned       ← date invalide → 422
TR-X	FG-APU-100	PLANT-LYON	DC-LILLE	20	2026-06-03	pending        ← statut inconnu → 422
```

---

## 7. Comportement à l'ingestion

### 7.1 Identification

Clé business : `external_id`.
- **Existe en base** → UPDATE des 6 autres champs (utile pour le cycle de vie)
- **N'existe pas** → INSERT (nouveau node TransferSupply)

### 7.2 Cycle typique

```
Jour J        : INSERT avec status='planned'
Jour expédition : UPDATE avec status='in_transit' (parallèlement, on_hand origine baisse)
Jour réception  : UPDATE avec status='delivered' (parallèlement, on_hand destination monte)
```

### 7.3 Validation FK

`item_external_id`, `from_location_external_id`, `to_location_external_id` doivent exister. **Et** `from ≠ to`. Toute violation = batch rejeté (422).

---

## 8. Pipeline

```
data/inbox/transfers.tsv
        │
        ▼
   parse TSV
        │
        ▼
   validation (3 FK + from ≠ to + quantity > 0 + date ISO + status enum)
        │
        ├─ FK manquante / invalid ──► 422 → data/rejected/
        │
        ▼ OK
   POST /v1/ingest/transfers
        │
        ▼
   upsert dans `nodes` (TransferSupply rattaché au PI destination)
        │
        ▼
   data/processed/transfers_YYYYMMDD_HHMMSS.tsv + .report.json
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
7.  purchase_orders.tsv     ← ✅
8.  customer_orders.tsv     ← ✅
9.  forecasts.tsv           ← ✅
10. transfers.tsv           ← ICI (2 FK locations + 1 FK item)
11. bom_*.tsv               ← après
```

---

## 10. Limitations V1.0

| Manque | V1.1 envisagé |
|---|---|
| Pas de `shipment_date` (date d'expédition vs date d'arrivée) | V1.1 — utile pour le suivi in_transit |
| Pas de `transport_mode`, `carrier`, `tracking_number` | Couvert par `transportation_lanes` (task LANES-LATER) |
| Pas de `transit_cost` | Idem |
| Pas de `received_quantity` (vs ordered) | V1.1 — livraisons partielles |
| Pas de FK vers `distribution_link_id` | V1.1 — quand LANES-LATER sera fait |

---

## 11. Validation rapide

```bash
python scripts/ingest_file.py data/inbox/transfers.tsv --dry-run
```
