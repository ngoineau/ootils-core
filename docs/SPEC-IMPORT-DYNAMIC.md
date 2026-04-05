# Ootils — Spec Import Données Dynamiques (Transactionnelles)

> **Version :** 1.0 — 2026-04-05  
> **Auteur :** Architecture Team  
> **Statut :** Draft — validé sur base du schéma DB sprint 1 et de l'API events v1

---

## 1. Vue d'ensemble des flux

```
ERP/WMS/EDI
    │
    ├── [Batch nightly]    → POST /v1/import/batch/{type}  → Ootils DB → Propagation
    └── [Streaming/event]  → POST /v1/events               → Ootils DB → Propagation
```

### Matrice flux × mode

| Flux            | Batch | Streaming | Fréquence batch | Full/Delta   |
|-----------------|-------|-----------|-----------------|--------------|
| On-Hand         | ✓     | ✓         | Quotidien       | Full replace |
| Purchase Orders | ✓     | ✓         | Quotidien       | Delta        |
| Customer Orders | ✓     | ✓         | Horaire         | Delta        |
| Forecasts       | ✓     | ✗         | Hebdo           | Full replace |
| Work Orders     | ✓     | ✓         | Quotidien       | Delta        |
| Transfers       | ✓     | ✓         | Quotidien       | Delta        |

### Architecture de propagation

Chaque import (batch ou streaming) génère un ou plusieurs événements dans la table `events`.  
Ces événements sont consommés par le moteur de propagation qui recalcule les nœuds affectés dans le graphe.

**Types d'événements existants (contrainte CHECK DB) :**

| event_type          | Déclencheur                              |
|---------------------|------------------------------------------|
| `onhand_updated`    | Mise à jour On-Hand (batch ou streaming) |
| `supply_qty_changed`| Modification quantité supply (PO, WO, Transfer) |
| `supply_date_changed`| Modification date supply               |
| `demand_qty_changed`| Modification quantité demand (CO, Forecast) |
| `po_date_changed`   | Modification date PO                     |
| `ingestion_complete`| Fin de traitement batch                  |
| `structure_changed` | Modification du graphe (nœud ajouté/supprimé) |

> **Note :** Le payload de l'event est typé (pas de JSONB libre) : champs `old_date`, `new_date`, `old_quantity`, `new_quantity`, `field_changed`, `trigger_node_id`. Source valide : `api | ingestion | engine | user | test`.

---

## 2. Flux : On-Hand Supply

> **Pourquoi TSV plutôt que CSV ?**
> Les données supply chain contiennent fréquemment des virgules dans les libellés (noms d'articles, descriptions, adresses). Le tab comme séparateur élimine ce risque sans guillemets d'échappement. SAP exporte nativement en tab (transactions SE16, MB52, ME2M). L'extension `.tsv` est reconnue par Excel, pandas, et tous les outils ETL standard.

### Champs TSV batch

```
item_external_id	location_external_id	quantity	uom	as_of_date	lot_number
SKU-001	DC-PARIS	450	EA	2026-04-05	LOT-2024-001
SKU-002	DC-PARIS	120	EA	2026-04-05	
```

**Champs obligatoires :** `item_external_id`, `location_external_id`, `quantity`, `uom`, `as_of_date`  
**Champs optionnels :** `lot_number` (nullable, gestion lotissement)

### Streaming event

```json
POST /v1/events
{
  "event_type": "onhand_updated",
  "source": "ingestion",
  "trigger_node_id": "<uuid-du-nœud-OnHandSupply>",
  "field_changed": "quantity",
  "old_quantity": 300,
  "new_quantity": 450
}
```

> Le `trigger_node_id` est résolu lors de l'ingestion : lookup sur `(item_external_id, location_external_id)` → `node_id`.

### Impact graph

- Crée ou met à jour le nœud `OnHandSupply` pour `(item × location × as_of_date)`
- Déclenche recalcul `ProjectedInventory` pour cet item × location (dirty flag propagation)
- **Mode Full replace :** avant insert, marque `is_active = false` sur tous les nœuds `OnHandSupply` existants pour `(item × location)`, puis insère les nouvelles lignes
- Génère événement `onhand_updated` → propagation downstream

---

## 3. Flux : Purchase Orders

### Champs TSV

```
po_number	line_number	item_external_id	supplier_external_id	location_external_id	quantity	uom	expected_date	status
PO-2026-001	1	SKU-001	SUP-001	DC-PARIS	500	EA	2026-04-20	confirmed
PO-2026-001	2	SKU-002	SUP-001	DC-PARIS	200	EA	2026-04-22	pending
```

**Clé métier :** `(po_number, line_number)`  
**Champs obligatoires :** `po_number`, `line_number`, `item_external_id`, `location_external_id`, `quantity`, `uom`, `expected_date`, `status`  
**Champs optionnels :** `supplier_external_id`

### Statuts valides

| Statut                | is_active | Comportement nœud          |
|-----------------------|-----------|----------------------------|
| `confirmed`           | true      | Crée/met à jour            |
| `pending`             | true      | Crée/met à jour            |
| `partially_received`  | true      | Met à jour quantité résiduelle |
| `cancelled`           | false     | Soft delete (is_active=false) |

### Streaming event

```json
POST /v1/events
{
  "event_type": "supply_date_changed",
  "source": "api",
  "trigger_node_id": "<uuid-nœud-PurchaseOrderSupply>",
  "field_changed": "expected_date",
  "old_date": "2026-04-18",
  "new_date": "2026-04-20"
}
```

### Impact graph

- Crée nœud `PurchaseOrderSupply` (clé : `po_number + line_number`)
- Si `status = cancelled` → `is_active = false` (soft delete), génère `structure_changed`
- Modification date → `po_date_changed` → recalcul `ProjectedInventory` downstream
- Modification quantité → `supply_qty_changed`

---

## 4. Flux : Customer Orders

### Champs TSV

```
order_number	line_number	item_external_id	location_external_id	quantity	uom	requested_date	confirmed_date	status
CO-2026-001	1	SKU-001	DC-PARIS	100	EA	2026-04-15	2026-04-15	confirmed
CO-2026-001	2	SKU-003	DC-PARIS	50	EA	2026-04-15		pending
```

**Clé métier :** `(order_number, line_number)`  
**Champs obligatoires :** `order_number`, `line_number`, `item_external_id`, `location_external_id`, `quantity`, `uom`, `requested_date`, `status`  
**Champs optionnels :** `confirmed_date` (nullable si statut pending)

### Statuts valides

`confirmed` | `pending` | `cancelled` | `shipped` | `partially_shipped`

### Streaming event

```json
POST /v1/events
{
  "event_type": "demand_qty_changed",
  "source": "api",
  "trigger_node_id": "<uuid-nœud-CustomerOrderDemand>",
  "field_changed": "quantity",
  "old_quantity": 80,
  "new_quantity": 100
}
```

### Impact graph

- Crée nœud `CustomerOrderDemand`
- Modification → `demand_qty_changed` → recalcul `ProjectedInventory` et alertes de couverture
- `status = cancelled` → soft delete `is_active = false`
- Fréquence horaire justifiée par la latence acceptable en demand sensing

---

## 5. Flux : Forecasts

### Champs TSV

```
item_external_id	location_external_id	forecast_date	quantity	uom	bucket_type	source
SKU-001	DC-PARIS	2026-04-05	15	EA	day	statistical
SKU-001	DC-PARIS	2026-W15	90	EA	week	consensus
SKU-001	DC-PARIS	2026-04	350	EA	month	budget
```

**Clé métier :** `(item_external_id, location_external_id, forecast_date, bucket_type, source)`  
**Champs obligatoires :** tous

### Valeurs bucket_type

| bucket_type | Format forecast_date | Exemple       |
|-------------|----------------------|---------------|
| `day`       | YYYY-MM-DD           | 2026-04-05    |
| `week`      | YYYY-Www             | 2026-W15      |
| `month`     | YYYY-MM              | 2026-04       |

### Pas de streaming

Les forecasts ne sont pas envoyés en streaming — ils résultent de processus batch (stat engine, consensus, budget upload). Un recalcul intraday déclenche un nouveau batch complet.

### Impact graph

- Crée nœuds `ForecastDemand` en Full replace par `(item × location × source)`
- Génère `demand_qty_changed` pour chaque bucket modifié
- Déclenche recalcul couverture et alertes

---

## 6. Flux : Work Orders

### Champs TSV

```
wo_number	item_external_id	location_external_id	quantity	uom	start_date	end_date	status
WO-2026-001	SKU-FIN-001	USINE-NORD	200	EA	2026-04-10	2026-04-15	released
WO-2026-002	SKU-FIN-002	USINE-NORD	100	EA	2026-04-12	2026-04-18	planned
```

**Clé métier :** `wo_number`  
**Champs obligatoires :** tous

### Statuts valides

`planned` | `released` | `in_progress` | `completed` | `cancelled`

### Streaming event

```json
POST /v1/events
{
  "event_type": "supply_date_changed",
  "source": "ingestion",
  "trigger_node_id": "<uuid-nœud-WorkOrderSupply>",
  "field_changed": "end_date",
  "old_date": "2026-04-15",
  "new_date": "2026-04-18"
}
```

### Impact graph

- Crée nœud `WorkOrderSupply` (supply de production)
- `status = completed` → clôture le nœud (is_active peut rester true pour historique)
- `status = cancelled` → soft delete
- Modification end_date → `supply_date_changed` → recalcul downstream
- Les composants consommés (BOM) génèrent des `demand_qty_changed` associés

---

## 7. Flux : Transfers

### Champs TSV

```
transfer_number	item_external_id	from_location	to_location	quantity	uom	ship_date	expected_receipt_date	status
TR-2026-001	SKU-001	USINE-NORD	DC-PARIS	300	EA	2026-04-08	2026-04-10	planned
TR-2026-002	SKU-002	DC-PARIS	DC-LYON	150	EA	2026-04-09	2026-04-11	in_transit
```

**Clé métier :** `transfer_number`  
**Champs obligatoires :** tous

### Statuts valides

`planned` | `in_transit` | `received` | `cancelled`

### Streaming event

```json
POST /v1/events
{
  "event_type": "supply_date_changed",
  "source": "ingestion",
  "trigger_node_id": "<uuid-nœud-TransferSupply>",
  "field_changed": "expected_receipt_date",
  "old_date": "2026-04-10",
  "new_date": "2026-04-12"
}
```

### Impact graph

- Un transfer génère **deux nœuds** : `TransferDemand` sur `from_location` et `TransferSupply` sur `to_location`
- Liés par une edge `fulfills`
- `status = in_transit` → ship_date figé, seul expected_receipt_date est modifiable
- `status = received` → soft close (historique conservé)
- `status = cancelled` → soft delete les deux nœuds

---

## 8. Règles de déduplication

### Clé de déduplication par flux

| Flux            | Clé métier                                                   |
|-----------------|--------------------------------------------------------------|
| On-Hand         | `(item_external_id, location_external_id, as_of_date)`       |
| Purchase Orders | `(po_number, line_number)`                                   |
| Customer Orders | `(order_number, line_number)`                                |
| Forecasts       | `(item_external_id, location_external_id, forecast_date, bucket_type, source)` |
| Work Orders     | `(wo_number)`                                                |
| Transfers       | `(transfer_number)`                                          |

### Comportement UPSERT

- Tous les imports utilisent **UPSERT sur clé métier** (INSERT … ON CONFLICT DO UPDATE)
- Les champs modifiés génèrent les événements correspondants (delta detection avant commit)
- Pas de duplication de nœuds : l'`external_id` composite est la clé de résolution

### Soft delete

- `status = cancelled` → `is_active = false` sur le nœud (jamais de DELETE physique)
- Les nœuds inactifs sont exclus des calculs moteur mais conservés pour audit/historique
- Full replace (On-Hand, Forecasts) : `is_active = false` sur l'ensemble existant, puis insert des nouvelles lignes actives

---

## 9. Gestion des erreurs batch

### Format de réponse

```json
{
  "imported": 450,
  "skipped": 3,
  "errors": [
    {
      "line": 12,
      "code": "ITEM_NOT_FOUND",
      "message": "item_external_id 'SKU-999' inconnu"
    },
    {
      "line": 45,
      "code": "INVALID_DATE",
      "message": "expected_date '2026-13-01' invalide"
    },
    {
      "line": 78,
      "code": "INVALID_STATUS",
      "message": "status 'draft' non autorisé pour ce flux"
    }
  ]
}
```

### Codes d'erreur

| Code               | Description                                         |
|--------------------|-----------------------------------------------------|
| `ITEM_NOT_FOUND`   | `item_external_id` non résolu dans le référentiel   |
| `LOCATION_NOT_FOUND` | `location_external_id` non résolu                |
| `SUPPLIER_NOT_FOUND` | `supplier_external_id` non résolu                |
| `INVALID_DATE`     | Date non parseable ou hors plage acceptée           |
| `INVALID_STATUS`   | Valeur de statut non autorisée pour ce flux         |
| `MISSING_FIELD`    | Champ obligatoire absent                            |
| `INVALID_QUANTITY` | Quantité négative ou non numérique                  |
| `DUPLICATE_KEY`    | Ligne en doublon dans le même fichier batch         |

### Comportement en erreur

- **Ligne en erreur** : skippée, les autres lignes sont traitées (pas de rollback global)
- **Fichier malformé** (header manquant, encoding invalide) : rejet complet, `imported = 0`
- Les erreurs sont loggées avec `(batch_run_id, line_number, code, raw_value)`
- Un événement `ingestion_complete` est émis en fin de batch avec le summary

---

## 10. Points ouverts

| # | Sujet | Description | Priorité |
|---|-------|-------------|----------|
| 1 | Endpoint `/v1/import/batch/{type}` | Non encore implémenté — à créer (sprint 2). Format multipart/form-data ou JSON body avec TSV encodé en base64 ? | Haute |
| 2 | `on_hand_update` vs `onhand_updated` | Le task brief utilise `on_hand_update` mais la contrainte CHECK DB et l'API utilisent `onhand_updated`. **Retenir `onhand_updated`** comme canonical. | Résolu |
| 3 | Payload events streaming | L'API events n'accepte pas de `payload` libre (pas de JSONB) — les champs sont typés (`old_date`, `new_date`, etc.). Les exemples de cette spec sont alignés sur le schéma réel. | Résolu |
| 4 | Gestion lot (lot_number) | Champ présent dans On-Hand TSV mais pas de nœud `Lot` dans le graphe actuel. Modélisation à confirmer. | Moyenne |
| 5 | Transfers → deux nœuds | La création atomique des deux nœuds (TransferDemand + TransferSupply) et de leur edge doit être transactionnelle. | Haute |
| 6 | Forecasts Full replace par source | Full replace scoped par `(item × location × source)` ou global `(item × location)` ? Impacte les scénarios multi-sources (stat + budget coexistants). | Haute |
| 7 | Auth batch endpoint | Bearer token comme `/v1/events` ? Ou API key dédiée pour les jobs ERP ? | Moyenne |
| 8 | Idempotence batch | Rejouer le même fichier deux fois doit être idempotent. Géré par UPSERT mais à valider sur Full replace. | Haute |
| 9 | `partially_received` PO | Quantité résiduelle = quantité originale - quantité reçue. Ce calcul est-il dans le payload TSV ou calculé par Ootils à partir d'un champ `received_quantity` ? | Moyenne |
| 10 | Streaming WO composants | Les consommations BOM liées à un WO released génèrent-elles des `demand_qty_changed` automatiquement ou faut-il les envoyer explicitement ? | Moyenne |
