# Template `planning_params` — paramètres MRP par (item × location)

**Endpoint** : `POST /v1/ingest/planning-params`
**Refresh model** : **SCD2 transparent** (ADR-014 D3). Le client pousse l'état courant ; l'API compare à la ligne active et :
- ne fait rien si rien n'a changé (idempotent),
- UPDATE en place si la ligne active a été créée aujourd'hui (changement intra-journée),
- ferme l'active row (`effective_to = aujourd'hui`) + insère une nouvelle ligne (`effective_from = aujourd'hui`) sinon.

L'historique est préservé automatiquement. Le client **ne gère jamais les dates `effective_*`** — c'est invisible pour lui.

**Target canonique** : table `item_planning_params` (mig 007 + 021).

## Champs

| Champ | Type | Obligatoire | Contraintes | Mappe vers |
|---|---|:---:|---|---|
| `item_external_id` | TEXT | ✓ | FK → items.external_id | `item_id` (résolu) |
| `location_external_id` | TEXT | ✓ | FK → locations.external_id | `location_id` (résolu) |
| `lead_time_sourcing_days` | INT | ✗ | ≥ 0 | `lead_time_sourcing_days` |
| `lead_time_manufacturing_days` | INT | ✗ | ≥ 0 | `lead_time_manufacturing_days` |
| `lead_time_transit_days` | INT | ✗ | ≥ 0 | `lead_time_transit_days` |
| `safety_stock_qty` | NUMERIC | ✗ | ≥ 0 | `safety_stock_qty` |
| `safety_stock_days` | NUMERIC | ✗ | ≥ 0 | `safety_stock_days` |
| `reorder_point_qty` | NUMERIC | ✗ | ≥ 0 | `reorder_point_qty` |
| `min_order_qty` | NUMERIC | ✗ | > 0 | `min_order_qty` |
| `max_order_qty` | NUMERIC | ✗ | > 0 | `max_order_qty` |
| `order_multiple` | NUMERIC | ✗ | > 0 | `order_multiple` |
| `lot_size_rule` | ENUM | ✗ | ∈ {`LOTFORLOT`, `FIXED_QTY`, `EOQ`, `POQ`, `MIN_MAX`, `MULTIPLE`} — défaut `LOTFORLOT` | `lot_size_rule` |
| `planning_horizon_days` | INT | ✗ | > 0 — défaut 90 | `planning_horizon_days` |
| `is_make` | BOOLEAN | ✗ | défaut `false` | `is_make` |
| `preferred_supplier_external_id` | TEXT | ✗ | FK → suppliers.external_id | `preferred_supplier_id` |
| `economic_order_qty` | NUMERIC | ✗ | > 0 (pour `lot_size_rule=EOQ`) | `economic_order_qty` |
| `lot_size_poq_periods` | INT | ✗ | > 0 — défaut 1 | `lot_size_poq_periods` |
| `order_multiple_qty` | NUMERIC | ✗ | > 0 | `order_multiple_qty` |
| `frozen_time_fence_days` | INT | ✗ | ≥ 0 — défaut 7 | `frozen_time_fence_days` |
| `slashed_time_fence_days` | INT | ✗ | > 0 — défaut 30 | `slashed_time_fence_days` |
| `forecast_consumption_strategy` | ENUM | ✗ | ∈ {`max_only`, `consume_forward`, `consume_backward`, `consume_both`} | `forecast_consumption_strategy` |
| `consumption_window_days` | INT | ✗ | > 0 — défaut 7 | `consumption_window_days` |

**Note** : `lead_time_total_days` est une colonne **GÉNÉRÉE** côté DB (`sourcing + manufacturing + transit`). Jamais à pousser côté client.

## Sémantique partial push (important)

Un champ **omis** dans la requête signifie "garde la valeur active courante" — pas "remets à NULL". Si tu veux explicitement effacer un champ, pousse `null`.

```json
// Push 1 — installe les valeurs initiales
{
  "params": [{
    "item_external_id": "PUMP-01",
    "location_external_id": "DC-ATL",
    "lead_time_sourcing_days": 5,
    "safety_stock_qty": 50,
    "lot_size_rule": "LOTFORLOT"
  }]
}

// Push 2 (même jour) — ne change que safety_stock_qty
// → UPDATE in place (même ligne, valeur écrasée)
{
  "params": [{
    "item_external_id": "PUMP-01",
    "location_external_id": "DC-ATL",
    "safety_stock_qty": 80
  }]
}

// Push 3 (lendemain) — ne change que safety_stock_qty
// → ROTATED : ferme l'ancienne ligne avec effective_to=aujourd'hui,
//   crée une nouvelle ligne avec effective_from=aujourd'hui
//   et carry-over lead_time_sourcing_days=5 + lot_size_rule=LOTFORLOT
{
  "params": [{
    "item_external_id": "PUMP-01",
    "location_external_id": "DC-ATL",
    "safety_stock_qty": 100
  }]
}
```

## Réponse type

```json
{
  "status": "completed",
  "summary": {
    "total": 1,
    "inserted": 1,        // CREATED + ROTATED (nouvelle ligne)
    "updated": 0,         // UPDATED_INPLACE (ligne courante modifiée)
    "errors": 0
  },
  "results": [
    {
      "item_external_id": "PUMP-01",
      "location_external_id": "DC-ATL",
      "action": "created" | "rotated" | "updated_inplace" | "noop",
      "changed_fields": ["safety_stock_qty"],
      "param_id": "uuid-de-la-ligne"
    }
  ],
  "batch_id": "uuid"
}
```

| `action` | Sens |
|---|---|
| `created` | Première ligne pour ce `(item, location)` — aucune historique préalable |
| `noop` | Tous les champs poussés matchent la ligne active — idempotent |
| `updated_inplace` | Changement le même jour que la création de la ligne active — UPDATE en place |
| `rotated` | Changement à un jour ultérieur — close active + insert new |

## Erreurs typiques

| HTTP | Cause |
|---|---|
| 422 | `item_external_id` ou `location_external_id` inconnu — référence métier manquante |
| 422 | `preferred_supplier_external_id` poussé mais inconnu |
| 422 | `lot_size_rule` hors enum |
| 422 | `forecast_consumption_strategy` hors enum |
| 422 | `min_order_qty <= 0` (et autres contraintes positives) |

## Exemple curl

```bash
curl -X POST "${OOTILS_API_URL}/v1/ingest/planning-params" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "params": [
      {
        "item_external_id": "PUMP-01",
        "location_external_id": "DC-ATL",
        "lead_time_sourcing_days": 5,
        "lead_time_manufacturing_days": 0,
        "lead_time_transit_days": 3,
        "safety_stock_qty": 50,
        "reorder_point_qty": 100,
        "min_order_qty": 10,
        "lot_size_rule": "LOTFORLOT",
        "planning_horizon_days": 90,
        "is_make": false
      }
    ],
    "dry_run": false
  }'
```

## Dry run

Pose `"dry_run": true` pour voir les actions qui seraient appliquées sans écrire en DB. Utile pour previewer un batch avant de le committer.
