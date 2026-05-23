# Template `work_orders` — ordres de fabrication en cours

**Refresh model** : full reload par `(entity_type='work_orders', source_system)`.
**Target canonique** : `nodes` table (node_type='WorkOrderSupply').

## Colonnes

| Colonne | Type | Obligatoire | Contraintes | Mappe vers |
|---------|------|-------------|-------------|------------|
| `external_id` | TEXT | ✓ | unique par source_system | `nodes.external_id` |
| `item_external_id` | TEXT | ✓ | doit exister dans `items` (typiquement FG ou SA) | `nodes.item_id` |
| `location_external_id` | TEXT | ✓ | site de production (plant) | `nodes.location_id` |
| `quantity` | NUMERIC | ✓ | > 0 | `nodes.quantity` |
| `uom` | TEXT | ✗ | code UOM (défaut: EA) | `nodes.qty_uom` |
| `expected_completion_date` | DATE | ✗ | ISO 8601 | `nodes.time_ref` |
| `status` | ENUM | ✓ | ∈ {`planned`, `released`, `in_progress`, `completed`, `cancelled`} | metadata |

## Règles DQ

| Niveau | Code | Severity |
|--------|------|----------|
| L1 | `MISSING_REQUIRED` | error |
| L2 | `L2_UNKNOWN_REF` | error |
| L3 | `L3_QUANTITY_NONPOSITIVE` | error |
| L3 | `L3_INVALID_WO_STATUS` | error |
| L3 | `L3_DATE_OUT_OF_RANGE` | warning |
| L4 | `L4_DUPLICATE_EXTERNAL_ID` | error |

## Exemple curl

```bash
curl -X POST "${OOTILS_API_URL}/v1/staging/upload" \
  -F "file=@work_orders.tsv" -F "entity_type=work_orders" \
  -F "source_system=MES-FACTORY-1" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}"
```
