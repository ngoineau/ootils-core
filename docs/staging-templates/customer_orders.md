# Template `customer_orders` — commandes clients ouvertes

**Refresh model** : full reload par `(entity_type='customer_orders', source_system)`.
**Target canonique** : `nodes` table (node_type='CustomerOrderDemand').

## Colonnes

| Colonne | Type | Obligatoire | Contraintes |
|---------|------|-------------|-------------|
| `external_id` | TEXT | ✓ | unique par source_system |
| `item_external_id` | TEXT | ✓ | doit exister dans `items` |
| `location_external_id` | TEXT | ✓ | typiquement un DC |
| `quantity` | NUMERIC | ✓ | > 0 |
| `due_date` | DATE | ✗ | ISO 8601 |
| `status` | ENUM | ✗ | ∈ {`open`, `shipped`, `delivered`, `cancelled`} |

## Règles DQ

| Niveau | Code | Severity |
|--------|------|----------|
| L1 | `MISSING_REQUIRED` | error |
| L2 | `L2_UNKNOWN_REF` | error |
| L3 | `L3_QUANTITY_NONPOSITIVE` | error |
| L3 | `L3_INVALID_CO_STATUS` | error |
| L3 | `L3_DATE_OUT_OF_RANGE` | warning |
| L4 | `L4_DUPLICATE_EXTERNAL_ID` | error |

## Exemple curl

```bash
curl -X POST "${OOTILS_API_URL}/v1/staging/upload" \
  -F "file=@open_orders.tsv" -F "entity_type=customer_orders" \
  -F "source_system=SAP-EU-COMMERCIAL" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}"
```
