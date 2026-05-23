# Template `supplier_items` — fournisseur × item

**Refresh model** : full reload par `(entity_type='supplier_items', source_system)`.
**Target canonique** : `supplier_items` table.

## Colonnes

| Colonne | Type | Obligatoire | Contraintes | Mappe vers |
|---------|------|-------------|-------------|------------|
| `supplier_external_id` | TEXT | ✓ | doit exister dans `suppliers` | join via `external_references` |
| `item_external_id` | TEXT | ✓ | doit exister dans `items` | join via `external_references` |
| `lead_time_days` | INT | ✓ | > 0 | `supplier_items.lead_time_days` |
| `moq` | NUMERIC | ✗ | > 0 si présent | `supplier_items.moq` |
| `unit_cost` | NUMERIC | ✗ | >= 0 si présent | `supplier_items.unit_cost` |
| `currency` | TEXT | ✗ | ISO 4217 (EUR, USD, ...) | `supplier_items.currency` |
| `is_preferred` | BOOLEAN | ✗ | true / false | `supplier_items.is_preferred` |
| `valid_from` | DATE | ✗ | ISO 8601 | `supplier_items.valid_from` |
| `valid_to` | DATE | ✗ | ISO 8601, >= valid_from | `supplier_items.valid_to` |

## Règles DQ

| Niveau | Code | Severity | Description |
|--------|------|----------|-------------|
| L1 | `MISSING_REQUIRED` | error | colonne obligatoire vide |
| L2 | `L2_UNKNOWN_REF` | error | `supplier_external_id` ou `item_external_id` introuvable |
| L3 | `L3_LEAD_TIME_NONPOSITIVE` | error | `lead_time_days <= 0` |
| L3 | `L3_LEAD_TIME_SUSPICIOUS` | warning | `lead_time_days > 365` |
| L3 | `L3_QUANTITY_NONPOSITIVE` | error | `moq <= 0` |
| L3 | `L3_UNIT_COST_NEGATIVE` | error | `unit_cost < 0` |
| L3 | `L3_DATE_RANGE_INVERTED` | error | `valid_to < valid_from` |
| L4 | `L4_SUPPLIER_INACTIVE` | error | fournisseur référencé n'est pas `active` |

## Exemple curl

```bash
curl -X POST "${OOTILS_API_URL}/v1/staging/upload" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}" \
  -F "file=@supplier_items.tsv" \
  -F "entity_type=supplier_items" \
  -F "source_system=SAP-EU"
```
