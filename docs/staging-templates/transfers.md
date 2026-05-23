# Template `transfers` — transferts inter-locations

**Refresh model** : full reload par `(entity_type='transfers', source_system)`.
**Target canonique** : `nodes` table (node_type='TransferSupply' / 'TransferDemand').

## Colonnes

| Colonne | Type | Obligatoire | Contraintes |
|---------|------|-------------|-------------|
| `external_id` | TEXT | ✓ | unique par source_system |
| `item_external_id` | TEXT | ✓ | doit exister dans `items` |
| `from_location_external_id` | TEXT | ✓ | source du transfert (plant typiquement) |
| `to_location_external_id` | TEXT | ✓ | destination (DC typiquement) |
| `quantity` | NUMERIC | ✓ | > 0 |
| `expected_arrival_date` | DATE | ✗ | ISO 8601 |
| `status` | TEXT | ✗ | freestyle (in_transit / received / cancelled) |

## Règles DQ

| Niveau | Code | Severity |
|--------|------|----------|
| L1 | `MISSING_REQUIRED` | error |
| L2 | `L2_UNKNOWN_REF` | error (sur les 3 references: item, from, to) |
| L3 | `L3_QUANTITY_NONPOSITIVE` | error |
| L3 | `L3_DATE_OUT_OF_RANGE` | warning |
| L4 | `L4_DUPLICATE_EXTERNAL_ID` | error |

## Exemple curl

```bash
curl -X POST "${OOTILS_API_URL}/v1/staging/upload" \
  -F "file=@transfers.tsv" -F "entity_type=transfers" \
  -F "source_system=WMS-CENTRAL" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}"
```
