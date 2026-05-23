# Template `purchase_orders` — commandes fournisseurs ouvertes

**Refresh model** : full reload par `(entity_type='purchase_orders', source_system)`.
**Target canonique** : `nodes` table (node_type='PurchaseOrderSupply') via le pipeline d'ingestion existant.

## Colonnes

| Colonne | Type | Obligatoire | Contraintes | Mappe vers |
|---------|------|-------------|-------------|------------|
| `external_id` | TEXT | ✓ | unique par source_system, max 255 | `nodes.external_id` (via mapping) |
| `item_external_id` | TEXT | ✓ | doit exister dans `items` | `nodes.item_id` |
| `location_external_id` | TEXT | ✓ | doit exister dans `locations` | `nodes.location_id` |
| `supplier_external_id` | TEXT | ✓ | doit exister dans `suppliers` | référence supplier |
| `quantity` | NUMERIC | ✓ | > 0 | `nodes.quantity` |
| `uom` | TEXT | ✓ | code UOM | `nodes.qty_uom` |
| `expected_delivery_date` | DATE | ✓ | ISO 8601 | `nodes.time_ref` |
| `status` | ENUM | ✓ | ∈ {`open`, `in_transit`, `received`, `cancelled`, `closed`} | metadata |

## Règles DQ

| Niveau | Code | Severity | Description |
|--------|------|----------|-------------|
| L1 | `MISSING_REQUIRED` | error | colonne obligatoire vide |
| L2 | `L2_UNKNOWN_REF` | error | item / location / supplier introuvables |
| L3 | `L3_QUANTITY_NONPOSITIVE` | error | `quantity <= 0` |
| L3 | `L3_QUANTITY_SUSPICIOUS` | warning | `quantity > 10M` |
| L3 | `L3_INVALID_PO_STATUS` | error | `status` hors liste |
| L3 | `L3_DATE_OUT_OF_RANGE` | warning | date hors [2000, 2100] |
| L4 | `L4_DUPLICATE_EXTERNAL_ID` | error | dup intra-batch |
| L4 | `L4_INTER_BATCH_COLLISION` | warning | `external_id` dans autre batch open |

## Exemple curl

```bash
curl -X POST "${OOTILS_API_URL}/v1/staging/upload" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}" \
  -F "file=@open_pos.tsv" \
  -F "entity_type=purchase_orders" \
  -F "source_system=SAP-EU"
```
