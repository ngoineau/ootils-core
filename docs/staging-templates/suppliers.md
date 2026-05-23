# Template `suppliers` — fournisseurs

**Refresh model** : full reload par `(entity_type='suppliers', source_system)`.
**Target canonique** : `suppliers` table.

## Colonnes

| Colonne | Type | Obligatoire | Contraintes | Mappe vers |
|---------|------|-------------|-------------|------------|
| `external_id` | TEXT | ✓ | unique par source_system, max 255 | `suppliers.external_id` |
| `name` | TEXT | ✓ | max 255 chars | `suppliers.name` |
| `country` | TEXT | ✗ | ISO 3166-1 alpha-2 | `suppliers.country` |
| `lead_time_days` | INT | ✗ | > 0 ; warning si > 365 | `suppliers.lead_time_days` |
| `reliability_score` | NUMERIC | ✗ | ∈ [0, 1] | `suppliers.reliability_score` |
| `status` | ENUM | ✓ | ∈ {`active`, `inactive`, `blocked`} | `suppliers.status` |

## Règles DQ

| Niveau | Code | Severity | Description |
|--------|------|----------|-------------|
| L1 | `MISSING_REQUIRED` | error | colonne obligatoire vide |
| L3 | `L3_INVALID_SUPPLIER_STATUS` | error | `status` hors liste |
| L3 | `L3_LEAD_TIME_NONPOSITIVE` | error | `lead_time_days <= 0` |
| L3 | `L3_LEAD_TIME_SUSPICIOUS` | warning | `lead_time_days > 365` |
| L3 | `L3_RELIABILITY_OUT_OF_RANGE` | error | score hors [0, 1] |
| L4 | `L4_DUPLICATE_EXTERNAL_ID` | error | dup intra-batch |
| L4 | `L4_INTER_BATCH_COLLISION` | warning | `external_id` dans autre batch open |

## Soft-delete

Suppliers absents du batch → `suppliers.status = 'inactive'` + mapping retiré pour cette source.

## Exemple curl

```bash
curl -X POST "${OOTILS_API_URL}/v1/staging/upload" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}" \
  -F "file=@suppliers_master.csv" \
  -F "entity_type=suppliers" \
  -F "source_system=SAP-EU"
```
