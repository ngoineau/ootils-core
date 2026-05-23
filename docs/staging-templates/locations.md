# Template `locations` — sites physiques

**Refresh model** : full reload par `(entity_type='locations', source_system)`.
**Target canonique** : `locations` table.

## Colonnes

| Colonne | Type | Obligatoire | Contraintes | Mappe vers |
|---------|------|-------------|-------------|------------|
| `external_id` | TEXT | ✓ | unique par source_system, max 255 chars | `locations.external_id` |
| `name` | TEXT | ✓ | max 255 chars, non vide après trim | `locations.name` |
| `location_type` | ENUM | ✓ | ∈ {`plant`, `dc`, `warehouse`, `supplier_virtual`, `customer_virtual`} | `locations.location_type` |
| `country` | TEXT | ✗ | ISO 3166-1 alpha-2 (DE, US, CN, ...) | `locations.country` |
| `timezone` | TEXT | ✗ | IANA TZ database (Europe/Berlin, ...) | `locations.timezone` |

## Règles DQ

| Niveau | Code | Severity | Description |
|--------|------|----------|-------------|
| L1 | `MISSING_REQUIRED` | error | colonne obligatoire vide |
| L1 | `MAX_LENGTH_EXCEEDED` | error | `name` ou `external_id` > 255 chars |
| L3 | `L3_INVALID_LOCATION_TYPE` | error | `location_type` hors liste |
| L4 | `L4_DUPLICATE_EXTERNAL_ID` | error | même `external_id` deux fois dans le batch |
| L4 | `L4_INTER_BATCH_COLLISION` | warning | `external_id` dans un autre batch open |

## Soft-delete

Les locations n'ont pas de colonne status. Une location absente du batch côté `source_system` voit son entrée dans `external_references` supprimée — la ligne canonique reste (d'autres sources peuvent encore la référencer).

## Exemple curl

```bash
curl -X POST "${OOTILS_API_URL}/v1/staging/upload" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}" \
  -F "file=@locations_sap.tsv" \
  -F "entity_type=locations" \
  -F "source_system=SAP-EU"
```
