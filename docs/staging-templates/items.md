# Template `items` — master article

**Refresh model** : full reload par `(entity_type='items', source_system)`.
Un fichier remplace intégralement le périmètre items du source_system.

**Target canonique** : table `items` (+ écriture audit dans `master_data_audit_log`).

## Colonnes

| Colonne | Type | Obligatoire | Contraintes | Mappe vers |
|---------|------|-------------|-------------|------------|
| `external_id` | TEXT | ✓ | unique par source_system, max 100 chars | `items.external_id` |
| `name` | TEXT | ✓ | max 200 chars, non vide après trim | `items.name` |
| `item_type` | ENUM | ✓ | ∈ {`finished_good`, `semi_finished`, `component`, `raw_material`} | `items.item_type` |
| `uom` | TEXT | ✓ | code UOM connu (∈ `uom_conversions.from_uom` ∪ `uom_conversions.to_uom` ∪ liste blanche `EA`, `KG`, `L`, `M`) | `items.uom` |
| `status` | ENUM | ✗ | ∈ {`active`, `phase_out`, `obsolete`}, défaut `active` | `items.status` |

## Règles DQ activées

| Niveau | Code | Type | Description |
|--------|------|------|-------------|
| L1 | `MISSING_REQUIRED` | error | une colonne obligatoire est vide |
| L1 | `INVALID_ENUM_VALUE` | error | `item_type` ou `status` hors liste |
| L1 | `MAX_LENGTH_EXCEEDED` | error | `name` > 200 ou `external_id` > 100 |
| L1 | `UNKNOWN_COLUMN` | warning | colonne en plus dans le fichier, ignorée |
| L2 | `UOM_UNKNOWN` | error | `uom` non référencé dans `uom_conversions` ou whitelist |
| L3 | `DUPLICATE_EXTERNAL_ID` | error | même `external_id` deux fois dans le batch |
| L3 | `NAME_TOO_GENERIC` | warning | `name` ∈ {`item`, `n/a`, `tbd`, single char} |
| L4 | `RECONCILE_DELETE_RATIO` | error | si > 20 % des items existants seraient soft-deleted (override possible avec `--force`) |

## Exemple

Voir [items.tsv](items.tsv) pour un fichier prêt à uploader (10 items représentatifs).

## Comportement à l'approval (D3 ADR-013)

Pour chaque ligne du batch :
- Si `external_id` n'existe pas en `items` (pour ce `source_system`) → **INSERT**
- Si `external_id` existe et que les valeurs diffèrent → **UPDATE** (champs uniquement, pas l'`item_id` UUID) + audit log
- Si l'item existe en `items` mais son `external_id` ne figure plus dans le fichier (pour ce `source_system`) → **soft delete** (`status='obsolete'` + `active=FALSE` sur les nodes liés)

Aucun `DELETE` SQL dur n'est jamais effectué — l'historique est immutable.

## Exemple curl

```bash
curl -X POST "${OOTILS_API_URL}/v1/staging/upload" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}" \
  -F "file=@items_sap_2026w22.tsv" \
  -F "entity_type=items" \
  -F "source_system=SAP-EU"
# -> 202 Accepted, returns { "batch_id": "uuid", "status": "pending" }

# Poll DQ status
curl "${OOTILS_API_URL}/v1/staging/batches/${BATCH_ID}" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}"
# Once status="validated", review diff:
curl "${OOTILS_API_URL}/v1/staging/batches/${BATCH_ID}/diff" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}"
# Approve:
curl -X POST "${OOTILS_API_URL}/v1/staging/batches/${BATCH_ID}/approve" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}" \
  -d '{"notes": "Weekly SAP master refresh, validated by ops"}'
```
