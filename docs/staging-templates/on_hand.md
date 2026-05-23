# Template `on_hand` — snapshots de stock disponible

**Refresh model** : full reload par `(entity_type='on_hand', source_system)`. Un import = un snapshot temporel ; il remplace l'état précédent du même `source_system`.

**Target canonique** : `nodes` table (node_type='OnHandSupply').

## Colonnes

| Colonne | Type | Obligatoire | Contraintes |
|---------|------|-------------|-------------|
| `item_external_id` | TEXT | ✓ | doit exister dans `items` |
| `location_external_id` | TEXT | ✓ | doit exister dans `locations` |
| `quantity` | NUMERIC | ✓ | **>= 0** (négatif refusé — stock physique) |
| `uom` | TEXT | ✓ | code UOM |
| `as_of_date` | DATE | ✓ | date du snapshot (ISO 8601) |

## Règles DQ

| Niveau | Code | Severity | Description |
|--------|------|----------|-------------|
| L1 | `MISSING_REQUIRED` | error | colonne obligatoire vide |
| L2 | `L2_UNKNOWN_REF` | error | item ou location inconnu |
| L3 | `L3_QUANTITY_NEGATIVE` | error | quantité négative (zero accepté) |
| L3 | `L3_QUANTITY_SUSPICIOUS` | warning | quantité > 10M (vérifier UOM) |
| L3 | `L3_DATE_OUT_OF_RANGE` | warning | `as_of_date` hors [2000, 2100] |

## Note importante

Le on-hand est un snapshot par construction — il n'y a pas de notion d'`external_id` au sens transactionnel. La clé naturelle est `(item, location)`. Les imports successifs depuis le même `source_system` remplacent l'état précédent.

## Exemple curl

```bash
curl -X POST "${OOTILS_API_URL}/v1/staging/upload" \
  -F "file=@daily_oh.csv" -F "entity_type=on_hand" \
  -F "source_system=WMS-DAILY-SNAPSHOT" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}"
```
