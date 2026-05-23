# Template `forecasts` — prévisions de demande

**Refresh model** : full reload par `(entity_type='forecasts', source_system)`.
**Target canonique** : `nodes` table (node_type='ForecastDemand').

Note : la pipeline `/v1/ingest/forecast-demand` historique acceptait `bucket_date` et `time_grain`. Le template ici suit cette convention. Pour des prévisions à grain mensuel avec un span, voir `forecast_demand.md` (version legacy avec time_span_start/end).

## Colonnes

| Colonne | Type | Obligatoire | Contraintes |
|---------|------|-------------|-------------|
| `item_external_id` | TEXT | ✓ | doit exister dans `items` (typiquement FG) |
| `location_external_id` | TEXT | ✓ | typiquement un DC |
| `quantity` | NUMERIC | ✓ | >= 0 (0 = ramp-down EOL legitime) |
| `bucket_date` | DATE | ✓ | date du bucket (début ou point-in-time selon grain) |
| `period_end` | DATE | ✗ | utilisé pour les forecasts mensuels (range) |
| `time_grain` | TEXT | ✗ | `day` / `week` / `month` (défaut: `day`) |

## Règles DQ

| Niveau | Code | Severity |
|--------|------|----------|
| L1 | `MISSING_REQUIRED` | error |
| L2 | `L2_UNKNOWN_REF` | error |
| L3 | `L3_QUANTITY_NEGATIVE` | error (0 toléré) |
| L3 | `L3_DATE_OUT_OF_RANGE` | warning |
| L3 | `L3_DATE_RANGE_INVERTED` | error (si `period_end < bucket_date`) |

## Exemple curl

```bash
curl -X POST "${OOTILS_API_URL}/v1/staging/upload" \
  -F "file=@monthly_forecasts.csv" -F "entity_type=forecasts" \
  -F "source_system=DEMAND-PLANNING" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}"
```
