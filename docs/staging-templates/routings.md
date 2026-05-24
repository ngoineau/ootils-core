# Template `routings` — gammes de fabrication

**Endpoint** : `POST /v1/ingest/routings`
**Refresh model** : **full-reload par `(item, sequence)`**. Une routing par item en V1 (sequence=1 par défaut). Chaque routing porte N opérations. Si une routing active existe déjà pour `(item, sequence)`, elle est intégralement remplacée (CASCADE delete des opérations).

**Target canoniques** : `routings` (header) + `routing_operations` (lignes).

## Payload — header routing

| Champ | Type | Obligatoire | Contraintes |
|---|---|:---:|---|
| `item_external_id` | TEXT | ✓ | FK → items.external_id |
| `sequence` | INT | ✗ | > 0, défaut `1`. V1 fixe `1` par convention ; multi-routing par item est V2. |
| `description` | TEXT | ✗ | Libellé libre |
| `operations[]` | ARRAY | ✓ | Au moins 1 |

## Payload — opérations (`operations[i]`)

| Champ | Type | Obligatoire | Contraintes |
|---|---|:---:|---|
| `sequence` | INT | ✓ | > 0, unique au sein du même routing |
| `resource_external_id` | TEXT | ✓ | FK → resources.external_id |
| `setup_time` | NUMERIC | ✗ | ≥ 0, défaut 0 |
| `run_time_per_unit` | NUMERIC | ✗ | ≥ 0, défaut 0 |
| `time_unit` | ENUM | ✗ | `unit` \| `minute` \| `hour` — défaut `unit`. `hour` est converti en `minute` à l'ingest (×60). |
| `description` | TEXT | ✗ | — |

## ⚠️ Cohérence des unités (ADR-014 D2)

Le `time_unit` de chaque opération, **après normalisation `hour→minute`**, doit matcher exactement le `capacity_unit` de la ressource cible. Sinon = erreur 422.

| Resource.capacity_unit | Op.time_unit autorisés | Conversion |
|---|---|---|
| `unit` | `unit` | aucune |
| `minute` | `minute`, `hour` | hour→minute (×60) à l'ingest |
| `unit` | `minute`, `hour` → **REFUSÉ** (mismatch) | — |
| `minute` | `unit` → **REFUSÉ** (mismatch) | — |

Le mismatch `unit ↔ minute` est volontairement refusé : un client qui mélange les deux mondes a un bug d'intégration silencieux dans le moteur capacité.

## Réponse type

```json
{
  "status": "completed",
  "summary": {
    "total": 1,
    "inserted": 1,
    "updated": 0,
    "errors": 0
  },
  "results": [
    {
      "item_external_id": "PUMP-01",
      "sequence": 1,
      "routing_id": "uuid-...",
      "operations_count": 2,
      "action": "created" | "replaced"
    }
  ],
  "batch_id": "uuid"
}
```

## Erreurs typiques

| HTTP | Cause |
|---|---|
| 422 | `item_external_id` inconnu |
| 422 | `operations[i].resource_external_id` inconnu |
| 422 | `operations[i].time_unit` ne matche pas la `capacity_unit` de la ressource (ADR-014 D2) |
| 422 | Deux opérations avec le même `sequence` dans le même routing |
| 422 | `setup_time` ou `run_time_per_unit` négatif |

## Exemple curl

Routing en heures (normalisé en minutes à l'ingest) :

```bash
curl -X POST "${OOTILS_API_URL}/v1/ingest/routings" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "routings": [
      {
        "item_external_id": "PUMP-01",
        "sequence": 1,
        "description": "Standard PUMP-01 assembly",
        "operations": [
          {
            "sequence": 1,
            "resource_external_id": "LINE-ATL-01",
            "setup_time": 0.5,
            "run_time_per_unit": 0.05,
            "time_unit": "hour",
            "description": "Pre-assembly"
          },
          {
            "sequence": 2,
            "resource_external_id": "LINE-ATL-01",
            "setup_time": 0.25,
            "run_time_per_unit": 0.02,
            "time_unit": "hour",
            "description": "Final assembly"
          }
        ]
      }
    ],
    "dry_run": false
  }'
```

Après ingest, en DB :
- `routing_operations.setup_time = 30` (0.5h × 60), `time_unit = 'minute'`
- `routing_operations.run_time_per_unit = 3` (0.05h × 60), `time_unit = 'minute'`

## Comportement full-reload

À chaque ingestion d'une routing pour un `(item, sequence)` existant :
1. La routing active est **supprimée** (`DELETE FROM routings WHERE routing_id = ...`)
2. Les `routing_operations` associées sont supprimées par CASCADE
3. Une nouvelle routing est insérée avec un nouveau `routing_id` (les UUIDs ne sont pas préservés)
4. Les nouvelles opérations sont insérées

Conséquence : **toute référence externe au `routing_id` est invalidée après un re-push**. Si tu as besoin de tracker une routing à travers les versions, garde le `(item_external_id, sequence)` comme clé stable.

## Dry run

Pose `"dry_run": true` pour valider la structure + cohérence FK + cohérence d'unités sans rien écrire.
