# Template `calendars` — calendriers opérationnels par site

**Endpoint** : `POST /v1/calendars/import` (chemin dédié, pas sous `/v1/ingest/`).
**Refresh model** : upsert par `(location_id, calendar_date)`. Un appel peut couvrir plusieurs jours d'un même site (1 appel = 1 site, N entrées).

**Target canonique** : table `operational_calendars` (mig 007).

## Pourquoi des calendriers ?

Les capacités quotidiennes des ressources sont modulées par le calendrier du site qui les héberge :
- `is_working_day=false` → 0 jour ouvré (ferié, fermeture)
- `capacity_factor=0.5` → demi-capacité (ex: dimanche réduit)
- `shift_count` → indication du nombre d'équipes (0..3)

Le module **RCCP** lit ces données pour calculer `capacity_per_bucket = capacity_per_day × nb_jours_ouvrés_du_bucket × capacity_factor_moyen`. Le module **CRP** ne lit pas encore le calendrier (follow-up ADR-014 §Ouvertures).

## Payload — header (1 location)

| Champ | Type | Obligatoire | Contraintes |
|---|---|:---:|---|
| `location_external_id` | TEXT | ✓ | FK → locations.external_id |
| `entries[]` | ARRAY | ✓ | Liste d'entrées calendrier |
| `dry_run` | BOOLEAN | ✗ | Défaut `false` |

## Payload — entrées (`entries[i]`)

| Champ | Type | Obligatoire | Contraintes |
|---|---|:---:|---|
| `calendar_date` | DATE | ✓ | ISO 8601 (`YYYY-MM-DD`) |
| `is_working_day` | BOOLEAN | ✗ | Défaut `false` |
| `shift_count` | INT | ✗ | ∈ [0, 3] |
| `capacity_factor` | NUMERIC | ✗ | ∈ [0.0, 2.0]. `1.0` = capacité normale. Au-dessus de 1 = sur-capacité (heures sup, 2x équipes). |
| `notes` | TEXT | ✗ | Libellé libre (ex: `'Noël'`, `'Pont du 14 juillet'`, `'Maintenance planifiée'`). |

## Sémantique

Pour chaque `(location_id, calendar_date)`, **un INSERT ON CONFLICT DO UPDATE** est exécuté. Pas de versioning historique : tu écrases la valeur précédente pour ce jour-là.

Si une date n'est pas dans le payload, son entrée précédente reste inchangée. Le calendrier construit incrémentalement, pas full-reload.

Pour effacer une entrée existante (ramener au comportement fallback "5/7 ouvré"), il n'y a pas d'endpoint dédié — soft delete via `is_working_day=false` + `capacity_factor=0`, ou DELETE SQL direct (administrateur).

## Fallback Mon-Fri

Si aucune entrée n'existe dans `operational_calendars` pour une date donnée à une location, le calcul RCCP retombe sur la convention **Mon-Fri ouvrés, Sam-Dim non**.

Conséquence pratique : tu n'es **pas obligé** de pousser un calendrier exhaustif. Pousse uniquement les exceptions (fériés, fermetures planifiées, journées intensives).

## Réponse type

```json
{
  "status": "completed",
  "summary": {
    "total": 5,
    "inserted": 3,
    "updated": 2,
    "errors": 0
  },
  "results": [
    {"calendar_date": "2026-12-25", "action": "inserted"},
    {"calendar_date": "2026-12-26", "action": "inserted"},
    ...
  ]
}
```

## Erreurs typiques

| HTTP | Cause |
|---|---|
| 422 | `location_external_id` inconnu ou vide |
| 422 | `capacity_factor` hors `[0, 2]` |
| 422 | `shift_count` hors `[0, 3]` |
| 422 | `calendar_date` mal formaté (non ISO 8601) |

## Exemple curl

Pousser le calendrier de fin d'année 2026 pour DC-ATL :

```bash
curl -X POST "${OOTILS_API_URL}/v1/calendars/import" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "location_external_id": "DC-ATL",
    "entries": [
      {
        "calendar_date": "2026-12-24",
        "is_working_day": true,
        "shift_count": 1,
        "capacity_factor": 0.5,
        "notes": "Veille de Noël — 1 équipe matin uniquement"
      },
      {
        "calendar_date": "2026-12-25",
        "is_working_day": false,
        "capacity_factor": 0.0,
        "notes": "Noël"
      },
      {
        "calendar_date": "2026-12-26",
        "is_working_day": false,
        "capacity_factor": 0.0,
        "notes": "Lendemain de Noël (US Bank holiday)"
      },
      {
        "calendar_date": "2026-12-31",
        "is_working_day": true,
        "shift_count": 1,
        "capacity_factor": 0.5,
        "notes": "Saint-Sylvestre — demi-journée"
      },
      {
        "calendar_date": "2027-01-01",
        "is_working_day": false,
        "capacity_factor": 0.0,
        "notes": "Jour de l'an"
      }
    ],
    "dry_run": false
  }'
```

## Convention recommandée

Pour un site donné, pousser au minimum les fériés nationaux de l'année à venir. Pour les fermetures industrielles (été en France, semaines de Noël en Allemagne), créer des plages spécifiques. Ne pas pousser tous les week-ends si le fallback Mon-Fri suffit.

## Voir aussi

- [resources.md](resources.md) — comment les ressources sont liées à une location (et donc au calendrier de cette location)
- [locations.md](locations.md) — les locations dont on parle ici
- ADR-014 §Ouvertures — le lookup calendar côté CRP engine est un follow-up (RCCP l'utilise déjà)
