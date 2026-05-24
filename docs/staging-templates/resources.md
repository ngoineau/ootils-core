# Template `resources` — ressources capacitaires (RCCP + CRP unifié)

**Endpoint** : `POST /v1/ingest/resources` (JSON, pas de staging upload pour cette entité).
**Refresh model** : upsert par `external_id`. Un re-push avec le même `external_id` met à jour les attributs en place — pas de versioning SCD2.

**Target canonique** : table `resources` (mig 009 + 034 fusionnée — voir [ADR-014 D1](../ADR-014-resources-units-scd2.md)).

## Contexte ADR-014

Cette table porte à la fois les **ressources RCCP** (machines, lignes, équipes, outils — vue agrégée) **et les work_centers CRP** (ordonnancement fin par opération). Le moteur ne discrimine pas par `resource_type` ; les deux modules CRP et RCCP lisent la même table.

## Champs

| Champ | Type | Obligatoire | Contraintes |
|---|---|:---:|---|
| `external_id` | TEXT | ✓ | Unique. Clé d'upsert. |
| `name` | TEXT | ✓ | Non vide après trim |
| `resource_type` | ENUM | ✓ | ∈ {`machine`, `line`, `team`, `tool`, `work_center`} |
| `location_external_id` | TEXT | ✗ | FK → locations.external_id. Optionnel (utile pour le lookup calendrier). |
| `capacity_per_day` | NUMERIC | ✗ | > 0, défaut 1.0. Voir unités ci-dessous. |
| `capacity_unit` | ENUM | ✗ | ∈ {`unit`, `minute`}, défaut `unit`. Voir ADR-014 D2. |
| `efficiency` | NUMERIC | ✗ | ∈ [0, 1], défaut 1.0. Facteur multiplicatif appliqué par le CRP. |
| `notes` | TEXT | ✗ | Libellé libre |

## ⚠️ Convention d'unités (ADR-014 D2)

Le `capacity_unit` détermine le **monde dimensionnel** de la ressource. Deux mondes sont supportés en base :

- `unit` — la capacité s'exprime en quantité produite par jour (ex: 100 unités/jour). Les opérations qui consomment cette ressource déclarent leur `run_time_per_unit` en `unit per produced item` (typiquement 1).
- `minute` — la capacité s'exprime en minutes par jour (ex: 480 = 8h × 60). Les opérations déclarent leur temps en minutes par unité produite.

Une opération avec `time_unit='minute'` ne peut **pas** consommer une ressource en `capacity_unit='unit'` (et inversement). Mismatch = erreur DQ L2.

Note : à l'ingest de routings, `time_unit='hour'` est aussi accepté mais converti automatiquement en `minute` (×60). Pour les ressources, **pas de conversion à l'ingest** : tu déclares directement en `unit` ou `minute`.

## Effet collatéral

Lors de l'upsert, un nœud `Resource` correspondant est aussi créé/mis à jour dans la table `nodes` du graphe (pour permettre aux edges `consumes_resource` de pointer vers une entité graphe).

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
      "external_id": "LINE-ATL-01",
      "resource_id": "uuid-...",
      "action": "inserted" | "updated"
    }
  ],
  "batch_id": "uuid"
}
```

## Exemple curl

```bash
curl -X POST "${OOTILS_API_URL}/v1/ingest/resources" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "resources": [
      {
        "external_id": "LINE-ATL-01",
        "name": "Atlanta Assembly Line 1",
        "resource_type": "line",
        "location_external_id": "DC-ATL",
        "capacity_per_day": 480,
        "capacity_unit": "minute",
        "efficiency": 0.92,
        "notes": "Shift 1 only"
      },
      {
        "external_id": "WC-PUMP-MAIN",
        "name": "Pump Assembly Work Center",
        "resource_type": "work_center",
        "location_external_id": "DC-ATL",
        "capacity_per_day": 100,
        "capacity_unit": "unit"
      }
    ],
    "dry_run": false
  }'
```

## Erreurs typiques

| HTTP | Cause |
|---|---|
| 422 | `external_id` ou `name` vide |
| 422 | `resource_type` hors enum |
| 422 | `location_external_id` poussé mais inconnu |
| 422 | `capacity_unit` hors `{unit, minute}` |
| 422 | `capacity_per_day <= 0` ou `efficiency` hors `[0, 1]` |

## Voir aussi

- [routings.md](routings.md) — comment les opérations consomment cette ressource (avec contrainte d'unité)
- [ADR-014](../ADR-014-resources-units-scd2.md) — décisions architecturales (fusion + unités typées)
