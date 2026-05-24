# Template `bom` — nomenclatures (Bill of Materials)

**Endpoint** : `POST /v1/ingest/bom` (JSON, pas de staging upload).
**Refresh model** : 1 appel = 1 BOM logique (= 1 article parent avec ses N composants).

**Target canoniques** : `bom_headers` (en-tête par parent) + `bom_lines` (composants).

## Sémantique d'appel

Contrairement aux autres entités batch-oriented, **chaque appel concerne UN SEUL parent**. Si tu as 100 BOMs à pousser, fais 100 appels (ou un wrapper côté client).

Comportement à l'ingest :
- Si une BOM active existe déjà pour `(parent_external_id, bom_version)` → toutes ses lignes sont remplacées intégralement.
- Sinon → nouvelle BOM créée.
- LLC (Low-Level Code) est **recalculé automatiquement** sur l'ensemble du graphe BOM après chaque ingest.

## Payload — header

| Champ | Type | Obligatoire | Contraintes |
|---|---|:---:|---|
| `parent_external_id` | TEXT | ✓ | FK → items.external_id (l'article fabriqué) |
| `bom_version` | TEXT | ✗ | Défaut `'1.0'`. Permet de coexister plusieurs versions d'une même BOM. |
| `effective_from` | DATE | ✗ | ISO 8601, défaut `today` |
| `components[]` | ARRAY | ✓ | Au moins 1 |

## Payload — composants (`components[i]`)

| Champ | Type | Obligatoire | Contraintes |
|---|---|:---:|---|
| `component_external_id` | TEXT | ✓ | FK → items.external_id |
| `quantity_per` | NUMERIC | ✓ | > 0. Quantité de ce composant pour 1 unité du parent. |
| `uom` | TEXT | ✗ | Défaut `'EA'`. Code UOM du composant. |
| `scrap_factor` | NUMERIC | ✗ | ∈ [0, 1), défaut 0.0. Taux de perte (0.05 = 5% scrap). |

## Réponse type

```json
{
  "status": "completed",
  "bom_id": "uuid-...",
  "parent_item_id": "uuid-...",
  "components_imported": 3,
  "llc_updated": 12
}
```

`llc_updated` = nombre de `bom_lines` dont le LLC a été recalculé suite à l'ingest (cascade sur le graphe BOM entier).

## Erreurs typiques

| HTTP | Cause |
|---|---|
| 422 | `parent_external_id` inconnu |
| 422 | `components[i].component_external_id` inconnu |
| 422 | `quantity_per <= 0` |
| 422 | `scrap_factor` hors `[0, 1)` |
| 422 | Cycle détecté dans le graphe BOM (un composant remonte vers son parent direct ou indirect) |

## Exemple curl

```bash
curl -X POST "${OOTILS_API_URL}/v1/ingest/bom" \
  -H "Authorization: Bearer ${OOTILS_API_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "parent_external_id": "PUMP-01",
    "bom_version": "1.0",
    "effective_from": "2026-01-01",
    "components": [
      {
        "component_external_id": "MOTOR-03",
        "quantity_per": 1,
        "uom": "EA",
        "scrap_factor": 0.02
      },
      {
        "component_external_id": "VALVE-02",
        "quantity_per": 2,
        "uom": "EA"
      },
      {
        "component_external_id": "GASKET-RBR",
        "quantity_per": 4,
        "uom": "EA",
        "scrap_factor": 0.05
      }
    ],
    "dry_run": false
  }'
```

## Multi-niveau

Une BOM n'est qu'un niveau parent → enfants directs. Pour modéliser une nomenclature multi-niveau (ex: `PUMP → SUB-ASSEMBLY → MOTOR → BEARING`), il faut **un appel par parent** :

```
POST /v1/ingest/bom { parent: PUMP, components: [SUB-ASSEMBLY, ...] }
POST /v1/ingest/bom { parent: SUB-ASSEMBLY, components: [MOTOR, ...] }
POST /v1/ingest/bom { parent: MOTOR, components: [BEARING, ...] }
```

Le LLC est recalculé à chaque ingest et reflète automatiquement la profondeur de chaque composant.

## Versioning des BOMs

`bom_version` permet de coexister plusieurs versions d'une même BOM (ex: `'1.0'`, `'1.1'`, `'2.0'`). À tout moment, **une seule version est active** par parent (les autres ont `status='inactive'`). Pour activer une version différente, c'est un endpoint séparé (hors scope de l'ingest).

## Dry run

Pose `"dry_run": true` pour valider les FKs + détection de cycle sans rien écrire ni recalculer le LLC.

## Voir aussi

- [items.md](items.md) — référentiel des articles (parent + composants)
- `GET /v1/bom/{external_id}` — récupère la BOM active d'un article
- `POST /v1/bom/explode` — explosion MRP (gross/net requirements)
