# ADR-014 : Fusion resources/work_centers, unités capacitaires typées, SCD2 transparent

## Statut
DRAFT — 2026-05-24

## Contexte

Trois décisions architecturales convergentes ont émergé pendant le travail sur les **interfaces inbound V1**, lors d'une revue fonctionnelle du modèle métier :

1. **Deux tables capacitaires parallèles** existent aujourd'hui — `resources` (mig 009, utilisée par RCCP) et `work_centers` (mig 028, utilisée par CRP/MPS/ATP-CTP). Elles modélisent le même concept métier (une entité de production avec une capacité journalière), mais avec des colonnes différentes et **aucun lien physique** entre les deux. Un client doit saisir sa capacité **deux fois** s'il veut à la fois CRP et RCCP. Ingérable côté maître de données.

2. **Les unités capacitaires sont non typées**. `resources.capacity_unit` est un TEXT free-form (défaut `'units'`, mais accepte n'importe quoi sans validation). `work_centers` n'a pas de colonne unité du tout — le commentaire SQL dit "standard hours or units" mais rien n'est imposé. `routing_operations.setup_time` et `run_time_per_unit` sont des NUMERIC sans unité déclarée. Un client qui mélangerait minutes et heures verrait son CRP se tromper d'un facteur 60 silencieusement.

3. **Les `item_planning_params` (lead time, safety stock, lot size...) n'ont aucun endpoint d'ingestion** — la table existe en DB et supporte SCD2 (effective_from / effective_to), mais aucun chemin REST ou staging ne permet aux clients de pousser des paramètres MRP. Côté pattern, deux options coexistent dans le code des autres entités SCD2 : SCD2 strict (le client gère effective_from) vs SCD2 transparent (le client pousse l'état courant, l'API fait le rollover sous le capot).

Ce besoin émerge maintenant parce qu'on amorce le travail sur les **contrats d'interfaces inbound V1** : on ne peut pas spécifier des templates staging et des endpoints REST pour les ressources / routings / planning_params tant que ces trois questions ne sont pas tranchées.

## Décisions

### D1 — Fusion `work_centers` → `resources` (une seule table unifiée)

Une seule table `resources` portera désormais à la fois les ressources RCCP (machines / lignes / équipes / outils) et les work centers CRP. L'enum `resource_type` est étendue avec une valeur `'work_center'` pour la rétrocompatibilité, mais en pratique tout client peut utiliser n'importe laquelle des 5 valeurs (`machine`, `line`, `team`, `tool`, `work_center`) — l'engine ne discrimine pas.

```sql
CREATE TABLE resources (
    resource_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id        TEXT NOT NULL UNIQUE,
    name               TEXT NOT NULL,
    resource_type      TEXT NOT NULL CHECK (resource_type IN
                          ('machine','line','team','tool','work_center')),
    location_id        UUID REFERENCES locations(location_id),

    -- Capacité (D2 — unités typées)
    capacity_per_day   NUMERIC(18,6) NOT NULL DEFAULT 0
                       CHECK (capacity_per_day >= 0),
    capacity_unit      TEXT NOT NULL DEFAULT 'unit'
                       CHECK (capacity_unit IN ('unit','minute')),

    -- Ex-work_centers (fusionnés)
    efficiency         NUMERIC(5,4) NOT NULL DEFAULT 1.0
                       CHECK (efficiency BETWEEN 0 AND 1),
    calendar_id        UUID,  -- lien optionnel vers operational_calendars (via location_id)

    notes              TEXT,
    active             BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Le rename de la FK est cosmétique mais nécessaire pour la lisibilité :

```sql
ALTER TABLE routing_operations
    RENAME COLUMN work_center_id TO resource_id;
-- (FK constraint rebasée vers resources(resource_id))
```

La table `work_centers` est supprimée après backfill (les UUIDs sont préservés pour ne pas casser les FK existantes).

**Anti-décision** : pas de table de mapping `work_center_resources(work_center_id, resource_id)`. Ça aurait évité de toucher les FK mais ça aurait pérennisé le doublon — exactement le problème qu'on règle.

### D2 — Unités capacitaires typées : `unit` | `minute` (deux mondes, normalisation à l'ingest)

Le moteur reconnaît **deux mondes dimensionnels distincts** pour la capacité :

- **`unit`** — la capacité s'exprime en quantité produite par jour. Une opération qui consomme cette ressource déclare son temps unitaire en `unit per produced item` (typiquement 1 par défaut). Convient aux lignes monoproduit ou aux équipes de réception/expédition où "1 colis = 1 unité de capacité".
- **`minute`** — la capacité s'exprime en minutes par jour (ex: 480 min/jour = 1 shift × 8h). Une opération déclare son temps unitaire en `minute per produced item` (ex: 0.5). Convient aux ateliers usinage / process industriel.

Le client peut **aussi saisir en `hour`** côté interface — c'est l'unité commune dans les ERPs. À l'ingest, **`hour` est converti vers `minute` automatiquement (×60)**. La table interne ne connaît que `unit` et `minute`.

```
INPUT côté ingest        →   INTERNE en DB
─────────────────────       ─────────────────
capacity_unit='unit'      →   capacity_unit='unit'
capacity_unit='minute'    →   capacity_unit='minute'
capacity_unit='hour'      →   capacity_unit='minute', capacity_per_day × 60
```

**Règle de cohérence** : une opération qui référence une ressource doit déclarer son `time_unit` dans **le même monde** que `capacity_unit` de la ressource. Mismatch entre les deux mondes (`unit` vs `minute|hour`) = **erreur DQ L2 à l'ingest**, le batch est refusé. C'est explicit — pas de coercition silencieuse.

```sql
ALTER TABLE routing_operations
    ADD COLUMN time_unit TEXT NOT NULL DEFAULT 'unit'
    CHECK (time_unit IN ('unit','minute'));
-- setup_time et run_time_per_unit sont désormais dans cette unité,
-- jamais mélangées.
```

À l'ingest des routings : si le client saisit `time_unit='hour'`, la valeur est multipliée par 60 et stockée en `minute`. Si la ressource cible est en `unit` mais l'opération en `minute`, refus DQ.

**Anti-décision** : pas de table `time_unit_conversions` paramétrable par item ou par work_center. C'est tentant pour "1 produit type X = 0.3 minute en moyenne" mais ça transforme la conversion en règle métier — au mauvais endroit. Si un client a besoin de ce genre de logique, c'est dans son ETL avant l'ingest, pas dans Ootils.

### D3 — SCD2 transparent pour `item_planning_params` (option b)

Les `item_planning_params` portent déjà les colonnes `effective_from` / `effective_to` (mig 007), conçues pour SCD2 — mais aucun chemin d'ingest n'existe. Le nouveau endpoint `POST /v1/ingest/planning-params` (Phase E de l'implémentation) implémente le pattern **SCD2 transparent** :

```
Le client pousse l'état courant pour chaque (item, location) :
  {
    "item_external_id": "PUMP-01",
    "location_external_id": "DC-ATL",
    "lead_time_total_days": 14,
    "safety_stock_qty": 50,
    ...
  }

L'API à l'ingest :
  1. Lookup la ligne active : WHERE effective_to IS NULL
  2. Compare champ par champ avec ce que le client pousse :
     - Tous égaux → no-op (idempotent)
     - Au moins un champ diffère →
         UPDATE active row SET effective_to = today - 1 day
         INSERT new row WITH effective_from = today, effective_to = NULL
```

Le client **ne gère jamais les dates effective_*** — c'est invisible pour lui. Il pousse un snapshot de son état courant. L'historique se construit naturellement côté DB.

**Bénéfice** : UX simple (le client n'a pas à savoir quand ses valeurs ont changé), audit complet préservé (on peut rejouer "quelle safety stock était active il y a 3 mois ?"), idempotent par construction.

**Trade-off accepté** : un changement intra-journée écrase silencieusement (une seule ligne SCD2 par jour). Acceptable pour des paramètres de planification qui changent au mieux à la semaine.

**Anti-décision** : pas de mode "SCD2 strict" où le client passerait son propre `effective_from`. C'était l'option (a) — rejetée car la convention "le client connaît la date de bascule" ne tient pas en pratique. Les ERPs livrent rarement un horodatage fiable du changement.

Ce même pattern SCD2-transparent s'appliquera aux autres entités master-data quand elles passeront en SCD2 (item_lifecycle_status, supplier_contracts...). Le pattern devient la convention par défaut pour toute table avec `effective_from`/`effective_to`.

## Conséquences

### Migrations à produire (Phase B+E)

| Migration | Contenu | Risque |
|---|---|---|
| `034_merge_work_centers_into_resources.sql` | ALTER resources (ajoute efficiency, calendar_id, contraint capacity_unit ENUM) ; INSERT work_centers data INTO resources avec resource_type='work_center' ; ALTER routing_operations RENAME work_center_id → resource_id ; ALTER routing_operations ADD time_unit ; DROP TABLE work_centers | Moyen — backfill de FK |
| `035_planning_params_ingest.sql` | (optionnel) Index sur (item_id, location_id, effective_to) pour accélérer le lookup SCD2 actif | Faible |

### Code à modifier (Phase C+D)

- `crp/engine.py` : 4-5 queries SQL `FROM work_centers` → `FROM resources WHERE resource_type IN ('work_center','machine','line')` (ou tout simplement plus de filtre, à arbitrer)
- `engine/mrp/llc_calculator.py` : utilisateur de `routing_operations` — la column rename est gérée par PG, mais les modèles Python `Routing` / `Operation` doivent suivre
- `crp/models.py` : remplacer `WorkCenter` dataclass par `Resource` (avec champs efficiency/calendar_id ajoutés)
- `seed/network/*.py`, `scripts/seed_demo_data.py`, `demo/phase1.py` : INSERT work_centers → INSERT resources
- Tests d'intégration (test_crp_engine_integration, test_phase1_e2e, test_router_rccp_integration) : adapter les seeds inline

### Endpoints à créer (Phase E+F+G)

| Endpoint | Méthode | Fonction |
|---|---|---|
| `POST /v1/ingest/resources` | (existe déjà) | Enrichi pour accepter efficiency, calendar_id, capacity_unit ENUM |
| `POST /v1/ingest/routings` | nouveau | Routings + routing_operations (payload header + array operations) |
| `POST /v1/ingest/planning-params` | nouveau | SCD2 transparent (D3) |

Templates staging correspondants à publier dans `docs/staging-templates/` : `resources.md`, `routings.md`, `planning_params.md`.

### Effets de bord à surveiller

- **CTP / ATP-CTP** (`atp/ctp.py`) consomme aussi `routings` indirectement via la propagation. À tester après la migration.
- **MPS capacity_engine** (`mps/capacity_engine.py`) — idem, à valider sur le test d'intégration Phase 1.
- **Le seed démo doit faire les deux** : pousser des `resources` avec resource_type='work_center' ET des routings qui les référencent — sinon CRP/RCCP n'ont rien à manger.
- **Les xfailed bugs ouverts** (#255 `_persist_adjustment`, #257 `trg_shortages_updated_at`) sont indépendants de cette ADR et restent à traiter séparément.

### Audit / rétrocompatibilité

Toute base existante avec des `work_centers` peuplés doit passer par la migration 034. La migration est :
- **idempotente** au sens "réjouable" — si elle a déjà tourné, les DROP IF EXISTS / INSERT ... ON CONFLICT DO NOTHING garantissent qu'aucun rerun n'écrase la donnée
- **destructive de schéma** (DROP TABLE work_centers) — un header explicite dans la migration documente le carve-out, comme pour `003_sprint2_schema.sql` (ADR review pattern documenté).

Les UUIDs des work_centers sont préservés tels quels dans `resources.resource_id`. Toutes les FK qui pointaient vers `work_centers.work_center_id` continuent de fonctionner après le rename.

## Ouvertures (out of scope cette ADR)

- **Routings alternatifs** (multi-sequence par item) : le schéma `routings.sequence UNIQUE per (item, sequence)` est déjà compatible. La logique de sélection runtime (CRP qui choisit dynamiquement parmi N routings actifs) est différée V2.
- **`calendar_id` sur resources** : la FK est en place mais l'engine CRP ne lit pas encore le calendar. Activer ce lookup est un follow-up, ouvert dès que cette ADR est en main.
- **Fusion `operational_calendars` ↔ `work_center_calendar_edges`** : laissée pour plus tard. Le second est non-utilisé après cette ADR (work_centers disparaît).
