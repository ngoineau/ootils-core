# ADR-025 — Overlay scénarisé des paramètres de planification : un résolveur unique, jamais promu

**Statut :** Accepté — chantier #347. PR1 (branche `feat/param-overlay-foundation`) : fondation (table + résolveur), non branchée à aucun lecteur. PR2 (lecteurs MRP batch) mergée. PR3 (propagation/`SHORTAGES_SQL` + 5ᵉ lecteur `mrp.py` mode simple) mergée — voir section dédiée ci-dessous. PR4 (chemin agent + endpoint REST) reste à faire.
**Date :** 2026-07-03
**Contexte mesuré :** REVIEW-2026-07-APS A10 (`docs/REVIEW-2026-07-APS.md`) — master data non forkable, what-if lead time/MOQ/safety stock structurellement impossible.

---

## Contexte

`items`, `locations`, `suppliers`, `supplier_items`, `item_planning_params`, `bom_headers/lines` n'ont aucune colonne `scenario_id` (REVIEW-2026-07-APS A10). Conséquence : le what-if n°1 attendu d'un APS — « et si le lead time sourcing passait à 21 jours ? », « et si on doublait le safety stock ? » — est impossible dans un fork sans écrire dans le master data lui-même, ce qui contaminerait la baseline et toute autre branche.

Deux options ont été écartées d'emblée (voir Alternatives) : forker le master data en entier, ou rendre `scenario_id` nullable directement sur `item_planning_params`. Le choix retenu est une table d'**overlay** séparée, purement additive au-dessus de la ligne SCD2 courante, lue à travers un résolveur SQL unique.

## Décision

**Pas de fork du master data.** `item_planning_params` (SCD2, migration 007/021) reste la seule vérité de base, jamais dupliquée par scénario.

1. **Table `scenario_planning_overrides`** (migration `060_scenario_planning_overrides.sql`) : une ligne = `(scenario_id, item_id, location_id NULLABLE, field_name) → value` (TEXT, scalaire sérialisé — pas un JSONB carve-out, cf. commentaire de la migration). Contrainte naturelle `UNIQUE NULLS NOT DISTINCT (scenario_id, item_id, location_id, field_name)` (PG16) : une seule ligne active par (scénario, item, location y compris NULL, champ). FK `scenario_id → scenarios` en `ON DELETE RESTRICT` (ADR-011 : les scénarios sont soft-deleted, jamais hard-deleted) ; `item_id`/`location_id` en `ON DELETE CASCADE` (filet de sécurité, pas un chemin vivant).
2. **Résolveur SQL unique : `resolved_params_sql()`** (`src/ootils_core/engine/scenario/param_overlay.py`). Construit un fragment SQL composable — un `LEFT JOIN LATERAL` par champ whitelisté sur `scenario_planning_overrides`, `COALESCE`é contre la colonne de base — paramétré par le seul placeholder nommé `%(scenario_id)s`. **Tout lecteur de params (PR2 : loaders MRP batch ; PR3 : propagation/`SHORTAGES_SQL` ; PR4 : chemin agent + endpoint REST) devra composer sur ce fragment.** Règle de contribution : **aucun nouveau `COALESCE` divergent** écrit à la main ailleurs dans le repo — un seul résolveur, un seul endroit où la précédence est arbitrée.
3. **Sémantique de résolution :**
   - L'overlay s'applique uniquement sur la ligne SCD2 **courante** — prédicat sentinelle `(effective_to IS NULL OR effective_to = '9999-12-31'::DATE)`, l'idiome répété dans `ingest.py` / `mrp.py` / `projection.py` / `propagator.py`.
   - Précédence par champ, du plus spécifique au plus général : override exact `(item, location)` > override `(item, location = NULL)` (« item-global ») > valeur de base `item_planning_params`. Implémentée par un `ORDER BY location_id NULLS LAST LIMIT 1` dans chaque LATERAL.
   - `scenario_id = NULL` (baseline pure) : aucun override ne peut matcher — `scenario_planning_overrides.scenario_id` est `NOT NULL` — le `LEFT JOIN` dégénère systématiquement en absence de ligne, donc en comportement baseline inchangé.
4. **Whitelist V1 — 15 champs** (`ALLOWED_PARAM_FIELDS` dans `param_overlay.py`, répliquée à l'identique dans le `CHECK (field_name IN (...))` de la migration 060) : champs qui ne changent **pas** la topologie du graphe.

   | Champ | Type de cast |
   |---|---|
   | `lead_time_sourcing_days` | integer |
   | `lead_time_manufacturing_days` | integer |
   | `lead_time_transit_days` | integer |
   | `safety_stock_qty` | numeric |
   | `safety_stock_days` | numeric |
   | `min_order_qty` | numeric |
   | `max_order_qty` | numeric |
   | `order_multiple_qty` | numeric |
   | `lot_size_rule` | text (ENUM `lot_size_rule_type`, validé contre `LOT_SIZE_RULE_VALUES`) |
   | `economic_order_qty` | numeric |
   | `lot_size_poq_periods` | integer |
   | `frozen_time_fence_days` | integer |
   | `slashed_time_fence_days` | integer |
   | `forecast_consumption_strategy` | text |
   | `consumption_window_days` | integer |

   Exclus V1, deux raisons distinctes : `is_make`, `preferred_supplier_id`, `reorder_point_qty` changeraient la **topologie**/LLC du graphe (hors périmètre d'un overlay purement paramétrique) ; BOM (`bom_headers/lines`) est différé ; `supplier_items` est un chantier distinct (sourcing multi-fournisseur, hors #347).
5. **Promotion : hors scope, décision ferme.** L'overlay est un outil de **simulation pure**. `promote()` (`scenario/manager.py`) ne rejoue **jamais** les overrides de params sur la baseline. Un changement de master data passe exclusivement par le canal humain gouverné (staging/import, décision L4). Justification : muter `item_planning_params` (SCD2, exclusion constraint GiST, CHECK métier) depuis un mécanisme de simulation casserait l'invariant « master data = canal gouverné » — le même invariant qui a motivé le refus d'écriture watchers dans `shortages` (ADR-021, point 3).
6. **Fail-loudly à l'écriture, deux ceintures.** `set_param_override()` valide : whitelist du `field_name` côté Python (`_validate_field_name`) **et** côté SQL (`CHECK` de la migration) ; castabilité stricte de `value` contre le type cible (`_validate_value` — littéraux ASCII stricts, bornes int4, `Decimal` fini pour numeric avec refus de NaN/Infinity et plafond pragmatique 10^12, appartenance à `LOT_SIZE_RULE_VALUES` pour l'ENUM) ; **bornes métier miroir des CHECK de la table de base** (`PARAM_FIELD_BOUNDS`, dérivées des migrations 007/021 — un override ne peut jamais porter une valeur que `item_planning_params` lui-même refuserait, p.ex. lead time négatif ou MOQ à zéro) ; la valeur stockée est la forme **normalisée** (strippée) qui a été validée ; existence du scénario et refus explicite sur la baseline (`is_baseline=TRUE` → `ParamOverlayError`) ; **existence d'une ligne SCD2 courante ciblée** — un override orphelin (item/location sans ligne de params courante) est REFUSÉ à l'écriture avec une erreur explicite, jamais accepté puis silencieusement inerte à la résolution ; `applied_by` non vide (attribution obligatoire, colonne `NOT NULL`). Les violations FK résiduelles (item/location fantôme) sont retypées en `ParamOverlayError` (UUID seuls, pas de message psycopg brut). Audit : `applied_by`/`applied_at` sur chaque ligne (`NOT NULL`, jamais d'écriture anonyme).

## PR3 — propagation & détection de pénurie scénarisées

**Statut :** Mergée. Portée : les 3 sites de propagation qui lisaient encore le safety stock **baseline** même à l'intérieur d'un fork — bug latent qui faussait le delta contre-factuel des watchers scenario-backed (#340), puisque la simulation retombait toujours sur la valeur non-overridée quel que soit l'override posé sur le scénario.

1. **3 sites corrigés :**
   - le SQL de détection de pénurie (`SHORTAGES_SQL`, `engine/orchestration/propagator_sql.py`) ;
   - le `safety_stock_cache` de pré-chargement + le fallback `_get_safety_stock()` du propagator Python (`engine/orchestration/propagator.py`) ;
   - le **5ᵉ lecteur** `_get_planning_params()` du mode simple de `api/routers/mrp.py` (`POST /v1/mrp/run`), découvert pendant PR2 — il n'avait pas été identifié dans l'inventaire initial des lecteurs MRP batch de PR1/PR2.

   Avant PR3, ces 3 chemins résolvaient `safety_stock_qty` directement contre `item_planning_params`, sans passer par le résolveur d'overlay ni même recevoir de `scenario_id` en paramètre de requête — un fork posant un override de safety stock n'avait donc aucun effet observable sur sa propre détection de pénurie.

2. **Helper mono-champ (`resolved_field_lateral_sql`).** Plutôt que d'injecter le fragment `resolved_params_sql()` complet (15 LATERAL) dans des requêtes déjà chargées (agrégations, jointures de coût), PR3 introduit un helper qui ne résout **qu'un seul champ** — `safety_stock_qty` pour les 3 sites ci-dessus. Il émet **deux** jointures corrélées, pas une : un `LEFT JOIN LATERAL (… ORDER BY location_id NULLS LAST LIMIT 1) … ON TRUE` qui va chercher la meilleure ligne d'override, puis un `CROSS JOIN LATERAL (SELECT COALESCE(override::type, base::type) AS <out_col>)` qui matérialise la valeur résolue. Point de sûreté : la sous-requête du `CROSS JOIN LATERAL` est un `SELECT` scalaire **sans `FROM`**, qui retourne donc toujours exactement une ligne — le `CROSS JOIN` ne peut jamais éliminer la ligne hôte (le `LEFT JOIN … ON TRUE` préserve de même la ligne quand aucun override ne matche, avec `value = NULL`). Même sémantique de précédence (item+location > item-global > base), même prédicat SCD2 courant, même source d'arbitrage que `resolved_params_sql()` : la règle « aucun `COALESCE` divergent » posée en PR1 est respectée — un seul point de vérité pour la précédence, décliné en deux formes SQL (fragment multi-champs pour les loaders batch, fragment mono-champ pour les requêtes de propagation).

3. **`SHORTAGES_SQL` scénarisé reste de la persistance scopée, pas un calcul pur — décision ferme.** La table `shortages` est déjà partitionnée par scénario (`scenario_id NOT NULL`, UUID déterministe qui l'inclut, `INSERT` portant `pi.scenario_id`). PR3 ne change **pas** l'ownership posé par l'ADR-021 : `ShortageDetector`/`SHORTAGES_SQL` restent les seuls écrivains de `shortages`. Un fork qui détecte ses propres pénuries continue d'écrire ses propres lignes scénarisées, exactement comme avant — la seule correction porte sur la valeur de safety stock consommée par le calcul, pas sur qui écrit ou où. Alternative rejetée : des lignes « shortages what-if » éphémères, non persistées, calculées à la volée pour la simulation — cela casserait `/v1/issues` et la lecture des watchers pour les scénarios forkés, et diviserait la vérité de persistance en deux, l'anti-pattern exact que l'ADR-021 a fermé pour les watchers.

4. **Cache scopé au calc_run, jamais promu.** `safety_stock_cache` reste local à un `calc_run` (reconstruit à chaque propagation), donc naturellement scopé au `scenario_id` de ce calc_run. PR3 corrige la requête qui peuple le cache pour qu'elle passe par le helper mono-champ — elle ne change ni la durée de vie du cache ni son niveau (il n'est toujours pas promu au niveau instance ; le promouvoir créerait une fuite inter-scénario, un fork lisant le safety stock résolu d'un autre).

5. **Baseline byte-identique — garanti par construction, pas par un cas spécial.** Sur le scénario baseline, le résolveur dégrade en « aucun override » : `scenario_planning_overrides` ne peut structurellement porter aucune ligne pour la baseline (`set_param_override()` refuse `is_baseline=TRUE`, cf. PR1 point 6), donc chaque `LEFT JOIN LATERAL` ne matche rien et `COALESCE(NULL, base) = base`. Aucune branche de code dédiée « si baseline, ignorer l'overlay » — c'est le même chemin SQL qui, sur baseline, ne trouve jamais de ligne à joindre. Preuve attendue : un run de propagation baseline avant/après le patch produit des lignes `shortages` identiques (UUID déterministes inchangés) et le garde-fou CI `tests/integration/test_shortage_truth_consistency_integration.py` reste vert sans modification de son propre code.

## Limites assumées V1

- Pas d'émission `StreamChanges` à la pose d'un overlay — l'observabilité passe par le `calc_run` scénario-scopé qui suit (PR2/PR3), pas par un événement dédié à l'overlay lui-même.
- Pas de dimension temporelle sur l'overlay (pas de fenêtre d'effectivité) — une seule valeur active par (scénario, item, location, champ).
- Effectivité au **prochain run scénario-scopé**, pas de push temps réel : poser un override ne recalcule rien tant qu'aucun caller ne relit à travers `resolved_params_sql()`.
- PR1 a fourni la fondation seule (table + résolveur + CRUD d'override), sans lecteur branché. Depuis PR2 (loaders MRP batch) et PR3 (propagation/`SHORTAGES_SQL` + 5ᵉ lecteur `mrp.py` mode simple), un override posé a un effet observable sur un run MRP batch et sur la propagation/détection de pénurie scénarisées. Reste non branché : le chemin agent + endpoint REST (PR4) — tant que PR4 n'est pas mergée, poser/lister/effacer un override ne passe encore que par appel direct au module Python, pas par l'API.

## Alternatives rejetées

- **Fork complet du master data (`items`, `item_planning_params`, `bom_*`, ...) par scénario.** Rejeté : volumétrie (chaque fork dupliquerait l'intégralité du référentiel), et cohérence SCD2 — dupliquer une ligne SCD2 par scénario multiplie les fenêtres d'effectivité à maintenir en phase, un risque de dérive du même ordre que le double moteur MRP avant l'ADR-020.
- **`scenario_id` nullable sur `item_planning_params`.** Rejeté : pollue le master data avec une dimension qui ne concerne que la simulation ; toute requête baseline (la grande majorité du trafic) devrait filtrer `scenario_id IS NULL` partout où elle ne le fait pas aujourd'hui — ralentissement systémique pour un besoin qui ne concerne qu'une minorité de lectures scénarisées.

## Conséquences

- **Positif :** le what-if lead time/MOQ/safety stock devient possible dans un fork sans toucher la baseline ni les scénarios frères ; un seul point de résolution (`resolved_params_sql()`) empêche la dérive de précédence que deux `COALESCE` écrits indépendamment produiraient tôt ou tard.
- **Négatif / dette :** le chemin agent + endpoint REST (PR4) n'est pas branché — poser/lister/effacer un override ne passe encore que par appel direct au module Python (pas d'exposition API), donc pas encore accessible aux watchers scenario-backed (#340) ni à un opérateur humain via l'API.
- **Reste à faire :** PR4 (chemin agent + endpoint REST pour poser/lister/effacer des overrides depuis l'API). PR2 (loaders MRP batch) et PR3 (propagation / `SHORTAGES_SQL` + 5ᵉ lecteur `mrp.py`) sont mergées — voir section dédiée PR3 ci-dessus.

## Code references

- `src/ootils_core/engine/scenario/param_overlay.py` — `resolved_params_sql()` (résolveur multi-champs) et son pendant mono-champ (PR3, propagation), `ALLOWED_PARAM_FIELDS` + `PARAM_FIELD_BOUNDS` (whitelist V1, 15 champs, bornes métier), `set_param_override()` / `clear_param_override()` / `list_param_overrides()`.
- `src/ootils_core/db/migrations/060_scenario_planning_overrides.sql` — table, contrainte, FK policy.
- `src/ootils_core/engine/orchestration/propagator_sql.py` — `SHORTAGES_SQL`, scénarisé en PR3.
- `src/ootils_core/engine/orchestration/propagator.py` — `safety_stock_cache` et `_get_safety_stock()`, scénarisés en PR3.
- `src/ootils_core/api/routers/mrp.py` — `_get_planning_params()`, le 5ᵉ lecteur (mode simple `POST /v1/mrp/run`), scénarisé en PR3.
- `tests/test_param_overlay.py` — couverture pure-Python (whitelist, garde d'alias, surface d'injection).
- `tests/integration/test_param_overlay_integration.py` — isolation par fork (C0), rétention FK.
- `docs/REVIEW-2026-07-APS.md` — item A10, source du chantier.
- `docs/ADR-011-scenario-retention.md` — politique FK `RESTRICT` sur `scenarios`.
- `docs/ADR-021-shortage-truth.md` — ownership `shortages` (ShortageDetector/`SHORTAGES_SQL` seuls écrivains) ; PR3 s'appuie sur cet arbitrage pour ne pas dupliquer la persistance des pénuries scénarisées.
