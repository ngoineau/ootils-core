# ADR-025 — Overlay scénarisé des paramètres de planification : un résolveur unique, jamais promu

**Statut :** Accepté — chantier #347 **CLÔTURÉ**, 4 PRs mergées. PR1 (`feat/param-overlay-foundation`) : fondation (table + résolveur), non branchée à aucun lecteur. PR2 (lecteurs MRP batch) mergée. PR3 (propagation/`SHORTAGES_SQL` + 5ᵉ lecteur `mrp.py` mode simple) mergée — voir section dédiée ci-dessous. PR4 (chemin agent + endpoint REST) mergée — voir section dédiée ci-dessous. Les 15 champs whitelistés (`ALLOWED_PARAM_FIELDS`) sont désormais forkables de bout en bout : loaders MRP batch → propagation/détection de pénurie → chemin agent (watcher scenario-backed) → endpoint REST.
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

## PR4 — chemin agent & endpoint REST

**Statut :** Mergée. Portée : brancher le dernier axe restant — poser/lister/effacer un override depuis un agent (contre-factuel scenario-backed) et depuis l'API REST — sans introduire un second point de résolution ni une deuxième forme de promotion.

1. **`simulate_param_overrides` — frère de `simulate_overrides`, cœur partagé.** `src/ootils_core/tools/agent_tools.py` ajoute `simulate_param_overrides`, qui pose N overrides de planning-param via `set_param_override()` (au lieu de `ScenarioManager.apply_override()` pour les node overrides), puis délègue le fork→propagate→delta au même helper interne `_fork_propagate_delta` que `simulate_overrides` — ce helper n'a aucune connaissance du type d'override appliqué en amont. Même contrat : une connexion dict_row dédiée, la fonction possède sa propre transaction et commit deux fois (fork+overrides d'abord, pour survivre à un recompute qui échoue ; fin de propagation ensuite), un override rejeté (`ParamOverlayError` — whitelist, valeur illégale, cible orpheline, scénario baseline) est consigné dans `failed_overrides` avec message hand-authored (UUID/field-only, même carve-out que `api/routers/staging.py`), jamais fatal et jamais fabriqué en delta silencieux.
2. **Harness `simulate_param_run`** (`scripts/agent_simulation.py`) — sibling de `simulate_run`, même forme de retour (`summary`, `results`), même cycle de vie fork→propagate→archive (TTL, jamais DELETE, jamais promue). Un candidat peut porter **un seul** override ou une **liste** d'overrides appliqués ensemble dans la même contribution au fork (cas `SET_LOT_RULE:POQ`, qui a besoin à la fois de `lot_size_rule` et `lot_size_poq_periods` pour être signifiant) ; le delta par-item reste attribué comme **un seul** candidat même quand plusieurs overrides le composent. Aucun fork n'est créé si aucun candidat n'est simulable (`scenario_id` reste `None`).
3. **`lot_policy_watcher` passe de baseline-only à scenario-backed.** `scripts/agent_lot_policy_watcher.py` forke désormais un what-if (`what-if-lot_policy_watcher-<ts>`) par run via `simulate_param_run`, sur le sous-ensemble mappable de ses 3 `change_type` :

   | `change_type` | Champ overlay ciblé |
   |---|---|
   | `SET_LOT_RULE` | `lot_size_rule` (+ `lot_size_poq_periods` si la règle proposée est POQ) |
   | `RENEGOTIATE_MOQ` | `min_order_qty` |
   | `REVIEW_MULTIPLE` | `order_multiple_qty` |

   Les 3 `change_type` que ce watcher émet sont **tous** dans la whitelist V1 (§ Décision, point 4) — donc l'intégralité de son périmètre d'actions est simulable, contrairement à shortage_watcher/material_watcher où seul EXPEDITE-sur-réception-ferme-future l'est. Une propagation de fork qui échoue démote la reco simulée à `NEEDS_DATA_REVIEW` sans delta fabriqué (`agent_simulation.effective_confidence`, même contrat fail-loudly que #340). Ce watcher reste **L1 DRAFT strict** : il n'écrit que dans `parameter_recommendations`, jamais dans `shortages` ni dans l'ERP — la simulation ne change ni son autorité d'écriture ni son niveau de gouvernance (`scripts/agent_governance.py:decision_level`).
4. **Endpoint REST `POST/GET/DELETE /v1/scenarios/{id}/param-overrides`** (`src/ootils_core/api/routers/param_overrides.py`, branché dans `api/app.py`) — wrapper HTTP fin sans logique métier propre : `POST` upsert un override scoped-scénario (201, corps `ParamOverrideIn`), `GET` liste (200, liste vide légitime sur baseline ou scénario sans override), `DELETE .../param-overrides/{field_name}` efface (204, idempotent — override absent = no-op, pas une erreur). Kill-switch `OOTILS_PARAM_OVERLAY_ENABLED` (défaut ON) vérifié comme dépendance FastAPI **avant** `Depends(get_db)` — un overlay désactivé répond 503 sans toucher au pool DB. `ParamOverlayError` → 422 avec `detail=str(e)`, même carve-out que `DiffError`/`ApprovalError`/`RejectionError` (`staging.py`) : message hand-authored (UUID/field/enum seuls), aucune fuite DSN/psycopg. **L0, aucune porte d'approbation** — poser un override est de la simulation pure, jamais promue sur baseline ; la porte humaine qui compte reste `POST /v1/scenarios/{id}/promote`, qui ne rejoue jamais les overrides de param (décision ferme, § Décision point 5 — pas un TODO).
5. **eando/dq restent hors périmètre, par nature — pas de la dette #347.** Leurs actions sont des changements de **disposition** (non paramétriques), non mappables sur un champ de la whitelist overlay ni sur un node override existant. Ce n'est pas un oubli de PR4 ni un reste-à-faire du chantier : les débloquer nécessiterait un mécanisme de simulation non paramétrique distinct — un autre chantier, hors #347.

## Limites assumées V1

- Pas d'émission `StreamChanges` à la pose d'un overlay — l'observabilité passe par le `calc_run` scénario-scopé qui suit (PR2/PR3), pas par un événement dédié à l'overlay lui-même.
- Pas de dimension temporelle sur l'overlay (pas de fenêtre d'effectivité) — une seule valeur active par (scénario, item, location, champ).
- Effectivité au **prochain run scénario-scopé**, pas de push temps réel : poser un override ne recalcule rien tant qu'aucun caller ne relit à travers `resolved_params_sql()`.
- Chantier #347 complet (PR1→PR4) : la fondation (PR1), les loaders MRP batch (PR2), la propagation/détection de pénurie (PR3) et le chemin agent + endpoint REST (PR4) sont tous mergés et branchés sur le même résolveur unique. eando/dq restent hors périmètre par nature (actions de disposition non paramétriques, cf. section PR4 point 5) — limite assumée, pas une PR différée.

## Alternatives rejetées

- **Fork complet du master data (`items`, `item_planning_params`, `bom_*`, ...) par scénario.** Rejeté : volumétrie (chaque fork dupliquerait l'intégralité du référentiel), et cohérence SCD2 — dupliquer une ligne SCD2 par scénario multiplie les fenêtres d'effectivité à maintenir en phase, un risque de dérive du même ordre que le double moteur MRP avant l'ADR-020.
- **`scenario_id` nullable sur `item_planning_params`.** Rejeté : pollue le master data avec une dimension qui ne concerne que la simulation ; toute requête baseline (la grande majorité du trafic) devrait filtrer `scenario_id IS NULL` partout où elle ne le fait pas aujourd'hui — ralentissement systémique pour un besoin qui ne concerne qu'une minorité de lectures scénarisées.

## Conséquences

- **Positif :** le what-if lead time/MOQ/safety stock devient possible dans un fork sans toucher la baseline ni les scénarios frères ; un seul point de résolution (`resolved_params_sql()`) empêche la dérive de précédence que deux `COALESCE` écrits indépendamment produiraient tôt ou tard ; le chemin agent (`simulate_param_overrides`/`simulate_param_run`) et l'endpoint REST partagent ce même résolveur et le même cycle fork/propagate/archive que les watchers node-override (#340), donc `lot_policy_watcher` est scenario-backed sans code de simulation dupliqué.
- **Négatif / dette :** aucune dette de branchement restante sur le périmètre V1 — la seule limite est le périmètre lui-même (15 champs, pas de topologie/BOM, eando/dq hors scope par nature, cf. section PR4 point 5).
- **Reste à faire :** rien sur le chantier #347 (PR1→PR4 toutes mergées). Débloquer eando/dq nécessiterait un mécanisme de simulation non paramétrique — hors #347, à instruire séparément si le besoin se confirme.

## Code references

- `src/ootils_core/engine/scenario/param_overlay.py` — `resolved_params_sql()` (résolveur multi-champs) et son pendant mono-champ (PR3, propagation), `ALLOWED_PARAM_FIELDS` + `PARAM_FIELD_BOUNDS` (whitelist V1, 15 champs, bornes métier), `set_param_override()` / `clear_param_override()` / `list_param_overrides()`.
- `src/ootils_core/db/migrations/060_scenario_planning_overrides.sql` — table, contrainte, FK policy.
- `src/ootils_core/engine/orchestration/propagator_sql.py` — `SHORTAGES_SQL`, scénarisé en PR3.
- `src/ootils_core/engine/orchestration/propagator.py` — `safety_stock_cache` et `_get_safety_stock()`, scénarisés en PR3.
- `src/ootils_core/api/routers/mrp.py` — `_get_planning_params()`, le 5ᵉ lecteur (mode simple `POST /v1/mrp/run`), scénarisé en PR3.
- `src/ootils_core/tools/agent_tools.py` — `simulate_param_overrides()` (PR4), factorisée sur `_fork_propagate_delta`, partagé avec `simulate_overrides()`.
- `src/ootils_core/api/routers/param_overrides.py` — endpoint REST `POST/GET/DELETE /v1/scenarios/{id}/param-overrides` (PR4), kill-switch `OOTILS_PARAM_OVERLAY_ENABLED`.
- `scripts/agent_simulation.py` — `simulate_param_run()` (PR4), harness one-fork-per-run partagé par le lot_policy watcher.
- `scripts/agent_lot_policy_watcher.py` — `_CHANGE_TYPE_FIELD` / `build_param_override()` (PR4), mapping des 3 `change_type` sur la whitelist V1.
- `tests/test_param_overlay.py` — couverture pure-Python (whitelist, garde d'alias, surface d'injection).
- `tests/integration/test_param_overlay_integration.py` — isolation par fork (C0), rétention FK.
- `docs/REVIEW-2026-07-APS.md` — item A10, source du chantier.
- `docs/ADR-011-scenario-retention.md` — politique FK `RESTRICT` sur `scenarios`.
- `docs/ADR-021-shortage-truth.md` — ownership `shortages` (ShortageDetector/`SHORTAGES_SQL` seuls écrivains) ; PR3 s'appuie sur cet arbitrage pour ne pas dupliquer la persistance des pénuries scénarisées.
