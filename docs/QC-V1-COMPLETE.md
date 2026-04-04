# QC-V1-COMPLETE — Ootils Core V1 Quality Control Report

**Date:** 2026-04-04  
**Scope:** 6 sprints merged on `main` — sprint1 foundation through M6 API  
**Reviewer:** Automated QC pass (full codebase read)  
**Audience:** Senior tech lead  

---

## A. Architecture Globale

### A.1 Layers kernel/orchestration maintenus ?

**Oui, avec exceptions localisées.**

Le pattern 2-layers est tenu sur l'ensemble de la V1 :
- `kernel/` : calcul pur (ProjectionKernel), graphe (GraphStore, GraphTraversal, DirtyFlagManager), temporal (TemporalBridge, ZoneTransitionEngine), allocation, explication, shortage — **zéro dépendance vers orchestration ou API**.
- `orchestration/` : PropagationEngine, CalcRunManager — orchestre les composants kernel, possède la logique de pipeline.

**Violations localisées (WARNING, pas BLOCKER) :**

1. **`propagator.py` contient 4 requêtes SQL inline** hors GraphStore :
   - `SELECT * FROM events WHERE event_id = %s`
   - `UPDATE calc_runs SET dirty_node_count = %s`
   - `SELECT * FROM scenarios WHERE scenario_id = %s`
   - `SAVEPOINT propagation_start` / `ROLLBACK TO SAVEPOINT`
   
   Les 3 premières devraient passer par le store ou CalcRunManager. Les SAVEPOINTs sont acceptables (infrastructure transactionnelle, pas de domaine).

2. **`calc_run.py` contient toutes ses requêtes SQL inline** (advisory lock, INSERT calc_runs, UPDATE events, etc.) — ce module n'est pas censé être un store, mais sa responsabilité SQL est confinée à la gestion du cycle de vie des runs. Pattern borderline mais défendable.

3. **`ExplanationBuilder` et `ShortageDetector` ont du SQL inline déclaré comme volontaire** ("ce module est le propriétaire exclusif de ces tables"). Pattern explicitement documenté, acceptable.

### A.2 SQL inline hors modules autorisés ?

| Module | SQL inline | Verdict |
|--------|-----------|---------|
| `propagator.py` | 4 requêtes (events, calc_runs, scenarios, SAVEPOINTs) | WARNING |
| `calc_run.py` | ~12 requêtes (tout le cycle de vie) | WARNING (acceptable) |
| `builder.py` | propriétaire explicite | OK |
| `detector.py` | propriétaire explicite | OK |
| `manager.py` | ~20 requêtes — propriétaire du domaine scénario | OK |
| `zone_transition.py` | 6 requêtes inline (idempotency checks) | WARNING |
| API routers | `graph.py`, `projection.py`, `issues.py` : SELECT inline | WARNING |
| `connection.py` | health_check : 2 queries inline | OK (utilitaire) |

**Aucun BLOCKER SQL inline** (pas de SQL inline dans les fichiers kernel purs).

### A.3 Nouvelles couches respectent les patterns ?

**API :** FastAPI avec Pydantic models, dependency injection, auth centralisée. Pattern propre. Mais `graph.py` bypass GraphStore pour la requête principale (SQL direct + import de `_row_to_node`, `_row_to_edge` — fonctions privées !).

**ScenarioManager :** Logique complexe (create, override, diff, promote) bien encapsulée. SQL inline justifié par la complexité. Les f-strings SQL pour les noms de colonnes (apply_override, promote) sont **whitelistées** → pas d'injection possible, mais pattern fragile.

**ShortageDetector / ExplanationBuilder :** Pattern propriétaire documenté, cohérent.

### A.4 Couplages inattendus / dépendances circulaires ?

**Aucune dépendance circulaire détectée.**

Couplages à signaler :
- `AllocationEngine` instancie `GraphStore` en interne (ligne 70) → couplage implicite avec psycopg3. Acceptable pour V1.
- `TemporalBridge` instancie `GraphStore` dans `_load_series_nodes` → même pattern.
- `graph.py` router importe `_row_to_node`, `_row_to_edge` (fonctions privées de store.py préfixées `_`). **Violation de l'encapsulation** — ces fonctions devraient être publiques ou le router devrait passer par le store.
- `Scenario` dataclass porte `BASELINE_ID` comme class variable — sémantiquement incohérent (une constante dans un modèle de données).

---

## B. Cohérence des migrations SQL

### B.1 Cohérence entre les 6 migrations

**BLOCKER :** `001_initial_schema.sql` est un schéma SQLite mal recyclé dans la pile PostgreSQL.

| Problème | Impact |
|---------|--------|
| `PRAGMA journal_mode = WAL` → syntax error PG | **crash migration** sur PG frais |
| `strftime('%Y-%m-%dT%H:%M:%fZ','now')` → invalid PG | crash |
| `INSERT OR IGNORE` → syntax error PG | crash |
| `TEXT NOT NULL PRIMARY KEY` → valid PG mais type incohérent avec 002 (UUID) | incohérence silencieuse si 001 passe en premier |
| `REAL` type (float) → remplacé par NUMERIC dans 002 | perte de précision si 001 crée les tables avant 002 |

Le handler `_apply_migrations` ne swallows QUE "already exists". `PRAGMA` génère "syntax error" → **sera re-raised → migration crash au bootstrap**. 001 doit être supprimé ou rendu no-op sur PG.

**Tables dupliquées entre 001 et 002 (IF NOT EXISTS protège si ordre) :**
- 001 crée : items, locations, suppliers, policies, scenarios, nodes, edges, events, calc_runs, explanations, explanation_steps, scenario_overrides
- 002 recrée : scenarios, items, locations, nodes, edges, projection_series, node_type_policies, events, calc_runs, dirty_nodes, zone_transition_runs

Si 001 crashe sur PRAGMA avant de créer les tables, 002 crée tout proprement. Si 001 était du SQLite natif et qu'un outil "corrige" le PRAGMA → 001 crée des tables TEXT-typed qui entrent en conflit avec les UUID de 002.

**Conflit de noms de tables :**
- 001 crée `explanation_steps` ; 004 crée `causal_steps` — deux tables pour la même sémantique. `builder.py` utilise `causal_steps`. `explanation_steps` est orpheline si 001 crashe avant.

**002 ALTER TABLE sans IF NOT EXISTS :**
```sql
ALTER TABLE nodes ADD CONSTRAINT fk_nodes_projection_series ...
ALTER TABLE nodes ADD CONSTRAINT fk_nodes_last_calc_run ...
```
Sans `IF NOT EXISTS` (non supporté par PG pour contraintes), ces ALTER échouent si relancés. L'error handler swallows "already exists" → OK en pratique, mais fragile.

### B.2 Ordre d'application

L'ordre filename-sorted est garanti : 001 → 002 → 003 → 004 → 005 → 006. Cohérent et correct si 001 est ignoré/supprimé.

**003 :** Drop + recreate de `zone_transition_runs` — schéma 002 ne matchait pas l'implémentation ZoneTransitionEngine. Correction légitime.

**006 :** `ALTER TABLE events DROP CONSTRAINT IF EXISTS ... ; ADD CONSTRAINT ...` — pattern correct pour modifier le CHECK constraint.

### B.3 JSONB pour données structurées ?

**Aucun JSONB utilisé dans 002–006.** Conformité totale avec l'ADR.

001 utilise des colonnes TEXT JSON (`attributes`, `parameters`, `override_value`, `payload`) mais 001 est obsolète.

### B.4 Index couvrent-ils les patterns d'accès réels ?

**Bons :**
- `idx_nodes_scenario_type` (scenario_id, node_type) WHERE active — utilisé par get_all_nodes, propagation
- `idx_nodes_time_window` — utilisé par expand_dirty_subgraph
- `idx_nodes_projection_series_seq` — utilisé par get_nodes_by_series
- `idx_edges_from`, `idx_edges_to` — bidirectionnel, critique pour traversal
- `shortages_pi_node_calc_run_uidx` UNIQUE — supporte ON CONFLICT dans detector.persist
- `idx_events_unprocessed` — supporte la coalescing dans CalcRunManager

**Manquants :**
- `shortages (scenario_id, status, shortage_date)` — la requête `get_active_shortages` filtre sur scenario_id + status + ORDER BY severity_score. L'index `shortages_scenario_id_idx` couvre scenario_id seul ; sans index sur severity_score, le sort peut être coûteux à l'échelle.
- `scenario_diffs (scenario_id, node_id)` — pattern de lookup après diff(). L'index `idx_scenario_diffs_node` couvre node_id mais pas la combinaison.
- Aucun index sur `nodes (last_calc_run_id)` — pourrait servir pour des requêtes de recovery.

---

## C. Cohérence modèles Python ↔ schéma SQL

### C.1 Mapping dataclasses → tables

| Dataclass | Table SQL | Statut |
|-----------|-----------|--------|
| `Scenario` | `scenarios` | ✅ |
| `Item` | `items` | ✅ |
| `Location` | `locations` | ✅ |
| `Node` | `nodes` | ✅ complet |
| `Edge` | `edges` | ✅ |
| `ProjectionSeries` | `projection_series` | ✅ |
| `NodeTypeTemporalPolicy` | `node_type_policies` | ✅ |
| `CalcRun` | `calc_runs` | ✅ |
| `PlanningEvent` | `events` | ✅ |
| `Explanation` | `explanations` | ✅ |
| `CausalStep` | `causal_steps` | ✅ |
| `ShortageRecord` | `shortages` | ✅ |
| `ScenarioOverride` | `scenario_overrides` | ✅ |
| `ScenarioDiff` | `scenario_diffs` | ✅ |
| `AllocationResult` | **aucune table** | WARNING (résultat ephémère, acceptable) |
| `SupplyEvent` / `DemandEvent` | **aucune table** | OK (DTOs kernel) |
| `ProjectedInventoryResult` | **aucune table** | OK (DTO) |

### C.2 Cohérence des types

| Champ | Python | SQL | Statut |
|-------|--------|-----|--------|
| PKs | `UUID` | `UUID DEFAULT gen_random_uuid()` | ✅ |
| Quantités | `Decimal` | `NUMERIC` | ✅ |
| Dates | `date` | `DATE` | ✅ |
| Timestamps | `datetime` (timezone.utc) | `TIMESTAMPTZ` | ✅ |
| Booléens | `bool` | `BOOLEAN` | ✅ |
| `Node.quantity` | `Optional[Decimal]` | `NUMERIC` (nullable) | ✅ |
| `CalcRun.triggered_by_event_ids` | `list[UUID]` | `UUID[]` (PG array) | ✅ |
| `Edge.weight_ratio` | `Decimal` | `NUMERIC` | ✅ |

**Divergences :**
- `Scenario.BASELINE_ID` est une class variable dans le dataclass → n'a pas d'équivalent SQL, crée une ambiguïté lors des comparaisons d'instances.
- `Explanation.summary` : NOT NULL dans 004, mais `Optional[str]` implicite (pas de default) dans 001 (TEXT nullable). En production 004 → `summary TEXT NOT NULL` → OK.
- `Node` n'a pas de champ `business_key` (présent dans 001 schema) — absent de 002 → **cohérent avec 002**.

### C.3 Champs manquants

**SQL → Python :**
- `scenarios.baseline_snapshot_id` → présent dans `Scenario.baseline_snapshot_id` ✅
- `node_type_policies.zone3_grain` → présent dans `NodeTypeTemporalPolicy.zone3_grain` ✅

**Python → SQL :**
- `Node.has_exact_date_inputs`, `has_week_inputs`, `has_month_inputs` → présents dans 002 ✅
- `AllocationResult.run_at` → pas de table (OK, DTO)

---

## D. Qualité Code Python

### D.1 Typing

**Global :** `from __future__ import annotations` présent partout → forward references OK. Typing cohérent dans les signatures publiques.

**Issues :**
- `ProjectionKernel.compute_pi_node` → `-> dict` au lieu de `-> ProjectedInventoryResult`. Le dataclass `ProjectedInventoryResult` est défini mais **jamais utilisé**. Le propagator accède aux clés via `result["closing_stock"]` etc. — pas de type safety sur le résultat de calcul. WARNING.
- `ProjectionKernel.compute_pi_node` : paramètres `supply_events: list` et `demand_events: list` non typés (devrait être `list[tuple[date, Decimal]]`). WARNING.
- `dirty.py` : `db` paramètre typé comme `db` (Any implicite) dans toutes les méthodes — pas de type hint sur le paramètre de connexion. NITPICK.
- `builder.py` et `detector.py` : `db` paramètre non typé (Any). Cohérent entre eux mais devrait être `psycopg.Connection`. NITPICK.
- `AllocationEngine._allocate_demand` : `store: GraphStore` paramètre typé, mais `db: psycopg.Connection` — bon.

### D.2 Gestion des erreurs

**Uniforme et défensive :**
- PropagationEngine swallows ExplanationBuilder et ShortageDetector failures avec `except Exception: logger.warning(...)` — **design correct** pour ne pas casser la propagation.
- CalcRunManager.fail_calc_run swallows advisory unlock exception — **design correct**.
- ZoneTransitionEngine lève `RuntimeError` si lock non acquis — **design correct**.
- `CycleDetectedError` est typée avec les paramètres utiles — **bon design**.

**Problèmes :**
- `OotilsDB._apply_migrations` : swallows toutes les exceptions "already exists" **par fichier entier**, pas par statement. Si un fichier a 30 statements et que le statement 15 génère "already exists", le reste du fichier est ignoré. Dans le cas de 001 (PRAGMA → "syntax error"), le fichier entier crash → comportement imprévisible.
- `resolve_scenario_id` : UUID invalide → **silently** falls back to baseline. Pas de 422 pour les clients — peut masquer des bugs.
- `AllocationEngine._allocate_demand` : PI node not found → logger.warning + skip. Pas de raise. Acceptable pour un engine resilient mais silence des incohérences graph.

### D.3 Imports late

- `propagator.py` : `from datetime import datetime, timezone` inside `_finish_run` (ligne ~152) et `update_pi_result` in store.py (inside method). **NITPICK** — à déplacer en tête de fichier.
- `propagator.py` : `from ootils_core.engine.kernel.graph.store import _row_to_node, _row_to_edge` inside `get_graph` in `graph.py`. Late import d'une fonction privée dans un handler. **WARNING**.

### D.4 Anti-patterns

- **Priority via Node.quantity** : `AllocationEngine` utilise `Node.quantity` comme clé de priorité pour les demandes. Champ multipurpose (quantité ET priorité). Fragile, non explicite, risque de bugs silencieux si `quantity=None` (fallback to 0 = highest priority). **WARNING**.
- **`_PRIORITY_KEY` sentinel** : `_SENTINEL_DATE = date(9999, 12, 31)` pour NULLS LAST sort — magic constant sans explication dans le code. NITPICK.
- **`Scenario.BASELINE_ID` as class var** : mélange constante de domaine et modèle de données. Devrait être dans un module `constants.py`. NITPICK.
- **`_node_business_key` dans manager.py** : tuple match sur (node_type, item_id, location_id, time_span_start, bucket_sequence) sans index DB sur cette combinaison. Pour de gros datasets, le diff() O(N²) en Python est problématique. WARNING à l'échelle.
- **`_copy_nodes` loop** : N INSERTs individuels dans une boucle Python. Pas de COPY ou INSERT batch. Pour des scénarios avec 10k+ nodes → performance concern. WARNING.

---

## E. API V1 — Sécurité et Complétude

### E.1 Couverture auth

| Endpoint | Auth | Statut |
|----------|------|--------|
| `GET /health` | Aucune | ✅ (correct) |
| `POST /v1/events` | `Depends(require_auth)` | ✅ |
| `GET /v1/projection` | `Depends(require_auth)` | ✅ |
| `GET /v1/issues` | `Depends(require_auth)` | ✅ |
| `GET /v1/explain` | `Depends(require_auth)` | ✅ |
| `POST /v1/simulate` | `Depends(require_auth)` | ✅ |
| `GET /v1/graph` | `Depends(require_auth)` | ✅ |
| `GET /docs` | Aucune | WARNING (OpenAPI accessible sans auth) |
| `GET /openapi.json` | Aucune | WARNING |

**Auth statique (env var OOTILS_API_TOKEN)** — acceptable pour V1 PoC, pas pour production multi-tenant.

### E.2 Réponses typées

Tous les endpoints retournent des Pydantic models typés. **Aucun `dict` brut** en réponse. Conforme.

**Exception :** `/health` retourne `dict` directement (sans Pydantic model) — acceptable pour un endpoint utilitaire, mais inconsistant. NITPICK.

### E.3 Gestion des erreurs HTTP

| Cas | Comportement |
|-----|-------------|
| 401 sans token | ✅ HTTPException 401 avec WWW-Authenticate |
| 401 mauvais token | ✅ |
| 404 série non trouvée (projection) | ✅ |
| 404 explication non trouvée | ✅ |
| 404 item/location non trouvé | ✅ |
| 422 event_type invalide | ✅ (validation manuelle dans handler) |
| 422 node_id UUID invalide (explain) | ✅ |
| 500 create_scenario failure | ✅ swallowed proprement |
| Override ValueError → 200 skip | WARNING (erreurs d'override silencieuses) |
| Invalid scenario UUID → baseline fallback | WARNING (pas de 422) |

### E.4 Fidélité OpenAPI spec vs api-spec.md

**Divergences significatives :**

1. **POST /events** — spec demande `payload: {field, old_value, new_value}` (JSONB) ; implémentation utilise `field_changed`, `old_date`, `new_date`, `old_quantity`, `new_quantity`, `old_text`, `new_text` (typed). **Implémentation meilleure que la spec**, mais spec non mise à jour. WARNING.

2. **Event types** — **BLOCKER critique** : les types acceptés par le router (`VALID_EVENT_TYPES`) **ne correspondent pas** au CHECK constraint en DB :
   - API accepte mais DB rejette : `demand_date_changed`, `onhand_changed`, `capacity_changed`, `constraint_changed`, `scenario_override_applied`
   - DB accepte mais API n'expose pas : `onhand_updated`, `scenario_created`, `calc_triggered`, `po_date_changed`, `test_event`
   - Résultat : POST /v1/events avec `demand_date_changed` → insert DB → **constraint violation** → 500 en runtime.

3. **GET /projection** — spec demande `from` et `to` comme required ; implémentation les ignore (retourne tous les buckets de la série). NITPICK.

4. **GET /projection** response shape — spec retourne `projection[]` avec `date`, `projected_qty`, `available_to_promise` ; implémentation retourne `buckets[]` avec `opening_stock`, `inflows`, `outflows`, `closing_stock`. Structure différente.

5. **Endpoints manquants dans l'implémentation vs spec :** GET /items, GET /locations, POST /simulate/diff, POST /simulate/promote non implémentés. Mais hors périmètre V1 selon git log. OK pour V1.

6. **Endpoints présents mais absents de la spec :** GET /v1/graph. NITPICK.

---

## F. Tests — Couverture et Qualité

### F.1 Couverture par sprint

| Sprint | Fichier test | Couverture |
|--------|-------------|-----------|
| Sprint 1 | `test_sprint1.py` | ProjectionKernel (6 unit + 5 unit), DirtyFlagManager (9 unit), CalcRunManager (2 unit), 1 integration | ✅ Bonne |
| Sprint 2 temporal | `test_sprint2_temporal.py` | GrainHelpers (8), ZoneBoundary (7), Bridge.aggregate (6), Bridge.disaggregate (9), ZoneTransition idempotency (5), SplitBuckets (5), 2 integration | ✅ Excellente |
| Sprint 2 allocation | `test_sprint2_allocation.py` | AllocationResult (1), Priority ordering (3), Partial (2), Zero stock (2), Determinism (2), Counters (2), PeggedTo (4), integration (skip) | ✅ Bonne |
| Sprint M3 | `test_m3_explanations.py` | build_pi_explanation (6), persist round-trip (3), PropagatorIntegration (4), models (4) | ✅ Bonne |
| Sprint M4 | `test_m4_shortage.py` | detect no shortage (3), detect shortage (7), severity (4), get_active (5), resolve_stale (4), PropagatorIntegration (4) | ✅ Excellente |
| Sprint M5 | `test_m5_scenarios.py` | create_scenario (3), apply_override (5), diff (3), promote (3) | ✅ Correcte |
| Sprint M6 | `test_m6_api.py` | auth (5), events (3), projection (2), issues (3), explain (3), simulate (2), graph (1), openapi (1) | ✅ Correcte |

### F.2 Edge cases manquants critiques

| Manquant | Impact |
|---------|--------|
| Test event_type mismatch API/DB → constraint violation | BLOCKER non couvert |
| Test 001 migration SQLite sur PG | BLOCKER non couvert |
| Test `_copy_nodes` avec projection_series FK → nouvelle série non copiée | WARNING non couvert |
| Test `diff()` quand aucun calc_run completed → ValueError | Edge case non couvert |
| Test advisory lock timeout (zone_transition concurrent) | Edge case non couvert |
| Test `shortage_detector.resolve_stale` **jamais appelé** dans le propagator | BLOCKER fonctionnel non couvert |
| Test allocation avec `demand.quantity=None` → priority_key returns 0 = highest priority | Edge case non couvert |
| Test `promote` quand baseline node match multiple rows | Edge case non couvert |

### F.3 Séparation unit/integration

**Bien maintenue :** marquage `@pytest.mark.skipif(not DATABASE_URL, ...)` cohérent. Les tests unit utilisent des mocks psycopg propres. Les fixtures d'integration font leur propre setup/teardown.

**Issues :**
- `TestAllocationIntegration.test_integration_basic_allocation` → `pytest.skip(...)` codé en dur = test zombie. WARNING.
- Integration test Sprint 1 utilise `conn.cursor()` (context manager ouvert sans usage, puis appelle `conn.execute()` directement) → cursor ouvert jamais utilisé mais ne crée pas de bug en psycopg3. NITPICK.
- Les tests M5 (`test_m5_scenarios.py`) sont **entièrement mocks** — aucun test integration pour les flows scenario critiques (create → override → diff → promote). WARNING.

---

## G. BLOCKERs, WARNINGs, NITPICKs

### 🔴 BLOCKERs

**B1 — `001_initial_schema.sql` crashe sur PostgreSQL**  
`PRAGMA journal_mode = WAL` génère "syntax error" sur PG → handler ne swallows que "already exists" → re-raise → migration crash au bootstrap. **Chaque déploiement fresh est cassé.**  
Fix : supprimer 001 ou ajouter un header `-- skip-on-pg` et un handler conditionnel.

**B2 — Divergence event_type API/DB → 500 en runtime**  
`VALID_EVENT_TYPES` dans `events.py` contient `demand_date_changed`, `onhand_changed`, `capacity_changed`, `constraint_changed`, `scenario_override_applied`. Le CHECK constraint PG ne les connaît pas. Un appel `POST /v1/events {"event_type": "demand_date_changed"}` → validation API OK → INSERT DB → constraint violation → 500.  
Fix : aligner VALID_EVENT_TYPES sur le CHECK constraint de migration 006.

**B3 — `ShortageDetector.resolve_stale()` jamais appelée**  
La méthode est implémentée et testée, mais **n'est jamais invoquée** dans `PropagationEngine`. Les shortages résolues lors d'un recalcul (closing_stock redevient >= 0) restent `status='active'` en DB indéfiniment. La liste `/v1/issues` retourne donc des faux positifs.  
Fix : appeler `shortage_detector.resolve_stale(scenario_id, calc_run_id, db)` dans `_finish_run()` ou après `_propagate()`.

### ⚠️ WARNINGs

**W1 — `ProjectionKernel.compute_pi_node` retourne `dict` non typé**  
`ProjectedInventoryResult` dataclass défini mais jamais utilisé. Accès aux résultats par clé string → aucune safety typing. Risque de KeyError silencieux.

**W2 — Priority via `Node.quantity` dans AllocationEngine**  
Champ multipurpose non documenté pour les demandes. Si une `CustomerOrderDemand` a `quantity=None`, elle prend la priorité maximale (0) par défaut silencieux. Sémantique ambigu, bugs potentiels en production.

**W3 — `graph.py` bypass GraphStore avec SQL inline + import de `_row_to_node` (privé)**  
Pattern incohérent avec l'architecture établie. La requête principale de `/v1/graph` (SELECT nodes WHERE scenario_id + item_id + location_id) devrait passer par le store.

**W4 — `_copy_nodes` loop individuelle N×INSERT**  
Pour des scénarios avec de nombreux nodes, la création de scénario sera lente. Pas de batch INSERT ou COPY.

**W5 — `_node_business_key` matching en Python pour diff() et promote()**  
O(N) lookup en boucle. Pour 10k+ nodes le diff() sera problématique. Pas d'index DB sur la combinaison (node_type, item_id, location_id, time_span_start, bucket_sequence).

**W6 — 4 requêtes SQL inline dans `propagator.py` hors GraphStore**  
`SELECT * FROM events`, `UPDATE calc_runs`, `SELECT * FROM scenarios` — violations légères du layer pattern.

**W7 — `resolve_scenario_id` silently falls back sur UUID invalide**  
Pas de 422. Mauvais UUID → baseline silencieux → comportement inattendu pour le client.

**W8 — Override errors silencieuses dans `/v1/simulate`**  
`ValueError` sur field_name interdit → skippé avec warning log seulement. Pas d'erreur remontée au client.

**W9 — `ALTER TABLE nodes ADD CONSTRAINT` non idempotent dans 002**  
Swallowed via "already exists" catch, mais fragile si l'erreur change. Devrait utiliser `IF NOT EXISTS` ou un check conditionnel.

**W10 — `TestAllocationIntegration` skip codé en dur**  
Test zombie. Soit l'implémenter soit le supprimer.

**W11 — Zone transition SQL inline dans `zone_transition.py`**  
6 méthodes privées avec SQL inline direct (idempotency checks, INSERT, UPDATE). Borderline avec l'ADR.

### 💡 NITPICKs

**N1 — Late imports dans `propagator.py`** (`from datetime import datetime, timezone` en milieu de fonction).

**N2 — `Scenario.BASELINE_ID` comme class var** → déplacer dans un module `constants.py`.

**N3 — `/health` retourne `dict` au lieu de Pydantic model** → incohérence stylistique.

**N4 — `DirtyFlagManager` methods : paramètre `db` non typé** (Any implicite).

**N5 — `ProjectionKernel.supply_events: list` sans type paramètre** → `list[tuple[date, Decimal]]`.

**N6 — `_SENTINEL_DATE = date(9999, 12, 31)`** → magic constant sans commentaire.

**N7 — `Explanation.causal_path` : champ non persisté en DB comme array** (steps dans table séparée) → la désérialisation est manuelle dans `get_explanation`. Pas de bug, mais complexité.

**N8 — `graph.py` depth parameter déclaré mais non utilisé** pour le BFS (récupère tous les nodes par item/location sans expansion à depth niveaux).

**N9 — `OotilsDB` singleton dans `dependencies.py`** → `_db: OotilsDB | None = None` global. Pas thread-safe pour les tests parallèles.

**N10 — `001_initial_schema.sql` contient `suppliers` et `policies` tables** non présentes dans 002–006 ni dans les modèles Python. Tables orphelines même si 001 était applicable.

---

## H. Score par Dimension et Verdict Global

| Dimension | Score /10 | Justification |
|-----------|-----------|---------------|
| A. Architecture globale | **7/10** | 2-layers tenu, couplages mineurs acceptables pour V1, SQL inline localisé |
| B. Migrations SQL | **5/10** | BLOCKER 001 SQLite, event_type mismatch BLOCKER, pas de JSONB (bon), index corrects |
| C. Cohérence Python ↔ SQL | **8/10** | Mapping complet, types cohérents, quelques DTOs sans table (volontaire) |
| D. Qualité code Python | **7/10** | Typing global propre, gestion erreurs défensive, anti-patterns allocation priority |
| E. API sécurité/complétude | **5/10** | Auth couvre tout, mais event_type mismatch BLOCKER, spec diverge, depth non implémenté |
| F. Tests couverture | **7/10** | Couverture unité correcte, BLOCKERs fonctionnels non couverts, integration M5 absente |

### Verdict Global

```
⛔ CONDITIONAL — Ne pas shipper en l'état.
```

**3 BLOCKERs à corriger avant tout déploiement :**

1. **B1** : Supprimer ou désactiver `001_initial_schema.sql` sur PG (15 min)
2. **B2** : Aligner `VALID_EVENT_TYPES` avec le CHECK constraint de 006 (5 min)
3. **B3** : Appeler `shortage_detector.resolve_stale()` dans le pipeline de propagation (30 min)

Après correction des 3 BLOCKERs et ajout de tests de non-régression pour chacun → **SHIP IT**.

Les WARNINGs (priority via quantity, copy_nodes loop, diff() scalabilité) peuvent être traités en V1.1 sans bloquer le déploiement PoC.

---

*Rapport généré par analyse statique complète — aucun fichier inventé, tous lus intégralement.*
