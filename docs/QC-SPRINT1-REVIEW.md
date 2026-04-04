# QC Sprint 1 — Ootils Core
*Review date: 2026-04-04 — Reviewer: Claw*

---

## A. Cohérence Architecture

**Verdict: PASS avec 2 warnings**

✅ **2-layers respectés.** `ProjectionKernel` est pur (zéro DB), `GraphStore` est la seule couche DB du kernel. L'orchestration (`PropagationEngine`, `CalcRunManager`) n'implémente pas de logique de calcul — séparation nette.

✅ **Pas de SQLite résiduel.** Tout est psycopg3/Postgres. `connection.py` docstring mentionne explicitement la migration depuis SQLite.

✅ **Pas de JSONB pour données structurées.** `triggered_by_event_ids` utilise `UUID[]` (array typé) — bon choix. Tous les deltas d'events sont en colonnes typées.

✅ **Interface kernel propre.** `GraphStore.__init__` prend une `psycopg.Connection` — swap vers Rust possible sans toucher l'orchestration.

⚠️ **WARNING: `db` passé comme paramètre non typé partout.** `DirtyFlagManager`, `PropagationEngine`, `CalcRunManager` acceptent `db` sans annotation de type. En Python, c'est `Any` implicite — casse la lisibilité et rend le swap plus risqué.

⚠️ **WARNING: couplage direct `propagator.py → store.py` via SQL inline.** `_recompute_pi_node` dans `propagator.py` exécute un `UPDATE nodes` direct (`db.execute("""UPDATE nodes...""")`). C'est de la logique de persistance dans la couche orchestration — ça devrait passer par `GraphStore.upsert_node()`.

---

## B. Qualité du Code Python

**Verdict: PASS avec 3 warnings**

✅ **Typing globalement correct.** Dataclasses typées, `Optional`, `UUID`, `Decimal`, `date` — tout est explicite. `from __future__ import annotations` systématique.

✅ **Gestion des erreurs structurée.** `CycleDetectedError`, `EngineStartupError` — custom exceptions propres. `fail_calc_run` release l'advisory lock en `try/except` best-effort — bon pattern.

✅ **Logging utilisé (pas print).** `logger = logging.getLogger(__name__)` dans `propagator.py`. Reste du code n'a pas de logging (pas de `print` non plus — neutre pour l'instant).

✅ **Docstrings utiles.** Toutes les méthodes publiques sont documentées, avec args/returns explicites sur les méthodes critiques.

✅ **Decimal pour les quantités partout.** Pas de float — correct pour du calcul financier/inventaire.

⚠️ **WARNING: `from datetime import date` importé à l'intérieur d'une méthode** dans `propagator.py` (`process_event`). Import late — antipattern Python, à remonter en haut du fichier.

⚠️ **WARNING: `list` non typé dans la signature de `compute_pi_node`.** `supply_events: list` et `demand_events: list` — devrait être `list[tuple[date, Decimal]]`. Les DTOs `SupplyEvent`/`DemandEvent` existent dans `models/__init__.py` mais ne sont pas utilisés par le kernel.

⚠️ **WARNING: `ProjectedInventoryNode`, `PurchaseOrderNode`, `OnHandNode` dans models** sont des aliases simples (`= Node`). C'est un antipattern — pas de vrai typage, trompe le lecteur. Soit supprimer, soit créer de vraies sous-classes ou des `NewType`.

---

## C. Schéma SQL / Migrations

**Verdict: PASS**

✅ **Toutes les PKs sont UUID.** Avec `DEFAULT gen_random_uuid()` — propre.

✅ **Tous les timestamps sont TIMESTAMPTZ.** Pas de `TIMESTAMP` sans timezone.

✅ **Pas de JSONB pour données structurées.** `events` utilise des colonnes typées (`old_date`, `new_date`, `old_quantity`, etc.). `triggered_by_event_ids` est `UUID[]`. ✅

✅ **CHECK constraints exhaustifs.** `node_type`, `edge_type`, `event_type`, `status` sur toutes les tables concernées.

✅ **Index complets et bien pensés.** Partial indexes avec `WHERE active = TRUE`, index composites sur les patterns d'accès réels (traversal, dirty scan, event queue).

✅ **FKs deferrable.** `fk_nodes_projection_series` et `fk_nodes_last_calc_run` en `DEFERRABLE INITIALLY DEFERRED` — nécessaire pour les inserts en batch circulaires.

✅ **Seed data idempotente.** `ON CONFLICT DO NOTHING` partout.

✅ **`zone_transition_runs` avec `idempotency_key` unique.** Conforme à ADR-006.

**1 point d'attention (pas BLOCKER) :**
- Le `CHECK` sur `scenarios.status` inclut `'running'` — valeur qui n'apparaît nulle part dans le code Python. À aligner ou documenter.

---

## D. Tests

**Verdict: CONDITIONAL — couverture fonctionnelle correcte, quelques edge cases manquants**

✅ **677 lignes de tests.** Substantiel pour un Sprint 1.

✅ **Séparation tests unitaires / intégration via fixtures.** Tests unitaires sur `ProjectionKernel` (pur, pas de DB), tests d'intégration sur `GraphStore`, `DirtyFlagManager`, `PropagationEngine`.

✅ **Cas critiques couverts :** cycle detection, dirty flag flush/recovery, projection de base (opening/inflows/outflows/shortage), advisory lock (coalescing).

**Edge cases manquants identifiés :**

⚠️ **Pas de test de crash recovery.** `load_from_postgres` (fallback dirty) n'est pas testé — c'est la feature la plus critique pour la durabilité.

⚠️ **Pas de test multi-bucket chain.** `feeds_forward` entre PI nodes en séquence non testée — c'est le cœur de la propagation PI.

⚠️ **Pas de test de demande pro-ratée.** La logique de pro-ration temporelle dans `_recompute_pi_node` (overlap `time_span`) n'a pas de test dédié.

⚠️ **Pas de test `startup_cycle_check`.** Le check de démarrage n'est pas couvert.

---

## E. BLOCKERs et Issues

### 🔴 BLOCKER

**B1 — `_recompute_pi_node` bypass `GraphStore.upsert_node()`**
`propagator.py:_recompute_pi_node` fait un `db.execute("UPDATE nodes ...")` direct. Ça court-circuite l'abstraction du kernel et casse l'interface propre définie pour le swap Rust. Toute écriture sur `nodes` doit passer par `GraphStore`.

**B2 — Advisory lock jamais releasé si crash entre `start_calc_run` et `fail_calc_run`**
`process_event` fait `SAVEPOINT propagation_start` puis `ROLLBACK TO SAVEPOINT` sur exception. Mais l'advisory lock est acquis dans `start_calc_run` et releasé dans `complete_calc_run` ou `fail_calc_run`. Si une exception se produit entre le `start_calc_run` et le bloc `try`, le lock n'est jamais releasé. → `recover_pending_runs` sur startup gère les `running` mais n'unlock pas les advisory locks orphelins (qui sont session-scoped côté Postgres, donc ils disparaissent si la connexion est fermée — mais c'est à documenter explicitement).

### ⚠️ WARNING

**W1 — `db` non typé** — voir section B. Risque de régression silencieuse.

**W2 — Aliases de types dans models.py** — `ProjectedInventoryNode = Node` trompe les outils d'analyse statique.

**W3 — Import late `from datetime import date`** dans `propagator.py`.

**W4 — Crash recovery non testée** — test manquant sur `load_from_postgres`.

**W5 — Chain PI multi-bucket non testée** — test manquant sur `feeds_forward` end-to-end.

**W6 — `supply_events: list` / `demand_events: list` non typés dans `compute_pi_node`** — les DTOs `SupplyEvent`/`DemandEvent` existent et ne sont pas utilisés.

### 🟡 NITPICK

**N1 — `_apply_migrations` swallow silencieux des erreurs "already exists"** — les autres erreurs DDL non-"already exists" devraient peut-être avoir un log WARNING avant re-raise.

**N2 — `scenarios.status = 'running'` dans le CHECK non utilisé côté code** — dead value à documenter ou supprimer.

**N3 — `validate_no_cycle` charge TOUTES les edges du scenario en mémoire** — acceptable pour PoA, à noter comme future optimisation à 50K+ nœuds.

---

## F. Score Global

| Dimension | Score | Note |
|---|---|---|
| Architecture | 8/10 | 2-layers tenus, 1 BLOCKER sur bypass store |
| Qualité Python | 7/10 | Typing solide, quelques lacunes sur les signatures |
| Schéma SQL | 9/10 | Excellent — indexes, constraints, pas de JSONB |
| Tests | 6/10 | Bon volume, edge cases critiques manquants |

**Verdict global : CONDITIONAL**

2 BLOCKERs à corriger avant Sprint 2 :
1. Faire passer `_recompute_pi_node` par `GraphStore.upsert_node()`
2. Documenter/tester le comportement de l'advisory lock sur crash

Le reste peut être adressé en début de Sprint 2 sans bloquer.
