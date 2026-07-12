# ADR-040 — Dérogation FK encadrée pour la copie bulk du fork de scénario

**Statut :** Accepté — implémenté dans ce worktree (`feat/fork/fastcopy`), non encore mergé sur `main`.
**Date :** 2026-07-12
**Auteurs :** ootils-core team
**Contexte mesuré :** profiling statement-par-statement sur `ootils_bench_s` (VM, 2026-07-12, `create_scenario` complet, 13,4 s total) ; `docs/ADR-012-scenario-fork-bulk.md` (le passage O(N) requêtes → O(1) requêtes, chantier précédent sur le même chemin).

---

## Contexte

ADR-012 a réduit le fork de scénario de O(N) requêtes (une par ligne) à O(1) requêtes (deux `INSERT…SELECT` bulk via tables de mapping temporaires `_series_map`/`_node_map`). Le nombre de requêtes est constant, mais leur **coût unitaire** grandit avec le volume copié — et à l'échelle `bench_s` (72 367 nœuds actifs + 100 817 arêtes actives sur le scénario baseline), le fork complet prend encore **13,4 s**.

Le diagnostic de ce soir (profiling statement-par-statement, verrouillé avant tout code) attribue ce temps ainsi :

- **76 % — validation FK ligne-à-ligne par les triggers Postgres**, déclenchée par les deux `INSERT…SELECT` bulk eux-mêmes : 2,79 s pour les 72 K lignes de `nodes`, 5,70 s pour les 100 K lignes de `edges`.
- **~2,2 s — maintenance des index** sur `edges` pendant l'`INSERT` bulk (5 index composites/partiels à tenir à jour par ligne insérée).
- Le reste (construction des tables de mapping, checks d'intégrité déjà existants, etc.) est marginal.

Le plan d'exécution est **O(N) sain** — hash joins partout, aucune pathologie de plan (pas de nested loop par ligne, pas de stats périmées façon #455). Le coût n'est pas un mauvais plan ; c'est le travail réel de N validations FK indépendantes que Postgres refait une par une même à l'intérieur d'un `INSERT…SELECT` bulk, alors que dans ce cas précis **chaque ligne copiée est, par construction, déjà FK-valide** (voir Décision).

## Décision

### 1. Dérogation ciblée : `session_replication_role = 'replica'` autour des deux `INSERT…SELECT` bulk uniquement

`ScenarioManager._copy_nodes` (`engine/scenario/manager.py`) encadre exactement les deux `INSERT…SELECT` bulk (copie des `nodes`, puis copie des `edges`) — **pas** la copie de `projection_series` (non profilée, volume bien plus petit, dérogation non justifiée là) — par :

```sql
SAVEPOINT scenario_fork_replica_role;
SET LOCAL session_replication_role = 'replica';
RELEASE SAVEPOINT scenario_fork_replica_role;
-- … les deux INSERT…SELECT …
SET LOCAL session_replication_role = 'origin';
```

**Pourquoi c'est sûr ICI, et seulement ici :**

- `nodes.item_id` / `nodes.location_id` sont copiés **verbatim** (`n.item_id`, `n.location_id`) depuis des lignes source qui ont déjà passé la validation FK au moment de leur propre écriture ; `items`/`locations` sont des données de référence scénario-indépendantes, jamais forkées, donc la ligne référencée existe toujours.
- `nodes.scenario_id` est le nouveau `scenario_id`, dont la ligne a été insérée dans `scenarios` en tout premier dans la même transaction (`create_scenario`, étape 1).
- `edges.from_node_id` / `edges.to_node_id` sont résolus via le JOIN sur `_node_map` — par construction, chaque valeur est un `node_id` que le `INSERT` précédent, dans la même transaction, vient d'écrire. `edges.scenario_id` est le même `scenario_id` déjà valide.

Il ne s'agit donc pas de désactiver la validation FK sur une écriture arbitraire, mais sur une **copie certifiée d'un graphe déjà FK-valide**. C'est une dérogation de performance, pas de correction.

**Ce qui la garde fail-loudly (compensation set-based, ~100 ms) :**

Deux checks tournent **inconditionnellement** juste après les deux `INSERT`, que la dérogation ait pu s'activer ou non — la correction ne doit jamais dépendre du chemin emprunté :

1. **Orphan-edge check (#158, préexistant, inchangé)** — vérifie qu'aucune arête active du nouveau scénario ne référence un `node_id` hors de l'ensemble copié.
2. **Nouveau check FK nodes** — vérifie qu'aucun nœud du nouveau scénario n'a un `item_id`/`location_id` non résolu :

```sql
SELECT COUNT(*) FROM nodes n
WHERE n.scenario_id = :target
  AND (
    (n.item_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM items i WHERE i.item_id = n.item_id))
    OR (n.location_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM locations l WHERE l.location_id = n.location_id))
  )
```

Un `count > 0` sur l'un ou l'autre check lève un `RuntimeError` — la transaction n'est jamais commitée avec un graphe corrompu (le caller possède la transaction ; `ScenarioManager` ne commit/rollback jamais lui-même, cf. le contrat de classe).

### 2. Fallback transparent si le rôle manque le privilège `SET`

`session_replication_role` désactive **tous** les triggers de la session, pas seulement les FK — d'où le `SET LOCAL` (borné à la transaction, réverti automatiquement au COMMIT/ROLLBACK) et le retour explicite à `'origin'` immédiatement après les deux `INSERT`, avant tout autre travail sur la connexion.

Le fixer ce paramètre exige que le rôle de connexion détienne le privilège `SET` sur ce GUC :

- **PG15+** : `GRANT SET ON PARAMETER session_replication_role TO <role>;`
- **< PG15** : réservé au superutilisateur.

Un `SET` refusé (permission) **avorte la transaction englobante** sous Postgres — la tentative est donc encadrée par un `SAVEPOINT` :

```python
db.execute("SAVEPOINT scenario_fork_replica_role")
try:
    db.execute("SET LOCAL session_replication_role = 'replica'")
except psycopg.errors.InsufficientPrivilege:
    db.execute("ROLLBACK TO SAVEPOINT scenario_fork_replica_role")
    logger.warning(...)   # une ligne, pas de spam
    return False          # fallback : chemin lent (triggers ON), inchangé
else:
    db.execute("RELEASE SAVEPOINT scenario_fork_replica_role")
    return True
```

Seule `InsufficientPrivilege` est traitée comme le cas attendu ("pas de GRANT") ; toute autre exception remonte (pas de `except Exception` muet). Le fork doit fonctionner sur tout déploiement, avec ou sans le `GRANT` — jamais de crash lié à ce point.

**Note d'implémentation :** un log de warning "une fois" ne peut pas s'appuyer sur une variable globale mutable au niveau module (anti-pattern explicitement proscrit dans ce dépôt) — le warning est donc émis une fois **par appel** de la dérogation (un fork qui tombe en fallback logue une ligne, pas une pile), pas une fois pour la durée du process. C'est suffisant : le volume de forks reste faible en usage normal, et un opérateur voit immédiatement le premier (et chaque) fork qui tombe en fallback plutôt que de deviner un état caché.

### 3. Index `idx_edges_composite_lookup` — investigué, **conservé** (migration 077)

Le plan initial envisageait de supprimer `idx_edges_composite_lookup` (redondant en apparence avec `uq_edges_composite`, l'index unique partiel sur les mêmes 4 colonnes `WHERE active = TRUE`) pour réduire le coût de maintenance d'index pendant la copie bulk des `edges`. L'investigation demandée (VM + code) invalide ce plan :

- **VM (`pg_stat_user_indexes`, bases bench UNIQUEMENT — jamais `ootils_pilote_test`) :** `idx_scan > 0` sur `idx_edges_composite_lookup` dans **les trois** bases bench (`ootils_bench_s`: 98 ; `ootils_bench_m`: 1104 ; `ootils_bench_l`: 432). La règle du chantier ("si `idx_scan > 0`, ne pas dropper") s'applique directement.
- **Code (`grep`) :** `GraphStore.upsert_edge` (`engine/kernel/graph/store.py`), appelé pour chaque arête `pegged_to` par le moteur d'allocation (`engine/kernel/allocation/engine.py`), fait un lookup d'existence **sans filtrer `active`** :
  ```sql
  SELECT edge_id FROM edges
  WHERE from_node_id = %s AND to_node_id = %s AND edge_type = %s AND scenario_id = %s
  ```
  Un index partiel dont le prédicat (`active = TRUE`) n'est pas impliqué par la clause `WHERE` de la requête ne peut pas servir seul de chemin d'accès — cette requête chaude du chemin d'allocation dépend donc de l'index **complet** (non partiel). Le dropper régresserait silencieusement `upsert_edge` vers un scan séquentiel à chaque allocation.

**Décision : l'index est conservé.** Migration 077 (`077_drop_redundant_edges_index.sql`) ne contient aucun DDL — c'est un enregistrement délibéré de l'investigation (pour qu'un futur lecteur tombant sur "migration 077" dans `schema_migrations` trouve l'investigation, pas un trou). Le gain de débit d'ADR-040 vient entièrement de la dérogation FK (§1), pas d'un index en moins.

## Alternatives rejetées

- **`DEFERRABLE INITIALLY DEFERRED` sur les contraintes FK.** Ne gagne rien ici : la validation reste ligne-à-ligne, seulement décalée à la fin de la transaction au lieu de s'exécuter à chaque `INSERT` — même coût total, juste déplacé. Le problème n'est pas *quand* la validation FK a lieu mais *qu'elle ait lieu du tout* sur une copie déjà prouvée valide.
- **CoW paresseux (vrai lazy copy-on-write, chaîne de scénarios lue à la volée).** C'est la vraie solution structurelle (coût O(overrides) au lieu de O(N) en stockage ET en temps), mais un chantier bien plus large — chaque lecteur de `GraphStore` doit devenir chaîne-aware, chaque écriture doit devenir materialise-or-update, la sémantique de `scenario_overrides`/diff doit être re-fondée. Déjà identifiée comme ADR-013/ADR-041 (SCALE-2), hors périmètre de ce fix ciblé "~3×".
- **Suppression d'`idx_edges_composite_lookup`.** Rejetée — voir §3 : usage réel confirmé sur les trois échelles bench, et dépendance code identifiée (`upsert_edge`). Documentée plutôt qu'exécutée (migration 077, no-op).

## Risques

- **`session_replication_role = 'replica'` désactive TOUS les triggers de la session**, pas seulement la validation FK — si un futur trigger métier (audit, dénormalisation, etc.) est ajouté sur `nodes`/`edges` et doit obligatoirement s'exécuter à l'écriture, il serait silencieusement sauté pendant la fenêtre de la dérogation. Mitigation : la fenêtre est la plus étroite possible (`SET LOCAL`, borné à la transaction, remis à `'origin'` immédiatement après les deux `INSERT`, avant tout autre travail) et le commentaire au site d'appel documente explicitement ce risque pour quiconque ajoute un trigger futur sur ces deux tables.
- **Dépendance à un privilège Postgres non universel** (`GRANT SET ON PARAMETER`, PG15+, ou superuser). Mitigée par le fallback transparent (§2) — le fork reste correct et fonctionnel sur un déploiement sans ce privilège, seulement plus lent (chemin inchangé, celui d'avant ce chantier).
- **Le check FK compensatoire ajoute un aller-retour SQL supplémentaire** (~100 ms mesurés) même quand la dérogation a réussi — accepté : c'est le prix du fail-loudly, négligeable face au gain.

## Mesures avant / après

Sonde en transaction **ROLLBACKée** sur `ootils_bench_s` (VM, 2026-07-12), rejouant statement-par-statement la séquence de `create_scenario`/`_copy_projection_series`/`_copy_nodes` — comparaison A/B dans la même session (cache chaud), scénario baseline source = 72 367 nœuds actifs + 100 817 arêtes actives :

| | `INSERT nodes` | `INSERT edges` | **Total (wall)** |
|---|---|---|---|
| **Avant** (triggers ON, chemin inchangé) | 3,624 s | 7,282 s | **11,446 s** |
| **Après** (dérogation active) | 0,883 s | 1,919 s | **3,345 s** |

**Speedup mesuré : 3,4×** — conforme au "~3×" validé par le pilote (l'estimation initiale du chantier, "~4-5 s", était prudente ; le résultat réel sur cache chaud est meilleur). Une seconde sonde à froid (cache non préchauffé) mesure un total de ~5,55 s — toujours une nette amélioration par rapport aux 13,4 s du diagnostic initial, la variance venant de l'état du cache de buffers, pas du chemin de code.

**Intégrité vérifiée à chaque run :** `node_fk_violations = 0`, `orphan_edges = 0`, `copied_nodes`/`copied_edges` = 72 367 / 100 817 (identiques à la source) dans les deux chemins. Après `ROLLBACK`, le nombre de lignes dans `scenarios` est inchangé (le scénario de sonde n'a jamais été commité) — confirmé sur les deux runs.

**Note de contexte VM :** le rôle `ootils` utilisé pour la sonde est superutilisateur sur cette VM (`rolsuper = t`) — le `SET LOCAL session_replication_role = 'replica'` y réussit donc sans nécessiter le `GRANT` PG15+. Un déploiement pilote avec un rôle applicatif non-superutilisateur devra exécuter ce `GRANT` pour bénéficier de la dérogation ; à défaut, le fallback (§2) garantit que le fork reste correct, simplement au débit d'avant ce chantier.

## Conséquences

- **Positif :** fork de scénario ~3,4× plus rapide à l'échelle `bench_s` sans changer la forme des données produites (copie toujours byte-for-byte équivalente à la source, mêmes lecteurs downstream inchangés — `GraphStore`, le propagateur, `apply_override`, `diff`, `promote`). Deux checks set-based supplémentaires (~100 ms) renforcent, plutôt qu'affaiblissent, la garantie d'intégrité déjà en place (#158).
- **Négatif / dette assumée :**
  - Le fork reste O(N) en **stockage** (chaque fork double toujours les lignes copiées) — ce chantier réduit le *temps*, pas l'empreinte. Le vrai CoW paresseux (ADR-013/ADR-041, SCALE-2) reste la solution structurelle pour ça.
  - La dérogation dépend d'un privilège Postgres (`GRANT SET ON PARAMETER`) que tous les déploiements n'auront pas configuré d'emblée — documenté comme prérequis opérationnel, pas bloquant (fallback).
  - Migration 077 est un no-op délibéré (décision "conserver", pas "dropper") — un futur lecteur doit lire le header, pas seulement le nom de fichier, pour comprendre pourquoi.
- **Reste à faire :** ADR-013/ADR-041 (SCALE-2) pour la vraie réduction d'empreinte ; ré-exécuter cette même sonde à l'échelle `bench_l`/`bench_m` si un futur chantier veut confirmer que le speedup tient à plus grande échelle (non fait ici — mandat de ce chantier scopé à `bench_s`, l'échelle du diagnostic verrouillé).

## Code references

- `src/ootils_core/engine/scenario/manager.py:214-459` (`_copy_nodes`) — la dérogation, les deux `INSERT…SELECT` encadrés, le check FK nodes compensatoire, l'orphan-edge check (#158) inchangé.
- `src/ootils_core/engine/scenario/manager.py:999-1047` — `_enable_replica_role_for_fork` / `_restore_origin_role`, la logique SAVEPOINT + fallback.
- `src/ootils_core/db/migrations/077_drop_redundant_edges_index.sql` — décision "index conservé", investigation documentée, aucun DDL.
- `src/ootils_core/engine/kernel/graph/store.py:609-672` (`upsert_edge`) — le lookup non filtré sur `active` qui dépend d'`idx_edges_composite_lookup`.
- `src/ootils_core/engine/kernel/allocation/engine.py:287` — l'appelant chaud d'`upsert_edge` (une fois par arête `pegged_to`).
- `docs/ADR-012-scenario-fork-bulk.md` — le chantier précédent sur le même chemin (O(N) requêtes → O(1) requêtes), dont ce chantier réduit maintenant le coût unitaire par requête.
- `scripts/bench_scenario_fork.py` — harness de bench existant (échelles au-delà de `bench_s`, non rejoué dans ce chantier).
