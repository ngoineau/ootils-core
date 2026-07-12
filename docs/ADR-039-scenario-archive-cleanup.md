# ADR-039 — Purge des forks archivés et rétention des pénuries résolues (PURGE-1)

**Statut :** Accepté — chantier **PURGE-1**. Implémentation dans ce worktree (`C:\dev\worktrees\feat-purge1`), non encore mergée sur `main`.
**Date :** 2026-07-12
**Auteurs :** ootils-core team
**Contexte mesuré :** [ADR-011](ADR-011-scenario-retention.md) « Reste à faire » (« New ADR — `ADR-NNN-scenario-archive-cleanup.md` — addressing what to do with archived scenarios over time ») ; migration `076_maintenance_purge.sql` ; `src/ootils_core/engine/maintenance/purge.py`.

---

## Contexte

L'ADR-011 (2026-05-21) a tranché : tout FK vers `scenarios(scenario_id)` est `ON DELETE RESTRICT` — un scénario ne peut **jamais** être hard-deleté par le code applicatif, seulement soft-deleté (`status='archived'`). C'était la bonne décision pour empêcher les lignes orphelines, mais elle laissait explicitement une dette ouverte (« Reste à faire », ADR-011) : un fork archivé retient pour toujours son sous-graphe deep-copié (nœuds, arêtes, pénuries, explications, overrides) sans mécanisme de purge. À l'échelle démo/pilote actuelle (`docs/SCALABILITY.md`) le volume reste gérable, mais chaque fork de scénario coûte un sous-graphe complet — le problème grandit avec le nombre de scénarios what-if créés par la flotte d'agents (les watchers scénario-backés de l'ADR-025 créent un fork PAR RUN, jamais nettoyé).

Deux nettoyages distincts étaient conflés dans le cadrage initial et ont dû être séparés :

1. **La purge de fork** — supprimer le sous-graphe d'un scénario archivé, jamais la ligne `scenarios` elle-même (le tombstone). C'est strictement le follow-up nommé par l'ADR-011.
2. **La rétention des pénuries résolues** — un problème voisin mais orthogonal : la table `shortages` (ADR-021) est append-only par `calc_run_id` (`ON CONFLICT (pi_node_id, calc_run_id) DO UPDATE`), donc même un scénario **vivant** et jamais archivé accumule des lignes `resolved` d'anciens runs indéfiniment. Ce chantier tenait les deux nettoyages dans le même module car ils partagent la même infrastructure (audit trail, event typé, garde TTL), mais ce sont deux sweeps **indépendants**, jamais couplés dans leur éligibilité.

Deux invariants du dépôt contraignaient fortement la conception :

- **ADR-005 (insert-only sur `events`)** : la table `events` est conceptuellement insert-only pour son payload. Purger le sous-graphe d'un scénario implique de supprimer SES propres lignes `events` (elles perdent leur raison d'être une fois le scénario purgé) — un cas non prévu par l'ADR-005 originale, qui doit être amendé explicitement plutôt que silencieusement contourné.
- **ADR-021 (vérité de pénurie unique)** : `ShortageDetector` est l'écrivain exclusif de `shortages` pour les lignes actives et leur cycle de vie de résolution (`resolve_stale`). Un module de maintenance qui supprime de vieilles lignes `resolved` ne doit **jamais** devenir un second écrivain de la vérité de pénurie — seulement un GC borné sur de l'historique déjà mort.

L'architecte a posé le contrat suivant avant l'implémentation : purge du payload + tombstone (jamais de hard-DELETE de `scenarios`, les FK RESTRICT de l'ADR-011 restent intactes) ; TTL 7 jours par défaut pour l'archive (🎯 pilote) ; rétention 30 jours pour les pénuries résolues (🎯 pilote) ; dry-run par défaut avec double garde avant tout `--apply` ; CLI + timer en V1, endpoint HTTP d'apply différé à une V2 ; whitelist de tables curée à la main + garde CI qui la garantit exhaustive ; un event typé par run ; l'invariance des réponses baseline (`/v1/issues`, `compare_scenarios`, l'évaluateur ADR-030) prouvée par test.

## Décision

### 1. Deux sweeps indépendants, un seul module

`src/ootils_core/engine/maintenance/purge.py` implémente deux capacités séparées, chacune avec son propre couple `plan_*` (SELECT-only) / `apply_*` (seul écrivain, ne commite jamais — l'appelant possède la transaction) :

- **`plan_fork_purge` / `apply_fork_purge`** — éligibilité : `status='archived'`, `is_baseline=FALSE`, `purged_at IS NULL`, `archived_at IS NOT NULL` et plus ancien que `--ttl-days` (défaut **7 jours**, 🎯 pilote), évalué contre l'horloge de la DB (`SELECT now()`), jamais celle de l'appelant. `apply_fork_purge` supprime le sous-graphe du fork à travers `PURGE_WHITELIST` (voir §2) puis stamp `scenarios.purged_at` — la ligne `scenarios` elle-même **n'est jamais supprimée** (le tombstone de l'ADR-011).
- **`plan_shortage_retention` / `apply_shortage_retention`** — éligibilité : `shortages.status='resolved'` (jamais `'active'`, littéral codé en dur, jamais un paramètre) ET `updated_at` plus vieux que `--retention-days` (défaut **30 jours**, 🎯 pilote) ET **hors** du dernier `calc_run` `completed` du scénario (un sentinel UUID tout-zéro via `COALESCE` garde éligible un scénario sans aucun run complété, plutôt que de l'exclure silencieusement par accident). Ce sweep n'est **pas** scopé aux scénarios archivés — il tourne sur tout scénario, vivant ou archivé.

Les deux `apply_*` re-vérifient **toutes** les gardes absolues sur des données **fraîchement relues** en base, jamais sur le plan reçu en paramètre — un plan peut être périmé (construit il y a plusieurs minutes, ou même construit à la main par un appelant hostile) ou une course concurrente peut avoir changé l'état entre le plan et l'apply. Une violation de garde authentique lève `PurgeGuardError` et interrompt tout l'appel. Le seul cas traité comme un no-op idempotent (pas une exception) est un scénario déjà purgé (`purged_at` déjà stampé) ou déjà rien à supprimer côté rétention — jamais une seconde ligne d'audit, jamais un second event.

### 2. `PURGE_WHITELIST` — l'ordre FK-safe, garanti exhaustif par CI

La purge de fork supprime, dans cet ordre précis (déterminé en lisant chaque migration qui déclare un FK vers `scenarios` directement ou transitivement via `nodes`/`calc_runs`/`explanations`) : `causal_steps`, `shortages`, `dirty_nodes`, `scenario_diffs`, `scenario_overrides`, `explanations`, `edges`, `ghost_nodes`, `events`, `nodes`, `projection_series`, `calc_runs`, `scenario_planning_overrides`. L'ordre existe pour que supprimer une table avant une autre qui la référence encore ne lève jamais `ForeignKeyViolation` — le module docstring de `purge.py` documente le raisonnement complet table par table, y compris deux cas non triviaux : `nodes.parent_node_id` (auto-référence de pegging MRP, migration 024) est nullifié par un `UPDATE` dédié **avant** le `DELETE FROM nodes` (une auto-référence bulk-deleted en un seul statement risque un échec RI selon l'ordre de traitement interne) ; `ghost_members` n'est **jamais** un DELETE direct (son FK vers `ghost_nodes` est `ON DELETE CASCADE`, migration 011) mais son compte est quand même affiché dans le plan pour la visibilité opérateur.

**Garde CI permanente : `tests/test_purge_whitelist_guard.py`.** Ce test re-dérive, en parsant tous les fichiers de migration sur disque (aucune DB requise — respecte le principe des tests purs de CLAUDE.md), l'ensemble complet des tables portant une colonne `scenario_id` (directe ou, pour `explanations`/`causal_steps`, indirecte via `calc_runs`), en respectant la sémantique réelle de Postgres pour `CREATE TABLE IF NOT EXISTS` sur une table déjà suivie (no-op) et `DROP TABLE` (reset du suivi — c'est ce qui exclut correctement `zone_transition_runs`, créée avec `scenario_id` en migration 002 puis recréée SANS en migration 003). Chaque table découverte doit être **soit** dans `PURGE_WHITELIST` **soit** dans `PURGE_EXEMPT_TABLES` avec une justification explicite d'au moins 20 caractères — sinon le build échoue. C'est le garde-fou qui empêche qu'une future migration ajoute une table scénario-scopée que la purge oublie silencieusement de traiter.

`PURGE_EXEMPT_TABLES` exclut délibérément, en V1, toute la **famille d'audit gouverné** (`recommendations`, `agent_runs`, `parameter_recommendations`, `dq_findings`, `eando_recommendations`, `forecast_drift_recommendations`, `scenario_promotions`) — un fork purgé ne doit jamais effacer la trace de gouvernance/accountability qui alimente les KPI de preuve ADR-030 (`recommendation_outcomes → recommendations`). `inventory_snapshots` est exclue car baseline-only par construction (ADR-030) — un candidat de purge est par définition `is_baseline=FALSE`, donc structurellement inapplicable. Les tables de sous-systèmes parallèles encore en évolution (`mrp_runs`, `forecasts`, `pyramide_runs`/`pyramide_snapshots`, `mps_*`, `routing_requires_capacity_edges`, `planned_supply`, `customer_order_demand`) sont hors du périmètre V1 — la whitelist cible le substrat cœur du moteur de propagation (nœuds/arêtes/pénuries/explications/dirty-flags/overrides), pas l'historique de run des sous-systèmes MRP/forecast.

### 3. L'amendement ADR-005 : le carve-out `events` du fork purgé

La purge d'un fork supprime aussi les lignes `events` de CE scénario — un cas que l'insert-only de l'ADR-005 (Décision 4, point 4 : « Make `events` insert-only from day one ») n'anticipait pas. La justification : les events d'un fork purgé sont un payload **régénérable** dans le sens où leur seule raison d'exister était d'auditer le cycle de vie d'un scénario qui n'existe plus (hormis son tombstone `purged_at`) — contrairement aux events de la baseline ou d'un scénario vivant, qui restent le journal d'audit permanent visé par l'ADR-005. Une seule exception à cette suppression : l'event de confirmation `purge_executed` lui-même est émis **après** le sweep de la table `events`, donc il survit à sa propre purge — c'est la seule ligne `events` d'un fork purgé qui subsiste, et elle documente précisément que la purge a eu lieu (voir l'amendement daté en fin de fichier ADR-005).

### 4. L'amendement ADR-021 : le GC délégué des pénuries résolues

`ShortageDetector` reste l'écrivain exclusif de `shortages` pour tout ce qui touche à la vérité de pénurie : la création des lignes actives, leur valorisation, et leur cycle de vie de résolution (`resolve_stale`). `apply_shortage_retention` n'ajoute **aucune** sémantique de pénurie — c'est un DELETE pur sur de l'historique déjà mort (`status='resolved'` uniquement, jamais `'active'`), qui ne touche jamais le dernier `calc_run` complété d'un scénario (la vue courante et auditable la plus récente reste toujours intacte). C'est un GC borné délégué à `engine/maintenance`, pas un second écrivain de la vérité — voir l'amendement daté en fin de fichier ADR-021.

### 5. Event typé, audit trail, invariance prouvée

Chaque `apply_*` réussi (jamais un no-op idempotent) écrit exactement une ligne `maintenance_purge_runs` (`run_id`, `scenario_id`, `mode='apply'`, `ttl_days`/`retention_days`, `per_table_counts` en JSONB — carve-out diagnostic, jamais requêté par clé, voir migration 076 §JSONB — `rows_deleted_total`, `executed_by`) et émet exactement un event `purge_executed` (ADR-027 : granularité par RUN, jamais par ligne supprimée), avec `field_changed` distinguant `'fork_purge'` de `'shortage_retention'`. `executed_by` est obligatoire (`ValueError` sinon) — l'attribution d'audit n'est jamais optionnelle.

**L'invariance est prouvée par test, pas seulement affirmée** (`tests/integration/test_purge_integration.py`, contrat 3) : trois chemins de lecture baseline canoniques sont capturés avant/après la purge d'un fork archivé — `ShortageDetector.get_active_shortages` sur la baseline, `compare_scenarios` (ADR-034) sur deux scénarios **vivants**, et l'évaluateur ADR-030 (`evaluate_and_persist`, dont la classification est une fonction pure de ses SELECTs sur pénuries/snapshots/recommandations baseline) — égalité structurelle stricte, pas seulement des comptes.

### 6. Dry-run par défaut, double garde avant apply, CLI + timer V1

`scripts/purge_maintenance.py` exécute les deux planners par défaut (aucune écriture) et n'applique quoi que ce soit que si `--apply` est passé **et** que `OOTILS_PURGE_ENABLED` vaut exactement `'1'` (toute autre valeur, y compris absente, refuse `--apply` avant même d'ouvrir une connexion) — la double garde. Le routeur HTTP `GET /v1/maintenance/purge-preview` (`api/routers/maintenance.py`, scope `admin`, même kill switch) est **volontairement en lecture seule** : aucun endpoint HTTP d'apply n'existe. Décision de l'architecte : une suppression multi-tables destructive et ordonnée-FK est un travail d'opérateur via CLI, jamais une seule requête POST. L'exécution récurrente en production (cron ou systemd timer invoquant le CLI en mode `--apply`) est la cadence opérationnelle visée mais **n'est pas livrée dans ce PR** — seul le CLI existe ; le fichier de timer/unit reste un suivi (voir Conséquences).

## Alternatives rejetées

- **Hard-DELETE de `scenarios` avec `ON DELETE CASCADE`.** Rejeté — reviendrait sur la décision de l'ADR-011 (Option A, `RESTRICT` partout) sans nouvel ADR qui la superséderait explicitement ; casserait aussi l'audit trail des recommandations/promotions qui référencent un scénario purgé.
- **Un seul TTL partagé pour la purge de fork et la rétention des pénuries.** Rejeté — les deux sweeps ont des sémantiques d'éligibilité disjointes (scope scénario archivé vs scope pénurie résolue sur tout scénario) ; les coupler forcerait un TTL unique qui n'a de sens pour ni l'un ni l'autre cas d'usage réel.
- **Laisser les watchers scénario écrire directement dans `shortages` pour leur propre nettoyage.** Rejeté — romprait l'invariant ADR-021 d'écrivain unique ; `apply_shortage_retention` est un module de maintenance séparé, jamais le detector lui-même.
- **Un endpoint HTTP `POST /v1/maintenance/purge` en V1.** Rejeté par l'architecte — trop de surface pour une action destructive multi-tables ; le CLI garde l'opérateur dans la boucle avec des logs explicites, un `--apply` qui doit être tapé consciemment, et pas de risque de déclenchement accidentel via un client HTTP mal configuré.
- **Faire confiance au plan reçu par `apply_*` sans re-vérification.** Rejeté — un plan est un SELECT figé dans le temps ; entre sa construction et son exécution, une course concurrente ou un plan périmé (voire construit à la main) pourrait faire passer une garde. Chaque `apply_*` relit l'état frais et revérifie tout.

## 🎯 Pilote

- **TTL fork = 7 jours, rétention pénuries résolues = 30 jours.** Valeurs par défaut raisonnables mais arbitraires — à recalibrer avec le volume réel de forks what-if créés par la flotte d'agents (ADR-025) une fois le pilote en production. Les deux sont des paramètres CLI/query, jamais des constantes gravées dans le moteur.
- **Cadence d'exécution récurrente (cron/systemd timer).** Le CLI existe ; le mécanisme d'ordonnancement en production reste à choisir avec le pilote (fréquence, fenêtre de maintenance) — non tranché dans ce chantier.

## Conséquences

- **Positif :** referme la dette explicitement laissée ouverte par l'ADR-011 ; les forks what-if créés en masse par les watchers scénario-backés (ADR-025) ont enfin un chemin de nettoyage ; la garde CI (`test_purge_whitelist_guard.py`) rend impossible l'oubli silencieux d'une future table scénario-scopée ; l'invariance baseline est prouvée par test, pas seulement affirmée.
- **Négatif / dette assumée :**
  - Aucun endpoint HTTP d'apply — un opérateur doit avoir un accès shell/CLI pour purger, ce qui est un choix délibéré mais un frein pour une V2 self-service.
  - Le mécanisme d'ordonnancement récurrent (systemd timer / cron) n'est **pas livré** dans ce PR — seul le CLI manuel existe ; jusqu'à ce qu'un timer soit câblé, la purge reste une action manuelle.
  - `PURGE_EXEMPT_TABLES` exclut délibérément la famille d'audit gouverné et les sous-systèmes MRP/forecast/Pyramide/MPS/CRP — un fork purgé continue donc de laisser des traces indirectes dans ces tables (ex. `recommendations.target_node_id` mis à `NULL` par cascade, `pyramide_snapshot_demand_nodes` cascade-deleted) ; documenté explicitement dans le module, pas un oubli.
- **Reste à faire :** wiring du timer de production ; étendre potentiellement la whitelist au sous-système MPS/CRP/ATP une fois ses propres sémantiques de cycle de vie scénario confirmées (PURGE-2, hors scope de ce chantier) ; envisager une politique de rétention séparée pour les DRAFT/EXPIRED de la famille d'audit gouverné (mentionné comme piste dans `PURGE_EXEMPT_TABLES["recommendations"]`, explicitement pas ce chantier).

## Code references

- `src/ootils_core/engine/maintenance/purge.py` — module entier ; `PURGE_WHITELIST` (lignes 133-147), `PURGE_EXEMPT_TABLES` (lignes 157-275).
- `src/ootils_core/engine/maintenance/purge.py:405-452` — `plan_fork_purge` (SELECT-only, éligibilité TTL).
- `src/ootils_core/engine/maintenance/purge.py:455-487` — `_verify_purge_guards` (les gardes absolues, revérifiées sur données fraîches).
- `src/ootils_core/engine/maintenance/purge.py:614-648` — `apply_fork_purge` (le seul écrivain de la purge de fork).
- `src/ootils_core/engine/maintenance/purge.py:590-599` — l'émission de l'event `purge_executed` APRÈS le sweep `events` (le carve-out ADR-005 amendé).
- `src/ootils_core/engine/maintenance/purge.py:708-741` — `plan_shortage_retention`.
- `src/ootils_core/engine/maintenance/purge.py:812-850` — `apply_shortage_retention` (le GC délégué ADR-021 amendé).
- `src/ootils_core/engine/maintenance/__init__.py` — exports du package.
- `src/ootils_core/db/migrations/076_maintenance_purge.sql` — `scenarios.archived_at`/`purged_at`, `events.event_type += 'purge_executed'`, `maintenance_purge_runs`.
- `src/ootils_core/api/routers/maintenance.py` — `GET /v1/maintenance/purge-preview` (scope `admin`, kill switch `OOTILS_PURGE_ENABLED`, lecture seule, pas d'endpoint d'apply).
- `scripts/purge_maintenance.py` — le CLI (dry-run par défaut, `--apply` doublement gardé).
- Tests : `tests/test_purge_whitelist_guard.py` (garde CI d'exhaustivité), `tests/test_purge_engine_pure.py` (gardes pures + validation fail-fast), `tests/integration/test_purge_integration.py` (cycle de vie complet, gardes, invariance, rétention, surface HTTP).
- `docs/ADR-011-scenario-retention.md` — le follow-up nommé, clos par cet ADR.
- `docs/ADR-005-storage-layer.md` — amendé (carve-out events du fork purgé).
- `docs/ADR-021-shortage-truth.md` — amendé (GC délégué des pénuries résolues).
