# ADR-034 — Comparaison multi-scénarios en KPI métier (SC-1)

**Statut :** Accepté — chantier **SC-1** (`docs/ROADMAP-AGENTS-2026-H2.md` §SC-1, « différenciateur démo majeur »). Implémentation dans ce worktree (`feat/sc1-scenario-compare`), non encore mergée sur `main`.
**Date :** 2026-07-08
**Auteurs :** ootils-core team
**Contexte mesuré :** `docs/ROADMAP-AGENTS-2026-H2.md` §SC-1 (« le diff ne compare que 6 champs bruts de nœuds ; aucune réponse à "le scénario B réduit-il la rupture de X % et le stock de Y € vs A ?" — C2 de la revue précédente, toujours ouvert ») ; l'implémentation `src/ootils_core/engine/scenario/compare.py` et `src/ootils_core/api/routers/scenarios.py` (`GET /v1/scenarios/compare`).

---

## Contexte

`GET /v1/scenarios/{id}/diff` (chantier #341b) ne compare que des champs bruts de nœuds (6 champs, valeurs texte avant/après) entre un fork et la baseline. Il ne répond à aucune question métier : combien de pénuries en moins, combien de $ de stock en plus ou en moins, quel fill-rate estimé, pour UN scénario face à un ou plusieurs autres. Or la comparaison multi-scénarios en KPI $ est exactement ce qu'une démo doit montrer pour concurrencer un APS établi (Kinaxis/o9) — deux forks rankés en $ côte à côte, pas un diff de champs.

Le North Star exige que toute capacité de lecture soit **scénario-scopée** (chaque scénario a ses propres pénuries, son propre stock projeté) et que toute sortie consommée par un agent de gouvernance porte une preuve, jamais une valeur masquée. Deux pièges connus du dépôt guidaient la conception :

- **La leçon #347** (ADR-025) : deux implémentations de la même précédence de coût qui divergent silencieusement est une catégorie de bug déjà vécue et corrigée ailleurs — toute nouvelle lecture de coût doit s'ancrer, pas réinventer.
- **La leçon ADR-030** : `inventory_snapshots` est baseline-only par construction (un fork est simulé, jamais observé) — l'utiliser comme source de stock pour un scénario non-baseline serait un mensonge silencieux (le snapshot n'existerait tout simplement pas pour ce scénario, ou pire, refléterait la baseline).
- **La contrainte "sans migration"** posée par le cadrage initial (ROADMAP §SC-1 : « flag `stale` calculé... sans migration ; statut `stale` en schéma si l'architecte le valide ») : l'architecte n'a **pas** validé de statut schéma — le calcul du `stale` devait donc rester dérivé, pas stocké.

Le cadrage de ce chantier a été interrompu trois fois par surcharge API avant que l'architecte ne puisse re-confirmer quatre points factuels (nom exact de la table de coût, exploitabilité des `outflows` sur ProjectedInventory, convention 503 vs 404 du kill switch, type d'event réel émis par un promote baseline). Ces quatre points ont été vérifiés directement contre le code avant l'écriture de ce chantier — voir chaque sous-section ci-dessous pour la confirmation et sa source.

## Décision

### 1. KPI par scénario — sources canoniques, une par KPI

**(a) Pénuries — table `shortages`, scopée au dernier calc_run complété.** `shortage_count`/`below_safety_stock_count`/`shortage_severity_usd` viennent de `shortages` avec le prédicat `scenario_id = :id AND status = 'active' AND calc_run_id = _latest_calc_run(:id)`. Le filtre `calc_run_id` est **obligatoire** : `shortages` est append-only par run (`ON CONFLICT (pi_node_id, calc_run_id) DO UPDATE` — une ligne par run, pas une ligne courante par PI), donc une somme non-scopée empile tous les runs historiques. `shortage_count` = `COUNT(*) FILTER (severity_class='stockout')` ; `below_safety_stock_count` est exposé à part (jamais fondu dans `shortage_count`) ; `shortage_severity_usd` = `SUM(severity_score)`. Ces trois figures sont **0-honnêtes**, pas None-honnêtes : zéro pénurie active sur ce calc_run est un vrai zéro (scénario sain), pas un trou de données — `severity_score` est `NOT NULL` par construction des deux writers de `shortages` (ADR-021). `_latest_calc_run` est le résolveur déjà utilisé par `/diff` (`engine/scenario/manager.py:658`), réutilisé tel quel — jamais réimplémenté.

**(b) `stock_value_usd` — nœuds `ProjectedInventory` du scénario, moyenne sur l'horizon.** `SUM(GREATEST(closing_stock, 0) * unit_cost)` par bucket, moyenné sur le nombre de buckets distincts de l'horizon (une somme brute sur le temps grandirait avec la longueur de l'horizon et ne voudrait rien dire ; le total par bucket, moyenné, est la figure « $ immobilisés en stock »). `nodes` (jamais `inventory_snapshots`) est la source — la table est mise à jour en place par la propagation (une ligne courante par coordonnée PI, pas d'empilement par calc_run), donc aucun filtre `calc_run_id` n'est nécessaire ni appliqué ici, contrairement à `shortages`.

**Précédence de coût — vérifiée contre `propagator_sql.py:262-274` avant écriture (point de vérification a) :**
```sql
COALESCE(sup.unit_cost, i.standard_cost, 1)::numeric AS unit_cost
```
Confirmé : la table est bien `supplier_items` (fournisseur préféré, ligne au prix le plus bas, `unit_cost > 0`) puis `items.standard_cost`, exactement le nom et l'ordre que la note de cadrage supposait. La requête de `compare.py` **mirrore structurellement** cette LATERAL (mêmes tables, mêmes colonnes, même `ORDER BY is_preferred DESC, unit_cost ASC`) mais **sans** le `, 1` final de `SHORTAGES_SQL` — ce `, 1` est un proxy « unpriced-item » qui masquerait un item non-tarifé derrière un coût fictif de 1, acceptable pour prioriser une pénurie mais inacceptable pour un $ affiché tel quel dans une comparaison de scénarios. Ici : `COALESCE(sup.unit_cost, i.standard_cost)` sans troisième terme — un `unit_cost` NULL contribue `0` à la somme et incrémente `unpriced_count`, jamais un $ masqué.

`basis_count` (`stock_value_basis_count`) = nombre de coordonnées PI avec un `closing_stock` calculé (non-NULL) — un nœud jamais propagé n'a pas de figure $ valide à apporter et est exclu en amont par le SQL (`pi.closing_stock IS NOT NULL`). `unpriced_count` est le sous-ensemble de cette base dont `unit_cost` résout à NULL.

**(c) `fill_rate_est` — `1 − SUM(shortage_qty stockout) / SUM(outflows PI)` sur l'horizon.**

**Vérifié avant écriture (point de vérification b) :** `nodes.outflows` existe et est persisté directement sur les lignes `ProjectedInventory` par le kernel de propagation — c'est une consommation par-bucket directement exploitable, sans besoin de redériver depuis les nœuds `Demand`. Aucun repli sur `Demand` n'a été nécessaire.

None-honnête : demande totale (somme des `outflows`) `<= 0` → `None`, `basis_count=0` — **jamais** un `1.0` masqué (une absence de demande n'est pas un fill-rate parfait, c'est une absence de donnée). Le `basis_count` est forcé à `0` dans la branche `None` par construction (des `outflows` non-négatifs sommant à plus de zéro impliquent au moins un bucket positif) — défensif, pas dérivé, pour que le déclencheur `None` et son `basis_count` ne puissent jamais diverger même sur une entrée malformée.

**NON à `inventory_snapshots` comme source (a), (b) ou (c) — raison ADR-030.** Les snapshots sont **baseline-only par nature** (ADR-030 §1 : « un outcome est le réel observé ; il est toujours baseline. Un fork est simulé, pas observé »). Utiliser `inventory_snapshots` pour le stock d'un fork serait soit une absence de données (le fork n'a jamais été snapshoté), soit — pire — une lecture accidentelle de l'état baseline présentée comme celui du fork : un mensonge silencieux exactement de la forme que ce chantier doit éviter. `nodes` (mis à jour en place par la propagation, scénario-scopé par construction) est la seule source qui reflète honnêtement l'état d'un fork.

### 2. `stale` — calculé, sans migration, sans nouveau statut schéma

**Vérifié avant écriture (point de vérification d) :** le type d'event réellement émis par un `promote` baseline. Confirmé dans `engine/scenario/manager.py:832-850` — `promote()` insère un event `event_type='scenario_merge'`, `scenario_id=_BASELINE_ID` (jamais le scénario source ; le scénario source est encodé dans `old_text`). Confirmé dans les CHECK constraints des migrations 002, 006, 051, 062 et 071 : `'scenario_merge'` fait partie de la liste `events.event_type` autorisée dans **toutes**, y compris la plus récente (071) — le type n'a jamais été retiré ni renommé. Le type de contrat de cadrage (« type à confirmer contre les enums 002/051/071 ») est donc exact : `'scenario_merge'` est réel, stable, et scopé à la baseline.

```
stale = (MAX(events.created_at) WHERE scenario_id=BASELINE AND event_type='scenario_merge'
          est POSTÉRIEUR au completed_at du _latest_calc_run de CE fork)
        OU (le calc_run le plus récent — TOUT statut — de CE fork a status='completed_stale')
```

Aucun `promote` jamais exécuté → `latest_merge_event_at` est `None` → la première branche est `False` par construction ; rien ici n'exige une nouvelle colonne. La deuxième branche lit `calc_runs.status='completed_stale'` (déjà posé par `CalcRunManager.complete_calc_run`, déclenché quand `scenarios.baseline_snapshot_id` est peuplé) via une **seconde requête indépendante**, non filtrée à `status='completed'` — parce que le calc_run porteur des KPI (résolu via `_latest_calc_run`, filtré `'completed'` uniquement par construction) ne peut par définition jamais être lui-même `'completed_stale'` ; le signal `completed_stale` ne peut être observé que sur la ligne la-plus-récente-tout-statut du fork, une requête séparée de celle qui alimente les KPI.

**NON à un statut `stale` en schéma — raison : non validé par l'architecte.** Le cadrage ROADMAP §SC-1 posait la question explicitement ouverte : « statut `stale` en schéma si l'architecte le valide ». L'architecte n'a **pas** validé de nouvelle colonne/statut avant l'interruption du cadrage ; en l'absence de validation, la règle de gouvernance de ce dépôt (« ne jamais documenter/construire une capacité que l'architecte n'a pas explicitement actée ») s'applique : `stale` reste un **booléen dérivé au moment de la lecture**, jamais persisté. C'est également cohérent avec le commentaire déjà présent dans `manager.py:852-855` sur l'invalidation des scénarios frères : « Logged as a logical invalidation (schema-level 'stale' status: future work) » — le manager lui-même documente cette même capacité comme non actée.

### 3. Forme de chaque entrée

Chaque entrée de la réponse porte `calc_run_id`, `computed_at` (= `completed_at` de ce calc_run), `stale`, `parent_scenario_id`, `computable`. `comparable` (top-level) = **tous** les scénarios demandés sont à la fois `computable=True` **et** `stale is False` — pas `not stale`, qui serait vrai aussi pour `stale=None` (l'entrée non-`computable`) en Python. La précédence de coût utilisée pour `stock_value_usd` est citée en toutes lettres dans la réponse (`cost_precedence`), pour qu'un consommateur (agent de gouvernance ou humain) puisse auditer la méthode sans relire le code.

Les **deltas** se calculent contre **une seule** entrée de référence par requête : la baseline si elle figure parmi les `ids` demandés, sinon le premier `id` passé par l'appelant (`resolve_reference_scenario_id`) — jamais une matrice N×N par paire. `shortage_count_delta`/`severity_usd_delta` sont toujours des nombres réels dès que les deux côtés ont des `kpis` (ces deux sous-KPI sont 0-honnêtes, jamais `None` eux-mêmes) ; `stock_value_usd_delta`/`fill_rate_delta` peuvent individuellement être `None` quand le KPI d'un des deux côtés est `None` (stock non-tarifé / demande nulle).

### 4. Endpoint `GET /v1/scenarios/compare?ids=a,b,c`

Enregistré **avant** `/{scenario_id}` dans le routeur (FastAPI/Starlette commet à la première route qui matche par ordre de déclaration — sans cela, `compare` serait avalé par le paramètre de chemin `{scenario_id}` et échouerait la validation UUID générique avant même d'atteindre ce handler).

- **Scope `read`** — lecture pure, **aucune** écriture `events`/audit (contrairement à `promote`, une action d'état). Bornes `2..5` ids inclus, `422` sinon.
- **ID malformé ou inconnu → 422 nommant l'id exact**, message écrit à la main (`ScenarioCompareError.detail`), zéro fuite psycopg/DSN — même famille de carve-out que `DiffError`/`ApprovalError`/`RejectionError` (`staging.py:243,325,390`).
- **Fork sans calc_run complété** (`ScenarioManager._latest_calc_run` lève `ValueError`) → l'exception est attrapée **par scénario**, dans la boucle de construction des entrées : cette entrée est présente avec `kpis=None`, `computable=False`, et une `note` (le texte de la `ValueError`, réutilisé tel quel) — le reste de la réponse n'est pas affecté. Ce n'est pas une erreur de requête, c'est un fait sur un scénario particulier.
- **Scénarios archivés et baseline sont des entrées ordinaires** — aucun filtre de statut ; SC-1 sert justement à comparer des scénarios de tout statut (y compris baseline elle-même comme point de référence).

**Kill switch `OOTILS_SCENARIO_COMPARE_ENABLED`, défaut ON.**

**Vérifié avant écriture (point de vérification c) :** la convention exacte de `api/routers/outcomes.py:85-100` (`_outcomes_enabled()` / `require_outcomes_enabled()`) — falsy → **`503`** (`HTTP_503_SERVICE_UNAVAILABLE`), vérifié **après** auth/scope mais **avant** `Depends(get_db)` (les dépendances FastAPI se résolvent dans l'ordre de la signature et court-circuitent sur la première `HTTPException` ; auth d'abord pour qu'un appelant non-authentifié reçoive toujours 401 sans pouvoir sonder l'état du switch, kill-switch avant la DB pour qu'un comparateur désactivé réponde sans toucher au pool). `require_scenario_compare_enabled()` (`api/routers/scenarios.py:72-84`) reproduit **exactement** ce motif — 503, pas 404 (un 404 laisserait croire que la route n'existe pas ; 503 dit honnêtement « existe, temporairement coupée »).

## Alternatives rejetées

- **`inventory_snapshots` comme source de `stock_value_usd`.** Rejeté — baseline-only par nature (ADR-030) ; lire les snapshots pour un fork produirait soit une absence de données soit une lecture accidentelle de l'état baseline. `nodes` (scénario-scopé, mis à jour en place) est la seule source honnête pour un fork.
- **Un nouveau statut/colonne `stale` en schéma.** Rejeté — non validé par l'architecte (le cadrage posait la question explicitement ouverte, jamais tranchée avant l'interruption). Calculé au moment de la lecture à partir de deux signaux déjà existants (`events.event_type='scenario_merge'`, `calc_runs.status='completed_stale'`) — zéro migration.
- **Une fonction Python partagée pour la précédence de coût, importée par `compare.py` et `propagator_sql.py`.** Envisagée pour satisfaire à la lettre « FACTORISÉE » (leçon #347) mais rejetée dans le périmètre de ce chantier : `propagator_sql.py` est explicitement documenté comme byte-identique / testé en parité contre le noyau Python (`scripts/parity_sql_vs_python.py`) et se trouve hors du périmètre de livraison à deux fichiers de ce chantier (`engine/scenario/compare.py` + `api/routers/scenarios.py`) ; y toucher pour assembler du SQL déplacerait un risque de régression sur un chemin critique (propagation) pour zéro changement de comportement de son côté. **Choix retenu à la place** : un miroir structurel littéral, ancré par un commentaire de numéro de ligne (`propagator_sql.py:262-274`), qui satisfait l'INTENTION de la leçon #347 (jamais un `COALESCE` divergent) sans le changement de fichier hors-périmètre. **Dette assumée, flaguée pour un suivi** : un futur chantier peut hisser les deux précédences dans un constructeur partagé, sur le modèle de `param_overlay.py:resolved_field_lateral_sql` (le précédent déjà établi pour ce type exact de factorisation).
- **Garder le `, 1` de repli de `SHORTAGES_SQL` dans le calcul de `stock_value_usd`.** Rejeté — ce repli masque un item non-tarifé derrière un coût fictif ; acceptable pour prioriser une pénurie interne, inacceptable pour un $ affiché directement dans une comparaison de scénarios consommée par un humain ou un agent de gouvernance. `unpriced_count` rend le trou visible plutôt que de le maquiller.
- **Une matrice de deltas par paire (N×N).** Rejeté — le contrat demande une seule référence (baseline si présente, sinon le premier id) ; une matrice complique l'API et la lecture pour un gain non demandé par le cadrage.
- **404 pour le kill switch.** Rejeté — 404 laisserait croire que la route n'existe pas. 503 (mirroré sur `outcomes.py`) dit honnêtement « la route existe, elle est temporairement coupée ».
- **`all(not e.stale for e in entries)` pour `comparable`.** Rejeté — `not None` est vrai en Python, donc une entrée non-`computable` (`stale=None`) passerait silencieusement le test. `e.stale is False` (littéral) est requis.

## 🎯 Pilote

- **Grain de `stock_value_usd` = moyenne sur les buckets de l'horizon (pas une somme sur le temps).** Choix documenté et raisonné (une somme temporelle grandirait avec la longueur d'horizon et perdrait son sens de figure « $ immobilisés »), mais **tunable** : un pilote pourrait préférer une figure de fin de période, un pic, ou une fenêtre glissante différente. À recalibrer avec le retour terrain démo, pas une constante métier gravée dans le marbre.
- **Résolution de la référence des deltas (baseline si présente, sinon le premier id).** Raisonnable par défaut mais arbitraire dans le cas général à N scénarios sans baseline parmi eux — un pilote pourrait vouloir désigner explicitement la référence via un paramètre de requête. Non implémenté en V1 (hors du contrat de cadrage), à considérer si la démo le réclame.

## Conséquences

- **Positif :** la démo peut enfin montrer 2+ forks rankés en $ côte à côte — pénuries, valeur de stock, fill-rate — avec deltas contre une référence commune, sans toucher au diff brut de champs existant. Lecture pure, zéro risque d'effet de bord (aucune écriture `events`/audit), donc zéro impact sur l'auditabilité existante.
- **Négatif / dette assumée :**
  - La précédence de coût est **mirrorée**, pas **partagée** avec `SHORTAGES_SQL` — deux textes SQL à synchroniser manuellement si l'un des deux évolue (atténué par le commentaire d'ancrage de ligne, mais reste une dette réelle par rapport à l'intention "FACTORISÉE" du contrat).
  - `stock_value_usd`/`fill_rate_est` par scénario ne portent pas de `calc_run_id` figé dans leur requête SQL (contrairement aux pénuries) — ils lisent l'état **courant** de `nodes`, qui peut avoir avancé depuis le `calc_run_id` retourné dans l'entrée si une propagation a tourné entre-temps sans que `_latest_calc_run` ait changé de valeur retournée pour les pénuries. C'est cohérent avec le fait que `nodes` est mis à jour en place (documenté dans le module), mais un consommateur strict voudrait le savoir : `computed_at` reflète le calc_run des **pénuries**, pas nécessairement l'instant exact des deux autres KPI.
  - Aucun test d'intégration n'existe encore dans ce worktree au moment de l'écriture de cet ADR (chantier en cours, PR non finalisée) — voir Code references pour les fichiers de test à écrire/vérifier avant merge.
- **Reste à faire :** hisser la précédence de coût partagée dans un constructeur commun (sur le modèle `param_overlay.py:resolved_field_lateral_sql`) ; considérer un paramètre de référence explicite pour les deltas si la démo le réclame ; page de comparaison server-rendered évoquée par EXP-1 (ROADMAP §EXP-1) consommant cet endpoint.

## Code references

- `src/ootils_core/engine/scenario/compare.py` — module entier (fonctions pures + requêtes DB + orchestration `compare_scenarios`).
- `src/ootils_core/engine/scenario/compare.py:135-141` — `COST_PRECEDENCE`, la précédence de coût citée en toutes lettres dans la réponse.
- `src/ootils_core/engine/scenario/compare.py:336-352` — `compute_stale`, la formule de fraîcheur dérivée.
- `src/ootils_core/engine/scenario/compare.py:378-388` — `compute_comparable`, le garde `stale is False`.
- `src/ootils_core/api/routers/scenarios.py:67-284` — l'endpoint `GET /v1/scenarios/compare`, le kill switch, les modèles de réponse.
- `src/ootils_core/engine/orchestration/propagator_sql.py:262-274` — `SHORTAGES_SQL`, la précédence de coût source (avec le `, 1` de repli, volontairement non repris ici).
- `src/ootils_core/engine/scenario/manager.py:658` — `ScenarioManager._latest_calc_run`, réutilisé verbatim.
- `src/ootils_core/engine/scenario/manager.py:832-850` — `promote()`, l'écriture réelle de l'event `scenario_merge` (`scenario_id=_BASELINE_ID`).
- `src/ootils_core/api/routers/outcomes.py:85-100` — `_outcomes_enabled()`/`require_outcomes_enabled()`, le motif de kill switch mirroré (503).
- Migrations : `db/migrations/002_sprint1_schema.sql`, `006_m5_scenarios.sql`, `051_recommendation_transition_event.sql`, `062_node_firm_event.sql`, `071_events_fleet_types.sql` — CHECK `events.event_type` incluant `'scenario_merge'` dans toutes.
