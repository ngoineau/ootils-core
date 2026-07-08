# ADR-033 — Routage demande tête/traîne câblé + première exception demande (dérive de forecast)

**Statut :** Accepté — chantier DEM-1. **PR-1** (routage tête/traîne câblé en opt-in, [#438](https://github.com/ngoineau/ootils-core/pull/438)) mergée sur `main` ; **PR-2** (watcher `FORECAST_DRIFT` + migration 072) dans ce worktree.
**Date :** 2026-07-08
**Auteurs :** ootils-core team
**Contexte mesuré :** `docs/ROADMAP-AGENTS-2026-H2.md` §DEM-1 (le constat « bibliothèque inerte ») ; l'implémentation `src/ootils_core/pyramide/routing.py` (le routeur DB-free, déjà écrit), son câblage appelant `src/ootils_core/pyramide/repository.py` / `src/ootils_core/api/routers/pyramide.py`, la provenance `src/ootils_core/db/migrations/058_pyramide_routing_provenance.sql`, et le premier watcher demande `scripts/agent_forecast_watcher.py` + `src/ootils_core/db/migrations/072_forecast_drift_recommendations.sql`.

---

## Contexte

Le routeur tête/traîne du DESIGN Pyramide (`route()` / `classify()` / `SeriesFeatures`, `pyramide/routing.py` — 577 lignes testées, déterministe, DB-free) répondait au **mur d'échelle** décrit dans `docs/DESIGN-pyramide-forecasting.md` §5 : ne pas envoyer une traîne à faible signal sur un foundation model au leaf, mais la prévoir à un nœud agrégé + désagrégation MinT. Problème mesuré par la ROADMAP §DEM-1 : ce routeur n'était appelé **que par ses tests**. Le `HierarchicalRunner` consommait un `routing_decisions` que **rien** ne peuplait, et aucun run réel n'émettait ni ne traçait de décision de routage. Une capacité conçue mais inerte.

Symétriquement, la flotte d'agents ne comptait **aucun watcher côté DEMANDE**. Les tours de contrôle existantes (pénurie, matière, reschedule) sont toutes SUPPLY : elles observent le plan d'approvisionnement, jamais la qualité du forecast qui l'alimente. Or Pyramide **backteste** déjà chaque run (rolling-origin, WAPE/MASE/bias, migration 055) sans que personne ne surveille la dérive de cette précision dans le temps — un forecast qui se dégrade silencieusement empoisonne tout le MRP en aval.

DEM-1 ferme les deux trous d'un même geste : (1) **câbler** le routeur dans le chemin de run, en opt-in et rétro-compatible ; (2) livrer le **premier watcher demande**, `agent_forecast_watcher`, qui transforme la dérive de précision backtestée en recommandation gouvernée. La contrainte forte, héritée du North Star : le cœur reste déterministe (aucun LLM dans la décision de routage ni dans le verdict de dérive), tout est tracé/auditable, et rien ne casse le contrat existant.

## Décision

### 1. Le routage tête/traîne câblé — OPT-IN, défaut byte-identique

Le câblage est branché derrière un flag `auto_route` (défaut `False`) sur les deux surfaces de run Pyramide (`api/routers/pyramide.py` : le run mono-série `POST /v1/pyramide/runs` et le run hiérarchique). Un run qui ne le passe pas se comporte **exactement** comme avant DEM-1 — aucune colonne de provenance écrite, aucun changement de méthode exécutée, sortie byte-identique. Le routage ne s'active que sur demande explicite.

- **Features calculées par agrégats SQL, côté appelant.** `routing.py` reste **DB-free par contrat** : il ne voit qu'un `SeriesFeatures` que l'appelant lui tend. Ce contrat « le caller calcule » est respecté — les constructeurs de features vivent dans `repository.py` (`build_series_features` pour un leaf, `_build_node_features` pour un nœud agrégé, `build_routing_decisions` pour tout un bloc sommant), seuls endroits qui transforment les faits `demand_history` / `item_asp` en vecteur de features. Ils sont **alias-aware (ADR-031)** : le filtre par site passe par le point de résolution unique `_warehouse_codes_subquery()` (`external_id ∪ aliases`), jamais une égalité `warehouse_id = external_id` en dur.
- **Sémantique de provenance 058 : `routed_method` = recommandé, `method` = exécuté.** Les colonnes `routed_method` / `routed_level` / `routing_reason` (migration 058, CHECK all-or-none) enregistrent ce que le routeur a **recommandé** ; `pyramide_runs.method` reste le contrat **exécuté**. La recommandation ne remplace la méthode exécutée **que** si l'appelant a laissé un `AUTO_SELECT` non-opiniâtré ; **un choix de méthode explicite n'est jamais écrasé** — la provenance enregistre alors « le routeur aurait pris X » tout en exécutant le choix forcé (`api/routers/pyramide.py`, garde `if body.method == METHOD_AUTO_SELECT`). Additif, rétro-compatible.
- **Level `leaf` forcé en mono-série.** Le run mono-série n'a **pas** de hiérarchie : le seul niveau de forecast honnête est `leaf` (rien à désagréger). Le routeur y renvoie déjà `leaf` sur chaque branche (`aggregate_signal_ok` défaut `False`), mais on **épingle** explicitement `LEVEL_LEAF` plutôt que de risquer un jour persister une provenance `aggregate` dénuée de sens pour une série isolée.

### 2. La décision structurante : la ré-agrégation grain/cadence SORT de DEM-1

Le choix de périmètre le plus important de ce chantier est un **non-goal assumé** : DEM-1 ne touche PAS au grain/cadence de la demande. Le routeur consomme la demande telle que `get_historical_demand` la rend aujourd'hui — des **lignes journalières sparse** (jours sans booking absents, jamais zéro-remplis), jamais un bucket dense hebdo/mensuel. Changer ce contrat (`sparse-daily → dense-bucketé`) **ondule** dans MinT et dans les goldens de réconciliation ; c'est un chantier à part entière, **raccroché à #433**, hors DEM-1.

Conséquence : DEM-1 assume, sur les features, des **approximations V1 conservatrices**, chacune documentée dans le code et levée par le chantier ré-agrégation :

- **`seasonal_strength` en proxy saison-7** (`_ROUTING_SEASON_LENGTH = 7`, `repository.py`). La sonde saisonnière tourne sur la série **journalière** sparse : c'est une saisonnalité hebdomadaire **positionnelle**, pas calendaire-alignée. Elle **sous-estime** le signal saisonnier — ce qui ne route jamais qu'une série *hors* de la branche head (conservateur : une vraie tête n'est jamais mal-classée en traîne à cause de ce proxy). La saison annuelle honnête attend la ré-agrégation.
- **`zero_ratio` calculé au jour** (`(jours de span − jours portant un booking) / jours de span`). Les bookings sont grumeleux en jours-calendaires même pour un article à fort volume, donc ce ratio journalier court **haut** : un **sur-classement en intermittent est possible**. Le garde-fou est le knob 🎯 `RoutingThresholds.intermittent_zero_ratio` ; l'intermittence honnête au niveau bucket attend #433.
- **Routage sur la vie entière vs forecast sur fenêtre bornée.** Le routeur classe une série sur **tout** son historique tandis que le forecast tourne sur une fenêtre bornée — une incohérence de fenêtre 🎯 assumée en V1, à réconcilier au chantier ré-agrégation.

### 3. Le premier watcher DEMANDE — `agent_forecast_watcher`

`agent_forecast_watcher` (`scripts/agent_forecast_watcher.py`) lit le dernier run Pyramide **baseline** par `(item, location)` et sa ligne de métriques backtest **agrégée** (`pyramide_accuracy_metrics`, `horizon IS NULL`), et émet une recommandation gouvernée **L1 DRAFT `FORECAST_DRIFT`** quand une série s'est dégradée. C'est l'analogue demande des tours de contrôle pénurie/matière : il **draft** un re-forecast/une revue qu'un planificateur dispose, jamais une action appliquée.

- **Table typée séparée `forecast_drift_recommendations` (migration 072).** La table canonique `recommendations` (migration 039) est **supply-only par CHECK/NOT NULL** : `shortage_date` NOT NULL + `deficit_qty` NOT NULL + un CHECK d'`action` procurement/reschedule. Un verdict de dérive ne porte NI date de pénurie NI déficit — le forcer dans `recommendations` exigerait de NULLer des colonnes NOT NULL et d'élargir un CHECK supply avec une action non-supply. **Même logique** que `transfer_recommendations` (eando, 066) et `parameter_recommendations` (param-piloting, 041) : chaque famille non-supply a sa propre table typée, calquée sur la **même** forme de gouvernance (`status` / `decision_level` / `agent_name` / `agent_run_id` / `evidence`) et le **même** state-machine #341.
- **Condition de dérive : `MASE > seuil` OU `tracking_ratio > seuil`, None-honnête stricte.** Le classifieur pur (`classify_drift`) rend `MASE_DEGRADED` (mase seul), `BIAS_SUSTAINED` (biais seul) ou `BOTH`. **None-honnête** : une métrique `NULL` ne **déclenche** ni ne **bloque** l'autre ; les deux absentes ⇒ la série est **ignorée** (une métrique manquante n'est PAS une dérive). L'absence de donnée est le travail de la **porte de fraîcheur (ADR-023)**, jamais du watcher de dérive.
- **`tracking_ratio = |bias| / mean_forecast` — et sa limite honnête.** La ligne agrégée 055 ne porte **aucune échelle de demande** : seuls `mase`/`wape`/`smape` sont des ratios, `bias` est la seule quantité absolue. On normalise donc le biais par le **mean(forecast)** du run lui-même (au même grain que `bias`) — un « biais de forecast en % » standard, persisté dans la colonne `tracking_ratio`. Une **vraie** normalisation par le réel (tracking signal `bias / MAE`, ou mean-ACTUAL) exigerait de persister MAE / mean-actual dans `pyramide_accuracy_metrics` — **un changement de migration**, arbitrage 🎯 futur, hors périmètre PR-2.
- **Identité `uuid5` déterministe + idempotence + réactivation de tombstone + supersede.** `drift_recommendation_id` = `uuid5(scenario / item / location / drift_kind)` — **PAS le `run_id`** : un re-run sur une dérive **inchangée** re-dérive le **même** id. **Correctif post-revue adversariale (verdict NO-GO, un seul MAJEUR) :** l'upsert initial était `ON CONFLICT (recommendation_id) DO NOTHING` — un pur no-op sur conflit. Bug confirmé par re-dérivation : la clé n'a **aucune composante temporelle**, donc la séquence DRAFT → dérive résolue → `_expire_stale_drafts` passe la ligne **EXPIRED** → la série re-dérive **avec le même `drift_kind`** → même uuid5 → `DO NOTHING` → **la ligne reste EXPIRED à jamais**, `agent_run_id` inchangé → `emit_recommendation_created_for_run` compte 0 → **aucun event**. Une dérive récurrente devenait invisible en permanence — et le MASE d'une série oscille couramment autour du seuil, donc la récidive est la norme, pas un cas limite. **Correctif retenu (réactivation tombstone uniquement) :** `ON CONFLICT (recommendation_id) DO UPDATE SET status='DRAFT', ... WHERE forecast_drift_recommendations.status = 'EXPIRED' RETURNING recommendation_id, (xmax = 0) AS was_insert`. Quatre issues, distinguées par `_upsert` (`inserted_ids` / `reactivated_ids` / `affirmed_ids`) : (1) id neuf → INSERT ; (2) DRAFT vivant identique → le `WHERE` échoue → no-op strict, idempotence re-run→0 préservée ; (3) tombstone **EXPIRED** → réactivé en DRAFT avec métriques fraîches **et `agent_run_id` re-stampé au run courant** → entre dans le COUNT de `emit_recommendation_created_for_run` → l'event est ré-émis ; (4) statut **posé par un humain** (REVIEWED/APPROVED/REJECTED/APPLIED) → le `WHERE` échoue aussi → **jamais touché**, même si la même dérive récidive — un `REJECTED` reste rejeté (choix assumé). Le `WHERE status = 'EXPIRED'` est la garde qui protège (2) et (4) : seule une ligne déjà tombstonée peut être ranimée. Les DRAFTs antérieurs **de ce watcher/scénario** dont la dérive ne se déclenche plus sont **EXPIRÉS** (`_expire_stale_drafts`) — le pattern d'idempotence par expiration du watcher reschedule #346, plus fort qu'un supersede-puis-réinsère aveugle. Un changement de `drift_kind` (ex. `MASE_DEGRADED → BOTH`) frappe un id genuinement neuf et expire l'ancien.
- **`FORECAST_DRIFT` = L1.** Un flag de qualité de forecast est réversible/bas-risque : `agent_governance.decision_level("FORECAST_DRIFT") = "L1"` (jamais codé en dur ; le CHECK 072 est délibérément plus serré que l'échelle 039/041 et n'admet que `L1`).
- **Baseline-only en V1 — justification ADR-030.** La dérive est mesurée contre le forecast **baseline** parce qu'elle est la précision opérationnelle **réellement observée** ; un fork est **simulé, pas observé** (même raison que la machine à preuve ADR-030). Passer un scénario non-baseline est **refusé** (`main` retourne `2`).
- **Event `recommendation_created` gratuit via `_RECO_TABLES` (AN-1).** `forecast_drift_recommendations` est dans `_RECO_TABLES` (`engine/events/emit.py`). À la clôture COMPLETED, `governed_run` appelle `emit_recommendation_created_for_run` qui COMPTE les lignes de ce run à travers `_RECO_TABLES` par `agent_run_id` et émet **un** event `recommendation_created` **ssi ≥ 1** ligne neuve — le watcher s'annonce donc sur `GET /v1/stream` sans une ligne de code réseau, et un re-run inchangé (0 insertion) n'annonce rien. **Corollaire de contribution : toute nouvelle table de recos gouvernées DOIT être ajoutée à `_RECO_TABLES`, sinon son watcher est invisible au stream.**

## Portée

- **PR-1 = master data + chemin de run, invariant par scénario sauf le run lui-même.** Le routage suit le `scenario_id` du run (un fork route l'état du fork), mais les features sont des faits demande partagés. Aucune forkabilité nouvelle introduite — le routage est une décision de méthode/niveau, pas un levier de simulation.
- **PR-2 = baseline-only par nature** (§3) — un outcome de dérive est le réel observé. Le schéma 072 porte néanmoins `scenario_id` (cohérence + forkabilité future) avec la FK `ON DELETE RESTRICT` explicite exigée par `test_scenario_fk_retention`.

## Non-goals (explicitement hors DEM-1)

- **La ré-agrégation grain/cadence** (`sparse-daily → dense-bucketé`) — chantier séparé **#433** (§2). Tant qu'il n'a pas atterri, les approximations saison-7 / `zero_ratio` journalier / fenêtre vie-entière restent en place, documentées.
- **Une normalisation du `tracking_ratio` par le réel** (mean-ACTUAL / MAE) — exige une migration sur 055 (§3), 🎯 futur.
- **La forkabilité du verdict de dérive** — baseline-only par nature (§3).

## Réglages 🎯 résiduels (arbitrages pilote)

- **Seuils de dérive** — défauts `--mase-threshold 1.3` (pire que 1,3× la naïve saisonnière) et `--bias-ratio-threshold 0.3` (biais relatif > 30 %). Réglables par CLI.
- **Normalisation du biais** par mean-ACTUAL plutôt que mean-forecast — attend la migration 055 (§3).
- **Calibration de `zero_ratio`** (`intermittent_zero_ratio`) — le ratio journalier court haut, le seuil compense en attendant #433.
- **Grain de forecast par défaut par classe ABC** — à trancher au chantier ré-agrégation.

## Alternatives rejetées

- **Câbler le routage en dur (toujours actif).** Rejeté : casserait le byte-identique et forcerait un changement de méthode exécutée sur des runs existants. L'opt-in `auto_route` préserve le contrat.
- **Écraser un choix de méthode explicite avec la recommandation du routeur.** Rejeté : la provenance enregistre « le routeur aurait pris X » sans jamais confisquer un choix opérateur ; seul un `AUTO_SELECT` non-opiniâtré cède la place.
- **Faire la ré-agrégation grain/cadence dans DEM-1.** Rejeté : le changement de contrat `get_historical_demand` ondule dans MinT + goldens — chantier à part (#433). DEM-1 assume les approximations conservatrices en attendant.
- **Forcer la dérive dans la table `recommendations`.** Rejeté : supply-only par CHECK/NOT NULL — il faudrait NULLer des colonnes NOT NULL et élargir un CHECK supply. La table typée séparée est la même réponse qu'eando/param (§3).
- **Une clé d'idempotence sur le `run_id`.** Rejeté : un id lié au run rendrait chaque re-run non-idempotent (nouvel id à chaque passage). La clé `(scenario/item/location/drift_kind)` rend un re-run inchangé strictement no-op.
- **Garder `ON CONFLICT DO NOTHING` pur (design initial).** Rejeté après revue adversariale (verdict NO-GO, un seul MAJEUR) : sans composante temporelle dans la clé, une dérive résolue-puis-récidivante (le régime normal — le MASE d'une série oscille couramment autour du seuil) re-frappe le même uuid5 sur une ligne déjà `EXPIRED` ; `DO NOTHING` la laisse tombstonée à vie, invisible à `emit_recommendation_created_for_run` et donc à `/v1/stream`. La réactivation `WHERE status = 'EXPIRED'` corrige sans rouvrir les statuts posés par un humain.
- **Réactiver aveuglément tout statut (DO UPDATE sans `WHERE`).** Rejeté : rouvrirait un `REJECTED`/`APPROVED`/`APPLIED` posé par un humain à chaque récidive de la même dérive, court-circuitant la décision de gouvernance déjà rendue. Le `WHERE status = 'EXPIRED'` limite la réactivation au seul statut que l'agent lui-même a posé.
- **Émettre la dérive à partir d'un LLM / d'un récit.** Rejeté par principe (North Star « deterministic core ») : le verdict de dérive est une fonction pure des métriques backtest.
- **Traiter une métrique `NULL` comme une dérive.** Rejeté : une métrique absente est un problème de fraîcheur (ADR-023), pas une dérive de précision. None-honnête stricte.

## Conséquences

- **Positif :** le routeur cesse d'être une bibliothèque inerte — un run opt-in émet et trace une décision pour 100 % des séries du bloc. La flotte gagne son **premier watcher demande** : la dérive de précision backtestée devient une reco gouvernée L1, idempotente, auditée, streamée — sans casser un seul run existant.
- **Négatif / dette assumée en V1 :**
  - Les features de routage reposent sur des approximations journalières (saison-7, `zero_ratio` haut) tant que #433 n'a pas livré la ré-agrégation — conservatrices mais imparfaites.
  - Le `tracking_ratio` normalise par mean-forecast faute de mean-ACTUAL/MAE en 055 — un « biais de forecast % » honnête mais pas un vrai tracking signal.
  - Baseline-only sur le verdict de dérive (par nature).
- **Reste à faire (hors DEM-1) :** ré-agrégation grain/cadence (#433) ; migration 055 pour la normalisation par le réel ; calibration pilote des seuils et du grain par classe ABC.

## Références

- **DEM-1** — `docs/ROADMAP-AGENTS-2026-H2.md` §DEM-1 (constat « bibliothèque inerte » + critères d'acceptation : décisions tracées pour 100 % des séries, dérive provoquée → reco DRAFT `FORECAST_DRIFT`).
- **#438** — PR-1 : routage tête/traîne câblé en opt-in (`auto_route`) + provenance persistée.
- **#433** — chantier ré-agrégation grain/cadence (le non-goal structurant de §2).
- `src/ootils_core/pyramide/routing.py` — `route()` / `classify()` / `SeriesFeatures` / `RoutingThresholds` (le routeur DB-free, déterministe, paramétré).
- `src/ootils_core/pyramide/repository.py` — `build_series_features` / `_build_node_features` / `build_routing_decisions` (le côté appelant qui calcule les features, alias-aware via `_warehouse_codes_subquery`), `_ROUTING_SEASON_LENGTH = 7` (le proxy saison-7 documenté).
- `src/ootils_core/api/routers/pyramide.py` — le flag `auto_route` (défaut `False`), l'épinglage `LEVEL_LEAF` en mono-série, la garde « ne jamais écraser un `method` explicite ».
- `src/ootils_core/db/migrations/058_pyramide_routing_provenance.sql` — `routed_method` / `routed_level` / `routing_reason` + le CHECK all-or-none.
- `scripts/agent_forecast_watcher.py` — `classify_drift` (classifieur pur), `relative_bias` (`|bias|/mean_forecast`), `drift_recommendation_id` (`uuid5` scenario/item/location/drift_kind), `_fetch_series` (dernier run baseline + ligne agrégée `horizon IS NULL`), `_expire_stale_drafts` (supersede #346).
- `src/ootils_core/db/migrations/072_forecast_drift_recommendations.sql` — la table typée, le CHECK `drift_kind` / `action='FORECAST_DRIFT'` / `decision_level='L1'`, le carve-out JSONB `evidence`, la FK `scenario_id` `ON DELETE RESTRICT`.
- `src/ootils_core/engine/events/emit.py` — `_RECO_TABLES` (dont `forecast_drift_recommendations`) + `emit_recommendation_created_for_run` (l'event `recommendation_created` gratuit, AN-1).
- `scripts/agent_governance.py` — `decision_level("FORECAST_DRIFT") = "L1"` + `governed_run` (le harnais d'audit/émission).
- `docs/DESIGN-pyramide-forecasting.md` §5 — le mur d'échelle tête/traîne que le routage câblé adresse.
- `docs/ADR-019-demand-model-pyramide.md`, `docs/ADR-022-pyramide-reconciliation.md` — le modèle Pyramide et la réconciliation MinT dans laquelle le routage s'insère.
- `docs/ADR-023-forecast-confidence.md` — la porte de fraîcheur qui possède l'absence-de-donnée (le watcher de dérive ne la double pas).
- `docs/ADR-030-proof-machine.md` — la justification baseline-only (le réel observé vs le simulé) reprise ici pour le verdict de dérive.
- `docs/ADR-031-location-aliases.md` — `_warehouse_codes_subquery` (`external_id ∪ aliases`), la résolution single-point que les features de routage réutilisent.
