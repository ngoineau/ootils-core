# ADR-035 — Segmentation par programme Buy (DEM-2 PR1) : preuve ΔFVA avant/après

**Statut :** Accepté — PR1 du chantier phasé **DEM-2** (`docs/ROADMAP-AGENTS-2026-H2.md` §5, contrat architecte issue #444). Implémentation dans ce worktree (branche `feat/dem2-segmentation-pr1`), non encore mergée sur `main`.
**Date :** 2026-07-09
**Auteurs :** ootils-core team
**Contexte mesuré :** ROADMAP-AGENTS-2026-H2 §5 DEM-2 — « ZÉRO variable exogène au runtime … `order_type`/`org_id` (programme Buy) ingérés (migration 048, commentaire "drive the seasonality") et JAMAIS lus » ; phasage 1/2/3 (segmentation → calendrier/féries LGBM → covariates Chronos), acceptation « FVA mesuré AVANT/APRÈS segmentation … la machine à preuve juge ».

---

## Contexte

`demand_history.order_type` (migration 048) porte le programme Buy de l'ERP (SPRING BUY / SUMMER BUY / EARLY BUY / FWD BUY, plus des types standards comme STANDARD/VISTA) depuis son ingestion — et n'était lu par aucun chemin runtime avant ce chantier. C'est la première variable causale déjà présente en base que Pyramide n'exploitait pas : la couche demande d'Ootils prévoit aujourd'hui une seule série mélangée par `(item, location)`, alors qu'un programme Buy réel a son propre calendrier (une fenêtre de commande annuelle, disjointe des autres programmes) — un signal qu'une seasonal-naive ou un LGBM univarié sur le total mélangé ne peut pas isoler.

Un script de preuve de concept existait déjà (`scripts/forecast_program_poc.py`), pandas/numpy, sur un périmètre pilote figé (`product_local_gen`, `org_id='PPS'`) : il compare une seasonal-naive calendaire au total groupe vs par bucket `order_type` recombiné. Il a établi que le signal existe mais n'est ni généralisable (hiérarchie/org codés en dur) ni intégré à la machine à preuve du dépôt (ADR-030) — un `print()` de comparaison, pas un FVA persistant/rejouable. Il porte aussi un bug d'honnêteté hérité : un `order_type` `None`/vide est fondu silencieusement dans le bucket `BASE` (`bucket()`, `forecast_program_poc.py:38-48`), indistinguable d'une vraie commande STANDARD classifiée.

Le cadrage architecte (issue #444) phase le chantier en 3 étapes réalistes plutôt que de promettre l'exogène complet d'un bloc (calendrier/féries LGBM et covariates Chronos restent hors PR1). **Ce PR1 est le cœur de preuve** : établir, avec la machine à preuve (ADR-030) et non un récit, si forecaster chaque programme Buy séparément puis sommer bat la prévision unique sur le total mélangé — READ-ONLY, ZÉRO migration.

Deux contraintes du cadrage pèsent directement sur la conception :

- **Pas de paramètre de segmentation dans `get_historical_demand`** (bien que ce soit la formulation littérale de la ligne roadmap §5 DEM-2 point 1) : ce lecteur retourne une série **sparse** (`repository.py:447-449` — « days without demand are absent, not zero-filled »), un contrat consommé tel quel par `ForecastingEngine`/`PyramideRunner`. Un programme Buy réel ne réserve qu'une poignée de jours calendaires par an : segmenter cette série sparse par programme produirait N calendriers **disjoints et non-sommables** (les jours présents pour SPRING ne sont pas ceux présents pour SUMMER) — exactement le problème de ré-agrégation que le chantier `#433` a explicitement mis hors périmètre du lecteur feuille. Ajouter un paramètre ici aurait rouvert #433 par la bande plutôt que de livrer DEM-2 PR1.
- **La machine à preuve existe déjà et ne doit pas être dupliquée** (ADR-030) : `pyramide/fva.py:compute_fva` est la seule fonction FVA du dépôt, backtestée sur les mêmes cutoffs rolling-origin que le rapport stat qu'elle compare. PR1 doit s'y brancher, jamais réimplémenter une formule ΔFVA parallèle.

## Décision

### 1. Un NOUVEAU lecteur dense-bucketé, pas un paramètre sur le lecteur existant

`pyramide/segmentation.py:get_historical_demand_by_program(db, item_id, location_id, lookback_days, granularity)` est un second lecteur, à côté de `repository.get_historical_demand`, jamais un paramètre dessus. Contrairement au lecteur feuille sparse, il retourne un **calendrier dense zéro-rempli partagé** entre tous les programmes présents dans la fenêtre (`ProgramDemandCalendar` — un bucket sans demande, sur AUCUN programme, apparaît quand même comme zéro), condition nécessaire pour que le backtest segmenté (§3) tranche chaque programme au même point d'origine glissant que les autres.

**Granularité `weekly`/`monthly` uniquement, `daily` explicitement refusé.** `SEGMENTED_GRANULARITIES = {"weekly": "week", "monthly": "month"}` (`segmentation.py:169`) ; un `granularity="daily"` (ou toute valeur hors de ce mapping) lève `ValueError` — à la fois côté SQL (`get_historical_demand_by_program`) et côté pur (`build_program_demand_calendar`) — plutôt que de produire silencieusement un calendrier journalier presque entièrement à zéro par programme, une comparaison dégénérée qui gonflerait artificiellement le FVA côté segmenté (un modèle qui prédit zéro sur une série à 95 % de zéros a un WAPE trivialement bas). Fail-loudly, même discipline que `accuracy.py`/`fva.py`.

**Alias-aware obligatoire.** La résolution de site réutilise le même helper single-source que tous les autres lecteurs par site — `pyramide/repository.py:_warehouse_codes_subquery()` (ADR-031, `external_id ∪ location_aliases`) — jamais une égalité `warehouse_id = external_id` écrite à la main, qui rouvrirait le fossé #408.

**Mêmes prédicats métier que le lecteur existant.** `_DEMAND_HISTORY_BUSINESS_PREDICATES` (`repository.py:433-436` : `stream='regular'`, inter-entity exclu, `booked_date` présent, fenêtre `lookback_days` au passé strict) est réutilisé tel quel — pas de copie divergente des règles métier du signal forecast-on-booking (ADR-019).

**Pas de repli CustomerOrderDemand.** Le lecteur feuille dégrade sur les nœuds graphe quand `demand_history` est vide (`repository.py:465-471`) ; ce lecteur ne le fait délibérément **pas** — un nœud graphe ne porte aucun `order_type` à segmenter, un repli silencieux collapserait tous les programmes en un seul bucket sur une install non encore ingérée en `demand_history`, plus trompeur que le calendrier vide honnête déjà retourné (`build_program_demand_calendar` sur zéro lignes).

**`org_id` hors périmètre PR1** (la note « clé extensible » du cadrage) : les lignes de tout `org_id` sont poolées dans la même série, comme le fait déjà le lecteur feuille aujourd'hui. Aucun filtre `org_id` n'est câblé — étendre la clé de segmentation à `(order_type, org_id)` est un futur possible, pas un choix de ce PR.

**Pas de `scenario_id`.** `demand_history` est invariant par scénario (comme tout autre lecteur du module) : le paramètre n'existe délibérément pas ici non plus.

### 2. `buy_program_bucket()` — taxonomie SOURCE UNIQUE, correction d'honnêteté vs le POC

`pyramide/segmentation.py:buy_program_bucket(order_type)` est la **seule** fonction de classification programme Buy du dépôt ; tout consommateur (ce lecteur, le harness, un futur watcher) doit y passer — une seconde copie serait exactement le type de divergence silencieuse que la discipline « deux vérités » d'ADR-021 existe à empêcher ailleurs.

Reprise de `scripts/forecast_program_poc.py:38-48` (mêmes marqueurs substring case-insensitive : `SPRING BUY`, `SUMMER BUY`, `EARLY BUY`, `FWD BUY`/`FORWARD BUY`), avec **une correction délibérée** :

- Le POC fait `(ot or "").upper()` puis, faute de correspondance à un marqueur, retombe sur `"BASE"` — y compris pour `order_type IS NULL` ou une chaîne vide. Un `order_type` manquant est alors **indistinguable** d'un `order_type` réellement classifié `"STANDARD"`/`"VISTA"` : un bug d'honnêteté (None-honnête violé) hérité tel quel dans le POC.
- `buy_program_bucket()` sépare les deux cas : `order_type is None` ou vide (après `strip()`) → bucket `BUCKET_UNKNOWN` **explicite** ; `order_type` présent mais ne matchant aucun marqueur programme → `BUCKET_BASE` (une vraie classification : un ordre standard, pas une absence de donnée).

**Partition EXHAUSTIVE par construction.** `BUY_PROGRAM_BUCKETS = (SPRING, SUMMER, EARLY, FWD, BASE, UNKNOWN)` est l'ensemble fixe et complet — `buy_program_bucket()` ne retourne jamais rien en dehors. L'invariant « Σ programmes = total mélangé, exactement » est donc garanti par construction (pas par un calcul indépendant qui pourrait diverger), et vérifiable explicitement via `verify_partition_exhaustive(calendar)` (égalité `Decimal` stricte, aucune tolérance) — le harness l'appelle une fois par série, en garde-fou belt-and-braces, avant de faire confiance à un ΔFVA.

`UNKNOWN` participe au total au même titre que les autres buckets (une commande sans `order_type` renseigné est une vraie demande, elle ne disparaît pas de la série) — mais est tracé séparément, permettant à un consommateur futur de voir la part de demande non-attribuable à un programme.

### 3. Backtest segmenté DB-free — AVANT/APRÈS sur la MÊME orchestration, `compute_fva` réutilisé tel quel

`run_segmented_fva_proof(calendar, forecast_fn, *, min_train, horizon, step)` (`segmentation.py:523`) est pur, DB-free, model-free (le modèle de prévision est injecté, même inversion `ForecastFn` que `accuracy.py`) :

- **AVANT (`mixed_report`)** : `accuracy.evaluate_rolling_origin` sur le `total` du calendrier (une seule partition), la même série que la machine à preuve backteste déjà pour un run non segmenté.
- **APRÈS (`segmented_report`)** : la **même** orchestration `evaluate_rolling_origin`, sur la **même** série `total`, avec **une** différence — la fonction de prévision injectée (`_sum_program_forecasts`) forecaste CHAQUE programme sur sa propre tranche d'entraînement au même point d'origine glissant (le calendrier partagé dense garantit que l'indice `origin` désigne le même point calendaire pour tous les programmes) puis somme les courbes. Même `min_train`/`horizon`/`step`, donc mêmes cutoffs **par construction** — vérifié par une assertion structurelle (`mixed_report.n_cutoffs == segmented_report.n_cutoffs`), pas une branche None-honnête : un mismatch ici serait un bug de cette fonction, pas une condition de données.
- **`compute_fva` est appelé deux fois, jamais réimplémenté** (`fva.py:166`) : une fois sur `mixed_report`, une fois sur `segmented_report`, tous deux backtestés sur le même `total`/`season_length` — donc la même baseline seasonal-naive des deux côtés. `ΔFVA_wape`/`ΔFVA_mase` = `fva_segmented.fva_* − fva_mixed.fva_*` (positif = la segmentation ajoute de la valeur), ce qui équivaut algébriquement à `WAPE_mélangé − WAPE_segmenté` (le terme naïve commun s'annule) — la formulation du cadrage, dérivée ici de deux vrais appels à `compute_fva`, jamais une formule parallèle.
- **None-honnête strict, à deux niveaux.** Court-circuit structurel : calendrier vide, ou `min_train` ne pouvant former aucun cutoff (`min_train < 1` ou `min_train >= n_buckets`) → tous les champs `report`/`fva` à `None`, `basis_count=0`, jamais un `AccuracyReport`/`FvaResult` fabriqué. Court-circuit FVA : `delta_fva_wape`/`delta_fva_mase` sont `None` dès que l'un des deux `compute_fva` renvoie `None` (naïve non alignable), et `basis_count` reste `0` dans ce cas — jamais un delta inventé accompagné d'un compte trompeur.

### 4. Harness `scripts/prove_segmentation_fva.py` — CLI read-only, style scripts existant

Le harness est le seul point qui touche une connexion DB (hors le SELECT du lecteur lui-même) : il découvre les séries pilote éligibles (au moins `--min-order-types` valeurs `order_type` distinctes — sinon la segmentation est un no-op trivial), construit le calendrier dense par série, vérifie `verify_partition_exhaustive`, lance `run_segmented_fva_proof` avec le **même** moteur stat que le chemin de production (`PyramideForecastEngine.forecast(method=AUTO_SELECT)`, identique aux deux orchestrations AVANT/APRÈS — la preuve isole l'effet de la segmentation, pas un changement de méthode), et imprime un tableau ΔFVA par série + un agrégat pondéré par volume (`aggregate_delta_fva_wape`, None-honnête : `(None, 0)` si aucune série ne contribue).

- **Read-only belt-and-braces**, même motif que `scripts/bench_mrp.py`/`scripts/bench_reconciliation.py` : `SET default_transaction_read_only = on` avant tout SELECT, aucune écriture nulle part dans ce PR.
- **`print()`, pas `logger`** : conforme à la convention repo (`print()` interdit en chemin de production, toléré « style maison » pour les CLI de `scripts/`) — les autres harnesses de preuve/bench du dépôt (`bench_mrp.py`) suivent le même style ; ce script ne fait pas exception.
- **CLI conventionnelle** : `--dsn` (défaut `DATABASE_URL`), `--top`, `--granularity` (`weekly`/`monthly`, défaut `monthly`), `--lookback-days`, `--tail-origins`, `--horizon`, `--min-order-types` — mêmes noms/valeurs par défaut que les scripts de bench voisins (`--tail-origins 52`, même convention de queue que `pyramide/engines.py:_backtest_report`).

### 5. Cohérence avec ADR-022 (MinT) et ADR-023 (confiance) — orthogonalité par construction, pas par vérification

**Orthogonal à ADR-022 (réconciliation hiérarchique / MinT) par construction.** Ce module ne touche ni `item_hierarchy` ni le réconciliateur (`pyramide/hierarchy/reconcile.py`) : les segments sont des programmes Buy d'**une seule** série feuille `(item, location)`, pas des nœuds de hiérarchie. Aucune interaction runtime n'existe entre les deux chemins — pas de flag à vérifier, pas de garde-fou à écrire, parce que les deux modules n'importent rien l'un de l'autre.

**Rien à câbler pour ADR-023 (confiance) en PR1 — documenté, pas oublié.** `pyramide/confidence.py` compose WAPE backtesté × profondeur d'historique × fraîcheur pour un **run servi** persisté (`pyramide_runs`). Un backtest de harness de preuve n'est pas un run servi : il ne persiste rien, ne porte pas de `pyramide_run_id`, et sa dégradation sur données insuffisantes passe déjà par son propre canal honnête — un `FvaResult` à `None` (§3) plutôt qu'un score de confiance composé. Rien à composer ici ; le jour où la segmentation devient un mode de run persistant (phase 2/3 du chantier DEM-2), la confiance de ce run suivra le même chemin `confidence.py` que tout run aujourd'hui — pas un score parallèle.

## Alternatives rejetées

- **Paramètre de segmentation sur `repository.get_historical_demand`.** Rejeté — la formulation littérale de la ligne roadmap, mais le contrat sparse du lecteur (jours absents, pas zéro-remplis) rendrait une série segmentée par programme disjointe et non-sommable, et rouvrirait la ré-agrégation `#433` explicitement mise hors périmètre du lecteur feuille. Un second lecteur dédié, dense et zéro-rempli, est la seule forme qui permette un backtest segmenté honnête.
- **Granularité `daily` segmentée.** Rejeté — un programme Buy réserve une poignée de jours calendaires par an ; une série journalière par programme serait presque entièrement à zéro, un backtest dégénéré qui gonflerait artificiellement le FVA côté segmenté. `ValueError` explicite plutôt qu'une comparaison silencieusement biaisée.
- **Fondre `order_type` `NULL`/vide dans `BASE` (comme le POC).** Rejeté — indistinguable d'une vraie commande classifiée STANDARD/VISTA ; viole le None-honnête. `BUCKET_UNKNOWN` explicite, tracé, sommé dans le total.
- **Réimplémenter une formule ΔFVA dédiée à la segmentation.** Rejeté — `pyramide/fva.py:compute_fva` est la seule fonction FVA du dépôt (ADR-030) ; une seconde formule serait exactement la duplication de vérité que la discipline « une seule maths » (ADR-020, ADR-021) existe à empêcher. Le delta est dérivé algébriquement de deux appels au vrai `compute_fva`.
- **Filtrer/segmenter par `org_id` dès PR1.** Différé, pas rejeté — hors du cadrage explicite (« org_id HORS périmètre, clé extensible ») ; le lecteur poole tous les `org_id`, comme le fait déjà le lecteur feuille aujourd'hui. Étendre la clé de segmentation à `(order_type, org_id)` reste un futur possible.
- **Câbler `pyramide/confidence.py` sur le backtest du harness.** Rejeté pour PR1 — le composeur de confiance existe pour un run servi persisté (`pyramide_runs`), pas pour un backtest de preuve DB-free et non persistant ; la dégradation sur données insuffisantes passe déjà par le canal `FvaResult=None` du module.

## 🎯 Pilote

- **Taxonomie des programmes Buy (`BUY_PROGRAM_BUCKETS` et ses marqueurs substring).** Reprise telle quelle du POC (`SPRING BUY`/`SUMMER BUY`/`EARLY BUY`/`FWD BUY`/`FORWARD BUY`), un vocabulaire observé sur le jeu pilote — pas une taxonomie universelle. Un futur ERP/pilote peut porter d'autres marqueurs de programme ; à recalibrer avec le retour terrain, pas une constante métier gravée dans le marbre.
- **`--min-order-types` (défaut 2), `--tail-origins` (défaut 52), `--lookback-days` (défaut 1095).** Seuils d'éligibilité et de fenêtre du harness, choisis par cohérence avec les conventions voisines (`pyramide/engines.py` pour la queue de 52 origines) — ajustables sans changer la logique de preuve.
- **Granularité par défaut `monthly`.** `weekly` reste disponible en option ; le choix du grain de preuve par défaut pour la démo/pilote peut évoluer avec le volume réel de commandes par programme observé sur la base pilote.
- **Extension future à `(order_type, org_id)`.** Non implémentée en PR1 (org_id hors périmètre) ; à considérer si le pilote veut isoler le signal par entité légale en plus du programme.

## Conséquences

- **Positif :** DEM-2 obtient son cœur de preuve — un chiffre ΔFVA défendable, produit par la machine à preuve existante (ADR-030), jamais un récit — sans toucher une seule ligne de schéma ni un seul chemin d'écriture. Le bug d'honnêteté du POC (`order_type` manquant fondu dans `BASE`) est corrigé au passage, dans le module qui devient la source unique de la taxonomie.
- **Négatif / dette assumée :**
  - Testé selon la discipline `accuracy.py`/`fva.py` : 75 tests unitaires DB-free (goldens Decimal dérivés à la main — ΔFVA +0.25/−0.25 exacts, invariant Σ programmes = total, UNKNOWN jamais fondu dans BASE) + 10 cas d'intégration Postgres pour `get_historical_demand_by_program` (alias-aware, prédicats partagés) livrés dans le même PR que cet ADR.
  - Le harness `prove_segmentation_fva.py` n'a jamais été exécuté contre une base réelle dans ce worktree (aucune DB locale disponible pendant le développement de PR1) — l'acceptation DEM-2 (« FVA mesuré AVANT/APRÈS sur les séries pilote ») reste à valider par un run sur la VM pilote, hors de ce PR.
  - `org_id` non segmenté : deux entités légales partageant un `(item, location)` et des programmes Buy différents restent poolées.
  - Aucun câblage vers un run Pyramide servi, un score de confiance ou une recommandation gouvernée — PR1 est délibérément un harness de preuve autonome, pas un mode de production.
- **Reste à faire (phases 2/3 du chantier DEM-2, hors PR1) :** calendrier/jours fériés comme features LGBM (M) ; covariates Chronos-2 connues-futures — programme, calendrier — branchées sur `forecast_batch` (M, `foundation.py`). Si PR1 prouve un ΔFVA positif significatif sur les séries pilote, un futur PR pourrait faire de `order_type` un mode de run Pyramide persistant (au-delà du harness de preuve) — non cadré ici.

## Hors périmètre PR1 (explicitement)

- Un paramètre de segmentation sur un endpoint Pyramide servi (`POST /v1/pyramide/run` ou équivalent) — PR1 est un harness de preuve hors ligne, pas un mode de run.
- Le calendrier/jours fériés comme feature LGBM (DEM-2 phase 2).
- Les covariates connues-futures Chronos-2 (DEM-2 phase 3).
- La segmentation par `org_id` ou toute clé composée au-delà de `order_type` seul.
- Toute écriture — persistance des calendriers par programme, d'un ΔFVA en base, d'une recommandation gouvernée sur le résultat de la preuve.

## Références

- `docs/ROADMAP-AGENTS-2026-H2.md` §5 — DEM-2, le cadrage phasé et son critère d'acceptation.
- `docs/ADR-030-proof-machine.md` — la machine à preuve (FVA, snapshots, chaînage outcome) dont ce PR réutilise `compute_fva` sans le dupliquer.
- `docs/ADR-019-demand-model-pyramide.md` — le modèle de demande forecast-on-booking (`demand_history`, prédicats métier partagés).
- `docs/ADR-020-mrp-consolidation.md`, `docs/ADR-021-shortage-truth.md` — la discipline « une seule vérité/maths », appliquée ici à la taxonomie programme Buy (`buy_program_bucket`) et au FVA (`compute_fva`).
- `docs/ADR-022-pyramide-reconciliation.md` — la réconciliation hiérarchique / MinT dont ce module reste orthogonal par construction.
- `docs/ADR-023-forecast-confidence.md` — le composeur de confiance, hors périmètre PR1 (raison gravée en §5 de la Décision).
- `docs/ADR-031-location-aliases.md` — `_warehouse_codes_subquery()`, réutilisé tel quel pour la résolution de site.

## Code references

- `src/ootils_core/pyramide/segmentation.py` — module entier (taxonomie, calendrier dense, backtest segmenté, agrégation).
- `src/ootils_core/pyramide/segmentation.py:114-151` — `buy_program_bucket()`, la source unique de la taxonomie, correction None-honnête vs le POC.
- `src/ootils_core/pyramide/segmentation.py:169-178` — `SEGMENTED_GRANULARITIES`, `daily` explicitement absent.
- `src/ootils_core/pyramide/segmentation.py:246-320` — `build_program_demand_calendar`, le calendrier dense zéro-rempli pur.
- `src/ootils_core/pyramide/segmentation.py:323-341` — `verify_partition_exhaustive`, le témoin testable de l'invariant de partition.
- `src/ootils_core/pyramide/segmentation.py:344-418` — `get_historical_demand_by_program`, le nouveau lecteur SELECT-only.
- `src/ootils_core/pyramide/segmentation.py:523-622` — `run_segmented_fva_proof`, l'orchestration AVANT/APRÈS et l'appel à `compute_fva`.
- `src/ootils_core/pyramide/fva.py:166` — `compute_fva`, réutilisé, jamais réimplémenté.
- `src/ootils_core/pyramide/repository.py:193-213` — `_warehouse_codes_subquery()`, réutilisé pour la résolution de site.
- `src/ootils_core/pyramide/repository.py:427-436` — `_DEMAND_HISTORY_BUSINESS_PREDICATES`, réutilisé tel quel.
- `scripts/prove_segmentation_fva.py` — le harness CLI read-only.
- `scripts/forecast_program_poc.py:38-48` — le POC d'origine (`bucket()`), dont `buy_program_bucket()` corrige le bug d'honnêteté.
- `src/ootils_core/db/migrations/048_demand_history_entity_program.sql` — `demand_history.order_type`, la colonne source, jamais modifiée par ce PR.
