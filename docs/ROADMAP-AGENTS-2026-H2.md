# ROADMAP AGENTS 2026-H2 — « Top Mondial »

**Document de passation pour la flotte d'agents (Opus 4.8) qui exécute, orchestrée par Fable, pilotée par l'expert métier (🎯).**
**Établi le 2026-07-06 au soir, après audit exhaustif multi-agents : 11 axes, 60 agents, chaque affirmation prouvée `fichier:ligne`, chaque gap P0/P1 passé par une réfutation adversariale indépendante (49 vérifiés → 44 confirmés, 5 réfutés — voir §8).**
**Commit de référence : main `bd223b2` + PR #418 (verte, non mergée).**

---

## §0 — Mission et définition du succès

- **Le pari (acté 2026-07-05)** : être **LA référence mondiale « APS designed by AI, for AI »**. PAS la parité de largeur avec Kinaxis/o9 — la course choisie est la catégorie agent-native, en cours de fondation, sans leader.
- **L'objectif 8 semaines (fin août 2026)** : une **démo crédible sur la base pilote réelle** (36 635 items, 3,76 M lignes de demande). Le véhicule est `scripts/demo_e2e.py` + `docs/DEMO-RUNBOOK.md` (#408, exécuté le 06/07 : 7 PASS / 3 SKIP / 0 FAIL).
- **Les humains** : le pilote = expert métier mondial. Checkpoints 🎯 non-bloquants (défauts proposés, il corrige). **Chaque merge exige son « GO merge #N » explicite.**
- **Tout se juge à deux aunes** : ① est-ce que ça rend le pitch AI-native VRAI au runtime (exécutable, pas aspirationnel) ? ② est-ce que ça rend la démo pilote plus crédible ?

### Scores au 2026-07-06 (audit, barème « vs le meilleur APS imaginable »)

| Axe | Score | Axe | Score |
|---|---|---|---|
| Scénarios / what-if | **7,0** | Demand planning (Pyramide) | 5,5 |
| Qualité d'ingénierie | **6,5** | Backlog & gouvernance | 5,5 |
| Moat (défendabilité) | **6,5** | Data & intégration | 4,5 |
| Supply (MRP/DRP) | 5,5 | Explicabilité / surface humaine | 4,5 |
| Substrat agent-native | 5,5 | Échelle & prod-readiness | 4,0 |
| | | S&OP / tactique | **3,5** |

---

## §1 — Règles de vol (NON NÉGOCIABLES — chacune a été payée)

1. **Revue adversariale avant CHAQUE merge. Jamais de merge rouge.** La CI est la seule validation des tests d'intégration.
2. **Tests d'intégration écrits EN AVEUGLE** — aucune DB locale sûre (la seule joignable est la base PILOTE, interdite aux fixtures). Cloner des patterns qui passent. Pièges payés : **pollution de ranking global** (un `LIMIT N` sur toute la base voit les restes des autres tests → ancrer le tri par données dédiées + cleanup FK-ordonné par test), **visibilité cross-connexion** (un seed transactionnel est invisible d'une connexion séparée), prémisse de colonne/statut inventée.
3. **`PYTHONPATH=<worktree>\src` en tête de TOUT pytest/mypy en worktree** — l'install editable (.pth) importe silencieusement le code de `C:\dev\Ootils` (main) ; seul ruff est immunisé (filesystem). L'exiger dans chaque brief d'agent.
4. **Jamais 2 agents pytest dans le même worktree.** 2 chantiers max en parallèle, fichiers disjoints, worktrees séparés.
5. **dict_row : accès par nom, JAMAIS `[0]`.** Toute fonction recevant une connexion externe ouvre ses curseurs avec `row_factory=dict_row` explicite.
6. **FK vers `scenarios`/`locations` : `ON DELETE RESTRICT` explicite** (défaut PG = NO ACTION ; garde-fou `test_scenario_fk_retention`).
7. **Migrations : idempotence défensive intégrale** (header de la 063 = pattern canonique) ; 1 fichier = 1 transaction ; le runner crash au boot sur erreur et rejoue tout.
8. **`openapi.json` régénéré à CHAQUE changement de contrat** (`scripts/export_openapi.py`).
9. `print()` interdit hors CLI `scripts/` ; SQL toujours paramétrée ; `detail=str(e)` interdit hors carve-out nommé (CLAUDE.md).
10. Messages commit/PR : `-F`/`--body-file` (PowerShell 5.1 mangle les guillemets). `jq` absent du Git Bash local (utiliser `gh --watch` ou python).
11. **DSN pilote : jamais affiché, jamais persisté** hors scratchpad de session. `OOTILS_API_TOKEN` généré par run (`secrets.token_urlsafe`), jamais affiché.
12. **Base pilote : non-destructif absolu.** Forks archivés (`DELETE /v1/scenarios` = archived), jamais TRUNCATE/DELETE. Writes = fonctionnement normal du produit uniquement.
13. **Anti-patterns North Star** (refuser même si demandé) : module baseline-only ; read sans `scenario_id` ; write L3+ contournant la state machine ; métrique sans confiance/fraîcheur ; LLM dans un calcul déterministe ; write sans event typé.
14. **Moirai (FM_MOIRAI) : exclu licence — ne JAMAIS re-proposer.** FM_CHRONOS ok.
15. **Playbook chantier** : architecte (plan, droit de dire NON) → db-specialist (migrations seules) → backend-dev (Python seul) → test-writer (unit exécutés + intégration aveugle) → reviewer adversarial (GO/NO-GO) → relecture orchestrateur du delta → commit `-F` → PR `--body-file` → CI watch → « GO merge #N » du pilote → merge `--squash` → nettoyage worktree.

---

## §2 — État exact à la passation (photo du 2026-07-06 soir)

- **main `bd223b2`** — 19 PR mergées le 06/07, zéro merge rouge. 70 migrations (CLAUDE.md dit encore « 32 » — dette de vérité, voir H0).
- **#408 DÉMO : livrée/fermée** (7/3/0). **#414 « Allumer la base pilote »** : A alias mergé (#416, ADR-031, migration 070) · D fix #398 mergé (#417) · **C = PR #418 VERTE non mergée** (bootstrap_pi scénarisé + garde-fou 2M + `docs/RUNBOOK-pilot-propagation.md` ; worktree `C:\dev\worktrees\feat-414-prop` conservé) · B lanes = données 🎯.
- **Révélations pilote MESURÉES** : 0/15 `warehouse_id` mappés (mécanisme d'alias livré, table 🎯 attendue) ; 0 lane DRP ; 0 nœud PI (propagation jamais exercée — la PR #418 y répond) ; **0 event pour 18 658 recos** (#401) ; machine à preuve opérationnelle jour 1 (15 895 snapshots, 51 AVOIDED / 226 367 $, approval 0,35 %).
- **Perf mesurée** : MRP math core 0,77 s compute / 259 K ordres/s (36 635 items), wall 4,80 s à 84 % DB-load. Propagation SQL ~4 000 nodes/s (2026-05, synthétique — jamais mesurée en chemin API sur pilote).
- **Ouverts** : PR dependabot #419/#420/#421 (traiter sous CI) ; chip « explosion BOM du promote = no-op silencieux » (MRPExplosionEngine mort depuis ADR-020 — rebrancher ou retirer).
- **DSN pilote : à redemander au pilote à la reprise** (scratchpad de session, non persistant).

---

## §3 — Reprise immédiate (H0, jour 1) — ordre exact

1. **« GO merge #418 »** → merge → supprimer worktree `feat-414-prop` + branche.
2. **Exécuter la première propagation pilote** : suivre `docs/RUNBOOK-pilot-propagation.md` (fork via `POST /v1/simulate` 0 override → `bootstrap_pi --scenario <fork> --sample-finished 300 --horizon-days 120` → `POST /v1/calc/run?scenario_id=<fork>` `{"full_recompute":true}` → relever nps/durée/mémoire → `demo_e2e --whatif-base-scenario <fork>` (step 7 PASS attendu) → archiver). Chiffres → `PERF-BASELINE.md` + `SCALABILITY.md`.
3. **Ingérer les données 🎯 dès réception** : table de mapping entrepôts (payload `aliases` de `POST /v1/ingest/locations`) puis lanes → **re-run du runbook démo, cible 9-10 PASS sur données réelles**.
4. **Dependabot #419-421** : merger sous CI verte, un par un.
5. **Hygiène de gouvernance (S, une demi-journée, tout est confirmé par l'audit)** :
   - Cocher les cases des épiques #397/#350/#351 (A1/A3/B1/B2 sont CLOSED mais non cochés) ;
   - Régénérer la section ADR de `docs/INDEX.md` (s'arrête à ADR-013 ; 18 ADR manquants 014→031) ;
   - **Écrire `docs/ADR-029`** (étage entreprise agents — cité 3× par la migration 064, jamais écrit) ;
   - `docs/PROJECT-STATUS.md` gelé au 30/05 (nomme encore Pyramide « chantier actif ») : le mettre à jour OU le remplacer par un pointeur vers l'épique #397 + ce document (décision : pointeur — une seule source de vérité vivante) ;
   - `ROADMAP.md` racine (dit « 32 migrations ») + CLAUDE.md (idem) : remettre à la vérité ;
   - Corriger `docs/DESIGN-shipping-plan.md` (référence « migration 049 » déjà prise par item_asp) ;
   - `SECURITY.md:28` « No f-string SQL » → reformuler honnêtement (14 sites d'identifiants allowlistés, valeurs toujours paramétrées).

---

## §4 — H1 (semaines 1-2) : « Le pari devient VRAI au runtime »

*Tout ce bloc ferme les P0 du pari AI-native — mesurés sur pilote, confirmés par réfutation. C'est la priorité absolue après H0.*

**H1 : FAIT 5/5 (2026-07-08).** Les cinq chantiers ci-dessous sont mergés sur `main` et vérifiés (fichiers, tests, garde-fous CI présents dans le repo). H2 (§5) est désormais la vague courante.

### AN-1 — Émission d'events complète + flotte en subscribe (#401) — P0 — ✅ FAIT (PR #430, mergée)
**Constat confirmé** : création de reco, détection de shortage, fin de calc run n'écrivent AUCUN event (`transfer.py:329-355`, `detector.py:174-210`, `calc_run.py`) ; la machine à preuve non plus (`capture.py:197`, `evaluator.py:705`) ; aucun watcher ne consomme `/v1/stream` (le flag `--subscribe` n'existe pas). Le principe « Streamable » est produit sans consommateur et writes sans producteur.
**Livrables** : (a) étendre `VALID_EVENT_TYPES` + CHECK (migration idempotente) avec `recommendation_created`/`shortage_detected`/`calc_run_finished`/`outcome_evaluated`/`snapshot_captured` ; émettre dans les 5 sites d'écriture ; (b) `--subscribe` sur les watchers (`/v1/stream?cursor=&once=true` en mode cron — le producteur SSE est prêt) ; (c) garde-fou CI : un test qui échoue si un write gouverné n'émet pas d'event (le « garde-fou » de #401).
**Attention (dette tracée)** : trancher AVANT d'émettre `shortage_detected` laquelle des deux vérités shortage émet (ADR-021 : la table `shortages`/ShortageDetector — c'est elle le système de persistance canonique).
**Acceptation** : re-run pilote → `GET /v1/stream?once=true` rejoue N > 0 events typés couvrant recos/shortages/calc/outcomes ; ≥ 1 watcher tourne en subscribe.
**Vérifié dans le repo** : migration `071_events_fleet_types.sql` (les 5 types) ; garde-fou CI `tests/integration/test_fleet_events_integration.py` (« la thèse North Star » — cases 1-7, un write gouverné sans event = rouge) ; `--subscribe` câblé sur `scripts/agent_shortage_watcher.py` via `scripts/agent_subscribe.py` (`drain_stream`/`fetch_stream_cursor`, cursor persisté).

### AN-2 — Scopes bout-en-bout + budgets par token (A2-PR2 + ADR-029) — P0 — ✅ FAIT (PRs #434/#435, mergées ; ADR-032 écrit)
**Constat confirmé** : `require_auth` ne vérifie JAMAIS les scopes (auth.py:539-553) — seuls 7 routers sur ~30 utilisent `require_scope` ; un token read-only peut déclencher `POST /v1/mrp/run` ou muter le graphe. `api_tokens.rate_per_min` (migration 064) est du schéma mort : lu par aucun code.
**Livrables** : (a) basculer TOUS les routers d'écriture (`mrp`, `ingest`, `calc`, `param_overrides`, `simulate`, `graph`, `mps`, `drp`…) vers `require_scope` — inventaire exhaustif par grep, aucun oublié ; (b) appliquer `rate_per_min` par token (middleware au checkout du Principal) ; (c) endpoints issue/revoke de tokens (gouvernés admin) ; (d) `/metrics` ; (e) **écrire ADR-029** (avec H0).
**Acceptation** : test d'intégration — token scope read tente un write sur chaque famille de routes → 403 systématique ; token agent dépasse son rate → 429.
**Vérifié dans le repo** : `require_scope` présent sur les 29 routers montés sous `src/ootils_core/api/routers/`, zéro `require_auth` restant côté routeurs ; `routers/tokens.py` (`POST/GET/DELETE /v1/tokens`) ; `GET /metrics` (`api/app.py`) ; `docs/ADR-032-scope-grid-and-budgets.md` écrit (grille des 8 scopes, doctrine coût≠réversibilité, `_RateCounter`) ; matrice d'enforcement `tests/integration/test_agent_floor_integration.py`.

### SUP-1 — #423 : ADR-020 PAS 4, une seule maths MRP — P0 (décision pilote 2026-07-06, absorbe #415) — ✅ FAIT (PR #432, mergée ; issue #423 fermée)
**Constat confirmé + arbitrage pilote** : le moteur APICS (`POST /v1/mrp/run`) **réimplémente la maths** du cœur au lieu de l'appeler — chaque amélioration du cœur (fenêtre de consommation #349) laisse l'API en retard (« deux moteurs, deux chiffres »), résidu de parité ~4 % médian, garde-fou CI en xfail qui documente la dérive au lieu de l'empêcher. Fermer #415 seul soignerait un symptôme en laissant la machine à divergences en place.
**Livrables (3 PR, détail dans #423)** : (1) arbitrages sémantiques tranchés dans le cœur (frozen fence 🎯, demande indépendante des composants, pièces de rechange, L4L/order_multiple) ; (2) délégation — l'APICS appelle le cœur et ne garde que la matérialisation graphe (`graph_integration`), **parité xfail → VERT DUR** ; (3) forkabilité réelle — l'on-hand APICS est en `BASELINE` codé en dur (risque nommé par l'ADR-020 : « les scénarios mentiraient ») → scénariser lecture+écriture via le loader du cœur, ferme une violation North Star silencieuse.
**Acceptation** : parité math core ↔ APICS en vert dur CI (Early Buy inclus — ferme #415) ; `POST /v1/mrp/run` scénarisé bout-en-bout ; la maths MRP n'existe plus qu'à UN endroit ; goldens du cœur inchangés.
**Vérifié dans le repo** : `.github/workflows/ci.yml` (« MRP A-vs-B parity guard #332 / #423 PR2 » — `parity_mrp_engines.py --check --max-median-drift 0.05`, gate dur, plus d'xfail) ; `tests/integration/test_mrp_delegation_integration.py` (run APICS sur fork isolé, overlay de scénario respecté) ; `docs/ADR-020-mrp-consolidation.md` §Séquence de migration PAS 4 « délégation ✅ fait ». Échelon DRP (per-site) reste hors périmètre #423, gated sur la demande per-site Pyramide — non réclamé ici.

### DEM-1 — Câbler le routage tête/traîne + première exception demande — P0 — ✅ FAIT (PR #438 routage + PR #439 watcher, mergées ; ADR-033 écrit ; migration 072)
**Constat confirmé** : `route()`/`classify()`/`SeriesFeatures` (577 lignes testées) ne sont appelés QUE par les tests ; le `HierarchicalRunner` consomme un mapping que rien ne peuple. La réponse au mur d'échelle du DESIGN est une bibliothèque inerte.
**Livrables** : (a) `build_series_features(item, location)` dans le repository (profondeur histo, zero_ratio, ABC via `item_asp` × volume, force saisonnière — tout est déjà calculable) ; appel de `route()` dans le runner quand `routing_decisions` absent ; raison de routage persistée (auditabilité) ; (b) **tracking signal** : surveiller `accuracy.bias`/MASE glissant sur les runs persistés → exception DRAFT gouvernée quand ça dérape (réutilise l'existant ; crée la PREMIÈRE boucle d'exception demande — gap confirmé « aucun watcher côté demande »).
**Acceptation** : run hiérarchique pilote → décisions de routage émises et tracées pour 100 % des séries ; une dérive provoquée en test → reco DRAFT `FORECAST_DRIFT`.
**Vérifié dans le repo** : `auto_route` câblé dans `pyramide/repository.py` et `pyramide/hierarchy/runner.py` (opt-in, byte-identique par défaut) ; `scripts/agent_forecast_watcher.py` (premier watcher DEMANDE de la flotte) ; migration `072_forecast_drift_recommendations.sql` ; `tests/integration/test_forecast_watcher_integration.py` ; `docs/ADR-033-demand-routing-and-drift.md` écrit.

### PROD-QW — Paquet quick-wins production (tous S, tous confirmés) — ✅ FAIT (PR #437, mergée)
- **Restore prouvé + copie off-host (#192 — P0)** : script `pg_restore` vers DB jetable + test + rsync/rclone off-host. « Un backup jamais restauré n'est pas un backup. »
- **Résilience pool** : `check=ConnectionPool.check_connection`, `max_lifetime`, `statement_timeout` + `idle_in_transaction_session_timeout` globaux.
- **pip-audit en CI + CodeQL default-setup** (2 gates, ~30 min).
- **`--cov-branch --cov-fail-under=<baseline>`** sur le job pytest (pytest-cov déjà en dep).
- **`GET /v1/audit`** : lire `api_request_log` (écrit depuis app.py:120, lu par personne) — pagination + filtres actor/path/date, scope admin. « Audit is a feature, not telemetry » devient vrai.
- **Webhook sortant minimal** : POST configurable sur transition de reco vers un statut L3 en attente — « l'exception te trouve » sans UI.
**Vérifié dans le repo** : `scripts/restore_postgres.sh` + `scripts/backup_offhost.sh` ; `db/connection.py` (les 4 réglages de résilience pool) ; `.github/workflows/ci.yml` (`--cov-branch --cov-fail-under=40`) + `.github/workflows/codeql.yml` + job pip-audit dans `ci.yml` ; `api/routers/audit.py` (`GET /v1/audit`, scope admin) ; `notifications/l3_webhook.py`.

---

## §5 — H2 (semaines 3-5) : « La démo qui gagne »

### DEM-2 — L'avantage causal, version 1 honnête — P0 (phasé)
**Constat confirmé** : ZÉRO variable exogène au runtime (repository univarié ; LGBM lags seuls ; `forecast_batch` sans covariates alors que Chronos-2 les supporte et que foundation.py:80 le vend). `order_type`/`org_id` (programme Buy) ingérés (migration 048, commentaire « drive the seasonality ») et JAMAIS lus.
**Phasage réaliste** (l'exogène complet est un L — ne pas le promettre d'un bloc) :
1. **Segmentation par programme Buy** (S/M) : paramètre de segmentation dans `get_historical_demand` → forecast par (programme) — première variable causale, données déjà là ;
2. **Calendrier/féries comme features LGBM** (M) ;
3. **Covariates Chronos** (M) : brancher `forecast_batch` sur les covariates connues-futures (programme, calendrier).
**Acceptation** : FVA mesuré AVANT/APRÈS segmentation sur les séries pilote (la machine à preuve juge — c'est exactement son rôle).

### SC-1 — Comparaison multi-scénarios en KPI métier — P1 (différenciateur démo majeur)
**Constat confirmé** : le diff ne compare que 6 champs bruts de nœuds ; aucune réponse à « le scénario B réduit-il la rupture de X % et le stock de Y € vs A ? ». C2 de la revue précédente, toujours ouvert.
**Livrables** : `GET /v1/scenarios/compare?ids=a,b,c` → nb pénuries, sévérité $, valeur de stock (via `cost_of`), fill-rate estimé, par scénario + delta ; flag `stale` calculé (dernier calc_run du fork vs dernier promote baseline — sans migration) ; statut `stale` en schéma si l'architecte le valide.
**Acceptation** : la démo montre 2 forks rankés en $ côte à côte.

### SOP-1 — B3 #327 : Shipping Plan (le S&OP prend corps) — P0 de l'axe le plus faible (3,5)
**Pré-requis (gates)** : les 4 décisions métier 🎯 de #327 (clé de désagrégation Ad'hoc, couplage dispo, retours en $, machine d'états) + les deux consommateurs de données : **ASP** (`item_asp` mig 049 existe, écrit par `compute_asp.py`, lu par RIEN — le câbler) et **`returns_history`** (ingéré, 0 lecture — écrire le consommateur net $ mensuel). Ces deux quick-wins (S) se font AVANT, indépendamment des décisions.
**Livrables** : PAS 2→6 du design (migration — PAS 049, numéro à corriger —, moteur de règles DB-free style mrp_core, budget = scénario gelé, variance 3 voies, gouvernance/stream).
**Complément confirmé (S)** : **brancher `check_capacity()` comme gate sur MPS approve/promote** (aujourd'hui la state machine approuve sans jamais vérifier la capacité — mps/api.py:812).

### EXP-1 — Une surface humaine minimale — P0 (🎯 ARBITRAGE PILOTE REQUIS)
**Constat confirmé** : zéro UI (aucun .html/.jsx dans src/), l'approbation L3 = CLI + POST JSON ; zéro alerting sortant. « La Decision Ladder exige un visage » — et la démo 8 semaines vs Kinaxis est aujourd'hui un curl.
**Tension honnête** : CONTRIBUTING.md dit « API-first, pas d'UI en V1 ». L'audit dit « l'écart n°1 qu'un dirigeant verra ».
**Proposition (défaut)** : une **page unique server-rendered** (FastAPI + template, zéro framework front) : inbox des recos par niveau L, bouton approve/reject (qui appelle l'API existante avec le token humain), les 5 KPI de preuve, la comparaison de scénarios de SC-1. Une « fenêtre », pas un produit UI. 🎯 le pilote tranche : page minimale / démo API-only assumée / repousser.
**Complément** : exposer le `graph_fragment` dans `GET /v1/explain` (le GraphStore a déjà tout — assemblage, honore ADR-004) ; router l'explication au-delà des pénuries PI (les recos portent `evidence` mais pas de chaîne traversable).

### MOAT-1 — Rendre le fossé démontrable — P0 (confirmé : « vrai dans le code mais invendable »)
**Livrables** : (a) `docs/MOAT.md` : les 4 propriétés non-copiables (déterminisme rejouable, actor_kind non-usurpable, tout-forkable + promote-conflit, triplet causalité+audit), chacune avec son test falsifiable existant (fichier:ligne) ; (b) séquence « moat en 5 minutes » dans le DEMO-RUNBOOK : rejouer un calc_run → IDs identiques ; token agent qui ment sur son actor_kind → 403 ; fork+delta+archive sous les yeux de l'acheteur ; (c) le pitch « covariate-informed »/« replay » ne doit revendiquer QUE ce qui tourne.

---

## §6 — H3 (semaines 5-8) : « La preuve à l'échelle »

### SCALE-1 — Vérité d'échelle sur le chemin API, base pilote réelle — P0
**Constat confirmé** : toutes les mesures SCALABILITY viennent de bench engine-direct sur datasets synthétiques ; le chemin HTTP n'a jamais été mesuré sur pilote ; l'en-tête du doc se contredit avec son corps.
**Livrables** : bench API-path (latence p50/p95 `POST /v1/events` incrémental, `GET` reads, fork, calc run) sur la base pilote post-mapping ; SCALABILITY.md réécrit sur les seules mesures (y compris le **coût O(N) STOCKAGE du fork deep-copy** — confirmé : chaque fork copie ~211K nœuds).
### SCALE-2 — Décision d'architecture fork : Rust ArcSwap par défaut OU lazy CoW — 🎯 + architecte
**Constat confirmé** : le « business unlock » (fork ~1 µs, rust/scenario.rs:163-193, câblé bout-en-bout) **dort en opt-in** (`OOTILS_ENGINE` défaut `sql`) ; le lazy CoW d'ADR-013 n'est pas écrit ; les deux forks (RAM vs PG) divergent sémantiquement (le sandbox ne porte ni overrides ni diff/promote). Plus on attend, plus la dette d'opt-in grossit.
**Livrable** : ADR d'arbitrage par l'architecte (options : Rust default-on pour le sandbox what-if avec parité de contrat ; ou lazy CoW SQL ; ou statu quo documenté avec seuils de bascule) — décision AVANT d'investir davantage dans l'un des deux chemins.
### SUP-2 — Allocation en pénurie digne du wedge + stock de sécurité vivant — P1
**Constats confirmés** : `AllocationEngine` = greedy pur (le premier prioritaire draine tout — engine.py:267) alors que le fair-share stratifié existe déjà dans le DRP ; `policies.py` (SS par variance combinée, EOQ, ROP) = **bibliothèque morte** importée uniquement par les tests, tous les chemins lisent le `safety_stock_qty` statique.
**Livrables** : (a) politique de rationnement paramétrable (pro-rata/priorité pondérée) réutilisant le pattern DRP — c'est LE wedge (control tower pénurie) ; (b) `POST /v1/policies/safety-stock` : service-level cible + σ → SS proposé en **override gouverné scénarisé** (jamais d'écriture directe).
### AN-3 — Surface MCP pour agents externes — P1 (le pitch « for AI » complet)
**Constat confirmé** : aucun serveur MCP, aucun tool-manifest — un agent tiers doit parler REST brut. Standard 2026 pour brancher une flotte externe.
**Livrable** : serveur MCP exposant les tools read + recommandations (scopes par tool mappés sur les scopes token AN-2) ; les writes L3+ restent gate humain. C'est une vitrine à coût modéré : l'API est propre, contract-first, openapi à jour.
### DATA-1 — Onboarding outillé v1 — P1
**Constats confirmés** : aucun mapping header→canonique (le fossé warehouse_id généralisé) ; aucune ingestion delta/CDC (full-reload + soft-delete) ; ingestion baseline-only (aucun `scenario_id` sur `/v1/ingest/*`).
**Livrables v1 (le reste = H4)** : (a) résolveur header→champ canonique par source_system (table de mapping + param au staging/upload) ; (b) `scenario_id` optionnel sur les endpoints d'ingest transactionnels (les helpers le supportent déjà) ; (c) `GET /v1/export/{entity}` CSV/JSON — fermer la boucle « une reco SORT du système ».

---

## §7 — H4 (au-delà des 8 semaines — la profondeur top mondial)

Par ordre de valeur estimée (chacun exigera son propre cadrage architecte + 🎯) :
1. **Boucle d'apprentissage preuve→décision** : ré-injecter les verdicts outcomes (AVOIDED/MATERIALIZED) dans le routage forecast et les seuils watchers — le moat qui se creuse tout seul avec le temps (confirmé absent ; c'est LA suite logique d'ADR-030).
2. **MEIO** (multi-échelon) : le DRP est single-hop, excess non time-phasé — LE différenciateur o9/Kinaxis sur réseau réel.
3. **Cycle S&OP gouverné** : calendrier mensuel, demand/supply review, consensus avec FVA de l'ajustement humain (la brique `forecast_adjustments` EXISTE — voir §8), plan famille→mix, plan financier $ (le diff scénario n'a aucune colonne $).
4. **Boucle matière+capacité** : MRP capacité-infinie + CRP détecteur → re-planification jointe (au moins itérative bornée).
5. **Collaboration fournisseur** : PO ack/re-promesse de date, σ lead-time, multi-sourcing.
6. **Multi-tenant** (RLS + tenant_id — le retrofit grossit à chaque table ajoutée), **HA** (workers multiples, le verrou advisory sérialise baseline), **observabilité OTel**, **NPI/TWIN exécutable**, **demand sensing**, **connecteurs ERP/CDC natifs**, **property-based + mutation testing sur le cœur déterministe**, **release/versioning** (0 tag git aujourd'hui).

---

## §8 — Ce que l'audit a RÉFUTÉ (ne pas re-claimer ces gaps)

L'audit adversarial a tué 5 affirmations — les futurs agents ne doivent pas les répéter :
1. **« Aucun override planificateur »** : FAUX — `forecast_adjustments` (migration 026) + `POST /v1/demand/forecast/{id}/adjust` existent (delta, type, reason, user, scenario). Ce qui manque = le CYCLE de consensus autour (H4), pas la brique.
2. **« Aucune boucle d'équilibrage offre-demande »** : PARTIEL — le module MPS (aggregate → capacity-check → suggest-adjustments → approve → promote) EST une boucle rough-cut multi-période. Ce qui manque = le gate capacité sur approve (H2/SOP-1) et le niveau famille.
3. **« Aucun worker async »** : PARTIEL — l'Architecture B Rust (write-behind async + WAL, auditée) existe en opt-in. Le vrai gap = elle dort (SCALE-2), pas qu'elle n'existe pas.
4. **« Zéro observabilité Python »** : PARTIEL — middleware de latence + `api_request_log` structuré existent. Le vrai gap = pas de lecture (`/v1/audit`, H1) ni d'OTel/alerting (H4).
5. **« Explicabilité mono-nœud sans multi-hop »** : PARTIEL — `peg_origins` (mrp/core.py:649) fait la cascade LLC multi-niveau et alimente les recos des watchers. Le vrai gap = `/v1/explain` ne l'expose pas (EXP-1).

---

## §9 — Registre 🎯 pilote (à date)

| # | Décision | Débloque | Défaut proposé |
|---|---|---|---|
| 1 | **Table mapping entrepôts (~15 lignes)** | Forecast+FVA réels, step 2 démo | — (données à fournir) |
| 2 | Lanes de distribution réelles | DRP réel, step 4 démo | — (données à fournir) |
| 3 | Sous-ensemble + horizon 1re propagation | Step 7 démo, chiffres échelle | top-300 demande + sous-arbre BOM, 120 j |
| 4 | Surface humaine minimale (EXP-1) | Crédibilité démo dirigeant | Page unique server-rendered |
| 5 | 4 décisions S&OP #327 + validation ASP/retours | B3 | (cadrage préparé avant ouverture) |
| 6 | Fork : Rust default-on vs lazy CoW vs statu quo (SCALE-2) | L'échelle what-if | ADR architecte d'abord |
| 7 | KPI phare parmi les 5 · seuils outcome (0.05/0.90) · cadence S&OP · durcissement DB alias · STOP_BUY L1→L2 · calendrier Buy | divers | défauts en place |

---

*Ce document est LE point d'entrée de la relève. Chaque chantier ci-dessus s'exécute selon le playbook §1.15, avec son issue GitHub dédiée (à créer à l'ouverture, référencer ce doc), sa revue adversariale, et le GO merge explicite du pilote. La discipline n'est pas un luxe : sur la seule journée du 06/07, elle a intercepté ~15 défauts avant merge — dont 3 trous d'ambiguïté de données et 2 violations attrapées par des tests écrits en aveugle. C'est elle, la vraie machine.*
