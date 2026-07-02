# Revue experte APS — Juillet 2026

**Commit revu :** `b752a38` (main, 2026-07-01)
**Méthode :** revue multi-agents (32 agents) — cartographie de 8 sous-systèmes, évaluation par 6 lentilles d'expert APS (demand planning, supply planning MRP/DRP, S&OP/capacité, what-if/scénarios, master data/architecture, control tower/autonomie), puis **contre-vérification adversariale de 16 affirmations structurantes dans le code** (12 confirmées, 4 nuancées — voir §Nuances).
**Statut :** backlog ouvert — voir le tableau des chantiers (§Backlog) et l'issue épique #351.

---

## Verdict

**Ootils n'est pas un APS faible — c'est un excellent moteur de diagnostic avec une périphérie de planification qui trahit son propre cœur.** Le noyau mathématique MRP est de qualité quasi-professionnelle (golden-master dérivé à la main, 2,88 s pour 36 635 items / ~220 K ordres), le substrat scénarios est conceptuellement la bonne réponse au fossé Kinaxis (sandbox Rust fork ~1 µs, cycle agent p50 12,45 ms), mais la promesse centrale du projet — *tout est forkable, les recommandations sont adossées à des scénarios* — est **factuellement fausse au runtime à la date de la revue**.

Le triplet réellement différenciant et défendable — **déterminisme reproductible + chaîne causale persistée requêtable + audit systématique** — n'est offert ensemble par aucun APS du marché. Il est aujourd'hui sous-vendu au profit de promesses (scenario-first, autonomie) que le runtime ne tient pas encore. Recommandation stratégique : vendre ce qui est vrai, corriger ce qui est faux, dans cet ordre.

### Scores par domaine (vs un APS de référence)

| Domaine | Score | Résumé |
|---|---|---|
| Demand planning | 2,5/10 | Couche de faits excellente (`demand_history`), couche de prévision sous un APS d'entrée de gamme (forecast plat, historique contaminé) |
| Supply planning (MRP/DRP) | 3,5/10 | Cœur de netting APICS crédible et golden-masterisé ; périphérie write défaillante ; DRP inexistant au runtime |
| S&OP / capacité | 3/10 | Design Shipping Plan remarquable sur le papier ; zéro runtime ; RCCP non conforme |
| What-if / scénarios | 4/10 | Meilleure infrastructure du projet ; mais contre-factuels silencieusement faux hors baseline |
| Master data | 4/10 | Discipline rare (SCD2, typed columns, staging gouverné) ; non forkable — contradiction frontale avec le North Star |
| Control tower / autonomie | 3,5/10 | Diagnostic et gouvernance solides ; boucle tronquée aux deux extrémités (pas de scénario en amont, pas d'exécution en aval) |

### Le méta-problème : la duplication

La moitié des découvertes sont des variantes d'un seul défaut structurel : **deux implémentations partout, sans arbitre désigné**. Deux moteurs MRP (~4 % de dérive médiane, parité jamais exécutée en CI), deux consommations de forecast, deux MPS (module `mps/` vs `scripts/mrp_grid.py`), deux vérités de pénurie (les watchers ne lisent pas la table `shortages` du kernel), deux systèmes de scénarios sans pont (PG persistant / Rust éphémère), deux lecteurs d'historique de demande. Règle terrain proposée : le jour où une seconde implémentation d'une capacité apparaît, l'implémentation canonique doit être désignée dans CLAUDE.md (le projet l'a fait pour le MRP via ADR-020 — à généraliser).

---

## Constats vérifiés (contre-vérification adversariale dans le code)

Chaque constat ci-dessous a été soumis à un agent vérificateur chargé de le **réfuter** en fouillant le dépôt. Verdicts : ✅ confirmé, 🟡 partiel (voir nuance).

### Critiques — invalident le pitch scenario-first

| ID | Verdict | Constat |
|---|---|---|
| A1 | ✅ | **Le MRP write nette contre le stock baseline dans tout fork** : `_get_initial_on_hand` code en dur `BASELINE = UUID('…0001')` (`engine/mrp/gross_to_net.py:352`, requêtes l.363/372) quel que soit le scénario du run. Un agent qui simule dans un fork obtient un plan silencieusement faux. (= issue #333, confirmée) |
| A2 | ✅ | **Chaque re-run MRP APICS double-compte la supply** : `cleanup_previous_run` (`engine/mrp/graph_integration.py:352`) n'a **aucun appelant**. Conséquence vicieuse : l'environnement de démo se dégrade avec l'usage (« demo rot »). |
| A3 | ✅ | **ATP/CTP et RCCP agrègent TOUS les scénarios** — pire que « baseline-only » : `src/ootils_core/atp/` ne contient aucune occurrence de `scenario` ; `api/routers/rccp.py:179-186` n'accepte pas `scenario_id` et sa requête de charge (l.278-295) somme les nœuds supply de tous les scénarios. Un fork qui écrit **contamine les réponses baseline** (bidirectionnel). |
| A4 | ✅ | **La prévision s'entraîne sur des prévisions** : les deux lecteurs d'historique (`api/routers/forecasting.py:196-228`, `pyramide/repository.py:103-126`) somment `ForecastDemand` + `CustomerOrderDemand` comme « historique », **sans filtre `scenario_id`**, en contournant `demand_history` pourtant livrée (migrations 047-050). Plus les agents forkent, plus l'historique gonfle. (= issue #331, confirmée et aggravée) |
| A5 | ✅ | **« Scenario-backed recommendations » est faux pour la production** : aucun des 5 watchers (`scripts/agent_*_watcher.py`) ne fork ni n'appelle `/v1/simulate` ; ils calculent sur `core.BASELINE` en dur avec `'L1'` codé en dur. Seul le `demo_agent` (M7) a un chemin scénarisé. `/v1/simulate` **avale les exceptions de propagation** (`simulate.py:242-244`) et renvoie `created` avec delta vide — un agent ne distingue pas « aucune pénurie » de « le calcul a planté ». |
| A6 | ✅ | **Aucun garde-fou de parité A-vs-B en CI** : `scripts/parity_mrp_engines.py` existe mais n'est référencé par aucun workflow ; ~4,1 % de dérive médiane entre les deux moteurs, non surveillée. La validation « byte-identique 99 765 ordres » était un one-shot de migration (A-avant vs A-après), pas un filet permanent. (= issue #332, confirmée) |

### Majeurs — crédibilité APS

| ID | Verdict | Constat |
|---|---|---|
| A7 | ✅ | **Aucune méthode saisonnière implémentée** : `SEASONAL` est déclarée (enum, regex API, CHECK SQL 026/038) mais `forecasting/algorithms.py` ne contient que MA/ES/Croston. Le chemin par défaut sort un forecast **plat** — disqualifiant pour un business piscine à saisonnalité extrême. Le PoC seasonal-naive (`scripts/forecast_poc.py`) existe et est validé sur données réelles. |
| A8 | ✅ | **Pas de messages de reprogrammation des ordres ouverts** : `_classify_shortage` (`graph_integration.py:614-646`) n'émet jamais DEFER/CANCEL (déclarés en docstring) ; les scheduled receipts sont lus comme supply figée sans comparaison date d'ordre vs date de besoin. Le premier réflexe d'un planificateur (re-dater un PO, gratuit) est impossible. |
| A9 | ✅ | **Aucun firm planned order** (`grep is_firm|firm_planned` = 0) → aucune stabilité de plan, nervosité maximale à chaque régénération. |
| A10 | ✅ | **Le master data n'est pas forkable** : `items`, `locations`, `suppliers`, `supplier_items`, `item_planning_params`, `bom_headers/lines` n'ont aucune colonne `scenario_id`. Le what-if n°1 en production (« et si le lead time / la MOQ changeait ? ») est structurellement impossible. |
| A11 | ✅ | **Aucun moteur DRP** : `distribution_links` / `transportation_lanes` (migration 029) ne sont référencées par aucun `.py` du dépôt — tables mortes, donc aucune recommandation de transfert inter-site possible. Idem `uom_conversions`. |
| A12 | ✅ | **Réconciliation hiérarchique inexistante en production** : `recon_method` est persisté dans `pyramide_runs` mais aucun moteur ne l'applique ; middle-out = PoC uniquement ; FM_CHRONOS/FM_MOIRAI = stubs retombant sur AUTO_SELECT. |
| A13 | ✅ | **Cycle de vie scénario incomplet côté API** : `ScenarioManager.diff` (l.497) et `.promote` (l.622) sans endpoint ; `promote()` rejoue les overrides sans détection de divergence de la baseline ; l'invalidation des scénarios frères n'est **même pas loggée** (promesse de docstring). Diff limité à 6 champs stock/pénurie, aucun KPI métier. |
| A14 | ✅ | **La couche S&OP n'existe pas en code** : aucune table/module S&OP, pas de calendrier de programmes (brique B ADR-019) ; Shipping Plan purement documentaire — et le numéro de migration prévu par le design (`049_shipping_plan.sql`) est **déjà pris** par `049_item_asp.sql`. |
| A15 | ✅ | **Priorisation financière factice** : `_UNIT_COST_PROXY = Decimal('1')` (`kernel/shortage/detector.py:24`) alors que `items.standard_cost` existe (migration 042 + roll-up). Le tri des pénuries n'est pas en valeur réelle. |

### Nuances issues de la contre-vérification (affirmations corrigées)

| ID | Verdict | Nuance |
|---|---|---|
| N1 (C4) | 🟡 | « Pas de backtest rolling-origin » est **partiellement faux** : `pyramide/engines.py:340-365` (`_backtest_score`) EST un rolling-origin à fenêtre expansive (jusqu'à 52 cutoffs, ré-entraînement par cutoff, erreur normalisée type WAPE), utilisé par AUTO_SELECT. Le manque réel : pas de cadre d'accuracy multi-horizon **persisté/exposé**, pas de MASE/WAPE nommés dans la mesure publiée, colonnes CI jamais alimentées, pas de FVA. |
| N2 (C9) | 🟡 | « Aucune règle de sourcing » est excessif : `loader.py:128-136` ordonne par `is_preferred DESC` pour `best_sup` (valorisation, watchers). Les manques réels : le lead time de **planification** reste `MIN()` tous fournisseurs, `preferred_supplier_id` (override item×location) non lu, pas de split multi-fournisseur. Le pooling item-level est une décision **documentée et transitoire** (ADR-020, cœur paramétrable `(item, location)`), pas une incohérence cachée. |
| N3 (C10) | 🟡 | « Buckets hebdo codés en dur » ne vaut que pour le cœur consolidé A : le moteur APICS B supporte `day\|week\|month` de bout en bout (`gross_to_net.py:120-168`, `MRPRunConfig.bucket_grain`, exposé via `routers/mrp.py:47`). L'offset lead-time calendaire (sans `kernel/calc/calendar.py`) reste confirmé. |
| N4 (C15) | 🟡 | Trois sous-points nuancés : le rate limiting global existe (slowapi opt-in `OOTILS_RATE_LIMIT_PER_MIN`, `app.py:87-94`), l'idempotence ingest existe (migration 023), et le `demo_agent` M7 produit bien une recommandation adossée à `/v1/simulate` avec `simulation_scenario_id` persisté. Le fond tient pour les 5 watchers de production. |

---

## Ce qui est réellement bon (et rare)

- **Déterminisme reproductible** : UUID déterministes, seeds persistés, golden-master MRP dérivé à la main (`tests/test_mrp_core_golden.py`) qui a déjà attrapé des bugs réels.
- **Chaîne causale persistée requêtable** (ADR-004, `/v1/explain`) — le « insight feed » d'un o9 est opaque en comparaison.
- **Audit systématique** : ledger `agent_runs`, machine d'états DRAFT→APPROVED avec transitions contraintes et `FOR UPDATE`.
- **Règle booking/shipping matérialisée dans le schéma** : `demand_history` (booked_date primaire, streams regular/warranty, `counts_for_asp`, retours jamais nettés) — plus rigoureux que la moyenne des implémentations SAP terrain.
- **Consommation de prévision APICS-correcte** : `max(orders, forecast)`, jamais la somme ; demand time fence gelée = commandes seules. Beaucoup de MRP maison ratent exactement ça.
- **Sandbox Rust** : fork ArcSwap ~1 µs, cycle agent p50 12,45 ms — structurellement la réponse Kinaxis (versionnement in-memory) appliquée au bon sous-problème.
- **Discipline typed-columns + SCD2 + staging gouverné** sur le master data.

## Non-objectifs assumés (sains pour le positionnement — à documenter en ADR)

- **Pas d'ordonnancement fin / capacité finie** : métier distribution, la contrainte est l'appro, pas la machine.
- **Pas de solveur d'optimisation** : Kinaxis a bâti sa domination sur la re-simulation heuristique rapide, pas sur un LP. À tracer : les 3 endroits où un solveur deviendra nécessaire (allocation fair-share, lissage capacité, placement shipping plan sous contrainte $) et le critère de bascule.

## Angles métier (pool) non couverts par les designs actuels

1. **Consommation de forecast et Early Buy** : bookings fermes pris 4-6 mois avant expédition ; une consommation `max()` **par bucket hebdo** ne consomme pas le forecast des buckets voisins → double comptage temporel précisément en pré-saison. Il faut une fenêtre de consommation backward/forward (le `forecast_consumer` a une fenêtre, `core.py` n'en a pas).
2. **Biais saisonnier de l'ASP T12M** : les remises Early Buy tirent l'ASP sous le prix in-season → variance $ du futur Shipping Plan structurellement biaisée par phase. Prévoir un ASP par programme (`order_type`) ou par saison — la granularité de `demand_history` le permet déjà.
3. **Famine d'allocation** : le greedy strictement priorisé (`kernel/allocation/engine.py`) affame les dealers B en pénurie prolongée — en saison c'est une crise à J+15, pas un raffinement V2.
4. **Chimie piscine** : péremption / FEFO / lots / hazmat transport — zéro trace dans le schéma (stock utilisable vs stock total dans le netting, rotation FEFO au DRP, colisage hazmat au shipping plan).
5. **Le goulot réel d'un distributeur saisonnier** est la capacité de réception/stockage des DC en pré-saison (docks, main-d'œuvre, cube), pas la machine — le RCCP orienté fabrication ne pose pas la bonne question.
6. **Open-to-buy** : le MRP génère des ordres planifiés sans plafond financier ni valorisation cumulée vs budget achat — jumeau indispensable du shipping plan pour un distributeur saisonnier.
7. **Preuve de valeur impossible aujourd'hui** : pas de snapshots de stock historiques, pas de chaînage recommandation→résultat observé. Un snapshot quotidien (item×location×on_hand) + le chaînage `recommendation_id→outcome` transformerait le ledger d'audit existant en machine à ROI — coût faible, impact commercial majeur.
8. **MEIO** : ADR-020 décide « SS central poolé (risk pooling) » mais rien ne dit comment dimensionner les buffers par échelon (service level, variabilité demande×LT).
9. **Nervosité / dampening rules** : sans seuils d'amortissement (re-datage minimal, tolérance quantité), un MRP régénératif piloté par agents en continu produira une tempête de messages inexploitables.
10. **Supersession côté demande** (chaining item→successeur pour le transfert d'historique au cold-start) — fréquent en équipement piscine (millésimes de pompes/robots).
11. **Multi-devise / FX** : orgs PPS/PCC = deux devises ; l'agrégation cross-org du shipping plan en $ est impossible sans conversion — absente des 4 questions ouvertes du design (§11).
12. **Sécurité agent** : un seul token Bearer global, pas de scopes par agent, pas de kill-switch, pas de budgets — exigés par le North Star avant tout pilote où des agents écrivent.

## Observations d'implémentation (pièges terrain)

- **« Démo propre, vraies données cassées »** : ADR-019 admet que `time_span_start` est souvent NULL sur données réelles (casse `_get_historical_demand`), `warehouse_id` reste un TEXT non résolu, l'ingestion réelle est un TSV manuel hors API/staging/**tests**. L'écart entre `seed_demo_data.py` et le chemin de données réel est le risque de pilote dominant.
- Le vrai champ de bataille face à Kinaxis/o9 n'est pas l'algorithme mais le **temps d'implémentation** : connecteurs, mapping master data, delta-loads réconciliés. C'est le maillon le plus faible (TSV manuel, cap 10 MB, pas de CDC, fondation demande sans tests).
- La politique de migration « erreur already-exists ⇒ enregistrée comme appliquée » est un piège de dérive de schéma silencieuse entre environnements ; combinée à la collision de numéro 049 déjà constatée, elle plaide pour un check de somme de contrôle du schéma en CI.
- Les inserts ligne à ligne du moteur APICS (`persist_planned_orders`, `_persist_bucket_records`) sous transactions longues + advisory locks par scénario sont une bombe de contention dès que plusieurs agents planifient en parallèle — le passage en COPY/executemany est une condition de la promesse multi-agents, pas une optimisation.
- Le réequilibrage inter-DC est LE premier réflexe d'un planificateur distribution : la question tombera à la première session pilote. Réponse honnête à préparer (« gated sur ADR-020 PAS 4 »).

---

## Backlog — chantiers priorisés

> Séquencement recommandé : **arrêter les nouvelles features de planification, faire le sprint correctness d'abord.** Tout usage au-delà de « baseline, premier run » produit aujourd'hui des chiffres faux.

### C0 — Sprint correctness scénarios (P0 — restaurer l'invariant fondateur)

| # | Chantier | Constat | Issue |
|---|---|---|---|
| C0.1 | MRP write : on-hand scénarisé | A1 | #333 |
| C0.2 | Brancher `cleanup_previous_run` (purge des runs APICS) | A2 | #337 |
| C0.3 | ATP/CTP + RCCP : filtre `scenario_id` | A3 | #338 |
| C0.4 | `/v1/simulate` : remonter les échecs de propagation (`propagation_status` + freshness) | A5 | #339 |
| C0.5 | Parité A-vs-B en CI (seuil de dérive) | A6 | #332 |
| C0.6 | Lecteurs d'historique → `demand_history` (bookings réels, scenario-aware) | A4 | #331 |

### C1 — Fermer la boucle wedge (P0/P1)

| # | Chantier | Constat | Issue |
|---|---|---|---|
| C1.1 | Watchers réellement adossés à un fork (delta pénuries dans l'evidence) | A5 | #340 |
| C1.2 | API recommandations (file/review/approve) + diff/promote avec détection de conflit minimale | A13 | #341 |
| C1.3 | `standard_cost` dans le severity_score (quick win) | A15 | #342 |
| C1.4 | Unifier les deux vérités de pénurie (kernel `shortages` vs `mrp_core.first_shortage`) | méta | #343 |
| C1.5 | Tests d'intégration fondation demande (`demand_history`, `item_asp`, `returns_history`, ingestion) | — | #344 |

### C2 — Profondeur APS (P1)

| # | Chantier | Constat | Issue |
|---|---|---|---|
| C2.1 | SeasonalForecaster en production (promouvoir le PoC, méthode SEASONAL déjà déclarée) | A7 | #345 |
| C2.2 | Messages de reprogrammation des ordres ouverts (reschedule-in/out, DEFER, CANCEL) + Firm Planned Orders | A8, A9 | #346 |
| C2.3 | Overlay scénarisé des paramètres de planification (`scenario_planning_overrides`, COALESCE dans les loaders) | A10 | #347 |
| C2.4 | Réconciliation middle-out du PoC à la production (débloquant DRP / ADR-020 PAS 4) | A12 | #348 |
| C2.5 | Fenêtre de consommation forecast (Early Buy backward/forward) | métier §1 | #349 |

### C3 — Fond de backlog (P2)

Regroupés dans l'issue collective #350 : ASP par programme/saison · allocation fair-share · snapshots stock + chaînage reco→résultat (ROI) · tables mortes (câbler ou documenter `uom_conversions`, `distribution_links`, `transportation_lanes`) · calendrier S&OP (brique B ADR-019) + fix collision migration 049 · effectivité BOM complète (`effective_from`, niveau ligne) + `preferred_supplier_id` · lead time jours ouvrés via calendrier existant · cadre d'accuracy persisté (WAPE/MASE, CI alimentées, FVA) + score de confiance North Star · gouvernance des time fences (`requires_approval` consommé) · dampening rules · purge scénarios archivés / partitionnement · ADR « simulation, pas optimisation » · ADR non-objectifs (scheduling fin) · bill of capacity RCCP · check de somme de contrôle du schéma en CI.

---

*Revue produite le 2026-07-01 par workflow multi-agents (cartographie → évaluation → vérification adversariale → critique de complétude). Les affirmations non vérifiées adversarialement sont signalées comme telles ; toute citation `fichier:ligne` a été produite par lecture directe du code au commit `b752a38`.*
