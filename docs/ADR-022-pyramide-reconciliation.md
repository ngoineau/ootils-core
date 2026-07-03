# ADR-022 — Réconciliation hiérarchique Pyramide : middle-out déterministe au cœur, MinT-shrink au bord

**Statut :** Accepté — Pyramide V1 axe A, PR3 (branche `feat/pyramide-a-reconcile`) ; s'appuie sur PR1 (SEASONAL en courbe) et PR2 (migration 053, blocs S creux).
**Date :** 2026-07-02
**Contexte mesuré :** `docs/DESIGN-pyramide-forecasting.md` §3-4 (la loi de l'axe A), `pyramide_runs.recon_method` morte depuis la migration 038, `SummingBlock` par bloc (PR2).

---

## Contexte

Le moteur Pyramide savait prévoir une série à la fois (PR1 : courbes saisonnières) et construire la matrice de sommation S par bloc (PR2), mais rien ne les assemblait : pas de désagrégation, pas de cohérence entre niveaux, `recon_method` stockée mais jamais appliquée. Le design (§3-4) impose deux exigences en tension :

- **Cohérence exacte** entre niveaux (`ŷ = S·b̂`) pour que MRP (agrégat) et DRP (feuille) consomment le même plan ;
- **Déterminisme du cœur** (ADR-003, North Star) alors que la méthode optimale de la littérature (MinT-shrink) est de l'algèbre flottante numpy/BLAS, non garantie bit-à-bit entre environnements.

## Décision

1. **Le chemin garanti est le middle-out par proportions historiques** (`pyramide/hierarchy/reconcile.py:middle_out`) : pur Python `Decimal`, golden-testé, zéro dépendance. Prévision au niveau de réconciliation (paramétrable, défaut = niveau du bloc), désagrégation vers les feuilles par parts historiques calculées sur la fenêtre de lookback (`demand_history`, prédicats métier partagés), agrégation de tous les niveaux via S.
2. **La cohérence `ŷ = S·b̂` est exacte PAR CONSTRUCTION, pas par vérification** : les courbes feuilles sont calculées d'abord, et *toute* valeur de série (y compris le niveau de réconciliation lui-même) est la somme creuse de ses feuilles. La courbe persistée au niveau de réconciliation est donc re-dérivée des feuilles — elle peut différer de la prévision de base brute par la poussière de division `Decimal` (< 1e-25 relatif), jamais l'inverse.
3. **Proportions V1 (réponse à la question §4 du design)** : parts = totaux réservés (booking) par feuille sur fenêtre glissante, normalisés par nœud de réconciliation (somme = 1 par construction). **Cold-start = règle du JUMEAU** : une feuille sans historique hérite de la moyenne des poids positifs de ses sœurs (même parent direct) ; sans sœur positive, **zéro naturel** documenté par warning (une feuille structurellement non servie ne doit pas recevoir de demande). Le forecast direct de la feuille par FM puis réconciliation (design §4(c)) est le point d'extension documenté qui remplacera le jumeau. **Non-objectif assumé de cette PR** : les profils de proportions par phase de saison (§4) — les parts V1 sont un scalaire par feuille.
4. **MinT-shrink est une amélioration OPTIONNELLE au bord stochastique** (`mint_shrink`) : via `hierarchicalforecast` (Nixtla), ajouté à l'extra `[forecast]`, import paresseux + fallback middle-out (pattern `_statsforecast` existant). **La reproductibilité bit-à-bit de MinT n'est PAS garantissable** (algèbre flottante numpy/BLAS) : le résultat est daté, seedé, versionné (`code_version`, `random_seed` de `pyramide_runs`) et re-basé sur les feuilles réconciliées (bornées à 0) puis re-dérivé via S — la cohérence reste exacte quel que soit le backend. Approximations V1 documentées dans le code : alignement positionnel des historiques creux sans dates, fitted in-sample = proxy naïf lag-1 ; en dessous de 8 points alignés, MinT est refusé et le middle-out prend le relais.
5. **`pyramide_runs.recon_method` devient réelle (migration 054)** : elle porte la méthode **effectivement appliquée** (`middleout`, `mintrace_wls_shrink`), jamais la requête rejetée — un fallback MinT→middle-out persiste `middleout` + warning. Les runs feuilles mono-série portent désormais `none` (défaut honnête). La 054 répare aussi un trou latent de la 038 : `SEASONAL` manquait aux CHECK `pyramide_runs.method` / `pyramide_snapshots.method`.
6. **Frontière graphe : feuilles seulement.** Les agrégats sont persistés et requêtables dans `forecasts`/`forecast_values` (schéma 053) mais ne sont **jamais** matérialisés en `ForecastDemand` : `commit_run` lève `PyramideAggregateCommitError` (409 côté API). Une demande de nœud agrégat dans le graphe double-compterait ses feuilles en propagation. `pyramide_snapshots` garde son contrat feuille NOT NULL — un snapshot n'existe que pour nourrir le commit graphe.
7. **Orchestration par bloc** (`pyramide/hierarchy/runner.py:HierarchicalRunner`) : un run = un bloc (nœud racine du `block_level`), scenario-aware (`scenario_id` estampille chaque ligne ; les lectures d'historique restent invariantes par scénario — les actuals ne forkent pas). Les feuilles de S étant des items (demande tous sites, lecteurs PR2), les forecasts feuilles sont adressés à une `leaf_location_id` paramétrable (le site réseau/central dont le graphe consomme le plan ; la ventilation par site est le rôle de la couche DRP, ADR-020). Générique — aucune constante métier.

## Alternatives rejetées

- **MinT comme chemin par défaut.** Rejeté : non reproductible bit-à-bit, opaque pour la gouvernance (pas de parts explicites lisibles par un humain/agent — design §4), et dépendance optionnelle. Le différenciateur d'Ootils est l'explicabilité + la confiance, pas 2 % d'accuracy (design §2).
- **Vérifier la cohérence a posteriori (tolérance) au lieu de la construire.** Rejeté : une tolérance est un mensonge qui grandit ; la dérivation par S rend l'incohérence impossible plutôt que détectée.
- **Matérialiser les agrégats dans le graphe.** Rejeté : double comptage en propagation, et le contrat de demande du graphe est (item, location) — même refus argumenté que les watchers écrivains dans `shortages` (ADR-021).
- **Parts prévues (mix-aware) plutôt qu'historiques.** Différé, pas rejeté : les parts historiques sont stables et lisibles ; le mix-aware est précisément ce que MinT apporte quand on l'active (design §4 « la vraie réponse moderne »).

## Conséquences

- **Positif :** un plan cohérent multi-niveaux requêtable par niveau, avec provenance complète (méthode de base + modèle sélectionné — qui embarque `season_length` quand un modèle saisonnier gagne — + `recon_method` effective + niveau de prévision) ; golden tests verrouillent l'arithmétique du middle-out.
- **Contrainte assumée :** `leaf_location_id` unique par run — la ventilation multi-sites des feuilles attend la couche DRP ; les profils de proportions par phase de saison restent hors périmètre V1.
- **À surveiller :** la dérive d'API de `hierarchicalforecast` est absorbée par le fallback (jamais par un crash), mais un environnement `[forecast]` cassé dégraderait silencieusement en middle-out — le warning persiste dans le résultat du run, et `recon_method` dit toujours la vérité.

## Références

- `docs/DESIGN-pyramide-forecasting.md` §3-4 — la spécification de l'axe A.
- `src/ootils_core/pyramide/hierarchy/reconcile.py` — middle-out + MinT-shrink.
- `src/ootils_core/pyramide/hierarchy/runner.py` — orchestration par bloc.
- `src/ootils_core/db/migrations/054_pyramide_recon_method.sql` — recon_method réelle.
- `docs/ADR-020-mrp-consolidation.md` — la couche DRP qui consommera les feuilles.
- `docs/ADR-021-shortage-truth.md` — le refus jumeau « un seul écrivain dans le graphe ».
