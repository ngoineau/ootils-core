# ADR-030 — La machine à preuve : snapshots de stock, FVA, chaînage reco → outcome

**Statut :** Accepté — chantier #393 axe A3 **CLOS**, 3 PRs mergées : PR1 (snapshots de stock, migration 067), PR3 (Forecast Value Added, migration 068), PR2 (chaînage reco → outcome + les 5 KPI de preuve, migration 069). Ensemble elles répondent à la question « la flotte d'agents a-t-elle réellement créé de la valeur ? » avec des faits déterministes, jamais un récit.
**Date :** 2026-07-06
**Auteurs :** ootils-core team
**Contexte mesuré :** REVIEW-2026-07-AI-NATIVE (`docs/REVIEW-2026-07-AI-NATIVE.md`, vague A3) et REVIEW-2026-07-APS (`docs/REVIEW-2026-07-APS.md`, item 7).

---

## Contexte

Les deux revues de juillet 2026 pointaient le même trou, du même angle : **la preuve de valeur était impossible**.

- REVIEW-2026-07-AI-NATIVE §4 : « Machine à preuve inexistante. Pas de snapshots stock, pas de chaînage reco→résultat, zéro FVA (grep = 0 hors revues). Le ledger d'audit contient déjà ~80 % de la plomberie. »
- REVIEW-2026-07-APS §7 : « Preuve de valeur impossible aujourd'hui : pas de snapshots de stock historiques, pas de chaînage recommandation→résultat observé. Un snapshot quotidien (item×location×on_hand) + le chaînage `recommendation_id→outcome` transformerait le ledger d'audit existant en machine à ROI — coût faible, impact commercial majeur. »

Le North Star exige que la flotte soit **auditable** (chaque écriture tracée) et que ses sorties soient consommables par des agents de gouvernance qui rejettent les recommandations sans preuve. Or, avant #393, le système savait dire « j'ai recommandé de commander » mais jamais « et voici le déficit qui n'a PAS eu lieu, valant $X ». La flotte produisait des recommandations gouvernées (ADR-026, #340) sans jamais **fermer la boucle** sur ce qui s'était réellement passé. Symétriquement, la couche demande (Pyramide, ADR-019/022/023) produisait un forecast statistique et son backtest (WAPE/MASE, migration 055) mais était incapable de répondre à la question de gouvernance la plus élémentaire : « ce forecast complexe bat-il un benchmark que personne ne défendrait de déployer ? ».

La contrainte forte : **aucune de ces réponses ne peut venir d'un LLM**. Une machine à preuve dont le verdict serait persuadable par un modèle stochastique ne prouve rien — elle raconte. Le calcul de preuve doit être une fonction déterministe des faits observés, rejouable à l'identique. C'est le corollaire direct du North Star « deterministic core, stochastic edge » : les agents proposent, le cœur déterministe score.

Trois capacités, trois PRs, une seule machine. Chacune est **baseline-only en V1** — pour une raison propre à chacune, gravée ci-dessous — et **None-honnête** partout : un `NULL` signifie « non calculable / pas de donnée », jamais un `0` masqué.

## Décision

### 1. Snapshots de stock — l'historisation par site (PR1, migration 067)

`inventory_snapshots` historise le on-hand point-in-time par coordonnée `(scenario, item, location, as_of_date)`, une ligne par coordonnée par jour. C'est le socle que les deux autres capacités lisent pour comparer « ce qu'on avait projeté » à « ce qui s'est réellement passé ».

- **Split DB-boundary, comme le reste du moteur** (mrp/core pur, mrp/loader porte le SQL) : `capture_snapshot` (`engine/snapshot/capture.py`) est un SELECT-only déterministe qui n'écrit **rien** (testable contre un golden sans persistance) ; `persist_snapshot` est l'unique writer, un upsert idempotent sur l'UNIQUE `(scenario_id, item_id, location_id, as_of_date)`. Re-capturer le même jour écrase, ne duplique jamais.
- **Par site, jamais poolé — la leçon DRP (ADR-028).** Le on-hand est scanné par `(item, location)` : le snapshot est un fait de niveau site. C'est délibérément **pas** le scan item-poolé de `mrp/loader.load_planning_data` (qui SOMME le on-hand à travers les locations pour l'échelon make/buy). Un nœud sans `location_id` est ignoré — un on-hand non-localisé n'a pas de place dans un historique par site (et la FK `location_id` est NOT NULL).
- **`severity_usd` NULL-différée, honnêtement.** `first_shortage_date` / `shortage_severity_usd` sont capturés `NULL` en PR1 (ensemble : les deux `NULL` ou les deux renseignées). Raison gravée : la math canonique de pénurie (`mrp/core.first_shortage`) est **item-poolée** (on-hand poolé + demande poolée) ; estamper sa date/severity sur chaque ligne par-location attribuerait une pénurie poolée à des sites individuels et **double-compterait** le même déficit item à travers N locations — exactement l'effondrement par-site que ce module (et ADR-028) existent pour empêcher. Le `projected_deficits` par-location du DRP est plus proche en grain mais (a) clé par external_id, pas les UUIDs de cette table, (b) ne nette **aucun** receipt firm, donc diverge de la vérité `shortages`, (c) rend un déficit en unités, pas la severity en $ du ShortageDetector. Une severity par-`(item, location)` honnête exige une projection par-location qui nette les FPO **et** applique la formule $ — travail futur (voir Hors périmètre).
- **N'écrit jamais dans `shortages` (ADR-021).** `shortage_severity_usd` est une **capture dénormalisée** de la severity AS OF le jour du snapshot, PAS une seconde source de vérité de pénurie. Le ShortageDetector reste canonique ; ce module ne lit ni n'écrit `shortages`.
- **Scenario-scopé en schéma, baseline-only en V1.** La colonne `scenario_id` existe (cohérence schéma + forkabilité future) et le scan lit les nœuds du scénario passé — un fork snapshoterait donc l'état du fork. Mais les captureurs V1 (CLI `scripts/snapshot_inventory.py`, cron, `POST /v1/snapshots`) passent toujours baseline.
- **Surface :** CLI cron-friendly (`scripts/snapshot_inventory.py`, source `cli`) + `POST /v1/snapshots` (source `api`, scope `ingest` — un snapshot est une écriture opérationnelle, pas un `read`) + `GET /v1/snapshots` (scope `read`). Kill switch `OOTILS_SNAPSHOTS_ENABLED`.

### 2. Forecast Value Added (FVA) — le stat vaut-il sa complexité ? (PR3, migration 068)

`fva_wape` / `fva_mase` = `naive − stat`, quatre colonnes additives typées nullables sur `pyramide_accuracy_metrics` (migration 055). FVA n'est **pas** une table sœur : c'est un **attribut** du même backtest rolling-origin — même `run_id`, même granularité per-horizon, même contrat None-honnête, calculé sur les **mêmes cutoffs** que les métriques stat qu'il côtoie. Une table séparée dupliquerait `(run_id, horizon)` et forcerait un join pour comparer un modèle à son propre baseline.

- **Baseline = seasonal-naive TRIVIALE.** `y_hat[t] = y[t − season_length]` — répète la valeur d'une saison en arrière, le modèle le plus trivial qui respecte encore la saisonnalité. C'est délibérément un benchmark que personne ne défendrait de déployer (`seasonal_naive_forecast`, `engine/../pyramide/fva.py`) : si le pipeline stat ne bat pas « même période la saison dernière », sa complexité ne se paie pas. Ce n'est **pas** le `SeasonalForecaster` complet (niveau × indices saisonniers), qui gonflerait artificiellement la valeur ajoutée.
- **Consistance méthodologique — l'invariant porteur.** FVA n'est honnête que si la naïve est scorée sur le MÊME backtest rolling-origin que le stat : même série, mêmes cutoffs, même horizon, même normalisation d'erreur. On le garantit en **réutilisant** `accuracy.evaluate_rolling_origin` (la fonction même qui a produit le rapport stat), en **récupérant** l'origine exacte du stat depuis son rapport (`min_train = len(history) − stat_report.n_cutoffs`) plutôt qu'en la devinant, et avec un **garde-fou** `n_cutoffs` identique : si la naïve ne peut être alignée aux cutoffs du stat (p. ex. la première origine partagée détient moins d'une saison pleine), `compute_fva` retourne `None` — jamais une comparaison décalée.
- **Sens de signe (porteur) : `fva = naive − stat`.** POSITIF = le stat bat la naïve (WAPE/MASE plus bas est meilleur, donc retirer de l'erreur donne un FVA positif). Un **FVA négatif est un résultat légitime et honnête** (le stat a perdu contre la naïve triviale sur cette donnée) et n'est **jamais clampé** — pas de CHECK de signe sur les colonnes `fva_*` (contrairement à une quantité). Les consommateurs (Decision Ladder) décident ce qu'un FVA non-positif signifie ; le module ne clampe pas.
- **None-honnête strict.** `naive_*` sont `NULL` quand la naïve est indéfinie (historique < 1 saison pleine → pas de valeur une saison en arrière) ou non alignable ; `fva_*` sont `NULL` dès qu'un opérande est `NULL` (naïve manquante, ou wape/mase stat déjà `NULL` per migration 055). Un FVA `NULL` = « non comparable », JAMAIS « aucune valeur ajoutée ».
- **Pur, DB-free, déterministe.** Séquences Decimal en mémoire in, `FvaResult` out. Aucun hasard, aucune horloge, aucune I/O — golden-testable isolément, même discipline que `pyramide/accuracy.py`.
- **Câblage & exposition.** FVA est calculé **uniquement pour la ligne agrégée** (`is_aggregate`, `repository.py`) — les lignes per-horizon restent `NULL`. Il suit le scénario du run (per-run, pas baseline-only par colonne). Persisté via `persist_accuracy_metrics(..., fva=...)`, exposé sur le GET run result de Pyramide (mapping `naive_wape`/`naive_mase`/`fva_wape`/`fva_mase`). Historique absent ⇒ `fva=None` ⇒ colonnes `NULL`, métriques 055 inchangées (déploiement rolling-safe, purement additif).

### 3. Chaînage reco → outcome — le cœur de preuve de valeur (PR2, migration 069)

`recommendation_outcomes` chaîne chaque recommandation gouvernée (migration 039) à son résultat réel observé et, quand c'est calculable, valorise le $ de pénurie évité. C'est la table qui transforme « on a dit de commander » en « et voici le déficit qui n'a PAS eu lieu, valant $X » — l'histoire de valeur de la flotte.

- **Classifieur DÉTERMINISTE, jamais un LLM.** `evaluate_outcome` (`engine/outcome/evaluator.py`) est une fonction PURE : un verdict 5-états + les figures $ None-honnêtes, à partir des faits chargés. Re-jouer sur les mêmes faits rend la même classification et les mêmes $. C'est un refus explicite du North Star (« agents propose, deterministic core scores ») — aucun modèle stochastique dans le chemin de scoring.
- **Les 5 statuts :**
  - `AVOIDED` — reco qui a agi, snapshot présent, déficit observé effectivement nul (≤ `max(prédit × AVOIDED_EPS_RATIO, AVOIDED_EPS_ABS)`) : `avoided_severity_usd` = le $ prédit (None-honnête si pas de base de coût).
  - `MATERIALIZED` — reco qui a agi, snapshot présent, déficit observé ≥ `prédit × MATERIALIZED_FLOOR_RATIO` (la pénurie a eu lieu quand même) : `avoided_severity_usd` = `0` (rien de réellement évité — distinct du `NULL` « non calculable »).
  - `PARTIAL` — reco qui a agi, snapshot présent, déficit strictement entre le plafond AVOIDED et le plancher MATERIALIZED : `avoided_severity_usd` = la fraction évitée du $ prédit = `prédit × (1 − observé/prédit)`, None-honnête.
  - `NOT_APPLICABLE` — **contrefactuel des recos qui n'ont JAMAIS agi** (statut hors `ACTED_STATUSES` = `{APPROVED, APPLIED}` : DRAFT/REVIEWED/REJECTED/EXPIRED). On ENREGISTRE le déficit qui s'est produit (`observed_deficit_qty`, ou 0) comme **signal de coût d'inaction**, mais on ne crédite **aucun** `avoided_severity_usd` (la reco n'a pas agi — `None`, pas `0`).
  - `INDETERMINATE` — reco qui a agi mais **pas de snapshot** d'observation à la coordonnée/jour : impossible de classer honnêtement (« pas de pénurie » pourrait juste vouloir dire « pas encore observé »). Tout observé/évité est `None`.
- **Seuils 🎯-ajustables pilote, documentés dans l'evaluator** (pas de magie cachée, pas en DB, pas chez le consommateur — classer une observation physique est le travail de l'evaluator) : `AVOIDED_EPS_RATIO` (défaut 0.05), `AVOIDED_EPS_ABS` (défaut 1, plancher absolu sous-unité), `MATERIALIZED_FLOOR_RATIO` (défaut 0.90). Ils vivent dans `engine/outcome/evaluator.py`.
- **Base $ prédite — un proxy honnête, documenté.** La reco ne persiste **pas** la `severity_score` days-weighted du ShortageDetector ; elle persiste la quantité de déficit et (en `evidence`) un coût unitaire. Le $ crédité comme évité est donc `predicted_deficit_qty × unit_cost` — une figure de **valeur de déficit**, PAS la severity `days × qty × unit_cost` de la table `shortages`. C'est la meilleure valorisation auto-contenue dérivable d'une reco seule ; sans coût unitaire dérivable, la figure est None-honnête (`avoided_severity_usd = None`, « pas de base de coût »), jamais un `0` masqué.
- **Lecture seule / écriture ciblée (ADR-021).** `evaluate_and_persist` est READ-ONLY sur `recommendations` / `shortages` / `inventory_snapshots` (on LIT `shortages`, on ne l'écrit JAMAIS) et écrit **uniquement** `recommendation_outcomes` via upsert idempotent `ON CONFLICT (recommendation_id, evaluated_as_of) DO UPDATE`. Ne commit pas (le caller possède la transaction).
- **Baseline-only — justification gravée.** Un outcome est le **réel observé** ; il est toujours baseline. Un fork est **simulé, pas observé** — on ne peut pas « observer » le résultat réel d'un contrefactuel qui n'a jamais touché la réalité. C'est pourquoi la V1 n'évalue que baseline, et c'est une contrainte de nature, pas une limite d'implémentation.
- **`scenario_id` hérité, pas de colonne redondante.** La table ne porte **aucune** colonne `scenario_id` ; le scénario est hérité via `recommendation_id → recommendations.scenario_id`. Les KPI qui slicent par scénario JOIN `recommendations`. Double intention : (1) pas de coordonnée dupliquée sujette à dérive ; (2) **pas de FK vers `scenarios`**, donc la table reste **hors** du garde-fou `test_scenario_fk_retention` (cf. la leçon FK ci-dessous) — une politique de suppression d'outcome (CASCADE depuis la reco) entrerait sinon en conflit avec la règle RESTRICT.
- **Surface :** CLI cron (`scripts/evaluate_outcomes.py`, à lancer après le snapshot du jour) + `POST /v1/outcomes/evaluate` (scope `ingest` — écrit des verdicts persistants ; kill switch `OOTILS_OUTCOMES_ENABLED`) + `GET /v1/recommendations/{id}/outcome` (dernier verdict, scope `read`) + `GET /v1/outcomes/summary` (les 5 KPI, scope `read`).

### 4. Les 5 KPI de preuve (`GET /v1/outcomes/summary`)

Cinq agrégats SQL, scopés à un scénario + une fenêtre d'observation, chacun **NULL/0-honnête** — les compteurs `*_basis_count` rendent le dénominateur explicite pour distinguer « aucune donnée » (`NULL`) d'un « zéro » réel :

| # | KPI | Définition | None/0-honnête |
|---|---|---|---|
| 1 | `pct_shortages_avoided` | `AVOIDED / (AVOIDED+MATERIALIZED+PARTIAL)` sur les recos ACTED | `NULL` si dénominateur 0 (aucun outcome de pénurie à noter) ; `avoided_basis_count` = dénominateur |
| 2 | `avoided_severity_usd_total` | `SUM(avoided_severity_usd)` | `NULL` si aucune ligne avoided-$ n'existe (jamais `0` masqué) ; distingué par le compte FILTERé |
| 3 | `avg_fva_wape` | moyenne du **vrai** FVA (`fva_wape` = naive − wape, migration 068) sur les lignes agrégées en scope | `NULL` si aucune ligne WAPE agrégée non-`NULL` ; `fva_basis_count` = dénominateur. Positif = le stat bat la naïve ; négatif légitime, non clampé |
| 4 | `reco_approval_rate` | `APPROVED(+APPLIED) / total recos émises` | `NULL` si aucune reco émise ; `reco_total_count` = dénominateur |
| 5 | `cost_of_inaction_usd` | `SUM(observed_deficit_qty × unit_cost)` sur les `NOT_APPLICABLE` de recos jamais approuvées ayant matérialisé | `NULL` si aucune telle ligne |

Le KPI 5 valorise le déficit matérialisé avec la **même** convention de coût unitaire que l'evaluator pour le côté avoided-$ (`evidence->>'unit_cost'` d'abord, sinon `estimated_cost / recommended_qty`) — une seule convention de valorisation, les deux sens. Le cast `evidence` est **gardé** par un regex de forme numérique : un `unit_cost` malformé est ignoré (la base `estimated_cost/qty` prend le relais), il n'avorte pas l'agrégat.

**Les seuils du classifieur (§3) ET le choix des 5 KPI phares sont 🎯-ajustables pilote.** Ce sont des réglages de démonstration de valeur, à calibrer avec le pilote, pas des constantes métier gravées dans le marbre.

## Alternatives rejetées

- **Un LLM pour classer l'outcome (AVOIDED vs MATERIALIZED…).** Rejeté par principe : une machine à preuve dont le verdict est persuadable par un modèle stochastique ne prouve rien. Le classifieur est une fonction pure des faits (North Star « deterministic core »).
- **Estamper la severity de pénurie par-location dans les snapshots dès la PR1.** Rejeté (différé) : la math canonique est item-poolée ; l'estamper par-location double-compterait le même déficit à travers N sites — la régression par-site qu'ADR-028 ferme. Le `projected_deficits` du DRP est plus proche en grain mais nette zéro receipt firm et clé par external_id. Nécessite une projection par-location nettant les FPO + la formule $ — hors V1.
- **FVA comme table sœur.** Rejeté : dupliquerait `(run_id, horizon)` et forcerait un join pour comparer un modèle à son propre baseline. FVA est un attribut du même backtest → quatre colonnes sur la même ligne.
- **Baseline SeasonalForecaster complet (niveau × indices) comme benchmark FVA.** Rejeté : gonflerait artificiellement la valeur ajoutée. Le benchmark doit être trivial — la seasonal-naive brute — pour que « battre le benchmark » veuille dire quelque chose.
- **Clamper le FVA négatif à 0.** Rejeté : un stat qui perd contre la naïve est un fait honnête que la gouvernance doit voir, pas masquer.
- **Une colonne `scenario_id` sur `recommendation_outcomes`.** Rejeté : coordonnée dupliquée sujette à dérive, et une FK vers `scenarios` entrerait en conflit avec la règle RESTRICT du garde-fou pendant que l'outcome a besoin d'un CASCADE depuis sa reco. Le scénario est hérité via la reco.
- **Créditer un `avoided_severity_usd` aux recos jamais approuvées.** Rejeté : une reco restée DRAFT/REJECTED n'a jamais influencé la réalité — son outcome est un contrefactuel (`NOT_APPLICABLE`), pas un crédit. On en tire le coût d'inaction, pas de la valeur évitée.
- **`NULL` traité comme `0` dans les KPI.** Rejeté partout : `NULL` = « pas de donnée », `0` = « zéro réel ». Les confondre sur/sous-estimerait la valeur prouvée de la flotte — ce qu'une machine à preuve ne doit jamais faire.

## Conséquences

- **Positif :** la boucle est fermée. La flotte peut enfin répondre « voici le déficit qui n'a PAS eu lieu, valant $X » et « le forecast stat bat/perd contre la naïve triviale de tant » — avec des faits déterministes, rejouables, audités (ledger immuable `recommendation_outcomes` + colonnes FVA + historique `inventory_snapshots`). Le ledger d'audit existant devient une machine à ROI, à coût faible (le gros de la plomberie préexistait).
- **Négatif / dette assumée en V1 :**
  - `severity_usd` par-site des snapshots reste `NULL` (enrichissement par-location nettant les FPO différé — voir Hors périmètre).
  - Le $ évité est un proxy **valeur de déficit** (`qty × unit_cost`), pas la severity days-weighted du ShortageDetector — documenté honnêtement, None quand pas de base de coût.
  - Attribution **corrélationnelle, pas causale** : un outcome `AVOIDED` observe que le déficit prédit n'a pas eu lieu APRÈS la reco, pas que la reco l'a *causé*. C'est une corrélation temporelle honnête, pas une preuve de causalité (voir Hors périmètre).
  - FVA calculé pour la ligne agrégée seulement (per-horizon restent `NULL`).
  - Pas d'émission `StreamChanges`/SSE à la création d'un snapshot ou d'un outcome — cohérent avec le reste de la flotte, dépend de l'axe A1 (ADR-027).
  - Baseline-only sur les trois capacités (justifié par nature pour l'outcome ; par choix V1 pour snapshot/FVA suit le scénario du run).
- **Reste à faire :** severity par-site enrichie ; attribution causale ; A/B ; ML d'outcome ; émission SSE. Tous **hors V1**, listés ci-dessous.

## Hors périmètre V1 (explicitement)

- **Attribution causale fine.** V1 = corrélation temporelle honnête (le déficit prédit n'a pas eu lieu après la reco), PAS causalité (la reco l'a causé). Distinguer les deux exige un contrefactuel observé, impossible sur du réel.
- **A/B testing** des recommandations (bras traité vs contrôle).
- **ML d'outcome** (prédire l'issue au lieu de l'observer déterministiquement).
- **Émission SSE / `StreamChanges`** à la capture d'un snapshot ou d'un outcome — dépend de l'axe A1 (ADR-027).
- **`severity_usd` par-site enrichie** : projection par-location nettant les FPO + formule $ du ShortageDetector, pour lever le `NULL`-différé des snapshots.
- **Rétention / partitionnement des snapshots** à l'échelle pilote (36K items = 36K lignes/jour) — négligeable en démo, à traiter en vague C1 (voir `docs/SCALABILITY.md`).

## Convention de schéma — la leçon FK

Le garde-fou d'intégration `test_scenario_fk_retention` (`tests/integration/test_scenario_fk_retention.py`, migration 032) impose que **toute FK référençant `scenarios(scenario_id)` déclare EXPLICITEMENT `ON DELETE RESTRICT`** — le défaut Postgres étant `NO ACTION`, il faut l'écrire (`confdeltype = 'r'` vérifié par le test). Un scénario ne doit jamais être supprimé sous des données vivantes ; la seule voie est le soft-delete `status='archived'` (ADR-011).

Cette machine à preuve l'applique de deux manières :
- `inventory_snapshots.scenario_id` (migration 067) déclare explicitement `ON DELETE RESTRICT` — cité en commentaire de migration comme rappel de la convention.
- `recommendation_outcomes` (migration 069) **n'a pas** de colonne `scenario_id` du tout : elle hérite le scénario via `recommendation_id`, ce qui la garde hors du garde-fou (pas de FK `scenarios` → pas de conflit avec le CASCADE dont l'outcome a besoin depuis sa reco).

## Références

- `docs/REVIEW-2026-07-AI-NATIVE.md` — vague A3 (« Machine à preuve »), §4.
- `docs/REVIEW-2026-07-APS.md` — item 7 (« Preuve de valeur impossible aujourd'hui »).
- `docs/ADR-021-shortage-truth.md` — `shortages` est propriété exclusive du ShortageDetector ; snapshots et outcome LISENT, n'écrivent jamais.
- `docs/ADR-028-drp-fair-share-rounding.md` — la leçon par-site vs poolé qui justifie le `severity_usd` NULL-différé des snapshots.
- `docs/ADR-026-reschedule-fpo.md` — les recommandations gouvernées que l'outcome chaîne à leur résultat.
- `docs/ADR-011-scenario-retention.md` — la politique RESTRICT / soft-delete des scénarios.
- **Snapshots (PR1) :** `src/ootils_core/engine/snapshot/capture.py` (`capture_snapshot` SELECT-only, `persist_snapshot` upsert) ; `src/ootils_core/api/routers/snapshots.py` ; `scripts/snapshot_inventory.py` ; `src/ootils_core/db/migrations/067_inventory_snapshots.sql`.
- **FVA (PR3) :** `src/ootils_core/pyramide/fva.py` (`compute_fva`, `seasonal_naive_forecast`, `resolve_season_length`, `FvaResult`) ; `src/ootils_core/pyramide/repository.py` (calcul aggregate-only + persistance) ; `src/ootils_core/db/migrations/068_fva.sql`.
- **Outcome + 5 KPI (PR2) :** `src/ootils_core/engine/outcome/evaluator.py` (`evaluate_outcome` PUR, `evaluate_and_persist`, seuils `AVOIDED_EPS_RATIO`/`AVOIDED_EPS_ABS`/`MATERIALIZED_FLOOR_RATIO`) ; `src/ootils_core/api/routers/outcomes.py` (les 5 KPI SQL) ; `scripts/evaluate_outcomes.py` ; `src/ootils_core/db/migrations/069_recommendation_outcomes.sql`.
- **Garde-fou FK :** `tests/integration/test_scenario_fk_retention.py` ; `src/ootils_core/db/migrations/032_scenario_fk_retention.sql`.
