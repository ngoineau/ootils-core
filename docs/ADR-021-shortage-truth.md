# ADR-021 — Vérité de pénurie unique : maths mrp_core, système table `shortages`

**Statut :** Accepté — chantier #343 (branche `feat/aps-c1-shortage-truth`) ; valorisation $ livrée au #342.
**Date :** 2026-07-02
**Contexte mesuré :** REVIEW-2026-07-APS (deux vérités de pénurie divergentes), garde-fou de parité MRP #332, seed démo `scripts/seed_demo_data.py`.

---

## Contexte

Ootils répond à « cet item est-il en pénurie ? » à deux endroits, nés de deux lignées différentes :

| | A — kernel (table `shortages`) | B — watchers (`engine/mrp/core.py`) |
|---|---|---|
| Producteur | propagation (`ShortageDetector` Python / `SHORTAGES_SQL`) sur les buckets ProjectedInventory | `load_planning_data` → `consume_demand` → `first_shortage` (read-only, en mémoire) |
| Sémantique | **chaque** bucket en dépassement, par (item, location), classes `stockout` ET `below_safety_stock` | **premier** bucket hebdo sous safety stock, par item poolé, demande **consommée** (`max(orders, forecast)` + demand time fence + proration) |
| Demande vue | somme des nœuds order + forecast câblés par arêtes `consumes` (brute) | consommation de prévision correcte (ADR-020, la maths canonique MRP) |
| Persistance | UUID déterministes, sévérité, chaîne causale ADR-004, cycle de vie `active`/`resolved`, API `/v1/issues` | aucune — recommandations gouvernées L1 DRAFT à côté (`agent_shortage_watcher.py`) |

Avant le #342, les deux vérités ne partageaient même pas la valorisation : le kernel priorisait avec un coût proxy = 1 quand les watchers valorisaient en $ réels via `mrp_core.cost_of`. Deux vérités sans contrat entre elles = le même risque de dérive silencieuse que les deux moteurs MRP avant l'ADR-020 (×48 non détecté).

## Décision

Il n'y a **pas** un moteur gagnant et un moteur perdant : chacun est canonique sur son axe, et un contrat testé en CI les tient ensemble.

1. **La MATHS canonique de pénurie est `mrp_core`** (`src/ootils_core/engine/mrp/core.py`, ADR-020) : c'est la seule implémentation qui consomme la prévision correctement (`max(orders, forecast)`, demand time fence, proration, dédup multi-location). Toute nouvelle logique de projection de pénurie (sémantique de demande, seuils, consommation) se décide là et se propage vers le kernel, jamais l'inverse.
2. **Le SYSTÈME canonique de persistance/requête est la table `shortages`** : UUID déterministes (`deterministic_uuid`), `severity_score` valorisé en $ (#342), chaîne causale explicable (ADR-004), cycle de vie (`resolve_stale`), et l'API `/v1/issues`. Tout consommateur qui veut *lire* des pénuries persistées lit là — agents inclus.
3. **Les watchers n'écrivent JAMAIS dans `shortages`.** Read-only by design, et c'est un refus argumenté :
   - la table appartient exclusivement au `ShortageDetector` (invariant documenté dans `detector.py`) ; un deuxième écrivain casserait les UUID déterministes, le lien `calc_run_id`/`pi_node_id` et le `resolve_stale` ;
   - une ligne `shortages` sans PI node ni chaîne causale ADR-004 serait une recommandation sans preuve — rejetée par la gouvernance (North Star : explicable ou refusé) ;
   - les watchers ont déjà leur canal d'écriture gouverné : recommandations L1 DRAFT auditées (`agent_runs`), jamais des faits kernel.
4. **`severity_score` = valeur $** via la précédence `mrp_core.cost_of` (unit_cost fournisseur négocié, puis `items.standard_cost`, puis proxy 1 pour les items non pricés) — livré au #342, tenu en phase dans les trois implémentations (Python `propagator.py`, SQL `SHORTAGES_SQL`, Rust qui réutilise le SQL).
5. **Garde-fou permanent en CI :** `tests/integration/test_shortage_truth_consistency_integration.py`. Sur le seed démo, après propagation complète baseline : **items(B) ⊆ items(A)** — tout item en pénurie pour les watchers doit exister dans `shortages`. Inclusion (pas égalité) parce que A est structurellement plus large (tous les buckets + `below_safety_stock` + demande brute sommée ≥ demande consommée) tandis que B ne voit que le premier passage sous safety des items à demande indépendante. Toute divergence = échec bruyant avec le diff des items ; ensemble B vide sur le seed = échec aussi (pas de faux-vert).

## Alternatives rejetées

- **Watchers écrivains dans `shortages`.** Rejeté : double-écrivain sur une table à propriétaire unique, sémantiques incompatibles (bucket hebdo item-poolé vs PI daily par location — quel `pi_node_id` ?), pas de chaîne causale, et le cycle de vie `resolve_stale` supprimerait ou ressusciterait les lignes de l'autre écrivain au gré des calc runs.
- **Fusionner les deux moteurs en un seul.** Rejeté (même arbitrage que l'ADR-020, PAS 4) : le kernel de propagation est incrémental, événementiel et par-location — le remplacer par la projection en mémoire de `mrp_core` détruirait l'incrémentalité (ADR-003) ; inversement porter la consommation de prévision dans le kernel est le vrai chemin cible, mais il passe par le PAS 4 de l'ADR-020 (B surcouche du cœur), pas par un big-bang. D'ici là, le test de cohérence tient le contrat.
- **Ne rien faire (deux vérités indépendantes).** Rejeté : c'est exactement la configuration qui a laissé le ×48 MRP grandir en silence (ADR-020) — et la tour de contrôle pénurie est le wedge V1, pas un module secondaire.

## Conséquences

- **Positif :** le front (watchers, tri par valeur $) et le back (kernel, `/v1/issues`) classent les mêmes pénuries avec la même valorisation ; une régression de câblage (nœud de demande sans arête `consumes`) casse la CI au lieu de mentir au planner.
- **Contrainte assumée :** l'inclusion est plus faible que l'égalité — un item vu par A seulement (pénurie par-location poolée par B, item sans demande indépendante) ne fait pas échouer le test. Le resserrage éventuel (égalité sur les stockouts francs) attend le PAS 4 de l'ADR-020.
- **À surveiller :** le test est calé sur l'horizon PI seedé (90 j). Élargir l'horizon de B sans élargir le seed casserait le contrat pour une mauvaise raison — documenté dans le test.

## Références

- `docs/ADR-020-mrp-consolidation.md` — la maths canonique MRP et le PAS 4.
- `docs/ADR-004-explainability.md` — la chaîne causale exigée des pénuries persistées.
- `src/ootils_core/engine/kernel/shortage/detector.py` — propriétaire exclusif de `shortages`.
- `src/ootils_core/engine/orchestration/propagator_sql.py:SHORTAGES_SQL` — détection SQL + valorisation #342.
- `scripts/agent_shortage_watcher.py` — le consommateur read-only type de la vérité B.
- `tests/integration/test_shortage_truth_consistency_integration.py` — le garde-fou.

---

## Amendement — 2026-07-12 (ADR-039, PURGE-1)

Un troisième acteur touche désormais `shortages`, mais borné à un rôle de garbage-collection sur de l'historique déjà mort, jamais à la vérité de pénurie elle-même. [ADR-039](ADR-039-scenario-archive-cleanup.md) (PURGE-1, migration 076) introduit `apply_shortage_retention` (`src/ootils_core/engine/maintenance/purge.py`), qui supprime les lignes `status='resolved'` plus vieilles qu'une fenêtre de rétention (défaut 30 jours) ET hors du dernier `calc_run` `completed` du scénario. Ce sweep ne crée, ne valorise et ne résout **jamais** une pénurie — `status='active'` est codé en dur comme non-éligible, jamais un paramètre — et ne touche jamais la vue la plus récente et auditable d'un scénario (son dernier `calc_run` complété est explicitement protégé). `ShortageDetector` reste l'écrivain **exclusif** de la SÉMANTIQUE de pénurie : création, valorisation `$` (§4 ci-dessus), chaîne causale ADR-004, cycle de vie `resolve_stale`. PURGE-1 ne fait que borner la RÉTENTION de l'historique déjà résolu — une opération de maintenance déléguée, jamais un second écrivain de vérité. Voir ADR-039 pour le détail complet, y compris la garde CI qui garantit que `shortages` reste dans la whitelist FK-safe de purge de fork (pénuries d'un scénario ARCHIVÉ purgé) indépendamment de ce sweep de rétention (qui, lui, tourne sur tout scénario, vivant ou archivé).

---

## Amendement — 2026-07-17 (netting consume_demand GREATEST — convergence Truth A/Truth B)

La propagation (Truth A) nette désormais la demande forecast vs commande client au grain fin, par bucket, au lieu de les sommer : `outflow = GREATEST(fc_out, co_out) + dep_out`, où `dep_out` (DependentDemand + TransferDemand) reste additif — demande dérivée/transférée, jamais un forecast qu'une CO pourrait consommer. SQL : `PROPAGATE_SQL` scinde l'agrégat d'outflows par type de nœud de demande dans `outflow_contribs`/`outflows_agg` et applique le `GREATEST` dans `per_bucket` (`src/ootils_core/engine/orchestration/propagator_sql.py:137-178,189-192`). Python : le netting est fait côté appelant, avant `compute_pi_node`, pour que le kernel (`engine/kernel/calc/projection.py`) reste une somme pure — parité bit-exacte préservée (`src/ootils_core/engine/orchestration/propagator.py:519-530,569-579`).

**Convergence vérifiée avec Truth B :** `engine/mrp/core.py:338` fait déjà `v = max(o, f)` — Truth B nettait déjà CO vs forecast avant cet amendement. Truth A s'aligne sur Truth B ; `core.py` lui-même n'est pas modifié, les goldens MRP sont inchangés.

**La RATIONALE de l'inclusion `items(B) ⊆ items(A)` (§5 ci-dessus) évolue ; l'invariant lui-même non.** Avant cet amendement, l'inclusion tenait notamment parce que A sommait CO+forecast (demande brute, majorante) là où B nettait au poolé (demande consommée, minorante). Après cet amendement, A nette lui aussi — mais au grain fin, PI par (item, location, bucket), tandis que B nette au grain poolé, item par item toutes locations confondues sur un bucket hebdomadaire. Or `Σ max(co_i, fc_i) ≥ max(Σ co_i, Σ fc_i)` : le max appliqué au grain fin, avant sommation, domine le max appliqué au grain poolé, après sommation. L'agrégat fin-grain de A reste donc structurellement ≥ à l'agrégat poolé de B, et l'inclusion `items(B) ⊆ items(A)` survit au changement — avec une rationale mise à jour, pas une rationale inchangée.

**Limitation documentée :** la demand time fence de Truth B (`engine/mrp/core.py:331`, `if t < dtf_weeks: v = o` — sous l'horizon gelé, seules les commandes fermes comptent, le forecast est ignoré) n'est pas répliquée dans Truth A ; le netting GREATEST introduit par cet amendement porte uniquement sur forecast-vs-CO par bucket, pas sur la fenêtre temporelle de gel. A reste donc structurellement ≥ B pour cette raison additionnelle, inchangée par cet amendement — tolérance déjà actée au §5 et à la ligne « Contrainte assumée » des Conséquences ci-dessus.

Garde-fou CI inchangé : `tests/integration/test_shortage_truth_consistency_integration.py` reste vert sur le même contrat d'inclusion.

---

## Amendement — 2026-07-17 (« is_stocking », plan modélisation PR-B)

Le premier chargement ERP réel (14 flux TSV) a exposé ~9 400 pénuries fantômes valorisées en $ sur trois canaux de demande virtuels (USA/CAN/ICO) : demande forecast/CO bien réelle, mais **aucune** supply d'aucune sorte (ni PO, ni transfert, ni on_hand, ni planning params) — ce sont des nœuds de routage/allocation virtuels, pas des sites de stockage physiques. DSH (drop-ship) est le cas inverse : entièrement modélisé avec de la vraie supply, et reste stockant. Avant ce chantier, `locations` n'avait aucun moyen de distinguer les deux — toute location était implicitement « stockante » pour la détection de pénurie.

**Décision :** `locations.is_stocking` (migration 081, `BOOLEAN NOT NULL DEFAULT TRUE`) gate la **DÉTECTION** uniquement, jamais la **PROJECTION**. Le CTE `pi_with_ss` de `SHORTAGES_SQL` (`src/ootils_core/engine/orchestration/propagator_sql.py`) fait un `LEFT JOIN locations l ON l.location_id = pi.location_id`, puis filtre dans la clause `WHERE ... AND COALESCE(l.is_stocking, TRUE) = TRUE` — LEFT JOIN (jamais INNER) + COALESCE appliqués en deux clauses distinctes, pour qu'un `location_id` NULL ou absent de `locations` dégrade vers le TRUE par défaut de la migration plutôt que de disparaître silencieusement de la détection par un join miss non lié. Le détecteur Python (`ShortageDetector.detect_with_params(is_stocking=...)`, `engine/kernel/shortage/detector.py`) miroir la même garde côté propagateur en mémoire. Dans les deux moteurs, la PROJECTION (ProjectedInventory, closing_stock négatif inclus) reste calculée pour **toutes** les locations sans exception — l'explicabilité (ADR-004) exige que le nombre existe et soit inspectable même là où il n'est pas surfacé comme pénurie actionnable. Délibérément **pas** modélisé comme `safety_stock = 0` : les pénuries fantômes sont des closing stock **négatifs** (demande sans aucune supply), pas une condition below-safety-stock, et mettre le safety stock à zéro ne les aurait ni supprimées ni honnêtement expliquées.

**Conséquence sur l'invariant du §5 (garde CI) :** l'inclusion **items(B) ⊆ items(A)** vaut désormais **« sur les locations stockantes »** — un item en pénurie côté B (`mrp_core`) sur une location `is_stocking=FALSE` n'a plus vocation à apparaître dans `shortages` (A), et ce n'est pas une régression de couverture : c'est la garde qui fonctionne comme prévu. La rationale du §5 (« B ne voit que le premier passage sous safety des items à demande indépendante ») reste vraie mais est désormais implicitement bornée aux locations où A détecte. Truth B (`load_planning_data`, `src/ootils_core/engine/mrp/loader.py:247-249`) ne filtre **pas** sur `is_stocking` — la table `locations` n'est même pas jointe à cet endroit — et continue de compter honnêtement la demande forecast/CO des canaux virtuels au pooling item-level : c'est de la vraie demande business (quelqu'un a commandé/prévu quelque chose), et B répond à « combien manque-t-il en agrégat », pas à « où dois-je agir ». Le seed CI (`scripts/seed_demo_data.py`) est 100 % `is_stocking=TRUE` (colonne posée avec son défaut, aucune location seed n'est un canal virtuel) : le garde `tests/integration/test_shortage_truth_consistency_integration.py` tourne donc inchangé, sans exercer la nouvelle branche d'exclusion — documenté ici pour que ce ne soit pas un faux sentiment de couverture.

**Limitation V1 assumée :** flipper `is_stocking` sur une location **après** qu'un premier chargement/calcul ait déjà tourné dessus exige un **full recompute explicite** (`POST /v1/calc/run {"full_recompute": true}`) pour que les lignes `shortages` existantes de cette location disparaissent ou apparaissent en cohérence avec le nouveau flag. Aucun re-dirty incrémental n'est câblé pour un simple changement de `is_stocking` en V1 — un `UPDATE locations SET is_stocking = ...` seul ne marque rien dirty et laisse les `shortages` déjà persistées stale jusqu'au prochain recompute complet ou jusqu'à ce qu'un autre événement redirtie ces PI par ailleurs. Les valeurs réelles USA/CAN/ICO=FALSE sont posées par le code d'ingestion/chargement au moment du premier chargement, jamais par la migration elle-même (voir l'en-tête de la migration 081).

### Références (amendement is_stocking)

- `src/ootils_core/db/migrations/081_location_is_stocking.sql` — colonne + doctrine complète en en-tête.
- `src/ootils_core/engine/orchestration/propagator_sql.py:SHORTAGES_SQL` (CTE `pi_with_ss`) — garde SQL.
- `src/ootils_core/engine/kernel/shortage/detector.py:detect_with_params` — garde Python (miroir).
- `src/ootils_core/engine/mrp/loader.py:247-249` — Truth B, demande forecast poolée, non filtrée par `is_stocking`.

---

## Amendement — 2026-07-18 (`safety_scope` national, ADR-043 §3, DESC-1 PR-C)

**Contexte :** [ADR-043](ADR-043-demand-descent.md) (« La descente de demande ») documente une décision pilote actée le 2026-07-18 mais explicitement **pas encore appliquée** dans son propre corps — voir ADR-043 §3 : « Ceci est une décision pilote actée le 2026-07-18, pas un fait déjà implémenté. L'amendement formel d'ADR-021 … et son code … sont portés par PR-C. » Cet amendement formalise cette décision côté ADR-021 et documente le code livré par PR-C (`src/ootils_core/engine/kernel/shortage/policy.py`, et son branchement dans `SHORTAGES_SQL` et `ShortageDetector.detect_with_params`).

Le modèle métier du pilote (ADR-043, verbatim) : la sécurité de stock est **mutualisée au national** — le risque de rupture est absorbé par un pool national (risk pooling), et une rupture *locale* se **route** vers un centre qui dispose de stock (via DRP) plutôt que de déclencher un réapprovisionnement isolé sur le centre en tension. Le coussin national **existe déjà** : Truth B (`src/ootils_core/engine/mrp/loader.py:93-113`) somme `safety_stock_qty` par item sur toutes les locations depuis avant ce chantier — cet amendement ne touche **pas** ce calcul, seulement la détection par-site côté Truth A.

**Décision :** nouvelle politique `safety_scope`, résolue en un point unique — `engine.kernel.shortage.policy.safety_scope()` — lue depuis `OOTILS_SAFETY_SCOPE`, **défaut `national`** (l'arbitrage pilote du 2026-07-18, documenté comme tel dans le module — pas un fallback silencieux). Deux valeurs valides :

- **`national`** (défaut) : la détection **par centre** (Truth A) ne déclenche plus que sur le **stockout physique** (`closing_stock < -EPS`) — la branche `below_safety_stock` par-site est structurellement désactivée, dans les deux moteurs.
- **`per_site`** : comportement pré-DESC-1, inchangé byte-for-byte — `below_safety_stock` continue de se déclencher par (item, location) exactement comme avant.

Application en miroir strict entre les deux moteurs (même pattern que l'amendement `is_stocking`) :

- **SQL** — la CTE `pi_with_ss` de `SHORTAGES_SQL` (`src/ootils_core/engine/orchestration/propagator_sql.py:308-359`) NULL-ise `safety_stock_qty` quand `%(safety_scope_national)s` est vrai (`CASE WHEN %(safety_scope_national)s THEN NULL::numeric ELSE ipp_ss.ipp_ss END`) — **NULL, jamais un `0` littéral** : un `0` laisserait passer la frange `[-EPS, 0)` que `SHORTAGE_EPSILON` existe pour absorber, ce qui ferait fuiter une ligne `below_safety_stock` quasi-nulle même en scope national. La branche `shortage_rows` (`WHERE closing_stock < -1e-9 OR (safety_stock_qty IS NOT NULL AND ...)`) ne peut alors plus matcher que sur le stockout physique. `%(safety_scope_national)s` est résolu une seule fois par calc_run dans `shortage_params()` (`propagator_sql.py:423-441`), aux côtés de `scenario_id`/`calc_run_id` — un `OOTILS_SAFETY_SCOPE` mal configuré échoue bruyamment (`ValueError`) tout au début du run, jamais au milieu d'une requête.
- **Python** — `ShortageDetector.detect_with_params(safety_scope=...)` (`src/ootils_core/engine/kernel/shortage/detector.py:83-172`) applique le même NULL-out (`effective_safety_stock_qty = None if safety_scope == "national" else safety_stock_qty`) avant le test de sévérité. Le défaut du paramètre reste `'per_site'` — inerte pour tout appelant qui l'ignore (tests existants, appels standalone) — mais l'unique site de production (`PropagationEngine._recompute_pi_node`, appelé depuis `_propagate`, `src/ootils_core/engine/orchestration/propagator.py:322-341,467-491,744-752`) résout la vraie politique une fois par calc_run (`resolve_safety_scope()`) et la propage explicitement, exactement comme `is_stocking`.
- **Rust** — hérité sans modification : les deux points d'appel de détection dans `propagator_rust.py` réutilisent la même chaîne `SHORTAGES_SQL` sur la session Python.
- **Les deux gates (`is_stocking` et `safety_scope`) sont indépendantes et cumulatives** : `is_stocking` exclut entièrement une location de la détection (canal virtuel, aucune supply) ; `safety_scope=national` ne restreint, sur les locations restantes, que le SEUIL testé (stockout physique seul, plus le seuil de sécurité). Une location `is_stocking=FALSE` reste exclue quel que soit `safety_scope`.

**Conséquence sur le premier chargement — 1 051 182 lignes `below_safety_stock` requalifiées.** Le premier chargement réel (18/07) a matérialisé 1 051 182 lignes `shortages` de classe `below_safety_stock` (compte mesuré — RAPPORT-PREMIER-CHARGEMENT-2026-07-18, sévérité cumulée ~2,73 Md$ jour-article ; ADR-043 §3 corrigé en conséquence) sous l'ancien comportement per-site. PR-C ne les corrige ni ne les supprime par une migration ou un script de backfill : au **prochain calcul complet** de chaque scénario concerné, le mécanisme `resolve_stale` déjà existant — **inchangé** par cet amendement — les résout naturellement. Sous `safety_scope=national`, `SHORTAGES_SQL`/`detect_with_params` ne réinsère (ni ne rafraîchit) plus jamais ces lignes below-safety-only ; leur `calc_run_id` ne matche donc plus le `calc_run_id` du nouveau run, et `resolve_stale`/`RESOLVE_STALE_SQL` (`UPDATE shortages SET status='resolved' WHERE status='active' AND calc_run_id != :run`) les bascule en `resolved` — par le chemin générique déjà en place pour toute ligne non rafraîchie, sans traitement spécial. Elles ne disparaissent pas (pas de suppression physique — cohérent avec la doctrine insert-only / cycle de vie soft du dépôt ; la rétention #468/ADR-039 les purgera plus tard selon sa propre fenêtre) : elles sont **requalifiées**, pas invalidées — une ligne `below_safety_stock` locale n'était jamais une donnée fausse, c'est la politique qui décide si c'est une alerte actionnable qui change. Les stockouts physiques éventuels sur ces mêmes (item, location), s'il y en a, continuent d'être détectés et rafraîchis sans interruption : le sweep ne les touche pas puisqu'ils restent produits par le run courant.

**Conséquence sur l'inclusion `items(B) ⊆ items(A)` (§5, amendée le 2026-07-17 en « sur les locations stockantes ») — la rationale se resserre encore ; l'invariant tient sur le seed CI mais n'est plus structurellement garanti en général.** Truth B (`mrp_core.first_shortage`) n'est **pas modifié** par cet amendement : il continue de comparer la demande consommée poolée au safety stock **national déjà sommé** (`loader.py:93-113`), exactement comme avant. Truth A, en scope national, ne détecte plus QUE le stockout physique par location. Un item que B juge en rupture uniquement parce que son solde poolé national passe sous le safety stock sommé — sans qu'aucune location individuelle n'ait de `closing_stock` négatif — n'a donc plus, structurellement, de ligne correspondante dans `shortages`. L'inclusion n'est plus prouvée pour ce cas ; elle reste vraie sur le seed CI (`tests/integration/test_shortage_truth_consistency_integration.py`) uniquement parce que les deux pénuries seedées (PUMP-01, VALVE-02) sont de vrais stockouts physiques au bucket 0 (solde négatif, pas seulement sous le seuil) — documenté explicitement dans le docstring de `test_seed_produces_shortages_in_both_truths` (ajouté par cette même PR), avec l'échappatoire `OOTILS_SAFETY_SCOPE=per_site` si un futur changement de seed introduit un cas below-safety-only pur. Ce n'est pas un oubli : c'est la conséquence assumée du modèle métier du pilote — un below-safety local sans stockout physique n'est, par construction, plus une alerte actionnable per-site, donc n'a plus vocation à être « inclus » dans A au même titre qu'avant.

**Réversibilité :** basculer sur l'ancien comportement est une opération sans migration — `OOTILS_SAFETY_SCOPE=per_site` (une seule variable d'environnement, lue à chaque calc_run par `policy.safety_scope()`, jamais mise en cache en base) restaure le comportement pré-DESC-1 byte-for-byte. Comme pour le flip `is_stocking` (amendement 2026-07-17), aucun re-dirty incrémental n'est câblé pour un simple changement de policy : les lignes `below_safety_stock` résolues par ce changement ne réapparaissent qu'au **prochain calcul complet** du scénario (`POST /v1/calc/run {"full_recompute": true}`) après le retour à `per_site`. Une valeur inconnue de `OOTILS_SAFETY_SCOPE` échoue bruyamment (`ValueError`), jamais un fallback silencieux.

### Références (amendement `safety_scope`)

- `docs/ADR-043-demand-descent.md` §3, §Décisions pilote — la décision pilote actée que cet amendement formalise côté ADR-021.
- `src/ootils_core/engine/kernel/shortage/policy.py` — point de résolution unique (`safety_scope()`, `is_national_scope()`, `OOTILS_SAFETY_SCOPE`, défaut `national`).
- `src/ootils_core/engine/kernel/shortage/detector.py:83-172` — `ShortageDetector.detect_with_params(safety_scope=...)`, garde Python.
- `src/ootils_core/engine/orchestration/propagator_sql.py:275-405` (CTE `pi_with_ss`), `:423-441` (`shortage_params`) — garde SQL, `%(safety_scope_national)s`.
- `src/ootils_core/engine/orchestration/propagator.py:322-341,467-491,744-752` — résolution une fois par calc_run + threading vers `_recompute_pi_node`.
- `src/ootils_core/engine/mrp/loader.py:93-113` — Truth B, le coussin national déjà sommé, inchangé par cet amendement.
- `tests/integration/test_shortage_truth_consistency_integration.py:223-291` — garde CI + docstring documentant le resserrement de couverture sous scope national.
