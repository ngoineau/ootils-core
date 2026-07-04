# ADR-020 — Consolidation des deux moteurs MRP en une source unique

**Statut :** Accepté (direction) — unité de planification tranchée le 2026-05-31 : **modèle à deux échelons DRP (per-site) → MRP (central)**, cf. §Unité de planification.
**Date :** 2026-05-31
**Contexte mesuré :** `scripts/parity_mrp_engines.py`, `scripts/bench_mrp.py` (DB pilote, 36 635 items, BOM 7 niveaux).

---

## Contexte

Ootils porte **deux implémentations MRP parallèles** qui réimplémentent la même mécanique APICS (lot sizing, consommation de prévision, cascade gross-to-net par LLC, pegging), **sans aucun test de parité entre elles** — contrairement au moteur de propagation, qui a `scripts/parity_sql_vs_python.py`.

| | A — `scripts/mrp_core.py` | B — `src/ootils_core/engine/mrp/` |
|---|---|---|
| Taille | ~520 lignes | ~4 000 lignes (7 modules) |
| Nature | read-only, calcul en mémoire | écrit nodes/edges + `mrp_bucket_records` |
| Unité de planification | **item, toutes locations poolées** | **item-à-location** |
| Consommateurs | CLIs `mrp_*.py`, watcher agents `agent_*_watcher.py` | API `/v1/mrp/run`, `/v1/mrp/apics/run` |
| Déterminisme | pur (DB-free calc) | calcul couplé à la DB |
| Perf (36K items) | **6 154 items / 2.5 s** | **~5 s / item → des heures** |

La « mrp-unification-tech-note.md » (2026-04-28) n'a unifié que les **endpoints** (`apics_mode`), pas les moteurs ; elle admet elle-même « one endpoint, **two implementations** », et n'a jamais été revue par un humain (« Reviewed: Pending »).

## Dérive mesurée

Parité croisée (horizon 360 j, échantillon 500 items à plus forte demande + explosion BOM), **avant** correction :

- Couverture : sur les 42 items que B planifie, A les planifie **tous** (0 désaccord sur « faut-il commander »).
- Quantités : **100 % des items divergent de >5 %**, médiane **96 %**, total B = 46.2 M vs A = 0.96 M → **B sur-planifie ~48×** (pire cas ×261).
- Perf : B = 43 items en **204 s** vs A = 6 154 items en 2.5 s.

## Cause racine du ×48 — un vrai bug de netting dans B

Dans `engine/mrp/`, la passe gross-to-net (`gross_to_net.py:228-271`) calcule `net_req = safety_stock − PAB` **par bucket**, sur un PAB qui ne chaîne que `scheduled_receipts − gross_requirements`. Le lot-sizing tourne **ensuite** (`mrp_apics_engine.py:_apply_lot_sizing_and_fences`) et ne mutait que le `projected_on_hand` du bucket courant — **sans jamais réinjecter les ordres planifiés dans le solde courant**. Conséquence : chaque bucket sous safety stock régénérait le **déficit complet** et le re-commandait → sur-comptage sériel. La méthode de re-chaînage `apply_planned_orders` existait mais **n'était jamais appelée**. A évite le piège par `pa += qty` immédiat (`mrp_core.py:385`).

Le mode `location=None` de la mesure (pas de pooling, on-hand lu en baseline) amplifie marginalement, mais le bug de netting tient indépendamment.

## Décision

1. **Le moteur canonique est A (`scripts/mrp_core.py`).** Critères : netting correct, déterminisme (calcul pur), perf (2 400×), et surtout **la flotte d'agents — la North Star — l'utilise déjà**.
2. **Cible :** promouvoir la maths de A dans un module packagé unique `src/ootils_core/engine/mrp/core.py` (DB-free : `PlanningData`, lot sizing, `consume_demand`, `run_timephased`, `peg_origins`, `first_shortage`, `excess_obsolete`). Deux surcouches au-dessus :
   - **read-only analytique** (CLIs + watchers) ;
   - **write / matérialisation scenario-first** (refonte de B : charge via le cœur, persiste sous `scenario_id`, émet `StreamChanges`). `GrossToNetCalculator` / `forecast_consumer` redondants seront supprimés.
3. **Garde-fou permanent :** un test de parité croisée A-vs-B en CI, qui aurait empêché la dérive de grandir en silence. C'est le **premier** livrable, avant toute fusion.

## Unité de planification — TRANCHÉE (2026-05-31) : deux échelons DRP → MRP

Décision du pilote, cohérente avec la topologie LOCKED du module demande
(Pyramide, 2026-05-30) :

- **Échelon DRP (distribution) = per-site.** La demande granulaire par site
  (le *shipping plan* produit par Pyramide — booking → plan d'expédition netté,
  cf. règle demande booking/shipping) arrive **en tête de DRP**. Le DRP nette
  par site, déploie/pousse le stock vers les DCs (chaque DC sert des états et
  familles produits définis), et fait remonter un besoin de réappro.
- **Échelon MRP (make/buy) = central**, **alimenté par le DRP**, avec **safety
  stock poolé central** (risk pooling = le bouclier inventaire ; ce n'est PAS du
  safety par-DC).

**Le per-location vit dans l'échelon DRP, pas en éclatant le MRP.** Et — point
structurant — **DRP et MRP sont la même maths de netting** (gross-to-net, lot
sizing, lead-time offset) appliquée à des arcs de graphe différents :
distribution (DC→site) pour le DRP, nomenclature (parent→composant) pour le MRP.
Donc **un seul cœur de calcul** traverse le graphe unifié et produit les deux.
Le cœur reste celui de A ; le DRP n'est pas un 3ᵉ moteur.

**Dépendance amont :** la fusion utile (PAS 3-4) suppose que Pyramide livre la
demande **per-site**. Tant que la demande est mono-location (état actuel des
données pilote), le DRP tourne à vide. Le chemin critique est donc côté module
demande (split per-site), pas côté MRP.

**Le cœur partagé doit paramétrer la clé de planification** (`item` au central /
`(item, location)` au DRP) — non plus comme une option indécise, mais comme les
deux échelons d'une seule cascade.

## Séquence de migration

| Pas | Contenu | Statut |
|---|---|---|
| **1** | Garde-fou : durcir `parity_mrp_engines.py` en test de non-régression CI ; oracle = `test_mrp_core_golden.py` (A) ; parité B-vs-A en `xfail` documentant la dérive jusqu'au fix | à faire |
| **2** | **Fix du bug de netting de B** (chaînage des ordres planifiés dans le PAB) | ✅ **fait + validé** (`mrp_apics_engine.py:_apply_lot_sizing_and_fences`) — voir ci-dessous |
| 3 | Déplacer la maths de A dans `engine/mrp/core.py` (cœur DB-free) + `loader.py` (DB) ; `scripts/mrp_core.py` = shim de ré-export (21 consommateurs inchangés) ; golden-master re-ciblé sur le package + garde-fou shim≡package (`test_mrp_shim_compat.py`) | ✅ **fait** (byte-identique : 99 765 ordres avant/après, 31 tests verts) |
| 4 | Refondre B en surcouche write au-dessus du cœur ; ajouter l'échelon DRP (cascade sur arcs de distribution) ; corriger la lecture on-hand en baseline dur (`gross_to_net.py:352` — fuite cross-scénario) | gated : demande **per-site** livrée par Pyramide |
| 5 | Déprécier `/v1/mrp/apics/run` : `deprecated=True` (OpenAPI) + headers RFC 8594 (`Deprecation: true`, `Link: successor-version` → `/v1/mrp/run`), sans date de sunset (TBD) ; `apics_mode` conservé sur `/v1/mrp/run` | ✅ **fait** |

## Validation du fix de netting (PAS 2)

Re-mesure parité croisée, mêmes paramètres (500 items, horizon 360 j) :

| Mesure | Avant | Après |
|---|---|---|
| Total quantité B (items communs) | 46.2 M | **0.94 M** |
| Total quantité A | 0.96 M | 0.93 M |
| Ratio B/A | ~48× | **~1.02×** |
| Dérive médiane par item | 96 % | **4.1 %** |

Le sur-comptage sériel est éliminé ; le résidu (~4 % médian) relève de l'unité
de planification (pooled vs per-location) et des nuances de règles de lot, à
fermer aux PAS 3-4. `test_gross_to_net` + golden-master A : 139/139 verts.

### Décomposition du résidu (mesurée au premier run CI du garde-fou, 2026-07-02)

Le garde-fou de parité en CI (#332) sur le seed démo (2 items, médiane 21-24 %
selon le jour de semaine — amplification petits-volumes du même résidu) a
permis de **nommer** les écarts qui composent la classe résiduelle. Tous
préexistants au fix de netting, tous absorbés par le PAS 4 (B devient une
surcouche du cœur A) — aucun fix côté B :

1. **L4L sans `order_multiple`** — le lot-for-lot de B applique le plancher
   `min_order_qty` mais jamais le multiple (`lot_sizing.py`), là où le cœur A
   applique MOQ + multiple à toutes les règles en garde finale (`core.py`).
   Aggravant dormant : le batch loader de B lit `order_multiple_qty` sans
   COALESCE sur la colonne legacy `order_multiple`.
2. **Demande indépendante des composants jetée par B** — pour un composant
   LLC>0 avec demande dépendante, `gross_to_net.py` retourne la seule demande
   dépendante et ignore forecast/commandes propres du composant (violation
   APICS : pièces de rechange). Matériel : −11 % sur VALVE-02 au run CI.
3. **Sémantique de frozen fence opposée** — A traite `frozen_time_fence_days`
   comme *demand* time fence (orders-only dans la zone), B comme *order
   placement* fence (demande conservée, ordre différé au premier bucket
   libre). S'y ajoutent le Monday-snap des buckets de B (sensibilité au jour
   de run) vs les buckets ancrés sur CURRENT_DATE de A, et le traitement des
   réceptions en retard (A les clampe au bucket 0, B les ignore).

## Conséquences

- **Positif :** une seule source de vérité MRP, déterministe, testée en parité ; B cesse de sur-planifier ; les agents et la matérialisation raisonnent sur le même monde.
- **Risque résiduel à traiter au PAS 4 :** B lit l'on-hand en `BASELINE` en dur → les scénarios mentiraient ; à corriger avant de câbler le MRP dans les forks.
- **Hors scope (ADR à part) :** signal de confiance / fraîcheur sur les recommandations MRP.

## Références

- `scripts/parity_mrp_engines.py` — harness de dérive croisée (à promouvoir en test).
- `scripts/bench_mrp.py`, `docs/PERF-BASELINE.md` — perf MRP.
- `docs/mrp-unification-tech-note.md` — l'« unification » d'endpoints (≠ moteurs).
- `scripts/mrp_core.py:344-403` (cascade correcte), `engine/mrp/gross_to_net.py:228-271` (siège du bug), `engine/mrp/mrp_apics_engine.py:_apply_lot_sizing_and_fences` (fix appliqué).
