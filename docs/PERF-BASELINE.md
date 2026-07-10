# Performance baseline — propagation engine

Mesures de référence pour le moteur de propagation, comparaison
`OOTILS_ENGINE=python` vs `OOTILS_ENGINE=sql`.

Référence pour [ADR-015 trigger D5-A](ADR-015-rust-readiness.md) :
*"p95 propagation > 30s sur 500-1 K SKU malgré OOTILS_ENGINE=sql"*.

## Méthodologie

Script : `scripts/bench_engine_comparison.py`

- DB seedée via `scripts/seed_realistic_dataset.py --profile {S|M}`
- Tous les PI nodes du scenario baseline marqués dirty
- Full propagation via `engine._propagate(calc_run, pi_ids, conn)`
- Wall-clock chronométré avec `time.perf_counter()`
- Mêmes hardware/réseau pour les deux engines (Postgres distant 192.168.1.176)
- 2 runs consécutifs pour vérifier la stabilité (résultats stables ±5%)

### Métrique throughput

**`throughput = dirty_node_count / elapsed`** — c'est le nombre de PI nodes
*processed* (marqués dirty et traités par le kernel), pas le nombre de PI
dont la valeur a changé. C'est l'indicateur non-ambigu, comparable entre
engines.

`CalcRun.nodes_recalculated` est intentionnellement **biaisé** entre engines
(cf docstring `models/__init__.py`) — Python compte les nodes *changed*,
SQL compte les *rows updated*. À ne pas utiliser pour des comparaisons
cross-engine. Le bench script reporte les deux mais le throughput
officiel est calculé sur `dirty_node_count`.

## Résultats

### Run du 2026-05-24 — direct LAN (Postgres infra refondue)

| Profile | Items | Locations | PI nodes | Edges | Engine | Elapsed | Throughput | Speedup |
|---|---|---|---|---|---|---|---|---|
| S | 1 900 | 6 | 47 520 | 100 817 | python | 56.5 s | 841 PI/s | — |
| S | 1 900 | 6 | 47 520 | 100 817 | sql | **5.6 s** | 8 419 PI/s | **10.0×** |
| M | 5 000 | 14 | 111 240 | 228 097 | python | 74.7 s | 1 489 PI/s | — |
| M | 5 000 | 14 | 111 240 | 228 097 | sql | **12.4 s** | 8 940 PI/s | **6.0×** |
| L | 10 000 | 14 | 226 800 | 459 614 | python | 165.7 s | 1 369 PI/s | — |
| L | 10 000 | 14 | 226 800 | 459 614 | sql | **34.5 s** | 6 579 PI/s | **4.81×** |

### Lecture

- Le moteur SQL window-function **passe le seuil 30s à profile L** (34.5s).
  L'extrapolation linéaire optimiste (~28s) ne tient pas — voir
  non-linéarité ci-dessous.
- **Non-linéarité confirmée S→M→L** : le throughput SQL chute de
  8419 → 8940 → 6579 PI/s. Une accélération entre S et M (cache /
  parallèle workers efficaces sur volume moyen) puis une **chute de
  26% entre M et L**. Probables causes :
  - `work_mem=32MB` saturé sur les CTE intermédiaires de PROPAGATE_SQL,
    forçant des spill disk.
  - Postgres planner switch (nested loop → hash join), induit par
    statistiques après l'ANALYZE.
  - Cache eviction sur le buffer (1GB `shared_buffers` ne tient pas
    227K PI × 460K edges en mémoire).
- Le Python engine reste linéaire mais **15-25× plus lent** au-delà de
  profile S.
- Speedup mesuré : **5-10× selon la taille**, en faveur du SQL —
  **diminue avec l'échelle**.

### Bench incremental (mode UX réel)

Mesuré via `scripts/bench_incremental.py` sur profile M, 10 events
`demand_qty_changed` sur ForecastDemand triggers (avec PI couplage garanti) :

| Métrique | SQL direct LAN | Note |
|---|---|---|
| p50 latence | **163 ms** | — |
| mean | 214 ms | — |
| p95 | 545 ms | — |
| max | 545 ms | — |
| Dirty subgraph (constant) | 91 PI | item × loc series sur 90 buckets |
| Throughput | 425 PI/s | par event, overhead orchestration inclus |

**Cible UX p95 < 500ms : atteinte en direct LAN.** Les mesures précédentes
à p95 7 s venaient d'un tunnel SSH qui ajoutait ~500 ms/event d'overhead
network.

### Bench burst (session agent simulée — 100 events séquentiels)

Mesuré sur profile M, 100 `demand_qty_changed` enchaînés sans pause, SQL
engine direct LAN. Bucketing par position pour détecter une éventuelle
dégradation cumulative (lock contention, autovacuum, WAL checkpoint).

| Position | count | p50 (ms) | p95 (ms) | max (ms) |
|---|---|---|---|---|
| 1-5 (cold start) | 5 | 167 | 649 | 649 |
| 6-20 | 15 | **93** | 646 | 646 |
| 21-50 | 30 | **98** | 677 | 764 |
| 51-100 | 50 | **91** | 664 | 700 |

**Stats globales sur 100 events :** min=81, p50=**93**, mean=218,
p95=**664**, p99=764, max=764. 0 failures.

**Lecture :**
- **Pas de dégradation cumulative.** Le p50 descend de 167ms (cold,
  buffer cache vide) à ~93ms après 5 events et reste plat jusqu'au 100e.
- Les outliers ~5% (latences à ~650-760ms) sont **uniformément
  distribués** dans le temps, pas concentrés à la fin → bruit Postgres
  (autovacuum/checkpoint statistiques), pas un comportement systémique.
- Throughput burst = 417 PI/s ≈ throughput single-event (425 PI/s) →
  aucun overhead supplémentaire à enchaîner les events.

**Conséquence pour les agents** : une session de 50-100 modifs en
quelques dizaines de secondes reste fluide (p50 ~100ms par event,
quelques pics ponctuels à ~700ms acceptables).

### Extrapolation V3 — révisée post-profile L

Le throughput SQL **n'est pas constant**. Le run L a invalidé
l'hypothèse linéaire. Hypothèse révisée : dégradation au-delà de
200K PI proportionnelle à `work_mem` / volume CTE.

| SKU cibles | PI nodes estimés | Temps SQL extrapolé (révisé) | Confiance |
|---|---|---|---|
| 1 K | ~25 K | ~3 s | ✅ extrapolé S |
| 5 K | ~120 K | ~13 s | ✅ mesuré (M) |
| 10 K | ~230 K | **34 s** | ✅ mesuré (L) |
| 25 K | ~570 K | **~120 s** | ⚠️ extrapolé avec ralentissement |
| 50 K | ~1.1 M | **~280 s** | 🔴 extrapolé conservateur |

**⚠️ Important** : la mesure full propagation est un cas batch
(tous les PI dirty). En usage opératoire normal, on n'a *jamais* tous
les PIs dirty d'un coup — un event utilisateur touche ~91 PIs (une
série × 90 buckets) et **l'incremental est insensible au volume total**
(p50 = 95ms constant sur M et L, cf section suivante).

→ Le seuil pertinent "Rust justifié" est :
- ❌ **Full prop > 30s** : déjà atteint sur L (34s), mais c'est du batch
  acceptable (en réalité ce mode tourne dans la nuit ou en async).
- ✅ **Incremental p95 > 500ms en charge réelle** : non atteint à L.
  L'engine SQL Python reste viable pour les agents jusqu'à 10K SKU
  *au moins*.

### Burst incremental sur profile L

Re-mesuré séquentiellement (le 1er run était en parallèle du full bench
et hit le scenario lock → invalide).

| Position | count | p50 (ms) | p95 (ms) | max (ms) |
|---|---|---|---|---|
| 1-5 | 5 | 99 | 715 | 715 |
| 6-20 | 15 | 95 | 447 | 447 |
| 21-50 | 30 | 94 | 439 | 626 |
| 51-100 | 50 | 94 | 680 | 683 |

Global L : p50=**95ms**, p95=**675ms**, mean=185, 0 failures.

**Comparaison M vs L : strictement identique.** L'incremental ne
dépend pas du volume total — le dirty subgraph reste constant (~91 PI
par event). Confirme que **l'engine reste utilisable pour les agents
jusqu'à 10K SKU**.

## Stratégie d'explainability (M3) — lazy regen

Depuis 2026-05-24, **`OOTILS_ENGINE=sql` est le défaut**, et les causal chains
sont régénérées **à la lecture** via `GET /v1/explain/{node_id}` :

- Pas de génération eager pendant `propagate()` — coûterait ~5 s / 1 K
  shortages avec le builder Python et ferait perdre tout le speedup SQL
  (mesuré : 306s eager-SQL vs 250s eager-Python — SQL devenait plus lent).
- Quand `GET /v1/explain` ne trouve pas d'explication mais le PI a un shortage
  actif → construction de la chaîne causale à la demande, persistée, liée au
  shortage. Coût mesuré : **~50 ms par chaîne** sur profile S.
- Aucun compromis sur le contrat M3 — la chaîne est toujours fraîche au moment
  où elle est consultée (amortie sur les seuls nœuds réellement explorés).

Configuration :
- Défaut : `OOTILS_ENGINE=sql` (8-9× plus rapide, explainability lazy).
- Fallback : `OOTILS_ENGINE=python` reste disponible pour la parité
  (`scripts/parity_sql_vs_python.py`) ou en régression de last resort.

## Historique des runs

| Date | Commit | Profile | Python | SQL | Speedup | Notes |
|---|---|---|---|---|---|---|
| 2026-05-24 | post-#270 | S | 47.2s | 5.4s | 8.8× | Baseline initial via tunnel SSH (15432) |
| 2026-05-24 | post-#270 | M | 73.2s | 14.5s | 5.0× | Baseline initial via tunnel SSH |
| 2026-05-24 | post-#273 | S | 56.5s | **5.6s** | **10.0×** | Direct LAN, Postgres infra refondue |
| 2026-05-24 | post-#273 | M | 74.7s | **12.4s** | **6.0×** | Direct LAN, Postgres infra refondue |
| 2026-05-24 | post-#275 | L | 165.7s | **34.5s** | **4.81×** | Profile L (10K SKU, 227K PI), seuil 30s franchi |

## Addendum 2026-05-31 — dé-corrélation `PROPAGATE_SQL` + perf MRP

### Dé-corrélation des sous-requêtes `inflows`/`outflows` (propagator_sql.py)

Le CTE `per_bucket` calculait `inflows` et `outflows` via deux **sous-requêtes
scalaires corrélées**, évaluées une paire par PI dirty (~450K exécutions à 227K
PI) — cause mécanique de la chute de throughput non-linéaire S→M→L. Réécrites en
**deux CTE agrégées séparées** (`inflows_agg`, `outflows_agg`, LEFT JOIN +
COALESCE — CTE distinctes pour éviter le produit cartésien inflow×outflow).

Mesure EXPLAIN ANALYZE de la requête de projection (read-only, bench isolée
`ootils_test_bench`, 18 000 PI dirty, 3 runs) :

| Variante | Médiane | SubPlans corrélés | Spill disque |
|---|---|---|---|
| Corrélée (avant) | 279 ms | 6 | non |
| Dé-corrélée (après) | **71 ms** | 2 | non |

→ **3.9× sur la requête chaude**, sans spill. Parité validée **bit-exacte vs
l'original** (`scripts/parity_sql_vs_python.py`, 200×90, full+incremental) : la
réécriture ne change **aucune** valeur — mêmes résultats que la version corrélée.

### ⚠️ Bug de parité pré-existant découvert — `has_shortage` au bord de zéro

La campagne de parité (200 items × 90 buckets, supplies/demands mixtes) révèle
une divergence **pré-existante** (présente AVANT la dé-corrélation, donc non
introduite par elle) : **10 nœuds sur 18 000** ont `has_shortage` Python=`False`
mais SQL=`True`, et 10 shortages SQL "en trop". Tous les champs **numériques**
(opening/inflows/outflows/closing/shortage_qty) concordent à 1e-12.

Cause racine : `has_shortage` est un **test de signe strict à 0** (`closing < 0`,
identique dans les deux moteurs). Sur les nœuds où le prorating multi-jours
amène `closing` à ~0, l'arithmétique exacte mais différemment arrondie
(`Decimal` 28 digits vs `numeric(50,28)` Postgres) place la valeur de part et
d'autre de zéro (écart < 1e-12). Un `closing` de −1e-13 n'est pas une vraie
rupture mais le test strict les départage. **Fix recommandé** (non appliqué —
décision sémantique) : seuil epsilon partagé (`closing < -1e-9`) dans les deux
moteurs. Le harness de parité full **échoue** sur ce point ; l'incrémental passe.

### Perf MRP (#301) — `scripts/bench_mrp.py`, lecture seule sur la pilote

Première mesure du MRP (jamais benché). DB pilote : 36 635 items, BOM 7 niveaux,
17 181 items impliqués, 220K ordres planifiés (horizon 540 j).

| Phase | Avant consolidation | Après consolidation |
|---|---|---|
| load (DB) | 2.58 s | **1.87 s** |
| cascade (compute) | 1.46 s | 1.46 s |
| **total** | 4.05 s | **2.88 s** (−29%) |

Constat : le MRP est **DB-bound** (load = 60-74% du temps), pas compute-bound
(la cascade 7-niveaux tourne à 120-150K ordres/s). Le levier appliqué :
consolidation des scans répétés dans `load_planning_data` (12 scans de
`item_planning_params` → 1, idem nodes/supplier_items/items), parité dict-exacte
vérifiée sur les 19 champs. Mesure isolée : 12 scans 473 ms → 1 scan 87 ms (5.5×).

### Re-mesure 2026-07-06 — post-#349 (fenêtre de consommation), via le runbook démo #408

Même base pilote (36 635 items, 3,76 M `demand_history`), `demo_e2e.py --bench`
(1 répétition, lecture seule) :

| Phase | 2026-05 (post-consolidation) | 2026-07-06 (post-#349) |
|---|---|---|
| consume | — (inclus cascade) | 0.110 s |
| timephased | — | 0.603 s |
| peg | — | 0.055 s |
| compute total | 1.46 s | **0.77 s** (258 957 ordres/s) |
| **total wall** | 2.88 s | **4.80 s** (load DB = 84 %) |

Le calcul pur a **doublé de vitesse** (150K → 259K ordres/s — la fenêtre de
consommation #349 réduit aussi les besoins nets à planifier), mais le wall est
dominé à 84 % par le chargement DB, qui a grossi depuis mai (loader #349 +
conditions réseau non contrôlées entre les deux mesures — pas un A/B strict).
Le levier suivant reste côté load, pas côté cascade. Mesure prise pendant
l'exécution pilote du runbook (`docs/DEMO-RUNBOOK.md`, step 10).

### Première propagation pilote — chemin API (2026-07-07, #414 / RUNBOOK-pilot-propagation)

Détail dans `docs/SCALABILITY.md` §« Measured — FIRST pilot-scale run over the
API path ». Chiffres bruts : fork baseline 211 K nodes **23,8 s** ; bootstrap
856 items × 120 j → **220 440 PI en 66,4 s** ; **full recompute API
464,4 s → 475 nps** (~8× sous le bench engine-direct synthétique — la
différence EST le coût du chemin HTTP + DB LAN, cf. #193 workers async) ;
174 769 pénuries item-jour sur le fork ; baseline intacte (0 PI). Exécuté
fork-first, fork archivé — reproductible via le runbook.

## Hardware / contexte

- Postgres 16.13 sur VM Debian (192.168.1.176:5432) — infra refondue 2026-05-24
- Python 3.13 client (machine dev locale)
- Réseau LAN gigabit, **accès direct** (pas de tunnel SSH)
- 2026-05-24 — pas de charge concurrente sur la VM

**À noter — biais tunnel SSH** : les mesures initiales (post-#270) passaient
par un tunnel SSH local (`127.0.0.1:15432`) qui ajoutait ~500 ms par event
en mode incremental (négligeable en bulk mais catastrophique pour l'UX). Le
direct LAN supprime cet overhead. Toujours bencher direct quand possible.

## Comment refaire le bench

```bash
# 1. Seeder la DB cible
DATABASE_URL="postgresql://ootils:ootils@<host>:5432/postgres" \
    python scripts/seed_realistic_dataset.py --profile S --dbname ootils_bench_s

# 2a. Full propagation bench (compare python vs sql)
DATABASE_URL="postgresql://ootils:ootils@<host>:5432/ootils_bench_s" \
    python scripts/bench_engine_comparison.py

# 2b. Incremental UX bench (p50/p95 latency per event)
DATABASE_URL="postgresql://ootils:ootils@<host>:5432/ootils_bench_m" \
    python scripts/bench_incremental.py --n 20 --warmup 5 --engine sql
```

À mettre à jour à chaque modification du propagator ou de la couche SQL.
