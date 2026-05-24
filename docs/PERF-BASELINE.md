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

### Run du 2026-05-24

| Profile | Items | Locations | PI nodes | Edges | Engine | Elapsed | Throughput | Speedup |
|---|---|---|---|---|---|---|---|---|
| S | 1 900 | 6 | 47 520 | 100 817 | python | 47.2 s | 1 008 PI/s | — |
| S | 1 900 | 6 | 47 520 | 100 817 | sql | **5.4 s** | 8 873 PI/s | **8.8×** |
| M | 5 000 | 14 | 116 910 | 239 805 | python | 73.2 s | 1 597 PI/s | — |
| M | 5 000 | 14 | 116 910 | 239 805 | sql | **14.5 s** | 8 055 PI/s | **5.0×** |

### Lecture

- **Le moteur SQL window-function est largement sous le seuil de 30s** sur les deux profiles, y compris à 5 K SKUs.
- Le SQL engine maintient un **throughput stable autour de 8 000 PI/s** quelle que soit l'échelle.
- Le Python engine montre environ **1 000-1 600 PI/s**, à comparer aux 3-6× speedup annoncés historiquement — la mesure réelle est plutôt **5-9× selon la taille**.

### Extrapolation V2

À throughput SQL ~8 000 PI/s constant et ~25 buckets PI par couple (item × location) :

| SKU cibles | PI nodes estimés | Temps SQL estimé |
|---|---|---|
| 1 K | ~25 K | ~3 s |
| 5 K | ~120 K | ~15 s ✅ (mesuré) |
| 10 K | ~250 K | ~30 s ⚠️ (à la frontière) |
| 50 K | ~1.2 M | ~150 s 🔴 (Rust justifié) |

**Le seuil "Rust justifié par la perf" se situe autour de 10-50 K SKU avec `OOTILS_ENGINE=sql` activé.** Pour des cibles client < 10 K, le moteur SQL Python suffit.

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
| 2026-05-24 | post-#270 | S | 47.2s | 5.4s | 8.8× | Baseline initial |
| 2026-05-24 | post-#270 | M | 73.2s | 14.5s | 5.0× | Baseline initial |

## Hardware / contexte

- Postgres 16 sur VM Debian (192.168.1.176)
- Postgres tuning : `shared_buffers=1GB`, `work_mem=32MB`, `jit=off`, parallel workers capped à 2 cores (cf `docker-compose.yml`)
- Python 3.13 client (machine dev locale)
- Réseau LAN gigabit
- 2026-05-24 — pas de charge concurrente sur la VM

## Comment refaire le bench

```bash
# 1. Seeder la DB cible
DATABASE_URL="postgresql://ootils:ootils@<host>:5432/postgres" \
    python scripts/seed_realistic_dataset.py --profile S --dbname ootils_bench_s

# 2. Lancer le bench (compare automatiquement python et sql)
DATABASE_URL="postgresql://ootils:ootils@<host>:5432/ootils_bench_s" \
    python scripts/bench_engine_comparison.py
```

À mettre à jour à chaque modification du propagator ou de la couche SQL.
