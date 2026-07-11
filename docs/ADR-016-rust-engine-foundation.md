# ADR-016 — Rust engine foundation (Architecture A, scope tight)

**Status** : Accepted — 2026-05-24
**Supersedes / extends** : [ADR-015 Rust readiness](ADR-015-rust-readiness.md)
**Related** : [PERF-BASELINE.md](PERF-BASELINE.md), [POC rust_kernel](../poc/rust_kernel/)

## Context

Le moteur SQL de propagation atteint **34.5s sur profile L (227K PI)**, ce qui
franchit le seuil 30s de l'ADR-015 trigger D5-A. Trois investigations menées
le 2026-05-24 ont éclairé la décision :

1. **Postgres tuning** (work_mem 32M→256M, shared_buffers 1G→10G,
   effective_cache_size 4G→32G) — **0% de gain** sur la full propagation.
   Le bottleneck n'est pas la mémoire ni le cache.

2. **SQL rewrite** (correlated subqueries → pre-aggregated LEFT JOIN +
   `MATERIALIZED`) — **catastrophique**, ×20-50 plus lent. Le plan
   original avec sous-requêtes corrélées exploite efficacement
   `idx_edges_to`. Mon "optimisation" force une matérialisation
   intermédiaire qui sature la mémoire.

3. **POC Rust kernel** (port de `compute_pi_node` en Rust avec
   `rust_decimal`) — **32× plus rapide que Python** sur l'arithmétique
   pure. Mais le kernel n'est que ~0.3% du temps total Python (le reste
   est DB roundtrip + sérialisation + dispatch).

Constat : **le compute lui-même est ultra-rapide en Rust, mais
l'orchestration Python est ce qui domine.** Pour exploiter Rust,
il faut porter l'orchestration de la propagation aussi — pas juste
le kernel.

## Decision

Implémenter un **3e moteur de propagation Rust** (en plus de Python
et SQL), avec un **scope serré** :

### Ce qui devient Rust

- Le port complet de `_propagate()` : lecture dirty subgraph, compute
  per-bucket, window function (cumulative opening_stock), détection
  shortage, **bulk writeback via Postgres COPY**.
- Le kernel arithmétique (`compute_pi_node` équivalent).
- La traversée du graphe pour la cascade dirty (équivalent
  `expand_dirty_subgraph`).

### Ce qui reste Python (impérativement)

- FastAPI + routes + validation Pydantic
- Ingestion / staging (CSV, XLSX, mapping)
- Agent tools + intégration Anthropic SDK
- Scenario management, calc_run lifecycle, event log
- Tout l'orchestrator de haut niveau (`process_event`, locks advisory,
  state machine)

### Interface entre les deux

Le module Rust expose une **API étroite** via PyO3 :

```python
from ootils_kernel import propagate

result = propagate(
    db_dsn="postgresql://...",
    calc_run_id=UUID,
    scenario_id=UUID,
)
# result.nodes_processed: int
# result.nodes_changed: int
# result.elapsed_ms: int
# result.shortages_detected: int
```

Le wrapper Python `RustPropagationEngine(PropagationEngine)` hérite du
moteur Python, override uniquement `_propagate()`, et délègue à
`ootils_kernel.propagate(...)`. Le reste (lock, calc_run init,
shortage_detector resolve_stale) reste Python.

## Rationale

### Pourquoi un 3e moteur et pas un remplacement

- L'engine SQL marche déjà et fonctionne sur 100% des cas. On le garde
  en fallback explicite (`OOTILS_ENGINE=sql`).
- L'engine Python reste pour parity testing
  (`scripts/parity_sql_vs_python.py` étendu).
- Le moteur Rust devient `OOTILS_ENGINE=rust`, **opt-in pendant 4 semaines
  après livraison**, puis bascule défaut si stable.

### Pourquoi cette frontière précise Python/Rust

Le kernel arithmétique et l'orchestration de propagation **ne changent
quasi jamais** depuis 6 mois. Les porter en Rust ne coûte rien en
flexibilité de dev.

À l'inverse, l'API, l'ingestion, les agents et le state management
changent **plusieurs fois par semaine**. Les laisser en Python préserve
95% de la vitesse d'itération du projet.

### Pourquoi pas Architecture B (service Rust complet en RAM)

Architecture B (service Rust dédié, graph en mémoire, async writeback)
donnerait 10-20× au lieu de 3-5×. Mais :

- **3 mois de freeze** sur les features.
- Refacto fondamental du model scenario (snapshot COW, réconciliation
  RAM↔Postgres en cas de crash).
- Aucun client signé à >25K SKU ne le justifie aujourd'hui.

On garde Architecture B comme **option future** déclenchée par un
trigger explicite (cf §"Conditions d'escalade").

## Consequences

### Positives

- **Perf full propagation** : profile L 34.5s → cible 6-10s (3-5×).
- **Perf incremental** : profile L p50 95ms → cible 30-50ms (2-3×).
- **Petits subgraphs** (10-20 PI dirty) : ~5-10ms — UX agent vraiment
  instantanée.
- **Foundation** : si jamais on bascule Architecture B, on a déjà le
  kernel Rust + l'IO Postgres en Rust. Transition incrémentale.
- **Cleaner separation** : la frontière compute/orchestration devient
  explicite. Bénéfice architectural au-delà de la perf.

### Negatives

- **6 semaines de focus partiel** (50-70% du temps). Features ralenties
  sur cette période.
- **Build chain** : maturin + Rust toolchain devient dépendance pour
  faire tourner le moteur défaut (mais l'engine SQL reste viable
  sans Rust).
- **Build time CI** : +30-60s sur les PRs qui touchent le crate Rust.
- **Hiring** : on commence à dépendre marginalement de Rust skill
  (pour maintenir le crate). Pas critique tant que le crate reste
  petit et stable.

### Neutral

- **Test surface** : on ajoute un 3e chemin à `parity_sql_vs_python.py`
  (devient `parity_engines.py` triple-way).
- **Docker image** : doit inclure le wheel Rust compilé. Multi-stage
  build avec Rust toolchain dans le stage builder.

## Scope serré — ce qu'on NE FAIT PAS

Explicitement **hors scope** de cet ADR :

- ❌ Port de l'API FastAPI en axum/actix
- ❌ Port de l'ingestion en Rust (CSV, XLSX parsing)
- ❌ Port du shortage_detector ou de l'explanation_builder
- ❌ Async runtime Rust (tokio) — on reste en blocking simple
- ❌ Cache en mémoire entre calc_runs (chaque appel relit depuis Postgres)
- ❌ Scenario snapshots COW
- ❌ Service Rust autonome (le crate est appelé via PyO3, pas via
  HTTP/gRPC)

## Plan d'exécution — 6 semaines

| Semaine | Livrable | Go/No-Go gate |
|---|---|---|
| 1 | Foundation : crate Rust + PyO3 + maturin + CI Linux/Windows + ADR-016 | `pip install` + `import ootils_kernel` + tests Decimal roundtrip OK |
| 2 | Read path : Rust lit dirty subgraph depuis Postgres | Read 227K PI + 230K demands en **2.2s (vs 4.6s psycopg, 2× gain)** ✅ |
| 3 | Compute + parity | 0 mismatch sur 100% des PIs profile L vs Python et SQL |
| 4 | Writeback via COPY + intégration `OOTILS_ENGINE=rust` | Full prop L < 12s, incremental L < 50ms p50 |
| 5 | Hardening : error handling, concurrence, mémoire, Docker | Tous tests intégration verts, mémoire < 500MB sur L |
| 6 | Production-readiness : bench complet, doc, rollout opt-in | `OOTILS_ENGINE=rust` opt-in en prod |

## Conditions d'escalade vers Architecture B

On déclenche Architecture B (service Rust autonome, graph in-memory,
async writeback) si **un** des critères suivants est rencontré :

- D2-A : Premier client signé à >25K SKU avec exigence full propagation
  temps réel (< 5s interactif).
- D2-B : 5+ utilisateurs concurrents sur un même tenant — lock contention
  mesurable sur scenario advisory lock.
- D2-C : Pivot business — "speed at any scale" devient le pitch
  principal et justifie une levée pour embaucher un dev Rust senior
  dédié.

Sinon, on **reste** sur Architecture A pour 18-24 mois minimum.

## Conditions d'abort de l'Architecture A en cours

Pendant les 6 semaines, on **abort** et on revient au SQL engine par
défaut si :

- A1 : Semaine 1 — PyO3 + Windows ne builde pas après 2 semaines de
  tentatives.
- A2 : Semaine 3 — Parity 3-way ne tient pas après debug (Rust ne peut
  pas reproduire byte-identique les résultats Python/SQL).
- A3 : Semaine 4 — Gain mesuré < 2× sur full prop L. Le port n'a pas
  débloqué ce qu'on visait.

En cas d'abort, le crate Rust reste dans le repo (foundation pour une
future tentative ou pour Architecture B), mais `OOTILS_ENGINE` reste
sur `sql` par défaut sans exposer `rust` comme option.

## Addendum 2026-07-11 — Industrialisation (PR-C, worktree `feat/rust-pyo3-industrialization`)

Ne change ni le statut `Accepted` ni les décisions de scope ci-dessus —
documente quatre décisions d'industrialisation prises sur cette fondation.
Défaut `OOTILS_ENGINE=sql` **inchangé** de bout en bout.

### 1. Build Docker gaté par `WITH_RUST`

`Dockerfile` gagne `ARG WITH_RUST=0`. Build par défaut (`WITH_RUST=0`) :
byte-for-byte identique au build pré-PR-C — aucune image `rust:1.82-slim`
tirée, aucun toolchain cargo/maturin téléchargé. `WITH_RUST=1` ajoute un
stage `rust-builder` qui compile le wheel `ootils_kernel` via
`maturin build --release` et l'installe dans l'image finale. Technique de
gating : **sélection de stage par ARG** (`FROM selected-${WITH_RUST} AS
selected`), pas un `RUN` conditionnel — un `RUN if` ne peut pas empêcher un
`FROM` inconditionnel d'être tiré ; BuildKit ne construit que les ancêtres du
stage réellement sélectionné. `docker-compose.yml`'s `api.build.args.WITH_RUST`
vaut `0` par défaut, surchargeable via `.env` ou `WITH_RUST=1 docker compose
build`. `.dockerignore` exclut `rust/target/` (plusieurs centaines de Mo —
sinon envoyé au daemon même à `WITH_RUST=0`). Le moteur par défaut au
runtime n'est pas touché : `OOTILS_ENGINE` reste non défini dans
`Dockerfile`/`docker-compose.yml`, donc résout à `sql` quel que soit
`WITH_RUST`. **Non validé par un `docker build` réel** (pas de daemon Docker
dans le sandbox de build) — validation statique seulement (`docker compose
config` avec/sans override, relecture manuelle) ; un `docker build .` et un
`docker build --build-arg WITH_RUST=1 .` réels restent un point de
validation ouvert (CI ou environnement avec daemon).

### 2. Fix PGPASSWORD racy

La signature PyO3 `propagate_and_write(dsn, password, calc_run_id_str,
scenario_id_str)` (`rust/ootils_kernel/src/lib.rs:255-262`) prend désormais
le mot de passe Postgres en argument explicite REQUIS au lieu d'une lecture
de `PGPASSWORD` — l'ancienne approche mutait `os.environ["PGPASSWORD"]`
autour de l'appel, racy dès que deux propagations tournent en concurrence
dans un même process (état mutable partagé). Threadé de bout en bout :
`lib.rs` → `writeback::propagate_and_write` → `pool::with_client` (un nouveau
champ `password` sur `PoolEntry` force une reconnexion s'il change) →
`io::connect_client` (`postgres::Config::password()`). Côté Python
(`propagator_rust.py::_propagate_via_rust`, docstring lignes 128-160) : `dsn`
est construit SANS credential (safe à logger) et `password` passé
explicitement. `ootils_kernel` bumpé 0.1.0 → 0.2.0 (`Cargo.toml` ET
`pyproject.toml`) pour marquer le changement de signature cassant ; un wheel
buildé avant 0.2.0 expose l'ancienne forme à 3 arguments — l'appeler avec 4
lève un `TypeError`, intercepté explicitement et re-levé en
`RuntimeError("wheel ootils_kernel < 0.2.0 incompatible, rebuild via
WITH_RUST=1 ...")` (`propagator_rust.py:186-199`). Confirmé : aucun chemin
côté Rust ne lit `PGPASSWORD` (les crates `postgres`/`tokio-postgres` ne la
lisent de toute façon jamais).

### 3. Commit mi-requête — documenté et durci, PAS refondu

Le `db.commit()` en milieu de fonction de `_propagate_via_rust`
(`propagator_rust.py:128-160`, inchangé — Rust ouvre SA PROPRE session
Postgres, qui doit voir les lignes event/calc_run/dirty_nodes que Python
vient d'écrire) est maintenant documenté intégralement en place, et un vrai
bug qu'il exposait est corrigé : un échec Rust APRÈS ce commit faisait
échouer le `ROLLBACK TO SAVEPOINT propagation_start` générique de
`process_event` (`propagator.py`, inchangé) — le savepoint mourait avec le
commit précédent — ce qui court-circuitait `fail_calc_run` et laissait le
calc_run bloqué en `running` + le lock advisory du scénario retenu jusqu'au
recyclage de la connexion poolée (`OOTILS_DB_POOL_MAX_LIFETIME_SECONDS`,
1800s par défaut). Fix, scopé entièrement dans `propagator_rust.py`
(`propagator.py` non touché) : `_fail_after_boundary_commit`
(`propagator_rust.py:234-265`) marque le calc_run `failed` via
`fail_calc_run` (qui relâche aussi le lock advisory de façon non
transactionnelle), commite cela durablement, puis rouvre un `SAVEPOINT
propagation_start` vide pour que le `ROLLBACK TO SAVEPOINT` ultérieur de
`process_event` devienne un no-op inoffensif au lieu d'une erreur dure.
`dirty_nodes` reste intact tout du long — la prochaine tentative de
propagation du scénario s'auto-guérit en recalculant les mêmes PIs. Le
docstring de `propagate_and_write` dans `lib.rs` est corrigé pour supprimer
deux affirmations fausses (« clear dirty_nodes », « une transaction
atomique ») et expliciter le vrai contrat multi-phases. Une **refonte**
complète vers un modèle snapshot-export/transaction unique était
explicitement hors scope de cette PR — signalée comme follow-up, non
tentée ici.

### 4. Parité CI baseline + fork overlay — écrite, PAS ENCORE câblée en CI

`tests/integration/test_rust_parity_integration.py` prouve que
`RustPropagationEngine` reproduit `SqlPropagationEngine` à l'identique
(champs de projection PI + lignes `shortages`) à la fois sur le graphe
baseline nu ET sur un fork portant un override de sécurité stock ADR-025
(`scenario_planning_overrides`) — le test qui garantit que le swap de moteur
ne peut pas silencieusement dé-forker la détection overlay-aware — plus le
contrat de récupération après échec mi-requête du point 3. **Trou honnête** :
ce fichier n'est actuellement PAS exercé par la CI. Le job `integration` de
`ci.yml` lance `tests/integration/` contre un vrai Postgres mais n'installe
jamais le wheel `ootils_kernel` (l'extra `[dev]` de `pyproject.toml` n'en
dépend pas) — le garde `pytest.importorskip("ootils_kernel", ...)` du fichier
le fait donc SKIP silencieusement. `rust-build.yml` build+installe bien le
wheel, mais n'a pas de service Postgres et ne lance que
`tests/test_rust_kernel_smoke.py`, jamais `tests/integration/`. Combler ce
trou — soit installer le wheel dans le job `integration`, soit ajouter un
service Postgres à `rust-build.yml` et y pointer ce fichier — reste un
follow-up ouvert, non fait par cet addendum.

### Défaut moteur — inchangé

`OOTILS_ENGINE` reste `sql` de bout en bout de ce chantier. `rust`
(Architecture A) et `rust-svc` (Architecture B) restent opt-in. Le seul
chemin vers un changement de défaut est l'item roadmap **SCALE-2**
(`docs/ROADMAP-AGENTS-2026-H2.md` §6 — ADR d'arbitrage porté par
l'architecte, Rust default-on vs lazy CoW vs statu quo), non résolu à ce
jour.

## References

- [POC kernel Rust — 32× speedup mesuré](../poc/rust_kernel/)
- [ADR-015 Rust readiness — préparation antérieure](ADR-015-rust-readiness.md)
- [PERF-BASELINE.md — chiffres pré-Rust](PERF-BASELINE.md)
- [PyO3 user guide](https://pyo3.rs/)
- [maturin docs](https://www.maturin.rs/)
