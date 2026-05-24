# ADR-015 : Rust readiness — préparer la portabilité du kernel sans le porter encore

## Statut
DRAFT — 2026-05-24

## Contexte

Ootils est un moteur de décision supply chain AI-native (cf. VISION.md) écrit en Python 3.11 / FastAPI / psycopg3 / Postgres 16. Le kernel actuel (3 707 LOC dans `src/ootils_core/engine/kernel/`) implémente la propagation incrémentale, la détection de pénuries, la génération d'explications causales et l'allocation. Performance validée à 50 SKUs (démo) et 5 K SKUs (générateur seed) ; cible client estimée 500 SKUs en V1, 5 K+ en V2.

Trois constats convergents motivent cette ADR :

1. **Personne n'a benchmarké à l'échelle cible client en production**. Le `docs/SCALABILITY.md` documente des "breaking points" théoriques mais aucun benchmark récent ne mesure la latence réelle. Sans données, l'argument "Python est trop lent" est de la spéculation.

2. **Le kernel a été conçu comme portable** (commentaire dans `graph/store.py:1-6` : *"Designed as a clean interface for a future Rust replacement"*). Les chantiers récents 5+6 (déterminisme strict via `deterministic_uuid` + `Clock` injecté) renforcent cette propriété : même input ⟹ même output, prérequis pour un dual-run Python/Rust.

3. **L'équipe sait lire Python "plus ou moins", pas Rust**. Un port complet du repository en Rust éliminerait la capacité d'audit visuel sur ~36 K LOC. Inacceptable pour un système métier.

Cette ADR formalise une **stratégie d'anticipation** : préparer le code Python pour qu'un port vers Rust soit mécanique le jour où il sera justifié, sans payer le coût de ce port aujourd'hui.

## Décisions

### D1 — Cible architecturale : kernel-only hybride Rust (PyO3 bindings)

Le moteur Rust, lorsqu'il existera, remplacera **uniquement** les ~3 707 LOC du kernel via bindings PyO3. Le reste du repository (API FastAPI, staging pipeline, ingest endpoints, DQ pipeline, forecasting algos, scenario manager, agent LLM, démos) **reste en Python en permanence**.

Le critère de séparation : si le module a des SQL queries directes ou des algorithmes purs (propagation, allocation, scoring), il est candidat Rust. Si le module orchestre des appels HTTP, parse des fichiers, ou interagit avec un LLM externe, il reste Python.

Conséquence : la capacité d'audit visuel est préservée sur ~32 K LOC sur 36 K (~90 %). Les 10 % opaques sont concentrés sur le hot path algorithmique.

**Anti-décision** : pas de port total. Pas de réécriture des routers FastAPI en axum/actix. Pas de réécriture du staging parser en Rust. L'effort coût/bénéfice n'est pas justifié sur la couche I/O.

### D2 — Préparation maintenant, port après trigger objectif

On **prépare** maintenant (6 PRs, ~32-45h cumulés). On **porte** plus tard, lorsqu'un signal mesurable l'exige (cf. D5).

La préparation est utile **indépendamment** du port :
- mypy strict catch des bugs Python aujourd'hui
- SQL centralisation simplifie les tests
- Pure helpers améliorent la couverture de test
- Decimal discipline élimine une classe de bugs d'arrondi
- Benchmarks donnent la visibilité perf qui manque aujourd'hui

Si le port Rust n'arrive jamais, l'investissement est amorti par la qualité Python.

### D3 — Six PRs préparatoires séquencés

#### PR-A : Étanchéité des imports du kernel

**Objectif** : `engine/kernel/` n'importe rien depuis `engine/mrp/`, `engine/dq/`, `staging/`, `forecasting/`, `api/`. Le kernel doit pouvoir être copié-collé dans un repo isolé et compiler.

**Action** :
- Audit `grep -rn "^from ootils_core" src/ootils_core/engine/kernel/` pour repérer les imports interdits.
- Corriger les violations (inversion de dépendance : extraire un protocole ou déplacer la fonction).
- Ajout d'un CI gate (`scripts/check_kernel_imports.py`) qui échoue si un import interdit est détecté.
- Documentation du DAG des couches dans `docs/ARCHITECTURE-LAYERS.md`.

**Sortie attendue** : 0 import "vers le haut" depuis le kernel. CI gate actif sur main.

#### PR-B : mypy strict sur le kernel

**Objectif** : mypy passe de "non-blocking" à "blocking" sur `engine/kernel/**` uniquement. Le reste du repo reste en mode tolérant (statut actuel `mypy non-blocking is the expected red` documenté en mémoire).

**Action** :
- `pyproject.toml` : ajout d'un override mypy `[[tool.mypy.overrides]] module = "ootils_core.engine.kernel.*"; strict = true`.
- Annotation explicite de toutes les signatures non-typées dans le kernel.
- CI : job mypy dédié sur le kernel, échec bloquant.
- mypy reste non-blocking sur le reste du repo (pas de big-bang).

**Sortie attendue** : `mypy --strict src/ootils_core/engine/kernel/` retourne 0 erreur. Le job CI dédié est bloquant.

#### PR-C : Extraction de pure helpers

**Objectif** : maximiser la part du kernel qui est testable sans DB et portable 1:1 en Rust. Aujourd'hui le kernel a déjà quelques helpers purs (`compute_llc_pure`, `_priority_key`, `scd2.decide_action`, `deterministic_uuid`, `SystemClock`). On en extrait davantage.

**Cibles identifiées** (non exhaustif, à confirmer pendant l'audit) :
- `shortage/detector.py` : extraire `compute_severity_score(qty, days, unit_cost) -> Decimal` du flux mêlant logique + persistance.
- `allocation/engine.py` : extraire `allocate_quantity_greedy(demand_qty, supplies: list[Supply]) -> AllocationDecision` de `_allocate_demand`.
- `temporal/zone_transition.py` : extraire `enumerate_day_buckets(span_start, span_end) -> list[(date, date)]` et `enumerate_week_buckets(...)`.
- `crp/engine.py` : extraire `schedule_operations_backward(operations, due_date, work_centers) -> list[OperationScheduled]`.

**Sortie attendue** : ≥ 30 % du kernel est constitué de pure functions sans DB ni I/O. Chaque pure helper a ses tests unitaires (no `conn` fixture).

#### PR-D : Discipline Decimal stricte dans le kernel

**Objectif** : zéro `float` dans le kernel. Tout en `Decimal`. Postgres `NUMERIC(N,M)` mappe vers `rust_decimal::Decimal` proprement uniquement si Python n'a pas pollué avec des `float`.

**Action** :
- Audit : `grep -rnE ": float|float\(|: List\[float\]|: dict\[.*float" src/ootils_core/engine/kernel/`. Convertir vers `Decimal`.
- Audit des littéraux numériques : `0.5`, `1.0` ⟹ `Decimal("0.5")`, `Decimal("1")`.
- Helper `_ensure_decimal(x: Any) -> Decimal` qui rejette `float` au runtime avec exception explicite.
- Documentation `docs/DECIMAL-DISCIPLINE.md` : règles de mapping `NUMERIC(18,6)` ↔ `Decimal` ↔ `rust_decimal::Decimal`. Notamment : précision, mode d'arrondi (`ROUND_HALF_EVEN` par défaut côté PG), comportement de la division.

**Sortie attendue** : 0 `float` dans le kernel. CI mypy strict sur les types numériques.

#### PR-E : Centralisation SQL dans `GraphStore`

**Objectif** : aujourd'hui 28 `db.execute(...)` existent dans le kernel **hors** `graph/store.py` (mesuré pendant la revue fonctionnelle). ADR-014 §Ouvertures flag ce point. Cette PR le règle.

**Action** :
- Migrer les SQL de `shortage/detector.py` (persist, resolve_stale, get_active_shortages) → nouvelles méthodes `GraphStore.persist_shortage(...)`, `GraphStore.resolve_stale_shortages(...)`, etc.
- Idem `explanation/builder.py` → `GraphStore.persist_explanation(...)`, `GraphStore.get_causal_steps(...)`.
- Idem `graph/dirty.py` → `GraphStore.mark_dirty(...)`, `GraphStore.flush_dirty_to_postgres(...)`.
- Si des SQL doivent rester direct-SQL hors store (cas rare, ex : transactions complexes), header comment explicite documentant le carve-out, comme pour les JSONB / DROP TABLE.

**Sortie attendue** : `grep -rE "db\.execute|conn\.execute" src/ootils_core/engine/kernel/ | grep -v graph/store.py` retourne 0 (ou uniquement des carve-outs documentés).

#### PR-F : Suite de benchmarks

**Objectif** : aujourd'hui aucune mesure de performance n'existe. Sans baseline, l'argument "Rust est nécessaire" est subjectif. Avec baseline, la décision devient mesurable.

**Action** :
- `tests/perf/` : suite `pytest-benchmark` (ou `pytest --benchmark-only`) qui mesure :
  - Propagation full sur 50 / 500 / 5 K SKUs (réutilise `seed_realistic_dataset.py` profile S/M/L)
  - Allocation greedy sur 1 K demands
  - Shortage detection batch sur 5 K PI nodes
  - MRP APICS run sur BOM 5-niveau
  - Forecast generation MA / EXP_SMOOTHING / CROSTON
  - Scenario fork + simulate + diff
- Tracking historique : `docs/PERF-BASELINE.md` met à jour les résultats au fil des PRs, avec date + commit SHA + métriques.
- CI bench : run **hebdomadaire** (cron GitHub Actions), pas à chaque PR (trop long). Tag manuel `bench-now` déclenche un run on-demand.

**Sortie attendue** : baseline mesuré et commité dans `docs/PERF-BASELINE.md`. Job CI hebdo opérationnel.

### D4 — Critères "Rust-ready" (acceptance pour clore la phase préparatoire)

Le repository est déclaré "Rust-ready" lorsque les 6 critères suivants sont remplis :

1. ✅ `engine/kernel/` n'importe rien depuis l'extérieur de `engine/kernel/`, `models/`, `db/connection.py` (PR-A).
2. ✅ `mypy --strict src/ootils_core/engine/kernel/` passe sans erreur (PR-B).
3. ✅ ≥ 30 % des LOC du kernel sont des pure functions testables sans DB (PR-C).
4. ✅ 0 `float` dans le kernel ; Decimal discipline documentée (PR-D).
5. ✅ Toute SQL du kernel passe par `GraphStore` (ou carve-out explicite et justifié) (PR-E).
6. ✅ Baseline de performance mesuré, tracké dans `docs/PERF-BASELINE.md`, refreshé hebdo (PR-F).

Lorsque ces 6 critères sont remplis, on peut **estimer honnêtement** que le port Rust kernel prendrait 4-6 semaines (vs 8-12 si on partait dans l'état actuel).

### D5 — Trigger conditions pour démarrer le port Rust

Une fois "Rust-ready", on ne démarre le port que si **au moins une** des conditions suivantes est satisfaite :

**Trigger A — perf insuffisante mesurée** :
- Le benchmark sur la cible client (typiquement 500-1 000 SKUs) montre une latence p95 > 30 s sur une propagation full.
- ET l'option `OOTILS_ENGINE=sql` (déjà disponible en production) ne suffit pas à descendre sous ce seuil.

**Trigger B — déploiement contraint** :
- Le besoin d'un binaire statique (déploiement edge, on-premise no-Python) émerge avec un client signé.

**Trigger C — concurrence parallèle obligatoire** :
- Un cas d'usage exige de propager **plusieurs scénarios en parallèle vrais** (multi-coeurs sans GIL), ex : 1 000 simulations agent par décision-cycle.

**Anti-trigger** : "ça serait cool d'avoir du Rust" n'est PAS un trigger. La démangeaison technique seule ne justifie pas le coût d'opacité.

### D6 — Anti-décisions explicites

- **Pas de port avant les 6 critères D4 remplis.** Y compris si la tentation est forte.
- **Pas de port total** (jamais). Cible kernel-only hybride permanente.
- **Pas de réécriture des tests Python en Rust.** Les tests d'intégration tapent les endpoints REST, indépendants du langage du kernel underlying. Au port, on ajoute des tests Rust unitaires (`cargo test`) en complément, pas en remplacement.
- **Pas de port "à temps perdu"** (= entre 2 PRs business). Le port doit être traité comme un projet dédié avec début / milieu / fin clairs, sinon il traîne 18 mois.

### D7 — Plan de migration (déclenché si trigger D5 satisfait)

Documenté pour mémoire — **n'est PAS engagé par cette ADR**.

**Phase Rust-1 — POC (1 semaine)** : port en Rust de `compute_llc_pure` + `deterministic_uuid` + `_priority_key`. PyO3 bindings via `maturin develop`. Validation byte-à-byte vs Python sur le générateur seed.

**Phase Rust-2 — GraphStore (2 semaines)** : port complet du store (CRUD nodes / edges / projection series / shortages / explanations). Bindings côté Python. Tests d'intégration restent verts.

**Phase Rust-3 — Propagation (2 semaines)** : port `propagator` + `traversal` + `dirty`. Hot path principal. Dual-run flag `OOTILS_KERNEL=rust|python` actif.

**Phase Rust-4 — Shortage + Explanation + Allocation (1-2 semaines)** : port des modules métier kernel.

**Phase Rust-5 — Cleanup + dépréciation Python kernel (1 semaine)** : suppression du code Python kernel, flag `OOTILS_KERNEL` retiré.

**Total estimé une fois Rust-ready : ~6-8 semaines.**

## Conséquences

### Effets sur la collaboration

L'équipe garde sa capacité d'audit visuel sur 100 % du code pendant toute la phase préparatoire. Les 6 PRs sont en Python pur, lisibles. ADRs en français, formats inchangés.

Si la phase de port est un jour déclenchée (D5), l'équipe accepte explicitement la perte d'audit visuel sur les 3 707 LOC du kernel, en échange de mitigations explicites (dual-run, docs en français, tests d'intégration Python préservés). Le choix sera revalidé à ce moment-là — cette ADR ne le tranche pas.

### Effets sur la performance

À court terme : aucun effet. Le code Python tourne pareil.
À moyen terme : les benchmarks (PR-F) donneront une visibilité qui manque aujourd'hui. Peut révéler qu'`OOTILS_ENGINE=sql` (déjà disponible) suffit, ce qui **élimine le besoin de Rust**.

### Effets sur la qualité

Bénéfices Python-only des 6 PRs (mypy strict, Decimal discipline, pure helpers, SQL centralisé) :
- Moins de bugs runtime sur le kernel
- Tests plus simples (pure helpers testables sans `conn`)
- Refactor cross-cutting plus sûr (mypy attrape les sites)
- Document clair pour l'onboarding de nouveaux contributeurs

### Risques

| Risque | Mitigation |
|---|---|
| Les 6 PRs deviennent une excuse pour ne jamais faire Rust | D5 — triggers objectifs et mesurables. Trigger A en particulier passe automatiquement la décision en main si le benchmark le montre. |
| mypy strict bloque tout le développement | PR-B limite mypy strict au seul `engine/kernel/`. Le reste du repo reste tolérant. |
| Decimal discipline introduit des régressions de perf Python | Mesuré par le benchmark PR-F. Si régression > 20 %, on évalue la balance discipline vs perf. |
| L'estimation 6-8 semaines pour le port est optimiste | Acceptée comme estimation directionnelle. Sera revalidée trimestriellement, et au moment du trigger D5. |
| Le marché bascule vers une autre stack avant qu'on porte | Acceptable. Les bénéfices Python des 6 PRs restent acquis. |

## Ouvertures (out of scope cette ADR)

- **Évaluation d'alternatives non-Rust** au moment du trigger : Cython, mypyc, Nuitka, ou simplement Postgres window functions plus poussées. Ces options doivent être benchmarkées avant Rust.
- **Choix de la stack Rust** au moment du port : tokio-postgres vs sqlx, axum vs actix (si full port jamais envisagé), PyO3 version, etc. À trancher dans une ADR-016 le jour venu.
- **Process de revue de code Rust** : si l'équipe n'a pas d'expertise Rust, qui review ? Sous-traitance ? Formation interne ? À discuter au moment du trigger.
- **Coût opérationnel CI Rust** : cache `target/`, sccache, sccache-distributed. Configuration à faire au démarrage Rust, pas avant.

## Décision finale demandée

Cette ADR engage **uniquement** les 6 PRs préparatoires (D3) et les critères de Rust-ready (D4). Le port lui-même reste conditionnel à D5.

Approuver cette ADR signifie :
- ✅ démarrer PR-A dans la foulée (étanchéité imports kernel)
- ✅ accepter le calendrier de ~3 semaines pour les 6 PRs
- ✅ accepter que le port Rust est différé et conditionnel
- ❌ ne pas écrire une ligne de Rust avant validation des 6 critères
