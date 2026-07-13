# ADR-041 — SCALE-2 : arbitrage de l’architecture de fork (Rust ArcSwap vs lazy-CoW SQL vs statu quo)

**Statut** : **Accepted** — décision pilote 2026-07-13.
**Date** : 2026-07-12 (accepté 2026-07-13)
**Auteur** : architecte ootils-core

> **Décision rendue (2026-07-13)** — Réponse pilote à Q1/Q2 (§5) : la douleur
> était le **fork gouverné**, et les **5,93 s mesurés** post-ADR-040 sur la
> pilote (#460 mergée le 12/07, mieux que les ~8 s prédits ici) sont
> **acceptables**. La recommandation §4 est adoptée telle quelle : statu quo
> verrouillé (`OOTILS_ENGINE=sql`), sandbox B2 **non déclenché**, lazy-CoW
> **différé**, PyO3-défaut **rejeté** ; réouverture uniquement par les seuils
> T1/T2/T3. Q5/Q6 (frontière de gouvernance et expressivité du sandbox)
> restent volontairement **sans réponse** : elles ne se posent que si T1 se
> déclenche, et seront tranchées dans l'ADR de suivi prévu au §6.
**Déclencheur** : `docs/ROADMAP-AGENTS-2026-H2.md` §6, item **SCALE-2** (lignes 155-157) + décision pilote 2026-07-11 « le fork lent est un VRAI problème ».
**Dépend de / relie** : [ADR-012 fork bulk INSERT…SELECT](ADR-012-scenario-fork-bulk.md), [ADR-016 Rust Architecture A](ADR-016-rust-engine-foundation.md), [ADR-017 Rust Architecture B (rust-svc)](ADR-017-architecture-b-rust-engine-service.md), [ADR-018 per-scenario propagation](ADR-018-per-scenario-propagation.md), ADR-039 (PURGE-1, recyclage forks archivés — **en fabrication**), ADR-040 (fork quick-fix `session_replication_role` — **en cours**).
**Invariant en jeu** : CLAUDE.md « *the default does not flip without an ADR* ». Cet ADR est le véhicule décisionnel exigé par la roadmap. **Sa recommandation ne flippe PAS `OOTILS_ENGINE` (reste `sql`)** — voir §4.

---

## 1. Contexte & problème précis

`ScenarioManager.create_scenario` (chemin PG, `engine/scenario/manager.py`) est appelé par `/v1/simulate`, les promotions MPS, et **chaque agent qui branche l’état** (watchers #340, `lot_policy_watcher` ADR-025). Depuis ADR-012 il fait un **deep-copy bulk `INSERT…SELECT`** : coût **O(N) structurel** (temps ET stockage), *ce n’est pas une régression* — le code est identique depuis 2026-05-23 et son débit s’est même **amélioré** depuis mai (~13 K rows/s mesuré sur pilote vs ~8,7 K extrapolés dans ADR-012:84).

**Deux presses distinctes, à ne pas confondre** :

- **Presse STOCKAGE** — chaque fork duplique tout le jeu nœuds/edges (211 K → 431 K rows après bootstrap sur pilote, `docs/SCALABILITY.md:206-209`). **Traitée par ailleurs** : PURGE-1 (ADR-039, en fabrication) recycle les forks archivés. **Hors périmètre de cet ADR.**
- **Presse LATENCE** — c’est l’objet de SCALE-2 et de la décision pilote du 11/07. C’est ce que cet ADR arbitre.

Le nœud du problème : **deux usages du fork ont des exigences de latence divergentes**, et la roadmap note déjà que « les deux forks (RAM vs PG) divergent sémantiquement » (ROADMAP:156) :

| Usage | Cadence réelle | Latence tolérable | Contraintes dures |
|---|---|---|---|
| **Fork gouverné agent** (watchers #340, `lot_policy_watcher` ADR-025) | 1×/run, **hebdo aujourd’hui** | ~secondes OK (batch) | doit produire un **delta `shortages` canonique** (ADR-021), audit, `events`/stream, overrides ADR-025, diff/promote |
| **What-if interactif humain** (demande pilote) | à la demande, potentiellement répété | **sub-seconde souhaitée** | c’est une **requête** (comme `/v1/scenarios/compare` ADR-034) : pas de write gouverné, pas de reco dérivée directement |

Confondre ces deux cas conduit à sur-investir. La demande pilote « fork lent » ne dit pas *lequel* fait mal (🎯 Q1).

---

## 2. Dossier de mesures (tout vérifié — citer la source)

### 2.1 Fork deep-copy PG (l’existant)

- **13,2 s à 47 K nodes ; 23,8 s à 211 K nodes** (pilote) — le second confirmé `docs/SCALABILITY.md:185` (`POST /v1/simulate`, deep-copy 211 K nodes, **23.8 s**).
- Débit **~13 K rows/s** — meilleur qu’en mai (ADR-012:84).
- Décomposition du coût : **~76 % = triggers FK ligne-à-ligne**, **~20 % = maintenance des 17 index** sur `nodes`/`edges`. Le compute applicatif est marginal → **c’est du travail Postgres pur** (déclencheurs + B-trees), pas de l’orchestration Python.

### 2.2 Quick-fix ADR-040 (en cours)

`session_replication_role = replica` (désactive les triggers FK le temps du bulk copy) + contrôles d’intégrité **set-based** (une passe agrégée au lieu du check orphan par ligne). Gain visé **~3×** → **fork pilote ~8 s** à 211 K. **C’est un palliatif, pas une réponse** : il attaque les 76 % de triggers mais garde le coût O(N) structurel (les 20 % d’index restent, la copie reste intégrale).

### 2.3 Re-bench VM 2026-07-11, post-#455 (`docs/PERF-BASELINE.md:338-401`)

- Le fix #455 (`ANALYZE dirty_nodes` dans `flush_to_postgres`) a rendu le moteur SQL sain : profil S **1 099 s → 4,04 s (272×)** — l’ancienne régression O(N²) était une staleness de stats, pas une limite de compute.
- **SQL ≈ Rust in-process (PyO3) à ±4 % end-to-end** sur S/M/L. Full L : **SQL 16,3 s vs Rust 16,8 s** (SQL gagne). Incrémental (le chemin chaud des agents) : **SQL ≥ Rust** (p95 M : SQL ~80 ms vs Rust 341 ms — le Rust in-process paie une connexion PG dédiée + handshake par appel).
- Le **kernel Rust pur** reste rapide (**0,80 s pour 111 K PIs**, load 570 ms + compute 55 ms, `PERF-BASELINE.md:389`) **mais ce n’est que ~7,5 % du wall** — l’orchestration (persistance shortages, calc_run, resolve_stale) domine.
- **Conclusion actée** (`PERF-BASELINE.md:393-401`) : *Rust ≠ « moteur plus rapide », Rust = « architecture de scénarios en mémoire »*.

### 2.4 rust-svc (Architecture B, ADR-017) — le potentiel et les 3 blockers

- **Fork RAM mesuré ~1 µs** (`ArcSwap`, post-P2.1.a, ADR-018:20 « 49 ms → 1 µs ») — soit **~7 ordres de grandeur** sous le deep-copy PG à 211 K (23,8 s), la comparaison fork-vs-fork honnête. Propagation : **0,225 ms/event en batch** (ADR-018:60 ; le handshake gRPC ~5 ms domine hors batch).
- **3 blockers vérifiés** (déjà consignés dans CLAUDE.md §propagation flavour `rust-svc`) :
  1. **Table `shortages` jamais écrite** (`propagator_rust_svc.py:51-55,156-159`) → `/v1/issues` et les watchers **aveugles** ; `QueryShortages`/`StreamChanges` sont **déclarés dans le proto** (`engine.proto:92,98`) mais le service les renvoie `UNIMPLEMENTED` (`service.rs:754-770`).
  2. **N’override que `process_event`** → `/v1/simulate` (`simulate.py:234`), full-recompute (`calc.py:93`) et les forks watchers **dégradent silencieusement en Python pur** + désync RAM/PG ; les **forks PG sont NOT_FOUND** côté service (deux namespaces de scénarios disjoints).
  3. **Graphe RAM chargé au boot, sans reload/refresh RPC** (le proto `rust/ootils_proto/proto/engine.proto` n’en définit aucun) → **tout ingest le périme** jusqu’au redémarrage ; `MergeScenario` sans WAL.
- Maturité CI : la batterie `tests/engine_service/` tourne désormais en CI (job informatif `engine-service-it` de `rust-build.yml`, #453) avec **3 échecs connus** au premier passage (2 sémantiques overlay de scénario « 37 vs 28 » dans `test_agent_workflow.py`, 1 hypothèse de volume dans `test_scenarios.py::test_fork_scenario_returns_info`) — investigation ouverte, promotion du job en bloquant après correction ; CI cargo (`fmt`/`clippy`/`test`/proto-drift) **active depuis #453**.

### 2.5 Lazy-CoW SQL (« ADR-013-scenario-lazy-cow »)

- **Jamais écrit.** Dette actée dans ADR-012:67-72 et ADR-012:96-97, à l’origine flaggée par REVIEW-2026-05 R10.
- Modèle : le fork n’écrit qu’une ligne `scenarios` ; toutes les lectures retombent sur le parent via une **chaîne de scénarios**, les writes **matérialisent** à la demande. **Gain théorique** : fork **O(overrides)** quasi instantané, **ET supprime par construction** le coût FK/index par ligne (rien n’est copié → 0 trigger, 0 maintenance d’index au fork).
- **Coût réel dérivé du diagnostic ADR-012:26,67-72** (estimation d’effort, non mesurée) :
  1. Un **résolveur de chaîne d’ancêtres** (`WITH RECURSIVE` / `scenario_id = ANY(chain)`) visible de **chaque reader scoped** de `GraphStore`. Précédent proche mais plus léger : `resolved_params_sql()` (ADR-025) résout déjà une chaîne — pour **15 champs de params**, pas pour **le jeu de nœuds entier**.
  2. Réécriture de **la sémantique de lecture scoped** dans le moteur **SQL par défaut** : `PROPAGATE_SQL`/`SHORTAGES_SQL` + la **projection par window function** (`SUM() OVER` par série) doivent d’abord dédupliquer *most-specific-wins* (`DISTINCT ON (node_id)` par profondeur de chaîne) — non trivial sur une fenêtre cumulative.
  3. Un wrapper **materialise-or-update** autour de **chaque write kernel** (`GraphStore.set_node`/apply).
  4. Re-fondation du **chemin diff/promote** (`manager.py`, events `scenario_merge`).
  5. Migration si on dénormalise la chaîne.
  6. Re-validation **golden-master + parité 3-way** sur la nouvelle forme de lecture.
- **Estimation honnête : ~4-6 semaines focus, risque de régression ÉLEVÉ** — parce que ça réécrit la sémantique de lecture scoped dont dépend **tout l’axe persistance/query** (ADR-021 `shortages`, `/v1/issues`, ADR-034 compare, ADR-030 outcomes) sur **le seul moteur qu’on ne peut pas déstabiliser** (le défaut `sql`). C’est le **même argument « 3 mois de freeze / risque kernel »** qui a fait rejeter Architecture B dans ADR-016:100-108 — appliqué à une surface **encore plus chaude** (chaque lecture, pas seulement la propagation).

---

## 3. Les trois options, coûts réels

| Option | Cible latence fork | Effort | Risque | Ce qu’elle résout / ne résout pas |
|---|---|---|---|---|
| **A. Statu quo + ADR-040** (+ seuils de bascule) | 23,8 s → **~8 s** @ 211 K | **jours** (ADR-040 quasi fini) | Faible | Résout **définitivement le fork gouverné** (hebdo, batch OK). **Ne résout PAS l’interactif sub-seconde.** |
| **B. rust-svc sandbox-scopé** (2 variantes) | **~1 µs** fork + ~0,35 ms/event | B1 (parité complète) **6-10 sem** ; **B2 (sandbox fencé) 2-3 sem** | B1 élevé, **B2 modéré** | B2 résout **l’interactif humain**. Garde `shortages`/gouvernance sur SQL/PG. |
| **C. Lazy-CoW SQL** | fork **O(overrides)** quasi instant, sur l’axe **canonique PG** | **4-6 sem, risque ÉLEVÉ** | Élevé (réécrit la lecture scoped du moteur défaut) | Seule option qui rend l’axe **gouverné/persistant** instantané. Mais sa valeur *stockage* est neutralisée par PURGE-1. |

**Variantes de B** :

- **B1 — parité complète** : fermer les 3 blockers (§2.4). Blocker 1 = écrire `shortages` (write-behind + `QueryShortages`) ; blocker 2 = override `_propagate` (que `full_recompute`/`simulate`/forks watchers ne dégradent plus) + sync RAM/PG + résolution des forks PG ; blocker 3 = RPC reload/refresh + WAL sur `MergeScenario`. C’est **finir les phases 5-8 d’ADR-017** — ~6-10 semaines, et ça **rouvre le freeze** qu’ADR-016 avait refusé (ADR-016:100-108).
- **B2 — sandbox fencé (recommandé si l’interactif est le vrai besoin)** : **ne PAS faire de rust-svc le moteur défaut**. Le cantonner à un **rôle « bac à sable what-if pur »** : propagation de scénario interactive pour le pilote, où l’axe canonique (`shortages`, `/v1/issues`, watchers, diff/promote) **reste sur SQL/PG**. Le sandbox rend sa liste de pénuries **in-RAM directement à l’appelant interactif**, ne prétend jamais être l’axe de persistance. Cela **contourne par le SCOPE** : blocker 1 (jamais d’écriture `shortages`), blocker 2 (ne sert que `process_event`/`GetNode`/`QueryShortages`-in-RAM du sandbox, pas le chemin baseline `full_recompute`/`simulate`), blocker 3 (le sandbox assume boot-snapshot + refresh explicite). **Limite honnête** : l’expressivité what-if de B2 = **overrides de nœud uniquement** (avancer un receipt, changer une qté) ; **PAS** l’overlay 15-champs d’ADR-025 (résolu au read-time SQL, non porté dans le moteur RAM) — sauf effort additionnel pour porter `resolved_params` (🎯 Q6).

**Rejeté d’emblée — PyO3 (Rust in-process) par défaut** : le re-bench §2.3 le tue (**±4 %**, SQL gagne même sur L et sur le p95 incrémental). Aucun dossier. **Fermé, pas différé.**

### Lentille North Star par option

- **A** : forkable ✓, queryable ✓, streamable ✓, auditable ✓ — c’est le chemin canonique inchangé. Aucun anti-pattern.
- **B2** : forkable ✓, queryable-par-scénario ✓ (`GetNode(scenario_id)`). **Streamable/auditable/explicable ⚠ PAR DESIGN** : le sandbox est **une requête** (comme `/v1/scenarios/compare` ADR-034 : pas de ligne `events`, pas d’audit) — **acceptable UNIQUEMENT** si **aucune reco gouvernée n’en dérive son delta canonique** et **aucun L3+ n’y transite**. Frontière dure à graver dans l’ADR de suivi.
- **B1** : viserait tout dans rust-svc mais réintroduit l’anti-pattern « write invisible au stream » tant que blocker 1 n’est pas fermé — c’est *pourquoi* B1 est cher.
- **C** : forkable ✓ sur l’axe canonique (son atout unique) mais **risque de casser silencieusement l’axe persistance** pendant la transition — le danger est la régression, pas l’architecture cible.

---

## 4. Décision (recommandation — 🎯 le pilote tranche)

**Split phasé par usage, PAS un flip de moteur unique. `OOTILS_ENGINE` reste `sql` (invariant respecté).**

### Court terme (H3, maintenant) — livrer ADR-040

Merger le quick-fix ADR-040 sur le chemin deep-copy PG. **~8 s @ 211 K**, coût *jours*. C’est un **Pareto-strict** quelle que soit la suite : il rend le **fork gouverné** (watchers hebdo, ADR-025/#340) acceptable **définitivement**, et lève la douleur aiguë de la décision pilote sur ce chemin. **À faire indépendamment de l’arbitrage long.**

### Long terme — CONDITIONNEL, décidé par seuils mesurables

1. **Défaut inchangé** : `OOTILS_ENGINE=sql`. Pas de flip.
2. **SI** la vraie douleur pilote est la **boucle what-if interactive humaine** (🎯 Q1) **ET** qu’elle exige le sub-seconde → investir **B2 (rust-svc sandbox fencé)**, ~2-3 sem, **sans** fermer blocker 1. L’axe canonique reste SQL/PG. **Assigner explicitement les rôles** (résout la divergence RAM/PG notée ROADMAP:156 non pas en *unifiant* mais en *attribuant*) : **fork PG = canonique/gouverné** (overrides ADR-025, diff/promote, `shortages`) ; **fork RAM = sandbox interactif éphémère** (overrides de nœud, pas de promote, pas de `shortages` canonique).
3. **Lazy-CoW SQL : DIFFÉRÉ**, documenté comme escalade. Sa valeur *stockage* est neutralisée par PURGE-1 (ADR-039) ; sa valeur *latence* pour l’interactif est mieux et moins chèrement servie par le sandbox RAM ; et il porte le risque de régression le plus élevé sur le read path du défaut. **À rouvrir seulement si l’axe canonique/gouverné** (qui doit toucher `shortages`) **exige de l’O(overrides)** — cf. seuil T2.
4. **PyO3 défaut : REJETÉ** (§3).

### Seuils de bascule explicites (le mandat SCALE-2)

- **T1 — investir B2 (sandbox rust-svc)** : *après* livraison d’ADR-040, si **fork+propagate interactif p95 > 3 s à l’échelle pilote (211 K)** OU **> 20 what-if interactifs/jour** exercés par le pilote. (Écho d’ADR-016 D2-A « >25K SKU, <5 s interactif » et D2-B « 5+ users concurrents ».) Le seuil **3 s** est un 🎯 knob (tolérance interactive).
- **T2 — reconsidérer lazy-CoW SQL plutôt que B2** : si le **taux de forks gouvernés** (watchers) passe d’hebdo à **> 5 forks/heure soutenus** **ET** que le fork doit produire un **delta `shortages` canonique** (donc un sandbox RAM-only ne peut pas le servir) **ET** que les **~8 s** d’ADR-040 sont le goulot mesuré.
- **T3 — reconsidérer PyO3 défaut** : uniquement si un re-bench formel S/M/L (méthodo `PERF-BASELINE.md`) remontre **Rust > 2× SQL end-to-end** (pas le cas aujourd’hui).

---

## 5. Questions 🎯 pilote restantes

1. **QUEL fork fait mal ?** Le fork gouverné hebdo (23,8 s, 1×/run, batch — ADR-040 suffit) ou une boucle what-if humaine répétée (→ B2 nécessaire) ? **La réponse change tout.**
2. **Tolérance de latence interactive** : sub-seconde requis, ou ~8 s (post-ADR-040) acceptable pour le pilote ?
3. **Concurrence attendue** : combien d’humains/agents forkent en parallèle sur le pilote (trigger ADR-016 D2-B « 5+ users ») ?
4. **Appétit pour un 2e process déployé** (complexité opérationnelle rust-svc : ADR-017 §6 — 2 process, monitoring, staleness RAM/PG) vs rester mono-process ?
5. **Accepter la frontière de gouvernance de B2** : le sandbox est une surface **read/simulate uniquement** ; toute action qu’il inspire est **re-créée et validée sur le chemin gouverné SQL/PG** (aucun L3+ n’y transite ; aucune reco gouvernée n’y prend son delta canonique). OK ?
6. **Expressivité what-if de B2** : se contenter des **overrides de nœud**, ou financer le port de l’**overlay params ADR-025** (15 champs : lead times, safety stock…) dans le moteur RAM ?

---

## 6. Conséquences & non-décisions

- **Ce que cet ADR NE décide PAS** : il ne flippe **pas** `OOTILS_ENGINE` (reste `sql`) ; il ne green-lite **pas** rust-svc par défaut (B1 hors scope sauf déclenchement T1 → B2, qui est un sandbox étroit, **pas** le défaut) ; il ne ressuscite **pas** PyO3-défaut ; il ne traite **pas** l’empreinte stockage (= PURGE-1 / ADR-039).
- **Dépendance** : la jambe courte suppose ADR-040 mergé.
- **Suivi requis si T1 se déclenche** : un ADR de suivi scoperait le **contrat du sandbox** (namespace de scénarios propre, refresh/reload — blocker 3 —, frontière de gouvernance §5-Q5, expressivité §5-Q6). B2 n’est **pas** auto-adopté par cet ADR.
- **Alignement roadmap** : répond à SCALE-2 (ROADMAP:155-157) en **désambiguïsant** les deux forks divergents (RAM vs PG) par attribution de rôle plutôt que par unification.

## 7. Références

- ADR-012:26,67-72,82-97 (deep-copy bulk, dette lazy-CoW) · ADR-016:100-108,169-182 (rejet Archi B, triggers d’escalade) · ADR-017 §2-6 (rust-svc, blockers, phases) · ADR-018:19-40 (fork 1 µs, limites P2.1).
- `docs/PERF-BASELINE.md:338-401` (re-bench #455, SQL≈Rust ±4 %, Rust = archi RAM).
- `docs/SCALABILITY.md:176-213` (fork 23,8 s @ 211 K, O(N) stockage).
- `docs/ROADMAP-AGENTS-2026-H2.md:150-157` (SCALE-1/SCALE-2).
- CLAUDE.md §propagation flavours (les 3 blockers rust-svc), §North Star (anti-patterns), « the default does not flip without an ADR ».
- En fabrication : ADR-039 (PURGE-1, empreinte stockage), ADR-040 (fork quick-fix `session_replication_role`).
