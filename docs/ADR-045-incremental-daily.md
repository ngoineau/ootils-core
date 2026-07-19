# ADR-045 — Quotidien incrémental (`process_pending`) + full hebdomadaire de réconciliation (chantier C3 « moteur d'exception »)

**Statut : Accepted (2026-07-19).** Chantier 3 du programme « moteur d'exception » (dossier de cadrage 2026-07-19), preuve 3 (quotidien incrémental). Bâti sur C2 ([ADR-044](ADR-044-value-ledger-changed-only.md), ingest changed-only) et sur son propre préalable C3-PR1 (#489, scope de `resolve_stale`). Livré en C3-PR2 ; C3-PR3 (dérive hebdomadaire) à venir.

## Contexte

La machinerie incrémentale (ADR-003 : dirty-flag + topo + `PROPAGATE_SQL`) existe et est rapide. C2 a supprimé le dernier faux dirty à la source (un re-push identique est désormais un no-op structurel, ADR-044). Restaient deux manques qui empêchaient un run quotidien *vraiment* incrémental :

- **Le déclencheur non-full était cassé.** `POST /v1/calc/run` avec `full_recompute=false` crée un event `calc_triggered` **sans `trigger_node_id`**, puis appelle `process_event` — qui à `propagator.py:175-176` logge « no trigger node — skipping propagation », marque l'event `processed` et appelle `_finish_run` **sans jamais propager**. Le chemin « incrémental » ne recomputait rien.
- **La vérité pénurie était structurellement full-run-shaped — LE blocker, pas la perf.** Avant #489, `ShortageDetector.resolve_stale` (appelé par `_finish_run` à chaque run) marquait `resolved` TOUTE pénurie active dont `calc_run_id != run courant`, **sur tout le scénario**. Couplé au déclencheur cassé ci-dessus, un `full_recompute=false` résolvait donc *toutes* les pénuries actives sans en re-détecter *aucune* — corruption silencieuse de `shortages`. Un run incrémental honnête aurait eu le même défaut sur les séries non touchées.

L'artefact `~/daily_recompute.py` de la VM (non versionné) contournait le problème en faisant un full à chaque nuit. Le besoin pilote (canal Dropbox, daily update « au plus vite ») est un recalcul quotidien **honnêtement incrémental** — seules les séries touchées la veille — doublé d'un **full hebdomadaire de réconciliation** anti-dérive.

## Décision

### 1. `resolve_stale` scopé au dirty set (#489, C3-PR1) — le préalable anti-corruption

Livré AVANT le batch consumer, par nécessité de correction (pas d'optimisation). `resolve_stale` ne retire plus une pénurie que si son `ProjectedInventory` porte `last_calc_run_id = calc_run_id` — le stamp uniforme que les DEUX moteurs écrivent sur chaque PI recomputé (Python : `GraphStore.update_pi_result(s)`, `store.py:423/475` ; SQL : `PROPAGATE_SQL`, `propagator_sql.py:233`). C'est la **généralisation stricte** du comportement full historique :

- **Full** — chaque PI actif porte le run courant, l'ensemble en scope est le scénario entier : comportement pré-C3 bit-pour-bit (le `EXISTS` est universellement vrai).
- **Incrémental** — seules les séries recomputées sont en scope ; une pénurie sur une série jamais touchée garde un `last_calc_run_id` antérieur et est **laissée intacte** au lieu d'être résolue à tort. Neutralise **par construction** la corruption du chemin « propagation sautée » : zéro PI stampé ⇒ zéro pénurie résolue.

`ShortageDetector` reste l'écrivain exclusif de `shortages` et de son cycle de résolution (ADR-021).

### 2. `process_pending(scenario_id, db, *, full=False)` sur la CLASSE DE BASE `PropagationEngine`

Un consommateur batch d'events, **polymorphe** (`propagator.py`). **UN calc_run, UN advisory lock, UN ANALYZE, UN `calc_run_finished` — jamais N.** Séquence :

1. `start_calc_run` (EXISTANT, `calc_run.py:90-138`) : advisory lock du scénario + **coalescence de TOUS les events pendants** (pas seulement les passés) + stamp du decision basis C2 (`anchor_date` résolu en-SQL, `engine_flavor`, `code_version`). Les events coalescés sont marqués `processed` à la complétion.
2. **INSERT set-based dans `dirty_nodes` — les SÉRIES ENTIÈRES.** Pour chaque `(item_id, location_id)` DISTINCT des `trigger_node_id` des events pendants, tous les PI actifs de la série sont dirtiés ; `full=True` → tous les PI actifs du scénario.
3. **`ANALYZE dirty_nodes` OBLIGATOIRE (#455).** Sans lui le planner voit `rows=1`, choisit un nested loop par ligne, et la propagation s'effondre en O(N²) (272× mesuré, `PERF-BASELINE.md`). Le site canonique est `DirtyFlagManager.flush_to_postgres` ; le chemin set-based de `process_pending` doit l'exécuter explicitement de la même façon.
4. Read-back du dirty set → `self._propagate(...)` (POLYMORPHE : sql/python/rust in-process héritent sans changement, y compris la détection pénurie `SHORTAGES_SQL` sur la session) → `_finish_run` (complétion + `resolve_stale` désormais scopé, §1) — cohérent par construction avec la granularité série-entière.

### 3. Granularité SÉRIE ENTIÈRE — le pourquoi

Dirtier la série complète (depuis le bucket 0), jamais un sous-ensemble de buckets, pour trois raisons convergentes :

- **Contiguïté exigée par `PROPAGATE_SQL`.** La projection window-function traite chaque série depuis son plus bas bucket dirty (le « seed ») et chaîne les suivants ; elle **ASSUME des buckets dirty contigus** (`propagator_sql.py:53-54`). Un seed à `seed_seq>0` lit son opening depuis le `closing_stock` du bucket précédent (`propagator_sql.py:92-100`) — non recomputé, cet opening est faux.
- **Invariant (b) `pi_chain_continuity` (migration 087).** `opening(bucket) = closing(bucket précédent)` le long d'un edge `feeds_forward` actif, asserté à ZÉRO ligne au teardown de chaque module d'intégration (`invariant_violations`). Un dirty partiel casserait la chaîne et ferait rougir le filet.
- **Bit-identique au full PAR SÉRIE.** Série entière depuis bucket 0 ⇒ `seed_seq=0`, opening = somme des OnHand (la vraie origine, `propagator_sql.py:76-91`), chaque bucket re-dérivé ⇒ sortie identique bit-à-bit à ce qu'un full produirait pour cette série. Le déterminisme (ADR-003) est préservé sans recomputer le scénario entier.

### 4. Rewire `POST /v1/calc/run` `full_recompute=false`

`calc.py:104-119` : au lieu du `process_event(event_id=trigger_event_id)` cassé (event sans trigger ⇒ propagation sautée), appeler `engine.process_pending(scenario_id, db)`. Le déclencheur le plus utilisé par les agents/CLI propage enfin.

`full_recompute=true` **INCHANGÉ** — chemin inline existant (`calc.py:66-103`), dont les mocks de `test_router_calc.py` assertent `_propagate`/`_finish_run` directement. NE PAS refactorer le full inline.

### 5. `scripts/daily_recompute.py` versionné (remplace l'artefact VM)

Le `~/daily_recompute.py` de la VM est un artefact non versionné ; ce script le **REMPLACE** dans le repo. Incrémental par défaut (`process_pending`), `--full` pour la réconciliation. Logging honnête : nombre d'events consommés, séries dirtiées, durée. Mêmes conventions d'arguments / exit codes que les scripts voisins (`run_daily_ingest.py`, `bench_mrp.py`). Le câblage du timer VM vers ce script versionné est une étape d'**assemblage** post-déploiement, hors de ce code.

### Hors périmètre

- **rust-svc** (Architecture B, ADR-017) override `process_event` **WHOLESALE**, pas `_propagate`, et calcule en RAM. `process_pending` sur la base ne le touche pas et n'est **pas supporté** sous `rust-svc` (limite documentée dans la docstring). sql / python / rust in-process héritent sans changement.
- **PAS de migration** en C3-PR2 (089 `drift_checks` = PR3). **PAS de checksum hebdomadaire** (PR3). **Pas de nouveau kill switch** : le run reste derrière `calc:run` (ADR-032) ; le script CLI suit le pattern dry-run/`--apply` des voisins.

## Conséquences

- Le **quotidien incrémental devient réel et honnête** : seules les séries touchées la veille cascadent ; un scénario sans event pendant est un no-op (aucune série dirtiée, aucune pénurie résolue à tort).
- `POST /v1/calc/run` `full_recompute=false` **propage enfin** — il n'était plus qu'un marqueur d'events `processed` doublé d'un résolveur destructeur.
- La **division quotidien-incrémental / full-hebdo-de-réconciliation** est posée : le full hebdo rattrape toute dérive que l'incrémental ne re-dirtie pas — un flip de flag post-load (type `is_stocking`, migration 081, sans re-dirty câblé, cf. ADR-021), un backfill de migration, une régression silencieuse.
- Le gain pilote visé : run quotidien ~10 min → 10-60 s à isopérimètre (« Kinaxis incrémental permanent » à une fraction du prix).
- **Prépare C3-PR3** : la détection de dérive hebdomadaire (migration 089 `drift_checks` + event `drift_detected`) comparera le résultat d'un full de réconciliation au dernier état incrémental et alertera sur tout écart — la preuve continue que l'incrémental ne diverge pas du full.

## Références

- `src/ootils_core/engine/orchestration/propagator.py` — `process_pending` (base, polymorphe), `process_event` (chemin trigger historique), `_finish_run`.
- `src/ootils_core/engine/orchestration/calc_run.py:90-138` — `start_calc_run` : coalescence + advisory lock + decision basis C2.
- `src/ootils_core/engine/orchestration/propagator_sql.py:49-102` — `PROPAGATE_SQL` : seed / contiguïté / opening bucket 0.
- `src/ootils_core/engine/kernel/shortage/detector.py:287-353` — `resolve_stale` scopé (#489, C3-PR1).
- `src/ootils_core/db/migrations/087_invariant_violations_view.sql` — invariant (b) `pi_chain_continuity`.
- `src/ootils_core/api/routers/calc.py:104-119` — rewire non-full ; `:66-103` full inline inchangé.
- `scripts/daily_recompute.py` — CLI versionné (incrémental par défaut, `--full`).
- [`ADR-044`](ADR-044-value-ledger-changed-only.md) — C2, préalable changed-only. [`ADR-003`](ADR-003-incremental-propagation.md) — dirty-flag + topo. [`ADR-021`](ADR-021-shortage-truth.md) — `shortages` truth, propriété `ShortageDetector`.
- Dossier de cadrage : scratchpad `DOSSIER-MOTEUR-EXCEPTION-2026-07-19.md` — chantier 3, §62/§74/§183-187/§223.
