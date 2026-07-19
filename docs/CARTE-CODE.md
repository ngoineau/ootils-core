# CARTE-CODE — statut par module de `src/`

**Date :** 2026-07-19
**Chantier :** moteur-c7 (hygiène du « moteur d'exception »)
**Portée :** `src/ootils_core/` (Python) + renvois vers `rust/` et `scripts/` là où c'est pertinent.

Cette carte fige l'état de l'audit du jour (~15–16 K lignes mortes ou gelées identifiées). Elle
distingue trois statuts et sert de référence pour la couverture, le nettoyage futur et les pièges
de nommage. Vérifier la réalité runtime avant de traiter une capacité comme livrée (cf. CLAUDE.md).

## Légende des statuts

| Statut | Sens |
|--------|------|
| **VIVANT** | Sur un chemin servi (router monté, appelé par la propagation, un watcher, un CLI livré). Compte en couverture. |
| **GELÉ** | Compile et ses tests unitaires passent, mais **aucun chemin servi ne l'appelle**. Bannière `GELÉ` datée en tête. **Ne compte PAS en couverture.** Candidat de réactivation nommé. |
| **ENTERRÉ** | Supprimé (ce chantier ou un précédent). Ligne conservée pour la traçabilité. |

## 1. Vue d'ensemble — top-level `src/ootils_core/`

Total mesuré : **~68,6 K LOC** Python (hors migrations SQL).

| Module | Statut | LOC approx | Appelé par | Raison |
|--------|--------|-----------:|------------|--------|
| `api/` | VIVANT | 17 044 | ASGI `app.py` (34 routers montés) | Couche REST — surface servie. |
| `engine/` | VIVANT* | 24 790 | `api/`, watchers, CLIs | Cœur déterministe. *Poches gelées à l'intérieur, cf. §2. |
| `pyramide/` | VIVANT | 8 387 | `api/routers/pyramide.py`, watchers demande | Prévision demande (ADR-023/033/035). |
| `mps/` | VIVANT | 2 773 | `app.py` (`mps_router` monté) | Master Production Schedule. |
| `seed/` | VIVANT | 2 744 | `scripts/seed_demo_data.py`, démo | Jeu de données pilote. |
| `interfaces/` | VIVANT | 2 182 | `scripts/load_feed_contracts.py`, ingest gouverné | Feed contracts (INT-1, ADR-037/042). |
| `atp/` | VIVANT | 1 814 | `app.py` (`atp_router` monté) | Available-To-Promise. **Piège :** `atp/api.py` (fichier interne) est non-monté — router vient de `atp/__init__`. |
| `crp/` | VIVANT | 1 790 | `app.py` (`crp_router`), `mps/api.py` | Capacity Requirements Planning. |
| `staging/` | **ENTERRÉ (acté)** | 1 589 (+ router ~2066 avec `api/routers/staging.py`) | — (routes démontées) | **Enterrement acté ADR-042 PR-1** — HORS SCOPE de ce chantier, déjà démonté côté routes. |
| `forecasting/` | VIVANT | 1 094 | `api/routers/forecasting.py`, `pyramide/{engines,models,routing}` | Moteur MA/ES/Croston + seasonal-naive. |
| `_grpc/` | GELÉ | 829 | `engine_rust_service/` uniquement | Stubs gRPC rust-svc (Architecture B, non-défaut). Cf. §3 rust. |
| `models/` | VIVANT | 614 | tout le cœur | Schémas Pydantic / dataclasses. |
| `engine_rust_service/` | GELÉ | 522 | rust-svc uniquement (`OOTILS_ENGINE=rust-svc`, non-défaut) | Pont Python du service Rust autonome. Cf. §3. |
| `tools/` | VIVANT | 513 | watchers, `api/routers/simulate.py` | `agent_tools` (simulate_overrides / param_overrides). |
| `db/` | VIVANT | 454 | tout (pool + migrations) | Connexion, pool, runner de migrations. |
| `agent/` | PÉRIPHÉRIQUE | 442 | `scripts/run_agent_demo.py` seulement | Démo agent — pas sur un chemin servi de l'API ; non gelé (script de démo actif). À réévaluer. |
| `demo/` | VIVANT | 370 | `api/routers/demo.py` | Demo runs. |
| `drp/` | **GELÉ** | 269 | `tests/test_drp_models.py` seulement | **Voir §2 + §4.** Modèles distribution orphelins. **Ne pas confondre avec `engine/drp/` (VIVANT).** |
| `notifications/` | VIVANT | 258 | `engine/`, run quotidien | Webhook L3. |
| `scd2.py` / `constants.py` / `__init__.py` | VIVANT | 169 | tout | Utilitaires SCD2 + constantes. |

## 2. Détail `engine/` — poches gelées vs vivantes

`engine/` = 24 790 LOC, très majoritairement VIVANT. Sous-arbres :

| Sous-module | Statut | LOC approx | Raison |
|-------------|--------|-----------:|--------|
| `engine/mrp/` | VIVANT | 4 068 | Math core + moteur APICS (ADR-020). |
| `engine/kernel/` | VIVANT* | 3 364 | Graph/calc/shortage/explanation. *Poches gelées : `allocation/`, `temporal/bridge.py`. |
| `engine/dq/` | VIVANT | 3 097 | Data Quality + agent de remédiation. |
| `engine/scenario/` | VIVANT | 2 711 | Fork deep-copy, overlay params, compare. |
| `engine/orchestration/` | VIVANT | 2 134 | `propagator*.py` (sql défaut, python/rust flavours). |
| `engine/ingest/` | VIVANT | 1 411 | Ingest gouverné. |
| `engine/descent/` | VIVANT | 1 380 | Descente per-site → DRP → MRP central. |
| `engine/reporting/` | VIVANT | 1 188 | Rapports / KPI. |
| `engine/drp/` | **VIVANT** | 1 176 | **Le vrai DRP** (`core.py`, `loader.py`) — appelé par `engine/recommendation/transfer.py`, router `drp`. À ne pas confondre avec `ootils_core/drp/` (§4). |
| `engine/maintenance/` | VIVANT | 1 026 | Purge / rétention (ADR-039). |
| `engine/recommendation/` | VIVANT | 985 | Transfer / reschedule. |
| `engine/outcome/` | VIVANT | 760 | Reco → outcome (ADR-030). |
| `engine/ghost/` | VIVANT | 415 | Ghost / virtual supply. |
| `engine/events/` | VIVANT | 335 | Émission typée (`/v1/stream`). |
| `engine/snapshot/` | VIVANT | 244 | Snapshots inventaire (ADR-030). |
| `engine/graph_wiring.py` | VIVANT | 314 | Câblage graphe. |
| `engine/kernel/allocation/` | **GELÉ** | 355 | AllocationEngine — 0 appelant servi (seule mention hors tests : exemple de docstring de `_clock.py`). Candidat : substitution N-way. Bannière en tête de `engine.py`. |
| `engine/kernel/temporal/bridge.py` | **GELÉ (sursis)** | 395 | TemporalBridge — 0 appelant servi. Candidat : #433 (re-agrégation grain/cadence). Bannière datée en tête. |
| `engine/kernel/temporal/zone_transition.py` | **ENTERRÉ** | 706 | Elastic-time roll-forward jamais shippé. Supprimé ce chantier. |
| `engine/policies.py` | **GELÉ** | 181 | Politiques de réappro pures — 0 import servi. Candidat : MEIO (chantier 6b). Code laissé intact, bannière seule. |

## 3. Rust — GELÉ avec sa thèse (ADR-041 / T1)

L'ensemble Rust ne tourne **nulle part en déployé** (`OOTILS_ENGINE=sql` par défaut, jamais surchargé).
Statut **GELÉ**, thèse consignée en **ADR-041 / T1** ; SCALE-2 est le déclencheur de réexamen.

| Composant | Statut | LOC approx | Raison |
|-----------|--------|-----------:|--------|
| `rust/` (workspace : `ootils_kernel`, `ootils_engine`, `ootils_proto`) | GELÉ | ~11 437 (audit ; ~6,8 K de `.rs` purs) | PyO3 (Archi A) opt-in ; service gRPC (Archi B) non-défaut. Preuve de parité conservée. |
| `src/ootils_core/engine_rust_service/` | GELÉ | 522 | Pont Python du service Rust autonome (rust-svc). |
| `src/ootils_core/_grpc/` | GELÉ | 829 | Stubs gRPC générés (drift-checkés). |
| `engine/orchestration/propagator_rust.py` / `propagator_rust_svc.py` | GELÉ | (dans les 2 134 de orchestration) | Flavours Rust opt-in, non-défaut. |

**Garde CI (ajoutée ce chantier) :** `.github/workflows/rust-build.yml` déclenche désormais aussi sur
`engine/orchestration/propagator*.py` et `engine/kernel/**` — un changement de sémantique Python
re-lance la preuve de parité Rust-vs-SQL.

## 4. Pièges de nommage à connaître

- **Deux « drp » :** `ootils_core/drp/` (top-level, `DistributionLink`/`TransportationLane`, **GELÉ**, importé seulement par `test_drp_models.py`) ≠ `ootils_core/engine/drp/` (le planificateur **VIVANT**, `core.py`/`loader.py`). Le router `drp` monté vient de `api/routers/drp.py` et utilise `engine/drp`, jamais le top-level.
- **`zone_transition` (module) ≠ `zone_transition_runs` (table) :** le module Python est ENTERRÉ ; la table DB (migrations 002/003) reste en place, non touchée par ce chantier.
- **`atp/api.py` non-monté** : le router ATP servi vient de `atp/__init__` (`atp_router`), pas du fichier `atp/api.py` (résidu). `require_auth` n'y survit que comme alias de test.

## 5. Inventaire des lignes mortes / gelées (audit 2026-07-19)

| Cible | LOC (audit) | Statut après moteur-c7 | Décision |
|-------|------------:|------------------------|----------|
| `engine/kernel/temporal/` | 1 114 | zone_transition **ENTERRÉ** (706) ; bridge **GELÉ sursis** (395) | Elastic-time non shippé (ADR-002d amendé). |
| `engine/kernel/allocation/` | 355 | **GELÉ** | Candidat substitution N-way. |
| `engine/policies.py` | 175 | **GELÉ** | Candidat MEIO (6b) ; code intact. |
| `ootils_core/drp/` (top-level) | 263 | **GELÉ** | Vérifié encore mort (descente MRP ne l'a pas réveillé). |
| `staging/` (+ router) | 2 066 | **ENTERRÉ acté ADR-042 PR-1** | HORS SCOPE moteur-c7 — déjà démonté côté routes. |
| POCs forecast (`scripts/`) | ~800 | 5 **ENTERRÉS**, 1 conservé | Supersédés par `pyramide/segmentation.py` & co (ADR-035). Cf. §6. |
| Rust (`rust/` + ponts Python) | 11 437 | **GELÉ** | Thèse ADR-041 / T1 ; réexamen à SCALE-2. |

## 6. Journal des actions — chantier moteur-c7 (2026-07-19)

**Enterrés (supprimés) :**
- `src/ootils_core/engine/kernel/temporal/zone_transition.py` (706 LOC) + tests dédiés `tests/test_zone_transition.py` (1018) et `tests/test_sprint2_temporal.py` (1035, mixte — la couverture bridge est redondante avec `test_temporal_bridge.py`).
- `scripts/forecast_program_poc.py` (superseded par `pyramide/segmentation.py`, cf. ADR-035).
- `scripts/forecast_autoets_poc.py`, `scripts/forecast_q2m_poc.py`, `scripts/forecast_quarter_poc.py`, `scripts/forecast_reconcile_poc.py` (0 référence).
- **Conservé :** `scripts/forecast_poc.py` — référencé dans `docs/REVIEW-2026-07-APS.md` (§A7, PoC seasonal-naive validé) ; ne remplit pas le critère « 0 référence ». À arbitrer séparément.

**Gelés (bannière datée, code intact) :**
- `engine/kernel/temporal/bridge.py` — sursis, candidat #433.
- `engine/kernel/allocation/engine.py` — candidat substitution N-way.
- `engine/policies.py` — candidat MEIO (6b).
- `src/ootils_core/drp/__init__.py` — DRP multi-échelon (ADR-020).

**Autres :**
- `engine/kernel/temporal/__init__.py` — export `ZoneTransitionEngine` retiré.
- `docs/ADR-002d-elastic-time-final.md` — amendement daté « elastic time non shippé, enterré 2026-07-19, bridge.py en sursis ».
- `.github/workflows/rust-build.yml` — paths propagateur/kernel ajoutés aux déclencheurs de parité.
