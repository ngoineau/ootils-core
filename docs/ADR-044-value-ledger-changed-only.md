# ADR-044 — Deltas typés changed-only à l'ingest + decision basis (chantier C2 « moteur d'exception »)

**Statut : Accepted (2026-07-19).** Chantier 2 du programme « moteur d'exception » (dossier de cadrage 2026-07-19), clef de voûte des preuves 3 (quotidien incrémental) et 4 (replay bit-identique).

## Contexte

Le schéma `events` (migration 002) a été **conçu** pour le delta typé — `field_changed`, `old_date/new_date`, `old_quantity/new_quantity`, `old_text/new_text` — mais ces colonnes sont restées **vides à l'ingest** depuis l'origine : chaque ré-ingest émettait un `ingestion_complete` sans valeurs et marquait le nœud `is_dirty`, même quand rien n'avait changé. Conséquences mesurées au pilote : ~40 K events/jour sans information, et un re-push quotidien inchangé salissait tout le graphe (propagation complète au lieu d'un no-op).

## Décision

### 1. Changed-only aux 6 bras d'upsert de nœuds (`api/routers/ingest.py`)

on_hand, purchase_orders, forecasts, work_orders, customer_orders, transfers : le lookup est **élargi** pour relire les colonnes métier (aucun bras ne les relisait — le piège fondamental du cadrage), la comparaison passe par `_changed_fields`, et :

- **AUCUN changement** → `unchanged++` et `continue` : ni UPDATE, ni event, ni `is_dirty`, ni `_wire_node_to_pi`. Un re-push strictement identique est un **no-op structurel** (la garde est par construction : le seul émetteur par-champ est appelé dans la boucle `for … in changes`, et `changes` ne contient que des champs réellement différents).
- **Changement** → UPDATE (toutes les colonnes d'origine réécrites) + **un event typé PAR CHAMP changé** + `is_dirty` + wiring, comme avant.

`_ensure_projection_series` reste appelé AVANT le test unchanged (le self-heal `feeds_forward` de la leçon #468/080 est préservé).

### 2. La matrice d'émission (events = ledger de valeurs)

| Champ | event_type | Colonnes remplies |
|---|---|---|
| quantity (supply) | `supply_qty_changed` | old/new_quantity |
| time_ref (supply) | `supply_date_changed` | old/new_date |
| active (supply) | `supply_status_changed` (NOUVEAU) | old/new_text `'true'`/`'false'` |
| qty_uom (PO) | `supply_uom_changed` (NOUVEAU) | old/new_text |
| quantity (demande) | `demand_qty_changed` | old/new_quantity |
| time_ref (CO) | `demand_date_changed` (NOUVEAU) | old/new_date |
| active (CO) | `demand_status_changed` (NOUVEAU) | old/new_text |
| quantity/qty_uom (on_hand) | `onhand_updated` | old/new_quantity, old/new_text |

Choix structurants :
- **on_hand : `as_of_date` (time_ref) DÉLIBÉRÉMENT exclu de la comparaison** — le snapshot quotidien bouge par construction ; l'inclure détruirait le gain sur le flux le plus volumineux. `nodes.time_ref` d'un OnHand devient « la date du dernier changement de quantité » (la projection n'y lit jamais — vérifié : `opening_stock` somme les edges, pas le time_ref).
- **forecast : quantity SEUL** — `time_ref`+`time_grain` sont la CLÉ de lookup (changer = nouveau nœud) ; `time_span_*` sont re-dérivés de la clé quand qty change.
- **INSERT initial** : `ingestion_complete` conservé, avec `new_*` remplis et `old_*` NULL (idiome CDC création vs modification).
- Les 4 nouveaux types sont **émis par l'ingest, jamais POSTés** : absents de `VALID_EVENT_TYPES` (le router les refuserait — voulu) et de `FLEET_EVENT_TYPES` ; visibles sur `GET /v1/stream` (filtrables par `types=`), curseur des watchers non affecté (`drain_stream` avance sur tout, `RELEVANT_EVENT_TYPES` inchangé).

### 3. Decision basis (migration 088)

- `calc_runs.anchor_date` (= `COALESCE(scenarios.as_of_date, CURRENT_DATE)` paramétré, jamais wall-clock implicite), `engine_flavor` (reflet exact de `_build_propagation_engine`), `code_version` (env `OOTILS_CODE_VERSION` sinon git sha court, résolu UNE FOIS à l'import, `'unknown'` en conteneur sans `.git` — à câbler au déploiement).
- `recommendations.anchor_date` + `stream_seq_hwm` : stampés par les watchers supply à l'INSERT (`anchor_date` = `horizon_start` du run, `stream_seq_hwm` = HWM au moment de la lecture — `seed_cursor` en `--subscribe`, sinon `current_max_seq`). `transfer._upsert` ON CONFLICT DO NOTHING ⇒ une reco existante garde son HWM d'origine (correct : c'est la base de SA décision).
- « Ce que savait le moteur » devient une requête : l'état reconstruit par les events ≤ `stream_seq_hwm`.

### 4. Migration 088 — discipline de superset

Le CHECK `events.event_type` de la 088 = **union intégrale** 086 (24 types, PR-5b) + 4 nouveaux = 28 — jamais reparti de la liste de main (piège de l'élargissement séquentiel : une 088 écrite depuis 085 ferait sauter `reconciliation_completed` à l'application 086→088). Garde pure dédiée : `test_088_check_is_widened_from_086_not_085` + `test_088_keeps_reconciliation_completed_in_the_check`. La 087 (vue `invariant_violations`, C1) ne touche pas le CHECK.

## Conséquences

- `events` devient le **ledger de valeurs** par nœud (forensic : `quelle valeur, quand, d'où`) sans nouvelle table.
- Un re-push quotidien inchangé ne salit plus le graphe : **le quotidien incrémental (C3) devient possible** — seuls les nœuds réellement changés cascadent.
- Volume events **négatif** (−~40 K rows/jour au pilote ; seules les lignes modifiées émettent, +40 octets utiles sur celles qui restent).
- Prérequis posé pour `node_versions` (A2, chantier C4 replay) : le même chemin changed-only alimentera l'historisation des nœuds source.
- Le test #468 anti-double-edge a été **ré-exprimé** (re-date → re-ciblage de l'edge, ancien bucket vidé) : un ré-ingest à colonnes identiques est désormais un no-op prouvé par `test_c2_changed_only_ingest_integration.py`, et la garde anti-doublon s'exerce sur un vrai UPDATE.

## Références

- `src/ootils_core/api/routers/ingest.py` — `_changed_fields`/`_values_equal`/`_emit_field_change` + les 6 bras.
- `src/ootils_core/db/migrations/088_c2_value_ledger_and_decision_basis.sql`.
- `src/ootils_core/engine/orchestration/calc_run.py` — stamps + résolution `code_version`.
- `src/ootils_core/engine/recommendation/transfer.py`, `scripts/agent_{shortage,material,reschedule}_watcher.py` — stamps reco.
- `tests/test_c2_migration_088_contract.py` (11), `tests/integration/test_c2_changed_only_ingest_integration.py`, `tests/integration/test_c2_decision_basis_integration.py`.
- Dossier de cadrage : scratchpad `DOSSIER-MOTEUR-EXCEPTION-2026-07-19.md` §A1/§353-355.
