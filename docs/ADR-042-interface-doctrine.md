# ADR-042 — Doctrine des interfaces : fichier pivot TSV, ingestion gouvernée, réconciliation heuristique

**Statut** : **Accepted** — décision pilote 2026-07-13 (cadrage architecte + 3 réponses structurantes du pilote intégrées : `customer_orders` blocking, réconciliation heuristique sans `ootils_ref`, ordre de livraison « la valeur d'abord »).
**Date** : 2026-07-13
**Auteurs** : architecte ootils-core (cadrage) + pilote (décision)
**Contexte mesuré** : [ADR-013](ADR-013-external-interfaces.md) (DRAFT depuis 2026-05-23, jamais fait passer à Accepted, D4 = approbation humaine systématique) ; [ADR-037](ADR-037-daily-run-and-governed-ingest.md) (INT-1, registre `feed_contracts` — PR1 seule livrée, « registre mort » par construction) ; `docs/SPEC-INTERFACES.md`, `docs/WIP-inbound-interfaces-spec.md`, `docs/SPEC-INTERFACES-INBOUND-V1.md` (trois specs antérieures, jamais réconciliées entre elles) ; `docs/contracts/TSV-FILES-SPEC.md` (référence colonnes) ; `config/feed-contracts/*.yaml` (3 contrats seed).

---

## Contexte

### L'état « 4 chemins concurrents »

Le dépôt contient aujourd'hui **quatre** façons distinctes de faire entrer une donnée dans Ootils, jamais arbitrées entre elles :

1. **Le pipeline `staging`** (`src/ootils_core/staging/{parser,loader,diff,approve,reject}.py`, monté via `api/routers/staging.py` — `staging.router` inclus dans l'app à `src/ootils_core/api/app.py:426`, import à la ligne 38). C'est le chemin le plus complet sur le papier : DQ L1-L4, un `GET /diff` qui calcule un `deletion_ratio` et bloque au-delà de 20 % sauf `force=true` explicite (`staging/diff.py:41`, `DELETION_RATIO_THRESHOLD = 0.20`), un `reject_batch` qui exige une raison non vide et trace `rejected_by`/`rejection_reason` (`staging/reject.py:42-43,62-63`). Mais `approve_batch` (`staging/approve.py:154`) exige `status == "validated"` — un statut que **rien dans le dépôt ne fait atteindre automatiquement** : il faut un `POST /v1/staging/upload` suivi d'un clic humain explicite sur `POST /v1/staging/batches/{id}/approve`. En pratique ce chemin n'est jamais câblé en production — c'est un chemin complet mais mort.
2. **L'ingest JSON direct** (`POST /v1/ingest/<entity>`, `src/ootils_core/api/routers/ingest.py`) — le chemin réellement utilisé aujourd'hui. Écriture canonique **immédiate** (ex. `items` : `INSERT`/`UPDATE` aux lignes 522-544) suivie d'un `_trigger_dq(db, batch_id)` (ligne 547) dont le docstring dit explicitement *"Run DQ pipeline on a batch. Returns dq_status string, never raises"* (`ingest.py:246-254`) — la DQ est donc **après** l'écriture canonique et **jamais bloquante** sur ce chemin : un batch structurellement délétère mais DQ-rouge est déjà en base au moment où la DQ tourne. C'est exactement l'inverse de l'ordre qu'un pipeline gouverné doit garantir (DQ **avant** canonique).
3. **`scripts/bulk_ingest.py`** — chargeur `COPY`/`INSERT...ON CONFLICT`, contourne entièrement la couche API (son propre docstring : *« Bypasses the API layer... Suitable for INITIAL pilote loading and bulk refreshes »*). Un outil de bootstrap, pas un chemin récurrent.
4. **`scripts/ingest_file.py`** — lit un TSV depuis `data/inbox/`, construit le payload JSON, et appelle les mêmes endpoints `/v1/ingest/<entity>` **en process via `FastAPI TestClient`** (docstring : *"calls the appropriate /v1/ingest/<entity> endpoint via the FastAPI TestClient (in-process, no HTTP server required)"*). C'est le chemin le plus proche d'un dépôt de fichier TSV, mais il reste un outil manuel/dev invoqué à la main, sans gouvernance, sans table de suivi de run.

Aucun de ces quatre chemins n'est un **pipeline quotidien multi-flux gouverné**. [ADR-037](ADR-037-daily-run-and-governed-ingest.md) (INT-1, 2026-07-11) a commencé à poser la pièce manquante — le registre `feed_contracts` (migration 073, 3 YAML seed sous `config/feed-contracts/`) — mais sa PR1 est explicitement un **« registre mort »** : citation de l'ADR lui-même, *« Aucun runtime ne lit encore le registre »*. PR2 (table `daily_runs` + évaluation runtime des gardes), PR3 (décision gouvernée) et PR4 (surface REST) étaient planifiées mais non implémentées.

Trois specs antérieures décrivaient chacune un fragment de cible sans se réconcilier : `docs/SPEC-INTERFACES.md` (référence mixte actuel/cible, très large, agent-facing/MCP/webhooks compris), `docs/WIP-inbound-interfaces-spec.md` (brouillon V0, catalogue de 30+ interfaces théoriques M01-S04, jamais implémenté), `docs/SPEC-INTERFACES-INBOUND-V1.md` (doc « décision COO » du 2026-04-19, pose JSON comme transport de référence et les fichiers/colonnes comme seule vérité métier — position qui entre en tension directe avec la doctrine ci-dessous, voir « Supersessions »).

### Le cadrage pilote du 2026-07-13

Le pilote a tranché trois points structurants qui débloquent la doctrine :

- **`customer_orders` devient un flux BLOCKING** (et non advisory comme un flux référentiel) : sans demande ferme à jour, la détection de pénurie est structurellement borgne — un run qui tournerait sans commandes clients fraîches produirait des recommandations fondées sur une image de demande obsolète.
- **La réconciliation sortante est HEURISTIQUE, jamais par identifiant échoué.** Il n'existe pas de champ `ootils_ref` que l'ERP du pilote peut faire l'aller-retour dans son propre PO — la seule option réaliste est un rapprochement déterministe sur des attributs métier (item, site, fournisseur, quantité, date), avec un taux d'ambiguïté mesuré et rapporté, jamais caché.
- **L'ordre de livraison est « la valeur d'abord »** : le chantier commence par ce qui produit un run quotidien gouverné utile (garde + décision + wiring + compte-rendu) et ne ferme les deux chemins concurrents (staging enterré, ingest direct fenced) qu'**après** — pas comme préalable bloquant.

Cet ADR est le véhicule décisionnel pour ces trois réponses, plus le cadrage architecte complet qu'elles débloquent.

---

## Décisions

### 1. Les flux V1 (le pivot fichier = LE contrat)

Chaque flux est coupé en deux moitiés autour d'un fichier pivot TSV nommé `<entity_type>_<YYYYMMDD>.tsv` — **c'est le contrat**, pas un détail de transport. La moitié entreprise dépose/reprend à sa cadence sans connaître Ootils ; la moitié Ootils reçoit/contrôle/charge/rend compte sans connaître l'ERP. Règle pilote déjà actée (2026-07-11, capturée dans [ADR-037](ADR-037-daily-run-and-governed-ingest.md) §1) : **TSV uniquement**, jamais de CSV (« c'est un enfer »), en entrant comme en sortant.

**Entrants** :

| Flux | Cadence | Criticité | Notes |
|---|---|---|---|
| `on_hand` | Quotidien | **Blocking** | Toute la chaîne pénurie/MRP part du stock physique (déjà déclaré `on-hand.yaml`, ADR-037). |
| `purchase_orders` | Quotidien | **Blocking** | Un PO ouvert manquant fait recommander une commande en double (déjà déclaré `open-purchase-orders.yaml`). |
| `work_orders` | Quotidien | Advisory | Tous les clients pilotes ne font pas de manufacturing discret suivi par OF (déjà déclaré `open-work-orders.yaml`). |
| `customer_orders` | Quotidien | **Blocking** — **décision pilote 2026-07-13** | Sans demande ferme, la détection de pénurie est borgne. Aucun `feed_contracts` YAML n'existe encore pour ce flux (voir §8, Q3). |
| `forecasts` | Hebdomadaire | Advisory | 🎯 Q2 ouverte : source ERP ou Pyramide (le module de prévision propre au dépôt). |
| Référentiel (`items`/`locations`/`suppliers`/`supplier_items`/`item_planning_params`/BOM) | À la demande | Advisory | **Jamais dans le run bloquant quotidien** — un changement de référentiel ne doit pas retarder le run pénurie du jour. |

**Historiques de calibrage** (ajout décision pilote 2026-07-13, « GO famille historiques ») :

Troisième famille, distincte des photos quotidiennes de l'ouvert : les **historiques du réalisé**, carburant du calibrage des paramètres (chantier PARAM-1). Cadence lente, **jamais bloquants** pour le run quotidien, chargement **append-only** (on empile l'historique, on n'écrase jamais — même logique que `demand_history`).

| Flux | Cadence | Criticité | Ce qu'il calibre |
|---|---|---|---|
| `po_receipts_history` (PO clôturés : date commande → date réception réelle) | Mensuel | Advisory | **Délais fournisseurs réels** vs `lead_time_*` théoriques de l'ERP — le premier paramètre que PARAM-1 doit challenger — et fiabilité fournisseur (OTIF). |
| `wo_closed_history` (OF clôturés) | Mensuel | Advisory | Délais de fabrication réels, rendements. |
| `consumption_history` (sorties de stock composants) | Mensuel | Advisory | Stock de sécurité des **composants** — `demand_history` couvre les ventes (produits finis), pas la consommation induite par la BOM. |
| `transfers_history` (transferts inter-sites réalisés) | Mensuel | Advisory | Flux inter-sites réels, calibrage des lanes DRP. Distinct de la question ouverte « transferts *ouverts* en flux quotidien » (voir Supersessions). |

Déjà couvert, à ne PAS dupliquer : l'**historique de la demande** (`demand_history`, commandes clients historisées à la prise de commande — règle pilote « forecast on booking ») est déjà chargé au bootstrap (~1,8 Go sur la base pilote) et alimente Pyramide/FVA/drift ; `returns_history` (migration 050) existe également. Ces flux historiques sont des **prérequis de PARAM-1** (PR-6+) : aucun d'eux n'est nécessaire aux PR-2/3/4 du run quotidien. Contrats colonnes et `feed_contracts` YAML : à écrire au moment de PARAM-1 (voir Q7 ci-dessous).

**Sortants** :

| Flux | Cadence | Contenu |
|---|---|---|
| `po_drafts` | Quotidien | Recommandations `APPROVED` de type `ORDER_NOW`/`ORDER_RUSH`/`EXPEDITE`. |
| `reschedule_messages` | Quotidien | `RESCHEDULE_IN`/`RESCHEDULE_OUT`/`CANCEL` (ADR-026). |
| `param_updates` | Hebdomadaire | Chantier PARAM-1, réservé (§6, non commencé). |
| `daily_report_<date>.txt` | Quotidien | Voir décision 5. |

### 2. Le sort des quatre chemins concurrents

Aucun des quatre chemins actuels (§ Contexte) n'est supprimé brutalement — chacun reçoit un rôle explicite :

1. **`staging/*` → enterré.** Jamais câblé jusqu'à `status='validated'` (`staging/approve.py:154`), seulement 3 entités de master-data couvertes, jamais utilisé en production. `/v1/staging/*` est retiré de l'API (le routeur `staging.router`, `api/app.py:426`, sera dé-monté dans la PR-1 du plan §6). **Les deux gardes de valeur sont sauvées, pas perdues** : le garde ratio-suppression 20 % (`staging/diff.py:41`, `DELETION_RATIO_THRESHOLD`) et l'audit de rejet (`staging/reject.py:42-43`, `rejected_by`/`rejection_reason`) sont **relogés** comme gardes du nouveau pipeline gouverné (décision 3) — leur logique reste utile, seul le chemin d'exécution mort autour d'eux disparaît.
2. **L'ingest JSON direct → fenced, pas supprimé.** L'upsert canonique est extrait dans un service `engine/ingest/apply.py` (**à écrire**, PR-1 du plan §6), appelé à la fois par le futur pipeline gouverné et par l'endpoint HTTP existant — un seul écrivain canonique, deux appelants. Cette extraction corrige au passage l'inversion DQ-avant-canonique constatée en Contexte (`_trigger_dq` après l'écriture, jamais bloquant) : le service `apply` fera tourner la DQ **avant** l'écriture canonique. Un kill switch `OOTILS_DIRECT_INGEST_ENABLED` (**à créer**, n'existe pas encore dans le dépôt) protège l'endpoint `POST /v1/ingest/<entity>` — défaut **OFF** en production, **ON** en dev/CI/seed (les tests et `scripts/seed_demo_data.py` continuent de fonctionner sans changement).
3. **`scripts/bulk_ingest.py` → réservé au bootstrap opérateur.** Rôle inchangé — chargement initial massif, hors chemin quotidien gouverné.
4. **`scripts/ingest_file.py` → outil manuel/dev**, désormais positionné **derrière le service `apply`** (plutôt que d'appeler directement l'endpoint HTTP via `TestClient`) une fois la PR-1 livrée.

**Chemin unique visé** : Dropbox → inbox → pipeline gouverné piloté par `feed_contracts` (migration 073, ADR-037).

### 3. La séquence entrante d'un run quotidien

1. **Landing** (existe déjà — le mécanisme de dépôt de fichier).
2. **Détection de run** : scan de l'inbox croisé avec `get_active_contract()` (`interfaces/contracts.py`, ADR-037 §2) pour chaque `feed_key` attendu.
3. **Checksum `sha256`** du fichier déposé.
4. **Garde de fenêtre d'arrivée** : `cadence` + `arrival_window_minutes` du contrat actif (ADR-037 §1) — un flux non arrivé dans sa fenêtre est traité comme manquant.
5. **Gardes de volume + ratio-suppression** : `volume_guard_min_rows`/`volume_guard_max_pct_delta` du contrat (ADR-037 §6, mode de mort n°1 — extraction partielle silencieuse) **plus** le garde 20 % relogé de `staging/diff.py` (décision 2.1).
6. **DQ AVANT canonique** — inversion délibérée du comportement actuel décrit en Contexte.
7. **Décision gouvernée** (ADR-037 §0, option a) : auto-approuve ssi DQ verte ET toutes les gardes de flux vertes ; une garde rouge sur un flux `blocking` bloque l'auto-approbation du run entier et déclenche l'escalade webhook L3 (`notifications/l3_webhook.py`, déjà en place, `OOTILS_WEBHOOK_L3_URL`, best-effort, sans secret dans le payload) ; une garde rouge sur un flux `advisory` dégrade la confiance du run sans le bloquer.
8. **Chargement full-reload** via le service `apply` (décision 2.2), dans l'ordre de dépendances (`depends_on` du contrat, ADR-037 §2), avec émission d'`events`.
9. **Propagation** → **détection des pénuries** → **recommandations**.
10. **Table `daily_runs`** (migration **078** — prochain numéro libre après `077_drop_redundant_edges_index.sql`, FK vers `feed_contracts`) **+ UN event `daily_run_completed`** par run (granularité par run, même convention qu'ADR-027/ADR-039's `purge_executed` — un event de confirmation par exécution, pas par ligne traitée ; `event_type` devra être ajouté au `CHECK` de la table `events` par une migration dédiée, même motif que la migration 076 pour `purge_executed`).

### 4. Le sortant et la réconciliation

- Dépôt dans `dropbox:ootils-outbox`.
- Colonne `exported_at` sur `recommendations` (migration 078, la même migration que `daily_runs`) — une reco stampée n'est **jamais** ré-exportée (idempotent par construction : un `WHERE exported_at IS NULL` suffit).
- Un event `export_executed` par run (même granularité que `daily_run_completed`).
- **Réconciliation HEURISTIQUE uniquement — décision pilote 2026-07-13** : il n'existe pas de champ `ootils_ref` échoable dans le PO de l'ERP du pilote. Un watcher déterministe **baseline-only** (cohérent avec la doctrine ADR-030 : un résultat observé n'est jamais un fork) rapproche un PO entrant avec une reco `APPROVED` déjà exportée sur `(item, location, supplier, qty ± tolérance, date ± fenêtre)`, et stampe `fulfilled_at` + `fulfilled_erp_id`. C'est une **observation**, jamais une écriture appliquée automatiquement — cohérent avec `HUMAN_ONLY_TARGETS` de la machine à états de recommandation (`engine/recommendation/state_machine.py`, cité par `notifications/l3_webhook.py`). **La marge d'erreur du matching est assumée et signalée** dans le compte-rendu quotidien : le taux de matchs ambigus (plusieurs recos candidates pour un même PO entrant, ou l'inverse) est un chiffre publié, jamais caché. Ce rapprochement alimente l'évaluateur de preuve ADR-030 (`recommendation_outcomes`).

### 5. Le compte-rendu quotidien

- Une ligne `daily_runs` par run.
- `GET /v1/daily-runs` (surface REST, absorbe ce qui était prévu en ADR-037 PR4).
- `daily_report_<date>.txt` déposé dans `ootils-outbox`.
- Un panneau `/ui` (le chemin humain déjà posé par [ADR-036](ADR-036-human-window.md), kill switch `OOTILS_UI_ENABLED`).
- Email : reporté en V2.
- 🎯 **Q6 ouverte** : quel canal le pilote préfère-t-il réellement comme destination principale du compte-rendu (fichier, `/ui`, autre) ? Non tranché.

### 6. Refus explicites en V1

Delta/CDC ; SFTP ou API/webhooks ERP directs ; write-back ERP (jamais, sous aucune forme — c'est la ligne rouge L4 de la doctrine) ; multi-source par entité ; intraday ; CSV/XLSX (règle TSV-only) ; réanimation du pipeline `staging` servi en production ; désagrégation automatique de prévisions ; push automatique d'annulations (`CANCEL` reste L3, humain-only, cf. ADR-026/l3_webhook.py).

---

## Lentille North Star

- **Forkable** : le run quotidien gouverné opère sur baseline par nature (une donnée ERP observée n'est pas un fork), mais rien n'empêche un scénario what-if de consommer les mêmes nœuds une fois chargés — cohérent avec l'axe ADR-030 (un outcome/reconciliation est baseline-only par construction, un fork reste simulé).
- **Déterministe** : la décision gouvernée (option a, ADR-037) est une fonction pure de DQ + gardes de flux — aucun LLM dans le chemin d'approbation.
- **Streamable** : `daily_run_completed` et `export_executed` sont des events typés qui alimentent `/v1/stream` (ADR-027) — un agent peut s'abonner au cycle de vie du run sans polling.
- **Explicable** : chaque garde rouge nomme la raison précise (fenêtre, volume, ratio, DQ) — jamais un blocage silencieux.
- **Auditable** : `daily_runs`, `events`, l'audit de rejet relogé (`rejected_by`/`rejection_reason`) et le taux de matchs ambigus de la réconciliation forment ensemble une trace complète du cycle entrant→sortant→confirmation.
- **Décision Ladder** : la réconciliation reste une observation (jamais L3+/appliquée automatiquement) ; `CANCEL` reste L3 humain-only ; le refus du write-back ERP est la garde L4 absolue de cette doctrine.
- **Anti-pattern explicitement refusé** : un chemin d'ingestion qui bypasse la gouvernance (bulk_ingest en production quotidienne, staging jamais fermé, ingest direct non-fenced) — c'est précisément ce que cette doctrine ferme.

---

## Plan de PRs (ordre décidé pilote 2026-07-13 : la valeur d'abord)

| PR | Contenu | Absorbe |
|---|---|---|
| **PR-0** | Ce document + doctrine pilote + patches de supersession. | — |
| **PR-2** | Migration 078 (`daily_runs` + `exported_at`) + gardes runtime (fenêtre, volume, ratio). | INT-1 PR2 |
| **PR-3** | Décision gouvernée via le service `apply`. | INT-1 PR3 |
| **PR-4** | Wiring inbound complet + compte-rendu + `GET /v1/daily-runs`. | — |
| **PR-1** | Fencer l'ingest direct (`engine/ingest/apply.py`, `OOTILS_DIRECT_INGEST_ENABLED`) + enterrer `staging` (`/v1/staging/*` retiré). **Après** la valeur, pas avant. | — |
| **PR-5** | Sortant idempotent (`exported_at`, `export_executed`) + réconciliation heuristique → `recommendation_outcomes` (ADR-030). | — |
| **PR-6+** | PARAM-1 (chantier réservé, non commencé), migration 074/ADR-038 (réservés, non écrits) — réutilise le mécanisme sortant de PR-5. | — |

---

## Questions 🎯 pilote restantes

- **Q2 — Prévisions** : source ERP ou module Pyramide interne pour le flux `forecasts` hebdomadaire ? Non tranché.
- **Q3 — Colonnes exactes des 4 TSV manquants ou à corriger.** Vérification faite dans ce PR (`docs/contracts/TSV-FILES-SPEC.md`) : `on_hand.tsv` (§2.6) et `purchase_orders.tsv` (§2.7) ont déjà un contrat colonnes complet. `customer_orders.tsv` (§2.8) a **aussi déjà** un contrat colonnes complet (avec sa limitation V1.0 documentée : pas de `customer_external_id`/`channel`/`region` — modélisé via une `customer_virtual` location). Ce qui manque réellement pour `customer_orders` n'est donc **pas** le contrat colonnes mais le **`feed_contracts` YAML** (aucun des 3 seed YAML de `config/feed-contracts/` ne couvre `customer_orders`, alors même que le pilote vient de le déclarer `blocking` — §1 ci-dessus). À l'inverse, **`work_orders.tsv` n'a AUCUN contrat colonnes** dans `TSV-FILES-SPEC.md` — il n'apparaît même pas dans sa table des matières §0 (11 entités listées, pas `work_orders`) — alors que son `feed_contracts` YAML (`open-work-orders.yaml`) existe déjà. Donc concrètement : **`customer-orders.yaml` (feed_contract) reste à écrire, et la section `work_orders.tsv` de `TSV-FILES-SPEC.md` reste à écrire** — deux manques disjoints, pas le même artefact.
- **Q4 — Cadences/fenêtres/planchers réels.** Les valeurs des 3 YAML seed (`cadence`, `arrival_window_minutes`, `volume_guard_min_rows`, `volume_guard_max_pct_delta`) sont des **placeholders réalistes explicitement marqués comme tels** dans leurs commentaires (ADR-037 §🎯 Pilote) — à recalibrer contre le volume réel du système du pilote.
- **Q6 — Destination préférée du compte-rendu.** Fichier `daily_report_<date>.txt` seul, `/ui`, ou combinaison ? Non tranché (décision 5 ci-dessus).
- **Q7 — Historiques de calibrage : disponibilité et profondeur.** L'ERP peut-il extraire les PO clôturés (avec dates de réception réelles), les OF clôturés, les consommations et les transferts réalisés — et sur quelle profondeur (12 mois ? 24 ?) ? Conditionne la richesse du calibrage PARAM-1 (décision 1, famille historiques). Non bloquant pour les PR-2/3/4.

---

## Supersessions

- **[ADR-013](ADR-013-external-interfaces.md) D4** (« Approval obligatoire, pas d'auto-approve ») — partiellement supersédée pour le cas du run quotidien gouverné, déjà notée par ADR-037 §0 et confirmée ici. Patch daté ajouté en fin de fichier ADR-013 (voir ce PR).
- **[ADR-037](ADR-037-daily-run-and-governed-ingest.md) §5** (tableau des 4 PR) — la cellule PR3 ciblait `staging/approve.py:approve_batch` comme point d'auto-approbation ; cette cible est remplacée par le service `engine/ingest/apply.py` (PR-1 du plan ci-dessus), et `staging/approve.py` lui-même est enterré par la décision 2.1. Patch daté ajouté en fin de fichier ADR-037.
- **`docs/SPEC-INTERFACES.md`** — bandeau superseded, contenu conservé (référence historique agent-facing/MCP/webhooks toujours utile comme catalogue de cibles au-delà du wedge V1).
- **`docs/WIP-inbound-interfaces-spec.md`** — bandeau superseded/archivé, contenu conservé (catalogue M01-S04 reste une référence de nommage utile pour les phases V2/V3, mais n'est plus le plan actif).
- **`docs/SPEC-INTERFACES-INBOUND-V1.md`** — bandeau « partiellement superseded » : sa thèse de transport (« API REST JSON = canal de référence aujourd'hui/futur ») est inversée par la décision 2 ci-dessus (le TSV-via-inbox devient le chemin canonique, JSON direct devient le mode fenced) ; ses sections de contrat commun (auth, validation, dry-run, idempotence, codes d'erreur §3-§8) restent des descriptions correctes du comportement de l'endpoint JSON tel qu'il continue d'exister en mode fenced. Sa prescription `transfers` obligatoire-blocking (§4.2, §8) **n'apparaît pas** dans la liste des flux V1 de la décision 1 ci-dessus — écart non résolu par cet ADR, à clarifier avec le pilote plutôt que silencieusement arbitré ici.

---

## Code references

- `src/ootils_core/staging/approve.py:154` — `if status != "validated"` : le garde qui rend le pipeline staging mort en pratique (rien ne l'atteint automatiquement).
- `src/ootils_core/staging/diff.py:39-41` — `DELETION_RATIO_THRESHOLD = 0.20`, le garde ratio-suppression à reloger.
- `src/ootils_core/staging/reject.py:42-43,62-63,90-116` — `rejected_by`/`rejection_reason`, l'audit de rejet à reloger.
- `src/ootils_core/api/app.py:38,426` — `staging.router` monté, à démonter en PR-1.
- `src/ootils_core/api/routers/ingest.py:246-254` — `_trigger_dq`, « never raises », appelé après l'écriture canonique (ex. lignes 522-547 pour `items`).
- `scripts/bulk_ingest.py:1-24` — docstring, rôle bootstrap confirmé.
- `scripts/ingest_file.py:1-31` — docstring, appel `TestClient` in-process confirmé.
- `src/ootils_core/db/migrations/073_feed_contracts.sql`, `src/ootils_core/interfaces/contracts.py` — le registre `feed_contracts` (ADR-037).
- `config/feed-contracts/on-hand.yaml`, `open-purchase-orders.yaml`, `open-work-orders.yaml` — les 3 contrats seed existants (aucun pour `customer_orders`, voir Q3).
- `docs/contracts/TSV-FILES-SPEC.md` §0 (table des matières), §2.6, §2.7, §2.8 — confirment la présence des contrats colonnes `on_hand`/`purchase_orders`/`customer_orders`, et l'absence de `work_orders`.
- `src/ootils_core/notifications/l3_webhook.py` — le mécanisme d'escalade L3 déjà en place, réutilisé par la décision 3.
- `src/ootils_core/db/migrations/077_drop_redundant_edges_index.sql` — dernière migration numérotée ; `078` est le prochain numéro libre pour `daily_runs`.
- `docs/ADR-027-streamchanges-sse.md` — le pattern d'event un-par-run réutilisé pour `daily_run_completed`/`export_executed`.
- `docs/ADR-030-proof-machine.md` — l'évaluateur d'outcome que la réconciliation heuristique alimente ; le principe baseline-only qu'elle respecte.
- `docs/ADR-036-human-window.md` — le panneau `/ui` du compte-rendu quotidien.
- `docs/ADR-039-scenario-archive-cleanup.md` — le précédent direct de « décision pilote datée, seuils 🎯 explicitement marqués placeholders ».
