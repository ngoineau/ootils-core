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

---

## Amendement — 2026-07-18 (PR-4 livrée : orchestrateur + compte-rendu quotidien + `GET /v1/daily-runs` + dépôt Dropbox)

Le bloc **PR-4** du plan (§ Plan de PRs) est livré en code — scan/résolution/décision/chargement (PR-4a/4b) et compte-rendu + surface de lecture (PR-4c). Deux corrections factuelles sur ce qui précède plutôt qu'une réécriture : le fichier sortant du compte-rendu est **`daily_report_<date>.md`** (Markdown), pas `.txt` comme écrit en décision 1 et décision 5 avant cet amendement ; et le dépôt Dropbox n'est plus une cible mais un script réel.

- **Rendu déterministe, DB-free.** `render_daily_report` (`src/ootils_core/engine/reporting/daily_report.py:444`) prend `DailyRunEvaluation` + `load_outcomes` + un `shortages_summary` optionnel + un `generated_at` fourni par l'appelant (jamais d'horloge interne) et rend un Markdown byte-identique à entrées identiques — même discipline que `interfaces/guards.py`. `build_shortages_summary` (`daily_report.py:494`) est le seul point DB du module : SELECT-only, jamais de commit/rollback, top-N pénuries actives du dernier `calc_run` complété du baseline, `[]` honnête (jamais une erreur) si aucun run n'a encore tourné.
- **CLI.** `scripts/run_daily_ingest.py` appelle le rendu à chaque invocation, dry-run ou `--apply` : en dry-run le rapport part sur STDOUT uniquement (rien n'est jamais écrit dans `--outbox` sans `--apply`) ; en `--apply` il est écrit sous `--outbox/daily_report_<AAAAMMJJ>.md` (défaut `/home/debian/outbox`). `--apply` reste gardé par le kill switch `OOTILS_DAILY_RUN_ENABLED` (défaut OFF, même forme double-garde que `OOTILS_PURGE_ENABLED`/ADR-039) — la lecture (dry-run) ne l'exige pas.
- **Dépôt Dropbox.** `scripts/deposit_outbox.sh` est un pas séparé, volontairement bête : un `rclone copy --include "daily_report_*.md"` à sens unique (jamais `rclone sync`, jamais de suppression) du dossier `--outbox` local vers `dropbox:ootils-outbox` (configurable via `OOTILS_DROPBOX_REMOTE`) — aucun secret dans le script, l'identifiant Dropbox vit dans la config `rclone` propre à la machine, hors dépôt. Ce script répond directement à la décision pilote du 2026-07-17 (« tous les échanges équipe ERP via la Dropbox, le compte-rendu quotidien "au plus vite" » — voir la note mémoire `erp-canal-dropbox`), qui a fait remonter la priorité de PR-4c juste après le premier chargement réel. Cette décision **répond la question Q6** pour le canal principal (Dropbox) ; `/ui` reste un canal de consultation secondaire, inchangé par cet amendement ; la planification récurrente du dépôt reste ouverte (point suivant).
- **`GET /v1/daily-runs` livré.** `src/ootils_core/api/routers/daily_runs.py`, monté (`api/app.py:446`) — lecture seule, scope `read`, baseline-only (pas de `scenario_id` : un run gouverné évalue des interfaces ERP, pas un état de scénario — même rationale que `daily_runs`/ADR-030). Deux SELECT, jamais de recalcul : l'historique des évaluations de garde (`daily_runs`, une ligne par tentative, la plus récente en tête) et la décision gouvernée du jour, relue depuis son event `daily_run_completed` (migration 079, jamais recalculée). Kill switch dédié `OOTILS_DAILY_RUN_REPORT_ENABLED` (défaut ON — délibérément différent du kill switch d'écriture `OOTILS_DAILY_RUN_ENABLED` du CLI, défaut OFF) : lire l'historique reste possible même quand le chargement réel est coupé, par exemple en démo/staging.
- **DQ non câblée — le plafond `DEGRADED` reste honnête.** `dq_status_by_feed` reste `None` sur tout le chemin réel (`engine/ingest/daily_orchestrator.py` le passe toujours vide) : la vraie intégration DQ est le périmètre de **PR-1** (« Fencer l'ingest direct »), pas encore livrée. Conséquence assumée et documentée en code (`engine/ingest/apply.py`, section « DQ STATUS HAS NO DB WIRING YET ») : un run réel V1 ne peut jamais atteindre `AUTO_APPROVED`, il plafonne à `DEGRADED` (ou `ESCALATED` sur une vraie garde bloquante rouge) — le compte-rendu l'explique en clair (`daily_report.py`'s `_DECISION_EXPLANATIONS[DEGRADED]`), pas comme une panne mais comme un état V1 assumé.
- **Manques assumés, non couverts par cet amendement.** Aucune planification récurrente (cron/systemd timer) de `run_daily_ingest.py --apply` ni de `deposit_outbox.sh` n'existe : les deux scripts sont opérationnels à l'invocation manuelle/opérateur, la cadence de production reste un sujet de déploiement (même limite qu'ADR-039/PURGE-1 pour `purge_maintenance.py`). (Note historique corrigée au moment de PR-5a : les tests dédiés de `render_daily_report`/`GET /v1/daily-runs` ont bien été livrés avec PR-4c — `tests/test_daily_report.py` 23 cas octet-exacts + `tests/integration/test_daily_report_integration.py`.)

### Références (amendement PR-4)

- `src/ootils_core/engine/reporting/daily_report.py` — rendu + `build_shortages_summary`.
- `src/ootils_core/engine/ingest/daily_orchestrator.py` — orchestrateur scan/résolution/décision/chargement (PR-4a/4b).
- `scripts/run_daily_ingest.py` — CLI, kill switch `OOTILS_DAILY_RUN_ENABLED`.
- `scripts/deposit_outbox.sh` — dépôt Dropbox, `rclone copy` à sens unique.
- `src/ootils_core/api/routers/daily_runs.py`, `src/ootils_core/api/app.py:446` — `GET /v1/daily-runs`, kill switch `OOTILS_DAILY_RUN_REPORT_ENABLED`.
- `src/ootils_core/engine/ingest/apply.py` — section « DQ STATUS HAS NO DB WIRING YET », le plafond `DEGRADED`.
- `tests/test_daily_orchestrator.py`, `tests/integration/test_daily_orchestrator_integration.py`, `tests/integration/test_daily_runs_integration.py`, `tests/integration/test_daily_run_decision_integration.py` — couverture indirecte existante.

---

## Amendement — 2026-07-18/19 (PR-1 livrée : fence de l'ingest direct + enterrement de staging)

Le bloc **PR-1** du plan (§ Plan de PRs) est livré, mais avec un périmètre plus étroit que ce que sa description initiale (décision 2 point 2) laissait entendre : cette PR livre le **fence** de l'ingest direct et l'**enterrement du pipeline staging** — pas l'extraction du writer canonique ni l'inversion DQ-avant-canonique, désormais nommées **PR-1-bis** (voir plus bas). Une correction factuelle sur ce qui précède, plutôt qu'une réécriture : la décision 2 point 2 décrivait le kill switch comme « défaut OFF en production, ON en dev/CI/seed » — il n'existe en réalité qu'un seul défaut (rien ne le conditionne à l'environnement au runtime) ; le défaut livré est **ON partout**, et c'est un déploiement de production gouverné qui doit explicitement le positionner à `0`.

- **Le fence, mécanisme.** Les 14 endpoints `POST /v1/ingest/*` (distribution-links inclus) (`api/routers/ingest.py`) dépendent désormais de `require_direct_ingest`, pas d'un `require_scope("ingest")` nu — même vérification de scope en amont (401/403 inchangés pour tout appelant), plus un **503 par requête** quand `OOTILS_DIRECT_INGEST_ENABLED` est falsy, SAUF pour le principal `is_legacy` (`auth.py`'s `legacy_principal()` — le jeton unique `OOTILS_API_TOKEN`, jamais un token minté par agent/service). Le détail du 503 nomme le chemin visé (« direct ingest disabled — use the governed daily-run pipeline (Dropbox inbox) ») — jamais un blocage muet, cohérent avec la lentille North Star « Explicable ».
- **Pourquoi pas un démontage à la `/ui`.** [ADR-036](ADR-036-human-window.md) démonte sa route entièrement à `create_app()` quand `OOTILS_UI_ENABLED` est faux (404 propre, jamais atteinte). Ce patron a été délibérément REFUSÉ ici : l'orchestrateur du run quotidien (`interfaces/ingest_exec.py:call_api`) et `scripts/ingest_file.py` appellent CES MÊMES endpoints **in-process via `TestClient`** — démonter `ingest.router` casserait le pipeline même que le fence est censé canaliser vers lui. D'où le choix d'un garde per-request avec exemption `is_legacy` : le routeur reste toujours monté et répond toujours, la porte ne se ferme que pour un appelant non-legacy — contre-modèle documenté explicitement dans le docstring de `require_direct_ingest` (`ingest.py:74-126`).
- **Défaut ON assumé.** `OOTILS_DIRECT_INGEST_ENABLED` vaut `1` (ON) si absent (`ingest.py:_direct_ingest_enabled`) — rien ne régresse en dev/CI/seed sans configuration. La posture de production gouvernée (positionner explicitement `OOTILS_DIRECT_INGEST_ENABLED=0`) est documentée comme la cible, pas encore appliquée nulle part par défaut.
- **L'enterrement de staging.** `staging.router` n'est plus importé/inclus dans `api/app.py` — toutes les routes `/v1/staging/*` sont désormais inatteignables (bandeau de dépréciation en tête de `api/routers/staging.py`). Le module et les tables `staging.*` sont CONSERVÉS, pas supprimés (nettoyage schéma/migration hors scope de cette PR). Les deux gardes de valeur sont relogées, pas perdues : le seuil de 20 % (`staging/diff.py`'s `DELETION_RATIO_THRESHOLD`) vit maintenant dans `interfaces/guards.py:DELETION_RATIO_THRESHOLD`/`evaluate_deletion_ratio_guard` (livré dès PR-2, §3) ; l'audit de rejet (`rejected_by`/`rejection_reason`, `staging/reject.py`) n'est pas rejoué automatiquement — c'est son **patron** (identité + raison obligatoire, tracé) que suit l'audit de la décision gouvernée du pipeline daily-run (`engine/ingest/apply.py:record_daily_run_decision`, dont l'event `daily_run_completed` porte la liste des flux fautifs).
- **Couverture de test du fence — livrée dans cette même PR.** `tests/integration/test_direct_ingest_fence_integration.py` exerce les 5 cas du contrat : défaut ON + token minté `ingest` → 200 ; switch OFF + token minté → 503 (détail « governed daily-run pipeline » asserté) ; switch OFF + jeton legacy → 200 (LE cas orchestrateur) ; composition : scope manquant → 403 AVANT toute considération de fence ; `/v1/staging/upload` → 404. Côté staging, les 5 tests d'intégration purement HTTP (`test_staging_{upload,diff,approve,reject,e2e}.py`) sont SUPPRIMÉS (routes définitivement démontées, réanimation refusée §6) ; `test_staging_roundtrip.py` est CONSERVÉ (contrairement à son nom, il ne teste que `staging.parser`/`staging.loader` en import direct — modules conservés), de même que `test_staging_parser.py`/`test_staging_loader.py` ; la section « Staging L3 gate » de `test_agent_floor_integration.py` est retirée avec bannière datée.
- **Split PR-1-bis, nommé explicitement.** Ce que la description initiale de PR-1 prévoyait mais que cette PR NE livre PAS : (1) l'extraction du writer canonique multi-entités hors `api/routers/ingest.py` vers `engine/ingest/apply.py` (aujourd'hui occupé uniquement par `decide_daily_run`/`record_daily_run_decision`, PR-3 — son propre docstring, lignes 23-36, décrit encore cette extraction comme « delivered AFTER this PR », un texte que la présente PR rend caduc sans le corriger ; PR-1-bis devra le faire) ; (2) l'inversion DQ-avant-canonique (`_trigger_dq` tourne toujours APRÈS l'écriture canonique, `ingest.py:246-254`, inchangé ici) ; (3) le rewire de l'orchestrateur hors `TestClient` in-process (`interfaces/ingest_exec.py:call_api` reste un appel `TestClient`, pas un appel HTTP/service réel). Tant que PR-1-bis n'est pas livrée, le plafond `DEGRADED` documenté dans l'amendement du 2026-07-18 (PR-4) reste l'état honnête de tout run réel — `dq_status_by_feed` n'a toujours aucun câblage DB, cette PR ne change rien à ce plafond.

### Références (amendement PR-1)

- `src/ootils_core/api/routers/ingest.py:63-126` — `_direct_ingest_enabled`, `require_direct_ingest` (le 503, l'exemption `is_legacy`).
- `src/ootils_core/api/routers/ingest.py` — les 14 sites `Depends(require_direct_ingest)`, un par endpoint `POST /v1/ingest/*`.
- `src/ootils_core/api/auth.py:140-154,219-232` — `Principal.is_legacy`, `legacy_principal()`.
- `src/ootils_core/interfaces/ingest_exec.py:786-801` — `call_api`, l'appel `TestClient` in-process réutilisé par l'orchestrateur et par `scripts/ingest_file.py`.
- `src/ootils_core/api/app.py:426-430` — `staging.router` non importé/non inclus.
- `src/ootils_core/api/routers/staging.py:1-27`, `src/ootils_core/staging/reject.py:1-7` — bandeaux de dépréciation, le patron d'audit relogé.
- `src/ootils_core/interfaces/guards.py:77-81,256-292` — `DELETION_RATIO_THRESHOLD` relogé, `evaluate_deletion_ratio_guard`.
- `src/ootils_core/engine/ingest/apply.py:23-36` — docstring désormais périmé sur le calendrier de l'extraction canonique (à corriger par PR-1-bis).
- `scripts/bulk_ingest.py:7-16` — confirme son statut hors fence (aucun appel HTTP, donc aucune exposition à `OOTILS_DIRECT_INGEST_ENABLED`).
- `tests/integration/test_staging_{upload,diff,approve,reject,e2e}.py` — supprimés (HTTP-only, routes démontées) ; `test_staging_roundtrip.py` conservé (module-direct) ; `test_agent_floor_integration.py` adapté (section staging retirée).

---

## Amendement — 2026-07-19 (PR-5a livrée : export sortant idempotent)

Le bloc **PR-5** du plan (§ Plan de PRs) est livré **en partie** — le périmètre réellement couvert justifie un split explicite en **PR-5a** (livrée, ce PR) et **PR-5b** (nommée, non livrée, portée ci-dessous). PR-5a couvre la moitié « export » de la décision 4 (§ Décisions) : un export idempotent, jamais deux fois la même recommandation, dans les trois fichiers sortants décrits en décision 1 (§"Sortants"). PR-5b — la réconciliation heuristique PO entrante ↔ reco exportée, qui alimente `recommendation_outcomes` (ADR-030) — **reste à faire**, avec un manque structurel identifié pendant PR-5a (voir plus bas).

- **Un fichier TSV par famille, colonnes disjointes.** `engine/reporting/outbound_export.py` route chaque recommandation `status IN ('APPROVED','APPLIED')` vers exactement une des trois familles par son `action` — `po_drafts_<AAAAMMJJ>.tsv` (`ORDER_NOW`/`ORDER_RUSH`/`EXPEDITE`), `reschedule_messages_<AAAAMMJJ>.tsv` (`RESCHEDULE_IN`/`RESCHEDULE_OUT`/`CANCEL`, ADR-026), `transfers_<AAAAMMJJ>.tsv` (`TRANSFER`, ADR-028) — jamais un fichier commun aux trois. Une action qui ne correspond à aucune des trois familles fait échouer tout le run (`UnroutableExportActionError`) plutôt que de laisser une ligne silencieusement jamais exportée — cohérent avec la doctrine « fail-loudly » de CONTRIBUTING.md. Une famille sans ligne éligible ne produit aucun fichier (jamais un header-only). Specs complètes, colonnes, exemples, lisibles équipe ERP : `docs/contracts/po_drafts/format-po-drafts-tsv.md`, `docs/contracts/reschedule_messages/format-reschedule-messages-tsv.md`, `docs/contracts/transfers_out/format-transfers-out-tsv.md`.
- **`exported_at` EST l'idempotence.** La colonne schema-only posée en migration 078 (§4 ci-dessus) est désormais réellement stampée : `execute_export` écrit les fichiers, PUIS stampe `recommendations.exported_at = now()` pour exactement les lignes écrites, PUIS émet l'event — même connexion, même transaction (write → stamp → emit, ordre délibéré, voir le docstring du module pour le raisonnement de sécurité en cas de crash). Le prochain run relit `WHERE exported_at IS NULL` : une recommandation déjà exportée ne réapparaît jamais dans un fichier ultérieur.
- **Un event `export_executed` par run.** Migration 085 (widening du CHECK `events.event_type`, suit le patron des migrations 076/079/084) + `engine/events/emit.py` (contrat de colonnes documenté) : granularité RUN (ADR-027), jamais par ligne — `new_quantity` = nombre de recommandations stampées, `new_text` = liste des noms de fichiers écrits. Pas d'event pour un run réellement vide (zéro ligne en attente) — même posture que `emit_recommendation_created_for_run`.
- **Kill switch dédié, défaut OFF.** `OOTILS_OUTBOUND_EXPORT_ENABLED` (`scripts/run_daily_ingest.py:_outbound_export_enabled`) gate uniquement l'écriture réelle de la phase EXPORT (3ᵉ phase du CLI, après le scan/chargement et le compte-rendu quotidien) — un `--apply` sans ce switch charge et rend compte normalement mais n'écrit **aucun** fichier sortant (juste un avertissement loggé), même double-garde que `OOTILS_DAILY_RUN_ENABLED`/`OOTILS_DAILY_RUN_REPORT_ENABLED`. En dry-run (pas de `--apply`), la phase EXPORT est toujours une prévisualisation STDOUT-only, indépendamment du switch.
- **`recommendation_id` = référence humaine, jamais échoable.** Conformément à la décision pilote du 2026-07-13 (§4 ci-dessus, « il n'existe pas de champ `ootils_ref` échoable ») : les trois fichiers sortants portent `recommendation_id` comme trace d'audit interne (retrouver la ligne dans `/ui`/`/v1/outcomes`), **jamais** comme un champ à ressaisir dans un PO ERP. C'est un choix délibéré et documenté (module docstring d'`outbound_export.py`, et §5 de chaque spec de fichier) — la conséquence directe est que PR-5b devra rapprocher une PO ERP entrante avec une reco exportée sur des attributs métier (item, fournisseur, quantité, date), jamais par identifiant.
- **`scripts/export_approved_pos.py` déprécié.** L'ancien script manuel de génération de PO drafts (consolidation multi-lignes par fournisseur, pas d'idempotence) porte désormais un bandeau `DEPRECATED — REFERENCE ONLY` pointant vers `outbound_export` — conservé uniquement comme référence historique pour l'idée de consolidation par fournisseur (non reprise en V1).

### PR-5b — nommée, non livrée : gap-location identifié

**PR-5b (réconciliation heuristique PO entrante ↔ reco exportée → `recommendation_outcomes`)** reste un chantier **non commencé** — aucun code de rapprochement (`fulfilled_at`/`fulfilled_erp_id` ou équivalent, décision 4 §"Réconciliation HEURISTIQUE") n'existe dans ce dépôt à ce jour. PR-5a a mis en évidence, en construisant le renderer, un manque structurel qui **contraint** la conception de PR-5b :

- **`recommendations` ne porte aucune colonne de site générique.** Vérifié sur les migrations 039 (création) et 061 (extension reschedule) : ni `location_id` ni `location_external_id`. Seule l'action `TRANSFER` porte un couple de sites, via `source_location_id`/`dest_location_id` (migration 066) — dédiés à cette seule famille. Pourtant la table source, `shortages`, **connaît** le site (`shortages.location_id`, migration 005, ADR-021's modèle per-site) : cette information est perdue entre la détection de pénurie et l'écriture de la recommandation, elle n'a jamais été propagée par les watchers qui écrivent dans `recommendations` (`scripts/agent_shortage_watcher.py` et consorts n'écrivent que `item_id`/`item_external_id`).
- **Conséquence pour `po_drafts`/`reschedule_messages`** : l'heuristique de rapprochement décrite en décision 4 (« item, site, fournisseur, quantité ± tolérance, date ± fenêtre ») ne peut PAS utiliser le site pour ces deux familles — seule `transfers_<date>.tsv` porte un site exploitable (source ET destination). PR-5b devra soit (a) composer sans le site pour ces deux familles (rapprochement item+fournisseur+quantité+date uniquement, taux d'ambiguïté probablement plus élevé — à mesurer et publier, comme la décision 4 l'exige déjà), soit (b) ajouter une colonne de site à `recommendations` en amont — un changement de schéma non trivial (il faudrait le faire remonter depuis `shortages` dans chaque watcher producteur), explicitement hors scope de PR-5a et non entamé.
- Ce gap est documenté dans les specs de fichiers elles-mêmes (`po_drafts`/`reschedule_messages`, §7/§6 « Limitations connues V1.0 ») pour que l'équipe ERP en soit informée sans attendre PR-5b.

### Manques assumés, non couverts par cette PR

- **Couverture de test livrée dans cette même PR** : `tests/test_outbound_export.py` (rendu déterministe octet-exact des 3 familles, familles vides sans fichier, `UnroutableExportActionError`, règles d'or du format) et `tests/integration/test_outbound_export_integration.py` (seuls APPROVED/APPLIED non exportés sortent — tous les autres statuts seedés et vérifiés exclus ; `exported_at` stampé pour exactement les ids écrits ; re-run = zéro fichier/stamp/event ; dry-run zéro écriture ; UN `export_executed` par run ; idempotence triple de la migration 085).
- **Aucune planification récurrente** de la phase EXPORT au-delà de l'invocation manuelle/opérateur de `scripts/run_daily_ingest.py --apply` — même limite qu'ADR-039/PURGE-1 et que l'amendement PR-4 pour `deposit_outbox.sh`.
- **PR-5b non entamée** (voir ci-dessus) : pas de réconciliation, pas d'alimentation de `recommendation_outcomes` par cette voie à ce jour.

### Références (amendement PR-5a)

- `src/ootils_core/engine/reporting/outbound_export.py` — `load_pending_export_rows`/`render_outbound_export`/`execute_export`, le module complet.
- `src/ootils_core/db/migrations/085_export_executed_event.sql` — widening du CHECK `events.event_type`.
- `src/ootils_core/db/migrations/078_daily_runs.sql:187-205` — `recommendations.exported_at` + `ix_reco_pending_export` (schema posé en PR-2, stampé pour de vrai en PR-5a).
- `src/ootils_core/engine/events/emit.py:88-114` — contrat de colonnes `export_executed`.
- `scripts/run_daily_ingest.py:48-63,129-137,232-269` — phase EXPORT, `_outbound_export_enabled`, `_run_outbound_export`.
- `scripts/export_approved_pos.py:1-27` — bandeau de dépréciation.
- `src/ootils_core/db/migrations/039_agent_recommendations.sql`, `061_reschedule_and_fpo.sql`, `066_transfer_recommendations.sql` — absence de colonne site générique sur `recommendations` (039/061) vs. `source_location_id`/`dest_location_id` réservés à `TRANSFER` (066).
- `src/ootils_core/db/migrations/005_m4_shortages.sql:9` — `shortages.location_id`, le site connu en amont mais non propagé jusqu'à `recommendations`.
- `docs/contracts/po_drafts/format-po-drafts-tsv.md`, `docs/contracts/reschedule_messages/format-reschedule-messages-tsv.md`, `docs/contracts/transfers_out/format-transfers-out-tsv.md` — les 3 specs de fichiers sortants, livrées avec ce PR.
