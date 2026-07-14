# SPEC — Interfaces Inbound V1

> **⚠️ PARTIELLEMENT SUPERSEDED — 2026-07-13.** [`ADR-042`](ADR-042-interface-doctrine.md) (doctrine des interfaces, décision pilote 2026-07-13) **inverse la thèse de transport** posée au §1/§9 ci-dessous : ce document affirmait « API REST JSON = canal de référence aujourd'hui/futur » ; la nouvelle doctrine fait du **dépôt de fichier TSV via inbox** le chemin canonique du run quotidien, et relègue l'ingestion JSON directe à un mode **fenced** (dev/bootstrap/outil manuel derrière un service `apply`, kill switch par défaut OFF en production). Ce qui reste valide : le contrat commun décrit §3-§8 (authentification, validation all-or-nothing, `dry_run`, idempotence, tailles, codes d'erreur) reste une description correcte du comportement de l'endpoint JSON tel qu'il continue d'exister en mode fenced. **Écart non résolu par ADR-042, à clarifier avec le pilote** : ce document impose `transfers` comme flux obligatoire/bloquant pour un « pilote multi-site réseau » (§4.2, §8) — ADR-042 §1 ne liste pas `transfers` parmi les flux V1 (`on_hand`, `purchase_orders`, `work_orders`, `customer_orders`, `forecasts`, référentiel). Ce n'est pas tranché silencieusement ici ; voir ADR-042 §8 (questions 🎯 restantes) pour le suivi. Le fichier `SPEC-DATA-INPUT-CANONIQUE-V1.md` cité en référence d'autorité (§1, §6) n'existe pas dans le dépôt — référence pendante déjà présente avant ce patch, non créée par ce PR. Contenu conservé tel quel ci-dessous, non réécrit.

Date: 2026-04-19  
Statut: **RÉFÉRENCE OPÉRATIONNELLE**  
Périmètre: interfaces d'entrée réellement supportées par le runtime Ootils validé sur VM 201  
Décision pilote: **pilote confirmé multi-site réseau**

## 1. Décision COO

Le V1 inbound d'Ootils est défini par:
- un **contrat data canonique unique** (`SPEC-DATA-INPUT-CANONIQUE-V1.md`)
- un **transport runtime actuel principal** = API REST JSON

Conséquence:
- les fichiers et colonnes sont la vérité métier,
- les endpoints ne sont qu'un mode d'entrée,
- aucun canal alternatif ne peut redéfinir le contrat.

## 2. Ce qui est réellement supporté aujourd'hui

### 2.1 Support runtime actuel
Le runtime actuel supporte réellement:
- authentification Bearer token
- endpoints `POST /v1/ingest/*`
- payloads JSON uniquement
- validation complète avant écriture
- `dry_run` sur tous les endpoints d'ingest
- idempotence via header `Idempotency-Key`
- limite de taille `10 MB` par requête ingest

### 2.2 Non supporté nativement aujourd'hui
Le runtime actuel **ne supporte pas nativement** comme interface primaire:
- upload CSV direct via API
- upload Excel direct
- shared directory branché directement au moteur
- SFTP consommé directement par l'API

Ces modes peuvent exister plus tard comme **transport périphérique**, mais ils devront produire le même contrat JSON canonique avant ingestion.

## 3. Contrat commun à tous les endpoints inbound

### 3.1 Authentification
- schéma: `Authorization: Bearer <token>`
- source token: `OOTILS_API_TOKEN`
- comportement: fail-closed si token absent ou invalide
- erreur: `401`

### 3.2 Format de payload
- `Content-Type: application/json`
- aucune promesse V1 sur CSV upload direct
- aucun contrat primaire en Excel

### 3.3 Validation
Règle commune à tous les endpoints d'ingest:
- toutes les lignes sont validées avant écriture
- validation structurelle + référentielle
- si une seule ligne est invalide → rejet du lot
- dans ce cas, rien n'est persisté
- erreur retournée: `422`

### 3.4 Dry run
Tous les endpoints d'ingest acceptent:
- `dry_run: true|false`

Effet:
- validation complète
- aucune écriture DB si `dry_run=true`
- retour `status="dry_run"`

### 3.5 Idempotence
Le runtime supporte:
- header optionnel `Idempotency-Key`

Règles:
- vide = ignoré
- longueur max = `128`
- même clé + même payload = replay possible
- même clé + payload différent = conflit `409`
- clé réservée par une requête en cours = conflit `409`

### 3.6 Taille max
- `/v1/ingest/*` limité à `10 MB` par requête
- au-delà: `413 payload_too_large`

### 3.7 Réponse type
Réponse standard d'ingest:
- `status`
- `summary.total`
- `summary.inserted`
- `summary.updated`
- `summary.errors`
- `results[]`
- `batch_id` (quand applicable)
- `dq_status` (quand applicable)

## 4. Endpoints inbound V1

## 4.1 Référentiels maîtres

### `POST /v1/ingest/items`
- body key: `items`
- source logique: `items.csv`
- rôle: référentiel articles

### `POST /v1/ingest/locations`
- body key: `locations`
- source logique: `locations.csv`
- rôle: référentiel sites

### `POST /v1/ingest/suppliers`
- body key: `suppliers`
- source logique: `suppliers.csv`
- rôle: référentiel fournisseurs

### `POST /v1/ingest/supplier-items`
- body key: `supplier_items`
- source logique: `supplier_items.csv`
- rôle: contrat article-fournisseur

## 4.2 Stocks et supply

### `POST /v1/ingest/on-hand`
- body key: `on_hand`
- source logique: `on_hand.csv`
- rôle: snapshot stock

### `POST /v1/ingest/purchase-orders`
- body key: `purchase_orders`
- source logique: `purchase_orders.csv`
- rôle: supply entrante fournisseur

### `POST /v1/ingest/transfers`
- body key: `transfers`
- source logique: `transfers.csv`
- rôle: supply inter-site
- décision pilote: **obligatoire dans ce pilote multi-site réseau**

## 4.3 Demande

### `POST /v1/ingest/customer-orders`
- body key: `customer_orders`
- source logique: `customer_orders.csv`
- rôle: demande ferme

### `POST /v1/ingest/forecast-demand`
- body key: `forecasts`
- source logique: `forecasts.csv`
- rôle: demande prévisionnelle

## 4.4 Nomenclatures

### `POST /v1/ingest/bom`
- body key: `components`
- sources logiques: `bom_header.csv` + `bom_components.csv`
- rôle: nomenclature active d'un parent donné

### Règle spécifique BOM
Le contrat canonique garde **deux fichiers** côté échange, mais l'API runtime attend un payload JSON assemblé par BOM:
- `parent_external_id`
- `bom_version`
- `effective_from`
- `components[]`

Donc, côté intégration:
- on groupe `bom_components.csv` par `parent_external_id + bom_version`
- on enrichit avec `bom_header.csv`
- on envoie un `POST /v1/ingest/bom` par BOM logique

## 5. Ordre canonique d'ingestion

Ordre à respecter pour éviter les erreurs référentielles:
1. `items.csv`
2. `locations.csv`
3. `suppliers.csv`
4. `supplier_items.csv`
5. `on_hand.csv`
6. `purchase_orders.csv`
7. `transfers.csv`
8. `customer_orders.csv`
9. `forecasts.csv`
10. `bom_header.csv`
11. `bom_components.csv`

## 6. Socle minimal de colonnes obligatoires côté interface

L'API runtime tolère parfois des defaults.  
Le canonique V1, lui, impose un socle plus strict.

Référence d'autorité:
- `SPEC-DATA-INPUT-CANONIQUE-V1.md`

Règle:
- un intégrateur ne doit pas se contenter du minimum OpenAPI si cela affaiblit le contrat métier
- le contrat canonique prime sur la tolérance technique du runtime

## 7. Erreurs à considérer comme normales et utiles

### `401`
- token absent
- token invalide

### `409`
- conflit d'idempotence
- clé déjà utilisée avec un autre payload
- clé réservée par une requête encore en cours

### `413`
- payload ingest > `10 MB`

### `422`
- erreur structurelle
- référence manquante
- lot rejeté intégralement

## 8. Ce qui est interdit en V1

- écrire directement en base
- contourner les endpoints d'ingest pour “aller plus vite”
- inventer un format spécifique par client
- utiliser Excel comme contrat primaire
- envoyer un payload mixte “tout-en-un” non canonicalisé
- traiter `transfers` comme facultatif sur un pilote revendiqué multi-site réseau

## 9. Position sur les transports futurs

### API REST JSON
- **canal de référence aujourd'hui**
- seul canal effectivement prouvé en runtime

### Shared directory
- acceptable plus tard comme **landing zone contrôlée**
- ne doit jamais devenir une lecture directe non validée par le moteur

### SFTP / batch
- acceptable plus tard comme canal de dépôt
- doit convertir vers le même contrat canonique avant POST API ou ingestion orchestrée

## 10. Recommandation COO

La suite logique est:
1. garder `SPEC-DATA-INPUT-CANONIQUE-V1.md` comme vérité métier,
2. garder `SPEC-INTERFACES-INBOUND-V1.md` comme vérité transport/runtime,
3. ne plus rouvrir le débat sur les colonnes au moment du choix du canal,
4. concevoir ensuite le mode d'échange opérationnel autour de ces deux specs figées.
