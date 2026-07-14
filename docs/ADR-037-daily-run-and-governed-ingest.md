# ADR-037 — Daily run & ingestion gouvernée : contrats de flux versionnés (INT-1)

**Statut :** Accepté — chantier **INT-1** (issue #449 + son commentaire plan). **PR1** (registre `feed_contracts`) implémentée dans ce worktree (`feat/int1-feed-contracts`, base `origin/main` = `f3acfa5`), non encore mergée sur `main`. **PR2/PR3/PR4** (table `daily_runs`, évaluation des gardes au runtime, décision gouvernée auto-approve/escalade, surface REST) sont planifiées ci-dessous (§5) mais **non implémentées** — ce chantier n'a encore aucun effet sur le comportement d'ingestion réel.
**Date :** 2026-07-11
**Auteurs :** ootils-core team
**Contexte mesuré :** issue #449 et son commentaire plan (modèle de contrat, doctrine d'approbation) ; `docs/ADR-013-external-interfaces.md` D4 (« Approval obligatoire, pas d'auto-approve ») — décision partiellement supersédée ici ; `src/ootils_core/notifications/l3_webhook.py` (le mécanisme d'escalade L3 déjà en place, réutilisé par référence pour PR3).

---

## Contexte

ADR-013 a posé, le 23 mai 2026, le contrat externe d'Ootils : formats de fichiers (D1), semantique full-reload (D3), et — en D4 — un principe unique et strict : **aucun batch n'entre jamais en base sans un clic humain explicite**, même DQ-vert. La justification de l'époque tenait en une phrase : un batch peut être DQ-vert et structurellement délétère (ex. une extraction tronquée qui soft-delete 80 % des items sans déclencher aucune règle DQ), et seul un humain regardant le `/diff` pouvait s'en apercevoir.

Cette doctrine tient pour un pilote qui pousse des fichiers ponctuellement. Elle casse à l'échelle d'un **run quotidien multi-flux** (on-hand, POs ouverts, WOs ouverts, …) : exiger un clic humain sur *chaque* flux, *chaque* jour, transforme la garantie de sécurité en friction pure — soit le pilote clique sans regarder (la garantie devient un théâtre), soit il finit par ne plus faire tourner le run quotidien du tout. Le risque qu'ADR-013 D4 visait à éliminer (une extraction partielle silencieuse) reste réel et **n'est toujours couvert par aucune garde structurée** — ADR-013 D4 ne proposait qu'un ratio de soft-delete comme garde-fou (`/diff`, seuil 20 %), pas un contrat déclaratif par flux avec plancher de volume, fenêtre d'arrivée, ni notion de criticité.

Le pilote a tranché le 2026-07-11 après cadrage : remplacer le clic systématique par une **garde objective, gouvernée, mesurée** — l'automatisation ne doit jamais dispenser de contrôle, elle doit le déplacer d'un geste manuel répétitif vers une vérification programmatique qui échoue bruyamment (fail-loudly) et escalade vers un humain exactement quand un signal concret le justifie.

## Décision

### 0. L'arbitrage qui supersède ADR-013 D4

> **Ingestion gouvernée — option (a)** : approbation automatique du run quotidien **si et seulement si** le pipeline DQ est vert **ET** toutes les gardes de flux sont vertes. Une garde rouge sur un flux **blocking** bloque l'auto-approbation du run entier et déclenche une **escalade webhook L3** vers un humain ; une garde rouge sur un flux **advisory** dégrade la confiance du run mais ne le bloque pas. La garde remplace le clic, pas le contrôle.

Ceci **supersède ADR-013 D4** ("Approval obligatoire, pas d'auto-approve") pour le cas précis d'un run quotidien de flux **couverts par un contrat `feed_contracts` actif** : l'auto-approbation devient possible, mais seulement sous condition mesurée et objective, jamais inconditionnelle. ADR-013 D4 reste la doctrine par défaut pour tout batch **hors** run quotidien gouverné (un upload ad-hoc via `POST /v1/staging/upload` sans contrat associé continue d'exiger `POST /v1/staging/batches/{id}/approve`) — ce chantier ne retire l'approbation humaine que là où une garde objective démontrée la remplace, il ne l'abolit pas globalement. La note de bas de page `docs/ADR-013-external-interfaces.md` D4 doit être marquée « Superseded (partially) by ADR-037 for daily-run governed ingest » lors d'un prochain patch chirurgical de ce fichier — non fait dans ce PR, périmètre volontairement limité (voir Code references).

### 1. Le modèle de contrat de flux, champ par champ

Un `feed_contract` déclare ce à quoi un flux source (on-hand, POs ouverts, WOs ouverts, …) **doit ressembler** avant qu'un run quotidien lui fasse confiance. Table `feed_contracts` (migration 073), mirrorée champ-pour-champ par `FeedContractSpec` (`src/ootils_core/interfaces/contracts.py`) :

| Champ | Contrainte DB | Contrainte Pydantic (YAML) | Rôle |
|---|---|---|---|
| `feed_key` | `TEXT NOT NULL`, non-blank/non-paddé | `str, min_length=1` | Identifiant stable du flux à travers toutes ses versions (ex. `on-hand`) |
| `version` | `INTEGER CHECK >= 1`, `UNIQUE(feed_key, version)` | absent du YAML — assigné par le loader | Version monotone par `feed_key`, calculée `MAX(version)+1` |
| `entity_type` | `CHECK IN (...)`, 13 valeurs | `Literal[...]`, mêmes 13 valeurs | Quelle entité canonique réelle (`ingest_batches.entity_type`) ce flux alimente |
| `source_system` | `TEXT NOT NULL`, libre | `str, min_length=1` | Nom du système source, vocabulaire ouvert |
| `format` | `CHECK IN ('tsv','csv','xlsx','json')` | `Literal[...]` | Format physique (ADR-013 D1). **Règle pilote 2026-07-11 : les contrats du déploiement pilote n'utilisent QUE `tsv`** (« pas de csv c'est un enfer ») — l'enum reste multi-formats pour d'autres déploiements, mais proposer du CSV au pilote est une erreur. |
| `key_columns` | `TEXT[] CHECK cardinality > 0` | `list[str], min_length=1` | Colonnes identifiant une ligne (clé métier) |
| `mandatory_columns` | `TEXT[] CHECK cardinality > 0` | `list[str], min_length=1` | Colonnes obligatoires non-nulles ; `key_columns` doit être un sous-ensemble |
| `load_mode` | `DEFAULT 'full'`, `CHECK IN ('full')` | `Literal["full"] = "full"` | V1 = full uniquement — voir §4 |
| `cadence` | `TEXT NOT NULL` | `str, min_length=1` | Cron texte, interprété par le runtime PR2/PR3, pas validé ici |
| `arrival_window_minutes` | `CHECK > 0` | `int, gt=0` | Tolérance de retard avant qu'un run traite le flux comme manquant |
| `owner` | `TEXT NOT NULL` | `str, min_length=1` | Humain/équipe responsable — celui que l'escalade L3 nomme |
| `criticality` | `CHECK IN ('blocking','advisory')` | `Literal[...]` | `blocking` bloque l'auto-approbation du run ; `advisory` dégrade seulement |
| `volume_guard_min_rows` | `NULL`, `CHECK >= 0` | `int \| None, ge=0` | Plancher de lignes — None-honnête, pas de défaut fabriqué |
| `volume_guard_max_pct_delta` | `NULL`, `NUMERIC(5,4) CHECK >= 0` | `Decimal \| None, ge=0, le=9.9999` | Tolérance de variation j/j — **fraction** (0.20 = 20 %), pas un entier pourcentage |
| `depends_on` | `TEXT[] NOT NULL DEFAULT '{}'` | `list[str], default=[]` | `feed_key`s amont — **pas** une FK (voir §2), validé par le loader Python |
| `active` | `BOOLEAN NOT NULL DEFAULT TRUE` | absent du YAML — assigné par le loader | Version en vigueur pour ce `feed_key`, au plus une par index partiel |
| `created_at`/`updated_at` | `TIMESTAMPTZ` | absent | `updated_at` n'est bumpé qu'au bookkeeping (flip `active`), jamais au contenu |

Trois PR1 seed contracts, livrés sous `config/feed-contracts/*.yaml` et commentés champ par champ **pour le pilote** (c'est son propre livrable — il les relit et les ajuste à son système réel) :

- `on-hand.yaml` — `entity_type: on_hand`, **blocking** (toute la chaîne pénurie/MRP part du stock physique).
- `open-purchase-orders.yaml` — `entity_type: purchase_orders`, **blocking** (un PO ouvert manquant fait recommander une commande en double).
- `open-work-orders.yaml` — `entity_type: work_orders`, **advisory** (tous les clients pilotes ne font pas de manufacturing discret suivi par OF ; un flux légitimement vide ou absent ne doit pas bloquer le run).

### 2. YAML = vérité révisable en repo, table = binding runtime + audit

`config/feed-contracts/*.yaml` est la source éditable par le pilote, sous revue git comme tout autre fichier du dépôt. `feed_contracts` (la table) est le **binding runtime + l'audit** : chargée par `scripts/load_feed_contracts.py` via `interfaces/contracts.py:load_contract_dir` → `upsert_contract`, exécuté chaque fois que les YAML changent.

Sémantique **append-only par version** (jamais d'`UPDATE` du contenu d'une version existante) : un contrat qui change de forme (nouvelle colonne obligatoire, garde de volume élargie, nouveau owner) est lui-même un fait qui mérite d'être conservé — écraser silencieusement la version N effacerait l'historique de ce qu'un run quotidien a réellement validé un jour donné. `upsert_contract` compare le contenu parsé du YAML au contenu de la version `active` courante (`_CONTENT_FIELDS`, 13 colonnes) ; identique → **no-op tracé** (loggé, rien écrit) ; tout diff → nouvelle ligne `version = MAX+1`, et la version précédente est **bookkeeping-flipée** à `active = FALSE` (jamais réécrite dans son contenu). Au plus une ligne `active = TRUE` par `feed_key`, garanti par un **index unique partiel** (`uq_feed_contracts_active_per_feed`, pas seulement un invariant applicatif) — `get_active_contract(feed_key)` est donc un simple `WHERE feed_key = $1 AND active` garanti 0-ou-1 ligne, sans `ORDER BY`/`LIMIT` de repli. Zéro ligne active = flux désactivé/retiré, **None-honnête** : le lecteur retourne `None`, jamais un repli silencieux sur la dernière version inactive.

`depends_on` n'est **pas** une vraie FK — `feed_key` seul n'est pas unique (seul `(feed_key, version)` l'est, un `feed_key` traverse plusieurs versions) — la référence est validée par `load_contract_dir` (intégrité référentielle cross-fichiers, même motif que l'invariant multi-sites non porté en DB de `location_aliases`, ADR-031).

### 3. `entity_type`/`format` en lockstep avec l'existant — vérifié, pas inventé

Le CHECK `entity_type` de la migration 073 est **délibérément identique** à `ingest_batches_entity_type_check` tel qu'il se présente après les migrations 023 → 035 → 036 (vérifié en lisant les trois fichiers avant d'écrire la liste, pas supposé) : `items`, `locations`, `suppliers`, `supplier_items`, `purchase_orders`, `customer_orders`, `forecasts`, `work_orders`, `transfers`, `on_hand`, `resources`, `planning_params`, `routings`. Les 3 contrats seed n'utilisent que `on_hand`, `purchase_orders`, `work_orders` — aucune valeur nouvelle, aucun élargissement d'enum nécessaire. `format` reprend l'univers à 4 valeurs déjà posé par `staging.uploads.file_format` (migration 033, ADR-013 D1). Les deux enums doivent rester synchronisés : élargir `ingest_batches`/`staging.uploads` d'abord, puis mirrorer ici dans une migration de suivi — jamais l'inverse.

### 4. `load_mode` — un piège fail-loudly à double ligne de défense

V1 n'admet **que** `'full'`. C'est un piège délibéré : la sémantique delta (flux change-only) est explicitement hors du périmètre de PR1 et arrivera par une migration V2 qui élargira ce CHECK. Un YAML qui déclare `load_mode: delta` aujourd'hui est rejeté **deux fois** : d'abord par Pydantic (`Literal["full"]`, en Python, avant même de toucher la DB), puis — si cette première ligne était contournée — par le CHECK SQL lui-même. Aucun repli silencieux vers `full`.

### 5. Le phasage en 4 PRs

| PR | Contenu | Statut |
|---|---|---|
| **PR1** (ce chantier) | Registre : migration 073, YAML seed × 3, `interfaces/contracts.py` (parse strict + loader idempotent + `get_active_contract`), CLI `scripts/load_feed_contracts.py`. **Aucun runtime ne lit encore le registre.** | **Implémenté** (ce PR, non mergé) |
| **PR2** | Table `daily_runs` (+ FK vers `feed_contracts`) ; évaluation runtime par flux : fenêtre d'arrivée (`cadence` + `arrival_window_minutes` vs l'horodatage réel d'upload), gardes de volume (`volume_guard_min_rows`/`volume_guard_max_pct_delta` vs le run précédent), lues via `get_active_contract()`. | Planifié, **non implémenté** |
| **PR3** | Le moteur de décision gouvernée (option a) : combine le statut DQ existant (L1-L4) du batch avec le verdict des gardes PR2 par flux ; auto-approuve (`staging/approve.py:approve_batch`) le run entier ssi tout est vert ; toute garde rouge sur un flux `blocking` bloque l'auto-approbation et **escalade** via le webhook L3 déjà en place (`notifications/l3_webhook.py`) ; une garde rouge `advisory` dégrade la confiance du run sans le bloquer. | Planifié, **non implémenté** |
| **PR4** | Surface REST : gestion des `daily_runs` (déclenchement, consultation de statut) et lecture des contrats via l'API (aujourd'hui CLI-seulement). | Planifié, **non implémenté** |

Cette découpe est une **intention documentée**, pas un contrat gravé — la frontière exacte entre PR2 et PR3 (où finit « évaluation runtime des gardes », où commence « décision d'auto-approbation ») peut se redessiner à l'implémentation.

### 6. Les modes de mort d'une interface — et où chaque garde les attrape

Un contrat de flux existe pour nommer, par avance, les façons dont un flux externe meurt silencieusement — et pour dire explicitement à quelle étape chacune est attrapée.

| Mode de mort | Exemple concret | Garde | Attrapé en |
|---|---|---|---|
| **Extraction partielle silencieuse (risque n°1)** | Un job WMS « réussit » mais n'écrit que la moitié des lignes ; aucune règle DQ structurelle ne le voit | `volume_guard_min_rows` / `volume_guard_max_pct_delta` | PR2 (runtime) — **construit en PR1 uniquement comme champ déclaratif**, pas encore évalué |
| **Flux totalement absent** | Le job cron du flux ne tourne pas ce jour-là | `cadence` + `arrival_window_minutes` (« manquant » après la fenêtre) | PR2 (runtime) |
| **Fichier malformé / mauvais schéma** | En-têtes renommés, colonnes manquantes | `mandatory_columns`, DQ L1 existant (ADR-009/013) | Déjà couvert par le pipeline DQ L1-L2 existant ; `mandatory_columns` est la déclaration côté contrat de la même exigence |
| **Lignes dupliquées / sans clé métier** | Deux lignes `(item, site)` identiques dans un export on-hand | `key_columns` | DQ existant (staging), le contrat déclare la clé attendue |
| **Mauvais type d'entité ou de format déclaré** | Un flux `purchase_orders` livré en fait au format `on_hand` | `entity_type`/`format` CHECK | Vérification contrat-vs-batch réel — **PR2/PR3, non construit** (PR1 ne fait que déclarer, pas croiser contre un batch réel) |
| **Le contrat lui-même dérive silencieusement** | Une colonne obligatoire disparaît du YAML sans que personne s'en aperçoive | Versioning append-only (§2) | **PR1 — déjà livré** : tout changement de contenu crée une nouvelle version, interrogeable (`idx_feed_contracts_feed_key_version`) |
| **Ordre amont violé** | `open-purchase-orders` traité avant un flux dont il dépendrait | `depends_on` | Intégrité référentielle validée au chargement (**PR1**) ; ordonnancement réel du run — **PR2/PR3, non construit** |
| **Sémantique delta qui se glisse avant que V2 l'ait définie** | Un contrat déclare `load_mode: delta` par erreur de copier-coller | CHECK Pydantic + CHECK SQL (§4) | **PR1 — déjà livré**, double ligne de défense |

### 7. Ce que PR1 ne fait délibérément pas

Aucune FK depuis `daily_runs` (la table n'existe pas encore). Aucun endpoint REST (`scripts/load_feed_contracts.py` est le seul point d'entrée — le runtime CRUD arrive en PR4). Aucun croisement contrat-vs-batch réel à l'ingestion. `feed_contracts` est, à la fin de ce PR, un **registre mort** au sens strict : peuplé, versionné, interrogeable en SQL, mais lu par aucun chemin de code hors du CLI qui l'écrit.

## Alternatives rejetées

- **Garder ADR-013 D4 tel quel (clic humain systématique, y compris pour un run quotidien).** Rejeté — viable pour un upload ponctuel, casse à l'échelle d'un run multi-flux quotidien (friction qui pousse soit au clic sans lecture soit à l'abandon du run).
- **Auto-approbation inconditionnelle dès que le pipeline DQ est vert, sans gardes de volume.** Rejeté — c'est exactement le trou qu'ADR-013 D4 visait à combler à l'origine : une extraction tronquée peut être DQ-structurellement verte (chaque ligne présente est valide) tout en étant un désastre volumétrique. Les gardes de volume sont la pièce qui manquait à ADR-013 D4, pas un relâchement du contrôle.
- **FK `daily_runs → feed_contracts` posée dès PR1.** Rejeté — `daily_runs` n'existe pas encore ; une FK vers une table absente n'a pas de sens, et l'anticiper introduirait un couplage non testable dans ce PR.
- **Stocker les contrats uniquement en YAML, sans table DB.** Rejeté — pas de binding runtime interrogeable en SQL, pas de garantie « au plus une version active par flux » imposée par un index (juste une convention applicative fragile), pas d'historique versionné auditable au même endroit que le reste de l'audit du dépôt.
- **Version assignée par une séquence Postgres plutôt que calculée par le loader.** Rejeté — le loader doit décider « contenu identique ⇒ no-op » **avant** de savoir s'il doit consommer un numéro de version ; une séquence gaspillerait des numéros à chaque no-op (ou obligerait un rollback explicite), quand `MAX(version)+1` calculé en Python dans la même transaction reste simple et correct.
- **`UPDATE` en place du contenu d'une version existante quand le YAML change.** Rejeté — perdrait la trace de ce qu'un run quotidien a réellement validé un jour donné ; l'historique de dérive d'un contrat est lui-même une donnée utile (voir §6, « le contrat dérive silencieusement »).
- **Autoriser `load_mode: delta` dès V1** (le pilote a un système capable d'exports incrémentaux). Rejeté — la sémantique delta touche au cœur du full-reload d'ADR-013 D3 (soft-delete implicite par absence) ; mélanger les deux sans design explicite serait une source de bug de netting silencieux. Reporté à une migration V2 dédiée.

## 🎯 Pilote

- **L'arbitrage du 2026-07-11 lui-même** (option a) est la décision-cadre ; les **seuils exacts** par flux (`volume_guard_min_rows`, `volume_guard_max_pct_delta`, `arrival_window_minutes`, `cadence`) dans les 3 YAML seed sont des **placeholders réalistes explicitement marqués comme tels** dans les commentaires — à calibrer par le pilote contre le volume réel de son système avant que PR2/PR3 ne s'y fient.
- **La frontière PR2/PR3** (où finit l'évaluation des gardes, où commence la décision d'auto-approbation/escalade) est indicative — peut se redessiner à l'implémentation sans remettre en cause la doctrine de fond (§0).
- **Le canal d'escalade L3 pour PR3** réutilise `notifications/l3_webhook.py` par intention documentée ici ; la charge utile exacte (quels champs d'un `daily_run`/flux en échec) reste à définir au moment de PR3, pas figée par ce PR.

## Conséquences

- **Positif :** un contrat de flux versionné et lisible, livrable propre pour le pilote (3 YAML commentés champ par champ, c'est littéralement son document de configuration à remplir) ; pose la fondation d'une ingestion gouvernée qui remplace un geste humain répétitif par une garde objective, **sans jamais renoncer au contrôle** — une garde rouge nomme toujours un humain (via L3), elle ne bypass jamais silencieusement. Le versioning append-only donne un audit trail de l'évolution du contrat sans coût de conception supplémentaire.
- **Négatif / dette assumée :**
  - PR1 est un **registre mort** — voir §7. Aucun comportement d'ingestion réel ne change tant que PR2/PR3 ne sont pas livrées.
  - **Aucun test n'existe encore dans ce worktree** au moment de l'écriture de cet ADR — à écrire avant merge : tests DB-free sur `FeedContractSpec`/`parse_contract_file`/`load_contract_dir` (validation Pydantic, `extra="forbid"`, `key_columns ⊆ mandatory_columns`, `depends_on` sans auto-référence, unicité `feed_key` cross-fichiers) dans `tests/`, et tests d'intégration (écrits en aveugle, sans DB locale disponible dans cet environnement) sur `upsert_contract`/`get_active_contract` contre une vraie Postgres dans `tests/integration/`.
  - `depends_on` n'est pas une vraie FK — sa cohérence dépend entièrement du loader Python, jamais de la DB ; un `UPDATE` manuel malveillant/erroné de la table pourrait casser l'invariant sans qu'aucune contrainte SQL ne s'y oppose.
  - Les gardes de volume sont des heuristiques statiques (pas de saisonnalité) — un jour légitimement plus gros ou plus petit (ex. un gros arrivage hebdomadaire de POs) peut déclencher un faux positif ; à recalibrer avec le retour terrain (voir 🎯 Pilote).
- **Reste à faire :** PR2 (`daily_runs` + évaluation runtime des gardes), PR3 (le moteur de décision gouvernée + escalade L3 effective), PR4 (surface REST) ; les tests unitaires/intégration de PR1 elle-même ; le patch chirurgical de `docs/ADR-013-external-interfaces.md` marquant D4 « superseded (partially) by ADR-037 » (non fait dans ce PR — périmètre volontairement limité à ADR-037 + `docs/INDEX.md` + `CLAUDE.md`, voir la ligne Statut).

## Code references

- `src/ootils_core/db/migrations/073_feed_contracts.sql` — la table `feed_contracts` entière (tous les CHECK, les 2 index dont l'index unique partiel `active`).
- `src/ootils_core/interfaces/contracts.py` — module entier : `FeedContractSpec`, `parse_contract_file`, `load_contract_dir`, `upsert_contract`, `get_active_contract`, `ContractError`.
- `src/ootils_core/interfaces/__init__.py` — ré-exports publics du package `interfaces`.
- `scripts/load_feed_contracts.py` — CLI (motif maison `mrp_core.guard_db`, `DATABASE_URL`, logger, codes de sortie 0/1/2 ; `--dry-run` ne touche jamais la DB).
- `config/feed-contracts/on-hand.yaml`, `open-purchase-orders.yaml`, `open-work-orders.yaml` — les 3 contrats seed, commentés pour le pilote.
- `pyproject.toml` — dépendance `PyYAML ~= 6.0` ajoutée en dépendance core (pas un extra) : le loader CLI doit tourner dans toute installation lean.
- `src/ootils_core/db/migrations/023_api_request_audit_and_ingest_idempotency.sql`, `035_ingest_batches_planning_params.sql`, `036_ingest_batches_routings.sql` — provenance vérifiée de l'enum `entity_type` que `feed_contracts` mirrore en lockstep.
- `src/ootils_core/db/migrations/033_staging_schema.sql` — provenance de l'enum `format` (`staging.uploads.file_format`, ADR-013 D1) mirroré ici.
- `src/ootils_core/notifications/l3_webhook.py` — le mécanisme d'escalade webhook L3 déjà en place dans le dépôt, réutilisé **par référence** pour PR3 (pas modifié par ce PR).
- `src/ootils_core/staging/approve.py:approve_batch` — le point d'entrée d'approbation existant que PR3 appellera pour l'auto-approbation gouvernée.
- `docs/ADR-013-external-interfaces.md` D4 — la décision partiellement supersédée par ce chantier pour le cas du run quotidien gouverné (voir §0 ; le fichier lui-même n'est pas patché dans ce PR).

---

## Amendement — 2026-07-13 (ADR-042)

[ADR-042](ADR-042-interface-doctrine.md) (décision pilote 2026-07-13, doctrine complète des interfaces) reprend et referme ce chantier INT-1, avec deux conséquences directes sur le tableau §5 ci-dessus :

- **La cible de PR3 change.** La ligne PR3 décrivait l'auto-approbation comme un appel à `staging/approve.py:approve_batch`. Ce point d'entrée est **enterré** par ADR-042 (décision 2, §2.1) — le pipeline `staging` n'a jamais été câblé jusqu'à `status='validated'` en usage réel (`staging/approve.py:154`) et n'est jamais retenu comme le chemin gouverné. PR3 (dans le plan renuméroté d'ADR-042 — voir son tableau de PRs) appellera à la place le nouveau service `engine/ingest/apply.py` (à écrire dans la PR-1 du plan ADR-042), qui devient l'unique écrivain canonique appelé à la fois par le pipeline gouverné et par l'endpoint `POST /v1/ingest/<entity>` (désormais fenced par `OOTILS_DIRECT_INGEST_ENABLED`).
- **L'ordre des PRs change.** ADR-042 réordonne la livraison « la valeur d'abord » : la table `daily_runs` + les gardes runtime (ce qui était PR2 ici) et le moteur de décision gouvernée via `apply` (ce qui était PR3 ici) sont livrés **avant** la fermeture des deux chemins concurrents (fencer l'ingest direct, enterrer `staging` — regroupés dans la PR-1 du plan ADR-042, livrée en 5ᵉ position). Le webhook L3 (`notifications/l3_webhook.py`) référencé en PR3 reste inchangé — c'est le même mécanisme, seul son appelant côté auto-approbation change de cible.

Voir ADR-042 pour la doctrine complète (flux V1, séquence entrante, sortant + réconciliation heuristique, compte-rendu, refus V1, questions 🎯 restantes).
