# Manuel utilisateur Ootils — DRAFT

> **Version** : 0.2 (draft)
> **Date** : 2026-05-31
> **Public** : directeurs supply chain, planificateurs lead, responsables IT impliqués dans le pilote.
> **Ce que ce manuel n'est PAS** : une documentation développeur. Pour la référence API complète, voir `docs/SPEC-INTERFACES.md`. En environnement contrôlé, l'OpenAPI et Swagger peuvent être réactivés explicitement, mais ils ne doivent pas être supposés exposés par défaut.
> **Note d'intégrité** : ce document ne vaut pas engagement produit complet. Il sert de support pilote et doit être relu contre le runtime effectivement déployé.

### Changelog v0.2 (2026-05-31)

- Bump version et date.
- Correction du compte de migrations (21 → 46) dans §3.3.
- Nouvelle section §6 **Flotte d'agents watchers (livré)** : 5 agents gouvernés, état DRAFT, primitive `governed_run`, smoke-test CI.
- Nouvelle section §8 **Module de demande Pyramide — en conception** : modèle décidé (ADR-019), clairement marqué comme cible non encore livrée.
- Mise à jour §1 (description produit) pour refléter les agents livrés.
- Mise à jour §5.1 (tableau des retours) : recommandations d'agents ajoutées comme livrées.
- Mise à jour §5.6 (machine à états des recommandations) pour coller à l'implémentation réelle.
- Mise à jour §9.4 (limites connues) : ajout du bug prévision et du module Pyramide non livré ; mise à jour de l'échelle réelle (36 000+ items en base pilote).
- Ajout §9.2 FAQ : question sur la prévision de demande.

---

## Sommaire

1. [Ce que fait Ootils](#1-ce-que-fait-ootils)
2. [Pré-requis avant de démarrer](#2-pré-requis-avant-de-démarrer)
3. [Étape 1 — Paramétrer et installer](#3-étape-1--paramétrer-et-installer)
4. [Étape 2 — Envoyer vos données réelles](#4-étape-2--envoyer-vos-données-réelles)
5. [Étape 3 — Lire les retours du moteur](#5-étape-3--lire-les-retours-du-moteur)
6. [Flotte d'agents watchers (livré)](#6-flotte-dagents-watchers-livré)
7. [Cadence de travail recommandée](#7-cadence-de-travail-recommandée)
8. [Module de demande Pyramide — en conception](#8-module-de-demande-pyramide--en-conception)
9. [Annexes](#9-annexes)

---

## 1. Ce que fait Ootils

Ootils est un **substrat opérationnel supply chain piloté par des agents**. Il prend en entrée votre état courant (articles, stocks, commandes, prévisions, nomenclatures, fournisseurs) et produit :

- Une **projection de stock** à horizon configurable (jour/semaine/mois).
- La **détection des ruptures** à venir, avec date et magnitude.
- Une **explication causale** de chaque rupture — quelle est la demande qui n'est pas couverte, par quel flux entrant, pourquoi ce flux arrive trop tard ou trop peu.
- Des **recommandations MRP** (suggestions d'approvisionnement et ordres planifiés, y compris lots, délais, time fences, valorisation).
- Des **recommandations d'agents automatisés** (shortage watcher, material watcher, lot policy watcher, DQ watcher, E&O watcher) — soumises à validation humaine avant tout effet (voir §6).
- Des **analyses de scénario** sur le périmètre activé : créez une branche de simulation, injectez un événement, comparez au baseline.

**Particularité Ootils** : le moteur est conçu pour être opéré par des agents IA, pas cliqué par des humains dans une interface. Les humains (votre équipe) :
- Fournissent les données (intégration avec votre ERP / WMS / fichiers).
- **Auditent** les recommandations produites par les agents.
- **Valident ou rejettent** chaque recommandation avant toute action vers l'ERP (*human-in-the-loop* garanti par architecture).

Il n'y a donc **pas d'écran de planification** à proprement parler dans Ootils. Les sorties sont des données structurées (JSON, TSV) et des rapports d'audit. Votre IT peut les brancher sur votre outil de visualisation préféré (Power BI, Tableau, Excel).

---

## 2. Pré-requis avant de démarrer

### 2.1 Côté métier (vous)

Checklist à valider avec votre équipe supply chain **avant** toute installation :

- [ ] **Périmètre pilote défini** : quels articles, quels sites, quel horizon de planification ? Recommandation v1 : 10–50 articles, 1–3 sites, horizon 13 semaines.
- [ ] **Objectif mesurable choisi** : réduction des ruptures de X %, réduction du stock de Y %, amélioration du taux de service, etc. Il faut pouvoir dire *"le pilote réussit si…"* en une phrase.
- [ ] **Référent métier identifié** : une personne de votre équipe qui répondra aux questions Ootils sur les règles métier (politiques de stock, délais standards, règles de substitution).
- [ ] **Référent IT identifié** : une personne capable d'extraire des données de l'ERP et de les pousser via API ou fichier (SAP, Dynamics, Sage, WMS — peu importe).
- [ ] **Cadence de rafraîchissement décidée** : à quelle fréquence les données maîtres et transactionnelles seront-elles envoyées à Ootils ? Voir §7 pour la cadence recommandée.

### 2.2 Côté technique (IT)

Checklist pour votre équipe IT :

- [ ] Machine ou VM Linux/Windows avec Docker et Docker Compose installés.
- [ ] Accès réseau entre la VM Ootils et votre source de données (ERP, SFTP, ou un poste intermédiaire qui exporte les fichiers).
- [ ] HTTPS obligatoire en production — certificat SSL à prévoir (Let's Encrypt, certificat interne, ou reverse proxy).
- [ ] 4 vCPU, 8 Go RAM, 100 Go disque minimum pour un périmètre pilote. Voir `docs/INFRA-RUNBOOK.md` pour le dimensionnement réel.
- [ ] Port 8000 exposé à vos utilisateurs autorisés (jamais publiquement sur Internet sans whitelist IP).
- [ ] Sauvegarde quotidienne de la base PostgreSQL (à mettre en place — **non automatisé aujourd'hui**, voir §9.2 FAQ).

---

## 3. Étape 1 — Paramétrer et installer

Durée estimée : **30 minutes** si votre IT a déjà Docker opérationnel.

### 3.1 Récupérer le code

```bash
git clone https://github.com/ngoineau/ootils-core.git
cd ootils-core
```

### 3.2 Créer le fichier de configuration

Copier le modèle de configuration :

```bash
cp .env.example .env
```

Éditer `.env` et remplir les quatre variables :

| Variable | Description | Exemple |
|----------|-------------|---------|
| `POSTGRES_USER` | Utilisateur de la base interne Ootils | `ootils_app` |
| `POSTGRES_PASSWORD` | Mot de passe base interne — **fort, aléatoire, 32 caractères minimum** | `z7K2p...` (généré) |
| `POSTGRES_DB` | Nom de la base | `ootils_prod` |
| `OOTILS_API_TOKEN` | Jeton d'accès à l'API — **fort, aléatoire, 48 caractères minimum**. C'est ce jeton que vous distribuerez aux systèmes sources qui pousseront des données. | `aX9mF...` (généré) |

> ⚠️ **Sécurité critique** : ne commiter *jamais* ce fichier `.env` dans git. Stocker les jetons dans votre coffre-fort d'entreprise (HashiCorp Vault, Azure Key Vault, 1Password Teams, etc.).

### 3.3 Lancer les services

```bash
docker-compose up -d
```

Cette commande démarre deux conteneurs :
- **postgres** : base de données PostgreSQL 16.
- **api** : serveur Ootils, exposé sur le port 8000.

Les migrations de schéma s'appliquent automatiquement au premier démarrage (46 migrations à ce jour).

### 3.4 Vérifier que le serveur répond

```bash
curl http://localhost:8000/health
```

Réponse attendue :

```json
{"status": "ok", "version": "1.0.0"}
```

Si vous obtenez une erreur, consulter les logs :

```bash
docker-compose logs api --tail 100
```

Causes les plus fréquentes :
- `OOTILS_API_TOKEN` non défini → le serveur refuse de démarrer (comportement *fail-closed* volontaire).
- Port 8000 déjà pris sur la machine.
- Volume Postgres corrompu → supprimer le volume et relancer (attention : perte de données).

### 3.5 Accéder à la documentation interactive (si explicitement activée)

Dans un environnement contrôlé où l'équipe technique a réactivé la documentation API, ouvrir dans un navigateur :

```
http://<ip-serveur>:8000/docs
```

Cette interface Swagger ne doit pas être présumée exposée par défaut. Elle sert à valider rapidement un token ou un appel API avant de coder l'intégration.

### 3.6 (Optionnel) Charger des données de démonstration

Pour voir Ootils fonctionner avant d'envoyer vos vraies données :

```bash
docker-compose exec api python scripts/seed_demo_data.py
```

Cela charge un scénario fictif (2 articles, 2 sites, historique et prévisions réalistes). Vous pouvez ensuite interroger `/v1/issues` ou `/v1/projection` pour voir les sorties.

---

## 4. Étape 2 — Envoyer vos données réelles

Ootils ne lit **jamais** directement dans votre ERP. Vos systèmes sources poussent les données vers Ootils, via API REST ou fichier batch. C'est une décision d'architecture volontaire : vos systèmes restent maîtres de leurs données.

### 4.1 Quels types de données envoyer

Ootils distingue deux grands groupes :

#### Données maîtres (changent rarement — envoi hebdo ou à la demande)

| Entité | Endpoint API | Description |
|--------|--------------|-------------|
| Articles | `POST /v1/ingest/items` | Votre référentiel SKU avec unité de mesure, type |
| Sites / Dépôts | `POST /v1/ingest/locations` | Usines, DC, magasins |
| Fournisseurs | `POST /v1/ingest/suppliers` | Référentiel fournisseur |
| Conditions fournisseur | `POST /v1/ingest/supplier-items` | Délai, MOQ, prix par couple (fournisseur × article) |
| Ressources | `POST /v1/ingest/resources` | Machines, lignes, capacité journalière |

#### Données transactionnelles (changent fréquemment — envoi quotidien minimum)

| Entité | Endpoint API | Description |
|--------|--------------|-------------|
| Stock disponible | `POST /v1/ingest/on-hand` | Stock libre par (article × site) |
| Commandes fournisseur | `POST /v1/ingest/purchase-orders` | PO ouverts avec date de livraison confirmée |
| Commandes clients | `POST /v1/ingest/customer-orders` | CO ouvertes avec date demandée |
| Prévisions de demande | `POST /v1/ingest/forecast-demand` | Prévisions par période |
| Ordres de fabrication | `POST /v1/ingest/work-orders` | Ordres en cours |
| Transferts inter-sites | `POST /v1/ingest/transfers` | Stocks en mouvement entre vos sites |

> **Nomenclatures (BOM)** : pour les articles multi-niveaux, un endpoint séparé existe (`POST /v1/bom/*`). À brancher dès que vous dépassez les articles achetés purs.

### 4.2 Principe fondamental : les codes métier

Tous les appels utilisent **vos codes ERP** (`external_id`), **jamais** des identifiants internes Ootils. Exemple pour un article :

```json
POST /v1/ingest/items
Authorization: Bearer <OOTILS_API_TOKEN>
Content-Type: application/json

{
  "source_system": "sap_ecc",
  "items": [
    {
      "external_id": "SKU-0042",
      "name": "Pompe hydraulique 50 bar",
      "uom": "EA",
      "item_type": "finished_good"
    },
    {
      "external_id": "SKU-0043",
      "name": "Joint torique Ø12",
      "uom": "EA",
      "item_type": "raw_material"
    }
  ]
}
```

**Avantage** : si vous changez d'ERP demain, ou si vous ajoutez une deuxième source, vos codes métier restent les clés — vous ne manipulez jamais des UUID techniques.

### 4.3 Exemple bout-en-bout : envoyer un lot de commandes fournisseur

Votre équipe IT extrait chaque nuit les PO ouvertes de SAP (transaction `ME2M` ou équivalent), les convertit en JSON, et les envoie :

```bash
curl -X POST https://ootils.votre-domaine.com/v1/ingest/purchase-orders \
  -H "Authorization: Bearer <OOTILS_API_TOKEN>" \
  -H "Content-Type: application/json" \
  -d @purchase_orders_20260418.json
```

Exemple de payload :

```json
{
  "source_system": "sap_ecc",
  "batch_ref": "PO_SAP_20260418_0600",
  "orders": [
    {
      "external_id": "4500123456-10",
      "item_external_id": "SKU-0042",
      "location_external_id": "DC-PARIS",
      "supplier_external_id": "SUP-FRA-001",
      "quantity": 500,
      "uom": "EA",
      "expected_date": "2026-04-22",
      "status": "open"
    }
  ]
}
```

Réponse d'Ootils :

```json
{
  "batch_id": "a3f2...",
  "rows_received": 1,
  "rows_accepted": 1,
  "rows_rejected": 0,
  "warnings": [],
  "errors": []
}
```

En cas de rejet (par exemple un `item_external_id` inconnu), Ootils retourne la liste précise des lignes en erreur avec le motif — votre IT peut alors corriger la source.

### 4.4 Format fichier batch (alternative à l'API)

Si votre IT préfère déposer des fichiers plutôt que coder des appels API, Ootils accepte le format **TSV** (tabulation) — voir `docs/SPEC-INTEGRATION-STRATEGY.md` §3 pour les conventions.

> ⚠️ **Statut aujourd'hui** : le dépôt SFTP automatique avec polling est **planifié mais pas encore implémenté**. En attendant, un script d'upload manuel peut lire un fichier TSV et le pousser via l'API. Demandez-le à votre interlocuteur Ootils si ce mode vous intéresse.

### 4.5 Check-list qualité à cocher avant le premier envoi

- [ ] Les dates sont au format ISO 8601 (`YYYY-MM-DD`), pas `DD/MM/YYYY`.
- [ ] Les décimaux utilisent un **point** (`.`), jamais une virgule.
- [ ] Encodage **UTF-8 sans BOM**.
- [ ] Les codes article et site existent dans vos données maîtres Ootils **avant** d'envoyer des transactions qui les référencent. Sinon, rejet.
- [ ] Les quantités sont exprimées dans l'unité de mesure (`uom`) déclarée sur l'article maître. Pas de conversion implicite par Ootils.
- [ ] Un seul `source_system` par fichier/batch. Pas de mélange SAP + Excel + WMS dans un même appel.

---

## 5. Étape 3 — Lire les retours du moteur

C'est là que la valeur arrive. Une fois les données envoyées, Ootils calcule, détecte les ruptures, et met à disposition plusieurs types de retours.

### 5.1 Les types de retours

| Retour | Disponibilité | Pour qui | Format |
|--------|---------------|----------|--------|
| **Liste des ruptures à venir** | Livré | Planificateur, directeur SC | JSON (API) — exportable Excel |
| **Projection de stock** | Livré | Planificateur | JSON — exportable Excel |
| **Explication causale d'une rupture** | Livré | Planificateur métier | JSON structuré |
| **Recommandations d'agents watchers** (DRAFT, validation humaine) | Livré — voir §6 | Planificateur | JSON / TSV |
| **Analyses de scénario sur périmètre activé** | Livré | Décideur SC | JSON |
| **Rapport d'audit Markdown** (synthèse humainement lisible) | Planifié — voir §5.5 | Directeur SC / audit | Markdown / PDF |

### 5.2 Consulter la liste des ruptures

```bash
curl -H "Authorization: Bearer <TOKEN>" \
  "http://localhost:8000/v1/issues?severity=all&scenario_id=<baseline>"
```

Réponse résumée :

```json
{
  "shortages": [
    {
      "item_external_id": "SKU-0042",
      "location_external_id": "DC-PARIS",
      "shortage_date": "2026-04-25",
      "severity_class": "stockout",
      "magnitude": 120,
      "uom": "EA",
      "explanation_available": true
    }
  ]
}
```

Lecture métier :
- `severity_class: stockout` = rupture complète (stock projeté ≤ 0).
- `severity_class: below_safety_stock` = passage sous le stock de sécurité, sans rupture stricte.
- `magnitude` = quantité manquante à la date indiquée.

Ce flux peut être consommé par Power BI, Tableau, ou un tableur. Votre IT branche un connecteur REST et vous disposez d'un tableau de bord temps réel.

### 5.3 Demander l'explication d'une rupture

```bash
curl -H "Authorization: Bearer <TOKEN>" \
  "http://localhost:8000/v1/explain/<pi_node_id>"
```

Ootils retourne une **chaîne causale structurée** :

```
Rupture : SKU-0042 sur DC-PARIS, 25/04/2026, magnitude 120 EA
├── Demande primaire : CO 5000234-20 (client ACME), 500 EA requis le 25/04
├── Approvisionnement principal : PO 4500123456-10, 500 EA attendus le 22/04
└── Cause racine : PO 4500123456-10 décalée du 22/04 au 28/04
    (événement reçu le 18/04/2026 via SAP, source: supplier_delay_confirmed)
```

**Pourquoi c'est important** : un planificateur peut valider (ou contester) chaque étape de la chaîne. Pas de boîte noire. C'est un principe non négociable (voir `CONTRIBUTING.md` §Explicit over magic).

### 5.4 Comparer un scénario au baseline (si cette surface est activée)

Cas typique : *"Si on accélère la PO 4500123456-10 de 3 jours, que se passe-t-il ?"*

Trois appels API :

1. `POST /v1/scenarios` — créer un scénario *"expediting_po_123456"* qui hérite du baseline.
2. `POST /v1/simulate` — injecter l'événement "PO date modifiée" dans ce scénario.
3. `GET /v1/scenarios/{id}/diff` — récupérer le delta vs baseline.

Résultat typique : *"La rupture du 25/04 disparaît. Le stock moyen passe de X à Y. Trois autres ruptures en aval sont résolues."*

### 5.5 Rapport d'audit pour la direction (🛠 en cours de livraison)

C'est le livrable clé pour faire valider le fonctionnement par des humains qui ne liront pas du JSON.

Format prévu (voir `docs/SPEC-VALIDATION-HARNESS.md` §4) : un document Markdown / PDF d'une à deux pages par *calc run*, contenant :
- Période couverte, scénario, événements déclencheurs.
- Top 5 ruptures détectées, avec la chaîne causale résumée.
- Top 5 recommandations d'action (avec priorité et échéance).
- Scénarios comparés au baseline quand cette surface est activée et validée.
- Zones d'incertitude (où le moteur signale une donnée douteuse ou une règle métier non résolue).

Cadence recommandée : un audit quotidien (le matin, après ingestion de nuit) ou hebdomadaire (vendredi avant S&OP).

**Statut** : la spec est écrite, la CLI `ootils-audit <calc_run_id>` est planifiée pour les 6 prochaines semaines. En attendant, votre interlocuteur Ootils peut produire ce rapport sur demande.

### 5.6 Décisions agent IA et validation humaine

Les agents watchers (décrits en §6) soumettent leurs recommandations **au statut `DRAFT`** dans une file de validation. La machine à états est la suivante :

| État | Signification | Qui agit |
|------|---------------|----------|
| `DRAFT` | L'agent a proposé une décision | Planificateur la revoit |
| `APPROVED` | Planificateur a validé | Système prêt à enregistrer l'action |
| `REJECTED` | Planificateur a rejeté avec motif | Feedback capturé, trace auditée |
| `EXPIRED` | Supplanté par une recommandation plus récente du même agent | Automatique |

**Règle absolue** : Ootils ne pousse **jamais** automatiquement vers votre ERP. Chaque décision passe par une validation humaine explicite. Ce n'est pas un garde-fou — c'est un principe architectural (voir `docs/SPEC-INTEGRATION-STRATEGY.md` §5.2).

> L'interface MCP pour agents externes (Claude, etc.) est spécifiée dans `docs/SPEC-INTERFACES.md` §5 mais n'est pas encore livrée.

---

## 6. Flotte d'agents watchers (livré)

Ootils embarque cinq agents automatisés (« watchers ») qui tournent sur votre périmètre et produisent des recommandations soumises à validation humaine.

### 6.1 Les cinq agents

| Agent | Ce qu'il surveille | Recommandations produites |
|-------|-------------------|--------------------------|
| **shortage_watcher** | Ruptures détectées par la projection de stock | Propositions d'action corrective (expediting, transfert inter-sites, ajustement ordre) |
| **material_watcher** | Disponibilité des composants (BOM) | Alertes sur composants manquants avant lancement d'OF |
| **lot_policy_watcher** | Cohérence des politiques de lot (MOQ, multiples, safety stock) | Suggestions de révision de paramètres de lot |
| **dq_watcher** | Qualité des données ingérées | Signalement d'anomalies (quantités nulles, dates incohérentes, codes inconnus) |
| **eando_watcher** | Exposition Excess & Obsolete (E&O) | Alertes sur stocks morts ou excédentaires, valorisation du risque |

### 6.2 Cycle de vie commun (governed_run)

Tous les agents partagent la même mécanique de gouvernance, implémentée dans `scripts/agent_governance.py` via la primitive `governed_run` :

1. Avant tout calcul, l'agent ouvre une trace (`agent_runs`, statut `RUNNING`).
2. Il marque ses recommandations précédentes comme `EXPIRED` (supersede).
3. Il insère ses nouvelles recommandations en statut `DRAFT`.
4. À la fin, il ferme sa trace (`COMPLETED`) ou, si une exception survient, la marque `FAILED` — garantissant qu'aucun run orphelin ne reste « RUNNING » indéfiniment.

Cette factorisation signifie qu'**un agent crashé ne bloque pas les autres** et que l'état du système est toujours lisible.

### 6.3 Ce que ça signifie pour votre équipe

- Chaque matin, vos planificateurs trouvent dans l'API (`GET /v1/recommendations?status=DRAFT`) la liste des recommandations en attente.
- Chaque recommandation est traçable : quel agent l'a produite, à quelle heure, sur quel périmètre, avec quelle justification.
- Valider (`APPROVED`) ou rejeter (`REJECTED`) une recommandation est l'action métier attendue — pas la modifier manuellement dans l'ERP sans passer par ce flux.

### 6.4 Garanties de non-régression

Un smoke-test CI (`tests/integration/test_agent_fleet_smoke.py`) vérifie à chaque merge que les cinq agents s'exécutent sans erreur et respectent le contrat d'idempotence (run × 2 → pas de doublon). Les agents sont donc protégés des régressions.

---

## 7. Cadence de travail recommandée

Une fois le pilote lancé, voici le rythme opérationnel que nous recommandons :

### 7.1 Cadence d'ingestion

| Donnée | Fréquence | Déclencheur |
|--------|-----------|-------------|
| Articles, sites, fournisseurs | Hebdomadaire ou à chaque création | Batch nocturne |
| Conditions fournisseurs | Hebdomadaire | Batch nocturne |
| Stock disponible | **Quotidienne** (6h du matin) | Batch nocturne après clôture |
| PO / CO / WO / Transferts | **Quotidienne** minimum, idéalement événementiel | Webhook ERP ou batch |
| Prévisions | Mensuelle ou à chaque révision S&OP | À la demande |

### 7.2 Cadence de revue

| Activité | Fréquence | Durée | Qui |
|----------|-----------|-------|-----|
| Consultation des ruptures du jour | Quotidienne | 10 min | Planificateur |
| Revue des recommandations agent IA | Quotidienne | 20 min | Planificateur |
| Audit Markdown par la direction | Hebdomadaire | 30 min | Directeur SC |
| Analyse de scénario sur décisions critiques | À la demande | Variable | Équipe S&OP |
| Revue de la qualité des données (DQ) | Hebdomadaire | 30 min | IT + métier |
| Comité pilote Ootils | Bimensuel | 1 h | Sponsor + équipe |

---

## 8. Module de demande Pyramide — en conception

> **Statut : EN CONCEPTION — PAS ENCORE LIVRÉ.** Les décisions d'architecture sont tranchées (ADR-019, 2026-05-30) mais aucune ligne de code correspondante n'est mergée sur main à ce jour. Cette section décrit la cible, pas l'existant.

### 8.1 Le problème actuel (pourquoi ce module est nécessaire)

Ootils ne dispose pas aujourd'hui d'une vraie prévision opérationnelle. La fonction de prévision actuelle lit une colonne (`time_span_start`) qui est vide sur toute donnée réellement ingérée en production, et mélange commandes et prévisions sans distinction — ce qui la rend non fonctionnelle en conditions réelles. Ce bug est identifié et sera corrigé dans le cadre du chantier Pyramide.

### 8.2 Le modèle décidé (ADR-019)

**Principe central : on prévoit la demande sur le *booking* (la prise de commande), jamais sur les expéditions.** Prévoir sur les expéditions revient à prévoir ses propres ruptures.

Le modèle suit **trois séries** :

| Série | Définition | Rôle |
|-------|-----------|------|
| **Booking** | Commandes reçues — la demande réelle du marché | Ce qu'on prévoit |
| **Shipping** | Expéditions effectives | Ce qu'on pilote, piloté par l'ERP |
| **Backlog** | Booking − Shipping cumulés | Indicateur de tension ou d'arriéré |

Le **shipping plan** (commandes fermes + prévision nettée du backlog) est ce qui descend dans le MRP — c'est la demande en entrée du moteur de planification.

### 8.3 La prévision granulaire automatique (Pyramide)

La prévision est produite **au niveau famille de produits × zone climatique** (approche middle-out), puis désagrégée jusqu'au SKU et au centre de distribution. Elle intègre :

- Les **dimensions métier** : canal de vente, région géographique, type de client, type de commande.
- Le **calendrier S&OP** : programmes d'achat nommés (March Buy, June Buy, Early Buy), phases de saison — variables d'une année sur l'autre, éditables.
- Les **deux mesures** : unités ET valeur (prix moyen glissant sur 12 mois, hors retours warranty à 0 €/$).

L'automatisation par Pyramide est la condition pour gérer cette richesse dimensionnelle sans que le planificateur ait à paramétrer chaque série manuellement.

### 8.4 Deux modèles d'opération avec la même prévision

Pyramide sert deux organisations différentes :

| Modèle | Description | Flux |
|--------|-------------|------|
| **Manufacturing** | Fabrication + distribution | Prévision → MRP (explosion fab/achat) → DRP (déploiement DC) |
| **Distribution pure** | Pas de fabrication, réseau de centres de distribution | Prévision → DRP seul (réapprovisionnement + déploiement) |

Dans les deux cas, le stock est planifié **en central** (safety stock mutualisé = moins de stock total) puis **déployé vers les centres de distribution via DRP**. Cette approche est directement chiffrable face aux organisations décentralisées où chaque DC porte son propre stock de sécurité.

### 8.5 Ce qui n'est pas dans la cible V1

Réconciliation statistique avancée (MinT) ; connecteurs ERP natifs ; modèles ML exogènes ; gestion multi-devises ; Demand Anomaly Watcher automatisé.

### 8.6 Prochaine étape concrète

Importer la première table `demand_history` (bookings + shippings + valeur + dimensions) dès disponibilité de l'extract réel. Aucune fonctionnalité Pyramide ne sera documentée comme livrée avant que le code soit mergé sur main et couvert par des tests.

---

## 9. Annexes

### 9.1 Glossaire

| Terme | Définition |
|-------|------------|
| **external_id** | Votre code métier ERP (ex: SAP `MATNR`). Clé universelle entre vos systèmes et Ootils. |
| **Scénario** | Version de votre plan. Le baseline = état courant. Une variante = branche de simulation quand cette surface est activée. |
| **Calc run** | Une exécution du moteur de calcul. Chaque run a un identifiant unique traçable. |
| **Projection (PI)** | Projected Inventory — stock projeté par bucket temporel. |
| **Shortage** | Rupture détectée : moment où la projection passe sous 0 (stockout) ou sous le stock de sécurité. |
| **Explain chain / chaîne causale** | Enchaînement explicite des causes d'une rupture, remontant de la demande à la cause racine. |
| **MRP** | Material Requirements Planning — explosion des besoins en composants via les nomenclatures. |
| **RCCP** | Rough-Cut Capacity Planning — détection des dépassements capacité ressource. |
| **DQ agent** | Module de validation qualité des données à l'ingestion. |
| **Ghost node** | Nœud virtuel (capacité agrégée ou transition) créé automatiquement par le moteur. |
| **DRP** | Distribution Requirements Planning — planification du déploiement du stock vers les centres de distribution. |
| **Watcher** | Agent automatisé qui surveille un périmètre (ruptures, lots, qualité…) et soumet des recommandations DRAFT. |
| **governed_run** | Primitive partagée qui garantit qu'un agent ouvre et ferme proprement sa trace, même en cas de crash. |
| **Booking** | Prise de commande (demande du marché) — série sur laquelle on prévoit (module Pyramide, en conception). |
| **Shipping plan** | Commandes fermes + prévision nettée du backlog : entrée du MRP (module Pyramide, en conception). |

### 9.2 FAQ

**Q : Ootils remplace-t-il mon APS / Kinaxis / Blue Yonder ?**
R : Non. Ootils est une couche de décision légère. Il peut coexister avec un APS existant, et vise plutôt les organisations qui tournent aujourd'hui sur Excel + ERP sans APS dédié.

**Q : Mes données sont-elles envoyées à un cloud externe ?**
R : Non, sauf pour un appel LLM optionnel sur l'agent qualité de données. En déploiement on-premise, Ootils tourne intégralement chez vous.

**Q : Combien de temps pour un pilote ?**
R : Typiquement 4 à 8 semaines : 1 semaine installation + connexion des données, 2–3 semaines ingestion + ajustements de mapping, 2–3 semaines exploitation + feedback.

**Q : Ootils peut-il écrire dans mon ERP ?**
R : Techniquement oui (roadmap Phase 3). Contractuellement, jamais sans validation humaine explicite d'une décision. C'est une garantie non négociable.

**Q : Que se passe-t-il si un fichier contient 100 000 lignes ?**
R : L'endpoint ingest a une limite de 10 Mo par appel. Pour les gros volumes, découper en lots (`batch_ref` différents). Votre interlocuteur Ootils peut fournir un script client.

**Q : La base est-elle sauvegardée automatiquement ?**
R : Non, pas aujourd'hui. C'est un point ouvert — votre IT doit mettre en place un `pg_dump` quotidien avec rotation 7 jours et copie offsite. Voir `docs/INFRA-RUNBOOK.md`.

**Q : Comment réinitialiser le système en cas de problème ?**
R : `docker-compose down -v` supprime les volumes (perte de données). Puis `docker-compose up -d` relance une instance vierge. À faire uniquement avec un backup récent, ou en pilote avant premier chargement réel.

**Q : Ootils fait-il des prévisions de demande aujourd'hui ?**
R : Non en conditions de production réelles. La fonction de prévision actuelle présente un bug bloquant sur les données réelles (colonne vide). Le module de prévision (Pyramide) est en conception — voir §8.

### 9.3 Qui fait quoi

| Action | Responsable |
|--------|-------------|
| Hébergement + installation + mise à jour | IT client |
| Génération et rotation du `OOTILS_API_TOKEN` | IT client |
| Backup PostgreSQL | IT client |
| Extraction des données depuis ERP / WMS | IT client |
| Mapping des champs ERP → Ootils | IT client (assistance Ootils) |
| Revue qualité des données | Métier + IT |
| Consultation des ruptures et explications | Planificateur |
| Validation des recommandations d'agents | Planificateur |
| Audit hebdomadaire direction | Directeur SC |
| Ajustement des règles métier | Métier + équipe Ootils |
| Évolutions du moteur, nouveaux connecteurs | Équipe Ootils |

### 9.4 Limites connues (honnêteté intellectuelle)

À date de ce manuel (draft v0.2, 2026-05-31), le produit est en phase pilote. Les limites à connaître :

- **Pas de push automatique vers l'ERP** — les recommandations se consultent en API ou se téléchargent en TSV.
- **Pas de connecteur SAP / Dynamics natif** — l'intégration passe par l'API générique. Les connecteurs natifs sont en Phase 2 roadmap.
- **Pas de backup automatisé** — à configurer côté IT.
- **Interface MCP (agents externes) non livrée** — la surface est spécifiée dans `docs/SPEC-INTERFACES.md` §5 mais pas encore câblée.
- **Rapport d'audit Markdown non livré** — peut être produit sur demande par l'équipe Ootils en attendant.
- **Prévision de demande non fonctionnelle en production** — bug `_get_historical_demand` identifié, sera corrigé dans le chantier Pyramide (voir §8).
- **Module de demande Pyramide en conception** — aucune fonctionnalité livrée sur ce périmètre à ce jour.
- **Échelle validée à 50 articles en démo** ; base pilote testée à 36 000+ items, travaux de scaling documentés dans `docs/SCALABILITY.md`.
- **Mono-tenant** — un déploiement = un client. Pas de SaaS multi-tenant en v1.

### 9.5 Contacts et ressources

| Ressource | Où |
|-----------|-----|
| Documentation API interactive | `/docs` uniquement si explicitement réactivé en environnement contrôlé |
| Documentation technique (référence) | `docs/SPEC-INTERFACES.md` |
| Stratégie d'intégration | `docs/SPEC-INTEGRATION-STRATEGY.md` |
| Spec validation & audit | `docs/SPEC-VALIDATION-HARNESS.md` |
| Module demande (ADR) | `docs/ADR-019-demand-model-pyramide.md` |
| Roadmap produit | `ROADMAP.md` |
| Runbook infra | `docs/INFRA-RUNBOOK.md` |
| Issues / bugs | GitHub `ngoineau/ootils-core/issues` |

---

*Document vivant. Toute divergence entre ce manuel et le comportement réel du produit est un bug du manuel à corriger.*
*Prochaine révision prévue : à la livraison de la première brique du module de demande Pyramide.*
