# ADR-019 — Modèle de demande unifié (Pyramide) : booking / shipping / backlog

**Status** : **Accepted (décisions tranchées)** — Implémentation à venir — 2026-05-30
**Owner** : ngoineau
**Depends on** : [ADR-017](ADR-017-architecture-b-rust-engine-service.md), [ADR-018](ADR-018-per-scenario-propagation.md)
**Décante** : [docs/WIP-demand-module-design-session.md](WIP-demand-module-design-session.md) (brouillon D1-D8, désormais tranché)

---

## Résumé (non technique)

Ootils ne sait aujourd'hui pas faire de vraie prévision en production : il n'enregistre nulle part les **ventes réellement réalisées**, et la fonction de prévision se nourrit de ses propres prévisions futures (et lit une colonne vide sur la donnée réelle). Cet ADR fige le modèle de demande cible.

Principe métier fondateur : **on prévoit la demande sur le *booking* (la prise de commande), jamais sur les expéditions** — sinon on prévoit ses propres ruptures. On suit **trois séries** : le **booking** (la demande, ce qu'on prévoit), le **shipping** (les expéditions, qu'on pilote), et le **backlog** (commandé non encore expédié = booking − shipping). Le **shipping plan** (commandes fermes + prévision nettée) est ce qui descend dans le MRP, pas le booking brut.

La demande est prévue **finement** (par famille de produit × zone climatique, jusqu'au centre de distribution) et **automatiquement** par le module **Pyramide** (sans quoi la richesse dimensionnelle serait ingérable). Côté appro, on **planifie en central** (safety stock mutualisé = moins de stock total) puis le **DRP répartit** le stock vers les centres de distribution.

Ootils sert **deux modèles** avec **la même prévision de demande** : le **manufacturing** (avec MRP, fabrication) et la **distribution pure** (DRP seul, sans fabrication — ex. un client à 325 centres de distribution). Dans les deux cas, mutualiser le stock en central et le déployer via DRP réduit le stock total — c'est une proposition de valeur chiffrable face aux organisations décentralisées actuelles.

---

## Contexte

Diagnostic (cf. WIP-demand §1 + audit 2026-05-30) :
- **Trois implémentations divergentes** de la consommation de demande : `scripts/mrp_core.py` (vérité réelle des watchers), `src/ootils_core/engine/mrp/forecast_consumer.py` (2ᵉ impl), `api/routers/forecasting.py:_get_historical_demand`.
- **Aucune notion d'« actuals »** (ventes réelles passées) dans le modèle de nodes — seuls `CustomerOrderDemand` et `ForecastDemand` existent.
- `_get_historical_demand` somme prévision + commandes, ignore le `scenario_id`, inclut des dates futures, et lit `time_span_start` qui est **NULL sur toute demande ingérée réellement** → prévision non fonctionnelle en production.
- Garder l'historique (années × SKU × dimensions) dans le graphe RAM = intenable (~136 Go).

---

## Décisions

### D1 — Vérité de demande & entrée du MRP
- On **prévoit le booking** (prise de commande), **jamais le shipping**.
- On suit **3 séries** : **booking** (commandes + forecast), **shipping** (expéditions, pilotables), **backlog** (booking − shipping).
- Le **shipping plan** = commandes fermes **+** forecast netté (pas de double-comptage), **pilotable**, et c'est **lui la demande en tête du MRP**.
- Une commande a deux rôles : elle alimente le booking **et** elle est une pièce ferme du shipping plan.

### D2 — Dimensions (conditionnelles à l'automatisation)
Découpages : **canal, région, type de client, type de commande** (+ « tiroir libre » extensible). **Condition non négociable** : ajoutés *uniquement* parce que Pyramide les gère automatiquement (choix du niveau de prévision, agrégation/désagrégation, réconciliation). Pas d'automatisation → pas de dimensions. D2 est donc soudée à D3 + D4.

### D3 — Hiérarchies
**Une table générique** décrit tous les arbres. Trois arbres initiaux :
- **Produits** : `Gen_Fam → Gen_Group → Gen_Prod` — structure de référence du métier (stable 15 ans), **importée telle quelle** (specs à fournir).
- **Régions** : États (US) → zones climatiques (snow belt / sun belt…).
- **Clients / canaux** : Distrib, eCom… (recouvrement à clarifier avec « canal » de D2).

### D4 — Réconciliation
**Middle-out** : prévision au niveau du signal le plus propre (**Gen_Fam / Gen_Group × zone climatique**), puis désagrégation vers le SKU/DC et agrégation vers les totaux, en gardant les niveaux cohérents. Méthode optimale (MinT) reportée.

### D5 — Mémoire du moteur
L'**historique profond vit en PostgreSQL** (`demand_history`), **pas dans le graphe RAM**. Le moteur de planif ne charge qu'une **fenêtre tournée vers l'avant** (+ fine tranche de passé récent, réglable). Pyramide lit l'historique en base au moment de prévoir.

### D6 — Sources de données
**Import fichier** (donnée réelle) **+ générateur synthétique** (tests contrôlés : vérifier que Pyramide retrouve des motifs connus). Connecteur ERP réel **plus tard**. Donnée réelle disponible (extract bookings + shippings + valeur, ~2026-05-31/06-01).

### D7 — Demande counter-factuelle
La demande est **forkable** : on teste les « et si » (promo, canicule, perte client) **dans un scénario**, jamais sur la baseline. What-if en scénario = action autonome légère ; modification de la demande réelle = approbation humaine (L3).

### D8 — Shipping & backlog
On **ingère les faits d'expédition depuis l'ERP** (système de référence), à côté des bookings — **pas** de statut « SHIPPED » interne (évite de dupliquer le cycle de vie ERP). **Backlog = bookings cumulés − shippings cumulés**, calculé. (Distinct du *shipping plan* forward de D1.)

### Topologie de planification (push)
- **Prévision** : granulaire (Gen_Fam/Group × zone climatique → DC/states), via Pyramide.
- **MRP** : **central / agrégé**, safety stock **piloté en central** (risk pooling → moins de stock total).
- **DRP** : redescend la demande **par DC** pour **pousser/répartir** le stock central vers les centres.
- La prévision granulaire **remonte** (MRP) et **redescend** (DRP) — jamais perdue.
- Règle réseau : **chaque DC sert des states précis ET certains types de produits** (carte DC ↔ states ↔ familles), nécessaire au DRP.

### Deux modèles d'opération — manufacturing (MRP) vs distribution (DRP seul)
Ootils doit servir **deux modèles** ; **Pyramide (la prévision de demande) est COMMUN aux deux** — seul l'aval diffère :
- **Manufacturing** : demande → shipping plan → **MRP** (explosion fab/achat) → DRP. (Cf. topologie ci-dessus.)
- **Distribution** : **pas de MRP**, **DRP seul** — réapprovisionnement + déploiement, sans fabrication. Cas réel : un gros client à **325 centres de distribution**.

La logique de demande (booking, 3 séries, prévision granulaire) est **identique** dans les deux cas.

**Échelle** : 325 DC × SKU × dimensions = centaines de milliers→millions de séries. La prévision DC-par-DC indépendante est **impossible** → confirme et durcit le middle-out (prévoir au niveau agrégé propre, déployer vers les DC), l'historique en PG (pas en RAM), et la gestion automatique par Pyramide.

**Topologie distribution recommandée + proposition de valeur** : le client à 325 DC fonctionne aujourd'hui en **décentralisé** (chaque DC achète en direct, safety stock **par DC** — sauf quelques familles en central). C'est sous-optimal : un safety par DC sur 325 DC additionne 325 variabilités → stock total excessif. Ootils doit **modéliser la réalité décentralisée actuelle ET simuler l'approche centralisée** (planif/achat central + **safety mutualisé (pooling)** + DRP qui déploie vers les DC), puis **chiffrer la réduction de stock**. C'est un argument de vente majeur (wedge) ; les familles déjà centralisées sont un point de départ. Comparaison décentralisé vs centralisé via **scénario** (D7).

### Brique A — Mesures (units + valeur)
- Stockage des **unités ET de la valeur** vendue (bookings et shippings).
- **ASP** = moyenne glissante **12 mois** (valeur ÷ unités, **hors warranty à $0**), recalculée **chaque jour/mois**, **niveau produit** (extensible canal/région).
- Demande en valeur = unités prévues × ASP courant.

### Brique B — Calendrier S&OP (saison & programmes d'achat)
- Programmes nommés : **March Buy, June Buy, Early Buy (Sept/Oct)** + phases de saison (early / peak / late).
- **Définis dans le S&OP, éditables, variables d'une année à l'autre.**
- **La prévision s'aligne sur ce calendrier** (pas le calendrier ISO figé) — c'est lui qui explique les décalages de booking des distributeurs.

---

## Conséquences

- **Nouvelle table `demand_history`** (faits, dimensionnelle, hors graphe RAM) portant booking & shipping, units & valeur, dimensions D2, `source_ref` pour idempotence.
- **Nouveau calendrier S&OP** (programmes/événements) éditable par année.
- **Une seule primitive de consommation** : `mrp_core.consume_demand` (la vérité testée) devient la source unique importable depuis `src/` ; `forecast_consumer` s'y aligne ou est déprécié. Le netting commandes + forecast est nommé **shipping plan**.
- **`_get_historical_demand` corrigé** : lit `demand_history` (bookings, passé seulement, scenario-aware) — fin du double-comptage et du `time_span_start` NULL.
- **DRP** consomme la demande granulaire par DC pour le déploiement.
- Pyramide produit des prévisions **avec score de confiance** (cf. North Star / STRATEGY), forkables, explicables.

## Hors périmètre (V1)
Réconciliation MinT ; connecteur ERP réel ; modèle exogène ML complet ; multi-devise/UoM ; Demand Anomaly Watcher ; StreamChanges dédié demande.

## Points ouverts
- Specs exactes `Gen_Fam / Gen_Group / Gen_Prod` (à fournir par le métier).
- Recouvrement « canal » (D2) vs « type de client » (D3) — à clarifier aux specs.
- Granularité ASP étendue (canal/région) — design extensible prévu.
- Définition précise des fenêtres de programmes S&OP et de leur édition annuelle.

## Prochaine étape
Cadrer la **première brique concrète** : table `demand_history` (bookings + shippings + valeur + dimensions) + import de l'extract réel — dès l'arrivée de la donnée (~2026-05-31/06-01). Aucune ligne de code avant ce cadrage.
