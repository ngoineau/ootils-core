# Doctrine des interfaces Ootils — comment les données entrent et sortent

**Date** : 2026-07-13
**Statut** : Décision pilote validée le 2026-07-13.
**Décision associée** : [ADR-042](ADR-042-interface-doctrine.md) (la version technique de ce document, avec les références de code).

> Ce document est destiné à être montré tel quel à l'équipe ERP du pilote.
> Il décrit ce qu'Ootils attend, ce qu'il refuse, et pourquoi.

---

## 1. Le principe en une phrase

Chaque flux de données entre l'ERP et Ootils est coupé en deux moitiés, séparées par **un fichier** :

```
   ERP / système du pilote          FICHIER TSV               Ootils
  ┌─────────────────────┐    ┌──────────────────────┐   ┌──────────────────┐
  │ dépose le fichier    │ ─▶ │  on_hand_20260713.tsv │─▶ │ reçoit, contrôle,│
  │ à sa propre cadence  │    │  = LE CONTRAT         │   │ charge, rend     │
  │ (sans savoir ce      │    │  (colonnes figées,    │   │ compte           │
  │ qu'Ootils en fait)   │    │  format TSV)          │   │ (sans savoir     │
  └─────────────────────┘    └──────────────────────┘   │ comment l'ERP     │
                                                          │ fonctionne)       │
                                                          └──────────────────┘
```

Le fichier est **le contrat**. Tant que le fichier respecte le format attendu (colonnes, cadence), aucune des deux parties n'a besoin de connaître les détails internes de l'autre. Le côté entreprise dépose et reprend selon son propre rythme ; le côté Ootils contrôle, charge, et publie un compte-rendu.

**En sortie, c'est symétrique** : Ootils dépose des recommandations déjà validées (jamais brutes), et la seule confirmation que ces recommandations ont été suivies d'effet vient du fichier du lendemain — **jamais** d'une écriture directe dans l'ERP.

**Règle absolue, sans exception** : Ootils n'écrit **jamais** directement dans l'ERP. Zéro API, zéro webhook, zéro connexion directe vers le système du pilote. Tout passe par le fichier.

---

## 2. Les flux, cadence par cadence

### 2.1 Ce qui entre dans Ootils

| Flux | Que contient-il | Cadence | Est-il bloquant ? | Qui le fournit |
|---|---|---|---|---|
| **Stock disponible** (`on_hand`) | Quantité en stock par article et par site | Quotidien | **Oui** — sans stock à jour, tout le reste du calcul est faux | Système d'entrepôt / ERP |
| **Commandes fournisseurs ouvertes** (`purchase_orders`) | Les commandes passées aux fournisseurs, pas encore reçues | Quotidien | **Oui** — sinon Ootils recommande une commande qui existe déjà | ERP achats |
| **Ordres de fabrication ouverts** (`work_orders`) | Les ordres de fabrication en cours | Quotidien | Non (« advisory ») — tous les sites pilotes ne fabriquent pas | ERP / MES |
| **Commandes clients** (`customer_orders`) | La demande ferme des clients | Quotidien | **Oui — décision du 13/07/2026** | ERP ventes |
| **Prévisions** (`forecasts`) | La demande anticipée | Hebdomadaire | Non | À décider (🎯 question ouverte, voir §5) |
| **Référentiel** (articles, sites, fournisseurs, conditions d'approvisionnement, nomenclatures) | Les données de base qui changent rarement | À la demande | Non — jamais dans le run bloquant du jour | ERP, mise à jour ponctuelle |

**Pourquoi `customer_orders` est devenu bloquant le 13/07** : sans commande client à jour, Ootils ne sait pas ce qui doit sortir du stock demain. Un calcul de pénurie fait sur une demande obsolète serait pire qu'utile — il donnerait une fausse impression de sécurité (ou d'alerte).

**Pourquoi le référentiel n'est jamais bloquant** : un changement de fiche article ou de site ne doit jamais retarder le calcul de pénurie du jour. Le référentiel se met à jour à son propre rythme, indépendamment du run quotidien.

### 2.1bis Les historiques de calibrage (ajout du 13/07/2026)

En plus des photos quotidiennes ci-dessus, Ootils a besoin — à un rythme beaucoup plus lent — de **l'historique du réalisé**. Pourquoi : pour vérifier si les paramètres de planification de l'ERP disent la vérité. Exemple concret : si l'ERP affiche « délai fournisseur : 30 jours » mais que l'historique des réceptions montre 45 jours en réalité, tous les calculs partent avec 15 jours de retard incorporé. C'est l'historique qui permet de le prouver et de proposer la correction.

| Historique | Ce qu'il permet de vérifier | Cadence |
|---|---|---|
| **Commandes fournisseurs clôturées** (date de commande → date de réception réelle) | Les délais fournisseurs réels vs les délais théoriques — et la fiabilité de chaque fournisseur | Mensuel |
| **Ordres de fabrication clôturés** | Les délais de fabrication réels, les rendements | Mensuel |
| **Consommations** (sorties de stock des composants) | Le bon dimensionnement des stocks de sécurité des composants | Mensuel |
| **Transferts réalisés** entre sites | Les flux réels entre entrepôts | Mensuel |

Trois choses importantes : ces fichiers ne sont **jamais bloquants** (leur absence ne retarde jamais le calcul du jour) ; ils s'**empilent** (on ajoute l'historique du mois, on n'écrase rien) ; et **l'historique des commandes clients est déjà chargé** (c'est lui qui alimente les prévisions) — inutile de le renvoyer.

### 2.2 Ce qui sort d'Ootils

| Flux | Que contient-il | Cadence |
|---|---|---|
| **Propositions de commande** (`po_drafts`) | Recommandations déjà validées : commander maintenant, commander en urgence, accélérer une commande existante | Quotidien |
| **Messages de re-datation** (`reschedule_messages`) | Avancer, retarder, ou annuler une commande existante | Quotidien |
| **Mises à jour de paramètres** (`param_updates`) | Ajustements de délais, stocks de sécurité, etc. | Hebdomadaire (chantier futur, pas encore commencé) |
| **Compte-rendu quotidien** (`daily_report_<date>.txt`) | Ce qui a tourné, ce qui a bloqué, ce qui a été proposé | Quotidien |

Toutes les recommandations sortantes ont déjà été **validées** avant d'être déposées — ce ne sont jamais des brouillons bruts. La validation (« gouvernance ») est décrite au §3.

---

## 3. Une journée type

Voici ce qui se passe, dans l'ordre, chaque jour :

1. **Dépôt** — l'ERP dépose ses fichiers du jour (stock, commandes fournisseurs, commandes clients, éventuellement ordres de fabrication) dans un dossier partagé.
2. **Détection** — Ootils repère les fichiers attendus pour aujourd'hui.
3. **Vérification d'identité** — chaque fichier reçoit une empreinte numérique (pour détecter une corruption ou un doublon).
4. **Vérification du délai** — chaque flux a une fenêtre d'arrivée acceptable (par exemple : le stock doit arriver avant 7h30). Un flux en retard au-delà de sa fenêtre est traité comme manquant.
5. **Vérification du volume** — Ootils compare le nombre de lignes du fichier du jour à ce qui est attendu, et à la veille. Une chute brutale (par exemple, un export qui n'a écrit que la moitié des lignes) est détectée ici — **c'est le principal filet de sécurité contre une extraction silencieusement incomplète.**
6. **Vérification qualité des données** — les valeurs sont contrôlées (dates valides, quantités positives, références connues) **avant** que quoi que ce soit ne soit chargé.
7. **Décision** — si tout est vert (qualité + toutes les vérifications), le run est approuvé automatiquement. Si un flux **bloquant** échoue une vérification, le run entier est bloqué et une alerte part vers un humain (voir §4). Si un flux **non bloquant** (`advisory`) échoue, le run continue mais la confiance du jour est dégradée — c'est signalé dans le compte-rendu, jamais caché.
8. **Chargement** — les données sont chargées dans Ootils, dans le bon ordre (le référentiel avant le stock, le stock avant les commandes, etc.).
9. **Calcul** — Ootils recalcule les pénuries et les recommandations à partir des données fraîches.
10. **Compte-rendu** — une ligne de suivi est enregistrée, et le compte-rendu du jour est produit (voir §6).
11. **Dépôt sortant** — les recommandations déjà validées sont déposées dans le dossier de sortie, prêtes à être reprises par l'ERP.
12. **Le lendemain, la boucle se referme** — quand l'ERP dépose ses nouvelles commandes fournisseurs, Ootils regarde si elles correspondent à une recommandation déposée la veille (voir §4bis, la réconciliation).

### 3.1 Que se passe-t-il si quelque chose ne va pas ?

- Un flux **bloquant** (stock, commandes fournisseurs, commandes clients) manquant, en retard, ou avec un volume anormal → **le run entier est bloqué**, et un humain est alerté immédiatement (message automatique). Rien n'est chargé sur une base de données douteuse.
- Un flux **non bloquant** (ordres de fabrication, référentiel) avec un problème → le run continue, mais c'est noté dans le compte-rendu comme une donnée de confiance dégradée.
- **Un automatisme ne dispense jamais du contrôle** — il déplace le contrôle d'un clic humain répété chaque jour vers une vérification programmée qui alerte précisément quand un signal concret le justifie.

---

## 4. La gouvernance des recommandations — jamais d'écriture directe

Toute recommandation qu'Ootils produit passe par un circuit de validation avant d'être déposée en sortie :

- Les actions **réversibles et de faible risque** (par exemple, avancer une commande existante) peuvent être approuvées automatiquement si les conditions sont réunies.
- Les actions **irréversibles ou à fort enjeu** (par exemple, annuler une commande fournisseur) exigent **toujours** une validation humaine, sans exception. Un message est envoyé au responsable désigné dès qu'une telle action est proposée.

**Dans tous les cas, Ootils ne pousse jamais rien directement dans l'ERP.** La seule action d'Ootils est de déposer un fichier dans le dossier de sortie. C'est un humain (ou le processus habituel de l'ERP) qui décide de reprendre — ou non — ce qui a été proposé.

### 4bis. Comment Ootils sait qu'une recommandation a été suivie

Il n'existe pas, dans le système du pilote, de moyen de faire porter un identifiant Ootils sur une commande fournisseur créée dans l'ERP (ni de champ dédié à cet effet). **Décision du 13/07/2026** : le rapprochement se fait donc par **ressemblance** — un rapprochement automatique compare une commande fournisseur qui arrive (dans le flux du lendemain) avec les recommandations déposées récemment, en comparant : le même article, le même site, le même fournisseur, une quantité proche (avec une tolérance), une date proche (dans une fenêtre).

Ce rapprochement est **une observation, jamais une décision automatique** : il ne modifie rien dans l'ERP, il note simplement « cette recommandation semble avoir été suivie ». Comme toute méthode de rapprochement par ressemblance, il peut se tromper sur des cas ambigus (deux recommandations très proches pour le même article, par exemple). **Ce taux d'erreur est mesuré et publié dans le compte-rendu, jamais dissimulé** — c'est une garantie de transparence, pas une promesse de perfection.

---

## 5. Ce que le compte-rendu quotidien contiendra

Chaque jour, un compte-rendu (fichier texte déposé dans le dossier de sortie, et une page de consultation) indiquera :

- Quels flux sont arrivés, à quelle heure, avec combien de lignes.
- Quelles vérifications sont passées, lesquelles ont échoué et pourquoi.
- Si le run a été approuvé automatiquement ou bloqué, et pourquoi.
- Combien de recommandations ont été produites et déposées.
- Le taux de rapprochement ambigu de la veille (voir §4bis).

**Question encore ouverte (🎯)** : quel doit être le canal préféré pour consulter ce compte-rendu — le fichier texte seul, une page web de consultation, ou les deux ? L'envoi par email est prévu pour une version ultérieure, pas cette première version.

---

## 6. Ce qu'on refuse en V1, et pourquoi

| Refusé | Pourquoi |
|---|---|
| **Envoi en temps réel / au fil de l'eau (« delta »)** | Complexité et risque disproportionnés pour un premier déploiement ; le rythme quotidien couvre le besoin réel identifié à ce stade. |
| **Connexion directe (SFTP, API, webhook) vers l'ERP** | Chaque connexion directe est un couplage supplémentaire à maintenir et un risque de sécurité ; le fichier déposé reste le seul contrat, plus simple à auditer et à faire évoluer sans casser l'autre côté. |
| **Écriture automatique dans l'ERP** | Ligne rouge absolue. Une erreur de calcul qui s'écrirait directement dans l'ERP serait irréversible et invisible avant qu'un humain ne la découvre. Le dépôt de fichier + reprise humaine reste le seul chemin. |
| **Plusieurs sources différentes pour un même flux** | Une seule source par type de donnée évite les conflits silencieux entre deux systèmes qui se contrediraient. |
| **Plusieurs fois par jour (« intraday »)** | Le rythme quotidien suffit au périmètre visé aujourd'hui ; le complexifier maintenant n'apporterait pas de valeur proportionnée. |
| **Fichiers CSV ou Excel** | Règle du pilote (2026-07-11) : le CSV pose des problèmes de séparateur et de guillemets sur des données métier réelles (« c'est un enfer »). Le TSV (séparateur = tabulation) évite ce problème par construction. |
| **Réactiver l'ancien circuit de validation manuelle par lot** | Un circuit existant exigeait un clic humain sur chaque flux, chaque jour — un frein qui finit soit par être ignoré (clic sans lecture), soit par arrêter le run quotidien. Il est remplacé par des vérifications automatiques objectives (§3), pas par de la confiance aveugle. |
| **Éclatement automatique des prévisions mensuelles en semaines/jours** | Pas encore fiable de façon automatique ; à traiter dans un chantier dédié plus tard. |
| **Annulation automatique d'une commande fournisseur** | Une annulation est une action irréversible côté fournisseur — elle reste toujours soumise à validation humaine (§4), jamais automatique. |

---

## 7. Ce qui reste à clarifier avec vous (l'équipe ERP)

- **Prévisions** : préférez-vous que les prévisions viennent de votre ERP, ou d'un module de calcul interne à Ootils ?
- **Colonnes exactes** : les formats de fichier pour le stock et les commandes fournisseurs sont déjà figés ; ceux pour les commandes clients et les ordres de fabrication restent à finaliser avec vous.
- **Cadences réelles** : les horaires et tolérances de retard actuellement documentés sont des valeurs de démarrage raisonnables mais provisoires — à ajuster ensemble une fois le rythme réel de vos extractions connu.
- **Compte-rendu** : quel format de consultation vous convient le mieux au quotidien ?
- **Historiques** (§2.1bis) : pouvez-vous extraire les commandes fournisseurs clôturées (avec les dates de réception réelles), les ordres de fabrication clôturés, les consommations et les transferts réalisés — et sur quelle profondeur (12 mois ? 24 mois ?) Plus l'historique est profond, plus le calibrage des paramètres sera fiable. Rien d'urgent : ces fichiers ne servent qu'au chantier « paramètres », pas au run quotidien.

---

*Document de référence technique : [ADR-042](ADR-042-interface-doctrine.md). Pour le détail des colonnes fichier par fichier : `docs/contracts/TSV-FILES-SPEC.md`.*
