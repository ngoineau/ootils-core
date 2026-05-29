# Format de fichier — `customer_orders.tsv`

> Fichier des **commandes clients** (sales orders) — demande ferme à livrer depuis un site donné à une date demandée.
> Entité transactionnelle, symétrique des purchase_orders mais côté **demande**.
> Endpoint cible : `POST /v1/ingest/customer-orders`.

---

## ⚠️ Limitation V1.0 majeure — pas d'identifiant client

**Le format V1.0 ne porte AUCUN identifiant client individuel.**

Pas de `customer_external_id`, pas de `channel`, pas de `region`, pas de `customer_segment`, pas de `order_type`. Il n'existe pas non plus de table `customers` dédiée dans le schéma V1.0 (uniquement `customer_order_demand` qui est la table cible).

**Workaround V1.0** : modéliser chaque client (ou agrégat client) comme une `customer_virtual` location et utiliser `location_external_id` comme proxy du client.

```
locations.tsv :
external_id          name                    location_type
CVL-CARREFOUR        Carrefour Global        customer_virtual
CVL-AUCHAN           Auchan Global           customer_virtual
CVL-RETAIL-NORD      Retail Nord (agrégat)   customer_virtual
```

**Impact sur le wedge V1 (Autonomous Shortage Control Tower)** :
- Le **Service Risk Watcher** (W02) ne peut pas prioriser finement par key account
- Le **Customer Agent** (G03) ne peut ranker l'impact qu'au niveau de la `customer_virtual` location
- Le **Demand Anomaly Watcher** (W09) et **Pyramide** ne peuvent pas désagréger par canal/région/segment
- L'OTIF n'est pas mesurable par client individuel

**Roadmap V1.1** : ajout d'une table `customers` + d'un fichier `customers.tsv` + extension de `customer_orders.tsv` avec `customer_external_id`, `channel`, `region`, `customer_segment`, `order_type`, `promised_delivery_date`, `order_date`. Voir `docs/WIP-demand-module-design-session.md` §3.1 pour le design détaillé.

→ Avant le module Demand V2, cette extension V1.1 doit être livrée. C'est un **prérequis**, pas un nice-to-have.

---

---

## 1. Caractéristiques du fichier

| Propriété | Valeur |
|---|---|
| **Nom** | `customer_orders.tsv` (exact) |
| **Format** | TSV |
| **Encodage** | UTF-8 (BOM toléré) |
| **Délimiteur** | tabulation (`\t`) |
| **Header** | ligne 1 obligatoire |
| **Endpoint** | `POST /v1/ingest/customer-orders` |

---

## 2. Nature de la donnée

Une ligne = **une ligne de commande client** :
> « Le client a commandé `quantity` unités de `item_external_id`, à livrer depuis `location_external_id` pour le `requested_delivery_date`. Statut actuel : `status`. »

C'est ce qui alimente la projection forward côté **demande** : les CO `open` / `confirmed` créent des **sorties de stock prévues** à leur date.

Note V1 :
- Pas de notion explicite de customer (pas de FK `customers`) — V1 utilise la location comme proxy
- Si tu veux modéliser des clients individuels, créer des `customer_virtual` locations (cf. `format-locations-tsv.md`)
- Pour modéliser une commande multi-lignes, créer un `external_id` distinct par ligne (`CO-2026-001-L1`, `CO-2026-001-L2`)

---

## 3. Colonnes

**Ordre figé par le contrat canonique** : `external_id`, `item_external_id`, `location_external_id`, `quantity`, `requested_delivery_date`, `status`.

| # | Colonne | Obligatoire | Type | Domaine | Description |
|---|---|---|---|---|---|
| 1 | `external_id` | **oui** | texte | non-vide, unique dans le fichier | Numéro de commande client ERP. Clé d'upsert. |
| 2 | `item_external_id` | **oui** | texte | FK `items` | Article commandé. |
| 3 | `location_external_id` | **oui** | texte | FK `locations` | Site qui **expédie** la marchandise (DC, WH). Souvent un `dc` ou un `customer_virtual`. |
| 4 | `quantity` | **oui** | décimal | > 0 | Quantité commandée. |
| 5 | `requested_delivery_date` | **oui** | date | ISO `YYYY-MM-DD` | Date demandée par le client. |
| 6 | `status` | non | enum | voir §4 | Défaut : `open`. |

---

## 4. Les 5 statuts de commande client

| Statut | Sémantique | Impact projection |
|---|---|---|
| `open` | Brouillon, en cours de saisie | **Inclus** comme demande prévue (mou — peut changer) |
| `confirmed` | Validée, engagement client pris | **Inclus** comme demande ferme |
| `shipped` | Expédiée au client | **N'incrémente plus la demande future** — déjà sortie du stock |
| `delivered` | Livrée chez le client | Idem — fait historique |
| `cancelled` | Annulée | **Ignorée** |

Le **Service Risk Watcher** (W02) surveille les `open` et `confirmed` dont le `requested_delivery_date` ne sera pas honoré selon la projection courante.

---

## 5. Lien avec `demand_history` (Pyramide, V2)

Les statuts `shipped` et `delivered` sont la **source primaire** des faits de demande historique qui alimenteront Pyramide (cf. `docs/WIP-demand-module-design-session.md`).

→ V1 : `customer_orders.tsv` ne distingue pas encore les statuts shipping pour alimenter une table `demand_history` séparée. C'est sur la roadmap V2.

---

## 6. Customer dimension (V1.1)

**Limitation V1.0** : pas de colonne `customer_external_id`, `channel`, `region`, `customer_segment`. Ces dimensions sont essentielles pour :
- Le **Demand Anomaly Watcher** (qui doit comparer la demande par canal/segment)
- Le **Service Risk Watcher** (qui doit prioriser les key accounts)
- La désagrégation forecast Pyramide (channel × region × segment)

Workaround V1.0 : utiliser des `customer_virtual` locations distinctes par segment/region. Pas idéal mais opérationnel.

→ V1.1 : ajouter `customer_external_id`, `channel`, `region`, `customer_segment`, `order_type` en colonnes. Documenté dans `docs/WIP-demand-module-design-session.md` §3.1.

---

## 7. Exemples

### 7.1 Exemple minimal

```
external_id	item_external_id	location_external_id	quantity	requested_delivery_date	status
CO-2026-001	FG-APU-100	DC-LILLE	30	2026-06-03	confirmed
```

### 7.2 Commandes réalistes multi-clients (via customer_virtual)

```
external_id	item_external_id	location_external_id	quantity	requested_delivery_date	status
CO-2026-001	FG-APU-100	DC-LILLE	30	2026-06-03	confirmed
CO-2026-002	FG-APU-100	DC-LILLE	15	2026-06-10	open
CO-2026-003	FG-APU-200	DC-LILLE	5	2026-06-05	confirmed
CO-2026-004	FG-APU-100	CVL-RETAIL-NORD	100	2026-06-08	confirmed
CO-2026-005	FG-APU-200	CVL-RETAIL-NORD	20	2026-06-15	confirmed
CO-2026-006	FG-APU-100	DC-LILLE	8	2026-05-25	shipped
```

Lecture :
- CO-001 à 003 : commandes servies depuis DC-LILLE (livraison locale)
- CO-004, 005 : commandes globales pour le canal Retail Nord (modélisé comme `customer_virtual`)
- CO-006 : déjà expédiée — n'impacte plus la projection future

### 7.3 Commande multi-ligne (1 commande, 3 articles)

```
external_id	item_external_id	location_external_id	quantity	requested_delivery_date	status
CO-2026-100-L1	FG-APU-100	DC-LILLE	10	2026-06-12	confirmed
CO-2026-100-L2	FG-APU-200	DC-LILLE	5	2026-06-12	confirmed
CO-2026-100-L3	SUB-HOUSING-100	DC-LILLE	2	2026-06-12	confirmed
```

### 7.4 Cas invalides

```
external_id	item_external_id	location_external_id	quantity	requested_delivery_date	status
	FG-APU-100	DC-LILLE	30	2026-06-03	confirmed                    ← external_id vide → 422
CO-X	ITEM-UNKNOWN	DC-LILLE	30	2026-06-03	confirmed              ← item inconnu → 422
CO-X	FG-APU-100	LOC-XYZ	30	2026-06-03	confirmed                  ← location inconnue → 422
CO-X	FG-APU-100	DC-LILLE	0	2026-06-03	confirmed                  ← quantity = 0 → 422
CO-X	FG-APU-100	DC-LILLE	30	03-06-2026	confirmed                ← date au mauvais format → 422
CO-X	FG-APU-100	DC-LILLE	30	2026-06-03	processing               ← statut inconnu → 422
```

---

## 8. Comportement à l'ingestion

### 8.1 Identification

Clé business : `external_id`.
- **Existe en base** → UPDATE des 5 autres champs (utile pour le cycle de vie open → confirmed → shipped → delivered)
- **N'existe pas** → INSERT (nouveau node CustomerOrderDemand)

### 8.2 Cycle typique d'une commande client

```
Jour J        : INSERT avec status='open'
Jour J+x      : UPDATE avec status='confirmed' (client a validé)
Jour expédition : UPDATE avec status='shipped' (parallèlement, on_hand baisse)
Jour livraison : UPDATE avec status='delivered'
```

### 8.3 Validation FK

`item_external_id` et `location_external_id` doivent exister. Toute FK manquante → batch entier rejeté (422).

---

## 9. Pipeline

```
data/inbox/customer_orders.tsv
        │
        ▼
   parse TSV
        │
        ▼
   validation (2 FK + quantity > 0 + date ISO + status enum)
        │
        ├─ FK manquante / type invalide ──► 422 → data/rejected/
        │
        ▼ OK
   POST /v1/ingest/customer-orders
        │
        ▼
   upsert dans `nodes` (CustomerOrderDemand)
        │
        ▼
   data/processed/customer_orders_YYYYMMDD_HHMMSS.tsv + .report.json
```

---

## 10. Ordre de chargement

```
1.  items.tsv               ← ✅
2.  locations.tsv           ← ✅
3.  suppliers.tsv           ← ✅
4.  supplier_items.tsv      ← ✅
5.  item_planning_params.tsv ← ✅
6.  on_hand.tsv             ← ✅
7.  purchase_orders.tsv     ← ✅
8.  customer_orders.tsv     ← ICI (2 FK : items + locations)
9.  forecasts.tsv           ← après (forecast complète la demande sur l'horizon non-couvert par les CO)
10. transfers.tsv           ← après
11. bom_*.tsv               ← après
```

---

## 11. Limitations connues V1.0

| Manque | V1.1 / V2 envisagé |
|---|---|
| Pas de `customer_external_id` | V1.1 — pour tracer le client individuel |
| Pas de `channel`, `region`, `customer_segment` | V1.1 — dimensions essentielles pour Pyramide |
| Pas de `order_type` (STANDARD/PROMO/PROJECT/SAMPLE) | V1.1 — pour DQ + filtres agents |
| Pas de `order_date` (date de création) | V1.1 — utile pour calculer le délai de notification |
| Pas de `promised_delivery_date` (vs requested) | V1.1 — pour OTIF |
| Pas de `actual_shipment_date`, `actual_delivery_date` | V1.1 — historique faits |
| Pas de `unit_price`, `currency`, `total_value` | V1.1 — pour Finance Agent |

---

## 12. Validation rapide

```bash
python scripts/ingest_file.py data/inbox/customer_orders.tsv --dry-run
```
