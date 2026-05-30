# Format de fichier — `on_hand.tsv`

> Fichier de **photo des stocks** par article × site à un instant donné.
> Première entité **transactionnelle** (vs master data) : ça bouge tous les jours / toutes les heures.
> Endpoint cible : `POST /v1/ingest/on-hand`.

---

## 1. Caractéristiques du fichier

| Propriété | Valeur |
|---|---|
| **Nom** | `on_hand.tsv` (exact) |
| **Format** | TSV |
| **Encodage** | UTF-8 (BOM toléré) |
| **Délimiteur** | tabulation (`\t`) |
| **Header** | ligne 1 obligatoire |
| **Endpoint** | `POST /v1/ingest/on-hand` |

---

## 2. Nature de la donnée

**SNAPSHOT à un instant T.** Chaque ligne dit : « au moment `as_of_date`, il y avait `quantity` unités de cet article à cet endroit ».

C'est différent de master data :
- Master data (items, locations, suppliers...) : photo de la **configuration** du système, change peu
- On-hand : photo de l'**état** opérationnel, change en permanence (chaque expédition, chaque réception, chaque inventaire)

Cadence typique :
- En production : push horaire ou temps réel depuis WMS/ERP
- En pilote : push quotidien le matin
- En MVP : push à la main quand tu veux refléter un nouvel état

---

## 3. Colonnes

**Ordre figé par le contrat canonique** : `item_external_id`, `location_external_id`, `quantity`, `uom`, `as_of_date`, `lot_number`.

| # | Colonne | Obligatoire | Type | Domaine / format | Description |
|---|---|---|---|---|---|
| 1 | `item_external_id` | **oui** | texte | doit exister dans `items` | FK vers `items.tsv` |
| 2 | `location_external_id` | **oui** | texte | doit exister dans `locations` | FK vers `locations.tsv` |
| 3 | `quantity` | **oui** | décimal | ≥ 0 | Quantité disponible à cette date |
| 4 | `uom` | non | texte | code UoM | Unité de mesure. Défaut : `EA`. Doit être cohérente avec l'UoM de base de l'article. |
| 5 | `as_of_date` | **oui** | date | format `YYYY-MM-DD` | Date à laquelle la photo a été prise |
| 6 | `lot_number` | non | texte | max 128 caractères | Numéro de lot (V1.1, **pas encore consommé par l'API V1.0**). Voir §6. |

---

## 4. Clé unique et comportement

| Aspect | Comportement |
|---|---|
| **Clé business** | couple (`item_external_id`, `location_external_id`) — un seul niveau de stock actif par couple |
| **Existe en base** | UPDATE de la quantité, UoM, et as_of_date |
| **N'existe pas** | INSERT (nouveau `node_id` OnHandSupply interne) |
| **Validation FK** | les 2 codes sont résolus contre `items` et `locations` — les deux master data doivent être chargés avant |

---

## 5. Sémantique de la quantité

- `quantity = 0` est valide → indique explicitement « stock à zéro ». Important : différent de l'absence de ligne (qui veut dire « pas de donnée »).
- `quantity = 1000` à `as_of_date = 2026-05-26` écrase l'ancienne valeur du même couple. **Pas d'historique** des photos précédentes en V1 (chaque push remplace).
- Si tu veux conserver l'historique, c'est l'engine qui le fait via les `calc_runs` et `events`, pas via on-hand directe.

---

## 6. À propos de `lot_number` (V1.1, optionnel)

Le contrat canonique V1 inclut `lot_number` mais **l'API V1.0 ne le consomme pas** (`OnHandRow` Pydantic ne le déclare pas).

Conséquences :
- Tu peux mettre la colonne dans ton TSV, elle sera **ignorée** par l'API
- Aucun risque de rejet, mais aucune persistence non plus
- Sera réellement consommé quand on supportera le lot tracking (à coupler avec `items.lot_tracked = true`)

→ Recommandation V1.0 : **ne pas mettre cette colonne** pour éviter la confusion. Le script l'ignore proprement de toute façon.

---

## 7. Exemples

### 7.1 Minimum vital (3 articles dans 1 site)

```
item_external_id	location_external_id	quantity	uom	as_of_date
COMP-MOTOR-24V	PLANT-LYON	60	EA	2026-05-26
COMP-IMPELLER-100	PLANT-LYON	80	EA	2026-05-26
SUB-HOUSING-100	PLANT-LYON	20	EA	2026-05-26
```

### 7.2 Photo réseau (plusieurs sites)

```
item_external_id	location_external_id	quantity	uom	as_of_date
FG-APU-100	DC-LILLE	15	EA	2026-05-26
FG-APU-100	WH-PARIS-01	40	EA	2026-05-26
FG-APU-200	DC-LILLE	5	EA	2026-05-26
SUB-HOUSING-100	PLANT-LYON	20	EA	2026-05-26
COMP-MOTOR-24V	PLANT-LYON	60	EA	2026-05-26
COMP-IMPELLER-100	PLANT-LYON	80	EA	2026-05-26
RAW-STEEL-50	PLANT-LYON	1200	KG	2026-05-26
```

### 7.3 Stock à zéro explicitement (déclencheur shortage)

```
item_external_id	location_external_id	quantity	uom	as_of_date
COMP-IMPELLER-100	DC-LILLE	0	EA	2026-05-26
```

→ Dit explicitement « rien en stock à DC-LILLE pour COMP-IMPELLER-100 ». Permet au Shortage Watcher de déclencher.

### 7.4 Cas invalides

```
item_external_id	location_external_id	quantity	uom	as_of_date
ITEM-UNKNOWN	PLANT-LYON	60	EA	2026-05-26         ← item inconnu → 422
COMP-MOTOR-24V	LOC-XYZ	60	EA	2026-05-26           ← location inconnue → 422
COMP-MOTOR-24V	PLANT-LYON	-5	EA	2026-05-26       ← quantity < 0 → 422
COMP-MOTOR-24V	PLANT-LYON	60	EA	26-05-2026       ← date au mauvais format → 422
COMP-MOTOR-24V	PLANT-LYON		EA	2026-05-26         ← quantity vide → 422
```

---

## 8. Pipeline

```
data/inbox/on_hand.tsv
        │
        ▼
   parse TSV
        │
        ▼
   validation FK (items + locations) + types
        │
        ├─ FK manquante ──► 422 → data/rejected/
        │
        ▼ OK
   POST /v1/ingest/on-hand
        │
        ▼
   upsert dans `nodes` (OnHandSupply)
        │
        ▼
   data/processed/on_hand_YYYYMMDD_HHMMSS.tsv + .report.json
```

---

## 9. Ordre de chargement

```
1. items.tsv               ← ✅
2. locations.tsv           ← ✅
3. suppliers.tsv           ← ✅
4. supplier_items.tsv      ← ✅
5. item_planning_params.tsv ← ✅
6. on_hand.tsv             ← ICI (premier fichier transactionnel)
7. purchase_orders.tsv     ← après
8. customer_orders.tsv     ← après
9. forecasts.tsv           ← après
10. bom_*.tsv              ← peut être chargé avant ou après le transactionnel
```

---

## 10. Cas opérationnels typiques

### 10.1 Cycle count quotidien (push automatisé)

Toutes les nuits à 02:00, ton WMS exporte un fichier `on_hand.tsv` avec les quantités au 23:59 et le drop dans `data/inbox/`. Un cron / scheduler lance le script.

### 10.2 Ajustement manuel ponctuel

Le planner trouve une erreur sur 1 article × 1 site. Il édite `on_hand.tsv` ne contenant que cette ligne, le drop, lance le script. **Pas besoin de réenvoyer tout le réseau** — l'upsert ne touche que les couples présents.

### 10.3 Photo initiale du pilote

Une seule fois en démarrage : un gros fichier avec **toutes** les paires (item × location) en stock significatif. Sert de baseline.

---

## 11. Validation rapide

```bash
python scripts/ingest_file.py data/inbox/on_hand.tsv --dry-run
```
