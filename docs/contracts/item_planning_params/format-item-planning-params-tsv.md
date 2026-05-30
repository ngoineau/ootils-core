# Format de fichier — `item_planning_params.tsv`

> Fichier des **paramètres de planification** par couple (article × site) — lead times décomposés, safety stock, MOQ, politique de lot, fournisseur préféré.
> **Niveau 3** dans la hiérarchie de résolution des lead times (override par article × site).
> Endpoint cible : `POST /v1/ingest/planning-params` (SCD2 transparent — ADR-014 D3).

---

## 1. Caractéristiques du fichier

| Propriété | Valeur |
|---|---|
| **Nom** | `item_planning_params.tsv` (exact) |
| **Format** | TSV |
| **Encodage** | UTF-8 (BOM toléré) |
| **Délimiteur** | tabulation (`\t`) |
| **Header** | ligne 1 obligatoire |
| **Endpoint** | `POST /v1/ingest/planning-params` |

---

## 2. Logique SCD2 — point clé à comprendre

**Ce fichier ne porte PAS `effective_from` / `effective_to`.** L'API gère le versioning automatiquement :

```
Tu pousses : « voici l'état courant des paramètres pour (item × location) »
                              │
                              ▼
                ┌─────────────────────────────┐
                │ L'API compare avec          │
                │ la ligne active en base     │
                │ (celle où effective_to IS NULL) │
                └────────┬────────────────────┘
                         │
        ┌────────────────┼─────────────────┐
        ▼                ▼                 ▼
   Pas de changement   Changement      Changement
   → NOOP              le MÊME jour    un autre jour
                       → UPDATE        → ROTATE :
                       en place          - close active (effective_to=hier)
                       (effective_from   - insert nouvelle (effective_from=today)
                        préservée)
```

Conséquences :
- **Toujours rejouable** : pousser plusieurs fois le même fichier sans changement = NOOP (zéro impact)
- **Historique préservé** : un vrai changement crée une nouvelle ligne, l'ancienne est fermée
- **Pas de date à manipuler** : tu pousses ton état du jour, l'API range
- **Push partiel autorisé** : tu peux ne pousser QUE le `safety_stock_qty` sans toucher au reste — les autres champs gardent leur valeur active. Une cellule **vide** = « ne touche pas ».

→ Documentation détaillée : `docs/ADR-014-resources-units-scd2.md` (décision D3).

---

## 3. Colonnes

### 3.1 Obligatoires (2)

| Colonne | Type | Description |
|---|---|---|
| `item_external_id` | texte | FK vers `items` |
| `location_external_id` | texte | FK vers `locations` |

### 3.2 Lead times (3)

| Colonne | Type | Contrainte | Description |
|---|---|---|---|
| `lead_time_sourcing_days` | entier | ≥ 0 | Délai commande → expédition fournisseur |
| `lead_time_manufacturing_days` | entier | ≥ 0 | Délai de transformation interne |
| `lead_time_transit_days` | entier | ≥ 0 | Délai de transport vers le site (**voir limitation ci-dessous**) |

Note : `lead_time_total_days` est **calculé automatiquement** en base (somme des 3) — **n'apparaît pas** dans le fichier.

> **⚠️ Limitation `lead_time_transit_days`**
>
> Cette valeur est **un simple nombre** : pas de notion de mode (truck/air/rail/ocean),
> pas de carrier, pas de service level (standard/expedited/economy), pas de fenêtre
> min-max, pas de coût de transport.
>
> Ootils a déjà en base les tables `distribution_links` et `transportation_lanes`
> (migration 029) qui couvrent ces dimensions, **mais aucun endpoint d'ingestion
> n'est exposé en V1**. Quand ces tables ne sont pas peuplées, le moteur retombe
> sur `lead_time_transit_days` comme valeur unique.
>
> Une session dédiée formalisera `distribution_links.tsv` + `transportation_lanes.tsv`
> avec leur format propre. Voir la task **LANES-LATER**.

### 3.3 Safety stock (2)

| Colonne | Type | Contrainte | Description |
|---|---|---|---|
| `safety_stock_qty` | décimal | ≥ 0 | Stock de sécurité exprimé en unités |
| `safety_stock_days` | décimal | ≥ 0 | Stock de sécurité exprimé en jours de couverture |

Les deux peuvent coexister, c'est au modèle planning d'arbitrer (typiquement on en utilise un seul à la fois).

### 3.4 Reorder / lot sizing (4)

| Colonne | Type | Contrainte | Description |
|---|---|---|---|
| `reorder_point_qty` | décimal | ≥ 0 | Point de commande (ROP) en unités |
| `min_order_qty` | décimal | > 0 | MOQ — quantité minimum par commande |
| `max_order_qty` | décimal | > 0 | Quantité maximum par commande |
| `order_multiple` | décimal | > 0 | Multiple de commande indivisible (palette = 60, etc.) |

### 3.5 Politique planning (4)

| Colonne | Type | Domaine | Description |
|---|---|---|---|
| `lot_size_rule` | enum | `LOTFORLOT` \| `FIXED_QTY` \| `EOQ` \| `POQ` \| `MIN_MAX` \| `MULTIPLE` | Règle de tailles de lot. Défaut DB : `LOTFORLOT`. |
| `planning_horizon_days` | entier | > 0 | Horizon de planification. Défaut DB : 90. |
| `is_make` | booléen | `true` \| `false` | `true` = fabriqué en interne, `false` = acheté. Défaut DB : `false`. |
| `preferred_supplier_external_id` | texte | FK suppliers | Fournisseur préféré pour cet article × site (optionnel). |

### 3.6 Extensions APICS (7, V1.1 — optionnel)

Pour cas avancés. Cellules vides = ignorées.

| Colonne | Type | Contrainte | Description |
|---|---|---|---|
| `economic_order_qty` | décimal | > 0 | EOQ pré-calculé (utilisé si `lot_size_rule = EOQ`) |
| `lot_size_poq_periods` | entier | > 0 | Nb de périodes pour `lot_size_rule = POQ` |
| `order_multiple_qty` | décimal | > 0 | Quantité du multiple (alias variante `MULTIPLE`) |
| `frozen_time_fence_days` | entier | ≥ 0 | Time fence gelé (pas de modification possible) |
| `slashed_time_fence_days` | entier | > 0 | Time fence négociable (modifications limitées) |
| `forecast_consumption_strategy` | enum | `max_only` \| `consume_forward` \| `consume_backward` \| `consume_both` | Stratégie de consommation forecast par order |
| `consumption_window_days` | entier | > 0 | Fenêtre de consommation forecast |

---

## 4. Lot size rules — quoi choisir ?

| Règle | Sémantique | Quand l'utiliser |
|---|---|---|
| `LOTFORLOT` | Commande = exactement le besoin net | Articles peu coûteux à commander, sur-mesure |
| `FIXED_QTY` | Commande = quantité fixe (`min_order_qty`) à chaque déclenchement | Articles à MOQ contractuel rigide |
| `EOQ` | Quantité économique de commande (formule Wilson, voir `economic_order_qty`) | Articles à demande stable, coût de commande significatif |
| `POQ` | Période Order Quantity — commande pour N périodes (`lot_size_poq_periods`) | Articles consolidés sur N semaines |
| `MIN_MAX` | Commande pour atteindre `max_order_qty` quand on touche `reorder_point_qty` | Articles avec espace stockage borné |
| `MULTIPLE` | Commande au multiple supérieur de `order_multiple` du besoin net | Conditionnement palette, multiples industriels |

Défaut : `LOTFORLOT`.

---

## 5. Exemples

### 5.1 Minimum vital — lead times seuls

```
item_external_id	location_external_id	lead_time_sourcing_days	lead_time_manufacturing_days	lead_time_transit_days
COMP-MOTOR-24V	PLANT-LYON	10	0	2
SUB-HOUSING-100	PLANT-LYON	0	5	0
```

→ Pour `COMP-MOTOR-24V @ PLANT-LYON`, le total est 12 jours (10 sourcing + 2 transit). C'est un article acheté.
→ Pour `SUB-HOUSING-100 @ PLANT-LYON`, le total est 5 jours (manufacturing seul). C'est un article fabriqué.

### 5.2 Lead times + safety stock + MOQ

```
item_external_id	location_external_id	lead_time_sourcing_days	lead_time_manufacturing_days	lead_time_transit_days	safety_stock_qty	min_order_qty	order_multiple	is_make	preferred_supplier_external_id
COMP-MOTOR-24V	PLANT-LYON	7	0	3	50	20	1	false	SUP-MOTOR-01
COMP-IMPELLER-100	PLANT-LYON	5	0	2	100	30	10	false	SUP-MECH-01
SUB-HOUSING-100	PLANT-LYON	0	5	0	20	1	1	true	
FG-APU-100	DC-LILLE	0	3	1	10	1	1	true	
```

Lecture :
- Le motor est acheté chez SUP-MOTOR-01, lead time 10j (7 sourcing + 3 transit), SS=50 unités, MOQ=20
- Le housing est fabriqué sur place (PLANT-LYON), lead time 5j manufacturing, SS=20
- La pompe finie FG-APU-100 est assemblée à DC-LILLE en 4j (3 mfg + 1 transit ?)

### 5.3 Avec règle lot sizing avancée

```
item_external_id	location_external_id	lead_time_sourcing_days	safety_stock_days	min_order_qty	max_order_qty	lot_size_rule	economic_order_qty	preferred_supplier_external_id
RAW-STEEL-50	PLANT-LYON	21	7	1000	5000	EOQ	2500	SUP-STEEL-DE
```

→ Acier : lead time 21j, SS 7 jours de couverture, EOQ pré-calculé à 2500 kg, fournisseur préféré SteelCo.

### 5.4 Push partiel — modification ciblée uniquement

Si tu veux **seulement** changer le SS d'un couple sans toucher au reste :

```
item_external_id	location_external_id	safety_stock_qty
COMP-MOTOR-24V	PLANT-LYON	75
```

→ Seul `safety_stock_qty` est poussé. Lead times, MOQ, lot rule, etc. de la ligne active restent intacts.

### 5.5 Cas invalides

```
item_external_id	location_external_id	lead_time_sourcing_days	min_order_qty	lot_size_rule
ITEM-UNKNOWN	PLANT-LYON	10	20	LOTFORLOT       ← item inconnu → 422
COMP-MOTOR-24V	LOC-XYZ	10	20	LOTFORLOT          ← location inconnue → 422
COMP-MOTOR-24V	PLANT-LYON	-5	20	LOTFORLOT     ← lead_time_sourcing_days < 0 → 422
COMP-MOTOR-24V	PLANT-LYON	10	0	LOTFORLOT      ← min_order_qty doit être > 0 → 422
COMP-MOTOR-24V	PLANT-LYON	10	20	WEEKLY        ← lot_size_rule inconnu → 422
```

---

## 6. Comportement à l'ingestion

### 6.1 Identification

Clé business : couple (`item_external_id`, `location_external_id`).

### 6.2 Validation FK (avant toute écriture)

L'API résout en batch les 3 FK :
- `item_external_id` doit exister dans `items`
- `location_external_id` doit exister dans `locations`
- `preferred_supplier_external_id` (si fourni) doit exister dans `suppliers`

Toute FK manquante = batch entier rejeté (422).

### 6.3 Décision SCD2 par ligne

Pour chaque ligne, l'API :
1. Cherche la ligne active actuelle (`effective_to IS NULL`) pour ce couple
2. Compare champ par champ (uniquement les champs **présents** dans le payload — push partiel)
3. Décide : `NOOP` / `UPDATED_INPLACE` (même jour) / `ROTATED` (autre jour)

### 6.4 Pipeline

```
data/inbox/item_planning_params.tsv
        │
        ▼
   parse TSV (cellules vides = champ absent)
        │
        ▼
   build payload (omet les cellules vides — push partiel)
        │
        ▼
   POST /v1/ingest/planning-params
        │
        ├─ FK manquante ──► 422 → data/rejected/
        │
        ▼
   SCD2 par ligne : noop | inplace | rotated
        │
        ▼
   data/processed/ + rapport JSON détaillant action par ligne
```

---

## 7. Ordre de chargement

```
1. items.tsv               ← ✅
2. locations.tsv           ← ✅
3. suppliers.tsv           ← ✅ (nécessaire pour preferred_supplier_external_id)
4. supplier_items.tsv      ← ✅ (optionnel ici mais utile pour multi-sourcing)
5. item_planning_params.tsv ← ici
6. bom_*.tsv               ← après
```

---

## 8. Quelques notes opérationnelles

- **Cellule vide ≠ valeur null forcée**. Une cellule vide veut dire « ne touche pas à ce champ ». Pour forcer un null (rare), ce n'est pas supporté par le push partiel V1 — il faudrait une API dédiée.
- **`is_preferred` côté `supplier_items`** vs **`preferred_supplier_external_id` côté `item_planning_params`** : les deux peuvent coexister. La règle est : `item_planning_params.preferred_supplier_external_id` **override** ce qui est dans `supplier_items.is_preferred`.
- **Re-charger le même fichier 2x** = NOOP la deuxième fois (aucun changement détecté). Très utile pour idempotency.

---

## 9. Validation rapide

```bash
python scripts/ingest_file.py data/inbox/item_planning_params.tsv --dry-run
```

Le dry-run **affiche par ligne** quelle action serait prise (noop / inplace / rotated + champs qui changeraient) — sans rien écrire en base.
