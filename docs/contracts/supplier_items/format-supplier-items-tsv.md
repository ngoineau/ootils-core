# Format de fichier — `supplier_items.tsv`

> Fichier des **conditions d'approvisionnement** par paire fournisseur × article (lead time, MOQ, prix, fournisseur préféré).
> Aligné avec le contrat canonique V1.

---

## 1. Caractéristiques du fichier

| Propriété | Valeur |
|---|---|
| **Nom** | `supplier_items.tsv` (exact) |
| **Format** | TSV |
| **Encodage** | UTF-8 (BOM toléré) |
| **Délimiteur** | tabulation (`\t`) |
| **Header** | ligne 1 obligatoire |
| **Endpoint** | `POST /v1/ingest/supplier-items` (notez le tiret) |

---

## 2. Colonnes

**Ordre figé par le contrat canonique** : `supplier_external_id`, `item_external_id`, `lead_time_days`, `currency`, `moq`, `unit_cost`, `is_preferred`.

| # | Colonne | Obligatoire | Type | Domaine / format | Description |
|---|---|---|---|---|---|
| 1 | `supplier_external_id` | **oui** | texte | doit exister dans `suppliers` | Code fournisseur (clé FK vers `suppliers.tsv`). |
| 2 | `item_external_id` | **oui** | texte | doit exister dans `items` | Code article (clé FK vers `items.tsv`). |
| 3 | `lead_time_days` | **oui** | entier | > 0 | Lead time pour cette paire fournisseur × article. **Override le `suppliers.lead_time_days` global**. |
| 4 | `currency` | non | texte | code ISO 4217 (`EUR`, `USD`, `GBP`...) | Devise du prix. Défaut : `EUR`. |
| 5 | `moq` | non | décimal | > 0 si fourni | Minimum Order Quantity dans l'UoM de l'article. |
| 6 | `unit_cost` | non | décimal | ≥ 0 | Prix unitaire dans la devise. |
| 7 | `is_preferred` | non | booléen | `true` \| `false` (insensible casse) | Fournisseur préféré pour cet article. Défaut : `false`. |

---

## 3. Clé unique et upsert

| Aspect | Comportement |
|---|---|
| **Clé business** | couple (`supplier_external_id`, `item_external_id`) |
| **Existe en base** | UPDATE des 5 autres colonnes |
| **N'existe pas** | INSERT (nouveau `supplier_item_id` UUID interne) |
| **Validation FK** | les 2 codes sont résolus contre `suppliers` et `items` ; **les deux master data doivent être chargés avant**. |

---

## 4. Logique multi-sourcing

Quand un article peut être fourni par plusieurs fournisseurs :

```
supplier_external_id    item_external_id      lead_time_days  is_preferred
SUP-MOTOR-01            COMP-MOTOR-24V        10              true       ← fournisseur préféré
SUP-MOTOR-ALT           COMP-MOTOR-24V        14              false      ← fournisseur secondaire (backup)
```

Sémantique :
- Plusieurs lignes par article = sourcing alternatif disponible
- **Un seul `is_preferred = true` par article** recommandé (non bloqué techniquement en V1, mais incohérent ; à surveiller par le DQ Watcher)
- Les agents (Expedite, Substitution, Supplier Switch) utilisent ces alternatives pour les recommandations

---

## 5. Hiérarchie de résolution des lead times (rappel)

```
1. suppliers.lead_time_days            ← fallback global
2. supplier_items.lead_time_days       ← ICI (override par paire)
3. item_planning_params.lead_time_*    ← override par (article × site), SCD2
```

→ Cette ligne **override la valeur globale du fournisseur** pour cet article.
→ Sera elle-même overridée si `item_planning_params` définit une valeur pour le couple (article × site).

---

## 6. Exemples

### 6.1 Exemple typique (issu du dataset minimal du repo)

```
supplier_external_id	item_external_id	lead_time_days	currency	moq	unit_cost	is_preferred
SUP-MOTOR-01	COMP-MOTOR-24V	10	EUR	20	45.00	true
SUP-MECH-01	COMP-IMPELLER-100	7	EUR	30	8.50	true
SUP-MECH-01	SUB-HOUSING-100	12	EUR	10	22.00	true
```

### 6.2 Exemple multi-sourcing

```
supplier_external_id	item_external_id	lead_time_days	currency	moq	unit_cost	is_preferred
SUP-MOTOR-01	COMP-MOTOR-24V	10	EUR	20	45.00	true
SUP-MOTOR-ALT	COMP-MOTOR-24V	14	EUR	50	48.00	false
SUP-MECH-01	COMP-IMPELLER-100	7	EUR	30	8.50	true
SUP-BOLT-FR	COMP-IMPELLER-100	5	EUR	100	7.20	false
SUP-STEEL-DE	RAW-STEEL-50	21	EUR	1000	1.20	true
```

### 6.3 Minimum vital

```
supplier_external_id	item_external_id	lead_time_days
SUP-MOTOR-01	COMP-MOTOR-24V	10
```

→ MOQ, prix, devise, préférence laissés par défaut.

### 6.4 Cas invalides

```
supplier_external_id	item_external_id	lead_time_days	currency	moq	unit_cost	is_preferred
SUP-UNKNOWN	COMP-MOTOR-24V	10	EUR	20	45.00	true              ← supplier inconnu → 422
SUP-MOTOR-01	ITEM-XYZ	10	EUR	20	45.00	true                  ← item inconnu → 422
SUP-MOTOR-01	COMP-MOTOR-24V	0	EUR	20	45.00	true               ← lead_time_days = 0 → 422
SUP-MOTOR-01	COMP-MOTOR-24V		EUR	20	45.00	true                ← lead_time_days vide (obligatoire) → 422
SUP-MOTOR-01	COMP-MOTOR-24V	10	EUR	-5	45.00	true              ← moq négatif → 422
SUP-MOTOR-01	COMP-MOTOR-24V	10	EUR	20	45.00	yes                ← is_preferred 'yes' non parsable → 422
```

---

## 7. Sémantique `is_preferred`

Valeurs acceptées (insensible à la casse) :
- `true`, `1`, `yes`, `y`, `t` → `true`
- `false`, `0`, `no`, `n`, `f`, vide → `false`

(Le script normalise avant envoi à l'API.)

---

## 8. Devises supportées

Pas de validation du code devise contre une whitelist en V1. Tout code ISO 4217 sur 3 lettres est accepté. Cohérence à surveiller par DQ Watcher (par exemple, plusieurs devises pour le même fournisseur).

Codes courants : `EUR`, `USD`, `GBP`, `CHF`, `JPY`, `CNY`.

---

## 9. Ordre de chargement

```
1. items.tsv               ← ✅
2. locations.tsv           ← ✅
3. suppliers.tsv           ← ✅
4. supplier_items.tsv      ← ici (nécessite items + suppliers)
5. item_planning_params.tsv ← nécessite items + locations + suppliers
6. bom_header.tsv + bom_components.tsv
```

**Si tu charges `supplier_items.tsv` avant `items.tsv` ou `suppliers.tsv`** → toutes les lignes seront rejetées (FK introuvables).

---

## 10. Validation rapide

```bash
python scripts/ingest_file.py data/inbox/supplier_items.tsv --dry-run
```
