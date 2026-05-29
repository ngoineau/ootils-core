# Format de fichier — `suppliers.tsv`

> Fichier de master data **fournisseurs** déposé dans `data/inbox/` pour ingestion automatique.
> Aligné avec le contrat canonique V1.

---

## 1. Caractéristiques du fichier

| Propriété | Valeur |
|---|---|
| **Nom** | `suppliers.tsv` (exact, sensible à la casse) |
| **Format** | TSV — Tab-Separated Values |
| **Encodage** | UTF-8 (BOM toléré) |
| **Délimiteur** | tabulation (`\t`) |
| **Header** | ligne 1 obligatoire |
| **Taille max** | 10 MB |
| **Lignes max** | ~50 000 |

---

## 2. Colonnes

**Ordre figé par le contrat canonique** : `external_id`, `name`, `country`, `status`, `lead_time_days`, `reliability_score`.

| # | Colonne | Obligatoire | Type | Domaine / format | Description |
|---|---|---|---|---|---|
| 1 | `external_id` | **oui** | texte | non-vide, max 128, **unique dans le fichier** | Code fournisseur business (ERP). Clé d'upsert. |
| 2 | `name` | **oui** | texte | non-vide, max 512 | Raison sociale / libellé fournisseur. |
| 3 | `country` | non | texte | ISO 3166-1 alpha-2 (`FR`, `DE`, `IT`...) | Pays du fournisseur. |
| 4 | `status` | non | enum | `active` \| `inactive` \| `blocked` | Statut commercial. Défaut : `active`. |
| 5 | `lead_time_days` | non | entier | > 0 si fourni | Lead time **de référence** du fournisseur (calendaire). Voir §3 ci-dessous. |
| 6 | `reliability_score` | non | décimal | dans `[0.0, 1.0]` | Score de fiabilité. 1.0 = livraison à l'heure systématique, 0.0 = jamais à l'heure. |

---

## 3. Rôle de `lead_time_days` ici

Important pour comprendre comment Ootils traite les lead times :

```
Hiérarchie de résolution (du moins prioritaire au plus prioritaire) :
                                                                       
  1. suppliers.lead_time_days            ← ICI (« lead time par défaut »)  
                                                                       
  2. supplier_items.lead_time_days       ← override par paire (fournisseur × article)
                                                                       
  3. item_planning_params.lead_time_*    ← override par (article × site), versionné SCD2
                                            décomposé en sourcing/manufacturing/transit
```

**Au moment de planifier, Ootils utilise le plus prioritaire disponible.** Si `item_planning_params` a une valeur pour le couple (article × site), elle gagne. Sinon Ootils descend vers `supplier_items`. Sinon vers `suppliers`. Sinon défaut système.

→ La colonne `lead_time_days` ici sert de **filet de sécurité** : ce sera utilisé pour les articles qui n'ont pas de lead time spécifique défini ailleurs. C'est aussi le « lead time contractuel » général du fournisseur.

→ Niveau 3 (`item_planning_params.tsv`) viendra après et écrasera ces valeurs là où c'est plus précis.

---

## 4. `reliability_score` — sémantique

| Valeur | Interprétation |
|---|---|
| `1.00` | Fournisseur parfait, toujours à l'heure |
| `0.95` | OTIF ~95 % |
| `0.80` | Retards récurrents, à surveiller |
| `< 0.50` | Fournisseur défaillant, candidat au blocage |
| `null` / absent | Non scoré (pas encore d'historique) |

Sera consommé par le **Supplier Watcher** (W03) et le **Supplier Agent** (G04) pour ranker les choix de sourcing.

---

## 5. Exemples

### 5.1 Minimum vital

```
external_id	name
SUP-MOTOR-01	MotorWorks Europe
```

→ Crée le fournisseur avec status `active`, pas de pays, pas de lead time, pas de score.

### 5.2 Exemple typique (issu du dataset minimal du repo)

```
external_id	name	country	status	lead_time_days	reliability_score
SUP-MOTOR-01	MotorWorks Europe	DE	active	10	0.96
SUP-MECH-01	Precision Mechanics SAS	FR	active	7	0.94
```

### 5.3 Exemple plus complet (cas réel pilote)

```
external_id	name	country	status	lead_time_days	reliability_score
SUP-MOTOR-01	MotorWorks Europe	DE	active	10	0.96
SUP-MECH-01	Precision Mechanics SAS	FR	active	7	0.94
SUP-STEEL-DE	SteelCo Germany	DE	active	21	0.88
SUP-BOLT-FR	Bolt France	FR	active	5	0.99
SUP-LEGACY-01	OldVendor Co	IT	blocked	45	0.42
```

### 5.4 Cas invalides

```
external_id	name	country	status	lead_time_days	reliability_score
	MotorWorks	DE	active	10	0.96                      ← external_id vide → 422
SUP-X		DE	active	10	0.96                          ← name vide → 422
SUP-X	MotorWorks	DE	suspended	10	0.96               ← status 'suspended' inconnu → 422
SUP-X	MotorWorks	DE	active	0	0.96                   ← lead_time_days = 0 (doit être > 0) → 422
SUP-X	MotorWorks	DE	active	10	1.5                    ← reliability_score > 1.0 → 422
SUP-X	MotorWorks	DE	active	10	-0.1                   ← reliability_score < 0.0 → 422
```

---

## 6. Comportement à l'ingestion

### 6.1 Identification

Clé business : `external_id`.
- Existe en base → UPDATE (les 5 autres champs écrasés)
- N'existe pas → INSERT (nouveau `supplier_id` UUID interne)

### 6.2 Atomicité

Identique aux autres entités : un seul défaut → tout le batch rejeté, fichier déplacé vers `data/rejected/`.

### 6.3 Notes importantes

- **Ne crée PAS automatiquement de `supplier_virtual` location** correspondante. Si tu veux que ce fournisseur apparaisse comme nœud dans le graphe, il faut **aussi** créer une ligne `supplier_virtual` dans `locations.tsv` (cf. `docs/contracts/locations/format-locations-tsv.md`).
- Convention recommandée : `SUP-ACME-001` (suppliers) ↔ `SVL-ACME` (supplier_virtual location). Le lien est fait manuellement / par l'agent Supply pour l'instant ; pas de FK automatique en V1.

---

## 7. Ordre de chargement

```
1. items.tsv               ← ✅
2. locations.tsv           ← ✅
3. suppliers.tsv           ← ici
4. supplier_items.tsv      ← nécessite items + suppliers
5. item_planning_params.tsv ← nécessite items + locations + suppliers (FK preferred_supplier)
6. bom_header.tsv + bom_components.tsv
```

`suppliers` doit être chargé avant `supplier_items` (qui matérialise la paire fournisseur × article) et avant `item_planning_params` (qui peut référencer un `preferred_supplier`).

---

## 8. Validation rapide

```bash
python scripts/ingest_file.py data/inbox/suppliers.tsv --dry-run
```
