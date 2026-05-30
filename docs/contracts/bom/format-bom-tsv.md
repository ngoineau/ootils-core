# Format de fichier — `bom_header.tsv` + `bom_components.tsv` (bundle)

> Bill of Materials — décomposition d'un article fabriqué en ses composants.
> **Particularité** : ce contrat utilise **2 fichiers fusionnés** en un payload JSON par BOM, avec **N appels API** (1 BOM = 1 appel).
> Endpoint cible : `POST /v1/ingest/bom`.

---

## 1. Pourquoi 2 fichiers ?

Une BOM = 1 article parent + N composants. Si on mettait tout dans un seul fichier, on aurait soit :
- des lignes dupliquées (la version, l'effective_from répétées N fois)
- une dénormalisation pénible à maintenir

Donc le contrat canonique sépare :

| Fichier | Granularité | Contenu |
|---|---|---|
| `bom_header.tsv` | 1 ligne par (parent × version) | Métadonnées BOM (version, date d'effet) |
| `bom_components.tsv` | N lignes par (parent × version) | Liste des composants avec quantité, UoM, scrap |

Le script reconstitue ensuite, pour chaque BOM, un payload JSON consolidé :

```json
{
  "parent_external_id": "FG-APU-100",
  "bom_version": "1.0",
  "effective_from": "2026-04-01",
  "components": [
    {"component_external_id": "SUB-HOUSING-100", "quantity_per": 1, "uom": "EA", "scrap_factor": 0.0},
    {"component_external_id": "COMP-MOTOR-24V", "quantity_per": 1, "uom": "EA", "scrap_factor": 0.0},
    {"component_external_id": "COMP-IMPELLER-100", "quantity_per": 1, "uom": "EA", "scrap_factor": 0.02}
  ]
}
```

→ **1 BOM = 1 POST sur `/v1/ingest/bom`**. Si tu pousses 5 BOMs (5 parents distincts), le script fait **5 appels API**.

---

## 2. Caractéristiques des fichiers

Communs aux 2 fichiers :

| Propriété | Valeur |
|---|---|
| **Format** | TSV |
| **Encodage** | UTF-8 (BOM toléré) |
| **Délimiteur** | tabulation (`\t`) |
| **Header** | ligne 1 obligatoire |
| **Endpoint** | `POST /v1/ingest/bom` (1 BOM = 1 call) |

Particularité commande :

```bash
python scripts/ingest_file.py data/inbox/bom_header.tsv
```

→ Le script détecte le mode bundle, charge automatiquement `bom_components.tsv` à côté, et orchestre les N appels.

**Ne pas lancer directement `bom_components.tsv`** — le script refusera (il manque les métadonnées du header).

---

## 3. Colonnes — `bom_header.tsv`

| # | Colonne | Obligatoire | Type | Description |
|---|---|---|---|---|
| 1 | `parent_external_id` | **oui** | texte | Article parent (item fabriqué). FK `items`. |
| 2 | `bom_version` | non | texte | Version BOM (ex : `1.0`, `2.1`). Défaut : `1.0`. |
| 3 | `effective_from` | non | date ISO | Date d'effet de la BOM. Défaut : aujourd'hui. |

→ **Clé business** : couple (`parent_external_id`, `bom_version`).

---

## 4. Colonnes — `bom_components.tsv`

| # | Colonne | Obligatoire | Type | Domaine | Description |
|---|---|---|---|---|---|
| 1 | `parent_external_id` | **oui** | texte | FK header | Article parent — référence le header |
| 2 | `bom_version` | **oui** | texte | FK header | Version BOM — référence le header |
| 3 | `component_external_id` | **oui** | texte | FK `items` | Article composant |
| 4 | `quantity_per` | **oui** | décimal | > 0 | Quantité de composant pour produire **1 unité** de parent |
| 5 | `uom` | non | texte | code UoM | UoM du composant. Défaut : `EA` |
| 6 | `scrap_factor` | non | décimal | `[0.0, 1.0[` | Taux de rebut. `0.02` = 2 % de perte → on consomme `quantity_per × 1.02` pour chaque parent produit. Défaut : `0.0` |

→ Chaque ligne = un composant d'une BOM. Plusieurs lignes avec mêmes `(parent_external_id, bom_version)` = composants multiples du même BOM.

---

## 5. Validation cross-fichiers (fait par le script avant l'API)

Le script vérifie **avant tout appel API** :

1. Chaque `(parent_external_id, bom_version)` du `bom_components.tsv` **doit** exister dans `bom_header.tsv`
2. Sinon → erreur claire, **aucun appel API émis**, fichiers déplacés vers `data/rejected/`
3. Inversement : un header sans composant → BOM vide → erreur ou warning selon politique (en V1 : erreur, un BOM doit avoir au moins 1 composant)

---

## 6. Exemples

### 6.1 Cas minimal — 1 BOM, 3 composants

**`bom_header.tsv`**
```
parent_external_id	bom_version	effective_from
FG-APU-100	1.0	2026-04-01
```

**`bom_components.tsv`**
```
parent_external_id	bom_version	component_external_id	quantity_per	uom	scrap_factor
FG-APU-100	1.0	SUB-HOUSING-100	1	EA	0.00
FG-APU-100	1.0	COMP-MOTOR-24V	1	EA	0.00
FG-APU-100	1.0	COMP-IMPELLER-100	1	EA	0.02
```

→ Pour produire 1 AquaPump 100 il faut :
- 1 Housing Assembly (pas de rebut)
- 1 Motor 24V (pas de rebut)
- 1 Impeller — mais avec 2 % de rebut on commande **1.02** en pratique

→ 1 appel API : `POST /v1/ingest/bom` avec le payload consolidé.

### 6.2 Plusieurs BOMs (2 articles parents)

**`bom_header.tsv`**
```
parent_external_id	bom_version	effective_from
FG-APU-100	1.0	2026-04-01
FG-APU-200	1.0	2026-04-01
SUB-HOUSING-100	1.0	2026-04-01
```

**`bom_components.tsv`**
```
parent_external_id	bom_version	component_external_id	quantity_per	uom	scrap_factor
FG-APU-100	1.0	SUB-HOUSING-100	1	EA	0.00
FG-APU-100	1.0	COMP-MOTOR-24V	1	EA	0.00
FG-APU-100	1.0	COMP-IMPELLER-100	1	EA	0.02
FG-APU-200	1.0	SUB-HOUSING-100	1	EA	0.00
FG-APU-200	1.0	COMP-MOTOR-24V	2	EA	0.00
FG-APU-200	1.0	COMP-IMPELLER-100	1	EA	0.02
SUB-HOUSING-100	1.0	RAW-STEEL-50	2	KG	0.05
```

→ 3 BOMs définis :
- FG-APU-100 (3 composants)
- FG-APU-200 (3 composants, 2 motors car version High Power)
- SUB-HOUSING-100 (1 composant — c'est un semi-fini fabriqué à partir d'acier brut)

→ **3 appels API** émis par le script, un par BOM.

### 6.3 Cas invalides

```
# bom_header.tsv
parent_external_id	bom_version	effective_from
FG-APU-100	1.0	2026-04-01

# bom_components.tsv
parent_external_id	bom_version	component_external_id	quantity_per	uom	scrap_factor
FG-APU-100	1.0	COMP-MOTOR-24V	1	EA	0.00
FG-APU-200	1.0	COMP-MOTOR-24V	1	EA	0.00       ← parent FG-APU-200 n'est pas dans le header → erreur
```

```
# bom_components.tsv
parent_external_id	bom_version	component_external_id	quantity_per	uom	scrap_factor
FG-APU-100	1.0	COMP-MOTOR-24V	0	EA	0.00         ← quantity_per = 0 (doit être > 0) → 422
FG-APU-100	1.0	COMP-MOTOR-24V	1	EA	1.5          ← scrap_factor >= 1.0 (impossible 100% rebut) → 422
```

---

## 7. Niveau bas, LLC et explosion

Quand un BOM est ingéré, Ootils calcule automatiquement le **LLC (Low-Level Code)** de chaque article : la profondeur dans toutes les structures BOM où il apparaît.

Exemple :
```
FG-APU-100 (LLC=0, top-level)
  └── SUB-HOUSING-100 (LLC=1)
        └── RAW-STEEL-50 (LLC=2)
  └── COMP-MOTOR-24V (LLC=1)
  └── COMP-IMPELLER-100 (LLC=1)
```

Le LLC sert au MRP pour traiter les articles dans le bon ordre (de bas en haut). C'est géré côté serveur, pas dans le fichier.

L'endpoint `POST /v1/bom/explode` permet ensuite de calculer la **demande dépendante** : « pour fabriquer 50 AquaPump 100, il me faut combien de chaque composant ? »

---

## 8. Comportement à l'ingestion

### 8.1 Cycle de vie

| Évènement | Action |
|---|---|
| Première ingestion d'une BOM | INSERT (header + components + edges `bom_component`) |
| Ré-ingestion même (parent, version) | UPDATE / REPLACE complète des composants |
| Nouvelle version (`bom_version = 2.0`) | INSERT distinct ; les 2 versions coexistent |
| Changement de `effective_from` | UPDATE de la date d'effet |

### 8.2 Validation FK

Avant tout appel API, le script valide localement la cohérence header ↔ components.
Le serveur valide ensuite que tous les `parent_external_id` et `component_external_id` existent dans `items`.

### 8.3 Atomicité

- Validation header ↔ components : **all-or-nothing** localement (1 mismatch = aucun appel API émis)
- Appels API : **par BOM** — si BOM #2 sur 5 échoue, BOMs #1 et #2 sont partiellement écrits, BOM #2 erreur reporté, #3-#5 non émis. C'est un sujet à arbitrer (cf. §11)

---

## 9. Pipeline

```
data/inbox/bom_header.tsv  +  data/inbox/bom_components.tsv
                    │
                    ▼
       parse les 2 TSV + validation cross-fichier
                    │
                    ├─ mismatch ──► 422 → data/rejected/ (les 2 fichiers + report)
                    │
                    ▼ OK
       Pour chaque BOM (parent × version) :
           construire payload JSON consolidé
           POST /v1/ingest/bom
                    │
                    ▼
       Si tous les BOM OK ──► data/processed/ (les 2 fichiers + report par BOM)
       Sinon              ──► data/rejected/  (les 2 fichiers + report détaillé)
```

---

## 10. Ordre de chargement

```
1-10. master data + transactionnel  ← ✅
11. bom_*.tsv                       ← ICI (1 FK : items pour parent + composants)
```

Note : techniquement BOM peut être chargé en parallèle du transactionnel — il ne dépend que de `items`. L'ordre conseillé le met après pour clarté.

---

## 11. Limitations V1.0

| Manque | V1.1 envisagé |
|---|---|
| Pas de `routing_id` (BOM lié à un site de production) | V1.1 — actuellement BOM est globale, pas par site |
| Pas de `phantom_flag` (composant fictif, agrégateur) | V1.1 |
| Pas de `substitute_group` (composants substituables) | V1.1 — pour Substitution Agent |
| Pas de `co_product` / `by_product` | V2 |
| Atomicité multi-BOM : si BOM #2 échoue, BOM #1 reste écrit | V1.1 — politique transactionnelle à arbitrer |
| Pas de `effective_to` (date de fin de validité) | V1.1 |

---

## 12. Validation rapide

```bash
python scripts/ingest_file.py data/inbox/bom_header.tsv --dry-run
```

→ Le script parse les 2 fichiers, valide cross-fichier, et fait N dry-run API calls (pas d'écriture).
