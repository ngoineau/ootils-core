# Format de fichier — `items.tsv`

> Fichier de master data **articles / SKU** déposé dans `data/inbox/` pour ingestion automatique.
> Aligné avec le contrat canonique V1 (`data-input-canonique-v1-template-tsv.zip` du repo).

---

## 1. Caractéristiques du fichier

| Propriété | Valeur |
|---|---|
| **Nom** | `items.tsv` (exact, sensible à la casse) |
| **Format** | TSV — Tab-Separated Values |
| **Encodage** | UTF-8 (sans BOM recommandé, BOM toléré) |
| **Délimiteur** | tabulation (`\t`) — **pas** de virgule, **pas** de point-virgule |
| **Fin de ligne** | LF (`\n`) ou CRLF (`\r\n`), les deux sont acceptés |
| **Header** | ligne 1 obligatoire avec les noms exacts des colonnes ci-dessous |
| **Encadrement** | aucun guillemet autour des valeurs (la tabulation est le séparateur) |
| **Lignes vides** | autorisées (ignorées) |
| **Lignes de commentaire** | non supportées |
| **Taille max** | 10 MB par fichier (limite ingest API) |
| **Lignes max** | ~50 000 lignes par fichier (au-delà, splitter) |

---

## 2. Colonnes

**Ordre conseillé** : `external_id`, `name`, `item_type`, `uom`, `status`.
(L'ordre n'est pas imposé : ce qui compte c'est le header. Mais le respecter facilite la lecture humaine.)

| # | Colonne | Obligatoire | Type | Domaine / format | Description |
|---|---|---|---|---|---|
| 1 | `external_id` | **oui** | texte | non-vide, max 128 caractères, **unique dans le fichier** | Identifiant business de l'article (code SKU ERP). Sert de clé d'upsert. |
| 2 | `name` | **oui** | texte | non-vide, max 512 caractères | Libellé / description de l'article. |
| 3 | `item_type` | optionnel | enum | `finished_good` \| `component` \| `raw_material` \| `semi_finished` | Type d'article. Défaut si absent : `finished_good`. |
| 4 | `uom` | optionnel | texte | code unité (max 16 caractères) | Unité de mesure de base. Défaut si absent : `EA` (each / unité). Codes usuels : `EA`, `KG`, `L`, `M`, `BOX`, `PAL`. |
| 5 | `status` | optionnel | enum | `active` \| `obsolete` \| `phase_out` | Statut commercial. Défaut si absent : `active`. |

### Notes de domaine

- **`item_type`** :
  - `finished_good` — produit fini vendu au client
  - `semi_finished` — sous-ensemble produit intermédiaire (anciennement appelé « subassembly » dans certains exemples — utiliser `semi_finished`)
  - `component` — composant acheté/fabriqué intégré dans un BOM
  - `raw_material` — matière première
- **`uom`** : doit exister dans la table `uom_conversions` ou être un code standard. UoM inconnu = rejet de la ligne en V1.
- **`status = obsolete`** : l'article reste lisible mais ne peut plus être planifié sur de nouvelles activités. `phase_out` = en cours de retrait.

### Cellule vide vs valeur par défaut

- Cellule **vide** sur colonne optionnelle → valeur par défaut appliquée (`finished_good`, `EA`, `active`).
- Cellule **vide** sur colonne obligatoire (`external_id`, `name`) → la ligne est rejetée, **et tout le batch est rejeté** (politique all-or-nothing actuelle).

---

## 3. Exemples

### 3.1 Fichier minimal valide (2 articles, 1 ligne header + 2 lignes data)

```
external_id	name	item_type	uom	status
FG-APU-100	AquaPump 100	finished_good	EA	active
COMP-MOTOR-24V	24V Motor	component	EA	active
```

### 3.2 Fichier avec valeurs par défaut implicites

```
external_id	name
FG-APU-100	AquaPump 100
COMP-MOTOR-24V	24V Motor
```
→ équivalent à mettre `finished_good`, `EA`, `active` sur les deux lignes.

### 3.3 Fichier complet (exemple 5 articles, cas réel)

```
external_id	name	item_type	uom	status
FG-APU-100	AquaPump 100	finished_good	EA	active
FG-APU-200	AquaPump 200 (HP)	finished_good	EA	active
SUB-HOUSING-100	Housing Assembly 100	semi_finished	EA	active
COMP-MOTOR-24V	24V DC Motor	component	EA	active
RAW-STEEL-50	Acier brut 50kg	raw_material	KG	active
```

### 3.4 Cas invalides — seront rejetés

```
external_id	name	item_type	uom	status
	AquaPump 100	finished_good	EA	active            ← external_id vide → 422
FG-APU-100		finished_good	EA	active            ← name vide → 422
FG-APU-100	AquaPump 100	subassembly	EA	active    ← item_type 'subassembly' inconnu → 422
FG-APU-100	AquaPump 100	finished_good	EA	deleted   ← status 'deleted' inconnu → 422
```

---

## 4. Comportement à l'ingestion

### 4.1 Identification

- Clé business : `external_id`
- Si `external_id` **existe déjà** en base → **UPDATE** (les 4 autres champs sont écrasés)
- Si `external_id` **n'existe pas** → **INSERT** (nouvel item créé avec un `item_id` UUID interne)

### 4.2 Atomicité (all-or-nothing)

Si **une seule ligne** du fichier est invalide (champ obligatoire vide, enum hors domaine, doublon `external_id` dans le fichier...) :
- **rien n'est écrit en base**
- le fichier est déplacé dans `data/rejected/`
- un rapport `<nom>.report.json` est généré à côté avec la liste des erreurs

### 4.3 Pipeline standard

```
data/inbox/items.tsv
        │
        ▼
   parse TSV (tab, UTF-8, header ligne 1)
        │
        ▼
   validation Pydantic (types, enums, non-vides)
        │
        ├─── erreur ──► data/rejected/items_YYYYMMDD_HHMMSS.tsv
        │               + items_YYYYMMDD_HHMMSS.report.json
        │
        ▼ OK
   appel API en process : POST /v1/ingest/items
        │
        ▼
   upsert dans table `items` + DQ engine
        │
        ▼
   data/processed/items_YYYYMMDD_HHMMSS.tsv
   + items_YYYYMMDD_HHMMSS.report.json
```

---

## 5. Conventions

### 5.1 Que faire après dépôt ?

Tu déposes le fichier dans `data/inbox/items.tsv` et tu lances le script :

```bash
python scripts/ingest_file.py data/inbox/items.tsv
```

Le script s'occupe du reste (parse, valide, charge, archive).

### 5.2 Replay d'un fichier déjà traité

Re-déposer le même fichier (même contenu) → idempotent : aucun changement en base (toutes les lignes seront détectées comme `updated` no-op).
Pour forcer la création d'un nouveau batch, changer le nom du fichier ou ajouter une ligne.

### 5.3 Suppression d'articles

**Hors scope V1** : `items.tsv` ne sert qu'à créer ou mettre à jour. Pour désactiver un article, passer son `status` à `obsolete` ou `phase_out`. Aucune suppression physique en V1.

---

## 6. Évolution future (hors V1)

Champs envisagés pour V1.1 (cf. `docs/contracts/items/v1.md` §6) :
- Classification : `family`, `category`, `abc_class`, `xyz_class`, `lifecycle_stage`
- Physique : `net_weight_kg`, `volume_m3`, `pack_size`...
- Réglementaire : `hs_code`, `country_of_origin`...
- Extensions libres : `attrs` (JSON), `tags`

Ces ajouts seront **rétro-compatibles** : un fichier V1.0 (5 colonnes) restera valide quand V1.1 sera publié.

---

## 7. Validation rapide d'un fichier

Avant de lancer l'ingestion réelle, on peut faire un **dry-run** :

```bash
python scripts/ingest_file.py data/inbox/items.tsv --dry-run
```

→ Valide tout, ne touche pas à la base, ne déplace pas le fichier. Affiche le rapport.
