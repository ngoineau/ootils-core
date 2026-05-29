# Format de fichier — `locations.tsv`

> Fichier de master data **sites / lieux** déposé dans `data/inbox/` pour ingestion automatique.
> Aligné avec le contrat canonique V1 (`data-input-canonique-v1-template-tsv.zip`).

---

## 1. Caractéristiques du fichier

| Propriété | Valeur |
|---|---|
| **Nom** | `locations.tsv` (exact, sensible à la casse) |
| **Format** | TSV — Tab-Separated Values |
| **Encodage** | UTF-8 (BOM toléré) |
| **Délimiteur** | tabulation (`\t`) |
| **Header** | ligne 1 obligatoire |
| **Encadrement** | aucun guillemet |
| **Taille max** | 10 MB |
| **Lignes max** | ~50 000 |

---

## 2. Colonnes

**Ordre conseillé** : `external_id`, `name`, `location_type`, `country`, `timezone`, `parent_external_id`.

| # | Colonne | Obligatoire | Type | Domaine / format | Description |
|---|---|---|---|---|---|
| 1 | `external_id` | **oui** | texte | non-vide, max 128, **unique dans le fichier** | Code site business (ERP). Clé d'upsert. |
| 2 | `name` | **oui** | texte | non-vide, max 512 | Libellé lisible du site. |
| 3 | `location_type` | non | enum | `plant` \| `dc` \| `warehouse` \| `supplier_virtual` \| `customer_virtual` | Type de site. Défaut : `dc`. |
| 4 | `country` | non | texte | ISO 3166-1 alpha-2 (`FR`, `DE`, `IT`...) | Pays. Utilisé pour calculs douaniers, calendars, fiscalité. |
| 5 | `timezone` | non | texte | IANA timezone (`Europe/Paris`, `America/New_York`) | Fuseau horaire. Sert au calcul des dates ouvrées. |
| 6 | `parent_external_id` | non | texte | `external_id` d'un autre site | Hiérarchie réseau (un WH enfant d'un plant, par exemple). Doit exister **dans le même fichier** ou déjà en base. |

---

## 3. Les 5 types de location

| Type | Rôle dans le graphe | Exemple |
|---|---|---|
| `plant` | Site qui **produit** (transforme des composants en produits) | Usine Lyon qui assemble les pompes |
| `warehouse` | Entrepôt **interne** qui stocke (sans production, sans vente directe) | Plateforme régionale Paris |
| `dc` | Distribution Center — stocke + **expédie au client** | DC Lille qui livre les clients Nord |
| `supplier_virtual` | Nœud virtuel représentant un fournisseur dans le graphe — point d'**entrée** des flux | « ACME Supplier IT » d'où arrivent les composants moteur |
| `customer_virtual` | Nœud virtuel représentant un client — point de **sortie** des flux | « Retail Nord » où partent les produits finis |

**Important** : `supplier_virtual` ≠ `suppliers` (master data). Le premier est un nœud du graphe (où passent les flux), le second est ton carnet d'adresses fournisseur (avec lead times, fiabilité, etc.). Voir `docs/contracts/items/format-items-tsv.md` pour les détails.

---

## 4. Hiérarchie via `parent_external_id`

Permet de modéliser la structure organique du réseau :

```
PLANT-LYON          (racine)
   └── WH-PARIS-01      (enfant de PLANT-LYON)
         └── DC-LILLE       (enfant de WH-PARIS-01)
```

- Référence un `external_id` qui doit exister **dans le même fichier** (ordre des lignes non important — la validation accepte les forward-refs) ou déjà en base
- Cellule vide ou absente = pas de parent (site racine)
- Pas de cycle autorisé
- Aucune contrainte de cohérence type/parent en V1 (un `supplier_virtual` enfant d'un `plant` est accepté techniquement)

---

## 5. Exemples

### 5.1 Minimum vital (1 site)

```
external_id	name
DC-LILLE	Lille DC
```

→ Crée un site `DC-LILLE` avec type par défaut `dc`, pas de pays, pas de timezone, pas de parent.

### 5.2 Réseau simple (3 sites hiérarchiques)

```
external_id	name	location_type	country	timezone	parent_external_id
PLANT-LYON	Lyon Plant	plant	FR	Europe/Paris	
WH-PARIS-01	Paris Warehouse	warehouse	FR	Europe/Paris	PLANT-LYON
DC-LILLE	Lille DC	dc	FR	Europe/Paris	WH-PARIS-01
```

### 5.3 Réseau complet avec fournisseurs et clients (pilote multi-site)

```
external_id	name	location_type	country	timezone	parent_external_id
PLANT-LYON	Lyon Plant	plant	FR	Europe/Paris	
WH-PARIS-01	Paris Warehouse	warehouse	FR	Europe/Paris	PLANT-LYON
DC-LILLE	Lille DC	dc	FR	Europe/Paris	WH-PARIS-01
SVL-ACME	ACME Supplier	supplier_virtual	IT	Europe/Rome	
SVL-BOLT-FR	Bolt France	supplier_virtual	FR	Europe/Paris	
SVL-STEEL-DE	SteelCo Germany	supplier_virtual	DE	Europe/Berlin	
CVL-RETAIL-NORD	Retail Nord	customer_virtual	FR	Europe/Paris	
```

### 5.4 Cas invalides — seront rejetés

```
external_id	name	location_type	country	timezone	parent_external_id
	Paris	dc	FR	Europe/Paris	                       ← external_id vide → 422
DC-PARIS		dc	FR	Europe/Paris	                       ← name vide → 422
DC-PARIS	Paris	region	FR	Europe/Paris	                ← location_type 'region' inconnu → 422
DC-PARIS	Paris	dc	FR	Europe/Paris	UNKNOWN-PARENT      ← parent_external_id inexistant → 422
```

---

## 6. Comportement à l'ingestion

### 6.1 Identification

Clé business : `external_id`.
- **Existe en base** → UPDATE (les 5 autres champs sont écrasés)
- **N'existe pas** → INSERT (nouveau `location_id` UUID interne)

### 6.2 Validation hiérarchique

Pour chaque ligne avec `parent_external_id` non-vide :
1. Vérifier que le parent existe **dans le payload** (autres lignes du même fichier), OU
2. Vérifier qu'il existe **déjà en base** (`SELECT location_id FROM locations WHERE external_id = ...`)

Si aucun des deux → ligne invalide → **tout le batch rejeté** (politique all-or-nothing).

### 6.3 Atomicité

Identique à items : un seul défaut → rien n'est écrit, fichier déplacé vers `data/rejected/` avec rapport.

### 6.4 Pipeline

```
data/inbox/locations.tsv
        │
        ▼
   parse TSV
        │
        ▼
   validation (Pydantic + parent_external_id check côté API)
        │
        ├── erreur ──► data/rejected/locations_YYYYMMDD_HHMMSS.tsv  + .report.json
        │
        ▼ OK
   POST /v1/ingest/locations
        │
        ▼
   upsert dans `locations`
        │
        ▼
   data/processed/locations_YYYYMMDD_HHMMSS.tsv  + .report.json
```

---

## 7. Conventions de nommage `external_id`

Recommandations (non imposées techniquement) :

| Préfixe | Type | Exemple |
|---|---|---|
| `PLANT-*` | plant | `PLANT-LYON`, `PLANT-MULHOUSE-2` |
| `WH-*` | warehouse | `WH-PARIS-01` |
| `DC-*` | dc | `DC-LILLE`, `DC-MARSEILLE` |
| `SVL-*` | supplier_virtual | `SVL-ACME`, `SVL-BOLT-FR` |
| `CVL-*` | customer_virtual | `CVL-RETAIL-NORD`, `CVL-KEY-ACCOUNT-CARREFOUR` |

→ Facilite la lecture humaine et permet aux agents Watcher de classifier par préfixe sans parser le `location_type`.

---

## 8. Ordre de chargement dans la séquence master data

```
1. items.tsv               ← d'abord (référencé par item_planning_params, supplier_items, etc.)
2. locations.tsv           ← ici (référencé par item_planning_params, on_hand, PO, transfers, etc.)
3. suppliers.tsv           ← après
4. supplier_items.tsv      ← après items + suppliers
5. item_planning_params.tsv ← après items + locations + suppliers
6. bom_header.tsv + bom_components.tsv ← après items
```

`locations.tsv` doit être chargé tôt car presque toutes les autres entités (`on_hand`, `purchase_orders`, `transfers`, `customer_orders`, `item_planning_params`) référencent une location.

---

## 9. Validation rapide d'un fichier

```bash
python scripts/ingest_file.py data/inbox/locations.tsv --dry-run
```
