# `data/inbox/` — boîte de dépôt fichiers à ingérer

> 📘 **Spécifications techniques consolidées** des 11 entités ingestibles : voir
> [`docs/contracts/TSV-FILES-SPEC.md`](../../docs/contracts/TSV-FILES-SPEC.md) —
> tableau récapitulatif de toutes les colonnes, types, contraintes, ordre de chargement et codes erreur.

## Usage

Deux chemins selon le besoin :

### Chemin API (incrémental, avec DQ + idempotency)

```bash
python scripts/ingest_file.py data/inbox/items.tsv
```

→ Pour les **mises à jour incrémentales** (1-1000 lignes), agents, push quotidien ERP.
Validation Pydantic, DQ engine post-load, idempotency via header, audit batch tracking.

### Chemin bulk (chargement initial / refresh massif)

```bash
python scripts/bulk_ingest.py data/inbox/                  # batch tout dans l'ordre
python scripts/bulk_ingest.py data/inbox/items.tsv         # un fichier seul
```

→ Pour les **chargements massifs** (>1000 lignes). COPY+UPSERT direct, ~9 000 rows/s.
Pas de DQ engine, pas de batch tracking. Mode tolérant FK (skippe les lignes orphelines).

## Fichiers supportés (11 entités)

| Nom de fichier | Entité Ootils | Endpoint cible | Spec format |
|---|---|---|---|
| `items.tsv` | Articles / SKU | `POST /v1/ingest/items` | [`format-items-tsv.md`](../../docs/contracts/items/format-items-tsv.md) |
| `locations.tsv` | Sites / lieux | `POST /v1/ingest/locations` | [`format-locations-tsv.md`](../../docs/contracts/locations/format-locations-tsv.md) |
| `suppliers.tsv` | Fournisseurs | `POST /v1/ingest/suppliers` | [`format-suppliers-tsv.md`](../../docs/contracts/suppliers/format-suppliers-tsv.md) |
| `supplier_items.tsv` | Conditions appro | `POST /v1/ingest/supplier-items` | [`format-supplier-items-tsv.md`](../../docs/contracts/supplier_items/format-supplier-items-tsv.md) |
| `item_planning_params.tsv` | Paramètres planning (SCD2) | `POST /v1/ingest/planning-params` | [`format-item-planning-params-tsv.md`](../../docs/contracts/item_planning_params/format-item-planning-params-tsv.md) |
| `on_hand.tsv` | Stock disponible | `POST /v1/ingest/on-hand` | [`format-on-hand-tsv.md`](../../docs/contracts/on_hand/format-on-hand-tsv.md) |
| `purchase_orders.tsv` | Commandes d'achat | `POST /v1/ingest/purchase-orders` | [`format-purchase-orders-tsv.md`](../../docs/contracts/purchase_orders/format-purchase-orders-tsv.md) |
| `customer_orders.tsv` | Commandes clients | `POST /v1/ingest/customer-orders` | [`format-customer-orders-tsv.md`](../../docs/contracts/customer_orders/format-customer-orders-tsv.md) |
| `forecasts.tsv` | Prévisions de demande | `POST /v1/ingest/forecast-demand` | [`format-forecasts-tsv.md`](../../docs/contracts/forecasts/format-forecasts-tsv.md) |
| `transfers.tsv` | Transferts inter-sites | `POST /v1/ingest/transfers` | [`format-transfers-tsv.md`](../../docs/contracts/transfers/format-transfers-tsv.md) |
| `bom_header.tsv` + `bom_components.tsv` | BOM (bundle) | `POST /v1/ingest/bom` | [`format-bom-tsv.md`](../../docs/contracts/bom/format-bom-tsv.md) |

⚠️ **Cas spécial BOM** : lancer le script avec `bom_header.tsv` ; il chargera automatiquement `bom_components.tsv` à côté.

## Ordre de chargement (FK obligatoires)

```bash
# Master data — racine
python scripts/bulk_ingest.py data/inbox/items.tsv
python scripts/bulk_ingest.py data/inbox/locations.tsv
python scripts/bulk_ingest.py data/inbox/suppliers.tsv

# Master data — dépendantes
python scripts/bulk_ingest.py data/inbox/supplier_items.tsv          # items + suppliers
python scripts/bulk_ingest.py data/inbox/item_planning_params.tsv    # items + locations + suppliers

# Transactionnel
python scripts/bulk_ingest.py data/inbox/on_hand.tsv                 # items + locations
python scripts/bulk_ingest.py data/inbox/purchase_orders.tsv         # items + locations + suppliers
python scripts/bulk_ingest.py data/inbox/customer_orders.tsv         # items + locations
python scripts/bulk_ingest.py data/inbox/forecasts.tsv               # items + locations
python scripts/bulk_ingest.py data/inbox/transfers.tsv               # items + locations
python scripts/bulk_ingest.py data/inbox/bom_header.tsv              # items
```

Ou tout d'un coup :
```bash
python scripts/bulk_ingest.py data/inbox/   # mode batch (ordre canonique appliqué)
```

## Cycle de vie d'un fichier (chemin API uniquement)

```
data/inbox/items.tsv      ← tu déposes ici
        │
        ▼
   script ingest_file.py
        │
        ├── succès ──► data/processed/items_YYYYMMDD_HHMMSS.tsv  + .report.json
        │
        └── erreur ──► data/rejected/items_YYYYMMDD_HHMMSS.tsv   + .report.json
```

Le `bulk_ingest.py` ne déplace pas les fichiers (laisse à l'opérateur).

## Templates

Voir `data/templates/` pour les fichiers vides avec en-têtes prêts à copier.

## Politique git

Les fichiers déposés ici **ne sont pas versionnés** (voir `.gitignore`). Seuls
ce README et les `.gitkeep` sont commités pour préserver la structure de
dossiers.
