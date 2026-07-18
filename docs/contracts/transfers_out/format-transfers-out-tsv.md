# Format de fichier — `transfers_<AAAAMMJJ>.tsv` (SORTANT)

> Fichier **sortant** des propositions de transfert inter-sites — recommandations `TRANSFER` (DRP, ADR-028) **déjà approuvées côté Ootils**.
> Produit par `engine/reporting/outbound_export.py` (ADR-042 décision 4, chantier PR-5a).
> Déposé dans `dropbox:ootils-outbox` par `scripts/deposit_outbox.sh`.
>
> ⚠️ **Attention nommage** : le fichier physique s'appelle bien `transfers_<AAAAMMJJ>.tsv` (`outbound_export.py:_filename("transfers", run_date)`) — préfixe **identique** à l'entrant `transfers.tsv` (`docs/contracts/transfers/format-transfers-tsv.md`). Ce dossier de contrat est nommé `transfers_out/` (et non `transfers/`, déjà pris par l'entrant) pour éviter toute confusion documentaire. Dans la pratique les deux fichiers sont **structurellement distincts** : le sortant est toujours daté (`transfers_20260718.tsv`) et vit dans **l'outbox**, l'entrant n'est pas daté (`transfers.tsv`) et vit dans **l'inbox** — deux dossiers différents, jamais mélangés. Voir §6 pour un comparatif colonne à colonne.

---

## 1. Caractéristiques du fichier

| Propriété | Valeur |
|---|---|
| **Nom** | `transfers_<AAAAMMJJ>.tsv` (daté du jour du run d'export, ex. `transfers_20260718.tsv`) |
| **Format** | TSV |
| **Encodage** | UTF-8, sans BOM |
| **Délimiteur** | tabulation (`\t`) |
| **Fin de ligne** | LF (`\n`) |
| **Header** | ligne 1 toujours présente |
| **Absence de fichier** | signal honnête « aucun transfert proposé ce jour-là » |
| **Généré par** | `ootils_core.engine.reporting.outbound_export.execute_export` |
| **Déclenché par** | `scripts/run_daily_ingest.py` (phase EXPORT), kill switch `OOTILS_OUTBOUND_EXPORT_ENABLED` (défaut **OFF**) |

---

## 2. Nature de la donnée

Une ligne = **une proposition de transfert inter-sites déjà validée côté Ootils** (recommandation DRP `TRANSFER`, ADR-028, décision level L1 — un nouveau mouvement, réversible tant qu'il n'est pas exécuté) :
> « Ootils recommande de déplacer `quantity` unités de `item_external_id` de `source_location_external_id` vers `dest_location_external_id`, besoin le `shortage_date`. »

Contrairement à `po_drafts`/`reschedule_messages`, cette famille **porte bien un site** (source ET destination) : `recommendations.source_location_id`/`dest_location_id` (migration 066) sont des colonnes dédiées, spécifiques à l'action `TRANSFER` — le gap-location documenté pour `po_drafts`/`reschedule_messages` (§7 de ces documents) **ne s'applique pas ici**.

---

## 3. Colonnes

**Ordre figé par le renderer** (`outbound_export.py:_TRANSFERS_HEADER`) : `item_external_id`, `source_location_external_id`, `dest_location_external_id`, `quantity`, `shortage_date`, `recommendation_id`.

Notez l'**absence d'une colonne `action`** dans ce fichier — contrairement aux deux autres familles sortantes, chaque ligne du fichier `transfers_<date>.tsv` est *par construction* une `TRANSFER` (c'est le fichier lui-même qui porte le sens de l'action, pas une colonne).

| # | Colonne | Toujours renseignée | Type | Domaine | Description |
|---|---|---|---|---|---|
| 1 | `item_external_id` | oui | texte | code SKU ERP | Article à transférer. |
| 2 | `source_location_external_id` | oui en pratique* | texte | code site ERP | Site d'origine (qui expédie). |
| 3 | `dest_location_external_id` | oui en pratique* | texte | code site ERP | Site de destination (qui reçoit). |
| 4 | `quantity` | oui | décimal point fixe | > 0 | Quantité à transférer (`recommended_qty`, déjà arrondie DRP fair-share + multiple de transfert). |
| 5 | `shortage_date` | oui | date ISO | — | Date de besoin **à destination** — c'est la raison métier du transfert (le site destinataire a une pénurie à cette date), pas une date d'expédition. Nommée `shortage_date` dans le fichier (reprise du nom interne de la colonne source) — voir §4.1. |
| 6 | `recommendation_id` | oui | UUID | — | Référence humaine interne Ootils — **pas un identifiant à ressaisir dans l'ERP** (même règle que les deux autres familles). |

*(*) `source_location_external_id`/`dest_location_external_id` proviennent d'une jointure `LEFT JOIN locations` sur des colonnes `ON DELETE SET NULL` (migration 066) : elles sont peuplées pour toute recommandation `TRANSFER` normale, mais pourraient en théorie apparaître vides dans le cas rare où le site référencé a été supprimé physiquement de la base après l'approbation de la recommandation et avant son export — un cas de perte d'intégrité, pas un comportement attendu.*

**Header exact** :
```
item_external_id	source_location_external_id	dest_location_external_id	quantity	shortage_date	recommendation_id
```

---

## 4. Détails

### 4.1 `shortage_date` — lire « date de besoin destination », pas « date d'expédition »

Le nom de colonne `shortage_date` est repris tel quel de la table `recommendations` (c'est la date de la pénurie qui a déclenché la recommandation de transfert) — il n'y a **pas** de colonne séparée pour une date d'expédition planifiée. L'équipe ERP doit lire cette date comme « le site destination en a besoin à cette date », et calculer elle-même la date d'expédition en reculant du délai de transit (`distribution_links.transit_lead_time_days` côté référentiel, si chargé).

### 4.2 `recommendation_id` — référence humaine, jamais échoable

Même règle que les deux autres familles : trace d'audit interne, pas un `ootils_ref` à ressaisir dans l'ERP.

### 4.3 Idempotence

Identique aux deux autres familles : `recommendations.exported_at` (migration 078) est stampée juste après écriture, dans la même transaction — une ligne n'apparaît **qu'une seule fois**. Absence de fichier = aucun transfert approuvé ce jour-là.

**Mode d'emploi équipe ERP** : chaque ligne = un mouvement de stock inter-sites à saisir une fois dans l'ERP (ou à exécuter physiquement). Une fois traitée, elle ne revient jamais avec le même `recommendation_id`.

---

## 5. Exemples

### 5.1 Deux transferts

```
item_external_id	source_location_external_id	dest_location_external_id	quantity	shortage_date	recommendation_id
FG-APU-100	WH-PARIS-01	DC-LILLE	20	2026-07-24	4d5e6f70-8192-4a3b-8c9d-3f4a5b6c7d8e
FG-APU-200	PLANT-LYON	DC-LILLE	10	2026-07-26	5e6f7081-9203-4a4c-8d0e-4a5b6c7d8e9f
```

Lecture :
- Ligne 1 : déplacer 20 AquaPump 100 de Paris vers Lille, besoin le 24/07 à Lille.
- Ligne 2 : déplacer 10 AquaPump 200 de Lyon vers Lille, besoin le 26/07 à Lille.

### 5.2 Aucun transfert ce jour-là

Aucun fichier `transfers_<date>.tsv` n'est déposé dans l'outbox.

---

## 6. Comparatif avec l'entrant `transfers.tsv`

| | Entrant `transfers.tsv` | Sortant `transfers_<AAAAMMJJ>.tsv` |
|---|---|---|
| Direction | Équipe ERP → Ootils | Ootils → équipe ERP |
| Dossier | `inbox` | `outbox` |
| Nom de fichier | Fixe, non daté | Daté (`_<AAAAMMJJ>`) |
| Contenu | Transferts déjà décidés/exécutés côté ERP (STO), avec `status` (planned/in_transit/delivered/cancelled) | Transferts **proposés** par le DRP Ootils, pas encore un STO ERP |
| Colonne clé | `external_id` (numéro STO ERP, clé d'upsert) | `recommendation_id` (référence Ootils interne, non échoable) |
| Colonnes locations | `from_location_external_id` / `to_location_external_id` | `source_location_external_id` / `dest_location_external_id` |

Les deux fichiers **ne se répondent pas automatiquement** en V1 : saisir une ligne du sortant crée un nouveau STO côté ERP, qui remontera plus tard comme une ligne de l'entrant `transfers.tsv` avec son propre `external_id` — le rapprochement entre les deux est le sujet de la réconciliation heuristique (PR-5b, non livrée à ce jour).

---

## 7. Pipeline

```
recommendations (action = 'TRANSFER', status IN ('APPROVED','APPLIED'), exported_at IS NULL)
        │
        ▼
   load_pending_export_rows()      ← SELECT-only, scenario BASELINE uniquement
        │
        ▼
   render_outbound_export()        ← déterministe, DB-free
        │
        ▼
   execute_export() :
        ├─ écrit outbox/transfers_<AAAAMMJJ>.tsv
        ├─ stampe recommendations.exported_at = now()
        └─ émet 1 event `export_executed` (partagé avec les autres familles du même run)
        │
        ▼
   scripts/deposit_outbox.sh (rclone copy, sens unique)
        │
        ▼
   dropbox:ootils-outbox/transfers_<AAAAMMJJ>.tsv
```

## 8. Prévisualisation

```bash
DATABASE_URL=... OOTILS_API_TOKEN=... python scripts/run_daily_ingest.py --date 2026-07-18
```
Sans `--apply`, la phase EXPORT reste une prévisualisation STDOUT-only.
