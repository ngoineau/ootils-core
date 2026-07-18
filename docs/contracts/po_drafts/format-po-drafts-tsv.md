# Format de fichier — `po_drafts_<AAAAMMJJ>.tsv` (SORTANT)

> Fichier **sortant** des propositions de commande d'achat — recommandations `ORDER_NOW` / `ORDER_RUSH` / `EXPEDITE` **déjà approuvées côté Ootils** (un planner a validé la recommandation dans `/ui` ou via l'API), prêtes à être saisies dans l'ERP.
> Produit par `engine/reporting/outbound_export.py` (ADR-042 décision 4, chantier PR-5a).
> Déposé dans `dropbox:ootils-outbox` par `scripts/deposit_outbox.sh` (même mécanisme que `daily_report_<date>.md`).
> Ootils ne pousse **jamais** cette commande dans l'ERP lui-même — c'est un DRAFT que l'équipe ERP saisit à la main. La ligne rouge L4 de la doctrine (ADR-042 §"Refus explicites en V1") : pas de write-back ERP, sous aucune forme.

---

## 1. Caractéristiques du fichier

| Propriété | Valeur |
|---|---|
| **Nom** | `po_drafts_<AAAAMMJJ>.tsv` (daté du jour du run d'export, ex. `po_drafts_20260718.tsv`) |
| **Format** | TSV |
| **Encodage** | UTF-8, **sans BOM** (`encoding="utf-8"`, jamais `"utf-8-sig"` — règle d'or pilote pour les sortants) |
| **Délimiteur** | tabulation (`\t`) |
| **Fin de ligne** | LF (`\n`) — jamais CRLF côté écriture (`newline="\n"` explicite) |
| **Header** | ligne 1 toujours présente |
| **Absence de fichier** | signal honnête « rien approuvé aujourd'hui » — jamais un fichier vide header-only (voir §4) |
| **Généré par** | `ootils_core.engine.reporting.outbound_export.execute_export` |
| **Déclenché par** | `scripts/run_daily_ingest.py` (phase EXPORT, après le compte-rendu quotidien), kill switch `OOTILS_OUTBOUND_EXPORT_ENABLED` (défaut **OFF**) |

---

## 2. Nature de la donnée

Une ligne = **une proposition de commande d'achat déjà validée côté Ootils** :
> « Ootils recommande de commander `quantity` unités de `item_external_id` chez `supplier_external_id`, besoin le `need_date`. »

Ce n'est **pas** une ligne à revoir — le statut source (`recommendations.status`) est déjà `APPROVED` ou `APPLIED` au moment de l'export. La revue/approbation se fait **avant** l'export, côté `/ui` ou API Ootils (hors scope de ce document). Une recommandation encore `DRAFT`/`REVIEWED`/`REJECTED`/`EXPIRED` n'apparaît **jamais** dans ce fichier — c'est le prédicat SQL lui-même qui l'exclut (`status IN ('APPROVED','APPLIED')`), pas un filtre applicatif qu'un futur changement pourrait contourner.

**Pas de site de réception dans ce fichier** — limitation V1.0 documentée en détail au §7.

---

## 3. Colonnes

**Ordre figé par le renderer** (`outbound_export.py:_PO_DRAFTS_HEADER`) : `item_external_id`, `supplier_external_id`, `quantity`, `need_date`, `action`, `recommendation_id`, `confidence`.

| # | Colonne | Toujours renseignée | Type | Domaine | Description |
|---|---|---|---|---|---|
| 1 | `item_external_id` | oui | texte | code SKU ERP | Article à commander (`recommendations.item_external_id`). |
| 2 | `supplier_external_id` | non — peut être vide | texte | code fournisseur ERP | Fournisseur suggéré par le moteur. Cellule vide si aucun fournisseur n'a pu être résolu (typiquement corrélé à `confidence = NEEDS_DATA_REVIEW`, voir §5.2). |
| 3 | `quantity` | oui | décimal point fixe | > 0 | Quantité recommandée (`recommended_qty`). Jamais de notation scientifique ni de séparateur de milliers. |
| 4 | `need_date` | oui | date ISO `YYYY-MM-DD` | — | Date de besoin. `proposed_date` si renseignée, sinon `shortage_date` — **en V1 toujours `shortage_date`** : `proposed_date` n'est peuplée que pour la famille reschedule (migration 061), jamais pour `ORDER_NOW`/`ORDER_RUSH`/`EXPEDITE` aujourd'hui. Le COALESCE reste dans le renderer pour rester compatible sans changement de format si une EXPEDITE-avec-date-proposée apparaît un jour. |
| 5 | `action` | oui | enum | `ORDER_NOW` \| `ORDER_RUSH` \| `EXPEDITE` | Action gouvernée décidée par le moteur (`agent_governance.decision_level`). |
| 6 | `recommendation_id` | oui | UUID | — | Référence humaine **interne Ootils** — voir §5.3, **ce n'est pas un identifiant à ressaisir dans le PO ERP**. |
| 7 | `confidence` | oui | enum | `HIGH` \| `MEDIUM` \| `LOW` \| `NEEDS_DATA_REVIEW` | Niveau de confiance du calcul source. `NEEDS_DATA_REVIEW` signale une donnée d'entrée incomplète (ex. fournisseur non résolu) — la ligne est quand même exportée (elle a été approuvée par un humain malgré cette réserve), mais mérite un second regard côté ERP avant saisie. |

**Header exact** :
```
item_external_id	supplier_external_id	quantity	need_date	action	recommendation_id	confidence
```

---

## 4. Comportement de génération (idempotence)

- Une recommandation apparaît **exactement une fois**, le jour de son export — `recommendations.exported_at` est stampée (migration 078) immédiatement après l'écriture du fichier, dans la **même transaction** que l'écriture. Un rerun du même jour, ou un rerun après crash, ne réexporte jamais une ligne déjà stampée (`WHERE exported_at IS NULL`).
- **Contrairement au compte-rendu quotidien** (`daily_report_<date>.md`, qui redécrit l'état complet du jour à chaque run), ce fichier ne contient **que les recommandations nouvellement approuvées** depuis le dernier export réussi. Ce n'est pas une photo de l'état ; c'est un flux d'événements.
- Une famille sans ligne éligible ce jour-là ne produit **aucun fichier** — jamais un `po_drafts_<date>.tsv` avec juste le header. L'absence de fichier est le signal honnête « rien à commander aujourd'hui ».
- Un événement `export_executed` (migration 085) est émis une fois par run d'export ayant réellement écrit au moins un fichier — visible sur `/v1/stream` pour un agent abonné.

### 4.1 Mode d'emploi équipe ERP

**Une ligne = une saisie, une fois.** Dès qu'une proposition est saisie dans l'ERP, elle est terminée côté Ootils — elle ne revient **jamais** dans un fichier `po_drafts` ultérieur avec le même `recommendation_id`. Il n'y a rien à cocher ni à archiver manuellement côté fichier : le marqueur d'idempotence vit côté Ootils, pas dans le fichier lui-même.

---

## 5. Détails

### 5.1 Cellule vide = valeur absente, jamais `NULL`/`None`

Une cellule vide (ex. `supplier_external_id`) signifie « pas de valeur » — jamais la chaîne littérale `NULL` ou `None`. Le renderer refuse par ailleurs d'écrire une valeur métier contenant une tabulation/retour ligne (corruption d'alignement de colonnes) — un tel cas fait échouer tout le run d'export plutôt que produire un fichier corrompu silencieusement.

### 5.2 `supplier_external_id` vide

Un déficit peut être détecté sans que le moteur ait pu résoudre un fournisseur unique/fiable (ex. plusieurs fournisseurs actifs sans priorité claire, ou aucun `supplier_items` actif pour l'article). Dans ce cas la ligne est quand même exportée (elle a franchi l'approbation humaine) mais la cellule fournisseur reste vide — l'équipe ERP doit choisir le fournisseur elle-même avant saisie. Corrélé en pratique à `confidence = NEEDS_DATA_REVIEW`, mais ce n'est pas une garantie stricte (pas de contrainte DB liant les deux colonnes).

### 5.3 `recommendation_id` — référence humaine, jamais échoable

**Décision pilote 2026-07-13** (ADR-042, réconciliation heuristique) : il n'existe pas de champ `ootils_ref` que l'ERP du pilote peut faire l'aller-retour dans son propre numéro de PO. `recommendation_id` sert de **trace d'audit interne** — pour retrouver la ligne dans `/ui`, `/v1/outcomes`, ou les logs — mais n'est **pas destiné à être saisi dans un champ de l'ERP**. Le rapprochement d'une PO ERP effectivement créée avec cette ligne exportée se fera plus tard par une **heuristique** sur les attributs métier (item, fournisseur, quantité, date) — chantier PR-5b, non livré à ce jour (voir l'amendement ADR-042 du 2026-07-18/19).

---

## 6. Exemples

### 6.1 Run avec 3 propositions

```
item_external_id	supplier_external_id	quantity	need_date	action	recommendation_id	confidence
COMP-MOTOR-24V	SUP-MOTOR-01	120	2026-07-25	ORDER_NOW	3f9a2e10-8b1c-4d5e-9a3f-1b2c3d4e5f60	HIGH
RAW-STEEL-50	SUP-STEEL-DE	4000	2026-07-22	ORDER_RUSH	7c1d4f22-9e3b-4a6d-8f2c-2a3b4c5d6e71	MEDIUM
COMP-IMPELLER-100		80	2026-07-20	EXPEDITE	a2b3c4d5-1e2f-4a5b-9c8d-3e4f5a6b7c82	NEEDS_DATA_REVIEW
```

Lecture :
- Ligne 1 : commander 120 moteurs chez SUP-MOTOR-01, besoin le 25/07, confiance haute.
- Ligne 2 : commande accélérée (`ORDER_RUSH`) de 4000 kg d'acier, besoin le 22/07.
- Ligne 3 : `EXPEDITE` sur un impeller — **fournisseur non résolu** (cellule vide), à qualifier avant saisie ERP.

### 6.2 Aucune proposition ce jour-là

Aucun fichier `po_drafts_<date>.tsv` n'est déposé dans l'outbox. Ce n'est pas une anomalie — c'est le signal « rien approuvé aujourd'hui ».

---

## 7. Limitations connues V1.0

| Manque | Détail | Suite |
|---|---|---|
| **Pas de site de réception (`location_external_id`)** | `recommendations` (migrations 039/061) ne porte **aucune** colonne `location_id`/`location_external_id` générique pour une commande d'achat — seules les recos `TRANSFER` (migration 066) portent `source_location_id`/`dest_location_id`, et elles ne sont pas dans cette famille de fichier. La ligne `shortages` source, elle, connaît le site (`shortages.location_id`, migration 005) mais cette information n'est **pas propagée** jusqu'à `recommendations`. | Gap documenté pour PR-5b (réconciliation heuristique) — voir amendement ADR-042 2026-07-18/19. Un ajout de colonne côté `recommendations` serait nécessaire pour fermer ce manque, pas encore planifié. |
| Pas d'unité de mesure (`uom`) | Le fichier entrant `purchase_orders.tsv` porte un `uom` ; ce sortant n'en porte pas. | 🎯 à ajouter si le pilote en a besoin. |
| Pas de prix/devise | `recommendations.estimated_cost`/`currency` existent en base mais ne sont pas exportés en V1. | 🎯 non demandé à ce jour. |
| Pas de consolidation multi-lignes par fournisseur | Une ligne = une recommandation = un article. Pas de regroupement en un seul PO multi-lignes par fournisseur (contrairement à l'idée explorée dans `scripts/export_approved_pos.py`, aujourd'hui déprécié et non idempotent — voir son bandeau de dépréciation). | Non planifié ; le regroupement reste un geste manuel côté ERP si souhaité. |

---

## 8. Pipeline

```
recommendations (status IN ('APPROVED','APPLIED'), exported_at IS NULL)
        │
        ▼
   load_pending_export_rows()      ← SELECT-only, scenario BASELINE uniquement
        │
        ▼
   render_outbound_export()        ← déterministe, DB-free, un fichier par famille
        │
        ▼
   execute_export() :
        ├─ écrit outbox/po_drafts_<AAAAMMJJ>.tsv
        ├─ stampe recommendations.exported_at = now()
        └─ émet 1 event `export_executed`
        │
        ▼
   scripts/deposit_outbox.sh (rclone copy, sens unique)
        │
        ▼
   dropbox:ootils-outbox/po_drafts_<AAAAMMJJ>.tsv
```

## 9. Prévisualisation

```bash
DATABASE_URL=... OOTILS_API_TOKEN=... python scripts/run_daily_ingest.py --date 2026-07-18
```
Sans `--apply`, la phase EXPORT est toujours une prévisualisation : rendu imprimé sur STDOUT, aucune écriture fichier, aucune écriture DB.
