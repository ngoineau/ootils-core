# Format de fichier — `reschedule_messages_<AAAAMMJJ>.tsv` (SORTANT)

> Fichier **sortant** des messages de replanification — recommandations `RESCHEDULE_IN` / `RESCHEDULE_OUT` / `CANCEL` (ADR-026) **déjà approuvées côté Ootils**, portant sur une commande d'achat ou un ordre planifié/fermé (Firm Planned Order) déjà existant.
> Produit par `engine/reporting/outbound_export.py` (ADR-042 décision 4, chantier PR-5a).
> Déposé dans `dropbox:ootils-outbox` par `scripts/deposit_outbox.sh`.
> Ootils ne modifie **jamais** l'ERP directement (ligne rouge L4) — c'est à l'équipe ERP d'appliquer le message. `CANCEL` reste une action L3, humain-only : elle est déjà passée par l'approbation `/v1/recommendations/{id}/approve` **avant** d'atterrir dans ce fichier (voir `notifications/l3_webhook.py`, ADR-026).

---

## 1. Caractéristiques du fichier

| Propriété | Valeur |
|---|---|
| **Nom** | `reschedule_messages_<AAAAMMJJ>.tsv` (daté du jour du run d'export, ex. `reschedule_messages_20260718.tsv`) |
| **Format** | TSV |
| **Encodage** | UTF-8, sans BOM |
| **Délimiteur** | tabulation (`\t`) |
| **Fin de ligne** | LF (`\n`) |
| **Header** | ligne 1 toujours présente |
| **Absence de fichier** | signal honnête « aucun message ce jour-là » |
| **Généré par** | `ootils_core.engine.reporting.outbound_export.execute_export` |
| **Déclenché par** | `scripts/run_daily_ingest.py` (phase EXPORT), kill switch `OOTILS_OUTBOUND_EXPORT_ENABLED` (défaut **OFF**) |

---

## 2. Nature de la donnée

Une ligne = **un message de replanification sur une commande/ordre déjà existant** :
> « Pour la commande/l'ordre `target_po_reference`, actuellement attendu le `current_receipt_date`, Ootils recommande le `action` (nouvelle date `proposed_date` si applicable). »

Contrairement à `po_drafts` (nouvelle commande), ce fichier **cible toujours un objet déjà en base ERP ou déjà planifié par Ootils** — un PO ouvert, une commande de fabrication, ou un Firm Planned Order (FPO, `nodes.is_firm`, migration 061). C'est le pendant sortant de `purchase_orders.tsv`/`work_orders.tsv` (entrants) : un message qui demande à l'ERP de **changer une date déjà connue**, jamais d'en créer une nouvelle.

Les trois actions :

| Action | Sémantique | `proposed_date` |
|---|---|---|
| `RESCHEDULE_IN` | Avancer la date de réception (le besoin est apparu plus tôt que prévu). | Renseignée — nouvelle date, antérieure à `current_receipt_date`. |
| `RESCHEDULE_OUT` | Repousser la date de réception (le besoin a reculé). | Renseignée — nouvelle date, postérieure à `current_receipt_date`. |
| `CANCEL` | Annuler l'ordre — il n'est plus nécessaire du tout. | **Toujours vide** — un CANCEL n'a pas de nouvelle date par construction (`engine/recommendation/reschedule.py` : l'identité déterministe de la recommandation encode explicitement `proposed_date=None` pour un CANCEL). |

`DEFER` est une valeur réservée dans le vocabulaire `recommendations.action` (migration 061) mais n'est **jamais émise** par le moteur en V1 (`reschedule.py` : « reserved for manual/agent use ») — elle n'apparaît donc jamais dans ce fichier aujourd'hui.

---

## 3. Colonnes

**Ordre figé par le renderer** (`outbound_export.py:_RESCHEDULE_MESSAGES_HEADER`) : `item_external_id`, `target_po_reference`, `current_receipt_date`, `proposed_date`, `action`, `recommendation_id`.

| # | Colonne | Toujours renseignée | Type | Domaine | Description |
|---|---|---|---|---|---|
| 1 | `item_external_id` | oui | texte | code SKU ERP | Article concerné. |
| 2 | `target_po_reference` | oui | texte | numéro PO ERP **ou** UUID interne | Voir §5.1 — la référence de l'ordre ciblé, résolution à deux niveaux. |
| 3 | `current_receipt_date` | oui | date ISO | — | Date de réception **actuelle** de l'ordre ciblé, telle que connue par Ootils au moment de la recommandation. |
| 4 | `proposed_date` | non — vide pour `CANCEL` | date ISO | — | Nouvelle date proposée. Toujours vide pour `CANCEL` (voir §2). |
| 5 | `action` | oui | enum | `RESCHEDULE_IN` \| `RESCHEDULE_OUT` \| `CANCEL` | Action gouvernée. |
| 6 | `recommendation_id` | oui | UUID | — | Référence humaine interne Ootils — **pas un identifiant à ressaisir dans l'ERP** (même règle que `po_drafts`, voir §5.2). |

**Header exact** :
```
item_external_id	target_po_reference	current_receipt_date	proposed_date	action	recommendation_id
```

---

## 4. Détails

### 4.1 `target_po_reference` — résolution à deux niveaux

Cette colonne peut contenir **deux natures de valeur différentes** — l'équipe ERP doit savoir les distinguer :

1. **Un numéro de PO ERP réel** (ex. `PO-2026-001`) — quand l'ordre ciblé (`recommendations.target_node_id`) est un node que Ootils a pu relier à un PO connu de l'ERP via la table de correspondance `external_references`. Dans ce cas, saisir le message directement contre ce numéro dans l'ERP.
2. **Un UUID interne Ootils** (ex. `9f1c2a3b-4d5e-6f70-8192-a3b4c5d6e7f8`) — quand l'ordre ciblé est un ordre **planifié/fermé côté Ootils** (Firm Planned Order, `nodes.is_firm`) que l'ERP n'a **jamais vu passer** dans son propre flux de commandes ouvertes. Dans ce cas, il n'existe **rien à chercher** dans l'ERP sous ce numéro — c'est un signal qu'un planificateur doit qualifier manuellement avant d'agir (créer l'ordre correspondant dans l'ERP, ou vérifier pourquoi Ootils voit un ordre que l'ERP ne connaît pas).

L'équipe ERP peut distinguer les deux cas visuellement : un numéro de PO suit le format ERP habituel (ex. `PO-YYYY-NNN`), un UUID est une chaîne à tirets de 36 caractères.

### 4.2 `recommendation_id` — référence humaine, jamais échoable

Même règle que `po_drafts` (§5.3 de ce document sœur) : `recommendation_id` est une trace d'audit interne Ootils, pas un champ à ressaisir dans l'ERP. Pas de champ `ootils_ref` échoable côté ERP pilote — décision actée le 2026-07-13.

### 4.3 Idempotence

Identique à `po_drafts` : une recommandation est stampée `exported_at` (migration 078) juste après écriture, dans la même transaction — elle n'apparaît **qu'une seule fois**, jamais réémise. Absence de fichier = aucun message de replanification approuvé ce jour-là.

**Mode d'emploi équipe ERP** : chaque ligne = une action à appliquer une fois dans l'ERP (changer la date, ou annuler l'ordre). Une fois appliquée, elle ne revient jamais avec le même `recommendation_id`.

---

## 5. Exemples

### 5.1 Un RESCHEDULE_IN et un CANCEL

```
item_external_id	target_po_reference	current_receipt_date	proposed_date	action	recommendation_id
COMP-MOTOR-24V	PO-2026-001	2026-08-05	2026-07-28	RESCHEDULE_IN	1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d
SUB-HOUSING-100	9f1c2a3b-4d5e-6f70-8192-a3b4c5d6e7f8	2026-08-10		CANCEL	2b3c4d5e-6f70-4a8b-9c0d-1e2f3a4b5c6e
```

Lecture :
- Ligne 1 : la PO ERP `PO-2026-001` doit être avancée du 05/08 au 28/07 (le besoin est apparu plus tôt).
- Ligne 2 : un ordre **interne** (UUID, pas de PO ERP connue) attendu le 10/08 doit être **annulé** — aucune date proposée (colonne vide), à qualifier par un planificateur.

### 5.2 Un RESCHEDULE_OUT

```
item_external_id	target_po_reference	current_receipt_date	proposed_date	action	recommendation_id
RAW-STEEL-50	PO-2026-014	2026-07-20	2026-08-03	RESCHEDULE_OUT	3c4d5e6f-7081-4a9b-8c0d-2f3a4b5c6d7e
```

Lecture : le besoin d'acier a reculé — repousser la réception du 20/07 au 03/08.

---

## 6. Limitations connues V1.0

| Manque | Détail |
|---|---|
| Pas de site (`location_external_id`) | Même gap que `po_drafts` (§7 du document sœur) : `recommendations` ne porte aucune colonne de site générique. **Ce gap pèse moins ici** : `target_po_reference`, quand c'est un vrai numéro de PO ERP, permet à l'équipe ERP de retrouver le site directement dans son propre système via ce numéro — le manque n'est réellement bloquant que dans le cas UUID interne (ordre non encore connu de l'ERP). |
| `DEFER` jamais émis | Valeur réservée dans le vocabulaire `action` (migration 061) mais le moteur ne l'émet pas en V1 — n'apparaîtra jamais dans ce fichier tant que ce n'est pas câblé. |

---

## 7. Pipeline

```
recommendations (action IN ('RESCHEDULE_IN','RESCHEDULE_OUT','CANCEL'),
                  status IN ('APPROVED','APPLIED'), exported_at IS NULL)
        │
        ▼
   load_pending_export_rows()      ← SELECT-only, scenario BASELINE uniquement
        │
        ▼
   render_outbound_export()        ← déterministe, DB-free
        │
        ▼
   execute_export() :
        ├─ écrit outbox/reschedule_messages_<AAAAMMJJ>.tsv
        ├─ stampe recommendations.exported_at = now()
        └─ émet 1 event `export_executed` (partagé avec les autres familles du même run)
        │
        ▼
   scripts/deposit_outbox.sh (rclone copy, sens unique)
        │
        ▼
   dropbox:ootils-outbox/reschedule_messages_<AAAAMMJJ>.tsv
```

## 8. Prévisualisation

```bash
DATABASE_URL=... OOTILS_API_TOKEN=... python scripts/run_daily_ingest.py --date 2026-07-18
```
Sans `--apply`, la phase EXPORT reste une prévisualisation STDOUT-only.
