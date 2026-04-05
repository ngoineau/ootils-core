# Ootils — Stratégie d'Intégration

> Version 1.0 — 2026-04-05
> Statut : **RÉFÉRENCE**

---

## 1. Topologie générale

```
┌─────────────────────────────────────────────────────────────┐
│                    SYSTÈMES SOURCES                         │
│  ERP (SAP/Dynamics/Sage)  WMS  EDI  Excel  API tierces     │
└──────────┬──────────────────────────────────────────────────┘
           │ TSV/SFTP (batch)  |  REST/Webhook (streaming)
           ▼
┌─────────────────────────────────────────────────────────────┐
│              COUCHE D'INTÉGRATION OOTILS                    │
│  • Validation & normalisation                               │
│  • Mapping codes ERP → IDs Ootils                          │
│  • Gestion des erreurs et rejets                            │
│  • Audit log de chaque import                               │
│  POST /v1/import/*  |  POST /v1/events                      │
└──────────┬──────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│                    MOTEUR OOTILS                             │
│  Graph + Propagation + Scénarios                            │
│  GET /v1/issues  |  GET /v1/projection                      │
└──────────┬──────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│                       OUTPUTS                               │
│  UI Ootils  |  Export TSV  |  Recommandations → ERP (P2)   │
└─────────────────────────────────────────────────────────────┘
```

**Principe fondateur :** Ootils est read-heavy côté intégration. Les systèmes sources restent maîtres de leurs données. Ootils consomme, calcule, recommande — mais n'écrit jamais en ERP sans validation humaine explicite.

---

## 2. Connecteurs — Matrice de priorité

| Connecteur | Source | Données | Direction | Protocole | Priorité | Complexité |
|------------|--------|---------|-----------|-----------|----------|------------|
| Excel/TSV manuel | Fichier local | Toutes | Inbound | Upload UI ou SFTP | P0 | Faible |
| API REST générique | Tout système | Toutes | Inbound | REST POST | P0 | Faible |
| SAP (BAPI/RFC) | SAP ECC/S4 | PO, Stock, CO, Items | Inbound | RFC/REST | P1 | Haute |
| MS Dynamics | D365 F&O | PO, Stock, CO | Inbound | REST OData | P1 | Moyenne |
| WMS générique | WMS | On-Hand, Transfers | Inbound | REST/SFTP | P1 | Moyenne |
| EDI 850/856/810 | Partenaires | CO, PO, ASN | Inbound | SFTP/AS2 | P1 | Haute |
| Webhook ERP | ERP moderne | Événements temps réel | Inbound | Webhook POST | P2 | Faible |
| Recommendations → ERP | Ootils | PlannedSupply | Outbound | REST/TSV | P2 | Moyenne |

**Règle de priorisation :** P0 = nécessaire au premier POC client / zéro dépendance ERP ; P1 = nécessaire à la signature d'un client récurrent ; P2 = différenciateur compétitif post-product-market-fit.

---

## 3. Format standard Ootils (batch TSV)

### Conventions

| Paramètre | Valeur |
|-----------|--------|
| Encoding | UTF-8 (BOM interdit) |
| Séparateur | Tabulation `\t` |
| Dates | ISO 8601 : `YYYY-MM-DD` |
| Décimaux | Point `.` (jamais virgule) |
| Boolean | `true` / `false` (minuscules) |
| Valeurs nulles | Colonne vide — jamais `NULL` en texte |
| Header | Obligatoire en ligne 1 |
| Line endings | LF (`\n`) — CRLF toléré à l'import |

> **Pourquoi TSV plutôt que CSV ?**
> Les données supply chain contiennent fréquemment des virgules dans les libellés (noms d'articles, descriptions, adresses). Le tab comme séparateur élimine ce risque sans guillemets d'échappement. SAP exporte nativement en tab (transactions SE16, MB52, ME2M). L'extension `.tsv` est reconnue par Excel, pandas, et tous les outils ETL standard.

### Nommage des fichiers

```
{type}_{source}_{YYYYMMDD}_{HHMMSS}.tsv

Exemples :
  items_sap_20260405_090000.tsv
  purchase_orders_dynamics_20260405_143022.tsv
  on_hand_wms_20260405_060000.tsv
  customer_orders_edi_20260405_120000.tsv
```

### Gestion des codes ERP vs UUIDs Ootils

- Tous les imports utilisent des `external_id` (codes métier ERP — jamais des UUIDs internes)
- Ootils maintient une table de mapping :

```sql
external_references (
  entity_type   TEXT,      -- 'item' | 'location' | 'supplier' | ...
  external_id   TEXT,      -- code ERP
  source_system TEXT,      -- 'sap_ecc' | 'dynamics' | 'wms_manhattan' | ...
  internal_id   UUID,      -- UUID Ootils
  created_at    TIMESTAMP,
  updated_at    TIMESTAMP,
  PRIMARY KEY (entity_type, external_id, source_system)
)
```

- **Règle à l'import :**
  - Master data (items, locations, suppliers) : `external_id` inconnu → **création automatique**
  - Données transactionnelles (PO, CO, stock) : `external_id` inconnu → **rejet avec erreur explicite**

---

## 4. Couche ETL — Mapping ERP → Ootils

### 4.1 SAP → Ootils

| Table SAP | Champ SAP | Entité Ootils | Champ Ootils | Notes |
|-----------|-----------|---------------|--------------|-------|
| MARA | MATNR | Item | external_id | Clé métier |
| MARA | MAKTX | Item | name | Description courte |
| MARA | MTART | Item | item_type | Voir enum_map ci-dessous |
| MARA | MEINS | Item | uom | Unité de mesure de base |
| T001W | WERKS | Location | external_id | Code usine/dépôt |
| T001W | NAME1 | Location | name | |
| EKKO/EKPO | EBELN/EBELP | PurchaseOrder | po_number / line | Concaténation `{EBELN}-{EBELP}` |
| EKPO | MENGE | PurchaseOrder | quantity | |
| EKPO | EINDT | PurchaseOrder | expected_date | Date livraison confirmée |
| MARD | LABST | OnHandSupply | quantity | Stock libre uniquement |
| VBAP | VBELN | CustomerOrder | order_number | |
| VBAP | POSNR | CustomerOrder | line | |
| VBAP | KWMENG | CustomerOrder | quantity | Qté confirmée |
| VBAP | EDATU | CustomerOrder | requested_date | |

### 4.2 Règles de transformation

Les règles de mapping vivent dans des fichiers YAML par source : `config/mappings/{source}.yaml`

Structure de référence :

```yaml
source: sap_ecc
entity: items
source_table: MARA
field_mappings:
  external_id: MATNR
  name: MAKTX
  uom: MEINS
  item_type:
    source_field: MTART
    transform: enum_map
    enum_map:
      FERT: finished_good
      ROH: raw_material
      HALB: semi_finished
      HIBE: consumable
      NLAG: non_stock
    default: other
```

**Principe :** si un `enum_map` ne couvre pas une valeur source, le champ prend la valeur `default`. Si `default` absent → rejet de la ligne avec warning.

### 4.3 Pipeline de validation à l'import

```
Fichier reçu
  → [1] Validation structure (colonnes requises, encoding, types)
  → [2] Lookup external_id → internal UUID
  → [3] Application des règles de transformation YAML
  → [4] Validation métier (dates cohérentes, quantités > 0, etc.)
  → [5] Upsert en base Ootils
  → [6] Audit log (lignes acceptées / rejetées / warnings)
  → [7] Réponse JSON avec summary + liste des rejets
```

---

## 5. Outputs Ootils → ERP

### 5.1 Recommandations disponibles

| Type | Description | Entité cible ERP |
|------|-------------|------------------|
| `planned_po` | Suggestion de Planned Purchase Order | Ordre d'achat planifié |
| `planned_wo` | Suggestion de Work Order | Ordre de fabrication |
| `shortage_alert` | Alerte rupture avec date et magnitude | Notification / queue de priorité |
| `simulation_result` | Comparaison scénarios (delta vs baseline) | Rapport décisionnel |

### 5.2 Règle absolue — Human in the loop

> **Aucune écriture en ERP sans validation humaine explicite.**

Flux obligatoire :

```
Ootils calcule
  → UI affiche recommandation (statut : DRAFT)
  → Planificateur révise / ajuste / valide
  → Statut → APPROVED
  → Export TSV ou API push vers ERP
```

Aucun mécanisme d'auto-push ne sera implémenté, même en Phase 3. Le push API vers ERP reste déclenché par une action utilisateur.

### 5.3 Format export recommandations (TSV)

```tsv
recommendation_type	item_external_id	location_external_id	supplier_external_id	quantity	uom	suggested_date	priority	reason
planned_po	SKU-001	DC-PARIS	SUP-001	500	EA	2026-04-20	high	Shortage detected J+12
planned_po	SKU-042	DC-LYON		1200	KG	2026-04-18	critical	Stockout risk J+8
shortage_alert	SKU-007	DC-PARIS		0	EA	2026-04-15	high	Zero stock projected
```

---

## 6. Roadmap d'intégration

### Phase 0 — Quick win TSV/Excel (aujourd'hui)

**Objectif :** premier POC client sans aucun développement connecteur.

- Upload TSV manuel via UI (page Import dédiée ou page Events)
- Support SFTP entrant optionnel (dépôt fichier → polling)
- Couvre 100% des besoins POC si le client accepte d'exporter manuellement depuis son ERP
- Templates TSV fournis par Ootils pour chaque entité

### Phase 1 — API REST générique (POC → Pilot)

**Objectif :** permettre à n'importe quel système de pousser des données sans connecteur natif.

- Endpoints REST documentés (OpenAPI 3.1)
- Authentification : API key + HMAC signature optionnelle
- Client script Python open-source (~100 lignes) pour pousser depuis n'importe quel système
- Support webhook entrant (ERP push → Ootils)
- Rate limiting + retry semantics documentés

### Phase 2 — Connecteurs natifs (clients récurrents)

**Objectif :** réduire le coût d'intégration pour les clients enterprise.

- **SAP ECC/S4** : priorité absolue si premier client enterprise est SAP
- **MS Dynamics 365 F&O** : OData REST, connecteur standard
- **WMS générique** (Manhattan, Blue Yonder, etc.) : REST ou SFTP selon capacité WMS
- **EDI 850/856/810** : via middleware EDI (MuleSoft, Boomi, ou script AS2)

### Phase 3 — Outputs vers ERP

**Objectif :** boucle fermée planificateur → ERP, toujours avec validation humaine.

- Export recommandations en TSV téléchargeable (disponible dès Phase 1)
- Push API vers ERP (SAP BAPI create PO, Dynamics PO API) avec écran de validation obligatoire
- Historique des recommandations acceptées / modifiées / rejetées (feedback loop futur ML)

---

## 7. Sécurité et gouvernance des imports

| Aspect | Décision |
|--------|----------|
| Authentification API | API key Bearer + HMAC optionnel (Webhook) |
| Transport | HTTPS obligatoire, SFTP avec clé SSH pour batch |
| Données sensibles | Pas de données personnelles dans les imports (B2B only) |
| Audit trail | Chaque import loggé : source, timestamp, user/system, nb lignes, rejets |
| Idempotence | Import identifié par `import_id` — rejouer un fichier ne crée pas de doublons |
| Retention logs | Logs d'import conservés 90 jours minimum |

---

## 8. Décisions architecturales clés

### DA-1 : external_id comme interface universelle

Tous les imports passent par des codes métier ERP (`external_id`), jamais par des UUIDs Ootils. La table `external_references` est la seule interface entre le monde ERP et le monde Ootils. **Avantage :** isolation totale — un client peut changer son ERP sans que le modèle de données Ootils ne soit impacté.

### DA-2 : Mapping déclaratif en YAML, pas en code

Les règles de transformation ERP → Ootils sont des fichiers YAML versionés dans `config/mappings/`. Aucune logique de mapping n'est hardcodée. **Avantage :** un nouveau client SAP avec un customizing différent = un nouveau fichier YAML, pas un nouveau déploiement. Les mappings sont auditables, modifiables sans redéploiement (config reload).

### DA-3 : Human-in-the-loop non négociable sur les outputs

Ootils ne pousse jamais automatiquement vers un ERP. Toute recommandation passe par une validation planificateur. **Avantage :** confiance des utilisateurs (les planificateurs gardent la main), conformité (traçabilité de chaque décision), et protection contre les erreurs de calcul en production.

---

*Document maintenu par : Architecture Ootils*
*Révision suivante : avant onboarding premier client enterprise (Phase 1→2)*
