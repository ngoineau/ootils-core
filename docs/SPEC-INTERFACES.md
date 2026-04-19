# Ootils Core — Specification Opérationnelle des Interfaces

> Version 1.0 — 2026-04-18
> Statut : **RÉFÉRENCE MIXTE (ACTUEL + CIBLE)** — ce document distingue ce qui existe aujourd'hui de ce qui est seulement proposé (`[PROPOSED]`). Il ne doit pas être lu comme une preuve que toutes les surfaces décrites sont déjà livrées.
> Audiences : (a) intégrateurs humains, (b) connecteurs ERP (SAP / Dynamics / WMS), (c) agents IA (MCP / API directe).
> Règle de lecture : quand une capacité n'est pas explicitement observée dans le code ou dans le runtime validé, elle doit être lue comme cible et non comme fonctionnalité déjà livrée.

---

## 1. Overview & principes

### 1.1 Taxonomie des interfaces

| Direction | Familles | Modes de consommation dominants |
|-----------|----------|---------------------------------|
| **Inbound** (le monde pousse vers Ootils) | Ingest (`/v1/ingest/*`), Events (`/v1/events`), Scenarios (`/v1/simulate`), DQ/Calc triggers | REST JSON sync ; `[PROPOSED]` SFTP batch TSV ; `[PROPOSED]` webhook ERP |
| **Outbound** (Ootils émet / expose) | Graph reads (`/v1/graph`, `/v1/nodes`), Projection, Issues, Explain, BOM, RCCP, Ghosts, Scenarios (GET), DQ reports | REST JSON sync ; `[PROPOSED]` export TSV ; `[PROPOSED]` webhooks signés |
| **Tooling (agent-facing)** | Sous-ensemble curaté d'endpoints + `[PROPOSED]` serveur MCP + `[PROPOSED]` SSE explain | JSON-RPC (MCP) ; SSE ; REST |

### 1.2 Trois modes de consommation

| Audience | Préféré aujourd'hui | Cible (post-Phase 1) |
|----------|---------------------|----------------------|
| **Intégrateur humain** (ops, data engineer) | POST JSON via curl / scripts / client API ; upload TSV manuel via UI = `[PROPOSED]` | SFTP drop + `[PROPOSED]` dashboard de runs d'import + templates TSV versionnés |
| **Connecteur ERP** (SAP/Dynamics/WMS) | REST JSON sync, un endpoint par entité | Webhook ERP→Ootils `[PROPOSED]` + outbound webhook recommandations signé HMAC `[PROPOSED]` |
| **Agent IA** | API REST directe + Bearer token partagé | `[PROPOSED]` MCP server + streaming explain SSE + determinism contract documenté |

### 1.3 Principes structurants

1. **Contract-first.** Aucun changement d'endpoint sans mise à jour de `docs/openapi.json` (généré via `scripts/export_openapi.py`). Les clients peuvent regénérer leurs stubs à chaque release.
2. **`external_id` comme clé d'interface.** Les UUIDs Ootils sont des surrogate keys internes : ne jamais demander aux ERP de les manipuler. Tous les ingest posent sur `external_id` (cf. table `external_references`, migration 007). Les outputs graphe exposent les UUIDs pour l'accès `/v1/explain?node_id=...`, mais l'identité métier reste `external_id`.
3. **Read-heavy côté ERP.** Ootils consomme les données maîtres ; il ne fait **jamais** d'écriture non-sollicitée vers un ERP. (Règle `SPEC-INTEGRATION-STRATEGY §5.2`.)
4. **Human-in-the-loop non négociable sur les outputs.** Pas d'auto-push de recommandations vers SAP/Dynamics même en Phase 3.
5. **Versioning.** Prefix `/v1` aujourd'hui. Schéma proposé :
   - Additive non-breaking → `/v1` + OpenAPI patch version bump (`1.1.x`)
   - Breaking → `/v2` monté en parallèle ; `/v1` marqué `Sunset: <RFC 8594 date>` dans les headers pendant **au moins 6 mois** ; coverage tests maintenus sur les deux versions durant le recouvrement.
6. **Déterminisme.** Les calculs (propagation, shortage, MRP, BOM explode) sont déterministes sur entrée constante. **Exceptions à ne pas pattern-matcher** : `node_id`/`edge_id`/`batch_id`/`event_id` (tous `uuid4()` — cf. `routers/ingest.py:96`, `engine/kernel/graph/store.py`), `created_at`, `updated_at`, `as_of`. Le déterminisme vise les quantités, dates, structures — pas les identifiants générés.

---

## 2. Authentication & transport

### 2.1 État courant

| Mécanisme | Implémentation | Localisation |
|-----------|----------------|--------------|
| Bearer unique global | `OOTILS_API_TOKEN` env var ; validé au démarrage (fail-closed) ; `hmac.compare_digest` anti-timing-attack | `src/ootils_core/api/auth.py:23-59` |
| Transport | HTTP (serveur uvicorn) ; HTTPS est de la responsabilité du reverse-proxy (Caddy / nginx) | `src/ootils_core/api/app.py` |
| Payload size | 10 MB hard cap sur `/v1/ingest/*` (middleware) ; aucun cap sur les autres endpoints | `src/ootils_core/api/app.py:29-47` |
| Rate limiting | **Aucun** | — |
| Scopes / RBAC | **Aucun** — un token = tous les droits | — |
| Signature HMAC webhook | **Aucun** (pas d'endpoint webhook inbound) | — |

### 2.2 Gaps et proposition

| Besoin | Audience prioritaire | Proposition `[PROPOSED]` | Priorité |
|--------|----------------------|--------------------------|----------|
| Tokens multiples (un par client/connecteur) | ERP, intégrateur | Table `api_keys(key_id, hashed_token, client_name, scopes[], created_at, revoked_at)` ; header `Authorization: Bearer ootils_<prefix>_<secret>` avec prefix lookup + hash verify | P0 avant multi-tenant |
| Scopes lecture / écriture | Agent IA (read-only), ERP (write) | Scopes : `ingest:write`, `read:*`, `simulate:write`, `recommendations:approve`, `events:write` | P1 |
| HMAC sur webhook inbound | ERP | Header `X-Ootils-Signature: sha256=<hex>` ; payload signé avec secret partagé par `source_system` | P1 |
| Rate limit | Tous | Token bucket per-client (Redis) : défaut 60 req/min, 5 req/s sur `/v1/simulate` et `/v1/calc/run` (coûteux CPU) ; header `X-RateLimit-*` | P1 |
| Audit log des appels | Sécurité / gouvernance | Table `api_request_log(request_id, token_prefix, path, method, status, latency_ms, correlation_id, ts)` — retention 90j (aligné avec §7 strategy) | P1 |

---

## 3. Inbound interfaces — contrats formels

### 3.1 Ingest master + transactionnel (implémenté)

Tous les endpoints `/v1/ingest/*` partagent le même contrat structurel :

- **Auth** : `Authorization: Bearer <OOTILS_API_TOKEN>` (401 sinon).
- **Content-Type** : `application/json` uniquement (pas de multipart TSV au MVP — cf. commentaire `routers/ingest.py:13`).
- **Body** : objet racine unique `{<entity>: [...], "dry_run": bool}` avec liste de rows (max ~15 colonnes × N rows, body ≤ 10 MB).
- **Sémantique** : validation **all-or-nothing**. Si **une** row échoue (structure ou FK), la requête retourne `422` et **rien n'est persisté** (`routers/ingest.py:80-82`).
- **`dry_run: true`** : toute la validation tourne, aucune écriture, retour `200` avec `status: "dry_run"`.
- **Upsert key** : toujours `external_id` (ou la combinaison PK métier pour les rows à granularité composite, ex. `supplier_items` = `(supplier_external_id, item_external_id)`).
- **Side-effects** : création/mise à jour de `ingest_batches` + `ingest_rows` (migration 007), déclenchement du pipeline DQ (L1/L2) synchrone sur chaque batch (`_trigger_dq()`), émission d'events `ingestion_complete` par nœud créé.
- **Pagination / cursor** : N/A (POST).
- **Rate limit `[PROPOSED]`** : 10 req/min par client sur endpoints d'ingest transactionnel ; 60 req/min sur master data.

| Endpoint | Entité cible | Table(s) métier | External ref key | Notes |
|----------|--------------|------------------|------------------|-------|
| `POST /v1/ingest/items` | Item | `items` | `items.external_id` (UNIQUE) | Master data, création auto si inconnu (`routers/ingest.py:345`) |
| `POST /v1/ingest/locations` | Location | `locations` | `locations.external_id` | `routers/ingest.py:436` |
| `POST /v1/ingest/suppliers` | Supplier | `suppliers` | `suppliers.external_id` | `routers/ingest.py:540` |
| `POST /v1/ingest/supplier-items` | SupplierItem | `supplier_items` | `(supplier_id, item_id)` | `routers/ingest.py:623` |
| `POST /v1/ingest/on-hand` | OnHandSupply | `nodes` (type=`OnHandSupply`) | `(item, location)` | `routers/ingest.py:745` |
| `POST /v1/ingest/purchase-orders` | PurchaseOrderSupply | `nodes` + `external_references(entity_type='purchase_order')` | `external_id` | Transactionnel : FK item/location **obligatoire** (`routers/ingest.py:892`) |
| `POST /v1/ingest/forecast-demand` | ForecastDemand | `nodes` (type=`ForecastDemand`) | `(item, location, bucket, grain)` | `routers/ingest.py:1036` |
| `POST /v1/ingest/resources` | Resource | `resources` + `nodes` (type=`Resource`) | `external_id` | `routers/ingest.py:1201` |
| `POST /v1/ingest/work-orders` | WorkOrderSupply | `nodes` + `external_references(entity_type='work_order')` | `external_id` | `routers/ingest.py:1353` |
| `POST /v1/ingest/customer-orders` | CustomerOrderDemand | `nodes` + `external_references(entity_type='customer_order')` | `external_id` | `routers/ingest.py:1490` |
| `POST /v1/ingest/transfers` | TransferSupply | `nodes` + `external_references(entity_type='transfer')` | `external_id` | Câblé sur la PI de destination (`routers/ingest.py:1628`) |
| `POST /v1/ingest/bom` | BOM | `bom_headers`, `bom_lines` | `parent_external_id` + `bom_version` | Détection de cycles ; recalcul LLC (`routers/bom.py:319`) |
| `POST /v1/ingest/calendars` | OperationalCalendar | `operational_calendars` | `(location, date)` | `routers/calendars.py:121` |
| `POST /v1/ingest/ghosts` | Ghost | `ghost_nodes`, `ghost_members` | `(name, ghost_type, scenario_id)` | `routers/ghosts.py:150` |

#### Exemple complet — `POST /v1/ingest/purchase-orders`

```bash
curl -s -X POST https://api.ootils.io/v1/ingest/purchase-orders \
  -H "Authorization: Bearer $OOTILS_API_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: sap-daily-po-20260418-0930" \
  -d '{
    "purchase_orders": [
      {
        "external_id": "PO-991",
        "item_external_id": "SKU-0042",
        "location_external_id": "DC-PARIS",
        "supplier_external_id": "SUP-ACME",
        "quantity": 500,
        "expected_delivery_date": "2026-04-28",
        "status": "open"
      },
      {
        "external_id": "PO-992",
        "item_external_id": "SKU-0077",
        "location_external_id": "DC-LYON",
        "supplier_external_id": "SUP-ACME",
        "quantity": 1200,
        "expected_delivery_date": "2026-05-03",
        "status": "open"
      }
    ],
    "dry_run": false
  }'
```

Réponse `200 OK` (happy path) :

```json
{
  "status": "ok",
  "summary": {"total": 2, "inserted": 1, "updated": 1, "errors": 0},
  "results": [
    {"external_id": "PO-991", "node_id": "7a1…-4f", "action": "updated"},
    {"external_id": "PO-992", "node_id": "9c4…-02", "action": "inserted"}
  ],
  "batch_id": "b3e6…-aa",
  "dq_status": "passed"
}
```

Réponse `422 Unprocessable Entity` (FK manquante) :

```json
{
  "detail": [
    {
      "external_id": "PO-991",
      "row": 0,
      "errors": ["item_external_id 'SKU-0042' not found in DB"]
    }
  ]
}
```

#### Codes d'erreur standard pour `/v1/ingest/*`

| Code | Cas | Action intégrateur |
|------|-----|--------------------|
| 400 | JSON malformé | Corriger la syntaxe |
| 401 | Token manquant ou invalide | Vérifier `OOTILS_API_TOKEN` |
| 413 | Body > 10 MB | Splitter en chunks (cf. `app.py:37-46`) |
| 422 | Erreur de validation (structurelle ou FK) | Corriger les rows listées dans `detail[]` |
| 500 | Exception interne (handler global masque les détails, `app.py:89-97`) | Vérifier les logs serveur ; remonter avec `X-Correlation-ID` |

#### Idempotency `[PROPOSED]`

Header `Idempotency-Key: <opaque string ≤ 128 chars>` à stocker côté serveur dans une nouvelle colonne `ingest_batches.idempotency_key` (UNIQUE). TTL : **72 h**. Rejeu dans la fenêtre → retour du batch original (200 `idempotent: true`), pas de double-import.

### 3.2 `POST /v1/events` (implémenté)

**Source** : `src/ootils_core/api/routers/events.py:112`

| Propriété | Valeur |
|-----------|--------|
| Purpose | Soumettre un événement supply chain qui peut déclencher une propagation synchrone |
| Auth | Bearer + `X-Scenario-ID` optionnel (défaut baseline) |
| Request schema | `event_type` ∈ `VALID_EVENT_TYPES` (cf. `events.py:53-68`), `trigger_node_id` optionnel, `scenario_id`, `field_changed`, `old_date`/`new_date`, `old_quantity`/`new_quantity`, `source` |
| Response | `202 Accepted` avec `event_id`, `status` ∈ {`queued`, `processed`}, `scenario_id`, `affected_nodes_estimate` |
| Side-effect | Insère dans `events` ; si `trigger_node_id` fourni, construit un `PropagationEngine` per-request (`events.py:23-47`, `events.py:170-184`) et propage **en synchrone** dans la même requête HTTP — best-effort, les erreurs sont loggées mais n'échouent pas la requête |
| Rate limit `[PROPOSED]` | 30 req/s par client — la propagation synchrone est coûteuse |
| Idempotency `[PROPOSED]` | `Idempotency-Key` par `source + business_key + timestamp` ; TTL 24 h |

**Exemple** :

```bash
curl -X POST https://api.ootils.io/v1/events \
  -H "Authorization: Bearer $OOTILS_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "supply_date_changed",
    "trigger_node_id": "7a1b9c2e-4f8a-4b12-9d3e-ff01aabbccdd",
    "field_changed": "expected_delivery_date",
    "old_date": "2026-04-20",
    "new_date": "2026-04-28",
    "source": "sap-webhook"
  }'
```

> **Note opérationnelle** : la propagation étant synchrone (cf. revues architecture — aucune queue, aucun worker), un `POST /v1/events` peut bloquer jusqu'à plusieurs secondes sur un grand graphe. Les connecteurs doivent implémenter un timeout client de **30s** et un retry exponentiel (2s, 8s, 30s, abandon).

### 3.3 `POST /v1/simulate` (implémenté)

**Source** : `src/ootils_core/api/routers/simulate.py:71`

| Propriété | Valeur |
|-----------|--------|
| Purpose | Créer un scénario dérivé de `base_scenario_id` avec une liste d'overrides, propager, et retourner le delta shortages |
| Request | `scenario_name`, `base_scenario_id` (UUID ou `"baseline"`), `overrides: [{node_id, field_name, new_value}]` (champs validés contre `_ALLOWED_FIELDS`) |
| Response | `201 Created` avec `scenario_id`, `override_count`, `failed_overrides[]`, `calc_run_id`, `nodes_recalculated`, `delta: {new_shortages, resolved_shortages, net_shortage_change}` |
| Side-effect | **Deep-copy complet** de tous les nœuds actifs du parent (cf. `engine/scenario/manager.py:93-97`), puis recompute full — coût **O(n_nodes)** par simulation, **pas** de copy-on-write malgré le vocabulaire du vision doc |
| Latence attendue | 1–30 s selon taille du graphe ; mauvais candidat pour des agents exécutant 1000 scénarios / cycle (cf. VISION §6) |
| Rate limit `[PROPOSED]` | 5 req/s par client ; 100 / heure / client |

**Payload exemple** :

```json
{
  "scenario_name": "expedite-po991",
  "base_scenario_id": "baseline",
  "overrides": [
    {
      "node_id": "7a1b9c2e-4f8a-4b12-9d3e-ff01aabbccdd",
      "field_name": "time_ref",
      "new_value": "2026-04-18"
    }
  ]
}
```

### 3.4 `POST /v1/calc/run` (implémenté)

**Source** : `src/ootils_core/api/routers/calc.py:35`

| Propriété | Valeur |
|-----------|--------|
| Purpose | Déclencher manuellement une propagation pour un scénario (ex. après un batch d'ingest massif) |
| Request | `{"full_recompute": false}` — si `true`, marque **tous** les PI dirty avant propagation |
| Response | `calc_run_id`, `status` ∈ {`completed`, `locked`}, `nodes_recalculated`, `nodes_unchanged` |
| Lock | Un seul run simultané par scénario ; retour `status: "locked"` si collision (cf. `calc.py:67-75`) |
| Rate limit `[PROPOSED]` | 2 req/min — opération très coûteuse |

### 3.5 `POST /v1/dq/run/{batch_id}` + `POST /v1/dq/agent/run/{batch_id}` (implémenté)

**Source** : `src/ootils_core/api/routers/dq.py:78` et `dq.py:324`

| Endpoint | Rôle |
|----------|------|
| `POST /v1/dq/run/{batch_id}` | Rejouer les checks L1 (structurel) + L2 (référentiel) sur un batch existant |
| `POST /v1/dq/agent/run/{batch_id}` | Lancer l'agent DQ (stat + temporal + impact SC + LLM) — enrichit `data_quality_issues.impact_score` et `llm_explanation` |

> **Gap connu** (cité des revues antérieures) : l'appel LLM dans `engine/dq/agent/llm_reporter.py:131` n'a **aucun timeout** côté SDK OpenAI. Un lag réseau peut bloquer le worker FastAPI arbitrairement longtemps. Mitigation proposée : `timeout=30.0` côté `client.chat.completions.create(...)` + circuit-breaker.

### 3.6 `[PROPOSED]` Batch TSV drop via SFTP

**État** : planifié `SPEC-INTEGRATION-STRATEGY §3` + roadmap Phase 0, **non implémenté** (aucun service SFTP, aucun watcher, aucun endpoint `POST /v1/ingest/tsv`).

Contrat proposé :

| Élément | Valeur |
|---------|--------|
| Protocole | SFTP avec auth clé SSH par `source_system` |
| Layout | `/inbound/{source}/{entity}/{file}.tsv` — ex. `/inbound/sap_ecc/purchase_orders/purchase_orders_sap_20260418_090000.tsv` |
| Polling | Worker externe (cron / systemd timer) scanne toutes les 60 s, déplace le fichier vers `/processing/...` avant parsing |
| Encoding / format | UTF-8 sans BOM, séparateur `\t`, dates ISO 8601, décimaux `.`, bool `true`/`false` (cf. §3 strategy) |
| Mapping vers endpoint REST | Le worker appelle l'endpoint REST interne équivalent (`POST /v1/ingest/<entity>`) avec chunking 5 000 rows / request |
| Résultat | Fichier déplacé vers `/archive/{source}/{YYYY}/{MM}/` ou `/rejected/{source}/` + rapport JSON `{file}.report.json` listant les rejets |
| Idempotency | Nom de fichier = clé ; replay interdit si déjà dans `archive/` |

### 3.7 `[PROPOSED]` Webhook inbound ERP → Ootils

**État** : planifié `SPEC-INTEGRATION-STRATEGY §6 Phase 1`, **non implémenté**.

Contrat proposé :

| Élément | Valeur |
|---------|--------|
| Endpoint | `POST /v1/webhooks/inbound/{source_system}` |
| Auth | Header `X-Ootils-Signature: sha256=<hex>` — HMAC-SHA256 du raw body avec secret partagé par `source_system` ; rotation supportée via `X-Ootils-Signature-Next` pendant fenêtre de migration |
| Anti-replay | Header `X-Ootils-Timestamp: <unix_seconds>` ; rejet si `|now - ts| > 300s` ; cache des `signature` des 5 dernières minutes pour détecter les replays |
| Payload | Enveloppe commune : `{event_id: uuid, source_event_type: "sap.po.changed", occurred_at: ISO8601, entity: "purchase_order", external_id: "PO-991", data: {...}}` |
| Traitement | Worker transforme `data` vers payload ingest approprié et poste en interne (ne PAS faire la transformation dans le handler HTTP pour éviter les timeouts) |
| Réponse | `202 Accepted` immédiat avec `webhook_id` ; le traitement est async `[PROPOSED]` |
| Retry côté ERP | Le handler Ootils retourne 2xx uniquement si le message est accepté (signature OK + enveloppe valide) ; sinon 4xx (pas de retry) |

---

## 4. Outbound interfaces — contrats formels

### 4.1 Read endpoints (implémenté)

Tous les endpoints ci-dessous partagent : Bearer auth, `X-Scenario-ID` optionnel (default baseline), `application/json` uniquement. Le comportement de filtrage est **offset-based** aujourd'hui — à migrer vers cursor (cf. §6.5).

| Famille | Endpoints | Purpose | Cardinalité | Pagination | Filtres clés | Latence attendue |
|---------|-----------|---------|-------------|------------|--------------|------------------|
| **Graph** | `GET /v1/graph`, `GET /v1/nodes` | Exposer sous-graphe (nœuds+arêtes) pour un `(item, location, scenario)` à profondeur paramétrable ; lister nœuds pour UI/agent | ~10²–10³ nodes / scope | `limit` (max 500) ; pas de cursor | `item_id`, `location_id`, `node_type`, `depth ∈ [1,5]`, `from`/`to` | < 500 ms sur scope bien ciblé |
| **Projection** | `GET /v1/projection`, `GET /v1/projection/portfolio`, `GET /v1/projection/pegging/{node_id}` | Projection d'inventaire bucketisée, synthèse portfolio, arbre de pegging | 1 série = 30–365 buckets | non paginé (borné par horizon) | `item_id`, `location_id`, `grain` (`day`/`week`/`month`), `scenario_id` | < 300 ms (projection) ; 1–3 s (portfolio sur gros dataset) |
| **Issues** | `GET /v1/issues` | Lister les shortages actifs filtrés (severity, horizon, item, location) | 0–10⁴ | `limit` (≤ 1000) + `offset` | `severity`, `horizon_days`, `item_id`, `location_id` | < 500 ms |
| **Explain** | `GET /v1/explain?node_id=<uuid>` | Retourner la chaîne causale (`ExplanationBuilder`) pour un nœud | 1 explanation = ~5–50 steps | N/A | `node_id` obligatoire | 100–800 ms (sync) |
| **Scenarios** | `GET /v1/scenarios`, `GET /v1/scenarios/{id}`, `DELETE /v1/scenarios/{id}` | CRUD scenarios (delete = archive, baseline protégé) | 10–10³ | `limit` + `offset` + `status` | `status` | < 200 ms |
| **BOM** | `GET /v1/bom/{parent_external_id}`, `POST /v1/bom/explode` | Lire BOM actif ou calculer explosion MRP | 1 BOM = 10–500 composants | N/A | `quantity`, `include_substitutes` | < 1 s |
| **RCCP** | `GET /v1/rccp/{resource_external_id}` | Charge vs capacité par bucket (day/week/month) | Horizon typique 12 semaines | N/A | `from_date`, `to_date`, `grain` | < 800 ms |
| **Ghosts** | `GET /v1/ghosts`, `GET /v1/ghosts/{id}`, `POST /v1/ghosts/{id}/run` | Gestion phase transitions + agrégats capacité | 10–100 | non paginé | `ghost_type`, `scenario_id`, `status` | < 500 ms |
| **Calendars** | `GET /v1/calendars/{location_external_id}`, `POST /v1/calendars/working-days` | Jours ouvrés / capacité par location | 365 entries / location / an | non paginé | `from_date`, `to_date`, `working_only` | < 200 ms |
| **Planning params** | `GET /v1/items/planning-params` | Paramètres MRP (lead-time, safety stock, lot size) | 1 par (item, location) actif | non paginé | `item_id`, `location_id` | < 200 ms |
| **DQ** | `GET /v1/dq/issues`, `GET /v1/dq/{batch_id}`, `GET /v1/dq/agent/report/{batch_id}`, `GET /v1/dq/agent/runs` | Issues DQ non résolues, détail par batch, rapport agent | 0–10⁴ issues | `limit` + `offset` (dq/issues, agent/runs) | `severity`, `dq_level`, `entity_type` | < 500 ms |
| **Calc** | `POST /v1/calc/run` (inbound mais outbound par résultat) | Voir §3.4 | — | — | — | 1–30 s |
| **MRP APICS** | `POST /v1/mrp/run`, `POST /v1/mrp/apics/run`, `POST /v1/consumption`, `POST /v1/lot-sizing`, `GET /v1/mrp/apics/llc` | Moteur MRP APICS multi-niveau | — | — | — | 1–60 s selon scope |
| **Events** | `GET /v1/events` | Historique du log d'événements (source of truth pour debug) | 10–10⁶ | `limit` (≤ 500) + `offset` | `event_type`, `scenario_id`, `processed` | < 500 ms |

#### Exemple — lecture portfolio + explain

```bash
# 1) Top-N shortages par impact
curl -H "Authorization: Bearer $TOK" \
  "https://api.ootils.io/v1/issues?severity=high&horizon_days=30&limit=10"

# 2) Pour chaque shortage, récupérer la causalité
curl -H "Authorization: Bearer $TOK" \
  "https://api.ootils.io/v1/explain?node_id=7a1b9c2e-4f8a-4b12-9d3e-ff01aabbccdd"

# 3) Lecture d'une projection pour vérifier
curl -H "Authorization: Bearer $TOK" \
  "https://api.ootils.io/v1/projection?item_id=SKU-0042&location_id=DC-PARIS&grain=week"
```

### 4.2 `[PROPOSED]` Export TSV

**État** : planifié `SPEC-INTEGRATION-STRATEGY §5.3`, **non implémenté** (aucun endpoint `GET /v1/export/*`).

Contrat proposé :

| Endpoint | Entité | Response |
|----------|--------|----------|
| `GET /v1/export/recommendations.tsv?scenario_id=<uuid>&status=APPROVED&since=<ISO>` | Recommandations validées | `Content-Type: text/tab-separated-values` + header `Content-Disposition: attachment; filename=...` ; 1 row / recommendation ; colonnes alignées sur §5.3 strategy |
| `GET /v1/export/shortages.tsv?scenario_id=<uuid>&severity=high&horizon_days=30` | Issues | Idem |
| `GET /v1/export/projection.tsv?item=<ext>&location=<ext>&from=<date>&to=<date>` | Projection | Idem |

Header de shortages proposé :

```
recommendation_type	item_external_id	location_external_id	supplier_external_id	quantity	uom	suggested_date	priority	reason
planned_po	SKU-0042	DC-PARIS	SUP-ACME	500	EA	2026-04-20	high	Shortage detected J+12
```

### 4.3 `[PROPOSED]` Outbound webhook feed

**État** : **non implémenté** — gap majeur pour les intégrations évent-driven.

Design proposé :

| Élément | Valeur |
|---------|--------|
| Souscription | `POST /v1/webhooks/subscriptions {url, event_types: [...], secret, active: true}` (retourne `subscription_id`) |
| Event types | `shortage.detected`, `shortage.resolved`, `scenario.created`, `recommendation.approved`, `ingest.batch.completed`, `ingest.batch.failed`, `calc_run.completed`, `dq.agent.completed` |
| Payload | `{event_id, event_type, occurred_at, scenario_id, data: {...}, version: "1.0"}` |
| Signature | Header `X-Ootils-Signature: sha256=<hex>` (HMAC du raw body avec `subscription.secret`) ; `X-Ootils-Timestamp` anti-replay |
| Garanties de livraison | **At-least-once** ; idempotency via `event_id` côté consommateur |
| Retry policy | Backoff exponentiel : 10 s, 1 min, 5 min, 30 min, 2 h, 6 h ; **max 6 tentatives** ; après échec → `dead_letter_events` |
| Dead-letter | Table `dead_letter_events(event_id, subscription_id, last_error, attempts, moved_at)` ; consultable via `GET /v1/webhooks/dead-letter` ; replay manuel via `POST /v1/webhooks/dead-letter/{id}/replay` |
| Ordre de livraison | **Non garanti** (best-effort FIFO per-subscription) ; les consommateurs doivent traiter par `event_id` + `occurred_at` |

Exemple de payload :

```json
{
  "event_id": "b3e6c4d1-…",
  "event_type": "shortage.detected",
  "occurred_at": "2026-04-18T09:32:17Z",
  "scenario_id": "00000000-0000-0000-0000-000000000001",
  "version": "1.0",
  "data": {
    "node_id": "shortage-SKU0042-DCPARIS-20260428",
    "item_external_id": "SKU-0042",
    "location_external_id": "DC-PARIS",
    "shortage_date": "2026-04-28",
    "shortage_qty": 130,
    "severity": "high",
    "explanation_url": "/v1/explain?node_id=shortage-SKU0042-DCPARIS-20260428"
  }
}
```

### 4.4 `[PROPOSED]` Recommendations push vers ERP (§5.1 roadmap)

**État** : **non implémenté**. Règle de gouvernance inchangée : **human-in-the-loop non négociable** (§5.2 strategy).

Design proposé — machine à états sur `recommendations` :

```
DRAFT ──(planner reviews)──▶ APPROVED ──(push)──▶ SENT ──(ERP ack)──▶ ACCEPTED
  │                               │                    │
  │                               └──(planner)──▶ REJECTED
  └──(planner archives)──▶ DISMISSED                   └──(ERP NACK)──▶ FAILED ──▶ (retry manuel)
```

Endpoints proposés :

| Endpoint | Sémantique |
|----------|------------|
| `GET /v1/recommendations?scenario=<id>&status=DRAFT&type=planned_po` | Lister les recos générées |
| `PATCH /v1/recommendations/{id}` `{status: "APPROVED", approved_by: "user:ngoineau"}` | Transition d'état (seul un planner avec scope `recommendations:approve` peut déclencher `APPROVED`) |
| `POST /v1/recommendations/{id}/push` `{target_system: "sap_ecc"}` | Pousse vers ERP (via connecteur dédié — hors périmètre V1) ; transition `APPROVED → SENT` |
| `GET /v1/recommendations/{id}/history` | Audit trail complet des transitions |

---

## 5. Agent-facing interfaces (AI-native)

C'est le différenciateur structurel de VISION.md. Aujourd'hui : **tout agent utilise les endpoints REST directement**. Cible : un serveur MCP curaté + streaming + contrat de déterminisme.

### 5.1 `[PROPOSED]` MCP server surface

Objectif : exposer un **sous-ensemble minimal et sémantique** des endpoints existants, enrichi de docs "when to call", pour qu'un agent LLM puisse opérer sans connaître l'ensemble de l'OpenAPI.

| Tool name | Type | Input schema (essentiel) | Output schema | When to call |
|-----------|------|--------------------------|---------------|--------------|
| `query_shortages` | read | `{severity?, horizon_days?, item_external_id?, location_external_id?, limit?}` | `{issues: [{node_id, item, location, date, shortage_qty, severity}], total}` | L'agent veut les top-N problèmes actuels |
| `explain_shortage` | read | `{node_id: string}` | `{summary, causal_path: [...], root_cause_node_id}` | L'agent vient de sélectionner un shortage et veut comprendre la cause |
| `get_projection` | read | `{item_external_id, location_external_id, grain?, scenario_id?}` | `{buckets: [...], safety_stock_qty}` | Vérifier la trajectoire d'inventaire avant/après une hypothèse |
| `list_supply_nodes` | read | `{item_external_id, location_external_id, node_types: ["PurchaseOrderSupply","WorkOrderSupply"], scenario_id?}` | `{nodes: [...]}` | L'agent a besoin des PO candidats à "expediter" |
| `create_scenario` | write | `{name, base_scenario_id?, overrides: [{node_id, field_name, new_value}]}` | `{scenario_id, override_count, failed_overrides, delta}` | Simulation — wrapper direct autour de `POST /v1/simulate` |
| `run_simulation` | write | `{scenario_id, full_recompute?}` | `{calc_run_id, nodes_recalculated, status}` | Re-propager après modifs ; wrapper de `POST /v1/calc/run` |
| `submit_recommendation_for_review` | write | `{type, item, location, supplier?, qty, uom, suggested_date, priority, reason}` | `{recommendation_id, status: "DRAFT"}` | Stockage d'une reco agent pour validation planner (`[PROPOSED]` — voir §4.4) |
| `get_bom_explosion` | read | `{item_external_id, quantity, location_external_id?}` | `{components: [{item, gross, net, llc}]}` | Évaluer faisabilité avant de recommander un WO |

**Protocole** : JSON-RPC 2.0 sur stdio (cas Claude Desktop) ou HTTP (cas agents serveurs). Le serveur MCP vit dans `src/ootils_core/mcp/` **`[PROPOSED]`** et réutilise les mêmes dépendances DB/auth que l'API FastAPI (injection par token client dédié `mcp-agent:<name>`).

**Invariant agent-safe** : les tools `read` sont **side-effect-free** ; les tools `write` retournent toujours un identifiant opaque pour que l'agent puisse référencer l'action dans ses logs, sans jamais avoir à deviner un UUID.

### 5.2 `[PROPOSED]` Streaming explain (SSE)

L'actuel `GET /v1/explain` est synchrone : il construit l'ensemble du `causal_path` puis renvoie un JSON unique (`explain.py:39`). Pour des agents qui veulent raisonner dès que la première étape est disponible (chain-of-thought progressif) :

```
GET /v1/explain/stream?node_id=<uuid>
Accept: text/event-stream

event: step
data: {"step": 1, "node_id": "co-CO778", "node_type": "CustomerOrderDemand", "fact": "…"}

event: step
data: {"step": 2, "node_id": "onhand-SKU0042-DCPARIS", "node_type": "OnHandSupply", "fact": "…"}

event: done
data: {"explanation_id": "e5a…", "root_cause_node_id": "po-PO991"}
```

Contrat :
- `event: step` pour chaque élément du `causal_path` au fil de la construction.
- `event: done` avec les métadonnées finales.
- `event: error` avec `{error_code, message}` en cas d'échec partiel ; la connexion est fermée.
- Heartbeat `event: ping` toutes les 15 s pour éviter les coupures proxy.

### 5.3 Contrat de déterminisme pour agents

| Garantie | Portée |
|----------|--------|
| **Idempotence calcul** | Mêmes nœuds + mêmes edges + même `scenario_id` → mêmes `closing_stock`, `shortage_qty`, `time_ref`, `causal_path.fact`, `root_cause_node_id` |
| **Pas de randomness kernel-side** | Aucun `random`, aucun `uuid4` dans les **règles de calcul** ; les `uuid4()` apparaissent uniquement pour les **identifiants de nouveaux objets** (PlannedSupply créés par MRP, batch_id, event_id) |
| **Non-déterministe (à ne pas pattern-matcher)** | `node_id`, `edge_id`, `batch_id`, `event_id`, `calc_run_id`, `explanation_id`, `created_at`, `updated_at`, `as_of` |
| **Ordre des collections** | `causal_path` ordonné par `step` ASC (stable) ; `issues[]` ordonné par `severity_score` DESC (stable) ; `graph.nodes/edges` ordonnés par UUID ASC (stable mais dépendant du UUID — ne pas utiliser comme signal) |
| **Violation connue** | `scenarios.create_scenario` émet des nouveaux `node_id` par copy ; un agent qui compare deux scénarios doit joindre via `(item_id, location_id, node_type, time_ref)` — **jamais** par `node_id` du baseline (cf. `engine/scenario/manager.py:538`) |

Un agent qui détecte une non-reproductibilité au-delà des champs ci-dessus **doit signaler un bug**.

---

## 6. Cross-cutting concerns

### 6.1 Error model `[PROPOSED]` — enveloppe normalisée

L'actuel handler global (`app.py:89-97`) renvoie `{error, message, status}` et **masque toute info** pour 500. Les autres endpoints retournent du FastAPI standard (`detail: string | list`) — incohérent.

Proposition d'enveloppe unique pour **tous** les endpoints :

```json
{
  "error": {
    "code": "ingest.validation.fk_missing",
    "message": "item_external_id 'SKU-0042' not found in DB",
    "status": 422,
    "correlation_id": "req_01HF…",
    "details": [
      {"row": 0, "external_id": "PO-991", "field": "item_external_id"}
    ],
    "docs_url": "https://docs.ootils.io/errors/ingest.validation.fk_missing"
  }
}
```

Table de codes minimale :

| Code namespace | Exemples | HTTP typique |
|----------------|----------|--------------|
| `auth.*` | `auth.token.missing`, `auth.token.invalid`, `auth.scope.denied` | 401 / 403 |
| `validation.*` | `validation.schema`, `validation.enum`, `validation.fk_missing`, `validation.cycle_detected` (BOM) | 422 |
| `idempotency.*` | `idempotency.conflict` (même clé, payload différent) | 409 |
| `ratelimit.*` | `ratelimit.exceeded` | 429 (+ `Retry-After`) |
| `upstream.*` | `upstream.llm_timeout`, `upstream.db_unavailable` | 502 / 503 |
| `internal.*` | `internal.error`, `internal.propagation_failed` | 500 |
| `not_found.*` | `not_found.scenario`, `not_found.item`, `not_found.node` | 404 |

### 6.2 Idempotency `[PROPOSED]`

- **Header** : `Idempotency-Key: <opaque ≤ 128 chars>` sur **tout POST** qui crée un état (`/v1/ingest/*`, `/v1/events`, `/v1/simulate`, `/v1/calc/run`, `/v1/dq/run/*`).
- **Stockage** : étendre `ingest_batches` avec colonne `idempotency_key UNIQUE` pour les ingest ; créer `api_idempotency(key, client_id, method, path, request_hash, response_json, status_code, expires_at)` pour les autres.
- **TTL** : 72 h pour `/v1/ingest/*` (typique d'un rejeu nightly) ; 24 h pour `/v1/events` ; 1 h pour `/v1/simulate`.
- **Replay** : même clé + même `request_hash` → retourner la réponse stockée avec header `X-Idempotent-Replay: true`. Même clé + hash différent → `409 idempotency.conflict`.

### 6.3 Versioning

- **Aujourd'hui** : `/v1` préfixe tous les routers (cf. chaque `APIRouter(prefix="/v1/...")`).
- **Proposé** :
  - Changements additifs (nouveau champ optional, nouvel endpoint) → même `/v1`, bump patch OpenAPI (`info.version: "1.1.0"`).
  - Changements breaking → nouveau `/v2` monté en parallèle. `/v1` continue à fonctionner, mais chaque réponse porte :
    - `Deprecation: version="v1"`
    - `Sunset: Sat, 18 Oct 2026 00:00:00 GMT` (RFC 8594)
    - `Link: </v2/...>; rel="successor-version"`
  - Période de recouvrement **minimum 6 mois** ; 12 mois recommandés pour les clients ERP.

### 6.4 Observability hooks `[PROPOSED]`

| Élément | Contrat |
|---------|---------|
| Header entrant | `X-Correlation-ID` (optional) — si absent, généré `req_<ulid>`, renvoyé dans la réponse |
| Header sortant | `X-Correlation-ID`, `X-API-Version: 1.0.0`, `X-Calc-Run-ID` (sur endpoints qui déclenchent une propagation) |
| Logs structurés | JSON avec champs : `ts`, `level`, `correlation_id`, `method`, `path`, `status`, `latency_ms`, `client_id`, `scenario_id`, `error_code?` |
| Trace spans | OpenTelemetry : span name = `POST /v1/ingest/purchase-orders` ; attributs `ootils.scenario_id`, `ootils.batch_id`, `ootils.entity_type`, `ootils.row_count` |
| Health | `GET /health` (implémenté, `app.py:66`) — à enrichir avec `GET /health/deep` vérifiant DB + LLM + migrations appliquées |

### 6.5 Pagination & filtering `[PROPOSED]` — migration vers cursor

**État courant** : toutes les listes utilisent `limit` + `offset` (`events.py:198`, `issues.py:62`, `scenarios.py:42`, `dq.py:134`). Problème : coût linéaire à mesure que l'offset grandit (`ORDER BY created_at DESC LIMIT X OFFSET Y`) et instable si des rows apparaissent / disparaissent entre appels.

Proposition pour les listes à forte cardinalité (`/v1/events`, `/v1/issues`, `/v1/nodes`, `/v1/dq/issues`, `[PROPOSED]` `/v1/recommendations`) :

```
GET /v1/events?limit=100&cursor=eyJjcmVhdGVkX2F0IjoiMjAyNi0wNC0xOFQwOToxOVoiLCJldmVudF9pZCI6IjdhLi4uIn0
```

Cursor = base64 JSON `{created_at, event_id}`. Réponse :

```json
{
  "events": [...],
  "page": {
    "next_cursor": "eyJjcmVhd...",
    "has_more": true,
    "limit": 100
  }
}
```

Endpoints petits (≤ 10³ rows) peuvent rester `limit`+`offset`.

---

## 7. Audit — dit vs fait

| Specced (source §) | Implémenté ? | Gap / action |
|---------------------|--------------|--------------|
| §2 — Excel/TSV manuel (P0) | **Partiel** — API REST JSON fait, pas d'upload TSV UI ni SFTP | Ajouter `POST /v1/ingest/<entity>:tsv` multipart + worker SFTP (cf. §3.6) |
| §2 — API REST générique (P0) | **OK** — 14 endpoints d'ingest listés §3.1 | — |
| §2 — SAP (BAPI/RFC) (P1) | **Non** | Hors scope V1 |
| §2 — MS Dynamics (P1) | **Non** | Hors scope V1 |
| §2 — WMS générique (P1) | **Non** | Hors scope V1 |
| §2 — EDI 850/856/810 (P1) | **Non** | Hors scope V1 |
| §2 — Webhook ERP (P2) | **Non** | Concevoir `POST /v1/webhooks/inbound/{source}` avec HMAC (§3.7) |
| §2 — Recommendations → ERP (P2) | **Non** | Concevoir state machine + endpoints `/v1/recommendations/*` (§4.4) |
| §3 — Format TSV standard (UTF-8, tab, ISO dates) | **Non matérialisé** — pas de parser TSV | Documenter format + livrer templates + livrer parser |
| §3 — Nommage fichiers `{type}_{source}_{date}.tsv` | **Non** | Adopter dans le worker SFTP |
| §3 — `external_references` (mapping ERP→UUID) | **OK** — migration 007 §2 | — |
| §3 — Master data auto-create vs transactionnel rejet | **OK** — confirmé dans `ingest.py` (items/locations créent, PO/CO rejettent si FK inconnue) | — |
| §4.1 — Mapping SAP MARA/T001W/EKKO/... | **Non** | Pas de YAML `config/mappings/` — à produire lors du premier client SAP |
| §4.2 — Mapping YAML déclaratif | **Non** | Dossier `config/mappings/` absent |
| §4.3 — Pipeline 7-step (structure→lookup→transform→business→upsert→audit→response) | **Partiel** — `ingest.py` fait structure + FK + upsert + audit (`ingest_batches`) + response ; transformation YAML et validation métier complexe = non |
| §5.1 — Types de recos (`planned_po`, `planned_wo`, `shortage_alert`, `simulation_result`) | **Non typés** — les shortages sont exposés mais il n'y a pas de table `recommendations` avec type discriminant | Créer table + endpoints (§4.4) |
| §5.2 — Human-in-the-loop (aucune auto-push) | **OK par absence** — aucun push ERP du tout | Garder la règle dans le design §4.4 |
| §5.3 — Export TSV recommandations | **Non** | Endpoint `GET /v1/export/recommendations.tsv` (§4.2) |
| §6 Phase 0 — Upload TSV via UI | **Non** | UI Import absente |
| §6 Phase 0 — SFTP polling | **Non** | Worker à écrire |
| §6 Phase 1 — API key + HMAC optionnel | **Partiel** — Bearer unique OK, HMAC absent | Implémenter HMAC sur webhooks (§2.2) |
| §6 Phase 1 — Client script Python ~100 lignes | **Non** | À livrer avec la 1re release publique |
| §6 Phase 1 — Rate limiting | **Non** | Implémenter (§2.2) |
| §6 Phase 1 — Webhook entrant | **Non** | Concevoir (§3.7) |
| §6 Phase 2 — Connecteurs natifs | **Non** | Post-PMF |
| §6 Phase 3 — Export TSV + Push ERP | **Non** | §4.2 + §4.4 |
| §7 — HTTPS obligatoire | **Délégué** au reverse-proxy | À documenter dans `INFRA-RUNBOOK.md` |
| §7 — Audit trail chaque import | **Partiel** — `ingest_batches` existe mais `submitted_by` rarement peuplé (handler global sans contexte user) | Ajouter propagation `client_id` → `submitted_by` |
| §7 — Idempotence `import_id` | **Non** — aucune `idempotency_key` sur `ingest_batches` | Ajouter (§6.2) |
| §7 — Retention logs 90j | **Non automatisé** | Job cron pg `DELETE FROM events WHERE created_at < now() - interval '90 days'` |
| §7 — DA-1 `external_id` seule interface | **OK** — validé dans `ingest.py` (tous les `*_external_id` en entrée) | — |
| §7 — DA-2 Mapping YAML déclaratif | **Non** | Répertoire `config/mappings/` à créer |
| §7 — DA-3 Human-in-the-loop outputs | **OK par absence** | Garder la règle quand §4.4 sera implémenté |

---

## 8. Roadmap par maturité d'interface

| Interface | Stade | Justification |
|-----------|-------|---------------|
| `/v1/ingest/*` (REST JSON) | **Hardened** | 14 endpoints couvrant le périmètre master + transactionnel ; all-or-nothing + dry_run + DQ pipeline |
| `/v1/events` + `/v1/calc/run` | **MVP** | Fonctionne mais propagation synchrone sans queue ⇒ timeouts probables en prod |
| `/v1/simulate` | **MVP** | Full-clone au lieu de copy-on-write ⇒ inadapté à 1000 scénarios/cycle (VISION §6) |
| `/v1/graph`, `/v1/projection`, `/v1/issues`, `/v1/explain` | **Hardened** | Contrats stables, bien testés ; latence acceptable sur scope ciblé |
| `/v1/bom/*`, `/v1/rccp/*`, `/v1/mrp/*`, `/v1/ghosts/*` | **MVP** | Fonctionnels, mais pas d'OpenAPI examples complets, peu de tests agent-facing |
| `/v1/dq/*` | **MVP** | L1/L2 OK, agent DQ OK mais bloquant sur LLM sans timeout |
| Upload TSV / SFTP | **Gap** | Non implémenté |
| Webhook inbound ERP | **Gap** | Non implémenté |
| Outbound webhook feed | **Gap** | Non implémenté |
| Export TSV | **Gap** | Non implémenté |
| Recommendations API + push ERP | **Gap** | Non implémenté |
| MCP server | **Gap** | Aucun code |
| Explain streaming (SSE) | **Gap** | Aucun code |
| Auth multi-client + scopes | **Gap** | Bearer unique seulement |
| Idempotency `Idempotency-Key` | **Gap** | Aucune déduplication des POST |
| Rate limiting | **Gap** | Aucun |
| Cursor pagination | **Gap** | Offset uniquement |

### 8.1 Top-5 interfaces à construire ensuite

1. **Idempotency + rate limit sur `/v1/ingest/*`** — une erreur réseau sur un batch nocturne peut doubler des PO aujourd'hui. Faible coût, énorme réduction de risque avant onboarding du premier client récurrent.
2. **Outbound webhook feed signé HMAC** — aujourd'hui, un consommateur (agent, UI, Slack, Jira) qui veut réagir à un shortage doit poller `/v1/issues`. Le webhook débloque l'ensemble de l'écosystème d'intégration event-driven sans attendre les connecteurs ERP.
3. **Machine à états `recommendations` + export TSV** — permet le flow "Ootils calcule → planner valide → export vers ERP" sans construire de connecteur SAP. Unblock la Phase 3 sans Phase 2.
4. **MCP server minimal (6 tools)** — prouve la thèse AI-native de VISION.md. Démarre avec `query_shortages`, `explain_shortage`, `get_projection`, `list_supply_nodes`, `create_scenario`, `run_simulation`. Les tools `write` sensibles (push ERP) restent hors MCP.
5. **Async propagation + queue worker** — sans ça, `/v1/simulate` ne scale pas pour des agents exécutant 1000 scénarios/cycle. Passer à une queue (Redis Streams ou Postgres LISTEN/NOTIFY) et transformer `POST /v1/simulate` en 202 + polling `/v1/scenarios/{id}/delta`.

---

*Document maintenu par : Architecture Ootils. Prochaine révision : après livraison du MCP minimal ou de la première intégration ERP réelle.*
