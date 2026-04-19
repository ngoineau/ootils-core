# Ootils Core — Specification Opérationnelle de l'UI Static-Data

> Version 1.0 — 2026-04-18
> Statut : **SPÉCIFICATION CIBLE** — ce document décrit une surface UI non livrée à date. Il ne doit pas être lu comme une preuve d'existence d'une UI dans le runtime actuel.
> Audience : équipe front-end (stack React Admin / Vite / TS), équipe back-end (contrats REST à ajouter), product owner, reviewers doctrine.
> Document de cible produit : aucune existence d'UI ne doit être inférée de cette spécification.

---

## §1 — Doctrine & périmètre

### 1.1 La doctrine corrigée

> **Si une UI Ootils est livrée, son seul périmètre légitime en v1 sera la curation des données maîtres, jamais la décision opérationnelle.**

Les agents IA et les intégrations ERP pilotent la surface de décision (simulate, explain, recommendations, events). Les humains qui entrent dans l'UI viennent corriger un `lead_time_days` manquant, archiver un fournisseur obsolète, résoudre une issue DQ — rien de plus. Toute feature proposée qui *déplace la décision dans un écran* (dashboard d'alertes, what-if visuel, approbation de planning) est hors-doctrine et doit être refusée en revue.

### 1.2 Proposition de doctrine à arbitrer avant implémentation

#### `CONTRIBUTING.md` — remplacer lignes 61-62

```diff
-**API first, UI never (for now)**
-We build the engine. The interface is someone else's problem for now. Do not propose UI features in V1.
+**API-first, UI only for master-data curation**
+The engine is consumed by agents and ERPs via API. The only legitimate UI surface in V1 is a thin
+admin console for humans who need to fix master data (items, suppliers, supplier-items, planning
+params) and triage DQ issues. Do not propose decision-surface UI (dashboards, approval queues for
+recommendations, what-if visualizers) — those belong to agents and integrated systems.
```

*Justification si cette doctrine est validée* : la règle originelle était défensive contre la dérive "Kinaxis-with-AI". Six mois plus tard, le gap opérationnel est clair : sans UI de curation, chaque correction de master data devient un ticket IT ou un `curl POST /v1/ingest/items`, ce qui tue l'adoption pilote. La règle est reformulée pour interdire la dérive (*UI never for decisions*) tout en autorisant le minimum vital (*UI only for curation*).

#### `VISION.md` — remplacer lignes 98-99

```diff
-**We are not building a UI product.**
-Ootils is infrastructure. Interfaces will be built on top of it.
+**We are not building a UI product.**
+Ootils is infrastructure. The decision surface (simulate, explain, recommendations) is agent-
+and API-only. A thin admin console for master-data curation is in-scope as tooling, not as
+product — it exists so humans can keep the inputs clean, not so humans can take planning decisions.
```

*Justification à committer* : même logique que ci-dessus, formulée sur le registre du vision doc (*what we are not building*). Le paragraphe admet une exception nominative (*master-data curation*) pour verrouiller l'interprétation.

### 1.3 Les 5 écrans v1

| # | Écran | Clé métier | Table(s) sous-jacente(s) | Lecture/écriture |
|---|-------|------------|--------------------------|------------------|
| 1 | **Articles** (Items) | `items.external_id` | `items` (migration 002:37-47) | R/W, soft-delete via `status='obsolete'` |
| 2 | **Fournisseurs** (Suppliers) | `suppliers.external_id` | `suppliers` (migration 007:154-166) | R/W, soft-delete via `status='inactive'` |
| 3 | **Conditions fournisseurs** (Supplier-items) | `(supplier_external_id, item_external_id)` | `supplier_items` (migration 007:168-181) | R/W, archive via `valid_to` |
| 4 | **Paramètres de planification** (Planning params) | `(item_external_id, location_external_id)` SCD2 | `item_planning_params` (migration 007:211-260) | R/W, nouvelle version SCD2 |
| 5 | **Inbox DQ** (Data Quality Queue) | `data_quality_issues.issue_id` | `data_quality_issues` (migration 007:129-145) | R + state transitions (resolve / ignore / assign) |

### 1.4 Hors-scope v1

| Famille | Entrera en | Raison du rejet v1 |
|---------|-----------|--------------------|
| Locations CRUD | v2 | 1 location nouvelle / trimestre typique — ingest manuel via `POST /v1/ingest/locations` suffit |
| BOM editor | v2 | Éditeur arborescent = dev coûteux ; import TSV + viewer read-only suffisent pour pilote |
| Calendars editor | v2 | Même logique que BOM |
| Scenarios & simulate | **Jamais** | Surface de décision — agents/API uniquement |
| Recommendations approval | **Jamais** | Surface de décision — agents/API uniquement |
| Explain viewer | **Jamais** | Surface de décision — agents/API uniquement (cf. §8) |
| Import TSV upload | v2 | Existe en CLI / `POST /v1/ingest/*` ; l'UI upload vient après JWT |
| Multi-tenant switcher | v2+ | Mono-VM mono-client v1 |
| Audit log "qui a changé quoi" | v2 | Nécessite users ; reporté avec JWT |

### 1.5 Pourquoi ces 5 et pas 4 ou 6

- **Articles + Fournisseurs + Supplier-items** sont le triptyque master-data incontournable de la planif achats. Supprimer l'un casse le parcours "corriger un LT manquant".
- **Planning params** est l'écran où se concentre 80% des interventions opérateur en pilote (safety stock, lot size, lead time policy) — retirer l'écran force le client à ré-ingester un fichier complet pour changer une valeur.
- **Inbox DQ** est la justification même de la doctrine *"humains curent les inputs"*. Sans elle, la pipeline DQ émet des issues que personne ne voit — l'engine ne fait qu'empiler de la dette.
- **Locations** est exclu parce que la cardinalité réelle en pilote (5-20 entrées) ne justifie pas l'écran : un POST/import initial et plus rien pendant 6 mois. Coût d'opportunité clair.
- **BOM editor** est exclu parce qu'un éditeur arbo correct ≈ 2-3 semaines de dev ; et le client pilote manipule son BOM dans son PLM, pas dans l'APS.

---

## §2 — Architecture frontend

### 2.1 Layout mono-repo

```
C:/dev/Ootils/
├── src/                          # back-end Python existant (ootils_core/)
├── tests/                        # tests pytest existants
├── frontend/                     # [PROPOSED] — nouveau
│   ├── public/                   # favicon, logo, statiques non bundlés
│   ├── src/
│   │   ├── main.tsx              # entrée Vite
│   │   ├── App.tsx               # <Admin> React Admin racine
│   │   ├── dataProvider.ts       # adapter REST → RA (cf. §2.5)
│   │   ├── authProvider.ts       # gestion token (cf. §5)
│   │   ├── i18n/
│   │   │   ├── fr.ts             # défaut
│   │   │   └── en.ts             # fallback
│   │   ├── resources/
│   │   │   ├── items/            # List, Edit, Create, Show
│   │   │   ├── suppliers/
│   │   │   ├── supplierItems/
│   │   │   ├── planningParams/
│   │   │   └── dqIssues/         # custom — pas un CRUD standard
│   │   ├── components/           # primitives partagées (ErrorBanner, ExternalIdField…)
│   │   ├── hooks/                # useCorrelationId, useTokenRotationDetector
│   │   └── lib/                  # errorEnvelope.ts, cursorPagination.ts
│   ├── tests/
│   │   └── e2e/                  # Playwright — 20 journeys (cf. §6)
│   ├── index.html
│   ├── vite.config.ts
│   ├── tsconfig.json
│   ├── tailwind.config.ts
│   ├── playwright.config.ts
│   ├── package.json
│   └── pnpm-lock.yaml
├── docker-compose.yml
├── docker-compose.dev.yml        # [PROPOSED] — hot-reload dev loop (cf. §7)
├── Dockerfile                    # stage 1 build frontend, stage 2 run api (cf. §2.3)
├── pyproject.toml
└── docs/
```

**Invariants** :
- Un seul `package.json` au repo root si on adopte workspaces ; **choix v1** : pas de workspaces, `frontend/package.json` autonome. Motif : mono-repo ne veut pas dire "mono-package-manager" — le back reste `pip`/`pyproject`, le front reste `pnpm`/`package.json`, pas de liant JS au niveau racine.
- Tests E2E dans `frontend/tests/e2e/` — **pas** dans `tests/` Python (Playwright ≠ pytest).

### 2.2 Build

**Vite + TypeScript strict + React 18**. Sortie figée dans `frontend/dist/`.

```ts
// vite.config.ts — [PROPOSED] (sketch)
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: 'dist',
    sourcemap: true,
    target: 'es2022',
  },
  server: {
    port: 5173,
    proxy: {
      '/v1': 'http://localhost:8000',
      '/health': 'http://localhost:8000',
    },
  },
})
```

```json
// tsconfig.json — [PROPOSED] (essentiel)
{
  "compilerOptions": {
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "exactOptionalPropertyTypes": true,
    "target": "ES2022",
    "jsx": "react-jsx",
    "module": "ESNext",
    "moduleResolution": "Bundler"
  }
}
```

### 2.3 Deploy — same-process static mount

**Décision** : le `frontend/dist/` est **monté sous `/app/*`** par le même process FastAPI/uvicorn qui sert l'API. Un seul port (`:8000`), un seul container, un seul process.

Implémentation `[PROPOSED]` (à ajouter dans `src/ootils_core/api/app.py` juste après l'enregistrement des routers) :

```python
# [PROPOSED]
from fastapi.staticfiles import StaticFiles
from pathlib import Path
_FRONTEND_DIST = Path(__file__).parents[4] / "frontend" / "dist"
if _FRONTEND_DIST.exists():
    application.mount("/app", StaticFiles(directory=_FRONTEND_DIST, html=True), name="app")
```

Le `Dockerfile` devient **multi-stage** `[PROPOSED]` :

```dockerfile
# stage 1: build frontend
FROM node:20-slim AS frontend-build
WORKDIR /build
COPY frontend/package.json frontend/pnpm-lock.yaml ./
RUN npm i -g pnpm@9 && pnpm i --frozen-lockfile
COPY frontend/ .
RUN pnpm build

# stage 2: run api with dist baked in
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
COPY src/ /app/src/
COPY scripts/ /app/scripts/
COPY --from=frontend-build /build/dist /app/frontend/dist
RUN pip install --no-cache-dir .
CMD ["uvicorn", "ootils_core.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
```

*Considéré et rejeté* : **nginx séparé** servant `/app/*` + reverse-proxy `/v1/*`. Rejeté parce que (a) le projet est mono-VM mono-tenant, deux containers doublent la surface opérationnelle sans bénéfice mesurable en v1, et (b) Caddy/nginx est déjà présent en front HTTPS de la VM (cf. `docs/INFRA-RUNBOOK.md`) — ajouter un second nginx dédié au static serving est redondant.

*Considéré et rejeté* : **page séparée déployée sur CDN/Vercel**. Rejeté parce que même token partagé = même origin souhaitée (pas de CORS) ; l'hébergement externe force un domaine séparé et un setup CORS qui n'apporte rien en mono-client.

### 2.4 Package manager : **pnpm**

**Choix** : pnpm 9.x. Motif : store global `~/.pnpm-store` = builds CI 2-3× plus rapides que npm, `pnpm-lock.yaml` plus déterministe qu'un `package-lock.json` en présence de peer-deps (React Admin tire 200+ packages). *Considéré et rejeté* : yarn (bon mais Berry introduit `.pnp.cjs` qui casse certains plugins Vite).

### 2.5 Data provider — `ra-data-simple-rest` + adapter custom

React Admin (RA) attend un `DataProvider` qui expose `getList`, `getOne`, `create`, `update`, `delete`, `getMany`, `getManyReference`, `updateMany`, `deleteMany`. On wrappe `ra-data-simple-rest` pour :

1. **Injecter** `Authorization: Bearer <token>` et `X-Correlation-ID`.
2. **Traduire** la pagination cursor-based (SPEC-INTERFACES §6.5) → le modèle `Range`/`X-Total-Count` attendu par RA.
3. **Traduire** l'enveloppe d'erreur Ootils (SPEC-INTERFACES §6.1 `{error: {code, message, status, correlation_id, details}}`) → `HttpError` RA.
4. **Injecter** `Idempotency-Key` sur `create` et `update` (SPEC-INTERFACES §6.2).

Contrat d'adapter `[PROPOSED]` :

```ts
// frontend/src/dataProvider.ts — [PROPOSED] (contract)
import simpleRestProvider from 'ra-data-simple-rest'
import { fetchUtils, DataProvider, HttpError } from 'react-admin'

const API_BASE = '/v1'   // même origin, cf. §2.3

const httpClient = async (url: string, options: fetchUtils.Options = {}) => {
  const headers = new Headers(options.headers ?? { Accept: 'application/json' })
  const token = sessionStorage.getItem('ootils_token')          // cf. §5.1
  if (token) headers.set('Authorization', `Bearer ${token}`)
  headers.set('X-Correlation-ID', crypto.randomUUID())
  if (options.method === 'POST' || options.method === 'PUT') {
    headers.set('Idempotency-Key', crypto.randomUUID())
  }
  try {
    return await fetchUtils.fetchJson(url, { ...options, headers })
  } catch (err: any) {
    // Translate Ootils error envelope → RA HttpError
    const envelope = err.body?.error
    if (envelope) {
      throw new HttpError(envelope.message, envelope.status, {
        code: envelope.code,
        correlationId: envelope.correlation_id,
        details: envelope.details,
      })
    }
    throw err
  }
}

// Cursor pagination wrapper: translate {page, perPage} → {cursor, limit}
// and cache next_cursor per resource+filter signature.
export const dataProvider: DataProvider = wrapCursorPagination(
  simpleRestProvider(API_BASE, httpClient),
)
```

Le `wrapCursorPagination` (à livrer dans `frontend/src/lib/cursorPagination.ts`) maintient un cache `Map<signature, cursor>` et convertit `getList({pagination: {page, perPage}})` en `GET /v1/<resource>?cursor=...&limit=<perPage>`. En page 1 il ne passe pas de cursor. Dès que la réponse renvoie `page.next_cursor`, il le stocke indexé par `(resource, filter, sort)`.

### 2.6 TanStack Query — délimitation avec le cache RA

RA embarque son propre cache via `react-query` (c'est littéralement TanStack Query sous le capot). **Règle de frontière** :

| Famille d'appel | Outil |
|----------------|-------|
| CRUD sur les 4 entités RA (items, suppliers, supplier-items, planning-params) | `<Admin>` + `dataProvider` (cache RA géré automatiquement) |
| Inbox DQ (liste + state transitions) — **pas un CRUD** RA natif | `useQuery` / `useMutation` TanStack directement |
| Appels non-entité (ex. `GET /health/deep` pour badge de statut, `GET /v1/dq/stats` pour compteur header) | `useQuery` TanStack directement |

En pratique, le `QueryClient` est partagé : RA expose le sien via `useQueryClient()`. On reprend cette instance dans les hooks custom Inbox DQ pour éviter deux caches concurrents.

### 2.7 Tailwind + Material UI — coexistence

**Décision** : **MUI pour les surfaces RA natives** (List, Edit, Create, Datagrid, Filter, Toolbar), **Tailwind pour les composants custom** (Inbox DQ panel, ErrorBanner, ExternalIdField, pages hors resources).

Motif : (1) RA est tellement couplé à MUI que remplacer MUI = forker RA ; (2) le v1 a exactement **un** écran custom significatif (Inbox DQ), Tailwind y économise 2-3 jours de wiring MUI pour un layout queue/détail. *Considéré et rejeté* : thème MUI pur (rejeté parce que l'Inbox DQ a un layout split-pane qui est plus propre en Tailwind qu'en `<Grid>` MUI) ; Tailwind pur avec override RA (rejeté, coût de rewrite trop élevé).

Garde-fou : Tailwind `tailwind.config.ts` scope `content: ['./src/components/**']` pour ne pas inonder les composants RA de classes utilitaires.

### 2.8 i18n

RA support natif `ra-i18n-polyglot` + messages JSON. Arborescence :

```
frontend/src/i18n/
├── fr.ts                         # default, exporte un objet messages
├── en.ts                         # fallback
└── index.ts                      # i18nProvider avec polyglot
```

`locale: 'fr'` au chargement, bouton de bascule dans l'AppBar. Tous les labels d'entité, champs, messages d'erreur humanisés passent par les catalogues.

---

## §3 — Contrats backend à ajouter

### 3.1 Principe et ADR express "ingest vs. edit"

**Les endpoints `POST /v1/ingest/*` existent (cf. `src/ootils_core/api/routers/ingest.py:1-37`) mais ne sont pas exploitables pour une UI de curation.** Leur contrat :
- All-or-nothing sur un batch (rollback si une row fail), cf. `ingest.py:80-82`.
- Déclenchement obligatoire du pipeline DQ L1+L2 (`_trigger_dq()`).
- Traçage `ingest_batches` + `ingest_rows` (audit lourd, 1 row = 1 entrée).
- 10 MB hard cap, optimisé pour des batches de milliers de rows.

Un humain qui clique "enregistrer" sur un écran d'article n'a pas besoin de créer un `ingest_batch`. Il lui faut un `PUT /v1/items/{external_id}` simple, idempotent sur la clé métier, qui renvoie la row mise à jour. Mélanger les deux contrats crée de la dette (batches fantômes à 1 row, pollution du DQ dashboard avec des "issues" de l'UI).

**ADR express** : l'UI parle à un nouvel ensemble de routes CRUD (`GET/PUT/DELETE /v1/<entity>/...`) qui écrivent sur les mêmes tables que l'ingest, sans passer par `ingest_batches`. Les routes existantes `/v1/ingest/*` restent inchangées et dédiées aux connecteurs ERP/scripts batch.

### 3.2 Règles transverses pour toutes les nouvelles routes

| Aspect | Décision |
|--------|----------|
| Auth | Même `require_auth` existant (`auth.py:32-59`) — Bearer `OOTILS_API_TOKEN` |
| Error envelope | SPEC-INTERFACES §6.1 : `{error: {code, message, status, correlation_id, details, docs_url}}` |
| Pagination | SPEC-INTERFACES §6.5 — cursor-based : `?cursor=<b64>&limit=<n>` ; réponse contient `page: {next_cursor, has_more, limit}` |
| Idempotency | SPEC-INTERFACES §6.2 — header `Idempotency-Key` sur PUT et POST, TTL 1 h pour les edits UI (à distinguer des 72 h ingest) |
| Soft-delete | DELETE ne hard-delete pas ; passe `status='obsolete'` (items) / `status='inactive'` (suppliers) / `valid_to=now()` (supplier-items) / nouvelle version SCD2 avec `effective_to=now()` (planning-params) |
| `?include_archived=true` | Par défaut, GET exclut les rows soft-deleted. `?include_archived=true` les inclut |
| Content-Type | `application/json` only |
| Versioning | `/v1` — additive, pas de breaking existant |

### 3.3 Items `[PROPOSED]`

| Méthode | URL | Purpose |
|--------|-----|---------|
| GET | `/v1/items` | Liste paginée |
| GET | `/v1/items/{external_id}` | Détail |
| PUT | `/v1/items/{external_id}` | Upsert single (create si absent, update si présent) |
| DELETE | `/v1/items/{external_id}` | Soft-delete (`status='obsolete'`) |

**GET `/v1/items`** — query params : `cursor`, `limit` (défaut 50, max 200), `q` (recherche sur `external_id` ou `name` ILIKE), `status` (`active` | `obsolete` | `phase_out`), `item_type`, `include_archived` (défaut `false` = filtre `status != 'obsolete'`).

Exemple de réponse :

```json
{
  "items": [
    {
      "external_id": "SKU-0042",
      "name": "Boîtier moteur MX-7",
      "item_type": "finished_good",
      "uom": "EA",
      "status": "active",
      "created_at": "2026-01-12T14:20:05Z",
      "updated_at": "2026-04-11T09:03:22Z"
    }
  ],
  "page": { "next_cursor": "eyJjcmVhdGVkX2F0IjoiMjAyNi0wMS0xMiIsImV4dGVybmFsX2lkIjoiU0tVLTAwNDIifQ==", "has_more": true, "limit": 50 }
}
```

**PUT `/v1/items/SKU-0042`** — body :

```json
{
  "name": "Boîtier moteur MX-7 rev.B",
  "item_type": "finished_good",
  "uom": "EA",
  "status": "active"
}
```

Retour `200` :

```json
{
  "external_id": "SKU-0042",
  "name": "Boîtier moteur MX-7 rev.B",
  "item_type": "finished_good",
  "uom": "EA",
  "status": "active",
  "created_at": "2026-01-12T14:20:05Z",
  "updated_at": "2026-04-18T11:02:17Z"
}
```

**DELETE `/v1/items/SKU-0042`** — `204 No Content`. Effet : `UPDATE items SET status='obsolete', updated_at=now() WHERE external_id = 'SKU-0042'`. Pre-check : si l'item est référencé par au moins un `nodes.item_id` actif ou un `supplier_items.item_id` sans `valid_to`, retour `409 conflict.has_dependencies` avec `details: {nodes_count, supplier_items_count}`. L'UI propose alors un flow "archive en cascade" (hors scope v1, `501 not_implemented`).

### 3.4 Suppliers `[PROPOSED]`

Mêmes endpoints, mêmes règles. Tables : `suppliers` (migration 007:154-166).

| Méthode | URL | Notes |
|--------|-----|-------|
| GET | `/v1/suppliers` | filtres : `q`, `status` (`active` / `inactive` / `blocked`), `country`, `include_archived` |
| GET | `/v1/suppliers/{external_id}` | — |
| PUT | `/v1/suppliers/{external_id}` | upsert |
| DELETE | `/v1/suppliers/{external_id}` | soft-delete via `status='inactive'` |

Soft-delete refusé si ≥1 `supplier_items` actif référence le fournisseur ; retour `409 conflict.has_dependencies`.

### 3.5 Supplier-items `[PROPOSED]` — composite key

La clé métier est `(supplier_external_id, item_external_id)`. Deux URL patterns envisagés :

| Option | URL | Verdict |
|--------|-----|---------|
| A | `/v1/supplier-items/{supplier_ext}/{item_ext}` | **Choisi** — REST-compatible, URL-safe tant que les `external_id` sont ASCII (garanti par DQ L1) |
| B | `/v1/supplier-items?supplier=...&item=...` sur `GET/PUT/DELETE` | Rejeté — `PUT /v1/supplier-items?supplier=...&item=...` est inhabituel et casse les proxies qui cachent sur la query |
| C | `/v1/suppliers/{supplier_ext}/items/{item_ext}` | Rejeté — implique une hiérarchie, mais supplier_items est autonome (peut exister côté item sans supplier list view) |

**Endpoints** :

| Méthode | URL |
|--------|-----|
| GET | `/v1/supplier-items` (filtres : `supplier_external_id`, `item_external_id`, `is_preferred`, `include_archived`) |
| GET | `/v1/supplier-items/{supplier_ext}/{item_ext}` |
| PUT | `/v1/supplier-items/{supplier_ext}/{item_ext}` |
| DELETE | `/v1/supplier-items/{supplier_ext}/{item_ext}` (soft-delete = `UPDATE supplier_items SET valid_to = CURRENT_DATE WHERE …`) |

Body PUT :

```json
{
  "lead_time_days": 21,
  "moq": 500,
  "unit_cost": 12.75,
  "currency": "EUR",
  "is_preferred": true,
  "valid_from": "2026-04-18",
  "valid_to": null
}
```

### 3.6 Planning params `[PROPOSED]` — composite key SCD2

Les `item_planning_params` sont **versionnés SCD2** (migration 007:243-260 : `effective_from` / `effective_to` + contrainte `EXCLUDE USING gist` qui empêche les chevauchements). L'édition n'est jamais un UPDATE : c'est un *close-old-then-insert-new*.

**URL retenue** : `/v1/planning-params/{item_ext}/{location_ext}` (même logique que supplier-items, clé composite dans le path).

*Considéré et rejeté* : `/v1/items/{ext}/planning-params/{location_ext}` — implique une hiérarchie qui n'existe pas en DB (la table est stand-alone).

| Méthode | URL | Sémantique |
|--------|-----|-----------|
| GET | `/v1/planning-params` | Liste (filtres : `item_external_id`, `location_external_id`, `as_of_date` défaut today, `include_history` défaut `false`) |
| GET | `/v1/planning-params/{item_ext}/{location_ext}` | Version active à `as_of_date` (défaut today) |
| PUT | `/v1/planning-params/{item_ext}/{location_ext}` | *Close + insert* : `UPDATE … SET effective_to = CURRENT_DATE WHERE effective_to IS NULL` puis `INSERT` nouvelle row avec `effective_from = CURRENT_DATE` |
| DELETE | `/v1/planning-params/{item_ext}/{location_ext}` | Close actif sans insert (`UPDATE … SET effective_to = CURRENT_DATE`) |

La route existante `GET /v1/items/planning-params` (`routers/planning_params.py:38-100`) reste — elle filtre sur UUID, sert les agents qui ont déjà les UUIDs graphe. La nouvelle route filtre sur `external_id`, sert l'UI.

Body PUT :

```json
{
  "lead_time_sourcing_days": 14,
  "lead_time_manufacturing_days": 0,
  "lead_time_transit_days": 7,
  "safety_stock_qty": 120,
  "safety_stock_days": null,
  "reorder_point_qty": 80,
  "min_order_qty": 50,
  "max_order_qty": 2000,
  "order_multiple": 10,
  "lot_size_rule": "FIXED_QTY",
  "planning_horizon_days": 120,
  "is_make": false,
  "preferred_supplier_external_id": "SUP-ACME",
  "source": "manual"
}
```

Réponse `200` : la nouvelle row, plus le `effective_to` appliqué à la précédente. L'UI affiche un timeline SCD2 en vue détail (read-only).

### 3.7 DQ Issues `[PROPOSED]`

La route `GET /v1/dq/issues` **existe** (`routers/dq.py:125-195`) — liste paginée offset. À compléter :

| Méthode | URL | État |
|--------|-----|------|
| GET | `/v1/dq/issues` | **Existe** — à migrer vers cursor pagination (SPEC-INTERFACES §6.5) et enrichir réponse (cf. §4.5 ci-dessous) |
| GET | `/v1/dq/issues/{issue_id}` | `[PROPOSED]` — détail + contexte de la row offensante |
| PATCH | `/v1/dq/issues/{issue_id}` | `[PROPOSED]` — transitions `resolved`, `ignored`, `assigned` |
| POST | `/v1/dq/issues/bulk-patch` | `[PROPOSED]` — bulk state transition (max 100 issue_ids) |

**PATCH body** :

```json
{
  "action": "resolve",
  "note": "Lead time corrigé manuellement après contact fournisseur"
}
```

Actions autorisées : `resolve`, `ignore`, `assign` (avec `assignee: <string_libre>` en v1, aucune liste d'utilisateurs — JWT v2). Transitions interdites retournent `422 validation.invalid_state_transition`.

**GET `/v1/dq/issues/{issue_id}`** enrichi — réponse :

```json
{
  "issue_id": "b3e6c4d1-…",
  "batch_id": "a1b2c3d4-…",
  "rule_code": "SUPPLIER_LT_MISSING",
  "severity": "error",
  "dq_level": 2,
  "message": "supplier SUP-ACME has no lead_time_days set",
  "field_name": "lead_time_days",
  "raw_value": null,
  "created_at": "2026-04-18T09:12:00Z",
  "resolved": false,
  "state": "open",
  "assignee": null,
  "entity_type": "suppliers",
  "entity_external_id": "SUP-ACME",
  "suggested_fix": {
    "action": "navigate",
    "screen": "suppliers",
    "target_external_id": "SUP-ACME",
    "field_to_edit": "lead_time_days"
  }
}
```

Le champ `suggested_fix` est une **nouvelle sémantique** `[PROPOSED]` : l'API suggère à l'UI où le curateur doit aller pour résoudre l'issue. Le mapping `rule_code → (screen, field)` est une table Python (pas de migration DB) dans `src/ootils_core/engine/dq/suggested_fix_map.py`.

### 3.8 Codes d'erreur dédiés UI (extension de SPEC-INTERFACES §6.1)

| Code | HTTP | Contexte |
|------|------|----------|
| `not_found.item` / `not_found.supplier` / `not_found.supplier_item` / `not_found.planning_params` / `not_found.dq_issue` | 404 | GET/PUT/DELETE sur clé inexistante |
| `conflict.has_dependencies` | 409 | soft-delete refusé parce que dépendants actifs existent |
| `conflict.concurrent_edit` | 409 | optimistic lock — si `If-Unmodified-Since` ou `If-Match` échoue (cf. §3.9) |
| `validation.invalid_state_transition` | 422 | PATCH DQ action incompatible avec l'état courant |
| `validation.schema` | 422 | body malformé |

### 3.9 Optimistic locking `[PROPOSED]` (essentiel pour une UI multi-onglets même mono-user)

PUT et DELETE acceptent un header `If-Match: "<updated_at ISO8601>"`. Si le `updated_at` courant en DB diffère, retour `409 conflict.concurrent_edit` avec `details: {server_updated_at: "…"}`. Sans header, la route accepte l'écriture (mode compatible pour scripts). L'UI RA l'injecte automatiquement via un hook `useOptimisticLock`.

---

## §4 — Parcours utilisateur (les 5 écrans)

Convention : toutes les routes RA montées sous `/app/#/<resource>` (hash routing, même origin).

### 4.1 Écran **Articles** (`/app/#/items`)

**Purpose** : permettre à un data steward de corriger un nom d'article, son UOM, son statut d'obsolescence, sans ouvrir un ticket IT.

**List view — colonnes** : `external_id`, `name`, `item_type`, `uom`, `status` (badge coloré : active/obsolete/phase_out), `updated_at` (relative).

**Filtres** : recherche `q` (substring sur `external_id` OR `name`), dropdown `status`, dropdown `item_type`, toggle "Include archived".

**Edit form — champs** : `name` (text, required, max 200), `item_type` (select enum), `uom` (text, 1-10 chars, upper), `status` (select).

**Actions** : Create, Edit, Soft-delete, Export CSV (bouton RA natif sur la liste, max 500 rows), Bulk edit désactivé v1 (cf. cas limite ci-dessous).

**Validations UI** :
- client-side : `external_id` non modifiable après création, `name` requis, `uom` length ≤ 10.
- server-side : unicité `external_id` (contrainte DB), valeurs enum.

**États d'erreur** :
- 4xx avec enveloppe → bandeau rouge en tête du form avec `error.message` + `details`.
- 5xx → toast rouge "Erreur serveur, correlation ID {id}" + bouton "Signaler".
- 409 concurrent_edit → modale "Un autre onglet a modifié cette row à {server_updated_at}. Recharger pour voir, puis re-appliquer vos changements ?".

**Scénario** : *Jean Dupont, planificateur, reçoit un mail automatisé "15 articles marqués obsolete en septembre 2025 sont encore actifs dans l'ERP". Il ouvre `/app/#/items`, filtre `status=active` + `q=2025`, trie par `updated_at` ascendant, sélectionne 3 articles un par un (bulk edit désactivé), change `status=obsolete` et sauve. Chaque écriture met 150 ms, le toast de confirmation inclut le `correlation_id` pour le log.*

**Cas limite** : Bulk edit désactivé en v1. Motif : l'écriture serveur se fait row-par-row via `PUT /v1/items/{external_id}` (pas de `PUT /v1/items` bulk en v1). Un bulk-edit de 1000 rows = 1000 requêtes = UX dégradée. Si le besoin survient → roadmap v2 avec endpoint bulk dédié.

### 4.2 Écran **Fournisseurs** (`/app/#/suppliers`)

**Purpose** : corriger `lead_time_days`, `reliability_score`, bloquer/débloquer un fournisseur.

**List columns** : `external_id`, `name`, `country`, `lead_time_days`, `reliability_score` (pourcentage), `status`, `updated_at`.

**Filtres** : `q`, `status`, `country`, "Include archived".

**Edit champs** : `name`, `country` (ISO-3166 2-letter, validation regex `/^[A-Z]{2}$/`), `lead_time_days` (integer > 0), `reliability_score` (numeric 0-1, slider avec 3 décimales), `status`.

**Actions** : Create, Edit, Soft-delete (avec pre-check dépendances côté serveur), Export CSV.

**Validations UI** : tout obligatoire sauf `country`, `reliability_score`. `lead_time_days` dans [1, 365] (au-delà : warning, pas erreur).

**Scénario** : *Marie, data steward, voit dans l'Inbox DQ un issue `SUPPLIER_LT_MISSING` sur `SUP-ACME`. Elle clique sur le lien "Fix in Suppliers" de l'issue, arrive sur `/app/#/suppliers/SUP-ACME`, saisit `lead_time_days=14`, enregistre, retourne à l'Inbox, l'issue est encore listée mais l'API `GET /v1/dq/issues/{id}` signale `state: open` inchangé — elle clique "Resolve" manuellement pour fermer.*

(Optionnel v2 : résolution automatique DQ quand le champ offensant change en DB.)

**Cas limite** : bloquer un fournisseur référencé par 150 supplier-items actifs → `409` — l'UI affiche "Impossible d'archiver : 150 conditions actives". Lien "Voir les conditions" vers `/app/#/supplier-items?supplier=SUP-ACME`.

### 4.3 Écran **Conditions fournisseurs** (`/app/#/supplier-items`)

**Purpose** : éditer la matrice (fournisseur, article) — lead time spécifique, MOQ, coût unitaire, is_preferred.

**List columns** : `supplier_external_id`, `item_external_id`, `lead_time_days`, `moq`, `unit_cost`, `currency`, `is_preferred` (badge), `valid_from`, `valid_to`.

**Filtres** : `supplier_external_id` (autocomplete sur `/v1/suppliers?q=…`), `item_external_id` (autocomplete), toggle `is_preferred_only`, "Include archived" (inclut les rows avec `valid_to` passé).

**Edit champs** : `lead_time_days` (int > 0), `moq` (numeric > 0), `unit_cost` (numeric), `currency` (ISO-4217 3-letter), `is_preferred` (checkbox), `valid_from` (date), `valid_to` (date nullable, ≥ `valid_from`).

**Validations UI** : contrainte unicité `(supplier, item)` pour `is_preferred=true` — l'UI refuse de cocher `is_preferred` si un autre fournisseur l'est déjà pour cet item (lookup préalable `/v1/supplier-items?item_external_id=…&is_preferred=true`).

**Scénario** : *Une alerte DQ `SUPPLIER_NO_PREFERRED_FOR_ITEM` signale que `SKU-0042` n'a aucun fournisseur préférentiel. Le curateur ouvre l'Inbox, clique sur l'issue, arrive en `/app/#/supplier-items?item_external_id=SKU-0042`, identifie les 3 candidats, édite le plus pertinent avec `is_preferred=true`, sauve, résout l'issue.*

**Cas limite** : édition en masse des `lead_time_days` pour un fournisseur — non supporté v1.

### 4.4 Écran **Paramètres de planification** (`/app/#/planning-params`)

**Purpose** : éditer les policies MRP (lead times détaillés, safety stock, lot size rule) par (article, location).

**List columns** : `item_external_id`, `location_external_id`, `lead_time_total_days` (calculé), `safety_stock_qty`, `reorder_point_qty`, `lot_size_rule`, `effective_from`.

**Filtres** : `item_external_id`, `location_external_id`, `lot_size_rule`, toggle "Show history" (affiche les rows avec `effective_to IS NOT NULL`).

**Edit form** : champs détaillés (§3.6). Important : **l'UI affiche un avertissement clair** "Cette modification crée une nouvelle version à compter d'aujourd'hui. L'historique est conservé." La validation UI interdit `effective_from` dans le passé.

**Vue détail** : timeline SCD2 avec les versions passées (`effective_from` → `effective_to`), chacune en read-only, la dernière éditable.

**Scénario** : *Le service achat a négocié une réduction du lead time sourcing pour `(SKU-0042, DC-PARIS)` de 21 à 14 jours. Le planificateur ouvre `/app/#/planning-params/SKU-0042/DC-PARIS`, modifie `lead_time_sourcing_days`, valide — l'UI affiche "Nouvelle version v3 créée, valide à partir du 2026-04-18". La version v2 précédente passe en `effective_to=2026-04-18`.*

**Cas limite** : contrainte DB `EXCLUDE USING gist` (migration 007:255-259) sur `daterange(effective_from, COALESCE(effective_to, '9999-12-31'))` — si un chevauchement est tenté (via un script tiers), la DB rejette. L'API retourne `422 validation.temporal_overlap` et l'UI affiche "Conflit temporel — une autre version couvre déjà cette période."

### 4.5 Écran **Inbox DQ** (`/app/#/dq-issues`)

**L'écran le plus important de la v1.** Pas un CRUD RA natif — une queue custom layoutée en split-pane.

**Layout** :

```
┌──────────────────────────────────────────────────────────────┐
│  Inbox DQ — 47 open, 12 assigned, 134 resolved (24h)         │
├─────────────────────────────────┬────────────────────────────┤
│  Filters                        │  Issue detail panel        │
│  ├ State: ☒open ☐assigned ☐…   │                            │
│  ├ Severity: ☒error ☒warning   │  SUPPLIER_LT_MISSING       │
│  ├ Rule code: [dropdown]        │  SUP-ACME — Acme Corp      │
│  ├ Entity type: [dropdown]      │  Field: lead_time_days     │
│  └ Created: last 24h ▼          │  Raw value: —              │
│                                 │                            │
│  Queue (47)                     │  Message: supplier SUP-ACME│
│  ┌───────────────────────────┐  │  has no lead_time_days set │
│  │ ☐ ERROR SUP-ACME  LT miss│  │                            │
│  │ ☐ WARN  SKU-0077 MOQ neg │  │  Suggested fix:            │
│  │ ☐ ERROR SUP-BETA country│  │  → Open SUP-ACME in        │
│  │ ☐ …                      │  │    Suppliers [Fix]         │
│  └───────────────────────────┘  │                            │
│                                 │  [Resolve] [Ignore] [Assign│
│  Bulk actions:                  │                            │
│  [Resolve selected]             │  Notes: [textarea]         │
│  [Ignore selected]              │                            │
└─────────────────────────────────┴────────────────────────────┘
```

**Filtres** : `state` (open/assigned/resolved/ignored, multi-select), `severity`, `rule_code`, `entity_type`, `created_at` range, `assignee` (input libre v1).

**Bulk actions** : "Resolve selected", "Ignore selected", "Assign to…" — sur sélection ≤ 100 rows, appelle `POST /v1/dq/issues/bulk-patch`.

**Panel de contexte** : pour chaque issue sélectionnée, affiche (a) les champs DQ bruts, (b) le `suggested_fix` — un bouton qui route l'utilisateur vers l'écran de curation adéquat avec filtre pré-rempli.

**Scénario** : *Lundi 8h30, Marie reçoit une notif Slack (via webhook outbound ingest.batch.completed future) "142 issues DQ ouvertes après l'ingest nocturne". Elle ouvre `/app/#/dq-issues`, filtre `severity=error`, trie par `rule_code`. Elle voit 12 issues `SUPPLIER_LT_MISSING`, les sélectionne toutes, clique sur la première pour aller la corriger : le suggested_fix l'envoie sur `/app/#/suppliers/SUP-ACME`, elle édite, sauve, Cmd-clique sur le bouton retour, corrige la suivante. Au bout de 12 minutes, toutes les 12 corrections sont faites ; elle retourne à l'Inbox, re-sélectionne les 12 issues (encore en `state=open`), clique "Resolve selected" avec la note "LT corrigé après revue achats T2". 134 issues encore ouvertes, mais ce sont des warnings — elle les traitera demain.*

**Validations UI** : l'action "Resolve" exige au moins 3 caractères de note si `severity=error` ; sinon bouton grisé. Prévient les résolutions en masse sans traçabilité.

**Cas limite** : si `POST /v1/dq/issues/bulk-patch` retourne 207 partiel (quelques issues échouent en transition), l'UI affiche le détail par issue et laisse celles en échec sélectionnées.

---

## §5 — Auth v1 et sécurité frontend

### 5.1 Stockage du token côté browser : **`sessionStorage`**

| Option | Persistance | Risque XSS | Rechargement | Verdict |
|--------|-------------|-----------|--------------|---------|
| Mémoire JS seule | perte au reload | ⚠ encore lisible | re-saisie | Rejeté — UX trop dure |
| `localStorage` | cross-session | ❌ vol XSS durable | OK | Rejeté — token long-lived en clair = risque majeur |
| **`sessionStorage`** | fermeture onglet | ⚠ lisible par scripts mais éphémère | OK pendant session | **Choisi** |
| HttpOnly cookie | backend | ✅ non-JS | OK | Rejeté — exige endpoint login/logout côté API, SameSite + CSRF à gérer, et on a décidé "pas de user session v1" |

Motif : le token est **shared** (un seul `OOTILS_API_TOKEN`), il vaut un superuser — on l'évacue du browser dès fermeture de l'onglet. Pas de "remember me" : si on veut "remember me", on active LocalStorage en feature flag explicite désactivée par défaut (non livré v1).

### 5.2 Login screen minimal

**Décision** : écran de login très simple à l'ouverture de `/app` :

- un champ password (masqué) pour le token ;
- pas de "remember me" ;
- bouton "Connect".

Au submit, l'UI appelle `GET /health` avec le token en header. Si `200` → stocke en `sessionStorage` → redirige vers `#/items`. Si `401` → affiche "Token invalide".

*Considéré et rejeté* : injection du token via variable Vite au build (`VITE_OOTILS_TOKEN`). Rejeté parce que (a) le token finirait dans le bundle JS public — lisible par n'importe qui ayant la page ; (b) un rebuild complet serait nécessaire à chaque rotation du token. Un champ d'input est plus sûr même en mono-user.

### 5.3 CORS

Même origin en prod (cf. §2.3), mais en dev Vite tourne en `:5173` et FastAPI en `:8000`. Il faut un middleware CORS côté FastAPI `[PROPOSED]` :

```python
# [PROPOSED] — src/ootils_core/api/app.py
from fastapi.middleware.cors import CORSMiddleware
_CORS_ORIGINS = os.environ.get("OOTILS_CORS_ORIGINS", "").split(",")
if _CORS_ORIGINS and _CORS_ORIGINS[0]:
    application.add_middleware(
        CORSMiddleware,
        allow_origins=_CORS_ORIGINS,
        allow_credentials=False,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "Idempotency-Key", "If-Match", "X-Correlation-ID"],
        expose_headers=["X-Correlation-ID"],
    )
```

Valeurs :
- **local dev** : `OOTILS_CORS_ORIGINS=http://localhost:5173,http://127.0.0.1:5173`
- **prod** : `OOTILS_CORS_ORIGINS=` (vide — same-origin, CORS désactivé par l'absence du middleware)

### 5.4 CSP

Header minimal à ajouter dans la réponse de `/app/*` `[PROPOSED]` :

```
Content-Security-Policy:
  default-src 'self';
  script-src 'self';
  style-src 'self' 'unsafe-inline';        /* MUI inline styles */
  img-src 'self' data:;
  connect-src 'self';
  font-src 'self' data:;
  frame-ancestors 'none';
  base-uri 'self';
```

`style-src 'unsafe-inline'` est requis par MUI (émet du CSS-in-JS inline). C'est un trade-off connu de l'écosystème ; `unsafe-eval` et `unsafe-inline` sur `script-src` sont interdits.

### 5.5 Rotation du token

Scénario : un admin rotate `OOTILS_API_TOKEN` sur le serveur (redéploie l'API). Le front en `sessionStorage` détient encore l'ancien → toute requête retourne `401`.

**Décision UX** :
- Le `dataProvider` détecte les `401` ; il (a) purge le `sessionStorage`, (b) affiche un modal "Session expirée — veuillez vous reconnecter", (c) redirige vers `/app/#/login`.
- Pas de retry silencieux avec l'ancien token. Pas de refresh token (pas de JWT v1).

### 5.6 Audit "qui a changé quoi" : **non livré v1**

Explicite. `ingest_batches.submitted_by` reste vide pour les nouvelles routes UI (il n'y a pas d'utilisateur à y mettre). En v2, quand les JWT entreront, on peuplera `<new_table> user_actions(action_id, user_id, resource, resource_id, diff, ts)`. À l'étape pilote, un `git log`-style de corrections master-data n'est pas requis.

---

## §6 — Tests Playwright (100% automatisés)

### 6.1 Philosophie

Tests E2E sur les **parcours utilisateur**, pas sur les composants React isolés. Motif : (a) RA évolue vite, les tests de composants cassent à chaque upgrade ; (b) la valeur métier est dans le parcours, pas dans le rendu d'un `<Datagrid>` ; (c) c'est le pendant v1 de la promesse "tests utilisateurs 100% auto" qui n'était pas réalisable sur la surface de décision (agents, pas d'UI).

Unit tests React (Vitest + React Testing Library) = bonus, non obligatoires pour passer CI v1.

### 6.2 Les 20 journeys baseline (5 écrans × 4 journeys)

| # | Écran | Journey |
|---|-------|---------|
| 1 | Items | `create` — créer `SKU-TEST-0001`, vérifier apparition en list |
| 2 | Items | `edit` — modifier `name`, vérifier `updated_at` change |
| 3 | Items | `soft-delete` — archiver, vérifier disparition (sans include_archived), réapparition (avec) |
| 4 | Items | `search-filter` — seeder 3 items, filtrer par `status=active` + `q=TEST`, vérifier 1 résultat |
| 5 | Suppliers | `create` — créer `SUP-TEST-ACME` |
| 6 | Suppliers | `edit-lead-time` — modifier `lead_time_days` de 7 à 14 |
| 7 | Suppliers | `soft-delete-with-deps` — tenter archive avec supplier_items actifs → vérifier le `409` affiché |
| 8 | Suppliers | `filter-by-country` — filtrer `country=FR` |
| 9 | Supplier-items | `create` — créer relation `(SUP-TEST, SKU-TEST)` |
| 10 | Supplier-items | `toggle-preferred` — cocher `is_preferred`, vérifier unicité |
| 11 | Supplier-items | `close-with-valid-to` — mettre `valid_to=today()`, vérifier disparition |
| 12 | Supplier-items | `filter-by-supplier` — autocomplete supplier → résultats |
| 13 | Planning-params | `create-first-version` — créer la 1re version pour `(SKU-TEST, DC-TEST)` |
| 14 | Planning-params | `edit-creates-scd2-version` — modifier, vérifier timeline 2 versions |
| 15 | Planning-params | `overlap-rejection` — tenter edit avec `effective_from` dans passé chevauchant → vérifier `422` |
| 16 | Planning-params | `history-toggle` — activer "Show history", vérifier lignes closed apparaissent |
| 17 | DQ Inbox | `view-open-queue` — 5 issues seedées, vérifier comptage header |
| 18 | DQ Inbox | `resolve-one` — sélectionner une issue, cliquer Resolve, vérifier passage en `resolved` |
| 19 | DQ Inbox | `bulk-resolve` — sélectionner 3 issues, Resolve selected avec note, vérifier 3 en `resolved` |
| 20 | DQ Inbox | `suggested-fix-navigation` — cliquer suggested_fix, vérifier route vers supplier avec filtre |

### 6.3 Fixture engine : réutilisation stricte

`frontend/tests/e2e/` ne réimplémente pas de seeder. Il **appelle la shared fixture engine** (`SPEC-VALIDATION-HARNESS §5.1`, `src/ootils_core/fixtures/` `[PROPOSED]`) via HTTP : chaque test `beforeAll` / `beforeEach` déclenche un POST Python qui applique un fixture YAML à la DB. Playwright sait faire :

```ts
// frontend/tests/e2e/fixtures.ts — [PROPOSED]
import { APIRequestContext } from '@playwright/test'

export async function seedFixture(
  request: APIRequestContext,
  fixturePath: string,        // e.g. 'fixtures/ui/dq_inbox_5_issues.yaml'
): Promise<{ idMap: Record<string, string> }> {
  const response = await request.post('/internal/test-fixtures/apply', {
    headers: { Authorization: `Bearer ${process.env.OOTILS_API_TOKEN}` },
    data: { fixture_path: fixturePath },
  })
  if (!response.ok()) throw new Error(`seed failed: ${await response.text()}`)
  return response.json()
}
```

L'endpoint `POST /internal/test-fixtures/apply` est `[PROPOSED]`, monté **uniquement quand `OOTILS_ENV=test`** dans `app.py` (guard explicite pour ne jamais l'exposer en prod). Il délègue à `ootils_core.fixtures.apply_to_db`.

Les fixtures YAML vivent dans `tests/fixtures/ui/` (partagées entre pytest et Playwright).

### 6.4 DB isolation : **template DB clone per test file**

Réutilisation directe de `SPEC-VALIDATION-HARNESS §2.5.1` : la template DB `ootils_scenarios_template` est créée une fois par run CI, chaque test file Playwright spawne son clone `ootils_ui_<uuid>_<file>`. `DROP DATABASE` en teardown.

*Considéré et rejeté* : **single DB truncate between tests**. Rejeté parce que (a) la contrainte `EXCLUDE USING gist` sur `item_planning_params` est fragile au TRUNCATE (les rows SCD2 rechargées doivent respecter l'ordre chronologique) ; (b) la concurrence CI tests E2E + pytest sur la même DB = flakes garantis.

Justification courte en 2 phrases : le pattern existe déjà côté pytest et se réutilise sans coût supplémentaire ; l'isolation totale est la seule qui rende les 20 journeys reproductibles en parallèle.

### 6.5 Visual regression

**Playwright `toHaveScreenshot()`** sur 5 pages-clés :
1. Items list (chargée avec 3 rows seedées)
2. Items edit (row `SKU-SCREENSHOT-001`)
3. Suppliers list (5 rows)
4. Planning-params detail (timeline avec 2 versions)
5. DQ Inbox (5 issues, panel de contexte ouvert sur la 1re)

**Threshold** : `maxDiffPixelRatio: 0.02` (2% du viewport). Stockage : `frontend/tests/e2e/__snapshots__/` committé au repo (pas en artifacts CI — trop transient). Review : une PR qui change une screenshot doit inclure le diff visible, le reviewer décide manuellement. Pas d'auto-update en CI.

### 6.6 Accessibilité

`@axe-core/playwright` sur chacun des 5 écrans au chargement initial. Target **WCAG 2.1 AA**. Assertions : `expect(await axe.analyze(page)).toHaveNoViolations()` avec exceptions documentées (MUI produit parfois des violations `color-contrast` mineures ; à auditer case-par-case, pas à exempter en bloc).

### 6.7 CI integration

Ajout à `.github/workflows/ci.yml` `[PROPOSED]` :

```yaml
# [PROPOSED] — append to .github/workflows/ci.yml
  frontend:
    name: frontend e2e (Playwright)
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16-alpine
        env:
          POSTGRES_USER: ootils
          POSTGRES_PASSWORD: ootils
          POSTGRES_DB: ootils_ui
        ports: ["5432:5432"]
        options: >-
          --health-cmd "pg_isready -U ootils -d ootils_ui"
          --health-interval 5s --health-timeout 5s --health-retries 10
    env:
      OOTILS_ENV: test
      OOTILS_DSN: postgresql://ootils:ootils@localhost:5432/ootils_ui
      OOTILS_API_TOKEN: test-token-ui
      DATABASE_URL: postgresql://ootils:ootils@localhost:5432/ootils_ui
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - uses: actions/setup-node@v4
        with: { node-version: "20" }
      - run: pip install -e ".[dev]"
      - run: npm i -g pnpm@9
      - run: cd frontend && pnpm i --frozen-lockfile
      - run: cd frontend && pnpm build
      - name: Start API
        run: nohup uvicorn ootils_core.api.app:app --host 0.0.0.0 --port 8000 &
      - name: Wait for API
        run: timeout 30 bash -c 'until curl -sf http://localhost:8000/health; do sleep 1; done'
      - name: Install Playwright browsers
        run: cd frontend && pnpm exec playwright install --with-deps chromium
      - name: Run Playwright
        run: cd frontend && pnpm exec playwright test
      - name: Upload Playwright report
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: playwright-report
          path: frontend/playwright-report/
```

**Schema contract test** : chaque journey Playwright qui appelle une route REST utilise un type TS généré depuis `docs/openapi.json` via `openapi-typescript`. Une PR backend qui casse un schéma casse aussi la compilation TS du test. Étape supplémentaire au CI front : `pnpm run codegen:openapi` vérifie que `src/types/api.ts` est à jour avec `openapi.json` (diff non vide → fail).

### 6.8 Time budget

Cible : **< 5 min** pour les 20 journeys + visual + a11y. Hypothèse : chaque journey ~12 s en moyenne (250 ms DB spawn + 2-3 s navigate/seed + 5-7 s actions UI + teardown). En parallélisant à 4 workers Playwright, on vise 3 min 30. Si une journey explose (> 30 s), on la route en `smoke-only` (une assertion basique sur le state final) et on monte la version full en tag `@slow` hors CI (nightly only).

---

## §7 — Observabilité et DX

### 7.1 Error tracking : **Sentry** à partir de la pré-prod

- **Dev** : `console.error` + UI toast.
- **CI** : pas de Sentry (bruit).
- **Staging/Prod** : Sentry init dans `main.tsx` avec `dsn: import.meta.env.VITE_SENTRY_DSN`. Capture les unhandled errors, les 5xx, les `HttpError` avec `correlation_id` en tag.

*Considéré et rejeté* : `console.error` + agrégation via logs serveur. Rejeté parce que 90% des erreurs UI sont browser-only (render glitches, hydration, oops) — ne remontent jamais au serveur sans instrumentation dédiée.

### 7.2 Correlation ID

Déjà traité §2.5 : l'adapter injecte `X-Correlation-ID: <uuid4>` sur **chaque** requête. Le backend l'honore (cf. SPEC-INTERFACES §6.4) et le renvoie dans la réponse (`expose_headers` CORS configuré §5.3). L'UI stocke le dernier correlation ID dans un context React, et l'affiche dans le toast d'erreur + dans le footer "Diagnostic" en bas d'écran.

### 7.3 Dev loop

**Choix** : `docker-compose.dev.yml` `[PROPOSED]` qui lance Postgres + API en mode `--reload` (mount `src/` en volume) + **pas de front dans Docker** — le dev lance `pnpm dev` en local qui proxy `/v1` vers `:8000`.

```yaml
# docker-compose.dev.yml — [PROPOSED]
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: ootils
      POSTGRES_PASSWORD: ootils
      POSTGRES_DB: ootils_dev
    ports: ["5432:5432"]
    volumes: ["postgres_dev_data:/var/lib/postgresql/data"]
  api:
    build:
      context: .
      dockerfile: Dockerfile.dev   # image plus légère, pip install, pas de frontend build
    volumes: ["./src:/app/src"]
    environment:
      DATABASE_URL: postgresql://ootils:ootils@postgres:5432/ootils_dev
      OOTILS_API_TOKEN: dev-token
      OOTILS_CORS_ORIGINS: http://localhost:5173,http://127.0.0.1:5173
      OOTILS_ENV: dev
    ports: ["8000:8000"]
    command: uvicorn ootils_core.api.app:app --host 0.0.0.0 --port 8000 --reload
    depends_on: [postgres]
volumes:
  postgres_dev_data:
```

Dev commands :

```bash
# terminal 1: back + DB
docker compose -f docker-compose.dev.yml up

# terminal 2: front hot-reload
cd frontend && pnpm dev
```

### 7.4 Storybook : **non** pour v1

Motif : seuls 5 écrans, majoritairement construits avec les primitives RA. Storybook = overhead (config Vite/Webpack, stories à écrire et maintenir) sans gain en v1. En v2, si on ajoute des composants custom complexes (BOM editor, timeline SCD2 riche), on réévalue.

---

## §8 — Ce que cette UI n'est PAS

Section obligatoire pour empêcher la dérive doctrine. À recopier telle quelle dans `frontend/README.md` quand il sera créé.

- **Ce n'est pas la surface de décision.** Pas de liste de shortages cliquable, pas de bouton "approuver cette reco", pas de visualisation de pegging. Tout ça vit dans les agents (MCP, API directe) et les intégrations ERP — jamais dans un écran.
- **Ce n'est pas un outil de reporting / BI.** Pas d'exports analytiques, pas de graphiques de tendance, pas de KPI dashboards. Les exports supportés (CSV sur les 4 écrans master-data) servent uniquement à la curation — ni à la gouvernance, ni à l'audit métier.
- **Ce n'est pas un simulateur.** Aucun what-if, aucun scenario branching visuel. La simulation est un outil agent (`POST /v1/simulate`, MCP `create_scenario`) — hors UI par doctrine.
- **Ce n'est pas un workspace multi-user.** Pas d'utilisateurs, pas de rôles, pas de diff "qui a changé quoi", pas de commentaires collaboratifs. Le token est partagé, et un fail du type "deux opérateurs éditent la même row" est rattrapé par l'optimistic lock (§3.9) et pas plus.
- **Ce n'est pas un client mobile.** Responsive minimal (≥ 1024 px) — sur mobile, les opérateurs curent la data depuis leur poste fixe. Pas de target iOS/Android.
- **Ce n'est pas un portail client.** Pas d'auth externe, pas de self-service onboarding, pas d'API key management par utilisateur. Le v1 est un outil interne pour le data steward / planificateur qui connaît déjà le système.

Future contributor qui arrive avec "On pourrait ajouter un widget de shortage dans le header" → pointer cette section en review.

---

## §9 — Diff précis à la doctrine

### 9.1 `CONTRIBUTING.md` lignes 61-62

```diff
@@ -58,9 +58,13 @@
 **Explicit over magic**
 Every calculation must be traceable. If you can't explain why the engine produced a result, the design is wrong.

-**API first, UI never (for now)**
-We build the engine. The interface is someone else's problem for now. Do not propose UI features in V1.
+**API-first, UI only for master-data curation**
+The engine is consumed by agents and ERPs via API — this is non-negotiable. The only legitimate
+UI surface in V1 is a thin admin console for humans who need to fix master data (items,
+suppliers, supplier-items, planning params) and triage DQ issues (cf. `docs/SPEC-STATIC-DATA-UI.md`).
+Do not propose decision-surface UI (dashboards, approval queues for recommendations, what-if
+visualizers) — those belong to agents and integrated systems.

 **Determinism is non-negotiable**
 The same inputs must always produce the same outputs. No randomness in the core engine.
```

### 9.2 `VISION.md` lignes 98-99

```diff
@@ -95,8 +95,11 @@
 ## What We Are Not Building

-**We are not building a UI product.**
-Ootils is infrastructure. Interfaces will be built on top of it.
+**We are not building a UI product.**
+Ootils is infrastructure. The decision surface (simulate, explain, recommendations) is agent-
+and API-only. A thin admin console for master-data curation (items, suppliers, supplier-items,
+planning params, DQ inbox) is in-scope as tooling, not as product — it exists so humans can keep
+the inputs clean, not so humans can take planning decisions. See `docs/SPEC-STATIC-DATA-UI.md`.

 **We are not building an AI model.**
 The planning logic is deterministic. AI is a consumer of the engine, not its replacement.
```

### 9.3 Message de commit recommandé

```
doc: clarify UI doctrine — admin console for master-data curation only

The original "UI never" rule was defensive against Kinaxis-with-AI drift. In
practice the engine accumulates DQ issues and stale master data that can't be
fixed without an IT ticket, which kills pilot adoption. Re-state the rule so it
*bans the decision surface UI* (dashboards, approval queues, what-if) while
authorising a narrow, doctrine-compliant admin console. Spec: SPEC-STATIC-DATA-UI.md.
```

---

## §10 — Roadmap 6 semaines

### 10.1 Plan hebdomadaire

| Semaine | Backend | Frontend | DevOps | Sortie de semaine |
|---------|---------|----------|--------|-------------------|
| **S1** | Cursor pagination sur 1 endpoint pilote (`/v1/items`) ; enveloppe erreur normalisée middleware ; endpoints CRUD items (GET list + get + PUT + DELETE) | Scaffolding `frontend/` avec Vite + TS + RA + Tailwind ; `dataProvider` avec adapter Bearer/erreur/correlation ; écran Items full CRUD | Ajout CORS middleware conditionnel ; `docker-compose.dev.yml` | Pilote "items CRUD end-to-end" démontrable sur `localhost` |
| **S2** | CRUD suppliers ; CRUD supplier-items (composite key URL) ; optimistic lock `If-Match` | Écrans Suppliers + Supplier-items ; autocomplete fournisseur/article via `q=` ; i18n fr/en | Dockerfile multi-stage ; mount `/app/*` | 3 écrans CRUD déployables en un container |
| **S3** | CRUD planning-params avec SCD2 ; `POST /internal/test-fixtures/apply` + `ootils_core.fixtures/` | Écran Planning-params + timeline SCD2 read-only ; 8 journeys Playwright (items, suppliers) | CI job `frontend` — Playwright sur PR | Playwright green sur items + suppliers |
| **S4** | PATCH DQ issues + bulk-patch + GET detail + `suggested_fix` map | Écran Inbox DQ complet (split-pane, bulk actions, suggested_fix routing) | Template DB per test file | 20 journeys Playwright passants |
| **S5** | Contention & 409 flows (dependencies, concurrent edits) ; codes erreur affinés ; perf audit (p95 GET list < 300 ms) | Visual regression 5 pages ; axe-core a11y ; UX polish (empty states, loading skeletons) ; Sentry staging | Déploiement staging | Staging avec pilote client simulé |
| **S6** | Bugfixes issus du staging ; durcissement CSP ; log scrub PII | Docs utilisateur (3 pages — 1 par écran critique) ; onboarding video 5 min | Prod deploy playbook ; monitoring dashboard | Pilote en prod chez le 1er client |

### 10.2 État cible S6

Un pilote client peut :
- se connecter à `/app`, saisir son token partagé ;
- lister ses ~1000 items, ~100 suppliers, ~2000 supplier-items, ~800 planning-params ;
- corriger un LT manquant sur un supplier, créer une nouvelle version SCD2 sur un planning-param, archiver un item obsolete — sans ticket IT ;
- recevoir dans l'Inbox DQ les issues émises par le pipeline DQ après chaque ingest nocturne, les résoudre en masse, les assigner ;
- naviguer depuis une issue vers l'écran de curation adéquat via `suggested_fix`.

### 10.3 Ce qui entre en v2

- **JWT + users + rôles** : table `users`, login user/password (ou SSO OIDC), scopes `items:write`, `dq:resolve`, etc. Colonne `submitted_by` / `user_id` peuplée partout.
- **Audit log** "qui a changé quoi" — table `user_actions` ; écran Audit en v2.
- **Locations CRUD**, **BOM editor** (read-only d'abord, puis éditeur), **Calendars editor**.
- **Bulk edit** sur Items / Suppliers via endpoint bulk dédié.
- **Import TSV upload** UI (drag & drop, résultat batch avec DQ inline).
- **Multi-tenant** : `tenant_id` sur toutes les tables, UI tenant switcher dans l'AppBar.

### 10.4 Critères de succès

| Métrique | Cible pilote semaine 3 | Cible pilote semaine 8 |
|---------|------------------------|------------------------|
| Corrections master data faites via l'UI (vs ticket IT ou POST curl) | > 70% | > 90% |
| MTTR d'une issue DQ `severity=error` | < 48 h médian | < 4 h médian |
| Temps moyen pour corriger un LT fournisseur | < 2 min de la notif à la résolution | < 1 min |
| Taux d'erreurs 5xx sur les 5 écrans | < 0.5% des requêtes | < 0.1% |
| Playwright E2E green rate sur main | 100% | 100% |
| p95 `GET /v1/items` list | < 300 ms | < 200 ms |
| Tickets IT "corriger master data" / semaine | ↓ 50% | ↓ 90% |

**Énoncé explicite du succès** : *"Pour être déclarée success, l'UI doit permettre au data steward pilote de passer à zéro ticket IT pour la curation master data à la fin de la semaine 3, sans jamais utiliser le terme *recommandation* ou *simulation* dans le produit."*

---

*Spec maintenue par : Architecture Ootils + équipe front-end. Prochaine révision : après la mise en prod pilote ou l'arrivée du 2e client (introduction JWT).*
