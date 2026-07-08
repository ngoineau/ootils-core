# ADR-032 — Grille de scopes, budgets par token, cycle de vie des credentials et /metrics

**Statut :** Accepté — chantier #392 **AN-2**. PR2a (enforcement des scopes bout-en-bout, [#434](https://github.com/ngoineau/ootils-core/pull/434)) mergée ; PR2b (budgets `rate_per_min` appliqués, cycle de vie des tokens par l'API, `/metrics` Prometheus) dans ce worktree. Cet ADR acte les décisions AN-2 posées en dette par ADR-029 (§« décidé mais pas encore appliqué », désormais résorbée).
**Date :** 2026-07-08
**Auteurs :** ootils-core team
**Contexte mesuré :** `docs/ADR-029-agent-enterprise-floor.md` (le substrat), `docs/ROADMAP-AGENTS-2026-H2.md` §4 (chantier AN-2), et l'implémentation `src/ootils_core/api/auth.py` / `token_service.py` / `metrics.py` / `routers/tokens.py`, qui portent ces décisions.

---

## Contexte

ADR-029 a **posé** l'étage entreprise : un registre `api_tokens` (migration 064), une identité d'acteur cryptographique (`actor_kind` dérivé du token, jamais du body), un cache de résolution borné, un kill-switch de flotte. Mais cet ADR fermait sur une section d'honnêteté — « décidé mais pas encore appliqué » — qui listait trois trous :

- **Les scopes n'étaient enforcés nulle part.** `require_auth` résolvait le principal mais ne vérifiait **aucun** scope ; une poignée de routers seulement utilisaient `require_scope`. Un token read-only pouvait déclencher `POST /v1/mrp/run` ou muter le graphe. Le `scopes TEXT[]` était un grant que rien ne consommait.
- **`api_tokens.rate_per_min` était du schéma mort.** La colonne (budget par token) existait mais aucun code ne la lisait — aucun rate-limit par token.
- **Aucun cycle de vie par l'API, aucune observabilité.** L'émission passait par un script CLI ; il n'existait ni endpoint gouverné d'émission/révocation, ni `/metrics`.

AN-2 ferme ces trois trous. Le point de doctrine central que ce chantier a dû trancher — et qui n'était pas résolu dans ADR-029 — est **quel scope exige quel endpoint**. La règle intuitive « c'est déterministe, donc ce n'est pas une décision, donc `require_auth` seul suffit » s'est révélée **fausse et dangereuse** : un run MRP est déterministe **et** coûteux **et** écrit des nœuds dérivés dans le graphe. La confondre avec une simple lecture parce qu'elle est reproductible, c'est laisser un token read-only saturer le moteur. Il fallait une doctrine explicite, orthogonale à la réversibilité (que gère déjà la Decision Ladder + le gate #341).

## Décision

### 1. La grille des 8 scopes — whitelist en code, validée à l'import

Le vocabulaire des scopes valides est une **frozenset applicative** (`VALID_SCOPES`, `src/ootils_core/api/auth.py:111`), **jamais** un CHECK SQL (ADR-029 : la base stocke le grant, l'appli décide ce qu'un grant peut contenir — élargir un scope ne doit pas exiger une migration). Les routers câblent `Depends(require_scope("..."))` à l'import ; un scope typé faux **crashe au boot** (`require_scope` lève `ValueError` à l'appel-fabrique, `auth.py:758`) plutôt que de 403-er silencieusement à la requête — fail-loudly.

| Scope | Sémantique | Exemples d'endpoints |
|---|---|---|
| `read` | Toute lecture. Aucune écriture, aucun moteur invoqué. | `GET /v1/nodes/{id}`, `GET /v1/issues`, `GET /v1/stream`, `GET /v1/outcomes/summary`, tous les `GET` Pyramide/DQ |
| `ingest` | Écriture de master-data / données opérationnelles via les pipelines d'ingest. | `POST /v1/ingest/items`, `POST /v1/ingest/locations`, `POST /v1/snapshots`, `POST /v1/outcomes/evaluate` |
| `calc:run` | **Invoquer un moteur** (calcul déterministe), y compris ses écritures graphe DÉRIVÉES. | `POST /v1/mrp/run`, `POST /v1/calc/run`, `POST /v1/pyramide/runs`, DQ engine run, CTP, ghosts, explosion BOM |
| `graph:write` | Mutation **directe** de master-data topologique. | `POST /v1/nodes/{id}/firm`, `DELETE /v1/nodes/{id}/firm` (firm/unfirm d'un FPO) |
| `scenario:write` | Créer / forker un scénario, écrire un overlay de scénario. | `POST /v1/simulate`, `POST /v1/scenarios`, `POST /v1/scenarios/{id}/param-overrides` |
| `recommend:draft` | Émettre / faire transiter une reco jusqu'aux états non-terminaux. | transition d'une reco vers `DRAFT`/`REVIEWED` |
| `recommend:approve` | Faire transiter une reco vers un état d'approbation. | transition d'une reco vers `APPROVED`/`APPLIED` |
| `admin` | **Superset** — satisfait tout `require_scope`. Émission/révocation de tokens, `/metrics`, démos. | `POST/GET/DELETE /v1/tokens`, `GET /metrics`, `POST /v1/demo/phase1/run` |

`admin` est le superset (`Principal.has_scope`, `auth.py:156`) : le token legacy y résout, donc aucun appelant pré-#392 ne régresse. Les scopes sont des **capacités orthogonales**, pas une hiérarchie : détenir `calc:run` ne confère pas `graph:write`, et inversement.

### 2. LA DOCTRINE : coût ≠ réversibilité

L'ancienne règle — « un endpoint déterministe n'est pas une décision, donc `require_auth` seul » — est **cassée et remplacée**. Elle confondait deux axes indépendants :

- **La réversibilité / le risque** est l'axe de la **Decision Ladder** (L0–L4) et du **gate humain #341**. Il gouverne *qui* (humain vs agent) peut *approuver* une action irréversible.
- **Le coût / la capacité** est l'axe des **scopes**. Il gouverne *quelle catégorie de travail* un token a le droit de déclencher.

La doctrine gravée :

- **Tout endpoint qui invoque un moteur exige `calc:run`** — MRP, propagation, Pyramide/Chronos, DQ, CTP, ghosts, explosion BOM — **y compris ses écritures graphe DÉRIVÉES du calcul**. Un run MRP qui écrit des nœuds `PlannedSupply` reste `calc:run`, pas `graph:write` : ces nœuds sont un *produit du calcul*, pas une mutation directe de master-data. Vérifié : `POST /v1/mrp/run` → `calc:run` (`routers/mrp.py:325`), Pyramide run → `calc:run` (`routers/pyramide.py:296,414`), DQ engine → `calc:run` (`routers/dq.py:92,351`).
- **`graph:write` est réservé aux mutations DIRECTES de master-data topologique** — aujourd'hui firm/unfirm d'un FPO (`POST`/`DELETE /v1/nodes/{id}/firm`, `routers/graph.py:430,446`). Ce sont des écritures que l'opérateur pose à la main sur le graphe, pas dérivées d'un moteur.
- **`ingest` est l'écriture de données, pas l'invocation de moteur** : charger des items, des locations, un snapshot, persister un verdict d'outcome.

La conséquence pratique : un token de watcher qui a besoin de *simuler* (fork + propagation) porte `scenario:write` + `calc:run`, jamais `graph:write` ; un opérateur qui *firme* une commande porte `graph:write` sans avoir le droit de lancer un MRP complet. Chaque token porte le strict nécessaire à sa mission.

### 3. Deux planchers empilés sur la gouvernance

Une écriture gouvernée (L3+, ex. `CANCEL`, approbation d'une reco) franchit **deux planchers successifs**, jamais un seul :

1. **Le plancher de scope (403)** — `require_scope` vérifie que le token détient la capacité (ex. `recommend:approve`). Résolu **avant** d'atteindre le corps de l'endpoint.
2. **Le gate humain #341 (403)** — inchangé par AN-2. Même un token qui a franchi le plancher de scope se voit refuser une transition `HUMAN_ONLY` si son `actor_kind` (lu du **token**, pas du body) n'est pas `human`.

Les deux sont indépendants et cumulatifs : un token `agent` + `recommend:approve` franchit le plancher de scope **mais** est arrêté par le gate humain (test `test_human_gate_blocks_agent_even_with_approve_scope_and_lying_body`, `tests/integration/test_agent_floor_integration.py`). Le gate #341 **ne bouge pas** — AN-2 ajoute un plancher *en amont*, il ne remplace rien.

### 4. Budgets — `rate_per_min` par token, fenêtre glissante per-worker

`_RateCounter` (`auth.py:376`) applique le budget `api_tokens.rate_per_min` : une fenêtre glissante de 60 s, un deque de timestamps monotoniques par `token_id`, purge des timestamps hors fenêtre à chaque touche, refus dès que le compte atteint la limite.

- **Per-worker, PAS global — caveat documenté (jumeau du cache TTL).** Comme `_TokenCache`, le compteur vit dans la mémoire d'**un** process. Sous N workers uvicorn, le plafond effectif d'un token est N × `rate_per_min` (chaque worker ne compte que ce qu'il a servi). C'est un arbitrage V1 délibéré : aucun store partagé sur le chemin d'auth chaud ; un limiteur global exigerait Redis/DB. Le budget est **approximatif, pas exact**, et documenté comme tel dans la docstring de `_RateCounter`.
- **Refus non consommant — backoff exact admis.** Une requête refusée n'est **pas** enregistrée (elle n'étend pas la fenêtre) : un appelant qui recule exactement `Retry-After` secondes est admis au prochain essai (`_RateCounter.check`, `auth.py:418`). Le refus renvoie **429 + `Retry-After`** en secondes entières, planché à 1 (`_enforce_rate_limit`, `auth.py:620`).
- **Ordre 401 → kill-switch → rate.** Dans `resolve_principal` : token manquant/invalide → **401** d'abord ; puis `request.state` posé (attribution d'audit) ; puis kill-switch flotte → **503** (une flotte désactivée répond sans dépenser de slot de rate) ; puis rate limit → **429** en dernier (`auth.py:711–715`). Le plancher de scope (403) suit, dans `require_scope`.
- **Legacy et NULL exemptés — opt-in à l'émission.** Le token legacy (`token_id is None`) et tout token minté à `rate_per_min = NULL` ne sont **jamais** rate-limités (fail-open : un budget absent = « illimité », jamais « bloqué » — `auth.py:632`). 🎯 Défaut pilote : **NULL = illimité** ; le pilote peut instaurer un défaut prudent à l'émission (`rate_per_min` doit être `> 0`, gardé côté `mint_token` et côté CHECK migration 064).

### 5. Cycle de vie des tokens par l'API — `POST/GET/DELETE /v1/tokens` (admin)

Le router `routers/tokens.py` remplace les INSERT à la main et le CLI comme voie principale d'émission/révocation. **Chaque verbe exige `admin`** : émettre ou tuer une credential est l'opération la plus privilégiée du système.

- **`POST /v1/tokens`** — émet une credential ; le **cleartext est montré exactement UNE fois** dans la réponse (`TokenCreateResponse.token`) et n'est jamais restituable (seul le SHA-256 vit en base). La logique de génération/hash/persist est centralisée dans `token_service.mint_token` (`token_service.py:59`), partagée avec `scripts/demo_e2e.py` — une seule place sait fabriquer un token. `actor_kind`/`scopes` invalides → **422 hand-authored** (message nommant la valeur fautive + la whitelist, jamais une chaîne psycopg — même carve-out que `param_overrides.py`/`staging.py`).
- **`GET /v1/tokens`** — liste les credentials **sans aucun matériel secret** (ni cleartext, ni hash), seulement le `prefix` non-secret.
- **`DELETE /v1/tokens/{id}`** — **soft-revoke** (`revoked_at = now()`, la ligne survit pour l'audit) + **invalidation globale du cache** de principal (`invalidate_token_cache`, `auth.py:351`, appelée par `revoke_token`, `token_service.py:118`) pour que la révocation prenne effet à la requête suivante au lieu de traîner jusqu'à 30 s derrière une entrée de cache encore positive. **Caveat multi-worker (miroir du §4)** : le cache de principal est per-process — le clear ne touche que le worker qui traite le DELETE ; sous N workers uvicorn, les N−1 autres conservent le principal jusqu'au TTL, donc **la révocation se propage en ≤ 30 s** au reste de la flotte de workers. Borné, assumé en V1 (le pilote tourne single-worker → effet immédiat en pratique) ; une invalidation cross-worker exigerait un bus partagé (Redis/pg_notify), hors périmètre. **Clear global, justifié** : le cache est indexé par le **hash du cleartext**, la révocation est adressée par `token_id` — l'appelant ne détient pas le cleartext, il ne peut donc pas calculer le hash pour évincer une entrée ciblée ; un revoke est un événement administratif rare, le cache est petit, le seul coût d'un clear est une poignée de cache-miss. **204 idempotent** : révoquer un token déjà révoqué renvoie quand même 204 ; **404** seulement si le `token_id` est inconnu (décidé par un SELECT d'existence).

### 6. `/metrics` Prometheus — admin, hors `/v1`, cardinalité bornée

L'endpoint `GET /metrics` (`app.py:399`) expose les collecteurs Prometheus.

- **Admin (🎯 défaut).** Gaté sur `require_scope("admin")` — la cible de scrape s'authentifie avec un Bearer admin ; les métriques ne sont **pas** world-readable, il n'existe aucun chemin non-authentifié (miroir du reste de l'API). Le choix « admin » est un 🎯 réglage pilote (un déploiement peut vouloir un scope de scrape dédié).
- **Hors `/v1` et hors OpenAPI.** Servi à la racine (`/metrics`, convention Prometheus de facto), **pas** sous `/v1` : c'est une surface ops, pas le contrat d'API versionné. `include_in_schema=False` → absent de `openapi.json`.
- **Cardinalité bornée par construction** (`metrics.py`). Le label `route` est le **template de route** (`/v1/tokens/{token_id}`), jamais le path brut (`/v1/tokens/9f3a...`) — un flot d'UUID distincts exploserait sinon le nombre de séries. Une requête qui n'a matché aucune route s'effondre sur le littéral `"unmatched"`. `method`/`status`/`actor_kind` sont des enums finis bornés.
- **Compteurs :** requêtes totales (`ootils_http_requests_total`, par route/méthode/statut/actor_kind), latence (`ootils_http_request_duration_seconds`), refus 429 (`ootils_rate_limited_total` par actor_kind), refus kill-switch 503 (`ootils_fleet_killswitch_total`). Instrumentation **best-effort** : un échec de métrique ne casse jamais une réponse (garde dans le middleware, `app.py:346`).

### 7. Carve-out assumé : le cycle de vie des tokens n'émet PAS d'event stream

Émettre/révoquer un token **n'écrit aucune ligne `events`** et n'apparaît donc **pas** dans `GET /v1/stream` (SSE). C'est un **carve-out délibéré** à la règle North Star « streamable » : le cycle de vie des credentials est de la **gouvernance administrative**, pas un delta de supply-chain qu'un agent devrait consommer en subscribe. Il est **audité** — chaque appel `/v1/tokens` passe par `api_request_log` (avec `token_id` + `actor_kind` dénormalisé, migration 064) comme toute requête `/v1/*`, et `mint`/`revoke` loguent au niveau INFO. La traçabilité passe par le ledger d'audit, pas par le stream d'agents.

## Portée

- **Auth / gouvernance, transverse aux scénarios.** Un token, ses scopes et son budget sont de l'infrastructure, invariante par scénario. Aucune forkabilité requise — un token n'est pas un levier de simulation (même portée qu'ADR-029).
- **PR2b n'ajoute aucune migration.** `rate_per_min` et `scopes` existent déjà (migration 064). Le budget, le cycle de vie API et `/metrics` sont **purement applicatifs** au-dessus du schéma posé par ADR-029.

## Alternatives rejetées

- **Garder `require_auth` (auth sans scope) sur les writes déterministes.** Rejeté : c'est exactement le trou d'ADR-029 §« pas appliqué ». Un token read-only pouvait saturer le moteur MRP. Le coût n'est pas la réversibilité.
- **Un CHECK SQL sur le contenu de `scopes`.** Rejeté (ADR-029) : figerait le vocabulaire dans le schéma ; chaque nouveau scope exigerait une migration. La whitelist vit en code, au même point de revue que le `require_scope` qui l'enforce.
- **Une hiérarchie de scopes (`graph:write` implique `calc:run`, etc.).** Rejeté : les scopes sont orthogonaux. Un opérateur qui firme n'a aucune raison de pouvoir lancer un MRP complet ; les coupler élargirait silencieusement la surface d'un token.
- **Un limiteur de rate global (Redis/DB).** Rejeté en V1 : exigerait un store partagé sur le chemin d'auth chaud. Le per-worker (plafond effectif N × limite) est l'arbitrage V1 documenté, jumeau du cache TTL per-worker.
- **Éviction ciblée d'une entrée de cache au revoke.** Rejeté : le cache est indexé par le hash du cleartext, que l'appelant ne détient pas (il révoque par `token_id`). Un clear global est correct, simple et bon marché pour un événement rare.
- **`/metrics` sous `/v1` et dans OpenAPI.** Rejeté : c'est une surface ops, pas le contrat versionné. La racine + `include_in_schema=False` est la convention.
- **Émettre un event stream à l'émission/révocation d'un token.** Rejeté : gouvernance admin, pas un delta supply-chain. Audité via `api_request_log` (§7).

## Conséquences

- **Positif :** la dette AN-2 d'ADR-029 est résorbée. Toute route montée exige désormais un scope (plus aucun `require_auth`) ; les budgets par token sont réels ; le cycle de vie passe par une surface gouvernée ; `/metrics` donne l'observabilité. La doctrine coût≠réversibilité est explicite et testée (matrice `test_agent_floor_integration.py` §12).
- **Négatif / dette assumée en V1 :**
  - Le rate-limit est **per-worker, approximatif** (plafond N × limite sous N workers) — pas de store partagé (§4).
  - 🎯 Le défaut `rate_per_min = NULL` (illimité) laisse un token minté sans budget tant que le pilote n'en fixe pas un à l'émission.
  - Le token legacy `admin`-superset reste un contournement de la grille tant qu'il n'est pas retiré (dette héritée d'ADR-029).
  - Le cycle de vie des tokens n'émet pas d'event stream (carve-out assumé, §7).
- **Reste à faire (hors AN-2) :** retrait du token legacy ; défaut prudent de `rate_per_min` au niveau du pilote ; éventuel limiteur global si le déploiement multi-worker l'exige.

## Références

- **#392** — chantier « étage entreprise agents » ; **AN-2** = scopes bout-en-bout + budgets + cycle de vie + `/metrics`.
- **PR [#434](https://github.com/ngoineau/ootils-core/pull/434)** — AN-2 PR2a : enforcement des scopes sur toutes les routes montées (plus aucun `require_auth` sur une route).
- `docs/ADR-029-agent-enterprise-floor.md` — le substrat (registre `api_tokens`, `actor_kind` cryptographique, cache borné, kill-switch) ; sa section « décidé mais pas appliqué » que cet ADR résorbe.
- `docs/ROADMAP-AGENTS-2026-H2.md` §4 (AN-2) — livrables et critères d'acceptation du chantier.
- `src/ootils_core/db/migrations/064_api_tokens_and_scopes.sql` — `api_tokens.scopes TEXT[]` (no CHECK, whitelist en code) + `rate_per_min` + binding audit `api_request_log`.
- `src/ootils_core/api/auth.py` — `VALID_SCOPES` (grille des 8), `require_scope` (validation à l'import), `_RateCounter` + `_enforce_rate_limit` (429 + Retry-After, ordre 401→kill-switch→rate), `invalidate_token_cache` (clear global au revoke), `_ADMIN_SCOPE` superset.
- `src/ootils_core/api/token_service.py` — `mint_token` (cleartext montré une fois, 256 bits, validation whitelist), `revoke_token` (soft-revoke + invalidation cache).
- `src/ootils_core/api/routers/tokens.py` — `POST/GET/DELETE /v1/tokens` (admin), 204 idempotent, 404 inconnu, 422 hand-authored.
- `src/ootils_core/api/metrics.py` — collecteurs Prometheus, `route_template` à cardinalité bornée.
- `src/ootils_core/api/app.py` — `GET /metrics` (admin, `include_in_schema=False`), instrumentation best-effort dans le middleware.
- `tests/integration/test_agent_floor_integration.py` §12 — la matrice d'enforcement (un write représentatif par famille de scope, les deux planchers).
- `tests/test_rate_counter_and_scopes.py` — tests unitaires purs du `_RateCounter` et de la validation `require_scope`.
