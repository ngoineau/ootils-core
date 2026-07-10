# ADR-036 — Une fenêtre humaine minimale au-dessus de l'API (EXP-1)

**Statut :** Accepté — chantier **EXP-1** (`docs/ROADMAP-AGENTS-2026-H2.md` §EXP-1, « P0, 🎯 ARBITRAGE PILOTE REQUIS »). PR1 (lecture seule) implémentée dans ce worktree (`feat/exp1-human-window`), non encore mergée sur `main`. PR2 (boutons d'action approve/reject) est hors périmètre de ce PR.
**Date :** 2026-07-10
**Auteurs :** ootils-core team
**Contexte mesuré :** `docs/ROADMAP-AGENTS-2026-H2.md` §EXP-1 (« zéro UI (aucun .html/.jsx dans src/), l'approbation L3 = CLI + POST JSON ; zéro alerting sortant. "La Decision Ladder exige un visage" — et la démo 8 semaines vs Kinaxis est aujourd'hui un curl ») ; `CONTRIBUTING.md:61-62` (« API first, UI never (for now) »).

---

## Contexte

Ootils n'a, avant ce chantier, strictement aucune surface humaine : `git grep` sur `.html`/`.jsx` dans `src/` ne renvoie rien, et la seule façon d'approuver une recommandation L3 (Decision Ladder, North Star) est un `curl -X POST /v1/recommendations/{id}/transition` avec un Bearer token en ligne de commande. Ce n'est pas un défaut cosmétique : le North Star pose explicitement que « chaque action est classée par réversibilité/risque » et que « L3-L4 requièrent une approbation humaine » — mais rien dans le produit ne *montre* à un humain ce qu'il approuve, ni ne lui donne un lieu où le faire. L'audit adversarial qui a nourri le cadrage H2 identifie ceci comme « l'écart n°1 qu'un dirigeant verra » en démo face à un APS établi.

La tension est nommée explicitement dans `CONTRIBUTING.md:61-62` : « API first, UI never (for now) ». Le cadrage ROADMAP §EXP-1 posait la question comme un arbitrage pilote ouvert à trois issues : page minimale / démo API-only assumée / repousser. **Le pilote a tranché le 2026-07-10 : une page unique server-rendered.**

## Décision

### 0. L'arbitrage qui lève la tension CONTRIBUTING.md

> API-first reste la doctrine ; la fenêtre est un client mince read+approve au-dessus de l'API existante — aucune logique métier, aucun chemin privilégié — sanctionnée pour la démo, pas un produit UI.

Ce n'est pas un reniement de « API first, UI never » : c'est la reconnaissance que **North Star exige un visage pour la Decision Ladder**, et que la seule façon de fournir ce visage sans trahir la doctrine est de construire un CLIENT au sens strict — zéro logique métier server-side propre à la page, zéro chemin d'accès aux données qui contourne `/v1/*`. Une « fenêtre », pas un produit UI (formule du cadrage, reprise ici comme critère d'acceptation, pas comme slogan) : PR1 est read-only, PR2 (hors périmètre de ce PR) ajoutera approve/reject en rejouant exactement `POST /v1/recommendations/{id}/transition`, jamais une route parallèle.

### 1. Une seule voie d'authentification — refus explicite du cookie de session

`api/auth.py` expose un unique mécanisme d'authentification (`resolve_principal` / `require_scope`, Bearer token, `hmac.compare_digest` ou lookup `api_tokens`). Ce chantier n'en introduit **aucun second** :

- **Pas de cookie de session.** Un cookie ouvrirait un chemin d'auth parallèle (le navigateur l'attache automatiquement à chaque requête, y compris cross-site) et une exposition CSRF que ce dépôt n'a jamais eu à gérer jusqu'ici — l'ajouter pour une seule page serait une dette de sécurité disproportionnée à la valeur livrée.
- **Pas de stockage serveur du token.** Le token humain est saisi dans un champ de `console.html`, gardé en `sessionStorage` du navigateur (jamais `localStorage` — la session s'efface à la fermeture de l'onglet, ce qui borne la durée d'exposition), et envoyé en `Authorization: Bearer` sur **chaque** `fetch()` vers `/v1/*`. Le serveur ne voit, ne mémorise, ne loggue jamais ce token — `api/ui/static/app.js` ne contient aucun `console.log` touchant `getToken()`/`setToken()`, et le token n'apparaît dans aucune URL (jamais en query string, uniquement en en-tête).
- **`GET /ui` lui-même ne porte aucune authentification** — exactement comme `/health`. Un navigateur n'attache pas d'en-tête `Authorization` à une navigation brute ; poser une dépendance `require_scope` sur cette route serait donc du théâtre, jamais une frontière réelle. La page est un HTML statique, **sans donnée métier serveur** : le contexte Jinja2 passé à `TemplateResponse` est un dict vide (`{}`) — toute donnée affichée vient d'un `fetch()` client authentifié après coup, jamais du rendu serveur.

### 2. `GET /ui` — shell HTML pur, kill switch au démarrage, pas par requête

`api/routers/ui.py` sert `console.html` via `Jinja2Templates` (autoescape ON par défaut dans `starlette.templating.Jinja2Templates` — vérifié contre la source : `env_options.setdefault("autoescape", True)`, non surchargé ici). Templates sous `src/ootils_core/api/ui/templates/`, assets statiques sous `src/ootils_core/api/ui/static/`.

**Kill switch `OOTILS_UI_ENABLED`, DÉFAUT OFF** (🎯 décision pilote consignée dans le cadrage — « la démo le flippe »). Contrairement aux kill switches habituels du dépôt (`outcomes.py`/`scenarios.py` : vérifiés **par requête**, après auth/scope, avant la DB, répondant `503`), celui-ci est évalué **une seule fois, à `create_app()`** — mirroir exact de `_api_docs_enabled()` qui gate `docs_url` dans `api/app.py`. Raison : `GET /ui` n'a **aucune** dépendance d'auth après laquelle ordonner un contrôle par requête (voir §1), et gater l'enregistrement lui-même signifie qu'une fenêtre désactivée est un **404 propre** — la route et son mount statique n'existent tout simplement pas — plutôt qu'une route qui existe et refuse poliment. Vérifié empiriquement (`TestClient`, switch off) : `GET /ui` → 404, `GET /ui/static/app.js` → 404.

**Piège FastAPI découvert et documenté** (`api/routers/ui.py`, docstring du module) : `Router.include_router()` ne copie **pas** les routes `Mount` d'un sous-`APIRouter` vers l'app parente — un `Mount` ajouté via `APIRouter.mount()` n'atteint jamais `app.routes` en passant par `include_router` (vérifié empiriquement contre fastapi 0.128 : un routeur de test avec un seul `Mount` produit un `app.routes` qui ne contient QUE les routes internes FastAPI, pas le mount). Le mount des fichiers statiques (`mount_ui_static`) doit donc être appelé **directement sur l'instance `FastAPI`**, jamais plié dans l'objet routeur — `include_ui(app)` est le point unique qui enregistre les deux ensemble, sous le même kill switch.

`GET /ui` est `include_in_schema=False` — comme `/metrics`, ce n'est pas une route du contrat API versionné (`/v1/*`) ; elle n'apparaît jamais dans `docs/openapi.json`, y compris quand le kill switch est ON (vérifié : `'/ui' in create_app().openapi()['paths']` → `False` avec `OOTILS_UI_ENABLED=1`).

### 3. `GET /v1/whoami` — introspection cosmétique, jamais une frontière

`api/routers/me.py` expose `GET /v1/whoami` sous `require_scope("read")` — un endpoint `/v1/*` ordinaire, dans le contrat API versionné (visible dans `openapi.json`), pas une route spéciale de la fenêtre. Il sérialise le `Principal` résolu pour la requête : `name`, `actor_kind`, `scopes` (triés), `is_legacy`, `token_prefix` (le préfixe non-secret `request.state.client_id`, `None` pour le token legacy dont le `client_id` est le sentinel `"global_token"`, pas un vrai préfixe de token miné). **Jamais le token** — `Principal` lui-même ne porte jamais le secret brut, seul son SHA-256 vit côté serveur (`api/auth.py:hash_token`).

`console.html`/`app.js` l'utilisent pour afficher « Connecté en tant que… » et — en PR2, hors périmètre ici — masquer les boutons d'action si `recommend:approve` est absent des scopes. C'est **cosmétique** : le serveur reste le seul arbitre via `require_scope` sur chaque écriture réelle ; un client qui cache un bouton qu'il ne peut pas utiliser est un confort UX, jamais une frontière de sécurité — repris texto de la doctrine ADR-032 (scopes = frontière serveur, jamais côté client).

### 4. Trois panneaux, lecture seule, zéro nouvel endpoint métier

PR1 consomme trois endpoints **existants**, sans en modifier le contrat :

| Panneau | Endpoint | Champs consommés |
|---|---|---|
| Inbox recommandations, groupée par niveau L | `GET /v1/recommendations` | `recommendations[].{item_external_id,action,status,decision_level,shortage_date,deficit_qty,recommended_qty,estimated_cost,currency,confidence}`, `total` |
| 5 KPI de preuve | `GET /v1/outcomes/summary` | `pct_shortages_avoided`, `avoided_basis_count`, `avoided_severity_usd_total`, `avg_fva_wape`, `fva_basis_count`, `reco_approval_rate`, `reco_total_count`, `cost_of_inaction_usd`, `from_date`, `to_date` |
| Comparaison scénarios | `GET /v1/scenarios/compare?ids=…` | `comparable`, `reference_scenario_id`, `cost_precedence`, `entries[].{name,status,computable,stale,note,kpis.{shortage_count,shortage_severity_usd,stock_value_usd,fill_rate_est},deltas.{shortage_count_delta,severity_usd_delta}}` |

**Vérifié avant écriture** : les trois routeurs (`api/routers/recommendations.py`, `outcomes.py`, `scenarios.py`) ont été relus en entier ; chaque champ listé ci-dessus existe littéralement dans le `response_model` Pydantic correspondant — aucun champ inventé côté `app.js`.

**Écarts assumés vs. le cadrage, justifiés :**
- « inbox des recommandations par niveau L » — `GET /v1/recommendations` n'expose **pas** de filtre `level` côté requête (seuls `status`/`action`/`agent_name` existent). Le regroupement par `decision_level` (`L1`/`L2`/`L3`) est donc fait **côté client**, après un seul fetch (`limit=200`) — pas une requête par niveau. Documenté ici plutôt que silencieusement contourné : un futur PR pourrait ajouter un filtre `level` server-side si la pagination le justifie à l'échelle.
- Seules les tables `recommendations` sont exposées (contrat explicite du cadrage : « table `recommendations` UNIQUEMENT, les tables sœurs n'ont pas de state machine HTTP — hors périmètre », confirmé contre le code : `dq_findings`/`mrp_action_messages` n'ont aucune route `/v1/*` de transition).
- La comparaison de scénarios n'affiche pas tous les champs de `ScenarioCompareKpisOut` (omis : `below_safety_stock_count`, `stock_value_basis_count`, `stock_value_unpriced_count`, `fill_rate_basis_count`) — un choix d'espace d'affichage pour une page minimale, pas un défaut de contrat ; les champs existent et restent disponibles pour PR2/itération si le retour démo le réclame.

**Aucun bouton d'action en PR1** (approve/reject) — conforme au contrat : « PAS de boutons d'action en PR1 (PR2) ».

### 5. `None`/`null` rendu `"n/a"`, jamais un zéro inventé

`app.js:fmt()` est la seule fonction de formatage utilisée pour toute valeur affichée : `value === null || value === undefined || value === ""` → `"n/a"`, sinon `String(value)`. Ce n'est pas cosmétique : les cinq KPI de preuve (`outcomes.py`, ADR-030) sont explicitement NULL/0-honnêtes côté serveur (« NULL = pas de données, 0 = vrai zéro ») — un rendu naïf qui confondrait `null` et `0` inventerait un zéro que le serveur a précisément refusé d'affirmer. `fmt()` préserve la distinction jusqu'au pixel : un `0` réel s'affiche `"0"`, un `null` s'affiche `"n/a"`.

### 6. CSP stricte, zéro script inline

`console.html` ne contient **aucun** `<script>` inline, **aucun** attribut `onclick=`/`onload=…`, **aucun** `<style>` inline — un seul `<script src="/ui/static/app.js" defer>`. `app.js` attache tous les gestionnaires d'événements via `addEventListener` (jamais d'attribut HTML). Ceci permet une CSP `default-src 'self'; frame-ancestors 'none'` **sans** `'unsafe-inline'`, branchée dans `api/app.py` (~l.371) sur une nouvelle branche `elif path == "/ui" or path.startswith("/ui/")`, au même niveau que la branche existante `/docs`/`/redoc` (qui, elle, garde `'unsafe-inline'` — nécessaire à Swagger UI, jamais retiré). `default-src 'self'` couvre `script-src`/`style-src`/`connect-src` par repli (aucune directive plus spécifique n'est posée), ce qui autorise `/ui/static/app.js` (même origine) et les `fetch()` vers `/v1/*` (même origine) sans rien ouvrir de plus. Vérifié par requête réelle (`TestClient`) : `GET /ui` et `GET /ui/static/app.js` renvoient tous deux `Content-Security-Policy: default-src 'self'; frame-ancestors 'none'`.

### 7. Aucune migration, `openapi.json` régénéré

Zéro table, zéro colonne touchée — les trois endpoints consommés existent déjà. `scripts/export_openapi.py` a été exécuté après l'ajout de `GET /v1/whoami` : `docs/openapi.json` gagne exactement une entrée (`/v1/whoami`), `GET /ui` n'y apparaît jamais (kill switch OFF par défaut à l'export, et `include_in_schema=False` de toute façon même si ON — vérifié les deux cas).

## Alternatives rejetées

- **Cookie de session pour le token.** Rejeté — ouvrirait un second chemin d'authentification parallèle à `resolve_principal`/`require_scope`, plus une exposition CSRF qu'aucune route du dépôt ne gère aujourd'hui. `sessionStorage` + en-tête `Authorization` explicite sur chaque `fetch()` reste dans l'unique voie d'auth existante.
- **Framework front (React/Vue/etc.) ou build step.** Rejeté par le cadrage lui-même (« FastAPI + template, zéro framework front ») et par l'esprit « une fenêtre, pas un produit UI » — un JS vanilla en fichier statique suffit à trois panneaux read-only et évite d'introduire une chaîne de build dans un dépôt qui n'en a aucune.
- **Kill switch `OOTILS_UI_ENABLED` vérifié par requête (503), comme `outcomes.py`/`scenarios.py`.** Envisagé pour rester cohérent avec le motif dominant du dépôt, mais rejeté : `GET /ui` n'a pas de dépendance d'auth après laquelle ordonner ce contrôle (c'est précisément le point du §1 — pas d'auth sur cette route), et un 404 au démarrage (mirroir de `_api_docs_enabled()`) est un signal plus honnête pour une page qui, désactivée, ne doit littéralement pas exister comme surface réseau.
- **Filtre `level` server-side sur `GET /v1/recommendations`.** Envisagé pour coller au texte du cadrage à la lettre, rejeté pour PR1 — le regroupement client-side sur un seul fetch (`limit=200`) suffit à l'échelle démo actuelle (2-50 items, `docs/SCALABILITY.md`) et évite de toucher un contrat d'endpoint stable pour un gain cosmétique ; à reconsidérer si la pagination le justifie.
- **Bouton approve/reject en PR1.** Rejeté par le contrat explicite du cadrage (« PAS de boutons d'action en PR1 (PR2) ») — PR1 prouve la lecture avant d'ouvrir la première écriture depuis la page.

## 🎯 Pilote

- **Kill switch au démarrage plutôt que par requête.** Cohérent avec `_api_docs_enabled()` mais c'est le premier kill switch du genre appliqué à une route `/v1`-adjacente consommée par un humain plutôt qu'un agent/CLI — si un besoin de bascule à chaud (sans redéploiement) émergeait pour la démo, il faudrait migrer vers le motif 503-par-requête des autres kill switches, au prix de perdre le 404 « la route n'existe pas ».
- **Regroupement par niveau L côté client, pas de filtre serveur.** Choix d'échelle démo, pas une contrainte architecturale — à recalibrer si `GET /v1/recommendations` gagne de la volumétrie avant PR2.
- **Champs de `ScenarioCompareKpisOut` affichés en PR1.** Sous-ensemble choisi pour la lisibilité d'une page minimale ; le reste des champs (basis counts détaillés) est disponible côté contrat et peut être ajouté sans changement serveur si la démo le réclame.

## Conséquences

- **Positif :** la Decision Ladder a enfin un visage — un humain peut voir l'inbox de recommandations, les 5 KPI de preuve et la comparaison de scénarios sans `curl`. Zéro nouvelle logique métier server-side : la page reste, au sens strict, un client de l'API existante — trois endpoints déjà couverts par leurs propres tests, aucun nouveau chemin de lecture/écriture de données.
- **Négatif / dette assumée :**
  - PR1 est lecture seule — l'approbation L3 reste `curl`/CLI tant que PR2 n'est pas livré. La fenêtre ne remplace donc pas encore le geste d'approbation lui-même, seulement sa préparation (voir l'inbox, les KPI, comparer).
  - Le regroupement par niveau L est client-side sur un `limit=200` fixe — pas de pagination ni de filtre serveur sur `decision_level` ; correct à l'échelle démo, pas conçu pour un volume de production.
  - `GET /ui`/`GET /v1/whoami` sont testés dans ce même PR : 27 tests unitaires DB-free (`tests/test_ui_window.py` — kill switch OFF testé comme défaut réel, shell dataless byte-identique, CSP sans `unsafe-inline`, whoami sans jamais le token) + 18 cas d'intégration (`tests/integration/test_ui_window_integration.py`, écrits en aveugle — legacy admin-équivalent, token minté à scopes réduits, 401 post-révocation) qui tournent contre une vraie DB en CI.
  - Aucune protection contre un opérateur qui coderait en dur son token dans un signet/raccourci navigateur — hors du contrôle serveur par construction (le token vit dans le navigateur de l'opérateur, pas dans Ootils).
- **Reste à faire :** PR2 (boutons approve/reject rejouant `POST /v1/recommendations/{id}/transition`, masqués si `recommend:approve` absent des scopes — cosmétique, le serveur reste l'arbitre) ; exposer `graph_fragment` dans `GET /v1/explain` (mentionné dans le même item de cadrage, hors périmètre de ce PR).

## Code references

- `src/ootils_core/api/routers/ui.py` — module entier : `GET /ui`, le kill switch `ui_enabled()`, le piège `include_router`/`Mount`, `include_ui()`.
- `src/ootils_core/api/routers/me.py` — module entier : `GET /v1/whoami`, `WhoAmIOut`.
- `src/ootils_core/api/ui/templates/console.html` — le shell, zéro donnée serveur, zéro script/style inline.
- `src/ootils_core/api/ui/static/app.js` — le client vanilla : `sessionStorage`, `fetch()` Bearer, `fmt()` None-honnête, rendu DOM (jamais `innerHTML` avec une chaîne concaténée — toujours `textContent`/`createElement`, une défense XSS structurelle indépendante de l'autoescape Jinja2).
- `src/ootils_core/api/app.py` — imports `me`/`ui` ; `application.include_router(me.router)` + `ui.include_ui(application)` (enregistrés après les routers `/v1`, juste avant le retour de `create_app`).
- `src/ootils_core/api/app.py:379-386` — branche CSP `/ui`, au même niveau que la branche `/docs`/`/redoc` existante.
- `src/ootils_core/api/auth.py` — `resolve_principal`/`require_scope`/`Principal` : l'unique mécanisme d'auth, non dupliqué par ce chantier.
- `src/ootils_core/api/routers/recommendations.py:66-96` — `RecommendationOut`/`RecommendationsListResponse`, les champs consommés par le panneau inbox.
- `src/ootils_core/api/routers/outcomes.py:127-162` — `OutcomeSummaryOut`, les 5 KPI de preuve consommés par le panneau KPI.
- `src/ootils_core/api/routers/scenarios.py:156-207` — `ScenarioCompareKpisOut`/`ScenarioCompareDeltasOut`/`ScenarioCompareEntryOut`/`ScenarioCompareOut`, les champs consommés par le panneau comparaison.
- `pyproject.toml` — dépendance `jinja2 ~= 3.1` (core, pas un extra) ; `package-data` étendu à `api/ui/templates/*.html` + `api/ui/static/*.js`.
- `docs/openapi.json` — régénéré (`scripts/export_openapi.py`) ; une seule entrée ajoutée, `/v1/whoami`.
- **Tests livrés dans ce PR** (`tests/test_ui_window.py` 27 unit + `tests/integration/test_ui_window_integration.py` 18 cas — la liste ci-dessous est couverte) :
  - `GET /ui` : 404 par défaut (switch OFF) ; 200 + `Content-Type: text/html` + CSP `default-src 'self'` sans `unsafe-inline` quand ON ; shell ne contient aucune donnée métier (pas de fetch au rendu serveur).
  - `GET /ui/static/app.js` : 404 par défaut, 200 quand ON, même branche CSP que `/ui`.
  - `GET /v1/whoami` : 401 sans token ; 200 avec le token legacy (`token_prefix=None`, `scopes=["admin"]`) ; 200 avec un token miné (scopes réels, `token_prefix` non-null) ; jamais le token en clair dans la réponse.
  - `/v1/whoami` n'apparaît dans `openapi.json` qu'avec ses champs attendus ; `/ui` n'y apparaît jamais, switch ON ou OFF.
  - Intégration : les trois panneaux contre une vraie DB seedée (`tests/integration`, pattern `test_recommendations_api_integration.py`/`test_scenario_compare_integration.py`) — vérifier que chaque champ que `app.js` lit est bien celui que l'endpoint renvoie sur un jeu de données réel (regroupement par `decision_level`, `n/a` sur un KPI NULL réel, `stale`/`computable` sur un scénario sans calc_run).
