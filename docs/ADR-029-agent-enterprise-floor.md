# ADR-029 — Étage entreprise agents : identité d'acteur cryptographique, tokens par agent, scopes, kill-switch

**Statut :** Accepté rétroactivement (2026-07-07). Le code est mergé — chantier #392 PR1, PR [#403](https://github.com/ngoineau/ootils-core/pull/403) le 2026-07-05 (migration `064_api_tokens_and_scopes.sql` + `api/auth.py`). Cet ADR acte la décision déjà appliquée et référencée trois fois par le header de la migration 064 (« ADR-029 (à venir) »).
**Date :** 2026-07-07
**Auteurs :** ootils-core team
**Contexte mesuré :** header de `src/ootils_core/db/migrations/064_api_tokens_and_scopes.sql` et implémentation `src/ootils_core/api/auth.py`, qui référencent tous deux cet ADR.

---

## Contexte

Le North Star exige que chaque action soit **classée sur la Decision Ladder L0–L4** et que les actions L3+ (irréversibles) soient réservées à un humain via la state machine d'approbation #341. Cette machine repose sur un fait : « c'est un humain, pas un agent, qui a approuvé ». Mais avant #392, ce fait n'était **pas prouvable**.

Deux trous précis, mesurés dans le code d'alors :

- **`actor_kind` était auto-déclaré par le corps de la requête.** L'API validait un unique `OOTILS_API_TOKEN` partagé, et chaque appelant affirmait lui-même son genre (`agent` / `human` / `service`) dans le payload. La machine #341 — qui verrouille les L3+ sur « approuvé par un humain » — faisait donc confiance à un champ que l'appelant remplit lui-même. Un agent compromis ou bogué pouvait se tamponner `human` et faire passer un `CANCEL` irréversible devant le gate humain. Le gate le plus critique du système reposait sur de l'honnêteté déclarative.
- **Aucune granularité de capacité, aucun kill-switch, aucun budget.** Le token partagé était tout-ou-rien : quiconque le détenait pouvait déclencher `POST /v1/mrp/run`, muter le graphe, tout faire. Le North Star (« budgeted / kill-switchable » — idempotence, scopes par agent, rate limits, kill-switch global) n'avait aucun substrat.

Il fallait un **étage entreprise** : faire de l'identité d'acteur une propriété de la **crédential** (posée une fois, à l'émission, par un opérateur), non du message ; et donner à chaque token un jeu de scopes, une durée de vie, un interrupteur. Sans casser un seul appelant pré-#392 — d'où une transition, pas une rupture.

## Décision

### 1. Registre de tokens `api_tokens` — l'identité d'acteur est cryptographique (migration 064)

Une ligne par crédential émise. Le **cleartext du token n'est jamais stocké** : seul son empreinte `token_hash` (SHA-256 hex, clé de lookup + unicité) et un `token_prefix` lisible et non-secret (`ootk_XXXXXXX`) vivent en base. Une fuite de la table ne fuite donc aucune crédential utilisable — le token en clair est montré **exactement une fois**, à l'émission.

- **`actor_kind TEXT NOT NULL CHECK (actor_kind IN ('agent', 'human', 'service'))`** — c'est la source cryptographique de la Decision Ladder. Posé une fois, par un opérateur, à l'émission du token. La machine #341 et le gate humain L3+ le lisent **depuis le token présenté**, jamais depuis le payload. Le corps de la requête ne peut plus influencer *qui* est l'appelant. C'est l'invariant central de l'ADR.
- **SHA-256 sans KDF — délibéré.** Un KDF de mot de passe (bcrypt/argon2) existe pour ralentir la force brute contre des secrets humains à faible entropie. Nos tokens ne sont pas des secrets humains : ils sont frappés à partir de 32 octets d'`os.urandom` (256 bits d'entropie), rendus `ootk_<base>`. Une chaîne aléatoire de 256 bits n'est pas brute-forçable quelle que soit la vitesse du hash ; un KDF lent n'achèterait donc aucune sécurité tout en ajoutant du CPU par requête sur le chemin d'auth chaud. SHA-256 hex est le choix correct et standard pour des clés d'API à haute entropie (même logique que les clés GitHub/Stripe : hash rapide + préfixe).
- **`scopes TEXT[]` — pas de JSONB, pas de table de jointure.** Un ensemble de scopes est une liste plate de chaînes courtes à forme connue : c'est exactement ce que modélise un `TEXT[]` natif, avec ses opérateurs de tableau (`'shortage:read' = ANY(scopes)`) et un containment indexable GIN. Ce n'est **pas** une entorse à la doctrine « no JSONB » (CLAUDE.md) : c'est une colonne typée. **Aucun CHECK sur le contenu du tableau** : le vocabulaire des scopes valides est validé en **code applicatif** (la whitelist de la couche auth), pas par une contrainte SQL — sinon chaque nouveau scope exigerait une migration pour élargir le CHECK, couplant l'évolution des scopes au runner de migrations. La base stocke le grant ; l'appli décide ce qu'un grant a le droit de contenir.
- **`recommendation_transitions.actor_kind` élargi ici même.** `'service'` devient un `actor_kind` de première classe dans cette migration ; la migration 040 ne CHECK-ait que `('human', 'agent')`. La migration qui introduit une nouvelle valeur d'acteur est le bon endroit — le seul — pour garder tous les CHECK `actor_kind` du schéma synchronisés (élargissement par introspection `pg_constraint`, name-safe, cf. header §3).

### 2. Résolution du principal — fail-closed, cache borné, kill-switch (`api/auth.py`)

`resolve_principal` est la dépendance FastAPI unique qui authentifie le Bearer et résout l'appelant en `Principal` (`token_id`, `name`, `actor_kind`, `scopes`, `is_legacy`).

- **Deux saveurs de token coexistent.** Un token `ootk_` est cherché dans `api_tokens` (la ligne est **la** vérité pour `actor_kind` + `scopes`). Tout autre token est traité comme le token legacy `OOTILS_API_TOKEN`, comparé en `hmac.compare_digest` comme avant, et résolu vers un `Principal` synthétique `human` / `admin` (superset). Chaque appelant pré-#392 continue de fonctionner à l'octet près.
- **Gate humain résolu depuis le TOKEN, jamais depuis le body** — `resolve_gate_kind()`. Pour un principal **minté**, le token EST la vérité : `declared_actor_kind` du body est ignoré, point. C'est l'objet même de #392.
- **Fail-closed, sans chemin d'auth optionnel.** `OOTILS_API_TOKEN` est validé à l'import (le process refuse de démarrer sans). Token manquant / malformé / inconnu / révoqué / expiré → 401. Un lookup de token minté qui **ne peut pas joindre la DB** → 503, **jamais** un fall-through vers 200 (une backend d'auth injoignable ne doit jamais laisser passer). Un 503 transitoire n'est jamais mis en cache.
- **Cache mémoïsé TTL-30 s, borné en taille (LRU).** Le lookup minté est mémoïsé par valeur de token pendant `_CACHE_TTL_SECONDS` (30 s) pour garder le chemin chaud sans pool. Les résultats **négatifs** (token inconnu) sont cachés aussi, ce qui émousse le re-probing d'une **même** mauvaise valeur — mais **pas** un flood de valeurs distinctes, chacune étant un vrai cache-miss. Le cache est donc aussi **borné en taille** (`_CACHE_MAX_ENTRIES = 10 000`, éviction LRU au débordement) : un flood de `ootk_<random>` distincts dégrade vers une mémoire bornée, jamais une croissance illimitée.
- **`last_used_at` bumpé best-effort, isolé de l'auth.** Le bump tourne dans sa propre connexion/transaction et **ne doit jamais faire échouer l'authentification** : un standby read-only ou un lock concurrent dégrade silencieusement le bump, jamais l'auth.
- **Kill-switch global `OOTILS_AGENTS_ENABLED`** (défaut ON). Une valeur falsy désactive tout principal `actor_kind='agent'` (503, identité de l'agent bloqué loguée : nom + token_id + prefix, jamais le token brut) en laissant passer humains et services. Décision env-only : une flotte désactivée répond 503 sans jamais toucher la DB.

### 3. Transition legacy — pas de rupture

Le token legacy résout vers un `Principal` synthétique `human` / `admin` (superset de scopes), donc tout appelant pré-#392 passe tous les scopes et le gate humain sans régression. Point subtil corrigé (defect 9 de la revue sécurité) : pour un principal **legacy uniquement**, si le body déclare encore un `actor_kind`, le gate décide sur cette valeur déclarée — exactement le comportement pré-#392 préservé jusqu'à ce que le token legacy soit retiré. Un token minté n'est jamais réinterprété par le body. Le token legacy est **déprécié, pas supprimé**.

## Portée

- **Auth / gouvernance, transverse aux scénarios.** Un token et son `actor_kind` sont une donnée d'infrastructure, invariante par scénario. Aucune forkabilité requise — un token n'est pas un levier de simulation.
- **Le ledger d'audit survit à la suppression d'un token.** `api_request_log.token_id → api_tokens` est `ON DELETE SET NULL` (les lignes d'audit survivent à un hard-delete de token), et `actor_kind` est **dénormalisé** sur chaque ligne d'audit à l'écriture : la trace reste répondable (« agent ou humain ? ») même après suppression du token. La dénormalisation est le bon choix pour un log immuable — il enregistre ce qui était vrai à l'instant t, pas ce que le registre dit maintenant.

## Décidé mais pas encore appliqué (honnêteté — dette tracée)

L'étage entreprise est **posé** (le substrat existe et est enforçable) mais n'est **pas encore branché partout**. À ne pas re-claimer comme livré :

- **Scopes non appliqués sur ~24 routers.** `require_auth` résout le principal mais ne vérifie **aucun** scope ; seule une poignée de routers utilisent `require_scope`. Un token read-only peut aujourd'hui déclencher un write (`POST /v1/mrp/run`, mutation de graphe). Basculer **tous** les routers d'écriture vers `require_scope` est le chantier **AN-2** de `docs/ROADMAP-AGENTS-2026-H2.md` (§4).
- **`api_tokens.rate_per_min` est du schéma mort.** La colonne existe (budget par token) mais **aucun code ne la lit** : aucun rate-limit par token n'est appliqué. Câblage prévu en AN-2 (middleware au checkout du Principal).
- **Endpoints issue/revoke de tokens gouvernés** et **`/metrics`** : prévus en AN-2. L'émission passe aujourd'hui par un script CLI (`scripts/issue_agent_token.py`).

Ces items ne sont **pas** de la dette silencieuse : ils sont ici pour que l'écart entre « le substrat est en place » et « il est enforcé partout » soit explicite.

## Conséquences

- **Positif :** la Decision Ladder et le gate humain #341 deviennent **réellement enforçables** — `actor_kind` n'est plus usurpable par un payload. Le kill-switch `OOTILS_AGENTS_ENABLED` donne un interrupteur global sur la flotte. L'audit reste répondable pour toujours, immunisé contre la suppression de token. Aucun appelant pré-#392 ne régresse (chemin legacy préservé à l'octet près).
- **Négatif / dette assumée en V1 :** l'enforcement des scopes et des budgets n'est pas encore généralisé (§ ci-dessus) — un token peut, aujourd'hui, dépasser ses scopes sur les routers non encore basculés. C'est le périmètre exact d'AN-2. Le token legacy `admin`-superset reste un contournement tant qu'il n'est pas retiré.

## Références

- **#392** — chantier « étage entreprise agents » (tokens par agent, scopes, budgets, kill-switch, /metrics).
- **PR [#403](https://github.com/ngoineau/ootils-core/pull/403)** — PR1 mergée le 2026-07-05 (migration 064 + auth.py).
- `src/ootils_core/db/migrations/064_api_tokens_and_scopes.sql` — table `api_tokens` (SHA-256, `actor_kind` CHECK, `scopes TEXT[]`), binding audit, élargissement `recommendation_transitions.actor_kind` ; source de vérité du schéma, header détaillé.
- `src/ootils_core/api/auth.py` — `resolve_principal`, `resolve_gate_kind`, `require_scope`, cache TTL-30 s borné LRU, kill-switch `OOTILS_AGENTS_ENABLED`, principal legacy synthétique.
- `docs/ROADMAP-AGENTS-2026-H2.md` §4 — chantier **AN-2** (scopes bout-en-bout + budgets `rate_per_min` + endpoints issue/revoke + /metrics).
- `docs/ADR-030-proof-machine.md` — même exigence North Star « déterministe / auditable » sur l'axe preuve ; convention FK `ON DELETE RESTRICT` explicite pour les FK vers `scenarios`.
