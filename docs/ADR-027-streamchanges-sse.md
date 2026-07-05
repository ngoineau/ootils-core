# ADR-027 — StreamChanges : flux SSE rejouable sur `events`

**Statut :** Accepté — chantier #391, PR1 (substrat + endpoint) code-complet sur `feat/streamchanges-sse`, revu et corrigé par une revue adversariale (13 agents, vérifications empiriques contre la stack installée Starlette 0.50/uvicorn 0.40/psycopg 3.3.3) qui a confirmé 7 défauts sur l'implémentation initiale — tous corrigés dans ce même code-complet (mécanisme de libération de slot, réveil `notifies()`, cadence de heartbeat, mode `once`, plus des corrections de doc). La PR2 (multiplexage flotte : émission des events manquants + watchers `--subscribe`) est une suite planifiée, cf. §Suite.
**Date :** 2026-07-05
**Contexte North Star :** principe **Streamable** de `CLAUDE.md` — « les agents s'abonnent aux deltas, ils ne pollent pas ». Jusqu'ici ce principe n'avait aucune surface concrète : les agents devaient poller `GET /v1/events`.

---

## Contexte

Le principe Streamable du North Star est resté aspirationnel : aucune surface ne permettait à un agent de s'abonner aux deltas. La seule voie était de poller `GET /v1/events` en boucle, ce qui est à la fois coûteux (une requête toutes les N secondes par agent) et intrinsèquement en retard (la latence de détection est le pas de polling). La flotte de watchers (#340, #346, #347) écrit déjà ses transitions dans la table `events` — la migration 051 émet un event par transition de recommandation *précisément pour que les agents s'abonnent au lieu de poller* — mais rien ne consommait ce flux en push.

Trois contraintes cadraient la décision :

1. **La table `events` n'a aucune clé d'ordre rejouable.** `event_id` est un UUID v4 (aléatoire, non ordonnable) et `created_at` est un `TIMESTAMPTZ DEFAULT now()` qui collisionne à la microseconde sous inserts groupés — deux lignes peuvent partager le même instant, donc un curseur `> last_seen_ts` sauterait ou dupliquerait des lignes. Un consommateur qui (re)connecte n'a aucun point de reprise fiable.
2. **Le pool de connexions synchrones est un budget rare.** Le threadpool qui sert les handlers `def` de FastAPI est borné à la taille du pool DB (`app.py` lifespan, breaking point #6 de `docs/SCALABILITY.md`). Un flux long-vécu qui emprunterait une connexion du pool serait un DoS auto-infligé : N flux concurrents épingleraient tout le pool et affameraient tout handler `def`.
3. **La reprise après coupure doit être triviale pour un `EventSource` de navigateur.** Une reconnexion doit reprendre exactement là où le client s'est arrêté, sans négociation applicative, en s'appuyant sur le mécanisme standard `Last-Event-ID` du protocole SSE.

Alternatives envisagées : WebSocket (bidirectionnel, négociation d'upgrade, dépendance/complexité serveur) ; une table `outbox` dédiée alimentée par un pattern transactionnel explicite ; du long-polling sur `GET /v1/events`.

## Décision

Un endpoint **Server-Sent Events** `GET /v1/stream` diffuse le tail de la table `events` pour un scénario. La vérité rejouable est un **SELECT keyset** sur une nouvelle colonne `events.stream_seq` (migration 063) ; `LISTEN`/`NOTIFY` sert de **simple réveil**. Le contrat REST détaillé est spécifié dans `docs/SPEC-INTERFACES.md` §5.2 ; cet ADR fixe les décisions structurantes.

### SSE plutôt que WebSocket

Le flux est **read-only descendant** : le serveur pousse des deltas, le client ne renvoie rien. SSE couvre exactement ce besoin avec `StreamingResponse` natif de Starlette — **zéro dépendance ajoutée**, `media_type="text/event-stream"`. L'auth Bearer est triviale (le header `Authorization` passe comme sur tout autre `/v1/*`, contrairement à WebSocket où l'auth pré-upgrade est plus tordue), et la reprise après coupure est native via `Last-Event-ID`. WebSocket serait de la sur-ingénierie : on paierait la bidirectionnalité et la négociation d'upgrade pour un canal qui n'a jamais de trafic montant.

### Substrat hybride : keyset = vérité, NOTIFY = réveil lossy

- **`stream_seq BIGINT GENERATED ALWAYS AS IDENTITY`** (migration 063) donne à chaque event un entier strictement croissant par insert : LE curseur rejouable. Un consommateur reprend avec `WHERE scenario_id = $1 AND stream_seq > $last_seen` — un scan keyset, sans OFFSET, sans polling du payload. La valeur est *engine-owned* (`GENERATED ALWAYS`, non fournissable par un INSERT), ce qui est exactement ce qu'un curseur digne de confiance exige.
- **Le trigger `events_stream_notify`** (AFTER INSERT) fait `pg_notify('ootils_events', <scenario_id>)` : un réveil, rien d'autre. Le payload est le `scenario_id` *seul* (le cap NOTIFY de 8 KB interdit d'y mettre le corps de l'event ; de toute façon le curseur va chercher les lignes). Un NOTIFY manqué (consommateur pas encore en LISTEN, rebond de connexion, gap de reconnexion) **coûte de la latence, jamais de la justesse** : tout (re)connexion déclenche un drain keyset depuis le dernier `stream_seq` livré *avant* de faire confiance aux notifications, et le heartbeat périodique redraine de toute façon.

Ce découplage est le cœur de l'ADR : la correction ne dépend jamais de la livraison fiable d'un NOTIFY.

### `events` EST l'outbox — pas de table dédiée

La table `events` **est** l'outbox de la flotte. Les transitions de recommandation y écrivent déjà (migration 051), la propagation y écrit ses deltas de dates/quantités (migrations 002/006) — toute feature qui change l'état émet déjà là. Ajouter une table `outbox` parallèle obligerait à toucher les N sites d'écriture existants pour un double-write, avec le risque de désync entre les deux tables. **Refusé.** À la place, la migration 063 ajoute une colonne d'ordre + un index + un trigger de réveil sur la table qui est *déjà* le point de convergence des changements. La convention devient : un event typé dans `events` = un delta streamé, gratuitement.

### Concurrence : `async def` + connexion async dédiée, hors du pool sync

Chaque flux est une coroutine `async def` qui possède une **connexion psycopg async dédiée**, ouverte **hors du pool synchrone** (même résolution de DSN que `OotilsDB` : `DATABASE_URL`/`OOTILS_DSN`, via `DEFAULT_DATABASE_URL`), en `autocommit=True` (requis pour que `LISTEN`/`NOTIFY` voie les notifications committées sans frontière de transaction explicite). Le flux ne touche jamais `get_db`/`dependencies.py`.

C'est **load-bearing, pas cosmétique** : un flux monté sur le pool synchrone serait un DoS auto-infligé (cf. contrainte 2). Garde-fous :

- **Kill-switch `OOTILS_STREAM_ENABLED`** (défaut ON) : falsy → 503 **avant tout accès DB**, en miroir du pattern de `param_overrides.py` (#347).
- **Budget `OOTILS_STREAM_MAX_CONN`** (défaut 32) : flux concurrents plafonnés, comptés par un compteur module sous un `threading.Lock` (pas `asyncio.Lock` — voir plus bas pourquoi).
- **Heartbeat ~15 s** : sur cadence horloge dépassée sans frame émise, une frame `event: ping` (sans ligne `id:`, donc n'avance pas le curseur client) est émise — anti-reap des proxies + filet de re-drain périodique du NOTIFY lossy.

#### Le budget est relâché par une bail idempotente à double chemin, pas un simple `finally`

**Version d'abord actée puis invalidée par la revue adversariale (13 agents, vérifications empiriques contre Starlette 0.50/psycopg 3.3.3 installés) :** « le slot est relâché dans un `finally` pour qu'un flux crashé/annulé ne le fuite jamais ». **Cette phrase est fausse telle quelle** — corrigée ici, pas gardée comme caveat gravé, parce que c'était un bug, pas un compromis assumé.

Le défaut réel : Python n'exécute **jamais** le corps d'un générateur asynchrone (donc jamais son `finally`) tant qu'il n'a pas été itéré au moins une fois, et Starlette 0.50 n'appelle **pas** `body_iterator.aclose()` sur le chemin où le client coupe la connexion TCP avant que l'ASGI runtime n'ait tiré le premier élément du flux. Un client qui tue sa connexion immédiatement après la requête (un `curl` tué, une boucle de reconnexion agressive côté agent) gagne donc systématiquement la course contre le premier `yield` : le générateur est abandonné sans jamais démarrer, son `finally` ne s'exécute jamais, et le slot réservé reste incrémenté **à vie** — un nombre de drops égal à `OOTILS_STREAM_MAX_CONN` transforme l'endpoint en 503 permanent jusqu'au redémarrage du process.

Correctif retenu — une bail (`_SlotLease`) dont `release()` est **idempotente** (drapeau `_done` sous verrou) et armée sur **deux chemins indépendants** :
1. Le `finally` du générateur appelle `lease.release()` — le chemin normal quand le générateur est effectivement itéré (démarré, annulé en cours de route, ou épuisé).
2. `weakref.finalize(gen_obj, lease.release)`, posé sur l'objet générateur **avant** qu'il ne soit donné à `StreamingResponse` — se déclenche quand l'objet générateur est collecté par le GC, ce qui arrive **même s'il n'a jamais été itéré** (le cas d'abandon ci-dessus).

Les deux chemins appellent la **même** bail : lequel se déclenche en premier gagne, le second est un no-op inoffensif. Conséquence directe sur l'implémentation du compteur : le verrou protégeant `_active_streams` doit être un `threading.Lock` **synchrone**, pas un `asyncio.Lock` — le callback de `weakref.finalize` peut être invoqué par le GC dans n'importe quel contexte, y compris hors d'une boucle événementielle en cours d'exécution, où `await lock.acquire()` n'a simplement pas de sens.

#### Le réveil `notifies()` ne doit jamais être abandonné en cours de génération

Défaut mineur relevé par la revue : la première implémentation faisait `async for _n in conn.notifies(timeout=…, stop_after=1): break`. Dans psycopg 3.3.3, `notifies()` est un générateur asynchrone qui `yield` **à l'intérieur** de `async with self.lock:` — le **même** verrou que `conn.execute` — et qui détache le backlog de notifications de la connexion pendant sa durée de vie. Sortir de la boucle avec `break` abandonne ce générateur en plein milieu de son corps, **sans jamais exécuter son `finally`** (qui restaure le backlog et relâche le verrou) : la seule raison pour laquelle le drain suivant n'était pas bloqué en permanence était le refcounting incidentel de CPython qui finissait par fermer le générateur abandonné « assez tôt ». Correctif : supprimer le `break` — avec `stop_after=1`, le générateur se termine **de lui-même** proprement dès qu'il a reçu une notification (vérifié dans la source installée : la boucle interne se `break` elle-même une fois `nreceived >= stop_after`, puis son propre `finally` s'exécute), rendant la restauration du backlog / la libération du verrou déterministe plutôt que dépendante du timing du GC.

#### La cadence de heartbeat est ancrée sur une horloge monotone, pas sur la source de réveil

Défaut majeur relevé par la revue : le canal `LISTEN`/`NOTIFY` `ootils_events` est **global** — chaque scénario y publie sur le même canal (migration 063, `pg_notify('ootils_events', scenario_id)`). Un flux abonné à un scénario **calme** sur un serveur **occupé** est donc réveillé par chaque `NOTIFY` d'un *autre* scénario, redraine à vide (rien à livrer sur son propre scénario), et — si le budget de temps du heartbeat était naïvement réarmé à chaque réveil plutôt qu'ancré sur l'horodatage de la **dernière frame réellement émise** — le timeout de 15 s ne serait jamais atteint : zéro ping pendant une durée non bornée, alors même que le contrat documenté est « un ping toutes les ~15 s ». Un proxy intermédiaire moissonnerait alors une connexion qu'il croit inactive, sur la foi d'un signal de vivacité qui ne se produit en réalité jamais.

Correctif retenu : un `last_frame = time.monotonic()`, mis à jour après **chaque** frame émise (donnée ou ping), et un budget d'attente recalculé à chaque itération comme `_HEARTBEAT_SECONDS - (monotonic() - last_frame)` — jamais réinitialisé à la fenêtre complète juste parce qu'un réveil (spurieux ou non) s'est produit. Un réveil qui ne produit aucune frame (le cas courant du `NOTIFY` étranger) laisse `last_frame` inchangé ; le drain périodique reste malgré tout le filet de rattrapage pour un `NOTIFY` réellement perdu sur le scénario suivi.

### Mode `once` — catch-up borné, pas un artifice de test

`?once=true` désactive `LISTEN` et le heartbeat : le flux draine le keyset depuis le curseur jusqu'à épuisement (pagination interne transparente, `LIMIT 500`) puis **ferme la réponse** — un flux fini, pas un flux ouvert indéfiniment. C'est une décision de contrat produit, pas un raccourci ajouté pour faire passer des tests : un watcher cron qui veut un « rattrape tout depuis mon dernier curseur » périodique n'a aucune raison de tenir une connexion ouverte entre deux exécutions — c'est exactement le mode de consommation d'un `once=true` borné, sans jamais entrer dans la boucle d'attente ouverte.

Ce mode a une deuxième conséquence, révélée par la revue adversariale : sans lui, l'endpoint n'est **testable dans aucun harnais synchrone**. Un `TestClient` Starlette bufferise la **totalité** de la réponse avant de rendre la main à l'appelant (l'appel du portail bloque jusqu'au retour complet de l'application ; l'événement `http.disconnect` n'arrive jamais dans ce chemin) — un flux `once=false` sans fin ne se termine donc jamais sous `TestClient`, et `__enter__` ne rend jamais la main. `once=true` n'est pas une concession à la testabilité au détriment du contrat produit : c'est le contrat produit (catch-up borné pour consommateur one-shot) qui *se trouve* aussi résoudre le problème de testabilité, dans cet ordre de priorité.

### Sémantique at-least-once, idempotence côté consommateur

La livraison est **at-least-once**. Un client qui reconnecte reprend depuis son dernier `id:` (via `?cursor=` ou le header `Last-Event-ID`) ; sur la couture il peut re-recevoir une frame déjà vue. **L'exactly-once est impossible en SSE** (le serveur ne sait pas ce que le client a effectivement traité avant la coupure) et est assumé. L'idempotence est le travail du consommateur, et elle est bon marché ici : les UUID des recommandations sont déterministes (UUID5, cf. ADR-026 pour le reschedule, #340 pour les watchers), donc re-traiter un event ne crée pas de doublon métier.

## Caveats gravés (contrat, ne pas « corriger »)

- **`stream_seq` est monotone mais PAS gap-free.** Une séquence IDENTITY avance à chaque *tentative* d'INSERT : une transaction rollbackée **brûle** sa valeur et laisse un trou (…, 41, 43, …). C'est correct et voulu. Les consommateurs traitent `stream_seq` comme un high-water mark opaque comparé avec `>` **uniquement** — jamais comme un compte, jamais comme `last + 1`, jamais gap-checké. « J'ai jusqu'à N » signifie « donne-moi `stream_seq > N` », rien sur le nombre de lignes.
- **Le backfill IDENTITY des lignes historiques ne suit pas `created_at`.** Ajouter la colonne IDENTITY réécrit la table et assigne `stream_seq` aux lignes préexistantes dans l'ordre physique/heap que PG16 parcourt pendant la réécriture — *pas* garanti aligné sur `created_at`. Seuls les events insérés *après* la migration ont un `stream_seq` aligné sur leur ordre d'insertion. Pour le contrat stream/replay c'est un non-problème (un nouvel abonné démarre au high-water mark courant et n'avance que) ; ça signifie seulement qu'un `ORDER BY stream_seq` sur des lignes historiques n'est **pas** un tri chronologique.

## Alternatives rejetées

- **WebSocket.** Rejeté : bidirectionnel pour un flux qui n'a aucun trafic montant ; négociation d'upgrade et auth pré-upgrade plus complexes ; dépendance/surface serveur en plus. SSE + `StreamingResponse` natif couvre le besoin à coût zéro.
- **Table `outbox` dédiée.** Rejeté : `events` est déjà le point de convergence des changements (migrations 002/006/051). Une outbox parallèle imposerait de toucher N sites d'écriture pour un double-write et introduirait un risque de désync entre les deux tables. La colonne `stream_seq` sur `events` obtient le même résultat sans nouveau site d'écriture.
- **`created_at` comme curseur.** Rejeté : collisionne à la microseconde sous inserts groupés → un resume `> last_seen_ts` sauterait ou dupliquerait des lignes. `stream_seq` (IDENTITY) est strictement croissant par insert.
- **Flux monté sur le pool de connexions synchrone.** Rejeté : le threadpool des handlers `def` est borné à la taille du pool DB (breaking point #6, `SCALABILITY.md`) ; N flux long-vécus épingleraient tout le pool → DoS auto-infligé. D'où la connexion async dédiée hors pool + le budget `OOTILS_STREAM_MAX_CONN`.
- **Exactly-once.** Rejeté comme impossible en SSE : le serveur ne peut pas savoir ce que le client a traité avant une coupure. At-least-once + idempotence consommateur (UUID déterministes) est la garantie retenue.

## Hors périmètre V1

- **Stream des UPDATE `processed`** (le flag de bookkeeping de `events`) : c'est un cycle de vie de traitement interne, pas un delta métier que la flotte consomme. `_EVENT_COLUMNS` exclut délibérément `processed`/`processed_at`.
- **WebSocket / canal bidirectionnel.**
- **Back-pressure au-delà de ~1M events** : à l'échelle démo actuelle (cf. `SCALABILITY.md`), un drain paginé (`LIMIT 500` par page) suffit. Une stratégie de back-pressure/compaction relève d'un ADR ultérieur.
- **Stream des nodes/edges bruts** : le flux transporte des *events* (deltas typés), pas l'état de graphe. Un agent qui veut l'état lit `GET /v1/nodes`/`/v1/graph` avec `scenario_id`.

## Conséquences

- **Positif :** le principe Streamable a enfin une surface concrète et exécutable ; un agent s'abonne via `GET /v1/stream?cursor=<seq>` au lieu de poller ; la reprise après coupure est native (`Last-Event-ID`) ; zéro dépendance ajoutée ; aucun nouveau site d'écriture (`events` reste l'unique outbox) ; le pool sync est protégé par construction (connexion async dédiée + budget + kill-switch) ; le mode `once` couvre en plus le cas d'un consommateur cron borné sans connexion tenue.
- **Négatif / dette assumée en V1 :**
  - Livraison at-least-once seulement — les consommateurs doivent être idempotents (ils le sont déjà via UUID déterministes).
  - `stream_seq` n'est pas gap-free et l'ordre historique n'est pas chronologique — deux caveats gravés ci-dessus, à ne jamais « corriger ».
  - Pas de back-pressure au-delà de l'échelle démo.
  - **Tous les changements d'état n'émettent pas encore un event typé.** À date, la propagation et les transitions de recommandation émettent ; plusieurs signaux de la flotte (`calc_run_finished`, `shortage_detected`) ne sont pas encore matérialisés en events — donc un abonné ne les voit pas. C'est précisément l'objet de la PR2 (cf. §Suite). Tant qu'un site de changement n'émet pas dans `events`, il est invisible au stream : c'est le nouveau critère « agent-ready » que `CLAUDE.md` rend désormais exécutable.
  - **La première implémentation de PR1 avait 7 défauts de mécanisme**, confirmés par une revue adversariale empirique avant tout merge sur `main` : une fuite de slot de budget sur déconnexion précoce (majeur), un abandon de générateur `notifies()` verrou tenu (mineur), une famine de heartbeat sous NOTIFY étrangers (majeur), et l'absence d'un mode borné testable (bloquant pour la vague de tests) — tous corrigés dans ce même code-complet avant merge, cf. les sous-sections dédiées ci-dessus. Consigné ici pour la traçabilité, pas comme dette restante.
- **Reste à faire :** cocher l'item ROADMAP correspondant une fois PR1+PR2 mergées sur `main` avec leurs tests ; PR2 (§Suite).

## Suite — PR2 (multiplexage flotte)

PR1 livre le substrat et l'endpoint. PR2 fera du stream le canal de la flotte entière :

- **Émettre les events manquants** là où un changement d'état ne produit pas encore de ligne `events` : notamment `calc_run_finished` (fin de propagation) et `shortage_detected` (détection de pénurie). Aujourd'hui ces signaux existent en base mais pas comme events streamables — un abonné ne peut pas réagir en push à « le calcul est fini » ou « une pénurie est apparue ».
- **Un test de garde** asserant que tout site de changement d'état émet bien un event typé (le pendant exécutable de l'anti-pattern `CLAUDE.md` « a new feature without StreamChanges emission »).
- **Watchers `--subscribe` opt-in** : les watchers de la flotte (#340/#346/#347) pourront consommer `/v1/stream` en push au lieu de leur boucle de polling actuelle. Note : l'ADR-026 §Conséquences relevait « pas d'émission StreamChanges à la création d'un DRAFT reschedule » comme cohérent avec le reste de la flotte — PR2 est le chantier qui referme cette dette transversalement, pas seulement pour le reschedule.

## Références

- `src/ootils_core/api/routers/stream.py` — l'endpoint `GET /v1/stream` : kill-switch/budget (`_SlotLease`, `_acquire_lease`), résolution du curseur, boucle async drain→wait avec cadence monotone (`_wait_for_wakeup`) et mode `once`, helpers purs (`_resolve_cursor`, `_parse_types`, `_envelope`, `_sse_frame`, `_heartbeat_frame`).
- `src/ootils_core/db/migrations/063_events_stream_seq.sql` — colonne `events.stream_seq` (IDENTITY), index `idx_events_stream_seq (scenario_id, stream_seq)`, fonction `ootils_notify_event()` + trigger `events_stream_notify`.
- `src/ootils_core/db/migrations/051_recommendation_transition_event.sql` — le précédent qui établit `events` comme outbox de la flotte (« subscribe … instead of polling »).
- `docs/SPEC-INTERFACES.md` §5.2 — le contrat REST détaillé de `GET /v1/stream` (params, précédence du curseur, format des frames, codes 503).
- `docs/SCALABILITY.md` — breaking point #6 (threadpool borné au pool DB) qui motive la connexion async dédiée hors pool.
- `docs/ADR-025-scenario-param-overlay.md` — origine du pattern kill-switch (`OOTILS_*_ENABLED` → 503 avant tout accès DB) réutilisé ici.
- `docs/ADR-026-reschedule-fpo.md` — §Conséquences note l'absence d'émission StreamChanges à la création d'un DRAFT, dette refermée par la PR2 de #391.
