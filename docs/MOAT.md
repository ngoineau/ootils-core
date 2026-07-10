# MOAT.md — les 4 propriétés non-copiables, prouvées par le code

> **Statut** : preuve de démontrabilité (MOAT-1) — 2026-07-10.
> **Contrat** : [`ROADMAP-AGENTS-2026-H2.md`](ROADMAP-AGENTS-2026-H2.md) §5, MOAT-1
> (« vrai dans le code mais invendable »).
> **Compagnon** : [`DEMO-RUNBOOK.md`](DEMO-RUNBOOK.md) §« Le moat en 5 minutes ».
> **Base vérifiée** : worktree `feat/moat1-demonstrable`, origin/main `2efb8c0`.
> Chaque `fichier:ligne` cité ci-dessous a été **lu**, pas grep-et-supposé — un
> ancrage sans lecture directe du test est un NO-GO en revue (voir §5).

Ootils est **open-core** (cf. `PITCH-investors.md` §7) : un concurrent peut lire
le moteur. Le fossé n'est donc pas le secret du code — c'est la **discipline
opérationnelle** : quatre propriétés tenues *simultanément et sans exception*
sur toute la surface produit, chacune gardée par un test qui casse (`FAIL`
rouge en CI) si la discipline est rompue ne serait-ce que sur un seul chemin.
Retrofiter ça sur un APS existant — où l'identité vient du payload, où un
fork est un export CSV, où l'audit est un log applicatif optionnel — n'est
pas une feature d'un trimestre : c'est une réécriture d'architecture.

---

## 1. Déterminisme rejouable — même input → mêmes IDs, mêmes résultats

**Pourquoi un concurrent ne peut pas le copier en un trimestre.** La plupart
des moteurs de planification (et tous les bolt-on IA génératifs) mintent un
nouvel identifiant à chaque exécution — rejouer un plan inchangé produit un
nouveau lot de lignes, jamais le même. Chez Ootils, l'identité d'une
recommandation, d'une pénurie ou d'un run est une **fonction pure** de son
contenu métier (scénario, nœud cible, action, date) via `uuid5` — jamais un
timestamp ni un compteur. Rejouer un plan inchangé ne crée rien : `INSERT ...
ON CONFLICT DO NOTHING` collabore avec l'ID déterministe pour produire un
diff vide. C'est ce qui rend un agent *rejouable en confiance* — condition
sine qua non pour qu'une flotte d'agents tourne en continu sans dupliquer
son propre travail à chaque cycle.

**Le test (primaire — replay de bout en bout, DB réelle) :**
`tests/integration/test_transfer_watcher_integration.py:277`
`test_rerun_on_unchanged_plan_inserts_zero_new_rows` — fait tourner le
watcher qui alimente `POST /v1/drp/run` (Step 4 du runbook) **deux fois** sur
le même plan et assert `drafts2 == drafts1` (le même *set* de
`recommendation_id`, byte pour byte) + `recommendations_inserted == 0` au
second passage.

```bash
DATABASE_URL=postgresql:///ootils_test python -m pytest \
  tests/integration/test_transfer_watcher_integration.py::test_rerun_on_unchanged_plan_inserts_zero_new_rows -q
```

**Renforcé par** (même discipline, deux autres couches du moteur) :
- `tests/test_m4_shortage.py:153` (`TestDetectShortage`)
  `test_shortage_id_is_deterministic` — la formule pure :
  `shortage_id == deterministic_uuid("shortage", scenario_id, calc_run_id, node_id)`.
  ```bash
  python -m pytest tests/test_m4_shortage.py::TestDetectShortage::test_shortage_id_is_deterministic -q
  ```
- `tests/integration/test_m4_shortage_integration.py:225` (`TestPersist`)
  `test_persist_is_idempotent_on_conflict` — deux `detect()`+`persist()` sur le
  **même** `calc_run_id` collapsent sur une seule ligne DB.
  ```bash
  DATABASE_URL=postgresql:///ootils_test python -m pytest \
    tests/integration/test_m4_shortage_integration.py::TestPersist::test_persist_is_idempotent_on_conflict -q
  ```
- `tests/integration/test_pyramide_reconcile_integration.py:229`
  (`TestHierarchicalRunPersistence`) `test_two_runs_are_deterministic` — fait
  tourner `HierarchicalRunner` deux fois et compare les valeurs persistées par
  clé (nuance honnête : ce test prouve des **résultats** identiques, pas des
  **IDs** de ligne identiques — le `forecast_id` diffère entre les deux runs,
  seul le contenu ne diffère pas).

---

## 2. `actor_kind` non-usurpable — un agent qui ment sur son identité → 403

**Pourquoi un concurrent ne peut pas le copier en un trimestre.** Tout bolt-on
IA sur un APS existant lit l'identité de l'appelant dans le **payload** de la
requête (`{"actor_kind": "human"}`) parce que c'est le seul endroit où elle
existe. Chez Ootils, `actor_kind` est une propriété du **credential**
(`api_tokens`, migration 064), fixée à l'émission, jamais du corps de la
requête — la porte humaine L3 (#392, ADR-029) lit le token, ignore ce que le
corps prétend. Retrofiter ça exige de sortir l'identité du payload sur
*chaque* endpoint d'écriture, pas seulement celui qu'on démo — sinon la porte
a un trou.

**Le test :**
`tests/integration/test_agent_floor_integration.py:369`
(`TestAgentCannotApproveL3`)
`test_human_gate_blocks_agent_even_with_approve_scope_and_lying_body` — un
token dont le credential porte `actor_kind='agent'` (et qui possède même le
scope `recommend:approve`, donc franchit l'étage des scopes) envoie un corps
qui ment `"actor_kind": "human"` sur une transition `REVIEWED → APPROVED`
(L3). Réponse : **403**, message côté « human gate » (pas côté scope), et la
recommandation reste `REVIEWED` en base — le mensonge n'a rien changé.

```bash
DATABASE_URL=postgresql:///ootils_test python -m pytest \
  tests/integration/test_agent_floor_integration.py::TestAgentCannotApproveL3::test_human_gate_blocks_agent_even_with_approve_scope_and_lying_body -q
```

**Renforcé par** : la même porte L3, sur une action L3 différente (promote
scénario, cf. propriété 3) :
`tests/integration/test_scenario_promote_integration.py:404`
(`TestPromoteGuards`) `test_agent_cannot_promote`.
```bash
DATABASE_URL=postgresql:///ootils_test python -m pytest \
  tests/integration/test_scenario_promote_integration.py::TestPromoteGuards::test_agent_cannot_promote -q
```

Voir aussi `docs/ADR-029-agent-enterprise-floor.md` (le substrat) et
`docs/ADR-032-scope-grid-and-budgets.md` (les scopes qui stackent par-dessus).

---

## 3. Tout-forkable + `promote` avec détection de conflit

**Pourquoi un concurrent ne peut pas le copier en un trimestre.** « Scénario »
dans un APS classique veut souvent dire export/import ou un module dédié
séparé du run nominal. Chez Ootils, un fork est une primitive de plateforme —
n'importe quel appelant (humain ou agent) obtient une copie complète du
graphe en un appel, teste un contre-factuel dessus, et ne peut le
re-fusionner sur baseline (`promote`, L3 humain-only) que si baseline n'a
**pas divergé** depuis la capture du fork — sinon 409 avec la liste typée des
champs en conflit, et **rien** n'est écrit (ni patch, ni archive, ni ligne
d'audit, ni event). C'est la détection de conflit qui manque à un simple
"copier la base" : sans elle, un `promote` tardif écrase silencieusement un
changement de production survenu entre-temps.

**Le test (fork) :**
`tests/integration/test_m6_api_integration.py:382` (`TestPostSimulate`)
`test_simulate_no_overrides_creates_scenario` — `POST /v1/simulate` sur une
DB réelle crée un nouveau scénario deep-copié de baseline (`base_scenario_id`
= baseline, ligne vérifiée dans `scenarios`).
```bash
DATABASE_URL=postgresql:///ootils_test python -m pytest \
  tests/integration/test_m6_api_integration.py::TestPostSimulate::test_simulate_no_overrides_creates_scenario -q
```

**Le test (détection de conflit à la fusion) :**
`tests/integration/test_scenario_promote_integration.py:371`
(`TestPromoteConflict`) `test_diverged_baseline_is_409_with_typed_conflicts` —
baseline diverge après la capture de l'override (`quantity` passe de `100` à
`999`) ; `POST /v1/scenarios/{id}/promote` répond **409** avec un conflit typé
(`node_id`, `field_name`, `expected`, `actual`) et assert explicitement que
rien n'a été écrit (`baseline_quantity()` inchangée, scénario toujours
`active`, aucune ligne `scenario_promotions`, aucun event `scenario_merge`).
```bash
DATABASE_URL=postgresql:///ootils_test python -m pytest \
  tests/integration/test_scenario_promote_integration.py::TestPromoteConflict::test_diverged_baseline_is_409_with_typed_conflicts -q
```

**Renforcé par** : `:308` `test_promote_patches_baseline_audits_and_emits_event`
(le chemin de succès symétrique) et `:263`
`test_re_override_does_not_false_conflict_at_promote` (garde de régression :
un champ ré-overridé deux fois ne doit pas générer un FAUX conflit — sinon la
détection de conflit serait inutilisable en pratique).

---

## 4. Le triplet causalité + audit + events

Trois garanties, jamais isolées ailleurs (un log applicatif OU un audit trail
OU des webhooks — rarement les trois comme des invariants testés).

### 4a. Chaîne causale requêtable (ADR-004)

**Pourquoi un concurrent ne peut pas le copier en un trimestre.** Une
« explication » IA générative n'est pas rejouable ni vérifiable pas à pas.
`ExplanationBuilder` produit un `causal_path` d'étapes numérotées
(demande → offre → pénurie), reconstruit depuis le graphe, et interrogeable
via `GET /v1/explain?node_id=...` — pas un texte généré, une trace du calcul
réel.

**Le test (garantie stricte, niveau noyau, DB-free) :**
`tests/test_m3_explanations.py:143` (`TestBuildPiExplanation`)
`test_causal_path_has_at_least_one_step` — assert `causal_path` contient au
moins un `CausalStep` typé.
```bash
python -m pytest tests/test_m3_explanations.py::TestBuildPiExplanation::test_causal_path_has_at_least_one_step -q
```

**Le test (requêtable via l'API, DB réelle) :**
`tests/integration/test_m6_api_integration.py:320` (`TestGetExplain`)
`test_explain_shortage_node_if_explainable` — appelle réellement
`GET /v1/explain?node_id=...` sur un nœud de pénurie seedé.
**Ancrage honnête — nuance à connaître** : ce test tolère `200` **ou** `404`
comme sorties valides et ne fait d'assertion de forme (`causal_path` liste,
`explanation_id` présent) que dans la branche `200` ; il ne garantit donc pas
qu'un nœud de pénurie *quelconque* soit explicable de bout en bout via l'API
sur toute base — la garantie **stricte** (au moins une étape, étapes
ordonnées) est portée par le test noyau ci-dessus, pas par ce test API.
```bash
DATABASE_URL=postgresql:///ootils_test python -m pytest \
  tests/integration/test_m6_api_integration.py::TestGetExplain::test_explain_shortage_node_if_explainable -q
```

### 4b. `api_request_log` — identité attribuée cryptographiquement, jamais auto-déclarée

**Pourquoi un concurrent ne peut pas le copier en un trimestre.** Chaque appel
authentifié est journalisé avec le `token_id` et l'`actor_kind` **du
credential résolu côté serveur** (migration 064), pas ce que l'appelant
prétend être. **Précision honnête sur le périmètre** : `api_request_log`
(migration 023 + 064) est un journal d'accès HTTP typé — `method`, `path`,
`status_code`, `latency_ms`, `token_id`, `actor_kind` — pas un blob
input/output par écriture. La trace « input/output/policy result » que décrit
le North Star (CLAUDE.md) est assemblée en **joignant** trois tables
typées : `api_request_log` (qui a appelé quoi, quand, avec quel statut),
`recommendation_transitions` (migration 040 — qui a fait quelle transition
d'état, de quel statut à quel statut, pourquoi) et les `events` typés (§4c) —
pas une seule ligne à colonnes JSONB fourre-tout (interdit par la politique
JSONB du projet).

**Le test :**
`tests/integration/test_agent_floor_integration.py:502`
(`TestAuditAttribution`) `test_minted_call_stamps_token_id_and_actor_kind` —
un appel `GET /v1/recommendations` avec un token minté `actor_kind='agent'`
est relu depuis `api_request_log` avec le **même** `token_id` et
`actor_kind='agent'` — l'attribution vient de la résolution serveur, pas du
client.
```bash
DATABASE_URL=postgresql:///ootils_test python -m pytest \
  tests/integration/test_agent_floor_integration.py::TestAuditAttribution::test_minted_call_stamps_token_id_and_actor_kind -q
```

### 4c. Garde-fou « un write gouverné sans event = rouge » (ADR-027)

**Pourquoi un concurrent ne peut pas le copier en un trimestre.** Une flotte
d'agents qui *poll* est une flotte qui rate des changements ou qui surcharge
la base. `GET /v1/stream` (SSE, curseur `stream_seq`) exige qu'**aucune**
écriture gouvernée ne soit invisible au flux — la garantie n'est pas
« documentée », elle est un test paramétré qui **échoue** si une seule des 5
capacités de la flotte (`calc_run_finished`, `shortage_detected`,
`recommendation_created`, `snapshot_captured`, `outcome_evaluated`) émet zéro
ou plus d'un event run-level.

**Le test :**
`tests/integration/test_fleet_events_integration.py:536`
`test_every_governed_write_emits_exactly_one_run_level_stream_event` —
paramétré sur les 5 capacités ; pour chacune, assert **exactement un** event,
`new_quantity` = le compte run-level (jamais un event par item — ADR-027), et
que le SELECT keyset de `/v1/stream` (`stream_seq > cursor`) le surface,
strictement ordonné.
```bash
DATABASE_URL=postgresql:///ootils_test python -m pytest \
  tests/integration/test_fleet_events_integration.py::test_every_governed_write_emits_exactly_one_run_level_stream_event -q
```

**Renforcé par** : `tests/integration/test_fleet_events_integration.py:660`
`test_recommendation_created_idempotent_rerun_emits_nothing` — le même rejeu
« plan inchangé » de la propriété 1 (§1) ne se contente pas de ne rien
insérer : il n'émet **aucun nouvel event** non plus — la stabilité des IDs et
la stabilité du flux sont la même discipline, testée au même endroit.

---

## 5. Ce que ce document N'affirme PAS (vérité du pitch)

- **« Covariate-informed » est interdit.** Le harness de segmentation par
  programme Buy (ADR-035, DEM-2 PR1, #444) est un **harness de preuve
  autonome, en lecture seule, non branché sur aucun run servi** —
  `pyramide/segmentation.py` + `scripts/prove_segmentation_fva.py`. Le
  calendrier/jours fériés en feature LGBM et les covariates Chronos-2
  connues-futures (phases 2/3 de DEM-2, cf. `ROADMAP-AGENTS-2026-H2.md` §5)
  **ne sont pas livrés**. Aucune phrase de pitch ne doit dire qu'Ootils
  prévoit *avec* des covariates aujourd'hui.
- **« Replay » ne se revendique que pour ce qui est prouvé ci-dessus** :
  rejeu d'un plan de recommandations (§1), et rejeu du flux d'events depuis
  un curseur (`GET /v1/stream?cursor=N`, testé par
  `tests/integration/test_stream_integration.py:290`
  `test_stream_cursor_replay_from_zero_and_resume`, démontré Step 9 du
  runbook). Ne pas étendre ce mot à un rejeu de calc_run à l'identique côté
  propagation — aucun test ne le garantit à ce périmètre (le `calc_run_id`
  change à chaque exécution ; c'est la couche recommandation/pénurie,
  paramétrée par le contenu métier et non par le run, qui est rejouable —
  voir la nuance en §1).
- **Aucune ligne de ce document ne prétend que le moteur est fermé/secret.**
  Ootils est open-core (`PITCH-investors.md` §7) ; le fossé est la discipline
  testée ci-dessus, pas l'opacité du code.
