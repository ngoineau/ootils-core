# ADR-026 — Messages de reprogrammation des ordres ouverts + Firm Planned Orders

**Statut :** Accepté — chantier #346 **CLOS**, 3 PRs mergées : PR-A (signaux de reschedule dans le core), PR-B (watcher gouverné + migration 061), PR-C (purge/netting FPO + endpoint firm/défirm + migration 062). Le geste APICS n°1 (re-dater/annuler un ordre existant) et la stabilité de plan (Firm Planned Orders) sont en place.
**Date :** 2026-07-04
**Contexte mesuré :** REVIEW-2026-07-APS A8/A9 (`docs/REVIEW-2026-07-APS.md`), item C2.2.

---

## Contexte

La revue REVIEW-2026-07-APS relevait deux trous jumeaux dans le moteur MRP :

- **A8 — aucune reprogrammation.** `_classify_shortage` (moteur APICS, `graph_integration.py`) n'a jamais émis DEFER/CANCEL malgré leur mention en docstring, et aucun chemin ne comparait la date d'un receipt planifié existant à sa date de besoin réelle. Résultat : le geste le plus élémentaire d'un planificateur — « cet ordre est mal daté, glisse-le » — était structurellement impossible. Le moteur ne savait que créer de nouveaux ordres, jamais corriger ceux qui existaient déjà.
- **A9 — aucun Firm Planned Order.** `grep is_firm|firm_planned` ne retournait aucune ligne. Chaque régénération MRP réécrivait l'intégralité du plan planifié sans qu'aucun ordre ne puisse être « gelé » par un planificateur ou un agent gouverné — nervosité maximale, aucune stabilité inter-run.

Les deux trous sont couplés : sans FPO, rien ne distingue un ordre qu'on peut laisser la purge de régénération effacer d'un ordre qu'un humain (ou une reco approuvée) a déjà engagé ; sans reschedule, un FPO figé deviendrait vite obsolète (la seule façon de le corriger serait de le déf-firmer puis laisser la régénération le recréer — perdant l'intérêt même du FPO). #346 livre les deux ensemble.

## Décision

### Comparaison canonique receipt-vs-besoin : un seul endroit, le math core

La comparaison vit exclusivement dans `reschedule_signals` (`src/ootils_core/engine/mrp/core.py`), DB-free, golden-masterisée, consommée par `agent_reschedule_watcher.py`. Aucun autre chemin (APICS `graph_integration.py`, routers) ne réimplémente cette comparaison.

- **Date de besoin** = projection demande vs on-hand vs **safety stock** (jamais lead time — le lead time sert à dater une *nouvelle* commande, pas à juger si une commande *existante* est bien placée), à la granularité bucket hebdomadaire (`_need_bucket_for_receipts`).
- **Need bucket d'un receipt = centre de gravité**, pas première touche : l'allocation FIFO cumulée sur ce receipt doit franchir 50 % de sa quantité pour fixer son bucket de besoin. Un receipt de 200 qui couvre 10 unités en semaine 1 et 190 en semaine 10 a son besoin en semaine 10, pas en semaine 1 — proposer de tirer les 200 unités à la semaine 1 serait un sur-tirage massif pour 190 d'entre elles. C'est le choix qui évite qu'un moteur naïf ne recommande de rapprocher un ordre bien plus qu'il n'est nécessaire.
- Un receipt dont la quantité n'est jamais consommée sur l'horizon → `need_bucket = None` → candidat CANCEL, sauf garde de bord d'horizon (dernier bucket chargé jamais annulé — la demande qui le justifierait peut être juste hors fenêtre).
- **Dampening** (anti-nervosité) : `reschedule_min_days` (défaut 3 j) et `reschedule_qty_tolerance_pct` (défaut 5 %, réservé au futur split partiel de receipt — no-op en V1 sur CANCEL, cf. commentaire du code) sur `item_planning_params` (migration 061c). Baseline-only V1 : volontairement **non ajoutés** à la whitelist `scenario_planning_overrides` (#347) — ce qui bouge réellement la date de besoin (lead times, safety stock) est déjà forkable via l'overlay ADR-025 ; les seuils de dampening eux-mêmes restent un réglage global.
- **L'invariant central** : un plan stable (rien n'a changé depuis le dernier run) émet **zéro signal**. Un receipt déjà dans son bucket de besoin, ou dont l'écart proposé est sous le seuil de dampening, ne produit aucun message. C'est la propriété testée par le golden-master et ce qui rend le watcher rejouable sans bruit.

### Canal gouverné : `recommendations`, jamais `mrp_action_messages`

Les messages de reprogrammation naissent comme recommandations `DRAFT` dans la table `recommendations` (la machine d'états gouvernée #341), avec les trois colonnes typées ajoutées par la migration 061 (`target_node_id`, `current_receipt_date`, `proposed_date`) — jamais dans `mrp_action_messages`.

**Refus argumenté :** la revue APS affirmait à tort que les deux tables ne faisaient qu'une. Ce sont deux tables disjointes avec des propriétaires et des garanties différents : `recommendations` porte la machine d'états DRAFT→APPROVED→APPLIED, l'audit (`agent_runs`), et le gate humain obligatoire sur les niveaux L3+ (#341) ; `mrp_action_messages` n'a aucune de ces garanties. Router des actions L2/L3 (re-datation, annulation d'un engagement) dans la table non-gouvernée violerait directement l'anti-pattern North Star « une écriture qui contourne l'approbation » — exactement le type d'erreur que l'ADR-021 a déjà fermé côté pénuries en interdisant aux watchers d'écrire dans `shortages`.

**Idempotence** : `recommendation_id` est un UUID5 déterministe sur `(scenario_id, target_node_id, action, proposed_date)` (`engine/recommendation/reschedule.py:reschedule_recommendation_id`), upserté avec `ON CONFLICT (recommendation_id) DO NOTHING`. Un re-run sur un plan inchangé re-dérive les mêmes ids → 0 nouvelle ligne. C'est plus fort que le patron supersede-puis-réinsertion des autres watchers (#340) qui mintent un UUID neuf à chaque run : un signal de reschedule est un fait stable (« cet ordre est mal daté »), pas une proposition re-coûtée à chaque passage. Les DRAFTs antérieurs de l'agent dont le signal a disparu (le plan a changé, le mésdatage est résolu) sont marqués `EXPIRED`, jamais supprimés.

Contrairement aux watchers scenario-backed (#340), **aucun fork contre-factuel** n'est créé ici : le signal est un fait déterministe dérivé du plan chargé (« cet ordre est mal daté vs son besoin calculé ») — le signal est sa propre preuve, simuler ajouterait du bruit, pas une preuve supplémentaire.

### Decision Ladder : L2 pour re-dater, L3 pour annuler

Via `agent_governance.py:decision_level(action)` — jamais un niveau codé en dur dans le watcher ou le mapper :

| Action | Niveau | Raison |
|---|---|---|
| `RESCHEDULE_IN` | L2 | Re-dater un engagement existant, réversible (comme EXPEDITE) |
| `RESCHEDULE_OUT` | L2 | Idem, sens inverse |
| `DEFER` | L2 | Même famille (présent dans la table de mapping et le CHECK vocabulaire de la migration 061 pour un usage manuel/agent futur ; **le core `reschedule_signals` n'émet jamais DEFER** — réservé, pas produit en V1) |
| `CANCEL` | **L3** | Irréversible côté fournisseur (relâcher un engagement n'est pas annulable ; une re-commande ultérieure est un nouvel engagement, pas un undo). **Premier DRAFT L3 émis par la flotte de watchers.** |

Le passage à L3 ne contourne pas la gouvernance : le watcher n'écrit jamais que `status='DRAFT'` ; c'est la machine d'états (#341, `HUMAN_ONLY_TARGETS`) qui interdit à tout acteur non-humain d'atteindre `APPROVED`/`APPLIED`. Un watcher qui émet un DRAFT L3 est donc sûr par construction : il propose, l'humain dispose.

### Firm Planned Orders : exclu de la purge ET netté des deux côtés

`nodes.is_firm` (migration 061a, index partiel `WHERE is_firm` — les FPO restent une petite minorité de `PlannedSupply`). Un FPO est :

1. **Exclu de la purge de régénération MRP** (`cleanup_previous_run(run_id=None)` côté moteur APICS) : la régénération complète du plan pour un scénario ne doit pas effacer un ordre que l'humain ou une recommandation approuvée a explicitement gelé.
2. **Netté comme supply engagé, pas re-plannable, dans les deux moteurs** : math core (`sched_b`/`sched_orders`, déjà porteur du champ `is_firm` par receipt depuis PR-A) et moteur APICS (`gross_to_net`). Le couplage est délibéré — exclure de la purge sans netter des deux côtés recréerait la double-planification (le même besoin déjà couvert par le FPO générerait un second ordre), le même risque de dérive silencieuse que le double moteur MRP avant l'ADR-020.
3. **Reste re-datable par un message reschedule** — c'est précisément l'intérêt APICS d'un FPO : le planificateur (ou une reco gouvernée approuvée) en possède la date, MRP ne le régénère plus, mais un `RESCHEDULE_IN`/`RESCHEDULE_OUT` peut encore le déplacer. Un FPO n'est donc pas gelé au sens absolu — il est soustrait au cycle de régénération automatique, pas à la gouvernance humaine.
4. **Endpoint firm/défirm audité** — poser ou lever `is_firm` sur un ordre planifié est une action de gouvernance à part entière, tracée comme les autres écritures gouvernées du North Star (pas un simple toggle silencieux).

## Alternatives rejetées

- **Naître dans `mrp_action_messages`.** Rejeté : canal non gouverné, aucune machine d'états, aucun gate humain sur L3 — violerait directement l'anti-pattern North Star sur les écritures qui contournent l'approbation.
- **Date de besoin = première touche du receipt (au lieu du centre de gravité).** Rejeté : sur-tirage systématique dès qu'un receipt couvre plusieurs besoins distincts dans le temps — une recommandation qui rapproche un ordre bien plus que nécessaire n'est pas une recommandation utile, c'est du bruit coûteux pour le planificateur.
- **Exclure les FPO de la purge sans les netter comme supply engagé.** Rejeté : recréerait la double-planification (le besoin déjà couvert par le FPO regénérerait un second ordre pour le même besoin) — le même type de bug de fond que le ×48 de l'ADR-020.
- **Forkabiliser les seuils de dampening dès la V1.** Différé, pas rejeté sur le principe : ce qui bouge réellement la date de besoin à l'intérieur d'un fork (lead times, safety stock) est déjà forkable via l'overlay #347/ADR-025 — c'est la substance du what-if. Les deux seuils de dampening eux-mêmes restent un réglage global baseline-only pour cette V1.

## Conséquences

- **Positif :** le geste n°1 d'un planificateur (re-dater ou annuler un ordre existant) existe enfin, sous forme de recommandation gouvernée et explicable ; un plan stable ne génère plus de bruit (l'invariant zéro-signal) ; les FPO stabilisent le plan inter-run sans sacrifier la capacité à corriger leur date.
- **Négatif / dette assumée en V1 :**
  - `CANCEL` est tout-ou-rien — pas de split partiel d'un receipt partiellement surplus (le seuil `reschedule_qty_tolerance_pct` est réservé à ce cas futur, no-op aujourd'hui).
  - `DEFER` existe dans le vocabulaire CHECK et le mapping de decision level, mais n'est émis par aucun chemin automatique du core — réservé à un usage manuel/agent futur.
  - Dampening baseline-only (pas dans la whitelist #347).
  - Pas d'émission `StreamChanges` à la création d'un DRAFT reschedule — cohérent avec le reste de la flotte de watchers (#340, #347), pas une régression propre à #346.
- **Reste à faire :** PR-C (purge FPO-aware + netting côté APICS + endpoint firm/défirm) est en cours d'écriture au moment de cet ADR ; cette section sera à confirmer contre le code mergé (fichiers, tests) avant de clore #346 dans le ROADMAP.

## Références

- `docs/REVIEW-2026-07-APS.md` — items A8, A9, C2.2.
- `docs/ADR-021-shortage-truth.md` — le précédent qui interdit aux watchers d'écrire dans une table de faits non-gouvernée ; #346 applique le même principe au canal `recommendations` vs `mrp_action_messages`.
- `docs/ADR-025-scenario-param-overlay.md` — pourquoi les seuils de dampening restent baseline-only alors que lead time/safety stock sont déjà forkables.
- `src/ootils_core/engine/mrp/core.py` — `reschedule_signals`, `_need_bucket_for_receipts`, `RescheduleSignal`, `ReceiptOrder`.
- `src/ootils_core/engine/recommendation/reschedule.py` — `build_recommendation`, `reschedule_recommendation_id` (UUID5 déterministe).
- `scripts/agent_reschedule_watcher.py` — orchestrateur : charge le plan, calcule les signaux, upsert idempotent, expire les DRAFTs obsolètes.
- `scripts/agent_governance.py` — `_ACTION_DECISION_LEVELS`, `decision_level()`.
- `src/ootils_core/db/migrations/061_reschedule_and_fpo.sql` — `nodes.is_firm`, colonnes `recommendations` (`target_node_id`/`current_receipt_date`/`proposed_date`), `reschedule_min_days`/`reschedule_qty_tolerance_pct`.
- `src/ootils_core/engine/mrp/graph_integration.py:cleanup_previous_run` — purge de régénération ; docstring actuelle anticipe déjà l'exclusion FPO (PR-C).
