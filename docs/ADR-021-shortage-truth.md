# ADR-021 — Vérité de pénurie unique : maths mrp_core, système table `shortages`

**Statut :** Accepté — chantier #343 (branche `feat/aps-c1-shortage-truth`) ; valorisation $ livrée au #342.
**Date :** 2026-07-02
**Contexte mesuré :** REVIEW-2026-07-APS (deux vérités de pénurie divergentes), garde-fou de parité MRP #332, seed démo `scripts/seed_demo_data.py`.

---

## Contexte

Ootils répond à « cet item est-il en pénurie ? » à deux endroits, nés de deux lignées différentes :

| | A — kernel (table `shortages`) | B — watchers (`engine/mrp/core.py`) |
|---|---|---|
| Producteur | propagation (`ShortageDetector` Python / `SHORTAGES_SQL`) sur les buckets ProjectedInventory | `load_planning_data` → `consume_demand` → `first_shortage` (read-only, en mémoire) |
| Sémantique | **chaque** bucket en dépassement, par (item, location), classes `stockout` ET `below_safety_stock` | **premier** bucket hebdo sous safety stock, par item poolé, demande **consommée** (`max(orders, forecast)` + demand time fence + proration) |
| Demande vue | somme des nœuds order + forecast câblés par arêtes `consumes` (brute) | consommation de prévision correcte (ADR-020, la maths canonique MRP) |
| Persistance | UUID déterministes, sévérité, chaîne causale ADR-004, cycle de vie `active`/`resolved`, API `/v1/issues` | aucune — recommandations gouvernées L1 DRAFT à côté (`agent_shortage_watcher.py`) |

Avant le #342, les deux vérités ne partageaient même pas la valorisation : le kernel priorisait avec un coût proxy = 1 quand les watchers valorisaient en $ réels via `mrp_core.cost_of`. Deux vérités sans contrat entre elles = le même risque de dérive silencieuse que les deux moteurs MRP avant l'ADR-020 (×48 non détecté).

## Décision

Il n'y a **pas** un moteur gagnant et un moteur perdant : chacun est canonique sur son axe, et un contrat testé en CI les tient ensemble.

1. **La MATHS canonique de pénurie est `mrp_core`** (`src/ootils_core/engine/mrp/core.py`, ADR-020) : c'est la seule implémentation qui consomme la prévision correctement (`max(orders, forecast)`, demand time fence, proration, dédup multi-location). Toute nouvelle logique de projection de pénurie (sémantique de demande, seuils, consommation) se décide là et se propage vers le kernel, jamais l'inverse.
2. **Le SYSTÈME canonique de persistance/requête est la table `shortages`** : UUID déterministes (`deterministic_uuid`), `severity_score` valorisé en $ (#342), chaîne causale explicable (ADR-004), cycle de vie (`resolve_stale`), et l'API `/v1/issues`. Tout consommateur qui veut *lire* des pénuries persistées lit là — agents inclus.
3. **Les watchers n'écrivent JAMAIS dans `shortages`.** Read-only by design, et c'est un refus argumenté :
   - la table appartient exclusivement au `ShortageDetector` (invariant documenté dans `detector.py`) ; un deuxième écrivain casserait les UUID déterministes, le lien `calc_run_id`/`pi_node_id` et le `resolve_stale` ;
   - une ligne `shortages` sans PI node ni chaîne causale ADR-004 serait une recommandation sans preuve — rejetée par la gouvernance (North Star : explicable ou refusé) ;
   - les watchers ont déjà leur canal d'écriture gouverné : recommandations L1 DRAFT auditées (`agent_runs`), jamais des faits kernel.
4. **`severity_score` = valeur $** via la précédence `mrp_core.cost_of` (unit_cost fournisseur négocié, puis `items.standard_cost`, puis proxy 1 pour les items non pricés) — livré au #342, tenu en phase dans les trois implémentations (Python `propagator.py`, SQL `SHORTAGES_SQL`, Rust qui réutilise le SQL).
5. **Garde-fou permanent en CI :** `tests/integration/test_shortage_truth_consistency_integration.py`. Sur le seed démo, après propagation complète baseline : **items(B) ⊆ items(A)** — tout item en pénurie pour les watchers doit exister dans `shortages`. Inclusion (pas égalité) parce que A est structurellement plus large (tous les buckets + `below_safety_stock` + demande brute sommée ≥ demande consommée) tandis que B ne voit que le premier passage sous safety des items à demande indépendante. Toute divergence = échec bruyant avec le diff des items ; ensemble B vide sur le seed = échec aussi (pas de faux-vert).

## Alternatives rejetées

- **Watchers écrivains dans `shortages`.** Rejeté : double-écrivain sur une table à propriétaire unique, sémantiques incompatibles (bucket hebdo item-poolé vs PI daily par location — quel `pi_node_id` ?), pas de chaîne causale, et le cycle de vie `resolve_stale` supprimerait ou ressusciterait les lignes de l'autre écrivain au gré des calc runs.
- **Fusionner les deux moteurs en un seul.** Rejeté (même arbitrage que l'ADR-020, PAS 4) : le kernel de propagation est incrémental, événementiel et par-location — le remplacer par la projection en mémoire de `mrp_core` détruirait l'incrémentalité (ADR-003) ; inversement porter la consommation de prévision dans le kernel est le vrai chemin cible, mais il passe par le PAS 4 de l'ADR-020 (B surcouche du cœur), pas par un big-bang. D'ici là, le test de cohérence tient le contrat.
- **Ne rien faire (deux vérités indépendantes).** Rejeté : c'est exactement la configuration qui a laissé le ×48 MRP grandir en silence (ADR-020) — et la tour de contrôle pénurie est le wedge V1, pas un module secondaire.

## Conséquences

- **Positif :** le front (watchers, tri par valeur $) et le back (kernel, `/v1/issues`) classent les mêmes pénuries avec la même valorisation ; une régression de câblage (nœud de demande sans arête `consumes`) casse la CI au lieu de mentir au planner.
- **Contrainte assumée :** l'inclusion est plus faible que l'égalité — un item vu par A seulement (pénurie par-location poolée par B, item sans demande indépendante) ne fait pas échouer le test. Le resserrage éventuel (égalité sur les stockouts francs) attend le PAS 4 de l'ADR-020.
- **À surveiller :** le test est calé sur l'horizon PI seedé (90 j). Élargir l'horizon de B sans élargir le seed casserait le contrat pour une mauvaise raison — documenté dans le test.

## Références

- `docs/ADR-020-mrp-consolidation.md` — la maths canonique MRP et le PAS 4.
- `docs/ADR-004-explainability.md` — la chaîne causale exigée des pénuries persistées.
- `src/ootils_core/engine/kernel/shortage/detector.py` — propriétaire exclusif de `shortages`.
- `src/ootils_core/engine/orchestration/propagator_sql.py:SHORTAGES_SQL` — détection SQL + valorisation #342.
- `scripts/agent_shortage_watcher.py` — le consommateur read-only type de la vérité B.
- `tests/integration/test_shortage_truth_consistency_integration.py` — le garde-fou.

---

## Amendement — 2026-07-12 (ADR-039, PURGE-1)

Un troisième acteur touche désormais `shortages`, mais borné à un rôle de garbage-collection sur de l'historique déjà mort, jamais à la vérité de pénurie elle-même. [ADR-039](ADR-039-scenario-archive-cleanup.md) (PURGE-1, migration 076) introduit `apply_shortage_retention` (`src/ootils_core/engine/maintenance/purge.py`), qui supprime les lignes `status='resolved'` plus vieilles qu'une fenêtre de rétention (défaut 30 jours) ET hors du dernier `calc_run` `completed` du scénario. Ce sweep ne crée, ne valorise et ne résout **jamais** une pénurie — `status='active'` est codé en dur comme non-éligible, jamais un paramètre — et ne touche jamais la vue la plus récente et auditable d'un scénario (son dernier `calc_run` complété est explicitement protégé). `ShortageDetector` reste l'écrivain **exclusif** de la SÉMANTIQUE de pénurie : création, valorisation `$` (§4 ci-dessus), chaîne causale ADR-004, cycle de vie `resolve_stale`. PURGE-1 ne fait que borner la RÉTENTION de l'historique déjà résolu — une opération de maintenance déléguée, jamais un second écrivain de vérité. Voir ADR-039 pour le détail complet, y compris la garde CI qui garantit que `shortages` reste dans la whitelist FK-safe de purge de fork (pénuries d'un scénario ARCHIVÉ purgé) indépendamment de ce sweep de rétention (qui, lui, tourne sur tout scénario, vivant ou archivé).
