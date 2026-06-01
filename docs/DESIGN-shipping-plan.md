# DESIGN — Shipping Plan layer

**Status:** Design / proposed — **deferred chantier** (gated on ASP + returns series). Vetted vs North Star invariants by ootils-architect.
**Date:** 2026-05-31
**Related:** `memory` demand-business-rule-booking (locked rules), ADR-020 (DRP/MRP echelons), Pyramide demand module, distributor layer (issue #326).

> **Verdict de faisabilité :** FAISABLE et bien aligné North Star — c'est même le cas d'usage qui fait *briller* le scenario engine (budget-as-frozen-scenario + variance 3-voies). Le cœur reste déterministe. Chantier **différé** : dépend d'ASP (pas encore calculé) et de la série returns (pas encore matérialisée). Deux arbitrages utilisateur restent ouverts (§11).

## 1. Contexte & lien wedge

Le Shipping Plan est la couche **pilotable** de la chaîne `Demand Plan (forecast booking) → SHIPPING PLAN → tête de DRP → DRP per-site → MRP central`. Il convertit *ce qui est demandé* (booking, non-contraint) en *ce qu'on s'engage à expédier, quand, où* — sous une cible S&OP mensuelle en $ (« Ad'hoc »). Pour le wedge V1, c'est la couche où un agent **drafte** un placement d'expéditions (L1), où le S&OP **approuve** (L3), et où la **variance Budget/Forecast/Réel** rend la décision auditable et scenario-backed. Sans cette couche, le DRP/MRP consomme du booking brut — ce qui sur-planifie (un booking n'est pas un engagement d'expédition).

## 2. Modèle conceptuel

**Inputs :** forecast booking (Pyramide, per hierarchy × org × programme × climat → désagrégé per-site, porte confiance+fraîcheur) · carnet d'ordres fermes (must-ship) · règles d'expédition par programme (§3) · **Ad'hoc $/mois** (valeur unique = NET d'expédition brut−retours, niveau company, posée par S&OP, rolling) · **Budget** (scénario figé de référence, §5) · **ASP** (units↔$) · **dispo/supply** (projection per-site virtuelle).

**Logique :** moteur de règles par programme (must-ship / fenêtres / réservoir Early-Buy) + allocation déterministe du réservoir flexible pour atteindre l'Ad'hoc $ mensuel sous contrainte de dispo (§4).

**Output :** un **schedule d'expédition per-site mensuel** = la demande indépendante en **tête de DRP** (consommé par le DRP echelon, pas le booking brut). Confidence + freshness propagés.

**Cadence :** rolling mensuel. Chaque cycle S&OP = un `shipping_plan_run` versionné, scenario-scoped ; le précédent reste pour audit/variance.

## 3. Moteur de règles par programme (déterministe, rule-based)

| Programme | Règle | Couplage dispo |
|---|---|---|
| Standard Vista | ship ≤ 4 j (SLA) | must-ship sauf rupture → SLA casse en rupture |
| Warranty | ship ≤ 1 j (SLA), **flux séparé** | must-ship sauf rupture |
| Spring/Summer Buy | ship à discrétion dans [mois commande, +1 mois] | semi-flexible |
| Early Buy | commandé Sep/Oct, ship-by Fév | **variable d'ajustement** — réservoir |

Le must-ship (SLA + ordres fermes) est posé **en premier** (plancher non-négociable). Le flexible (réservoir Early-Buy + fenêtres ouvertes) est ce que l'allocation déplace. Un SLA breaké en rupture émet un signal shortage (→ wedge).

## 4. Allocation Early-Buy / placement flexible

**Approche : heuristique greedy déterministe. PAS de solveur LP/MILP en V1. ZÉRO LLM dans le calcul.**

Par mois cible : (1) poser le plancher must-ship (SLA + fermes), valorisé à l'ASP → $ engagés ; (2) gap = Ad'hoc $ − $ plancher ; (3) tirer dans le **réservoir Early-Buy** éligible (fenêtre ouverte), trié par clé totalement ordonnée déterministe (ex. ship-by le plus proche / ancienneté booking), jusqu'à combler le gap **sous contrainte de dispo per-site** ; (4) reste reporté au mois suivant tant que dans la fenêtre ; au-delà de ship-by Fév → must-ship forcé + flag.

**Pourquoi greedy plutôt que LP :** déterminisme/reproductibilité triviaux + explicables ligne à ligne (chaque unité placée porte sa raison) ; reflète le revenue-management réel mieux qu'un optimum opaque ; un LP introduit du non-déterminisme (tie-break solveur) et de l'opacité = anti-pattern explicabilité. Besoin d'optimum exact un jour → ADR dédié, solveur déterministe (seed fixe, ordre canonique), **jamais** dans le chemin par défaut.

## 5. Intégration scenario engine & variance 3-voies

- **Budget** = scénario **figé** via `ScenarioManager.create_scenario` (deep-copy fork), status référence (non-recalculé). Convention : `budget:<FY>`.
- **Forecast/Ad'hoc** = scénario **working** (rolling), forké du baseline, recalculé chaque cycle.
- **Réel** = baseline (facts `demand_history` shipping series).
- **Variance 3-voies** : réutilise `scenario_diffs` (mig 006) étendu aux champs shipping-plan, ou calcul à la volée. Chaque shipping plan vit **dans un scenario_id** → forkable (un agent teste un Ad'hoc alternatif dans un fork sans toucher au plan S&OP).

## 6. Stockage

**Deux tables typées, scenario-scoped** (pas de JSONB — forme bornée) ; **migration `049_shipping_plan.sql`** (idempotente) :

- `shipping_plan_run` : run_id, scenario_id, org_id, plan_month, adhoc_target_value ($ net), asp_snapshot, calc_run_id, confidence, freshness, created_at/by, policy_result.
- `shipping_plan_line` : run_id, scenario_id, item_id, warehouse_id, ship_month, order_type, planned_ship_qty, planned_ship_value, source (`must_ship`|`flexible_reservoir`|`forecast`), reason. = **independent demand en tête de DRP**.

Index : `(scenario_id, plan_month)`, `(run_id)`, `(scenario_id, warehouse_id, ship_month)`. **Le DRP lit `shipping_plan_line` par scenario_id** comme tête (cohérent ADR-020). NE PAS matérialiser de nœuds `ProjectedInventory` ici (bloat) — table de demande indépendante, le DRP projette virtuellement.

## 7. North Star — checklist

Forkable ✓ · Déterministe ✓ (greedy ordonné, no LLM, no aléa) · Queryable par scenario_id ✓ · Streamable ⚠ (câbler delta `shipping_plan_changed`) · Explicable ✓ (chaque ligne `source`+`reason`) · Auditable ✓ (run : Ad'hoc/ASP in, output, scenario, calc_run, policy, created_by) · Confidence-aware ✓ · **Decision Ladder** : *proposer un placement* = **L1 DRAFT** (agent) ; *publier en tête de DRP / figer le budget / réajuster l'Ad'hoc* = **L3** (S&OP/humain) · Budgeté/kill-switch ✓.

## 8. Anti-patterns refusés

1. Shipping plan baseline-only / hors-scénario. 2. Forecast/Ad'hoc → DRP sans confidence/freshness. 3. Solveur LP non-déterministe dans le chemin par défaut. 4. LLM dans le placement (l'agent drafte/explique, le calcul est pur). 5. Agent qui publie en tête de DRP sans approbation L3. 6. Netter les returns dans la demande units (returns = série séparée ; le « net » de l'Ad'hoc est en **$** au niveau company, pas un netting unitaire par ligne).

## 9. Découpage en chantiers / PAS

- **PAS 0 — ADR-021** « Shipping Plan layer » (greedy-déterministe, budget-as-scenario, tête de DRP). Ce DESIGN en est le brouillon.
- **PAS 1 — dépendances amont (GATE, bloquant)** : **ASP calculée** (rolling T12M value÷units hors-warranty — pas encore implémenté) + **série returns matérialisée** (pour le « net » de l'Ad'hoc).
- **PAS 2 — schéma** : migration `049_shipping_plan.sql` (2 tables typées, scenario-scoped).
- **PAS 3 — moteur de règles** : module pur DB-free (style `mrp_core`) — règles par programme + greedy reservoir, paramétré par scenario_id.
- **PAS 4 — scenario + variance** : budget figé, working rolling, diff 3-voies.
- **PAS 5 — gouvernance/stream** : delta `shipping_plan_changed`, state machine L1→L3, audit, kill-switch.
- **PAS 6 — câblage DRP** : le DRP (ADR-020 PAS 4) lit `shipping_plan_line` par scenario_id comme tête. Dépend de la refonte DRP de B.
- **Tests** : golden-master du greedy (Postgres réel) — must-ship floor, reservoir drawdown atteint l'Ad'hoc, dispo respectée, déterminisme (run×N byte-identique), variance 3-voies, isolation scénario.

**Dépendances :** ASP (bloquant), returns series (bloquant), scenario engine (existe), DRP/MRP aval (ADR-020 PAS 4, gated), Pyramide forecast per-site (data OK).

## 10. Risques & non-objectifs

**Non-objectifs :** le calcul ASP (brique Pyramide), la refonte DRP/MRP write (ADR-020 PAS 4), l'optimisation par solveur exact, le forecast warranty. **Risques :** (1) **couplage shipping↔supply** — la contrainte de dispo crée une boucle ; designer en **single-pass** (must-ship pose le plancher, flexible comble sous dispo *figée du run*), pas un point-fixe non borné. (2) Désagrégation Ad'hoc $ (company) → per-site/per-item : proportions déterministes (historique shipping per-site × ASP). (3) No-double-count : exclure `fulfillment='inter_entity'` de l'end-demand.

## 11. Questions ouvertes (arbitrage utilisateur avant code)

1. **Désagrégation de l'Ad'hoc $** company → per-site × per-item × programme : quelle clé déterministe ? (proportions historiques shipping ? mix forecast Pyramide ?)
2. **Couplage dispo** : single-pass (reco V1) vs itératif borné ?
3. **« Net » de l'Ad'hoc** : confirmé $ net/mois company ; le netting returns reste en **$ company**, jamais par ligne unitaire — à valider vu la sign rule.
4. **State machine** : réutiliser `recommendations` (mig 039, shortage-centric) ou table de transitions shipping-plan dédiée ?
