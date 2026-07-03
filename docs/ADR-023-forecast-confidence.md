# ADR-023 — Score de confiance des prévisions : composition déterministe accuracy x profondeur x fraîcheur

**Statut :** Accepté — Pyramide V1 axe D, PR-D4 (branche `feat/pyramide-d-confidence`) ; s'appuie sur PR-D3 (`pyramide_accuracy_metrics`, migration 055) et sur la fondation demande (migration 047, `demand_history.ingested_at`).
**Date :** 2026-07-03
**Contexte mesuré :** `docs/DESIGN-pyramide-forecasting.md` §2.D, heuristique à paliers de `api/routers/forecasting.py:_compute_confidence_score` (supprimée par cette PR), North Star « confidence-aware » (CLAUDE.md).

---

## Contexte

Le North Star exige que toute sortie consommée par un agent porte un score de confiance et un signal de fraîcheur — un agent L0-L2 n'agit jamais sur une donnée périmée ou peu fiable. Or le seul « score » existant était une heuristique à paliers dans le routeur `/generate` : des seuils codés en dur (7/30 points d'historique, MAPE in-sample), aucun composant tracé, aucune notion de fraîcheur, et une MAPE in-sample qui surestime structurellement l'accuracy. Rien ne disait à un agent *pourquoi* le score valait 0.7, ni si la donnée d'entrée datait d'hier ou d'un mois.

## Décision

1. **Un module PUR et déterministe, `pyramide/confidence.py`** : `compute_confidence(wape, history_depth_days, ingest_age_days, *, sla_days, depth_saturation_days, weights) -> ConfidenceScore(score, components, stale, explanation)`. Aucun I/O, aucun aléa, `Decimal` de bout en bout, quantifié à 1e-4. Les appelants (routeurs, agents) assemblent les trois signaux bruts ; le module compose.
2. **Formule à composants explicites, chacun dans [0, 1], somme pondérée** (poids par défaut documentés : accuracy 0.5, profondeur 0.25, fraîcheur 0.25 — l'accuracy porte la moitié car c'est ce sur quoi l'agent agit ; les poids sont un paramètre normalisé, pas une constante) :
   - **accuracy** = `1 / (1 + wape)` — mapping monotone borné : WAPE 0 → 1.0, WAPE 1 (100 % d'erreur) → 0.5, WAPE → ∞ → 0. La source est la ligne AGRÉGAT (horizon NULL) la plus récente de `pyramide_accuracy_metrics` pour la série (backtest rolling-origin honnête, PR-D3) — **jamais la MAPE in-sample du moteur**, qui s'auto-note.
   - **profondeur** = `min(1, depth_days / depth_saturation_days)` — saturant ; l'horizon de saturation est un PARAMÈTRE (défaut 365 jours ≈ un cycle saisonnier complet), ajustable par l'appelant, pas une règle métier codée.
   - **fraîcheur** = 1 si `ingest_age <= sla_days`, sinon décroissance hyperbolique `sla_days / ingest_age` ; `stale = ingest_age > sla_days`.
3. **Source de fraîcheur : `demand_history.ingested_at` (migration 047), horloge du serveur DB** — `repository.get_demand_freshness(db, item_id, warehouse_id)` retourne `ingest_age_days` (le pipeline d'ingestion est-il vivant ?) et `coverage_lag_days` (le signal booking lui-même est-il en retard ?). Table vide → tous champs `None` : une fraîcheur n'est **jamais inventée**. Les routeurs mesurent au niveau ITEM (le pipeline charge des extracts entiers ; un filtre par entrepôt confondrait trou de couverture et pipeline mort).
4. **SLA de fraîcheur : PARAMÈTRE, défaut 7 jours — décision pilote ajustable.** Exposé dans les requêtes API (`freshness_sla_days`), borné 1-365. Générique tout business : un pilote hebdomadaire vit avec 7 j, un flux quotidien passera à 2 j sans toucher au code.
5. **Composant manquant → défaut PRUDENT 0.25, tracé.** Métrique NULL, aucune ligne d'accuracy, fraîcheur inconnue : le composant vaut `MISSING_COMPONENT_DEFAULT = 0.25` (« on ne sait rien, on suppose médiocre ») — jamais un 1.0 optimiste — et l'explication le nomme. Un `stale` n'est en revanche posé que sur preuve (`ingest_age` connu ET > SLA) : l'inconnu dégrade le score, il ne fabrique pas de drapeau.
6. **Le run Pyramide porte la staleness dans sa provenance** : quand la fraîcheur mesurée au moment d'un `POST /v1/forecast/runs` dépasse le SLA, le run est quand même produit (un agent a le droit de simuler sur du périmé) mais (a) `pyramide_runs.stale_demand = TRUE` (colonne typée, migration 056), (b) **un** `dq_findings` `STALE_DEMAND` est émis (une fois par run, evidence JSONB = run_id, âges, SLA ; ledger `agent_runs` attribué à `pyramide_freshness_gate`), (c) la réponse API expose `stale_demand`.
7. **Contrat API rétro-compatible** : `/v1/demand/forecast/generate` garde `confidence_score` (même nom, désormais calculé par le module) et gagne `confidence_components` optionnel (composants, stale, explication, sources nommées, SLA effectif) — le score est recomposable à la main depuis la trace (esprit ADR-004).
8. **Lien Decision Ladder (stratégie §5)** : un agent L0-L2 **ne doit pas agir** sur `stale=True`, ni sous un score jugé insuffisant. Le SEUIL de score est du ressort des consommateurs (politique de gouvernance par type d'action), pas de ce module : l'ADR fixe le contrat du signal, pas la politique qui le consomme.

## Alternatives rejetées

- **Garder l'heuristique à paliers.** Rejetée : seuils métier codés en dur, MAPE in-sample auto-complaisante, aucune fraîcheur, aucun composant tracé — inauditable par un agent de gouvernance (anti-pattern « métrique sans signal de confiance » du CLAUDE.md, ironiquement incarné par le score de confiance lui-même).
- **Un score composite opaque (modèle appris, pondération implicite).** Rejeté : le différenciateur d'Ootils est l'explicabilité ; un score que l'on ne peut pas recalculer à la main depuis sa trace serait refusé par les agents de gouvernance (ADR-004). Un LLM dans ce chemin violerait « deterministic core ».
- **Défaut optimiste (1.0) ou neutre (0.5) pour un composant manquant.** Rejeté : l'absence d'information n'est pas une information favorable ; 0.25 force la prudence et rend l'absence visible dans le score au lieu de la masquer.
- **Émettre le finding STALE_DEMAND à chaque lecture / à chaque score.** Rejeté : spam de findings sans nouvel événement ; l'émission est liée à l'acte qui consomme la donnée périmée (la création du run), une fois par run.

## Conséquences

- **Positif :** tout forecast exposé porte désormais score + composants + stale reproductibles ; les runs Pyramide générés sur demande périmée sont requêtables (`WHERE stale_demand`) et tracés dans `dq_findings` ; la brique « les agents gatent sur confiance + fraîcheur » du wedge V1 est réelle.
- **Contrainte assumée :** le composant accuracy vaut le défaut prudent tant qu'aucun run Pyramide n'a persisté de WAPE agrégat pour la série — un premier forecast est donc structurellement modeste (comportement voulu : la confiance se gagne par le backtest).
- **À surveiller :** le gate de fraîcheur des runs hiérarchiques (`persist_series_run`) reste à brancher (colonne DEFAULT FALSE = « non prouvé périmé », jamais « prouvé frais ») ; les poids par défaut 0.5/0.25/0.25 sont une décision pilote à recalibrer quand les consommateurs auront des seuils réels.

## Références

- `src/ootils_core/pyramide/confidence.py` — formule, défauts prudents, trace.
- `src/ootils_core/pyramide/repository.py` — `get_demand_freshness`, `fetch_latest_aggregate_wape`, `record_stale_demand_finding`.
- `src/ootils_core/db/migrations/056_pyramide_run_stale_demand.sql`.
- `docs/ADR-004-explainability.md`, `docs/STRATEGY-autonomous-supply-chain-operations.md` §5, migration 044 (`dq_findings`), migration 055 (PR-D3).
