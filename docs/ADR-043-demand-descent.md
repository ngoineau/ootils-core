# ADR-043 — La descente de demande : planification nationale, exécution par centre

**Statut** : **Accepted** — décision pilote 2026-07-18.
**Date** : 2026-07-18
**Auteurs** : architecte ootils-core (cadrage) + pilote (décision)
**Contexte mesuré** : premier chargement réel (18/07, 14 flux TSV) — zéro rupture physique détectée aux entrepôts ; [ADR-020](ADR-020-mrp-consolidation.md) §Unité de planification (le DRP per-site est mergé et tourne à vide, faute de demande localisée) ; [ADR-021](ADR-021-shortage-truth.md) (les deux vérités de pénurie) et son amendement `is_stocking` du 2026-07-17 (~9 400 pénuries fantômes sur les canaux nationaux virtuels USA/CAN/ICO) ; `src/ootils_core/engine/mrp/loader.py:93-113` (le safety stock national existe déjà par sommation) ; `C:\Users\ngoin\.claude\plans\giggly-wandering-moon.md` (DESC-1, plan approuvé).

---

## Contexte

Le premier chargement réel (18/07) a montré **zéro rupture physique** aux entrepôts. Ce n'est pas un défaut de qualité de données : la demande enregistrée sur le canal national USA ne « descend » jamais vers les centres de distribution physiques. Toute la demande (forecast + commandes clients) est portée par des nœuds rattachés aux canaux virtuels nationaux (`USA`/`CAN`/`ICO`, `locations.is_stocking = FALSE` depuis la migration 081) — des nœuds de routage/allocation, pas des sites de stockage. Les vrais centres (PAT/DCW/DAL) n'ont donc, à ce jour, aucune demande directement rattachée, et la détection de pénurie par site (Truth A, `shortages`) n'a structurellement rien à détecter là où elle regarde.

### Le modèle métier du pilote — « la logique que Kinaxis n'a jamais compris »

Le pilote a formulé un modèle à deux niveaux, verbatim :

- **Planification GLOBALE au niveau national USA.** La demande et le stock de sécurité sont **mutualisés** au national (risk pooling) — une rupture *locale* sur un centre se **route** vers un centre qui dispose du stock, plutôt que de déclencher un réapprovisionnement isolé sur le centre en tension.
- **Exécution PAR CENTRE.** Les commandes sont **dispatchées par état** (US state) vers les vrais centres physiques (PAT/DCW/DAL). Les **ordres d'achat** sont posés par le DRP **sur le centre** dont la projection montre le besoin, via un **pourcentage par produit** dérivé de l'historique par état, combiné à une **éligibilité produit → centres** (tous les produits ne sont pas servis par tous les centres).

C'est un modèle de **distribution planifiée au national, exécutée localement** — exactement l'articulation risk-pooling / DRP que la littérature APICS décrit, mais qu'aucun des deux moteurs (planification et exécution) ne portait jusqu'ici de façon reliée dans Ootils : la demande s'arrêtait au national, l'exécution par centre n'avait rien à consommer.

### Découverte clé de l'exploration : le moteur était déjà prêt

L'échelon DRP per-site (`src/ootils_core/engine/drp/`, ADR-020/ADR-028 — répartition équitable, lanes, recommandations de transfert gouvernées) est **mergé** et tourne **à vide**, faute de demande localisée : ADR-020 l'avait déjà écrit noir sur blanc — *« Tant que la demande est mono-location (état actuel des données pilote), le DRP tourne à vide. Le chemin critique est donc côté module demande (split per-site), pas côté MRP »* (ADR-020, §Unité de planification). Le loader DRP (`src/ootils_core/engine/drp/loader.py:1-34`) est déjà câblé sur les mêmes `node_type` de demande que le MRP (`DRP_DEMAND_TYPES = ["CustomerOrderDemand", "ForecastDemand"]`, `loader.py:34`) et clé déjà sur `(item, location)` — il ne lui manque que des nœuds de demande posés sur de vrais centres `is_stocking = TRUE`.

## Convergence structurelle avec ADR-021

La descente n'invente **aucune** vérité nouvelle — elle branche un flux de données sur une architecture à deux vérités déjà décidée et déjà en place :

| | Truth B — `mrp_core` (poolé, item) | Truth A — table `shortages` (per-site) |
|---|---|---|
| Rôle dans DESC-1 | **SA planification** (le national) | **SON exécution** (le centre) |
| Où c'est déjà vrai aujourd'hui | Le safety stock national existe **déjà** par sommation multi-location : `SUM(COALESCE(rp.safety_stock_qty, 0)) … GROUP BY rp.item_id` (`src/ootils_core/engine/mrp/loader.py:97,113`) — aucune maths nouvelle à écrire sur cet axe. | Le système canonique de persistance/query per-(item, location), déjà lu par le DRP et les watchers (ADR-021 §2). |
| Ce que la descente change | Rien : Truth B continue de pooler la même demande, qu'elle soit portée par un nœud national ou par les nœuds descendus (le total national est préservé par construction — voir §Vérification). | Tout : elle reçoit enfin de la demande physiquement rattachée à un site `is_stocking = TRUE`, donc redevient un signal actionnable au lieu d'un CTE structurellement vide sur ces items. |

Autrement dit : l'amendement `is_stocking` du 2026-07-17 (ADR-021) avait déjà posé la moitié droite de l'équation — « USA/CAN/ICO ne sont pas des sites, exclus de la DÉTECTION » — sans encore répondre à la question symétrique, « alors où va la demande physiquement ? ». Cet ADR répond : elle descend sur les vrais centres, et c'est Truth A (déjà canonique, déjà lue par tout le monde) qui en hérite. La descente est un **flux de données alimentant une vérité existante**, pas un troisième système.

---

## Décision

### 1. Matérialisation par un run dédié, jamais à l'ingestion, jamais dans le propagateur

`POST /v1/demand/descend` (kill switch, scoped par scénario) :

1. Lit la demande nationale (nœuds `ForecastDemand`/`CustomerOrderDemand` sur les canaux virtuels `is_stocking = FALSE`) + la table des parts (`demand_split_pct`) + l'éligibilité produit→centres.
2. **Écrit des nœuds `ForecastDemand`/`CustomerOrderDemand` dérivés sur les VRAIS centres** (`is_stocking = TRUE`) — **aucun nouveau `node_type`**.
3. **Désactive les nœuds nationaux sources** (`active = FALSE`, anti-double-comptage — sans cette désactivation, Truth B compterait deux fois la même demande, une fois via le nœud national encore actif, une fois via les nœuds descendus). L'audit est conservé (la ligne désactivée reste lisible, elle n'est jamais supprimée — même doctrine que le reste du dépôt, cf. `ADR-005` insert-only / `ADR-011` soft-delete).
4. **Ledger de provenance** : chaque nœud descendu trace sa source (le nœud national d'origine) + le pourcentage appliqué + la méthode (`demand_split_pct.method`) + le run qui l'a produit.
5. **Event typé `demand_descended`** — un événement par run, même granularité qu'`ADR-027`/`ADR-039`'s `purge_executed` et `ADR-042`'s `daily_run_completed` (un event de confirmation par exécution, pas par ligne).

**Zéro read-path modifié.** Le loader DRP, le loader MRP, la projection et le détecteur de pénurie captent la demande descendue *telle quelle* — ce sont des nœuds `ForecastDemand`/`CustomerOrderDemand` ordinaires, indiscernables pour ces lecteurs d'une demande saisie directement sur le centre. **Le DRP se réveille automatiquement**, sans aucune modification de son code.

**Forkable.** Un fork re-exécute la descente avec **ses propres** parts (`demand_split_pct` scénario-scopée, §2) pour tester des politiques de répartition alternatives sans toucher la baseline — cohérent avec le North Star (« every state-changing capability must work inside a scenario fork »). `promote()` ne rejoue **jamais** la descente sur baseline (L0, simulation pure — même doctrine que l'overlay ADR-025).

### 2. Table `demand_split_pct` (migration 083, scénario-scopée avec fallback baseline)

Même **pattern de table** que `scenario_planning_overrides` (ADR-025) — scénario-scopée, fallback baseline explicite — mais **pas le même usage** : `resolved_params_sql()` d'ADR-025 est fanned-out à 5 lecteurs à la volée ; `demand_split_pct` n'a **qu'un seul lecteur**, le run de descente lui-même (§1), précisément parce que la matérialisation évite d'avoir à porter la résolution dans N chemins de lecture (voir §Alternatives rejetées, résolveur virtuel).

Colonnes : `(scenario_id, item_id, centre/location_id, pct, method, manual_override, confidence, freshness)` — + une colonne de saisonnalité posée mais non exploitée en V1 (voir §🎯 défauts assumés).

**Calcul déterministe pur** : `engine/descent/shares.py` (module DB-free à écrire en PR-A), miroir de la primitive de désagrégation Pyramide `middle_out` (ADR-022) — **Σ = 1 garanti** par construction, **cold-start explicite**.

- **Sans historique par état disponible** : split **égal** sur les centres éligibles, **flaggé** (`method = 'equal_flagged'` ou équivalent — jamais silencieux : un lecteur du ledger doit pouvoir distinguer un split calibré d'un split placeholder).
- **Zéro centre éligible pour un item** : la demande **reste nationale** (fail-loudly — pas de perte silencieuse de demande, pas de split forcé sur un centre non éligible).

### 3. Sécurité nationale (`safety_scope`) — décision actée ici, PAS ENCORE appliquée

Politique `safety_scope`, défaut `national`, lue par le détecteur de pénurie : la détection **par centre** ne déclenchera plus que sur le **stockout physique** (closing stock négatif) — le coussin de sécurité vit dans la vérité poolée nationale (déjà en place, §Convergence). Les 1 051 182 jours-article de « sous-sécurité » mesurés par site lors du premier chargement (sévérité cumulée ~2,73 Md$ jour-article — RAPPORT-PREMIER-CHARGEMENT-2026-07-18) sont un artefact à re-baser en ruptures réelles une fois la demande descendue.

**Ceci est une décision pilote actée le 2026-07-18, pas un fait déjà implémenté.** L'amendement formel d'ADR-021 (le `safety_scope` gate, en miroir de l'amendement `is_stocking` déjà livré le 2026-07-17) et son code (`ShortageDetector`, `SHORTAGES_SQL`) sont portés par **PR-C**, après PR-B. Tant que PR-C n'est pas mergée, la détection par centre continue de déclencher sur `below_safety_stock` local, comme aujourd'hui — cette section documente l'intention actée, pas l'état runtime.

### 4. Désactivation des sources nationales

Voir §1 point 3 — traité comme faisant partie de la matérialisation, pas d'une passe séparée : la désactivation est atomique avec l'écriture des nœuds descendus, dans le même run.

---

## Alternatives rejetées

- **Résolveur virtuel à la ADR-025 (résolution au read-time, jamais de matérialisation).** Rejeté : exigerait de réécrire les **4 loaders** qui consomment de la demande (DRP, MRP, projection, détecteur de pénurie) pour qu'ils résolvent la répartition à la volée — contrairement à `resolved_params_sql()`, qui résout un scalaire par champ, une répartition demande de fabriquer des lignes de demande *nouvelles* réparties sur N centres, pas de corriger la valeur d'un champ existant. Le coût réel est supérieur à celui d'ADR-025 (qui ne portait que 15 champs scalaires sur 5 lecteurs). Et l'**explicabilité** (ADR-004) et le **streaming** (ADR-027) en sortiraient dégradés : un nœud de demande qui n'existe que comme un artefact de jointure à la lecture ne peut ni porter de chaîne causale propre ni émettre un delta observable sur `/v1/stream` — le North Star exige une trace matérialisée, pas une resolution implicite invisible entre deux runs.
- **Split à l'ingestion.** Rejeté : figerait la politique de répartition au moment où la donnée entre dans le système, la rendant impossible à re-simuler par scénario (aucun fork ne pourrait tester une politique de split alternative sans re-ingérer), et confondrait une décision métier versionnable (comment répartir) avec un fait d'ingestion (quelle demande existe).
- **Nouveau `node_type` (p. ex. `RoutedDemand`).** Rejeté : casserait **tous** les read-paths existants — le loader DRP et le loader MRP filtrent explicitement sur `["CustomerOrderDemand", "ForecastDemand"]` (`drp/loader.py:34`, et l'équivalent côté `mrp/core.py`), la projection et le détecteur de pénurie de même. Un nouveau type exigerait de modifier N consommateurs pour qu'ils reconnaissent le nouveau type en plus des deux existants — l'exact inverse du principe « zéro read-path touché » qui rend cette matérialisation bon marché.

---

## Décisions pilote (2026-07-18)

Quatre arbitrages tranchés par le pilote, qui débloquent la mise en œuvre :

1. **Règles MIXTE** : l'ERP extrait la table état→centre (`state_to_dc`, §Séquencement PR-F) ; Ootils calcule les pourcentages depuis l'historique.
2. **Historique par état extractible** : `demand_history.ship_state` (`src/ootils_core/db/migrations/047_demand_foundation.sql:236`) porte déjà la granularité état — aucune extension de schéma nécessaire pour calibrer les parts.
3. **Sécurité NATIONALE seulement** : l'alerte par centre du premier chargement est un artefact à re-baser (§Décision 3), pas un signal à corriger site par site.
4. **Périmètre = prévisions + commandes** (forecast + customer orders) — pas les autres familles de demande (`DependentDemand`/`TransferDemand` restent hors périmètre de la descente, elles sont déjà rattachées à de vrais sites par construction).

### 🎯 Défauts assumés (ajustables)

| Axe | Défaut V1 | Note |
|---|---|---|
| Saisonnalité des % | **Annuelle** | Colonne prête dans `demand_split_pct` pour une granularité plus fine plus tard. |
| Recalibration | **À la demande** | Cron/automatisation différée à un chantier ultérieur. |
| Éligibilité produit→centres | **Dérivée** (historique ∪ lanes `distribution_links` existantes) | Jusqu'à ce que l'extrait ERP `item_dc_eligibility` (PR-F) soit livré. |
| Cold-start (item sans historique) | **Split égal, flaggé** | Jamais silencieux — voir §2. |
| `safety_scope` | **National** | Décision actée, code en PR-C (voir §Décision 3). |

---

## Séquencement (PRs) et gate ERP

| PR | Contenu | Démarre |
|---|---|---|
| **F** | Specs TSV pour l'ERP (`state_to_dc` — dispatch d'exécution, `item_dc_eligibility`, `distribution_links` — lanes de réappro, **distinctes** du dispatch) → déposées dans la Dropbox. Note « historique profond AVEC `ship_state` » (le schéma le porte déjà, migration 047). | **Maintenant** — débloque l'ERP, dont le délai de tour est long. |
| **A** | Cet ADR + migration 083 (`demand_split_pct`, ledger de provenance, schémas de routage/éligibilité) + moteur pur des parts (`engine/descent/shares.py`) + goldens. | Maintenant, en parallèle de F. |
| **B** | Le run de descente (« activer le lien ») + migration suivante (event `demand_descended`) + `POST /v1/demand/descend` + intégration (anti-double-comptage, forkabilité, idempotence, `ANALYZE` post-bulk-insert — invariant #455). | Après A. |
| **C** | Sécurité nationale (`safety_scope`) + amendement formel ADR-021 + garde de cohérence verte. | Après B. |
| **D** | Ingest `distribution_links` réel + preuve DRP (le run de descente doit faire émettre au DRP des recommandations de transfert non vides). | Après B. |
| **E** | Calibration réelle des % depuis l'historique par état ERP. | **Gated données ERP** (dépend de la livraison PR-F). |

**Le lien s'active AVANT les données ERP réelles** : split égal flaggé sur les centres dérivés-éligibles en attendant, bascule vers les % calibrés dès que l'historique ERP est livré — aucune étape n'attend l'ERP sauf E elle-même.

---

## Vérification

- **Goldens du moteur de parts** (`engine/descent/shares.py`) : Σ = 1, déterminisme, cold-start, None-honnête (miroir des goldens `middle_out`/ADR-022).
- **Intégration Postgres réelle** : le total national est **préservé** après descente (la vérité poolée B ne change pas de valeur, seulement de nœuds porteurs) ; un fork avec des % différents n'affecte pas la baseline ; un re-run est idempotent ; **un** event `demand_descended` par run.
- **Preuve de bout en bout** sur la base de debug avec le bundle du 18/07 : après descente, des ruptures physiques par centre apparaissent, `/v1/drp/run` émet des transferts non vides, l'alerte sécurité re-basée (PR-C) s'effondre en signal actionnable.
- **Garde CI ADR-021** (`tests/integration/test_shortage_truth_consistency_integration.py`) reste verte à chaque étape du séquencement — la descente ne doit jamais faire diverger l'inclusion `items(B) ⊆ items(A)`.

## Conséquences

- **Positif** : le DRP (mergé, ADR-020/ADR-028) cesse de tourner à vide ; Truth A (`shortages`, ADR-021) redevient un signal actionnable par site au lieu d'un CTE structurellement vide sur les items nationaux ; aucune nouvelle vérité de pénurie n'est créée, aucun read-path n'est modifié — la descente est un flux de données, pas un troisième système.
- **Négatif / dette assumée** : le split est un **placeholder égal-flaggé** tant que l'historique ERP par état (PR-E) n'est pas livré — les recommandations DRP dérivées avant PR-E doivent être lues avec cette réserve. L'éligibilité produit→centres reste **dérivée** (historique ∪ lanes existantes) jusqu'à l'extrait ERP dédié.
- **Reste à faire** : PR-B (le run lui-même) est un préalable dur à tout ce qui suit ; PR-C (amendement `safety_scope`) est décidé mais non codé — ne pas présumer son comportement runtime avant merge ; PR-D (preuve DRP end-to-end) et PR-E (calibration réelle) restent gated respectivement par PR-B et par la livraison ERP.

## Code references

- `src/ootils_core/engine/mrp/loader.py:93-113` — le safety stock national existe déjà par sommation multi-location (`SUM(COALESCE(rp.safety_stock_qty, 0)) … GROUP BY rp.item_id`), la moitié « planification » du modèle est déjà en place.
- `src/ootils_core/engine/drp/loader.py:1-34` — le loader DRP, déjà keyed `(item, location)`, déjà scopé sur `DRP_DEMAND_TYPES = ["CustomerOrderDemand", "ForecastDemand"]` — zéro modification nécessaire pour capter la demande descendue.
- `docs/ADR-020-mrp-consolidation.md` §Unité de planification — « le DRP tourne à vide », le diagnostic dont ce chantier est la résolution.
- `docs/ADR-021-shortage-truth.md` — les deux vérités de pénurie ; amendement 2026-07-17 (`is_stocking`) — le constat des ~9 400 pénuries fantômes qui a déclenché l'exploration DESC-1 ; amendement `safety_scope` formel **à venir en PR-C** (non encore écrit).
- `docs/ADR-025-scenario-param-overlay.md` — le pattern de table scénario-scopée + fallback baseline dont `demand_split_pct` s'inspire (usage différent : un seul lecteur, pas un résolveur fanned-out).
- `docs/ADR-022-pyramide-reconciliation.md` — la primitive `middle_out` dont `engine/descent/shares.py` est le miroir (Σ = 1, cold-start).
- `src/ootils_core/db/migrations/081_location_is_stocking.sql` — `locations.is_stocking`, la moitié « détection exclue » du modèle, déjà livrée.
- `src/ootils_core/db/migrations/047_demand_foundation.sql:236` — `demand_history.ship_state`, l'historique par état déjà porté par le schéma.
- `src/ootils_core/db/migrations/065_distribution_links_transfer_multiple.sql`, migration 029 (`distribution_links`) — les lanes de réappro DRP existantes, distinctes du dispatch état→centre à spécifier en PR-F.
- `docs/ADR-042-interface-doctrine.md` — la doctrine TSV-only / `feed_contracts` que les specs PR-F suivront ; le précédent direct « décision pilote datée, défauts 🎯 explicitement marqués placeholders ».
- `docs/ADR-027-streamchanges-sse.md` — le pattern d'event un-par-run repris pour `demand_descended`.
- `C:\Users\ngoin\.claude\plans\giggly-wandering-moon.md` — le plan approuvé (DESC-1), source du séquencement PR-F/A/B/C/D/E.
