# ADR-010 : Ghosts et Tags — Virtualisation et Groupements ad-hoc

**Statut :** PROPOSED — 2026-04-07  
**Auteur :** Architecture (assisté par Claw)  
**Références :** ADR-001 (graph model), ADR-003 (propagation incrémentale), ADR-009 (import pipeline), SPEC-HIERARCHIES, SPEC-GHOSTS-TAGS

---

## Contexte

Le modèle de graphe Ootils (ADR-001) définit des nœuds typés représentant des objets métier physiques ou calculés : Item, Location, ForecastDemand, WorkOrderSupply, ProjectedInventory, Shortage, etc.

Deux besoins émergent qui requièrent des mécanismes de groupement, de natures très différentes :

**Besoin 1 — Gestion de transition produit (NPD/phase-in/phase-out).**  
Lors d'un lancement, la demande migre progressivement d'un item sortant vers un item entrant. Les APS existants gèrent cela avec des "product substitutions" ou des "interchangeability rules" — des mécanismes hors-graphe, opaques pour le moteur, qui produisent des calculs non-auditables. Le planificateur se retrouve à gérer deux projections séparées avec un Excel de transition intercalé.

**Besoin 2 — RCCP (Rough-Cut Capacity Planning) natif.**  
Plusieurs items partagent une ressource contrainte (ligne de production, capacité fournisseur). Pour vérifier que le plan est réalisable, il faut agréger la **charge** (load) de production de ces items et la comparer à la capacité disponible. Les APS existants font ce RCCP dans des modules séparés, déconnectés du graphe de planification — résultat : le planificateur détecte les surcharges trop tard, souvent en exécution.

**Besoin 3 — Groupements transverses légers.**  
Les planificateurs ont besoin de filtrer, annoter et collaborer sur des sous-ensembles ad-hoc d'entités : "tous les items criticité-A", "scénario stress-test Q3", "fournisseurs sous surveillance". Ces groupements ne doivent pas impacter le calcul — ce sont des métadonnées de travail.

---

## Décisions

### D1 — Ghost comme nœud virtuel de premier ordre dans le graphe

Le Ghost est implémenté comme un **nœud typé dans le graphe Ootils**, avec `node_type = 'Ghost'`. Il n'est pas stocké hors-graphe dans une table de configuration séparée.

**Conséquences :**
- Le propagateur (ADR-003) reconnaît nativement les Ghost nodes et applique la logique de distribution/agrégation lors de la traversée du graphe.
- Les edges `ghost_member` relient le nœud Ghost à ses membres dans la table `edges` — la traversée "qui sont les membres de ce Ghost ?" est une requête graph standard.
- La traçabilité causale est native : une demande sur un item B issue d'un Ghost de transition est explicitement reliée au Ghost par un edge, et le Ghost est relié à la demande originelle.
- Les événements (table `events`) peuvent référencer un Ghost comme `trigger_node_id` — les modifications de configuration Ghost génèrent des événements de propagation comme n'importe quel nœud.

**Type `'Ghost'` à ajouter au CHECK constraint de `nodes.node_type` (migration 008).**  
**Type `'ghost_member'` à ajouter au CHECK constraint de `edges.edge_type` (migration 008).**

*Alternative rejetée :* stocker les Ghosts dans une table de configuration indépendante du graphe (`ghost_groups`, `transition_rules`). Rejeté car le propagateur devrait charger une couche de configuration séparée à chaque passe de calcul, rompant l'uniformité du modèle ADR-001 et rendant l'audit causal impossible.

---

### D2 — Deux ghost_types avec contraintes de membership distinctes

Deux types de Ghosts, avec des sémantiques et des contraintes de membership fondamentalement différentes :

#### Table comparative des deux ghost_types

| Dimension | `phase_transition` | `capacity_aggregate` |
|-----------|-------------------|---------------------|
| **Objet supervisé** | Flux de supply (PlannedSupply A et B) | Charge (WorkOrderSupply / PlannedSupply des membres) |
| **Membres** | Exactement 1 `outgoing` + 1 `incoming` | N `member` (N ≥ 1) |
| **Rôle demande** | Indépendante — pilotée par demand planners à item level. Somme A+B ~constante. | Sans objet (pas de demande agrégée) |
| **Output moteur** | Alerte `transition_inconsistency` si ProjectedInventory(A)+ProjectedInventory(B) dévie de la baseline | Alerte surcharge si load agrégé > capacité ressource |
| **Safety stock** | Volume global conservé, redistribué A→B selon la courbe | Par membre |
| **Courbes supportées** | `linear`, `step`, `s_curve` | Sans objet (agrégation brute par période) |

> **Distinctions sémantiques fondamentales :**
> - Un Ghost `phase_transition` **ne distribue pas de demande**. Il surveille la cohérence du flux de supply lors d'une transition produit. La demande est pilotée à item level par les demand planners.
> - Un Ghost `capacity_aggregate` **n'agrège pas de demande**. Le RCCP opère sur la charge (load de production), pas sur la demande brute. La demande est l'input du plan ; la charge est la conséquence du plan de production calculé pour répondre à cette demande. Agréger de la demande pour faire du RCCP serait une erreur conceptuelle — cela confondrait deux niveaux du processus S&OP.

La capacité n'est pas portée par le Ghost — elle est un attribut de la ressource (`Resource`, entité V2). Le Ghost est le lien entre les items membres et leur ressource contrainte partagée.

---

### D3 — Courbes de transition calculées à l'exécution, pas pré-matérialisées

Les poids de transition `weight_A(t)` et `weight_B(t)` sont **calculés dynamiquement lors de chaque passe de propagation**, à partir des paramètres stockés dans `ghost_members` (`transition_start_date`, `transition_end_date`, `transition_curve`, `weight_at_start`, `weight_at_end`).

Aucune table de timeseries pré-calculée (ex : `ghost_transition_weights(ghost_id, date, weight_a, weight_b)`) n'est maintenue.

**Justification :**
- Un changement de courbe (ex : linéaire → s_curve) ou de dates de transition n'invalide qu'un paramètre. Si les poids étaient pré-matérialisés, il faudrait recalculer et remplacer toute la série temporelle — risque de double maintenance et de désynchronisation.
- Le calcul d'interpolation (linear, step, s_curve) est O(1) par date — le coût marginal de calculer à la demande vs lire une table est négligeable.
- La pré-matérialisation crée un état dérivé supplémentaire à maintenir en cohérence avec les paramètres sources. En supply chain planning, les sources de vérité multiples sont une source de bugs opérationnels documentés.

*Alternative rejetée :* table `ghost_weights_materialized` peuplée par un job de pré-calcul sur l'horizon de planification. Rejeté — double maintenance, cohérence fragilisée, complexité opérationnelle non justifiée par le gain de performance.

---

### D4 — Tags comme métadonnées légères sans impact moteur

Les Tags sont implémentés dans deux tables séparées du graphe principal (`tags`, `entity_tags`), sans participation à la propagation, sans FK générique en base de données.

**Principes :**
- **Sans sémantique moteur :** aucun code du propagateur ne lit les tables `tags` ou `entity_tags`. Les Tags sont strictement une fonctionnalité de présentation et de collaboration.
- **Many-to-many polymorphique :** un Tag peut être attaché à tout type d'entité (item, location, supplier, ghost, scenario, node). L'intégrité référentielle est enforced au niveau service.
- **Pas de hiérarchie entre Tags :** les Tags sont des étiquettes plates. La structuration des groupements métier complexes relève des hiérarchies (SPEC-HIERARCHIES), pas des Tags.
- **Idempotence :** attacher deux fois le même Tag à la même entité est une no-op (contrainte UNIQUE sur `entity_tags`).

*Alternative rejetée :* implémenter les Tags comme des HierarchyNodes de dimension `TAG`. Rejeté — ajoute une complexité injustifiée (les Tags n'ont pas de parent/enfant, pas de règles d'agrégation/désagrégation, pas d'impact moteur). Mélanger les deux surchargerait le modèle hiérarchique.

---

### D5 — Ghosts et hiérarchies sont co-dépendants (sequencing)

Un Ghost de type `phase_transition` appartient nécessairement à une famille produit. La planification de la transition A→B est pilotée au niveau de cette famille : le S&OP alloue de la capacité à la famille, et le Ghost est le mécanisme par lequel cette capacité est répartie entre A et B au fil de la transition.

Sans hiérarchie, il est impossible de :
1. Positionner le Ghost dans le plan familial (contribution aux KPIs de niveau supérieur)
2. Définir le contexte capacitaire de la transition (quelle ligne de production est concernée)
3. Appliquer les règles de désagrégation correctement (comment le plan famille descend sur les membres du Ghost)

De même, un Ghost `capacity_aggregate` repose sur une ressource qui appartient à une hiérarchie de capacités.

**Cette décision contredit explicitement la recommandation Sage #001 (parquer les hiérarchies en V2).**

La recommandation Sage #001 traite les hiérarchies comme une fonctionnalité optionnelle d'amélioration de l'interface utilisateur. Cette analyse est incorrecte dans le contexte Ootils : les hiérarchies sont une dépendance structurelle des Ghosts, qui sont eux-mêmes nécessaires pour la gestion NPD et le RCCP — deux cas d'usage cœur du produit.

**Décision :** les hiérarchies (SPEC-HIERARCHIES) et les Ghosts doivent être développés en parallèle ou dans un ordre cohérent. L'implémentation des Ghosts sans modèle hiérarchique disponible produira des Ghosts "orphelins de contexte" — fonctionnels au niveau calcul, mais impossibles à piloter correctement dans un process S&OP. C'est un risque produit documenté ici.

---

## Alternatives rejetées

### Ghost comme "règle de substitution" hors-graphe

Stocker les transitions produit comme des règles de configuration (ex : `substitution_rules(item_from, item_to, start_date, end_date, curve)`) appliquées par le moteur comme un préprocesseur avant la propagation.

**Rejeté** : perd la traçabilité causale (pourquoi B reçoit 65% de la demande ce mois-ci ?), rend l'audit impossible, crée une couche de logique hors-graphe qui rompt ADR-001. C'est exactement ce que font Kinaxis et SAP IBP — et c'est pourquoi leurs transitions sont des boîtes noires pour les planificateurs.

### Timeseries de poids pré-calculée

Table `ghost_transition_weights` peuplée par un job batch, lue par le propagateur.

**Rejeté** : voir D3.

### Tags comme attributs JSON sur les entités

Stocker les tags dans un champ JSONB `tags TEXT[]` sur les tables `items`, `locations`, etc.

**Rejeté** : contre ADR-001 (pas de JSONB pour données structurées), rend les requêtes inter-entités impossibles sans full-table-scan, fragmente les opérations de tag management sur N tables.

---

## Conséquences

### Positives

- **Traçabilité native :** la distribution de demande d'un Ghost phase_transition est un edge dans le graphe — auditable et traversable par les agents.
- **RCCP graph-natif :** la surcharge capacitaire est détectable par traversée de graphe, pas par un module externe.
- **Propagation incrémentale préservée :** les Ghost nodes s'intègrent dans le mécanisme de dirty flags (ADR-003) sans couche spéciale.
- **Flexibilité Tags :** les planificateurs peuvent créer des groupements ad-hoc sans intervention technique.

### Négatives / Points de vigilance

- **Complexité moteur accrue :** le propagateur doit distinguer les ghost_nodes des nodes standards et appliquer la logique de surveillance/agrégation (selon ghost_type). Pour phase_transition : surveillance cohérence supply + émission alertes `transition_inconsistency`. Pour capacity_aggregate : agrégation de charge. Risque de régression si mal isolé.
- **Migration 008 requise :** ajout des tables `ghost_nodes`, `ghost_members`, `tags`, `entity_tags` + mise à jour des CHECK constraints sur `nodes.node_type` et `edges.edge_type`.
- **Entité Resource absente (V2) :** le Ghost `capacity_aggregate` est partiellement opérationnel sans l'entité Resource — la comparaison charge/capacité ne peut pas être automatisée tant que la capacité n'est pas modélisée. Mitigation temporaire : attribut `capacity_override` sur `ghost_nodes` en attendant.
- **Contraintes de membership enforced applicativement :** les règles "exactement 1 outgoing + 1 incoming" ne peuvent pas être exprimées en SQL pur sans trigger. Elles doivent être dans le layer service — à tester explicitement.
- **Co-dépendance hiérarchies :** sans livraison parallèle des hiérarchies, les Ghosts sont sous-utilisables en contexte S&OP (voir D5).

---

## Références

- [ADR-001 — Graph-Based Domain Model](ADR-001-graph-model.md)
- [ADR-003 — Incremental Propagation](ADR-003-incremental-propagation.md)
- [ADR-009 — Import Pipeline](ADR-009-import-pipeline.md)
- [SPEC-HIERARCHIES](SPEC-HIERARCHIES.md)
- [SPEC-GHOSTS-TAGS](SPEC-GHOSTS-TAGS.md)
