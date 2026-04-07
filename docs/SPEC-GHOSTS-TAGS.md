# SPEC-GHOSTS-TAGS — Ghosts et Tags

**Statut :** DRAFT — 2026-04-07  
**Auteur :** Architecture (assisté par Claw)  
**Version :** 0.1  
**Références :** ADR-001, ADR-003, ADR-009, SPEC-HIERARCHIES, ADR-010-ghosts-tags

---

## Table des matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Ghosts](#2-ghosts)
   - [2.1 Définition et cas d'usage](#21-définition-et-cas-dusage)
   - [2.2 Modèle de données](#22-modèle-de-données)
   - [2.3 Règles de propagation](#23-règles-de-propagation)
   - [2.4 Endpoints API](#24-endpoints-api)
   - [2.5 Import](#25-import)
3. [Tags](#3-tags)
   - [3.1 Définition et cas d'usage](#31-définition-et-cas-dusage)
   - [3.2 Modèle de données](#32-modèle-de-données)
   - [3.3 Endpoints API](#33-endpoints-api)
4. [Interactions avec les autres composants](#4-interactions-avec-les-autres-composants)
   - [4.1 Ghosts et hiérarchies](#41-ghosts-et-hiérarchies)
   - [4.2 Ghosts et propagation incrémentale (ADR-003)](#42-ghosts-et-propagation-incrémentale-adr-003)
   - [4.3 Tags et scénarios](#43-tags-et-scénarios)
5. [Points ouverts](#5-points-ouverts)

---

## 1. Vue d'ensemble

Deux mécanismes de groupement coexistent dans Ootils, avec des natures et des impacts moteur fondamentalement différents :

| Concept | Nature | Impact moteur | Persistance |
|---------|--------|---------------|-------------|
| **Ghost** | Nœud virtuel de premier ordre dans le graphe | Oui — participe à la propagation | Table dédiée + edges |
| **Tag** | Métadonnée légère many-to-many | Non — aucun | Table de lookup |

Les **Ghosts** sont des nœuds synthétiques qui participent au calcul de planification. Ils ont deux modes : gestion de transition produit (phase-in/phase-out) et agrégation de charge capacitaire (RCCP). Dans les deux cas, le Ghost est un citoyen de première classe du graphe au sens d'ADR-001.

Les **Tags** sont des étiquettes libres, sans sémantique moteur, destinées au filtrage ad-hoc, à la collaboration et aux focus reviews transverses.

---

## 2. Ghosts

### 2.1 Définition et cas d'usage

Un Ghost est un **nœud virtuel dans le graphe Ootils**. Il n'est pas un item physique planifiable — il n'a ni stock, ni approvisionnement direct, ni localisation physique. Sa fonction est d'encapsuler un groupe d'items réels et de porter une logique de distribution ou d'agrégation que le moteur applique lors de la propagation.

#### Cas d'usage 1 — Phase-in / Phase-out (NPD)

**Problème résolu :** lors d'un lancement produit, la demande migre progressivement d'un item sortant (A) vers un item entrant (B). Sans Ghost, deux projections d'inventaire séparées ne voient pas la transition — le planificateur doit arbitrer manuellement.

**Fonctionnement :**
- Le Ghost est créé dès la décision de lancement, avant même que B existe en stock.
- **La demande n'est pas portée par le Ghost.** Elle existe indépendamment au niveau des items A et B — les demand planners la pilotent à item level. Le Ghost n'interfère pas dans le forecast.
- **Sémantique du Ghost :** il représente le niveau de demande agrégée qu'aurait eu A sans l'introduction de B. La somme A+B reste ~constante dans le temps — c'est une substitution, pas une création ou destruction de demande.
- Des règles de transition définissent la courbe de migration A→B dans le temps (poids A/B à chaque date).
- **Ce que le Ghost pilote réellement : la cohérence du flux de supply**
  - Stock A baisse progressivement (déstockage contrôlé selon la courbe)
  - Stock B monte progressivement (montée en charge)
  - Safety stock : le volume global est conservé, mais se transfère de A vers B selon la courbe de transition
  - Les plans de supply (PlannedSupply) de A et B doivent respecter la courbe
- **Détection d'anomalie :** si `ProjectedInventory(A) + ProjectedInventory(B)` dévie significativement de la projection baseline (ce que A aurait eu seul), le moteur génère une alerte `transition_inconsistency`.
- Le Ghost observe, surveille la cohérence supply, et génère des alertes. Il ne crée pas de nœuds de demande (pas de ForecastDemand produit par le Ghost).

**Bénéfices :**
- Anticipation des besoins supply sur B avant disponibilité stock
- Gestion cohérente de la fin de vie de A (déstockage progressif contrôlé, évite les surstocks)
- Détection automatique d'incohérences supply via alerte `transition_inconsistency`
- Traçabilité : la courbe de transition est explicite et auditable dans le graphe

#### Cas d'usage 2 — Agrégat capacitaire (RCCP graph-native)

**Problème résolu :** plusieurs items partagent une contrainte de production (ligne, machine, fournisseur). Le RCCP (Rough-Cut Capacity Planning) consiste à vérifier que la charge agrégée sur cette ressource ne dépasse pas sa capacité.

**Fonctionnement :**
- Le Ghost consolide la **charge** (load) de ses membres — c'est-à-dire les WorkOrderSupply et PlannedSupply actifs sur ces items, **pas la demande**.
- La ressource (entité `Resource`, à créer en V2) déclare sa capacité maximale (heures, unités, etc.).
- Le moteur compare : charge agrégée du Ghost vs capacité de la ressource liée → détection des surcharges.
- En cas de surcharge, le Ghost devient le point d'entrée pour la désagrégation (repriorisation entre membres).

> **Distinction critique :** le RCCP ne s'applique jamais sur de la demande brute. La demande est ce qu'on cherche à satisfaire. La charge est la conséquence du plan de production décidé pour y répondre. Un Ghost capacity_aggregate agrège des WorkOrderSupply / PlannedSupply — jamais des ForecastDemand ou CustomerOrderDemand.

**Capacité portée par la ressource, pas par le Ghost :**
Le Ghost est une vue agrégée de la charge. La capacité disponible est un attribut de la ressource (ligne de production, fournisseur). Le Ghost est le lien entre les items membres et la ressource commune.

---

### 2.2 Modèle de données

#### Table `ghost_nodes`

Nœud virtuel de planification. Enregistré aussi dans `nodes` avec `node_type = 'Ghost'` pour participation native au graphe.

```sql
-- Migration 008
CREATE TABLE IF NOT EXISTS ghost_nodes (
    ghost_id            UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    name                TEXT        NOT NULL,
    ghost_type          TEXT        NOT NULL
                        CHECK (ghost_type IN ('phase_transition', 'capacity_aggregate')),
    scenario_id         UUID        REFERENCES scenarios(scenario_id),  -- NULL = cross-scenario (master ghost)
    -- node_id : référence au nœud générique dans nodes (pour participation au graphe)
    node_id             UUID        REFERENCES nodes(node_id),
    status              TEXT        NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'archived', 'draft')),
    description         TEXT,
    -- Pour capacity_aggregate : référence à la ressource contrainte (future entité Resource)
    resource_id         UUID,       -- FK vers resources(resource_id) quand disponible (V2)
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

#### Table `ghost_members`

Membres d'un Ghost avec leurs attributs de rôle et de transition.

```sql
CREATE TABLE IF NOT EXISTS ghost_members (
    membership_id           UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    ghost_id                UUID        NOT NULL REFERENCES ghost_nodes(ghost_id) ON DELETE CASCADE,
    item_id                 UUID        NOT NULL REFERENCES items(item_id),
    role                    TEXT        NOT NULL
                            CHECK (role IN ('outgoing', 'incoming', 'member')),
    -- Fenêtre temporelle de la transition (pertinent pour phase_transition)
    transition_start_date   DATE,
    transition_end_date     DATE,
    -- Courbe d'interpolation
    transition_curve        TEXT        NOT NULL DEFAULT 'linear'
                            CHECK (transition_curve IN ('linear', 'step', 's_curve')),
    -- Poids de début et fin de fenêtre (0.0 à 1.0)
    -- Pour phase_transition : weight_at_start = part de la demande au début de la transition
    -- Pour capacity_aggregate : weight fixe = proportion de la charge consolidée (1.0 par défaut)
    weight_at_start         NUMERIC     NOT NULL DEFAULT 1.0
                            CHECK (weight_at_start BETWEEN 0.0 AND 1.0),
    weight_at_end           NUMERIC     NOT NULL DEFAULT 1.0
                            CHECK (weight_at_end BETWEEN 0.0 AND 1.0),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (ghost_id, item_id)
);

-- Contrainte applicative (enforced au niveau service) :
-- ghost_type = 'phase_transition' => exactement 1 member avec role='outgoing' + 1 avec role='incoming'
-- ghost_type = 'capacity_aggregate' => N membres avec role='member', N >= 1
```

#### Contraintes de membership (niveau service)

| ghost_type | Rôles valides | Cardinalité |
|------------|---------------|-------------|
| `phase_transition` | `outgoing`, `incoming` | Exactement 1 outgoing + 1 incoming |
| `capacity_aggregate` | `member` | N ≥ 1 |

Les rôles `outgoing`/`incoming` sont interdits dans un Ghost de type `capacity_aggregate` et inversement. Cette contrainte est enforced au niveau service (pas de CHECK SQL cross-table) — documentée ici comme règle canonique.

#### Edge `ghost_member` dans la table `edges`

Chaque membership génère également un edge dans la table `edges` (ADR-001) pour que le graphe soit traversable nativement :

```
edge_type = 'ghost_member'
from_node_id = ghost_nodes.node_id
to_node_id   = nodes.node_id (du membre)
weight_ratio = weight_at_start (référence initiale ; le poids calculé à t est dynamique)
```

> Le type `'ghost_member'` doit être ajouté au CHECK constraint de la table `edges` dans la migration 008.

---

### 2.3 Règles de propagation

#### Ghost phase_transition — surveillance et cohérence supply

**Rôle :** le Ghost phase_transition est une **couche de surveillance supply**, pas un distributeur de demande. La demande existe indépendamment au niveau des items A et B, pilotée par les demand planners. Le Ghost ne crée aucun ForecastDemand. Il surveille que le flux de supply (stocks, PlannedSupply, safety stocks) reste cohérent avec la courbe de transition.

**Calcul du poids de transition à la date t (référence pour la surveillance supply) :**

**Calcul du poids à la date t :**

Soit `[T_start, T_end]` la fenêtre de transition du membre outgoing (A).

```
Pour t < T_start :
    weight_A(t) = weight_at_start_A  (ex: 1.0 — 100% A avant le début)
    weight_B(t) = 1 - weight_A(t)

Pour t ∈ [T_start, T_end] :
    ratio = (t - T_start) / (T_end - T_start)   ∈ [0, 1]
    
    linear  :  weight_A(t) = weight_at_start_A + ratio × (weight_at_end_A - weight_at_start_A)
    step    :  weight_A(t) = weight_at_start_A  (inchangé jusqu'à T_end, puis bascule)
    s_curve :  weight_A(t) = weight_at_start_A + (weight_at_end_A - weight_at_start_A)
                             × (3ratio² - 2ratio³)   [formule Hermite smoothstep]
    
    weight_B(t) = 1 - weight_A(t)

Pour t > T_end :
    weight_A(t) = weight_at_end_A   (ex: 0.0 — 100% B après la fin)
    weight_B(t) = 1 - weight_A(t)
```

**Invariant :** `weight_A(t) + weight_B(t) = 1.0` à tout instant t.

**Ce que le propagateur surveille :**

```
baseline(t) = ProjectedInventory hypothétique de A sans introduction de B
observed(t) = ProjectedInventory(A, t) + ProjectedInventory(B, t)
delta(t)    = observed(t) - baseline(t)

|delta(t)| > seuil_transition → alerte transition_inconsistency émise
```

**Règles supply surveillées :**
- `PlannedSupply_A(t)` doit décroître conformément à `weight_A(t)`
- `PlannedSupply_B(t)` doit croître conformément à `weight_B(t)`
- `SafetyStock_A(t) + SafetyStock_B(t)` = `SafetyStock_baseline` (volume total conservé, redistribué selon la courbe)

**Résultat moteur :** aucun ForecastDemand créé sur les membres. Le moteur émet des alertes `transition_inconsistency` si la somme des inventaires projetés dévie de la baseline. Les PlannedSupply sont sous la responsabilité du planificateur, guidé par la courbe.

**Les poids ne sont pas pré-matérialisés** — ils sont calculés à la demande lors de chaque passe de propagation (voir ADR-010 D3).

#### Ghost capacity_aggregate — agrégation de charge

**Input :** les WorkOrderSupply et PlannedSupply actifs sur les items membres, pour le scénario courant.

**Calcul :**

```
load_total(ghost, t) = Σ_{item ∈ membres} load(item, t)
```

où `load(item, t)` est la charge (en unités de la ressource — heures, slots, etc.) portée par les WorkOrderSupply / PlannedSupply de cet item sur la période t.

**Comparaison capacitaire :**

```
slack(t) = capacity_resource(t) - load_total(ghost, t)
slack(t) < 0 → surcharge → événement de type 'capacity_overload' émis
```

La capacité `capacity_resource(t)` est lue sur l'entité `Resource` associée (via `ghost_nodes.resource_id`). En attendant l'entité Resource (V2), la capacité peut être portée comme attribut temporaire sur `ghost_nodes`.

**Le Ghost capacity_aggregate ne distribue pas de demande.** Il expose une vue agrégée de charge et détecte les surcharges. La désagrégation (repriorisation inter-membres) est une opération distincte, déclenchée manuellement ou par règle.

---

### 2.4 Endpoints API

#### Endpoints Ghost

| Méthode | Route | Description |
|---------|-------|-------------|
| `POST` | `/v1/ghosts` | Créer un Ghost (type + membres initiaux optionnels) |
| `GET` | `/v1/ghosts` | Lister les Ghosts (filtres : ghost_type, status, scenario_id) |
| `GET` | `/v1/ghosts/{ghost_id}` | Détail d'un Ghost avec ses membres |
| `PATCH` | `/v1/ghosts/{ghost_id}` | Modifier nom, status, description |
| `POST` | `/v1/ghosts/{ghost_id}/members` | Ajouter un membre |
| `DELETE` | `/v1/ghosts/{ghost_id}/members/{membership_id}` | Retirer un membre |

#### Endpoints de consultation — phase_transition

| Méthode | Route | Description |
|---------|-------|-------------|
| `GET` | `/v1/ghosts/{ghost_id}/transition` | Courbe de transition (poids A/B par date), projection agrégée A+B vs baseline, alertes `transition_inconsistency` actives |
| `GET` | `/v1/ghosts/{ghost_id}/alerts` | Alertes `transition_inconsistency` actives pour ce Ghost |

> **Supprimé :** l'endpoint `demand-split` (présent dans des versions antérieures de cette spec) n'existe pas dans ce modèle. La demande est pilotée à item level par les demand planners — le Ghost ne crée pas et ne distribue pas de ForecastDemand. Il n'y a pas de "répartition de demande" à exposer.

**Exemple de réponse `GET /v1/ghosts/{ghost_id}/transition` :**
```json
{
  "ghost_id": "...",
  "ghost_type": "phase_transition",
  "transition_window": {
    "start": "2026-05-01",
    "end": "2026-08-31"
  },
  "curve": [
    { "date": "2026-06-15", "weight_outgoing": 0.65, "weight_incoming": 0.35 }
  ],
  "inventory_projection": [
    {
      "date": "2026-06-15",
      "projected_inventory_a": 420,
      "projected_inventory_b": 580,
      "sum_observed": 1000,
      "baseline": 980,
      "delta": 20,
      "alert": false
    }
  ],
  "active_alerts": []
}
```

#### Endpoints de consultation — capacity_aggregate

| Méthode | Route | Description |
|---------|-------|-------------|
| `GET` | `/v1/ghosts/{ghost_id}/load-summary` | Charge agrégée des membres sur l'horizon (par période) |

> L'endpoint est `load-summary`, pas `demand-split` — la distinction est sémantiquement fondamentale : un capacity_aggregate n'agrège pas de demande, il agrège de la charge (WorkOrderSupply / PlannedSupply).

**Exemple de réponse `load-summary` :**
```json
{
  "ghost_id": "...",
  "ghost_type": "capacity_aggregate",
  "resource_id": "...",
  "periods": [
    {
      "period_start": "2026-06-01",
      "period_end": "2026-06-08",
      "load_total": 420.5,
      "capacity": 480.0,
      "slack": 59.5,
      "overloaded": false,
      "member_breakdown": [
        { "item_id": "...", "load": 180.0 },
        { "item_id": "...", "load": 240.5 }
      ]
    }
  ]
}
```

---

### 2.5 Import

Les Ghosts peuvent être importés via le pipeline staging (ADR-009). Deux entity_types à ajouter au pipeline DQ :

| entity_type | Table cible | Règles DQ notables |
|-------------|-------------|-------------------|
| `ghost_node` | `ghost_nodes` | ghost_type valide, name non vide |
| `ghost_member` | `ghost_members` | ghost_id existant, item_id existant, rôles cohérents avec ghost_type, weights ∈ [0,1], transition_start < transition_end si phase_transition |

**Règle L3 spécifique phase_transition :** à la fin de l'import d'un batch Ghost, vérifier que chaque Ghost de type phase_transition a exactement 1 outgoing + 1 incoming. Si non, issue DQ `GHOST_MEMBERSHIP_INVALID` de sévérité `error`.

---

## 3. Tags

### 3.1 Définition et cas d'usage

Les Tags sont des **étiquettes libres many-to-many** attachables à n'importe quelle entité du graphe Ootils. Ils n'ont aucun impact sur la logique de planification — le moteur les ignore.

**Usages :**

- **Filtrage ad-hoc** : `GET /v1/items?tags=criticité-A,famille-pompe` — vue transverse sans modifier le modèle
- **Focus reviews** : regrouper des items par marque, canal client, technologie, projet pour animer une réunion S&OP
- **Annotation collaborative** : marquer un scénario comme "à présenter au COMEX", un item comme "en investigation qualité"
- **Triage** : `tags=bloqué,urgence-Q2` pour un planificateur qui gère ses priorités

**Ce que les Tags ne sont PAS :**
- Pas une hiérarchie (pas de parent/enfant entre Tags)
- Pas un filtre moteur (le calcul de plan n'utilise jamais les Tags)
- Pas un mécanisme de sécurité / RBAC (pas de droits basés sur les Tags)

---

### 3.2 Modèle de données

#### Table `tags`

```sql
-- Migration 008
CREATE TABLE IF NOT EXISTS tags (
    tag_id          UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT        NOT NULL UNIQUE,
    color           TEXT,       -- ex: '#FF5733' pour affichage UI
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tags_name ON tags (name);
```

#### Table `entity_tags`

```sql
CREATE TABLE IF NOT EXISTS entity_tags (
    entity_tag_id   UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    tag_id          UUID        NOT NULL REFERENCES tags(tag_id) ON DELETE CASCADE,
    entity_type     TEXT        NOT NULL
                    CHECK (entity_type IN (
                        'item', 'location', 'supplier', 'ghost', 'scenario', 'node'
                    )),
    entity_id       UUID        NOT NULL,
    tagged_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    tagged_by       TEXT,       -- user_ref (opaque, pas de FK — pas de user table encore)

    UNIQUE (tag_id, entity_type, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_entity_tags_tag
    ON entity_tags (tag_id);

CREATE INDEX IF NOT EXISTS idx_entity_tags_entity
    ON entity_tags (entity_type, entity_id);
```

> `entity_id` est une UUID sans FK générique — l'intégrité référentielle est enforced au niveau service (vérification de l'existence de l'entité avant attach). Une FK polymorphique en SQL pur n'est pas viable ici sans trigger complexe.

---

### 3.3 Endpoints API

| Méthode | Route | Description |
|---------|-------|-------------|
| `POST` | `/v1/tags` | Créer un tag (name requis, color + description optionnels) |
| `GET` | `/v1/tags` | Lister les tags |
| `GET` | `/v1/tags/{tag_id}` | Détail d'un tag |
| `PATCH` | `/v1/tags/{tag_id}` | Modifier color, description |
| `DELETE` | `/v1/tags/{tag_id}` | Supprimer un tag (cascade sur entity_tags) |
| `POST` | `/v1/tags/{tag_id}/attach` | Attacher à une entité (`entity_type` + `entity_id` dans le body) |
| `DELETE` | `/v1/tags/{tag_id}/detach` | Détacher d'une entité (`entity_type` + `entity_id` dans le body) |
| `GET` | `/v1/tags/{tag_id}/entities` | Lister les entités taggées (avec pagination) |

**Extension future — filtrage par tags sur les endpoints existants :**

```
GET /v1/items?tags=criticité-A,famille-pompe
```

Le filtre est une intersection (AND) des tags listés. Implémentation : jointure sur `entity_tags` filtrée par `entity_type='item'`. À documenter comme extension lors du sprint UI.

---

## 4. Interactions avec les autres composants

### 4.1 Ghosts et hiérarchies (pourquoi les deux sont liés)

Un Ghost de phase_transition appartient à une famille produit. Sans hiérarchie, il est impossible de définir :
- Le **contexte de transition** : quelle famille est concernée, quels sites, quelle capacité fournisseur
- L'**agrégation du Ghost dans le plan familial** : le S&OP raisonne au niveau famille — le Ghost doit être positionnable dans cette hiérarchie pour contribuer aux KPIs de niveau supérieur
- La **règle de désagrégation** : quand le plan famille est désagrégé sur les SKUs, comment le Ghost est-il traité ?

De même, un Ghost capacity_aggregate est associé à une ressource (ligne, fournisseur) qui appartient elle-même à une hiérarchie de capacités (atelier → usine → région).

**Conséquence directe :** la recommandation de parquer les hiérarchies en V2 (Sage #001) est incorrecte dans ce contexte. Les Ghosts et les hiérarchies sont co-dépendants. Voir ADR-010 D5.

### 4.2 Ghosts et propagation incrémentale (ADR-003)

Le propagateur incrémental (ADR-003) doit traiter les `ghost_nodes` comme une couche de virtualisation au-dessus des items réels :

1. **Dirty propagation depuis le Ghost** : si les paramètres de transition d'un Ghost phase_transition sont modifiés (courbe, dates, poids) → le Ghost est marqué dirty → le propagateur recalcule la surveillance de cohérence supply (comparaison ProjectedInventory A+B vs baseline) et peut émettre ou résoudre des alertes `transition_inconsistency`. Pour un Ghost capacity_aggregate, si un WorkOrderSupply d'un membre est modifié → le Ghost est marqué dirty → recalcul de la charge agrégée.

2. **Dirty propagation vers le Ghost** : si un WorkOrderSupply d'un membre est modifié → le Ghost capacity_aggregate associé est marqué dirty (recalcul de la charge agrégée nécessaire).

3. **Isolation scénario** : un Ghost avec `scenario_id != NULL` est scénario-spécifique. Un Ghost avec `scenario_id = NULL` est un Ghost master (transversal à tous les scénarios, comme les items et locations).

### 4.3 Tags et scénarios

Les Tags peuvent être attachés à des scénarios (`entity_type='scenario'`). Cas d'usage :
- Marquer un scénario "présenté S&OP Avril 2026"
- Distinguer les scénarios "optimiste" / "pessimiste" / "stress-test" dans une liste

Les Tags ne participent pas à l'héritage de scénario (quand un scénario fork un parent, ses tags ne sont pas copiés automatiquement — décision explicite de l'utilisateur).

---

## 5. Points ouverts

| # | Question | Décision attendue |
|---|----------|-------------------|
| PO-01 | Un Ghost peut-il lui-même être membre d'un autre Ghost ? (Ghost de Ghosts pour hiérarchie capacitaire à plusieurs niveaux) | À décider avec le PO — risque de complexité moteur |
| PO-02 | ~~Demande portée par le Ghost phase_transition~~ — **question caduque** : le Ghost phase_transition ne porte pas de demande. Demande pilotée à item level par les demand planners. | Clos — voir correction mécanique phase_transition |
| PO-03 | Entité Resource (capacité) : dans quelle migration ? Bloquant pour le RCCP effectif | Dépend de la roadmap V2 |
| PO-04 | Gestion des Tags orphelins (entity supprimée, tag attaché survivant) | Cron de nettoyage ou soft-delete sur les entités ? |
| PO-05 | Un Ghost capacity_aggregate peut-il être attaché à plusieurs ressources (contraintes multiples) ? | Scénario à analyser (ex : ligne + fournisseur simultanément) |
| PO-06 | Conversion d'unité pour la charge (load) : le load est-il toujours dans l'unité de la ressource ? Qui fait la conversion item-UOM → resource-UOM ? | À préciser dans la spec Resource |
