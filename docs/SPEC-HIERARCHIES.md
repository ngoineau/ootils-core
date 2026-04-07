# SPEC-HIERARCHIES — Architecture des Hiérarchies dans Ootils

**Statut :** Draft — Architecture Fondatrice  
**Date :** 2026-04-06  
**Auteur :** Nicolas GOINEAU, avec assistance architecture  
**Version :** 0.1

---

> *Ce document définit l'architecture des hiérarchies dans Ootils. C'est l'un des points de différenciation les plus importants par rapport aux APS existants. Lisez-le comme un manifeste autant que comme une spec.*

---

## Table des matières

1. [Le problème que les APS actuels ne résolvent pas](#1-le-problème-que-les-aps-actuels-ne-résolvent-pas)
2. [Modèle conceptuel universel — l'Hypergraph Hiérarchique](#2-modèle-conceptuel-universel--lhypergraph-hiérarchique)
3. [Intégration avec le graph Ootils existant](#3-intégration-avec-le-graph-ootils-existant)
4. [Agrégation et désagrégation](#4-agrégation-et-désagrégation)
5. [BOM multi-niveaux dans le graph](#5-bom-multi-niveaux-dans-le-graph)
6. [Allocation en cas de shortage](#6-allocation-en-cas-de-shortage)
7. [Flexibilité maximale — configuration sans code](#7-flexibilité-maximale--configuration-sans-code)
8. [DDL PostgreSQL complet](#8-ddl-postgresql-complet)
9. [Comparaison vs Kinaxis / o9 / SAP IBP](#9-comparaison-vs-kinaxis--o9--sap-ibp)
10. [Roadmap d'implémentation](#10-roadmap-dimplémentation)

---

## 1. Le problème que les APS actuels ne résolvent pas

### Le modèle mental dominant : la hiérarchie comme table

Tous les APS existants partagent le même péché originel : **la hiérarchie est codée en dur dans le modèle de données**, sous forme de tables spécialisées.

```
Kinaxis :     PRODUCT_FAMILY → PRODUCT → PART_NUMBER  (3 niveaux, figés)
SAP IBP :     Cubes HANA avec caractéristiques hiérarchiques (MDX-like)
o9 :          Tensor multi-dimensionnel avec axes configurables (mais implémentation excessive)
Blue Yonder : Modèle propriétaire, hiérarchie opaque, black box complète
```

Les conséquences opérationnelles sont prévisibles et toujours les mêmes :

**1. La rigidité structurelle** — Ajouter un niveau dans la hiérarchie produit (ex : segmenter une famille en deux sous-familles) nécessite un projet d'implémentation de 3-6 mois dans Kinaxis. Ce n'est pas une exagération : c'est la norme vécue par des dizaines de grandes entreprises.

**2. Les hiérarchies multiples non supportées** — Un SKU a une hiérarchie commerciale (famille → sous-famille par usage client), une hiérarchie logistique (poids volumétrique → mode de transport), une hiérarchie financière (centre de coût → BU → division). Ces trois hiérarchies sont différentes. Kinaxis vous oblige à en choisir une comme "maître" et à simuler les autres avec des attributs. Résultat : la planification financière et la planification logistique ne vivent jamais dans le même espace.

**3. L'agrégation sans réconciliation** — SAP IBP peut agréger du bas vers le haut. Il peut désagréger du haut vers le bas. Mais gérer les incohérences (le forecast S&OP au niveau famille ≠ la somme des SKUs) relève de l'artisanat Excel interposé entre les deux systèmes.

**4. La BOM comme objet séparé** — Dans tous ces systèmes, la BOM est une structure à part, dans ses propres tables, reliée au monde de la planification par des jointures batch. La BOM n'est pas un citoyen de première classe du graph de planification.

**5. Les hiérarchies temporelles inexistantes** — La réconciliation entre le plan annuel, le S&OP mensuel, et le schedule hebdomadaire est universellement gérée par des meetings, pas par un moteur. Il n't'existe aucun APS qui modélise les frozen zones et les règles de réconciliation inter-horizon comme objets de première classe.

### Ce qu'Ootils doit faire fondamentalement différemment

**Les hiérarchies ne sont pas des tables. Les hiérarchies sont des edges typés dans le graph.**

Un nœud Item ne "connaît pas" sa hiérarchie. Il existe, avec ses attributs. Ce sont les edges qui définissent sa position dans N hiérarchies simultanées. Changer la hiérarchie revient à ajouter, supprimer ou modifier des edges — sans toucher aux nœuds, sans migration de schéma, sans projet.

C'est la thèse centrale de cette spec.

---

## 2. Modèle conceptuel universel — l'Hypergraph Hiérarchique

### 2.1 Le modèle générique

Ootils adopte un **modèle hiérarchique générique** reposant sur deux concepts fondamentaux :

**`HierarchyDef`** — La définition d'une hiérarchie. Pas un objet métier. Une métadonnée.

```
HierarchyDef {
  id:           "hier_product_commercial"
  name:         "Hiérarchie Produit Commerciale"
  dimension:    PRODUCT | LOCATION | CUSTOMER | TIME | ORG
  levels:       ["Famille", "Sous-famille", "Catégorie", "SKU", "Variante"]
  aggregation_rules: { ... }
  disaggregation_rules: { ... }
  version:      3
  valid_from:   "2026-01-01"
  valid_to:     null
}
```

**`hier_member`** edge — Le lien hiérarchique entre deux nœuds dans une hiérarchie donnée.

```
edge {
  edge_type:    "hier_member"
  from_node_id: <node_id du parent>
  to_node_id:   <node_id de l'enfant>
  hierarchy_id: "hier_product_commercial"
  level_parent: "Famille"
  level_child:  "Sous-famille"
  weight:       1.0               # pour désagrégation proportionnelle
  effective_start: "2026-01-01"
  effective_end:   null
}
```

Voilà. C'est tout. Avec ce modèle à deux éléments, on peut représenter n'importe quelle hiérarchie, dans n'importe quelle dimension, avec n'importe quel nombre de niveaux, avec versioning temporel natif.

### 2.2 Les six dimensions hiérarchiques — unifiées dans le même modèle

| Dimension | Nœuds impliqués | Exemples de hiérarchies multiples |
|-----------|-----------------|-----------------------------------|
| **Produit** | `Item` → `HierarchyNode` | Commerciale, Logistique, Financière, Approvisionnement |
| **Location** | `Location` → `HierarchyNode` | Réseau DC, Org financière, Zone transport, Région commerciale |
| **Client/Canal** | `Customer` → `HierarchyNode` | Groupe → Client → Ship-to ; Canal retail/B2B/export |
| **BOM** | `Item` → `Item` (via edges spécialisés) | FG → SF → Composant → MP ; BOM alternatives |
| **Temps** | `TimePeriod` → `TimePeriod` | Année → Trimestre → Mois → Semaine → Jour |
| **Organisation** | `OrgUnit` → `OrgUnit` | BU → Division → Département → Planificateur |

### 2.3 Le nœud HierarchyNode — pour les nœuds synthétiques

Certains nœuds de hiérarchie n'ont pas d'existence physique dans le monde réel. Par exemple, "Pompes industrielles" est une famille produit — ce n'est pas un Item planifiable. C'est un **nœud synthétique d'agrégation**.

```
HierarchyNode {
  node_type:    "HierarchyNode"
  hierarchy_id: "hier_product_commercial"
  level:        "Famille"
  label:        "Pompes industrielles"
  dimension:    PRODUCT
  attributes: {
    "owner":    "user_jean_dupont",
    "forecast_method": "croston"
  }
}
```

Les `Item` SKU planifiables sont liés à ces nœuds via des edges `hier_member`. Les `HierarchyNode` peuvent eux-mêmes être liés entre eux pour former les niveaux supérieurs.

### 2.4 Exemple concret — SKU dans trois hiérarchies simultanées

```
Item: PUMP-01 (Pompe centrifuge 50Hz, DN50)

Hiérarchie Commerciale:
  Équipement industriel → Pompes → Pompes centrifuges → PUMP-01

Hiérarchie Logistique:
  Colis lourd (>30kg) → Palette industrielle → PUMP-01

Hiérarchie Financière:
  BU Fluides → Centre de coût Pompes → PUMP-01

Hiérarchie Approvisionnement:
  Famille achat Composants rotatifs → PUMP-01
```

Chacune de ces relations est un edge `hier_member` avec un `hierarchy_id` différent. PUMP-01 n'a pas bougé. Sa hiérarchie commerciale peut changer (nouvelle sous-famille "Pompes haute performance") sans affecter les trois autres. Zero impact sur le schéma.

### 2.5 Un SKU peut appartenir à plusieurs nœuds parents dans la même hiérarchie

C'est l'un des cas les plus complexes — et que Kinaxis gère mal. Un SKU peut appartenir à deux familles simultanément (produit "crossover" entre gammes).

Ootils le supporte nativement : deux edges `hier_member` du même `hierarchy_id`, pointant vers deux parents différents, avec des `weight` qui définissent la répartition (ex : 60%/40% pour la désagrégation).

```
PUMP-01 → hier_member (hier_commercial, parent: Pompes centrifuges, weight: 0.6)
PUMP-01 → hier_member (hier_commercial, parent: Pompes process, weight: 0.4)
```

---

## 3. Intégration avec le graph Ootils existant

### 3.1 Principe : les hiérarchies sont des citoyens du graph

Le graph Ootils V1 définit 18 types de nœuds et 14 types d'edges (ADR-001). Les hiérarchies ne vivent pas "à côté" du graph — elles sont **dans** le graph.

Nouveaux types de nœuds :

| Type | Rôle |
|------|------|
| `HierarchyNode` | Nœud synthétique d'agrégation (famille, région, canal, etc.) |
| `HierarchyDef` | Métadonnée : définition d'une hiérarchie et de ses règles |
| `BOMLine` | Lien BOM typé (quantité, rendement, substitut) — voir section 5 |
| `TimePeriod` | Période temporelle (mois, semaine, trimestre) pour les hiérarchies temporelles |
| `Customer` | Entité client (Groupe → Client → Ship-to) |
| `OrgUnit` | Unité organisationnelle (BU → Division → Planificateur) |

Nouveaux types d'edges :

| Type | De | Vers | Sémantique |
|------|----|------|------------|
| `hier_member` | HierarchyNode ou entité métier | HierarchyNode ou entité métier | Appartenance hiérarchique |
| `bom_consumes` | Item (parent) | Item (composant) | Relation BOM avec quantité et rendement |
| `bom_substitute` | Item | Item | Substituabilité de composant |
| `bom_coproduct` | Item (process) | Item (co-produit) | Co-production / sous-produit |
| `customer_of` | Ship-to | Client → Groupe | Hiérarchie client |
| `served_by` | Customer/Channel | Location | Quelle location sert quel client |
| `time_child_of` | TimePeriod | TimePeriod | Hiérarchie temporelle (semaine → mois) |
| `org_reports_to` | OrgUnit | OrgUnit | Hiérarchie organisationnelle |
| `owns_plan` | OrgUnit | HierarchyNode ou Location | Ownership du plan |

### 3.2 Propagation dans les hiérarchies

Les hiérarchies participent au moteur de propagation incrémentale (ADR-003). Quand un nœud feuille est dirty (ex : forecast SKU modifié), le moteur sait qu'il doit remonter les agrégats via les edges `hier_member`.

**Règle de propagation pour `hier_member` :**
- Direction bottom-up : enfant dirty → parent agrégé devient dirty
- Direction top-down : parent modifié (plan S&OP) → enfants désagrégés deviennent dirty

**Optimisation clé :** Le moteur ne remonte pas systématiquement toute la hiérarchie. Il utilise les mêmes dirty flags que pour les autres edges. Seuls les nœuds dont un enfant a changé sont invalidés.

### 3.3 Exemple de traversal graph pour un forecast hiérarchique

```
Scénario : le planificateur modifie le forecast famille "Pompes" à 1000 unités en mai.

Traversal engine :
1. Nœud "Pompes" (HierarchyNode) → dirty
2. Engine trouve tous les edges hier_member entrants (enfants de "Pompes")
3. Pour chaque SKU enfant, applique règle de désagrégation (proportionnelle par défaut)
4. ForecastDemand de chaque SKU → dirty
5. ProjectedInventory de chaque SKU → dirty (par edge consumes existant)
6. Shortages potentiels recalculés

Tout cela en un seul traversal incrémental — pas de batch overnight.
```

---

## 4. Agrégation et désagrégation

### 4.1 Architecture des règles — séparation données / logique

Les règles d'agrégation et de désagrégation sont définies dans `HierarchyDef`, pas dans le code. C'est un choix architectural fondamental : **la logique de transformation est une donnée, pas du code**.

```yaml
# Extrait de HierarchyDef.aggregation_rules
aggregation_rules:
  ForecastDemand.qty:
    method: sum
    uom_normalization: true      # convertit les UoMs avant agrégation
    
  ForecastDemand.accuracy:
    method: weighted_average
    weight_field: qty             # pondéré par les volumes
    
  Shortage.severity:
    method: max                   # le pire cas remonte
    
  ProjectedInventory.days_of_cover:
    method: custom
    function: "ootils.agg.weighted_doc"  # fonction Python référencée
```

```yaml
disaggregation_rules:
  ForecastDemand.qty:
    method: proportional
    weight_source: historical_avg_12m   # moyenne des 12 derniers mois
    fallback: equal_split               # si pas d'historique : split égal
    
  ForecastDemand.qty:
    method: historical                  # alternative : répliquer la saisonnalité historique
    
  # Règle custom pour un client spécifique
  ForecastDemand.qty:
    method: custom
    function: "ootils.disagg.market_share_weighted"
```

### 4.2 Méthodes d'agrégation supportées

| Méthode | Formule | Usage typique |
|---------|---------|---------------|
| `sum` | Σ(enfants) | Quantités (forecast, stock, commandes) |
| `weighted_average` | Σ(val × weight) / Σ(weight) | Taux (accuracy, service level) |
| `max` | max(enfants) | Sévérité, risque, criticité |
| `min` | min(enfants) | Capacité contraignante |
| `weighted_max` | max pondéré par volume | Pénurie critique |
| `count` | nb enfants | Nombre de SKUs, de sites |
| `custom` | Python function | Cas spécifiques métier |

### 4.3 Méthodes de désagrégation supportées

| Méthode | Logique | Usage typique |
|---------|---------|---------------|
| `proportional` | Part historique relative de chaque enfant | Désagrégation forecast famille → SKU |
| `historical` | Réplication de la saisonnalité historique exacte | S&OP annuel → mensuel |
| `equal_split` | Parts égales | Cas par défaut sans historique |
| `manual_weights` | Poids définis manuellement par le planificateur | Nouvelles familles, lancements produit |
| `market_share` | Basé sur des données de marché externes | Expansion géographique |
| `custom` | Python function | Cas complexes (promotion, événements) |

### 4.4 Réconciliation algorithmique — l'épineux problème des incohérences

**Le problème :** Le planificateur S&OP valide un forecast famille "Pompes" à 800 unités en mai. Mais la somme des forecasts SKU donne 950 unités. Incohérence = 150 unités. Que fait-on ?

Trois stratégies, configurables dans `HierarchyDef` :

**Stratégie 1 : Top-down wins** (par défaut en S&OP)
Le niveau supérieur est autoritaire. On désagrège le chiffre validé (800) vers les SKUs en respectant les proportions historiques. Les forecasts SKU sont overridés.

**Stratégie 2 : Bottom-up wins** (par défaut en scheduling)
La somme des SKUs est la vérité. Le nœud parent est recalculé à 950. Alerte au planificateur S&OP : son enveloppe est dépassée.

**Stratégie 3 : Reconciliation proportionnelle** (mode consensus)
On applique un ratio de correction : chaque SKU est multiplié par 800/950 = 0.842. Les forecasts SKU sont réduits proportionnellement.

**Stratégie 4 : Réconciliation par contrainte**
Le moteur optimise la désagrégation sous contraintes (MOQ, batch size, capacité) pour maximiser le service level tout en respectant l'enveloppe top-down. C'est la stratégie avancée — V2.

### 4.5 Détection des incohérences inter-niveaux

Ootils génère des nœuds `HierarchyInconsistency` (type de nœud résultat) quand :
- |agrégat calculé - valeur top-down| > seuil configurable
- La désagrégation produit des valeurs négatives
- Un enfant représente > 80% du total (concentration anormale)

Ces incohérences sont des nœuds du graph — elles peuvent être interrogées, expliquées, et assignées à un planificateur (via `owns_plan`).

---

## 5. BOM multi-niveaux dans le graph

### 5.1 Représentation des BOM — choix architectural fondamental

La BOM dans Ootils n'est pas une table séparée. C'est un **sous-graph dans le graph principal**, avec des edges typés `bom_consumes` entre nœuds `Item`.

```
WorkOrderSupply (FG-PUMP-ASSY, 100 units)
    │
    ├── bom_consumes (qty: 1.0, yield: 0.98) ──→ Item: CORPS-POMPE
    │       │
    │       ├── bom_consumes (qty: 2.0, yield: 1.0) ──→ Item: VIS-M8-INOX
    │       └── bom_consumes (qty: 1.0, yield: 0.99) ──→ Item: JOINT-NBR-50
    │
    ├── bom_consumes (qty: 1.0, yield: 0.97) ──→ Item: MOTEUR-0.75KW
    │       │
    │       └── bom_consumes (qty: 1.0, yield: 1.0) ──→ Item: ROULEMENT-6205
    │
    └── bom_consumes (qty: 0.5, yield: 1.0) ──→ Item: GRAISSE-INDUS-1KG
            (0.5 kg par unité — co-consommation)
```

**Structure de l'edge `bom_consumes` :**

```json
{
  "edge_type": "bom_consumes",
  "from_node_id": "<item_parent_id>",
  "to_node_id": "<item_composant_id>",
  "bom_id": "bom_pump_assy_v3",
  "bom_version": 3,
  "effective_start": "2026-01-15",
  "effective_end": null,
  "qty_per_parent": 1.0,
  "uom": "EA",
  "yield_factor": 0.98,
  "scrap_rate": 0.02,
  "lead_time_offset": -5,
  "is_phantom": false,
  "is_configurable": false,
  "attributes": {
    "operation": "OP-10",
    "position": "A1"
  }
}
```

### 5.2 Explosion BOM multi-niveaux — algorithme

L'explosion BOM dans Ootils est une **traversal récursive du graph** via les edges `bom_consumes`. C'est un DFS (Depth-First Search) avec accumulation des quantités nettes.

```python
def explode_bom(item_id: str, qty: float, bom_version: str, level: int = 0) -> List[DependentDemand]:
    """
    Traverse récursivement le graph via edges bom_consumes.
    Retourne la liste des DependentDemand à tous les niveaux.
    """
    demands = []
    
    # Edges bom_consumes sortants depuis item_id
    bom_edges = graph.get_edges(
        from_node=item_id,
        edge_type="bom_consumes",
        bom_id=bom_version,
        active=True
    )
    
    for edge in bom_edges:
        component = edge.to_node
        
        # Gérer les phantom items : traversal transparente
        if component.is_phantom:
            sub_demands = explode_bom(component.item_id, qty * edge.qty_per_parent, bom_version, level)
            demands.extend(sub_demands)
            continue
        
        # Quantité nette avec yield factor
        net_qty = qty * edge.qty_per_parent / edge.yield_factor
        
        # Créer le DependentDemand
        dep_demand = DependentDemand(
            item_id=component.item_id,
            qty=net_qty,
            level=level,
            bom_edge_ref=edge.edge_id,
            lead_time_offset=edge.lead_time_offset
        )
        demands.append(dep_demand)
        
        # Récursion si le composant a lui-même une BOM
        if graph.has_children(component.item_id, "bom_consumes"):
            sub_demands = explode_bom(component.item_id, net_qty, bom_version, level + 1)
            demands.extend(sub_demands)
    
    return demands
```

**Optimisations critiques :**
- **Memoization par item/qty :** Si COMPOSANT-X apparaît dans plusieurs chemins BOM, on ne le calcule qu'une fois et on accumule les quantités (low-level code principle).
- **Low-level code (LLC) :** Ootils calcule et stocke le niveau BOM le plus bas de chaque item. L'explosion se fait en ordre LLC croissant pour éviter les recalculs.
- **Dirty flags BOM :** Quand un edge `bom_consumes` est modifié (nouveau rendement, changement de quantité), tous les items parents dans la hiérarchie BOM remontante sont marqués dirty.

### 5.3 BOM alternatives — substituts de composants

```json
{
  "edge_type": "bom_substitute",
  "from_node_id": "<item_composant_alternatif_id>",
  "to_node_id": "<item_composant_primaire_id>",
  "bom_id": "bom_pump_assy_v3",
  "substitution_ratio": 1.05,
  "priority": 2,
  "conditions": {
    "min_shortage_qty": 10,
    "max_lead_time_delta": 3,
    "approval_required": false
  },
  "valid_locations": ["DC-ATL", "DC-CHI"],
  "attributes": {}
}
```

L'algorithme d'explosion évalue les substituts quand :
1. Le composant primaire est en shortage (nœud `Shortage` existant dans le graph)
2. Le substitut satisfait les conditions définies dans l'edge
3. Le planificateur a autorisé la substitution automatique (Policy)

### 5.4 Phantom items

Un item phantom est un assemblage intermédiaire qui n'est jamais stocké physiquement — il est "transparent" dans le calcul MRP.

Implémentation dans Ootils : `is_phantom: true` sur le nœud `Item`. L'algorithme d'explosion BOM traverse le phantom sans créer de `DependentDemand` pour lui — il propage directement aux enfants du phantom avec les quantités consolidées.

```
FG-PUMP → [phantom: KIT-ETANCHEITE] → JOINT-A, JOINT-B, RONDELLE-C
                                       ↑
                    Explosion directe vers ces trois composants
                    KIT-ETANCHEITE n'apparaît jamais dans les besoins
```

### 5.5 Co-produits et sous-produits (process industry)

```json
{
  "edge_type": "bom_coproduct",
  "from_node_id": "<item_process_id>",
  "to_node_id": "<item_coproduct_id>",
  "co_type": "coproduct | byproduct | waste",
  "yield_ratio": 0.15,
  "cost_allocation_pct": 0.08,
  "is_plannable": true,
  "attributes": {}
}
```

Les co-produits sont des nœuds `Item` à part entière dans le graph. Leur production est liée à la production du process parent via `bom_coproduct`. Le moteur les inclut dans le calcul de l'inventory projeté (ils augmentent le stock) et dans les calculs de coût (allocation du coût de production).

### 5.6 Versioning des BOM

Chaque edge `bom_consumes` porte un `bom_version` et des dates `effective_start`/`effective_end`. Le moteur sélectionne automatiquement la version de BOM active à la date de besoin (date de l'ordre de fabrication).

Cela permet de gérer nativement :
- Les changements d'ingénierie (ECN — Engineering Change Notice)
- Les BOM alternatives par site (même FG, composants différents selon la factory)
- Les BOM temporaires (substitution pendant une pénurie, BOM promotion)

---

## 6. Allocation en cas de shortage

### 6.1 Principe — l'allocation comme traversal de graph prioritaire

Quand le moteur détecte une pénurie (supply disponible < demande totale), il doit décider qui est servi et qui ne l'est pas. C'est le problème d'allocation.

Dans Ootils, l'allocation est une **traversal de graph ordonnée par priorité**, pilotée par la hiérarchie client/canal et les Policy nodes.

### 6.2 La hiérarchie client/canal comme discriminant d'allocation

```
Groupe client (ex: Groupe Schneider)
    └── Client (ex: Schneider France SAS)
            └── Ship-to (ex: Grenoble - Site 1)
                    └── Ligne de commande (ex: CO-8821-L1)
```

Chaque niveau de cette hiérarchie porte des attributs d'allocation :

```json
{
  "node_type": "Customer",
  "customer_id": "schneider-fr",
  "allocation_class": "A",
  "allocation_priority": 10,
  "channel": "B2B",
  "contract_type": "framework",
  "min_service_level": 0.98,
  "shortage_rules": {
    "partial_shipment_allowed": true,
    "substitution_allowed": false,
    "backorder_allowed": true
  }
}
```

Le canal (`B2B`, `retail`, `e-commerce`, `export`) est l'attribut le plus important pour les règles de priorisation inter-clients.

### 6.3 Les trois modes d'allocation

**Mode 1 : Priority Rules (par défaut)**

Ordre de service strict par priorité. Les demandes de priorité 1 sont servies en totalité avant de passer à la priorité 2.

```
Algorithme :
1. Trier toutes les demandes par (priority, due_date, order_date)
2. Pour chaque demande dans l'ordre :
   a. Allouer le minimum de (qty_demanded, supply_available)
   b. Créer edge pegged_to (supply → demand, qty=allocated)
   c. Réduire supply_available
3. Les demandes non servies → Shortage nodes
```

**Mode 2 : Fair Share**

Répartition proportionnelle du supply disponible entre toutes les demandes de même niveau de priorité.

```
Algorithme :
1. Grouper les demandes par niveau de priorité
2. Pour chaque groupe de même priorité :
   a. ratio = supply_available / sum(qty_demanded dans le groupe)
   b. Si ratio >= 1 : tout le monde est servi, passer au groupe suivant
   c. Si ratio < 1 : chaque demande reçoit qty × ratio (fair share)
3. Mode fair_share_min : appliquer un minimum par demande avant fair share
```

**Mode 3 : ATP (Available-to-Promise)**

Allocation séquentielle dans le temps avec recalcul continu. Chaque commande reçoit une date de livraison possible basée sur le supply projeté.

```
Algorithme ATP :
1. Construire la courbe supply projetée cumulée dans le temps
2. Pour chaque commande (triée par priorité et date demandée) :
   a. Trouver la première date où supply_cumul >= qty_needed
   b. Assigner cette date comme ATP date
   c. Décrémenter le supply projeté
3. Générer des edges pegged_to avec la date ATP (peut ≠ date demandée)
```

### 6.4 Configuration des règles d'allocation par canal

```yaml
# Policy node : allocation_policy_v2
allocation_policy:
  name: "Standard Allocation Policy 2026"
  channels:
    B2B_framework:
      priority: 1
      mode: priority_rules
      min_service_level: 0.98
      partial_shipment: true
      
    retail_key_account:
      priority: 2
      mode: fair_share
      min_fill_rate: 0.85
      
    e-commerce:
      priority: 3
      mode: atp
      lead_time_commitment_days: 2
      
    export:
      priority: 4
      mode: priority_rules
      substitution_allowed: true
      
  tiebreak_rules:
    - field: due_date
      direction: asc
    - field: order_value
      direction: desc
    - field: customer_seniority_years
      direction: desc
```

### 6.5 Allocation optimale avec contraintes multiples — V2

L'allocation multi-contrainte (shortage simultanée sur plusieurs items, plusieurs locations, avec des demandes qui peuvent être satisfaites partiellement ou replanifiées) est un problème d'optimisation combinatoire.

Ootils V2 intègre un solveur LP/MILP (PuLP ou OR-Tools) pour cette allocation. La formulation :

```
Maximiser : Σ (service_level × priority_weight × qty_allocated)
Sous contraintes :
  - qty_allocated[demand_i] ≤ qty_demanded[demand_i]  (ne pas sur-livrer)
  - Σ qty_allocated[demand consomme supply_j] ≤ supply_available[j]  (ne pas sur-allouer)
  - qty_allocated[demand_i] ≥ min_lot_size si qty > 0  (MOQ)
  - qty_allocated ∈ ℤ+  (entiers positifs)
```

Le graph Ootils fournit exactement la structure de données nécessaire à cette formulation.

---

## 7. Flexibilité maximale — configuration sans code

### 7.1 Philosophie : la hiérarchie comme donnée, pas comme schéma

**Règle d'or :** Aucune hiérarchie métier ne doit nécessiter une migration de schéma SQL ou un déploiement de code pour être créée, modifiée, ou supprimée.

La flexibilité est obtenue par trois mécanismes :
1. **Schéma générique** — les tables `hierarchy_def`, `hierarchy_nodes`, `graph_edges` supportent n'importe quelle hiérarchie
2. **Configuration YAML/JSON** — les règles d'agrégation/désagrégation sont des données
3. **API REST** — toute la gestion des hiérarchies est exposée via API, pas via migration

### 7.2 Définir une nouvelle hiérarchie — via API

```bash
# Créer une nouvelle hiérarchie
POST /api/v1/hierarchies
{
  "name": "Hiérarchie ABC-XYZ",
  "dimension": "PRODUCT",
  "levels": ["Classe ABC", "Classe XYZ", "SKU"],
  "aggregation_rules": {
    "ForecastDemand.qty": {"method": "sum"},
    "Shortage.severity": {"method": "max"}
  },
  "disaggregation_rules": {
    "ForecastDemand.qty": {"method": "proportional", "weight_source": "historical_avg_6m"}
  }
}

# Assigner un Item à cette hiérarchie
POST /api/v1/hierarchies/{hier_id}/members
{
  "child_node_id": "item_pump01",
  "parent_node_id": "hiernode_abc_a_xyz_x",
  "effective_start": "2026-04-01"
}
```

### 7.3 Versioning temporel des hiérarchies

Chaque edge `hier_member` porte `effective_start` et `effective_end`. La `HierarchyDef` elle-même est versionnée.

```
Cas d'usage : réorganisation commerciale au 1er juillet 2026
  - Avant : PUMP-01 est dans "Pompes centrifuges"
  - Après : PUMP-01 migre vers "Pompes process industriel"

Action :
  PATCH /api/v1/hierarchies/members/{member_id}
  { "effective_end": "2026-06-30" }
  
  POST /api/v1/hierarchies/members
  { "child": "item_pump01", "parent": "hiernode_pompes_process", "effective_start": "2026-07-01" }

Résultat :
  - Les plans historiques (avant juillet) conservent l'ancienne hiérarchie
  - Les plans futurs (après juillet) utilisent la nouvelle
  - Zero migration SQL
```

### 7.4 Hiérarchies configurables via YAML — import batch

Pour les implémentations initiales (chargement d'un modèle de données client), Ootils supporte l'import d'hiérarchies via YAML :

```yaml
# fichier: hierarchies/product_commercial.yaml
hierarchy:
  id: hier_product_commercial
  name: "Hiérarchie Produit Commerciale"
  dimension: PRODUCT
  levels:
    - name: Famille
      level_no: 1
    - name: Sous-famille
      level_no: 2
    - name: Catégorie
      level_no: 3
    - name: SKU
      level_no: 4

nodes:
  - id: fam_pompes
    label: "Pompes industrielles"
    level: Famille
    
  - id: sfam_centrifuges
    label: "Pompes centrifuges"
    level: Sous-famille
    parent: fam_pompes

members:
  - child: item_pump01
    parent: sfam_centrifuges
    effective_start: "2026-01-01"
    weight: 0.35
    
  - child: item_pump02
    parent: sfam_centrifuges
    effective_start: "2026-01-01"
    weight: 0.65
```

### 7.5 UI de gestion des hiérarchies — vision

L'interface planificateur (V2) expose un drag-and-drop visuel de la hiérarchie. Déplacer un SKU d'une famille à une autre génère les appels API correspondants, avec gestion des dates effectives. Les planificateurs peuvent configurer leurs hiérarchies sans DBA, sans ticket IT, sans projet.

C'est une rupture radicale avec le modèle Kinaxis.

---

## 8. DDL PostgreSQL complet

```sql
-- ============================================================
-- OOTILS — DDL HIÉRARCHIES V1
-- PostgreSQL 16
-- ============================================================

-- Extensions requises
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "ltree";       -- pour les paths hiérarchiques
CREATE EXTENSION IF NOT EXISTS "pg_trgm";     -- pour la recherche full-text sur labels


-- ============================================================
-- 1. DÉFINITIONS DE HIÉRARCHIES
-- ============================================================

CREATE TYPE hierarchy_dimension AS ENUM (
    'PRODUCT',
    'LOCATION',
    'CUSTOMER',
    'TIME',
    'ORG',
    'BOM'
);

CREATE TABLE hierarchy_def (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code                VARCHAR(100) UNIQUE NOT NULL,
    name                VARCHAR(255) NOT NULL,
    dimension           hierarchy_dimension NOT NULL,
    levels              JSONB NOT NULL DEFAULT '[]',
    -- ex: [{"level_no": 1, "name": "Famille"}, {"level_no": 2, "name": "SKU"}]
    aggregation_rules   JSONB NOT NULL DEFAULT '{}',
    disaggregation_rules JSONB NOT NULL DEFAULT '{}',
    version             INTEGER NOT NULL DEFAULT 1,
    valid_from          DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to            DATE,
    is_primary          BOOLEAN NOT NULL DEFAULT FALSE,
    -- hiérarchie "maître" pour une dimension (une seule par dimension)
    metadata            JSONB NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by          VARCHAR(100),
    
    CONSTRAINT valid_dates CHECK (valid_to IS NULL OR valid_to > valid_from)
);

CREATE INDEX idx_hier_def_dimension ON hierarchy_def (dimension);
CREATE INDEX idx_hier_def_valid ON hierarchy_def (valid_from, valid_to);

-- Version history des hiérarchies (append-only)
CREATE TABLE hierarchy_def_history (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hierarchy_id    UUID NOT NULL REFERENCES hierarchy_def(id),
    version         INTEGER NOT NULL,
    snapshot        JSONB NOT NULL,   -- snapshot complet de hierarchy_def à ce moment
    changed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    changed_by      VARCHAR(100),
    change_reason   TEXT
);


-- ============================================================
-- 2. NŒUDS DE HIÉRARCHIE (nœuds synthétiques d'agrégation)
-- ============================================================

CREATE TABLE hierarchy_nodes (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hierarchy_id    UUID NOT NULL REFERENCES hierarchy_def(id) ON DELETE CASCADE,
    node_code       VARCHAR(200) NOT NULL,
    label           VARCHAR(500) NOT NULL,
    level_name      VARCHAR(100) NOT NULL,
    level_no        INTEGER NOT NULL,
    path            LTREE,           -- ex: famille.sous_famille.categorie (pour queries hiérarchiques rapides)
    attributes      JSONB NOT NULL DEFAULT '{}',
    is_leaf         BOOLEAN NOT NULL DEFAULT FALSE,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT uq_hiernode_code UNIQUE (hierarchy_id, node_code)
);

CREATE INDEX idx_hiernode_hierarchy ON hierarchy_nodes (hierarchy_id);
CREATE INDEX idx_hiernode_level ON hierarchy_nodes (hierarchy_id, level_no);
CREATE INDEX idx_hiernode_path ON hierarchy_nodes USING GIST (path);
CREATE INDEX idx_hiernode_label_trgm ON hierarchy_nodes USING GIN (label gin_trgm_ops);


-- ============================================================
-- 3. LIENS HIÉRARCHIQUES (edges hier_member — dans graph_edges existant)
-- ============================================================

-- Note : les edges hier_member sont stockés dans la table graph_edges générale
-- d'Ootils V1. On ajoute ici une vue matérialisée spécialisée pour les
-- requêtes hiérarchiques fréquentes.

-- Vue matérialisée : closure table pour requêtes ancêtres/descendants rapides
CREATE TABLE hierarchy_closure (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hierarchy_id    UUID NOT NULL REFERENCES hierarchy_def(id) ON DELETE CASCADE,
    ancestor_id     UUID NOT NULL,   -- node_id ou business entity_id
    descendant_id   UUID NOT NULL,   -- node_id ou business entity_id
    depth           INTEGER NOT NULL DEFAULT 0,
    path_array      UUID[] NOT NULL DEFAULT '{}',
    -- agrégé dans les deux sens pour requêtes rapides
    
    CONSTRAINT uq_closure UNIQUE (hierarchy_id, ancestor_id, descendant_id)
);

CREATE INDEX idx_closure_ancestor ON hierarchy_closure (hierarchy_id, ancestor_id);
CREATE INDEX idx_closure_descendant ON hierarchy_closure (hierarchy_id, descendant_id);
CREATE INDEX idx_closure_depth ON hierarchy_closure (hierarchy_id, depth);


-- ============================================================
-- 4. MEMBRES DE HIÉRARCHIE (appartenance d'une entité métier à une hiérarchie)
-- ============================================================

CREATE TABLE hierarchy_members (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hierarchy_id    UUID NOT NULL REFERENCES hierarchy_def(id) ON DELETE CASCADE,
    
    -- L'entité enfant (peut être item_id, location_id, customer_id, etc.)
    child_entity_type   VARCHAR(50) NOT NULL,   -- 'item', 'location', 'customer', 'orgunit'
    child_entity_id     UUID NOT NULL,
    
    -- Le nœud parent dans la hiérarchie
    parent_node_id      UUID REFERENCES hierarchy_nodes(id),
    -- OU un autre member (pour les membres qui sont eux-mêmes parents d'autres membres)
    parent_member_id    UUID REFERENCES hierarchy_members(id),
    
    -- Position et poids
    level_name          VARCHAR(100) NOT NULL,
    level_no            INTEGER NOT NULL,
    weight              NUMERIC(10, 6) NOT NULL DEFAULT 1.0,
    -- poids pour désagrégation proportionnelle (doit sommer à 1.0 par parent)
    
    -- Validité temporelle
    effective_start     DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_end       DATE,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Multi-parent support (un enfant peut avoir plusieurs parents dans la même hiérarchie)
    is_primary_parent   BOOLEAN NOT NULL DEFAULT TRUE,
    
    metadata            JSONB NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by          VARCHAR(100),
    
    CONSTRAINT valid_member_dates CHECK (effective_end IS NULL OR effective_end > effective_start),
    CONSTRAINT check_parent_xor CHECK (
        (parent_node_id IS NOT NULL AND parent_member_id IS NULL) OR
        (parent_node_id IS NULL AND parent_member_id IS NOT NULL)
    )
);

CREATE INDEX idx_member_hierarchy ON hierarchy_members (hierarchy_id);
CREATE INDEX idx_member_child ON hierarchy_members (child_entity_type, child_entity_id);
CREATE INDEX idx_member_parent_node ON hierarchy_members (parent_node_id);
CREATE INDEX idx_member_effective ON hierarchy_members (effective_start, effective_end);
CREATE INDEX idx_member_active ON hierarchy_members (hierarchy_id, is_active) WHERE is_active = TRUE;


-- ============================================================
-- 5. BOM (Bill of Materials)
-- ============================================================

CREATE TYPE bom_type AS ENUM (
    'manufacturing',    -- BOM production standard
    'engineering',      -- BOM de conception (pas directement planifiable)
    'sales',            -- BOM configurateur produit
    'costing'           -- BOM coût (peut différer de la BOM prod)
);

CREATE TABLE bom_header (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bom_code        VARCHAR(200) UNIQUE NOT NULL,
    parent_item_id  UUID NOT NULL,   -- référence vers graph_nodes (item)
    bom_type        bom_type NOT NULL DEFAULT 'manufacturing',
    version         INTEGER NOT NULL DEFAULT 1,
    effective_start DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_end   DATE,
    location_id     UUID,            -- BOM spécifique à un site (nullable = toutes locations)
    base_qty        NUMERIC(18, 6) NOT NULL DEFAULT 1.0,
    base_uom        VARCHAR(20) NOT NULL DEFAULT 'EA',
    is_primary      BOOLEAN NOT NULL DEFAULT TRUE,
    status          VARCHAR(20) NOT NULL DEFAULT 'active',
    attributes      JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by      VARCHAR(100),
    
    CONSTRAINT valid_bom_dates CHECK (effective_end IS NULL OR effective_end > effective_start)
);

CREATE INDEX idx_bom_parent ON bom_header (parent_item_id);
CREATE INDEX idx_bom_effective ON bom_header (effective_start, effective_end);
CREATE INDEX idx_bom_location ON bom_header (location_id) WHERE location_id IS NOT NULL;

CREATE TYPE bom_component_type AS ENUM (
    'standard',     -- composant normal
    'phantom',      -- phantom item (transparent dans MRP)
    'configurable', -- composant variable selon la configuration
    'coproduct',    -- co-produit (produit aussi lors du process)
    'byproduct',    -- sous-produit
    'waste'         -- déchet ou rebut mesurable
);

CREATE TABLE bom_lines (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bom_id              UUID NOT NULL REFERENCES bom_header(id) ON DELETE CASCADE,
    component_item_id   UUID NOT NULL,       -- référence vers graph_nodes (item)
    component_type      bom_component_type NOT NULL DEFAULT 'standard',
    
    -- Quantité et rendement
    qty_per_parent      NUMERIC(18, 6) NOT NULL,
    uom                 VARCHAR(20) NOT NULL,
    yield_factor        NUMERIC(8, 6) NOT NULL DEFAULT 1.0,  -- 0.98 = 2% de perte
    scrap_rate          NUMERIC(8, 6) NOT NULL DEFAULT 0.0,
    -- qty_nette = qty_per_parent / yield_factor * (1 + scrap_rate)
    
    -- Timing
    lead_time_offset    INTEGER NOT NULL DEFAULT 0,  -- jours avant/après la date FG
    operation_seq       INTEGER,                      -- séquence d'opération dans le routage
    
    -- Substituts
    can_substitute      BOOLEAN NOT NULL DEFAULT FALSE,
    substitute_group_id UUID,   -- groupe de substitution (plusieurs substituts possibles)
    
    -- Fantôme
    is_phantom          BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Co-produit
    cost_allocation_pct NUMERIC(6, 4),   -- % du coût alloué à ce co-produit
    
    -- Validité
    effective_start     DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_end       DATE,
    
    -- Positionnement
    reference_designator VARCHAR(50),   -- ex: "R12", "C45" dans une BOM électronique
    position            VARCHAR(50),
    
    attributes          JSONB NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT valid_bomline_dates CHECK (effective_end IS NULL OR effective_end > effective_start),
    CONSTRAINT positive_qty CHECK (qty_per_parent > 0),
    CONSTRAINT valid_yield CHECK (yield_factor > 0 AND yield_factor <= 1.0)
);

CREATE INDEX idx_bomline_bom ON bom_lines (bom_id);
CREATE INDEX idx_bomline_component ON bom_lines (component_item_id);
CREATE INDEX idx_bomline_effective ON bom_lines (effective_start, effective_end);
CREATE INDEX idx_bomline_phantom ON bom_lines (bom_id) WHERE is_phantom = TRUE;

-- Low-Level Code (LLC) — calculé et stocké pour optimiser l'explosion BOM
CREATE TABLE item_low_level_code (
    item_id         UUID PRIMARY KEY,
    llc             INTEGER NOT NULL DEFAULT 0,
    -- 0 = niveau le plus haut (FG), MAX = matière première
    last_computed   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_stale        BOOLEAN NOT NULL DEFAULT FALSE
);

-- Table des substituts de composants
CREATE TABLE bom_substitutes (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    substitute_group_id     UUID NOT NULL,
    bom_line_id             UUID NOT NULL REFERENCES bom_lines(id) ON DELETE CASCADE,
    primary_component_id    UUID NOT NULL,   -- composant qu'on remplace
    substitute_item_id      UUID NOT NULL,   -- composant de remplacement
    substitution_ratio      NUMERIC(10, 6) NOT NULL DEFAULT 1.0,
    -- ex: 1.05 = besoin de 5% de plus du substitut
    priority                INTEGER NOT NULL DEFAULT 1,
    -- 1 = premier choix si pénurie
    conditions              JSONB NOT NULL DEFAULT '{}',
    -- {"min_shortage_qty": 10, "max_lead_time_delta": 5, "approval_required": false}
    valid_locations         UUID[] DEFAULT NULL,   -- NULL = valable partout
    effective_start         DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_end           DATE,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_substitute_group ON bom_substitutes (substitute_group_id);
CREATE INDEX idx_substitute_primary ON bom_substitutes (primary_component_id);
CREATE INDEX idx_substitute_item ON bom_substitutes (substitute_item_id);


-- ============================================================
-- 6. HIÉRARCHIES CLIENT / CANAL
-- ============================================================

CREATE TYPE customer_channel AS ENUM (
    'retail',
    'key_account',
    'e-commerce',
    'B2B',
    'B2B_framework',
    'export',
    'intercompany',
    'direct'
);

CREATE TABLE customers (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_code       VARCHAR(200) UNIQUE NOT NULL,
    customer_name       VARCHAR(500) NOT NULL,
    customer_type       VARCHAR(50) NOT NULL DEFAULT 'customer',
    -- 'group', 'customer', 'ship_to', 'bill_to'
    parent_customer_id  UUID REFERENCES customers(id),
    channel             customer_channel,
    
    -- Allocation
    allocation_class    VARCHAR(10),    -- A, B, C
    allocation_priority INTEGER NOT NULL DEFAULT 100,
    -- 1 = plus haute priorité
    min_service_level   NUMERIC(5, 4),  -- ex: 0.98 = 98%
    
    -- Règles shortage
    partial_shipment_allowed    BOOLEAN NOT NULL DEFAULT TRUE,
    substitution_allowed        BOOLEAN NOT NULL DEFAULT FALSE,
    backorder_allowed           BOOLEAN NOT NULL DEFAULT TRUE,
    backorder_max_days          INTEGER,
    
    -- Locations de livraison par défaut
    default_ship_from_location_id   UUID,
    
    attributes          JSONB NOT NULL DEFAULT '{}',
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_customer_parent ON customers (parent_customer_id);
CREATE INDEX idx_customer_channel ON customers (channel);
CREATE INDEX idx_customer_priority ON customers (allocation_priority);


-- ============================================================
-- 7. HIÉRARCHIES TEMPORELLES
-- ============================================================

CREATE TYPE time_grain_type AS ENUM (
    'year', 'half_year', 'quarter', 'month', 'week', 'day', 'shift', 'hour'
);

CREATE TABLE time_periods (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    period_code     VARCHAR(50) UNIQUE NOT NULL,   -- ex: '2026-W14', '2026-04', '2026-Q2'
    grain           time_grain_type NOT NULL,
    start_date      DATE NOT NULL,
    end_date        DATE NOT NULL,
    parent_period_id UUID REFERENCES time_periods(id),
    calendar_id     UUID,   -- référence vers calendrier business
    is_frozen       BOOLEAN NOT NULL DEFAULT FALSE,
    -- frozen = on ne modifie plus le plan sur cet horizon
    freeze_level    VARCHAR(50),
    -- 'schedule', 'production', 'procurement' — quel niveau est gelé
    
    attributes      JSONB NOT NULL DEFAULT '{}',
    
    CONSTRAINT valid_period CHECK (end_date >= start_date)
);

CREATE INDEX idx_period_grain ON time_periods (grain);
CREATE INDEX idx_period_parent ON time_periods (parent_period_id);
CREATE INDEX idx_period_dates ON time_periods (start_date, end_date);
CREATE INDEX idx_period_frozen ON time_periods (is_frozen) WHERE is_frozen = TRUE;

-- Règles de réconciliation inter-horizons
CREATE TABLE horizon_reconciliation_rules (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    from_grain          time_grain_type NOT NULL,
    to_grain            time_grain_type NOT NULL,
    -- ex: from month to week (désagrégation S&OP → scheduling)
    disagg_method       VARCHAR(50) NOT NULL DEFAULT 'proportional',
    frozen_zone_grain   time_grain_type,
    -- au-dessous de ce grain, le plan est gelé
    frozen_zone_periods INTEGER,
    -- nombre de périodes gelées depuis aujourd'hui
    override_allowed    BOOLEAN NOT NULL DEFAULT FALSE,
    approval_required   BOOLEAN NOT NULL DEFAULT TRUE,
    attributes          JSONB NOT NULL DEFAULT '{}',
    
    CONSTRAINT uq_reconciliation UNIQUE (from_grain, to_grain)
);


-- ============================================================
-- 8. HIÉRARCHIES ORGANISATIONNELLES
-- ============================================================

CREATE TABLE org_units (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_code        VARCHAR(200) UNIQUE NOT NULL,
    org_name        VARCHAR(500) NOT NULL,
    org_type        VARCHAR(50) NOT NULL,
    -- 'company', 'BU', 'division', 'department', 'team', 'planner'
    parent_org_id   UUID REFERENCES org_units(id),
    
    -- Responsabilités planning
    owns_items      UUID[],         -- items dont cette org est propriétaire
    owns_locations  UUID[],         -- locations gérées
    owns_hierarchy_nodes UUID[],    -- nœuds de hiérarchie gérés
    
    -- Droits d'approbation
    can_approve_forecast_change NUMERIC(18, 2),
    -- seuil en $ au-dessus duquel l'approbation est requise (NULL = pas de limite)
    can_approve_supply_change   NUMERIC(18, 2),
    can_release_orders          BOOLEAN NOT NULL DEFAULT FALSE,
    can_override_frozen_zone    BOOLEAN NOT NULL DEFAULT FALSE,
    
    attributes      JSONB NOT NULL DEFAULT '{}',
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_org_parent ON org_units (parent_org_id);
CREATE INDEX idx_org_type ON org_units (org_type);


-- ============================================================
-- 9. OWNERSHIP ET DROITS SUR LES PLANS
-- ============================================================

CREATE TABLE plan_ownership (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_unit_id     UUID NOT NULL REFERENCES org_units(id),
    
    -- Ce que cette org possède (scope du plan)
    scope_type      VARCHAR(50) NOT NULL,
    -- 'hierarchy_node', 'item', 'location', 'customer', 'all'
    scope_entity_id UUID,           -- NULL si scope_type = 'all'
    hierarchy_id    UUID REFERENCES hierarchy_def(id),
    -- pour les scopes de type hierarchy_node
    
    -- Type de plan possédé
    plan_type       VARCHAR(50) NOT NULL,
    -- 'forecast', 'supply', 'capacity', 'allocation', 'all'
    
    -- Horizon de responsabilité
    horizon_start   DATE,
    horizon_end     DATE,
    
    -- Workflow d'approbation
    requires_approval   BOOLEAN NOT NULL DEFAULT FALSE,
    approver_org_id     UUID REFERENCES org_units(id),
    
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    effective_start DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_end   DATE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ownership_org ON plan_ownership (org_unit_id);
CREATE INDEX idx_ownership_scope ON plan_ownership (scope_type, scope_entity_id);


-- ============================================================
-- 10. VUES UTILITAIRES
-- ============================================================

-- Vue : items avec leur position dans toutes les hiérarchies actives
CREATE VIEW v_item_hierarchy_positions AS
SELECT
    hm.child_entity_id              AS item_id,
    hd.id                           AS hierarchy_id,
    hd.code                         AS hierarchy_code,
    hd.name                         AS hierarchy_name,
    hd.dimension                    AS dimension,
    hm.level_name                   AS level_name,
    hm.level_no                     AS level_no,
    COALESCE(hn.label, 'ROOT')      AS parent_label,
    hm.weight                       AS disagg_weight,
    hm.effective_start,
    hm.effective_end
FROM hierarchy_members hm
JOIN hierarchy_def hd ON hd.id = hm.hierarchy_id
LEFT JOIN hierarchy_nodes hn ON hn.id = hm.parent_node_id
WHERE hm.child_entity_type = 'item'
  AND hm.is_active = TRUE
  AND (hm.effective_end IS NULL OR hm.effective_end > CURRENT_DATE);


-- Vue : BOM multi-niveaux aplatie (avec LLC)
CREATE VIEW v_bom_flat AS
WITH RECURSIVE bom_tree AS (
    -- Niveau 0 : items parents (FG / SF)
    SELECT
        bh.parent_item_id           AS root_item_id,
        bh.id                       AS bom_id,
        bl.component_item_id        AS component_id,
        bl.component_type,
        bl.qty_per_parent,
        bl.yield_factor,
        bl.qty_per_parent / bl.yield_factor AS net_qty,
        1                           AS bom_level,
        bl.is_phantom,
        ARRAY[bh.parent_item_id]    AS path_array
    FROM bom_header bh
    JOIN bom_lines bl ON bl.bom_id = bh.id
    WHERE bh.status = 'active'
      AND (bh.effective_end IS NULL OR bh.effective_end > CURRENT_DATE)
    
    UNION ALL
    
    -- Niveaux suivants : récursion
    SELECT
        bt.root_item_id,
        bh2.id,
        bl2.component_item_id,
        bl2.component_type,
        bt.net_qty * bl2.qty_per_parent,
        bl2.yield_factor,
        bt.net_qty * bl2.qty_per_parent / bl2.yield_factor,
        bt.bom_level + 1,
        bl2.is_phantom,
        bt.path_array || bt.component_id
    FROM bom_tree bt
    JOIN bom_header bh2 ON bh2.parent_item_id = bt.component_id
    JOIN bom_lines bl2 ON bl2.bom_id = bh2.id
    WHERE bh2.status = 'active'
      AND NOT bt.component_id = ANY(bt.path_array)  -- éviter les cycles
      AND (bh2.effective_end IS NULL OR bh2.effective_end > CURRENT_DATE)
)
SELECT
    root_item_id,
    component_id,
    component_type,
    bom_level,
    SUM(net_qty)    AS total_net_qty,  -- accumulation si même composant à plusieurs niveaux
    is_phantom,
    MAX(bom_level)  AS max_level
FROM bom_tree
WHERE is_phantom = FALSE   -- exclure les phantoms de la vue aplatie
GROUP BY root_item_id, component_id, component_type, bom_level, is_phantom;


-- Vue : arbre allocation par canal
CREATE VIEW v_allocation_priority AS
SELECT
    c.id                AS customer_id,
    c.customer_code,
    c.customer_name,
    c.channel,
    c.allocation_class,
    c.allocation_priority,
    c.min_service_level,
    pg.customer_code    AS parent_group_code,
    pg.customer_name    AS parent_group_name
FROM customers c
LEFT JOIN customers pg ON pg.id = c.parent_customer_id
ORDER BY c.allocation_priority ASC, c.channel;


-- ============================================================
-- 11. TRIGGERS ET MAINTENANCE
-- ============================================================

-- Trigger : mise à jour automatique du timestamp updated_at
CREATE OR REPLACE FUNCTION trigger_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_updated_at_hierarchy_def
    BEFORE UPDATE ON hierarchy_def
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

CREATE TRIGGER set_updated_at_hierarchy_members
    BEFORE UPDATE ON hierarchy_members
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

CREATE TRIGGER set_updated_at_bom_header
    BEFORE UPDATE ON bom_header
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

CREATE TRIGGER set_updated_at_customers
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();


-- Trigger : invalider le LLC quand une BOM line est modifiée
CREATE OR REPLACE FUNCTION trigger_invalidate_llc()
RETURNS TRIGGER AS $$
BEGIN
    -- Marquer l'item parent comme ayant un LLC stale
    UPDATE item_low_level_code
    SET is_stale = TRUE
    WHERE item_id = (
        SELECT parent_item_id FROM bom_header WHERE id = NEW.bom_id
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER invalidate_llc_on_bom_change
    AFTER INSERT OR UPDATE OR DELETE ON bom_lines
    FOR EACH ROW EXECUTE FUNCTION trigger_invalidate_llc();


-- ============================================================
-- 12. INDEX POUR PERFORMANCES À GRANDE ÉCHELLE
-- ============================================================

-- Index composite pour la closure table (requêtes ancêtres/descendants)
CREATE INDEX idx_closure_lookup ON hierarchy_closure (hierarchy_id, ancestor_id, depth);
CREATE INDEX idx_closure_descendants ON hierarchy_closure (hierarchy_id, descendant_id, depth);

-- Index pour la BOM (requêtes fréquentes : quels parents utilise ce composant ?)
CREATE INDEX idx_bomline_reverse ON bom_lines (component_item_id, bom_id);

-- Index GIN sur les JSONB pour les requêtes sur attributs
CREATE INDEX idx_hiernode_attrs ON hierarchy_nodes USING GIN (attributes jsonb_path_ops);
CREATE INDEX idx_hierdef_agg_rules ON hierarchy_def USING GIN (aggregation_rules jsonb_path_ops);
CREATE INDEX idx_customer_attrs ON customers USING GIN (attributes jsonb_path_ops);
```

---

## 9. Comparaison vs Kinaxis / o9 / SAP IBP

### 9.1 Tableau de synthèse

| Dimension | Kinaxis RapidResponse | o9 Solutions | SAP IBP | **Ootils** |
|-----------|----------------------|--------------|---------|------------|
| **Hiérarchies produit** | Figée à l'implémentation, 3-6 mois pour modifier | Flexible mais configuration complexe | Cubes HANA, performant mais rigide | **N hiérarchies simultanées, modification en temps réel via API** |
| **Multi-hiérarchies** | 1 hiérarchie maître + attributs simulant les autres | Possible mais sur-complexe | Non natif | **Nativement : 1 SKU dans N hiérarchies distinctes avec poids** |
| **BOM dans le graph** | Tables séparées, jointures batch | Module séparé | PP/DS séparé | **BOM = sous-graph typé, citoyen de première classe** |
| **Explosion BOM** | Batch overnight | Batch / near-real-time | Batch | **Temps réel, traversal incrémentale** |
| **Hiérarchies temporelles** | Buckets fixes (semaine/mois) | Configurable mais limité | Périodes HANA | **Réconciliation inter-horizons native, frozen zones comme objets** |
| **Allocation shortage** | Priority rules basic | ATP avancé | Heuristiques | **Priority + Fair Share + ATP + LP optimization (V2)** |
| **Versioning hiérarchies** | Non supporté nativement | Partiel | Versions MDM | **Temporel natif : effective_start/end sur chaque lien** |
| **Configuration sans code** | Non — projet IT systématique | Partiel — UI complexe | Non | **Oui — API REST + YAML, zéro migration SQL** |
| **Explainability** | Rapport Excel généré | Dashboards | KPI sans causalité | **Graph traversal : chaque résultat avec sa chaîne causale complète** |
| **AI agent ready** | Non — screen-centric | Non | Non | **Architecture conçue pour agents, API first** |

### 9.2 L'avantage structural fondamental d'Ootils

Les APS actuels ont tous le même problème : **ils ont été conçus pour des humains devant des écrans**, avec des batch overnight et des exports Excel. La hiérarchie dans ces systèmes sert à organiser les données pour l'affichage humain.

Ootils part d'un postulat radicalement différent : **les hiérarchies sont des structures de calcul**, pas des structures d'affichage. Elles définissent comment les informations se propagent, comment les décisions s'agrègent, comment les contraintes se répartissent. Elles sont des citoyens actifs du moteur de planification — pas des métadonnées décoratives.

### 9.3 Ce que Kinaxis ne peut pas faire

**Cas 1 : Réorganisation commerciale**
Un client Kinaxis réorganise sa gamme produit (fusion de deux familles, création d'une nouvelle sous-famille). Processus :
- Ticket à l'équipe Kinaxis ou au partenaire implémenteur
- Analyse d'impact sur le modèle de données
- Script de migration des données
- Tests
- Déploiement en production
- **Durée : 3 à 6 mois. Coût : >100k€ souvent.**

Dans Ootils :
- `POST /api/v1/hierarchies/{id}/nodes` — créer le nouveau nœud
- `PATCH /api/v1/hierarchies/members/{id}` — changer l'appartenance avec effective_date
- **Durée : 5 minutes. Coût : 0.**

**Cas 2 : BOM multi-sites avec variantes**
Un FG produit sur 3 sites avec des BOM différentes (composants locaux). Dans Kinaxis : une seule BOM par item, les variantes sont des hacks (items fantômes, attributs custom). Dans Ootils : 3 `bom_header` avec `location_id` différent sur le même `parent_item_id`. Le moteur choisit automatiquement la bonne BOM selon la location de la WO.

**Cas 3 : Shortage allocation multi-canal**
Une pénurie touche simultanément les canaux retail, B2B, et e-commerce avec des règles différentes (fair share pour le retail, priority rules pour le B2B framework, ATP pour l'e-commerce). Dans Kinaxis : règle unique par run de planification. Dans Ootils : chaque canal porte ses propres règles dans le nœud `Customer`, le moteur les applique par canal en un seul passage.

---

## 10. Roadmap d'implémentation

### Principe de priorisation

On priorise selon trois critères :
1. **Impact POC** — ce qui permet de démontrer la valeur différenciante le plus vite
2. **Dépendances** — certains blocs sont requis avant d'autres
3. **Complexité / risque** — minimiser le risque technique dans les premières itérations

### Phase 1 — Fondations hiérarchiques (POC Sprint 1, 2 semaines)

**Objectif :** Démontrer qu'un SKU peut appartenir à plusieurs hiérarchies simultanées, et qu'une modification de hiérarchie ne nécessite pas de migration SQL.

Livrables :
- [ ] Tables `hierarchy_def`, `hierarchy_nodes`, `hierarchy_members`
- [ ] API CRUD : `POST/GET/PATCH /api/v1/hierarchies`
- [ ] API membres : `POST/GET /api/v1/hierarchies/{id}/members`
- [ ] Import YAML initial (chargement d'une hiérarchie produit depuis fichier)
- [ ] Test : un SKU dans 3 hiérarchies différentes, modification d'une hiérarchie via API

**KPI démo :** "Voilà comment on change une hiérarchie produit en 30 secondes vs 6 mois chez Kinaxis."

### Phase 2 — BOM dans le graph (POC Sprint 2, 2 semaines)

**Objectif :** Explosion BOM temps réel avec calcul des besoins nets.

Livrables :
- [ ] Tables `bom_header`, `bom_lines`, `item_low_level_code`
- [ ] Edge type `bom_consumes` intégré dans graph_edges V1
- [ ] Algorithme d'explosion BOM (DFS avec memoization + LLC)
- [ ] API : `POST /api/v1/bom/explode` — retourne les besoins nets par niveau
- [ ] Phantom items supportés
- [ ] Test : BOM 3 niveaux, 50+ composants, explosion < 100ms

**KPI démo :** "Explosion BOM temps réel — pas de batch overnight."

### Phase 3 — Agrégation / désagrégation (S&OP Sprint 3, 2 semaines)

**Objectif :** Réconciliation forecast entre niveau famille (S&OP) et niveau SKU.

Livrables :
- [ ] Moteur d'agrégation bottom-up (sum, weighted_average)
- [ ] Moteur de désagrégation top-down (proportional, equal_split)
- [ ] Détection et création de `HierarchyInconsistency` nodes
- [ ] API : `POST /api/v1/hierarchies/{id}/aggregate` et `/disaggregate`
- [ ] Test : forecast S&OP famille 1000 unités → désagrégation vers 20 SKUs

**KPI démo :** "Le S&OP et le scheduling parlent le même langage, sans Excel interposé."

### Phase 4 — Hiérarchies client et allocation (Sprint 4, 2 semaines)

**Objectif :** Allocation shortage multi-canal avec règles configurables.

Livrables :
- [ ] Table `customers` avec hiérarchie Groupe → Client → Ship-to
- [ ] Modes d'allocation : priority_rules, fair_share
- [ ] Policy node pour les règles d'allocation par canal
- [ ] Intégration avec le moteur de shortage V1 (ADR-001)
- [ ] Test : shortage 30%, 3 canaux, allocation selon priorité

**KPI démo :** "Qui est servi en premier — décision explicable en une requête API."

### Phase 5 — Hiérarchies temporelles et frozen zones (Sprint 5, 1 semaine)

**Objectif :** Réconciliation inter-horizons et protection des plans gelés.

Livrables :
- [ ] Tables `time_periods`, `horizon_reconciliation_rules`
- [ ] Frozen zone enforcement dans le moteur (rejet des modifications sur horizon gelé)
- [ ] API : `GET /api/v1/periods/{grain}` — retourne les périodes avec statut gelé/ouvert
- [ ] Test : modification bloquée sur horizon gelé, approuvée sur horizon ouvert

### Phase 6 — Hiérarchies organisationnelles et workflow (Sprint 6, 1 semaine)

**Objectif :** Ownership des plans et droits d'approbation.

Livrables :
- [ ] Tables `org_units`, `plan_ownership`
- [ ] API : validation qu'un utilisateur peut modifier un plan donné
- [ ] Workflow simple d'approbation (submit → approve/reject)
- [ ] Test : planificateur junior soumet une modification → manager approuve

### Phase 7 — Substituts de composants et ATP (Sprint 7, 2 semaines) — V2

**Objectif :** Gestion automatique des substitutions en cas de pénurie composant.

Livrables :
- [ ] Table `bom_substitutes` active dans le moteur
- [ ] Algorithme de sélection du meilleur substitut (priority + conditions)
- [ ] Mode allocation ATP
- [ ] Test : composant en shortage, substitut automatiquement sélectionné

### Phase 8 — Allocation LP optimale (Sprint 8, 2 semaines) — V2

**Objectif :** Allocation multi-contrainte par solveur LP.

Livrables :
- [ ] Intégration PuLP ou OR-Tools
- [ ] Formulation LP du problème d'allocation
- [ ] API : `POST /api/v1/allocate/optimize`
- [ ] Comparaison : allocation heuristique vs LP optimal

---

## Annexe A — Glossaire

| Terme | Définition dans le contexte Ootils |
|-------|-------------------------------------|
| **Hiérarchie** | Un graphe orienté acyclique définissant des relations parent-enfant entre entités métier, dans une dimension donnée |
| **HierarchyDef** | La définition métadonnée d'une hiérarchie (niveaux, règles, dimension) |
| **HierarchyNode** | Un nœud synthétique d'agrégation qui n'a pas d'existence physique (famille, région, canal) |
| **hier_member** | Edge typé reliant une entité métier à son parent dans une hiérarchie |
| **LLC** | Low-Level Code — niveau BOM le plus bas d'un item, utilisé pour optimiser l'ordre d'explosion |
| **Phantom item** | Composant BOM transparent dans le calcul MRP (non stocké, non planifié individuellement) |
| **Fair share** | Mode d'allocation proportionnelle du supply disponible entre toutes les demandes d'un même niveau de priorité |
| **ATP** | Available-to-Promise — quantité disponible à promettre à une date donnée, intégrant les supply futurs |
| **Frozen zone** | Horizon temporel dans lequel le plan est protégé contre les modifications non approuvées |
| **Top-down wins** | Stratégie de réconciliation où le niveau supérieur de la hiérarchie est autoritaire |
| **Bottom-up wins** | Stratégie de réconciliation où la somme des niveaux inférieurs est autoritaire |
| **Closure table** | Structure PostgreSQL qui stocke tous les couples ancêtre-descendant d'une hiérarchie pour des requêtes O(1) |

---

## Annexe B — Exemples de requêtes SQL utiles

```sql
-- Tous les descendants d'un nœud dans la hiérarchie produit commerciale
SELECT d.*
FROM hierarchy_closure hc
JOIN hierarchy_nodes d ON d.id = hc.descendant_id
WHERE hc.hierarchy_id = 'hier_product_commercial'
  AND hc.ancestor_id = 'hiernode_pompes_centrifuges'
  AND hc.depth > 0
ORDER BY hc.depth;


-- Tous les ancêtres d'un SKU (sa position dans toutes les hiérarchies)
SELECT hd.name AS hierarchy, hn.label AS ancestor, hc.depth
FROM hierarchy_closure hc
JOIN hierarchy_def hd ON hd.id = hc.hierarchy_id
JOIN hierarchy_nodes hn ON hn.id = hc.ancestor_id
WHERE hc.descendant_id = 'item_pump01'
ORDER BY hd.name, hc.depth DESC;


-- Explosion BOM aplatie pour PUMP-ASSY
SELECT
    root_item_id,
    component_id,
    bom_level,
    total_net_qty,
    component_type
FROM v_bom_flat
WHERE root_item_id = 'item_pump_assy'
ORDER BY bom_level, component_id;


-- Clients par priorité d'allocation pour un canal donné
SELECT * FROM v_allocation_priority
WHERE channel = 'B2B_framework'
ORDER BY allocation_priority ASC;


-- Items sans position dans la hiérarchie commerciale (orphelins)
SELECT i.item_id, i.item_code
FROM graph_nodes i
WHERE i.node_type = 'Item'
  AND NOT EXISTS (
    SELECT 1 FROM hierarchy_members hm
    WHERE hm.child_entity_id = i.id
      AND hm.child_entity_type = 'item'
      AND hm.hierarchy_id = (SELECT id FROM hierarchy_def WHERE code = 'hier_product_commercial')
      AND hm.is_active = TRUE
  );


-- Vérifier la cohérence des poids de désagrégation (doit sommer à 1.0 par parent)
SELECT
    parent_node_id,
    hierarchy_id,
    SUM(weight)     AS total_weight,
    COUNT(*)        AS child_count,
    ABS(SUM(weight) - 1.0) > 0.001 AS is_inconsistent
FROM hierarchy_members
WHERE is_active = TRUE
GROUP BY parent_node_id, hierarchy_id
HAVING ABS(SUM(weight) - 1.0) > 0.001;
```

---

*Ce document est vivant. Il évoluera avec chaque sprint d'implémentation. Les décisions architecturales majeures qui en découlent feront l'objet d'ADR dédiés (ADR-010 et suivants).*

*Ootils n'est pas Kinaxis avec de la flexibilité ajoutée. C'est une architecture repensée depuis le modèle de données jusqu'au moteur de calcul, avec les hiérarchies comme citoyens actifs du graph de planification.*
