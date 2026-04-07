# Review — feat/ingest-router + feat/hierarchies-spec — 2026-04-07

**Reviewer :** Claw (subagent senior — backend Python + architecte supply chain)  
**Base :** `main` @ b55a496  
**Branches reviewées :**
- `feat/ingest-router` — diff sur 3 fichiers : `ingest.py` (944 lignes), `test_ingest.py` (604 lignes), `app.py` (+2 lignes), `docs/SPEC-HIERARCHIES.md` (+1608 lignes)
- `feat/hierarchies-spec` — diff sur 3 fichiers docs : `SPEC-GHOSTS-TAGS.md` (+480 lignes), `ADR-010-ghosts-tags.md` (+170 lignes), `SPEC-HIERARCHIES.md` (+1608 lignes)

---

## feat/ingest-router

### ✅ Points validés

**Cohérence schéma (migrations 001-007)**

- `items` : colonnes utilisées (`item_id`, `external_id`, `name`, `item_type`, `uom`, `status`, `updated_at`) — toutes présentes dans migration 002. Les valeurs CHECK (`item_type`, `status`) correspondent exactement aux contraintes SQL.
- `locations` : colonnes utilisées (`location_id`, `external_id`, `name`, `location_type`, `country`, `timezone`) — toutes présentes dans migration 002. CHECK sur `location_type` aligné.
- `suppliers` : colonnes utilisées (`supplier_id`, `external_id`, `name`, `lead_time_days`, `reliability_score`, `country`, `status`, `updated_at`) — toutes dans migration 007. Les champs `moq` et `currency` acceptés dans le payload mais explicitement non persistés (commentaire dans le modèle) — comportement intentionnel documenté dans le code. ✅
- `supplier_items` : colonnes utilisées (`supplier_item_id`, `supplier_id`, `item_id`, `lead_time_days`, `moq`, `unit_cost`, `is_preferred`, `currency`) — toutes dans migration 007.
- `nodes` : colonnes utilisées (`node_id`, `node_type`, `scenario_id`, `item_id`, `location_id`, `quantity`, `qty_uom`, `time_grain`, `time_ref`, `active`, `updated_at`) — toutes dans migration 002. Types `'OnHandSupply'`, `'PurchaseOrderSupply'`, `'ForecastDemand'` présents dans le CHECK de `nodes.node_type`. Valeurs de `time_grain` alignées avec le CHECK SQL.
- `external_references` : colonnes utilisées (`entity_type`, `external_id`, `source_system`, `internal_id`) — toutes dans migration 007. Valeur `'purchase_order'` présente dans le CHECK de `entity_type`.

**Aucune colonne inventée détectée.**

**Auth**

- `require_auth` appliqué sur les 7 endpoints via `Depends(require_auth)`. Retourne HTTP 401 correctement (testé dans `test_ingest_items_no_auth`).
- Auth : 1 test 401 présent pour items. Pas de test 401 explicite sur les 6 autres endpoints — acceptable car le mécanisme est partagé (même dépendance), la couverture est suffisante en integration.

**Gestion des erreurs FK**

- Sur `supplier-items`, `on-hand`, `purchase-orders`, `forecast-demand` : les FK manquantes (external_id inconnu) sont capturées par batch lookup avant INSERT/UPDATE. Les messages d'erreur sont explicites (`"item_external_id 'X' not found in DB"`).
- Retour : status 200 avec `status: "error"` ou `"partial"` dans le corps. **Note : ce n'est pas un 422 HTTP**, c'est une réponse métier applicative à 200 — voir ⚠️ ci-dessous.

**dry_run**

- Implémenté sur les 7 endpoints. Sur les endpoints avec pré-validation (items, locations, forecast-demand), le dry_run est correctement placé *après* la validation métier (item_type, status, time_grain) et *avant* les requêtes DB — comportement correct.

**Gestion des connexions (asyncpg / psycopg)**

- Le code utilise psycopg3 (sync), cohérent avec le reste du projet.
- La connexion est gérée par le context manager `get_db` dans `dependencies.py` : commit/rollback automatiques, connexion fermée dans tous les cas. Pas de leak de connexion possible avec ce pattern.
- Pas de connection pool custom — utilise la connexion fournie par la dépendance FastAPI. Correct pour le MVP.

**Qualité code**

- Code lisible, structuré en sections claires avec commentaires délimiteurs.
- Pattern `_batch_existing()` factorisé pour les lookups par `external_id`. Bon.
- Modèles Pydantic avec validators appropriés.
- Logging cohérent avec le reste du projet (`ingest.items total=... inserted=... updated=...`).

**Tests — couverture fonctionnelle**

- 27 tests `@requires_db` couvrant les 7 endpoints.
- Cas nominaux (insert + update) : couverts pour tous les endpoints.
- Erreurs FK : couverts pour `supplier-items`, `on-hand`, `purchase-orders`.
- dry_run : couverts pour items, locations, suppliers, supplier-items, forecast-demand. Manque `on-hand` et `purchase-orders` — voir ⚠️.
- 401 : 1 test (items) — acceptable.
- Validation métier : item_type invalide, reliability_score hors plage, status invalide, time_grain invalide — couverts.

---

### ⚠️ Points à corriger avant merge

**⚠️ W-01 — Atomicité batch non garantie sur supplier-items / on-hand / purchase-orders / forecast-demand**

Les endpoints qui gèrent des erreurs FK en mode "partial" (supplier-items, on-hand, purchase-orders, forecast-demand) continuent d'insérer les lignes valides même si d'autres sont en erreur. Le comportement attendu décrit dans la review request est : **tout le batch est rejeté si une ligne est invalide**.

Actuellement, items, locations, suppliers rejettent tout le batch (early return sur `if errors`). Mais supplier-items, on-hand, purchase-orders, forecast-demand adoptent un mode "partial" — les lignes valides sont committées et les invalides sont remontées en erreur dans le body.

Ce comportement partiel est utilisable en production mais n'est pas documenté dans l'API spec et diffère de la sémantique déclarée. Deux options :
1. Aligner sur rejet total (cohérent avec les 3 premiers endpoints) — ajouter une passe de pré-validation FK avant tout INSERT
2. Documenter explicitement le comportement partial dans l'API spec et rendre `conflict_strategy` ou un flag `atomic: bool` disponible sur tous les endpoints

**⚠️ W-02 — Erreurs FK retournées en HTTP 200, pas HTTP 422**

Les erreurs de FK manquante (external_id inconnu) sont retournées en HTTP 200 avec `status: "error"` dans le body JSON. La convention FastAPI/REST standard pour les erreurs de validation de données entrantes serait HTTP 422 Unprocessable Entity.

Cette approche applicative à 200 est valide si documentée et cohérente, mais elle diverge de l'intention mentionnée dans la review request ("retournées en 422"). À aligner avec l'API spec ou à corriger.

**⚠️ W-03 — `updated_at` non mis à jour sur `locations` lors du UPDATE**

Dans `ingest_locations`, le UPDATE ne met pas à jour `updated_at` :
```python
UPDATE locations SET name = %s, location_type = %s, country = %s, timezone = %s WHERE external_id = %s
```
Contrairement à `items` et `suppliers` qui font `updated_at = now()`. La table `locations` n'a pas de colonne `updated_at` dans migration 002 — ce n'est donc pas un bug SQL, mais c'est une incohérence de traitement entre entités. **À vérifier** : si `locations` n'a pas `updated_at`, le code items/suppliers est-il le modèle à suivre quand/si la colonne est ajoutée ?

**⚠️ W-04 — dry_run manquant en test pour `on-hand` et `purchase-orders`**

`test_ingest_on_hand` n'a pas de test `dry_run`. `test_ingest_purchase_orders` non plus. Le code implémente bien le dry_run sur ces deux endpoints, mais sans couverture test. À ajouter.

**⚠️ W-05 — SupplierRow : champs `moq` et `currency` acceptés mais non persistés — comportement silencieux**

Le modèle `SupplierRow` accepte `moq` et `currency` avec un commentaire "not a suppliers column, accepted but not persisted". Ce comportement silencieux (accept + ignore) peut surprendre les intégrateurs. Recommandation : soit retirer ces champs du modèle, soit ajouter un `warning` dans la réponse indiquant qu'ils sont ignorés.

**⚠️ W-06 — Pas de validation `lead_time_days > 0` dans IngestSuppliersRequest**

La migration 007 déclare `lead_time_days INTEGER CHECK (lead_time_days > 0)` sur `suppliers`. Le router ne valide pas cette contrainte — elle sera levée en exception psycopg si elle viole le CHECK SQL. L'exception ne sera pas capturée proprement (pas de try/except sur les INSERT) et retournera une 500. À valider côté applicatif.

---

### ❌ Blockers

Aucun blocker absolu. Le W-01 (atomicité) et W-02 (HTTP 422 vs 200) sont des questions de spécification à trancher — le code fonctionne tel quel.

---

## feat/hierarchies-spec

### ✅ Points validés

**Mécanique phase_transition**

La spec est **très claire et corrective** sur ce point. Section 2.1 (Cas d'usage 1) :
> "La demande n'est pas portée par le Ghost. Elle existe indépendamment au niveau des items A et B — les demand planners la pilotent à item level. Le Ghost n'interfère pas dans le forecast."
> "La somme A+B reste ~constante dans le temps — c'est une substitution, pas une création ou destruction de demande."

Section 2.3 :
> "le Ghost phase_transition est une couche de surveillance supply, pas un distributeur de demande."
> "Résultat moteur : aucun ForecastDemand créé sur les membres."

ADR-010 D2 (table) :
> "Un Ghost phase_transition ne distribue pas de demande. Il surveille la cohérence du flux de supply lors d'une transition produit."

Point PO-02 dans la section Points ouverts : explicitement clos comme "question caduque". ✅

L'endpoint `demand-split` est explicitement supprimé, avec note explicative. ✅

**RCCP / capacity_aggregate**

Section 2.1 (Cas d'usage 2) :
> "Le Ghost consolide la charge (load) de ses membres — c'est-à-dire les WorkOrderSupply et PlannedSupply actifs sur ces items, pas la demande."

Section 2.3 (Ghost capacity_aggregate — agrégation de charge) :
> "Input : les WorkOrderSupply et PlannedSupply actifs sur les items membres."

ADR-010 D2 :
> "Un Ghost capacity_aggregate n'agrège pas de demande. Le RCCP opère sur la charge (load de production), pas sur la demande brute."
> "Agréger de la demande pour faire du RCCP serait une erreur conceptuelle — cela confondrait deux niveaux du processus S&OP."

Distinction charge/demande expliquée clairement avec le bloc "Distinction critique" en callout. ✅

**Endpoint load-summary**

Route `GET /v1/ghosts/{ghost_id}/load-summary` documentée pour `capacity_aggregate` uniquement. Sémantique explicite : agrège la charge (WorkOrderSupply / PlannedSupply), pas la demande. Exemple de réponse détaillé avec `load_total`, `capacity`, `slack`, `overloaded`, `member_breakdown`. ✅

**Endpoint transition**

Route `GET /v1/ghosts/{ghost_id}/transition` documentée avec :
- Courbe de poids A/B par date
- Projection `ProjectedInventory(A) + ProjectedInventory(B)` vs baseline
- Alertes `transition_inconsistency`

Pas de "répartition de demande" dans la réponse — ce n'est pas son rôle. ✅

**ADR-010 D5 — Argument contre Sage #001**

Argument clairement articulé :
> "La recommandation Sage #001 traite les hiérarchies comme une fonctionnalité optionnelle d'amélioration de l'interface utilisateur. Cette analyse est incorrecte dans le contexte Ootils : les hiérarchies sont une dépendance structurelle des Ghosts..."

Trois dépendances explicites listées (plan familial, contexte capacitaire, désagrégation correcte). Qualification du risque : "Ghosts orphelins de contexte — fonctionnels au niveau calcul, mais impossibles à piloter correctement dans un process S&OP." ✅

**Point ouvert entité Resource**

PO-03 : "Entité Resource (capacité) : dans quelle migration ? Bloquant pour le RCCP effectif." Documenté explicitement comme dépendance V2. La section Conséquences de l'ADR mentionne également : "le Ghost capacity_aggregate est partiellement opérationnel sans l'entité Resource — la comparaison charge/capacité ne peut pas être automatisée." Mitigation temporaire proposée (`capacity_override` sur `ghost_nodes`). ✅

**Cohérence avec ADR-001 — schéma ghost_nodes / ghost_members**

- `ghost_nodes` : `node_id UUID REFERENCES nodes(node_id)` — le Ghost est bien un citoyen du graphe, enregistré dans `nodes` avec `node_type = 'Ghost'`. Cohérent avec ADR-001.
- `ghost_members` : chaque membership génère un edge `ghost_member` dans la table `edges`. Cohérent avec ADR-001 (le graphe est traversable nativement).
- Les deux tables utilisent UUIDs, timestamps UTC, `created_at`/`updated_at` — conventions conformes.

---

### ⚠️ Points à corriger avant merge

**⚠️ W-10 — ADR-010 D2 : contradiction interne sur phase_transition**

La table de synthèse dans D2 de l'ADR-010 contient encore :
```
| Objet agrégé | Demande (ForecastDemand, CustomerOrderDemand) portée sur le Ghost |
| Output moteur | Distribution de la demande du Ghost sur A et B selon poids calculé à t |
```

Or la section de texte juste en dessous du tableau corrige explicitement ce point :
> "Un Ghost phase_transition ne distribue pas de demande. Il surveille la cohérence du flux de supply..."

**La table et le texte sont en contradiction directe.** La table est un vestige d'une version antérieure de la spec. À corriger avant merge — risque de confusion pour tout développeur qui lit la table sans lire le corps.

Correction attendue dans la table D2 :

| Attribut | Valeur corrigée |
|----------|-----------------|
| Objet agrégé | Flux de supply (PlannedSupply, OnHandSupply) des membres — surveillance de cohérence |
| Output moteur | Alertes `transition_inconsistency` si `ProjectedInventory(A) + ProjectedInventory(B)` dévie du baseline |

**⚠️ W-11 — ghost_members : poids pour capacity_aggregate mal définis**

Le commentaire dans `ghost_members` indique :
> "Pour capacity_aggregate : weight fixe = proportion de la charge consolidée (1.0 par défaut)"

Mais la formule d'agrégation en section 2.3 est :
```
load_total(ghost, t) = Σ_{item ∈ membres} load(item, t)
```
Sans pondération — tous les membres contribuent à 100% de leur charge. Si `weight_at_start` et `weight_at_end` sont utilisés comme coefficients dans cette somme, cela doit être explicité. Sinon, les colonnes de poids sont inutiles pour `capacity_aggregate` et peuvent prêter à confusion. À clarifier.

**⚠️ W-12 — Migration 008 non incluse dans cette branche**

La spec décrit les tables `ghost_nodes`, `ghost_members`, `tags`, `entity_tags` et les mises à jour de CHECK constraints sur `nodes.node_type` et `edges.edge_type`. Aucune migration 008 n'est fournie dans la branche `feat/hierarchies-spec` (branche purement documentaire). C'est acceptable si la migration fait l'objet d'une issue séparée, mais il faut le documenter explicitement dans la spec (lien vers l'issue de migration 008).

**⚠️ W-13 — `locations` : colonne `updated_at` absente dans migration 002**

La table `locations` dans migration 002 n'a pas de colonne `updated_at`. La spec Ghosts crée une nouvelle table `ghost_nodes` avec `updated_at` (cohérent), mais si la roadmap prévoit d'ajouter `updated_at` sur `locations`, c'est le bon moment de le mentionner (migration 008 est déjà requise).

**⚠️ W-14 — PO-05 non analysé (Ghost multi-ressources)**

Le point ouvert PO-05 "Un Ghost capacity_aggregate peut-il être attaché à plusieurs ressources ?" est laissé sans analyse de risque. Cas réel : une ligne de production est contrainte par un fournisseur ET par de l'outillage partagé. Ce cas est fréquent en manufacturing. Recommandation : ajouter une note sur l'impact de ce cas sur le modèle de données (`resource_id` sur `ghost_nodes` devient insuffisant si multi-ressources).

---

### ❌ Blockers

**❌ B-10 — Contradiction D2 sur phase_transition (voir W-10)**

La contradiction entre la table D2 et le texte de l'ADR-010 est un blocker : tout développeur qui implémentera la logique moteur Ghost en se référant à ce document peut implémenter incorrectement la distribution de demande. Ce point **doit être corrigé avant merge** pour que la spec serve de contrat fiable d'implémentation.

---

## Verdict global

| Branche | Verdict | Conditions |
|---------|---------|------------|
| `feat/ingest-router` | **MERGE AVEC CORRECTIONS** | Corriger W-03 (`updated_at` locations), W-04 (tests dry_run manquants), W-06 (validation lead_time_days). Trancher W-01 (atomicité partielle vs totale) et W-02 (200 vs 422) avec le PO — documenter la décision dans l'API spec. W-05 (champs silencieux) recommandé mais non bloquant. |
| `feat/hierarchies-spec` | **MERGE AVEC CORRECTIONS** | Blocker B-10 : corriger la contradiction dans la table D2 de l'ADR-010 (phase_transition). Après correction, les points W-11 à W-14 sont à adresser mais n'empêchent pas le merge (branche documentaire). |

---

*Review produite par Claw — 2026-04-07*
