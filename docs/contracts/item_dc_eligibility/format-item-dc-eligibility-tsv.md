# Format de fichier — `item_dc_eligibility.tsv`

> Fichier référentiel : **quels centres de distribution ont le droit de stocker/servir un article donné**.
> Famille référentielle « à la demande », **jamais bloquante** pour le run quotidien (ADR-042 §1, ligne « Référentiel »).
> Endpoint cible : **à construire** — voir « Statut » ci-dessous.

---

## ⚠️ Statut — spec cible, ingestion pas encore construite

Comme `state_to_dc.tsv`, ce fichier fixe un contrat cible avant que l'ingestion existe (ADR-042, famille référentiel « à la demande »).

- **Aucun endpoint `POST /v1/ingest/...` n'existe encore.**
- **Aucune table dédiée n'existe encore** — réservée au chantier **DESC-1**, migration **083** (PR-A, plan `giggly-wandering-moon.md`), nom de table non arrêté.
- Tant que ce fichier n'est pas chargé, l'éligibilité produit→centre est **dérivée** par le moteur (historique de vente ∪ lanes de réappro actives) — défaut assumé et flaggé, jamais silencieux. Ce fichier, une fois disponible, devient la source de vérité **pour les articles qu'il couvre** (voir §4, modèle fermé).

---

## 1. Caractéristiques du fichier

| Propriété | Valeur |
|---|---|
| **Nom** | `item_dc_eligibility.tsv` (exact) |
| **Format** | TSV |
| **Encodage** | UTF-8 (sans BOM de préférence) |
| **Délimiteur** | tabulation (`\t`) |
| **Header** | ligne 1 obligatoire |
| **Cadence** | À la demande — renvoyer uniquement lors d'un changement (nouvel article, retrait d'un centre, arrêt d'un article dans un centre). |
| **Criticité** | Advisory (jamais bloquant, ADR-042 §1) |
| **Endpoint** | à construire (chantier DESC-1) |

---

## 2. Nature de la donnée

Une ligne = **une autorisation produit × centre** :
> « L'article `item_external_id` est autorisé au centre `dc_location_external_id` — sauf si `eligible` dit explicitement le contraire. »

Certains articles n'existent physiquement que dans un sous-ensemble des centres (contraintes réglementaires, gamme régionale, capacité). Sans ce fichier, le moteur de descente de demande (DESC-1) ne peut pas savoir qu'un article ne doit **jamais** atterrir sur un centre donné, et risque de proposer une répartition ou une commande sur un site qui, en réalité, ne le référence pas. C'est le complément produit de `state_to_dc.tsv` (qui, lui, route le *client*, pas l'article).

---

## 3. Colonnes

| # | Colonne | Obligatoire | Type | Domaine | Description |
|---|---|---|---|---|---|
| 1 | `item_external_id` | **oui** | texte | FK `items` | Article concerné. |
| 2 | `dc_location_external_id` | **oui** | texte | FK `locations` | Centre pour lequel l'éligibilité est déclarée. |
| 3 | `eligible` | non | booléen | voir §5 (**divergence assumée** de la convention booléenne générale — lire avant d'utiliser cette colonne) | Défaut : `true`. |

**Clé business** : couple (`item_external_id`, `dc_location_external_id`).

---

## 4. Modèle : fermé par article, ouvert par défaut

- Un article qui **n'apparaît jamais** dans ce fichier (dans aucun envoi cumulé) reste sans restriction connue — son éligibilité continue d'être **dérivée** par le moteur (historique ∪ lanes), pas fermée.
- Dès qu'un article apparaît **au moins une fois** (toutes lignes confondues, cumulées sur l'ensemble des envois — cette table est en ajout/mise à jour, jamais remise à zéro par un nouvel envoi), ce fichier devient la **source de vérité exclusive pour cet article** : seuls les centres marqués `eligible=true` restent autorisés ; tout centre absent de la liste de cet article, ou marqué `eligible=false`, est **non éligible**.
- Cas limite légitime et **volontairement fail-loud** : un article dont tous les centres connus finissent à `eligible=false` n'a **aucun** centre éligible. Le moteur de descente ne fabrique pas d'exception silencieuse dans ce cas — la demande nationale de cet article **reste au national**, non descendue, jusqu'à correction du référentiel (comportement du moteur DESC-1, cf. plan `giggly-wandering-moon.md` §« Approche retenue »).

---

## 5. La colonne `eligible` — divergence assumée par rapport à la convention booléenne générale

La convention booléenne générale du dépôt (`docs/contracts/TSV-FILES-SPEC.md` §1.2) traite une **cellule vide** comme `false`. Sur ce fichier précis, l'intention métier (« la présence d'une ligne = éligible ») est différente : la plupart des envois n'auront **pas du tout** de colonne `eligible` — un fichier à 2 colonnes (`item_external_id`, `dc_location_external_id`) suffit dans le cas courant.

Règle retenue (à respecter par l'implémentation, PR-A) :

| Situation | Résultat |
|---|---|
| Colonne `eligible` **absente du header** | Toutes les lignes du fichier valent `eligible=true` implicite. |
| Colonne `eligible` **présente**, cellule renseignée (`true`/`false`, voir §1.2 pour les alias acceptés) | Valeur explicite appliquée. |
| Colonne `eligible` **présente**, cellule **vide** pour une ligne donnée | Suit la convention booléenne générale : `false` — **pas** de traitement spécial ligne par ligne. Pour déclarer une ligne éligible dans un fichier qui a par ailleurs la colonne, écrire `true` explicitement. |

En clair : pour **révoquer** une éligibilité déjà déclarée, renvoyer la ligne avec `eligible=false` explicite — ne jamais se contenter d'omettre la ligne (l'omission ne signifie *rien* pour un article déjà « fermé », voir §4 : elle laisse simplement l'état précédent inchangé).

---

## 6. Exemples

### 6.1 Cas courant — fichier à 2 colonnes (liste blanche simple)

```
item_external_id	dc_location_external_id
FG-APU-100	DC-LILLE
FG-APU-100	DC-PARIS
FG-APU-200	DC-LILLE
```

Lecture : `FG-APU-100` éligible uniquement à Lille et Paris (fermé dès la première ligne le concernant) ; `FG-APU-200` éligible uniquement à Lille.

### 6.2 Avec révocation explicite

```
item_external_id	dc_location_external_id	eligible
FG-APU-100	DC-LILLE	true
FG-APU-100	DC-PARIS	true
FG-APU-200	DC-LILLE	false
```

Lecture : `FG-APU-200` était éligible à Lille (envoi antérieur) et vient d'en être explicitement retiré — s'il n'a aucun autre centre déclaré ailleurs, il n'a **plus aucun** centre éligible (cf. §4, cas fail-loud).

### 6.3 Cas invalides

```
item_external_id	dc_location_external_id	eligible
	DC-LILLE	true                          ← item_external_id vide → 422
ITEM-UNKNOWN	DC-LILLE	true                  ← item inconnu (FK items) → 422
FG-APU-100	LOC-XYZ	true                      ← centre inconnu (FK locations) → 422
FG-APU-100	DC-LILLE	true
FG-APU-100	DC-LILLE	false                     ← doublon (item, centre) dans le même fichier → 422
```

---

## 7. Règles de validation côté Ootils (prévues)

- `item_external_id` : obligatoire, FK `items`.
- `dc_location_external_id` : obligatoire, FK `locations`.
- `eligible` : optionnel, booléen — voir §5 pour la sémantique exacte (absence de colonne ≠ cellule vide).
- Couple (`item_external_id`, `dc_location_external_id`) **unique dans le fichier** — doublon exact rejeté (all-or-nothing).
- Comportement all-or-nothing général (`docs/contracts/TSV-FILES-SPEC.md` §1.3).

---

## 8. Comportement à l'ingestion (prévu)

- Upsert par couple (`item_external_id`, `dc_location_external_id`).
- **Jamais de suppression par absence** : une paire déjà connue et non renvoyée garde sa dernière valeur — la seule façon de fermer une éligibilité est le `eligible=false` explicite (§5).
- La table cumule l'historique de tous les envois pour un article donné (§4 : le modèle fermé se construit par accumulation, pas par un seul fichier isolé).

---

## 9. Limitations connues V1

| Manque | Raison |
|---|---|
| Pas de date d'effet (`effective_from`) | Contrairement à `state_to_dc.tsv` — un changement d'éligibilité est immédiat en V1. |
| Pas de raison/commentaire sur une exclusion | Utile pour l'audit (« pourquoi cet article n'est plus à ce centre ») — V1.1 si besoin. |

## 10. Ordre de chargement

Nécessite `items.tsv` et `locations.tsv` déjà chargés (2 FK). Indépendant des flux quotidiens.
