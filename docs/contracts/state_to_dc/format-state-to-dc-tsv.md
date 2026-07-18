# Format de fichier — `state_to_dc.tsv`

> Fichier de **dispatch d'exécution** : pour chaque état client US, quel centre de distribution (DC) livre.
> Famille référentielle « à la demande », **jamais bloquante** pour le run quotidien (ADR-042 §1, ligne « Référentiel »).
> Endpoint cible : **à construire** — voir « Statut » ci-dessous.

---

## ⚠️ Statut — spec cible, ingestion pas encore construite

Ce document fixe **le contrat de fichier** avant que l'ingestion existe côté Ootils, pour que l'équipe ERP puisse commencer l'extraction pendant que le moteur est construit (ADR-042, famille référentiel « à la demande » — long délai côté ERP, donc spec livrée en premier).

- **Aucun endpoint `POST /v1/ingest/...` n'existe encore** pour cette entité.
- **Aucune table dédiée n'existe encore** — elle sera créée par le chantier **DESC-1** (plan `giggly-wandering-moon.md`, PR-A, migration **083**, prochain numéro libre après `082_supplier_items_updated_at.sql`). Le nom exact de la table n'est pas arrêté à ce stade.
- Tant que ce fichier n'est pas chargé, le moteur de descente de demande (DESC-1 PR-B) traite l'éligibilité/dispatch par défaut dérivé (historique de vente ∪ lanes actives) — ce fichier, une fois disponible, devient la source de vérité et remplace ce défaut.
- Ne pas confondre avec `distribution_links.tsv` (lanes de **réapprovisionnement** inter-sites, un tout autre flux physique — voir §7).

---

## 1. Caractéristiques du fichier

| Propriété | Valeur |
|---|---|
| **Nom** | `state_to_dc.tsv` (exact) |
| **Format** | TSV |
| **Encodage** | UTF-8 (sans BOM de préférence) |
| **Délimiteur** | tabulation (`\t`) |
| **Header** | ligne 1 obligatoire |
| **Cadence** | À la demande — pas de cadence fixe. Renvoyer le fichier uniquement quand une affectation état→centre change. |
| **Criticité** | Advisory (jamais bloquant pour le run quotidien pénurie, ADR-042 §1) |
| **Endpoint** | à construire (chantier DESC-1) |

---

## 2. Nature de la donnée

Une ligne = **une règle d'exécution** :
> « Les clients de l'état `state_code` sont livrés depuis le centre `dc_location_external_id`, à compter du `effective_from`. »

C'est la table de dispatch qui permet au moteur de descente de demande (DESC-1) de faire atterrir la demande nationale (aujourd'hui posée sur les canaux virtuels USA/CAN/ICO) sur les **vrais** centres physiques. Sans elle, la planification reste correcte au niveau national mais l'exécution (quel entrepôt sert quel client) reste invisible — c'est exactement le trou identifié au premier chargement réel (`RAPPORT-PREMIER-CHARGEMENT-2026-07-18.md` : « la masse de la demande vit sur les canaux virtuels […] l'étape qui donnera tout son sens à ces chiffres : les lanes de distribution »).

Modèle V1 : **un seul centre actif par état** (pas de multi-sourcing géographique — un état a un centre de rattachement, pas une liste ordonnée). Portée : États-Unis uniquement (`state_code` est un code US 2 lettres) — les canaux CAN/ICO ne sont pas concernés par ce fichier.

---

## 3. Colonnes

| # | Colonne | Obligatoire | Type | Domaine | Description |
|---|---|---|---|---|---|
| 1 | `state_code` | **oui** | texte (2) | code USPS 2 lettres, majuscules (`CA`, `TX`, `NY`...), doit appartenir à la liste des 50 états + DC (District of Columbia) | État client US. Clé d'upsert (une ligne par état). |
| 2 | `dc_location_external_id` | **oui** | texte | FK `locations` | Centre de distribution qui livre cet état en exécution. |
| 3 | `effective_from` | non | date | ISO `YYYY-MM-DD` | Date à partir de laquelle l'affectation prend effet. Défaut : date d'ingestion. Sémantique exacte (simple horodatage informatif vs bascule SCD2 façon `item_planning_params`) non tranchée — à décider à l'implémentation (PR-A). |

---

## 4. Exemples

### 4.1 Exemple minimal

```
state_code	dc_location_external_id
NY	DC-PAT
```

### 4.2 Réseau à 3 centres

```
state_code	dc_location_external_id	effective_from
NY	DC-PAT	2026-01-01
TX	DC-TXO	2026-01-01
CA	DC-DCW	2026-01-01
```

Lecture : les clients New York sont livrés depuis PAT, le Texas depuis TXO, la Californie depuis DCW — 3 lignes, 3 états, 3 centres distincts (illustratif — remplacer par les codes de centre réels de l'équipe ERP).

### 4.3 Cas invalides (prévus)

```
state_code	dc_location_external_id	effective_from
ZZ	DC-PAT	2026-01-01                  ← code état hors liste USPS → 422
	DC-PAT	2026-01-01                  ← state_code vide → 422
NY	DC-UNKNOWN	2026-01-01              ← centre inconnu (FK locations) → 422
NY	DC-PAT	2026-01-01
NY	DC-TXO	2026-02-01                  ← doublon state_code dans le même fichier → 422 (un seul centre actif par état en V1)
```

---

## 5. Règles de validation côté Ootils (prévues)

- `state_code` : obligatoire, non vide, exactement 2 caractères, normalisé en majuscule côté serveur, doit appartenir à la liste canonique des codes USPS (50 états + DC). Code hors liste ou mal formé → 422, code fautif nommé dans le rapport de rejet (jamais un rejet silencieux).
- `state_code` **unique dans le fichier** — une seule ligne par état (pas de second centre de repli en V1 ; le repli en cas de rupture locale reste porté par le stock de sécurité national, ADR-021, pas par une deuxième ligne ici).
- `dc_location_external_id` : obligatoire, doit exister en base (`locations.external_id`). Pas de contrainte stricte de `location_type` en V1 (même logique que `parent_external_id` sur `locations.tsv` §4), mais un `dc` est attendu en pratique.
- `effective_from` : optionnel, ISO `YYYY-MM-DD`.
- Comportement all-or-nothing (comme tous les fichiers du contrat, cf. `docs/contracts/TSV-FILES-SPEC.md` §1.3) : une seule ligne invalide → rien n'est chargé.

---

## 6. Comportement à l'ingestion (prévu)

**Clé business** : `state_code`.
- Existe en base → UPDATE (`dc_location_external_id`/`effective_from` écrasés — un changement d'affectation se fait en renvoyant la même ligne avec le nouveau centre).
- N'existe pas → INSERT.
- **Jamais de suppression par absence** : un état omis dans un envoi ultérieur garde sa dernière affectation connue (même convention que les 11 entités déjà en production, `docs/contracts/TSV-FILES-SPEC.md` §1.3).

---

## 7. Lien avec `distribution_links.tsv` — ne pas confondre

| | `state_to_dc.tsv` | `distribution_links.tsv` |
|---|---|---|
| Sens du flux | **Sortant** — quel centre livre quel client (dispatch d'exécution) | **Entrant vers un centre** — comment un centre se réapprovisionne depuis un autre site |
| Question posée | « Ce client de cet état, qui le sert ? » | « Ce centre en manque, qui peut lui envoyer du stock ? » |
| Consommateur moteur | Le run de descente de demande (DESC-1 PR-B) | Le moteur DRP existant (`engine/drp/`) |

Les deux sont complémentaires et distinctes — voir `docs/contracts/distribution_links/format-distribution-links-tsv.md`.

---

## 8. Limitations connues V1

| Manque | Raison |
|---|---|
| Pas de multi-sourcing par état (un seul centre actif) | Décision V1 délibérée — cf. §5. Un repli géographique par état est un sujet V2. |
| Pas de granularité infra-état (code postal, comté) | Hors périmètre — le pilote raisonne à la maille état. |
| Sémantique exacte de `effective_from` (horodatage vs SCD2) | Non tranchée — à décider à l'implémentation (PR-A). |

---

## 9. Dépôt

Une fois l'ingestion construite : dossier Dropbox `ootils-inbox/`, nommage `state_to_dc_<AAAAMMJJ>.tsv` (convention générale `SPEC-FICHIERS-ENTREE-OOTILS.md` §1) — envoyé uniquement lors d'un changement, pas de cadence fixe.

## 10. Ordre de chargement

Nécessite `locations.tsv` déjà chargé (FK `dc_location_external_id`). Indépendant des flux quotidiens.
