# Format de fichier — `distribution_links.tsv`

> Fichier référentiel : les **lanes de réapprovisionnement inter-sites** — d'où un centre peut recevoir du stock quand il en manque.
> Famille référentielle « à la demande », **jamais bloquante** pour le run quotidien (ADR-042 §1, ligne « Référentiel »).
> Endpoint cible : **à construire** — voir « Statut » ci-dessous.

---

## ⚠️ Statut — spec cible, ingestion pas encore construite

- **Aucun endpoint `POST /v1/ingest/...` n'existe encore.** La table `distribution_links` elle-même **existe déjà** en base (migration `029_drp_models.sql`, colonne `transfer_multiple` ajoutée par la migration `065_distribution_links_transfer_multiple.sql`) mais n'est aujourd'hui peuplée que par script/seed direct, jamais par un fichier ERP. L'ingestion TSV décrite ici est le travail de la **PR-D** du chantier DESC-1 (plan `giggly-wandering-moon.md`, « Ingest `distribution_links` + preuve DRP »).
- Le moteur DRP qui **consomme** cette table (`src/ootils_core/engine/drp/`) est, lui, déjà écrit et mergé — c'est le point clé : « l'échelon DRP per-site […] tourne à vide faute de demande localisée » (plan DESC-1, Contexte). Charger ce fichier n'active pas un nouveau moteur, il **alimente un moteur qui attend déjà des données**.
- Ne pas confondre avec `state_to_dc.tsv` — voir §7 de ce document.

---

## 1. Caractéristiques du fichier

| Propriété | Valeur |
|---|---|
| **Nom** | `distribution_links.tsv` (exact) |
| **Format** | TSV |
| **Encodage** | UTF-8 (sans BOM de préférence) |
| **Délimiteur** | tabulation (`\t`) |
| **Header** | ligne 1 obligatoire |
| **Cadence** | À la demande — renvoyer uniquement lors d'un changement de réseau logistique (nouvelle lane, délai de transit révisé, changement de multiple d'expédition). |
| **Criticité** | Advisory (jamais bloquant, ADR-042 §1) |
| **Endpoint** | à construire (chantier DESC-1 PR-D) |
| **Table cible** | `distribution_links` (migration 029 + 065, déjà en base) |

---

## 2. Nature de la donnée

Une ligne = **une lane de réapprovisionnement autorisée entre deux sites** :
> « Le site `upstream_external_id` peut réapprovisionner le site `downstream_external_id` (pour l'article `item_external_id`, ou tout article si vide), en `transit_lead_time_days` jours. »

C'est le réseau physique que le moteur DRP (Distribution Requirements Planning, déjà mergé et opérationnel) utilise pour proposer des **transferts inter-sites gouvernés** quand un centre est en déficit projeté et qu'un autre a de l'excédent. Sans ce fichier, le DRP n'a aucune lane à considérer et ne peut rien recommander, même si le déséquilibre entre centres est réel et détecté ailleurs.

**Différence avec un transfert (`transfers.tsv`)** : une lane n'est pas un mouvement, c'est une **autorisation structurelle** (« PAT peut réapprovisionner DCW ») — le mouvement concret (quantité, date) est soit posé manuellement via `transfers.tsv`, soit généré comme recommandation par le DRP à partir des lanes actives.

---

## 3. Colonnes

**Ordre du contrat** : `upstream_external_id`, `downstream_external_id`, `item_external_id`, `transit_lead_time_days`, `minimum_shipment_qty`, `transfer_multiple`, `priority`.

| # | Colonne | Obligatoire | Type | Domaine | Description |
|---|---|---|---|---|---|
| 1 | `upstream_external_id` | **oui** | texte | FK `locations` | Site **amont** — celui qui expédie / source de réappro. |
| 2 | `downstream_external_id` | **oui** | texte | FK `locations`, ≠ `upstream_external_id` | Site **aval** — celui qui reçoit / demande. |
| 3 | `item_external_id` | non | texte | FK `items` si renseigné ; **vide = lane générique** (valable pour tout article sur ce couple amont/aval) | Portée du lien. |
| 4 | `transit_lead_time_days` | **oui** | décimal | ≥ 0 | Délai de transit en jours. **Obligatoire dans ce fichier** bien que la colonne DB porte un défaut technique de 7 jours — un délai de transit réseau est une donnée structurante ; le contrat fichier refuse le silence dessus plutôt que d'hériter d'une valeur générique non vérifiée. |
| 5 | `minimum_shipment_qty` | non | décimal | ≥ 0 | Quantité minimum par expédition. Défaut : `1` (défaut DB — `0` est une valeur légitime signifiant « pas de plancher »). |
| 6 | `transfer_multiple` | non | décimal | **strictement** > 0 | Multiple logistique d'expédition (carton/palette/camion complet), arrondi **vers le bas** côté DRP (ADR-028 — délibérément l'inverse de l'arrondi MRP, qui arrondit vers le haut). Défaut : `1` (= pas d'arrondi). `0` ou négatif rejeté. |
| 7 | `priority` | non | entier | ≥ 1 (1 = le plus prioritaire) | Priorité de sourcing quand plusieurs lanes desservent le même site aval. Défaut : `100`. |

**Clé business (proposée)** : triplet (`upstream_external_id`, `downstream_external_id`, `item_external_id`) — `item_external_id` vide compte comme sa propre valeur (NULL) dans ce triplet. C'est une proposition de conception pour la PR-D, cohérente avec le modèle « clé composite » déjà utilisé par `supplier_items.tsv` (pas de colonne `external_id` dédiée sur la table `distribution_links`) — à confirmer à l'implémentation.

---

## 4. Comportement du moteur — lane générique vs lane spécifique à un article

Le moteur DRP (`engine/drp/core.py:_resolve_candidate_links`, déjà en place) applique une règle de **spécificité** : pour un couple amont/aval donné, une lane **spécifique à un article** prime sur la lane **générique** du même couple quand les deux existent — elles ne sont pas des doublons, leurs portées diffèrent. Utile pour affiner les paramètres (délai, quantité minimum, multiple, priorité) d'un article particulier sans dupliquer le réseau générique pour tous les autres.

---

## 5. Exemples

### 5.1 Réseau générique (aucun article ciblé)

```
upstream_external_id	downstream_external_id	item_external_id	transit_lead_time_days	minimum_shipment_qty	transfer_multiple	priority
PLANT-LYON	WH-PARIS-01		2
WH-PARIS-01	DC-LILLE		1
```

(`minimum_shipment_qty`, `transfer_multiple`, `priority` vides → défauts serveur `1`/`1`/`100`.)

### 5.2 Avec une lane affinée pour un article précis

```
upstream_external_id	downstream_external_id	item_external_id	transit_lead_time_days	minimum_shipment_qty	transfer_multiple	priority
WH-PARIS-01	DC-LILLE		1
WH-PARIS-01	DC-LILLE	FG-APU-100	1	200	24	1
```

Lecture : la lane générique Paris→Lille dessert tous les articles en 1 jour, sans contrainte de lot. Une deuxième ligne affine spécifiquement `FG-APU-100` sur le même couple : même délai, mais expédition par lot minimum de 200, multiple logistique de 24 (carton), priorité 1 (préférée pour cet article précis) — remplace la lane générique pour cet article seul (§4), sans toucher aux autres articles qui restent sur la ligne générique.

### 5.3 Cas invalides (prévus)

```
upstream_external_id	downstream_external_id	item_external_id	transit_lead_time_days	minimum_shipment_qty	transfer_multiple	priority
PLANT-LYON	PLANT-LYON		2                        ← amont == aval → 422
LOC-XYZ	DC-LILLE		2                            ← amont inconnu → 422
PLANT-LYON	DC-LILLE	ITEM-UNKNOWN	2            ← item inconnu → 422
PLANT-LYON	DC-LILLE				                 ← transit_lead_time_days manquant → 422 (obligatoire dans ce fichier)
PLANT-LYON	DC-LILLE		2			0            ← transfer_multiple = 0 → 422 (doit être strictement > 0)
PLANT-LYON	DC-LILLE		2
PLANT-LYON	DC-LILLE		3                            ← doublon exact du triplet (amont, aval, item vide) → 422
```

---

## 6. Règles de validation côté Ootils (prévues)

- `upstream_external_id`, `downstream_external_id` : obligatoires, FK `locations`, doivent différer (même contrainte DB que `chk_distribution_link_locations_different`, migration 029).
- `item_external_id` : optionnel ; si renseigné, FK `items` — un code inconnu est rejeté (jamais traité silencieusement comme générique).
- `transit_lead_time_days` : obligatoire, décimal ≥ 0.
- `minimum_shipment_qty` : optionnel, décimal ≥ 0, défaut `1`.
- `transfer_multiple` : optionnel, décimal strictement > 0, défaut `1`.
- `priority` : optionnel, entier ≥ 1, défaut `100`.
- Triplet (`upstream_external_id`, `downstream_external_id`, `item_external_id`) **unique dans le fichier** — une lane générique et une lane spécifique à un article pour le **même** couple amont/aval **coexistent** (portées différentes, pas un doublon ; voir §4).
- Comportement all-or-nothing général (`docs/contracts/TSV-FILES-SPEC.md` §1.3).

---

## 7. Lien avec `state_to_dc.tsv` — ne pas confondre

Voir `docs/contracts/state_to_dc/format-state-to-dc-tsv.md` §7 pour le tableau comparatif complet. En bref : `state_to_dc.tsv` route la **demande sortante** (client → centre) ; `distribution_links.tsv` route le **réapprovisionnement entrant** d'un centre (centre → centre). Les deux sont nécessaires et n'ont aucune ligne en commun.

---

## 8. Limitations connues V1 (non couvertes par ce fichier)

La table DB `distribution_links` porte des colonnes supplémentaires que ce fichier V1 ne couvre pas (coûts de transit, quantité maximum, fréquence/jours d'expédition, flag `active`) — elles restent gérées hors fichier (script/seed) tant qu'un besoin ERP concret ne les réclame pas. Le détail transporteur/mode (`transportation_lanes`, table enfant de `distribution_links`) reste également hors périmètre de ce fichier — un lien = un jeu de paramètres de planification, pas un catalogue de transporteurs.

## 9. Dépôt

Une fois l'ingestion construite : dossier Dropbox `ootils-inbox/`, nommage `distribution_links_<AAAAMMJJ>.tsv` (convention générale `SPEC-FICHIERS-ENTREE-OOTILS.md` §1) — envoyé uniquement lors d'un changement réseau.

## 10. Ordre de chargement

Nécessite `locations.tsv` (2 FK) et, si `item_external_id` est renseigné, `items.tsv`. Indépendant des flux quotidiens.
