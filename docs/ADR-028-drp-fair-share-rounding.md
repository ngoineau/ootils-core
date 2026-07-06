# ADR-028 — DRP fair-share proportionnel + arrondi logistique descendant

**Statut :** Accepté — chantier #395 **PR2a CLOSE** (le cœur `engine/drp/core.py` et la migration 065 sont mergés ; PR2b — recommandations gouvernées, watcher, endpoint, seed — reste à venir, cf. §Suite). Les deux règles métier tranchées par le pilote (fair-share, arrondi descendant) et les trois choix d'implémentation de l'architecte sont gravés ici.
**Date :** 2026-07-06
**Contexte mesuré :** décision pilote du 2026-07-06 ; docstrings du cœur DRP (`engine/drp/core.py` §SCOPE, `transfer_signals`, `_fair_share_round`) et migration `065_distribution_links_transfer_multiple.sql`, qui référencent tous cet ADR.

---

## Contexte

L'échelon DRP (ADR-020 §Unité de planification) déplace du stock **fini** entre sites : une source excédentaire couvre le déficit d'un ou plusieurs sites qu'elle dessert. La première version du cœur DRP répartissait cet excédent en **greedy priorisé, dest-first** : le premier site (dans l'ordre de priorité) vidait la source jusqu'à saturation de son besoin, puis le suivant se servait sur le reste.

Ce comportement a un défaut métier grave, remonté par le pilote (expert métier mondial de son domaine) : la **famine des dealers B**. Quand une source ne peut pas couvrir tous les sites en déficit qu'elle dessert, un site basse-priorité en pénurie prolongée reste à **zéro** run après run, pendant qu'un site prioritaire est servi à **100 %**. Le greedy dest-first traduit « préférence de sourcing » en « exclusivité de sourcing » — ce qui n'est pas ce que veut le métier : une priorité doit *incliner* le partage, pas *affamer* le palier inférieur.

Second point : le greedy travaillait en quantités continues, alors qu'un transfert physique se fait par **unités logistiques** (colis, palette, camion complet). Rien dans le modèle ne permettait de dire « on expédie des colis entiers, pas des fractions », ni de choisir le sens d'arrondi — question d'autant plus sensible qu'un transfert DRP a une contrainte de conservation que le MRP n'a pas (voir Décision, point 5).

Deux règles métier sont donc tranchées par le pilote (fair-share proportionnel ; arrondi à un multiple logistique configurable), et trois choix d'implémentation par l'architecte (algorithme exact et ordre déterministe ; interaction priorité × fair-share ; sens et gestion du reliquat de l'arrondi). Le cœur `engine/drp/core.py` implémente déjà la totalité ; cet ADR fige les décisions.

## Décision

### 1. Fair-share proportionnel remplace le greedy priorisé — décision **PILOTE**

Quand une source ne peut pas couvrir tous les sites en déficit qu'elle dessert, son excédent est réparti au **prorata des déficits résiduels** de ces sites, au lieu que le premier draine la source. Raison métier : tuer la **famine des dealers B** — un site basse-priorité ne doit plus rester à zéro pendant qu'un site prioritaire est à 100 %. La règle vit en un seul endroit (`transfer_signals`) :

```python
part_dest = avail_snapshot * (residual_deficit_dest / Σ residual_deficits)
```

`avail_snapshot` est l'excédent de la source figé au **début** du palier de priorité traité, de sorte que la répartition est une vraie proportion (la somme des parts idéales égale le snapshot) ; le compteur d'excédent **vivant** est ce qui est réellement décrémenté, donc aucun jeu d'arrondi ou de reliquat n'est jamais sur-tiré.

### 2. Arrondi à un multiple logistique configurable — décision **PILOTE**

Chaque lane porte un multiple d'expédition : `distribution_links.transfer_multiple` (migration 065, `NUMERIC(18,6)`, `CHECK > 0`, **DEFAULT 1 = pas d'arrondi**). Toute lane antérieure à la colonne, ou tout link de test construit à la main, garde donc exactement le comportement continu pré-arrondi. La quantité transférée est arrondie à un multiple entier de cette valeur (sens et reliquat : point 5).

### 3. Algorithme fair-share = **option (a) par source, ordre total figé** — architecte

L'itération est **source-first** : boucle externe sur le palier de priorité (ascendant), puis sur les **sources** actives à ce palier dans l'ordre total figé `(priority_min(source), source_location, item)`, chaque source répartissant son excédent restant au prorata des déficits résiduels des destinations qu'elle dessert à ce palier. La **destination est la dimension interne** — l'exact opposé de l'ancien drain dest-first. L'ordre de traitement (palier → sources → destinations triées) est entièrement déterministe, et la clé de tri de **sortie** est logiquement inchangée (`item, dest_location, deficit_bucket, priority, source_location, link_ref, …`), donc le plan émis est **byte-stable** run-to-run, indépendant de l'ordre d'insertion des dicts/listes d'entrée.

> **Le déterminisme run-to-run est l'invariant dur.** C'est le critère qui a écarté les alternatives (voir §Alternatives rejetées) : toute variante dont la stabilité byte-à-byte serait non triviale à prouver est refusée.

**Déterminisme désormais INCONDITIONNEL (revue adversariale #395 PR2a, fix MINOR).** La clé initiale `(priority, source_location, link_ref)` n'était une clé de tri **totale** qu'en pratique, pas par construction : `link_ref` est unique pour toute lane réellement chargée par le loader (il vient de `distribution_links.distribution_link_id`, une UUID PRIMARY KEY), mais un `TransferLink` construit à la main (tests, ou tout futur appelant) laissé à la valeur par défaut `""` peut entrer en collision avec un autre — à ce point, la clé n'était plus totale et l'issue retombait silencieusement sur l'ordre d'insertion, qui n'est pas un signal métier. Le fix ajoute un tie-break final sur les champs discriminants restants de la lane : la clé unique et partagée `_sort_key(link)` est désormais **`(priority, source_location, link_ref, max_qty, min_qty, transfer_multiple)`**, avec `max_qty=None` mappé à `+inf` (une lane non plafonnée trie après toute lane plafonnée à parité des champs précédents — choix arbitraire mais fixe, seule l'existence d'un ordre total déterministe compte, pas un sens particulier). Cette clé est désormais **la seule** utilisée aux trois sites où un ensemble de lanes candidates est ordonné (`_resolve_candidate_links`, la boucle interne de répartition fair-share, le tri de sortie de `transfer_signals`) — un seul endroit de définition, plus de dérive possible entre les trois. Deux lanes identiques sur **tous** ces champs sont désormais véritablement interchangeables : même sortie quel que soit l'ordre d'appel. Deux lanes différant sur un seul champ, aussi mineur soit-il, trient de façon déterministe par cette différence — jamais par accident de site d'appel.

### 4. Interaction priorité × fair-share = **greedy ENTRE paliers, fair-share À priorité égale**

Les lanes sont servies par palier de priorité **ascendant** ; le fair-share ne s'applique **qu'entre lanes de priorité égale**. Entre paliers, le comportement est **greedy** : un déficit qu'une lane p1 couvre voit son résiduel **rétrécir avant qu'aucune lane p2 ne soit même considérée**, et ce **globalement à travers toutes les sources** (c'est pourquoi la boucle externe est le palier de priorité, pas la source : c'est le seul ordre sous lequel toute lane p1, quelle que soit sa source, agit avant toute lane p2 — c'est le sens de « priorité » ici).

L'interrupteur module `_FAIR_SHARE_RESPECTS_PRIORITY` (constante, **défaut `True`**) gouverne ce comportement. À `False`, tous les candidats s'effondrent en **un seul palier virtuel** : fair-share pur sur toutes les lanes, priorité **ignorée** pour la stratification (la priorité réelle de la lane reste émise sur le signal et reste dans la clé de tri de sortie). Le pilote peut basculer tout le comportement trivialement.

> ### 🎯 Arbitrage métier OUVERT — ajustable par le pilote
>
> **Tension à documenter explicitement.** Le défaut (`True`) tue la famine entre dealers du **même palier** — c'est exactement le cas remonté par le pilote et ce que la V1 corrige. **Mais** un dealer sur une lane **basse-priorité** peut encore être servi **après** un dealer premium (greedy entre paliers) : le défaut n'élimine donc pas toute forme d'attente inter-palier, seulement la famine intra-palier.
>
> Si le pilote veut du **fair-share pur** — priorité entièrement ignorée, tous les dealers partagés au prorata quel que soit le palier — il bascule `_FAIR_SHARE_RESPECTS_PRIORITY` à `False`. Le choix entre « priorité respectée entre paliers » et « fair-share plat » est un **arbitrage métier ouvert**, laissé à la main du pilote, pas un invariant technique.

### 5. Arrondi **INFÉRIEUR (floor) borné**, reliquat reversé — architecte

Chaque part fair-share est arrondie **vers le bas** au multiple entier le plus proche, bornée par le besoin réel (`_fair_share_round`) :

```python
qty_brute = min(raw_part, avail, max_qty if not None)   # besoin borné
qty       = floor(qty_brute / mult) * mult              # DESCENDANT
```

C'est une divergence **VOLONTAIRE** du `lot_size` MRP, qui fait `ceil` (`engine/mrp/core.py:34`, `math.ceil(qty / mult) * mult`). Justification : le **MRP fabrique / achète** — sur-commander pour atteindre un multiple est acceptable (on satisfait le besoin et on porte un petit surplus). Le **DRP DÉPLACE du stock fini** — sur-transférer **PRIVE la source** de stock qu'elle peut encore avoir besoin. Le floor est donc la borne conservatrice : on expédie des colis entiers, jamais plus que le besoin. Même idée « respecter un multiple », **sens d'arrondi opposé**, parce qu'un moteur crée de la supply et l'autre ne fait que la relocaliser — divergence assumée, pas une incohérence à réconcilier.

Le **reliquat** de l'arrondi (`qty_brute - qty`) n'est **jamais consommé** : il reste dans l'excédent vivant de la source, disponible pour la destination suivante / le palier suivant (jamais perdu — c'est la propriété `rounding_remnant` portée sur chaque `TransferSignal`, preuve d'explicabilité ADR-004).

Deux règles de bord, business defaults verrouillés mais **🎯 ajustables** :

- **Sous le minimum de lane → 0.** Si la quantité arrondie est `< min_qty`, aucun transfert sur cette lane (règle minimum-shipment existante) : la lane ne peut physiquement pas expédier sous son minimum, et on ne gonfle jamais le besoin jusqu'à lui. Le déficit reste pour la lane suivante.
- **Déficit résiduel dégénéré `< multiple` → 0.** Quand `qty_brute < mult`, le floor vaut déjà 0 : pas de micro-transfert d'un colis pour un besoin sous le colis — couvert par le floor lui-même, sans cas spécial.

### 6. Passe de balayage du reliquat (« remnant sweep ») — architecte, revue adversariale #395 PR2a (fix MAJOR)

**Défaut découvert :** le fair-share proportionnel **seul** (points 1+5 ci-dessus, sans ce point 6) peut bloquer un excédent entier à zéro alors qu'il pourrait servir quelqu'un. Exemple qui casse la répartition proportionnelle nue : excès de la source = 12, `transfer_multiple` = 12, **deux** destinations de même palier chacune en déficit de 20. La part idéale proportionnelle est 6/6 ; `_fair_share_round` arrondit **chacune** des deux parts à 0 (une demi-palette ne tient pas) → **aucune des deux destinations n'est servie**, et les 12 unités d'excédent réel restent inertes. C'est **strictement pire** que l'ancien greedy dest-first, qui aurait au moins expédié une palette entière à la première destination rencontrée — et une violation directe de l'intention anti-famine du fair-share (une source rare ne doit jamais finir par ne servir **personne** alors qu'au moins un multiple entier est expédiable à **quelqu'un**).

**Correction :** après la passe de répartition proportionnelle d'une source à un palier donné (points 1+5), une **passe de balayage** répète : tant que l'excédent vivant de la source peut encore expédier **≥ 1 multiple entier** à une destination de ce palier dont le résiduel est `> 0`, servir la destination **la plus nécessiteuse** — résiduel restant le plus grand, tie-break `dest_location` alphabétique — avec la plus grande quantité de multiple entier qui tient (`floor(min(résiduel, excédent, max_qty restant) / multiple) * multiple`), essayée lane par lane dans l'ordre `_sort_key`. Une destination qu'aucune de ses lanes ne peut servir (bloquée par `min_qty`, un `max_qty` restant épuisé, ou un résiduel réellement `< 1` multiple) est marquée **épuisée pour ce (source, palier)** et n'est jamais retentée — l'excédent vivant ne fait que décroître pendant le balayage, donc une destination non servable maintenant ne peut jamais le redevenir plus tard dans le même balayage (borne la boucle : chaque itération soit expédie un multiple, soit épuise définitivement une destination — deux quantités strictement décroissantes et finies).

> ### 🎯 Arbitrage métier OUVERT — ajustable par le pilote
>
> **« Le plus nécessiteux d'abord » n'est PAS le seul choix défendable.** V1 sert la destination au plus grand résiduel restant en premier — c'est le correctif anti-famine le plus conservateur (servir en priorité celui qui est le plus loin d'être couvert). L'alternative explicite et tout aussi défendable est le **ROUND-ROBIN** entre destinations à égalité : étaler les palettes disponibles entre les dealers plutôt que concentrer chaque palette entière sur le seul dealer le plus en manque. Le choix entre « le plus nécessiteux d'abord » et « round-robin » est un **arbitrage métier ouvert** — le pilote tranchera ; ce n'est pas un invariant technique, et une future révision peut rendre ce choix aussi trivialement bascule-able que l'interrupteur du point 4.

`transfer_multiple` = 1 (le défaut) n'est **jamais affecté en pratique** par cette passe : la répartition proportionnelle couvre déjà tout résiduel jusqu'à l'unité entière, donc il ne reste jamais de multiple entier non tiré à trouver — la boucle de balayage ne trouve rien à faire et sort immédiatement.

## Alternatives rejetées

- **Greedy priorisé dest-first (l'existant).** Rejeté par le pilote : traduit une préférence de sourcing en exclusivité de sourcing → **famine des dealers B**. C'est la motivation même de #395 PR2a.
- **(b) Water-filling global.** Rejeté : convergence **non triviale à prouver byte-stable** run-to-run — l'invariant dur (§Décision point 3) exclut toute variante dont on ne peut garantir trivialement la stabilité byte-à-byte. Sur-ingénierie pour une démo.
- **(c) Deux passes (allocation continue puis passe d'arrondi séparée).** Rejeté : complexité **reliquat × arrondi** — deux couches de résiduel à réconcilier, alors que l'option (a) traite l'arrondi et le report du reliquat dans la même passe source-first.
- **Arrondi supérieur (`ceil`, aligné sur le `lot_size` MRP).** Rejeté : sur-transférerait du stock fini et **priverait la source** (violation de l'invariant de conservation) — le MRP peut sur-commander parce qu'il crée de la supply, le DRP ne le peut pas parce qu'il la déplace (§Décision point 5).
- **Priorité entièrement ignorée par défaut (fair-share plat).** Non rejeté sur le principe — c'est précisément l'autre position de l'interrupteur `_FAIR_SHARE_RESPECTS_PRIORITY`. Le **défaut** est « priorité respectée entre paliers » ; le fair-share plat reste à un flip du pilote (§Décision point 4, encart 🎯).

## Conséquences

- **Positif :** la famine des dealers du même palier est éliminée ; les transferts respectent une granularité logistique par lane sans jamais sur-transférer ; le plan reste déterministe et explicable (chaque signal porte `deficit_qty`, `source_excess_before`, `fair_share_qty`, `rounding_remnant`).

- **Invariants garantis (à vérifier en test — voir §Suite / test-writer #395 PR2a) :**

  | Invariant | Énoncé |
  |---|---|
  | Conservation source | Σ des quantités sortant d'une source ≤ son excédent |
  | Conservation dest | Σ des quantités reçues par une destination ≤ son déficit |
  | Déterminisme total | plan byte-stable run-to-run, indépendant de l'ordre d'insertion des entrées |
  | Anti-blocage palette | aucune palette (multiple) entière d'excédent ne reste bloquée tant qu'une destination reliée peut la recevoir — corrigé par la passe de balayage (point 6) |
  | `min_qty` préservé | aucun transfert sous le minimum de lane ; une quantité arrondie `< min_qty` → 0 |
  | Cœur DB-free | `core.py` reste pur, sans I/O ; le chargement DB vit dans `loader.py` (SELECT-only) |

  **⚠ L'invariant « anti-blocage palette » n'est PAS « toute destination reçoit `> 0` ».** Cette formulation plus forte est **mathématiquement inatteignable** sous arrondi palette dès qu'une source ne peut expédier qu'**un seul** multiple entier à **plusieurs** destinations en déficit du même palier (excès=12, multiple=12, deux destinations : une seule palette tient, donc l'une des deux reçoit forcément 0 — on ne coupe pas une palette). L'invariant réellement garanti est plus précis et plus vrai : **aucun multiple entier expédiable à SOMEONE ne reste inerte dans l'excédent d'une source** — c'est-à-dire que si la source peut expédier ≥ 1 multiple entier à une destination dont le résiduel est ≥ ce multiple, ce transfert **est émis**, sur SOMEONE (le point 6). Sans la passe de balayage (fair-share proportionnel **seul**, points 1+5), une part fractionnaire peut être rognée sous le multiple pour **chaque** destination du palier simultanément (0,5 palette chacune, dans l'exemple 12/12/deux-dests) → **zéro transfert émis du tout**, un résultat strictement pire que l'ancien greedy dest-first (qui aurait au moins servi la première destination). La passe de balayage (point 6) est ce qui restaure l'anti-famine réelle après le fair-share proportionnel.

- **Négatif / dette assumée en V1 :**
  - La passe de balayage (point 6) ajoute une boucle et un état (`shipped_by_link` par identité de lane, `exhausted_dests`) au-dessus de la passe proportionnelle simple — complexité assumée pour fermer un vrai trou anti-famine (0/0 sur toutes les destinations d'un palier), pas une fonctionnalité de confort.
  - Le choix « le plus nécessiteux d'abord » dans la passe de balayage reste un **arbitrage métier ouvert** (round-robin est l'alternative défendable) — encart 🎯 du point 6.
  - Le défaut `_FAIR_SHARE_RESPECTS_PRIORITY=True` n'élimine pas l'attente inter-palier (un dealer basse-priorité peut être servi après un dealer premium) — **arbitrage métier ouvert**, à la main du pilote (encart 🎯).
  - **Single-hop uniquement** : un déficit est servi directement depuis l'excédent d'une source reliée, jamais relayé (source → hub → dest hors scope).
  - **Excédent = test horizon-total**, pas time-phased : borne basse sûre de ce qu'une source peut céder ; un excédent transitoire en début d'horizon qu'elle consomme plus tard n'est pas offert. Raffinement PR2+.
  - **Demande nette = `max(orders, forecast)` par bucket** : la fenêtre de consommation de prévision #349 (backward-before-forward, cross-bucket) est de la machinerie item-level dans `mrp/core.consume_demand` ; une variante per-location est un raffinement PR2+ délibérément non réimplémenté (`window==0` reproduit exactement la sémantique golden-master `max`).
  - Pas d'émission `StreamChanges` au niveau du cœur — cohérent avec le reste du chemin de calcul (le cœur est pur) ; l'émission gouvernée arrive avec PR2b.

- **Reste à faire :** voir §Suite. Cet ADR ne sera référençable comme « chantier #395 clos » qu'une fois PR2b mergée.

## Suite (hors scope PR2a)

- **PR2b :** recommandations **TRANSFER gouvernées L1** (canal `recommendations`, machine d'états #341, jamais une table de faits non-gouvernée — même principe qu'ADR-021/ADR-026) + **watcher scenario-backed** (fork contre-factuel, à l'image de #340) + endpoint **`POST /v1/drp/run`** + **seed démo** DRP + `agent_governance` action **TRANSFER** (`decision_level`) + **CHECK vocabulaire** de l'action. Aucun de ces éléments n'existe encore — la PR2a livre uniquement le cœur math + la colonne.
- **ADR-020 « per-site → DRP → central » :** cet ADR-028 est une **brique** de la cascade à deux échelons décidée en ADR-020 §Unité de planification (DRP per-site → MRP central, même maths de netting sur des arcs de graphe différents). La dépendance amont d'ADR-020 tient : la fusion utile suppose que Pyramide livre la demande **per-site** ; tant que la demande pilote est mono-location, l'échelon DRP tourne à vide.

## Références

- **#395** — chantier DRP fair-share (PR2a : cœur + migration 065).
- **Décision pilote du 2026-07-06** — fair-share proportionnel (anti-famine dealers B) + arrondi à un multiple logistique configurable.
- `src/ootils_core/engine/drp/core.py` — cœur pur : `transfer_signals`, `projected_deficits`, `excess_by_location`, `_fair_share_round`, `_resolve_candidate_links`, `_FAIR_SHARE_RESPECTS_PRIORITY`, dataclasses `TransferLink` / `TransferSignal`.
- `src/ootils_core/engine/drp/loader.py` — chargement SELECT-only, scenario-parameterized (safety stock overlay-resolved #347) → `DRPData`.
- `src/ootils_core/db/migrations/065_distribution_links_transfer_multiple.sql` — `distribution_links.transfer_multiple` (`NUMERIC(18,6)`, `CHECK > 0`, DEFAULT 1).
- `src/ootils_core/db/migrations/029_drp_models.sql` — table `distribution_links` d'origine.
- `src/ootils_core/engine/mrp/core.py:34` — `lot_size` (`math.ceil`), le **contraste** revendiqué du sens d'arrondi (MRP crée de la supply → `ceil` ; DRP la déplace → `floor`).
- `docs/ADR-020-mrp-consolidation.md` — §Unité de planification (per-site → DRP → central) dont ceci est une brique.
- `docs/ADR-021-shortage-truth.md`, `docs/ADR-026-reschedule-fpo.md` — précédents « les watchers émettent des DRAFT gouvernés, jamais dans une table de faits non-gouvernée » (à appliquer en PR2b).
