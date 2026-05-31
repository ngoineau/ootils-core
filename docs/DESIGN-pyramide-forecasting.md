# DESIGN — Pyramide : architecture forecasting avancée

> **Statut** : **CONCEPTION** — pas implémenté. Addendum technique à
> [`ADR-019`](ADR-019-demand-model-pyramide.md) (décisions D1-D8 du module
> demande). Daté 2026-05-31.
> **Périmètre** : la *couche prévision* de Pyramide (le « comment on prévoit »).
> ADR-019 couvre le *quoi* (booking/shipping/backlog, dimensions, S&OP, deux
> modèles) ; ce doc couvre les **méthodes et l'architecture de calcul**.
> **Code de référence** : `src/ootils_core/pyramide/` (`engines.py`, `models.py`,
> `runner.py`, `repository.py`) — scaffolding existant.
> **Règle** : rien n'est livré ; on n'implémente qu'après cadrage de la 1ʳᵉ brique
> (`demand_history` + import réel) et réception des specs `Gen_Fam/Group/Prod`.

---

## 0. Objet

Transformer le moteur Pyramide actuel — un **routeur de modèles mono-série qui
prévoit plat** — en un **moteur hiérarchique, saisonnier, causal et gouverné**,
conforme au métier (saison piscine, zones climatiques US, hiérarchie produit
`Gen_*`, programmes d'achat S&OP, longue traîne SKU, réseau à 325 DC).

---

## 1. État actuel du moteur (honnête)

`PyramideForecastEngine` (`engines.py`) oriente 4 familles avec fallback :

| Méthode | État |
|---|---|
| Classique (MA, lissage expo, **Croston** intermittent) | ✅ réel, déterministe, sans dépendance |
| `AUTO_SELECT` (backtest des candidats) | ✅ réel |
| `ENSEMBLE_STAT` (mélange pondéré par l'erreur) | ✅ réel |
| `STAT_AUTOETS` / `STAT_AUTOARIMA` (Nixtla *statsforecast*) | ✅ réel (import paresseux + fallback) |
| `ML_LGBM` (LightGBM via *mlforecast*) | ✅ réel (idem) |
| `FM_CHRONOS` / `FM_MOIRAI` (modèles de fondation TS) | ⚠️ **STUB** — retombent sur AUTO_SELECT |

**4 trous critiques pour le métier :**
1. 🔴 **Saisonnalité absente du chemin par défaut** : classique/auto/ensemble
   sortent **une valeur répétée** (forecast plat). Seuls AutoETS/ARIMA et LGBM
   capturent la saison. Inacceptable pour une demande piscine saisonnière.
2. 🔴 **Réconciliation hiérarchique non implémentée** : `recon_method` est
   stocké mais inutilisé ; le moteur prévoit **une série par (article × site)**,
   sans middle-out ni MinT (≠ décision D4 d'ADR-019).
3. 🟠 **Exogène non câblé** (météo, programmes Buy).
4. 🟠 **Pas de vrai cadre d'accuracy** (backtest naïf 1-pas ; pas de MASE/WAPE/
   biais multi-horizon → pas de score de confiance solide).

Le **routeur et la taxonomie** sont un bon socle ; le « super avancé » est
aujourd'hui surtout du scaffolding.

---

## 2. Les 4 axes

### A — Saisonnalité + réconciliation hiérarchique *(le socle)*
- **Saisonnalité native** : modèles sortant une **courbe** (AutoETS/ARIMA
  saisonniers `season_length` 52/12 ; **MSTL** si multi-saisonnalité). La
  **forme** de la saison diffère **par zone climatique** (sun belt étalé vs snow
  belt pic court) → apprise par zone.
- **Réconciliation croisée** : produit (`Gen_Fam→Gen_Group→Gen_Prod`) × géo
  (`zone climatique→état→DC`) = hiérarchie **groupée** (plusieurs chemins vers un
  agrégat). Détail §3.

### B — Modèles de fondation (Chronos) *(la longue traîne)*
- Transformers TS **pré-entraînés**, **zero-shot** (pas d'entraînement par
  série), sortie **probabiliste**.
- **🔒 Licence — TRANCHÉ (vérifié à la source 2026-05-31)** :
  - **Chronos / Chronos-Bolt** (Amazon) = **Apache-2.0** → usage commercial OK →
    **modèle de fondation par défaut d'Ootils.**
  - **Moirai** (Salesforce) = poids **`cc-by-nc-4.0`** (non-commercial,
    « research purposes only » ; version proprio réservée à Salesforce) →
    **EXCLU pour un usage commercial.** Le framework `uni2ts` est permissif mais
    les *poids* ne le sont pas, et réentraîner est irréaliste. Réf :
    fiche HF `Salesforce/moirai-2.0-R-small` + `amazon/chronos-bolt-small`.
  - Réserve commerciale-safe alternative : **TimesFM** (Google, Apache-2.0).
  - **Covariables** (météo/Buy qu'on visait via Moirai) → couvertes par **LGBM +
    exogène (axe C)** ; pas besoin de Moirai.
- **Pour toi** : milliers de SKU à **faible historique** (cold-start, longue
  traîne) où stat/ML échouent. Détail du routage §5.
- **Câblage** : packages avec poids HuggingFace, **dépendance optionnelle**
  (`ootils[foundation]`) + import paresseux + fallback (pattern existant). Charger
  le modèle **une seule fois** (cache), **inférence par batch** (jamais série par
  série), batch **offline** (runner → `forecasts` → matérialise `ForecastDemand`),
  poids **pré-téléchargés** pour une VM hors-ligne.
- **Caveats** : échelle (millions de séries → pas tout en FM, cf. §5/§6) ;
  **déterminisme** = moteur déterministe, FM au **bord stochastique**, forecast
  FM = artefact **daté, seedé, versionné, loggué** (jamais dans la propagation).

### C — Variables exogènes *(l'avantage causal)*
- Demande piscine **météo-dirigée** + rythmée par les **programmes Buy** (March/
  June/Early Buy → décalages de timing distributeurs).
- **Comment** : LGBM/mlforecast (`static_features` + exogènes futurs), ARIMAX,
  Moirai (covariables). Features : température par zone, jours avant/après chaque
  Buy, phase de saison, fériés.
- **Subtilité** : l'exogène futur doit être **connu sur l'horizon** — programmes
  Buy ✅ (calendrier S&OP) ; météo ✅ ~2 semaines puis **normales climatiques**.
- C'est ici que le modèle **apprend les décalages de timing** (early/late buy).

### D — Accuracy + score de confiance *(la gouvernance)*
- **Backtest sérieux** : rolling-origin multi-cutoffs, **multi-horizon** (h=1..H)
  — Nixtla `cross_validation`.
- **Métriques métier** : **MASE** (sans échelle), **WAPE/sMAPE** (pondéré volume,
  lisible en % et en $ via ASP), **biais** (critique stock), **couverture** des
  intervalles.
- **Alimente** : sélection de modèle, **score de confiance** par forecast
  (=f(accuracy récente, profondeur historique, DQ/fraîcheur)) sur lequel les
  agents s'autorisent à agir, **poids MinT** (covariance des erreurs),
  **intervalles probabilistes** → safety stock par niveau de service.
- **Fraîcheur** : liée au SLA d'ingestion `demand_history` (feed périmé → flag →
  blocage action autonome).

---

## 3. Deep-dive — réconciliation croisée

**Pas un arbre : un croisé.** Même `Gen_Prod` dans plusieurs DC, même DC porte
plusieurs produits → **plusieurs chemins** vers un agrégat (hiérarchie *grouped*).

**Feuilles** = `Gen_Prod × DC`. **Agrégats** voulus cohérents : par produit, par
DC, par zone climatique, par **`Gen_Group × zone`** (niveau de prévision D4),
grand total. Ex. `Pompes×Sun = P1×S1 + P1×S2 + P2×S1 + P2×S2`.

**Matrice de sommation `S`** : chaque ligne = une série, chaque colonne = une
feuille ; encode « série = somme de ces feuilles ». Cohérence ⇔
`ŷ = S · b̂` (tout se déduit des feuilles réconciliées). `S` se **construit
automatiquement** depuis `dimension_hierarchy` (produit × géo) — générique.

**Réconciliation** : `ŷ_réc = S · G · ŷ_base`. Le choix de `G` = la méthode :
- **Bottom-up** / **Top-down** / **Middle-out (D4)** / **MinT** (optimal).
- **MinT (Minimum Trace)** : `G` qui minimise la variance d'erreur réconciliée
  via la **covariance des erreurs** → une série de traîne **emprunte** la
  saisonnalité fiable de sa famille. Variantes : OLS < **WLS** (diag des
  variances) < **MinT-shrink** (covariance régularisée Ledoit-Wolf, obligatoire à
  grand nombre de séries).

**⚠️ Mur d'échelle** : 325 DC × milliers de SKU = millions de feuilles → `S` /
covariance pleines **infaisables**. Stratégies (à trancher à l'implémentation) :
- **Réconcilier par blocs** : un `S` **par `Gen_Fam`** (familles indépendantes
  côté demande) → N problèmes moyens parallélisables.
- **`S` creuse** (sparse), jamais dense.
- **MinT-shrink / WLS** (jamais de covariance pleine dense).
- **Réconciliation 2 étapes** (géo puis produit) comme approximation du croisé —
  à benchmarker vs le croisé exact sur un `Gen_Fam`.

**Déterminisme ✓** : la réconciliation est de l'algèbre linéaire (cœur
déterministe) ; les forecasts de base sont le bord stochastique.

---

## 4. Deep-dive — désagrégation middle-out

**Tension** : redescendre `Gen_Group×zone → Gen_Prod×DC` par des **parts**.
Historiques (stables, en retard sur le mix) vs prévues (mix-aware, réintroduisent
le bruit feuille).

**Les parts ne sont pas constantes** : le **mix change au fil de la saison**
(produits early-season vs late-season) et **par zone** → proportions = **profils
temporels par phase de saison × zone**, pas un scalaire (relie au calendrier C).

**Cold-start (nouveaux produits, fréquent en piscine)** : pas de part historique
→ (a) profil d'un **produit jumeau** (même Gen_Group/attributs), (b) **courbe de
lancement**, ou (c) **FM (B)** prévoit la feuille puis **MinT réconcilie** — *c'est
ici que B se branche dans A*.

**Contrainte réseau** : chaque DC sert des états + types produits précis → un
couple non desservi a une part **structurellement zéro** (pas « faible »). La
carte DC↔états↔familles pose des **zéros durs** dans `S`. Évite de pousser du
stock vers un DC qui ne porte pas le produit.

**Unités vs valeur** : on désagrège et réconcilie **en unités** (base additive) ;
la **valeur = unités × ASP** (ASP niveau produit) se calcule **après**. On ne
réconcilie jamais la valeur directement.

**La vraie réponse moderne** : **MinT n'a PAS besoin de proportions explicites**
(il dérive `G` de la covariance). Donc :
- **Réconciliation par défaut = MinT-shrink** (mix-aware, optimal).
- **Proportions explicites conservées** pour (1) **cold-start** et (2)
  **explicabilité** (une part « 60/40 selon la part de saison glissante » est
  lisible par un humain/agent ; MinT est plus opaque → compte pour les décisions
  gouvernées).

---

## 5. Deep-dive — routage tête/traîne

**Pourquoi** : millions de séries → impossible de tout passer en toutes méthodes.
On **route** chaque série vers le bon **moteur ET le bon niveau**.

**Insight** : router = choisir une **méthode** *et* un **niveau**. Une série de
traîne se prévoit souvent **en agrégé** (`Gen_Group×zone`) + désagrégation MinT,
**pas** en FM au niveau feuille. FM = forecast **direct feuille** quand la série
est nouvelle/sparse **et** sans jumeau ni agrégat suffisant.

**Axes de classification** : profondeur d'historique ; intermittence (taux de
zéros) ; **volume/valeur (ABC)** via unités×ASP ; force de saisonnalité ; **cycle
de vie** (lancement / mature / fin de vie).

**Arbre de décision** :
```
cold-start ?      → jumeau / FM (B) + MinT
intermittente ?   → Croston / TSB (feuille)
tête (histo riche + A-class + saison) → stat saisonnier ou LGBM+exogène, MinT  [meilleur]
moyenne (B-class) → AUTO_SELECT stat, MinT
traîne (sparse / C-class / signal faible) → prévoir AGRÉGÉ + désagréger MinT
                                            (FM si l'agrégat manque de signal)
fin de vie        → décroissance bornée, NE PAS extrapoler la saison
```

**Piloté par les données, pas codé en dur** : on backteste les **familles par
CLASSE de série** et l'**accuracy (D)** décide qui gagne ; le routeur
s'auto-améliore quand la donnée change.

**Économie à l'échelle** : stat = quotidien (gros parc) ; MinT-shrink par
`Gen_Fam` = quotidien ; LGBM = quotidien/hebdo ; **FM = réservé** (cold-start +
traîne à valeur), hebdo. **Incrémental/streamable** : ne re-prévoir que les séries
dont la donnée a changé (`demand.history.appended`), pas tout chaque nuit.

**Gouvernance** : chaque forecast porte sa **provenance** (méthode, niveau,
pourquoi ce routage) — déterministe, loggué, auditable, explicable.

---

## 6. Vue d'ensemble

```
        ┌─────────── ROUTEUR (features par série, piloté par accuracy D) ───────┐
        ▼                                                                        ▼
   TÊTE : stat saisonnier + exogène (C), niveau feuille/milieu      TRAÎNE : agrégé + désagrégation,
                                                                     ou FM (B) si vraiment nouveau
        └────────────────────► RÉCONCILIATION MinT (A) ◄────────────────────────┘
                               cohérence tous niveaux
                               → MRP (agrégat, safety pooled) + DRP (Gen_Prod×DC)
```
A = colonne vertébrale (saison + cohérence) ; B = longue traîne ; C = lift causal ;
D = confiance + boucle dans A (covariance) et le safety stock.

---

## 7. Ordre d'implémentation (quand le chantier sera dé-parké)

Prérequis : `demand_history` chargé (booking+shipping+valeur) + specs `Gen_*` +
table `dimension_hierarchy` (produit × géo) + carte DC↔états↔familles.

1. **A (socle)** : saisonnalité native + construction de `S` (par `Gen_Fam`) +
   MinT-shrink (Nixtla `statsforecast` + `hierarchicalforecast`).
2. **D (gouvernance)** : backtest rolling-origin + MASE/WAPE/biais + score de
   confiance. (A+D = couple socle indispensable.)
3. **C (causal)** : exogène (météo + calendrier Buy) sur LGBM/ARIMAX.
4. **B (traîne)** : câbler Chronos/Moirai (remplacer les stubs) + routage
   tête/traîne complet.

B avant C/D donnerait des FM non gouvernés et non causaux — moins utile.

---

## 8. Non-objectifs / questions ouvertes

- **Hors V1** : multi-devise/UoM, Demand Anomaly Watcher, StreamChanges dédié
  demande (mais l'incrémental est un objectif d'échelle).
- **À trancher à l'implémentation** : croisé exact vs réconciliation 2 étapes
  (bench par `Gen_Fam`) ; cadence FM ; stratégie cold-start par défaut
  (jumeau vs FM) ; granularité des profils de proportion (par phase × zone).
- **Dépend des specs métier** : structure exacte `Gen_Fam/Group/Prod` ;
  définition des zones climatiques ; carte DC↔états↔familles ; fenêtres des
  programmes Buy par année.
