# Revue stratégique — 5 juillet 2026 · barre « LA référence AI-native »

**Commit revu :** `7a0b91b` (main, 2026-07-05)
**Méthode :** audit runtime read-only par agent (49 lectures outillées, preuves `fichier:ligne`), croisé avec `docs/REVIEW-2026-07-APS.md` (2026-07-01) et l'état du backlog GitHub. Chaque constat de la revue de juillet a été re-vérifié au commit courant avant citation — six de ses constats les plus sévères sont **périmés** (le code a rattrapé en 4 jours).
**Décision du pilote (2026-07-05) :** l'ambition est actée — **LA référence de la catégorie « APS designed by AI, for AI »**, pas la parité de largeur avec les suites installées. Le pilote (expert métier mondial) apporte la touche business aux checkpoints marqués 🎯.

---

## Verdict

**3,4/10 (01/07) → ~5,8/10 (05/07)** vs un APS de référence, à barème constant. L'intégralité du backlog C0/C1/C2 de la revue de juillet est fermée (21 issues, 93 commits en 7 jours). Constats périmés depuis le 01/07, vérifiés au code : saisonnalité en production (`SeasonalForecaster` + golden), RCCP scénarisé (#338), reschedule/FPO livrés (ADR-026, migrations 061/062), diff/promote exposés avec détection de conflit (#341), intervalles conformal alimentés (`persist_run` repository.py:818, `persist_series_run` :1015), ATP scénarisé (`resolve_scenario_id` atp/routers.py:116).

**Mais « le meilleur APS du monde » ne se gagne pas en largeur** : Kinaxis/o9/Blue Yonder/SAP IBP ont 20 ans et des milliers d'ingénieurs d'avance — et aucun n'est « meilleur » partout. La catégorie **en cours de fondation, sans leader** : la planification supply chain **agent-native**. Les incumbents boulonnent des agents sur des moteurs opaques ; Ootils est le seul système connu construit agent-natif par fondation (déterminisme reproductible, tout-forkable, chaîne causale requêtable, contre-factuels honnêtes, gouvernance L0-L4, audit systématique). **C'est la course choisie. Tout est noté à cette aune.**

### Scores (barème identique au 01/07)

| Axe | 01/07 | 05/07 | Distance au podium |
|---|---|---|---|
| Demand planning | 2,5 | **6** | FVA inexistant ; réconciliation hiérarchique codée+testée-DB mais NON exposée (`recon_method` forcé `"none"`, pyramide.py:54) ; CI conformal écrits mais invisibles sur `/v1/forecast/runs/{id}/result` (`PyramideValueOut` sans bornes) ; pas de promo/causal/NPI |
| Supply (MRP/DRP) | 3,5 | **5,5** | **DRP = 4 dataclasses, zéro moteur/router ; `distribution_links`/`transportation_lanes` (mig 029) mortes.** Fossé n°1 pour un métier distribution |
| S&OP / capacité | 3 | **3,5** | Shipping Plan papier (#327) ; pas de calendrier S&OP ; RCCP orienté machine (le goulot distributeur = réception DC pré-saison) ; **CRP non scénarisé** (crp/routers.py sans `resolve_scenario_id` — dernier module en infraction avec la doctrine) |
| What-if / scénarios | 4 | **7,5** | Meilleur axe (overlay 15 params bout-en-bout, delta contre-factuel honnête #387, promote avec conflits). Manque : coût du fork deep-copy à l'échelle, comparaison scénarios en KPI métier |
| Master data | 4 | **6,5** | Le champ de bataille réel vs Kinaxis = temps d'implémentation : ingestion TSV manuelle, zéro connecteur, zéro CDC |
| Control tower / autonomie | 3,5 | **6** | Boucle amont complète (watcher→fork→évidence→reco gouvernée→approbation). Manque l'aval : écriture ERP, alerting, StreamChanges |

## Les 6 fossés vers le podium (audit runtime 2026-07-05)

1. **DRP inexistant au runtime.** `drp/` = `models.py` seul (4 dataclasses, drp/models.py:16-250) ; tables mig 029 référencées par zéro code de production. La réconciliation middle-out qui le débloque est livrée (#348) — il ne manque *que* le moteur de transfert.
2. **StreamChanges aspirationnel — les agents pollent.** Aucun SSE/WebSocket dans `api/` (grep exhaustif) ; `GET /v1/explain/stream` documenté (SPEC-INTERFACES.md:440) mais inexistant ; seul flux réel = gRPC Rust opt-in (service.rs:99). ROADMAP.md:76 l'admet en TODO. Écart intention/réalité maximal pour un produit « for AI ».
3. **Étage entreprise agents absent.** Un seul Bearer global (`client_id="global_token"`, auth.py:65), `actor_kind` auto-déclaré (recommendations.py:54), zéro scope/budget par agent, kill-switch d'une seule feature, zéro `/metrics` FastAPI (Prometheus = Rust uniquement). Le North Star l'exige avant tout pilote écrivain.
4. **Machine à preuve inexistante.** Pas de snapshots stock, pas de chaînage reco→résultat, zéro FVA (grep = 0 hors revues). Le ledger d'audit contient déjà ~80 % de la plomberie.
5. **Échelle réelle > échelle affichée, non prouvée.** SCALABILITY.md s'auto-contredit : en-tête « SMB CRITICAL / mid-market IMPOSSIBLE » (l.13-14) vs corps post-Tier-2 « 730 K nœuds en 3,8 min, VM 2 cœurs » (l.162-163), ~4 000 nœuds/s. Pilote : 36 635 items / 2,88 s (math core). Le chemin API reste à prouver à cette échelle ; la réponse Kinaxis (Rust ArcSwap fork ~1 µs) dort en opt-in (`OOTILS_ENGINE` défaut `sql`, events.py:56).
6. **Zéro shell produit.** Aucun asset UI (vérifié). Démo = reçu d'exécution JSONB (`demo_runs`) au curl. La Decision Ladder exige une surface d'approbation humaine L3/L4 qui n'a aucun visage.

## Déjà de classe mondiale (à vendre tel quel)

- Triplet **déterminisme + causalité persistée + audit** — offert ensemble par aucun APS du marché.
- **Confiance honnête** : défaut prudent 0,25, `stale_demand` gouverné (mig 056 + dq_findings), bornes conformal NULL-honnêtes, quantiles FM refusés par principe (ADR-024).
- **Profondeur APICS du cœur MRP** : fenêtre de consommation (#349), FPO + dampening + reschedule au centre de gravité (ADR-026).
- **Discipline d'ingénierie** : 62 migrations, 29 ADR, golden masters, mypy bloquant, 178 fichiers de tests, revue adversariale systématique — c'est ce qui rend la vélocité possible sans casse.

---

## Plan d'action — vagues A/B/C (exécution : agents Opus 4.8, orchestration Fable)

**Règles de vol :** 2 chantiers max en parallèle (fichiers disjoints) ; chaque milestone = PR mergée après revue adversariale + CI verte ; bilan à chaque merge ; tout test d'intégration écrit sans DB locale est calqué sur un pattern existant qui passe. 🎯 = checkpoint input métier pilote (défauts proposés, il corrige — jamais bloquant).

### Vague A — rendre le pitch AI-native VRAI (P0)

| # | Milestone | Contenu | 🎯 |
|---|---|---|---|
| A1 | **StreamChanges réel** | SSE `/v1/stream/changes` (events, recommandations, pénuries), curseurs rejouables (reprise déterministe), scenario-scopé, auth, heartbeat ; chemin de migration des watchers polling→subscribe | — |
| A2 | **Étage entreprise agents** | Tokens par agent (hashés) + scopes par famille de routes + budgets + kill-switch global + `/metrics` Prometheus FastAPI | — |
| A3 | **Machine à preuve** | Snapshots stock quotidiens (item×location×on_hand) + chaînage `recommendation_id→outcome` + **FVA v1** (vs naïve saisonnière, persisté) | KPI ROI |

### Vague B — couverture APS démo (objectif 2 mois inchangé)

| # | Milestone | Contenu | 🎯 |
|---|---|---|---|
| B1 | Finitions demande | Exposer CI conformal sur le résultat Pyramide + exposer la réconciliation hiérarchique via REST ; #373 MinT sparse | — |
| B2 | **DRP runtime minimal** | Moteur de transfert inter-site sur `distribution_links` → recommandations de transfert gouvernées L2, forkables (per-site demand #348 → DRP → MRP central, clôture ADR-020) | **politiques de transfert** (qté min, priorités lane, fair-share) |
| B3 | S&OP léger | #327 Shipping Plan runtime + calendrier S&OP squelette | **cadence du cycle** |

### Vague C — preuve (après A/B)

C1 échelle chemin API (36 635 items pilote) + vérité SCALABILITY.md · C2 surface de supervision minimale (approbation L3/L4, comparaison scénarios) 🎯 UX · C3 durcissement pilote (#192 restore testé, #191 CI).

### Quick wins immédiats

CRP scénarisé (pattern `resolve_scenario_id` d'ATP) · en-tête SCALABILITY.md remis à la vérité mesurée.

### Non-objectifs maintenus

Ordonnancement fin · solveur LP (jusqu'aux points de bascule documentés) · TMS/transport · retail-shelf.

---

*Revue et plan produits le 2026-07-05 (Fable + audit agent Opus). Toute citation `fichier:ligne` provient d'une lecture directe du code au commit `7a0b91b`.*
