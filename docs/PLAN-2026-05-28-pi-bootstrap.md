# Plan d'action — 2026-05-28

> **Contexte** : Hier soir (2026-05-27, 19h50-22h30) on a livré `scripts/bulk_ingest.py` et chargé 329K rows pilote en DB `ootils_pilote_test`. Reste à :
> 1. Bootstrap le graphe PI (ProjectedInventory) pour activer la propagation
> 2. Vérifier que le moteur calcule des projections cohérentes + détecte des shortages
> 3. Décider l'architecture pour les évolutions V1.1
> 4. Commit/push tout sur GitHub

État DB actuel : 329 236 rows, 0 PI, 0 edges. Détail : `memory/session_2026-05-27_pilote_import.md`.

---

## 1. Décisions architecturales à trancher (BLOQUANT)

Ces 3 questions conditionnent tout le reste. À discuter en début de session.

### D1 — Où vit le PI bootstrap : Python/SQL ou Rust ?

**Le débat** : Architecture B (ADR-017) positionne le Rust engine comme moteur de compute. Mais l'initialisation du graphe PI (création projection_series + N buckets/série + edges) est plus du **write-heavy SQL** que du compute. Les vrais APS du marché font ça côté DB.

| Option | Pour | Contre |
|---|---|---|
| **A. Python+SQL one-shot (`bootstrap_pi.py`)** | Aligne avec `seed/projection/graph.py` existant. Write-heavy convient à PG bulk. Rapide à finaliser. | Code en double avec ce que pourrait faire le Rust engine. |
| **B. Rust engine bootstrap** | Architecturalement plus pur (un seul lieu pour la logique de graphe PI). | Code Rust à écrire. Risque d'overengineering pour une opération one-shot. |
| **C. Mixte** : Python crée les `projection_series` + buckets vides ; Rust engine wire les edges et propage | Sépare init structurelle (SQL) et compute (Rust). | Plus de code, deux endroits à maintenir. |

**Reco** : **A pour V1**, **C en V2 quand Rust engine sera plus mature**. Justification : `seed/projection/graph.py` est déjà sur cette voie, c'est ce qui marche aujourd'hui.

### D2 — Granularité de l'horizon 540 jours

| Option | Buckets/série | PI nodes total (35K séries) | Réalisme |
|---|---|---|---|
| **Daily pur** | 540 | 19 M | Brutal, irréaliste sur 18 mois |
| **Multi-grain** : daily 90 + weekly 40 + monthly 6 | 135 | 4.7 M | Pratique standard SAP IBP / o9 |
| **Daily 90 + weekly 60** (12 mois) | 150 | 5.2 M | Compromis pragmatique |

**Reco** : **Multi-grain (135 buckets/série)**. L'infrastructure existe (`nodes.time_grain` accepte `day/week/month`), il faut juste générer les buckets aux bonnes granularités.

### D3 — Périmètre des séries à matérialiser

23 139 (item, location) pairs ont au moins une activité dans le pilote.

| Option | Périmètre | Séries résultantes |
|---|---|---|
| **A. Tout** | toutes les paires avec activité | 23 139 séries × 135 buckets = ~3.1 M PI |
| **B. Avec stock OU PI** | items en stock ou demande prévue | ~12 K séries × 135 = ~1.6 M PI |
| **C. Sample pour validation** | 1000 séries random | 135 000 PI |

**Reco** : **C pour validation ce matin**, **A pour production**.

---

## 2. Tâches concrètes (séquentielles)

### Phase 1 — Validation rapide (1h)

- [ ] Trancher D1, D2, D3 (15 min discussion)
- [ ] Refondre `scripts/bootstrap_pi.py` selon décisions :
  - Multi-grain support (param `--grain-policy daily-weekly-monthly`)
  - Validation : tester sur sample 1000 séries via **container docker exec** (pas tunnel)
- [ ] Mesurer perf : créer 1000 séries × 135 buckets + edges + wire supply/demand
- [ ] Sanity check : 1 PI bucket arbitraire doit avoir des inflows/outflows cohérents avec les supply/demand de cet (item, location, date)

### Phase 2 — Première propagation (30 min)

- [ ] Trigger `POST /v1/calc/run?full_recompute=true` sur la DB pilote
- [ ] Mesurer : combien de PI calculés, en combien de temps, combien de shortages détectés
- [ ] Vérifier la cohérence sur 5 articles arbitraires :
  - opening_stock(t) == closing_stock(t-1) ?
  - closing_stock(t) == opening + inflows - outflows ?
  - has_shortage flag bien set quand closing < 0 ?

### Phase 3 — Scope complet (1h30)

- [ ] Si phase 2 OK, étendre bootstrap aux 23 139 paires complètes
- [ ] Re-propagation full
- [ ] Mesurer temps complet et taille DB finale
- [ ] Documenter les shortages détectés (synthèse top 50 par sévérité)

### Phase 4 — Cleanup + commit (1h)

- [ ] Nettoyer les scripts temp (`_check_fk.py`, `_status.py`, etc. déjà supprimés)
- [ ] Documenter `bulk_ingest.py` dans `docs/contracts/BULK-INGEST-GUIDE.md`
- [ ] Documenter `bootstrap_pi.py` (limitations + usage)
- [ ] Commit propre dans une branche `feat/bulk-ingest-and-pilote-pipeline`
- [ ] Push sur GitHub + ouvrir une PR
- [ ] Mettre à jour `TSV-FILES-SPEC.md` avec mention du chemin bulk

### Phase 5 — Limitations V1.1 (à arbitrer)

3 tasks pending de la session précédente, à statuer pour V1.1 :

- [ ] **CUST-V1.1** — table customers + customer_external_id
- [ ] **PRORATA-V1.1** — ventilation forecast monthly → weekly/daily auto
- [ ] **LANES-LATER** — distribution_links + transportation_lanes (multi-mode transit)

Note : si on lance le pilote propre demain, ces 3 sujets reviennent vite. À prioriser CUST > PRORATA > LANES en termes d'impact V1.

---

## 3. Risques / points d'attention

| Risque | Mitigation |
|---|---|
| **Bootstrap PI 23K séries × 135 buckets = 3M PI** trop gros pour PG en un coup | Faire par chunks de 500-1000 séries, commit intermédiaire |
| **Edges wiring supply→PI** très lent sur grandes tables (manque index) | Créer index temporaire `(item_id, location_id, time_ref)` sur nodes avant wire-up |
| **Propagation full_recompute** sur 3M PI peut être longue | Mesurer phase 2, splitter si > 5 min |
| **Dataset utilisateur incohérent** (210+ items orphelins) | Le bulk les a déjà skippés — pas un blocker, mais documenter pour reprojection |
| **Anomalies métier** (POs dans le passé) | Pré-filtrer ou nettoyer la source avant ré-import production |

---

## 4. État des assets

### Sur disque local `C:\dev\Ootils\` — non commité

```
scripts/bulk_ingest.py          ✅ fonctionnel, 11 entités
scripts/bootstrap_pi.py         ⚠️ à finaliser (multi-grain + container exec)
data/inbox/*.tsv (12 fichiers)  ✅ dataset pilote
data/{processed,rejected}/      ✅ workflow archivage actif
docs/contracts/* (12 docs)      ✅ contrats canoniques V1
docs/PLAN-2026-05-28-*.md       ✅ ce fichier
docs/STRATEGY-*.md              ✅ position paper (existant)
docs/WIP-*.md                   ✅ WIPs en décantation
docs/AGENT-FLEET-CATALOG.md     ✅ 86 agents catalogués
.gitignore                      ✅ data/inbox|processed|rejected ignorés
CLAUDE.md                       ✅ section North Star ajoutée
```

### Sur VM `192.168.1.176`

```
/home/debian/ootils-core/         repo synced HEAD master + migrations 001-038
/tmp/inbox/*.tsv                  ⚠️ copie pilote (à nettoyer)
container ootils-core-api-1       ✅ contient les fichiers TSV + scripts dans /tmp/ootils/
DB ootils_pilote_test             ✅ 329K rows chargées
```

### Sur PostgreSQL VM

```
ootils_pilote_test      329K rows (chargée hier soir)
ootils_dev              semi-prod, ne pas toucher sans confirmation
ootils_seed_test        synthétique benchmark
ootils_test_*           disposable
```

---

## 5. Définition du "fait" pour fin de journée

À 18h demain, on doit avoir :
1. ✅ PI graph bootstrap fonctionnel (23K séries × 135 buckets = ~3M PI)
2. ✅ Propagation full_recompute exécutée → projections calculées
3. ✅ Shortages détectés et listés (top 50)
4. ✅ Tout commité sur GitHub via PR
5. ✅ `bulk_ingest.py` + `bootstrap_pi.py` documentés
6. ✅ 3 décisions V1.1 (CUST, PRORATA, LANES) priorisées avec milestones

---

## 6. Si on a du temps en bonus

- Backend forecasts/Pyramide : connecter l'historique sur les forecasts pilotes
- Premier agent Watcher (Shortage Watcher W01) qui scrolle les shortages et propose un classement
- Tester scenario fork : créer un what-if depuis le pilote, modifier 1 PO, voir les shortages bouger

---

## 7. Ressources

- **Code source** : `C:\dev\Ootils\`
- **DB pilote** : `postgresql://ootils:ootils@127.0.0.1:15432/ootils_pilote_test` (via tunnel SSH)
- **Tunnel SSH** : `ssh -L 15432:localhost:5432 -i ~/.ssh/id_ed25519_ootils debian@192.168.1.176 -N`
- **Docker exec container** : `docker exec ootils-core-api-1` (équivalent local sur la VM)
- **Mémoire persistante** : `~/.claude/projects/C--dev-Paperasse/memory/MEMORY.md`
- **Session récap d'hier** : `memory/session_2026-05-27_pilote_import.md`
