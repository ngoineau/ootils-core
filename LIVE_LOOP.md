# LIVE_LOOP.md — Brief pour l'agent VS Code

> **Contexte :** Ce fichier est écrit par Claw (l'agent IA côté repo/architecture).
> Tu es l'agent VS Code côté machine live. On travaille en boucle courte :
> tu testes, tu rapportes, je fixe, tu re-testes.

---

## Notre protocole de collaboration

### Comment me parler (Claw)

Ouvre un GitHub Issue sur `ngoineau/ootils-core` avec :
- **Titre** : `[LIVE] <ce qui s'est passé>`
- **Label** : `live-feedback`
- **Corps** : stack trace exacte, commande utilisée, comportement attendu vs observé

Je lis les Issues, je fix dans la branche `live/v1-bootstrap`, tu pull et tu re-testes.

**Cycle cible : < 10 minutes par itération.**

---

## Ce que tu dois faire — dans l'ordre

### Étape 0 — Prérequis

```bash
# Vérifie que tu as Docker
docker --version
docker compose version

# Clone le repo si pas encore fait
git clone https://github.com/ngoineau/ootils-core
cd ootils-core
git checkout live/v1-bootstrap
```

---

### Étape 1 — Démarrer l'infrastructure

```bash
docker compose up --build
```

**Attendre que tu voies :**
```
api_1       | INFO:     Application startup complete.
api_1       | INFO:     Uvicorn running on http://0.0.0.0:8000
```

**Si ça plante ici** → ouvre un Issue avec le log complet.

**Vérification rapide :**
```bash
curl http://localhost:8000/health
# Attendu : {"status":"ok","version":"1.0.0"}
```

---

### Étape 2 — Appliquer les migrations

Les migrations s'appliquent automatiquement au démarrage de l'API (via `OotilsDB.__init__`).
Vérifie qu'elles sont passées :

```bash
# Dans un autre terminal
docker compose exec postgres psql -U ootils -d ootils_dev -c "\dt"
```

**Attendu :** une liste de tables incluant `nodes`, `edges`, `scenarios`, `calc_runs`, `shortages`, `explanations`, `zone_transition_runs`, `scenario_overrides`, `scenario_diffs`.

**Si des tables manquent** → note lesquelles dans un Issue.

---

### Étape 3 — Seed les données de démo

```bash
# Depuis la racine du repo (pas dans Docker — sur ta machine)
pip install psycopg[binary]  # si pas déjà installé
DATABASE_URL=postgresql://ootils:ootils@localhost:5432/ootils_dev \
  python scripts/seed_demo_data.py
```

**Attendu :**
```
🌱 Seeding Ootils demo data...
  ✓ Items: PUMP-01 (...), VALVE-02 (...)
  ✓ Locations: DC-ATL (...), DC-LAX (...)
  ✓ Projection series created
  → Creating PI nodes (90 days × 2 series)...
  ✓ Supply and demand nodes created
  ✓ Trigger event inserted
✅ Seed complete.
   → PUMP-01 @ DC-ATL: PO delayed → shortage expected ~day 18
   → VALVE-02 @ DC-LAX: demand spike → shortage expected ~day 10
```

**Si le seeder crashe** → colle le stack trace dans un Issue.

---

### Étape 4 — Tester les endpoints API

**Token d'auth :** `dev-token` (header : `Authorization: Bearer dev-token`)

```bash
# Issues actifs
curl -H "Authorization: Bearer dev-token" \
  "http://localhost:8000/v1/issues?severity=all&horizon_days=90"

# Attendu : liste de shortages avec severity_score, shortage_date, etc.
```

```bash
# Prendre le premier pi_node_id de la réponse issues et l'expliquer
curl -H "Authorization: Bearer dev-token" \
  "http://localhost:8000/v1/explain?node_id=<PI_NODE_ID>"

# Attendu : explanation avec causal_path (au moins 1 step)
```

```bash
# Simuler une correction (prendre un node_id d'un PO supply)
curl -X POST -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "scenario_name": "test-expedite",
    "base_scenario_id": "00000000-0000-0000-0000-000000000001",
    "overrides": [{"node_id": "<PO_NODE_ID>", "field_name": "time_ref", "new_value": "2026-04-10"}]
  }' \
  "http://localhost:8000/v1/simulate"
```

**Pour chaque endpoint :** note dans l'Issue :
- ✅ Si ça marche : la réponse JSON
- ❌ Si ça plante : le status HTTP + body d'erreur

---

### Étape 5 — Lancer l'agent demo

```bash
DATABASE_URL=postgresql://ootils:ootils@localhost:5432/ootils_dev \
OOTILS_API_TOKEN=dev-token \
  python scripts/run_agent_demo.py
```

**Attendu :** un rapport agent formaté qui montre les shortages détectés, les recommandations, les simulations.

**Si l'agent ne trouve pas de shortages** → les données ne sont pas seedées ou `/v1/issues` retourne vide.

---

### Étape 6 — Rapport final

Ouvre un Issue `[LIVE] Résultats run complet` avec :

```markdown
## Run results

### Infrastructure
- [ ] docker compose up OK
- [ ] migrations appliquées : <liste des tables>
- [ ] seed OK

### API endpoints
- [ ] GET /health : OK
- [ ] GET /v1/issues : <nb de shortages retournés>
- [ ] GET /v1/explain : OK / FAIL + détails
- [ ] POST /v1/simulate : OK / FAIL + détails
- [ ] GET /v1/graph : OK / FAIL + détails

### Agent demo
- [ ] run_agent_demo.py : OK / FAIL
- Shortages détectés : X
- Recommandations : X
- Simulations : X

### Erreurs rencontrées
<stack traces si applicable>

### Questions / points bloquants
<ce que tu n'arrives pas à résoudre seul>
```

---

## Erreurs connues probables et solutions

### psycopg3 UUID type error
```
ProgrammingError: can't adapt type 'str' for UUID
```
→ Signaler dans un Issue. Je fixe le cast.

### Migration 001 "syntax error"
```
syntax error at or near "PRAGMA"
```
→ Normalement corrigé. Si ça arrive encore, commenter la ligne dans 001.

### Port 5432 already in use
```bash
docker compose stop
lsof -i :5432  # voir quel process
```

### API démarre mais /v1/issues retourne []
→ Le seeder a peut-être planté silencieusement. Re-run `seed_demo_data.py`.

---

## Ce que je (Claw) vais faire de ton feedback

1. Je lis l'Issue dans les minutes qui suivent
2. Je fix dans `live/v1-bootstrap`
3. Je commente l'Issue avec le commit hash du fix
4. Tu pull et tu re-testes

**Un seul fichier de feedback par cycle** — pas besoin d'ouvrir 10 Issues. Un seul avec tout dedans.

---

## Architecture reminder (pour que tu comprennes ce que tu testes)

```
PostgreSQL
    ↑ migrations (001-006)
    ↑ seed_demo_data.py
    ↑
FastAPI (port 8000)
    /health
    /v1/events     → insert PlanningEvent
    /v1/issues     → ShortageDetector.get_active_shortages()
    /v1/explain    → ExplanationBuilder.get_explanation()
    /v1/simulate   → ScenarioManager.create_scenario() + apply_override()
    /v1/graph      → GraphStore.get_all_nodes() + get_all_edges()
    /v1/projection → GraphStore.get_projection_series()
    ↑
OotilsAgent (scripts/run_agent_demo.py)
    → appelle l'API via httpx
    → décisions déterministes sur causal_path
    → produit AgentReport
```

**OpenAPI / Swagger :** http://localhost:8000/docs (accessible sans auth pour explorer)

---

*Dernière mise à jour : 2026-04-04 — Claw*
