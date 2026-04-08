# SPEC-DQ-AGENT — Agent Data Quality V1

**Statut :** DRAFT — 2026-04-08
**Références :** ADR-009, SPEC-GHOSTS-TAGS, ShortageDetector, GraphStore

---

## 1. Vision

Le DQ Engine (L1+L2) détecte les problèmes connus et déterministes. Il ne peut pas :
- Détecter les anomalies statistiques (valeurs hors distribution historique)
- Prioriser intelligemment selon l'impact supply chain réel
- Relier une donnée corrompue à ses conséquences sur le plan

L'Agent DQ comble ces trois lacunes. C'est la première pièce de la flotte d'agents Ootils.

**Différenciation produit :** n'importe quel ETL fait L1/L2. Peu d'outils font la détection statistique correctement. **Personne ne relie automatiquement une anomalie de donnée à son impact sur le plan d'approvisionnement.** C'est le graph Ootils qui rend ça possible.

---

## 2. Périmètre V1

### Catégorie 1 — Anomalies de valeur (statistiques)

Comparaison du batch entrant vs historique des N derniers batches de même `entity_type` :

| Rule code | Détection | Sévérité |
|-----------|-----------|---------|
| `STAT_LEAD_TIME_SPIKE` | lead_time_days dévie de >3σ vs historique item | ERROR |
| `STAT_FORECAST_SPIKE` | forecast_qty × 10 vs moyenne historique | WARNING |
| `STAT_PRICE_OUTLIER` | unit_price hors [Q1 - 1.5×IQR, Q3 + 1.5×IQR] | WARNING |
| `STAT_SAFETY_STOCK_ZERO` | safety_stock_qty = 0 sur item critique (a des shortages actifs) | WARNING |
| `STAT_NEGATIVE_ONHAND` | on_hand_qty < 0 après calcul | ERROR |

### Catégorie 3 — Anomalies temporelles

| Rule code | Détection | Sévérité |
|-----------|-----------|---------|
| `TEMP_DUPLICATE_BATCH` | batch avec >95% valeurs identiques au batch précédent même entity_type | WARNING |
| `TEMP_PO_DATE_PAST` | PO expected_date dans le passé et status != 'received' | WARNING |
| `TEMP_FORECAST_HORIZON_SHORT` | horizon forecast < max(lead_time_days) des items importés | WARNING |
| `TEMP_MASS_CHANGE` | >30% des valeurs d'un champ changent entre deux batches successifs | ERROR |

### Catégorie 4 — Impact SC (le différenciateur)

Pour chaque issue DQ (L1/L2/stat/temporal) sur un item :

1. **Graph traversal** : identifier tous les nœuds affectés via le graph Ootils (ShortageDetector + GraphStore)
2. **Impact scoring** : compter les shortages actifs sur les items impactés
3. **Propagation BOM** : si l'item est un composant, remonter aux produits finis impactés
4. **Output** : enrichir chaque issue avec `impact_score`, `affected_items`, `active_shortages_count`

Priorisation finale :
```
priority = severity_weight × (1 + log(1 + active_shortages_count))
```

---

## 3. Architecture

```
POST /v1/ingest/*
        ↓
DQ Engine (L1+L2)              ← règles déterministes, synchrone
        ↓
DQ Agent (analyse async)       ← statistiques + impact SC
        ↓
data_quality_issues enrichies
        ↓
GET /v1/dq/agent/report/{batch_id}   ← rapport narratif + priorisé
```

### Modules

```
src/ootils_core/engine/dq/
  engine.py          ← DQ Engine L1+L2 (existant après feat/dq-engine-v1)
  agent/
    agent.py         ← dispatcher principal run_dq_agent(db, batch_id)
    stat_rules.py    ← catégorie 1 (statistiques)
    temporal_rules.py ← catégorie 3 (temporel)
    impact_scorer.py  ← catégorie 4 (graph traversal + scoring)
    report.py         ← génération rapport narratif
```

### Pas d'appel LLM externe en V1

L'agent V1 est **entièrement déterministe** — Python pur, pas d'API OpenAI/Claude.
Les règles stat et l'impact scorer sont du code, pas du LLM.
Le "rapport narratif" est un template structuré, pas du texte généré.

Rationale : fiabilité, coût, latence. Le LLM arrive en V2 pour la couche de recommandation (suggérer des corrections, expliquer les anomalies en langage naturel).

---

## 4. API

### POST /v1/dq/agent/run/{batch_id}
Déclenche l'analyse agent sur un batch (async).
```json
Response: { "status": "queued", "batch_id": "...", "agent_run_id": "..." }
```

### GET /v1/dq/agent/report/{batch_id}
Rapport complet du batch.
```json
{
  "batch_id": "...",
  "entity_type": "purchase_orders",
  "analyzed_at": "...",
  "summary": {
    "total_rows": 120,
    "issues_count": 7,
    "critical_count": 2,
    "affected_items_count": 5,
    "active_shortages_impacted": 3
  },
  "issues": [
    {
      "rule_code": "STAT_LEAD_TIME_SPIKE",
      "severity": "error",
      "field": "lead_time_days",
      "raw_value": "140",
      "expected_range": "12–18",
      "impact_score": 4.2,
      "affected_shortages": ["shortage_id_1", "shortage_id_2"],
      "message": "lead_time_days=140 dévie de 8.3σ vs historique (μ=14, σ=1.5)"
    }
  ],
  "priority_actions": [
    "Corriger lead_time_days pour VALVE-02 : impact direct sur 2 shortages actifs DC-LAX"
  ]
}
```

### GET /v1/dq/agent/runs
Historique des runs agent (batch_id, status, score, issues_count).

---

## 5. Table DB

```sql
CREATE TABLE dq_agent_runs (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id        UUID NOT NULL REFERENCES ingest_batches(batch_id),
    status          TEXT NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed')),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    summary         JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Issues enrichies : ajouter colonnes `impact_score NUMERIC`, `agent_run_id UUID` sur `data_quality_issues`.

---

## 6. Trigger

L'agent est déclenché automatiquement après chaque run DQ Engine (L1+L2).
Séquence complète post-ingest :
```
ingest → DQ Engine → DQ Agent → issues enrichies disponibles en ~2s
```

---

## 7. UI

Nouvelle section dans la page `/events` ou page dédiée `/dq` :
- Feed des rapports agent par batch
- Badge "⚠️ 2 critiques" sur les batches avec issues hautes priorité
- Panel détail : issues triées par priority_score, avec affected_shortages linkés

---

## 8. V2 (hors scope V1)

- LLM pour suggestions de correction en langage naturel
- Règles L3 (métier SC) + L4 (croisé)
- Auto-approbation des batches clean (0 erreur, 0 anomalie stat)
- Apprentissage des patterns fournisseur (ML)
