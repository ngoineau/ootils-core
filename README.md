# ootils-core

**The first supply chain decision engine designed for the age of AI agents.**

`ootils-core` gives AI agents — and the developers who build them — a principled, batteries-included library for making intelligent supply chain decisions. It implements the classical inventory-management algorithms (EOQ, safety stock, reorder point) in a clean, dependency-free Python package and wraps them in an **agent-first tool interface** that integrates directly with OpenAI function calling, Anthropic tool use, and any other LLM framework that supports structured tool schemas.

This library is the first executable slice of the broader Ootils vision: AI-native supply chain operations built on deterministic, explainable decision primitives that agents can actually use.

---

## Features

| Capability | Description |
|---|---|
| 📦 **Inventory policies** | Economic Order Quantity, safety stock (combined variance model), reorder point |
| 🏭 **Supplier selection** | Composite scoring (cost × lead time × reliability); full ranking |
| 🚦 **Risk assessment** | Urgency classification (`critical / high / medium / low`) with plain-English explanations |
| 🤖 **Agent tool interface** | One class, five callable tools, OpenAI-compatible JSON schemas ready to paste into any LLM runtime |
| 📋 **Portfolio evaluation** | Evaluate an entire product catalog at once, sorted by urgency |
| ✅ **Zero dependencies** | Pure Python ≥ 3.10 — no numpy, no pandas, no heavy ML frameworks |

---

## Quick start

```python
from ootils_core import SupplyChainDecisionEngine
from ootils_core.models import Product, Supplier, InventoryState

engine = SupplyChainDecisionEngine()

product = Product(
    sku="WIDGET-001",
    name="Widget A",
    unit_cost=10.0,          # $ per unit
    ordering_cost=50.0,      # $ per purchase order
    holding_cost_rate=0.25,  # 25 % of unit cost per year
    service_level=0.95,      # 95 % in-stock target
    lead_time_days=14,
    lead_time_std_days=2,
)

supplier = Supplier(
    name="FastCo",
    lead_time_days=14,
    reliability_score=0.97,
)

state = InventoryState(
    product=product,
    current_stock=50,
    daily_demand=5.0,
    demand_std_daily=1.5,
    open_order_quantity=0,
)

recommendation = engine.decide(state, suppliers=[supplier])

if recommendation:
    print(f"Order {recommendation.order_quantity} units from {recommendation.supplier.name}")
    print(f"Urgency: {recommendation.urgency}")
    print(recommendation.rationale)
else:
    print("Stock levels are adequate — no action needed.")
```

---

## AI agent usage

```python
from ootils_core.tools import SupplyChainTools

tools = SupplyChainTools()

# --- Use as a plain callable tool ---
result = tools.recommend_order({
    "sku": "WIDGET-001",
    "name": "Widget A",
    "unit_cost": 10.0,
    "current_stock": 50,
    "daily_demand": 5.0,
    "demand_std_daily": 1.5,
    "lead_time_days": 14,
    "suppliers": [
        {"name": "FastCo", "lead_time_days": 14, "reliability_score": 0.97},
        {"name": "CheapCo", "lead_time_days": 21, "unit_price_multiplier": 0.85},
    ],
})
print(result)
# {"status": "ok", "result": {"sku": ..., "supplier": "FastCo", "order_quantity": 141, ...}}

# --- Pass schemas directly to an LLM ---
schemas = tools.tool_schemas()  # OpenAI-compatible function definitions
# openai_client.chat.completions.create(model="gpt-4o", tools=schemas, ...)
```

### Available tools

| Tool method | What it does |
|---|---|
| `calculate_reorder_point` | Compute ROP and safety stock for a given demand and lead time |
| `calculate_eoq` | Compute the Economic Order Quantity |
| `recommend_order` | Full end-to-end decision: ROP → EOQ → supplier selection → rationale |
| `rank_suppliers` | Score and rank a list of suppliers for a product |
| `assess_risk` | Evaluate urgency without committing to an order recommendation |

All tool methods accept a plain `dict` and return a plain `dict` with `"status"` (`"ok"` / `"no_action"` / `"error"`), making them trivially serialisable to JSON.

---

## Installation

```bash
pip install ootils-core
```

Or for development:

```bash
git clone https://github.com/ngoineau/ootils-core.git
cd ootils-core
pip install -e ".[dev]"
pytest
```

---

## Architecture

```
src/ootils_core/
├── models/          # Pure data classes: Product, Supplier, InventoryState, OrderRecommendation
├── engine/
│   ├── policies.py          # EOQ, safety stock, ROP, urgency classification
│   ├── supplier_selection.py  # Composite scoring & ranking
│   └── decision_engine.py   # SupplyChainDecisionEngine (single product + portfolio)
└── tools/
    └── agent_tools.py       # SupplyChainTools — the agent-facing interface
```

### Decision logic

1. **Safety stock** is computed with the combined-variance formula:
   `SS = z × √(L·σ_d² + d²·σ_L²)`
   where `z` is the service-level z-score, `L` is lead time, `d` is daily demand, `σ_d` is demand std-dev, and `σ_L` is lead time std-dev.

2. **Reorder point** = average demand during lead time + safety stock.

3. **Economic Order Quantity** (Wilson/Harris formula):
   `EOQ = √(2DS / H)`
   where `D` is annual demand, `S` is ordering cost, and `H` is annual holding cost per unit.

4. **Supplier selection** scores each active supplier as:
   `score = reliability × (0.5 × cost_score + 0.5 × lead_time_score)`
   The highest-scoring supplier is selected and its min/max order constraints are applied to the EOQ.

5. **Urgency** is classified by days-of-supply relative to safety stock and reorder point thresholds.

---

## License

MIT
