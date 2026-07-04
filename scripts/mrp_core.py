"""
mrp_core.py — compatibility shim for the 21 scripts that do `import mrp_core as core`.

The MRP math has moved to ootils_core.engine.mrp.core (DB-free) and
ootils_core.engine.mrp.loader (DB layer). This file re-exports every public
and private symbol so existing consumers are unaffected.
"""
from ootils_core.engine.mrp.core import (
    BASELINE,
    DEFAULT_LT_DAYS,
    SUPPLY_TYPES,
    FIRM_RECEIPT_TYPES,
    DEMAND_TYPES,
    lot_size,
    cost_of,
    _spread_period,
    apply_lot_rule,
    PlanningData,
    ReceiptOrder,
    RescheduleSignal,
    consume_demand,
    run_timephased,
    first_shortage,
    reschedule_signals,
    excess_obsolete,
    peg_origins,
)
from ootils_core.engine.mrp.loader import guard_db, _m, load_planning_data
