-- ============================================================
-- Migration 042 — Item standard cost (valuation backbone)
-- ============================================================
-- Cost in the model lived ONLY on supplier_items.unit_cost (per supplier
-- link), so manufactured items and any item without a supplier row had no
-- cost — leaving ~11% of planned purchase VOLUME unvalued.
--
-- This adds an item-level standard cost: the single place to value ANY item.
-- Precedence for valuation is: negotiated supplier unit_cost (most accurate for
-- buy items) -> item standard_cost (fallback; covers made items via BOM roll-up
-- and uncosted buy items). standard_cost is populated by scripts/
-- compute_cost_rollup.py (bottom-up BOM roll-up) or imported from the ERP.
-- ============================================================

BEGIN;

ALTER TABLE items ADD COLUMN IF NOT EXISTS standard_cost  NUMERIC;
ALTER TABLE items ADD COLUMN IF NOT EXISTS cost_currency  TEXT;

COMMENT ON COLUMN items.standard_cost IS
  'Item-level standard unit cost (valuation fallback). For made items, the '
  'bottom-up BOM roll-up of component costs; for bought items, the supplier '
  'unit_cost. Populated by compute_cost_rollup.py or ERP import.';
COMMENT ON COLUMN items.cost_currency IS
  'Currency of standard_cost (no FX conversion applied across currencies).';

COMMIT;
