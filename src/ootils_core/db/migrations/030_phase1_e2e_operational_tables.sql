-- ============================================================
-- Ootils Core — Migration 030: Phase 1 E2E operational tables
-- QA-001: DB-backed Forecast → MPS → MRP/CRP → ATP proof
-- ============================================================
-- Adds the relational operational tables consumed by the Phase 1
-- ATP/CRP/MPS engines. Existing graph nodes remain the source of truth for
-- legacy flows; these tables support deterministic Phase 1 integration tests
-- and API execution.
-- ============================================================

-- Forecast values are soft-deleted/read by API and MPS code via active flag.
ALTER TABLE forecast_values
    ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'forecast_values') THEN
        DROP TRIGGER IF EXISTS trg_forecast_values_updated_at ON forecast_values;
        CREATE TRIGGER trg_forecast_values_updated_at
            BEFORE UPDATE ON forecast_values
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

-- Planned supplies produced by MPS/MRP and consumed by CRP/ATP.
CREATE TABLE IF NOT EXISTS planned_supply (
    planned_supply_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    item_id           UUID NOT NULL REFERENCES items(item_id),
    location_id       UUID NOT NULL REFERENCES locations(location_id),
    scenario_id       UUID NOT NULL REFERENCES scenarios(scenario_id) DEFAULT '00000000-0000-0000-0000-000000000001',
    source_type       TEXT NOT NULL DEFAULT 'MPS',
    source_id         UUID,
    quantity          NUMERIC(18,6) NOT NULL CHECK (quantity >= 0),
    due_date          DATE NOT NULL,
    status            TEXT NOT NULL DEFAULT 'PLANNED' CHECK (status IN ('PLANNED', 'APPROVED', 'RELEASED', 'CANCELLED')),
    priority          INTEGER NOT NULL DEFAULT 100,
    active            BOOLEAN NOT NULL DEFAULT true,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_planned_supply_item_location ON planned_supply(item_id, location_id);
CREATE INDEX IF NOT EXISTS idx_planned_supply_due_date ON planned_supply(due_date);
CREATE INDEX IF NOT EXISTS idx_planned_supply_status ON planned_supply(status);
CREATE INDEX IF NOT EXISTS idx_planned_supply_scenario ON planned_supply(scenario_id);

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'planned_supply') THEN
        DROP TRIGGER IF EXISTS trg_planned_supply_updated_at ON planned_supply;
        CREATE TRIGGER trg_planned_supply_updated_at
            BEFORE UPDATE ON planned_supply
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

-- ATP material supply snapshot.
CREATE TABLE IF NOT EXISTS on_hand_supply (
    on_hand_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    item_id      UUID NOT NULL REFERENCES items(item_id),
    location_id  UUID NOT NULL REFERENCES locations(location_id),
    quantity     NUMERIC(18,6) NOT NULL CHECK (quantity >= 0),
    as_of_date   DATE NOT NULL,
    active       BOOLEAN NOT NULL DEFAULT true,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_on_hand_supply_item_location ON on_hand_supply(item_id, location_id);
CREATE INDEX IF NOT EXISTS idx_on_hand_supply_as_of ON on_hand_supply(as_of_date);

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'on_hand_supply') THEN
        DROP TRIGGER IF EXISTS trg_on_hand_supply_updated_at ON on_hand_supply;
        CREATE TRIGGER trg_on_hand_supply_updated_at
            BEFORE UPDATE ON on_hand_supply
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

-- ATP committed demand table. MPS still also reads graph CustomerOrderDemand
-- nodes; this table is for ATP/CTP commitment netting.
CREATE TABLE IF NOT EXISTS customer_order_demand (
    customer_order_demand_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    item_id        UUID NOT NULL REFERENCES items(item_id),
    location_id    UUID NOT NULL REFERENCES locations(location_id),
    scenario_id    UUID NOT NULL REFERENCES scenarios(scenario_id) DEFAULT '00000000-0000-0000-0000-000000000001',
    quantity       NUMERIC(18,6) NOT NULL CHECK (quantity >= 0),
    requested_date DATE NOT NULL,
    status         TEXT NOT NULL DEFAULT 'CONFIRMED' CHECK (status IN ('DRAFT', 'CONFIRMED', 'RELEASED', 'CANCELLED')),
    priority       INTEGER NOT NULL DEFAULT 100,
    is_committed   BOOLEAN NOT NULL DEFAULT true,
    active         BOOLEAN NOT NULL DEFAULT true,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_customer_order_demand_item_location ON customer_order_demand(item_id, location_id);
CREATE INDEX IF NOT EXISTS idx_customer_order_demand_requested_date ON customer_order_demand(requested_date);
CREATE INDEX IF NOT EXISTS idx_customer_order_demand_status ON customer_order_demand(status);
CREATE INDEX IF NOT EXISTS idx_customer_order_demand_scenario ON customer_order_demand(scenario_id);

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'customer_order_demand') THEN
        DROP TRIGGER IF EXISTS trg_customer_order_demand_updated_at ON customer_order_demand;
        CREATE TRIGGER trg_customer_order_demand_updated_at
            BEFORE UPDATE ON customer_order_demand
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;
