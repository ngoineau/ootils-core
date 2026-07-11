//! io.rs — Postgres read path for the dirty subgraph (ADR-016 §week 4-5).
//!
//! Loads everything `_propagate()` needs into in-memory Rust structs:
//! - The dirty PI buckets themselves
//! - Incoming `replenishes` supply nodes
//! - Incoming `consumes` demand nodes
//! - Seed openings per affected series
//!
//! **Single-query strategy** (week 5 perf optim): all four data shapes
//! arrive in ONE result set, tagged by a `kind` text marker. Saves 3
//! roundtrips per call (~25-40ms on LAN), critical for incremental
//! events. The single query is uglier than four targeted ones but the
//! latency win is decisive.

use chrono::NaiveDate;
use postgres::types::Type;
use postgres::{Client, NoTls};
use rust_decimal::Decimal;
use std::collections::HashMap;
use uuid::Uuid;

// -------------------------------------------------------------------- //
//  In-memory representation of the dirty subgraph
// -------------------------------------------------------------------- //

#[derive(Debug, Clone)]
pub struct DirtyPi {
    pub node_id: Uuid,
    pub projection_series_id: Uuid,
    pub bucket_sequence: i32,
    pub time_span_start: NaiveDate,
    pub time_span_end: NaiveDate,
}

#[derive(Debug, Clone)]
pub struct Supply {
    pub quantity: Decimal,
    pub time_ref: NaiveDate,
}

#[derive(Debug, Clone)]
pub struct Demand {
    pub quantity: Decimal,
    pub time_span_start: Option<NaiveDate>,
    pub time_span_end: Option<NaiveDate>,
    pub time_ref: Option<NaiveDate>,
}

#[derive(Debug, Default)]
pub struct Subgraph {
    pub dirty_pis: Vec<DirtyPi>,
    pub supplies_by_pi: HashMap<Uuid, Vec<Supply>>,
    pub demands_by_pi: HashMap<Uuid, Vec<Demand>>,
    pub seed_openings: HashMap<Uuid, (i32, Decimal)>,
}

impl Subgraph {
    pub fn n_dirty_pis(&self) -> usize {
        self.dirty_pis.len()
    }
    pub fn n_supplies(&self) -> usize {
        self.supplies_by_pi.values().map(|v| v.len()).sum()
    }
    pub fn n_demands(&self) -> usize {
        self.demands_by_pi.values().map(|v| v.len()).sum()
    }
    pub fn n_series_seeds(&self) -> usize {
        self.seed_openings.len()
    }
}

// -------------------------------------------------------------------- //
//  Loader
// -------------------------------------------------------------------- //

pub struct Loader {
    client: Client,
}

impl Loader {
    pub fn connect(dsn: &str) -> Result<Self, postgres::Error> {
        let client = Client::connect(dsn, NoTls)?;
        Ok(Self { client })
    }

    pub fn load_subgraph(
        &mut self,
        calc_run_id: Uuid,
        scenario_id: Uuid,
    ) -> Result<Subgraph, postgres::Error> {
        load_subgraph(&mut self.client, calc_run_id, scenario_id)
    }
}

/// The combined UNION ALL load query — one roundtrip for everything.
///
/// Each row has a `kind` column identifying which sub-result it
/// belongs to. NULLs fill the cells that don't apply to that kind.
/// Column ordering (kept stable for Rust deserializer):
///   kind, uuid_a, uuid_b, qty, int_a, date_a, date_b, date_c
///
/// Per-kind semantics:
///   - kind='pi'     : uuid_a=node_id, uuid_b=projection_series_id,
///     int_a=bucket_sequence, date_a=time_span_start,
///     date_b=time_span_end
///   - kind='supply' : uuid_a=pi_node_id, qty=quantity, date_a=time_ref
///   - kind='demand' : uuid_a=pi_node_id, qty=quantity,
///     date_a=time_span_start, date_b=time_span_end,
///     date_c=time_ref
///   - kind='seed'   : uuid_a=projection_series_id, int_a=seed_seq,
///     qty=seed_opening
const COMBINED_LOAD_SQL: &str = "\
WITH dirty AS ( \
    SELECT pi.node_id, pi.projection_series_id, pi.bucket_sequence, \
           pi.time_span_start, pi.time_span_end, pi.scenario_id \
    FROM nodes pi \
    JOIN dirty_nodes dn \
      ON dn.node_id = pi.node_id AND dn.scenario_id = pi.scenario_id \
    WHERE dn.calc_run_id = $1 \
      AND pi.node_type = 'ProjectedInventory' \
      AND pi.scenario_id = $2 \
      AND pi.active = TRUE \
), \
series_first_dirty AS ( \
    SELECT projection_series_id, MIN(bucket_sequence) AS seed_seq \
    FROM dirty GROUP BY projection_series_id \
), \
seed_calc AS ( \
    SELECT \
        sfd.projection_series_id, \
        sfd.seed_seq, \
        CASE WHEN sfd.seed_seq = 0 THEN \
            COALESCE((\
                SELECT SUM(oh.quantity) \
                FROM nodes pi_seed \
                JOIN edges r ON r.to_node_id = pi_seed.node_id \
                JOIN nodes oh ON oh.node_id = r.from_node_id \
                WHERE pi_seed.projection_series_id = sfd.projection_series_id \
                  AND pi_seed.bucket_sequence = 0 \
                  AND pi_seed.scenario_id = $2 \
                  AND pi_seed.active = TRUE \
                  AND r.edge_type = 'replenishes' \
                  AND r.scenario_id = $2 \
                  AND r.active = TRUE \
                  AND oh.node_type = 'OnHandSupply' \
                  AND oh.active = TRUE \
            ), 0)::numeric \
        ELSE \
            COALESCE((\
                SELECT prev.closing_stock \
                FROM nodes prev \
                WHERE prev.projection_series_id = sfd.projection_series_id \
                  AND prev.bucket_sequence = sfd.seed_seq - 1 \
                  AND prev.scenario_id = $2 \
                  AND prev.active = TRUE \
            ), 0)::numeric \
        END AS seed_opening \
    FROM series_first_dirty sfd \
) \
SELECT 'pi'::text AS kind, \
       node_id AS uuid_a, projection_series_id AS uuid_b, \
       NULL::numeric AS qty, \
       bucket_sequence AS int_a, \
       time_span_start AS date_a, time_span_end AS date_b, \
       NULL::date AS date_c \
FROM dirty \
UNION ALL \
SELECT 'supply'::text, \
       e.to_node_id, NULL, s.quantity, \
       NULL, s.time_ref, NULL, NULL \
FROM dirty d \
JOIN edges e ON e.to_node_id = d.node_id \
            AND e.edge_type = 'replenishes' \
            AND e.scenario_id = d.scenario_id \
            AND e.active = TRUE \
JOIN nodes s ON s.node_id = e.from_node_id \
            AND s.active = TRUE \
            AND s.node_type IN ('PurchaseOrderSupply','WorkOrderSupply','TransferSupply','PlannedSupply') \
            AND s.time_ref IS NOT NULL \
UNION ALL \
SELECT 'demand'::text, \
       e.to_node_id, NULL, dnode.quantity, \
       NULL, dnode.time_span_start, dnode.time_span_end, dnode.time_ref \
FROM dirty d \
JOIN edges e ON e.to_node_id = d.node_id \
            AND e.edge_type = 'consumes' \
            AND e.scenario_id = d.scenario_id \
            AND e.active = TRUE \
JOIN nodes dnode ON dnode.node_id = e.from_node_id \
                AND dnode.active = TRUE \
                AND dnode.node_type IN ('ForecastDemand','CustomerOrderDemand','DependentDemand','TransferDemand') \
UNION ALL \
SELECT 'seed'::text, \
       projection_series_id, NULL, seed_opening, \
       seed_seq, NULL, NULL, NULL \
FROM seed_calc";

/// Load via the single combined query.
pub fn load_subgraph(
    client: &mut Client,
    calc_run_id: Uuid,
    scenario_id: Uuid,
) -> Result<Subgraph, postgres::Error> {
    let mut sg = Subgraph::default();

    let rows = client.query(COMBINED_LOAD_SQL, &[&calc_run_id, &scenario_id])?;
    for r in rows {
        let kind: &str = r.get(0);
        match kind {
            "pi" => {
                sg.dirty_pis.push(DirtyPi {
                    node_id: r.get(1),
                    projection_series_id: r.get(2),
                    bucket_sequence: r.get(4),
                    time_span_start: r.get(5),
                    time_span_end: r.get(6),
                });
            }
            "supply" => {
                let pi_id: Uuid = r.get(1);
                sg.supplies_by_pi.entry(pi_id).or_default().push(Supply {
                    quantity: r.get(3),
                    time_ref: r.get(5),
                });
            }
            "demand" => {
                let pi_id: Uuid = r.get(1);
                sg.demands_by_pi.entry(pi_id).or_default().push(Demand {
                    quantity: r.get(3),
                    time_span_start: r.get(5),
                    time_span_end: r.get(6),
                    time_ref: r.get(7),
                });
            }
            "seed" => {
                let series_id: Uuid = r.get(1);
                let seed_seq: i32 = r.get(4);
                let opening: Decimal = r.get(3);
                sg.seed_openings.insert(series_id, (seed_seq, opening));
            }
            other => {
                // Schema drift: COMBINED_LOAD_SQL emits one of four
                // literal markers; anything else means the SQL was
                // edited without updating this match. Fail loudly.
                panic!("io::load_subgraph: unknown kind marker {other:?}");
            }
        }
    }

    Ok(sg)
}

#[allow(dead_code)]
const _DECIMAL_IS_NUMERIC: Type = Type::NUMERIC;
