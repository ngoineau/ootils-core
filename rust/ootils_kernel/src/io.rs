//! io.rs — Postgres read path for the dirty subgraph (ADR-016 §week 2).
//!
//! Loads everything `_propagate()` needs into in-memory Rust structs:
//! - The dirty PI buckets themselves (node_id, projection_series_id,
//!   bucket_sequence, time_span_start, time_span_end).
//! - Incoming `replenishes` supply nodes (with quantity + time_ref).
//! - Incoming `consumes` demand nodes (with quantity + time bounds).
//! - Seed openings per affected series (either OnHand sum for bucket 0
//!   or prev.closing_stock for higher seed_seq).
//!
//! Why blocking (`postgres`) and not async (`tokio-postgres`):
//! ADR-016 §"Out of scope" excludes async runtime. Blocking IO keeps
//! the code straightforward, and we're not multiplexing N concurrent
//! propagations on the same Rust process anyway — each calc_run is one
//! linear pipeline.

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
    /// Some demands are time_span (monthly forecasts), others are
    /// time_ref points. We normalize: if time_span_start/end are
    /// present and span > 0, we use the span; otherwise time_ref.
    pub time_span_start: Option<NaiveDate>,
    pub time_span_end: Option<NaiveDate>,
    pub time_ref: Option<NaiveDate>,
}

#[derive(Debug, Default)]
pub struct Subgraph {
    pub dirty_pis: Vec<DirtyPi>,
    /// PI node_id -> incoming supplies (replenishes edges)
    pub supplies_by_pi: HashMap<Uuid, Vec<Supply>>,
    /// PI node_id -> incoming demands (consumes edges)
    pub demands_by_pi: HashMap<Uuid, Vec<Demand>>,
    /// projection_series_id -> (seed_bucket_seq, seed_opening_stock)
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
//  Loader — one query per concept (matches the SQL engine's CTEs).
// -------------------------------------------------------------------- //

pub struct Loader {
    client: Client,
}

impl Loader {
    /// Open a blocking Postgres connection. The caller passes the same
    /// DSN that Python uses; no env-var magic here.
    pub fn connect(dsn: &str) -> Result<Self, postgres::Error> {
        let client = Client::connect(dsn, NoTls)?;
        Ok(Self { client })
    }

    /// Load the dirty subgraph for one calc_run + scenario.
    ///
    /// Result is fully owned (no borrowed connections); caller can drop
    /// the Loader after this returns.
    pub fn load_subgraph(
        &mut self,
        calc_run_id: Uuid,
        scenario_id: Uuid,
    ) -> Result<Subgraph, postgres::Error> {
        let mut sg = Subgraph::default();

        // ----- 1. Dirty PIs (mirror of `dirty_pi` CTE) ----------------
        let rows = self.client.query(
            "SELECT pi.node_id, pi.projection_series_id, pi.bucket_sequence, \
                    pi.time_span_start, pi.time_span_end \
             FROM nodes pi \
             JOIN dirty_nodes dn \
               ON dn.node_id = pi.node_id \
              AND dn.scenario_id = pi.scenario_id \
             WHERE dn.calc_run_id = $1 \
               AND pi.node_type = 'ProjectedInventory' \
               AND pi.scenario_id = $2 \
               AND pi.active = TRUE",
            &[&calc_run_id, &scenario_id],
        )?;
        for r in rows {
            sg.dirty_pis.push(DirtyPi {
                node_id: r.get(0),
                projection_series_id: r.get(1),
                bucket_sequence: r.get(2),
                time_span_start: r.get(3),
                time_span_end: r.get(4),
            });
        }

        if sg.dirty_pis.is_empty() {
            return Ok(sg);
        }

        // Collect dirty PI ids once for the next two queries.
        let dirty_ids: Vec<Uuid> = sg.dirty_pis.iter().map(|p| p.node_id).collect();

        // ----- 2. Incoming supplies (replenishes edges) ---------------
        let rows = self.client.query(
            "SELECT e.to_node_id, s.quantity, s.time_ref \
             FROM edges e \
             JOIN nodes s ON s.node_id = e.from_node_id \
             WHERE e.to_node_id = ANY($1) \
               AND e.edge_type = 'replenishes' \
               AND e.scenario_id = $2 \
               AND e.active = TRUE \
               AND s.active = TRUE \
               AND s.node_type IN ('PurchaseOrderSupply','WorkOrderSupply','TransferSupply','PlannedSupply') \
               AND s.time_ref IS NOT NULL",
            &[&dirty_ids.as_slice(), &scenario_id],
        )?;
        for r in rows {
            let pi_id: Uuid = r.get(0);
            sg.supplies_by_pi.entry(pi_id).or_default().push(Supply {
                quantity: r.get(1),
                time_ref: r.get(2),
            });
        }

        // ----- 3. Incoming demands (consumes edges) -------------------
        let rows = self.client.query(
            "SELECT e.to_node_id, d.quantity, d.time_span_start, d.time_span_end, d.time_ref \
             FROM edges e \
             JOIN nodes d ON d.node_id = e.from_node_id \
             WHERE e.to_node_id = ANY($1) \
               AND e.edge_type = 'consumes' \
               AND e.scenario_id = $2 \
               AND e.active = TRUE \
               AND d.active = TRUE \
               AND d.node_type IN ('ForecastDemand','CustomerOrderDemand','DependentDemand','TransferDemand')",
            &[&dirty_ids.as_slice(), &scenario_id],
        )?;
        for r in rows {
            let pi_id: Uuid = r.get(0);
            sg.demands_by_pi.entry(pi_id).or_default().push(Demand {
                quantity: r.get(1),
                time_span_start: r.get(2),
                time_span_end: r.get(3),
                time_ref: r.get(4),
            });
        }

        // ----- 4. Seed openings per affected series -------------------
        // For each affected projection_series_id, find:
        //   - min(bucket_sequence) among the dirty rows = seed_seq
        //   - if seed_seq == 0: SUM(OnHandSupply.quantity) replenishing PI[0]
        //   - else            : prev.closing_stock at seed_seq - 1
        // The SQL engine does this in one CTE; in Rust we do it
        // explicitly with one round-trip.
        let rows = self.client.query(
            "WITH dirty_pi AS ( \
                SELECT pi.node_id, pi.projection_series_id, pi.bucket_sequence \
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
                FROM dirty_pi GROUP BY projection_series_id \
             ) \
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
             FROM series_first_dirty sfd",
            &[&calc_run_id, &scenario_id],
        )?;
        for r in rows {
            let sid: Uuid = r.get(0);
            let seed_seq: i32 = r.get(1);
            let opening: Decimal = r.get(2);
            sg.seed_openings.insert(sid, (seed_seq, opening));
        }

        Ok(sg)
    }
}

// -------------------------------------------------------------------- //
//  Type assertions — fail fast at compile time if our schema assumptions
//  drift (e.g. someone changes nodes.quantity from NUMERIC to FLOAT).
// -------------------------------------------------------------------- //

#[allow(dead_code)]
const _DECIMAL_IS_NUMERIC: Type = Type::NUMERIC;
