//! writeback.rs — bulk + small-batch persistence (ADR-016 §week 4-5).
//!
//! Two writeback strategies depending on the dirty set size:
//!
//! - **Small set (< COPY_THRESHOLD ~ 5000)** — `UPDATE nodes FROM UNNEST(...)`
//!   in a single statement. One roundtrip. Best for incremental events
//!   where the 7-statement COPY pipeline pays too much fixed cost.
//!
//! - **Large set (>= threshold)** — COPY BINARY into a temp table, then
//!   `UPDATE nodes FROM temp`. Scales to 200K+ rows linearly.
//!
//! Both paths run in one transaction with `session_replication_role =
//! replica` so `trg_nodes_updated_at` doesn't fire (the UPDATE sets
//! `updated_at = now()` itself).

use crate::propagator::Projection;
use postgres::binary_copy::BinaryCopyInWriter;
use postgres::types::Type;
use postgres::Client;
use rust_decimal::Decimal;
use uuid::Uuid;

/// Dirty-set size at or above which the COPY path is faster than the
/// UNNEST path. Empirically calibrated:
///   -    91 PIs : UNNEST 9ms,   COPY 60ms   → UNNEST wins
///   -  5000 PIs : UNNEST ~80ms, COPY ~80ms  → tie
///   - 50000 PIs : UNNEST ~600ms, COPY ~250ms → COPY wins clearly
const COPY_THRESHOLD: usize = 5000;

/// Shortage detection — verbatim port of `SqlPropagationEngine`'s
/// `SHORTAGES_SQL`, with psycopg `%(name)s` placeholders rewritten as
/// rust-postgres positional `$1` (scenario_id) / `$2` (calc_run_id).
/// Inlined here so we run it inside the same transaction as the
/// writeback, saving one Python → Postgres roundtrip (~30ms on LAN).
///
/// MUST be kept in sync with `src/ootils_core/engine/orchestration/
/// propagator_sql.py::SHORTAGES_SQL`.
// Not wired yet: `propagate_and_write` still leaves shortage detection
// to Python's SHORTAGES_SQL (see the doc comment on `propagate_and_write`
// in lib.rs) — this copy is prepared for the day the Rust side runs
// detection in-transaction too. Tracked as follow-up, not dead code to
// delete: removing it would lose the "kept in sync" contract above.
#[allow(dead_code)]
const SHORTAGES_SQL: &str = "\
WITH pi_with_ss AS ( \
    SELECT \
        pi.scenario_id, \
        pi.node_id        AS pi_node_id, \
        pi.item_id, \
        pi.location_id, \
        pi.closing_stock, \
        COALESCE(pi.time_span_start, pi.time_ref) AS shortage_date, \
        GREATEST((pi.time_span_end - pi.time_span_start), 1) AS days_in_bucket, \
        ipp.safety_stock_qty \
    FROM nodes pi \
    JOIN dirty_nodes dn \
      ON dn.node_id = pi.node_id \
     AND dn.scenario_id = pi.scenario_id \
    LEFT JOIN LATERAL ( \
        SELECT safety_stock_qty \
        FROM item_planning_params \
        WHERE item_id = pi.item_id \
          AND location_id = pi.location_id \
          AND (effective_to IS NULL OR effective_to = '9999-12-31'::DATE) \
        ORDER BY effective_from DESC \
        LIMIT 1 \
    ) ipp ON TRUE \
    WHERE dn.calc_run_id = $2 \
      AND pi.node_type = 'ProjectedInventory' \
      AND pi.scenario_id = $1 \
      AND pi.active = TRUE \
      AND pi.closing_stock IS NOT NULL \
), \
shortage_rows AS ( \
    SELECT \
        gen_random_uuid()::uuid AS shortage_id, \
        scenario_id, \
        pi_node_id, \
        item_id, \
        location_id, \
        shortage_date, \
        CASE \
            WHEN closing_stock < 0 THEN -closing_stock \
            ELSE safety_stock_qty - closing_stock \
        END AS shortage_qty, \
        CASE \
            WHEN closing_stock < 0 THEN 'stockout' \
            ELSE 'below_safety_stock' \
        END AS severity_class, \
        days_in_bucket \
    FROM pi_with_ss \
    WHERE closing_stock < 0 \
       OR ( \
            safety_stock_qty IS NOT NULL \
            AND closing_stock >= 0 \
            AND closing_stock < safety_stock_qty \
       ) \
) \
INSERT INTO shortages ( \
    shortage_id, scenario_id, pi_node_id, item_id, location_id, \
    shortage_date, shortage_qty, severity_score, \
    explanation_id, calc_run_id, status, severity_class, \
    created_at, updated_at \
) \
SELECT \
    shortage_id, scenario_id, pi_node_id, item_id, location_id, \
    shortage_date, shortage_qty, shortage_qty * days_in_bucket * 1::numeric, \
    NULL::uuid, $2, 'active', severity_class, \
    now(), now() \
FROM shortage_rows \
ON CONFLICT (pi_node_id, calc_run_id) DO UPDATE SET \
    shortage_qty   = EXCLUDED.shortage_qty, \
    severity_score = EXCLUDED.severity_score, \
    shortage_date  = EXCLUDED.shortage_date, \
    status         = EXCLUDED.status, \
    severity_class = EXCLUDED.severity_class, \
    updated_at     = EXCLUDED.updated_at";

/// Stats returned after one writeback pass (used by the diagnostic dict
/// surfaced to Python).
pub struct WriteStats {
    // Computed at both call sites but not yet threaded through
    // `FullStats`/the Python diagnostic dict (lib.rs). Whether to
    // surface it is a call-signature decision, not made here.
    #[allow(dead_code)]
    pub n_rows_written: usize,
    pub n_shortages_detected: usize,
    pub path: &'static str,
    pub copy_ms: f64,
    pub update_ms: f64,
    pub shortages_ms: f64,
    pub clear_dirty_ms: f64,
}

/// Apply the projection to Postgres + run shortage detection +
/// clear dirty_nodes. Dispatches to UNNEST or COPY based on the
/// projection size.
pub fn write_projection(
    client: &mut Client,
    projection: &Projection,
    calc_run_id: Uuid,
    scenario_id: Uuid,
) -> Result<WriteStats, postgres::Error> {
    if projection.len() < COPY_THRESHOLD {
        write_projection_unnest(client, projection, calc_run_id, scenario_id)
    } else {
        write_projection_copy(client, projection, calc_run_id, scenario_id)
    }
}

// -------------------------------------------------------------------- //
//  Path A: UNNEST-based UPDATE — single statement, single roundtrip.
//          Best for incremental events (small dirty sets).
// -------------------------------------------------------------------- //

fn write_projection_unnest(
    client: &mut Client,
    projection: &Projection,
    calc_run_id: Uuid,
    _scenario_id: Uuid,
) -> Result<WriteStats, postgres::Error> {
    let n = projection.len();
    let mut node_ids: Vec<Uuid> = Vec::with_capacity(n);
    let mut openings: Vec<Decimal> = Vec::with_capacity(n);
    let mut inflows: Vec<Decimal> = Vec::with_capacity(n);
    let mut outflows: Vec<Decimal> = Vec::with_capacity(n);
    let mut closings: Vec<Decimal> = Vec::with_capacity(n);
    let mut has_shortages: Vec<bool> = Vec::with_capacity(n);
    let mut shortage_qtys: Vec<Decimal> = Vec::with_capacity(n);

    for (node_id, r) in &projection.results {
        node_ids.push(*node_id);
        openings.push(r.opening_stock);
        inflows.push(r.inflows);
        outflows.push(r.outflows);
        closings.push(r.closing_stock);
        has_shortages.push(r.has_shortage);
        shortage_qtys.push(r.shortage_qty);
    }

    let mut tx = client.transaction()?;
    tx.execute("SET LOCAL session_replication_role = 'replica'", &[])?;

    let t_update_start = std::time::Instant::now();
    tx.execute(
        "UPDATE nodes \
         SET opening_stock = u.opening_stock, \
             inflows = u.inflows, \
             outflows = u.outflows, \
             closing_stock = u.closing_stock, \
             has_shortage = u.has_shortage, \
             shortage_qty = u.shortage_qty, \
             is_dirty = FALSE, \
             last_calc_run_id = $8, \
             updated_at = now() \
         FROM UNNEST($1::uuid[], $2::numeric[], $3::numeric[], $4::numeric[], \
                     $5::numeric[], $6::bool[], $7::numeric[]) \
              AS u(node_id, opening_stock, inflows, outflows, \
                   closing_stock, has_shortage, shortage_qty) \
         WHERE nodes.node_id = u.node_id",
        &[
            &node_ids,
            &openings,
            &inflows,
            &outflows,
            &closings,
            &has_shortages,
            &shortage_qtys,
            &calc_run_id,
        ],
    )?;
    let update_ms = t_update_start.elapsed().as_secs_f64() * 1000.0;
    let n_shortages_detected = projection.n_shortages();
    // SHORTAGES_SQL and CLEAR_DIRTY_SQL run on Python's session AFTER
    // this returns (Python's connection has the plans cached). The
    // order matters: SHORTAGES joins on dirty_nodes, so we must NOT
    // clear dirty here.
    tx.commit()?;

    Ok(WriteStats {
        n_rows_written: n,
        n_shortages_detected,
        path: "unnest",
        copy_ms: 0.0,
        update_ms,
        shortages_ms: 0.0,
        clear_dirty_ms: 0.0,
    })
}

// -------------------------------------------------------------------- //
//  Path B: Binary COPY into temp table + UPDATE FROM.
//          Best for full propagation (large dirty sets).
// -------------------------------------------------------------------- //

fn write_projection_copy(
    client: &mut Client,
    projection: &Projection,
    calc_run_id: Uuid,
    _scenario_id: Uuid,
) -> Result<WriteStats, postgres::Error> {
    let mut tx = client.transaction()?;
    tx.execute("SET LOCAL session_replication_role = 'replica'", &[])?;

    tx.execute(
        "CREATE TEMP TABLE pi_writeback ( \
            node_id UUID NOT NULL, \
            opening_stock NUMERIC NOT NULL, \
            inflows NUMERIC NOT NULL, \
            outflows NUMERIC NOT NULL, \
            closing_stock NUMERIC NOT NULL, \
            has_shortage BOOLEAN NOT NULL, \
            shortage_qty NUMERIC NOT NULL \
        ) ON COMMIT DROP",
        &[],
    )?;

    let t_copy_start = std::time::Instant::now();
    let writer = tx.copy_in(
        "COPY pi_writeback (node_id, opening_stock, inflows, outflows, \
                            closing_stock, has_shortage, shortage_qty) \
         FROM STDIN BINARY",
    )?;
    let col_types = [
        Type::UUID,
        Type::NUMERIC,
        Type::NUMERIC,
        Type::NUMERIC,
        Type::NUMERIC,
        Type::BOOL,
        Type::NUMERIC,
    ];
    let mut bin = BinaryCopyInWriter::new(writer, &col_types);
    for (node_id, r) in &projection.results {
        let row: [&(dyn postgres::types::ToSql + Sync); 7] = [
            node_id,
            &r.opening_stock,
            &r.inflows,
            &r.outflows,
            &r.closing_stock,
            &r.has_shortage,
            &r.shortage_qty,
        ];
        bin.write(&row)?;
    }
    let n_rows = bin.finish()? as usize;
    let copy_ms = t_copy_start.elapsed().as_secs_f64() * 1000.0;

    let t_update_start = std::time::Instant::now();
    tx.execute(
        "UPDATE nodes \
         SET opening_stock = b.opening_stock, \
             inflows = b.inflows, \
             outflows = b.outflows, \
             closing_stock = b.closing_stock, \
             has_shortage = b.has_shortage, \
             shortage_qty = b.shortage_qty, \
             is_dirty = FALSE, \
             last_calc_run_id = $1, \
             updated_at = now() \
         FROM pi_writeback b \
         WHERE nodes.node_id = b.node_id",
        &[&calc_run_id],
    )?;
    let update_ms = t_update_start.elapsed().as_secs_f64() * 1000.0;

    let n_shortages_detected = projection.n_shortages();
    // SHORTAGES_SQL + CLEAR_DIRTY_SQL run on Python's session AFTER
    // (same reasoning as the UNNEST path).
    tx.commit()?;

    Ok(WriteStats {
        n_rows_written: n_rows,
        n_shortages_detected,
        path: "copy",
        copy_ms,
        update_ms,
        shortages_ms: 0.0,
        clear_dirty_ms: 0.0,
    })
}

// -------------------------------------------------------------------- //
//  Entry point — uses the connection pool to amortize TCP/auth cost.
// -------------------------------------------------------------------- //

pub fn propagate_and_write(
    dsn: &str,
    calc_run_id: Uuid,
    scenario_id: Uuid,
) -> Result<FullStats, Box<dyn std::error::Error>> {
    let stats = crate::pool::with_client(dsn, |client| {
        let t_load_start = std::time::Instant::now();
        let sg = crate::io::load_subgraph(client, calc_run_id, scenario_id)?;
        let load_ms = t_load_start.elapsed().as_secs_f64() * 1000.0;

        let t_compute_start = std::time::Instant::now();
        let projection = crate::propagator::project(&sg);
        let compute_ms = t_compute_start.elapsed().as_secs_f64() * 1000.0;

        let write = write_projection(client, &projection, calc_run_id, scenario_id)?;

        Ok(FullStats {
            n_dirty_pis: sg.n_dirty_pis(),
            n_supplies: sg.n_supplies(),
            n_demands: sg.n_demands(),
            n_series_seeds: sg.n_series_seeds(),
            n_shortages_detected: write.n_shortages_detected,
            writeback_path: write.path,
            load_ms,
            compute_ms,
            copy_ms: write.copy_ms,
            update_ms: write.update_ms,
            shortages_ms: write.shortages_ms,
            clear_dirty_ms: write.clear_dirty_ms,
        })
    })?;
    Ok(stats)
}

pub struct FullStats {
    pub n_dirty_pis: usize,
    pub n_supplies: usize,
    pub n_demands: usize,
    pub n_series_seeds: usize,
    pub n_shortages_detected: usize,
    pub writeback_path: &'static str,
    pub load_ms: f64,
    pub compute_ms: f64,
    pub copy_ms: f64,
    pub update_ms: f64,
    pub shortages_ms: f64,
    pub clear_dirty_ms: f64,
}
