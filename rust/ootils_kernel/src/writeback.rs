//! writeback.rs — bulk persistence via Postgres binary COPY (ADR-016 §week 4).
//!
//! Strategy:
//!   1. CREATE TEMP TABLE with the same column shape as the projected
//!      result, ON COMMIT DROP so cleanup is automatic.
//!   2. Stream the projection results into the temp table via
//!      `COPY ... FROM STDIN BINARY`. Binary format avoids text-encoding
//!      every Decimal on the way in.
//!   3. Single `UPDATE nodes ... FROM <temp>` that touches every dirty
//!      PI in one server-side pass. The planner uses a hash join on
//!      `node_id`, which is cheap for ~200K rows.
//!   4. `DELETE FROM dirty_nodes WHERE calc_run_id = ...` — the same
//!      clear-dirty semantics the SQL engine uses (CLEAR_DIRTY_SQL).
//!   5. `SET LOCAL session_replication_role = 'replica'` is used to
//!      suppress `trg_nodes_updated_at` during the bulk UPDATE. The
//!      UPDATE explicitly sets `updated_at = now()` so we lose nothing.
//!      EXPLAIN ANALYZE showed the trigger costs ~450ms over 226K rows.
//!
//! All steps run inside one transaction. If anything fails, the whole
//! propagation rolls back — same atomicity contract as the SQL engine.

use crate::propagator::Projection;
use postgres::binary_copy::BinaryCopyInWriter;
use postgres::types::Type;
use postgres::{Client, NoTls};
use uuid::Uuid;

/// Result of one full propagate-and-write cycle. Mirrors the metadata
/// fields the Python wrapper needs to update `calc_run`.
pub struct WriteStats {
    pub n_rows_written: usize,
    pub n_shortages_detected: usize,
    pub copy_ms: f64,
    pub update_ms: f64,
    pub clear_dirty_ms: f64,
}

/// Apply the projection to Postgres: COPY into temp + UPDATE FROM +
/// clear dirty. Returns timing breakdown for diagnostic.
pub fn write_projection(
    client: &mut Client,
    projection: &Projection,
    calc_run_id: Uuid,
) -> Result<WriteStats, postgres::Error> {
    let mut tx = client.transaction()?;

    // Disable triggers for this transaction (we set updated_at ourselves).
    tx.execute("SET LOCAL session_replication_role = 'replica'", &[])?;

    // Create the temp buffer. ON COMMIT DROP = goes away automatically.
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

    // ---------- COPY binary ----------
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
        // Each &dyn ToSql is borrowed for the duration of the write call.
        // We avoid heap-allocating per row by passing references to the
        // existing struct fields.
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

    // ---------- UPDATE FROM ----------
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

    // ---------- Clear dirty_nodes for this calc_run ----------
    let t_clear_start = std::time::Instant::now();
    tx.execute(
        "DELETE FROM dirty_nodes WHERE calc_run_id = $1",
        &[&calc_run_id],
    )?;
    let clear_dirty_ms = t_clear_start.elapsed().as_secs_f64() * 1000.0;

    let n_shortages_detected = projection.n_shortages();
    tx.commit()?;

    Ok(WriteStats {
        n_rows_written: n_rows,
        n_shortages_detected,
        copy_ms,
        update_ms,
        clear_dirty_ms,
    })
}

/// Convenience: open one connection, do everything (load + compute +
/// write + clear), close. Single entry point exposed to Python.
///
/// Performance note: the load step uses the SAME `client` as the
/// writeback transaction (via `crate::io::load_subgraph`). This avoids
/// opening a second TCP/auth session, which on profile L incremental
/// (91 dirty PIs) was the difference between 250ms p50 and ~100ms.
pub fn propagate_and_write(
    dsn: &str,
    calc_run_id: Uuid,
    scenario_id: Uuid,
) -> Result<FullStats, Box<dyn std::error::Error>> {
    let mut client = Client::connect(dsn, NoTls)?;

    let t_load_start = std::time::Instant::now();
    let sg = crate::io::load_subgraph(&mut client, calc_run_id, scenario_id)?;
    let load_ms = t_load_start.elapsed().as_secs_f64() * 1000.0;

    let t_compute_start = std::time::Instant::now();
    let projection = crate::propagator::project(&sg);
    let compute_ms = t_compute_start.elapsed().as_secs_f64() * 1000.0;

    let write_stats = write_projection(&mut client, &projection, calc_run_id)?;

    Ok(FullStats {
        n_dirty_pis: sg.n_dirty_pis(),
        n_supplies: sg.n_supplies(),
        n_demands: sg.n_demands(),
        n_series_seeds: sg.n_series_seeds(),
        n_shortages_detected: write_stats.n_shortages_detected,
        load_ms,
        compute_ms,
        copy_ms: write_stats.copy_ms,
        update_ms: write_stats.update_ms,
        clear_dirty_ms: write_stats.clear_dirty_ms,
    })
}

/// All-up diagnostic counters returned to Python.
pub struct FullStats {
    pub n_dirty_pis: usize,
    pub n_supplies: usize,
    pub n_demands: usize,
    pub n_series_seeds: usize,
    pub n_shortages_detected: usize,
    pub load_ms: f64,
    pub compute_ms: f64,
    pub copy_ms: f64,
    pub update_ms: f64,
    pub clear_dirty_ms: f64,
}
