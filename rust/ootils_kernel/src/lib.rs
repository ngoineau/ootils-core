//! ootils_kernel — Rust hot path for the ootils-core propagation engine.
//!
//! Week 1 foundation: round-trip Decimal + date between Python and Rust to
//! validate the build chain (PyO3 + maturin + abi3 + cross-platform) and
//! the type marshalling for the two domain primitives that will dominate
//! the real kernel: arbitrary-precision decimal quantities and bucket
//! dates.
//!
//! Type boundary contracts (ADR-016 §3.2):
//!
//! - **Decimal** : passed as `str` (Python sends `str(decimal_value)`).
//!   We parse via `rust_decimal::Decimal::from_str` and return strings.
//!   Python re-wraps with `Decimal(...)` on receipt. No float involved.
//!
//! - **Date** : passed as ISO `str` (Python sends `date.isoformat()`).
//!   `chrono::NaiveDate::parse_from_str(...)` parses. Returned as ISO
//!   string. Python re-wraps with `date.fromisoformat()`.
//!
//! Why strings everywhere? PyO3's stable ABI (`abi3-py311`) does NOT
//! expose `PyDate` (datetime module is excluded from the Python limited
//! API). Strings cost ~50 ns each to parse — negligible vs the
//! arithmetic — and keep one wheel compatible across Python 3.11/3.12/
//! 3.13+ without rebuild.

mod io;
mod kernel;
mod pool;
mod propagator;
mod writeback;

use chrono::NaiveDate;
use pyo3::prelude::*;
use rust_decimal::Decimal;
use std::str::FromStr;
use uuid::Uuid;

/// Parse a Python-side ISO date string ("YYYY-MM-DD") into a NaiveDate.
fn parse_iso_date(s: &str) -> PyResult<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("bad date {s:?}: {e}")))
}

/// Parse a decimal as string, return parse errors as Python ValueError.
fn parse_decimal(s: &str) -> PyResult<Decimal> {
    Decimal::from_str(s)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("bad decimal {s:?}: {e}")))
}

/// Round-trip diagnostic — accepts (decimal-as-string, date-as-iso-string),
/// returns the same pair after a Rust-side parse + re-serialize. Used to
/// confirm the PyO3 boundary preserves precision and date semantics.
#[pyfunction]
fn echo(decimal_str: &str, date_iso: &str) -> PyResult<(String, String)> {
    let d = parse_decimal(decimal_str)?;
    let nd = parse_iso_date(date_iso)?;
    Ok((d.to_string(), nd.format("%Y-%m-%d").to_string()))
}

/// Add two decimal strings — proves arithmetic survives the boundary.
#[pyfunction]
fn add_decimals(a: &str, b: &str) -> PyResult<String> {
    let da = parse_decimal(a)?;
    let db = parse_decimal(b)?;
    Ok((da + db).to_string())
}

/// Compute days between two ISO dates (positive if `end >= start`).
/// Mirrors the engine's bucket math: `(end - start).num_days()`.
#[pyfunction]
fn days_between(start_iso: &str, end_iso: &str) -> PyResult<i64> {
    let s = parse_iso_date(start_iso)?;
    let e = parse_iso_date(end_iso)?;
    Ok((e - s).num_days())
}

/// Module version — useful to verify which wheel got loaded in case of
/// upgrade weirdness.
#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Parse a UUID string, ValueError on failure.
fn parse_uuid(s: &str) -> PyResult<Uuid> {
    Uuid::parse_str(s)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("bad uuid {s:?}: {e}")))
}

/// Diagnostic: load the dirty subgraph for (calc_run_id, scenario_id) and
/// return the count tuples. Week 2 deliverable — validates the Rust read
/// path against Postgres without doing any compute.
///
/// Returns a dict with:
///   - "n_dirty_pis": int
///   - "n_supplies": int
///   - "n_demands": int
///   - "n_series_seeds": int
///   - "elapsed_ms": float (server-side wall clock from connect to return)
#[pyfunction]
fn load_subgraph_stats<'py>(
    py: Python<'py>,
    dsn: &str,
    calc_run_id_str: &str,
    scenario_id_str: &str,
) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
    let calc_run_id = parse_uuid(calc_run_id_str)?;
    let scenario_id = parse_uuid(scenario_id_str)?;

    let t0 = std::time::Instant::now();
    let mut loader = io::Loader::connect(dsn).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("postgres connect failed: {e}"))
    })?;
    let sg = loader.load_subgraph(calc_run_id, scenario_id).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("load_subgraph failed: {e}"))
    })?;
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let d = pyo3::types::PyDict::new_bound(py);
    d.set_item("n_dirty_pis", sg.n_dirty_pis())?;
    d.set_item("n_supplies", sg.n_supplies())?;
    d.set_item("n_demands", sg.n_demands())?;
    d.set_item("n_series_seeds", sg.n_series_seeds())?;
    d.set_item("elapsed_ms", elapsed_ms)?;
    Ok(d)
}

/// Week 3: project all dirty PIs in the (calc_run_id, scenario_id) subgraph
/// **in memory**, without writing back to Postgres. Returns a list of dicts
/// where each entry corresponds to one PI, ready for parity comparison
/// against the Python and SQL engines.
///
/// Each dict has the keys:
///   - "node_id"      : str (UUID)
///   - "opening_stock": str (Decimal)
///   - "inflows"      : str (Decimal)
///   - "outflows"     : str (Decimal)
///   - "closing_stock": str (Decimal)
///   - "has_shortage" : bool
///   - "shortage_qty" : str (Decimal)
///
/// Also returns metadata in a separate dict (`stats`) with timings.
#[pyfunction]
fn project_subgraph<'py>(
    py: Python<'py>,
    dsn: &str,
    calc_run_id_str: &str,
    scenario_id_str: &str,
) -> PyResult<(Vec<Bound<'py, pyo3::types::PyDict>>, Bound<'py, pyo3::types::PyDict>)> {
    let calc_run_id = parse_uuid(calc_run_id_str)?;
    let scenario_id = parse_uuid(scenario_id_str)?;

    let t_load_start = std::time::Instant::now();
    let mut loader = io::Loader::connect(dsn).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("postgres connect failed: {e}"))
    })?;
    let sg = loader
        .load_subgraph(calc_run_id, scenario_id)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("load_subgraph failed: {e}")))?;
    let load_ms = t_load_start.elapsed().as_secs_f64() * 1000.0;

    let t_compute_start = std::time::Instant::now();
    let projection = propagator::project(&sg);
    let compute_ms = t_compute_start.elapsed().as_secs_f64() * 1000.0;

    // Serialize the results back to Python.
    let mut out: Vec<Bound<'py, pyo3::types::PyDict>> = Vec::with_capacity(projection.len());
    for (node_id, r) in &projection.results {
        let d = pyo3::types::PyDict::new_bound(py);
        d.set_item("node_id", node_id.to_string())?;
        d.set_item("opening_stock", r.opening_stock.to_string())?;
        d.set_item("inflows", r.inflows.to_string())?;
        d.set_item("outflows", r.outflows.to_string())?;
        d.set_item("closing_stock", r.closing_stock.to_string())?;
        d.set_item("has_shortage", r.has_shortage)?;
        d.set_item("shortage_qty", r.shortage_qty.to_string())?;
        out.push(d);
    }

    let stats = pyo3::types::PyDict::new_bound(py);
    stats.set_item("n_dirty_pis", sg.n_dirty_pis())?;
    stats.set_item("n_supplies", sg.n_supplies())?;
    stats.set_item("n_demands", sg.n_demands())?;
    stats.set_item("n_series_seeds", sg.n_series_seeds())?;
    stats.set_item("n_shortages_detected", projection.n_shortages())?;
    stats.set_item("load_ms", load_ms)?;
    stats.set_item("compute_ms", compute_ms)?;

    Ok((out, stats))
}

/// Week 4: full propagate-and-write — load dirty subgraph, compute every
/// PI in memory, COPY the projection into a temp table, UPDATE FROM
/// the temp table, and clear `dirty_nodes` for this calc_run. Everything
/// happens inside one transaction (atomic — same contract as the SQL
/// engine).
///
/// Returns a dict with timing breakdown + counts:
///   - n_dirty_pis, n_supplies, n_demands, n_series_seeds
///   - n_shortages_detected (PIs with closing_stock < 0)
///   - load_ms, compute_ms, copy_ms, update_ms, clear_dirty_ms
///
/// Note: shortage *detection* in the `shortages` table (safety-stock
/// based, severity score) is intentionally left to the Python wrapper
/// which calls SHORTAGES_SQL afterwards. The Rust side only persists
/// the projection results onto `nodes`.
#[pyfunction]
fn propagate_and_write<'py>(
    py: Python<'py>,
    dsn: &str,
    calc_run_id_str: &str,
    scenario_id_str: &str,
) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
    let calc_run_id = parse_uuid(calc_run_id_str)?;
    let scenario_id = parse_uuid(scenario_id_str)?;

    let stats = writeback::propagate_and_write(dsn, calc_run_id, scenario_id)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("propagate_and_write failed: {e}")))?;

    let d = pyo3::types::PyDict::new_bound(py);
    d.set_item("n_dirty_pis", stats.n_dirty_pis)?;
    d.set_item("n_supplies", stats.n_supplies)?;
    d.set_item("n_demands", stats.n_demands)?;
    d.set_item("n_series_seeds", stats.n_series_seeds)?;
    d.set_item("n_shortages_detected", stats.n_shortages_detected)?;
    d.set_item("writeback_path", stats.writeback_path)?;
    d.set_item("load_ms", stats.load_ms)?;
    d.set_item("compute_ms", stats.compute_ms)?;
    d.set_item("copy_ms", stats.copy_ms)?;
    d.set_item("update_ms", stats.update_ms)?;
    d.set_item("shortages_ms", stats.shortages_ms)?;
    d.set_item("clear_dirty_ms", stats.clear_dirty_ms)?;
    Ok(d)
}

#[pymodule]
fn ootils_kernel(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(echo, m)?)?;
    m.add_function(wrap_pyfunction!(add_decimals, m)?)?;
    m.add_function(wrap_pyfunction!(days_between, m)?)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(load_subgraph_stats, m)?)?;
    m.add_function(wrap_pyfunction!(project_subgraph, m)?)?;
    m.add_function(wrap_pyfunction!(propagate_and_write, m)?)?;
    Ok(())
}
