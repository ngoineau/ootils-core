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

use chrono::NaiveDate;
use pyo3::prelude::*;
use rust_decimal::Decimal;
use std::str::FromStr;

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

#[pymodule]
fn ootils_kernel(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(echo, m)?)?;
    m.add_function(wrap_pyfunction!(add_decimals, m)?)?;
    m.add_function(wrap_pyfunction!(days_between, m)?)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    Ok(())
}
