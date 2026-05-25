//! kernel.rs — pure compute for one PI bucket.
//!
//! Direct port of `ootils_kernel/src/kernel.rs` (PyO3 module from
//! Architecture A, parity-validated against the Python kernel — 0
//! mismatches across 385K PIs on profiles S/M/L per the chantier A
//! benches).
//!
//! Why duplicate instead of depending on ootils_kernel? The PyO3 crate
//! exposes Python-bound entry points (`#[pymodule]`, `#[pyfunction]`)
//! and pulls in libpython at link time. The standalone engine service
//! doesn't want any Python runtime in its dependency closure — it's a
//! pure binary. So we lift the pure-Rust kernel functions here and
//! drop the Python bindings. Same arithmetic, same parity contract.

use chrono::NaiveDate;
use rust_decimal::Decimal;

/// Result of one bucket compute. Same shape as the Python kernel's dict
/// + the parity-validated PyO3 `PiResult`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiResult {
    pub opening_stock: Decimal,
    pub inflows: Decimal,
    pub outflows: Decimal,
    pub closing_stock: Decimal,
    pub has_shortage: bool,
    pub shortage_qty: Decimal,
}

/// Inputs for one bucket — references into the in-RAM Graph so the
/// kernel touches no Postgres.
pub struct SupplyContrib<'a> {
    pub quantity: &'a Decimal,
    pub time_ref: NaiveDate,
}

pub struct DemandContrib<'a> {
    pub quantity: &'a Decimal,
    pub time_span_start: Option<NaiveDate>,
    pub time_span_end: Option<NaiveDate>,
    pub time_ref: Option<NaiveDate>,
}

/// Sum supplies whose `time_ref` falls in `[bucket_start, bucket_end)`.
/// Mirrors the `point_in_bucket` contribution rule.
#[inline]
pub fn sum_inflows<'a, I>(
    supplies: I,
    bucket_start: NaiveDate,
    bucket_end: NaiveDate,
) -> Decimal
where
    I: IntoIterator<Item = SupplyContrib<'a>>,
{
    let mut total = Decimal::ZERO;
    for s in supplies {
        if s.time_ref >= bucket_start && s.time_ref < bucket_end {
            total += s.quantity;
        }
    }
    total
}

/// Sum demand contributions. Handles both:
///  - Span demands (e.g. monthly forecasts): allocate proportionally to
///    the day-overlap between the demand span and the bucket.
///  - Point demands (CustomerOrderDemand): full quantity if date in
///    bucket.
/// The CASE / numeric(50,28) cast preserves parity with Python kernel.
#[inline]
pub fn sum_outflows<'a, I>(
    demands: I,
    bucket_start: NaiveDate,
    bucket_end: NaiveDate,
) -> Decimal
where
    I: IntoIterator<Item = DemandContrib<'a>>,
{
    let mut total = Decimal::ZERO;
    for d in demands {
        // Case 1: time_span allocation.
        if let (Some(ds), Some(de)) = (d.time_span_start, d.time_span_end) {
            if de > ds {
                let span_days = (de - ds).num_days();
                if span_days > 0 {
                    let overlap_start = bucket_start.max(ds);
                    let overlap_end = bucket_end.min(de);
                    let overlap_days = (overlap_end - overlap_start).num_days().max(0);
                    if overlap_days > 0 {
                        // F-021: multiply first, divide last, to avoid
                        // losing low-order digits inside rust_decimal's
                        // 28-digit fixed-point representation. The
                        // SQL engine's numeric(50,28) has 50-digit
                        // precision — divide-first here could drift
                        // > TOLERANCE (1e-18) under high-quantity /
                        // long-span demands.
                        let frac = d.quantity * Decimal::from(overlap_days)
                            / Decimal::from(span_days);
                        total += frac;
                    }
                }
                continue;
            }
        }
        // Case 2: time_ref point.
        let pt = d.time_ref.or(d.time_span_start);
        if let Some(p) = pt {
            if p >= bucket_start && p < bucket_end {
                total += d.quantity;
            }
        }
    }
    total
}

/// Compute one PI bucket given its inputs. Pure function — no IO.
#[inline]
pub fn compute_pi_bucket<'a, IS, ID>(
    opening_stock: Decimal,
    supplies: IS,
    demands: ID,
    bucket_start: NaiveDate,
    bucket_end: NaiveDate,
) -> PiResult
where
    IS: IntoIterator<Item = SupplyContrib<'a>>,
    ID: IntoIterator<Item = DemandContrib<'a>>,
{
    let inflows = sum_inflows(supplies, bucket_start, bucket_end);
    let outflows = sum_outflows(demands, bucket_start, bucket_end);
    let closing_stock = opening_stock + inflows - outflows;
    let has_shortage = closing_stock < Decimal::ZERO;
    let shortage_qty = if has_shortage { -closing_stock } else { Decimal::ZERO };
    PiResult {
        opening_stock,
        inflows,
        outflows,
        closing_stock,
        has_shortage,
        shortage_qty,
    }
}
