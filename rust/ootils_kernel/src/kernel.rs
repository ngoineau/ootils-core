//! kernel.rs — pure compute for one PI bucket (ADR-016 §week 3).
//!
//! Mirrors `ProjectionKernel.compute_pi_node` in Python. Same semantics,
//! byte-identical results within Decimal precision (validated by the
//! 3-way parity harness in week 3).
//!
//! Pure function, no IO, no allocation beyond the result tuple. The
//! window-function part (cumulative opening_stock across buckets in a
//! series) lives in `propagator.rs` — this file is only the per-bucket
//! math.

use crate::io::{Demand, Supply};
use chrono::NaiveDate;
use rust_decimal::Decimal;

/// Result of one bucket compute. Same shape as the Python kernel's dict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiResult {
    pub opening_stock: Decimal,
    pub inflows: Decimal,
    pub outflows: Decimal,
    pub closing_stock: Decimal,
    pub has_shortage: bool,
    pub shortage_qty: Decimal,
}

/// Aggregate supplies whose `time_ref` falls in [bucket_start, bucket_end).
///
/// Mirrors the SQL engine's `SubPlan 2` (inflows correlated subquery) and
/// the Python `compute_pi_node` inflows loop. The contribution rule is
/// `point_in_bucket`: a supply contributes its full quantity iff its
/// date lands strictly inside the bucket half-open interval.
#[inline]
pub fn sum_inflows(supplies: &[Supply], bucket_start: NaiveDate, bucket_end: NaiveDate) -> Decimal {
    let mut total = Decimal::ZERO;
    for s in supplies {
        if s.time_ref >= bucket_start && s.time_ref < bucket_end {
            total += s.quantity;
        }
    }
    total
}

/// Aggregate demands. Handles two cases (mirrors the SQL engine `SubPlan 3`
/// CASE expression and the Python kernel's allocation logic):
///
/// 1. **Time-span demand** (e.g. monthly ForecastDemand): allocate the
///    quantity proportionally to the day-overlap between the demand
///    span and the bucket. The numeric(50,28) cast matches Python's
///    Decimal default precision so the SQL/Python parity holds.
///
/// 2. **Time-ref demand** (CustomerOrderDemand etc.): contribute full
///    quantity if the date falls inside the bucket, else zero.
#[inline]
pub fn sum_outflows(demands: &[Demand], bucket_start: NaiveDate, bucket_end: NaiveDate) -> Decimal {
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
                        // qty / span_days * overlap_days, preserving Decimal precision
                        let frac = d.quantity
                            / Decimal::from(span_days)
                            * Decimal::from(overlap_days);
                        total += frac;
                    }
                }
                continue;
            }
        }
        // Case 2: time_ref point. Falls back to time_span_start if time_ref
        // is None (CustomerOrderDemand can store its date either way).
        let pt = d.time_ref.or(d.time_span_start);
        if let Some(p) = pt {
            if p >= bucket_start && p < bucket_end {
                total += d.quantity;
            }
        }
    }
    total
}

/// One bucket compute. `opening_stock` is provided by the caller — it's
/// either the seed opening (for the first dirty bucket of a series) or
/// the cumulative `closing_stock` of the previous bucket (computed by
/// `propagate_series` in `propagator.rs`).
#[inline]
pub fn compute_pi_bucket(
    opening_stock: Decimal,
    supplies: &[Supply],
    demands: &[Demand],
    bucket_start: NaiveDate,
    bucket_end: NaiveDate,
) -> PiResult {
    let inflows = sum_inflows(supplies, bucket_start, bucket_end);
    let outflows = sum_outflows(demands, bucket_start, bucket_end);
    let closing_stock = opening_stock + inflows - outflows;
    let has_shortage = closing_stock < Decimal::ZERO;
    let shortage_qty = if has_shortage {
        -closing_stock
    } else {
        Decimal::ZERO
    };
    PiResult {
        opening_stock,
        inflows,
        outflows,
        closing_stock,
        has_shortage,
        shortage_qty,
    }
}

// -------------------------------------------------------------------- //
//  Unit tests — apples-to-apples vs Python kernel semantics.
// -------------------------------------------------------------------- //

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn date(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }
    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn empty_bucket() {
        let r = compute_pi_bucket(dec("100"), &[], &[], date("2026-01-01"), date("2026-01-08"));
        assert_eq!(r.opening_stock, dec("100"));
        assert_eq!(r.inflows, Decimal::ZERO);
        assert_eq!(r.outflows, Decimal::ZERO);
        assert_eq!(r.closing_stock, dec("100"));
        assert!(!r.has_shortage);
    }

    #[test]
    fn supply_point_inside_bucket() {
        let supplies = vec![Supply {
            quantity: dec("50"),
            time_ref: date("2026-01-04"),
        }];
        let r = compute_pi_bucket(dec("10"), &supplies, &[], date("2026-01-01"), date("2026-01-08"));
        assert_eq!(r.inflows, dec("50"));
        assert_eq!(r.closing_stock, dec("60"));
    }

    #[test]
    fn supply_on_bucket_end_excluded() {
        // bucket_end is EXCLUSIVE
        let supplies = vec![Supply {
            quantity: dec("50"),
            time_ref: date("2026-01-08"),
        }];
        let r = compute_pi_bucket(dec("10"), &supplies, &[], date("2026-01-01"), date("2026-01-08"));
        assert_eq!(r.inflows, Decimal::ZERO);
    }

    #[test]
    fn demand_span_full_overlap() {
        let demands = vec![Demand {
            quantity: dec("30"),
            time_span_start: Some(date("2026-01-01")),
            time_span_end: Some(date("2026-01-08")),
            time_ref: None,
        }];
        let r = compute_pi_bucket(dec("100"), &[], &demands, date("2026-01-01"), date("2026-01-08"));
        // Full 7-day span = 7 days overlap, so 100% of the 30 -> 30.
        assert_eq!(r.outflows, dec("30"));
    }

    #[test]
    fn demand_span_partial_overlap() {
        // 30-day demand, 1-day bucket starting at day 0 of the span → 1/30 of qty.
        let demands = vec![Demand {
            quantity: dec("30"),
            time_span_start: Some(date("2026-01-01")),
            time_span_end: Some(date("2026-01-31")),
            time_ref: None,
        }];
        let r = compute_pi_bucket(dec("100"), &[], &demands, date("2026-01-01"), date("2026-01-02"));
        assert_eq!(r.outflows, dec("1"));
    }

    #[test]
    fn demand_time_ref_inside() {
        let demands = vec![Demand {
            quantity: dec("20"),
            time_span_start: None,
            time_span_end: None,
            time_ref: Some(date("2026-01-05")),
        }];
        let r = compute_pi_bucket(dec("100"), &[], &demands, date("2026-01-01"), date("2026-01-08"));
        assert_eq!(r.outflows, dec("20"));
    }

    #[test]
    fn shortage_negative_closing() {
        let demands = vec![Demand {
            quantity: dec("150"),
            time_span_start: None,
            time_span_end: None,
            time_ref: Some(date("2026-01-04")),
        }];
        let r = compute_pi_bucket(dec("100"), &[], &demands, date("2026-01-01"), date("2026-01-08"));
        assert_eq!(r.closing_stock, dec("-50"));
        assert!(r.has_shortage);
        assert_eq!(r.shortage_qty, dec("50"));
    }
}
