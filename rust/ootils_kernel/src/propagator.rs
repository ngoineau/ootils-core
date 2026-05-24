//! propagator.rs — orchestrate the per-bucket compute across a series.
//!
//! Mirrors the SQL engine's window-function CTE (`projected`) and the
//! Python engine's topological loop. For each affected projection_series:
//!
//! 1. Sort its dirty buckets by `bucket_sequence`.
//! 2. The seed opening for the lowest dirty bucket comes from
//!    `Subgraph::seed_openings` (either OnHand sum at bucket 0 or
//!    prev.closing_stock at seed_seq - 1).
//! 3. Walk forward through the dirty buckets, using the previous
//!    bucket's `closing_stock` as the current bucket's `opening_stock`.
//!
//! Output: one `PiResult` per dirty PI, keyed by `node_id`, ready for
//! the writeback step (week 4).

use crate::io::Subgraph;
use crate::kernel::{compute_pi_bucket, PiResult};
use rust_decimal::Decimal;
use std::collections::HashMap;
use uuid::Uuid;

/// Per-bucket compute results for every dirty PI in the subgraph.
///
/// Key = PI node_id. The map is sized to `dirty_pis.len()` exactly.
pub struct Projection {
    pub results: HashMap<Uuid, PiResult>,
}

impl Projection {
    pub fn len(&self) -> usize {
        self.results.len()
    }

    pub fn n_shortages(&self) -> usize {
        self.results.values().filter(|r| r.has_shortage).count()
    }
}

/// Project all dirty PIs in the subgraph.
///
/// Algorithm: group by `projection_series_id`, sort each group by
/// `bucket_sequence`, then propagate `closing_stock` forward as the next
/// bucket's `opening_stock`. Equivalent to the SQL CTE that does:
///
/// ```text
///     SUM(oh_seed) OVER (...) + COALESCE(SUM(inflows - outflows) OVER (...), 0)
/// ```
///
/// with `oh_seed` zero everywhere except at the seed bucket of each
/// series (where it equals the seed opening).
pub fn project(sg: &Subgraph) -> Projection {
    let mut results: HashMap<Uuid, PiResult> = HashMap::with_capacity(sg.dirty_pis.len());

    // Group dirty PIs by series id. We keep the index into sg.dirty_pis
    // so we can preserve owned data on the heap; the borrow checker is
    // happy because sg outlives this function call.
    let mut by_series: HashMap<Uuid, Vec<&crate::io::DirtyPi>> = HashMap::new();
    for pi in &sg.dirty_pis {
        by_series.entry(pi.projection_series_id).or_default().push(pi);
    }

    for (series_id, mut buckets) in by_series {
        // Sort by bucket_sequence ascending so we can chain closing_stock.
        buckets.sort_by_key(|p| p.bucket_sequence);

        // Seed opening: provided by io.rs's seed_openings query for
        // (series_id, min(bucket_sequence)). If the series isn't present
        // (shouldn't happen — seed_openings has one row per affected
        // series), fall back to zero with a quiet default.
        let seed_opening = sg
            .seed_openings
            .get(&series_id)
            .map(|(_, v)| *v)
            .unwrap_or(Decimal::ZERO);

        // Walk forward.
        let mut prev_closing = seed_opening;
        for pi in buckets {
            let supplies = sg.supplies_by_pi.get(&pi.node_id).map(|v| v.as_slice()).unwrap_or(&[]);
            let demands = sg.demands_by_pi.get(&pi.node_id).map(|v| v.as_slice()).unwrap_or(&[]);
            let r = compute_pi_bucket(
                prev_closing,
                supplies,
                demands,
                pi.time_span_start,
                pi.time_span_end,
            );
            prev_closing = r.closing_stock;
            results.insert(pi.node_id, r);
        }
    }

    Projection { results }
}
