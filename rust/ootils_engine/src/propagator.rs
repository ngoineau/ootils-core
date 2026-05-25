//! propagator.rs — in-RAM propagation orchestration.
//!
//! Mirrors the SQL engine's window-function CTE and the PyO3 module's
//! `propagator.rs` from chantier A. Same algorithm, but the graph
//! lives in RAM and we never touch Postgres in the hot path.
//!
//! Algorithm:
//!   For each affected projection_series:
//!     1. Identify the dirty buckets (their `is_dirty` flag set).
//!     2. Sort them by `bucket_sequence` so we walk a series in order.
//!     3. The seed opening_stock is either:
//!         - For seed_seq=0: SUM(OnHandSupply.quantity) replenishing PI[0]
//!         - For seed_seq>0: prev.closing_stock at seed_seq-1
//!     4. Walk the dirty buckets, chaining closing_stock → next opening.
//!     5. Clear the dirty flag, update has_shortage flag, store results.
//!
//! Concurrency: a single `&mut Graph` is passed in — caller (service.rs)
//! takes the RwLock write guard. Phase 4 will refactor to per-scenario
//! COW so multiple scenarios can propagate in parallel.

use crate::kernel::{compute_pi_bucket, DemandContrib, PiResult, SupplyContrib};
use crate::state::{EdgeType, Graph, Node, NodeIndex, NodeType};
use rayon::prelude::*;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub struct PropagationStats {
    pub n_dirty: usize,
    pub n_processed: usize,
    pub n_changed: usize,
    pub n_shortages: usize,
    pub compute_us: u64,
    /// Per-PI deltas for the WAL + write-behind queue. Only the rows
    /// that actually changed. Empty if nothing changed.
    pub deltas: Vec<crate::wal::NodeDelta>,
}

/// Propagate over an explicit set of dirty PI node indices.
///
/// Returns timing + counts. Mutates the `Graph` in place: the PI
/// fields (opening/inflows/outflows/closing/shortage_qty) get the new
/// values, `has_shortage` is updated, and `is_dirty` is cleared.
pub fn propagate(graph: &mut Graph, dirty: &HashSet<NodeIndex>) -> PropagationStats {
    let t0 = std::time::Instant::now();

    // Group dirty PIs by projection_series.
    let mut by_series: HashMap<Uuid, Vec<NodeIndex>> = HashMap::new();
    for &idx in dirty {
        let n = &graph.nodes[idx as usize];
        if n.node_type != NodeType::ProjectedInventory {
            continue;
        }
        if let Some(sid) = n.series_id {
            by_series.entry(sid).or_default().push(idx);
        }
    }

    // ---- Compute phase (read-only on graph, parallel across series) ----
    // Series are independent — closing_stock cascade is internal to one
    // series, not cross-series. So we compute all series in parallel,
    // collect their per-bucket (NodeIndex, PiResult) tuples, then apply
    // mutations in a single sequential pass at the end.
    //
    // This parallelization is what lets us hit sub-100ms on profile L.
    // Empirically (8 logical cores) : 106ms → ~40ms.
    let series_batches: Vec<Vec<NodeIndex>> = by_series
        .into_iter()
        .map(|(_, mut buckets)| {
            buckets.sort_by_key(|&idx| graph.nodes[idx as usize].bucket_sequence);
            buckets
        })
        .collect();

    // F-010 + F-023 (Cluster E): each series job is wrapped in
    // catch_unwind so a panic in compute_one_bucket (e.g. arithmetic
    // overflow on bad data) doesn't unwind through rayon and kill the
    // worker thread. Combined with panic="unwind" in [profile.release]
    // (Cargo.toml) this gives the propagator a per-series fault
    // boundary: one bad PI series fails, the rest still compute.
    // PIs with NULL time_span_* are also skipped at compute time
    // (compute_one_bucket returns None).
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let mut results: Vec<(NodeIndex, PiResult)> = series_batches
        .par_iter()
        .flat_map_iter(|buckets| {
            // AssertUnwindSafe is safe here because `graph` is a
            // shared immutable reference during the compute phase
            // (we only mutate in the apply phase below, single-threaded).
            // A panicked series leaves no broken state to observe.
            match catch_unwind(AssertUnwindSafe(|| compute_one_series(graph, buckets))) {
                Ok(local) => local,
                Err(_panic) => {
                    tracing::error!(
                        n_buckets = buckets.len(),
                        first_node_id = %graph.nodes[buckets[0] as usize].node_id,
                        "rayon propagator job panicked, skipping series — \
                         engine survives (F-023). Check upstream data quality."
                    );
                    Vec::new()
                }
            }
        })
        .collect();

    // ---- Apply phase (single-threaded, mutable) ----
    // Sort by NodeIndex for cache-friendly access. Negligible vs the
    // compute phase that dominates.
    results.sort_unstable_by_key(|(idx, _)| *idx);

    let mut n_processed = 0usize;
    let mut n_changed = 0usize;
    let mut n_shortages = 0usize;
    let mut deltas: Vec<crate::wal::NodeDelta> = Vec::new();

    for (pi_idx, result) in results {
        n_processed += 1;
        let node = &mut graph.nodes[pi_idx as usize];
        let changed = node.opening_stock != result.opening_stock
            || node.inflows != result.inflows
            || node.outflows != result.outflows
            || node.closing_stock != result.closing_stock
            || node.shortage_qty != result.shortage_qty
            || node.has_shortage() != result.has_shortage;
        node.opening_stock = result.opening_stock;
        node.inflows = result.inflows;
        node.outflows = result.outflows;
        node.closing_stock = result.closing_stock;
        node.shortage_qty = result.shortage_qty;
        if result.has_shortage {
            node.flags |= Node::FLAG_SHORTAGE;
            n_shortages += 1;
        } else {
            node.flags &= !Node::FLAG_SHORTAGE;
        }
        node.flags &= !Node::FLAG_DIRTY;
        if changed {
            n_changed += 1;
            deltas.push(crate::wal::NodeDelta {
                node_id: node.node_id,
                opening_stock: node.opening_stock,
                inflows: node.inflows,
                outflows: node.outflows,
                closing_stock: node.closing_stock,
                has_shortage: result.has_shortage,
                shortage_qty: node.shortage_qty,
            });
        }
    }

    graph.generation = graph.generation.wrapping_add(1);
    let compute_us = t0.elapsed().as_micros() as u64;

    PropagationStats {
        n_dirty: dirty.len(),
        n_processed,
        n_changed,
        n_shortages,
        compute_us,
        deltas,
    }
}

/// Compute one series end-to-end: walk the dirty buckets in order
/// from the seed, chaining closing_stock → next opening. Returns a
/// vector of (NodeIndex, PiResult) to apply in the mutation phase.
/// Buckets with NULL time_span_* are skipped (F-010) without
/// advancing the cascade — subsequent buckets continue from the last
/// good closing_stock.
fn compute_one_series(graph: &Graph, buckets: &[NodeIndex]) -> Vec<(NodeIndex, PiResult)> {
    let mut local = Vec::with_capacity(buckets.len());
    let seed_opening = compute_seed_opening_from_sorted(graph, buckets);
    let mut prev_closing = seed_opening;
    for &pi_idx in buckets {
        match compute_one_bucket(graph, pi_idx, prev_closing) {
            Some(result) => {
                prev_closing = result.closing_stock;
                local.push((pi_idx, result));
            }
            None => {
                let n = &graph.nodes[pi_idx as usize];
                tracing::warn!(
                    node_id = %n.node_id,
                    "skipping PI with NULL time_span_* — fix upstream data"
                );
            }
        }
    }
    local
}

/// Compute the seed opening_stock from a pre-sorted list of dirty bucket
/// indices for one series. Takes the first bucket's `series_id` itself
/// (no need to pass it separately).
fn compute_seed_opening_from_sorted(graph: &Graph, sorted_dirty: &[NodeIndex]) -> Decimal {
    let &seed_idx = match sorted_dirty.first() {
        Some(x) => x,
        None => return Decimal::ZERO,
    };
    let seed_node = &graph.nodes[seed_idx as usize];
    let seed_seq = seed_node.bucket_sequence;

    if seed_seq == 0 {
        // Sum OnHand supplies replenishing PI[0] of this series.
        let mut total = Decimal::ZERO;
        if let Some(edges) = graph.edges_in.get(&seed_idx) {
            for e in edges {
                if e.edge_type != EdgeType::Replenishes {
                    continue;
                }
                let src = &graph.nodes[e.from as usize];
                if src.node_type == NodeType::OnHandSupply && src.is_active() {
                    total += src.quantity;
                }
            }
        }
        return total;
    }

    // F-022 fix: the loader pre-sorts `by_series[sid]` by
    // `bucket_sequence` so we can binary-search for `seed_seq - 1` in
    // O(log N) instead of linearly scanning the ~90 buckets. Hot path
    // on every incremental propagation.
    if let Some(sid) = seed_node.series_id {
        if let Some(buckets) = graph.by_series.get(&sid) {
            let target = seed_seq - 1;
            if let Ok(pos) = buckets
                .binary_search_by_key(&target, |&idx| graph.nodes[idx as usize].bucket_sequence)
            {
                return graph.nodes[buckets[pos] as usize].closing_stock;
            }
        }
    }
    Decimal::ZERO
}

/// Compute one PI bucket using the kernel: collect supplies + demands
/// from incoming edges, call `compute_pi_bucket`.
///
/// F-010 fix: a PI with NULL time_span_* used to panic via
/// `expect()`. With panic=abort that meant a single bad row in PG
/// became an infinite engine crash loop on boot+restart. Now we
/// return `None` and the caller skips this PI cleanly. The
/// `mark_all_pi_dirty` and dirty-set construction paths log a warn
/// when they encounter such PIs so operators can fix the upstream
/// data.
fn compute_one_bucket(
    graph: &Graph,
    pi_idx: NodeIndex,
    opening_stock: Decimal,
) -> Option<PiResult> {
    let pi_node = &graph.nodes[pi_idx as usize];
    let bucket_start = pi_node.time_span_start?;
    let bucket_end = pi_node.time_span_end?;

    let edges = graph.edges_in.get(&pi_idx);

    // Build the supplies + demands iterators lazily — `compute_pi_bucket`
    // takes IntoIterator so we don't allocate Vecs here.
    let supplies = edges.into_iter().flat_map(|es| {
        es.iter().filter_map(|e| {
            if e.edge_type != EdgeType::Replenishes {
                return None;
            }
            let src = &graph.nodes[e.from as usize];
            // OnHand supplies feed PI[0] only (handled in seed). For all
            // other buckets we consume PO/WO/Transfer/Planned supplies.
            if !src.is_active()
                || src.node_type == NodeType::OnHandSupply
                || !src.node_type.is_supply()
            {
                return None;
            }
            let t = src.time_ref?;
            Some(SupplyContrib {
                quantity: &src.quantity,
                time_ref: t,
            })
        })
    });

    let demands = edges.into_iter().flat_map(|es| {
        es.iter().filter_map(|e| {
            if e.edge_type != EdgeType::Consumes {
                return None;
            }
            let src = &graph.nodes[e.from as usize];
            if !src.is_active() || !src.node_type.is_demand() {
                return None;
            }
            Some(DemandContrib {
                quantity: &src.quantity,
                time_span_start: src.time_span_start,
                time_span_end: src.time_span_end,
                time_ref: src.time_ref,
            })
        })
    });

    Some(compute_pi_bucket(opening_stock, supplies, demands, bucket_start, bucket_end))
}

/// Convenience for the "full propagation" bench: mark every active PI
/// dirty, then propagate.
pub fn mark_all_pi_dirty(graph: &mut Graph) -> HashSet<NodeIndex> {
    let mut dirty = HashSet::with_capacity(graph.nodes.len() / 2);
    for (idx, n) in graph.nodes.iter_mut().enumerate() {
        if n.node_type == NodeType::ProjectedInventory && n.is_active() {
            n.flags |= Node::FLAG_DIRTY;
            dirty.insert(idx as NodeIndex);
        }
    }
    dirty
}
