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

    let mut results: Vec<(NodeIndex, PiResult)> = series_batches
        .par_iter()
        .flat_map_iter(|buckets| {
            let mut local = Vec::with_capacity(buckets.len());
            let seed_opening = compute_seed_opening_from_sorted(graph, buckets);
            let mut prev_closing = seed_opening;
            for &pi_idx in buckets {
                let result = compute_one_bucket(graph, pi_idx, prev_closing);
                prev_closing = result.closing_stock;
                local.push((pi_idx, result));
            }
            local
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

    // Otherwise look up the prev bucket (seed_seq - 1) via the series
    // index of the seed node.
    if let Some(sid) = seed_node.series_id {
        if let Some(buckets) = graph.by_series.get(&sid) {
            for &b in buckets {
                let n = &graph.nodes[b as usize];
                if n.bucket_sequence == seed_seq - 1 {
                    return n.closing_stock;
                }
            }
        }
    }
    Decimal::ZERO
}

/// Compute one PI bucket using the kernel: collect supplies + demands
/// from incoming edges, call `compute_pi_bucket`.
fn compute_one_bucket(graph: &Graph, pi_idx: NodeIndex, opening_stock: Decimal) -> PiResult {
    let pi_node = &graph.nodes[pi_idx as usize];
    let bucket_start = pi_node
        .time_span_start
        .expect("PI without time_span_start");
    let bucket_end = pi_node.time_span_end.expect("PI without time_span_end");

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

    compute_pi_bucket(opening_stock, supplies, demands, bucket_start, bucket_end)
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
