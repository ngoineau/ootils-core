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

/// Output of the read-only compute phase, ready to be applied to the
/// graph under a brief write lock. Carries the dirty-PI count so the
/// caller can still report `n_dirty` accurately.
pub struct ComputedResults {
    // `apply`/`apply_scenario` callers derive their own dirty count
    // from the `&HashSet<NodeIndex>` they already hold (see
    // `PropagationStats.n_dirty`, populated from `dirty.len()`), so
    // this field is never read back out of `ComputedResults`. Kept on
    // the struct as the natural place to carry it if that changes.
    #[allow(dead_code)]
    pub n_dirty: usize,
    pub results: Vec<(NodeIndex, PiResult)>,
    pub compute_us: u64,
}

/// F-009 fix (Cluster F-009 / E2 follow-up): two-phase propagate.
///
/// Old contract: one function took `&mut Graph` and held the write
/// lock through both the rayon parallel compute (~ms) and the apply.
/// This blocked all concurrent readers (Health/GetNode/ListScenarios)
/// for the entire compute duration.
///
/// New contract:
/// - `plan_compute(&Graph, &dirty)` runs the parallel rayon work under
///   a READ lock — concurrent reads of the graph (other handlers)
///   coexist via parking_lot::RwLock's multi-reader semantics.
/// - `apply(&mut Graph, ComputedResults, &dirty)` takes a brief write
///   lock to write back the values + clear dirty flags + bump
///   generation. Apply is sequential and bounded by N_dirty
///   memory-writes — sub-millisecond.
///
/// Callers (service.rs) must serialize propagations among themselves
/// via a separate non-async mutex so two simultaneous propagations
/// don't compute against the same read state and then both apply
/// (the second would overwrite the first's deltas with stale data).
/// `EngineSvc::propagation_lock` is that gate.
pub fn plan_compute(graph: &Graph, dirty: &HashSet<NodeIndex>) -> ComputedResults {
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

    // Series are independent — closing_stock cascade is internal to
    // one series, not cross-series. So we compute all series in
    // parallel, collect their per-bucket (NodeIndex, PiResult) tuples,
    // then return them; the caller's apply phase mutates.
    //
    // This parallelization is what lets us hit sub-100ms on profile L.
    // Empirically (8 logical cores): 106ms → ~40ms.
    let series_batches: Vec<Vec<NodeIndex>> = by_series
        .into_values()
        .map(|mut buckets| {
            buckets.sort_by_key(|&idx| graph.nodes[idx as usize].bucket_sequence);
            buckets
        })
        .collect();

    // F-010 + F-023: per-series catch_unwind fault boundary.
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let mut results: Vec<(NodeIndex, PiResult)> = series_batches
        .par_iter()
        .flat_map_iter(|buckets| {
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

    // Sort by NodeIndex once here, in the compute phase, so the apply
    // phase has cache-friendly sequential writes (no cost vs sorting
    // after — the data is already in this thread's cache).
    results.sort_unstable_by_key(|(idx, _)| *idx);

    ComputedResults {
        n_dirty: dirty.len(),
        results,
        compute_us: t0.elapsed().as_micros() as u64,
    }
}

/// Apply the precomputed results to the graph under a brief write
/// lock. F-009: this is the ONLY phase that needs an exclusive lock.
/// Apply is sequential O(N_changed) — sub-ms on profile L.
pub fn apply(
    graph: &mut Graph,
    computed: ComputedResults,
    dirty: &HashSet<NodeIndex>,
) -> PropagationStats {
    let mut n_processed = 0usize;
    let mut n_changed = 0usize;
    let mut n_shortages = 0usize;
    let mut deltas: Vec<crate::wal::NodeDelta> = Vec::new();

    for (pi_idx, result) in computed.results {
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

    PropagationStats {
        n_dirty: dirty.len(),
        n_processed,
        n_changed,
        n_shortages,
        compute_us: computed.compute_us,
        deltas,
    }
}

/// Compatibility shim for callers (the bench mode in main.rs) that
/// still expect a single-call mutating propagate. Runs compute + apply
/// back-to-back under the caller's existing write lock. Production
/// hot path (service.rs::propagate) uses plan_compute + apply
/// separately for the F-009 locking improvement.
pub fn propagate(graph: &mut Graph, dirty: &HashSet<NodeIndex>) -> PropagationStats {
    let computed = plan_compute(graph, dirty);
    apply(graph, computed, dirty)
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

    Some(compute_pi_bucket(
        opening_stock,
        supplies,
        demands,
        bucket_start,
        bucket_end,
    ))
}

// ============================================================
// Per-scenario propagation (P2.1.b — ADR-018 closure)
// ============================================================
//
// Scenarios use a different storage model than the baseline:
//   - The baseline (Graph) is mutated in-place during baseline propag
//     (then atomic-swapped via ArcSwap from P2.1.a).
//   - A Scenario has an IMMUTABLE Arc<Graph> snapshot + a sparse
//     `overlay: DashMap<NodeIndex, Node>` of modified nodes.
//   - All reads check overlay first, fall back to snapshot.
//   - All writes go to overlay (snapshot is never touched).
//
// Edges + indexes (by_series, by_node_id, etc.) are NEVER mutated by
// scenario propagation — they live in the snapshot and are shared
// read-only.
//
// Scenario propagation is much cheaper than baseline propagation:
//   - No graph clone (overlay writes are O(1) DashMap inserts)
//   - No WAL append (scenarios are ephemeral by design in 2.1.b;
//     persistence comes in P2.2)
//   - No PG write-behind (scenarios don't reach Postgres in 2.1.b)
//
// The per-bucket compute is slightly costlier than baseline because
// each source node read involves an overlay lookup + a clone. For
// incremental propagation (1 series = ~90 PIs × ~20 source nodes =
// ~1800 reads), that's a few µs of extra DashMap probing. Worth it
// for the architectural simplicity.

use crate::scenario::Scenario;

/// Scenario equivalent of `plan_compute`. Same rayon parallelism,
/// same per-series catch_unwind boundary, same output shape — but
/// all node reads route through the overlay-aware view.
pub fn plan_compute_scenario(scenario: &Scenario, dirty: &HashSet<NodeIndex>) -> ComputedResults {
    let t0 = std::time::Instant::now();
    let snapshot = &scenario.baseline_snapshot;

    // Group dirty PIs by projection_series. Reads via the overlay
    // because a user may have changed the series_id (rare but
    // legal). For node_type we trust the snapshot — node_type is
    // not user-mutable through the engine API.
    let mut by_series: HashMap<Uuid, Vec<NodeIndex>> = HashMap::new();
    for &idx in dirty {
        let n_snap = &snapshot.nodes[idx as usize];
        if n_snap.node_type != NodeType::ProjectedInventory {
            continue;
        }
        let sid_opt = scenario.get_node_cloned(idx).and_then(|n| n.series_id);
        if let Some(sid) = sid_opt {
            by_series.entry(sid).or_default().push(idx);
        }
    }

    // Sort each series' buckets by bucket_sequence. Read from snapshot
    // (bucket_sequence is set at boot, immutable in practice).
    let series_batches: Vec<Vec<NodeIndex>> = by_series
        .into_values()
        .map(|mut buckets| {
            buckets.sort_by_key(|&idx| snapshot.nodes[idx as usize].bucket_sequence);
            buckets
        })
        .collect();

    use std::panic::{catch_unwind, AssertUnwindSafe};
    let mut results: Vec<(NodeIndex, PiResult)> = series_batches
        .par_iter()
        .flat_map_iter(|buckets| {
            match catch_unwind(AssertUnwindSafe(|| {
                compute_one_series_scenario(scenario, buckets)
            })) {
                Ok(local) => local,
                Err(_panic) => {
                    tracing::error!(
                        scenario_id = %scenario.id,
                        n_buckets = buckets.len(),
                        first_node_id = %snapshot.nodes[buckets[0] as usize].node_id,
                        "rayon scenario propagator job panicked, skipping series \
                         — engine survives (F-023)."
                    );
                    Vec::new()
                }
            }
        })
        .collect();

    results.sort_unstable_by_key(|(idx, _)| *idx);

    ComputedResults {
        n_dirty: dirty.len(),
        results,
        compute_us: t0.elapsed().as_micros() as u64,
    }
}

/// Apply scenario results: writes to the overlay (DashMap insert).
/// No graph mutation. No WAL. No PG. Just overlay updates.
pub fn apply_scenario(
    scenario: &Scenario,
    computed: ComputedResults,
    dirty: &HashSet<NodeIndex>,
) -> PropagationStats {
    let mut n_processed = 0usize;
    let mut n_changed = 0usize;
    let mut n_shortages = 0usize;

    for (pi_idx, result) in computed.results {
        n_processed += 1;
        // Start from the current scenario view (overlay or snapshot).
        let mut node = match scenario.get_node_cloned(pi_idx) {
            Some(n) => n,
            None => continue, // PI not in graph (shouldn't happen, defensive)
        };
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
        // ALWAYS write to overlay (even if !changed, so the overlay
        // captures the "computed against this scenario state" intent).
        // For perf we could skip the write when !changed AND not in
        // overlay yet — but that's a micro-opt; DashMap insert is
        // fast.
        scenario.overlay.insert(pi_idx, node);
        if changed {
            n_changed += 1;
        }
    }

    PropagationStats {
        n_dirty: dirty.len(),
        n_processed,
        n_changed,
        n_shortages,
        compute_us: computed.compute_us,
        deltas: Vec::new(), // scenarios don't go to WAL/PG in P2.1.b
    }
}

/// Scenario equivalent of `compute_one_series`. Same chaining logic;
/// per-bucket reads via overlay-aware path.
fn compute_one_series_scenario(
    scenario: &Scenario,
    buckets: &[NodeIndex],
) -> Vec<(NodeIndex, PiResult)> {
    let mut local = Vec::with_capacity(buckets.len());
    let seed_opening = compute_seed_opening_scenario(scenario, buckets);
    let mut prev_closing = seed_opening;
    for &pi_idx in buckets {
        match compute_one_bucket_scenario(scenario, pi_idx, prev_closing) {
            Some(result) => {
                prev_closing = result.closing_stock;
                local.push((pi_idx, result));
            }
            None => {
                let node_id = scenario
                    .get_node_cloned(pi_idx)
                    .map(|n| n.node_id.to_string())
                    .unwrap_or_else(|| "?".to_string());
                tracing::warn!(
                    scenario_id = %scenario.id,
                    node_id = %node_id,
                    "skipping PI with NULL time_span_* — fix upstream data"
                );
            }
        }
    }
    local
}

/// Scenario equivalent of `compute_seed_opening_from_sorted`.
/// PI[0] seed = sum of OnHand supplies (overlay-aware).
/// PI[N>0] seed = previous bucket's closing_stock (overlay-aware).
fn compute_seed_opening_scenario(scenario: &Scenario, sorted_dirty: &[NodeIndex]) -> Decimal {
    let &seed_idx = match sorted_dirty.first() {
        Some(x) => x,
        None => return Decimal::ZERO,
    };
    let seed_node = match scenario.get_node_cloned(seed_idx) {
        Some(n) => n,
        None => return Decimal::ZERO,
    };
    let seed_seq = seed_node.bucket_sequence;
    let snapshot = &scenario.baseline_snapshot;

    if seed_seq == 0 {
        // Sum overlay-aware OnHand supplies replenishing PI[0].
        let mut total = Decimal::ZERO;
        if let Some(edges) = snapshot.edges_in.get(&seed_idx) {
            for e in edges {
                if e.edge_type != EdgeType::Replenishes {
                    continue;
                }
                let src = match scenario.get_node_cloned(e.from) {
                    Some(n) => n,
                    None => continue,
                };
                if src.node_type == NodeType::OnHandSupply && src.is_active() {
                    total += src.quantity;
                }
            }
        }
        return total;
    }

    // Previous bucket lookup via the immutable snapshot index, but
    // the prev bucket's CLOSING_STOCK may be in overlay.
    if let Some(sid) = seed_node.series_id {
        if let Some(buckets) = snapshot.by_series.get(&sid) {
            let target = seed_seq - 1;
            if let Ok(pos) = buckets
                .binary_search_by_key(&target, |&idx| snapshot.nodes[idx as usize].bucket_sequence)
            {
                let prev_idx = buckets[pos];
                if let Some(prev_node) = scenario.get_node_cloned(prev_idx) {
                    return prev_node.closing_stock;
                }
            }
        }
    }
    Decimal::ZERO
}

/// Scenario equivalent of `compute_one_bucket`. Reads source nodes
/// via overlay-aware path; calls the same kernel.
fn compute_one_bucket_scenario(
    scenario: &Scenario,
    pi_idx: NodeIndex,
    opening_stock: Decimal,
) -> Option<PiResult> {
    let pi_node = scenario.get_node_cloned(pi_idx)?;
    let bucket_start = pi_node.time_span_start?;
    let bucket_end = pi_node.time_span_end?;
    let snapshot = &scenario.baseline_snapshot;

    // Pre-fetch source nodes (overlay-aware). The kernel borrows
    // `&Decimal` from the contribs, so we need to hold these Vec'd
    // copies for the whole kernel call. ~20-40 Node clones per
    // bucket × 150 bytes ≈ a few KB of stack/alloc churn — well
    // within mimalloc's fast path.
    let edges = snapshot.edges_in.get(&pi_idx);
    let mut active_srcs: Vec<(EdgeType, Node)> = Vec::new();
    if let Some(es) = edges {
        for e in es {
            if let Some(src) = scenario.get_node_cloned(e.from) {
                if src.is_active() {
                    active_srcs.push((e.edge_type, src));
                }
            }
        }
    }

    let supplies = active_srcs.iter().filter_map(|(et, src)| {
        if *et != EdgeType::Replenishes {
            return None;
        }
        if src.node_type == NodeType::OnHandSupply || !src.node_type.is_supply() {
            return None;
        }
        let t = src.time_ref?;
        Some(SupplyContrib {
            quantity: &src.quantity,
            time_ref: t,
        })
    });

    let demands = active_srcs.iter().filter_map(|(et, src)| {
        if *et != EdgeType::Consumes {
            return None;
        }
        if !src.node_type.is_demand() {
            return None;
        }
        Some(DemandContrib {
            quantity: &src.quantity,
            time_span_start: src.time_span_start,
            time_span_end: src.time_span_end,
            time_ref: src.time_ref,
        })
    });

    Some(compute_pi_bucket(
        opening_stock,
        supplies,
        demands,
        bucket_start,
        bucket_end,
    ))
}

/// Convenience for the "full propagation" bench: mark every active PI
/// dirty, then propagate.
///
/// F-049: NOTE on the dirty flag's semantics. This function sets
/// FLAG_DIRTY on the in-RAM Node so a future is_dirty() check would
/// reflect it. The propagator's `apply` phase ALSO clears FLAG_DIRTY
/// for every PI it processes, so by the time propagate() returns the
/// flag is back to 0 for every entry in the returned set. The
/// returned HashSet is the authoritative source of "what was dirty
/// this call" — callers should not consult the flag afterwards.
/// (See `propagator::apply` which clears `node.flags &= !FLAG_DIRTY`.)
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
