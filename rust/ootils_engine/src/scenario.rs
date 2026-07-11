//! scenario.rs — Copy-on-write scenario forks (ADR-017 §3.2, phase 4).
//!
//! Model:
//! - One implicit "baseline" — held by `EngineSvc` as `Arc<RwLock<Graph>>`,
//!   mutated in place by the phase-3 propagator.
//! - Many explicit named scenarios — each holds an `Arc<Graph>` snapshot
//!   taken at fork time + a `DashMap<NodeIndex, Node>` overlay of
//!   modified nodes. The Arc<Graph> is cheap to clone (refcount bump);
//!   the snapshot itself was paid for at fork time.
//! - Reads on a scenario consult the overlay first, fall back to the
//!   snapshot.
//! - Writes go to the overlay only — the snapshot is immutable.
//! - Merge: apply the overlay into the baseline under the write lock.
//!
//! Cost model (profile L, ~330K nodes, 76 MB graph):
//! - Fork = clone the baseline Graph into Arc<Graph> + alloc empty
//!   DashMap. Measured: 100-200 ms. ADR §3.2 promised 20-50 ms — that
//!   was for an Arc<Graph> baseline (pure refcount). Achieving the
//!   ADR target requires switching baseline to ArcSwap<Graph>, which
//!   makes baseline mutation expensive. Trade-off documented; phase 5+
//!   may revisit.
//! - Read = O(1) overlay lookup + O(1) baseline lookup.
//! - Write to overlay = O(1) DashMap insert.
//! - Merge = O(|overlay|) — fast if overlay is small.

use crate::state::{Graph, Node, NodeIndex};
use ahash::RandomState;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use uuid::Uuid;

/// One scenario — a fork of the baseline with its own diff overlay.
pub struct Scenario {
    pub id: Uuid,
    pub name: String,
    pub parent_id: Option<Uuid>,
    /// Snapshot of the baseline at fork time. Immutable from here on.
    pub baseline_snapshot: Arc<Graph>,
    /// Modified nodes — diff from the snapshot.
    pub overlay: DashMap<NodeIndex, Node, RandomState>,
    // Monotonic counterpart to `created_at_system` below — kept for a
    // future age/uptime computation; nothing reads it today (gRPC
    // responses serialize `created_at_system` via `Timestamp::from`,
    // and TTL eviction uses `last_accessed_at`, not this field).
    #[allow(dead_code)]
    pub created_at_instant: Instant,
    pub created_at_system: SystemTime,
    /// P2.1.c: per-scenario propagation serializer. Two propagations
    /// on the SAME scenario are sequenced (prevents lost-update on
    /// overlay); propagations on DIFFERENT scenarios run in parallel
    /// (Alice and Bob don't block each other).
    pub propagation_lock: parking_lot::Mutex<()>,
    /// P2.1.d: last time this scenario was accessed (read OR mutated).
    /// Used by the TTL eviction background task. Updated to "now"
    /// on every Propagate / GetNode / merge etc.
    pub last_accessed_at: parking_lot::Mutex<Instant>,
    /// P3.4 (agent-first): per-scenario minimum TTL. Set at fork time
    /// (via ForkRequest.ttl_seconds). The eviction scanner uses
    /// `max(default_ttl, this)` so an agent that requests 6h lifetime
    /// won't be reaped by the default 1h policy. 0 = use default.
    pub min_ttl_seconds: u64,
}

impl Scenario {
    // Convenience constructor for the per-scenario propagation module
    // sketched in `propagator_scenario.rs.skeleton` (not yet wired
    // into `mod` — see main.rs). Every live call site goes through
    // `with_options` directly (fork_from_baseline_with_ttl,
    // fork_from_scenario).
    #[allow(dead_code)]
    pub fn new(name: String, parent_id: Option<Uuid>, baseline: Arc<Graph>) -> Self {
        Self::with_options(
            name,
            parent_id,
            baseline,
            DashMap::with_hasher(RandomState::new()),
            0,
        )
    }

    /// Full constructor used by fork paths that may inherit a parent's
    /// overlay (P3.5) or pin a per-scenario TTL (P3.4).
    pub fn with_options(
        name: String,
        parent_id: Option<Uuid>,
        baseline: Arc<Graph>,
        overlay: DashMap<NodeIndex, Node, RandomState>,
        min_ttl_seconds: u64,
    ) -> Self {
        let now_instant = Instant::now();
        Self {
            id: Uuid::new_v4(),
            name,
            parent_id,
            baseline_snapshot: baseline,
            overlay,
            created_at_instant: now_instant,
            created_at_system: SystemTime::now(),
            propagation_lock: parking_lot::Mutex::new(()),
            last_accessed_at: parking_lot::Mutex::new(now_instant),
            min_ttl_seconds,
        }
    }

    /// Touch the access timestamp — call on any read or mutation so
    /// the TTL eviction doesn't drop an actively-used scenario.
    pub fn touch_accessed(&self) {
        *self.last_accessed_at.lock() = Instant::now();
    }

    /// Seconds since last access — used by the TTL eviction task.
    pub fn idle_seconds(&self) -> u64 {
        self.last_accessed_at.lock().elapsed().as_secs()
    }

    /// Look up a node, preferring the overlay. Returns an owned `Node`
    /// (clone). Cheap — Node is ~150 bytes.
    pub fn get_node_cloned(&self, idx: NodeIndex) -> Option<Node> {
        if let Some(entry) = self.overlay.get(&idx) {
            return Some(entry.value().clone());
        }
        self.baseline_snapshot.nodes.get(idx as usize).cloned()
    }

    /// Same as `get_node_cloned` but returns the node by reference from
    /// the snapshot if not in overlay. Caller must NOT mutate.
    // Prepared for `propagator_scenario.rs.skeleton` (unwired draft
    // module); `get_node_cloned` is what current callers use instead.
    #[allow(dead_code)]
    pub fn read_node<R>(&self, idx: NodeIndex, f: impl FnOnce(&Node) -> R) -> Option<R> {
        if let Some(entry) = self.overlay.get(&idx) {
            return Some(f(entry.value()));
        }
        self.baseline_snapshot.nodes.get(idx as usize).map(f)
    }

    // Same skeleton-module note as `read_node` above.
    #[allow(dead_code)]
    pub fn write_node(&self, idx: NodeIndex, node: Node) {
        self.overlay.insert(idx, node);
    }

    /// Number of nodes diff'd in the overlay.
    pub fn overlay_size(&self) -> usize {
        self.overlay.len()
    }

    /// Rough memory of the overlay (snapshot is shared, not counted here).
    pub fn overlay_memory_bytes(&self) -> usize {
        self.overlay.len() * std::mem::size_of::<Node>()
    }
}

/// Manages all scenarios in the process. Concurrent-safe.
pub struct ScenarioManager {
    /// scenario_id -> Scenario (Arc'd so handlers can hold references
    /// without blocking the manager).
    scenarios: DashMap<Uuid, Arc<Scenario>, RandomState>,
}

impl ScenarioManager {
    pub fn new() -> Self {
        Self {
            scenarios: DashMap::with_hasher(RandomState::new()),
        }
    }

    /// Fork the current baseline into a new scenario.
    ///
    /// Phase 2.1.a (F-026 audit closure): switched from
    /// `Arc<RwLock<Graph>>` deep-clone (~50ms, 76 MB per fork) to
    /// `ArcSwap<Graph>::load_full()` (refcount bump only, sub-µs).
    /// The scenario's `baseline_snapshot` is a refcounted Arc to the
    /// CURRENT baseline; when the baseline gets updated via ArcSwap,
    /// existing scenarios keep their historic snapshot consistent.
    /// New forks pick up the new baseline. That's by design — what-if
    /// must stay coherent with the state it was created from.
    pub fn fork_from_baseline(
        &self,
        name: String,
        baseline: &arc_swap::ArcSwap<Graph>,
    ) -> (Arc<Scenario>, ForkStats) {
        self.fork_from_baseline_with_ttl(name, baseline, 0)
    }

    pub fn fork_from_baseline_with_ttl(
        &self,
        name: String,
        baseline: &arc_swap::ArcSwap<Graph>,
        min_ttl_seconds: u64,
    ) -> (Arc<Scenario>, ForkStats) {
        let t0 = Instant::now();
        let snapshot: Arc<Graph> = baseline.load_full();
        let clone_ms = t0.elapsed().as_millis() as u64;

        let scenario = Arc::new(Scenario::with_options(
            name,
            None,
            snapshot,
            DashMap::with_hasher(RandomState::new()),
            min_ttl_seconds,
        ));
        let id = scenario.id;
        self.scenarios.insert(id, scenario.clone());

        let total_ms = t0.elapsed().as_millis() as u64;
        (scenario, ForkStats { clone_ms, total_ms })
    }

    /// P3.5 (agent-first MCTS): fork from an existing scenario, NOT
    /// the baseline. The new scenario inherits the parent's overlay
    /// (deep-copied entries) + same snapshot. Lets an agent branch
    /// its reasoning tree: explore alternative continuations from a
    /// promising state without re-applying all the prior events.
    pub fn fork_from_scenario(
        &self,
        name: String,
        parent_id: Uuid,
        min_ttl_seconds: u64,
    ) -> Option<(Arc<Scenario>, ForkStats)> {
        let t0 = Instant::now();
        let parent = self.scenarios.get(&parent_id)?.clone();
        parent.touch_accessed();

        // Snapshot Arc is shared (refcount bump only).
        let snapshot = parent.baseline_snapshot.clone();

        // Overlay must be deep-copied: child mutations must not leak
        // into the parent. DashMap iteration + insertion is O(N) but
        // typical overlays are small (a handful of dirty PIs) so this
        // is microsecond-scale in practice.
        let child_overlay: DashMap<NodeIndex, Node, RandomState> =
            DashMap::with_hasher(RandomState::new());
        for entry in parent.overlay.iter() {
            child_overlay.insert(*entry.key(), entry.value().clone());
        }

        let clone_ms = t0.elapsed().as_millis() as u64;
        let scenario = Arc::new(Scenario::with_options(
            name,
            Some(parent_id),
            snapshot,
            child_overlay,
            min_ttl_seconds,
        ));
        let id = scenario.id;
        self.scenarios.insert(id, scenario.clone());
        let total_ms = t0.elapsed().as_millis() as u64;
        Some((scenario, ForkStats { clone_ms, total_ms }))
    }

    pub fn get(&self, id: &Uuid) -> Option<Arc<Scenario>> {
        self.scenarios.get(id).map(|e| e.value().clone())
    }

    pub fn remove(&self, id: &Uuid) -> Option<Arc<Scenario>> {
        self.scenarios.remove(id).map(|(_, s)| s)
    }

    pub fn list(&self) -> Vec<Arc<Scenario>> {
        self.scenarios.iter().map(|e| e.value().clone()).collect()
    }

    pub fn len(&self) -> usize {
        self.scenarios.len()
    }

    /// P2.1.d + P3.4: scan + evict scenarios idle for longer than
    /// max(default_ttl, scenario.min_ttl_seconds). Returns evicted
    /// UUIDs. P3.4 lets agents pin a longer-than-default lifetime
    /// at fork time for batch sessions.
    pub fn evict_idle(&self, default_ttl_seconds: u64) -> Vec<Uuid> {
        let mut evicted = Vec::new();
        let to_evict: Vec<Uuid> = self
            .scenarios
            .iter()
            .filter_map(|e| {
                let s = e.value();
                let effective_ttl = default_ttl_seconds.max(s.min_ttl_seconds);
                if s.idle_seconds() >= effective_ttl {
                    Some(*e.key())
                } else {
                    None
                }
            })
            .collect();
        for id in to_evict {
            if self.scenarios.remove(&id).is_some() {
                evicted.push(id);
            }
        }
        evicted
    }
}

impl Default for ScenarioManager {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ForkStats {
    pub clone_ms: u64,
    pub total_ms: u64,
}

// -------------------------------------------------------------------- //
//  Graph::clone — required for `Arc::new((*g).clone())` above.
//  This implementation is auto-derived from the Clone derive on the
//  fields. We make it explicit here as documentation: it's NOT cheap.
// -------------------------------------------------------------------- //

// (Graph already derives Clone via the manual impl below. Confirmed by
// reviewing state.rs — all fields are Clone. We add the impl in
// state.rs alongside the struct definition.)
