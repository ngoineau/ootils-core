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
use parking_lot::RwLock;
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
    pub created_at_instant: Instant,
    pub created_at_system: SystemTime,
}

impl Scenario {
    pub fn new(name: String, parent_id: Option<Uuid>, baseline: Arc<Graph>) -> Self {
        Self {
            id: Uuid::new_v4(),
            name,
            parent_id,
            baseline_snapshot: baseline,
            overlay: DashMap::with_hasher(RandomState::new()),
            created_at_instant: Instant::now(),
            created_at_system: SystemTime::now(),
        }
    }

    /// Look up a node, preferring the overlay. Returns an owned `Node`
    /// (clone). Cheap — Node is ~150 bytes.
    pub fn get_node_cloned(&self, idx: NodeIndex) -> Option<Node> {
        if let Some(entry) = self.overlay.get(&idx) {
            return Some(entry.value().clone());
        }
        self.baseline_snapshot
            .nodes
            .get(idx as usize)
            .cloned()
    }

    /// Same as `get_node_cloned` but returns the node by reference from
    /// the snapshot if not in overlay. Caller must NOT mutate.
    pub fn read_node<R>(&self, idx: NodeIndex, f: impl FnOnce(&Node) -> R) -> Option<R> {
        if let Some(entry) = self.overlay.get(&idx) {
            return Some(f(entry.value()));
        }
        self.baseline_snapshot
            .nodes
            .get(idx as usize)
            .map(f)
    }

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
    /// The clone of the Graph is the dominant cost (~100-200 ms on L).
    /// Cheaper paths are possible with persistent data structures; see
    /// the module-level doc and ADR-017 phase-5 notes.
    pub fn fork_from_baseline(
        &self,
        name: String,
        baseline_lock: &RwLock<Graph>,
    ) -> (Arc<Scenario>, ForkStats) {
        let t0 = Instant::now();
        // Clone the baseline Graph under the read lock — this is the
        // expensive bit. We MUST release the lock before allocating
        // anything else lest we serialize all forks behind it.
        let snapshot: Arc<Graph> = {
            let g = baseline_lock.read();
            Arc::new((*g).clone())
        };
        let clone_ms = t0.elapsed().as_millis() as u64;

        let scenario = Arc::new(Scenario::new(name, None, snapshot));
        let id = scenario.id;
        self.scenarios.insert(id, scenario.clone());

        let total_ms = t0.elapsed().as_millis() as u64;
        (
            scenario,
            ForkStats {
                clone_ms,
                total_ms,
            },
        )
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
