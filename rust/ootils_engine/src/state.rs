//! state.rs — in-RAM graph model (ADR-017 §3.1).
//!
//! Everything the propagator needs to do its job, indexed for O(1) /
//! O(log n) access without ever touching Postgres in the hot path:
//!
//! - Nodes in a contiguous arena (`Vec<Node>`) addressable by `NodeIndex`
//!   (u32). Cache-friendly, no `Box` overhead, ~144 bytes per node.
//! - HashMaps keyed by node_id (UUID → NodeIndex), (item_id, location_id)
//!   → PI indices, projection_series_id → bucket indices.
//! - Edges stored as adjacency lists per target node, partitioned by
//!   edge type (replenishes / consumes / feeds_forward). For phase 2
//!   we keep `HashMap<NodeIndex, Vec<EdgeRef>>`; CSR migration is a
//!   phase-3 perf knob if profiling demands it.
//!
//! The Graph is held behind `Arc<ArcSwap<Graph>>` so:
//! - Readers clone an `Arc<Graph>` (cheap pointer copy) for the
//!   duration of a request — no lock contention on reads.
//! - Writers build a new Graph and atomically swap the pointer.
//! - Old generations get freed when their last reader drops them.
//!
//! This is the COW substrate that phase 4 (scenarios) builds on.

use ahash::RandomState;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;
use uuid::Uuid;

/// Stable handle into `Graph::nodes`. 32 bits cap us at 4 billion nodes
/// per scenario — comfortable for any imaginable supply chain.
pub type NodeIndex = u32;

/// Node category — distinguishes the various supply chain entities.
/// `repr(u8)` keeps the Node struct compact.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeType {
    Unknown = 0,
    ProjectedInventory = 1,
    OnHandSupply = 2,
    PurchaseOrderSupply = 3,
    WorkOrderSupply = 4,
    TransferSupply = 5,
    PlannedSupply = 6,
    ForecastDemand = 7,
    CustomerOrderDemand = 8,
    DependentDemand = 9,
    TransferDemand = 10,
}

impl NodeType {
    /// Decode the string form used in the DB into the enum. Falls back
    /// to `Unknown` for shapes we don't model in the engine yet (Resource,
    /// Ghost, etc.) — those nodes are loaded but ignored by the
    /// propagator.
    pub fn from_db(s: &str) -> Self {
        match s {
            "ProjectedInventory" => Self::ProjectedInventory,
            "OnHandSupply" => Self::OnHandSupply,
            "PurchaseOrderSupply" => Self::PurchaseOrderSupply,
            "WorkOrderSupply" => Self::WorkOrderSupply,
            "TransferSupply" => Self::TransferSupply,
            "PlannedSupply" => Self::PlannedSupply,
            "ForecastDemand" => Self::ForecastDemand,
            "CustomerOrderDemand" => Self::CustomerOrderDemand,
            "DependentDemand" => Self::DependentDemand,
            "TransferDemand" => Self::TransferDemand,
            _ => Self::Unknown,
        }
    }

    pub fn is_supply(self) -> bool {
        matches!(
            self,
            Self::OnHandSupply
                | Self::PurchaseOrderSupply
                | Self::WorkOrderSupply
                | Self::TransferSupply
                | Self::PlannedSupply
        )
    }

    pub fn is_demand(self) -> bool {
        matches!(
            self,
            Self::ForecastDemand
                | Self::CustomerOrderDemand
                | Self::DependentDemand
                | Self::TransferDemand
        )
    }
}

/// Compact, packed representation of one node in the graph.
///
/// Field order is chosen so the larger types (Decimal = 16 bytes,
/// Uuid = 16 bytes) come first — reduces alignment padding. Total
/// without padding is ~150 bytes; with default alignment ~160 bytes.
///
/// Mutable per-PI computed state lives here (opening/closing/etc.) —
/// the propagator updates these in place inside a scenario's overlay.
#[derive(Debug, Clone)]
pub struct Node {
    pub node_id: Uuid,
    pub item_id: Option<Uuid>,
    pub location_id: Option<Uuid>,
    /// Only set on PI buckets — the projection_series they belong to.
    pub series_id: Option<Uuid>,

    pub opening_stock: Decimal,
    pub inflows: Decimal,
    pub outflows: Decimal,
    pub closing_stock: Decimal,
    pub shortage_qty: Decimal,
    /// Used by supplies (PurchaseOrderSupply etc.) to carry the supplied
    /// quantity.  For PI nodes, this is just zero.
    pub quantity: Decimal,

    /// `time_span_start` / `_end` for PIs (and span-based forecasts);
    /// `time_ref` for point-in-time supplies/orders. We store both —
    /// memory is cheap, conditional logic on every kernel call isn't.
    pub time_span_start: Option<NaiveDate>,
    pub time_span_end: Option<NaiveDate>,
    pub time_ref: Option<NaiveDate>,

    pub bucket_sequence: i32,
    pub node_type: NodeType,
    /// Bit flags: 0x01=is_dirty, 0x02=has_shortage, 0x04=active.
    pub flags: u8,
}

impl Node {
    pub const FLAG_DIRTY: u8 = 0x01;
    pub const FLAG_SHORTAGE: u8 = 0x02;
    pub const FLAG_ACTIVE: u8 = 0x04;

    pub fn is_dirty(&self) -> bool {
        self.flags & Self::FLAG_DIRTY != 0
    }
    pub fn has_shortage(&self) -> bool {
        self.flags & Self::FLAG_SHORTAGE != 0
    }
    pub fn is_active(&self) -> bool {
        self.flags & Self::FLAG_ACTIVE != 0
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EdgeType {
    Unknown = 0,
    Replenishes = 1,
    Consumes = 2,
    FeedsForward = 3,
    PeggedTo = 4,
}

impl EdgeType {
    pub fn from_db(s: &str) -> Self {
        match s {
            "replenishes" => Self::Replenishes,
            "consumes" => Self::Consumes,
            "feeds_forward" => Self::FeedsForward,
            "pegged_to" => Self::PeggedTo,
            _ => Self::Unknown,
        }
    }
}

/// One incoming edge — `from` is the source node, `edge_type` the
/// semantic. The target is implicit (key of the `edges_in` map).
#[derive(Debug, Clone, Copy)]
pub struct EdgeRef {
    pub from: NodeIndex,
    pub edge_type: EdgeType,
}

/// The complete in-RAM graph state for ONE scenario (baseline initially;
/// phase 4 adds COW forks via `scenario::Scenario`). Holding it behind
/// `Arc<RwLock<Graph>>` (engine) or `Arc<Graph>` (scenario snapshot)
/// is the caller's choice.
///
/// `Clone` is implemented (auto-derived) to support taking a deep
/// snapshot at scenario fork time. The clone is ~76 MB on profile L
/// and ~100-200 ms — paid at fork time, not on the hot path.
#[derive(Clone)]
pub struct Graph {
    /// Arena of nodes — `NodeIndex` indexes directly into this Vec.
    pub nodes: Vec<Node>,

    /// UUID → arena index. Built once at bootstrap, kept in sync on
    /// mutation. Uses ahash (2-3× faster than std SipHash).
    pub by_node_id: HashMap<Uuid, NodeIndex, RandomState>,

    /// (item_id, location_id) → PI bucket indices for that pair.
    /// Lets the propagator answer "all PIs for this item/loc in window"
    /// without a Postgres roundtrip — the heart of incremental cascade.
    pub by_item_location: HashMap<(Uuid, Uuid), Vec<NodeIndex>, RandomState>,

    /// projection_series_id → bucket indices in that series.
    /// Used by the window function logic to walk a series in
    /// bucket_sequence order.
    pub by_series: HashMap<Uuid, Vec<NodeIndex>, RandomState>,

    /// Incoming edges per target node, indexed by NodeIndex.
    /// `Vec<EdgeRef>` because nodes can have multiple incoming edges
    /// of various types (replenishes from N supplies, consumes from M
    /// demands, etc.).
    pub edges_in: HashMap<NodeIndex, Vec<EdgeRef>, RandomState>,

    /// Bumped on every mutation. Used to invalidate downstream caches
    /// (e.g. open gRPC streams) and to order snapshots.
    pub generation: u64,
}

impl Graph {
    pub fn new() -> Self {
        let s = RandomState::new();
        Self {
            nodes: Vec::new(),
            by_node_id: HashMap::with_hasher(s.clone()),
            by_item_location: HashMap::with_hasher(s.clone()),
            by_series: HashMap::with_hasher(s.clone()),
            edges_in: HashMap::with_hasher(s),
            generation: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn get_node(&self, node_id: &Uuid) -> Option<&Node> {
        self.by_node_id
            .get(node_id)
            .and_then(|idx| self.nodes.get(*idx as usize))
    }

    /// Rough memory footprint in bytes. Counts the Vec/HashMap heap
    /// allocations but not the per-allocation overhead — close enough
    /// for observability.
    pub fn memory_bytes(&self) -> usize {
        use std::mem::size_of;
        size_of::<Self>()
            + self.nodes.capacity() * size_of::<Node>()
            + self.by_node_id.capacity() * (size_of::<Uuid>() + size_of::<NodeIndex>())
            + self
                .by_item_location
                .iter()
                .map(|(_, v)| v.capacity() * size_of::<NodeIndex>())
                .sum::<usize>()
            + self
                .by_series
                .iter()
                .map(|(_, v)| v.capacity() * size_of::<NodeIndex>())
                .sum::<usize>()
            + self
                .edges_in
                .iter()
                .map(|(_, v)| v.capacity() * size_of::<EdgeRef>())
                .sum::<usize>()
    }
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}
