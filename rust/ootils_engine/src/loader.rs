//! loader.rs — bootstrap the in-RAM `Graph` from Postgres at startup.
//!
//! ADR-017 §3.1 + phase 2 deliverable.
//!
//! Strategy: 2 queries — one for nodes, one for edges. Both are pure
//! sequential scans on Postgres' side (filtered by scenario_id +
//! active=TRUE), streamed back through tokio-postgres' binary protocol.
//! Each row is decoded into a `Node` / `EdgeRef` and pushed into the
//! arena. Indexes are built in a second pass over the in-memory arena
//! (fast, cache-friendly, no Postgres involvement).
//!
//! For profile L (~230K nodes + ~460K edges), measured cold load < 5s
//! on LAN-direct Postgres (per the read-path benchmark in chantier A).

use crate::state::{EdgeRef, EdgeType, Graph, Node, NodeIndex, NodeType};
use ahash::RandomState;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::Instant;
use tokio_postgres::{Client, NoTls};
use tracing::{info, warn};
use uuid::Uuid;

/// Hardcoded baseline scenario UUID — same constant the rest of the
/// system uses. Phase 4 will replace this with per-scenario loaders.
pub const BASELINE_SCENARIO_ID: Uuid =
    Uuid::from_u128(0x00000000_0000_0000_0000_000000000001);

pub struct LoadStats {
    pub n_nodes: usize,
    pub n_pi: usize,
    pub n_supplies: usize,
    pub n_demands: usize,
    /// F-025: nodes the engine doesn't model (e.g. Resource, Ghost,
    /// future types). They're loaded into the graph but ignored by
    /// the propagator. Surfaced here + as a Prometheus counter so a
    /// new node type doesn't silently miscompute totals.
    pub n_unknown: usize,
    pub n_edges: usize,
    pub elapsed_ms: u64,
    pub memory_bytes: usize,
}

/// Open one connection, load the baseline graph in full, return it.
pub async fn load_baseline(dsn: &str) -> anyhow::Result<(Graph, LoadStats)> {
    let t0 = Instant::now();

    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            warn!(error = %e, "postgres connection task ended");
        }
    });

    let graph = build_graph(&client).await?;
    let elapsed_ms = t0.elapsed().as_millis() as u64;

    let n_pi = graph
        .nodes
        .iter()
        .filter(|n| n.node_type == NodeType::ProjectedInventory)
        .count();
    let n_supplies = graph.nodes.iter().filter(|n| n.node_type.is_supply()).count();
    let n_demands = graph.nodes.iter().filter(|n| n.node_type.is_demand()).count();
    let n_unknown = graph
        .nodes
        .iter()
        .filter(|n| n.node_type == NodeType::Unknown)
        .count();
    let n_edges: usize = graph.edges_in.values().map(|v| v.len()).sum();
    let memory_bytes = graph.memory_bytes();
    let n_nodes = graph.len();

    info!(
        n_nodes,
        n_pi,
        n_supplies,
        n_demands,
        n_unknown,
        n_edges,
        elapsed_ms,
        memory_mb = memory_bytes / 1_048_576,
        "baseline graph loaded into RAM"
    );

    // F-011: a healthy baseline must have PIs. If the engine boots
    // pointed at the wrong database (e.g. fresh DB without seed data)
    // we'd report "SERVING" with zero PIs and every Propagate would
    // return NOT_FOUND — the worst operational signal. Fail fast so
    // the operator notices at deploy time. The `--allow-empty-baseline`
    // flag (see main.rs) escapes this for CI / test scenarios.
    if n_nodes == 0 || n_pi == 0 {
        anyhow::bail!(
            "baseline appears empty: {} nodes, {} PIs — refusing to boot. \
             Check DATABASE_URL points at the right database, or use \
             --allow-empty-baseline for tests.",
            n_nodes,
            n_pi
        );
    }

    // F-025: warn loudly when a non-trivial fraction of nodes have a
    // node_type the engine doesn't model. They're loaded but ignored
    // by the propagator — silently dropping them would make the engine
    // diverge from SQL/Python results without explanation.
    if n_unknown > 0 {
        let fraction = n_unknown as f64 / n_nodes as f64;
        if fraction > 0.01 {
            warn!(
                n_unknown,
                n_nodes,
                fraction_pct = (fraction * 100.0) as u32,
                "more than 1% of loaded nodes have an unknown node_type — \
                 they will be ignored by the propagator. Check for a schema \
                 upgrade the engine doesn't know about yet."
            );
        } else {
            info!(n_unknown, "loaded nodes with unknown node_type (ignored by propagator)");
        }
    }

    Ok((
        graph,
        LoadStats {
            n_nodes,
            n_pi,
            n_supplies,
            n_demands,
            n_unknown,
            n_edges,
            elapsed_ms,
            memory_bytes,
        },
    ))
}

/// The actual two-query loader. Separated from `load_baseline` to
/// keep that one focused on connection management + observability.
async fn build_graph(client: &Client) -> anyhow::Result<Graph> {
    // -------- nodes --------
    let t_nodes_start = Instant::now();
    let node_rows = client
        .query(
            "SELECT \
                node_id, node_type, item_id, location_id, \
                projection_series_id, \
                COALESCE(opening_stock, 0)::numeric, \
                COALESCE(inflows, 0)::numeric, \
                COALESCE(outflows, 0)::numeric, \
                COALESCE(closing_stock, 0)::numeric, \
                COALESCE(shortage_qty, 0)::numeric, \
                COALESCE(quantity, 0)::numeric, \
                time_span_start, time_span_end, time_ref, \
                COALESCE(bucket_sequence, -1)::int, \
                COALESCE(is_dirty, FALSE), \
                COALESCE(has_shortage, FALSE), \
                active \
             FROM nodes \
             WHERE scenario_id = $1 AND active = TRUE",
            &[&BASELINE_SCENARIO_ID],
        )
        .await?;
    let nodes_loaded_in = t_nodes_start.elapsed().as_millis();

    let n = node_rows.len();
    let s = RandomState::new();
    let mut nodes: Vec<Node> = Vec::with_capacity(n);
    let mut by_node_id: HashMap<Uuid, NodeIndex, RandomState> =
        HashMap::with_capacity_and_hasher(n, s.clone());
    let mut by_item_location: HashMap<(Uuid, Uuid), Vec<NodeIndex>, RandomState> =
        HashMap::with_capacity_and_hasher(n / 4, s.clone());
    let mut by_series: HashMap<Uuid, Vec<NodeIndex>, RandomState> =
        HashMap::with_capacity_and_hasher(n / 90, s.clone()); // ~90 PIs per series

    for row in &node_rows {
        let node_id: Uuid = row.get(0);
        let node_type_str: &str = row.get(1);
        let item_id: Option<Uuid> = row.get(2);
        let location_id: Option<Uuid> = row.get(3);
        let series_id: Option<Uuid> = row.get(4);
        let opening_stock: Decimal = row.get(5);
        let inflows: Decimal = row.get(6);
        let outflows: Decimal = row.get(7);
        let closing_stock: Decimal = row.get(8);
        let shortage_qty: Decimal = row.get(9);
        let quantity: Decimal = row.get(10);
        let time_span_start: Option<NaiveDate> = row.get(11);
        let time_span_end: Option<NaiveDate> = row.get(12);
        let time_ref: Option<NaiveDate> = row.get(13);
        let bucket_sequence: i32 = row.get(14);
        let is_dirty: bool = row.get(15);
        let has_shortage: bool = row.get(16);
        let active: bool = row.get(17);

        let node_type = NodeType::from_db(node_type_str);

        let mut flags = 0u8;
        if is_dirty {
            flags |= Node::FLAG_DIRTY;
        }
        if has_shortage {
            flags |= Node::FLAG_SHORTAGE;
        }
        if active {
            flags |= Node::FLAG_ACTIVE;
        }

        let node = Node {
            node_id,
            item_id,
            location_id,
            series_id,
            opening_stock,
            inflows,
            outflows,
            closing_stock,
            shortage_qty,
            quantity,
            time_span_start,
            time_span_end,
            time_ref,
            bucket_sequence,
            node_type,
            flags,
        };

        let idx = nodes.len() as NodeIndex;
        nodes.push(node);
        by_node_id.insert(node_id, idx);

        if node_type == NodeType::ProjectedInventory {
            if let (Some(item), Some(loc)) = (item_id, location_id) {
                by_item_location.entry((item, loc)).or_default().push(idx);
            }
            if let Some(sid) = series_id {
                by_series.entry(sid).or_default().push(idx);
            }
        }
    }

    // F-022: pre-sort each series' bucket list by `bucket_sequence` so
    // the propagator can binary-search for the seed's previous bucket
    // (`seed_seq - 1`) in O(log N) instead of linearly scanning the
    // ~90 buckets per series per dirty event. Cheap one-time cost.
    for buckets in by_series.values_mut() {
        buckets.sort_unstable_by_key(|&idx| nodes[idx as usize].bucket_sequence);
    }

    info!(
        nodes = n,
        sql_ms = nodes_loaded_in,
        "nodes loaded + indexed"
    );

    // -------- edges --------
    let t_edges_start = Instant::now();
    let edge_rows = client
        .query(
            "SELECT from_node_id, to_node_id, edge_type \
             FROM edges \
             WHERE scenario_id = $1 AND active = TRUE",
            &[&BASELINE_SCENARIO_ID],
        )
        .await?;
    let edges_loaded_in = t_edges_start.elapsed().as_millis();

    let mut edges_in: HashMap<NodeIndex, Vec<EdgeRef>, RandomState> =
        HashMap::with_capacity_and_hasher(edge_rows.len() / 2, s);

    let mut n_skipped = 0usize;
    for row in &edge_rows {
        let from: Uuid = row.get(0);
        let to: Uuid = row.get(1);
        let edge_type_str: &str = row.get(2);

        let from_idx = match by_node_id.get(&from) {
            Some(i) => *i,
            None => {
                n_skipped += 1;
                continue;
            }
        };
        let to_idx = match by_node_id.get(&to) {
            Some(i) => *i,
            None => {
                n_skipped += 1;
                continue;
            }
        };

        let edge_type = EdgeType::from_db(edge_type_str);
        edges_in
            .entry(to_idx)
            .or_default()
            .push(EdgeRef { from: from_idx, edge_type });
    }
    if n_skipped > 0 {
        warn!(
            skipped = n_skipped,
            "skipped edges referencing nodes not in the active baseline set"
        );
    }
    info!(
        edges = edge_rows.len(),
        sql_ms = edges_loaded_in,
        skipped = n_skipped,
        "edges loaded + indexed"
    );

    Ok(Graph {
        nodes,
        by_node_id,
        by_item_location,
        by_series,
        edges_in,
        generation: 1,
    })
}
