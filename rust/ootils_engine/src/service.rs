//! service.rs — gRPC server implementation.
//!
//! Phase 2: the baseline `Graph` lives behind `Arc<ArcSwap<Graph>>` —
//! readers clone a snapshot Arc for the duration of their RPC, writers
//! atomically swap the pointer when they publish a new generation.
//! Lock-free for reads, no blocking on reads ever.
//!
//! `Health`, `Metrics`, `GetNode`, `ListScenarios` are implemented from
//! the in-RAM state. Mutating RPCs (Propagate, ForkScenario, etc.)
//! still return `Unimplemented` and reference the phase that will fill
//! them in.

use parking_lot::RwLock;
use prost_types::Timestamp;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};
use tracing::debug;
use uuid::Uuid;

use ootils_proto::engine::v1::{engine_server::Engine, health_status::Status as HealthEnum, *};

use crate::propagator;
use crate::state::{Graph, NodeType};

pub struct EngineSvc {
    boot_time: Instant,
    boot_timestamp: Timestamp,
    /// Baseline scenario state — phase 3 uses an RwLock for in-place
    /// mutation. Phase 4 will refactor to `ArcSwap<Graph>` + per-
    /// scenario COW overlays (matching ADR-017 §3.2). For now, all
    /// reads grab the read lock (concurrent), the propagator grabs
    /// the write lock briefly per call.
    baseline: Arc<RwLock<Graph>>,
}

impl EngineSvc {
    pub fn new(boot_time: Instant, baseline: Arc<RwLock<Graph>>) -> Self {
        let now = std::time::SystemTime::now();
        Self {
            boot_time,
            boot_timestamp: Timestamp::from(now),
            baseline,
        }
    }
}

fn date_to_iso(d: chrono::NaiveDate) -> String {
    d.format("%Y-%m-%d").to_string()
}

#[tonic::async_trait]
impl Engine for EngineSvc {
    type QueryShortagesStream =
        tokio_stream::wrappers::ReceiverStream<Result<Shortage, Status>>;
    type StreamChangesStream =
        tokio_stream::wrappers::ReceiverStream<Result<ChangeEvent, Status>>;

    async fn health(&self, _req: Request<()>) -> Result<Response<HealthStatus>, Status> {
        let uptime = self.boot_time.elapsed().as_secs() as i64;
        let g = self.baseline.read();
        let detail = format!(
            "phase 2: baseline loaded ({} nodes, gen {})",
            g.len(),
            g.generation
        );
        Ok(Response::new(HealthStatus {
            status: HealthEnum::Serving as i32,
            detail,
            boot_time: Some(self.boot_timestamp.clone()),
            uptime_seconds: uptime,
        }))
    }

    async fn metrics(&self, _req: Request<()>) -> Result<Response<EngineMetrics>, Status> {
        let g = self.baseline.read();
        Ok(Response::new(EngineMetrics {
            baseline_graph_bytes: g.memory_bytes() as i64,
            total_scenarios_bytes: 0,
            active_scenarios: 0,
            events_processed_total: 0,
            nodes_recomputed_total: 0,
            shortages_detected_total: 0,
            propagate_p50_us: 0.0,
            propagate_p95_us: 0.0,
            propagate_p99_us: 0.0,
            pg_writeback_queue_depth: 0,
            wal_size_bytes: 0,
            last_pg_flush: None,
        }))
    }

    async fn list_scenarios(
        &self,
        _req: Request<()>,
    ) -> Result<Response<ScenarioList>, Status> {
        // Phase 2 surfaces only the baseline. Forks land in phase 4.
        let g = self.baseline.read();
        Ok(Response::new(ScenarioList {
            scenarios: vec![ScenarioInfo {
                id: "00000000-0000-0000-0000-000000000001".into(),
                name: "baseline".into(),
                parent_id: String::new(),
                created_at: Some(self.boot_timestamp.clone()),
                overlay_size: 0,
                memory_bytes: g.memory_bytes() as i64,
            }],
        }))
    }

    async fn get_node(
        &self,
        req: Request<NodeQuery>,
    ) -> Result<Response<NodeState>, Status> {
        let q = req.into_inner();
        let node_id = Uuid::from_str(&q.node_id)
            .map_err(|e| Status::invalid_argument(format!("bad node_id: {e}")))?;

        let g = self.baseline.read();
        let node = g
            .get_node(&node_id)
            .ok_or_else(|| Status::not_found(format!("node {node_id} not found")))?;

        Ok(Response::new(NodeState {
            node_id: node.node_id.to_string(),
            node_type: format!("{:?}", node.node_type),
            item_id: node.item_id.map(|u| u.to_string()).unwrap_or_default(),
            location_id: node.location_id.map(|u| u.to_string()).unwrap_or_default(),
            opening_stock: node.opening_stock.to_string(),
            inflows: node.inflows.to_string(),
            outflows: node.outflows.to_string(),
            closing_stock: node.closing_stock.to_string(),
            has_shortage: node.has_shortage(),
            shortage_qty: node.shortage_qty.to_string(),
            time_span_start: node.time_span_start.map(date_to_iso).unwrap_or_default(),
            time_span_end: node.time_span_end.map(date_to_iso).unwrap_or_default(),
            bucket_sequence: node.bucket_sequence,
        }))
    }

    async fn propagate(
        &self,
        req: Request<PropagateRequest>,
    ) -> Result<Response<PropagateResponse>, Status> {
        let q = req.into_inner();
        debug!(event_id = %q.event_id, event_type = %q.event_type, "Propagate");

        // Phase 3 minimal contract: trigger_node_id identifies one PI
        // (or a node whose item/loc maps to PIs). We mark the
        // associated PI series dirty + propagate. Real event-type
        // dispatch lands in phase 6 alongside the Python client.
        let trigger_id = Uuid::from_str(&q.trigger_node_id)
            .map_err(|e| Status::invalid_argument(format!("bad trigger_node_id: {e}")))?;

        let mut dirty = HashSet::new();
        {
            let g = self.baseline.read();
            // Look up the trigger node, then enumerate PIs in the same
            // (item, location) couple — same dirty-cascade contract as
            // the Python/SQL/Rust-A engines.
            if let Some(node) = g.get_node(&trigger_id) {
                if let (Some(item), Some(loc)) = (node.item_id, node.location_id) {
                    if let Some(pis) = g.by_item_location.get(&(item, loc)) {
                        for &idx in pis {
                            let n = &g.nodes[idx as usize];
                            if n.node_type == NodeType::ProjectedInventory && n.is_active() {
                                dirty.insert(idx);
                            }
                        }
                    }
                }
            } else {
                return Err(Status::not_found(format!(
                    "trigger_node_id {trigger_id} not found"
                )));
            }
        }

        if dirty.is_empty() {
            // Nothing to propagate — return an empty result, not an error.
            return Ok(Response::new(PropagateResponse {
                calc_run_id: String::new(),
                nodes_processed: 0,
                nodes_changed: 0,
                shortages_detected: 0,
                timing: Some(EngineTiming {
                    dirty_expand_us: 0.0,
                    compute_us: 0.0,
                    shortage_detect_us: 0.0,
                    wal_fsync_us: 0.0,
                    total_us: 0.0,
                }),
            }));
        }

        let t_total = Instant::now();
        let stats = {
            let mut g = self.baseline.write();
            propagator::propagate(&mut g, &dirty)
        };
        let total_us = t_total.elapsed().as_micros() as f64;

        Ok(Response::new(PropagateResponse {
            // Phase 3 doesn't yet generate a calc_run row — leave empty.
            calc_run_id: String::new(),
            nodes_processed: stats.n_processed as i32,
            nodes_changed: stats.n_changed as i32,
            shortages_detected: stats.n_shortages as i32,
            timing: Some(EngineTiming {
                dirty_expand_us: 0.0,
                compute_us: stats.compute_us as f64,
                shortage_detect_us: 0.0,
                wal_fsync_us: 0.0,
                total_us,
            }),
        }))
    }

    async fn fork_scenario(
        &self,
        _req: Request<ForkRequest>,
    ) -> Result<Response<ScenarioInfo>, Status> {
        Err(Status::unimplemented(
            "fork_scenario is not implemented yet — see ADR-017 phase 4",
        ))
    }

    async fn merge_scenario(
        &self,
        _req: Request<MergeRequest>,
    ) -> Result<Response<MergeResult>, Status> {
        Err(Status::unimplemented(
            "merge_scenario is not implemented yet — see ADR-017 phase 4",
        ))
    }

    async fn query_shortages(
        &self,
        _req: Request<ShortagesQuery>,
    ) -> Result<Response<Self::QueryShortagesStream>, Status> {
        Err(Status::unimplemented(
            "query_shortages is not implemented yet — see ADR-017 phase 6",
        ))
    }

    async fn stream_changes(
        &self,
        _req: Request<StreamRequest>,
    ) -> Result<Response<Self::StreamChangesStream>, Status> {
        Err(Status::unimplemented(
            "stream_changes is not implemented yet — see ADR-017 phase 7",
        ))
    }
}
