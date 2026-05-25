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

use arc_swap::ArcSwap;
use prost_types::Timestamp;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};
use tracing::debug;
use uuid::Uuid;

use ootils_proto::engine::v1::{engine_server::Engine, health_status::Status as HealthEnum, *};

use crate::state::Graph;

pub struct EngineSvc {
    boot_time: Instant,
    boot_timestamp: Timestamp,
    /// Baseline scenario state — atomically swappable Arc.
    /// Cheap to clone (just bumps the refcount), so readers can hold a
    /// snapshot for the duration of their request without blocking
    /// writers.
    baseline: Arc<ArcSwap<Graph>>,
}

impl EngineSvc {
    pub fn new(boot_time: Instant, baseline: Arc<ArcSwap<Graph>>) -> Self {
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
        let g = self.baseline.load();
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
        let g = self.baseline.load();
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
        let g = self.baseline.load();
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

        let g = self.baseline.load();
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
        _req: Request<PropagateRequest>,
    ) -> Result<Response<PropagateResponse>, Status> {
        debug!("Propagate called (phase 2 stub — propagator lands in phase 3)");
        Err(Status::unimplemented(
            "propagate is not implemented yet — see ADR-017 phase 3",
        ))
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
