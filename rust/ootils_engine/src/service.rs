//! service.rs — gRPC server implementation.
//!
//! Phase 1 stub: `Health` returns SERVING with uptime, `Metrics` returns
//! zeros, everything else returns `Unimplemented`. Subsequent phases fill
//! in the real implementations following ADR-017.

use std::time::Instant;
use tonic::{Request, Response, Status};
use tracing::debug;

use ootils_proto::engine::v1::{
    engine_server::Engine,
    health_status::Status as HealthEnum,
    *,
};

pub struct EngineSvc {
    boot_time: Instant,
    boot_timestamp: prost_types::Timestamp,
}

impl EngineSvc {
    pub fn new(boot_time: Instant) -> Self {
        let now = std::time::SystemTime::now();
        Self {
            boot_time,
            boot_timestamp: prost_types::Timestamp::from(now),
        }
    }
}

#[tonic::async_trait]
impl Engine for EngineSvc {
    type QueryShortagesStream =
        tokio_stream::wrappers::ReceiverStream<Result<Shortage, Status>>;
    type StreamChangesStream =
        tokio_stream::wrappers::ReceiverStream<Result<ChangeEvent, Status>>;

    async fn health(
        &self,
        _req: Request<()>,
    ) -> Result<Response<HealthStatus>, Status> {
        let uptime = self.boot_time.elapsed().as_secs() as i64;
        Ok(Response::new(HealthStatus {
            status: HealthEnum::Serving as i32,
            detail: "phase 1 skeleton: gRPC up, in-RAM graph not loaded yet".into(),
            boot_time: Some(self.boot_timestamp.clone()),
            uptime_seconds: uptime,
        }))
    }

    async fn metrics(
        &self,
        _req: Request<()>,
    ) -> Result<Response<EngineMetrics>, Status> {
        // Phase 1: everything is zero. Real numbers land in later phases.
        Ok(Response::new(EngineMetrics::default()))
    }

    async fn propagate(
        &self,
        _req: Request<PropagateRequest>,
    ) -> Result<Response<PropagateResponse>, Status> {
        debug!("Propagate called (phase 1 stub)");
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

    async fn list_scenarios(
        &self,
        _req: Request<()>,
    ) -> Result<Response<ScenarioList>, Status> {
        Ok(Response::new(ScenarioList::default()))
    }

    async fn get_node(
        &self,
        _req: Request<NodeQuery>,
    ) -> Result<Response<NodeState>, Status> {
        Err(Status::unimplemented(
            "get_node is not implemented yet — see ADR-017 phase 2",
        ))
    }

    async fn query_shortages(
        &self,
        _req: Request<ShortagesQuery>,
    ) -> Result<Response<Self::QueryShortagesStream>, Status> {
        Err(Status::unimplemented(
            "query_shortages is not implemented yet — see ADR-017 phase 2",
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
