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
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};
use tracing::{debug, info};
use uuid::Uuid;

use ootils_proto::engine::v1::{engine_server::Engine, health_status::Status as HealthEnum, *};

use crate::metrics::Metrics;
use crate::propagator;
use crate::scenario::ScenarioManager;
use crate::state::{Graph, NodeType};
use crate::wal::make_record;
use crate::write_behind::{PendingDelta, WriteBehindQueue};

pub struct EngineSvc {
    boot_time: Instant,
    boot_timestamp: Timestamp,
    /// Baseline state.
    ///
    /// Phase 2.1.a (F-026 closure): `ArcSwap<Graph>` replaces the
    /// previous `RwLock<Graph>`. Reads take zero-cost `load_full()`
    /// snapshots; writes clone the Graph, mutate, then atomic-swap.
    /// Trade-off: baseline propagations are now ~clone-time slower
    /// (~50-100 ms vs ~ms in-place) BUT scenario forks become O(1)
    /// instead of O(N) — the multi-user what-if pattern that Phase 2
    /// targets has scenario propagations as the hot path, baseline
    /// updates are rare (Q3 design decision: max hourly).
    ///
    /// F-009 still holds: plan_compute reads via load() (no lock),
    /// apply mutates a CLONE of the current Arc<Graph> + swaps.
    /// The `propagation_lock` serializes baseline mutations among
    /// themselves so two concurrent baseline propagations can't both
    /// clone-mutate-swap and clobber.
    baseline: Arc<ArcSwap<Graph>>,
    /// COW scenarios on top of the baseline (phase 4).
    scenarios: Arc<ScenarioManager>,
    /// WAL + write-behind queue (phase 5). After propagation we append
    /// deltas to the WAL synchronously (fsync) and enqueue them for
    /// async Postgres flush.
    writeback: Arc<WriteBehindQueue>,
    /// Prometheus metrics registry (item #2).
    metrics: Arc<Metrics>,
    /// F-009 propagation serializer. Held across compute + apply so
    /// two concurrent propagations cannot both read the same state,
    /// compute deltas in parallel, and then both apply (the second
    /// would overwrite the first's deltas with stale values). The
    /// graph RwLock is released between compute and apply; this
    /// mutex re-establishes "one propagation at a time" without
    /// blocking concurrent READS.
    propagation_lock: Arc<parking_lot::Mutex<()>>,
}

impl EngineSvc {
    pub fn new(
        boot_time: Instant,
        baseline: Arc<ArcSwap<Graph>>,
        scenarios: Arc<ScenarioManager>,
        writeback: Arc<WriteBehindQueue>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let now = std::time::SystemTime::now();
        Self {
            boot_time,
            boot_timestamp: Timestamp::from(now),
            baseline,
            scenarios,
            writeback,
            metrics,
            propagation_lock: Arc::new(parking_lot::Mutex::new(())),
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
        // F-039: explicit cast — saturating semantics on the unlikely
        // overflow (uptime > 292 billion years).
        let uptime = i64::try_from(self.boot_time.elapsed().as_secs())
            .unwrap_or(i64::MAX);
        let g = self.baseline.load_full();
        // F-040 fix: user-facing detail — no internal "phase N"
        // nomenclature (ADR-017 implementation jargon).
        let detail = format!(
            "baseline loaded: {} nodes, generation {}",
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
        // F-041 fix: populate from the real registry instead of zeros.
        // The /metrics HTTP endpoint exposes the same data in
        // Prometheus exposition; this RPC is the typed counterpart for
        // programmatic callers.
        use std::sync::atomic::Ordering;
        let g = self.baseline.load_full();
        let baseline_bytes = g.memory_bytes() as i64;
        drop(g);
        let scenarios_bytes: i64 = self
            .scenarios
            .list()
            .iter()
            .map(|s| (s.baseline_snapshot.memory_bytes() + s.overlay_memory_bytes()) as i64)
            .sum();
        let events_total = self.metrics.events_total.load(Ordering::Relaxed) as i64;
        let nodes_processed = self.metrics.nodes_processed_total.load(Ordering::Relaxed) as i64;
        let shortages = self.metrics.shortages_detected_total.load(Ordering::Relaxed) as i64;
        // p50/p95/p99 require a histogram, which the hand-rolled
        // metrics registry doesn't keep (it accumulates sum-only).
        // Report mean as p50 — Prometheus consumers should use the
        // counter pair (compute_us_sum / events_total) for accuracy.
        // p95/p99 stay zero until a histogram lands (deferred — not
        // urgent enough to pull in prometheus-client).
        let mean_compute_us = if events_total > 0 {
            self.metrics.propagate_compute_us_sum.load(Ordering::Relaxed) as f64 / events_total as f64
        } else {
            0.0
        };
        let wal_size = self.metrics.wal_size_bytes.load(Ordering::Relaxed) as i64;
        let queue_depth =
            self.metrics.writeback_queue_depth.load(Ordering::Relaxed) as i32;

        Ok(Response::new(EngineMetrics {
            baseline_graph_bytes: baseline_bytes,
            total_scenarios_bytes: scenarios_bytes,
            active_scenarios: self.scenarios.len() as i32,
            events_processed_total: events_total,
            nodes_recomputed_total: nodes_processed,
            shortages_detected_total: shortages,
            propagate_p50_us: mean_compute_us,
            propagate_p95_us: 0.0,
            propagate_p99_us: 0.0,
            pg_writeback_queue_depth: queue_depth,
            wal_size_bytes: wal_size,
            last_pg_flush: None,
        }))
    }

    async fn list_scenarios(
        &self,
        _req: Request<()>,
    ) -> Result<Response<ScenarioList>, Status> {
        // Always surface the baseline + every active fork.
        let mut out: Vec<ScenarioInfo> = Vec::new();
        {
            let g = self.baseline.load_full();
            out.push(ScenarioInfo {
                id: "00000000-0000-0000-0000-000000000001".into(),
                name: "baseline".into(),
                parent_id: String::new(),
                created_at: Some(self.boot_timestamp.clone()),
                overlay_size: 0,
                memory_bytes: g.memory_bytes() as i64,
            });
        }
        for s in self.scenarios.list() {
            out.push(ScenarioInfo {
                id: s.id.to_string(),
                name: s.name.clone(),
                parent_id: s
                    .parent_id
                    .map(|u| u.to_string())
                    .unwrap_or_default(),
                created_at: Some(Timestamp::from(s.created_at_system)),
                overlay_size: s.overlay_size() as i32,
                memory_bytes: (s.baseline_snapshot.memory_bytes()
                    + s.overlay_memory_bytes()) as i64,
            });
        }
        Ok(Response::new(ScenarioList { scenarios: out }))
    }

    async fn fork_scenario(
        &self,
        req: Request<ForkRequest>,
    ) -> Result<Response<ScenarioInfo>, Status> {
        let q = req.into_inner();
        let name = if q.name.is_empty() {
            format!("fork-{}", &Uuid::new_v4().to_string()[..8])
        } else {
            q.name
        };
        // Phase 4: forks are always from baseline. Parent-scenario forks
        // (forking a fork) is a phase-5 elaboration.
        let (scenario, stats) = self.scenarios.fork_from_baseline(name.clone(), &self.baseline);
        self.metrics.record_fork();
        self.metrics
            .active_scenarios
            .store(self.scenarios.len() as i64, std::sync::atomic::Ordering::Relaxed);
        info!(
            scenario_id = %scenario.id,
            name = %scenario.name,
            clone_ms = stats.clone_ms,
            total_ms = stats.total_ms,
            "scenario forked from baseline"
        );
        Ok(Response::new(ScenarioInfo {
            id: scenario.id.to_string(),
            name: scenario.name.clone(),
            parent_id: scenario
                .parent_id
                .map(|u| u.to_string())
                .unwrap_or_default(),
            created_at: Some(Timestamp::from(scenario.created_at_system)),
            overlay_size: 0,
            memory_bytes: scenario.baseline_snapshot.memory_bytes() as i64,
        }))
    }

    async fn delete_scenario(
        &self,
        req: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResult>, Status> {
        // F-037/F-038: explicit scenario disposal (what-if discard).
        let q = req.into_inner();
        let sid = Uuid::from_str(&q.scenario_id)
            .map_err(|e| Status::invalid_argument(format!("bad scenario_id: {e}")))?;
        if sid == crate::loader::BASELINE_SCENARIO_ID {
            return Err(Status::invalid_argument(
                "cannot delete the baseline scenario",
            ));
        }
        let scenario = self
            .scenarios
            .remove(&sid)
            .ok_or_else(|| Status::not_found(format!("scenario {sid} not found")))?;
        let overlay_entries = scenario.overlay_size() as i32;
        self.metrics
            .active_scenarios
            .store(self.scenarios.len() as i64, std::sync::atomic::Ordering::Relaxed);
        info!(scenario_id = %sid, overlay_entries, "scenario deleted (F-038)");
        Ok(Response::new(DeleteResult {
            overlay_entries_freed: overlay_entries,
        }))
    }

    async fn merge_scenario(
        &self,
        req: Request<MergeRequest>,
    ) -> Result<Response<MergeResult>, Status> {
        let q = req.into_inner();
        let sid = Uuid::from_str(&q.scenario_id)
            .map_err(|e| Status::invalid_argument(format!("bad scenario_id: {e}")))?;

        let scenario = self
            .scenarios
            .get(&sid)
            .ok_or_else(|| Status::not_found(format!("scenario {sid} not found")))?;

        // Apply the overlay into the baseline. With ArcSwap (P2.1.a)
        // this is now a clone-on-write: take a snapshot, mutate the
        // snapshot, atomic-swap. The propagation_lock serializes with
        // concurrent baseline propagations so we don't lose a write.
        let n_merged: i64 = {
            let _guard = self.propagation_lock.lock();
            let current = self.baseline.load_full();
            let mut new_graph: Graph = (*current).clone();
            let mut n = 0i64;
            for entry in scenario.overlay.iter() {
                let idx = *entry.key();
                if let Some(slot) = new_graph.nodes.get_mut(idx as usize) {
                    *slot = entry.value().clone();
                    n += 1;
                }
            }
            new_graph.generation = new_graph.generation.wrapping_add(1);
            self.baseline.store(Arc::new(new_graph));
            n
        };

        // Drop the scenario from the manager — merged is consumed.
        self.scenarios.remove(&sid);
        self.metrics.record_merge();
        self.metrics
            .active_scenarios
            .store(self.scenarios.len() as i64, std::sync::atomic::Ordering::Relaxed);

        let new_gen = {
            let g = self.baseline.load_full();
            g.generation.to_string()
        };

        info!(
            scenario_id = %sid,
            nodes_merged = n_merged,
            new_baseline_gen = %new_gen,
            "scenario merged into baseline"
        );

        Ok(Response::new(MergeResult {
            nodes_merged: n_merged as i32,
            new_baseline_generation: new_gen,
        }))
    }

    async fn get_node(
        &self,
        req: Request<NodeQuery>,
    ) -> Result<Response<NodeState>, Status> {
        let q = req.into_inner();
        let node_id = Uuid::from_str(&q.node_id)
            .map_err(|e| Status::invalid_argument(format!("bad node_id: {e}")))?;

        let g = self.baseline.load_full();
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

        // P2.1.b (ADR-018 closure): determine target scenario.
        // - Empty string OR baseline UUID → baseline propagation
        //   (clone-on-write via ArcSwap, writes to WAL + PG).
        // - Any other UUID → per-scenario propagation (overlay
        //   write only, ephemeral, no WAL/PG).
        let target_scenario: Option<Arc<crate::scenario::Scenario>> = if q.scenario_id.is_empty() {
            None
        } else {
            let req_scenario = Uuid::from_str(&q.scenario_id)
                .map_err(|e| Status::invalid_argument(format!("bad scenario_id: {e}")))?;
            if req_scenario == crate::loader::BASELINE_SCENARIO_ID {
                None
            } else {
                let s = self.scenarios.get(&req_scenario).ok_or_else(|| {
                    Status::not_found(format!("scenario {req_scenario} not found"))
                })?;
                Some(s)
            }
        };

        // F-015: event_id must parse cleanly. Empty = caller asked us to
        // generate a calc_run_id; bad UUID = strict error (no silent
        // fallback to a fresh v4 which would break the audit chain).
        let cr_uuid = if q.event_id.is_empty() {
            Uuid::new_v4()
        } else {
            Uuid::parse_str(&q.event_id)
                .map_err(|e| Status::invalid_argument(format!("bad event_id: {e}")))?
        };

        // Phase 3 minimal contract: trigger_node_id identifies one PI
        // (or a node whose item/loc maps to PIs). We mark the
        // associated PI series dirty + propagate. Real event-type
        // dispatch lands in phase 6 alongside the Python client.
        let trigger_id = Uuid::from_str(&q.trigger_node_id)
            .map_err(|e| Status::invalid_argument(format!("bad trigger_node_id: {e}")))?;

        // Compute the dirty set: look up trigger_node_id, then
        // enumerate PIs in the same (item, location) couple. For
        // scenarios we read via the snapshot (item/location/series_id
        // are immutable in practice). The `active` and `node_type`
        // fields come from snapshot as well — those don't change in
        // overlay either.
        let mut dirty = HashSet::new();
        {
            let g = match &target_scenario {
                Some(s) => s.baseline_snapshot.clone(),
                None => self.baseline.load_full(),
            };
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
            // Nothing to propagate — return an empty result, not an
            // error. Reviewer B2 fix: surface the parsed cr_uuid in
            // calc_run_id even when there are no deltas, so the
            // caller's event_id → calc_run_id audit chain (F-015)
            // holds for no-op propagations too.
            return Ok(Response::new(PropagateResponse {
                calc_run_id: cr_uuid.to_string(),
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

        // Dispatch on baseline vs scenario propagation.
        // - Baseline: F-008/F-009 spawn_blocking + ArcSwap CoW + WAL/PG.
        // - Scenario (P2.1.b ADR-018): spawn_blocking for rayon compute,
        //   overlay write only — no WAL/PG. Per-scenario propagation
        //   lock so parallel propags on DIFFERENT scenarios don't
        //   serialize against each other.
        let metrics = self.metrics.clone();
        let blocking_outcome = if let Some(scenario) = target_scenario.clone() {
            // ---- Scenario propagation path ----
            tokio::task::spawn_blocking(
                move || -> Result<(propagator::PropagationStats, f64), Status> {
                    // P2.1.c: per-scenario lock. Two propagations on
                    // the same scenario serialize; propagations on
                    // different scenarios run in parallel.
                    let _scenario_guard = scenario.propagation_lock.lock();
                    scenario.touch_accessed();

                    let computed = propagator::plan_compute_scenario(&scenario, &dirty);
                    let stats = propagator::apply_scenario(&scenario, computed, &dirty);

                    // Scenarios don't write to WAL or PG in P2.1.b
                    // (they're ephemeral; P2.2 will persist them).
                    Ok((stats, 0.0))
                },
            )
            .await
        } else {
            // ---- Baseline propagation path ----
            let baseline = self.baseline.clone();
            let writeback = self.writeback.clone();
            let metrics_inner = self.metrics.clone();
            let prop_lock = self.propagation_lock.clone();
            tokio::task::spawn_blocking(
                move || -> Result<(propagator::PropagationStats, f64), Status> {
                    let _propagation_guard = prop_lock.lock();

                    let snapshot = baseline.load_full();
                    let computed = propagator::plan_compute(&snapshot, &dirty);

                    let mut new_graph: Graph = (*snapshot).clone();
                    drop(snapshot);
                    let stats = propagator::apply(&mut new_graph, computed, &dirty);
                    baseline.store(Arc::new(new_graph));

                    let mut wal_fsync_us = 0.0;
                    if !stats.deltas.is_empty() {
                        let t_wal = Instant::now();
                        let scenario_uuid = crate::loader::BASELINE_SCENARIO_ID;
                        let record = make_record(cr_uuid, scenario_uuid, stats.deltas.clone());
                        let assigned_seq = match writeback.wal().append(&record) {
                            Ok(s) => s,
                            Err(e) => {
                                metrics_inner.record_failure();
                                if let Some(full) = e.downcast_ref::<crate::wal::WalFull>() {
                                    return Err(Status::resource_exhausted(full.to_string()));
                                }
                                return Err(Status::internal(format!("WAL append failed: {e}")));
                            }
                        };
                        wal_fsync_us = t_wal.elapsed().as_micros() as f64;
                        metrics_inner
                            .wal_appends_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        let pending: Vec<PendingDelta> = stats
                            .deltas
                            .iter()
                            .cloned()
                            .map(|d| PendingDelta::from_delta(assigned_seq, d))
                            .collect();
                        if let Err(full) = writeback.try_push(pending) {
                            metrics_inner.record_failure();
                            return Err(Status::resource_exhausted(full.to_string()));
                        }
                    }

                    Ok((stats, wal_fsync_us))
                },
            )
            .await
        };

        // Translate spawn_blocking JoinError → INTERNAL (the closure
        // would panic for a hard bug). The closure's own Status
        // errors pass through.
        let (stats, wal_fsync_us) = match blocking_outcome {
            Ok(Ok(tup)) => tup,
            Ok(Err(status)) => return Err(status),
            Err(join_err) => {
                self.metrics.record_failure();
                return Err(Status::internal(format!(
                    "propagate worker task failed: {join_err}"
                )));
            }
        };

        let total_us = t_total.elapsed().as_micros() as f64;

        // Item #2: register the propagation in metrics.
        self.metrics.record_propagation(
            stats.n_processed as u64,
            stats.n_changed as u64,
            stats.n_shortages as u64,
            stats.compute_us,
            wal_fsync_us as u64,
        );

        Ok(Response::new(PropagateResponse {
            calc_run_id: cr_uuid.to_string(),
            nodes_processed: stats.n_processed as i32,
            nodes_changed: stats.n_changed as i32,
            shortages_detected: stats.n_shortages as i32,
            timing: Some(EngineTiming {
                dirty_expand_us: 0.0,
                compute_us: stats.compute_us as f64,
                shortage_detect_us: 0.0,
                wal_fsync_us,
                total_us,
            }),
        }))
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
