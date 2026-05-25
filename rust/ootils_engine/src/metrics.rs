//! metrics.rs — Prometheus text-format `/metrics` endpoint (item #2).
//!
//! Lightweight on purpose: no `prometheus-client` crate dep (would
//! pull in ~30 more crates), just hand-rolled atomic counters +
//! a small hyper HTTP server.
//!
//! All counters are global statics — cheap to read from anywhere in
//! the engine. The engine code calls `record_propagation` after each
//! Propagate; the metrics task formats them on every scrape.

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

// ---------------------------------------------------------------------- //
//  Counter registry — atomics, lock-free reads.
// ---------------------------------------------------------------------- //

pub struct Metrics {
    pub events_total: AtomicU64,
    pub events_failures: AtomicU64,
    pub nodes_processed_total: AtomicU64,
    pub nodes_changed_total: AtomicU64,
    pub shortages_detected_total: AtomicU64,
    pub forks_total: AtomicU64,
    pub merges_total: AtomicU64,
    pub wal_appends_total: AtomicU64,
    pub wal_fsync_us_sum: AtomicU64,
    pub propagate_compute_us_sum: AtomicU64,
    pub pg_flush_success_total: AtomicU64,
    pub pg_flush_failure_total: AtomicU64,
    /// Last sampled write-behind queue depth (gauge).
    pub writeback_queue_depth: AtomicI64,
    /// Last sampled active scenario count (gauge).
    pub active_scenarios: AtomicI64,
    /// Configured queue depth cap (F-005). Set once at boot.
    pub queue_max_depth: AtomicI64,
    /// Last sampled WAL file size in bytes (gauge, updated by the
    /// flusher loop every flush_interval_ms).
    pub wal_size_bytes: AtomicU64,
    /// Configured WAL size cap in bytes (F-005). Set once at boot.
    pub wal_max_bytes: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            events_total: AtomicU64::new(0),
            events_failures: AtomicU64::new(0),
            nodes_processed_total: AtomicU64::new(0),
            nodes_changed_total: AtomicU64::new(0),
            shortages_detected_total: AtomicU64::new(0),
            forks_total: AtomicU64::new(0),
            merges_total: AtomicU64::new(0),
            wal_appends_total: AtomicU64::new(0),
            wal_fsync_us_sum: AtomicU64::new(0),
            propagate_compute_us_sum: AtomicU64::new(0),
            pg_flush_success_total: AtomicU64::new(0),
            pg_flush_failure_total: AtomicU64::new(0),
            writeback_queue_depth: AtomicI64::new(0),
            active_scenarios: AtomicI64::new(0),
            queue_max_depth: AtomicI64::new(0),
            wal_size_bytes: AtomicU64::new(0),
            wal_max_bytes: AtomicU64::new(0),
        }
    }

    /// Update counters after a successful propagation.
    pub fn record_propagation(
        &self,
        nodes_processed: u64,
        nodes_changed: u64,
        shortages_detected: u64,
        compute_us: u64,
        wal_fsync_us: u64,
    ) {
        self.events_total.fetch_add(1, Ordering::Relaxed);
        self.nodes_processed_total
            .fetch_add(nodes_processed, Ordering::Relaxed);
        self.nodes_changed_total
            .fetch_add(nodes_changed, Ordering::Relaxed);
        self.shortages_detected_total
            .fetch_add(shortages_detected, Ordering::Relaxed);
        self.propagate_compute_us_sum
            .fetch_add(compute_us, Ordering::Relaxed);
        self.wal_fsync_us_sum
            .fetch_add(wal_fsync_us, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.events_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_fork(&self) {
        self.forks_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_merge(&self) {
        self.merges_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pg_flush_success(&self) {
        self.pg_flush_success_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pg_flush_failure(&self) {
        self.pg_flush_failure_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Render the registry as Prometheus exposition text format.
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(2048);
        macro_rules! counter {
            ($name:expr, $help:expr, $val:expr) => {{
                out.push_str("# HELP ");
                out.push_str($name);
                out.push(' ');
                out.push_str($help);
                out.push('\n');
                out.push_str("# TYPE ");
                out.push_str($name);
                out.push_str(" counter\n");
                out.push_str($name);
                out.push(' ');
                out.push_str(&$val.to_string());
                out.push('\n');
            }};
        }
        macro_rules! gauge {
            ($name:expr, $help:expr, $val:expr) => {{
                out.push_str("# HELP ");
                out.push_str($name);
                out.push(' ');
                out.push_str($help);
                out.push('\n');
                out.push_str("# TYPE ");
                out.push_str($name);
                out.push_str(" gauge\n");
                out.push_str($name);
                out.push(' ');
                out.push_str(&$val.to_string());
                out.push('\n');
            }};
        }

        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let load_i = |a: &AtomicI64| a.load(Ordering::Relaxed);

        counter!(
            "ootils_engine_events_total",
            "Total Propagate events successfully processed",
            load(&self.events_total)
        );
        counter!(
            "ootils_engine_events_failures_total",
            "Total Propagate events that failed",
            load(&self.events_failures)
        );
        counter!(
            "ootils_engine_nodes_processed_total",
            "Cumulative PI nodes processed across all propagations",
            load(&self.nodes_processed_total)
        );
        counter!(
            "ootils_engine_nodes_changed_total",
            "Cumulative PI nodes whose values actually changed",
            load(&self.nodes_changed_total)
        );
        counter!(
            "ootils_engine_shortages_detected_total",
            "Cumulative shortages detected",
            load(&self.shortages_detected_total)
        );
        counter!(
            "ootils_engine_forks_total",
            "Total scenario fork operations",
            load(&self.forks_total)
        );
        counter!(
            "ootils_engine_merges_total",
            "Total scenario merge operations",
            load(&self.merges_total)
        );
        counter!(
            "ootils_engine_propagate_compute_us_sum_total",
            "Sum of compute microseconds across all Propagate calls",
            load(&self.propagate_compute_us_sum)
        );
        counter!(
            "ootils_engine_wal_fsync_us_sum_total",
            "Sum of WAL fsync microseconds across all appends",
            load(&self.wal_fsync_us_sum)
        );
        counter!(
            "ootils_engine_pg_flush_success_total",
            "Successful background flushes to Postgres",
            load(&self.pg_flush_success_total)
        );
        counter!(
            "ootils_engine_pg_flush_failure_total",
            "Failed background flushes to Postgres (will retry with backoff)",
            load(&self.pg_flush_failure_total)
        );
        gauge!(
            "ootils_engine_writeback_queue_depth",
            "Current depth of the write-behind queue",
            load_i(&self.writeback_queue_depth)
        );
        gauge!(
            "ootils_engine_active_scenarios",
            "Number of active forked scenarios (excludes baseline)",
            load_i(&self.active_scenarios)
        );
        gauge!(
            "ootils_engine_queue_max_depth",
            "Configured cap on write-behind queue depth (F-005)",
            load_i(&self.queue_max_depth)
        );
        gauge!(
            "ootils_engine_wal_size_bytes",
            "Current WAL file size in bytes (sampled every flush interval)",
            load(&self.wal_size_bytes)
        );
        gauge!(
            "ootils_engine_wal_max_bytes",
            "Configured cap on WAL file size in bytes (F-005)",
            load(&self.wal_max_bytes)
        );

        out
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------- //
//  HTTP server — minimal hyper-based /metrics handler.
// ---------------------------------------------------------------------- //

/// Spawn the Prometheus HTTP server on `addr`. Returns the JoinHandle.
pub fn spawn_metrics_server(
    metrics: Arc<Metrics>,
    addr: SocketAddr,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                error!(error = %e, %addr, "failed to bind metrics endpoint");
                return;
            }
        };
        info!(%addr, "Prometheus /metrics endpoint listening");

        loop {
            let (stream, _peer) = match listener.accept().await {
                Ok(x) => x,
                Err(e) => {
                    warn!(error = %e, "metrics accept failed");
                    continue;
                }
            };
            let io = TokioIo::new(stream);
            let metrics = metrics.clone();
            tokio::spawn(async move {
                let svc = service_fn(move |req: Request<Incoming>| {
                    let metrics = metrics.clone();
                    async move { handle(req, metrics).await }
                });
                if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                    warn!(error = %e, "metrics connection error");
                }
            });
        }
    })
}

async fn handle(
    req: Request<Incoming>,
    metrics: Arc<Metrics>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path();
    let body = match path {
        "/metrics" => metrics.render(),
        "/health" | "/healthz" => "ok\n".to_string(),
        _ => {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from("not found\n")))
                .unwrap());
        }
    };
    Ok(Response::builder()
        .header("content-type", "text/plain; version=0.0.4")
        .body(Full::new(Bytes::from(body)))
        .unwrap())
}
