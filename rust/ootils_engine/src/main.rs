//! ootils-engine — standalone Rust service (ADR-017 Architecture B).
//!
//! Phase 3 milestone:
//! - In-RAM Graph (phase 2).
//! - Native propagator on top of the graph (this phase).
//! - `Propagate` RPC implemented.
//! - One-shot CLI mode: `--bench` runs a full propagation on the loaded
//!   baseline and exits, printing timing — used to validate the
//!   ADR-017 phase-3 gate (compute < 100ms on profile L).

use clap::Parser;
use mimalloc::MiMalloc;
use parking_lot::RwLock;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tonic::transport::Server;
use tracing::{info, warn};

mod kernel;
mod loader;
mod metrics;
mod propagator;
mod scenario;
mod service;
mod state;
mod wal;
mod write_behind;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "ootils-engine")]
#[command(version, about = "ootils-core Rust engine service (ADR-017)")]
struct Cli {
    #[arg(long, env = "DATABASE_URL")]
    dsn: String,

    #[arg(long, env = "OOTILS_ENGINE_LISTEN", default_value = "127.0.0.1:50051")]
    listen: SocketAddr,

    #[arg(long, env = "RUST_LOG", default_value = "info,ootils_engine=debug")]
    log: String,

    /// Run a one-shot full propagation on the loaded baseline + exit.
    /// Used by the phase-3 perf gate.
    #[arg(long, default_value = "false")]
    bench: bool,

    /// Run a fork-stress bench: fork N scenarios from baseline, then
    /// drop them. Used by the phase-4 perf gate (Fork < 50ms target).
    #[arg(long)]
    bench_fork: Option<usize>,

    /// Path to the local WAL file (phase 5). Default: `./ootils-engine.wal`
    /// relative to CWD. Use an absolute path in production.
    #[arg(long, env = "OOTILS_WAL_PATH", default_value = "./ootils-engine.wal")]
    wal_path: std::path::PathBuf,

    /// How often the write-behind flusher drains the queue to Postgres.
    /// Lower = lower Postgres-lag, higher CPU cost. 100ms is the
    /// ADR-017 default.
    #[arg(long, env = "OOTILS_FLUSH_INTERVAL_MS", default_value = "100")]
    flush_interval_ms: u64,

    /// Prometheus /metrics endpoint listen address. Set "off" (or
    /// empty) to disable the metrics server. ANY other value must
    /// parse as `host:port` or the engine refuses to boot — silent
    /// fall-through on a misconfigured address would leave operators
    /// flying blind during canary rollout (F-007).
    #[arg(long, env = "OOTILS_METRICS_LISTEN", default_value = "127.0.0.1:9090")]
    metrics_listen: String,

    /// Hard cap on the WAL file size. Above this, Propagate returns
    /// RESOURCE_EXHAUSTED instead of growing the WAL unboundedly
    /// during a sustained PG outage. Default: 1 GB.
    #[arg(long, env = "OOTILS_WAL_MAX_BYTES", default_value_t = wal::DEFAULT_WAL_MAX_BYTES)]
    wal_max_bytes: u64,

    /// Hard cap on the write-behind queue depth (number of pending
    /// deltas in RAM). Above this, Propagate returns
    /// RESOURCE_EXHAUSTED. Default: 1,000,000.
    #[arg(long, env = "OOTILS_QUEUE_MAX_DEPTH", default_value_t = 1_000_000)]
    queue_max_depth: usize,

    /// Per-call gRPC timeout (milliseconds). A slow client or stuck
    /// handler is cancelled past this deadline rather than holding
    /// resources indefinitely (F-018). Default: 30 s.
    #[arg(long, env = "OOTILS_REQUEST_TIMEOUT_MS", default_value_t = 30_000)]
    request_timeout_ms: u64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log);
    let boot_time = Instant::now();

    info!(
        version = env!("CARGO_PKG_VERSION"),
        listen = %cli.listen,
        bench = cli.bench,
        "ootils-engine starting"
    );

    verify_postgres(&cli.dsn).await?;

    let (graph, load_stats) = loader::load_baseline(&cli.dsn).await?;
    let graph_lock = Arc::new(RwLock::new(graph));
    info!(
        nodes = load_stats.n_nodes,
        edges = load_stats.n_edges,
        memory_mb = load_stats.memory_bytes / 1_048_576,
        boot_load_ms = load_stats.elapsed_ms,
        "baseline ready in RAM"
    );

    if cli.bench {
        run_bench(&graph_lock);
        return Ok(());
    }

    if let Some(n) = cli.bench_fork {
        run_bench_fork(&graph_lock, n);
        return Ok(());
    }

    // ---- WAL + write-behind durability (phase 5, v2 hardened) ----
    // F-005: WAL is opened with explicit size caps. Default rotation
    // threshold (256 MB) + default max bytes (1 GB) means the engine
    // gracefully rejects new propagations during a sustained PG
    // outage rather than filling the volume.
    let wal = Arc::new(wal::WalWriter::open_with_caps(
        &cli.wal_path,
        wal::DEFAULT_ROTATION_THRESHOLD_BYTES,
        cli.wal_max_bytes,
    )?);
    info!(
        wal_max_bytes = cli.wal_max_bytes,
        queue_max_depth = cli.queue_max_depth,
        "WAL + queue caps configured (F-005)"
    );
    // Replay any records left over from a prior crash. WAL v2's
    // `replay()` already skips records with seq <= applied_pg_seq
    // (the durably-flushed-to-PG marker), so what we get back is
    // exactly the set of records that are durable on disk but may NOT
    // be in Postgres yet. We:
    //   (a) apply them to RAM (so the in-RAM graph reflects post-crash
    //       state)
    //   (b) re-enqueue them for the bg flusher with their original seq
    //   (c) let the seq-guarded PG UPDATE either insert them or skip
    //       them if PG already has equal/newer data (F-014).
    let recovered = wal.replay()?;
    if !recovered.is_empty() {
        let n_recs = recovered.len();
        let n_deltas: usize = recovered.iter().map(|r| r.record.deltas.len()).sum();
        let min_seq = recovered.first().map(|r| r.seq).unwrap_or(0);
        let max_seq = recovered.last().map(|r| r.seq).unwrap_or(0);
        warn!(
            n_records = n_recs,
            n_deltas,
            applied_pg_seq = wal.applied_pg_seq(),
            seq_range_min = min_seq,
            seq_range_max = max_seq,
            wal = %cli.wal_path.display(),
            "recovering from non-empty WAL — replay starting (records with seq > applied_pg_seq)"
        );
        let mut g = graph_lock.write();
        let by_id = &g.by_node_id;
        let mut idx_pairs: Vec<(usize, wal::NodeDelta)> = Vec::with_capacity(n_deltas);
        for sr in &recovered {
            for d in &sr.record.deltas {
                if let Some(&idx) = by_id.get(&d.node_id) {
                    idx_pairs.push((idx as usize, d.clone()));
                }
            }
        }
        for (idx, d) in idx_pairs {
            let n = &mut g.nodes[idx];
            n.opening_stock = d.opening_stock;
            n.inflows = d.inflows;
            n.outflows = d.outflows;
            n.closing_stock = d.closing_stock;
            n.shortage_qty = d.shortage_qty;
            if d.has_shortage {
                n.flags |= state::Node::FLAG_SHORTAGE;
            } else {
                n.flags &= !state::Node::FLAG_SHORTAGE;
            }
        }
        info!(n_records = n_recs, n_deltas, "WAL replay applied to RAM");
    }

    // Item #2: Prometheus metrics — process-wide counter registry.
    // F-007: Boot fails fast on a malformed metrics_listen. Operators
    // who misconfigure the address learn at startup, not three weeks
    // later during an incident. "off" (and empty for backward compat)
    // explicitly disable the endpoint.
    let metrics_registry = Arc::new(metrics::Metrics::new());
    let metrics_addr_str = cli.metrics_listen.trim();
    let metrics_addr: Option<SocketAddr> = if metrics_addr_str.is_empty()
        || metrics_addr_str.eq_ignore_ascii_case("off")
    {
        info!("metrics endpoint explicitly disabled (OOTILS_METRICS_LISTEN={metrics_addr_str:?})");
        None
    } else {
        Some(metrics_addr_str.parse::<SocketAddr>().map_err(|e| {
            anyhow::anyhow!(
                "OOTILS_METRICS_LISTEN={:?} is not a valid host:port: {} \
                 (use \"off\" to disable the metrics server)",
                metrics_addr_str,
                e
            )
        })?)
    };
    if let Some(addr) = metrics_addr {
        let _metrics_handle =
            metrics::spawn_metrics_server(metrics_registry.clone(), addr);
    }

    let queue = Arc::new(write_behind::WriteBehindQueue::with_caps(
        wal.clone(),
        metrics_registry.clone(),
        cli.queue_max_depth,
    ));
    // Publish the configured caps as gauges so operators can see them
    // from /metrics (vs having to dig through engine logs).
    metrics_registry
        .queue_max_depth
        .store(cli.queue_max_depth as i64, std::sync::atomic::Ordering::Relaxed);
    metrics_registry
        .wal_max_bytes
        .store(cli.wal_max_bytes, std::sync::atomic::Ordering::Relaxed);
    // F-013 (Cluster F): hold the flusher's JoinHandle so we can
    // gracefully tear it down at shutdown. The boot-time
    // verify_postgres / loader / metrics-server tasks are detached
    // because their lifetimes are naturally bounded (the connection
    // future returns once the Client is dropped). The bg flusher is
    // long-lived and worth aborting cleanly to avoid leaving a
    // half-flushed batch in tokio's runtime when main exits.
    let dsn_for_flusher = cli.dsn.clone();
    let flusher_handle = write_behind::spawn_flusher(
        queue.clone(),
        dsn_for_flusher,
        cli.flush_interval_ms,
    );

    // If we recovered from WAL, also re-enqueue those deltas for
    // Postgres flush. Each delta carries the seq of the record it
    // came from — the PG UPDATE's seq-guard (write_behind.rs A6 /
    // F-014) ensures we never clobber newer PG state with an older
    // replay value.
    if !recovered.is_empty() {
        let mut deltas = Vec::new();
        for sr in recovered {
            let seq = sr.seq;
            for d in sr.record.deltas {
                deltas.push(write_behind::PendingDelta::from_delta(seq, d));
            }
        }
        queue.push(deltas);
        info!("recovered deltas re-enqueued for Postgres flush");
    }

    let engine = service::EngineSvc::new(boot_time, graph_lock, queue, metrics_registry);

    info!(
        addr = %cli.listen,
        request_timeout_ms = cli.request_timeout_ms,
        "gRPC server listening"
    );
    // F-016: symmetric message-size limits with the Python client
    // (which lifts to 256 MB in client.py). Without explicit
    // .max_decoding_message_size on the server, tonic 0.12 defaults to
    // 4 MB and rejects larger requests with an unhelpful
    // RESOURCE_EXHAUSTED.
    const MAX_MSG_BYTES: usize = 256 * 1024 * 1024;
    let engine_svc = ootils_proto::engine::v1::engine_server::EngineServer::new(engine)
        .max_decoding_message_size(MAX_MSG_BYTES)
        .max_encoding_message_size(MAX_MSG_BYTES);
    // F-018: per-call timeout via tonic's built-in helper. Past this
    // deadline tonic cancels the handler future and returns
    // Status::cancelled to the client. Prevents a stuck rayon worker
    // or slow client from holding the connection indefinitely.
    let server = Server::builder()
        .timeout(std::time::Duration::from_millis(cli.request_timeout_ms))
        .add_service(engine_svc)
        .serve_with_shutdown(cli.listen, shutdown_signal());
    server.await?;
    // F-013 graceful shutdown: abort the bg flusher and join its
    // handle. The WAL has fsync'd everything we acked to clients, so
    // an in-flight PG batch can be safely cancelled — recovery on
    // next boot will re-flush via replay.
    info!("gRPC server stopped — aborting write-behind flusher");
    flusher_handle.abort();
    match flusher_handle.await {
        Ok(()) => info!("flusher exited cleanly"),
        Err(e) if e.is_cancelled() => info!("flusher cancelled (expected)"),
        Err(e) => warn!(error = %e, "flusher join error"),
    }
    info!("ootils-engine shut down cleanly");
    Ok(())
}

/// One-shot full propagation bench. Marks every active PI dirty, runs
/// the propagator, prints timing. Phase 3 gate: compute < 100ms on L.
fn run_bench(graph_lock: &Arc<RwLock<state::Graph>>) {
    info!("running full-propagation bench (phase 3 gate)");
    let t_mark = Instant::now();
    let dirty = {
        let mut g = graph_lock.write();
        propagator::mark_all_pi_dirty(&mut g)
    };
    let mark_ms = t_mark.elapsed().as_millis();
    info!(n_dirty = dirty.len(), mark_ms, "marked all active PIs dirty");

    let t_prop = Instant::now();
    let stats = {
        let mut g = graph_lock.write();
        propagator::propagate(&mut g, &dirty)
    };
    let total_ms = t_prop.elapsed().as_millis();
    info!(
        n_dirty = stats.n_dirty,
        n_processed = stats.n_processed,
        n_changed = stats.n_changed,
        n_shortages = stats.n_shortages,
        compute_us = stats.compute_us,
        compute_ms = stats.compute_us / 1000,
        total_ms,
        "BENCH RESULT — full propagation (phase 3 gate)"
    );
}

/// Phase-4 gate: fork N scenarios in sequence, log per-fork timing.
/// Validates that Fork target (< 50ms) holds — or honestly surfaces
/// the gap if not.
fn run_bench_fork(graph_lock: &Arc<RwLock<state::Graph>>, n: usize) {
    info!(n, "running fork bench (phase 4 gate)");
    let mgr = scenario::ScenarioManager::new();
    let mut timings_ms: Vec<u64> = Vec::with_capacity(n);
    for i in 0..n {
        let (_s, st) = mgr.fork_from_baseline(format!("bench-fork-{}", i), graph_lock);
        timings_ms.push(st.total_ms);
        info!(
            iter = i,
            clone_ms = st.clone_ms,
            total_ms = st.total_ms,
            active_scenarios = mgr.len(),
            "fork done"
        );
    }
    let total: u64 = timings_ms.iter().sum();
    let avg = total as f64 / n as f64;
    let mut sorted = timings_ms.clone();
    sorted.sort_unstable();
    let p50 = sorted[sorted.len() / 2];
    let p95 = sorted[((sorted.len() as f64) * 0.95) as usize];
    let max = *sorted.last().unwrap();
    info!(
        n,
        total_ms = total,
        avg_ms = avg,
        p50_ms = p50,
        p95_ms = p95,
        max_ms = max,
        "BENCH RESULT — fork bench (phase 4 gate)"
    );
}

fn init_tracing(filter: &str) {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    let env_filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt::layer().with_target(true).with_thread_ids(false))
        .init();
}

/// F-017: redact the password component of a Postgres DSN so logs +
/// error messages can include the connection target without leaking
/// credentials. Handles both URL form
/// (`postgresql://user:PW@host/db`) and key-value form
/// (`host=... user=... password=PW dbname=...`).
///
/// This function is the single allowed sink for DSN values that may
/// land in `tracing` output. New code MUST route DSN through here
/// before logging, never raw.
#[allow(dead_code)]
pub fn redact_dsn(dsn: &str) -> String {
    // URL form: scheme://user:password@host/...
    if let Some(scheme_end) = dsn.find("://") {
        let after_scheme = &dsn[scheme_end + 3..];
        if let Some(at_pos) = after_scheme.find('@') {
            let userinfo = &after_scheme[..at_pos];
            let after_at = &after_scheme[at_pos..];
            // userinfo = "user" | "user:password"
            let redacted_userinfo = match userinfo.find(':') {
                Some(colon) => format!("{}:****", &userinfo[..colon]),
                None => userinfo.to_string(),
            };
            return format!(
                "{}://{}{}",
                &dsn[..scheme_end],
                redacted_userinfo,
                after_at
            );
        }
    }
    // Key-value form: ... password=... ...
    let mut out = String::with_capacity(dsn.len());
    let mut chars = dsn.chars().peekable();
    while let Some(c) = chars.next() {
        if c == 'p'
            && dsn[out.len()..].starts_with("password=")
        {
            // Emit "password=****" and skip to next whitespace.
            out.push_str("password=****");
            // Skip past the original "password=value".
            for _ in 0.."password=".len() - 1 {
                chars.next();
            }
            while let Some(&next) = chars.peek() {
                if next.is_whitespace() {
                    break;
                }
                chars.next();
            }
            continue;
        }
        out.push(c);
    }
    out
}

async fn verify_postgres(dsn: &str) -> anyhow::Result<()> {
    use tokio_postgres::NoTls;
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            warn!(error = %e, "postgres connection task ended");
        }
    });
    let row = client.query_one("SELECT version()", &[]).await?;
    let version: String = row.get(0);
    info!(pg_version = %version, "Postgres reachable");
    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal;
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install ctrl_c");
    };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("ctrl-c received, shutting down"),
        _ = terminate => info!("SIGTERM received, shutting down"),
    }
}
