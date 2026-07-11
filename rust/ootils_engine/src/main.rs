//! ootils-engine — standalone Rust service (ADR-017 Architecture B).
//!
//! Phase 3 milestone:
//! - In-RAM Graph (phase 2).
//! - Native propagator on top of the graph (this phase).
//! - `Propagate` RPC implemented.
//! - One-shot CLI mode: `--bench` runs a full propagation on the loaded
//!   baseline and exits, printing timing — used to validate the
//!   ADR-017 phase-3 gate (compute < 100ms on profile L).

use arc_swap::ArcSwap;
use clap::Parser;
use mimalloc::MiMalloc;
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

    /// Log filter (tracing-subscriber EnvFilter syntax).
    ///
    /// F-047: when RUST_LOG is set by the operator (common in
    /// containerized prod to reduce noise), the entire default
    /// "info,ootils_engine=debug" is replaced — NOT merged. To keep
    /// debug-level for the engine while silencing the rest, use
    /// `RUST_LOG=warn,ootils_engine=debug`. The default below applies
    /// only when RUST_LOG is unset.
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

    /// Allow the engine to boot even if the baseline scenario has 0
    /// nodes or 0 PIs (F-011). Default OFF — operators see a hard
    /// fail at startup when DATABASE_URL points at the wrong DB. CI
    /// and tests against synthetic empty fixtures must opt in
    /// explicitly via this flag or `OOTILS_ALLOW_EMPTY_BASELINE=1`.
    #[arg(long, env = "OOTILS_ALLOW_EMPTY_BASELINE", default_value_t = false)]
    allow_empty_baseline: bool,

    /// Log output format. `text` = human-readable (dev default);
    /// `json` = one JSON object per line (prod default — log
    /// aggregators parse it natively). F-061 audit closure: the
    /// json feature of tracing-subscriber was pulled in but never
    /// wired; this flag activates it.
    #[arg(long, env = "OOTILS_LOG_FORMAT", default_value = "text")]
    log_format: String,

    /// P2.1.d: scenarios idle longer than this are evicted by the
    /// background scanner (frees their overlay RAM). Default 1 hour;
    /// set to 0 to disable eviction (e.g. for tests or short-lived
    /// processes). With Q3 design (200 active users), this caps the
    /// engine's accumulated overlay memory regardless of how many
    /// abandoned what-if scenarios pile up.
    #[arg(long, env = "OOTILS_SCENARIO_TTL_SEC", default_value_t = 3600)]
    scenario_ttl_sec: u64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log, &cli.log_format)?;
    let boot_time = Instant::now();

    info!(
        version = env!("CARGO_PKG_VERSION"),
        listen = %cli.listen,
        bench = cli.bench,
        "ootils-engine starting"
    );

    // F-007 + reviewer follow-up: validate metrics_listen FIRST,
    // before the expensive verify_postgres + loader steps. A
    // misconfigured address should fail in milliseconds, not after
    // a 5-second baseline load. (The actual server is spawned later;
    // this is just early validation of the parsed addr.)
    let metrics_addr_str = cli.metrics_listen.trim().to_string();
    let metrics_addr: Option<SocketAddr> =
        if metrics_addr_str.is_empty() || metrics_addr_str.eq_ignore_ascii_case("off") {
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

    verify_postgres(&cli.dsn).await?;

    let (graph, load_stats) = loader::load_baseline(&cli.dsn, cli.allow_empty_baseline).await?;
    // P2.1.a (F-026): ArcSwap baseline. Reads are zero-cost
    // refcount bumps; writes use clone-on-write (clone the Graph,
    // mutate, atomic-swap). Forks become O(1) — the multi-user
    // what-if pattern this enables is the target of Phase 2.
    let graph_lock = Arc::new(ArcSwap::from_pointee(graph));
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
        // P2.1.a: clone-on-write recovery. Single-shot at boot, so
        // the clone cost (~50ms on profile L) is well under the
        // dominant baseline-load cost (~2s).
        let current = graph_lock.load_full();
        let mut new_graph: state::Graph = (*current).clone();
        let by_id = &new_graph.by_node_id;
        let mut idx_pairs: Vec<(usize, wal::NodeDelta)> = Vec::with_capacity(n_deltas);
        for sr in &recovered {
            for d in &sr.record.deltas {
                if let Some(&idx) = by_id.get(&d.node_id) {
                    idx_pairs.push((idx as usize, d.clone()));
                }
            }
        }
        for (idx, d) in idx_pairs {
            let n = &mut new_graph.nodes[idx];
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
        graph_lock.store(Arc::new(new_graph));
        info!(n_records = n_recs, n_deltas, "WAL replay applied to RAM");
    }

    // Item #2: Prometheus metrics — process-wide counter registry.
    // Address was already parsed + validated at the top of main()
    // (F-007 fail-fast).
    let metrics_registry = Arc::new(metrics::Metrics::new());
    if let Some(addr) = metrics_addr {
        let _metrics_handle = metrics::spawn_metrics_server(metrics_registry.clone(), addr);
    } else {
        info!("metrics endpoint explicitly disabled");
    }

    let queue = Arc::new(write_behind::WriteBehindQueue::with_caps(
        wal.clone(),
        metrics_registry.clone(),
        cli.queue_max_depth,
    ));
    // Publish the configured caps as gauges so operators can see them
    // from /metrics (vs having to dig through engine logs).
    metrics_registry.queue_max_depth.store(
        cli.queue_max_depth as i64,
        std::sync::atomic::Ordering::Relaxed,
    );
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
    let flusher_handle =
        write_behind::spawn_flusher(queue.clone(), dsn_for_flusher, cli.flush_interval_ms);

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

    // P2.1.d: ScenarioManager is constructed here (instead of inside
    // EngineSvc::new) so we can hand an Arc to the eviction task too.
    let scenarios = Arc::new(scenario::ScenarioManager::new());

    // Spawn the TTL eviction background task if ttl > 0.
    let eviction_handle: Option<tokio::task::JoinHandle<()>> = if cli.scenario_ttl_sec > 0 {
        let scenarios_for_evict = scenarios.clone();
        let metrics_for_evict = metrics_registry.clone();
        let ttl = cli.scenario_ttl_sec;
        // Scan every (ttl/4) seconds so we catch evictions ~ttl after
        // last access on average. Floor at 30 s for very short TTLs.
        let scan_interval_sec = (ttl / 4).max(30);
        info!(
            scenario_ttl_sec = ttl,
            scan_interval_sec, "scenario TTL eviction task spawned (P2.1.d)"
        );
        Some(tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(std::time::Duration::from_secs(scan_interval_sec));
            // Skip first immediate fire — give the engine a moment to settle.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let evicted = scenarios_for_evict.evict_idle(ttl);
                if !evicted.is_empty() {
                    info!(
                        n_evicted = evicted.len(),
                        active_remaining = scenarios_for_evict.len(),
                        "TTL eviction freed idle scenarios"
                    );
                    metrics_for_evict.active_scenarios.store(
                        scenarios_for_evict.len() as i64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
            }
        }))
    } else {
        info!("scenario TTL eviction disabled (OOTILS_SCENARIO_TTL_SEC=0)");
        None
    };

    let engine = service::EngineSvc::new(boot_time, graph_lock, scenarios, queue, metrics_registry);

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
    // P2.1.d: tear down the eviction task too.
    if let Some(h) = eviction_handle {
        h.abort();
        let _ = h.await;
    }
    info!("ootils-engine shut down cleanly");
    Ok(())
}

/// One-shot full propagation bench. Marks every active PI dirty, runs
/// the propagator, prints timing.
///
/// P2.1.a (F-026): with ArcSwap baseline, this now exercises the
/// clone-on-write path explicitly (mark needs &mut Graph, so we
/// clone the whole graph for the bench, mutate, and store back).
/// The bench reports the clone cost separately so the trade-off is
/// visible.
fn run_bench(graph_lock: &Arc<ArcSwap<state::Graph>>) {
    info!("running full-propagation bench (phase 3 gate)");

    // P2.1.a: clone-on-write — measured separately so the bench
    // surfaces the new cost honestly.
    let t_clone = Instant::now();
    let current = graph_lock.load_full();
    let mut new_graph: state::Graph = (*current).clone();
    let clone_ms = t_clone.elapsed().as_millis();

    let t_mark = Instant::now();
    let dirty = propagator::mark_all_pi_dirty(&mut new_graph);
    let mark_ms = t_mark.elapsed().as_millis();
    info!(
        n_dirty = dirty.len(),
        clone_ms, mark_ms, "marked all active PIs dirty (post-CoW)"
    );

    let t_prop = Instant::now();
    let stats = propagator::propagate(&mut new_graph, &dirty);
    let prop_ms = t_prop.elapsed().as_millis();

    let t_store = Instant::now();
    graph_lock.store(Arc::new(new_graph));
    let store_ms = t_store.elapsed().as_millis();

    let total_ms = clone_ms + mark_ms + prop_ms + store_ms;
    info!(
        n_dirty = stats.n_dirty,
        n_processed = stats.n_processed,
        n_changed = stats.n_changed,
        n_shortages = stats.n_shortages,
        compute_us = stats.compute_us,
        compute_ms = stats.compute_us / 1000,
        clone_ms,
        store_ms,
        total_ms,
        "BENCH RESULT — full propagation (phase 3 gate)"
    );
}

/// Phase-4 gate: fork N scenarios in sequence, log per-fork timing.
/// P2.1.a target: < 1ms per fork (was 40-60ms with deep clone).
fn run_bench_fork(graph_lock: &Arc<ArcSwap<state::Graph>>, n: usize) {
    info!(n, "running fork bench (phase 4 gate)");
    let mgr = scenario::ScenarioManager::new();
    // For sub-ms forks we need µs precision in the per-call timing.
    let mut timings_us: Vec<u64> = Vec::with_capacity(n);
    for i in 0..n {
        let t = Instant::now();
        let (_s, _st) = mgr.fork_from_baseline(format!("bench-fork-{}", i), graph_lock);
        let us = t.elapsed().as_micros() as u64;
        timings_us.push(us);
        if i < 5 || i % 50 == 0 {
            info!(
                iter = i,
                fork_us = us,
                active_scenarios = mgr.len(),
                "fork done"
            );
        }
    }
    let total_us: u64 = timings_us.iter().sum();
    let timings_ms: Vec<u64> = timings_us.iter().map(|&us| us / 1000).collect();
    // Also report µs versions because ms is too coarse with ArcSwap.
    let mut sorted_us = timings_us.clone();
    sorted_us.sort_unstable();
    let p50_us = sorted_us[sorted_us.len() / 2];
    let p95_us = sorted_us[((sorted_us.len() as f64) * 0.95) as usize];
    let max_us = *sorted_us.last().unwrap();
    info!(
        total_us,
        p50_us, p95_us, max_us, "BENCH (µs precision — P2.1.a ArcSwap)"
    );
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

fn init_tracing(filter: &str, format: &str) -> anyhow::Result<()> {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    let env_filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));
    match format.to_ascii_lowercase().as_str() {
        "text" => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().with_target(true).with_thread_ids(false))
                .init();
        }
        "json" => {
            // F-061 audit closure: prod default. One JSON object per
            // line; log aggregators (Loki/Splunk/CloudWatch) parse it
            // natively without regex.
            tracing_subscriber::registry()
                .with(env_filter)
                .with(
                    fmt::layer()
                        .json()
                        .with_target(true)
                        .with_thread_ids(false)
                        .with_current_span(true),
                )
                .init();
        }
        other => anyhow::bail!(
            "OOTILS_LOG_FORMAT={:?} not recognized — use \"text\" or \"json\"",
            other
        ),
    }
    Ok(())
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
            return format!("{}://{}{}", &dsn[..scheme_end], redacted_userinfo, after_at);
        }
    }
    // Key-value form: ... password=... ...
    let mut out = String::with_capacity(dsn.len());
    let mut chars = dsn.chars().peekable();
    while let Some(c) = chars.next() {
        if c == 'p' && dsn[out.len()..].starts_with("password=") {
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

    // F-028: probe the schema we depend on. Pointing the engine at
    // the wrong DB (or a DB that hasn't run migrations) used to fail
    // deep inside the loader with an opaque "column does not exist"
    // — much harder to debug than an early "your schema is missing X".
    // We probe a single discriminating column (last_calc_seq, added
    // by migration 037 which is part of the rust-svc engine's
    // contract). Missing → bail loudly.
    let probe = client
        .query_opt(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = 'public' \
               AND table_name = 'nodes' \
               AND column_name = 'last_calc_seq'",
            &[],
        )
        .await?;
    if probe.is_none() {
        anyhow::bail!(
            "schema check failed: nodes.last_calc_seq column missing. \
             Run migration 037_nodes_last_calc_seq.sql (or all pending \
             migrations). DATABASE_URL appears correct (PG {})",
            version
        );
    }

    info!(pg_version = %version, "Postgres reachable + schema check OK");
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
