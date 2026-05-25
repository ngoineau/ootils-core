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
mod propagator;
mod service;
mod state;

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

    let engine = service::EngineSvc::new(boot_time, graph_lock);

    info!(addr = %cli.listen, "gRPC server listening");
    let server = Server::builder()
        .add_service(ootils_proto::engine::v1::engine_server::EngineServer::new(engine))
        .serve_with_shutdown(cli.listen, shutdown_signal());
    server.await?;
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

fn init_tracing(filter: &str) {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    let env_filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt::layer().with_target(true).with_thread_ids(false))
        .init();
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
