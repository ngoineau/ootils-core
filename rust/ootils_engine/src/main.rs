//! ootils-engine — standalone Rust service (ADR-017 Architecture B).
//!
//! Phase 1 (skeleton):
//! - Process boots with mimalloc as global allocator.
//! - Reads config from CLI/env (DSN, listen address).
//! - Spins up tokio multi-threaded runtime.
//! - Starts a tonic gRPC server with a stub `Engine` implementation
//!   that only answers `Health` and `Metrics`. Everything else returns
//!   `Unimplemented`.
//! - Connects to Postgres (verifies credentials).
//! - Exits cleanly on SIGTERM/SIGINT.
//!
//! Next phases (per ADR-017):
//! - Phase 2: bootstrap the in-RAM graph from Postgres
//! - Phase 3: port the propagator
//! - Phase 4: scenarios
//! - Phase 5: WAL + write-behind
//! - Phase 6: full gRPC API
//! - Phase 7: stress + observability
//! - Phase 8: production rollout

use arc_swap::ArcSwap;
use clap::Parser;
use mimalloc::MiMalloc;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tonic::transport::Server;
use tracing::{info, warn};

mod loader;
mod service;
mod state;

use state::Graph;

/// Use mimalloc as the global allocator. Bench-justified : 5-15% faster
/// than the default on multi-threaded loads, lower fragmentation, mature.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "ootils-engine")]
#[command(version, about = "ootils-core Rust engine service (ADR-017)")]
struct Cli {
    /// Postgres DSN — same shape as DATABASE_URL.
    #[arg(long, env = "DATABASE_URL")]
    dsn: String,

    /// gRPC listen address.
    #[arg(long, env = "OOTILS_ENGINE_LISTEN", default_value = "127.0.0.1:50051")]
    listen: SocketAddr,

    /// Log level — passed to `tracing_subscriber` env filter.
    #[arg(long, env = "RUST_LOG", default_value = "info,ootils_engine=debug")]
    log: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log);
    let boot_time = Instant::now();

    info!(
        version = env!("CARGO_PKG_VERSION"),
        listen = %cli.listen,
        "ootils-engine starting"
    );

    // Verify Postgres connectivity before binding the socket.
    // Saves the operator from booting a useless service if creds are wrong.
    verify_postgres(&cli.dsn).await?;

    // Bootstrap: load the baseline graph from Postgres into RAM.
    // This is the heart of phase 2 — Postgres becomes a cold-start dep
    // only, not a hot-path dep.
    let (graph, load_stats) = loader::load_baseline(&cli.dsn).await?;
    let baseline = Arc::new(ArcSwap::from_pointee(graph));
    info!(
        nodes = load_stats.n_nodes,
        edges = load_stats.n_edges,
        memory_mb = load_stats.memory_bytes / 1_048_576,
        boot_load_ms = load_stats.elapsed_ms,
        "baseline ready in RAM"
    );

    let engine = service::EngineSvc::new(boot_time, baseline);

    info!(addr = %cli.listen, "gRPC server listening");
    let server = Server::builder()
        .add_service(
            ootils_proto::engine::v1::engine_server::EngineServer::new(engine),
        )
        .serve_with_shutdown(cli.listen, shutdown_signal());

    server.await?;
    info!("ootils-engine shut down cleanly");
    Ok(())
}

fn init_tracing(filter: &str) {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    let env_filter = EnvFilter::try_new(filter)
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt::layer().with_target(true).with_thread_ids(false))
        .init();
}

async fn verify_postgres(dsn: &str) -> anyhow::Result<()> {
    use tokio_postgres::NoTls;
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    // Spawn the connection driver so the client can talk.
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
