//! write_behind.rs — async write-behind queue to Postgres (ADR-017 §3.4).
//!
//! Phase 5 deliverable. Lets the propagation hot path commit RAM state
//! (and the WAL barrier) without waiting for Postgres. A background
//! tokio task drains the queue every 100 ms or when 10K deltas
//! accumulate, whichever comes first.
//!
//! Durability sequence per propagation:
//!   1. compute results (in RAM, sub-ms)
//!   2. apply to Graph (in RAM)
//!   3. WAL.append() + fsync   <-- durability barrier; caller can be acked
//!   4. WriteBehindQueue.push(deltas)  <-- non-blocking, returns to caller
//!   ----- caller-visible latency stops here -----
//!   5. background task flushes to Postgres
//!   6. on flush success: WAL.truncate_after_flush()
//!
//! If the process dies between (3) and (6), the WAL still has the
//! deltas; on restart, replay them.

use crate::wal::{NodeDelta, WalWriter};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// One PI's worth of writeback state.
#[derive(Debug, Clone)]
pub struct PendingDelta {
    pub node_id: Uuid,
    pub opening_stock: Decimal,
    pub inflows: Decimal,
    pub outflows: Decimal,
    pub closing_stock: Decimal,
    pub has_shortage: bool,
    pub shortage_qty: Decimal,
}

impl From<NodeDelta> for PendingDelta {
    fn from(d: NodeDelta) -> Self {
        Self {
            node_id: d.node_id,
            opening_stock: d.opening_stock,
            inflows: d.inflows,
            outflows: d.outflows,
            closing_stock: d.closing_stock,
            has_shortage: d.has_shortage,
            shortage_qty: d.shortage_qty,
        }
    }
}

pub struct WriteBehindQueue {
    pending: Mutex<VecDeque<PendingDelta>>,
    wal: Arc<WalWriter>,
    /// Tunable: max queue size before forcing a flush regardless of
    /// the timer.
    max_pending_before_flush: usize,
    metrics: Arc<crate::metrics::Metrics>,
}

impl WriteBehindQueue {
    pub fn new(wal: Arc<WalWriter>, metrics: Arc<crate::metrics::Metrics>) -> Self {
        Self {
            pending: Mutex::new(VecDeque::with_capacity(1024)),
            wal,
            max_pending_before_flush: 10_000,
            metrics,
        }
    }

    /// Enqueue deltas. Non-blocking. Caller has already paid the WAL
    /// fsync, so durability is guaranteed at this point.
    pub fn push(&self, deltas: impl IntoIterator<Item = PendingDelta>) -> bool {
        let mut q = self.pending.lock();
        q.extend(deltas);
        let len = q.len();
        self.metrics
            .writeback_queue_depth
            .store(len as i64, std::sync::atomic::Ordering::Relaxed);
        len >= self.max_pending_before_flush
    }

    pub fn drain_batch(&self) -> Vec<PendingDelta> {
        let mut q = self.pending.lock();
        let out: Vec<_> = q.drain(..).collect();
        self.metrics
            .writeback_queue_depth
            .store(0, std::sync::atomic::Ordering::Relaxed);
        out
    }

    pub fn len(&self) -> usize {
        self.pending.lock().len()
    }

    pub fn wal(&self) -> &Arc<WalWriter> {
        &self.wal
    }
}

/// Spawn the background flush task. Returns the JoinHandle so callers
/// (main) can wait for it during graceful shutdown.
///
/// Behaviour:
/// - On the normal cadence (every `flush_interval_ms`), drain the queue
///   and bulk-UPDATE Postgres. On success, truncate the WAL.
/// - On flush failure (PG down, network blip), re-enqueue the batch
///   and back off exponentially up to 30s. Restored to baseline
///   cadence after the first success. Bounded — no CPU hot loop.
/// - Phase 7 explicitly does NOT alert on backoff yet — that wires
///   into OTLP/Prometheus in phase 8. For now we just log.
pub fn spawn_flusher(
    queue: Arc<WriteBehindQueue>,
    dsn: String,
    flush_interval_ms: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let baseline_interval = Duration::from_millis(flush_interval_ms);
        let max_backoff = Duration::from_secs(30);
        let mut current_interval = baseline_interval;
        let mut consecutive_failures = 0u32;
        info!(flush_interval_ms, "write-behind flusher started");

        loop {
            tokio::time::sleep(current_interval).await;
            let batch = queue.drain_batch();
            if batch.is_empty() {
                // Reset backoff if we were in failure mode but the queue
                // is naturally empty now.
                if consecutive_failures > 0 {
                    info!("flusher idle, resetting backoff");
                    consecutive_failures = 0;
                    current_interval = baseline_interval;
                }
                continue;
            }
            let n = batch.len();
            match flush_to_postgres(&dsn, &batch).await {
                Ok(_) => {
                    queue.metrics.record_pg_flush_success();
                    if let Err(e) = queue.wal.truncate_after_flush() {
                        error!(error = %e, "WAL truncate failed after PG flush");
                    } else {
                        debug!(n, "WriteBehindQueue: batch flushed + WAL truncated");
                    }
                    if consecutive_failures > 0 {
                        info!(
                            consecutive_failures,
                            "flusher recovered, resetting backoff"
                        );
                        consecutive_failures = 0;
                        current_interval = baseline_interval;
                    }
                }
                Err(e) => {
                    queue.metrics.record_pg_flush_failure();
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    // Exponential backoff: baseline * 2^failures, capped at 30s.
                    let shift = consecutive_failures.min(8);
                    let multiplier = 1u32.checked_shl(shift).unwrap_or(256);
                    let new_interval = baseline_interval.saturating_mul(multiplier);
                    current_interval = new_interval.min(max_backoff);
                    error!(
                        error = %e,
                        n,
                        consecutive_failures,
                        next_attempt_ms = current_interval.as_millis() as u64,
                        "WriteBehindQueue: flush to Postgres FAILED, backing off"
                    );
                    // Push the batch back so the next attempt retries.
                    queue.pending.lock().extend(batch);
                }
            }
        }
    })
}

/// Bulk UPDATE Postgres `nodes` via UNNEST array params.
///
/// Same approach as `ootils_kernel::writeback` chantier-A code: one
/// roundtrip, server-side hash join. Suppress triggers via
/// `session_replication_role = replica` (we set updated_at ourselves).
async fn flush_to_postgres(
    dsn: &str,
    batch: &[PendingDelta],
) -> anyhow::Result<()> {
    let (mut client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            warn!(error = %e, "flush postgres connection task ended");
        }
    });

    // Begin tx, disable trigger for this session, run UPDATE, commit.
    let tx = client.build_transaction().start().await?;
    tx.execute("SET LOCAL session_replication_role = 'replica'", &[]).await?;

    let n = batch.len();
    let mut node_ids: Vec<Uuid> = Vec::with_capacity(n);
    let mut openings: Vec<Decimal> = Vec::with_capacity(n);
    let mut inflows: Vec<Decimal> = Vec::with_capacity(n);
    let mut outflows: Vec<Decimal> = Vec::with_capacity(n);
    let mut closings: Vec<Decimal> = Vec::with_capacity(n);
    let mut has_shortages: Vec<bool> = Vec::with_capacity(n);
    let mut shortage_qtys: Vec<Decimal> = Vec::with_capacity(n);
    for d in batch {
        node_ids.push(d.node_id);
        openings.push(d.opening_stock);
        inflows.push(d.inflows);
        outflows.push(d.outflows);
        closings.push(d.closing_stock);
        has_shortages.push(d.has_shortage);
        shortage_qtys.push(d.shortage_qty);
    }
    // Drop the immutable borrow of `batch` before we move into params.
    let _ = batch;

    tx.execute(
        "UPDATE nodes \
         SET opening_stock = u.opening_stock, \
             inflows = u.inflows, \
             outflows = u.outflows, \
             closing_stock = u.closing_stock, \
             has_shortage = u.has_shortage, \
             shortage_qty = u.shortage_qty, \
             updated_at = now() \
         FROM UNNEST($1::uuid[], $2::numeric[], $3::numeric[], $4::numeric[], \
                     $5::numeric[], $6::bool[], $7::numeric[]) \
              AS u(node_id, opening_stock, inflows, outflows, \
                   closing_stock, has_shortage, shortage_qty) \
         WHERE nodes.node_id = u.node_id",
        &[
            &node_ids,
            &openings,
            &inflows,
            &outflows,
            &closings,
            &has_shortages,
            &shortage_qtys,
        ],
    )
    .await?;

    tx.commit().await?;
    debug!(n, "wrote batch to Postgres");
    Ok(())
}
