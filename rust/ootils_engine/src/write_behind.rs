//! write_behind.rs — async write-behind queue to Postgres (ADR-017 §3.4).
//!
//! Phase 5 deliverable, hardened in Cluster A of the senior audit
//! response (F-001/002/003/006/014).
//!
//! Lets the propagation hot path commit RAM state (and the WAL barrier)
//! without waiting for Postgres. A background tokio task drains the
//! queue every 100 ms or when 10K deltas accumulate, whichever comes
//! first.
//!
//! ## Durability sequence per propagation
//!
//!   1. compute results (in RAM, sub-ms)
//!   2. apply to Graph (in RAM)
//!   3. WAL.append() + fsync     -- assigns + returns a sequence number
//!   4. WriteBehindQueue.push(PendingDelta { seq, ... })  -- non-blocking
//!   ----- caller-visible latency stops here -----
//!   5. background task flushes batch to Postgres
//!   6. on flush success: WAL.set_applied_pg_seq(max_seq_in_batch)
//!      (NOT a truncate — see wal.rs for the v2 marker contract)
//!   7. periodically: WAL.maybe_rotate() (atomic-rename of compacted file)
//!
//! ## Key changes vs v1 (audit F-001/002/003)
//!
//! - Drain happens AFTER bulk UPDATE prepares its params (drain order
//!   no longer matters for correctness since each delta carries its
//!   seq). On failed flush, the batch is merged back into the queue
//!   with per-node-id dedupe keeping the highest seq (F-002).
//! - Successful flush no longer truncates the WAL; it advances
//!   `applied_pg_seq` (F-001 + F-003 — replay now skips records below
//!   the marker, no more double-flush).
//! - PG UPDATE uses a seq-guard (`WHERE nodes.last_calc_seq < u.seq`)
//!   so older WAL records can never overwrite newer PG state (F-014).
//! - `SET session_replication_role = 'replica'` removed (F-004): we
//!   set updated_at = now() ourselves and the trigger has been dropped
//!   by migration.

use crate::wal::{NodeDelta, WalWriter};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// One PI's worth of writeback state, tagged with the seq assigned at
/// WAL-append time. Deduped by node_id (keep highest seq) on failed
/// flush.
#[derive(Debug, Clone)]
pub struct PendingDelta {
    pub seq: u64,
    pub node_id: Uuid,
    pub opening_stock: Decimal,
    pub inflows: Decimal,
    pub outflows: Decimal,
    pub closing_stock: Decimal,
    pub has_shortage: bool,
    pub shortage_qty: Decimal,
}

impl PendingDelta {
    pub fn from_delta(seq: u64, d: NodeDelta) -> Self {
        Self {
            seq,
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

    /// Drain everything currently pending. Returns the deltas + the
    /// highest seq seen in the batch (used by the flusher to advance
    /// `applied_pg_seq` after PG commit).
    pub fn drain_batch(&self) -> (Vec<PendingDelta>, u64) {
        let mut q = self.pending.lock();
        let out: Vec<_> = q.drain(..).collect();
        let max_seq = out.iter().map(|d| d.seq).max().unwrap_or(0);
        self.metrics
            .writeback_queue_depth
            .store(0, std::sync::atomic::Ordering::Relaxed);
        (out, max_seq)
    }

    pub fn len(&self) -> usize {
        self.pending.lock().len()
    }

    pub fn wal(&self) -> &Arc<WalWriter> {
        &self.wal
    }

    /// Re-enqueue a failed batch by merging it with the current queue
    /// contents, deduping on node_id and keeping the highest-seq entry
    /// per node (F-002 fix).
    ///
    /// This is correctness-preserving: only the latest value per node
    /// needs to land in PG, so dropping older intermediate deltas is
    /// fine and bounds memory under sustained churn.
    pub fn reenqueue_with_dedupe(&self, failed_batch: Vec<PendingDelta>) {
        let mut q = self.pending.lock();
        // Build a map of all known node_id → highest-seq delta, scanning
        // both the current queue (which has new deltas appended during
        // the failed flush) and the failed batch.
        let mut latest: HashMap<Uuid, PendingDelta> = HashMap::with_capacity(q.len() + failed_batch.len());
        for d in q.drain(..) {
            keep_latest(&mut latest, d);
        }
        for d in failed_batch {
            keep_latest(&mut latest, d);
        }
        // Re-insert ordered by seq so on the next successful flush the
        // PG UPDATE's UNNEST array is in monotonic seq order (cosmetic
        // — the seq-guard makes order semantically irrelevant).
        let mut merged: Vec<PendingDelta> = latest.into_values().collect();
        merged.sort_unstable_by_key(|d| d.seq);
        q.extend(merged);
        self.metrics
            .writeback_queue_depth
            .store(q.len() as i64, std::sync::atomic::Ordering::Relaxed);
    }
}

fn keep_latest(map: &mut HashMap<Uuid, PendingDelta>, d: PendingDelta) {
    map.entry(d.node_id)
        .and_modify(|existing| {
            if d.seq > existing.seq {
                *existing = d.clone();
            }
        })
        .or_insert(d);
}

/// Spawn the background flush task. Returns the JoinHandle so callers
/// (main) can wait for it during graceful shutdown.
///
/// Behaviour:
/// - On the normal cadence (every `flush_interval_ms`), drain the queue
///   and bulk-UPDATE Postgres. On success, advance the WAL marker.
///   Periodically attempt a WAL rotation.
/// - On flush failure (PG down, network blip), re-enqueue the batch
///   with per-node-id dedupe and back off exponentially up to 30s.
///   Restored to baseline cadence after the first success. Bounded —
///   no CPU hot loop.
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
            let (batch, max_seq) = queue.drain_batch();
            if batch.is_empty() {
                // Reset backoff if we were in failure mode but the queue
                // is naturally empty now.
                if consecutive_failures > 0 {
                    info!("flusher idle, resetting backoff");
                    consecutive_failures = 0;
                    current_interval = baseline_interval;
                }
                // Opportunistic rotation when idle — cheap to call,
                // short-circuits if not eligible.
                if let Err(e) = queue.wal.maybe_rotate() {
                    warn!(error = %e, "WAL rotation failed (will retry)");
                }
                continue;
            }
            let n = batch.len();
            match flush_to_postgres(&dsn, &batch).await {
                Ok(_) => {
                    queue.metrics.record_pg_flush_success();
                    if let Err(e) = queue.wal.set_applied_pg_seq(max_seq) {
                        error!(
                            error = %e,
                            max_seq,
                            "WAL marker advance failed after PG flush — will retry on next flush"
                        );
                    } else {
                        debug!(
                            n,
                            max_seq,
                            "WriteBehindQueue: batch flushed + WAL marker advanced"
                        );
                    }
                    // After a successful flush is a good moment to attempt
                    // rotation (the applied_pg_seq just advanced).
                    if let Err(e) = queue.wal.maybe_rotate() {
                        warn!(error = %e, "WAL rotation failed (will retry)");
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
                    // F-002 fix: merge the failed batch back into the
                    // queue with dedupe-by-node_id, keeping the highest
                    // seq per node.
                    queue.reenqueue_with_dedupe(batch);
                }
            }
        }
    })
}

/// Bulk UPDATE Postgres `nodes` via UNNEST array params, guarded by
/// `last_calc_seq` so older WAL records cannot clobber newer PG state
/// (F-014). Sets `last_calc_seq = u.seq` on every applied row.
///
/// F-004 fix: no `SET session_replication_role = 'replica'` — that
/// required SUPERUSER and the trigger has been dropped by migration.
async fn flush_to_postgres(
    dsn: &str,
    batch: &[PendingDelta],
) -> anyhow::Result<()> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            warn!(error = %e, "flush postgres connection task ended");
        }
    });

    let n = batch.len();
    let mut node_ids: Vec<Uuid> = Vec::with_capacity(n);
    let mut seqs: Vec<i64> = Vec::with_capacity(n);
    let mut openings: Vec<Decimal> = Vec::with_capacity(n);
    let mut inflows: Vec<Decimal> = Vec::with_capacity(n);
    let mut outflows: Vec<Decimal> = Vec::with_capacity(n);
    let mut closings: Vec<Decimal> = Vec::with_capacity(n);
    let mut has_shortages: Vec<bool> = Vec::with_capacity(n);
    let mut shortage_qtys: Vec<Decimal> = Vec::with_capacity(n);
    for d in batch {
        node_ids.push(d.node_id);
        seqs.push(d.seq as i64); // u64 → i64; safe up to 2^63 propagations
        openings.push(d.opening_stock);
        inflows.push(d.inflows);
        outflows.push(d.outflows);
        closings.push(d.closing_stock);
        has_shortages.push(d.has_shortage);
        shortage_qtys.push(d.shortage_qty);
    }

    client
        .execute(
            "UPDATE nodes \
             SET opening_stock = u.opening_stock, \
                 inflows = u.inflows, \
                 outflows = u.outflows, \
                 closing_stock = u.closing_stock, \
                 has_shortage = u.has_shortage, \
                 shortage_qty = u.shortage_qty, \
                 last_calc_seq = u.seq, \
                 updated_at = now() \
             FROM UNNEST($1::uuid[], $2::bigint[], $3::numeric[], $4::numeric[], \
                         $5::numeric[], $6::numeric[], $7::bool[], $8::numeric[]) \
                  AS u(node_id, seq, opening_stock, inflows, outflows, \
                       closing_stock, has_shortage, shortage_qty) \
             WHERE nodes.node_id = u.node_id \
               AND (nodes.last_calc_seq IS NULL OR nodes.last_calc_seq < u.seq)",
            &[
                &node_ids,
                &seqs,
                &openings,
                &inflows,
                &outflows,
                &closings,
                &has_shortages,
                &shortage_qtys,
            ],
        )
        .await?;

    debug!(n, "wrote batch to Postgres");
    Ok(())
}
