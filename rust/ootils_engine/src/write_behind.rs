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
//!      ----- caller-visible latency stops here -----
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
use tokio_postgres::{IsolationLevel, NoTls, Statement};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Statement timeout applied to the cached PG client. Caps the worst
/// case of a single flush hanging on a slow/wedged Postgres. 30 s is
/// generous for a 10K-batch UPDATE and well under the 30 s backoff
/// cap so a wedged flush retries quickly.
const PG_STATEMENT_TIMEOUT_MS: u32 = 30_000;

/// Bulk UPDATE for the write-behind path. Guarded by `last_calc_seq`
/// (F-014) so older WAL records cannot clobber newer PG state.
const FLUSH_UPDATE_SQL: &str = "\
    UPDATE nodes \
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
      AND (nodes.last_calc_seq IS NULL OR nodes.last_calc_seq < u.seq)";

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

/// Reason a push was rejected. Surfaced upward as gRPC
/// `Status::resource_exhausted` so clients can backoff intelligently
/// rather than crashing the engine via unbounded queue growth (F-005).
#[derive(Debug, Clone)]
pub struct QueueFull {
    pub current_depth: usize,
    pub max_depth: usize,
}

impl std::fmt::Display for QueueFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "write-behind queue full: {} >= {} (Postgres flush may be stalled — check pg_flush_failure_total)",
            self.current_depth, self.max_depth
        )
    }
}

impl std::error::Error for QueueFull {}

pub struct WriteBehindQueue {
    pending: Mutex<VecDeque<PendingDelta>>,
    wal: Arc<WalWriter>,
    /// Tunable: max queue size before forcing a flush regardless of
    /// the timer.
    max_pending_before_flush: usize,
    /// Hard cap on queue depth (F-005). Push rejects above this with
    /// QueueFull so the engine doesn't unboundedly grow its in-RAM
    /// queue during a sustained PG outage.
    max_depth: usize,
    metrics: Arc<crate::metrics::Metrics>,
}

impl WriteBehindQueue {
    // Production boots via `with_caps` directly (main.rs, explicit
    // depth cap from CLI args); this default-depth wrapper has no
    // current caller.
    #[allow(dead_code)]
    pub fn new(wal: Arc<WalWriter>, metrics: Arc<crate::metrics::Metrics>) -> Self {
        Self::with_caps(wal, metrics, 1_000_000)
    }

    pub fn with_caps(
        wal: Arc<WalWriter>,
        metrics: Arc<crate::metrics::Metrics>,
        max_depth: usize,
    ) -> Self {
        Self {
            pending: Mutex::new(VecDeque::with_capacity(1024)),
            wal,
            max_pending_before_flush: 10_000,
            max_depth,
            metrics,
        }
    }

    // No current caller; kept as the natural accessor for a future
    // health/metrics endpoint (same rationale as WalWriter's).
    #[allow(dead_code)]
    pub fn max_depth(&self) -> usize {
        self.max_depth
    }

    /// Enqueue deltas. Non-blocking. Caller has already paid the WAL
    /// fsync, so durability is guaranteed at this point.
    ///
    /// F-005: returns `Err(QueueFull)` when the queue is over its
    /// configured cap. The caller (gRPC handler) should translate
    /// that into `Status::resource_exhausted` so clients can retry
    /// after the bg flusher drains.
    pub fn try_push(
        &self,
        deltas: impl IntoIterator<Item = PendingDelta>,
    ) -> Result<bool, QueueFull> {
        let mut q = self.pending.lock();
        // Check cap BEFORE extending so we never blow past it. We
        // hold the lock for the check + extend so concurrent appenders
        // can't race the boundary.
        let incoming: Vec<PendingDelta> = deltas.into_iter().collect();
        let projected = q.len() + incoming.len();
        if projected > self.max_depth {
            let current = q.len();
            drop(q);
            self.metrics
                .writeback_queue_depth
                .store(current as i64, std::sync::atomic::Ordering::Relaxed);
            return Err(QueueFull {
                current_depth: current,
                max_depth: self.max_depth,
            });
        }
        q.extend(incoming);
        let len = q.len();
        self.metrics
            .writeback_queue_depth
            .store(len as i64, std::sync::atomic::Ordering::Relaxed);
        Ok(len >= self.max_pending_before_flush)
    }

    /// Unbounded push — kept for the bg flusher's re-enqueue path
    /// (which has already proven it CAN fit, because the deltas were
    /// previously in the queue). New external callers should prefer
    /// `try_push`.
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

    // Exercised by the unit tests below (queue-depth assertions);
    // production code reads depth via `metrics.writeback_queue_depth`
    // instead.
    #[allow(dead_code)]
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
        let mut latest: HashMap<Uuid, PendingDelta> =
            HashMap::with_capacity(q.len() + failed_batch.len());
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

/// Cached Postgres client + prepared statement for the write-behind
/// path. F-012 fix: avoids the TCP+auth handshake on every flush
/// (~10-30 ms overhead at the previous 100 ms cadence = 10-30% of the
/// flush budget wasted). On any PG error the client is dropped and
/// the next flush reconnects + re-prepares — bounded blast radius for
/// transient PG hiccups.
pub struct PgFlushClient {
    dsn: String,
    state: Option<(tokio_postgres::Client, Statement)>,
}

impl PgFlushClient {
    pub fn new(dsn: String) -> Self {
        Self { dsn, state: None }
    }

    /// Open the cached connection if not already live + prepare the
    /// bulk UPDATE statement. Sets statement_timeout on the session.
    async fn ensure_connected(&mut self) -> anyhow::Result<()> {
        if self.state.is_some() {
            return Ok(());
        }
        let (client, connection) = tokio_postgres::connect(&self.dsn, NoTls).await?;
        // The connection future drives the wire protocol. When it
        // returns, the client is dead. We spawn it detached because
        // the client's lifetime owns the failure semantics — when we
        // detect an error on a flush we drop the client, which makes
        // the spawned future return Ready and exit.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                warn!(error = %e, "PG flush connection task ended");
            }
        });
        // F-012: statement timeout caps the worst-case hang on a
        // wedged PG.
        client
            .batch_execute(&format!(
                "SET statement_timeout = {}",
                PG_STATEMENT_TIMEOUT_MS
            ))
            .await?;
        let stmt = client.prepare(FLUSH_UPDATE_SQL).await?;
        info!(
            timeout_ms = PG_STATEMENT_TIMEOUT_MS,
            "PG flush client connected + statement prepared"
        );
        self.state = Some((client, stmt));
        Ok(())
    }

    /// Drop the cached connection. Used when a flush errors so the
    /// next call reconnects from scratch (the connection may be in a
    /// half-broken state after a network blip or PG restart).
    fn invalidate(&mut self) {
        if self.state.take().is_some() {
            debug!("PG flush client invalidated, will reconnect on next flush");
        }
    }

    /// Run one bulk UPDATE batch inside a REPEATABLE READ tx.
    ///
    /// F-024 fix: explicit isolation level. The previous code relied
    /// on PG's READ COMMITTED default, which under mixed-mode canary
    /// (rust-svc + SQL engine writing concurrently) allowed
    /// field-level inconsistency between the two writers. REPEATABLE
    /// READ guarantees that the rows we update see a consistent
    /// snapshot of last_calc_seq across the whole batch.
    pub async fn flush(&mut self, batch: &[PendingDelta]) -> anyhow::Result<()> {
        self.ensure_connected().await?;

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
            seqs.push(d.seq as i64);
            openings.push(d.opening_stock);
            inflows.push(d.inflows);
            outflows.push(d.outflows);
            closings.push(d.closing_stock);
            has_shortages.push(d.has_shortage);
            shortage_qtys.push(d.shortage_qty);
        }

        let result: anyhow::Result<()> = async {
            let (client, stmt) = self.state.as_mut().expect("ensure_connected returned Ok");
            let tx = client
                .build_transaction()
                .isolation_level(IsolationLevel::RepeatableRead)
                .start()
                .await?;
            tx.execute(
                stmt,
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
            tx.commit().await?;
            debug!(
                n,
                "wrote batch to Postgres (REPEATABLE READ tx, cached client)"
            );
            Ok(())
        }
        .await;

        if result.is_err() {
            // Conn may be wedged / closed by PG. Force reconnect next call.
            self.invalidate();
        }
        result
    }
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
        // F-012: one cached client for the lifetime of the flusher,
        // reconnecting only on PG-side errors.
        let mut pg = PgFlushClient::new(dsn);
        info!(flush_interval_ms, "write-behind flusher started");

        loop {
            tokio::time::sleep(current_interval).await;
            // Refresh the WAL size gauge for /metrics scrapers.
            queue.metrics.wal_size_bytes.store(
                queue.wal.current_size_bytes(),
                std::sync::atomic::Ordering::Relaxed,
            );
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
            match pg.flush(&batch).await {
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
                            max_seq, "WriteBehindQueue: batch flushed + WAL marker advanced"
                        );
                    }
                    // After a successful flush is a good moment to attempt
                    // rotation (the applied_pg_seq just advanced).
                    if let Err(e) = queue.wal.maybe_rotate() {
                        warn!(error = %e, "WAL rotation failed (will retry)");
                    }
                    if consecutive_failures > 0 {
                        info!(consecutive_failures, "flusher recovered, resetting backoff");
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

// ============================================================
// Inline tests (Cluster B / F-005 backpressure).
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;
    use rust_decimal::Decimal;
    use tempfile::TempDir;
    use uuid::Uuid;

    fn fresh_queue(max_depth: usize) -> (TempDir, Arc<WriteBehindQueue>) {
        let dir = TempDir::new().unwrap();
        let wal = Arc::new(WalWriter::open(dir.path().join("test.wal")).unwrap());
        let metrics = Arc::new(Metrics::new());
        let q = Arc::new(WriteBehindQueue::with_caps(wal, metrics, max_depth));
        (dir, q)
    }

    fn dummy_delta(seq: u64, i: u128) -> PendingDelta {
        PendingDelta {
            seq,
            node_id: Uuid::from_u128(i),
            opening_stock: Decimal::ZERO,
            inflows: Decimal::ZERO,
            outflows: Decimal::ZERO,
            closing_stock: Decimal::ZERO,
            has_shortage: false,
            shortage_qty: Decimal::ZERO,
        }
    }

    #[test]
    fn try_push_accepts_below_cap() {
        let (_d, q) = fresh_queue(10);
        let deltas: Vec<_> = (0..5).map(|i| dummy_delta(1, i)).collect();
        q.try_push(deltas).unwrap();
        assert_eq!(q.len(), 5);
    }

    #[test]
    fn try_push_rejects_when_cap_would_be_exceeded() {
        // F-005: queue depth cap must be enforced atomically — neither
        // partial-push nor over-push, just an early reject so the
        // caller can return RESOURCE_EXHAUSTED.
        let (_d, q) = fresh_queue(10);
        let first_batch: Vec<_> = (0..7).map(|i| dummy_delta(1, i)).collect();
        q.try_push(first_batch).unwrap();
        assert_eq!(q.len(), 7);

        let overflow_batch: Vec<_> = (0..5).map(|i| dummy_delta(2, 100 + i)).collect();
        let err = q.try_push(overflow_batch).unwrap_err();
        assert_eq!(err.max_depth, 10);
        assert_eq!(err.current_depth, 7);
        // Queue state is unchanged after the failed push (no partial
        // insertion).
        assert_eq!(q.len(), 7);
    }

    #[test]
    fn reenqueue_with_dedupe_keeps_highest_seq_per_node() {
        // F-002 fix: failed flush merges back with dedupe-by-node_id
        // keeping highest seq. Validates the dedupe is correct AND
        // bounds memory.
        let (_d, q) = fresh_queue(1000);
        // Pre-existing pending: node 1 at seq=5, node 2 at seq=6.
        q.try_push(vec![dummy_delta(5, 1), dummy_delta(6, 2)])
            .unwrap();

        // Failed batch coming back from PG: node 1 at seq=2 (older),
        // node 3 at seq=3 (new), node 2 at seq=8 (newer than current).
        let failed = vec![dummy_delta(2, 1), dummy_delta(3, 3), dummy_delta(8, 2)];
        q.reenqueue_with_dedupe(failed);

        // After dedupe: 3 unique nodes, with their respective highest seqs:
        //  node 1 → max(5, 2) = 5
        //  node 2 → max(6, 8) = 8
        //  node 3 → 3
        let (drained, max_seq) = q.drain_batch();
        assert_eq!(drained.len(), 3);
        assert_eq!(max_seq, 8);
        let by_node: std::collections::HashMap<Uuid, u64> =
            drained.iter().map(|d| (d.node_id, d.seq)).collect();
        assert_eq!(by_node[&Uuid::from_u128(1)], 5);
        assert_eq!(by_node[&Uuid::from_u128(2)], 8);
        assert_eq!(by_node[&Uuid::from_u128(3)], 3);
    }
}

// `flush_to_postgres` (one-shot connect-per-flush) was replaced by
// PgFlushClient above (Cluster D, F-012). The new client caches the
// connection + prepared statement and explicitly uses REPEATABLE READ
// isolation (F-024).
