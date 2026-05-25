//! wal.rs — local write-ahead log for durability (ADR-017 §3.4, phase 5).
//!
//! Append-only file of `WalRecord` entries, each preceded by a length
//! prefix so we can stream-read on replay even if the file was
//! truncated mid-write (we stop at the last fully-readable record).
//!
//! Durability contract:
//! - Every `append()` calls `sync_data()` (POSIX fdatasync) before
//!   returning. Caller does not get an "OK" until the bytes are on disk.
//! - Checkpoint (`truncate_after_flush`) is called by the write-behind
//!   queue once a batch is fully flushed to Postgres. Records BEFORE
//!   the checkpoint are no longer needed for recovery.
//!
//! File format:
//!
//!   [u32 magic = 0x57414C00 "WAL\0"]      -- 4 bytes header (file open)
//!   [u32 record_len_be][bincode bytes]    -- one record
//!   [u32 record_len_be][bincode bytes]    -- another record
//!   ...
//!
//! On replay, if the last record's length prefix is short, OR the
//! bytes don't deserialize, we stop — assume the process died mid-write
//! and treat everything before as durable. Last-record loss is
//! acceptable because the propagator only acks AFTER fsync — i.e.,
//! the caller would have got an error.
//!
//! On Windows: std::fs gives us synchronous fsync.
//! On Linux: same (we can swap in compio + io_uring in phase 7 for
//!   async fsync if profile signals it's worth it; the std::fs path
//!   is already only ~5-10ms on NVMe).

use chrono::Utc;
use parking_lot::Mutex;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info, warn};
use uuid::Uuid;

const MAGIC: [u8; 4] = *b"WAL\0";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDelta {
    pub node_id: Uuid,
    pub opening_stock: Decimal,
    pub inflows: Decimal,
    pub outflows: Decimal,
    pub closing_stock: Decimal,
    pub has_shortage: bool,
    pub shortage_qty: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    /// Monotonic-ish ms since UNIX epoch.
    pub timestamp_ms: i64,
    /// Source calc_run for this batch (for joining with Postgres
    /// after recovery if needed).
    pub calc_run_id: Uuid,
    pub scenario_id: Uuid,
    pub deltas: Vec<NodeDelta>,
}

pub struct WalWriter {
    path: PathBuf,
    file: Mutex<File>,
    /// Total bytes appended since the last `truncate_after_flush()`.
    bytes_since_checkpoint: AtomicU64,
}

impl WalWriter {
    /// Open (or create) the WAL at `path`. If the file already exists
    /// without our magic header, we refuse to touch it — the caller
    /// is presumed to have meant something else and we don't want to
    /// corrupt unrelated data.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let exists = path.exists();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        if !exists {
            // New file — write the magic header.
            file.write_all(&MAGIC)?;
            file.sync_data()?;
            info!(path = %path.display(), "WAL created");
        } else {
            // Existing file — verify magic.
            let mut buf = [0u8; 4];
            file.seek(SeekFrom::Start(0))?;
            match file.read_exact(&mut buf) {
                Ok(_) if buf == MAGIC => {
                    info!(path = %path.display(), "WAL opened (existing)");
                }
                Ok(_) => {
                    anyhow::bail!(
                        "WAL file {} exists but magic mismatch (got {:?}, want {:?}) — refusing to overwrite",
                        path.display(),
                        buf,
                        MAGIC
                    );
                }
                Err(e) => anyhow::bail!("WAL file {} could not be read: {}", path.display(), e),
            }
            file.seek(SeekFrom::End(0))?;
        }

        Ok(Self {
            path,
            file: Mutex::new(file),
            bytes_since_checkpoint: AtomicU64::new(0),
        })
    }

    /// Append + fsync. Returns the bytes written for this record
    /// (length prefix + payload).
    pub fn append(&self, record: &WalRecord) -> anyhow::Result<usize> {
        let bytes = bincode::serialize(record)?;
        let len = bytes.len() as u32;

        let mut f = self.file.lock();
        f.write_all(&len.to_be_bytes())?;
        f.write_all(&bytes)?;
        // Durability barrier — caller will not see success until disk has it.
        f.sync_data()?;

        let total = 4 + bytes.len();
        self.bytes_since_checkpoint
            .fetch_add(total as u64, Ordering::Relaxed);
        Ok(total)
    }

    /// After the write-behind worker has flushed a batch to Postgres,
    /// truncate the WAL — the records are now durable in PG too.
    /// Truncation is atomic at the FS level on POSIX/NTFS.
    pub fn truncate_after_flush(&self) -> anyhow::Result<()> {
        let mut f = self.file.lock();
        // Seek to right after the magic header.
        f.seek(SeekFrom::Start(MAGIC.len() as u64))?;
        f.set_len(MAGIC.len() as u64)?;
        f.sync_data()?;
        let prior = self
            .bytes_since_checkpoint
            .swap(0, Ordering::Relaxed);
        debug!(prior_bytes = prior, "WAL truncated after flush");
        Ok(())
    }

    pub fn current_size_bytes(&self) -> u64 {
        self.bytes_since_checkpoint.load(Ordering::Relaxed)
    }

    /// Read every fully-readable record from the file. Stops cleanly at
    /// the first truncated / corrupt record (treated as "died mid-write")
    /// and returns everything before.
    pub fn replay(&self) -> anyhow::Result<Vec<WalRecord>> {
        let mut f = self.file.lock();
        f.seek(SeekFrom::Start(0))?;
        let mut magic = [0u8; 4];
        if f.read_exact(&mut magic).is_err() || magic != MAGIC {
            // Empty or bad file — treat as no records (we already
            // verified magic in `open`; this branch is for replay
            // from a totally fresh file).
            f.seek(SeekFrom::End(0))?;
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        let mut len_buf = [0u8; 4];
        loop {
            // Try to read the length prefix.
            match f.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Clean EOF — done.
                    break;
                }
                Err(e) => {
                    warn!(error = %e, "WAL: read error on length prefix, stopping replay");
                    break;
                }
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            // Sanity cap — refuse anything over 64 MB (a single record).
            if len > 64 * 1024 * 1024 {
                warn!(len, "WAL: record length overflow, treating as truncated, stopping");
                break;
            }
            let mut bytes = vec![0u8; len];
            match f.read_exact(&mut bytes) {
                Ok(_) => {}
                Err(e) => {
                    warn!(error = %e, "WAL: incomplete record body, stopping replay");
                    break;
                }
            }
            match bincode::deserialize::<WalRecord>(&bytes) {
                Ok(rec) => out.push(rec),
                Err(e) => {
                    warn!(error = %e, "WAL: deserialize failure, stopping replay");
                    break;
                }
            }
        }
        // Position the file at end-of-file for subsequent appends.
        f.seek(SeekFrom::End(0))?;
        Ok(out)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Helper to build a `WalRecord` with timestamp filled in.
pub fn make_record(
    calc_run_id: Uuid,
    scenario_id: Uuid,
    deltas: Vec<NodeDelta>,
) -> WalRecord {
    WalRecord {
        timestamp_ms: Utc::now().timestamp_millis(),
        calc_run_id,
        scenario_id,
        deltas,
    }
}

// WAL unit tests live in `tests/wal_recovery.rs` (integration tests) — they
// require a real filesystem and span open/append/replay roundtrips that
// are clearer to write as end-to-end scenarios. Phase 5 validates the
// WAL via the engine's bench / live process kill-9 procedure, not via
// inline unit tests.
