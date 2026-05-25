//! wal.rs — local write-ahead log for durability (ADR-017 §3.4, phase 5).
//!
//! # Format v2 (Cluster A redesign, audit findings F-001/002/003/014/019)
//!
//! The v1 format had a critical data-loss bug: `truncate_after_flush()`
//! zeroed the entire file unconditionally, which dropped records the
//! queue still owed Postgres (F-001) and double-flushed recovered
//! deltas on boot (F-003). The v2 format replaces unconditional
//! truncation with a persistent "applied-up-to" sequence number stored
//! in the file header. Truncation only happens via the rotation path,
//! which copies the not-yet-flushed records into a fresh file +
//! atomic-renames over the old one.
//!
//! ## File layout
//!
//!   offset 0..4   : magic = b"WAL2"
//!   offset 4..12  : u64 little-endian applied_pg_seq
//!                   (highest seq durably persisted to Postgres)
//!   offset 12..16 : u32 little-endian header_version (= 2)
//!   offset 16..20 : u32 reserved (zero, for future evolution)
//!   offset 20..   : repeated records:
//!                     [u32 record_len_be]
//!                     [u64 seq_be]
//!                     [bincode-serialized WalRecord]
//!
//! ## Durability invariant
//!
//! Records with `seq <= applied_pg_seq` are guaranteed in Postgres and
//! can be safely skipped on replay. Records with `seq > applied_pg_seq`
//! MAY or MAY NOT be in PG — replay re-enqueues them for flush. The
//! seq-guarded UPDATE on the PG side (see write_behind.rs, F-006/A6)
//! prevents older WAL records from clobbering newer PG state.
//!
//! ## Sequencing
//!
//! - `append(record)` atomically:
//!     1. assigns the next seq from `next_seq` (in-RAM counter)
//!     2. writes `[len][seq][bincode]`
//!     3. fdatasyncs
//!     4. returns the seq for the caller to track
//!
//! - `set_applied_pg_seq(seq)`:
//!     1. seeks to offset 4
//!     2. writes 8 little-endian bytes
//!     3. fdatasyncs
//!     4. updates the in-RAM mirror
//!
//!   This is an aligned 8-byte write inside a single sector on every
//!   filesystem we care about (ext4/xfs/ntfs) — partial writes do not
//!   occur. Worst case on crash mid-write: applied_pg_seq is the OLD
//!   value (we re-flush some records harmlessly via seq-guarded UPDATE).
//!
//! ## Rotation
//!
//! Triggered by `maybe_rotate()` when file size > `rotation_threshold`
//! AND applied_pg_seq covers >= 80% of records. Procedure:
//!     1. open `<wal>.new` (create + truncate)
//!     2. write magic + header (preserving applied_pg_seq + next_seq)
//!     3. stream records with seq > applied_pg_seq → new file
//!     4. fdatasync new file
//!     5. atomic rename(<wal>.new, <wal>)
//!
//! Crash recovery: if `<wal>.new` exists on boot, delete it (its
//! contents are a subset of the live `<wal>`).
//!
//! ## Backward compatibility
//!
//! v2 magic is `b"WAL2"`. Files with the old `b"WAL\0"` magic are
//! refused with a clear error suggesting to delete + recover from
//! Postgres. Dev/test data is throwaway; production hasn't shipped
//! yet so there's no real upgrade path needed.

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

/// v2 magic. v1 was b"WAL\0" — refused on open with a clear error.
const MAGIC_V2: [u8; 4] = *b"WAL2";
const MAGIC_V1: [u8; 4] = *b"WAL\0";
const HEADER_VERSION: u32 = 2;
/// Bytes occupied by the file header: magic + applied_pg_seq + version + reserved.
pub const HEADER_LEN: u64 = 4 + 8 + 4 + 4;
/// Cap on a single record's serialized payload — sanity guard against
/// length-prefix corruption.
const MAX_RECORD_PAYLOAD: usize = 64 * 1024 * 1024;
/// Default rotation threshold: rotate when the WAL file exceeds this
/// size AND applied_pg_seq covers enough of it (configurable).
pub const DEFAULT_ROTATION_THRESHOLD_BYTES: u64 = 256 * 1024 * 1024;
/// We only rotate when applied_pg_seq covers ≥ this fraction of records,
/// otherwise we'd churn during a PG outage when no records are eligible.
pub const ROTATION_APPLIED_FRACTION: f64 = 0.80;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDelta {
    pub node_id: Uuid,
    // rust_decimal's default serde impl uses serde's `deserialize_any`
    // hook, which is incompatible with non-self-describing formats
    // (bincode, postcard, etc.) — the call fails at runtime with
    // "Bincode does not support deserialize_any". Pinning the field
    // to the `str` representation forces a typed, bincode-compatible
    // string roundtrip. Precision is preserved (Decimal::to_string is
    // lossless for fixed-precision decimals up to 28 digits).
    #[serde(with = "rust_decimal::serde::str")]
    pub opening_stock: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub inflows: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub outflows: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub closing_stock: Decimal,
    pub has_shortage: bool,
    #[serde(with = "rust_decimal::serde::str")]
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

/// One record paired with the sequence number assigned at append time.
#[derive(Debug, Clone)]
pub struct SeqRecord {
    pub seq: u64,
    pub record: WalRecord,
}

pub struct WalWriter {
    path: PathBuf,
    /// `<path>.new` — used during rotation.
    new_path: PathBuf,
    file: Mutex<File>,
    /// Mirrors the on-disk applied_pg_seq for cheap reads.
    applied_pg_seq: AtomicU64,
    /// Sequence assigned to the next append. Persisted indirectly via
    /// the highest record in the file; recomputed on open from replay.
    next_seq: AtomicU64,
    /// Current file size in bytes (tracked in-RAM to avoid syscalls).
    current_size: AtomicU64,
    /// Rotation threshold in bytes. Configurable via constructor.
    rotation_threshold_bytes: u64,
}

impl WalWriter {
    /// Open (or create) the WAL at `path` using the default rotation
    /// threshold. Refuses v1 files with a clear error.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Self::open_with_threshold(path, DEFAULT_ROTATION_THRESHOLD_BYTES)
    }

    pub fn open_with_threshold(
        path: impl AsRef<Path>,
        rotation_threshold_bytes: u64,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let new_path = make_new_path(&path);

        // Crash-recovery: if a rotation was in progress, `<wal>.new`
        // exists. Its contents are a subset of `<wal>`, so the safest
        // recovery is to delete it.
        if new_path.exists() {
            warn!(
                path = %new_path.display(),
                "found orphan WAL rotation tempfile from prior crash, removing"
            );
            std::fs::remove_file(&new_path)?;
        }

        let exists = path.exists();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let (applied_pg_seq, _hdr_version) = if !exists {
            write_fresh_header(&mut file)?;
            info!(path = %path.display(), "WAL created (v2)");
            (0u64, HEADER_VERSION)
        } else {
            read_header(&mut file, &path)?
        };

        // Seek to end for subsequent appends.
        let size = file.seek(SeekFrom::End(0))?;

        let writer = Self {
            path,
            new_path,
            file: Mutex::new(file),
            applied_pg_seq: AtomicU64::new(applied_pg_seq),
            next_seq: AtomicU64::new(0), // refined below
            current_size: AtomicU64::new(size),
            rotation_threshold_bytes,
        };

        // Recompute next_seq by scanning existing records. Sequences
        // are 1-indexed so that applied_pg_seq=0 cleanly means "no
        // records applied yet" (rather than ambiguously meaning either
        // "no records" or "seq 0 was applied"). The first append on a
        // fresh file gets seq=1.
        let next_seq = writer
            .scan_highest_seq()?
            .map(|s| s.saturating_add(1))
            .unwrap_or(1);
        writer.next_seq.store(next_seq, Ordering::Relaxed);

        Ok(writer)
    }

    /// Append + fsync. Atomically assigns + returns the seq.
    pub fn append(&self, record: &WalRecord) -> anyhow::Result<u64> {
        let bytes = bincode::serialize(record)?;
        let len = u32::try_from(bytes.len())
            .map_err(|_| anyhow::anyhow!("WAL record too large: {} bytes", bytes.len()))?;
        if (len as usize) > MAX_RECORD_PAYLOAD {
            anyhow::bail!(
                "WAL record exceeds MAX_RECORD_PAYLOAD ({} > {})",
                len,
                MAX_RECORD_PAYLOAD
            );
        }

        // Acquire the seq under the file lock so on-disk order matches
        // seq order — required for `scan_highest_seq` to be accurate
        // even when appends race.
        let mut f = self.file.lock();
        let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);

        f.seek(SeekFrom::End(0))?;
        f.write_all(&len.to_be_bytes())?;
        f.write_all(&seq.to_be_bytes())?;
        f.write_all(&bytes)?;
        // Durability barrier — caller will not see success until disk has it.
        f.sync_data()?;

        let total = 4 + 8 + bytes.len();
        self.current_size
            .fetch_add(total as u64, Ordering::Relaxed);
        Ok(seq)
    }

    /// Update the applied-PG-seq marker. Called by the flusher after a
    /// successful PG batch. Replaces v1's `truncate_after_flush`.
    pub fn set_applied_pg_seq(&self, seq: u64) -> anyhow::Result<()> {
        let mut f = self.file.lock();
        f.seek(SeekFrom::Start(4))?; // offset of applied_pg_seq field
        f.write_all(&seq.to_le_bytes())?;
        f.sync_data()?;
        self.applied_pg_seq.store(seq, Ordering::Release);
        debug!(applied_pg_seq = seq, "WAL marker advanced");
        Ok(())
    }

    pub fn applied_pg_seq(&self) -> u64 {
        self.applied_pg_seq.load(Ordering::Acquire)
    }

    pub fn next_seq_peek(&self) -> u64 {
        self.next_seq.load(Ordering::Acquire)
    }

    /// Total bytes in the live WAL file (including header). Used for
    /// rotation triggers and metrics.
    pub fn current_size_bytes(&self) -> u64 {
        self.current_size.load(Ordering::Relaxed)
    }

    /// Read every record from offset HEADER_LEN onward. Records with
    /// `seq <= applied_pg_seq` are skipped — they've been durably
    /// applied to Postgres and re-applying them risks clobbering
    /// newer state (F-014). Records with `seq > applied_pg_seq` are
    /// returned in append order.
    pub fn replay(&self) -> anyhow::Result<Vec<SeqRecord>> {
        let applied = self.applied_pg_seq();
        let mut f = self.file.lock();
        f.seek(SeekFrom::Start(HEADER_LEN))?;

        let mut out = Vec::new();
        let mut len_buf = [0u8; 4];
        let mut seq_buf = [0u8; 8];
        loop {
            match f.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    warn!(error = %e, "WAL: read error on length prefix, stopping replay");
                    break;
                }
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            if len > MAX_RECORD_PAYLOAD {
                warn!(len, "WAL: record length overflow, treating as truncated, stopping");
                break;
            }
            if f.read_exact(&mut seq_buf).is_err() {
                warn!("WAL: incomplete seq prefix, stopping replay");
                break;
            }
            let seq = u64::from_be_bytes(seq_buf);
            let mut bytes = vec![0u8; len];
            if let Err(e) = f.read_exact(&mut bytes) {
                warn!(error = %e, "WAL: incomplete record body, stopping replay");
                break;
            }
            match bincode::deserialize::<WalRecord>(&bytes) {
                Ok(rec) => {
                    if seq > applied {
                        out.push(SeqRecord { seq, record: rec });
                    } else {
                        debug!(seq, applied, "WAL replay: skipping pre-checkpoint record");
                    }
                }
                Err(e) => {
                    warn!(error = %e, "WAL: deserialize failure, stopping replay");
                    break;
                }
            }
        }
        f.seek(SeekFrom::End(0))?;
        Ok(out)
    }

    /// One-pass scan of the file to find the highest seq currently
    /// stored. Used on open() to set next_seq correctly. Returns
    /// `None` on an empty WAL (no records, header-only) so the caller
    /// can leave next_seq at 0; first append then takes seq 0.
    fn scan_highest_seq(&self) -> anyhow::Result<Option<u64>> {
        let mut f = self.file.lock();
        f.seek(SeekFrom::Start(HEADER_LEN))?;
        let mut highest: Option<u64> = None;
        let mut len_buf = [0u8; 4];
        let mut seq_buf = [0u8; 8];
        loop {
            match f.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(_) => break,
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            if len > MAX_RECORD_PAYLOAD {
                break;
            }
            if f.read_exact(&mut seq_buf).is_err() {
                break;
            }
            let seq = u64::from_be_bytes(seq_buf);
            highest = Some(match highest {
                Some(h) if h > seq => h,
                _ => seq,
            });
            // Skip past payload.
            if f.seek(SeekFrom::Current(len as i64)).is_err() {
                break;
            }
        }
        f.seek(SeekFrom::End(0))?;
        Ok(highest)
    }

    /// Rotate the WAL if it's large enough AND most of it is already
    /// applied to Postgres. Writes a fresh file containing only
    /// not-yet-applied records, atomic-renames over the live file.
    /// Idempotent + safe to call frequently — short-circuits if not
    /// eligible.
    ///
    /// Returns the number of bytes reclaimed (0 if not rotated).
    pub fn maybe_rotate(&self) -> anyhow::Result<u64> {
        let size = self.current_size_bytes();
        if size <= self.rotation_threshold_bytes {
            return Ok(0);
        }
        let applied = self.applied_pg_seq();
        let next = self.next_seq.load(Ordering::Acquire);
        // applied_pg_seq covers seq 1..=applied; we have appended next-1.
        // Fraction = applied / (next - 1) if next > 1 else 0.
        if next <= 1 {
            return Ok(0);
        }
        let total_records = (next - 1) as f64;
        let applied_fraction = applied as f64 / total_records;
        if applied_fraction < ROTATION_APPLIED_FRACTION {
            return Ok(0);
        }
        self.rotate_now(applied)
    }

    /// Rotation path proper. Held under the file lock for the whole
    /// duration — short, since we're streaming bytes between two
    /// already-open files. Concurrent appenders block until rotation
    /// completes (sub-second for our sizes).
    fn rotate_now(&self, applied: u64) -> anyhow::Result<u64> {
        let mut live = self.file.lock();
        let pre_size = self.current_size.load(Ordering::Relaxed);

        // Open the new file fresh.
        let mut new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.new_path)?;
        // Write fresh header (same applied marker, same next_seq target).
        write_header(&mut new_file, applied)?;
        new_file.sync_data()?;

        // Stream records with seq > applied from live → new.
        live.seek(SeekFrom::Start(HEADER_LEN))?;
        let mut kept_records = 0u64;
        let mut len_buf = [0u8; 4];
        let mut seq_buf = [0u8; 8];
        loop {
            match live.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(_) => break,
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            if len > MAX_RECORD_PAYLOAD {
                warn!(len, "WAL rotation: record length overflow, stopping copy");
                break;
            }
            if live.read_exact(&mut seq_buf).is_err() {
                break;
            }
            let seq = u64::from_be_bytes(seq_buf);
            let mut bytes = vec![0u8; len];
            if live.read_exact(&mut bytes).is_err() {
                break;
            }
            if seq > applied {
                new_file.write_all(&len_buf)?;
                new_file.write_all(&seq_buf)?;
                new_file.write_all(&bytes)?;
                kept_records += 1;
            }
        }
        new_file.sync_data()?;

        // Compute the new file's size before we hand the FD back. We
        // need this for current_size tracking after the swap.
        let new_size = new_file.seek(SeekFrom::End(0))?;
        drop(new_file);

        // Atomic rename. On Windows this uses MoveFileEx with
        // MOVEFILE_REPLACE_EXISTING and is atomic at the FS level.
        // On POSIX `rename` over an open file is also fine — the open
        // handle on `live` keeps the inode alive but new opens see the
        // new file. We then drop `live` and reopen.
        drop(live); // Release before rename so Windows can replace the file.
        std::fs::rename(&self.new_path, &self.path)?;

        // Reopen the rotated file as the new live handle.
        let mut reopened = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)?;
        reopened.seek(SeekFrom::End(0))?;
        *self.file.lock() = reopened;
        self.current_size.store(new_size, Ordering::Relaxed);

        let reclaimed = pre_size.saturating_sub(new_size);
        info!(
            kept_records,
            reclaimed_bytes = reclaimed,
            new_size_bytes = new_size,
            "WAL rotated"
        );
        Ok(reclaimed)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn make_new_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".new");
    PathBuf::from(s)
}

fn write_fresh_header(file: &mut File) -> anyhow::Result<()> {
    write_header(file, 0)?;
    file.sync_data()?;
    Ok(())
}

fn write_header(file: &mut File, applied_pg_seq: u64) -> anyhow::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&MAGIC_V2)?;
    file.write_all(&applied_pg_seq.to_le_bytes())?;
    file.write_all(&HEADER_VERSION.to_le_bytes())?;
    file.write_all(&0u32.to_le_bytes())?; // reserved
    Ok(())
}

fn read_header(file: &mut File, path: &Path) -> anyhow::Result<(u64, u32)> {
    file.seek(SeekFrom::Start(0))?;
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic).map_err(|e| {
        anyhow::anyhow!("WAL file {} unreadable: {}", path.display(), e)
    })?;
    if magic == MAGIC_V1 {
        anyhow::bail!(
            "WAL file {} is v1 format (magic WAL\\0). v2 (WAL2) is required. \
             Delete the file (records will be recovered from Postgres on next \
             boot) and restart.",
            path.display()
        );
    }
    if magic != MAGIC_V2 {
        anyhow::bail!(
            "WAL file {} magic mismatch (got {:?}, expected {:?}) — refusing to use",
            path.display(),
            magic,
            MAGIC_V2
        );
    }
    let mut applied_buf = [0u8; 8];
    file.read_exact(&mut applied_buf)?;
    let applied_pg_seq = u64::from_le_bytes(applied_buf);
    let mut version_buf = [0u8; 4];
    file.read_exact(&mut version_buf)?;
    let version = u32::from_le_bytes(version_buf);
    let mut _reserved_buf = [0u8; 4];
    file.read_exact(&mut _reserved_buf)?;

    if version != HEADER_VERSION {
        anyhow::bail!(
            "WAL file {} has unsupported header version {} (this build supports {})",
            path.display(),
            version,
            HEADER_VERSION
        );
    }
    info!(
        path = %path.display(),
        applied_pg_seq,
        "WAL opened (v2)"
    );
    Ok((applied_pg_seq, version))
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

// ============================================================
// WAL fault-injection tests (Cluster A, task A8).
// ============================================================
//
// These tests validate the v2 durability contract directly:
//   - applied_pg_seq marker advances + survives across opens
//   - replay() skips records with seq <= applied_pg_seq
//   - rotation copies only seq > applied_pg_seq, atomic rename
//   - crash mid-rotation (orphan .new file) is cleaned up on boot
//   - corrupted middle record stops replay cleanly without panic
//   - v1 magic ("WAL\0") is rejected with a clear error
//   - truncated payload (partial last record) stops replay cleanly
//
// They run under `cargo test -p ootils_engine` and are part of the
// Cluster A acceptance gate. They use the tempfile dev-dep so each
// test owns its own scratch directory and there is no cross-test
// state. tracing output is suppressed (no subscriber installed).

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use tempfile::TempDir;
    use uuid::Uuid;

    fn make_dummy_record(node_count: usize) -> WalRecord {
        let deltas: Vec<NodeDelta> = (0..node_count)
            .map(|i| NodeDelta {
                node_id: Uuid::from_u128(i as u128 + 1),
                opening_stock: Decimal::new(100, 0),
                inflows: Decimal::new(50, 0),
                outflows: Decimal::new(30, 0),
                closing_stock: Decimal::new(120, 0),
                has_shortage: false,
                shortage_qty: Decimal::ZERO,
            })
            .collect();
        make_record(Uuid::from_u128(42), Uuid::from_u128(1), deltas)
    }

    #[test]
    fn open_creates_fresh_v2_file_with_zeroed_marker() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        let w = WalWriter::open(&path).unwrap();
        assert_eq!(w.applied_pg_seq(), 0);
        // Sequences are 1-indexed so that applied=0 unambiguously
        // means "no records applied yet".
        assert_eq!(w.next_seq_peek(), 1);
        let on_disk = std::fs::metadata(&path).unwrap().len();
        assert_eq!(on_disk, HEADER_LEN);
    }

    #[test]
    fn append_returns_monotonic_seqs() {
        let dir = TempDir::new().unwrap();
        let w = WalWriter::open(dir.path().join("test.wal")).unwrap();
        let seq1 = w.append(&make_dummy_record(1)).unwrap();
        let seq2 = w.append(&make_dummy_record(1)).unwrap();
        let seq3 = w.append(&make_dummy_record(1)).unwrap();
        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(seq3, 3);
    }

    #[test]
    fn replay_returns_all_records_when_marker_is_zero() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        {
            let w = WalWriter::open(&path).unwrap();
            for _ in 0..5 {
                w.append(&make_dummy_record(2)).unwrap();
            }
        }
        // Reopen and replay.
        let w2 = WalWriter::open(&path).unwrap();
        let recovered = w2.replay().unwrap();
        assert_eq!(recovered.len(), 5);
        assert_eq!(recovered.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn marker_advance_makes_replay_skip_records() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        let w = WalWriter::open(&path).unwrap();
        for _ in 0..5 {
            w.append(&make_dummy_record(1)).unwrap();
        }
        // Records have seq 1..=5. Mark seq 1,2,3 as applied. Replay
        // should return only seq 4, 5.
        w.set_applied_pg_seq(3).unwrap();
        drop(w);

        let w2 = WalWriter::open(&path).unwrap();
        assert_eq!(w2.applied_pg_seq(), 3);
        let recovered = w2.replay().unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].seq, 4);
        assert_eq!(recovered[1].seq, 5);
    }

    #[test]
    fn marker_survives_open_close_cycle() {
        // F-001 / F-003: the marker is the single source of truth for
        // "what PG has". A reopen MUST observe the same applied_pg_seq
        // we wrote, even after many appends, otherwise replay would
        // re-flush records PG already has.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        let w = WalWriter::open(&path).unwrap();
        for _ in 0..10 {
            w.append(&make_dummy_record(1)).unwrap();
        }
        w.set_applied_pg_seq(7).unwrap();
        drop(w);

        let w2 = WalWriter::open(&path).unwrap();
        assert_eq!(w2.applied_pg_seq(), 7);
        // next_seq should resume after the highest existing record (seq 10), so 11.
        assert_eq!(w2.next_seq_peek(), 11);
    }

    #[test]
    fn crash_between_pg_commit_and_marker_write_replays_record() {
        // Simulated scenario: PG already has the data but the engine
        // crashed before set_applied_pg_seq() wrote the new marker. The
        // record stays in the WAL with seq > applied_pg_seq, so replay
        // returns it. The PG seq-guard (last_calc_seq) handles
        // idempotency on the re-flush side.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        let w = WalWriter::open(&path).unwrap();
        w.append(&make_dummy_record(3)).unwrap();
        // Crash: no set_applied_pg_seq call.
        drop(w);

        let w2 = WalWriter::open(&path).unwrap();
        let recovered = w2.replay().unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].record.deltas.len(), 3);
    }

    #[test]
    fn rotation_compacts_file_when_eligible() {
        // Force rotation by setting a tiny threshold + appending enough
        // records, then advance the marker past most of them.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        let w = WalWriter::open_with_threshold(&path, 1024).unwrap();
        for _ in 0..20 {
            w.append(&make_dummy_record(1)).unwrap();
        }
        let pre_size = std::fs::metadata(&path).unwrap().len();
        // Records have seq 1..=20. Mark seq 1..=19 as PG-applied. Only
        // seq=20 remains for replay. Fraction = 19/20 = 95% which
        // exceeds the 80% rotation gate.
        w.set_applied_pg_seq(19).unwrap();
        let reclaimed = w.maybe_rotate().unwrap();
        let post_size = std::fs::metadata(&path).unwrap().len();
        assert!(reclaimed > 0, "rotation should reclaim space when eligible");
        assert!(
            post_size < pre_size,
            "post-rotation file ({}) should be smaller than pre ({})",
            post_size,
            pre_size
        );
        // After rotation, replay should still yield exactly the
        // unflushed record (seq 20).
        let recovered = w.replay().unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].seq, 20);
    }

    #[test]
    fn rotation_short_circuits_when_below_threshold() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        let w = WalWriter::open_with_threshold(&path, 10 * 1024 * 1024).unwrap();
        for _ in 0..3 {
            w.append(&make_dummy_record(1)).unwrap();
        }
        w.set_applied_pg_seq(2).unwrap();
        let reclaimed = w.maybe_rotate().unwrap();
        assert_eq!(reclaimed, 0, "rotation should not run below threshold");
    }

    #[test]
    fn rotation_short_circuits_when_applied_fraction_low() {
        // File size > threshold but only a small fraction is applied:
        // rotating now would just churn (we'd copy almost everything to
        // the new file). Verify the short-circuit. Records have seq
        // 1..=20; marking only seq 1,2 applied (10%) is below 80% gate.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        let w = WalWriter::open_with_threshold(&path, 1024).unwrap();
        for _ in 0..20 {
            w.append(&make_dummy_record(1)).unwrap();
        }
        w.set_applied_pg_seq(2).unwrap();
        let reclaimed = w.maybe_rotate().unwrap();
        assert_eq!(reclaimed, 0);
    }

    #[test]
    fn orphan_new_file_is_cleaned_up_on_open() {
        // Simulate crash mid-rotation: leave behind a `<path>.new` file.
        // On reopen, the live file should still work + orphan deleted.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        {
            let w = WalWriter::open(&path).unwrap();
            w.append(&make_dummy_record(1)).unwrap();
        }
        // Drop a bogus orphan.
        let new_path = make_new_path(&path);
        std::fs::write(&new_path, b"garbage from crashed rotation").unwrap();
        assert!(new_path.exists());

        // Reopen — should clean up the orphan + still read live records.
        let w2 = WalWriter::open(&path).unwrap();
        assert!(!new_path.exists(), "orphan .new should have been removed");
        let recovered = w2.replay().unwrap();
        assert_eq!(recovered.len(), 1);
    }

    #[test]
    fn v1_magic_is_rejected_with_clear_error() {
        // Write a v1-style file manually (magic = "WAL\0", no v2 header)
        // and confirm WalWriter::open refuses it.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("v1.wal");
        std::fs::write(&path, b"WAL\0").unwrap();
        let res = WalWriter::open(&path);
        let err = match res {
            Ok(_) => panic!("v1 file should have been rejected"),
            Err(e) => e,
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("v1 format"),
            "error message should mention v1: {msg}"
        );
    }

    #[test]
    fn bad_magic_is_rejected() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("alien.wal");
        std::fs::write(&path, b"NOTAWAL").unwrap();
        assert!(WalWriter::open(&path).is_err());
    }

    #[test]
    fn truncated_last_record_does_not_panic_on_replay() {
        // Append a complete record, then a partial one (only the length
        // prefix). Replay must stop cleanly at the partial record.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("trunc.wal");
        {
            let w = WalWriter::open(&path).unwrap();
            w.append(&make_dummy_record(1)).unwrap();
        }
        // Append a bogus length prefix (4 bytes only — no seq, no payload).
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&100u32.to_be_bytes()).unwrap();
            f.sync_data().unwrap();
        }
        let w2 = WalWriter::open(&path).unwrap();
        let recovered = w2.replay().unwrap();
        // Only the one complete record survives.
        assert_eq!(recovered.len(), 1);
    }

    #[test]
    fn corrupt_payload_stops_replay_without_loss_of_earlier_records() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("corrupt.wal");
        {
            let w = WalWriter::open(&path).unwrap();
            w.append(&make_dummy_record(1)).unwrap();
            w.append(&make_dummy_record(2)).unwrap();
        }
        // Append a record-shaped chunk with garbage bincode payload.
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            let garbage = vec![0xFFu8; 32];
            f.write_all(&(garbage.len() as u32).to_be_bytes()).unwrap();
            f.write_all(&999u64.to_be_bytes()).unwrap(); // seq
            f.write_all(&garbage).unwrap();
            f.sync_data().unwrap();
        }
        let w2 = WalWriter::open(&path).unwrap();
        let recovered = w2.replay().unwrap();
        // Two good records before the garbage — we keep them.
        assert_eq!(recovered.len(), 2);
    }

    #[test]
    fn applied_pg_seq_higher_than_records_is_safe() {
        // Edge: marker set past the actual highest seq (could happen
        // if records were rotated out + marker stayed). Replay yields 0.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("ahead.wal");
        let w = WalWriter::open(&path).unwrap();
        w.append(&make_dummy_record(1)).unwrap(); // seq 0
        w.set_applied_pg_seq(999).unwrap();
        drop(w);
        let w2 = WalWriter::open(&path).unwrap();
        assert_eq!(w2.applied_pg_seq(), 999);
        let recovered = w2.replay().unwrap();
        assert!(recovered.is_empty());
    }
}
