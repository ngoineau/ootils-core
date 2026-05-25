//! pool.rs — process-wide persistent Postgres connection cache.
//!
//! Why: opening a Postgres connection (TCP + protocol + auth) takes
//! 10-30ms on LAN. For incremental events that need ~80-150ms total
//! latency, paying this per call is 10-30% of the budget. By keeping
//! the connection alive across `propagate_and_write` calls we amortize
//! that cost to zero for all but the first call.
//!
//! Concurrency model:
//! - One process-wide `OnceCell<Mutex<Option<Client>>>` keyed by DSN.
//!   In practice we only ever have one DSN per process so one entry
//!   is enough. A `HashMap<String, Mutex<Client>>` is overkill for
//!   our deployment shape.
//! - The Mutex serializes propagation calls on the same connection.
//!   Postgres doesn't allow concurrent queries on one session anyway,
//!   so this matches the underlying constraint.
//! - If the connection dies (server restart, network blip), the
//!   `get_or_open` helper reconnects transparently.

use postgres::{Client, NoTls};
use std::sync::Mutex;

use once_cell::sync::OnceCell;

/// Singleton lock around the cached client. `None` = not yet opened or
/// previously poisoned and dropped.
static POOL: OnceCell<Mutex<PoolEntry>> = OnceCell::new();

struct PoolEntry {
    /// The current cached client, or `None` if we need to reconnect.
    client: Option<Client>,
    /// The DSN this client was opened against — we re-open if it
    /// changes (shouldn't happen in a single process, but cheap to
    /// guard against).
    dsn: String,
}

/// Acquire an exclusive handle to a Postgres client open against `dsn`.
/// Opens lazily on first call; reuses the existing client on
/// subsequent calls; reconnects automatically if the cached client
/// errors out (caller will see the new error from the retry).
///
/// The closure receives `&mut Client` and may issue queries. When the
/// closure returns, the client is parked back in the pool, ready for
/// the next caller.
pub fn with_client<R>(
    dsn: &str,
    f: impl FnOnce(&mut Client) -> Result<R, postgres::Error>,
) -> Result<R, postgres::Error> {
    let entry_lock = POOL.get_or_init(|| {
        Mutex::new(PoolEntry {
            client: None,
            dsn: String::new(),
        })
    });

    // First try with the existing cached client.
    let mut entry = entry_lock.lock().expect("pool mutex poisoned");

    // If the cached DSN differs from what's requested, drop the old client.
    if entry.dsn != dsn {
        entry.client = None;
        entry.dsn = dsn.to_string();
    }

    // Lazy-open if missing.
    if entry.client.is_none() {
        entry.client = Some(Client::connect(dsn, NoTls)?);
    }

    // Run the closure. If it returns an error, drop the cached client
    // so the next call reconnects from scratch (a dead socket would
    // otherwise produce confusing failures on every subsequent call).
    let mut client = entry.client.take().expect("just inserted");
    let result = f(&mut client);
    if result.is_ok() {
        entry.client = Some(client);
    }
    // else: dropping `client` here closes the socket and forces a
    // fresh connection on the next call.
    result
}
