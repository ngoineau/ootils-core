"""
propagator_rust.py — Rust-backed propagation engine (ADR-016 §week 4).

Inherits the Python `PropagationEngine` for everything except the
`_propagate` hot path, which is delegated to the Rust extension module
`ootils_kernel`. The Rust side does:

  1. Load dirty subgraph (PIs + supplies + demands + seed openings)
  2. Compute every PI in memory (parity-validated week 3)
  3. UNNEST/COPY the projection + UPDATE FROM (does NOT clear dirty_nodes —
     see `_propagate_via_rust` below)

Shortage *detection* (safety-stock vs. closing_stock, persisted to the
`shortages` table) stays in SQL — the wrapper calls SHORTAGES_SQL from
`propagator_sql` after the Rust pass finishes, then clears `dirty_nodes`.
Same contract, same shortage rows.

Boundary:
- Python keeps the calc_run lifecycle, advisory lock, scenario state
  machine, agent tools, FastAPI routes — everything that changes often.
- Rust owns the read + compute + bulk writeback — the stable hot path.
- Rust's Postgres session is separate from Python's `db` connection, and
  the connection credential (password) is passed explicitly on every call
  — never via a `PGPASSWORD` environment variable (that was racy: process
  env is shared mutable state across concurrent requests). See
  `_propagate_via_rust` for the full boundary-commit + failure contract.
"""
from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING
from uuid import UUID


try:
    import ootils_kernel
except ImportError as _exc:  # pragma: no cover — guarded at construction
    ootils_kernel = None
    _IMPORT_ERROR: ImportError | None = _exc
else:
    _IMPORT_ERROR = None

from ootils_core.db.types import DictRowConnection
from ootils_core.engine.orchestration.propagator import PropagationEngine
from ootils_core.engine.orchestration.propagator_sql import (
    CLEAR_DIRTY_SQL,
    PROPAGATE_SQL,
    SHORTAGES_SQL,
)

if TYPE_CHECKING:
    from ootils_core.models import CalcRun

logger = logging.getLogger(__name__)


# Crossover point: below this many dirty PIs, fall back to the SQL
# inline path.
#
# HISTORY / CAUTION: an earlier version of this comment claimed "Rust
# wins at every scale (full prop L: 9.5s vs 36.8s)". The 36.8s SQL
# figure was measured against a SQL engine with STALE dirty_nodes stats
# — the O(N²) nested-loop pathology fixed in #455 (ANALYZE dirty_nodes
# in flush_to_postgres). The 2026-07-11 VM re-bench with the fix live
# shows SQL and Rust are within ±4% end-to-end on profiles S/M/L (SQL
# even wins on L: 16.3s vs 16.8s), because propagation wall time is
# dominated by the Python/SQL orchestration around the kernel (shortage
# persistence, calc_run, resolve_stale), not the per-node compute — the
# Rust kernel itself is fast (0.8s for 111k PIs engine-direct) but that
# is ~7.5% of the wall. See docs/PERF-BASELINE.md (§ re-bench 2026-07-11).
# In-process Rust is therefore NOT a decisive speedup over healthy SQL;
# the real Rust lever is the rust-svc in-RAM architecture (SCALE-2).
#
# Default 0 = use Rust whenever OOTILS_ENGINE=rust is opted into. Set
# OOTILS_RUST_MIN_DIRTY > 0 to force the SQL fallback below a threshold.
RUST_DISPATCH_THRESHOLD = int(os.environ.get("OOTILS_RUST_MIN_DIRTY", "0"))


class RustPropagationEngine(PropagationEngine):
    """
    Drop-in replacement for `PropagationEngine._propagate` that delegates
    to the Rust extension. The rest (dirty cascade, calc_run lifecycle,
    advisory lock, finish_run, stale-shortage resolution) is inherited
    unchanged.
    """

    def __init__(self, *args, **kwargs) -> None:
        if ootils_kernel is None:
            raise RuntimeError(
                "RustPropagationEngine requires the `ootils_kernel` extension "
                f"to be installed. ImportError was: {_IMPORT_ERROR!r}. Build it "
                "with `cd rust/ootils_kernel && maturin build --release` then "
                "pip install the produced wheel."
            )
        super().__init__(*args, **kwargs)

    def _propagate(
        self,
        calc_run: "CalcRun",
        dirty_nodes: set[UUID],
        db: DictRowConnection,
    ) -> None:
        """
        Hybrid dispatch:
        - Small dirty sets (< RUST_DISPATCH_THRESHOLD): use the SQL engine's
          single-statement PROPAGATE_SQL. Lower per-call overhead.
        - Large dirty sets (full propagation, scenario fork): delegate to
          the Rust path (load + compute + bulk COPY writeback).

        Then run shortage detection in SQL either way.
        """
        if not dirty_nodes:
            return

        if len(dirty_nodes) < RUST_DISPATCH_THRESHOLD:
            # Small subgraph — SQL beats Rust here (one UPDATE in-process
            # vs 4 SELECT + COPY + UPDATE + DELETE roundtrips).
            self._propagate_via_sql(calc_run, db)
            return

        # Large subgraph — Rust beats SQL by 3-5× thanks to bulk COPY.
        self._propagate_via_rust(calc_run, db)

    def _propagate_via_sql(self, calc_run: "CalcRun", db: DictRowConnection) -> None:
        """SQL-engine fallback for small dirty sets. Same SQL strings."""
        params = {
            "scenario_id": calc_run.scenario_id,
            "calc_run_id": calc_run.calc_run_id,
        }
        cur = db.execute(PROPAGATE_SQL, params)
        calc_run.nodes_recalculated += cur.rowcount or 0
        if self._shortage_detector is not None:
            db.execute(SHORTAGES_SQL, params)
        db.execute(CLEAR_DIRTY_SQL, params)

    def _propagate_via_rust(self, calc_run: "CalcRun", db: DictRowConnection) -> None:
        """Delegate the heavy lifting to ootils_kernel.

        Boundary-commit contract (why this commits mid-request):
        `ootils_kernel.propagate_and_write` opens its OWN, separate
        Postgres session (process-wide cached connection, see
        rust/ootils_kernel/src/pool.rs) — it is NOT `db`. That Rust
        session reads the dirty subgraph this request just built (the
        `events` row, the `calc_runs` row for this run, and the
        `dirty_nodes` rows flushed by `process_event` before `_propagate`
        was called). None of that is visible to a different session until
        it is committed on `db`. `db.commit()` below is that boundary: it
        makes the event + calc_run(running) + dirty_nodes durable BEFORE
        Rust ever connects, at the cost of no longer being a single atomic
        transaction with the rest of this request.

        Consequence for failure handling: from this point on, retrying is
        the recovery mechanism, not rollback. If the Rust call itself
        raises, the `SAVEPOINT propagation_start` that `process_event`
        (propagator.py) set up before calling into `_propagate` no longer
        exists — it died with the commit above (a COMMIT ends the
        transaction and drops every savepoint in it). `_fail_after_boundary_commit`
        handles that explicitly: it persists the failure record directly
        (calc_run -> 'failed', advisory lock released — `fail_calc_run`)
        and durably (its own commit), then re-opens an EMPTY savepoint
        under the same name so `process_event`'s later, generic
        `ROLLBACK TO SAVEPOINT propagation_start` becomes a harmless no-op
        instead of a hard error that would itself abort the connection and
        swallow the real failure. `dirty_nodes` is deliberately left
        untouched throughout this path — nothing here clears it — so the
        next propagation attempt for this scenario recomputes the exact
        same PIs (self-healing retry).
        """
        db.commit()

        info = db.info
        # DSN carries no credential — safe to appear in a PyO3 panic
        # message, a tracing field, or a log line. The password is
        # threaded through as an explicit argument all the way to
        # postgres::Config::password() on the Rust side (ootils_kernel
        # >= 0.2.0) — never a PGPASSWORD env var, which was racy the
        # moment two propagations ran concurrently in this process (env
        # is shared mutable state; see the pre-0.2.0 version of this
        # method for the mutate/restore dance this replaces).
        dsn = (
            f"host={info.host} port={info.port} "
            f"user={info.user} "
            f"dbname={info.dbname}"
        )
        password = info.password or None

        try:
            stats = ootils_kernel.propagate_and_write(
                dsn,
                password,
                str(calc_run.calc_run_id),
                str(calc_run.scenario_id),
            )
        except TypeError as exc:
            # A wheel built against ootils_kernel < 0.2.0 exposes
            # propagate_and_write(dsn, calc_run_id, scenario_id) — 3
            # positional args, no password. Calling the 0.2.0+ 4-arg form
            # against it raises TypeError from PyO3's argument-count
            # check, not a domain/DB error — surface that distinctly
            # instead of a confusing generic failure.
            wheel_version = getattr(ootils_kernel, "version", lambda: "unknown")()
            message = (
                f"wheel ootils_kernel < 0.2.0 incompatible, rebuild via "
                f"WITH_RUST=1 (detected version={wheel_version!r})"
            )
            self._fail_after_boundary_commit(calc_run, db, message)
            raise RuntimeError(message) from exc
        except Exception as exc:
            self._fail_after_boundary_commit(calc_run, db, str(exc))
            raise

        calc_run.nodes_recalculated += stats["n_dirty_pis"]

        logger.info(
            "RustPropagationEngine: load=%.0fms compute=%.0fms copy=%.0fms "
            "update=%.0fms shortages=%.0fms clear_dirty=%.0fms detected=%d",
            stats["load_ms"],
            stats["compute_ms"],
            stats["copy_ms"],
            stats["update_ms"],
            stats["shortages_ms"],
            stats["clear_dirty_ms"],
            stats["n_shortages_detected"],
        )

        # SHORTAGES_SQL and CLEAR_DIRTY_SQL run on Python's session because
        # Postgres caches the query plan per-session — Python's connection
        # has the plans warm from previous calls (the SqlPropagationEngine
        # uses the same SQL), while a fresh Rust session pays the planning
        # cost each time. Measured: in-Rust SHORTAGES added ~2s on profile
        # L full prop vs ~0.3s on Python's warm connection.
        # Order matters: SHORTAGES joins on dirty_nodes, so it must run
        # BEFORE CLEAR_DIRTY_SQL.
        params = {
            "scenario_id": calc_run.scenario_id,
            "calc_run_id": calc_run.calc_run_id,
        }
        if self._shortage_detector is not None:
            db.execute(SHORTAGES_SQL, params)
        db.execute(CLEAR_DIRTY_SQL, params)

    def _fail_after_boundary_commit(
        self,
        calc_run: "CalcRun",
        db: DictRowConnection,
        error_message: str,
    ) -> None:
        """Persist a Rust-path failure that happened AFTER `_propagate_via_rust`'s
        boundary commit — see that method's docstring for the full contract.

        `process_event` (propagator.py) always tries
        `ROLLBACK TO SAVEPOINT propagation_start` on the way out of its
        `except` block. That savepoint was defined in the transaction the
        boundary commit above just ended, so left alone the ROLLBACK TO
        SAVEPOINT would itself raise ("savepoint ... does not exist"),
        aborting the connection before `fail_calc_run` ever runs — losing
        the failure record and leaving the scenario's advisory lock stuck
        until the pooled connection eventually recycles.

        Fix: mark + commit the failure HERE, then re-open an empty
        savepoint under the same name so `process_event`'s rollback is a
        harmless no-op and its own (now redundant) `fail_calc_run` call is
        safe to run again — it will be undone by the request's final
        rollback, which is fine, since the failure was already made
        durable by the `db.commit()` below.

        `dirty_nodes` is intentionally untouched by this method — nothing
        here clears it, so the next propagation attempt for this scenario
        recomputes the same PIs (self-healing retry).
        """
        self._calc_run_mgr.fail_calc_run(calc_run, error_message, db)
        db.commit()
        db.execute("SAVEPOINT propagation_start")
