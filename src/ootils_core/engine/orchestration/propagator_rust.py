"""
propagator_rust.py — Rust-backed propagation engine (ADR-016 §week 4).

Inherits the Python `PropagationEngine` for everything except the
`_propagate` hot path, which is delegated to the Rust extension module
`ootils_kernel`. The Rust side does:

  1. Load dirty subgraph (PIs + supplies + demands + seed openings)
  2. Compute every PI in memory (parity-validated week 3)
  3. COPY the projection into a temp table + UPDATE FROM
  4. Clear `dirty_nodes` for this calc_run

Shortage *detection* (safety-stock vs. closing_stock, persisted to the
`shortages` table) stays in SQL — the wrapper calls SHORTAGES_SQL from
`propagator_sql` after the Rust pass finishes. Same contract, same
shortage rows.

Boundary:
- Python keeps the calc_run lifecycle, advisory lock, scenario state
  machine, agent tools, FastAPI routes — everything that changes often.
- Rust owns the read + compute + bulk writeback — the stable hot path.
"""
from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING
from uuid import UUID


try:
    import ootils_kernel  # type: ignore[import-not-found]
except ImportError as _exc:  # pragma: no cover — guarded at construction
    ootils_kernel = None  # type: ignore[assignment]
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
# inline path. Measured on profile L after the week-5 perf optims
# (connection pool + UNNEST writeback + combined UNION ALL load):
#   -    91 PIs : SQL 95ms vs Rust ~40ms (warm pool)  → Rust 2.4× FASTER
#   -  5000 PIs : Rust still wins
#   - 50000+ PIs: Rust wins decisively (full prop L: 9.5s vs 36.8s)
#
# Default 0 = always use Rust (post-week-5 optims made it win at every
# scale we've measured). Set OOTILS_RUST_MIN_DIRTY > 0 to force the
# SQL fallback below a threshold if regressions show up.
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
        """Delegate the heavy lifting to ootils_kernel."""
        # Commit dirty_nodes inserts so Rust (separate session) can see them.
        db.commit()

        # F-017: build a DSN WITHOUT the password embedded — pass the
        # password via PGPASSWORD instead. tokio-postgres + libpq
        # consult that env var when the connection string omits it.
        # An embedded password ends up in any log that includes the
        # DSN string (PyO3 panic message, tracing field, etc.); the
        # env-var approach is the documented safe pattern.
        info = db.info
        dsn = (
            f"host={info.host} port={info.port} "
            f"user={info.user} "
            f"dbname={info.dbname}"
        )
        if info.password:
            # Localized scope: set + restore so we don't pollute the
            # process env for other callers.
            prior = os.environ.get("PGPASSWORD")
            os.environ["PGPASSWORD"] = info.password
            try:
                stats = ootils_kernel.propagate_and_write(
                    dsn,
                    str(calc_run.calc_run_id),
                    str(calc_run.scenario_id),
                )
            finally:
                if prior is None:
                    os.environ.pop("PGPASSWORD", None)
                else:
                    os.environ["PGPASSWORD"] = prior
        else:
            stats = ootils_kernel.propagate_and_write(
                dsn,
                str(calc_run.calc_run_id),
                str(calc_run.scenario_id),
            )

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
