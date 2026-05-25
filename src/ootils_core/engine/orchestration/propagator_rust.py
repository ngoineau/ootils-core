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

import psycopg

try:
    import ootils_kernel  # type: ignore[import-not-found]
except ImportError as _exc:  # pragma: no cover — guarded at construction
    ootils_kernel = None  # type: ignore[assignment]
    _IMPORT_ERROR: ImportError | None = _exc
else:
    _IMPORT_ERROR = None

from ootils_core.engine.orchestration.propagator import PropagationEngine
from ootils_core.engine.orchestration.propagator_sql import (
    CLEAR_DIRTY_SQL,
    PROPAGATE_SQL,
    SHORTAGES_SQL,
)

if TYPE_CHECKING:
    from ootils_core.models import CalcRun

logger = logging.getLogger(__name__)


# Crossover point: below this many dirty PIs, the SQL engine's single
# UPDATE statement beats the Rust path (load + COPY + UPDATE + clear)
# because Rust pays a per-query roundtrip × 4 SELECTs that the SQL
# engine collapses into one statement. Measured on profile L:
#   - 91 PIs  : SQL 95ms vs Rust 220ms (SQL 2.3× faster)
#   - 5000 PIs: roughly parity
#   - 50000+ PIs: Rust wins clearly (full prop L: Rust 9.5s vs SQL 36.8s)
# 5000 is a conservative threshold — Rust likely wins below too once we
# pipeline the load queries in week 5. Tune via env var if needed.
RUST_DISPATCH_THRESHOLD = int(os.environ.get("OOTILS_RUST_MIN_DIRTY", "5000"))


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
        db: psycopg.Connection,
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

    def _propagate_via_sql(self, calc_run: "CalcRun", db: psycopg.Connection) -> None:
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

    def _propagate_via_rust(self, calc_run: "CalcRun", db: psycopg.Connection) -> None:
        """Delegate the heavy lifting to ootils_kernel."""
        # Commit dirty_nodes inserts so Rust (separate session) can see them.
        db.commit()

        # psycopg's `info.dsn` redacts the password. Reconstruct an explicit DSN.
        info = db.info
        dsn = (
            f"host={info.host} port={info.port} "
            f"user={info.user} password={info.password} "
            f"dbname={info.dbname}"
        )

        stats = ootils_kernel.propagate_and_write(
            dsn,
            str(calc_run.calc_run_id),
            str(calc_run.scenario_id),
        )

        calc_run.nodes_recalculated += stats["n_dirty_pis"]

        logger.info(
            "RustPropagationEngine: load=%.0fms compute=%.0fms copy=%.0fms "
            "update=%.0fms clear_dirty=%.0fms shortages=%d",
            stats["load_ms"],
            stats["compute_ms"],
            stats["copy_ms"],
            stats["update_ms"],
            stats["clear_dirty_ms"],
            stats["n_shortages_detected"],
        )

        if self._shortage_detector is not None:
            db.execute(
                SHORTAGES_SQL,
                {
                    "scenario_id": calc_run.scenario_id,
                    "calc_run_id": calc_run.calc_run_id,
                },
            )
