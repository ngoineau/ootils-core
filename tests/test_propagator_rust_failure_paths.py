"""
test_propagator_rust_failure_paths.py — pure unit tests for the PR-C
hardening of `RustPropagationEngine._propagate_via_rust` (no DB, no
compiled wheel required):

  1. Credential threading (ootils_kernel >= 0.2.0): the password is an
     EXPLICIT 2nd positional argument to `propagate_and_write`, the DSN
     never embeds it (safe to log), and the racy pre-0.2.0
     `os.environ["PGPASSWORD"]` set/restore dance is gone — the env var
     is never mutated, not even transiently during the call.
  2. Wheel-mismatch detection: a `TypeError` out of the PyO3
     argument-count check (a stale < 0.2.0 wheel called with the 4-arg
     form) is re-surfaced as a `RuntimeError` with an actionable message
     ("rebuild via WITH_RUST=1"), chained (`__cause__`) to the original
     TypeError.
  3. The boundary-commit failure contract
     (`_fail_after_boundary_commit`): on ANY Rust-call failure after the
     mid-request `db.commit()`, the engine persists the failure record
     itself — `fail_calc_run` → `db.commit()` → re-open an EMPTY
     `SAVEPOINT propagation_start`, in that order — so `process_event`'s
     generic `ROLLBACK TO SAVEPOINT propagation_start` becomes a
     harmless no-op instead of a "savepoint does not exist" error that
     would strand the calc_run in 'running' and the advisory lock held.

`ootils_kernel` is monkeypatched with in-test stubs: the module under
test import-guards the extension, so these tests run on any machine,
wheel or not. Mocks are deliberate here (CLAUDE.md's no-mock rule is
about DB-touching tests) — the DB-backed, end-to-end version of the
failure contract lives in
tests/integration/test_rust_parity_integration.py.
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from ootils_core.engine.orchestration import propagator_rust
from ootils_core.engine.orchestration.propagator_sql import (
    CLEAR_DIRTY_SQL,
    SHORTAGES_SQL,
    shortage_params,
)
from ootils_core.models import CalcRun


# ---------------------------------------------------------------------------
# Stubs & helpers
# ---------------------------------------------------------------------------

PASSWORD = "s3cret-pw"


def _stats(n_dirty: int = 7) -> dict:
    """A stats dict shaped like the real ootils_kernel.propagate_and_write."""
    return {
        "n_dirty_pis": n_dirty,
        "n_supplies": 3,
        "n_demands": 4,
        "n_series_seeds": 2,
        "n_shortages_detected": 2,
        "writeback_path": "unnest",
        "load_ms": 1.0,
        "compute_ms": 1.0,
        "copy_ms": 1.0,
        "update_ms": 1.0,
        "shortages_ms": 0.0,
        "clear_dirty_ms": 0.0,
    }


class _RecordingKernel:
    """Stub ootils_kernel: records call args + the PGPASSWORD env var AS SEEN
    DURING the call (the only way to prove the old set/restore dance is gone
    — it restored the env afterwards, so a post-hoc check can't tell)."""

    def __init__(self, result=None, exc: Exception | None = None,
                 version_str: str = "0.2.0") -> None:
        self.result = result if result is not None else _stats()
        self.exc = exc
        self.version_str = version_str
        self.calls: list[tuple] = []
        self.env_password_during_call: list = []

    def version(self) -> str:
        return self.version_str

    def propagate_and_write(self, dsn, password, calc_run_id_str, scenario_id_str):
        self.calls.append((dsn, password, calc_run_id_str, scenario_id_str))
        self.env_password_during_call.append(os.environ.get("PGPASSWORD"))
        if self.exc is not None:
            raise self.exc
        return self.result


class _VersionlessKernel:
    """A stub with NO version() attribute — exercises the getattr fallback.
    Deliberately NOT a _RecordingKernel subclass (it would inherit version)."""

    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    def propagate_and_write(self, dsn, password, calc_run_id_str, scenario_id_str):
        raise self.exc


def _make_db(password=PASSWORD) -> MagicMock:
    db = MagicMock(name="db")
    db.info.host = "dbhost"
    db.info.port = 5432
    db.info.user = "ootils"
    db.info.dbname = "ootils_unit"
    db.info.password = password
    return db


def _make_calc_run() -> CalcRun:
    return CalcRun(calc_run_id=uuid4(), scenario_id=uuid4(), status="running")


def _make_engine(monkeypatch, kernel_stub, *, calc_run_mgr=None,
                 shortage_detector=...):
    """Build a RustPropagationEngine against a stubbed ootils_kernel module."""
    monkeypatch.setattr(propagator_rust, "ootils_kernel", kernel_stub)
    if shortage_detector is ...:
        shortage_detector = MagicMock(name="shortage_detector")
    return propagator_rust.RustPropagationEngine(
        store=MagicMock(),
        traversal=MagicMock(),
        dirty=MagicMock(),
        calc_run_mgr=calc_run_mgr if calc_run_mgr is not None else MagicMock(),
        kernel=MagicMock(),
        shortage_detector=shortage_detector,
    )


def _executed_sql(db: MagicMock) -> list:
    return [c.args[0] for c in db.execute.call_args_list]


# ---------------------------------------------------------------------------
# 1. Credential threading — explicit argument, clean DSN, no PGPASSWORD
# ---------------------------------------------------------------------------


def test_password_is_explicit_argument_and_dsn_carries_no_credential(monkeypatch):
    monkeypatch.delenv("PGPASSWORD", raising=False)
    kernel = _RecordingKernel()
    engine = _make_engine(monkeypatch, kernel)
    db = _make_db()
    calc_run = _make_calc_run()

    engine._propagate_via_rust(calc_run, db)

    assert len(kernel.calls) == 1
    dsn, password, calc_run_id_str, scenario_id_str = kernel.calls[0]
    # DSN is credential-free (safe to log / show in a PyO3 panic message).
    assert dsn == "host=dbhost port=5432 user=ootils dbname=ootils_unit"
    assert PASSWORD not in dsn
    # The credential flows through the explicit argument, nowhere else.
    assert password == PASSWORD
    assert calc_run_id_str == str(calc_run.calc_run_id)
    assert scenario_id_str == str(calc_run.scenario_id)
    # The pre-0.2.0 dance mutated PGPASSWORD BEFORE the call and restored it
    # after — assert the env var was untouched DURING the call as well.
    assert kernel.env_password_during_call == [None]
    assert "PGPASSWORD" not in os.environ


def test_pgpassword_env_var_is_never_clobbered(monkeypatch):
    """A pre-existing PGPASSWORD (set by the operator for psql etc.) must
    survive the call unchanged AND never be replaced mid-call — env is
    shared mutable state across concurrent requests (the original race)."""
    monkeypatch.setenv("PGPASSWORD", "operator-canary")
    kernel = _RecordingKernel()
    engine = _make_engine(monkeypatch, kernel)

    engine._propagate_via_rust(_make_calc_run(), _make_db())

    assert kernel.env_password_during_call == ["operator-canary"]
    assert os.environ["PGPASSWORD"] == "operator-canary"


@pytest.mark.parametrize("empty_credential", ["", None])
def test_empty_password_is_passed_as_none(monkeypatch, empty_credential):
    """`info.password or None`: trust-auth / socket connections carry no
    password — the Rust side must receive None, not an empty string."""
    kernel = _RecordingKernel()
    engine = _make_engine(monkeypatch, kernel)

    engine._propagate_via_rust(_make_calc_run(), _make_db(password=empty_credential))

    assert kernel.calls[0][1] is None


def test_success_path_runs_shortages_then_clear_dirty_on_python_session(monkeypatch):
    """After a successful Rust pass: SHORTAGES_SQL runs BEFORE CLEAR_DIRTY_SQL
    (it joins on dirty_nodes), both on Python's `db` session, and the
    calc_run counter absorbs n_dirty_pis."""
    kernel = _RecordingKernel(result=_stats(n_dirty=42))
    engine = _make_engine(monkeypatch, kernel)
    db = _make_db()
    calc_run = _make_calc_run()

    engine._propagate_via_rust(calc_run, db)

    assert calc_run.nodes_recalculated == 42
    executed = _executed_sql(db)
    assert SHORTAGES_SQL in executed
    assert CLEAR_DIRTY_SQL in executed
    assert executed.index(SHORTAGES_SQL) < executed.index(CLEAR_DIRTY_SQL)
    # shortage_params() (ADR-021 safety_scope amendment, DESC-1 PR-C, added
    # 2026-07-18) is the single builder both call sites now share — includes
    # `safety_scope_national`, resolved from OOTILS_SAFETY_SCOPE (unset here,
    # so it defaults to the pilot's 'national').
    params = shortage_params(calc_run.scenario_id, calc_run.calc_run_id)
    db.execute.assert_any_call(SHORTAGES_SQL, params)
    db.execute.assert_any_call(CLEAR_DIRTY_SQL, params)


def test_success_path_without_shortage_detector_skips_shortages_sql(monkeypatch):
    kernel = _RecordingKernel()
    engine = _make_engine(monkeypatch, kernel, shortage_detector=None)
    db = _make_db()

    engine._propagate_via_rust(_make_calc_run(), db)

    executed = _executed_sql(db)
    assert SHORTAGES_SQL not in executed
    assert CLEAR_DIRTY_SQL in executed


# ---------------------------------------------------------------------------
# 2. Wheel-mismatch detection (TypeError → explicit RuntimeError)
# ---------------------------------------------------------------------------


def test_wheel_mismatch_typeerror_surfaces_actionable_runtimeerror(monkeypatch):
    original = TypeError(
        "propagate_and_write() takes 3 positional arguments but 4 were given"
    )
    kernel = _RecordingKernel(exc=original, version_str="0.1.0")
    mgr = MagicMock(name="calc_run_mgr")
    engine = _make_engine(monkeypatch, kernel, calc_run_mgr=mgr)
    db = _make_db()
    calc_run = _make_calc_run()

    with pytest.raises(RuntimeError) as excinfo:
        engine._propagate_via_rust(calc_run, db)

    message = str(excinfo.value)
    assert "wheel ootils_kernel < 0.2.0 incompatible" in message
    assert "WITH_RUST=1" in message
    assert "detected version='0.1.0'" in message
    # The original PyO3 TypeError stays attached for diagnosis.
    assert excinfo.value.__cause__ is original
    # Failure is persisted through the boundary-commit contract, once.
    mgr.fail_calc_run.assert_called_once_with(calc_run, message, db)
    # Boundary commit + failure-durability commit.
    assert db.commit.call_count == 2
    assert "SAVEPOINT propagation_start" in _executed_sql(db)
    # The post-Rust SQL steps never ran.
    executed = _executed_sql(db)
    assert SHORTAGES_SQL not in executed
    assert CLEAR_DIRTY_SQL not in executed


def test_wheel_mismatch_without_version_attribute_reports_unknown(monkeypatch):
    """A wheel so old it lacks version() must still produce the actionable
    message — getattr fallback reports 'unknown', never AttributeError."""
    kernel = _VersionlessKernel(exc=TypeError("argument count mismatch"))
    assert not hasattr(kernel, "version")
    engine = _make_engine(monkeypatch, kernel)

    with pytest.raises(RuntimeError) as excinfo:
        engine._propagate_via_rust(_make_calc_run(), _make_db())

    assert "detected version='unknown'" in str(excinfo.value)


# ---------------------------------------------------------------------------
# 3. Boundary-commit failure contract
# ---------------------------------------------------------------------------


def test_generic_rust_failure_reraises_original_after_persisting_failure(monkeypatch):
    boom = RuntimeError("connection refused: simulated rust-session failure")
    kernel = _RecordingKernel(exc=boom)
    mgr = MagicMock(name="calc_run_mgr")
    engine = _make_engine(monkeypatch, kernel, calc_run_mgr=mgr)
    db = _make_db()
    calc_run = _make_calc_run()

    with pytest.raises(RuntimeError) as excinfo:
        engine._propagate_via_rust(calc_run, db)

    # Bare re-raise: the ORIGINAL exception object, not a wrapper — a generic
    # Rust/DB error is not a wheel mismatch and must not be disguised as one.
    assert excinfo.value is boom
    mgr.fail_calc_run.assert_called_once_with(calc_run, str(boom), db)
    assert db.commit.call_count == 2  # boundary + failure durability
    assert "SAVEPOINT propagation_start" in _executed_sql(db)
    executed = _executed_sql(db)
    assert SHORTAGES_SQL not in executed
    assert CLEAR_DIRTY_SQL not in executed


def test_fail_after_boundary_commit_order_fail_then_commit_then_savepoint(monkeypatch):
    """Order is the contract: (1) fail_calc_run marks 'failed' + releases the
    advisory lock, (2) commit makes it durable, (3) an EMPTY savepoint named
    `propagation_start` is re-opened so process_event's ROLLBACK TO SAVEPOINT
    is a no-op. Any other order either loses the failure record or leaves the
    savepoint missing."""
    parent = MagicMock(name="parent")
    db = parent.db
    mgr = parent.mgr
    engine = _make_engine(monkeypatch, _RecordingKernel(), calc_run_mgr=mgr)
    calc_run = _make_calc_run()

    engine._fail_after_boundary_commit(calc_run, db, "boom-message")

    mgr.fail_calc_run.assert_called_once_with(calc_run, "boom-message", db)
    names = [name for name, _args, _kwargs in parent.mock_calls]
    assert "mgr.fail_calc_run" in names
    assert "db.commit" in names
    i_fail = names.index("mgr.fail_calc_run")
    i_commit = names.index("db.commit")
    savepoint_calls = [
        i for i, (name, args, _kw) in enumerate(parent.mock_calls)
        if name == "db.execute" and args and args[0] == "SAVEPOINT propagation_start"
    ]
    assert savepoint_calls, "the empty savepoint must be re-opened"
    assert i_fail < i_commit < savepoint_calls[0]


def test_constructor_refuses_missing_extension(monkeypatch):
    monkeypatch.setattr(propagator_rust, "ootils_kernel", None)
    with pytest.raises(RuntimeError, match="maturin build"):
        propagator_rust.RustPropagationEngine(
            store=MagicMock(),
            traversal=MagicMock(),
            dirty=MagicMock(),
            calc_run_mgr=MagicMock(),
            kernel=MagicMock(),
            shortage_detector=None,
        )
