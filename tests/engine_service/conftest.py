"""
conftest.py — shared fixtures for the engine-service test battery.

Key fixtures:
  - `engine_binary`   : Path to the built ootils-engine executable.
  - `dsn`             : DATABASE_URL from env (skip if absent).
  - `pick_pi_node`    : factory yielding a PI node UUID from the seeded DB.
  - `engine`          : EngineHarness already started + EngineClient connected,
                        torn down at end of test. One per test (clean WAL).
  - `engine_session`  : Module-scoped, shared engine instance for fast
                        tests that don't mutate.
"""
from __future__ import annotations

import os
import socket
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional
from uuid import UUID

import pytest

# Make sure the src layout is on sys.path even if pytest doesn't pick it up.
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))

BASELINE = UUID("00000000-0000-0000-0000-000000000001")


def pytest_collection_modifyitems(config, items):
    """Tag all tests in this directory with `live_db`. Allows
    `pytest -m 'not live_db'` to skip them on CI cells without DB."""
    for item in items:
        if "engine_service" in str(item.fspath):
            item.add_marker(pytest.mark.live_db)


@pytest.fixture(scope="session")
def engine_binary() -> Path:
    base = ROOT / "rust" / "target" / "release"
    for name in ("ootils-engine.exe", "ootils-engine"):
        p = base / name
        if p.exists():
            return p
    pytest.skip(
        f"ootils-engine binary not found in {base}. "
        "Build it: cd rust && cargo build --release -p ootils_engine"
    )


@pytest.fixture(scope="session")
def dsn() -> str:
    v = os.environ.get("DATABASE_URL")
    if not v:
        pytest.skip("DATABASE_URL not set — engine_service tests require a live DB")
    return v


@pytest.fixture(scope="session")
def grpc_module():
    """Skip cleanly if grpcio isn't installed (CI cells without it)."""
    grpc = pytest.importorskip("grpc")
    return grpc


def _free_port(start: int = 50100) -> int:
    """Find an unused TCP port — multiple harnesses can run in parallel."""
    for p in range(start, start + 100):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", p))
                return p
            except OSError:
                continue
    raise RuntimeError(f"no free port in [{start}, {start + 100})")


@pytest.fixture
def engine(engine_binary, dsn, grpc_module):
    """Function-scoped engine — clean WAL, fresh process per test.

    Yields a tuple `(harness, client)`. The client is closed and the
    process stopped at teardown.
    """
    from ootils_core.engine_rust_service import EngineClient, EngineHarness

    port = _free_port()
    addr = f"127.0.0.1:{port}"
    wal = Path(tempfile.gettempdir()) / f"engine-test-{os.getpid()}-{port}.wal"
    if wal.exists():
        wal.unlink()

    harness = EngineHarness(
        binary_path=engine_binary,
        dsn=dsn,
        listen_addr=addr,
        wal_path=wal,
        flush_interval_ms=100,
    )
    harness.start(wait_for_ready=True, ready_timeout_s=30.0)
    client = EngineClient.connect(addr)
    try:
        yield harness, client
    finally:
        client.close()
        harness.stop()
        if wal.exists():
            try:
                wal.unlink()
            except OSError:
                pass


@pytest.fixture(scope="module")
def engine_session(engine_binary, dsn, grpc_module):
    """Module-scoped engine — shared across read-only tests for speed.
    Don't use this if your test mutates state in a way other tests
    will read."""
    from ootils_core.engine_rust_service import EngineClient, EngineHarness

    port = _free_port(start=50200)
    addr = f"127.0.0.1:{port}"
    wal = Path(tempfile.gettempdir()) / f"engine-session-{os.getpid()}.wal"
    if wal.exists():
        wal.unlink()

    harness = EngineHarness(
        binary_path=engine_binary,
        dsn=dsn,
        listen_addr=addr,
        wal_path=wal,
        flush_interval_ms=200,
    )
    harness.start(wait_for_ready=True, ready_timeout_s=30.0)
    client = EngineClient.connect(addr)
    yield harness, client
    client.close()
    harness.stop()
    if wal.exists():
        try:
            wal.unlink()
        except OSError:
            pass


@pytest.fixture
def pick_pi_node(dsn):
    """Factory that returns a fresh PI node UUID + its (item, location)
    couple from the live DB. Useful for triggering Propagate."""
    import psycopg
    from psycopg.rows import dict_row

    def _pick(must_have_shortage: Optional[bool] = None) -> tuple[UUID, UUID, UUID]:
        with psycopg.connect(dsn, row_factory=dict_row) as conn:
            sql = (
                "SELECT node_id, item_id, location_id FROM nodes "
                "WHERE node_type='ProjectedInventory' AND scenario_id=%s "
                "AND active=TRUE "
            )
            params: list = [BASELINE]
            if must_have_shortage is not None:
                sql += "AND has_shortage = %s "
                params.append(must_have_shortage)
            sql += "ORDER BY random() LIMIT 1"
            row = conn.execute(sql, tuple(params)).fetchone()
            if row is None:
                raise pytest.skip("no PI node matches the filter")
            return (
                UUID(str(row["node_id"])),
                UUID(str(row["item_id"])),
                UUID(str(row["location_id"])),
            )

    return _pick
