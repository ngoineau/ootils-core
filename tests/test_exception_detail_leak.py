"""
test_exception_detail_leak.py — regression tests for #352.

Asserts the generic-exception carve-out doctrine (CLAUDE.md): broad
``except Exception`` handlers in routers must NOT echo ``str(e)`` /
``f"...{exc}..."`` back to the client, because psycopg raises expose raw
SQL text, table/column/constraint names and sometimes the DSN. The operator
keeps the full detail via ``logger.exception`` (unchanged); the client only
gets a stable, generic message.

Each test forces the underlying engine/manager call to raise an exception
whose message SIMULATES a leaked psycopg error (SQL text + a fake DSN), then
asserts the HTTP response body contains neither that text nor 'psycopg' /
'postgresql://'.

No DB is used — ``get_db`` is overridden with a mock, and the exception is
injected via ``unittest.mock.patch`` on the call the router awaits/invokes.
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient

os.environ.setdefault("OOTILS_API_TOKEN", "test-token")

from ootils_core.api.app import create_app
from ootils_core.api.dependencies import get_db


AUTH = {"Authorization": "Bearer test-token"}

# A message shaped like a real psycopg / SQL error — if this text (or the
# generic 'psycopg' / 'postgresql://' markers) ever leaks into a response
# body, the test fails.
_FAKE_DB_LEAK = (
    'relation "nodes" does not exist\n'
    "LINE 1: SELECT * FROM nodes WHERE id = %s\n"
    "connection to server at postgresql://ootils:s3cr3t@10.0.0.5:5432/ootils_prod failed: "
    "psycopg.errors.UndefinedTable"
)


def _make_db_mock() -> MagicMock:
    conn = MagicMock(name="psycopg_conn")
    conn.execute.return_value = MagicMock()
    return conn


def _make_client(db_mock: MagicMock) -> TestClient:
    app = create_app()

    def override_db():
        yield db_mock

    app.dependency_overrides[get_db] = override_db
    return TestClient(app)


def _assert_no_leak(resp) -> None:
    body_text = resp.text
    assert "nodes" not in body_text or "does not exist" not in body_text, (
        f"raw SQL error text leaked into response: {body_text!r}"
    )
    assert _FAKE_DB_LEAK not in body_text
    assert "psycopg" not in body_text.lower()
    assert "postgresql://" not in body_text
    assert "s3cr3t" not in body_text


# ─────────────────────────────────────────────────────────────
# POST /v1/atp/check
# ─────────────────────────────────────────────────────────────

def test_atp_check_failure_does_not_leak_db_error():
    db = _make_db_mock()
    item_uuid = uuid4()
    location_uuid = uuid4()

    payload = {
        "item_id": "ITEM-1",
        "location_id": "LOC-1",
        "quantity": "10",
        "requested_date": "2026-08-01",
        "horizon_days": 30,
    }

    with patch(
        "ootils_core.atp.routers._resolve_item_uuid", return_value=item_uuid
    ), patch(
        "ootils_core.atp.routers._resolve_location_uuid", return_value=location_uuid
    ), patch(
        "ootils_core.atp.routers.ATPEngine.calculate",
        side_effect=Exception(_FAKE_DB_LEAK),
    ):
        client = _make_client(db)
        resp = client.post("/v1/atp/check", json=payload, headers=AUTH)

    assert resp.status_code == 500, resp.text
    _assert_no_leak(resp)
    assert resp.json()["detail"] == "ATP calculation failed."


# ─────────────────────────────────────────────────────────────
# POST /v1/simulate
# ─────────────────────────────────────────────────────────────

def test_create_simulation_failure_does_not_leak_db_error():
    db = _make_db_mock()

    payload = {"scenario_name": "what-if-leak-test"}

    with patch(
        "ootils_core.api.routers.simulate.ScenarioManager.create_scenario",
        side_effect=Exception(_FAKE_DB_LEAK),
    ):
        client = _make_client(db)
        resp = client.post("/v1/simulate", json=payload, headers=AUTH)

    assert resp.status_code == 500, resp.text
    _assert_no_leak(resp)
    assert resp.json()["detail"] == "Failed to create scenario."


# ─────────────────────────────────────────────────────────────
# POST /v1/dq/run/{batch_id}
# ─────────────────────────────────────────────────────────────

def test_run_dq_batch_failure_does_not_leak_db_error():
    db = _make_db_mock()
    batch_id = uuid4()

    cur = MagicMock()
    cur.fetchone.return_value = {"batch_id": batch_id}
    db.execute.return_value = cur

    with patch(
        "ootils_core.api.routers.dq.run_dq",
        side_effect=Exception(_FAKE_DB_LEAK),
    ):
        client = _make_client(db)
        resp = client.post(f"/v1/dq/run/{batch_id}", headers=AUTH)

    assert resp.status_code == 500, resp.text
    _assert_no_leak(resp)
    assert resp.json()["detail"] == "DQ run failed."


# ─────────────────────────────────────────────────────────────
# POST /v1/dq/agent/run/{batch_id}
# ─────────────────────────────────────────────────────────────

def test_run_agent_batch_failure_does_not_leak_db_error():
    db = _make_db_mock()
    batch_id = uuid4()

    cur = MagicMock()
    cur.fetchone.return_value = {"batch_id": batch_id}
    db.execute.return_value = cur

    with patch(
        "ootils_core.engine.dq.agent.run_dq_agent",
        side_effect=Exception(_FAKE_DB_LEAK),
    ):
        client = _make_client(db)
        resp = client.post(f"/v1/dq/agent/run/{batch_id}", headers=AUTH)

    assert resp.status_code == 500, resp.text
    _assert_no_leak(resp)
    assert resp.json()["detail"] == "DQ Agent run failed."


# ─────────────────────────────────────────────────────────────
# POST /v1/crp/calculate
# ─────────────────────────────────────────────────────────────

def test_crp_calculate_failure_does_not_leak_db_error():
    db = _make_db_mock()

    payload = {"horizon_days": 90}

    with patch(
        "ootils_core.crp.routers.CRPEngine.calculate",
        side_effect=Exception(_FAKE_DB_LEAK),
    ):
        client = _make_client(db)
        resp = client.post("/v1/crp/calculate", json=payload, headers=AUTH)

    assert resp.status_code == 500, resp.text
    _assert_no_leak(resp)
    assert resp.json()["detail"] == "CRP calculation failed."


# ─────────────────────────────────────────────────────────────
# Structural guard — the whole doctrine, enforced source-wide
# ─────────────────────────────────────────────────────────────

import ast  # noqa: E402
from pathlib import Path  # noqa: E402

import ootils_core  # noqa: E402

_BROAD_EXC_NAMES = {"Exception", "BaseException"}


def _handler_is_broad(handler: ast.ExceptHandler) -> bool:
    """A bare ``except:`` or one catching Exception / BaseException. Narrow
    named excepts (ValueError, ParamOverlayError, PyramideError, ...) — the
    documented domain carve-outs — are NOT broad and are never flagged."""
    t = handler.type
    if t is None:
        return True
    candidates = t.elts if isinstance(t, ast.Tuple) else [t]
    return any(isinstance(n, ast.Name) and n.id in _BROAD_EXC_NAMES for n in candidates)


def _references(node: ast.AST, varname: str) -> bool:
    return any(isinstance(sub, ast.Name) and sub.id == varname for sub in ast.walk(node))


def _detail_echoes(call: ast.Call, varname: str) -> bool:
    """True if a call has a ``detail=`` kwarg whose value references varname
    (covers ``str(e)``, ``f"...{e}..."``, ``f"...{str(e)}..."``, bare ``e``)."""
    return any(kw.arg == "detail" and _references(kw.value, varname) for kw in call.keywords)


def _find_broad_except_detail_echoes() -> list[str]:
    src_root = Path(ootils_core.__file__).resolve().parent
    violations: list[str] = []
    for py in src_root.rglob("*.py"):
        tree = ast.parse(py.read_text(encoding="utf-8"), filename=str(py))
        for handler in ast.walk(tree):
            if not isinstance(handler, ast.ExceptHandler) or not _handler_is_broad(handler):
                continue
            var = handler.name
            if var is None:  # bare ``except:`` with no ``as x`` — cannot echo a var
                continue
            for node in ast.walk(handler):
                if isinstance(node, ast.Call) and _detail_echoes(node, var):
                    violations.append(f"{py.relative_to(src_root.parent)}:{node.lineno}")
    return violations


def test_no_broad_except_echoes_exception_in_http_detail():
    """Structural regression guard for #352 (like the ADR-021 consistency
    guard): a broad ``except Exception as e`` must NEVER put the exception into
    an HTTPException ``detail=`` (``str(e)`` / ``f"...{e}..."``) — psycopg raises
    expose raw SQL, schema names and sometimes the DSN. To satisfy this, either
    narrow the except to a named domain exception with a hand-authored safe
    message (the documented carve-out), or return a generic ``detail`` and keep
    ``logger.exception``. Narrow named excepts are not flagged."""
    leaky = _find_broad_except_detail_echoes()
    assert leaky == [], (
        "broad-except handlers echo the caught exception into an HTTPException "
        "detail= (psycopg leak risk — CLAUDE.md generic-handler doctrine): "
        + ", ".join(leaky)
    )
