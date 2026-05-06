"""Tests for MPS approval endpoint."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import uuid4
from unittest.mock import MagicMock

import psycopg
import pytest
from fastapi import HTTPException

from ootils_core.mps.api import ApproveMPSRequest, approve_mps_node


def _make_result(row):
    result = MagicMock()
    result.fetchone.return_value = row
    result.fetchall.return_value = [row] if row else []
    result.rowcount = 1 if row else 0
    return result


def _make_db(first_row):
    db = MagicMock(spec=psycopg.Connection)
    db.execute = MagicMock(side_effect=[_make_result(first_row), _make_result(None)])
    return db


def test_approve_request_defaults():
    req = ApproveMPSRequest()
    assert req.reviewed_by is None
    assert req.approved_by is None
    assert req.notes is None


def test_approve_draft_records_review_and_approval():
    mps_id = uuid4()
    db = _make_db({
        "mps_id": mps_id,
        "status": "DRAFT",
        "reviewed_by": None,
        "approved_by": None,
        "reviewed_at": None,
        "approved_at": None,
        "notes": None,
    })

    resp = asyncio.run(approve_mps_node(
        mps_id=mps_id,
        body=ApproveMPSRequest(approved_by="director", notes="go"),
        db=db,
        token="token-user",
    ))

    assert resp.mps_id == mps_id
    assert resp.previous_status == "DRAFT"
    assert resp.status == "APPROVED"
    assert resp.reviewed_by == "director"
    assert resp.approved_by == "director"
    assert resp.reviewed_at is not None
    assert resp.approved_at is not None
    assert resp.notes == "go"

    update_args = db.execute.call_args_list[1].args
    assert "UPDATE mps_nodes" in update_args[0]
    assert update_args[1][0] == "APPROVED"
    assert update_args[1][1] == "director"
    assert update_args[1][3] == "director"
    assert update_args[1][5] == "go"
    assert update_args[1][7] == mps_id


def test_approve_reviewed_preserves_reviewer():
    mps_id = uuid4()
    reviewed_at = datetime.now(timezone.utc)
    db = _make_db({
        "mps_id": mps_id,
        "status": "REVIEWED",
        "reviewed_by": "planner",
        "approved_by": None,
        "reviewed_at": reviewed_at,
        "approved_at": None,
        "notes": "reviewed",
    })

    resp = asyncio.run(approve_mps_node(
        mps_id=mps_id,
        body=ApproveMPSRequest(approved_by="director"),
        db=db,
        token="token-user",
    ))

    assert resp.previous_status == "REVIEWED"
    assert resp.status == "APPROVED"
    assert resp.reviewed_by == "planner"
    assert resp.reviewed_at == reviewed_at
    assert resp.approved_by == "director"
    assert resp.notes == "reviewed"


def test_approve_already_approved_is_idempotent():
    mps_id = uuid4()
    approved_at = datetime.now(timezone.utc)
    db = _make_db({
        "mps_id": mps_id,
        "status": "APPROVED",
        "reviewed_by": "planner",
        "approved_by": "director",
        "reviewed_at": approved_at,
        "approved_at": approved_at,
        "notes": "approved",
    })

    resp = asyncio.run(approve_mps_node(
        mps_id=mps_id,
        body=ApproveMPSRequest(),
        db=db,
        token="token-user",
    ))

    assert resp.previous_status == "APPROVED"
    assert resp.status == "APPROVED"
    assert resp.approved_by == "director"
    assert db.execute.call_count == 1


def test_approve_not_found_returns_404():
    mps_id = uuid4()
    db = _make_db(None)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(approve_mps_node(
            mps_id=mps_id,
            body=ApproveMPSRequest(),
            db=db,
            token="token-user",
        ))

    assert exc.value.status_code == 404


def test_approve_released_returns_409():
    mps_id = uuid4()
    db = _make_db({
        "mps_id": mps_id,
        "status": "RELEASED",
        "reviewed_by": "planner",
        "approved_by": "director",
        "reviewed_at": None,
        "approved_at": None,
        "notes": None,
    })

    with pytest.raises(HTTPException) as exc:
        asyncio.run(approve_mps_node(
            mps_id=mps_id,
            body=ApproveMPSRequest(),
            db=db,
            token="token-user",
        ))

    assert exc.value.status_code == 409
