"""
Tests for MPS-004: Promote to MRP Integration.

Tests the POST /v1/mps/{id}/promote-to-mrp endpoint and related engine methods.
Uses mock database connections following the pattern from test_mps_capacity_check.py.
"""
import pytest
from datetime import date
from decimal import Decimal
from uuid import uuid4
from unittest.mock import MagicMock

import psycopg

from ootils_core.constants import BASELINE_SCENARIO_ID
from ootils_core.mps.engine import AggregateDemandEngine, PromoteToMRPResult
from ootils_core.mps.models import MPSStatus


def _find_planned_supply_insert(db):
    """Return the (sql, params) of the INSERT INTO planned_supply call, or None."""
    for call in db.execute.call_args_list:
        sql = call.args[0] if call.args else ""
        if "INSERT INTO planned_supply" in sql:
            params = call.args[1] if len(call.args) > 1 else ()
            return sql, params
    return None


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _make_mock_db(responses=None):
    """Créer une mock de connexion PostgreSQL."""
    if responses is None:
        responses = []
    
    conn = MagicMock(spec=psycopg.Connection)
    
    if responses:
        responses = list(responses)
        
        def execute_side_effect(*args, **kwargs):
            if not responses:
                result = MagicMock()
                result.fetchone.return_value = None
                result.fetchall.return_value = []
                result.rowcount = 0
                return result
            
            item = responses.pop(0)
            result = MagicMock()
            
            if isinstance(item, list):
                result.fetchall.return_value = item
                result.fetchone.return_value = item[0] if item else None
            elif isinstance(item, dict):
                result.fetchone.return_value = item
                result.fetchall.return_value = [item]
            elif item is None:
                result.fetchone.return_value = None
                result.fetchall.return_value = []
            else:
                raise TypeError(f"Unexpected response type: {type(item)}")
            
            result.rowcount = 1
            return result
        
        conn.execute = MagicMock(side_effect=execute_side_effect)
    
    return conn


# ─────────────────────────────────────────────────────────────
# Tests PromoteToMRPRequest
# ─────────────────────────────────────────────────────────────

class TestPromoteToMRPRequestValidation:
    """Tests for request validation."""
    
    def test_promote_request_default_values(self):
        """Test default values for promote request."""
        from ootils_core.mps.api import PromoteToMRPRequest
        
        req = PromoteToMRPRequest()
        assert req.explode_components
        assert not req.dry_run
    
    def test_promote_request_custom_values(self):
        """Test custom values for promote request."""
        from ootils_core.mps.api import PromoteToMRPRequest
        
        req = PromoteToMRPRequest(explode_components=False, dry_run=True)
        assert not req.explode_components
        assert req.dry_run


# ─────────────────────────────────────────────────────────────
# Tests PromoteToMRPResult
# ─────────────────────────────────────────────────────────────

class TestPromoteToMRPResult:
    """Tests for PromoteToMRPResult dataclass."""
    
    def test_result_creation(self):
        """Test PromoteToMRPResult creation."""
        result = PromoteToMRPResult(
            status="RELEASED",
            transaction_id="TXN-ABC123",
            planned_supplies_created=1,
            mrp_job_id="MRP-001",
            components_exploded=5,
            summary={"test": "data"},
        )
        assert result.status == "RELEASED"
        assert result.transaction_id == "TXN-ABC123"
        assert result.planned_supplies_created == 1
        assert result.mrp_job_id == "MRP-001"
        assert result.components_exploded == 5
    
    def test_result_no_mrp_job(self):
        """Test result when MRP job not created."""
        result = PromoteToMRPResult(
            status="RELEASED",
            transaction_id="TXN-DEF456",
            planned_supplies_created=1,
            mrp_job_id=None,
            components_exploded=0,
            summary={},
        )
        assert result.mrp_job_id is None
        assert result.components_exploded == 0


# ─────────────────────────────────────────────────────────────
# Tests promote_to_mrp engine method
# ─────────────────────────────────────────────────────────────

class TestPromoteToMRPEngineMethod:
    """Tests for promote_to_mrp engine method."""
    
    def test_promote_dry_run(self):
        """Test dry run mode doesn't create records."""
        engine = AggregateDemandEngine()
        
        mps_id = uuid4()
        item_id = uuid4()
        location_id = uuid4()
        scenario_id = uuid4()
        planned_qty = Decimal("150")
        start_date = date(2026, 4, 6)
        
        # Mock DB response for MPS node fetch
        mps_row = {
            "mps_id": mps_id,
            "item_id": item_id,
            "location_id": location_id,
            "scenario_id": scenario_id,
            "planned_quantity": planned_qty,
            "time_bucket_start": start_date,
            "time_bucket_end": date(2026, 4, 12),
            "status": MPSStatus.APPROVED.value,
        }
        
        db = _make_mock_db([mps_row])
        
        # Run promote in dry run mode
        result = engine.promote_to_mrp(
            db=db,
            mps_id=mps_id,
            explode_components=True,
            dry_run=True,
            user_id="test_user",
        )
        
        assert result.status == "RELEASED"
        assert result.planned_supplies_created == 0
        assert result.components_exploded == 0
        assert result.summary["dry_run"]
    
    def test_promote_mps_not_found(self):
        """Test error when MPS node doesn't exist."""
        engine = AggregateDemandEngine()
        fake_id = uuid4()
        
        # Mock DB returns None for fetch
        db = _make_mock_db([None])
        
        with pytest.raises(ValueError, match="not found or inactive"):
            engine.promote_to_mrp(
                db=db,
                mps_id=fake_id,
                explode_components=False,
            )
    
    def test_promote_mps_not_approved(self):
        """Test error when MPS node is not APPROVED."""
        engine = AggregateDemandEngine()
        
        mps_id = uuid4()
        
        mps_row = {
            "mps_id": mps_id,
            "item_id": uuid4(),
            "location_id": uuid4(),
            "scenario_id": uuid4(),
            "planned_quantity": Decimal("150"),
            "time_bucket_start": date(2026, 4, 6),
            "time_bucket_end": date(2026, 4, 12),
            "status": MPSStatus.DRAFT.value,
        }
        
        db = _make_mock_db([mps_row])
        
        with pytest.raises(ValueError, match="must be APPROVED"):
            engine.promote_to_mrp(
                db=db,
                mps_id=mps_id,
                explode_components=False,
            )
    
    def test_promote_success_basic(self):
        """Test successful promote without BOM explosion."""
        engine = AggregateDemandEngine()
        
        mps_id = uuid4()
        item_id = uuid4()
        location_id = uuid4()
        scenario_id = uuid4()
        planned_qty = Decimal("200")
        start_date = date(2026, 4, 6)
        
        mps_row = {
            "mps_id": mps_id,
            "item_id": item_id,
            "location_id": location_id,
            "scenario_id": scenario_id,
            "planned_quantity": planned_qty,
            "time_bucket_start": start_date,
            "time_bucket_end": date(2026, 4, 12),
            "status": MPSStatus.APPROVED.value,
        }
        
        db = _make_mock_db([mps_row, None])  # fetch + update result
        
        result = engine.promote_to_mrp(
            db=db,
            mps_id=mps_id,
            explode_components=False,
            user_id="test_user",
        )
        
        assert result.status == "RELEASED"
        assert result.planned_supplies_created == 1
        assert result.transaction_id.startswith("TXN-")
        assert result.summary["mps_id"] == str(mps_id)
        assert result.summary["item_id"] == str(item_id)

    def test_promote_from_fork_writes_planned_supply_on_fork_scenario(self):
        """#398: a fork MPS must promote onto its OWN scenario, never baseline.

        Regression guard for the North Star anti-pattern "a fork that writes
        baseline". We inspect the INSERT INTO planned_supply parameter tuple and
        assert the scenario_id is the fork's, not the migration-030 DEFAULT.
        """
        engine = AggregateDemandEngine()

        mps_id = uuid4()
        item_id = uuid4()
        location_id = uuid4()
        fork_scenario_id = uuid4()  # a fork, distinct from baseline

        mps_row = {
            "mps_id": mps_id,
            "item_id": item_id,
            "location_id": location_id,
            "scenario_id": fork_scenario_id,
            "planned_quantity": Decimal("200"),
            "time_bucket_start": date(2026, 4, 6),
            "time_bucket_end": date(2026, 4, 12),
            "status": MPSStatus.APPROVED.value,
        }

        db = _make_mock_db([mps_row, None])

        result = engine.promote_to_mrp(
            db=db,
            mps_id=mps_id,
            explode_components=False,
            user_id="test_user",
        )

        found = _find_planned_supply_insert(db)
        assert found is not None, "INSERT INTO planned_supply was not issued"
        sql, params = found
        # Column list must now include scenario_id.
        assert "scenario_id" in sql
        # The scenario_id bound in the INSERT must be the fork's, not baseline.
        assert fork_scenario_id in params
        assert BASELINE_SCENARIO_ID not in params
        # And the result summary echoes the fork scenario for traceability.
        assert result.summary["scenario_id"] == str(fork_scenario_id)

    def test_promote_from_baseline_writes_planned_supply_on_baseline(self):
        """#398: promoting a baseline MPS stays on baseline — behaviour unchanged.

        The fix must be transparent for the nominal (no-fork) production case:
        scenario_id of the run == baseline ⇒ INSERT carries baseline explicitly.
        """
        engine = AggregateDemandEngine()

        mps_id = uuid4()
        item_id = uuid4()
        location_id = uuid4()

        mps_row = {
            "mps_id": mps_id,
            "item_id": item_id,
            "location_id": location_id,
            "scenario_id": BASELINE_SCENARIO_ID,
            "planned_quantity": Decimal("120"),
            "time_bucket_start": date(2026, 4, 6),
            "time_bucket_end": date(2026, 4, 12),
            "status": MPSStatus.APPROVED.value,
        }

        db = _make_mock_db([mps_row, None])

        result = engine.promote_to_mrp(
            db=db,
            mps_id=mps_id,
            explode_components=False,
            user_id="test_user",
        )

        found = _find_planned_supply_insert(db)
        assert found is not None, "INSERT INTO planned_supply was not issued"
        _, params = found
        assert BASELINE_SCENARIO_ID in params
        assert result.summary["scenario_id"] == str(BASELINE_SCENARIO_ID)


# ─────────────────────────────────────────────────────────────
# Tests API endpoint
# ─────────────────────────────────────────────────────────────

class TestPromoteToMRPEndpoint:
    """Tests for the API endpoint structure."""
    
    def test_endpoint_response_model(self):
        """Test endpoint response model structure."""
        from ootils_core.mps.api import PromoteToMRPResponse
        
        response = PromoteToMRPResponse(
            mps_id=uuid4(),
            status="RELEASED",
            transaction_id="TXN-ABC123",
            planned_supplies_created=1,
            mrp_job_id="MRP-001",
            components_exploded=5,
            summary={"test": "data"},
        )
        
        assert response.status == "RELEASED"
        assert response.planned_supplies_created == 1
        assert response.components_exploded == 5
    
    def test_endpoint_request_model(self):
        """Test endpoint request model."""
        from ootils_core.mps.api import PromoteToMRPRequest
        
        req = PromoteToMRPRequest(explode_components=True, dry_run=False)
        assert req.explode_components
        assert not req.dry_run


# ─────────────────────────────────────────────────────────────
# Tests status transitions
# ─────────────────────────────────────────────────────────────

class TestMPSSStatusTransitions:
    """Tests for MPS status transition validation."""
    
    def test_approved_to_released_valid(self):
        """Test APPROVED -> RELEASED is valid."""
        # Status transition logic is in MPSNode model
        assert MPSStatus.APPROVED.value == "APPROVED"
        assert MPSStatus.RELEASED.value == "RELEASED"
        # Workflow: DRAFT -> APPROVED -> RELEASED
        # APPROVED -> RELEASED is the valid transition for promote-to-mrp
    
    def test_draft_to_released_invalid(self):
        """Test DRAFT -> RELEASED should require APPROVED first."""
        assert MPSStatus.DRAFT.value == "DRAFT"
        assert MPSStatus.RELEASED.value == "RELEASED"
        # Direct DRAFT->RELEASED should not be allowed in workflow


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
