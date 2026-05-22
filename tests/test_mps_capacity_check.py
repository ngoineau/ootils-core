"""
Tests pour MPS-003 — Capacity Check Pre-MRP.

Couverture:
- CapacityCheckEngine: vérification de capacité
- CapacityCheckEngine: détection de violations (overload)
- CapacityCheckEngine: génération de suggestions (delay, reduce, outsource)
- API endpoint POST /v1/mps/capacity-check
- API endpoint GET /v1/mps/{id}/suggest-adjustments
"""

import pytest
from datetime import date
from decimal import Decimal
from uuid import uuid4
from unittest.mock import MagicMock

import psycopg

from ootils_core.mps.capacity_engine import (
    CapacityCheckEngine,
    CapacityViolation,
    AdjustmentSuggestion,
    CapacityCheckResult,
)


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _make_mock_db():
    """Créer une mock de connexion PostgreSQL."""
    conn = MagicMock(spec=psycopg.Connection)
    return conn


def _make_execute_mock(responses):
    """Créer un mock pour execute() qui retourne des réponses prédéfinies."""
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
    
    return MagicMock(side_effect=execute_side_effect)


# ─────────────────────────────────────────────────────────────
# Tests CapacityCheckEngine
# ─────────────────────────────────────────────────────────────

class TestCapacityCheckEngine:
    """Tests pour le moteur de vérification de capacité."""

    def test_check_capacity_empty_list(self):
        """Vérifier qu'une liste vide retourne feasible=True."""
        engine = CapacityCheckEngine()
        db = _make_mock_db()
        
        result = engine.check_capacity(db, [], horizon_buffer_days=7)
        
        assert result.feasible is True
        assert result.violations == []
        assert result.suggested_adjustments == []
        assert result.summary["checked_count"] == 0

    def test_check_capacity_no_valid_nodes(self):
        """Vérifier qu'aucun node valide retourne feasible=True."""
        engine = CapacityCheckEngine()
        db = _make_mock_db()
        
        # Mock: aucun MPS node trouvé
        db.execute = _make_execute_mock([[]])
        
        mps_ids = [uuid4(), uuid4()]
        result = engine.check_capacity(db, mps_ids, horizon_buffer_days=7)
        
        assert result.feasible is True
        assert result.summary["checked_count"] == 0

    def test_check_capacity_feasible(self):
        """Vérifier un cas où la capacité est suffisante."""
        engine = CapacityCheckEngine()
        db = _make_mock_db()
        
        mps_id = uuid4()
        item_id = uuid4()
        location_id = uuid4()
        
        # Mock MPS nodes
        mps_row = {
            "mps_id": mps_id,
            "item_id": item_id,
            "location_id": location_id,
            "scenario_id": uuid4(),
            "time_bucket": "2026-W20",
            "time_bucket_start": date(2026, 5, 11),
            "time_bucket_end": date(2026, 5, 17),
            "planned_quantity": Decimal("100"),
            "status": "DRAFT",
            "active": True,
        }
        
        # Mock resources (aucune ressource critique)
        resource_rows = []
        
        # Setup execute mock
        db.execute = _make_execute_mock([
            [mps_row],  # fetch MPS nodes
            resource_rows,  # fetch critical resources
        ])
        
        result = engine.check_capacity(db, [mps_id], horizon_buffer_days=7)
        
        assert result.feasible is True
        assert result.violations == []
        assert result.summary["checked_count"] == 1

    def test_check_capacity_with_violation(self):
        """Vérifier la détection d'une violation de capacité."""
        engine = CapacityCheckEngine()
        db = _make_mock_db()
        
        mps_id = uuid4()
        item_id = uuid4()
        location_id = uuid4()
        resource_id = uuid4()
        
        # Mock MPS nodes
        mps_row = {
            "mps_id": mps_id,
            "item_id": item_id,
            "location_id": location_id,
            "scenario_id": uuid4(),
            "time_bucket": "2026-W20",
            "time_bucket_start": date(2026, 5, 11),
            "time_bucket_end": date(2026, 5, 17),
            "planned_quantity": Decimal("500"),
            "status": "DRAFT",
            "active": True,
        }
        
        # Mock resource
        resource_row = {
            "resource_id": resource_id,
            "external_id": "WC-001",
            "name": "Work Center 1",
            "resource_type": "work_center",
            "capacity_per_day": 50,
            "capacity_unit": "hours",
            "location_id": location_id,
        }
        
        # Mock capacity override
        capacity_override_rows = [
            {"override_date": date(2026, 5, 11), "capacity": 50},
        ]
        
        # Mock resource capacity fetch (separate query for capacity_per_day)
        resource_capacity_row = {"capacity_per_day": 50}
        
        # Setup execute mock - order must match actual query sequence:
        # 1. fetch_mps_nodes
        # 2. get_critical_resources  
        # 3. get_base_resource_load
        # 4. get_resource_capacity (capacity_per_day)
        # 5. get_resource_capacity (overrides)
        # 6. get_resource_capacity (operational calendar)
        db.execute = _make_execute_mock([
            [mps_row],  # fetch MPS nodes
            [resource_row],  # fetch critical resources
            [],  # base load (vide)
            [resource_capacity_row],  # resource capacity_per_day
            capacity_override_rows,  # capacity overrides
            [],  # operational calendar
        ])
        
        result = engine.check_capacity(db, [mps_id], horizon_buffer_days=7)
        
        # Devrait détecter une violation (500 > 250 capacity)
        assert result.feasible is False
        assert len(result.violations) > 0 or len(result.suggested_adjustments) > 0


class TestCapacityViolation:
    """Tests pour le modèle CapacityViolation."""

    def test_violation_creation(self):
        """Créer une violation de capacité."""
        violation = CapacityViolation(
            violation_type="overload",
            resource_id=uuid4(),
            resource_external_id="WC-001",
            resource_name="Work Center 1",
            period_start=date(2026, 5, 11),
            period_end=date(2026, 5, 17),
            required_capacity=Decimal("500"),
            available_capacity=Decimal("250"),
            overload_pct=Decimal("100"),
            affected_mps_ids=[uuid4()],
            severity="high",
        )
        
        assert violation.violation_type == "overload"
        assert violation.overload_pct == Decimal("100")
        assert violation.severity == "high"


class TestAdjustmentSuggestion:
    """Tests pour le modèle AdjustmentSuggestion."""

    def test_suggestion_delay(self):
        """Créer une suggestion de type delay."""
        suggestion = AdjustmentSuggestion(
            suggestion_type="delay",
            mps_id=uuid4(),
            description="Décaler production de 2026-05-11 à 2026-05-18",
            original_quantity=Decimal("100"),
            suggested_quantity=None,
            original_date=date(2026, 5, 11),
            suggested_date=date(2026, 5, 18),
            impact_description="Évite la surcharge en décalant",
            confidence=Decimal("0.8"),
        )
        
        assert suggestion.suggestion_type == "delay"
        assert suggestion.suggested_date == date(2026, 5, 18)
        assert suggestion.confidence == Decimal("0.8")

    def test_suggestion_reduce(self):
        """Créer une suggestion de type reduce."""
        suggestion = AdjustmentSuggestion(
            suggestion_type="reduce",
            mps_id=uuid4(),
            description="Réduire quantité de 100 à 80",
            original_quantity=Decimal("100"),
            suggested_quantity=Decimal("80"),
            original_date=date(2026, 5, 11),
            suggested_date=None,
            impact_description="Réduit la charge de 20%",
            confidence=Decimal("0.7"),
        )
        
        assert suggestion.suggestion_type == "reduce"
        assert suggestion.suggested_quantity == Decimal("80")

    def test_suggestion_outsource(self):
        """Créer une suggestion de type outsource."""
        suggestion = AdjustmentSuggestion(
            suggestion_type="outsource",
            mps_id=uuid4(),
            description="Sous-traiter 30 unités",
            original_quantity=Decimal("100"),
            suggested_quantity=Decimal("70"),
            original_date=date(2026, 5, 11),
            suggested_date=None,
            impact_description="Sous-traitance partielle",
            confidence=Decimal("0.6"),
        )
        
        assert suggestion.suggestion_type == "outsource"
        assert suggestion.suggested_quantity == Decimal("70")


class TestSeverityClassification:
    """Tests pour la classification de sévérité."""

    def test_severity_low(self):
        """Sévérité low pour overload < 10%."""
        engine = CapacityCheckEngine()
        assert engine._classify_severity(5) == "low"
        assert engine._classify_severity(9.9) == "low"

    def test_severity_medium(self):
        """Sévérité medium pour overload 10-25%."""
        engine = CapacityCheckEngine()
        assert engine._classify_severity(10) == "medium"
        assert engine._classify_severity(20) == "medium"
        assert engine._classify_severity(24.9) == "medium"

    def test_severity_high(self):
        """Sévérité high pour overload 25-50%."""
        engine = CapacityCheckEngine()
        assert engine._classify_severity(25) == "high"
        assert engine._classify_severity(40) == "high"
        assert engine._classify_severity(49.9) == "high"

    def test_severity_critical(self):
        """Sévérité critical pour overload >= 50%."""
        engine = CapacityCheckEngine()
        assert engine._classify_severity(50) == "critical"
        assert engine._classify_severity(100) == "critical"


# ─────────────────────────────────────────────────────────────
# Tests API Endpoints
# ─────────────────────────────────────────────────────────────

class TestCapacityCheckEndpoint:
    """Tests pour l'endpoint POST /v1/mps/capacity-check."""

    def test_capacity_check_request_validation(self):
        """Valider le modèle de requête CapacityCheckRequest."""
        from ootils_core.mps.api import CapacityCheckRequest
        
        # Cas valide
        req = CapacityCheckRequest(
            mps_node_ids=["550e8400-e29b-41d4-a716-446655440000"],
            horizon_buffer_days=7,
        )
        assert len(req.mps_node_ids) == 1
        assert req.horizon_buffer_days == 7
        
        # Cas avec buffer invalide (trop grand)
        with pytest.raises(Exception):  # Validation error
            CapacityCheckRequest(
                mps_node_ids=["550e8400-e29b-41d4-a716-446655440000"],
                horizon_buffer_days=100,  # > 90
            )

    def test_capacity_check_response_format(self):
        """Valider le format de réponse CapacityCheckResponse."""
        from ootils_core.mps.api import (
            CapacityCheckResponse,
            CapacityViolationOut,
            AdjustmentSuggestionOut,
        )
        
        violation = CapacityViolationOut(
            violation_type="overload",
            resource_id=uuid4(),
            resource_external_id="WC-001",
            resource_name="Work Center 1",
            period_start=date(2026, 5, 11),
            period_end=date(2026, 5, 17),
            required_capacity="500",
            available_capacity="250",
            overload_pct="100",
            affected_mps_ids=[uuid4()],
            severity="high",
        )
        
        suggestion = AdjustmentSuggestionOut(
            suggestion_type="delay",
            mps_id=uuid4(),
            description="Décaler production",
            original_quantity="100",
            suggested_quantity=None,
            original_date=date(2026, 5, 11),
            suggested_date=date(2026, 5, 18),
            impact_description="Évite surcharge",
            confidence="0.8",
        )
        
        response = CapacityCheckResponse(
            feasible=False,
            violations=[violation],
            suggested_adjustments=[suggestion],
            summary={"checked_count": 1, "violation_count": 1},
        )
        
        assert response.feasible is False
        assert len(response.violations) == 1
        assert len(response.suggested_adjustments) == 1


class TestSuggestAdjustmentsEndpoint:
    """Tests pour l'endpoint GET /v1/mps/{id}/suggest-adjustments."""

    def test_suggest_adjustments_response_format(self):
        """Valider le format de réponse des suggestions."""
        from ootils_core.mps.api import AdjustmentSuggestionOut
        
        suggestion = AdjustmentSuggestionOut(
            suggestion_type="reduce",
            mps_id=uuid4(),
            description="Réduire quantité",
            original_quantity="100",
            suggested_quantity="80",
            original_date=date(2026, 5, 11),
            suggested_date=None,
            impact_description="Réduit charge",
            confidence="0.7",
        )
        
        assert suggestion.suggestion_type == "reduce"
        assert suggestion.original_quantity == "100"
        assert suggestion.suggested_quantity == "80"
        assert suggestion.confidence == "0.7"


# ─────────────────────────────────────────────────────────────
# Tests Integration (avec mock DB complet)
# ─────────────────────────────────────────────────────────────

class TestCapacityCheckIntegration:
    """Tests d'intégration pour la vérification de capacité."""

    def test_full_capacity_check_flow(self):
        """Tester le flux complet de vérification."""
        engine = CapacityCheckEngine()
        db = _make_mock_db()
        
        mps_id = uuid4()
        item_id = uuid4()
        location_id = uuid4()
        resource_id = uuid4()
        
        # Setup comprehensive mock
        mps_row = {
            "mps_id": mps_id,
            "item_id": item_id,
            "location_id": location_id,
            "scenario_id": uuid4(),
            "time_bucket": "2026-W20",
            "time_bucket_start": date(2026, 5, 11),
            "time_bucket_end": date(2026, 5, 17),
            "planned_quantity": Decimal("100"),
            "status": "DRAFT",
            "active": True,
        }
        
        resource_row = {
            "resource_id": resource_id,
            "external_id": "WC-001",
            "name": "Work Center 1",
            "resource_type": "work_center",
            "capacity_per_day": 50.0,
            "capacity_unit": "hours",
            "location_id": location_id,
        }
        
        # Mock chain: MPS → Resources → Base Load → Capacity → Calendar
        db.execute = _make_execute_mock([
            [mps_row],  # fetch_mps_nodes
            [resource_row],  # get_critical_resources
            [],  # get_base_resource_load (vide)
            [],  # get_resource_capacity overrides (vide)
            [],  # operational_calendars (vide, fallback Mon-Fri)
        ])
        
        result = engine.check_capacity(db, [mps_id], horizon_buffer_days=7)
        
        # Vérifier structure du résultat
        assert isinstance(result, CapacityCheckResult)
        assert "checked_count" in result.summary
        assert "horizon_start" in result.summary
        assert "horizon_end" in result.summary
        assert result.summary["checked_count"] == 1

    def test_multiple_mps_nodes(self):
        """Tester avec plusieurs MPS nodes."""
        engine = CapacityCheckEngine()
        db = _make_mock_db()
        
        mps_ids = [uuid4() for _ in range(3)]
        item_id = uuid4()
        location_id = uuid4()
        
        # Créer 3 MPS rows
        mps_rows = [
            {
                "mps_id": mps_id,
                "item_id": item_id,
                "location_id": location_id,
                "scenario_id": uuid4(),
                "time_bucket": f"2026-W{20+i}",
                "time_bucket_start": date(2026, 5, 11 + i*7),
                "time_bucket_end": date(2026, 5, 17 + i*7),
                "planned_quantity": Decimal("100"),
                "status": "DRAFT",
                "active": True,
            }
            for i, mps_id in enumerate(mps_ids)
        ]
        
        db.execute = _make_execute_mock([
            mps_rows,  # fetch_mps_nodes
            [],  # get_critical_resources (aucune ressource)
        ])
        
        result = engine.check_capacity(db, mps_ids, horizon_buffer_days=7)
        
        assert result.summary["checked_count"] == 3
        assert result.feasible is True  # Pas de ressources = pas de violations
