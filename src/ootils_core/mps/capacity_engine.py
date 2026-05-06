"""
CapacityCheckEngine — Vérification de capacité avant release du MPS.

Utilise le moteur RCCP existant pour vérifier la faisabilité capacitaire
des MPS nodes avant leur release.

Fonctionnalités:
- Vérifie l'impact capacitaire d'un ensemble de MPS nodes
- Détecte les violations: surcharge ressources, conflits calendriers
- Génère des suggestions d'ajustement: delay, reduce, outsource
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID

import psycopg

logger = logging.getLogger(__name__)


@dataclass
class CapacityViolation:
    """Représente une violation de capacité."""
    violation_type: str  # 'overload' | 'calendar_conflict' | 'resource_unavailable'
    resource_id: UUID
    resource_external_id: str
    resource_name: str
    period_start: date
    period_end: date
    required_capacity: Decimal
    available_capacity: Decimal
    overload_pct: Decimal
    affected_mps_ids: List[UUID]
    severity: str  # 'low' | 'medium' | 'high' | 'critical'


@dataclass
class AdjustmentSuggestion:
    """Suggestion d'ajustement pour résoudre une violation."""
    suggestion_type: str  # 'delay' | 'reduce' | 'outsource'
    mps_id: UUID
    description: str
    original_quantity: Decimal
    suggested_quantity: Optional[Decimal]
    original_date: date
    suggested_date: Optional[date]
    impact_description: str
    confidence: Decimal  # 0-1, confiance dans la suggestion


@dataclass
class CapacityCheckResult:
    """Résultat de la vérification de capacité."""
    feasible: bool
    violations: List[CapacityViolation]
    suggested_adjustments: List[AdjustmentSuggestion]
    summary: Dict[str, Any]


@dataclass
class ResourceLoad:
    """Charge sur une ressource pour une période."""
    resource_id: UUID
    resource_external_id: str
    period_start: date
    period_end: date
    load: Decimal
    capacity: Decimal
    utilization_pct: Decimal


class CapacityCheckEngine:
    """
    Moteur de vérification de capacité pour les MPS nodes.
    
    Utilise les données RCCP pour vérifier si les MPS nodes peuvent
    être released sans créer de surcharge capacitaire.
    
    Exemple:
        engine = CapacityCheckEngine()
        result = engine.check_capacity(
            db=conn,
            mps_node_ids=[mps_uuid_1, mps_uuid_2],
            horizon_buffer_days=7,
        )
    """
    
    def __init__(self):
        """Initialiser le moteur de vérification de capacité."""
        logger.info("CapacityCheckEngine initialisé")
    
    def check_capacity(
        self,
        db: psycopg.Connection,
        mps_node_ids: List[UUID],
        horizon_buffer_days: int = 7,
    ) -> CapacityCheckResult:
        """
        Vérifier la faisabilité capacitaire d'un ensemble de MPS nodes.
        
        Args:
            db: Connection PostgreSQL.
            mps_node_ids: Liste des UUIDs des MPS nodes à vérifier.
            horizon_buffer_days: Nombre de jours de buffer avant/après l'horizon MPS.
        
        Returns:
            CapacityCheckResult avec violations et suggestions.
        """
        if not mps_node_ids:
            return CapacityCheckResult(
                feasible=True,
                violations=[],
                suggested_adjustments=[],
                summary={"checked_count": 0, "message": "No MPS nodes to check"},
            )
        
        # 1. Fetch MPS nodes avec détails
        mps_nodes = self._fetch_mps_nodes(db, mps_node_ids)
        if not mps_nodes:
            return CapacityCheckResult(
                feasible=True,
                violations=[],
                suggested_adjustments=[],
                summary={"checked_count": 0, "message": "No valid MPS nodes found"},
            )
        
        # 2. Déterminer l'horizon de vérification
        horizon_start = min(n["time_bucket_start"] for n in mps_nodes) - timedelta(days=horizon_buffer_days)
        horizon_end = max(n["time_bucket_end"] for n in mps_nodes) + timedelta(days=horizon_buffer_days)
        
        # 3. Identifier les ressources critiques pour ces MPS nodes
        #    (via BOM et routing des items)
        critical_resources = self._get_critical_resources(db, mps_nodes)
        
        # 4. Pour chaque ressource, calculer la charge ajoutée par les MPS nodes
        violations: List[CapacityViolation] = []
        for resource in critical_resources:
            resource_id = resource["resource_id"]
            
            # Calculer la charge de base (sans les MPS nodes)
            base_load = self._get_base_resource_load(
                db, resource_id, horizon_start, horizon_end
            )
            
            # Calculer la capacité disponible
            capacity = self._get_resource_capacity(
                db, resource_id, resource["location_id"],
                horizon_start, horizon_end
            )
            
            # Calculer la charge ajoutée par les MPS nodes
            added_load = self._calculate_added_load_from_mps(
                db, mps_nodes, resource_id
            )
            
            # Vérifier les violations par période
            period_violations = self._check_period_violations(
                resource, base_load, added_load, capacity,
                horizon_start, horizon_end, mps_node_ids
            )
            violations.extend(period_violations)
        
        # 5. Générer des suggestions d'ajustement
        suggestions = self._generate_adjustment_suggestions(
            db, violations, mps_nodes
        )
        
        # 6. Déterminer si feasible
        feasible = len(violations) == 0
        
        summary = {
            "checked_count": len(mps_nodes),
            "violation_count": len(violations),
            "suggestion_count": len(suggestions),
            "horizon_start": horizon_start.isoformat(),
            "horizon_end": horizon_end.isoformat(),
            "resources_checked": len(critical_resources),
        }
        
        logger.info(
            "mps.capacity_check mps_count=%d feasible=%s violations=%d suggestions=%d",
            len(mps_nodes), feasible, len(violations), len(suggestions),
        )
        
        return CapacityCheckResult(
            feasible=feasible,
            violations=violations,
            suggested_adjustments=suggestions,
            summary=summary,
        )
    
    def _fetch_mps_nodes(
        self,
        db: psycopg.Connection,
        mps_node_ids: List[UUID],
    ) -> List[Dict[str, Any]]:
        """Fetch les MPS nodes avec leurs détails."""
        if not mps_node_ids:
            return []
        
        placeholders = ",".join(["%s"] * len(mps_node_ids))
        rows = db.execute(
            f"""
            SELECT 
                mps_id, item_id, location_id, scenario_id,
                time_bucket, time_bucket_start, time_bucket_end,
                planned_quantity, status, active
            FROM mps_nodes
            WHERE mps_id IN ({placeholders})
              AND active = TRUE
            ORDER BY time_bucket_start ASC
            """,
            mps_node_ids,
        ).fetchall()
        
        return [dict(row) for row in rows]
    
    def _get_critical_resources(
        self,
        db: psycopg.Connection,
        mps_nodes: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Identifier les ressources critiques pour les MPS nodes.
        
        Utilise le BOM et routing des items pour trouver les ressources
        contraintes (work centers, machines, etc.).
        """
        item_ids = list(set(n["item_id"] for n in mps_nodes))
        location_ids = list(set(n["location_id"] for n in mps_nodes))
        
        if not item_ids:
            return []
        
        # Fetch resources from BOM/routing
        # Simplified: fetch all resources associated with these items via BOM
        item_placeholders = ",".join(["%s"] * len(item_ids))
        
        rows = db.execute(
            f"""
            SELECT DISTINCT
                r.resource_id,
                r.external_id,
                r.name,
                r.resource_type,
                r.capacity_per_day,
                r.capacity_unit,
                r.location_id
            FROM resources r
            JOIN bill_of_materials bom ON bom.location_id = r.location_id
            JOIN routing_operations ro ON ro.bom_id = bom.bom_id
            JOIN routing_resources rr ON rr.operation_id = ro.operation_id
            WHERE bom.item_id IN ({item_placeholders})
              AND r.location_id = ANY(%s)
              AND r.active = TRUE
            """,
            item_ids + [location_ids],
        ).fetchall()
        
        # Si pas de routing trouvé, utiliser une approche par défaut:
        # associer les items aux ressources par type de location
        if not rows:
            loc_placeholders = ",".join(["%s"] * len(location_ids))
            rows = db.execute(
                f"""
                SELECT 
                    r.resource_id,
                    r.external_id,
                    r.name,
                    r.resource_type,
                    r.capacity_per_day,
                    r.capacity_unit,
                    r.location_id
                FROM resources r
                WHERE r.location_id IN ({loc_placeholders})
                  AND r.active = TRUE
                LIMIT 10
                """,
                location_ids,
            ).fetchall()
        
        return [dict(row) for row in rows]
    
    def _get_base_resource_load(
        self,
        db: psycopg.Connection,
        resource_id: UUID,
        horizon_start: date,
        horizon_end: date,
    ) -> Dict[date, Decimal]:
        """
        Obtenir la charge de base d'une ressource (sans les MPS nodes).
        
        Inclut les WorkOrderSupply et PlannedSupply existants.
        """
        rows = db.execute(
            """
            SELECT 
                n.time_ref AS period_date,
                COALESCE(n.quantity, 0) AS quantity
            FROM nodes n
            JOIN edges e ON e.from_node_id = n.node_id
            JOIN nodes rn ON rn.node_id = e.to_node_id
            WHERE n.node_type IN ('WorkOrderSupply', 'PlannedSupply')
              AND e.edge_type = 'consumes_resource'
              AND e.active = TRUE
              AND n.active = TRUE
              AND rn.resource_id = %s
              AND n.time_ref BETWEEN %s AND %s
            """,
            (resource_id, horizon_start, horizon_end),
        ).fetchall()
        
        load_by_date: Dict[date, Decimal] = {}
        for row in rows:
            period_date = row["period_date"]
            if period_date:
                qty = Decimal(str(row["quantity"]))
                load_by_date[period_date] = load_by_date.get(period_date, Decimal("0")) + qty
        
        return load_by_date
    
    def _calculate_added_load_from_mps(
        self,
        db: psycopg.Connection,
        mps_nodes: List[Dict[str, Any]],
        resource_id: UUID,
    ) -> Dict[date, Decimal]:
        """
        Calculer la charge ajoutée par les MPS nodes sur une ressource.
        
        Utilise le BOM/routing pour convertir les quantités MPS en
        charge ressource (hours, units, etc.).
        """
        # Simplified: assume 1:1 conversion pour V1
        # Dans une version avancée, utiliser routing_times et efficiency factors
        
        added_load: Dict[date, Decimal] = {}
        
        for mps in mps_nodes:
            # Distribuer la charge sur la période du time bucket
            bucket_start = mps["time_bucket_start"]
            bucket_end = mps["time_bucket_end"]
            quantity = Decimal(str(mps["planned_quantity"]))
            
            # Nombre de jours dans le bucket
            num_days = (bucket_end - bucket_start).days + 1
            if num_days <= 0:
                num_days = 1
            
            # Charge quotidienne moyenne
            daily_load = quantity / num_days
            
            # Ajouter à chaque jour du bucket
            current = bucket_start
            while current <= bucket_end:
                added_load[current] = added_load.get(current, Decimal("0")) + daily_load
                current += timedelta(days=1)
        
        return added_load
    
    def _get_resource_capacity(
        self,
        db: psycopg.Connection,
        resource_id: UUID,
        location_id: Optional[UUID],
        horizon_start: date,
        horizon_end: date,
    ) -> Dict[date, Decimal]:
        """
        Obtenir la capacité disponible d'une ressource par jour.
        
        Prend en compte:
        - capacity_per_day de la ressource
        - operational_calendars (jours ouvrés)
        - resource_capacity_overrides
        """
        # Fetch resource base capacity
        resource_row = db.execute(
            """
            SELECT capacity_per_day
            FROM resources
            WHERE resource_id = %s
            """,
            (resource_id,),
        ).fetchone()
        
        if not resource_row:
            return {}
        
        base_capacity = Decimal(str(resource_row["capacity_per_day"]))
        
        # Fetch overrides
        override_rows = db.execute(
            """
            SELECT override_date, capacity
            FROM resource_capacity_overrides
            WHERE resource_id = %s
              AND override_date BETWEEN %s AND %s
            """,
            (resource_id, horizon_start, horizon_end),
        ).fetchall()
        
        overrides: Dict[date, Decimal] = {}
        for row in override_rows:
            overrides[row["override_date"]] = Decimal(str(row["capacity"]))
        
        # Build capacity by date
        capacity_by_date: Dict[date, Decimal] = {}
        current = horizon_start
        
        while current <= horizon_end:
            if current in overrides:
                capacity_by_date[current] = overrides[current]
            else:
                # Check operational calendar
                if location_id:
                    cal_row = db.execute(
                        """
                        SELECT is_working_day, capacity_factor
                        FROM operational_calendars
                        WHERE location_id = %s AND calendar_date = %s
                        """,
                        (location_id, current),
                    ).fetchone()
                    
                    if cal_row and not cal_row["is_working_day"]:
                        capacity_by_date[current] = Decimal("0")
                    elif cal_row and cal_row["capacity_factor"]:
                        capacity_by_date[current] = base_capacity * Decimal(str(cal_row["capacity_factor"]))
                    else:
                        # Fallback: Mon-Fri working days
                        if current.weekday() < 5:
                            capacity_by_date[current] = base_capacity
                        else:
                            capacity_by_date[current] = Decimal("0")
                else:
                    # No location: use Mon-Fri heuristic
                    if current.weekday() < 5:
                        capacity_by_date[current] = base_capacity
                    else:
                        capacity_by_date[current] = Decimal("0")
            
            current += timedelta(days=1)
        
        return capacity_by_date
    
    def _check_period_violations(
        self,
        resource: Dict[str, Any],
        base_load: Dict[date, Decimal],
        added_load: Dict[date, Decimal],
        capacity: Dict[date, Decimal],
        horizon_start: date,
        horizon_end: date,
        mps_node_ids: List[UUID],
    ) -> List[CapacityViolation]:
        """
        Vérifier les violations de capacité par période hebdomadaire.
        """
        violations: List[CapacityViolation] = []
        
        # Agréger par semaine
        current = horizon_start
        while current <= horizon_end:
            week_end = min(current + timedelta(days=6), horizon_end)
            
            # Calculer totaux pour la semaine
            week_base_load = sum(
                base_load.get(d, Decimal("0"))
                for d in self._date_range(current, week_end)
            )
            week_added_load = sum(
                added_load.get(d, Decimal("0"))
                for d in self._date_range(current, week_end)
            )
            week_capacity = sum(
                capacity.get(d, Decimal("0"))
                for d in self._date_range(current, week_end)
            )
            
            total_load = week_base_load + week_added_load
            
            if week_capacity > 0:
                utilization = (total_load / week_capacity * Decimal("100")).quantize(Decimal("0.01"))
            else:
                utilization = Decimal("100") if total_load > 0 else Decimal("0")
            
            # Vérifier violation (>100%)
            if utilization > Decimal("100"):
                overload_pct = utilization - Decimal("100")
                severity = self._classify_severity(float(overload_pct))
                
                violation = CapacityViolation(
                    violation_type="overload",
                    resource_id=resource["resource_id"],
                    resource_external_id=resource["external_id"],
                    resource_name=resource["name"],
                    period_start=current,
                    period_end=week_end,
                    required_capacity=total_load,
                    available_capacity=week_capacity,
                    overload_pct=overload_pct,
                    affected_mps_ids=mps_node_ids,  # Simplified: tous les MPS
                    severity=severity,
                )
                violations.append(violation)
            
            current = week_end + timedelta(days=1)
        
        return violations
    
    def _date_range(self, start: date, end: date) -> List[date]:
        """Générer la liste des dates entre start et end."""
        dates = []
        current = start
        while current <= end:
            dates.append(current)
            current += timedelta(days=1)
        return dates
    
    def _classify_severity(self, overload_pct: float) -> str:
        """Classifier la sévérité d'une violation."""
        if overload_pct < 10:
            return "low"
        elif overload_pct < 25:
            return "medium"
        elif overload_pct < 50:
            return "high"
        else:
            return "critical"
    
    def _generate_adjustment_suggestions(
        self,
        db: psycopg.Connection,
        violations: List[CapacityViolation],
        mps_nodes: List[Dict[str, Any]],
    ) -> List[AdjustmentSuggestion]:
        """
        Générer des suggestions d'ajustement pour résoudre les violations.
        
        Types de suggestions:
        - delay: décaler la production à une période moins chargée
        - reduce: réduire la quantité (si possible)
        - outsource: sous-traiter une partie de la production
        """
        suggestions: List[AdjustmentSuggestion] = []
        
        for violation in violations:
            # Pour chaque MPS affecté, générer des suggestions
            for mps_id in violation.affected_mps_ids:
                mps = next((m for m in mps_nodes if m["mps_id"] == mps_id), None)
                if not mps:
                    continue
                
                original_qty = Decimal(str(mps["planned_quantity"]))
                original_date = mps["time_bucket_start"]
                
                # Suggestion 1: Delay
                suggested_date = self._find_next_available_slot(
                    db, mps, violation.resource_id,
                    violation.period_end + timedelta(days=1)
                )
                
                if suggested_date:
                    suggestions.append(
                        AdjustmentSuggestion(
                            suggestion_type="delay",
                            mps_id=mps_id,
                            description=f"Décaler production de {original_date} à {suggested_date}",
                            original_quantity=original_qty,
                            suggested_quantity=None,
                            original_date=original_date,
                            suggested_date=suggested_date,
                            impact_description="Évite la surcharge en décalant à une période disponible",
                            confidence=Decimal("0.8"),
                        )
                    )
                
                # Suggestion 2: Reduce (réduire de 20%)
                reduce_qty = (original_qty * Decimal("0.8")).quantize(Decimal("0.01"))
                suggestions.append(
                    AdjustmentSuggestion(
                        suggestion_type="reduce",
                        mps_id=mps_id,
                        description=f"Réduire quantité de {original_qty} à {reduce_qty} (sous-traiter le reste)",
                        original_quantity=original_qty,
                        suggested_quantity=reduce_qty,
                        original_date=original_date,
                        suggested_date=None,
                        impact_description="Réduit la charge de 20%, le reste peut être sous-traité",
                        confidence=Decimal("0.7"),
                    )
                )
                
                # Suggestion 3: Outsource (sous-traiter 30%)
                outsource_qty = (original_qty * Decimal("0.7")).quantize(Decimal("0.01"))
                suggestions.append(
                    AdjustmentSuggestion(
                        suggestion_type="outsource",
                        mps_id=mps_id,
                        description=f"Sous-traiter {original_qty - outsource_qty} unités, produire {outsource_qty} en interne",
                        original_quantity=original_qty,
                        suggested_quantity=outsource_qty,
                        original_date=original_date,
                        suggested_date=None,
                        impact_description="Sous-traitance partielle pour réduire la charge interne",
                        confidence=Decimal("0.6"),
                    )
                )
        
        return suggestions
    
    def _find_next_available_slot(
        self,
        db: psycopg.Connection,
        mps: Dict[str, Any],
        resource_id: UUID,
        search_from: date,
        max_days: int = 30,
    ) -> Optional[date]:
        """
        Trouver la prochaine période disponible pour la production.
        
        Cherche dans les max_days prochains jours une période où
        la ressource a de la capacité disponible.
        """
        horizon_end = search_from + timedelta(days=max_days)
        
        capacity = self._get_resource_capacity(
            db, resource_id, mps.get("location_id"),
            search_from, horizon_end
        )
        
        base_load = self._get_base_resource_load(
            db, resource_id, search_from, horizon_end
        )
        
        # Chercher un jour avec capacité > charge
        current = search_from
        while current <= horizon_end:
            day_capacity = capacity.get(current, Decimal("0"))
            day_load = base_load.get(current, Decimal("0"))
            
            if day_capacity > day_load:
                return current
            
            current += timedelta(days=1)
        
        return None
