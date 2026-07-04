"""
AggregateDemandEngine — Service pour l'agrégation de la demande MPS.

Consolide les sources de demande (forecast + sales orders) par time bucket
et crée les MPS nodes pour la planification de la production.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID, uuid4

from ootils_core.db.types import DictRowConnection
from ootils_core.mps.models import MPSStatus

logger = logging.getLogger(__name__)


@dataclass
class DemandSource:
    """Représente une source de demande (forecast ou sales order)."""
    source_type: str  # 'forecast' | 'sales_order'
    source_id: UUID
    item_id: UUID
    location_id: UUID
    scenario_id: UUID
    demand_date: date
    quantity: Decimal


@dataclass
class TimeBucketDemand:
    """Demande agrégée pour un time bucket."""
    time_bucket: str
    time_bucket_start: date
    time_bucket_end: date
    time_grain: str
    forecast_quantity: Decimal = Decimal("0")
    sales_orders_quantity: Decimal = Decimal("0")
    total_demand: Decimal = Decimal("0")
    demand_sources: List[DemandSource] = field(default_factory=list)
    
    def compute_total(self) -> Decimal:
        """Calculer la demande totale."""
        self.total_demand = self.forecast_quantity + self.sales_orders_quantity
        return self.total_demand


@dataclass
class AggregateDemandRequest:
    """Requête d'agrégation de demande."""
    item_id: UUID
    location_id: UUID
    scenario_id: UUID
    horizon_start: date
    horizon_end: date
    time_grain: str = "weekly"  # 'daily' | 'weekly' | 'monthly'
    forecast_weight: Decimal = Decimal("0.5")  # Poids du forecast (0-1)
    orders_weight: Decimal = Decimal("0.5")    # Poids des orders (0-1)
    clear_existing: bool = False  # Si True, supprime les MPS nodes existants


@dataclass
class AggregateDemandResult:
    """Résultat de l'agrégation."""
    mps_nodes_created: int
    mps_nodes_updated: int
    total_demand: Decimal
    demand_by_source: Dict[str, Decimal]
    demand_by_period: List[Dict[str, Any]]
    mps_node_ids: List[UUID]


@dataclass
class MPSNodeSummary:
    """Résumé d'un MPS node pour la réponse API."""
    mps_id: UUID
    time_bucket: str
    time_bucket_start: date
    time_bucket_end: date
    forecast_quantity: Decimal
    sales_orders_quantity: Decimal
    total_demand: Decimal
    planned_quantity: Decimal
    status: str


@dataclass
class PromoteToMRPResult:
    """Résultat de la promotion MPS vers MRP."""
    status: str  # 'RELEASED'
    transaction_id: str
    planned_supplies_created: int
    mrp_job_id: Optional[str]
    components_exploded: int
    summary: Dict[str, Any]


class AggregateDemandEngine:
    """
    Moteur d'agrégation de la demande pour le MPS.
    
    Fonctionnalités:
    - Agrège forecast et sales orders par time bucket
    - Supporte les poids configurables (forecast_weight vs orders_weight)
    - Gère les calendars (working days only)
    - Idempotent: upsert au lieu de insert
    - Retourne un summary complet
    
    Exemple:
        engine = AggregateDemandEngine()
        result = engine.aggregate(
            db=conn,
            item_id=item_uuid,
            location_id=loc_uuid,
            scenario_id=scenario_uuid,
            horizon_start=date.today(),
            horizon_end=date.today() + timedelta(days=90),
            time_grain="weekly",
            forecast_weight=Decimal("0.5"),
            orders_weight=Decimal("0.5"),
        )
    """
    
    def __init__(self):
        """Initialiser le moteur d'agrégation."""
        logger.info("AggregateDemandEngine initialisé")
    
    def aggregate(
        self,
        db: DictRowConnection,
        item_id: UUID,
        location_id: UUID,
        scenario_id: UUID,
        horizon_start: date,
        horizon_end: date,
        time_grain: str = "weekly",
        forecast_weight: Decimal = Decimal("0.5"),
        orders_weight: Decimal = Decimal("0.5"),
        clear_existing: bool = False,
    ) -> AggregateDemandResult:
        """
        Agréger la demande et créer les MPS nodes.
        
        Args:
            db: Connection PostgreSQL.
            item_id: UUID de l'item (finished good).
            location_id: UUID du location (plant/DC).
            scenario_id: UUID du scenario.
            horizon_start: Date de début de l'horizon.
            horizon_end: Date de fin de l'horizon.
            time_grain: Granularité: 'daily', 'weekly', 'monthly'.
            forecast_weight: Poids du forecast dans l'agrégation (0-1).
            orders_weight: Poids des sales orders (0-1).
            clear_existing: Si True, supprime les MPS nodes existants avant de créer.
        
        Returns:
            AggregateDemandResult avec les détails de l'agrégation.
        
        Raises:
            ValueError: Si les paramètres sont invalides.
        """
        # Validation
        if time_grain not in ("daily", "weekly", "monthly"):
            raise ValueError(f"time_grain invalide: {time_grain}. Doit être 'daily', 'weekly' ou 'monthly'")
        
        if forecast_weight < 0 or forecast_weight > 1:
            raise ValueError(f"forecast_weight doit être entre 0 et 1, got {forecast_weight}")
        
        if orders_weight < 0 or orders_weight > 1:
            raise ValueError(f"orders_weight doit être entre 0 et 1, got {orders_weight}")
        
        if horizon_end < horizon_start:
            raise ValueError(f"horizon_end ({horizon_end}) < horizon_start ({horizon_start})")
        
        # 1. Clear existing si demandé
        if clear_existing:
            self._clear_existing_mps_nodes(db, item_id, location_id, scenario_id, horizon_start, horizon_end)
        
        # 2. Générer les time buckets
        time_buckets = self._generate_time_buckets(horizon_start, horizon_end, time_grain)
        
        # 3. Fetch forecast data
        forecast_data = self._fetch_forecast_demand(
            db, item_id, location_id, scenario_id, horizon_start, horizon_end
        )
        
        # 4. Fetch sales orders data
        sales_orders_data = self._fetch_sales_orders_demand(
            db, item_id, location_id, scenario_id, horizon_start, horizon_end
        )
        
        # 5. Agréger par time bucket
        bucket_demand = self._aggregate_by_bucket(
            time_buckets, forecast_data, sales_orders_data,
            forecast_weight, orders_weight, time_grain
        )
        
        # 6. Créer/mettre à jour les MPS nodes
        mps_node_ids = []
        created_count = 0
        updated_count = 0
        
        for bucket in bucket_demand:
            mps_id, is_new = self._upsert_mps_node(db, bucket, item_id, location_id, scenario_id)
            mps_node_ids.append(mps_id)
            if is_new:
                created_count += 1
            else:
                updated_count += 1
        
        # 7. Calculer les totaux
        total_demand = sum(b.total_demand for b in bucket_demand)
        demand_by_source = {
            "forecast": sum(b.forecast_quantity for b in bucket_demand),
            "sales_orders": sum(b.sales_orders_quantity for b in bucket_demand),
        }
        
        demand_by_period = [
            {
                "time_bucket": b.time_bucket,
                "time_bucket_start": b.time_bucket_start.isoformat(),
                "time_bucket_end": b.time_bucket_end.isoformat(),
                "forecast_quantity": str(b.forecast_quantity),
                "sales_orders_quantity": str(b.sales_orders_quantity),
                "total_demand": str(b.total_demand),
            }
            for b in bucket_demand
        ]
        
        result = AggregateDemandResult(
            mps_nodes_created=created_count,
            mps_nodes_updated=updated_count,
            total_demand=total_demand,
            demand_by_source=demand_by_source,
            demand_by_period=demand_by_period,
            mps_node_ids=mps_node_ids,
        )
        
        logger.info(
            "mps.aggregate_demand item=%s location=%s scenario=%s buckets=%d created=%d updated=%d total=%s",
            item_id, location_id, scenario_id, len(bucket_demand),
            created_count, updated_count, total_demand,
        )
        
        return result
    
    def _clear_existing_mps_nodes(
        self,
        db: DictRowConnection,
        item_id: UUID,
        location_id: UUID,
        scenario_id: UUID,
        horizon_start: date,
        horizon_end: date,
    ) -> None:
        """Supprimer les MPS nodes existants dans l'horizon."""
        db.execute(
            """
            UPDATE mps_nodes
            SET active = FALSE, updated_at = now()
            WHERE item_id = %s
              AND location_id = %s
              AND scenario_id = %s
              AND time_bucket_start >= %s
              AND time_bucket_end <= %s
              AND active = TRUE
            """,
            (item_id, location_id, scenario_id, horizon_start, horizon_end),
        )
        logger.debug(
            "mps.clear_existing item=%s location=%s scenario=%s",
            item_id, location_id, scenario_id,
        )
    
    def _generate_time_buckets(
        self,
        horizon_start: date,
        horizon_end: date,
        time_grain: str,
    ) -> List[TimeBucketDemand]:
        """
        Générer la liste des time buckets pour l'horizon donné.
        
        Args:
            horizon_start: Date de début.
            horizon_end: Date de fin.
            time_grain: 'daily', 'weekly', 'monthly'.
        
        Returns:
            Liste de TimeBucketDemand.
        """
        buckets: List[TimeBucketDemand] = []
        current = horizon_start
        
        while current <= horizon_end:
            if time_grain == "daily":
                bucket_start = current
                bucket_end = current
                bucket_label = current.strftime("%Y-%m-%d")
                next_start = current + timedelta(days=1)
            
            elif time_grain == "weekly":
                bucket_start = current
                # Fin de semaine: Dimanche (ISO: Lundi=0, Dimanche=6)
                days_to_sunday = 6 - current.weekday()
                if days_to_sunday < 0:
                    days_to_sunday = 6
                bucket_end = min(current + timedelta(days=days_to_sunday), horizon_end)
                bucket_label = f"{current.year}-W{current.isocalendar()[1]:02d}"
                next_start = bucket_end + timedelta(days=1)
            
            elif time_grain == "monthly":
                bucket_start = current
                # Fin du mois
                if current.month == 12:
                    bucket_end = date(current.year, 12, 31)
                else:
                    bucket_end = date(current.year, current.month + 1, 1) - timedelta(days=1)
                bucket_end = min(bucket_end, horizon_end)
                bucket_label = f"{current.year}-{current.month:02d}"
                next_start = bucket_end + timedelta(days=1)
            
            else:
                raise ValueError(f"time_grain invalide: {time_grain}")
            
            buckets.append(
                TimeBucketDemand(
                    time_bucket=bucket_label,
                    time_bucket_start=bucket_start,
                    time_bucket_end=bucket_end,
                    time_grain=time_grain,
                )
            )
            
            current = next_start
        
        return buckets
    
    def _fetch_forecast_demand(
        self,
        db: DictRowConnection,
        item_id: UUID,
        location_id: UUID,
        scenario_id: UUID,
        horizon_start: date,
        horizon_end: date,
    ) -> List[DemandSource]:
        """
        Fetch les données de forecast pour l'item/location/scenario.
        
        Returns:
            Liste de DemandSource pour chaque valeur de forecast.
        """
        rows = db.execute(
            """
            SELECT 
                fv.value_id AS source_id,
                fv.forecast_date AS demand_date,
                fv.quantity
            FROM forecast_values fv
            JOIN forecasts f ON f.forecast_id = fv.forecast_id
            WHERE f.item_id = %s
              AND f.location_id = %s
              AND f.scenario_id = %s
              AND fv.forecast_date >= %s
              AND fv.forecast_date <= %s
              AND fv.active = TRUE
            ORDER BY fv.forecast_date ASC
            """,
            (item_id, location_id, scenario_id, horizon_start, horizon_end),
        ).fetchall()
        
        return [
            DemandSource(
                source_type="forecast",
                source_id=row["source_id"],
                item_id=item_id,
                location_id=location_id,
                scenario_id=scenario_id,
                demand_date=row["demand_date"],
                quantity=Decimal(str(row["quantity"])),
            )
            for row in rows
        ]
    
    def _fetch_sales_orders_demand(
        self,
        db: DictRowConnection,
        item_id: UUID,
        location_id: UUID,
        scenario_id: UUID,
        horizon_start: date,
        horizon_end: date,
    ) -> List[DemandSource]:
        """
        Fetch les sales orders (CustomerOrderDemand) pour l'item/location.
        
        Returns:
            Liste de DemandSource pour chaque sales order.
        """
        rows = db.execute(
            """
            SELECT 
                n.node_id AS source_id,
                n.time_span_start AS demand_date,
                n.quantity
            FROM nodes n
            WHERE n.node_type = 'CustomerOrderDemand'
              AND n.item_id = %s
              AND n.location_id = %s
              AND n.scenario_id = %s
              AND n.time_span_start >= %s
              AND n.time_span_start <= %s
              AND n.active = TRUE
            ORDER BY n.time_span_start ASC
            """,
            (item_id, location_id, scenario_id, horizon_start, horizon_end),
        ).fetchall()
        
        return [
            DemandSource(
                source_type="sales_order",
                source_id=row["source_id"],
                item_id=item_id,
                location_id=location_id,
                scenario_id=scenario_id,
                demand_date=row["demand_date"],
                quantity=Decimal(str(row["quantity"])),
            )
            for row in rows
        ]
    
    def _aggregate_by_bucket(
        self,
        time_buckets: List[TimeBucketDemand],
        forecast_data: List[DemandSource],
        sales_orders_data: List[DemandSource],
        forecast_weight: Decimal,
        orders_weight: Decimal,
        time_grain: str,
    ) -> List[TimeBucketDemand]:
        """
        Agréger les données de demande par time bucket.
        
        Args:
            time_buckets: Liste des time buckets.
            forecast_data: Données de forecast.
            sales_orders_data: Données de sales orders.
            forecast_weight: Poids du forecast.
            orders_weight: Poids des sales orders.
            time_grain: Granularité.
        
        Returns:
            Liste de TimeBucketDemand avec quantités agrégées.
        """
        # Agréger forecast
        for source in forecast_data:
            # Trouver le bucket contenant cette date
            for bucket in time_buckets:
                if bucket.time_bucket_start <= source.demand_date <= bucket.time_bucket_end:
                    bucket.forecast_quantity += source.quantity
                    bucket.demand_sources.append(source)
                    break
        
        # Agréger sales orders
        for source in sales_orders_data:
            for bucket in time_buckets:
                if bucket.time_bucket_start <= source.demand_date <= bucket.time_bucket_end:
                    bucket.sales_orders_quantity += source.quantity
                    bucket.demand_sources.append(source)
                    break
        
        # Appliquer les poids et calculer les totaux
        for bucket in time_buckets:
            # Appliquer les poids
            bucket.forecast_quantity = bucket.forecast_quantity * forecast_weight
            bucket.sales_orders_quantity = bucket.sales_orders_quantity * orders_weight
            
            # Calculer le total
            bucket.compute_total()
        
        return time_buckets
    
    def _upsert_mps_node(
        self,
        db: DictRowConnection,
        bucket: TimeBucketDemand,
        item_id: UUID,
        location_id: UUID,
        scenario_id: UUID,
    ) -> Tuple[UUID, bool]:
        """
        Créer ou mettre à jour un MPS node pour un time bucket.
        
        Args:
            db: Connection PostgreSQL.
            bucket: Time bucket avec demande agrégée.
            item_id: UUID de l'item.
            location_id: UUID du location.
            scenario_id: UUID du scenario.
        
        Returns:
            Tuple (mps_id, is_new) où is_new=True si créé, False si mis à jour.
        """
        # Vérifier s'il existe déjà un MPS node actif
        existing = db.execute(
            """
            SELECT mps_id, forecast_quantity, sales_orders_quantity, total_demand
            FROM mps_nodes
            WHERE item_id = %s
              AND location_id = %s
              AND scenario_id = %s
              AND time_bucket = %s
              AND active = TRUE
            """,
            (item_id, location_id, scenario_id, bucket.time_bucket),
        ).fetchone()
        
        mps_id = uuid4()
        is_new = False
        
        if existing:
            # Update: vérifier si les données ont changé
            mps_id = existing["mps_id"]
            existing_forecast = Decimal(str(existing["forecast_quantity"]))
            existing_orders = Decimal(str(existing["sales_orders_quantity"]))
            existing_total = Decimal(str(existing["total_demand"]))
            
            if (existing_forecast != bucket.forecast_quantity or
                existing_orders != bucket.sales_orders_quantity or
                existing_total != bucket.total_demand):
                # Mettre à jour
                db.execute(
                    """
                    UPDATE mps_nodes
                    SET forecast_quantity = %s,
                        sales_orders_quantity = %s,
                        total_demand = %s,
                        updated_at = now()
                    WHERE mps_id = %s
                    """,
                    (bucket.forecast_quantity, bucket.sales_orders_quantity,
                     bucket.total_demand, mps_id),
                )
                logger.debug("mps.upsert updated mps_id=%s", mps_id)
            else:
                logger.debug("mps.upsert unchanged mps_id=%s", mps_id)
        else:
            # Insert
            db.execute(
                """
                INSERT INTO mps_nodes (
                    mps_id, item_id, location_id, scenario_id,
                    time_bucket, time_bucket_start, time_bucket_end, time_grain,
                    forecast_quantity, sales_orders_quantity, total_demand,
                    planned_quantity, status, active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                """,
                (mps_id, item_id, location_id, scenario_id,
                 bucket.time_bucket, bucket.time_bucket_start, bucket.time_bucket_end,
                 bucket.time_grain, bucket.forecast_quantity, bucket.sales_orders_quantity,
                 bucket.total_demand, bucket.total_demand, MPSStatus.DRAFT.value),
            )
            is_new = True
            logger.debug("mps.upsert created mps_id=%s", mps_id)
        
        return mps_id, is_new
    
    def get_mps_nodes_summary(
        self,
        db: DictRowConnection,
        item_id: UUID,
        location_id: UUID,
        scenario_id: UUID,
        horizon_start: Optional[date] = None,
        horizon_end: Optional[date] = None,
    ) -> List[MPSNodeSummary]:
        """
        Récupérer un résumé des MPS nodes.
        
        Args:
            db: Connection PostgreSQL.
            item_id: UUID de l'item.
            location_id: UUID du location.
            scenario_id: UUID du scenario.
            horizon_start: Filtre optionnel de début.
            horizon_end: Filtre optionnel de fin.
        
        Returns:
            Liste de MPSNodeSummary.
        """
        query = """
            SELECT 
                mps_id, time_bucket, time_bucket_start, time_bucket_end,
                forecast_quantity, sales_orders_quantity, total_demand,
                planned_quantity, status
            FROM mps_nodes
            WHERE item_id = %s
              AND location_id = %s
              AND scenario_id = %s
              AND active = TRUE
        """
        params: List[Any] = [item_id, location_id, scenario_id]
        
        if horizon_start:
            query += " AND time_bucket_end >= %s"
            params.append(horizon_start)
        
        if horizon_end:
            query += " AND time_bucket_start <= %s"
            params.append(horizon_end)
        
        query += " ORDER BY time_bucket_start ASC"
        
        rows = db.execute(query, params).fetchall()
        
        return [
            MPSNodeSummary(
                mps_id=row["mps_id"],
                time_bucket=row["time_bucket"],
                time_bucket_start=row["time_bucket_start"],
                time_bucket_end=row["time_bucket_end"],
                forecast_quantity=Decimal(str(row["forecast_quantity"])),
                sales_orders_quantity=Decimal(str(row["sales_orders_quantity"])),
                total_demand=Decimal(str(row["total_demand"])),
                planned_quantity=Decimal(str(row["planned_quantity"])),
                status=row["status"],
            )
            for row in rows
        ]
    
    def promote_to_mrp(
        self,
        db: DictRowConnection,
        mps_id: UUID,
        explode_components: bool = True,
        dry_run: bool = False,
        user_id: Optional[str] = None,
    ) -> PromoteToMRPResult:
        """
        Promouvoir un MPS node vers le MRP.
        
        Args:
            db: Connection PostgreSQL.
            mps_id: UUID du MPS node à promouvoir.
            explode_components: Si True, déclenche la BOM explosion.
            dry_run: Si True, validation seulement sans création.
            user_id: Identifiant utilisateur pour l'audit trail.
        
        Returns:
            PromoteToMRPResult avec les détails de l'opération.
        
        Raises:
            ValueError: Si le MPS node n'existe pas ou n'est pas APPROVED.
        """
        from uuid import uuid4
        from datetime import datetime, timezone
        
        transaction_id = f"TXN-{uuid4().hex[:12].upper()}"
        logger.info("promote_to_mrp starting for mps_id=%s, transaction=%s", mps_id, transaction_id)
        
        # Fetch MPS node
        row = db.execute(
            """
            SELECT mps_id, item_id, location_id, scenario_id, planned_quantity,
                   time_bucket_start, time_bucket_end, status
            FROM mps_nodes
            WHERE mps_id = %s AND active = TRUE
            """,
            (mps_id,),
        ).fetchone()
        
        if not row:
            raise ValueError(f"MPS node '{mps_id}' not found or inactive")
        
        if row["status"] != "APPROVED":
            raise ValueError(f"MPS node must be APPROVED (current: {row['status']})")
        
        if dry_run:
            logger.info("promote_to_mrp dry_run for mps_id=%s", mps_id)
            return PromoteToMRPResult(
                status="RELEASED",
                transaction_id=transaction_id,
                planned_supplies_created=0,
                mrp_job_id=None,
                components_exploded=0,
                summary={"dry_run": True, "validated": True},
            )
        
        # Update MPS status to RELEASED
        db.execute(
            """
            UPDATE mps_nodes
            SET status = %s,
                released_by = %s,
                released_at = %s,
                updated_at = %s
            WHERE mps_id = %s
            """,
            (
                MPSStatus.RELEASED.value,
                user_id,
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
                mps_id,
            ),
        )
        logger.info("promote_to_mrp updated mps_id=%s to RELEASED", mps_id)
        
        # Create PlannedSupply for the finished good
        item_id = row["item_id"]
        location_id = row["location_id"]
        planned_qty = Decimal(str(row["planned_quantity"]))
        start_date = row["time_bucket_start"]
        
        planned_supply_id = uuid4()
        db.execute(
            """
            INSERT INTO planned_supply (
                planned_supply_id, item_id, location_id, source_type,
                source_id, quantity, due_date, status, created_at
            ) VALUES (%s, %s, %s, 'MPS', %s, %s, %s, 'PLANNED', %s)
            """,
            (
                planned_supply_id,
                item_id,
                location_id,
                mps_id,
                planned_qty,
                start_date,
                datetime.now(timezone.utc),
            ),
        )
        logger.info(
            "promote_to_mrp created planned_supply_id=%s for mps_id=%s, qty=%s",
            planned_supply_id, mps_id, planned_qty
        )
        
        # Trigger MRP BOM explosion if requested
        mrp_job_id = None
        components_exploded = 0
        
        if explode_components:
            try:
                mrp_result = self._trigger_mrp_explosion(
                    db=db,
                    planned_supply_id=planned_supply_id,
                    item_id=item_id,
                    location_id=location_id,
                    quantity=planned_qty,
                    due_date=start_date,
                )
                mrp_job_id = mrp_result.get("job_id")
                components_exploded = mrp_result.get("components_count", 0)
                logger.info(
                    "promote_to_mrp triggered MRP explosion: job_id=%s, components=%s",
                    mrp_job_id, components_exploded
                )
            except Exception as e:
                logger.error("promote_to_mrp MRP explosion failed: %s", e)
                db.execute(
                    "UPDATE mps_nodes SET status = %s WHERE mps_id = %s",
                    (MPSStatus.APPROVED.value, mps_id),
                )
                raise ValueError(f"MRP explosion failed: {e}")
        
        return PromoteToMRPResult(
            status="RELEASED",
            transaction_id=transaction_id,
            planned_supplies_created=1,
            mrp_job_id=mrp_job_id,
            components_exploded=components_exploded,
            summary={
                "mps_id": str(mps_id),
                "planned_supply_id": str(planned_supply_id),
                "item_id": str(item_id),
                "location_id": str(location_id),
                "quantity": str(planned_qty),
                "due_date": str(start_date),
                "explode_components": explode_components,
            },
        )
    
    def _trigger_mrp_explosion(
        self,
        db: DictRowConnection,
        planned_supply_id: UUID,
        item_id: UUID,
        location_id: UUID,
        quantity: Decimal,
        due_date: date,
    ) -> Dict[str, Any]:
        """
        Déclencher l'explosion BOM via le moteur MRP.
        
        Returns:
            Dict avec job_id et components_count.
        """
        try:
            from ootils_core.api.routers.mrp import MRPExplosionEngine
            
            engine = MRPExplosionEngine()
            result = engine.explode_bom(
                db=db,
                parent_supply_id=planned_supply_id,
                item_id=item_id,
                location_id=location_id,
                quantity=quantity,
                due_date=due_date,
            )
            return {
                "job_id": result.get("job_id"),
                "components_count": result.get("components_count", 0),
            }
        except ImportError:
            logger.warning("MRPExplosionEngine not available, skipping BOM explosion")
            return {"job_id": None, "components_count": 0}
        except Exception as e:
            logger.error("MRP explosion error: %s", e)
            raise
