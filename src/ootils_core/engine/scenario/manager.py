"""
manager.py — ScenarioManager: create, apply_override, diff, promote.

Design principles (per EXPERT-dirty-flags-and-scenarios.md Q4.1–Q4.7):
- Overrides are user intent → TEXT serialization, persisted in scenario_overrides.
- Computed results live independently in nodes (scoped by scenario_id).
- Merge is a first-class event (scenario_merge) not a schema patch.
- Diff compares computed columns only (closing_stock, opening_stock, inflows,
  outflows, has_shortage, shortage_qty) between two calc_runs.
- All DB access via psycopg3; caller owns the transaction.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID, uuid4

import psycopg

from ootils_core.models import (
    Node,
    PlanningEvent,
    Scenario,
    ScenarioDiff,
    ScenarioOverride,
)

logger = logging.getLogger(__name__)

# Fields compared during a baseline→scenario diff
_DIFF_FIELDS: tuple[str, ...] = (
    "closing_stock",
    "opening_stock",
    "inflows",
    "outflows",
    "has_shortage",
    "shortage_qty",
)

# Baseline sentinel UUID (matches seed in migration 002)
_BASELINE_ID = UUID("00000000-0000-0000-0000-000000000001")


class ScenarioManager:
    """
    Manages scenario lifecycle operations.

    All methods accept a psycopg3 Connection.  The caller owns commit/rollback
    — this class never calls conn.commit() directly.
    """

    # ------------------------------------------------------------------
    # create_scenario
    # ------------------------------------------------------------------

    def create_scenario(
        self,
        name: str,
        parent_scenario_id: UUID,
        db: psycopg.Connection,
    ) -> Scenario:
        """
        Create a new (non-baseline) scenario branched from parent_scenario_id.

        Steps:
          1. Insert a new row in scenarios (is_baseline=False, status='active').
          2. Deep-copy all active nodes from the parent scenario, assigning
             new node_id values and the new scenario_id.

        Returns the newly created Scenario.
        """
        scenario_id = uuid4()
        now = datetime.now(timezone.utc)

        db.execute(
            """
            INSERT INTO scenarios (
                scenario_id, name, parent_scenario_id,
                is_baseline, status, created_at, updated_at
            ) VALUES (%s, %s, %s, FALSE, 'active', %s, %s)
            """,
            (scenario_id, name, parent_scenario_id, now, now),
        )
        logger.info(
            "scenario.created scenario_id=%s name=%r parent=%s",
            scenario_id,
            name,
            parent_scenario_id,
        )

        # Deep-copy parent nodes into the new scenario
        self._copy_nodes(parent_scenario_id, scenario_id, db)

        return Scenario(
            scenario_id=scenario_id,
            name=name,
            parent_scenario_id=parent_scenario_id,
            is_baseline=False,
            status="active",
            created_at=now,
            updated_at=now,
        )

    def _copy_nodes(
        self,
        source_scenario_id: UUID,
        target_scenario_id: UUID,
        db: psycopg.Connection,
    ) -> int:
        """
        Copy all active nodes from source_scenario_id to target_scenario_id,
        assigning fresh node_id UUIDs.

        Returns the number of nodes copied.
        """
        source_nodes = db.execute(
            """
            SELECT * FROM nodes
            WHERE scenario_id = %s AND active = TRUE
            """,
            (source_scenario_id,),
        ).fetchall()

        count = 0
        for row in source_nodes:
            new_node_id = uuid4()
            now = datetime.now(timezone.utc)
            db.execute(
                """
                INSERT INTO nodes (
                    node_id, node_type, scenario_id, item_id, location_id,
                    quantity, qty_uom,
                    time_grain, time_ref, time_span_start, time_span_end,
                    is_dirty, active,
                    projection_series_id, bucket_sequence,
                    opening_stock, inflows, outflows, closing_stock,
                    has_shortage, shortage_qty,
                    has_exact_date_inputs, has_week_inputs, has_month_inputs,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    FALSE, TRUE,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s
                )
                """,
                (
                    new_node_id,
                    row["node_type"],
                    target_scenario_id,
                    row["item_id"],
                    row["location_id"],
                    row["quantity"],
                    row["qty_uom"],
                    row["time_grain"],
                    row["time_ref"],
                    row["time_span_start"],
                    row["time_span_end"],
                    row["projection_series_id"],
                    row["bucket_sequence"],
                    row["opening_stock"],
                    row["inflows"],
                    row["outflows"],
                    row["closing_stock"],
                    row["has_shortage"],
                    row["shortage_qty"],
                    row["has_exact_date_inputs"],
                    row["has_week_inputs"],
                    row["has_month_inputs"],
                    now,
                    now,
                ),
            )
            count += 1

        logger.info(
            "scenario.copy_nodes src=%s dst=%s count=%d",
            source_scenario_id,
            target_scenario_id,
            count,
        )
        return count

    # ------------------------------------------------------------------
    # apply_override
    # ------------------------------------------------------------------

    def apply_override(
        self,
        scenario_id: UUID,
        node_id: UUID,
        field_name: str,
        new_value: str,
        applied_by: Optional[str],
        db: psycopg.Connection,
    ) -> ScenarioOverride:
        """
        Apply a field-level override to a node within a scenario.

        Steps:
          1. Read current field value from nodes (becomes old_value).
          2. Upsert into scenario_overrides (UNIQUE: scenario_id, node_id, field_name).
          3. UPDATE nodes SET {field_name} = new_value for this (node_id, scenario_id).
          4. Insert a PlanningEvent of type 'policy_changed' to trigger recalculation.

        Returns the persisted ScenarioOverride.

        Security note: field_name is validated against a whitelist before being
        interpolated into the UPDATE query.
        """
        _validate_field_name(field_name)

        # 1. Fetch current node value — also validates that the node exists
        row = db.execute(
            f"SELECT {field_name} FROM nodes WHERE node_id = %s AND scenario_id = %s",
            (node_id, scenario_id),
        ).fetchone()

        if row is None:
            raise ValueError(
                f"Node {node_id} not found in scenario {scenario_id}. "
                "Override cannot be applied to a non-existent node."
            )

        old_value: Optional[str] = None
        if row[field_name] is not None:
            old_value = str(row[field_name])

        now = datetime.now(timezone.utc)
        override_id = uuid4()

        # 2. Upsert override (one per scenario/node/field)
        # ON CONFLICT: if a prior override exists, update values.
        # The PK (override_id) is not changed on conflict — we use our generated UUID
        # for new inserts; existing rows retain their original override_id.
        db.execute(
            """
            INSERT INTO scenario_overrides (
                override_id, scenario_id, node_id,
                field_name, old_value, new_value,
                applied_at, applied_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (scenario_id, node_id, field_name) DO UPDATE SET
                old_value  = EXCLUDED.old_value,
                new_value  = EXCLUDED.new_value,
                applied_at = EXCLUDED.applied_at,
                applied_by = EXCLUDED.applied_by
            """,
            (
                override_id,
                scenario_id,
                node_id,
                field_name,
                old_value,
                new_value,
                now,
                applied_by,
            ),
        )
        persisted_override_id = override_id

        # 3. Apply to node column (field_name validated above)
        db.execute(
            f"""
            UPDATE nodes
            SET {field_name} = %s,
                is_dirty     = TRUE,
                updated_at   = %s
            WHERE node_id = %s AND scenario_id = %s
            """,
            (new_value, now, node_id, scenario_id),
        )
        logger.info(
            "override.applied scenario=%s node=%s field=%s old=%r new=%r by=%s",
            scenario_id,
            node_id,
            field_name,
            old_value,
            new_value,
            applied_by,
        )

        # 4. Create policy_changed event to trigger recalculation
        event_id = uuid4()
        db.execute(
            """
            INSERT INTO events (
                event_id, event_type, scenario_id, trigger_node_id,
                field_changed, old_text, new_text,
                processed, source, user_ref, created_at
            ) VALUES (%s, 'policy_changed', %s, %s, %s, %s, %s, FALSE, 'engine', %s, %s)
            """,
            (
                event_id,
                scenario_id,
                node_id,
                field_name,
                old_value,
                new_value,
                applied_by,
                now,
            ),
        )
        logger.debug(
            "override.event event_id=%s scenario=%s trigger_node=%s",
            event_id,
            scenario_id,
            node_id,
        )

        return ScenarioOverride(
            override_id=persisted_override_id,
            scenario_id=scenario_id,
            node_id=node_id,
            field_name=field_name,
            old_value=old_value,
            new_value=new_value,
            applied_at=now,
            applied_by=applied_by,
        )

    # ------------------------------------------------------------------
    # diff
    # ------------------------------------------------------------------

    def diff(
        self,
        scenario_id: UUID,
        baseline_id: UUID,
        db: psycopg.Connection,
        baseline_calc_run_id: Optional[UUID] = None,
        scenario_calc_run_id: Optional[UUID] = None,
    ) -> list[ScenarioDiff]:
        """
        Diff all nodes in scenario vs baseline across the DIFF_FIELDS.

        If calc_run IDs are not provided, the latest completed calc_run for
        each scenario is used.  Only nodes that differ in at least one field
        are returned.

        Differences are persisted in scenario_diffs (upsert on UNIQUE key).

        Returns a list of ScenarioDiff (one entry per changed field per node).
        """
        # Resolve calc_runs if not supplied
        if baseline_calc_run_id is None:
            baseline_calc_run_id = self._latest_calc_run(baseline_id, db)
        if scenario_calc_run_id is None:
            scenario_calc_run_id = self._latest_calc_run(scenario_id, db)

        # Fetch all nodes for baseline and scenario
        baseline_nodes = _fetch_nodes_as_dict(baseline_id, db)
        scenario_nodes = _fetch_nodes_as_dict(scenario_id, db)

        diffs: list[ScenarioDiff] = []
        now = datetime.now(timezone.utc)

        # Match by (item_id, location_id, node_type, time_span_start, bucket_sequence)
        # Nodes are matched by a stable business key, not node_id (which differs after copy).
        baseline_index = _build_node_index(baseline_nodes)
        scenario_index = _build_node_index(scenario_nodes)

        all_keys = set(baseline_index.keys()) | set(scenario_index.keys())

        for key in all_keys:
            b_node = baseline_index.get(key)
            s_node = scenario_index.get(key)

            for field in _DIFF_FIELDS:
                b_val = _node_field_str(b_node, field) if b_node else None
                s_val = _node_field_str(s_node, field) if s_node else None

                if b_val == s_val:
                    continue

                # Use the scenario node_id if available, else baseline node_id
                node_id = UUID(str(s_node["node_id"])) if s_node else UUID(str(b_node["node_id"]))

                diff_id = uuid4()
                db.execute(
                    """
                    INSERT INTO scenario_diffs (
                        diff_id, scenario_id,
                        baseline_calc_run_id, scenario_calc_run_id,
                        node_id, field_name,
                        baseline_value, scenario_value, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (scenario_id, baseline_calc_run_id, scenario_calc_run_id, node_id, field_name)
                    DO UPDATE SET
                        baseline_value = EXCLUDED.baseline_value,
                        scenario_value = EXCLUDED.scenario_value
                    """,
                    (
                        diff_id,
                        scenario_id,
                        baseline_calc_run_id,
                        scenario_calc_run_id,
                        node_id,
                        field,
                        b_val,
                        s_val,
                        now,
                    ),
                )
                persisted_diff_id = diff_id

                diffs.append(
                    ScenarioDiff(
                        diff_id=persisted_diff_id,
                        scenario_id=scenario_id,
                        baseline_calc_run_id=baseline_calc_run_id,
                        scenario_calc_run_id=scenario_calc_run_id,
                        node_id=node_id,
                        field_name=field,
                        baseline_value=b_val,
                        scenario_value=s_val,
                        created_at=now,
                    )
                )

        logger.info(
            "diff.complete scenario=%s baseline=%s diff_count=%d",
            scenario_id,
            baseline_id,
            len(diffs),
        )
        return diffs

    def _latest_calc_run(self, scenario_id: UUID, db: psycopg.Connection) -> UUID:
        """Return the calc_run_id of the latest completed calc_run for a scenario."""
        row = db.execute(
            """
            SELECT calc_run_id FROM calc_runs
            WHERE scenario_id = %s AND status = 'completed'
            ORDER BY completed_at DESC NULLS LAST, created_at DESC
            LIMIT 1
            """,
            (scenario_id,),
        ).fetchone()
        if row is None:
            raise ValueError(
                f"No completed calc_run found for scenario {scenario_id}. "
                "Run a calculation first."
            )
        return UUID(str(row["calc_run_id"]))

    # ------------------------------------------------------------------
    # promote
    # ------------------------------------------------------------------

    def promote(
        self,
        scenario_id: UUID,
        db: psycopg.Connection,
    ) -> None:
        """
        Promote a scenario to baseline.

        Per EXPERT Q4.5 — merge is a first-class event:
          1. For each node override in the scenario, apply its new_value to
             the corresponding baseline node (matched by business key).
          2. Mark the old baseline scenario as 'archived'.
          3. Create a 'scenario_merge' event.
          4. Mark all other active variant scenarios as 'stale' (not
             implemented at schema level yet — logged only for now).

        The scenario being promoted retains its own status and nodes.
        The baseline scenario's nodes are patched with the promoted values.
        """
        # 1. Load all overrides for this scenario
        override_rows = db.execute(
            """
            SELECT node_id, field_name, new_value
            FROM scenario_overrides
            WHERE scenario_id = %s
            """,
            (scenario_id,),
        ).fetchall()

        # 2. Load nodes from the promoted scenario → build match key → patch baseline
        scenario_nodes = _fetch_nodes_as_dict(scenario_id, db)
        scenario_index = {UUID(str(n["node_id"])): n for n in scenario_nodes}

        now = datetime.now(timezone.utc)
        patched = 0

        for ov in override_rows:
            ov_node_id = UUID(str(ov["node_id"]))
            field_name = ov["field_name"]
            new_value = ov["new_value"]

            _validate_field_name(field_name)

            s_node = scenario_index.get(ov_node_id)
            if s_node is None:
                logger.warning(
                    "promote.skip node_id=%s not found in scenario nodes", ov_node_id
                )
                continue

            # Find the matching baseline node by business key
            b_key = _node_business_key(s_node)
            b_rows = db.execute(
                """
                SELECT node_id FROM nodes
                WHERE scenario_id = %s
                  AND node_type = %s
                  AND item_id IS NOT DISTINCT FROM %s
                  AND location_id IS NOT DISTINCT FROM %s
                  AND time_span_start IS NOT DISTINCT FROM %s
                  AND bucket_sequence IS NOT DISTINCT FROM %s
                  AND active = TRUE
                """,
                (
                    _BASELINE_ID,
                    b_key["node_type"],
                    b_key["item_id"],
                    b_key["location_id"],
                    b_key["time_span_start"],
                    b_key["bucket_sequence"],
                ),
            ).fetchall()

            for b_row in b_rows:
                db.execute(
                    f"""
                    UPDATE nodes
                    SET {field_name} = %s,
                        is_dirty     = TRUE,
                        updated_at   = %s
                    WHERE node_id = %s AND scenario_id = %s
                    """,
                    (new_value, now, UUID(str(b_row["node_id"])), _BASELINE_ID),
                )
                patched += 1

        # 3. Archive the promoted scenario
        db.execute(
            """
            UPDATE scenarios
            SET status     = 'archived',
                updated_at = %s
            WHERE scenario_id = %s
            """,
            (now, scenario_id),
        )

        # 4. Create scenario_merge event (baseline scope)
        event_id = uuid4()
        db.execute(
            """
            INSERT INTO events (
                event_id, event_type, scenario_id,
                old_text, new_text,
                processed, source, created_at
            ) VALUES (%s, 'scenario_merge', %s, %s, %s, FALSE, 'engine', %s)
            """,
            (
                event_id,
                _BASELINE_ID,
                str(scenario_id),   # old_text = source scenario_id
                "promoted",
                now,
            ),
        )

        logger.info(
            "promote.complete scenario=%s patched_nodes=%d merge_event=%s",
            scenario_id,
            patched,
            event_id,
        )


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

# Allowed field names for override and dynamic UPDATE (whitelist)
_ALLOWED_FIELDS: frozenset[str] = frozenset(
    [
        "quantity",
        "time_ref",
        "time_span_start",
        "time_span_end",
        "opening_stock",
        "inflows",
        "outflows",
        "closing_stock",
        "has_shortage",
        "shortage_qty",
        "is_dirty",
        "active",
        "qty_uom",
        "time_grain",
    ]
)


def _validate_field_name(field_name: str) -> None:
    """Guard against SQL injection in dynamic column names."""
    if field_name not in _ALLOWED_FIELDS:
        raise ValueError(
            f"field_name {field_name!r} is not in the allowed override field list. "
            f"Allowed: {sorted(_ALLOWED_FIELDS)}"
        )


def _fetch_nodes_as_dict(scenario_id: UUID, db: psycopg.Connection) -> list[dict]:
    """Fetch all active nodes for a scenario as raw dicts."""
    return db.execute(
        "SELECT * FROM nodes WHERE scenario_id = %s AND active = TRUE",
        (scenario_id,),
    ).fetchall()


def _build_node_index(nodes: list[dict]) -> dict[tuple, dict]:
    """
    Build a lookup index keyed by (node_type, item_id, location_id,
    time_span_start, bucket_sequence) — the stable business key for node matching
    across scenarios.
    """
    index: dict[tuple, dict] = {}
    for node in nodes:
        key = _node_business_key(node)
        index[tuple(key.values())] = node
    return index


def _node_business_key(node: dict) -> dict:
    return {
        "node_type": node["node_type"],
        "item_id": node["item_id"],
        "location_id": node["location_id"],
        "time_span_start": node["time_span_start"],
        "bucket_sequence": node["bucket_sequence"],
    }


def _node_field_str(node: dict, field: str) -> Optional[str]:
    """Return field value as string, or None."""
    val = node.get(field)
    if val is None:
        return None
    return str(val)
