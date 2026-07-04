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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

import psycopg

from ootils_core.db.types import DictRowConnection
from ootils_core.models import (
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


@dataclass(frozen=True)
class PromoteConflict:
    """One field where the baseline diverged since the override captured it.

    `expected` is the baseline value at override time (scenario_overrides.
    old_value — the scenario node was a verbatim deep-copy of the baseline
    node, so the value read just before the first override IS the baseline
    value at fork time). `actual` is the baseline value now. Both are the
    TEXT serialization used by apply_override (str() of the column value).
    """

    node_id: UUID  # baseline node id
    field_name: str
    expected: Optional[str]
    actual: Optional[str]


class PromoteConflictError(Exception):
    """
    Raised by promote() when the baseline diverged from the value captured
    at override time (ADR-018 P2.2.c: no more blind overlay apply).
    Nothing has been written when this is raised.
    """

    def __init__(self, scenario_id: UUID, conflicts: list[PromoteConflict]) -> None:
        self.scenario_id = scenario_id
        self.conflicts = conflicts
        super().__init__(
            f"Promote of scenario {scenario_id} aborted: baseline diverged on "
            f"{len(conflicts)} node field(s) since the overrides were captured"
        )


@dataclass(frozen=True)
class PromoteResult:
    """Outcome of a successful promote (consumed by the API audit row)."""

    scenario_id: UUID
    override_count: int
    patched_nodes: int
    siblings_invalidated: int
    merge_event_id: UUID
    conflict_checked: bool = True


class ScenarioManager:
    """
    Manages scenario lifecycle operations.

    Forking strategy: explicit deep-copy via bulk INSERT…SELECT through two
    temp mapping tables (_series_map, _node_map). One scenario fork costs a
    constant ~10 SQL statements regardless of source row count — see
    REVIEW-2026-05 R10 / docs/ADR-012-scenario-fork-bulk.md. True lazy CoW
    (no copy at create time, scenario-chain read fallback at the GraphStore
    layer) is a future ADR.

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
        db: DictRowConnection,
    ) -> Scenario:
        """
        Create a new (non-baseline) scenario branched from parent_scenario_id.

        Steps:
          1. Insert a new row in scenarios (is_baseline=False, status='active').
          2. Deep-copy all active nodes from the parent scenario, assigning
             new node_id values and the new scenario_id.
          3. Deep-copy edges and projection series needed by those nodes.

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

        # Deep-copy projection_series first (nodes reference them)
        series_mapping = self._copy_projection_series(parent_scenario_id, scenario_id, db)

        # Deep-copy parent nodes into the new scenario
        self._copy_nodes(parent_scenario_id, scenario_id, db, series_mapping)

        return Scenario(
            scenario_id=scenario_id,
            name=name,
            parent_scenario_id=parent_scenario_id,
            is_baseline=False,
            status="active",
            created_at=now,
            updated_at=now,
        )

    def _copy_projection_series(
        self,
        source_scenario_id: UUID,
        target_scenario_id: UUID,
        db: DictRowConnection,
    ) -> dict:
        """
        Copy projection_series from source to target scenario.
        Returns a mapping old_series_id -> new_series_id.

        Bulk path: 2 SQL statements (build mapping table + INSERT…SELECT).
        Was: 1 INSERT per row, dominating fork latency at scale (see
        REVIEW-2026-05 R10 / scripts/bench_scenario_fork.py).
        """
        db.execute(
            """
            CREATE TEMP TABLE _series_map (
                old_id UUID PRIMARY KEY,
                new_id UUID NOT NULL DEFAULT gen_random_uuid()
            ) ON COMMIT DROP
            """
        )
        db.execute(
            """
            INSERT INTO _series_map (old_id)
            SELECT series_id FROM projection_series WHERE scenario_id = %s
            """,
            (source_scenario_id,),
        )
        db.execute(
            """
            INSERT INTO projection_series
                (series_id, item_id, location_id, scenario_id,
                 horizon_start, horizon_end, created_at, updated_at)
            SELECT m.new_id, ps.item_id, ps.location_id, %s,
                   ps.horizon_start, ps.horizon_end, NOW(), NOW()
            FROM projection_series ps
            JOIN _series_map m ON m.old_id = ps.series_id
            WHERE ps.scenario_id = %s
            ON CONFLICT (item_id, location_id, scenario_id) DO NOTHING
            """,
            (target_scenario_id, source_scenario_id),
        )
        rows = db.execute(
            "SELECT old_id::text AS old_id, new_id FROM _series_map"
        ).fetchall()
        return {r["old_id"]: r["new_id"] for r in rows}

    def _copy_nodes(
        self,
        source_scenario_id: UUID,
        target_scenario_id: UUID,
        db: DictRowConnection,
        series_mapping: dict | None = None,
    ) -> int:
        """
        Copy all active nodes (and their edges) from source_scenario_id to
        target_scenario_id, assigning fresh UUIDs.

        series_mapping: optional dict {str(old_series_id): new_series_id} to
        remap projection_series_id references to the new scenario's series.

        Returns the number of nodes copied.

        Bulk path: ~5 SQL statements regardless of node count.
        - 2 statements build temp mapping tables (_node_map, optionally _series_map).
        - 1 INSERT…SELECT copies nodes, remapping series via JOIN on the
          series mapping (which the caller materialised in _copy_projection_series).
        - 1 INSERT…SELECT copies edges with both endpoints remapped via JOIN.
        - 1 SELECT runs the post-copy orphan-edge integrity check.
        Was: 1 INSERT per row — see REVIEW-2026-05 R10.
        """
        # Build node mapping in a temp table so the edge JOIN can resolve
        # both endpoints in a single INSERT…SELECT.
        db.execute(
            """
            CREATE TEMP TABLE _node_map (
                old_id UUID PRIMARY KEY,
                new_id UUID NOT NULL DEFAULT gen_random_uuid()
            ) ON COMMIT DROP
            """
        )
        db.execute(
            """
            INSERT INTO _node_map (old_id)
            SELECT node_id FROM nodes WHERE scenario_id = %s AND active = TRUE
            """,
            (source_scenario_id,),
        )

        # series_mapping (dict[str(old_series_id), new_series_id]) was built by
        # _copy_projection_series via the _series_map temp table. We can read
        # that table directly here if it still exists, otherwise we synthesize
        # a CTE from the dict (used by tests that bypass _copy_projection_series).
        series_map_available = False
        try:
            db.execute("SELECT 1 FROM _series_map LIMIT 1").fetchone()
            series_map_available = True
        except psycopg.errors.UndefinedTable:
            # The _series_map temp table is created within a SAVEPOINT-less
            # transaction; a failed lookup aborts the current transaction
            # state. The caller must roll back or we cannot continue.
            db.rollback()
            raise RuntimeError(
                "_copy_nodes called without a prior _copy_projection_series; "
                "the bulk path requires both temp mapping tables to be present."
            )

        # Bulk-insert nodes with series remapping via JOIN.
        result = db.execute(
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
            )
            SELECT
                m.new_id, n.node_type, %s, n.item_id, n.location_id,
                n.quantity, n.qty_uom,
                n.time_grain, n.time_ref, n.time_span_start, n.time_span_end,
                FALSE, TRUE,
                COALESCE(sm.new_id, n.projection_series_id), n.bucket_sequence,
                n.opening_stock, n.inflows, n.outflows, n.closing_stock,
                n.has_shortage, n.shortage_qty,
                n.has_exact_date_inputs, n.has_week_inputs, n.has_month_inputs,
                NOW(), NOW()
            FROM nodes n
            JOIN _node_map m ON m.old_id = n.node_id
            LEFT JOIN _series_map sm ON sm.old_id = n.projection_series_id
            WHERE n.scenario_id = %s AND n.active = TRUE
            """,
            (target_scenario_id, source_scenario_id),
        )
        count = result.rowcount or 0

        # Bulk-insert edges with both endpoints remapped via JOIN.
        # Edges whose endpoints are missing from _node_map are dropped here —
        # the orphan check below would fail for them anyway.
        edge_result = db.execute(
            """
            INSERT INTO edges (
                edge_id, edge_type, from_node_id, to_node_id, scenario_id,
                priority, weight_ratio, effective_start, effective_end,
                active, created_at
            )
            SELECT
                gen_random_uuid(), e.edge_type, mf.new_id, mt.new_id, %s,
                e.priority, e.weight_ratio, e.effective_start, e.effective_end,
                TRUE, NOW()
            FROM edges e
            JOIN _node_map mf ON mf.old_id = e.from_node_id
            JOIN _node_map mt ON mt.old_id = e.to_node_id
            WHERE e.scenario_id = %s AND e.active = TRUE
            """,
            (target_scenario_id, source_scenario_id),
        )
        edge_count = edge_result.rowcount or 0

        _ = series_map_available  # silence unused warning; the check above is the gate

        # Post-copy integrity check: verify no active edges in the new scenario
        # reference node_ids outside the copied set (fix for issue #158).
        orphan_row = db.execute(
            """
            SELECT COUNT(*) AS cnt FROM edges e
            WHERE e.scenario_id = %s AND e.active = TRUE
              AND (
                NOT EXISTS (
                    SELECT 1 FROM nodes n
                    WHERE n.node_id = e.from_node_id
                      AND n.scenario_id = %s AND n.active = TRUE
                )
                OR NOT EXISTS (
                    SELECT 1 FROM nodes n
                    WHERE n.node_id = e.to_node_id
                      AND n.scenario_id = %s AND n.active = TRUE
                )
              )
            """,
            (target_scenario_id, target_scenario_id, target_scenario_id),
        ).fetchone()
        orphan_count = int(orphan_row["cnt"]) if orphan_row else 0
        if orphan_count > 0:
            logger.error(
                "scenario.copy_nodes: %d orphaned edge(s) detected in new scenario %s — "
                "graph connectivity is broken; scenario creation should be rolled back",
                orphan_count,
                target_scenario_id,
            )
            raise RuntimeError(
                f"Scenario copy produced {orphan_count} orphaned edge(s) in {target_scenario_id}. "
                "This indicates a data integrity issue in the source scenario. "
                "The transaction has been aborted."
            )

        logger.info(
            "scenario.copy_nodes src=%s dst=%s nodes=%d edges=%d",
            source_scenario_id,
            target_scenario_id,
            count,
            edge_count,
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
        db: DictRowConnection,
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
            f"SELECT node_id, {field_name} FROM nodes WHERE node_id = %s AND scenario_id = %s",
            (node_id, scenario_id),
        ).fetchone()

        if row is None:
            # Fallback: the node_id may be from the baseline scenario.
            # Resolve to the corresponding node in the target scenario via
            # semantic match (node_type, item_id, location_id, time_ref).
            source_row = db.execute(
                "SELECT node_type, item_id, location_id, time_ref FROM nodes WHERE node_id = %s",
                (node_id,),
            ).fetchone()
            if source_row is not None:
                resolved = db.execute(
                    f"""
                    SELECT node_id, {field_name} FROM nodes
                    WHERE scenario_id = %s
                      AND node_type = %s
                      AND item_id IS NOT DISTINCT FROM %s
                      AND location_id IS NOT DISTINCT FROM %s
                      AND time_ref IS NOT DISTINCT FROM %s
                    LIMIT 1
                    """,
                    (
                        scenario_id,
                        source_row["node_type"],
                        source_row["item_id"],
                        source_row["location_id"],
                        source_row["time_ref"],
                    ),
                ).fetchone()
                if resolved is not None:
                    node_id = UUID(str(resolved["node_id"]))
                    row = resolved

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
        # ON CONFLICT: if a prior override exists, update values — but KEEP
        # the original old_value: it is the pre-first-override capture that
        # promote's conflict detection compares against the current baseline
        # (#341). Overwriting it with EXCLUDED.old_value would store the
        # scenario's CURRENT value (= the previous override's new_value) and
        # guarantee a false conflict at promote after any re-override.
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
                old_value  = scenario_overrides.old_value,
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
        db: DictRowConnection,
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

                # Use the scenario node_id if available, else baseline node_id.
                # At least one side is non-None here: if both were None, b_val and
                # s_val would both be None (equal) and we would have `continue`d
                # above. Resolve explicitly so the type is a plain dict.
                node_row = s_node if s_node is not None else b_node
                if node_row is None:  # unreachable given the invariant above; guard for the type
                    continue
                node_id = UUID(str(node_row["node_id"]))

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

    def _latest_calc_run(self, scenario_id: UUID, db: DictRowConnection) -> UUID:
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
        db: DictRowConnection,
        promoted_by: Optional[str] = None,
    ) -> PromoteResult:
        """
        Promote a scenario to baseline.

        Per EXPERT Q4.5 — merge is a first-class event:
          1. Conflict detection (ADR-018 P2.2.c): for each override, compare
             the baseline value captured at override time (scenario_overrides.
             old_value) with the CURRENT baseline value. Any divergence →
             PromoteConflictError with the full conflict list, nothing written.
             A missing baseline node (deleted since the fork) is logged and
             skipped, matching the historical patch behaviour.
          2. For each node override in the scenario, apply its new_value to
             the corresponding baseline node (matched by business key).
          3. Mark the promoted scenario as 'archived'.
          4. Create a 'scenario_merge' event (user_ref = promoted_by).
          5. Log the logical invalidation of sibling scenarios (same parent,
             still active): their computed state predates the new baseline.
             Schema-level 'stale' status is still future work.

        The scenario being promoted retains its own nodes. The baseline
        scenario's nodes are patched with the promoted values.

        Raises PromoteConflictError before any write if the baseline diverged.
        Returns a PromoteResult for the caller's audit trail.
        """
        # 1. Load all overrides for this scenario (old_value = baseline value
        #    captured at override time — the conflict-detection reference).
        override_rows = db.execute(
            """
            SELECT node_id, field_name, old_value, new_value
            FROM scenario_overrides
            WHERE scenario_id = %s
            """,
            (scenario_id,),
        ).fetchall()

        # 2. Load nodes from the promoted scenario → build match key
        scenario_nodes = _fetch_nodes_as_dict(scenario_id, db)
        scenario_index = {UUID(str(n["node_id"])): n for n in scenario_nodes}

        now = datetime.now(timezone.utc)

        # PASS 1 — read-only: resolve baseline targets and detect divergence.
        # No write happens until the whole overlay is known to be clean.
        conflicts: list[PromoteConflict] = []
        patch_plan: list[tuple[UUID, str, str]] = []  # (baseline_node_id, field, new_value)

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

            # Find the matching baseline node(s) by business key, reading the
            # current value of the overridden field for divergence detection.
            # field_name is whitelisted above (never raw user input in SQL).
            b_key = _node_business_key(s_node)
            b_rows = db.execute(
                f"""
                SELECT node_id, {field_name} FROM nodes
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

            if not b_rows:
                logger.warning(
                    "promote.skip no active baseline node matches business key "
                    "for override node_id=%s field=%s",
                    ov_node_id,
                    field_name,
                )
                continue

            expected: Optional[str] = ov["old_value"]
            for b_row in b_rows:
                b_node_id = UUID(str(b_row["node_id"]))
                current = b_row[field_name]
                actual: Optional[str] = str(current) if current is not None else None
                if actual != expected:
                    conflicts.append(
                        PromoteConflict(
                            node_id=b_node_id,
                            field_name=field_name,
                            expected=expected,
                            actual=actual,
                        )
                    )
                else:
                    patch_plan.append((b_node_id, field_name, new_value))

        if conflicts:
            logger.warning(
                "promote.conflict scenario=%s conflicts=%d — baseline diverged, "
                "nothing written",
                scenario_id,
                len(conflicts),
            )
            raise PromoteConflictError(scenario_id, conflicts)

        # PASS 2 — apply the (clean) overlay to the baseline.
        patched = 0
        for b_node_id, field_name, new_value in patch_plan:
            db.execute(
                f"""
                UPDATE nodes
                SET {field_name} = %s,
                    is_dirty     = TRUE,
                    updated_at   = %s
                WHERE node_id = %s AND scenario_id = %s
                """,
                (new_value, now, b_node_id, _BASELINE_ID),
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
                processed, source, user_ref, created_at
            ) VALUES (%s, 'scenario_merge', %s, %s, %s, FALSE, 'engine', %s, %s)
            """,
            (
                event_id,
                _BASELINE_ID,
                str(scenario_id),   # old_text = source scenario_id
                "promoted",
                promoted_by,
                now,
            ),
        )

        # 5. Sibling invalidation — the baseline just moved under their feet,
        #    so every still-active scenario forked from the same parent now
        #    holds computed results that predate the new baseline. Logged as
        #    a logical invalidation (schema-level 'stale' status: future work).
        siblings_invalidated = 0
        parent_row = db.execute(
            "SELECT parent_scenario_id FROM scenarios WHERE scenario_id = %s",
            (scenario_id,),
        ).fetchone()
        if parent_row is not None and parent_row["parent_scenario_id"] is not None:
            sibling_rows = db.execute(
                """
                SELECT scenario_id, name FROM scenarios
                WHERE parent_scenario_id = %s
                  AND scenario_id <> %s
                  AND status = 'active'
                  AND is_baseline = FALSE
                """,
                (parent_row["parent_scenario_id"], scenario_id),
            ).fetchall()
            for sib in sibling_rows:
                logger.warning(
                    "promote.sibling_invalidated scenario_id=%s name=%r — computed "
                    "state predates baseline changes from promoted scenario %s; "
                    "re-run a calculation before trusting its results",
                    sib["scenario_id"],
                    sib["name"],
                    scenario_id,
                )
            siblings_invalidated = len(sibling_rows)

        logger.info(
            "promote.complete scenario=%s overrides=%d patched_nodes=%d "
            "siblings_invalidated=%d merge_event=%s by=%s",
            scenario_id,
            len(override_rows),
            patched,
            siblings_invalidated,
            event_id,
            promoted_by,
        )
        return PromoteResult(
            scenario_id=scenario_id,
            override_count=len(override_rows),
            patched_nodes=patched,
            siblings_invalidated=siblings_invalidated,
            merge_event_id=event_id,
            conflict_checked=True,
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


def _fetch_nodes_as_dict(scenario_id: UUID, db: DictRowConnection) -> list[dict]:
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
