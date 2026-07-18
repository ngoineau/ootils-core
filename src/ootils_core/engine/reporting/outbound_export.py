"""
outbound_export.py — idempotent outbound export of governed recommendations
into the ootils-outbox TSV pivot files (ADR-042 decision 4, chantier PR-5 of
the pilot-decided delivery order — "Sortant idempotent (`exported_at`,
`export_executed`) + réconciliation heuristique -> `recommendation_outcomes`,
la réconciliation elle-même reste hors scope de ce module).

Same three-way DB-boundary split as every other engine module touching this
concern (``engine/maintenance/purge.py``'s ``plan_*``/``apply_*``,
``engine/reporting/daily_report.py``'s render/build split):

  (a) ``load_pending_export_rows`` — the ONE SELECT-only helper. Reads the
      ``recommendations`` rows eligible for export (``status IN ('APPROVED',
      'APPLIED')`` AND ``exported_at IS NULL`` — the ``ix_reco_pending_export``
      partial index, migration 078), with every foreign lookup (target PO
      external id, source/dest location external id) already resolved so the
      renderer stays DB-free. Zero DRAFT/REVIEWED/REJECTED/EXPIRED row is
      EVER eligible — by construction of the WHERE clause, not by a runtime
      filter a caller could get wrong.
  (b) ``render_outbound_export`` — the renderer. DETERMINISTIC and DB-FREE:
      given the SAME ``PendingExportRow`` tuple and ``run_date``, it returns
      byte-identical TSV content, every time. One file per family, DISJOINT
      columns per the outbound flows of ADR-042 decision 1 (§"Sortants"):
      ``po_drafts_<AAAAMMJJ>.tsv`` (ORDER_NOW/ORDER_RUSH/EXPEDITE),
      ``reschedule_messages_<AAAAMMJJ>.tsv`` (RESCHEDULE_IN/RESCHEDULE_OUT/
      CANCEL, ADR-026), ``transfers_<AAAAMMJJ>.tsv`` (TRANSFER). A family with
      ZERO eligible rows produces NO file (never an empty TSV with header-only
      — an absent file is the honest signal "nothing of this kind today").
  (c) ``execute_export`` — the sole writer. Writes the rendered files to
      ``outbox_dir``, THEN stamps ``recommendations.exported_at`` for exactly
      the recommendation_ids that were written, THEN emits ONE
      ``export_executed`` stream event (migration 085) — same connection,
      same transaction. Never commits/rolls back itself (the caller owns the
      transaction, same convention as every other engine module in this
      repo).

ORDER IS THE CONTRACT (write -> stamp -> emit), and it is deliberate: a crash
or rollback between "files written" and "the caller's commit" leaves the
just-written files on disk WITHOUT their ``exported_at`` stamp having
survived (the UPDATE rolls back with everything else in the same
transaction) — the NEXT run's ``load_pending_export_rows`` sees the exact
same pending rows again, re-renders BYTE-IDENTICAL content (rendering is pure
and deterministic over the same inputs) and overwrites the orphaned files
in place. The one state that must NEVER happen — a recommendation stamped
``exported_at`` with no corresponding file ever having reached disk — is
structurally impossible here: the stamp only runs AFTER every file in the
render has been written successfully, and the stamp itself only NEVER
commits without the caller's own commit succeeding too (same all-or-nothing
transaction as the files' logical write). A caller that wants durability
guarantees beyond "files may occasionally be rewritten identically" must
still commit — this module offers idempotent-by-construction retries, not a
distributed transaction across the filesystem and Postgres.

SCENARIO: BASELINE-ONLY, HARDCODED, NOT A PARAMETER. ``recommendations`` is
NOT scenario-exclusive the way ``inventory_snapshots``/``daily_runs`` are —
it carries a real ``scenario_id`` column (migration 039) and at least two
watchers accept an explicit ``--scenario`` override for what-if runs against
a fork (``scripts/agent_reschedule_watcher.py``, ``engine/recommendation/
transfer.py`` — a DRP/reschedule counter-factual can be approved inside a
fork for analysis purposes without ever being a real-world decision). A
fork's recommendation is SIMULATED, never OBSERVED (ADR-030's baseline-only
doctrine — the exact same reasoning ``inventory_snapshots``/
``recommendation_outcomes``/``daily_runs`` already apply, and the one
``engine/events/emit.py``'s ``export_executed`` contract block states
explicitly). Exporting a fork's APPROVED/APPLIED row to the REAL ERP outbox
would push a what-if action into the real world — a correctness bug this
module refuses structurally: ``load_pending_export_rows`` filters
``scenario_id = BASELINE_SCENARIO_ID`` unconditionally, with no parameter to
override it. A promoted fork's recommendations become baseline rows through
``ScenarioManager.promote`` (a separate, already-governed path) and are
picked up on their own merit once they are baseline.

NEVER A DRAFT, BY CONSTRUCTION: the ``status IN ('APPROVED', 'APPLIED')``
predicate in ``load_pending_export_rows`` is the ENTIRE gate — there is no
second check anywhere in this module that could be bypassed. A DRAFT
recommendation is structurally invisible to this SELECT.

UNROUTABLE ACTIONS FAIL LOUDLY, NEVER SILENTLY STRANDED. ``DEFER`` is a real
value in the ``recommendations.action`` CHECK (migration 061) but the
deterministic core never emits it in V1 (``engine/recommendation/
reschedule.py``'s own module docstring: "reserved for manual/agent use").
Today, therefore, every ``status IN ('APPROVED','APPLIED')`` row's action is
expected to land in exactly one of the three families below. If a future
action is added to the CHECK without updating this module's routing (or a
DEFER genuinely reaches APPROVED/APPLIED), ``render_outbound_export`` raises
``UnroutableExportActionError`` rather than leaving that row's
``exported_at`` NULL forever with no visible signal — a silent permanent
"never exported" state would be strictly worse than a loud failure that
blocks THAT run until the routing is fixed (fail-loudly over silent wrong
answers, CONTRIBUTING.md).
"""
from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional
from uuid import UUID

from psycopg.rows import dict_row

from ootils_core.constants import BASELINE_SCENARIO_ID
from ootils_core.db.types import DictRowConnection
from ootils_core.engine.events.emit import emit_stream_event

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Family routing — action -> outbound TSV file (ADR-042 decision 1 §Sortants)
# ---------------------------------------------------------------------------
PO_DRAFT_ACTIONS: frozenset[str] = frozenset({"ORDER_NOW", "ORDER_RUSH", "EXPEDITE"})
RESCHEDULE_ACTIONS: frozenset[str] = frozenset({"RESCHEDULE_IN", "RESCHEDULE_OUT", "CANCEL"})
TRANSFER_ACTIONS: frozenset[str] = frozenset({"TRANSFER"})

_ELIGIBLE_STATUSES: tuple[str, ...] = ("APPROVED", "APPLIED")


class UnroutableExportActionError(Exception):
    """Raised by ``render_outbound_export`` when a pending-export
    recommendation's action matches none of the three known outbound
    families. See the module docstring's "UNROUTABLE ACTIONS" section — this
    is a fail-loud guard, not a recoverable/retryable condition; it means the
    routing table in this module is out of sync with the
    ``recommendations.action`` CHECK vocabulary and needs a code change."""


# ---------------------------------------------------------------------------
# (a) load_pending_export_rows — the SELECT-only helper
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PendingExportRow:
    """One recommendation eligible for outbound export — every field the
    three TSV renderers need, already resolved (target PO external id via
    ``external_references``, source/dest location external id) so
    ``render_outbound_export`` never touches the DB."""

    recommendation_id: UUID
    action: str
    item_external_id: str
    supplier_external_id: Optional[str]
    recommended_qty: Decimal
    shortage_date: date
    proposed_date: Optional[date]
    current_receipt_date: Optional[date]
    confidence: str
    target_node_id: Optional[UUID]
    target_po_external_id: Optional[str]
    source_location_external_id: Optional[str]
    dest_location_external_id: Optional[str]


_PENDING_EXPORT_SQL = """
    SELECT
        r.recommendation_id,
        r.action,
        r.item_external_id,
        r.supplier_external_id,
        r.recommended_qty,
        r.shortage_date,
        r.proposed_date,
        r.current_receipt_date,
        r.confidence,
        r.target_node_id,
        po_ref.external_id  AS target_po_external_id,
        src_loc.external_id AS source_location_external_id,
        dst_loc.external_id AS dest_location_external_id
    FROM recommendations r
    LEFT JOIN external_references po_ref
        ON po_ref.entity_type = 'purchase_order'
       AND po_ref.internal_id = r.target_node_id
    LEFT JOIN locations src_loc ON src_loc.location_id = r.source_location_id
    LEFT JOIN locations dst_loc ON dst_loc.location_id = r.dest_location_id
    WHERE r.scenario_id = %s
      AND r.status = ANY(%s)
      AND r.exported_at IS NULL
    ORDER BY r.action, r.recommendation_id
"""


def load_pending_export_rows(conn: DictRowConnection) -> tuple[PendingExportRow, ...]:
    """SELECT-only (a): every BASELINE recommendation eligible for outbound
    export. See the module docstring's "SCENARIO" section for why
    ``scenario_id`` is hardcoded to ``BASELINE_SCENARIO_ID`` and never a
    parameter. Writes nothing, never commits/rolls back."""
    cur = conn.cursor(row_factory=dict_row)
    rows = cur.execute(
        _PENDING_EXPORT_SQL, (BASELINE_SCENARIO_ID, list(_ELIGIBLE_STATUSES))
    ).fetchall()
    return tuple(_to_pending_row(r) for r in rows)


def _to_pending_row(row: dict) -> PendingExportRow:
    return PendingExportRow(
        recommendation_id=_coerce_uuid(row["recommendation_id"]),
        action=row["action"],
        item_external_id=row["item_external_id"],
        supplier_external_id=row["supplier_external_id"],
        recommended_qty=row["recommended_qty"],
        shortage_date=row["shortage_date"],
        proposed_date=row["proposed_date"],
        current_receipt_date=row["current_receipt_date"],
        confidence=row["confidence"],
        target_node_id=_coerce_uuid(row["target_node_id"]) if row["target_node_id"] else None,
        target_po_external_id=row["target_po_external_id"],
        source_location_external_id=row["source_location_external_id"],
        dest_location_external_id=row["dest_location_external_id"],
    )


def _coerce_uuid(value: object) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


# ---------------------------------------------------------------------------
# (b) render_outbound_export — the renderer. DETERMINISTIC, DB-free.
# ---------------------------------------------------------------------------
# REGLES D'OR (pilot rule, 2026-07-11/13 — TSV-only, "c'est un enfer" sinon):
#   * tab-separated, no quoting (mirrors TSV-FILES-SPEC.md §1.1 for inbound)
#   * UTF-8, NO BOM (``encoding="utf-8"``, never ``"utf-8-sig"``)
#   * dates ISO 8601 (YYYY-MM-DD)
#   * no literal "NULL"/"None" — an absent value is an EMPTY cell


def _tsv_cell(value: Optional[str]) -> str:
    """None -> empty cell (never a literal 'NULL'/'None'). Fails loudly if a
    business value somehow carries a TSV-structural character (tab/newline)
    — silently substituting it would corrupt the file's column alignment,
    which the ERP side cannot detect on its own."""
    if value is None:
        return ""
    if "\t" in value or "\n" in value or "\r" in value:
        raise ValueError(
            "outbound_export: cell value contains a TSV-structural "
            f"character (tab/CR/LF), refusing to write a corrupt outbox "
            f"file: {value!r}"
        )
    return value


def _fmt_decimal(value: Decimal) -> str:
    """Fixed-point, no thousands separator, no scientific notation — a
    machine-readable TSV cell, not the human-formatted Markdown daily
    report's ``_fmt_qty``."""
    return format(value, "f")


def _fmt_date(value: Optional[date]) -> str:
    return value.isoformat() if value is not None else ""


def _render_tsv(
    header: tuple[str, ...], rows: Sequence[tuple[Optional[str], ...]]
) -> str:
    lines = ["\t".join(header)]
    lines.extend("\t".join(_tsv_cell(cell) for cell in row) for row in rows)
    return "\n".join(lines) + "\n"


def _filename(family: str, run_date: date) -> str:
    return f"{family}_{run_date:%Y%m%d}.tsv"


@dataclass(frozen=True)
class RenderedExportFile:
    """One rendered outbound TSV — a family with >=1 eligible row. A family
    with zero eligible rows produces NO ``RenderedExportFile`` (see module
    docstring: "a family with ZERO eligible rows produces NO file")."""

    filename: str
    content: str
    recommendation_ids: tuple[UUID, ...]


_PO_DRAFTS_HEADER: tuple[str, ...] = (
    "item_external_id",
    "supplier_external_id",
    "quantity",
    "need_date",
    "action",
    "recommendation_id",
    "confidence",
)

_RESCHEDULE_MESSAGES_HEADER: tuple[str, ...] = (
    "item_external_id",
    "target_po_reference",
    "current_receipt_date",
    "proposed_date",
    "action",
    "recommendation_id",
)

_TRANSFERS_HEADER: tuple[str, ...] = (
    "item_external_id",
    "source_location_external_id",
    "dest_location_external_id",
    "quantity",
    "shortage_date",
    "recommendation_id",
)


def _render_po_drafts(
    rows: Sequence[PendingExportRow], run_date: date
) -> Optional[RenderedExportFile]:
    """ORDER_NOW/ORDER_RUSH/EXPEDITE. ``need_date`` is ``proposed_date`` when
    present, else ``shortage_date`` — in practice always ``shortage_date`` in
    V1 (migration 061: ``proposed_date`` is NULL for every non-reschedule
    action today), but the COALESCE keeps this file forward-compatible with
    an eventual EXPEDITE-with-a-proposed-date variant without a schema/format
    change."""
    eligible = [r for r in rows if r.action in PO_DRAFT_ACTIONS]
    if not eligible:
        return None
    body = [
        (
            r.item_external_id,
            r.supplier_external_id,
            _fmt_decimal(r.recommended_qty),
            _fmt_date(r.proposed_date if r.proposed_date is not None else r.shortage_date),
            r.action,
            str(r.recommendation_id),
            r.confidence,
        )
        for r in eligible
    ]
    return RenderedExportFile(
        filename=_filename("po_drafts", run_date),
        content=_render_tsv(_PO_DRAFTS_HEADER, body),
        recommendation_ids=tuple(r.recommendation_id for r in eligible),
    )


def _target_po_reference(row: PendingExportRow) -> Optional[str]:
    """PO reference resolution: the real ERP external id when the target
    node is a tracked ``purchase_order`` in ``external_references``, else the
    raw internal ``target_node_id`` (a planned/firm order the ERP has never
    seen — see the module docstring's UUID fallback rationale)."""
    if row.target_po_external_id is not None:
        return row.target_po_external_id
    if row.target_node_id is not None:
        return str(row.target_node_id)
    return None


def _render_reschedule_messages(
    rows: Sequence[PendingExportRow], run_date: date
) -> Optional[RenderedExportFile]:
    """RESCHEDULE_IN/RESCHEDULE_OUT/CANCEL (ADR-026). DEFER is deliberately
    NOT routed here — see the module docstring's "UNROUTABLE ACTIONS"
    section."""
    eligible = [r for r in rows if r.action in RESCHEDULE_ACTIONS]
    if not eligible:
        return None
    body = [
        (
            r.item_external_id,
            _target_po_reference(r),
            _fmt_date(r.current_receipt_date),
            _fmt_date(r.proposed_date),
            r.action,
            str(r.recommendation_id),
        )
        for r in eligible
    ]
    return RenderedExportFile(
        filename=_filename("reschedule_messages", run_date),
        content=_render_tsv(_RESCHEDULE_MESSAGES_HEADER, body),
        recommendation_ids=tuple(r.recommendation_id for r in eligible),
    )


def _render_transfers(
    rows: Sequence[PendingExportRow], run_date: date
) -> Optional[RenderedExportFile]:
    """TRANSFER (DRP inter-site moves, ADR-028/#395)."""
    eligible = [r for r in rows if r.action in TRANSFER_ACTIONS]
    if not eligible:
        return None
    body = [
        (
            r.item_external_id,
            r.source_location_external_id,
            r.dest_location_external_id,
            _fmt_decimal(r.recommended_qty),
            _fmt_date(r.shortage_date),
            str(r.recommendation_id),
        )
        for r in eligible
    ]
    return RenderedExportFile(
        filename=_filename("transfers", run_date),
        content=_render_tsv(_TRANSFERS_HEADER, body),
        recommendation_ids=tuple(r.recommendation_id for r in eligible),
    )


@dataclass(frozen=True)
class OutboundExportRender:
    """The full, deterministic, DB-free render of one export run (b)."""

    run_date: date
    files: tuple[RenderedExportFile, ...]

    @property
    def recommendation_ids(self) -> tuple[UUID, ...]:
        ids: list[UUID] = []
        for f in self.files:
            ids.extend(f.recommendation_ids)
        return tuple(ids)


def render_outbound_export(
    rows: Sequence[PendingExportRow], run_date: date
) -> OutboundExportRender:
    """Render every non-empty outbound family for ``run_date`` — pure,
    deterministic, DB-free (b). Raises ``UnroutableExportActionError`` if any
    row's action matches none of the three known families (see module
    docstring)."""
    files: list[RenderedExportFile] = []
    routed_ids: set[UUID] = set()
    for renderer in (_render_po_drafts, _render_reschedule_messages, _render_transfers):
        rendered = renderer(rows, run_date)
        if rendered is not None:
            files.append(rendered)
            routed_ids.update(rendered.recommendation_ids)

    unrouted = [r for r in rows if r.recommendation_id not in routed_ids]
    if unrouted:
        offenders = ", ".join(f"{r.recommendation_id}(action={r.action!r})" for r in unrouted)
        raise UnroutableExportActionError(
            f"outbound_export: {len(unrouted)} pending recommendation(s) "
            "matched no known outbound family (po_drafts/"
            f"reschedule_messages/transfers): {offenders}"
        )

    return OutboundExportRender(run_date=run_date, files=tuple(files))


# ---------------------------------------------------------------------------
# (c) execute_export — the sole writer: files -> stamp -> event
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExportRunResult:
    """The outcome of one ``execute_export`` call."""

    run_date: date
    dry_run: bool
    files_written: tuple[str, ...]
    recommendation_ids_exported: tuple[UUID, ...]
    event_id: Optional[UUID]
    render: OutboundExportRender


def execute_export(
    conn: DictRowConnection,
    outbox_dir: Path,
    *,
    now: datetime,
    dry_run: bool,
) -> ExportRunResult:
    """The sole writer of the outbound-export lifecycle (c). See the module
    docstring's "ORDER IS THE CONTRACT" section for the write -> stamp ->
    emit ordering rationale.

    ``dry_run=True``: loads the pending rows and renders the full preview —
    ZERO file write, ZERO DB write (no stamp, no event). Safe from a
    read-only preview / dry-run CLI invocation.

    ``dry_run=False``: writes every rendered file under ``outbox_dir``, then
    stamps ``recommendations.exported_at = now`` for EXACTLY the
    recommendation_ids just written, then emits ONE ``export_executed``
    event (migration 085) — all on ``conn``, never committed/rolled back
    here (the caller owns the transaction).

    No event is emitted for a genuinely empty run (zero pending rows) — same
    "nothing to announce" posture as
    ``engine.events.emit.emit_recommendation_created_for_run``, so the
    ``events`` table does not accumulate a zero-content row on the (common)
    days nothing was approved.

    Does NOT commit — the caller owns the transaction.
    """
    run_date = now.date()
    rows = load_pending_export_rows(conn)
    render = render_outbound_export(rows, run_date)

    if dry_run:
        logger.info(
            "outbound_export.preview run_date=%s families=%d recommendations=%d",
            run_date, len(render.files), len(render.recommendation_ids),
        )
        return ExportRunResult(
            run_date=run_date,
            dry_run=True,
            files_written=(),
            recommendation_ids_exported=(),
            event_id=None,
            render=render,
        )

    if not render.files:
        logger.info("outbound_export.apply run_date=%s nothing_pending", run_date)
        return ExportRunResult(
            run_date=run_date,
            dry_run=False,
            files_written=(),
            recommendation_ids_exported=(),
            event_id=None,
            render=render,
        )

    outbox_dir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    for f in render.files:
        out_path = outbox_dir / f.filename
        # newline="\n": TSV-FILES-SPEC.md-style LF line endings, never
        # translated to CRLF by Python's universal-newlines write path.
        # encoding="utf-8" (never "utf-8-sig"): NO BOM, per the pilot's
        # REGLES D'OR for outbound files.
        out_path.write_text(f.content, encoding="utf-8", newline="\n")
        written.append(f.filename)
        logger.info(
            "outbound_export.file_written path=%s bytes=%d recommendations=%d",
            out_path, len(f.content.encode("utf-8")), len(f.recommendation_ids),
        )

    all_ids = render.recommendation_ids
    _stamp_exported(conn, all_ids, now)

    event_id = emit_stream_event(
        conn,
        "export_executed",
        BASELINE_SCENARIO_ID,
        field_changed="export_executed",
        new_date=run_date,
        new_quantity=len(all_ids),
        new_text=",".join(written),
        source="engine",
    )

    logger.info(
        "outbound_export.applied run_date=%s files=%s recommendations=%d event_id=%s",
        run_date, written, len(all_ids), event_id,
    )

    return ExportRunResult(
        run_date=run_date,
        dry_run=False,
        files_written=tuple(written),
        recommendation_ids_exported=all_ids,
        event_id=event_id,
        render=render,
    )


def _stamp_exported(
    conn: DictRowConnection, recommendation_ids: tuple[UUID, ...], now: datetime
) -> None:
    """The sole stamper: ``exported_at = now`` for EXACTLY these ids, still
    re-guarded by ``exported_at IS NULL`` (defense in depth against a
    concurrent export run racing this one on the same rows). Raises
    ``RuntimeError`` — fail loudly, before the event is ever emitted — if
    fewer rows were stamped than expected: the files are already on disk at
    this point (see the module docstring's crash-safety section for why that
    is safe), but committing a PARTIAL stamp with no explanation would be a
    silent wrong answer."""
    if not recommendation_ids:
        return
    cur = conn.execute(
        "UPDATE recommendations SET exported_at = %s "
        "WHERE recommendation_id = ANY(%s) AND exported_at IS NULL",
        (now, list(recommendation_ids)),
    )
    stamped = cur.rowcount if cur.rowcount is not None else 0
    if stamped != len(recommendation_ids):
        raise RuntimeError(
            f"outbound_export: expected to stamp exported_at on "
            f"{len(recommendation_ids)} recommendation(s), actually stamped "
            f"{stamped} — refusing to emit export_executed for a partial "
            "stamp (the files already written to disk remain there, safely "
            "overwritten by the next run)"
        )
