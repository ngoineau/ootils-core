"""
Human/ERP-facing report + export rendering (ADR-042 decision 3 §5 / decision 4).

daily_report: ``render_daily_report`` — deterministic, DB-free Markdown
    compte-rendu for one governed daily run (the "daily update via la
    Dropbox" the pilot asked for 2026-07-17). ``build_shortages_summary`` —
    the SELECT-only helper that gathers the top-N active shortages by $
    severity for the report's pénuries section.
outbound_export: the idempotent outbound export of governed recommendations
    into the ootils-outbox TSV pivot files (ADR-042 decision 4, PR-5).
    ``load_pending_export_rows`` (SELECT-only) / ``render_outbound_export``
    (deterministic, DB-free) / ``execute_export`` (the sole writer — files,
    then the ``exported_at`` stamp, then the ``export_executed`` event).
"""
from ootils_core.engine.reporting.daily_report import (
    build_shortages_summary,
    render_daily_report,
)
from ootils_core.engine.reporting.outbound_export import (
    ExportRunResult,
    OutboundExportRender,
    PendingExportRow,
    RenderedExportFile,
    UnroutableExportActionError,
    execute_export,
    load_pending_export_rows,
    render_outbound_export,
)

__all__ = [
    "build_shortages_summary",
    "render_daily_report",
    "ExportRunResult",
    "OutboundExportRender",
    "PendingExportRow",
    "RenderedExportFile",
    "UnroutableExportActionError",
    "execute_export",
    "load_pending_export_rows",
    "render_outbound_export",
]
