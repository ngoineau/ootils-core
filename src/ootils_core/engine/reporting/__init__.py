"""
Human-facing report rendering (ADR-042 decision 3 §5, PR-4c).

daily_report: ``render_daily_report`` — deterministic, DB-free Markdown
    compte-rendu for one governed daily run (the "daily update via la
    Dropbox" the pilot asked for 2026-07-17). ``build_shortages_summary`` —
    the SELECT-only helper that gathers the top-N active shortages by $
    severity for the report's pénuries section.
"""
from ootils_core.engine.reporting.daily_report import (
    build_shortages_summary,
    render_daily_report,
)

__all__ = [
    "build_shortages_summary",
    "render_daily_report",
]
