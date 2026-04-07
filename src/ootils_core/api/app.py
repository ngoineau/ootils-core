"""
app.py — FastAPI application factory for Ootils Core API.

Usage:
    uvicorn ootils_core.api.app:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from ootils_core.api.routers import events, explain, graph, ingest, issues, projection, simulate

logger = logging.getLogger(__name__)

# Load description from spec file if available
_SPEC_PATH = Path(__file__).parents[4] / "docs" / "api-spec.md"
_DESCRIPTION = _SPEC_PATH.read_text(encoding="utf-8") if _SPEC_PATH.exists() else (
    "Ootils Core REST API — supply chain planning engine."
)

API_VERSION = "1.0.0"


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    application = FastAPI(
        title="Ootils Core API",
        version=API_VERSION,
        description=_DESCRIPTION,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # Health endpoint (no auth)
    @application.get("/health", tags=["health"], include_in_schema=True)
    async def health() -> dict:
        return {"status": "ok", "version": API_VERSION}

    # Register routers
    application.include_router(events.router)
    application.include_router(projection.router)
    application.include_router(issues.router)
    application.include_router(explain.router)
    application.include_router(simulate.router)
    application.include_router(graph.router)
    application.include_router(ingest.router)

    @application.exception_handler(Exception)
    async def generic_exception_handler(request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled exception: %s", exc)
        return JSONResponse(
            status_code=500,
            content={"error": "internal_error", "message": str(exc), "status": 500},
        )

    logger.info("Ootils Core API v%s initialized", API_VERSION)
    return application


# Module-level app instance (for uvicorn / gunicorn)
app = create_app()
