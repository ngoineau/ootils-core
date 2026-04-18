"""
app.py — FastAPI application factory for Ootils Core API.

Usage:
    uvicorn ootils_core.api.app:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from ootils_core.api.auth import _expected_token
from ootils_core.api.routers import bom, calc, calendars, dq, events, explain, ghosts, graph, ingest, issues, mrp, planning_params, projection, rccp, scenarios, simulate
from ootils_core.api.routers.graph import nodes_router

logger = logging.getLogger(__name__)

# Load description from spec file if available
_SPEC_PATH = Path(__file__).parents[4] / "docs" / "api-spec.md"
_DESCRIPTION = _SPEC_PATH.read_text(encoding="utf-8") if _SPEC_PATH.exists() else (
    "Ootils Core REST API — supply chain planning engine."
)

API_VERSION = "1.0.0"

_INGEST_MAX_BYTES = 10 * 1024 * 1024  # 10 MB


class IngestPayloadSizeLimitMiddleware(BaseHTTPMiddleware):
    """Reject ingest requests whose body exceeds _INGEST_MAX_BYTES (10 MB)."""

    async def dispatch(self, request: Request, call_next):
        if request.url.path.startswith("/v1/ingest/"):
            content_length = request.headers.get("content-length")
            if content_length is not None and int(content_length) > _INGEST_MAX_BYTES:
                return JSONResponse(
                    status_code=413,
                    content={
                        "error": "payload_too_large",
                        "message": f"Request body exceeds the 10 MB limit for /v1/ingest/* endpoints.",
                        "status": 413,
                    },
                )
        return await call_next(request)


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    _expected_token()

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        if os.environ.get("OOTILS_ENABLE_STARTUP_RECOVERY", "0").lower() in {"1", "true", "yes", "on"}:
            from ootils_core.api.dependencies import _get_ootils_db
            from ootils_core.api.routers.events import _build_propagation_engine
            from ootils_core.engine.orchestration.calc_run import CalcRunManager

            db_handle = _get_ootils_db()
            calc_run_mgr = CalcRunManager()

            with db_handle.conn() as conn:
                replayable_runs = calc_run_mgr.recover_pending_runs(conn)
                if not replayable_runs:
                    logger.info("startup.recovery none")
                else:
                    engine = _build_propagation_engine(conn)
                    for run in replayable_runs:
                        dirty_nodes = engine._dirty.get_dirty_nodes(run.calc_run_id, run.scenario_id, conn)
                        if not dirty_nodes:
                            logger.warning(
                                "startup.recovery skipped calc_run=%s scenario=%s, no durable dirty nodes",
                                run.calc_run_id,
                                run.scenario_id,
                            )
                            continue

                        try:
                            logger.info(
                                "startup.recovery replay calc_run=%s scenario=%s dirty_nodes=%d",
                                run.calc_run_id,
                                run.scenario_id,
                                len(dirty_nodes),
                            )
                            engine._propagate(run, dirty_nodes, conn)
                            engine._finish_run(run, run.scenario_id, conn)
                        except Exception as exc:
                            logger.exception(
                                "startup.recovery failed calc_run=%s scenario=%s: %s",
                                run.calc_run_id,
                                run.scenario_id,
                                exc,
                            )
                            engine._calc_run_mgr.fail_calc_run(
                                run,
                                f"Startup replay failed: {exc}",
                                conn,
                            )

        yield

    application = FastAPI(
        title="Ootils Core API",
        version=API_VERSION,
        description=_DESCRIPTION,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # Payload size limit middleware for ingest routes
    application.add_middleware(IngestPayloadSizeLimitMiddleware)

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
    application.include_router(nodes_router)
    application.include_router(ingest.router)
    application.include_router(dq.router)
    application.include_router(bom.router)
    application.include_router(calendars.router)
    application.include_router(rccp.router)
    application.include_router(ghosts.router)
    application.include_router(planning_params.router)
    application.include_router(scenarios.router)
    application.include_router(calc.router)
    application.include_router(mrp.router)

    @application.exception_handler(Exception)
    async def generic_exception_handler(request, exc: Exception) -> JSONResponse:
        # Log full exception internally but never return raw error strings to callers —
        # they can leak stack traces, DB connection strings, or file paths.
        logger.exception("Unhandled exception: %s", exc)
        return JSONResponse(
            status_code=500,
            content={"error": "internal_error", "message": "An internal error occurred.", "status": 500},
        )

    logger.info("Ootils Core API v%s initialized", API_VERSION)
    return application


# Module-level app instance (for uvicorn / gunicorn)
app = create_app()
