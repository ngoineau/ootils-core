"""
app.py — FastAPI application factory for Ootils Core API.

Usage:
    uvicorn ootils_core.api.app:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from ootils_core.api.auth import _expected_token
from ootils_core.api.dependencies import _get_ootils_db, get_db
from ootils_core.api.routers import bom, calc, calendars, dq, events, explain, ghosts, graph, ingest, issues, mrp, mrp_apics, planning_params, projection, rccp, scenarios, simulate
from ootils_core.api.routers.graph import nodes_router

logger = logging.getLogger(__name__)

# Load description from the workspace operational spec first, then fall back to
# the repo-local legacy API spec if the workspace spec is absent.
_WORKSPACE_ROOT = Path(__file__).parents[4]
_REPO_ROOT = Path(__file__).parents[3]
_SPEC_CANDIDATES = [
    _WORKSPACE_ROOT / "specs" / "SPEC-INTERFACES.md",
    _REPO_ROOT / "docs" / "api-spec.md",
]

_DESCRIPTION = "Ootils Core REST API — supply chain planning engine."
for _spec_path in _SPEC_CANDIDATES:
    if _spec_path.exists():
        _DESCRIPTION = _spec_path.read_text(encoding="utf-8")
        break

API_VERSION = "1.0.0"

_INGEST_MAX_BYTES = 10 * 1024 * 1024  # 10 MB


def _correlation_id_from_request(request: Request) -> str:
    raw = request.headers.get("X-Correlation-ID", "").strip()
    if raw:
        return raw[:128]
    return f"req_{uuid4().hex}"


def _should_audit_request(request: Request) -> bool:
    path = request.url.path
    if path != "/health" and not path.startswith("/v1/"):
        return False
    if request.app.dependency_overrides.get(get_db) is not None:
        return False
    return True


def _log_api_request(request: Request, status_code: int, latency_ms: int) -> None:
    if not _should_audit_request(request):
        return

    client_ip = request.client.host if request.client else None
    token_prefix = getattr(request.state, "client_id", None)
    correlation_id = getattr(request.state, "correlation_id", None)

    try:
        db_handle = _get_ootils_db()
        with db_handle.conn() as conn:
            conn.execute(
                """
                INSERT INTO api_request_log (
                    correlation_id, token_prefix, method, path,
                    status_code, latency_ms, client_ip
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    correlation_id,
                    token_prefix,
                    request.method,
                    request.url.path,
                    status_code,
                    latency_ms,
                    client_ip,
                ),
            )
    except Exception as exc:
        logger.warning(
            "audit.log_failed path=%s status=%s error=%s",
            request.url.path,
            status_code,
            exc,
        )


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
                        "message": "Request body exceeds the 10 MB limit for /v1/ingest/* endpoints.",
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

    @application.middleware("http")
    async def request_context_middleware(request: Request, call_next):
        correlation_id = _correlation_id_from_request(request)
        request.state.correlation_id = correlation_id
        started = time.perf_counter()

        try:
            response = await call_next(request)
        except Exception as exc:
            logger.exception("Unhandled exception: %s", exc)
            response = JSONResponse(
                status_code=500,
                content={"error": "internal_error", "message": "An internal error occurred.", "status": 500},
            )

        latency_ms = int((time.perf_counter() - started) * 1000)
        response.headers["X-Correlation-ID"] = correlation_id
        response.headers["X-API-Version"] = API_VERSION
        _log_api_request(request, response.status_code, latency_ms)
        return response

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
    application.include_router(mrp_apics.router)

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
