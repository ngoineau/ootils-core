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

from anyio import to_thread
import psycopg

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

# slowapi is an optional dependency — used only when rate limiting is opted in
# via OOTILS_RATE_LIMIT_PER_MIN. Importing inline keeps `pip install` minimal
# for users who don't need it.
try:
    from slowapi import Limiter
    from slowapi.middleware import SlowAPIMiddleware
    from slowapi.util import get_remote_address
    _SLOWAPI_AVAILABLE = True
except ImportError:
    _SLOWAPI_AVAILABLE = False

from ootils_core.api.auth import _expected_token
from ootils_core.api.dependencies import _get_ootils_db, get_db
from ootils_core.api.routers import bom, calc, calendars, demo, dq, drp, events, explain, forecasting, ghosts, graph, ingest, issues, mrp, mrp_apics, param_overrides, planning_params, projection, pyramide, rccp, recommendations, scenarios, simulate, staging, stream
from ootils_core.api.routers.graph import nodes_router
from ootils_core.mps import router as mps_router
from ootils_core.atp import atp_router
from ootils_core.crp import crp_router

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
_TRUTHY = {"1", "true", "yes", "on"}


def _api_docs_enabled() -> bool:
    return os.environ.get("OOTILS_ENABLE_API_DOCS", "0").strip().lower() in _TRUTHY


def _security_headers_enabled() -> bool:
    # Default ON. Opt-out only with an explicit truthy flag, so accidental
    # misconfiguration keeps the safer behavior.
    return os.environ.get("OOTILS_DISABLE_SECURITY_HEADERS", "0").strip().lower() not in _TRUTHY


def _cors_allowed_origins() -> list[str]:
    """Parse OOTILS_CORS_ALLOWED_ORIGINS (comma-separated list).

    Default is empty — no cross-origin requests allowed. Wildcard "*" is
    intentionally NOT a default: opt-in only.
    """
    raw = os.environ.get("OOTILS_CORS_ALLOWED_ORIGINS", "").strip()
    if not raw:
        return []
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


def _rate_limit_config() -> str | None:
    """Return the slowapi rate-limit string from OOTILS_RATE_LIMIT_PER_MIN.

    Accepts an integer (interpreted as "<N>/minute") or a raw slowapi limit
    string like "60/minute;1000/hour". Returns None if unset → rate limiting
    is disabled.
    """
    raw = os.environ.get("OOTILS_RATE_LIMIT_PER_MIN", "").strip()
    if not raw:
        return None
    if raw.isdigit():
        return f"{raw}/minute"
    return raw


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


_AUDIT_INSERT_SQL = """
    INSERT INTO api_request_log (
        correlation_id, token_prefix, method, path,
        status_code, latency_ms, client_ip,
        token_id, actor_kind
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def _log_api_request(request: Request, status_code: int, latency_ms: int) -> None:
    if not _should_audit_request(request):
        return

    client_ip = request.client.host if request.client else None
    token_prefix = getattr(request.state, "client_id", None)
    correlation_id = getattr(request.state, "correlation_id", None)

    # #392: stamp the authenticated principal's identity when present. Absent on
    # an unauthenticated /health call or when a test overrides require_auth
    # without setting a principal — both write NULL, which the migration-064
    # columns allow.
    principal = getattr(request.state, "principal", None)
    token_id = getattr(principal, "token_id", None)
    actor_kind = getattr(principal, "actor_kind", None)

    params = (
        correlation_id,
        token_prefix,
        request.method,
        request.url.path,
        status_code,
        latency_ms,
        client_ip,
        token_id,
        actor_kind,
    )

    try:
        db_handle = _get_ootils_db()
        with db_handle.conn() as conn:
            try:
                conn.execute(_AUDIT_INSERT_SQL, params)
            except psycopg.errors.ForeignKeyViolation:
                # #392 security-review fix: token_id can reference an
                # api_tokens row that was HARD-DELETED after the in-process
                # principal cache (30 s TTL) authenticated this request —
                # the cache still vouches for the token, but the FK target
                # is gone. Losing the WHOLE audit row over a dangling
                # token_id would erase precisely the forensic window
                # (the last ≤30 s of a since-deleted, possibly compromised
                # token) that matters most. Retry once with token_id=NULL —
                # actor_kind + token_prefix are still written, so the row
                # stays attributable ("an agent-kind call happened, prefix
                # ootk_XXXXXXX") even though the FK link is gone. Soft
                # revoke (api_tokens.revoked_at) is the recommended
                # lifecycle op for exactly this reason; hard-DELETE remains
                # supported but must never puncture the audit trail.
                conn.rollback()
                fallback_params = params[:7] + (None, actor_kind)
                conn.execute(_AUDIT_INSERT_SQL, fallback_params)
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
        # Cap the anyio threadpool to the DB pool size so sync handlers
        # (dispatched to threads by FastAPI) never exceed the number of
        # available DB connections. OOTILS_THREADPOOL_SIZE overrides;
        # falls back to OOTILS_DB_POOL_MAX_SIZE (default 10).
        _threadpool_size = int(
            os.environ.get(
                "OOTILS_THREADPOOL_SIZE",
                os.environ.get("OOTILS_DB_POOL_MAX_SIZE", "10"),
            )
        )
        limiter = to_thread.current_default_thread_limiter()
        limiter.total_tokens = _threadpool_size
        logger.info("threadpool.sized tokens=%d", _threadpool_size)

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
        docs_url="/docs" if _api_docs_enabled() else None,
        redoc_url="/redoc" if _api_docs_enabled() else None,
        openapi_url="/openapi.json" if _api_docs_enabled() else None,
        lifespan=lifespan,
    )

    # Rate limiting — disabled by default. Opt in with OOTILS_RATE_LIMIT_PER_MIN
    # (e.g. "60" or "60/minute;1000/hour"). Per-IP via X-Forwarded-For when
    # present, otherwise via direct client address.
    # slowapi's SlowAPIMiddleware emits 429 responses itself; we keep its
    # default body since a custom exception_handler is not invoked when
    # the middleware short-circuits the request.
    rate_limit = _rate_limit_config()
    if rate_limit and _SLOWAPI_AVAILABLE:
        limiter = Limiter(
            key_func=get_remote_address,
            default_limits=[rate_limit],
            headers_enabled=True,
        )
        application.state.limiter = limiter
        application.add_middleware(SlowAPIMiddleware)
        logger.info("rate_limit.enabled config=%s", rate_limit)
    elif rate_limit and not _SLOWAPI_AVAILABLE:
        logger.warning(
            "OOTILS_RATE_LIMIT_PER_MIN=%s set but slowapi is not installed; "
            "install with `pip install slowapi` to enable rate limiting.",
            rate_limit,
        )

    # CORS — disabled by default. Configure with OOTILS_CORS_ALLOWED_ORIGINS.
    cors_origins = _cors_allowed_origins()
    if cors_origins:
        application.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            allow_headers=["Authorization", "Content-Type", "X-Correlation-ID"],
            max_age=600,
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

        if _security_headers_enabled():
            response.headers.setdefault("X-Content-Type-Options", "nosniff")
            response.headers.setdefault("X-Frame-Options", "DENY")
            response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
            # HSTS only on HTTPS — sending it over plain HTTP is harmless but
            # can trip strict scanners. Honor X-Forwarded-Proto for proxies.
            scheme = (
                request.headers.get("X-Forwarded-Proto")
                or request.url.scheme
            )
            if scheme == "https":
                response.headers.setdefault(
                    "Strict-Transport-Security",
                    "max-age=31536000; includeSubDomains",
                )
            # CSP: relax on /docs and /redoc (Swagger needs inline scripts +
            # the swagger CDN). Strict default elsewhere.
            path = request.url.path
            if path in ("/docs", "/redoc") or path.startswith("/docs/") or path.startswith("/redoc/"):
                response.headers.setdefault(
                    "Content-Security-Policy",
                    "default-src 'self' https://cdn.jsdelivr.net; "
                    "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
                    "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
                    "img-src 'self' data: https://fastapi.tiangolo.com",
                )
            else:
                response.headers.setdefault(
                    "Content-Security-Policy",
                    "default-src 'none'; frame-ancestors 'none'",
                )

        _log_api_request(request, response.status_code, latency_ms)
        return response

    # Health endpoint (no auth)
    @application.get("/health", tags=["health"], include_in_schema=True)
    async def health() -> dict:
        return {"status": "ok", "version": API_VERSION}

    # Register routers
    application.include_router(events.router)
    application.include_router(stream.router)
    application.include_router(projection.router)
    application.include_router(issues.router)
    application.include_router(explain.router)
    application.include_router(simulate.router)
    application.include_router(graph.router)
    application.include_router(nodes_router)
    application.include_router(ingest.router)
    application.include_router(staging.router)
    application.include_router(dq.router)
    application.include_router(bom.router)
    application.include_router(calendars.router)
    application.include_router(rccp.router)
    application.include_router(ghosts.router)
    application.include_router(planning_params.router)
    application.include_router(recommendations.router)
    application.include_router(scenarios.router)
    application.include_router(param_overrides.router)
    application.include_router(calc.router)
    application.include_router(demo.router)
    application.include_router(mrp.router)
    application.include_router(mrp_apics.router)
    application.include_router(drp.router)
    application.include_router(forecasting.router)
    application.include_router(pyramide.router)
    application.include_router(mps_router)
    application.include_router(atp_router)
    application.include_router(crp_router)

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
