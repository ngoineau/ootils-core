"""
metrics.py — Prometheus instrumentation for the Ootils Core API (#392 AN-2 PR2b).

Defines the process-global metric collectors and the thin helpers the request
middleware / auth layer call to record them. Kept in its OWN module (no import
of ``auth`` / ``app``) so it can be imported from both without a cycle.

The metric objects are module-level singletons on purpose: prometheus-client
registers each collector in the default ``REGISTRY`` at construction, and
constructing the SAME metric twice raises ``Duplicated timeseries``. Defining
them once at import time (rather than inside ``create_app``) is what makes the
app safe to build more than once in a process — tests build several apps, and
``scripts/export_openapi.py`` builds one too. These are effectively constants
(registered collectors with a fixed shape), not the mutable-module-global
anti-pattern the repo forbids.

CARDINALITY IS BOUNDED BY CONSTRUCTION — the golden rule for a metrics label
set. ``route`` is the matched ROUTE TEMPLATE (``/v1/tokens/{token_id}``), never
the raw request path (``/v1/tokens/9f3a...``): an unbounded stream of distinct
UUIDs in the path would otherwise explode the time-series count. A request that
matched no route collapses to the single literal ``"unmatched"``. ``method`` /
``status`` are small finite enums; ``actor_kind`` is one of
{agent, human, service, none}.
"""
from __future__ import annotations

from fastapi import Request
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

# actor_kind label value for a request with no resolved principal (an
# unauthenticated /health probe, or a 401 that never reached a Principal).
_ACTOR_KIND_NONE = "none"

# Route-template label value for a request that matched no route (404 on an
# unknown path). Keeps cardinality bounded — every unmatched path collapses here.
_ROUTE_UNMATCHED = "unmatched"


http_requests_total = Counter(
    "ootils_http_requests_total",
    "Total HTTP requests handled, by route template, method, status and actor kind.",
    ["route", "method", "status", "actor_kind"],
)

http_request_duration_seconds = Histogram(
    "ootils_http_request_duration_seconds",
    "HTTP request handling latency in seconds, by route template and method.",
    ["route", "method"],
)

rate_limited_total = Counter(
    "ootils_rate_limited_total",
    "Requests rejected with 429 by the per-token rate limiter, by actor kind.",
    ["actor_kind"],
)

fleet_killswitch_total = Counter(
    "ootils_fleet_killswitch_total",
    "Agent requests rejected with 503 by the fleet kill switch (OOTILS_AGENTS_ENABLED).",
)


def route_template(request: Request) -> str:
    """Return the matched route TEMPLATE for cardinality-safe labelling.

    FastAPI's ``APIRoute.matches`` stores the matched route on
    ``request.scope["route"]`` (its ``.path`` is the template, e.g.
    ``/v1/tokens/{token_id}``). Read AFTER the downstream app has routed the
    request (i.e. after ``call_next`` in the middleware). No match → the literal
    ``"unmatched"`` so an unknown path never mints a new label value."""
    route = request.scope.get("route")
    path = getattr(route, "path", None)
    return path if isinstance(path, str) and path else _ROUTE_UNMATCHED


def _actor_kind(request: Request) -> str:
    principal = getattr(request.state, "principal", None)
    kind = getattr(principal, "actor_kind", None)
    return kind if isinstance(kind, str) and kind else _ACTOR_KIND_NONE


def observe_request(
    request: Request, *, status_code: int, duration_seconds: float
) -> None:
    """Record one completed request into the counter + latency histogram.

    Best-effort: instrumentation must never break a response, so any failure
    here is swallowed by the middleware's own guard (see app.py). Called once
    per request from ``request_context_middleware``."""
    route = route_template(request)
    method = request.method
    http_requests_total.labels(
        route=route,
        method=method,
        status=str(status_code),
        actor_kind=_actor_kind(request),
    ).inc()
    http_request_duration_seconds.labels(route=route, method=method).observe(
        duration_seconds
    )


def record_rate_limited(actor_kind: str) -> None:
    """Increment the 429 counter (called from ``auth._enforce_rate_limit``)."""
    rate_limited_total.labels(actor_kind=actor_kind).inc()


def record_fleet_killswitch() -> None:
    """Increment the fleet-kill-switch 503 counter (from ``auth._enforce_fleet_kill_switch``)."""
    fleet_killswitch_total.inc()


def render_latest() -> tuple[bytes, str]:
    """Return ``(body, content_type)`` for the ``/metrics`` scrape endpoint."""
    return generate_latest(), CONTENT_TYPE_LATEST
