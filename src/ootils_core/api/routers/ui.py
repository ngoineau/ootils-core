"""
ui.py — GET /ui: the EXP-1 human window shell (ADR-036, issue #445).

DOCTRINE (ADR-036 — the arbitrage that lifts CONTRIBUTING.md's "API first, UI
never"): this page is a CLIENT of the API, never a privileged path. There is
exactly ONE auth mechanism in this codebase (``resolve_principal`` /
``require_scope``, ``api/auth.py``) and this router does not introduce a
second one. ``GET /ui`` itself carries NO auth and NO business data — it is a
static HTML shell, exactly like ``/health``: a browser does not attach an
``Authorization`` header to a plain navigation, so any auth check on this
route would be theatre, not a boundary. The human operator's token is entered
in a form field and held in the BROWSER's ``sessionStorage`` — never a
server-side session, never a cookie. A cookie is explicitly refused: it would
open a second, parallel auth path plus CSRF exposure this project does not
want to carry. Every subsequent read against the API happens from
``static/app.js``, a plain Bearer call against the EXISTING ``/v1/*``
endpoints — this router adds zero new business-logic endpoints beyond the
shell itself and ``GET /v1/whoami`` (``api/routers/me.py``).

Kill switch ``OOTILS_UI_ENABLED``, DEFAULT OFF (🎯 pilot decision, ADR-036 —
the demo flips it). Unlike the per-request 503 kill switches elsewhere
(``outcomes.py``, ``scenarios.py``'s ``/compare``), this one is evaluated
ONCE, at ``create_app()`` time (mirrors ``api/app.py``'s ``_api_docs_enabled()``
gating ``docs_url``) — there is no auth dependency on this route to order a
per-request check after, and gating registration itself means a disabled
window is a clean 404: the route and its static mount simply do not exist,
rather than existing and merely refusing.

Mount ordering caveat: FastAPI's ``Router.include_router()`` does not copy
``Mount`` routes from a sub-``APIRouter`` onto the parent app (a ``Mount``
added via ``APIRouter.mount()`` never reaches ``app.routes`` through
``include_router`` — verified empirically against fastapi 0.128). Static
files must therefore be mounted directly on the ``FastAPI`` app instance —
``mount_ui_static`` takes the app, and ``include_ui`` (called from
``api/app.py``) is the single place that wires both the router and the mount
together, gated by the same kill switch.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ui"])

_TRUTHY = {"1", "true", "yes", "on"}

_UI_ROOT = Path(__file__).resolve().parents[1] / "ui"
TEMPLATES_DIR = _UI_ROOT / "templates"
STATIC_DIR = _UI_ROOT / "static"

# autoescape defaults to True in starlette's Jinja2Templates (env_options
# setdefault) — not overridden here, so it stays on.
_templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def ui_enabled() -> bool:
    """Kill switch, DEFAULT OFF. Read once by ``include_ui`` at
    ``create_app()`` time — see the module docstring for why this is
    startup-gated rather than a per-request dependency."""
    return os.environ.get("OOTILS_UI_ENABLED", "0").strip().lower() in _TRUTHY


@router.get("/ui", response_class=HTMLResponse, include_in_schema=False)
async def ui_console(request: Request) -> HTMLResponse:
    """The console shell. Zero business data in the rendered context — the
    page fetches everything client-side, authenticated with the operator's
    own Bearer token, via ``static/app.js``."""
    return _templates.TemplateResponse(request, "console.html", {})


def mount_ui_static(app: FastAPI) -> None:
    """Mount ``/ui/static`` onto the FastAPI app. Must be called on the app
    directly — see the module docstring's mount-ordering caveat."""
    app.mount("/ui/static", StaticFiles(directory=str(STATIC_DIR)), name="ui-static")


def include_ui(app: FastAPI) -> None:
    """Register the ``/ui`` shell and its static assets, gated by
    ``ui_enabled()``. Disabled → neither the route nor the mount exist
    (clean 404), never a route that exists and merely refuses."""
    if not ui_enabled():
        logger.info("ui.disabled")
        return
    app.include_router(router)
    mount_ui_static(app)
    logger.info("ui.enabled path=/ui static=/ui/static")
