"""
me.py — GET /v1/whoami: the authenticated caller's own identity.

Chantier EXP-1 PR1 (ADR-036, issue #445). There is exactly ONE auth mechanism
in this codebase (``resolve_principal`` / ``require_scope``, ``api/auth.py``);
this endpoint does not add a second one. It lets any authenticated client
(the human console at ``GET /ui``, or an agent) introspect its OWN Principal —
``actor_kind`` and the ``scopes`` capability set, plus a non-secret
``token_prefix`` for audit correlation — without ever echoing the token
itself back (``Principal`` never carries the raw secret to begin with; only
its SHA-256 lives server-side).

Consumed COSMETICALLY by the EXP-1 window (``api/routers/ui.py``) to decide
whether to render action affordances (e.g. approve, in a later PR) — this
endpoint changes nothing about authorization. The server remains the sole
enforcer via ``require_scope`` on every actual write; a client hiding a
button it can't use is a UX convenience, never a security boundary.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from ootils_core.api.auth import Principal, require_scope

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["me"])


class WhoAmIOut(BaseModel):
    """The caller's own Principal. NEVER the token — see module docstring.

    ``token_prefix`` is None for the legacy single token: its
    ``request.state.client_id`` is the sentinel ``"global_token"``, not a
    real minted-token prefix, so it is deliberately not surfaced as one."""

    name: str
    actor_kind: str
    scopes: list[str]
    is_legacy: bool
    token_prefix: Optional[str] = None


@router.get("/whoami", response_model=WhoAmIOut)
def whoami(
    request: Request,
    principal: Principal = Depends(require_scope("read")),
) -> WhoAmIOut:
    """Return the authenticated Principal resolved for this request."""
    client_id = getattr(request.state, "client_id", None)
    token_prefix = client_id if (client_id and not principal.is_legacy) else None
    return WhoAmIOut(
        name=principal.name,
        actor_kind=principal.actor_kind,
        scopes=sorted(principal.scopes),
        is_legacy=principal.is_legacy,
        token_prefix=token_prefix,
    )
