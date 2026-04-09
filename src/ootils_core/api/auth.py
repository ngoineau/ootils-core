"""
auth.py — Bearer token authentication for Ootils Core API.

Token is read from env var OOTILS_API_TOKEN.
The server FAILS TO START if the variable is not set (fail-closed, no default).
Returns HTTP 401 if the token is absent or invalid.
"""
from __future__ import annotations

import hmac
import logging
import os

from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger(__name__)

_bearer = HTTPBearer(auto_error=False)


def _expected_token() -> str:
    """
    Return the expected API token from the environment.

    Raises RuntimeError at startup if OOTILS_API_TOKEN is not set,
    so the server fails loudly rather than silently accepting 'dev-token'.
    """
    token = os.environ.get("OOTILS_API_TOKEN")
    if not token:
        raise RuntimeError(
            "OOTILS_API_TOKEN environment variable is not set. "
            "The API cannot start without an explicit token — "
            "set OOTILS_API_TOKEN to a strong secret before launching."
        )
    return token


async def require_auth(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer),
) -> str:
    """
    FastAPI dependency — validates Bearer token.
    Raises HTTP 401 if missing or invalid.
    Returns the token string on success.

    Uses hmac.compare_digest to prevent timing-attack token enumeration.
    """
    if credentials is None:
        logger.warning("auth.missing_token")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    expected = _expected_token()
    # hmac.compare_digest prevents timing-based token enumeration
    if not hmac.compare_digest(credentials.credentials, expected):
        logger.warning("auth.invalid_token")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return credentials.credentials
