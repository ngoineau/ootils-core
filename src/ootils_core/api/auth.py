"""
auth.py — Bearer token authentication for Ootils Core API.

Token is read from env var OOTILS_API_TOKEN (default: "dev-token").
Returns HTTP 401 if the token is absent or invalid.
"""
from __future__ import annotations

import logging
import os

from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger(__name__)

_bearer = HTTPBearer(auto_error=False)


def _expected_token() -> str:
    return os.environ.get("OOTILS_API_TOKEN", "dev-token")


async def require_auth(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer),
) -> str:
    """
    FastAPI dependency — validates Bearer token.
    Raises HTTP 401 if missing or invalid.
    Returns the token string on success.
    """
    if credentials is None:
        logger.warning("auth.missing_token")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if credentials.credentials != _expected_token():
        logger.warning("auth.invalid_token")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return credentials.credentials
