"""
singleton.py — module-level shared EngineClient for FastAPI routes.

# Why a singleton

The FastAPI app talks to the Rust engine over a single gRPC channel
shared across requests. Creating a fresh channel per request would:
- Pay TCP+HTTP/2 handshake (~5-10 ms) on every call.
- Churn `pg_stat_activity`-equivalent on the engine side.
- Defeat the point of grpc.Channel's connection multiplexing.

This module exposes a lazily-initialized, thread-safe `get_client()`
that returns the shared `EngineClient` configured from
`OOTILS_ENGINE_ADDR` (default 127.0.0.1:50051).

# When the engine isn't running

Routes that depend on the engine (e.g., `/v1/scenarios/sandbox`)
return 503 if `get_client()` fails to connect. We don't crash the
FastAPI process — other backends (SQL, Python kernel) keep working.

# Lifecycle

The client is lazily created on first `get_client()` call. There is
no explicit shutdown — the gRPC channel is reclaimed when the
Python process exits. FastAPI lifespan events could be wired to
call `close()` for clean shutdown, but the current `close()` is a
no-op effectively (channel close).
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Optional

from .client import EngineClient

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_client: Optional[EngineClient] = None


def get_engine_addr() -> str:
    """Resolve the engine address. Default = local dev convention."""
    return os.environ.get("OOTILS_ENGINE_ADDR", "127.0.0.1:50051")


def get_client() -> EngineClient:
    """Return the process-wide EngineClient singleton.

    Raises RuntimeError on connection failure (the caller — usually a
    FastAPI dependency — should translate to HTTP 503).
    """
    global _client
    if _client is not None:
        return _client
    with _lock:
        if _client is not None:
            return _client
        addr = get_engine_addr()
        try:
            client = EngineClient.connect(addr)
            # Smoke-test the connection by calling Health. This catches
            # "engine not running" early instead of letting the first
            # real RPC fail.
            client.health(timeout=2.0)
        except Exception as exc:
            logger.error(
                "EngineClient singleton failed to connect to %s: %s",
                addr,
                exc,
            )
            raise RuntimeError(
                f"engine unreachable at {addr}: {exc}. "
                "Is OOTILS_ENGINE=rust-svc + the engine process running?"
            ) from exc
        logger.info("EngineClient singleton connected to %s", addr)
        _client = client
        return _client


def close_client() -> None:
    """Shut down the singleton — useful for tests + clean teardown."""
    global _client
    with _lock:
        if _client is not None:
            try:
                _client.close()
            except Exception:  # noqa: BLE001
                pass
            _client = None
