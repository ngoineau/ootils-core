"""
types.py — shared type aliases for psycopg connections.

Every runtime connection in this codebase is configured with
``row_factory=dict_row`` (see connection.py). Plain ``psycopg.Connection``
annotations tell mypy nothing about the row factory, so it falls back to
assuming tuple rows and rejects ``row["col"]`` access across ~275 call
sites. ``DictRowConnection`` documents the runtime reality in the type
system so mypy can check dict-style row access correctly.
"""
from __future__ import annotations

from typing import Any

import psycopg

DictRowConnection = psycopg.Connection[dict[str, Any]]
