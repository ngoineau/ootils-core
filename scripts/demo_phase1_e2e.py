#!/usr/bin/env python3
"""
Demo Phase 1 E2E API flow: Forecast -> MPS -> MRP planned supply -> CRP -> ATP.

Runs against the DATABASE_URL database using the real FastAPI app/TestClient and
PostgreSQL. Use a disposable migrated test database; the script inserts demo rows.
"""
from __future__ import annotations

import argparse
import json
import os
from decimal import Decimal

from ootils_core.demo.phase1 import run_phase1_demo


def _decimal_default(value):
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Missing {name}. Set it to a disposable PostgreSQL test database.")
    return value


def run_demo() -> dict:
    database_url = _require_env("DATABASE_URL")
    token = os.environ.setdefault("OOTILS_API_TOKEN", "phase1-demo-token")
    return run_phase1_demo(database_url, token)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Ootils Phase 1 E2E demo flow.")
    parser.add_argument("--json", action="store_true", help="Print compact JSON only.")
    args = parser.parse_args()

    result = run_demo()
    if args.json:
        print(json.dumps(result, default=_decimal_default, separators=(",", ":")))
        return

    print(json.dumps(result, default=_decimal_default, indent=2))


if __name__ == "__main__":
    main()
