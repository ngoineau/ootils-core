#!/usr/bin/env python3
"""
export_openapi.py — Export the Ootils Core API OpenAPI schema to docs/api/openapi.json.

Usage:
    python3 scripts/export_openapi.py
"""
import json
import sys
from pathlib import Path

# Ensure src/ is on the path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

from ootils_core.api.app import app  # noqa: E402

OUTPUT = ROOT / "docs" / "api" / "openapi.json"
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

schema = app.openapi()
OUTPUT.write_text(json.dumps(schema, indent=2, default=str), encoding="utf-8")
print(f"✓ OpenAPI schema exported to {OUTPUT}")
