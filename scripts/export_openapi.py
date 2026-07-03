#!/usr/bin/env python3
"""Export FastAPI OpenAPI spec to docs/openapi.json."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ootils_core.api.app import create_app

app = create_app()
spec = app.openapi()

out = Path(__file__).parent.parent / "docs" / "openapi.json"
# encoding='utf-8' explicit: write_text defaults to the locale encoding
# (cp1252 on Windows) and dies mid-write on any non-latin char in an API
# description — leaving a truncated/empty openapi.json behind.
out.write_text(json.dumps(spec, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"Written: {out} ({len(spec['paths'])} paths)")
