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
out.write_text(json.dumps(spec, indent=2, ensure_ascii=False))
print(f"Written: {out} ({len(spec['paths'])} paths)")
