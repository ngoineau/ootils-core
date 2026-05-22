"""
examples/simple_propagation.py — End-to-end demo of an Ootils Core propagation.

Drives the same chain an LLM agent uses via `ootils_core.tools.agent_tools`:
ingest → trigger propagation → enumerate shortages → explain one.

Usage:
    # 1. Start the stack and seed demo data first
    docker compose up -d
    docker compose exec api python scripts/seed_demo_data.py

    # 2. Run this script against the local API
    OOTILS_API_TOKEN=dev-token python examples/simple_propagation.py

Or against a different host:
    OOTILS_API_BASE=https://api.example.com \\
        OOTILS_API_TOKEN=<token> \\
        python examples/simple_propagation.py

The script uses only the standard library + `httpx` (which is in the
dev dependencies) so it does not require importing ootils_core itself.
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any

import httpx

BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"
API_BASE = os.environ.get("OOTILS_API_BASE", "http://localhost:8000")
API_TOKEN = os.environ.get("OOTILS_API_TOKEN", "dev-token")

HEADERS = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}


def _check_health(client: httpx.Client) -> None:
    r = client.get(f"{API_BASE}/health", timeout=5.0)
    r.raise_for_status()
    body = r.json()
    print(f"[health] {body}")


def _trigger_propagation(client: httpx.Client, scenario_id: str) -> dict[str, Any]:
    """Trigger a full recompute for a scenario and return the calc-run payload."""
    print(f"[step 1] POST /v1/calc/run?scenario_id={scenario_id[:8]}…")
    r = client.post(
        f"{API_BASE}/v1/calc/run",
        headers=HEADERS,
        params={"scenario_id": scenario_id},
        timeout=120.0,
    )
    r.raise_for_status()
    payload = r.json()
    status = payload.get("status", "?")
    nodes = payload.get("nodes_recalculated", 0)
    print(f"          status={status} nodes_recalculated={nodes}")
    return payload


def _list_shortages(client: httpx.Client, scenario_id: str) -> list[dict[str, Any]]:
    print(f"[step 2] GET  /v1/issues?scenario_id={scenario_id[:8]}…")
    r = client.get(
        f"{API_BASE}/v1/issues",
        headers=HEADERS,
        params={"scenario_id": scenario_id},
        timeout=30.0,
    )
    r.raise_for_status()
    issues = r.json().get("issues", [])
    print(f"          {len(issues)} shortage(s) detected")
    for i in issues[:5]:
        node = i.get("pi_node_id", "?")[:8]
        qty = i.get("shortage_qty", 0)
        date = i.get("shortage_date", "?")
        score = i.get("severity_score", 0)
        print(f"            - node={node}…  qty={qty}  date={date}  severity={score}")
    if len(issues) > 5:
        print(f"            (… {len(issues) - 5} more)")
    return issues


def _explain(client: httpx.Client, shortage_id: str) -> None:
    short = shortage_id[:8]
    print(f"[step 3] GET  /v1/explain/{short}…")
    r = client.get(
        f"{API_BASE}/v1/explain/{shortage_id}",
        headers=HEADERS,
        timeout=30.0,
    )
    if r.status_code == 404:
        print(f"          (no explanation persisted for shortage {short}…)")
        return
    r.raise_for_status()
    explanation = r.json()
    print(json.dumps(explanation, indent=2)[:600])


def main() -> int:
    scenario_id = os.environ.get("OOTILS_SCENARIO_ID", BASELINE_SCENARIO_ID)

    with httpx.Client() as client:
        try:
            _check_health(client)
        except httpx.HTTPError as exc:
            print(f"FATAL: cannot reach {API_BASE}/health → {exc}")
            print("Hint: docker compose up -d && docker compose exec api python scripts/seed_demo_data.py")
            return 2

        try:
            _trigger_propagation(client, scenario_id)
        except httpx.HTTPStatusError as exc:
            print(f"FATAL: propagation failed: {exc.response.status_code} {exc.response.text[:200]}")
            return 1

        shortages = _list_shortages(client, scenario_id)
        if shortages:
            _explain(client, shortages[0]["shortage_id"])
        else:
            print("[step 3] (no shortages — try seeding demo data first)")

    print("\nDone. This is the same chain an LLM agent drives via "
          "ootils_core.tools.agent_tools.trigger_recalculation + get_active_issues.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
