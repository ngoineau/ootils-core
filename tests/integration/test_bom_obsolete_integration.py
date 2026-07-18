"""
tests/integration/test_bom_obsolete_integration.py — BOM et items obsolètes
(découvert au premier chargement réel, 2026-07-18).

Avant ce fix, `_resolve_item_id` (filtre `status != 'obsolete'`) s'appliquait
aussi au chemin d'INGESTION : toute nomenclature dont le parent OU un seul
composant était obsolète partait en 422 « not found » (message trompeur —
l'item existe) et le parent restait SANS AUCUNE structure — l'explosion MRP
ne voyait plus rien du tout, le pire des silences. Les données ERP réelles
référencent légitimement des composants obsolètes (historique d'ingénierie,
phase-out) et le bundle incluait ces items exprès pour que les références se
résolvent.

Nouveau contrat d'ingestion :
  (a) composant obsolète → BOM ACCEPTÉE, ligne chargée, warning explicite
      dans la réponse (« planning governed by item status ») ;
  (b) parent obsolète → BOM ACCEPTÉE, warning symétrique ;
  (c) item réellement inconnu → 422 inchangé (le vrai « not found ») ;
  (d) les endpoints de LECTURE/explosion gardent leur filtre (hors scope).

Isolation : seeds sous PREFIX unique via l'API réelle, neutralisés par
DÉSACTIVATION (items obsoleted, bom_headers status='inactive') — jamais de
DELETE cascade (leçon #461).
"""
from __future__ import annotations

import os
from uuid import uuid4

import pytest

from .conftest import requires_db

pytestmark = requires_db

TOKEN = "integration-test-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}

PREFIX = f"BOMO-{uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB
    (same pattern as test_ingest_retraction_integration.py)."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = TOKEN

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB
    from fastapi.testclient import TestClient

    app = create_app()

    def override_db():
        db = OotilsDB(migrated_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


def _ext(base: str) -> str:
    return f"{PREFIX}-{base}"


@pytest.fixture(scope="module")
def seed(api_client, request, migrated_db):
    """3 items : parent actif, composant actif, composant OBSOLÈTE + un
    parent obsolète pour le cas (b). Neutralisation par désactivation."""
    items = [
        {"external_id": _ext("PARENT"), "name": "BOM obsolete parent actif",
         "item_type": "finished_good", "uom": "EA", "status": "active"},
        {"external_id": _ext("COMP-OK"), "name": "Composant actif",
         "item_type": "component", "uom": "EA", "status": "active"},
        {"external_id": _ext("COMP-OBS"), "name": "Composant obsolete",
         "item_type": "component", "uom": "EA", "status": "obsolete"},
        {"external_id": _ext("PARENT-OBS"), "name": "Parent obsolete",
         "item_type": "finished_good", "uom": "EA", "status": "obsolete"},
    ]
    resp = api_client.post("/v1/ingest/items", json={"items": items}, headers=AUTH)
    assert resp.status_code == 200, resp.text

    def _neutralize():
        import psycopg
        with psycopg.connect(migrated_db, autocommit=True) as conn:
            conn.execute(
                """
                UPDATE bom_headers SET status = 'inactive'
                WHERE parent_item_id IN (
                    SELECT item_id FROM items WHERE external_id LIKE %s
                )
                """,
                (PREFIX + "%",),
            )
            conn.execute(
                "UPDATE items SET status = 'obsolete' WHERE external_id LIKE %s",
                (PREFIX + "%",),
            )

    request.addfinalizer(_neutralize)
    return {
        "parent": _ext("PARENT"),
        "comp_ok": _ext("COMP-OK"),
        "comp_obs": _ext("COMP-OBS"),
        "parent_obs": _ext("PARENT-OBS"),
    }


def _bom_payload(parent: str, components: list[str], version: str = "1.0") -> dict:
    return {
        "parent_external_id": parent,
        "bom_version": version,
        "effective_from": "2026-01-01",
        "components": [
            {"component_external_id": c, "quantity_per": 2.0, "uom": "EA"}
            for c in components
        ],
    }


class TestBomObsoleteReferences:
    def test_obsolete_component_loads_with_warning(self, api_client, seed, migrated_db):
        """(a) LE cas du premier chargement : composant obsolète → BOM chargée."""
        resp = api_client.post(
            "/v1/ingest/bom",
            json=_bom_payload(seed["parent"], [seed["comp_ok"], seed["comp_obs"]]),
            headers=AUTH,
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["status"] == "ok"
        assert body["components_imported"] == 2
        warned = [w for w in body["warnings"]
                  if w.get("component_external_id") == seed["comp_obs"]]
        assert len(warned) == 1
        assert "obsolete" in warned[0]["warning"]

        # La STRUCTURE est bien en base : 2 lignes actives, obsolète incluse.
        import psycopg
        from psycopg.rows import dict_row
        with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
            n = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM bom_lines bl
                JOIN bom_headers bh ON bh.bom_id = bl.bom_id
                JOIN items p ON p.item_id = bh.parent_item_id
                WHERE p.external_id = %s AND bl.active
                """,
                (seed["parent"],),
            ).fetchone()["n"]
        assert n == 2

    def test_obsolete_parent_loads_with_warning(self, api_client, seed):
        """(b) parent obsolète → structure historique chargée, warning."""
        resp = api_client.post(
            "/v1/ingest/bom",
            json=_bom_payload(seed["parent_obs"], [seed["comp_ok"]]),
            headers=AUTH,
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        warned = [w for w in body["warnings"]
                  if w.get("external_id") == seed["parent_obs"]]
        assert len(warned) == 1

    def test_truly_unknown_component_still_422(self, api_client, seed):
        """(c) le vrai « not found » reste un refus net."""
        resp = api_client.post(
            "/v1/ingest/bom",
            json=_bom_payload(seed["parent"],
                              [seed["comp_ok"], _ext("NEVER-EXISTED")],
                              version="2.0"),
            headers=AUTH,
        )
        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert any(_ext("NEVER-EXISTED") in str(d) for d in detail)
