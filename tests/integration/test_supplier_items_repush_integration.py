"""
tests/integration/test_supplier_items_repush_integration.py — Le test qui
manquait au premier chargement réel (2026-07-18) : le bras UPDATE de
POST /v1/ingest/supplier-items.

Bug d'origine : le bras UPDATE stampe `updated_at = now()` mais aucune
migration n'avait jamais créé la colonne (007 crée supplier_items sans).
Invisible sur base vide (bras INSERT seulement — la répétition générale
passait), fatal sur la base pilote où des liens préexistaient (re-push du
bundle → UndefinedColumn → 500 → fichier rejeté). Migration 082 pose la
colonne ; ce test exerce EXPLICITEMENT les deux bras :

  (a) premier push  → action='inserted', ligne créée ;
  (b) re-push mêmes clés, valeurs changées → action='updated', valeurs
      remplacées, updated_at stampé STRICTEMENT après le created_at (le
      point exact qui explosait avant 082) ;
  (c) idempotence 082 : ré-exécution du SQL de la migration = no-op.

Isolation : seeds via l'API réelle sous PREFIX unique, neutralisés par
DÉSACTIVATION (items obsoleted) — jamais de DELETE cascade (leçon #461).
supplier_items n'a pas de flag actif : les lignes de test restent, inertes
(aucun chemin métier ne les lit sans passer par items/suppliers actifs),
même politique que les lignes sans flag du test de rétractation.
"""
from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest

from .conftest import requires_db

pytestmark = requires_db

TOKEN = "integration-test-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}

PREFIX = f"SIRP-{uuid4().hex[:8]}"

MIGRATION_082 = (
    Path(__file__).resolve().parents[2]
    / "src" / "ootils_core" / "db" / "migrations"
    / "082_supplier_items_updated_at.sql"
)


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
    """Un item + un supplier via l'API réelle, neutralisés en fin de module."""
    resp = api_client.post(
        "/v1/ingest/items",
        json={"items": [{
            "external_id": _ext("ITEM"), "name": "Repush supplier-item test",
            "item_type": "component", "uom": "EA", "status": "active",
        }]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text

    resp = api_client.post(
        "/v1/ingest/suppliers",
        json={"suppliers": [{
            "external_id": _ext("SUP"), "name": "Repush supplier",
        }]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text

    def _neutralize():
        import psycopg
        with psycopg.connect(migrated_db, autocommit=True) as conn:
            conn.execute(
                "UPDATE items SET status = 'obsolete' WHERE external_id LIKE %s",
                (PREFIX + "%",),
            )

    request.addfinalizer(_neutralize)
    return {"item": _ext("ITEM"), "supplier": _ext("SUP")}


def _push(api_client, seed, lead_time, moq, cost):
    return api_client.post(
        "/v1/ingest/supplier-items",
        json={"supplier_items": [{
            "supplier_external_id": seed["supplier"],
            "item_external_id": seed["item"],
            "lead_time_days": lead_time,
            "moq": moq,
            "unit_cost": cost,
            "is_preferred": True,
            "currency": "EUR",
        }]},
        headers=AUTH,
    )


class TestSupplierItemsRepush:
    def test_first_push_inserts(self, api_client, seed):
        resp = _push(api_client, seed, lead_time=10, moq=100, cost=2.5)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["results"][0]["action"] == "inserted"

    def test_repush_updates_and_stamps_updated_at(self, api_client, seed, migrated_db):
        """LE bras qui explosait : re-push mêmes clés → UPDATE ... updated_at."""
        resp = _push(api_client, seed, lead_time=21, moq=250, cost=3.75)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["results"][0]["action"] == "updated"

        import psycopg
        from psycopg.rows import dict_row
        with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
            row = conn.execute(
                """
                SELECT si.lead_time_days, si.moq, si.unit_cost,
                       si.created_at, si.updated_at
                FROM supplier_items si
                JOIN suppliers s ON s.supplier_id = si.supplier_id
                JOIN items i ON i.item_id = si.item_id
                WHERE s.external_id = %s AND i.external_id = %s
                """,
                (seed["supplier"], seed["item"]),
            ).fetchone()
        assert row is not None
        assert row["lead_time_days"] == 21
        assert row["moq"] == 250
        assert float(row["unit_cost"]) == 3.75
        # Le point exact du bug : la colonne existe ET le bras UPDATE la stampe.
        assert row["updated_at"] is not None
        assert row["updated_at"] > row["created_at"]

    def test_migration_082_is_idempotent(self, migrated_db):
        """Ré-exécuter le SQL de 082 sur un schéma déjà à jour = no-op propre."""
        import psycopg
        sql = MIGRATION_082.read_text(encoding="utf-8")
        with psycopg.connect(migrated_db, autocommit=True) as conn:
            conn.execute(sql)
            conn.execute(sql)
            n = conn.execute(
                """
                SELECT COUNT(*) AS n FROM information_schema.columns
                WHERE table_name = 'supplier_items' AND column_name = 'updated_at'
                """
            ).fetchone()[0]
        assert n == 1
