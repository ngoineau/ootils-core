"""
tests/integration/test_impact_scorer_bom_integration.py — la remontée BOM de
l'impact scorer DQ contre le VRAI schéma (fix PR-4a).

Avant ce fix, `_get_finished_goods_via_bom` (engine/dq/agent/impact_scorer.py)
interrogeait une table `bom_components` qui n'existe dans AUCUNE migration —
chaque appel levait psycopg.errors.UndefinedTable, avalé plus haut ou pas
selon le chemin : la catégorie 4 (impact supply chain) ne remontait JAMAIS
la BOM. Le fix réécrit la requête sur le schéma réel (migrations 008/013) :
bom_headers (parent + version, status='active') JOIN bom_lines
(component_item_id, active=TRUE).

Ces tests appellent la fonction corrigée directement avec une connexion
dict_row réelle, sur une mini-BOM semée via l'API (pattern
test_bom_obsolete_integration.py) :
  - composant → produit fini résolu via bom_lines, sans UndefinedTable ;
  - dédup : 2 composants du même parent → UN SEUL produit fini ;
  - filtres du fix : bh.status='active' ET bl.active=TRUE excluent bien ;
  - transitivité : la boucle remonte niveau par niveau (comp → sub → top).

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

PREFIX = f"IMPS-{uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB
    (same pattern as test_bom_obsolete_integration.py)."""
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


@pytest.fixture(scope="module")
def seed(api_client, request, migrated_db):
    """Mini-BOM via l'API :
      - PARENT (produit fini) ← COMP-A + COMP-B          (le cas nominal)
      - TOP ← SUB ← COMP-ML                              (transitivité)
      - PARENT2 ← COMP-C                                 (filtres actif/inactif)
    Neutralisation par désactivation, jamais de delete-cascade."""
    items = [
        {"external_id": _ext("PARENT"), "name": "Impact scorer parent",
         "item_type": "finished_good", "uom": "EA", "status": "active"},
        {"external_id": _ext("COMP-A"), "name": "Composant A",
         "item_type": "component", "uom": "EA", "status": "active"},
        {"external_id": _ext("COMP-B"), "name": "Composant B",
         "item_type": "component", "uom": "EA", "status": "active"},
        {"external_id": _ext("TOP"), "name": "Produit fini multi-niveau",
         "item_type": "finished_good", "uom": "EA", "status": "active"},
        {"external_id": _ext("SUB"), "name": "Sous-ensemble",
         "item_type": "semi_finished", "uom": "EA", "status": "active"},
        {"external_id": _ext("COMP-ML"), "name": "Composant feuille multi-niveau",
         "item_type": "component", "uom": "EA", "status": "active"},
        {"external_id": _ext("PARENT2"), "name": "Parent pour filtres",
         "item_type": "finished_good", "uom": "EA", "status": "active"},
        {"external_id": _ext("COMP-C"), "name": "Composant filtres",
         "item_type": "component", "uom": "EA", "status": "active"},
    ]
    resp = api_client.post("/v1/ingest/items", json={"items": items}, headers=AUTH)
    assert resp.status_code == 200, resp.text

    for parent, comps in [
        (_ext("PARENT"), [_ext("COMP-A"), _ext("COMP-B")]),
        (_ext("SUB"), [_ext("COMP-ML")]),
        (_ext("TOP"), [_ext("SUB")]),
        (_ext("PARENT2"), [_ext("COMP-C")]),
    ]:
        resp = api_client.post(
            "/v1/ingest/bom", json=_bom_payload(parent, comps), headers=AUTH
        )
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
        "comp_a": _ext("COMP-A"),
        "comp_b": _ext("COMP-B"),
        "top": _ext("TOP"),
        "sub": _ext("SUB"),
        "comp_ml": _ext("COMP-ML"),
        "parent2": _ext("PARENT2"),
        "comp_c": _ext("COMP-C"),
    }


@pytest.fixture()
def db_conn(migrated_db):
    """Connexion dict_row réelle — le type que score_issues reçoit en prod
    (DictRowConnection). La fonction testée est read-only ; rollback par
    sécurité en sortie."""
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        yield conn
        conn.rollback()


class TestImpactScorerBomTraversal:
    def test_component_resolves_finished_good_without_undefined_table(
        self, seed, db_conn
    ):
        """LE test de régression : la requête vit sur bom_headers/bom_lines,
        plus sur la table fantôme bom_components."""
        import psycopg
        from ootils_core.engine.dq.agent.impact_scorer import (
            _get_finished_goods_via_bom,
        )

        try:
            fgs = _get_finished_goods_via_bom(db_conn, [seed["comp_a"]])
        except psycopg.errors.UndefinedTable as e:  # pragma: no cover — regression guard
            pytest.fail(f"la requête bom_components morte est revenue : {e}")

        assert fgs == [seed["parent"]]

    def test_two_components_same_parent_dedup(self, seed, db_conn):
        """DISTINCT + set : 2 composants du même parent → UN produit fini."""
        from ootils_core.engine.dq.agent.impact_scorer import (
            _get_finished_goods_via_bom,
        )

        fgs = _get_finished_goods_via_bom(db_conn, [seed["comp_a"], seed["comp_b"]])
        assert fgs == [seed["parent"]]

    def test_unknown_external_id_returns_empty(self, seed, db_conn):
        from ootils_core.engine.dq.agent.impact_scorer import (
            _get_finished_goods_via_bom,
        )

        assert _get_finished_goods_via_bom(db_conn, [_ext("NEVER-EXISTED")]) == []
        assert _get_finished_goods_via_bom(db_conn, []) == []

    def test_multilevel_traversal_climbs_to_top(self, seed, db_conn):
        """La boucle remonte niveau par niveau : COMP-ML → SUB → TOP.
        Les intermédiaires (semi_finished) sont inclus — le contrat est
        « tout l'amont », pas seulement les feuilles finies."""
        from ootils_core.engine.dq.agent.impact_scorer import (
            _get_finished_goods_via_bom,
        )

        fgs = _get_finished_goods_via_bom(db_conn, [seed["comp_ml"]])
        assert set(fgs) == {seed["sub"], seed["top"]}

    def test_inactive_line_and_header_filters(self, seed, db_conn, migrated_db):
        """Les deux filtres du fix, sur une BOM dédiée (PARENT2 ← COMP-C) :
        bl.active=FALSE puis bh.status='inactive' excluent chacun la
        remontée. Désactivations locales au test (même mécanisme que le
        finalizer) — jamais de delete."""
        import psycopg
        from ootils_core.engine.dq.agent.impact_scorer import (
            _get_finished_goods_via_bom,
        )

        # État initial : COMP-C remonte vers PARENT2.
        assert _get_finished_goods_via_bom(db_conn, [seed["comp_c"]]) == [
            seed["parent2"]
        ]

        with psycopg.connect(migrated_db, autocommit=True) as admin:
            # 1. Ligne désactivée → plus de remontée.
            admin.execute(
                """
                UPDATE bom_lines SET active = FALSE
                WHERE component_item_id = (
                    SELECT item_id FROM items WHERE external_id = %s
                )
                """,
                (seed["comp_c"],),
            )
        assert _get_finished_goods_via_bom(db_conn, [seed["comp_c"]]) == []

        with psycopg.connect(migrated_db, autocommit=True) as admin:
            # 2. Ligne restaurée mais header inactif → toujours rien.
            admin.execute(
                """
                UPDATE bom_lines SET active = TRUE
                WHERE component_item_id = (
                    SELECT item_id FROM items WHERE external_id = %s
                )
                """,
                (seed["comp_c"],),
            )
            admin.execute(
                """
                UPDATE bom_headers SET status = 'inactive'
                WHERE parent_item_id = (
                    SELECT item_id FROM items WHERE external_id = %s
                )
                """,
                (seed["parent2"],),
            )
        assert _get_finished_goods_via_bom(db_conn, [seed["comp_c"]]) == []
