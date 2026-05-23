"""
Unit tests for ATP Engine — no-DB cases only.

DB-backed tests live in ``tests/integration/test_atp_engine_integration.py``
(per CLAUDE.md: "Tests run against real Postgres, no mocks"). This file keeps
only the cases that exercise pure-Python behaviour of ``ATPEngine`` /
``ATPConfig`` without touching the database.
"""

import unittest
from datetime import date
from decimal import Decimal
from uuid import uuid4

from ootils_core.atp.engine import ATPEngine
from ootils_core.atp.models import ATPConfig


class TestATPEngineNoDatabase(unittest.TestCase):
    """Engine behaviour when no database connection is wired."""

    def test_no_connection_raises_error(self):
        """calculate() must raise ValueError without a DB connection."""
        engine = ATPEngine(db_conn=None)

        with self.assertRaises(ValueError) as context:
            engine.calculate(uuid4(), uuid4(), Decimal("10"), date.today())

        self.assertIn("Database connection not set", str(context.exception))

    def test_connection_property_initially_none(self):
        """A freshly constructed engine has connection=None."""
        engine = ATPEngine(db_conn=None)
        self.assertIsNone(engine.connection)

    def test_connection_setter_accepts_any_object(self):
        """The setter assigns the value verbatim — no validation."""
        engine = ATPEngine(db_conn=None)
        sentinel = object()
        engine.connection = sentinel  # type: ignore[assignment]
        self.assertIs(engine.connection, sentinel)


class TestATPConfigDefaults(unittest.TestCase):
    """ATPConfig dataclass defaults — pure dataclass behaviour."""

    def test_default_config_values(self):
        config = ATPConfig()
        self.assertEqual(config.time_grain, "daily")
        self.assertEqual(config.netting_rule, "fifo")
        self.assertTrue(config.consume_on_hand_first)
        self.assertTrue(config.respect_supply_priority)
        self.assertEqual(config.default_horizon_days, 365)

    def test_engine_uses_default_config_when_none(self):
        """Passing config=None falls back to ATPConfig() defaults."""
        engine = ATPEngine(db_conn=None, config=None)
        # Access the private config through the engine's behaviour:
        # the default horizon is exposed via _config.default_horizon_days.
        # We only assert it is an ATPConfig with the expected defaults.
        self.assertEqual(engine._config.default_horizon_days, 365)
        self.assertEqual(engine._config.time_grain, "daily")

    def test_engine_accepts_custom_config(self):
        custom = ATPConfig(default_horizon_days=30, netting_rule="lifo")
        engine = ATPEngine(db_conn=None, config=custom)
        self.assertEqual(engine._config.default_horizon_days, 30)
        self.assertEqual(engine._config.netting_rule, "lifo")


if __name__ == "__main__":
    unittest.main()
