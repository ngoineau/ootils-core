"""
tests/test_l3_webhook.py — PURE unit tests (no PostgreSQL, no real network) for
the best-effort L3+ outbound webhook (PROD-QW,
src/ootils_core/notifications/l3_webhook.py).

The webhook is the fleet's "the exception finds you" ping: when the reschedule
watcher emits a genuinely-new L3+ DRAFT (a CANCEL — the first watcher-emitted
L3, human-only by the #341 state machine), it POSTs a minimal, secret-free
payload to an operator-supplied endpoint. Everything here is exercised WITHOUT a
real network: httpx.post is patched, so timeout / 5xx / exception paths are all
driven deterministically. The single hard contract under test:

  * BEST-EFFORT — post_l3_pending / notify_l3_pending MUST NEVER raise (a
    missing endpoint, a timeout, a 5xx or a hard exception are swallowed and
    logged), so the caller can invoke them around a committed DB transaction.
  * GATED — notify_l3_pending fires ONLY at L3+; below L3 it never touches the
    network.
  * NO SECRET — the payload carries the recommendation id + action + level +
    external ids + a human message, and NOTHING that looks like a credential.
"""
from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import httpx
import pytest

import ootils_core.notifications.l3_webhook as l3
from ootils_core.notifications.l3_webhook import (
    L3PendingPayload,
    L3_TIMEOUT_SECONDS,
    WEBHOOK_URL_ENV,
    build_l3_payload,
    decision_level_rank,
    is_l3_or_higher,
    notify_l3_pending,
    post_l3_pending,
    resolve_webhook_url,
)


# ===========================================================================
# 1. decision_level_rank / is_l3_or_higher — the gate predicate
# ===========================================================================


class TestDecisionLevelRank:
    @pytest.mark.parametrize(
        "level,expected",
        [("L0", 0), ("L1", 1), ("L2", 2), ("L3", 3), ("L4", 4), ("L10", 10)],
    )
    def test_well_formed_levels_parse_to_their_number(self, level, expected):
        assert decision_level_rank(level) == expected

    def test_rank_is_case_insensitive_and_strips(self):
        assert decision_level_rank("  l3 ") == 3

    @pytest.mark.parametrize("bad", ["", "L", "LX", "3", "foo", "L-1", "L 3", "level3"])
    def test_malformed_levels_rank_minus_one(self, bad):
        # A malformed level must NOT crash the notify path — it degrades to -1,
        # which fails the >= L3 test and stays silent.
        assert decision_level_rank(bad) == -1


class TestIsL3OrHigher:
    @pytest.mark.parametrize("level", ["L0", "L1", "L2"])
    def test_below_l3_is_false(self, level):
        assert is_l3_or_higher(level) is False

    @pytest.mark.parametrize("level", ["L3", "L4", "L10"])
    def test_l3_and_above_is_true(self, level):
        assert is_l3_or_higher(level) is True

    @pytest.mark.parametrize("bad", ["", "L", "LX", "foo", "L-1"])
    def test_malformed_is_false(self, bad):
        assert is_l3_or_higher(bad) is False


# ===========================================================================
# 2. resolve_webhook_url — env / param / empty precedence
# ===========================================================================


class TestResolveWebhookUrl:
    def test_explicit_param_wins_over_env(self, monkeypatch):
        monkeypatch.setenv(WEBHOOK_URL_ENV, "https://env.example/hook")
        assert resolve_webhook_url("https://param.example/hook") == "https://param.example/hook"

    def test_env_used_when_no_param(self, monkeypatch):
        monkeypatch.setenv(WEBHOOK_URL_ENV, "https://env.example/hook")
        assert resolve_webhook_url() == "https://env.example/hook"

    def test_unset_env_and_no_param_is_none(self, monkeypatch):
        monkeypatch.delenv(WEBHOOK_URL_ENV, raising=False)
        assert resolve_webhook_url() is None

    def test_empty_param_resolves_to_none(self, monkeypatch):
        monkeypatch.delenv(WEBHOOK_URL_ENV, raising=False)
        assert resolve_webhook_url("   ") is None

    def test_whitespace_env_resolves_to_none(self, monkeypatch):
        monkeypatch.setenv(WEBHOOK_URL_ENV, "   ")
        assert resolve_webhook_url() is None

    def test_param_is_stripped(self, monkeypatch):
        monkeypatch.delenv(WEBHOOK_URL_ENV, raising=False)
        assert resolve_webhook_url("  https://x.example/h  ") == "https://x.example/h"


# ===========================================================================
# 3. post_l3_pending — best-effort POST, every branch (httpx patched)
# ===========================================================================


def _payload(**over) -> L3PendingPayload:
    base = dict(
        recommendation_id=uuid4(),
        action="CANCEL",
        decision_level="L3",
        message="CANCEL firm receipt X. L3 human approval required.",
    )
    base.update(over)
    return build_l3_payload(**base)


class TestPostL3Pending:
    def test_emits_when_url_configured_and_returns_true(self, monkeypatch):
        fake_post = MagicMock(return_value=MagicMock(status_code=200))
        monkeypatch.setattr(l3.httpx, "post", fake_post)
        monkeypatch.setattr(l3, "_HTTPX_AVAILABLE", True)

        p = _payload()
        assert post_l3_pending(p, url="https://x.example/hook") is True
        # Exactly one attempt, no retry, with the documented timeout + JSON body.
        assert fake_post.call_count == 1
        args, kwargs = fake_post.call_args
        assert args[0] == "https://x.example/hook"
        assert kwargs["timeout"] == L3_TIMEOUT_SECONDS
        assert kwargs["json"]["action"] == "CANCEL"
        assert kwargs["json"]["decision_level"] == "L3"

    def test_silent_no_op_when_no_url(self, monkeypatch):
        monkeypatch.delenv(WEBHOOK_URL_ENV, raising=False)
        fake_post = MagicMock()
        monkeypatch.setattr(l3.httpx, "post", fake_post)

        # No URL configured (param None, env unset) → opt-out, never touches
        # the network, returns False.
        assert post_l3_pending(_payload(), url=None) is False
        fake_post.assert_not_called()

    def test_returns_false_and_does_not_post_when_httpx_unavailable(self, monkeypatch):
        fake_post = MagicMock()
        monkeypatch.setattr(l3.httpx, "post", fake_post)
        monkeypatch.setattr(l3, "_HTTPX_AVAILABLE", False)

        assert post_l3_pending(_payload(), url="https://x.example/hook") is False
        fake_post.assert_not_called()

    def test_swallows_timeout_without_raising(self, monkeypatch):
        fake_post = MagicMock(side_effect=httpx.TimeoutException("slow"))
        monkeypatch.setattr(l3.httpx, "post", fake_post)
        monkeypatch.setattr(l3, "_HTTPX_AVAILABLE", True)

        # MUST NOT raise — a timeout is swallowed and logged; returns False.
        assert post_l3_pending(_payload(), url="https://x.example/hook") is False

    def test_swallows_generic_exception_without_raising(self, monkeypatch):
        fake_post = MagicMock(side_effect=RuntimeError("connection refused"))
        monkeypatch.setattr(l3.httpx, "post", fake_post)
        monkeypatch.setattr(l3, "_HTTPX_AVAILABLE", True)

        assert post_l3_pending(_payload(), url="https://x.example/hook") is False

    def test_5xx_is_not_an_exception_and_does_not_raise(self, monkeypatch):
        fake_post = MagicMock(return_value=MagicMock(status_code=503))
        monkeypatch.setattr(l3.httpx, "post", fake_post)
        monkeypatch.setattr(l3, "_HTTPX_AVAILABLE", True)

        # A 5xx response is a completed request (it was sent) — the contract is
        # "never raise", and post_l3_pending returns True (sent) while logging
        # the non-2xx. The point of the test: no exception escapes.
        assert post_l3_pending(_payload(), url="https://x.example/hook") is True
        assert fake_post.call_count == 1

    def test_uses_env_url_when_param_absent(self, monkeypatch):
        fake_post = MagicMock(return_value=MagicMock(status_code=204))
        monkeypatch.setattr(l3.httpx, "post", fake_post)
        monkeypatch.setattr(l3, "_HTTPX_AVAILABLE", True)
        monkeypatch.setenv(WEBHOOK_URL_ENV, "https://env.example/hook")

        assert post_l3_pending(_payload()) is True
        assert fake_post.call_args[0][0] == "https://env.example/hook"


# ===========================================================================
# 4. notify_l3_pending — gate + build + post in one call
# ===========================================================================


class TestNotifyL3Pending:
    @pytest.mark.parametrize("level", ["L0", "L1", "L2"])
    def test_below_l3_posts_nothing(self, monkeypatch, level):
        fake_post = MagicMock(return_value=MagicMock(status_code=200))
        monkeypatch.setattr(l3.httpx, "post", fake_post)
        monkeypatch.setattr(l3, "_HTTPX_AVAILABLE", True)
        monkeypatch.setenv(WEBHOOK_URL_ENV, "https://x.example/hook")

        result = notify_l3_pending(
            recommendation_id=uuid4(),
            action="RESCHEDULE_OUT",
            decision_level=level,
            message="reschedule",
        )
        assert result is False
        fake_post.assert_not_called()  # gated below L3 → no network at all

    def test_l3_with_url_posts(self, monkeypatch):
        fake_post = MagicMock(return_value=MagicMock(status_code=200))
        monkeypatch.setattr(l3.httpx, "post", fake_post)
        monkeypatch.setattr(l3, "_HTTPX_AVAILABLE", True)

        result = notify_l3_pending(
            recommendation_id=uuid4(),
            action="CANCEL",
            decision_level="L3",
            message="CANCEL firm receipt",
            item_external_id="ITM-1",
            location_external_id="LOC-1",
            url="https://x.example/hook",
        )
        assert result is True
        assert fake_post.call_count == 1
        body = fake_post.call_args.kwargs["json"]
        assert body["item_external_id"] == "ITM-1"
        assert body["location_external_id"] == "LOC-1"

    def test_l3_without_url_is_silent_false(self, monkeypatch):
        monkeypatch.delenv(WEBHOOK_URL_ENV, raising=False)
        fake_post = MagicMock()
        monkeypatch.setattr(l3.httpx, "post", fake_post)
        monkeypatch.setattr(l3, "_HTTPX_AVAILABLE", True)

        result = notify_l3_pending(
            recommendation_id=uuid4(),
            action="CANCEL",
            decision_level="L3",
            message="CANCEL",
        )
        assert result is False
        fake_post.assert_not_called()


# ===========================================================================
# 5. Payload carries NO secret (assert on the exact key set)
# ===========================================================================


class TestPayloadHasNoSecret:
    def test_payload_keys_are_exactly_the_non_secret_fields(self):
        p = build_l3_payload(
            recommendation_id=uuid4(),
            action="CANCEL",
            decision_level="L3",
            message="CANCEL firm receipt",
            item_external_id="ITM-1",
            location_external_id="LOC-1",
        )
        body = p.model_dump(mode="json")
        assert set(body.keys()) == {
            "event",
            "recommendation_id",
            "action",
            "decision_level",
            "item_external_id",
            "location_external_id",
            "message",
        }
        # No credential-shaped key leaks into the body.
        lowered = {k.lower() for k in body.keys()}
        for forbidden in ("token", "token_hash", "secret", "authorization", "password", "api_key"):
            assert forbidden not in lowered

    def test_no_payload_value_is_a_bearer_or_token(self):
        p = build_l3_payload(
            recommendation_id=uuid4(),
            action="CANCEL",
            decision_level="L3",
            message="a human-readable message with no credential",
        )
        for value in p.model_dump(mode="json").values():
            text = str(value).lower()
            assert "bearer" not in text
            assert "ootk_" not in text

    def test_event_defaults_to_the_stable_type(self):
        p = _payload()
        assert p.event == "l3_recommendation_pending"
