"""
l3_webhook.py — minimal outbound webhook for L3+ recommendations awaiting a
human (PROD-QW, North Star "the exception finds you", no UI).

WHY HERE (the hook point). The Decision Ladder reserves L3+ actions for a human
(engine/recommendation/state_machine.py: HUMAN_ONLY_TARGETS — a non-human actor
can never reach APPROVED/APPLIED). The fleet's first L3 emitter is the reschedule
watcher's CANCEL (agent_governance.decision_level('CANCEL') == 'L3'), and it is
BORN as a ``DRAFT`` row: there is no separate PENDING_APPROVAL status in this
state machine, so for an L3 recommendation the DRAFT state IS "awaiting human
approval" (an agent cannot self-approve it). The moment "the human must now act"
is therefore the EMISSION of a new L3+ DRAFT — that is the single point this
webhook fires from (the watcher calls it only for rows it actually inserted, so a
re-run on an unchanged plan sends nothing, consistent with #346's zero-new-rows
idempotence). Firing on a later API transition would be too late — a human would
already have looked at the row.

CONTRACT (deliberately minimal, V1):
  * BEST-EFFORT: a POST failure (unreachable endpoint, timeout, missing httpx)
    is swallowed and logged at WARNING — it never breaks the caller or the DB
    transaction that emitted the recommendation.
  * NO RETRY: one attempt, ``timeout=5`` s. The recommendation is already
    durably persisted; the webhook is a courtesy ping, not the source of truth.
  * ENV-ONLY, NO SECRET: the destination is ``OOTILS_WEBHOOK_L3_URL`` (unset =>
    silent no-op — the feature is opt-in, the pilot supplies the URL, e.g. a
    Slack incoming-webhook). The payload carries NO token and NO secret: only
    the recommendation id, action, decision level, the external item/location
    ids and a human-readable message.
"""
from __future__ import annotations

import logging
import os
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field

# httpx is a light dependency (dev extra). Import it lazily/defensively so
# importing this module never fails in an environment that omits it — the
# webhook simply degrades to a logged no-op there (same pattern as slowapi in
# api/app.py).
try:  # pragma: no cover - import guard
    import httpx

    _HTTPX_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only where httpx is absent
    _HTTPX_AVAILABLE = False

logger = logging.getLogger(__name__)

WEBHOOK_URL_ENV = "OOTILS_WEBHOOK_L3_URL"
L3_TIMEOUT_SECONDS = 5.0


class L3PendingPayload(BaseModel):
    """The minimal typed body POSTed when an L3+ recommendation awaits a human.

    No secret, no token: attribution is via the recommendation id and the
    external (source-system) item/location ids a human can act on."""

    event: str = Field(default="l3_recommendation_pending")
    recommendation_id: UUID
    action: str
    decision_level: str
    item_external_id: Optional[str] = None
    location_external_id: Optional[str] = None
    message: str


def decision_level_rank(decision_level: str) -> int:
    """Numeric rank of an ``L<n>`` decision level (L0->0 .. L4->4).

    Returns -1 for an unparseable value (defensive: a malformed level must not
    crash the notify path — it simply fails the >= L3 test and stays silent)."""
    text = decision_level.strip().upper()
    if len(text) >= 2 and text[0] == "L" and text[1:].isdigit():
        return int(text[1:])
    return -1


def is_l3_or_higher(decision_level: str) -> bool:
    """True when the decision level is L3 or above (the human-gated, webhook tier)."""
    return decision_level_rank(decision_level) >= 3


def resolve_webhook_url(url: Optional[str] = None) -> Optional[str]:
    """Return the configured webhook URL: the explicit ``url`` if given, else
    ``OOTILS_WEBHOOK_L3_URL``. An empty/whitespace value resolves to None (unset
    => opt-out)."""
    candidate = url if url is not None else os.environ.get(WEBHOOK_URL_ENV)
    if candidate is None:
        return None
    candidate = candidate.strip()
    return candidate or None


def build_l3_payload(
    *,
    recommendation_id: UUID,
    action: str,
    decision_level: str,
    message: str,
    item_external_id: Optional[str] = None,
    location_external_id: Optional[str] = None,
) -> L3PendingPayload:
    """Pure builder for the webhook body (no IO)."""
    return L3PendingPayload(
        recommendation_id=recommendation_id,
        action=action,
        decision_level=decision_level,
        item_external_id=item_external_id,
        location_external_id=location_external_id,
        message=message,
    )


def post_l3_pending(payload: L3PendingPayload, *, url: Optional[str] = None) -> bool:
    """POST ``payload`` to the L3 webhook, best-effort.

    Returns True when the request was actually sent (completed without raising),
    False when it was skipped (no URL configured, httpx unavailable) or failed.
    Every failure is swallowed and logged at WARNING — this MUST NEVER raise, so
    a caller can invoke it inside or around a DB transaction without risk."""
    target = resolve_webhook_url(url)
    if target is None:
        # Opt-out: no destination configured. Silent by design.
        return False
    if not _HTTPX_AVAILABLE:
        logger.warning(
            "l3_webhook.skipped reason=httpx_unavailable recommendation_id=%s",
            payload.recommendation_id,
        )
        return False

    try:
        response = httpx.post(
            target,
            json=payload.model_dump(mode="json"),
            timeout=L3_TIMEOUT_SECONDS,
        )
        if response.status_code >= 400:
            logger.warning(
                "l3_webhook.non_2xx recommendation_id=%s status=%s",
                payload.recommendation_id,
                response.status_code,
            )
        else:
            logger.info(
                "l3_webhook.sent recommendation_id=%s action=%s level=%s status=%s",
                payload.recommendation_id,
                payload.action,
                payload.decision_level,
                response.status_code,
            )
        return True
    except Exception as exc:
        # Best-effort: never propagate. A missing/unreachable endpoint or a
        # timeout must not affect the recommendation that was just persisted.
        logger.warning(
            "l3_webhook.failed recommendation_id=%s error=%s",
            payload.recommendation_id,
            exc,
        )
        return False


def notify_l3_pending(
    *,
    recommendation_id: UUID,
    action: str,
    decision_level: str,
    message: str,
    item_external_id: Optional[str] = None,
    location_external_id: Optional[str] = None,
    url: Optional[str] = None,
) -> bool:
    """Gate + build + post in one call.

    Fires the webhook ONLY when ``decision_level`` is L3 or higher (the
    human-gated, irreversible tier). Below L3 it returns False without touching
    the network. Best-effort throughout (see ``post_l3_pending``)."""
    if not is_l3_or_higher(decision_level):
        return False
    payload = build_l3_payload(
        recommendation_id=recommendation_id,
        action=action,
        decision_level=decision_level,
        message=message,
        item_external_id=item_external_id,
        location_external_id=location_external_id,
    )
    return post_l3_pending(payload, url=url)
