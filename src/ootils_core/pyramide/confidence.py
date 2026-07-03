"""
Deterministic forecast confidence score — Pyramide axis D (ADR-023).

PURE module: no DB, no I/O, no randomness. The router (or any agent-facing
consumer) assembles the three raw signals and calls ``compute_confidence``:

  - accuracy   : recent aggregate WAPE-like error RATIO (0.2 = 20 % error),
                 e.g. the aggregate ``wape`` row of pyramide_accuracy_metrics
                 for the run that produced the forecast;
  - depth      : how much demand history backed the model (in days of
                 observed demand);
  - freshness  : age in days of the most recent demand_history ingest
                 (repository.get_demand_freshness().ingest_age_days).

Score contract (all components in [0, 1], weighted sum, weights documented):

  accuracy  = 1 / (1 + wape)            -- monotone, bounded: wape 0 -> 1.0,
                                           wape 1 -> 0.5, wape -> inf -> 0.
  depth     = min(1, depth_days / depth_saturation_days)
                                        -- saturating; the saturation horizon
                                           is a PARAMETER (default 365 days),
                                           never a hard-coded business rule.
  freshness = 1 if ingest_age <= sla_days else sla_days / ingest_age
                                        -- hyperbolic decay past the SLA;
                                           the SLA is a PARAMETER (default 7
                                           days, pilot decision — ADR-023).

  stale     = ingest_age_days > sla_days (PROVEN staleness only; an unknown
              freshness never invents a stale flag — it degrades the score
              through the prudent default instead, and the explanation says
              so, so gating consumers can still refuse to act).

Missing input (None) -> PRUDENT default component ``MISSING_COMPONENT_DEFAULT``
(0.25, deliberately pessimistic — never an optimistic 1.0), traced in the
components dict and named in the explanation. Nothing is invented.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import Mapping

# Pilot default (ADR-023): demand ingested more than 7 days ago is stale.
# ALWAYS overridable by the caller — the SLA is a parameter, not a constant.
DEFAULT_SLA_DAYS = 7

# Saturating depth horizon: a year of observed demand = full depth credit.
# Parameter, not business logic — callers with strong seasonality can raise it.
DEFAULT_DEPTH_SATURATION_DAYS = 365

# Prudent default for a MISSING component (metric NULL, freshness unknown).
# 0.25 = "we know nothing, assume poor" — never 1.0 optimistic.
MISSING_COMPONENT_DEFAULT = Decimal("0.25")

# Documented weighting: accuracy is what agents ultimately act on, so it
# carries half the score; depth and freshness split the other half.
DEFAULT_WEIGHTS: Mapping[str, Decimal] = MappingProxyType(
    {
        "accuracy": Decimal("0.5"),
        "depth": Decimal("0.25"),
        "freshness": Decimal("0.25"),
    }
)

_COMPONENT_KEYS = frozenset({"accuracy", "depth", "freshness"})
_ONE = Decimal("1")
_ZERO = Decimal("0")
_QUANTUM = Decimal("0.0001")


@dataclass(frozen=True)
class ConfidenceScore:
    """Traced confidence output — agents gate actions on ``score`` + ``stale``.

    ``components`` carries every component EFFECTIVELY used (0..1 each,
    prudent defaults included), so the score is reproducible by hand from
    the trace (explainability, ADR-004 spirit).
    """

    score: Decimal
    components: Mapping[str, Decimal]
    stale: bool
    explanation: str


def compute_confidence(
    wape: Decimal | None,
    history_depth_days: int | None,
    ingest_age_days: Decimal | int | None,
    *,
    sla_days: int = DEFAULT_SLA_DAYS,
    depth_saturation_days: int = DEFAULT_DEPTH_SATURATION_DAYS,
    weights: Mapping[str, Decimal] = DEFAULT_WEIGHTS,
) -> ConfidenceScore:
    """Deterministic confidence score in [0, 1] from explicit components.

    Inputs may be None (unknown): the corresponding component falls back to
    the prudent ``MISSING_COMPONENT_DEFAULT`` and the explanation names it.
    Invalid inputs fail loudly (ValueError) — a negative WAPE or depth is a
    caller bug, not a data condition to paper over.
    """
    if sla_days < 1:
        raise ValueError(f"sla_days must be >= 1, got {sla_days}")
    if depth_saturation_days < 1:
        raise ValueError(
            f"depth_saturation_days must be >= 1, got {depth_saturation_days}"
        )
    if set(weights) != _COMPONENT_KEYS:
        raise ValueError(
            f"weights must define exactly {sorted(_COMPONENT_KEYS)}, "
            f"got {sorted(weights)}"
        )
    normalized_weights = _normalize_weights(weights)

    notes: list[str] = []

    # ── accuracy: 1 / (1 + wape), bounded (0, 1] ──
    if wape is None:
        accuracy = MISSING_COMPONENT_DEFAULT
        notes.append("accuracy unknown (no backtest WAPE) -> prudent default")
        accuracy_detail = f"accuracy={_fmt(accuracy)} (prudent default)"
    else:
        wape = Decimal(wape)
        if wape < 0:
            raise ValueError(f"wape must be >= 0, got {wape}")
        accuracy = _quantize(_ONE / (_ONE + wape))
        accuracy_detail = f"accuracy={_fmt(accuracy)} (wape={_fmt(wape)})"

    # ── depth: saturating min(1, depth / saturation) ──
    if history_depth_days is None:
        depth = MISSING_COMPONENT_DEFAULT
        notes.append("history depth unknown -> prudent default")
        depth_detail = f"depth={_fmt(depth)} (prudent default)"
    else:
        if history_depth_days < 0:
            raise ValueError(
                f"history_depth_days must be >= 0, got {history_depth_days}"
            )
        depth = _quantize(
            min(_ONE, Decimal(history_depth_days) / Decimal(depth_saturation_days))
        )
        depth_detail = (
            f"depth={_fmt(depth)} ({history_depth_days}/{depth_saturation_days}d)"
        )

    # ── freshness: 1 within SLA, hyperbolic decay past it ──
    stale = False
    if ingest_age_days is None:
        freshness = MISSING_COMPONENT_DEFAULT
        notes.append(
            "freshness unknown (no demand_history ingest) -> prudent default"
        )
        freshness_detail = f"freshness={_fmt(freshness)} (prudent default)"
    else:
        age = Decimal(ingest_age_days)
        if age < 0:
            # Sub-day clock skew between app and DB server: clamp, don't crash.
            age = _ZERO
        if age <= sla_days:
            # Quantized like every other component so the trace is uniform
            # ('1.0000', not '1') — display consistency (review PR-D4).
            freshness = _quantize(_ONE)
        else:
            stale = True
            freshness = _quantize(Decimal(sla_days) / age)
        freshness_detail = (
            f"freshness={_fmt(freshness)} (ingest age {_fmt(age)}d, SLA {sla_days}d"
            f"{', STALE' if stale else ''})"
        )

    components: Mapping[str, Decimal] = MappingProxyType(
        {"accuracy": accuracy, "depth": depth, "freshness": freshness}
    )
    score = _quantize(
        sum(
            (components[key] * normalized_weights[key] for key in sorted(components)),
            _ZERO,
        )
    )

    explanation = (
        f"score={_fmt(score)} — {accuracy_detail}; {depth_detail}; "
        f"{freshness_detail}"
    )
    if notes:
        explanation += " | " + "; ".join(notes)

    return ConfidenceScore(
        score=score, components=components, stale=stale, explanation=explanation
    )


def _normalize_weights(weights: Mapping[str, Decimal]) -> Mapping[str, Decimal]:
    converted = {key: Decimal(value) for key, value in weights.items()}
    if any(value < 0 for value in converted.values()):
        raise ValueError(f"weights must be >= 0, got {converted}")
    total = sum(converted.values(), _ZERO)
    if total <= 0:
        raise ValueError("weights must sum to a positive value")
    return {key: value / total for key, value in converted.items()}


def _quantize(value: Decimal) -> Decimal:
    return value.quantize(_QUANTUM)


def _fmt(value: Decimal) -> str:
    return format(value.normalize(), "f")
