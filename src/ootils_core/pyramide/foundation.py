"""
Pyramide axis B — PR-B2: real Chronos-2 foundation-model wrapper.

Thin, dependency-lazy layer over the ``chronos-forecasting`` package
(Amazon, Apache-2.0 — the license decision of
docs/DESIGN-pyramide-forecasting.md §2.B; Moirai stays EXCLUDED,
cc-by-nc-4.0). The heavy imports (``chronos``, ``torch``) happen inside
functions only, mirroring the existing statsforecast /
hierarchicalforecast pattern: importing THIS module is always safe, and
an absent backend surfaces as :class:`FoundationUnavailable` — never an
ImportError at import time.

Contract with the deterministic core (spec §2.B "déterminisme"):

* The FM lives on the **stochastic edge**. Its output is a dated,
  seeded, versioned, logged artifact (``pyramide_runs.model_revision``,
  migration 059) — it never enters the propagation core.
* **Point forecast = quantile 0.5** (median). The model's native
  quantiles are deliberately **NOT** mapped to
  ``forecast_values.confidence_interval_lower/upper``: those columns
  carry conformal bounds calibrated on deterministic backtest residuals
  (PR-D2, ``engines.conformal_bounds``) and an FM has no such backtest
  — mixing calibrated conformal bounds with raw model quantiles would
  make the columns' coverage semantics unreadable (architect decision,
  documented refusal). FM runs therefore persist NULL bounds.
* **Batch inference only**: :func:`forecast_batch` takes ALL the
  series of a run in one call (never series-by-series — spec §2.B
  "inférence par batch").

Reproducibility (best-effort, documented limits):
    ``forecast_batch`` seeds torch (``torch.manual_seed``) before every
    batch, which makes CPU inference reproducible for a fixed
    environment (same torch build, same weights revision). This is NOT
    a bit-for-bit guarantee across environments: GPU kernels
    (non-deterministic reductions), BLAS builds and torch versions can
    all change low-order bits — the same caveat as MinT (ADR-022). The
    honest seal is the persisted (seed, model_id@revision,
    code_version) triple, not a cross-machine bitwise promise.

Offline VMs / pre-downloaded weights:
    Weight resolution goes through huggingface_hub, which honours the
    standard environment variables — set ``HF_HOME`` (cache location)
    and ``HF_HUB_OFFLINE=1`` (and/or ``TRANSFORMERS_OFFLINE=1``) to run
    fully offline against a pre-populated cache. Pre-download once with
    ``huggingface-cli download amazon/chronos-2`` (or a first online
    ``load_pipeline()``), then ship/mount the cache directory. This
    module never sets those variables itself: the deployment owns them.

What ``revision`` really seals:
    ``load_pipeline`` tries to resolve the HuggingFace **commit SHA** of
    the snapshot actually loaded (transformers exposes it as
    ``config._commit_hash``). When the backend does not expose it, we do
    NOT fabricate a SHA: the fallback seal is the explicitly requested
    revision (if any), else ``chronos-forecasting==<package version>`` —
    ``LoadedPipeline.revision_source`` says which one you got.

Generic by design: no business constant, no domain assumption — a
model_id and float series in, median curves out.
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from importlib import metadata
from typing import Any, Sequence

logger = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_MODEL_ID",
    "FoundationUnavailable",
    "LoadedPipeline",
    "forecast_batch",
    "load_pipeline",
    "reset_pipeline_cache",
]

# Default FM of Ootils (spec §2.B, locked 2026-05-31): Apache-2.0,
# multivariate + covariate-informed zero-shot.
DEFAULT_MODEL_ID = "amazon/chronos-2"


class FoundationUnavailable(RuntimeError):
    """The optional foundation-model backend cannot serve forecasts.

    Raised when ``chronos-forecasting`` is not installed
    (``pip install 'ootils-core[foundation]'``) or the model weights
    cannot be loaded (offline without a populated cache, bad model_id or
    revision). Not a data error: callers fall back to the deterministic
    AUTO_SELECT path unless ``strict_backend`` was requested — the same
    contract as ``ReconciliationUnavailable`` (reconcile.py).
    """


@dataclass(frozen=True)
class LoadedPipeline:
    """A loaded FM pipeline plus the provenance seal of its weights.

    ``revision`` is what migration 059 persists in
    ``pyramide_runs.model_revision`` (through the engine's
    ``selected_model`` label too). ``revision_source`` documents what the
    seal really is:

    * ``'hf_commit_sha'`` — the commit SHA of the snapshot the backend
      actually loaded (the strongest seal).
    * ``'requested_revision'`` — the caller-pinned revision, echoed
      back because the backend did not expose the loaded SHA.
    * ``'package_version'`` — ``chronos-forecasting==X.Y.Z``: no SHA
      available at all; the package version is the honest best seal
      (never a fabricated SHA).
    """

    pipeline: Any
    model_id: str
    revision: str
    revision_source: str


# One pipeline per (model_id, requested_revision) per PROCESS — loading is
# the expensive step (weights in memory); every run of the process reuses it.
_PIPELINE_CACHE: dict[tuple[str, str | None], LoadedPipeline] = {}
_CACHE_LOCK = threading.Lock()


def reset_pipeline_cache() -> None:
    """Drop every cached pipeline (frees the weights; mostly for tests)."""
    with _CACHE_LOCK:
        _PIPELINE_CACHE.clear()


def load_pipeline(
    model_id: str = DEFAULT_MODEL_ID, revision: str | None = None
) -> LoadedPipeline:
    """Load (once per process) a Chronos pipeline and seal its revision.

    Lazy backend import: ``chronos`` / ``torch`` are only touched here.
    The returned object is cached module-level keyed by
    ``(model_id, revision)`` — repeated calls are free.

    Args:
        model_id: HuggingFace model id (default ``amazon/chronos-2``).
        revision: optional HF revision (tag / branch / commit SHA) to
            pin the weights. None = the backend's default branch.

    Raises:
        FoundationUnavailable: backend not installed or weights not
            loadable (see class docstring).
    """
    key = (model_id, revision)
    with _CACHE_LOCK:
        cached = _PIPELINE_CACHE.get(key)
        if cached is not None:
            return cached

        try:
            from chronos import BaseChronosPipeline
        except ImportError as exc:
            raise FoundationUnavailable(
                "chronos-forecasting is not installed "
                "(pip install 'ootils-core[foundation]')"
            ) from exc

        kwargs: dict[str, Any] = {}
        if revision is not None:
            kwargs["revision"] = revision
        try:
            pipeline = BaseChronosPipeline.from_pretrained(model_id, **kwargs)
        except Exception as exc:
            raise FoundationUnavailable(
                f"cannot load foundation model '{model_id}'"
                f"{f' @ {revision}' if revision else ''}: {exc}"
            ) from exc

        resolved, source = _resolve_revision(pipeline, revision)
        loaded = LoadedPipeline(
            pipeline=pipeline,
            model_id=model_id,
            revision=resolved,
            revision_source=source,
        )
        _PIPELINE_CACHE[key] = loaded
        logger.info(
            "foundation pipeline loaded: %s @ %s (%s)",
            model_id, resolved, source,
        )
        return loaded


def _resolve_revision(pipeline: Any, requested: str | None) -> tuple[str, str]:
    """Best honest seal of the loaded weights (see module docstring).

    Never fabricates a SHA: commit hash if the backend exposes one, else
    the caller's pinned revision, else the chronos package version.
    """
    sha = _commit_hash_of(pipeline)
    if sha is not None:
        return sha, "hf_commit_sha"
    if requested is not None:
        return requested, "requested_revision"
    try:
        package_version = metadata.version("chronos-forecasting")
    except metadata.PackageNotFoundError:  # pragma: no cover - installed if we got here
        package_version = "unknown"
    return f"chronos-forecasting=={package_version}", "package_version"


def _commit_hash_of(pipeline: Any) -> str | None:
    """Commit SHA of the loaded snapshot, if the backend exposes it.

    transformers stamps ``config._commit_hash`` on hub-loaded configs;
    chronos pipelines nest the model one or two attributes deep and the
    exact layout varies by pipeline class — probe the known shapes.
    """
    candidates = [pipeline]
    for name in ("model", "inner_model"):
        obj = getattr(pipeline, name, None)
        if obj is not None:
            candidates.append(obj)
            nested = getattr(obj, "model", None)
            if nested is not None:
                candidates.append(nested)
    for obj in candidates:
        config = getattr(obj, "config", None)
        sha = getattr(config, "_commit_hash", None)
        if isinstance(sha, str) and sha:
            return sha
    return None


def forecast_batch(
    pipeline: Any,
    histories: Sequence[Sequence[float]],
    horizon: int,
    *,
    seed: int,
) -> list[list[float]]:
    """Median forecast curves for a BATCH of series — one backend call.

    This is the only inference entry point: callers gather every
    FM-routed series of a run and pass them together (never
    series-by-series, spec §2.B). ``pipeline`` is injected (the
    ``LoadedPipeline.pipeline`` object, or any object exposing
    ``predict_quantiles``) — dependency injection keeps tests pure: a
    fake pipeline exercises the batching without mocking the library.

    Point forecast = quantile 0.5. The other native quantiles are
    intentionally dropped here (NOT surfaced as confidence intervals —
    see the module docstring for the conformal-coherence rationale).

    Seeding: ``torch.manual_seed(seed)`` is set before the batch when
    torch is importable — best-effort reproducibility (CPU, fixed
    environment); GPU/BLAS caveats in the module docstring. With an
    injected non-torch fake, seeding is a no-op by construction.

    Args:
        pipeline: object exposing
            ``predict_quantiles(context, prediction_length, quantile_levels)``
            returning ``(quantiles, mean)`` with ``quantiles`` indexable
            as ``[series][step][quantile]``.
        histories: one non-empty numeric history per series, oldest
            first (the caller's ordering is preserved in the output).
        horizon: number of forecast steps (>= 1).
        seed: torch seed for the batch.

    Returns:
        One median curve (``horizon`` floats) per input history, in the
        same order.

    Raises:
        ValueError: empty history, or horizon < 1 (caller bugs —
            fail loudly, not a backend availability issue).
        FoundationUnavailable: the backend call itself failed.
    """
    if horizon < 1:
        raise ValueError(f"horizon must be >= 1 (got {horizon})")
    if not histories:
        return []
    for index, history in enumerate(histories):
        if not history:
            raise ValueError(
                f"histories[{index}] is empty — every series needs at least "
                "one observation"
            )

    try:
        import torch
    except ImportError:
        torch = None  # injected fake pipeline: plain lists are fine

    contexts: list[Any]
    if torch is not None:
        torch.manual_seed(int(seed))
        contexts = [
            torch.tensor([float(v) for v in history], dtype=torch.float32)
            for history in histories
        ]
    else:
        contexts = [[float(v) for v in history] for history in histories]

    try:
        quantiles, _mean = pipeline.predict_quantiles(
            contexts, prediction_length=horizon, quantile_levels=[0.5]
        )
    except Exception as exc:
        raise FoundationUnavailable(
            f"foundation batch inference failed: {exc}"
        ) from exc

    curves: list[list[float]] = []
    for index in range(len(histories)):
        series_quantiles = quantiles[index]
        curves.append(
            [float(series_quantiles[step][0]) for step in range(horizon)]
        )
    return curves
