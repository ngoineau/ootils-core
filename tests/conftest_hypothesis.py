"""Deterministic Hypothesis profiles for the property-based suite (moteur-c1 C1).

This module is imported and executed once by ``tests/conftest.py`` at session
start (guarded — a lean install without Hypothesis degrades gracefully, the
property files self-skip on their own import). It registers two profiles and
loads the right one from the environment:

* ``ci``  — activated when the ``CI`` env var is set (GitHub Actions sets it to
  ``"true"`` automatically). ``derandomize=True`` so the example stream is a
  pure function of the test source (no wall-clock/PID seed): a red build on one
  runner reproduces byte-for-byte on any other from the SAME commit.
  ``max_examples=200`` (a wider net than dev), ``deadline=None`` (per-example
  timing must never flake a correctness gate on a shared runner),
  ``database=None`` (no cross-run example DB — CI is stateless and the ``.hypothesis``
  cache directory is not persisted between jobs).
* ``dev`` — the default. ``max_examples=50`` for a fast local edit loop,
  randomized (finds fresh counterexamples run-to-run), ``deadline=None``.

Reproducing a counterexample
----------------------------
On failure Hypothesis prints the MINIMAL failing example inline, e.g.::

    Falsifying example: test_foo(weights=[Decimal('1'), Decimal('1')])

Both profiles set ``print_blob=True``, so Hypothesis ALSO prints a ready-to-paste
decorator::

    You can reproduce this example by temporarily adding
    @reproduce_failure('6.156.9', b'AXicY2BgYGAEAA...')
    as a decorator on your test

Copy that ``@reproduce_failure(...)`` line onto the failing test function and
re-run just that test to replay the exact input:

    PYTHONPATH=C:\\dev\\worktrees\\moteur-c1\\src \\
        python -m pytest tests/test_prop_<name>.py::<test> -q

Remove the decorator once fixed (the blob is pinned to a Hypothesis version).
Under the ``ci`` profile the failure is already deterministic for a given
commit, so re-running with ``CI=1`` on the same source reproduces it without any
blob. To force a specific stream locally use ``--hypothesis-seed=<n>``.
"""
from __future__ import annotations

import os

from hypothesis import HealthCheck, settings

CI_PROFILE = "ci"
DEV_PROFILE = "dev"


def register_profiles() -> str:
    """Register the ``ci``/``dev`` profiles and load one from ``$CI``.

    Returns the name of the loaded profile (handy for a diagnostic print or an
    assertion in a meta-test). Idempotent: re-registering a profile name simply
    overwrites it, so a second call (e.g. a nested conftest) is harmless.
    """
    settings.register_profile(
        DEV_PROFILE,
        max_examples=50,
        deadline=None,
        print_blob=True,
    )
    settings.register_profile(
        CI_PROFILE,
        max_examples=200,
        deadline=None,
        derandomize=True,
        database=None,
        print_blob=True,
        # A shared CI runner's timing is noisy; the too_slow health check would
        # flake a correctness gate for a reason that is not a correctness
        # problem. Every strategy here is deliberately cheap and lightly
        # filtered, so this is the only health check we relax.
        suppress_health_check=[HealthCheck.too_slow],
    )
    active = CI_PROFILE if os.environ.get("CI") else DEV_PROFILE
    settings.load_profile(active)
    return active
