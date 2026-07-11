"""
Smoke tests for the ootils_kernel Rust extension (ADR-016 §week 1).

These tests run only if the Rust module is built and installed in the
active environment. They validate the type boundary contract:

- Decimal precision survives a Python → Rust → Python round-trip
- Date semantics (timezone-naive, byte-identical) are preserved
- Module version is queryable

If `ootils_kernel` isn't installed, the tests are skipped — the rest of
the Python suite stays green even on machines without the Rust toolchain.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

ootils_kernel = pytest.importorskip(
    "ootils_kernel",
    reason="Rust kernel not built locally — run `maturin develop` in rust/ootils_kernel/",
)


def test_version_reachable() -> None:
    # 0.2.0 = the PGPASSWORD-fix wheel: propagate_and_write takes the password
    # as an explicit 2nd positional argument (see propagator_rust.py's
    # wheel-mismatch detection, which keys off this exact version boundary).
    assert ootils_kernel.version() == "0.2.0"


@pytest.mark.parametrize(
    "value",
    [
        "0",
        "1",
        "-1",
        "0.1",
        "0.0000000000000000000001",  # 22-digit precision
        "1234567890.123456789012345678",
        "-9999999999.9999999999",
    ],
)
def test_decimal_roundtrip_precision(value: str) -> None:
    """Each Decimal must come back byte-identical via the str round-trip."""
    py_decimal = Decimal(value)
    returned_str, _ = ootils_kernel.echo(str(py_decimal), date(2026, 5, 24).isoformat())
    assert Decimal(returned_str) == py_decimal


def test_date_roundtrip() -> None:
    """Python date → ISO str → Rust NaiveDate → ISO str → Python date must be exact."""
    orig = date(2026, 5, 24)
    _, returned_iso = ootils_kernel.echo("0", orig.isoformat())
    assert date.fromisoformat(returned_iso) == orig


def test_decimal_arithmetic_no_drift() -> None:
    """Adding two Decimals in Rust must match Python Decimal exactly."""
    a, b = Decimal("0.1"), Decimal("0.2")
    result = Decimal(ootils_kernel.add_decimals(str(a), str(b)))
    # Python Decimal: 0.1 + 0.2 = Decimal("0.3") (no float drift)
    assert result == a + b
    assert result == Decimal("0.3")


def test_days_between_inclusive_start_exclusive_end() -> None:
    """Mirrors the engine's bucket semantics: end - start in days."""
    assert ootils_kernel.days_between("2026-01-01", "2026-01-08") == 7
    assert ootils_kernel.days_between("2026-05-24", "2026-05-24") == 0
    assert ootils_kernel.days_between("2026-05-25", "2026-05-24") == -1


def test_bad_decimal_raises_valueerror() -> None:
    with pytest.raises(ValueError):
        ootils_kernel.echo("not a number", date(2026, 5, 24).isoformat())


def test_bad_date_raises_valueerror() -> None:
    with pytest.raises(ValueError):
        ootils_kernel.echo("0", "not a date")
