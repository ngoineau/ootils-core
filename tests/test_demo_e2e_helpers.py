"""
tests/test_demo_e2e_helpers.py — pure unit tests for the demo runbook helpers
(#408, scripts/demo_e2e.py).

No DB, no network, no auth token required. Exercises ONLY the pure / no-IO
surface of the executable wedge runbook:

  - mask_dsn        : credential-stripping display DSN (never leaks user/pass/
                      host/port/query; idempotent; db name only survives)
  - _guard_target   : the 'ootils*' target guard (accepts the test/demo bases,
                      refuses foreign DBs, gates ootils_dev behind --allow-dev,
                      never echoes the DSN in its message)
  - _parse_sse      : SSE frame counter (id:-bearing frames only, last stream_seq,
                      per-event-type tally, empty stream -> (0, None, {}))
  - _kpi / _fmt_num : NULL-honest rendering (None -> label, 0.0 -> "0" — never
                      confused, the NULL-honest contract all the way to display)
  - _write_artefact : scoreboard JSON writer (never persists any DSN fragment;
                      summary + steps structure)
  - main            : argv-only exit codes reachable WITHOUT a DB (missing token
                      env -> 2; non-ootils target -> 2)

The DB round-trip coverage (a full run against a seeded Postgres, the gate,
idempotence) lives in tests/integration/test_demo_e2e_integration.py.

demo_e2e.py has zero top-level ootils_core imports (every one is deferred into a
step/build function), so importing it here needs neither OOTILS_API_TOKEN nor a
reachable database — the two main() branches under test both return BEFORE
step0_boot ever constructs the app.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

# demo_e2e.py lives in scripts/ (outside the installed package). Load it by path
# so the test needs neither scripts/ on sys.path a priori nor a console entry.
_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

if "demo_e2e" in sys.modules:
    demo_e2e = sys.modules["demo_e2e"]
else:
    _SPEC = importlib.util.spec_from_file_location(
        "demo_e2e", _SCRIPTS_DIR / "demo_e2e.py"
    )
    assert _SPEC and _SPEC.loader
    demo_e2e = importlib.util.module_from_spec(_SPEC)
    # Register BEFORE exec: the module's @dataclass decorators resolve
    # cls.__module__ via sys.modules during class construction.
    sys.modules["demo_e2e"] = demo_e2e
    _SPEC.loader.exec_module(demo_e2e)

mask_dsn = demo_e2e.mask_dsn
_guard_target = demo_e2e._guard_target
_parse_sse = demo_e2e._parse_sse
_kpi = demo_e2e._kpi
_fmt_num = demo_e2e._fmt_num
_fmt_usd = demo_e2e._fmt_usd
_fmt_pct = demo_e2e._fmt_pct
_write_artefact = demo_e2e._write_artefact
_scoreboard = demo_e2e._scoreboard
_verdict_line = demo_e2e._verdict_line
StepResult = demo_e2e.StepResult
DemoContext = demo_e2e.DemoContext
step8_compare = demo_e2e.step8_compare
PASS = demo_e2e.PASS
SKIP = demo_e2e.SKIP
FAIL = demo_e2e.FAIL


# A recognisable, fully-loaded DSN whose every secret-bearing component is a
# distinctive sentinel — so any leak in any output is trivially detectable.
_SECRET_USER = "s3cretuser"
_SECRET_PASS = "s3cretpass"
_SECRET_HOST = "prod-db-42.internal.example.com"
_SECRET_PORT = "6543"
_SECRET_QUERY = "sslmode=require&application_name=leaky"
_LOUD_DSN = (
    f"postgresql://{_SECRET_USER}:{_SECRET_PASS}@{_SECRET_HOST}:{_SECRET_PORT}"
    f"/ootils_pilote_test?{_SECRET_QUERY}"
)
_SECRET_FRAGMENTS = (
    _SECRET_USER,
    _SECRET_PASS,
    _SECRET_HOST,
    _SECRET_PORT,
    "sslmode",
    "application_name",
    "leaky",
)


# ===========================================================================
# mask_dsn — credential/host/port/query stripped; only the db NAME survives
# ===========================================================================


def test_mask_dsn_strips_every_secret_component():
    masked = mask_dsn(_LOUD_DSN)
    assert masked == "db=ootils_pilote_test"
    for fragment in _SECRET_FRAGMENTS:
        assert fragment not in masked, f"leaked {fragment!r} in {masked!r}"


def test_mask_dsn_keeps_only_the_db_name():
    # The db name (and nothing else) is the one component allowed to survive.
    assert mask_dsn("postgresql://u:p@h:5432/ootils_test") == "db=ootils_test"


def test_mask_dsn_drops_query_string():
    masked = mask_dsn("postgresql://u:p@h:5432/ootils_demo?sslmode=disable")
    assert masked == "db=ootils_demo"
    assert "sslmode" not in masked
    assert "?" not in masked


def test_mask_dsn_bare_name_maps_to_itself():
    # A slash-less DSN (e.g. a bare db name) maps to itself, no credentials.
    assert mask_dsn("ootils_test") == "db=ootils_test"


def test_mask_dsn_socket_style_dsn():
    # postgresql:///ootils_dev (local socket, no host) — name still survives clean.
    assert mask_dsn("postgresql:///ootils_dev") == "db=ootils_dev"


def test_mask_dsn_trailing_slash_tolerated():
    assert mask_dsn("postgresql://u:p@h:5432/ootils_test/") == "db=ootils_test"


def test_mask_dsn_idempotent_never_resurrects_a_secret():
    # The load-bearing property: masking an already-masked value can never
    # bring a stripped credential back and never drops the db name. (The masked
    # string has no '/' or '?', so the tail/name extraction is a fixed point on
    # the meaningful part — it stays a db= label carrying only the name.)
    once = mask_dsn(_LOUD_DSN)
    twice = mask_dsn(once)
    for fragment in _SECRET_FRAGMENTS:
        assert fragment not in twice
    assert "ootils_pilote_test" in twice
    assert twice.startswith("db=")


def test_mask_dsn_empty_is_labelled_unknown_not_blank():
    # Never emit a bare/empty target that could read as "no guard" — explicit label.
    assert mask_dsn("") == "db=<unknown>"


def test_mask_dsn_never_contains_at_or_colon_credentials():
    # Structural: the '@user:pass' / host:port punctuation must be gone.
    masked = mask_dsn(_LOUD_DSN)
    assert "@" not in masked
    assert f":{_SECRET_PORT}" not in masked


# ===========================================================================
# _guard_target — 'ootils*' guard; ootils_dev gated behind --allow-dev
# ===========================================================================


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://u:p@h:5432/ootils_pilote_test",
        "postgresql://u:p@h:5432/ootils_demo",
        "postgresql:///ootils_test",
    ],
)
def test_guard_target_accepts_ootils_bases(dsn):
    assert _guard_target(dsn, allow_dev=False) is None


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://u:p@h:5432/postgres",
        "postgresql://u:p@h:5432/mydb",
        "postgresql:///template1",
    ],
)
def test_guard_target_refuses_non_ootils_bases(dsn):
    msg = _guard_target(dsn, allow_dev=False)
    assert msg is not None
    assert "REFUSED" in msg


def test_guard_target_refuses_ootils_dev_without_allow_dev():
    msg = _guard_target("postgresql://u:p@h:5432/ootils_dev", allow_dev=False)
    assert msg is not None
    assert "REFUSED" in msg
    assert "ootils_dev" in msg  # names the semi-prod base it is protecting


def test_guard_target_accepts_ootils_dev_with_allow_dev():
    assert _guard_target("postgresql://u:p@h:5432/ootils_dev", allow_dev=True) is None


def test_guard_target_message_never_leaks_the_dsn():
    # The refusal message is printed to the operator — it must carry the db NAME
    # only (via mask_dsn), never the credentials/host/port/query.
    msg = _guard_target(_LOUD_DSN.replace("ootils_pilote_test", "postgres"), allow_dev=False)
    assert msg is not None
    for fragment in _SECRET_FRAGMENTS:
        assert fragment not in msg, f"guard message leaked {fragment!r}: {msg!r}"


def test_guard_target_query_string_does_not_smuggle_a_bad_name():
    # The '?...' is stripped before the ootils* prefix check — a foreign db with
    # a query string is still refused, and 'ootils' inside the query never fools it.
    msg = _guard_target("postgresql://u:p@h/postgres?opt=ootils", allow_dev=False)
    assert msg is not None and "REFUSED" in msg


def test_guard_target_allow_dev_does_not_widen_to_foreign_bases():
    # --allow-dev only unlocks ootils_dev; it must NOT let a non-ootils db through.
    msg = _guard_target("postgresql://u:p@h/postgres", allow_dev=True)
    assert msg is not None and "REFUSED" in msg


# ===========================================================================
# _parse_sse — count id:-bearing frames, last stream_seq, per-type tally
# ===========================================================================


def _frame(seq, event_type=None, data="{}"):
    """Build one SSE frame the way the stream router emits them."""
    lines = [f"id: {seq}"]
    if event_type is not None:
        lines.append(f"event: {event_type}")
    lines.append(f"data: {data}")
    return "\n".join(lines) + "\n\n"


# A heartbeat/ping frame as the stream router emits it: event line + data, but
# deliberately NO id: line (a ping must never advance the client cursor).
_PING_FRAME = "event: ping\ndata: {}\n\n"


def test_parse_sse_empty_stream():
    assert _parse_sse("") == (0, None, {})


def test_parse_sse_whitespace_only_stream():
    # No id: lines at all -> zero frames, no last seq, empty tally.
    assert _parse_sse("\n\n\n") == (0, None, {})


def test_parse_sse_counts_only_id_bearing_frames():
    text = (
        _frame(1, "supply_date_changed")
        + _PING_FRAME  # event: ping, NO id: -> excluded
        + _frame(2, "shortage_opened")
    )
    count, last_seq, types = _parse_sse(text)
    assert count == 2
    assert last_seq == 2
    assert types == {"supply_date_changed": 1, "shortage_opened": 1}


def test_parse_sse_ping_frames_never_counted():
    # A stream of nothing but pings advances no cursor and counts zero.
    text = _PING_FRAME * 3
    assert _parse_sse(text) == (0, None, {})


def test_parse_sse_last_stream_seq_is_the_final_id():
    text = _frame(5) + _frame(9) + _frame(7)
    count, last_seq, _types = _parse_sse(text)
    assert count == 3
    assert last_seq == 7  # last frame wins, not the max


def test_parse_sse_per_type_tally_accumulates():
    text = (
        _frame(1, "reco_transitioned")
        + _frame(2, "reco_transitioned")
        + _frame(3, "snapshot_captured")
    )
    _count, _last, types = _parse_sse(text)
    assert types == {"reco_transitioned": 2, "snapshot_captured": 1}


def test_parse_sse_frame_without_event_type_counts_but_untyped():
    # id: present, event: absent -> counted, last_seq updated, no type tallied.
    text = _frame(4, event_type=None)
    count, last_seq, types = _parse_sse(text)
    assert count == 1
    assert last_seq == 4
    assert types == {}


def test_parse_sse_non_numeric_id_is_not_a_frame():
    # id: must be all-digits to advance; a garbage id line yields no frame.
    text = "id: not-a-number\nevent: x\ndata: {}\n\n"
    assert _parse_sse(text) == (0, None, {})


def test_parse_sse_trailing_frame_without_blank_line_is_dropped():
    # The counter commits a frame on the blank-line terminator. An unterminated
    # final frame (no trailing "\n\n") is intentionally NOT counted.
    text = "id: 1\nevent: x\ndata: {}"
    assert _parse_sse(text) == (0, None, {})


# ===========================================================================
# _kpi / _fmt_num — NULL-honest to the pixel: None != 0.0, ever
# ===========================================================================


def test_kpi_none_is_labelled_not_zero():
    assert _kpi(None) == "n/a (no data)"


def test_kpi_zero_float_renders_as_zero_not_na():
    # The whole NULL-honest contract: a genuine 0.0 must read "0", never "n/a".
    assert _kpi(0.0) == "0"


def test_kpi_none_and_zero_are_never_the_same_string():
    assert _kpi(None) != _kpi(0.0)


def test_kpi_float_trims_trailing_zeros():
    assert _kpi(0.2500) == "0.25"


def test_kpi_float_whole_number_has_no_dot():
    assert _kpi(3.0) == "3"


def test_kpi_non_float_passthrough_str():
    assert _kpi(42) == "42"
    assert _kpi("APPROVED") == "APPROVED"


def test_fmt_num_none_is_na():
    assert _fmt_num(None) == "n/a"


def test_fmt_num_none_and_zero_are_distinct():
    # Same NULL-honest guarantee for the forecast-metric formatter.
    assert _fmt_num(None) != _fmt_num(0.0)
    assert _fmt_num(0.0) == "0"


def test_fmt_num_trims_trailing_zeros():
    assert _fmt_num(0.2500) == "0.25"


def test_fmt_num_whole_number_has_no_dot():
    assert _fmt_num(12.0) == "12"


def test_fmt_num_non_numeric_string_passthrough():
    # A non-castable value degrades to its str(), never crashes.
    assert _fmt_num("AUTO_SELECT") == "AUTO_SELECT"


def test_kpi_and_fmt_num_use_distinct_null_labels():
    # Guards the contract that each helper has its OWN exact label (a refactor
    # that collapsed them would trip this).
    assert _kpi(None) == "n/a (no data)"
    assert _fmt_num(None) == "n/a"


# ===========================================================================
# _fmt_usd / _fmt_pct — step 8 (SC-1 scenario compare) $-honest / ratio-honest
# rendering: None != 0.0/0%, ever — same NULL-honest contract as _kpi/_fmt_num.
# ===========================================================================


def test_fmt_usd_none_is_na_never_a_masked_dollar_zero():
    assert _fmt_usd(None) == "n/a"


def test_fmt_usd_zero_renders_as_a_real_dollar_zero_not_na():
    # The NULL-honest contract, mirrored for $: a genuine $0 (e.g. a healthy
    # scenario with zero shortage severity) must read as such, never fall
    # back to the None label.
    assert _fmt_usd(0.0) == "$0.00"


def test_fmt_usd_none_and_zero_are_never_the_same_string():
    assert _fmt_usd(None) != _fmt_usd(0.0)


def test_fmt_usd_formats_with_thousands_separator_and_two_decimals():
    assert _fmt_usd(1234.5) == "$1,234.50"


def test_fmt_usd_negative_value_keeps_the_sign():
    # A delta can legitimately be negative (a fork with LOWER $ exposure than
    # the reference) — the sign must survive, never be clamped away.
    assert _fmt_usd(-42.1) == "$-42.10"


def test_fmt_pct_none_is_na_never_a_masked_zero_or_hundred_percent():
    assert _fmt_pct(None) == "n/a"


def test_fmt_pct_zero_renders_as_a_real_zero_percent_not_na():
    assert _fmt_pct(0.0) == "0.0%"


def test_fmt_pct_none_and_zero_are_never_the_same_string():
    assert _fmt_pct(None) != _fmt_pct(0.0)


def test_fmt_pct_scales_ratio_to_percentage():
    assert _fmt_pct(0.923) == "92.3%"


def test_fmt_pct_full_fill_rate_is_a_hundred_percent():
    assert _fmt_pct(1.0) == "100.0%"


def test_fmt_usd_and_fmt_pct_use_the_same_null_label_as_fmt_num():
    # Both step-8 formatters intentionally reuse _fmt_num's exact "n/a" label
    # (unlike _kpi's more verbose "n/a (no data)") — a deliberate choice, not
    # an accident; this guards it.
    assert _fmt_usd(None) == _fmt_num(None) == "n/a"
    assert _fmt_pct(None) == _fmt_num(None) == "n/a"


# ===========================================================================
# step8_compare — the SKIP branch is pure/DB-free: ctx.fork_scenario_id is
# read BEFORE any ctx.client/ctx.agent_auth access, so a fake DemoContext with
# no fork short-circuits before any I/O — no DB, no mock needed.
# ===========================================================================


def _fork_free_ctx() -> DemoContext:
    return DemoContext(
        dsn="postgresql:///ootils_test",
        client=None,  # never touched: the SKIP branch returns before any I/O
        verbose=False,
        skip_watchers=False,
        run_bench=False,
        show_tokens=False,
    )


def test_step8_compare_skips_honestly_without_a_whatif_fork():
    ctx = _fork_free_ctx()
    assert ctx.fork_scenario_id is None  # the dataclass default, unset by step 7
    result = step8_compare(ctx)
    assert result.status == SKIP
    assert result.number == 8
    assert result.title == "Scenario compare"


def test_step8_compare_skip_detail_names_the_missing_fork():
    result = step8_compare(_fork_free_ctx())
    assert "fork" in result.detail
    assert "step 7" in result.detail


# ===========================================================================
# _write_artefact — scoreboard JSON: DSN never persisted; summary+steps shape
# ===========================================================================


class _FakeCtx:
    """Minimal stand-in for DemoContext, enough for _run_step: it only reads
    .verbose (traceback gate) and .dsn (the value it scrubs out of a raising
    step's message)."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.verbose = False


def _leaky_step(ctx):
    # A step that raises with the raw DSN in its message — exactly the shape
    # _run_step guards against by replacing ctx.dsn with mask_dsn(ctx.dsn)
    # before building the FAIL StepResult. Named step3_* so number/title recover.
    raise RuntimeError(f"connection to {ctx.dsn} exploded mid-query")


_leaky_step.__name__ = "step3_watchers"


def _results_via_run_step():
    """Build the step-result list the way main() actually does — through
    _run_step, which is the ONLY place a DSN could reach a step message and is
    exactly where the script scrubs it. This mirrors production: the writer never
    sees a raw DSN because _run_step already masked it."""
    ctx = _FakeCtx(_LOUD_DSN)
    return [
        StepResult(0, "Boot & migration catch-up", PASS, "booted, schema 069"),
        demo_e2e._run_step(_leaky_step, ctx),  # FAIL, DSN scrubbed to db=name
        StepResult(8, "StreamChanges", SKIP, "n/a"),
    ]


def test_run_step_scrubs_dsn_from_a_raising_step_detail():
    # The production leak guard: a step that raises with the raw DSN in its
    # message becomes a FAIL whose detail carries only the masked db name.
    ctx = _FakeCtx(_LOUD_DSN)
    result = demo_e2e._run_step(_leaky_step, ctx)
    assert result.status == FAIL
    assert result.number == 3
    for fragment in _SECRET_FRAGMENTS:
        assert fragment not in result.detail, f"_run_step leaked {fragment!r}"
    assert "db=ootils_pilote_test" in result.detail


def test_write_artefact_contains_no_dsn_fragment(tmp_path):
    # End-to-end of the leak contract: results built through _run_step, written
    # to the artefact — no secret component of the DSN survives anywhere in it.
    out = tmp_path / "scoreboard.json"
    _write_artefact(out, _LOUD_DSN, _results_via_run_step())
    raw = out.read_text(encoding="utf-8")
    for fragment in _SECRET_FRAGMENTS:
        assert fragment not in raw, f"artefact leaked {fragment!r}"


def _results_with_leaky_message():
    """Simple result list (no raw DSN anywhere) for the structural assertions
    below — 1 pass / 1 fail / 1 skip."""
    return [
        StepResult(0, "Boot & migration catch-up", PASS, "booted, schema 069"),
        StepResult(3, "Governed watchers", FAIL, "watcher crashed (db=ootils_test)"),
        StepResult(8, "StreamChanges", SKIP, "n/a"),
    ]


def test_write_artefact_masks_db_field_to_name_only(tmp_path):
    out = tmp_path / "scoreboard.json"
    _write_artefact(out, _LOUD_DSN, _results_with_leaky_message())
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["db"] == "db=ootils_pilote_test"


def test_write_artefact_summary_matches_scoreboard(tmp_path):
    out = tmp_path / "scoreboard.json"
    results = _results_with_leaky_message()
    _write_artefact(out, _LOUD_DSN, results)
    payload = json.loads(out.read_text(encoding="utf-8"))
    passed, skipped, failed = _scoreboard(results)
    assert payload["summary"] == {"pass": passed, "skip": skipped, "fail": failed}
    assert payload["summary"] == {"pass": 1, "skip": 1, "fail": 1}


def test_write_artefact_step_structure(tmp_path):
    out = tmp_path / "scoreboard.json"
    results = _results_with_leaky_message()
    _write_artefact(out, _LOUD_DSN, results)
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert isinstance(payload["steps"], list)
    assert len(payload["steps"]) == len(results)
    keys = set(payload["steps"][0])
    assert {"number", "title", "status", "detail", "data"} <= keys


def test_write_artefact_is_valid_json_with_generated_at(tmp_path):
    out = tmp_path / "scoreboard.json"
    _write_artefact(out, _LOUD_DSN, _results_with_leaky_message())
    payload = json.loads(out.read_text(encoding="utf-8"))  # raises if invalid
    assert "generated_at" in payload
    assert isinstance(payload["generated_at"], str)


# ===========================================================================
# main — argv-only exit codes reachable WITHOUT a DB (guards + token env)
# ===========================================================================


def test_main_missing_token_env_exits_2(monkeypatch, capsys):
    # No OOTILS_API_TOKEN -> the app would refuse to boot; main short-circuits
    # to exit code 2 BEFORE touching a DB. A valid ootils target proves it is the
    # token check, not the guard, that fires.
    monkeypatch.delenv("OOTILS_API_TOKEN", raising=False)
    rc = demo_e2e.main(["--dsn", "postgresql://u:p@h:5432/ootils_test"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "OOTILS_API_TOKEN" in err


def test_main_non_ootils_target_exits_2(monkeypatch, capsys):
    # Token present so we get PAST the token check and land on the target guard.
    monkeypatch.setenv("OOTILS_API_TOKEN", "irrelevant-for-this-branch")
    rc = demo_e2e.main(["--dsn", "postgresql://u:p@h:5432/postgres"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "REFUSED" in err


def test_main_non_ootils_target_error_does_not_leak_dsn(monkeypatch, capsys):
    monkeypatch.setenv("OOTILS_API_TOKEN", "irrelevant-for-this-branch")
    rc = demo_e2e.main(["--dsn", _LOUD_DSN.replace("ootils_pilote_test", "postgres")])
    assert rc == 2
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    for fragment in _SECRET_FRAGMENTS:
        assert fragment not in combined, f"main leaked {fragment!r} on guard refusal"


def test_main_ootils_dev_without_allow_dev_exits_2(monkeypatch, capsys):
    monkeypatch.setenv("OOTILS_API_TOKEN", "irrelevant-for-this-branch")
    rc = demo_e2e.main(["--dsn", "postgresql://u:p@h:5432/ootils_dev"])
    assert rc == 2
    assert "REFUSED" in capsys.readouterr().err


def test_main_token_check_precedes_guard(monkeypatch, capsys):
    # Both would fail (no token AND foreign db) — the token check must win, i.e.
    # the message is the token one, proving the documented ordering.
    monkeypatch.delenv("OOTILS_API_TOKEN", raising=False)
    rc = demo_e2e.main(["--dsn", "postgresql://u:p@h:5432/postgres"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "OOTILS_API_TOKEN" in err
    assert "REFUSED" not in err


# ===========================================================================
# _verdict_line — the human-facing scoreboard line (used in stdout AND artefact
# consumers); a cheap guard that the tag/label wiring is stable.
# ===========================================================================


def test_verdict_line_tags_each_status():
    assert _verdict_line(StepResult(1, "Governed tokens", PASS, "ok")).startswith("[PASS]")
    assert _verdict_line(StepResult(2, "Forecast + FVA", SKIP, "no series")).startswith("[SKIP]")
    assert _verdict_line(StepResult(3, "Governed watchers", FAIL, "boom")).startswith("[FAIL]")


def test_verdict_line_includes_number_title_detail():
    line = _verdict_line(StepResult(5, "Governance gate", PASS, "agent 403 human 200"))
    assert "step 5" in line
    assert "Governance gate" in line
    assert "agent 403 human 200" in line
