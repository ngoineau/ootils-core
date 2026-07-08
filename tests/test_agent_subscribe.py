"""
test_agent_subscribe.py — pure unit tests for the --subscribe drain helpers
(chantier AN-1, #401; scripts/agent_subscribe.py).

No DB: a fake connection returns canned rows so we assert the cursor PARSING
(fetch_stream_cursor over agent_runs.metrics) and the keyset DRAIN counting
(drain_stream over events) in isolation. The end-to-end skip/run gate against a
live Postgres is left to the integration suite.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Import seam: agent_subscribe lives under scripts/ (outside the package).
_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import agent_subscribe  # noqa: E402


class _CannedConn:
    """Fake connection: each execute() returns the next queued result set.

    A result set is a list of rows; fetchone() yields its first row, fetchall()
    the whole list. dict rows and tuple rows are both supported so the tests can
    exercise the row-factory-agnostic accessors.
    """

    def __init__(self, results: list[list]) -> None:
        self._results = list(results)

    def execute(self, sql: str, params: tuple):  # noqa: ANN201 - test double
        rows = self._results.pop(0) if self._results else []

        class _Cur:
            def __init__(self, rs):
                self._rs = rs

            def fetchone(self):
                return self._rs[0] if self._rs else None

            def fetchall(self):
                return self._rs

        return _Cur(rows)


# ---------------------------------------------------------------------------
# fetch_stream_cursor — parse agent_runs.metrics
# ---------------------------------------------------------------------------


def test_cursor_parsed_from_metrics_dict() -> None:
    conn = _CannedConn([[{"metrics": {"stream_cursor": 128, "recommendations": 3}}]])
    assert agent_subscribe.fetch_stream_cursor(conn, "shortage_watcher", "baseline") == 128


def test_cursor_none_when_no_prior_run() -> None:
    conn = _CannedConn([[]])  # no completed run
    assert agent_subscribe.fetch_stream_cursor(conn, "shortage_watcher", "baseline") is None


def test_cursor_none_when_metrics_absent_key() -> None:
    # A pre-subscribe run stored metrics without a stream_cursor key.
    conn = _CannedConn([[{"metrics": {"recommendations": 5}}]])
    assert agent_subscribe.fetch_stream_cursor(conn, "material_watcher", "baseline") is None


def test_cursor_none_when_metrics_null() -> None:
    conn = _CannedConn([[{"metrics": None}]])
    assert agent_subscribe.fetch_stream_cursor(conn, "material_watcher", "baseline") is None


def test_cursor_tolerates_string_integer() -> None:
    # JSONB may surface the number as a string depending on how it was stored.
    conn = _CannedConn([[{"metrics": {"stream_cursor": "77"}}]])
    assert agent_subscribe.fetch_stream_cursor(conn, "shortage_watcher", "baseline") == 77


def test_cursor_none_on_malformed_value() -> None:
    conn = _CannedConn([[{"metrics": {"stream_cursor": "not-a-number"}}]])
    assert agent_subscribe.fetch_stream_cursor(conn, "shortage_watcher", "baseline") is None


def test_cursor_parsed_from_tuple_row() -> None:
    # tuple_row: metrics is the single selected column.
    conn = _CannedConn([[({"stream_cursor": 9},)]])
    assert agent_subscribe.fetch_stream_cursor(conn, "shortage_watcher", "baseline") == 9


# ---------------------------------------------------------------------------
# drain_stream — keyset advance + relevance counting
# ---------------------------------------------------------------------------


def test_drain_counts_only_relevant_types_and_advances_cursor() -> None:
    rows = [
        {"stream_seq": 11, "event_type": "calc_run_finished"},
        {"stream_seq": 12, "event_type": "recommendation_created"},  # not relevant
        {"stream_seq": 13, "event_type": "shortage_detected"},
        {"stream_seq": 14, "event_type": "snapshot_captured"},  # not relevant
    ]
    conn = _CannedConn([rows])
    new_cursor, relevant = agent_subscribe.drain_stream(conn, "baseline", 10)
    assert new_cursor == 14  # advanced over EVERY drained row
    assert relevant == 2  # calc_run_finished + shortage_detected only


def test_drain_empty_keeps_cursor_and_zero_relevant() -> None:
    conn = _CannedConn([[]])
    new_cursor, relevant = agent_subscribe.drain_stream(conn, "baseline", 42)
    assert new_cursor == 42
    assert relevant == 0


def test_drain_tuple_rows() -> None:
    # tuple_row: (stream_seq, event_type)
    rows = [(5, "calc_run_finished"), (6, "outcome_evaluated")]
    conn = _CannedConn([rows])
    new_cursor, relevant = agent_subscribe.drain_stream(conn, "baseline", 0)
    assert new_cursor == 6
    assert relevant == 1


def test_current_max_seq_reads_scalar() -> None:
    conn = _CannedConn([[{"seq": 99}]])
    assert agent_subscribe.current_max_seq(conn, "baseline") == 99


def test_current_max_seq_zero_on_empty_stream() -> None:
    conn = _CannedConn([[{"seq": 0}]])
    assert agent_subscribe.current_max_seq(conn, "baseline") == 0
