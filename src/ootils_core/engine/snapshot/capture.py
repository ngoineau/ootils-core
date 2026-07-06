"""
capture.py — inventory snapshot capture (chantier #393 A3-PR1, ADR-030).

The proof-machine foundation: a per-day, per-(item, location) point-in-time
record of on-hand stock, persisted in ``inventory_snapshots`` (migration 067)
so a later pass can compare "what we said would happen" against "what actually
happened".

Split, mirroring the engine's DB-boundary convention (mrp/core is pure,
mrp/loader owns SQL):

  * ``capture_snapshot`` is a SELECT-only read that scans on-hand per
    (item, location) coordinate from the scenario's ``OnHandSupply`` nodes and
    returns a deterministic, stably-sorted list of in-memory ``SnapshotRow``.
    It writes NOTHING — testable against a golden without any persistence.
  * ``persist_snapshot`` is the only writer: an idempotent upsert into
    ``inventory_snapshots`` keyed on the UNIQUE (scenario_id, item_id,
    location_id, as_of_date), so a re-capture of the same coordinate/day
    overwrites rather than duplicates.

PER-SITE, NEVER POOLED (the DRP lesson, ADR-028): on-hand is scanned per
(item, location) — the snapshot is a site-level fact. This is deliberately NOT
the item-pooled scan of ``mrp/loader.load_planning_data`` (which SUMs on-hand
across locations for the make/buy echelon). A node with a NULL location_id is
skipped: an un-located on-hand has no place in a per-site history (the FK on
``inventory_snapshots.location_id`` is NOT NULL anyway).

RAW UUID COORDINATES: the snapshot stores the raw ``nodes.item_id`` /
``nodes.location_id`` UUIDs (matching the migration 067 FK columns), NOT the
external_id business keys the DRP resolves — a snapshot is an internal stock
fact anchored to master data, and the UNIQUE key is UUID-shaped.

SHORTAGE ENRICHMENT (first_shortage_date / shortage_severity_usd) IS DEFERRED
to a later PR — captured NULL here, honestly. The canonical shortage math
(``mrp/core.first_shortage``) is ITEM-POOLED (pooled on-hand + pooled demand),
so stamping its date/severity onto each per-location row would attribute a
pooled shortage to individual sites and double-count the same item deficit
across N locations — the exact per-site collapse this module (and ADR-028)
exists to prevent. The DRP's per-location ``projected_deficits`` is closer in
grain but (a) keys by external_id, not the UUIDs this table stores, (b) nets NO
firm receipts (its projection is deliberately receipt-free), so its shortage
picture diverges from the canonical ``shortages`` truth, and (c) yields a
unit deficit, not the $-valued ShortageDetector severity. An honest per-(item,
location) severity therefore needs a per-location projection that nets firm
receipts AND applies the $ formula — future work. Until then both columns stay
NULL, which the migration's contract reads as "no projected shortage / not
calculable at this grain" (first_shortage_date and shortage_severity_usd are
NULL together by design).

Scenario-scoped (schema-consistent, V1 baseline-only per migration 067): the
scan reads the scenario's own nodes, so a fork would snapshot fork state — but
the V1 capturers (CLI/cron/API) always pass baseline.
"""
from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID

from psycopg.rows import tuple_row

from ootils_core.db.types import DictRowConnection
from ootils_core.engine.mrp.core import BASELINE

# Capture channels — kept in sync with the inventory_snapshots.source CHECK
# constraint (migration 067). Validated here so a bad source fails loudly at the
# persistence boundary rather than as an opaque DB CHECK violation.
VALID_SOURCES: frozenset[str] = frozenset({"cli", "api", "cron"})


@dataclass(frozen=True)
class SnapshotRow:
    """One captured coordinate — an in-memory row destined for
    ``inventory_snapshots``.

    Coordinates are the raw graph UUIDs (``nodes.item_id`` / ``location_id``),
    matching the migration 067 FK columns. ``on_hand_qty`` is a Decimal so it
    round-trips into NUMERIC(18,6) without float drift.

    ``first_shortage_date`` and ``shortage_severity_usd`` are the NULL-honest
    shortage picture: both are None in PR1 (enrichment deferred — see the module
    docstring). The contract is that they are None together or set together.
    """

    scenario_id: UUID
    item_id: UUID
    location_id: UUID
    as_of_date: _dt.date
    on_hand_qty: Decimal
    first_shortage_date: _dt.date | None
    shortage_severity_usd: Decimal | None
    source: str


def capture_snapshot(
    conn: DictRowConnection,
    scenario: str = BASELINE,
    as_of_date: _dt.date | None = None,
    *,
    source: str = "cli",
) -> list[SnapshotRow]:
    """Scan on-hand per (item, location) for ``scenario`` and build snapshot rows.

    SELECT-only — writes nothing (persistence is ``persist_snapshot``'s job).
    Deterministic: rows are sorted by (item_id, location_id) so the output is
    stable regardless of scan order, and on-hand is summed per coordinate with
    Decimal arithmetic (no float pooling error).

    ``as_of_date`` defaults to the DB's CURRENT_DATE — resolved from the same
    connection so the capture day matches the scenario's own clock (the MRP /
    DRP loaders anchor their horizon to CURRENT_DATE identically).

    ``source`` is carried onto every row (validated at persist time) so the
    caller declares its channel once; it does not affect the scan.
    """
    # tuple_row for positional access regardless of the connection's configured
    # row_factory — a scenario-aware caller (API path) hands us the app's
    # dict_row connection, under which positional access would raise; the CLI
    # hands us a tuple_row connection. Same defensive pin as mrp/drp loaders.
    cur = conn.cursor(row_factory=tuple_row)

    if as_of_date is None:
        today_row = cur.execute("SELECT CURRENT_DATE").fetchone()
        if today_row is None:
            raise RuntimeError("capture_snapshot: SELECT CURRENT_DATE yielded no row")
        as_of_date = today_row[0]

    # On-hand per (item, location), summed. Scenario-scoped like the MRP/DRP
    # nodes scans. A NULL location_id (or item_id) is excluded: a snapshot row
    # must carry both coordinates (NOT NULL FKs on inventory_snapshots), and an
    # un-located on-hand has no per-site home. Raw UUIDs — no external_id
    # resolution (the snapshot stores UUID coordinates, unlike the DRP).
    on_hand: dict[tuple[UUID, UUID], Decimal] = {}
    for item_id, location_id, qty in cur.execute(
        "SELECT item_id, location_id, quantity FROM nodes "
        "WHERE scenario_id = %(b)s AND active "
        "AND node_type = 'OnHandSupply' "
        "AND item_id IS NOT NULL AND location_id IS NOT NULL "
        "AND quantity IS NOT NULL",
        {"b": scenario},
    ).fetchall():
        coord = (item_id, location_id)
        # quantity is NUMERIC -> psycopg yields Decimal; keep it exact.
        on_hand[coord] = on_hand.get(coord, Decimal(0)) + qty

    scenario_uuid = UUID(scenario)
    rows: list[SnapshotRow] = []
    for (item_id, location_id), qty in sorted(
        on_hand.items(), key=lambda kv: (str(kv[0][0]), str(kv[0][1]))
    ):
        rows.append(
            SnapshotRow(
                scenario_id=scenario_uuid,
                item_id=item_id,
                location_id=location_id,
                as_of_date=as_of_date,
                on_hand_qty=qty,
                # Enrichment deferred (see module docstring). NULL-honest: both
                # shortage columns stay None together in PR1.
                first_shortage_date=None,
                shortage_severity_usd=None,
                source=source,
            )
        )
    return rows


def persist_snapshot(
    conn: DictRowConnection,
    rows: list[SnapshotRow],
    source: str,
) -> int:
    """Idempotently upsert snapshot rows into ``inventory_snapshots``.

    The ONLY writer of the table. ON CONFLICT on the UNIQUE (scenario_id,
    item_id, location_id, as_of_date) DO UPDATE — a re-capture of the same
    coordinate/day overwrites the stock/shortage picture and re-stamps
    ``captured_at``, never duplicates. Returns the number of rows written
    (inserted or updated).

    ``source`` overrides the per-row source so the channel is declared once by
    the caller (CLI='cli', API='api', cron='cron'). Validated here — an invalid
    channel fails loudly rather than as an opaque DB CHECK violation. Does NOT
    commit: the caller owns the transaction (``get_db`` for the API, the CLI's
    own ``with psycopg.connect`` context).
    """
    if source not in VALID_SOURCES:
        raise ValueError(
            f"invalid snapshot source {source!r} — must be one of {sorted(VALID_SOURCES)}"
        )
    if not rows:
        return 0

    written = 0
    for row in rows:
        conn.execute(
            """
            INSERT INTO inventory_snapshots
                (scenario_id, item_id, location_id, as_of_date, on_hand_qty,
                 first_shortage_date, shortage_severity_usd, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (scenario_id, item_id, location_id, as_of_date) DO UPDATE SET
                on_hand_qty           = EXCLUDED.on_hand_qty,
                first_shortage_date   = EXCLUDED.first_shortage_date,
                shortage_severity_usd = EXCLUDED.shortage_severity_usd,
                source                = EXCLUDED.source,
                captured_at           = now()
            """,
            (
                row.scenario_id,
                row.item_id,
                row.location_id,
                row.as_of_date,
                row.on_hand_qty,
                row.first_shortage_date,
                row.shortage_severity_usd,
                source,
            ),
        )
        written += 1
    return written
