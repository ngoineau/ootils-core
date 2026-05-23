"""
scripts/seed_realistic_dataset.py — CLI for the realistic dataset generator.

Generates a discrete-manufacturing dataset (master data + BOMs + sourcing
+ transactional + historic), driven by a named profile (S / M / ...).

Phase 1 (this iteration): master data only — items, locations, suppliers.
Subsequent phases (BOMs, sourcing, transactional, history) land in
follow-up commits per the structured plan in chat.

Usage:
    DATABASE_URL=postgresql://ootils:ootils@127.0.0.1:15432/ootils_dev \\
        python scripts/seed_realistic_dataset.py --profile S --dbname ootils_seed_test

    # Same seed + same profile = byte-identical output across runs.
    python scripts/seed_realistic_dataset.py --profile M --seed 42

WARNING: when --recreate is set (default), DROPs and CREATEs the target
database. Never point at a DB you care about. Defaults to
`ootils_seed_test` for safety.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import replace

import psycopg
from psycopg.rows import dict_row

from ootils_core.seed.config import PROFILES, Profile
from ootils_core.seed.master.items import generate_items, insert_items
from ootils_core.seed.master.locations import generate_locations, insert_locations
from ootils_core.seed.master.suppliers import generate_suppliers, insert_suppliers


def _admin_recreate_db(dsn: str, dbname: str) -> None:
    """Drop and recreate `dbname` via the postgres DB. Caller must have privileges."""
    base = dsn.rsplit("/", 1)[0]
    admin_dsn = f"{base}/postgres"
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.execute(f'CREATE DATABASE "{dbname}" OWNER ootils')
    print(f"[setup] recreated database {dbname}")


def _apply_migrations(dsn: str) -> None:
    """Replay all migrations on the target DB."""
    from ootils_core.db.connection import OotilsDB
    OotilsDB(dsn)
    print("[setup] migrations applied")


def _phase1_master(conn: psycopg.Connection, profile: Profile) -> dict:
    """Generate + insert master data (items, locations, suppliers). Returns counts/timings."""
    t0 = time.perf_counter()
    items = generate_items(profile)
    t_items_gen = time.perf_counter() - t0

    t0 = time.perf_counter()
    locations = generate_locations(profile)
    t_loc_gen = time.perf_counter() - t0

    t0 = time.perf_counter()
    suppliers = generate_suppliers(profile)
    t_sup_gen = time.perf_counter() - t0

    t0 = time.perf_counter()
    n_items = insert_items(conn, items)
    t_items_ins = time.perf_counter() - t0

    t0 = time.perf_counter()
    n_loc = insert_locations(conn, locations)
    t_loc_ins = time.perf_counter() - t0

    t0 = time.perf_counter()
    n_sup = insert_suppliers(conn, suppliers)
    t_sup_ins = time.perf_counter() - t0

    conn.commit()

    return {
        "items": {
            "total": items.total,
            "by_level": {lvl: len(b) for lvl, b in items.by_level.items()},
            "inserted": n_items,
            "gen_seconds": round(t_items_gen, 3),
            "insert_seconds": round(t_items_ins, 3),
        },
        "locations": {
            "total": locations.total,
            "dcs": len(locations.dcs()),
            "plants": len(locations.plants()),
            "inserted": n_loc,
            "gen_seconds": round(t_loc_gen, 3),
            "insert_seconds": round(t_loc_ins, 3),
        },
        "suppliers": {
            "total": suppliers.total,
            "active": len(suppliers.active()),
            "inserted": n_sup,
            "gen_seconds": round(t_sup_gen, 3),
            "insert_seconds": round(t_sup_ins, 3),
        },
    }


def _validate_master(conn: psycopg.Connection) -> dict:
    """Sanity checks on what we just wrote. Counts are returned as plain ints."""
    def _agg(sql: str, key_col: str) -> dict:
        rows = conn.execute(sql).fetchall()
        return {r[key_col]: int(r["n"]) for r in rows}

    items_by_type = _agg(
        "SELECT item_type, COUNT(*) AS n FROM items GROUP BY item_type ORDER BY item_type",
        "item_type",
    )
    items_by_status = _agg(
        "SELECT status, COUNT(*) AS n FROM items GROUP BY status ORDER BY status",
        "status",
    )
    items_by_uom = _agg(
        "SELECT uom, COUNT(*) AS n FROM items GROUP BY uom ORDER BY n DESC",
        "uom",
    )
    loc_by_type = _agg(
        "SELECT location_type, COUNT(*) AS n FROM locations GROUP BY location_type ORDER BY location_type",
        "location_type",
    )
    sup_by_country = _agg(
        "SELECT country, COUNT(*) AS n FROM suppliers GROUP BY country ORDER BY n DESC",
        "country",
    )
    lt_row = conn.execute(
        "SELECT MIN(lead_time_days) AS mn, MAX(lead_time_days) AS mx, "
        "ROUND(AVG(lead_time_days)::numeric, 1) AS avg FROM suppliers"
    ).fetchone()
    return {
        "items_by_type": items_by_type,
        "items_by_status": items_by_status,
        "items_by_uom": items_by_uom,
        "locations_by_type": loc_by_type,
        "suppliers_by_country": sup_by_country,
        "supplier_lead_time_min_max_avg": (lt_row["mn"], lt_row["mx"], lt_row["avg"]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=sorted(PROFILES), default="S",
                        help="Profile size (S=small/POC, M=mid/realistic)")
    parser.add_argument("--dbname", default="ootils_seed_test",
                        help="DB to recreate (default: ootils_seed_test). NEVER point at prod.")
    parser.add_argument("--seed", type=int, default=None,
                        help="Override the profile's RNG seed (for ad-hoc variations).")
    parser.add_argument("--no-recreate", action="store_true",
                        help="Skip DB recreate + migrations (insert into existing DB).")
    args = parser.parse_args()

    base_dsn = os.environ.get("DATABASE_URL")
    if not base_dsn:
        print("FATAL: set DATABASE_URL (e.g. postgresql://ootils:ootils@127.0.0.1:15432/ootils_dev)")
        return 2

    target_dsn = base_dsn.rsplit("/", 1)[0] + f"/{args.dbname}"
    os.environ["DATABASE_URL"] = target_dsn

    profile = PROFILES[args.profile]
    if args.seed is not None:
        profile = replace(profile, seed=args.seed)

    if not args.no_recreate:
        _admin_recreate_db(base_dsn, args.dbname)
        _apply_migrations(target_dsn)

    print(f"[profile] {profile.name}  seed={profile.seed}  "
          f"horizon=+{profile.horizon_days_forward}/-{profile.horizon_days_back} days")
    print()

    with psycopg.connect(target_dsn, row_factory=dict_row) as conn:
        stats = _phase1_master(conn, profile)
        validation = _validate_master(conn)

    print()
    print("=" * 60)
    print("PHASE 1 — master data")
    print("=" * 60)
    for entity, info in stats.items():
        print(f"  {entity}:")
        for k, v in info.items():
            print(f"    {k:18s}  {v}")

    print()
    print("=" * 60)
    print("VALIDATION")
    print("=" * 60)
    for k, v in validation.items():
        print(f"  {k:30s}  {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
