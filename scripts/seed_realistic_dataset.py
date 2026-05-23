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
from ootils_core.seed.master.boms import (
    generate_boms,
    insert_boms,
    validate_acyclic,
)
from ootils_core.seed.master.items import generate_items, insert_items
from ootils_core.seed.master.locations import generate_locations, insert_locations
from ootils_core.seed.master.suppliers import generate_suppliers, insert_suppliers
from ootils_core.seed.network.planning_params import (
    generate_planning_params,
    insert_planning_params,
)
from ootils_core.seed.network.supplier_items import (
    generate_supplier_items,
    insert_supplier_items,
)


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
        "_items_ref": items,
        "_locations_ref": locations,
        "_suppliers_ref": suppliers,
    }


def _phase2_boms(conn: psycopg.Connection, profile: Profile, items) -> dict:
    """Generate + insert BOMs (headers + lines). Pure-graph acyclicity check."""
    t0 = time.perf_counter()
    bom_set = generate_boms(profile, items)
    t_gen = time.perf_counter() - t0

    t0 = time.perf_counter()
    validate_acyclic(bom_set, items)
    t_check = time.perf_counter() - t0

    t0 = time.perf_counter()
    n_h, n_l = insert_boms(conn, bom_set)
    t_ins = time.perf_counter() - t0
    conn.commit()

    return {
        "headers_generated": bom_set.total_headers,
        "lines_generated": bom_set.total_lines,
        "headers_inserted": n_h,
        "lines_inserted": n_l,
        "gen_seconds": round(t_gen, 3),
        "acyclicity_check_seconds": round(t_check, 3),
        "insert_seconds": round(t_ins, 3),
    }


def _phase3_network(
    conn: psycopg.Connection,
    profile: Profile,
    items,
    locations,
    suppliers,
) -> dict:
    """Generate + insert supplier_items + item_planning_params."""
    t0 = time.perf_counter()
    si_set = generate_supplier_items(profile, items, suppliers)
    t_si_gen = time.perf_counter() - t0

    t0 = time.perf_counter()
    pp_set = generate_planning_params(profile, items, locations, si_set)
    t_pp_gen = time.perf_counter() - t0

    t0 = time.perf_counter()
    n_si = insert_supplier_items(conn, si_set)
    t_si_ins = time.perf_counter() - t0

    t0 = time.perf_counter()
    n_pp = insert_planning_params(conn, pp_set)
    t_pp_ins = time.perf_counter() - t0
    conn.commit()

    return {
        "supplier_items_generated": si_set.total,
        "supplier_items_inserted": n_si,
        "bought_items_count": len(si_set.bought_items),
        "planning_params_generated": pp_set.total,
        "planning_params_inserted": n_pp,
        "supplier_items_gen_seconds": round(t_si_gen, 3),
        "planning_params_gen_seconds": round(t_pp_gen, 3),
        "supplier_items_insert_seconds": round(t_si_ins, 3),
        "planning_params_insert_seconds": round(t_pp_ins, 3),
    }


def _validate_network(conn: psycopg.Connection) -> dict:
    """Sanity checks on the network (sourcing + planning_params)."""
    def _agg(sql: str, key_col: str) -> dict:
        rows = conn.execute(sql).fetchall()
        return {r[key_col]: int(r["n"]) for r in rows}

    # Distinct items that have AT LEAST one supplier link
    bought_items = conn.execute(
        "SELECT COUNT(DISTINCT item_id) AS n FROM supplier_items"
    ).fetchone()["n"]
    # Items with multi-sourcing (>=2 suppliers)
    multi_sourced = conn.execute(
        """
        SELECT COUNT(*) AS n FROM (
            SELECT item_id FROM supplier_items GROUP BY item_id HAVING COUNT(*) >= 2
        ) s
        """
    ).fetchone()["n"]
    # planning_params: by is_make
    pp_by_make = _agg(
        "SELECT is_make::text AS is_make, COUNT(*) AS n FROM item_planning_params GROUP BY is_make",
        "is_make",
    )
    # planning_params: by location_type via join
    pp_by_loc_type = _agg(
        """
        SELECT l.location_type, COUNT(*) AS n
        FROM item_planning_params ipp
        JOIN locations l ON l.location_id = ipp.location_id
        GROUP BY l.location_type
        """,
        "location_type",
    )
    # Coverage: items with at least one planning_params entry
    item_coverage = conn.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM items)                                AS total_items,
            (SELECT COUNT(DISTINCT item_id) FROM item_planning_params)  AS covered_items
        """
    ).fetchone()
    # Safety stock coverage
    ss_coverage = conn.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE safety_stock_qty IS NOT NULL AND safety_stock_qty > 0) AS with_ss,
            COUNT(*) AS total
        FROM item_planning_params
        """
    ).fetchone()
    lot_rules = _agg(
        "SELECT lot_size_rule::text AS lot_size_rule, COUNT(*) AS n "
        "FROM item_planning_params GROUP BY lot_size_rule ORDER BY n DESC",
        "lot_size_rule",
    )
    return {
        "items_with_at_least_one_supplier": int(bought_items),
        "items_multi_sourced": int(multi_sourced),
        "planning_params_by_is_make": pp_by_make,
        "planning_params_by_location_type": pp_by_loc_type,
        "item_coverage_total_vs_covered": (
            int(item_coverage["total_items"]),
            int(item_coverage["covered_items"]),
        ),
        "safety_stock_coverage": (int(ss_coverage["with_ss"]), int(ss_coverage["total"])),
        "lot_size_rule_distribution": lot_rules,
    }


def _validate_boms(conn: psycopg.Connection) -> dict:
    """Sanity checks on BOM structure post-insert."""
    def _agg(sql: str, key_col: str) -> dict:
        rows = conn.execute(sql).fetchall()
        return {r[key_col]: int(r["n"]) for r in rows}

    # BOMs by parent level — join headers->items->derived level from item_type
    # Note: level is implicit in the BOM graph, but item_type carries enough
    # signal for L0 (finished_good), L1 (semi_finished), L4 (raw_material).
    # L2/L3 both 'component' so we can't distinguish here; just count by type.
    headers_by_parent_type = _agg(
        """
        SELECT i.item_type, COUNT(*) AS n
        FROM bom_headers h
        JOIN items i ON i.item_id = h.parent_item_id
        GROUP BY i.item_type
        ORDER BY i.item_type
        """,
        "item_type",
    )
    lines_per_bom = conn.execute(
        """
        SELECT MIN(c) AS mn, MAX(c) AS mx, ROUND(AVG(c)::numeric, 2) AS avg
        FROM (SELECT COUNT(*) AS c FROM bom_lines GROUP BY bom_id) s
        """
    ).fetchone()
    # Raw material as component (should be 0 BOMs with raw as PARENT, but many as CHILD)
    raw_as_parent = conn.execute(
        """
        SELECT COUNT(*) AS n FROM bom_headers h
        JOIN items i ON i.item_id = h.parent_item_id
        WHERE i.item_type = 'raw_material'
        """
    ).fetchone()
    raw_as_child = conn.execute(
        """
        SELECT COUNT(*) AS n FROM bom_lines l
        JOIN items i ON i.item_id = l.component_item_id
        WHERE i.item_type = 'raw_material'
        """
    ).fetchone()
    return {
        "headers_by_parent_type": headers_by_parent_type,
        "lines_per_bom_min_max_avg": (lines_per_bom["mn"], lines_per_bom["mx"], lines_per_bom["avg"]),
        "raw_material_as_bom_parent": int(raw_as_parent["n"]),  # MUST be 0
        "raw_material_as_bom_child": int(raw_as_child["n"]),
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
        items_ref = stats.pop("_items_ref")
        locations_ref = stats.pop("_locations_ref")
        suppliers_ref = stats.pop("_suppliers_ref")
        validation_p1 = _validate_master(conn)
        stats_p2 = _phase2_boms(conn, profile, items_ref)
        validation_p2 = _validate_boms(conn)
        stats_p3 = _phase3_network(conn, profile, items_ref, locations_ref, suppliers_ref)
        validation_p3 = _validate_network(conn)

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
    print("PHASE 1 — validation")
    print("=" * 60)
    for k, v in validation_p1.items():
        print(f"  {k:30s}  {v}")

    print()
    print("=" * 60)
    print("PHASE 2 — BOMs")
    print("=" * 60)
    for k, v in stats_p2.items():
        print(f"  {k:30s}  {v}")

    print()
    print("=" * 60)
    print("PHASE 2 — validation")
    print("=" * 60)
    for k, v in validation_p2.items():
        print(f"  {k:30s}  {v}")

    print()
    print("=" * 60)
    print("PHASE 3 — network (supplier_items + planning_params)")
    print("=" * 60)
    for k, v in stats_p3.items():
        print(f"  {k:30s}  {v}")

    print()
    print("=" * 60)
    print("PHASE 3 — validation")
    print("=" * 60)
    for k, v in validation_p3.items():
        print(f"  {k:35s}  {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
