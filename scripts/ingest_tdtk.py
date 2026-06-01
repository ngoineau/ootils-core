"""
ingest_tdtk.py — load the T_DTK item master into the demand foundation.

Populates, idempotently (safe to re-run):
  - items.item_cost  <- T_DTK.CDS  (the single at-cost basis; business name CDS)
  - hierarchy + hierarchy_node + item_hierarchy for the TWO product hierarchies:
      * product_local_gen  : GEN_FAM > GEN_GROUP > GEN_PROD  (native codes, is_default)
      * product_corp_oracle: ORACLE_SECTOR > SOLUTION > CATEGORY > FAMILY > LINE
        (labels — coded as the cumulative ">"-joined PATH, because corporate labels
         repeat across levels and would collide on a raw-label PK)

Read-only on items identity (resolves external_id -> item_id); writes item_cost,
hierarchy_node, item_hierarchy. Wrapped in one transaction with a server-side
statement_timeout.

Usage:
    DATABASE_URL=postgresql://ootils:ootils@host:5432/ootils_pilote_test \
        python scripts/ingest_tdtk.py
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg

# T_DTK column indices (0-based)
C_ITEM, C_CDS = 0, 27
C_GEN = {"fam": (9, 10), "group": (11, 12), "prod": (13, 14)}  # (code, desc)
C_ORA = [50, 51, 52, 53, 54]  # sector, solution, category, family, line
ORA_LEVELS = ["sector", "solution", "category", "family", "line"]

H_LOCAL = "product_local_gen"
H_CORP = "product_corp_oracle"


def parse_tdtk(path: Path):
    nodes: dict[str, dict[str, tuple]] = {H_LOCAL: {}, H_CORP: {}}  # code -> (level, desc, parent)
    item_leaf: list[tuple[str, str, str]] = []  # (external_id, hierarchy_id, leaf_code)
    costs: list[tuple[str, float]] = []
    n = 0
    with open(path, encoding="utf-8-sig") as f:
        next(f)  # header
        for line in f:
            r = line.rstrip("\n").split("\t")
            if len(r) <= C_ORA[-1]:
                continue
            n += 1
            ext = r[C_ITEM].strip()
            if not ext:
                continue

            # item_cost (CDS) — keep even 0 (a real $0 cost); skip blank
            cds = r[C_CDS].strip()
            if cds != "":
                try:
                    costs.append((ext, float(cds)))
                except ValueError:
                    pass

            # local GEN hierarchy (native codes)
            gf, gfd = r[C_GEN["fam"][0]].strip(), r[C_GEN["fam"][1]].strip()
            gg, ggd = r[C_GEN["group"][0]].strip(), r[C_GEN["group"][1]].strip()
            gp, gpd = r[C_GEN["prod"][0]].strip(), r[C_GEN["prod"][1]].strip()
            if gf and gg and gp:
                nodes[H_LOCAL].setdefault(gf, ("family", gfd, None))
                nodes[H_LOCAL].setdefault(gg, ("group", ggd, gf))
                nodes[H_LOCAL].setdefault(gp, ("product", gpd, gg))
                item_leaf.append((ext, H_LOCAL, gp))

            # corporate ORACLE hierarchy (labels -> cumulative PATH codes)
            labels = [r[i].strip() for i in C_ORA]
            if all(labels):
                path = ""
                parent = None
                for lvl, lbl in zip(ORA_LEVELS, labels):
                    path = lbl if not path else f"{path}>{lbl}"
                    nodes[H_CORP].setdefault(path, (lvl, lbl, parent))
                    parent = path
                item_leaf.append((ext, H_CORP, path))  # leaf = full 5-level path
    return nodes, item_leaf, costs, n


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--tdtk", default="Raw Data/T_DTK.full.tsv")
    args = p.parse_args(argv)
    if not args.dsn:
        print("ERROR: set DATABASE_URL", file=sys.stderr)
        return 2

    nodes, item_leaf, costs, n = parse_tdtk(Path(args.tdtk))
    print(f"[parse] {n} T_DTK rows | local nodes={len(nodes[H_LOCAL])} "
          f"corp nodes={len(nodes[H_CORP])} | item-leaf links={len(item_leaf)} "
          f"| item_cost rows={len(costs)}")

    with psycopg.connect(args.dsn) as conn:
        conn.execute("SET statement_timeout = '180s'")

        # 1. hierarchy registry
        conn.execute(
            "INSERT INTO hierarchy (hierarchy_id, domain, scope, label, levels, is_default) "
            "VALUES (%s,'product','local','Local product (GEN)', %s, true), "
            "       (%s,'product','corporate','Corporate product (Oracle)', %s, false) "
            "ON CONFLICT (hierarchy_id) DO NOTHING",
            (H_LOCAL, ["family", "group", "product"], H_CORP, ORA_LEVELS),
        )

        # 2. hierarchy_node (bulk per hierarchy, idempotent)
        for hid, nd in nodes.items():
            codes = list(nd.keys())
            levels = [nd[c][0] for c in codes]
            descs = [nd[c][1] for c in codes]
            parents = [nd[c][2] for c in codes]
            conn.execute(
                "INSERT INTO hierarchy_node (hierarchy_id, code, level, description, parent_code) "
                "SELECT %s, c, l, d, p FROM UNNEST(%s::text[], %s::text[], %s::text[], %s::text[]) "
                "  AS t(c, l, d, p) "
                "ON CONFLICT (hierarchy_id, code) DO UPDATE "
                "  SET description=EXCLUDED.description, level=EXCLUDED.level, parent_code=EXCLUDED.parent_code",
                (hid, codes, levels, descs, parents),
            )

        # 3. resolve external_id -> item_id
        ext2id = {
            row[1]: row[0]
            for row in conn.execute("SELECT item_id, external_id FROM items WHERE external_id IS NOT NULL").fetchall()
        }

        # 4. item_cost (CDS) bulk update
        ce = [e for e, _ in costs if e in ext2id]
        cv = [c for e, c in costs if e in ext2id]
        conn.execute(
            "UPDATE items i SET item_cost = d.cost, updated_at = now() "
            "FROM (SELECT * FROM UNNEST(%s::text[], %s::numeric[]) AS t(ext, cost)) d "
            "WHERE i.external_id = d.ext",
            (ce, cv),
        )
        n_cost = len(ce)

        # 5. item_hierarchy links (resolve to item_id; skip unresolved)
        iid, hids, leaves = [], [], []
        n_unresolved = 0
        for ext, hid, leaf in item_leaf:
            uid = ext2id.get(ext)
            if uid is None:
                n_unresolved += 1
                continue
            iid.append(uid)
            hids.append(hid)
            leaves.append(leaf)
        conn.execute(
            "INSERT INTO item_hierarchy (item_id, hierarchy_id, leaf_code) "
            "SELECT * FROM UNNEST(%s::uuid[], %s::text[], %s::text[]) "
            "ON CONFLICT (item_id, hierarchy_id) DO UPDATE SET leaf_code=EXCLUDED.leaf_code",
            (iid, hids, leaves),
        )

        conn.commit()

    print(f"[load] item_cost set={n_cost} | item_hierarchy links={len(iid)} "
          f"(unresolved item codes skipped={n_unresolved})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
