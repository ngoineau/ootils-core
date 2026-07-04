"""
suppliers.py — supplier generator.

Distributes ~50 suppliers across a geographic mix tuned for discrete-manuf:
offshore (CN/VN/IN) for low-cost raw materials with long lead times, on-shore
(DE/FR/US) for higher-cost / shorter-lead reliable supply, plus EU-east
(CZ/PL) for medium tier.

Each supplier gets a country-derived lead_time_days and reliability_score
drawn from the country's range in SupplierMix. The actual per-item lead
time will be set later when supplier_items are linked (step 3 / network).
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from uuid import UUID, uuid4

from ootils_core.db.types import DictRowConnection
from ootils_core.seed.config import Profile


# Realistic-ish company name shapes per country. Kept simple to avoid
# leaking real company names into test data.
_NAME_SHAPES: dict[str, tuple[str, ...]] = {
    "DE": ("Müller GmbH", "Schmidt Werke", "Bauer KG", "Klein AG", "Weiss Industries",
           "Hofmann GmbH", "Wagner Technik", "Schmitt KG", "Becker AG"),
    "FR": ("Société Lambert", "Établissements Martin", "Groupe Petit",
           "Industries Robert", "Atelier Durand", "Mécanique Bernard"),
    "IT": ("Fonderia Ricci", "Officine Conti", "Industrie Romano",
           "Componenti Greco", "Meccanica Bruno", "Forgia Esposito"),
    "PL": ("Zaklady Kowalski", "Produkcja Nowak", "Przemysl Wojcik"),
    "CZ": ("Vyroba Novak", "Strojirny Svoboda"),
    "US": ("Acme Industries", "Pioneer Metals", "Beacon Components",
           "Liberty Manufacturing", "Continental Parts", "Apex Forge"),
    "MX": ("Industrias Reyes", "Maquinaria Hernandez", "Fundición Garcia"),
    "CN": ("Shenzhen Electronics", "Suzhou Precision", "Guangzhou Industrial",
           "Wuxi Components", "Ningbo Plastics", "Dongguan Metalworks",
           "Hangzhou Tech", "Tianjin Steel", "Xiamen Castings",
           "Foshan Hardware", "Qingdao Trading", "Zhuhai Manufacturing"),
    "VN": ("Hanoi Components", "Saigon Industries", "Hai Phong Steel"),
    "IN": ("Mumbai Castings", "Bangalore Components", "Pune Industries"),
    "JP": ("Tanaka Seiko", "Suzuki Industries"),
}


@dataclass(frozen=True)
class SupplierRecord:
    supplier_id: UUID
    external_id: str         # e.g. "SUP-CN-0007" — referenced from ERP exports
    name: str
    country: str
    lead_time_days: int      # baseline lead time; per-item lt may differ
    reliability_score: float  # 0..1, lower for offshore
    status: str              # "active" / "inactive" / "blocked"


@dataclass
class SupplierSet:
    records: list[SupplierRecord]

    def by_country(self, country: str) -> list[SupplierRecord]:
        return [r for r in self.records if r.country == country]

    def active(self) -> list[SupplierRecord]:
        return [r for r in self.records if r.status == "active"]

    @property
    def total(self) -> int:
        return len(self.records)


def _make_name(country: str, idx: int, rng: random.Random) -> str:
    """Build a plausible supplier name. Falls back to '<country> Supplier-NN'."""
    shapes = _NAME_SHAPES.get(country)
    if not shapes:
        return f"{country} Supplier-{idx:02d}"
    return rng.choice(shapes)


def generate_suppliers(profile: Profile) -> SupplierSet:
    """Build the supplier list in memory. No DB access.

    Uses the profile RNG seed offset by a constant so this generator's
    randomness is independent of items.generate_items'. Same seed +
    same profile -> same supplier set across runs.
    """
    rng = random.Random(profile.seed + 1001)
    used_names: set[str] = set()
    records: list[SupplierRecord] = []
    global_idx = 1

    for country, (n_suppliers, (lt_min, lt_max), (rel_min, rel_max)) in \
            profile.supplier_mix.distribution.items():
        for k in range(n_suppliers):
            # Try a few names to avoid duplicates within a country
            for _ in range(5):
                name = _make_name(country, k + 1, rng)
                if name not in used_names:
                    break
            else:
                name = f"{country} Supplier-{k + 1:02d}"
            used_names.add(name)

            # ~3% blocked / inactive across the population — exercises filter paths
            roll = rng.random()
            if roll < 0.02:
                status = "blocked"
            elif roll < 0.05:
                status = "inactive"
            else:
                status = "active"

            records.append(SupplierRecord(
                supplier_id=uuid4(),
                external_id=f"SUP-{country}-{global_idx:04d}",
                name=name,
                country=country,
                lead_time_days=rng.randint(lt_min, lt_max),
                reliability_score=round(rng.uniform(rel_min, rel_max), 3),
                status=status,
            ))
            global_idx += 1

    return SupplierSet(records=records)


def insert_suppliers(conn: DictRowConnection, sup_set: SupplierSet) -> int:
    """Bulk-insert suppliers via UNNEST. Returns rowcount."""
    ids = [r.supplier_id for r in sup_set.records]
    ext_ids = [r.external_id for r in sup_set.records]
    names = [r.name for r in sup_set.records]
    countries = [r.country for r in sup_set.records]
    lts = [r.lead_time_days for r in sup_set.records]
    rels = [r.reliability_score for r in sup_set.records]
    statuses = [r.status for r in sup_set.records]
    cur = conn.execute(
        """
        INSERT INTO suppliers
            (supplier_id, external_id, name, country, lead_time_days, reliability_score, status)
        SELECT * FROM UNNEST(
            %s::uuid[], %s::text[], %s::text[], %s::text[],
            %s::int[], %s::numeric[], %s::text[]
        )
        """,
        (ids, ext_ids, names, countries, lts, rels, statuses),
    )
    return cur.rowcount or 0
