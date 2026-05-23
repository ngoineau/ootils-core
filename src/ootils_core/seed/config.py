"""
config.py — profile dataclasses for the realistic dataset generator.

Each profile (S, M, L) pins down volumes, distributions, and time horizons
so the generator is fully deterministic given (profile, seed). Changes here
should be reviewed because they shift the realism vs. iteration-speed
trade-off.
"""
from __future__ import annotations

from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Item pyramid — discrete-manufacturing 5-level shape
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ItemPyramid:
    """How many items at each BOM level + their item_type mapping.

    The schema (migrations 002) has only 4 item_types but the BOM graph is
    5 levels deep. Levels 2 and 3 share the 'component' type — they differ
    by where they sit in the BOM, not by category.
    """
    fg: int = 500           # L0 finished goods         -> 'finished_good'
    sub_assembly: int = 900 # L1 sub-assemblies         -> 'semi_finished'
    component: int = 1200   # L2 components             -> 'component'
    part: int = 1300        # L3 parts                  -> 'component'
    raw_material: int = 1100  # L4 raw materials        -> 'raw_material'

    @property
    def total(self) -> int:
        return self.fg + self.sub_assembly + self.component + self.part + self.raw_material


@dataclass(frozen=True)
class StatusDistribution:
    """Lifecycle status mix on items. Phase-out / obsolete exercise temporal logic."""
    active_pct: float = 0.85
    phase_out_pct: float = 0.10
    obsolete_pct: float = 0.05

    def __post_init__(self) -> None:
        total = self.active_pct + self.phase_out_pct + self.obsolete_pct
        assert abs(total - 1.0) < 1e-9, f"status percentages must sum to 1, got {total}"


@dataclass(frozen=True)
class UomMix:
    """UoM distribution per item type (rough discrete-manuf priors)."""
    fg_uom: tuple[str, ...] = ("EA",)                       # 100% EA
    sub_assembly_uom: tuple[str, ...] = ("EA",)             # 100% EA
    component_uom: tuple[str, ...] = ("EA",)                # 100% EA
    part_uom: tuple[str, ...] = ("EA", "EA", "EA", "M")     # 75% EA, 25% M (cables)
    raw_uom: tuple[str, ...] = ("KG", "KG", "L", "M", "EA") # mix metal/plastic/cable/liquid/sheet


# ---------------------------------------------------------------------------
# Network — locations and suppliers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LocationSpec:
    """One location entry — name, type, country, timezone."""
    name: str
    location_type: str   # 'dc' or 'plant'
    country: str
    timezone: str


# A representative discrete-manuf footprint: 3 DCs across NA/EU + 11 plants
# spread across a mix of low-cost and on-shore countries. Names are stable
# so tests can refer to them by name if needed.
DEFAULT_LOCATIONS: tuple[LocationSpec, ...] = (
    # Distribution centers
    LocationSpec("DC-USA-East",  "dc",    "US", "America/New_York"),
    LocationSpec("DC-USA-West",  "dc",    "US", "America/Los_Angeles"),
    LocationSpec("DC-EU-Central","dc",    "DE", "Europe/Berlin"),
    # Plants — discrete manuf often has 1 anchor in HQ country + offshoring
    LocationSpec("PL-DE-Munich",   "plant", "DE", "Europe/Berlin"),
    LocationSpec("PL-DE-Stuttgart","plant", "DE", "Europe/Berlin"),
    LocationSpec("PL-FR-Lyon",     "plant", "FR", "Europe/Paris"),
    LocationSpec("PL-CZ-Brno",     "plant", "CZ", "Europe/Prague"),
    LocationSpec("PL-PL-Wroclaw",  "plant", "PL", "Europe/Warsaw"),
    LocationSpec("PL-MX-Monterrey","plant", "MX", "America/Monterrey"),
    LocationSpec("PL-US-Detroit",  "plant", "US", "America/Detroit"),
    LocationSpec("PL-CN-Shenzhen", "plant", "CN", "Asia/Shanghai"),
    LocationSpec("PL-CN-Suzhou",   "plant", "CN", "Asia/Shanghai"),
    LocationSpec("PL-IN-Pune",     "plant", "IN", "Asia/Kolkata"),
    LocationSpec("PL-VN-Hanoi",    "plant", "VN", "Asia/Ho_Chi_Minh"),
)


@dataclass(frozen=True)
class SupplierMix:
    """Geographic distribution of suppliers. Lead times derive from country."""
    # Country -> (n_suppliers, lead_time_days_range_(min,max), reliability_range)
    distribution: dict[str, tuple[int, tuple[int, int], tuple[float, float]]] = field(
        default_factory=lambda: {
            "DE": (8,  (5,  20),  (0.92, 0.99)),
            "FR": (4,  (5,  20),  (0.90, 0.98)),
            "IT": (4,  (10, 25),  (0.85, 0.95)),
            "PL": (3,  (5,  20),  (0.88, 0.96)),
            "CZ": (2,  (5,  15),  (0.90, 0.97)),
            "US": (6,  (10, 30),  (0.92, 0.99)),
            "MX": (3,  (15, 35),  (0.85, 0.95)),
            "CN": (12, (30, 75),  (0.78, 0.93)),  # offshore — long but cheap
            "VN": (3,  (30, 70),  (0.78, 0.92)),
            "IN": (3,  (30, 70),  (0.80, 0.94)),
            "JP": (2,  (15, 35),  (0.95, 0.99)),
        }
    )

    def total_suppliers(self) -> int:
        return sum(n for n, _, _ in self.distribution.values())


# ---------------------------------------------------------------------------
# Top-level profiles
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Profile:
    """A named bundle of all generator inputs.

    The generator is fully deterministic given (profile, seed). Use the same
    pair for reproducible perf bench fixtures.
    """
    name: str
    pyramid: ItemPyramid
    status_dist: StatusDistribution
    uom_mix: UomMix
    locations: tuple[LocationSpec, ...]
    supplier_mix: SupplierMix
    # Time horizon (forward + back). Drives PI bucket count and order history.
    horizon_days_forward: int
    horizon_days_back: int
    # Shortage target — what fraction of PIs we aim to put in stockout
    # after propagation. The on-hand calibration step uses this.
    target_shortage_pct: float
    # RNG seed — pass through to generators for byte-identical reruns.
    seed: int = 20260523


PROFILE_S = Profile(
    name="S",
    pyramid=ItemPyramid(fg=200, sub_assembly=350, component=450, part=500, raw_material=400),
    status_dist=StatusDistribution(),
    uom_mix=UomMix(),
    locations=DEFAULT_LOCATIONS[:6],  # 2 DC + 4 plants
    supplier_mix=SupplierMix(distribution={
        "DE": (3, (5, 20),  (0.92, 0.99)),
        "FR": (2, (5, 20),  (0.90, 0.98)),
        "CN": (4, (30, 75), (0.78, 0.93)),
        "US": (3, (10, 30), (0.92, 0.99)),
    }),
    horizon_days_forward=90,
    horizon_days_back=90,
    target_shortage_pct=0.05,
)


PROFILE_M = Profile(
    name="M",
    pyramid=ItemPyramid(),  # Defaults: 5000 SKU pyramid as per design
    status_dist=StatusDistribution(),
    uom_mix=UomMix(),
    locations=DEFAULT_LOCATIONS,  # All 14
    supplier_mix=SupplierMix(),    # All 50 suppliers
    horizon_days_forward=365,
    horizon_days_back=365,
    target_shortage_pct=0.07,
)


PROFILES = {"S": PROFILE_S, "M": PROFILE_M}
