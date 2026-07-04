"""
locations.py — generate the 3 DC + 11 plant network.

Locations are pre-defined in `config.DEFAULT_LOCATIONS` so the geography is
stable across runs. The generator simply allocates UUIDs and inserts them.
"""
from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID, uuid4

from ootils_core.db.types import DictRowConnection
from ootils_core.seed.config import Profile


@dataclass(frozen=True)
class LocationRecord:
    location_id: UUID
    name: str
    location_type: str
    country: str
    timezone: str


@dataclass
class LocationSet:
    records: list[LocationRecord]

    def dcs(self) -> list[LocationRecord]:
        return [r for r in self.records if r.location_type == "dc"]

    def plants(self) -> list[LocationRecord]:
        return [r for r in self.records if r.location_type == "plant"]

    def by_country(self, country: str) -> list[LocationRecord]:
        return [r for r in self.records if r.country == country]

    @property
    def total(self) -> int:
        return len(self.records)


def generate_locations(profile: Profile) -> LocationSet:
    """Materialise the profile's LocationSpecs into LocationRecords (UUIDs assigned)."""
    records = [
        LocationRecord(
            location_id=uuid4(),
            name=spec.name,
            location_type=spec.location_type,
            country=spec.country,
            timezone=spec.timezone,
        )
        for spec in profile.locations
    ]
    return LocationSet(records=records)


def insert_locations(conn: DictRowConnection, loc_set: LocationSet) -> int:
    """Bulk-insert all locations via UNNEST. Returns rowcount."""
    ids = [r.location_id for r in loc_set.records]
    names = [r.name for r in loc_set.records]
    types = [r.location_type for r in loc_set.records]
    countries = [r.country for r in loc_set.records]
    timezones = [r.timezone for r in loc_set.records]
    cur = conn.execute(
        """
        INSERT INTO locations (location_id, name, location_type, country, timezone)
        SELECT * FROM UNNEST(%s::uuid[], %s::text[], %s::text[], %s::text[], %s::text[])
        """,
        (ids, names, types, countries, timezones),
    )
    return cur.rowcount or 0
