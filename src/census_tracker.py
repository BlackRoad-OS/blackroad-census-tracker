#!/usr/bin/env python3
"""BlackRoad Census Tracker - population data collection and analysis."""

from __future__ import annotations
import argparse
import json
import sqlite3
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

GREEN = "\033[0;32m"
RED = "\033[0;31m"
CYAN = "\033[0;36m"
YELLOW = "\033[1;33m"
BLUE = "\033[0;34m"
BOLD = "\033[1m"
NC = "\033[0m"

DB_PATH = Path.home() / ".blackroad" / "census-tracker.db"


@dataclass
class District:
    id: int
    name: str
    region: str
    area_sqkm: float
    district_type: str
    created_at: str


@dataclass
class CensusRecord:
    id: int
    district_id: int
    year: int
    population: int
    households: int
    avg_age: float
    median_income: float
    unemployment_rate: float
    collected_at: str
    notes: str


@dataclass
class PopulationSummary:
    district_name: str
    region: str
    latest_year: int
    population: int
    households: int
    density_per_sqkm: float
    avg_age: float
    median_income: float
    unemployment_rate: float
    yoy_growth: float


class CensusTracker:
    """Population census data collection and analysis engine."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS districts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    region TEXT DEFAULT 'unknown',
                    area_sqkm REAL DEFAULT 0,
                    district_type TEXT DEFAULT 'urban',
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS census_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    district_id INTEGER NOT NULL REFERENCES districts(id),
                    year INTEGER NOT NULL,
                    population INTEGER DEFAULT 0,
                    households INTEGER DEFAULT 0,
                    avg_age REAL DEFAULT 0,
                    median_income REAL DEFAULT 0,
                    unemployment_rate REAL DEFAULT 0,
                    collected_at TEXT NOT NULL,
                    notes TEXT DEFAULT '',
                    UNIQUE(district_id, year)
                );
                CREATE INDEX IF NOT EXISTS idx_census_district
                    ON census_records(district_id, year);
            """)

    def add_district(self, name: str, region: str = "unknown",
                     area_sqkm: float = 0, district_type: str = "urban") -> District:
        """Register a census district."""
        with sqlite3.connect(self.db_path) as conn:
            now = datetime.now().isoformat()
            cur = conn.execute(
                "INSERT INTO districts (name,region,area_sqkm,district_type,created_at)"
                " VALUES (?,?,?,?,?)",
                (name, region, area_sqkm, district_type, now),
            )
            return District(cur.lastrowid, name, region, area_sqkm, district_type, now)

    def record_census(self, district_name: str, year: int, population: int,
                      households: int = 0, avg_age: float = 0.0,
                      median_income: float = 0.0, unemployment_rate: float = 0.0,
                      notes: str = "") -> CensusRecord:
        """Record census data for a district and year."""
        with sqlite3.connect(self.db_path) as conn:
            d = conn.execute(
                "SELECT id FROM districts WHERE name=?", (district_name,)
            ).fetchone()
            if not d:
                raise ValueError(f"District '{district_name}' not found")
            now = datetime.now().isoformat()
            cur = conn.execute(
                "INSERT OR REPLACE INTO census_records"
                " (district_id,year,population,households,avg_age,"
                "  median_income,unemployment_rate,collected_at,notes)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (d[0], year, population, households, avg_age,
                 median_income, unemployment_rate, now, notes),
            )
            return CensusRecord(cur.lastrowid, d[0], year, population, households,
                                avg_age, median_income, unemployment_rate, now, notes)

    def list_districts(self, region: str = None) -> list:
        """Return districts optionally filtered by region."""
        with sqlite3.connect(self.db_path) as conn:
            if region:
                rows = conn.execute(
                    "SELECT * FROM districts WHERE region=?", (region,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM districts").fetchall()
            return [District(*r) for r in rows]

    def get_summary(self, district_name: str) -> PopulationSummary:
        """Build a population summary with YoY growth for the latest census year."""
        with sqlite3.connect(self.db_path) as conn:
            d = conn.execute(
                "SELECT * FROM districts WHERE name=?", (district_name,)
            ).fetchone()
            if not d:
                raise ValueError(f"District '{district_name}' not found")
            district = District(*d)
            rows = conn.execute(
                "SELECT * FROM census_records WHERE district_id=? ORDER BY year DESC LIMIT 2",
                (district.id,),
            ).fetchall()

        if not rows:
            raise ValueError(f"No census data for '{district_name}'")

        latest = CensusRecord(*rows[0])
        density = (latest.population / district.area_sqkm) if district.area_sqkm > 0 else 0
        yoy = 0.0
        if len(rows) > 1:
            prev = CensusRecord(*rows[1])
            if prev.population > 0:
                yoy = round((latest.population - prev.population) / prev.population * 100, 2)

        return PopulationSummary(
            district.name, district.region, latest.year, latest.population,
            latest.households, round(density, 1), latest.avg_age,
            latest.median_income, latest.unemployment_rate, yoy,
        )

    def regional_report(self, region: str) -> dict:
        """Aggregate census stats across all districts in a region."""
        districts = self.list_districts(region)
        if not districts:
            return {"region": region, "districts": 0}
        total_pop = 0
        total_hh = 0
        count = 0
        for d in districts:
            try:
                s = self.get_summary(d.name)
                total_pop += s.population
                total_hh += s.households
                count += 1
            except ValueError:
                continue
        return {
            "region": region,
            "districts_with_data": count,
            "total_population": total_pop,
            "total_households": total_hh,
            "avg_household_size": round(total_pop / total_hh, 2) if total_hh else 0,
        }

    def status(self) -> dict:
        """High-level statistics."""
        with sqlite3.connect(self.db_path) as conn:
            districts = conn.execute("SELECT COUNT(*) FROM districts").fetchone()[0]
            records = conn.execute("SELECT COUNT(*) FROM census_records").fetchone()[0]
            years = conn.execute(
                "SELECT MIN(year), MAX(year) FROM census_records"
            ).fetchone()
        return {
            "districts": districts,
            "census_records": records,
            "year_range": f"{years[0]}–{years[1]}" if years[0] else "none",
            "db_path": str(self.db_path),
        }

    def export_data(self) -> dict:
        """Full data export."""
        with sqlite3.connect(self.db_path) as conn:
            districts = [District(*r) for r in conn.execute("SELECT * FROM districts").fetchall()]
            records = [CensusRecord(*r)
                       for r in conn.execute("SELECT * FROM census_records").fetchall()]
        return {
            "districts": [asdict(d) for d in districts],
            "census_records": [asdict(r) for r in records],
            "exported_at": datetime.now().isoformat(),
        }


def _fmt_district(d: District) -> None:
    print(f"  {CYAN}[{d.id}]{NC} {BOLD}{d.name}{NC}  region={YELLOW}{d.region}{NC}"
          f"  area={d.area_sqkm:.1f}km²  type={d.district_type}")


def _fmt_summary(s: PopulationSummary) -> None:
    yoy_col = GREEN if s.yoy_growth >= 0 else RED
    print(f"  {BOLD}{s.district_name}{NC} ({s.region})  year={s.latest_year}")
    print(f"    population={GREEN}{s.population:,}{NC}  households={s.households:,}")
    print(f"    density={s.density_per_sqkm}/km²  avg_age={s.avg_age:.1f}")
    print(f"    income=${s.median_income:,.0f}  unemployment={s.unemployment_rate:.1f}%"
          f"  yoy={yoy_col}{s.yoy_growth:+.2f}%{NC}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="census_tracker",
        description=f"{BOLD}BlackRoad Census Tracker{NC}",
    )
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("status", help="System status")
    sub.add_parser("export", help="Export all data as JSON")

    ls = sub.add_parser("list", help="List districts")
    ls.add_argument("--region", default=None)

    ad = sub.add_parser("add-district", help="Register a district")
    ad.add_argument("name")
    ad.add_argument("--region", default="unknown")
    ad.add_argument("--area", type=float, default=0.0, dest="area_sqkm")
    ad.add_argument("--type", dest="district_type", default="urban")

    rc = sub.add_parser("record", help="Record census data")
    rc.add_argument("district")
    rc.add_argument("year", type=int)
    rc.add_argument("population", type=int)
    rc.add_argument("--households", type=int, default=0)
    rc.add_argument("--avg-age", type=float, default=0.0)
    rc.add_argument("--income", type=float, default=0.0)
    rc.add_argument("--unemployment", type=float, default=0.0)
    rc.add_argument("--notes", default="")

    sm = sub.add_parser("summary", help="Population summary for a district")
    sm.add_argument("district")

    rr = sub.add_parser("region", help="Regional aggregate report")
    rr.add_argument("region_name")

    args = parser.parse_args()
    ct = CensusTracker()

    if args.cmd == "list":
        districts = ct.list_districts(args.region)
        label = f"region={args.region}" if args.region else "all regions"
        print(f"\n{BOLD}{BLUE}Districts ({len(districts)}) — {label}{NC}")
        [_fmt_district(d) for d in districts] or print(f"  {YELLOW}none{NC}")

    elif args.cmd == "add-district":
        d = ct.add_district(args.name, args.region, args.area_sqkm, args.district_type)
        print(f"{GREEN}✓{NC} District {BOLD}{d.name}{NC} registered (id={d.id})")

    elif args.cmd == "record":
        r = ct.record_census(args.district, args.year, args.population,
                             args.households, args.avg_age, args.income,
                             args.unemployment, args.notes)
        print(f"{GREEN}✓{NC} Census recorded (id={r.id}) {r.year} pop={r.population:,}")

    elif args.cmd == "summary":
        s = ct.get_summary(args.district)
        print(f"\n{BOLD}{BLUE}Population Summary{NC}")
        _fmt_summary(s)

    elif args.cmd == "region":
        rpt = ct.regional_report(args.region_name)
        print(f"\n{BOLD}{BLUE}Regional Report — {args.region_name}{NC}")
        for k, v in rpt.items():
            print(f"  {CYAN}{k}{NC}: {v}")

    elif args.cmd == "status":
        st = ct.status()
        print(f"\n{BOLD}{BLUE}Census Tracker Status{NC}")
        for k, v in st.items():
            print(f"  {CYAN}{k}{NC}: {GREEN}{v}{NC}")

    elif args.cmd == "export":
        print(json.dumps(ct.export_data(), indent=2))

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
