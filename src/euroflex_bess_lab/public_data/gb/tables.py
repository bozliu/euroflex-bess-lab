"""Registry of the normalized GB tables under ``data/public/gb`` (relative paths, time columns, fetch targets)."""

from __future__ import annotations

from dataclasses import dataclass

GB_START_UTC = "2023-12-01T00:00:00Z"  # one month of warm-up history before the 2024-01-01 report start
FETCH_SCRIPT = ".venv/bin/python scripts/research/gb_fetch_data.py"


@dataclass(frozen=True)
class GBTableSpec:
    key: str
    pattern: str  # relative to the data root; may contain ``{year}``
    time_column: str
    target: str  # fetch target that (re)builds the table
    title: str
    first_year: int = 2024

    @property
    def partitioned(self) -> bool:
        return "{year}" in self.pattern

    def relpath(self, year: int | None = None) -> str:
        if self.partitioned:
            if year is None:
                raise ValueError(f"{self.key} is partitioned by year; pass `year`")
            return self.pattern.format(year=year)
        return self.pattern

    def fetch_command(self) -> str:
        return f"euroflex data gb fetch {self.target}  (or: {FETCH_SCRIPT} --target {self.target})"


GB_TABLES: dict[str, GBTableSpec] = {
    spec.key: spec
    for spec in [
        GBTableSpec(
            "gb_system_prices",
            "gb/system_prices.parquet",
            "timestamp_utc",
            "system-prices",
            "GB single imbalance (cash-out) price and net imbalance volume per settlement period (BMRS DISEBSP).",
        ),
        GBTableSpec(
            "gb_market_index",
            "gb/market_index.parquet",
            "timestamp_utc",
            "market-index",
            "GB Market Index Data per settlement period (BMRS MID; APX/EPEX and N2EX providers).",
        ),
        GBTableSpec(
            "gb_response_auctions",
            "gb/response_auctions.parquet",
            "delivery_start_utc",
            "eac",
            "NESO EAC day-ahead clearing prices and volumes for DC/DM/DR low and high per EFA block.",
        ),
        GBTableSpec(
            "gb_reserve_auctions",
            "gb/reserve_auctions.parquet",
            "delivery_start_utc",
            "eac",
            "NESO EAC day-ahead clearing prices and volumes for Quick, Slow and Balancing Reserve per window.",
        ),
        GBTableSpec(
            "gb_frequency",
            "gb/frequency_15s_{year}.parquet",
            "timestamp_utc",
            "frequency",
            "GB system frequency at 15-second resolution (BMRS FREQ) as deviation from 50 Hz in mHz.",
        ),
        GBTableSpec(
            "gb_response_energy",
            "gb/response_energy.parquet",
            "timestamp_utc",
            "frequency",
            "Energy delivered per MW of contracted DC/DM/DR (low/high) per settlement period, from BMRS FREQ.",
        ),
        GBTableSpec(
            "gb_fx",
            "gb/ecb_gbp_per_eur.parquet",
            "timestamp_utc",
            "fx",
            "ECB euro foreign exchange reference rate, GBP per EUR (daily).",
        ),
    ]
}

FETCH_TARGETS: tuple[str, ...] = ("system-prices", "market-index", "eac", "frequency", "fx")
