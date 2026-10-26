"""Registry of upstream datasets and of the normalized tables they feed (the data contract of Plan.md section 5)."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from .elia import ELIA_ATTRIBUTION, ELIA_LICENCE, ELIA_LICENCE_URL
from .energy_charts import ENERGY_CHARTS_ATTRIBUTION, ENERGY_CHARTS_LICENCE, ENERGY_CHARTS_LICENCE_URL
from .regelleistung import REGELLEISTUNG_ATTRIBUTION, REGELLEISTUNG_LICENCE, REGELLEISTUNG_TERMS_URL

# First minute published after the MARI local go-live (2024-05-22 00:00 Europe/Brussels).
MARI_GO_LIVE_UTC = pd.Timestamp("2024-05-21T22:00:00Z")


@dataclass(frozen=True)
class UpstreamDataset:
    """One upstream dataset (an Elia ODS id, an Energy-Charts endpoint, a regelleistung.net file family)."""

    dataset_id: str
    source: str
    title: str
    time_field: str
    chunk: str
    licence: str
    licence_url: str
    attribution: str
    redistributable: bool = True
    notes: str = ""
    frozen_after: pd.Timestamp | None = None


ELIA_DATASETS: dict[str, UpstreamDataset] = {
    d.dataset_id: d
    for d in [
        UpstreamDataset(
            "ods133",
            "elia",
            "Imbalance price per minute (historical, as of 22/05/2024)",
            "datetime",
            "month",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
            notes="Cumulative quarter-hour-to-date values; the minute labelled hh:mm covers events up to hh:mm:59.",
        ),
        UpstreamDataset(
            "ods126",
            "elia",
            "Current system imbalance (historical, as of 22/05/2024)",
            "datetime",
            "month",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
            notes="Instantaneous minute values of SI, ACE and activated volumes.",
        ),
        UpstreamDataset(
            "ods165",
            "elia",
            "Balancing energy price components per minute (historical, as of 22/05/2024)",
            "datetime",
            "month",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
        ),
        UpstreamDataset(
            "ods134",
            "elia",
            "Imbalance prices per quarter-hour (historical, as of 22/05/2024)",
            "datetime",
            "year",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
        ),
        UpstreamDataset(
            "ods166",
            "elia",
            "Balancing energy price components per quarter-hour (historical, as of 22/05/2024)",
            "datetime",
            "year",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
        ),
        UpstreamDataset(
            "ods127",
            "elia",
            "Balancing energy volume components per quarter-hour (historical, as of 22/05/2024)",
            "datetime",
            "year",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
        ),
        UpstreamDataset(
            "ods211",
            "elia",
            "[Simulated] Imbalance prices per minute, new 2026 formula (Jan-Jul 2026; removed by Elia on 17/11/2026)",
            "datetime",
            "all",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
            notes="Best-effort, non-validated simulation published by Elia ahead of the 24/11/2026 go-live.",
            frozen_after=pd.Timestamp("2026-08-01", tz="UTC"),
        ),
        UpstreamDataset(
            "ods212",
            "elia",
            "[Simulated] Imbalance prices per quarter-hour, new 2026 formula (Jan-Jul 2026; removed 17/11/2026)",
            "datetime",
            "all",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
            frozen_after=pd.Timestamp("2026-08-01", tz="UTC"),
        ),
        UpstreamDataset(
            "ods136",
            "elia",
            "System imbalance forecast, current quarter-hour",
            "predictiontimeutc",
            "month",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
        ),
        UpstreamDataset(
            "ods147",
            "elia",
            "System imbalance forecast, next quarter-hour",
            "predictiontimeutc",
            "month",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
        ),
        UpstreamDataset(
            "ods125",
            "elia",
            "Individual aFRR capacity bids (up to 19/05/2026)",
            "delivery_date",
            "year",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
        ),
        UpstreamDataset(
            "ods207",
            "elia",
            "Individual aFRR capacity bids (as of 20/05/2026)",
            "deliverydate",
            "year",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
        ),
        UpstreamDataset(
            "ods055",
            "elia",
            "Individual mFRR capacity bids (up to 19/05/2026)",
            "delivery_date",
            "year",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
        ),
        UpstreamDataset(
            "ods206",
            "elia",
            "Individual mFRR capacity bids (as of 20/05/2026)",
            "deliverydate",
            "year",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
        ),
        UpstreamDataset(
            "ods001",
            "elia",
            "Measured and forecasted total load on the Belgian grid (historical)",
            "datetime",
            "year",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
        ),
        UpstreamDataset(
            "ods031",
            "elia",
            "Wind power production estimation and forecast on the Belgian grid (historical)",
            "datetime",
            "year",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
        ),
        UpstreamDataset(
            "ods032",
            "elia",
            "Photovoltaic power production estimation and forecast on the Belgian grid (historical)",
            "datetime",
            "year",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
            notes="Elia announced the move of PV history to ods209 (go-live 26/10/2026 10:00 CET).",
        ),
        UpstreamDataset(
            "ods156",
            "elia",
            "Individual incremental balancing energy bids (historical, as of 22/05/2024)",
            "datetime",
            "month",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
            notes="Rolling window (about the last 20 months); raw rows are not redistributed, only curves.",
        ),
        UpstreamDataset(
            "ods157",
            "elia",
            "Individual decremental balancing energy bids (historical, as of 22/05/2024)",
            "datetime",
            "month",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
            notes="Rolling window; raw rows are not redistributed, only curves.",
        ),
        UpstreamDataset(
            "ods047",
            "elia",
            "Imbalance prices per quarter-hour (historical, up to 21/05/2024)",
            "datetime",
            "year",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
        ),
        UpstreamDataset(
            "ods046",
            "elia",
            "Imbalance prices per minute (historical, up to 21/05/2024)",
            "datetime",
            "month",
            ELIA_LICENCE,
            ELIA_LICENCE_URL,
            ELIA_ATTRIBUTION,
        ),
    ]
}

OTHER_DATASETS: dict[str, UpstreamDataset] = {
    "energy_charts_price": UpstreamDataset(
        "energy_charts_price",
        "energy_charts",
        "Energy-Charts day-ahead auction prices (/price endpoint)",
        "unix_seconds",
        "month",
        ENERGY_CHARTS_LICENCE,
        ENERGY_CHARTS_LICENCE_URL,
        ENERGY_CHARTS_ATTRIBUTION,
    ),
    "regelleistung_capacity": UpstreamDataset(
        "regelleistung_capacity",
        "regelleistung",
        "regelleistung.net FCR / aFRR capacity auction results and demands",
        "deliveryDate",
        "day",
        REGELLEISTUNG_LICENCE,
        REGELLEISTUNG_TERMS_URL,
        REGELLEISTUNG_ATTRIBUTION,
        redistributable=False,
    ),
}


@dataclass(frozen=True)
class TableSpec:
    """A normalized output table (one file or one file per UTC year)."""

    key: str
    pattern: str
    time_column: str
    fetch_target: str
    partition: str = "none"  # "none" | "year"
    first_year: int | None = None
    sources: tuple[str, ...] = field(default_factory=tuple)
    redistributed: bool = True
    description: str = ""

    def relpath(self, year: int | None = None) -> str:
        if self.partition == "year":
            if year is None:
                raise ValueError(f"{self.key} is partitioned by year; pass `year`")
            return self.pattern.format(year=year)
        return self.pattern

    def fetch_command(self, year: int | None = None) -> str:
        base = f"euroflex data fetch {self.fetch_target}"
        if self.partition == "year" and year is not None:
            return f"{base} --start {year}-01-01 --end {year + 1}-01-01"
        return base


TABLES: dict[str, TableSpec] = {
    t.key: t
    for t in [
        TableSpec(
            "belgium_minute",
            "belgium/minute_{year}.parquet",
            "timestamp_utc",
            "belgium-minute",
            "year",
            2024,
            ("ods133", "ods126", "ods165"),
            description="Per-minute imbalance price (cumulative QH-to-date), SI/ACE/volumes and price components.",
        ),
        TableSpec(
            "belgium_qh",
            "belgium/qh_{year}.parquet",
            "timestamp_utc",
            "belgium-qh",
            "year",
            2024,
            ("ods134", "ods166", "ods127"),
            description="Per-quarter-hour final imbalance prices, price components and volumes.",
        ),
        TableSpec(
            "belgium_new_formula_minute",
            "belgium/new_formula_minute_2026.parquet",
            "timestamp_utc",
            "belgium-new-formula",
            sources=("ods211",),
            description="Elia simulation of the new 2026 imbalance-price formula, per minute (Jan-Jul 2026).",
        ),
        TableSpec(
            "belgium_new_formula_qh",
            "belgium/new_formula_qh_2026.parquet",
            "timestamp_utc",
            "belgium-new-formula",
            sources=("ods212",),
            description="Elia simulation of the new 2026 imbalance-price formula, per quarter-hour (Jan-Jul 2026).",
        ),
        TableSpec(
            "belgium_si_forecast_current",
            "belgium/si_forecast_current_qh_{year}.parquet",
            "prediction_time_utc",
            "belgium-si-forecast",
            "year",
            2024,
            ("ods136",),
            description="Elia SI forecast for the current quarter-hour, issued every minute.",
        ),
        TableSpec(
            "belgium_si_forecast_next",
            "belgium/si_forecast_next_qh_{year}.parquet",
            "prediction_time_utc",
            "belgium-si-forecast",
            "year",
            2024,
            ("ods147",),
            description="Elia SI forecast for the next quarter-hour, issued every minute.",
        ),
        TableSpec(
            "belgium_afrr_capacity_awards",
            "belgium/afrr_capacity_awards.parquet",
            "delivery_start_utc",
            "belgium-capacity-awards",
            sources=("ods125", "ods207"),
            description="Belgian aFRR capacity auction awards aggregated per delivery block and direction.",
        ),
        TableSpec(
            "belgium_mfrr_capacity_awards",
            "belgium/mfrr_capacity_awards.parquet",
            "delivery_start_utc",
            "belgium-capacity-awards",
            sources=("ods055", "ods206"),
            description="Belgian mFRR capacity auction awards aggregated per delivery block and direction.",
        ),
        TableSpec(
            "belgium_res_load",
            "belgium/res_load_qh_{year}.parquet",
            "timestamp_utc",
            "belgium-res-load",
            "year",
            2023,
            ("ods001", "ods031", "ods032"),
            description="Belgian load, wind and PV measured + forecast per quarter-hour.",
        ),
        TableSpec(
            "belgium_energy_bid_curves",
            "belgium/energy_bid_curves_qh_{year}.parquet",
            "timestamp_utc",
            "belgium-bid-curves",
            "year",
            2025,
            ("ods156", "ods157"),
            description="Merit-order curves of available aFRR/mFRR energy bids per quarter-hour on a fixed MW grid.",
        ),
        TableSpec(
            "da_prices_BE",
            "energy_charts/da_prices_BE.parquet",
            "timestamp_utc",
            "day-ahead --zone BE",
            sources=("energy_charts_price",),
            description="Belgian day-ahead auction prices (hourly until 2025-09-30, 15-minute afterwards).",
        ),
        TableSpec(
            "da_prices_DE-LU",
            "energy_charts/da_prices_DE-LU.parquet",
            "timestamp_utc",
            "day-ahead --zone DE-LU",
            sources=("energy_charts_price",),
            description="German-Luxembourg day-ahead auction prices (hourly until 2025-09-30, 15-minute afterwards).",
        ),
        TableSpec(
            "germany_fcr_capacity",
            "germany/fcr_capacity.parquet",
            "delivery_start_utc",
            "germany-reserve --product fcr",
            sources=("regelleistung_capacity",),
            redistributed=False,
            description="German FCR capacity auction results (4-hour blocks).",
        ),
        TableSpec(
            "germany_afrr_capacity",
            "germany/afrr_capacity.parquet",
            "delivery_start_utc",
            "germany-reserve --product afrr",
            sources=("regelleistung_capacity",),
            redistributed=False,
            description="German aFRR capacity auction results (4-hour blocks, up/down).",
        ),
        TableSpec(
            "belgium_qh_legacy",
            "belgium/qh_{year}_legacy.parquet",
            "timestamp_utc",
            "belgium-legacy",
            "year",
            2019,
            ("ods047",),
            description="Pre-MARI quarter-hour imbalance prices (before 2024-05-22), legacy layout.",
        ),
        TableSpec(
            "belgium_minute_legacy",
            "belgium/minute_{year}_legacy.parquet",
            "timestamp_utc",
            "belgium-legacy",
            "year",
            2023,
            ("ods046",),
            description="Pre-MARI per-minute imbalance prices (2023-01 to 2024-05-21), legacy layout.",
        ),
    ]
}


def upstream(dataset_id: str) -> UpstreamDataset:
    if dataset_id in ELIA_DATASETS:
        return ELIA_DATASETS[dataset_id]
    if dataset_id in OTHER_DATASETS:
        return OTHER_DATASETS[dataset_id]
    raise KeyError(f"Unknown upstream dataset {dataset_id!r}")
