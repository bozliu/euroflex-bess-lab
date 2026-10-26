"""Build the Nordic tables under ``data/public/nordic`` from the raw cache (``data/raw/nordic``).

Every builder is idempotent: closed raw chunks are reused, open ones are refreshed after 12 hours (see
``public_data.http.cached_download``), tables are cut at the snapshot boundary (Copenhagen midnight of the as-of
day) and written as zstd parquet with a ``.meta.json`` sidecar (source, URLs, licence, attribution, coverage,
validation, rows, sha256).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from ..http import HttpClient
from ..paths import data_root, raw_root
from ..validate import minute_grid_report
from ..writers import WrittenTable, coverage, write_table
from . import energinet, svk
from .normalize import (
    capacity_zone_coverage,
    dk_imbalance_rule_check,
    normalize_afrr,
    normalize_dk_imbalance,
    normalize_energinet_day_ahead,
    normalize_fcr_dk1,
    normalize_fcr_nordic,
    normalize_mfrr,
    normalize_svk_day_ahead,
)
from .sources import (
    AFRR_ZONES,
    DA_15MIN_GO_LIVE_UTC,
    DEFAULT_START_UTC,
    DK_AREAS,
    ENERGINET_ATTRIBUTION,
    ENERGINET_DA_ZONES,
    ENERGINET_DATASETS,
    ENERGINET_LICENCE,
    ENERGINET_LICENCE_URL,
    ENERGINET_SOURCE,
    ENERGINET_TERMS_URL,
    FCR_NORDIC_ZONES,
    MARKET_TZ,
    MFRR_EAM_GO_LIVE_UTC,
    MFRR_ZONES,
    SVK_ATTRIBUTION,
    SVK_LICENCE,
    SVK_LICENCE_URL,
    SVK_RESOURCES,
    SVK_SOURCE,
    fetch_hint,
)

LOGGER = logging.getLogger(__name__)
GENERATOR = "euroflex_bess_lab.public_data.nordic (scripts/research/nordic_fetch.py)"
TABLE_NAMES = ("day_ahead", "fcr_nordic", "fcr_dk1", "afrr", "mfrr", "dk_imbalance")
RELPATHS = {
    "fcr_nordic": "nordic/fcr_nordic.parquet",
    "fcr_dk1": "nordic/fcr_dk1.parquet",
    "afrr": "nordic/afrr_capacity.parquet",
    "mfrr": "nordic/mfrr_capacity.parquet",
    "dk_imbalance": "nordic/dk_imbalance.parquet",
}


def day_ahead_relpath(zone: str) -> str:
    return f"nordic/da_prices_{zone}.parquet"


def snapshot_end_utc(at: pd.Timestamp | None = None) -> pd.Timestamp:
    """Latest Copenhagen midnight at or before ``at`` (exclusive end of every table)."""

    moment = (at or pd.Timestamp.now(tz="UTC")).tz_convert(MARKET_TZ)
    return moment.normalize().tz_convert("UTC")


def as_of_snapshot(as_of: str | None) -> pd.Timestamp:
    if not as_of:
        return snapshot_end_utc()
    local = pd.Timestamp(as_of)
    local = local.tz_localize(MARKET_TZ) if local.tzinfo is None else local.tz_convert(MARKET_TZ)
    return snapshot_end_utc(local.tz_convert("UTC"))


@dataclass
class NordicContext:
    root: Path = field(default_factory=data_root)
    raw_dir: Path = field(default_factory=raw_root)
    client: HttpClient = field(default_factory=lambda: HttpClient(config=energinet.ENERGINET_HTTP))
    refresh: bool = False
    offline: bool = False
    start_utc: pd.Timestamp = field(default_factory=lambda: pd.Timestamp(DEFAULT_START_UTC))
    end_utc: pd.Timestamp = field(default_factory=snapshot_end_utc)

    def energinet(self, dataset: str) -> list[energinet.EnerginetChunk]:
        return energinet.fetch_range(
            self.client,
            self.raw_dir,
            dataset,
            self.start_utc,
            self.end_utc,
            refresh=self.refresh,
            offline=self.offline,
        )

    def svk(self, key: str, zone: str) -> svk.SvkDump:
        return svk.fetch_dump(
            self.client,
            self.raw_dir,
            key,
            zone,
            snapshot_end_utc=self.end_utc,
            refresh=self.refresh,
            offline=self.offline,
        )


def _iso(ts: pd.Timestamp | None) -> str | None:
    return None if ts is None or pd.isna(ts) else f"{pd.Timestamp(ts).tz_convert('UTC'):%Y-%m-%dT%H:%M:%SZ}"


def _window(frame: pd.DataFrame, column: str, ctx: NordicContext) -> pd.DataFrame:
    mask = (frame[column] >= ctx.start_utc) & (frame[column] < ctx.end_utc)
    return frame.loc[mask].reset_index(drop=True)


def _energinet_meta(datasets: Sequence[str], chunks: Sequence[energinet.EnerginetChunk]) -> dict[str, Any]:
    return {
        "source": ENERGINET_SOURCE,
        "dataset_ids": list(datasets),
        "dataset_pages": [ENERGINET_DATASETS[d].page_url for d in datasets],
        "source_urls": energinet.chunk_urls(chunks),
        "licence": ENERGINET_LICENCE,
        "licence_url": ENERGINET_LICENCE_URL,
        "terms_url": ENERGINET_TERMS_URL,
        "attribution": ENERGINET_ATTRIBUTION,
        "redistributed": True,
        "retrieved_at_utc": energinet.latest_retrieval(chunks),
    }


def _svk_meta(key: str, dumps: Sequence[svk.SvkDump]) -> dict[str, Any]:
    resource = SVK_RESOURCES[key]
    return {
        "source": SVK_SOURCE,
        "dataset_ids": [resource.package],
        "dataset_pages": [resource.page_url],
        "source_urls": [d.record.full_url for d in dumps],
        "licence": SVK_LICENCE,
        "licence_url": SVK_LICENCE_URL,
        "attribution": SVK_ATTRIBUTION,
        "redistributed": True,
        "retrieved_at_utc": max((d.record.retrieved_at_utc for d in dumps), default=None),
    }


def _write(ctx: NordicContext, frame: pd.DataFrame, relpath: str, meta: dict[str, Any]) -> WrittenTable:
    meta = dict(meta)
    meta.setdefault("generated_by", GENERATOR)
    meta["snapshot_end_utc"] = _iso(ctx.end_utc)
    written = write_table(frame, ctx.root, relpath, meta)
    LOGGER.info("wrote %s (%d rows, %.2f MB)", written.relpath, written.rows, written.size_bytes / 1e6)
    return written


def _capacity_coverage(frame: pd.DataFrame) -> tuple[str | None, str | None]:
    return coverage(frame, "delivery_start_utc", "delivery_end_utc")


# --- day-ahead ----------------------------------------------------------------------------------------------------

DA_NOTES = [
    "timestamp_utc = start of the SDAC day-ahead product (UTC); resolution_minutes = 60 until delivery day "
    "2025-09-30 and 15 from 2025-10-01 (SDAC 15-minute MTU go-live).",
    "price_eur_mwh is float32; null prices dropped; duplicates on timestamp_utc dropped (last kept).",
]


def build_day_ahead(
    ctx: NordicContext, zones: Sequence[str] = ("DK1", "DK2", "SE3", "SE4", "FI")
) -> list[WrittenTable]:
    written: list[WrittenTable] = []
    energinet_zones = [z for z in zones if z in ENERGINET_DA_ZONES]
    if energinet_zones:
        elspot_chunks = ctx.energinet("Elspotprices")
        da_chunks = ctx.energinet("DayAheadPrices")
        elspot = energinet.read_records(elspot_chunks)
        day_ahead = energinet.read_records(da_chunks)
        cross_check: dict[str, dict[str, Any]] = {}
        for zone in ("SE3", "SE4"):
            if zone in energinet_zones:
                cross_check[zone] = _svk_cross_check(ctx, elspot, day_ahead, zone)
        suspect_days = sorted({d for c in cross_check.values() for d in c.get("mismatch_delivery_days", [])})
        for zone in energinet_zones:
            frame, stats = normalize_energinet_day_ahead(elspot, day_ahead, zone)
            frame = _window(frame, "timestamp_utc", ctx)
            stats.update(_da_grid_stats(frame))
            if zone in cross_check:
                stats["cross_check_svk_data_service"] = cross_check[zone]
            meta = _energinet_meta(["Elspotprices", "DayAheadPrices"], [*elspot_chunks, *da_chunks])
            meta.update(_da_meta(frame, zone, stats))
            meta["transformation_notes"] = [
                *DA_NOTES,
                "Hourly rows come from Elspotprices (SpotPriceEUR) before 2025-09-30T22:00Z, 15-minute rows from "
                "DayAheadPrices (DayAheadPriceEUR) afterwards; requests are filtered to PriceArea in "
                f"{list(ENERGINET_DA_ZONES)} and to the EUR columns.",
            ]
            written.append(_write(ctx, frame, day_ahead_relpath(zone), meta))
    else:
        suspect_days = []
    if "FI" in zones:
        dump = ctx.svk("day_ahead", "FI")
        frame, stats = normalize_svk_day_ahead(svk.read_dump(dump), "FI")
        frame = _window(frame, "timestamp_utc", ctx)
        local_day = frame["timestamp_utc"].dt.tz_convert(MARKET_TZ).dt.strftime("%Y-%m-%d")
        dropped = local_day.isin(suspect_days)
        stats["dropped_suspect_delivery_days"] = {"days": suspect_days, "rows": int(dropped.sum())}
        frame = frame.loc[~dropped].reset_index(drop=True)
        stats.update(_da_grid_stats(frame))
        meta = _svk_meta("day_ahead", [dump])
        meta.update(_da_meta(frame, "FI", stats))
        meta["transformation_notes"] = [
            *DA_NOTES,
            "Source: SVK Data Service dataset market_data_day_ahead_area_prices (bidding_zone = FI), fetched as a "
            "filtered CSV dump. The dataset is marked 'currently not updating' (last row 2026-07-01) and has gaps "
            "(see validation); no other no-key CC BY source for Finnish day-ahead prices was found.",
            "resolution_minutes is 60 before 2025-09-30T22:00Z and 15 afterwards (SDAC MTU).",
            "Delivery days on which SVK's SE3/SE4 values contradict Energinet's (cross-check in the SE3/SE4 "
            "sidecars) are dropped, because the same publisher's FI values for that day cannot be verified.",
        ]
        written.append(_write(ctx, frame, day_ahead_relpath("FI"), meta))
    return written


def _da_meta(frame: pd.DataFrame, zone: str, stats: dict[str, Any]) -> dict[str, Any]:
    end = None
    if len(frame):
        ends = frame["timestamp_utc"] + pd.to_timedelta(frame["resolution_minutes"].astype("int64"), unit="min")
        end = _iso(ends.max())
    return {
        "table": f"nordic_da_prices_{zone}",
        "title": f"{zone} day-ahead auction prices (hourly until 2025-09-30, 15-minute afterwards).",
        "zone": zone,
        "time_column": "timestamp_utc",
        "coverage_start_utc": coverage(frame, "timestamp_utc")[0],
        "coverage_end_utc": end,
        "validation": stats,
        "fetch_command": fetch_hint("day_ahead"),
    }


def _da_grid_stats(frame: pd.DataFrame) -> dict[str, Any]:
    hourly = frame.loc[frame["timestamp_utc"] < DA_15MIN_GO_LIVE_UTC, "timestamp_utc"]
    quarter = frame.loc[frame["timestamp_utc"] >= DA_15MIN_GO_LIVE_UTC, "timestamp_utc"]
    return {
        "resolution_counts": {str(k): int(v) for k, v in frame["resolution_minutes"].value_counts().items()},
        "grid_60min_until_2025_09_30": minute_grid_report(hourly, "60min"),
        "grid_15min_from_2025_10_01": minute_grid_report(quarter, "15min"),
    }


def _svk_cross_check(ctx: NordicContext, elspot: pd.DataFrame, day_ahead: pd.DataFrame, zone: str) -> dict[str, Any]:
    """Compare Energinet prices with SVK Data Service prices for the same Swedish zone (independent publisher)."""

    try:
        dump = ctx.svk("day_ahead", zone)
    except (FileNotFoundError, OSError) as exc:
        return {"skipped": str(exc)}
    ours, _ = normalize_energinet_day_ahead(elspot, day_ahead, zone)
    theirs, _ = normalize_svk_day_ahead(svk.read_dump(dump), zone)
    both = ours.merge(theirs, on="timestamp_utc", suffixes=("", "_svk"))
    both = both.loc[(both["timestamp_utc"] >= ctx.start_utc) & (both["timestamp_utc"] < ctx.end_utc)]
    if both.empty:
        return {"rows_compared": 0}
    diff = (both["price_eur_mwh"].astype("float64") - both["price_eur_mwh_svk"].astype("float64")).abs()
    days = both.loc[diff > 0.01, "timestamp_utc"].dt.tz_convert(MARKET_TZ).dt.strftime("%Y-%m-%d")
    return {
        "source_url": dump.record.full_url,
        "rows_compared": int(len(both)),
        "max_abs_diff_eur_mwh": float(diff.max()),
        "share_within_0_01_eur": float((diff <= 0.01).mean()),
        "mismatch_delivery_days": sorted(days.unique().tolist()),
        "note": "Energinet's values agree with Energy-Charts/SMARD on the mismatch days; SVK repeats some rows there.",
    }


# --- reserves -----------------------------------------------------------------------------------------------------


def build_fcr_nordic(ctx: NordicContext) -> list[WrittenTable]:
    chunks = ctx.energinet("FcrNdDK2")
    raw = energinet.read_records(chunks)
    raw = raw.loc[raw["PriceArea"].isin(FCR_NORDIC_ZONES)]
    frame, stats = normalize_fcr_nordic(raw)
    frame = _window(frame, "delivery_start_utc", ctx)
    stats["zone_price_identical_share"] = _identical_across_zones(frame)
    early = frame.loc[frame["auction"] == "early"]
    stats["grid_early_auction_hourly"] = minute_grid_report(
        early.drop_duplicates("delivery_start_utc")["delivery_start_utc"], "60min"
    )
    meta = _energinet_meta(["FcrNdDK2"], chunks)
    meta.update(
        {
            "table": "nordic_fcr_nordic",
            "title": "FCR-N, FCR-D up and FCR-D down capacity prices of the joint Swedish-DK2 auctions (hourly).",
            "zones": sorted(frame["zone"].dropna().unique().tolist()),
            "time_column": "delivery_start_utc",
            "coverage_start_utc": _capacity_coverage(frame)[0],
            "coverage_end_utc": _capacity_coverage(frame)[1],
            "transformation_notes": [
                "One row per delivery hour, zone (DK2, SE3, SE4), product (fcr_n symmetric, fcr_d_up, fcr_d_down) "
                "and auction (early = 'D-1 early', former D-2; late = 'D-1 late', former D-1; total = volume-"
                "weighted average of the two marginal prices, as published).",
                "price_eur_per_mw_h = PriceTotalEUR (EUR per MW and hour). From 2024-02-01 it is the marginal "
                "(pay-as-cleared) price of each auction; before that the published value is an average price.",
                "volume_local_mw = PurchasedVolumeLocal (procured in the zone), volume_total_mw = PurchasedVolumeTotal.",
                "The documented DST-2024 labelling error (HourUTC 2024-10-27 01:00 and 01:01) is repaired, see "
                "validation.dst_label_rows_repaired.",
            ],
            "validation": stats,
            "fetch_command": fetch_hint("fcr_nordic"),
        }
    )
    return [_write(ctx, frame, RELPATHS["fcr_nordic"], meta)]


def _identical_across_zones(frame: pd.DataFrame) -> float | None:
    pivot = frame.pivot_table(
        index=["delivery_start_utc", "product", "auction"], columns="zone", values="price_eur_per_mw_h"
    )
    if pivot.shape[1] < 2:
        return None
    first = pivot.iloc[:, 0]
    same = pivot.apply(lambda col: (col - first).abs() < 1e-6).all(axis=1)
    return float(same.mean())


def build_fcr_dk1(ctx: NordicContext) -> list[WrittenTable]:
    chunks = ctx.energinet("FcrDK1")
    frame, stats = normalize_fcr_dk1(energinet.read_records(chunks))
    frame = _window(frame, "delivery_start_utc", ctx)
    stats["grid_hourly"] = minute_grid_report(frame["delivery_start_utc"], "60min")
    stats["constant_within_4h_blocks_share"] = _constant_within_blocks(frame)
    meta = _energinet_meta(["FcrDK1"], chunks)
    meta.update(
        {
            "table": "nordic_fcr_dk1",
            "title": "DK1 FCR capacity prices and volumes (Continental Europe FCR cooperation, symmetric).",
            "zones": ["DK1"],
            "time_column": "delivery_start_utc",
            "coverage_start_utc": _capacity_coverage(frame)[0],
            "coverage_end_utc": _capacity_coverage(frame)[1],
            "transformation_notes": [
                "price_eur_per_mw_h = FCRdk_EUR (Danish price, EUR per MW and hour); cross_border_price_eur_per_mw_h "
                "= FCRcross_EUR; volume_domestic_mw / volume_abroad_mw = FCRdomestic_MW / FCRabroad_MW.",
                "Published per hour; the FCR cooperation trades 4-hour products, see "
                "validation.constant_within_4h_blocks_share.",
            ],
            "validation": stats,
            "fetch_command": fetch_hint("fcr_dk1"),
        }
    )
    return [_write(ctx, frame, RELPATHS["fcr_dk1"], meta)]


def _constant_within_blocks(frame: pd.DataFrame) -> float | None:
    if frame.empty:
        return None
    local = frame["delivery_start_utc"].dt.tz_convert(MARKET_TZ)
    block = local.dt.floor("D").dt.tz_localize(None).astype("int64") // 10**9 + (local.dt.hour // 4)
    spread = frame.groupby(block)["price_eur_per_mw_h"].agg(lambda s: float(s.max() - s.min()))
    return float((spread < 1e-6).mean())


def build_afrr(ctx: NordicContext) -> list[WrittenTable]:
    chunks = ctx.energinet("AfrrReservesNordic")
    raw = energinet.read_records(chunks)
    raw = raw.loc[raw["PriceArea"].isin(AFRR_ZONES)]
    frame, stats = normalize_afrr(raw)
    frame = _window(frame, "delivery_start_utc", ctx)
    stats["per_zone"] = capacity_zone_coverage(frame)
    meta = _energinet_meta(["AfrrReservesNordic"], chunks)
    meta.update(
        {
            "table": "nordic_afrr_capacity",
            "title": "aFRR capacity marginal prices, procured volumes and demand per zone, direction and hour.",
            "zones": sorted(frame["zone"].dropna().unique().tolist()),
            "time_column": "delivery_start_utc",
            "coverage_start_utc": _capacity_coverage(frame)[0],
            "coverage_end_utc": _capacity_coverage(frame)[1],
            "transformation_notes": [
                "Nordic aFRR capacity market (common Nordic platform, D-1, gate closure 07:30 CET, marginal pricing) "
                "for DK2, FI, SE3, SE4; DK1 rows (local DK1 aFRR capacity market) start 2024-10-02.",
                "Long format: one row per hour, zone and direction (up/down); price_eur_per_mw_h = Up/DownPriceEUR "
                "(EUR per MW and hour), procured_mw = Up/DownProcuredMW, demand_mw = Up/DownDemandMW (published "
                "from 2025-08-21, null before).",
                "Requests are filtered to PriceArea in the modelled zones.",
            ],
            "validation": stats,
            "fetch_command": fetch_hint("afrr"),
        }
    )
    return [_write(ctx, frame, RELPATHS["afrr"], meta)]


def build_mfrr(ctx: NordicContext) -> list[WrittenTable]:
    chunks = ctx.energinet("mFRRCapacityMarket")
    raw = energinet.read_records(chunks)
    raw = raw.loc[raw["PriceArea"].isin(DK_AREAS)]
    sweden = [z for z in MFRR_ZONES if z.startswith("SE")]
    dumps = [ctx.svk("mfrr_capacity", zone) for zone in sweden]
    frame, stats = normalize_mfrr(raw, {d.zone: svk.read_dump(d) for d in dumps})
    frame = _window(frame, "delivery_start_utc", ctx)
    stats["per_zone"] = capacity_zone_coverage(frame)
    meta = _energinet_meta(["mFRRCapacityMarket"], chunks)
    svk_meta = _svk_meta("mfrr_capacity", dumps)
    meta.update(
        {
            "source": f"{ENERGINET_SOURCE}; {SVK_SOURCE}",
            "dataset_ids": [*meta["dataset_ids"], *svk_meta["dataset_ids"]],
            "dataset_pages": [*meta["dataset_pages"], *svk_meta["dataset_pages"]],
            "source_urls": [*meta["source_urls"], *svk_meta["source_urls"]],
            "attribution": f"{ENERGINET_ATTRIBUTION}; {SVK_ATTRIBUTION}",
            "retrieved_at_utc": max(x for x in (meta["retrieved_at_utc"], svk_meta["retrieved_at_utc"]) if x),
            "table": "nordic_mfrr_capacity",
            "title": "mFRR capacity marginal prices and procured volumes per zone, direction and hour (DK1, DK2, SE3, SE4).",
            "zones": sorted(frame["zone"].dropna().unique().tolist()),
            "time_column": "delivery_start_utc",
            "coverage_start_utc": _capacity_coverage(frame)[0],
            "coverage_end_utc": _capacity_coverage(frame)[1],
            "transformation_notes": [
                "DK1/DK2: Energinet daily (morning) auctions, national until 2024-11-18 and the trilateral DK-SE-FI "
                "market from 2024-11-19; extra afternoon auctions (MfrrCapacityMarketExtra) and the DK2 monthly "
                "slow-reserve auctions are not included. Columns as for aFRR (source = energinet).",
                "SE3/SE4: SVK Data Service dataset mfrr_capacity_market (marginal price EUR/MW per hour and "
                "contracted volume); only hours with Swedish procurement are published, so missing hours mean no "
                "local procurement (source = svk, demand_mw null).",
            ],
            "validation": stats,
            "fetch_command": fetch_hint("mfrr"),
        }
    )
    return [_write(ctx, frame, RELPATHS["mfrr"], meta)]


# --- Danish imbalance ---------------------------------------------------------------------------------------------


def build_dk_imbalance(ctx: NordicContext) -> list[WrittenTable]:
    q_chunks = ctx.energinet("ImbalancePrice")
    h_chunks = ctx.energinet("RegulatingBalancePowerdata")
    quarter = energinet.read_records(q_chunks)
    hourly = energinet.read_records(h_chunks)
    # First full hour of 15-minute data (the hourly row covering the go-live hour is kept, the transitional
    # quarter-hours inside it are not), so intervals never overlap.
    cutover = energinet.parse_utc(quarter["TimeUTC"]).min().ceil("h") if not quarter.empty else MFRR_EAM_GO_LIVE_UTC
    frame, stats = normalize_dk_imbalance(quarter, hourly, cutover)
    frame = _window(frame, "timestamp_utc", ctx)
    design_start = pd.Timestamp("2025-03-19T23:00:00Z")
    stats["price_rule_check_from_2025_03_20"] = dk_imbalance_rule_check(
        frame.loc[frame["timestamp_utc"] >= design_start]
    )
    stats["cutover_utc"] = _iso(cutover)
    meta = _energinet_meta(["RegulatingBalancePowerdata", "ImbalancePrice"], [*h_chunks, *q_chunks])
    end = None
    if len(frame):
        end = _iso(
            (frame["timestamp_utc"] + pd.to_timedelta(frame["resolution_minutes"].astype("int64"), unit="min")).max()
        )
    meta.update(
        {
            "table": "nordic_dk_imbalance",
            "title": "Danish (DK1, DK2) single imbalance prices and their components.",
            "zones": list(DK_AREAS),
            "time_column": "timestamp_utc",
            "coverage_start_utc": coverage(frame, "timestamp_utc")[0],
            "coverage_end_utc": end,
            "transformation_notes": [
                "Hourly rows (resolution_minutes = 60) from RegulatingBalancePowerdata up to the first full hour of "
                "ImbalancePrice data (validation.cutover_utc, mFRR EAM go-live on 2025-03-04); 15-minute rows from "
                "ImbalancePrice afterwards (the three transitional quarter-hours before the cutover are dropped).",
                "Hourly era: imbalance_price_eur_mwh = ImbalancePriceEUR, balancing_power_price_up/down = "
                "BalancingPowerPriceUp/DownEUR, mfrr_up/down_activated_mwh = mFRRUp/DownActBal, imbalance_mwh.",
                "15-minute era: imbalance_price_eur_mwh, spot_price_eur_mwh, dominating_direction (+1 up, -1 down, "
                "0 none), satisfied_demand_mw, afrr_up/down_mwh and afrr_vwa_up/down_eur_mwh (aFRR volume-weighted "
                "averages), mfrr_marginal_up/down_eur_mwh. Energinet: before 2025-03-19 the price equals the "
                "Pricing Module price; the dominating direction is set by balancing (mFRR) demand before 2026-03-04 "
                "and by satisfied demand afterwards.",
            ],
            "validation": stats,
            "fetch_command": fetch_hint("dk_imbalance"),
        }
    )
    return [_write(ctx, frame, RELPATHS["dk_imbalance"], meta)]


BUILDERS: dict[str, Callable[[NordicContext], list[WrittenTable]]] = {
    "day_ahead": build_day_ahead,
    "fcr_nordic": build_fcr_nordic,
    "fcr_dk1": build_fcr_dk1,
    "afrr": build_afrr,
    "mfrr": build_mfrr,
    "dk_imbalance": build_dk_imbalance,
}


def build_tables(ctx: NordicContext, tables: Sequence[str] | None = None) -> list[WrittenTable]:
    names = list(tables or TABLE_NAMES)
    unknown = [n for n in names if n not in BUILDERS]
    if unknown:
        raise ValueError(f"Unknown Nordic tables {unknown}; expected a subset of {list(TABLE_NAMES)}")
    written: list[WrittenTable] = []
    for name in names:
        written.extend(BUILDERS[name](ctx))
    return written
