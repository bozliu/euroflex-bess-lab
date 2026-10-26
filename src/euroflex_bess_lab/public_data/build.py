"""Table builders: fetch (or reuse cached) raw chunks, normalize, validate, write parquet + sidecar.

Each ``build_*`` function is idempotent: cached raw chunks are reused (closed periods are never re-downloaded
unless ``refresh=True``), so re-running ``scripts/data/fetch_all_public.py`` reproduces the same files.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from . import aggregate, energy_charts, regelleistung
from .elia import (
    ELIA_ATTRIBUTION,
    ELIA_LICENCE,
    ELIA_LICENCE_URL,
    RawChunk,
    chunk_provenance,
    dataset_page_url,
    fetch_dataset,
    read_chunks,
    to_utc,
)
from .http import HttpClient, HttpConfig
from .normalize import (
    normalize_legacy,
    normalize_minute,
    normalize_new_formula,
    normalize_qh,
    normalize_res_load,
    normalize_si_forecast,
)
from .paths import data_root, raw_root
from .registry import ELIA_DATASETS, MARI_GO_LIVE_UTC, TABLES
from .validate import curve_vs_published_prices, last_minute_vs_qh, minute_grid_report
from .writers import WrittenTable, coverage, write_stub_sidecar, write_table

LOGGER = logging.getLogger(__name__)
GENERATOR = "euroflex_bess_lab.public_data (scripts/data/fetch_all_public.py)"


def snapshot_end_utc(at: pd.Timestamp | None = None) -> pd.Timestamp:
    """Exclusive end of a data snapshot: the latest Europe/Brussels midnight (as UTC) at or before ``at``.

    Cutting every table at a Belgian local-day boundary keeps rebuilds made on the same day identical and avoids
    partial delivery days.
    """

    moment = (at or pd.Timestamp.now(tz="UTC")).tz_convert("Europe/Brussels")
    return moment.normalize().tz_convert("UTC")


def as_of_snapshot(as_of: str | None) -> pd.Timestamp:
    """``--as-of YYYY-MM-DD`` -> snapshot end at Brussels midnight starting that day (default: today)."""

    if not as_of:
        return snapshot_end_utc()
    local = pd.Timestamp(as_of)
    local = local.tz_localize("Europe/Brussels") if local.tzinfo is None else local.tz_convert("Europe/Brussels")
    return snapshot_end_utc(local.tz_convert("UTC"))


@dataclass
class BuildContext:
    """Where to read/write and how to fetch."""

    root: Path = field(default_factory=data_root)
    raw_dir: Path = field(default_factory=raw_root)
    client: HttpClient = field(default_factory=lambda: HttpClient(config=HttpConfig()))
    refresh: bool = False
    offline: bool = False
    now: pd.Timestamp = field(default_factory=lambda: snapshot_end_utc())

    def fetch(self, dataset_id: str, start: pd.Timestamp | None, end: pd.Timestamp | None) -> list[RawChunk]:
        spec = ELIA_DATASETS[dataset_id]
        return fetch_dataset(
            self.client,
            self.raw_dir,
            dataset_id,
            spec.time_field,
            start,
            end,
            freq=spec.chunk,
            refresh=self.refresh,
            offline=self.offline,
            frozen_after=spec.frozen_after,
        )


def _elia_meta(
    key: str,
    dataset_ids: Sequence[str],
    chunks: Sequence[RawChunk],
    notes: Sequence[str],
    validation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    spec = TABLES[key]
    retrieved = [c.record.retrieved_at_utc for c in chunks]
    return {
        "table": key,
        "title": spec.description,
        "source": "Elia Open Data (opendata.elia.be)",
        "dataset_ids": list(dataset_ids),
        "dataset_pages": [dataset_page_url(d) for d in dataset_ids],
        "source_urls": [c.record.full_url for c in chunks],
        "licence": ELIA_LICENCE,
        "licence_url": ELIA_LICENCE_URL,
        "attribution": ELIA_ATTRIBUTION,
        "redistributed": True,
        "retrieved_at_utc": max(retrieved) if retrieved else None,
        "time_column": spec.time_column,
        "transformation_notes": list(notes),
        "validation": validation or {},
        "inputs": chunk_provenance(chunks),
        "fetch_command": spec.fetch_command(),
        "generated_by": GENERATOR,
    }


def _year_window(year: int, data_start: pd.Timestamp, now: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = max(pd.Timestamp(year=year, month=1, day=1, tz="UTC"), data_start)
    end = min(pd.Timestamp(year=year + 1, month=1, day=1, tz="UTC"), now)
    return start, end


def _chunks_overlapping(chunks: Iterable[RawChunk], start: pd.Timestamp, end: pd.Timestamp) -> list[RawChunk]:
    out = []
    for chunk in chunks:
        c0, c1 = chunk.chunk.start, chunk.chunk.end
        if c0 is None or c1 is None or (c0 < end and c1 > start):
            out.append(chunk)
    return out


def _years(start: pd.Timestamp, end: pd.Timestamp) -> list[int]:
    return list(range(start.year, (end - pd.Timedelta(microseconds=1)).year + 1))


def _write(
    ctx: BuildContext, frame: pd.DataFrame, relpath: str, meta: dict[str, Any], *, root: Path | None = None
) -> WrittenTable:
    """``write_table`` plus the snapshot boundary used for this build (for exact reproduction with --as-of)."""

    meta = dict(meta)
    meta["snapshot_end_utc"] = f"{ctx.now:%Y-%m-%dT%H:%M:%SZ}"
    return write_table(frame, root or ctx.root, relpath, meta)


def _finish(written: WrittenTable) -> WrittenTable:
    LOGGER.info("wrote %s (%d rows, %.2f MB)", written.relpath, written.rows, written.size_bytes / 1e6)
    return written


# --- Belgium core ----------------------------------------------------------------------------------------------

MINUTE_NOTES = [
    "timestamp_utc = start of the minute (UTC). ODS133/ODS165 label the minute at the end of which the "
    "estimate was computed: the row hh:mm includes all events of the running quarter-hour up to hh:mm:59, so it "
    "becomes usable at hh:mm+1 plus publication lag.",
    "ODS133 columns (*_cum_*, mip/mdp/alpha/price_cum) are cumulative quarter-hour-to-date estimates; the row at "
    "minute 14 of a quarter-hour equals the final quarter-hour value (see validation.last_minute_vs_qh).",
    "ODS126 columns (si_mw, ace_mw, *_up_mw, *_down_mw, rs_*) are instantaneous minute values.",
    "ODS165 duplicates of ODS133 (systemimbalance, ace, marginal prices) are dropped; quality_status is ODS133's.",
    "Rows are reindexed to a complete 1-minute grid between the first and last minute seen in any source; minutes "
    "missing upstream are null (counts in validation).",
    "Numeric columns are float32; timestamps datetime64[ns, UTC].",
]
QH_NOTES = [
    "timestamp_utc = start of the quarter-hour (UTC).",
    "ODS134 provides final prices, SI/ACE, alpha/alpha' and MIP/MDP; ODS166 price components; ODS127 volumes "
    "(ODS127 SI/ACE duplicates dropped).",
    "Rows are reindexed to a complete 15-minute grid between the first and last quarter-hour seen in any source.",
    "Numeric columns are float32.",
]


def build_belgium_minute(ctx: BuildContext, years: Sequence[int]) -> list[WrittenTable]:
    written: list[WrittenTable] = []
    for year in years:
        start, end = _year_window(year, MARI_GO_LIVE_UTC, ctx.now)
        if end <= start:
            continue
        chunks = {ds: ctx.fetch(ds, start, end) for ds in ("ods133", "ods126", "ods165", "ods134")}
        raw = {ds: read_chunks(_chunks_overlapping(chunks[ds], start, end)) for ds in chunks}
        frame, stats = normalize_minute(raw["ods133"], raw["ods126"], raw["ods165"], start, end)
        if frame.empty:
            continue
        stats["grid"] = minute_grid_report(frame["timestamp_utc"], "1min")
        qh_frame, _ = normalize_qh(
            raw["ods134"], pd.DataFrame(columns=["datetime"]), pd.DataFrame(columns=["datetime"]), start, end
        )
        stats["last_minute_vs_qh"] = last_minute_vs_qh(frame, qh_frame)
        cov = coverage(frame, "timestamp_utc", step=pd.Timedelta(minutes=1))
        meta = _elia_meta(
            "belgium_minute",
            ["ods133", "ods126", "ods165"],
            [c for ds in ("ods133", "ods126", "ods165") for c in _chunks_overlapping(chunks[ds], start, end)],
            MINUTE_NOTES + ["ODS134 was read only to validate the last minute of each QH against the final price."],
            stats,
        )
        meta.update({"coverage_start_utc": cov[0], "coverage_end_utc": cov[1], "partition_year": year})
        meta["fetch_command"] = TABLES["belgium_minute"].fetch_command(year)
        written.append(_finish(_write(ctx, frame, TABLES["belgium_minute"].relpath(year), meta)))
    return written


def build_belgium_qh(ctx: BuildContext, years: Sequence[int]) -> list[WrittenTable]:
    written: list[WrittenTable] = []
    for year in years:
        start, end = _year_window(year, MARI_GO_LIVE_UTC, ctx.now)
        if end <= start:
            continue
        chunks = {ds: ctx.fetch(ds, start, end) for ds in ("ods134", "ods166", "ods127")}
        raw = {ds: read_chunks(_chunks_overlapping(chunks[ds], start, end)) for ds in chunks}
        frame, stats = normalize_qh(raw["ods134"], raw["ods166"], raw["ods127"], start, end)
        if frame.empty:
            continue
        stats["grid"] = minute_grid_report(frame["timestamp_utc"], "15min")
        stats["quality_status_counts"] = {
            str(k): int(v) for k, v in frame["quality_status"].value_counts(dropna=False).items()
        }
        cov = coverage(frame, "timestamp_utc", step=pd.Timedelta(minutes=15))
        all_chunks = [c for ds in chunks for c in _chunks_overlapping(chunks[ds], start, end)]
        meta = _elia_meta("belgium_qh", ["ods134", "ods166", "ods127"], all_chunks, QH_NOTES, stats)
        meta.update({"coverage_start_utc": cov[0], "coverage_end_utc": cov[1], "partition_year": year})
        meta["fetch_command"] = TABLES["belgium_qh"].fetch_command(year)
        written.append(_finish(_write(ctx, frame, TABLES["belgium_qh"].relpath(year), meta)))
    return written


def _new_formula_decomposition(qh: pd.DataFrame) -> dict[str, float]:
    """Share of quarter-hours where the published price equals main component (by SI sign) + incentive."""

    main = np.where(qh["si_mw"] < 0, qh["main_up_eur_mwh"], qh["main_down_eur_mwh"])
    implied = main + qh["incentive_eur_mwh"].fillna(0.0).to_numpy()
    ok = np.isclose(implied, qh["imbalance_price_eur_mwh"].to_numpy(), atol=0.05)
    weighted_up = qh["weight_afrr"] * qh["afrr_price_up_eur_mwh"].fillna(0.0) + qh["weight_mfrr"] * qh[
        "mfrr_price_up_eur_mwh"
    ].fillna(0.0)
    return {
        "price_equals_main_by_si_sign_plus_incentive": float(ok.mean()),
        "main_up_equals_weighted_afrr_mfrr_up": float(np.isclose(weighted_up, qh["main_up_eur_mwh"], atol=0.05).mean()),
        "quarter_hours_with_incentive": int(qh["incentive_eur_mwh"].fillna(0.0).ne(0.0).sum()),
    }


def build_new_formula(ctx: BuildContext) -> list[WrittenTable]:
    common_notes = [
        "Elia simulation of the imbalance price under the new formula consulted for go-live on 24/11/2026 "
        "(conditional on CREG approval), computed on historical data for 2026-01..2026-07. Best-effort, "
        "non-validated data; Elia removes it from ODS211/ODS212 on 17/11/2026, this file preserves it.",
        "imbalance_price = main_up (SI < 0) or main_down (SI >= 0) + incentive_eur_mwh (only when FRCE exceeds the "
        "L2 target and |SI| > 250 MW); main components are volume-weighted aFRR/mFRR prices with weights "
        "weight_afrr/weight_mfrr (decomposition match rates in validation.decomposition of the QH file).",
        "Numeric columns float32; timestamps datetime64[ns, UTC]; reindexed to a complete grid between the first "
        "and last published interval.",
    ]
    minute_notes = [
        common_notes[0],
        "Minute rows are cumulative quarter-hour-to-date estimates like ODS133: at minute 14 FRCE equals the "
        "ODS212 value and the price matches in about 95% of quarter-hours, the SI estimate differs (see "
        "validation.last_minute_vs_ods212). frce_rolling_mw / si_rolling_mw are 15-minute rolling windows that "
        "reach back past the start of the quarter-hour.",
        *common_notes[1:],
    ]
    parts: dict[str, tuple[pd.DataFrame, dict[str, Any], list[RawChunk]]] = {}
    for key, dataset_id, minute in (
        ("belgium_new_formula_minute", "ods211", True),
        ("belgium_new_formula_qh", "ods212", False),
    ):
        chunks = ctx.fetch(dataset_id, None, None)
        frame, stats = normalize_new_formula(read_chunks(chunks), minute=minute)
        stats["grid"] = minute_grid_report(frame["timestamp_utc"], "1min" if minute else "15min")
        parts[key] = (frame, stats, chunks)
    minute_frame, minute_stats, _ = parts["belgium_new_formula_minute"]
    qh_frame, qh_stats, _ = parts["belgium_new_formula_qh"]
    if not minute_frame.empty and not qh_frame.empty:
        minute_stats["last_minute_vs_ods212"] = last_minute_vs_qh(
            minute_frame.assign(qh_start_utc=minute_frame["timestamp_utc"].dt.floor("15min")),
            qh_frame,
            pairs=(("imbalance_price_eur_mwh", "imbalance_price_eur_mwh"), ("si_mw", "si_mw"), ("frce_mw", "frce_mw")),
        )
        qh_stats["decomposition"] = _new_formula_decomposition(qh_frame)
    written: list[WrittenTable] = []
    for key, (frame, stats, chunks) in parts.items():
        minute = key.endswith("_minute")
        cov = coverage(frame, "timestamp_utc", step=pd.Timedelta(minutes=1 if minute else 15))
        dataset_id = "ods211" if minute else "ods212"
        meta = _elia_meta(key, [dataset_id], chunks, minute_notes if minute else common_notes, stats)
        meta.update({"coverage_start_utc": cov[0], "coverage_end_utc": cov[1], "simulated": True})
        written.append(_finish(_write(ctx, frame, TABLES[key].relpath(), meta)))
    return written


SI_NOTES = [
    "prediction_time_utc = time the forecast was made (Elia `predictiontimeutc`, one forecast per minute); "
    "target_qh_utc = start of the quarter-hour whose average SI is forecast.",
    "ODS136 targets the running quarter-hour, ODS147 the next one. Probabilities are for the target-QH average "
    "SI falling in [-inf,-400], [-400,-200], [-200,0], [0,200], [200,400], [400,inf] MW.",
    "prediction_quality = Elia `predictionquality` (share of input data available, in percent).",
    "Only contract columns are kept (resolutioncode and the ODS147 plotting helper column are dropped).",
]


def build_si_forecast(ctx: BuildContext, kind: str, years: Sequence[int]) -> list[WrittenTable]:
    key, dataset_id = {
        "current": ("belgium_si_forecast_current", "ods136"),
        "next": ("belgium_si_forecast_next", "ods147"),
    }[kind]
    written: list[WrittenTable] = []
    for year in years:
        start, end = _year_window(year, MARI_GO_LIVE_UTC, ctx.now)
        if end <= start:
            continue
        chunks = _chunks_overlapping(ctx.fetch(dataset_id, start, end), start, end)
        raw = read_chunks(chunks)
        if raw.empty:
            continue
        raw_ts = pd.to_datetime(raw["predictiontimeutc"], utc=True)
        raw = raw[(raw_ts >= start) & (raw_ts < end)]
        frame, stats = normalize_si_forecast(raw)
        stats["prediction_minutes"] = minute_grid_report(
            frame["prediction_time_utc"].dt.floor("1min").drop_duplicates()
        )
        lead = (frame["target_qh_utc"] - frame["prediction_time_utc"]).dt.total_seconds() / 60.0
        stats["lead_minutes_range"] = [float(lead.min()), float(lead.max())]
        cov = coverage(frame, "prediction_time_utc", step=pd.Timedelta(minutes=1))
        meta = _elia_meta(key, [dataset_id], chunks, SI_NOTES, stats)
        meta.update({"coverage_start_utc": cov[0], "coverage_end_utc": cov[1], "partition_year": year})
        meta["fetch_command"] = TABLES[key].fetch_command(year)
        written.append(_finish(_write(ctx, frame, TABLES[key].relpath(year), meta)))
    return written


# --- Capacity awards, RES/load, bid curves ----------------------------------------------------------------------


def build_capacity_awards(ctx: BuildContext, products: Sequence[str] = ("afrr", "mfrr")) -> list[WrittenTable]:
    written: list[WrittenTable] = []
    end = ctx.now + pd.Timedelta(days=3)
    for product in products:
        if product == "afrr":
            legacy_id, current_id, key = "ods125", "ods207", "belgium_afrr_capacity_awards"
            legacy_start = pd.Timestamp("2022-01-01", tz="UTC")
        else:
            legacy_id, current_id, key = "ods055", "ods206", "belgium_mfrr_capacity_awards"
            legacy_start = pd.Timestamp("2021-01-01", tz="UTC")
        legacy_chunks = ctx.fetch(legacy_id, legacy_start, end)
        current_chunks = ctx.fetch(current_id, pd.Timestamp("2026-01-01", tz="UTC"), end)
        legacy = read_chunks(legacy_chunks)
        current = read_chunks(current_chunks)
        if product == "afrr":
            bids = aggregate.afrr_bids_long(legacy, current)
        else:
            bids = aggregate.mfrr_bids_long(legacy, current)
        frame, stats = aggregate.aggregate_capacity_awards(bids)
        frame = frame[frame["delivery_start_utc"] < ctx.now].reset_index(drop=True)
        cov = coverage(frame, "delivery_start_utc", end_column="delivery_end_utc")
        meta = _elia_meta(
            key,
            [legacy_id, current_id],
            [*legacy_chunks, *current_chunks],
            aggregate.CAPACITY_NOTES[product],
            stats,
        )
        meta.update({"coverage_start_utc": cov[0], "coverage_end_utc": cov[1]})
        written.append(_finish(_write(ctx, frame, TABLES[key].relpath(), meta)))
    return written


RES_NOTES = [
    "timestamp_utc = start of the quarter-hour (UTC); values in MW (Elia publishes MW averages per QH).",
    "load_* from ODS001 (total load on the Belgian grid incl. estimated losses); wind_* are sums over all "
    "ODS031 rows (offshore + onshore, Elia- and DSO-connected, all regions); pv_* are the ODS032 region "
    "'Belgium' rows.",
    "Forecast vintages: *_da_11h_* = day-ahead forecast published 11:00 D-1 (usable before the 12:00 CET "
    "day-ahead gate); *_da_forecast_* = day-ahead forecast published 18:00 D-1 (after the DA auction); "
    "*_week_ahead_*; *_most_recent_* = latest revision before delivery (look-ahead risk for anything but "
    "near-real-time use: its issue time is not published).",
    "P10/P90 bands are dropped (they are not additive across regions).",
]


def build_res_load(ctx: BuildContext, years: Sequence[int]) -> list[WrittenTable]:
    written: list[WrittenTable] = []
    for year in years:
        start, end = _year_window(year, pd.Timestamp("2023-01-01", tz="UTC"), ctx.now)
        if end <= start:
            continue
        chunks = {
            ds: _chunks_overlapping(ctx.fetch(ds, start, end), start, end) for ds in ("ods001", "ods031", "ods032")
        }
        raw = {
            "ods001": read_chunks(chunks["ods001"]),
            "ods031": read_chunks(chunks["ods031"]),
            "ods032": read_chunks(chunks["ods032"]),
        }
        frame, stats = normalize_res_load(raw["ods001"], raw["ods031"], raw["ods032"], start, end)
        if frame.empty:
            continue
        cov = coverage(frame, "timestamp_utc", step=pd.Timedelta(minutes=15))
        meta = _elia_meta(
            "belgium_res_load",
            ["ods001", "ods031", "ods032"],
            [c for v in chunks.values() for c in v],
            RES_NOTES,
            stats,
        )
        meta.update({"coverage_start_utc": cov[0], "coverage_end_utc": cov[1], "partition_year": year})
        meta["fetch_command"] = TABLES["belgium_res_load"].fetch_command(year)
        written.append(_finish(_write(ctx, frame, TABLES["belgium_res_load"].relpath(year), meta)))
    return written


def build_energy_bid_curves(ctx: BuildContext, years: Sequence[int]) -> list[WrittenTable]:
    written: list[WrittenTable] = []
    for year in years:
        start, end = _year_window(year, pd.Timestamp("2025-01-01", tz="UTC"), ctx.now)
        if end <= start:
            continue
        curves: list[pd.DataFrame] = []
        used: list[RawChunk] = []
        stats_all: dict[str, Any] = {}
        for dataset_id, direction in (("ods156", "up"), ("ods157", "down")):
            chunks = _chunks_overlapping(ctx.fetch(dataset_id, start, end), start, end)
            used.extend(chunks)
            for chunk in chunks:
                raw = read_chunks([chunk])
                curve, stats = aggregate.bid_curves(raw, direction)
                curve = curve[(curve["timestamp_utc"] >= start) & (curve["timestamp_utc"] < end)]
                curves.append(curve)
                for k, v in stats.items():
                    if isinstance(v, int | float):
                        stats_all[f"{direction}_{k}"] = stats_all.get(f"{direction}_{k}", 0) + v
        frame = pd.concat([c for c in curves if not c.empty], ignore_index=True) if curves else pd.DataFrame()
        if frame.empty:
            continue
        frame = frame.sort_values(["timestamp_utc", "direction", "cum_mw"]).reset_index(drop=True)
        qh_path = ctx.root / TABLES["belgium_qh"].relpath(year)
        if qh_path.exists():
            stats_all["curve_vs_published_prices"] = curve_vs_published_prices(frame, pd.read_parquet(qh_path))
        stats_all["qh_per_direction"] = {
            str(k): int(v) for k, v in frame.groupby("direction")["timestamp_utc"].nunique().items()
        }
        cov = coverage(frame, "timestamp_utc", step=pd.Timedelta(minutes=15))
        meta = _elia_meta("belgium_energy_bid_curves", ["ods156", "ods157"], used, aggregate.CURVE_NOTES, stats_all)
        meta.update({"coverage_start_utc": cov[0], "coverage_end_utc": cov[1], "partition_year": year})
        meta["grid"] = {"cum_mw_min": 0, "cum_mw_max": aggregate.CURVE_MAX_MW, "cum_mw_step": aggregate.CURVE_STEP_MW}
        meta["fetch_command"] = TABLES["belgium_energy_bid_curves"].fetch_command(year)
        written.append(_finish(_write(ctx, frame, TABLES["belgium_energy_bid_curves"].relpath(year), meta)))
    return written


# --- Energy-Charts day-ahead ------------------------------------------------------------------------------------

DA_NOTES = [
    "timestamp_utc = start of the day-ahead product; resolution_minutes = product length inferred from the "
    "spacing of consecutive timestamps (60 until delivery day 2025-09-30, 15 from 2025-10-01 after the SDAC "
    "15-minute MTU go-live).",
    "Monthly API requests (local delivery days, end inclusive) are concatenated and de-duplicated on "
    "timestamp_utc; null prices are dropped. price_eur_mwh is float32.",
]


def build_day_ahead(
    ctx: BuildContext, zones: Sequence[str] = energy_charts.SUPPORTED_ZONES, start: str = "2023-01-01"
) -> list[WrittenTable]:
    written: list[WrittenTable] = []
    first = pd.Timestamp(start)
    last = ctx.now.tz_localize(None)
    for zone in zones:
        months = energy_charts.month_starts(first, last)
        chunks = [
            energy_charts.fetch_month(ctx.client, ctx.raw_dir, zone, m, refresh=ctx.refresh, offline=ctx.offline)
            for m in months
        ]
        frame, stats = energy_charts.normalize_months(chunks)
        lo = pd.Timestamp(first).tz_localize("Europe/Brussels").tz_convert("UTC")
        frame = frame[(frame["timestamp_utc"] >= lo) & (frame["timestamp_utc"] < ctx.now)].reset_index(drop=True)
        stats["resolution_counts"] = {str(k): int(v) for k, v in frame["resolution_minutes"].value_counts().items()}
        stats["grid_15min_from_2025_10_01"] = minute_grid_report(
            frame.loc[frame["resolution_minutes"] == 15, "timestamp_utc"], "15min"
        )
        stats["grid_60min_until_2025_09_30"] = minute_grid_report(
            frame.loc[frame["resolution_minutes"] == 60, "timestamp_utc"], "60min"
        )
        end_series = frame["timestamp_utc"] + pd.to_timedelta(frame["resolution_minutes"].astype("int64"), unit="min")
        cov = (
            coverage(frame, "timestamp_utc")[0],
            f"{end_series.max():%Y-%m-%dT%H:%M:%SZ}" if len(frame) else None,
        )
        key = f"da_prices_{zone}"
        retrieved = [c.record.retrieved_at_utc for c in chunks]
        meta = {
            "table": key,
            "title": TABLES[key].description,
            "source": "Energy-Charts API (api.energy-charts.info, Fraunhofer ISE)",
            "dataset_ids": [f"energy_charts_price_{zone}"],
            "dataset_pages": ["https://api.energy-charts.info/"],
            "source_urls": [c.record.full_url for c in chunks],
            "licence": energy_charts.ENERGY_CHARTS_LICENCE,
            "licence_url": energy_charts.ENERGY_CHARTS_LICENCE_URL,
            "attribution": energy_charts.ENERGY_CHARTS_ATTRIBUTION,
            "upstream_licence_info": stats.pop("licence_info", []),
            "redistributed": True,
            "retrieved_at_utc": max(retrieved) if retrieved else None,
            "coverage_start_utc": cov[0],
            "coverage_end_utc": cov[1],
            "time_column": "timestamp_utc",
            "zone": zone,
            "transformation_notes": DA_NOTES,
            "validation": stats,
            "fetch_command": TABLES[key].fetch_command(),
            "generated_by": GENERATOR,
        }
        written.append(_finish(_write(ctx, frame, TABLES[key].relpath(), meta)))
    return written


# --- Germany (regelleistung.net, not redistributed) -------------------------------------------------------------

GERMANY_NOTES = {
    "fcr": [
        "FCR capacity results (D-1 auction, 4-hour blocks NEGPOS_hh_hh in Europe/Berlin local time). "
        "price_eur_per_mw_h = GERMANY_SETTLEMENTCAPACITY_PRICE_[EUR/MW] divided by the block length in hours "
        "(3 or 5 hours on DST-change days); FCR clears at a uniform price, so marginal = settlement price. "
        "volume_mw = GERMANY_DEMAND_[MW]. direction = symmetric.",
        "fcr_capacity_by_country.parquet (local only) keeps all FCR-cooperation countries incl. BELGIUM.",
    ],
    "afrr": [
        "aFRR capacity results (4-hour blocks POS/NEG_hh_hh, Europe/Berlin). price_eur_per_mw_h = "
        "GERMANY_AVERAGE_CAPACITY_PRICE_[(EUR/MW)/h]; marginal_price_eur_per_mw_h = "
        "GERMANY_MARGINAL_CAPACITY_PRICE_[(EUR/MW)/h]; volume_mw = GERMANY_BLOCK_DEMAND_[MW] from the demands file "
        "(falls back to GERMANY_ALLOCATED_VOLUME_[MW] when the demand is missing). direction up = POS, down = NEG.",
    ],
}


def build_germany(
    ctx: BuildContext, products: Sequence[str] = ("fcr", "afrr"), start: str = "2024-01-01"
) -> list[WrittenTable]:
    written: list[WrittenTable] = []
    last_day = ctx.now.tz_convert("Europe/Berlin").tz_localize(None).normalize() - pd.Timedelta(days=1)
    days = pd.date_range(start, last_day, freq="D")
    for product in products:
        upstream_product = {"fcr": "FCR", "afrr": "aFRR"}[product]
        germany_frames: list[pd.DataFrame] = []
        country_frames: list[pd.DataFrame] = []
        records = []
        failures: list[str] = []
        for day in days:
            try:
                results = regelleistung.fetch_day(
                    ctx.client, ctx.raw_dir, upstream_product, "results", day, refresh=ctx.refresh, offline=ctx.offline
                )
            except (FileNotFoundError, OSError, RuntimeError) as exc:
                failures.append(f"{day:%Y-%m-%d}: {exc}")
                continue
            records.append(results.record)
            results_frame = regelleistung.xlsx_frame(results.path)
            if product == "fcr":
                germany, countries = regelleistung.parse_fcr_results(results_frame)
                germany_frames.append(germany)
                country_frames.append(countries)
            else:
                demands_frame = None
                try:
                    demands = regelleistung.fetch_day(
                        ctx.client,
                        ctx.raw_dir,
                        upstream_product,
                        "demands",
                        day,
                        refresh=ctx.refresh,
                        offline=ctx.offline,
                    )
                    records.append(demands.record)
                    demands_frame = regelleistung.xlsx_frame(demands.path)
                except (FileNotFoundError, OSError, RuntimeError) as exc:
                    failures.append(f"{day:%Y-%m-%d} demands: {exc}")
                germany_frames.append(regelleistung.parse_afrr_results(results_frame, demands_frame))
        frame = regelleistung.finalize_germany(germany_frames)
        frame = frame[frame["delivery_start_utc"] < ctx.now].reset_index(drop=True)
        key = f"germany_{product}_capacity"
        spec = TABLES[key]
        cov = coverage(frame, "delivery_start_utc", end_column="delivery_end_utc")
        retrieved = [r.retrieved_at_utc for r in records]
        meta: dict[str, Any] = {
            "table": key,
            "title": spec.description,
            "source": "regelleistung.net (German TSOs) data center, crds API v2",
            "dataset_ids": [f"regelleistung_{upstream_product}_capacity_results"]
            + (["regelleistung_aFRR_capacity_demands"] if product == "afrr" else []),
            "dataset_pages": ["https://www.regelleistung.net/apps/datacenter/tenders/"],
            "source_url_template": f"{regelleistung.REGELLEISTUNG_API}/results/aggregated?deliveryDate=YYYY-MM-DD"
            f"&productType={upstream_product}&market=CAPACITY&exportFormat=xlsx",
            "licence": regelleistung.REGELLEISTUNG_LICENCE,
            "licence_url": regelleistung.REGELLEISTUNG_TERMS_URL,
            "attribution": regelleistung.REGELLEISTUNG_ATTRIBUTION,
            "redistributed": False,
            "local_path": f"data/raw/{spec.relpath()}",
            "retrieved_at_utc": max(retrieved) if retrieved else None,
            "coverage_start_utc": cov[0],
            "coverage_end_utc": cov[1],
            "time_column": "delivery_start_utc",
            "transformation_notes": GERMANY_NOTES[product],
            "validation": {"days_requested": len(days), "failures": failures[:20], "n_failures": len(failures)},
            "fetch_command": spec.fetch_command(),
            "generated_by": GENERATOR,
        }
        local = _write(ctx, frame, spec.relpath(), meta, root=ctx.raw_dir)
        if product == "fcr" and country_frames:
            countries = pd.concat([f for f in country_frames if not f.empty], ignore_index=True)
            if not countries.empty:
                for column in (
                    "demand_mw",
                    "price_eur_per_mw",
                    "price_eur_per_mw_h",
                    "deficit_surplus_mw",
                    "tender_number",
                ):
                    countries[column] = pd.to_numeric(countries[column], errors="coerce").astype("float32")
                countries["country"] = countries["country"].astype("string")
                countries = countries.sort_values(["delivery_start_utc", "country", "tender_number"]).reset_index(
                    drop=True
                )
                meta_c = dict(meta)
                meta_c["table"] = "germany_fcr_capacity_by_country"
                meta_c["local_path"] = "data/raw/germany/fcr_capacity_by_country.parquet"
                _write(ctx, countries, "germany/fcr_capacity_by_country.parquet", meta_c, root=ctx.raw_dir)
        stub = {k: v for k, v in local.sidecar.items() if k not in {"schema_version", "path"}}
        stub["note"] = (
            "Not redistributed (regelleistung.net terms require prior written consent for reproduction). "
            "rows/sha256 describe the locally built file at retrieval time; rebuild with the fetch command."
        )
        write_stub_sidecar(ctx.root, spec.relpath(), stub)
        written.append(_finish(local))
    return written


# --- Legacy (pre-MARI) -------------------------------------------------------------------------------------------


LEGACY_NOTES = [
    "Pre-MARI Elia data (before the 22/05/2024 MARI local go-live), kept separate because the price formula and "
    "field set differ from the current tables. timestamp_utc = interval start (UTC).",
    "imbalance_price_long_eur_mwh / imbalance_price_short_eur_mwh = Elia positive / negative imbalance price "
    "(applied to long / short BRPs); they are identical from 2020 on (single pricing). imbalance_price_eur_mwh "
    "(QH) and price_cum_eur_mwh (minute) repeat the long-side price for convenience.",
    "Minute rows (ODS046) are cumulative quarter-hour-to-date estimates like ODS133; calculation_time_utc is "
    "Elia's calculation timestamp. nrv = net regulation volume.",
    "Numeric columns float32; reindexed to a complete grid between the first and last published interval.",
]


def build_legacy(ctx: BuildContext, years: Sequence[int]) -> list[WrittenTable]:
    """Pre-MARI history: ODS047 quarter-hours (2019 on) and ODS046 minutes (2023 on), files suffixed ``_legacy``."""

    written: list[WrittenTable] = []
    for key, dataset_id, minute, first in (
        ("belgium_qh_legacy", "ods047", False, pd.Timestamp("2019-01-01", tz="UTC")),
        ("belgium_minute_legacy", "ods046", True, pd.Timestamp("2023-01-01", tz="UTC")),
    ):
        for year in years:
            start = max(pd.Timestamp(year=year, month=1, day=1, tz="UTC"), first)
            end = min(pd.Timestamp(year=year + 1, month=1, day=1, tz="UTC"), MARI_GO_LIVE_UTC)
            if end <= start:
                continue
            chunks = _chunks_overlapping(ctx.fetch(dataset_id, start, end), start, end)
            raw = read_chunks(chunks)
            if raw.empty:
                continue
            frame, stats = normalize_legacy(raw, minute=minute, window_start=start, window_end=end)
            if frame.empty:
                continue
            stats["grid"] = minute_grid_report(frame["timestamp_utc"], "1min" if minute else "15min")
            cov = coverage(frame, "timestamp_utc", step=pd.Timedelta(minutes=1 if minute else 15))
            meta = _elia_meta(key, [dataset_id], chunks, LEGACY_NOTES, stats)
            meta.update({"coverage_start_utc": cov[0], "coverage_end_utc": cov[1], "partition_year": year})
            meta["fetch_command"] = TABLES[key].fetch_command(year)
            written.append(_finish(_write(ctx, frame, TABLES[key].relpath(year), meta)))
    return written


# --- Orchestration -------------------------------------------------------------------------------------------------

TARGETS = (
    "belgium-new-formula",
    "belgium-minute",
    "belgium-qh",
    "day-ahead",
    "belgium-si-forecast",
    "belgium-capacity-awards",
    "germany-reserve",
    "belgium-res-load",
    "belgium-bid-curves",
    "belgium-legacy",
)


def build_target(
    ctx: BuildContext,
    target: str,
    *,
    start: str | None = None,
    end: str | None = None,
    zone: str | None = None,
    product: str | None = None,
    kind: str | None = None,
) -> list[WrittenTable]:
    """Build one CLI target for the years overlapping ``[start, end)`` (defaults: full coverage up to today)."""

    end_ts = to_utc(end) if end else ctx.now

    def years_from(default_start: str) -> list[int]:
        start_ts = to_utc(start) if start else to_utc(default_start)
        return _years(start_ts, end_ts)

    if target == "belgium-new-formula":
        return build_new_formula(ctx)
    if target == "belgium-minute":
        return build_belgium_minute(ctx, years_from("2024-05-22"))
    if target == "belgium-qh":
        return build_belgium_qh(ctx, years_from("2024-05-22"))
    if target == "belgium-si-forecast":
        kinds = [kind] if kind else ["current", "next"]
        return [w for k in kinds for w in build_si_forecast(ctx, k, years_from("2024-05-22"))]
    if target == "belgium-capacity-awards":
        return build_capacity_awards(ctx, [product] if product else ["afrr", "mfrr"])
    if target == "belgium-res-load":
        return build_res_load(ctx, years_from("2023-01-01"))
    if target == "belgium-bid-curves":
        return build_energy_bid_curves(ctx, years_from("2025-01-01"))
    if target == "day-ahead":
        return build_day_ahead(ctx, [zone] if zone else list(energy_charts.SUPPORTED_ZONES), start or "2023-01-01")
    if target == "germany-reserve":
        return build_germany(ctx, [product] if product else ["fcr", "afrr"], start or "2024-01-01")
    if target == "belgium-legacy":
        return build_legacy(ctx, years_from("2019-01-01"))
    raise ValueError(f"Unknown target {target!r}; expected one of {TARGETS} or 'all'")
