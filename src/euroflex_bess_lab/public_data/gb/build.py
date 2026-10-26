"""Build the normalized GB tables (zstd parquet + ``.meta.json`` sidecars) from cached raw downloads.

Every builder is idempotent: closed raw chunks are reused from ``data/raw/gb`` (gitignored); recent chunks are
re-downloaded when stale (see :func:`euroflex_bess_lab.public_data.http.cached_download`). Snapshots end at the
latest Europe/London midnight so rebuilds on the same day are identical.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ..http import FetchError, HttpClient, HttpConfig
from ..paths import data_root, raw_root
from ..writers import WrittenTable, coverage, write_table
from . import ecb, elexon, frequency, neso
from .sources import (
    BMRS_LICENCE,
    BMRS_LICENCE_TERMS,
    BMRS_LICENCE_URL,
    ECB_ATTRIBUTION,
    ECB_DATASET_PAGE,
    ECB_LICENCE,
    ECB_LICENCE_TERMS,
    ECB_LICENCE_URL,
    ELEXON_DATASET_PAGES,
    ELEXON_FREQUENCY_STREAM_URL,
    ELEXON_MID_STREAM_URL,
    ELEXON_SYSTEM_PRICES_URL,
    NESO_ATTRIBUTION,
    NESO_DATASTORE_DUMP_URL,
    NESO_EAC_PACKAGE,
    NESO_EAC_PAGE,
    NESO_LICENCE,
    NESO_LICENCE_TERMS,
    NESO_LICENCE_URL,
    bmrs_attribution,
)
from .tables import FETCH_TARGETS, GB_START_UTC, GB_TABLES
from .timeutils import SP_MINUTES, latest_uk_midnight_utc, settlement_grid, to_utc

LOGGER = logging.getLogger(__name__)
GENERATOR = "euroflex_bess_lab.public_data.gb (scripts/research/gb_fetch_data.py)"


@dataclass
class GBBuildContext:
    root: Path = field(default_factory=data_root)
    raw_dir: Path = field(default_factory=raw_root)
    client: HttpClient = field(default_factory=lambda: HttpClient(config=HttpConfig(min_interval_s=0.2)))
    refresh: bool = False
    offline: bool = False
    now: pd.Timestamp = field(default_factory=latest_uk_midnight_utc)
    start: pd.Timestamp = field(default_factory=lambda: to_utc(GB_START_UTC))

    @property
    def years(self) -> list[int]:
        return list(range(self.start.year, (self.now - pd.Timedelta(microseconds=1)).year + 1))


def _iso(ts: pd.Timestamp) -> str:
    return ts.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def _file_entry(key: str, record: Any) -> dict[str, Any]:
    return {
        "chunk": key,
        "url": record.full_url,
        "retrieved_at_utc": record.retrieved_at_utc,
        "sha256": record.sha256,
        "size_bytes": record.size_bytes,
    }


def inputs_summary(files: Sequence[elexon.RawFile]) -> dict[str, Any]:
    """Compact provenance for many small raw files: counts, sizes and a digest of the per-file sha256 values."""

    if not files:
        return {"n_files": 0}
    hashes = sorted(f.record.sha256 for f in files)
    digest = hashlib.sha256("\n".join(hashes).encode("utf-8")).hexdigest()
    retrieved = sorted(f.record.retrieved_at_utc for f in files)
    return {
        "n_files": len(files),
        "total_size_bytes": int(sum(f.record.size_bytes for f in files)),
        "first_chunk": files[0].key,
        "last_chunk": files[-1].key,
        "retrieved_first_utc": retrieved[0],
        "retrieved_last_utc": retrieved[-1],
        "sha256_of_sorted_file_sha256": digest,
    }


def grid_report(
    frame: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, column: str = "timestamp_utc"
) -> dict[str, Any]:
    expected = settlement_grid(start, end)["timestamp_utc"]
    present = pd.DatetimeIndex(pd.to_datetime(frame[column], utc=True))
    missing = expected[~expected.isin(present)]
    report: dict[str, Any] = {
        "rows": int(len(frame)),
        "expected_rows": int(len(expected)),
        "missing_intervals": int(len(missing)),
        "unique": bool(present.is_unique),
        "sorted": bool(present.is_monotonic_increasing),
    }
    if len(missing):
        days = pd.Series(missing.dt.strftime("%Y-%m-%d")).value_counts().sort_index()
        report["missing_by_utc_day"] = {str(k): int(v) for k, v in days.head(40).items()}
    return report


def _common_meta(spec_key: str, retrieved: Iterable[str], snapshot_end: pd.Timestamp) -> dict[str, Any]:
    spec = GB_TABLES[spec_key]
    stamps = sorted(retrieved)
    return {
        "table": spec_key,
        "title": spec.title,
        "time_column": spec.time_column,
        "redistributed": True,
        "retrieved_at_utc": stamps[-1] if stamps else None,
        "fetch_command": spec.fetch_command(),
        "generated_by": GENERATOR,
        "snapshot_end_utc": _iso(snapshot_end),
    }


def _bmrs_meta(years: Sequence[int]) -> dict[str, Any]:
    return {
        "source": "Elexon BMRS Insights Solution API (data.elexon.co.uk)",
        "licence": BMRS_LICENCE,
        "licence_url": BMRS_LICENCE_URL,
        "licence_terms": BMRS_LICENCE_TERMS,
        "attribution": bmrs_attribution(*years),
    }


# ------------------------------------------------------------------------------------------- system prices


def build_system_prices(ctx: GBBuildContext) -> WrittenTable:
    days = pd.date_range(
        ctx.start.tz_convert("Europe/London").date(),
        (ctx.now - pd.Timedelta(hours=1)).tz_convert("Europe/London").date(),
        freq="D",
    )
    files: list[elexon.RawFile] = []
    failures: list[str] = []
    for day in days:
        try:
            files.append(
                elexon.fetch_system_prices_day(
                    ctx.client, ctx.raw_dir, f"{day:%Y-%m-%d}", refresh=ctx.refresh, offline=ctx.offline
                )
            )
        except (FetchError, FileNotFoundError) as exc:
            LOGGER.warning("system prices %s: %s", f"{day:%Y-%m-%d}", exc)
            failures.append(f"{day:%Y-%m-%d}: {exc}")
    frame = elexon.parse_system_prices(elexon.read_payloads(files))
    frame = frame[(frame["timestamp_utc"] >= ctx.start) & (frame["timestamp_utc"] < ctx.now)].reset_index(drop=True)
    lag = frame["publication_lag_minutes"].to_numpy(dtype=float)
    initial = lag[lag < 120.0]
    validation: dict[str, Any] = {
        "grid": grid_report(frame, ctx.start, ctx.now),
        "ssp_ne_sbp_rows": int((frame["system_sell_price_gbp_mwh"] != frame["system_buy_price_gbp_mwh"]).sum()),
        "niv_positive_share": float((frame["niv_mwh"] > 0).mean()) if len(frame) else None,
        "price_derivation_code_counts": {
            str(k): int(v) for k, v in frame["price_derivation_code"].value_counts(dropna=False).items()
        },
        "publication_lag_minutes_after_sp_end": {
            "note": (
                "createdDateTime of the latest settlement-run message. Rows still on their first publication "
                "(lag < 120 min) show the indicative publication delay; most rows were re-issued about 24 h later."
            ),
            "first_publication_rows": int(len(initial)),
            "first_publication_median": float(np.median(initial)) if len(initial) else None,
            "first_publication_p95": float(np.percentile(initial, 95)) if len(initial) else None,
            "first_publication_max": float(initial.max()) if len(initial) else None,
            "reissued_rows_share": float((lag >= 120.0).mean()) if len(lag) else None,
        },
        "failures": failures,
    }
    meta = {
        **_common_meta("gb_system_prices", [f.record.retrieved_at_utc for f in files], ctx.now),
        **_bmrs_meta(ctx.years),
        "dataset_ids": ["DISEBSP"],
        "dataset_pages": [ELEXON_DATASET_PAGES["DISEBSP"]],
        "source_url_template": ELEXON_SYSTEM_PRICES_URL + "?format=json",
        "transformation_notes": [
            "timestamp_utc = start of the 30-minute settlement period (UTC, from startTime); settlement_date and "
            "settlement_period are the Europe/London settlement day and 1-based period (46/50 on clock-change days).",
            "system_price_gbp_mwh is the single cash-out price (system sell price; equal to the system buy price "
            "since BSC P305, 5 Nov 2015 - see validation.ssp_ne_sbp_rows).",
            "niv_mwh > 0 means the system was short (net offer acceptances); < 0 long.",
            "Values are from the latest settlement-run message returned by the API; published_utc is its "
            "createdDateTime and publication_lag_minutes = published_utc - SP end.",
            "Numeric columns are float32.",
        ],
        "validation": validation,
        "inputs_summary": inputs_summary(files),
    }
    start, end = coverage(frame, "timestamp_utc", step=pd.Timedelta(minutes=SP_MINUTES))
    meta.update({"coverage_start_utc": start, "coverage_end_utc": end})
    return write_table(frame, ctx.root, GB_TABLES["gb_system_prices"].relpath(), meta)


# ----------------------------------------------------------------------------------------------------- MID


def build_market_index(ctx: GBBuildContext) -> WrittenTable:
    files = [
        elexon.fetch_mid_month(ctx.client, ctx.raw_dir, month, refresh=ctx.refresh, offline=ctx.offline)
        for month in elexon.month_starts_utc(ctx.start, ctx.now)
    ]
    frame = elexon.parse_market_index(elexon.read_payloads(files))
    frame = frame[(frame["timestamp_utc"] >= ctx.start) & (frame["timestamp_utc"] < ctx.now)].reset_index(drop=True)
    validation = {
        "grid": grid_report(frame, ctx.start, ctx.now),
        "mid_missing_rows": int(frame["mid_price_gbp_mwh"].isna().sum()),
        "apx_traded_rows": int((frame["apx_volume_mwh"] > 0).sum()),
        "n2ex_traded_rows": int((frame["n2ex_volume_mwh"] > 0).sum()),
        "negative_mid_rows": int((frame["mid_price_gbp_mwh"] < 0).sum()),
    }
    meta = {
        **_common_meta("gb_market_index", [f.record.retrieved_at_utc for f in files], ctx.now),
        **_bmrs_meta(ctx.years),
        "dataset_ids": ["MID"],
        "dataset_pages": [ELEXON_DATASET_PAGES["MID"]],
        "source_url_template": ELEXON_MID_STREAM_URL + "?from=YYYY-MM-01T00:00Z&to=<next month>T00:00Z",
        "upstream_note": (
            "Market Index Data are calculated by the Market Index Data Providers (EPEX SPOT 'APXMIDP', Nord Pool "
            "'N2EXMIDP') under the Market Index Definition Statement (BSC) and published on BMRS: volume-weighted "
            "prices of the half-hour, 1-, 2- and 4-hour products traded within eight hours before gate closure "
            "(day-ahead auction excluded). Redistributed here as BMRS data."
        ),
        "transformation_notes": [
            "timestamp_utc = SP start (UTC). Provider rows with zero volume (below the Individual Liquidity "
            "Threshold, reported as price 0) are treated as missing.",
            "mid_price_gbp_mwh = volume-weighted average of the provider prices with volume > 0 (NaN if none); "
            "mid_volume_mwh = their total volume. In practice N2EX volumes are zero, so the MID equals the APX value.",
            "Monthly stream requests overlap at month boundaries; duplicates are dropped. Numeric columns float32.",
        ],
        "validation": validation,
        "inputs": [_file_entry(f.key, f.record) for f in files],
    }
    start, end = coverage(frame, "timestamp_utc", step=pd.Timedelta(minutes=SP_MINUTES))
    meta.update({"coverage_start_utc": start, "coverage_end_utc": end})
    return write_table(frame, ctx.root, GB_TABLES["gb_market_index"].relpath(), meta)


# ----------------------------------------------------------------------------------------------------- EAC


def build_eac(ctx: GBBuildContext) -> list[WrittenTable]:
    resources, package_record = neso.fetch_eac_summaries(
        ctx.client, ctx.raw_dir, now=ctx.now, refresh=ctx.refresh, offline=ctx.offline
    )
    frames = [neso.read_eac_csv(r.path) for r in resources]
    response, reserve, stats = neso.normalize_eac(frames)
    end_limit = ctx.now + pd.Timedelta(days=2)  # day-ahead results for the next EFA day are already published
    response = response[(response["delivery_start_utc"] >= ctx.start - pd.Timedelta(days=60))]
    response = response[response["delivery_start_utc"] < end_limit].reset_index(drop=True)
    reserve = reserve[reserve["delivery_start_utc"] < end_limit].reset_index(drop=True)
    inputs = [
        {**_file_entry(r.name, r.record), "resource_id": r.resource_id, "archived": r.archived} for r in resources
    ]
    retrieved = [r.record.retrieved_at_utc for r in resources] + [package_record.retrieved_at_utc]
    common = {
        "source": "NESO data portal (api.neso.energy, CKAN)",
        "dataset_ids": [NESO_EAC_PACKAGE],
        "dataset_pages": [NESO_EAC_PAGE],
        "source_url_template": NESO_DATASTORE_DUMP_URL,
        "licence": NESO_LICENCE,
        "licence_url": NESO_LICENCE_URL,
        "licence_terms": NESO_LICENCE_TERMS,
        "attribution": NESO_ATTRIBUTION,
        "inputs": inputs,
        "package_show": _file_entry("package_show", package_record),
    }
    written: list[WrittenTable] = []
    meta_r = {
        **_common_meta("gb_response_auctions", retrieved, ctx.now),
        **common,
        "transformation_notes": [
            "Results Summary resources (live, daily and yearly archives) of the EAC package are concatenated; "
            "deliveryStart/deliveryEnd are UTC per the NESO dataset notes; duplicates on (product, delivery start) "
            "keep the highest auctionID.",
            "Only the dynamic response products are kept: DCL/DCH (Dynamic Containment low/high), DML/DMH "
            "(Moderation), DRL/DRH (Regulation). Low = response to low frequency (export), high = import.",
            "efa_date/efa_block derived from delivery_start_utc (EFA day starts 23:00 Europe/London; 4-hour "
            "blocks, 3 or 5 hours on clock-change days). Prices are pay-as-clear in GBP per MW per hour of "
            "availability and can be negative.",
        ],
        "validation": {**stats, **neso.response_block_completeness(response)},
    }
    start, end = coverage(response, "delivery_start_utc", end_column="delivery_end_utc")
    meta_r.update({"coverage_start_utc": start, "coverage_end_utc": end})
    written.append(write_table(response, ctx.root, GB_TABLES["gb_response_auctions"].relpath(), meta_r))
    meta_q = {
        **_common_meta("gb_reserve_auctions", retrieved, ctx.now),
        **common,
        "transformation_notes": [
            "Same source as gb/response_auctions; only the reserve products are kept: PQR/NQR (Quick Reserve "
            "positive/negative), PSR/NSR (Slow Reserve), PBR/NBR (Balancing Reserve). Context only: these products "
            "are not used by the GB strategies in this repository.",
        ],
        "validation": {
            "rows_by_product": {str(k): int(v) for k, v in reserve["product"].value_counts().sort_index().items()},
            "first_delivery_by_product": {
                str(k): _iso(v) for k, v in reserve.groupby("product")["delivery_start_utc"].min().items()
            },
        },
    }
    start, end = coverage(reserve, "delivery_start_utc", end_column="delivery_end_utc")
    meta_q.update({"coverage_start_utc": start, "coverage_end_utc": end})
    written.append(write_table(reserve, ctx.root, GB_TABLES["gb_reserve_auctions"].relpath(), meta_q))
    return written


# ------------------------------------------------------------------------------------------------ frequency


def build_frequency(ctx: GBBuildContext) -> list[WrittenTable]:
    files = [
        elexon.fetch_frequency_month(ctx.client, ctx.raw_dir, month, refresh=ctx.refresh, offline=ctx.offline)
        for month in elexon.month_starts_utc(ctx.start, ctx.now)
    ]
    written: list[WrittenTable] = []
    energy_parts: list[pd.DataFrame] = []
    by_year: dict[int, list[elexon.RawFile]] = {}
    for f in files:
        by_year.setdefault(int(f.key[:4]), []).append(f)
    for year, year_files in sorted(by_year.items()):
        lo = max(ctx.start, pd.Timestamp(year=year, month=1, day=1, tz="UTC"))
        hi = min(ctx.now, pd.Timestamp(year=year + 1, month=1, day=1, tz="UTC"))
        # Neighbouring months may contain the boundary sample; parse this year's files only.
        freq, parse_stats = elexon.parse_frequency_with_stats(elexon.read_payloads(year_files))
        freq = freq[(freq["timestamp_utc"] >= lo) & (freq["timestamp_utc"] < hi)].reset_index(drop=True)
        expected = int((hi - lo) / pd.Timedelta(seconds=frequency.SAMPLE_SECONDS))
        steps = freq["timestamp_utc"].diff().dt.total_seconds().dropna()
        validation = {
            "rows": int(len(freq)),
            "expected_rows_15s": expected,
            "missing_samples": int(expected - len(freq)),
            "gaps_over_60s": int((steps > 60).sum()),
            "largest_gap_seconds": float(steps.max()) if len(steps) else None,
            "invalid_samples_dropped_outside_48_52_hz": parse_stats["invalid_samples_dropped"],
            "min_dev_mhz": int(freq["frequency_dev_mhz"].min()) if len(freq) else None,
            "max_dev_mhz": int(freq["frequency_dev_mhz"].max()) if len(freq) else None,
        }
        meta = {
            **_common_meta("gb_frequency", [f.record.retrieved_at_utc for f in year_files], ctx.now),
            **_bmrs_meta([year]),
            "dataset_ids": ["FREQ"],
            "dataset_pages": [ELEXON_DATASET_PAGES["FREQ"]],
            "source_url_template": ELEXON_FREQUENCY_STREAM_URL + "?from=YYYY-MM-01T00:00Z&to=<next month>T00:00Z",
            "partition_year": year,
            "transformation_notes": [
                "timestamp_utc = measurementTime (UTC, 15-second samples). Samples outside 48-52 Hz (telemetry "
                "zeros) are dropped (validation.invalid_samples_dropped_outside_48_52_hz).",
                "frequency_dev_mhz = round((frequency_hz - 50) * 1000) as int16 (the source has three decimals, so "
                "this is lossless); frequency_hz = 50 + frequency_dev_mhz / 1000.",
            ],
            "validation": validation,
            "inputs": [_file_entry(f.key, f.record) for f in year_files],
        }
        start, end = coverage(freq, "timestamp_utc", step=pd.Timedelta(seconds=frequency.SAMPLE_SECONDS))
        meta.update({"coverage_start_utc": start, "coverage_end_utc": end})
        written.append(write_table(freq, ctx.root, GB_TABLES["gb_frequency"].relpath(year), meta))
        grid = settlement_grid(lo, hi)
        energy = frequency.response_energy_per_sp(freq, pd.DatetimeIndex(grid["timestamp_utc"]))
        energy.insert(1, "settlement_date", grid["settlement_date"].to_numpy())
        energy.insert(2, "settlement_period", grid["settlement_period"].to_numpy())
        energy_parts.append(energy)
    energy_all = pd.concat(energy_parts, ignore_index=True) if energy_parts else pd.DataFrame()
    mean_energy = {c: float(energy_all[c].mean()) for c in energy_all.columns if c.endswith("_mwh_per_mw")}
    meta_e = {
        **_common_meta("gb_response_energy", [f.record.retrieved_at_utc for f in files], ctx.now),
        **_bmrs_meta(ctx.years),
        "dataset_ids": ["FREQ"],
        "dataset_pages": [ELEXON_DATASET_PAGES["FREQ"]],
        "derived_from": [GB_TABLES["gb_frequency"].pattern],
        "method_reference": "NESO Response Services Service Terms, Schedule 2 Table 1 and para 6.7 (delivery curves)",
        "transformation_notes": [
            "For each SP and product (DCL, DCH, DML, DMH, DRL, DRH): MWh delivered per MW contracted = mean over the "
            "SP's 15-second samples of the product's delivery fraction x 0.5 h (sample-and-hold).",
            "Delivery curves: deadband +/-0.015 Hz; DC 5 % at +/-0.2 Hz and 100 % at +/-0.5 Hz; DM 5 % at +/-0.1 Hz "
            "and 100 % at +/-0.2 Hz; DR linear to 100 % at +/-0.2 Hz; linear interpolation, saturation beyond.",
            "Low products export (frequency below 50 Hz), high products import; values are non-negative.",
            "SPs with fewer than 60 of 120 samples are NaN. freq_* columns are per-SP statistics of the deviation "
            "from 50 Hz in mHz; share_outside_200mhz is the share of samples with |deviation| > 0.2 Hz.",
            "15-second sampling misses sub-15-second excursions (mostly relevant for DC); this is an approximation "
            "of the 1-second performance-monitoring view.",
        ],
        "validation": {
            "grid": grid_report(energy_all, ctx.start, ctx.now) if len(energy_all) else {},
            "nan_rows": int(energy_all["drl_mwh_per_mw"].isna().sum()) if len(energy_all) else None,
            "mean_mwh_per_mw_per_sp": mean_energy,
        },
    }
    start, end = coverage(energy_all, "timestamp_utc", step=pd.Timedelta(minutes=SP_MINUTES))
    meta_e.update({"coverage_start_utc": start, "coverage_end_utc": end})
    written.append(write_table(energy_all, ctx.root, GB_TABLES["gb_response_energy"].relpath(), meta_e))
    return written


# ------------------------------------------------------------------------------------------------------ FX


def build_fx(ctx: GBBuildContext) -> WrittenTable:
    path, record = ecb.fetch_ecb_gbp(ctx.client, ctx.raw_dir, now=ctx.now, refresh=ctx.refresh, offline=ctx.offline)
    frame = ecb.parse_ecb_csv(path.read_text(encoding="utf-8"))
    frame = frame[frame["timestamp_utc"] < ctx.now].reset_index(drop=True)
    meta = {
        **_common_meta("gb_fx", [record.retrieved_at_utc], ctx.now),
        "source": "European Central Bank data portal (data-api.ecb.europa.eu), series EXR.D.GBP.EUR.SP00.A",
        "dataset_ids": ["EXR.D.GBP.EUR.SP00.A"],
        "dataset_pages": [ECB_DATASET_PAGE],
        "source_urls": [record.full_url],
        "licence": ECB_LICENCE,
        "licence_url": ECB_LICENCE_URL,
        "licence_terms": ECB_LICENCE_TERMS,
        "attribution": ECB_ATTRIBUTION,
        "transformation_notes": [
            "timestamp_utc = reference date 00:00 UTC (rates are set around 16:00 CET on TARGET business days).",
            "gbp_per_eur = OBS_VALUE (pound sterling per euro). Consumers as-of join the latest rate dated on or "
            "before each delivery date (forward fill over weekends and holidays).",
        ],
        "validation": {
            "rows": int(len(frame)),
            "min_gbp_per_eur": float(frame["gbp_per_eur"].min()) if len(frame) else None,
            "max_gbp_per_eur": float(frame["gbp_per_eur"].max()) if len(frame) else None,
        },
        "inputs": [_file_entry("exr_d_gbp_eur", record)],
    }
    start, end = coverage(frame, "timestamp_utc", step=pd.Timedelta(days=1))
    meta.update({"coverage_start_utc": start, "coverage_end_utc": end})
    return write_table(frame, ctx.root, GB_TABLES["gb_fx"].relpath(), meta)


BUILDERS = {
    "system-prices": lambda ctx: [build_system_prices(ctx)],
    "market-index": lambda ctx: [build_market_index(ctx)],
    "eac": build_eac,
    "frequency": build_frequency,
    "fx": lambda ctx: [build_fx(ctx)],
}


def build_gb(targets: Sequence[str], ctx: GBBuildContext | None = None) -> list[WrittenTable]:
    """Build the given targets (``all`` for every target) and return the written tables."""

    context = ctx or GBBuildContext()
    names = list(FETCH_TARGETS) if list(targets) == ["all"] else list(targets)
    unknown = [n for n in names if n not in FETCH_TARGETS]
    if unknown:
        raise ValueError(f"Unknown GB fetch target(s) {unknown}; expected {', '.join(FETCH_TARGETS)} or all")
    written: list[WrittenTable] = []
    for name in names:
        LOGGER.info("Building GB target %s", name)
        written.extend(BUILDERS[name](context))
    return written


def describe(written: Sequence[WrittenTable]) -> str:
    return json.dumps([{"path": w.relpath, "rows": w.rows, "size_bytes": w.size_bytes} for w in written], indent=2)
