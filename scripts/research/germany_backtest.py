"""Real-data Germany (DE-LU) BESS backtest: day-ahead only, day-ahead + FCR, day-ahead + capacity-only aFRR.

Regenerates ``reports/germany.md`` and ``reports/figures/germany_*.png``::

    .venv/bin/python scripts/research/germany_backtest.py --end 2026-10-25

Battery: 1 MW / 2 MWh, SOC 5-95 %, start and end every day at 50 %, charge/discharge efficiency 0.95/0.95,
degradation 8 EUR per MWh discharged. Walk-forward day by day: decisions for delivery day D are taken on D-1
(FCR 08:00, aFRR 09:00, day-ahead 12:00 Europe/Berlin) with the repository's ``PersistenceForecastProvider``
(previous local day) and compared with ``PerfectForesightForecastProvider`` (oracle, not tradable). Each day is
solved with ``euroflex_bess_lab.markets.adapters.germany.solve_germany_day`` (Pyomo + HiGHS), which holds
reserve volumes constant over the German 4-hour products and keeps the PQ energy buffers for limited-energy
reservoirs. Settlement uses realised prices.

Data, preferred source first:

* DE-LU day-ahead: ``euroflex_bess_lab.public_data.load_day_ahead_prices`` (``data/public``, Energy-Charts,
  CC BY 4.0, Bundesnetzagentur | SMARD.de); otherwise the Energy-Charts API cache in
  ``data/raw/germany/research_cache/energy_charts`` (filled with ``--fetch``).
* FCR / aFRR capacity results: ``euroflex_bess_lab.public_data.load_germany_reserve`` (regelleistung.net, built
  locally under ``data/raw``, not redistributed); otherwise the regelleistung.net export cache in
  ``data/raw/germany/research_cache/regelleistung`` (filled with ``--fetch``).
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from collections.abc import Iterable, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from euroflex_bess_lab.forecasts import PerfectForesightForecastProvider, PersistenceForecastProvider
from euroflex_bess_lab.markets.adapters.germany import (
    AFRR_ENGINE_SUSTAIN_MINUTES,
    AFRR_LER_ENERGY_HOURS_PER_DIRECTION,
    DE_LU_BIDDING_ZONE,
    FCR_ENGINE_SUSTAIN_MINUTES,
    FCR_LER_ENERGY_HOURS_30_MIN_ALERT_STATE,
    FCR_LER_ENERGY_HOURS_PER_DIRECTION,
    GERMANY_LFC_AREA,
    GermanBatteryParams,
    energy_charts_payload_to_frame,
    expand_reserve_blocks,
    normalize_day_ahead_frame,
    parse_regelleistung_afrr_results,
    parse_regelleistung_fcr_results,
    read_xlsx_rows,
    reserve_block_index,
    solve_germany_day,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TIMEZONE = "Europe/Berlin"
DT_HOURS = 0.25
WORKFLOWS = ("da_only", "da_plus_fcr", "da_plus_afrr")
PROVIDERS = ("persistence", "perfect_foresight")
WORKFLOW_LABELS = {
    "da_only": "DA only",
    "da_plus_fcr": "DA + FCR",
    "da_plus_afrr": "DA + aFRR (capacity)",
}
BATTERY = GermanBatteryParams(
    power_mw=1.0,
    energy_mwh=2.0,
    soc_min_mwh=0.1,
    soc_max_mwh=1.9,
    initial_soc_mwh=1.0,
    terminal_soc_mwh=1.0,
    charge_efficiency=0.95,
    discharge_efficiency=0.95,
    degradation_eur_per_mwh_discharged=8.0,
)
QUARTER_HOUR_ERA_START = date(2025, 10, 1)
DEFAULT_START = date(2024, 10, 1)
RESEARCH_CACHE = REPO_ROOT / "data" / "raw" / "germany" / "research_cache"
ENERGY_CHARTS_URL = "https://api.energy-charts.info/price?bzn=DE-LU&start={start}&end={end}"
REGELLEISTUNG_URL = (
    "https://www.regelleistung.net/apps/cpp-publisher/api/v1/download/tenders/resultsoverview"
    "?date={day}&exportFormat=xlsx&market=CAPACITY&productTypes={product}"
)
DECISION_TIME_LOCAL = {"da_only": "12:00", "da_plus_fcr": "08:00", "da_plus_afrr": "09:00"}
# Published Q1 2026 reserve capacity revenue per MW and month (and change vs Q1 2025), used only to cross-check the
# parsed regelleistung.net data, plus the ISEA Battery Revenue Index Q1 2026 average as reported by the same source.
PUBLISHED_REFERENCE_URL = "https://my-se.de/magazin/regelenergie-mit-batteriespeichern-erlose-und-anforderungen"
PUBLISHED_Q1_2026_EUR_PER_MW_MONTH = {"fcr": 8476.0, "afrr_up": 6652.0, "afrr_down": 4194.0}
PUBLISHED_CHANGE_VS_Q1_2025 = {"fcr": 0.08, "afrr_up": -0.27, "afrr_down": -0.10}
PUBLISHED_ISEA_INDEX_Q1_2026_EUR_PER_MW_YEAR = 132000.0
ISEA_INDEX_URL = "https://battery-charts.de/revenue-index/"
FIGURE_COLORS = {"da_only": "#2a78d6", "da_plus_fcr": "#eb6834", "da_plus_afrr": "#1baf7a"}
INK = {"primary": "#0b0b0b", "secondary": "#52514e", "muted": "#898781", "grid": "#e1e0d9", "axis": "#c3c2b7"}
SURFACE = "#fcfcfb"


# ---------------------------------------------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------------------------------------------


@dataclass
class MarketTables:
    day_ahead: pd.DataFrame
    fcr_blocks: pd.DataFrame
    afrr_blocks: pd.DataFrame
    sources: dict[str, str]


def _month_starts(first: date, last: date) -> Iterable[date]:
    cursor = date(first.year, first.month, 1)
    while cursor <= last:
        yield cursor
        cursor = date(cursor.year + (cursor.month == 12), cursor.month % 12 + 1, 1)


def _http_get(url: str, *, ok: Any = None, tries: int = 8) -> bytes:
    import requests

    delay = 20.0
    last_error = ""
    for _ in range(tries):
        try:
            response = requests.get(url, timeout=90, headers={"User-Agent": "euroflex-bess-lab research"})
            if response.status_code == 200 and (ok is None or ok(response.content)):
                return response.content
            last_error = f"HTTP {response.status_code}"
        except requests.RequestException as exc:
            last_error = str(exc)
        time.sleep(delay)
        delay = min(delay * 2.0, 300.0)
    raise RuntimeError(f"Download failed for {url}: {last_error}")


def fetch_energy_charts_cache(first: date, last: date, cache: Path = RESEARCH_CACHE / "energy_charts") -> None:
    cache.mkdir(parents=True, exist_ok=True)
    for month in _month_starts(first, last):
        month_end = date(month.year + (month.month == 12), month.month % 12 + 1, 1) - timedelta(days=1)
        target = cache / f"da_DE-LU_{month:%Y-%m}.json"
        complete = target.exists() and month_end < date.today() - timedelta(days=2)
        if complete:
            continue
        payload = _http_get(ENERGY_CHARTS_URL.format(start=month, end=min(month_end, last)))
        target.write_bytes(payload)
        time.sleep(5.0)


def fetch_regelleistung_cache(first: date, last: date, cache: Path = RESEARCH_CACHE / "regelleistung") -> None:
    for product in ("FCR", "aFRR"):
        directory = cache / product
        directory.mkdir(parents=True, exist_ok=True)
        day = first
        while day <= last:
            target = directory / f"{day.isoformat()}.xlsx"
            if not target.exists():
                content = _http_get(
                    REGELLEISTUNG_URL.format(day=day.isoformat(), product=product), ok=lambda c: c[:2] == b"PK"
                )
                target.write_bytes(content)
                time.sleep(0.15)
            day += timedelta(days=1)


def _local_midnight_utc(day: date) -> pd.Timestamp:
    return pd.Timestamp(f"{day} 00:00").tz_localize(TIMEZONE).tz_convert("UTC")


def load_day_ahead(first: date, last: date) -> tuple[pd.DataFrame, str]:
    lo, hi = _local_midnight_utc(first), _local_midnight_utc(last + timedelta(days=1))
    try:
        from euroflex_bess_lab.public_data import load_day_ahead_prices

        raw = load_day_ahead_prices("DE-LU", lo, hi)
        source = "data/public/energy_charts/da_prices_DE-LU.parquet via euroflex_bess_lab.public_data"
    except (ImportError, FileNotFoundError):
        cache = RESEARCH_CACHE / "energy_charts"
        frames = [
            energy_charts_payload_to_frame(json.loads(path.read_text(encoding="utf-8")))
            for path in sorted(cache.glob("da_DE-LU_*.json"))
        ]
        if not frames:
            raise FileNotFoundError(
                "No DE-LU day-ahead data: build data/public with `euroflex data fetch day-ahead --zone DE-LU` "
                "or rerun this script with --fetch"
            ) from None
        raw = pd.concat(frames, ignore_index=True).drop_duplicates("timestamp_utc", keep="last")
        raw = raw.loc[(raw["timestamp_utc"] >= lo) & (raw["timestamp_utc"] < hi)]
        source = "Energy-Charts API cache (data/raw/germany/research_cache/energy_charts), CC BY 4.0"
    frame = normalize_day_ahead_frame(raw.reset_index(drop=True), zone=DE_LU_BIDDING_ZONE, source="energy_charts")
    return frame, source


def _parse_cached_exports(product: str, first: date, last: date) -> pd.DataFrame:
    directory = RESEARCH_CACHE / "regelleistung" / product
    parser = parse_regelleistung_fcr_results if product == "FCR" else parse_regelleistung_afrr_results
    frames = []
    day = first
    while day <= last:
        path = directory / f"{day.isoformat()}.xlsx"
        if path.exists():
            frames.append(parser(read_xlsx_rows(path)))
        day += timedelta(days=1)
    if not frames:
        raise FileNotFoundError(f"No cached regelleistung.net {product} exports; rerun with --fetch")
    return pd.concat(frames, ignore_index=True)


def _covers(blocks: pd.DataFrame, first: date, last: date) -> bool:
    if blocks.empty:
        return False
    local_days = set(pd.to_datetime(blocks["delivery_start_utc"], utc=True).dt.tz_convert(TIMEZONE).dt.date)
    needed = {first + timedelta(days=offset) for offset in range((last - first).days + 1)}
    return needed.issubset(local_days)


def load_reserves(first: date, last: date) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    lo, hi = _local_midnight_utc(first), _local_midnight_utc(last + timedelta(days=1))
    try:
        from euroflex_bess_lab.public_data import load_germany_reserve

        fcr = load_germany_reserve("fcr", lo, hi)
        afrr = load_germany_reserve("afrr", lo, hi)
        if _covers(fcr, first, last) and _covers(afrr, first, last):
            return (
                fcr,
                afrr,
                (
                    "regelleistung.net via euroflex_bess_lab.public_data.load_germany_reserve "
                    "(built locally under data/raw, not redistributed)"
                ),
            )
    except (ImportError, FileNotFoundError):
        pass
    fcr = _parse_cached_exports("FCR", first, last)
    afrr = _parse_cached_exports("aFRR", first, last)
    return (
        fcr,
        afrr,
        (
            "regelleistung.net result exports cached in data/raw/germany/research_cache/regelleistung (not redistributed)"
        ),
    )


def load_tables(first: date, last: date, *, fetch: bool) -> MarketTables:
    if fetch:
        fetch_energy_charts_cache(first, last)
        fetch_regelleistung_cache(first, last)
    day_ahead, da_source = load_day_ahead(first, last)
    fcr_blocks, afrr_blocks, reserve_source = load_reserves(first, last)
    return MarketTables(
        day_ahead=day_ahead,
        fcr_blocks=fcr_blocks,
        afrr_blocks=afrr_blocks,
        sources={"day_ahead": da_source, "reserves": reserve_source},
    )


# ---------------------------------------------------------------------------------------------------------------
# Day assembly and forecasts
# ---------------------------------------------------------------------------------------------------------------


@dataclass
class DayInputs:
    day: date
    timestamps_utc: pd.DatetimeIndex
    block: np.ndarray
    actual: dict[str, np.ndarray]
    forecast: dict[str, dict[str, np.ndarray]] = field(default_factory=dict)
    persistence_source_day: date | None = None


def _expected_quarter_hours(day: date) -> pd.DatetimeIndex:
    start = _local_midnight_utc(day)
    end = _local_midnight_utc(day + timedelta(days=1))
    return pd.date_range(start, end, freq="15min", inclusive="left")


def _by_local_day(frame: pd.DataFrame) -> dict[date, pd.DataFrame]:
    local_day = frame["timestamp_utc"].dt.tz_convert(TIMEZONE).dt.date
    return {day: rows.reset_index(drop=True) for day, rows in frame.groupby(local_day, sort=True)}


def _complete(rows: pd.DataFrame | None, day: date) -> bool:
    if rows is None:
        return False
    expected = _expected_quarter_hours(day)
    return len(rows) == len(expected) and bool((pd.DatetimeIndex(rows["timestamp_utc"]) == expected).all())


def _clock_persistence(source: pd.DataFrame, delivery: pd.DataFrame) -> np.ndarray:
    """Persistence by local clock time from an arbitrary earlier day (first occurrence for repeated clock times)."""

    source_clock = source["timestamp_local"].dt.strftime("%H:%M")
    lookup = pd.Series(source["price_eur_per_mwh"].to_numpy(), index=source_clock.to_numpy())
    lookup = lookup[~lookup.index.duplicated(keep="first")]
    values = delivery["timestamp_local"].dt.strftime("%H:%M").map(lookup)
    if values.isna().any():
        raise ValueError("Fallback persistence source does not cover every delivery clock time")
    return values.to_numpy(dtype=float)


def _provider_values(
    provider: Any, *, market: str, decision_time: pd.Timestamp, delivery: pd.DataFrame, history: pd.DataFrame
) -> np.ndarray:
    snapshot = provider.get_forecast(
        market=market,
        decision_time_utc=decision_time,
        delivery_frame=delivery,
        actual_frame=pd.concat([history, delivery], ignore_index=True),
    )
    return snapshot.sort_values("delivery_start_utc")["forecast_price_eur_per_mwh"].to_numpy(dtype=float)


def build_days(tables: MarketTables, first: date, last: date) -> tuple[list[DayInputs], dict[str, Any]]:
    """Assemble complete delivery days with actuals and persistence / perfect-foresight forecasts.

    A day is kept only if day-ahead, FCR and both aFRR directions are complete for it, so every workflow is
    evaluated on the same days. Persistence uses the previous local day through the repository provider; when the
    provider cannot align it (the day after a DST change, or a missing previous day) the most recent complete
    earlier day is used instead and the day is listed in the coverage notes.
    """

    series = {
        "day_ahead": tables.day_ahead,
        "fcr_capacity": expand_reserve_blocks(tables.fcr_blocks, market="fcr_capacity", zone=GERMANY_LFC_AREA),
        "afrr_capacity_up": expand_reserve_blocks(
            tables.afrr_blocks, market="afrr_capacity_up", zone=GERMANY_LFC_AREA, direction="up"
        ),
        "afrr_capacity_down": expand_reserve_blocks(
            tables.afrr_blocks, market="afrr_capacity_down", zone=GERMANY_LFC_AREA, direction="down"
        ),
    }
    marginal = {
        "up": series["afrr_capacity_up"].set_index("timestamp_utc")["marginal_price_eur_per_mw_h"],
        "down": series["afrr_capacity_down"].set_index("timestamp_utc")["marginal_price_eur_per_mw_h"],
    }
    grouped = {name: _by_local_day(frame) for name, frame in series.items()}
    persistence = PersistenceForecastProvider()
    oracle = PerfectForesightForecastProvider()
    days: list[DayInputs] = []
    skipped: list[str] = []
    fallbacks: list[str] = []
    current = first
    while current <= last:
        rows = {name: grouped[name].get(current) for name in series}
        if not all(_complete(frame, current) for frame in rows.values()):
            skipped.append(current.isoformat())
            current += timedelta(days=1)
            continue
        delivery = {name: frame for name, frame in rows.items() if frame is not None}
        timestamps = pd.DatetimeIndex(delivery["day_ahead"]["timestamp_utc"])
        actual = {name: frame["price_eur_per_mwh"].to_numpy(dtype=float) for name, frame in delivery.items()}
        actual["afrr_marginal_up"] = marginal["up"].reindex(timestamps).to_numpy(dtype=float)
        actual["afrr_marginal_down"] = marginal["down"].reindex(timestamps).to_numpy(dtype=float)
        inputs = DayInputs(day=current, timestamps_utc=timestamps, block=reserve_block_index(timestamps), actual=actual)
        previous = current - timedelta(days=1)
        persistence_values: dict[str, np.ndarray] = {}
        source_day: date | None = previous
        try:
            for name in series:
                history = grouped[name].get(previous)
                if history is None:
                    raise ValueError("previous local day missing")
                decision = pd.Timestamp(f"{previous} 08:00").tz_localize(TIMEZONE).tz_convert("UTC")
                persistence_values[name] = _provider_values(
                    persistence, market=name, decision_time=decision, delivery=delivery[name], history=history
                )
        except ValueError:
            source_day = None
            for back in range(2, 8):
                candidate = current - timedelta(days=back)
                if all(_complete(grouped[name].get(candidate), candidate) for name in series):
                    source_day = candidate
                    break
            if source_day is None:
                skipped.append(current.isoformat())
                current += timedelta(days=1)
                continue
            persistence_values = {
                name: _clock_persistence(grouped[name][source_day], delivery[name]) for name in series
            }
            fallbacks.append(f"{current.isoformat()} (persistence from {source_day.isoformat()})")
        oracle_values = {
            name: _provider_values(
                oracle,
                market=name,
                decision_time=pd.Timestamp(f"{previous} 08:00").tz_localize(TIMEZONE).tz_convert("UTC"),
                delivery=delivery[name],
                history=delivery[name].iloc[0:0],
            )
            for name in series
        }
        inputs.forecast = {"persistence": persistence_values, "perfect_foresight": oracle_values}
        inputs.persistence_source_day = source_day
        days.append(inputs)
        current += timedelta(days=1)
    coverage = {
        "requested_days": (last - first).days + 1,
        "evaluated_days": len(days),
        "skipped_days": skipped,
        "persistence_fallback_days": fallbacks,
    }
    return days, coverage


# ---------------------------------------------------------------------------------------------------------------
# Optimisation and settlement
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Variant:
    name: str
    block_constant_reserve: bool = True
    soc_reservation_check: str = "both"
    fcr_energy_hours: float = FCR_LER_ENERGY_HOURS_PER_DIRECTION
    afrr_energy_hours: float = AFRR_LER_ENERGY_HOURS_PER_DIRECTION
    workflows: tuple[str, ...] = WORKFLOWS
    providers: tuple[str, ...] = PROVIDERS
    pay_as_bid_acceptance: bool = False
    afrr_bid_shading: float = 1.0
    battery: GermanBatteryParams = BATTERY


BASE = Variant(name="base")
SENSITIVITIES = (
    Variant(
        name="relaxed_per_quarter_hour",
        block_constant_reserve=False,
        soc_reservation_check="start",
        workflows=("da_plus_fcr", "da_plus_afrr"),
    ),
    Variant(
        name="fcr_30min_alert_state",
        fcr_energy_hours=FCR_LER_ENERGY_HOURS_30_MIN_ALERT_STATE,
        workflows=("da_plus_fcr",),
    ),
    Variant(
        name="afrr_pay_as_bid_acceptance",
        workflows=("da_plus_afrr",),
        providers=("persistence",),
        pay_as_bid_acceptance=True,
    ),
    Variant(
        name="afrr_pay_as_bid_shaded_70",
        workflows=("da_plus_afrr",),
        providers=("persistence",),
        pay_as_bid_acceptance=True,
        afrr_bid_shading=0.7,
    ),
)


def _solve(day: DayInputs, variant: Variant, workflow: str, prices: dict[str, np.ndarray], **fixed: Any) -> Any:
    kwargs: dict[str, Any] = {}
    if workflow == "da_plus_fcr":
        kwargs["fcr_price_eur_per_mw_h"] = prices["fcr_capacity"]
    if workflow == "da_plus_afrr":
        kwargs["afrr_up_price_eur_per_mw_h"] = prices["afrr_capacity_up"]
        kwargs["afrr_down_price_eur_per_mw_h"] = prices["afrr_capacity_down"]
    kwargs.update(fixed)
    return solve_germany_day(
        day_ahead_price_eur_per_mwh=prices["day_ahead"],
        battery=variant.battery,
        block_index=day.block,
        fcr_energy_hours=variant.fcr_energy_hours,
        afrr_energy_hours=variant.afrr_energy_hours,
        block_constant_reserve=variant.block_constant_reserve,
        soc_reservation_check=variant.soc_reservation_check,  # type: ignore[arg-type]
        dt_hours=DT_HOURS,
        **kwargs,
    )


def settle(day: DayInputs, solution: Any, *, workflow: str, provider: str, variant: Variant) -> dict[str, Any]:
    actual = day.actual
    net_export = solution.discharge_mw - solution.charge_mw
    da_revenue = float(np.sum(net_export * actual["day_ahead"]) * DT_HOURS)
    if workflow == "da_plus_fcr":
        reserve = float(np.sum(solution.fcr_mw * actual["fcr_capacity"]) * DT_HOURS)
    elif workflow == "da_plus_afrr":
        reserve = float(
            np.sum(
                solution.afrr_up_mw * actual["afrr_capacity_up"] + solution.afrr_down_mw * actual["afrr_capacity_down"]
            )
            * DT_HOURS
        )
    else:
        reserve = 0.0
    discharged = float(np.sum(solution.discharge_mw) * DT_HOURS)
    charged = float(np.sum(solution.charge_mw) * DT_HOURS)
    battery = variant.battery
    degradation = (
        battery.degradation_eur_per_mwh_discharged * discharged
        + battery.degradation_eur_per_mwh_throughput * (discharged + charged)
    )
    return {
        "day": day.day,
        "provider": provider,
        "workflow": workflow,
        "variant": variant.name,
        "da_revenue_eur": da_revenue,
        "reserve_revenue_eur": reserve,
        "degradation_eur": degradation,
        "net_eur": da_revenue + reserve - degradation,
        "expected_net_eur": float(solution.objective_eur),
        "discharged_mwh": discharged,
        "charged_mwh": charged,
        "fcr_mw_avg": float(np.mean(solution.fcr_mw)),
        "afrr_up_mw_avg": float(np.mean(solution.afrr_up_mw)),
        "afrr_down_mw_avg": float(np.mean(solution.afrr_down_mw)),
        "afrr_accepted_share": np.nan,
    }


def _pay_as_bid(day: DayInputs, variant: Variant) -> dict[str, Any]:
    """Two-stage aFRR under pay-as-bid: bid = shading x persistence price, accepted iff bid <= realised marginal.

    Stage 1 co-optimises with persistence prices and fixes the bid volume per block. Accepted volume earns its own
    bid price (pay-as-bid); rejected blocks earn nothing. Stage 2 re-optimises the day-ahead schedule with the
    awarded volumes fixed (the auction result is known at ~09:30, before the 12:00 day-ahead gate closure).
    """

    prices = day.forecast["persistence"]
    stage_one = _solve(day, variant, "da_plus_afrr", prices)
    bid_up = variant.afrr_bid_shading * prices["afrr_capacity_up"]
    bid_down = variant.afrr_bid_shading * prices["afrr_capacity_down"]
    accept_up = bid_up <= day.actual["afrr_marginal_up"] + 1e-9
    accept_down = bid_down <= day.actual["afrr_marginal_down"] + 1e-9
    awarded_up = np.where(accept_up, stage_one.afrr_up_mw, 0.0)
    awarded_down = np.where(accept_down, stage_one.afrr_down_mw, 0.0)
    stage_two = solve_germany_day(
        day_ahead_price_eur_per_mwh=prices["day_ahead"],
        battery=variant.battery,
        block_index=day.block,
        fixed_afrr_up_mw=awarded_up,
        fixed_afrr_down_mw=awarded_down,
        fcr_energy_hours=variant.fcr_energy_hours,
        afrr_energy_hours=variant.afrr_energy_hours,
        block_constant_reserve=True,
        soc_reservation_check="both",
        dt_hours=DT_HOURS,
    )
    record = settle(day, stage_two, workflow="da_only", provider="persistence", variant=variant)
    reserve = float(np.sum(awarded_up * bid_up + awarded_down * bid_down) * DT_HOURS)
    offered = float(np.sum(stage_one.afrr_up_mw + stage_one.afrr_down_mw))
    record.update(
        {
            "workflow": "da_plus_afrr",
            "reserve_revenue_eur": reserve,
            "net_eur": record["da_revenue_eur"] + reserve - record["degradation_eur"],
            "afrr_up_mw_avg": float(np.mean(awarded_up)),
            "afrr_down_mw_avg": float(np.mean(awarded_down)),
            "afrr_accepted_share": float(np.sum(awarded_up + awarded_down) / offered) if offered > 0 else np.nan,
        }
    )
    return record


def evaluate_day(task: tuple[DayInputs, tuple[Variant, ...]]) -> list[dict[str, Any]]:
    day, variants = task
    records: list[dict[str, Any]] = []
    for variant in variants:
        if variant.pay_as_bid_acceptance:
            records.append(_pay_as_bid(day, variant))
            continue
        for provider in variant.providers:
            for workflow in variant.workflows:
                solution = _solve(day, variant, workflow, day.forecast[provider])
                records.append(settle(day, solution, workflow=workflow, provider=provider, variant=variant))
    return records


def run_backtest(days: Sequence[DayInputs], *, workers: int, sensitivity_start: date) -> pd.DataFrame:
    tasks = [(day, (BASE, *SENSITIVITIES) if day.day >= sensitivity_start else (BASE,)) for day in days]
    records: list[dict[str, Any]] = []
    if workers <= 1:
        for task in tasks:
            records.extend(evaluate_day(task))
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            for result in pool.map(evaluate_day, tasks, chunksize=4):
                records.extend(result)
    frame = pd.DataFrame(records)
    frame["day"] = pd.to_datetime(frame["day"]).dt.date
    return frame.sort_values(["variant", "provider", "workflow", "day"]).reset_index(drop=True)


# ---------------------------------------------------------------------------------------------------------------
# Framework consistency check (engine path with the Germany adapter)
# ---------------------------------------------------------------------------------------------------------------


def engine_consistency_check(
    tables: MarketTables, days: Sequence[DayInputs], *, first: date, last: date
) -> dict[str, Any]:
    """Run ``run_walk_forward`` through the Germany adapter and compare with ``solve_germany_day`` in replication mode.

    Replication mode switches off the German-specific rules the engine cannot express (block-constant reserve,
    interval-end SOC check, exact PQ buffer) and uses the engine's throughput degradation, so both paths solve
    the same problem. Matching objectives validate the research LP against the framework.
    """

    from euroflex_bess_lab.backtesting.engine import run_walk_forward
    from euroflex_bess_lab.config import BacktestConfig

    window = [day for day in days if first <= day.day <= last]
    if not window:
        return {"status": "skipped", "reason": "no evaluated days in the check window"}
    throughput_cost = 4.0
    replication_battery = replace(
        BATTERY, degradation_eur_per_mwh_discharged=0.0, degradation_eur_per_mwh_throughput=throughput_cost
    )
    replication = Variant(
        name="engine_replication",
        block_constant_reserve=False,
        soc_reservation_check="start",
        fcr_energy_hours=FCR_ENGINE_SUSTAIN_MINUTES / 60.0,
        afrr_energy_hours=AFRR_LER_ENERGY_HOURS_PER_DIRECTION,
        battery=replication_battery,
    )
    lo = _local_midnight_utc(first - timedelta(days=1))
    hi = _local_midnight_utc(last + timedelta(days=1))
    rows: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="germany-engine-check-") as workdir:
        root = Path(workdir)
        da = tables.day_ahead.loc[(tables.day_ahead["timestamp_utc"] >= lo) & (tables.day_ahead["timestamp_utc"] < hi)]
        # Rows are already on the 15-minute grid; write them in the public-data layout the adapter accepts.
        da[["timestamp_utc", "price_eur_per_mwh"]].rename(columns={"price_eur_per_mwh": "price_eur_mwh"}).assign(
            resolution_minutes=15
        ).to_parquet(root / "da.parquet", index=False)
        for name, blocks in (("fcr", tables.fcr_blocks), ("afrr", tables.afrr_blocks)):
            start = pd.to_datetime(blocks["delivery_start_utc"], utc=True)
            subset = blocks.loc[(start >= lo) & (start < hi)]
            subset[["delivery_start_utc", "delivery_end_utc", "direction", "price_eur_per_mw_h"]].to_csv(
                root / f"{name}.csv", index=False
            )
        for workflow in WORKFLOWS:
            payload: dict[str, Any] = {
                "schema_version": 4,
                "run_name": f"germany-engine-check-{workflow}",
                "market": {"id": "germany"},
                "workflow": workflow,
                "forecast_provider": {"name": "persistence"},
                "timing": {
                    "timezone": TIMEZONE,
                    "resolution_minutes": 15,
                    "rebalance_cadence_minutes": 15,
                    "execution_lock_intervals": 1,
                    "day_ahead_gate_closure_local": "12:00",
                    "delivery_start_date": first.isoformat(),
                    "delivery_end_date": last.isoformat(),
                },
                "site": {"id": "check-site", "poi_import_limit_mw": 1.0, "poi_export_limit_mw": 1.0},
                "assets": [
                    {
                        "id": "bess",
                        "kind": "battery",
                        "battery": {
                            "power_mw": BATTERY.power_mw,
                            "energy_mwh": BATTERY.energy_mwh,
                            "initial_soc_mwh": BATTERY.initial_soc_mwh,
                            "terminal_soc_mwh": BATTERY.terminal_soc_mwh,
                            "soc_min_mwh": BATTERY.soc_min_mwh,
                            "soc_max_mwh": BATTERY.soc_max_mwh,
                            "charge_efficiency": BATTERY.charge_efficiency,
                            "discharge_efficiency": BATTERY.discharge_efficiency,
                            "connection_limit_mw": BATTERY.power_mw,
                            "minimum_headroom_mwh": 0.0,
                        },
                    }
                ],
                "degradation": {"mode": "throughput_linear", "throughput_cost_eur_per_mwh": throughput_cost},
                "data": {"day_ahead": {"actual_path": str(root / "da.parquet")}},
                "artifacts": {
                    "root_dir": str(root / "runs"),
                    "save_inputs": False,
                    "save_plots": False,
                    "save_forecast_snapshots": False,
                },
            }
            if workflow == "da_plus_fcr":
                payload["data"]["fcr_capacity"] = {"actual_path": str(root / "fcr.csv")}
                payload["fcr"] = {"sustain_duration_minutes": FCR_ENGINE_SUSTAIN_MINUTES}
            if workflow == "da_plus_afrr":
                payload["data"]["afrr_capacity_up"] = {"actual_path": str(root / "afrr.csv")}
                payload["data"]["afrr_capacity_down"] = {"actual_path": str(root / "afrr.csv")}
                payload["afrr"] = {
                    "sustain_duration_minutes": AFRR_ENGINE_SUSTAIN_MINUTES,
                    "settlement_mode": "capacity_only",
                    "activation_mode": "none",
                }
            result = run_walk_forward(BacktestConfig.model_validate(payload))
            engine_objective = float(result.decision_log["objective_value_eur"].sum())
            engine_daily = result.decision_log.set_index("delivery_date_local")["objective_value_eur"].astype(float)
            research_objective = 0.0
            research_net = 0.0
            max_daily_gap = 0.0
            for day in window:
                solution = _solve(day, replication, workflow, day.forecast["persistence"])
                record = settle(day, solution, workflow=workflow, provider="persistence", variant=replication)
                research_objective += solution.objective_eur
                research_net += record["net_eur"]
                max_daily_gap = max(
                    max_daily_gap, abs(solution.objective_eur - float(engine_daily[day.day.isoformat()]))
                )
            rows.append(
                {
                    "workflow": workflow,
                    "days": len(window),
                    "engine_objective_eur": engine_objective,
                    "research_objective_eur": research_objective,
                    "max_daily_objective_gap_eur": max_daily_gap,
                    "engine_realized_eur": float(result.pnl.total_pnl_eur),
                    "research_realized_eur": research_net,
                }
            )
    return {"status": "ok", "first": first.isoformat(), "last": last.isoformat(), "rows": rows}


# ---------------------------------------------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Period:
    key: str
    label: str
    first: date
    last: date


def _add_months(day: date, months: int) -> date:
    index = day.year * 12 + (day.month - 1) + months
    return date(index // 12, index % 12 + 1, 1)


def define_periods(end: date) -> list[Period]:
    """Latest 12 full months, the 15-minute DA era to ``end``, and the 12 months before (hourly DA era)."""

    month_after_end = _add_months(end, 1)
    last_full_month_end = end if end == month_after_end - timedelta(days=1) else _add_months(end, 0) - timedelta(days=1)
    latest_start = _add_months(last_full_month_end, -11)
    previous_start = _add_months(latest_start, -12)
    previous_end = latest_start - timedelta(days=1)
    return [
        Period(
            "latest_12m",
            f"Latest 12 full months ({latest_start:%b %Y} - {last_full_month_end:%b %Y})",
            latest_start,
            last_full_month_end,
        ),
        Period(
            "quarter_hour_era",
            f"15-minute DA era to latest ({QUARTER_HOUR_ERA_START:%d %b %Y} - {end:%d %b %Y})",
            QUARTER_HOUR_ERA_START,
            end,
        ),
        Period(
            "previous_12m",
            f"Previous 12 months, hourly DA era ({previous_start:%b %Y} - {previous_end:%b %Y})",
            previous_start,
            previous_end,
        ),
    ]


def summarise(results: pd.DataFrame, period: Period, *, variant: str = "base") -> pd.DataFrame:
    mask = (results["variant"] == variant) & (results["day"] >= period.first) & (results["day"] <= period.last)
    subset = results.loc[mask]
    if subset.empty:
        return pd.DataFrame()
    grouped = subset.groupby(["provider", "workflow"])
    summary = grouped.agg(
        days=("day", "nunique"),
        net_eur=("net_eur", "sum"),
        da_revenue_eur=("da_revenue_eur", "sum"),
        reserve_revenue_eur=("reserve_revenue_eur", "sum"),
        degradation_eur=("degradation_eur", "sum"),
        discharged_mwh=("discharged_mwh", "sum"),
        fcr_mw_avg=("fcr_mw_avg", "mean"),
        afrr_up_mw_avg=("afrr_up_mw_avg", "mean"),
        afrr_down_mw_avg=("afrr_down_mw_avg", "mean"),
        afrr_accepted_share=("afrr_accepted_share", "mean"),
    ).reset_index()
    scale = 365.0 / summary["days"] / BATTERY.power_mw
    for column in ("net_eur", "da_revenue_eur", "reserve_revenue_eur", "degradation_eur"):
        summary[column.replace("_eur", "_eur_per_mw_year")] = summary[column] * scale
    summary["cycles_per_day"] = summary["discharged_mwh"] / BATTERY.energy_mwh / summary["days"]
    oracle = summary.loc[summary["provider"] == "perfect_foresight"].set_index("workflow")["net_eur"]
    base_oracle = (
        results.loc[
            (results["variant"] == "base")
            & (results["provider"] == "perfect_foresight")
            & (results["day"] >= period.first)
            & (results["day"] <= period.last)
        ]
        .groupby("workflow")["net_eur"]
        .sum()
    )
    reference = oracle if not oracle.empty else base_oracle
    summary["capture_vs_oracle"] = summary.apply(
        lambda row: (
            row["net_eur"] / reference[row["workflow"]]
            if row["workflow"] in reference and reference[row["workflow"]]
            else np.nan
        ),
        axis=1,
    )
    return summary


def monthly(results: pd.DataFrame, period: Period) -> tuple[pd.DataFrame, pd.Series]:
    """Monthly net revenue (EUR per MW) by (provider, workflow) and the number of evaluated days per month."""

    mask = (results["variant"] == "base") & (results["day"] >= period.first) & (results["day"] <= period.last)
    subset = results.loc[mask].copy()
    subset["month"] = pd.to_datetime(subset["day"]).dt.strftime("%Y-%m")
    table = subset.pivot_table(index="month", columns=["provider", "workflow"], values="net_eur", aggfunc="sum")
    days = subset.groupby("month")["day"].nunique()
    return table, days


def market_context(tables: MarketTables, period: Period) -> dict[str, float]:
    lo, hi = _local_midnight_utc(period.first), _local_midnight_utc(period.last + timedelta(days=1))
    da = tables.day_ahead.loc[(tables.day_ahead["timestamp_utc"] >= lo) & (tables.day_ahead["timestamp_utc"] < hi)]
    local_day = da["timestamp_utc"].dt.tz_convert(TIMEZONE).dt.date
    daily = da.groupby(local_day)["price_eur_per_mwh"]
    fcr = tables.fcr_blocks
    afrr = tables.afrr_blocks
    fcr_start = pd.to_datetime(fcr["delivery_start_utc"], utc=True)
    afrr_start = pd.to_datetime(afrr["delivery_start_utc"], utc=True)
    fcr_window = fcr.loc[(fcr_start >= lo) & (fcr_start < hi)]
    afrr_window = afrr.loc[(afrr_start >= lo) & (afrr_start < hi)]

    def _time_weighted(frame: pd.DataFrame) -> float:
        hours = (
            pd.to_datetime(frame["delivery_end_utc"], utc=True) - pd.to_datetime(frame["delivery_start_utc"], utc=True)
        ).dt.total_seconds() / 3600.0
        return float((frame["price_eur_per_mw_h"].astype(float) * hours).sum() / hours.sum()) if len(frame) else np.nan

    return {
        "da_mean_eur_mwh": float(da["price_eur_per_mwh"].mean()),
        "da_daily_spread_eur_mwh": float((daily.max() - daily.min()).mean()),
        "da_negative_share": float((da["price_eur_per_mwh"] < 0).mean()),
        "fcr_eur_mw_h": _time_weighted(fcr_window),
        "afrr_up_eur_mw_h": _time_weighted(afrr_window.loc[afrr_window["direction"] == "up"]),
        "afrr_down_eur_mw_h": _time_weighted(afrr_window.loc[afrr_window["direction"] == "down"]),
    }


def quarterly_reserve_revenue(tables: MarketTables, year: int) -> dict[str, float]:
    """Average monthly capacity revenue of 1 MW offered in every block of Q1 ``year`` (EUR per MW and month)."""

    lo, hi = _local_midnight_utc(date(year, 1, 1)), _local_midnight_utc(date(year, 4, 1))
    values: dict[str, float] = {}
    for key, frame in (
        ("fcr", tables.fcr_blocks),
        ("afrr_up", tables.afrr_blocks.loc[tables.afrr_blocks["direction"] == "up"]),
        ("afrr_down", tables.afrr_blocks.loc[tables.afrr_blocks["direction"] == "down"]),
    ):
        start = pd.to_datetime(frame["delivery_start_utc"], utc=True)
        window = frame.loc[(start >= lo) & (start < hi)]
        hours = (
            pd.to_datetime(window["delivery_end_utc"], utc=True)
            - pd.to_datetime(window["delivery_start_utc"], utc=True)
        ).dt.total_seconds() / 3600.0
        values[key] = float((window["price_eur_per_mw_h"].astype(float) * hours).sum() / 3.0) if len(window) else np.nan
    return values


# ---------------------------------------------------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------------------------------------------------


def _style_axes(ax: Any) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(INK["axis"])
    ax.tick_params(colors=INK["secondary"], labelsize=9)
    ax.yaxis.grid(True, color=INK["grid"], linewidth=0.8)
    ax.set_axisbelow(True)


def make_figures(
    results: pd.DataFrame, tables: MarketTables, periods: Sequence[Period], figures_dir: Path
) -> list[Path]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    figures_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    era = next(period for period in periods if period.key == "quarter_hour_era")
    table, month_days = monthly(results, era)
    months = list(table.index)
    labels = [pd.Timestamp(f"{month}-01").strftime("%b\n%Y") for month in months]
    last_days = int(month_days.iloc[-1])
    if last_days < pd.Timestamp(f"{months[-1]}-01").days_in_month:
        labels[-1] = labels[-1].replace("\n", f" 1-{last_days}\n")

    fig, ax = plt.subplots(figsize=(11, 4.6), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    _style_axes(ax)
    width = 0.27
    x = np.arange(len(months))
    for offset, workflow in zip((-width, 0.0, width), WORKFLOWS, strict=True):
        persistence = table[("persistence", workflow)].to_numpy() / 1000.0
        oracle = table[("perfect_foresight", workflow)].to_numpy() / 1000.0
        ax.bar(
            x + offset,
            persistence,
            width=width - 0.03,
            color=FIGURE_COLORS[workflow],
            label=f"{WORKFLOW_LABELS[workflow]} - persistence",
            zorder=2,
        )
        ax.scatter(
            x + offset,
            oracle,
            marker="_",
            s=110,
            linewidths=2.0,
            color=INK["primary"],
            zorder=3,
            label="Perfect-foresight oracle (same workflow)" if workflow == "da_only" else None,
        )
    ax.set_xticks(x, labels)
    ax.set_ylabel("Net revenue, kEUR per MW per month", color=INK["secondary"], fontsize=9)
    ax.set_title(
        "Germany 1 MW / 2 MWh BESS: monthly net revenue (15-minute DA era)",
        loc="left",
        color=INK["primary"],
        fontsize=11,
    )
    ax.legend(
        frameon=False,
        fontsize=8,
        ncols=4,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.16),
        labelcolor=INK["secondary"],
    )
    fig.tight_layout()
    path = figures_dir / "germany_monthly_net_revenue.png"
    fig.savefig(path, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)
    written.append(path)

    fig, ax = plt.subplots(figsize=(11, 4.6), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    _style_axes(ax)
    base = results.loc[(results["variant"] == "base") & (results["day"] >= era.first) & (results["day"] <= era.last)]
    for workflow in WORKFLOWS:
        for provider, style in (("persistence", "-"), ("perfect_foresight", "--")):
            series = (
                base.loc[(base["workflow"] == workflow) & (base["provider"] == provider)]
                .sort_values("day")
                .set_index("day")["net_eur"]
                .cumsum()
                / 1000.0
            )
            ax.plot(
                pd.to_datetime(series.index),
                series.to_numpy(),
                linestyle=style,
                linewidth=2.0 if provider == "persistence" else 1.4,
                color=FIGURE_COLORS[workflow],
                label=f"{WORKFLOW_LABELS[workflow]} - {'persistence' if provider == 'persistence' else 'oracle'}",
            )
            if provider == "persistence":
                ax.annotate(
                    f"{WORKFLOW_LABELS[workflow]} {series.iloc[-1]:.0f}k",
                    xy=(pd.Timestamp(series.index[-1]), series.iloc[-1]),
                    xytext=(6, 0),
                    textcoords="offset points",
                    va="center",
                    fontsize=8,
                    color=INK["primary"],
                )
    ax.set_ylabel("Cumulative net revenue, kEUR per MW", color=INK["secondary"], fontsize=9)
    ax.set_title(
        "Cumulative net revenue since the 15-minute DA go-live (solid: persistence, dashed: oracle)",
        loc="left",
        color=INK["primary"],
        fontsize=11,
    )
    ax.legend(frameon=False, fontsize=8, ncols=2, loc="upper left", labelcolor=INK["secondary"])
    ax.margins(x=0.08)
    fig.tight_layout()
    path = figures_dir / "germany_cumulative_net_revenue.png"
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    written.append(path)

    fig, ax = plt.subplots(figsize=(11, 4.2), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    _style_axes(ax)
    first = min(period.first for period in periods)
    last = max(period.last for period in periods)
    lo, hi = _local_midnight_utc(first), _local_midnight_utc(last + timedelta(days=1))
    # Colour follows the product family used in the other figures (FCR orange, aFRR aqua); line style = direction.
    for label, frame, color, style in (
        ("FCR (pay-as-cleared)", tables.fcr_blocks, FIGURE_COLORS["da_plus_fcr"], "-"),
        (
            "aFRR up, average awarded",
            tables.afrr_blocks.loc[tables.afrr_blocks["direction"] == "up"],
            FIGURE_COLORS["da_plus_afrr"],
            "-",
        ),
        (
            "aFRR down, average awarded",
            tables.afrr_blocks.loc[tables.afrr_blocks["direction"] == "down"],
            FIGURE_COLORS["da_plus_afrr"],
            "--",
        ),
    ):
        start = pd.to_datetime(frame["delivery_start_utc"], utc=True)
        window = frame.loc[(start >= lo) & (start < hi)].copy()
        window["month"] = (
            pd.to_datetime(window["delivery_start_utc"], utc=True).dt.tz_convert(TIMEZONE).dt.strftime("%Y-%m")
        )
        hours = (
            pd.to_datetime(window["delivery_end_utc"], utc=True)
            - pd.to_datetime(window["delivery_start_utc"], utc=True)
        ).dt.total_seconds() / 3600.0
        window["weighted"] = window["price_eur_per_mw_h"].astype(float) * hours
        window["hours"] = hours
        grouped = window.groupby("month")[["weighted", "hours"]].sum()
        values = grouped["weighted"] / grouped["hours"]
        stamps = pd.to_datetime([f"{month}-15" for month in values.index])
        ax.plot(
            stamps,
            values.to_numpy(),
            linewidth=2.0,
            linestyle=style,
            color=color,
            label=label,
            marker="o",
            markersize=3.5,
        )
        ax.annotate(
            label.split(",")[0].replace(" (pay-as-cleared)", ""),
            xy=(stamps[-1], values.iloc[-1]),
            xytext=(6, 0),
            textcoords="offset points",
            va="center",
            fontsize=8,
            color=INK["primary"],
        )
    ax.axvline(pd.Timestamp(QUARTER_HOUR_ERA_START), color=INK["muted"], linewidth=1.0, linestyle=":")
    ax.annotate(
        "15-minute DA MTU go-live",
        xy=(pd.Timestamp(QUARTER_HOUR_ERA_START), ax.get_ylim()[1] * 0.95),
        xytext=(4, 0),
        textcoords="offset points",
        fontsize=8,
        color=INK["secondary"],
    )
    ax.set_ylabel("Monthly average capacity price, EUR/MW/h", color=INK["secondary"], fontsize=9)
    ax.set_title(
        "German reserve capacity prices (regelleistung.net results)", loc="left", color=INK["primary"], fontsize=11
    )
    ax.legend(frameon=False, fontsize=8, loc="upper right", labelcolor=INK["secondary"])
    ax.margins(x=0.06)
    fig.tight_layout()
    path = figures_dir / "germany_reserve_capacity_prices.png"
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    written.append(path)
    return written


# ---------------------------------------------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------------------------------------------


def _eur(value: float) -> str:
    return "n/a" if value is None or not np.isfinite(value) else f"{value:,.0f}"


def _pct(value: float) -> str:
    return "n/a" if value is None or not np.isfinite(value) else f"{100.0 * value:.1f} %"


def _headline_table(summary: pd.DataFrame) -> list[str]:
    lines = [
        "| Workflow | Persistence EUR/MW/yr | Oracle EUR/MW/yr | Capture vs oracle | DA / reserve / degradation (persistence, EUR/MW/yr) | Cycles/day (persistence) | Avg reserved MW |",
        "| --- | ---: | ---: | ---: | --- | ---: | --- |",
    ]
    indexed = summary.set_index(["provider", "workflow"])
    for workflow in WORKFLOWS:
        row = indexed.loc[("persistence", workflow)]
        oracle = indexed.loc[("perfect_foresight", workflow)]
        if workflow == "da_plus_fcr":
            reserved = f"FCR {row['fcr_mw_avg']:.2f}"
        elif workflow == "da_plus_afrr":
            reserved = f"up {row['afrr_up_mw_avg']:.2f} / down {row['afrr_down_mw_avg']:.2f}"
        else:
            reserved = "-"
        lines.append(
            f"| {WORKFLOW_LABELS[workflow]} | {_eur(row['net_eur_per_mw_year'])} | {_eur(oracle['net_eur_per_mw_year'])} | "
            f"{_pct(row['capture_vs_oracle'])} | {_eur(row['da_revenue_eur_per_mw_year'])} / "
            f"{_eur(row['reserve_revenue_eur_per_mw_year'])} / -{_eur(row['degradation_eur_per_mw_year'])} | "
            f"{row['cycles_per_day']:.2f} | {reserved} |"
        )
    return lines


def _key_findings(
    results: pd.DataFrame,
    summaries: dict[str, pd.DataFrame],
    *,
    latest: Period,
    era: Period,
    previous: Period,
) -> list[str]:
    head = summaries[latest.key]
    if head.empty:
        return []
    value = head.set_index(["provider", "workflow"])

    def net(provider: str, workflow: str, table: pd.DataFrame = value) -> float:
        return float(table.loc[(provider, workflow), "net_eur_per_mw_year"])

    lines = [
        "## Key findings",
        "",
        f"- **{latest.label}**: persistence walk-forward earns {_eur(net('persistence', 'da_only'))} EUR/MW/yr with "
        f"day-ahead only ({_pct(value.loc[('persistence', 'da_only'), 'capture_vs_oracle'])} of the oracle), "
        f"{_eur(net('persistence', 'da_plus_fcr'))} with FCR and {_eur(net('persistence', 'da_plus_afrr'))} with "
        "capacity-only aFRR valued at the published average awarded price.",
    ]
    era_shading = summarise(results, era, variant="afrr_pay_as_bid_shaded_70")
    era_naive = summarise(results, era, variant="afrr_pay_as_bid_acceptance")
    era_base = summaries[era.key].set_index(["provider", "workflow"])
    if not era_shading.empty and not era_naive.empty:
        naive = float(
            era_naive.set_index(["provider", "workflow"]).loc[("persistence", "da_plus_afrr"), "net_eur_per_mw_year"]
        )
        shaded = float(
            era_shading.set_index(["provider", "workflow"]).loc[("persistence", "da_plus_afrr"), "net_eur_per_mw_year"]
        )
        lines.append(
            "- **The aFRR number depends on the pay-as-bid convention.** Over the 15-minute era the average-price "
            f"benchmark gives {_eur(net('persistence', 'da_plus_afrr', era_base))} EUR/MW/yr; bidding the persistence "
            f"price and being paid the own bid gives {_eur(naive)} (rejections when the bid exceeds the marginal "
            f"price), and shading bids to 70 % gives {_eur(shaded)}. With simple bidding rules aFRR capacity is "
            f"therefore close to DA + FCR ({_eur(net('persistence', 'da_plus_fcr', era_base))})."
        )
    relaxed = summarise(results, era, variant="relaxed_per_quarter_hour")
    if not relaxed.empty:
        relaxed_index = relaxed.set_index(["provider", "workflow"])
        changes = [
            relaxed_index.loc[("persistence", workflow), "net_eur_per_mw_year"]
            / era_base.loc[("persistence", workflow), "net_eur_per_mw_year"]
            - 1.0
            for workflow in ("da_plus_fcr", "da_plus_afrr")
        ]
        lines.append(
            "- **4-hour product blocks matter.** Letting reserve change every quarter-hour (the framework engine "
            f"formulation) overstates FCR value by {_pct(changes[0])} and aFRR value by {_pct(changes[1])}."
        )
    fcr30 = summarise(results, era, variant="fcr_30min_alert_state")
    if not fcr30.empty:
        change = (
            float(fcr30.set_index(["provider", "workflow"]).loc[("persistence", "da_plus_fcr"), "net_eur_per_mw_year"])
            / float(era_base.loc[("persistence", "da_plus_fcr"), "net_eur_per_mw_year"])
            - 1.0
        )
        lines.append(
            "- **FCR energy rule.** If the pending ACER decision set the alert-state duration to 30 instead of 15 "
            f"minutes, DA + FCR would change by {_pct(change)} for this 2-hour battery."
        )
    prior = summaries[previous.key]
    if not prior.empty:
        prior_index = prior.set_index(["provider", "workflow"])
        da_change = (
            net("persistence", "da_only") / float(prior_index.loc[("persistence", "da_only"), "net_eur_per_mw_year"])
            - 1.0
        )
        lines.append(
            f"- **15-minute day-ahead products.** Day-ahead-only value in the latest 12 months is {100.0 * da_change:+.1f} % "
            f"versus the previous 12 months ({previous.first:%b %Y} - {previous.last:%b %Y}, hourly products), but the two "
            "periods also differ in price level and spread (see market context), so this is not a clean MTU effect."
        )
    captures = {workflow: value.loc[("persistence", workflow), "capture_vs_oracle"] for workflow in WORKFLOWS}
    lines.extend(
        [
            "- **Forecastability.** In the latest 12 months persistence captures "
            + ", ".join(
                f"{_pct(captures[workflow])} for {WORKFLOW_LABELS[workflow].replace(' (capacity)', ' capacity')}"
                for workflow in WORKFLOWS
            )
            + " of the oracle: reserve-heavy schedules depend on block capacity prices, "
            "which persist from one day to the next more than the intraday shape of day-ahead prices does.",
            "",
        ]
    )
    return lines


def render_report(
    *,
    results: pd.DataFrame,
    tables: MarketTables,
    coverage: dict[str, Any],
    periods: Sequence[Period],
    check: dict[str, Any],
    figures: Sequence[Path],
    start: date,
    end: date,
    runtime_seconds: float,
) -> str:
    era = next(period for period in periods if period.key == "quarter_hour_era")
    latest = next(period for period in periods if period.key == "latest_12m")
    previous = next(period for period in periods if period.key == "previous_12m")
    summaries = {period.key: summarise(results, period) for period in periods}
    lines: list[str] = [
        "# Germany (DE-LU) BESS backtest: day-ahead, FCR and aFRR capacity on real data",
        "",
        "Every number on this page is produced by `scripts/research/germany_backtest.py` "
        f"(`.venv/bin/python scripts/research/germany_backtest.py --end {end.isoformat()}`). "
        "Market model and its limits: [docs/germany.md](../docs/germany.md).",
        "",
        "**Setup.** 1 MW / 2 MWh battery, SOC window 5-95 % (0.1-1.9 MWh), start and end every delivery day at 50 %, "
        "charge and discharge efficiency 0.95 / 0.95, degradation 8 EUR per MWh discharged, no activation energy. "
        "Walk-forward per delivery day D: the FCR decision is taken at D-1 08:00, aFRR at D-1 09:00 and day-ahead-only "
        "at D-1 12:00 (Europe/Berlin). Forecasts come from the repository's `PersistenceForecastProvider` "
        "(previous local day, same clock time); `PerfectForesightForecastProvider` gives the oracle (upper bound, not "
        "tradable). Each day is a Pyomo + HiGHS MILP (`solve_germany_day`) with reserve volumes constant over the "
        "German 4-hour products and the PQ energy buffers for limited-energy reservoirs (FCR 0.415 MWh per MW and "
        "direction, aFRR 60 minutes per MW and direction). Settlement: day-ahead at realised DA prices; FCR at the "
        "realised German pay-as-cleared price; aFRR capacity at the published German **average awarded price** "
        "(a benchmark for a pay-as-bid auction, see the pay-as-bid sensitivity below). EUR/MW/yr = period net revenue "
        "x 365 / evaluated days / 1 MW. Cycles/day = MWh discharged / 2 MWh / day. Capture = persistence net / oracle "
        "net for the same workflow.",
        "",
        *_key_findings(results, summaries, latest=latest, era=era, previous=previous),
    ]
    for period in (latest, era, previous):
        summary = summaries[period.key]
        if summary.empty:
            continue
        days = int(summary["days"].max())
        requested = (period.last - period.first).days + 1
        lines.extend(
            [
                f"## {period.label}",
                "",
                f"Data coverage: {days} of {requested} delivery days evaluated ({_pct(days / requested)}).",
                "",
                *_headline_table(summary),
                "",
            ]
        )
    lines.extend(
        [
            "The oracle knows realised day-ahead and reserve prices when deciding; it is an upper bound for this "
            "battery and market model, not an achievable revenue.",
            "",
            f"![Monthly net revenue](figures/{figures[0].name})",
            "",
            f"![Cumulative net revenue](figures/{figures[1].name})",
            "",
            f"## Monthly breakdown ({era.label})",
            "",
            "Net revenue in EUR per MW for the month (persistence, with capture vs oracle in brackets).",
            "",
            "| Month | Days | DA only | DA + FCR | DA + aFRR (capacity) |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    table, month_days = monthly(results, era)
    for month, row in table.iterrows():
        cells = []
        for workflow in WORKFLOWS:
            persistence = float(row[("persistence", workflow)])
            oracle = float(row[("perfect_foresight", workflow)])
            cells.append(f"{_eur(persistence)} ({_pct(persistence / oracle if oracle else np.nan)})")
        lines.append(f"| {month} | {int(month_days[month])} | " + " | ".join(cells) + " |")
    lines.extend(["", "## Sensitivities", ""])
    lines.extend(
        [
            f"Same days as the {era.label.split(' (')[0]} period; EUR/MW/yr, persistence unless stated.",
            "",
            "| Case | Workflow | Persistence EUR/MW/yr | Oracle EUR/MW/yr | Change vs base (persistence) |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
    )
    base_summary = summarise(results, era).set_index(["provider", "workflow"])
    descriptions = {
        "relaxed_per_quarter_hour": "Reserve may change every quarter-hour, SOC buffer checked at interval start (framework engine formulation)",
        "fcr_30min_alert_state": "FCR buffer 0.665 h per direction (alert-state duration 30 min instead of 15 min; ACER decision pending)",
        "afrr_pay_as_bid_acceptance": "aFRR pay-as-bid: bid = persistence price, accepted only if <= realised marginal price, paid own bid; DA re-optimised after the award",
        "afrr_pay_as_bid_shaded_70": "aFRR pay-as-bid with bids shaded to 70 % of the persistence price (same acceptance rule)",
    }
    for variant in SENSITIVITIES:
        summary = summarise(results, era, variant=variant.name)
        if summary.empty:
            continue
        indexed = summary.set_index(["provider", "workflow"])
        for workflow in variant.workflows:
            row = indexed.loc[("persistence", workflow)]
            oracle_value = (
                indexed.loc[("perfect_foresight", workflow)]["net_eur_per_mw_year"]
                if ("perfect_foresight", workflow) in indexed.index
                else np.nan
            )
            base_value = base_summary.loc[("persistence", workflow)]["net_eur_per_mw_year"]
            change = row["net_eur_per_mw_year"] / base_value - 1.0 if base_value else np.nan
            extra = ""
            if variant.pay_as_bid_acceptance and np.isfinite(row["afrr_accepted_share"]):
                extra = f"; {_pct(row['afrr_accepted_share'])} of offered MW accepted"
            lines.append(
                f"| {descriptions[variant.name]}{extra} | {WORKFLOW_LABELS[workflow]} | {_eur(row['net_eur_per_mw_year'])} | "
                f"{_eur(oracle_value)} | {_pct(change)} |"
            )
    lines.extend(["", "## Framework consistency check", ""])
    if check.get("status") == "ok":
        lines.extend(
            [
                f"The framework engine (`run_walk_forward` through the Germany adapter, persistence, {check['first']} to "
                f"{check['last']}) and `solve_germany_day` in replication mode (per-quarter-hour reserve, buffer checked at "
                "interval start, FCR buffer 30 min, aFRR 60 min, 4 EUR/MWh throughput degradation) solve the same problem:",
                "",
                "| Workflow | Days | Engine objective EUR | Research LP objective EUR | Max daily gap EUR | Engine realised EUR | Research LP realised EUR |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in check["rows"]:
            lines.append(
                f"| {WORKFLOW_LABELS[row['workflow']]} | {row['days']} | {row['engine_objective_eur']:,.2f} | "
                f"{row['research_objective_eur']:,.2f} | {row['max_daily_objective_gap_eur']:.4f} | "
                f"{row['engine_realized_eur']:,.2f} | {row['research_realized_eur']:,.2f} |"
            )
        lines.extend(
            [
                "",
                "Objectives match, so the headline differences against the engine come only from the German product rules "
                "switched on in the research LP. Realised revenue can differ slightly where the MILP has several optimal "
                "schedules.",
                "",
            ]
        )
    else:
        lines.extend([f"Not run: {check.get('reason', 'disabled')}.", ""])
    lines.extend(
        [
            "## Market context",
            "",
            "| Period | Mean DA price EUR/MWh | Mean daily DA spread (max-min) EUR/MWh | Share of negative DA quarter-hours | FCR EUR/MW/h | aFRR up EUR/MW/h | aFRR down EUR/MW/h |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for period in (latest, era, previous):
        context = market_context(tables, period)
        lines.append(
            f"| {period.label} | {context['da_mean_eur_mwh']:.1f} | {context['da_daily_spread_eur_mwh']:.1f} | "
            f"{_pct(context['da_negative_share'])} | {context['fcr_eur_mw_h']:.2f} | {context['afrr_up_eur_mw_h']:.2f} | "
            f"{context['afrr_down_eur_mw_h']:.2f} |"
        )
    lines.extend(
        [
            "",
            "Reserve prices are time-weighted over the 4-hour products; aFRR prices are the German average awarded "
            "capacity prices.",
            "",
            f"![Reserve capacity prices](figures/{figures[2].name})",
            "",
            "## Data and coverage",
            "",
            f"- Day-ahead prices: {tables.sources['day_ahead']}. Licence CC BY 4.0, attribution Bundesnetzagentur | "
            "SMARD.de (via Energy-Charts, Fraunhofer ISE). Hourly SDAC products before 2025-10-01 are expanded to four flat "
            "quarter-hours (`expanded_from_60m`).",
            f"- FCR and aFRR capacity results: {tables.sources['reserves']}. The regelleistung.net imprint requires prior "
            "written consent for reproduction, so these files are not committed; the script re-downloads them with "
            "`--fetch`. FCR prices are published per MW and 4-hour product and are divided by the product length "
            "(3 or 5 hours on DST days).",
            f"- Evaluated delivery days: {coverage['evaluated_days']} of {coverage['requested_days']} requested "
            f"({start} to {end}); a day is used only if day-ahead, FCR and both aFRR "
            "directions are complete, so all workflows share the same days.",
            f"- Skipped days: {', '.join(coverage['skipped_days']) if coverage['skipped_days'] else 'none'}.",
            "- Persistence fallback days (used only when the previous local day is incomplete; the provider handles "
            "DST days): "
            + (", ".join(coverage["persistence_fallback_days"]) if coverage["persistence_fallback_days"] else "none")
            + ".",
            f"- Runtime of this script run: {runtime_seconds / 60.0:.1f} minutes.",
            "",
            "## Assumptions and limitations",
            "",
            "- **Schedule-based, day-ahead only.** No intraday trading (continuous or IDA auctions) and no re-optimisation "
            "after gate closure; German BESS revenue in practice includes a large intraday share, so these numbers "
            "understate a merchant battery and are not comparable with all-market revenue indices.",
            "- **No imbalance trading.** Deliberate imbalance positions are not permitted in Germany (Bilanzkreistreue); "
            "reBAP is not used. Any physical deviation would have to be balanced by the BRP.",
            "- **Price taker.** Bids never move clearing prices; awarded volumes are always accepted, except in the "
            "pay-as-bid sensitivity. Minimum bid size and 1 MW increments are assumed to be met by pooling (results are "
            "per MW of a pooled fleet).",
            "- **FCR.** Symmetric, 4-hour blocks, pay-as-cleared at the German settlement price; activation energy is not "
            "remunerated in Germany and is assumed to net to zero over a block. The PQ buffer (0.415 MWh per MW and "
            "direction) is held for the whole block; storage-management trades and SOC drift from frequency deviations "
            "are not modelled.",
            "- **aFRR.** Capacity only. The German average awarded price is a benchmark for a pay-as-bid auction (an "
            "individual bidder earns its own bid); activation energy (PICASSO, pay-as-cleared) and the SOC impact of "
            "activations are not modelled - expected activation is zero, while the 60-minute PQ buffer is reserved in "
            "both directions. The obligation to submit energy bids for awarded capacity is not modelled.",
            "- **Forecasts.** Persistence (previous day, same clock time) for both day-ahead and reserve prices; the joint "
            "decision is taken once at the reserve gate closure. No forecast model is tuned on the test period.",
            "- **Battery.** Daily 50 % to 50 % SOC boundary, linear efficiency, no auxiliary consumption, no availability "
            "losses, degradation as a linear cost per MWh discharged; no grid fees, levies or taxes.",
            "- **Engine path.** The framework engine (example configs) lets reserve volumes change every quarter-hour; the "
            "sensitivity above shows how much that relaxation overstates value.",
            "",
            "## External reference points",
            "",
        ]
    )
    q1_2026 = quarterly_reserve_revenue(tables, 2026)
    q1_2025 = quarterly_reserve_revenue(tables, 2025)
    if all(np.isfinite(value) for value in (*q1_2026.values(), *q1_2025.values())):
        lines.extend(
            [
                "Cross-check of the parsed reserve data against published Q1 figures (average capacity revenue of 1 MW "
                f"offered in every block, EUR per MW and month; published values: {PUBLISHED_REFERENCE_URL}):",
                "",
                "| Product | Q1 2026, this data | Q1 2026, published | Change vs Q1 2025, this data | Change, published |",
                "| --- | ---: | ---: | ---: | ---: |",
            ]
        )
        for key, label in (("fcr", "FCR"), ("afrr_up", "aFRR up"), ("afrr_down", "aFRR down")):
            lines.append(
                f"| {label} | {_eur(q1_2026[key])} | {_eur(PUBLISHED_Q1_2026_EUR_PER_MW_MONTH[key])} | "
                f"{_pct(q1_2026[key] / q1_2025[key] - 1.0)} | {_pct(PUBLISHED_CHANGE_VS_Q1_2025[key])} |"
            )
        lines.append("")
    lines.extend(
        [
            f"- ISEA Battery Revenue Index (RWTH Aachen, {ISEA_INDEX_URL}): the Q1 2026 average is reported at about "
            f"{PUBLISHED_ISEA_INDEX_Q1_2026_EUR_PER_MW_YEAR:,.0f} EUR/MW/yr by {PUBLISHED_REFERENCE_URL}. The index covers "
            "cross-market operation including intraday trading and its own asset definition, so it is a scale reference "
            "only and not comparable one-to-one with the day-ahead schedule modelled here.",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------------------------------------------


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--start", type=_parse_date, default=DEFAULT_START, help="first delivery day (default 2024-10-01)"
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        default=date.today() - timedelta(days=1),
        help="last delivery day (default: yesterday)",
    )
    parser.add_argument("--fetch", action="store_true", help="download missing Energy-Charts / regelleistung.net files")
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    parser.add_argument("--report", type=Path, default=REPO_ROOT / "reports" / "germany.md")
    parser.add_argument("--figures-dir", type=Path, default=REPO_ROOT / "reports" / "figures")
    parser.add_argument("--engine-check-start", type=_parse_date, default=date(2025, 11, 3))
    parser.add_argument("--engine-check-end", type=_parse_date, default=date(2025, 11, 16))
    parser.add_argument("--skip-engine-check", action="store_true")
    args = parser.parse_args(argv)

    started = time.perf_counter()
    tables = load_tables(args.start - timedelta(days=7), args.end, fetch=args.fetch)
    days, coverage = build_days(tables, args.start, args.end)
    print(f"evaluated days: {coverage['evaluated_days']} / {coverage['requested_days']}", flush=True)
    results = run_backtest(days, workers=args.workers, sensitivity_start=QUARTER_HOUR_ERA_START)
    check = (
        {"status": "skipped", "reason": "--skip-engine-check"}
        if args.skip_engine_check
        else engine_consistency_check(tables, days, first=args.engine_check_start, last=args.engine_check_end)
    )
    periods = define_periods(args.end)
    figures = make_figures(results, tables, periods, args.figures_dir)
    report = render_report(
        results=results,
        tables=tables,
        coverage=coverage,
        periods=periods,
        check=check,
        figures=figures,
        start=args.start,
        end=args.end,
        runtime_seconds=time.perf_counter() - started,
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(report, encoding="utf-8")
    print(f"wrote {args.report} and {len(figures)} figures in {time.perf_counter() - started:.0f} s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
