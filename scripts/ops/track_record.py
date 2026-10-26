#!/usr/bin/env python
"""Forward track record: the frozen live strategies paper-traded on every Elia day after the committed data set.

Every complete Belgian delivery day after the end of the committed ``data/public`` set is simulated minute by minute
with the engine and publication model of the backtests (:func:`euroflex_bess_lab.realtime.simulate`: minute rows
usable one minute after they end, final quarter-hour prices five minutes after the quarter-hour, day-ahead prices
from 13:00 on D-1) for a 1 MW / 2 MWh battery and appended to ``reports/track_record/``:

- ``daily_pnl.csv``: one row per delivery day and strategy (P&L, energy, cycles, SOC, data-quality flags);
- ``qh/<YYYY-MM>.csv``: the quarter-hour ledger (one file per month of delivery days);
- ``summary.json``, ``README.md`` and ``cumulative_pnl.png``: generated from the two CSV files.

Strategies are frozen: ``threshold`` (price bands; same parameters as the backtest's ``threshold_price``) always;
``forecast_mpc`` and ``scales_l`` (risk filter) with the deployment models fitted on data up to 2026-10-24 when they
are present locally (``--models-dir`` or ``$EUROFLEX_MODELS_DIR``; build them with ``scripts/ops/build_models.py``).
The battery state of charge carries over from one day to the next; each day sees a 28-day history window in which
the strategy does not trade. Settlement follows the formula in force: current formula (ODS134 price) before the
new-formula go-live, new formula (ODS212 price) from it. Paper money: nothing is submitted to any market.

Data: the committed ``data/public`` set for the days it covers; newer days through the public-data fetch code
(read-only GETs to Elia Open Data and Energy-Charts) into the gitignored raw cache ``data/raw/track_record``.
Appending is idempotent: a recorded day is never recomputed or rewritten (``verify`` re-simulates and compares).

Usage::

    python scripts/ops/track_record.py update            # fetch, simulate and append every new complete day
    python scripts/ops/track_record.py update --offline  # cached data only, no network
    python scripts/ops/track_record.py render            # regenerate summary.json, README.md and the chart
    python scripts/ops/track_record.py verify            # re-simulate recorded days from cached data and compare
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Protocol

import numpy as np
import pandas as pd

from euroflex_bess_lab import public_data
from euroflex_bess_lab.public_data import energy_charts
from euroflex_bess_lab.public_data.build import BuildContext
from euroflex_bess_lab.public_data.elia import RawChunk, fetch_dataset, plan_chunks, read_chunks
from euroflex_bess_lab.public_data.http import HttpClient, HttpConfig
from euroflex_bess_lab.public_data.normalize import (
    normalize_minute,
    normalize_new_formula,
    normalize_qh,
    normalize_si_forecast,
)
from euroflex_bess_lab.public_data.registry import ELIA_DATASETS
from euroflex_bess_lab.realtime import BatteryParams, DecisionContext, MarketData, PublicationModel, simulate
from euroflex_bess_lab.realtime.live import NEW_FORMULA_GO_LIVE_UTC
from euroflex_bess_lab.realtime.live.strategies import SIGNAL_BY_MECHANISM
from euroflex_bess_lab.realtime.strategies import BaseStrategy, ThresholdStrategy
from euroflex_bess_lab.realtime.strategies.base import Strategy

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_models  # noqa: E402  (sibling script: model manifest and identity)

REPO = Path(__file__).resolve().parents[2]
TZ = "Europe/Brussels"
SCHEMA = "euroflex-track-record/1"
#: First delivery day after the committed data set the deployment models were fitted on (data up to 2026-10-24).
TRACK_START = date(2026, 10, 25)
BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)
PUBLICATION = PublicationModel()
WARMUP_DAYS = 28
MAX_WAIT_DAYS = 5.0
DEFAULT_OUT = REPO / "reports" / "track_record"
DAILY_CSV = "daily_pnl.csv"
LEDGER_DIR = "qh"
SUMMARY_JSON = "summary.json"
README_MD = "README.md"
CHART_PNG = "cumulative_pnl.png"
NEW_FORMULA_DATASETS = ("ods211", "ods212")
CORE_DATASETS = ("ods133", "ods126", "ods165", "ods134", "ods166", "ods127", "ods136", "ods147")
#: Running quarter-hour price that must be published through the end of a day for it to count as complete.
MINUTE_SIGNAL = {"current": "price_cum_eur_mwh", "new2026": "nf_imbalance_price_eur_mwh"}
SIMULATION_WINDOW = (pd.Timestamp("2026-01-01", tz="UTC"), pd.Timestamp("2026-08-01", tz="UTC"))
ATTRIBUTION = (
    "Elia Transmission Belgium SA, Elia Open Data (CC BY 4.0)",
    "Bundesnetzagentur | SMARD.de via Energy-Charts (CC BY 4.0)",
)
PAPER_NOTE = "Paper money: a simulation on published data. No order or bid is submitted to any market."
DAILY_COLUMNS: dict[str, str] = {
    "local_date": "str",
    "strategy": "str",
    "mechanism": "str",
    "n_qh": "int",
    "pnl_eur": "float",
    "revenue_eur": "float",
    "degradation_eur": "float",
    "discharged_mwh": "float",
    "charged_mwh": "float",
    "cycles": "float",
    "cumulative_pnl_eur": "float",
    "soc_start_mwh": "float",
    "soc_end_mwh": "float",
    "price_missing_qh": "int",
    "qh_not_validated": "int",
    "minutes_missing": "int",
    "data_complete": "bool",
    "data_source": "str",
    "models_id": "str",
    "computed_at_utc": "str",
}
LEDGER_COLUMNS: dict[str, str] = {
    "timestamp_utc": "str",
    "strategy": "str",
    "mechanism": "str",
    "energy_mwh": "float",
    "discharged_mwh": "float",
    "charged_mwh": "float",
    "price_eur_mwh": "float",
    "price_missing": "bool",
    "revenue_eur": "float",
    "degradation_eur": "float",
    "pnl_eur": "float",
    "soc_end_mwh": "float",
}
ROUND_EUR = 4
ROUND_MWH = 6


# ----------------------------------------------------------------------------------------------------------------
# strategies


@dataclass(frozen=True)
class StrategySpec:
    name: str
    label: str
    mechanisms: tuple[str, ...]
    needs_models: bool
    backtest_name: str
    description: str


STRATEGIES: dict[str, StrategySpec] = {
    "threshold": StrategySpec(
        "threshold",
        "Threshold (price bands)",
        ("current", "new2026"),
        False,
        "threshold_price",
        "charge below the 25 % / discharge above the 75 % quantile of the last 14 days of final prices, read "
        "against the running quarter-hour price of the formula in force",
    ),
    "forecast_mpc": StrategySpec(
        "forecast_mpc",
        "Forecast MPC (risk filter)",
        ("current",),
        True,
        "forecast_mpc_risk",
        "rolling stochastic MPC on the current-formula price forecaster (30 % / 70 % quantile filter); retired "
        "at the new-formula go-live because its model prices the current formula",
    ),
    "scales_l": StrategySpec(
        "scales_l",
        "SCALES-L (risk filter)",
        ("current", "new2026"),
        True,
        "prism_risk",
        "mechanism-aware state forecasts priced by the settlement operator, same MPC core; the zero-history "
        "new-formula forecaster fused with the live running price after the go-live",
    ),
}
COLORS = {"threshold": "#eda100", "forecast_mpc": "#008300", "scales_l": "#6250d6"}  # as in the backtest report


def build_strategy(name: str, mechanism: str, models_dir: Path | None) -> Strategy:
    """The frozen live strategy ``name`` for one settlement mechanism (same factories as ``euroflex live run``)."""
    if name == "threshold":
        return ThresholdStrategy(mode="price", signal_field=SIGNAL_BY_MECHANISM[mechanism], name="threshold")
    directory = str(models_dir) if models_dir is not None else None
    if name == "forecast_mpc":
        from euroflex_bess_lab.realtime.strategies.forecast_mpc import live_forecast_mpc

        return live_forecast_mpc(risk="quantile", models_dir=directory, name="forecast_mpc")
    if name == "scales_l":
        from euroflex_bess_lab.realtime.strategies.prism import live_prism

        return live_prism(mechanism=mechanism, risk="quantile", models_dir=directory)
    raise KeyError(f"unknown strategy {name!r}; known: {sorted(STRATEGIES)}")


class DeferredStrategy(BaseStrategy):
    """Idle before ``start_minute`` (the history window), then the wrapped strategy decides."""

    def __init__(self, inner: Strategy, start_minute: int) -> None:
        self.inner = inner
        self.start_minute = start_minute
        self.name = str(getattr(inner, "name", type(inner).__name__))

    def reset(self, ctx: DecisionContext) -> None:
        self.inner.reset(ctx)

    def decide(self, ctx: DecisionContext) -> float:
        return 0.0 if ctx.t < self.start_minute else self.inner.decide(ctx)


@dataclass(frozen=True)
class ModelSet:
    directory: Path | None
    models_id: str | None
    problem: str | None
    matches_reference: bool | None


def inspect_models(directory: Path | None) -> ModelSet:
    """Identity of the local deployment models (``None`` id when absent or failing their manifest)."""
    if directory is None or not directory.is_dir():
        return ModelSet(directory, None, "models directory not found", None)
    files = build_models.describe_files(directory)
    missing = [name for name in build_models.REQUIRED_FILES if name not in files]
    if missing:
        return ModelSet(directory, None, f"missing model files: {', '.join(missing)}", None)
    manifest = directory / build_models.MANIFEST_NAME
    if manifest.is_file():
        problems = build_models.verify(directory, build_models.read_json(manifest))
        if problems:
            return ModelSet(directory, None, f"models fail their MANIFEST.json: {problems[0]}", None)
    models_id = build_models.models_id(files)
    reference = build_models.REFERENCE_MANIFEST
    matches = None
    if reference.is_file():
        matches = build_models.read_json(reference).get("models_id") == models_id
    return ModelSet(directory, models_id, None, matches)


def default_models_dir() -> Path:
    from euroflex_bess_lab.realtime.forecasting.deploy import models_dir

    return models_dir()


# ----------------------------------------------------------------------------------------------------------------
# time helpers


def to_utc(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def iso_z(ts: pd.Timestamp) -> str:
    return to_utc(ts).strftime("%Y-%m-%dT%H:%M:%SZ")


def day_bounds(day: date, timezone: str = TZ) -> tuple[pd.Timestamp, pd.Timestamp]:
    """UTC ``[start, end)`` of a local delivery day (23, 24 or 25 hours)."""
    start = pd.Timestamp(day).tz_localize(timezone).tz_convert("UTC")
    end = pd.Timestamp(day + timedelta(days=1)).tz_localize(timezone).tz_convert("UTC")
    return start, end


def day_segments(
    start: pd.Timestamp, end: pd.Timestamp, go_live: pd.Timestamp
) -> list[tuple[pd.Timestamp, pd.Timestamp, str]]:
    """Settlement mechanism by date: ``current`` before the go-live instant, ``new2026`` from it."""
    if go_live <= start:
        return [(start, end, "new2026")]
    if go_live >= end:
        return [(start, end, "current")]
    return [(start, go_live, "current"), (go_live, end, "new2026")]


def days_between(first: date, last: date) -> Iterator[date]:
    day = first
    while day <= last:
        yield day
        day += timedelta(days=1)


def minute_offset(market: MarketData, ts: pd.Timestamp) -> int:
    return int((ts - market.start_utc) / pd.Timedelta(minutes=1))


# ----------------------------------------------------------------------------------------------------------------
# data


@dataclass
class LoadedWindow:
    """Market data for ``[start, end)`` plus what the record needs besides the arrays."""

    market: MarketData
    qh_quality: pd.Series
    committed_end: pd.Timestamp | None
    provenance: dict[str, Any] = field(default_factory=dict)

    def origin(self, day_start: pd.Timestamp, day_end: pd.Timestamp) -> str:
        if self.committed_end is not None and day_end <= self.committed_end:
            return "public"
        if self.committed_end is not None and day_start < self.committed_end:
            return "public+forward"
        return "forward"


class DataSource(Protocol):
    def load(self, start: pd.Timestamp, end: pd.Timestamp) -> LoadedWindow: ...


TABLE_KEYS: dict[str, tuple[str, ...]] = {
    "minute": ("timestamp_utc",),
    "qh": ("timestamp_utc",),
    "nf_minute": ("timestamp_utc",),
    "nf_qh": ("timestamp_utc",),
    "si_current": ("prediction_time_utc", "target_qh_utc"),
    "si_next": ("prediction_time_utc", "target_qh_utc"),
    "day_ahead": ("timestamp_utc",),
}


def _empty_raw(*columns: str) -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype="datetime64[ns, UTC]") for c in columns})


def merge_tables(parts: Sequence[Mapping[str, pd.DataFrame | None]]) -> dict[str, pd.DataFrame | None]:
    """Concatenate per-source tables; on overlapping keys the earlier source (committed data) wins."""
    out: dict[str, pd.DataFrame | None] = {}
    for name, keys in TABLE_KEYS.items():
        frames: list[pd.DataFrame] = []
        for part in parts:
            frame = part.get(name)
            if frame is not None and len(frame):
                frames.append(frame)
        if not frames:
            out[name] = None
            continue
        merged = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0].reset_index(drop=True)
        merged = merged.drop_duplicates(subset=list(keys), keep="first").sort_values(list(keys), kind="stable")
        out[name] = merged.reset_index(drop=True)
    return out


def window_from_tables(
    tables: Mapping[str, pd.DataFrame | None],
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    publication: PublicationModel,
    committed_end: pd.Timestamp | None,
    provenance: dict[str, Any] | None = None,
) -> LoadedWindow:
    """``MarketData`` with exactly the field layout of ``MarketData.from_public`` for the same tables."""
    minute_frames: dict[str, pd.DataFrame] = {}
    qh_frames: dict[str, pd.DataFrame] = {}
    si_frames: dict[str, pd.DataFrame] = {}
    for target, prefix, key in (
        (minute_frames, "", "minute"),
        (minute_frames, "nf_", "nf_minute"),
        (qh_frames, "", "qh"),
        (qh_frames, "nf_", "nf_qh"),
        (si_frames, "current_qh", "si_current"),
        (si_frames, "next_qh", "si_next"),
    ):
        frame = tables.get(key)
        if frame is not None:
            target[prefix] = frame
    market = MarketData.from_frames(
        start=start,
        end=end,
        minute_frames=minute_frames,
        qh_frames=qh_frames,
        da_frame=tables.get("day_ahead"),
        si_forecast_frames=si_frames,
        publication=publication,
        meta={"source": "track_record"},
    )
    qh = tables.get("qh")
    if qh is not None and "quality_status" in qh.columns:
        quality = pd.Series(qh["quality_status"].astype(str).to_numpy(), index=pd.DatetimeIndex(qh["timestamp_utc"]))
    else:
        quality = pd.Series(dtype=str, index=pd.DatetimeIndex([], tz="UTC"))
    return LoadedWindow(market, quality, committed_end, dict(provenance or {}))


def committed_tables(start: pd.Timestamp, end: pd.Timestamp, go_live: pd.Timestamp) -> dict[str, pd.DataFrame | None]:
    """Contract tables from ``data/public`` through the public loaders (missing optional tables are ``None``)."""

    def optional(loader: Any, *args: Any) -> pd.DataFrame | None:
        try:
            frame = loader(*args)
        except (FileNotFoundError, KeyError):
            return None
        return frame if len(frame) else None

    tables: dict[str, pd.DataFrame | None] = {
        "minute": optional(public_data.load_belgium_minute, start, end),
        "qh": optional(public_data.load_belgium_qh, start, end),
        "si_current": optional(public_data.load_belgium_si_forecast, "current", start, end),
        "si_next": optional(public_data.load_belgium_si_forecast, "next", start, end),
        "day_ahead": optional(public_data.load_day_ahead_prices, "BE", start, end),
        "nf_minute": None,
        "nf_qh": None,
    }
    if (start < SIMULATION_WINDOW[1] and end > SIMULATION_WINDOW[0]) or end > go_live:
        tables["nf_minute"] = optional(public_data.load_belgium_new_formula_minute, start, end)
        tables["nf_qh"] = optional(public_data.load_belgium_new_formula_qh, start, end)
    return tables


def committed_data_end(timezone: str = TZ) -> pd.Timestamp | None:
    """Exclusive end of the committed quarter-hour prices, floored to a local midnight (complete days only)."""
    year = pd.Timestamp.now(tz="UTC").year
    for candidate in range(year, 2023, -1):
        try:
            qh = public_data.load_belgium_qh(f"{candidate}-01-01", f"{candidate + 1}-01-01")
        except FileNotFoundError:
            continue
        qh = qh.dropna(subset=["imbalance_price_eur_mwh"])
        if len(qh):
            last = pd.Timestamp(qh["timestamp_utc"].max()) + pd.Timedelta(minutes=15)
            return last.tz_convert(timezone).normalize().tz_convert("UTC")
    return None


class PublicForwardSource:
    """Committed ``data/public`` where it covers the window; newer days via the public-data fetch code.

    Newer days are fetched with :meth:`BuildContext.fetch` (Elia, month/year chunks) and
    :func:`energy_charts.fetch_month` (day-ahead prices) into ``raw_dir`` only (read-only GETs; ``offline=True``
    never touches the network), then normalized with the same functions that build ``data/public``.
    """

    def __init__(
        self,
        *,
        raw_dir: Path,
        offline: bool = False,
        refresh: bool = False,
        go_live_utc: pd.Timestamp = NEW_FORMULA_GO_LIVE_UTC,
        publication: PublicationModel = PUBLICATION,
        committed_end: pd.Timestamp | None | str = "auto",
        new_formula_datasets: tuple[str, str] = NEW_FORMULA_DATASETS,
        client: HttpClient | None = None,
    ) -> None:
        self.raw_dir = raw_dir
        self.offline = offline
        self.refresh = refresh
        self.go_live_utc = go_live_utc
        self.publication = publication
        self.committed_end: pd.Timestamp | None = (
            committed_data_end() if isinstance(committed_end, str) else committed_end
        )
        self.new_formula_datasets = new_formula_datasets
        self.client = client or HttpClient(config=HttpConfig(read_timeout_s=300.0, max_retries=4))
        self.records: list[dict[str, Any]] = []

    def load(self, start: pd.Timestamp, end: pd.Timestamp) -> LoadedWindow:
        cut = self.committed_end
        parts: list[Mapping[str, pd.DataFrame | None]] = []
        if cut is not None and start < cut:
            parts.append(committed_tables(start, min(end, cut), self.go_live_utc))
        if cut is None or end > cut:
            parts.append(self.forward_tables(max(start, cut) if cut is not None else start, end))
        inputs = {(str(r["dataset_id"]), str(r["chunk"])): r for r in self.records}
        provenance = {
            "committed_end_utc": iso_z(cut) if cut is not None else None,
            "forward_inputs": [inputs[key] for key in sorted(inputs)],
        }
        return window_from_tables(
            merge_tables(parts),
            start,
            end,
            publication=self.publication,
            committed_end=cut,
            provenance=provenance,
        )

    # -- forward (newer than the committed data set) --

    def _context(self) -> BuildContext:
        return BuildContext(
            root=public_data.data_root(),
            raw_dir=self.raw_dir,
            client=self.client,
            refresh=self.refresh,
            offline=self.offline,
        )

    def _fetch(
        self, dataset_id: str, start: pd.Timestamp, end: pd.Timestamp, *, freq: str | None = None
    ) -> list[RawChunk]:
        """Chunk by chunk, so that a chunk missing from an offline cache only drops that chunk."""
        spec = ELIA_DATASETS[dataset_id]
        chunk_freq = freq or spec.chunk
        chunks: list[RawChunk] = []
        for chunk in plan_chunks(start, end, chunk_freq):
            assert chunk.start is not None and chunk.end is not None
            try:
                if freq is None:
                    got = self._context().fetch(dataset_id, chunk.start, chunk.end)
                else:
                    got = fetch_dataset(
                        self.client,
                        self.raw_dir,
                        dataset_id,
                        spec.time_field,
                        chunk.start,
                        chunk.end,
                        freq=chunk_freq,
                        refresh=self.refresh,
                        offline=self.offline,
                    )
            except FileNotFoundError:
                continue
            chunks.extend(got)
        for item in chunks:
            self.records.append(
                {
                    "dataset_id": item.dataset_id,
                    "chunk": item.chunk.label,
                    "retrieved_at_utc": item.record.retrieved_at_utc,
                    "sha256": item.record.sha256,
                }
            )
        return chunks

    def _raw(self, dataset_id: str, start: pd.Timestamp, end: pd.Timestamp, *, time_field: str) -> pd.DataFrame:
        chunks = self._fetch(dataset_id, start, end)
        frame = read_chunks(chunks) if chunks else pd.DataFrame()
        return frame if time_field in frame.columns else _empty_raw(time_field)

    def forward_tables(self, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, pd.DataFrame | None]:
        raw = {ds: self._raw(ds, start, end, time_field=ELIA_DATASETS[ds].time_field) for ds in CORE_DATASETS}
        minute, _ = normalize_minute(raw["ods133"], raw["ods126"], raw["ods165"], start, end)
        qh, _ = normalize_qh(raw["ods134"], raw["ods166"], raw["ods127"], start, end)
        tables: dict[str, pd.DataFrame | None] = {
            "minute": minute if len(minute) else None,
            "qh": qh if len(qh) else None,
            "si_current": self._si_forecast(raw["ods136"], start, end),
            "si_next": self._si_forecast(raw["ods147"], start, end),
            "day_ahead": self._day_ahead(start, end),
            "nf_minute": None,
            "nf_qh": None,
        }
        if end > self.go_live_utc:
            lo = max(start, self.go_live_utc - pd.Timedelta(days=1))
            for key, dataset_id, minute_rows in (
                ("nf_minute", self.new_formula_datasets[0], True),
                ("nf_qh", self.new_formula_datasets[1], False),
            ):
                chunks = self._fetch(dataset_id, lo, end, freq="month")
                frame = read_chunks(chunks) if chunks else pd.DataFrame()
                if "datetime" not in frame.columns or frame.empty:
                    continue
                normalized, _ = normalize_new_formula(frame, minute=minute_rows)
                normalized = normalized[(normalized["timestamp_utc"] >= start) & (normalized["timestamp_utc"] < end)]
                tables[key] = normalized.reset_index(drop=True) if len(normalized) else None
        return tables

    @staticmethod
    def _si_forecast(raw: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame | None:
        if raw.empty or "predictions_forecastedtimeutc" not in raw.columns:
            return None
        ts = pd.to_datetime(raw["predictiontimeutc"], utc=True)
        frame, _ = normalize_si_forecast(raw[(ts >= start) & (ts < end)])
        return frame if len(frame) else None

    def _day_ahead(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame | None:
        first = start.tz_convert(TZ).tz_localize(None)
        last = (end - pd.Timedelta(minutes=1)).tz_convert(TZ).tz_localize(None)
        chunks = []
        for month in energy_charts.month_starts(first, last):
            try:
                chunk = energy_charts.fetch_month(
                    self.client, self.raw_dir, "BE", month, refresh=self.refresh, offline=self.offline
                )
            except FileNotFoundError:
                continue
            chunks.append(chunk)
            self.records.append(
                {
                    "dataset_id": "energy_charts_price_BE",
                    "chunk": f"{month:%Y-%m}",
                    "retrieved_at_utc": chunk.record.retrieved_at_utc,
                    "sha256": chunk.record.sha256,
                }
            )
        frame, _ = energy_charts.normalize_months(chunks)
        frame = frame[(frame["timestamp_utc"] >= start) & (frame["timestamp_utc"] < end)].reset_index(drop=True)
        return frame if len(frame) else None


# ----------------------------------------------------------------------------------------------------------------
# one delivery day


@dataclass(frozen=True)
class DayCheck:
    day: date
    complete: bool
    reason: str
    n_qh: int
    price_missing_qh: int
    minutes_missing: int
    qh_not_validated: int


def check_day(window: LoadedWindow, day: date, *, go_live: pd.Timestamp, timezone: str = TZ) -> DayCheck:
    """A day is complete when every quarter-hour has a published settlement price and minute data reach its end."""
    market = window.market
    d0, d1 = day_bounds(day, timezone)
    problems: list[str] = []
    price_missing = minutes_missing = n_qh = 0
    for s0, s1, mechanism in day_segments(d0, d1, go_live):
        q0, q1 = minute_offset(market, s0) // 15, minute_offset(market, s1) // 15
        m0, m1 = minute_offset(market, s0), minute_offset(market, s1)
        n_qh += q1 - q0
        prices = market.settlement_prices.get(mechanism)
        price_missing += (q1 - q0) if prices is None else int(np.isnan(prices[q0:q1]).sum())
        values = market.minute.get(MINUTE_SIGNAL[mechanism])
        if values is None:
            minutes_missing += m1 - m0
            problems.append(f"no {MINUTE_SIGNAL[mechanism]} minute data")
            continue
        segment = values[m0:m1]
        minutes_missing += int(np.isnan(segment).sum())
        if not np.isfinite(segment[-15:]).any():
            problems.append("minute data do not reach the end of the day")
    if price_missing:
        problems.insert(0, f"{price_missing} quarter-hours without a published settlement price")
    quality = window.qh_quality
    in_day = quality[(quality.index >= d0) & (quality.index < d1)]
    not_validated = int((in_day != "Validated").sum()) + max(0, n_qh - len(in_day))
    return DayCheck(
        day=day,
        complete=not problems,
        reason="; ".join(problems) if problems else "complete",
        n_qh=n_qh,
        price_missing_qh=price_missing,
        minutes_missing=minutes_missing,
        qh_not_validated=not_validated,
    )


class StrategyCache:
    """Builds each (strategy, mechanism) once per run (model pickles are loaded once)."""

    def __init__(self, models_dir: Path | None) -> None:
        self.models_dir = models_dir
        self._built: dict[tuple[str, str], Strategy] = {}

    def get(self, name: str, mechanism: str) -> Strategy:
        key = (name, mechanism)
        if key not in self._built:
            self._built[key] = build_strategy(name, mechanism, self.models_dir)
        return self._built[key]


def soc_fraction(soc_mwh: float, battery: BatteryParams) -> float:
    return min(max(soc_mwh / battery.energy_mwh, battery.soc_min_frac), battery.soc_max_frac)


def run_day(
    market: MarketData,
    day: date,
    name: str,
    soc_start_mwh: float,
    strategies: StrategyCache,
    *,
    battery: BatteryParams = BATTERY,
    publication: PublicationModel = PUBLICATION,
    go_live: pd.Timestamp = NEW_FORMULA_GO_LIVE_UTC,
    timezone: str = TZ,
) -> pd.DataFrame | None:
    """Quarter-hour ledger of strategy ``name`` on ``day``; ``market`` must end at the end of the day.

    Each settlement segment is simulated on the market cut at the segment end (nothing later is loaded), with the
    strategy idle before the segment start and the state of charge carried over. ``None``: not run that day.
    """
    spec = STRATEGIES[name]
    d0, d1 = day_bounds(day, timezone)
    if market.end_utc != d1:
        raise ValueError("the market window must end at the end of the delivery day")
    soc = soc_start_mwh
    ledgers: list[pd.DataFrame] = []
    for s0, s1, mechanism in day_segments(d0, d1, go_live):
        if mechanism not in spec.mechanisms:
            continue
        segment_market = market if s1 == market.end_utc else market.slice(market.start_utc, s1)
        signal = SIGNAL_BY_MECHANISM[mechanism]
        if name == "threshold" and signal not in segment_market.minute:
            segment_market = segment_market.with_minute_fields({signal: np.full(segment_market.n_minutes, np.nan)})
        strategy = DeferredStrategy(strategies.get(name, mechanism), minute_offset(segment_market, s0))
        result = simulate(
            segment_market,
            strategy,
            replace(battery, initial_soc_frac=soc_fraction(soc, battery)),
            mechanism=mechanism,
            publication=publication,
            record_minutes=False,
        )
        qh = result.qh[result.qh["timestamp_utc"] >= s0].copy()
        qh["mechanism"] = mechanism
        soc = float(qh["soc_end_mwh"].iloc[-1])
        ledgers.append(qh)
    if not ledgers:
        return None
    return pd.concat(ledgers, ignore_index=True)


def daily_row(
    qh: pd.DataFrame,
    *,
    day: date,
    name: str,
    soc_start_mwh: float,
    check: DayCheck,
    origin: str,
    models_id: str,
    computed_at: pd.Timestamp,
    battery: BatteryParams = BATTERY,
) -> dict[str, Any]:
    discharged = float(qh["discharged_mwh"].sum())

    def eur(column: str) -> float:
        return round(float(qh[column].sum()), ROUND_EUR) + 0.0

    return {
        "local_date": day.isoformat(),
        "strategy": name,
        "mechanism": "+".join(dict.fromkeys(qh["mechanism"].astype(str))),
        "n_qh": len(qh),
        "pnl_eur": eur("pnl_eur"),
        "revenue_eur": eur("revenue_eur"),
        "degradation_eur": eur("degradation_eur"),
        "discharged_mwh": round(discharged, ROUND_MWH),
        "charged_mwh": round(float(qh["charged_mwh"].sum()), ROUND_MWH),
        "cycles": round(discharged / battery.usable_energy_mwh, 4),
        "cumulative_pnl_eur": math.nan,
        "soc_start_mwh": float(soc_start_mwh),
        "soc_end_mwh": float(qh["soc_end_mwh"].iloc[-1]),
        "price_missing_qh": int(qh["price_missing"].astype(bool).sum()),
        "qh_not_validated": check.qh_not_validated,
        "minutes_missing": check.minutes_missing,
        "data_complete": check.complete,
        "data_source": origin,
        "models_id": models_id,
        "computed_at_utc": iso_z(computed_at),
    }


def ledger_rows(qh: pd.DataFrame, name: str) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            "timestamp_utc": [iso_z(ts) for ts in pd.to_datetime(qh["timestamp_utc"], utc=True)],
            "strategy": name,
            "mechanism": qh["mechanism"].astype(str).to_numpy(),
        }
    )
    for column in ("energy_mwh", "discharged_mwh", "charged_mwh", "soc_end_mwh"):
        out[column] = qh[column].to_numpy(dtype=float).round(ROUND_MWH) + 0.0  # + 0.0: no negative zeros
    for column in ("price_eur_mwh", "revenue_eur", "degradation_eur", "pnl_eur"):
        out[column] = qh[column].to_numpy(dtype=float).round(ROUND_EUR) + 0.0
    out["price_missing"] = qh["price_missing"].astype(bool).to_numpy()
    return out[list(LEDGER_COLUMNS)]


# ----------------------------------------------------------------------------------------------------------------
# storage (append-only, idempotent, canonical order)


def coerce(frame: pd.DataFrame, schema: Mapping[str, str]) -> pd.DataFrame:
    out = pd.DataFrame(index=frame.index)
    for column, kind in schema.items():
        values = frame[column] if column in frame.columns else pd.Series([None] * len(frame), index=frame.index)
        if kind == "str":
            out[column] = values.where(values.notna(), "").astype(str)
        elif kind == "int":
            out[column] = pd.to_numeric(values, errors="coerce").fillna(0).astype("int64")
        elif kind == "bool":
            out[column] = values.map(lambda v: str(v).strip().lower() in {"true", "1"}).astype(bool)
        else:
            out[column] = pd.to_numeric(values, errors="coerce").astype(float)
    return out.reset_index(drop=True)


def write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    frame.to_csv(tmp, index=False, lineterminator="\n")
    os.replace(tmp, path)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


class TrackStore:
    """The files under ``reports/track_record``; existing rows always win (a recorded day is never rewritten)."""

    def __init__(self, out_dir: Path) -> None:
        self.out_dir = out_dir

    @property
    def daily_path(self) -> Path:
        return self.out_dir / DAILY_CSV

    def read_daily(self) -> pd.DataFrame:
        if not self.daily_path.is_file():
            return coerce(pd.DataFrame(columns=list(DAILY_COLUMNS)), DAILY_COLUMNS)
        return coerce(pd.read_csv(self.daily_path, dtype=str, keep_default_na=False), DAILY_COLUMNS)

    def read_ledger(self) -> pd.DataFrame:
        files = sorted((self.out_dir / LEDGER_DIR).glob("*.csv"))
        if not files:
            return coerce(pd.DataFrame(columns=list(LEDGER_COLUMNS)), LEDGER_COLUMNS)
        frames = [pd.read_csv(f, dtype=str, keep_default_na=False) for f in files]
        return coerce(pd.concat(frames, ignore_index=True), LEDGER_COLUMNS)

    def append(self, ledger: pd.DataFrame, daily: pd.DataFrame) -> None:
        if daily.empty:
            return
        months = sorted({d[:7] for d in daily["local_date"]})
        new_ledger = coerce(ledger, LEDGER_COLUMNS)
        local_month = pd.to_datetime(new_ledger["timestamp_utc"], utc=True).dt.tz_convert(TZ).dt.strftime("%Y-%m")
        for month in months:
            path = self.out_dir / LEDGER_DIR / f"{month}.csv"
            existing = (
                coerce(pd.read_csv(path, dtype=str, keep_default_na=False), LEDGER_COLUMNS) if path.is_file() else None
            )
            fresh = new_ledger[local_month.to_numpy() == month]
            merged = pd.concat([f for f in (existing, fresh) if f is not None and len(f)], ignore_index=True)
            merged = merged.drop_duplicates(["strategy", "timestamp_utc"], keep="first")
            merged = merged.sort_values(["timestamp_utc", "strategy"], kind="stable")
            write_csv(path, coerce(merged, LEDGER_COLUMNS))
        old = self.read_daily()
        merged_daily = pd.concat([f for f in (old, coerce(daily, DAILY_COLUMNS)) if len(f)], ignore_index=True)
        merged_daily = merged_daily.drop_duplicates(["local_date", "strategy"], keep="first")
        write_csv(self.daily_path, finalize_daily(merged_daily))


def finalize_daily(daily: pd.DataFrame) -> pd.DataFrame:
    """Canonical order and the running cumulative P&L per strategy (derived from the stored daily P&L)."""
    frame = coerce(daily, DAILY_COLUMNS).sort_values(["strategy", "local_date"], kind="stable")
    frame["cumulative_pnl_eur"] = frame.groupby("strategy")["pnl_eur"].cumsum().round(ROUND_EUR)
    frame = frame.sort_values(["local_date", "strategy"], kind="stable")
    return coerce(frame, DAILY_COLUMNS)


# ----------------------------------------------------------------------------------------------------------------
# update


@dataclass
class TrackConfig:
    out_dir: Path = DEFAULT_OUT
    track_start: date = TRACK_START
    battery: BatteryParams = BATTERY
    publication: PublicationModel = PUBLICATION
    warmup_days: int = WARMUP_DAYS
    go_live_utc: pd.Timestamp = NEW_FORMULA_GO_LIVE_UTC
    timezone: str = TZ
    max_wait_days: float = MAX_WAIT_DAYS
    strategies: tuple[str, ...] = tuple(STRATEGIES)
    models_dir: Path | None = None
    allow_model_change: bool = False


@dataclass
class UpdateReport:
    appended: list[dict[str, Any]] = field(default_factory=list)
    skipped: dict[str, str] = field(default_factory=dict)
    pending: dict[str, str] | None = None
    errors: list[str] = field(default_factory=list)
    last_complete_day: str | None = None

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def runnable_strategies(
    cfg: TrackConfig, daily: pd.DataFrame, models: ModelSet
) -> tuple[dict[str, str], dict[str, str]]:
    """``({name: models_id}, {name: reason not run})`` for the configured strategies."""
    run: dict[str, str] = {}
    skipped: dict[str, str] = {}
    for name in cfg.strategies:
        spec = STRATEGIES[name]
        if not spec.needs_models:
            run[name] = ""
            continue
        if models.models_id is None:
            skipped[name] = f"models not available ({models.problem})"
            continue
        recorded = daily.loc[(daily["strategy"] == name) & (daily["models_id"] != ""), "models_id"]
        if len(recorded) and recorded.iloc[-1] != models.models_id and not cfg.allow_model_change:
            skipped[name] = (
                f"models changed: recorded {recorded.iloc[-1]}, found {models.models_id} "
                "(frozen strategies; pass --allow-model-change to continue with new models)"
            )
            continue
        run[name] = models.models_id
    return run, skipped


def update(
    cfg: TrackConfig,
    source: DataSource,
    *,
    now: pd.Timestamp,
    until: date | None = None,
    dry_run: bool = False,
) -> UpdateReport:
    """Simulate and append every complete day not yet recorded (per strategy, in order)."""
    store = TrackStore(cfg.out_dir)
    daily = store.read_daily()
    models = inspect_models(cfg.models_dir) if any(STRATEGIES[n].needs_models for n in cfg.strategies) else None
    run, skipped = runnable_strategies(cfg, daily, models or ModelSet(None, None, "not needed", None))
    report = UpdateReport(skipped=skipped)
    last_day = (now.tz_convert(cfg.timezone) - pd.Timedelta(days=1)).date()
    if until is not None:
        last_day = min(last_day, until)
    report.last_complete_day = last_day.isoformat()
    next_day: dict[str, date] = {}
    soc: dict[str, float] = {}
    for name in list(run):
        rows = daily[daily["strategy"] == name].sort_values("local_date")
        if len(rows):
            next_day[name] = date.fromisoformat(str(rows["local_date"].iloc[-1])) + timedelta(days=1)
            soc[name] = float(rows["soc_end_mwh"].iloc[-1])
        else:
            next_day[name] = cfg.track_start
            soc[name] = cfg.battery.initial_soc_mwh
        if (
            "new2026" not in STRATEGIES[name].mechanisms
            and day_bounds(next_day[name], cfg.timezone)[0] >= cfg.go_live_utc
        ):
            skipped[name] = "retired at the new-formula go-live (its model prices the current formula)"
            del run[name], next_day[name], soc[name]
    if not run:
        return report
    first = min(next_day.values())
    if first > last_day:
        return report
    span_start = day_bounds(first, cfg.timezone)[0] - pd.Timedelta(days=cfg.warmup_days)
    span_end = day_bounds(last_day, cfg.timezone)[1]
    window = source.load(span_start, span_end)
    strategies = StrategyCache(cfg.models_dir)
    ledgers: list[pd.DataFrame] = []
    rows_out: list[dict[str, Any]] = []
    for day in days_between(first, last_day):
        due = [name for name in run if next_day[name] == day]
        if not due:
            continue
        check = check_day(window, day, go_live=cfg.go_live_utc, timezone=cfg.timezone)
        d0, d1 = day_bounds(day, cfg.timezone)
        # An incomplete day waits for Elia; after max_wait_days it is recorded with its gaps flagged, but only if
        # most of its prices exist (a day without data is never recorded).
        overdue = now >= d1 + pd.Timedelta(days=cfg.max_wait_days)
        if not check.complete and not (overdue and 2 * check.price_missing_qh <= check.n_qh):
            report.pending = {"day": day.isoformat(), "reason": check.reason}
            break
        market = window.market.slice(d0 - pd.Timedelta(days=cfg.warmup_days), d1)
        for name in due:
            try:
                qh = run_day(
                    market,
                    day,
                    name,
                    soc[name],
                    strategies,
                    battery=cfg.battery,
                    publication=cfg.publication,
                    go_live=cfg.go_live_utc,
                    timezone=cfg.timezone,
                )
            except Exception as exc:  # noqa: BLE001 - one strategy failing must not block the others
                report.errors.append(f"{day.isoformat()} {name}: {type(exc).__name__}: {exc}"[:400])
                next_day[name] = date.max  # this strategy stops here for this run (its days stay in order)
                continue
            if qh is None:  # retired for this mechanism (e.g. forecast MPC after the go-live)
                next_day[name] = date.max
                skipped.setdefault(name, f"not run from {day.isoformat()}: no model for the formula in force")
                continue
            row = daily_row(
                qh,
                day=day,
                name=name,
                soc_start_mwh=soc[name],
                check=check,
                origin=window.origin(d0, d1),
                models_id=run[name],
                computed_at=now,
                battery=cfg.battery,
            )
            ledgers.append(ledger_rows(qh, name))
            rows_out.append(row)
            report.appended.append({k: row[k] for k in ("local_date", "strategy", "pnl_eur", "n_qh")})
            next_day[name] = day + timedelta(days=1)
            soc[name] = float(row["soc_end_mwh"])
    if rows_out and not dry_run:  # nothing new: no file is touched (re-running is a no-op)
        store.append(pd.concat(ledgers, ignore_index=True), pd.DataFrame(rows_out))
        context = {
            "committed_data_end_utc": window.provenance.get("committed_end_utc"),
            "last_update": {
                "ran_at_utc": iso_z(now),
                "appended_rows": len(rows_out),
                "pending": report.pending,
                "not_run": dict(sorted(skipped.items())),
                "errors": report.errors,
                "models_id": models.models_id if models is not None else None,
                "models_match_reference": models.matches_reference if models is not None else None,
                "forward_inputs": window.provenance.get("forward_inputs", []),
            },
        }
        render(cfg, context=context)
    return report


# ----------------------------------------------------------------------------------------------------------------
# verify


def verify(cfg: TrackConfig, source: DataSource, *, days: Sequence[date] | None = None) -> list[dict[str, Any]]:
    """Re-simulate recorded days from the recorded starting SOC with current data; report P&L differences."""
    store = TrackStore(cfg.out_dir)
    daily = store.read_daily()
    if days is not None:
        wanted = {d.isoformat() for d in days}
        daily = daily[daily["local_date"].isin(wanted)]
    if daily.empty:
        return []
    first = date.fromisoformat(str(daily["local_date"].min()))
    last = date.fromisoformat(str(daily["local_date"].max()))
    window = source.load(
        day_bounds(first, cfg.timezone)[0] - pd.Timedelta(days=cfg.warmup_days), day_bounds(last, cfg.timezone)[1]
    )
    strategies = StrategyCache(cfg.models_dir)
    out: list[dict[str, Any]] = []
    for row in daily.itertuples(index=False):
        day = date.fromisoformat(str(row.local_date))
        d0, d1 = day_bounds(day, cfg.timezone)
        market = window.market.slice(d0 - pd.Timedelta(days=cfg.warmup_days), d1)
        try:
            qh = run_day(
                market,
                day,
                str(row.strategy),
                float(row.soc_start_mwh),
                strategies,
                battery=cfg.battery,
                publication=cfg.publication,
                go_live=cfg.go_live_utc,
                timezone=cfg.timezone,
            )
        except Exception as exc:  # noqa: BLE001
            out.append({"local_date": row.local_date, "strategy": row.strategy, "error": f"{type(exc).__name__}"})
            continue
        pnl = round(float(qh["pnl_eur"].sum()), ROUND_EUR) if qh is not None else math.nan
        out.append(
            {
                "local_date": row.local_date,
                "strategy": row.strategy,
                "recorded_pnl_eur": float(row.pnl_eur),
                "recomputed_pnl_eur": pnl,
                "abs_diff_eur": round(abs(pnl - float(row.pnl_eur)), ROUND_EUR),
            }
        )
    return out


# ----------------------------------------------------------------------------------------------------------------
# rendering: summary.json, README.md, chart


def strategy_stats(rows: pd.DataFrame, battery: BatteryParams) -> dict[str, Any]:
    rows = rows.sort_values("local_date")
    pnl = rows["pnl_eur"].to_numpy(dtype=float)
    days_equiv = float(rows["n_qh"].sum()) / 96.0
    cumulative = np.cumsum(pnl)
    drawdown = float(np.max(np.maximum.accumulate(cumulative) - cumulative)) if len(cumulative) else 0.0
    best = rows.loc[rows["pnl_eur"].idxmax()]
    worst = rows.loc[rows["pnl_eur"].idxmin()]
    total = float(pnl.sum())
    return {
        "first_day": str(rows["local_date"].iloc[0]),
        "last_day": str(rows["local_date"].iloc[-1]),
        "days": len(rows),
        "pnl_eur": round(total, 2),
        "eur_per_mw_year_to_date": round(total / battery.power_mw / (days_equiv / 365.25), 0) if days_equiv else None,
        "mean_daily_pnl_eur": round(float(pnl.mean()), 2),
        "cycles_per_day": round(float(rows["discharged_mwh"].sum()) / battery.usable_energy_mwh / days_equiv, 3),
        "positive_day_share": round(float(np.mean(pnl > 0)), 3),
        "best_day": {"local_date": str(best["local_date"]), "pnl_eur": round(float(best["pnl_eur"]), 2)},
        "worst_day": {"local_date": str(worst["local_date"]), "pnl_eur": round(float(worst["pnl_eur"]), 2)},
        "max_drawdown_eur": round(drawdown, 2),
        "price_missing_qh": int(rows["price_missing_qh"].sum()),
        "incomplete_data_days": int((~rows["data_complete"]).sum()),
        "soc_end_mwh": float(rows["soc_end_mwh"].iloc[-1]),
        "models_id": next((m for m in reversed(rows["models_id"].tolist()) if m), None),
    }


def build_summary(cfg: TrackConfig, daily: pd.DataFrame, context: Mapping[str, Any]) -> dict[str, Any]:
    reference = (
        build_models.read_json(build_models.REFERENCE_MANIFEST) if build_models.REFERENCE_MANIFEST.is_file() else {}
    )
    strategies: dict[str, Any] = {}
    for name in cfg.strategies:
        spec = STRATEGIES[name]
        rows = daily[daily["strategy"] == name]
        entry: dict[str, Any] = {
            "label": spec.label,
            "description": spec.description,
            "backtest_name": spec.backtest_name,
            "settles_under": list(spec.mechanisms),
            "needs_models": spec.needs_models,
        }
        if len(rows):
            entry.update(strategy_stats(rows, cfg.battery))
        else:
            entry["days"] = 0
        strategies[name] = entry
    return {
        "schema": SCHEMA,
        "title": "Forward paper track record of the frozen live strategies (Belgian implicit balancing)",
        "paper_only": True,
        "market_submission": False,
        "note": PAPER_NOTE,
        "track_start": cfg.track_start.isoformat(),
        "last_day": str(daily["local_date"].max()) if len(daily) else None,
        "updated_at_utc": str(daily["computed_at_utc"].max()) if len(daily) else None,
        "battery": asdict(cfg.battery),
        "publication": asdict(cfg.publication),
        "warmup_days": cfg.warmup_days,
        "timezone": cfg.timezone,
        "settlement": {
            "current": "Elia current imbalance-price formula, ODS134 published price (before the go-live)",
            "new2026": "Elia new imbalance-price formula, ODS212 published price (from the go-live)",
            "go_live_utc": iso_z(cfg.go_live_utc),
        },
        "models": {
            "reference_models_id": reference.get("models_id"),
            "fold": reference.get("fold"),
            "reference_manifest": "models/reference_manifest.json",
            "build": "python scripts/ops/build_models.py build --out models",
        },
        "strategies": strategies,
        "attribution": list(ATTRIBUTION),
        "files": {"daily": DAILY_CSV, "ledger": f"{LEDGER_DIR}/<YYYY-MM>.csv", "chart": CHART_PNG},
        "context": dict(context),
    }


def _fmt(value: Any, digits: int = 0) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "-"
    return f"{float(value):,.{digits}f}"


def render_readme(summary: Mapping[str, Any], daily: pd.DataFrame) -> str:
    battery = summary["battery"]
    strategies = summary["strategies"]
    recorded = [n for n, s in strategies.items() if s.get("days")]
    n_days = int(daily["local_date"].nunique()) if len(daily) else 0
    lines = [
        "# Forward track record (paper)",
        "",
        "> Generated by `scripts/ops/track_record.py` from `daily_pnl.csv` and `qh/`; do not edit by hand.",
        f"> {PAPER_NOTE}",
        "",
        "Out-of-sample record of the frozen live strategies on every complete Belgian delivery day after the end of the",
        "committed data set: the forecasters were fitted on data up to 2026-10-24 and are never refitted. Each day is",
        "simulated minute by minute with the backtest engine and publication model on Elia's published data.",
        "",
    ]
    if n_days:
        first_day = str(daily["local_date"].min())
        period = first_day if first_day == summary["last_day"] else f"{first_day} to {summary['last_day']}"
        lines.append(f"Days recorded: **{n_days}** ({period}); last update {summary['updated_at_utc']}.")
    else:
        lines.append(f"No day recorded yet (the record starts on {summary['track_start']}).")
    lines += [
        "",
        "| Strategy | Days | Cumulative P&L (EUR) | EUR/MW/yr to date | Mean day (EUR) | Cycles/day | Positive days | Best day | Worst day |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name, entry in strategies.items():
        if not entry.get("days"):
            reason = summary.get("context", {}).get("last_update", {}).get("not_run", {}).get(name)
            note = f"not run yet{': ' + reason if reason else ''}"
            lines.append(f"| {entry['label']} | 0 | {note} | | | | | | |")
            continue
        lines.append(
            f"| {entry['label']} | {entry['days']} | {_fmt(entry['pnl_eur'], 2)} | "
            f"{_fmt(entry['eur_per_mw_year_to_date'])} | {_fmt(entry['mean_daily_pnl_eur'], 2)} | "
            f"{_fmt(entry['cycles_per_day'], 2)} | {100 * entry['positive_day_share']:.0f} % | "
            f"{_fmt(entry['best_day']['pnl_eur'], 2)} ({entry['best_day']['local_date']}) | "
            f"{_fmt(entry['worst_day']['pnl_eur'], 2)} ({entry['worst_day']['local_date']}) |"
        )
    lines += [
        "",
        f"EUR/MW/yr annualises the P&L to date ({n_days} day{'s' if n_days != 1 else ''}); until the record covers",
        "several months it is dominated by day-to-day noise and should not be compared with the backtest figures.",
        "",
    ]
    if recorded:
        lines += [f"![Cumulative paper P&L]({CHART_PNG})", ""]
        recent = daily.pivot_table(index="local_date", columns="strategy", values="pnl_eur", aggfunc="sum")
        recent = recent.sort_index(ascending=False).head(14)
        names = [n for n in strategies if n in recent.columns]
        lines += [
            "## Daily P&L (EUR, latest 14 days)",
            "",
            "| Day | " + " | ".join(strategies[n]["label"] for n in names) + " |",
            "| --- | " + " | ".join("---:" for _ in names) + " |",
        ]
        for day, values in recent.iterrows():
            lines.append(f"| {day} | " + " | ".join(_fmt(values.get(n), 2) for n in names) + " |")
        lines.append("")
    models = summary["models"]
    fold = models.get("fold") or {}
    lines += [
        "## Method",
        "",
        f"- Battery: {battery['power_mw']:g} MW / {battery['energy_mwh']:g} MWh, efficiency "
        f"{battery['charge_efficiency']:g} x {battery['discharge_efficiency']:g}, SOC band "
        f"{100 * battery['soc_min_frac']:.0f}-{100 * battery['soc_max_frac']:.0f} %, degradation "
        f"{battery['degradation_eur_per_mwh']:g} EUR per MWh discharged; price taker (no own price impact).",
        "- Engine: `euroflex_bess_lab.realtime.simulate`, one decision per minute; minute data usable "
        f"{summary['publication']['minute_lag']} minute after the minute ends, final quarter-hour prices "
        f"{summary['publication']['qh_final_lag_minutes']} minutes after the quarter-hour, day-ahead prices from "
        f"{summary['publication']['da_publication_hour_local']}:00 on D-1. The live paper-trading engine takes "
        "identical decisions on identical data (`euroflex live replay`, `docs/live_paper_trading.md`).",
        f"- Continuity: the state of charge carries over from day to day; each day sees a {summary['warmup_days']}-day "
        "history window in which the strategy does not trade.",
        f"- Settlement: current formula (ODS134 price) before the new-formula go-live ({summary['settlement']['go_live_utc']},"
        " planned, subject to CREG approval), new formula (ODS212 price) from it.",
        f"- Models: deployment forecasters fitted on {str(fold.get('train_start', '?'))[:10]} to "
        f"{str(fold.get('train_end', '?'))[:10]} (models id `{models.get('reference_models_id')}`, "
        "`models/reference_manifest.json`); a run with different models is refused unless explicitly allowed.",
        "- Data: Elia's historical datasets as first published (most quarter-hours are not yet validated at that "
        "point; recorded days are not restated). Days after the committed `data/public` set are downloaded into "
        "the gitignored `data/raw/track_record` cache.",
        "",
        "Strategies:",
        "",
    ]
    for entry in strategies.values():
        lines.append(f"- {entry['label']} (backtest `{entry['backtest_name']}`): {entry['description']}.")
    lines += [
        "",
        "Files: `daily_pnl.csv` (one row per day and strategy), `qh/<YYYY-MM>.csv` (quarter-hour ledger),",
        "`summary.json` (machine-readable summary). Update with `python scripts/ops/track_record.py update`; see",
        "`docs/track_record.md`.",
        "",
        "Data: " + "; ".join(summary["attribution"]) + ".",
        "",
    ]
    return "\n".join(lines)


def render_chart(daily: pd.DataFrame, strategies: Mapping[str, StrategySpec], path: Path) -> bool:
    """Cumulative paper P&L per strategy and day (light surface; colours follow the strategy, as in the reports)."""
    if daily.empty:
        return False
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter, MaxNLocator

    names = [n for n in strategies if n in set(daily["strategy"])]
    if not names:
        return False

    surface, ink, ink2, muted, grid, axis = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
    days = pd.to_datetime(sorted(set(daily["local_date"])))
    with plt.rc_context({"font.family": "sans-serif", "font.sans-serif": ["Helvetica", "Arial", "DejaVu Sans"]}):
        fig, ax = plt.subplots(figsize=(8.0, 3.6), dpi=150)
        fig.patch.set_facecolor(surface)
        ax.set_facecolor(surface)
        ends: list[tuple[float, str, pd.Timestamp]] = []
        for name in names:
            rows = daily[daily["strategy"] == name].sort_values("local_date")
            dates = pd.to_datetime(rows["local_date"])
            # cumulative P&L at the end of each day, anchored at zero at the end of the day before the first one
            x = pd.Series([dates.iloc[0] - pd.Timedelta(days=1), *dates])
            y = np.concatenate([[0.0], rows["cumulative_pnl_eur"].to_numpy(dtype=float)])
            few = len(rows) <= 31
            ax.plot(
                x,
                y,
                color=COLORS.get(name, "#2a78d6"),
                linewidth=2,
                solid_capstyle="round",
                solid_joinstyle="round",
                marker="o" if few else None,
                markersize=6,
                markeredgecolor=surface,
                markeredgewidth=2,
                label=strategies[name].label,
                zorder=3,
            )
            if not few:
                ax.plot(
                    x.iloc[-1:],
                    y[-1:],
                    "o",
                    color=COLORS.get(name, "#2a78d6"),
                    markersize=6,
                    markeredgecolor=surface,
                    markeredgewidth=2,
                    zorder=4,
                )
            ends.append((float(y[-1]), strategies[name].label, x.iloc[-1]))
        ax.axhline(0.0, color=axis, linewidth=1, zorder=1)
        ax.grid(axis="y", color=grid, linewidth=1)
        ax.grid(axis="x", visible=False)
        for side in ("top", "right", "left"):
            ax.spines[side].set_visible(False)
        ax.spines["bottom"].set_color(axis)
        ax.tick_params(colors=muted, labelsize=8, length=0)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1000:,.1f}K" if abs(v) >= 1000 else f"{v:,.0f}"))
        ax.xaxis.set_major_locator(MaxNLocator(nbins=6, integer=True))
        fig.autofmt_xdate(rotation=0, ha="center")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
        span = max((abs(v) for v, _, _ in ends), default=1.0) or 1.0
        values = sorted(v for v, _, _ in ends)
        separated = all(b - a > 0.08 * span for a, b in zip(values, values[1:], strict=False))
        if separated:  # direct end labels only when they cannot collide (the legend always carries identity)
            for value, label, x_end in ends:
                ax.annotate(
                    f"{label}: {value:,.0f}",
                    (x_end, value),
                    xytext=(8, 0),
                    textcoords="offset points",
                    va="center",
                    fontsize=8,
                    color=ink2,
                )
        ax.set_ylabel("EUR (cumulative)", color=ink2, fontsize=8)
        ax.set_title(
            "Cumulative paper P&L, 1 MW / 2 MWh, forward days after the committed data set",
            loc="left",
            fontsize=10,
            color=ink,
        )
        if len(names) > 1:  # a single series is named by its end label
            legend = ax.legend(loc="upper left", frameon=False, fontsize=8, ncol=len(names))
            for text in legend.get_texts():
                text.set_color(ink2)
        if len(days) <= 2:
            ax.set_xlim(days[0] - pd.Timedelta(days=1.5), days[-1] + pd.Timedelta(days=1.5))
        ax.margins(y=0.25)
        fig.tight_layout()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp.png")
        fig.savefig(tmp, facecolor=surface, metadata={"Software": None})
        plt.close(fig)
    os.replace(tmp, path)
    return True


def render(cfg: TrackConfig, *, context: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Regenerate ``summary.json``, ``README.md`` and the chart from the CSV files (deterministic)."""
    store = TrackStore(cfg.out_dir)
    daily = store.read_daily()
    previous = cfg.out_dir / SUMMARY_JSON
    if context is None and previous.is_file():
        context = json.loads(previous.read_text(encoding="utf-8")).get("context", {})
    summary = build_summary(cfg, daily, context or {})
    write_text(cfg.out_dir / SUMMARY_JSON, json.dumps(summary, indent=1, sort_keys=False, allow_nan=False) + "\n")
    write_text(cfg.out_dir / README_MD, render_readme(summary, daily))
    render_chart(daily, {n: STRATEGIES[n] for n in cfg.strategies}, cfg.out_dir / CHART_PNG)
    return summary


# ----------------------------------------------------------------------------------------------------------------
# CLI


def _config(args: argparse.Namespace) -> TrackConfig:
    models_dir = Path(args.models_dir).expanduser() if args.models_dir else default_models_dir()
    strategies = tuple(s.strip() for s in args.strategies.split(",") if s.strip())
    unknown = [s for s in strategies if s not in STRATEGIES]
    if unknown:
        raise SystemExit(f"unknown strategies {unknown}; known: {sorted(STRATEGIES)}")
    return TrackConfig(
        out_dir=Path(args.out),
        track_start=date.fromisoformat(args.track_start),
        warmup_days=int(args.warmup_days),
        strategies=strategies,
        models_dir=models_dir,
        allow_model_change=bool(getattr(args, "allow_model_change", False)),
        max_wait_days=float(getattr(args, "max_wait_days", MAX_WAIT_DAYS)),
        go_live_utc=to_utc(args.go_live),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("update", "render", "verify"):
        p = sub.add_parser(name)
        p.add_argument("--out", default=str(DEFAULT_OUT), help="output directory (default reports/track_record)")
        p.add_argument("--strategies", default=",".join(STRATEGIES))
        p.add_argument("--models-dir", default=None, help="deployment models (default $EUROFLEX_MODELS_DIR)")
        p.add_argument("--go-live", default=NEW_FORMULA_GO_LIVE_UTC.isoformat(), help="new-formula go-live (UTC)")
        p.add_argument("--track-start", default=TRACK_START.isoformat(), help="first delivery day of the record")
        p.add_argument("--warmup-days", type=int, default=WARMUP_DAYS, help="history window before each day")
        if name in ("update", "verify"):
            p.add_argument("--raw-dir", default=None, help="raw download cache (default data/raw/track_record)")
            p.add_argument("--offline", action="store_true", help="use cached data only (no network)")
            p.add_argument("--refresh", action="store_true", help="re-download cached chunks of open periods")
        if name == "update":
            p.add_argument("--until", default=None, help="last delivery day to record (YYYY-MM-DD)")
            p.add_argument("--now", default=None, help="evaluation time (UTC; default now)")
            p.add_argument("--max-wait-days", type=float, default=MAX_WAIT_DAYS)
            p.add_argument("--allow-model-change", action="store_true")
            p.add_argument("--dry-run", action="store_true", help="simulate but write nothing")
        if name == "verify":
            p.add_argument("--day", action="append", default=None, help="only this day (repeatable)")
    args = parser.parse_args(argv)
    cfg = _config(args)
    if args.command == "render":
        summary = render(cfg)
        print(json.dumps({n: {k: s.get(k) for k in ("days", "pnl_eur")} for n, s in summary["strategies"].items()}))
        return 0
    raw_dir = Path(args.raw_dir) if args.raw_dir else public_data.raw_root() / "track_record"
    source = PublicForwardSource(
        raw_dir=raw_dir, offline=args.offline, refresh=args.refresh, go_live_utc=cfg.go_live_utc
    )
    if args.command == "verify":
        results = verify(cfg, source, days=[date.fromisoformat(d) for d in args.day] if args.day else None)
        print(json.dumps(results, indent=1))
        worst = max((r.get("abs_diff_eur", 0.0) for r in results), default=0.0)
        return 0 if worst < 0.01 and not any("error" in r for r in results) else 1
    now = to_utc(args.now) if args.now else pd.Timestamp.now(tz="UTC")
    until = date.fromisoformat(args.until) if args.until else None
    report = update(cfg, source, now=now, until=until, dry_run=args.dry_run)
    print(json.dumps(report.to_json(), indent=1, default=str))
    return 1 if report.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
