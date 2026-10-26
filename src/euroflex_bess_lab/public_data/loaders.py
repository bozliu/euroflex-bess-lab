"""Loader API for the normalized public tables (``euroflex_bess_lab.public_data``).

All loaders take half-open ``[start, end)`` bounds (``str``, ``datetime`` or ``pd.Timestamp``; naive values are
UTC), return frames sorted by their time column with tz-aware ``datetime64[ns, UTC]`` timestamps, and raise
``FileNotFoundError`` naming the ``euroflex data fetch ...`` command when a needed file is missing.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from .paths import data_root, raw_root
from .registry import MARI_GO_LIVE_UTC, TABLES, TableSpec
from .writers import MANIFEST_NAME

type TimeLike = str | datetime | pd.Timestamp


def _utc(value: TimeLike) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _bounds(start: TimeLike, end: TimeLike) -> tuple[pd.Timestamp, pd.Timestamp]:
    lo, hi = _utc(start), _utc(end)
    if hi <= lo:
        raise ValueError(f"`end` ({hi}) must be after `start` ({lo}); intervals are half-open [start, end)")
    return lo, hi


def _missing(spec: TableSpec, path: Path, year: int | None = None) -> FileNotFoundError:
    hint = spec.fetch_command(year)
    if not spec.redistributed:
        hint += "  (this source is not redistributed; the file is built locally under data/raw/)"
    return FileNotFoundError(f"Public data file not found: {path}. Create it with: {hint}")


def _resolve(spec: TableSpec, year: int | None, root: Path | None) -> Path:
    base = root or data_root()
    path = base / spec.relpath(year)
    if path.exists() or spec.redistributed:
        return path
    fallback = raw_root() / spec.relpath(year)
    return fallback if fallback.exists() else path


def _read(
    path: Path, columns: Sequence[str] | None, time_column: str, lo: pd.Timestamp, hi: pd.Timestamp
) -> pd.DataFrame:
    schema_names = pq.read_schema(path).names
    selected = None
    if columns is not None:
        unknown = [c for c in columns if c not in schema_names]
        if unknown:
            raise KeyError(f"Unknown columns {unknown} for {path.name}; available: {schema_names}")
        selected = [time_column, *[c for c in columns if c != time_column]]
    table = pq.read_table(path, columns=selected, filters=[(time_column, ">=", lo), (time_column, "<", hi)])
    return table.to_pandas()


def _finalize(frame: pd.DataFrame, sort_by: Sequence[str]) -> pd.DataFrame:
    for column in frame.columns:
        if column.endswith("_utc"):
            frame[column] = pd.to_datetime(frame[column], utc=True).astype("datetime64[ns, UTC]")
    present = [c for c in sort_by if c in frame.columns]
    if present:
        frame = frame.sort_values(present, kind="stable")
    return frame.reset_index(drop=True)


def _load(
    key: str,
    start: TimeLike,
    end: TimeLike,
    columns: Sequence[str] | None = None,
    *,
    time_column: str | None = None,
    sort_by: Sequence[str] | None = None,
    root: Path | None = None,
) -> pd.DataFrame:
    spec = TABLES[key]
    lo, hi = _bounds(start, end)
    column = time_column or spec.time_column
    frames: list[pd.DataFrame] = []
    if spec.partition == "year":
        last_year = pd.Timestamp.now(tz="UTC").year
        first_year = spec.first_year or lo.year
        for year in range(lo.year, (hi - pd.Timedelta(microseconds=1)).year + 1):
            if year < first_year or year > last_year:
                continue
            path = _resolve(spec, year, root)
            if not path.exists():
                raise _missing(spec, path, year)
            frames.append(_read(path, columns, column, lo, hi))
    else:
        path = _resolve(spec, None, root)
        if not path.exists():
            raise _missing(spec, path)
        frames.append(_read(path, columns, column, lo, hi))
    if not frames:
        return _finalize(_empty_like(spec, root, columns, column), sort_by or [column])
    frame = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    return _finalize(frame, sort_by or [column])


def _empty_like(spec: TableSpec, root: Path | None, columns: Sequence[str] | None, time_column: str) -> pd.DataFrame:
    """Empty frame with the table's schema (taken from any existing partition) for out-of-coverage requests."""

    base = root or data_root()
    pattern = spec.pattern.replace("{year}", "[0-9][0-9][0-9][0-9]")
    candidates = sorted(base.glob(pattern)) or (sorted(raw_root().glob(pattern)) if not spec.redistributed else [])
    if not candidates:
        names = [time_column, *[c for c in (columns or []) if c != time_column]]
        return pd.DataFrame(
            {name: pd.Series(dtype="datetime64[ns, UTC]" if name.endswith("_utc") else "float32") for name in names}
        )
    schema = pq.read_schema(candidates[0])
    selected = schema.names if columns is None else [time_column, *[c for c in columns if c != time_column]]
    empty = schema.empty_table().select(selected)
    return empty.to_pandas()


def load_belgium_minute(start: TimeLike, end: TimeLike, columns: Sequence[str] | None = None) -> pd.DataFrame:
    """Per-minute Belgian table (ODS133 cumulative QH-to-date + ODS126 instantaneous + ODS165 components)."""

    return _load("belgium_minute", start, end, columns)


def load_belgium_qh(start: TimeLike, end: TimeLike, columns: Sequence[str] | None = None) -> pd.DataFrame:
    """Per-quarter-hour Belgian table (ODS134 prices + ODS166 components + ODS127 volumes)."""

    return _load("belgium_qh", start, end, columns)


def load_belgium_new_formula_minute(
    start: TimeLike = "2026-01-01", end: TimeLike = "2026-08-01", columns: Sequence[str] | None = None
) -> pd.DataFrame:
    """Elia's simulated new-formula (2026) imbalance prices per minute (ODS211, Jan-Jul 2026)."""

    return _load("belgium_new_formula_minute", start, end, columns)


def load_belgium_new_formula_qh(
    start: TimeLike = "2026-01-01", end: TimeLike = "2026-08-01", columns: Sequence[str] | None = None
) -> pd.DataFrame:
    """Elia's simulated new-formula (2026) imbalance prices per quarter-hour (ODS212, Jan-Jul 2026)."""

    return _load("belgium_new_formula_qh", start, end, columns)


_SI_KINDS = {
    "current": "belgium_si_forecast_current",
    "current_qh": "belgium_si_forecast_current",
    "ods136": "belgium_si_forecast_current",
    "next": "belgium_si_forecast_next",
    "next_qh": "belgium_si_forecast_next",
    "ods147": "belgium_si_forecast_next",
}


def load_belgium_si_forecast(
    kind: str,
    start: TimeLike,
    end: TimeLike,
    columns: Sequence[str] | None = None,
    *,
    on: str = "prediction_time_utc",
) -> pd.DataFrame:
    """Elia SI forecasts: ``kind="current"`` (ODS136) or ``"next"`` (ODS147).

    Rows are selected on ``on`` (default ``prediction_time_utc`` = when the forecast was issued, which is what a
    no-look-ahead strategy should filter on; pass ``on="target_qh_utc"`` to select by target quarter-hour).
    """

    key = _SI_KINDS.get(kind.lower())
    if key is None:
        raise ValueError(f"Unknown SI forecast kind {kind!r}; expected 'current' or 'next'")
    if on not in {"prediction_time_utc", "target_qh_utc"}:
        raise ValueError("`on` must be 'prediction_time_utc' or 'target_qh_utc'")
    if on == "target_qh_utc":
        lo, hi = _bounds(start, end)
        # Forecasts for a target quarter-hour are issued at most 15 minutes before it starts (ODS147) or during
        # it (ODS136); read a slightly wider window of prediction times, then select on the target.
        wide = _load(key, lo - pd.Timedelta(hours=1), hi, columns=None)
        frame = wide.loc[(wide["target_qh_utc"] >= lo) & (wide["target_qh_utc"] < hi)]
        if columns is not None:
            keys = ["prediction_time_utc", "target_qh_utc"]
            frame = frame[[*keys, *[c for c in columns if c not in keys]]]
        return frame.sort_values(["target_qh_utc", "prediction_time_utc"], kind="stable").reset_index(drop=True)
    frame = _load(key, start, end, columns, sort_by=["prediction_time_utc", "target_qh_utc"])
    return frame


_ZONES = {"BE": "da_prices_BE", "DE-LU": "da_prices_DE-LU", "DE_LU": "da_prices_DE-LU", "DE": "da_prices_DE-LU"}


def load_day_ahead_prices(zone: str, start: TimeLike, end: TimeLike, *, quarter_hourly: bool = False) -> pd.DataFrame:
    """Day-ahead prices for ``zone`` in ``{"BE", "DE-LU"}``: ``timestamp_utc``, ``resolution_minutes``,
    ``price_eur_mwh`` (hourly products until 2025-09-30, 15-minute products afterwards).

    ``quarter_hourly=True`` expands every product onto the 15-minute grid (an hourly price is repeated for its
    four quarter-hours; ``resolution_minutes`` keeps the product length) and trims to ``[start, end)``.
    """

    key = _ZONES.get(zone.upper())
    if key is None:
        raise ValueError(f"Unknown bidding zone {zone!r}; expected 'BE' or 'DE-LU'")
    if not quarter_hourly:
        return _load(key, start, end)
    lo, hi = _bounds(start, end)
    frame = _load(key, lo - pd.Timedelta(hours=1), hi)
    if frame.empty:
        return frame
    repeats = (frame["resolution_minutes"].astype("int64") // 15).clip(lower=1).to_numpy()
    expanded = frame.loc[frame.index.repeat(repeats)].reset_index(drop=True)
    offsets = expanded.groupby(frame.index.repeat(repeats)).cumcount().to_numpy() * pd.Timedelta(minutes=15)
    expanded["timestamp_utc"] = expanded["timestamp_utc"] + offsets
    mask = (expanded["timestamp_utc"] >= lo) & (expanded["timestamp_utc"] < hi)
    return expanded.loc[mask].reset_index(drop=True)


_CAPACITY = {"afrr": "belgium_afrr_capacity_awards", "mfrr": "belgium_mfrr_capacity_awards"}


def load_capacity_awards(product: str, start: TimeLike, end: TimeLike) -> pd.DataFrame:
    """Belgian aFRR / mFRR capacity awards per delivery block and direction (rows with delivery start in range)."""

    key = _CAPACITY.get(product.lower())
    if key is None:
        raise ValueError(f"Unknown capacity product {product!r}; expected 'afrr' or 'mfrr'")
    return _load(key, start, end, sort_by=["delivery_start_utc", "direction"])


def load_res_load(start: TimeLike, end: TimeLike, columns: Sequence[str] | None = None) -> pd.DataFrame:
    """Belgian load / wind / PV measured and forecast per quarter-hour (ODS001, ODS031, ODS032)."""

    return _load("belgium_res_load", start, end, columns)


def load_energy_bid_curves(start: TimeLike, end: TimeLike, direction: str | None = None) -> pd.DataFrame:
    """Merit-order curves (``timestamp_utc``, ``direction``, ``cum_mw``, ``price_eur_mwh``) per quarter-hour."""

    frame = _load("belgium_energy_bid_curves", start, end, sort_by=["timestamp_utc", "direction", "cum_mw"])
    if direction is not None:
        if direction not in {"up", "down"}:
            raise ValueError("`direction` must be 'up' or 'down'")
        frame = frame[frame["direction"] == direction].reset_index(drop=True)
    return frame


_GERMANY = {"fcr": "germany_fcr_capacity", "afrr": "germany_afrr_capacity"}


def load_germany_reserve(product: str, start: TimeLike, end: TimeLike) -> pd.DataFrame:
    """German FCR / aFRR capacity auction results (regelleistung.net; built locally, not redistributed)."""

    key = _GERMANY.get(product.lower())
    if key is None:
        raise ValueError(f"Unknown German reserve product {product!r}; expected 'fcr' or 'afrr'")
    return _load(key, start, end, sort_by=["delivery_start_utc", "direction"])


def load_belgium_legacy(
    kind: str, start: TimeLike, end: TimeLike, columns: Sequence[str] | None = None
) -> pd.DataFrame:
    """Pre-MARI history: ``kind="qh"`` (ODS047, 2019 to 2024-05-21) or ``"minute"`` (ODS046, 2023 to 2024-05-21)."""

    key = {"qh": "belgium_qh_legacy", "minute": "belgium_minute_legacy"}.get(kind.lower())
    if key is None:
        raise ValueError(f"Unknown legacy kind {kind!r}; expected 'qh' or 'minute'")
    lo, hi = _bounds(start, end)
    hi = min(hi, MARI_GO_LIVE_UTC)
    if hi <= lo:
        spec = TABLES[key]
        return _finalize(_empty_like(spec, None, columns, spec.time_column), [spec.time_column])
    return _load(key, lo, hi, columns)


def read_manifest(root: Path | None = None) -> dict[str, Any]:
    """Return ``MANIFEST.json`` of the data root as a dict."""

    base = root or data_root()
    path = base / MANIFEST_NAME
    if not path.exists():
        raise FileNotFoundError(f"No {MANIFEST_NAME} under {base}. Create it with: euroflex data manifest")
    payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return payload


def table_path(key: str, year: int | None = None, root: Path | None = None) -> Path:
    """Path of a registered table (for scripts that want to read parquet directly)."""

    return _resolve(TABLES[key], year, root)
