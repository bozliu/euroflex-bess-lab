"""Loaders for the normalized GB tables (half-open ``[start, end)`` bounds; naive values are UTC).

Missing files raise ``FileNotFoundError`` naming the fetch command that creates them. The data root is
:func:`euroflex_bess_lab.public_data.paths.data_root` (``$EUROFLEX_DATA_DIR`` or ``data/public``) unless ``root``
is given.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from ..paths import data_root
from .tables import GB_TABLES, GBTableSpec
from .timeutils import to_utc

type TimeLike = str | datetime | pd.Timestamp


def _bounds(start: TimeLike, end: TimeLike) -> tuple[pd.Timestamp, pd.Timestamp]:
    lo, hi = to_utc(start), to_utc(end)
    if hi <= lo:
        raise ValueError(f"`end` ({hi}) must be after `start` ({lo}); intervals are half-open [start, end)")
    return lo, hi


def gb_table_path(key: str, year: int | None = None, root: Path | None = None) -> Path:
    spec = GB_TABLES[key]
    return (root or data_root()) / spec.relpath(year)


def _missing(spec: GBTableSpec, path: Path) -> FileNotFoundError:
    return FileNotFoundError(f"GB public data file not found: {path}. Create it with: {spec.fetch_command()}")


def _read(
    path: Path, columns: Sequence[str] | None, time_column: str, lo: pd.Timestamp, hi: pd.Timestamp
) -> pd.DataFrame:
    names = pq.read_schema(path).names
    selected = None
    if columns is not None:
        unknown = [c for c in columns if c not in names]
        if unknown:
            raise KeyError(f"Unknown columns {unknown} for {path.name}; available: {names}")
        selected = [time_column, *[c for c in columns if c != time_column]]
    table = pq.read_table(path, columns=selected, filters=[(time_column, ">=", lo), (time_column, "<", hi)])
    frame = table.to_pandas()
    for column in frame.columns:
        if column.endswith("_utc"):
            frame[column] = pd.to_datetime(frame[column], utc=True).astype("datetime64[ns, UTC]")
    return frame


def _load(
    key: str,
    start: TimeLike,
    end: TimeLike,
    columns: Sequence[str] | None = None,
    *,
    root: Path | None = None,
    sort_by: Sequence[str] | None = None,
) -> pd.DataFrame:
    spec = GB_TABLES[key]
    lo, hi = _bounds(start, end)
    frames: list[pd.DataFrame] = []
    if spec.partitioned:
        years = range(max(lo.year, spec.first_year), (hi - pd.Timedelta(microseconds=1)).year + 1)
        for year in years:
            path = gb_table_path(key, year, root)
            if not path.exists():
                if year > pd.Timestamp.now(tz="UTC").year:
                    continue
                raise _missing(spec, path)
            frames.append(_read(path, columns, spec.time_column, lo, hi))
    else:
        path = gb_table_path(key, None, root)
        if not path.exists():
            raise _missing(spec, path)
        frames.append(_read(path, columns, spec.time_column, lo, hi))
    if not frames:
        return pd.DataFrame({spec.time_column: pd.Series(dtype="datetime64[ns, UTC]")})
    frame = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    order = [c for c in (sort_by or [spec.time_column]) if c in frame.columns]
    return frame.sort_values(order, kind="stable").reset_index(drop=True)


def load_gb_system_prices(
    start: TimeLike, end: TimeLike, columns: Sequence[str] | None = None, *, root: Path | None = None
) -> pd.DataFrame:
    """Single cash-out price, NIV and price-stack summary per settlement period (BMRS DISEBSP)."""

    return _load("gb_system_prices", start, end, columns, root=root)


def load_gb_market_index(
    start: TimeLike, end: TimeLike, columns: Sequence[str] | None = None, *, root: Path | None = None
) -> pd.DataFrame:
    """Market Index Data per settlement period (BMRS MID); ``mid_price_gbp_mwh`` is NaN when nothing traded."""

    return _load("gb_market_index", start, end, columns, root=root)


def load_gb_response_auctions(
    start: TimeLike,
    end: TimeLike,
    products: Sequence[str] | None = None,
    *,
    root: Path | None = None,
) -> pd.DataFrame:
    """EAC clearing prices for DC/DM/DR low/high per EFA block (rows whose delivery starts in ``[start, end)``)."""

    frame = _load("gb_response_auctions", start, end, root=root, sort_by=["delivery_start_utc", "product"])
    if products is not None:
        frame = frame[frame["product"].isin([p.upper() for p in products])].reset_index(drop=True)
    return frame


def load_gb_reserve_auctions(
    start: TimeLike,
    end: TimeLike,
    products: Sequence[str] | None = None,
    *,
    root: Path | None = None,
) -> pd.DataFrame:
    """EAC clearing prices for Quick/Slow/Balancing Reserve (context data)."""

    frame = _load("gb_reserve_auctions", start, end, root=root, sort_by=["delivery_start_utc", "product"])
    if products is not None:
        frame = frame[frame["product"].isin([p.upper() for p in products])].reset_index(drop=True)
    return frame


def load_gb_frequency(start: TimeLike, end: TimeLike, *, root: Path | None = None) -> pd.DataFrame:
    """15-second GB system frequency; adds ``frequency_hz`` (float64) next to ``frequency_dev_mhz``."""

    frame = _load("gb_frequency", start, end, root=root)
    if "frequency_dev_mhz" in frame.columns:
        frame["frequency_hz"] = 50.0 + frame["frequency_dev_mhz"].astype(float) / 1000.0
    return frame


def load_gb_response_energy(
    start: TimeLike, end: TimeLike, columns: Sequence[str] | None = None, *, root: Path | None = None
) -> pd.DataFrame:
    """MWh delivered per MW of contracted DC/DM/DR (low/high) per settlement period, from BMRS frequency."""

    return _load("gb_response_energy", start, end, columns, root=root)


def load_gb_fx(start: TimeLike, end: TimeLike, *, root: Path | None = None) -> pd.DataFrame:
    """ECB GBP-per-EUR reference rates with reference dates in ``[start, end)``."""

    return _load("gb_fx", start, end, root=root)
