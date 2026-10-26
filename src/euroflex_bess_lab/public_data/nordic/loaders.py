"""Loaders for the Nordic tables (``data/public/nordic``, override the root with ``EUROFLEX_DATA_DIR``).

All loaders take half-open ``[start, end)`` bounds (``str``, ``datetime`` or ``pd.Timestamp``; naive values are UTC),
return frames sorted by time with tz-aware ``datetime64[ns, UTC]`` columns, and raise ``FileNotFoundError`` naming
the command that builds a missing file.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from ..paths import data_root
from .build import RELPATHS, day_ahead_relpath
from .sources import AFRR_ZONES, DA_ZONES, DK_AREAS, FCR_NORDIC_ZONES, MFRR_ZONES, fetch_hint

type TimeLike = str | datetime | pd.Timestamp

RESERVE_PRODUCTS = ("fcr_n", "fcr_d_up", "fcr_d_down", "fcr", "afrr", "mfrr")
FCR_AUCTIONS = ("early", "late", "total")


def _utc(value: TimeLike) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _bounds(start: TimeLike, end: TimeLike) -> tuple[pd.Timestamp, pd.Timestamp]:
    lo, hi = _utc(start), _utc(end)
    if hi <= lo:
        raise ValueError(f"`end` ({hi}) must be after `start` ({lo}); intervals are half-open [start, end)")
    return lo, hi


def nordic_table_path(table: str, zone: str | None = None, root: Path | None = None) -> Path:
    """Path of a Nordic table (``day_ahead`` needs ``zone``)."""

    base = root or data_root()
    if table == "day_ahead":
        if zone is None:
            raise ValueError("day_ahead needs a zone")
        return base / day_ahead_relpath(zone)
    if table not in RELPATHS:
        raise KeyError(f"Unknown Nordic table {table!r}; expected day_ahead or one of {sorted(RELPATHS)}")
    return base / RELPATHS[table]


def _read(
    path: Path,
    time_column: str,
    lo: pd.Timestamp,
    hi: pd.Timestamp,
    filters: Sequence[tuple[str, str, Any]] = (),
    *,
    table: str,
) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Nordic public data file not found: {path}. Create it with: {fetch_hint(table)}")
    predicate = [(time_column, ">=", lo), (time_column, "<", hi), *filters]
    frame = pq.read_table(path, filters=predicate).to_pandas()
    for column in frame.columns:
        if column.endswith("_utc"):
            frame[column] = pd.to_datetime(frame[column], utc=True).astype("datetime64[ns, UTC]")
    return frame


def load_nordic_day_ahead(
    zone: str, start: TimeLike, end: TimeLike, *, quarter_hourly: bool = False, root: Path | None = None
) -> pd.DataFrame:
    """Day-ahead prices for ``zone`` in ``DK1, DK2, SE3, SE4, FI``: ``timestamp_utc``, ``resolution_minutes``,
    ``price_eur_mwh`` (hourly products until 2025-09-30, 15-minute products afterwards).

    ``quarter_hourly=True`` repeats each hourly price on its four quarter-hours (``resolution_minutes`` keeps the
    product length) and trims to ``[start, end)``.
    """

    zone = zone.upper()
    if zone not in DA_ZONES:
        raise ValueError(f"Unknown Nordic bidding zone {zone!r}; expected one of {DA_ZONES}")
    lo, hi = _bounds(start, end)
    path = nordic_table_path("day_ahead", zone, root)
    frame = _read(path, "timestamp_utc", lo - pd.Timedelta(hours=1) if quarter_hourly else lo, hi, table="day_ahead")
    frame = frame.sort_values("timestamp_utc").reset_index(drop=True)
    if not quarter_hourly or frame.empty:
        return frame.loc[frame["timestamp_utc"] >= lo].reset_index(drop=True)
    repeats = (frame["resolution_minutes"].astype("int64") // 15).clip(lower=1).to_numpy()
    expanded = frame.loc[frame.index.repeat(repeats)].reset_index(drop=True)
    offsets = expanded.groupby(frame.index.repeat(repeats)).cumcount().to_numpy() * pd.Timedelta(minutes=15)
    expanded["timestamp_utc"] = expanded["timestamp_utc"] + offsets
    mask = (expanded["timestamp_utc"] >= lo) & (expanded["timestamp_utc"] < hi)
    return expanded.loc[mask].reset_index(drop=True)


def load_nordic_reserve(
    product: str,
    zone: str,
    start: TimeLike,
    end: TimeLike,
    *,
    auction: str | None = "early",
    direction: str | None = None,
    root: Path | None = None,
) -> pd.DataFrame:
    """Capacity prices per delivery hour (``delivery_start_utc``, ``delivery_end_utc``, ``zone``, ``product``,
    ``direction``, ``price_eur_per_mw_h`` plus volume columns).

    Products: ``fcr_n``, ``fcr_d_up``, ``fcr_d_down`` (joint SE-DK2 auctions; zones DK2, SE3, SE4; ``auction`` in
    ``early`` (default), ``late``, ``total`` or ``None`` for all), ``fcr`` (DK1, Continental Europe FCR),
    ``afrr`` (DK1, DK2, FI, SE3, SE4) and ``mfrr`` (DK1, DK2, SE3, SE4); ``direction`` filters ``up``/``down``.
    """

    product = product.lower()
    zone = zone.upper()
    if product not in RESERVE_PRODUCTS:
        raise ValueError(f"Unknown Nordic reserve product {product!r}; expected one of {RESERVE_PRODUCTS}")
    lo, hi = _bounds(start, end)
    filters: list[tuple[str, str, Any]] = [("zone", "==", zone)]
    if product in {"fcr_n", "fcr_d_up", "fcr_d_down"}:
        if zone not in FCR_NORDIC_ZONES:
            raise ValueError(f"{product} is published for {FCR_NORDIC_ZONES}, not {zone!r}")
        if auction is not None:
            if auction not in FCR_AUCTIONS:
                raise ValueError(f"`auction` must be one of {FCR_AUCTIONS} or None")
            filters.append(("auction", "==", auction))
        filters.append(("product", "==", product))
        table = "fcr_nordic"
    elif product == "fcr":
        if zone != "DK1":
            raise ValueError(
                "product 'fcr' (Continental Europe FCR) is only published for DK1; use fcr_n/fcr_d_up/fcr_d_down"
            )
        table = "fcr_dk1"
    elif product == "afrr":
        if zone not in AFRR_ZONES:
            raise ValueError(f"aFRR is stored for {AFRR_ZONES}, not {zone!r}")
        table = "afrr"
    else:
        if zone not in MFRR_ZONES:
            raise ValueError(f"mFRR is stored for {MFRR_ZONES}, not {zone!r}")
        table = "mfrr"
    if direction is not None:
        if direction not in {"up", "down", "symmetric"}:
            raise ValueError("`direction` must be 'up', 'down' or 'symmetric'")
        filters.append(("direction", "==", direction))
    frame = _read(nordic_table_path(table, root=root), "delivery_start_utc", lo, hi, filters, table=table)
    sort = [c for c in ("delivery_start_utc", "direction", "auction") if c in frame.columns]
    return frame.sort_values(sort, kind="stable").reset_index(drop=True)


def load_dk_imbalance(area: str, start: TimeLike, end: TimeLike, *, root: Path | None = None) -> pd.DataFrame:
    """Danish single imbalance price and components for ``area`` in ``DK1``/``DK2`` (hourly before the
    2025-03-04 mFRR EAM go-live, 15-minute afterwards; see ``resolution_minutes``)."""

    area = area.upper()
    if area not in DK_AREAS:
        raise ValueError(f"Unknown Danish area {area!r}; expected one of {DK_AREAS}")
    lo, hi = _bounds(start, end)
    frame = _read(
        nordic_table_path("dk_imbalance", root=root),
        "timestamp_utc",
        lo,
        hi,
        [("area", "==", area)],
        table="dk_imbalance",
    )
    return frame.sort_values("timestamp_utc", kind="stable").reset_index(drop=True)
