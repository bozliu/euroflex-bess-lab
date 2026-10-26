"""ECB euro foreign exchange reference rates (GBP per EUR, daily, published around 16:00 CET on TARGET days)."""

from __future__ import annotations

from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd

from ..http import FetchError, FetchRecord, HttpClient, cached_download
from .sources import ECB_EXR_URL


def validate_ecb_csv(path: Path) -> None:
    head = path.read_text(encoding="utf-8", errors="replace")[:2000]
    if "TIME_PERIOD" not in head or "OBS_VALUE" not in head:
        raise FetchError(f"{path.name} is not an ECB SDMX CSV export")


def fetch_ecb_gbp(
    client: HttpClient,
    raw_dir: Path,
    *,
    start: str = "2023-12-01",
    now: pd.Timestamp,
    refresh: bool = False,
    offline: bool = False,
) -> tuple[Path, FetchRecord]:
    dest = raw_dir / "gb" / "ecb" / "exr_d_gbp_eur.csv"
    record = cached_download(
        client,
        ECB_EXR_URL,
        dest,
        params={"startPeriod": start, "format": "csvdata"},
        validate=validate_ecb_csv,
        refresh=refresh,
        period_end_utc=now,
        offline=offline,
    )
    return dest, record


def parse_ecb_csv(text: str) -> pd.DataFrame:
    """``timestamp_utc`` (reference date 00:00 UTC) and ``gbp_per_eur`` (float64)."""

    raw = pd.read_csv(StringIO(text), usecols=["TIME_PERIOD", "OBS_VALUE"])
    frame = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(raw["TIME_PERIOD"]).dt.tz_localize("UTC"),
            "gbp_per_eur": pd.to_numeric(raw["OBS_VALUE"], errors="coerce"),
        }
    ).dropna()
    frame = frame.drop_duplicates("timestamp_utc").sort_values("timestamp_utc").reset_index(drop=True)
    return frame


def gbp_per_eur_on(dates: pd.DatetimeIndex | pd.Series, fx: pd.DataFrame) -> np.ndarray:
    """As-of join: the latest ECB reference rate dated on or before each UTC date (forward-filled over
    weekends and TARGET holidays; the first published rate is back-filled for dates before it)."""

    if fx.empty:
        raise ValueError("empty FX table")
    stamps = pd.DatetimeIndex(pd.to_datetime(dates, utc=True)).floor("D")
    ref = pd.DatetimeIndex(pd.to_datetime(fx["timestamp_utc"], utc=True))
    rates = fx["gbp_per_eur"].to_numpy(dtype=float)
    pos = ref.searchsorted(stamps, side="right") - 1
    pos = np.clip(pos, 0, len(rates) - 1)
    return rates[pos]
