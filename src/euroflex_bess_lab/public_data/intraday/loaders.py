"""Loader for the quarter-hourly intraday price index (``load_intraday_index``).

The ID AEP is not redistributed, so the file normally lives under ``data/raw/intraday/`` (built locally with
``python -m euroflex_bess_lab.public_data.intraday fetch``). A copy under ``data/public/intraday/`` (e.g. a
licensed in-house tree pointed to by ``EUROFLEX_DATA_DIR``) takes precedence when present.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from ..paths import data_root, raw_root
from .build import FETCH_COMMAND, ID_AEP_RELPATH
from .netztransparenz import ID_AEP_COLUMN, ID_AEP_ZONE

type TimeLike = str | datetime | pd.Timestamp

_SOURCES = {"id_aep": ID_AEP_RELPATH}
_ZONES = {"DE-LU", "DE_LU", "DE"}


def _utc(value: TimeLike) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def intraday_index_path(source: str = "id_aep", *, root: Path | None = None, raw: Path | None = None) -> Path:
    """Resolve the local file of an intraday index (public tree first, then the raw tree)."""

    relpath = _SOURCES.get(source)
    if relpath is None:
        raise ValueError(f"Unknown intraday index source {source!r}; expected one of {sorted(_SOURCES)}")
    public = (root or data_root()) / relpath
    if public.exists():
        return public
    return (raw or raw_root()) / relpath


def load_intraday_index(
    start: TimeLike,
    end: TimeLike,
    *,
    zone: str = ID_AEP_ZONE,
    source: str = "id_aep",
    root: Path | None = None,
    raw: Path | None = None,
) -> pd.DataFrame:
    """Quarter-hourly intraday index for ``[start, end)``: ``timestamp_utc``, ``resolution_minutes``,
    ``id_aep_eur_mwh`` (NaN where the index is undefined or missing). Naive bounds are UTC.

    Raises ``FileNotFoundError`` naming the fetch command when the local file does not exist.
    """

    if zone.upper() not in _ZONES:
        raise ValueError(f"The ID AEP covers the German market area only (zone 'DE-LU'), not {zone!r}")
    lo, hi = _utc(start), _utc(end)
    if hi <= lo:
        raise ValueError(f"`end` ({hi}) must be after `start` ({lo}); intervals are half-open [start, end)")
    path = intraday_index_path(source, root=root, raw=raw)
    if not path.exists():
        raise FileNotFoundError(
            f"Intraday index file not found: {path}. Create it with: {FETCH_COMMAND}  (the ID AEP is not "
            "redistributed; the file is built locally under data/raw/intraday/)"
        )
    table = pq.read_table(path, filters=[("timestamp_utc", ">=", lo), ("timestamp_utc", "<", hi)])
    frame = table.to_pandas()
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True).astype("datetime64[ns, UTC]")
    frame = frame.sort_values("timestamp_utc", kind="stable").reset_index(drop=True)
    return frame[["timestamp_utc", "resolution_minutes", ID_AEP_COLUMN]]
