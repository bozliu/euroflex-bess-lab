"""Walk-forward (expanding or rolling) monthly splits with an embargo on target availability.

A fold trains at ``fit_time = test_start``: it may only use decision rows earlier than ``test_start`` whose target
(the final value of a quarter-hour, published ``qh_final_lag_minutes`` after it ends) was already published at
``fit_time``. :func:`training_rows` enforces both conditions; tests check that no training row ever falls inside or
after the test window.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ..replay import MarketData
from ..types import PublicationModel
from .features import target_known_minute


def _utc(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


@dataclass(frozen=True)
class Fold:
    """Train on ``[train_start, train_end)`` (rows whose target is known at ``train_end``), test on
    ``[test_start, test_end)``. ``train_end <= test_start`` always holds."""

    train_start: pd.Timestamp
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp

    def __post_init__(self) -> None:
        if not self.train_start < self.train_end <= self.test_start < self.test_end:
            raise ValueError(f"invalid fold ordering: {self}")

    @property
    def label(self) -> str:
        return f"{self.test_start:%Y-%m}"

    def to_dict(self) -> dict[str, str]:
        return {
            "train_start": self.train_start.isoformat(),
            "train_end": self.train_end.isoformat(),
            "test_start": self.test_start.isoformat(),
            "test_end": self.test_end.isoformat(),
        }


def monthly_folds(
    test_start: str | pd.Timestamp,
    test_end: str | pd.Timestamp,
    *,
    data_start: str | pd.Timestamp,
    train_months: int | None = 6,
    min_train_days: float = 7.0,
) -> list[Fold]:
    """One fold per calendar month (UTC) of ``[test_start, test_end)``; retrain at every month start.

    ``train_months=None`` gives an expanding window from ``data_start``. A month is skipped when less than
    ``min_train_days`` of history precede it.
    """
    start, end, first = _utc(test_start), _utc(test_end), _utc(data_start)
    boundaries = list(pd.date_range(start.normalize().replace(day=1), end, freq="MS", tz="UTC"))
    if not boundaries or boundaries[0] > start:
        boundaries.insert(0, start)
    folds: list[Fold] = []
    for i, month_start in enumerate(boundaries):
        lo = max(month_start, start)
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else end
        hi = min(hi, end)
        if hi <= lo:
            continue
        train_end = lo
        train_start = first if train_months is None else max(first, lo - pd.DateOffset(months=train_months))
        if (train_end - train_start) < pd.Timedelta(days=min_train_days):
            continue
        folds.append(Fold(pd.Timestamp(train_start), train_end, lo, hi))
    return folds


def single_fold(
    train_start: str | pd.Timestamp,
    train_end: str | pd.Timestamp,
    test_start: str | pd.Timestamp,
    test_end: str | pd.Timestamp,
) -> list[Fold]:
    """A frozen model: train once, apply to the whole test window (e.g. zero-history transfer)."""
    return [Fold(_utc(train_start), _utc(train_end), _utc(test_start), _utc(test_end))]


def minute_range(market: MarketData, start: pd.Timestamp, end: pd.Timestamp) -> tuple[int, int]:
    """Clip ``[start, end)`` to the market grid and return minute indices."""
    m0 = int(np.ceil((start - market.start_utc) / pd.Timedelta(minutes=1)))
    m1 = int(np.ceil((end - market.start_utc) / pd.Timedelta(minutes=1)))
    return max(0, m0), min(market.n_minutes, max(0, m1))


def training_rows(
    market: MarketData,
    fold: Fold,
    *,
    horizon: int,
    publication: PublicationModel,
    stride: int = 1,
    offset: int = 0,
) -> NDArray[np.int64]:
    """Row indices usable to train a model for ``fold`` and target horizon ``horizon`` (embargoed)."""
    m0, m1 = minute_range(market, fold.train_start, fold.train_end)
    if m1 <= m0:
        return np.zeros(0, dtype=np.int64)
    rows = np.arange(m0 + (offset % max(stride, 1)), m1, max(stride, 1), dtype=np.int64)
    fit_minute = (fold.train_end - market.start_utc) / pd.Timedelta(minutes=1)
    known = target_known_minute(market, horizon, publication)[rows]
    return rows[known <= fit_minute]


def window_rows(market: MarketData, fold: Fold) -> NDArray[np.int64]:
    m0, m1 = minute_range(market, fold.test_start, fold.test_end)
    return np.arange(m0, m1, dtype=np.int64)
