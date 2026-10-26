"""Same-time-of-day empirical price distributions (used for quarter-hours beyond the model horizon)."""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

from .models import quantile_weights

FloatArray = NDArray[np.float64]

#: Quantile levels summarising the slot climatology; the outer levels keep price spikes in the distribution.
CLIMATOLOGY_LEVELS: tuple[float, ...] = (0.03, 0.1, 0.25, 0.5, 0.75, 0.9, 0.97)
QH_PER_DAY = 96


def slot_climatology(
    prices_qh: FloatArray,
    *,
    days: int = 28,
    neighbours: int = 1,
    levels: tuple[float, ...] = CLIMATOLOGY_LEVELS,
    min_samples: int = 8,
) -> tuple[NDArray[np.float32], NDArray[np.float32]]:
    """Per quarter-hour ``j``: quantiles of ``prices[j - 96 d + delta]`` for ``d = 1..days`` and
    ``|delta| <= neighbours`` (NaN-aware). Returns ``(values (n_qh, S), weights (S,))``.

    Only quarter-hours at least ``96 - neighbours`` slots before ``j`` are used, so the row for ``j`` is causal for
    any decision taken after quarter-hour ``j - 96 + neighbours`` has been published.
    """
    prices = np.asarray(prices_qh, dtype=float)
    n = len(prices)
    offsets = np.array(
        [QH_PER_DAY * d - delta for d in range(1, days + 1) for delta in range(-neighbours, neighbours + 1)],
        dtype=np.int64,
    )
    weights = quantile_weights(levels).astype(np.float32)
    out = np.full((n, len(levels)), np.nan, dtype=np.float32)
    chunk = 8192
    for start in range(0, n, chunk):
        j = np.arange(start, min(n, start + chunk), dtype=np.int64)
        idx = j[:, None] - offsets[None, :]
        valid = idx >= 0
        sample = np.where(valid, prices[np.clip(idx, 0, max(n - 1, 0))], np.nan)
        counts = np.isfinite(sample).sum(axis=1)
        enough = counts >= min_samples
        if enough.any():
            out[j[enough]] = np.nanquantile(sample[enough], levels, axis=1).T.astype(np.float32)
    return out, weights


def trailing_mean(prices_qh: FloatArray, window: int) -> FloatArray:
    """NaN-aware trailing mean over ``prices[k - window + 1 : k + 1]``."""
    prices = np.asarray(prices_qh, dtype=float)
    present = np.isfinite(prices)
    cs = np.concatenate([[0.0], np.cumsum(np.where(present, prices, 0.0))])
    cc = np.concatenate([[0], np.cumsum(present)])
    k = np.arange(len(prices))
    lo = np.maximum(k - window + 1, 0)
    count = cc[k + 1] - cc[lo]
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(count > 0, (cs[k + 1] - cs[lo]) / np.maximum(count, 1), np.nan)
