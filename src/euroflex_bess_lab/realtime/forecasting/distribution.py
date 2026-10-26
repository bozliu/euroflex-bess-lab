"""Vectorised statistics of per-row discrete distributions (values with probability weights)."""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

FloatArray = NDArray[np.float64]


def _sorted(values: NDArray[np.floating], weights: NDArray[np.floating]) -> tuple[FloatArray, FloatArray]:
    v = np.asarray(values, dtype=float)
    w = np.asarray(weights, dtype=float)
    w = np.broadcast_to(w, v.shape)
    order = np.argsort(v, axis=1)
    vs = np.take_along_axis(v, order, axis=1)
    ws = np.take_along_axis(w, order, axis=1)
    total = ws.sum(axis=1, keepdims=True)
    with np.errstate(invalid="ignore", divide="ignore"):
        ws = ws / total
    return vs, ws


def weighted_quantile(values: NDArray[np.floating], weights: NDArray[np.floating], tau: float) -> FloatArray:
    """Per-row ``tau`` quantile (lower inverse CDF) of a discrete distribution."""
    if not 0.0 <= tau <= 1.0:
        raise ValueError("tau must lie in [0, 1]")
    vs, ws = _sorted(values, weights)
    cum = np.cumsum(ws, axis=1)
    idx = np.argmax(cum >= tau - 1e-9, axis=1)
    out = vs[np.arange(len(vs)), idx]
    bad = ~np.isfinite(cum[:, -1]) | np.isnan(vs).any(axis=1)
    out[bad] = np.nan
    return out


def tail_mean(values: NDArray[np.floating], weights: NDArray[np.floating], alpha: float, *, lower: bool) -> FloatArray:
    """Mean of the lowest (``lower=True``) or highest ``alpha`` probability mass (CVaR of a long/short position)."""
    if not 0.0 < alpha <= 1.0:
        raise ValueError("alpha must lie in (0, 1]")
    vs, ws = _sorted(values, weights)
    if not lower:
        vs, ws = vs[:, ::-1], ws[:, ::-1]
    cum = np.cumsum(ws, axis=1)
    before = cum - ws
    take = np.clip(np.minimum(cum, alpha) - before, 0.0, None)
    with np.errstate(invalid="ignore"):
        out: FloatArray = (take * vs).sum(axis=1) / alpha
    bad = ~np.isfinite(cum[:, -1]) | np.isnan(vs).any(axis=1)
    out[bad] = np.nan
    return out


def distribution_mean(values: NDArray[np.floating], weights: NDArray[np.floating]) -> FloatArray:
    vs, ws = _sorted(values, weights)
    out: FloatArray = (vs * ws).sum(axis=1)
    out[np.isnan(vs).any(axis=1)] = np.nan
    return out


def crps_discrete(values: NDArray[np.floating], weights: NDArray[np.floating], y: NDArray[np.floating]) -> FloatArray:
    """CRPS of a discrete forecast: ``E|X - y| - 0.5 E|X - X'|`` (lower is better)."""
    v = np.asarray(values, dtype=float)
    w = np.broadcast_to(np.asarray(weights, dtype=float), v.shape)
    w = w / w.sum(axis=1, keepdims=True)
    obs = np.asarray(y, dtype=float)[:, None]
    term1 = (w * np.abs(v - obs)).sum(axis=1)
    pair = np.abs(v[:, :, None] - v[:, None, :])
    term2 = 0.5 * np.einsum("ni,nij,nj->n", w, pair, w)
    return term1 - term2
