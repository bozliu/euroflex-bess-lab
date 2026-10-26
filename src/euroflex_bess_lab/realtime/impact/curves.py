"""Monotone volume -> price response curves estimated from history.

The impact operators need to know how a balancing price (MIP, MDP, the aFRR / mFRR product prices) moves when the
activated balancing volume changes. Without Elia's real-time merit order this is estimated from history:

``price_k = mu_{g(k)} + f(volume_k) + e_k``

where ``g(k)`` is a fixed-effect group (default: the local delivery hour, so only quarter-hours of the same hour are
compared and slow drivers such as the day-ahead price, fuel prices and the bid stack of the day are absorbed) and
``f`` is a continuous piecewise-linear function on volume knots, constrained to be monotone (non-decreasing for
upward prices, non-increasing for downward prices) and normalised to ``f(0) = 0``. The model is linear in the
segment increments, so it is solved exactly with bounded least squares after within-group demeaning
(Frisch-Waugh). Prices are winsorised before fitting so that a handful of scarcity spikes do not dominate.

``RegimeCurves`` adds state dependence: one curve per local-hour block (for example night, morning ramp, solar
hours, evening peak) with the pooled curve as fallback where a regime has too few observations.

All curves are *incremental*: they are used as ``price' = price_observed + f(v') - f(v)``, so the observed price
level of a quarter-hour is always kept and only the volume-induced change is modelled.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

import numpy as np
import pandas as pd
from numpy.typing import ArrayLike, NDArray
from scipy.optimize import lsq_linear

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]

PRICE_FLOOR_EUR_MWH = -15_000.0
PRICE_CAP_EUR_MWH = 15_000.0
DEFAULT_REGIME_EDGES_HOURS: tuple[int, ...] = (0, 6, 10, 17, 22, 24)


@dataclass(frozen=True)
class ResponseCurve:
    """Continuous piecewise-linear monotone curve ``f(v)`` with ``f(knots[0]) = 0``.

    ``knots`` are volumes in MW (increasing, first knot 0), ``values`` the curve at the knots (EUR/MWh). Beyond the
    last knot the curve is extended linearly with ``tail_slope`` (EUR/MWh per MW). Negative volumes are clipped to
    zero (activated volumes are non-negative).
    """

    knots: tuple[float, ...]
    values: tuple[float, ...]
    increasing: bool
    tail_slope: float
    n_obs: int = 0
    label: str = ""

    def __post_init__(self) -> None:
        if len(self.knots) != len(self.values) or len(self.knots) < 2:
            raise ValueError("a response curve needs at least two knots and one value per knot")
        if np.any(np.diff(np.asarray(self.knots)) <= 0.0):
            raise ValueError("knots must be strictly increasing")
        steps = np.diff(np.asarray(self.values))
        if self.increasing and np.any(steps < -1e-9):
            raise ValueError("an increasing curve must have non-decreasing values")
        if not self.increasing and np.any(steps > 1e-9):
            raise ValueError("a decreasing curve must have non-increasing values")
        if (self.increasing and self.tail_slope < 0.0) or (not self.increasing and self.tail_slope > 0.0):
            raise ValueError("tail slope must have the curve's monotone direction")

    @classmethod
    def flat(cls, label: str = "flat", increasing: bool = True) -> ResponseCurve:
        """A curve with zero response (prices do not react to volume)."""
        return cls(knots=(0.0, 1.0), values=(0.0, 0.0), increasing=increasing, tail_slope=0.0, label=label)

    @classmethod
    def linear(cls, slope_eur_mwh_per_mw: float, label: str = "linear") -> ResponseCurve:
        """A straight line through the origin (positive slope = increasing curve)."""
        return cls(
            knots=(0.0, 1.0),
            values=(0.0, float(slope_eur_mwh_per_mw)),
            increasing=slope_eur_mwh_per_mw >= 0.0,
            tail_slope=float(slope_eur_mwh_per_mw),
            label=label,
        )

    @property
    def knots_array(self) -> FloatArray:
        return np.asarray(self.knots, dtype=float)

    @property
    def values_array(self) -> FloatArray:
        return np.asarray(self.values, dtype=float)

    def value(self, volume: ArrayLike) -> FloatArray:
        v = np.clip(np.asarray(volume, dtype=float), 0.0, None)
        knots = self.knots_array
        values = self.values_array
        inside = np.interp(v, knots, values)
        beyond = values[-1] + self.tail_slope * (v - knots[-1])
        out: FloatArray = np.where(v > knots[-1], beyond, inside)
        return out

    def delta(self, v_from: ArrayLike, v_to: ArrayLike) -> FloatArray:
        """``f(v_to) - f(v_from)``: the modelled price change when the activated volume moves."""
        result: FloatArray = self.value(v_to) - self.value(v_from)
        return result

    def slope(self, volume: ArrayLike) -> FloatArray:
        """Right derivative of the curve (EUR/MWh per MW)."""
        v = np.clip(np.asarray(volume, dtype=float), 0.0, None)
        knots = self.knots_array
        seg_slopes = np.diff(self.values_array) / np.diff(knots)
        idx = np.clip(np.searchsorted(knots, v, side="right") - 1, 0, len(seg_slopes) - 1)
        out: FloatArray = np.where(v >= knots[-1], self.tail_slope, seg_slopes[idx])
        return out

    def scaled(self, factor: float) -> ResponseCurve:
        """Same shape with the response multiplied by ``factor`` (for misspecification stress tests)."""
        if factor < 0.0:
            raise ValueError("factor must be non-negative")
        return replace(
            self,
            values=tuple(float(v) * factor for v in self.values),
            tail_slope=self.tail_slope * factor,
            label=f"{self.label}x{factor:g}",
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "knots_mw": list(self.knots),
            "values_eur_mwh": list(self.values),
            "increasing": self.increasing,
            "tail_slope_eur_mwh_per_mw": self.tail_slope,
            "n_obs": self.n_obs,
            "label": self.label,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ResponseCurve:
        return cls(
            knots=tuple(float(x) for x in data["knots_mw"]),
            values=tuple(float(x) for x in data["values_eur_mwh"]),
            increasing=bool(data["increasing"]),
            tail_slope=float(data["tail_slope_eur_mwh_per_mw"]),
            n_obs=int(data.get("n_obs", 0)),
            label=str(data.get("label", "")),
        )


def _group_codes(groups: ArrayLike | None, n: int) -> IntArray:
    if groups is None:
        return np.zeros(n, dtype=np.int64)
    _, codes = np.unique(np.asarray(groups), return_inverse=True)
    return np.asarray(codes, dtype=np.int64).reshape(-1)


def _demean(values: FloatArray, codes: IntArray) -> FloatArray:
    """Subtract group means (column-wise for 2-D input)."""
    counts = np.bincount(codes).astype(float)
    if values.ndim == 1:
        means = np.bincount(codes, weights=values) / counts
        out1: FloatArray = values - means[codes]
        return out1
    out = np.empty_like(values)
    for j in range(values.shape[1]):
        means = np.bincount(codes, weights=values[:, j]) / counts
        out[:, j] = values[:, j] - means[codes]
    return out


def ramp_basis(volume: FloatArray, knots: FloatArray) -> FloatArray:
    """Ramp functions ``phi_j`` so that ``f(v) = sum_j delta_j phi_j(v)`` is the piecewise-linear curve.

    ``phi_j(v) = clip((v - k_{j-1}) / (k_j - k_{j-1}), 0, 1)`` for ``j = 1..K-1``; the last ramp is not clipped above
    so the last segment extends linearly beyond the last knot.
    """
    lo, hi = knots[:-1], knots[1:]
    ramps = (volume[:, None] - lo[None, :]) / (hi - lo)[None, :]
    basis = np.clip(ramps, 0.0, 1.0)
    basis[:, -1] = np.clip(ramps[:, -1], 0.0, None)
    return basis


def default_knots(
    volume: FloatArray, *, n_knots: int = 10, max_quantile: float = 0.995, min_spacing_mw: float = 10.0
) -> FloatArray:
    """Knots at quantiles of the positive volumes (plus zero), at least ``min_spacing_mw`` apart."""
    positive = volume[volume > 1e-6]
    if len(positive) < 2:
        return np.array([0.0, max(float(np.max(volume, initial=0.0)), 1.0)])
    qs = np.quantile(positive, np.linspace(0.0, max_quantile, n_knots))
    knots = [0.0]
    for q in qs:
        if q - knots[-1] >= min_spacing_mw:
            knots.append(float(q))
    if len(knots) < 2:
        knots.append(max(float(np.max(positive)), min_spacing_mw))
    return np.asarray(knots, dtype=float)


def _design(
    v: FloatArray, p: FloatArray, codes_all: IntArray, knot_arr: FloatArray, estimator: str, min_group_size: int
) -> tuple[FloatArray, FloatArray, int]:
    """Regression design ``(x, y, n_used)`` with the level absorbed by within-group demeaning or first differences."""
    finite = np.isfinite(v) & np.isfinite(p)
    if estimator == "first_difference":
        k = np.flatnonzero((codes_all[1:] == codes_all[:-1]) & finite[1:] & finite[:-1]) + 1
        if len(k) == 0:
            return np.zeros((0, len(knot_arr) - 1)), np.zeros(0), 0
        x_fd = ramp_basis(v[k], knot_arr) - ramp_basis(v[k - 1], knot_arr)
        y_fd = p[k] - p[k - 1]
        return x_fd, y_fd, int(len(k))
    if estimator != "within":
        raise ValueError("estimator must be 'within' or 'first_difference'")
    vv, pp, codes = v[finite], p[finite], codes_all[finite]
    if len(vv) == 0:
        return np.zeros((0, len(knot_arr) - 1)), np.zeros(0), 0
    _, codes = np.unique(codes, return_inverse=True)
    keep = np.bincount(codes)[codes] >= min_group_size
    vv, pp, codes = vv[keep], pp[keep], codes[keep]
    if len(vv) == 0:
        return np.zeros((0, len(knot_arr) - 1)), np.zeros(0), 0
    _, codes = np.unique(codes, return_inverse=True)
    return _demean(ramp_basis(vv, knot_arr), codes), _demean(pp, codes), int(len(vv))


def fit_response_curve(
    volume: ArrayLike,
    price: ArrayLike,
    *,
    increasing: bool,
    groups: ArrayLike | None = None,
    knots: Sequence[float] | None = None,
    n_knots: int = 10,
    estimator: str = "first_difference",
    winsor: tuple[float, float] = (0.01, 0.99),
    price_clip: tuple[float, float] = (0.001, 0.999),
    min_group_size: int = 2,
    max_tail_slope: float | None = None,
    label: str = "",
) -> ResponseCurve:
    """Estimate a monotone piecewise-linear volume -> price curve with fixed effects.

    ``groups`` (default: one group) define the fixed effects; rows must be in time order. ``estimator``:

    - ``first_difference`` (default): regress the change of price between *consecutive rows of the same group* on
      the change of the curve basis -- the quantity the impact operators predict (how the price of this quarter-hour
      moves when its activated volume moves);
    - ``within``: demean price and basis within each group (rows in groups smaller than ``min_group_size`` dropped).

    Increments are constrained to the curve's monotone direction. Robustness: prices are clipped at the ``price_clip``
    quantiles (leverage of isolated scarcity spikes), a first fit is made, its *residuals* are winsorised at the
    ``winsor`` quantiles and the curve is refitted (keeps systematic high prices at high volumes). Segments without
    identifying variation stay flat. ``max_tail_slope`` caps the extrapolation slope beyond the last knot.
    """
    v = np.asarray(volume, dtype=float).reshape(-1)
    p = np.asarray(price, dtype=float).reshape(-1)
    if v.shape != p.shape:
        raise ValueError("volume and price must have the same length")
    codes_all = _group_codes(groups, len(v))
    finite = np.isfinite(v) & np.isfinite(p)
    if finite.sum() < 10:
        return ResponseCurve.flat(label=label, increasing=increasing)
    v = np.where(finite, np.clip(v, 0.0, None), np.nan)
    lo_q, hi_q = np.quantile(p[finite], price_clip)
    p = np.where(finite, np.clip(p, lo_q, hi_q), np.nan)
    knot_arr = np.asarray(knots, dtype=float) if knots is not None else default_knots(v[finite], n_knots=n_knots)
    if knot_arr[0] != 0.0:
        knot_arr = np.concatenate([[0.0], knot_arr[knot_arr > 0.0]])
    x, y, n_used = _design(v, p, codes_all, knot_arr, estimator, min_group_size)
    increments = np.zeros(len(knot_arr) - 1, dtype=float)
    if n_used < 10:
        return ResponseCurve.flat(label=label, increasing=increasing)
    # segments without identifying variation are not identified: hold them flat
    identified = np.sqrt(np.mean(x**2, axis=0)) > 1e-6
    if identified.any():
        xi = x[:, identified]
        bounds = (0.0, np.inf) if increasing else (-np.inf, 0.0)
        method = "bvls" if xi.shape[1] <= 40 else "trf"
        first = np.asarray(lsq_linear(xi, y, bounds=bounds, method=method).x, dtype=float)
        fitted = xi @ first
        resid = y - fitted
        r_lo, r_hi = np.quantile(resid, winsor)
        y_robust = fitted + np.clip(resid, r_lo, r_hi)
        increments[identified] = np.asarray(lsq_linear(xi, y_robust, bounds=bounds, method=method).x, dtype=float)
    if increasing:
        increments = np.clip(increments, 0.0, None)
    else:
        increments = np.clip(increments, None, 0.0)
    values = np.concatenate([[0.0], np.cumsum(increments)])
    tail = float(increments[-1] / (knot_arr[-1] - knot_arr[-2]))
    if max_tail_slope is not None:
        tail = float(np.clip(tail, -abs(max_tail_slope), abs(max_tail_slope)))
    return ResponseCurve(
        knots=tuple(float(k) for k in knot_arr),
        values=tuple(float(x) for x in values),
        increasing=increasing,
        tail_slope=tail,
        n_obs=n_used,
        label=label,
    )


def local_hour_regime(
    times: pd.DatetimeIndex | pd.Series | ArrayLike,
    *,
    timezone: str = "Europe/Brussels",
    edges_hours: Sequence[int] = DEFAULT_REGIME_EDGES_HOURS,
) -> IntArray:
    """Regime id per timestamp from local-hour blocks (``edges_hours`` = block boundaries, 0 and 24 included)."""
    index = pd.DatetimeIndex(pd.to_datetime(np.asarray(times), utc=True))
    hours = np.asarray(index.tz_convert(timezone).hour, dtype=np.int64)
    edges = np.asarray(edges_hours, dtype=np.int64)
    if edges[0] != 0 or edges[-1] != 24 or np.any(np.diff(edges) <= 0):
        raise ValueError("edges_hours must increase from 0 to 24")
    out: IntArray = np.clip(np.searchsorted(edges, hours, side="right") - 1, 0, len(edges) - 2).astype(np.int64)
    return out


def hour_groups(times: pd.DatetimeIndex | pd.Series | ArrayLike) -> IntArray:
    """Fixed-effect group id = delivery hour (Belgian local hours coincide with UTC hours, so this is DST-safe)."""
    index = pd.DatetimeIndex(pd.to_datetime(np.asarray(times), utc=True))
    hour_ns = index.floor("1h").asi8
    _, codes = np.unique(hour_ns, return_inverse=True)
    return np.asarray(codes, dtype=np.int64)


def day_groups(times: pd.DatetimeIndex | pd.Series | ArrayLike, *, timezone: str = "Europe/Brussels") -> IntArray:
    index = pd.DatetimeIndex(pd.to_datetime(np.asarray(times), utc=True))
    days = index.tz_convert(timezone).normalize().asi8
    _, codes = np.unique(days, return_inverse=True)
    return np.asarray(codes, dtype=np.int64)


@dataclass(frozen=True)
class RegimeCurves:
    """One response curve per regime with a pooled fallback."""

    pooled: ResponseCurve
    by_regime: dict[int, ResponseCurve] = field(default_factory=dict)

    def curve(self, regime: int) -> ResponseCurve:
        return self.by_regime.get(int(regime), self.pooled)

    def delta(self, regime: ArrayLike, v_from: ArrayLike, v_to: ArrayLike) -> FloatArray:
        """Price change per row; ``regime`` has one entry per row (broadcast over trailing axes of the volumes)."""
        reg = np.asarray(regime, dtype=np.int64).reshape(-1)
        a = np.asarray(v_from, dtype=float)
        b = np.asarray(v_to, dtype=float)
        a, b = np.broadcast_arrays(a, b)
        if a.ndim == 0:
            return np.asarray(self.curve(int(reg[0]) if len(reg) else 0).delta(a, b), dtype=float)
        if len(reg) == 1 and a.shape[0] != 1:
            reg = np.repeat(reg, a.shape[0])
        if len(reg) != a.shape[0]:
            raise ValueError("regime must have one entry per row")
        out = np.empty(a.shape, dtype=float)
        for r in np.unique(reg):
            mask = reg == r
            out[mask] = self.curve(int(r)).delta(a[mask], b[mask])
        return out

    def scaled(self, factor: float) -> RegimeCurves:
        return RegimeCurves(
            pooled=self.pooled.scaled(factor),
            by_regime={k: c.scaled(factor) for k, c in self.by_regime.items()},
        )

    def to_dict(self) -> dict[str, Any]:
        return {"pooled": self.pooled.to_dict(), "by_regime": {str(k): c.to_dict() for k, c in self.by_regime.items()}}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> RegimeCurves:
        return cls(
            pooled=ResponseCurve.from_dict(data["pooled"]),
            by_regime={int(k): ResponseCurve.from_dict(v) for k, v in dict(data.get("by_regime", {})).items()},
        )


def fit_regime_curves(
    volume: ArrayLike,
    price: ArrayLike,
    *,
    increasing: bool,
    regime: ArrayLike | None = None,
    groups: ArrayLike | None = None,
    min_regime_obs: int = 2000,
    label: str = "",
    **kwargs: Any,
) -> RegimeCurves:
    """Pooled curve plus one curve per regime with at least ``min_regime_obs`` usable rows (same knots)."""
    v = np.asarray(volume, dtype=float).reshape(-1)
    p = np.asarray(price, dtype=float).reshape(-1)
    g = None if groups is None else np.asarray(groups).reshape(-1)
    pooled = fit_response_curve(v, p, increasing=increasing, groups=g, label=f"{label}:pooled", **kwargs)
    by_regime: dict[int, ResponseCurve] = {}
    if regime is not None:
        reg = np.asarray(regime, dtype=np.int64).reshape(-1)
        knots = kwargs.pop("knots", None) or list(pooled.knots)
        for r in np.unique(reg):
            mask = reg == r
            usable = int(np.sum(mask & np.isfinite(v) & np.isfinite(p)))
            if usable < min_regime_obs:
                continue
            by_regime[int(r)] = fit_response_curve(
                v[mask],
                p[mask],
                increasing=increasing,
                groups=None if g is None else g[mask],
                knots=knots,
                label=f"{label}:regime{int(r)}",
                **kwargs,
            )
    return RegimeCurves(pooled=pooled, by_regime=by_regime)
