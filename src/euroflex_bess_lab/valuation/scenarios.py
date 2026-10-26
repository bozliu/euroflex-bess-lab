"""Multi-year revenue-distribution generator for contract pricing.

The generator turns one or more histories of *daily P&L per MW* into annual revenue paths for a contract tenor
(typically 5-10 years):

``R[p, y] = B_r[p, y] * S_r(Q[p, y]) / S_r(Q_ref) * K[y] * F[y] * A``

- ``B_r[p, y]``: sum over the real calendar days of year ``y`` of days resampled from the history of regime ``r``
  (seasonal block or stationary bootstrap, optionally weekday-aligned, so seasonality, weekday structure and
  multi-day volatility clusters survive);
- ``r = regime[p, y]``: probability-weighted regime (e.g. ``current`` vs ``new2026`` imbalance-price formula) with
  a persistence parameter (1 = one regime per path = model uncertainty, 0 = independent draw every year);
- ``S_r(Q) / S_r(Q_ref)``: saturation / cannibalisation multiplier from a capacity-value curve (per-MW value as a
  function of the total implicit-reaction fleet ``Q``) along a fleet build-out path, relative to the fleet
  ``Q_ref`` already reflected in the history;
- ``K[y]``: optimiser skill decay on the skill share of revenue; ``F[y]``: capacity fade; ``A``: availability;
- optionally ``L[p, y] = exp(s x[p, y] - s^2 / 2)`` with ``x`` a unit-variance AR(1) across years: a market
  *level* shock for inter-annual spread uncertainty that a short history cannot show (multiplies ``R``).

All randomness comes from one :class:`numpy.random.SeedSequence`, so a seed reproduces every path exactly.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd
from numpy.typing import ArrayLike, NDArray

from .revenue import complete_days

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]
BootstrapMethod = Literal["block", "stationary", "iid"]

DEFAULT_SEED = 20261124
"""Default seed (the planned go-live date of the new Belgian imbalance-price formula)."""

_FLEET_COLUMNS = ("fleet_mw", "total_fleet_mw", "implicit_fleet_mw", "q_mw")
_VALUE_COLUMNS = ("value_eur_per_mw_year", "value_per_mw", "eur_per_mw_year", "relative_value", "value")


def exceedance_levels(values: ArrayLike) -> dict[str, float]:
    """P90 / P50 / P10 in the project-finance *exceedance* convention.

    ``p90`` is the level exceeded with 90 % probability (the 10th percentile), ``p10`` the level exceeded with 10 %
    probability (the 90th percentile).
    """
    x = np.asarray(values, dtype=float).ravel()
    return {
        "p90": float(np.quantile(x, 0.10)),
        "p50": float(np.quantile(x, 0.50)),
        "p10": float(np.quantile(x, 0.90)),
    }


# ---------------------------------------------------------------------------------------------------------------
# Saturation inputs
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class CapacityValueCurve:
    """Per-MW value as a function of the total implicit-reaction fleet (MW).

    Only ratios ``value(Q) / value(Q_ref)`` enter the scenarios, so the curve may be in EUR/MW/year or a relative
    index. Values are linearly interpolated; outside the sampled range they are extrapolated flat or linearly
    (default, slope of the last segment), floored at zero.
    """

    fleet_mw: tuple[float, ...]
    value_per_mw: tuple[float, ...]
    name: str = "capacity_value_curve"
    source: str = "unspecified"
    units: str = "EUR/MW/year"
    placeholder: bool = False
    extrapolation: Literal["flat", "linear"] = "linear"

    def __post_init__(self) -> None:
        x = np.asarray(self.fleet_mw, dtype=float)
        v = np.asarray(self.value_per_mw, dtype=float)
        if x.ndim != 1 or len(x) < 2 or x.shape != v.shape:
            raise ValueError("capacity-value curve needs at least two (fleet_mw, value) points of equal length")
        if not (np.all(np.isfinite(x)) and np.all(np.isfinite(v))):
            raise ValueError("capacity-value curve points must be finite")
        if np.any(np.diff(x) <= 0.0):
            raise ValueError("capacity-value curve fleet_mw must be strictly increasing")
        if np.any(v < 0.0):
            raise ValueError("capacity-value curve values must be non-negative")
        if self.extrapolation not in ("flat", "linear"):
            raise ValueError("extrapolation must be 'flat' or 'linear'")

    def value(self, fleet_mw: ArrayLike) -> FloatArray:
        q = np.asarray(fleet_mw, dtype=float)
        x = np.asarray(self.fleet_mw, dtype=float)
        v = np.asarray(self.value_per_mw, dtype=float)
        out = np.interp(q, x, v)
        if self.extrapolation == "linear":
            low_slope = (v[1] - v[0]) / (x[1] - x[0])
            high_slope = (v[-1] - v[-2]) / (x[-1] - x[-2])
            out = np.where(q < x[0], v[0] + low_slope * (q - x[0]), out)
            out = np.where(q > x[-1], v[-1] + high_slope * (q - x[-1]), out)
        result: FloatArray = np.clip(out, 0.0, None)
        return result

    def multiplier(self, fleet_mw: ArrayLike, reference_fleet_mw: float) -> FloatArray:
        """``value(Q) / value(Q_ref)``: scaling of per-MW revenue when the fleet moves from ``Q_ref`` to ``Q``."""
        reference = float(self.value(reference_fleet_mw))
        if reference <= 0.0:
            raise ValueError("capacity-value curve must be positive at the reference fleet size")
        result: FloatArray = self.value(fleet_mw) / reference
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source": self.source,
            "units": self.units,
            "placeholder": self.placeholder,
            "extrapolation": self.extrapolation,
            "fleet_mw": [float(x) for x in self.fleet_mw],
            "value_per_mw": [float(v) for v in self.value_per_mw],
        }

    @classmethod
    def from_frame(
        cls,
        frame: pd.DataFrame,
        *,
        mechanism: str | None = None,
        name: str | None = None,
        source: str = "file",
        extrapolation: Literal["flat", "linear"] = "linear",
    ) -> CapacityValueCurve:
        """Build a curve from a table with ``fleet_mw`` and ``value_eur_per_mw_year`` (aliases accepted).

        An optional ``mechanism`` column holds one curve per settlement mechanism; select one with ``mechanism``.
        """
        fleet_col = next((c for c in _FLEET_COLUMNS if c in frame.columns), None)
        value_col = next((c for c in _VALUE_COLUMNS if c in frame.columns), None)
        if fleet_col is None or value_col is None:
            raise ValueError(
                f"capacity-value curve needs a fleet column {_FLEET_COLUMNS} and a value column {_VALUE_COLUMNS}"
            )
        data = frame
        if "mechanism" in frame.columns:
            available = sorted(str(m) for m in frame["mechanism"].dropna().unique())
            if mechanism is None:
                if len(available) > 1:
                    raise ValueError(f"curve file holds several mechanisms {available}; choose one")
            else:
                data = frame.loc[frame["mechanism"].astype(str) == mechanism]
                if data.empty:
                    raise ValueError(f"mechanism `{mechanism}` not in curve file (available: {available})")
        points = data.groupby(fleet_col, sort=True)[value_col].mean()
        units = "EUR/MW/year" if value_col in ("value_eur_per_mw_year", "eur_per_mw_year") else "relative"
        return cls(
            fleet_mw=tuple(float(x) for x in points.index),
            value_per_mw=tuple(float(v) for v in points.to_numpy(dtype=float)),
            name=name or (f"capacity_value_curve[{mechanism}]" if mechanism else "capacity_value_curve"),
            source=source,
            units=units,
            extrapolation=extrapolation,
        )


def placeholder_capacity_value_curve(
    half_value_mw: float = 1500.0, max_fleet_mw: float = 6000.0, n_points: int = 25
) -> CapacityValueCurve:
    """PLACEHOLDER curve - a shape for tests and demos, not an estimate.

    Relative per-MW value ``V(Q) = 1 / (1 + Q / half_value_mw)``: per-MW value halves once the implicit-reaction
    fleet reaches ``half_value_mw``. Replace it with the curve estimated by the impact study
    (``fleet_mw``, ``value_eur_per_mw_year``) before using any number commercially.
    """
    if half_value_mw <= 0.0 or max_fleet_mw <= 0.0 or n_points < 2:
        raise ValueError("half_value_mw and max_fleet_mw must be positive and n_points >= 2")
    q = np.linspace(0.0, max_fleet_mw, n_points)
    v = 1.0 / (1.0 + q / half_value_mw)
    return CapacityValueCurve(
        fleet_mw=tuple(float(x) for x in q),
        value_per_mw=tuple(float(x) for x in v),
        name=f"placeholder_hyperbolic_half_value_{half_value_mw:g}mw",
        source="placeholder (not an estimate): V(Q) = 1 / (1 + Q / Q_half)",
        units="relative",
        placeholder=True,
    )


def load_capacity_value_curve(
    path: str | Path, *, mechanism: str | None = None, extrapolation: Literal["flat", "linear"] = "linear"
) -> CapacityValueCurve:
    """Read a capacity-value curve from CSV, parquet or JSON (``{"fleet_mw": [...], "value_eur_per_mw_year": [...]}``).

    The literal ``placeholder`` returns :func:`placeholder_capacity_value_curve`.
    """
    if str(path) == "placeholder":
        return placeholder_capacity_value_curve()
    file = Path(path)
    suffix = file.suffix.lower()
    if suffix == ".csv":
        frame = pd.read_csv(file)
    elif suffix in {".parquet", ".pq"}:
        frame = pd.read_parquet(file)
    elif suffix == ".json":
        frame = pd.read_json(file)
    else:
        raise ValueError(f"unsupported capacity-value curve format `{file.suffix}`")
    return CapacityValueCurve.from_frame(frame, mechanism=mechanism, source=file.name, extrapolation=extrapolation)


@dataclass(frozen=True)
class FleetPath:
    """Implicit-reaction fleet (MW) per calendar year, on the same axis as the capacity-value curve.

    The base path is linearly interpolated between the given years and held flat outside them. Uncertainty is a
    persistent lognormal build-out factor per path (``path_log_sigma``) times independent year noise
    (``year_log_sigma``); both factors have mean 1.
    """

    years: tuple[int, ...]
    mw: tuple[float, ...]
    path_log_sigma: float = 0.0
    year_log_sigma: float = 0.0

    def __post_init__(self) -> None:
        if len(self.years) == 0 or len(self.years) != len(self.mw):
            raise ValueError("fleet path needs matching, non-empty years and mw")
        if any(m < 0.0 for m in self.mw):
            raise ValueError("fleet MW must be non-negative")
        if self.path_log_sigma < 0.0 or self.year_log_sigma < 0.0:
            raise ValueError("fleet log-sigmas must be non-negative")

    @classmethod
    def from_points(
        cls, points: Mapping[int, float], *, path_log_sigma: float = 0.0, year_log_sigma: float = 0.0
    ) -> FleetPath:
        ordered = sorted((int(year), float(mw)) for year, mw in points.items())
        return cls(
            years=tuple(y for y, _ in ordered),
            mw=tuple(m for _, m in ordered),
            path_log_sigma=path_log_sigma,
            year_log_sigma=year_log_sigma,
        )

    @classmethod
    def constant(cls, mw: float) -> FleetPath:
        return cls(years=(2000,), mw=(float(mw),))

    def base(self, years: Sequence[int]) -> FloatArray:
        result: FloatArray = np.interp(
            np.asarray(years, dtype=float), np.asarray(self.years, dtype=float), np.asarray(self.mw, dtype=float)
        )
        return result

    def sample(self, years: Sequence[int], n_paths: int, rng: np.random.Generator) -> FloatArray:
        base = self.base(years)
        z_path = rng.standard_normal((n_paths, 1))
        z_year = rng.standard_normal((n_paths, len(base)))
        s_p, s_y = self.path_log_sigma, self.year_log_sigma
        factor = np.exp(s_p * z_path - 0.5 * s_p**2 + s_y * z_year - 0.5 * s_y**2)
        result: FloatArray = base[None, :] * factor
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "years": list(self.years),
            "mw": [float(m) for m in self.mw],
            "path_log_sigma": self.path_log_sigma,
            "year_log_sigma": self.year_log_sigma,
        }


@dataclass(frozen=True)
class SaturationSpec:
    """Capacity-value curve(s) + fleet path + the fleet size already reflected in the history.

    ``curve`` may be one curve or a mapping ``regime name -> curve`` (key ``default`` as fallback). ``series``
    restricts the multiplier to some jointly simulated series (``None`` = all).
    """

    curve: CapacityValueCurve | Mapping[str, CapacityValueCurve]
    fleet: FleetPath
    reference_fleet_mw: float = 0.0
    series: tuple[str, ...] | None = None

    def curve_for(self, regime: str) -> CapacityValueCurve:
        if isinstance(self.curve, CapacityValueCurve):
            return self.curve
        if regime in self.curve:
            return self.curve[regime]
        if "default" in self.curve:
            return self.curve["default"]
        raise KeyError(f"no capacity-value curve for regime `{regime}` and no `default` curve")

    def to_dict(self) -> dict[str, Any]:
        curves: Any
        if isinstance(self.curve, CapacityValueCurve):
            curves = self.curve.to_dict()
        else:
            curves = {name: curve.to_dict() for name, curve in self.curve.items()}
        return {
            "curve": curves,
            "fleet": self.fleet.to_dict(),
            "reference_fleet_mw": self.reference_fleet_mw,
            "series": list(self.series) if self.series is not None else None,
        }


@dataclass(frozen=True)
class SkillDecay:
    """Erosion of the optimiser's edge: ``K[y] = (1 - share) + share * (1 - decay) ** (y + offset)``.

    ``skill_share`` is the fraction of revenue attributable to skill (e.g. ``1 - naive / strategy`` from the
    attribution waterfall); ``decay_per_year`` is the yearly fractional loss of that skill component.
    """

    skill_share: float
    decay_per_year: float
    offset_years: float = 0.0

    def __post_init__(self) -> None:
        if not 0.0 <= self.skill_share <= 1.0:
            raise ValueError("skill_share must be in [0, 1]")
        if not 0.0 <= self.decay_per_year < 1.0:
            raise ValueError("decay_per_year must be in [0, 1)")

    def multiplier(self, n_years: int) -> FloatArray:
        t = np.arange(n_years, dtype=float) + self.offset_years
        result: FloatArray = (1.0 - self.skill_share) + self.skill_share * (1.0 - self.decay_per_year) ** t
        return result


# ---------------------------------------------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class BootstrapConfig:
    """How history days are resampled into simulated calendar days.

    - ``block``: fixed blocks of ``block_days`` consecutive history days (7 = weekly blocks);
    - ``stationary``: Politis-Romano stationary bootstrap, geometric block lengths with mean ``block_days``;
    - ``iid``: independent days.

    Each block starts at a history day within ``season_window_days`` (circular day-of-year distance) of the
    simulated day and, with ``align_weekday``, on the same weekday. When fewer than ``min_candidates`` history days
    qualify (e.g. a regime observed only from January to July), the window is widened, then weekday alignment is
    dropped; the share of such fallbacks is reported in the diagnostics. A block that reaches a gap in the history
    ends early and a new block starts.
    """

    method: BootstrapMethod = "block"
    block_days: int = 7
    season_window_days: int | None = 30
    align_weekday: bool = True
    min_candidates: int = 8

    def __post_init__(self) -> None:
        if self.method not in ("block", "stationary", "iid"):
            raise ValueError("bootstrap method must be 'block', 'stationary' or 'iid'")
        if self.block_days < 1:
            raise ValueError("block_days must be >= 1")
        if self.season_window_days is not None and self.season_window_days < 0:
            raise ValueError("season_window_days must be >= 0 or None")
        if self.min_candidates < 1:
            raise ValueError("min_candidates must be >= 1")

    def to_dict(self) -> dict[str, Any]:
        return {
            "method": self.method,
            "block_days": self.block_days,
            "season_window_days": self.season_window_days,
            "align_weekday": self.align_weekday,
            "min_candidates": self.min_candidates,
        }


class _HistorySampler:
    """Seasonal / weekday-aligned block sampler over one daily panel (rows = days, columns = series)."""

    def __init__(self, panel: pd.DataFrame, bootstrap: BootstrapConfig) -> None:
        if panel.empty:
            raise ValueError("history panel is empty")
        index = pd.DatetimeIndex(panel.index)
        self.values = panel.to_numpy(dtype=float)
        if not np.all(np.isfinite(self.values)):
            raise ValueError("history panel contains non-finite values")
        self.n = len(index)
        self.doy = np.asarray(index.day_of_year, dtype=np.int64)
        self.weekday = np.asarray(index.day_of_week, dtype=np.int64)
        day_number = np.asarray((index.normalize() - pd.Timestamp("1970-01-01")).days, dtype=np.int64)
        nxt = np.full(self.n, -1, dtype=np.int64)
        if self.n > 1:
            consecutive = np.diff(day_number) == 1
            nxt[:-1] = np.where(consecutive, np.arange(1, self.n, dtype=np.int64), -1)
        self.next_index = nxt
        self.bootstrap = bootstrap
        self._cache: dict[tuple[int, int], IntArray] = {}
        self.fallback_lookups = 0
        self.lookups = 0

    def candidates(self, doy: int, weekday: int) -> IntArray:
        cfg = self.bootstrap
        key = (doy, weekday if cfg.align_weekday else -1)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        self.lookups += 1
        same_weekday = self.weekday == weekday
        fallback = False
        pool: IntArray
        if cfg.season_window_days is None:
            pool = np.flatnonzero(same_weekday) if cfg.align_weekday else np.arange(self.n, dtype=np.int64)
        else:
            gap = np.abs(self.doy - doy)
            distance = np.minimum(gap, 366 - gap)
            window = cfg.season_window_days
            while True:
                mask = distance <= window
                if cfg.align_weekday:
                    mask = mask & same_weekday
                pool = np.flatnonzero(mask)
                if len(pool) >= cfg.min_candidates or window >= 183:
                    break
                window = max(2 * window, window + 1)
                fallback = True
            if len(pool) < cfg.min_candidates and cfg.align_weekday:
                pool = np.flatnonzero(distance <= window)
                fallback = True
        if len(pool) == 0:
            pool = np.arange(self.n, dtype=np.int64)
            fallback = True
        if fallback:
            self.fallback_lookups += 1
        pool = pool.astype(np.int64)
        self._cache[key] = pool
        return pool

    def simulate(
        self,
        calendar: pd.DatetimeIndex,
        year_of_day: IntArray,
        n_years: int,
        n_paths: int,
        rng: np.random.Generator,
    ) -> tuple[FloatArray, dict[str, Any]]:
        cfg = self.bootstrap
        k = self.values.shape[1]
        annual = np.zeros((n_paths, n_years, k), dtype=float)
        doy = np.asarray(calendar.day_of_year, dtype=np.int64)
        weekday = np.asarray(calendar.day_of_week, dtype=np.int64)
        block = 1 if cfg.method == "iid" else cfg.block_days
        idx = np.zeros(n_paths, dtype=np.int64)
        used = np.zeros(self.n, dtype=bool)
        jumps = 0
        for t in range(len(calendar)):
            if t == 0 or block == 1:
                jump = np.ones(n_paths, dtype=bool)
                cont = idx
            else:
                cont = self.next_index[idx]
                jump = cont < 0
                if cfg.method == "block":
                    if t % block == 0:
                        jump = np.ones(n_paths, dtype=bool)
                else:
                    jump = jump | (rng.random(n_paths) < 1.0 / block)
            idx = np.where(jump, 0, cont)
            n_jump = int(jump.sum())
            if n_jump:
                pool = self.candidates(int(doy[t]), int(weekday[t]))
                idx[jump] = pool[rng.integers(0, len(pool), size=n_jump)]
                jumps += n_jump
            annual[:, year_of_day[t], :] += self.values[idx]
            used[idx] = True
        n_days = len(calendar)
        diagnostics = {
            "n_history_days": self.n,
            "mean_block_days": float(n_days * n_paths / jumps) if jumps else float("nan"),
            "history_days_used_share": float(used.mean()),
            "seasonal_lookups": self.lookups,
            "seasonal_fallback_share": float(self.fallback_lookups / self.lookups) if self.lookups else 0.0,
        }
        return annual, diagnostics


# ---------------------------------------------------------------------------------------------------------------
# Daily panels
# ---------------------------------------------------------------------------------------------------------------


def as_panel(
    history: pd.DataFrame,
    *,
    value_column: str = "pnl_eur_per_mw",
    min_coverage: float = 0.98,
    name: str = "asset",
) -> pd.DataFrame:
    """Daily panel (``DatetimeIndex`` of local dates x series) from a :func:`daily_pnl` table or a panel.

    A table with a ``date`` column is treated as one series (``value_column`` of its complete days); anything
    else must already be indexed by date with one numeric column per series.
    """
    if "date" in history.columns:
        if value_column not in history.columns:
            raise KeyError(f"daily table has no column `{value_column}` (pass power_mw to daily_pnl for per-MW)")
        kept = complete_days(history, min_coverage)
        panel = pd.DataFrame(
            {name: kept[value_column].to_numpy(dtype=float)}, index=pd.DatetimeIndex(kept["date"]).normalize()
        )
    else:
        panel = history.copy()
        panel.index = pd.DatetimeIndex(panel.index).normalize()
        panel.columns = [str(column) for column in panel.columns]
        panel = panel.astype(float)
    panel = panel.sort_index()
    if panel.index.duplicated().any():
        raise ValueError("daily panel has duplicate dates")
    if panel.empty:
        raise ValueError("daily panel is empty after removing partial days")
    return panel


def daily_panel(
    dailies: Mapping[str, pd.DataFrame],
    *,
    value_column: str = "pnl_eur_per_mw",
    min_coverage: float = 0.98,
) -> pd.DataFrame:
    """Join several daily tables into one panel on their common complete local dates (one column per name)."""
    if not dailies:
        raise ValueError("need at least one daily table")
    panels = [
        as_panel(table, value_column=value_column, min_coverage=min_coverage, name=name)
        for name, table in dailies.items()
    ]
    joined = pd.concat(panels, axis=1, join="inner").sort_index()
    if joined.empty:
        raise ValueError("daily tables have no common complete dates")
    return joined


def derive_regime_from_overlap(
    base: pd.DataFrame,
    variant: pd.DataFrame,
    *,
    method: Literal["ratio", "delta"] = "ratio",
    value_column: str = "pnl_eur_per_mw",
    min_coverage: float = 0.98,
    min_overlap_days: int = 28,
    prefer_observed: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Extend a short regime history (e.g. ``new2026``, observed Jan-Jul 2026) over the long base history.

    The paired overlap (same days priced under both mechanisms) gives either the ratio of totals
    (``method="ratio"``: ``variant = base * sum(variant) / sum(base)``) or the mean daily difference
    (``method="delta"``). Days observed in the variant keep their observed values when ``prefer_observed``.
    Returns the derived single-series panel and a record of the estimate.
    """
    base_panel = as_panel(base, value_column=value_column, min_coverage=min_coverage)
    variant_panel = as_panel(variant, value_column=value_column, min_coverage=min_coverage)
    if base_panel.shape[1] != 1 or variant_panel.shape[1] != 1:
        raise ValueError("derive_regime_from_overlap works on single-series histories")
    b = base_panel.iloc[:, 0]
    v = variant_panel.iloc[:, 0]
    common = b.index.intersection(v.index)
    if len(common) < min_overlap_days:
        raise ValueError(f"only {len(common)} overlapping days (need {min_overlap_days})")
    b_common = b.loc[common].to_numpy(dtype=float)
    v_common = v.loc[common].to_numpy(dtype=float)
    info: dict[str, Any] = {
        "method": method,
        "overlap_days": len(common),
        "overlap_first_day": common[0].date().isoformat(),
        "overlap_last_day": common[-1].date().isoformat(),
        "base_mean_daily": float(b_common.mean()),
        "variant_mean_daily": float(v_common.mean()),
    }
    if method == "ratio":
        if b_common.sum() <= 0.0:
            raise ValueError("ratio method needs a positive base total on the overlap")
        ratio = float(v_common.sum() / b_common.sum())
        derived = b * ratio
        info["ratio"] = ratio
    elif method == "delta":
        delta = float(np.mean(v_common - b_common))
        derived = b + delta
        info["delta_per_day"] = delta
    else:
        raise ValueError("method must be 'ratio' or 'delta'")
    if prefer_observed:
        derived = derived.copy()
        derived.loc[common] = v.loc[common]
    info["n_days"] = len(derived)
    return derived.to_frame(name=base_panel.columns[0]), info


# ---------------------------------------------------------------------------------------------------------------
# Scenario generation
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ScenarioConfig:
    """Tenor, path count, seed and the scenario layers (bootstrap, regimes, saturation, skill, fade)."""

    n_paths: int = 2000
    n_years: int = 7
    start_year: int = 2027
    seed: int = DEFAULT_SEED
    bootstrap: BootstrapConfig = field(default_factory=BootstrapConfig)
    regime_persistence: float = 1.0
    saturation: SaturationSpec | None = None
    skill: SkillDecay | None = None
    capacity_fade_per_year: float = 0.0
    availability_factor: float = 1.0
    level_log_sigma: float = 0.0
    level_persistence: float = 0.0

    def __post_init__(self) -> None:
        if self.n_paths < 1:
            raise ValueError("n_paths must be >= 1")
        if not 1 <= self.n_years <= 40:
            raise ValueError("n_years must be between 1 and 40")
        if not 0.0 <= self.regime_persistence <= 1.0:
            raise ValueError("regime_persistence must be in [0, 1]")
        if not 0.0 <= self.capacity_fade_per_year < 1.0:
            raise ValueError("capacity_fade_per_year must be in [0, 1)")
        if not 0.0 < self.availability_factor <= 2.0:
            raise ValueError("availability_factor must be in (0, 2]")
        if self.level_log_sigma < 0.0:
            raise ValueError("level_log_sigma must be non-negative")
        if not -1.0 < self.level_persistence < 1.0:
            raise ValueError("level_persistence must be in (-1, 1)")

    def to_dict(self) -> dict[str, Any]:
        return {
            "n_paths": self.n_paths,
            "n_years": self.n_years,
            "start_year": self.start_year,
            "seed": self.seed,
            "bootstrap": self.bootstrap.to_dict(),
            "regime_persistence": self.regime_persistence,
            "saturation": self.saturation.to_dict() if self.saturation is not None else None,
            "skill": (
                {
                    "skill_share": self.skill.skill_share,
                    "decay_per_year": self.skill.decay_per_year,
                    "offset_years": self.skill.offset_years,
                }
                if self.skill is not None
                else None
            ),
            "capacity_fade_per_year": self.capacity_fade_per_year,
            "availability_factor": self.availability_factor,
            "level_log_sigma": self.level_log_sigma,
            "level_persistence": self.level_persistence,
        }


@dataclass(frozen=True)
class ScenarioSet:
    """Annual revenue paths (EUR per MW per year) for one or more jointly simulated series.

    ``series[name]`` has shape ``(n_paths, n_years)``; ``primary`` names the series contracts are priced on.
    ``base_series`` holds the bootstrap sums before the saturation / skill / fade / availability multipliers.
    """

    years: tuple[int, ...]
    series: dict[str, FloatArray]
    primary: str
    base_series: dict[str, FloatArray]
    regime_index: IntArray
    regime_names: tuple[str, ...]
    fleet_mw: FloatArray | None
    saturation_multiplier: FloatArray
    skill_multiplier: FloatArray
    fade_multiplier: FloatArray
    level_multiplier: FloatArray
    days_in_year: IntArray
    config: dict[str, Any] = field(default_factory=dict)
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.primary not in self.series:
            raise KeyError(f"primary series `{self.primary}` not in {sorted(self.series)}")
        shape = self.series[self.primary].shape
        if len(shape) != 2 or shape[1] != len(self.years):
            raise ValueError("series arrays must have shape (n_paths, n_years)")
        for name, values in self.series.items():
            if values.shape != shape:
                raise ValueError(f"series `{name}` has shape {values.shape}, expected {shape}")

    @property
    def annual(self) -> FloatArray:
        return self.series[self.primary]

    @property
    def n_paths(self) -> int:
        return int(self.annual.shape[0])

    @property
    def n_years(self) -> int:
        return int(self.annual.shape[1])

    def get(self, name: str | None = None) -> FloatArray:
        key = name or self.primary
        if key not in self.series:
            raise KeyError(f"unknown scenario series `{key}` (available: {sorted(self.series)})")
        return self.series[key]

    def with_primary(self, name: str) -> ScenarioSet:
        return replace(self, primary=name)

    def annual_table(self, series: str | None = None) -> pd.DataFrame:
        """Per-year mean, standard deviation and exceedance levels (P90 = 10th percentile)."""
        values = self.get(series)
        rows = []
        for j, year in enumerate(self.years):
            column = values[:, j]
            levels = exceedance_levels(column)
            rows.append(
                {
                    "year": year,
                    "mean": float(column.mean()),
                    "std": float(column.std(ddof=0)),
                    "p90": levels["p90"],
                    "p50": levels["p50"],
                    "p10": levels["p10"],
                    "saturation_multiplier_mean": float(self.saturation_multiplier[:, j].mean()),
                    "fleet_mw_mean": float(self.fleet_mw[:, j].mean()) if self.fleet_mw is not None else None,
                }
            )
        return pd.DataFrame(rows)

    def regime_shares(self) -> dict[str, float]:
        return {name: float(np.mean(self.regime_index == i)) for i, name in enumerate(self.regime_names)}

    def summary(self, series: str | None = None) -> dict[str, Any]:
        values = self.get(series)
        tenor_mean = values.mean(axis=1)
        return {
            "series": series or self.primary,
            "units": "EUR per MW per year",
            "percentile_convention": "exceedance: P90 is exceeded with 90 % probability (10th percentile)",
            "years": list(self.years),
            "n_paths": self.n_paths,
            "mean_annual": float(values.mean()),
            "std_annual": float(values.std(ddof=0)),
            "annual_pooled": exceedance_levels(values),
            "tenor_average": {"mean": float(tenor_mean.mean()), **exceedance_levels(tenor_mean)},
            "prob_negative_year": float(np.mean(values < 0.0)),
            "regime_shares": self.regime_shares(),
            "by_year": self.annual_table(series).to_dict(orient="records"),
            "config": self.config,
            "diagnostics": self.diagnostics,
        }

    def to_frame(self) -> pd.DataFrame:
        """Long table: one row per (path, year) with regime, fleet, multipliers and every series."""
        n_paths, n_years = self.n_paths, self.n_years
        frame = pd.DataFrame(
            {
                "path": np.repeat(np.arange(n_paths), n_years),
                "year": np.tile(np.asarray(self.years), n_paths),
                "regime": np.asarray(self.regime_names, dtype=object)[self.regime_index.ravel()],
                "fleet_mw": self.fleet_mw.ravel() if self.fleet_mw is not None else np.nan,
                "saturation_multiplier": self.saturation_multiplier.ravel(),
                "skill_multiplier": np.tile(self.skill_multiplier, n_paths),
                "fade_multiplier": np.tile(self.fade_multiplier, n_paths),
                "level_multiplier": self.level_multiplier.ravel(),
            }
        )
        for name, values in self.series.items():
            frame[name] = values.ravel()
        return frame

    @classmethod
    def from_annual(
        cls,
        annual: ArrayLike | Mapping[str, ArrayLike],
        *,
        years: Sequence[int] | None = None,
        start_year: int = 2027,
        primary: str | None = None,
        source: str = "external annual revenue paths",
    ) -> ScenarioSet:
        """Wrap externally generated annual paths (shape ``(n_paths, n_years)``, EUR/MW/year)."""
        raw = {"asset": annual} if not isinstance(annual, Mapping) else dict(annual)
        series = {name: np.atleast_2d(np.asarray(values, dtype=float)) for name, values in raw.items()}
        first = next(iter(series))
        n_paths, n_years = series[first].shape
        year_tuple = tuple(years) if years is not None else tuple(range(start_year, start_year + n_years))
        if len(year_tuple) != n_years:
            raise ValueError("years must match the number of columns")
        ones = np.ones((n_paths, n_years), dtype=float)
        days = np.asarray([366 if _is_leap(y) else 365 for y in year_tuple], dtype=np.int64)
        return cls(
            years=year_tuple,
            series=series,
            primary=primary or first,
            base_series={name: values.copy() for name, values in series.items()},
            regime_index=np.zeros((n_paths, n_years), dtype=np.int64),
            regime_names=("external",),
            fleet_mw=None,
            saturation_multiplier=ones,
            skill_multiplier=np.ones(n_years, dtype=float),
            fade_multiplier=np.ones(n_years, dtype=float),
            level_multiplier=ones.copy(),
            days_in_year=days,
            config={"source": source},
            diagnostics={},
        )


def _is_leap(year: int) -> bool:
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def _weight_matrix(
    names: Sequence[str], weights: Mapping[str, float | Sequence[float]] | None, n_years: int
) -> FloatArray:
    k = len(names)
    if weights is None:
        return np.full((n_years, k), 1.0 / k)
    unknown = sorted(set(weights) - set(names))
    if unknown:
        raise KeyError(f"weights for unknown regimes {unknown} (regimes: {list(names)})")
    matrix = np.zeros((n_years, k), dtype=float)
    for j, name in enumerate(names):
        value = weights.get(name, 0.0)
        if isinstance(value, int | float):
            matrix[:, j] = float(value)
        else:
            column = np.asarray(list(value), dtype=float)
            if column.shape != (n_years,):
                raise ValueError(f"per-year weights for `{name}` need {n_years} values")
            matrix[:, j] = column
    if np.any(matrix < 0.0):
        raise ValueError("regime weights must be non-negative")
    totals = matrix.sum(axis=1, keepdims=True)
    if np.any(totals <= 0.0):
        raise ValueError("regime weights must have a positive sum in every year")
    result: FloatArray = matrix / totals
    return result


def _draw_regimes(weights: FloatArray, persistence: float, n_paths: int, rng: np.random.Generator) -> IntArray:
    n_years, k = weights.shape
    regime = np.zeros((n_paths, n_years), dtype=np.int64)
    if k == 1:
        return regime
    regime[:, 0] = rng.choice(k, size=n_paths, p=weights[0])
    for y in range(1, n_years):
        keep = rng.random(n_paths) < persistence
        redraw = rng.choice(k, size=n_paths, p=weights[y])
        regime[:, y] = np.where(keep, regime[:, y - 1], redraw)
    return regime


def _level_shock(sigma: float, persistence: float, n_paths: int, n_years: int, rng: np.random.Generator) -> FloatArray:
    """Mean-one lognormal level multiplier with AR(1) persistence across years (ones when ``sigma`` is 0)."""
    z = rng.standard_normal((n_paths, n_years))
    if sigma == 0.0:
        return np.ones((n_paths, n_years), dtype=float)
    x = np.empty_like(z)
    x[:, 0] = z[:, 0]
    scale = np.sqrt(1.0 - persistence**2)
    for y in range(1, n_years):
        x[:, y] = persistence * x[:, y - 1] + scale * z[:, y]
    result: FloatArray = np.exp(sigma * x - 0.5 * sigma**2)
    return result


def generate_scenarios(
    history: pd.DataFrame | Mapping[str, pd.DataFrame],
    config: ScenarioConfig | None = None,
    *,
    weights: Mapping[str, float | Sequence[float]] | None = None,
    value_column: str = "pnl_eur_per_mw",
    min_coverage: float = 0.98,
    primary: str | None = None,
) -> ScenarioSet:
    """Simulate annual revenue paths from daily histories.

    ``history`` is one daily table / panel (single regime ``base``) or a mapping ``regime name -> daily table or
    panel`` (e.g. ``{"current": ..., "new2026": ...}``); all regimes must carry the same series columns.
    ``weights`` gives regime probabilities, either one number per regime or a per-year sequence (the draw
    probabilities; with ``regime_persistence`` > 0 a path keeps its previous regime with that probability).
    """
    cfg = config or ScenarioConfig()
    raw = {"base": history} if isinstance(history, pd.DataFrame) else dict(history)
    if not raw:
        raise ValueError("need at least one regime history")
    panels = {
        name: as_panel(frame, value_column=value_column, min_coverage=min_coverage) for name, frame in raw.items()
    }
    names = tuple(panels)
    series_names = tuple(str(c) for c in panels[names[0]].columns)
    for name in names[1:]:
        if set(map(str, panels[name].columns)) != set(series_names):
            raise ValueError(f"regime `{name}` has series {list(panels[name].columns)}, expected {list(series_names)}")
        panels[name] = panels[name].loc[:, list(series_names)]

    years = tuple(range(cfg.start_year, cfg.start_year + cfg.n_years))
    calendar = pd.date_range(f"{years[0]}-01-01", f"{years[-1]}-12-31", freq="D")
    year_of_day = np.asarray(calendar.year - years[0], dtype=np.int64)
    days_in_year = np.bincount(year_of_day, minlength=cfg.n_years).astype(np.int64)

    children = np.random.SeedSequence(cfg.seed).spawn(len(names) + 3)
    regime_rng = np.random.default_rng(children[0])
    fleet_rng = np.random.default_rng(children[1])
    level_rng = np.random.default_rng(children[2])

    base_by_regime: dict[str, FloatArray] = {}
    regime_diagnostics: dict[str, Any] = {}
    for i, name in enumerate(names):
        sampler = _HistorySampler(panels[name], cfg.bootstrap)
        annual, diagnostics = sampler.simulate(
            calendar, year_of_day, cfg.n_years, cfg.n_paths, np.random.default_rng(children[3 + i])
        )
        index = pd.DatetimeIndex(panels[name].index)
        diagnostics.update(
            {
                "history_first_day": index[0].date().isoformat(),
                "history_last_day": index[-1].date().isoformat(),
                "history_mean_daily": {s: float(panels[name][s].mean()) for s in series_names},
                "history_annualised": {s: float(panels[name][s].mean() * 365.25) for s in series_names},
            }
        )
        base_by_regime[name] = annual
        regime_diagnostics[name] = diagnostics

    weight_matrix = _weight_matrix(names, weights, cfg.n_years)
    regime_index = _draw_regimes(weight_matrix, cfg.regime_persistence, cfg.n_paths, regime_rng)
    base = np.zeros((cfg.n_paths, cfg.n_years, len(series_names)), dtype=float)
    for i, name in enumerate(names):
        mask = regime_index == i
        base[mask] = base_by_regime[name][mask]

    saturation = np.ones((cfg.n_paths, cfg.n_years), dtype=float)
    fleet: FloatArray | None = None
    if cfg.saturation is not None:
        fleet = cfg.saturation.fleet.sample(years, cfg.n_paths, fleet_rng)
        for i, name in enumerate(names):
            multiplier = cfg.saturation.curve_for(name).multiplier(fleet, cfg.saturation.reference_fleet_mw)
            saturation = np.where(regime_index == i, multiplier, saturation)
    skill = cfg.skill.multiplier(cfg.n_years) if cfg.skill is not None else np.ones(cfg.n_years, dtype=float)
    fade = (1.0 - cfg.capacity_fade_per_year) ** np.arange(cfg.n_years, dtype=float)
    level = _level_shock(cfg.level_log_sigma, cfg.level_persistence, cfg.n_paths, cfg.n_years, level_rng)
    common = skill[None, :] * fade[None, :] * cfg.availability_factor * level

    series: dict[str, FloatArray] = {}
    base_series: dict[str, FloatArray] = {}
    for j, name in enumerate(series_names):
        apply = cfg.saturation is not None and (cfg.saturation.series is None or name in cfg.saturation.series)
        multiplier = common * saturation if apply else common
        base_series[name] = base[:, :, j].copy()
        series[name] = base[:, :, j] * multiplier

    record = cfg.to_dict()
    record.update(
        {
            "value_column": value_column,
            "min_coverage": min_coverage,
            "regimes": list(names),
            "regime_weights_by_year": {name: weight_matrix[:, i].tolist() for i, name in enumerate(names)},
            "series": list(series_names),
        }
    )
    return ScenarioSet(
        years=years,
        series=series,
        primary=primary or series_names[0],
        base_series=base_series,
        regime_index=regime_index,
        regime_names=names,
        fleet_mw=fleet,
        saturation_multiplier=saturation,
        skill_multiplier=skill,
        fade_multiplier=fade,
        level_multiplier=level,
        days_in_year=days_in_year,
        config=record,
        diagnostics={"regimes": regime_diagnostics},
    )
