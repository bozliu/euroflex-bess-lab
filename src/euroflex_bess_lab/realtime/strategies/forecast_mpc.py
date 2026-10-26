"""Forecast-driven rolling MPC for minute-level implicit balancing.

At every minute the strategy re-optimises the energy of the *rest of the current quarter-hour* against the value
of the energy it leaves in the battery for the next ``horizon_qh`` quarter-hours:

1. ``reset`` asks a forecaster (:class:`~euroflex_bess_lab.realtime.forecasting.SupportsForecast`) for per-minute
   distributions of the final price of the current and the next quarter-hour (computed from visible data only).
2. A stochastic dynamic programme on a state-of-charge grid gives the value-to-go ``V_1(s)`` at the start of the
   next quarter-hour. Stage 1 uses the model's next-quarter-hour distribution (refreshed at ``replan_minutes``);
   stages 2..H use same-time-of-day price climatology from *published* prices; a terminal value prices the energy
   left at the horizon at the trailing mean price. Because the DP takes expectations over price scenarios, it
   values the option of keeping energy for price spikes.
3. ``decide`` solves ``max_e  pi_d e+ - pi_c e- - deg e+ + V_1(s - e+/eta_d + eta_c e-)`` over the remaining
   energy ``|e| <= P * minutes_left / 60`` in closed form (walk along the piecewise-linear ``V_1``) and spreads it
   over the remaining minutes. ``pi_d``/``pi_c`` are the forecast mean (risk-neutral) or risk-adjusted prices:
   a quantile filter (discharge on a low quantile, charge on a high quantile) or a mean-CVaR blend.

The decision for minute ``t`` only touches forecast rows ``<= t`` and history prices below ``visible_qh_end``, so
:func:`~euroflex_bess_lab.realtime.check_no_lookahead` audits the whole pipeline (features, models, DP, decision).
"""

from __future__ import annotations

import math
import weakref
from dataclasses import dataclass, field
from typing import Any, Literal

import numpy as np
from numpy.typing import NDArray

from ..forecasting.base import DiscreteForecast, ForecastFrame, SupportsForecast
from ..forecasting.climatology import slot_climatology, trailing_mean
from ..forecasting.distribution import distribution_mean, tail_mean, weighted_quantile
from ..replay import DecisionContext, MarketData
from ..types import MINUTES_PER_QH, QH_HOURS, BatteryParams, PublicationModel
from .base import BaseStrategy

FloatArray = NDArray[np.float64]
F32 = NDArray[np.float32]


@dataclass(frozen=True)
class MPCConfig:
    """Settings of the forecast MPC (fixed a priori; see ``docs/realtime_strategies.md``)."""

    horizon_qh: int = 16
    replan_minutes: tuple[int, ...] = (0, 5, 10)
    grid_steps_per_qh: int = 6
    risk: Literal["neutral", "quantile", "cvar"] = "neutral"
    risk_quantile: float = 0.3
    cvar_alpha: float = 0.2
    cvar_weight: float = 0.5
    climatology_days: int = 28
    climatology_neighbours: int = 1
    terminal_days: int = 7
    discount_per_qh: float = 0.995
    allow_negative_price_discharge: bool = False
    mean_match_next: bool = True
    chunk_rows: int = 2048

    def __post_init__(self) -> None:
        if not 1 <= self.horizon_qh <= 90:
            raise ValueError("horizon_qh must be in [1, 90] (climatology must stay in the published past)")
        if not self.replan_minutes or any(not 0 <= m < MINUTES_PER_QH for m in self.replan_minutes):
            raise ValueError("replan_minutes must be minute offsets inside the quarter-hour")
        if not 0.0 < self.risk_quantile < 0.5:
            raise ValueError("risk_quantile must be in (0, 0.5)")
        if not 0.0 < self.discount_per_qh <= 1.0:
            raise ValueError("discount_per_qh must be in (0, 1]")


# --------------------------------------------------------------------------------------------------------------
# state-of-charge grid and the Bellman stage


@dataclass(frozen=True)
class _Band:
    lo: int
    frac: float
    energy_mwh: float


@dataclass(frozen=True)
class SocGrid:
    """Uniform grid over ``[soc_min, soc_max]`` and the feasible one-quarter-hour transitions.

    Integer bands move ``b`` grid steps; two fractional bands represent exactly full power in each direction
    (linear interpolation between grid points), so the DP does not understate the power limit.
    """

    levels: FloatArray
    step: float
    soc_min: float
    bands: tuple[_Band, ...]

    @property
    def n(self) -> int:
        return len(self.levels)

    @classmethod
    def build(cls, battery: BatteryParams, steps_per_qh: int = 6) -> SocGrid:
        p = battery.max_power_mw
        charge_stored = p * QH_HOURS * battery.charge_efficiency
        discharge_stored = p * QH_HOURS / battery.discharge_efficiency
        step0 = charge_stored / max(steps_per_qh, 1)
        n = max(3, int(math.floor(battery.usable_energy_mwh / step0 + 1e-9)) + 1)
        step = battery.usable_energy_mwh / (n - 1)
        levels = np.asarray(battery.soc_min_mwh + step * np.arange(n), dtype=np.float64)
        bands: list[_Band] = []
        k_d = int(math.floor(discharge_stored / step + 1e-9))
        k_c = int(math.floor(charge_stored / step + 1e-9))
        for b in range(-k_d, k_c + 1):
            if b < 0:
                energy = -b * step * battery.discharge_efficiency
            elif b > 0:
                energy = -b * step / battery.charge_efficiency
            else:
                energy = 0.0
            bands.append(_Band(b, 0.0, energy))
        full_d = discharge_stored / step
        if full_d - k_d > 1e-6:
            lo = -k_d - 1
            frac = 1.0 - (full_d - k_d)
            bands.append(_Band(lo, frac, p * QH_HOURS))
        full_c = charge_stored / step
        if full_c - k_c > 1e-6:
            bands.append(_Band(k_c, full_c - k_c, -p * QH_HOURS))
        return cls(levels=levels, step=step, soc_min=battery.soc_min_mwh, bands=tuple(bands))


def bellman_stage(
    v_next: F32,
    prices: F32,
    probs: F32,
    grid: SocGrid,
    degradation_eur_per_mwh: float,
    discount: float = 1.0,
) -> F32:
    """``V(i) = sum_s probs[s] * max_band (price[s] * e_band - deg * e_band+ + discount * V_next(i + band))``.

    ``discount < 1`` is a time preference per quarter-hour: with equal expected prices it acts now rather than
    later (without it, a long run of identical prices - e.g. hours at the -15,000 EUR/MWh floor - can make the
    policy keep "room for the next quarter-hour" forever and never cash it in).
    """
    if discount != 1.0:
        v_next = (v_next * np.float32(discount)).astype(np.float32)
    n_rows, n_grid = v_next.shape
    bad = ~np.isfinite(prices)
    price = np.where(bad, 0.0, prices).astype(np.float32)
    prob = np.where(bad, 0.0, probs).astype(np.float32)
    total = prob.sum(axis=1, keepdims=True)
    prob = np.where(total > 0, prob / np.where(total > 0, total, 1.0), 0.0).astype(np.float32)
    n_s = price.shape[1]
    best = np.full((n_rows, n_s, n_grid), -np.inf, dtype=np.float32)
    for band in grid.bands:
        extra = 1 if band.frac > 0.0 else 0
        i0 = max(0, -band.lo)
        i1 = min(n_grid, n_grid - band.lo - extra)
        if i1 <= i0:
            continue
        target = v_next[:, i0 + band.lo : i1 + band.lo]
        if extra:
            upper = v_next[:, i0 + band.lo + 1 : i1 + band.lo + 1]
            target = (1.0 - band.frac) * target + band.frac * upper
        gain = price * np.float32(band.energy_mwh) - np.float32(degradation_eur_per_mwh * max(band.energy_mwh, 0.0))
        cand = gain[:, :, None] + target[:, None, :]
        sub = best[:, :, i0:i1]
        np.maximum(sub, cand, out=sub)
    value: F32 = np.einsum("ns,nsi->ni", prob, best).astype(np.float32)
    # rows without any usable price keep the continuation value (idle)
    empty = total[:, 0] <= 0
    if empty.any():
        value[empty] = v_next[empty]
    return value


def remaining_qh_energy(
    soc_mwh: float,
    slopes: F32,
    grid: SocGrid,
    battery: BatteryParams,
    price_discharge: float,
    price_charge: float,
    minutes_left: int,
    allow_negative_price_discharge: bool = False,
) -> float:
    """Optimal net energy (MWh AC, + = discharge) for the rest of the quarter-hour given ``V_1`` slopes.

    By default the strategy never pays to inject (no discharge when the discharge price is <= 0). Discharging at a
    negative price only pays as the first leg of a "make room, then charge at an even lower price" trade; with a
    one-step forecast horizon a rolling plan can keep re-opening that first leg without ever closing it (observed
    in the 6 April 2026 event, five quarter-hours at the -15,000 EUR/MWh floor).
    """
    e_max = battery.max_power_mw * minutes_left / 60.0
    eta_d, eta_c = battery.discharge_efficiency, battery.charge_efficiency
    step = grid.step
    n_seg = len(slopes)
    pos = (soc_mwh - grid.soc_min) / step
    # discharge: value per stored MWh = (price - deg) * eta_d against the water value (left slope)
    unit_d = (price_discharge - battery.degradation_eur_per_mwh) * eta_d
    budget_d = e_max / eta_d
    used_d = gain_d = 0.0
    x = pos
    if price_discharge <= 0.0 and not allow_negative_price_discharge:
        x = 0.0  # skip the discharge walk
    while used_d < budget_d - 1e-12 and x > 1e-9:
        i = min(max(math.ceil(x) - 1, 0), n_seg - 1)
        marginal = unit_d - float(slopes[i])
        if marginal <= 0.0:
            break
        take = min((x - i) * step, budget_d - used_d)
        if take <= 1e-15:
            x = float(i)
            continue
        used_d += take
        gain_d += marginal * take
        x -= take / step
    # charge: cost per stored MWh = price / eta_c against the water value (right slope)
    unit_c = price_charge / eta_c
    budget_c = e_max * eta_c
    used_c = gain_c = 0.0
    x = pos
    while used_c < budget_c - 1e-12 and x < n_seg - 1e-9:
        i = min(max(math.floor(x), 0), n_seg - 1)
        marginal = float(slopes[i]) - unit_c
        if marginal <= 0.0:
            break
        take = min((i + 1 - x) * step, budget_c - used_c)
        if take <= 1e-15:
            x = float(i + 1)
            continue
        used_c += take
        gain_c += marginal * take
        x += take / step
    if gain_d > gain_c and gain_d > 0.0:
        return used_d * eta_d
    if gain_c > 0.0:
        return -used_c / eta_c
    return 0.0


# --------------------------------------------------------------------------------------------------------------
# plan precomputation


def risk_prices(current: DiscreteForecast, config: MPCConfig) -> tuple[FloatArray, FloatArray]:
    """Prices used for discharge (long) and charge (short) decisions in the current quarter-hour."""
    mean = np.asarray(current.mean, dtype=float)
    if config.risk == "neutral":
        return mean, mean
    if config.risk == "quantile":
        tau = config.risk_quantile
        return (
            weighted_quantile(current.values, current.weights, tau),
            weighted_quantile(current.values, current.weights, 1.0 - tau),
        )
    lam, alpha = config.cvar_weight, config.cvar_alpha
    low = tail_mean(current.values, current.weights, alpha, lower=True)
    high = tail_mean(current.values, current.weights, alpha, lower=False)
    return (1.0 - lam) * mean + lam * low, (1.0 - lam) * mean + lam * high


def _terminal_inputs(
    frame: ForecastFrame,
    market: MarketData,
    battery: BatteryParams,
    publication: PublicationModel,
    config: MPCConfig,
) -> tuple[F32, F32, FloatArray]:
    """Slot climatology (``n_qh + H + 2`` rows) and the terminal value of stored energy per quarter-hour."""
    n_qh = market.n_qh
    deg = battery.degradation_eur_per_mwh
    history = np.asarray(frame.history_prices, dtype=float)
    extended = np.concatenate([history, np.full(config.horizon_qh + 2, np.nan)])
    clim, clim_w = slot_climatology(extended, days=config.climatology_days, neighbours=config.climatology_neighbours)
    # trailing mean of published prices at the last quarter-hour visible at each quarter-hour start
    trail = trailing_mean(history, 96 * config.terminal_days)
    k = np.arange(n_qh, dtype=np.int64)
    qh_start = k * MINUTES_PER_QH
    k_last = np.maximum((qh_start - publication.qh_final_lag_minutes) // MINUTES_PER_QH, 0) - 1
    p_term = np.where(k_last >= 0, trail[np.clip(k_last, 0, max(n_qh - 1, 0))], np.nan)
    # causal fallback before any price is published: the day-ahead price if it is already public, else zero
    da_public = np.where(market.da_visible_from_minute <= qh_start, market.da_price, np.nan)
    p_term = np.where(np.isfinite(p_term), p_term, np.nan_to_num(da_public))
    v_term = battery.discharge_efficiency * np.maximum(p_term - deg, 0.0)
    return clim, clim_w, v_term


def _value_to_go(
    qhs: NDArray[np.int64],
    clim: F32,
    clim_w: F32,
    v_term: FloatArray,
    grid: SocGrid,
    degradation: float,
    config: MPCConfig,
) -> F32:
    """Value of stored energy at the start of quarter-hour ``k + 2`` for each ``k`` in ``qhs``: stages
    ``k + 2 .. k + H`` on slot climatology, then the terminal value."""
    rel = (grid.levels - grid.soc_min).astype(np.float32)
    out = np.empty((len(qhs), grid.n), dtype=np.float32)
    clim_probs = np.broadcast_to(clim_w, (1, len(clim_w)))
    for start in range(0, len(qhs), config.chunk_rows):
        kk = qhs[start : start + config.chunk_rows]
        value = (v_term[kk, None] * rel[None, :]).astype(np.float32)
        probs = np.repeat(clim_probs, len(kk), axis=0)
        for h in range(config.horizon_qh, 1, -1):
            value = bellman_stage(value, clim[kk + h], probs, grid, degradation, config.discount_per_qh)
        out[start : start + len(kk)] = value
    return out


def _stage_one_slopes(
    rows: NDArray[np.int64],
    v2_rows: F32,
    nxt: DiscreteForecast,
    grid: SocGrid,
    degradation: float,
    config: MPCConfig,
) -> tuple[F32, NDArray[np.bool_]]:
    """Slopes of ``V_1`` at the re-plan ``rows`` from the model's next-quarter-hour distribution."""
    slopes = np.zeros((len(rows), grid.n - 1), dtype=np.float32)
    usable = np.zeros(len(rows), dtype=bool)
    for start in range(0, len(rows), config.chunk_rows):
        rr = rows[start : start + config.chunk_rows]
        values = nxt.values[rr].astype(np.float32)
        weights = nxt.weights[rr].astype(np.float32)
        ok = np.isfinite(values).all(axis=1) & np.isfinite(weights).all(axis=1)
        if config.mean_match_next:
            current_mean = distribution_mean(values, weights)
            shift = np.where(np.isfinite(nxt.mean[rr]), nxt.mean[rr] - current_mean, 0.0)
            values = values + np.nan_to_num(shift)[:, None].astype(np.float32)
        v1 = bellman_stage(v2_rows[start : start + len(rr)], values, weights, grid, degradation, config.discount_per_qh)
        # the current quarter-hour's remaining energy is weighed against the discounted value-to-go
        slopes[start : start + len(rr)] = np.diff(v1, axis=1) * np.float32(config.discount_per_qh / grid.step)
        usable[start : start + len(rr)] = ok
    return slopes, usable


def _plan_rows(replan_rows: NDArray[np.int64], usable: NDArray[np.bool_], n_min: int) -> NDArray[np.int64]:
    """For every minute, the index of the latest usable re-plan row of its own quarter-hour (-1 if none)."""
    row_index = np.full(n_min, -1, dtype=np.int64)
    row_index[replan_rows[usable]] = np.flatnonzero(usable)
    by_qh = row_index.reshape(-1, MINUTES_PER_QH)
    filled = np.maximum.accumulate(np.where(by_qh >= 0, np.arange(MINUTES_PER_QH)[None, :], -1), axis=1)
    take = np.take_along_axis(by_qh, np.maximum(filled, 0), axis=1)
    out: NDArray[np.int64] = np.where(filled >= 0, take, -1).reshape(-1)
    return out


@dataclass
class MPCPlan:
    """Precomputed decision inputs for every minute: risk prices and the slopes of ``V_1`` at re-plan rows."""

    grid: SocGrid
    price_discharge: FloatArray
    price_charge: FloatArray
    plan_row: NDArray[np.int64]
    slopes: F32
    replan_rows: NDArray[np.int64] = field(default_factory=lambda: np.zeros(0, dtype=np.int64))
    usable: NDArray[np.bool_] = field(default_factory=lambda: np.zeros(0, dtype=bool))
    meta: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def replan_offsets(config: MPCConfig) -> NDArray[np.int64]:
        return np.array(sorted(set(config.replan_minutes)), dtype=np.int64)

    @classmethod
    def build(
        cls,
        frame: ForecastFrame,
        market: MarketData,
        battery: BatteryParams,
        publication: PublicationModel,
        config: MPCConfig,
    ) -> MPCPlan:
        n_min, n_qh = market.n_minutes, market.n_qh
        grid = SocGrid.build(battery, config.grid_steps_per_qh)
        deg = battery.degradation_eur_per_mwh
        clim, clim_w, v_term = _terminal_inputs(frame, market, battery, publication, config)
        k = np.arange(n_qh, dtype=np.int64)
        v2 = _value_to_go(k, clim, clim_w, v_term, grid, deg, config)
        offsets = cls.replan_offsets(config)
        rows = (k[:, None] * MINUTES_PER_QH + offsets[None, :]).reshape(-1)
        rows = rows[rows < n_min]
        slopes, usable = _stage_one_slopes(rows, v2[rows // MINUTES_PER_QH], frame.next, grid, deg, config)
        price_d, price_c = risk_prices(frame.current, config)
        cur_ok = frame.current.available()
        return cls(
            grid=grid,
            price_discharge=np.where(cur_ok, price_d, np.nan),
            price_charge=np.where(cur_ok, price_c, np.nan),
            plan_row=_plan_rows(rows, usable, n_min),
            slopes=slopes,
            replan_rows=rows,
            usable=usable,
            meta={"grid_points": grid.n, "plan_rows": len(rows), "coverage": float(np.mean(cur_ok))},
        )

    def update(
        self,
        frame: ForecastFrame,
        market: MarketData,
        battery: BatteryParams,
        publication: PublicationModel,
        config: MPCConfig,
        rows: NDArray[np.int64],
    ) -> None:
        """Re-plan in place for the decision ``rows`` (live use): their risk prices, the value-to-go of their
        quarter-hours and the re-plan rows of those quarter-hours up to the latest decision row. ``frame`` needs
        forecasts for ``rows`` and those re-plan rows; everything else is left as it was."""
        rows = np.unique(np.asarray(rows, dtype=np.int64))
        if len(rows) == 0:
            return
        deg = battery.degradation_eur_per_mwh
        price_d, price_c = risk_prices(frame.current, config)
        ok = frame.current.available()[rows]
        self.price_discharge[rows] = np.where(ok, price_d[rows], np.nan)
        self.price_charge[rows] = np.where(ok, price_c[rows], np.nan)
        qhs = np.unique(rows // MINUTES_PER_QH)
        offsets = self.replan_offsets(config)
        latest = int(rows.max())
        index = (qhs[:, None] * len(offsets) + np.arange(len(offsets))[None, :]).reshape(-1)
        index = index[index < len(self.replan_rows)]
        index = index[self.replan_rows[index] <= latest]
        if len(index) == 0:
            return
        replan = self.replan_rows[index]
        clim, clim_w, v_term = _terminal_inputs(frame, market, battery, publication, config)
        v2 = _value_to_go(np.unique(replan // MINUTES_PER_QH), clim, clim_w, v_term, self.grid, deg, config)
        lookup = {int(k): i for i, k in enumerate(np.unique(replan // MINUTES_PER_QH))}
        v2_rows = v2[[lookup[int(k)] for k in replan // MINUTES_PER_QH]]
        slopes, usable = _stage_one_slopes(replan, v2_rows, frame.next, self.grid, deg, config)
        self.slopes[index] = slopes
        self.usable[index] = usable
        minutes = (qhs[:, None] * MINUTES_PER_QH + np.arange(MINUTES_PER_QH)[None, :]).reshape(-1)
        minutes = minutes[minutes < len(self.plan_row)]
        self.plan_row[minutes] = _plan_rows(self.replan_rows, self.usable, len(self.plan_row))[minutes]


# --------------------------------------------------------------------------------------------------------------
# strategy


class CachedForecaster:
    """Memoises :meth:`predict` per market object (a few entries) so variants can share forecasts.

    A perturbed copy of a market (as built by the look-ahead audit) is a different object and is recomputed.
    """

    def __init__(self, forecaster: SupportsForecast, max_entries: int = 3) -> None:
        self.forecaster = forecaster
        self.name = forecaster.name
        self.max_entries = max_entries
        self._cache: list[tuple[weakref.ref[MarketData], PublicationModel, ForecastFrame]] = []

    def predict(
        self,
        market: MarketData,
        publication: PublicationModel | None = None,
        rows: NDArray[np.int64] | None = None,
    ) -> ForecastFrame:
        publication = publication or PublicationModel()
        if rows is not None:  # partial (live) predictions are never cached
            return self.forecaster.predict(market, publication, rows=rows)
        for ref, pub, frame in self._cache:
            if ref() is market and pub == publication:
                return frame
        frame = self.forecaster.predict(market, publication)
        self._cache = [entry for entry in self._cache if entry[0]() is not None][-(self.max_entries - 1) :]
        self._cache.append((weakref.ref(market), publication, frame))
        return frame


class ForecastMPCStrategy(BaseStrategy):
    """Rolling stochastic MPC on forecast price distributions (see module docstring)."""

    def __init__(
        self,
        forecaster: SupportsForecast,
        config: MPCConfig | None = None,
        name: str = "forecast_mpc",
    ) -> None:
        self.forecaster = forecaster
        self.config = config or MPCConfig()
        self.name = name
        self.plan: MPCPlan | None = None

    def reset(self, ctx: DecisionContext) -> None:
        frame = self.forecaster.predict(ctx.market, ctx.publication)
        self.plan = MPCPlan.build(frame, ctx.market, ctx.battery, ctx.publication, self.config)
        plan = self.plan
        self._price_d = plan.price_discharge.tolist()
        self._price_c = plan.price_charge.tolist()
        self._plan_row = plan.plan_row.tolist()

    def refresh(self, ctx: DecisionContext) -> None:
        """Live re-planning, called before each decision on a market window that is filled in place.

        Recomputes only what the decision at ``ctx.t`` uses - the forecast at ``t`` and at the re-plan minutes of
        the current quarter-hour, and the value-to-go of that quarter-hour - from the data visible now. In a
        backtest (all data present) this reproduces the plan built by :meth:`reset` exactly.
        """
        plan = self.plan
        if plan is None or len(self._price_d) != ctx.market.n_minutes:
            self.reset(ctx)
            return
        t, start = ctx.t, ctx.qh_start_minute
        replan = [start + m for m in self.config.replan_minutes if start + m <= t]
        rows = np.unique(np.asarray([t, *replan], dtype=np.int64))
        frame = self.forecaster.predict(ctx.market, ctx.publication, rows=rows)
        plan.update(frame, ctx.market, ctx.battery, ctx.publication, self.config, rows)
        for row in rows.tolist():
            self._price_d[row] = float(plan.price_discharge[row])
            self._price_c[row] = float(plan.price_charge[row])
        stop = min(start + MINUTES_PER_QH, len(self._plan_row))
        self._plan_row[start:stop] = plan.plan_row[start:stop].tolist()

    def decide(self, ctx: DecisionContext) -> float:
        plan = self.plan
        if plan is None:
            return 0.0
        t = ctx.t
        row = self._plan_row[t]
        price_d = self._price_d[t]
        price_c = self._price_c[t]
        if row < 0 or price_d != price_d or price_c != price_c:
            return 0.0
        minutes_left = MINUTES_PER_QH - ctx.minute_in_qh
        energy = remaining_qh_energy(
            ctx.soc_mwh,
            plan.slopes[row],
            plan.grid,
            ctx.battery,
            price_d,
            price_c,
            minutes_left,
            self.config.allow_negative_price_discharge,
        )
        return energy * 60.0 / minutes_left


# --------------------------------------------------------------------------------------------------------------
# live (paper) trading


def live_forecast_mpc(
    risk: str = "quantile",
    models_dir: str | None = None,
    name: str = "forecast_mpc",
) -> ForecastMPCStrategy:
    """Live factory: the current-formula price model fitted by the backtest's ``deploy`` stage.

    The model is trained on current-formula prices; after the formula switch use ``prism`` (SCALES-L), which
    follows the settlement mechanism. ``risk`` is ``quantile`` (default, the risk filter), ``neutral`` or ``cvar``.
    """
    from ..forecasting.deploy import load_forecaster

    config = MPCConfig() if risk == "neutral" else MPCConfig(risk=risk)  # type: ignore[arg-type]
    return ForecastMPCStrategy(CachedForecaster(load_forecaster("price_current", models_dir)), config, name=name)


#: Factories for ``euroflex live run --strategy <name>`` (discovered by ``realtime.live.strategies``).
LIVE_STRATEGIES = {"forecast-mpc": live_forecast_mpc}
