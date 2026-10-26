"""Impact-aware implicit-balancing policy: a threshold rule that prices the fleet's own impact.

:class:`ThresholdStrategy` (price mode) is a *price taker*: whenever the running price estimate crosses its band it
moves the whole fleet at full power. A fleet of ``Q`` MW following it moves the system imbalance by ``Q`` and, if
``Q > |SI|``, flips the price branch against itself (herding / overshoot).

:class:`ImpactAwareThresholdStrategy` uses the same signal (running QH-to-date price of the mechanism), the same
bands (quantiles of recently *published* final prices) and the same no-look-ahead information, but chooses the
fleet's quarter-hour deviation ``q`` in ``[-Q, Q]`` that maximises

``J(q) = q * (P(q) - c(q))``,  ``c(q) = upper band if q > 0 (discharge) else lower band (charge)``,

where ``P(q) = signal + [pi(X_run, q) - pi(X_run, 0)]`` applies the impact operator to the *running* state ``X_run``
(the latest visible minute row; previous quarter-hour's final values for alpha). With no impact ``J`` is linear and
the rule reduces exactly to the threshold rule (bang-bang); with impact it is the monopolist's quantity
``P(q) + q P'(q) = c`` -- the fleet stops adding MW once the next MW would cost the fleet more on its whole volume
than it earns. Each unit then runs at ``q / Q`` of its power. The band acts as the opportunity value of stored
energy, as in the threshold rule (a heuristic, not an intertemporal optimum).

Running states: the current formula uses ODS133 cumulative SI / MIP / MDP / alpha; activated volumes are mapped
from the running SI with the historical median gross up / down activation per SI bin (:class:`VolumeFromSI`,
fitted on the training quarter-hours) because the ODS126 minute volumes are not on the same scale as the ODS127
quarter-hour volumes the curves were fitted on. The 2026 formula uses the ODS211 QH-to-date fields directly.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from typing import Any

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ..replay import DecisionContext, MarketData
from ..strategies.base import BaseStrategy
from ..strategies.threshold import ThresholdStrategy
from ..types import MINUTES_PER_QH, PublicationModel
from .curves import local_hour_regime
from .operators import CurrentResponseFit, NewFormulaResponseFit
from .response import (
    CurveDelta,
    assemble_current_price,
    counterfactual_marginal_prices,
    current_price,
    displace,
    new_formula_price,
)
from .state import CurrentState, new_running_state, previous_qh_final, structural_marginal_prices

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]

SIGNAL_FIELDS: dict[str, str] = {"current": "price_cum_eur_mwh", "new2026": "nf_imbalance_price_eur_mwh"}


def price_taking_threshold(mechanism: str, **kwargs: Any) -> ThresholdStrategy:
    """The replicated price-taking baseline: :class:`ThresholdStrategy` on the mechanism's running price."""
    params: dict[str, Any] = {"signal_field": SIGNAL_FIELDS[mechanism], "name": f"price_taking_threshold_{mechanism}"}
    params.update(kwargs)
    return ThresholdStrategy(**params)


@dataclass(frozen=True)
class VolumeFromSI:
    """Median gross upward / downward aFRR + mFRR activation per system-imbalance bin (QH history)."""

    si_grid: tuple[float, ...]
    up: tuple[float, ...]
    down: tuple[float, ...]

    @classmethod
    def fit(cls, si: FloatArray, up: FloatArray, down: FloatArray, *, bin_mw: float = 50.0) -> VolumeFromSI:
        ok = np.isfinite(si) & np.isfinite(up) & np.isfinite(down)
        s, u, d = si[ok], up[ok], down[ok]
        if len(s) < 20:
            grid = np.array([-1000.0, 0.0, 1000.0])
            return cls(tuple(grid), (1000.0, 0.0, 0.0), (0.0, 0.0, 1000.0))
        lo, hi = np.quantile(s, [0.005, 0.995])
        edges = np.arange(np.floor(lo / bin_mw) * bin_mw, np.ceil(hi / bin_mw) * bin_mw + bin_mw, bin_mw)
        centres, ups, downs = [], [], []
        for a, b in zip(edges[:-1], edges[1:], strict=True):
            m = (s >= a) & (s < b)
            if m.sum() >= 20:
                centres.append(0.5 * (a + b))
                ups.append(float(np.median(u[m])))
                downs.append(float(np.median(d[m])))
        if len(centres) < 2:
            return (
                cls.fit(si, up, down, bin_mw=bin_mw * 4) if bin_mw < 1000 else cls((-1.0, 1.0), (0.0, 0.0), (0.0, 0.0))
            )
        # enforce monotone shapes: up falls with SI, down rises with SI
        ups_arr = np.minimum.accumulate(np.asarray(ups))
        downs_arr = np.maximum.accumulate(np.asarray(downs))
        return cls(tuple(float(c) for c in centres), tuple(ups_arr.tolist()), tuple(downs_arr.tolist()))

    def __call__(self, si: FloatArray) -> tuple[FloatArray, FloatArray]:
        grid = np.asarray(self.si_grid)
        s = np.asarray(si, dtype=float)
        up = np.interp(s, grid, np.asarray(self.up))
        down = np.interp(s, grid, np.asarray(self.down))
        # beyond the grid follow the imbalance one-for-one
        up = np.where(s < grid[0], self.up[0] + (grid[0] - s), up)
        down = np.where(s > grid[-1], self.down[-1] + (s - grid[-1]), down)
        return np.where(np.isnan(s), np.nan, up), np.where(np.isnan(s), np.nan, down)


def current_running_state_from_si(
    market: MarketData, fit: CurrentResponseFit, volume_map: VolumeFromSI
) -> CurrentState:
    """Running current-formula state per minute row (row ``j`` uses minute rows ``<= j`` only)."""
    minute = market.minute
    n = market.n_minutes
    si = np.asarray(minute.get("si_cum_mw", np.full(n, np.nan)), dtype=float)
    up, down = volume_map(si)

    def get(name: str) -> FloatArray:
        return np.asarray(minute[name], dtype=float) if name in minute else np.full(n, np.nan)

    data = {
        name: get(name)
        for name in (
            "mip_eur_mwh",
            "mdp_eur_mwh",
            "floor_price_eur_mwh",
            "cap_price_eur_mwh",
            "afrr_price_up_eur_mwh",
            "afrr_price_down_eur_mwh",
        )
    }
    # running structural MIP / MDP (only the aFRR price is published per minute; mFRR is in the running MIP/MDP)
    mip, mdp, floor, cap = structural_marginal_prices(data, si)
    return CurrentState(
        si_mw=si,
        si_prev_mw=previous_qh_final(si),
        up_mw=up,
        down_mw=down,
        mip_eur_mwh=mip,
        mdp_eur_mwh=mdp,
        alpha_eur_mwh=get("alpha_eur_mwh"),
        regime=local_hour_regime(market.minute_times, timezone=market.timezone, edges_hours=fit.regime_edges_hours),
        curve_row=np.full(n, -1, dtype=np.int64),
        floor_eur_mwh=floor,
        cap_eur_mwh=cap,
    )


def threshold_bands(
    prices: FloatArray,
    n_qh: int,
    publication: PublicationModel,
    *,
    lookback_qh: int,
    upper_quantile: float,
    lower_quantile: float,
    min_spread_eur_mwh: float,
    min_history_qh: int,
    default_upper_eur_mwh: float,
    default_lower_eur_mwh: float,
) -> tuple[FloatArray, FloatArray]:
    """Per-QH ``(lower, upper)`` bands exactly as :class:`ThresholdStrategy` computes them at the QH's first minute."""
    series = pd.Series(np.asarray(prices, dtype=float))
    roll = series.rolling(lookback_qh, min_periods=1)
    q_up = roll.quantile(upper_quantile).to_numpy()
    q_lo = roll.quantile(lower_quantile).to_numpy()
    count = roll.count().to_numpy()
    upper = np.full(n_qh, default_upper_eur_mwh)
    lower = np.full(n_qh, default_lower_eur_mwh)
    for k in range(n_qh):
        end = publication.visible_qh_end(k * MINUTES_PER_QH)
        if end > 0 and count[end - 1] >= min_history_qh and count[end - 1] > 0:
            upper[k], lower[k] = q_up[end - 1], q_lo[end - 1]
    narrow = upper - lower < min_spread_eur_mwh
    mid = 0.5 * (upper + lower)
    upper = np.where(narrow, mid + 0.5 * min_spread_eur_mwh, upper)
    lower = np.where(narrow, mid - 0.5 * min_spread_eur_mwh, lower)
    return lower, upper


@dataclass
class ImpactAwareThresholdStrategy(BaseStrategy):
    """Threshold rule that internalises the fleet's own price impact (see module docstring).

    ``fit`` is the response model the policy *believes* (a :class:`CurrentResponseFit` or
    :class:`NewFormulaResponseFit`); ``fleet_mw`` the size of the fleet that follows the policy in lockstep (each unit
    of ``battery.power_mw``). ``impact_scale`` multiplies the believed price impact (0 = price taker, i.e. exactly
    the threshold rule; >1 or <1 for misspecification tests). ``volume_map`` is required for the current formula.
    """

    mechanism: str
    fit: CurrentResponseFit | NewFormulaResponseFit
    fleet_mw: float
    volume_map: VolumeFromSI | None = None
    lookback_qh: int = 96 * 14
    upper_quantile: float = 0.75
    lower_quantile: float = 0.25
    min_spread_eur_mwh: float = 20.0
    min_history_qh: int = 96
    default_upper_eur_mwh: float = 150.0
    default_lower_eur_mwh: float = 0.0
    grid_points: int = 21
    track_previous_deviation: bool = True
    impact_scale: float = 1.0
    name: str = "impact_aware_threshold"
    _signal: FloatArray = field(default_factory=lambda: np.zeros(0), init=False, repr=False)
    _lower: FloatArray = field(default_factory=lambda: np.zeros(0), init=False, repr=False)
    _upper: FloatArray = field(default_factory=lambda: np.zeros(0), init=False, repr=False)
    _grid_u: FloatArray = field(default_factory=lambda: np.zeros(0), init=False, repr=False)
    _delta: FloatArray | None = field(default=None, init=False, repr=False)
    _mip: FloatArray | None = field(default=None, init=False, repr=False)
    _mdp: FloatArray | None = field(default=None, init=False, repr=False)
    _state: CurrentState | None = field(default=None, init=False, repr=False)
    _acc_energy: float = field(default=0.0, init=False, repr=False)
    _acc_qh: int = field(default=-1, init=False, repr=False)
    _q_prev: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.mechanism not in SIGNAL_FIELDS:
            raise KeyError(f"unknown mechanism `{self.mechanism}`")
        if self.fleet_mw <= 0.0:
            raise ValueError("fleet_mw must be positive")
        if self.grid_points < 3 or self.grid_points % 2 == 0:
            raise ValueError("grid_points must be an odd number >= 3")
        if self.mechanism == "current" and not isinstance(self.fit, CurrentResponseFit):
            raise TypeError("current mechanism needs a CurrentResponseFit")
        if self.mechanism == "new2026" and not isinstance(self.fit, NewFormulaResponseFit):
            raise TypeError("new2026 mechanism needs a NewFormulaResponseFit")
        if self.mechanism == "current" and self.volume_map is None:
            raise ValueError("the current-formula policy needs a VolumeFromSI map")

    # -- precomputation (visible data only: row t uses minute rows <= t - minute_lag - 1) ------------------------

    def reset(self, ctx: DecisionContext) -> None:
        market = ctx.market
        n = market.n_minutes
        lag = ctx.publication.minute_lag
        signal_field = SIGNAL_FIELDS[self.mechanism]
        raw = np.asarray(market.require_minute(signal_field), dtype=float)
        rows = np.arange(n) - lag - 1
        valid = rows >= 0
        safe = np.clip(rows, 0, max(n - 1, 0))
        self._signal = np.where(valid, raw[safe], np.nan)
        self._lower, self._upper = threshold_bands(
            market.settlement(ctx.mechanism),
            market.n_qh,
            ctx.publication,
            lookback_qh=self.lookback_qh,
            upper_quantile=self.upper_quantile,
            lower_quantile=self.lower_quantile,
            min_spread_eur_mwh=self.min_spread_eur_mwh,
            min_history_qh=self.min_history_qh,
            default_upper_eur_mwh=self.default_upper_eur_mwh,
            default_lower_eur_mwh=self.default_lower_eur_mwh,
        )
        self._grid_u = np.linspace(-1.0, 1.0, self.grid_points)
        q_grid = self._grid_u * self.fleet_mw * ctx.battery.availability
        if self.mechanism == "new2026":
            assert isinstance(self.fit, NewFormulaResponseFit)
            state = new_running_state(market, regime_edges_hours=self.fit.regime_edges_hours).take(safe)
            q = np.broadcast_to(q_grid, (n, len(q_grid)))
            curves = self.fit.curves()
            with_q = new_formula_price(
                state, q, curves=curves, pass_through=self.fit.pass_through, displacement=self.fit.displacement
            )
            at_zero = new_formula_price(
                state,
                np.zeros(n),
                curves=curves,
                pass_through=self.fit.pass_through,
                displacement=self.fit.displacement,
            )
            delta = with_q - at_zero[:, None]
            self._delta = np.where(np.isfinite(delta), delta, 0.0)
        else:
            assert isinstance(self.fit, CurrentResponseFit) and self.volume_map is not None
            state_c = current_running_state_from_si(market, self.fit, self.volume_map).take(safe)
            self._state = state_c
            q = np.broadcast_to(q_grid, (n, len(q_grid)))
            if self.track_previous_deviation:
                # precompute the volume-driven marginal prices; alpha and the branch are applied in decide()
                u = state_c.up_mw[:, None]
                d = state_c.down_mw[:, None]
                u2, d2 = displace(u, d, q, self.fit.pass_through)
                self._mip, self._mdp = counterfactual_marginal_prices(
                    state_c, u2, d2, up=CurveDelta(self.fit.mip), down=CurveDelta(self.fit.mdp)
                )
            else:
                kwargs = {
                    "up": CurveDelta(self.fit.mip),
                    "down": CurveDelta(self.fit.mdp),
                    "alpha_model": self.fit.alpha,
                    "alpha_mode": self.fit.alpha_mode,
                    "pass_through": self.fit.pass_through,
                }
                with_q = current_price(state_c, q, **kwargs)  # type: ignore[arg-type]
                at_zero = current_price(state_c, np.zeros(n), **kwargs)  # type: ignore[arg-type]
                delta = with_q - at_zero[:, None]
                self._delta = np.where(np.isfinite(delta), delta, 0.0)
        self._acc_energy = 0.0
        self._acc_qh = -1
        self._q_prev = 0.0

    # -- decision ------------------------------------------------------------------------------------------------

    def _current_delta(self, t: int, q_grid: FloatArray) -> FloatArray:
        """Impact increment for the current formula with the fleet's own previous-QH deviation in alpha."""
        assert self._state is not None and self._mip is not None and self._mdp is not None
        assert isinstance(self.fit, CurrentResponseFit)
        st = self._state
        si, si_prev = st.si_mw[t], st.si_prev_mw[t]
        if not math.isfinite(si):
            return np.zeros_like(q_grid)
        prev = si if not math.isfinite(si_prev) else si_prev
        g = len(q_grid)
        zero_idx = g // 2
        si2 = np.concatenate([si + q_grid, [si]])
        prev2 = np.concatenate([np.full(g, prev + self._q_prev), [prev]])
        mip2 = np.concatenate([self._mip[t], [self._mip[t][zero_idx]]])
        mdp2 = np.concatenate([self._mdp[t], [self._mdp[t][zero_idx]]])
        prices = assemble_current_price(
            si2,
            prev2,
            mip2,
            mdp2,
            np.full(g + 1, st.floor_eur_mwh[t]),
            np.full(g + 1, st.cap_eur_mwh[t]),
            self.fit.alpha,
            alpha_mode=self.fit.alpha_mode,
            alpha_obs=np.full(g + 1, st.alpha_eur_mwh[t]),
            si=np.full(g + 1, si),
            si_prev=np.full(g + 1, prev),
        )
        delta = prices[:g] - prices[g]
        out: FloatArray = np.where(np.isfinite(delta), delta, 0.0)
        return out

    def decide(self, ctx: DecisionContext) -> float:
        t = ctx.t
        k = ctx.qh_index
        if t > 0:
            # own energy of the quarter-hour that minute t-1 belongs to (the fleet knows its own past actions)
            prev_qh = (t - 1) // MINUTES_PER_QH
            if prev_qh != self._acc_qh:
                self._acc_qh, self._acc_energy = prev_qh, 0.0
            self._acc_energy += ctx.last_power_mw / 60.0
            if ctx.minute_in_qh == 0 and self.track_previous_deviation:
                fleet_scale = self.fleet_mw / ctx.battery.power_mw
                self._q_prev = self._acc_energy / 0.25 * fleet_scale
        signal = self._signal[t]
        if not math.isfinite(signal):
            return 0.0
        lower, upper = self._lower[k], self._upper[k]
        q_grid = self._grid_u * self.fleet_mw * ctx.battery.availability
        if self._delta is not None:
            delta = self._delta[t]
        else:
            delta = self._current_delta(t, q_grid)
        price = signal + self.impact_scale * delta
        # ties at the band act, exactly as in ThresholdStrategy (signal >= upper discharges, <= lower charges)
        tie = 1e-9
        cost = np.where(q_grid > 0.0, upper - tie, np.where(q_grid < 0.0, lower + tie, 0.0))
        objective = q_grid * (price - cost)
        best = int(np.argmax(objective))
        if objective[best] <= 0.0:
            return 0.0
        return float(self._grid_u[best] * ctx.battery.max_power_mw)

    def with_fleet(self, fleet_mw: float) -> ImpactAwareThresholdStrategy:
        return replace(self, fleet_mw=fleet_mw)
