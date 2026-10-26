"""Reactive implicit-balancing baselines that act on the running quarter-hour signal."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Literal

import numpy as np

from ..replay import DecisionContext
from .base import BaseStrategy


@dataclass
class ThresholdStrategy(BaseStrategy):
    """Discharge when the running signal says the system is short, charge when it is long.

    ``mode="price"``: compare the running imbalance-price estimate of the current quarter-hour with adaptive
    bands taken from the quantiles of recently *published* final prices (recomputed once per quarter-hour).
    ``mode="si"``: compare the running system imbalance with fixed bands (discharge if SI <= -band, charge if
    SI >= +band); this rule does not depend on the price formula.

    When no minute of the current quarter-hour is visible yet the strategy can fall back to the latest visible
    value from the previous quarter-hour. Outside the bands it drifts back towards ``soc_target_frac``.
    """

    mode: Literal["price", "si"] = "price"
    signal_field: str = "price_cum_eur_mwh"
    lookback_qh: int = 96 * 14
    upper_quantile: float = 0.75
    lower_quantile: float = 0.25
    min_spread_eur_mwh: float = 20.0
    min_history_qh: int = 96
    default_upper_eur_mwh: float = 150.0
    default_lower_eur_mwh: float = 0.0
    si_band_mw: float = 150.0
    min_visible_minutes: int = 0
    fallback_to_previous_qh: bool = True
    soc_target_frac: float = 0.5
    rebalance_power_frac: float = 0.0
    name: str = "threshold"
    _cached_qh: int = field(default=-1, init=False, repr=False)
    _upper: float = field(default=math.inf, init=False, repr=False)
    _lower: float = field(default=-math.inf, init=False, repr=False)

    def reset(self, ctx: DecisionContext) -> None:
        self._cached_qh = -1
        ctx.market.require_minute(self.signal_field)

    def _bands(self, ctx: DecisionContext) -> tuple[float, float]:
        if self.mode == "si":
            return -self.si_band_mw, self.si_band_mw
        if ctx.qh_index != self._cached_qh:
            history = ctx.settlement_window(self.lookback_qh)
            history = history[~np.isnan(history)]
            if len(history) < self.min_history_qh:
                upper, lower = self.default_upper_eur_mwh, self.default_lower_eur_mwh
            else:
                upper = float(np.quantile(history, self.upper_quantile))
                lower = float(np.quantile(history, self.lower_quantile))
            if upper - lower < self.min_spread_eur_mwh:
                mid = 0.5 * (upper + lower)
                upper, lower = mid + 0.5 * self.min_spread_eur_mwh, mid - 0.5 * self.min_spread_eur_mwh
            self._upper, self._lower = upper, lower
            self._cached_qh = ctx.qh_index
        return self._lower, self._upper

    def _rebalance(self, ctx: DecisionContext) -> float:
        if self.rebalance_power_frac <= 0.0:
            return 0.0
        gap = ctx.soc_frac - self.soc_target_frac
        if abs(gap) < 0.02:
            return 0.0
        power = self.rebalance_power_frac * ctx.battery.max_power_mw
        return power if gap > 0 else -power

    def decide(self, ctx: DecisionContext) -> float:
        if ctx.visible_minutes_in_current_qh() >= self.min_visible_minutes:
            signal = ctx.latest_in_current_qh(self.signal_field)
        else:
            signal = math.nan
        if math.isnan(signal) and self.fallback_to_previous_qh:
            signal = ctx.latest(self.signal_field)
        if math.isnan(signal):
            return self._rebalance(ctx)
        lower, upper = self._bands(ctx)
        p_max = ctx.battery.max_power_mw
        if self.mode == "si":
            if signal <= lower:
                return p_max
            if signal >= upper:
                return -p_max
            return self._rebalance(ctx)
        if signal >= upper:
            return p_max
        if signal <= lower:
            return -p_max
        return self._rebalance(ctx)
