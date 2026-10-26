"""Settings of the DE-LU day-ahead + continuous-intraday two-stage battery strategy.

Defaults (all overridable; every default is an *assumption* and is stated in ``docs/intraday.md``):

* Day-ahead decision at D-1 12:00 Europe/Berlin (SDAC gate closure); results are known from about 13:00.
* Intraday quantity for quarter-hour product k frozen ``gate_closure_minutes`` = 30 minutes before delivery
  (the cross-zonal SIDC gate closure targeted by the EU market-design reform; German market-area trading on EPEX
  continues until 5 minutes before delivery, so 30 minutes is conservative for the information set).
* Execution at the ID AEP of the product: a VWAP-execution proxy (the index is the volume-weighted price of the
  last 500 MW traded in the product), plus a per-MWh slippage and exchange fees.
* Trade-feed information model: the closing trades of product j are known from the end of its delivery
  (``visibility_lag_minutes`` after it), as for any exchange member watching the public trade ticker. The
  "published" model instead only uses whole days published on netztransparenz.de (days <= D-2 for day D).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from typing import Literal

from ...realtime.types import BatteryParams

LOCAL_TZ = "Europe/Berlin"
QH_HOURS = 0.25

type ForecastKind = Literal["da", "profile", "profile_live"]
type StrategyKind = Literal["da_only", "id_static", "id_rolling", "oracle_da", "oracle_id", "oracle_da_id"]
type ProfileShrinkage = Literal["none", "js", "soft"]


def default_battery() -> BatteryParams:
    """1 MW / 2 MWh, SOC 5-95 %, start and end every day at 50 %, 0.95 / 0.95, 8 EUR/MWh discharged."""

    return BatteryParams(power_mw=1.0, energy_mwh=2.0, degradation_eur_per_mwh=8.0)


@dataclass(frozen=True)
class TradingCosts:
    """Per-MWh trading costs. Exchange + clearing fees are assumptions (EPEX prices via member packages)."""

    da_fee_eur_mwh: float = 0.10
    id_fee_eur_mwh: float = 0.15
    slippage_eur_mwh: float = 1.0

    def __post_init__(self) -> None:
        for name in ("da_fee_eur_mwh", "id_fee_eur_mwh", "slippage_eur_mwh"):
            if getattr(self, name) < 0.0:
                raise ValueError(f"{name} must be non-negative")

    @property
    def id_cost_eur_mwh(self) -> float:
        """Total cost per MWh traded intraday (fee + slippage)."""

        return self.id_fee_eur_mwh + self.slippage_eur_mwh


@dataclass(frozen=True)
class IntradayTiming:
    da_decision_hour_local: int = 12
    da_publication_hour_local: int = 13
    gate_closure_minutes: int = 30
    visibility_lag_minutes: int = 0
    published_day_lag: int = 2

    def __post_init__(self) -> None:
        if self.gate_closure_minutes < 0 or self.visibility_lag_minutes < 0:
            raise ValueError("gate_closure_minutes and visibility_lag_minutes must be non-negative")
        if self.published_day_lag < 1:
            raise ValueError("published_day_lag must be >= 1 (a day is never published before it ends)")
        if not 0 <= self.da_decision_hour_local < self.da_publication_hour_local <= 23:
            raise ValueError("require 0 <= da_decision_hour_local < da_publication_hour_local <= 23")

    @property
    def visible_offset_qh(self) -> int:
        """Product k may use the closing price of product j iff ``j <= k - visible_offset_qh``.

        The decision for k is taken at ``start_k - gate_closure``; product j is visible from
        ``end_j + lag = start_j + 15 min + lag``.
        """

        return max(1, math.ceil((self.gate_closure_minutes + self.visibility_lag_minutes + 15) / 15))


@dataclass(frozen=True)
class ForecastSettings:
    """Causal intraday spread forecast: trailing quarter-of-day profile + decaying live residual.

    Defaults were chosen on forecast error only (mean absolute / squared error of the execution-price forecast,
    Mar 2024 - Oct 2026), never on backtest revenue: a 56-day window, per-slot soft thresholding of the profile
    at 2 standard errors (slots whose mean spread is not clearly non-zero shrink to zero) and the mean of the
    2 latest visible residuals.
    """

    profile_days: int = 56
    residual_window: int = 2
    max_lead: int = 128
    winsor_eur_mwh: float = 300.0
    profile_shrinkage: ProfileShrinkage = "soft"
    profile_threshold_se: float = 2.0
    beta_min_t: float = 2.0

    def __post_init__(self) -> None:
        if self.profile_days < 2 or self.residual_window < 1 or self.max_lead < 8:
            raise ValueError("profile_days >= 2, residual_window >= 1 and max_lead >= 8 are required")
        if self.winsor_eur_mwh <= 0.0:
            raise ValueError("winsor_eur_mwh must be positive")
        if self.beta_min_t < 0.0 or self.profile_threshold_se < 0.0:
            raise ValueError("beta_min_t and profile_threshold_se must be non-negative")


@dataclass(frozen=True)
class StrategySpec:
    name: str
    label: str
    kind: StrategyKind
    forecast: ForecastKind | None = None
    tradable: bool = True


STRATEGIES: dict[str, StrategySpec] = {
    spec.name: spec
    for spec in (
        StrategySpec("da_only", "DA only (persistence)", "da_only"),
        StrategySpec("da_id_reopt_da", "DA + ID: re-optimise at DA prices", "id_static", "da"),
        StrategySpec("da_id_reopt_profile", "DA + ID: + intraday profile (published index)", "id_static", "profile"),
        StrategySpec("da_id_rolling", "DA + ID: rolling, live residual", "id_rolling", "profile_live"),
        StrategySpec("oracle_id", "Oracle ID (perfect ID, persistence DA)", "oracle_id", tradable=False),
        StrategySpec("oracle_da", "Oracle DA only", "oracle_da", tradable=False),
        StrategySpec("oracle_da_id", "Oracle DA + ID", "oracle_da_id", tradable=False),
    )
}
DEFAULT_STRATEGIES = tuple(STRATEGIES)
MAIN_STRATEGY = "da_id_rolling"


@dataclass(frozen=True)
class IntradayConfig:
    battery: BatteryParams = field(default_factory=default_battery)
    costs: TradingCosts = field(default_factory=TradingCosts)
    timing: IntradayTiming = field(default_factory=IntradayTiming)
    forecast: ForecastSettings = field(default_factory=ForecastSettings)
    charge_tiebreak_eur_mwh: float = 1e-3

    def with_costs(self, **changes: float) -> IntradayConfig:
        return replace(self, costs=replace(self.costs, **changes))

    def with_timing(self, **changes: int) -> IntradayConfig:
        return replace(self, timing=replace(self.timing, **changes))

    def describe(self) -> dict[str, object]:
        b = self.battery
        return {
            "battery": {
                "power_mw": b.power_mw,
                "energy_mwh": b.energy_mwh,
                "soc_min_mwh": b.soc_min_mwh,
                "soc_max_mwh": b.soc_max_mwh,
                "initial_and_terminal_soc_mwh": b.initial_soc_mwh,
                "charge_efficiency": b.charge_efficiency,
                "discharge_efficiency": b.discharge_efficiency,
                "degradation_eur_per_mwh_discharged": b.degradation_eur_per_mwh,
            },
            "costs": {
                "da_fee_eur_mwh": self.costs.da_fee_eur_mwh,
                "id_fee_eur_mwh": self.costs.id_fee_eur_mwh,
                "slippage_eur_mwh": self.costs.slippage_eur_mwh,
            },
            "timing": {
                "da_decision_local": f"D-1 {self.timing.da_decision_hour_local:02d}:00 Europe/Berlin",
                "gate_closure_minutes_before_delivery": self.timing.gate_closure_minutes,
                "visibility_lag_minutes_after_delivery_end": self.timing.visibility_lag_minutes,
                "visible_offset_qh": self.timing.visible_offset_qh,
                "published_day_lag": self.timing.published_day_lag,
            },
            "forecast": {
                "profile_days": self.forecast.profile_days,
                "residual_window": self.forecast.residual_window,
                "max_lead": self.forecast.max_lead,
                "winsor_eur_mwh": self.forecast.winsor_eur_mwh,
                "profile_shrinkage": self.forecast.profile_shrinkage,
                "profile_threshold_se": self.forecast.profile_threshold_se,
                "beta_min_t": self.forecast.beta_min_t,
            },
        }
