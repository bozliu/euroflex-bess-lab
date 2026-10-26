"""Core value types for the minute-level real-time engine."""

from __future__ import annotations

from dataclasses import dataclass, replace

MINUTES_PER_QH = 15
MINUTE_HOURS = 1.0 / 60.0
QH_HOURS = 0.25


@dataclass(frozen=True)
class BatteryParams:
    """Physical and economic parameters of one battery (or of a fleet treated as one asset).

    Power sign convention used everywhere in :mod:`euroflex_bess_lab.realtime`: ``power_mw > 0`` is discharge
    (net injection above nomination, i.e. the BRP becomes long), ``power_mw < 0`` is charge.
    Efficiencies are one-way. Degradation is charged per MWh discharged.
    """

    power_mw: float
    energy_mwh: float
    charge_efficiency: float = 0.95
    discharge_efficiency: float = 0.95
    soc_min_frac: float = 0.05
    soc_max_frac: float = 0.95
    initial_soc_frac: float = 0.5
    degradation_eur_per_mwh: float = 8.0
    availability: float = 1.0

    def __post_init__(self) -> None:
        if self.power_mw <= 0.0 or self.energy_mwh <= 0.0:
            raise ValueError("power_mw and energy_mwh must be positive")
        for name in ("charge_efficiency", "discharge_efficiency"):
            value = getattr(self, name)
            if not 0.0 < value <= 1.0:
                raise ValueError(f"{name} must be in (0, 1]")
        if not 0.0 <= self.soc_min_frac < self.soc_max_frac <= 1.0:
            raise ValueError("require 0 <= soc_min_frac < soc_max_frac <= 1")
        if not self.soc_min_frac <= self.initial_soc_frac <= self.soc_max_frac:
            raise ValueError("initial_soc_frac must lie within the SOC band")
        if self.degradation_eur_per_mwh < 0.0:
            raise ValueError("degradation_eur_per_mwh must be non-negative")
        if not 0.0 < self.availability <= 1.0:
            raise ValueError("availability must be in (0, 1]")

    @property
    def soc_min_mwh(self) -> float:
        return self.soc_min_frac * self.energy_mwh

    @property
    def soc_max_mwh(self) -> float:
        return self.soc_max_frac * self.energy_mwh

    @property
    def initial_soc_mwh(self) -> float:
        return self.initial_soc_frac * self.energy_mwh

    @property
    def usable_energy_mwh(self) -> float:
        return self.soc_max_mwh - self.soc_min_mwh

    @property
    def max_power_mw(self) -> float:
        return self.power_mw * self.availability

    @property
    def duration_hours(self) -> float:
        return self.energy_mwh / self.power_mw

    def scaled(self, factor: float) -> BatteryParams:
        """Return the same asset scaled in power and energy (e.g. a fleet of identical batteries)."""
        if factor <= 0.0:
            raise ValueError("factor must be positive")
        return replace(self, power_mw=self.power_mw * factor, energy_mwh=self.energy_mwh * factor)


@dataclass(frozen=True)
class PublicationModel:
    """When market information becomes usable for a decision.

    Time is measured in minutes from the start of the simulation grid. The decision for minute ``t`` is taken at
    the start of that minute and applies to ``[t, t + 1)``.

    - Minute row ``j`` describes ``[j, j + 1)`` and is published ``minute_lag`` minutes after the minute ends, so it
      is usable for decision ``t`` iff ``j + 1 + minute_lag <= t``. Rows with index below
      :meth:`visible_minute_end` are usable.
    - The final price of quarter-hour ``k`` (minutes ``[15k, 15k + 15)``) is usable iff
      ``15k + 15 + qh_final_lag_minutes <= t``.
    - Day-ahead prices for delivery day D (local time) are usable from ``da_publication_hour_local`` on D-1.
    """

    minute_lag: int = 1
    qh_final_lag_minutes: int = 5
    da_publication_hour_local: int = 13

    def __post_init__(self) -> None:
        if self.minute_lag < 0 or self.qh_final_lag_minutes < 0:
            raise ValueError("lags must be non-negative")
        if not 0 <= self.da_publication_hour_local <= 23:
            raise ValueError("da_publication_hour_local must be an hour of the day")

    def visible_minute_end(self, t: int) -> int:
        """Exclusive end index of the minute rows usable at decision minute ``t``."""
        return max(0, t - self.minute_lag)

    def visible_qh_end(self, t: int) -> int:
        """Exclusive end index of the quarter-hours whose final price is usable at decision minute ``t``."""
        return max(0, (t - self.qh_final_lag_minutes) // MINUTES_PER_QH)
