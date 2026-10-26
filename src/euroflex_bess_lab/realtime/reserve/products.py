"""Belgian aFRR product rules used by the reserve layer: blocks, gate closure, energy and power headroom.

Product facts used here (Elia, Belgium):

- aFRR balancing capacity is procured per direction (up / down, asymmetric products) in a daily auction held on
  D-1 for six 4-hour delivery blocks of day D (Europe/Brussels local time; 3 or 5 hours on DST-change days). The
  Elia Open Data bid tables (ODS125 up to 19/05/2026, ODS207 from 20/05/2026) have a 4-hour granularity and a
  tender date of D-1. The auction closes at 09:00 local time on D-1 (``gate_closure_hour_local``, as reported by
  public market summaries; Elia publishes the next day's dimensioned needs before 07:00 D-1); results are assumed
  known one hour later (``results_delay_minutes``). Capacity remuneration is pay-as-bid, minimum bid 1 MW.
- Since Elia joined PICASSO on 26 Nov 2024, aFRR balancing energy is remunerated pay-as-cleared at the
  cross-border marginal price of each 4-second optimisation cycle; before that date it was pay-as-bid.
- Energy headroom: a battery that sells ``up_mw`` must be able to sustain full upward activation for
  ``sustain_minutes`` and, symmetrically, full downward activation of ``down_mw``. The default of 15 minutes is the
  repository's existing aFRR convention (``AfrrConfig.sustain_duration_minutes``), which mirrors the 15-minute
  limited-energy-reservoir criterion that SOGL Art. 156 applies to FCR in continental Europe. Elia's aFRR rules
  do not publish a battery-specific duration in the sources we could verify: the BSP must deliver the requested
  aFRR energy throughout the block and is penalised for missing energy. Treat 15 minutes as a modelling choice
  and use the 60-minute sensitivity in the study for a stricter reading.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from ..types import BatteryParams

LOCAL_TIMEZONE = "Europe/Brussels"
AFRR_BLOCK_HOURS = 4
PICASSO_GO_LIVE_UTC = pd.Timestamp("2024-11-26T00:00:00", tz=LOCAL_TIMEZONE).tz_convert("UTC")
MARI_GO_LIVE_UTC = pd.Timestamp("2025-05-21T00:00:00", tz=LOCAL_TIMEZONE).tz_convert("UTC")
DIRECTIONS = ("up", "down")


@dataclass(frozen=True)
class AfrrProduct:
    """Timing and delivery obligations of the Belgian aFRR capacity product (see module docstring)."""

    block_hours: int = AFRR_BLOCK_HOURS
    gate_closure_hour_local: int = 9
    results_delay_minutes: int = 60
    sustain_minutes: float = 15.0
    timezone: str = LOCAL_TIMEZONE
    min_bid_mw: float = 1.0

    def __post_init__(self) -> None:
        if self.block_hours <= 0 or 24 % self.block_hours:
            raise ValueError("block_hours must divide 24")
        if not 0 <= self.gate_closure_hour_local <= 23:
            raise ValueError("gate_closure_hour_local must be an hour of the day")
        if self.results_delay_minutes < 0 or self.sustain_minutes < 0:
            raise ValueError("delays and durations must be non-negative")

    @property
    def sustain_hours(self) -> float:
        return self.sustain_minutes / 60.0


def reserve_soc_band(battery: BatteryParams, up_mw: float, down_mw: float, sustain_hours: float) -> tuple[float, float]:
    """SOC interval (MWh) that keeps ``up_mw`` / ``down_mw`` deliverable for ``sustain_hours``.

    Lower bound: enough stored energy to discharge ``up_mw`` for the sustain duration on top of the minimum SOC.
    Upper bound: enough empty room to absorb ``down_mw`` for the same duration below the maximum SOC.
    The interval is empty (``lo > hi``) when the reservation is not feasible for this battery.
    """
    lo = battery.soc_min_mwh + max(up_mw, 0.0) * sustain_hours / battery.discharge_efficiency
    hi = battery.soc_max_mwh - max(down_mw, 0.0) * sustain_hours * battery.charge_efficiency
    return lo, hi


def reserve_is_feasible(
    battery: BatteryParams, up_mw: float, down_mw: float, sustain_hours: float, *, tol: float = 1e-9
) -> bool:
    """Power and energy feasibility of a reservation (each direction at most the available power)."""
    if up_mw < -tol or down_mw < -tol:
        return False
    if up_mw > battery.max_power_mw + tol or down_mw > battery.max_power_mw + tol:
        return False
    lo, hi = reserve_soc_band(battery, up_mw, down_mw, sustain_hours)
    return lo <= hi + tol


def max_symmetric_reserve_mw(battery: BatteryParams, sustain_hours: float) -> float:
    """Largest ``x`` such that reserving ``x`` MW up and ``x`` MW down is feasible."""
    per_mw = sustain_hours / battery.discharge_efficiency + sustain_hours * battery.charge_efficiency
    if per_mw <= 0.0:
        return battery.max_power_mw
    return min(battery.max_power_mw, battery.usable_energy_mwh / per_mw)


def cap_reservation(battery: BatteryParams, up_mw: float, down_mw: float, sustain_hours: float) -> tuple[float, float]:
    """Clip a requested reservation to the power limit, then scale both directions down to fit the energy band."""
    up = min(max(up_mw, 0.0), battery.max_power_mw)
    down = min(max(down_mw, 0.0), battery.max_power_mw)
    need = up * sustain_hours / battery.discharge_efficiency + down * sustain_hours * battery.charge_efficiency
    room = battery.usable_energy_mwh
    if need > room > 0.0:
        scale = room / need
        up, down = up * scale, down * scale
    return up, down


def implicit_power_limits(battery: BatteryParams, up_mw: float, down_mw: float) -> tuple[float, float]:
    """``(max_discharge_mw, max_charge_mw)`` left for implicit trading after reserving ``up_mw`` / ``down_mw``.

    The battery must be able to add ``up_mw`` of discharge or ``down_mw`` of charge on top of its implicit
    position at any time, so the implicit power ``u`` (+ discharge) is restricted to
    ``-(P - down_mw) <= u <= P - up_mw``.
    """
    p_max = battery.max_power_mw
    return max(p_max - max(up_mw, 0.0), 0.0), max(p_max - max(down_mw, 0.0), 0.0)
