"""Minute-level battery physics."""

from __future__ import annotations

from .types import MINUTE_HOURS, BatteryParams


def feasible_power(power_mw: float, soc_mwh: float, battery: BatteryParams, dt_hours: float = MINUTE_HOURS) -> float:
    """Clip a requested power (+ discharge, - charge) to power, availability and SOC limits for one step."""
    p_max = battery.max_power_mw
    power = min(max(power_mw, -p_max), p_max)
    if power > 0.0:
        room = (soc_mwh - battery.soc_min_mwh) * battery.discharge_efficiency / dt_hours
        power = min(power, max(room, 0.0))
    elif power < 0.0:
        room = (battery.soc_max_mwh - soc_mwh) / (battery.charge_efficiency * dt_hours)
        power = max(power, -max(room, 0.0))
    return power


def next_soc(power_mw: float, soc_mwh: float, battery: BatteryParams, dt_hours: float = MINUTE_HOURS) -> float:
    """SOC after applying an already-feasible power for one step."""
    if power_mw > 0.0:
        return soc_mwh - power_mw * dt_hours / battery.discharge_efficiency
    return soc_mwh - power_mw * dt_hours * battery.charge_efficiency


def apply_power(
    power_mw: float, soc_mwh: float, battery: BatteryParams, dt_hours: float = MINUTE_HOURS
) -> tuple[float, float]:
    """Return ``(applied_power_mw, new_soc_mwh)``."""
    power = feasible_power(power_mw, soc_mwh, battery, dt_hours)
    soc = next_soc(power, soc_mwh, battery, dt_hours)
    # guard against floating drift outside the band
    soc = min(max(soc, battery.soc_min_mwh), battery.soc_max_mwh)
    return power, soc
