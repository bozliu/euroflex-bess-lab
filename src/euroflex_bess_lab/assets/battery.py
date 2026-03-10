from __future__ import annotations

import pandas as pd

from ..types import BatterySpec


def build_battery_state_frame(index: pd.DatetimeIndex, battery: BatterySpec) -> pd.DataFrame:
    availability = battery.availability.to_series(index)
    return pd.DataFrame(
        {
            "timestamp_utc": index,
            "availability_factor": availability.values,
            "power_limit_mw": battery.effective_power_limit_mw * availability.values,
            "effective_soc_min_mwh": battery.effective_soc_min_mwh,
            "effective_soc_max_mwh": battery.effective_soc_max_mwh,
        }
    )
