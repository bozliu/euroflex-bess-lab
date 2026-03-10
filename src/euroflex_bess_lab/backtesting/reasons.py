from __future__ import annotations

import pandas as pd

from ..types import BatterySpec, SiteSpec


def assign_reason_codes(dispatch: pd.DataFrame, battery: BatterySpec, *, overlay: bool) -> pd.DataFrame:
    frame = dispatch.copy()
    codes: list[str] = []
    limit_epsilon = 1e-6
    for row in frame.itertuples():
        if row.power_limit_mw > 0 and abs(row.net_export_mw) >= row.power_limit_mw - limit_epsilon:
            codes.append("blocked_by_connection_limit")
            continue
        if (
            (
                getattr(row, "fcr_reserved_mw", 0.0) > limit_epsilon
                or getattr(row, "afrr_up_reserved_mw", 0.0) > limit_epsilon
                or getattr(row, "afrr_down_reserved_mw", 0.0) > limit_epsilon
            )
            and row.charge_mw <= limit_epsilon
            and row.discharge_mw <= limit_epsilon
        ):
            codes.append("reserve_capacity_commitment")
            continue
        if overlay and abs(row.imbalance_mw) > limit_epsilon and row.net_export_mw > row.baseline_net_export_mw:
            codes.append("discharge_for_imbalance_capture")
            continue
        if row.charge_mw > limit_epsilon:
            codes.append("charge_for_da_spread")
            continue
        if row.discharge_mw > limit_epsilon:
            codes.append("discharge_for_da_spread")
            continue
        if (
            row.soc_mwh <= battery.effective_soc_min_mwh + limit_epsilon
            or row.soc_mwh >= battery.effective_soc_max_mwh - limit_epsilon
        ):
            codes.append("blocked_by_soc_limit")
            continue
        codes.append("idle_due_to_efficiency_or_degradation")
    frame["reason_code"] = codes
    return frame


def assign_site_reason_codes(dispatch: pd.DataFrame, site: SiteSpec) -> pd.DataFrame:
    frame = dispatch.copy()
    codes: list[str] = []
    limit_epsilon = 1e-6
    for row in frame.itertuples():
        if (
            row.discharge_mw + getattr(row, "fcr_reserved_mw", 0.0) + getattr(row, "afrr_up_reserved_mw", 0.0)
            >= site.poi_export_limit_mw - limit_epsilon
        ):
            codes.append("blocked_by_site_poi_limit")
            continue
        if (
            row.charge_mw + getattr(row, "fcr_reserved_mw", 0.0) + getattr(row, "afrr_down_reserved_mw", 0.0)
            >= site.poi_import_limit_mw - limit_epsilon
        ):
            codes.append("blocked_by_site_poi_limit")
            continue
        if (
            (
                getattr(row, "fcr_reserved_mw", 0.0) > limit_epsilon
                or getattr(row, "afrr_up_reserved_mw", 0.0) > limit_epsilon
                or getattr(row, "afrr_down_reserved_mw", 0.0) > limit_epsilon
            )
            and row.charge_mw <= limit_epsilon
            and row.discharge_mw <= limit_epsilon
        ):
            codes.append("reserve_capacity_commitment")
            continue
        if row.charge_mw > limit_epsilon:
            codes.append("charge_for_da_spread")
            continue
        if row.discharge_mw > limit_epsilon:
            codes.append("discharge_for_da_spread")
            continue
        codes.append("idle_due_to_efficiency_or_degradation")
    frame["reason_code"] = codes
    return frame
