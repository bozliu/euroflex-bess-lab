"""Pure normalizers: map upstream Elia fields to the snake_case data contract (Plan.md section 5).

All functions take raw DataFrames (as read from the cached parquet exports) and return contract frames with
tz-aware UTC timestamps (``datetime64[ns, UTC]``), float32 measurements and string quality flags.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

# --- column maps (upstream name -> contract name) -------------------------------------------------------------

ODS133_MAP: dict[str, str] = {
    "systemimbalance": "si_cum_mw",
    "ace": "ace_cum_mw",
    "alpha": "alpha_eur_mwh",
    "alpha_prime": "alpha_prime_eur_mwh",
    "marginalincrementalprice": "mip_eur_mwh",
    "marginaldecrementalprice": "mdp_eur_mwh",
    "imbalanceprice": "price_cum_eur_mwh",
    "qualitystatus": "quality_status",
}
ODS126_MAP: dict[str, str] = {
    "systemimbalance": "si_mw",
    "ace": "ace_mw",
    "igccvolumeup": "igcc_up_mw",
    "afrrvolumeup": "afrr_up_mw",
    "mfrrsaup": "mfrr_sa_up_mw",
    "mfrrdaup": "mfrr_da_up_mw",
    "reserve_sharing_import": "rs_import_mw",
    "igccvolumedown": "igcc_down_mw",
    "afrrvolumedown": "afrr_down_mw",
    "mfrrsadown": "mfrr_sa_down_mw",
    "mfrrdadown": "mfrr_da_down_mw",
    "reserve_sharing_export": "rs_export_mw",
}
ODS165_MAP: dict[str, str] = {
    "r2priceup": "afrr_price_up_eur_mwh",
    "r2pricedown": "afrr_price_down_eur_mwh",
    "upwardavailablemfrrprice": "mfrr_avail_price_up_eur_mwh",
    "downwardavailablemfrrprice": "mfrr_avail_price_down_eur_mwh",
    "floorprice": "floor_price_eur_mwh",
    "cap": "cap_price_eur_mwh",
    "intertsopriceimport": "inter_tso_price_import_eur_mwh",
    "intertsopriceexport": "inter_tso_price_export_eur_mwh",
}
ODS134_MAP: dict[str, str] = {
    "systemimbalance": "si_mw",
    "ace": "ace_mw",
    "alpha": "alpha_eur_mwh",
    "alpha_prime": "alpha_prime_eur_mwh",
    "marginalincrementalprice": "mip_eur_mwh",
    "marginaldecrementalprice": "mdp_eur_mwh",
    "imbalanceprice": "imbalance_price_eur_mwh",
    "qualitystatus": "quality_status",
}
ODS166_MAP: dict[str, str] = {
    "afrrpriceup": "afrr_price_up_eur_mwh",
    "afrrpricedown": "afrr_price_down_eur_mwh",
    "mfrrsaup": "mfrr_sa_price_up_eur_mwh",
    "mfrrdaup": "mfrr_da_price_up_eur_mwh",
    "mfrrsadown": "mfrr_sa_price_down_eur_mwh",
    "mfrrdadown": "mfrr_da_price_down_eur_mwh",
    "upwardavailablemfrrprice": "mfrr_avail_price_up_eur_mwh",
    "downwardavailablemfrrprice": "mfrr_avail_price_down_eur_mwh",
    "floorprice": "floor_price_eur_mwh",
    "cap": "cap_price_eur_mwh",
    "intertsopriceimport": "inter_tso_price_import_eur_mwh",
    "intertsopriceexport": "inter_tso_price_export_eur_mwh",
}
ODS127_MAP: dict[str, str] = {k: v for k, v in ODS126_MAP.items() if k not in {"systemimbalance", "ace"}}

NEW_FORMULA_MAP: dict[str, str] = {
    "frce": "frce_mw",
    "frcerolling": "frce_rolling_mw",
    "systemimbalance": "si_mw",
    "systemimbalancerolling": "si_rolling_mw",
    "imbalanceprice": "imbalance_price_eur_mwh",
    "mainpricecomponentup": "main_up_eur_mwh",
    "mainpricecomponentdown": "main_down_eur_mwh",
    "incentivizingcomponent": "incentive_eur_mwh",
    "voaaup": "voaa_up_eur_mwh",
    "afrrpriceup": "afrr_price_up_eur_mwh",
    "mfrrpriceup": "mfrr_price_up_eur_mwh",
    "mfrrsapriceup": "mfrr_sa_price_up_eur_mwh",
    "mfrrdapriceup": "mfrr_da_price_up_eur_mwh",
    "reservesharingpriceup": "rs_price_up_eur_mwh",
    "voaadown": "voaa_down_eur_mwh",
    "afrrpricedown": "afrr_price_down_eur_mwh",
    "mfrrpricedown": "mfrr_price_down_eur_mwh",
    "mfrrsapricedown": "mfrr_sa_price_down_eur_mwh",
    "mfrrdapricedown": "mfrr_da_price_down_eur_mwh",
    "reservesharingpricedown": "rs_price_down_eur_mwh",
    "weightafrr": "weight_afrr",
    "weightmfrr": "weight_mfrr",
    "igccvolumeup": "igcc_up_mw",
    "afrrvolumeup": "afrr_up_mw",
    "mfrrsavolumeup": "mfrr_sa_up_mw",
    "mfrrdavolumeup": "mfrr_da_up_mw",
    "reservesharingvolumeup": "rs_up_mw",
    "igccvolumedown": "igcc_down_mw",
    "afrrvolumedown": "afrr_down_mw",
    "mfrrsadown": "mfrr_sa_down_mw",
    "mfrrdadown": "mfrr_da_down_mw",
    "reservesharingvolumedown": "rs_down_mw",
    "frcel2target": "frce_l2_target_mw",
    "qualitystatus": "quality_status",
}

SI_FORECAST_MAP: dict[str, str] = {
    "predictions_silinearregressionforecast": "si_forecast_mw",
    "predictions_categoricalsiprediction_from_minus_inf_to_minus_400": "p_si_lt_m400",
    "predictions_categoricalsiprediction_from_minus_400_to_minus_200": "p_si_m400_m200",
    "predictions_categoricalsiprediction_from_minus_200_to_0": "p_si_m200_0",
    "predictions_categoricalsiprediction_from_0_to_200": "p_si_0_200",
    "predictions_categoricalsiprediction_from_200_to_400": "p_si_200_400",
    "predictions_categoricalsiprediction_from_400_to_inf": "p_si_gt_400",
    "predictionquality": "prediction_quality",
    "quality_status": "quality_status",
}

# --- contract column orders --------------------------------------------------------------------------------------

MINUTE_COLUMNS: list[str] = [
    "timestamp_utc",
    "qh_start_utc",
    *ODS133_MAP.values(),
    *ODS126_MAP.values(),
    *ODS165_MAP.values(),
]
QH_COLUMNS: list[str] = ["timestamp_utc", *ODS134_MAP.values(), *ODS166_MAP.values(), *ODS127_MAP.values()]
NEW_FORMULA_MINUTE_COLUMNS: list[str] = ["timestamp_utc", *NEW_FORMULA_MAP.values()]
NEW_FORMULA_QH_COLUMNS: list[str] = [
    c for c in NEW_FORMULA_MINUTE_COLUMNS if c not in {"frce_rolling_mw", "si_rolling_mw"}
]
SI_FORECAST_COLUMNS: list[str] = ["prediction_time_utc", "target_qh_utc", *SI_FORECAST_MAP.values()]
SI_PROBABILITY_DECIMALS = 4
SI_FORECAST_DECIMALS = 2
STRING_COLUMNS = frozenset({"quality_status", "direction", "product"})


# --- helpers -----------------------------------------------------------------------------------------------------


def utc_series(values: pd.Series | Sequence[object]) -> pd.Series:
    """Parse to ``datetime64[ns, UTC]`` (index preserved); naive values are interpreted as UTC."""

    series = values if isinstance(values, pd.Series) else pd.Series(list(values))
    return pd.Series(pd.to_datetime(series, utc=True)).astype("datetime64[ns, UTC]")


def cast_contract_types(frame: pd.DataFrame) -> pd.DataFrame:
    """float32 for numeric measurement columns, ns-UTC timestamps, pandas strings for flags (in place)."""

    for column in frame.columns:
        if column.endswith("_utc"):
            frame[column] = utc_series(frame[column])
        elif column in STRING_COLUMNS:
            frame[column] = frame[column].astype("string")
        elif column in {"resolution_minutes", "n_bids", "n_offered_bids"}:
            continue
        else:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("float32")
    return frame


def rename_select(raw: pd.DataFrame, mapping: Mapping[str, str], time_field: str, time_name: str) -> pd.DataFrame:
    """Rename upstream columns to contract names and index the frame by the parsed UTC timestamp."""

    raw = raw.reset_index(drop=True)
    frame = pd.DataFrame({time_name: utc_series(raw[time_field])})
    for upstream, contract in mapping.items():
        frame[contract] = raw[upstream] if upstream in raw.columns else np.nan
    frame = frame.dropna(subset=[time_name])
    frame = frame.drop_duplicates(time_name, keep="last").sort_values(time_name)
    return frame.set_index(time_name)


def full_grid(start: pd.Timestamp, end: pd.Timestamp, freq: str) -> pd.DatetimeIndex:
    """Half-open ``[start, end)`` UTC grid."""

    index = pd.date_range(start, end, freq=freq, tz="UTC", inclusive="left")
    return pd.DatetimeIndex(index.astype("datetime64[ns, UTC]"), name="timestamp_utc")


def grid_bounds(
    indexes: Sequence[pd.Index], step: pd.Timedelta, window_start: pd.Timestamp, window_end: pd.Timestamp
) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    """Data-driven grid bounds: from the first to the last timestamp seen in any source, clipped to the window."""

    present = [idx for idx in indexes if len(idx)]
    if not present:
        return None
    first = min(pd.Timestamp(idx.min()) for idx in present)
    last = max(pd.Timestamp(idx.max()) for idx in present)
    start = max(first, window_start)
    end = min(last + step, window_end)
    if end <= start:
        return None
    return start, end


def _duplicates_in_window(raw: pd.DataFrame, field: str, start: pd.Timestamp, end: pd.Timestamp) -> int:
    if raw.empty or field not in raw.columns:
        return 0
    ts = utc_series(raw[field])
    ts = ts[(ts >= start) & (ts < end)]
    return int(ts.duplicated().sum())


# --- table normalizers ---------------------------------------------------------------------------------------------


def normalize_minute(
    ods133: pd.DataFrame,
    ods126: pd.DataFrame,
    ods165: pd.DataFrame,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Merge ODS133 (cumulative QH-to-date), ODS126 (instantaneous) and ODS165 (price components) per minute."""

    a = rename_select(ods133, ODS133_MAP, "datetime", "timestamp_utc")
    b = rename_select(ods126, ODS126_MAP, "datetime", "timestamp_utc")
    c = rename_select(ods165, ODS165_MAP, "datetime", "timestamp_utc")
    parts = [p[(p.index >= window_start) & (p.index < window_end)] for p in (a, b, c)]
    bounds = grid_bounds([p.index for p in parts], pd.Timedelta(minutes=1), window_start, window_end)
    stats: dict[str, object] = {}
    if bounds is None:
        return pd.DataFrame(columns=MINUTE_COLUMNS), {"rows": 0}
    grid = full_grid(bounds[0], bounds[1], "1min")
    merged = pd.concat([p.reindex(grid) for p in parts], axis=1)
    merged.index.name = "timestamp_utc"
    merged = merged.reset_index()
    merged["qh_start_utc"] = merged["timestamp_utc"].dt.floor("15min")
    # Consistency of the upstream `quarterhour` field with the floor of the minute timestamp.
    if "quarterhour" in ods133.columns and len(ods133):
        qh = utc_series(ods133["quarterhour"])
        ts = utc_series(ods133["datetime"])
        stats["ods133_quarterhour_mismatches"] = int((qh != ts.dt.floor("15min")).sum())
    for name, part, raw in (("ods133", parts[0], ods133), ("ods126", parts[1], ods126), ("ods165", parts[2], ods165)):
        stats[f"{name}_rows"] = len(part)
        stats[f"{name}_missing_minutes"] = int(len(grid) - grid.isin(part.index).sum())
        stats[f"{name}_duplicate_rows_dropped"] = _duplicates_in_window(raw, "datetime", window_start, window_end)
    all_missing = merged[[c for c in MINUTE_COLUMNS if c not in {"timestamp_utc", "qh_start_utc"}]].isna().all(axis=1)
    stats["minutes_missing_in_all_sources"] = int(all_missing.sum())
    stats["grid_start_utc"] = f"{grid[0]:%Y-%m-%dT%H:%M:%SZ}"
    stats["grid_end_utc"] = f"{grid[-1] + pd.Timedelta(minutes=1):%Y-%m-%dT%H:%M:%SZ}"
    frame = cast_contract_types(merged[MINUTE_COLUMNS].copy())
    return frame, stats


def normalize_qh(
    ods134: pd.DataFrame,
    ods166: pd.DataFrame,
    ods127: pd.DataFrame,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
) -> tuple[pd.DataFrame, dict[str, object]]:
    a = rename_select(ods134, ODS134_MAP, "datetime", "timestamp_utc")
    b = rename_select(ods166, ODS166_MAP, "datetime", "timestamp_utc")
    c = rename_select(ods127, ODS127_MAP, "datetime", "timestamp_utc")
    parts = [p[(p.index >= window_start) & (p.index < window_end)] for p in (a, b, c)]
    bounds = grid_bounds([p.index for p in parts], pd.Timedelta(minutes=15), window_start, window_end)
    if bounds is None:
        return pd.DataFrame(columns=QH_COLUMNS), {"rows": 0}
    grid = full_grid(bounds[0], bounds[1], "15min")
    merged = pd.concat([p.reindex(grid) for p in parts], axis=1)
    merged.index.name = "timestamp_utc"
    merged = merged.reset_index()
    stats: dict[str, object] = {}
    for name, part in (("ods134", parts[0]), ("ods166", parts[1]), ("ods127", parts[2])):
        stats[f"{name}_rows"] = len(part)
        stats[f"{name}_missing_qh"] = int(len(grid) - grid.isin(part.index).sum())
    stats["grid_start_utc"] = f"{grid[0]:%Y-%m-%dT%H:%M:%SZ}"
    stats["grid_end_utc"] = f"{grid[-1] + pd.Timedelta(minutes=15):%Y-%m-%dT%H:%M:%SZ}"
    return cast_contract_types(merged[QH_COLUMNS].copy()), stats


def normalize_new_formula(raw: pd.DataFrame, *, minute: bool) -> tuple[pd.DataFrame, dict[str, object]]:
    """ODS211 (minute, cumulative QH-to-date like ODS133) or ODS212 (quarter-hour) to the contract layout."""

    columns = NEW_FORMULA_MINUTE_COLUMNS if minute else NEW_FORMULA_QH_COLUMNS
    mapping = {k: v for k, v in NEW_FORMULA_MAP.items() if v in columns}
    part = rename_select(raw, mapping, "datetime", "timestamp_utc")
    step = pd.Timedelta(minutes=1 if minute else 15)
    if part.empty:
        return pd.DataFrame(columns=columns), {"rows": 0}
    grid = full_grid(
        pd.Timestamp(part.index.min()), pd.Timestamp(part.index.max()) + step, "1min" if minute else "15min"
    )
    frame = part.reindex(grid)
    frame.index.name = "timestamp_utc"
    frame = frame.reset_index()
    stats: dict[str, object] = {
        "source_rows": int(len(raw)),
        "missing_intervals_inside_coverage": int(len(grid) - len(part)),
        "resolution_codes": sorted({str(v) for v in raw.get("resolutioncode", pd.Series(dtype=str)).dropna().unique()}),
        "quality_status_values": sorted(
            {str(v) for v in raw.get("qualitystatus", pd.Series(dtype=str)).dropna().unique()}
        ),
    }
    return cast_contract_types(frame[columns].copy()), stats


def normalize_si_forecast(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object]]:
    raw = raw.reset_index(drop=True)
    frame = pd.DataFrame(
        {
            "prediction_time_utc": utc_series(raw["predictiontimeutc"]),
            "target_qh_utc": utc_series(raw["predictions_forecastedtimeutc"]),
        }
    )
    for upstream, contract in SI_FORECAST_MAP.items():
        frame[contract] = raw[upstream] if upstream in raw.columns else np.nan
    before = len(frame)
    frame = frame.dropna(subset=["prediction_time_utc", "target_qh_utc"])
    frame = frame.drop_duplicates(["prediction_time_utc", "target_qh_utc"], keep="last")
    frame = frame.sort_values(["prediction_time_utc", "target_qh_utc"]).reset_index(drop=True)
    # Elia switched `predictionquality` from a fraction to a percentage on 2025-08-22 (new forecasting system);
    # normalize to a fraction in [0, 1]. No post-switch value lies in (0, 1], so the rule is unambiguous.
    quality = pd.to_numeric(frame["prediction_quality"], errors="coerce")
    percent = quality > 1.0
    frame["prediction_quality"] = quality.where(~percent, quality / 100.0)
    # Storage rounding (immaterial for trading, halves the file size): probabilities to 1e-4, SI to 0.01 MW.
    for column in [c for c in SI_FORECAST_COLUMNS if c.startswith("p_si_")]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").round(SI_PROBABILITY_DECIMALS)
    frame["si_forecast_mw"] = pd.to_numeric(frame["si_forecast_mw"], errors="coerce").round(SI_FORECAST_DECIMALS)
    stats: dict[str, object] = {
        "duplicates_or_null_keys_dropped": int(before - len(frame)),
        "prediction_quality_rows_converted_from_percent": int(percent.sum()),
    }
    return cast_contract_types(frame[SI_FORECAST_COLUMNS].copy()), stats


# --- RES / load ---------------------------------------------------------------------------------------------------

RES_LOAD_COLUMNS: list[str] = [
    "timestamp_utc",
    "load_mw",
    "load_most_recent_forecast_mw",
    "load_da_forecast_mw",
    "load_week_ahead_forecast_mw",
    "wind_mw",
    "wind_offshore_mw",
    "wind_onshore_mw",
    "wind_most_recent_forecast_mw",
    "wind_da_11h_forecast_mw",
    "wind_da_forecast_mw",
    "wind_week_ahead_forecast_mw",
    "wind_monitored_capacity_mw",
    "pv_mw",
    "pv_most_recent_forecast_mw",
    "pv_da_11h_forecast_mw",
    "pv_da_forecast_mw",
    "pv_week_ahead_forecast_mw",
    "pv_monitored_capacity_mw",
]
_WIND_SUMS = {
    "measured": "wind_mw",
    "mostrecentforecast": "wind_most_recent_forecast_mw",
    "dayahead11hforecast": "wind_da_11h_forecast_mw",
    "dayaheadforecast": "wind_da_forecast_mw",
    "weekaheadforecast": "wind_week_ahead_forecast_mw",
    "monitoredcapacity": "wind_monitored_capacity_mw",
}
_PV_MAP = {
    "measured": "pv_mw",
    "mostrecentforecast": "pv_most_recent_forecast_mw",
    "dayahead11hforecast": "pv_da_11h_forecast_mw",
    "dayaheadforecast": "pv_da_forecast_mw",
    "weekaheadforecast": "pv_week_ahead_forecast_mw",
    "monitoredcapacity": "pv_monitored_capacity_mw",
}
_LOAD_MAP = {
    "totalload": "load_mw",
    "mostrecentforecast": "load_most_recent_forecast_mw",
    "dayaheadforecast": "load_da_forecast_mw",
    "weekaheadforecast": "load_week_ahead_forecast_mw",
}


def _sum_min_count(group: Any, columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(group[list(columns)].sum(min_count=1))


def normalize_res_load(
    ods001: pd.DataFrame,
    ods031: pd.DataFrame,
    ods032: pd.DataFrame,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Belgian total load (ODS001), wind summed over all regions/connection types (ODS031), PV for region
    ``Belgium`` (ODS032), on one quarter-hour grid."""

    stats: dict[str, object] = {}
    load = rename_select(ods001, _LOAD_MAP, "datetime", "timestamp_utc")
    wind_raw = ods031.reset_index(drop=True).copy()
    wind_raw["timestamp_utc"] = utc_series(wind_raw["datetime"])
    wind_raw = wind_raw[(wind_raw["timestamp_utc"] >= window_start) & (wind_raw["timestamp_utc"] < window_end)]
    for column in (*_WIND_SUMS, "offshoreonshore"):
        if column not in wind_raw.columns:
            wind_raw[column] = np.nan
    wind_raw = wind_raw.drop_duplicates(
        [c for c in ("timestamp_utc", "offshoreonshore", "region", "gridconnectiontype") if c in wind_raw.columns],
        keep="last",
    )
    grouped = wind_raw.groupby("timestamp_utc")
    wind = _sum_min_count(grouped, list(_WIND_SUMS)).rename(columns=_WIND_SUMS)
    offshore_mask = wind_raw["offshoreonshore"].astype(str).str.lower() == "offshore"
    wind["wind_offshore_mw"] = wind_raw[offshore_mask].groupby("timestamp_utc")["measured"].sum(min_count=1)
    wind["wind_onshore_mw"] = wind_raw[~offshore_mask].groupby("timestamp_utc")["measured"].sum(min_count=1)
    rows_per_qh = grouped.size()
    stats["wind_rows_per_qh"] = {str(k): int(v) for k, v in rows_per_qh.value_counts().sort_index().items()}
    pv_raw = ods032[ods032["region"].astype(str) == "Belgium"] if "region" in ods032.columns else ods032
    pv = rename_select(pv_raw, _PV_MAP, "datetime", "timestamp_utc")
    parts = [p[(p.index >= window_start) & (p.index < window_end)] for p in (load, wind, pv)]
    bounds = grid_bounds([p.index for p in parts], pd.Timedelta(minutes=15), window_start, window_end)
    if bounds is None:
        return pd.DataFrame(columns=RES_LOAD_COLUMNS), {"rows": 0}
    grid = full_grid(bounds[0], bounds[1], "15min")
    merged = pd.concat([p.reindex(grid) for p in parts], axis=1)
    merged.index.name = "timestamp_utc"
    merged = merged.reset_index()
    for name, part in (("ods001", parts[0]), ("ods031", parts[1]), ("ods032", parts[2])):
        stats[f"{name}_missing_qh"] = int(len(grid) - grid.isin(part.index).sum())
    return cast_contract_types(merged[RES_LOAD_COLUMNS].copy()), stats


# --- legacy (pre-MARI, before 2024-05-22) ---------------------------------------------------------------------------

LEGACY_QH_MAP: dict[str, str] = {
    "systemimbalance": "si_mw",
    "netregulationvolume": "nrv_mw",
    "alpha": "alpha_eur_mwh",
    "marginalincrementalprice": "mip_eur_mwh",
    "marginaldecrementalprice": "mdp_eur_mwh",
    "strategicreserveprice": "strategic_reserve_price_eur_mwh",
    "positiveimbalanceprice": "imbalance_price_long_eur_mwh",
    "negativeimbalanceprice": "imbalance_price_short_eur_mwh",
    "qualitystatus": "quality_status",
}
LEGACY_MINUTE_MAP: dict[str, str] = {
    "systemimbalance": "si_cum_mw",
    "netregulationvolume": "nrv_cum_mw",
    "alpha": "alpha_eur_mwh",
    "marginalincrementalprice": "mip_eur_mwh",
    "marginaldecrementalprice": "mdp_eur_mwh",
    "strategicreserveprice": "strategic_reserve_price_eur_mwh",
    "systemimbalancevsincrementalbidscoordinable": "si_vs_incremental_coordinable_bids_mw",
    "positiveimbalanceprice": "price_cum_long_eur_mwh",
    "negativeimbalanceprice": "price_cum_short_eur_mwh",
    "calculationtime": "calculation_time_utc",
    "qualitystatus": "quality_status",
}
LEGACY_QH_COLUMNS: list[str] = [
    "timestamp_utc",
    "si_mw",
    "nrv_mw",
    "alpha_eur_mwh",
    "mip_eur_mwh",
    "mdp_eur_mwh",
    "strategic_reserve_price_eur_mwh",
    "imbalance_price_eur_mwh",
    "imbalance_price_long_eur_mwh",
    "imbalance_price_short_eur_mwh",
    "quality_status",
]
LEGACY_MINUTE_COLUMNS: list[str] = [
    "timestamp_utc",
    "qh_start_utc",
    "si_cum_mw",
    "nrv_cum_mw",
    "alpha_eur_mwh",
    "mip_eur_mwh",
    "mdp_eur_mwh",
    "strategic_reserve_price_eur_mwh",
    "si_vs_incremental_coordinable_bids_mw",
    "price_cum_eur_mwh",
    "price_cum_long_eur_mwh",
    "price_cum_short_eur_mwh",
    "calculation_time_utc",
    "quality_status",
]


def normalize_legacy(
    raw: pd.DataFrame, *, minute: bool, window_start: pd.Timestamp, window_end: pd.Timestamp
) -> tuple[pd.DataFrame, dict[str, object]]:
    """ODS046 (minute, cumulative QH-to-date) / ODS047 (quarter-hour) before the MARI go-live."""

    mapping = LEGACY_MINUTE_MAP if minute else LEGACY_QH_MAP
    columns = LEGACY_MINUTE_COLUMNS if minute else LEGACY_QH_COLUMNS
    part = rename_select(raw, mapping, "datetime", "timestamp_utc")
    part = part[(part.index >= window_start) & (part.index < window_end)]
    if part.empty:
        return pd.DataFrame(columns=columns), {"rows": 0}
    step = pd.Timedelta(minutes=1 if minute else 15)
    grid = full_grid(
        pd.Timestamp(part.index.min()), pd.Timestamp(part.index.max()) + step, "1min" if minute else "15min"
    )
    frame = part.reindex(grid)
    frame.index.name = "timestamp_utc"
    frame = frame.reset_index()
    if minute:
        frame["qh_start_utc"] = frame["timestamp_utc"].dt.floor("15min")
        frame["price_cum_eur_mwh"] = frame["price_cum_long_eur_mwh"]
    else:
        frame["imbalance_price_eur_mwh"] = frame["imbalance_price_long_eur_mwh"]
    long_col = "price_cum_long_eur_mwh" if minute else "imbalance_price_long_eur_mwh"
    short_col = "price_cum_short_eur_mwh" if minute else "imbalance_price_short_eur_mwh"
    both = frame[long_col].notna() & frame[short_col].notna()
    stats: dict[str, object] = {
        "source_rows": int(len(part)),
        "missing_intervals_inside_coverage": int(len(grid) - len(part)),
        "dual_price_intervals": int((both & (frame[long_col] != frame[short_col])).sum()),
    }
    return cast_contract_types(frame[columns].copy()), stats
