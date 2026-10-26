"""Pure normalisation functions: raw Energi Data Service / SVK frames -> Nordic contract tables.

Contract (all timestamps tz-aware UTC, intervals half-open ``[start, end)``):

* day-ahead: ``timestamp_utc``, ``resolution_minutes`` (60 until delivery day 2025-09-30, 15 afterwards),
  ``price_eur_mwh``.
* reserve capacity (``fcr_nordic``, ``fcr_dk1``, ``afrr_capacity``, ``mfrr_capacity``): ``delivery_start_utc``,
  ``delivery_end_utc``, ``zone``, ``product``, ``direction`` (``symmetric``/``up``/``down``), ``price_eur_per_mw_h``
  (marginal clearing price per MW and hour of availability) plus volume columns.
* Danish imbalance (``dk_imbalance``): ``timestamp_utc``, ``area``, ``resolution_minutes`` and price components.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from .energinet import parse_utc
from .sources import DA_15MIN_GO_LIVE_UTC

HOUR = pd.Timedelta(hours=1)

FCR_PRODUCTS = {"FCR-N": "fcr_n", "FCR-D upp": "fcr_d_up", "FCR-D ned": "fcr_d_down"}
FCR_DIRECTIONS = {"fcr_n": "symmetric", "fcr_d_up": "up", "fcr_d_down": "down", "fcr": "symmetric"}
# Energinet renamed the SE-DK2 FCR auctions in 2024 ("D-2" -> "D-1 early", "D-1" -> "D-1 late").
FCR_AUCTIONS = {"D-1 early": "early", "D-2": "early", "D-1 late": "late", "D-1": "late", "Total": "total"}


def _float32(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("float32")


# --- day-ahead --------------------------------------------------------------------------------------------------


def normalize_energinet_day_ahead(
    elspot: pd.DataFrame, day_ahead: pd.DataFrame, zone: str
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Hourly ``Elspotprices`` before the 15-minute go-live, 15-minute ``DayAheadPrices`` from 2025-10-01."""

    parts: list[pd.DataFrame] = []
    if not elspot.empty:
        e = elspot.loc[elspot["PriceArea"] == zone]
        parts.append(
            pd.DataFrame(
                {
                    "timestamp_utc": parse_utc(e["HourUTC"]),
                    "resolution_minutes": np.int16(60),
                    "price_eur_mwh": _float32(e["SpotPriceEUR"]),
                }
            ).loc[lambda f: f["timestamp_utc"] < DA_15MIN_GO_LIVE_UTC]
        )
    if not day_ahead.empty:
        d = day_ahead.loc[day_ahead["PriceArea"] == zone]
        parts.append(
            pd.DataFrame(
                {
                    "timestamp_utc": parse_utc(d["TimeUTC"]),
                    "resolution_minutes": np.int16(15),
                    "price_eur_mwh": _float32(d["DayAheadPriceEUR"]),
                }
            ).loc[lambda f: f["timestamp_utc"] >= DA_15MIN_GO_LIVE_UTC]
        )
    return _finish_day_ahead(parts)


def normalize_svk_day_ahead(frame: pd.DataFrame, zone: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    """SVK Data Service day-ahead dump (``start_time_utc``, ``bidding_zone``, ``price`` in EUR/MWh)."""

    f = frame.loc[frame["bidding_zone"] == zone]
    if "price_unit" in f.columns:
        units = set(f["price_unit"].dropna().unique())
        if units - {"EUR-MWh"}:
            raise ValueError(f"Unexpected SVK day-ahead price units {sorted(units)}")
    part = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(f["start_time_utc"], utc=True).astype("datetime64[ns, UTC]"),
            "price_eur_mwh": _float32(f["price"]),
        }
    )
    part = part.drop_duplicates("timestamp_utc", keep="last").sort_values("timestamp_utc").reset_index(drop=True)
    # SDAC products are hourly until delivery day 2025-09-30 and 15-minute afterwards; the row spacing of the SVK
    # dump is not used because the dump has gaps. Off-grid rows (not on the product grid) are dropped.
    hourly_era = part["timestamp_utc"] < DA_15MIN_GO_LIVE_UTC
    part["resolution_minutes"] = np.where(hourly_era, 60, 15).astype("int16")
    on_grid = np.where(hourly_era, part["timestamp_utc"].dt.minute == 0, part["timestamp_utc"].dt.minute % 15 == 0)
    off_grid = int((~on_grid).sum())
    frame, stats = _finish_day_ahead([part.loc[on_grid, ["timestamp_utc", "resolution_minutes", "price_eur_mwh"]]])
    stats["off_grid_rows_dropped"] = off_grid
    return frame, stats


def _finish_day_ahead(parts: list[pd.DataFrame]) -> tuple[pd.DataFrame, dict[str, Any]]:
    columns = ["timestamp_utc", "resolution_minutes", "price_eur_mwh"]
    if not parts:
        empty = pd.DataFrame(
            {c: pd.Series(dtype=t) for c, t in zip(columns, ("datetime64[ns, UTC]", "int16", "float32"), strict=True)}
        )
        return empty, {"null_prices_dropped": 0, "duplicates_dropped": 0}
    frame = pd.concat(parts, ignore_index=True)
    before = len(frame)
    frame = frame.drop_duplicates("timestamp_utc", keep="last").sort_values("timestamp_utc").reset_index(drop=True)
    duplicates = before - len(frame)
    nulls = int(frame["price_eur_mwh"].isna().sum())
    frame = frame.dropna(subset=["price_eur_mwh"]).reset_index(drop=True)
    frame["resolution_minutes"] = frame["resolution_minutes"].astype("int16")
    frame["price_eur_mwh"] = frame["price_eur_mwh"].astype("float32")
    return frame[columns], {"null_prices_dropped": nulls, "duplicates_dropped": int(duplicates)}


# --- FCR ----------------------------------------------------------------------------------------------------------


def fix_fcr_dst_labels(frame: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Repair the documented ``FcrNdDK2`` timestamp error at the end of DST 2024 (and any similar one).

    Energinet: "HourUTC 2024-10-27 00.00 = HourDK 02.00 (incorrect in dataset: HourUTC 01.00 = HourDK 02.00);
    HourUTC 01.00 = HourDK 02.00 (incorrect in dataset: HourUTC 01.01 = HourDK 02.01)". Rows whose ``HourUTC`` is
    not on the hour are floored to the hour, and the rows previously labelled with that hour move one hour back.
    """

    ts = parse_utc(frame["HourUTC"])
    off_hour = ts.dt.minute != 0
    if not off_hour.any():
        return frame.assign(_ts=ts), 0
    fixed = ts.copy()
    keys = ["PriceArea", "ProductName", "AuctionType"]
    repaired = 0
    for idx in frame.index[off_hour]:
        hour = ts[idx].floor("h")
        same = (ts == hour) & (frame[keys] == frame.loc[idx, keys]).all(axis=1)
        fixed[same] = hour - HOUR
        fixed[idx] = hour
        repaired += int(same.sum()) + 1
    return frame.assign(_ts=fixed), repaired


def normalize_fcr_nordic(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """``FcrNdDK2`` -> one row per hour, zone, product (fcr_n / fcr_d_up / fcr_d_down) and auction."""

    frame, repaired = fix_fcr_dst_labels(raw)
    unknown_products = sorted(set(frame["ProductName"].dropna()) - set(FCR_PRODUCTS))
    unknown_auctions = sorted(set(frame["AuctionType"].dropna()) - set(FCR_AUCTIONS))
    frame = frame.loc[frame["ProductName"].isin(FCR_PRODUCTS) & frame["AuctionType"].isin(FCR_AUCTIONS)]
    product = frame["ProductName"].map(FCR_PRODUCTS)
    out = pd.DataFrame(
        {
            "delivery_start_utc": frame["_ts"].astype("datetime64[ns, UTC]"),
            "delivery_end_utc": (frame["_ts"] + HOUR).astype("datetime64[ns, UTC]"),
            "zone": frame["PriceArea"].astype("string"),
            "product": product.astype("string"),
            "direction": product.map(FCR_DIRECTIONS).astype("string"),
            "auction": frame["AuctionType"].map(FCR_AUCTIONS).astype("string"),
            "price_eur_per_mw_h": _float32(frame["PriceTotalEUR"]),
            "volume_local_mw": _float32(frame["PurchasedVolumeLocal"]),
            "volume_total_mw": _float32(frame["PurchasedVolumeTotal"]),
        }
    )
    keys = ["delivery_start_utc", "zone", "product", "auction"]
    before = len(out)
    out = out.drop_duplicates(keys, keep="last").sort_values(keys).reset_index(drop=True)
    stats: dict[str, Any] = {
        "dst_label_rows_repaired": repaired,
        "duplicates_dropped": int(before - len(out)),
        "unknown_products_dropped": unknown_products,
        "unknown_auction_types_dropped": unknown_auctions,
        "null_prices": int(out["price_eur_per_mw_h"].isna().sum()),
        "rows_per_auction": {str(k): int(v) for k, v in out["auction"].value_counts().sort_index().items()},
    }
    return out, stats


def normalize_fcr_dk1(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """``FcrDK1`` -> DK1 FCR (Continental Europe FCR cooperation, symmetric) per hour."""

    ts = parse_utc(raw["HourUTC"])
    out = pd.DataFrame(
        {
            "delivery_start_utc": ts,
            "delivery_end_utc": (ts + HOUR).astype("datetime64[ns, UTC]"),
            "zone": pd.Series("DK1", index=raw.index, dtype="string"),
            "product": pd.Series("fcr", index=raw.index, dtype="string"),
            "direction": pd.Series("symmetric", index=raw.index, dtype="string"),
            "price_eur_per_mw_h": _float32(raw["FCRdk_EUR"]),
            "cross_border_price_eur_per_mw_h": _float32(raw["FCRcross_EUR"]),
            "volume_domestic_mw": _float32(raw["FCRdomestic_MW"]),
            "volume_abroad_mw": _float32(raw["FCRabroad_MW"]),
        }
    )
    before = len(out)
    out = out.drop_duplicates("delivery_start_utc", keep="last").sort_values("delivery_start_utc")
    out = out.reset_index(drop=True)
    stats = {
        "duplicates_dropped": int(before - len(out)),
        "null_prices": int(out["price_eur_per_mw_h"].isna().sum()),
        "danish_equals_cross_border_share": float(
            np.isclose(out["price_eur_per_mw_h"], out["cross_border_price_eur_per_mw_h"]).mean()
        )
        if len(out)
        else None,
    }
    return out, stats


# --- aFRR / mFRR capacity -----------------------------------------------------------------------------------------


def _up_down_long(raw: pd.DataFrame, product: str, time_column: str, source: str) -> pd.DataFrame:
    ts = parse_utc(raw[time_column])
    parts = []
    for direction, prefix in (("up", "Up"), ("down", "Down")):
        parts.append(
            pd.DataFrame(
                {
                    "delivery_start_utc": ts,
                    "delivery_end_utc": (ts + HOUR).astype("datetime64[ns, UTC]"),
                    "zone": raw["PriceArea"].astype("string"),
                    "product": pd.Series(product, index=raw.index, dtype="string"),
                    "direction": pd.Series(direction, index=raw.index, dtype="string"),
                    "price_eur_per_mw_h": _float32(raw[f"{prefix}PriceEUR"]),
                    "procured_mw": _float32(raw[f"{prefix}ProcuredMW"]),
                    "demand_mw": _float32(raw[f"{prefix}DemandMW"])
                    if f"{prefix}DemandMW" in raw
                    else np.float32(np.nan),
                    "source": pd.Series(source, index=raw.index, dtype="string"),
                }
            )
        )
    return pd.concat(parts, ignore_index=True)


def capacity_zone_coverage(frame: pd.DataFrame) -> dict[str, Any]:
    """First/last delivery hour, rows and null prices per zone of a capacity table."""

    return {
        str(zone): {
            "first_utc": f"{g['delivery_start_utc'].min():%Y-%m-%dT%H:%M:%SZ}",
            "last_utc": f"{g['delivery_start_utc'].max():%Y-%m-%dT%H:%M:%SZ}",
            "rows": int(len(g)),
            "null_prices": int(g["price_eur_per_mw_h"].isna().sum()),
        }
        for zone, g in frame.groupby("zone", sort=True)
    }


def _finish_capacity(out: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    keys = ["delivery_start_utc", "zone", "direction"]
    before = len(out)
    out = out.drop_duplicates(keys, keep="last").sort_values(keys).reset_index(drop=True)
    for column in ("price_eur_per_mw_h", "procured_mw", "demand_mw"):
        out[column] = out[column].astype("float32")
    return out, {"duplicates_dropped": int(before - len(out)), "per_zone": capacity_zone_coverage(out)}


def normalize_afrr(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """``AfrrReservesNordic`` (hourly marginal capacity prices per zone and direction) -> long format."""

    return _finish_capacity(_up_down_long(raw, "afrr", "TimeUTC", "energinet"))


def normalize_mfrr(
    energinet_raw: pd.DataFrame, svk_frames: Mapping[str, pd.DataFrame]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Danish daily mFRR capacity auctions (Energinet) + Swedish mFRR CM (SVK Data Service) -> long format."""

    parts = []
    if not energinet_raw.empty:
        parts.append(_up_down_long(energinet_raw, "mfrr", "TimeUTC", "energinet"))
    units: set[str] = set()
    for zone, frame in svk_frames.items():
        f = frame.loc[frame["bidding_zone"] == zone]
        if "price_unit" in f.columns:
            units |= set(f["price_unit"].dropna().unique())
        ts = pd.to_datetime(f["start_time_utc"], utc=True).astype("datetime64[ns, UTC]")
        parts.append(
            pd.DataFrame(
                {
                    "delivery_start_utc": ts,
                    "delivery_end_utc": (ts + HOUR).astype("datetime64[ns, UTC]"),
                    "zone": f["bidding_zone"].astype("string"),
                    "product": pd.Series("mfrr", index=f.index, dtype="string"),
                    "direction": f["reserve_direction"].astype("string").str.lower(),
                    "price_eur_per_mw_h": _float32(f["price"]),
                    "procured_mw": _float32(f["volume"]),
                    "demand_mw": np.float32(np.nan),
                    "source": pd.Series("svk", index=f.index, dtype="string"),
                }
            )
        )
    if units - {"EUR-MW"}:
        raise ValueError(f"Unexpected SVK mFRR price units {sorted(units)}")
    out, stats = _finish_capacity(pd.concat(parts, ignore_index=True))
    return out, stats


# --- Danish imbalance ---------------------------------------------------------------------------------------------


def normalize_dk_imbalance(
    quarter_hour: pd.DataFrame, hourly: pd.DataFrame, cutover_utc: pd.Timestamp
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Danish imbalance prices: hourly ``RegulatingBalancePowerdata`` rows that end by ``cutover_utc``, 15-minute
    ``ImbalancePrice`` rows (aFRR VWA + mFRR marginal design) from ``cutover_utc`` (intervals never overlap)."""

    parts = []
    if not hourly.empty:
        ts = parse_utc(hourly["HourUTC"])
        h = pd.DataFrame(
            {
                "timestamp_utc": ts,
                "area": hourly["PriceArea"].astype("string"),
                "resolution_minutes": np.int16(60),
                "imbalance_price_eur_mwh": _float32(hourly["ImbalancePriceEUR"]),
                "balancing_power_price_up_eur_mwh": _float32(hourly["BalancingPowerPriceUpEUR"]),
                "balancing_power_price_down_eur_mwh": _float32(hourly["BalancingPowerPriceDownEUR"]),
                "mfrr_up_activated_mwh": _float32(hourly["mFRRUpActBal"]),
                "mfrr_down_activated_mwh": _float32(hourly["mFRRDownActBal"]),
                "imbalance_mwh": _float32(hourly["ImbalanceMWh"]),
            }
        )
        parts.append(h.loc[h["timestamp_utc"] + HOUR <= cutover_utc])  # no overlap with the 15-minute rows
    if not quarter_hour.empty:
        ts = parse_utc(quarter_hour["TimeUTC"])
        q = pd.DataFrame(
            {
                "timestamp_utc": ts,
                "area": quarter_hour["PriceArea"].astype("string"),
                "resolution_minutes": np.int16(15),
                "imbalance_price_eur_mwh": _float32(quarter_hour["ImbalancePriceEUR"]),
                "spot_price_eur_mwh": _float32(quarter_hour["SpotPriceEUR"]),
                "dominating_direction": pd.to_numeric(quarter_hour["DominatingDirection"], errors="coerce").astype(
                    "Int8"
                ),
                "satisfied_demand_mw": _float32(quarter_hour["SatisfiedDemand"]),
                "afrr_up_mwh": _float32(quarter_hour["aFRRUpMW"]),
                "afrr_vwa_up_eur_mwh": _float32(quarter_hour["aFRRVWAUpEUR"]),
                "afrr_down_mwh": _float32(quarter_hour["aFRRDownMW"]),
                "afrr_vwa_down_eur_mwh": _float32(quarter_hour["aFRRVWADownEUR"]),
                "mfrr_marginal_up_eur_mwh": _float32(quarter_hour["mFRRMarginalPriceUpEUR"]),
                "mfrr_marginal_down_eur_mwh": _float32(quarter_hour["mFRRMarginalPriceDownEUR"]),
            }
        )
        parts.append(q.loc[q["timestamp_utc"] >= cutover_utc])
    out = pd.concat(parts, ignore_index=True)
    keys = ["timestamp_utc", "area"]
    before = len(out)
    out = out.drop_duplicates(keys, keep="last").sort_values(keys).reset_index(drop=True)
    float_columns = [c for c in out.columns if c.endswith(("_eur_mwh", "_mwh", "_mw"))]
    for column in float_columns:
        out[column] = out[column].astype("float32")
    out["resolution_minutes"] = out["resolution_minutes"].astype("int16")
    if "dominating_direction" in out.columns:
        out["dominating_direction"] = out["dominating_direction"].astype("Int8")
    stats = {
        "duplicates_dropped": int(before - len(out)),
        "null_imbalance_prices": int(out["imbalance_price_eur_mwh"].isna().sum()),
        "rows_per_resolution": {str(k): int(v) for k, v in out["resolution_minutes"].value_counts().items()},
    }
    return out, stats


def dk_imbalance_rule_check(frame: pd.DataFrame) -> dict[str, Any]:
    """Share of 15-minute rows where the published price equals the documented design (Energinet,
    "Imbalance price design"): dominant up -> max(mFRR up, aFRR VWA up); dominant down -> min(mFRR down, aFRR VWA
    down); no dominant direction -> spot price. Missing components are skipped."""

    q = frame.loc[frame["resolution_minutes"] == 15].copy()
    if q.empty:
        return {"rows": 0}
    direction = q["dominating_direction"].astype("float64").to_numpy()
    published = q["imbalance_price_eur_mwh"].to_numpy(np.float64)

    def share(mask_idle_afrr: bool) -> tuple[float | None, int]:
        afrr_up = q["afrr_vwa_up_eur_mwh"].to_numpy(np.float64).copy()
        afrr_down = q["afrr_vwa_down_eur_mwh"].to_numpy(np.float64).copy()
        if mask_idle_afrr:  # a VWA of exactly 0 with zero aFRR energy means "no aFRR price"
            afrr_up[(afrr_up == 0) & (q["afrr_up_mwh"].to_numpy(np.float64) == 0)] = np.nan
            afrr_down[(afrr_down == 0) & (q["afrr_down_mwh"].to_numpy(np.float64) == 0)] = np.nan
        up = np.fmax(q["mfrr_marginal_up_eur_mwh"].to_numpy(np.float64), afrr_up)
        down = np.fmin(q["mfrr_marginal_down_eur_mwh"].to_numpy(np.float64), afrr_down)
        spot = q["spot_price_eur_mwh"].to_numpy(np.float64)
        expected = np.where(direction > 0, up, np.where(direction < 0, down, spot))
        valid = np.isfinite(expected) & np.isfinite(published)
        if not valid.any():
            return None, 0
        return float(np.isclose(expected[valid], published[valid], atol=0.011).mean()), int(valid.sum())

    raw_share, checked = share(False)
    masked_share, _ = share(True)
    return {
        "rows": int(len(q)),
        "rows_checked": checked,
        "share_matching_rule": raw_share,
        "share_matching_rule_idle_afrr_as_missing": masked_share,
        "rule": "up: max(mFRR up, aFRR VWA up); down: min(mFRR down, aFRR VWA down); none: spot",
    }
