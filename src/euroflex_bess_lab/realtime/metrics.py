"""Summary metrics for simulation ledgers."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .types import BatteryParams


def daily_pnl(qh: pd.DataFrame, timezone: str = "Europe/Brussels") -> pd.DataFrame:
    """Daily P&L in local delivery days."""
    frame = qh.copy()
    frame["date"] = pd.to_datetime(frame["timestamp_utc"], utc=True).dt.tz_convert(timezone).dt.date
    grouped = frame.groupby("date", sort=True)
    out = grouped[["revenue_eur", "degradation_eur", "pnl_eur", "discharged_mwh", "charged_mwh"]].sum()
    out["n_qh"] = grouped.size()
    return out.reset_index()


def summarize_ledger(
    qh: pd.DataFrame,
    battery: BatteryParams,
    *,
    n_minutes: int,
    oracle_pnl_eur: float | None = None,
    timezone: str = "Europe/Brussels",
) -> dict[str, Any]:
    days = n_minutes / 1440.0
    years = days / 365.25
    pnl = float(qh["pnl_eur"].sum())
    revenue = float(qh["revenue_eur"].sum())
    degradation = float(qh["degradation_eur"].sum())
    discharged = float(qh["discharged_mwh"].sum())
    charged = float(qh["charged_mwh"].sum())
    traded = (qh["discharged_mwh"] + qh["charged_mwh"]) > 1e-9
    daily = daily_pnl(qh, timezone=timezone)
    daily_values = daily["pnl_eur"].to_numpy(dtype=float)
    worst = np.sort(daily_values)[: max(1, int(np.ceil(0.05 * len(daily_values))))] if len(daily_values) else []
    cumulative = np.cumsum(daily_values) if len(daily_values) else np.array([0.0])
    drawdown = float(np.max(np.maximum.accumulate(cumulative) - cumulative)) if len(cumulative) else 0.0
    sell = qh["discharged_mwh"] > 0
    buy = qh["charged_mwh"] > 0
    summary: dict[str, Any] = {
        "days": days,
        "pnl_eur": pnl,
        "revenue_eur": revenue,
        "degradation_eur": degradation,
        "eur_per_mw_year": pnl / battery.power_mw / years if years > 0 else float("nan"),
        "eur_per_mwh_capacity_year": pnl / battery.energy_mwh / years if years > 0 else float("nan"),
        "discharged_mwh": discharged,
        "charged_mwh": charged,
        "cycles_per_day": discharged / battery.usable_energy_mwh / days if days > 0 else float("nan"),
        "traded_qh_share": float(traded.mean()) if len(qh) else 0.0,
        "avg_sell_price_eur_mwh": float(
            np.average(qh.loc[sell, "price_eur_mwh"], weights=qh.loc[sell, "discharged_mwh"])
        )
        if sell.any()
        else float("nan"),
        "avg_buy_price_eur_mwh": float(np.average(qh.loc[buy, "price_eur_mwh"], weights=qh.loc[buy, "charged_mwh"]))
        if buy.any()
        else float("nan"),
        "daily_pnl_mean_eur": float(np.mean(daily_values)) if len(daily_values) else 0.0,
        "daily_pnl_std_eur": float(np.std(daily_values)) if len(daily_values) else 0.0,
        "daily_pnl_cvar05_eur": float(np.mean(worst)) if len(daily_values) else 0.0,
        "negative_day_share": float(np.mean(daily_values < 0)) if len(daily_values) else 0.0,
        "max_drawdown_eur": drawdown,
        "price_missing_qh": int(qh["price_missing"].sum()),
    }
    if oracle_pnl_eur is not None and oracle_pnl_eur > 0:
        summary["capture_vs_oracle"] = pnl / oracle_pnl_eur
    return summary
