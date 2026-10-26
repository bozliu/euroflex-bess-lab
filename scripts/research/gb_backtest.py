"""Real-data Great Britain BESS backtest: wholesale (MID proxy), NIV chasing, DC/DM/DR stacking and a combined stack.

Regenerates ``reports/great_britain.md``, ``reports/figures/gb_*.png`` and ``reports/ledgers/gb_*``::

    .venv/bin/python scripts/research/gb_backtest.py

Inputs (committed, see ``data/public/gb/*.meta.json``): Elexon BMRS system prices, Market Index Data and 15-second
frequency (BMRS open data licence), NESO EAC auction results (NESO Open Data Licence) and ECB GBP/EUR reference
rates. Battery: 1 MW / 2 MWh, SOC window 5-95 %, start at 50 %, efficiencies 0.95 / 0.95, degradation 8 EUR per MWh
discharged (converted to GBP at the ECB rate of each delivery day). Simulation from 2023-12-01 (one month of
warm-up history); results are reported from 2024-01-01. Every strategy, setting and rule is documented in
``docs/great_britain.md`` and :mod:`euroflex_bess_lab.markets.gb`.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from euroflex_bess_lab.markets.gb import GBMarket
from euroflex_bess_lab.markets.gb.rules import RESPONSE_PRODUCTS, SOURCES
from euroflex_bess_lab.markets.gb.strategies import (
    DEFAULT_BATTERY,
    MAIN_CONFIGS,
    MAIN_FORECAST,
    PLANNING_HURDLE_GBP_MWH,
    SENSITIVITY_CONFIGS,
    STRATEGIES,
    run_strategy,
)
from euroflex_bess_lab.public_data.gb import load_gb_response_auctions
from euroflex_bess_lab.public_data.gb.timeutils import UK_TZ

REPO_ROOT = Path(__file__).resolve().parents[2]
SIM_START = "2023-12-01"
REPORT_START = "2024-01-01"
MAIN = ["wholesale_forecast", "niv_chasing", "fr_stack", "combined_stack"]
ORACLES = ["wholesale_oracle", "imbalance_oracle", "fr_oracle", "combined_oracle"]
SENSITIVITIES = list(SENSITIVITY_CONFIGS)
H = 0.5
PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
INK, INK2, MUTED, GRID, AXIS, SURFACE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "#fcfcfb"

# Modo Energy ME BESS GB (2H) index: annual means of the monthly annualised values published at
# MODO_STACK_URL (GBP k per MW per year). Reference points only (fleet-weighted real assets incl. BM and CM).
MODO_STACK_URL = (
    "https://modoenergy.github.io/modo-charts/charts/europe/gb/geo-foundation-bess-revenue/"
    "chart-e-gb-revenue-stack-evolution.html"
)
MODO_CROSS_MARKET_URL = (
    "https://modoenergy.github.io/modo-charts/charts/cross-market/power-prices-battery-returns/"
    "power-prices-battery-returns-3-cross-market-revenue-index.html"
)
MODO_INDEX = {
    "2024": {"wholesale": 27.6, "balancing": 16.5, "ancillary": 3.1, "capacity": 13.5, "total": 60.7},
    "2025": {"wholesale": 31.4, "balancing": 19.3, "ancillary": 22.1, "capacity": 7.3, "total": 80.1},
}
DEV_WINDOW = "2023-12-01 to 2024-06-30"


# ----------------------------------------------------------------------------------------------- running


def _run(args: tuple[str, str, str]) -> tuple[str, pd.DataFrame, dict[str, Any], float]:
    name, start, end = args
    market = GBMarket.from_public(start, end)
    began = time.perf_counter()
    result = run_strategy(name, market, DEFAULT_BATTERY)
    return name, result.ledger, result.diagnostics, time.perf_counter() - began


def run_all(
    names: Sequence[str], start: str, end: str, workers: int
) -> dict[str, tuple[pd.DataFrame, dict[str, Any], float]]:
    out: dict[str, tuple[pd.DataFrame, dict[str, Any], float]] = {}
    jobs = [(n, start, end) for n in names]
    if workers <= 1:
        for job in jobs:
            name, ledger, diag, secs = _run(job)
            out[name] = (ledger, diag, secs)
            print(f"  {name}: {secs:.0f} s", flush=True)
        return out
    # longest first
    order = sorted(jobs, key=lambda j: 0 if "combined" in j[0] else 1)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for name, ledger, diag, secs in pool.map(_run, order):
            out[name] = (ledger, diag, secs)
            print(f"  {name}: {secs:.0f} s", flush=True)
    return out


# --------------------------------------------------------------------------------------------- summaries


def period_bounds(end: pd.Timestamp) -> list[tuple[str, pd.Timestamp, pd.Timestamp]]:
    last12_start = end - pd.Timedelta(days=365)
    periods = [
        ("2024", pd.Timestamp("2024-01-01", tz="UTC"), pd.Timestamp("2025-01-01", tz="UTC")),
        ("2025", pd.Timestamp("2025-01-01", tz="UTC"), pd.Timestamp("2026-01-01", tz="UTC")),
        (f"2026 to {(end - pd.Timedelta(days=1)):%d %b}", pd.Timestamp("2026-01-01", tz="UTC"), end),
        ("Latest 12 months", last12_start, end),
    ]
    return [(label, lo, min(hi, end)) for label, lo, hi in periods if lo < end]


def summarize(ledger: pd.DataFrame, lo: pd.Timestamp, hi: pd.Timestamp) -> dict[str, float]:
    part = ledger[(ledger["timestamp_utc"] >= lo) & (ledger["timestamp_utc"] < hi)]
    days = len(part) / 48.0
    f = 365.0 / days if days else float("nan")
    out: dict[str, float] = {"days": days}
    for key in ("wholesale", "imbalance", "response"):
        out[key] = float(part[f"revenue_{key}_gbp"].sum() * f)
    for p in RESPONSE_PRODUCTS:
        out[p] = float(part[f"revenue_{p.lower()}_gbp"].sum() * f)
    out["DC"] = out["DCL"] + out["DCH"]
    out["DM"] = out["DML"] + out["DMH"]
    out["DR"] = out["DRL"] + out["DRH"]
    out["degradation"] = float(-part["degradation_gbp"].sum() * f)
    out["net_gbp"] = float(part["pnl_gbp"].sum() * f)
    out["net_eur"] = float(part["pnl_eur"].sum() * f)
    out["cycles_per_day"] = float(part["discharged_mwh"].sum() / DEFAULT_BATTERY.energy_mwh / days) if days else 0.0
    contracted = part[[f"mw_{p.lower()}" for p in RESPONSE_PRODUCTS]].sum(axis=1) > 0
    out["response_share_of_sps"] = float(contracted.mean())
    low = part[[f"mw_{p.lower()}" for p in ("DCL", "DML", "DRL")]].sum(axis=1)
    high = part[[f"mw_{p.lower()}" for p in ("DCH", "DMH", "DRH")]].sum(axis=1)
    out["avg_low_mw"] = float(low.mean())
    out["avg_high_mw"] = float(high.mean())
    with_low, with_high = low > 0, high > 0
    out["soe_ok_low"] = float(part.loc[with_low, "available_low"].mean()) if with_low.any() else 1.0
    out["soe_ok_high"] = float(part.loc[with_high, "available_high"].mean()) if with_high.any() else 1.0
    for p in RESPONSE_PRODUCTS:
        out[f"mw_{p}"] = float(part[f"mw_{p.lower()}"].mean())
    out["undelivered_response_mwh"] = float(part["response_undelivered_mwh"].sum())
    return out


def monthly(ledger: pd.DataFrame, report_start: pd.Timestamp) -> pd.Series:
    """Net revenue per complete calendar month (UK time); a trailing partial month is dropped."""

    part = ledger[ledger["timestamp_utc"] >= report_start]
    month = part["timestamp_utc"].dt.tz_convert(UK_TZ).dt.strftime("%Y-%m")
    totals = part.groupby(month)["pnl_gbp"].sum()
    counts = part.groupby(month)["pnl_gbp"].size()
    expected = pd.Series(
        [pd.Period(m, freq="M").days_in_month * 48 for m in counts.index], index=counts.index, dtype=float
    )
    return totals[counts >= expected - 2]


# -------------------------------------------------------------------------------------- market context


def market_context(market: GBMarket, auctions: pd.DataFrame, lo: pd.Timestamp, hi: pd.Timestamp) -> dict[str, float]:
    mask = (market.ts >= lo) & (market.ts < hi)
    days = pd.Series(market.ts[mask].tz_convert(UK_TZ).strftime("%Y-%m-%d"))
    mid = pd.Series(market.mid[mask])
    sip = pd.Series(market.sip[mask])
    out = {
        "mean_mid": float(np.nanmean(market.mid[mask])),
        "mean_sip": float(np.nanmean(market.sip[mask])),
        "mid_daily_spread": float(mid.groupby(days.to_numpy()).agg(lambda s: s.max() - s.min()).mean()),
        "sip_daily_spread": float(sip.groupby(days.to_numpy()).agg(lambda s: s.max() - s.min()).mean()),
        "share_short": float(np.mean(market.niv[mask] > 0)),
        "share_negative_sip": float(np.mean(market.sip[mask] < 0)),
    }
    part = auctions[(auctions["delivery_start_utc"] >= lo) & (auctions["delivery_start_utc"] < hi)]
    for p in RESPONSE_PRODUCTS:
        rows = part[part["product"] == p]
        weights = rows["duration_hours"].astype(float)
        out[f"price_{p}"] = (
            float(np.average(rows["clearing_price_gbp_per_mw_h"], weights=weights)) if len(rows) else float("nan")
        )
        out[f"volume_{p}"] = (
            float(np.average(rows["cleared_volume_mw"], weights=weights)) if len(rows) else float("nan")
        )
    return out


# ---------------------------------------------------------------------------------------------- figures


def _style(ax: Any) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
        ax.spines[side].set_linewidth(1.0)
    ax.grid(axis="y", color=GRID, linewidth=1.0)
    ax.set_axisbelow(True)
    ax.tick_params(colors=MUTED, labelcolor=INK2, labelsize=9)


def _thousands(ax: Any) -> None:
    from matplotlib.ticker import FuncFormatter

    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1000:.0f}k" if abs(v) >= 1000 else f"{v:.0f}"))


def figure_monthly(series: dict[str, pd.Series], path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 4.6), dpi=150, facecolor=SURFACE)
    _style(ax)
    for i, (name, values) in enumerate(series.items()):
        x = pd.to_datetime(values.index + "-01")
        ax.plot(
            x, values.to_numpy(), color=PALETTE[i], linewidth=2.0, solid_capstyle="round", label=STRATEGIES[name].label
        )
        ax.plot(
            x[-1:],
            values.to_numpy()[-1:],
            "o",
            color=PALETTE[i],
            markersize=6,
            markeredgecolor=SURFACE,
            markeredgewidth=2,
        )
    ax.axhline(0, color=AXIS, linewidth=1.0)
    _thousands(ax)
    ax.set_ylabel("GBP per MW and month", color=INK2, fontsize=9)
    ax.set_title(
        "Monthly net revenue, 1 MW / 2 MWh, causal strategies (complete months)", color=INK, fontsize=11, loc="left"
    )
    ax.legend(frameon=False, fontsize=8.5, loc="upper left", labelcolor=INK2)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def figure_cumulative(ledgers: dict[str, pd.DataFrame], report_start: pd.Timestamp, path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 4.6), dpi=150, facecolor=SURFACE)
    _style(ax)
    for i, (name, ledger) in enumerate(ledgers.items()):
        part = ledger[ledger["timestamp_utc"] >= report_start]
        daily = part.groupby(part["timestamp_utc"].dt.floor("D"))["pnl_gbp"].sum().cumsum()
        ax.plot(daily.index, daily.to_numpy(), color=PALETTE[i], linewidth=2.0, label=STRATEGIES[name].label)
        ax.annotate(
            f"{daily.iloc[-1] / 1000:,.0f}k",
            (daily.index[-1], daily.iloc[-1]),
            xytext=(6, 0),
            textcoords="offset points",
            color=INK2,
            fontsize=8.5,
            va="center",
        )
    _thousands(ax)
    ax.set_ylabel("cumulative GBP per MW", color=INK2, fontsize=9)
    ax.set_title("Cumulative net revenue since 1 Jan 2024 (causal strategies)", color=INK, fontsize=11, loc="left")
    ax.legend(frameon=False, fontsize=8.5, loc="upper left", labelcolor=INK2)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def figure_stack(stacks: dict[str, dict[str, float]], path: Path, title: str) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    parts = [
        ("wholesale", "Wholesale (MID)"),
        ("imbalance", "Imbalance (system price)"),
        ("DC", "DC"),
        ("DM", "DM"),
        ("DR", "DR"),
        ("degradation", "Degradation"),
    ]
    fig, ax = plt.subplots(figsize=(10, 5.2), dpi=150, facecolor=SURFACE)
    _style(ax)
    labels = list(stacks)
    x = np.arange(len(labels))
    width = 0.32
    pos = np.zeros(len(labels))
    neg = np.zeros(len(labels))
    colors = [*PALETTE[:5], "#b5b3ad"]  # degradation is a cost, drawn in a neutral gray
    for i, (key, name) in enumerate(parts):
        values = np.array([stacks[label][key] for label in labels])
        bottoms = np.where(values >= 0, pos, neg)
        ax.bar(x, values, width, bottom=bottoms, color=colors[i], label=name, edgecolor=SURFACE, linewidth=2.0)
        pos = pos + np.clip(values, 0, None)
        neg = neg + np.clip(values, None, 0)
    ax.set_ylim(min(neg.min() * 1.25, 0), pos.max() * 1.12)
    for xi, label in zip(x, labels, strict=True):
        net = stacks[label]["net_gbp"]
        ax.annotate(
            f"net {net / 1000:,.1f}k",
            (xi, pos[xi]),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center",
            color=INK,
            fontsize=9,
        )
    ax.axhline(0, color=AXIS, linewidth=1.0)
    ax.set_xticks(x, labels)
    _thousands(ax)
    ax.set_ylabel("GBP per MW and year", color=INK2, fontsize=9)
    ax.set_title(title, color=INK, fontsize=11, loc="left", pad=28)
    ax.legend(
        frameon=False,
        fontsize=8.5,
        ncol=6,
        loc="lower left",
        bbox_to_anchor=(0.0, 1.0),
        labelcolor=INK2,
        handlelength=1.2,
        columnspacing=1.2,
    )
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def figure_market(auctions: pd.DataFrame, market: GBMarket, report_start: pd.Timestamp, path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 3, figsize=(12, 4.0), dpi=150, facecolor=SURFACE, sharex=True)
    part = auctions[auctions["delivery_start_utc"] >= report_start].copy()
    part["month"] = pd.to_datetime(part["delivery_start_utc"].dt.tz_convert(UK_TZ).dt.strftime("%Y-%m-01"))
    for ax, products, title in [
        (axes[0], ("DCL", "DML", "DRL"), "Low-frequency products"),
        (axes[1], ("DCH", "DMH", "DRH"), "High-frequency products"),
    ]:
        _style(ax)
        for i, p in enumerate(products):
            rows = part[part["product"] == p]
            series = rows.groupby("month").apply(
                lambda g: float(np.average(g["clearing_price_gbp_per_mw_h"], weights=g["duration_hours"])),
                include_groups=False,
            )
            ax.plot(series.index, series.to_numpy(), color=PALETTE[i], linewidth=2.0, label=p)
        ax.axhline(0, color=AXIS, linewidth=1.0)
        ax.set_title(f"{title}, GBP/MW/h", color=INK, fontsize=10, loc="left")
        ax.legend(frameon=False, fontsize=8, labelcolor=INK2)
    ax = axes[2]
    _style(ax)
    mask = market.ts >= report_start
    local_day = pd.Series(market.ts[mask].tz_convert(UK_TZ).strftime("%Y-%m-%d"))
    for i, (values, label) in enumerate([(market.mid[mask], "MID"), (market.sip[mask], "System price")]):
        spread = pd.Series(values).groupby(local_day.to_numpy()).agg(lambda s: s.max() - s.min())
        spread.index = pd.to_datetime(spread.index)
        monthly_spread = spread.groupby(spread.index.to_period("M")).mean()
        ax.plot(
            monthly_spread.index.to_timestamp(), monthly_spread.to_numpy(), color=PALETTE[i], linewidth=2.0, label=label
        )
    ax.set_title("Mean daily max-min spread, GBP/MWh", color=INK, fontsize=10, loc="left")
    ax.legend(frameon=False, fontsize=8, labelcolor=INK2)
    for a in axes:
        a.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


# ---------------------------------------------------------------------------------------------- ledgers

LEDGER_COLUMNS = [
    "timestamp_utc",
    "settlement_date",
    "settlement_period",
    "wholesale_mw",
    "baseline_mw",
    "imbalance_mw",
    *[f"mw_{p.lower()}" for p in RESPONSE_PRODUCTS],
    "response_export_mwh",
    "response_import_mwh",
    "energy_mwh",
    "discharged_mwh",
    "charged_mwh",
    "soc_end_mwh",
    "mid_gbp_mwh",
    "system_price_gbp_mwh",
    "revenue_wholesale_gbp",
    "revenue_imbalance_gbp",
    *[f"revenue_{p.lower()}_gbp" for p in RESPONSE_PRODUCTS],
    "revenue_response_gbp",
    "degradation_gbp",
    "pnl_gbp",
    "gbp_per_eur",
    "revenue_eur",
    "degradation_eur",
    "pnl_eur",
    "available_low",
    "available_high",
    "strategy",
    "market",
    "mechanism",
    "fleet_mw",
]


def write_ledger(name: str, ledger: pd.DataFrame, out_dir: Path, meta: dict[str, Any]) -> tuple[Path, int]:
    frame = ledger.copy()
    frame["energy_mwh"] = frame["baseline_mw"] * H + frame["response_export_mwh"] - frame["response_import_mwh"]
    gross = frame["revenue_wholesale_gbp"] + frame["revenue_imbalance_gbp"] + frame["revenue_response_gbp"]
    frame["revenue_eur"] = gross / frame["gbp_per_eur"]
    frame["degradation_eur"] = frame["degradation_gbp"] / frame["gbp_per_eur"]
    frame["market"] = "gb"
    frame["mechanism"] = "gb_single_imbalance_price"
    frame["fleet_mw"] = 1.0
    frame = frame[frame["timestamp_utc"] >= pd.Timestamp(REPORT_START, tz="UTC")][LEDGER_COLUMNS]
    for column in frame.columns:
        if frame[column].dtype == np.float64:
            frame[column] = frame[column].astype("float32")
    frame["timestamp_utc"] = frame["timestamp_utc"].astype("datetime64[ns, UTC]")
    path = out_dir / f"gb_{name}.parquet"
    frame.to_parquet(path, index=False, compression="zstd")
    sidecar = {
        "name": f"gb_{name}",
        "strategy": name,
        "label": STRATEGIES[name].label,
        "kind": STRATEGIES[name].kind,
        **meta,
        "rows": int(len(frame)),
        "columns": list(frame.columns),
        "sign_convention": "MW > 0 = discharge/export; GBP > 0 = income",
    }
    (out_dir / f"gb_{name}.json").write_text(json.dumps(sidecar, indent=2, default=str) + "\n", encoding="utf-8")
    return path, path.stat().st_size


def write_daily_ledger(ledgers: dict[str, pd.DataFrame], out_dir: Path, meta: dict[str, Any]) -> tuple[Path, int]:
    """One row per strategy and Europe/London day: revenue by stream and product, degradation, net (GBP, EUR)."""

    value_columns = [
        "revenue_wholesale_gbp",
        "revenue_imbalance_gbp",
        "revenue_response_gbp",
        *[f"revenue_{p.lower()}_gbp" for p in RESPONSE_PRODUCTS],
        "degradation_gbp",
        "pnl_gbp",
        "pnl_eur",
        "discharged_mwh",
        "charged_mwh",
        "response_export_mwh",
        "response_import_mwh",
    ]
    frames = []
    for name, ledger in ledgers.items():
        part = ledger[ledger["timestamp_utc"] >= pd.Timestamp(REPORT_START, tz="UTC")]
        day = part["timestamp_utc"].dt.tz_convert(UK_TZ).dt.strftime("%Y-%m-%d")
        daily = part.groupby(day)[value_columns].sum()
        daily.insert(0, "sps", part.groupby(day).size())
        daily = daily.reset_index(names="date_uk")
        daily.insert(0, "strategy", name)
        frames.append(daily)
    frame = pd.concat(frames, ignore_index=True)
    for column in value_columns:
        frame[column] = frame[column].astype("float32")
    path = out_dir / "gb_daily.parquet"
    frame.to_parquet(path, index=False, compression="zstd")
    sidecar = {"name": "gb_daily", **meta, "rows": int(len(frame)), "columns": list(frame.columns)}
    (out_dir / "gb_daily.json").write_text(json.dumps(sidecar, indent=2, default=str) + "\n", encoding="utf-8")
    return path, path.stat().st_size


# --------------------------------------------------------------------------------------------- markdown


def gbp(v: float) -> str:
    return "0" if abs(v) < 0.5 else f"{v:,.0f}"


def pct(v: float) -> str:
    return f"{100 * v:.0f} %" if np.isfinite(v) else "n/a"


def render(ctx: dict[str, Any]) -> str:
    lines: list[str] = []
    w = lines.append
    end_label = ctx["end_label"]
    periods = ctx["periods"]
    s = ctx["summary"]
    last = periods[-1]
    comb = s["combined_stack"][last]
    w("# Great Britain BESS backtest: wholesale, NIV chasing and DC/DM/DR stacking on real data")
    w("")
    w(
        "Every number on this page is produced by `scripts/research/gb_backtest.py` "
        "(`.venv/bin/python scripts/research/gb_backtest.py`) from the committed GB tables in `data/public/gb` "
        "(Elexon BMRS, NESO data portal, ECB). Market rules and model: [docs/great_britain.md](../docs/great_britain.md)."
    )
    w("")
    w(
        f"**Setup.** 1 MW / 2 MWh battery, SOC window 5-95 % (0.1-1.9 MWh), start at 50 %, charge/discharge "
        "efficiency 0.95 / 0.95, degradation 8 EUR per MWh discharged (converted to GBP at the ECB rate of each "
        f"delivery day; {ctx['deg_gbp_mean']:.2f} GBP/MWh on average). 30-minute settlement periods, single "
        "imbalance price. Wholesale energy is priced at the BMRS Market Index Price (MID) as a **traded-price "
        "proxy** for within-day trading. Dynamic response (DC/DM/DR, low and high) is paid its EAC pay-as-clear "
        "clearing price per MW and hour of availability; delivered response energy is not paid (ABSVD) but moves "
        "the SOC, which is modelled from 15-second BMRS frequency. Money is GBP; EUR columns use the daily ECB "
        f"rate. Reporting period {REPORT_START} to {end_label} (simulation starts 2023-12-01 as warm-up). "
        "GBP/MW/yr = period net revenue x 365 / days. Cycles/day = MWh discharged (baseline and response) / 2 MWh "
        "/ day. Capture = causal strategy / its perfect-foresight oracle."
    )
    w("")
    w("## Key findings")
    w("")
    w(
        f"- **Combined stack: {gbp(comb['net_gbp'])} GBP/MW/yr over the latest 12 months** "
        f"({gbp(comb['net_eur'])} EUR/MW/yr), from public data and decisions that only use published information: "
        f"{gbp(s['combined_stack']['2025']['net_gbp'])} GBP/MW/yr in 2025 and "
        f"{gbp(s['combined_stack']['2024']['net_gbp'])} in 2024."
    )
    ratio24 = (
        s["combined_stack"]["2024"]["net_gbp"] / 1000 / (MODO_INDEX["2024"]["total"] - MODO_INDEX["2024"]["capacity"])
    )
    ratio25 = (
        s["combined_stack"]["2025"]["net_gbp"] / 1000 / (MODO_INDEX["2025"]["total"] - MODO_INDEX["2025"]["capacity"])
    )
    w(
        f"- **Scale check against the market:** that is {pct(ratio24)} (2024) and {pct(ratio25)} (2025) of the published "
        "Modo Energy GB BESS index excluding capacity payments, although the simulated battery does not use the "
        "Balancing Mechanism (see External reference points)."
    )
    fr = s["fr_stack"][last]
    w(
        f"- **Frequency-response stacking alone** earns {gbp(fr['net_gbp'])} GBP/MW/yr (latest 12 months) with the "
        f"State of Energy requirement met in {pct(fr['soe_ok_low'])} / {pct(fr['soe_ok_high'])} of contracted "
        "low / high SPs; the day-ahead allocation uses only the previous day's clearing prices."
    )
    niv = s["niv_chasing"][last]
    wf = s["wholesale_forecast"][last]
    mk = ctx["market"][last]
    w(
        f"- **NIV chasing on published system prices** (position fixed at gate closure, 60 minutes ahead) earns "
        f"{gbp(niv['net_gbp'])} GBP/MW/yr, more than wholesale arbitrage on MID ({gbp(wf['net_gbp'])}, "
        f"{pct(wf['net_gbp'] / s['wholesale_oracle'][last]['net_gbp'])} of its perfect-foresight value): the single "
        f"cash-out price has wider daily spreads than MID ({mk['sip_daily_spread']:.0f} vs "
        f"{mk['mid_daily_spread']:.0f} GBP/MWh)."
    )
    w(
        f"- **Stacking pays:** in the latest 12 months the combined stack beats the best single workflow by "
        f"{gbp(comb['net_gbp'] - max(fr['net_gbp'], niv['net_gbp'], wf['net_gbp']))} GBP/MW/yr while cycling "
        f"{comb['cycles_per_day']:.2f} times per day."
    )
    w("")
    w("![Revenue stack of the combined strategy](figures/gb_revenue_stack.png)")
    w("")
    for label in periods:
        days = s["combined_stack"][label]["days"]
        w(f"## {label}")
        w("")
        w(f"{days:.0f} days.")
        w("")
        w(
            "| Strategy | GBP/MW/yr | EUR/MW/yr | Capture vs oracle | Wholesale | Imbalance | DC | DM | DR | Degradation | "
            "Cycles/day |"
        )
        w("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
        for name in MAIN:
            row = s[name][label]
            oracle = STRATEGIES[name].oracle
            cap = row["net_gbp"] / s[oracle][label]["net_gbp"] if oracle else float("nan")
            w(
                f"| {STRATEGIES[name].label} | {gbp(row['net_gbp'])} | {gbp(row['net_eur'])} | {pct(cap)} | "
                f"{gbp(row['wholesale'])} | {gbp(row['imbalance'])} | {gbp(row['DC'])} | {gbp(row['DM'])} | "
                f"{gbp(row['DR'])} | {gbp(row['degradation'])} | {row['cycles_per_day']:.2f} |"
            )
        for name in ORACLES:
            row = s[name][label]
            w(
                f"| {STRATEGIES[name].label} | {gbp(row['net_gbp'])} | {gbp(row['net_eur'])} | - | "
                f"{gbp(row['wholesale'])} | {gbp(row['imbalance'])} | {gbp(row['DC'])} | {gbp(row['DM'])} | "
                f"{gbp(row['DR'])} | {gbp(row['degradation'])} | {row['cycles_per_day']:.2f} |"
            )
        w("")
    w(
        "Revenue columns are GBP/MW/yr; DC/DM/DR are availability payments (low + high, negative when a product "
        "clears below zero). Oracles know realised prices (and, for response, realised clearing prices and "
        "frequency) when deciding; they are upper bounds for this battery and market model, not achievable revenue."
    )
    w("")
    w("![Monthly net revenue](figures/gb_monthly_net_revenue.png)")
    w("")
    w("![Cumulative net revenue](figures/gb_cumulative_net_revenue.png)")
    w("")
    w(f"## Response products ({last})")
    w("")
    w("| Strategy | DCL | DCH | DML | DMH | DRL | DRH | Avg low MW | Avg high MW | SOE met (low / high) |")
    w("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
    for name in ("fr_stack", "combined_stack", "fr_oracle"):
        row = s[name][last]
        w(
            f"| {STRATEGIES[name].label} | "
            + " | ".join(gbp(row[p]) for p in RESPONSE_PRODUCTS)
            + f" | {row['avg_low_mw']:.2f} | {row['avg_high_mw']:.2f} | {pct(row['soe_ok_low'])} / {pct(row['soe_ok_high'])} |"
        )
    w("")
    w(
        "GBP/MW/yr of availability payments per product. DR high frequency (DRH) usually clears below zero: the "
        "provider pays for the right to absorb energy, which it later sells, so its value appears in the "
        "imbalance/wholesale columns. SOE met = share of contracted SPs in which the State of Energy requirement "
        "(Service Terms 6.11, including the post-delivery relaxation and 20 %/SP recovery) is satisfied; payments "
        "are withheld otherwise."
    )
    w("")
    w("## Sensitivities")
    w("")
    w(f"Latest 12 months and full period ({REPORT_START} to {end_label}); GBP/MW/yr.")
    w("")
    w(
        "| Case | Latest 12 months | Full period | Change vs main (latest 12 months) | Capture vs oracle (latest 12 months) |"
    )
    w("| --- | ---: | ---: | ---: | ---: |")
    base_of = {
        "niv_chasing_period_start": "niv_chasing",
        "niv_chasing_no_spread_term": "niv_chasing",
        "wholesale_gate_closure": "wholesale_forecast",
        "wholesale_naive_persistence": "wholesale_forecast",
        "fr_stack_floor_bids": "fr_stack",
        "fr_stack_market_cap_only": "fr_stack",
        "fr_stack_reserve_export_only": "fr_stack",
        "combined_stack_market_cap_only": "combined_stack",
    }
    for name, base in base_of.items():
        if name not in s:
            continue
        row = s[name][last]
        full = ctx["full"][name]
        change = row["net_gbp"] / s[base][last]["net_gbp"] - 1.0
        oracle = STRATEGIES[name].oracle
        cap = row["net_gbp"] / s[oracle][last]["net_gbp"] if oracle else float("nan")
        w(
            f"| {STRATEGIES[name].label} | {gbp(row['net_gbp'])} | {gbp(full['net_gbp'])} | {100 * change:+.1f} % | {pct(cap)} |"
        )
    w("")
    w(
        "Period start = the physical position is set at the start of the SP with the prices published by then "
        "(possible for a non-BM asset without a Physical Notification); gate closure is the conservative main case. "
        "Stacking cap: the market rule compares offered plus reserved capacity (10 / 20 / 40 % for DC / DM / DR) of "
        "both directions with the unit's registered capacity, which we read as export plus import capability (2 MW "
        "here; in 506 sampled 2026 battery unit-blocks of the EAC results by unit this cap is exceeded in 1 % of "
        "cases, an export-only reading in 64 %). The main strategies keep offered plus reserved MW within 1 MW per "
        "direction, a stricter self-imposed limit chosen on the development window; the rows above show the market "
        "cap alone and the strictest reading."
    )
    w("")
    w("## Market context")
    w("")
    w(
        "| Period | Mean MID GBP/MWh | Mean system price | Daily MID spread | Daily system-price spread | SPs system short | "
        "DCL | DCH | DML | DMH | DRL | DRH |"
    )
    w("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for label in periods:
        c = ctx["market"][label]
        w(
            f"| {label} | {c['mean_mid']:.1f} | {c['mean_sip']:.1f} | {c['mid_daily_spread']:.1f} | "
            f"{c['sip_daily_spread']:.1f} | {pct(c['share_short'])} | "
            + " | ".join(f"{c[f'price_{p}']:.2f}" for p in RESPONSE_PRODUCTS)
            + " |"
        )
    w("")
    w(
        "Spreads are the mean over Europe/London days of (max - min) of the half-hourly values. Response columns "
        "are time-weighted EAC clearing prices in GBP/MW/h. Mean cleared volumes in the latest 12 months: "
        + ", ".join(f"{p} {ctx['market'][last][f'volume_{p}']:,.0f} MW" for p in RESPONSE_PRODUCTS)
        + "."
    )
    w("")
    w("![Market context](figures/gb_market_context.png)")
    w("")
    w("## External reference points")
    w("")
    w(
        "Modo Energy ME BESS GB (2H) index (fleet-weighted revenues of GB 2-hour batteries, at least 6 MW, "
        f"monthly values annualised), annual means of the published monthly values ({MODO_STACK_URL}):"
    )
    w("")
    w(
        "| Year | Modo total | Modo excluding capacity | Wholesale | Balancing (BM + imbalance) | Ancillary (net) | This backtest, combined stack |"
    )
    w("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for year, values in MODO_INDEX.items():
        w(
            f"| {year} | {values['total']:.1f}k | {values['total'] - values['capacity']:.1f}k | {values['wholesale']:.1f}k | "
            f"{values['balancing']:.1f}k | {values['ancillary']:.1f}k | {s['combined_stack'][year]['net_gbp'] / 1000:.1f}k |"
        )
    w("")
    w(
        f"The 2025 total matches the 80.1k GBP/MW/yr average quoted by Modo ({MODO_CROSS_MARKET_URL}). The index "
        "measures real assets, including Balancing Mechanism dispatch and Capacity Market payments that are not "
        "modelled here, so it is a scale reference rather than a like-for-like benchmark."
    )
    w("")
    w("## Encoded market rules")
    w("")
    for rule in ctx["rules"]:
        w(f"- {rule}")
    w("")
    w("## Data and coverage")
    w("")
    for item in ctx["data_notes"]:
        w(f"- {item}")
    w("")
    w("## Scope")
    w("")
    for item in ctx["scope"]:
        w(f"- {item}")
    w("")
    return "\n".join(lines)


# ------------------------------------------------------------------------------------------------- main


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--end", default=None, help="exclusive end (UTC midnight); default: end of the GB data")
    parser.add_argument("--workers", type=int, default=min(8, os.cpu_count() or 1))
    parser.add_argument("--skip-sensitivities", action="store_true")
    parser.add_argument("--report", type=Path, default=REPO_ROOT / "reports" / "great_britain.md")
    parser.add_argument("--figures", type=Path, default=REPO_ROOT / "reports" / "figures")
    parser.add_argument("--ledgers", type=Path, default=REPO_ROOT / "reports" / "ledgers")
    args = parser.parse_args()
    began = time.perf_counter()
    meta_path = REPO_ROOT / "data" / "public" / "gb" / "system_prices.meta.json"
    sp_meta = json.loads(meta_path.read_text(encoding="utf-8"))
    end = pd.Timestamp(args.end, tz="UTC") if args.end else pd.Timestamp(sp_meta["coverage_end_utc"])
    end_label = f"{(end - pd.Timedelta(days=1)):%Y-%m-%d}"
    names = MAIN + ORACLES + ([] if args.skip_sensitivities else SENSITIVITIES)
    print(f"Running {len(names)} strategies {SIM_START} .. {end:%Y-%m-%d} with {args.workers} workers", flush=True)
    results = run_all(names, SIM_START, f"{end:%Y-%m-%d}", args.workers)
    market = GBMarket.from_public(SIM_START, end)
    auctions = load_gb_response_auctions(SIM_START, end)
    report_start = pd.Timestamp(REPORT_START, tz="UTC")
    periods = period_bounds(end)
    summary: dict[str, dict[str, dict[str, float]]] = {}
    full: dict[str, dict[str, float]] = {}
    for name, (ledger, _, _) in results.items():
        summary[name] = {label: summarize(ledger, lo, hi) for label, lo, hi in periods}
        full[name] = summarize(ledger, report_start, end)
    labels = [label for label, _, _ in periods]
    context_market = {label: market_context(market, auctions, lo, hi) for label, lo, hi in periods}

    args.figures.mkdir(parents=True, exist_ok=True)
    args.ledgers.mkdir(parents=True, exist_ok=True)
    figure_monthly({n: monthly(results[n][0], report_start) for n in MAIN}, args.figures / "gb_monthly_net_revenue.png")
    figure_cumulative({n: results[n][0] for n in MAIN}, report_start, args.figures / "gb_cumulative_net_revenue.png")
    figure_stack(
        {label: summary["combined_stack"][label] for label in labels},
        args.figures / "gb_revenue_stack.png",
        "Combined stack: revenue by stream, GBP per MW and year",
    )
    figure_market(auctions, market, report_start, args.figures / "gb_market_context.png")

    ledger_meta = {
        "period_start_utc": REPORT_START,
        "period_end_utc": f"{end:%Y-%m-%dT%H:%M:%SZ}",
        "battery": {
            "power_mw": DEFAULT_BATTERY.power_mw,
            "energy_mwh": DEFAULT_BATTERY.energy_mwh,
            "charge_efficiency": DEFAULT_BATTERY.charge_efficiency,
            "discharge_efficiency": DEFAULT_BATTERY.discharge_efficiency,
            "soc_min_frac": DEFAULT_BATTERY.soc_min_frac,
            "soc_max_frac": DEFAULT_BATTERY.soc_max_frac,
            "degradation_eur_per_mwh": DEFAULT_BATTERY.degradation_eur_per_mwh,
        },
        "settlement": "30-minute SPs; wholesale at MID (traded-price proxy), imbalance at the single system price, "
        "response availability at EAC clearing prices",
        "currency": "GBP (EUR columns converted at the ECB reference rate of the delivery day)",
        "generated_by": "scripts/research/gb_backtest.py",
    }
    written = []
    for name in MAIN:
        meta = {**ledger_meta, "settings": {"config": str(MAIN_CONFIGS[name])}}
        path, size = write_ledger(name, results[name][0], args.ledgers, meta)
        written.append((path.name, size))
    path, size = write_daily_ledger(
        {n: results[n][0] for n in results}, args.ledgers, {**ledger_meta, "strategies": sorted(results)}
    )
    written.append((path.name, size))
    for stale in [*ORACLES, *SENSITIVITIES]:  # SP-level ledgers are kept for the causal strategies only
        for suffix in (".parquet", ".json"):
            (args.ledgers / f"gb_{stale}{suffix}").unlink(missing_ok=True)
    summary_json = {
        "generated_by": "scripts/research/gb_backtest.py",
        "periods": {label: [f"{lo:%Y-%m-%d}", f"{hi:%Y-%m-%d}"] for label, lo, hi in periods},
        "summary": summary,
        "full_period": full,
        "market_context": context_market,
        "diagnostics": {n: {k: v for k, v in results[n][1].items()} for n in results},
        "runtime_seconds": {n: results[n][2] for n in results},
        "settings": {
            "forecast": str(MAIN_FORECAST),
            "planning_hurdle_gbp_mwh": PLANNING_HURDLE_GBP_MWH,
            "development_window": DEV_WINDOW,
        },
        "modo_index_reference": {"url": MODO_STACK_URL, "values_gbp_k_per_mw_yr": MODO_INDEX},
    }
    (args.ledgers / "gb_summary.json").write_text(
        json.dumps(summary_json, indent=2, default=str) + "\n", encoding="utf-8"
    )

    deg_gbp = float(np.mean(DEFAULT_BATTERY.degradation_eur_per_mwh * market.gbp_per_eur[market.ts >= report_start]))
    lag = sp_meta["validation"]["publication_lag_minutes_after_sp_end"]
    resp_meta = json.loads(
        (REPO_ROOT / "data" / "public" / "gb" / "response_auctions.meta.json").read_text(encoding="utf-8")
    )
    energy_meta = json.loads(
        (REPO_ROOT / "data" / "public" / "gb" / "response_energy.meta.json").read_text(encoding="utf-8")
    )
    mid_meta = json.loads((REPO_ROOT / "data" / "public" / "gb" / "market_index.meta.json").read_text(encoding="utf-8"))
    mean_e = energy_meta["validation"]["mean_mwh_per_mw_per_sp"]
    rules = [
        f"30-minute settlement periods and a single imbalance price (BSC P305, {SOURCES['bsc_p305_single_price']}); "
        f"system sell and buy prices are identical in all {sp_meta['rows']:,} SPs of the data.",
        f"System prices are first published a median {lag['first_publication_median']:.0f} minutes (p95 "
        f"{lag['first_publication_p95']:.0f}) after the SP ends (BMRS createdDateTime of SPs still on their first "
        "publication); the backtest uses them only from 30 minutes after the SP end. NIV chasing fixes the position "
        "at gate closure (60 minutes before the SP), so the latest price it can use is four SPs old.",
        "MID is the volume-weighted price of half-hour, 1-, 2- and 4-hour products traded within eight hours before "
        f"gate closure ({SOURCES['mids_review_2024']}); wholesale positions are fixed 9 hours ahead and settled at "
        "the realised MID (traded-price proxy).",
        "EAC response auctions: gate closure 14:00 UK the day before, results 15:00, pay-as-clear per 4-hour EFA "
        f"block ({SOURCES['eac_auction_timing']}); delivery durations DC 15, DM 30, DR 60 minutes "
        f"({SOURCES['neso_dynamic_services']}); availability payment per SP = price x MW x 0.5 h, withheld when "
        f"the unit is unavailable ({SOURCES['neso_provider_guidance']}).",
        "Energy limited units: Response Energy Volume = MW x duration must be held from the start of each contracted "
        "block, relaxed by delivered energy and recovered at up to 20 % per SP from the fifth SP after a delivery "
        f"(Service Terms 6.11, {SOURCES['neso_service_terms_v6']}); Reserved Capacity of 10 / 20 / 40 % of the "
        "offered MW for DC / DM / DR, with offered plus reserved MW of all stacked products within the unit's "
        f"registered capacity (Procurement Rules 8.3.3.2, {SOURCES['neso_procurement_rules_v5']}; read as export "
        "plus import capability for a battery); the strategies keep offered plus reserved MW within 1 MW per "
        "direction.",
        "A MW cannot serve two services at once: per direction, response MW + baseline power <= 1 MW and the "
        "response energy volumes must fit in the SOC window along the baseline trajectory.",
        "Delivered response energy is applied in ABSVD (not paid, no imbalance); it is computed per SP from 15-second "
        "BMRS frequency with the Service Terms delivery curves (mean per MW and SP: DC "
        f"{mean_e['dcl_mwh_per_mw']:.4f} / {mean_e['dch_mwh_per_mw']:.4f} MWh, DM {mean_e['dml_mwh_per_mw']:.4f} / "
        f"{mean_e['dmh_mwh_per_mw']:.4f}, DR {mean_e['drl_mwh_per_mw']:.4f} / {mean_e['drh_mwh_per_mw']:.4f} "
        "low / high).",
    ]
    data_notes = [
        "Elexon BMRS (Insights Solution API): system prices DISEBSP, Market Index Data MID, 15-second frequency "
        "FREQ - 'Contains BMRS data © Elexon Limited copyright and database right 2023-2026', licence "
        "https://www.elexon.co.uk/bsc/data/balancing-mechanism-reporting-agent/copyright-licence-bmrs-data/ .",
        "NESO data portal: Enduring Auction Capability (EAC) auction results summary (DC/DM/DR per EFA block; "
        "QR/SR/BR kept as context) - 'Supported by National Energy SO Open Data', NESO Open Data Licence v1.0 "
        "https://www.neso.energy/data-portal/neso-open-licence .",
        "ECB euro foreign exchange reference rates (GBP per EUR) - Source: European Central Bank; used for the EUR "
        "columns and the GBP value of the 8 EUR/MWh degradation cost (as-of join of daily rates).",
        f"Coverage: {sp_meta['rows']:,} settlement periods from {sp_meta['coverage_start_utc'][:10]} to "
        f"{end_label}, no missing SP; MID has no traded volume in {mid_meta['validation']['mid_missing_rows']} SPs "
        "(settled at the system price); EAC response results complete for "
        f"{resp_meta['validation']['complete_efa_days']} EFA days; {market.energy_filled_sps} SPs without enough "
        "frequency samples use the trailing 7-day mean response energy.",
        f"Forecast settings and the 5 GBP/MWh planning hurdle were selected on the development window {DEV_WINDOW} "
        "before the full run, so results from July 2024 onwards are out of sample for them; the per-direction "
        "stacking limit is a modelling choice also checked on that window, with the alternatives shown under "
        "Sensitivities.",
        f"Runtime of this script run: {(time.perf_counter() - began) / 60:.1f} minutes.",
    ]
    scope = [
        "Simulated revenue for a price-taking 1 MW battery (results per MW of a pooled fleet; EAC orders are whole "
        "MW): offers never move clearing prices and are accepted whenever the clearing price is at or above the "
        "offer price. Cleared response volumes are about 0.5-1.5 GW per product, so a fleet cannot all earn these "
        "per-MW response values.",
        "MID is a proxy for within-day wholesale trading: fills are assumed at the half-hourly market index price "
        "with no bid-ask cost; day-ahead auctions and the Balancing Mechanism are not modelled.",
        "Response delivery is assumed perfect (performance factor 1) apart from SOE unavailability; frequency at 15 "
        "seconds misses sub-15-second excursions (mostly relevant for DC). Quick, Slow and Balancing Reserve and "
        "the Capacity Market are not part of the strategies.",
        "No grid charges, BSUoS/TNUoS, levies, taxes or auxiliary load; linear efficiency and degradation.",
    ]
    ctx = {
        "end_label": end_label,
        "periods": labels,
        "summary": summary,
        "full": full,
        "market": context_market,
        "deg_gbp_mean": deg_gbp,
        "rules": rules,
        "data_notes": data_notes,
        "scope": scope,
    }
    args.report.write_text(render(ctx), encoding="utf-8")
    print(
        f"wrote {args.report.relative_to(REPO_ROOT)}; ledgers: "
        + ", ".join(f"{n} {s / 1e6:.2f} MB" for n, s in written)
    )
    print(f"done in {(time.perf_counter() - began) / 60:.1f} min")


if __name__ == "__main__":
    main()
