"""Real-data DE-LU backtest: day-ahead only vs day-ahead + continuous-intraday re-trading at the ID AEP index.

Regenerates ``reports/intraday.md``, ``reports/figures/intraday_*.png`` and ``reports/ledgers/intraday_*``::

    .venv/bin/python scripts/research/intraday_backtest.py            # uses the local ID AEP table
    .venv/bin/python scripts/research/intraday_backtest.py --fetch    # downloads / refreshes it first

Data: DE-LU day-ahead prices from ``data/public/energy_charts`` (Energy-Charts, CC BY 4.0, Bundesnetzagentur |
SMARD.de) and the German TSOs' intraday index ID AEP from netztransparenz.de, built locally under
``data/raw/intraday`` (not redistributed: the site terms require prior written consent for reproduction).
Committed outputs contain only simulated revenues and aggregate statistics, never the quarter-hourly index.
Strategy, timing and cost assumptions: ``euroflex_bess_lab.markets.intraday`` and ``docs/intraday.md``.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from euroflex_bess_lab.markets.intraday import (
    MAIN_STRATEGY,
    STRATEGIES,
    IntradayConfig,
    IntradayMarket,
    LiveForecaster,
    audit_causality,
    fit_spread_model,
    run_parallel,
    summarize_daily,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
QUARTER_HOUR_ERA_START = date(2025, 10, 1)
DEFAULT_START = date(2024, 3, 1)
HISTORY_DAYS = 62
TRADABLE = ("da_only", "da_id_reopt_da", "da_id_reopt_profile", "da_id_rolling")
ID_STRATEGIES = ("da_id_reopt_da", "da_id_reopt_profile", "da_id_rolling")
SHORT_LABELS = {
    "da_only": "DA only (persistence)",
    "da_id_reopt_da": "DA + ID, re-optimised at DA prices",
    "da_id_reopt_profile": "DA + ID, + intraday profile",
    "da_id_rolling": "DA + ID, rolling (main)",
    "oracle_id": "Oracle: perfect ID index, persistence DA",
    "oracle_da": "Oracle: perfect DA only",
    "oracle_da_id": "Oracle: perfect DA + ID index",
}
COLORS = {
    "da_only": "#2a78d6",
    "da_id_rolling": "#eb6834",
    "da_id_reopt_da": "#1baf7a",
    "da_id_reopt_profile": "#eda100",
}
ERA_COLORS = {"hourly": "#6250d6", "quarter_hour": "#008300"}
INK = {"primary": "#0b0b0b", "secondary": "#52514e", "muted": "#898781", "grid": "#e1e0d9", "axis": "#c3c2b7"}
SURFACE = "#fcfcfb"
SLIPPAGE_GRID = (0.0, 0.5, 1.0, 2.0, 3.0, 5.0, 10.0)
FEE_GRID = (0.0, 0.15, 0.5, 1.0)
GATE_CLOSURE_GRID = (5, 30, 60)
LAG_GRID = (0, 15, 60)
DA_SOURCE = "data/public/energy_charts/da_prices_DE-LU.parquet (Energy-Charts; CC BY 4.0, Bundesnetzagentur | SMARD.de)"


@dataclass(frozen=True)
class Period:
    key: str
    label: str
    first: date
    last: date

    def days(self, available: Sequence[date]) -> list[date]:
        return [d for d in available if self.first <= d <= self.last]


def _add_months(day: date, months: int) -> date:
    index = day.year * 12 + (day.month - 1) + months
    return date(index // 12, index % 12 + 1, 1)


def define_periods(start: date, end: date) -> list[Period]:
    """Latest 12 full months, the 15-minute DA era to ``end``, the 12 months before (hourly DA era), all days."""

    month_after = _add_months(end, 1)
    last_full = end if end == month_after - timedelta(days=1) else _add_months(end, 0) - timedelta(days=1)
    latest_start = _add_months(last_full, -11)
    previous_start = _add_months(latest_start, -12)
    previous_end = latest_start - timedelta(days=1)
    periods = [
        Period(
            "latest_12m", f"Latest 12 full months ({latest_start:%b %Y} - {last_full:%b %Y})", latest_start, last_full
        ),
        Period(
            "quarter_hour_era",
            f"15-minute DA era to latest ({QUARTER_HOUR_ERA_START:%d %b %Y} - {end:%d %b %Y})",
            QUARTER_HOUR_ERA_START,
            end,
        ),
        Period(
            "previous_12m",
            f"Previous 12 months, hourly DA era ({previous_start:%b %Y} - {previous_end:%b %Y})",
            max(previous_start, start),
            previous_end,
        ),
        Period("full", f"All evaluated days ({start:%d %b %Y} - {end:%d %b %Y})", start, end),
    ]
    return [p for p in periods if p.first <= p.last]


# ---------------------------------------------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------------------------------------------


def latest_common_day() -> date:
    """Last local day covered by both the local ID AEP table and the committed day-ahead prices."""

    import pyarrow.parquet as pq

    from euroflex_bess_lab.public_data import data_root
    from euroflex_bess_lab.public_data.intraday import FETCH_COMMAND, intraday_index_path

    path = intraday_index_path()
    if not path.exists():
        raise FileNotFoundError(f"ID AEP table missing ({path.name}); build it with: {FETCH_COMMAND} (or --fetch)")
    stamps = pq.read_table(path, columns=["timestamp_utc"]).column("timestamp_utc").to_pandas()
    id_last = pd.Timestamp(stamps.max()).tz_convert("Europe/Berlin").date()
    sidecar = json.loads((data_root() / "energy_charts" / "da_prices_DE-LU.meta.json").read_text(encoding="utf-8"))
    da_end = pd.Timestamp(sidecar["coverage_end_utc"]).tz_convert("Europe/Berlin") - pd.Timedelta(minutes=15)
    return min(id_last, da_end.date())


def load_market(start: date, end: date) -> IntradayMarket:
    return IntradayMarket.from_public(start, end, history_days=HISTORY_DAYS)


# ---------------------------------------------------------------------------------------------------------------
# Experiments
# ---------------------------------------------------------------------------------------------------------------


def uplift_stats(daily: pd.DataFrame, period: Period, strategy: str, base: str = "da_only") -> tuple[float, float]:
    """Mean uplift of ``strategy`` over ``base`` in EUR/MW/yr and its standard error over days."""

    subset = daily[(daily["day"] >= period.first) & (daily["day"] <= period.last)]
    pivot = subset.pivot_table(index="day", columns="strategy", values="pnl_eur")
    if strategy not in pivot.columns or base not in pivot.columns or pivot.empty:
        return float("nan"), float("nan")
    diff = (pivot[strategy] - pivot[base]).to_numpy(dtype=float)
    se = diff.std(ddof=1) / np.sqrt(len(diff)) if len(diff) > 1 else float("nan")
    return float(diff.mean() * 365.0), float(se * 365.0)


def sensitivity_cases(base: IntradayConfig) -> list[tuple[str, str, float, IntradayConfig, tuple[str, ...]]]:
    """(group, label, value, config, strategies) for every sensitivity run (base case included per group)."""

    cases: list[tuple[str, str, float, IntradayConfig, tuple[str, ...]]] = []
    for slip in SLIPPAGE_GRID:
        cases.append(
            ("slippage", f"slippage {slip:g} EUR/MWh", slip, base.with_costs(slippage_eur_mwh=slip), ID_STRATEGIES)
        )
    for fee in FEE_GRID:
        cases.append(
            ("id_fee", f"intraday fee {fee:g} EUR/MWh", fee, base.with_costs(id_fee_eur_mwh=fee), ID_STRATEGIES)
        )
    for gc in GATE_CLOSURE_GRID:
        cases.append(
            (
                "gate_closure",
                f"decision {gc} min before delivery",
                gc,
                base.with_timing(gate_closure_minutes=gc),
                (MAIN_STRATEGY,),
            )
        )
    for lag in LAG_GRID:
        cases.append(
            (
                "visibility_lag",
                f"closing prices usable {lag} min after delivery end",
                lag,
                base.with_timing(visibility_lag_minutes=lag),
                (MAIN_STRATEGY,),
            )
        )
    return cases


def run_sensitivities(
    market: IntradayMarket,
    base: IntradayConfig,
    days: Sequence[date],
    base_daily: pd.DataFrame,
    periods: Sequence[Period],
    workers: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    da_only = base_daily[base_daily["strategy"] == "da_only"]
    for group, label, value, cfg, strategies in sensitivity_cases(base):
        daily = run_parallel(market, cfg, days, strategies=strategies, workers=workers, history_days=HISTORY_DAYS)
        daily = pd.concat([daily, da_only[da_only["day"].isin(days)]], ignore_index=True)
        for period in periods:
            summary = summarize_daily(daily, cfg, first=period.first, last=period.last)
            if summary.empty:
                continue
            for strategy in strategies:
                if strategy not in summary.index:
                    continue
                mean, se = uplift_stats(daily, period, strategy)
                rows.append(
                    {
                        "group": group,
                        "case": label,
                        "value": float(value),
                        "period": period.key,
                        "strategy": strategy,
                        "pnl_eur_per_mw_year": float(summary.loc[strategy, "pnl_eur_per_mw_year"]),
                        "uplift_eur_per_mw_year": mean,
                        "uplift_se_eur_per_mw_year": se,
                        "id_volume_mwh_per_year": float(summary.loc[strategy, "id_volume_mwh_per_year"]),
                        "cycles_per_day": float(summary.loc[strategy, "cycles_per_day"]),
                    }
                )
    return pd.DataFrame(rows)


def forecast_skill(market: IntradayMarket, config: IntradayConfig, periods: Sequence[Period]) -> pd.DataFrame:
    """Error of the execution-price forecast each information set gives for product k at its decision time."""

    rows = []
    offset = config.timing.visible_offset_qh
    for period in periods:
        errors: dict[str, list[np.ndarray]] = {"da": [], "profile": [], "profile_live": []}
        for day in period.days(market.days):
            lo, hi = market.day_bounds(day)
            model = fit_spread_model(market, day, config.forecast)
            live = LiveForecaster(model, market, lo, hi, offset)
            actual = market.id_price[lo:hi]
            errors["da"].append(actual - market.da_price[lo:hi])
            errors["profile"].append(actual - model.base(market, lo, hi))
            errors["profile_live"].append(actual - np.array([live.forecast(k)[0] for k in range(hi - lo)]))
        if not errors["da"]:
            continue
        for kind, chunks in errors.items():
            err = np.concatenate(chunks)
            err = err[np.isfinite(err)]
            rows.append(
                {
                    "period": period.key,
                    "forecast": kind,
                    "n": int(err.size),
                    "mae_eur_mwh": float(np.mean(np.abs(err))),
                    "rmse_clipped_eur_mwh": float(np.sqrt(np.mean(np.clip(err, -300.0, 300.0) ** 2))),
                }
            )
    return pd.DataFrame(rows)


def causality_audit(
    market: IntradayMarket, config: IntradayConfig, days: Sequence[date], *, n_days: int, seed: int = 20261026
) -> list[dict[str, Any]]:
    rng = np.random.default_rng(seed)
    candidates = [d for d in days if market.day_position(d) >= HISTORY_DAYS]
    chosen = sorted(rng.choice(len(candidates), size=min(n_days, len(candidates)), replace=False))
    records: list[dict[str, Any]] = []
    for index in chosen:
        day = candidates[int(index)]
        lo, hi = market.day_bounds(day)
        cut = int(rng.integers(8, hi - lo - 8))
        position = market.day_position(day)
        # keep two later days so look-ahead into the next days would also be caught by the perturbation
        sub = market.slice_days(
            market.days[max(0, position - HISTORY_DAYS)], market.days[min(len(market.days) - 1, position + 2)]
        )
        for strategy in (
            "da_only",
            "da_id_reopt_da",
            "da_id_reopt_profile",
            "da_id_rolling",
            "oracle_id",
            "oracle_da_id",
        ):
            strategy_cut = cut if STRATEGIES[strategy].kind in {"id_rolling", "oracle_id", "oracle_da_id"} else 0
            records.append(
                audit_causality(sub, day, strategy, config, cut=strategy_cut, seed=int(rng.integers(1 << 30)))
            )
    return records


def market_context(market: IntradayMarket, periods: Sequence[Period]) -> pd.DataFrame:
    rows = []
    for period in periods:
        days = period.days(market.days)
        if not days:
            continue
        lo, _ = market.day_bounds(days[0])
        _, hi = market.day_bounds(days[-1])
        da = market.da_price[lo:hi]
        idp = market.id_price[lo:hi]
        ok = np.isfinite(idp)
        spread = idp[ok] - da[ok]
        rows.append(
            {
                "period": period.key,
                "mean_da_eur_mwh": float(np.mean(da)),
                "mean_id_eur_mwh": float(np.mean(idp[ok])),
                "mean_abs_spread_eur_mwh": float(np.mean(np.abs(spread))),
                "std_spread_eur_mwh": float(np.std(spread)),
                "share_abs_spread_gt_50": float(np.mean(np.abs(spread) > 50.0)),
                "undefined_index_qh": int((~ok).sum()),
            }
        )
    return pd.DataFrame(rows)


def spread_profile(market: IntradayMarket, period: Period) -> np.ndarray:
    """Mean ID AEP minus day-ahead per local quarter-hour of the day over ``period`` (aggregate statistic)."""

    days = period.days(market.days)
    lo, _ = market.day_bounds(days[0])
    _, hi = market.day_bounds(days[-1])
    spread = market.id_price[lo:hi] - market.da_price[lo:hi]
    slot = market.slot[lo:hi]
    ok = np.isfinite(spread)
    sums = np.bincount(slot[ok], weights=spread[ok], minlength=96)
    counts = np.bincount(slot[ok], minlength=96)
    out: np.ndarray = np.where(counts > 0, sums / np.maximum(counts, 1), np.nan)
    return out


def monthly_table(daily: pd.DataFrame, period: Period) -> pd.DataFrame:
    subset = daily[(daily["day"] >= period.first) & (daily["day"] <= period.last)].copy()
    subset["month"] = pd.to_datetime(subset["day"]).dt.strftime("%Y-%m")
    pnl = subset.pivot_table(index="month", columns="strategy", values="pnl_eur", aggfunc="sum")
    days = subset[subset["strategy"] == "da_only"].groupby("month")["day"].nunique()
    pnl["days"] = days
    return pnl


# ---------------------------------------------------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------------------------------------------------


def _style_axes(ax: Any) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(INK["axis"])
    ax.tick_params(colors=INK["secondary"], labelsize=9)
    ax.yaxis.grid(True, color=INK["grid"], linewidth=0.8)
    ax.set_axisbelow(True)


def _place_end_labels(ax: Any, items: Sequence[tuple[float, float, str]], *, min_gap_frac: float = 0.065) -> None:
    """Direct labels at the right end of lines, spread vertically so they never overlap (thin leader lines).

    ``items`` are ``(x, y, text)`` in data coordinates (dates as matplotlib date numbers); call after plotting.
    """

    if not items:
        return
    y_lo, y_hi = ax.get_ylim()
    x_lo, x_hi = ax.get_xlim()
    gap = (y_hi - y_lo) * min_gap_frac
    order = sorted(range(len(items)), key=lambda i: items[i][1])
    placed: list[float] = []
    for i in order:
        y = items[i][1]
        if placed and y - placed[-1] < gap:
            y = placed[-1] + gap
        placed.append(y)
    overflow = placed[-1] - (y_hi - 0.3 * gap)
    if overflow > 0:
        placed = [y - overflow for y in placed]
    dx = (x_hi - x_lo) * 0.015
    for i, y_text in zip(order, placed, strict=True):
        x, y, text = items[i]
        ax.annotate(
            text,
            xy=(x, y),
            xytext=(x + dx, y_text),
            textcoords="data",
            va="center",
            fontsize=8,
            color=INK["secondary"],
            arrowprops={"arrowstyle": "-", "color": INK["axis"], "linewidth": 0.6, "shrinkA": 0, "shrinkB": 1},
        )


def make_figures(
    daily: pd.DataFrame,
    sensitivity: pd.DataFrame,
    profiles: dict[str, np.ndarray],
    periods: Sequence[Period],
    figures_dir: Path,
) -> list[Path]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    figures_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    full = next(p for p in periods if p.key == "full")
    table = monthly_table(daily, full)
    months = list(table.index)
    x = np.arange(len(months))
    labels = [pd.Timestamp(f"{m}-01").strftime("%b\n%Y") if i % 3 == 0 else "" for i, m in enumerate(months)]
    era_index = next((i for i, m in enumerate(months) if m >= f"{QUARTER_HOUR_ERA_START:%Y-%m}"), None)

    # 1. Monthly uplift over day-ahead only
    fig, ax = plt.subplots(figsize=(11, 4.6), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    _style_axes(ax)
    ax.axhline(0.0, color=INK["axis"], linewidth=1.0, zorder=1)
    end_labels: list[tuple[float, float, str]] = []
    for strategy in ("da_id_reopt_da", "da_id_reopt_profile", "da_id_rolling"):
        uplift = (table[strategy] - table["da_only"]).to_numpy() / 1000.0
        ax.plot(
            x, uplift, color=COLORS[strategy], linewidth=2.0, marker="o", markersize=4, label=SHORT_LABELS[strategy]
        )
        end_labels.append((float(x[-1]), float(uplift[-1]), SHORT_LABELS[strategy].replace("DA + ID, ", "")))
    if era_index is not None:
        ax.axvline(era_index - 0.5, color=INK["muted"], linewidth=1.0, linestyle="--")
        ax.text(
            era_index - 0.4, ax.get_ylim()[1] * 0.95, "15-minute day-ahead products", fontsize=8, color=INK["secondary"]
        )
    ax.set_xticks(x, labels)
    ax.set_xlim(-0.5, len(months) + 5.5)
    _place_end_labels(ax, end_labels)
    ax.set_ylabel("Uplift over DA only, kEUR per MW per month", color=INK["secondary"], fontsize=9)
    ax.set_title(
        "Germany 1 MW / 2 MWh: monthly value of intraday re-trading on top of day-ahead (ID AEP execution proxy)",
        loc="left",
        color=INK["primary"],
        fontsize=11,
    )
    ax.legend(
        frameon=False, fontsize=8, ncols=3, loc="upper center", bbox_to_anchor=(0.5, -0.16), labelcolor=INK["secondary"]
    )
    fig.tight_layout()
    path = figures_dir / "intraday_monthly_uplift.png"
    fig.savefig(path, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)
    written.append(path)

    # 2. Cumulative net revenue
    subset = daily[(daily["day"] >= full.first) & (daily["day"] <= full.last)]
    pivot = subset.pivot_table(index="day", columns="strategy", values="pnl_eur").sort_index()
    cumulative = pivot.cumsum() / 1000.0
    fig, ax = plt.subplots(figsize=(11, 4.6), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    _style_axes(ax)
    import matplotlib.dates as mdates

    stamps = pd.to_datetime(cumulative.index)
    x_end = float(mdates.date2num(stamps[-1]))
    end_labels = []
    for strategy in ("da_only", "da_id_reopt_da", "da_id_rolling"):
        ax.plot(stamps, cumulative[strategy], color=COLORS[strategy], linewidth=2.0, label=SHORT_LABELS[strategy])
        end_labels.append(
            (x_end, float(cumulative[strategy].iloc[-1]), SHORT_LABELS[strategy].replace("DA + ID, ", ""))
        )
    ax.plot(
        stamps,
        cumulative["oracle_da"],
        color=INK["primary"],
        linewidth=1.2,
        linestyle="--",
        label=SHORT_LABELS["oracle_da"],
    )
    end_labels.append((x_end, float(cumulative["oracle_da"].iloc[-1]), "oracle DA only"))
    ax.axvline(pd.Timestamp(QUARTER_HOUR_ERA_START), color=INK["muted"], linewidth=1.0, linestyle="--")
    ax.set_xlim(stamps[0], stamps[-1] + pd.Timedelta(days=130))
    _place_end_labels(ax, end_labels)
    ax.set_ylabel("Cumulative net revenue, kEUR per MW", color=INK["secondary"], fontsize=9)
    ax.set_title(
        "Cumulative net revenue: day-ahead only vs day-ahead + intraday re-trading",
        loc="left",
        color=INK["primary"],
        fontsize=11,
    )
    ax.legend(
        frameon=False, fontsize=8, ncols=4, loc="upper center", bbox_to_anchor=(0.5, -0.12), labelcolor=INK["secondary"]
    )
    fig.tight_layout()
    path = figures_dir / "intraday_cumulative_net_revenue.png"
    fig.savefig(path, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)
    written.append(path)

    # 3. Cost sensitivity (15-minute era)
    sens = sensitivity[(sensitivity["group"] == "slippage") & (sensitivity["period"] == "quarter_hour_era")]
    if not sens.empty:
        fig, ax = plt.subplots(figsize=(8, 4.4), dpi=150)
        fig.patch.set_facecolor(SURFACE)
        _style_axes(ax)
        ax.axhline(0.0, color=INK["axis"], linewidth=1.0)
        end_labels = []
        for strategy in ("da_id_reopt_da", "da_id_reopt_profile", "da_id_rolling"):
            rows = sens[sens["strategy"] == strategy].sort_values("value")
            values = rows["uplift_eur_per_mw_year"].to_numpy() / 1000.0
            ax.plot(
                rows["value"],
                values,
                color=COLORS[strategy],
                linewidth=2.0,
                marker="o",
                markersize=5,
                label=SHORT_LABELS[strategy],
            )
            end_labels.append(
                (float(rows["value"].iloc[-1]), float(values[-1]), SHORT_LABELS[strategy].replace("DA + ID, ", ""))
            )
        ax.set_xlim(-0.3, max(SLIPPAGE_GRID) * 1.45)
        _place_end_labels(ax, end_labels)
        ax.set_xlabel(
            "Slippage vs the ID AEP index, EUR/MWh traded (fee 0.15 EUR/MWh on top)", color=INK["secondary"], fontsize=9
        )
        ax.set_ylabel("Uplift over DA only, kEUR per MW per year", color=INK["secondary"], fontsize=9)
        ax.set_title(
            "Intraday uplift vs execution cost (15-minute DA era)", loc="left", color=INK["primary"], fontsize=11
        )
        ax.legend(
            frameon=False,
            fontsize=8,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.16),
            ncols=3,
            labelcolor=INK["secondary"],
        )
        fig.tight_layout()
        path = figures_dir / "intraday_cost_sensitivity.png"
        fig.savefig(path, facecolor=SURFACE, bbox_inches="tight")
        plt.close(fig)
        written.append(path)

    # 4. Average intraday-minus-day-ahead spread by local quarter-hour (aggregate statistic)
    if profiles:
        fig, ax = plt.subplots(figsize=(11, 4.2), dpi=150)
        fig.patch.set_facecolor(SURFACE)
        _style_axes(ax)
        hours = np.arange(96) / 4.0
        ax.axhline(0.0, color=INK["axis"], linewidth=1.0)
        end_labels = []
        for key, label in (("hourly", "hourly DA products"), ("quarter_hour", "15-minute DA products")):
            if key not in profiles:
                continue
            ax.plot(hours, profiles[key], color=ERA_COLORS[key], linewidth=1.6, marker="o", markersize=2.5, label=label)
            end_labels.append((float(hours[-1]), float(profiles[key][-1]), label))
        ax.set_xticks(np.arange(0, 25, 3), [f"{h:02d}:00" for h in range(0, 25, 3)])
        ax.set_xlim(0, 27.8)
        _place_end_labels(ax, end_labels)
        ax.set_xlabel("Local time of day (Europe/Berlin), quarter-hour start", color=INK["secondary"], fontsize=9)
        ax.set_ylabel("Mean ID AEP minus DA price, EUR/MWh", color=INK["secondary"], fontsize=9)
        ax.set_title(
            "Average intraday-minus-day-ahead spread per quarter-hour: the intra-hour pattern before and after 15-minute DA",
            loc="left",
            color=INK["primary"],
            fontsize=11,
        )
        ax.legend(
            frameon=False,
            fontsize=8,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.16),
            ncols=2,
            labelcolor=INK["secondary"],
        )
        fig.tight_layout()
        path = figures_dir / "intraday_spread_profile.png"
        fig.savefig(path, facecolor=SURFACE, bbox_inches="tight")
        plt.close(fig)
        written.append(path)
    return written


# ---------------------------------------------------------------------------------------------------------------
# Ledgers
# ---------------------------------------------------------------------------------------------------------------


def daily_ledger(daily: pd.DataFrame) -> pd.DataFrame:
    """Valuation-ready daily ledger (one row per day and strategy); contains no intraday prices."""

    out = daily.copy()
    out["timestamp_utc"] = [
        pd.Timestamp(d).tz_localize("Europe/Berlin").tz_convert("UTC") for d in pd.to_datetime(out["day"])
    ]
    out["day"] = pd.to_datetime(out["day"]).dt.strftime("%Y-%m-%d")
    out["mechanism"] = "de_lu_day_ahead_plus_id_aep"
    out["fleet_mw"] = 1.0
    out["revenue_eur"] = out["pnl_eur"] + out["degradation_eur"]
    columns = [
        "timestamp_utc",
        "day",
        "strategy",
        "mechanism",
        "fleet_mw",
        "revenue_eur",
        "degradation_eur",
        "pnl_eur",
        "da_revenue_eur",
        "id_revenue_eur",
        "da_fees_eur",
        "id_fees_eur",
        "slippage_eur",
        "discharged_mwh",
        "charged_mwh",
        "da_sold_mwh",
        "da_bought_mwh",
        "id_sold_mwh",
        "id_bought_mwh",
        "fallback_trades",
        "lp_fallbacks",
        "soc_violations",
    ]
    frame = out[columns].copy()
    for column in columns[5:-3]:
        frame[column] = frame[column].astype("float64")
    return frame.sort_values(["timestamp_utc", "strategy"], kind="stable").reset_index(drop=True)


def _jsonable(value: Any) -> Any:
    """Recursively convert numpy / pandas scalars so ``json.dumps`` writes numbers, not strings."""

    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, float) and not np.isfinite(value):
        return None
    if isinstance(value, date | pd.Timestamp):
        return str(value)
    return value


def write_ledgers(
    ledgers_dir: Path,
    daily: pd.DataFrame,
    sensitivity: pd.DataFrame,
    audit: dict[str, Any],
    config: IntradayConfig,
    periods: Sequence[Period],
) -> list[Path]:
    ledgers_dir.mkdir(parents=True, exist_ok=True)
    written = []
    ledger = daily_ledger(daily)
    path = ledgers_dir / "intraday_daily.parquet"
    ledger.to_parquet(path, index=False, compression="zstd")
    written.append(path)
    sidecar = {
        "table": "intraday_daily",
        "owner": "intraday",
        "description": "Daily P&L per strategy of the DE-LU day-ahead + continuous-intraday backtest (1 MW / 2 MWh). "
        "pnl_eur = da_revenue - da_fees + id_revenue - id_fees - slippage - degradation; revenue_eur = pnl_eur + "
        "degradation_eur. Daily (not quarter-hourly) on purpose: quarter-hour ledgers would let readers back out the "
        "ID AEP index, which is not redistributable.",
        "period": [f"{periods[-1].first}", f"{periods[-1].last}"],
        "strategies": {name: STRATEGIES[name].label for name in sorted(set(ledger["strategy"]))},
        "settings": config.describe(),
        "intraday_price": "ID AEP (netztransparenz.de) as a VWAP-execution proxy; DA-price fallback when undefined",
        "data_sources": {
            "day_ahead": DA_SOURCE,
            "intraday": "netztransparenz.de Index Ausgleichsenergiepreis, built locally under data/raw/intraday (not "
            "redistributed; reproduction requires the German TSOs' prior written consent)",
        },
        "generated_by": "scripts/research/intraday_backtest.py",
        "rows": int(len(ledger)),
    }
    path = ledgers_dir / "intraday_daily.json"
    path.write_text(json.dumps(_jsonable(sidecar), indent=2, default=str) + "\n", encoding="utf-8")
    written.append(path)
    path = ledgers_dir / "intraday_sensitivity.parquet"
    sensitivity.to_parquet(path, index=False, compression="zstd")
    written.append(path)
    path = ledgers_dir / "intraday_audit.json"
    path.write_text(json.dumps(_jsonable(audit), indent=2, default=str) + "\n", encoding="utf-8")
    written.append(path)
    return written


# ---------------------------------------------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------------------------------------------


def _eur(value: float) -> str:
    return "n/a" if not np.isfinite(value) else f"{value:,.0f}"


def _signed(value: float) -> str:
    return "n/a" if not np.isfinite(value) else f"{value:+,.0f}"


def _pct(value: float) -> str:
    return "n/a" if not np.isfinite(value) else f"{100.0 * value:.1f} %"


def _period_table(daily: pd.DataFrame, config: IntradayConfig, period: Period) -> list[str]:
    summary = summarize_daily(daily, config, first=period.first, last=period.last)
    base = float(summary.loc["da_only", "pnl_eur_per_mw_year"])
    lines = [
        "| Strategy | Net EUR/MW/yr | Uplift vs DA only (± 1 SE) | Uplift % | ID traded MWh/yr | Cycles/day | Share of oracle DA + ID |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name in summary.index:
        row = summary.loc[name]
        if name == "da_only":
            uplift = "-"
            pct = "-"
        elif STRATEGIES[name].tradable:
            mean, se = uplift_stats(daily, period, name)
            uplift = f"{_signed(mean)} (± {_eur(se)})"
            pct = _pct(mean / base)
        else:
            uplift = _signed(float(row["uplift_vs_da_only_eur_per_mw_year"]))
            pct = _pct(float(row["uplift_vs_da_only_eur_per_mw_year"]) / base)
        label = SHORT_LABELS[name]
        if name == MAIN_STRATEGY:
            label = f"**{label}**"
        lines.append(
            f"| {label} | {_eur(float(row['pnl_eur_per_mw_year']))} | {uplift} | {pct} | "
            f"{_eur(float(row['id_volume_mwh_per_year']))} | {float(row['cycles_per_day']):.2f} | "
            f"{_pct(float(row['capture_vs_oracle_da_id']))} |"
        )
    return lines


def _decomposition_table(daily: pd.DataFrame, config: IntradayConfig, period: Period) -> list[str]:
    summary = summarize_daily(daily, config, first=period.first, last=period.last)
    lines = [
        "| Strategy | DA revenue | ID revenue at index | DA fees | ID fees | Slippage | Degradation | Net |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name in TRADABLE:
        row = summary.loc[name]
        lines.append(
            f"| {SHORT_LABELS[name]} | {_eur(row['da_revenue_eur_per_mw_year'])} | {_eur(row['id_revenue_eur_per_mw_year'])} | "
            f"-{_eur(row['da_fees_eur_per_mw_year'])} | -{_eur(row['id_fees_eur_per_mw_year'])} | "
            f"-{_eur(row['slippage_eur_per_mw_year'])} | -{_eur(row['degradation_eur_per_mw_year'])} | "
            f"{_eur(row['pnl_eur_per_mw_year'])} |"
        )
    return lines


def _break_even(sensitivity: pd.DataFrame, period: str, strategy: str) -> str:
    rows = sensitivity[
        (sensitivity["group"] == "slippage") & (sensitivity["period"] == period) & (sensitivity["strategy"] == strategy)
    ].sort_values("value")
    if rows.empty:
        return "n/a"
    values = rows["value"].to_numpy()
    uplift = rows["uplift_eur_per_mw_year"].to_numpy()
    for i in range(1, len(values)):
        if uplift[i - 1] > 0 >= uplift[i]:
            frac = uplift[i - 1] / (uplift[i - 1] - uplift[i])
            return f"{values[i - 1] + frac * (values[i] - values[i - 1]):.1f} EUR/MWh"
    return f"above {values[-1]:g} EUR/MWh" if uplift[-1] > 0 else f"below {values[0]:g} EUR/MWh"


def render_report(
    *,
    daily: pd.DataFrame,
    sensitivity: pd.DataFrame,
    skill: pd.DataFrame,
    audit_records: list[dict[str, Any]],
    context: pd.DataFrame,
    periods: Sequence[Period],
    config: IntradayConfig,
    figures: Sequence[Path],
    coverage: dict[str, Any],
    runtime_minutes: float,
    report_path: Path,
) -> str:
    by_key = {p.key: p for p in periods}
    latest = by_key.get("latest_12m")
    era = by_key.get("quarter_hour_era")
    previous = by_key.get("previous_12m")
    costs = config.costs
    timing = config.timing

    def net(period: Period, name: str) -> float:
        return float(
            summarize_daily(daily, config, first=period.first, last=period.last).loc[name, "pnl_eur_per_mw_year"]
        )

    lines: list[str] = [
        "# Germany (DE-LU) intraday re-trading on real data: day-ahead + continuous intraday vs day-ahead only",
        "",
    ]
    lines += [
        "Every number on this page is produced by `scripts/research/intraday_backtest.py` "
        "(`.venv/bin/python scripts/research/intraday_backtest.py`). Model, data access and limits: "
        "[docs/intraday.md](../docs/intraday.md).",
        "",
        f"**Setup.** 1 MW / 2 MWh battery, SOC window 5-95 %, 50 % at the start and end of every delivery day, charge "
        f"and discharge efficiency 0.95 / 0.95, degradation {config.battery.degradation_eur_per_mwh:g} EUR per MWh "
        "discharged. *Day-ahead stage* at D-1 12:00 Europe/Berlin: a price-taking schedule on the persistence forecast "
        "(previous local day, as in [reports/germany.md](germany.md)), held constant over hourly products before the "
        "15-minute MTU of 1 Oct 2025 and settled at realised DE-LU day-ahead prices. *Intraday stage*: re-trades "
        f"around the day-ahead position product by product; the quantity for quarter-hour k is frozen {timing.gate_closure_minutes} "
        "minutes before delivery and executed at the German TSOs' **ID AEP** index of that quarter-hour, used as a "
        "**VWAP-execution proxy** (the index is the volume-weighted price of the last 500 MW traded in the product), "
        f"less / plus {costs.slippage_eur_mwh:g} EUR/MWh slippage and {costs.id_fee_eur_mwh:g} EUR/MWh exchange fee "
        f"(day-ahead fee {costs.da_fee_eur_mwh:g} EUR/MWh). Decisions use only information visible at decision time: "
        "day-ahead prices from 13:00 on D-1, closing prices of products whose delivery has ended (an exchange member's "
        f"trade feed; product k sees products up to k-{timing.visible_offset_qh}) and a trailing "
        f"{config.forecast.profile_days}-day intraday profile from days up to D-2. Oracles know realised prices and are "
        "upper bounds, not tradable. EUR/MW/yr = period net revenue x 365 / evaluated days / 1 MW; ± is one standard "
        "error over days.",
        "",
        "Strategies: *re-optimised at DA prices* re-plans the day once after the day-ahead auction with the DA price as "
        "the forecast of every intraday price; *+ intraday profile* adds the trailing quarter-of-day intraday spread "
        "profile (published index only); *rolling (main)* re-optimises before every gate closure with the profile and "
        "the decaying residual of the latest closing prices.",
        "",
        "## Key findings",
        "",
    ]
    if latest is not None:
        base_l, main_l = net(latest, "da_only"), net(latest, MAIN_STRATEGY)
        mean_l, se_l = uplift_stats(daily, latest, MAIN_STRATEGY)
        lines.append(
            f"- **Intraday re-trading adds {_signed(mean_l)} EUR/MW/yr (± {_eur(se_l)}, +{100 * mean_l / base_l:.1f} %) on top "
            f"of day-ahead in the latest 12 full months ({latest.first:%b %Y} - {latest.last:%b %Y})**: "
            f"{_eur(base_l)} EUR/MW/yr with day-ahead only, "
            f"{_eur(main_l)} with the rolling day-ahead + intraday strategy."
        )
    if era is not None:
        mean_e, se_e = uplift_stats(daily, era, MAIN_STRATEGY)
        base_e = net(era, "da_only")
        repair, _ = uplift_stats(daily, era, "da_id_reopt_da")
        oracle_gap = net(era, "oracle_da") - base_e
        lines.append(
            f"- **15-minute DA era ({era.first:%d %b %Y} - {era.last:%d %b %Y}): {_signed(mean_e)} EUR/MW/yr (± {_eur(se_e)}), "
            f"+{100 * mean_e / base_e:.1f} %.** Most of it comes from one simple step: re-planning the battery "
            f"once the day-ahead prices are known and trading the difference intraday earns {_signed(repair)} EUR/MW/yr, "
            f"against a {_eur(oracle_gap)} EUR/MW/yr gap between the persistence and the perfect-foresight day-ahead schedule."
        )
    if previous is not None:
        mean_p, se_p = uplift_stats(daily, previous, MAIN_STRATEGY)
        prof_p, prof_se = uplift_stats(daily, previous, "da_id_reopt_profile", base="da_id_reopt_da")
        lines.append(
            f"- **With hourly day-ahead products (previous 12 months) the quarter-hour structure inside each hour is worth "
            f"extra:** the rolling strategy adds {_signed(mean_p)} EUR/MW/yr (± {_eur(se_p)}); the intraday profile alone "
            f"contributes {_signed(prof_p)} (± {_eur(prof_se)}) of it, because intraday quarter-hour prices follow a stable "
            "ramp pattern within hourly products (see the spread-profile figure)."
        )
    if era is not None and not sensitivity.empty:
        be = _break_even(sensitivity, "quarter_hour_era", MAIN_STRATEGY)
        worst = sensitivity[
            (sensitivity["group"] == "slippage")
            & (sensitivity["period"] == "quarter_hour_era")
            & (sensitivity["strategy"] == MAIN_STRATEGY)
        ].sort_values("value")
        if be.startswith("above") and not worst.empty:
            top = worst.iloc[-1]
            lines.append(
                f"- **Robust to execution cost:** in the 15-minute era the uplift stays positive over the whole tested "
                f"range; at {top['value']:g} EUR/MWh slippage against the index (fee on top) the rolling strategy still "
                f"adds {_signed(float(top['uplift_eur_per_mw_year']))} EUR/MW/yr, because the optimiser trades less as "
                "costs rise."
            )
        else:
            lines.append(
                f"- **Execution cost:** in the 15-minute era the uplift stays positive up to a slippage of about {be} "
                "against the index (fee on top); the optimiser trades less as costs rise."
            )
    tradable_audit = [r for r in audit_records if r["tradable"]]
    oracle_audit = [r for r in audit_records if not r["tradable"]]
    n_days_audit = len({r["day"] for r in audit_records})
    lines.append(
        f"- **Causal by construction and by test:** in a perturbation audit on {n_days_audit} real days (all later intraday "
        f"prices and later days replaced by noise) {sum(r['passed'] for r in tradable_audit)} of {len(tradable_audit)} "
        "tradable runs kept every protected decision unchanged, while the oracles changed decisions taken before the "
        f"perturbation in {sum(r['lookahead_detected'] for r in oracle_audit)} of {len(oracle_audit)} runs (look-ahead "
        "detected, as designed)."
    )
    if latest is not None:
        lines.append(
            f"- **Headroom:** perfect foresight of the index would be worth {_eur(net(latest, 'oracle_da_id'))} EUR/MW/yr "
            "(latest 12 months); the index is very volatile close to delivery, so the oracle is a loose upper bound, not a "
            "target."
        )
    lines.append("")
    for period in periods:
        if period.key == "full":
            continue
        n_days = len(period.days(sorted(set(daily["day"]))))
        lines += [f"## {period.label}", "", f"Evaluated delivery days: {n_days}.", ""]
        lines += _period_table(daily, config, period)
        lines.append("")
        if period.key == "latest_12m":
            lines += ["Revenue decomposition (EUR/MW/yr):", ""]
            lines += _decomposition_table(daily, config, period)
            lines.append("")
    names = {p.name for p in figures}
    if "intraday_monthly_uplift.png" in names:
        lines += ["![Monthly uplift](figures/intraday_monthly_uplift.png)", ""]
    if "intraday_cumulative_net_revenue.png" in names:
        lines += ["![Cumulative net revenue](figures/intraday_cumulative_net_revenue.png)", ""]
    if era is not None:
        table = monthly_table(daily, era)
        lines += [
            f"## Monthly breakdown ({era.label})",
            "",
            "Net revenue in EUR per MW for the month.",
            "",
            "| Month | Days | DA only | DA + ID rolling | Uplift | Uplift % |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
        for month, row in table.iterrows():
            uplift = row[MAIN_STRATEGY] - row["da_only"]
            pct = uplift / row["da_only"] if row["da_only"] > 0 else float("nan")
            lines.append(
                f"| {month} | {int(row['days'])} | {_eur(row['da_only'])} | {_eur(row[MAIN_STRATEGY])} | "
                f"{_signed(uplift)} | {_pct(pct)} |"
            )
        lines.append("")
    # Sensitivities
    lines += ["## Sensitivities", ""]
    if not sensitivity.empty:
        lines += [
            "Uplift over DA only in EUR/MW/yr. Cost cases re-run all three intraday strategies; timing cases re-run the "
            "rolling strategy (the others decide before the day starts).",
            "",
            "| Case | Period | Re-optimised at DA prices | + intraday profile | Rolling (main) | Rolling ID traded MWh/yr |",
            "| --- | --- | ---: | ---: | ---: | ---: |",
        ]
        for (_group, _value, period_key), rows in sensitivity.groupby(["group", "value", "period"], sort=False):
            if period_key not in {"quarter_hour_era", "previous_12m"}:
                continue
            by_strategy = rows.set_index("strategy")
            label = rows["case"].iloc[0]

            def cell(name: str, frame: pd.DataFrame = by_strategy) -> str:
                return _signed(float(frame.loc[name, "uplift_eur_per_mw_year"])) if name in frame.index else "-"

            vol = (
                _eur(float(by_strategy.loc[MAIN_STRATEGY, "id_volume_mwh_per_year"]))
                if MAIN_STRATEGY in by_strategy.index
                else "-"
            )
            period_label = "15-min era" if period_key == "quarter_hour_era" else "hourly era (prev. 12 m)"
            lines.append(
                f"| {label} | {period_label} | {cell('da_id_reopt_da')} | {cell('da_id_reopt_profile')} | "
                f"{cell(MAIN_STRATEGY)} | {vol} |"
            )
        lines.append("")
        lines.append(
            f"Base case: slippage {costs.slippage_eur_mwh:g} EUR/MWh, intraday fee {costs.id_fee_eur_mwh:g} EUR/MWh, "
            f"decision {timing.gate_closure_minutes} minutes before delivery, closing prices usable "
            f"{timing.visibility_lag_minutes} minutes after delivery end. Break-even slippage of the rolling strategy: "
            f"{_break_even(sensitivity, 'quarter_hour_era', MAIN_STRATEGY)} (15-minute era), "
            f"{_break_even(sensitivity, 'previous_12m', MAIN_STRATEGY)} (hourly era)."
        )
        lines.append("")
    if "intraday_cost_sensitivity.png" in names:
        lines += ["![Cost sensitivity](figures/intraday_cost_sensitivity.png)", ""]
    # Forecast skill
    if not skill.empty:
        lines += [
            "## Forecast skill of the intraday execution price",
            "",
            "Error of the forecast each information set gives for quarter-hour k at its own decision time "
            f"({timing.gate_closure_minutes} minutes before delivery). RMSE uses errors clipped at ± 300 EUR/MWh so a few "
            "extreme quarter-hours do not dominate.",
            "",
            "| Period | Forecast | MAE EUR/MWh | RMSE (clipped) EUR/MWh |",
            "| --- | --- | ---: | ---: |",
        ]
        names_fc = {
            "da": "day-ahead price",
            "profile": "+ intraday profile (published index)",
            "profile_live": "+ live residual (rolling)",
        }
        for _, row in skill.iterrows():
            period = by_key[row["period"]]
            lines.append(
                f"| {period.label} | {names_fc[row['forecast']]} | {row['mae_eur_mwh']:.1f} | {row['rmse_clipped_eur_mwh']:.1f} |"
            )
        lines.append("")
    if "intraday_spread_profile.png" in names:
        lines += ["![Spread profile](figures/intraday_spread_profile.png)", ""]
    # Audit
    lines += [
        "## Causality audit",
        "",
        "For each sampled day the intraday prices of all quarter-hours from a random cut onwards, and every price of "
        "later days, are replaced with noise and the day is re-run. A decision for quarter-hour k may only change if a "
        "perturbed price was visible to it.",
        "",
        "| Strategy | Runs | Runs with every protected decision unchanged | Largest change of a protected decision (MW) "
        "| Runs where decisions before the cut changed (look-ahead) |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    audit_frame = pd.DataFrame(audit_records)
    for name, rows in audit_frame.groupby("strategy", sort=False):
        tradable = bool(rows["tradable"].iloc[0])
        unchanged = f"{int((rows['max_abs_change_protected_mw'] <= 1e-7).sum())}" if tradable else "n/a (oracle)"
        largest = f"{rows['max_abs_change_protected_mw'].max():.2g}" if tradable else "n/a"
        lines.append(
            f"| {SHORT_LABELS[str(name)]} | {len(rows)} | {unchanged} | {largest} | "
            f"{int(rows['lookahead_detected'].sum())} |"
        )
    lines += [
        "",
        "Tradable strategies never react to a perturbed price before it is visible. The oracles see realised prices, "
        "so their earlier decisions move when later prices change (the few runs without a change are days where the "
        "perturbation did not alter the optimal plan before the cut).",
        "",
    ]
    # Market context
    if not context.empty:
        lines += [
            "## Market context",
            "",
            "| Period | Mean DA EUR/MWh | Mean ID AEP EUR/MWh | Mean abs(ID AEP - DA) | Std (ID AEP - DA) | Quarter-hours with abs spread > 50 |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
        for _, row in context.iterrows():
            lines.append(
                f"| {by_key[row['period']].label} | {row['mean_da_eur_mwh']:.1f} | {row['mean_id_eur_mwh']:.1f} | "
                f"{row['mean_abs_spread_eur_mwh']:.1f} | {row['std_spread_eur_mwh']:.1f} | {_pct(row['share_abs_spread_gt_50'])} |"
            )
        lines.append("")
    baseline_note: list[str] = []
    if latest is not None:
        base_summary = summarize_daily(daily, config, first=latest.first, last=latest.last).loc["da_only"]
        before_fees = float(base_summary["pnl_eur_per_mw_year"] + base_summary["da_fees_eur_per_mw_year"])
        baseline_note.append(
            f"- Baseline consistency: before the {costs.da_fee_eur_mwh:g} EUR/MWh day-ahead fee, day-ahead only earns "
            f"{_eur(before_fees)} EUR/MW/yr in the latest 12 full months, the same persistence workflow and battery as "
            "the DA-only row of [reports/germany.md](germany.md) (which solves the day with exclusive charge / "
            "discharge binaries; here `d + c <= P`)."
        )
    lines += [
        "## Data and coverage",
        "",
        f"- Day-ahead prices: {DA_SOURCE}. Hourly SDAC products before 2025-10-01 are expanded to four quarter-hours "
        "and traded as one hourly position.",
        *baseline_note,
        "- Intraday index: ID AEP (Index Ausgleichsenergiepreis) of the four German TSOs, downloaded from the public "
        "CSV download of netztransparenz.de (no account) and built locally under `data/raw/intraday/` with "
        "`python -m euroflex_bess_lab.public_data.intraday fetch`. It is **not redistributed**: the netztransparenz.de "
        "imprint requires the TSOs' prior written consent for reproducing site content, so this repository commits only "
        "the fetch code, a sidecar stub (`data/public/intraday/id_aep_DE-LU.meta.json`, `redistributed: false`) and "
        "results derived from the data (daily simulated P&L and aggregate statistics).",
        f"- Evaluated delivery days: {coverage['n_days']} ({coverage['first']} to {coverage['last']}); every day has "
        f"complete day-ahead prices and {coverage['undefined_qh']} quarter-hours have an undefined index "
        f"(trades there settle at the day-ahead price; {coverage['fallback_trades']} such trades).",
        f"- Physical checks on every committed schedule (SOC window, power limit, position balance, terminal SOC): "
        f"{coverage['soc_violations']} violations. LP fallbacks (warm and cold start both non-optimal; product "
        f"committed as planned one step earlier): {coverage['lp_fallbacks']}. Runtime of this script run: "
        f"{runtime_minutes:.1f} minutes.",
        "- Ledgers: `reports/ledgers/intraday_daily.parquet` (daily P&L per strategy, valuation-ready), "
        "`intraday_sensitivity.parquet`, `intraday_audit.json` (causality audit, forecast skill, run settings).",
        "",
        "## Scope",
        "",
        "- Simulated revenue for one price-taking 1 MW battery; no real orders. Execution at the ID AEP is a proxy for "
        "continuous trading (no order book, queue position or partial fills); the slippage parameter stands for the "
        "difference to achievable prices, and the fees are assumptions.",
        "- Day-ahead bids are quantity schedules from a persistence forecast; intraday auctions (IDA), reserve markets "
        "and imbalance positions are not part of this study (reserves: [reports/germany.md](germany.md)).",
        "",
    ]
    text = "\n".join(lines)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(text, encoding="utf-8")
    return text


# ---------------------------------------------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------------------------------------------


def run_all(
    market: IntradayMarket,
    *,
    start: date,
    end: date,
    config: IntradayConfig,
    workers: int,
    report_path: Path,
    figures_dir: Path,
    ledgers_dir: Path,
    audit_days: int = 30,
    sensitivities: bool = True,
    figures: bool = True,
) -> dict[str, Any]:
    started = time.perf_counter()
    days = [d for d in market.days if start <= d <= end]
    periods = define_periods(start, end)
    daily = run_parallel(market, config, days, workers=workers, history_days=HISTORY_DAYS)
    sens_days = [d for d in days if d >= next((p.first for p in periods if p.key == "previous_12m"), start)]
    sens_periods = [p for p in periods if p.key in {"quarter_hour_era", "previous_12m", "latest_12m"}]
    sensitivity = (
        run_sensitivities(market, config, sens_days, daily, sens_periods, workers) if sensitivities else pd.DataFrame()
    )
    skill_periods = [p for p in periods if p.key in {"previous_12m", "quarter_hour_era"}]
    skill = forecast_skill(market, config, skill_periods)
    audit_records = causality_audit(market, config, days, n_days=audit_days)
    context = market_context(market, [p for p in periods if p.key != "full"])
    profiles: dict[str, np.ndarray] = {}
    for key, period_key in (("hourly", "previous_12m"), ("quarter_hour", "quarter_hour_era")):
        period = next((p for p in periods if p.key == period_key), None)
        if period is not None and period.days(market.days):
            profiles[key] = spread_profile(market, period)
    figure_paths = make_figures(daily, sensitivity, profiles, periods, figures_dir) if figures else []
    lo, _ = market.day_bounds(days[0])
    _, hi = market.day_bounds(days[-1])
    coverage = {
        "n_days": len(days),
        "first": f"{days[0]}",
        "last": f"{days[-1]}",
        "undefined_qh": int(np.sum(~np.isfinite(market.id_price[lo:hi]))),
        "fallback_trades": int(daily.loc[daily["strategy"].isin(TRADABLE), "fallback_trades"].sum()),
        "lp_fallbacks": int(daily["lp_fallbacks"].sum()),
        "soc_violations": int(daily["soc_violations"].sum()),
    }
    runtime = (time.perf_counter() - started) / 60.0
    audit = {
        "generated_by": "scripts/research/intraday_backtest.py",
        "settings": config.describe(),
        "periods": {p.key: [f"{p.first}", f"{p.last}"] for p in periods},
        "coverage": coverage,
        "causality_audit": audit_records,
        "forecast_skill": skill.to_dict(orient="records"),
        "market_context": context.to_dict(orient="records"),
        "summaries": {
            p.key: summarize_daily(daily, config, first=p.first, last=p.last).reset_index().to_dict(orient="records")
            for p in periods
        },
        "runtime_minutes": round(runtime, 2),
    }
    ledger_paths = write_ledgers(ledgers_dir, daily, sensitivity, audit, config, periods)
    text = render_report(
        daily=daily,
        sensitivity=sensitivity,
        skill=skill,
        audit_records=audit_records,
        context=context,
        periods=periods,
        config=config,
        figures=figure_paths,
        coverage=coverage,
        runtime_minutes=runtime,
        report_path=report_path,
    )
    return {
        "daily": daily,
        "sensitivity": sensitivity,
        "skill": skill,
        "audit": audit_records,
        "figures": figure_paths,
        "ledgers": ledger_paths,
        "report": text,
        "periods": periods,
    }


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=_parse_date, default=DEFAULT_START, help="first evaluated delivery day")
    parser.add_argument("--end", type=_parse_date, default=None, help="last evaluated day (default: latest common day)")
    parser.add_argument("--fetch", action="store_true", help="download / refresh the local ID AEP table first")
    parser.add_argument("--workers", type=int, default=max(1, min(10, (os.cpu_count() or 2) - 1)))
    parser.add_argument("--audit-days", type=int, default=30)
    parser.add_argument("--no-sensitivities", action="store_true")
    parser.add_argument("--report", type=Path, default=REPO_ROOT / "reports" / "intraday.md")
    parser.add_argument("--figures-dir", type=Path, default=REPO_ROOT / "reports" / "figures")
    parser.add_argument("--ledgers-dir", type=Path, default=REPO_ROOT / "reports" / "ledgers")
    args = parser.parse_args(argv)
    if args.fetch:
        from euroflex_bess_lab.public_data.intraday import build_id_aep

        build = build_id_aep(start="2024-01-01")
        print(f"ID AEP table: {build.local.rows} quarter-hours, last complete day {build.last_complete_day}")
    end = args.end or latest_common_day()
    market = load_market(args.start, end)
    config = IntradayConfig()
    result = run_all(
        market,
        start=args.start,
        end=end,
        config=config,
        workers=args.workers,
        report_path=args.report,
        figures_dir=args.figures_dir,
        ledgers_dir=args.ledgers_dir,
        audit_days=args.audit_days,
        sensitivities=not args.no_sensitivities,
    )
    for period in result["periods"]:
        summary = summarize_daily(result["daily"], config, first=period.first, last=period.last)
        print(period.label)
        columns = ["days", "pnl_eur_per_mw_year", "uplift_vs_da_only_eur_per_mw_year", "cycles_per_day"]
        print(summary[columns].round(1).to_string())
    print(f"report: {args.report.name}; figures: {len(result['figures'])}; ledgers: {len(result['ledgers'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
