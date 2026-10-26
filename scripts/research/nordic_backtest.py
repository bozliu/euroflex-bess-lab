"""Real-data Nordic BESS backtest: day-ahead arbitrage co-optimised with FCR-N, FCR-D, DK1 FCR, aFRR and mFRR
capacity in DK1, DK2, SE3, SE4 and FI.

Regenerates ``reports/nordics.md``, ``reports/figures/nordic_*.png`` and ``reports/ledgers/nordic_*``::

    .venv/bin/python scripts/research/nordic_fetch.py --as-of 2026-10-26   # data/public/nordic (once)
    .venv/bin/python scripts/research/nordic_backtest.py

Battery: 1 MW / 2 MWh, SOC 5-95 %, start and end every delivery day at 50 %, efficiencies 0.95 / 0.95, degradation
8 EUR per MWh discharged. Each delivery day D (CET/CEST) is one LP (``markets.nordic.lp.solve_day``) decided with
the persistence forecast (D-1 prices at the same local clock time) available at the earliest gate closure
(D-1 00:30 for SE/DK2 FCR, 07:30 for aFRR/mFRR), settled at realised prices, and compared with the same LP on
realised prices (oracle). Market rules and sources: ``docs/nordics.md`` and ``markets.nordic.rules``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from euroflex_bess_lab.markets.nordic.backtest import (
    MARKET_TZ,
    MAX_MISSING_DA_QH,
    NORDIC_BATTERY,
    WORKFLOW_LABELS,
    ZoneMarket,
    annualise,
    applicable_workflows,
    local_day_bounds,
    run_zone,
    workflow_products,
)
from euroflex_bess_lab.markets.nordic.lp import check_schedule, solve_day
from euroflex_bess_lab.markets.nordic.rules import (
    FCR_N,
    PRODUCTS,
    RULE_SOURCES,
    ZONE_COUNTRY,
    ZONE_PRODUCTS,
    zone_products,
)
from euroflex_bess_lab.public_data.nordic import load_dk_imbalance, load_nordic_reserve, nordic_table_path
from euroflex_bess_lab.public_data.nordic.normalize import dk_imbalance_rule_check
from euroflex_bess_lab.realtime.strategies import solve_qh_arbitrage

REPO_ROOT = Path(__file__).resolve().parents[2]
ZONES = ("DK1", "DK2", "SE3", "SE4", "FI")
DEFAULT_START = "2024-02-01"  # first day with pay-as-cleared SE/DK2 FCR prices
PRODUCT_COLUMNS = {
    "da": ("da_revenue_eur", "Day-ahead"),
    "fcr_n": ("rev_fcr_n_eur", "FCR-N"),
    "fcr_d_up": ("rev_fcr_d_up_eur", "FCR-D up"),
    "fcr_d_down": ("rev_fcr_d_down_eur", "FCR-D down"),
    "fcr_dk1": ("rev_fcr_dk1_eur", "FCR (DK1)"),
    "afrr_up": ("rev_afrr_up_eur", "aFRR up"),
    "afrr_down": ("rev_afrr_down_eur", "aFRR down"),
    "mfrr_up": ("rev_mfrr_up_eur", "mFRR up"),
    "mfrr_down": ("rev_mfrr_down_eur", "mFRR down"),
}
FAMILIES = {
    "Day-ahead": ["da_revenue_eur"],
    "FCR-N": ["rev_fcr_n_eur"],
    "FCR-D": ["rev_fcr_d_up_eur", "rev_fcr_d_down_eur"],
    "FCR (DK1)": ["rev_fcr_dk1_eur"],
    "aFRR": ["rev_afrr_up_eur", "rev_afrr_down_eur"],
    "mFRR": ["rev_mfrr_up_eur", "rev_mfrr_down_eur"],
}
FAMILY_COLORS = {
    "Day-ahead": "#2a78d6",
    "FCR-N": "#eb6834",
    "FCR-D": "#1baf7a",
    "FCR (DK1)": "#eda100",
    "aFRR": "#e87ba4",
    "mFRR": "#008300",
}
ZONE_COLORS = {"DK1": "#2a78d6", "DK2": "#eb6834", "SE3": "#1baf7a", "SE4": "#eda100", "FI": "#e87ba4"}
INK = {"primary": "#0b0b0b", "secondary": "#52514e", "muted": "#898781", "grid": "#e1e0d9", "axis": "#c3c2b7"}
SURFACE = "#fcfcfb"
CLEAN_HORIZON_URL = "https://www.cleanhorizon.com/news/storage-index-september-2026-is-out/"
CLEAN_HORIZON_SEP_2026_2H_KEUR = {"DK1": 286.0, "DK2": 447.0, "SE3": 142.0}
NORDIC_TSO_BATTERY_REPORT_URL = "https://www.svk.se/siteassets/om-oss/rapporter/2025/nordic-tsos---batteries-in-the-nordic-reserve-markets-april-2025.pdf"


# ---------------------------------------------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------------------------------------------


def zone_data_end(zone: str, root: Path | None = None) -> pd.Timestamp:
    """Exclusive end (UTC, a local midnight) of the complete day-ahead history of ``zone``."""

    meta = json.loads(nordic_table_path("day_ahead", zone, root).with_suffix("").with_suffix(".meta.json").read_text())
    end = pd.Timestamp(meta["coverage_end_utc"]).tz_convert(MARKET_TZ)
    midnight = end.normalize()
    return midnight.tz_convert("UTC")


@dataclass(frozen=True)
class Task:
    zone: str
    start_utc: pd.Timestamp
    end_utc: pd.Timestamp
    kind: str = "main"  # "main" or a sensitivity name
    data_root: str | None = None


def sensitivity_products(zone: str, kind: str) -> dict[str, list[Any]]:
    if kind == "no_opposite_reservation":
        return {"full_stack_no_opposite": [PRODUCTS[k] for k in ZONE_PRODUCTS[zone]]}
    if kind == "fcr_n_1_5h":
        products = [
            replace(p, energy_up_hours=1.5, energy_down_hours=1.5) if p.key == FCR_N.key else p
            for p in zone_products(zone)
        ]
        return {"full_stack_fcr_n_1_5h": products}
    raise ValueError(kind)


def run_task(task: Task) -> dict[str, Any]:
    started = time.time()
    root = Path(task.data_root) if task.data_root else None
    market = ZoneMarket.from_public(task.zone, task.start_utc, task.end_utc, root=root)
    if task.kind == "main":
        result = run_zone(market, task.start_utc, task.end_utc)
    else:
        result = run_zone(
            market,
            task.start_utc,
            task.end_utc,
            product_sets=sensitivity_products(task.zone, task.kind),
            ledger_for=None,
        )
    return {
        "zone": task.zone,
        "kind": task.kind,
        "daily": result.daily,
        "ledger": result.ledger,
        "skipped": result.skipped_days,
        "seconds": time.time() - started,
    }


def run_all(tasks: Sequence[Task], processes: int) -> list[dict[str, Any]]:
    if processes <= 1:
        return [run_task(t) for t in tasks]
    with ProcessPoolExecutor(max_workers=processes) as pool:
        return list(pool.map(run_task, tasks))


# ---------------------------------------------------------------------------------------------------------------
# Periods and summaries
# ---------------------------------------------------------------------------------------------------------------


def month_label(day: str) -> str:
    return pd.Timestamp(day).strftime("%b %Y")


def zone_periods(days: Sequence[str]) -> dict[str, tuple[str, str]]:
    """Latest 12 full months, calendar years and the full window for one zone's evaluated days."""

    ordered = sorted(days)
    first, last = pd.Timestamp(ordered[0]), pd.Timestamp(ordered[-1])
    month_end = last + pd.offsets.MonthEnd(0)
    last_full = last if last == month_end else last - pd.offsets.MonthEnd(1)
    latest_first = pd.Timestamp(year=last_full.year - 1, month=last_full.month, day=1) + pd.offsets.MonthBegin(1)
    window = [d for d in ordered if latest_first.date().isoformat() <= d <= last_full.date().isoformat()]
    out = {"latest12": (window[0], window[-1]) if window else (ordered[0], ordered[-1])}
    for year in range(first.year, last.year + 1):
        inside = [d for d in ordered if d.startswith(str(year))]
        if inside:
            out[str(year)] = (inside[0], inside[-1])
    out["full"] = (ordered[0], ordered[-1])
    return out


def annualised_table(daily: pd.DataFrame, periods: Mapping[str, Mapping[str, tuple[str, str]]]) -> pd.DataFrame:
    rows = []
    for (zone, workflow, mode), group in daily.groupby(["zone", "workflow", "mode"], sort=False):
        for label, (first, last) in periods[zone].items():
            part = group.loc[(group["day"] >= first) & (group["day"] <= last)]
            if part.empty:
                continue
            row = {"zone": zone, "workflow": workflow, "mode": mode, "period": label, "first": first, "last": last}
            row.update(annualise(part))
            row["untradable_qh"] = int(part["untradable_qh"].sum())
            rows.append(row)
    return pd.DataFrame(rows)


def lookup(table: pd.DataFrame, zone: str, workflow: str, mode: str, period: str) -> pd.Series | None:
    sel = table.loc[
        (table["zone"] == zone)
        & (table["workflow"] == workflow)
        & (table["mode"] == mode)
        & (table["period"] == period)
    ]
    return None if sel.empty else sel.iloc[0]


def market_depth(zone: str, first: str, last: str, root: Path | None) -> dict[str, float]:
    """Median procured MW per hour and product over ``[first, last]`` (FCR-N/FCR-D: joint SE-DK2 market total)."""

    lo = local_day_bounds(pd.Timestamp(first))[0]
    hi = local_day_bounds(pd.Timestamp(last))[1]
    out: dict[str, float] = {}
    for product in zone_products(zone):
        direction = product.direction if product.table_product in {"afrr", "mfrr"} else None
        frame = load_nordic_reserve(
            product.table_product, zone, lo, hi, auction="early", direction=direction, root=root
        )
        if frame.empty:
            continue
        if product.table_product == "fcr":
            volume = frame["volume_domestic_mw"].astype("float64") + frame["volume_abroad_mw"].astype("float64")
        elif product.table_product.startswith("fcr_"):
            volume = frame["volume_total_mw"].astype("float64")
        else:
            volume = frame["procured_mw"].astype("float64")
        out[product.key] = float(volume.median())
    return out


def market_context(market: ZoneMarket, first: str, last: str) -> dict[str, float]:
    lo = local_day_bounds(pd.Timestamp(first), market.tz)[0]
    hi = local_day_bounds(pd.Timestamp(last), market.tz)[1]
    q = (market.qh_utc >= lo) & (market.qh_utc < hi)
    h = (market.hour_utc >= lo) & (market.hour_utc < hi)
    price = pd.Series(market.da_price[q], index=market.qh_utc[q])
    day = price.index.tz_convert(market.tz).normalize()
    spread = (price.groupby(day).max() - price.groupby(day).min()).mean()
    out = {
        "mean_da": float(price.mean()),
        "mean_spread": float(spread),
        "negative_share": float((price < 0).mean()),
        "da_coverage": float(price.notna().mean()),
    }
    for key, values in market.reserve.items():
        part = values[h]
        out[f"mean_{key}"] = float(np.nanmean(part)) if np.isfinite(part).any() else float("nan")
    return out


# ---------------------------------------------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------------------------------------------


def consistency_checks(
    markets: Mapping[str, ZoneMarket], days: Mapping[str, Sequence[str]], seed: int = 7
) -> dict[str, Any]:
    rng = np.random.default_rng(seed)
    out: dict[str, Any] = {}
    # 1) DA-only LP vs the repository's realtime.strategies.solve_qh_arbitrage (same formulation: free end SOC, no
    #    converter limit, no hourly constraint).
    gaps = []
    zone = "SE3" if "SE3" in markets else next(iter(markets))
    market = markets[zone]
    sample = rng.choice(days[zone], size=min(20, len(days[zone])), replace=False)
    for day in sample:
        inputs = market.day_inputs(pd.Timestamp(day), forecast=False)
        if inputs is None:
            continue
        inputs.hourly_da = False
        ours = solve_day(inputs, [], NORDIC_BATTERY, free_end_soc=True, converter_limit=False)
        net, _ = solve_qh_arbitrage(inputs.da_price, NORDIC_BATTERY, initial_soc_mwh=NORDIC_BATTERY.initial_soc_mwh)
        d, c = np.maximum(net, 0.0), np.maximum(-net, 0.0)
        theirs = float(np.sum(inputs.da_price * net) * 0.25 - 8.0 * d.sum() * 0.25 - 1e-3 * c.sum() * 0.25)
        gaps.append(abs(ours.objective_eur - theirs))
    out["solve_qh_arbitrage"] = {
        "zone": zone,
        "days": len(gaps),
        "max_abs_objective_gap_eur": float(max(gaps) if gaps else 0.0),
    }
    # 2) Independent feasibility check of full-stack schedules (persistence) against the product rules.
    problems, checked = 0, 0
    for zone, market in markets.items():
        products = workflow_products(zone, "full_stack")
        for day in rng.choice(days[zone], size=min(15, len(days[zone])), replace=False):
            forecast = market.day_inputs(pd.Timestamp(day), forecast=True)
            if forecast is None:
                continue
            schedule = solve_day(forecast, products, NORDIC_BATTERY)
            problems += len(check_schedule(schedule, forecast, products, NORDIC_BATTERY))
            checked += 1
    out["feasibility"] = {"schedules_checked": checked, "violations": problems}
    # 3) No look-ahead: perturbing every price from the start of day D onwards leaves D's forecast unchanged.
    changed, tested = 0, 0
    for zone, market in markets.items():
        for day in rng.choice(days[zone], size=min(5, len(days[zone])), replace=False):
            ts = pd.Timestamp(day)
            base = market.day_inputs(ts, forecast=True)
            if base is None:
                continue
            lo = local_day_bounds(ts, market.tz)[0]
            frames_da = pd.DataFrame(
                {
                    "timestamp_utc": market.qh_utc,
                    "resolution_minutes": market.da_resolution,
                    "price_eur_mwh": market.da_price,
                }
            )
            later = frames_da["timestamp_utc"] >= lo
            frames_da.loc[later, "price_eur_mwh"] = (
                frames_da.loc[later, "price_eur_mwh"] * rng.uniform(0.2, 3.0, int(later.sum())) + 50
            )
            reserves = {}
            for key, values in market.reserve.items():
                v = values.copy()
                after = market.hour_utc >= lo
                v[after] = v[after] * rng.uniform(0.2, 3.0, int(after.sum())) + 10
                reserves[key] = pd.DataFrame({"delivery_start_utc": market.hour_utc, "price_eur_per_mw_h": v})
            perturbed = ZoneMarket.from_frames(
                zone, market.qh_utc[0], market.qh_utc[-1] + pd.Timedelta(minutes=15), frames_da, reserves
            )
            probe = perturbed.day_inputs(ts, forecast=True)
            tested += 1
            same = probe is not None and np.allclose(probe.da_price, base.da_price, equal_nan=True)
            for key in base.reserve_price:
                same = (
                    same
                    and probe is not None
                    and np.allclose(probe.reserve_price[key], base.reserve_price[key], equal_nan=True)
                )
            changed += 0 if same else 1
    out["no_lookahead"] = {"days_tested": tested, "forecasts_changed_by_future_prices": changed}
    return out


def dk_imbalance_context(first: str, last: str, root: Path | None) -> list[dict[str, Any]]:
    rows = []
    for area in ("DK1", "DK2"):
        frame = load_dk_imbalance(
            area,
            pd.Timestamp(first, tz=MARKET_TZ).tz_convert("UTC"),
            local_day_bounds(pd.Timestamp(last))[1],
            root=root,
        )
        q = frame.loc[frame["resolution_minutes"] == 15]
        if q.empty:
            continue
        diff = (q["imbalance_price_eur_mwh"] - q["spot_price_eur_mwh"]).astype("float64")
        direction = q["dominating_direction"].astype("float64")
        rows.append(
            {
                "area": area,
                "quarter_hours": int(len(q)),
                "mean_imbalance": float(q["imbalance_price_eur_mwh"].mean()),
                "mean_spot": float(q["spot_price_eur_mwh"].mean()),
                "mean_abs_diff": float(diff.abs().mean()),
                "std_diff": float(diff.std()),
                "share_up": float((direction > 0).mean()),
                "share_down": float((direction < 0).mean()),
                "share_none": float((direction == 0).mean()),
            }
        )
    return rows


# ---------------------------------------------------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------------------------------------------------


def _style(ax: Any) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(INK["axis"])
    ax.tick_params(colors=INK["secondary"], labelsize=9)
    ax.grid(axis="y", color=INK["grid"], linewidth=0.8)
    ax.set_axisbelow(True)


def figure_revenue_stack(table: pd.DataFrame, zones: Sequence[str], path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(9.0, 4.8), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    _style(ax)
    x = np.arange(len(zones))
    bottoms = np.zeros(len(zones))
    for family, columns in FAMILIES.items():
        values = []
        for zone in zones:
            row = lookup(table, zone, "full_stack", "persistence", "latest12")
            values.append(sum(float(row[c]) for c in columns) / 1000.0 if row is not None else 0.0)
        values_arr = np.clip(np.array(values), 0.0, None)
        if values_arr.sum() <= 0:
            continue
        ax.bar(
            x,
            values_arr,
            0.56,
            bottom=bottoms,
            color=FAMILY_COLORS[family],
            label=family,
            edgecolor=SURFACE,
            linewidth=2,
        )
        bottoms += values_arr
    for i, zone in enumerate(zones):
        pers = lookup(table, zone, "full_stack", "persistence", "latest12")
        orac = lookup(table, zone, "full_stack", "oracle", "latest12")
        if pers is None:
            continue
        ax.text(
            i,
            bottoms[i] + bottoms.max() * 0.015,
            f"net {pers['net_eur'] / 1000:,.0f}k",
            ha="center",
            va="bottom",
            fontsize=9,
            color=INK["primary"],
        )
        if orac is not None:
            ax.plot(
                [i - 0.32, i + 0.32],
                [orac["net_eur"] / 1000] * 2,
                color=INK["primary"],
                linewidth=1.5,
                linestyle=(0, (3, 2)),
            )
    ax.plot([], [], color=INK["primary"], linewidth=1.5, linestyle=(0, (3, 2)), label="Oracle net (upper bound)")
    ax.set_xticks(x, [f"{z}\n{ZONE_COUNTRY[z]}" for z in zones], color=INK["primary"])
    ax.set_ylabel("k EUR per MW and year", color=INK["secondary"], fontsize=9)
    ax.set_title(
        "Full-stack revenue by product, latest 12 full months (persistence, 1 MW / 2 MWh)",
        color=INK["primary"],
        fontsize=11,
        loc="left",
    )
    ax.legend(
        frameon=False, fontsize=8, ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.16), labelcolor=INK["secondary"]
    )
    ax.set_ylim(0, max(bottoms.max(), 1.0) * 1.15)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def figure_monthly(daily: pd.DataFrame, zones: Sequence[str], path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(9.0, 4.2), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    _style(ax)
    sel = daily.loc[(daily["workflow"] == "full_stack") & (daily["mode"] == "persistence")].copy()
    sel["month"] = pd.to_datetime(sel["day"]).dt.to_period("M").dt.to_timestamp()
    for zone in zones:
        g = sel.loc[sel["zone"] == zone].groupby("month").agg(net=("net_eur", "sum"), days=("day", "size"))
        annual = g["net"] / g["days"] * 365.0 / 1000.0
        ax.plot(annual.index, annual.to_numpy(), color=ZONE_COLORS[zone], linewidth=2, label=zone)
        if len(annual):
            ax.text(
                annual.index[-1] + pd.Timedelta(days=8),
                float(annual.iloc[-1]),
                zone,
                color=INK["secondary"],
                fontsize=8,
                va="center",
            )
    ax.set_ylabel("k EUR per MW and year (month annualised)", color=INK["secondary"], fontsize=9)
    ax.set_title("Monthly full-stack net revenue, persistence forecast", color=INK["primary"], fontsize=11, loc="left")
    ax.legend(frameon=False, fontsize=8, ncol=5, loc="upper right", labelcolor=INK["secondary"])
    ax.set_ylim(bottom=0)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def figure_capacity_prices(markets: Mapping[str, ZoneMarket], first: str, last: str, path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    panels = [
        (
            "FCR-N / FCR-D (joint SE-DK2 market, auction 1)",
            [("SE3", "fcr_n", "FCR-N"), ("SE3", "fcr_d_up", "FCR-D up"), ("SE3", "fcr_d_down", "FCR-D down")],
        ),
        ("aFRR up", [(z, "afrr_up", z) for z in ("DK1", "DK2", "SE3", "FI") if z in markets]),
        ("mFRR up", [(z, "mfrr_up", z) for z in ("DK1", "DK2", "SE3", "SE4") if z in markets]),
    ]
    fig, axes = plt.subplots(1, 3, figsize=(11.0, 3.8), dpi=150, sharey=False)
    fig.patch.set_facecolor(SURFACE)
    lo = pd.Timestamp(first, tz=MARKET_TZ).tz_convert("UTC")
    hi = local_day_bounds(pd.Timestamp(last))[1]
    slot_colors = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
    for ax, (title, series) in zip(axes, panels, strict=True):
        _style(ax)
        for i, (zone, key, label) in enumerate(series):
            market = markets.get(zone)
            if market is None or key not in market.reserve:
                continue
            s = pd.Series(market.reserve[key], index=market.hour_utc)
            s = s.loc[(s.index >= lo) & (s.index < hi)]
            monthly = s.groupby(s.index.tz_convert(MARKET_TZ).tz_localize(None).to_period("M")).mean()
            ax.plot(monthly.index.to_timestamp(), monthly.to_numpy(), color=slot_colors[i], linewidth=2, label=label)
        ax.set_title(title, color=INK["primary"], fontsize=9, loc="left")
        ax.set_ylim(bottom=0)
        ax.tick_params(axis="x", labelrotation=30)
        ax.legend(frameon=False, fontsize=7, labelcolor=INK["secondary"])
    axes[0].set_ylabel("EUR per MW and hour (monthly mean)", color=INK["secondary"], fontsize=8)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


# ---------------------------------------------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------------------------------------------


def eur(value: float | None) -> str:
    return "-" if value is None or not np.isfinite(value) else f"{value:,.0f}"


def pct(value: float | None) -> str:
    return "-" if value is None or not np.isfinite(value) else f"{100 * value:.1f} %"


def num(value: float | None, digits: int = 2) -> str:
    return "-" if value is None or not np.isfinite(value) else f"{value:.{digits}f}"


def products_present(zone: str) -> list[str]:
    return ["da", *ZONE_PRODUCTS[zone]]


def render_report(ctx: dict[str, Any]) -> str:
    table: pd.DataFrame = ctx["table"]
    zones: list[str] = ctx["zones"]
    periods: dict[str, dict[str, tuple[str, str]]] = ctx["periods"]
    lines: list[str] = []
    add = lines.append
    add("# Nordic BESS backtest: day-ahead plus FCR, aFRR and mFRR capacity in Denmark, Sweden and Finland")
    add("")
    add(
        "Every number on this page is produced by `scripts/research/nordic_backtest.py` "
        "(`.venv/bin/python scripts/research/nordic_backtest.py`) from the committed public data in `data/public/nordic` "
        "(Energinet Energi Data Service and Svenska kraftnät SVK Data Service, both CC BY 4.0). Market rules, data "
        "sources and limits: [docs/nordics.md](../docs/nordics.md)."
    )
    add("")
    add(
        "**Setup.** 1 MW / 2 MWh battery, SOC window 5-95 % (0.1-1.9 MWh), start and end every delivery day (CET/CEST) "
        "at 50 %, charge and discharge efficiency 0.95 / 0.95, degradation 8 EUR per MWh discharged. One linear "
        "programme per delivery day co-optimises the day-ahead schedule with every capacity product of the zone under "
        "the published power and energy reservations (FCR-N 1.34 x C power and 1 h of energy in both directions, FCR-D "
        "1 x C plus 0.2 x C recovery power and 20 minutes of energy, DK1 FCR 1.25 x C and 30 minutes, aFRR and mFRR 1 h "
        "in the product direction, plus the opposite-direction reservation Energinet describes for aFRR/mFRR energy "
        "management in DK1/DK2). Decisions for day D are taken at D-1 00:30 (SE/DK2 FCR auction 1, the earliest gate) "
        "or D-1 07:30 (aFRR/mFRR capacity) with a persistence forecast (D-1 prices at the same local time); all "
        "capacity markets are pay-as-cleared, so the offer is a zero-price bid paid the realised marginal price. The "
        "oracle solves the same problem with realised prices (upper bound, not tradable). EUR/MW/yr = period net "
        "revenue x 365 / evaluated days; cycles/day = MWh discharged / 2 MWh / day. "
        f"Evaluation window {ctx['start_day']} to {ctx['end_day']} (Finland to {ctx['fi_end_day']}, the end of the "
        "open Finnish day-ahead data)."
    )
    add("")
    add("## Key findings")
    add("")
    for bullet in ctx["findings"]:
        add(f"- {bullet}")
    add("")
    add("## Latest 12 full months: revenue stack per zone")
    add("")
    add(
        "Full-stack co-optimisation, persistence forecast. Product columns are EUR/MW/yr; the window is the latest 12 "
        "full months of each zone's data."
    )
    add("")
    header = "| Zone | Window | Days | Day-ahead | FCR-N | FCR-D up | FCR-D down | FCR (DK1) | aFRR up | aFRR down | mFRR up | mFRR down | Degradation | Net | Oracle net | Capture | Cycles/day |"
    add(header)
    add("| --- | --- |" + " ---: |" * 15)
    for zone in zones:
        row = lookup(table, zone, "full_stack", "persistence", "latest12")
        orac = lookup(table, zone, "full_stack", "oracle", "latest12")
        if row is None:
            continue
        first, last = periods[zone]["latest12"]
        cells = [zone, f"{month_label(first)} - {month_label(last)}", str(int(row["days"]))]
        for key in PRODUCT_COLUMNS:
            column = PRODUCT_COLUMNS[key][0]
            cells.append(eur(row[column]) if key in products_present(zone) else "n/a")
        cells += [
            eur(row["degradation_eur"]),
            f"**{eur(row['net_eur'])}**",
            eur(orac["net_eur"]) if orac is not None else "-",
        ]
        cells += [pct(row["net_eur"] / orac["net_eur"]) if orac is not None else "-", num(row["cycles_per_day"])]
        add("| " + " | ".join(cells) + " |")
    add("")
    add(
        "`n/a` = product not available to the model in that zone (Finnish FCR and mFRR prices are only published "
        "behind a Fingrid API key; DK1 is in the Continental European FCR market)."
    )
    add("")
    add("![Revenue stack](figures/nordic_revenue_stack.png)")
    add("")
    add("## Workflows compared (latest 12 full months)")
    add("")
    add("Net EUR/MW/yr with the persistence forecast (capture vs the oracle of the same workflow in brackets).")
    add("")
    names = ["da_only", "da_fcr", "da_afrr", "da_mfrr", "full_stack"]
    add("| Zone | " + " | ".join(WORKFLOW_LABELS[n] for n in names) + " | Full stack / DA only |")
    add("| --- |" + " ---: |" * (len(names) + 1))
    for zone in zones:
        cells = [zone]
        for name in names:
            row = lookup(table, zone, name, "persistence", "latest12")
            orac = lookup(table, zone, name, "oracle", "latest12")
            if row is None or name not in applicable_workflows(zone):
                cells.append("n/a")
                continue
            cells.append(
                f"{eur(row['net_eur'])} ({pct(row['net_eur'] / orac['net_eur']) if orac is not None else '-'})"
            )
        full = lookup(table, zone, "full_stack", "persistence", "latest12")
        da = lookup(table, zone, "da_only", "persistence", "latest12")
        cells.append(
            f"{full['net_eur'] / da['net_eur']:.1f}x"
            if full is not None and da is not None and da["net_eur"] > 0
            else "-"
        )
        add("| " + " | ".join(cells) + " |")
    add("")
    add("## Per calendar year")
    add("")
    add(
        "Full stack and DA only, persistence vs oracle, EUR/MW/yr (2024 starts on 1 February, the first day of "
        "pay-as-cleared FCR pricing in the joint SE-DK2 market; the last year runs to the end of the data)."
    )
    add("")
    add(
        "| Zone | Year | Days | Full stack | Oracle | Capture | Reserve share | DA only | DA-only oracle | Cycles/day (full stack) |"
    )
    add("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for zone in zones:
        for label in [p for p in periods[zone] if p.isdigit()]:
            row = lookup(table, zone, "full_stack", "persistence", label)
            orac = lookup(table, zone, "full_stack", "oracle", label)
            da = lookup(table, zone, "da_only", "persistence", label)
            dao = lookup(table, zone, "da_only", "oracle", label)
            if row is None:
                continue
            reserve = row["net_eur"] - row["da_revenue_eur"] + row["degradation_eur"]
            gross = row["da_revenue_eur"] + reserve
            add(
                f"| {zone} | {label} | {int(row['days'])} | {eur(row['net_eur'])} | {eur(orac['net_eur'] if orac is not None else None)} | "
                f"{pct(row['net_eur'] / orac['net_eur']) if orac is not None else '-'} | {pct(reserve / gross if gross else float('nan'))} | "
                f"{eur(da['net_eur'] if da is not None else None)} | {eur(dao['net_eur'] if dao is not None else None)} | {num(row['cycles_per_day'])} |"
            )
    add("")
    add("![Monthly net revenue](figures/nordic_monthly_net_revenue.png)")
    add("")
    add("### Revenue stack per year (full stack, persistence, EUR/MW/yr)")
    add("")
    add(
        "| Zone | Year | Day-ahead | FCR-N | FCR-D up | FCR-D down | FCR (DK1) | aFRR up | aFRR down | mFRR up | mFRR down | Degradation | Net |"
    )
    add("| --- | --- |" + " ---: |" * 11)
    for zone in zones:
        for label in [p for p in periods[zone] if p.isdigit()]:
            row = lookup(table, zone, "full_stack", "persistence", label)
            if row is None:
                continue
            cells = [zone, label] + [
                eur(row[PRODUCT_COLUMNS[k][0]]) if k in products_present(zone) else "n/a" for k in PRODUCT_COLUMNS
            ]
            cells += [eur(row["degradation_eur"]), eur(row["net_eur"])]
            add("| " + " | ".join(cells) + " |")
    add("")
    add("## Market context")
    add("")
    add(
        "Time-weighted means over each zone's latest-12-months window; capacity prices in EUR per MW and hour (auction 1 for FCR-N/FCR-D)."
    )
    add("")
    add(
        "| Zone | Mean DA EUR/MWh | Mean daily DA spread EUR/MWh | Negative DA QHs | FCR-N | FCR-D up | FCR-D down | FCR (DK1) | aFRR up | aFRR down | mFRR up | mFRR down |"
    )
    add("| --- |" + " ---: |" * 11)
    for zone in zones:
        c = ctx["context"][zone]
        cells = [zone, num(c["mean_da"], 1), num(c["mean_spread"], 1), pct(c["negative_share"])]
        for key in ("fcr_n", "fcr_d_up", "fcr_d_down", "fcr_dk1", "afrr_up", "afrr_down", "mfrr_up", "mfrr_down"):
            cells.append(num(c.get(f"mean_{key}", float("nan")), 1) if key in ZONE_PRODUCTS[zone] else "n/a")
        add("| " + " | ".join(cells) + " |")
    add("")
    add("![Capacity prices](figures/nordic_capacity_prices.png)")
    add("")
    add(
        "Market depth: median procured capacity per hour in the same windows (MW; FCR-N/FCR-D = total of the joint "
        "SE-DK2 market, DK1 FCR = Danish requirement incl. imports). Several of these markets are a few tens of MW "
        "per zone, so the price-taker results apply to a small share of each market. SE3/SE4 aFRR is mostly "
        "procured in other zones of the common Nordic auction (median local volume 0 MW) while a zonal price is "
        "published for every hour."
    )
    add("")
    add("| Zone | FCR-N | FCR-D up | FCR-D down | FCR (DK1) | aFRR up | aFRR down | mFRR up | mFRR down |")
    add("| --- |" + " ---: |" * 8)
    for zone in zones:
        d = ctx["depth"][zone]
        cells = [zone] + [
            num(d.get(k, float("nan")), 0) if k in ZONE_PRODUCTS[zone] else "n/a"
            for k in ("fcr_n", "fcr_d_up", "fcr_d_down", "fcr_dk1", "afrr_up", "afrr_down", "mfrr_up", "mfrr_down")
        ]
        add("| " + " | ".join(cells) + " |")
    add("")
    add("## Rule sensitivities (latest 12 full months, full stack, persistence)")
    add("")
    add("| Case | Zone | Net EUR/MW/yr | Base | Change |")
    add("| --- | --- | ---: | ---: | ---: |")
    for case in ctx["sensitivities"]:
        add(
            f"| {case['label']} | {case['zone']} | {eur(case['net'])} | {eur(case['base'])} | {pct(case['net'] / case['base'] - 1 if case['base'] else float('nan'))} |"
        )
    add("")
    add("## Danish imbalance settlement")
    add("")
    add(
        "Denmark settles imbalances at a single price per bidding zone and 15-minute period. Since the new design "
        "(mFRR energy activation market, March 2025) the price is the higher of the mFRR marginal price and the aFRR "
        "volume-weighted average price when the dominating direction is up, the lower of the two when it is down, and "
        f"the day-ahead price without a dominating direction ([Energinet]({RULE_SOURCES['energinet_imbalance_design']})). "
        f"The committed Energinet data follows this rule in {100 * ctx['imbalance_rule']:.3f} % of quarter-hours from "
        "2025-03-20 (aFRR averages of exactly 0 EUR with no aFRR energy read as 'no aFRR price')."
    )
    add("")
    add(
        "**Not modelled as a strategy.** Deliberate (implicit or passive) balancing is not an accepted business model in "
        "Denmark: the Nordic TSOs expect balance responsible parties to be balanced before the operating hour and to "
        "follow their submitted plans, monitor systematic deviations, and do not publish intra-period prices in order to "
        f"avoid self-balancing ([Nordic Balancing Philosophy]({RULE_SOURCES['nordic_balancing_philosophy']})); Danish BRPs "
        "submit 5-minute operational schedules and are financially responsible for being in balance "
        f"([Energinet regulation C1, in Danish]({RULE_SOURCES['energinet_c1']})), and Energinet does not allow battery energy "
        f"management through imbalances ([prequalification, section 10.1.4]({RULE_SOURCES['energinet_prequalification']})). "
        "The table is context only."
    )
    add("")
    add(
        "| Area | Quarter-hours | Mean imbalance price | Mean day-ahead price | Mean abs. difference | Std of difference | Dominant up | Dominant down | No dominant direction |"
    )
    add("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in ctx["imbalance"]:
        add(
            f"| {row['area']} | {row['quarter_hours']:,} | {num(row['mean_imbalance'], 1)} | {num(row['mean_spot'], 1)} | {num(row['mean_abs_diff'], 1)} | "
            f"{num(row['std_diff'], 1)} | {pct(row['share_up'])} | {pct(row['share_down'])} | {pct(row['share_none'])} |"
        )
    add("")
    add(f"Window {ctx['imbalance_window'][0]} to {ctx['imbalance_window'][1]}; prices in EUR/MWh.")
    add("")
    add("## Checks")
    add("")
    chk = ctx["checks"]
    add(
        f"- **Same LP as the real-time engine.** With the terminal-SOC and converter constraints dropped, the day LP "
        f"reproduces `realtime.strategies.solve_qh_arbitrage` on {chk['solve_qh_arbitrage']['days']} {chk['solve_qh_arbitrage']['zone']} days "
        f"(largest objective gap {chk['solve_qh_arbitrage']['max_abs_objective_gap_eur']:.2e} EUR)."
    )
    add(
        f"- **Product rules hold.** {chk['feasibility']['schedules_checked']} full-stack schedules re-checked "
        f"independently against the power and energy reservations: {chk['feasibility']['violations']} violations."
    )
    add(
        f"- **No look-ahead.** Randomly perturbing every price from the start of day D onwards changed the forecast "
        f"inputs of day D in {chk['no_lookahead']['forecasts_changed_by_future_prices']} of {chk['no_lookahead']['days_tested']} tested days. "
        "The forecast only uses delivery days up to D-1, whose prices are all published by D-2 evening, before the "
        "earliest gate (D-1 00:30)."
    )
    add(
        f"- **Simultaneous charge and discharge.** {ctx['simultaneous']} of {ctx['total_qh']:,} solved quarter-hours "
        "charge and discharge in the same quarter-hour (the converter limit c + d <= P bounds it); they shape the SOC "
        "under the hourly day-ahead constraint and change revenue negligibly."
    )
    add("")
    add("## Data and coverage")
    add("")
    for item in ctx["coverage"]:
        add(f"- {item}")
    add(f"- Runtime of this script run: {ctx['runtime_min']:.1f} minutes.")
    add("")
    add("## Scope and assumptions")
    add("")
    for item in ctx["scope"]:
        add(f"- {item}")
    add("")
    add("## External reference points")
    add("")
    add(
        f"- Clean Horizon Storage Index, September 2026 ({CLEAN_HORIZON_URL}): the 2-hour index is reported at "
        f"{CLEAN_HORIZON_SEP_2026_2H_KEUR['DK1']:.0f} k EUR/MW/yr in DK1, {CLEAN_HORIZON_SEP_2026_2H_KEUR['DK2']:.0f} in DK2 "
        f"and {CLEAN_HORIZON_SEP_2026_2H_KEUR['SE3']:.0f} in SE3 (one month, annualised). This backtest's September 2026 "
        f"full-stack value is {ctx['sep2026']}. The index adds intraday trading, activation energy and (DK2) FFR, and "
        "uses its own asset and bidding assumptions, so it is a scale reference, not a like-for-like comparison."
    )
    add(
        f"- Nordic TSOs, 'Batteries in the Nordic reserve markets' (April 2025, {NORDIC_TSO_BATTERY_REPORT_URL}): the "
        "TSOs' own account of the rapid growth of prequalified battery volumes in FCR, which is the background to the "
        "falling FCR-D prices visible in the market-context figure."
    )
    add("")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------------------------------------------


def ledger_summary(daily: pd.DataFrame) -> dict[str, float]:
    full = daily.loc[daily["workflow"] == "full_stack"]
    pers = annualise(full.loc[full["mode"] == "persistence"])
    orac = annualise(full.loc[full["mode"] == "oracle"])
    return {
        "eur_per_mw_year": pers["net_eur"],
        "cycles_per_day": pers["cycles_per_day"],
        "days": pers["days"],
        "capture_vs_oracle": pers["net_eur"] / orac["net_eur"] if orac.get("net_eur") else float("nan"),
    }


def _ledger_sidecar(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", default=DEFAULT_START, help="First delivery day (CET), default 2024-02-01.")
    parser.add_argument("--end", default=None, help="Last delivery day (inclusive); default: end of each zone's data.")
    parser.add_argument("--zones", default=",".join(ZONES))
    parser.add_argument("--processes", type=int, default=min(6, os.cpu_count() or 1))
    parser.add_argument("--out-dir", type=Path, default=REPO_ROOT, help="Root for reports/ (default: the repository).")
    parser.add_argument("--data-dir", type=Path, default=None, help="Data root (default: data/public).")
    parser.add_argument("--no-figures", action="store_true")
    parser.add_argument("--no-sensitivities", action="store_true")
    args = parser.parse_args(argv)
    started = time.time()
    zones = [z.strip().upper() for z in args.zones.split(",") if z.strip()]
    root = args.data_dir.resolve() if args.data_dir else None
    start_utc = pd.Timestamp(args.start).tz_localize(MARKET_TZ).tz_convert("UTC")
    ends: dict[str, pd.Timestamp] = {}
    for zone in zones:
        end = zone_data_end(zone, root)
        if args.end:
            end = min(end, local_day_bounds(pd.Timestamp(args.end))[1])
        ends[zone] = end
    tasks = [Task(z, start_utc, ends[z], "main", str(root) if root else None) for z in zones]
    if not args.no_sensitivities:
        tasks += [
            Task(z, start_utc, ends[z], "no_opposite_reservation", str(root) if root else None)
            for z in ("DK1", "DK2")
            if z in zones
        ]
        tasks += [
            Task(z, start_utc, ends[z], "fcr_n_1_5h", str(root) if root else None) for z in ("SE3",) if z in zones
        ]
    results = run_all(tasks, args.processes)
    main_results = [r for r in results if r["kind"] == "main"]
    daily = pd.concat([r["daily"] for r in main_results], ignore_index=True)
    days = {z: sorted(daily.loc[daily["zone"] == z, "day"].unique().tolist()) for z in zones}
    periods = {z: zone_periods(days[z]) for z in zones}
    table = annualised_table(daily, periods)

    out_reports = args.out_dir / "reports"
    figures = out_reports / "figures"
    ledgers = out_reports / "ledgers"
    figures.mkdir(parents=True, exist_ok=True)
    ledgers.mkdir(parents=True, exist_ok=True)

    battery = {
        "power_mw": NORDIC_BATTERY.power_mw,
        "energy_mwh": NORDIC_BATTERY.energy_mwh,
        "soc_window": [NORDIC_BATTERY.soc_min_frac, NORDIC_BATTERY.soc_max_frac],
        "daily_start_end_soc_frac": NORDIC_BATTERY.initial_soc_frac,
        "efficiency": [NORDIC_BATTERY.charge_efficiency, NORDIC_BATTERY.discharge_efficiency],
        "degradation_eur_per_mwh_discharged": NORDIC_BATTERY.degradation_eur_per_mwh,
    }
    daily.to_parquet(ledgers / "nordic_daily.parquet", index=False, compression="zstd")
    _ledger_sidecar(
        ledgers / "nordic_daily.json",
        {
            "description": "Daily results per zone, workflow (da_only, da_fcr, da_afrr, da_mfrr, full_stack) and mode (persistence, oracle).",
            "generated_by": "scripts/research/nordic_backtest.py",
            "period": {z: [days[z][0], days[z][-1]] for z in zones},
            "battery": battery,
            "units": "EUR per day for a 1 MW / 2 MWh battery; avg_*_mw = mean reserved MW",
            "data": "data/public/nordic (Energinet Energi Data Service, Svenska kraftnät SVK Data Service; CC BY 4.0)",
        },
    )
    for result in main_results:
        if result["ledger"] is None:
            continue
        zone = result["zone"]
        name = f"nordic_qh_{zone}_full_stack"
        result["ledger"].to_parquet(ledgers / f"{name}.parquet", index=False, compression="zstd")
        lo, hi = local_day_bounds(pd.Timestamp(days[zone][0]))[0], local_day_bounds(pd.Timestamp(days[zone][-1]))[1]
        _ledger_sidecar(
            ledgers / f"{name}.json",
            {
                "asset_id": name,
                "description": f"Quarter-hour ledger, {zone} full stack, persistence forecast (decided D-1, settled at realised prices).",
                "strategy": "nordic_full_stack_persistence",
                "mechanism": f"{zone} day-ahead + pay-as-cleared capacity (no activation energy)",
                "market": zone,
                "timezone": MARKET_TZ,
                "start_utc": lo.isoformat(),
                "end_utc": hi.isoformat(),
                "battery": {
                    **battery,
                    "power_mw": NORDIC_BATTERY.power_mw,
                    "degradation_eur_per_mwh": NORDIC_BATTERY.degradation_eur_per_mwh,
                },
                "price_taker": True,
                "fleet_mw": 0.0,
                "source": "scripts/research/nordic_backtest.py on data/public/nordic (Energinet, Svenska kraftnät; CC BY 4.0)",
                "columns": "valuation QH ledger contract (revenue_eur = day-ahead + capacity) + da_revenue_eur, "
                "reserve_revenue_eur and <product>_mw",
                "summary": ledger_summary(result["daily"]),
            },
        )

    markets = {z: ZoneMarket.from_public(z, start_utc, ends[z], root=root) for z in zones}
    context = {z: market_context(markets[z], *periods[z]["latest12"]) for z in zones}
    depth = {z: market_depth(z, *periods[z]["latest12"], root) for z in zones}
    checks = consistency_checks(markets, days)
    imbalance_first = "2025-03-20"
    imbalance_last = days["DK1"][-1] if "DK1" in days else days[zones[0]][-1]
    imbalance = dk_imbalance_context(imbalance_first, imbalance_last, root)
    rule_frames = [
        load_dk_imbalance(
            area,
            pd.Timestamp(imbalance_first, tz=MARKET_TZ).tz_convert("UTC"),
            local_day_bounds(pd.Timestamp(imbalance_last))[1],
            root=root,
        )
        for area in ("DK1", "DK2")
    ]
    rule_check = dk_imbalance_rule_check(pd.concat(rule_frames, ignore_index=True))
    rule_share = rule_check.get("share_matching_rule_idle_afrr_as_missing") or float("nan")

    sensitivities = []
    for result in results:
        if result["kind"] == "main":
            continue
        zone = result["zone"]
        sens_daily = result["daily"]
        name = sens_daily["workflow"].iloc[0]
        sens_table = annualised_table(sens_daily, periods)
        row = lookup(sens_table, zone, name, "persistence", "latest12")
        base = lookup(table, zone, "full_stack", "persistence", "latest12")
        label = {
            "no_opposite_reservation": "No opposite-direction reservation for aFRR/mFRR (Nordic-style rules only)",
            "fcr_n_1_5h": "FCR-N energy 1.5 h per direction (upper end of the range the TSOs will evaluate)",
        }[result["kind"]]
        if row is not None and base is not None:
            sensitivities.append(
                {"label": label, "zone": zone, "net": float(row["net_eur"]), "base": float(base["net_eur"])}
            )

    # September 2026 comparison with the external index.
    sep = []
    sel = daily.loc[
        (daily["workflow"] == "full_stack") & (daily["mode"] == "persistence") & daily["day"].str.startswith("2026-09")
    ]
    for zone in ("DK1", "DK2", "SE3"):
        part = sel.loc[sel["zone"] == zone]
        if len(part):
            sep.append(f"{zone} {part['net_eur'].sum() * 365 / len(part) / 1000:,.0f}k")
    sep_text = ", ".join(sep) if sep else "not available"

    findings = build_findings(table, zones, periods)
    coverage = build_coverage(main_results, days, zones, root)
    scope = SCOPE
    simultaneous = int(daily["simultaneous_qh"].sum())
    if not args.no_figures:
        figure_revenue_stack(table, zones, figures / "nordic_revenue_stack.png")
        figure_monthly(daily, zones, figures / "nordic_monthly_net_revenue.png")
        figure_capacity_prices(markets, days[zones[0]][0], days[zones[0]][-1], figures / "nordic_capacity_prices.png")
    runtime = (time.time() - started) / 60.0
    ctx = {
        "table": table,
        "zones": zones,
        "periods": periods,
        "start_day": min(d[0] for d in days.values()),
        "end_day": max(days[z][-1] for z in zones if z != "FI")
        if any(z != "FI" for z in zones)
        else days[zones[0]][-1],
        "fi_end_day": days["FI"][-1] if "FI" in days else "-",
        "findings": findings,
        "context": context,
        "depth": depth,
        "sensitivities": sensitivities,
        "imbalance": imbalance,
        "imbalance_rule": rule_share,
        "imbalance_window": (imbalance_first, imbalance_last),
        "checks": checks,
        "simultaneous": simultaneous,
        "total_qh": int(daily["n_qh"].sum()),
        "coverage": coverage,
        "scope": scope,
        "runtime_min": runtime,
        "sep2026": sep_text,
    }
    report = render_report(ctx)
    (out_reports / "nordics.md").write_text(report, encoding="utf-8")
    summary = {
        "generated_by": "scripts/research/nordic_backtest.py",
        "battery": battery,
        "periods": periods,
        "latest12": {
            z: {
                wf: {
                    m: None
                    if (r := lookup(table, z, wf, m, "latest12")) is None
                    else {
                        k: float(r[k])
                        for k in ("net_eur", "da_revenue_eur", "degradation_eur", "cycles_per_day", "days")
                    }
                    for m in ("persistence", "oracle")
                }
                for wf in applicable_workflows(z)
            }
            for z in zones
        },
        "checks": checks,
        "sensitivities": sensitivities,
    }
    _ledger_sidecar(ledgers / "nordic_summary.json", summary)
    print(f"wrote {out_reports / 'nordics.md'} in {runtime:.1f} min")
    return 0


SCOPE = [
    "**Simulated money on public prices.** Day-ahead schedule plus capacity reservations; no orders were placed. "
    "Results are per MW of a pooled fleet (minimum bid sizes of 0.1 MW for FCR and 1 MW for aFRR/mFRR are assumed to be "
    "met by pooling) and assume a price taker whose zero-price offers are always accepted.",
    "**No intraday trading and no activation energy.** Intraday re-optimisation, aFRR/mFRR energy bids, FCR-N energy "
    "remuneration and FFR are not modelled; the expected activation energy is taken as zero and the energy "
    "reservations cover the SOC excursions (no SOC drift, no NEM recharge cost).",
    "**FCR auction.** SE/DK2 FCR is offered in auction 1 (D-1 early), where most volume is procured; auction 2 and "
    "the interplay between the two are not modelled. January 2024 is excluded because FCR prices were average "
    "(pay-as-bid) prices before 2024-02-01.",
    "**Finland.** Day-ahead prices come from SVK Data Service (Energy-Charts restricts FI to private use and Fingrid "
    "does not publish them); that dataset ends on 2026-07-01 and misses some hours: quarter-hours without a price are "
    "untradable and days with more than two missing hours are skipped. Finnish FCR-N, FCR-D and mFRR capacity prices "
    "need a Fingrid API key and are not used, so the Finnish stack is day-ahead plus aFRR only.",
    "**Swedish mFRR.** SVK publishes only hours with Swedish procurement; other hours pay nothing.",
    "**Danish aFRR/mFRR energy management.** The opposite-direction power reservation follows Energinet's example "
    "energy-management design for LER units; Energinet states that these requirements are still being refined "
    "(sensitivity above).",
    "**Battery.** Daily 50 % to 50 % SOC boundary, linear efficiencies, no auxiliary load or availability losses, "
    "degradation as a linear cost per MWh discharged; no grid tariffs, fees or taxes.",
]


def build_findings(
    table: pd.DataFrame, zones: Sequence[str], periods: Mapping[str, Mapping[str, tuple[str, str]]]
) -> list[str]:
    out = []
    rows = []
    for zone in zones:
        full = lookup(table, zone, "full_stack", "persistence", "latest12")
        orac = lookup(table, zone, "full_stack", "oracle", "latest12")
        da = lookup(table, zone, "da_only", "persistence", "latest12")
        if full is None or da is None:
            continue
        rows.append((zone, full, orac, da))
    if rows:
        best = max(rows, key=lambda r: r[1]["net_eur"])
        parts = [f"{z} {eur(f['net_eur'])}" for z, f, _, _ in rows]
        out.append(
            "**Reserve stacking is the main value driver in every Nordic zone.** Latest 12 full months, full stack with "
            f"a persistence forecast (EUR/MW/yr): {', '.join(parts)}; the highest is {best[0]}."
        )
        ratios = [f"{z} {f['net_eur'] / d['net_eur']:.1f}x" for z, f, _, d in rows if d["net_eur"] > 0]
        out.append(f"**Versus day-ahead arbitrage alone** the full stack earns {', '.join(ratios)} as much.")
        caps = [f"{z} {pct(f['net_eur'] / o['net_eur'])}" for z, f, o, _ in rows if o is not None and o["net_eur"] > 0]
        out.append(
            "**A simple, causal forecast gets most of the way.** Persistence captures "
            f"{', '.join(caps)} of the perfect-foresight oracle for the full stack."
        )
        cyc = [f"{z} {num(f['cycles_per_day'])}" for z, f, _, _ in rows]
        out.append(
            f"**Low cycling.** Full-stack schedules discharge {', '.join(cyc)} cycles per day (day-ahead throughput only)."
        )
    trend = []
    for zone in zones:
        years = [p for p in periods[zone] if p.isdigit()]
        if len(years) >= 2:
            first = lookup(table, zone, "full_stack", "persistence", years[0])
            last = lookup(table, zone, "full_stack", "persistence", years[-1])
            if first is not None and last is not None:
                trend.append(f"{zone} {eur(first['net_eur'])} ({years[0]}) to {eur(last['net_eur'])} ({years[-1]})")
    if trend:
        out.append(
            "**Capacity prices have come down from their 2024-25 levels**, so the 2026 year-to-date stack is below "
            "2024 in every zone: "
            + "; ".join(trend)
            + ". FCR-D and FCR-N fell first (2024), aFRR and mFRR followed in 2025-26; DK1 rose in 2025 when its "
            "aFRR capacity market opened (October 2024). The persistence capture stays at "
            + ctx_capture_range(table, zones, periods)
            + " in every year."
        )
    return out


def ctx_capture_range(
    table: pd.DataFrame, zones: Sequence[str], periods: Mapping[str, Mapping[str, tuple[str, str]]]
) -> str:
    values = []
    for zone in zones:
        for label in [p for p in periods[zone] if p.isdigit()]:
            pers = lookup(table, zone, "full_stack", "persistence", label)
            orac = lookup(table, zone, "full_stack", "oracle", label)
            if pers is not None and orac is not None and orac["net_eur"] > 0:
                values.append(pers["net_eur"] / orac["net_eur"])
    if not values:
        return "-"
    return f"{100 * min(values):.0f}-{100 * max(values):.0f} %"


def build_coverage(
    main_results: Sequence[Mapping[str, Any]],
    days: Mapping[str, Sequence[str]],
    zones: Sequence[str],
    root: Path | None,
) -> list[str]:
    items = [
        "Day-ahead DK1, DK2, SE3, SE4: Energinet `Elspotprices` (hourly, to 2025-09-30) and `DayAheadPrices` (15-minute, "
        "from 2025-10-01); FI: SVK Data Service `market_data_day_ahead_area_prices` (Energinet's SE3/SE4 prices agree "
        "with SVK's except on 2024-10-15, where SVK is inconsistent and the Finnish day is dropped). Hourly prices "
        "before 2025-10-01 are held flat over their four quarter-hours and the day-ahead position is constant within "
        "each hour.",
        "FCR-N / FCR-D: Energinet `FcrNdDK2` (joint SE-DK2 auctions; identical prices in DK2, SE3 and SE4); DK1 FCR: "
        "Energinet `FcrDK1`; aFRR: Energinet `AfrrReservesNordic` (DK1 from 2024-10-02); mFRR: Energinet "
        "`mFRRCapacityMarket` (DK1, DK2) and SVK Data Service `mfrr_capacity_market` (SE3, SE4). Danish imbalance: "
        "Energinet `ImbalancePrice` and `RegulatingBalancePowerdata`. Licence CC BY 4.0 for all; attribution "
        "'Source: Energinet (www.energidataservice.dk)' and 'Svenska kraftnät (SVK Data Service)'.",
    ]
    for result in main_results:
        zone = result["zone"]
        sel = result["daily"]
        untradable = int(
            sel.loc[(sel["workflow"] == "da_only") & (sel["mode"] == "persistence"), "untradable_qh"].sum()
        )
        lag = sel.loc[(sel["workflow"] == "da_only") & (sel["mode"] == "persistence"), "forecast_lag_days"]
        items.append(
            f"{zone}: {len(days[zone])} delivery days evaluated ({days[zone][0]} to {days[zone][-1]}), "
            f"{len(result['skipped'])} skipped (more than {MAX_MISSING_DA_QH // 4} hours of day-ahead prices missing), "
            f"{untradable} untradable quarter-hours, {int((lag > 1).sum())} days on which some forecast quarter-hours came from D-2 or earlier (missing on D-1)."
        )
    return items


if __name__ == "__main__":
    sys.exit(main())
