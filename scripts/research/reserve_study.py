"""Belgian reserve study: aFRR capacity + aFRR energy + implicit balancing on one 1 MW / 2 MWh battery.

Regenerates ``reports/reserve_belgium.md``, ``reports/figures/reserve_*.png`` and
``reports/ledgers/reserve_*.{parquet,json}`` from ``data/public`` (Elia Open Data, CC BY 4.0, attribution
"Elia Transmission Belgium SA")::

    .venv/bin/python scripts/research/reserve_study.py                 # full study, writes the report
    .venv/bin/python scripts/research/reserve_study.py --quick --out-dir /tmp/x   # two-month smoke run

Every number in the report is computed here; nothing is typed by hand. The implicit-balancing layer is the core
``ThresholdStrategy`` unless ``--implicit-factory module:callable`` names another zero-argument strategy factory
(the callable receives the mechanism name).
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from euroflex_bess_lab import public_data  # noqa: E402
from euroflex_bess_lab.realtime import BatteryParams, MarketData, PublicationModel, ThresholdStrategy  # noqa: E402
from euroflex_bess_lab.realtime.reserve import (  # noqa: E402
    MARI_GO_LIVE_UTC,
    PICASSO_GO_LIVE_UTC,
    ActivationModel,
    AfrrProduct,
    AwardModel,
    CapacityAwards,
    DeliveryBlocks,
    ForecastSplitPolicy,
    PolicySuite,
    ReservePlan,
    ReserveSettings,
    ReserveStudyConfig,
    SplitCandidate,
    activation_diagnostics,
    build_activation,
    check_allocation_causality,
    check_reserve_no_lookahead,
    run_policy_suite,
    save_ledger,
    stack_table,
)
from euroflex_bess_lab.realtime.strategies.base import Strategy  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
TZ = "Europe/Brussels"
POLICIES = ("fixed_0", "fixed_50", "fixed_100", "forecast", "oracle")
POLICY_LABELS = {
    "fixed_0": "0 % reserved (implicit only)",
    "fixed_50": "50 % reserved",
    "fixed_100": "100 % reserved",
    "forecast": "forecast split (causal)",
    "oracle": "ex-post oracle split",
}
LEDGER_POLICIES = ("fixed_0", "fixed_100", "forecast")
SIGNAL_FIELD = {"current": "price_cum_eur_mwh", "new2026": "nf_imbalance_price_eur_mwh"}
MINUTE_COLUMNS = [
    "price_cum_eur_mwh",
    "si_cum_mw",
    "afrr_up_mw",
    "afrr_down_mw",
    "afrr_price_up_eur_mwh",
    "afrr_price_down_eur_mwh",
]
# chart tokens (dataviz reference palette, light mode)
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#e4e3df"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]


# --------------------------------------------------------------------------------------------------------------
# helpers


def local_midnight_utc(day: str) -> pd.Timestamp:
    return pd.Timestamp(day, tz=TZ).tz_convert("UTC")


def utc(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def data_end() -> pd.Timestamp:
    """Local midnight after the last complete local day of the minute table."""
    probe_start = pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=60)
    frame = public_data.load_belgium_minute(probe_start, pd.Timestamp.now(tz="UTC"), columns=["si_mw"])
    last = pd.Timestamp(frame["timestamp_utc"].max()) + pd.Timedelta(minutes=1)
    local = last.tz_convert(TZ)
    end_local = local.normalize() if local != local.normalize() else local
    return end_local.tz_convert("UTC")


def threshold_factory(mechanism: str) -> Callable[[], Strategy]:
    signal = SIGNAL_FIELD[mechanism]

    def make() -> Strategy:
        return ThresholdStrategy(signal_field=signal)

    return make


def resolve_factory(spec: str | None, mechanism: str) -> Callable[[], Strategy]:
    if not spec:
        return threshold_factory(mechanism)
    module_name, _, attr = spec.partition(":")
    factory = getattr(importlib.import_module(module_name), attr)

    def make() -> Strategy:
        return factory(mechanism)

    return make


def load_market(start: pd.Timestamp, end: pd.Timestamp, *, new_formula: bool) -> MarketData:
    include_da = True
    try:
        public_data.load_day_ahead_prices("BE", start, start + pd.Timedelta(days=1))
    except FileNotFoundError:
        include_da = False
    return MarketData.from_public(
        start,
        end,
        include_new_formula=new_formula,
        include_si_forecast=False,
        include_day_ahead=include_da,
        minute_columns=MINUTE_COLUMNS,
    )


def awards_for(market: MarketData, history: CapacityAwards) -> CapacityAwards:
    return history.aligned_to(DeliveryBlocks.regular(market.start_utc, market.end_utc, history.blocks.product))


def k(value: float) -> str:
    rounded = round(value / 1000.0, 1)
    return f"{(rounded if rounded != 0 else 0.0):,.1f}"


def md_table(frame: pd.DataFrame, formats: Mapping[str, Callable[[Any], str]] | None = None) -> str:
    formats = dict(formats or {})
    header = "| " + " | ".join(str(c) for c in frame.columns) + " |"
    rule = "| " + " | ".join("---:" if pd.api.types.is_numeric_dtype(frame[c]) else "---" for c in frame.columns) + " |"
    lines = [header, rule]
    for _, row in frame.iterrows():
        cells = []
        for column in frame.columns:
            value = row[column]
            fmt = formats.get(column)
            if fmt is not None:
                cells.append(fmt(value))
            elif isinstance(value, float):
                cells.append(f"{value:,.2f}")
            else:
                cells.append(str(value))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def style_axes(ax: plt.Axes) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
        ax.spines[side].set_linewidth(1.0)
    ax.grid(axis="y", color=GRID, linewidth=0.8, linestyle="-")
    ax.set_axisbelow(True)
    ax.tick_params(colors=INK_2, labelsize=9, length=0)
    ax.yaxis.label.set_color(INK_2)
    ax.xaxis.label.set_color(INK_2)
    ax.title.set_color(INK)


def new_figure(width: float, height: float, nrows: int = 1, ncols: int = 1, **kwargs: Any) -> tuple[Any, Any]:
    fig, axes = plt.subplots(nrows, ncols, figsize=(width, height), facecolor=SURFACE, **kwargs)
    for ax in np.atleast_1d(axes).ravel():
        style_axes(ax)
    return fig, axes


# --------------------------------------------------------------------------------------------------------------
# analyses


def monthly_capacity_prices(history: CapacityAwards, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    frame = history.to_frame()
    frame = frame[(frame["delivery_start_utc"] >= start) & (frame["delivery_start_utc"] < end)].copy()
    frame["month"] = pd.to_datetime(frame["delivery_start_utc"], utc=True).dt.tz_convert(TZ).dt.strftime("%Y-%m")
    grouped = frame.groupby("month", sort=True)
    return pd.DataFrame(
        {
            "vwap_up": grouped["vwap_up_eur_per_mw_h"].mean(),
            "vwap_down": grouped["vwap_down_eur_per_mw_h"].mean(),
            "marginal_up": grouped["marginal_up_eur_per_mw_h"].mean(),
            "marginal_down": grouped["marginal_down_eur_per_mw_h"].mean(),
            "awarded_up_mw": grouped["awarded_up_mw"].mean(),
            "awarded_down_mw": grouped["awarded_down_mw"].mean(),
        }
    ).reset_index()


def monthly_volumes(market: MarketData) -> pd.DataFrame:
    """Belgian aFRR activated in the LFC area (ODS126 minutes) vs Elia's satisfied aFRR demand (ODS127)."""
    minute_up = np.abs(market.require_minute("afrr_up_mw")).reshape(-1, 15).mean(axis=1)
    minute_down = np.abs(market.require_minute("afrr_down_mw")).reshape(-1, 15).mean(axis=1)
    frame = pd.DataFrame(
        {
            "timestamp_utc": market.qh_times,
            "ods126_up": minute_up,
            "ods126_down": minute_down,
            "ods127_up": np.abs(market.require_qh("afrr_up_mw")),
            "ods127_down": np.abs(market.require_qh("afrr_down_mw")),
        }
    )
    frame["month"] = frame["timestamp_utc"].dt.tz_convert(TZ).dt.strftime("%Y-%m")
    return (
        frame.groupby("month", sort=True)[["ods126_up", "ods126_down", "ods127_up", "ods127_down"]].mean().reset_index()
    )


def activation_table(
    market: MarketData, awards: CapacityAwards, periods: Mapping[str, tuple[Any, Any, str]]
) -> pd.DataFrame:
    variants = {
        "pro-rata, QH price (main)": ActivationModel(),
        "pro-rata, minute price": ActivationModel(price_source="minute"),
        "merit-order (bid = DA +/- 10)": ActivationModel(kind="merit_order"),
        "pro-rata, published price pre-PICASSO": ActivationModel(pay_as_bid_until_utc=None),
    }
    rows: list[dict[str, Any]] = []
    for label, model in variants.items():
        series = build_activation(market, awards, model)
        for period, (start, end, _) in periods.items():
            m0 = int((start - market.start_utc) / pd.Timedelta(minutes=1))
            m1 = int((end - market.start_utc) / pd.Timedelta(minutes=1))
            sub_market = market.slice(start, end)
            sub_series = series.slice_minutes(m0, m1)
            sub_awards = awards.aligned_to(DeliveryBlocks.regular(start, end, awards.blocks.product))
            diag = activation_diagnostics(sub_market, sub_awards, sub_series)
            rows.append(
                {
                    "variant": label,
                    "period": period,
                    "ratio_up": diag["up"]["mean_ratio"],
                    "ratio_down": diag["down"]["mean_ratio"],
                    "raw_ratio_above_1_up": diag["up"]["raw_ratio_above_one_share"],
                    "price_up_when_active": diag["up"]["activation_weighted_price_eur_mwh"],
                    "price_down_when_active": diag["down"]["activation_weighted_price_eur_mwh"],
                    "energy_eur_per_mw_day_up": diag["up"]["energy_revenue_eur_per_reserved_mw_day"],
                    "energy_eur_per_mw_day_down": diag["down"]["energy_revenue_eur_per_reserved_mw_day"],
                }
            )
    return pd.DataFrame(rows)


def allocation_monthly(suite: PolicySuite, start: pd.Timestamp) -> pd.DataFrame:
    blocks = suite.awards.blocks
    frame = blocks.to_frame()
    frame["month"] = pd.to_datetime(frame["delivery_start_utc"], utc=True).dt.tz_convert(TZ).dt.strftime("%Y-%m")
    for name in ("forecast", "oracle"):
        decision = suite.decisions[name]
        frame[f"{name}_share"] = decision.reserved_share()
    forecast = suite.decisions["forecast"]
    hours = blocks.hours
    power = suite.config.battery.max_power_mw
    cap = np.nan_to_num(suite.capacity_forecast["up"]) + np.nan_to_num(suite.capacity_forecast["down"])
    frame["forecast_capacity_eur_mw_h"] = np.where(
        np.isfinite(suite.capacity_forecast["up"]) & np.isfinite(suite.capacity_forecast["down"]), cap, np.nan
    )
    implicit_value = forecast.values_eur[:, 0] / (power * hours)
    frame["expected_implicit_eur_mw_h"] = implicit_value
    full_index = [c.label for c in forecast.candidates].index("fixed_100")
    energy_value = forecast.values_eur[:, full_index] / (power * hours) - frame["forecast_capacity_eur_mw_h"]
    frame["expected_reserve_energy_eur_mw_h"] = energy_value
    frame["agree"] = forecast.choice == suite.decisions["oracle"].choice
    frame = frame[frame["delivery_start_utc"] >= start]
    grouped = frame.groupby("month", sort=True)
    return pd.DataFrame(
        {
            "forecast_share": grouped["forecast_share"].mean(),
            "oracle_share": grouped["oracle_share"].mean(),
            "agreement": grouped["agree"].mean(),
            "forecast_capacity_eur_mw_h": grouped["forecast_capacity_eur_mw_h"].mean(),
            "expected_implicit_eur_mw_h": grouped["expected_implicit_eur_mw_h"].mean(),
            "expected_reserve_energy_eur_mw_h": grouped["expected_reserve_energy_eur_mw_h"].mean(),
        }
    ).reset_index()


def causality_audits(
    market: MarketData,
    history: CapacityAwards,
    config: ReserveStudyConfig,
    make_inner: Callable[[], Strategy],
    publication: PublicationModel,
    start: pd.Timestamp,
) -> dict[str, Any]:
    """Run the no-look-ahead audits on a 12-day slice of real data (cut at a gate closure on day 9)."""
    sub = market.slice(start, start + pd.Timedelta(days=12))
    sub_awards = awards_for(sub, history)
    gate = sub_awards.blocks.gate_closure_utc
    cut = pd.Timestamp(gate[np.flatnonzero(gate >= start + pd.Timedelta(days=8))[0]])

    def builder(policy: str) -> Callable[[MarketData, CapacityAwards], ReservePlan]:
        def build(m: MarketData, a: CapacityAwards) -> ReservePlan:
            suite = run_policy_suite(
                m, a, make_inner=make_inner, config=config, publication=publication, policies=[policy]
            )
            return suite.results[policy].plan

        return build

    forecast = check_allocation_causality(builder("forecast"), sub, sub_awards, cut_utc=cut, publication=publication)
    oracle = check_allocation_causality(builder("oracle"), sub, sub_awards, cut_utc=cut, publication=publication)
    n = sub_awards.blocks.n
    plan = ReservePlan.from_offers(
        "half", sub_awards, config.battery, offered_up_mw=np.full(n, 0.5), offered_down_mw=np.full(n, 0.5)
    )
    cut_minute = int((cut - sub.start_utc) / pd.Timedelta(minutes=1)) + 7
    implicit = check_reserve_no_lookahead(
        make_inner,
        sub,
        config.battery,
        plan,
        build_activation(sub, sub_awards, config.activation),
        cut_minute=cut_minute,
        publication=publication,
        settings=config.settings,
    )
    return {
        "slice": f"{start.tz_convert(TZ):%Y-%m-%d} + 12 days",
        "cut_utc": str(cut),
        "forecast_passed": forecast.passed,
        "forecast_decided_blocks": forecast.decided_blocks,
        "forecast_diverging_blocks": forecast.diverging_blocks,
        "oracle_passed": oracle.passed,
        "oracle_diverging_blocks": oracle.diverging_blocks,
        "implicit_passed": implicit.passed,
        "implicit_compared_minutes": implicit.compared_minutes,
    }


def run_sensitivities(
    market: MarketData,
    awards: CapacityAwards,
    history: CapacityAwards,
    base: ReserveStudyConfig,
    make_inner: Callable[[], Strategy],
    periods: Mapping[str, tuple[Any, Any, str]],
) -> pd.DataFrame:
    asymmetric = (
        SplitCandidate(0.0, 0.0),
        SplitCandidate(0.5, 0.5),
        SplitCandidate(1.0, 1.0),
        SplitCandidate(1.0, 0.0),
        SplitCandidate(0.0, 1.0),
    )
    variants: dict[str, ReserveStudyConfig] = {
        "capacity paid at marginal (upper bound)": replace(base, award=AwardModel("marginal")),
        "capacity pay-as-bid at forecast VWAP": replace(base, award=AwardModel("pay_as_bid")),
        "sustain 60 min instead of 15": replace(base, product=replace(base.product, sustain_minutes=60.0)),
        "activation merit-order (bid = DA +/- 10)": replace(base, activation=ActivationModel(kind="merit_order")),
        "aFRR energy at reconstructed minute price": replace(base, activation=ActivationModel(price_source="minute")),
        "pre-PICASSO energy at published price (upper bound)": replace(
            base, activation=ActivationModel(pay_as_bid_until_utc=None)
        ),
        "candidates incl. up-only / down-only": replace(
            base, candidates=asymmetric, forecast=replace(base.forecast, candidates=asymmetric)
        ),
        "forecast lookback 14 days": replace(base, forecast=replace(base.forecast, lookback_days=14)),
        "undelivered energy neither paid nor charged": replace(base, settings=ReserveSettings(settle_on="delivered")),
    }
    rows: list[dict[str, Any]] = []
    for label, config in variants.items():
        t0 = time.time()
        suite = run_policy_suite(
            market,
            awards,
            make_inner=make_inner,
            config=config,
            history_awards=history,
            policies=["forecast"],
        )
        table = stack_table(suite.results, {p: (s, e) for p, (s, e, _) in periods.items()})
        for _, row in table.iterrows():
            if row["policy"] not in {"fixed_0", "fixed_50", "fixed_100", "forecast"}:
                continue
            rows.append({"variant": label, **row.to_dict()})
        print(f"  sensitivity '{label}' done in {time.time() - t0:.0f} s", flush=True)
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------------------------------------
# figures


def plot_capacity_prices(monthly: pd.DataFrame, periods: Mapping[str, tuple[Any, Any, str]], path: Path) -> None:
    fig, ax = new_figure(9.0, 4.2)
    x = pd.to_datetime(monthly["month"] + "-15")
    ax.plot(x, monthly["vwap_up"], color=SERIES[0], linewidth=2, solid_capstyle="round", label="up (VWAP)")
    ax.plot(x, monthly["vwap_down"], color=SERIES[1], linewidth=2, solid_capstyle="round", label="down (VWAP)")
    for xs, ys, color in ((x, monthly["vwap_up"], SERIES[0]), (x, monthly["vwap_down"], SERIES[1])):
        ax.scatter(xs.iloc[[-1]], ys.iloc[[-1]], s=40, color=color, edgecolor=SURFACE, linewidth=2, zorder=3)
        ax.annotate(
            f"{ys.iloc[-1]:.1f}",
            (xs.iloc[-1], ys.iloc[-1]),
            xytext=(6, 0),
            textcoords="offset points",
            va="center",
            fontsize=9,
            color=INK_2,
        )
    top = float(np.nanmax(monthly[["vwap_up", "vwap_down"]].to_numpy())) * 1.12
    for label, when in (("PICASSO", PICASSO_GO_LIVE_UTC), ("MARI", MARI_GO_LIVE_UTC)):
        stamp = when.tz_convert(TZ).tz_localize(None)
        ax.axvline(stamp, color=INK_2, linewidth=0.8)
        ax.text(stamp, top, f" {label}", fontsize=8, color=INK_2, va="top")
    ax.set_ylim(0, top)
    ax.set_ylabel("EUR/MW/h")
    ax.set_title(
        "Belgian aFRR capacity price, monthly mean of the 4-hour blocks (Elia ODS125/ODS207)", fontsize=10, loc="left"
    )
    ax.legend(frameon=False, fontsize=9, labelcolor=INK_2, loc="upper right")
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def plot_revenue_stack(table: pd.DataFrame, periods: Mapping[str, tuple[Any, Any, str]], path: Path) -> None:
    components = [
        ("afrr_capacity", "aFRR capacity", SERIES[0]),
        ("afrr_energy", "aFRR energy", SERIES[1]),
        ("implicit_balancing", "implicit balancing", SERIES[2]),
        ("degradation", "degradation", SERIES[3]),
        ("shortfall_imbalance", "delivery shortfall", SERIES[4]),
    ]
    names = list(periods)
    fig, axes = new_figure(4.0 * len(names), 4.6, 1, len(names), sharey=True)
    axes = np.atleast_1d(axes)
    peaks: list[float] = []
    for ax, period in zip(axes, names, strict=True):
        sub = table[table["period"] == period].set_index("policy").reindex(list(POLICIES))
        x = np.arange(len(POLICIES))
        pos = np.zeros(len(POLICIES))
        neg = np.zeros(len(POLICIES))
        for column, label, color in components:
            values = sub[column].to_numpy(dtype=float) / 1000.0
            up = np.clip(values, 0.0, None)
            down = np.clip(values, None, 0.0)
            ax.bar(x, up, bottom=pos, width=0.55, color=color, edgecolor=SURFACE, linewidth=1.5, label=label)
            ax.bar(x, down, bottom=neg, width=0.55, color=color, edgecolor=SURFACE, linewidth=1.5)
            pos += up
            neg += down
        totals = sub["total"].to_numpy(dtype=float) / 1000.0
        ax.scatter(x, totals, s=36, color=INK, edgecolor=SURFACE, linewidth=2, zorder=4, label="net P&L")
        for xi, total, height in zip(x, totals, pos, strict=True):
            ax.text(xi, height + 4, f"{total:.0f}", ha="center", fontsize=8, color=INK_2)
        ax.axhline(0, color=INK_2, linewidth=0.8)
        ax.set_xticks(x, ["0 %", "50 %", "100 %", "forecast", "oracle"], fontsize=8)
        ax.set_title(f"{period}: {periods[period][2].split(',')[0]}", fontsize=9, loc="left")
        peaks.append(float(np.nanmax(pos)))
    axes[0].set_ylabel("kEUR per MW per year")
    lo, _ = axes[0].get_ylim()
    axes[0].set_ylim(lo, max(peaks) * 1.1)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, frameon=False, fontsize=8, labelcolor=INK_2, loc="lower center", ncol=6)
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def plot_allocation(monthly: pd.DataFrame, path: Path) -> None:
    fig, axes = new_figure(9.0, 6.4, 2, 1, sharex=True)
    x = pd.to_datetime(monthly["month"] + "-15")
    ax = axes[0]
    ax.plot(x, monthly["forecast_share"] * 100, color=SERIES[0], linewidth=2, label="forecast split (causal)")
    ax.plot(x, monthly["oracle_share"] * 100, color=SERIES[1], linewidth=2, label="ex-post oracle split")
    ax.set_ylim(0, 105)
    ax.set_ylabel("% of power reserved")
    ax.set_title(
        "Share of the battery sold as aFRR capacity (mean of 4-hour blocks per month)", fontsize=10, loc="left"
    )
    ax.legend(frameon=False, fontsize=9, labelcolor=INK_2, loc="lower left")
    ax = axes[1]
    ax.plot(
        x,
        monthly["forecast_capacity_eur_mw_h"],
        color=SERIES[0],
        linewidth=2,
        label="forecast capacity price, up + down",
    )
    ax.plot(
        x,
        monthly["expected_implicit_eur_mw_h"],
        color=SERIES[2],
        linewidth=2,
        label="expected implicit value of 1 MW (0 % run)",
    )
    ax.plot(
        x,
        monthly["expected_reserve_energy_eur_mw_h"],
        color=SERIES[1],
        linewidth=2,
        label="expected aFRR energy net of wear (100 % run)",
    )
    ax.axhline(0, color=INK_2, linewidth=0.8)
    ax.set_ylabel("EUR per MW per hour")
    ax.set_title("What the policy compares at the D-1 gate closure (monthly means)", fontsize=10, loc="left")
    ax.legend(frameon=False, fontsize=8, labelcolor=INK_2, loc="upper right")
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def plot_mechanisms(table: pd.DataFrame, path: Path) -> None:
    fig, ax = new_figure(9.0, 4.2)
    x = np.arange(len(POLICIES))
    width = 0.3
    for i, (mechanism, color) in enumerate((("current", SERIES[0]), ("new2026", SERIES[1]))):
        sub = table[table["mechanism"] == mechanism].set_index("policy").reindex(list(POLICIES))
        values = sub["total"].to_numpy(dtype=float) / 1000.0
        offset = (i - 0.5) * (width + 0.03)
        ax.bar(x + offset, values, width=width, color=color, label=f"{mechanism} imbalance price")
        for xi, value in zip(x + offset, values, strict=True):
            ax.text(xi, value + 3, f"{value:.0f}", ha="center", fontsize=8, color=INK_2)
    ax.set_xticks(x, ["0 % reserved", "50 % reserved", "100 % reserved", "forecast split", "oracle split"], fontsize=8)
    ax.set_ylabel("net P&L, kEUR per MW per year")
    ax.set_title("Jan-Jul 2026: same days, current vs new (2026) Elia imbalance-price formula", fontsize=10, loc="left")
    ax.legend(frameon=False, fontsize=9, labelcolor=INK_2, loc="upper left")
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def plot_activation_validation(volumes: pd.DataFrame, path: Path) -> None:
    fig, ax = new_figure(9.0, 4.2)
    x = pd.to_datetime(volumes["month"] + "-15")
    ax.plot(x, volumes["ods126_up"], color=SERIES[0], linewidth=2, label="up, activated in Belgium (ODS126)")
    ax.plot(x, volumes["ods127_up"], color=SERIES[1], linewidth=2, label="up, Elia satisfied demand (ODS127)")
    ax.plot(x, volumes["ods126_down"], color=SERIES[2], linewidth=2, label="down, activated in Belgium (ODS126)")
    ax.plot(x, volumes["ods127_down"], color=SERIES[3], linewidth=2, label="down, Elia satisfied demand (ODS127)")
    stamp = PICASSO_GO_LIVE_UTC.tz_convert(TZ).tz_localize(None)
    ax.axvline(stamp, color=INK_2, linewidth=0.8)
    ax.text(stamp, 1.0, " PICASSO", fontsize=8, color=INK_2, va="bottom")
    ax.set_ylim(0, None)
    ax.set_ylabel("mean MW")
    ax.set_title(
        "aFRR activated by Belgian providers vs Elia's satisfied aFRR demand (monthly means)", fontsize=10, loc="left"
    )
    ax.legend(frameon=False, fontsize=8, labelcolor=INK_2, loc="upper center", bbox_to_anchor=(0.5, -0.1), ncol=2)
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


# --------------------------------------------------------------------------------------------------------------
# report


def _total(table: pd.DataFrame, period: str, policy: str, column: str = "total", mechanism: str | None = None) -> float:
    sub = table[(table["period"] == period) & (table["policy"] == policy)]
    if mechanism is not None:
        sub = sub[sub["mechanism"] == mechanism]
    return float(sub[column].iloc[0]) if len(sub) else float("nan")


def headline(
    main_table: pd.DataFrame, window_table: pd.DataFrame, periods: Mapping[str, tuple[Any, Any, str]]
) -> list[str]:
    """Computed headline sentences (kEUR per MW per year)."""
    names = list(periods)
    items: list[str] = []
    implicit = ", ".join(f"{k(_total(main_table, n, 'fixed_0'))} ({n})" for n in names)
    reserve = ", ".join(f"{k(_total(main_table, n, 'fixed_100'))} ({n})" for n in names)
    forecast = ", ".join(f"{k(_total(main_table, n, 'forecast'))} ({n})" for n in names)
    items.append(f"Implicit balancing only (0 % reserved) earned {implicit} kEUR/MW/year net of wear.")
    items.append(f"Selling the whole battery as symmetric aFRR (100 % reserved) earned {reserve}.")
    items.append(f"The causal per-block split (forecast policy) earned {forecast}.")
    cap = " / ".join(k(_total(main_table, n, "fixed_100", "afrr_capacity")) for n in names)
    energy = " / ".join(k(_total(main_table, n, "fixed_100", "afrr_energy")) for n in names)
    short = " / ".join(k(_total(main_table, n, "fixed_100", "shortfall_imbalance")) for n in names)
    share = " / ".join(
        f"{_total(main_table, n, 'fixed_100', 'afrr_energy') / (_total(main_table, n, 'fixed_100', 'afrr_capacity') + _total(main_table, n, 'fixed_100', 'afrr_energy')):.0%}"
        for n in names
    )
    items.append(
        f"100 % reserve stack by period ({' / '.join(names)}): aFRR capacity {cap}, aFRR energy {energy}, "
        f"undelivered energy settled at the imbalance price {short} kEUR/MW/year; aFRR energy was {share} of the "
        "reserve payments (capacity + energy)."
    )
    window = "2026-01_2026-07"
    cur = _total(window_table, window, "fixed_0", mechanism="current")
    new = _total(window_table, window, "fixed_0", mechanism="new2026")
    fc_cur = _total(window_table, window, "forecast", mechanism="current")
    fc_new = _total(window_table, window, "forecast", mechanism="new2026")
    items.append(
        f"Jan-Jul 2026, same days: implicit-only {k(cur)} (current formula) vs {k(new)} (new 2026 formula); "
        f"forecast split {k(fc_cur)} vs {k(fc_new)}."
    )
    return items


def best_fixed_note(table: pd.DataFrame, period: str) -> str:
    sub = table[(table["period"] == period) & table["policy"].str.startswith("fixed_")].set_index("policy")
    if sub.empty:
        return ""
    best = str(sub["total"].idxmax())
    forecast = table[(table["period"] == period) & (table["policy"] == "forecast")]["total"]
    capture = (
        f"; the forecast split earns {float(forecast.iloc[0]) / float(sub.loc[best, 'total']):.0%} of it"
        if len(forecast) and sub.loc[best, "total"] > 0
        else ""
    )
    return (
        f"Best fixed split in hindsight for {period}: {POLICY_LABELS[best]} at {k(float(sub.loc[best, 'total']))} "
        f"kEUR/MW/year{capture}."
    )


def stack_markdown(table: pd.DataFrame, period: str, *, mechanism: str | None = None) -> str:
    sub = table[table["period"] == period]
    if mechanism is not None:
        sub = sub[sub["mechanism"] == mechanism]
    sub = sub.set_index("policy").reindex([p for p in POLICIES if p in set(sub["policy"])]).reset_index()
    frame = pd.DataFrame(
        {
            "policy": [POLICY_LABELS.get(p, p) for p in sub["policy"]],
            "aFRR capacity": sub["afrr_capacity"],
            "aFRR energy": sub["afrr_energy"],
            "implicit balancing": sub["implicit_balancing"],
            "shortfall at imbalance price": sub["shortfall_imbalance"],
            "degradation": sub["degradation"],
            "net P&L": sub["total"],
            "cycles/day": sub["cycles_per_day"],
            "mean reserved MW (up/down)": [
                f"{u:.2f} / {d:.2f}"
                for u, d in zip(sub["mean_up_reserved_mw"], sub["mean_down_reserved_mw"], strict=True)
            ],
        }
    )
    money = {
        c: k
        for c in (
            "aFRR capacity",
            "aFRR energy",
            "implicit balancing",
            "shortfall at imbalance price",
            "degradation",
            "net P&L",
        )
    }
    return md_table(frame, {**money, "cycles/day": lambda v: f"{v:.2f}"})


def write_report(
    path: Path,
    *,
    context: dict[str, Any],
    main_table: pd.DataFrame,
    window_table: pd.DataFrame,
    sensitivity: pd.DataFrame | None,
    capacity_monthly: pd.DataFrame,
    allocation: pd.DataFrame,
    activation: pd.DataFrame,
    volumes: pd.DataFrame,
    figures: Mapping[str, str],
) -> None:
    periods = context["periods"]
    battery = context["battery"]
    lines: list[str] = []
    add = lines.append
    add("# Belgian battery revenue: aFRR capacity, aFRR energy and implicit balancing on one battery")
    add("")
    add(
        f"_Generated by `scripts/research/reserve_study.py` on {context['generated_utc']} from `data/public`: "
        "Elia Open Data (CC BY 4.0, attribution: Elia Transmission Belgium SA) and day-ahead prices from "
        "Bundesnetzagentur | SMARD.de via Energy-Charts (Fraunhofer ISE), CC BY 4.0. Every number below is "
        "recomputed by that script; rerun it to refresh._"
    )
    add("")
    add("## Headline")
    add("")
    for item in headline(main_table, window_table, periods):
        add(f"- {item}")
    add("")
    add("## Setup")
    add("")
    add(
        f"- **Battery**: {battery['power_mw']:.0f} MW / {battery['energy_mwh']:.0f} MWh, one-way efficiencies "
        f"{battery['charge_efficiency']:.2f} / {battery['discharge_efficiency']:.2f}, SOC band "
        f"{battery['soc_min_frac']:.0%}-{battery['soc_max_frac']:.0%}, degradation "
        f"{battery['degradation_eur_per_mwh']:.0f} EUR per MWh discharged (all discharge, including aFRR). "
        "Results are per MW of battery power; a 1 MW battery stands for a share of a pooled portfolio "
        "(Elia's minimum aFRR bid is 1 MW)."
    )
    add(
        f"- **Period**: simulated {context['sim_start_local']} to {context['end_local']} (local time, minute "
        f"resolution, quarter-hour settlement); reported from {context['report_start_local']}. Periods: "
        + "; ".join(
            f"**{name}** = {s.tz_convert(TZ):%Y-%m-%d} to {e.tz_convert(TZ):%Y-%m-%d} ({desc})"
            for name, (s, e, desc) in periods.items()
        )
        + "."
    )
    add(
        "- **Mechanism**: implicit energy settled at the published current-formula imbalance price (ODS134) for the "
        "full period; the Jan-Jul 2026 window is also settled at the new-formula price (ODS212, Elia's simulation "
        "of the design planned for 24 Nov 2026) on the same days."
    )
    add(
        f"- **Publication lag**: minute rows usable {context['publication']['minute_lag']} minute after they end; "
        f"final quarter-hour prices {context['publication']['qh_final_lag_minutes']} minutes after the "
        "quarter-hour. The capacity split of delivery day D is decided at the auction gate closure, 09:00 local on "
        "D-1, with auction results assumed public one hour later."
    )
    add(
        "- **Implicit layer**: `ThresholdStrategy` (core baseline, default parameters; running quarter-hour price "
        "estimate vs adaptive bands from published prices) restricted to the power and SOC headroom the "
        "reservation leaves free."
        if not context.get("implicit_factory")
        else f"- **Implicit layer**: `{context['implicit_factory']}`."
    )
    add("")
    add("## Modelling choices and approximations")
    add("")
    add(
        "- **aFRR capacity** is procured by Elia per direction for six 4-hour blocks per day (D-1 auction, "
        "pay-as-bid). Clearing proxy: the battery is paid the block's awarded-volume-weighted average price "
        "(VWAP of ODS125/ODS207 awards) and every offered MW is assumed awarded. A pay-as-bid award is "
        "uncertain; the sensitivity table shows the marginal price (upper bound) and an explicit pay-as-bid "
        "model (bid = causal VWAP forecast, awarded only if it does not exceed the realised highest accepted bid)."
    )
    add(
        "- **Headroom**: implicit power is limited to `-(P - down) <= u <= P - up`, and implicit trading never "
        "moves the SOC outside `[soc_min + up*T/eta_d, soc_max - down*T*eta_c]` with T = "
        f"{context['product']['sustain_minutes']:.0f} minutes (the repository's aFRR sustain convention, mirroring "
        "the 15-minute limited-energy-reservoir criterion SOGL Art. 156 sets for FCR; Elia's aFRR terms do not "
        "publish a battery-specific duration in the sources checked, so a 60-minute sensitivity is shown). When "
        "activation pushes the SOC out of that band the free implicit power restores it."
    )
    add(
        "- **aFRR activation (approximation)**: the reserved MW are activated pro rata to Belgian aFRR activation, "
        "`ratio = activated / awarded` per minute and direction, clipped to [0, 1]. The numerator is ODS126 "
        "('aFRR volumes activated in the Belgian LFC area', which includes activations for other TSOs via PICASSO), "
        "not ODS127 (Elia's satisfied aFRR demand, partly served by foreign bids); the validation section shows "
        "why the choice matters after PICASSO. Activated energy is paid the published quarter-hour aFRR price "
        "(ODS166; up earns it, down pays it). Before PICASSO (26 Nov 2024) aFRR energy was pay-as-bid: the battery "
        "is assumed to bid at the day-ahead price +/- 10 EUR/MWh and is paid its bid"
        + (
            " (day-ahead prices were not available in `data/public` for this run, so the bid reference is the "
            "trailing 24-hour mean of published imbalance prices)."
            if context.get("da_fallback")
            else "."
        )
    )
    add(
        "- **BRP neutrality**: the BRP perimeter is corrected by the requested aFRR energy, so activated energy "
        "carries no imbalance exposure; only implicit energy (and any undelivered aFRR energy) is settled at the "
        "imbalance price. Activation-control and availability penalties are ignored (0 EUR/MWh); undelivered "
        "energy is reported and charged at the imbalance price."
    )
    add(
        "- **Allocation**: fixed splits reserve 0 %, 50 % or 100 % of the power symmetrically (up and down). The "
        "**forecast split** chooses per block among those three at the D-1 gate closure: value = forecast capacity "
        "price (mean of the same block over the last 7 published auctions) x MW x hours + the mean non-capacity "
        "P&L (aFRR energy, implicit balancing, shortfalls, degradation) that each fixed split realised in the "
        "same block of day over the last 7 settled days. The **oracle split** picks, per block, the fixed split "
        "that realised the most in that block and is then re-simulated; it is an ex-post benchmark, not tradable "
        "and, because of SOC coupling, not a strict upper bound. The forecast policy passes a perturbation test "
        "(`check_allocation_causality`) and the oracle fails it."
    )
    add("")
    add("## Capacity prices: collapse and partial recovery")
    add("")
    add(f"![aFRR capacity prices]({figures['prices']})")
    add("")
    cap = capacity_monthly.copy()
    cap_frame = pd.DataFrame(
        {
            "month": cap["month"],
            "VWAP up": cap["vwap_up"],
            "VWAP down": cap["vwap_down"],
            "marginal up": cap["marginal_up"],
            "marginal down": cap["marginal_down"],
            "awarded up MW": cap["awarded_up_mw"],
            "awarded down MW": cap["awarded_down_mw"],
        }
    )
    add("Monthly means of the 4-hour blocks, EUR/MW/h:")
    add("")
    add(md_table(cap_frame, {c: (lambda v: f"{v:.1f}") for c in cap_frame.columns if c != "month"}))
    add("")
    add("## Revenue stack by period (current imbalance formula)")
    add("")
    add(f"![Revenue stack]({figures['stack']})")
    add("")
    add("kEUR per MW of battery power per year (annualised over the period; costs negative).")
    add("")
    for name, (_, _, desc) in periods.items():
        add(f"### {name}: {desc}")
        add("")
        add(stack_markdown(main_table, name))
        add("")
        add(best_fixed_note(main_table, name))
        add("")
    full = main_table[main_table["period"] == "full"]
    if len(full):
        add(f"### Whole reported period ({context['report_start_local']} to {context['end_local']})")
        add("")
        add(stack_markdown(main_table, "full"))
        add("")
    add("## Allocation over time")
    add("")
    add(f"![Allocation]({figures['allocation']})")
    add("")
    alloc = pd.DataFrame(
        {
            "month": allocation["month"],
            "forecast reserved %": allocation["forecast_share"] * 100,
            "oracle reserved %": allocation["oracle_share"] * 100,
            "same choice as oracle %": allocation["agreement"] * 100,
            "forecast capacity price up+down": allocation["forecast_capacity_eur_mw_h"],
            "expected implicit value 0 %": allocation["expected_implicit_eur_mw_h"],
            "expected aFRR energy 100 %": allocation["expected_reserve_energy_eur_mw_h"],
        }
    )
    add("Shares in % of blocks-weighted power; values in EUR per MW per hour as seen at the gate closure.")
    add("")
    add(md_table(alloc, {c: (lambda v: f"{v:.1f}") for c in alloc.columns if c != "month"}))
    add("")
    add("## Jan-Jul 2026 under both imbalance mechanisms")
    add("")
    add(f"![Mechanism comparison]({figures['mechanisms']})")
    add("")
    add(
        "Same days (2026-01-01 to 2026-08-01 UTC), separate runs per mechanism (the forecast policy learns its "
        "implicit-value estimate under the mechanism it is settled with; first days are a warm-up without "
        "history, so these figures differ slightly from the same months inside the full-period run)."
    )
    add("")
    for mechanism in ("current", "new2026"):
        add(f"### {mechanism}")
        add("")
        add(stack_markdown(window_table, "2026-01_2026-07", mechanism=mechanism))
        add("")
    add("## Validation of the activation approximation")
    add("")
    add(f"![Activation validation]({figures['activation']})")
    add("")
    vol = pd.DataFrame(
        {
            "month": volumes["month"],
            "up ODS126 (activated in BE)": volumes["ods126_up"],
            "up ODS127 (satisfied demand)": volumes["ods127_up"],
            "down ODS126": volumes["ods126_down"],
            "down ODS127": volumes["ods127_down"],
        }
    )
    add(
        "Before PICASSO the two publications agree (Belgian demand was met by Belgian providers). Afterwards Elia's "
        "satisfied demand is several times the volume activated in Belgium, because cheaper foreign bids serve "
        "part of it: using ODS127 would overstate a Belgian provider's activation. Mean MW:"
    )
    add("")
    add(md_table(vol, {c: (lambda v: f"{v:.1f}") for c in vol.columns if c != "month"}))
    add("")
    act = activation.copy()
    act_frame = pd.DataFrame(
        {
            "variant": act["variant"],
            "period": act["period"],
            "mean ratio up": act["ratio_up"],
            "mean ratio down": act["ratio_down"],
            "minutes ratio>1 (up)": act["raw_ratio_above_1_up"] * 100,
            "price when active up": act["price_up_when_active"],
            "price when active down": act["price_down_when_active"],
            "EUR/MW/day up": act["energy_eur_per_mw_day_up"],
            "EUR/MW/day down": act["energy_eur_per_mw_day_down"],
        }
    )
    add(
        "Per reserved MW (before SOC limits): mean activation ratio, share of minutes where Belgian activation "
        "exceeded the awarded volume (clipped), activation-weighted aFRR energy price (EUR/MWh) and energy "
        "revenue (EUR per reserved MW per day; down = minus price x energy)."
    )
    add("")
    add(
        md_table(
            act_frame,
            {
                "mean ratio up": lambda v: f"{v:.3f}",
                "mean ratio down": lambda v: f"{v:.3f}",
                "minutes ratio>1 (up)": lambda v: f"{v:.1f} %",
                "price when active up": lambda v: f"{v:.1f}",
                "price when active down": lambda v: f"{v:.1f}",
                "EUR/MW/day up": lambda v: f"{v:.0f}",
                "EUR/MW/day down": lambda v: f"{v:.0f}",
            },
        )
    )
    add("")
    add(
        "Reading: the minute-price variant reconstructs instantaneous prices from the cumulative ODS165 values and "
        "shows activation is concentrated in the more expensive minutes of a quarter-hour, so the main (QH-price) "
        "variant is conservative. The merit-order variant (fully activated whenever the price clears a bid at the "
        "day-ahead price +/- 10 EUR/MWh) brackets the pro-rata view from above in activated volume. Before PICASSO "
        "the published (marginal) price would hugely overstate pay-as-bid revenue, which is why it is only shown "
        "as an upper bound."
    )
    add("")
    audits = context.get("audits")
    if audits:
        add("### No-look-ahead audits on real data")
        add("")
        add(
            f"On {audits['slice']} with the cut at the gate closure {audits['cut_utc']}: every market value not yet "
            "published at the cut and every auction result published after it is replaced by noise, and the whole "
            "pipeline (reference runs, forecasts, choice) is rerun."
        )
        add("")
        add(
            f"- Forecast split: {'passed' if audits['forecast_passed'] else 'FAILED'} "
            f"({audits['forecast_decided_blocks']} blocks decided by the cut, "
            f"{audits['forecast_diverging_blocks']} changed)."
        )
        add(
            f"- Ex-post oracle split: {'passed (unexpected)' if audits['oracle_passed'] else 'fails, as it must'} "
            f"({audits['oracle_diverging_blocks']} block(s) changed)."
        )
        add(
            f"- Implicit layer with live aFRR activation (50 % reserved): "
            f"{'passed' if audits['implicit_passed'] else 'FAILED'} over the first "
            f"{audits['implicit_compared_minutes']} minutes that cannot depend on the perturbed data."
        )
        add("")
    if sensitivity is not None and len(sensitivity):
        add("## Sensitivities (net P&L, kEUR/MW/year, current formula)")
        add("")
        piv = sensitivity.pivot_table(index=["variant", "policy"], columns="period", values="total", sort=False)
        piv = piv[[c for c in [*periods, "full"] if c in piv.columns]].reset_index()
        piv["policy"] = [POLICY_LABELS.get(p, p) for p in piv["policy"]]
        base_rows = main_table[main_table["policy"].isin(["fixed_0", "fixed_50", "fixed_100", "forecast"])]
        base = base_rows.pivot_table(index="policy", columns="period", values="total")
        base = base[[c for c in [*periods, "full"] if c in base.columns]].reset_index()
        base.insert(0, "variant", "main assumptions")
        base["policy"] = [POLICY_LABELS.get(p, p) for p in base["policy"]]
        merged = pd.concat([base, piv], ignore_index=True)
        add(md_table(merged, {c: k for c in merged.columns if c not in {"variant", "policy"}}))
        add("")
    add("## Limitations")
    add("")
    for item in context["limitations"]:
        add(f"- {item}")
    add("")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


LIMITATIONS = [
    "Provider-level aFRR activations are not public: the pro-rata ratio is an average-provider proxy. Under "
    "PICASSO's merit order a battery's activation depends on its energy bid; the merit-order sensitivity brackets "
    "the effect. Free (non-contracted) aFRR energy bids share the activated volume, which the ratio ignores.",
    "aFRR energy is settled at quarter-hour (ODS166) prices, not 4-second cross-border marginal prices; within-QH "
    "price/activation correlation is only covered by the minute-price sensitivity.",
    "Capacity: the battery is a price-taker at the published VWAP and always awarded (main case). Its own bids do "
    "not move the auction, and the D-1 bid does not include a strategic mark-up.",
    "Pre-PICASSO energy remuneration (pay-as-bid) uses an assumed bid (day-ahead or trailing imbalance price +/- "
    "10 EUR/MWh); the true bids of a battery operator are unknown.",
    "Headroom uses a 15-minute sustain rule and restores the SOC with free implicit power; at 100 % reservation "
    "there is no free power, so SOC drift leads to undelivered energy, settled at the imbalance price. Real "
    "operators would restore the SOC with intraday or day-ahead trades (not modelled), so the 100 % row is a "
    "lower bound for a pure reserve strategy.",
    "No price impact: neither the capacity auction nor the imbalance price reacts to the battery (1 MW).",
    "Activation-control / availability penalties and the aFRR energy-bid gate closure (25 minutes before the "
    "quarter-hour) are not modelled; reserved MW are assumed always offered in the energy market.",
    "The implicit layer is a simple causal threshold rule; a better implicit strategy raises the value of the "
    "unreserved MW and shifts the optimal split towards implicit balancing.",
    "The new-formula window uses Elia's published simulation of the 2026 design (ODS211/ODS212), which assumes "
    "unchanged market behaviour.",
]


# --------------------------------------------------------------------------------------------------------------
# main


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--quick", action="store_true", help="two-month smoke run (no sensitivities)")
    parser.add_argument("--no-sensitivities", action="store_true")
    parser.add_argument("--out-dir", type=Path, default=REPO, help="root for reports/ (default: the repository)")
    parser.add_argument("--implicit-factory", default=None, help="module:callable(mechanism) -> Strategy")
    parser.add_argument(
        "--all-ledgers",
        action="store_true",
        help="also save fixed_50 and oracle ledgers (default: forecast, 0 %%, 100 %%)",
    )
    args = parser.parse_args(argv)

    t_start = time.time()
    end = data_end()
    sim_start = local_midnight_utc("2024-05-22")
    report_start = local_midnight_utc("2024-06-01")
    periods: dict[str, tuple[pd.Timestamp, pd.Timestamp, str]] = {
        "2024H2": (report_start, local_midnight_utc("2025-01-01"), "high capacity prices, PICASSO from 26 Nov"),
        "2025": (local_midnight_utc("2025-01-01"), local_midnight_utc("2026-01-01"), "capacity-price collapse"),
        "2026YTD": (local_midnight_utc("2026-01-01"), end, "partial recovery"),
    }
    window = (utc("2026-01-01T00:00:00Z"), utc("2026-08-01T00:00:00Z"))
    if args.quick:
        sim_start, report_start = local_midnight_utc("2024-11-15"), local_midnight_utc("2024-12-01")
        end = local_midnight_utc("2025-02-01")
        periods = {
            "Dec24": (report_start, local_midnight_utc("2025-01-01"), "quick check"),
            "Jan25": (local_midnight_utc("2025-01-01"), end, "quick check"),
        }
        window = (utc("2026-03-01T00:00:00Z"), utc("2026-04-01T00:00:00Z"))
    out = args.out_dir
    report_path = out / "reports" / "reserve_belgium.md"
    figure_dir = out / "reports" / "figures"
    ledger_dir = out / "reports" / "ledgers"
    for directory in (figure_dir, ledger_dir):
        directory.mkdir(parents=True, exist_ok=True)

    battery = BatteryParams(
        power_mw=1.0, energy_mwh=2.0, charge_efficiency=0.95, discharge_efficiency=0.95, degradation_eur_per_mwh=8.0
    )
    product = AfrrProduct()
    config = ReserveStudyConfig(battery=battery, product=product, forecast=ForecastSplitPolicy())
    publication = PublicationModel()

    print(f"loading {sim_start} .. {end}", flush=True)
    market = load_market(sim_start, end, new_formula=False)
    history = CapacityAwards.from_public(
        min(sim_start, window[0]) - pd.Timedelta(days=30), max(end, window[1]), product=product
    )
    awards = awards_for(market, history)
    da_fallback = bool(np.isnan(market.da_price).any())
    make_current = resolve_factory(args.implicit_factory, "current")

    print("main suite (current formula)", flush=True)
    t0 = time.time()
    suite = run_policy_suite(
        market, awards, make_inner=make_current, config=config, publication=publication, history_awards=history
    )
    print(f"  done in {time.time() - t0:.0f} s", flush=True)
    table_periods = {name: (s, e) for name, (s, e, _) in periods.items()}
    table_periods["full"] = (report_start, end)
    main_table = stack_table(suite.results, table_periods)

    print("Jan-Jul 2026 window under both mechanisms", flush=True)
    window_market = load_market(window[0], window[1], new_formula=True)
    window_awards = awards_for(window_market, history)
    window_suites: dict[str, PolicySuite] = {}
    window_rows = []
    for mechanism in ("current", "new2026"):
        window_suites[mechanism] = run_policy_suite(
            window_market,
            window_awards,
            make_inner=resolve_factory(args.implicit_factory, mechanism),
            mechanism=mechanism,
            config=config,
            publication=publication,
            history_awards=history,
        )
        window_rows.append(stack_table(window_suites[mechanism].results, {"2026-01_2026-07": window}))
    window_table = pd.concat(window_rows, ignore_index=True)

    sensitivity = None
    if not (args.quick or args.no_sensitivities):
        print("sensitivities", flush=True)
        sensitivity = run_sensitivities(
            market, awards, history, config, make_current, periods | {"full": (report_start, end, "")}
        )

    print("causality audits on real data", flush=True)
    audit_start = report_start if args.quick else local_midnight_utc("2025-03-01")
    audits = causality_audits(market, history, config, make_current, publication, audit_start)
    print(f"  {audits}", flush=True)
    print("diagnostics and figures", flush=True)
    capacity_monthly = monthly_capacity_prices(history, report_start, end)
    allocation = allocation_monthly(suite, report_start)
    report_market = market.slice(report_start, end)
    activation = activation_table(report_market, awards_for(report_market, history), periods)
    volumes = monthly_volumes(report_market)
    figures = {
        "prices": "figures/reserve_capacity_prices.png",
        "stack": "figures/reserve_revenue_stack.png",
        "allocation": "figures/reserve_allocation.png",
        "mechanisms": "figures/reserve_mechanism_window.png",
        "activation": "figures/reserve_activation_validation.png",
    }
    plot_capacity_prices(capacity_monthly, periods, out / "reports" / figures["prices"])
    plot_revenue_stack(main_table, periods, out / "reports" / figures["stack"])
    plot_allocation(allocation, out / "reports" / figures["allocation"])
    plot_mechanisms(window_table, out / "reports" / figures["mechanisms"])
    plot_activation_validation(volumes, out / "reports" / figures["activation"])

    print("ledgers", flush=True)
    ledgers: list[str] = []
    for policy in POLICIES if args.all_ledgers else LEDGER_POLICIES:
        result = suite.results[policy]
        for name, (s, e, _) in periods.items():
            paths = save_ledger(result, ledger_dir, period_label=name, start=s, end=e)
            ledgers.append(paths["parquet"].name)
        for window_suite in window_suites.values():
            paths = save_ledger(window_suite.results[policy], ledger_dir, period_label="2026-01_2026-07")
            ledgers.append(paths["parquet"].name)

    context = {
        "generated_utc": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M UTC"),
        "periods": periods,
        "battery": asdict(battery),
        "product": asdict(product),
        "publication": asdict(publication),
        "sim_start_local": f"{sim_start.tz_convert(TZ):%Y-%m-%d}",
        "report_start_local": f"{report_start.tz_convert(TZ):%Y-%m-%d}",
        "end_local": f"{end.tz_convert(TZ):%Y-%m-%d}",
        "implicit_factory": args.implicit_factory,
        "da_fallback": da_fallback,
        "limitations": LIMITATIONS,
    }
    context["audits"] = audits
    write_report(
        report_path,
        context=context,
        main_table=main_table,
        window_table=window_table,
        sensitivity=sensitivity,
        capacity_monthly=capacity_monthly,
        allocation=allocation,
        activation=activation,
        volumes=volumes,
        figures=figures,
    )
    summary = {
        "generated_utc": context["generated_utc"],
        "config": config.describe(),
        "publication": asdict(publication),
        "periods": {n: [str(s), str(e), d] for n, (s, e, d) in periods.items()},
        "day_ahead_fallback": da_fallback,
        "main_table": main_table.to_dict(orient="records"),
        "window_table": window_table.to_dict(orient="records"),
        "sensitivity": None if sensitivity is None else sensitivity.to_dict(orient="records"),
        "allocation_monthly": allocation.to_dict(orient="records"),
        "ledgers": ledgers,
        "runtime_s": round(time.time() - t_start, 1),
    }
    (ledger_dir / "reserve_study_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    pd.set_option("display.width", 220)
    show = main_table.copy()
    for column in ("afrr_capacity", "afrr_energy", "implicit_balancing", "shortfall_imbalance", "degradation", "total"):
        show[column] = (show[column] / 1000).round(1)
    print(
        show[
            [
                "period",
                "policy",
                "afrr_capacity",
                "afrr_energy",
                "implicit_balancing",
                "shortfall_imbalance",
                "degradation",
                "total",
                "cycles_per_day",
            ]
        ].to_string(index=False)
    )
    print(f"finished in {time.time() - t_start:.0f} s", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
