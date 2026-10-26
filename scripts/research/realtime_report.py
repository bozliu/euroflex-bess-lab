"""Report writer for ``realtime_backtest.py``: tables, figures and ``reports/realtime_belgium.md``.

Imported by the backtest script (``--stages report``); it only reads cached results, so the report can be rebuilt
without re-running simulations.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

TZ = "Europe/Brussels"
# validated categorical order (dataviz reference palette, light mode); colour follows the strategy everywhere
COLORS = {
    "forecast_mpc": "#2a78d6",
    "prism": "#eb6834",
    "threshold_si": "#1baf7a",
    "threshold_price": "#eda100",
    "rl_dqn": "#e87ba4",
    "forecast_mpc_risk": "#008300",
    "prism_risk": "#6250d6",
}
INK, INK2, MUTED, GRID, AXIS, SURFACE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "#fcfcfb"
LABELS = {
    "idle": "Idle",
    "threshold_price": "Threshold (running price)",
    "threshold_si": "Threshold (running SI)",
    "forecast_mpc": "Forecast MPC",
    "forecast_mpc_risk": "Forecast MPC, risk filter",
    "prism": "SCALES-L",
    "prism_risk": "SCALES-L, risk filter",
    "rl_dqn": "RL (DQN)",
    "oracle_qh": "Oracle (perfect foresight)",
    "ne_naive_frozen": "(a) naive: current-formula model, frozen 2026-01-01",
    "ne_naive_frozen_risk": "(a) naive, risk filter",
    "ne_naive_wf": "(a') naive: current-formula model, monthly retrain",
    "ne_price_new2026": "(b) model trained on new-formula prices Jan-Mar",
    "ne_price_new2026_risk": "(b) new-formula model, risk filter",
    "ne_prism_zero_frozen": "(c) SCALES-L zero-history, frozen 2026-01-01",
    "ne_prism_zero_frozen_risk": "(c) SCALES-L zero-history, risk filter",
    "ne_prism_zero_wf": "(c') SCALES-L zero-history, monthly retrain",
    "ne_prism_new_published": "(d) SCALES-L on published new components Jan-Mar",
    "ne_prism_zero_live": "(c'') SCALES-L zero-history + live running price, frozen",
    "ne_prism_zero_live_risk": "(c'') SCALES-L zero-history + live running price, risk filter",
    "prism_live": "SCALES-L + live running price",
    "prism_live_risk": "SCALES-L + live running price, risk filter",
}
MAIN_ORDER = (
    "threshold_price",
    "threshold_si",
    "forecast_mpc",
    "forecast_mpc_risk",
    "prism",
    "prism_risk",
    "prism_live",
    "prism_live_risk",
    "rl_dqn",
    "oracle_qh",
)
NE_ORDER = (
    "threshold_price",
    "threshold_si",
    "ne_naive_frozen",
    "ne_naive_frozen_risk",
    "ne_naive_wf",
    "ne_price_new2026",
    "ne_price_new2026_risk",
    "ne_prism_zero_frozen",
    "ne_prism_zero_frozen_risk",
    "ne_prism_zero_wf",
    "ne_prism_zero_live",
    "ne_prism_zero_live_risk",
    "ne_prism_new_published",
    "oracle_qh",
)


def _style(ax: Any) -> None:
    ax.set_facecolor(SURFACE)
    ax.grid(True, color=GRID, linewidth=0.8, linestyle="-")
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
        ax.spines[side].set_linewidth(0.8)
    ax.tick_params(colors=INK2, labelsize=8, length=0)
    ax.xaxis.label.set_color(INK2)
    ax.yaxis.label.set_color(INK2)
    ax.title.set_color(INK)


def _fmt(value: Any, digits: int = 0) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, bool | np.bool_):
        return "yes" if value else "NO"
    if isinstance(value, float | int | np.floating | np.integer):
        if not np.isfinite(float(value)):
            return "n/a"
        return f"{float(value):,.{digits}f}"
    return str(value)


def _table(rows: list[dict[str, Any]], columns: list[tuple[str, str, int]]) -> str:
    head = "| " + " | ".join(c[1] for c in columns) + " |"
    sep = "| " + " | ".join("---" if i == 0 else "---:" for i in range(len(columns))) + " |"
    body = ["| " + " | ".join(_fmt(r.get(c[0]), c[2]) for c in columns) + " |" for r in rows]
    return "\n".join([head, sep, *body])


SUMMARY_COLUMNS = [
    ("label", "Strategy", 0),
    ("eur_per_mw_year", "EUR/MW/yr", 0),
    ("capture_vs_oracle", "Capture vs oracle", 3),
    ("cycles_per_day", "Cycles/day", 2),
    ("avg_sell_price_eur_mwh", "Avg sell price", 1),
    ("avg_buy_price_eur_mwh", "Avg buy price", 1),
    ("daily_pnl_cvar05_eur", "Daily CVaR 5 % (EUR)", 0),
    ("max_drawdown_eur", "Max drawdown (EUR)", 0),
    ("negative_day_share", "Loss-day share", 3),
]


def load_group(cache: Path, key: str) -> tuple[dict[str, dict[str, Any]], dict[str, pd.DataFrame]]:
    out_dir = cache / "sim" / key
    summaries: dict[str, dict[str, Any]] = {}
    ledgers: dict[str, pd.DataFrame] = {}
    if not out_dir.exists():
        return summaries, ledgers
    for path in sorted(out_dir.glob("*.json")):
        if path.name.startswith("skill_"):
            continue
        summary = json.loads(path.read_text())
        summary["label"] = LABELS.get(path.stem, path.stem)
        summaries[path.stem] = summary
        ledger = out_dir / f"{path.stem}.parquet"
        if ledger.exists():
            ledgers[path.stem] = pd.read_parquet(ledger)
    # capture ratios relative to the oracle of the same group (oracle is simulated first, but recompute to be safe)
    if "oracle_qh" in summaries:
        oracle = summaries["oracle_qh"]["pnl_eur"]
        for summary in summaries.values():
            summary["capture_vs_oracle"] = summary["pnl_eur"] / oracle if oracle > 0 else float("nan")
    return summaries, ledgers


def load_skill(cache: Path, key: str, model: str) -> dict[str, pd.DataFrame] | None:
    path = cache / "sim" / key / f"skill_{model}.json"
    if not path.exists():
        return None
    raw = json.loads(path.read_text())
    return {h: pd.DataFrame(v).set_index("minute_in_qh") for h, v in raw.items()}


def summary_table(summaries: dict[str, dict[str, Any]], order: tuple[str, ...]) -> str:
    rows = [summaries[n] for n in order if n in summaries]
    return _table(rows, SUMMARY_COLUMNS)


def monthly_pnl(ledgers: dict[str, pd.DataFrame], names: tuple[str, ...]) -> pd.DataFrame:
    frames = {}
    for name in names:
        if name not in ledgers:
            continue
        ledger = ledgers[name]
        month = pd.to_datetime(ledger["timestamp_utc"], utc=True).dt.tz_convert(TZ).dt.strftime("%Y-%m")
        frames[name] = ledger.groupby(month)["pnl_eur"].sum()
    return pd.DataFrame(frames)


# ------------------------------------------------------------------------------------------------------------------
# figures


def fig_cumulative(cache: Path, periods: dict[str, Any], path: Path) -> None:
    panels = [(p, m) for p, (_, _, mechs) in periods.items() for m in mechs]
    fig, axes = plt.subplots(1, len(panels), figsize=(4.2 * len(panels), 3.4), sharey=False)
    fig.patch.set_facecolor(SURFACE)
    names = ("threshold_si", "forecast_mpc", "forecast_mpc_risk", "prism", "prism_risk", "rl_dqn")
    for ax, (period, mechanism) in zip(np.atleast_1d(axes), panels, strict=True):
        _, ledgers = load_group(cache, f"{period}_{mechanism}_1mw_2mwh")
        for name in names:
            if name not in ledgers:
                continue
            ledger = ledgers[name]
            t = pd.to_datetime(ledger["timestamp_utc"], utc=True)
            ax.plot(t, ledger["pnl_eur"].cumsum() / 1000.0, color=COLORS[name], linewidth=1.6, label=LABELS[name])
        _style(ax)
        ax.set_title(f"Period {period}, {mechanism} formula", fontsize=9, loc="left")
        ax.set_ylabel("Cumulative P&L, kEUR per MW", fontsize=8)
        ax.tick_params(axis="x", labelrotation=30)
    handles, labels = np.atleast_1d(axes)[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=len(labels), frameon=False, fontsize=8, labelcolor=INK2)
    fig.suptitle(
        "Cumulative P&L of tradable strategies, 1 MW / 2 MWh (oracle omitted for scale)",
        fontsize=10,
        color=INK,
        x=0.01,
        ha="left",
    )
    fig.tight_layout(rect=(0, 0.08, 1, 0.94))
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def fig_monthly(month_table: pd.DataFrame, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 3.6))
    fig.patch.set_facecolor(SURFACE)
    x = np.arange(len(month_table))
    for name in month_table.columns:
        if name not in COLORS:
            continue
        ax.plot(
            x,
            month_table[name] / 1000.0,
            color=COLORS[name],
            linewidth=1.8,
            marker="o",
            markersize=4,
            label=LABELS[name],
        )
    _style(ax)
    ax.set_xticks(x)
    ax.set_xticklabels(month_table.index, rotation=60, fontsize=7)
    ax.set_ylabel("Monthly P&L, kEUR per MW", fontsize=8)
    ax.axhline(0.0, color=AXIS, linewidth=0.8)
    ax.legend(frameon=False, fontsize=8, labelcolor=INK2, ncol=2, loc="upper right")
    ax.set_title("Monthly P&L under the current formula, 1 MW / 2 MWh (periods A, B, C)", fontsize=10, loc="left")
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def fig_skill(price: dict[str, pd.DataFrame], prism: dict[str, pd.DataFrame] | None, path: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.4))
    fig.patch.set_facecolor(SURFACE)
    t = price["h0"].drop(index="all")
    minutes = t.index.astype(int)
    ax = axes[0]
    series = [
        ("mae_mean", "Model mean", COLORS["forecast_mpc"]),
        ("mae_median", "Model median", COLORS["prism"]),
        ("mae_running", "Running QH-to-date estimate", COLORS["threshold_si"]),
        ("mae_persistence", "Last published price", COLORS["threshold_price"]),
    ]
    for column, label, color in series:
        if column in t:
            ax.plot(minutes, t[column], color=color, linewidth=1.8, marker="o", markersize=4, label=label)
    _style(ax)
    ax.set_xlabel("Decision minute in the quarter-hour (1-minute publication lag)", fontsize=8)
    ax.set_ylabel("MAE of the final price, EUR/MWh", fontsize=8)
    ax.set_title("Current quarter-hour price", fontsize=9, loc="left")
    ax.legend(frameon=False, fontsize=7, labelcolor=INK2)
    ax = axes[1]
    ax.plot(
        minutes,
        t["brier"],
        color=COLORS["forecast_mpc"],
        linewidth=1.8,
        marker="o",
        markersize=4,
        label="Price model sign head",
    )
    if prism is not None:
        tp = prism["h0"].drop(index="all")
        ax.plot(
            minutes,
            tp["brier"],
            color=COLORS["prism"],
            linewidth=1.8,
            marker="o",
            markersize=4,
            label="SCALES-L regime head",
        )
    ax.plot(
        minutes,
        t["brier_climatology"],
        color=COLORS["threshold_si"],
        linewidth=1.8,
        marker="o",
        markersize=4,
        label="Base rate",
    )
    _style(ax)
    ax.set_xlabel("Decision minute in the quarter-hour", fontsize=8)
    ax.set_ylabel("Brier score of P(SI < 0)", fontsize=8)
    ax.set_title("Sign of the final system imbalance", fontsize=9, loc="left")
    ax.legend(frameon=False, fontsize=7, labelcolor=INK2)
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def fig_bars(rows: list[tuple[str, float]], title: str, xlabel: str, path: Path, highlight: tuple[str, ...]) -> None:
    fig, ax = plt.subplots(figsize=(8.5, 0.42 * len(rows) + 1.2))
    fig.patch.set_facecolor(SURFACE)
    labels = [r[0] for r in rows]
    values = [r[1] / 1000.0 for r in rows]
    colors = [COLORS["prism"] if any(h in r[0] for h in highlight) else COLORS["forecast_mpc"] for r in rows]
    y = np.arange(len(rows))
    ax.barh(y, values, height=0.55, color=colors)
    for yi, v in zip(y, values, strict=True):
        ax.text(v + (0.01 * max(max(values), 1.0)), yi, f"{v:,.0f}k", va="center", fontsize=7, color=INK2)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=7)
    ax.invert_yaxis()
    _style(ax)
    ax.set_xlabel(xlabel, fontsize=8)
    ax.set_title(title, fontsize=9, loc="left")
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def fig_lag(table: pd.DataFrame, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(6.5, 3.4))
    fig.patch.set_facecolor(SURFACE)
    for name in table.columns:
        if name not in COLORS:
            continue
        ax.plot(
            table.index,
            table[name] / 1000.0,
            color=COLORS[name],
            linewidth=1.8,
            marker="o",
            markersize=5,
            label=LABELS[name],
        )
    _style(ax)
    ax.set_xticks(list(table.index))
    ax.set_xlabel("Minute-data publication lag (minutes)", fontsize=8)
    ax.set_ylabel("kEUR per MW per year", fontsize=8)
    ax.set_title("Publication-lag sensitivity, period C, current formula", fontsize=9, loc="left")
    ax.legend(frameon=False, fontsize=7, labelcolor=INK2)
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def fig_running(semantics: dict[str, Any], path: Path) -> None:
    rows = pd.DataFrame(semantics["running_estimate_mae_by_minute"])
    fig, ax = plt.subplots(figsize=(6.5, 3.4))
    fig.patch.set_facecolor(SURFACE)
    palette = [
        COLORS["forecast_mpc"],
        COLORS["prism"],
        COLORS["threshold_si"],
        COLORS["threshold_price"],
        COLORS["rl_dqn"],
    ]
    for i, ((window, mechanism), group) in enumerate(rows.groupby(["window", "mechanism"], sort=False)):
        ax.plot(
            group["minute"],
            group["mae"],
            color=palette[i % len(palette)],
            linewidth=1.8,
            marker="o",
            markersize=4,
            label=f"{mechanism} formula, {window}",
        )
    _style(ax)
    ax.set_xlabel("Minute of the quarter-hour of the cumulative row", fontsize=8)
    ax.set_ylabel("MAE vs final price, EUR/MWh", fontsize=8)
    ax.set_title("How fast the running QH-to-date price converges", fontsize=9, loc="left")
    ax.legend(frameon=False, fontsize=7, labelcolor=INK2)
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


# ------------------------------------------------------------------------------------------------------------------
# report


def _skill_rows(tables: dict[str, pd.DataFrame], minutes: tuple[str, ...]) -> list[dict[str, Any]]:
    t = tables["h0"]
    rows = []
    for m in minutes:
        if m not in t.index:
            continue
        r = t.loc[m].to_dict()
        r["minute"] = m
        rows.append(r)
    return rows


SKILL_COLUMNS = [
    ("minute", "Minute in QH", 0),
    ("mae_mean", "MAE mean", 1),
    ("mae_median", "MAE median", 1),
    ("mae_running", "MAE running est.", 1),
    ("mae_persistence", "MAE last final", 1),
    ("pinball", "Pinball (avg 5 q)", 2),
    ("crps", "CRPS", 2),
    ("coverage_80", "80 % coverage", 3),
    ("brier", "Brier P(SI<0)", 3),
    ("brier_climatology", "Brier base rate", 3),
]


def _value(summaries: dict[str, dict[str, Any]], name: str, key: str = "eur_per_mw_year") -> float | None:
    row = summaries.get(name)
    if row is None:
        return None
    value = row.get(key)
    return float(value) if value is not None and np.isfinite(float(value)) else None


def key_findings(cache: Path, periods: dict[str, tuple[str, str, tuple[str, ...]]]) -> list[str]:
    """Bullet points computed from the cached runs (no hand-written numbers)."""
    out: list[str] = []
    for period, (_, _, mechanisms) in periods.items():
        for mechanism in mechanisms:
            summaries, _ = load_group(cache, f"{period}_{mechanism}_1mw_2mwh")
            if not summaries:
                continue
            thr = max(
                (v for v in (_value(summaries, "threshold_price"), _value(summaries, "threshold_si")) if v is not None),
                default=None,
            )
            best_name = None
            best_val = -np.inf
            for name in (
                "forecast_mpc",
                "forecast_mpc_risk",
                "prism",
                "prism_risk",
                "prism_live",
                "prism_live_risk",
                "rl_dqn",
            ):
                v = _value(summaries, name)
                if v is not None and v > best_val:
                    best_name, best_val = name, v
            oracle = _value(summaries, "oracle_qh")
            if best_name is None or thr is None or oracle is None:
                continue
            uplift = (best_val / thr - 1.0) * 100.0 if thr > 0 else float("nan")
            out.append(
                f"Period {period}, {mechanism} formula: best learned strategy is {LABELS[best_name]} with"
                f" {best_val:,.0f} EUR/MW/yr ({best_val / oracle:.0%} of the {oracle:,.0f} oracle), {uplift:+.0f} % vs the"
                f" best reactive threshold ({thr:,.0f})."
            )
    b_cur, _ = load_group(cache, "B_current_1mw_2mwh")
    b_new, _ = load_group(cache, "B_new2026_1mw_2mwh")
    if b_cur and b_new:
        oc, on = _value(b_cur, "oracle_qh"), _value(b_new, "oracle_qh")
        if oc and on:
            out.append(
                f"Same period-B days ({periods['B'][0][:10]} to {periods['B'][1][:10]}), perfect foresight: {on:,.0f}"
                f" EUR/MW/yr under the new formula vs {oc:,.0f} under the current one ({on / oc - 1.0:+.0%})."
            )
        for name in ("forecast_mpc_risk", "prism_risk", "threshold_si"):
            vc, vn = _value(b_cur, name), _value(b_new, name)
            if vc and vn:
                out.append(f"{LABELS[name]}: {vn:,.0f} (new) vs {vc:,.0f} (current) EUR/MW/yr on the same days.")
    ne, _ = load_group(cache, "NE_AprJul_new2026")
    if ne:
        arms = {
            "naive (a)": _value(ne, "ne_naive_frozen_risk") or _value(ne, "ne_naive_frozen"),
            "new-formula-trained (b)": _value(ne, "ne_price_new2026_risk") or _value(ne, "ne_price_new2026"),
            "SCALES-L zero-history (c)": _value(ne, "ne_prism_zero_frozen_risk") or _value(ne, "ne_prism_zero_frozen"),
            "SCALES-L zero-history + live running price (c'')": _value(ne, "ne_prism_zero_live_risk")
            or _value(ne, "ne_prism_zero_live"),
        }
        if all(v is not None for v in arms.values()):
            text = ", ".join(f"{k} {v:,.0f}" for k, v in arms.items())
            out.append(f"Natural experiment, Apr-Jul 2026 at the new price (risk-filter variants, EUR/MW/yr): {text}.")
    lag = {}
    for k in (0, 1, 2, 3):
        summaries, _ = load_group(cache, f"LAG{k}_C_current")
        v = _value(summaries, "forecast_mpc_risk")
        if v is not None:
            lag[k] = v
    if len(lag) >= 2 and 0 in lag and max(lag) in lag:
        top = max(lag)
        out.append(
            f"Publication lag: forecast MPC (risk) earns {lag[0]:,.0f} EUR/MW/yr with 0-minute lag and {lag[top]:,.0f}"
            f" with {top}-minute lag in period C ({(lag[top] / lag[0] - 1.0):+.0%})."
        )
    return out


def write_report(s: Any, written_ledgers: list[str]) -> Path:  # noqa: C901 - one linear report
    cache: Path = s.cache
    figures: Path = s.figures
    figures.mkdir(parents=True, exist_ok=True)
    out_md: Path = s.reports / "realtime_belgium.md"
    periods: dict[str, tuple[str, str, tuple[str, ...]]] = s.periods
    semantics_path = s.ledgers / "realtime_data_semantics.json"
    semantics = json.loads(semantics_path.read_text()) if semantics_path.exists() else {}
    audit_path = s.ledgers / "realtime_lookahead_audit.json"
    audit = pd.read_json(audit_path) if audit_path.exists() else pd.DataFrame()
    rl_meta_path = cache / "rl_policy.json"
    rl_meta = json.loads(rl_meta_path.read_text()) if rl_meta_path.exists() else None
    train_log_path = cache / "train_log.json"
    train_log = json.loads(train_log_path.read_text()) if train_log_path.exists() else []

    lines: list[str] = []
    add = lines.append
    add("# Belgian real-time implicit balancing: forecast MPC, SCALES-L and RL on Elia data")
    add("")
    add("> Generated by `scripts/research/realtime_backtest.py` from `data/public` (Elia Open Data, CC BY 4.0,")
    add('> attribution "Elia Transmission Belgium SA"; day-ahead prices from Energy-Charts, CC BY 4.0). Every number')
    add("> below is produced by that script; do not edit by hand. Simulated money, not trading advice.")
    add("")

    # ---------------------------------------------------------------- headline
    add("## Headline")
    add("")
    head_rows = []
    for period, (start, end, mechanisms) in periods.items():
        for mechanism in mechanisms:
            summaries, _ = load_group(cache, f"{period}_{mechanism}_1mw_2mwh")
            if not summaries:
                continue
            tradable = {k: v for k, v in summaries.items() if k not in ("oracle_qh", "idle")}
            best = max(tradable.values(), key=lambda r: r["eur_per_mw_year"]) if tradable else None
            row = {
                "period": f"{period}: {start[:10]} to {end[:10]}",
                "mechanism": mechanism,
                "oracle": summaries.get("oracle_qh", {}).get("eur_per_mw_year"),
                "threshold": max(
                    (summaries[n]["eur_per_mw_year"] for n in ("threshold_price", "threshold_si") if n in summaries),
                    default=None,
                ),
                "mpc": summaries.get("forecast_mpc_risk", {}).get("eur_per_mw_year"),
                "prism": summaries.get("prism_risk", {}).get("eur_per_mw_year"),
                "best": best["label"] if best else None,
                "best_value": best["eur_per_mw_year"] if best else None,
                "best_capture": best.get("capture_vs_oracle") if best else None,
            }
            head_rows.append(row)
    add(
        _table(
            head_rows,
            [
                ("period", "Period", 0),
                ("mechanism", "Formula", 0),
                ("threshold", "Best threshold", 0),
                ("mpc", "Forecast MPC (risk)", 0),
                ("prism", "SCALES-L (risk)", 0),
                ("best", "Best tradable", 0),
                ("best_value", "Best EUR/MW/yr", 0),
                ("best_capture", "Capture", 3),
                ("oracle", "Oracle EUR/MW/yr", 0),
            ],
        )
    )
    add("")
    add("1 MW / 2 MWh battery, round-trip efficiency 0.95 x 0.95, SOC band 5-95 %, degradation 8 EUR per MWh")
    add("discharged, minute data usable 1 minute after the minute ends, final quarter-hour prices 5 minutes after the")
    add(
        "quarter-hour, price taker (no own price impact). EUR/MW/yr annualises the net P&L (revenue minus degradation)."
    )
    add("")

    findings = key_findings(cache, periods)
    if findings:
        add("### Key findings (computed)")
        add("")
        for item in findings:
            add(f"- {item}")
        add("")

    # ---------------------------------------------------------------- data semantics
    add("## Data and minute-level semantics (checked, not assumed)")
    add("")
    if semantics:
        rows = semantics.get("ods133_last_minute_vs_ods134", [])
        add("**ODS133 (cumulative quarter-hour-to-date) at the last minute vs the validated ODS134 quarter-hour.** The")
        add("running estimate at minute 14 is *close to* but not identical to the final price: Elia validates the")
        add("quarter-hour after real time, so even a perfect reader of the running feed faces residual uncertainty.")
        add("")
        add(
            _table(
                rows,
                [
                    ("window", "Window", 0),
                    ("qh", "QHs", 0),
                    ("price_exact_share", "Price identical (+/-0.01)", 3),
                    ("price_mae_eur_mwh", "Price MAE", 2),
                    ("price_p95_abs_err_eur_mwh", "Price p95 abs err", 1),
                    ("si_mae_mw", "SI MAE (MW)", 1),
                    ("si_sign_agreement", "SI sign agreement", 4),
                ],
            )
        )
        add("")
        if rows:
            offsets = rows[0]["ods126_vs_ods133_inst_mae_by_offset"]
            best = min(offsets, key=lambda k: offsets[k])
            add(
                "**ODS126 is instantaneous, ODS133 is cumulative, and they are offset by one minute.** Differencing the"
                " ODS133 cumulative SI recovers an instantaneous series that matches ODS126 best when ODS126 is read"
                f" {best} minute(s) later (MAE by offset: "
                + ", ".join(f"{k}: {v:.1f} MW" for k, v in offsets.items())
                + "). The loaders keep Elia's timestamps, so ODS126 rows enter the features one minute later than"
                " strictly necessary (conservative, never look-ahead)."
            )
            add("")
        nf = semantics.get("ods211_last_minute_vs_ods212", [])
        if nf:
            add("**New-formula minute data (ODS211) are cumulative quarter-hour-to-date values as well**: at the last")
            add("minute the cumulative fields equal the ODS212 quarter-hour values, and the minute-to-minute change of")
            add(
                "`nf_si_mw` shrinks like 1/(m+1) through the quarter-hour (the signature of a running mean), exactly like"
            )
            add("ODS133 `si_cum_mw` and unlike the instantaneous ODS126 `si_mw`.")
            add("")
            add(
                _table(
                    nf,
                    [
                        ("field", "ODS211 field", 0),
                        ("qh", "QHs", 0),
                        ("last_minute_exact_share", "Last minute == ODS212", 3),
                        ("mae", "MAE", 2),
                    ],
                )
            )
            add("")
            sig = semantics.get("minute_change_std_by_minute", {})
            if sig:
                add(
                    "Std of the minute-to-minute change at minutes 1, 5, 10, 14 of the quarter-hour: "
                    + "; ".join(f"`{k}` {v[0]:.1f} / {v[4]:.1f} / {v[9]:.1f} / {v[13]:.1f}" for k, v in sig.items())
                    + "."
                )
                add("")
        si = semantics.get("si_ods134_vs_ods212")
        rec = semantics.get("reconstruction_vs_ods212")
        if si and rec:
            add(
                f"ODS212 and ODS134 report almost the same final SI (MAE {si['mae_mw']:.1f} MW, sign agreement"
                f" {si['sign_agreement']:.4f}). The counterfactual reconstruction of the new price from current-design"
                f" components (`reconstruct_new_formula_history`) has a quarter-hour MAE of {rec['mae_eur_mwh']:.1f}"
                f" EUR/MWh and correlation {rec['corr']:.3f} with ODS212 (the current price correlates"
                f" {rec['corr_current_vs_new']:.3f}). The error comes from a genuinely new input: the new formula uses"
                " *directional* aFRR prices, while current-design data only publish one aFRR price per quarter-hour"
                f" (MAE {rec['afrr_single_vs_directional_up_mae']:.1f} up / {rec['afrr_single_vs_directional_down_mae']:.1f}"
                " down EUR/MWh). At the regime level the reconstruction is nearly unbiased:"
            )
            add("")
            reg_rows = [{"regime": k, **v} for k, v in rec["regime_means"].items()]
            add(
                _table(
                    reg_rows,
                    [
                        ("regime", "Final SI regime", 0),
                        ("share", "Share of QHs", 3),
                        ("current_mean", "Current price mean", 1),
                        ("new2026_published_mean", "New price (ODS212) mean", 1),
                        ("new2026_reconstructed_mean", "Reconstructed mean", 1),
                    ],
                )
            )
            add("")
            spread_cur = rec["regime_means"]["short"]["current_mean"] - rec["regime_means"]["long"]["current_mean"]
            spread_new = (
                rec["regime_means"]["short"]["new2026_published_mean"]
                - rec["regime_means"]["long"]["new2026_published_mean"]
            )
            add(
                f"The short-minus-long price spread - what an implicit-balancing battery monetises - falls from"
                f" {spread_cur:.0f} EUR/MWh under the current formula to {spread_new:.0f} EUR/MWh under the new one on the"
                f" same Jan-Jul 2026 days. The incentive component was active in {rec['incentive_qh']} quarter-hours."
            )
            add("")
        fig_running(semantics, figures / "realtime_running_estimate.png")
        add("![Running estimate convergence](figures/realtime_running_estimate.png)")
        add("")

    # ---------------------------------------------------------------- methods
    add("## Methods")
    add("")
    add("**Information model.** The decision for minute t is taken at the start of the minute and may use minute rows")
    add("j <= t - lag - 1, final quarter-hour values published by t (quarter-hour end + 5 minutes) and day-ahead")
    add(
        "prices published by t (13:00 local on D-1). With the default 1-minute lag a strategy sees no row of the current"
    )
    add("quarter-hour at minutes 0 and 1, and at the last minute it has seen 13 of 15 minutes.")
    add("")
    add("**Features** (`realtime.forecasting.features`, one row per decision minute, visible data only): running")
    add(
        "quarter-hour-to-date SI, price, MIP/MDP/alpha and aFRR price (ODS133/ODS165); instantaneous SI, its 1/3/5-minute"
    )
    add("trend and activated aFRR/mFRR/IGCC volumes (ODS126); a projected final SI (visible running mean extended with")
    add(
        "the latest instantaneous value) and the mechanism applied to it; the previous quarter-hour's last visible values;"
    )
    add("Elia's own SI forecasts and probability bins for the current and next quarter-hour (ODS136/ODS147); day-ahead")
    add("price; recent published final prices and SI; calendar. New-formula running fields (`nf_*`) form a separate")
    add(
        "group. A perturbation test proves every feature row is unchanged when all unpublished data is replaced by noise."
    )
    add("")
    add("**Forecasters** (scikit-learn histogram gradient boosting, fixed hyper-parameters chosen a priori: 200 trees,")
    add("learning rate 0.08, 31 leaves, min 80 samples per leaf). Heads predict the residual to the running")
    add("quarter-hour-to-date estimate (trees cannot reproduce a near-identity map on heavy-tailed prices):")
    add("quantiles 10/25/50/75/90 %, the conditional mean, and P(SI < 0), for the current and the next quarter-hour.")
    add("Walk-forward: retrained at every month start on the previous 6 months (every third minute), with an embargo")
    add(
        "that drops training rows whose target was not yet published at the fit time; a fold never sees its test month."
    )
    add("Every fold's train and test window is listed in `reports/ledgers/realtime_forecaster_folds.json`.")
    add("")
    add("**Forecast MPC** (`strategies/forecast_mpc.py`). A stochastic dynamic programme on a 46-point SOC grid values")
    add("energy at the start of the next quarter-hour: stage 1 uses the model's next-quarter-hour price distribution")
    add(
        "(re-planned at minutes 0, 5, 10), stages 2-16 (4 hours) use 28-day same-time-of-day quantiles of published prices,"
    )
    add("and a terminal value prices leftover energy at the trailing 7-day mean. Every minute it solves the remaining")
    add(
        "energy of the current quarter-hour in closed form against that value function, with SOC limits, efficiencies and"
    )
    add("degradation, and spreads it over the remaining minutes. The risk-filter variant discharges only on the 30 %")
    add(
        "quantile and charges only on the 70 % quantile of the current-quarter-hour distribution (a quantile trade filter)."
    )
    add(
        "Grid: 46 SOC points for 2 MWh (23 for 1 MWh) plus exact full-power transitions; per-quarter-hour discount 0.995."
    )
    add("Structural guard: the strategy never pays to inject (no discharge while its discharge price is negative).")
    add("")
    add("**SCALES-L** - the mechanism-aware, lag-aware control layer of the SCALES framework (State, Code/mechanism,")
    add("Agent impact, Lag-aware control, Equilibrium, Structured contracts); implemented in `strategies/prism.py` and")
    add("`forecasting/state.py` (called PRISM in earlier drafts; class names `PrismStrategy`/`PrismForecaster` kept).")
    add("Forecast the mechanism-invariant state, then price it")
    add(
        "with the explicit operator: P(SI < 0) times the regime-conditional component distributions (current: MIP + alpha /"
    )
    add(
        "MDP - alpha; new: volume-weighted aFRR/mFRR main components), i.e. `p * D_up + (1 - p) * D_down`, traded by the"
    )
    add("same MPC core. For the new formula without new-formula history (zero-history transfer) the components are")
    add("rebuilt from current-design state with the new operator, and only mechanism-invariant features are used.")
    add("")
    add(
        "**SCALES-L + live running price** (`forecasting/fusion.py`). After go-live Elia publishes the running new-formula"
    )
    add("price every minute; a zero-history model cannot have learned to use it, and current-design minute data do not")
    add(
        "allow rebuilding it (minute-level reconstruction MAE about 30 EUR/MWh). The fusion treats the live running value"
    )
    add(
        "as a noisy observation of the final price and combines it with SCALES-L's distribution by precision weighting; the"
    )
    add("observation variance per visible minute is the *current* formula's running-estimate error on 2025-07..2025-12")
    add("(no new-formula data, no tuned parameter). It was added after the main run, as an extra arm.")
    add("")
    add(
        "**RL baseline** (`strategies/rl.py`): double DQN over five power levels (-1, -0.5, 0, 0.5, 1 x P) on 26 causal"
    )
    add("market features plus SOC and the energy already delivered in the quarter-hour; reward = realised quarter-hour")
    add("price x energy - degradation; trained on 2024-06..2025-09 with a 2025-10..2025-12 validation window for")
    add("checkpoint selection, wall-clock capped; evaluated out of sample (periods B and C only).")
    add("")
    add(
        "**Simulation.** `realtime.simulate` (minute loop, quarter-hour settlement at the published single price). Each run"
    )
    add(f"starts {3} days before its period (warm-up, then trimmed) so that features and SOC are not cold-started.")
    add("No hyper-parameter was tuned on any reported window. Two development looks at test-period data are disclosed:")
    add(
        "(1) the pipeline was sanity-checked on January 2026 (period B); (2) a development smoke run exposed a failure in"
    )
    add(
        "the 6 April 2026 event (five quarter-hours at the -15,000 EUR/MWh floor): every re-plan paid the floor price to"
    )
    add("make room for one more floor-price quarter-hour and never charged that room. The fix is structural, not tuned")
    add("(the no-negative-price-discharge guard and a 0.995 per-quarter-hour discount, fixed before the full run).")
    add("")

    # ---------------------------------------------------------------- results
    add("## Results")
    add("")
    for period, (start, end, mechanisms) in periods.items():
        for mechanism in mechanisms:
            for battery, label in (("1mw_2mwh", "1 MW / 2 MWh"), ("1mw_1mwh", "1 MW / 1 MWh")):
                summaries, _ = load_group(cache, f"{period}_{mechanism}_{battery}")
                if not summaries:
                    continue
                add(f"### Period {period} ({start[:10]} to {end[:10]}), {mechanism} formula, {label}")
                add("")
                add(summary_table(summaries, MAIN_ORDER))
                add("")
                if period == "A" and battery == "1mw_2mwh":
                    add("RL is not reported for period A: it is the RL training/validation window (in-sample).")
                    add("")
                if mechanism == "new2026":
                    add("Under the new formula the forecast MPC uses the current-formula price model unchanged (naive")
                    add(
                        "transfer); SCALES-L uses the zero-history new-formula operator (no new-formula price ever used)."
                    )
                    add("")
    fig_cumulative(cache, periods, figures / "realtime_cumulative_pnl.png")
    add("![Cumulative P&L](figures/realtime_cumulative_pnl.png)")
    add("")

    # monthly
    month_frames = []
    for period, (_, _, mechanisms) in periods.items():
        if "current" not in mechanisms:
            continue
        _, ledgers = load_group(cache, f"{period}_current_1mw_2mwh")
        month_frames.append(
            monthly_pnl(
                ledgers, ("threshold_si", "forecast_mpc", "forecast_mpc_risk", "prism", "prism_risk", "oracle_qh")
            )
        )
    if month_frames:
        # periods meet at UTC midnight, so a local month can be split across two ledgers: sum, never drop
        months = pd.concat(month_frames).groupby(level=0).sum(min_count=1).sort_index()
        fig_monthly(months.drop(columns=["oracle_qh"], errors="ignore"), figures / "realtime_monthly.png")
        add("### Monthly breakdown (current formula, 1 MW / 2 MWh, EUR per MW per month)")
        add("")
        add("![Monthly P&L](figures/realtime_monthly.png)")
        add("")
        rows = []
        for month, r in months.iterrows():
            row = {"month": month}
            row.update({k: r[k] for k in months.columns})
            if "oracle_qh" in r and r["oracle_qh"] > 0 and "forecast_mpc_risk" in r:
                row["capture"] = r["forecast_mpc_risk"] / r["oracle_qh"]
            rows.append(row)
        cols = [("month", "Month", 0)] + [(c, LABELS.get(c, c), 0) for c in months.columns]
        cols.append(("capture", "MPC-risk capture", 3))
        add(_table(rows, cols))
        add("")

    # ---------------------------------------------------------------- natural experiment
    add("## Natural experiment: the new formula without new-formula history")
    add("")
    add(
        "Elia published the new-formula price (ODS212) for the same Jan-Jul 2026 days that were settled under the current"
    )
    add(
        "formula. Every arm below is settled at the **new-formula** price. Arms (a) and (c) never see a new-formula price"
    )
    add(
        "in training; (b) and (d) are trained on Jan-Mar 2026 new-formula data, so the common test window is Apr-Jul 2026."
    )
    add("")
    ne, _ = load_group(cache, "NE_AprJul_new2026")
    ne_cur, _ = load_group(cache, "NE_AprJul_current")
    ne_jan, _ = load_group(cache, "NE_JanJul_new2026")
    if ne:
        skill_rows = {}
        for model, arm in (
            ("price_current_frozen", "ne_naive_frozen"),
            ("price_current", "ne_naive_wf"),
            ("price_new2026", "ne_price_new2026"),
            ("prism_zero_frozen", "ne_prism_zero_frozen"),
            ("prism_zero_wf", "ne_prism_zero_wf"),
            ("prism_new_published", "ne_prism_new_published"),
            ("prism_zero_frozen_live", "ne_prism_zero_live"),
        ):
            sk = load_skill(cache, "NE_AprJul_new2026", model)
            if sk is not None:
                skill_rows[arm] = sk["h0"].loc["all"].to_dict()
        rows = []
        for name in NE_ORDER:
            if name not in ne:
                continue
            row = dict(ne[name])
            for arm, sk in skill_rows.items():
                if name.startswith(arm):
                    row.update({f"skill_{k}": v for k, v in sk.items()})
            rows.append(row)
        add("### Apr-Jul 2026, new formula, 1 MW / 2 MWh")
        add("")
        add(
            _table(
                rows,
                [
                    ("label", "Arm", 0),
                    ("eur_per_mw_year", "EUR/MW/yr", 0),
                    ("capture_vs_oracle", "Capture", 3),
                    ("cycles_per_day", "Cycles/day", 2),
                    ("daily_pnl_cvar05_eur", "Daily CVaR 5 %", 0),
                    ("skill_mae_mean", "Price MAE (mean)", 1),
                    ("skill_pinball", "Pinball", 2),
                    ("skill_brier", "Brier P(SI<0)", 3),
                ],
            )
        )
        add("")
        add(
            "Forecast skill columns score the current-quarter-hour forecast of the *new-formula* price over all minutes"
        )
        add("of the window; the naive arm is scored on the new price although it was trained on current prices.")
        add("")
        bars = [(LABELS.get(n, n), ne[n]["eur_per_mw_year"]) for n in NE_ORDER if n in ne and n != "oracle_qh"]
        fig_bars(
            bars,
            "Apr-Jul 2026 settled at the new-formula price (EUR/MW/yr)",
            "kEUR per MW per year",
            figures / "realtime_natural_experiment.png",
            highlight=("SCALES-L",),
        )
        add("![Natural experiment](figures/realtime_natural_experiment.png)")
        add("")
    if ne_cur:
        add("Same window settled at the **current** formula (in-domain reference for the transfer gap):")
        add("")
        add(summary_table(ne_cur, ("threshold_si", "ne_naive_frozen", "forecast_mpc", "prism", "oracle_qh")))
        add("")
    if ne_jan:
        add("### Jan-Jul 2026 (full overlap), new formula, frozen pre-2026 models")
        add("")
        add(
            summary_table(
                ne_jan,
                (
                    "threshold_si",
                    "ne_naive_frozen",
                    "ne_naive_frozen_risk",
                    "ne_prism_zero_frozen",
                    "ne_prism_zero_frozen_risk",
                    "ne_prism_zero_wf",
                    "ne_prism_zero_live",
                    "ne_prism_zero_live_risk",
                    "oracle_qh",
                ),
            )
        )
        add("")

    # ---------------------------------------------------------------- skill
    add("## Forecast skill (walk-forward, out of sample)")
    add("")
    minutes = ("0", "2", "5", "8", "11", "14", "all")
    first_skill = None
    for period, (_, _, mechanisms) in periods.items():
        for mechanism in mechanisms:
            key = f"{period}_{mechanism}_1mw_2mwh"
            for model in ("price_current", "prism_current" if mechanism == "current" else "prism_zero_wf"):
                sk = load_skill(cache, key, model)
                if sk is None:
                    continue
                if first_skill is None and model == "price_current" and mechanism == "current":
                    first_skill = (sk, load_skill(cache, key, "prism_current"))
                add(f"**{model}**, period {period}, scored on the {mechanism}-formula price, current quarter-hour:")
                add("")
                add(_table(_skill_rows(sk, minutes), SKILL_COLUMNS))
                add("")
                nxt = sk["h1"].loc["all"]
                add(
                    f"Next quarter-hour: MAE (mean) {nxt.get('mae_mean', np.nan):.1f} vs last published"
                    f" {nxt.get('mae_persistence', np.nan):.1f} EUR/MWh, Brier {nxt.get('brier', np.nan):.3f} vs base"
                    f" rate {nxt.get('brier_climatology', np.nan):.3f}."
                )
                add("")
    if first_skill is not None:
        fig_skill(first_skill[0], first_skill[1], figures / "realtime_forecast_skill.png")
        add("![Forecast skill](figures/realtime_forecast_skill.png)")
        add("")

    # ---------------------------------------------------------------- sensitivity
    add("## Sensitivity")
    add("")
    lag_rows = {}
    for lag in (0, 1, 2, 3):
        summaries, _ = load_group(cache, f"LAG{lag}_C_current")
        if summaries:
            lag_rows[lag] = {k: v["eur_per_mw_year"] for k, v in summaries.items()}
    if lag_rows:
        lag_table = pd.DataFrame(lag_rows).T.sort_index()
        oracle = lag_table.pop("oracle_qh") if "oracle_qh" in lag_table else None
        add("**Publication lag** (period C, current formula, 1 MW / 2 MWh; forecasters re-trained for every lag):")
        add("")
        rows = [{"lag": lag, **{LABELS.get(k, k): v for k, v in r.items()}} for lag, r in lag_table.iterrows()]
        add(_table(rows, [("lag", "Lag (min)", 0)] + [(LABELS.get(c, c), LABELS.get(c, c), 0) for c in lag_table]))
        add("")
        if oracle is not None:
            add(f"Oracle (lag-independent): {float(oracle.dropna().iloc[0]):,.0f} EUR/MW/yr.")
            add("")
        fig_lag(lag_table, figures / "realtime_lag_sensitivity.png")
        add("![Lag sensitivity](figures/realtime_lag_sensitivity.png)")
        add("")
    deg_rows = []
    for deg in (0, 4, 8, 16):
        key = "B_current_1mw_2mwh" if deg == 8 else f"DEG{deg}_B_current"
        summaries, _ = load_group(cache, key)
        for name in ("threshold_si", "forecast_mpc", "forecast_mpc_risk", "prism", "oracle_qh"):
            if name in summaries:
                deg_rows.append(
                    {
                        "deg": deg,
                        "strategy": LABELS[name],
                        "eur": summaries[name]["eur_per_mw_year"],
                        "cycles": summaries[name]["cycles_per_day"],
                    }
                )
    if deg_rows:
        frame = pd.DataFrame(deg_rows)
        pivot = frame.pivot(index="strategy", columns="deg", values="eur")
        cycles = frame.pivot(index="strategy", columns="deg", values="cycles")
        add("**Degradation cost** (period B, current formula, 1 MW / 2 MWh; EUR/MW/yr, cycles/day in brackets). The")
        add("strategies see the cost in their objective, so they trade less as it rises:")
        add("")
        rows = []
        for strategy in pivot.index:
            row = {"strategy": strategy}
            for deg in pivot.columns:
                row[f"d{deg}"] = f"{pivot.loc[strategy, deg]:,.0f} ({cycles.loc[strategy, deg]:.2f})"
            rows.append(row)
        add(_table(rows, [("strategy", "Strategy", 0)] + [(f"d{d}", f"{d} EUR/MWh", 0) for d in pivot.columns]))
        add("")

    # ---------------------------------------------------------------- audit
    add("## Look-ahead audit on real data")
    add("")
    if not audit.empty:
        add("`check_no_lookahead` replaces every value not yet published at a cut minute (minute rows, quarter-hour")
        add(
            "values, settlement prices, unpublished day-ahead prices) with noise and re-runs the strategy; any change of"
        )
        add(
            "a decision before the earliest legitimate use of that data proves look-ahead. Forecast strategies are also"
        )
        add("audited white-box: their precomputed plan (risk prices, value-function slopes) must be identical before")
        add(f"the cut. Cuts per slice: {', '.join(str(c) for c in sorted(audit['cut_minute'].unique()))} (minutes from")
        add("the slice start).")
        add("")
        oracle_rows = audit[audit["strategy"] == "oracle_qh"]
        if len(oracle_rows):
            failed = int((~oracle_rows["passed"].astype(bool)).sum())
            add(
                f"The oracle is the positive control: it reads unpublished final prices and fails {failed} of"
                f" {len(oracle_rows)} audits (a failure needs the perturbed future to change an earlier decision)."
            )
            add("")
        grouped = audit.groupby(["strategy", "mechanism"], sort=False)
        rows = []
        for (strategy, mechanism), g in grouped:
            white = g["white_box_passed"].dropna()
            rows.append(
                {
                    "strategy": LABELS.get(strategy, strategy),
                    "mechanism": mechanism,
                    "slices": ", ".join(sorted(g["slice"].unique())),
                    "runs": len(g),
                    "passed": int(g["passed"].sum()),
                    "white": f"{int(white.sum())}/{len(white)}" if len(white) else "n/a",
                }
            )
        add(
            _table(
                rows,
                [
                    ("strategy", "Strategy", 0),
                    ("mechanism", "Formula", 0),
                    ("slices", "Real-data slices", 0),
                    ("runs", "Audits", 0),
                    ("passed", "Passed", 0),
                    ("white", "White-box passed", 0),
                ],
            )
        )
        add("")

    # ---------------------------------------------------------------- RL
    add("## RL training details")
    add("")
    if rl_meta:
        log = rl_meta.get("log", {})
        cfg = rl_meta.get("config", {})
        add(
            f"- Device: `{log.get('device')}`; wall-clock {log.get('wall_clock_minutes', 0):.1f} min (cap"
            f" {cfg.get('max_minutes')} min); {log.get('updates', 0):,} gradient updates on"
            f" {log.get('transitions', 0):,} environment transitions ({cfg.get('n_envs')} parallel days)."
        )
        add(
            f"- Network {cfg.get('hidden')}, gamma {cfg.get('gamma')}, lr {cfg.get('lr')}, batch {cfg.get('batch_size')},"
            f" replay {cfg.get('buffer_size'):,}, target sync every {cfg.get('target_update_every')} updates,"
            f" epsilon {cfg.get('eps_start')} -> {cfg.get('eps_end')} over {cfg.get('eps_decay_updates'):,} updates."
        )
        add(
            f"- Train {log.get('train')}, validation {log.get('validation')}; best validation daily P&L"
            f" {log.get('best_valid_daily_pnl_eur', float('nan')):.1f} EUR (checkpoint kept)."
        )
        hist = log.get("validation_history", [])
        if hist:
            add(
                "- Validation curve (updates: EUR/day): "
                + ", ".join(f"{h['updates']:,}: {h['valid_daily_pnl']:.0f}" for h in hist[:: max(1, len(hist) // 10)])
                + "."
            )
        add("")
    else:
        add("RL was not trained in this run.")
        add("")

    # ---------------------------------------------------------------- live deployment
    add("## Live deployment (paper trading)")
    add("")
    add("The same strategy objects run in the live paper-trading loop (`realtime.live`, `euroflex live run`):")
    add("")
    add("- `refresh(ctx)` re-plans before every live decision on the window that fills in place: the forecast MPC and")
    add(
        "  SCALES-L recompute only the forecasts and value-to-go of the current quarter-hour from the data visible now;"
    )
    add("  the RL policy rebuilds its observations but keeps the energy already delivered in the quarter-hour.")
    add("  `tests/test_rt_strategy_live_refresh.py` replays a window minute by minute and checks that every live")
    add("  decision equals the backtest decision.")
    add("- `LIVE_STRATEGIES` factories: `forecast-mpc` (current-formula model), `prism` / `scales-l` (current operator")
    add("  before the switch, zero-history new operator fused with the live running new price after it; `auto` follows")
    add("  the live settlement rule), `rl-dqn` (numpy policy, committed as `reports/ledgers/realtime_rl_policy.npz`).")
    manifest_path = s.ledgers / "realtime_deploy_manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        fold = manifest.get("fold", {})
        add(
            f"- Deployment fold (`--stages deploy`): trained {fold.get('train_start', '?')[:16]} to"
            f" {fold.get('train_end', '?')[:16]}, valid for decisions from {fold.get('test_start', '?')[:16]} to"
            f" {fold.get('test_end', '?')[:10]}; models are written to `$EUROFLEX_MODELS_DIR` (default"
            " `~/.cache/euroflex_bess_lab/realtime_backtest/models`)."
        )
    add("")

    # ---------------------------------------------------------------- limitations
    add("## Limitations")
    add("")
    add("- **Price taker.** A single 1 MW battery does not move the Belgian imbalance price; a fleet does. Own-impact,")
    add("  herding and saturation are handled in `reports/impact*.md` (separate agent); numbers here are per-MW values")
    add("  of an infinitesimal participant.")
    add("- **Real-time data quality.** Decisions use the non-validated real-time feeds; settlement uses validated")
    add("  quarter-hour prices. The gap (table above) is real risk and is included in every result.")
    add("- **Counterfactual new-formula prices** (reconstruction, zero-history training targets) ignore how BRPs would")
    add(
        "  have behaved under the new price, and the incentive component is not reconstructed. Elia's ODS212 is itself a"
    )
    add("  simulation of the new design on historical days (participants were reacting to the current formula).")
    add("- **Execution.** Minute-level power changes are assumed feasible and fully delivered; no nomination/aFRR")
    add("  interaction, no availability outages, no intraday market, no imbalance from other portfolio assets.")
    add(
        "- **RL** is a single, wall-clock-capped run with one seed; results are indicative of the approach, not its limit."
    )
    add("- **Development looks** at January 2026 and at the 6 April 2026 floor event (see Methods); no parameter was")
    add("  optimised on a reported window.")
    add("")

    # ---------------------------------------------------------------- references
    add("## References")
    add("")
    add("- Elia Open Data, imbalance prices per minute (ODS133) and per quarter-hour (ODS134), system imbalance per")
    add("  minute (ODS126), SI forecasts (ODS136/ODS147): https://opendata.elia.be/explore/dataset/ods133/ (and")
    add("  `/ods134/`, `/ods126/`, `/ods136/`, `/ods147/`).")
    add("- Elia simulation of the new imbalance price formula (to be removed on 17 Nov 2026, go-live planned 24 Nov")
    add(
        "  2026): https://opendata.elia.be/explore/dataset/ods211/ (minute) and https://opendata.elia.be/explore/dataset/ods212/"
    )
    add("  (quarter-hour).")
    add("- RL literature on Belgian imbalance arbitrage that the DQN baseline follows in spirit: S. S. Karimi Madahi,")
    add(
        "  B. Claessens, C. Develder, Distributional reinforcement learning-based energy arbitrage strategies in imbalance"
    )
    add(
        "  settlement mechanism, https://arxiv.org/abs/2401.00015 (no result of that paper is reproduced or compared here)."
    )
    add(
        "- Mechanism-level natural experiment (oracle values under both formulas): `reports/mechanism_natural_experiment.md`."
    )
    add("")

    # ---------------------------------------------------------------- reproduce
    add("## Reproduce")
    add("")
    add("```bash")
    add("python scripts/research/realtime_backtest.py            # all stages (about 1-2 h on 11 cores)")
    add("python scripts/research/realtime_backtest.py --stages report   # rebuild this report from cached runs")
    add("```")
    add("")
    add("Outputs: this report, `reports/figures/realtime_*.png`, quarter-hour ledgers")
    add("`reports/ledgers/realtime_<strategy>_<mechanism>_<period>.parquet` (+ JSON sidecars; `_1mwh` for the 1 MWh")
    add("battery), the look-ahead audit `reports/ledgers/realtime_lookahead_audit.json` and the data checks")
    add("`reports/ledgers/realtime_data_semantics.json`.")
    if train_log:
        seconds = sum(float(x.get("seconds", 0.0)) for x in train_log)
        add(f"Forecaster training in this run: {len(train_log)} fold fits, {seconds / 60:.0f} CPU-process minutes.")
    add(f"Ledgers written: {len(written_ledgers)}.")
    add("")
    out_md.write_text("\n".join(lines), encoding="utf-8")
    return out_md
