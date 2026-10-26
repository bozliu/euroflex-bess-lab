"""Figures and Markdown report of the impact study (called by ``impact_study.py``; reads its saved results).

Every number in ``reports/impact.md`` is read from the files written by ``scripts/research/impact_study.py``
under ``reports/ledgers/impact_*``; nothing is typed in by hand.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

# reference palette (dataviz skill, light mode): categorical slots in fixed order + text / surface tokens
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
BLUE_RAMP = ["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#104281"]
SURFACE = "#fcfcfb"
TEXT = "#0b0b0b"
TEXT_2 = "#52514e"
GRID = "#e4e3df"

POLICY_LABEL = {
    "price_taking_threshold": "Price-taking threshold (herding)",
    "impact_aware_threshold": "Impact-aware threshold",
    "herding": "Herding (heterogeneous price takers)",
    "herding_homogeneous": "Herding (identical price takers)",
    "naive": "Naive impact-aware (own impact only)",
    "nash": "Symmetric Nash equilibrium",
    "central": "Central optimiser (joint profit)",
}
MECH_LABEL = {"current": "current formula", "new2026": "2026 formula"}


def _style() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": SURFACE,
            "axes.facecolor": SURFACE,
            "savefig.facecolor": SURFACE,
            "axes.edgecolor": GRID,
            "axes.labelcolor": TEXT_2,
            "axes.titlecolor": TEXT,
            "axes.titlesize": 11,
            "axes.labelsize": 9.5,
            "axes.grid": True,
            "axes.axisbelow": True,
            "grid.color": GRID,
            "grid.linewidth": 0.8,
            "grid.linestyle": "-",
            "xtick.color": TEXT_2,
            "ytick.color": TEXT_2,
            "xtick.labelsize": 8.5,
            "ytick.labelsize": 8.5,
            "legend.frameon": False,
            "legend.fontsize": 8.5,
            "legend.labelcolor": TEXT,
            "lines.linewidth": 2.0,
            "lines.solid_capstyle": "round",
            "lines.solid_joinstyle": "round",
            "font.family": "DejaVu Sans",
            "axes.spines.top": False,
            "axes.spines.right": False,
        }
    )


def _line(ax: Any, x: Any, y: Any, color: str, label: str, marker: str = "o") -> None:
    ax.plot(
        x,
        y,
        color=color,
        label=label,
        marker=marker,
        markersize=6,
        markeredgecolor=SURFACE,
        markeredgewidth=1.5,
    )


def _log_x(ax: Any, values: Any) -> None:
    """Log x-axis with plain tick labels at the plotted values (no 10^x labels, no minor labels)."""
    from matplotlib.ticker import FixedLocator, FuncFormatter, NullFormatter

    ticks = sorted({float(v) for v in values})
    ax.set_xscale("log")
    ax.xaxis.set_major_locator(FixedLocator(ticks))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _pos: f"{v:,.0f}"))
    ax.xaxis.set_minor_formatter(NullFormatter())
    for label in ax.get_xticklabels():
        label.set_rotation(45)


def _k(value: float | None, digits: int = 1) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "n/a"
    return f"{value / 1000.0:,.{digits}f}k"


def _f(value: float | None, digits: int = 1) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "n/a"
    return f"{value:,.{digits}f}"


def _pct(value: float | None, digits: int = 0) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "n/a"
    return f"{100.0 * value:.{digits}f} %"


def _table(frame: pd.DataFrame) -> str:
    cols = list(frame.columns)
    lines = ["| " + " | ".join(str(c) for c in cols) + " |", "| " + " | ".join("---" for _ in cols) + " |"]
    for _, row in frame.iterrows():
        lines.append("| " + " | ".join(str(row[c]) for c in cols) + " |")
    return "\n".join(lines)


# --------------------------------------------------------------------------------------------------------------
# figures
# --------------------------------------------------------------------------------------------------------------


def fig_response_curves(validation: dict[str, Any], path: Path) -> None:
    halves = validation.get("half_years", [])
    if not halves:
        return
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8))
    ramp = BLUE_RAMP[-len(halves) :] if len(halves) <= len(BLUE_RAMP) else BLUE_RAMP
    for ax, key, title in (
        (axes[0], "mip_curve", "MIP response to upward activation"),
        (axes[1], "mdp_curve", "MDP response to downward activation"),
    ):
        for i, half in enumerate(halves):
            curve = half[key]
            knots = np.asarray(curve["knots_mw"], dtype=float)
            values = np.asarray(curve["values_eur_mwh"], dtype=float)
            x = np.linspace(0.0, 450.0, 200)
            y = np.interp(x, knots, values)
            beyond = x > knots[-1]
            y[beyond] = values[-1] + curve["tail_slope_eur_mwh_per_mw"] * (x[beyond] - knots[-1])
            ax.plot(x, y, color=ramp[i % len(ramp)], label=f"fitted on {half['period']}")
        ax.set_title(title, loc="left")
        ax.set_xlabel("activated volume (aFRR + mFRR, QH average), MW")
        ax.set_ylabel("price change vs no activation, EUR/MWh")
        ax.axhline(0.0, color=GRID, linewidth=1.0)
    axes[0].legend(loc="upper left")
    fig.suptitle(
        "Belgian marginal prices respond less and less to the activated volume (first-difference fits)",
        x=0.01,
        ha="left",
        color=TEXT,
        fontsize=11,
    )
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def fig_alpha(validation: dict[str, Any], path: Path) -> None:
    alpha = validation.get("alpha")
    if not alpha:
        return
    tariff = alpha.get("tariff_only_exact_share_by_month", {})
    exact = alpha.get("exact_rule_share_by_month", {})
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.6))
    x = np.linspace(0.0, 1000.0, 400)
    for i, c in enumerate((1.0, 0.5)):
        axes[0].plot(
            x, c * 200.0 / (1.0 + np.exp((450.0 - x) / 65.0)), color=SERIES[i], label=f"price factor c = {c:g}"
        )
    axes[0].set_title("alpha = 200 / (1 + exp((450 - x) / 65)) * c", loc="left")
    axes[0].set_xlabel("x = (|SI_k| + |SI_k-1|) / 2, MW (alpha = 0 when |SI_k| <= 150 MW)")
    axes[0].set_ylabel("alpha, EUR/MWh")
    axes[0].legend(loc="upper left")
    keys = sorted(set(tariff) | set(exact))
    if keys:
        xs = np.arange(len(keys))
        width = 0.38
        axes[1].bar(
            xs - width / 2 - 0.01,
            [tariff.get(k, np.nan) for k in keys],
            width=width,
            color=SERIES[0],
            label="tariff curve only (|err| < 0.05)",
        )
        axes[1].bar(
            xs + width / 2 + 0.01,
            [exact.get(k, np.nan) for k in keys],
            width=width,
            color=SERIES[1],
            label="with price factor c (|err| < 0.011)",
        )
        axes[1].set_xticks(xs[::3], keys[::3], rotation=45, ha="right")
        axes[1].set_ylim(0, 1.35)
        axes[1].set_yticks([0.0, 0.25, 0.5, 0.75, 1.0])
        axes[1].set_title("Published alpha reproduced (|SI| > 150 MW QHs)", loc="left")
        axes[1].set_ylabel("share of quarter-hours")
        axes[1].legend(loc="upper left", ncol=2, fontsize=8)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def fig_validation(validation: dict[str, Any], path: Path) -> None:
    rows = []
    for mech in ("current", "new2026"):
        folds = validation.get(mech, [])
        if not folds:
            continue
        fold = folds[-1] if mech == "current" else folds[0]
        for subset in ("all", "sign_flip", "same_sign"):
            vs = fold["si_shift"][subset]["vs_persistence"]
            flat = fold["si_shift"][subset]["flat_curves_vs_persistence"]
            rows.append((mech, subset, vs["baseline_mae"], flat["mae"], vs["mae"], fold["fold"]))
    if not rows:
        return
    fig, axes = plt.subplots(1, len({r[0] for r in rows}), figsize=(10, 3.6), squeeze=False)
    for ax, mech in zip(axes[0], [m for m in ("current", "new2026") if any(r[0] == m for r in rows)], strict=True):
        sub = [r for r in rows if r[0] == mech]
        x = np.arange(len(sub))
        width = 0.24
        labels = ("persistence", "operator, flat curves", "operator, fitted curves")
        for j, label in enumerate(labels):
            ax.bar(x + (j - 1) * (width + 0.02), [r[2 + j] for r in sub], width=width, color=SERIES[j], label=label)
        ax.set_xticks(x, [r[1].replace("_", " ") for r in sub])
        ax.set_title(f"{MECH_LABEL[mech]}: hold-out {sub[0][5]}", loc="left")
        ax.set_ylabel("MAE of next-QH price, EUR/MWh")
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3, frameon=False, fontsize=8.5)
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    fig.savefig(path, dpi=150)
    plt.close(fig)


def _curve_panels(curve: pd.DataFrame) -> list[tuple[str, str]]:
    panels = []
    for window in ("2026H1", "2025"):
        for mech in ("current", "new2026"):
            if ((curve["window"] == window) & (curve["mechanism"] == mech)).any():
                panels.append((window, mech))
    return panels


def fig_capacity(curve: pd.DataFrame, path: Path, metric: str, ylabel: str, title: str) -> None:
    panels = _curve_panels(curve)
    if not panels:
        return
    fig, axes = plt.subplots(1, len(panels), figsize=(4.0 * len(panels), 3.8), squeeze=False, sharey=True)
    for ax, (window, mech) in zip(axes[0], panels, strict=True):
        sub = curve[(curve["window"] == window) & (curve["mechanism"] == mech)]
        for i, policy in enumerate(("price_taking_threshold", "impact_aware_threshold")):
            p = sub[sub["policy"] == policy].sort_values("fleet_mw")
            if len(p):
                scale = 1e-3 if metric == "eur_per_mw_year" else 1.0
                _line(ax, p["fleet_mw"], p[metric] * scale, SERIES[i], POLICY_LABEL[policy])
        _log_x(ax, sub["fleet_mw"])
        ax.set_title(f"{window}, {MECH_LABEL[mech]}", loc="left")
        ax.set_xlabel("fleet size Q, MW (log scale)")
        ax.axhline(0.0, color=TEXT_2, linewidth=0.8)
    axes[0][0].set_ylabel(ylabel)
    axes[0][0].legend(loc="lower left")
    fig.suptitle(title, x=0.01, ha="left", color=TEXT, fontsize=11)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def fig_equilibrium(eq: pd.DataFrame, path: Path) -> None:
    if eq.empty:
        return
    mechs = [m for m in ("current", "new2026") if (eq["mechanism"] == m).any()]
    metrics = (
        ("eur_per_mw_year", "EUR/MW/year (thousands)", 1e-3),
        ("overshoot_share", "overshoot share of active QHs", 1.0),
        ("residual_imbalance_change_pct", "residual |SI| change, %", 1.0),
        ("activation_cost_change_pct", "activation-cost proxy, % change", 1.0),
    )
    fig, axes = plt.subplots(len(mechs), len(metrics), figsize=(14, 3.8 * len(mechs)), squeeze=False)
    policies = [p for p in ("herding", "herding_homogeneous", "naive", "nash", "central") if (eq["policy"] == p).any()]
    for r, mech in enumerate(mechs):
        for c, (metric, label, scale) in enumerate(metrics):
            ax = axes[r][c]
            for i, policy in enumerate(policies):
                p = eq[(eq["mechanism"] == mech) & (eq["policy"] == policy)].sort_values("total_mw")
                if metric in p:
                    _line(ax, p["total_mw"], p[metric] * scale, SERIES[i], POLICY_LABEL[policy])
            _log_x(ax, eq["total_mw"])
            ax.axhline(0.0, color=TEXT_2, linewidth=0.8)
            ax.set_title(f"{MECH_LABEL[mech]}\n{label}", loc="left", fontsize=9.5)
            if r == len(mechs) - 1:
                ax.set_xlabel("total implicit capacity, MW (4 fleets, log scale)")
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=len(policies), frameon=False, fontsize=8.5)
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    fig.savefig(path, dpi=150)
    plt.close(fig)


# --------------------------------------------------------------------------------------------------------------
# report
# --------------------------------------------------------------------------------------------------------------


def _value_at(
    curve: pd.DataFrame, window: str, mech: str, policy: str, q: float, col: str = "eur_per_mw_year"
) -> float:
    sub = curve[
        (curve["window"] == window)
        & (curve["mechanism"] == mech)
        & (curve["policy"] == policy)
        & np.isclose(curve["fleet_mw"], q)
    ]
    return float(sub[col].iloc[0]) if len(sub) else float("nan")


def _eq_at(eq: pd.DataFrame, mech: str, policy: str, q: float, col: str) -> float:
    if eq.empty:
        return float("nan")
    sub = eq[(eq["mechanism"] == mech) & (eq["policy"] == policy) & np.isclose(eq["total_mw"], q)]
    return float(sub[col].iloc[0]) if len(sub) else float("nan")


def _breakeven(curve: pd.DataFrame, window: str, mech: str, policy: str) -> str:
    sub = curve[(curve["window"] == window) & (curve["mechanism"] == mech) & (curve["policy"] == policy)]
    sub = sub.sort_values("fleet_mw")
    neg = sub[sub["eur_per_mw_year"] <= 0.0]
    if neg.empty:
        return f"> {sub['fleet_mw'].max():,.0f} MW"
    first = neg.iloc[0]
    prev = sub[sub["fleet_mw"] < first["fleet_mw"]]
    if prev.empty:
        return f"<= {first['fleet_mw']:,.0f} MW"
    return f"{prev['fleet_mw'].iloc[-1]:,.0f}-{first['fleet_mw']:,.0f} MW"


def _best_total(curve: pd.DataFrame, window: str, mech: str, policy: str) -> tuple[float, float]:
    sub = curve[(curve["window"] == window) & (curve["mechanism"] == mech) & (curve["policy"] == policy)]
    if sub.empty:
        return float("nan"), float("nan")
    total = sub["eur_per_mw_year"] * sub["fleet_mw"]
    i = int(np.argmax(total.to_numpy()))
    return float(sub["fleet_mw"].iloc[i]), float(total.iloc[i])


def render(out: Path, *, quick: bool = False) -> Path:
    _style()
    ledgers = out / "ledgers"
    figures = out / "figures"
    figures.mkdir(parents=True, exist_ok=True)
    validation = json.loads((ledgers / "impact_validation.json").read_text(encoding="utf-8"))
    curve = pd.read_parquet(ledgers / "impact_capacity_value_curve.parquet")
    sens_path = ledgers / "impact_capacity_value_sensitivity.parquet"
    sens = pd.read_parquet(sens_path) if sens_path.exists() else pd.DataFrame()
    eq_path = ledgers / "impact_equilibrium.parquet"
    eq = pd.read_parquet(eq_path) if eq_path.exists() else pd.DataFrame()
    ch_path = ledgers / "impact_channels.parquet"
    channels = pd.read_parquet(ch_path) if ch_path.exists() else pd.DataFrame()
    fig_response_curves(validation, figures / "impact_response_curves.png")
    fig_alpha(validation, figures / "impact_alpha.png")
    fig_validation(validation, figures / "impact_validation.png")
    fig_capacity(
        curve,
        figures / "impact_capacity_value_curve.png",
        "eur_per_mw_year",
        "net value, kEUR per MW per year",
        "Per-MW value of a fleet that follows one implicit-balancing policy",
    )
    fig_capacity(
        curve,
        figures / "impact_overshoot.png",
        "overshoot_share",
        "overshoot share of active QHs",
        "How often the fleet's own deviation flips the sign of the system imbalance",
    )
    fig_equilibrium(eq, figures / "impact_equilibrium.png")
    text = report_markdown(validation, curve, sens, eq, quick=quick, channels=channels)
    path = out / "impact.md"
    path.write_text(text, encoding="utf-8")
    return path


def _fold_row(fold: dict[str, Any], mech: str) -> list[str]:
    s = fold["si_shift"]
    a, f = s["all"]["vs_persistence"], s["all"]["flat_curves_vs_persistence"]
    fl, ffl = s["sign_flip"]["vs_persistence"], s["sign_flip"]["flat_curves_vs_persistence"]
    if mech == "current":
        c1, c2 = fold["curve_mip"], fold["curve_mdp"]
    else:
        c1, c2 = fold["curve_afrr_up"], fold["curve_afrr_down"]
    return [
        fold["fold"],
        _f(fold["pass_through"], 2),
        f"{_f(c1['calibration_slope'], 2)} / {_f(c1['r2_vs_baseline'], 3)}",
        f"{_f(c2['calibration_slope'], 2)} / {_f(c2['r2_vs_baseline'], 3)}",
        f"{_f(a['baseline_mae'])} / {_f(f['mae'])} / {_f(a['mae'])}",
        f"{_f(fl['baseline_mae'])} / {_f(ffl['mae'])} / {_f(fl['mae'])}",
        _pct(s["sign_flip_share"]),
    ]


def _channels_section(channels: pd.DataFrame) -> str:
    """Where a price-taking fleet's value goes: revenue effect of each impact channel."""
    if channels.empty:
        return ""
    names = ["merit", "alpha_own", "alpha_carry", "weight", "branch", "total"]
    rows = []
    for r in channels.itertuples():
        row: dict[str, str] = {
            "window": r.window,
            "mechanism": r.mechanism,
            "displacement": r.displacement,
            "Q MW": f"{r.fleet_mw:,.0f}",
        }
        for name in names:
            col = f"{name}_eur_per_mw_year"
            if hasattr(r, col):
                value = getattr(r, col)
                row[f"{name} kEUR/MW/yr"] = _k(value).rstrip("k") if math.isfinite(value) else "-"
        rows.append(row)
    lines = [
        "### Where the value goes: impact channels",
        "",
        "Revenue effect of each own-price channel for the price-taking fleet (sequential split, sums to the total "
        "impact; negative = value lost to the fleet's own impact): `merit` = marginal / product prices moving along "
        "the merit order; `alpha_own` = alpha of the same quarter-hour (imbalance size and price factor); "
        "`alpha_carry` = alpha of the next quarter-hour through `x` (current rule only); `weight` = aFRR / mFRR "
        "re-weighting (2026 rule; non-zero only with the `afrr_first` displacement, where aFRR absorbs the deviation "
        "while scheduled mFRR stays); `branch` = neutral band / sign flip (overshoot).",
        "",
        _table(pd.DataFrame(rows)),
        "",
    ]
    cur = channels[(channels["mechanism"] == "current") & np.isclose(channels["fleet_mw"], 400.0)]
    if len(cur):
        r = cur.iloc[0]
        parts = {
            "merit": r["merit_eur_per_mw_year"],
            "alpha (own + next QH)": r["alpha_own_eur_per_mw_year"] + r["alpha_carry_eur_per_mw_year"],
            "branch / overshoot": r["branch_eur_per_mw_year"],
        }
        worst = min(parts, key=lambda k: parts[k])
        lines.append(
            f"At 400 MW under the current rule ({r['window']}) the largest loss comes from the {worst} channel "
            f"({_k(parts[worst])} EUR/MW/yr of a total {_k(r['total_eur_per_mw_year'])})."
        )
        lines.append("")
    return "\n".join(lines)


def report_markdown(
    validation: dict[str, Any],
    curve: pd.DataFrame,
    sens: pd.DataFrame,
    eq: pd.DataFrame,
    *,
    quick: bool,
    channels: pd.DataFrame | None = None,
) -> str:
    w26 = "2026H1"
    lines: list[str] = []
    add = lines.append
    windows = curve.groupby(["window", "mechanism"])[["start_utc", "end_utc"]].first().reset_index()
    add("# Impact and herding: the price response to a fleet's own deviation (Belgium, both formulas)")
    add("")
    if quick:
        add("> **Smoke run (`--quick`)**: two-week windows; numbers are not representative.")
        add("")
    add(
        "Generated by `scripts/research/impact_study.py` from `data/public` (Elia Open Data, CC BY 4.0, "
        "attribution Elia Transmission Belgium SA). Every number below is read from `reports/ledgers/impact_*`."
    )
    add("")
    add(
        "**Setup.** Battery unit 1 MW / 2 MWh, 95 % / 95 % one-way efficiency, SOC 5-95 %, degradation 8 EUR/MWh "
        "discharged; publication lags of the real-time core (minute rows usable one minute after they end, final "
        "quarter-hour prices five minutes after the quarter-hour); no transaction costs beyond degradation. A fleet "
        "of Q MW is Q identical units following the same policy. Mechanisms: `current` (Elia marginal price +/- alpha, "
        "ODS134) and `new2026` (volume-weighted aFRR/mFRR price, ODS212 simulation). Windows: "
        + "; ".join(
            f"{r.window} {r.mechanism} {str(r.start_utc)[:10]}..{str(r.end_utc)[:10]}" for r in windows.itertuples()
        )
        + "."
    )
    add("")
    # ---------------- key results
    add("## Key results")
    add("")
    pt, ia = "price_taking_threshold", "impact_aware_threshold"
    for mech in ("current", "new2026"):
        v1 = _value_at(curve, w26, mech, pt, 1.0)
        v100 = _value_at(curve, w26, mech, pt, 100.0)
        v400 = _value_at(curve, w26, mech, pt, 400.0)
        v1600 = _value_at(curve, w26, mech, pt, 1600.0)
        a400 = _value_at(curve, w26, mech, ia, 400.0)
        a1600 = _value_at(curve, w26, mech, ia, 1600.0)
        o1600 = _value_at(curve, w26, mech, pt, 1600.0, "overshoot_share")
        add(
            f"- **{MECH_LABEL[mech]}, Jan-Jul 2026**: a price-taking threshold fleet earns {_k(v1)} EUR/MW/yr at "
            f"1 MW, {_k(v100)} at 100 MW, {_k(v400)} at 400 MW and {_k(v1600)} at 1,600 MW (it flips the sign of SI "
            f"in {_pct(o1600)} of its active quarter-hours at 1,600 MW; value turns negative at "
            f"{_breakeven(curve, w26, mech, pt)}). Internalising its own impact keeps {_k(a400)} at 400 MW and "
            f"{_k(a1600)} at 1,600 MW."
        )
    if "2025" in set(curve["window"]):
        v1 = _value_at(curve, "2025", "current", pt, 1.0)
        v100 = _value_at(curve, "2025", "current", pt, 100.0)
        v400 = _value_at(curve, "2025", "current", pt, 400.0)
        a400 = _value_at(curve, "2025", "current", ia, 400.0)
        add(
            f"- **current formula, 2025**: {_k(v1)} EUR/MW/yr at 1 MW and {_k(v100)} at 100 MW for the price taker "
            f"(negative at {_breakeven(curve, '2025', 'current', pt)}); at 400 MW the price taker earns {_k(v400)} and "
            f"the impact-aware policy {_k(a400)}."
        )
    q_c, v_c = _best_total(curve, w26, "current", pt)
    q_n, v_n = _best_total(curve, w26, "new2026", pt)
    if math.isfinite(v_c) and math.isfinite(v_n):
        add(
            f"- **Saturation**: the total value of a price-taking fleet peaks at about {q_c:,.0f} MW under the current "
            f"formula ({_f(v_c / 1e6, 1)} MEUR/yr) and {q_n:,.0f} MW under the 2026 formula ({_f(v_n / 1e6, 1)} MEUR/yr); "
            "beyond that every extra reacting MW lowers the fleet's total. The impact-aware policy keeps the total "
            "growing but each extra MW adds little (marginal values in the implications section)."
        )
    sens_rows = sens[(sens["policy"] == pt) & np.isclose(sens["fleet_mw"], 400.0)] if not sens.empty else sens
    if not sens.empty and len(sens_rows):
        flat = sens_rows[
            sens_rows["environment"].isin(["branch_alpha_only", "branch_only"]) & (sens_rows["window"] == w26)
        ]
        parts = [f"{MECH_LABEL[r.mechanism]} {_k(r.eur_per_mw_year)}" for r in flat.itertuples()]
        if parts:
            add(
                "- **Robust to the weak volume curves**: with the volume response switched off (branch switch and alpha "
                "only) the 400 MW price taker earns " + ", ".join(parts) + " EUR/MW/yr on Jan-Jul 2026 - the "
                "saturation comes mainly from the sign flip and alpha, not from the fitted curves."
            )
    h_c = _value_at(curve, w26, "current", pt, 1.0, "harmful_share")
    h_n = _value_at(curve, w26, "new2026", pt, 1.0, "harmful_share")
    if math.isfinite(h_c) and math.isfinite(h_n):
        add(
            f"- **Signal quality**: even a 1 MW price taker pushes |SI| up in {_pct(h_c)} of its active quarter-hours "
            f"under the current formula but in {_pct(h_n)} under the 2026 formula."
        )
    gaps = validation.get("branch_gaps", {})
    if gaps:
        add(
            "- **Why the formulas differ**: the gap between the short-branch and the long-branch price of the same "
            f"quarter-hour (what an overshooting fleet loses when it flips the sign of SI) has a median of "
            f"{_f(gaps['current_gap_quantiles']['0.5'])} EUR/MWh under the current formula (never negative: "
            f"{_pct(gaps['current_gap_nonpositive_share'], 1)} of QHs <= 0) but only "
            f"{_f(gaps['new_gap_quantiles']['0.5'])} EUR/MWh under the 2026 formula, where it is <= 0 in "
            f"{_pct(gaps['new_gap_nonpositive_share'])} of quarter-hours. The 2026 formula also drops alpha. "
            "Overshooting is therefore much cheaper for the fleet under the new formula."
        )
        align = gaps.get("signal_alignment", {})
        if {"current", "new2026"} <= set(align):
            c, n = align["current"], align["new2026"]
            add(
                "- **Signal alignment**: when the final price is above a band trader's upper band (discharge signal) "
                f"the system was short in {_pct(c['short_given_discharge_signal'])} of quarter-hours under the current "
                f"formula but in {_pct(n['short_given_discharge_signal'])} under the 2026 formula; below the lower band "
                f"(charge signal) it was long in {_pct(c['long_given_charge_signal'])} vs "
                f"{_pct(n['long_given_charge_signal'])}. A price-following fleet therefore helps the system less often "
                "under the new formula, even before any impact."
            )
    if channels is not None and not channels.empty:
        cur = channels[
            (channels["mechanism"] == "current") & np.isclose(channels["fleet_mw"], 400.0) & (channels["window"] == w26)
        ]
        if len(cur):
            r = cur.iloc[0]
            add(
                "- **Where the value goes** (400 MW price taker, current formula, Jan-Jul 2026): of the "
                f"{_k(r['total_eur_per_mw_year'])} EUR/MW/yr lost to the fleet's own impact, the sign flip / neutral "
                f"band takes {_k(r['branch_eur_per_mw_year'])}, alpha {_k(r['alpha_own_eur_per_mw_year'])} (plus "
                f"{_k(r['alpha_carry_eur_per_mw_year'])} through the next quarter-hour's alpha) and the merit order only "
                f"{_k(r['merit_eur_per_mw_year'])}."
            )
    if not eq.empty:
        for mech in ("current", "new2026"):
            q = 1600.0
            h = _eq_at(eq, mech, "herding", q, "eur_per_mw_year")
            n = _eq_at(eq, mech, "nash", q, "eur_per_mw_year")
            c = _eq_at(eq, mech, "central", q, "eur_per_mw_year")
            hr = _eq_at(eq, mech, "herding", q, "residual_imbalance_change_pct")
            nr = _eq_at(eq, mech, "nash", q, "residual_imbalance_change_pct")
            ho = _eq_at(eq, mech, "herding", q, "overshoot_share")
            no = _eq_at(eq, mech, "nash", q, "overshoot_share")
            add(
                f"- **Four fleets, 1,600 MW of implicit capacity, {MECH_LABEL[mech]}**: herding earns {_k(h)} EUR/MW/yr "
                f"(overshoot {_pct(ho)} of active QHs, residual |SI| {_f(hr)} %), the symmetric Nash equilibrium "
                f"{_k(n)} (overshoot {_pct(no)}, residual |SI| {_f(nr)} %), a central optimiser {_k(c)}."
            )
    add("")
    # ---------------- method
    add("## Method: the impact layer of PRISM")
    add("")
    add(
        "The imbalance price is `pi_k = M(X_k)` (mechanism operator applied to the quarter-hour state). A fleet that "
        "deviates by `q` MW on average over quarter-hour k (+ = long) changes the state to `X_k(q)`; the impact "
        "operator returns `pi_k(q) = M(X_k(q))`. Implemented in `euroflex_bess_lab.realtime.impact` as "
        "`ImpactModel` classes (`CurrentFormulaImpact`, `NewFormula2026Impact`) built from a `MarketData` period and "
        "plugged into `simulate(..., impact=..., fleet_mw=...)`; `adjust` returns `base + pi(q) - pi(0)`, so zero "
        "deviation leaves every published price unchanged."
    )
    add("")
    add(
        "1. **System imbalance and activations.** `SI' = SI + q`. Elia's aFRR + mFRR activation offsets a share "
        "`kappa` of the change (pass-through, estimated per period as the slope of the change in net aFRR + mFRR "
        "activation on the change in SI between consecutive quarter-hours; the rest is absorbed by imbalance "
        "netting / IGCC and the ACE). A long deviation first removes gross upward activation and only then adds "
        "downward activation (`U' = max(U - kappa q, 0)`, `D' = D + max(kappa q - U, 0)`), and symmetrically."
    )
    add(
        "2. **Prices along volume-response curves.** Monotone piecewise-linear curves of MIP versus gross upward "
        "activation and MDP versus gross downward activation (current formula), and of the aFRR / mFRR up and down "
        "product prices versus their volumes (2026 formula), estimated with first differences between consecutive "
        "quarter-hours of the same delivery hour (the hour fixed effect absorbs the day-ahead level and the bid "
        "stack of the hour), robust to price spikes (clipped prices, winsorised residuals), one curve per local-hour "
        "block (00-06, 06-10, 10-17, 17-22, 22-24) with a pooled fallback. Prices move incrementally: "
        "`MIP' = MIP + f(U') - f(U)`."
    )
    add(
        "3. **Elia's current rule, exactly.** `alpha = 1{|SI| > 150 MW} * 200 / (1 + exp((450 - x) / 65)) * c` with "
        "**x = (|SI_k| + |SI_k-1|) / 2** (this and the previous quarter-hour: why alpha is not a function of |SI| "
        "alone) and the price factor `c = clip((400 - MIP) / 200, 0, 1)` if short, `clip((MDP + 200) / 200, 0, 1)` if "
        "long; `MIP = max(aFRR, mFRR SA, mFRR DA up prices, FLOOR)`, `MDP = min(down prices, CAP)`; and if "
        "`|SI| < 25 MW` the price is the midpoint `(FLOOR + CAP) / 2` (structure from the paper agent, verified "
        "here on ODS134 - see validation). The counterfactual evaluates the rule at `SI_k + q_k`, `SI_k-1 + q_k-1` "
        "and MIP' / MDP' (bounded by FLOOR / CAP), so a deviation also prices the next quarter-hour through alpha."
    )
    add(
        "4. **Branch switch (overshoot).** The branch follows `SI'`: current midpoint if `|SI'| < 25 MW`, else "
        "`MIP' + alpha'` if `SI' < 0`, else `MDP' - alpha'`; new `main_up'` / `main_down'` through "
        "`NewWeightedMechanism2026.price_given_si` "
        "(incentive rule included). Within the original branch the published aFRR/mFRR weights are kept (reductions "
        "pro-rata, increases aFRR); after a flip the new-direction regulation is aFRR priced along the aFRR curve."
    )
    add(
        "5. **Guarantees (tested).** Non-increasing in `q` within a branch, exact at `q = 0`, branch switch exactly "
        "when `|q| > |SI|` with the opposite sign, operators satisfy the `ImpactModel` protocol."
    )
    add("")
    # ---------------- validation
    add("## Validation (hold-out, honest errors)")
    add("")
    alpha = validation.get("alpha", {})
    if alpha:
        cands = [c for c in alpha.get("candidates", [])][:6]
        add(
            f"**Elia's current rule** ({alpha['period'][0]}..{alpha['period'][1]}): the exact alpha rule (tariff "
            f"logistic x price factor c) reproduces the published alpha to 0.011 EUR/MWh in "
            f"{_pct(alpha.get('exact_rule_share'), 2)} of the quarter-hours with |SI| > 150 MW (the tariff curve "
            f"alone: {_pct(alpha.get('tariff_exact_share'))} to 0.05 EUR/MWh); alpha = 0 below 150 MW in "
            f"{_pct(alpha.get('zero_rule_share'), 1)} of cases; the whole rule (neutral band, structural MIP / MDP, "
            f"alpha) reproduces {_pct(alpha.get('price_rule_match_share'), 2)} of "
            f"{alpha.get('price_rule_compared', 0):,} published prices; |SI| < 25 MW in "
            f"{_pct(alpha.get('neutral_band_share'))} of quarter-hours. Predicting each hold-out quarter-hour's alpha "
            "(last 30 %) from the previous one:"
        )
        add("")
        tab = pd.DataFrame(
            [
                {
                    "driver x": c["feature"],
                    "model": c["kind"],
                    "mode": c.get("mode", "-"),
                    "change MAE EUR/MWh": _f(c["change_mae"], 3),
                    "level MAE": _f(c["level_mae"], 2),
                    "level exact share": _pct(c["level_exact_share"]) if c["level_exact_share"] is not None else "-",
                }
                for c in cands
            ]
        )
        add(_table(tab))
        add("")
    add(
        "**Volume-response curves and the full operator.** Columns: activation pass-through `kappa` of the training "
        "period; curve calibration slope / out-of-sample R^2 when predicting within-hour price changes from the "
        "realised activation changes (1 = calibrated; R^2 against a no-change baseline); MAE of the next quarter-hour's "
        "published price when the operator is fed the realised SI change from the previous quarter-hour's state "
        "(persistence / operator with flat curves / full operator), on all pairs and on pairs where SI changed sign."
    )
    add("")
    for mech, name in (("current", "current formula (MIP / MDP)"), ("new2026", "2026 formula (aFRR up / down)")):
        folds = validation.get(mech, [])
        if not folds:
            continue
        add(f"*{name}*")
        add("")
        rows = [_fold_row(f, mech) for f in folds]
        header = [
            "fold (train -> test)",
            "kappa",
            "curve 1 calib / R^2",
            "curve 2 calib / R^2",
            "price MAE all: persist / flat / full",
            "price MAE sign flips: persist / flat / full",
            "sign-flip share",
        ]
        add(_table(pd.DataFrame(rows, columns=header)))
        add("")
    bid_rows = [f for f in validation.get("current", []) if "bid_curve_mip" in f]
    if bid_rows:
        f = bid_rows[-1]
        add(
            f"*Local merit order (ODS156/157 bid curves, fold {f['fold']}).* The Belgian bid ladder at the activated "
            f"volume tracks MIP only loosely (corr {_f(f['bid_curve_level']['mip_corr'], 2)}, MAE "
            f"{_f(f['bid_curve_level']['mip_mae'])} EUR/MWh) and its local slope "
            f"({_f(f['bid_curve_level']['median_local_slope_up_eur_mwh_per_mw'], 2)} EUR/MWh per MW, median) "
            f"overstates the realised price response: calibration slope {_f(f['bid_curve_mip']['calibration_slope'], 2)} "
            f"(MIP) and {_f(f['bid_curve_mdp']['calibration_slope'], 2)} (MDP). The bid curves are therefore only a "
            "stress scenario; cross-border activation (PICASSO / MARI) makes the effective curve much flatter."
        )
        add("")
    halves = validation.get("half_years", [])
    if halves:
        add("*Market deepening.* Fitted price change at 100 / 200 / 400 MW of activation, by half-year:")
        add("")
        tab = pd.DataFrame(
            [
                {
                    "period": h["period"],
                    "kappa": _f(h["pass_through"], 2),
                    "MIP +100/+200/+400 MW": " / ".join(
                        _f(h["mip_curve_eur_mwh_at_mw"][k]) for k in ("100", "200", "400")
                    ),
                    "MDP +100/+200/+400 MW": " / ".join(
                        _f(h["mdp_curve_eur_mwh_at_mw"][k]) for k in ("100", "200", "400")
                    ),
                }
                for h in halves
            ]
        )
        add(_table(tab))
        add("")
    add(_validation_reading(validation))
    add("")
    add("![response curves](figures/impact_response_curves.png)")
    add("")
    add("![alpha](figures/impact_alpha.png)")
    add("")
    add("![validation](figures/impact_validation.png)")
    add("")
    # ---------------- capacity curve
    add("## Capacity-value curve")
    add("")
    add(
        "`reports/ledgers/impact_capacity_value_curve.parquet` (columns `mechanism, policy, fleet_mw, "
        "eur_per_mw_year, cycles_per_day, overshoot_share`, plus `window`, overshoot / harm shares, average prices, "
        "deployed share). Price taker: `ThresholdStrategy` (running QH price of the mechanism, bands = 0.25 / 0.75 "
        "quantiles of the last 14 days of published prices) replicated across the fleet; impact-aware: same signal and "
        "bands, chooses `q` in `[-Q, Q]` maximising `q (P(q) - band)` with `P(q)` from the impact operator at the "
        "running state. Signals are open loop (the fleet does not see its own effect inside the quarter-hour)."
    )
    add("")
    tab_rows = []
    for (window, mech), sub in curve.groupby(["window", "mechanism"], sort=False):
        for q in sorted(sub["fleet_mw"].unique()):
            row = {"window": window, "mechanism": mech, "Q MW": f"{q:,.0f}"}
            for policy, short in ((pt, "price taker"), (ia, "impact-aware")):
                p = sub[(sub["policy"] == policy) & np.isclose(sub["fleet_mw"], q)]
                if len(p):
                    r = p.iloc[0]
                    row[f"{short} kEUR/MW/yr"] = _k(r["eur_per_mw_year"]).rstrip("k")
                    row[f"{short} cycles/d"] = _f(r["cycles_per_day"], 2)
                    row[f"{short} overshoot"] = _pct(r["overshoot_share"])
            tab_rows.append(row)
    add(_table(pd.DataFrame(tab_rows)))
    add("")
    strat = curve[curve["policy"].astype(str).str.startswith("strat:")]
    if len(strat):
        add(
            "**Other agents' strategies** (Agent STRAT's published quarter-hour ledgers `reports/ledgers/realtime_*`, "
            "price takers, re-settled at the counterfactual price of a fleet of Q MW; `_1h` = 1 MW / 1 MWh battery). "
            "kEUR per MW per year:"
        )
        add("")
        srows = []
        for (policy, window, mech), part in strat.groupby(["policy", "window", "mechanism"], sort=True):
            row = {"strategy": str(policy).removeprefix("strat:"), "window": window, "mechanism": mech}
            for q in (1.0, 100.0, 400.0, 1600.0):
                v = part[np.isclose(part["fleet_mw"], q)]["eur_per_mw_year"]
                row[f"Q={q:,.0f}"] = _k(float(v.iloc[0])).rstrip("k") if len(v) else "-"
            srows.append(row)
        add(_table(pd.DataFrame(srows)))
        add("")
        sub = curve[(curve["window"] == w26) & (curve["mechanism"] == "current")]
        piv = sub.pivot_table(index="policy", columns="fleet_mw", values="eur_per_mw_year")
        if {1.0, 100.0} <= set(piv.columns) and pt in piv.index:
            keep = piv[100.0] / piv[1.0]
            strat_keep = keep[[i for i in keep.index if str(i).startswith("strat:")]]
            if len(strat_keep):
                add(
                    f"On Jan-Jul 2026 (current formula) the re-settled strategies keep {_pct(strat_keep.min())}-"
                    f"{_pct(strat_keep.max())} of their 1 MW per-MW value at 100 MW, against "
                    f"{_pct(keep[pt])} for the threshold rule and {_pct(keep.get(ia, float('nan')))} for the "
                    "impact-aware rule: a better price forecast does not protect a price taker from its own impact, so "
                    "any of these strategies needs an impact-aware sizing layer once the reacting fleet reaches a few "
                    "tens of MW."
                )
                add("")
    add("![capacity-value curve](figures/impact_capacity_value_curve.png)")
    add("")
    add("![overshoot](figures/impact_overshoot.png)")
    add("")
    if not sens.empty:
        add(
            "**Sensitivity.** Price taker re-settled under alternative impact environments (`branch_alpha_only` / "
            "`branch_only`: no volume response, only the branch switch and alpha; `local_bid_curves`: ODS156/157 merit "
            "order; `pass_through_1`: Elia activations offset the whole deviation). Impact-aware policy that believes "
            "half / double the true impact, or that uses the response fitted on the previous half-year (what an "
            "optimiser that only knows history would use):"
        )
        add("")
        srows = []
        for r in sens.itertuples():
            srows.append(
                {
                    "window": r.window,
                    "mechanism": r.mechanism,
                    "policy": r.policy,
                    "environment": r.environment,
                    "belief": getattr(r, "belief", "-"),
                    "Q MW": f"{r.fleet_mw:,.0f}",
                    "kEUR/MW/yr": _k(r.eur_per_mw_year).rstrip("k"),
                    "overshoot": _pct(r.overshoot_share),
                }
            )
        add(_table(pd.DataFrame(srows)))
        add("")
    if channels is not None and not channels.empty:
        add(_channels_section(channels))
    # ---------------- equilibrium
    if not eq.empty:
        add("## Multi-fleet herding versus equilibrium (Jan-Jul 2026, both formulas)")
        add("")
        add(
            "Four fleets of equal size react every minute to the same running price signal; the quarter-hour settles "
            "at the counterfactual price of their *total* deviation. Herding fleets are price takers whose signal is "
            "closed loop (Elia's running price already contains the fleets' own deviations so far). Equilibrium "
            "fleets best-respond to each other on the price-response function of the visible running state (bands as "
            "opportunity cost of energy). TSO metrics: change in residual imbalance energy sum |SI + q| and in an "
            "activation-cost proxy `0.25 (U' MIP' - D' MDP')` (same activation model for both formulas)."
        )
        add("")
        cols = {
            "total_mw": "total MW",
            "eur_per_mw_year": "kEUR/MW/yr",
            "cycles_per_day": "cycles/d",
            "overshoot_share": "overshoot",
            "harmful_share": "harmful",
            "residual_imbalance_change_pct": "resid |SI| %",
            "activation_cost_change_pct": "act. cost %",
        }
        for mech in ("current", "new2026"):
            sub = eq[eq["mechanism"] == mech]
            if sub.empty:
                continue
            add(f"*{MECH_LABEL[mech]}*")
            add("")
            eq_rows: list[dict[str, Any]] = []
            for r in sub.sort_values(["policy", "total_mw"]).itertuples():
                eq_rows.append(
                    {
                        "policy": r.policy,
                        cols["total_mw"]: f"{r.total_mw:,.0f}",
                        cols["eur_per_mw_year"]: _k(r.eur_per_mw_year).rstrip("k"),
                        cols["cycles_per_day"]: _f(r.cycles_per_day, 2),
                        cols["overshoot_share"]: _pct(r.overshoot_share),
                        cols["harmful_share"]: _pct(r.harmful_share),
                        cols["residual_imbalance_change_pct"]: _f(r.residual_imbalance_change_pct),
                        cols["activation_cost_change_pct"]: _f(getattr(r, "activation_cost_change_pct", float("nan"))),
                    }
                )
            add(_table(pd.DataFrame(eq_rows)))
            add("")
        add("![equilibrium](figures/impact_equilibrium.png)")
        add("")
    add(_implications(curve, eq))
    add("")
    add(_limitations())
    add("")
    add("## Files")
    add("")
    add("- Code: `src/euroflex_bess_lab/realtime/impact/` (operators, validation, policies, fleet curve, equilibrium).")
    add("- Script: `scripts/research/impact_study.py` (compute) and `scripts/research/impact_report.py` (this report).")
    add(
        "- Results: `reports/ledgers/impact_capacity_value_curve.parquet` (+ `.json`), `impact_qh_ledgers_<window>.parquet`, "
        "`impact_capacity_value_sensitivity.parquet`, `impact_channels.parquet`, `impact_equilibrium.parquet` (+ `.json`), "
        "`impact_validation.json`, "
        "`impact_response_fits.json`; figures `reports/figures/impact_*.png`; method notes `docs/impact_and_herding.md`."
    )
    add("")
    return "\n".join(lines)


def _validation_reading(validation: dict[str, Any]) -> str:
    """Plain-language summary of the hold-out results (numbers from the validation file)."""
    parts = ["**Reading.**"]
    for mech, curve_keys, names in (
        ("current", ("curve_mip", "curve_mdp"), ("MIP", "MDP")),
        ("new2026", ("curve_afrr_up", "curve_afrr_down"), ("aFRR-up price", "aFRR-down price")),
    ):
        folds = validation.get(mech, [])
        if not folds:
            continue
        fold = folds[-1] if mech == "current" else folds[0]
        s = fold["si_shift"]
        flip, allp = s["sign_flip"], s["all"]
        persist, full, flat = (
            flip["vs_persistence"]["baseline_mae"],
            flip["vs_persistence"]["mae"],
            flip["flat_curves_vs_persistence"]["mae"],
        )
        gain = 100.0 * (1.0 - full / persist) if persist else float("nan")
        c1, c2 = fold[curve_keys[0]], fold[curve_keys[1]]
        parts.append(
            f"{MECH_LABEL[mech].capitalize()} (hold-out {fold['fold']}): on quarter-hours where SI changes sign the "
            f"operator cuts the next-price MAE from {_f(persist)} EUR/MWh (persistence) to {_f(full)} "
            f"({_f(gain, 0)} % lower; {_f(flat)} with flat volume curves); over all pairs {_f(allp['vs_persistence']['mae'])} "
            f"vs {_f(allp['vs_persistence']['baseline_mae'])} (flat curves {_f(allp['flat_curves_vs_persistence']['mae'])}). "
            f"The volume curves, scored on realised activation changes, have calibration slopes "
            f"{_f(c1['calibration_slope'], 2)} ({names[0]}) and {_f(c2['calibration_slope'], 2)} ({names[1]}) "
            f"but explain only {_f(100 * max(c1['r2_vs_baseline'], 0.0), 1)} % / {_f(100 * max(c2['r2_vs_baseline'], 0.0), 1)} % "
            "of within-hour price changes."
        )
    halves = validation.get("half_years", [])
    if len(halves) >= 2:
        first, last = halves[0], halves[-1]
        parts.append(
            f"The fitted MIP response at +200 MW went from {_f(first['mip_curve_eur_mwh_at_mw']['200'])} EUR/MWh "
            f"({first['period']}) to {_f(last['mip_curve_eur_mwh_at_mw']['200'])} ({last['period']}), the MDP response "
            f"from {_f(first['mdp_curve_eur_mwh_at_mw']['200'])} to {_f(last['mdp_curve_eur_mwh_at_mw']['200'])}: "
            "the Belgian balancing-energy market has become much deeper. The branch switch and alpha therefore carry "
            "most of the price impact of a fleet; the capacity-value results include a branch-and-alpha-only "
            "sensitivity."
        )
    return " ".join(parts)


def _marginal_table(curve: pd.DataFrame, window: str, mech: str) -> pd.DataFrame:
    """Total fleet value and the marginal value of the last MW added, per policy and fleet size."""
    rows = []
    sub = curve[(curve["window"] == window) & (curve["mechanism"] == mech)]
    sizes = sorted(sub["fleet_mw"].unique())
    totals: dict[str, list[float]] = {}
    for policy in ("price_taking_threshold", "impact_aware_threshold"):
        p = sub[sub["policy"] == policy].set_index("fleet_mw")
        totals[policy] = [float(p.loc[q, "eur_per_mw_year"] * q) if q in p.index else float("nan") for q in sizes]
    for i, q in enumerate(sizes):
        row: dict[str, str] = {"Q MW": f"{q:,.0f}"}
        for policy, short in (("price_taking_threshold", "price taker"), ("impact_aware_threshold", "impact-aware")):
            tot = totals[policy]
            row[f"{short} total MEUR/yr"] = _f(tot[i] / 1e6, 2)
            if i == 0:
                row[f"{short} marginal kEUR/MW/yr"] = _k(tot[i] / q).rstrip("k")
            else:
                row[f"{short} marginal kEUR/MW/yr"] = _k((tot[i] - tot[i - 1]) / (q - sizes[i - 1])).rstrip("k")
        rows.append(row)
    return pd.DataFrame(rows)


def _implications(curve: pd.DataFrame, eq: pd.DataFrame) -> str:
    out = ["## Implications for a large optimiser", ""]
    w26 = "2026H1"
    for mech in ("current", "new2026"):
        if not ((curve["window"] == w26) & (curve["mechanism"] == mech)).any():
            continue
        q_pt, v_pt = _best_total(curve, w26, mech, "price_taking_threshold")
        q_ia, v_ia = _best_total(curve, w26, mech, "impact_aware_threshold")
        dep = _value_at(curve, w26, mech, "impact_aware_threshold", 1600.0, "mean_deployed_share")
        dep400 = _value_at(curve, w26, mech, "impact_aware_threshold", 400.0, "mean_deployed_share")
        out.append(
            f"- **{MECH_LABEL[mech]}**: the total implicit-balancing value of a price-taking fleet peaks at about "
            f"{q_pt:,.0f} MW ({_f(v_pt / 1e6, 2)} MEUR/yr, Jan-Jul 2026 annualised); every MW added beyond that "
            f"lowers the fleet's total. The impact-aware fleet's total peaks at {q_ia:,.0f} MW of the grid "
            f"({_f(v_ia / 1e6, 2)} MEUR/yr) because it throttles itself: in its active quarter-hours it deploys on "
            f"average {_pct(dep400)} of its MW at 400 MW and {_pct(dep)} at 1,600 MW."
        )
        out.append("")
        out.append(_table(_marginal_table(curve, w26, mech)))
        out.append("")
    out.append(
        "- Rule for a ~1 GW optimiser: do not let the whole fleet react implicitly to the same signal at full power. "
        "Size the implicit-reacting MW where the marginal value in the table above falls to the value of the next-best "
        "use of a MW (explicit aFRR capacity, day-ahead / intraday), or run an impact-aware controller that sizes the "
        "reaction every minute (`ImpactAwareThresholdStrategy`). The capacity-value curve "
        "(`reports/ledgers/impact_capacity_value_curve.parquet`) is the input for the valuation saturation scenarios."
    )
    if not eq.empty:
        out.append(_herding_incentive(eq))
    return "\n".join(out)


def _herding_incentive(eq: pd.DataFrame) -> str:
    """Data-driven comparison of the two formulas in the multi-fleet game."""
    lines = []
    caps = sorted(eq["total_mw"].unique())
    big = [q for q in caps if q >= 800.0] or caps[-1:]
    for mech in ("current", "new2026"):
        if not (eq["mechanism"] == mech).any():
            continue
        parts = []
        for q in big:
            h = _eq_at(eq, mech, "herding", q, "eur_per_mw_year")
            n = _eq_at(eq, mech, "nash", q, "eur_per_mw_year")
            nv = _eq_at(eq, mech, "naive", q, "eur_per_mw_year")
            hr = _eq_at(eq, mech, "herding", q, "residual_imbalance_change_pct")
            nr = _eq_at(eq, mech, "nash", q, "residual_imbalance_change_pct")
            parts.append(
                f"{q:,.0f} MW: Nash - herding = {_k(n - h)} EUR/MW/yr, naive - herding = {_k(nv - h)}, residual |SI| "
                f"herding {_f(hr)} % vs Nash {_f(nr)} %"
            )
        lines.append(f"- **{MECH_LABEL[mech]}** (4 fleets): " + "; ".join(parts) + ".")
    cur_h = _eq_at(eq, "current", "herding", 1600.0, "residual_imbalance_change_pct")
    new_h = _eq_at(eq, "new2026", "herding", 1600.0, "residual_imbalance_change_pct")
    cur_gap = _eq_at(eq, "current", "nash", 1600.0, "eur_per_mw_year") - _eq_at(
        eq, "current", "herding", 1600.0, "eur_per_mw_year"
    )
    new_gap = _eq_at(eq, "new2026", "nash", 1600.0, "eur_per_mw_year") - _eq_at(
        eq, "new2026", "herding", 1600.0, "eur_per_mw_year"
    )
    if all(math.isfinite(v) for v in (cur_h, new_h, cur_gap, new_gap)):
        direction = "weaker" if new_gap < cur_gap else "stronger"
        text = (
            f"- **Does the 2026 formula change the herding incentive?** At 1,600 MW the private cost of herding "
            f"(Nash minus herding per-MW value) is {_k(cur_gap)} EUR/MW/yr under the current formula and "
            f"{_k(new_gap)} under the 2026 formula, i.e. the market's own penalty on herding is {direction} under the "
            f"new formula, while herding changes the residual imbalance by {_f(cur_h)} % (current) versus "
            f"{_f(new_h)} % (2026)."
        )
        if new_gap < cur_gap and new_h > cur_h:
            text += (
                " Without alpha and with a small, often negative short/long price gap, an overshoot costs the fleet "
                "little under the new design: the price signal disciplines the herd less and the system carries more "
                "residual imbalance."
            )
        lines.append(text)
    return "\n".join(["", "### Herding incentive by formula", "", *lines])


def _limitations() -> str:
    return "\n".join(
        [
            "## Limitations (read before using the numbers)",
            "",
            "- The settlement rules are exact (current: neutral band, FLOOR / CAP-bounded marginal prices and alpha "
            "with its price factor, reproducing the published ODS134 prices; 2026: `NewWeightedMechanism2026`, 100 % "
            "of ODS212). The approximations are in how the *state* responds to a fleet: the volume curves explain "
            "little within-hour variance (see validation), the pass-through and the netting rule are averages, "
            "aFRR/mFRR activation inside the quarter-hour is represented by quarter-hour averages (the 2026 operator's "
            "default keeps the aFRR/mFRR weights; `afrr_first` is the alternative), and the incentive component of the "
            "2026 formula is kept at its published value.",
            "- ODS126 minute activation volumes are not on the scale of the ODS127 quarter-hour volumes, so running "
            "activations are mapped from the running SI with historical medians (current formula).",
            "- The simulation curves are fitted in-sample on each evaluation window (the 'environment'); the impact-aware "
            "policy is given the same response function (known impact function, unknown future state). Misspecified "
            "beliefs are shown in the sensitivity table.",
            "- The fleets are incremental to the historical state: today's implicit reactions (including the ~1 GW of "
            "existing batteries) are already in the data; bands use exogenous published prices; equilibrium fleets "
            "solve a per-minute stage game (band = opportunity cost of energy) and SOC only clips their power.",
            "- The 2026-formula data is Elia's simulation of the new design on historical days (ODS211/212); real BSP and "
            "BRP behaviour will adapt to the new design.",
        ]
    )
