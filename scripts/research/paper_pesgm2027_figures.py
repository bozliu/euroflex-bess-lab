"""Regenerate every data figure of the PES GM 2027 paper as vector PDF (IEEE column width).

Inputs are committed repository outputs only: ``data/public`` (Elia Open Data CC BY 4.0, Energy-Charts CC BY 4.0) via
``euroflex_bess_lab.public_data`` and the quarter-hour ledgers in ``reports/ledgers``. Fig. 1 (the framework
schematic) is drawn in TikZ inside ``main.tex``.

Run from the repository root:

    .venv/bin/python artifacts/paper_pesgm2027/figures.py

Writes ``artifacts/paper_pesgm2027/figures/fig_operator.pdf`` (Fig. 2) and ``fig_frozen.pdf`` (Fig. 3) plus the plotted values as CSV
(``artifacts/paper_pesgm2027/outputs/fig_*.csv``) so every plotted number can be audited.

Style: 8-pt Times New Roman labels (PES Author's Kit), TrueType fonts embedded (pdf.fonttype 42), colours from a
CVD-validated categorical order with secondary encoding (line style, marker, hatch) so the figures stay readable in
grayscale print.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from euroflex_bess_lab import public_data

REPO = Path(__file__).resolve().parents[2]
HERE = REPO / "artifacts" / "paper_pesgm2027"
LEDGERS = REPO / "reports" / "ledgers"
FIG = HERE / "figures"
OUT = HERE / "outputs"
NE_START, NE_END = "2026-01-01T00:00:00Z", "2026-08-01T00:00:00Z"
DAYS_PER_YEAR = 365.25

COLUMN_IN = 3.5  # IEEE conference column width (IEEEtran \columnwidth = 252 pt)
BLUE, ORANGE, AQUA, VIOLET = "#1c5cab", "#eb6834", "#1baf7a", "#6250d6"  # blue step 550 + slots 2, 3, 7 (validated)
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e1e0d9"
RAMP = ("#86b6ef", "#2a78d6", "#104281")  # ordinal blue ramp (steps 250, 450, 650): light -> dark

plt.rcParams.update(
    {
        "font.family": "serif",
        "font.serif": ["Times New Roman", "Times", "STIXGeneral"],
        "mathtext.fontset": "stix",
        "font.size": 8,
        "axes.labelsize": 8,
        "axes.titlesize": 8,
        "xtick.labelsize": 7.5,
        "ytick.labelsize": 7.5,
        "legend.fontsize": 7,
        "axes.linewidth": 0.6,
        "xtick.major.width": 0.6,
        "ytick.major.width": 0.6,
        "xtick.major.size": 2.5,
        "ytick.major.size": 2.5,
        "lines.linewidth": 1.2,
        "axes.edgecolor": MUTED,
        "axes.labelcolor": INK,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "text.color": INK,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.015,
        "hatch.linewidth": 0.45,
        "axes.spines.top": False,
        "axes.spines.right": False,
    }
)


def ledger_qh(name: str) -> pd.DataFrame:
    return pd.read_parquet(LEDGERS / f"realtime_{name}.parquet")[["timestamp_utc", "pnl_eur"]]


# ----------------------------------------------------------------------------------------------------------------
# Fig. 2: same state, two operators


def fig_operator() -> None:
    cur = public_data.load_belgium_qh(NE_START, NE_END)[["timestamp_utc", "si_mw", "imbalance_price_eur_mwh"]]
    new = public_data.load_belgium_new_formula_qh(NE_START, NE_END)[["timestamp_utc", "imbalance_price_eur_mwh"]]
    da = public_data.load_day_ahead_prices("BE", NE_START, NE_END)
    da_qh = da.set_index("timestamp_utc")["price_eur_mwh"]
    da_qh = da_qh[~da_qh.index.duplicated()].resample("15min").ffill()
    m = cur.merge(new, on="timestamp_utc", suffixes=("_cur", "_new"))
    m["da"] = m["timestamp_utc"].map(da_qh)
    m = m.dropna(subset=["si_mw", "imbalance_price_eur_mwh_cur", "imbalance_price_eur_mwh_new", "da"])
    edges = np.arange(-650.0, 651.0, 50.0)
    centers = 0.5 * (edges[:-1] + edges[1:])
    m["bin"] = pd.cut(m["si_mw"], edges, labels=centers)
    rows = []
    for tag in ("cur", "new"):
        prem = m[f"imbalance_price_eur_mwh_{tag}"] - m["da"]
        g = prem.groupby(m["bin"], observed=False)
        q = pd.DataFrame({"q25": g.quantile(0.25), "q50": g.quantile(0.5), "q75": g.quantile(0.75), "n": g.size()})
        q["rule"] = tag
        q["si_center_mw"] = centers
        rows.append(q.reset_index(drop=True))
    curve = pd.concat(rows, ignore_index=True)
    curve = curve[curve["n"] >= 30]
    OUT.mkdir(parents=True, exist_ok=True)
    curve.to_csv(OUT / "fig_operator_premium_by_si.csv", index=False)

    # value by |SI| regime (oracle upper bound, same days)
    regimes = ("|SI|<25", "25-150", ">150")
    reg_rows = []
    for tag, name, si_frame in (
        ("cur", "oracle_qh_current_B", cur),
        (
            "new",
            "oracle_qh_new2026_B",
            public_data.load_belgium_new_formula_qh(NE_START, NE_END)[["timestamp_utc", "si_mw"]],
        ),
    ):
        led = ledger_qh(name).merge(si_frame[["timestamp_utc", "si_mw"]], on="timestamp_utc", how="left")
        a = led["si_mw"].abs()
        reg = np.select([a < 25.0, a <= 150.0], [regimes[0], regimes[1]], default=regimes[2])
        days = pd.DatetimeIndex(led["timestamp_utc"]).floor("D").nunique()
        kpy = led.groupby(reg)["pnl_eur"].sum() / (days / DAYS_PER_YEAR) / 1000.0
        for r in regimes:
            reg_rows.append({"rule": tag, "regime": r, "k_eur_per_mw_year": float(kpy.get(r, 0.0))})
    reg_df = pd.DataFrame(reg_rows)
    reg_df.to_csv(OUT / "fig_operator_oracle_value_by_regime.csv", index=False)

    fig, (ax, bx) = plt.subplots(
        1, 2, figsize=(COLUMN_IN, 1.46), gridspec_kw={"width_ratios": [1.45, 1.0], "wspace": 0.42}
    )
    for tag, color, ls, mk, label, hatch in (
        ("cur", BLUE, "-", "o", "Current rule", None),
        ("new", ORANGE, (0, (4, 1.6)), "^", "New rule (Elia sim.)", "////"),
    ):
        c = curve[curve["rule"] == tag]
        if hatch:
            ax.fill_between(
                c["si_center_mw"],
                c["q25"],
                c["q75"],
                facecolor="none",
                edgecolor=color,
                hatch=hatch,
                linewidth=0.0,
                alpha=0.55,
                zorder=1,
            )
        else:
            ax.fill_between(c["si_center_mw"], c["q25"], c["q75"], color=color, alpha=0.18, linewidth=0, zorder=1)
        ax.plot(
            c["si_center_mw"], c["q50"], color=color, ls=ls, marker=mk, ms=2.6, mew=0.0, lw=1.1, label=label, zorder=3
        )
    ax.axhline(0.0, color=GRID, lw=0.6, zorder=0)
    for xv in (-150.0, 150.0):
        ax.axvline(xv, color=MUTED, lw=0.5, ls=":", zorder=0)
    ax.text(-160, 250, r"$\alpha$ active", ha="right", va="top", fontsize=6.8, color=MUTED)
    ax.text(160, 250, r"$\alpha$ active", ha="left", va="top", fontsize=6.8, color=MUTED)
    ax.set_xlim(-540, 560)
    ax.set_ylim(-260, 260)
    ax.set_xticks([-500, -250, 0, 250, 500])
    ax.set_xlabel("System imbalance, QH (MW)")
    ax.set_ylabel("Price minus day-ahead (EUR/MWh)")
    ax.legend(loc="lower left", frameon=False, handlelength=2.2, borderaxespad=0.1, labelspacing=0.25)
    ax.text(0.0, 1.03, "(a)", transform=ax.transAxes, fontsize=8, ha="left", va="bottom")

    x = np.arange(len(regimes))
    w = 0.38
    for k, (tag, color, hatch, label) in enumerate((("cur", BLUE, None, "Current"), ("new", ORANGE, "//////", "New"))):
        vals = [reg_df[(reg_df.rule == tag) & (reg_df.regime == r)]["k_eur_per_mw_year"].iloc[0] for r in regimes]
        bars = bx.bar(
            x + (k - 0.5) * w,
            vals,
            width=w * 0.92,
            color=color if hatch is None else "white",
            edgecolor=color,
            hatch=hatch,
            linewidth=0.8,
            label=label,
            zorder=2,
        )
        for b, v in zip(bars, vals, strict=True):
            y = v + 3 if v >= 0 else v - 3
            bx.text(
                b.get_x() + b.get_width() / 2,
                y,
                f"{v:.0f}".replace("-", "\u2212"),
                ha="center",
                va="bottom" if v >= 0 else "top",
                fontsize=6.8,
                color=INK,
            )
    bx.axhline(0.0, color=MUTED, lw=0.6, zorder=1)
    bx.set_xticks(x)
    bx.set_xticklabels(["$<$25", "25–150", "$>$150"])
    bx.set_xlabel(r"$|$SI$|$ regime (MW)")
    bx.set_ylabel("Oracle value (kEUR/MW/yr)")
    bx.set_ylim(-24, 182)
    bx.legend(loc="upper left", frameon=False, handlelength=1.2, borderaxespad=0.1, labelspacing=0.25)
    bx.text(0.0, 1.03, "(b)", transform=bx.transAxes, fontsize=8, ha="left", va="bottom")
    FIG.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG / "fig_operator.pdf")
    plt.close(fig)


# ----------------------------------------------------------------------------------------------------------------
# Fig. 3: frozen models (reports/ledgers/oos_daily_pnl.parquet, scripts/research/out_of_sample.py)


def local_daily(name: str) -> pd.Series:
    """Daily P&L per local (Europe/Brussels) delivery day from a realtime QH ledger (same convention as oos_*)."""
    df = pd.read_parquet(LEDGERS / f"realtime_{name}.parquet")
    day = pd.DatetimeIndex(df["timestamp_utc"]).tz_convert("Europe/Brussels").tz_localize(None).normalize()
    return df.groupby(day)["pnl_eur"].sum()


def fig_frozen() -> None:
    oos = pd.read_parquet(LEDGERS / "oos_daily_pnl.parquet")
    oos["date"] = pd.to_datetime(oos["date"])

    def arm(evaluation: str, name: str) -> pd.Series:
        s = oos[(oos["evaluation"] == evaluation) & (oos["arm"] == name)].set_index("date")["pnl_eur"].sort_index()
        return s.cumsum() / 1000.0

    panel_a = (
        ("prism_risk_frozen", "SCALES-L, frozen", BLUE, "-", 1.5),
        ("prism_risk_wf", "SCALES-L, retrained", ORANGE, (0, (3, 1.8)), 1.0),
        ("threshold_price", "price rule", AQUA, (0, (5, 1.5, 1.2, 1.5)), 1.0),
        ("threshold_si", "SI rule", VIOLET, (0, (1.2, 1.2)), 1.1),
    )
    ev_a = "current_2025-06-30"
    a = pd.DataFrame({n: arm(ev_a, n) for n, *_ in panel_a})
    ev_b = "new2026_2025-12-31"
    b = pd.DataFrame(
        {
            "prism_zero_live_risk_frozen": arm(ev_b, "prism_zero_live_risk_frozen"),
            "prism_zero_risk_frozen": arm(ev_b, "prism_zero_risk_frozen"),
            "threshold_price": arm(ev_b, "threshold_price"),
        }
    )
    for col, name in (
        ("direct_rebuilt_risk_new2026_JanJul2026", "direct_rebuilt_risk_new2026_JanJul2026"),
        ("forecast_mpc_risk_new2026_B", "forecast_mpc_risk_new2026_B"),
    ):
        daily_pnl = local_daily(name)
        daily_pnl = daily_pnl[(daily_pnl.index >= b.index.min()) & (daily_pnl.index <= b.index.max())]
        b[col] = daily_pnl.cumsum().reindex(b.index).ffill() / 1000.0
    panel_b = (
        ("prism_zero_live_risk_frozen", "SCALES-L + live, frozen", BLUE, "-", 1.5),
        ("prism_zero_risk_frozen", "SCALES-L, frozen", BLUE, (0, (3, 1.6)), 1.1),
        ("direct_rebuilt_risk_new2026_JanJul2026", "direct, rebuilt targets", VIOLET, (0, (1.2, 1.2)), 1.3),
        ("threshold_price", "price rule", AQUA, (0, (5, 1.5, 1.2, 1.5)), 1.0),
        ("forecast_mpc_risk_new2026_B", "naive MPC", ORANGE, "-", 1.0),
    )
    a.to_csv(OUT / "fig_frozen_current_2025-06-30_keur_per_mw.csv")
    b.to_csv(OUT / "fig_frozen_new2026_keur_per_mw.csv")

    fig, (ax, bx) = plt.subplots(1, 2, figsize=(COLUMN_IN, 1.52), gridspec_kw={"wspace": 0.34})
    for frame, spec, axis, title in (
        (a, panel_a, ax, "(a) current rule, frozen 30 Jun 2025"),
        (b, panel_b, bx, "(b) new rule, frozen 1 Jan 2026"),
    ):
        for name, label, color, ls, lw in spec:
            axis.plot(frame.index, frame[name], color=color, ls=ls, lw=lw, label=label)
        axis.set_xlim(frame.index[0], frame.index[-1])
        axis.set_ylim(0, (1.36 if len(spec) < 5 else 1.55) * float(frame.max().max()))
        axis.grid(axis="y", color=GRID, lw=0.5)
        axis.text(0.0, 1.03, title, transform=axis.transAxes, fontsize=7, ha="left", va="bottom")
        axis.legend(
            loc="upper left",
            frameon=False,
            fontsize=6.6,
            handlelength=2.2,
            borderaxespad=0.05,
            labelspacing=0.15,
            handletextpad=0.4,
        )
    ax.set_ylabel("Cumulative P&L (kEUR/MW)")
    ax.set_xticks(pd.to_datetime(["2025-07-01", "2026-01-01", "2026-07-01"]))
    ax.set_xticklabels(["Jul 25", "Jan 26", "Jul 26"])
    bx.set_xticks(pd.to_datetime(["2026-01-01", "2026-04-01", "2026-07-01"]))
    bx.set_xticklabels(["Jan", "Apr", "Jul"])
    fig.savefig(FIG / "fig_frozen.pdf")
    plt.close(fig)


def main() -> None:
    fig_operator()
    fig_frozen()
    print("wrote", ", ".join(str(p.relative_to(REPO)) for p in sorted(FIG.glob("*.pdf"))))


if __name__ == "__main__":
    main()
