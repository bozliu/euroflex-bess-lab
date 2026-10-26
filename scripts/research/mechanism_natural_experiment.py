"""Natural experiment: the same Belgian system days priced under Elia's current and new (2026) formulas.

Regenerates `reports/mechanism_natural_experiment.md` and `reports/figures/mechanism_*.png` from `data/public`:

1. reproduction of published prices from components (both formulas);
2. validation of the counterfactual new-formula reconstruction against Elia's own simulation (ODS212);
3. price statistics on Jan-Jul 2026 under both formulas (same physical days);
4. perfect-foresight battery value per MW under each formula (upper bound of the value shift);
5. the counterfactual new-formula value for 2024-06..2025-12.

Usage: python scripts/research/mechanism_natural_experiment.py [--out reports]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from euroflex_bess_lab import public_data
from euroflex_bess_lab.realtime import (
    BatteryParams,
    CurrentAlphaMechanism,
    NewWeightedMechanism2026,
    validate_mechanism,
)
from euroflex_bess_lab.realtime.mechanisms import reconstruct_new_formula_history
from euroflex_bess_lab.realtime.strategies import solve_qh_arbitrage

NE_START, NE_END = "2026-01-01T00:00:00Z", "2026-08-01T00:00:00Z"
HIST_START, HIST_END = "2024-06-01T00:00:00Z", "2026-01-01T00:00:00Z"
TZ = "Europe/Brussels"
BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0, degradation_eur_per_mwh=8.0)


def oracle_value(prices: np.ndarray, battery: BatteryParams, window: int = 96 * 7, overlap: int = 96) -> float:
    """Rolling-window perfect-foresight P&L (EUR) on quarter-hour prices."""
    soc = battery.initial_soc_mwh
    total = 0.0
    start = 0
    n = len(prices)
    while start < n:
        stop = min(n, start + window + overlap)
        power, soc_path = solve_qh_arbitrage(prices[start:stop], battery, initial_soc_mwh=soc)
        commit = min(window, n - start)
        p = power[:commit]
        price = np.nan_to_num(prices[start : start + commit])
        total += float(np.sum(p * 0.25 * price) - np.sum(np.clip(p, 0, None)) * 0.25 * battery.degradation_eur_per_mwh)
        soc = float(soc_path[commit - 1])
        start += commit
    return total


def per_mw_year(pnl: float, n_qh: int, battery: BatteryParams) -> float:
    years = n_qh / 96 / 365.25
    return pnl / battery.power_mw / years


def monthly_oracle(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    rows = []
    local_month = pd.to_datetime(frame["timestamp_utc"], utc=True).dt.tz_convert(TZ).dt.strftime("%Y-%m")
    for month, chunk in frame.groupby(local_month, sort=True):
        prices = chunk[column].to_numpy(dtype=float)
        pnl = oracle_value(prices, BATTERY)
        rows.append({"month": month, "eur_per_mw_year": per_mw_year(pnl, len(prices), BATTERY), "n_qh": len(prices)})
    return pd.DataFrame(rows)


def price_stats(series: pd.Series, da: pd.Series, local_hour: pd.Series) -> dict[str, float]:
    evening = series[(local_hour >= 17) & (local_hour < 23)].mean()
    midday = series[(local_hour >= 10) & (local_hour < 16)].mean()
    return {
        "mean_eur_mwh": float(series.mean()),
        "std_eur_mwh": float(series.std()),
        "p01_eur_mwh": float(series.quantile(0.01)),
        "p99_eur_mwh": float(series.quantile(0.99)),
        "min_eur_mwh": float(series.min()),
        "max_eur_mwh": float(series.max()),
        "negative_share": float((series < 0).mean()),
        "evening_minus_midday_eur_mwh": float(evening - midday),
        "mean_abs_minus_da_eur_mwh": float((series - da).abs().mean()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="reports", type=Path)
    args = parser.parse_args()
    out: Path = args.out
    fig_dir = out / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    qh_all = public_data.load_belgium_qh(HIST_START, NE_END)
    nf = public_data.load_belgium_new_formula_qh(NE_START, NE_END)
    da = public_data.load_day_ahead_prices("BE", HIST_START, NE_END)

    # 1. reproduction
    cur_val = validate_mechanism(CurrentAlphaMechanism(), qh_all)
    new_val = validate_mechanism(NewWeightedMechanism2026(), nf)

    # 2. counterfactual reconstruction vs Elia simulation
    qh_ne = qh_all[
        (qh_all["timestamp_utc"] >= pd.Timestamp(NE_START)) & (qh_all["timestamp_utc"] < pd.Timestamp(NE_END))
    ]
    recon = reconstruct_new_formula_history(qh_ne.reset_index(drop=True))
    merged = recon.merge(
        nf[["timestamp_utc", "imbalance_price_eur_mwh", "main_up_eur_mwh", "main_down_eur_mwh", "weight_afrr"]],
        on="timestamp_utc",
        suffixes=("_recon", "_elia"),
    )
    err = merged["imbalance_price_eur_mwh_recon"] - merged["imbalance_price_eur_mwh_elia"]
    recon_stats = {
        "n_qh": int(len(merged)),
        "within_1_eur_share": float((err.abs() <= 1.0).mean()),
        "within_5_eur_share": float((err.abs() <= 5.0).mean()),
        "mae_eur_mwh": float(err.abs().mean()),
        "median_abs_error_eur_mwh": float(err.abs().median()),
        "corr": float(
            np.corrcoef(merged["imbalance_price_eur_mwh_recon"], merged["imbalance_price_eur_mwh_elia"])[0, 1]
        ),
        "weight_afrr_mae": float((merged["weight_afrr_recon"] - merged["weight_afrr_elia"]).abs().mean()),
    }

    # 3. natural-experiment price statistics
    ne = qh_ne[["timestamp_utc", "imbalance_price_eur_mwh"]].merge(
        nf[["timestamp_utc", "imbalance_price_eur_mwh"]], on="timestamp_utc", suffixes=("_current", "_new")
    )
    da_qh = da.set_index("timestamp_utc")["price_eur_mwh"]
    da_qh = da_qh[~da_qh.index.duplicated()].resample("15min").ffill()
    ne["da_eur_mwh"] = ne["timestamp_utc"].map(da_qh)
    local = pd.to_datetime(ne["timestamp_utc"], utc=True).dt.tz_convert(TZ)
    hour = local.dt.hour
    stats = {
        "current": price_stats(ne["imbalance_price_eur_mwh_current"], ne["da_eur_mwh"], hour),
        "new2026": price_stats(ne["imbalance_price_eur_mwh_new"], ne["da_eur_mwh"], hour),
    }
    hourly = (
        ne.assign(hour=hour)
        .groupby("hour")[["imbalance_price_eur_mwh_current", "imbalance_price_eur_mwh_new", "da_eur_mwh"]]
        .median()
    )

    # 4. oracle value per MW under both formulas on the same days
    ne_cur = oracle_value(ne["imbalance_price_eur_mwh_current"].to_numpy(dtype=float), BATTERY)
    ne_new = oracle_value(ne["imbalance_price_eur_mwh_new"].to_numpy(dtype=float), BATTERY)
    ne_da = oracle_value(ne["da_eur_mwh"].to_numpy(dtype=float), BATTERY)
    monthly = monthly_oracle(ne, "imbalance_price_eur_mwh_current").merge(
        monthly_oracle(ne, "imbalance_price_eur_mwh_new"), on=["month", "n_qh"], suffixes=("_current", "_new")
    )

    # 5. counterfactual history
    hist = qh_all[qh_all["timestamp_utc"] < pd.Timestamp(HIST_END)].reset_index(drop=True)
    hist_recon = reconstruct_new_formula_history(hist)
    hist_cur = oracle_value(hist["imbalance_price_eur_mwh"].to_numpy(dtype=float), BATTERY)
    hist_new = oracle_value(hist_recon["imbalance_price_eur_mwh"].to_numpy(dtype=float), BATTERY)

    results = {
        "reproduction": {
            "current": {"rows": cur_val.rows, "match_rate": cur_val.match_rate, "max_abs_error": cur_val.max_abs_error},
            "new2026": {"rows": new_val.rows, "match_rate": new_val.match_rate, "max_abs_error": new_val.max_abs_error},
        },
        "reconstruction_vs_elia_simulation": recon_stats,
        "natural_experiment_price_stats": stats,
        "oracle_eur_per_mw_year": {
            "jan_jul_2026_current": per_mw_year(ne_cur, len(ne), BATTERY),
            "jan_jul_2026_new2026": per_mw_year(ne_new, len(ne), BATTERY),
            "jan_jul_2026_day_ahead_only": per_mw_year(ne_da, len(ne), BATTERY),
            "jun2024_dec2025_current": per_mw_year(hist_cur, len(hist), BATTERY),
            "jun2024_dec2025_new2026_reconstructed": per_mw_year(hist_new, len(hist), BATTERY),
        },
    }
    (out / "ledgers").mkdir(parents=True, exist_ok=True)
    (out / "ledgers" / "mechanism_natural_experiment.json").write_text(json.dumps(results, indent=2), encoding="utf-8")

    # figures
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(hourly.index, hourly["imbalance_price_eur_mwh_current"], label="current formula")
    ax.plot(hourly.index, hourly["imbalance_price_eur_mwh_new"], label="new 2026 formula")
    ax.plot(hourly.index, hourly["da_eur_mwh"], label="day-ahead", linestyle="--", color="grey")
    ax.set_xlabel("hour (Europe/Brussels)")
    ax.set_ylabel("median price (EUR/MWh)")
    ax.set_title("Belgium, same days Jan-Jul 2026: median price by hour")
    ax.legend()
    fig.tight_layout()
    fig.savefig(fig_dir / "mechanism_hourly_median.png", dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 4))
    x = np.arange(len(monthly))
    ax.bar(x - 0.2, monthly["eur_per_mw_year_current"] / 1000, width=0.4, label="current formula")
    ax.bar(x + 0.2, monthly["eur_per_mw_year_new"] / 1000, width=0.4, label="new 2026 formula")
    ax.set_xticks(x, monthly["month"], rotation=0)
    ax.set_ylabel("perfect-foresight value (kEUR/MW/yr)")
    ax.set_title("Upper-bound battery value (1 MW / 2 MWh) by month, same days")
    ax.legend()
    fig.tight_layout()
    fig.savefig(fig_dir / "mechanism_oracle_monthly.png", dpi=150)
    plt.close(fig)

    o = results["oracle_eur_per_mw_year"]
    lines = [
        "# Natural experiment: Elia's current vs new (2026) imbalance price formula",
        "",
        "Generated by `scripts/research/mechanism_natural_experiment.py` from `data/public` (Elia Open Data, CC BY 4.0;",
        "Energy-Charts day-ahead, CC BY 4.0). Battery for value numbers: 1 MW / 2 MWh, efficiency 0.95 each way,",
        "SOC 5-95 %, degradation 8 EUR/MWh discharged, perfect foresight on final quarter-hour prices (7-day windows).",
        "",
        "## 1. Both formulas are reproduced from published components",
        "",
        "| Formula | Quarter-hours | Match rate (abs error <= 0.011 EUR/MWh) | Max abs error |",
        "| --- | ---: | ---: | ---: |",
        f"| current (ODS134, {HIST_START[:10]}..{NE_END[:10]}) | {cur_val.rows} | {cur_val.match_rate:.4%} | {cur_val.max_abs_error:.3f} |",
        f"| new 2026 (ODS212, Jan-Jul 2026) | {new_val.rows} | {new_val.match_rate:.4%} | {new_val.max_abs_error:.3f} |",
        "",
        "## 2. Counterfactual reconstruction of the new formula from current-design components",
        "",
        "`reconstruct_new_formula_history` uses only ODS134/ODS166/ODS127 fields (no new-formula data). Compared with",
        "Elia's own simulation on the same quarter-hours:",
        "",
        f"- quarter-hours compared: {recon_stats['n_qh']}",
        f"- within 1 EUR/MWh: {recon_stats['within_1_eur_share']:.1%}; within 5 EUR/MWh: {recon_stats['within_5_eur_share']:.1%}",
        f"- MAE {recon_stats['mae_eur_mwh']:.2f} EUR/MWh, median abs error {recon_stats['median_abs_error_eur_mwh']:.2f}, correlation {recon_stats['corr']:.3f}",
        f"- aFRR weight MAE {recon_stats['weight_afrr_mae']:.3f}",
        "",
        "The weights are reproduced closely, but the prices are not: the new design's aFRR price is a volume-weighted",
        "average over 4-second PICASSO cycles, which the historical public data does not contain (a minute-level",
        "volume-weighted rebuild only lifts the correlation to about 0.89). The 2024-2025 counterfactual below is",
        "therefore indicative; the natural experiment itself uses Elia's official simulation.",
        "",
        "## 3. Same days, two prices (Jan-Jul 2026)",
        "",
        "| Statistic | current | new 2026 |",
        "| --- | ---: | ---: |",
    ]
    for key in stats["current"]:
        lines.append(f"| {key} | {stats['current'][key]:.3f} | {stats['new2026'][key]:.3f} |")
    lines += [
        "",
        "![hourly](figures/mechanism_hourly_median.png)",
        "",
        "## 4. Upper-bound battery value under each formula",
        "",
        "| Window | Pricing | Perfect-foresight value (EUR/MW/yr) |",
        "| --- | --- | ---: |",
        f"| Jan-Jul 2026 | current formula | {o['jan_jul_2026_current']:,.0f} |",
        f"| Jan-Jul 2026 | new 2026 formula (Elia simulation) | {o['jan_jul_2026_new2026']:,.0f} |",
        f"| Jan-Jul 2026 | day-ahead only | {o['jan_jul_2026_day_ahead_only']:,.0f} |",
        f"| Jun 2024-Dec 2025 | current formula | {o['jun2024_dec2025_current']:,.0f} |",
        f"| Jun 2024-Dec 2025 | new 2026 formula (approximate reconstruction, indicative) | {o['jun2024_dec2025_new2026_reconstructed']:,.0f} |",
        "",
        "![monthly](figures/mechanism_oracle_monthly.png)",
        "",
        "## Caveats",
        "",
        "- Elia calls its simulation best effort and non-validated; it reprices historical system states and does not",
        "  include how market parties would react to the new price. The same holds for our reconstruction.",
        "- Perfect-foresight values are upper bounds, not achievable revenue; see `reports/realtime_belgium.md` for",
        "  causal strategies.",
    ]
    (out / "mechanism_natural_experiment.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
