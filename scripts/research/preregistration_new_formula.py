"""Pre-registered prediction for the first weeks of Elia's new imbalance-price formula (hypothesis H0).

Before the planned go-live (24 Nov 2026 10:00 CET) this script freezes 90 % prediction intervals for the
new-formula quarter-hour price over the first post-reform window W1 = 24 Nov 2026 10:00 CET .. 1 Feb 2027.

Decision statistics are measured relative to the day-ahead price of the same quarter-hour (spread = imbalance price
minus day-ahead price), so that fuel-price level shifts between the analogue months and W1 do not decide the test:
mean, standard deviation, 1st and 99th percentile of the spread, and the share of negative imbalance prices.
H0 is falsified if fewer than 4 of these 5 statistics of the realised W1 data fall inside their intervals.
Raw price statistics and the perfect-foresight value of a 1 MW / 2 MWh battery are frozen for information.

Analogue pool (seasonally matched, same physical system states):
- Elia's own simulation of the new formula for 1 Jan-28 Feb 2026 (ODS212);
- Nov-Dec 2025 repriced with `reconstruct_new_formula_history`, quantile-mapped onto the ODS212 distribution on
  the Jan-Jul 2026 overlap to remove the reconstruction's distributional bias.

Intervals come from a 7-day block bootstrap of whole local days (2,000 windows of W1's length). They capture
day-to-day variability of analogue days only; behavioural responses of market parties to the new price are not in
the pool. That is exactly what H0 tests.

Usage:
    python scripts/research/preregistration_new_formula.py            # freeze (writes reports/)
    python scripts/research/preregistration_new_formula.py --evaluate # after W1, compare with realised data
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from euroflex_bess_lab import public_data
from euroflex_bess_lab.realtime import BatteryParams
from euroflex_bess_lab.realtime.mechanisms import reconstruct_new_formula_history
from euroflex_bess_lab.realtime.strategies import solve_qh_arbitrage

TZ = "Europe/Brussels"
W1_START = pd.Timestamp("2026-11-24T10:00:00", tz=TZ).tz_convert("UTC")
W1_END = pd.Timestamp("2027-02-01T00:00:00", tz=TZ).tz_convert("UTC")
BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0, degradation_eur_per_mwh=8.0)
DECISION_STATS = (
    "spread_mean_eur_mwh",
    "spread_std_eur_mwh",
    "spread_p01_eur_mwh",
    "spread_p99_eur_mwh",
    "negative_share",
)
INFO_STATS = ("mean_eur_mwh", "std_eur_mwh", "p01_eur_mwh", "p99_eur_mwh")
N_BOOT = 2000
BLOCK_DAYS = 7
SEED = 20261124

Day = tuple[np.ndarray, np.ndarray]


def daily_oracle(prices: np.ndarray) -> float:
    """Perfect-foresight P&L of one local day; SOC left above/below the start is valued at the day's median price."""
    power, soc = solve_qh_arbitrage(prices, BATTERY, initial_soc_mwh=BATTERY.initial_soc_mwh)
    pnl = float(np.sum(power * 0.25 * np.nan_to_num(prices)))
    pnl -= float(np.sum(np.clip(power, 0, None)) * 0.25 * BATTERY.degradation_eur_per_mwh)
    pnl += float((soc[-1] - BATTERY.initial_soc_mwh) * np.nanmedian(prices))
    return pnl


def with_day_ahead(frame: pd.DataFrame, price_column: str) -> pd.DataFrame:
    start, end = frame["timestamp_utc"].min(), frame["timestamp_utc"].max() + pd.Timedelta(minutes=15)
    da = public_data.load_day_ahead_prices("BE", start, end, quarter_hourly=True)[["timestamp_utc", "price_eur_mwh"]]
    out = frame[["timestamp_utc", price_column]].rename(columns={price_column: "price"})
    return out.merge(da.rename(columns={"price_eur_mwh": "da"}), on="timestamp_utc", how="left")


def local_days(frame: pd.DataFrame) -> list[Day]:
    day = pd.to_datetime(frame["timestamp_utc"], utc=True).dt.tz_convert(TZ).dt.date
    return [
        (chunk["price"].to_numpy(dtype=float), chunk["da"].to_numpy(dtype=float))
        for _, chunk in frame.groupby(day, sort=True)
    ]


def quantile_map(source: np.ndarray, reference: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    qs = np.linspace(0.0, 1.0, 1001)
    return np.nanquantile(source, qs), np.nanquantile(reference, qs)


def window_stats(prices: np.ndarray, da: np.ndarray) -> dict[str, float]:
    spread = prices - da
    return {
        "spread_mean_eur_mwh": float(np.nanmean(spread)),
        "spread_std_eur_mwh": float(np.nanstd(spread)),
        "spread_p01_eur_mwh": float(np.nanquantile(spread, 0.01)),
        "spread_p99_eur_mwh": float(np.nanquantile(spread, 0.99)),
        "negative_share": float(np.nanmean(prices < 0)),
        "mean_eur_mwh": float(np.nanmean(prices)),
        "std_eur_mwh": float(np.nanstd(prices)),
        "p01_eur_mwh": float(np.nanquantile(prices, 0.01)),
        "p99_eur_mwh": float(np.nanquantile(prices, 0.99)),
    }


def build_pool() -> tuple[list[Day], dict[str, int]]:
    sim = public_data.load_belgium_new_formula_qh("2026-01-01", "2026-08-01")
    current = public_data.load_belgium_qh("2025-11-01", "2026-08-01")
    jan = pd.Timestamp("2026-01-01", tz="UTC")
    recon_overlap = reconstruct_new_formula_history(current[current["timestamp_utc"] >= jan].reset_index(drop=True))
    merged = recon_overlap.merge(
        sim[["timestamp_utc", "imbalance_price_eur_mwh"]], on="timestamp_utc", suffixes=("_r", "_e")
    )
    src, ref = quantile_map(
        merged["imbalance_price_eur_mwh_r"].to_numpy(dtype=float),
        merged["imbalance_price_eur_mwh_e"].to_numpy(dtype=float),
    )
    recon = reconstruct_new_formula_history(current[current["timestamp_utc"] < jan].reset_index(drop=True))
    recon["mapped"] = np.interp(recon["imbalance_price_eur_mwh"].to_numpy(dtype=float), src, ref)
    sim_winter = sim[sim["timestamp_utc"] < pd.Timestamp("2026-03-01", tz="UTC")]
    pool = local_days(with_day_ahead(sim_winter, "imbalance_price_eur_mwh"))
    n_sim = len(pool)
    pool += local_days(with_day_ahead(recon, "mapped"))
    return pool, {"days_simulated_jan_feb_2026": n_sim, "days_reconstructed_nov_dec_2025": len(pool) - n_sim}


def bootstrap(pool: list[Day], n_days: int, rng: np.random.Generator) -> tuple[dict[str, np.ndarray], np.ndarray]:
    oracle_by_day = np.array([daily_oracle(prices) for prices, _ in pool])
    samples: dict[str, list[float]] = {name: [] for name in (*DECISION_STATS, *INFO_STATS)}
    oracle: list[float] = []
    for _ in range(N_BOOT):
        idx: list[int] = []
        while len(idx) < n_days:
            start = int(rng.integers(0, len(pool)))
            idx.extend((start + offset) % len(pool) for offset in range(BLOCK_DAYS))
        idx = idx[:n_days]
        prices = np.concatenate([pool[i][0] for i in idx])
        da = np.concatenate([pool[i][1] for i in idx])
        for name, value in window_stats(prices, da).items():
            samples[name].append(value)
        oracle.append(float(np.sum(oracle_by_day[idx])) / n_days * 365.25)
    return {k: np.array(v) for k, v in samples.items()}, np.array(oracle)


def _interval(values: np.ndarray) -> dict[str, float]:
    return {q: float(np.quantile(values, p)) for q, p in (("p05", 0.05), ("p50", 0.5), ("p95", 0.95))}


LABELS = {
    "spread_mean_eur_mwh": "Mean of imbalance minus day-ahead price (EUR/MWh)",
    "spread_std_eur_mwh": "Standard deviation of imbalance minus day-ahead (EUR/MWh)",
    "spread_p01_eur_mwh": "1st percentile of imbalance minus day-ahead (EUR/MWh)",
    "spread_p99_eur_mwh": "99th percentile of imbalance minus day-ahead (EUR/MWh)",
    "negative_share": "Share of quarter-hours with a negative imbalance price",
    "mean_eur_mwh": "Mean imbalance price (EUR/MWh)",
    "std_eur_mwh": "Standard deviation of the imbalance price (EUR/MWh)",
    "p01_eur_mwh": "1st percentile of the imbalance price (EUR/MWh)",
    "p99_eur_mwh": "99th percentile of the imbalance price (EUR/MWh)",
    "oracle_eur_per_mw_year": "Perfect-foresight value, 1 MW / 2 MWh (EUR/MW/yr, daily SOC reset)",
}


def _row(name: str, iv: dict[str, float]) -> str:
    fmt = "{:.3f}" if name == "negative_share" else "{:,.1f}"
    return f"| {LABELS[name]} | {fmt.format(iv['p05'])} | {fmt.format(iv['p50'])} | {fmt.format(iv['p95'])} |"


def freeze(out_dir: Path) -> dict[str, object]:
    rng = np.random.default_rng(SEED)
    pool, info = build_pool()
    n_days = int(round((W1_END - W1_START) / pd.Timedelta(days=1)))
    samples, oracle = bootstrap(pool, n_days, rng)
    intervals = {name: _interval(values) for name, values in samples.items()}
    intervals["oracle_eur_per_mw_year"] = _interval(oracle)
    payload: dict[str, object] = {
        "hypothesis": "H0: at least 4 of the 5 decision statistics of the realised W1 data fall inside their 90 % intervals",
        "decision_statistics": list(DECISION_STATS),
        "window_w1_utc": [str(W1_START), str(W1_END)],
        "window_days": n_days,
        "battery": {"power_mw": 1.0, "energy_mwh": 2.0, "efficiency": 0.95, "degradation_eur_per_mwh": 8.0},
        "pool": info,
        "bootstrap": {"replicates": N_BOOT, "block_days": BLOCK_DAYS, "seed": SEED},
        "intervals_90pct": intervals,
    }
    ledgers = out_dir / "ledgers"
    ledgers.mkdir(parents=True, exist_ok=True)
    (ledgers / "preregistration_new_formula.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    start_local, end_local = W1_START.tz_convert(TZ), W1_END.tz_convert(TZ)
    lines = [
        "# Pre-registered prediction: Elia's new imbalance-price formula, first post-reform window",
        "",
        "Frozen before the planned go-live of the new Belgian imbalance-price formula (24 Nov 2026 10:00 CET,",
        "subject to CREG approval). Generated by `scripts/research/preregistration_new_formula.py` from `data/public`",
        "(Elia Open Data and Energy-Charts day-ahead prices, both CC BY 4.0). The git history of this file is the time stamp.",
        "",
        f"- Window W1: {start_local:%Y-%m-%d %H:%M} to {end_local:%Y-%m-%d %H:%M} Europe/Brussels ({n_days} days).",
        f"- Analogue pool: {info['days_simulated_jan_feb_2026']} days of Elia's simulated new-formula prices (Jan-Feb 2026)"
        f" and {info['days_reconstructed_nov_dec_2025']} days of Nov-Dec 2025 repriced under the new formula"
        " (reconstruction, quantile-mapped onto Elia's simulation).",
        f"- Method: {N_BOOT} block-bootstrap windows ({BLOCK_DAYS}-day blocks of whole days), seed {SEED}.",
        "",
        "## Decision statistics (90 % intervals)",
        "",
        "| Statistic of the new-formula quarter-hour price in W1 | 5 % | median | 95 % |",
        "| --- | ---: | ---: | ---: |",
        *[_row(name, intervals[name]) for name in DECISION_STATS],
        "",
        "## For information (not part of the decision rule)",
        "",
        "| Statistic | 5 % | median | 95 % |",
        "| --- | ---: | ---: | ---: |",
        *[_row(name, intervals[name]) for name in (*INFO_STATS, "oracle_eur_per_mw_year")],
        "",
        "## Decision rule",
        "",
        "H0 holds if at least 4 of the 5 decision statistics of the realised W1 data fall inside their 90 % intervals.",
        "A failure is itself a finding: it means market parties responded to the new price beyond the first-order",
        "counterfactual that both Elia's simulation and this repository assume.",
        "",
        "## What the intervals do not include",
        "",
        "- level shifts between the analogue months and W1 beyond what the day-ahead spread removes;",
        "- behavioural responses of BRPs and BSPs to the new price (that is what H0 tests);",
        "- a go-live slip: if the formula does not go live on 24 Nov 2026, W1 starts at the actual go-live.",
        "",
        "Evaluate after W1 with `python scripts/research/preregistration_new_formula.py --evaluate`.",
    ]
    (out_dir / "preregistration_new_formula.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return payload


def evaluate(out_dir: Path) -> dict[str, object]:
    frozen = json.loads((out_dir / "ledgers" / "preregistration_new_formula.json").read_text(encoding="utf-8"))
    realised = public_data.load_belgium_new_formula_qh(W1_START, W1_END)
    frame = with_day_ahead(realised, "imbalance_price_eur_mwh")
    stats = window_stats(frame["price"].to_numpy(dtype=float), frame["da"].to_numpy(dtype=float))
    iv = frozen["intervals_90pct"]
    inside = {name: iv[name]["p05"] <= stats[name] <= iv[name]["p95"] for name in DECISION_STATS}
    result = {"realised": stats, "inside": inside, "h0_holds": sum(inside.values()) >= 4, "quarter_hours": len(frame)}
    (out_dir / "ledgers" / "preregistration_new_formula_evaluation.json").write_text(
        json.dumps(result, indent=2), encoding="utf-8"
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="reports", type=Path)
    parser.add_argument("--evaluate", action="store_true")
    args = parser.parse_args()
    result = evaluate(args.out) if args.evaluate else freeze(args.out)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
