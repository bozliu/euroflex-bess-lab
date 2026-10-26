"""Contract valuation of a Belgian 1 MW / 2 MWh battery from the committed real-data ledgers.

Builds the inputs of `scripts/research/valuation_demo.py` from `reports/ledgers/` and runs the full chain
(revenue scenarios, toll, floor, partial toll, swap, attribution) into `reports/valuation_belgium.md`:

- current regime: SCALES-L with risk filter settled under the current formula, Jun 2024-Oct 2026 (periods A+B+C);
- new regime: the deployable SCALES-L (zero-history operator + live running price, risk filter) settled under the
  new 2026 formula on Jan-Jul 2026, stretched over the current history by the paired-overlap ratio;
- oracle and benchmark index: perfect foresight and the reactive price threshold over the same span;
- market saturation: the symmetric-Nash per-MW value curve of `reports/ledgers/impact_equilibrium.parquet`
  (four fleets, Jan-Jul 2026, one curve per formula) applied along an assumed path of implicitly reacting MW.

Assumptions that are judgement, not data (stated again in the report):
- implicit-reaction fleet in Belgium: 500 MW in 2026, 800 (2027), 1,300 (2028), 1,700 (2029), 2,000 (2030),
  2,400 (2033) MW, i.e. about half of an operational fleet growing from ~1 GW towards ~4 GW;
- regime weights for the tenor: new formula 0.9, current formula 0.1 (residual risk that the go-live slips);
- tenor 7 years from 2027, other settings as in `valuation_demo.py` defaults.

Usage: python scripts/research/valuation_belgium.py [--scratch artifacts/valuation_inputs]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
LEDGERS = REPO_ROOT / "reports" / "ledgers"
FLEET_PATH = "2027=800,2028=1300,2029=1700,2030=2000,2033=2400"
REFERENCE_FLEET_MW = 500.0
WEIGHTS = "current=0.1,new2026=0.9"
# 2024 to mid-2025 were exceptional Belgian revenue years (aFRR scarcity, a shallow market); a 2027+ contract is
# priced off the latest 12 months by default, and the full history is kept as a sensitivity.
RECENT_HISTORY_START = "2025-11-01"


def concat_ledgers(
    names: list[str], out: Path, *, strategy: str, mechanism: str, history_start: str | None = None
) -> Path:
    frames = [pd.read_parquet(LEDGERS / f"{name}.parquet") for name in names]
    ledger = pd.concat(frames, ignore_index=True).sort_values("timestamp_utc")
    ledger = ledger.drop_duplicates(subset=["timestamp_utc"], keep="first").reset_index(drop=True)
    if history_start is not None:
        ledger = ledger[ledger["timestamp_utc"] >= pd.Timestamp(history_start, tz="UTC")].reset_index(drop=True)
    ledger.to_parquet(out, index=False)
    first = json.loads((LEDGERS / f"{names[0]}.json").read_text(encoding="utf-8"))
    battery = first.get("battery", {})
    meta = {
        "asset_id": out.stem,
        "market": "BE",
        "power_mw": battery.get("power_mw", 1.0),
        "energy_mwh": battery.get("energy_mwh", 2.0),
        "mechanism": mechanism,
        "strategy": strategy,
        "degradation_eur_per_mwh": battery.get("degradation_eur_per_mwh", 8.0),
        "period_start": str(ledger["timestamp_utc"].iloc[0]),
        "period_end": str(ledger["timestamp_utc"].iloc[-1] + pd.Timedelta(minutes=15)),
        "synthetic": False,
        "sources": [f"reports/ledgers/{name}.parquet" for name in names],
    }
    out.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return out


def nash_curve(out: Path) -> Path:
    eq = pd.read_parquet(LEDGERS / "impact_equilibrium.parquet")
    nash = eq[(eq["policy"] == "nash") & (eq["window"] == "2026H1")]
    curve = nash.rename(columns={"total_mw": "fleet_mw", "eur_per_mw_year": "value_eur_per_mw_year"})
    curve[["mechanism", "fleet_mw", "value_eur_per_mw_year"]].sort_values(["mechanism", "fleet_mw"]).to_csv(
        out, index=False
    )
    return out


IMPLICIT_LEDGERS: dict[str, tuple[list[str], str, str]] = {
    "current": (
        ["realtime_prism_risk_current_A", "realtime_prism_risk_current_B", "realtime_prism_risk_current_C"],
        "scales_l_risk",
        "current",
    ),
    "new2026": (["realtime_prism_live_risk_new2026_B"], "scales_l_live_risk", "new2026"),
    "oracle": (
        ["realtime_oracle_qh_current_A", "realtime_oracle_qh_current_B", "realtime_oracle_qh_current_C"],
        "oracle_qh",
        "current",
    ),
    "index": (
        [
            "realtime_threshold_price_current_A",
            "realtime_threshold_price_current_B",
            "realtime_threshold_price_current_C",
        ],
        "threshold_price",
        "current",
    ),
}

STACK_LEDGERS: dict[str, tuple[list[str], str, str]] = {
    "current": (
        ["reserve_forecast_current_2025", "reserve_forecast_current_2026YTD"],
        "afrr_implicit_forecast_split",
        "current",
    ),
    "new2026": (["reserve_forecast_new2026_2026-01_2026-07"], "afrr_implicit_forecast_split", "new2026"),
    "index": (["reserve_fixed_100_current_2025", "reserve_fixed_100_current_2026YTD"], "afrr_fixed_100", "current"),
}


def run_variant(
    scratch: Path,
    *,
    ledgers: dict[str, tuple[list[str], str, str]],
    history_start: str | None,
    report_name: str,
    prefix: str,
    title: str,
    reprice: bool = True,
    scope: str = "implicit balancing only (SCALES-L)",
) -> Path:
    scratch.mkdir(parents=True, exist_ok=True)
    paths = {
        role: concat_ledgers(
            names,
            scratch / f"be_{role}_{strategy}.parquet",
            strategy=strategy,
            mechanism=mechanism,
            history_start=history_start if role != "new2026" else None,
        )
        for role, (names, strategy, mechanism) in ledgers.items()
    }
    curve = nash_curve(scratch / "be_nash_capacity_curve.csv")
    report = REPO_ROOT / "reports" / report_name
    cmd = [sys.executable, str(REPO_ROOT / "scripts" / "research" / "valuation_demo.py")]
    for role, path in paths.items():
        cmd += [f"--{role}", str(path)]
    if reprice:
        cmd += ["--new-prices", str(REPO_ROOT / "data" / "public" / "belgium" / "new_formula_qh_2026.parquet")]
    cmd += [
        "--capacity-curve",
        str(curve),
        "--fleet",
        FLEET_PATH,
        "--reference-fleet-mw",
        str(REFERENCE_FLEET_MW),
        "--weights",
        WEIGHTS,
        "--report",
        str(report),
        "--figure-prefix",
        prefix,
        "--title",
        title,
    ]
    subprocess.run(cmd, check=True, cwd=REPO_ROOT)
    history = f"from {history_start}" if history_start else "full history (Jun 2024 onwards)"
    note = [
        "",
        "## Inputs and judgement calls of this run",
        "",
        "Built by `scripts/research/valuation_belgium.py` from committed ledgers in `reports/ledgers/`:",
        "",
        f"- revenue scope: {scope}; current-regime history {history};",
        "- ledgers: " + ", ".join(f"{role}: " + " + ".join(names) for role, (names, _, _) in ledgers.items()) + ";",
        "- the new-formula ledger covers Jan-Jul 2026 (Elia's simulation) and is stretched over the current history by",
        "  the paired-overlap ratio;",
        "- saturation: symmetric-Nash per-MW value versus total implicitly reacting MW (four fleets, Jan-Jul 2026,",
        "  `reports/impact.md`), one curve per formula"
        + (
            "; applied to the whole stack here, which is conservative for the aFRR part (its capacity prices already"
            " collapsed once as batteries entered, and that history is in the ledgers)."
            if "aFRR" in scope
            else "."
        ),
        "",
        "Judgement calls (not data): implicitly reacting Belgian fleet "
        f"{REFERENCE_FLEET_MW:,.0f} MW in 2026 and {FLEET_PATH.replace(',', ', ')} MW afterwards (about half of an",
        "operational fleet growing from ~1 GW towards ~4 GW by 2030); regime weights "
        f"{WEIGHTS.replace(',', ', ')} for the tenor. Change them on the command line before quoting a number.",
    ]
    report.write_text(report.read_text(encoding="utf-8") + "\n".join(note) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, default=REPO_ROOT / "artifacts" / "valuation_inputs")
    parser.add_argument(
        "--history-start",
        default=RECENT_HISTORY_START,
        help="first day of the current-regime history used for the main run (latest 12 months by default)",
    )
    args = parser.parse_args()
    run_variant(
        args.scratch / "recent",
        ledgers=IMPLICIT_LEDGERS,
        history_start=args.history_start,
        report_name="valuation_belgium.md",
        prefix="valuation_belgium",
        title="Belgian 1 MW / 2 MWh battery: contract valuation from real-data ledgers (latest 12 months)",
    )
    run_variant(
        args.scratch / "full",
        ledgers=IMPLICIT_LEDGERS,
        history_start=None,
        report_name="valuation_belgium_full_history.md",
        prefix="valuation_belgium_full_history",
        title="Sensitivity: the same valuation on the full Jun 2024-Oct 2026 history",
    )
    run_variant(
        args.scratch / "stack",
        ledgers=STACK_LEDGERS,
        history_start=args.history_start,
        report_name="valuation_belgium_stack.md",
        prefix="valuation_belgium_stack",
        title="Belgian 1 MW / 2 MWh battery: toll on the aFRR + implicit-balancing stack (latest 12 months)",
        reprice=False,
        scope="aFRR capacity + aFRR energy + implicit balancing (reserve forecast split, threshold implicit layer)",
    )


if __name__ == "__main__":
    main()
