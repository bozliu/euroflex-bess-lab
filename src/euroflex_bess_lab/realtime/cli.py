"""`euroflex rt ...` commands for the minute-level real-time engine."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Annotated, Any

import typer

rt_app = typer.Typer(help="Minute-level real-time engine: Belgian implicit balancing on public data.")


def _public_data() -> Any:
    return importlib.import_module("euroflex_bess_lab.public_data")


@rt_app.command("validate-mechanisms")
def validate_mechanisms(
    start: Annotated[str, typer.Option(help="UTC start, quarter-hour aligned")] = "2026-01-01T00:00:00Z",
    end: Annotated[str, typer.Option(help="UTC end (exclusive)")] = "2026-08-01T00:00:00Z",
) -> None:
    """Reproduce Elia's published imbalance prices from their components under both formulas."""
    public_data = _public_data()
    from .mechanisms import CurrentAlphaMechanism, NewWeightedMechanism2026, validate_mechanism

    qh = public_data.load_belgium_qh(start, end)
    report = {"current": validate_mechanism(CurrentAlphaMechanism(), qh).__dict__}
    try:
        nf = public_data.load_belgium_new_formula_qh(start, end)
        report["new2026"] = validate_mechanism(NewWeightedMechanism2026(), nf).__dict__
    except FileNotFoundError:
        pass
    for item in report.values():
        item.pop("mismatches", None)
    typer.echo(json.dumps(report, indent=2, default=str))


@rt_app.command("backtest")
def backtest(
    start: Annotated[str, typer.Option(help="UTC start, quarter-hour aligned")],
    end: Annotated[str, typer.Option(help="UTC end (exclusive)")],
    strategy: Annotated[str, typer.Option(help="threshold | threshold-si | oracle | idle")] = "threshold",
    mechanism: Annotated[str, typer.Option(help="current | new2026")] = "current",
    power_mw: float = 1.0,
    energy_mwh: float = 2.0,
    degradation_eur_per_mwh: float = 8.0,
    minute_lag: int = 1,
    out_dir: Annotated[Path | None, typer.Option(help="write ledger + summary here")] = None,
) -> None:
    """Run one strategy on real data from `data/public` and print the summary (plus oracle capture)."""
    from .replay import MarketData
    from .simulator import simulate
    from .strategies import IdleStrategy, OracleQHStrategy, ThresholdStrategy
    from .types import BatteryParams, PublicationModel

    battery = BatteryParams(power_mw=power_mw, energy_mwh=energy_mwh, degradation_eur_per_mwh=degradation_eur_per_mwh)
    publication = PublicationModel(minute_lag=minute_lag)
    market = MarketData.from_public(start, end, publication=publication)
    signal = "nf_imbalance_price_eur_mwh" if mechanism == "new2026" else "price_cum_eur_mwh"
    factories = {
        "threshold": lambda: ThresholdStrategy(signal_field=signal),
        "threshold-si": lambda: ThresholdStrategy(mode="si", signal_field="si_cum_mw", name="threshold_si"),
        "oracle": lambda: OracleQHStrategy(),
        "idle": lambda: IdleStrategy(),
    }
    if strategy not in factories:
        raise typer.BadParameter(f"unknown strategy `{strategy}`; choose from {sorted(factories)}")
    oracle = simulate(market, OracleQHStrategy(), battery, mechanism=mechanism, record_minutes=False)
    result = simulate(market, factories[strategy](), battery, mechanism=mechanism, publication=publication)
    summary = result.summary(oracle)
    if out_dir is not None:
        result.to_parquet(out_dir, oracle=oracle)
    typer.echo(json.dumps(summary, indent=2, default=str))
