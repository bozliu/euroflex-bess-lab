"""`euroflex live ...`: live paper trading (shadow mode) on Elia near-real-time data. Never submits orders."""

from __future__ import annotations

import json
import logging
import signal
from dataclasses import replace
from pathlib import Path
from typing import Annotated, Any

import pandas as pd
import typer

from ..types import BatteryParams, PublicationModel
from .clock import Clock, SystemClock
from .dryrun import replay_check
from .engine import NEW_FORMULA_GO_LIVE_UTC, SAFETY_STATEMENT, LiveConfig, LiveEngine, PublicDataHistory
from .execution import build_execution
from .feeds import DEFAULT_FEEDS, EliaLiveFeeds, LiveSource
from .reconcile import SOURCES, reconcile_run
from .store import STATUS, RunStore
from .strategies import available_strategies, make_strategy, parse_params

live_app = typer.Typer(
    help="Live paper trading (shadow mode) on Elia near-real-time feeds. Paper only: never submits orders.",
    no_args_is_help=True,
)

DEFAULT_LIVE_ROOT = Path("artifacts") / "live"


def _build_source(poll_seconds: float) -> LiveSource:
    """Real Elia + Energy-Charts feeds (tests replace this factory)."""
    specs = [replace(spec, poll_every_s=poll_seconds) if spec.table == "minute" else spec for spec in DEFAULT_FEEDS]
    return EliaLiveFeeds(specs)


def _build_clock() -> Clock:
    return SystemClock()


def _echo_json(payload: Any) -> None:
    typer.echo(json.dumps(payload, indent=2, default=str))


@live_app.command("run")
def run_command(
    strategy: Annotated[
        str, typer.Option(help="threshold | threshold-si | idle | a STRAT strategy | module:Class")
    ] = "threshold",
    power_mw: Annotated[float, typer.Option(help="Battery power (MW)")] = 1.0,
    energy_mwh: Annotated[float, typer.Option(help="Battery energy (MWh)")] = 2.0,
    mechanism: Annotated[
        str, typer.Option(help="auto (current until the go-live, new2026 after) | current | new2026")
    ] = "auto",
    max_minutes: Annotated[
        int | None, typer.Option(help="Stop after this many decision minutes (default: until Ctrl-C)")
    ] = None,
    run_dir: Annotated[
        Path | None,
        typer.Option(help="Run directory (default artifacts/live/<UTC stamp>_<strategy>); an existing run is resumed"),
    ] = None,
    execution: Annotated[
        str, typer.Option(help="paper (log only) | csv (setpoint file for an EMS) | webhook")
    ] = "paper",
    setpoint_csv: Annotated[
        Path | None, typer.Option(help="CSV path for --execution csv (default <run-dir>/setpoints.csv)")
    ] = None,
    webhook_url: Annotated[str | None, typer.Option(help="Your EMS gateway URL for --execution webhook")] = None,
    enable_webhook: Annotated[
        bool, typer.Option(help="Required to actually POST setpoints with --execution webhook")
    ] = False,
    param: Annotated[
        list[str] | None, typer.Option("--param", help="Strategy parameter key=value (repeatable)")
    ] = None,
    minute_lag: Annotated[int, typer.Option(help="PublicationModel.minute_lag (minutes)")] = 1,
    qh_final_lag: Annotated[int, typer.Option(help="PublicationModel.qh_final_lag_minutes")] = 5,
    history_days: Annotated[
        float, typer.Option(help="Days of quarter-hour price history in the rolling window")
    ] = 14.0,
    poll_seconds: Annotated[float, typer.Option(help="Polling interval of the minute feeds (seconds)")] = 15.0,
    degradation_eur_per_mwh: Annotated[float, typer.Option(help="Degradation cost per MWh discharged")] = 8.0,
    initial_soc_frac: Annotated[float, typer.Option(help="Initial state of charge (fraction)")] = 0.5,
    finish_quarter_hour: Annotated[
        bool, typer.Option(help="After --max-minutes, keep deciding until the quarter-hour ends")
    ] = True,
    drain_minutes: Annotated[
        float, typer.Option(help="After the last decision, wait up to this long for final prices")
    ] = 10.0,
    public_history: Annotated[bool, typer.Option(help="Seed the window from data/public when it covers it")] = True,
    go_live: Annotated[
        str, typer.Option(help="UTC instant of the new-formula go-live used by --mechanism auto")
    ] = NEW_FORMULA_GO_LIVE_UTC.isoformat(),
    replan_every: Annotated[
        str, typer.Option(help="never | minute: re-run strategy.reset() on visible data before every decision")
    ] = "never",
    verbose: Annotated[bool, typer.Option(help="Log every paper setpoint")] = False,
) -> None:
    """Paper-trade a strategy minute by minute on live Elia data and settle a paper ledger per quarter-hour."""
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING, format="%(asctime)s %(levelname)s %(message)s"
    )
    if mechanism not in ("auto", "current", "new2026"):
        raise typer.BadParameter("--mechanism must be auto, current or new2026")
    if replan_every not in ("never", "minute"):
        raise typer.BadParameter("--replan-every must be never or minute")
    try:
        strategy_obj, label = make_strategy(strategy, mechanism=mechanism, params=parse_params(param))
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    battery = BatteryParams(
        power_mw=power_mw,
        energy_mwh=energy_mwh,
        degradation_eur_per_mwh=degradation_eur_per_mwh,
        initial_soc_frac=initial_soc_frac,
    )
    stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    target = run_dir or DEFAULT_LIVE_ROOT / f"{stamp}_{label}"
    try:
        adapter = build_execution(
            execution, run_dir=target, setpoint_csv=setpoint_csv, webhook_url=webhook_url, enable_webhook=enable_webhook
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    go_live_ts = pd.Timestamp(go_live)
    config = LiveConfig(
        mechanism=mechanism,  # type: ignore[arg-type]
        publication=PublicationModel(minute_lag=minute_lag, qh_final_lag_minutes=qh_final_lag),
        go_live_utc=go_live_ts.tz_localize("UTC") if go_live_ts.tzinfo is None else go_live_ts.tz_convert("UTC"),
        history_days=history_days,
        drain_minutes=drain_minutes,
        replan_every=replan_every,  # type: ignore[arg-type]
    )
    engine = LiveEngine(
        strategy_obj,
        battery,
        source=_build_source(poll_seconds),
        run_dir=target,
        execution=adapter,
        clock=_build_clock(),
        config=config,
        strategy_label=label,
        history=PublicDataHistory(minute_hours=config.minute_history_hours) if public_history else None,
        metadata={"launcher": "cli", "strategy_spec": strategy, "params": parse_params(param)},
    )
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: engine.request_stop())
    typer.echo(f"{SAFETY_STATEMENT}\nrun directory: {target.as_posix()}")
    try:
        summary = engine.run(max_minutes=max_minutes, finish_quarter_hour=finish_quarter_hour)
    finally:
        signal.signal(signal.SIGTERM, previous)
    _echo_json(summary)


@live_app.command("status")
def status_command(
    run_dir: Annotated[Path, typer.Argument(help="Live run directory")],
    as_json: Annotated[bool, typer.Option("--json", help="Print the raw status.json")] = False,
) -> None:
    """Heartbeat, paper P&L and feed health of a live run."""
    store = RunStore(run_dir, fsync=False)
    status = store.read_json(STATUS)
    if status is None:
        raise typer.BadParameter(f"no {STATUS} in {run_dir.as_posix()}")
    heartbeat = pd.Timestamp(status["heartbeat_utc"])
    age_s = (pd.Timestamp.now(tz="UTC") - heartbeat).total_seconds()
    if as_json:
        _echo_json({**status, "heartbeat_age_s": round(age_s, 1)})
        return
    ledger = status.get("ledger", {})
    counters = status.get("counters", {})
    lines = [
        f"run {status.get('run_id')}  strategy={status.get('strategy')}  state={status.get('state')}  mode=paper",
        f"heartbeat {status.get('heartbeat_utc')} ({age_s:.0f} s ago)  last minute {status.get('last_minute_utc')}",
        f"mechanism setting={status.get('mechanism_setting')} now={status.get('mechanism_now')}  "
        f"SOC {status.get('soc_mwh')} MWh ({100 * float(status.get('soc_frac') or 0):.1f} %)",
        f"decisions {counters.get('decisions', 0)}  missed {counters.get('missed', 0)}  "
        f"gap decisions {counters.get('gap_decisions', 0)}  strategy errors {counters.get('strategy_errors', 0)}",
        f"paper P&L {ledger.get('pnl_eur', 0.0):.2f} EUR over {ledger.get('settled_qh', 0)} settled QH "
        f"(revenue {ledger.get('revenue_eur', 0.0):.2f}, degradation {ledger.get('degradation_eur', 0.0):.2f}); "
        f"awaiting price: {len(status.get('awaiting_price', []))}",
        "feeds:",
    ]
    for name, feed in (status.get("feeds") or {}).items():
        latency = feed.get("latency_upper_s") or {}
        lines.append(
            f"  {name:<26} {feed.get('dataset_id', ''):<24} latest {feed.get('latest_row_utc')}  "
            f"staleness {feed.get('staleness_s')} s  latency p50/p90 {latency.get('p50')}/{latency.get('p90')} s  "
            f"errors {feed.get('errors')}  gaps {feed.get('gaps', {}).get('count')}"
        )
    typer.echo("\n".join(lines))


@live_app.command("reconcile")
def reconcile_command(
    run_dir: Annotated[Path, typer.Argument(help="Live run directory")],
    source: Annotated[str, typer.Option(help="auto (public, api, nrt) | public | api | nrt")] = "auto",
    output_dir: Annotated[
        Path | None, typer.Option(help="Where to write the reconciliation (default <run-dir>/reconcile)")
    ] = None,
    minutes: Annotated[bool, typer.Option(help="Also compare minute signals with the validated minute table")] = True,
    current_dataset: Annotated[
        str, typer.Option(help="Elia dataset with validated current-formula QH prices")
    ] = "ods134",
    new_formula_dataset: Annotated[
        str, typer.Option(help="Elia dataset with new-formula QH prices (settlement after the go-live)")
    ] = "ods212",
) -> None:
    """Compare the paper ledger with validated Elia data (price revisions, missing prices, missed minutes).

    Quarter-hours settled under the new formula (after the go-live) are checked against ``ods212``.
    """
    sources = SOURCES if source == "auto" else (source,)
    if any(s not in SOURCES for s in sources):
        raise typer.BadParameter(f"--source must be auto or one of {SOURCES}")
    result = reconcile_run(
        run_dir,
        sources=sources,
        include_minutes=minutes,
        output_dir=output_dir,
        current_dataset=current_dataset,
        new_formula_dataset=new_formula_dataset,
    )
    _echo_json(result.summary)


@live_app.command("replay")
def replay_command(
    start: Annotated[str, typer.Option(help="UTC start, quarter-hour aligned, e.g. 2026-10-13T22:00:00Z")],
    end: Annotated[str, typer.Option(help="UTC end (exclusive)")],
    strategy: Annotated[str, typer.Option(help="Strategy name or module:Class (see `live strategies`)")] = "threshold",
    power_mw: Annotated[float, typer.Option(help="Battery power (MW)")] = 1.0,
    energy_mwh: Annotated[float, typer.Option(help="Battery energy (MWh)")] = 2.0,
    mechanism: Annotated[str, typer.Option(help="current | new2026 (Jan-Jul 2026 simulation data)")] = "current",
    param: Annotated[
        list[str] | None, typer.Option("--param", help="Strategy parameter key=value (repeatable)")
    ] = None,
    minute_lag: Annotated[int, typer.Option(help="PublicationModel.minute_lag (minutes)")] = 1,
    replan_every: Annotated[str, typer.Option(help="never | minute")] = "never",
    run_dir: Annotated[Path | None, typer.Option(help="Keep the replayed run directory here")] = None,
) -> None:
    """Dry run on data/public: the live engine must reproduce the backtest decisions and ledger exactly."""
    if mechanism not in ("current", "new2026"):
        raise typer.BadParameter("--mechanism must be current or new2026 for a replay")
    if replan_every not in ("never", "minute"):
        raise typer.BadParameter("--replan-every must be never or minute")
    params = parse_params(param)
    try:
        make_strategy(strategy, mechanism=mechanism, params=params)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    battery = BatteryParams(power_mw=power_mw, energy_mwh=energy_mwh)
    report = replay_check(
        lambda: make_strategy(strategy, mechanism=mechanism, params=params)[0],
        battery,
        start,
        end,
        mechanism=mechanism,  # type: ignore[arg-type]
        publication=PublicationModel(minute_lag=minute_lag),
        replan_every=replan_every,  # type: ignore[arg-type]
        run_dir=run_dir,
    )
    _echo_json(report)
    if not report["identical_decisions"]:
        raise typer.Exit(code=1)


@live_app.command("strategies")
def strategies_command() -> None:
    """List strategies that can be paper-traded live (and optional ones that are not available)."""
    registry, unavailable = available_strategies()
    for name, info in sorted(registry.items()):
        typer.echo(f"{name:<16} {info.origin:<48} {info.description}")
    for module, reason in sorted(unavailable.items()):
        typer.echo(f"(skipped) {module:<10} {reason}")
