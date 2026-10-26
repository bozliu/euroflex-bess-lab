"""``value`` command group: contract valuation from ledgers (parquet / CSV + optional ``.meta.json`` sidecar).

Register in the main CLI with ``app.add_typer(value_app, name="value")``. Every command writes a JSON file
(machine-readable, full precision) and a markdown summary into ``--out-dir``.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from pathlib import Path
from typing import Annotated, Any, cast

import pandas as pd
import typer

from .attribution import WaterfallResult, mechanism_decomposition, value_waterfall
from .contracts import (
    ContractResult,
    RiskMeasure,
    RiskProfile,
    price_floor,
    price_partial_toll,
    price_revenue_swap,
    price_toll,
    solve_floor_upside_share,
    toll_sensitivity,
)
from .plots import plot_annual_distribution, plot_portfolio_risk, plot_toll_sensitivity, plot_waterfall
from .portfolio import PortfolioAsset, portfolio_risk
from .report import (
    md_table,
    render_contract,
    render_portfolio,
    render_revenue_summaries,
    render_scenarios,
    render_waterfall,
    write_json,
    write_markdown,
)
from .revenue import (
    LedgerMetadata,
    RevenueSummary,
    daily_pnl,
    infer_interval_minutes,
    read_ledger,
    summarize_daily,
)
from .scenarios import (
    DEFAULT_SEED,
    BootstrapConfig,
    BootstrapMethod,
    CapacityValueCurve,
    FleetPath,
    SaturationSpec,
    ScenarioConfig,
    ScenarioSet,
    SkillDecay,
    daily_panel,
    generate_scenarios,
    load_capacity_value_curve,
)

value_app = typer.Typer(
    help="Contract valuation from simulated ledgers: summaries, revenue scenarios, tolls, floors, partial tolls, "
    "swaps, value attribution and portfolio risk.",
    no_args_is_help=True,
)

# ------------------------------------------------------------------------------------------------ option types
LedgerOpt = Annotated[
    list[str],
    typer.Option(
        "--ledger",
        "-l",
        help="Ledger file (.parquet/.csv). Repeat as REGIME=PATH (e.g. current=be.parquet, new2026=be_new.parquet) "
        "to mix settlement regimes.",
    ),
]
OutDirOpt = Annotated[Path, typer.Option("--out-dir", "-o", help="Directory for the JSON + markdown outputs.")]
PowerOpt = Annotated[
    float | None, typer.Option("--power-mw", help="Battery power behind the ledgers in MW (overrides the sidecar).")
]
MarketOpt = Annotated[
    str | None, typer.Option("--market", help="Market code for local delivery days: BE, DE-LU, NL (default BE).")
]
TimezoneOpt = Annotated[str | None, typer.Option("--timezone", help="Explicit local timezone (overrides --market).")]
ValueOpt = Annotated[str, typer.Option("--value", help="Quantity to value: `pnl` (net of wear) or `revenue` (gross).")]
CoverageOpt = Annotated[float, typer.Option("--min-coverage", help="Minimum interval coverage of a usable day.")]
TenorOpt = Annotated[int, typer.Option("--tenor-years", help="Contract tenor in years.")]
StartYearOpt = Annotated[int, typer.Option("--start-year", help="First calendar year of the contract.")]
PathsOpt = Annotated[int, typer.Option("--paths", help="Number of simulated revenue paths.")]
SeedOpt = Annotated[int, typer.Option("--seed", help="Random seed (reproduces every path).")]
BootstrapOpt = Annotated[str, typer.Option("--bootstrap", help="Day resampling: block, stationary or iid.")]
BlockOpt = Annotated[int, typer.Option("--block-days", help="Block length (mean length for stationary).")]
WindowOpt = Annotated[
    int, typer.Option("--season-window-days", help="Seasonal window +/- days around the simulated day (-1 = off).")
]
WeekdayOpt = Annotated[
    bool, typer.Option("--align-weekday/--no-align-weekday", help="Start blocks on the same weekday.")
]
WeightsOpt = Annotated[
    str | None, typer.Option("--weights", help="Regime probabilities, e.g. `current=0.3,new2026=0.7`.")
]
PersistenceOpt = Annotated[
    float, typer.Option("--regime-persistence", help="Probability a path keeps last year's regime (1 = per path).")
]
CurveOpt = Annotated[
    list[str] | None,
    typer.Option(
        "--capacity-curve",
        help="Capacity-value curve file (fleet_mw, value_eur_per_mw_year) or `placeholder`; repeat as REGIME=PATH "
        "for mechanism-specific curves. Enables saturation (needs --fleet).",
    ),
]
FleetOpt = Annotated[
    str | None,
    typer.Option("--fleet", help="Implicit-reaction fleet MW by year on the curve axis, e.g. `2027=900,2030=2200`."),
]
FleetSigmaOpt = Annotated[float, typer.Option("--fleet-log-sigma", help="Persistent build-out uncertainty per path.")]
ReferenceFleetOpt = Annotated[
    float, typer.Option("--reference-fleet-mw", help="Fleet MW already reflected in the history ledgers.")
]
SkillShareOpt = Annotated[float, typer.Option("--skill-share", help="Share of revenue attributable to skill.")]
SkillDecayOpt = Annotated[float, typer.Option("--skill-decay", help="Yearly decay of the skill share.")]
FadeOpt = Annotated[float, typer.Option("--capacity-fade", help="Yearly capacity fade (fraction).")]
LevelSigmaOpt = Annotated[
    float,
    typer.Option("--level-log-sigma", help="Yearly market-level shock (lognormal sigma) for inter-annual uncertainty."),
]
LevelPersistenceOpt = Annotated[
    float, typer.Option("--level-persistence", help="AR(1) persistence of the market-level shock across years.")
]
AvailabilityOpt = Annotated[
    float, typer.Option("--availability-factor", help="Availability relative to the ledger (1 = as simulated).")
]
MeasureOpt = Annotated[str, typer.Option(help="Risk measure: neutral, cvar or exponential.")]
AlphaOpt = Annotated[float, typer.Option(help="CVaR tail probability.")]
WeightOpt = Annotated[float, typer.Option(help="Weight on CVaR in the mean-CVaR certainty equivalent.")]
ToleranceOpt = Annotated[float | None, typer.Option(help="Risk tolerance in EUR for exponential utility.")]
DiscountOpt = Annotated[float, typer.Option(help="Discount rate.")]
MarginOpt = Annotated[float, typer.Option(help="Required margin in EUR/MW/yr.")]
DealOpt = Annotated[float, typer.Option("--deal-mw", help="Contract size in MW (scales totals and CARA risk).")]
CaptureOpt = Annotated[
    float, typer.Option("--owner-capture", help="Share of simulated merchant revenue the owner would capture alone.")
]
PriceOpt = Annotated[
    float | None, typer.Option("--price", help="Quoted price in EUR/MW/yr; omit to solve the fair price.")
]
EscalationOpt = Annotated[float, typer.Option("--escalation", help="Yearly escalation of the fixed payment.")]
PathsOutOpt = Annotated[
    bool, typer.Option("--write-paths/--no-write-paths", help="Also write every annual path (CSV).")
]
FiguresOpt = Annotated[bool, typer.Option("--figures/--no-figures", help="Also write PNG figures.")]


@dataclass(frozen=True)
class _ScenarioOptions:
    ledger: list[str]
    power_mw: float | None
    market: str | None
    timezone: str | None
    value: str
    min_coverage: float
    tenor_years: int
    start_year: int
    paths: int
    seed: int
    bootstrap: str
    block_days: int
    season_window_days: int
    align_weekday: bool
    weights: str | None
    regime_persistence: float
    capacity_curve: list[str] | None
    fleet: str | None
    fleet_log_sigma: float
    reference_fleet_mw: float
    skill_share: float
    skill_decay: float
    capacity_fade: float
    availability_factor: float
    level_log_sigma: float
    level_persistence: float

    @classmethod
    def pick(cls, values: dict[str, Any]) -> _ScenarioOptions:
        return cls(**{f.name: values[f.name] for f in fields(cls)})


@dataclass(frozen=True)
class _LoadedLedger:
    name: str
    path: Path
    ledger: pd.DataFrame
    metadata: LedgerMetadata
    daily: pd.DataFrame
    summary: RevenueSummary


# ------------------------------------------------------------------------------------------------ helpers
def _display_path(path: Path) -> str:
    """Path for reports: relative when possible, never an absolute local path."""
    if not path.is_absolute():
        return path.as_posix()
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return path.name


def _default_name(path: Path) -> str:
    """File stem, or the run directory for generic names such as ``qh_ledger.parquet``."""
    stem = path.name.split(".")[0]
    if stem in {"qh_ledger", "ledger", "settlement_breakdown"} and path.parent.name:
        return path.parent.name
    return stem


def _named_paths(specs: list[str], default: str | None = "base") -> dict[str, Path]:
    """``NAME=PATH`` specs; a bare path is named ``default`` (single spec) or after its file stem."""
    named: dict[str, Path] = {}
    for spec in specs:
        name, sep, raw = spec.partition("=")
        if not sep:
            name, raw = (default if default and len(specs) == 1 else _default_name(Path(spec))), spec
        name = name.strip()
        if name in named:
            raise typer.BadParameter(f"duplicate name `{name}` in {specs}")
        named[name] = Path(raw.strip())
    return named


def _float_map(text: str | None) -> dict[str, float]:
    if not text:
        return {}
    out: dict[str, float] = {}
    for item in text.split(","):
        key, sep, raw = item.partition("=")
        if not sep:
            raise typer.BadParameter(f"expected NAME=VALUE pairs, got `{item}`")
        out[key.strip()] = float(raw)
    return out


def _load(name: str, path: Path, *, min_coverage: float = 0.98, **overrides: Any) -> _LoadedLedger:
    if not path.exists():
        raise typer.BadParameter(f"ledger file not found: {_display_path(path)}")
    try:
        ledger, metadata = read_ledger(path, **{k: v for k, v in overrides.items() if v is not None})
        if metadata.source == path.name:
            metadata = metadata.with_overrides(source=_display_path(path))
        power = metadata.require_power_mw()
        step = infer_interval_minutes(ledger["timestamp_utc"])
        daily = daily_pnl(ledger, timezone=metadata.local_timezone, power_mw=power, interval_minutes=step)
        summary = summarize_daily(
            daily, metadata.with_overrides(asset_id=name), min_coverage=min_coverage, interval_minutes=step
        )
    except ValueError as exc:
        raise typer.BadParameter(f"{_display_path(path)}: {exc}") from exc
    return _LoadedLedger(name=name, path=path, ledger=ledger, metadata=metadata, daily=daily, summary=summary)


def _value_column(value: str) -> str:
    if value not in ("pnl", "revenue"):
        raise typer.BadParameter("--value must be `pnl` or `revenue`")
    return f"{value}_eur_per_mw"


def _risk(
    prefix: str, measure: str, alpha: float, weight: float, tolerance: float | None, discount: float, margin: float
) -> RiskProfile:
    if measure not in ("neutral", "cvar", "exponential"):
        raise typer.BadParameter(f"--{prefix}-measure must be neutral, cvar or exponential")
    try:
        return RiskProfile(
            name=prefix,
            measure=cast(RiskMeasure, measure),
            cvar_alpha=alpha,
            cvar_weight=weight,
            risk_tolerance_eur=tolerance,
            discount_rate=discount,
            required_margin_eur_per_mw_year=margin,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _curves(specs: list[str] | None, regimes: list[str]) -> CapacityValueCurve | dict[str, CapacityValueCurve] | None:
    if not specs:
        return None
    if len(specs) == 1 and "=" not in specs[0]:
        return load_capacity_value_curve(specs[0])
    curves: dict[str, CapacityValueCurve] = {}
    for spec in specs:
        name, sep, raw = spec.partition("=")
        if not sep:
            raise typer.BadParameter("repeat --capacity-curve as REGIME=PATH when giving several curves")
        if name not in regimes and name != "default":
            raise typer.BadParameter(f"curve for unknown regime `{name}` (regimes: {regimes})")
        curves[name] = load_capacity_value_curve(raw)
    return curves


def _scenario_config(opts: _ScenarioOptions, regimes: list[str]) -> ScenarioConfig:
    if opts.bootstrap not in ("block", "stationary", "iid"):
        raise typer.BadParameter("--bootstrap must be block, stationary or iid")
    method = cast(BootstrapMethod, opts.bootstrap)
    curves = _curves(opts.capacity_curve, regimes)
    saturation = None
    if curves is not None:
        points = {int(k): v for k, v in _float_map(opts.fleet).items()}
        if not points:
            raise typer.BadParameter("--capacity-curve needs --fleet (e.g. `2027=900,2030=2200`)")
        saturation = SaturationSpec(
            curve=curves,
            fleet=FleetPath.from_points(points, path_log_sigma=opts.fleet_log_sigma),
            reference_fleet_mw=opts.reference_fleet_mw,
        )
    skill = SkillDecay(opts.skill_share, opts.skill_decay) if opts.skill_share > 0.0 else None
    try:
        return ScenarioConfig(
            n_paths=opts.paths,
            n_years=opts.tenor_years,
            start_year=opts.start_year,
            seed=opts.seed,
            bootstrap=BootstrapConfig(
                method=method,
                block_days=opts.block_days,
                season_window_days=None if opts.season_window_days < 0 else opts.season_window_days,
                align_weekday=opts.align_weekday,
            ),
            regime_persistence=opts.regime_persistence,
            saturation=saturation,
            skill=skill,
            capacity_fade_per_year=opts.capacity_fade,
            availability_factor=opts.availability_factor,
            level_log_sigma=opts.level_log_sigma,
            level_persistence=opts.level_persistence,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _scenarios(opts: _ScenarioOptions, reference: list[str] | None = None) -> tuple[ScenarioSet, list[_LoadedLedger]]:
    regimes = _named_paths(opts.ledger)
    overrides = {"power_mw": opts.power_mw, "market": opts.market, "timezone": opts.timezone}
    loaded = [_load(name, path, min_coverage=opts.min_coverage, **overrides) for name, path in regimes.items()]
    column = _value_column(opts.value)
    histories: dict[str, pd.DataFrame] = {}
    refs = _named_paths(reference) if reference else {}
    if refs and set(refs) != set(regimes):
        if len(refs) == 1 and len(regimes) == 1:
            refs = {next(iter(regimes)): next(iter(refs.values()))}
        else:
            raise typer.BadParameter(f"--reference-ledger needs the same regimes as --ledger ({list(regimes)})")
    references: list[_LoadedLedger] = []
    for item in loaded:
        if refs:
            ref = _load(f"{item.name}_reference", refs[item.name], min_coverage=opts.min_coverage, **overrides)
            references.append(ref)
            histories[item.name] = daily_panel(
                {"asset": item.daily, "reference": ref.daily}, value_column=column, min_coverage=opts.min_coverage
            )
        else:
            histories[item.name] = item.daily
    loaded = loaded + references
    weights = _float_map(opts.weights) or None
    config = _scenario_config(opts, list(regimes))
    try:
        scenarios = generate_scenarios(
            histories, config, weights=weights, value_column=column, min_coverage=opts.min_coverage, primary="asset"
        )
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    return scenarios, loaded


def _inputs_markdown(loaded: list[_LoadedLedger]) -> str:
    rows = []
    for item in loaded:
        m = item.metadata
        rows.append(
            [
                item.name,
                f"`{_display_path(item.path)}`",
                m.market,
                m.mechanism or "-",
                m.strategy or "-",
                f"{m.power_mw:g} MW / {m.energy_mwh:g} MWh" if m.energy_mwh else f"{m.power_mw:g} MW",
                f"{m.fleet_mw:,.0f}",
                m.publication_lag or "-",
                f"{m.degradation_eur_per_mwh:g} EUR/MWh" if m.degradation_eur_per_mwh is not None else "-",
                "yes" if m.synthetic else "no",
            ]
        )
    return md_table(
        ["input", "file", "market", "mechanism", "strategy", "battery", "fleet MW", "lag", "wear cost", "synthetic"],
        rows,
        align="llllllrlll",
    )


def _banner(loaded: list[_LoadedLedger]) -> str:
    if any(item.metadata.synthetic for item in loaded):
        return "> **SYNTHETIC INPUTS** - at least one ledger is synthetic; numbers are illustrative, not evidence.\n\n"
    return ""


def _write_contract(
    kind: str,
    title: str,
    result: ContractResult,
    scenarios: ScenarioSet,
    loaded: list[_LoadedLedger],
    out_dir: Path,
    *,
    write_paths: bool,
    figures: bool,
    sensitivity: pd.DataFrame | None = None,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "contract": result.to_dict(),
        "scenarios": scenarios.summary(),
        "inputs": [
            {
                "name": item.name,
                "file": _display_path(item.path),
                "metadata": item.metadata.to_dict(),
                "summary": item.summary.to_dict(),
            }
            for item in loaded
        ],
    }
    if sensitivity is not None:
        payload["toll_sensitivity"] = sensitivity.to_dict(orient="records")
    write_json(payload, out_dir / f"{kind}.json")
    parts = [
        f"# {title}",
        _banner(loaded) + "## Inputs\n\n" + _inputs_markdown(loaded),
        "## Historical revenue\n\n" + render_revenue_summaries([item.summary for item in loaded]),
        "## Revenue scenarios\n\n" + render_scenarios(scenarios),
        "## Contract\n\n" + render_contract(result),
    ]
    if figures:
        plot_annual_distribution(
            scenarios,
            out_dir / f"{kind}_annual_distribution.png",
            reference_lines={result.price_name: result.price_eur_per_mw_year}
            if kind != "floor"
            else {"floor": result.terms["floor_eur_per_mw_year"]},
        )
        parts.append(f"![annual distribution]({kind}_annual_distribution.png)")
        if sensitivity is not None:
            plot_toll_sensitivity(
                sensitivity,
                out_dir / f"{kind}_sensitivity.png",
                fair_toll=result.price_eur_per_mw_year,
                break_even=result.break_even_eur_per_mw_year,
            )
            parts.append(f"![toll sensitivity]({kind}_sensitivity.png)")
    write_markdown("\n\n".join(parts), out_dir / f"{kind}.md")
    if write_paths:
        scenarios.to_frame().to_csv(out_dir / f"{kind}_annual_paths.csv", index=False)
    typer.echo(
        f"{title}: {result.price_name} {result.price_eur_per_mw_year:,.0f} EUR/MW/yr -> {_display_path(out_dir)}"
    )


# ------------------------------------------------------------------------------------------------ commands
@value_app.command("summary")
def summary_command(
    ledger: LedgerOpt,
    out_dir: OutDirOpt,
    power_mw: PowerOpt = None,
    market: MarketOpt = None,
    timezone: TimezoneOpt = None,
    min_coverage: CoverageOpt = 0.98,
) -> None:
    """Daily P&L and per-MW-year statistics of one or more ledgers."""
    out_dir.mkdir(parents=True, exist_ok=True)
    loaded = []
    for name, path in _named_paths(ledger, default=None).items():
        item = _load(name, path, min_coverage=min_coverage, power_mw=power_mw, market=market, timezone=timezone)
        item.daily.to_csv(out_dir / f"daily_{name}.csv", index=False)
        loaded.append(item)
    write_json(
        {
            "ledgers": [
                {
                    "name": i.name,
                    "file": _display_path(i.path),
                    "metadata": i.metadata.to_dict(),
                    "summary": i.summary.to_dict(),
                }
                for i in loaded
            ]
        },
        out_dir / "summary.json",
    )
    text = (
        "# Ledger summary\n\n"
        + _banner(loaded)
        + _inputs_markdown(loaded)
        + "\n\n"
        + render_revenue_summaries([i.summary for i in loaded])
    )
    write_markdown(text, out_dir / "summary.md")
    typer.echo(f"summarised {len(loaded)} ledger(s) -> {_display_path(out_dir)}")


@value_app.command("scenarios")
def scenarios_command(
    ledger: LedgerOpt,
    out_dir: OutDirOpt,
    power_mw: PowerOpt = None,
    market: MarketOpt = None,
    timezone: TimezoneOpt = None,
    value: ValueOpt = "pnl",
    min_coverage: CoverageOpt = 0.98,
    tenor_years: TenorOpt = 7,
    start_year: StartYearOpt = 2027,
    paths: PathsOpt = 2000,
    seed: SeedOpt = DEFAULT_SEED,
    bootstrap: BootstrapOpt = "block",
    block_days: BlockOpt = 7,
    season_window_days: WindowOpt = 30,
    align_weekday: WeekdayOpt = True,
    weights: WeightsOpt = None,
    regime_persistence: PersistenceOpt = 1.0,
    capacity_curve: CurveOpt = None,
    fleet: FleetOpt = None,
    fleet_log_sigma: FleetSigmaOpt = 0.0,
    reference_fleet_mw: ReferenceFleetOpt = 0.0,
    skill_share: SkillShareOpt = 0.0,
    skill_decay: SkillDecayOpt = 0.0,
    capacity_fade: FadeOpt = 0.0,
    availability_factor: AvailabilityOpt = 1.0,
    level_log_sigma: LevelSigmaOpt = 0.0,
    level_persistence: LevelPersistenceOpt = 0.0,
    write_paths: PathsOutOpt = False,
    figures: FiguresOpt = True,
) -> None:
    """Multi-year annual revenue distribution (bootstrap x regimes x saturation x skill decay)."""
    opts = _ScenarioOptions.pick(locals())
    scenarios, loaded = _scenarios(opts)
    out_dir.mkdir(parents=True, exist_ok=True)
    write_json(
        {"scenarios": scenarios.summary(), "inputs": [i.summary.to_dict() for i in loaded]}, out_dir / "scenarios.json"
    )
    parts = [
        "# Revenue scenarios",
        _banner(loaded) + "## Inputs\n\n" + _inputs_markdown(loaded),
        "## Historical revenue\n\n" + render_revenue_summaries([i.summary for i in loaded]),
        "## Scenarios\n\n" + render_scenarios(scenarios),
    ]
    if figures:
        plot_annual_distribution(scenarios, out_dir / "scenarios_annual_distribution.png")
        parts.append("![annual distribution](scenarios_annual_distribution.png)")
    write_markdown("\n\n".join(parts), out_dir / "scenarios.md")
    if write_paths:
        scenarios.to_frame().to_csv(out_dir / "scenarios_annual_paths.csv", index=False)
    typer.echo(f"scenarios: {scenarios.n_paths} paths x {scenarios.n_years} years -> {_display_path(out_dir)}")


@value_app.command("toll")
def toll_command(
    ledger: LedgerOpt,
    out_dir: OutDirOpt,
    price: PriceOpt = None,
    escalation: EscalationOpt = 0.0,
    deal_mw: DealOpt = 1.0,
    owner_capture: CaptureOpt = 1.0,
    offtaker_measure: MeasureOpt = "cvar",
    offtaker_alpha: AlphaOpt = 0.10,
    offtaker_weight: WeightOpt = 0.3,
    offtaker_tolerance_eur: ToleranceOpt = None,
    offtaker_discount: DiscountOpt = 0.08,
    offtaker_margin: MarginOpt = 0.0,
    owner_measure: MeasureOpt = "cvar",
    owner_alpha: AlphaOpt = 0.10,
    owner_weight: WeightOpt = 0.6,
    owner_tolerance_eur: ToleranceOpt = None,
    owner_discount: DiscountOpt = 0.08,
    owner_margin: MarginOpt = 0.0,
    power_mw: PowerOpt = None,
    market: MarketOpt = None,
    timezone: TimezoneOpt = None,
    value: ValueOpt = "pnl",
    min_coverage: CoverageOpt = 0.98,
    tenor_years: TenorOpt = 7,
    start_year: StartYearOpt = 2027,
    paths: PathsOpt = 2000,
    seed: SeedOpt = DEFAULT_SEED,
    bootstrap: BootstrapOpt = "block",
    block_days: BlockOpt = 7,
    season_window_days: WindowOpt = 30,
    align_weekday: WeekdayOpt = True,
    weights: WeightsOpt = None,
    regime_persistence: PersistenceOpt = 1.0,
    capacity_curve: CurveOpt = None,
    fleet: FleetOpt = None,
    fleet_log_sigma: FleetSigmaOpt = 0.0,
    reference_fleet_mw: ReferenceFleetOpt = 0.0,
    skill_share: SkillShareOpt = 0.0,
    skill_decay: SkillDecayOpt = 0.0,
    capacity_fade: FadeOpt = 0.0,
    availability_factor: AvailabilityOpt = 1.0,
    level_log_sigma: LevelSigmaOpt = 0.0,
    level_persistence: LevelPersistenceOpt = 0.0,
    write_paths: PathsOutOpt = False,
    figures: FiguresOpt = True,
) -> None:
    """Fixed toll: fair price (offtaker certainty equivalent), break-even, deal zone and both views."""
    values = locals()
    opts = _ScenarioOptions.pick(values)
    offtaker, owner = _profiles(values)
    scenarios, loaded = _scenarios(opts)
    result = price_toll(
        scenarios,
        offtaker=offtaker,
        owner=owner,
        toll_eur_per_mw_year=price,
        escalation_per_year=escalation,
        deal_mw=deal_mw,
        owner_capture=owner_capture,
    )
    grid = [result.break_even_eur_per_mw_year * f for f in (0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3)]
    sensitivity = toll_sensitivity(scenarios, grid, offtaker=offtaker, escalation_per_year=escalation, deal_mw=deal_mw)
    _write_contract(
        "toll",
        "Toll valuation",
        result,
        scenarios,
        loaded,
        out_dir,
        write_paths=write_paths,
        figures=figures,
        sensitivity=sensitivity,
    )


@value_app.command("floor")
def floor_command(
    ledger: LedgerOpt,
    out_dir: OutDirOpt,
    floor: Annotated[float, typer.Option("--floor", help="Guaranteed annual revenue in EUR/MW/yr.")],
    price: PriceOpt = None,
    upside_share: Annotated[
        float, typer.Option("--upside-share", help="Share of revenue above the threshold paid to the provider.")
    ] = 0.0,
    upside_threshold: Annotated[
        float | None, typer.Option("--upside-threshold", help="Upside threshold (default: the floor).")
    ] = None,
    solve_upside_share: Annotated[
        bool, typer.Option("--solve-upside-share", help="Solve the upside share for a zero premium.")
    ] = False,
    deal_mw: DealOpt = 1.0,
    offtaker_measure: MeasureOpt = "cvar",
    offtaker_alpha: AlphaOpt = 0.10,
    offtaker_weight: WeightOpt = 0.3,
    offtaker_tolerance_eur: ToleranceOpt = None,
    offtaker_discount: DiscountOpt = 0.08,
    offtaker_margin: MarginOpt = 0.0,
    owner_measure: MeasureOpt = "cvar",
    owner_alpha: AlphaOpt = 0.10,
    owner_weight: WeightOpt = 0.6,
    owner_tolerance_eur: ToleranceOpt = None,
    owner_discount: DiscountOpt = 0.08,
    owner_margin: MarginOpt = 0.0,
    power_mw: PowerOpt = None,
    market: MarketOpt = None,
    timezone: TimezoneOpt = None,
    value: ValueOpt = "pnl",
    min_coverage: CoverageOpt = 0.98,
    tenor_years: TenorOpt = 7,
    start_year: StartYearOpt = 2027,
    paths: PathsOpt = 2000,
    seed: SeedOpt = DEFAULT_SEED,
    bootstrap: BootstrapOpt = "block",
    block_days: BlockOpt = 7,
    season_window_days: WindowOpt = 30,
    align_weekday: WeekdayOpt = True,
    weights: WeightsOpt = None,
    regime_persistence: PersistenceOpt = 1.0,
    capacity_curve: CurveOpt = None,
    fleet: FleetOpt = None,
    fleet_log_sigma: FleetSigmaOpt = 0.0,
    reference_fleet_mw: ReferenceFleetOpt = 0.0,
    skill_share: SkillShareOpt = 0.0,
    skill_decay: SkillDecayOpt = 0.0,
    capacity_fade: FadeOpt = 0.0,
    availability_factor: AvailabilityOpt = 1.0,
    level_log_sigma: LevelSigmaOpt = 0.0,
    level_persistence: LevelPersistenceOpt = 0.0,
    write_paths: PathsOutOpt = False,
    figures: FiguresOpt = True,
) -> None:
    """Revenue floor (annual put) with optional upside sharing; both views and the zero-premium share."""
    values = locals()
    opts = _ScenarioOptions.pick(values)
    offtaker, owner = _profiles(values)
    scenarios, loaded = _scenarios(opts)
    share = upside_share
    if solve_upside_share:
        solved = solve_floor_upside_share(
            scenarios,
            floor_eur_per_mw_year=floor,
            offtaker=offtaker,
            upside_threshold_eur_per_mw_year=upside_threshold,
            target_premium_eur_per_mw_year=price or 0.0,
            deal_mw=deal_mw,
        )
        if solved is None:
            raise typer.BadParameter("no upside share <= 100 % pays for this floor at the requested premium")
        share = solved
    result = price_floor(
        scenarios,
        floor_eur_per_mw_year=floor,
        premium_eur_per_mw_year=price,
        upside_share=share,
        upside_threshold_eur_per_mw_year=upside_threshold,
        offtaker=offtaker,
        owner=owner,
        deal_mw=deal_mw,
    )
    _write_contract(
        "floor", "Revenue floor valuation", result, scenarios, loaded, out_dir, write_paths=write_paths, figures=figures
    )


@value_app.command("partial-toll")
def partial_toll_command(
    ledger: LedgerOpt,
    out_dir: OutDirOpt,
    fixed_fraction: Annotated[
        float, typer.Option("--fixed-fraction", help="Share of capacity under the fixed toll.")
    ] = 0.5,
    owner_merchant_share: Annotated[
        float, typer.Option("--owner-merchant-share", help="Owner's share of merchant revenue on the rest.")
    ] = 0.8,
    price: PriceOpt = None,
    escalation: EscalationOpt = 0.0,
    deal_mw: DealOpt = 1.0,
    owner_capture: CaptureOpt = 1.0,
    offtaker_measure: MeasureOpt = "cvar",
    offtaker_alpha: AlphaOpt = 0.10,
    offtaker_weight: WeightOpt = 0.3,
    offtaker_tolerance_eur: ToleranceOpt = None,
    offtaker_discount: DiscountOpt = 0.08,
    offtaker_margin: MarginOpt = 0.0,
    owner_measure: MeasureOpt = "cvar",
    owner_alpha: AlphaOpt = 0.10,
    owner_weight: WeightOpt = 0.6,
    owner_tolerance_eur: ToleranceOpt = None,
    owner_discount: DiscountOpt = 0.08,
    owner_margin: MarginOpt = 0.0,
    power_mw: PowerOpt = None,
    market: MarketOpt = None,
    timezone: TimezoneOpt = None,
    value: ValueOpt = "pnl",
    min_coverage: CoverageOpt = 0.98,
    tenor_years: TenorOpt = 7,
    start_year: StartYearOpt = 2027,
    paths: PathsOpt = 2000,
    seed: SeedOpt = DEFAULT_SEED,
    bootstrap: BootstrapOpt = "block",
    block_days: BlockOpt = 7,
    season_window_days: WindowOpt = 30,
    align_weekday: WeekdayOpt = True,
    weights: WeightsOpt = None,
    regime_persistence: PersistenceOpt = 1.0,
    capacity_curve: CurveOpt = None,
    fleet: FleetOpt = None,
    fleet_log_sigma: FleetSigmaOpt = 0.0,
    reference_fleet_mw: ReferenceFleetOpt = 0.0,
    skill_share: SkillShareOpt = 0.0,
    skill_decay: SkillDecayOpt = 0.0,
    capacity_fade: FadeOpt = 0.0,
    availability_factor: AvailabilityOpt = 1.0,
    level_log_sigma: LevelSigmaOpt = 0.0,
    level_persistence: LevelPersistenceOpt = 0.0,
    write_paths: PathsOutOpt = False,
    figures: FiguresOpt = True,
) -> None:
    """Partial toll: x % of capacity at a fixed toll, the rest merchant with a revenue share."""
    values = locals()
    opts = _ScenarioOptions.pick(values)
    offtaker, owner = _profiles(values)
    scenarios, loaded = _scenarios(opts)
    try:
        result = price_partial_toll(
            scenarios,
            fixed_fraction=fixed_fraction,
            owner_merchant_share=owner_merchant_share,
            toll_eur_per_mw_year=price,
            escalation_per_year=escalation,
            offtaker=offtaker,
            owner=owner,
            deal_mw=deal_mw,
            owner_capture=owner_capture,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_contract(
        "partial_toll",
        "Partial toll valuation",
        result,
        scenarios,
        loaded,
        out_dir,
        write_paths=write_paths,
        figures=figures,
    )


@value_app.command("swap")
def swap_command(
    ledger: LedgerOpt,
    out_dir: OutDirOpt,
    reference_ledger: Annotated[
        list[str] | None,
        typer.Option(
            "--reference-ledger",
            help="Ledger of the revenue index the swap settles on (REGIME=PATH per regime); default: the asset itself.",
        ),
    ] = None,
    notional: Annotated[float, typer.Option("--notional", help="Swapped share of capacity.")] = 1.0,
    price: PriceOpt = None,
    deal_mw: DealOpt = 1.0,
    owner_capture: CaptureOpt = 1.0,
    offtaker_measure: MeasureOpt = "cvar",
    offtaker_alpha: AlphaOpt = 0.10,
    offtaker_weight: WeightOpt = 0.3,
    offtaker_tolerance_eur: ToleranceOpt = None,
    offtaker_discount: DiscountOpt = 0.08,
    offtaker_margin: MarginOpt = 0.0,
    owner_measure: MeasureOpt = "cvar",
    owner_alpha: AlphaOpt = 0.10,
    owner_weight: WeightOpt = 0.6,
    owner_tolerance_eur: ToleranceOpt = None,
    owner_discount: DiscountOpt = 0.08,
    owner_margin: MarginOpt = 0.0,
    power_mw: PowerOpt = None,
    market: MarketOpt = None,
    timezone: TimezoneOpt = None,
    value: ValueOpt = "pnl",
    min_coverage: CoverageOpt = 0.98,
    tenor_years: TenorOpt = 7,
    start_year: StartYearOpt = 2027,
    paths: PathsOpt = 2000,
    seed: SeedOpt = DEFAULT_SEED,
    bootstrap: BootstrapOpt = "block",
    block_days: BlockOpt = 7,
    season_window_days: WindowOpt = 30,
    align_weekday: WeekdayOpt = True,
    weights: WeightsOpt = None,
    regime_persistence: PersistenceOpt = 1.0,
    capacity_curve: CurveOpt = None,
    fleet: FleetOpt = None,
    fleet_log_sigma: FleetSigmaOpt = 0.0,
    reference_fleet_mw: ReferenceFleetOpt = 0.0,
    skill_share: SkillShareOpt = 0.0,
    skill_decay: SkillDecayOpt = 0.0,
    capacity_fade: FadeOpt = 0.0,
    availability_factor: AvailabilityOpt = 1.0,
    level_log_sigma: LevelSigmaOpt = 0.0,
    level_persistence: LevelPersistenceOpt = 0.0,
    write_paths: PathsOutOpt = False,
    figures: FiguresOpt = True,
) -> None:
    """Revenue swap: owner pays a revenue index (asset or benchmark ledger) and receives a fixed leg."""
    values = locals()
    opts = _ScenarioOptions.pick(values)
    offtaker, owner = _profiles(values)
    scenarios, loaded = _scenarios(opts, reference=reference_ledger)
    try:
        result = price_revenue_swap(
            scenarios,
            notional_fraction=notional,
            fixed_eur_per_mw_year=price,
            reference_series="reference" if reference_ledger else None,
            asset_series="asset",
            offtaker=offtaker,
            owner=owner,
            deal_mw=deal_mw,
            owner_capture=owner_capture,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_contract(
        "swap", "Revenue swap valuation", result, scenarios, loaded, out_dir, write_paths=write_paths, figures=figures
    )


def _profiles(values: dict[str, Any]) -> tuple[RiskProfile, RiskProfile]:
    profiles = []
    for prefix in ("offtaker", "owner"):
        profiles.append(
            _risk(
                prefix,
                values[f"{prefix}_measure"],
                values[f"{prefix}_alpha"],
                values[f"{prefix}_weight"],
                values[f"{prefix}_tolerance_eur"],
                values[f"{prefix}_discount"],
                values[f"{prefix}_margin"],
            )
        )
    return profiles[0], profiles[1]


@value_app.command("attribution")
def attribution_command(
    out_dir: OutDirOpt,
    oracle: Annotated[Path | None, typer.Option(help="Perfect-foresight ledger (anchor).")] = None,
    strategy: Annotated[
        Path | None, typer.Option(help="Tradable strategy ledger, same mechanism as the oracle.")
    ] = None,
    mechanism: Annotated[Path | None, typer.Option(help="Strategy re-run under the other mechanism (new2026).")] = None,
    impact: Annotated[Path | None, typer.Option(help="The same, impact-aware at the fleet size.")] = None,
    availability: Annotated[Path | None, typer.Option(help="The same, with contracted availability.")] = None,
    degradation: Annotated[Path | None, typer.Option(help="The same, with the cycling limit / wear cost.")] = None,
    availability_factor: Annotated[
        float | None, typer.Option("--availability-factor", help="Analytic availability step when no ledger is given.")
    ] = None,
    basis: Annotated[str, typer.Option(help="`net` (P&L) or `gross` (revenue, wear shown last).")] = "net",
    new_prices: Annotated[
        Path | None,
        typer.Option(
            help="QH prices of the new mechanism (timestamp_utc + price column) to split repricing vs re-optimisation."
        ),
    ] = None,
    new_price_column: Annotated[str, typer.Option(help="Price column in --new-prices.")] = "imbalance_price_eur_mwh",
    power_mw: PowerOpt = None,
    market: MarketOpt = None,
    timezone: TimezoneOpt = None,
    figures: FiguresOpt = True,
) -> None:
    """Value waterfall: oracle -> strategy -> mechanism -> impact -> availability -> degradation."""
    given = {
        key: path
        for key, path in (
            ("oracle", oracle),
            ("strategy", strategy),
            ("mechanism", mechanism),
            ("impact", impact),
            ("availability", availability),
            ("degradation", degradation),
        )
        if path is not None
    }
    if not given:
        raise typer.BadParameter("give at least --oracle or --strategy")
    if basis not in ("net", "gross"):
        raise typer.BadParameter("--basis must be net or gross")
    loaded = [_load(key, path, power_mw=power_mw, market=market, timezone=timezone) for key, path in given.items()]
    power = loaded[0].metadata.require_power_mw()
    tz = loaded[0].metadata.local_timezone
    ledgers = {item.name: item.ledger for item in loaded}
    result = value_waterfall(
        ledgers,
        power_mw=power,
        timezone=tz,
        basis="net" if basis == "net" else "gross",
        availability_factor=availability_factor,
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {"waterfall": result.to_dict(), "inputs": [i.summary.to_dict() for i in loaded]}
    parts = [
        "# Value attribution",
        _banner(loaded) + "## Inputs\n\n" + _inputs_markdown(loaded),
        "## Waterfall\n\n" + render_waterfall(result),
    ]
    decomposition: WaterfallResult | None = None
    if new_prices is not None:
        if strategy is None or mechanism is None:
            raise typer.BadParameter("--new-prices needs both --strategy and --mechanism")
        prices = (
            pd.read_parquet(new_prices) if new_prices.suffix.lower() in {".parquet", ".pq"} else pd.read_csv(new_prices)
        )
        decomposition = mechanism_decomposition(
            ledgers["strategy"],
            ledgers["mechanism"],
            prices,
            power_mw=power,
            timezone=tz,
            price_column=new_price_column,
        )
        payload["mechanism_decomposition"] = decomposition.to_dict()
        parts.append("## Mechanism effect: repricing vs re-optimisation\n\n" + render_waterfall(decomposition))
    write_json(payload, out_dir / "attribution.json")
    result.daily.to_csv(out_dir / "attribution_daily.csv", index=False)
    if figures:
        plot_waterfall(result, out_dir / "attribution_waterfall.png")
        parts.append("![waterfall](attribution_waterfall.png)")
    write_markdown("\n\n".join(parts), out_dir / "attribution.md")
    typer.echo(
        f"attribution: {result.anchor_eur_per_mw_year:,.0f} -> {result.final_eur_per_mw_year:,.0f} EUR/MW/yr "
        f"-> {_display_path(out_dir)}"
    )


@value_app.command("portfolio")
def portfolio_command(
    asset: Annotated[list[str], typer.Option("--asset", "-a", help="Asset ledger as NAME=PATH (repeat).")],
    out_dir: OutDirOpt,
    weights: Annotated[
        str | None,
        typer.Option("--weights", help="P&L scaling per asset, e.g. `be=2,de=1` (contracted / simulated MW)."),
    ] = None,
    alpha: Annotated[float, typer.Option(help="Tail probability for VaR / CVaR.")] = 0.05,
    paths: PathsOpt = 2000,
    seed: SeedOpt = DEFAULT_SEED,
    block_days: BlockOpt = 7,
    min_coverage: CoverageOpt = 0.98,
    power_mw: PowerOpt = None,
    figures: FiguresOpt = True,
) -> None:
    """Portfolio of assets / markets: correlation, VaR / CVaR, diversification, contributions, concentration."""
    named = _named_paths(asset)
    scale = _float_map(weights)
    unknown = sorted(set(scale) - set(named))
    if unknown:
        raise typer.BadParameter(f"weights for unknown assets {unknown}")
    loaded = [_load(name, path, min_coverage=min_coverage, power_mw=power_mw) for name, path in named.items()]
    assets = [
        PortfolioAsset(
            name=item.name,
            daily=item.daily,
            weight=scale.get(item.name, 1.0),
            market=item.metadata.market,
            power_mw=item.metadata.power_mw,
        )
        for item in loaded
    ]
    try:
        result = portfolio_risk(
            assets,
            alpha=alpha,
            min_coverage=min_coverage,
            annual_paths=paths,
            seed=seed,
            bootstrap=BootstrapConfig(block_days=block_days),
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    out_dir.mkdir(parents=True, exist_ok=True)
    write_json(
        {"portfolio": result.to_dict(), "inputs": [i.summary.to_dict() for i in loaded]}, out_dir / "portfolio.json"
    )
    parts = [
        "# Portfolio risk",
        _banner(loaded) + "## Inputs\n\n" + _inputs_markdown(loaded),
        "## Historical revenue\n\n" + render_revenue_summaries([i.summary for i in loaded]),
        "## Portfolio\n\n" + render_portfolio(result),
    ]
    if figures:
        plot_portfolio_risk(result, out_dir / "portfolio_risk.png")
        parts.append("![portfolio risk](portfolio_risk.png)")
    write_markdown("\n\n".join(parts), out_dir / "portfolio.md")
    typer.echo(
        f"portfolio: annual shortfall benefit {result.diversification['annual_shortfall_benefit_pct']:.1%} "
        f"-> {_display_path(out_dir)}"
    )
