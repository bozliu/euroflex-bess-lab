"""Run the full contract-valuation chain and write a markdown report with figures.

Without ledger arguments every input is SYNTHETIC (the seeded toy generator in
``euroflex_bess_lab.valuation.synthetic``) and the report says so. Pass ledgers produced by the real-time
simulator or a backtest (parquet / CSV, ideally with ``.meta.json`` sidecars) to run the same chain on them::

    .venv/bin/python scripts/research/valuation_demo.py \\
        --current runs/be_strategy_current.parquet --new2026 runs/be_strategy_new2026.parquet \\
        --oracle runs/be_oracle_current.parquet --impact runs/be_strategy_new2026_q1500.parquet \\
        --index runs/be_threshold_current.parquet --germany runs/de_strategy.parquet \\
        --capacity-curve placeholder --report reports/valuation_belgium.md --figure-prefix valuation_belgium

Roles: ``--current`` / ``--new2026`` are the same strategy settled under the two Belgian imbalance-price
formulas (the two revenue regimes; also the ``strategy`` and ``mechanism`` steps of the waterfall); ``--oracle``
anchors the waterfall; ``--impact`` / ``--availability`` / ``--degradation`` add one factor each; ``--index`` is a
benchmark strategy (swap reference, skill share); ``--germany`` is a second-market asset for the portfolio. A
``new2026`` ledger shorter than a year (Elia's ODS211/212 simulation covers Jan-Jul 2026) is extended over the
``current`` history with the paired-overlap ratio. Every number in the report comes from this script.
"""

from __future__ import annotations

import argparse
import math
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from euroflex_bess_lab.valuation import (
    CapacityValueCurve,
    FleetPath,
    LedgerMetadata,
    PortfolioAsset,
    RiskProfile,
    SaturationSpec,
    ScenarioConfig,
    SkillDecay,
    daily_panel,
    daily_pnl,
    derive_regime_from_overlap,
    generate_scenarios,
    load_capacity_value_curve,
    mechanism_decomposition,
    portfolio_risk,
    price_floor,
    price_partial_toll,
    price_revenue_swap,
    price_toll,
    read_ledger,
    solve_floor_upside_share,
    summarize_daily,
    toll_sensitivity,
    value_waterfall,
)
from euroflex_bess_lab.valuation.plots import (
    plot_annual_distribution,
    plot_portfolio_risk,
    plot_toll_sensitivity,
    plot_waterfall,
)
from euroflex_bess_lab.valuation.report import (
    fmt_eur,
    fmt_pct,
    md_table,
    render_contract,
    render_portfolio,
    render_revenue_summaries,
    render_scenarios,
    render_waterfall,
    write_markdown,
)
from euroflex_bess_lab.valuation.revenue import infer_interval_minutes
from euroflex_bess_lab.valuation.synthetic import SYNTHETIC_LABEL, synthetic_ledger_set, synthetic_prices

REPO_ROOT = Path(__file__).resolve().parents[2]
ROLES = ("oracle", "current", "new2026", "impact", "availability", "degradation", "index", "germany")
CHAIN = {
    "oracle": "oracle",
    "current": "strategy",
    "new2026": "mechanism",
    "impact": "impact",
    "availability": "availability",
    "degradation": "degradation",
}
SYNTHETIC_KEYS = {
    "oracle": "oracle",
    "current": "strategy",
    "new2026": "mechanism",
    "impact": "impact",
    "availability": "availability",
    "degradation": "degradation",
    "index": "index",
    "germany": "germany",
}
NEW2026_WINDOW = ("2026-01-01", "2026-07-01")  # synthetic stand-in for the ODS211/212 simulation window


@dataclass
class Input:
    role: str
    ledger: pd.DataFrame
    metadata: LedgerMetadata
    source: str
    synthetic: bool


def _display(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return path.name


def _points(text: str) -> dict[int, float]:
    out = {}
    for item in text.split(","):
        year, _, value = item.partition("=")
        out[int(year)] = float(value)
    return out


def _weights(text: str) -> dict[str, float]:
    out = {}
    for item in text.split(","):
        name, _, value = item.partition("=")
        out[name.strip()] = float(value)
    return out


def load_inputs(args: argparse.Namespace) -> dict[str, Input]:
    """Ledgers by role: the given files only, or - when no ledger is given at all - the full synthetic set.

    Real and synthetic inputs are never mixed silently; roles without a ledger are skipped.
    """
    given = {role: getattr(args, role) for role in ROLES if getattr(args, role) is not None}
    inputs: dict[str, Input] = {}
    if given:
        overrides = {"power_mw": args.power_mw} if args.power_mw else {}
        for role, path in given.items():
            ledger, metadata = read_ledger(path, **overrides)
            inputs[role] = Input(role, ledger, metadata, _display(path), metadata.synthetic)
        return inputs
    synthetic = synthetic_ledger_set(args.synthetic_start, args.synthetic_end, seed=args.synthetic_seed)
    for role in ROLES:
        ledger, metadata = synthetic[SYNTHETIC_KEYS[role]]
        if role == "new2026":
            stamps = ledger["timestamp_utc"]
            start, end = (pd.Timestamp(day, tz="Europe/Brussels") for day in NEW2026_WINDOW)
            ledger = ledger.loc[(stamps >= start) & (stamps < end)].reset_index(drop=True)
        inputs[role] = Input(role, ledger, metadata, SYNTHETIC_LABEL, True)
    return inputs


def _daily(item: Input) -> pd.DataFrame:
    return daily_pnl(item.ledger, timezone=item.metadata.local_timezone, power_mw=item.metadata.require_power_mw())


def _step(item: Input) -> float:
    return infer_interval_minutes(item.ledger["timestamp_utc"])


def _relative(target: Path, report: Path) -> str:
    return Path(os.path.relpath(target, report.parent)).as_posix()


def _load_curves(path: str) -> CapacityValueCurve | dict[str, CapacityValueCurve]:
    """One curve, or one per formula when the file has a `mechanism` column with several values."""
    if str(path) != "placeholder":
        file = Path(path)
        suffix = file.suffix.lower()
        frame = None
        if suffix == ".csv":
            frame = pd.read_csv(file)
        elif suffix in {".parquet", ".pq"}:
            frame = pd.read_parquet(file)
        if frame is not None and "mechanism" in frame.columns and frame["mechanism"].nunique() > 1:
            return {
                str(m): load_capacity_value_curve(file, mechanism=str(m)) for m in sorted(frame["mechanism"].unique())
            }
    return load_capacity_value_curve(path)


def build_report(args: argparse.Namespace) -> str:
    inputs = load_inputs(args)
    for role in ("current", "new2026"):
        if role not in inputs:
            raise SystemExit(f"--{role} is required when ledgers are given")
    any_synthetic = any(item.synthetic for item in inputs.values())
    all_synthetic = all(item.synthetic for item in inputs.values())
    report_path = Path(args.report)
    figures_dir = Path(args.figures_dir)
    prefix = args.figure_prefix
    dailies = {role: _daily(item) for role, item in inputs.items()}
    summaries = [
        summarize_daily(
            dailies[role], item.metadata.with_overrides(asset_id=role), min_coverage=0.98, interval_minutes=_step(item)
        )
        for role, item in inputs.items()
    ]

    # ---------------------------------------------------------------- attribution
    chain = {CHAIN[role]: inputs[role].ledger for role in CHAIN if role in inputs}
    power = inputs["current"].metadata.require_power_mw()
    timezone = inputs["current"].metadata.local_timezone
    waterfall = value_waterfall(chain, power_mw=power, timezone=timezone)
    plot_waterfall(
        waterfall,
        figures_dir / f"{prefix}_waterfall.png",
        title="Value attribution waterfall" + (" (synthetic)" if any_synthetic else ""),
        subtitle=f"EUR per MW per year, common period {waterfall.first_local_day} .. {waterfall.last_local_day}",
    )
    decomposition = None
    if args.new_prices:
        prices = (
            pd.read_parquet(args.new_prices) if args.new_prices.suffix == ".parquet" else pd.read_csv(args.new_prices)
        )
        price_column = args.new_price_column
    elif all_synthetic:
        prices = synthetic_prices(args.synthetic_start, args.synthetic_end, seed=args.synthetic_seed)
        price_column = "price_new2026_eur_mwh"
    else:
        prices = None
        price_column = ""
    if prices is not None:
        decomposition = mechanism_decomposition(
            inputs["current"].ledger,
            inputs["new2026"].ledger,
            prices,
            power_mw=power,
            timezone=timezone,
            price_column=price_column,
        )

    # ---------------------------------------------------------------- revenue regimes
    current_daily = dailies["current"]
    new_daily = dailies["new2026"]
    new_days = int((new_daily["coverage"] >= 0.98).sum())
    overlap_info = None
    if new_days < 330:
        new_panel, overlap_info = derive_regime_from_overlap(current_daily, new_daily, method="ratio")
        mechanism_ratio = float(overlap_info["ratio"])
    else:
        new_panel = daily_panel({"asset": new_daily})
        common = daily_panel({"asset": current_daily, "new": new_daily})
        mechanism_ratio = float(common["new"].sum() / common["asset"].sum())
    current_panel = daily_panel({"asset": current_daily})
    skill_share = args.skill_share
    index_ratio = None
    if "index" in inputs:
        # the benchmark index is only simulated under the current formula; under `new2026` it is assumed to move
        # with the same paired-overlap ratio as the asset, which keeps the day-by-day asset/index basis
        current_panel = daily_panel({"asset": current_daily, "index": dailies["index"]})
        index_ratio = float(current_panel["index"].sum() / current_panel["asset"].sum())
        skill_share = float(np.clip(1.0 - index_ratio, 0.0, 1.0))
        new_index = current_panel["index"] * mechanism_ratio
        new_panel = pd.concat([new_panel.iloc[:, 0].rename("asset"), new_index.rename("index")], axis=1, join="inner")
    regime_histories = {"current": current_panel, "new2026": new_panel.rename(columns={new_panel.columns[0]: "asset"})}

    curves = _load_curves(args.capacity_curve)
    curve = next(iter(curves.values())) if isinstance(curves, dict) else curves
    fleet_points = _points(args.fleet)
    config = ScenarioConfig(
        n_paths=args.paths,
        n_years=args.tenor_years,
        start_year=args.start_year,
        seed=args.seed,
        saturation=SaturationSpec(
            curve=curves,
            fleet=FleetPath.from_points(fleet_points, path_log_sigma=args.fleet_log_sigma),
            reference_fleet_mw=args.reference_fleet_mw,
        ),
        skill=SkillDecay(skill_share, args.skill_decay) if skill_share > 0 else None,
        level_log_sigma=args.level_log_sigma,
        level_persistence=args.level_persistence,
    )
    weights = _weights(args.weights)
    scenarios = generate_scenarios(regime_histories, config, weights=weights, primary="asset")

    # ---------------------------------------------------------------- contracts
    offtaker = RiskProfile(name="offtaker", measure="cvar", cvar_alpha=0.10, cvar_weight=0.3, discount_rate=0.08)
    owner = RiskProfile(name="owner", measure="cvar", cvar_alpha=0.10, cvar_weight=0.6, discount_rate=0.08)
    toll = price_toll(scenarios, offtaker=offtaker, owner=owner, deal_mw=args.deal_mw)
    declining = price_toll(
        scenarios, offtaker=offtaker, owner=owner, escalation_per_year=args.toll_escalation, deal_mw=args.deal_mw
    )
    grid = [toll.break_even_eur_per_mw_year * f for f in np.linspace(0.5, 1.5, 21)]
    sensitivity = toll_sensitivity(scenarios, grid, offtaker=offtaker, deal_mw=args.deal_mw)
    floor_level = args.floor or 1000.0 * round(0.65 * toll.break_even_eur_per_mw_year / 1000.0)
    threshold = 1000.0 * round(toll.break_even_eur_per_mw_year / 1000.0)
    floor = price_floor(
        scenarios, floor_eur_per_mw_year=floor_level, offtaker=offtaker, owner=owner, deal_mw=args.deal_mw
    )
    share = solve_floor_upside_share(
        scenarios,
        floor_eur_per_mw_year=floor_level,
        upside_threshold_eur_per_mw_year=threshold,
        offtaker=offtaker,
        deal_mw=args.deal_mw,
    )
    collar = (
        price_floor(
            scenarios,
            floor_eur_per_mw_year=floor_level,
            upside_share=share,
            upside_threshold_eur_per_mw_year=threshold,
            offtaker=offtaker,
            owner=owner,
            deal_mw=args.deal_mw,
        )
        if share is not None
        else None
    )
    partial = price_partial_toll(
        scenarios, fixed_fraction=0.5, owner_merchant_share=0.8, offtaker=offtaker, owner=owner, deal_mw=args.deal_mw
    )
    swap = price_revenue_swap(
        scenarios,
        reference_series="index" if "index" in scenarios.series else None,
        offtaker=offtaker,
        owner=owner,
        deal_mw=args.deal_mw,
    )
    plot_annual_distribution(
        scenarios,
        figures_dir / f"{prefix}_annual_distribution.png",
        reference_lines={"fair toll": toll.price_eur_per_mw_year, "break-even": toll.break_even_eur_per_mw_year},
        title="Annual merchant revenue per MW" + (" (synthetic)" if any_synthetic else ""),
        subtitle=f"{scenarios.n_paths:,} paths; band = P90 to P10 (exceedance)",
    )
    plot_toll_sensitivity(
        sensitivity,
        figures_dir / f"{prefix}_toll_sensitivity.png",
        fair_toll=toll.price_eur_per_mw_year,
        break_even=toll.break_even_eur_per_mw_year,
        title="Toll sensitivity, offtaker view" + (" (synthetic)" if any_synthetic else ""),
    )

    # ---------------------------------------------------------------- portfolio
    portfolio = None
    if "germany" in inputs:
        assets = [
            PortfolioAsset("belgium", current_daily, market="BE", power_mw=power),
            PortfolioAsset(
                "germany",
                dailies["germany"],
                market=inputs["germany"].metadata.market,
                power_mw=inputs["germany"].metadata.power_mw,
            ),
        ]
        portfolio = portfolio_risk(assets, annual_paths=args.paths, seed=args.seed)
        plot_portfolio_risk(
            portfolio,
            figures_dir / f"{prefix}_portfolio.png",
            title="Belgium + Germany diversification" + (" (synthetic)" if any_synthetic else ""),
            subtitle=f"daily P&L correlation {portfolio.correlation.iloc[0, 1]:.2f}; one-year joint block bootstrap",
        )

    # ---------------------------------------------------------------- markdown
    figure = {
        name: _relative(figures_dir / f"{prefix}_{name}.png", report_path)
        for name in ("waterfall", "annual_distribution", "toll_sensitivity", "portfolio")
    }
    title = args.title or ("Contract valuation demo - SYNTHETIC DATA" if any_synthetic else "Contract valuation")
    lines: list[str] = [f"# {title}", ""]
    if all_synthetic:
        lines += [
            "> **Synthetic inputs - not market evidence.** Every ledger in this report comes from the seeded toy "
            "generator in `euroflex_bess_lab.valuation.synthetic` (synthetic imbalance-like prices and a toy "
            "daily-arbitrage dispatch). The numbers demonstrate the method and the report layout only. The "
            "capacity-value curve is a placeholder and the fleet path is illustrative.",
            "",
        ]
    elif any_synthetic:
        lines += ["> **Mixed inputs** - at least one ledger below is synthetic (see the inputs table).", ""]
    zone = toll.deal_zone_eur_per_mw_year
    lines += [
        "## Headline numbers",
        "",
        f"- Fair flat toll (offtaker certainty equivalent): **{fmt_eur(toll.price_eur_per_mw_year)} EUR/MW/yr** "
        f"({toll.price_eur_per_kw_month:.2f} EUR/kW/month); risk-neutral break-even "
        f"{fmt_eur(toll.break_even_eur_per_mw_year)}; owner reservation "
        f"{fmt_eur(toll.owner_reservation_eur_per_mw_year)}; "
        + (f"deal zone {fmt_eur(zone[0])} .. {fmt_eur(zone[1])}." if zone else "no deal zone."),
        f"- At the fair toll the offtaker expects {fmt_eur(toll.expected_offtaker_margin_eur_per_mw_year)} EUR/MW/yr "
        f"margin, loses money over the tenor on {fmt_pct(toll.prob_offtaker_loss)} of paths and in "
        f"{fmt_pct(toll.prob_offtaker_loss_year)} of contract years.",
        f"- Revenue floor at {fmt_eur(floor_level)} EUR/MW/yr: fair premium {fmt_eur(floor.price_eur_per_mw_year)} "
        f"EUR/MW/yr; "
        + (
            f"zero-premium alternative: owner gives {fmt_pct(share)} of revenue above {fmt_eur(threshold)}."
            if share is not None
            else "no upside share up to 100 % pays for it."
        ),
        f"- Partial toll (50 % fixed, owner keeps 80 % of the merchant half): fair toll on the fixed share "
        f"{fmt_eur(partial.price_eur_per_mw_year)} EUR/MW/yr.",
        f"- Revenue swap on `{swap.terms['reference_series']}`: fair fixed leg {fmt_eur(swap.price_eur_per_mw_year)} "
        f"EUR/MW/yr, hedge effectiveness {fmt_pct(swap.extras['hedge_effectiveness_pv_variance'])}.",
        f"- Value attribution: oracle {fmt_eur(waterfall.anchor_eur_per_mw_year)} -> contractable "
        f"{fmt_eur(waterfall.final_eur_per_mw_year)} EUR/MW/yr "
        f"({fmt_pct(waterfall.final_eur_per_mw_year / waterfall.anchor_eur_per_mw_year)} of the oracle).",
    ]
    if portfolio is not None:
        lines.append(
            f"- Belgium + Germany: daily P&L correlation {portfolio.correlation.iloc[0, 1]:.2f}; diversification "
            f"cuts the one-year shortfall (mean minus CVaR 5 %) by "
            f"{fmt_pct(portfolio.diversification['annual_shortfall_benefit_pct'])}."
        )
    lines += ["", "## Inputs", ""]
    rows = []
    for role, item in inputs.items():
        m = item.metadata
        rows.append(
            [
                role,
                item.source,
                m.market,
                m.mechanism or "-",
                m.strategy or "-",
                f"{m.power_mw:g} MW / {m.energy_mwh:g} MWh" if m.energy_mwh else f"{m.power_mw:g} MW",
                m.publication_lag or "-",
                f"{m.degradation_eur_per_mwh:g} EUR/MWh" if m.degradation_eur_per_mwh is not None else "-",
                f"{m.fleet_mw:,.0f}",
                f"{m.availability:.0%}",
            ]
        )
    lines.append(
        md_table(
            [
                "role",
                "source",
                "market",
                "mechanism",
                "strategy",
                "battery",
                "publication lag",
                "wear cost",
                "fleet MW",
                "availability",
            ],
            rows,
            align="lllllllllr",
        )
    )
    lines += [
        "",
        "## 1. Historical revenue per ledger",
        "",
        "Complete local delivery days only (coverage >= 98 %); EUR per MW of battery power.",
        "",
        render_revenue_summaries(summaries),
        "",
        "## 2. Value attribution",
        "",
        "Sequential waterfall: each step re-runs the same battery with one more factor, so each effect is "
        "conditional on the steps before it.",
        "",
        render_waterfall(waterfall),
        "",
        "Levels are annualised over the common period of all ledgers (no seasonal adjustment); a period shorter "
        "than a year inherits its seasons.",
        "",
        f"![waterfall]({figure['waterfall']})",
    ]
    if decomposition is not None:
        lines += [
            "",
            "Mechanism effect split into *repricing* (same dispatch settled at new-formula prices) and "
            "*re-optimisation* (strategy re-run under the new formula):",
            "",
            render_waterfall(decomposition),
        ]
    lines += ["", "## 3. Revenue scenarios", ""]
    if overlap_info is not None:
        lines += [
            f"The `new2026` ledger covers only {new_days} complete days, so the `new2026` regime history is the "
            f"`current` history scaled by the paired-overlap ratio {overlap_info['ratio']:.3f} "
            f"({overlap_info['overlap_days']} common days, {overlap_info['overlap_first_day']} .. "
            f"{overlap_info['overlap_last_day']}; observed `new2026` days are kept as observed).",
            "",
        ]
    if index_ratio is not None:
        lines += [
            f"Skill share {fmt_pct(skill_share)} = 1 - (benchmark index P&L / strategy P&L) on the `current` history. "
            "The index is resampled jointly with the asset (same days); under `new2026` it is scaled by the same "
            f"mechanism ratio ({mechanism_ratio:.3f}) as the asset.",
            "",
        ]
    lines += [render_scenarios(scenarios), "", f"![annual distribution]({figure['annual_distribution']})", ""]
    lines += [
        "## 4. Contracts",
        "",
        f"Offtaker: {offtaker.describe()}. Owner: {owner.describe()}. Prices in EUR per MW per year; present "
        "values use mid-year discounting.",
        "",
        "### 4.1 Fixed toll",
        "",
        render_contract(toll, include_merchant=False),
        "",
        f"![toll sensitivity]({figure['toll_sensitivity']})",
        "",
        f"A flat toll against declining merchant revenue loses money in late years. With a toll escalating at "
        f"{args.toll_escalation:+.0%} per year the fair first-year toll is "
        f"{fmt_eur(declining.price_eur_per_mw_year)} EUR/MW/yr and the share of loss-making offtaker years falls "
        f"from {fmt_pct(toll.prob_offtaker_loss_year)} to {fmt_pct(declining.prob_offtaker_loss_year)}.",
        "",
        "### 4.2 Revenue floor",
        "",
        render_contract(floor, include_merchant=False),
    ]
    if collar is not None:
        lines += [
            "",
            "Zero-premium version (upside sharing pays for the floor):",
            "",
            render_contract(collar, include_merchant=False),
        ]
    lines += [
        "",
        "### 4.3 Partial toll",
        "",
        render_contract(partial, include_merchant=False),
        "",
        "The toll per *tolled* MW exceeds the full toll because the offtaker also keeps 20 % of the merchant half; "
        "in its valuation that fee income subsidises the fixed payment. If the fee only covers trading costs, "
        "raise the offtaker's required margin instead.",
        "",
        "### 4.4 Revenue swap",
        "",
        render_contract(swap, include_merchant=False),
        "",
    ]
    if portfolio is not None:
        lines += [
            "## 5. Portfolio: Belgium + Germany",
            "",
            render_portfolio(portfolio),
            "",
            f"![portfolio]({figure['portfolio']})",
            "",
        ]
    lines += [
        "## Assumptions and limitations",
        "",
        "- Revenue paths resample *historical days* of the ledgers; they cannot contain market states that never "
        "occurred in the history. The market-level shock (log-sigma "
        f"{args.level_log_sigma:g}, persistence {args.level_persistence:g}) is an explicit, uncalibrated "
        "allowance for inter-annual spread uncertainty.",
        "- Saturation scales all days of a year by the capacity-value ratio V(Q)/V(Q_ref); "
        + (
            "the curve used here is the documented PLACEHOLDER V(Q) = 1 / (1 + Q / 1500 MW), not an estimate."
            if curve.placeholder
            else f"curve source: {curve.source}"
            + (f" (one curve per formula: {', '.join(sorted(curves))})." if isinstance(curves, dict) else ".")
        )
        + f" Fleet path {', '.join(f'{y}={m:,.0f}' for y, m in sorted(fleet_points.items()))} MW (reference "
        f"{args.reference_fleet_mw:,.0f} MW) is an assumption to replace.",
        f"- Regime weights {args.weights}: probability that the realised behaviour of each settlement regime "
        "applies for the whole tenor (persistence 1).",
        "- Contract values are on net P&L (`pnl_eur`: revenue minus the ledger's degradation charge), i.e. a toll "
        "with a throughput charge at the ledger's EUR/MWh wear rate; use the CLI `--value revenue` for gross.",
        "- No credit risk, collateral, tax, inflation indexation or operating costs; the risk profiles are "
        "illustrative, not calibrated to any company.",
        "",
        "## Reproduce",
        "",
        "```bash",
        ".venv/bin/python scripts/research/valuation_demo.py" + ("" if all_synthetic else " <ledger arguments>"),
        "```",
        "",
        f"Seed {args.seed}; {args.paths:,} paths; tenor {args.start_year}-{args.start_year + args.tenor_years - 1}.",
    ]
    return "\n".join(lines)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for role in ROLES:
        parser.add_argument(f"--{role}", type=Path, default=None, help=f"{role} ledger (parquet/CSV)")
    parser.add_argument("--new-prices", type=Path, default=None, help="QH prices of the new mechanism")
    parser.add_argument("--new-price-column", default="imbalance_price_eur_mwh")
    parser.add_argument("--power-mw", type=float, default=None, help="override power_mw of every real ledger")
    parser.add_argument("--capacity-curve", default="placeholder")
    parser.add_argument("--fleet", default="2027=900,2028=1300,2029=2000,2030=2200,2033=2600")
    parser.add_argument("--reference-fleet-mw", type=float, default=400.0)
    parser.add_argument("--fleet-log-sigma", type=float, default=0.25)
    parser.add_argument("--weights", default="current=0.3,new2026=0.7")
    parser.add_argument("--skill-share", type=float, default=0.3, help="used when no --index ledger is given")
    parser.add_argument("--skill-decay", type=float, default=0.10)
    parser.add_argument("--level-log-sigma", type=float, default=0.15)
    parser.add_argument("--level-persistence", type=float, default=0.5)
    parser.add_argument("--tenor-years", type=int, default=7)
    parser.add_argument("--start-year", type=int, default=2027)
    parser.add_argument("--paths", type=int, default=4000)
    parser.add_argument("--seed", type=int, default=20261124)
    parser.add_argument("--deal-mw", type=float, default=1.0)
    parser.add_argument("--floor", type=float, default=None, help="floor level; default 65 %% of the break-even toll")
    parser.add_argument("--toll-escalation", type=float, default=-0.06)
    parser.add_argument("--synthetic-seed", type=int, default=7, help="seed of the synthetic ledger generator")
    parser.add_argument("--synthetic-start", default="2024-07-01")
    parser.add_argument("--synthetic-end", default="2026-07-01")
    parser.add_argument("--report", type=Path, default=REPO_ROOT / "reports" / "valuation_demo.md")
    parser.add_argument("--figures-dir", type=Path, default=REPO_ROOT / "reports" / "figures")
    parser.add_argument("--figure-prefix", default="valuation_demo")
    parser.add_argument("--title", default=None)
    args = parser.parse_args(argv)
    if not args.figure_prefix.startswith("valuation_"):
        parser.error("--figure-prefix must start with `valuation_`")
    if args.paths < 100 or not math.isfinite(args.level_log_sigma):
        parser.error("--paths must be >= 100")
    return args


def main(argv: Sequence[str] | None = None) -> Path:
    args = parse_args(argv)
    text = build_report(args)
    path = write_markdown(text, args.report)
    print(f"wrote {_display(path)}")
    return path


if __name__ == "__main__":
    main()
