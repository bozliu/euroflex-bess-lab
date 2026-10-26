"""Markdown and JSON rendering of valuation results."""

from __future__ import annotations

import json
import math
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .attribution import WaterfallResult
from .contracts import ContractResult
from .portfolio import PortfolioResult
from .revenue import RevenueSummary
from .scenarios import ScenarioSet

EXCEEDANCE_NOTE = "P90 / P50 / P10 follow the exceedance convention: P90 is exceeded with 90 % probability."


def to_jsonable(value: Any) -> Any:
    """Recursively convert numpy / pandas objects to JSON types (non-finite floats become ``null``)."""
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [to_jsonable(item) for item in value]
    if isinstance(value, np.ndarray):
        return [to_jsonable(item) for item in value.tolist()]
    if isinstance(value, pd.DataFrame):
        return [to_jsonable(row) for row in value.to_dict(orient="records")]
    if isinstance(value, pd.Series):
        return to_jsonable(value.to_dict())
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.generic):
        return to_jsonable(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def write_json(payload: Any, path: str | Path) -> Path:
    file = Path(path)
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(json.dumps(to_jsonable(payload), indent=2, allow_nan=False) + "\n", encoding="utf-8")
    return file


def write_markdown(text: str, path: str | Path) -> Path:
    file = Path(path)
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(text.rstrip() + "\n", encoding="utf-8")
    return file


def fmt_eur(value: float | None, decimals: int = 0) -> str:
    if value is None or not math.isfinite(float(value)):
        return "n/a"
    number = float(value)
    if abs(number) < 0.5 * 10.0**-decimals:
        number = 0.0  # avoid "-0"
    return f"{number:,.{decimals}f}"


def fmt_pct(value: float | None, decimals: int = 1) -> str:
    if value is None or not math.isfinite(float(value)):
        return "n/a"
    return f"{100.0 * float(value):.{decimals}f} %"


def md_table(headers: Sequence[str], rows: Sequence[Sequence[Any]], align: str | None = None) -> str:
    """GitHub markdown table; ``align`` is a string of ``l``/``r`` per column (default: first left, rest right)."""
    spec = align or ("l" + "r" * (len(headers) - 1))
    rule = ["---:" if a == "r" else "---" for a in spec]
    lines = ["| " + " | ".join(str(h) for h in headers) + " |", "| " + " | ".join(rule) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(cell) for cell in row) + " |")
    return "\n".join(lines)


def render_revenue_summaries(summaries: Sequence[RevenueSummary]) -> str:
    headers = [
        "ledger",
        "mechanism",
        "period (local days)",
        "days",
        "net EUR/MW/yr",
        "gross EUR/MW/yr",
        "wear EUR/MW/yr",
        "season-weighted net",
        "daily mean / std",
        "daily P05",
        "neg. days",
        "top-10% days share",
        "cycles/day",
    ]
    rows = []
    for s in summaries:
        rows.append(
            [
                s.asset_id,
                s.mechanism or "-",
                f"{s.first_day} .. {s.last_day}",
                s.n_days,
                fmt_eur(s.pnl_eur_per_mw_year),
                fmt_eur(s.revenue_eur_per_mw_year),
                fmt_eur(s.degradation_eur_per_mw_year),
                fmt_eur(s.seasonal_pnl_eur_per_mw_year),
                f"{fmt_eur(s.daily_mean_eur_per_mw)} / {fmt_eur(s.daily_std_eur_per_mw)}",
                fmt_eur(s.daily_p05_eur_per_mw),
                fmt_pct(s.negative_day_share),
                fmt_pct(s.top_decile_day_share),
                f"{s.full_cycles_per_day:.2f}" if s.full_cycles_per_day is not None else "n/a",
            ]
        )
    return md_table(headers, rows, align="lllrrrrrrrrrr")


def render_scenarios(scenarios: ScenarioSet, series: str | None = None) -> str:
    summary = scenarios.summary(series)
    cfg = scenarios.config
    boot = cfg.get("bootstrap", {})
    lines = [
        f"- Series: `{summary['series']}`; tenor {scenarios.years[0]}-{scenarios.years[-1]} "
        f"({scenarios.n_years} years), {scenarios.n_paths:,} paths, seed {cfg.get('seed', 'n/a')}.",
        f"- Bootstrap: {boot.get('method', 'n/a')}, block {boot.get('block_days', 'n/a')} days, seasonal window "
        f"+/- {boot.get('season_window_days', 'n/a')} days, weekday-aligned: {boot.get('align_weekday', 'n/a')}.",
        "- Regimes (share of path-years): "
        + ", ".join(f"`{name}` {fmt_pct(share)}" for name, share in summary["regime_shares"].items())
        + f"; persistence {cfg.get('regime_persistence', 'n/a')}.",
    ]
    saturation = cfg.get("saturation")
    if saturation:
        curve = saturation["curve"]
        curve_names = (
            curve.get("name")
            if isinstance(curve, dict) and "name" in curve
            else ", ".join(f"{k}: {v.get('name')}" for k, v in curve.items())
        )
        placeholder = (
            curve.get("placeholder")
            if isinstance(curve, dict) and "placeholder" in curve
            else any(v.get("placeholder") for v in curve.values())
        )
        fleet = saturation["fleet"]
        points = ", ".join(f"{y}={m:,.0f}" for y, m in zip(fleet["years"], fleet["mw"], strict=True))
        lines.append(
            f"- Saturation: curve {curve_names}{' (PLACEHOLDER)' if placeholder else ''}; reference fleet "
            f"{saturation['reference_fleet_mw']:,.0f} MW; fleet path {points} MW "
            f"(path log-sigma {fleet['path_log_sigma']:g}, year log-sigma {fleet['year_log_sigma']:g})."
        )
    else:
        lines.append("- Saturation: none.")
    skill = cfg.get("skill")
    lines.append(
        f"- Skill decay: share {skill['skill_share']:.0%}, {skill['decay_per_year']:.0%} per year."
        if skill
        else "- Skill decay: none."
    )
    lines.append(
        f"- Capacity fade {fmt_pct(cfg.get('capacity_fade_per_year', 0.0))} per year; availability factor "
        f"{cfg.get('availability_factor', 1.0):g}; market-level shock log-sigma {cfg.get('level_log_sigma', 0.0):g} "
        f"(persistence {cfg.get('level_persistence', 0.0):g})."
    )
    pooled = summary["annual_pooled"]
    tenor = summary["tenor_average"]
    lines.append(
        f"- Pooled annual revenue P90 / P50 / P10: {fmt_eur(pooled['p90'])} / {fmt_eur(pooled['p50'])} / "
        f"{fmt_eur(pooled['p10'])} EUR/MW/yr; tenor-average P90 / P50 / P10: {fmt_eur(tenor['p90'])} / "
        f"{fmt_eur(tenor['p50'])} / {fmt_eur(tenor['p10'])}."
    )
    rows = [
        [
            r["year"],
            fmt_eur(r["mean"]),
            fmt_eur(r["std"]),
            fmt_eur(r["p90"]),
            fmt_eur(r["p50"]),
            fmt_eur(r["p10"]),
            f"{r['saturation_multiplier_mean']:.3f}",
            fmt_eur(r["fleet_mw_mean"]) if r["fleet_mw_mean"] is not None else "-",
        ]
        for r in summary["by_year"]
    ]
    table = md_table(
        ["year", "mean", "std", "P90", "P50", "P10", "saturation x", "fleet MW (mean)"],
        rows,
    )
    return "\n".join(lines) + "\n\n" + table + "\n\n" + EXCEEDANCE_NOTE


_EXTRA_LABELS: dict[str, tuple[str, str]] = {
    "prob_floor_triggered_year": ("probability the floor pays in a year", "pct"),
    "expected_floor_payout_eur_per_mw_year": ("expected floor payout", "eur"),
    "expected_upside_paid_eur_per_mw_year": ("expected upside paid to the provider", "eur"),
    "max_floor_payout_eur_per_mw_year": ("largest annual floor payout", "eur"),
    "hedge_effectiveness_pv_variance": ("hedge effectiveness (PV variance reduction)", "pct"),
    "annual_correlation_reference_asset": ("annual correlation, index vs asset", "ratio"),
    "basis_std_eur_per_mw_year": ("basis risk (std of asset minus index)", "eur"),
    "owner_fixed_share_of_expected_cash": ("fixed share of the owner's expected cash", "pct"),
}

_TERM_LABELS: dict[str, tuple[str, str]] = {
    "toll_eur_per_mw_year": ("toll", "eur"),
    "floor_eur_per_mw_year": ("floor", "eur"),
    "premium_eur_per_mw_year": ("premium", "eur"),
    "upside_share": ("upside share", "pct"),
    "upside_threshold_eur_per_mw_year": ("upside threshold", "eur"),
    "fixed_fraction": ("fixed share of capacity", "pct"),
    "owner_merchant_share": ("owner share of merchant revenue", "pct"),
    "notional_fraction": ("notional", "pct"),
    "fixed_eur_per_mw_year": ("fixed leg", "eur"),
    "escalation_per_year": ("escalation per year", "pct"),
    "owner_capture": ("owner capture if merchant", "pct"),
    "reference_series": ("reference index", "text"),
    "deal_mw": ("deal size MW", "text"),
}


def _terms_text(terms: dict[str, Any]) -> str:
    parts = []
    for key, value in terms.items():
        if key not in _TERM_LABELS:
            continue
        label, kind = _TERM_LABELS[key]
        if kind == "eur":
            shown = f"{fmt_eur(value)} EUR/MW/yr"
        elif kind == "pct":
            shown = fmt_pct(value)
        else:
            shown = f"{value:g}" if isinstance(value, float) else str(value)
        parts.append(f"{label} {shown}")
    return "; ".join(parts)


def render_contract(result: ContractResult, *, include_merchant: bool = True) -> str:
    """Markdown block: terms and prices, both counterparties' views and (optionally) merchant revenue by year."""
    zone = result.deal_zone_eur_per_mw_year
    zone_text = (
        f"{fmt_eur(zone[0])} .. {fmt_eur(zone[1])}"
        if zone is not None
        else "none (the reservation prices do not overlap)"
    )
    off, own = result.offtaker, result.owner
    direction = (
        "offtaker pays (offtaker reservation = maximum, owner reservation = minimum)"
        if result.price_direction == "offtaker_pays"
        else "owner pays (offtaker reservation = minimum premium, owner reservation = maximum premium)"
    )
    head = [
        ["terms", _terms_text(result.terms)],
        [
            f"{'fair ' if result.priced else 'quoted '}{result.price_name}",
            f"{fmt_eur(result.price_eur_per_mw_year)} EUR/MW/yr ({result.price_eur_per_kw_month:.2f} EUR/kW/month)",
        ],
        [
            "break-even (risk-neutral, zero expected offtaker margin)",
            f"{fmt_eur(result.break_even_eur_per_mw_year)} EUR/MW/yr",
        ],
        ["offtaker reservation", f"{fmt_eur(result.offtaker_reservation_eur_per_mw_year)} EUR/MW/yr"],
        ["owner reservation", f"{fmt_eur(result.owner_reservation_eur_per_mw_year)} EUR/MW/yr"],
        ["deal zone", zone_text],
        ["price direction", direction],
        [
            "expected offtaker margin",
            f"{fmt_eur(result.expected_offtaker_margin_eur_per_mw_year)} EUR/MW/yr (annuity); "
            f"PV {fmt_eur(result.expected_offtaker_margin_pv_eur)} EUR for {result.deal_mw:g} MW",
        ],
        ["probability of offtaker loss (tenor PV)", fmt_pct(result.prob_offtaker_loss)],
        ["share of loss-making offtaker years", fmt_pct(result.prob_offtaker_loss_year)],
    ]
    for key, value in result.extras.items():
        label, kind = _EXTRA_LABELS.get(key, (key.replace("_", " "), "eur"))
        if not isinstance(value, float):
            shown = str(value)
        elif kind == "pct":
            shown = fmt_pct(value)
        elif kind == "ratio":
            shown = f"{value:.3f}"
        else:
            shown = f"{fmt_eur(value)} EUR/MW/yr"
        head.append([label, shown])
    views = [
        ["risk preference", off.risk, own.risk],
        ["benchmark", off.benchmark, own.benchmark],
        ["expected PV, EUR/MW", fmt_eur(off.expected_pv_eur_per_mw), fmt_eur(own.expected_pv_eur_per_mw)],
        [
            "certainty-equivalent PV, EUR/MW",
            fmt_eur(off.certainty_equivalent_pv_eur_per_mw),
            fmt_eur(own.certainty_equivalent_pv_eur_per_mw),
        ],
        ["risk premium (E - CE), EUR/MW", fmt_eur(off.risk_premium_eur_per_mw), fmt_eur(own.risk_premium_eur_per_mw)],
        ["CVaR of PV, EUR/MW", fmt_eur(off.cvar_pv_eur_per_mw), fmt_eur(own.cvar_pv_eur_per_mw)],
        [
            "risk-adjusted gain vs benchmark, EUR/MW/yr",
            fmt_eur(off.risk_adjusted_gain_eur_per_mw_year),
            fmt_eur(own.risk_adjusted_gain_eur_per_mw_year),
        ],
        ["probability worse off than benchmark", fmt_pct(off.prob_worse_off), fmt_pct(own.prob_worse_off)],
        [
            "annual cash P90 / P50 / P10, EUR/MW/yr",
            " / ".join(fmt_eur(off.annual_cash[k]) for k in ("p90", "p50", "p10")),
            " / ".join(fmt_eur(own.annual_cash[k]) for k in ("p90", "p50", "p10")),
        ],
    ]
    merchant_rows = [
        [r["year"], fmt_eur(r["mean"]), fmt_eur(r["p90"]), fmt_eur(r["p50"]), fmt_eur(r["p10"])]
        for r in result.merchant["by_year"]
    ]
    parts = [
        f"*{result.description}.* Series `{result.series}`, {result.years[0]}-{result.years[-1]}, deal size "
        f"{result.deal_mw:g} MW.",
        md_table(["item", "value"], head, align="ll"),
        "Both counterparties' views (per MW, present values at each party's discount rate):",
        md_table(["", "offtaker", "owner"], views, align="lll"),
    ]
    if include_merchant:
        parts += ["Merchant revenue (EUR/MW/yr):", md_table(["year", "mean", "P90", "P50", "P10"], merchant_rows)]
    return "\n\n".join(parts)


def render_waterfall(result: WaterfallResult) -> str:
    rows = []
    for row in result.table.itertuples(index=False):
        rows.append(
            [
                row.label,
                row.kind,
                row.source,
                fmt_eur(row.delta_eur_per_mw_year) if row.kind == "effect" else "",
                fmt_eur(row.level_eur_per_mw_year),
                fmt_pct(row.share_of_anchor),
                fmt_pct(row.share_days_delta_negative) if row.kind == "effect" else "",
            ]
        )
    table = md_table(
        [
            "step",
            "kind",
            "source",
            "effect EUR/MW/yr",
            "level EUR/MW/yr",
            "share of anchor",
            "days with negative effect",
        ],
        rows,
        align="lllrrrr",
    )
    header = (
        f"Basis: {result.basis} ({'P&L net of wear' if result.basis == 'net' else 'gross revenue, wear shown last'}); "
        f"common period {result.first_local_day} .. {result.last_local_day} ({result.timezone} delivery days; "
        f"{result.period_start_utc} to {result.period_end_utc} UTC; {result.n_intervals:,} intervals of "
        f"{result.interval_minutes:g} min, {result.years_equivalent:.2f} years); power {result.power_mw:g} MW."
    )
    notes = "".join(f"\n- {note}" for note in result.notes)
    return header + "\n\n" + table + (("\n\nNotes:" + notes) if notes else "")


def render_portfolio(result: PortfolioResult) -> str:
    stats_rows = []
    for row in result.stats.itertuples(index=False):
        stats_rows.append(
            [
                row.name,
                row.horizon,
                fmt_eur(row.mean_eur),
                fmt_eur(row.std_eur),
                fmt_eur(row.var_eur),
                fmt_eur(row.cvar_eur),
                fmt_eur(row.shortfall_eur),
                fmt_pct(row.prob_negative),
            ]
        )
    a = f"{result.alpha:.0%}"
    stats = md_table(
        ["asset", "horizon", "mean", "std", f"VaR {a}", f"CVaR {a}", "shortfall (mean - CVaR)", "P(negative)"],
        stats_rows,
        align="llrrrrrr",
    )
    contrib_rows = [
        [
            row.name,
            f"{row.weight:g}",
            fmt_pct(row.expected_share),
            fmt_pct(row.variance_share),
            fmt_eur(row.annual_component_shortfall_eur),
            fmt_pct(row.annual_shortfall_share),
        ]
        for row in result.contributions.itertuples(index=False)
    ]
    contributions = md_table(
        [
            "asset",
            "weight",
            "share of expected P&L",
            "share of variance",
            "annual shortfall contribution",
            "share of shortfall",
        ],
        contrib_rows,
    )
    names = list(result.assets)
    corr_rows = [[name, *[f"{result.correlation.loc[name, other]:.2f}" for other in names]] for name in names]
    correlation = md_table(["daily P&L correlation", *names], corr_rows)
    d = result.diversification
    c = result.concentration

    def num(value: float | None, decimals: int) -> str:
        return f"{value:.{decimals}f}" if value is not None and math.isfinite(value) else "n/a"

    bullets = [
        f"- Common complete days: {result.n_days} ({result.first_day} .. {result.last_day}).",
        f"- Diversification benefit, annual shortfall: {fmt_eur(d['annual_shortfall_benefit_eur'])} EUR "
        f"({fmt_pct(d['annual_shortfall_benefit_pct'])} of the standalone sum); daily shortfall: "
        f"{fmt_eur(d['daily_shortfall_benefit_eur'])} EUR ({fmt_pct(d['daily_shortfall_benefit_pct'])}).",
        f"- Concentration: HHI of expected P&L {num(c['hhi_expected_pnl'], 3)} (effective assets "
        f"{num(c['effective_assets_expected_pnl'], 2)}); HHI of annual shortfall contributions "
        f"{num(c['hhi_annual_shortfall'], 3)}; top-10 % days carry {fmt_pct(c['top_decile_day_share'])} of "
        "portfolio P&L.",
    ]
    return "\n".join(bullets) + "\n\n" + correlation + "\n\n" + stats + "\n\n" + contributions
