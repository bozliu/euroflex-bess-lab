"""Static matplotlib figures for valuation reports (PNG, light surface, deterministic metadata)."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from .scenarios import ScenarioSet

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .attribution import WaterfallResult
    from .portfolio import PortfolioResult

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"
SERIES_1 = "#2a78d6"
SERIES_2 = "#eb6834"
SERIES_3 = "#1baf7a"
POSITIVE = "#2a78d6"
NEGATIVE = "#e34948"
NEUTRAL_BAR = "#52514e"
DEEMPHASIS = "#c3c2b7"


def _figure(width: float, height: float) -> Any:
    """A standalone Agg-backed figure (no pyplot global state, safe in notebooks and threads)."""
    from matplotlib.figure import Figure

    return Figure(figsize=(width, height), facecolor=SURFACE)


def _style(ax: Any) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(BASELINE)
    ax.spines["bottom"].set_linewidth(1.0)
    ax.tick_params(colors=INK_MUTED, labelcolor=INK_SECONDARY, length=0, labelsize=9)
    ax.grid(axis="y", color=GRID, linewidth=1.0, linestyle="-")
    ax.set_axisbelow(True)


def _titles(fig: Any, title: str | None, subtitle: str | None) -> None:
    if title:
        fig.suptitle(title, x=0.01, y=0.985, ha="left", va="top", fontsize=12, color=INK, fontweight="bold")
    if subtitle:
        fig.text(0.01, 0.925, subtitle, ha="left", va="top", fontsize=9, color=INK_SECONDARY)


def _thousands(value: float, _pos: Any = None) -> str:
    """Compact tick / label text: 950, 2.5k, 120k, 1.2M."""
    magnitude = abs(value)
    if magnitude >= 1e6:
        text, suffix = f"{value / 1e6:,.1f}", "M"
    elif magnitude >= 1e3:
        text, suffix = f"{value / 1e3:,.1f}", "k"
    else:
        return f"{value:,.0f}"
    if text.endswith(".0"):
        text = text[:-2]
    return text + suffix


def _save(fig: Any, path: str | Path) -> Path:
    file = Path(path)
    file.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(file, dpi=150, facecolor=SURFACE, metadata={"Software": None})
    return file


def plot_waterfall(
    result: WaterfallResult, path: str | Path, *, title: str | None = None, subtitle: str | None = None
) -> Path:
    """Waterfall: anchor and total bars in neutral ink, positive effects blue, negative effects red."""
    from matplotlib.ticker import FuncFormatter

    table = result.table
    n = len(table)
    fig = _figure(max(7.0, 1.25 * n + 2.0), 4.6)
    ax = fig.subplots()
    fig.subplots_adjust(left=0.08, right=0.98, top=0.82, bottom=0.25)
    _style(ax)
    running = 0.0
    tops: list[float] = []
    for i, row in enumerate(table.itertuples(index=False)):
        kind = str(row.kind)
        delta = float(row.delta_eur_per_mw_year)
        if kind in ("anchor", "total"):
            bottom, height, color = 0.0, float(row.level_eur_per_mw_year), NEUTRAL_BAR
            running = float(row.level_eur_per_mw_year)
        else:
            bottom = running if delta >= 0.0 else running + delta
            height, color = abs(delta), POSITIVE if delta >= 0.0 else NEGATIVE
            running += delta
        ax.bar(i, height, bottom=bottom, width=0.56, color=color, edgecolor=SURFACE, linewidth=2.0)
        top = bottom + height
        tops.append(running)
        text = f"{delta:+,.0f}" if kind == "effect" else f"{float(row.level_eur_per_mw_year):,.0f}"
        ax.annotate(
            text,
            (i, max(top, 0.0)),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=8.5,
            color=INK_SECONDARY,
        )
    for i in range(n - 1):
        ax.plot([i + 0.28, i + 0.72], [tops[i], tops[i]], color=INK_MUTED, linewidth=0.8)
    ax.axhline(0.0, color=BASELINE, linewidth=1.0)
    ax.set_xticks(range(n))
    ax.set_xticklabels([_wrap(str(label)) for label in table["label"]], fontsize=8.5, color=INK_SECONDARY)
    ax.yaxis.set_major_formatter(FuncFormatter(_thousands))
    ax.set_ylabel("EUR per MW per year", color=INK_SECONDARY, fontsize=9)
    _titles(fig, title or "Value attribution waterfall", subtitle)
    return _save(fig, path)


def _wrap(text: str, width: int = 16) -> str:
    words, lines, current = text.split(), [], ""
    for word in words:
        if current and len(current) + 1 + len(word) > width:
            lines.append(current)
            current = word
        else:
            current = f"{current} {word}".strip()
    if current:
        lines.append(current)
    return "\n".join(lines)


def plot_annual_distribution(
    scenarios: ScenarioSet,
    path: str | Path,
    *,
    series: str | None = None,
    reference_lines: Mapping[str, float] | None = None,
    title: str | None = None,
    subtitle: str | None = None,
) -> Path:
    """Fan chart of annual revenue per MW: P90-P10 band (exceedance), P50 line, optional price references."""
    from matplotlib.ticker import FuncFormatter

    table = scenarios.annual_table(series)
    years = table["year"].to_numpy()
    fig = _figure(8.0, 4.4)
    ax = fig.subplots()
    fig.subplots_adjust(left=0.1, right=0.97, top=0.82, bottom=0.12)
    _style(ax)
    ax.fill_between(years, table["p90"], table["p10"], color=SERIES_1, alpha=0.14, linewidth=0, label="P90 - P10 band")
    ax.plot(years, table["p50"], color=SERIES_1, linewidth=2.0, solid_capstyle="round", label="P50")
    ax.plot(years, table["p50"], "o", color=SERIES_1, markersize=4.5, markeredgecolor=SURFACE, markeredgewidth=1.5)
    colors = (SERIES_2, SERIES_3, INK_SECONDARY)
    for (label, value), color in zip((reference_lines or {}).items(), colors, strict=False):
        ax.axhline(value, color=color, linewidth=1.6, label=f"{label} ({value:,.0f})")
    ax.set_xticks(years)
    ax.set_xticklabels([str(y) for y in years])
    ax.yaxis.set_major_formatter(FuncFormatter(_thousands))
    ax.set_ylabel("EUR per MW per year", color=INK_SECONDARY, fontsize=9)
    ax.legend(loc="lower left", frameon=False, fontsize=8.5, labelcolor=INK_SECONDARY)
    ax.set_ylim(bottom=min(0.0, float(table["p90"].min()) * 1.1))
    _titles(fig, title or "Annual merchant revenue distribution", subtitle)
    return _save(fig, path)


def plot_toll_sensitivity(
    table: pd.DataFrame,
    path: str | Path,
    *,
    fair_toll: float | None = None,
    break_even: float | None = None,
    title: str | None = None,
    subtitle: str | None = None,
) -> Path:
    """Two small multiples: offtaker margin (expected and risk-adjusted) and loss probability versus the toll."""
    from matplotlib.ticker import FuncFormatter, PercentFormatter

    tolls = table["toll_eur_per_mw_year"].to_numpy(dtype=float)
    fig = _figure(10.0, 4.2)
    left, right = fig.subplots(1, 2)
    fig.subplots_adjust(left=0.08, right=0.98, top=0.8, bottom=0.14, wspace=0.28)
    for ax in (left, right):
        _style(ax)
        ax.xaxis.set_major_formatter(FuncFormatter(_thousands))
        ax.set_xlabel("Toll, EUR per MW per year", color=INK_SECONDARY, fontsize=9)
        for value, label in ((break_even, "break-even"), (fair_toll, "fair toll")):
            if value is not None:
                ax.axvline(value, color=INK_MUTED, linewidth=1.0)
                ax.annotate(
                    label,
                    (value, 1.0),
                    xycoords=("data", "axes fraction"),
                    xytext=(3, -2),
                    textcoords="offset points",
                    va="top",
                    fontsize=8,
                    color=INK_MUTED,
                    rotation=90,
                )
    left.plot(tolls, table["expected_margin_eur_per_mw_year"], color=SERIES_1, linewidth=2.0, label="expected")
    left.plot(
        tolls, table["risk_adjusted_margin_eur_per_mw_year"], color=SERIES_2, linewidth=2.0, label="risk-adjusted (CE)"
    )
    left.axhline(0.0, color=BASELINE, linewidth=1.0)
    left.yaxis.set_major_formatter(FuncFormatter(_thousands))
    left.set_title("Offtaker margin, EUR per MW per year", loc="left", fontsize=9.5, color=INK)
    left.legend(frameon=False, fontsize=8.5, labelcolor=INK_SECONDARY, loc="upper right")
    right.plot(tolls, table["prob_offtaker_loss"], color=SERIES_1, linewidth=2.0)
    right.yaxis.set_major_formatter(PercentFormatter(1.0, decimals=0))
    right.set_ylim(0.0, 1.0)
    right.set_title("Probability of an offtaker loss over the tenor", loc="left", fontsize=9.5, color=INK)
    _titles(fig, title or "Toll sensitivity (offtaker view)", subtitle)
    return _save(fig, path)


def plot_portfolio_risk(
    result: PortfolioResult, path: str | Path, *, title: str | None = None, subtitle: str | None = None
) -> Path:
    """Annual shortfall (mean - CVaR) per asset, their sum and the portfolio: the gap is diversification."""
    from matplotlib.ticker import FuncFormatter

    annual = result.stats.loc[result.stats["horizon"] == "annual"].set_index("name")
    assets = list(result.assets)
    labels = [*assets, "sum of standalone", "portfolio"]
    values = [float(annual.loc[a, "shortfall_eur"]) for a in assets]
    values += [float(np.sum(values)), float(annual.loc["portfolio", "shortfall_eur"])]
    colors = [DEEMPHASIS] * (len(labels) - 1) + [SERIES_1]
    fig = _figure(8.0, 0.55 * len(labels) + 1.9)
    ax = fig.subplots()
    fig.subplots_adjust(left=0.24, right=0.9, top=0.78, bottom=0.14)
    _style(ax)
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=GRID, linewidth=1.0)
    positions = np.arange(len(labels))[::-1]
    ax.barh(positions, values, height=0.5, color=colors, edgecolor=SURFACE, linewidth=2.0)
    for pos, value in zip(positions, values, strict=True):
        ax.annotate(
            _thousands(value),
            (value, pos),
            xytext=(4, 0),
            textcoords="offset points",
            va="center",
            fontsize=8.5,
            color=INK_SECONDARY,
        )
    ax.set_yticks(positions)
    ax.set_yticklabels(labels, fontsize=9, color=INK_SECONDARY)
    ax.xaxis.set_major_formatter(FuncFormatter(_thousands))
    ax.set_xlabel(f"Annual shortfall: mean minus CVaR {result.alpha:.0%}, EUR", color=INK_SECONDARY, fontsize=9)
    _titles(fig, title or "Portfolio diversification", subtitle)
    return _save(fig, path)
