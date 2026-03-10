#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.gridspec import GridSpec
from matplotlib.ticker import MaxNLocator

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

DEFAULT_CONFIG = REPO_ROOT / "examples" / "configs" / "canonical" / "belgium_full_stack.yaml"
DEFAULT_OUTPUT = REPO_ROOT / "docs" / "assets" / "canonical-belgium-demo.gif"
FRAME_FPS = 10
OUTPUT_WIDTH = 1200
FIGURE_SIZE = (12.0, 6.75)
SCENE_FRAME_COUNTS = {
    "signals": 26,
    "revision": 34,
    "portfolio": 34,
    "waterfall": 30,
}
BACKGROUND = "#f4efe8"
PANEL = "#fffdfa"
TEXT = "#18212b"
MUTED = "#66778a"
GRID = "#d6d0c7"
BASELINE = "#98a3b3"
REVISED = "#2563eb"
REALIZED = "#c2410c"
POSITIVE = "#15803d"
NEGATIVE = "#dc2626"
A_FRR_UP = "#7c3aed"
A_FRR_DOWN = "#ec4899"
CHARGE = "#1d4ed8"
DISCHARGE = "#f97316"
ACTIVATION_UP = "#c4b5fd"
ACTIVATION_DOWN = "#f9a8d4"
POI_LIMIT = "#475569"
SOFT_BLUE = "#dbeafe"
SOFT_ORANGE = "#fed7aa"
FONT_FAMILY = "DejaVu Sans"

plt.rcParams.update(
    {
        "font.family": FONT_FAMILY,
        "axes.titlesize": 13,
        "axes.labelsize": 11,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
    }
)


def _load_runtime_helpers():
    from euroflex_bess_lab.backtesting.engine import run_walk_forward
    from euroflex_bess_lab.config import load_config
    from euroflex_bess_lab.exports import export_bids, export_schedule
    from euroflex_bess_lab.reconciliation import reconcile_run

    return run_walk_forward, load_config, export_bids, export_schedule, reconcile_run


@dataclass(frozen=True)
class WaterfallStep:
    label: str
    value: float
    kind: Literal["total", "delta"]
    color: str


@dataclass(frozen=True)
class ExportCard:
    title: str
    subtitle: str
    detail: str


@dataclass
class DemoStory:
    run_id: str
    run_dir: Path
    summary: dict[str, Any]
    site_dispatch: pd.DataFrame
    asset_dispatch: pd.DataFrame
    baseline_schedule: pd.DataFrame
    revision_schedule: pd.DataFrame
    reconciliation_breakdown: pd.DataFrame
    checkpoints: list[pd.Timestamp]
    checkpoint_labels: list[str]
    changed_intervals: pd.DataFrame
    waterfall_steps: list[WaterfallStep]
    export_cards: list[ExportCard]
    site_poi_limit_mw: float
    asset_ids: list[str]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _material_delta(value: float, *, threshold: float = 0.01) -> bool:
    return abs(value) >= threshold


def _build_waterfall_steps(reconciliation_summary: dict[str, Any], *, threshold: float = 0.01) -> list[WaterfallStep]:
    baseline_total = float(reconciliation_summary["baseline_expected_total_pnl_eur"])
    revised_total = float(reconciliation_summary["revised_expected_total_pnl_eur"])
    realized_total = float(reconciliation_summary["realized_total_pnl_eur"])
    revision_fields = (
        ("Locked commitments", float(reconciliation_summary.get("locked_commitment_opportunity_cost_eur", 0.0))),
        ("Reserve headroom", float(reconciliation_summary.get("reserve_headroom_opportunity_cost_eur", 0.0))),
    )
    realized_fields = (
        ("Activation settlement", float(reconciliation_summary.get("activation_settlement_deviation_eur", 0.0))),
        ("Imbalance settlement", float(reconciliation_summary.get("imbalance_settlement_deviation_eur", 0.0))),
        ("Availability", float(reconciliation_summary.get("availability_deviation_eur", 0.0))),
        ("Degradation drift", float(reconciliation_summary.get("degradation_cost_drift_eur", 0.0))),
        ("Forecast error", float(reconciliation_summary.get("forecast_error_eur", 0.0))),
    )

    steps = [WaterfallStep(label="Baseline", value=baseline_total, kind="total", color=BASELINE)]
    for label, value in revision_fields:
        if _material_delta(value, threshold=threshold):
            steps.append(
                WaterfallStep(label=label, value=value, kind="delta", color=POSITIVE if value >= 0 else NEGATIVE)
            )
    steps.append(WaterfallStep(label="Revised", value=revised_total, kind="total", color=REVISED))
    for label, value in realized_fields:
        if _material_delta(value, threshold=threshold):
            steps.append(
                WaterfallStep(label=label, value=value, kind="delta", color=POSITIVE if value >= 0 else NEGATIVE)
            )
    steps.append(WaterfallStep(label="Realized", value=realized_total, kind="total", color=REALIZED))
    return steps


def _checkpoint_timestamps(
    config_snapshot: dict[str, Any], timestamps: pd.Series
) -> tuple[list[pd.Timestamp], list[str]]:
    revision = config_snapshot.get("revision", {})
    labels = list(revision.get("revision_checkpoints_local", []))
    if not labels:
        return [], []
    first_timestamp = pd.Timestamp(timestamps.min())
    first_date = first_timestamp.date().isoformat()
    checkpoints = [pd.Timestamp(f"{first_date} {label}").tz_localize(first_timestamp.tz) for label in labels]
    return checkpoints, labels


def _changed_intervals(baseline_schedule: pd.DataFrame, revision_schedule: pd.DataFrame) -> pd.DataFrame:
    merged = (
        baseline_schedule[
            ["timestamp_local", "net_export_mw", "soc_mwh", "afrr_up_reserved_mw", "afrr_down_reserved_mw"]
        ]
        .rename(
            columns={
                "net_export_mw": "baseline_net_export_mw",
                "soc_mwh": "baseline_soc_mwh",
                "afrr_up_reserved_mw": "baseline_afrr_up_reserved_mw",
                "afrr_down_reserved_mw": "baseline_afrr_down_reserved_mw",
            }
        )
        .merge(
            revision_schedule[
                ["timestamp_local", "net_export_mw", "soc_mwh", "afrr_up_reserved_mw", "afrr_down_reserved_mw"]
            ].rename(
                columns={
                    "net_export_mw": "revised_net_export_mw",
                    "soc_mwh": "revised_soc_mwh",
                    "afrr_up_reserved_mw": "revised_afrr_up_reserved_mw",
                    "afrr_down_reserved_mw": "revised_afrr_down_reserved_mw",
                }
            ),
            on="timestamp_local",
            how="inner",
        )
    )
    difference_mask = ((merged["baseline_net_export_mw"] - merged["revised_net_export_mw"]).abs() > 1e-6) | (
        (merged["baseline_soc_mwh"] - merged["revised_soc_mwh"]).abs() > 1e-6
    )
    return merged.loc[difference_mask].reset_index(drop=True)


def load_demo_story(run_dir: Path) -> DemoStory:
    resolved_run_dir = run_dir.resolve()
    summary = _load_json(resolved_run_dir / "summary.json")
    config_snapshot = _load_json(resolved_run_dir / "config_snapshot.json")
    reconciliation_summary = _load_json(resolved_run_dir / "reconciliation" / "reconciliation_summary.json")
    schedule_payload = _load_json(resolved_run_dir / "exports" / "schedule-operator" / "site_schedule.json")
    schedule_manifest = _load_json(resolved_run_dir / "exports" / "schedule-operator" / "manifest.json")
    bids_payload = _load_json(resolved_run_dir / "exports" / "bids-bid_planning" / "site_bids.json")
    bids_manifest = _load_json(resolved_run_dir / "exports" / "bids-bid_planning" / "manifest.json")

    site_dispatch = (
        pd.read_parquet(resolved_run_dir / "site_dispatch.parquet")
        .sort_values("timestamp_local")
        .reset_index(drop=True)
    )
    asset_dispatch = (
        pd.read_parquet(resolved_run_dir / "asset_dispatch.parquet")
        .sort_values(["asset_id", "timestamp_local"])
        .reset_index(drop=True)
    )
    baseline_schedule = (
        pd.read_parquet(resolved_run_dir / "baseline_schedule.parquet")
        .sort_values("timestamp_local")
        .reset_index(drop=True)
    )
    revision_schedule = (
        pd.read_parquet(resolved_run_dir / "revision_schedule.parquet")
        .sort_values("timestamp_local")
        .reset_index(drop=True)
    )
    reconciliation_breakdown = (
        pd.read_parquet(resolved_run_dir / "reconciliation_breakdown.parquet")
        .sort_values("timestamp_utc")
        .reset_index(drop=True)
    )

    checkpoints, checkpoint_labels = _checkpoint_timestamps(config_snapshot, revision_schedule["timestamp_local"])
    changed = _changed_intervals(baseline_schedule, revision_schedule)
    export_cards = [
        ExportCard(
            title="Operator export",
            subtitle=f"{len(schedule_payload['records'])} schedule rows",
            detail=str(schedule_manifest["metadata"]["intended_consumer"]).replace("_", " "),
        ),
        ExportCard(
            title="Bid planning export",
            subtitle=f"{len(bids_payload['records'])} bid rows",
            detail=str(bids_manifest["metadata"]["intended_consumer"]).replace("_", " "),
        ),
    ]

    return DemoStory(
        run_id=str(summary["run_id"]),
        run_dir=resolved_run_dir,
        summary=summary,
        site_dispatch=site_dispatch,
        asset_dispatch=asset_dispatch,
        baseline_schedule=baseline_schedule,
        revision_schedule=revision_schedule,
        reconciliation_breakdown=reconciliation_breakdown,
        checkpoints=checkpoints,
        checkpoint_labels=checkpoint_labels,
        changed_intervals=changed,
        waterfall_steps=_build_waterfall_steps(reconciliation_summary),
        export_cards=export_cards,
        site_poi_limit_mw=float(summary["poi_export_limit_mw"]),
        asset_ids=sorted(asset_dispatch["asset_id"].dropna().unique().tolist()),
    )


def _execute_canonical_run(config_path: Path, artifact_root: Path) -> Path:
    run_walk_forward, load_config, _, _, _ = _load_runtime_helpers()
    config = load_config(config_path)
    config.artifacts.root_dir = artifact_root.resolve()
    result = run_walk_forward(config)
    if result.output_dir is None:
        raise RuntimeError("run_walk_forward did not return an output_dir")
    return result.output_dir.resolve()


def _ensure_story_artifacts(run_dir: Path, config_path: Path) -> None:
    _, _, export_bids, export_schedule, reconcile_run = _load_runtime_helpers()
    reconcile_run(run_dir, config_path.resolve())
    export_schedule(run_dir, profile="operator")
    export_bids(run_dir, profile="bid_planning")


def _create_figure(phase_title: str, phase_subtitle: str) -> Figure:
    fig = plt.figure(figsize=FIGURE_SIZE, facecolor=BACKGROUND)
    fig.text(0.055, 0.952, "Belgium BESS Portfolio", fontsize=22, fontweight="bold", color=TEXT)
    fig.text(0.055, 0.918, phase_title, fontsize=13, color=MUTED)
    fig.text(0.965, 0.952, "schedule_revision + aFRR", fontsize=11, color=MUTED, ha="right")
    fig.text(0.965, 0.918, phase_subtitle, fontsize=11, color=MUTED, ha="right")
    return fig


def _style_axis(ax: Axes, *, title: str, y_label: str | None = None) -> None:
    ax.set_facecolor(PANEL)
    ax.grid(True, axis="y", color=GRID, linewidth=0.8, alpha=0.9)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.tick_params(axis="both", colors=MUTED)
    ax.set_title(title, loc="left", fontsize=12, color=TEXT, pad=10, fontweight="bold")
    if y_label is not None:
        ax.set_ylabel(y_label, color=MUTED)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))


def _scene_progress(index: int, total: int) -> float:
    if total <= 1:
        return 1.0
    return index / float(total - 1)


def _visible_count(total_points: int, progress: float) -> int:
    minimum_points = min(total_points, 6)
    return max(minimum_points, min(total_points, int(round(progress * (total_points - 1))) + 1))


def _add_checkpoint_lines(ax: Axes, checkpoints: list[pd.Timestamp], labels: list[str], *, alpha: float = 0.55) -> None:
    ylim = ax.get_ylim()
    for checkpoint, label in zip(checkpoints, labels, strict=True):
        ax.axvline(checkpoint, color=GRID, linewidth=1.1, linestyle=(0, (4, 4)), alpha=alpha)
        ax.text(
            checkpoint,
            ylim[1] - (ylim[1] - ylim[0]) * 0.06,
            label,
            color=MUTED,
            fontsize=8,
            ha="center",
            va="top",
            bbox={"boxstyle": "round,pad=0.22", "facecolor": PANEL, "edgecolor": GRID, "alpha": 0.96},
        )


def _render_signals_scene(story: DemoStory, progress: float, output_path: Path) -> None:
    site = story.site_dispatch
    times = site["timestamp_local"]
    visible = _visible_count(len(site), progress)
    visible_times = times.iloc[:visible]
    figure = _create_figure("Visible inputs move first", "prices, reserve curves, activation ratios")
    grid = GridSpec(2, 1, figure=figure, top=0.84, bottom=0.1, left=0.06, right=0.97, hspace=0.34)

    price_ax = figure.add_subplot(grid[0])
    _style_axis(price_ax, title="Day-ahead forecast price", y_label="EUR/MWh")
    price_ax.plot(
        visible_times, site["day_ahead_forecast_price_eur_per_mwh"].iloc[:visible], color=REVISED, linewidth=2.5
    )
    price_ax.fill_between(
        visible_times,
        0,
        site["day_ahead_forecast_price_eur_per_mwh"].iloc[:visible],
        color=SOFT_BLUE,
        alpha=0.45,
    )
    price_ax.set_xlim(times.iloc[0], times.iloc[-1])
    price_ax.text(
        0.015,
        0.88,
        "day-ahead price",
        transform=price_ax.transAxes,
        fontsize=10,
        color=TEXT,
        bbox={"boxstyle": "round,pad=0.28", "facecolor": PANEL, "edgecolor": GRID},
    )

    reserve_ax = figure.add_subplot(grid[1], sharex=price_ax)
    _style_axis(reserve_ax, title="aFRR capacity prices + activation ratio band", y_label="EUR/MW/h")
    reserve_ax.plot(
        visible_times,
        site["afrr_capacity_up_price_forecast_eur_per_mw_per_h"].iloc[:visible],
        color=A_FRR_UP,
        linewidth=2.1,
        label="aFRR up",
    )
    reserve_ax.plot(
        visible_times,
        site["afrr_capacity_down_price_forecast_eur_per_mw_per_h"].iloc[:visible],
        color=A_FRR_DOWN,
        linewidth=2.1,
        label="aFRR down",
    )
    ratio_ax = reserve_ax.twinx()
    ratio_ax.set_facecolor("none")
    ratio_ax.fill_between(
        visible_times,
        0,
        site["afrr_activation_ratio_up_forecast"].iloc[:visible],
        color=ACTIVATION_UP,
        alpha=0.42,
    )
    ratio_ax.fill_between(
        visible_times,
        0,
        site["afrr_activation_ratio_down_forecast"].iloc[:visible],
        color=ACTIVATION_DOWN,
        alpha=0.32,
    )
    ratio_ax.set_ylim(
        0.0,
        max(
            0.12,
            float(site[["afrr_activation_ratio_up_forecast", "afrr_activation_ratio_down_forecast"]].max().max())
            * 1.25,
        ),
    )
    ratio_ax.tick_params(axis="y", colors=MUTED)
    for spine in ratio_ax.spines.values():
        spine.set_visible(False)
    ratio_ax.set_ylabel("ratio", color=MUTED)
    reserve_ax.set_xlim(times.iloc[0], times.iloc[-1])
    reserve_ax.legend(loc="upper left", frameon=False, fontsize=9)
    reserve_ax.text(
        0.985,
        0.9,
        "activation band",
        transform=reserve_ax.transAxes,
        fontsize=10,
        ha="right",
        color=TEXT,
        bbox={"boxstyle": "round,pad=0.28", "facecolor": PANEL, "edgecolor": GRID},
    )
    current_time = times.iloc[min(visible - 1, len(times) - 1)]
    price_ax.axvline(current_time, color=TEXT, linewidth=1.0, alpha=0.18)
    reserve_ax.axvline(current_time, color=TEXT, linewidth=1.0, alpha=0.18)

    figure.savefig(output_path, dpi=150, facecolor=BACKGROUND)
    plt.close(figure)


def _render_revision_scene(story: DemoStory, progress: float, output_path: Path) -> None:
    baseline = story.baseline_schedule
    revised = story.revision_schedule
    times = baseline["timestamp_local"]
    visible = _visible_count(len(baseline), progress)
    visible_times = times.iloc[:visible]
    figure = _create_figure(
        "Revision overlays the unlocked future", "baseline stays fixed where commitments are locked"
    )
    grid = GridSpec(2, 1, figure=figure, top=0.84, bottom=0.1, left=0.06, right=0.97, hspace=0.32)

    dispatch_ax = figure.add_subplot(grid[0])
    _style_axis(dispatch_ax, title="Baseline vs revised site dispatch", y_label="MW")
    dispatch_ax.axhline(0.0, color=GRID, linewidth=1.0)
    dispatch_ax.step(times, baseline["net_export_mw"], where="post", color=BASELINE, linewidth=2.3, label="baseline")
    dispatch_ax.step(
        visible_times,
        revised["net_export_mw"].iloc[:visible],
        where="post",
        color=REVISED,
        linewidth=2.5,
        label="revised",
    )
    changed = story.changed_intervals
    if not changed.empty:
        dispatch_ax.scatter(
            changed["timestamp_local"],
            changed["revised_net_export_mw"],
            color=REVISED,
            s=13,
            alpha=min(0.95, 0.2 + 0.8 * progress),
            zorder=3,
        )
    dispatch_ax.set_xlim(times.iloc[0], times.iloc[-1])
    _add_checkpoint_lines(dispatch_ax, story.checkpoints, story.checkpoint_labels)
    dispatch_ax.legend(loc="upper left", frameon=False, fontsize=9)
    dispatch_ax.text(
        0.985,
        0.9,
        "locked commitments preserved",
        transform=dispatch_ax.transAxes,
        fontsize=10,
        ha="right",
        color=TEXT,
        bbox={"boxstyle": "round,pad=0.28", "facecolor": PANEL, "edgecolor": GRID},
    )

    soc_ax = figure.add_subplot(grid[1], sharex=dispatch_ax)
    _style_axis(soc_ax, title="SoC follows the revised schedule", y_label="MWh")
    soc_ax.step(times, baseline["soc_mwh"], where="post", color=BASELINE, linewidth=2.1)
    soc_ax.step(visible_times, revised["soc_mwh"].iloc[:visible], where="post", color=REALIZED, linewidth=2.3)
    soc_ax.set_xlim(times.iloc[0], times.iloc[-1])
    _add_checkpoint_lines(soc_ax, story.checkpoints, story.checkpoint_labels, alpha=0.4)

    figure.savefig(output_path, dpi=150, facecolor=BACKGROUND)
    plt.close(figure)


def _render_portfolio_scene(story: DemoStory, progress: float, output_path: Path) -> None:
    site = story.site_dispatch
    times = site["timestamp_local"]
    visible = _visible_count(len(site), progress)
    visible_times = times.iloc[:visible]
    figure = _create_figure("Physical state and shared POI move together", "asset SoC + site allocation")
    grid = GridSpec(1, 2, figure=figure, top=0.84, bottom=0.1, left=0.06, right=0.97, wspace=0.18)

    soc_ax = figure.add_subplot(grid[0])
    _style_axis(soc_ax, title="Asset SoC", y_label="MWh")
    asset_colors = ["#0f766e", "#2563eb", "#9333ea", "#ca8a04"]
    for index, asset_id in enumerate(story.asset_ids):
        frame = story.asset_dispatch.loc[story.asset_dispatch["asset_id"] == asset_id].reset_index(drop=True)
        soc_ax.step(
            frame["timestamp_local"].iloc[:visible],
            frame["soc_mwh"].iloc[:visible],
            where="post",
            linewidth=2.2,
            color=asset_colors[index % len(asset_colors)],
            label=asset_id,
        )
    soc_ax.set_xlim(times.iloc[0], times.iloc[-1])
    soc_ax.legend(loc="upper left", frameon=False, fontsize=9)

    allocation_ax = figure.add_subplot(grid[1], sharex=soc_ax)
    _style_axis(allocation_ax, title="Site dispatch + reserve allocation", y_label="MW")
    interval_days = max(0.004, (times.iloc[1] - times.iloc[0]).total_seconds() / 86400 * 0.85)
    discharge = site["discharge_mw"].iloc[:visible].to_numpy()
    charge = site["charge_mw"].iloc[:visible].to_numpy()
    reserve_up = site["afrr_up_reserved_mw"].iloc[:visible].to_numpy()
    reserve_down = site["afrr_down_reserved_mw"].iloc[:visible].to_numpy()
    allocation_ax.bar(visible_times, discharge, width=interval_days, color=DISCHARGE, alpha=0.94, label="discharge")
    allocation_ax.bar(
        visible_times,
        reserve_up,
        width=interval_days,
        bottom=discharge,
        color=A_FRR_UP,
        alpha=0.88,
        label="reserve up",
    )
    allocation_ax.bar(visible_times, -charge, width=interval_days, color=CHARGE, alpha=0.94, label="charge")
    allocation_ax.bar(
        visible_times,
        -reserve_down,
        width=interval_days,
        bottom=-charge,
        color=A_FRR_DOWN,
        alpha=0.84,
        label="reserve down",
    )
    allocation_ax.axhline(story.site_poi_limit_mw, color=POI_LIMIT, linewidth=1.4, linestyle=(0, (4, 4)))
    allocation_ax.axhline(-story.site_poi_limit_mw, color=POI_LIMIT, linewidth=1.4, linestyle=(0, (4, 4)))
    allocation_ax.text(
        0.985,
        0.9,
        "shared POI cap",
        transform=allocation_ax.transAxes,
        fontsize=10,
        ha="right",
        color=TEXT,
        bbox={"boxstyle": "round,pad=0.28", "facecolor": PANEL, "edgecolor": GRID},
    )
    allocation_ax.set_xlim(times.iloc[0], times.iloc[-1])
    allocation_ax.legend(loc="upper left", frameon=False, fontsize=8, ncol=2)

    figure.savefig(output_path, dpi=150, facecolor=BACKGROUND)
    plt.close(figure)


def _waterfall_geometry(steps: list[WaterfallStep]) -> list[tuple[float, float]]:
    geometry: list[tuple[float, float]] = []
    running_total = 0.0
    for step in steps:
        if step.kind == "total":
            geometry.append((0.0, step.value))
            running_total = step.value
        else:
            start = running_total
            running_total += step.value
            geometry.append((start, running_total))
    return geometry


def _render_export_cards(ax: Axes, cards: list[ExportCard], alpha: float) -> None:
    ax.axis("off")
    ax.set_facecolor(PANEL)
    ax.text(0.0, 0.92, "Handoff artifacts", fontsize=12, fontweight="bold", color=TEXT, transform=ax.transAxes)
    for index, card in enumerate(cards):
        top = 0.72 - index * 0.33
        ax.text(
            0.02,
            top,
            f"{card.title}\n{card.subtitle}\n{card.detail}",
            transform=ax.transAxes,
            fontsize=10,
            color=TEXT,
            va="top",
            alpha=alpha,
            linespacing=1.5,
            bbox={"boxstyle": "round,pad=0.45", "facecolor": PANEL, "edgecolor": GRID, "alpha": alpha},
        )


def _render_waterfall_scene(story: DemoStory, progress: float, output_path: Path) -> None:
    steps = story.waterfall_steps
    geometry = _waterfall_geometry(steps)
    figure = _create_figure(
        "Expected value bridges to realized value", "only material reconciliation buckets stay on screen"
    )
    grid = GridSpec(
        1, 2, figure=figure, top=0.84, bottom=0.12, left=0.06, right=0.97, width_ratios=[2.3, 1], wspace=0.16
    )

    waterfall_ax = figure.add_subplot(grid[0])
    _style_axis(waterfall_ax, title="Baseline -> revised -> realized PnL", y_label="EUR")
    waterfall_ax.yaxis.set_major_locator(MaxNLocator(6))

    visible_bars = max(1, min(len(steps), int(progress * len(steps)) + 1))
    x_positions = list(range(len(steps)))
    lower_bounds: list[float] = [0.0]
    upper_bounds: list[float] = [0.0]
    for index, step in enumerate(steps[:visible_bars]):
        start, end = geometry[index]
        if step.kind == "total":
            bottom = min(0.0, end)
            height = abs(end)
        else:
            bottom = min(start, end)
            height = abs(end - start)
        waterfall_ax.bar(index, height, bottom=bottom, color=step.color, width=0.68, alpha=0.94)
        lower_bounds.append(bottom)
        upper_bounds.append(bottom + height)
        label_y = end if step.kind == "delta" else step.value
        waterfall_ax.text(
            index,
            label_y + (20 if label_y >= 0 else -35),
            f"{step.value:+.0f}" if step.kind == "delta" else f"{step.value:.0f}",
            ha="center",
            va="bottom" if label_y >= 0 else "top",
            fontsize=9,
            color=TEXT,
        )
    waterfall_ax.set_xticks(x_positions)
    waterfall_ax.set_xticklabels([step.label for step in steps], rotation=0)
    waterfall_ax.set_xlim(-0.6, len(steps) - 0.4)
    lower_limit = min(lower_bounds)
    upper_limit = max(upper_bounds)
    padding = max(40.0, (upper_limit - lower_limit) * 0.14)
    waterfall_ax.set_ylim(lower_limit - padding, upper_limit + padding)
    card_alpha = max(0.18, min(1.0, (progress - 0.45) / 0.55))
    cards_ax = figure.add_subplot(grid[1])
    _render_export_cards(cards_ax, story.export_cards, alpha=card_alpha)

    figure.savefig(output_path, dpi=150, facecolor=BACKGROUND)
    plt.close(figure)


def _render_frame(story: DemoStory, *, scene_name: str, progress: float, output_path: Path) -> None:
    if scene_name == "signals":
        _render_signals_scene(story, progress, output_path)
        return
    if scene_name == "revision":
        _render_revision_scene(story, progress, output_path)
        return
    if scene_name == "portfolio":
        _render_portfolio_scene(story, progress, output_path)
        return
    if scene_name == "waterfall":
        _render_waterfall_scene(story, progress, output_path)
        return
    raise ValueError(f"Unsupported scene_name: {scene_name}")


def _scene_frame_total(frame_scale: float) -> dict[str, int]:
    return {name: max(6, int(round(frame_count * frame_scale))) for name, frame_count in SCENE_FRAME_COUNTS.items()}


def _render_story_frames(story: DemoStory, frame_dir: Path, *, frame_scale: float) -> list[Path]:
    frame_paths: list[Path] = []
    scene_counts = _scene_frame_total(frame_scale)
    frame_index = 0
    for scene_name, frame_count in scene_counts.items():
        for scene_index in range(frame_count):
            progress = _scene_progress(scene_index, frame_count)
            frame_path = frame_dir / f"frame-{frame_index:03d}.png"
            _render_frame(story, scene_name=scene_name, progress=progress, output_path=frame_path)
            frame_paths.append(frame_path)
            frame_index += 1
    return frame_paths


def _build_mp4(frame_dir: Path, output_path: Path) -> None:
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-framerate",
            str(FRAME_FPS),
            "-i",
            str(frame_dir / "frame-%03d.png"),
            "-vf",
            f"scale={OUTPUT_WIDTH}:-2:flags=lanczos",
            "-pix_fmt",
            "yuv420p",
            str(output_path),
        ],
        cwd=REPO_ROOT,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _build_gif_from_mp4(mp4_path: Path, output_path: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="euroflex-demo-palette-") as temp_dir_name:
        palette_path = Path(temp_dir_name) / "palette.png"
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(mp4_path),
                "-vf",
                f"fps={FRAME_FPS},scale={OUTPUT_WIDTH}:-2:flags=lanczos,palettegen",
                str(palette_path),
            ],
            cwd=REPO_ROOT,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(mp4_path),
                "-i",
                str(palette_path),
                "-lavfi",
                f"fps={FRAME_FPS},scale={OUTPUT_WIDTH}:-2:flags=lanczos[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=3",
                str(output_path),
            ],
            cwd=REPO_ROOT,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def build_demo_gif(
    *,
    config_path: Path,
    output_path: Path,
    run_dir: Path | None = None,
    mp4_path: Path | None = None,
    frame_scale: float = 1.0,
) -> Path:
    resolved_config_path = config_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if run_dir is None:
        with tempfile.TemporaryDirectory(prefix="euroflex-demo-run-") as temp_dir_name:
            artifact_root = Path(temp_dir_name) / "artifacts"
            resolved_run_dir = _execute_canonical_run(resolved_config_path, artifact_root)
            _ensure_story_artifacts(resolved_run_dir, resolved_config_path)
            story = load_demo_story(resolved_run_dir)
            with tempfile.TemporaryDirectory(prefix="euroflex-demo-frames-") as frame_dir_name:
                frame_dir = Path(frame_dir_name)
                _render_story_frames(story, frame_dir, frame_scale=frame_scale)
                if mp4_path is None:
                    with tempfile.TemporaryDirectory(prefix="euroflex-demo-mp4-") as mp4_dir_name:
                        master_mp4 = Path(mp4_dir_name) / "canonical-belgium-demo.mp4"
                        _build_mp4(frame_dir, master_mp4)
                        _build_gif_from_mp4(master_mp4, output_path)
                else:
                    mp4_path.parent.mkdir(parents=True, exist_ok=True)
                    _build_mp4(frame_dir, mp4_path)
                    _build_gif_from_mp4(mp4_path, output_path)
    else:
        resolved_run_dir = run_dir.resolve()
        _ensure_story_artifacts(resolved_run_dir, resolved_config_path)
        story = load_demo_story(resolved_run_dir)
        with tempfile.TemporaryDirectory(prefix="euroflex-demo-frames-") as frame_dir_name:
            frame_dir = Path(frame_dir_name)
            _render_story_frames(story, frame_dir, frame_scale=frame_scale)
            if mp4_path is None:
                with tempfile.TemporaryDirectory(prefix="euroflex-demo-mp4-") as mp4_dir_name:
                    master_mp4 = Path(mp4_dir_name) / "canonical-belgium-demo.mp4"
                    _build_mp4(frame_dir, master_mp4)
                    _build_gif_from_mp4(master_mp4, output_path)
            else:
                mp4_path.parent.mkdir(parents=True, exist_ok=True)
                _build_mp4(frame_dir, mp4_path)
                _build_gif_from_mp4(mp4_path, output_path)

    return output_path.resolve()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render the README demo GIF for the Belgium canonical path.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Canonical config to execute.")
    parser.add_argument("--run-dir", type=Path, default=None, help="Optional existing run directory to render from.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Target GIF path.")
    parser.add_argument("--write-mp4", type=Path, default=None, help="Optional path for the intermediate MP4.")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    output_path = build_demo_gif(
        config_path=args.config,
        output_path=args.output,
        run_dir=args.run_dir,
        mp4_path=args.write_mp4,
    )
    print(output_path)


if __name__ == "__main__":
    main()
