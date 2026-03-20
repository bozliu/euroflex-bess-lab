#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.gridspec import GridSpec
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

DEFAULT_CONFIG = REPO_ROOT / "examples" / "configs" / "canonical" / "netherlands_full_stack.yaml"
DEFAULT_OUTPUT = REPO_ROOT / "docs" / "assets" / "tennet-live-workflow.gif"
FRAME_FPS = 10
OUTPUT_WIDTH = 1200
OUTPUT_HEIGHT = 675
FIGURE_SIZE = (12.0, 6.75)
SCENE_FRAME_COUNTS = {
    "live_input": 28,
    "normalize": 28,
    "revision": 36,
    "handoff": 28,
}

BACKGROUND = "#f4efe8"
PANEL = "#fffdfa"
TEXT = "#18212b"
MUTED = "#66778a"
GRID = "#d6d0c7"
ACCENT = "#1d4ed8"
ACCENT_SOFT = "#dbeafe"
TEAL = "#0f766e"
TEAL_SOFT = "#ccfbf1"
ORANGE = "#ea580c"
ORANGE_SOFT = "#fed7aa"
GREEN = "#15803d"
GREEN_SOFT = "#dcfce7"
PURPLE = "#7c3aed"
PURPLE_SOFT = "#ede9fe"
BASELINE = "#98a3b3"
REVISED = "#2563eb"
SOC = "#ea580c"
LOCK = "#475569"
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
class ExportCard:
    title: str
    subtitle: str
    detail: str


@dataclass
class TenneTHeroStory:
    run_id: str
    run_dir: Path
    summary: dict[str, Any]
    site_dispatch: pd.DataFrame
    baseline_schedule: pd.DataFrame
    revision_schedule: pd.DataFrame
    changed_intervals: pd.DataFrame
    checkpoints: list[pd.Timestamp]
    checkpoint_labels: list[str]
    export_cards: list[ExportCard]
    live_signal: pd.DataFrame
    live_meta: dict[str, Any]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _execute_run(config_path: Path, artifact_root: Path) -> Path:
    run_walk_forward, load_config, _, _, _ = _load_runtime_helpers()
    config = load_config(config_path)
    config.artifacts.root_dir = artifact_root.resolve()
    result = run_walk_forward(config)
    if result.output_dir is None:
        raise RuntimeError("run_walk_forward did not return an output directory")
    return result.output_dir.resolve()


def _ensure_story_artifacts(run_dir: Path, config_path: Path) -> None:
    _, _, export_bids, export_schedule, reconcile_run = _load_runtime_helpers()
    reconcile_run(run_dir, config_path.resolve())
    export_schedule(run_dir, profile="operator")
    export_bids(run_dir, profile="bid_planning")


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
        baseline_schedule[["timestamp_local", "net_export_mw", "soc_mwh"]]
        .rename(columns={"net_export_mw": "baseline_net_export_mw", "soc_mwh": "baseline_soc_mwh"})
        .merge(
            revision_schedule[["timestamp_local", "net_export_mw", "soc_mwh"]].rename(
                columns={"net_export_mw": "revised_net_export_mw", "soc_mwh": "revised_soc_mwh"}
            ),
            on="timestamp_local",
            how="inner",
        )
    )
    difference_mask = ((merged["baseline_net_export_mw"] - merged["revised_net_export_mw"]).abs() > 1e-6) | (
        (merged["baseline_soc_mwh"] - merged["revised_soc_mwh"]).abs() > 1e-6
    )
    return merged.loc[difference_mask].reset_index(drop=True)


def _coerce_frame(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        frame = pd.read_parquet(path)
    elif path.suffix == ".csv":
        frame = pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported live signal path: {path}")
    if "timestamp_local" not in frame.columns and "timestamp_utc" in frame.columns:
        frame["timestamp_local"] = pd.to_datetime(frame["timestamp_utc"], utc=True).dt.tz_convert("Europe/Amsterdam")
    elif "timestamp_local" in frame.columns:
        frame["timestamp_local"] = pd.to_datetime(frame["timestamp_local"], utc=True, format="mixed").dt.tz_convert(
            "Europe/Amsterdam"
        )
    return frame


def _align_signal_frame_to_story_day(signal_frame: pd.DataFrame, site_dispatch: pd.DataFrame) -> pd.DataFrame:
    if signal_frame.empty or site_dispatch.empty:
        return signal_frame
    aligned = signal_frame.copy()
    story_start = pd.Timestamp(site_dispatch["timestamp_local"].iloc[0])
    if story_start.tzinfo is None:
        raise ValueError("Expected site dispatch timestamps to be timezone-aware")
    signal_local = pd.to_datetime(aligned["timestamp_local"], utc=True, format="mixed").dt.tz_convert(story_start.tz)
    story_day_start = story_start.normalize()
    aligned["timestamp_local"] = story_day_start + (signal_local - signal_local.dt.normalize())
    return aligned.sort_values("timestamp_local").reset_index(drop=True)


def _resolve_live_signal_frame(
    site_dispatch: pd.DataFrame,
    *,
    signal_path: Path | None,
    signal_meta_path: Path | None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if signal_path is not None:
        frame = _coerce_frame(signal_path)
        signal_column = next(
            (
                candidate
                for candidate in (
                    "price_eur_per_mwh",
                    "imbalance_shortage_price_eur_per_mwh",
                    "day_ahead_forecast_price_eur_per_mwh",
                )
                if candidate in frame.columns
            ),
            None,
        )
        if signal_column is None:
            raise ValueError("Live signal input does not contain a supported price column")
        signal_frame = (
            frame[["timestamp_local", signal_column]]
            .rename(columns={signal_column: "signal_value"})
            .sort_values("timestamp_local")
            .reset_index(drop=True)
        )
        signal_frame = _align_signal_frame_to_story_day(signal_frame, site_dispatch)
        metadata = _load_json(signal_meta_path) if signal_meta_path is not None else {}
        return signal_frame, metadata

    fallback_column = next(
        (
            candidate
            for candidate in (
                "day_ahead_forecast_price_eur_per_mwh",
                "day_ahead_actual_price_eur_per_mwh",
                "afrr_capacity_up_price_forecast_eur_per_mw_per_h",
            )
            if candidate in site_dispatch.columns
        ),
        None,
    )
    if fallback_column is None:
        raise ValueError("Site dispatch does not include a supported fallback signal column")
    signal_frame = (
        site_dispatch[["timestamp_local", fallback_column]]
        .rename(columns={fallback_column: "signal_value"})
        .sort_values("timestamp_local")
        .reset_index(drop=True)
    )
    metadata = {
        "source_operator": "Dutch workflow",
        "environment": "workflow",
        "base_url": None,
        "normalization_name": "workflow_signal_projection",
        "local_timezone": "Europe/Amsterdam",
    }
    return signal_frame, metadata


def load_tennet_hero_story(
    run_dir: Path,
    *,
    signal_path: Path | None = None,
    signal_meta_path: Path | None = None,
) -> TenneTHeroStory:
    resolved_run_dir = run_dir.resolve()
    summary = _load_json(resolved_run_dir / "summary.json")
    config_snapshot = _load_json(resolved_run_dir / "config_snapshot.json")
    schedule_manifest = _load_json(resolved_run_dir / "exports" / "schedule-operator" / "manifest.json")
    bids_manifest = _load_json(resolved_run_dir / "exports" / "bids-bid_planning" / "manifest.json")
    schedule_payload = _load_json(resolved_run_dir / "exports" / "schedule-operator" / "site_schedule.json")
    bids_payload = _load_json(resolved_run_dir / "exports" / "bids-bid_planning" / "site_bids.json")

    site_dispatch = pd.read_parquet(resolved_run_dir / "site_dispatch.parquet").sort_values("timestamp_local")
    baseline_schedule = pd.read_parquet(resolved_run_dir / "baseline_schedule.parquet").sort_values("timestamp_local")
    revision_schedule = pd.read_parquet(resolved_run_dir / "revision_schedule.parquet").sort_values("timestamp_local")
    checkpoints, checkpoint_labels = _checkpoint_timestamps(config_snapshot, revision_schedule["timestamp_local"])
    live_signal, live_meta = _resolve_live_signal_frame(
        site_dispatch,
        signal_path=signal_path,
        signal_meta_path=signal_meta_path,
    )

    export_cards = [
        ExportCard(
            title="operator export",
            subtitle=f"{len(schedule_payload['records'])} schedule rows",
            detail=str(schedule_manifest["metadata"]["intended_consumer"]).replace("_", " "),
        ),
        ExportCard(
            title="bid_planning export",
            subtitle=f"{len(bids_payload['records'])} bid rows",
            detail=str(bids_manifest["metadata"]["intended_consumer"]).replace("_", " "),
        ),
    ]

    return TenneTHeroStory(
        run_id=str(summary["run_id"]),
        run_dir=resolved_run_dir,
        summary=summary,
        site_dispatch=site_dispatch.reset_index(drop=True),
        baseline_schedule=baseline_schedule.reset_index(drop=True),
        revision_schedule=revision_schedule.reset_index(drop=True),
        changed_intervals=_changed_intervals(baseline_schedule, revision_schedule),
        checkpoints=checkpoints,
        checkpoint_labels=checkpoint_labels,
        export_cards=export_cards,
        live_signal=live_signal,
        live_meta=live_meta,
    )


def _scene_progress(index: int, total: int) -> float:
    if total <= 1:
        return 1.0
    return index / float(total - 1)


def _visible_count(total_points: int, progress: float) -> int:
    minimum_points = min(total_points, 6)
    return max(minimum_points, min(total_points, int(round(progress * (total_points - 1))) + 1))


def _create_figure(title: str, subtitle: str) -> Figure:
    figure = plt.figure(figsize=FIGURE_SIZE, facecolor=BACKGROUND)
    figure.text(0.055, 0.95, title, fontsize=22, fontweight="bold", color=TEXT)
    figure.text(0.055, 0.915, subtitle, fontsize=12, color=MUTED)
    figure.text(0.965, 0.95, "euroflex_bess_lab", fontsize=11, color=MUTED, ha="right")
    figure.text(0.965, 0.915, "Dutch live workflow", fontsize=11, color=MUTED, ha="right")
    return figure


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


def _add_round_box(
    ax: Axes,
    *,
    x: float,
    y: float,
    width: float,
    height: float,
    facecolor: str,
    edgecolor: str = GRID,
    linewidth: float = 1.0,
    alpha: float = 1.0,
) -> FancyBboxPatch:
    patch = FancyBboxPatch(
        (x, y),
        width,
        height,
        boxstyle="round,pad=0.02,rounding_size=0.03",
        transform=ax.transAxes,
        facecolor=facecolor,
        edgecolor=edgecolor,
        linewidth=linewidth,
        alpha=alpha,
    )
    ax.add_patch(patch)
    return patch


def _add_badge(ax: Axes, *, x: float, y: float, label: str, facecolor: str, alpha: float = 1.0) -> None:
    ax.text(
        x,
        y,
        label,
        transform=ax.transAxes,
        fontsize=9,
        color=TEXT,
        va="center",
        ha="left",
        alpha=alpha,
        bbox={
            "boxstyle": "round,pad=0.28",
            "facecolor": facecolor,
            "edgecolor": "none",
            "alpha": alpha,
        },
    )


def _add_export_card(ax: Axes, *, y: float, card: ExportCard, alpha: float) -> None:
    _add_round_box(ax, x=0.04, y=y, width=0.9, height=0.23, facecolor=PANEL, alpha=alpha)
    ax.text(0.08, y + 0.17, card.title, transform=ax.transAxes, fontsize=13, color=TEXT, fontweight="bold", alpha=alpha)
    ax.text(0.08, y + 0.1, card.subtitle, transform=ax.transAxes, fontsize=10, color=MUTED, alpha=alpha)
    ax.text(0.08, y + 0.05, card.detail, transform=ax.transAxes, fontsize=10, color=ACCENT, alpha=alpha)
    ax.scatter([0.88], [y + 0.17], transform=ax.transAxes, s=52, color=GREEN, alpha=alpha, zorder=3)


def _render_live_input_scene(story: TenneTHeroStory, progress: float, output_path: Path) -> None:
    figure = _create_figure("From TenneT live data to operator handoff", "live Dutch inputs start the story")
    grid = GridSpec(1, 2, figure=figure, top=0.84, bottom=0.1, left=0.05, right=0.97, wspace=0.16)

    card_ax = figure.add_subplot(grid[0])
    card_ax.axis("off")
    card_ax.set_facecolor(PANEL)
    _add_round_box(card_ax, x=0.02, y=0.08, width=0.92, height=0.78, facecolor=PANEL)
    card_ax.text(
        0.08, 0.74, "Dutch live market data", transform=card_ax.transAxes, fontsize=20, fontweight="bold", color=TEXT
    )
    card_ax.text(
        0.08,
        0.58,
        "Live Dutch inputs enter a rule-aware\nscheduling layer with tracked freshness,\nnormalization, and provenance.",
        transform=card_ax.transAxes,
        fontsize=11,
        color=MUTED,
        linespacing=1.45,
    )
    badge_alpha = max(0.2, min(1.0, progress * 1.6))
    _add_badge(card_ax, x=0.08, y=0.54, label="fetched now", facecolor=GREEN_SOFT, alpha=badge_alpha)
    _add_badge(card_ax, x=0.08, y=0.45, label="freshness tracked", facecolor=TEAL_SOFT, alpha=max(0.0, progress - 0.12))
    _add_badge(
        card_ax, x=0.08, y=0.36, label="production data path", facecolor=ACCENT_SOFT, alpha=max(0.0, progress - 0.24)
    )
    _add_badge(card_ax, x=0.08, y=0.18, label="live ingest", facecolor=ORANGE_SOFT, alpha=max(0.2, progress))
    source_operator = story.live_meta.get("source_operator") or "TenneT"
    card_ax.text(0.08, 0.26, f"source: {source_operator}", transform=card_ax.transAxes, fontsize=10, color=MUTED)

    signal_ax = figure.add_subplot(grid[1])
    _style_axis(signal_ax, title="Live Dutch market signal", y_label="EUR/MWh")
    signal = story.live_signal
    visible = _visible_count(len(signal), progress)
    visible_frame = signal.iloc[:visible]
    signal_ax.plot(signal["timestamp_local"], signal["signal_value"], color=GRID, linewidth=1.4, alpha=0.65)
    signal_ax.plot(
        visible_frame["timestamp_local"],
        visible_frame["signal_value"],
        color=ACCENT,
        linewidth=2.7,
    )
    signal_ax.fill_between(
        visible_frame["timestamp_local"],
        0,
        visible_frame["signal_value"],
        color=ACCENT_SOFT,
        alpha=0.55,
    )
    signal_ax.set_xlim(signal["timestamp_local"].iloc[0], signal["timestamp_local"].iloc[-1])
    signal_ax.text(
        0.97,
        0.9,
        "live-supported",
        transform=signal_ax.transAxes,
        ha="right",
        fontsize=10,
        color=TEXT,
        bbox={"boxstyle": "round,pad=0.28", "facecolor": PANEL, "edgecolor": GRID},
    )

    figure.savefig(output_path, dpi=100, facecolor=BACKGROUND)
    plt.close(figure)


def _render_normalize_scene(story: TenneTHeroStory, progress: float, output_path: Path) -> None:
    figure = _create_figure("Normalization makes live data operational", "trusted ingest is more than a raw API pull")
    grid = GridSpec(1, 3, figure=figure, top=0.84, bottom=0.1, left=0.05, right=0.97, wspace=0.08)

    input_ax = figure.add_subplot(grid[0])
    transform_ax = figure.add_subplot(grid[1])
    output_ax = figure.add_subplot(grid[2])
    for ax in (input_ax, transform_ax, output_ax):
        ax.axis("off")
        ax.set_facecolor(PANEL)

    _add_round_box(input_ax, x=0.04, y=0.16, width=0.88, height=0.66, facecolor=PANEL)
    input_ax.text(0.1, 0.72, "live input", transform=input_ax.transAxes, fontsize=15, fontweight="bold", color=TEXT)
    for index, color in enumerate((ACCENT_SOFT, PURPLE_SOFT, ORANGE_SOFT)):
        alpha = min(1.0, max(0.18, progress * 1.8 - index * 0.15))
        _add_round_box(
            input_ax,
            x=0.12,
            y=0.5 - index * 0.14,
            width=0.68,
            height=0.08,
            facecolor=color,
            alpha=alpha,
        )
    input_ax.text(0.1, 0.22, "market timestamps + price states", transform=input_ax.transAxes, fontsize=10, color=MUTED)

    _add_round_box(transform_ax, x=0.08, y=0.24, width=0.84, height=0.52, facecolor=ACCENT_SOFT)
    transform_ax.text(
        0.5,
        0.56,
        "normalize + validate\n+ provenance",
        transform=transform_ax.transAxes,
        fontsize=18,
        color=TEXT,
        fontweight="bold",
        ha="center",
        va="center",
        linespacing=1.45,
    )
    arrow_alpha = max(0.3, progress)
    transform_ax.add_patch(
        FancyArrowPatch(
            (0.02, 0.5),
            (0.98, 0.5),
            transform=transform_ax.transAxes,
            arrowstyle="-|>",
            mutation_scale=18,
            linewidth=1.6,
            color=ACCENT,
            alpha=arrow_alpha,
        )
    )

    _add_round_box(output_ax, x=0.04, y=0.12, width=0.9, height=0.74, facecolor=PANEL)
    output_ax.text(
        0.1, 0.76, "workflow-ready series", transform=output_ax.transAxes, fontsize=15, fontweight="bold", color=TEXT
    )
    _add_badge(output_ax, x=0.1, y=0.63, label="schema checked", facecolor=GREEN_SOFT, alpha=max(0.2, progress))
    _add_badge(output_ax, x=0.1, y=0.51, label="timezone aligned", facecolor=TEAL_SOFT, alpha=max(0.0, progress - 0.12))
    _add_badge(output_ax, x=0.1, y=0.39, label="source tracked", facecolor=PURPLE_SOFT, alpha=max(0.0, progress - 0.22))
    _add_badge(
        output_ax,
        x=0.1,
        y=0.27,
        label="cache / fetch metadata",
        facecolor=ORANGE_SOFT,
        alpha=max(0.0, progress - 0.32),
    )
    output_ax.text(0.1, 0.15, "validated + normalized", transform=output_ax.transAxes, fontsize=10, color=MUTED)

    figure.savefig(output_path, dpi=100, facecolor=BACKGROUND)
    plt.close(figure)


def _render_revision_scene(story: TenneTHeroStory, progress: float, output_path: Path) -> None:
    figure = _create_figure("Revision-aware planning changes the schedule", "live-supported Dutch workflow")
    grid = GridSpec(3, 1, figure=figure, top=0.84, bottom=0.1, left=0.06, right=0.97, hspace=0.24)

    signal_ax = figure.add_subplot(grid[0])
    dispatch_ax = figure.add_subplot(grid[1], sharex=signal_ax)
    soc_ax = figure.add_subplot(grid[2], sharex=signal_ax)

    _style_axis(signal_ax, title="Dutch market signal", y_label="EUR/MWh")
    _style_axis(dispatch_ax, title="Baseline vs revised schedule", y_label="MW")
    _style_axis(soc_ax, title="State of charge", y_label="MWh")

    signal = story.live_signal
    baseline = story.baseline_schedule
    revised = story.revision_schedule
    checkpoint = story.checkpoints[0] if story.checkpoints else revised["timestamp_local"].iloc[len(revised) // 3]
    end_time = revised["timestamp_local"].iloc[-1]
    sweep_time = checkpoint + progress * (end_time - checkpoint)

    signal_ax.plot(signal["timestamp_local"], signal["signal_value"], color=GRID, linewidth=1.4, alpha=0.75)
    visible_signal = signal.loc[signal["timestamp_local"] <= sweep_time]
    signal_ax.plot(visible_signal["timestamp_local"], visible_signal["signal_value"], color=ACCENT, linewidth=2.4)
    signal_ax.set_xlim(revised["timestamp_local"].iloc[0], revised["timestamp_local"].iloc[-1])

    dispatch_ax.axhline(0.0, color=GRID, linewidth=1.0)
    dispatch_ax.step(
        baseline["timestamp_local"],
        baseline["net_export_mw"],
        where="post",
        color=BASELINE,
        linewidth=2.1,
        label="baseline",
    )
    revised_visible = revised.loc[revised["timestamp_local"] <= sweep_time]
    dispatch_ax.step(
        revised_visible["timestamp_local"],
        revised_visible["net_export_mw"],
        where="post",
        color=REVISED,
        linewidth=2.4,
        label="revised",
    )
    dispatch_ax.axvspan(revised["timestamp_local"].iloc[0], checkpoint, color=GRID, alpha=0.16)
    dispatch_ax.axvline(sweep_time, color=ACCENT, linewidth=1.6, linestyle=(0, (4, 4)))
    changed = story.changed_intervals
    if not changed.empty:
        changed_visible = changed.loc[changed["timestamp_local"] <= sweep_time]
        dispatch_ax.scatter(
            changed_visible["timestamp_local"],
            changed_visible["revised_net_export_mw"],
            s=16,
            color=REVISED,
            alpha=0.95,
            zorder=4,
        )
    dispatch_ax.legend(loc="upper left", frameon=False, fontsize=9)
    dispatch_ax.text(
        0.98,
        0.9,
        "locked commitments preserved",
        transform=dispatch_ax.transAxes,
        ha="right",
        fontsize=10,
        color=TEXT,
        bbox={"boxstyle": "round,pad=0.28", "facecolor": PANEL, "edgecolor": GRID},
    )
    dispatch_ax.text(0.02, 0.08, "revision checkpoint", transform=dispatch_ax.transAxes, fontsize=10, color=MUTED)

    soc_ax.step(
        baseline["timestamp_local"],
        baseline["soc_mwh"],
        where="post",
        color=BASELINE,
        linewidth=2.0,
    )
    soc_ax.step(
        revised_visible["timestamp_local"],
        revised_visible["soc_mwh"],
        where="post",
        color=SOC,
        linewidth=2.4,
    )
    soc_ax.axvspan(revised["timestamp_local"].iloc[0], checkpoint, color=GRID, alpha=0.16)
    soc_ax.axvline(sweep_time, color=ACCENT, linewidth=1.6, linestyle=(0, (4, 4)))

    figure.savefig(output_path, dpi=100, facecolor=BACKGROUND)
    plt.close(figure)


def _render_handoff_scene(story: TenneTHeroStory, progress: float, output_path: Path) -> None:
    figure = _create_figure(
        "Operator-facing outputs close the loop", "human-in-the-loop handoff, not opaque research output"
    )
    grid = GridSpec(1, 2, figure=figure, top=0.84, bottom=0.1, left=0.05, right=0.97, wspace=0.12)

    preview_ax = figure.add_subplot(grid[0])
    cards_ax = figure.add_subplot(grid[1])
    preview_ax.axis("off")
    cards_ax.axis("off")
    preview_ax.set_facecolor(PANEL)
    cards_ax.set_facecolor(PANEL)

    _add_round_box(preview_ax, x=0.04, y=0.1, width=0.9, height=0.78, facecolor=PANEL)
    preview_ax.text(
        0.08, 0.8, "workflow outputs", transform=preview_ax.transAxes, fontsize=15, fontweight="bold", color=TEXT
    )
    _add_badge(preview_ax, x=0.08, y=0.67, label="operator export", facecolor=ACCENT_SOFT, alpha=max(0.25, progress))
    _add_badge(
        preview_ax, x=0.08, y=0.57, label="bid_planning export", facecolor=PURPLE_SOFT, alpha=max(0.2, progress - 0.08)
    )
    _add_badge(preview_ax, x=0.08, y=0.47, label="source tracked", facecolor=TEAL_SOFT, alpha=max(0.1, progress - 0.16))
    _add_badge(
        preview_ax,
        x=0.08,
        y=0.37,
        label="validated + normalized",
        facecolor=GREEN_SOFT,
        alpha=max(0.1, progress - 0.24),
    )
    preview_ax.text(
        0.08,
        0.18,
        "live-supported Dutch workflow",
        transform=preview_ax.transAxes,
        fontsize=16,
        color=TEXT,
        fontweight="bold",
    )

    cards_ax.set_xlim(0, 1)
    cards_ax.set_ylim(0, 1)
    first_alpha = max(0.0, min(1.0, progress * 1.8))
    second_alpha = max(0.0, min(1.0, (progress - 0.18) * 1.8))
    _add_export_card(cards_ax, y=0.58, card=story.export_cards[0], alpha=first_alpha)
    _add_export_card(cards_ax, y=0.28, card=story.export_cards[1], alpha=second_alpha)
    cards_ax.text(
        0.04,
        0.08,
        "source: TenneT | normalized | validated | export-ready",
        transform=cards_ax.transAxes,
        fontsize=9,
        color=MUTED,
        alpha=max(first_alpha, second_alpha, 0.3),
    )

    figure.savefig(output_path, dpi=100, facecolor=BACKGROUND)
    plt.close(figure)


def _render_frame(story: TenneTHeroStory, *, scene_name: str, progress: float, output_path: Path) -> None:
    if scene_name == "live_input":
        _render_live_input_scene(story, progress, output_path)
        return
    if scene_name == "normalize":
        _render_normalize_scene(story, progress, output_path)
        return
    if scene_name == "revision":
        _render_revision_scene(story, progress, output_path)
        return
    if scene_name == "handoff":
        _render_handoff_scene(story, progress, output_path)
        return
    raise ValueError(f"Unsupported scene_name: {scene_name}")


def _scene_frame_total(frame_scale: float) -> dict[str, int]:
    return {name: max(6, int(round(frame_count * frame_scale))) for name, frame_count in SCENE_FRAME_COUNTS.items()}


def _render_story_frames(story: TenneTHeroStory, frame_dir: Path, *, frame_scale: float) -> list[Path]:
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


def _resolve_ffmpeg_binary() -> str | None:
    return shutil.which("ffmpeg")


def _build_mp4(frame_dir: Path, output_path: Path, *, ffmpeg_binary: str) -> None:
    subprocess.run(
        [
            ffmpeg_binary,
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


def _build_gif_from_frames(frame_dir: Path, output_path: Path, *, ffmpeg_binary: str) -> None:
    with tempfile.TemporaryDirectory(prefix="euroflex-tennet-palette-") as temp_dir_name:
        palette_path = Path(temp_dir_name) / "palette.png"
        subprocess.run(
            [
                ffmpeg_binary,
                "-y",
                "-framerate",
                str(FRAME_FPS),
                "-i",
                str(frame_dir / "frame-%03d.png"),
                "-vf",
                f"fps={FRAME_FPS},scale={OUTPUT_WIDTH}:{OUTPUT_HEIGHT}:flags=lanczos,palettegen",
                str(palette_path),
            ],
            cwd=REPO_ROOT,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                ffmpeg_binary,
                "-y",
                "-framerate",
                str(FRAME_FPS),
                "-i",
                str(frame_dir / "frame-%03d.png"),
                "-i",
                str(palette_path),
                "-lavfi",
                f"fps={FRAME_FPS},scale={OUTPUT_WIDTH}:{OUTPUT_HEIGHT}:flags=lanczos[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=3",
                str(output_path),
            ],
            cwd=REPO_ROOT,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def _build_gif_with_pillow(frame_paths: list[Path], output_path: Path) -> None:
    try:
        from PIL import Image
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Animated GIF rendering without `ffmpeg` requires Pillow. "
            "Install the dev dependencies or activate the `dl` conda environment."
        ) from exc

    frames = []
    for frame_path in frame_paths:
        with Image.open(frame_path) as image:
            frames.append(image.convert("RGB"))

    first_frame, *remaining_frames = frames
    first_frame.save(
        output_path,
        save_all=True,
        append_images=remaining_frames,
        duration=max(1, int(round(1000 / FRAME_FPS))),
        loop=0,
        optimize=False,
        disposal=2,
    )


def _render_story_outputs(
    story: TenneTHeroStory,
    *,
    output_path: Path,
    mp4_path: Path | None,
    frame_scale: float,
) -> None:
    with tempfile.TemporaryDirectory(prefix="euroflex-tennet-frames-") as frame_dir_name:
        frame_dir = Path(frame_dir_name)
        frame_paths = _render_story_frames(story, frame_dir, frame_scale=frame_scale)
        ffmpeg_binary = _resolve_ffmpeg_binary()

        if mp4_path is not None:
            if ffmpeg_binary is None:
                raise RuntimeError(
                    "`--write-mp4` requires `ffmpeg` on PATH. "
                    "Activate the `dl` conda environment to render the full-quality MP4 + GIF bundle."
                )
            mp4_path.parent.mkdir(parents=True, exist_ok=True)
            _build_mp4(frame_dir, mp4_path, ffmpeg_binary=ffmpeg_binary)
            _build_gif_from_frames(frame_dir, output_path, ffmpeg_binary=ffmpeg_binary)
            return

        if ffmpeg_binary is None:
            _build_gif_with_pillow(frame_paths, output_path)
            return

        with tempfile.TemporaryDirectory(prefix="euroflex-tennet-mp4-") as mp4_dir_name:
            master_mp4 = Path(mp4_dir_name) / "tennet-live-workflow.mp4"
            _build_mp4(frame_dir, master_mp4, ffmpeg_binary=ffmpeg_binary)
            _build_gif_from_frames(frame_dir, output_path, ffmpeg_binary=ffmpeg_binary)


def build_tennet_hero_gif(
    *,
    config_path: Path,
    output_path: Path,
    run_dir: Path | None = None,
    mp4_path: Path | None = None,
    signal_path: Path | None = None,
    signal_meta_path: Path | None = None,
    frame_scale: float = 1.0,
) -> Path:
    resolved_config_path = config_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if run_dir is None:
        with tempfile.TemporaryDirectory(prefix="euroflex-tennet-run-") as temp_dir_name:
            artifact_root = Path(temp_dir_name) / "artifacts"
            resolved_run_dir = _execute_run(resolved_config_path, artifact_root)
            _ensure_story_artifacts(resolved_run_dir, resolved_config_path)
            story = load_tennet_hero_story(
                resolved_run_dir,
                signal_path=signal_path.resolve() if signal_path is not None else None,
                signal_meta_path=signal_meta_path.resolve() if signal_meta_path is not None else None,
            )
            _render_story_outputs(story, output_path=output_path, mp4_path=mp4_path, frame_scale=frame_scale)
    else:
        resolved_run_dir = run_dir.resolve()
        _ensure_story_artifacts(resolved_run_dir, resolved_config_path)
        story = load_tennet_hero_story(
            resolved_run_dir,
            signal_path=signal_path.resolve() if signal_path is not None else None,
            signal_meta_path=signal_meta_path.resolve() if signal_meta_path is not None else None,
        )
        _render_story_outputs(story, output_path=output_path, mp4_path=mp4_path, frame_scale=frame_scale)

    return output_path.resolve()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render the README hero GIF for the Dutch TenneT live workflow.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Dutch canonical config to execute.")
    parser.add_argument("--run-dir", type=Path, default=None, help="Optional existing run directory to render from.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Target GIF path.")
    parser.add_argument("--write-mp4", type=Path, default=None, help="Optional path for the MP4 output.")
    parser.add_argument("--signal-parquet", type=Path, default=None, help="Optional live-signal parquet or CSV path.")
    parser.add_argument("--signal-meta", type=Path, default=None, help="Optional metadata JSON for the live signal.")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    output_path = build_tennet_hero_gif(
        config_path=args.config,
        output_path=args.output,
        run_dir=args.run_dir,
        mp4_path=args.write_mp4,
        signal_path=args.signal_parquet,
        signal_meta_path=args.signal_meta,
    )
    print(output_path)


if __name__ == "__main__":
    main()
