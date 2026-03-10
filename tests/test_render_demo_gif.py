from __future__ import annotations

import importlib.util
import shutil
import sys
from pathlib import Path

import pytest

from euroflex_bess_lab.backtesting.engine import run_walk_forward
from euroflex_bess_lab.config import load_config
from euroflex_bess_lab.exports import export_bids, export_schedule
from euroflex_bess_lab.reconciliation import reconcile_run

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "render_demo_gif.py"
FIXTURE_CONFIG = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "example_configs"
    / "reserve"
    / "belgium_portfolio_schedule_revision_da_plus_afrr_base.yaml"
)


def _load_render_demo_module():
    spec = importlib.util.spec_from_file_location("render_demo_gif_test_module", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def render_demo_module():
    return _load_render_demo_module()


@pytest.fixture()
def revision_run_dir(tmp_path: Path) -> Path:
    config = load_config(FIXTURE_CONFIG)
    config.artifacts.root_dir = tmp_path / "artifacts"
    result = run_walk_forward(config)
    assert result.output_dir is not None
    run_dir = result.output_dir.resolve()
    reconcile_run(run_dir, FIXTURE_CONFIG)
    export_schedule(run_dir, profile="operator")
    export_bids(run_dir, profile="bid_planning")
    return run_dir


def test_load_demo_story_extracts_checkpoints_changed_intervals_and_filtered_waterfall(
    render_demo_module, revision_run_dir: Path
) -> None:
    story = render_demo_module.load_demo_story(revision_run_dir)

    assert story.checkpoint_labels == ["06:00", "12:00"]
    assert [checkpoint.strftime("%H:%M") for checkpoint in story.checkpoints] == ["06:00", "12:00"]
    assert not story.changed_intervals.empty
    assert (
        story.changed_intervals["baseline_net_export_mw"] - story.changed_intervals["revised_net_export_mw"]
    ).abs().max() > 0.0

    labels = [step.label for step in story.waterfall_steps]
    assert labels[0] == "Baseline"
    assert "Revised" in labels
    assert labels[-1] == "Realized"
    assert "Reserve headroom" not in labels
    assert "Availability" not in labels


def test_build_demo_gif_renders_from_existing_run(render_demo_module, revision_run_dir: Path, tmp_path: Path) -> None:
    output_path = tmp_path / "canonical-belgium-demo.gif"

    resolved_output = render_demo_module.build_demo_gif(
        config_path=FIXTURE_CONFIG,
        output_path=output_path,
        run_dir=revision_run_dir,
        frame_scale=0.25,
    )

    assert resolved_output == output_path.resolve()
    assert output_path.exists()
    assert output_path.stat().st_size > 1_000


def test_build_demo_gif_falls_back_to_pillow_without_ffmpeg(
    render_demo_module, revision_run_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_path = tmp_path / "canonical-belgium-demo.gif"
    original_which = shutil.which
    monkeypatch.setattr(
        render_demo_module.shutil,
        "which",
        lambda binary: None if binary == "ffmpeg" else original_which(binary),
    )

    resolved_output = render_demo_module.build_demo_gif(
        config_path=FIXTURE_CONFIG,
        output_path=output_path,
        run_dir=revision_run_dir,
        frame_scale=0.2,
    )

    assert resolved_output == output_path.resolve()
    assert output_path.exists()
    assert output_path.stat().st_size > 1_000


def test_build_demo_gif_requires_ffmpeg_for_mp4_output(
    render_demo_module, revision_run_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_path = tmp_path / "canonical-belgium-demo.gif"
    mp4_path = tmp_path / "canonical-belgium-demo.mp4"
    original_which = shutil.which
    monkeypatch.setattr(
        render_demo_module.shutil,
        "which",
        lambda binary: None if binary == "ffmpeg" else original_which(binary),
    )

    with pytest.raises(RuntimeError, match="write-mp4.*ffmpeg|ffmpeg.*PATH"):
        render_demo_module.build_demo_gif(
            config_path=FIXTURE_CONFIG,
            output_path=output_path,
            run_dir=revision_run_dir,
            mp4_path=mp4_path,
            frame_scale=0.2,
        )

    assert not mp4_path.exists()


@pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="ffmpeg is required for MP4 rendering")
def test_build_demo_gif_renders_mp4_when_ffmpeg_is_available(
    render_demo_module, revision_run_dir: Path, tmp_path: Path
) -> None:
    output_path = tmp_path / "canonical-belgium-demo.gif"
    mp4_path = tmp_path / "canonical-belgium-demo.mp4"

    resolved_output = render_demo_module.build_demo_gif(
        config_path=FIXTURE_CONFIG,
        output_path=output_path,
        run_dir=revision_run_dir,
        mp4_path=mp4_path,
        frame_scale=0.25,
    )

    assert resolved_output == output_path.resolve()
    assert output_path.exists()
    assert output_path.stat().st_size > 1_000
    assert mp4_path.exists()
    assert mp4_path.stat().st_size > 1_000
