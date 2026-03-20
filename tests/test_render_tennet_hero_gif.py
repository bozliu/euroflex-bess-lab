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
SCRIPT_PATH = REPO_ROOT / "scripts" / "render_tennet_hero_gif.py"
FIXTURE_CONFIG = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "example_configs"
    / "reserve"
    / "netherlands_portfolio_schedule_revision_da_plus_afrr_base.yaml"
)
SIGNAL_FIXTURE = REPO_ROOT / "examples" / "data" / "netherlands_imbalance_prices.csv"


def _load_render_tennet_module():
    spec = importlib.util.spec_from_file_location("render_tennet_hero_gif_test_module", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def render_tennet_module():
    return _load_render_tennet_module()


@pytest.fixture()
def dutch_revision_run_dir(tmp_path: Path) -> Path:
    config = load_config(FIXTURE_CONFIG)
    config.artifacts.root_dir = tmp_path / "artifacts"
    result = run_walk_forward(config)
    assert result.output_dir is not None
    run_dir = result.output_dir.resolve()
    reconcile_run(run_dir, FIXTURE_CONFIG)
    export_schedule(run_dir, profile="operator")
    export_bids(run_dir, profile="bid_planning")
    return run_dir


def test_load_tennet_hero_story_extracts_revision_shape_and_export_cards(
    render_tennet_module, dutch_revision_run_dir: Path
) -> None:
    story = render_tennet_module.load_tennet_hero_story(dutch_revision_run_dir, signal_path=SIGNAL_FIXTURE)

    assert story.checkpoint_labels == ["06:00", "12:00"]
    assert [checkpoint.strftime("%H:%M") for checkpoint in story.checkpoints] == ["06:00", "12:00"]
    assert not story.changed_intervals.empty
    assert len(story.export_cards) == 2
    assert story.export_cards[0].title == "operator export"
    assert story.export_cards[1].title == "bid_planning export"
    assert story.live_signal["signal_value"].abs().max() > 0.0


def test_build_tennet_hero_gif_renders_from_existing_run(
    render_tennet_module, dutch_revision_run_dir: Path, tmp_path: Path
) -> None:
    output_path = tmp_path / "tennet-live-workflow.gif"

    resolved_output = render_tennet_module.build_tennet_hero_gif(
        config_path=FIXTURE_CONFIG,
        output_path=output_path,
        run_dir=dutch_revision_run_dir,
        signal_path=SIGNAL_FIXTURE,
        frame_scale=0.25,
    )

    assert resolved_output == output_path.resolve()
    assert output_path.exists()
    assert output_path.stat().st_size > 1_000


def test_build_tennet_hero_gif_falls_back_to_pillow_without_ffmpeg(
    render_tennet_module, dutch_revision_run_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_path = tmp_path / "tennet-live-workflow.gif"
    original_which = shutil.which
    monkeypatch.setattr(
        render_tennet_module.shutil,
        "which",
        lambda binary: None if binary == "ffmpeg" else original_which(binary),
    )

    resolved_output = render_tennet_module.build_tennet_hero_gif(
        config_path=FIXTURE_CONFIG,
        output_path=output_path,
        run_dir=dutch_revision_run_dir,
        signal_path=SIGNAL_FIXTURE,
        frame_scale=0.2,
    )

    assert resolved_output == output_path.resolve()
    assert output_path.exists()
    assert output_path.stat().st_size > 1_000


def test_build_tennet_hero_gif_requires_ffmpeg_for_mp4_output(
    render_tennet_module, dutch_revision_run_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_path = tmp_path / "tennet-live-workflow.gif"
    mp4_path = tmp_path / "tennet-live-workflow.mp4"
    original_which = shutil.which
    monkeypatch.setattr(
        render_tennet_module.shutil,
        "which",
        lambda binary: None if binary == "ffmpeg" else original_which(binary),
    )

    with pytest.raises(RuntimeError, match="write-mp4.*ffmpeg|ffmpeg.*PATH"):
        render_tennet_module.build_tennet_hero_gif(
            config_path=FIXTURE_CONFIG,
            output_path=output_path,
            run_dir=dutch_revision_run_dir,
            mp4_path=mp4_path,
            signal_path=SIGNAL_FIXTURE,
            frame_scale=0.2,
        )

    assert not mp4_path.exists()


@pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="ffmpeg is required for MP4 rendering")
def test_build_tennet_hero_gif_renders_mp4_when_ffmpeg_is_available(
    render_tennet_module, dutch_revision_run_dir: Path, tmp_path: Path
) -> None:
    output_path = tmp_path / "tennet-live-workflow.gif"
    mp4_path = tmp_path / "tennet-live-workflow.mp4"

    resolved_output = render_tennet_module.build_tennet_hero_gif(
        config_path=FIXTURE_CONFIG,
        output_path=output_path,
        run_dir=dutch_revision_run_dir,
        mp4_path=mp4_path,
        signal_path=SIGNAL_FIXTURE,
        frame_scale=0.25,
    )

    assert resolved_output == output_path.resolve()
    assert output_path.exists()
    assert output_path.stat().st_size > 1_000
    assert mp4_path.exists()
    assert mp4_path.stat().st_size > 1_000
