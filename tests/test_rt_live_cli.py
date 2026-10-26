from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from euroflex_bess_lab.realtime.live import ManualClock, MechanismSwitchingStrategy, ReplayFeeds, make_strategy
from euroflex_bess_lab.realtime.live import cli as live_cli
from euroflex_bess_lab.realtime.live.strategies import available_strategies, parse_params
from euroflex_bess_lab.realtime.live.testing import synthetic_market_frames
from euroflex_bess_lab.realtime.strategies import IdleStrategy, ThresholdStrategy

START = pd.Timestamp("2026-03-02T00:00:00Z")
runner = CliRunner()


def _plain(text: str) -> str:
    """CLI output without ANSI colour codes, box drawing and line wrapping (CI terminals colour typer errors)."""
    text = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text)
    text = re.sub(r"[\u2500-\u257f]", " ", text)
    return " ".join(text.split())


@pytest.fixture
def offline_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the real Elia feeds and wall clock with a replay at simulated time."""
    frames = synthetic_market_frames(START, hours=2)
    monkeypatch.setattr(
        live_cli,
        "_build_source",
        lambda poll_seconds: ReplayFeeds(
            {"minute": frames["minute"], "qh": frames["qh"], "day_ahead": frames["day_ahead"]}
        ),
    )
    monkeypatch.setattr(live_cli, "_build_clock", lambda: ManualClock(START - pd.Timedelta(seconds=30)))


def test_run_status_and_reconcile_commands(tmp_path: Path, offline_cli: None, monkeypatch: pytest.MonkeyPatch) -> None:
    run_dir = tmp_path / "run"
    result = runner.invoke(
        live_cli.live_app,
        [
            "run",
            "--strategy",
            "threshold",
            "--param",
            "min_history_qh=2",
            "--max-minutes",
            "20",
            "--run-dir",
            str(run_dir),
            "--history-days",
            "0",
            "--no-public-history",
            "--execution",
            "csv",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Paper trading (shadow mode) only" in result.output
    summary = json.loads(result.output[result.output.index("{") :])
    # 20 minutes requested, finished at the quarter-hour boundary, then drained until the prices were final
    assert summary["counters"]["decisions"] == 30 and summary["ledger"]["settled_qh"] == 2
    assert summary["market_submission"] is False and summary["run_dir"] == "run"
    assert (run_dir / "setpoints.csv").read_text(encoding="utf-8").count("\n") == 31
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    assert config["strategy"] == "threshold" and config["metadata"]["params"] == {"min_history_qh": 2}

    status = runner.invoke(live_cli.live_app, ["status", str(run_dir)])
    assert status.exit_code == 0, status.output
    assert "paper P&L" in status.output and "replay_minute" in status.output
    raw = runner.invoke(live_cli.live_app, ["status", str(run_dir), "--json"])
    assert json.loads(raw.output)["state"] == "completed"

    monkeypatch.setenv("EUROFLEX_DATA_DIR", str(tmp_path / "no_public_data"))
    reconciled = runner.invoke(live_cli.live_app, ["reconcile", str(run_dir), "--source", "public"])
    assert reconciled.exit_code == 0, reconciled.output
    report = json.loads(reconciled.output)
    assert report["quarter_hours"] == 2 and report["validated_available"] == 0
    assert report["status_counts"] == {"pending": 2}
    assert all(str(Path.home()) not in note for note in report["notes"])


def test_run_refuses_untradable_and_unconfigured_options(tmp_path: Path, offline_cli: None) -> None:
    oracle = runner.invoke(live_cli.live_app, ["run", "--strategy", "oracle", "--run-dir", str(tmp_path / "a")])
    assert oracle.exit_code != 0 and "perfect-foresight" in _plain(oracle.output)
    webhook = runner.invoke(
        live_cli.live_app,
        [
            "run",
            "--execution",
            "webhook",
            "--webhook-url",
            "https://ems.example.test",
            "--run-dir",
            str(tmp_path / "b"),
        ],
    )
    assert webhook.exit_code != 0 and "enable-webhook" in _plain(webhook.output)
    unknown = runner.invoke(
        live_cli.live_app, ["run", "--strategy", "does-not-exist", "--run-dir", str(tmp_path / "c")]
    )
    assert unknown.exit_code != 0 and "available:" in _plain(unknown.output)
    assert not (tmp_path / "a").exists() and not (tmp_path / "c").exists()


def test_strategies_command_lists_builtins_and_skipped_modules() -> None:
    result = runner.invoke(live_cli.live_app, ["strategies"])
    assert result.exit_code == 0
    for name in ("threshold", "threshold-si", "idle"):
        assert name in result.output
    registry, unavailable = available_strategies()
    assert {"threshold", "threshold-si", "idle"} <= set(registry)
    for module, reason in unavailable.items():
        assert module in result.output and reason


def test_strategy_registry_builds_the_backtest_objects() -> None:
    plain, label = make_strategy("threshold", mechanism="current", params={"lookback_qh": 96})
    assert isinstance(plain, ThresholdStrategy) and plain.signal_field == "price_cum_eur_mwh" and label == "threshold"
    assert plain.lookback_qh == 96
    nf, _ = make_strategy("threshold", mechanism="new2026")
    assert isinstance(nf, ThresholdStrategy) and nf.signal_field == "nf_imbalance_price_eur_mwh"
    auto, _ = make_strategy("threshold", mechanism="auto")
    assert isinstance(auto, MechanismSwitchingStrategy) and set(auto.strategies) == {"current", "new2026"}
    si, label = make_strategy("threshold_si")
    assert isinstance(si, ThresholdStrategy) and si.mode == "si" and label == "threshold-si"
    explicit, label = make_strategy("euroflex_bess_lab.realtime.strategies:IdleStrategy")
    assert isinstance(explicit, IdleStrategy) and label == "idle"
    assert parse_params(["a=1", "b=x", "c=[1, 2]", "d=true"]) == {"a": 1, "b": "x", "c": [1, 2], "d": True}
    with pytest.raises(ValueError):
        parse_params(["novalue"])


def test_replay_command_proves_live_equals_backtest(monkeypatch: pytest.MonkeyPatch) -> None:
    frames = synthetic_market_frames(START, hours=3)
    tables = {"minute": frames["minute"], "qh": frames["qh"], "day_ahead": frames["day_ahead"]}
    monkeypatch.setattr(
        "euroflex_bess_lab.realtime.live.dryrun.public_tables", lambda start, end, new_formula=False: tables
    )
    result = runner.invoke(
        live_cli.live_app,
        [
            "replay",
            "--start",
            "2026-03-02T00:00:00Z",
            "--end",
            "2026-03-02T03:00:00Z",
            "--param",
            "min_history_qh=2",
        ],
    )
    assert result.exit_code == 0, result.output
    report = json.loads(result.output)
    assert report["identical_decisions"] is True and report["minutes"] == report["live_minutes"] == 180
    assert report["ledger_rows_compared"] == 12 and report["max_abs_qh_pnl_difference_eur"] == 0.0
    bad = runner.invoke(
        live_cli.live_app,
        ["replay", "--start", "2026-03-02T00:00:00Z", "--end", "2026-03-02T01:00:00Z", "--mechanism", "auto"],
    )
    assert bad.exit_code != 0
