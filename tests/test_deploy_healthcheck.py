"""scripts/ops/healthcheck.py: exit codes from the live engine heartbeat (``status.json``); no network."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams, ThresholdStrategy
from euroflex_bess_lab.realtime.live import LiveConfig, LiveEngine, ManualClock, ReplayFeeds
from euroflex_bess_lab.realtime.live.testing import synthetic_market_frames

REPO_ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 26, 12, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def hc() -> ModuleType:
    path = REPO_ROOT / "scripts" / "ops" / "healthcheck.py"
    spec = importlib.util.spec_from_file_location("ops_healthcheck_under_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _status(**overrides: Any) -> dict[str, Any]:
    status: dict[str, Any] = {
        "schema": "euroflex-live-status/1",
        "run_id": "live-test",
        "state": "running",
        "mode": "paper",
        "market_submission": False,
        "strategy": "threshold",
        "heartbeat_utc": (NOW - timedelta(seconds=30)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "last_minute_utc": "2026-10-26T11:59:00Z",
        "mechanism_now": "current",
        "counters": {"decisions": 12, "missed": 0, "strategy_errors": 0},
        "ledger": {"settled_qh": 3, "pnl_eur": 1.5},
    }
    status.update(overrides)
    return status


def test_fresh_running_heartbeat_is_healthy(hc: ModuleType) -> None:
    code, report = hc.evaluate(_status(), now=NOW)
    assert code == hc.HEALTHY == 0
    assert report["healthy"] and report["reason"] == "ok"
    assert report["heartbeat_age_s"] == pytest.approx(30.0)
    assert report["decisions"] == 12 and report["mode"] == "paper"


@pytest.mark.parametrize(
    ("overrides", "fragment"),
    [
        ({"heartbeat_utc": "2026-10-26T11:50:00Z"}, "heartbeat is 600 s old"),
        ({"state": "completed"}, "engine state is 'completed'"),
        ({"state": "interrupted"}, "engine state is 'interrupted'"),
        ({"heartbeat_utc": "2026-10-26T12:30:00Z"}, "in the future"),
    ],
)
def test_stale_or_stopped_engine_is_unhealthy(hc: ModuleType, overrides: dict[str, Any], fragment: str) -> None:
    code, report = hc.evaluate(_status(**overrides), now=NOW)
    assert code == hc.UNHEALTHY == 1
    assert fragment in report["reason"] and not report["healthy"]


def test_draining_counts_as_healthy_and_limits_apply(hc: ModuleType) -> None:
    assert hc.evaluate(_status(state="draining"), now=NOW)[0] == hc.HEALTHY
    assert hc.evaluate(_status(), now=NOW, min_decisions=13)[0] == hc.UNHEALTHY
    errors = _status(counters={"decisions": 5, "strategy_errors": 4})
    assert hc.evaluate(errors, now=NOW, max_strategy_errors=3)[0] == hc.UNHEALTHY
    assert hc.evaluate(errors, now=NOW, max_strategy_errors=4)[0] == hc.HEALTHY
    assert hc.evaluate(_status(), now=NOW, max_age_s=10)[0] == hc.UNHEALTHY


def test_missing_foreign_or_non_paper_status(hc: ModuleType) -> None:
    assert hc.evaluate(None, now=NOW)[0] == hc.NOT_STARTED == 3
    assert hc.evaluate(_status(schema="something-else/1"), now=NOW)[0] == hc.INVALID == 4
    assert hc.evaluate(_status(mode="live"), now=NOW)[0] == hc.INVALID
    assert hc.evaluate(_status(market_submission=True), now=NOW)[0] == hc.INVALID
    assert hc.evaluate(_status(heartbeat_utc="yesterday"), now=NOW)[0] == hc.INVALID


def test_cli_exit_codes_and_json(
    hc: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("EUROFLEX_RUN_DIR", raising=False)
    run = tmp_path / "run"
    run.mkdir()
    now = "--now=2026-10-26T12:00:00Z"
    assert hc.main(["--run-dir", str(run), now]) == hc.NOT_STARTED
    (run / "status.json").write_text("{not json", encoding="utf-8")
    assert hc.main(["--run-dir", str(run), now]) == hc.INVALID
    (run / "status.json").write_text(json.dumps(_status()), encoding="utf-8")
    capsys.readouterr()
    assert hc.main(["--run-dir", str(run), now, "--json"]) == hc.HEALTHY
    report = json.loads(capsys.readouterr().out)
    assert report["exit_code"] == 0 and report["strategy"] == "threshold"
    assert hc.main(["--run-dir", str(run), "--now=2026-10-26T13:00:00Z", "--quiet"]) == hc.UNHEALTHY
    assert capsys.readouterr().out == ""
    assert hc.main([now]) == hc.INVALID  # no run directory at all


def test_run_dir_from_environment(hc: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "status.json").write_text(json.dumps(_status()), encoding="utf-8")
    monkeypatch.setenv("EUROFLEX_RUN_DIR", str(tmp_path))
    assert hc.main(["--now=2026-10-26T12:00:00Z", "--quiet"]) == hc.HEALTHY
    monkeypatch.delenv("EUROFLEX_RUN_DIR")
    assert hc.main(["--now=2026-10-26T12:00:00Z", "--quiet"]) == hc.INVALID


def test_heartbeat_written_by_the_live_engine(hc: ModuleType, tmp_path: Path) -> None:
    """A real status.json from the paper engine (synthetic replay, manual clock): healthy while running."""
    start = pd.Timestamp("2026-03-02T00:00:00Z")
    frames = synthetic_market_frames(start, hours=2)
    source = ReplayFeeds({"minute": frames["minute"], "qh": frames["qh"], "day_ahead": frames["day_ahead"]})
    clock = ManualClock(start - pd.Timedelta(seconds=30))
    config = LiveConfig(
        window_start_utc=start, window_end_utc=start + pd.Timedelta(hours=2), fsync=False, history_days=0.0
    )
    engine = LiveEngine(
        ThresholdStrategy(min_history_qh=4, lookback_qh=48),
        BatteryParams(power_mw=1.0, energy_mwh=2.0),
        source=source,
        run_dir=tmp_path / "run",
        clock=clock,
        config=config,
    )
    engine.start()
    for _ in range(20):
        engine.step()
    status = json.loads((tmp_path / "run" / "status.json").read_text(encoding="utf-8"))
    assert status["state"] == "running" and status["mode"] == "paper" and status["market_submission"] is False
    heartbeat = clock.now()
    now = f"--now={heartbeat.isoformat()}"
    assert hc.main(["--run-dir", str(tmp_path / "run"), now, "--min-decisions", "1", "--quiet"]) == hc.HEALTHY
    later = f"--now={(heartbeat + pd.Timedelta(minutes=10)).isoformat()}"
    assert hc.main(["--run-dir", str(tmp_path / "run"), later, "--quiet"]) == hc.UNHEALTHY
    engine.stop("stopped")
    assert hc.main(["--run-dir", str(tmp_path / "run"), now, "--quiet"]) == hc.UNHEALTHY
