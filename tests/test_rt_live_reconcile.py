from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams, DecisionContext
from euroflex_bess_lab.realtime.live import (
    EliaLiveFeeds,
    JsonHttpClient,
    LiveConfig,
    LiveEngine,
    ManualClock,
    ReplayFeeds,
)
from euroflex_bess_lab.realtime.live.feeds import IMBALANCE_PRICE_MINUTE, IMBALANCE_PRICE_QH
from euroflex_bess_lab.realtime.live.reconcile import reconcile_run
from euroflex_bess_lab.realtime.live.testing import FakeEliaSession, synthetic_market_frames
from euroflex_bess_lab.realtime.strategies import BaseStrategy

START = pd.Timestamp("2026-03-02T00:00:00Z")
BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)


class _Schedule(BaseStrategy):
    name = "schedule"

    def decide(self, ctx: DecisionContext) -> float:
        return {0: 1.0, 1: -1.0, 2: 0.5}.get(ctx.qh_index, 0.0)


def _run(tmp_path: Path, frames: dict[str, pd.DataFrame], *, mechanism: str = "current", minutes: int = 60) -> Path:
    end = START + pd.Timedelta(minutes=minutes)
    source = ReplayFeeds({"minute": frames["minute"], "qh": frames["qh"], "day_ahead": frames["day_ahead"]})
    engine = LiveEngine(
        _Schedule(),
        BATTERY,
        source=source,
        run_dir=tmp_path / "run",
        clock=ManualClock(START - pd.Timedelta(seconds=30)),
        config=LiveConfig(
            mechanism=mechanism,  # type: ignore[arg-type]
            window_start_utc=START,
            window_end_utc=end,
            fsync=False,
            history_days=0.0,
            settle_timeout_minutes=10,
        ),
    )
    engine.run(drain_minutes=15)
    return tmp_path / "run"


def _validated(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    qh = frames["qh"].set_index("timestamp_utc")["imbalance_price_eur_mwh"].astype(float)
    t = list(qh.index)
    return pd.DataFrame(
        [
            {
                "timestamp_utc": t[0],
                "mechanism": "current",
                "price_eur_mwh": qh.iloc[0],
                "quality_status": "Validated",
                "source": "api:ods134",
            },
            {
                "timestamp_utc": t[1],
                "mechanism": "current",
                "price_eur_mwh": qh.iloc[1] + 10.0,
                "quality_status": "Validated",
                "source": "api:ods134",
            },
            # QH 2: a non-validated candidate first, then a validated one -> the validated one wins
            {
                "timestamp_utc": t[2],
                "mechanism": "current",
                "price_eur_mwh": 999.0,
                "quality_status": "NotValidated",
                "source": "nrt:ods162",
            },
            {
                "timestamp_utc": t[2],
                "mechanism": "current",
                "price_eur_mwh": qh.iloc[2] - 4.0,
                "quality_status": "Validated",
                "source": "data/public:ods134",
            },
            # QH 3: nothing validated yet
        ]
    )


def test_reconcile_reports_confirmed_revised_and_pending_prices(tmp_path: Path) -> None:
    frames = synthetic_market_frames(START, hours=1)
    run_dir = _run(tmp_path, frames)
    result = reconcile_run(run_dir, prices=_validated(frames), include_minutes=False)
    qh = result.quarter_hours.set_index("timestamp_utc")
    statuses = list(qh["status"])
    assert statuses == ["confirmed", "revised", "revised", "pending"]
    assert qh["energy_matches_ledger"].iloc[:3].all()
    assert qh.iloc[1]["price_revision_eur_mwh"] == pytest.approx(10.0, abs=1e-3)
    assert qh.iloc[1]["pnl_difference_eur"] == pytest.approx(-0.25 * 10.0, abs=1e-3)  # short 0.25 MWh
    assert qh.iloc[2]["validated_source"] == "data/public:ods134"
    assert qh.iloc[2]["pnl_difference_eur"] == pytest.approx(0.125 * -4.0, abs=1e-3)
    summary = result.summary
    assert summary["price_revisions"]["count"] == 2 and summary["validated_available"] == 3
    assert summary["pnl"]["difference_eur"] == pytest.approx(-2.5 - 0.5, abs=1e-3)
    assert summary["minutes"]["missed"] == 0 and summary["minutes"]["decisions"] == 60
    out = run_dir / "reconcile"
    assert (out / "qh_reconciliation.csv").exists() and (out / "qh_reconciliation.parquet").exists()
    assert json.loads((out / "summary.json").read_text())["quarter_hours"] == 4


def test_reconcile_fills_prices_that_were_missing_live(tmp_path: Path) -> None:
    frames = synthetic_market_frames(START, hours=1)
    run_dir = _run(tmp_path, frames, mechanism="new2026", minutes=30)  # no live new-formula feed
    validated = pd.DataFrame(
        {
            "timestamp_utc": [START, START + pd.Timedelta(minutes=15)],
            "mechanism": "new2026",
            "price_eur_mwh": [100.0, 50.0],
            "quality_status": "Validated",
            "source": "api:ods212",
        }
    )
    result = reconcile_run(run_dir, prices=validated, include_minutes=False)
    qh = result.quarter_hours
    assert list(qh["status"]) == ["filled", "filled"]
    assert qh["validated_pnl_eur"].iloc[0] == pytest.approx(0.25 * 100.0 - 0.25 * BATTERY.degradation_eur_per_mwh)
    assert result.summary["pnl"]["difference_eur"] == pytest.approx(0.25 * 100.0 - 0.25 * 50.0)


def test_reconcile_against_the_api_compares_minute_signals(tmp_path: Path) -> None:
    frames = synthetic_market_frames(START, hours=1)
    run_dir = _run(tmp_path, frames)
    validated_minute = frames["minute"].copy()
    validated_minute.loc[10, "price_cum_eur_mwh"] = validated_minute.loc[10, "price_cum_eur_mwh"] + 7.0
    clock = ManualClock("2026-03-05T00:00:00Z")
    historical = [IMBALANCE_PRICE_MINUTE.historical(), IMBALANCE_PRICE_QH.historical()]
    session = FakeEliaSession(
        {"ods133": validated_minute, "ods134": frames["qh"].assign(quality_status="Validated")},
        clock=clock.now,
        specs=[s for s in historical if s is not None],
    )
    feeds = EliaLiveFeeds(client=JsonHttpClient(session=session, now=clock.now, sleep=lambda _: None))
    result = reconcile_run(run_dir, sources=("api",), feeds=feeds)
    assert set(result.quarter_hours["status"]) == {"confirmed"}
    assert set(result.quarter_hours["validated_source"]) == {"api:ods134"}
    revisions = result.summary["minute_signal_revisions"]["price_cum"]
    # decisions read the row two minutes back; row 10 was used by the decision at minute 12
    assert revisions["compared"] == 58 and revisions["revised"] == 1 and revisions["source"] == "api:ods133"
    assert revisions["max_abs_eur_mwh"] == pytest.approx(7.0, abs=1e-3)
    minutes = result.minutes.set_index("minute_utc")
    assert minutes.loc[START + pd.Timedelta(minutes=12), "price_cum_revision_eur_mwh"] == pytest.approx(7.0, abs=1e-3)
    assert (run_dir / "reconcile" / "minute_reconciliation.csv").exists()
    assert any("ods212" in r["url"] for r in session.requests)


def test_reconcile_requires_a_run_directory(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        reconcile_run(tmp_path, prices=pd.DataFrame())
