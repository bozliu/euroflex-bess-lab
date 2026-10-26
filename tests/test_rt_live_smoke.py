"""Real-API smoke test for live paper trading. Polls Elia Open Data for about three minutes.

Run with ``EUROFLEX_LIVE_SMOKE=1 .venv/bin/python -m pytest -q tests/test_rt_live_smoke.py``.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams, ThresholdStrategy
from euroflex_bess_lab.realtime.live import EliaLiveFeeds, LiveConfig, LiveEngine

pytestmark = pytest.mark.skipif(
    os.environ.get("EUROFLEX_LIVE_SMOKE") != "1", reason="set EUROFLEX_LIVE_SMOKE=1 to poll the real Elia API"
)


def test_paper_trading_on_the_real_elia_feeds(tmp_path: Path) -> None:
    engine = LiveEngine(
        ThresholdStrategy(),
        BatteryParams(power_mw=1.0, energy_mwh=2.0),
        source=EliaLiveFeeds(),
        run_dir=tmp_path / "run",
        config=LiveConfig(history_days=2.0, fsync=False),
    )
    summary = engine.run(max_minutes=3)
    assert summary["counters"]["decisions"] + summary["counters"]["missed"] >= 3
    status = engine.status(pd.Timestamp.now(tz="UTC"))
    for name in ("imbalance_price_minute", "system_imbalance_minute", "imbalance_price_qh"):
        feed = status["feeds"][name]
        assert feed["rows_new"] > 0, name
        assert feed["staleness_s"] is not None and feed["staleness_s"] < 30 * 60, name
    decisions = engine.decisions()
    ok = decisions[decisions["status"] == "ok"]
    assert len(ok) >= 1 and ok["signals.price_cum_eur_mwh"].notna().all()
    # every row seen is archived exactly as Elia published it (raw JSON) and in contract form
    archived = set(engine.store.archived_datasets())
    assert {"ods161", "ods169", "ods162"} <= archived, archived
    raw = engine.store.read_archive_raw("ods161")
    assert raw and {"datetime", "imbalanceprice", "systemimbalance"} <= set(raw[-1]["record"])
