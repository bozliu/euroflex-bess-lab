"""Go-live readiness: settlement rule by date *and* by which feed is live, a lossless row archive, ods212 reconcile."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import numpy as np
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
from euroflex_bess_lab.realtime.live.feeds import (
    IMBALANCE_PRICE_MINUTE,
    IMBALANCE_PRICE_QH,
    NEW_FORMULA_MINUTE,
    NEW_FORMULA_QH,
)
from euroflex_bess_lab.realtime.live.reconcile import reconcile_run
from euroflex_bess_lab.realtime.live.testing import FakeEliaSession, split_minute_frame, synthetic_market_frames
from euroflex_bess_lab.realtime.strategies import BaseStrategy

START = pd.Timestamp("2026-03-02T00:00:00Z")
BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)


class _Pulse(BaseStrategy):
    name = "pulse"

    def __init__(self, hook=None) -> None:  # type: ignore[no-untyped-def]
        self.hook = hook

    def decide(self, ctx: DecisionContext) -> float:
        if self.hook is not None:
            self.hook(ctx)
        return 1.0 if ctx.minute_in_qh < 4 else (-1.0 if ctx.minute_in_qh > 10 else 0.0)


def _config(end: pd.Timestamp, **overrides) -> LiveConfig:  # type: ignore[no-untyped-def]
    settings = {
        "window_start_utc": START,
        "window_end_utc": end,
        "history_days": 0.0,
        "fsync": False,
        "status_every_minutes": 30,
    }
    settings.update(overrides)
    return LiveConfig(**settings)


def test_archive_keeps_every_row_version_exactly_as_published(tmp_path: Path) -> None:
    frames = synthetic_market_frames(START, hours=1)
    end = START + pd.Timedelta(minutes=40)
    clock = ManualClock(START - pd.Timedelta(seconds=30))
    tables = {
        "imbalance_price_minute": split_minute_frame(frames["minute"])["imbalance_price_minute"],
        "imbalance_price_qh": frames["qh"],
    }
    session = FakeEliaSession(tables, clock=clock.now)
    revised = frames["qh"].copy()
    revised.loc[0, "imbalance_price_eur_mwh"] = np.float32(revised.loc[0, "imbalance_price_eur_mwh"] + 100.0)

    def revise(ctx: DecisionContext) -> None:
        if ctx.t == 25:  # Elia revises the first quarter-hour price after it was published and settled
            session.update_table("imbalance_price_qh", revised)

    specs = [replace(IMBALANCE_PRICE_MINUTE, poll_every_s=60.0), replace(IMBALANCE_PRICE_QH, poll_every_s=60.0)]
    feeds = EliaLiveFeeds(specs, client=JsonHttpClient(session=session, now=clock.now, sleep=lambda _: None))
    engine = LiveEngine(
        _Pulse(revise), BATTERY, source=feeds, run_dir=tmp_path / "run", clock=clock, config=_config(end)
    )
    engine.run(drain_minutes=10)

    raw_qh = engine.store.read_archive_raw("ods162")
    first = [line for line in raw_qh if line["record"]["datetime"] == "2026-03-02T00:00:00+00:00"]
    assert [line["kind"] for line in first] == ["new", "revised"]
    original = float(frames["qh"].loc[0, "imbalance_price_eur_mwh"])
    assert first[0]["record"]["imbalanceprice"] == pytest.approx(original)
    assert first[1]["record"]["imbalanceprice"] == pytest.approx(original + 100.0)
    # the archived record is the upstream object itself, field for field, as the API serves it now
    served = session.get(feeds.records_url("ods162"), {"order_by": "datetime desc", "limit": "50"}).json()["results"]
    served_first = next(r for r in served if r["datetime"] == "2026-03-02T00:00:00+00:00")
    assert first[1]["record"] == served_first and list(first[1]["record"]) == list(served_first)

    minute_raw = engine.store.read_archive_raw("ods161")
    archived = [line["record"]["datetime"] for line in minute_raw]
    last_poll = max(r["at"] for r in session.requests if "/ods161/" in r["url"])
    minutes = pd.date_range(START, end, freq="1min", inclusive="left")
    published = minutes[minutes + pd.Timedelta(seconds=80) <= last_poll]  # rows appear 20 s after the minute
    # every row the API had published by the last poll, exactly once: nothing lost, nothing duplicated
    assert archived == [ts.strftime("%Y-%m-%dT%H:%M:%S+00:00") for ts in published]
    normalized = engine.store.read_archive("ods162")
    assert list(normalized.loc[normalized["timestamp_utc"] == START, "kind"]) == ["new", "revised"]
    latest = engine.store.read_archive("ods162", latest_only=True).set_index("timestamp_utc")
    assert latest.loc[START, "imbalance_price_eur_mwh"] == pytest.approx(original + 100.0, abs=1e-3)
    assert set(engine.store.archived_datasets()) == {"ods161", "ods162"}
    # the ledger followed the revision with a corrected row
    ledger = engine.store.read_ledger(all_revisions=True)
    first_qh = ledger[ledger["timestamp_utc"] == START]
    assert list(first_qh["revision"]) == [0, 1]
    assert first_qh["price_eur_mwh"].iloc[-1] == pytest.approx(original + 100.0, abs=1e-3)


def _switch_tables(frames: dict[str, pd.DataFrame], switch: pd.Timestamp) -> dict[str, pd.DataFrame]:
    """Current-rule datasets stop at ``switch``; the new-formula datasets publish from ``switch`` on."""
    minute = frames["minute"]
    nf_minute = minute[["timestamp_utc"]].assign(imbalance_price_eur_mwh=minute["price_cum_eur_mwh"] + 1.0)
    return {
        "minute:cur": minute[minute["timestamp_utc"] < switch],
        "qh": frames["qh"][frames["qh"]["timestamp_utc"] < switch],
        "nf_minute": nf_minute[nf_minute["timestamp_utc"] >= switch],
        "nf_qh": frames["nf_qh"][frames["nf_qh"]["timestamp_utc"] >= switch],
    }


def test_switch_is_detected_from_the_feeds_even_before_the_configured_go_live(tmp_path: Path) -> None:
    frames = synthetic_market_frames(START, hours=3)
    switch = START + pd.Timedelta(minutes=45)  # Elia switched earlier than configured
    end = START + pd.Timedelta(hours=2)
    engine = LiveEngine(
        _Pulse(),
        BATTERY,
        source=ReplayFeeds(_switch_tables(frames, switch)),
        run_dir=tmp_path / "run",
        clock=ManualClock(START - pd.Timedelta(seconds=30)),
        config=_config(end, go_live_utc=START + pd.Timedelta(hours=3), stale_after_minutes=20.0),
    )
    engine.run(drain_minutes=30)
    ledger = engine.ledger.set_index("timestamp_utc")
    before, after = ledger[ledger.index < switch], ledger[ledger.index >= switch]
    assert (before["mechanism"] == "current").all() and (before["mechanism_basis"] == "date").all()
    assert len(after) == 5 and (after["mechanism"] == "new2026").all()
    assert (after["mechanism_basis"] == "switch_observed").all() and not after["price_missing"].any()
    nf_price = frames["nf_qh"].set_index("timestamp_utc")["imbalance_price_eur_mwh"].astype(float)
    assert np.allclose(after["price_eur_mwh"], nf_price.loc[after.index])
    decisions = engine.decisions().set_index("minute_utc")
    detected = switch + pd.Timedelta(minutes=21)  # current-rule prices silent for stale_after_minutes
    assert (decisions.loc[decisions.index < switch, "mechanism"] == "current").all()
    assert (decisions.loc[decisions.index >= detected, "mechanism"] == "new2026").all()
    # the stopped current-rule minute feed is flagged while it could still be late, then treated as stopped
    flagged = decisions["missing_expected_rows"].map(lambda v: "replay_cur" in v)
    assert flagged[(decisions.index > switch + pd.Timedelta(minutes=2)) & (decisions.index < detected)].all()
    assert not flagged[decisions.index > switch + pd.Timedelta(minutes=24)].any()
    detection = engine.status(engine.clock.now())["mechanism_detection"]
    assert detection["switch_observed"] and detection["new_rule_live"] and not detection["current_rule_live"]


def test_reconcile_checks_post_go_live_quarter_hours_against_ods212(tmp_path: Path) -> None:
    frames = synthetic_market_frames(START, hours=2)
    go_live = START + pd.Timedelta(minutes=30)
    end = START + pd.Timedelta(hours=1)
    engine = LiveEngine(
        _Pulse(),
        BATTERY,
        source=ReplayFeeds(_switch_tables(frames, go_live)),
        run_dir=tmp_path / "run",
        clock=ManualClock(START - pd.Timedelta(seconds=30)),
        config=_config(end, go_live_utc=go_live),
    )
    engine.run(drain_minutes=15)
    ledger = engine.ledger.set_index("timestamp_utc")
    assert list(ledger["mechanism"]) == ["current", "current", "new2026", "new2026"]

    validated_nf = frames["nf_qh"].assign(quality_status="Validated")
    validated_nf.loc[3, "imbalance_price_eur_mwh"] = validated_nf.loc[3, "imbalance_price_eur_mwh"] + 25.0
    nf_minute = _switch_tables(frames, go_live)["nf_minute"]
    clock = ManualClock("2026-03-05T00:00:00Z")
    historical = IMBALANCE_PRICE_QH.historical()
    assert historical is not None
    session = FakeEliaSession(
        {"ods134": frames["qh"].assign(quality_status="Validated"), "ods212": validated_nf, "ods211": nf_minute},
        clock=clock.now,
        specs=[historical, NEW_FORMULA_QH, NEW_FORMULA_MINUTE],
    )
    feeds = EliaLiveFeeds(client=JsonHttpClient(session=session, now=clock.now, sleep=lambda _: None))
    result = reconcile_run(tmp_path / "run", sources=("api",), feeds=feeds)
    qh = result.quarter_hours.set_index("timestamp_utc")
    assert list(qh["validated_source"]) == ["api:ods134", "api:ods134", "api:ods212", "api:ods212"]
    assert list(qh["status"]) == ["confirmed", "confirmed", "confirmed", "revised"]
    energy = float(qh["energy_mwh"].iloc[3])
    assert qh["pnl_difference_eur"].iloc[3] == pytest.approx(energy * 25.0, abs=1e-3)
    nf_rev = result.summary["minute_signal_revisions"]["nf_price"]
    assert nf_rev["source"] == "api:ods211" and nf_rev["compared"] >= 25 and nf_rev["revised"] == 0
    assert json.loads((tmp_path / "run" / "reconcile" / "summary.json").read_text())["price_revisions"]["count"] == 1
