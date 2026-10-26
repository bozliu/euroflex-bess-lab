from __future__ import annotations

import json
import math
import weakref
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import (
    BatteryParams,
    DecisionContext,
    IdleStrategy,
    MarketData,
    PublicationModel,
    ThresholdStrategy,
    settle_quarter_hours,
    simulate,
)
from euroflex_bess_lab.realtime.live import (
    DEFAULT_FEEDS,
    EliaLiveFeeds,
    JsonHttpClient,
    LiveConfig,
    LiveEngine,
    ManualClock,
    MechanismSwitchingStrategy,
    ReplayFeeds,
)
from euroflex_bess_lab.realtime.live.store import DECISIONS, LEDGER_CSV, STATE, STATUS
from euroflex_bess_lab.realtime.live.testing import FakeEliaSession, split_minute_frame, synthetic_market_frames
from euroflex_bess_lab.realtime.strategies import BaseStrategy

START = pd.Timestamp("2026-03-02T00:00:00Z")
BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)


def _offline_market(frames: dict[str, pd.DataFrame], end: pd.Timestamp, *, new_formula: bool = False) -> MarketData:
    qh_frames = {"": frames["qh"]}
    if new_formula:
        qh_frames["nf_"] = frames["nf_qh"]
    return MarketData.from_frames(
        start=START, end=end, minute_frames={"": frames["minute"]}, qh_frames=qh_frames, da_frame=frames["day_ahead"]
    )


def _engine(
    strategy,  # type: ignore[no-untyped-def]
    tmp_path: Path,
    source,  # type: ignore[no-untyped-def]
    *,
    end: pd.Timestamp,
    clock: ManualClock | None = None,
    **config,  # type: ignore[no-untyped-def]
) -> LiveEngine:
    settings = {
        "window_start_utc": START,
        "window_end_utc": end,
        "fsync": False,
        "history_days": 0.0,
        "status_every_minutes": 30,
    }
    settings.update(config)
    return LiveEngine(
        strategy,
        BATTERY,
        source=source,
        run_dir=tmp_path / "run",
        clock=clock or ManualClock(START - pd.Timedelta(seconds=30)),
        config=LiveConfig(**settings),
    )


def _replay(frames: dict[str, pd.DataFrame], **kwargs) -> ReplayFeeds:  # type: ignore[no-untyped-def]
    return ReplayFeeds({"minute": frames["minute"], "qh": frames["qh"], "day_ahead": frames["day_ahead"]}, **kwargs)


def _assert_same_ledger(live: pd.DataFrame, offline: pd.DataFrame) -> None:
    merged = live.merge(offline, on="timestamp_utc", suffixes=("_live", "_off"))
    assert len(merged) == len(offline)
    for column in (
        "energy_mwh",
        "discharged_mwh",
        "charged_mwh",
        "price_eur_mwh",
        "revenue_eur",
        "degradation_eur",
        "pnl_eur",
        "soc_end_mwh",
    ):
        assert np.allclose(merged[f"{column}_live"], merged[f"{column}_off"], atol=1e-12, rtol=0.0), column


@pytest.mark.parametrize(
    "make",
    [
        lambda: ThresholdStrategy(min_history_qh=4, lookback_qh=48),
        lambda: ThresholdStrategy(mode="si", signal_field="si_cum_mw", si_band_mw=120.0, rebalance_power_frac=0.3),
    ],
    ids=["price", "si"],
)
def test_live_decisions_equal_offline_simulation(tmp_path: Path, make) -> None:  # type: ignore[no-untyped-def]
    frames = synthetic_market_frames(START, hours=8)
    end = START + pd.Timedelta(hours=8)
    offline = simulate(_offline_market(frames, end), make(), BATTERY, publication=PublicationModel())
    engine = _engine(make(), tmp_path, _replay(frames), end=end)
    summary = engine.run(drain_minutes=20)
    decisions = engine.decisions()
    assert len(decisions) == 480 and (decisions["status"] == "ok").all()
    assert offline.minute is not None
    assert np.array_equal(decisions["applied_mw"].to_numpy(dtype=float), offline.minute["power_mw"].to_numpy())
    assert np.allclose(decisions["soc_end_mwh"].to_numpy(dtype=float), offline.minute["soc_mwh"].to_numpy(), atol=1e-12)
    _assert_same_ledger(engine.ledger, offline.qh)
    assert summary["ledger"]["settled_qh"] == 32 and summary["counters"]["gap_decisions"] == 0
    assert summary["ledger"]["pnl_eur"] == pytest.approx(offline.pnl_eur, abs=1e-3)


def test_live_with_history_matches_a_delayed_start(tmp_path: Path) -> None:
    """Rows published before the session are backfilled; decisions from the start equal the simulator's."""
    frames = synthetic_market_frames(START, hours=8)
    end = START + pd.Timedelta(hours=8)
    go = START + pd.Timedelta(hours=3)

    class DelayedStart(BaseStrategy):
        name = "delayed"

        def __init__(self) -> None:
            self.inner = ThresholdStrategy(min_history_qh=4, lookback_qh=48)

        def reset(self, ctx: DecisionContext) -> None:
            self.inner.reset(ctx)

        def decide(self, ctx: DecisionContext) -> float:
            return self.inner.decide(ctx) if ctx.now_utc >= go else 0.0

    offline = simulate(_offline_market(frames, end), DelayedStart(), BATTERY)
    clock = ManualClock(go - pd.Timedelta(seconds=20))
    engine = _engine(
        ThresholdStrategy(min_history_qh=4, lookback_qh=48), tmp_path, _replay(frames), end=end, clock=clock
    )
    engine.run(drain_minutes=20)
    decisions = engine.decisions()
    assert decisions["minute_utc"].iloc[0] == go
    assert offline.minute is not None
    expected = offline.minute.loc[offline.minute["timestamp_utc"] >= go, "power_mw"].to_numpy()
    assert np.array_equal(decisions["applied_mw"].to_numpy(dtype=float), expected)
    first_qh = engine.ledger[engine.ledger["timestamp_utc"] >= go]
    _assert_same_ledger(first_qh, offline.qh[offline.qh["timestamp_utc"] >= go])


def test_full_http_path_matches_offline_simulation(tmp_path: Path) -> None:
    """Fake Elia/Energy-Charts HTTP -> normalization -> engine gives the simulator's decisions."""
    frames = synthetic_market_frames(START, hours=3)
    end = START + pd.Timedelta(hours=3)
    make = lambda: ThresholdStrategy(min_history_qh=2, lookback_qh=24)  # noqa: E731
    offline = simulate(_offline_market(frames, end), make(), BATTERY)
    clock = ManualClock(START - pd.Timedelta(seconds=30))
    tables = {
        **split_minute_frame(frames["minute"]),
        "imbalance_price_qh": frames["qh"],
        "day_ahead_be": frames["day_ahead"],
    }
    session = FakeEliaSession(tables, clock=clock.now, delays_s={"price_components_minute": 95.0})
    specs = [replace(spec, poll_every_s=60.0) if spec.poll_every_s < 60 else spec for spec in DEFAULT_FEEDS]
    feeds = EliaLiveFeeds(specs, client=JsonHttpClient(session=session, now=clock.now, sleep=lambda _: None))
    engine = _engine(make(), tmp_path, feeds, end=end, clock=clock)
    summary = engine.run(drain_minutes=15)
    decisions = engine.decisions()
    assert offline.minute is not None
    assert np.array_equal(decisions["applied_mw"].to_numpy(dtype=float), offline.minute["power_mw"].to_numpy())
    _assert_same_ledger(engine.ledger, offline.qh)
    latency = summary["feed_latency_s"]
    # rows are published 20 s after the minute; the 60 s poll lands 8 s before the next minute boundary
    assert latency["imbalance_price_minute"]["latency_upper_s"]["max"] == pytest.approx(52.0, abs=1.0)
    assert latency["imbalance_price_qh"]["latency_upper_s"]["p50"] >= 150.0
    # ods175 (aFRR prices) is slower than the one-minute lag: decisions are flagged, values never leak
    assert summary["counters"]["gap_decisions"] > 0
    flagged = decisions["missing_expected_rows"].map(lambda v: "price_components_minute" in v)
    assert flagged.mean() > 0.9
    assert all(r["url"].startswith("https://") for r in session.requests)
    assert not any(r["params"].get("limit") not in (None, "20", "12", "5", "8") for r in session.requests)


class _Spy(BaseStrategy):
    """Asserts at every decision that nothing beyond the visible indices exists in the live window."""

    name = "spy"

    def __init__(self) -> None:
        self.checked = 0
        self.mechanisms: list[str] = []

    def decide(self, ctx: DecisionContext) -> float:
        market = ctx.market
        for name in ("price_cum_eur_mwh", "si_cum_mw"):
            assert np.isnan(market.minute[name][ctx.visible_minute_end :]).all()
        assert np.isnan(market.settlement(ctx.mechanism)[ctx.visible_qh_end :]).all()
        hidden = market.da_visible_from_minute > ctx.t
        assert np.isnan(market.da_price[hidden]).all()
        self.checked += 1
        self.mechanisms.append(ctx.mechanism)
        return 1.0 if ctx.minute_in_qh < 5 else -1.0


def test_unpublished_rows_never_reach_the_strategy_and_late_rows_are_flagged(tmp_path: Path) -> None:
    frames = synthetic_market_frames(START, hours=2)
    end = START + pd.Timedelta(hours=2)
    spy = _Spy()
    # minute rows arrive 90 s after the minute ends: later than the one-minute publication lag allows
    engine = _engine(spy, tmp_path, _replay(frames, delays_s={"minute": 90.0, "qh": 30.0}), end=end)
    summary = engine.run(drain_minutes=5)
    assert spy.checked == 120
    decisions = engine.decisions()
    assert summary["counters"]["gap_decisions"] >= 115
    assert decisions["missing_expected_rows"].iloc[-1] == ["replay_minute"]
    # rows that arrive early (qh prices after 30 s) are held back until qh_final_lag allows them
    assert summary["ledger"]["settled_qh"] == 8


class _Constant(BaseStrategy):
    name = "constant"

    def __init__(self, schedule: dict[int, float]) -> None:
        self.schedule = schedule

    def decide(self, ctx: DecisionContext) -> float:
        return self.schedule.get(ctx.qh_index, 0.0)


def test_quarter_hour_settlement_pays_long_and_charges_short(tmp_path: Path) -> None:
    frames = synthetic_market_frames(START, hours=1)
    end = START + pd.Timedelta(hours=1)
    engine = _engine(_Constant({0: 1.0, 1: -1.0}), tmp_path, _replay(frames), end=end)
    engine.run(drain_minutes=10)
    ledger = engine.ledger.set_index("timestamp_utc")
    prices = frames["qh"].set_index("timestamp_utc")["imbalance_price_eur_mwh"].astype(float)
    q0, q1 = START, START + pd.Timedelta(minutes=15)
    assert ledger.loc[q0, "energy_mwh"] == pytest.approx(0.25)
    assert ledger.loc[q0, "revenue_eur"] == pytest.approx(0.25 * prices[q0])
    assert ledger.loc[q0, "degradation_eur"] == pytest.approx(0.25 * BATTERY.degradation_eur_per_mwh)
    assert ledger.loc[q1, "revenue_eur"] == pytest.approx(-0.25 * prices[q1])
    assert ledger.loc[q1, "degradation_eur"] == 0.0
    assert (ledger["mechanism"] == "current").all() and ledger["provisional"].all()
    assert (ledger["price_source"] == "replay_qh").all() and (ledger["minutes_decided"] == 15).all()
    power = np.r_[np.ones(15), -np.ones(15), np.zeros(30)]
    expected = settle_quarter_hours(power, np.zeros(60), prices.to_numpy(), pd.DatetimeIndex(prices.index), BATTERY)
    assert np.allclose(ledger["pnl_eur"].to_numpy(), expected["pnl_eur"].to_numpy())


def test_session_starting_mid_quarter_hour_books_the_minutes_before_as_idle(tmp_path: Path) -> None:
    frames = synthetic_market_frames(START, hours=1)
    end = START + pd.Timedelta(hours=1)
    clock = ManualClock(START + pd.Timedelta(minutes=7, seconds=10))
    engine = _engine(_Constant({0: 1.0}), tmp_path, _replay(frames), end=end, clock=clock)
    engine.run(drain_minutes=10)
    first = engine.ledger.iloc[0]
    assert first["timestamp_utc"] == START
    assert first["minutes_outside_session"] == 8 and first["minutes_decided"] == 7
    assert first["energy_mwh"] == pytest.approx(7 / 60)


def _switch_frames(hours: int = 2) -> tuple[dict[str, pd.DataFrame], pd.Timestamp]:
    frames = synthetic_market_frames(START, hours=hours)
    return frames, START + pd.Timedelta(hours=1)


def test_mechanism_switches_to_new2026_at_the_go_live_when_the_feed_carries_it(tmp_path: Path) -> None:
    frames, go_live = _switch_frames()
    end = START + pd.Timedelta(hours=2)
    nf_minute = frames["minute"][["timestamp_utc"]].assign(
        imbalance_price_eur_mwh=frames["minute"]["price_cum_eur_mwh"] + 1.0
    )
    source = ReplayFeeds(
        {
            "minute": frames["minute"],
            "qh": frames["qh"],
            "nf_minute": nf_minute[nf_minute["timestamp_utc"] >= go_live],
            "nf_qh": frames["nf_qh"][frames["nf_qh"]["timestamp_utc"] >= go_live],
        }
    )
    strategy = MechanismSwitchingStrategy(
        {
            "current": ThresholdStrategy(min_history_qh=2, signal_field="price_cum_eur_mwh"),
            "new2026": ThresholdStrategy(min_history_qh=2, signal_field="nf_imbalance_price_eur_mwh"),
        },
        name="threshold",
    )
    engine = _engine(strategy, tmp_path, source, end=end, go_live_utc=go_live)
    engine.run(drain_minutes=15)
    ledger = engine.ledger.set_index("timestamp_utc")
    assert (ledger.loc[ledger.index < go_live, "mechanism"] == "current").all()
    after = ledger.loc[ledger.index >= go_live]
    assert (after["mechanism"] == "new2026").all() and not after["mechanism_fallback"].any()
    nf_price = frames["nf_qh"].set_index("timestamp_utc")["imbalance_price_eur_mwh"].astype(float)
    assert np.allclose(after["price_eur_mwh"], nf_price.loc[after.index])
    decisions = engine.decisions().set_index("minute_utc")
    assert (decisions.loc[decisions.index < go_live, "mechanism"] == "current").all()
    # the first minute row of the new feed becomes visible two minutes after the go-live
    assert (decisions.loc[decisions.index >= go_live + pd.Timedelta(minutes=2), "mechanism"] == "new2026").all()
    assert decisions["strategy_state.active_mechanism"].iloc[-1] == "new2026"


def test_auto_mechanism_falls_back_to_current_when_the_new_feed_is_not_live(tmp_path: Path) -> None:
    frames, go_live = _switch_frames()
    end = START + pd.Timedelta(hours=2)
    engine = _engine(
        _Constant({}), tmp_path, _replay(frames), end=end, go_live_utc=go_live, new_formula_grace_minutes=10
    )
    engine.run(drain_minutes=20)
    ledger = engine.ledger.set_index("timestamp_utc")
    after = ledger.loc[ledger.index >= go_live]
    assert len(after) == 4 and (after["mechanism"] == "current").all() and after["mechanism_fallback"].all()
    assert not ledger.loc[ledger.index < go_live, "mechanism_fallback"].any()


def test_forced_new2026_without_a_live_feed_leaves_prices_missing(tmp_path: Path) -> None:
    frames = synthetic_market_frames(START, hours=1)
    end = START + pd.Timedelta(hours=1)
    engine = _engine(
        _Constant({0: 1.0}), tmp_path, _replay(frames), end=end, mechanism="new2026", settle_timeout_minutes=20
    )
    summary = engine.run(drain_minutes=25)
    assert summary["ledger"]["price_missing_qh"] == summary["ledger"]["settled_qh"] == 4
    assert engine.ledger["pnl_eur"].iloc[0] == pytest.approx(-0.25 * BATTERY.degradation_eur_per_mwh)
    assert any("new2026 before the go-live" in note for note in engine.status(engine.clock.now())["notes"])


def test_strategy_errors_idle_the_battery_and_are_journaled(tmp_path: Path) -> None:
    class Broken(BaseStrategy):
        name = "broken"

        def decide(self, ctx: DecisionContext) -> float:
            if ctx.minute_in_qh == 3:
                raise RuntimeError("model unavailable")
            return math.nan if ctx.minute_in_qh == 4 else 1.0

    frames = synthetic_market_frames(START, hours=1)
    engine = _engine(Broken(), tmp_path, _replay(frames), end=START + pd.Timedelta(minutes=15))
    summary = engine.run()
    decisions = engine.decisions().set_index("minute_in_qh")
    assert decisions.loc[3, "status"] == "strategy_error" and "model unavailable" in decisions.loc[3, "error"]
    assert decisions.loc[3, "applied_mw"] == 0.0 and decisions.loc[4, "applied_mw"] == 0.0
    assert decisions.loc[4, "requested_mw"] is None or pd.isna(decisions.loc[4, "requested_mw"])
    assert summary["counters"]["strategy_errors"] == 1


def test_a_late_loop_records_missed_minutes_as_idle(tmp_path: Path) -> None:
    frames = synthetic_market_frames(START, hours=1)
    clock = ManualClock(START - pd.Timedelta(seconds=30))
    engine = _engine(
        _Constant({0: 1.0, 1: 1.0}), tmp_path, _replay(frames), end=START + pd.Timedelta(hours=1), clock=clock
    )
    engine.start()
    for _ in range(5):
        engine.step()
    soc = engine.state.soc_mwh if engine.state else 0.0
    clock.advance(4 * 60 + 25)  # the process stalls: minutes 5..8 can no longer be acted on
    engine.step()
    decisions = engine.decisions()
    missed = decisions[decisions["status"] == "missed"]
    assert list(missed["minute_utc"].dt.minute) == [5, 6, 7, 8]
    assert (missed["applied_mw"] == 0.0).all() and (missed["soc_end_mwh"] == soc).all()
    assert engine.next_minute == START + pd.Timedelta(minutes=9)
    engine.run(max_minutes=20, drain_minutes=10)
    first = engine.ledger.iloc[0]
    assert first["minutes_missed"] == 4 and first["energy_mwh"] == pytest.approx(11 / 60)


def test_crash_and_resume_is_seamless(tmp_path: Path) -> None:
    """Crash after 45 minutes (partial journal line), restart 15 minutes later: no duplicates, idle gap."""
    frames = synthetic_market_frames(START, hours=3)
    end = START + pd.Timedelta(hours=3)
    crash, back = START + pd.Timedelta(minutes=45), START + pd.Timedelta(minutes=60)
    make = lambda: ThresholdStrategy(min_history_qh=2, lookback_qh=24)  # noqa: E731

    class WithOutage(BaseStrategy):
        name = "outage"

        def __init__(self) -> None:
            self.inner = make()

        def reset(self, ctx: DecisionContext) -> None:
            self.inner.reset(ctx)

        def decide(self, ctx: DecisionContext) -> float:
            return 0.0 if crash <= ctx.now_utc < back else self.inner.decide(ctx)

    offline = simulate(_offline_market(frames, end), WithOutage(), BATTERY)

    clock = ManualClock(START - pd.Timedelta(seconds=30))
    first = _engine(make(), tmp_path, _replay(frames), end=end, clock=clock)
    first.start()
    while first.next_minute is not None and first.next_minute < crash:
        first.step()
    run_dir = tmp_path / "run"
    with (run_dir / DECISIONS).open("a", encoding="utf-8") as handle:
        handle.write('{"minute_utc": "2026-03-02T00:45:00Z", "status": "o')  # torn write
    with (run_dir / LEDGER_CSV).open("a", encoding="utf-8") as handle:
        handle.write("2026-03-02T00:30:00.000Z,0.1")
    del first  # no stop(): the process died

    clock2 = ManualClock(back - pd.Timedelta(seconds=10))
    second = _engine(make(), tmp_path, _replay(frames), end=end, clock=clock2)
    summary = second.run(drain_minutes=15)
    decisions = second.decisions()
    assert decisions["minute_utc"].is_unique and len(decisions) == 180
    missed = decisions[decisions["status"] == "missed"]
    assert len(missed) == 15 and (missed["minute_utc"] >= crash).all() and (missed["minute_utc"] < back).all()
    assert offline.minute is not None
    assert np.array_equal(decisions["applied_mw"].to_numpy(dtype=float), offline.minute["power_mw"].to_numpy())
    ledger = second.store.read_ledger(all_revisions=True)
    assert ledger["timestamp_utc"].is_unique
    _assert_same_ledger(second.ledger, offline.qh)
    assert summary["counters"]["resumes"] == 1
    events = [json.loads(line)["event"] for line in (run_dir / "events.jsonl").read_text().splitlines()]
    assert events == ["start", "resume", "stop"]


def _reject_constant(name: str) -> None:
    pytest.fail(f"non-strict JSON constant {name}")


def test_run_directory_files_are_relative_and_strict_json(tmp_path: Path) -> None:
    frames = synthetic_market_frames(START, hours=1)
    engine = _engine(IdleStrategy(), tmp_path, _replay(frames), end=START + pd.Timedelta(minutes=30))
    engine.run(drain_minutes=10)
    run_dir = tmp_path / "run"
    for path in run_dir.iterdir():
        if path.suffix in {".json", ".jsonl", ".csv"}:
            text = path.read_text(encoding="utf-8")
            assert str(tmp_path) not in text and str(Path.home()) not in text, path.name
            if path.suffix == ".json":
                json.loads(text, parse_constant=_reject_constant)
    status = json.loads((run_dir / STATUS).read_text())
    assert status["market_submission"] is False and status["mode"] == "paper" and status["state"] == "completed"
    state = json.loads((run_dir / STATE).read_text())
    assert state["schema"] == "euroflex-live-state/1"


def test_window_extends_without_changing_decisions(tmp_path: Path) -> None:
    start = pd.Timestamp("2026-03-02T20:00:00Z")
    frames = synthetic_market_frames(start, hours=6)
    end = start + pd.Timedelta(hours=6)
    make = lambda: ThresholdStrategy(min_history_qh=2, lookback_qh=24)  # noqa: E731
    market = MarketData.from_frames(
        start=start,
        end=end,
        minute_frames={"": frames["minute"]},
        qh_frames={"": frames["qh"]},
        da_frame=frames["day_ahead"],
    )
    offline = simulate(market, make(), BATTERY)
    clock = ManualClock(start - pd.Timedelta(seconds=30))
    engine = LiveEngine(
        make(),
        BATTERY,
        source=_replay(frames),
        run_dir=tmp_path / "run",
        clock=clock,
        config=LiveConfig(window_start_utc=start, horizon_days=0, fsync=False, history_days=0.0),
    )
    engine.run(until=end)
    assert engine.market is not None and engine.market.end_utc > end
    assert offline.minute is not None
    assert np.array_equal(engine.decisions()["applied_mw"].to_numpy(dtype=float), offline.minute["power_mw"].to_numpy())


def test_rebuild_shifts_arrays_and_requests_a_reset(tmp_path: Path) -> None:
    frames = synthetic_market_frames(START, hours=2)
    engine = _engine(IdleStrategy(), tmp_path, _replay(frames), end=START + pd.Timedelta(hours=2))
    engine.run(max_minutes=40)
    assert engine.market is not None
    before = engine.market.minute["price_cum_eur_mwh"].copy()
    engine._rebuild(START + pd.Timedelta(minutes=30), START + pd.Timedelta(hours=3))
    after = engine.market.minute["price_cum_eur_mwh"]
    assert len(after) == 150 and np.array_equal(after[:10], before[30:40], equal_nan=True)
    assert np.isnan(after[120:]).all() and engine._needs_reset
    assert engine.market.settlement("current") is engine.market.qh["imbalance_price_eur_mwh"]


class _PlansInReset(BaseStrategy):
    """Precomputes a plan for every minute in reset() from causal features, memoised per market object (like the
    forecast-MPC strategies with a cached forecaster)."""

    name = "plans_in_reset"

    def __init__(self) -> None:
        self._memo: tuple[weakref.ref[MarketData], np.ndarray] | None = None
        self.plan = np.zeros(0)

    def reset(self, ctx: DecisionContext) -> None:
        if self._memo is not None and self._memo[0]() is ctx.market:
            self.plan = self._memo[1]
            return
        price = ctx.market.minute["price_cum_eur_mwh"]
        shift = ctx.publication.minute_lag + 1  # the row visible at t is t - lag - 1
        seen = np.full_like(price, np.nan)
        seen[shift:] = price[:-shift]
        self.plan = np.where(seen > 100.0, 1.0, np.where(seen < 40.0, -1.0, 0.0))
        self._memo = (weakref.ref(ctx.market), self.plan)

    def decide(self, ctx: DecisionContext) -> float:
        return float(self.plan[ctx.t])


def test_replanning_makes_precomputing_strategies_live_identical(tmp_path: Path) -> None:
    frames = synthetic_market_frames(START, hours=2)
    end = START + pd.Timedelta(hours=2)
    offline = simulate(_offline_market(frames, end), _PlansInReset(), BATTERY)
    assert offline.minute is not None
    expected = offline.minute["power_mw"].to_numpy()
    naive = _engine(_PlansInReset(), tmp_path / "naive", _replay(frames), end=end)
    naive.run(drain_minutes=10)
    assert not np.array_equal(naive.decisions()["applied_mw"].to_numpy(dtype=float), expected)
    replanned = _engine(_PlansInReset(), tmp_path / "replan", _replay(frames), end=end, replan_every="minute")
    replanned.run(drain_minutes=10)
    assert np.array_equal(replanned.decisions()["applied_mw"].to_numpy(dtype=float), expected)
    _assert_same_ledger(replanned.ledger, offline.qh)


def test_refresh_hook_runs_before_every_decision_on_a_fresh_view(tmp_path: Path) -> None:
    class Refreshing(BaseStrategy):
        name = "refreshing"

        def __init__(self) -> None:
            self.views: list[int] = []

        def refresh(self, ctx: DecisionContext) -> None:
            self.views.append(id(ctx.market))
            assert np.isnan(ctx.market.minute["price_cum_eur_mwh"][ctx.visible_minute_end :]).all()

        def decide(self, ctx: DecisionContext) -> float:
            return 0.0

    frames = synthetic_market_frames(START, hours=1)
    strategy = Refreshing()
    engine = _engine(strategy, tmp_path, _replay(frames), end=START + pd.Timedelta(minutes=30))
    engine.run()
    assert len(strategy.views) == 29  # the first decision runs reset() instead
    assert engine.ctx is not None and engine.ctx.market is not engine.market  # a view, not the window itself


def test_stop_request_ends_the_loop_cleanly(tmp_path: Path) -> None:
    frames = synthetic_market_frames(START, hours=1)

    class StopsAfterTen(BaseStrategy):
        name = "stops"

        def __init__(self) -> None:
            self.engine: LiveEngine | None = None

        def decide(self, ctx: DecisionContext) -> float:
            if ctx.t == 9 and self.engine is not None:
                self.engine.request_stop()
            return 0.0

    strategy = StopsAfterTen()
    engine = _engine(strategy, tmp_path, _replay(frames), end=START + pd.Timedelta(hours=1))
    strategy.engine = engine
    summary = engine.run()
    assert summary["state"] == "stopped" and summary["counters"]["decisions"] == 10
    assert json.loads((tmp_path / "run" / STATUS).read_text())["state"] == "stopped"
