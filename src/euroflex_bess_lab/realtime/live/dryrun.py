"""Dry run: replay a historical period through the live engine and compare it with the backtest.

:func:`replay_check` loads a period from the committed public data set, runs the strategy once with
:func:`~euroflex_bess_lab.realtime.simulate` and once through :class:`LiveEngine` fed by :class:`ReplayFeeds` (rows
published after their completion time, simulated clock), and reports whether every decision and every ledger row is
identical. It is the reproducible evidence that a backtested strategy runs unchanged in the live loop.
"""

from __future__ import annotations

import importlib
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ..replay import MarketData
from ..simulator import simulate
from ..strategies.base import Strategy
from ..types import BatteryParams, PublicationModel
from .clock import ManualClock
from .engine import LiveConfig, LiveEngine, MechanismSetting, ReplanSetting
from .replay_feeds import ReplayFeeds


def public_tables(start: pd.Timestamp, end: pd.Timestamp, *, new_formula: bool = False) -> dict[str, pd.DataFrame]:
    """Contract tables for ``[start, end)`` from ``data/public`` (keys as expected by :class:`ReplayFeeds`)."""
    pdata: Any = importlib.import_module("euroflex_bess_lab.public_data")
    tables: dict[str, pd.DataFrame] = {
        "minute": pdata.load_belgium_minute(start, end),
        "qh": pdata.load_belgium_qh(start, end),
    }
    for kind, table in (("current", "si_forecast_current"), ("next", "si_forecast_next")):
        try:
            tables[table] = pdata.load_belgium_si_forecast(kind, start, end)
        except FileNotFoundError:
            continue
    try:
        tables["day_ahead"] = pdata.load_day_ahead_prices(
            "BE", start - pd.Timedelta(days=1), end + pd.Timedelta(days=1)
        )
    except FileNotFoundError:
        pass
    if new_formula:
        tables["nf_minute"] = pdata.load_belgium_new_formula_minute(start, end)
        tables["nf_qh"] = pdata.load_belgium_new_formula_qh(start, end)
    return {name: frame for name, frame in tables.items() if frame is not None and len(frame)}


def market_from_tables(tables: dict[str, pd.DataFrame], start: pd.Timestamp, end: pd.Timestamp) -> MarketData:
    minute_frames = {"": tables["minute"]}
    qh_frames = {"": tables["qh"]}
    if "nf_minute" in tables:
        minute_frames["nf_"] = tables["nf_minute"]
    if "nf_qh" in tables:
        qh_frames["nf_"] = tables["nf_qh"]
    forecasts = {
        kind: tables[table]
        for kind, table in (("current_qh", "si_forecast_current"), ("next_qh", "si_forecast_next"))
        if table in tables
    }
    return MarketData.from_frames(
        start=start,
        end=end,
        minute_frames=minute_frames,
        qh_frames=qh_frames,
        da_frame=tables.get("day_ahead"),
        si_forecast_frames=forecasts,
    )


def replay_check(
    make_strategy: Callable[[], Strategy],
    battery: BatteryParams,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    *,
    mechanism: MechanismSetting = "current",
    publication: PublicationModel | None = None,
    replan_every: ReplanSetting = "never",
    tables: dict[str, pd.DataFrame] | None = None,
    run_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Run ``make_strategy()`` offline and through the live engine on the same period; report the differences."""
    start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
    start_ts = start_ts.tz_localize("UTC") if start_ts.tzinfo is None else start_ts.tz_convert("UTC")
    end_ts = end_ts.tz_localize("UTC") if end_ts.tzinfo is None else end_ts.tz_convert("UTC")
    if mechanism == "auto":
        raise ValueError("replay_check compares one settlement mechanism: use current or new2026")
    publication = publication or PublicationModel()
    tables = tables if tables is not None else public_tables(start_ts, end_ts, new_formula=mechanism == "new2026")
    market = market_from_tables(tables, start_ts, end_ts)
    started = time.perf_counter()
    offline = simulate(market, make_strategy(), battery, mechanism=mechanism, publication=publication)
    offline_s = time.perf_counter() - started
    config = LiveConfig(
        mechanism=mechanism,
        publication=publication,
        window_start_utc=start_ts,
        window_end_utc=end_ts,
        history_days=0.0,
        fsync=False,
        status_every_minutes=240,
        replan_every=replan_every,
    )
    with tempfile.TemporaryDirectory() as scratch:
        target = Path(run_dir) if run_dir is not None else Path(scratch) / "replay"
        engine = LiveEngine(
            make_strategy(),
            battery,
            source=ReplayFeeds(tables),
            run_dir=target,
            clock=ManualClock(start_ts - pd.Timedelta(seconds=30)),
            config=config,
            metadata={"launcher": "replay_check"},
        )
        started = time.perf_counter()
        summary = engine.run(drain_minutes=30)
        live_s = time.perf_counter() - started
        decisions = engine.decisions()
        ledger = engine.ledger
    assert offline.minute is not None
    live_power = decisions["applied_mw"].to_numpy(dtype=float) if len(decisions) else np.zeros(0)
    offline_power = offline.minute["power_mw"].to_numpy()
    n = min(len(live_power), len(offline_power))
    differing = np.flatnonzero(~np.isclose(live_power[:n], offline_power[:n], atol=1e-12, rtol=0.0))
    merged = ledger.merge(offline.qh, on="timestamp_utc", suffixes=("_live", "_offline")) if len(ledger) else ledger
    max_pnl_diff = (
        float(np.max(np.abs(merged["pnl_eur_live"] - merged["pnl_eur_offline"]))) if len(merged) else float("nan")
    )
    return {
        "period": {"start_utc": start_ts.isoformat(), "end_utc": end_ts.isoformat()},
        "strategy": summary["strategy"],
        "mechanism": mechanism,
        "minutes": int(len(offline_power)),
        "live_minutes": int(len(live_power)),
        "identical_decisions": bool(len(live_power) == len(offline_power) and len(differing) == 0),
        "first_differing_minutes": [str(offline.minute["timestamp_utc"].iloc[i]) for i in differing[:5]],
        "quarter_hours": int(len(offline.qh)),
        "ledger_rows_compared": int(len(merged)),
        "max_abs_qh_pnl_difference_eur": max_pnl_diff,
        "pnl_live_eur": round(float(ledger["pnl_eur"].sum()), 6) if len(ledger) else 0.0,
        "pnl_offline_eur": round(offline.pnl_eur, 6),
        "gap_decisions": summary["counters"]["gap_decisions"],
        "offline_seconds": round(offline_s, 2),
        "live_replay_seconds": round(live_s, 2),
    }
