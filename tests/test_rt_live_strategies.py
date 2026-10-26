"""Live integration of the forecast-driven strategies from :mod:`euroflex_bess_lab.realtime.strategies`."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams, DecisionContext, MarketData, simulate
from euroflex_bess_lab.realtime.live import LiveConfig, LiveEngine, ManualClock, ReplayFeeds, make_strategy
from euroflex_bess_lab.realtime.strategies import BaseStrategy

pytest.importorskip("sklearn")
forecasting = pytest.importorskip("euroflex_bess_lab.realtime.forecasting")
mpc = pytest.importorskip("euroflex_bess_lab.realtime.strategies.forecast_mpc")
synthetic = pytest.importorskip("euroflex_bess_lab.realtime.forecasting.synthetic")

BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)


def _tables(market: MarketData) -> dict[str, pd.DataFrame]:
    """Contract tables (for ReplayFeeds) from a MarketData bundle."""
    plain = {k: v for k, v in market.minute.items() if not k.startswith(("nf_", "elia_fc_"))}
    tables = {
        "minute": pd.DataFrame({"timestamp_utc": market.minute_times, **plain}),
        "nf_minute": pd.DataFrame(
            {
                "timestamp_utc": market.minute_times,
                **{k[3:]: v for k, v in market.minute.items() if k.startswith("nf_")},
            }
        ),
        "qh": pd.DataFrame(
            {"timestamp_utc": market.qh_times, **{k: v for k, v in market.qh.items() if not k.startswith("nf_")}}
        ),
        "nf_qh": pd.DataFrame(
            {"timestamp_utc": market.qh_times, **{k[3:]: v for k, v in market.qh.items() if k.startswith("nf_")}}
        ),
        "day_ahead": pd.DataFrame(
            {"timestamp_utc": market.qh_times, "resolution_minutes": 15, "price_eur_mwh": market.da_price}
        ),
    }
    for prefix, table in (("elia_fc_cur_", "si_forecast_current"), ("elia_fc_next_", "si_forecast_next")):
        columns = {k[len(prefix) :]: v for k, v in market.minute.items() if k.startswith(prefix)}
        if columns:
            frame = pd.DataFrame(
                {
                    "prediction_time_utc": market.minute_times,
                    "target_qh_utc": market.minute_times.floor("15min"),
                    **columns,
                }
            )
            tables[table] = frame.dropna(subset=list(columns), how="all")
    return tables


def test_forecast_mpc_strategy_trades_live_exactly_as_in_the_backtest(tmp_path: Path) -> None:
    market = synthetic.synthetic_market(days=4, seed=21)
    go = market.start_utc + pd.Timedelta(days=3)
    folds = forecasting.single_fold(market.start_utc, go, go, market.end_utc)
    params = forecasting.GBMParams(max_iter=8, max_leaf_nodes=15, min_samples_leaf=20)
    forecaster = forecasting.PriceForecaster(forecasting.PriceForecasterConfig(params=params, stride=4)).fit(
        market, folds
    )
    w0, w1 = go - pd.Timedelta(hours=12), go + pd.Timedelta(minutes=45)
    window = market.slice(w0, w1)

    class FromGo(BaseStrategy):
        name = "from_go"

        def __init__(self) -> None:
            self.inner = mpc.ForecastMPCStrategy(mpc.CachedForecaster(forecaster))

        def reset(self, ctx: DecisionContext) -> None:
            self.inner.reset(ctx)

        def decide(self, ctx: DecisionContext) -> float:
            return self.inner.decide(ctx) if ctx.now_utc >= go else 0.0

    offline = simulate(window, FromGo(), BATTERY)
    assert offline.minute is not None
    expected = offline.minute.loc[offline.minute["timestamp_utc"] >= go, "power_mw"].to_numpy()
    assert np.count_nonzero(expected) > 10
    engine = LiveEngine(
        mpc.ForecastMPCStrategy(mpc.CachedForecaster(forecaster)),
        BATTERY,
        source=ReplayFeeds(_tables(window)),
        run_dir=tmp_path / "run",
        clock=ManualClock(go - pd.Timedelta(seconds=20)),
        config=LiveConfig(
            window_start_utc=w0,
            window_end_utc=w1,
            history_days=0.0,
            minute_history_hours=24.0,
            replan_every="minute",
            fsync=False,
            status_every_minutes=60,
        ),
    )
    engine.run(drain_minutes=15)
    live = engine.decisions()["applied_mw"].to_numpy(dtype=float)
    assert np.array_equal(live, expected)
    merged = engine.ledger.merge(offline.qh, on="timestamp_utc", suffixes=("_live", "_off"))
    assert len(merged) == 3 and np.allclose(merged["pnl_eur_live"], merged["pnl_eur_off"], atol=1e-9)


def test_forecast_strategies_without_deployed_models_fail_cleanly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from euroflex_bess_lab.realtime.forecasting.deploy import DeploymentModelsMissing

    monkeypatch.setenv("EUROFLEX_MODELS_DIR", str(tmp_path / "no-models"))
    with pytest.raises(DeploymentModelsMissing):
        make_strategy("forecast-mpc")
