from __future__ import annotations

import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams, DecisionContext, PublicationModel, simulate
from euroflex_bess_lab.realtime.forecasting import (
    GBMParams,
    PriceForecaster,
    PriceForecasterConfig,
    PrismForecaster,
    PrismForecasterConfig,
)
from euroflex_bess_lab.realtime.forecasting.deploy import (
    DEPLOY_FILES,
    FUSION_FILE,
    deployment_fold,
    load_forecaster,
    models_dir,
)
from euroflex_bess_lab.realtime.forecasting.synthetic import synthetic_market
from euroflex_bess_lab.realtime.strategies import forecast_mpc, prism, rl
from euroflex_bess_lab.realtime.strategies.base import BaseStrategy
from euroflex_bess_lab.realtime.strategies.prism import MechanismSwitchStrategy

FAST = GBMParams(max_iter=10, max_leaf_nodes=15, min_samples_leaf=20)
BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)


def test_modules_publish_live_factories() -> None:
    assert set(forecast_mpc.LIVE_STRATEGIES) == {"forecast-mpc"}
    assert set(prism.LIVE_STRATEGIES) == {"prism", "scales-l"}
    assert set(rl.LIVE_STRATEGIES) == {"rl-dqn"}


def test_deployment_fold_covers_the_live_period() -> None:
    fold = deployment_fold("2026-10-24T22:00:00Z", data_start="2024-05-21T22:00:00Z")
    assert fold.train_end == fold.test_start == pd.Timestamp("2026-10-24T22:00:00Z")
    assert fold.test_end - fold.test_start >= pd.Timedelta(days=365)
    assert fold.train_start == pd.Timestamp("2026-04-24T22:00:00Z")


def test_missing_models_raise_with_build_hint(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="--stages deploy"):
        load_forecaster("price_current", tmp_path)
    with pytest.raises(FileNotFoundError, match="--stages deploy"):
        forecast_mpc.live_forecast_mpc(models_dir=str(tmp_path))
    assert models_dir(tmp_path) == tmp_path


def test_factories_build_working_strategies(tmp_path: Path) -> None:
    market = synthetic_market(days=9, seed=29)
    end = market.start_utc + pd.Timedelta(days=7)
    fold = deployment_fold(end, data_start=market.start_utc)
    price = PriceForecaster(PriceForecasterConfig(params=FAST, stride=4)).fit(market, [fold])
    current = PrismForecaster(PrismForecasterConfig(params=FAST, stride=4)).fit(market, [fold])
    zero = PrismForecaster(
        PrismForecasterConfig(mechanism="new2026", components="reconstructed", params=FAST, stride=4)
    ).fit(market, [fold])
    for key, model in (("price_current", price), ("prism_current", current), ("prism_new2026", zero)):
        with open(tmp_path / DEPLOY_FILES[key], "wb") as fh:
            pickle.dump(model, fh)
    (tmp_path / FUSION_FILE).write_text(json.dumps({"variance": list(np.linspace(4000.0, 100.0, 16))}))
    live = market.slice(end, market.end_utc)
    runs = [
        (forecast_mpc.live_forecast_mpc(models_dir=str(tmp_path)), "current"),
        (prism.live_prism(mechanism="current", models_dir=str(tmp_path)), "current"),
        (prism.live_prism(mechanism="new2026", models_dir=str(tmp_path)), "new2026"),
        (prism.live_prism(mechanism="auto", models_dir=str(tmp_path)), "new2026"),
    ]
    for strategy, mechanism in runs:
        result = simulate(live, strategy, BATTERY, mechanism=mechanism, record_minutes=False)
        assert np.isfinite(result.pnl_eur)
        assert float(result.qh["discharged_mwh"].sum() + result.qh["charged_mwh"].sum()) > 0.0


class _Constant(BaseStrategy):
    def __init__(self, value: float) -> None:
        self.value = value
        self.resets = 0
        self.refreshes = 0

    def reset(self, ctx: DecisionContext) -> None:
        self.resets += 1

    def refresh(self, ctx: DecisionContext) -> None:
        self.refreshes += 1

    def decide(self, ctx: DecisionContext) -> float:
        return self.value


def test_mechanism_switch_follows_the_settlement_rule() -> None:
    market = synthetic_market(days=1, seed=1)
    old, new = _Constant(0.5), _Constant(-0.5)
    switch = MechanismSwitchStrategy({"current": old, "new2026": new})
    ctx = DecisionContext(market, BATTERY, PublicationModel(), "current")
    ctx.advance(10, BATTERY.initial_soc_mwh)
    switch.reset(ctx)
    assert switch.decide(ctx) == 0.5
    switch.refresh(ctx)
    assert old.refreshes == 1 and new.resets == 0
    ctx.mechanism = "new2026"  # go-live: the engine flips the settlement rule
    ctx.advance(11, BATTERY.initial_soc_mwh)
    switch.refresh(ctx)
    assert new.resets == 1
    assert switch.decide(ctx) == -0.5
    ctx.mechanism = "unknown"
    with pytest.raises(ValueError, match="no strategy"):
        switch.decide(ctx)
