from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams, MarketData, ThresholdStrategy
from euroflex_bess_lab.realtime.reserve import (
    CapacityAwards,
    DeliveryBlocks,
    ForecastSplitPolicy,
    ReserveStudyConfig,
    activation_diagnostics,
    build_activation,
    monthly_allocation,
    run_policy_suite,
    save_ledger,
    stack_table,
    synthetic_reserve_inputs,
)

BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "public"


def _inner() -> ThresholdStrategy:
    return ThresholdStrategy(min_history_qh=8, lookback_qh=96)


def _suite(days: int = 5):
    market, frame = synthetic_reserve_inputs(days=days, seed=21, capacity_price_up=6.0, capacity_price_down=4.0)
    blocks = DeliveryBlocks.regular(market.start_utc, market.end_utc)
    history = CapacityAwards.from_frame(frame)
    awards = history.aligned_to(blocks)
    config = ReserveStudyConfig(forecast=ForecastSplitPolicy(lookback_days=3, min_history=1))
    suite = run_policy_suite(market, awards, make_inner=_inner, config=config, history_awards=history)
    return market, suite


def test_policy_suite_runs_every_policy_and_tabulates_periods() -> None:
    market, suite = _suite()
    assert set(suite.results) == {"fixed_0", "fixed_50", "fixed_100", "forecast", "oracle"}
    # the capacity forecast already has history on day one thanks to the earlier auctions
    assert np.isfinite(suite.capacity_forecast["up"]).all()
    mid = market.start_utc + pd.Timedelta(days=2)
    table = stack_table(suite.results, {"first": (market.start_utc, mid), "second": (mid, market.end_utc)})
    assert set(table["period"]) == {"first", "second"}
    assert len(table) == 10
    fixed_0 = table[(table["policy"] == "fixed_0")]
    assert (fixed_0["afrr_capacity"] == 0.0).all() and (fixed_0["mean_up_reserved_mw"] == 0.0).all()
    full = table[(table["policy"] == "fixed_100")]
    assert np.allclose(full["mean_up_reserved_mw"], 1.0) and (full["implicit_balancing"] == 0.0).all()
    shares = suite.decisions["forecast"].reserved_share()
    assert set(np.unique(shares)) <= {0.0, 0.5, 1.0}
    months = monthly_allocation(suite.results["forecast"])
    assert {"month", "up_reserved_mw", "down_reserved_mw", "pnl_eur"} <= set(months.columns)


def test_ledgers_are_small_relative_and_documented(tmp_path: Path) -> None:
    _, suite = _suite(days=3)
    result = suite.results["forecast"]
    paths = save_ledger(result, tmp_path, period_label="test")
    assert paths["parquet"].name == "reserve_forecast_current_test.parquet"
    frame = pd.read_parquet(paths["parquet"])
    for column in (
        "timestamp_utc",
        "strategy",
        "mechanism",
        "fleet_mw",
        "energy_mwh",
        "price_eur_mwh",
        "revenue_eur",
        "degradation_eur",
        "pnl_eur",
        "afrr_capacity_eur",
        "afrr_energy_eur",
        "implicit_revenue_eur",
        "afrr_up_reserved_mw",
    ):
        assert column in frame.columns, column
    assert frame["pnl_eur"].astype(float).sum() == pytest.approx(result.pnl_eur, rel=1e-5)
    text = paths["json"].read_text(encoding="utf-8")
    assert "/" + "Users" + "/" not in text  # no absolute local paths in sidecars
    sidecar = json.loads(text)
    assert sidecar["policy"] == "forecast" and sidecar["rows"] == len(frame)
    assert paths["parquet"].stat().st_size < 5_000_000


@pytest.mark.skipif(
    not (FIXTURES / "belgium" / "afrr_capacity_awards.parquet").exists(), reason="public fixtures not built"
)
def test_reserve_suite_on_public_fixture_days(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EUROFLEX_DATA_DIR", str(FIXTURES))
    start, end = "2026-03-02T00:00:00Z", "2026-03-05T00:00:00Z"
    columns = ["price_cum_eur_mwh", "si_cum_mw", "afrr_up_mw", "afrr_down_mw", "afrr_price_up_eur_mwh"]
    market = MarketData.from_public(
        start, end, include_new_formula=True, include_si_forecast=False, include_day_ahead=True, minute_columns=columns
    )
    history = CapacityAwards.from_public("2025-06-01T00:00:00Z", end)
    awards = history.aligned_to(DeliveryBlocks.regular(market.start_utc, market.end_utc))
    assert awards.meta["matched_blocks"] >= 12
    # after PICASSO, Belgian-provider activation (ODS126) is well below Elia's satisfied demand (ODS127)
    assert np.nanmean(np.abs(market.minute["afrr_up_mw"])) < np.nanmean(np.abs(market.qh["afrr_up_mw"]))
    diagnostics = activation_diagnostics(market, awards, build_activation(market, awards))
    assert 0.0 < diagnostics["up"]["mean_ratio"] < 1.0 and 0.0 < diagnostics["down"]["mean_ratio"] < 1.0
    config = ReserveStudyConfig(forecast=ForecastSplitPolicy(lookback_days=1, min_history=1))
    totals = {}
    for mechanism, signal in (("current", "price_cum_eur_mwh"), ("new2026", "nf_imbalance_price_eur_mwh")):

        def make(signal: str = signal) -> ThresholdStrategy:
            return ThresholdStrategy(signal_field=signal, min_history_qh=8)

        suite = run_policy_suite(
            market, awards, make_inner=make, mechanism=mechanism, config=config, history_awards=history
        )
        table = stack_table(suite.results, {"fixture": (start, end)}).set_index("policy")
        assert table.loc["fixed_100", "afrr_capacity"] > 0.0
        assert table.loc["fixed_0", "afrr_capacity"] == 0.0
        assert np.isfinite(table["total"]).all()
        # capacity and aFRR energy do not depend on the imbalance mechanism, only implicit energy does
        totals[mechanism] = table
    assert totals["current"].loc["fixed_100", "afrr_capacity"] == pytest.approx(
        totals["new2026"].loc["fixed_100", "afrr_capacity"]
    )
    assert totals["current"].loc["fixed_100", "afrr_energy"] == pytest.approx(
        totals["new2026"].loc["fixed_100", "afrr_energy"]
    )


@pytest.mark.slow
def test_reserve_suite_on_one_month_of_public_data() -> None:
    start, end = "2025-03-01T00:00:00Z", "2025-04-01T00:00:00Z"
    columns = ["price_cum_eur_mwh", "si_cum_mw", "afrr_up_mw", "afrr_down_mw"]
    market = MarketData.from_public(
        start,
        end,
        include_new_formula=False,
        include_si_forecast=False,
        include_day_ahead=False,
        minute_columns=columns,
    )
    history = CapacityAwards.from_public("2025-02-01T00:00:00Z", end)
    awards = history.aligned_to(DeliveryBlocks.regular(market.start_utc, market.end_utc))
    suite = run_policy_suite(
        market, awards, make_inner=ThresholdStrategy, history_awards=history, config=ReserveStudyConfig()
    )
    table = stack_table(suite.results, {"march": (start, end)}).set_index("policy")
    assert table.loc["fixed_100", "afrr_capacity"] > 0.0
    assert table.loc["fixed_0", "implicit_balancing"] > 0.0
    assert (table["shortfall_mwh_per_mw_year"] >= 0.0).all()
