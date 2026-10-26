"""Two-stage DE-LU backtest on SYNTHETIC prices: bounds, physics, causality (perturbation), chunking, plug-ins."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.markets.intraday import (
    DEFAULT_STRATEGIES,
    STRATEGIES,
    FrameIntradaySource,
    IdAepIndexSource,
    IntradayConfig,
    IntradayMarket,
    IntradayPriceSource,
    SyntheticIntradaySource,
    TradingCosts,
    audit_causality,
    run_day,
    run_intraday_backtest,
    run_parallel,
    schedules_to_frame,
    summarize_daily,
    synthetic_market,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "public"


@pytest.fixture(scope="module")
def market() -> IntradayMarket:
    # 60 history days + evaluation days in the hourly era and after the 15-minute MTU switch
    return synthetic_market("2025-08-01", 66, hourly_until="2025-10-01", seed=11)


@pytest.fixture(scope="module")
def result(market: IntradayMarket) -> pd.DataFrame:
    days = market.days[-6:]
    return run_intraday_backtest(market, IntradayConfig(), days=days).daily


def test_every_strategy_runs_and_oracles_bound_the_tradable_strategies(result: pd.DataFrame) -> None:
    assert set(result["strategy"]) == set(DEFAULT_STRATEGIES)
    pnl = result.groupby("strategy")["pnl_eur"].sum()
    for name in ("da_only", "da_id_reopt_da", "da_id_reopt_profile", "da_id_rolling", "oracle_id", "oracle_da"):
        assert pnl["oracle_da_id"] >= pnl[name] - 1e-6, name
    for name in ("da_id_reopt_da", "da_id_reopt_profile", "da_id_rolling"):
        assert pnl["oracle_id"] >= pnl[name] - 1e-6, name
    assert pnl["oracle_da"] >= pnl["da_only"] - 1e-6
    assert (result["soc_violations"] == 0).all()
    assert (result["lp_fallbacks"] == 0).all()
    assert (result.loc[result["strategy"] == "da_only", ["id_sold_mwh", "id_bought_mwh"]] == 0).all().all()


def test_pnl_components_add_up(result: pd.DataFrame) -> None:
    rebuilt = (
        result["da_revenue_eur"]
        - result["da_fees_eur"]
        + result["id_revenue_eur"]
        - result["id_fees_eur"]
        - result["slippage_eur"]
        - result["degradation_eur"]
    )
    assert np.allclose(rebuilt, result["pnl_eur"])
    costs = TradingCosts()
    traded = result["id_sold_mwh"] + result["id_bought_mwh"]
    assert np.allclose(result["slippage_eur"], traded * costs.slippage_eur_mwh)
    assert np.allclose(result["id_fees_eur"], traded * costs.id_fee_eur_mwh)


def test_summary_scales_to_eur_per_mw_year(result: pd.DataFrame) -> None:
    config = IntradayConfig()
    summary = summarize_daily(result, config)
    days = result["day"].nunique()
    expected = result[result["strategy"] == "da_id_rolling"]["pnl_eur"].sum() * 365 / days
    assert summary.loc["da_id_rolling", "pnl_eur_per_mw_year"] == pytest.approx(expected)
    assert summary.loc["da_only", "uplift_vs_da_only_eur_per_mw_year"] == 0.0
    assert summary.loc["oracle_da_id", "capture_vs_oracle_da_id"] == pytest.approx(1.0)
    assert list(summary.index) == [s for s in STRATEGIES if s in summary.index]


def test_hourly_products_hold_one_day_ahead_position_per_hour(market: IntradayMarket) -> None:
    day = date(2025, 9, 28)
    _, schedules = run_day(market, day, IntradayConfig(), ["da_only", "da_id_rolling"])
    blocks = market.da_blocks(day)
    position = schedules["da_only"].position_mw
    for block in np.unique(blocks):
        assert np.ptp(position[blocks == block]) < 1e-9
    # intraday re-trading reshapes inside the hour (quarter-hour products)
    rolling = schedules["da_id_rolling"]
    physical = rolling.discharge_mw - rolling.charge_mw
    assert np.allclose(physical, rolling.position_mw + rolling.intraday_net_mw, atol=1e-7)
    after = market.da_blocks(date(2025, 10, 2))
    assert len(np.unique(after)) == len(after), "15-minute products after the MTU switch"


@pytest.mark.parametrize("cut", [12, 47, 80])
def test_rolling_strategy_passes_the_perturbation_test(market: IntradayMarket, cut: int) -> None:
    config = IntradayConfig()
    record = audit_causality(market, market.days[-3], "da_id_rolling", config, cut=cut, seed=cut)
    assert record["passed"] and record["max_abs_change_protected_mw"] == 0.0
    assert record["protected_products"] == cut + config.timing.visible_offset_qh


def test_static_and_day_ahead_strategies_ignore_the_whole_delivery_day(market: IntradayMarket) -> None:
    for strategy in ("da_only", "da_id_reopt_da", "da_id_reopt_profile"):
        record = audit_causality(market, market.days[-2], strategy, IntradayConfig(), cut=0, seed=3)
        assert record["passed"] and record["protected_products"] == 96, strategy


def test_oracles_fail_the_perturbation_test_by_design(market: IntradayMarket) -> None:
    hits = 0
    for seed, day in enumerate(market.days[-4:]):
        record = audit_causality(market, day, "oracle_id", IntradayConfig(), cut=60, seed=seed)
        hits += int(record["lookahead_detected"])
    assert hits >= 3


def test_perturbation_has_power_on_later_products(market: IntradayMarket) -> None:
    record = audit_causality(market, market.days[-3], "da_id_rolling", IntradayConfig(), cut=20, seed=5)
    assert record["max_abs_change_after_mw"] > 0.0, "decisions after the visibility horizon must react to prices"


def test_gate_closure_changes_the_information_offset() -> None:
    assert IntradayConfig().timing.visible_offset_qh == 3
    assert IntradayConfig().with_timing(gate_closure_minutes=5).timing.visible_offset_qh == 2
    assert IntradayConfig().with_timing(gate_closure_minutes=60).timing.visible_offset_qh == 5
    assert IntradayConfig().with_timing(visibility_lag_minutes=15).timing.visible_offset_qh == 4


def test_chunked_and_parallel_runs_match_the_sequential_run(market: IntradayMarket, result: pd.DataFrame) -> None:
    days = market.days[-6:]
    strategies = ("da_only", "da_id_rolling")
    sequential = result[result["strategy"].isin(strategies)].sort_values(["day", "strategy"]).reset_index(drop=True)
    chunked = run_parallel(market, IntradayConfig(), days, strategies=strategies, workers=1)
    assert np.allclose(chunked["pnl_eur"], sequential["pnl_eur"])
    parallel = run_parallel(market, IntradayConfig(), days, strategies=strategies, workers=2)
    assert np.allclose(parallel["pnl_eur"], sequential["pnl_eur"])
    assert run_parallel(market, IntradayConfig(), [], workers=1).empty


def test_undefined_index_settles_at_the_day_ahead_price(market: IntradayMarket) -> None:
    day = market.days[-1]
    config = IntradayConfig()
    _, schedules = run_day(market, day, config, ["da_id_reopt_da"])
    traded = np.flatnonzero(schedules["da_id_reopt_da"].sell_mw + schedules["da_id_reopt_da"].buy_mw > 1e-6)
    assert traded.size > 0
    lo, _ = market.day_bounds(day)
    holes = market.id_price.copy()
    holes[lo + traded[:3]] = np.nan
    records, schedules2 = run_day(market.with_id_prices(holes), day, config, ["da_id_reopt_da"])
    assert records[0]["fallback_trades"] == 3
    assert np.allclose(schedules2["da_id_reopt_da"].sell_mw, schedules["da_id_reopt_da"].sell_mw)
    frame = schedules_to_frame(market.with_id_prices(holes), day, schedules2, config)
    exec_price = frame["id_exec_price_eur_mwh"].to_numpy()
    assert np.allclose(exec_price[traded[:3]], market.da_price[lo + traded[:3]])


def test_oracle_value_falls_as_trading_costs_rise(market: IntradayMarket) -> None:
    days = market.days[-2:]
    cheap = run_intraday_backtest(
        market, IntradayConfig().with_costs(slippage_eur_mwh=0.0), days=days, strategies=["oracle_da_id"]
    )
    dear = run_intraday_backtest(
        market, IntradayConfig().with_costs(slippage_eur_mwh=5.0), days=days, strategies=["oracle_da_id"]
    )
    assert cheap.daily["pnl_eur"].sum() >= dear.daily["pnl_eur"].sum()
    with pytest.raises(ValueError):
        TradingCosts(slippage_eur_mwh=-1.0)
    with pytest.raises(ValueError, match="unknown strategies"):
        run_intraday_backtest(market, IntradayConfig(), days=days, strategies=["bogus"])


def test_price_sources_are_pluggable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EUROFLEX_DATA_DIR", str(FIXTURES))
    from euroflex_bess_lab.public_data import load_day_ahead_prices

    da = load_day_ahead_prices("DE-LU", "2026-03-02T23:00Z", "2026-03-04T23:00Z", quarter_hourly=True)
    synthetic = SyntheticIntradaySource(
        pd.Series(da["price_eur_mwh"].to_numpy(), index=pd.DatetimeIndex(da["timestamp_utc"]))
    )
    licensed = FrameIntradaySource(
        pd.DataFrame({"timestamp_utc": da["timestamp_utc"], "vwap": da["price_eur_mwh"].to_numpy() + 3.0}),
        price_column="vwap",
        name="licensed_epex_vwap",
    )
    for source in (synthetic, licensed):
        assert isinstance(source, IntradayPriceSource)
        market = IntradayMarket.from_public("2026-03-04", "2026-03-04", source=source, history_days=1)
        assert market.days == [date(2026, 3, 3), date(2026, 3, 4)]
        assert market.id_source == source.name
        out = run_intraday_backtest(
            market, IntradayConfig(), days=[date(2026, 3, 4)], strategies=["da_only", "da_id_rolling"]
        )
        assert len(out.daily) == 2 and out.meta["id_source"] == source.name
    assert IdAepIndexSource().redistributable is False
    with pytest.raises(KeyError):
        FrameIntradaySource(pd.DataFrame({"x": [1]})).load(
            pd.Timestamp("2026-01-01", tz="UTC"), pd.Timestamp("2026-01-02", tz="UTC")
        )


def test_market_construction_skips_incomplete_day_ahead_days() -> None:
    from euroflex_bess_lab.markets.intraday import synthetic_day_ahead

    da = synthetic_day_ahead("2026-05-01", 4, hourly_until=None)
    da = da.drop(index=range(100, 110))  # punch a hole into the second local day
    intraday = pd.Series(da["price_eur_mwh"].to_numpy(), index=pd.DatetimeIndex(da["timestamp_utc"]))
    market = IntradayMarket.from_frames(da, intraday)
    assert date(2026, 5, 2) not in market.days and len(market.days) == 3
    sliced = market.slice_days(market.days[1], market.days[2])
    assert sliced.days == market.days[1:] and sliced.n_qh == 192
    with pytest.raises(ValueError, match="complete"):
        IntradayMarket.from_frames(da.iloc[:50], intraday)
