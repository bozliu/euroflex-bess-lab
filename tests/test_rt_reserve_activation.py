from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import MarketData
from euroflex_bess_lab.realtime.reserve import (
    PICASSO_GO_LIVE_UTC,
    ActivationModel,
    CapacityAwards,
    DeliveryBlocks,
    activation_diagnostics,
    build_activation,
    reconstruct_minute_price,
    synthetic_reserve_inputs,
)
from euroflex_bess_lab.realtime.reserve.activation import bid_reference_price


def _market_with(volume_up: np.ndarray, volume_down: np.ndarray, price: np.ndarray, *, start: str) -> MarketData:
    n = len(volume_up)
    minutes = pd.date_range(start, periods=n, freq="1min", tz="UTC")
    qh_times = pd.date_range(start, periods=n // 15, freq="15min", tz="UTC")
    minute = pd.DataFrame({"timestamp_utc": minutes, "afrr_up_mw": volume_up, "afrr_down_mw": volume_down})
    qh = pd.DataFrame(
        {
            "timestamp_utc": qh_times,
            "imbalance_price_eur_mwh": np.full(len(qh_times), 50.0),
            "afrr_price_up_eur_mwh": price,
            "afrr_price_down_eur_mwh": price,
        }
    )
    hours = pd.date_range(start, periods=max(1, n // 60), freq="1h", tz="UTC")
    da = pd.DataFrame({"timestamp_utc": hours, "resolution_minutes": 60, "price_eur_mwh": 60.0})
    return MarketData.from_frames(
        start=start,
        end=minutes[-1] + pd.Timedelta(minutes=1),
        minute_frames={"": minute},
        qh_frames={"": qh},
        da_frame=da,
    )


def _awards(market: MarketData, contracted: float = 100.0) -> CapacityAwards:
    blocks = DeliveryBlocks.regular(market.start_utc, market.end_utc)
    ones = np.ones(blocks.n)
    return CapacityAwards(
        blocks=blocks,
        awarded_mw={"up": contracted * ones, "down": contracted * ones},
        vwap_eur_per_mw_h={"up": 10.0 * ones, "down": 5.0 * ones},
        marginal_eur_per_mw_h={"up": 20.0 * ones, "down": 8.0 * ones},
    )


def test_pro_rata_ratio_is_activated_over_contracted_and_clipped() -> None:
    up = np.array([0.0, 50.0, 100.0, 250.0] * 15)
    down = np.array([10.0, 0.0, -30.0, 0.0] * 15)  # sign of the published down volume is ignored
    price = np.array([80.0, 120.0, 90.0, 60.0])
    market = _market_with(up, down, price, start="2025-03-02T00:00:00Z")
    awards = _awards(market)
    series = build_activation(market, awards, ActivationModel(pay_as_bid_until_utc=None))
    assert np.allclose(series.ratio_up[:4], [0.0, 0.5, 1.0, 1.0])
    assert series.raw_ratio_up[3] == pytest.approx(2.5)
    assert np.allclose(series.ratio_down[:4], [0.1, 0.0, 0.3, 0.0])
    assert np.allclose(series.price_up[:15], 80.0) and np.allclose(series.price_up[15:30], 120.0)
    diagnostics = activation_diagnostics(market, awards, series)
    assert diagnostics["up"]["raw_ratio_above_one_share"] == pytest.approx(0.25)
    assert diagnostics["up"]["mean_ratio"] == pytest.approx(np.mean([0.0, 0.5, 1.0, 1.0]))


def test_merit_order_alternative_activates_fully_when_the_price_clears_the_bid() -> None:
    up = np.full(60, 5.0)
    down = np.full(60, 5.0)
    price = np.array([65.0, 75.0, 45.0, 55.0])  # bid = day-ahead 60 +/- markup 10
    market = _market_with(up, down, price, start="2025-03-02T00:00:00Z")
    series = build_activation(
        market, _awards(market), ActivationModel(kind="merit_order", pay_as_bid_until_utc=None, bid_markup_eur_mwh=10.0)
    )
    by_qh_up = series.ratio_up.reshape(4, 15)[:, 0]
    by_qh_down = series.ratio_down.reshape(4, 15)[:, 0]
    assert by_qh_up.tolist() == [0.0, 1.0, 0.0, 0.0]
    assert by_qh_down.tolist() == [0.0, 0.0, 1.0, 0.0]


def test_pay_as_bid_before_picasso_pays_the_own_bid() -> None:
    start = (PICASSO_GO_LIVE_UTC - pd.Timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    up = np.full(60, 20.0)
    price = np.full(4, 200.0)
    market = _market_with(up, up, price, start=start)
    series = build_activation(market, _awards(market), ActivationModel(bid_markup_eur_mwh=10.0))
    assert np.allclose(series.price_up[:30], 70.0) and np.allclose(series.price_down[:30], 50.0)  # DA 60 +/- 10
    assert np.allclose(series.price_up[30:], 200.0) and np.allclose(series.price_down[30:], 200.0)
    assert series.meta["pay_as_bid_minutes_up"] == 30


def test_bid_reference_falls_back_to_trailing_settlement_prices() -> None:
    base, _ = synthetic_reserve_inputs(days=2, seed=2)
    market = MarketData(**{**base.__dict__, "da_price": np.full(base.n_qh, np.nan)})
    reference, fallback = bid_reference_price(market)
    assert fallback == market.n_qh
    prices = market.settlement("current")
    k = 120
    assert reference[k] == pytest.approx(np.mean(prices[k - 2 - 95 : k - 1]))
    assert np.isnan(reference[:9]).all()


def test_minute_price_reconstruction_inverts_the_running_mean() -> None:
    rng = np.random.default_rng(1)
    instantaneous = rng.normal(100.0, 40.0, size=45)
    running = (np.cumsum(instantaneous.reshape(3, 15), axis=1) / np.arange(1, 16)).reshape(-1)
    assert np.allclose(reconstruct_minute_price(running), instantaneous)


def test_delivery_blocks_follow_local_time_and_dst() -> None:
    blocks = DeliveryBlocks.regular("2024-10-26T22:00:00Z", "2024-10-27T23:00:00Z")  # 27 Oct: 25-hour day
    local = blocks.local_start
    first = int(np.flatnonzero(blocks.delivery_day == pd.Timestamp("2024-10-27"))[0])
    assert local[first].hour == 0
    assert blocks.hours[first] == pytest.approx(5.0)  # 00-04 local contains the repeated hour
    assert blocks.block_of_day[first : first + 6].tolist() == [0, 1, 2, 3, 4, 5]
    gate = blocks.gate_closure_utc[first]
    assert gate == pd.Timestamp("2024-10-26T07:00:00Z")  # 09:00 CEST on D-1
    assert blocks.results_published_utc[first] == pd.Timestamp("2024-10-26T08:00:00Z")
    times = pd.DatetimeIndex([blocks.start_utc[first] + pd.Timedelta(hours=4, minutes=59)])
    assert blocks.index_of(times).tolist() == [first]
    assert blocks.index_of(pd.DatetimeIndex([pd.Timestamp("2020-01-01", tz="UTC")])).tolist() == [-1]


def test_awards_aggregate_duplicates_and_align_across_datetime_units() -> None:
    start = pd.Timestamp("2026-03-02T23:00:00Z")
    end = start + pd.Timedelta(hours=4)
    frame = pd.DataFrame(
        {
            "delivery_start_utc": [start, start, start],
            "delivery_end_utc": [end, end, end],
            "direction": ["up", "up", "down"],
            "awarded_mw": [60.0, 40.0, 100.0],
            "vwap_eur_per_mw_h": [10.0, 20.0, 5.0],
            "marginal_eur_per_mw_h": [12.0, 25.0, 6.0],
        }
    )
    frame["delivery_start_utc"] = frame["delivery_start_utc"].astype("datetime64[ns, UTC]")
    frame["delivery_end_utc"] = frame["delivery_end_utc"].astype("datetime64[ns, UTC]")
    awards = CapacityAwards.from_frame(frame)
    assert awards.awarded_mw["up"][0] == pytest.approx(100.0)
    assert awards.vwap_eur_per_mw_h["up"][0] == pytest.approx(14.0)
    assert awards.marginal_eur_per_mw_h["up"][0] == pytest.approx(25.0)
    target = DeliveryBlocks.regular("2026-03-02T23:00:00Z", "2026-03-03T07:00:00Z")  # microsecond resolution
    aligned = awards.aligned_to(target)
    assert aligned.meta["matched_blocks"] == 1
    assert aligned.vwap_eur_per_mw_h["down"][0] == pytest.approx(5.0)
    assert np.isnan(aligned.vwap_eur_per_mw_h["up"][1])
    assert aligned.available("up").tolist() == [True, False]
