from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import (
    BatteryParams,
    CurrentAlphaMechanism,
    IdleStrategy,
    LinearImpact,
    MarketData,
    NewWeightedMechanism2026,
    OracleQHStrategy,
    PublicationModel,
    ThresholdStrategy,
    apply_power,
    check_no_lookahead,
    simulate,
    validate_mechanism,
)
from euroflex_bess_lab.realtime.strategies import BaseStrategy, solve_qh_arbitrage

START = "2026-03-02T00:00:00Z"


def _synthetic_market(days: int = 2, seed: int = 3) -> MarketData:
    rng = np.random.default_rng(seed)
    minutes = pd.date_range(START, periods=days * 1440, freq="1min", tz="UTC")
    qh_times = pd.date_range(START, periods=days * 96, freq="15min", tz="UTC")
    si_qh = rng.normal(0.0, 200.0, size=len(qh_times))
    mip = 80.0 + np.clip(-si_qh, 0, None) * 0.4
    mdp = 40.0 - np.clip(si_qh, 0, None) * 0.3
    alpha = np.where(np.abs(si_qh) > 150, 10.0, 0.0)
    price = CurrentAlphaMechanism.price_given_si(si_qh, mip=mip, mdp=mdp, alpha=alpha)
    qh = pd.DataFrame(
        {
            "timestamp_utc": qh_times,
            "si_mw": si_qh,
            "mip_eur_mwh": mip,
            "mdp_eur_mwh": mdp,
            "alpha_eur_mwh": alpha,
            "imbalance_price_eur_mwh": price,
        }
    )
    # running QH-to-date estimates converge to the final value at the last minute
    progress = (np.arange(len(minutes)) % 15 + 1) / 15.0
    qh_of_minute = np.arange(len(minutes)) // 15
    noise = rng.normal(0.0, 30.0, size=len(minutes)) * (1 - progress)
    minute = pd.DataFrame(
        {
            "timestamp_utc": minutes,
            "si_cum_mw": si_qh[qh_of_minute] + 3 * noise,
            "price_cum_eur_mwh": price[qh_of_minute] + noise,
        }
    )
    da = pd.DataFrame(
        {
            "timestamp_utc": pd.date_range(START, periods=days * 24, freq="1h", tz="UTC"),
            "resolution_minutes": 60,
            "price_eur_mwh": 60.0 + 20 * np.sin(np.arange(days * 24) / 24 * 2 * np.pi),
        }
    )
    return MarketData.from_frames(
        start=START,
        end=minutes[-1] + pd.Timedelta(minutes=1),
        minute_frames={"": minute},
        qh_frames={"": qh},
        da_frame=da,
    )


BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)


def test_battery_respects_soc_limits_and_efficiency() -> None:
    battery = BatteryParams(power_mw=10.0, energy_mwh=1.0, initial_soc_frac=0.06)
    power, soc = apply_power(10.0, battery.initial_soc_mwh, battery)
    assert power == pytest.approx((0.06 - 0.05) * 0.95 * 60)
    assert soc == pytest.approx(battery.soc_min_mwh)
    power, soc = apply_power(-1.0, 0.5, BatteryParams(power_mw=1.0, energy_mwh=1.0))
    assert power == -1.0
    assert soc == pytest.approx(0.5 + 0.95 / 60)


def test_current_mechanism_sign_convention_and_validation() -> None:
    frame = pd.DataFrame(
        {
            "si_mw": [-300.0, 200.0, 0.0],
            "mip_eur_mwh": [120.0, 90.0, 70.0],
            "mdp_eur_mwh": [30.0, -5.0, 20.0],
            "alpha_eur_mwh": [15.0, 4.0, 0.0],
        }
    )
    prices = CurrentAlphaMechanism().price_from_components(frame)
    assert prices.tolist() == [135.0, -9.0, 20.0]
    frame["imbalance_price_eur_mwh"] = prices
    assert validate_mechanism(CurrentAlphaMechanism(), frame).match_rate == 1.0


def test_new_mechanism_incentive_rule_and_reconstruction() -> None:
    # rows copied from Elia ODS212 (simulated new formula, Feb-Jun 2026)
    frame = pd.DataFrame(
        {
            "si_mw": [318.818648, 368.487160, -332.751926, 44.852982],
            "main_up_eur_mwh": [73.588536, 69.95, 1784.589363, 1.282167],
            "main_down_eur_mwh": [58.390630, -175.923180, 83.23, 48.573903],
            "incentive_eur_mwh": [1000.0, -519.370339, 974.355001, np.nan],
            "imbalance_price_eur_mwh": [58.390630, -519.370339, 1784.589363, 48.573903],
        }
    )
    assert validate_mechanism(NewWeightedMechanism2026(), frame).match_rate == 1.0
    ingredients = pd.DataFrame(
        {
            "si_mw": [-100.0, 100.0],
            "weight_afrr": [0.25, 1.0],
            "weight_mfrr": [0.75, 0.0],
            "afrr_price_up_eur_mwh": [100.0, 50.0],
            "mfrr_price_up_eur_mwh": [200.0, np.nan],
            "afrr_price_down_eur_mwh": [10.0, -20.0],
            "mfrr_price_down_eur_mwh": [np.nan, np.nan],
        }
    )
    up, down = NewWeightedMechanism2026.reconstruct_main(ingredients)
    assert up.tolist() == [175.0, 50.0]
    assert down.tolist() == [2.5, -20.0]


def test_market_data_alignment_and_day_ahead_visibility() -> None:
    market = _synthetic_market(days=2)
    assert market.n_minutes == 2 * 1440 and market.n_qh == 2 * 96
    assert set(market.settlement_prices) == {"current"}
    # hourly day-ahead expanded to four identical quarter-hours
    assert np.allclose(market.da_price[:4], market.da_price[0])
    # local day 2026-03-03 (CET) starts at 23:00 UTC = QH 92; its prices are public from 2026-03-02 13:00 CET
    # = 12:00 UTC = minute 720. QH 91 belongs to 2026-03-02, published before the grid starts.
    assert int(market.da_visible_from_minute[92]) == 720
    assert int(market.da_visible_from_minute[96]) == 720
    assert int(market.da_visible_from_minute[91]) == 0
    sliced = market.slice("2026-03-02T12:00:00Z", "2026-03-03T00:00:00Z")
    assert sliced.n_minutes == 720
    assert int(sliced.da_visible_from_minute[0]) == 0


class _ConstantStrategy(BaseStrategy):
    def __init__(self, power: float) -> None:
        self.power = power
        self.name = "constant"

    def decide(self, ctx) -> float:  # type: ignore[no-untyped-def]
        return self.power if ctx.qh_index == 0 else 0.0


def test_settlement_pays_long_and_charges_short() -> None:
    market = _synthetic_market(days=1)
    price0 = float(market.settlement("current")[0])
    long_run = simulate(market, _ConstantStrategy(1.0), BATTERY)
    energy = float(long_run.qh["energy_mwh"].iloc[0])
    assert energy == pytest.approx(0.25)
    assert float(long_run.qh["revenue_eur"].iloc[0]) == pytest.approx(0.25 * price0)
    assert float(long_run.qh["degradation_eur"].iloc[0]) == pytest.approx(0.25 * BATTERY.degradation_eur_per_mwh)
    short_run = simulate(market, _ConstantStrategy(-1.0), BATTERY)
    assert float(short_run.qh["revenue_eur"].iloc[0]) == pytest.approx(-0.25 * price0)
    idle = simulate(market, IdleStrategy(), BATTERY)
    assert idle.pnl_eur == 0.0


def test_oracle_lp_buys_low_sells_high() -> None:
    battery = BatteryParams(
        power_mw=1.0,
        energy_mwh=1.0,
        initial_soc_frac=0.05,
        degradation_eur_per_mwh=0.0,
        charge_efficiency=1.0,
        discharge_efficiency=1.0,
    )
    prices = np.array([10.0, 10.0, 10.0, 10.0, 100.0, 100.0, 100.0, 100.0])
    power, soc = solve_qh_arbitrage(prices, battery, initial_soc_mwh=battery.initial_soc_mwh)
    assert np.all(power[:4] <= 1e-9) and np.all(power[4:] >= -1e-9)
    assert -power[:4].sum() * 0.25 == pytest.approx(battery.usable_energy_mwh, abs=1e-6)
    assert power[4:].sum() * 0.25 == pytest.approx(battery.usable_energy_mwh, abs=1e-6)
    assert soc[-1] == pytest.approx(battery.soc_min_mwh, abs=1e-6)


def test_oracle_dominates_threshold_and_threshold_beats_idle() -> None:
    market = _synthetic_market(days=2)
    oracle = simulate(market, OracleQHStrategy(window_days=1), BATTERY, record_minutes=False)
    rule = simulate(market, ThresholdStrategy(min_history_qh=10_000), BATTERY, record_minutes=False)
    assert oracle.pnl_eur > rule.pnl_eur > 0.0
    summary = rule.summary(oracle)
    assert 0.0 < summary["capture_vs_oracle"] < 1.0
    assert summary["cycles_per_day"] > 0.0


def test_lookahead_audit_passes_causal_strategy_and_catches_oracle() -> None:
    market = _synthetic_market(days=1)
    publication = PublicationModel(minute_lag=1, qh_final_lag_minutes=5)
    causal = check_no_lookahead(
        lambda: ThresholdStrategy(min_history_qh=4, lookback_qh=48),
        market,
        BATTERY,
        cut_minute=600,
        publication=publication,
    )
    assert causal.passed, causal
    cheat = check_no_lookahead(
        lambda: OracleQHStrategy(window_days=1), market, BATTERY, cut_minute=600, publication=publication
    )
    assert not cheat.passed


def test_linear_impact_lowers_price_when_fleet_goes_long() -> None:
    market = _synthetic_market(days=1)
    base = simulate(market, _ConstantStrategy(1.0), BATTERY)
    impacted = simulate(market, _ConstantStrategy(1.0), BATTERY, impact=LinearImpact(0.1), fleet_mw=100.0)
    assert float(impacted.qh["price_eur_mwh"].iloc[0]) == pytest.approx(float(base.qh["price_eur_mwh"].iloc[0]) - 10.0)
