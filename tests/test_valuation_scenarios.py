from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.valuation import (
    BootstrapConfig,
    CapacityValueCurve,
    FleetPath,
    SaturationSpec,
    ScenarioConfig,
    ScenarioSet,
    SkillDecay,
    as_panel,
    daily_panel,
    derive_regime_from_overlap,
    exceedance_levels,
    generate_scenarios,
    load_capacity_value_curve,
    placeholder_capacity_value_curve,
)


def history(values: np.ndarray | float, start: str = "2024-01-01", days: int = 3 * 365 + 1) -> pd.DataFrame:
    dates = pd.date_range(start, periods=days, freq="D")
    data = np.broadcast_to(np.asarray(values, dtype=float), (days,)) if np.ndim(values) == 0 else values
    return pd.DataFrame({"date": dates, "pnl_eur_per_mw": data, "coverage": 1.0})


def seasonal_values(days: int = 3 * 365 + 1, start: str = "2024-01-01") -> np.ndarray:
    dates = pd.date_range(start, periods=days, freq="D")
    return 100.0 + 60.0 * np.cos(2.0 * np.pi * (dates.day_of_year.to_numpy() - 15) / 365.25)


@pytest.mark.parametrize("method", ["block", "stationary", "iid"])
def test_constant_history_gives_exact_calendar_sums(method: str) -> None:
    config = ScenarioConfig(n_paths=50, n_years=3, start_year=2027, bootstrap=BootstrapConfig(method=method))
    scenarios = generate_scenarios(history(10.0), config)
    expected = 10.0 * np.array([365, 366, 365])  # 2028 is a leap year
    assert scenarios.years == (2027, 2028, 2029)
    assert scenarios.days_in_year.tolist() == [365, 366, 365]
    assert np.allclose(scenarios.annual, expected[None, :])


def test_same_seed_reproduces_paths_and_other_seed_differs() -> None:
    base = history(seasonal_values() + np.random.default_rng(0).normal(0.0, 30.0, 3 * 365 + 1))
    config = ScenarioConfig(n_paths=100, n_years=4, seed=42)
    first = generate_scenarios(base, config)
    again = generate_scenarios(base, config)
    other = generate_scenarios(base, ScenarioConfig(n_paths=100, n_years=4, seed=43))
    assert np.array_equal(first.annual, again.annual)
    assert not np.array_equal(first.annual, other.annual)


def test_seasonal_window_preserves_seasonality_and_removes_seasonal_variance() -> None:
    base = history(seasonal_values())
    narrow = BootstrapConfig(method="iid", season_window_days=5, align_weekday=False)
    seasonal = generate_scenarios(base, ScenarioConfig(n_paths=300, n_years=1, bootstrap=narrow))
    off = BootstrapConfig(method="iid", season_window_days=None, align_weekday=False)
    unconditioned = generate_scenarios(base, ScenarioConfig(n_paths=300, n_years=1, bootstrap=off))
    target = float(seasonal_values(365, "2027-01-01").sum())
    assert seasonal.annual.mean() == pytest.approx(target, rel=0.005)
    assert unconditioned.annual.mean() == pytest.approx(target, rel=0.02)
    assert seasonal.annual.std() < 0.2 * unconditioned.annual.std()
    assert seasonal.diagnostics["regimes"]["base"]["seasonal_fallback_share"] == 0.0
    # weekday alignment within +/- 5 days leaves fewer than 8 candidates in 3 years: the window is widened
    aligned = BootstrapConfig(method="iid", season_window_days=5, align_weekday=True)
    widened = generate_scenarios(base, ScenarioConfig(n_paths=10, n_years=1, bootstrap=aligned))
    assert widened.diagnostics["regimes"]["base"]["seasonal_fallback_share"] == 1.0


def test_weekday_aligned_weekly_blocks_keep_the_weekday_pattern_exactly() -> None:
    dates = pd.date_range("2024-01-01", periods=3 * 365 + 1, freq="D")
    weekday_only = history((dates.day_of_week < 5).astype(float))
    config = ScenarioConfig(n_paths=40, n_years=1, start_year=2027, bootstrap=BootstrapConfig(block_days=7))
    scenarios = generate_scenarios(weekday_only, config)
    calendar = pd.date_range("2027-01-01", "2027-12-31", freq="D")
    assert np.allclose(scenarios.annual, float((calendar.day_of_week < 5).sum()))


def test_regime_mixing_weights_and_persistence() -> None:
    regimes = {"current": history(100.0), "new2026": history(200.0)}
    sticky = generate_scenarios(
        regimes, ScenarioConfig(n_paths=4000, n_years=5), weights={"current": 0.25, "new2026": 0.75}
    )
    per_path = sticky.regime_index
    assert np.all(per_path == per_path[:, :1])
    assert sticky.regime_shares()["new2026"] == pytest.approx(0.75, abs=0.03)
    assert sticky.annual.mean() / 365.2 == pytest.approx(0.25 * 100 + 0.75 * 200, rel=0.02)

    loose = generate_scenarios(
        regimes,
        ScenarioConfig(n_paths=4000, n_years=5, regime_persistence=0.0),
        weights={"current": 0.25, "new2026": 0.75},
    )
    assert np.any(loose.regime_index != loose.regime_index[:, :1])
    assert np.mean(loose.regime_index == 1, axis=0) == pytest.approx(np.full(5, 0.75), abs=0.03)

    schedule = generate_scenarios(
        regimes,
        ScenarioConfig(n_paths=200, n_years=3, regime_persistence=0.0),
        weights={"current": [1.0, 0.0, 0.0], "new2026": [0.0, 1.0, 1.0]},
    )
    assert np.all(schedule.regime_index[:, 0] == 0) and np.all(schedule.regime_index[:, 1:] == 1)
    with pytest.raises(KeyError):
        generate_scenarios(regimes, ScenarioConfig(n_paths=10, n_years=1), weights={"other": 1.0})


def test_saturation_multiplier_follows_the_capacity_value_curve() -> None:
    curve = placeholder_capacity_value_curve(half_value_mw=1500.0)
    assert curve.placeholder
    assert curve.multiplier(1500.0, 0.0) == pytest.approx(0.5)
    spec = SaturationSpec(curve=curve, fleet=FleetPath.constant(1500.0), reference_fleet_mw=0.0)
    scenarios = generate_scenarios(history(10.0), ScenarioConfig(n_paths=20, n_years=2, saturation=spec))
    assert np.allclose(scenarios.saturation_multiplier, 0.5, atol=1e-3)
    assert np.allclose(scenarios.annual, 0.5 * 10.0 * scenarios.days_in_year[None, :], rtol=1e-3)
    assert np.allclose(scenarios.base_series["asset"], 10.0 * scenarios.days_in_year[None, :])

    path = FleetPath.from_points({2027: 900.0, 2030: 2100.0})
    assert path.base([2026, 2027, 2028, 2030, 2033]).tolist() == [900.0, 900.0, 1300.0, 2100.0, 2100.0]
    noisy = FleetPath.from_points({2027: 1000.0}, path_log_sigma=0.3, year_log_sigma=0.1)
    draws = noisy.sample([2027, 2028], 20000, np.random.default_rng(1))
    assert draws.mean() == pytest.approx(1000.0, rel=0.02)


def test_mechanism_specific_curves_apply_per_regime() -> None:
    flat = CapacityValueCurve(fleet_mw=(0.0, 1000.0), value_per_mw=(1.0, 1.0), name="flat")
    steep = CapacityValueCurve(fleet_mw=(0.0, 1000.0), value_per_mw=(1.0, 0.5), name="steep")
    spec = SaturationSpec(curve={"current": flat, "new2026": steep}, fleet=FleetPath.constant(1000.0))
    regimes = {"current": history(10.0), "new2026": history(10.0)}
    scenarios = generate_scenarios(regimes, ScenarioConfig(n_paths=400, n_years=1, saturation=spec))
    multipliers = scenarios.saturation_multiplier[:, 0]
    assert np.allclose(multipliers[scenarios.regime_index[:, 0] == 0], 1.0)
    assert np.allclose(multipliers[scenarios.regime_index[:, 0] == 1], 0.5)


def test_capacity_value_curve_io_validation_and_extrapolation(tmp_path) -> None:
    frame = pd.DataFrame(
        {
            "mechanism": ["current"] * 3 + ["new2026"] * 3,
            "fleet_mw": [0.0, 1000.0, 2000.0] * 2,
            "value_eur_per_mw_year": [200e3, 150e3, 100e3, 180e3, 120e3, 90e3],
        }
    )
    path = tmp_path / "curve.csv"
    frame.to_csv(path, index=False)
    with pytest.raises(ValueError, match="several mechanisms"):
        load_capacity_value_curve(path)
    curve = load_capacity_value_curve(path, mechanism="new2026")
    assert curve.units == "EUR/MW/year"
    assert curve.value(500.0) == pytest.approx(150e3)
    assert curve.value(3000.0) == pytest.approx(60e3)  # linear extrapolation of the last segment
    flat = CapacityValueCurve(curve.fleet_mw, curve.value_per_mw, extrapolation="flat")
    assert flat.value(3000.0) == pytest.approx(90e3)
    assert curve.value(10_000.0) == 0.0
    assert load_capacity_value_curve("placeholder").placeholder
    with pytest.raises(ValueError):
        CapacityValueCurve(fleet_mw=(0.0, 0.0), value_per_mw=(1.0, 1.0))
    with pytest.raises(ValueError):
        CapacityValueCurve(fleet_mw=(0.0, 1.0), value_per_mw=(1.0, -1.0))


def test_skill_fade_availability_and_level_shock_layers() -> None:
    skill = SkillDecay(skill_share=0.4, decay_per_year=0.1)
    assert skill.multiplier(3) == pytest.approx([1.0, 0.6 + 0.4 * 0.9, 0.6 + 0.4 * 0.81])
    config = ScenarioConfig(
        n_paths=4000,
        n_years=3,
        skill=skill,
        capacity_fade_per_year=0.02,
        availability_factor=0.95,
        level_log_sigma=0.2,
        level_persistence=0.8,
    )
    scenarios = generate_scenarios(history(10.0), config)
    expected = 10.0 * scenarios.days_in_year * skill.multiplier(3) * 0.98 ** np.arange(3) * 0.95
    assert scenarios.annual.mean(axis=0) == pytest.approx(expected, rel=0.02)
    level = np.log(scenarios.level_multiplier)
    assert np.corrcoef(level[:, 0], level[:, 1])[0, 1] == pytest.approx(0.8, abs=0.03)
    assert level.std() == pytest.approx(0.2, rel=0.05)
    with pytest.raises(ValueError):
        ScenarioConfig(level_persistence=1.0)


def test_short_regime_history_uses_seasonal_fallback_and_overlap_derivation() -> None:
    base = history(100.0, start="2024-07-01", days=730)
    short = history(80.0, start="2026-01-01", days=150)
    scenarios = generate_scenarios(short, ScenarioConfig(n_paths=20, n_years=1))
    assert scenarios.diagnostics["regimes"]["base"]["seasonal_fallback_share"] > 0.3
    assert np.allclose(scenarios.annual, 80.0 * 365)

    derived, info = derive_regime_from_overlap(base, short, method="ratio")
    assert info["ratio"] == pytest.approx(0.8)
    assert info["overlap_days"] == 150
    assert np.allclose(derived.to_numpy(), 80.0)
    assert len(derived) == 730
    delta, info = derive_regime_from_overlap(base, short, method="delta", prefer_observed=False)
    assert info["delta_per_day"] == pytest.approx(-20.0)
    assert np.allclose(delta.to_numpy(), 80.0)
    with pytest.raises(ValueError, match="overlapping"):
        derive_regime_from_overlap(base, history(80.0, start="2030-01-01", days=40))


def test_joint_series_share_the_same_resampled_days() -> None:
    noise = np.random.default_rng(3).normal(0.0, 50.0, 3 * 365 + 1)
    panel = daily_panel(
        {"asset": history(100.0 + noise), "index": history(50.0 + 0.5 * noise)}, value_column="pnl_eur_per_mw"
    )
    scenarios = generate_scenarios(panel, ScenarioConfig(n_paths=300, n_years=2))
    asset, index = scenarios.get("asset"), scenarios.get("index")
    assert np.allclose(index, 0.5 * asset)
    assert scenarios.primary == "asset"
    frame = scenarios.to_frame()
    assert len(frame) == 600 and {"asset", "index", "regime", "level_multiplier"} <= set(frame.columns)
    with pytest.raises(KeyError):
        scenarios.get("missing")


def test_from_annual_summary_and_exceedance_convention() -> None:
    annual = np.random.default_rng(5).normal(100.0, 20.0, size=(5000, 2))
    scenarios = ScenarioSet.from_annual(annual, start_year=2030)
    table = scenarios.annual_table()
    assert table["year"].tolist() == [2030, 2031]
    assert np.all(table["p90"] < table["p50"]) and np.all(table["p50"] < table["p10"])
    levels = exceedance_levels(annual[:, 0])
    assert levels["p90"] == pytest.approx(np.quantile(annual[:, 0], 0.1))
    summary = scenarios.summary()
    assert summary["n_paths"] == 5000 and "exceedance" in summary["percentile_convention"]
    assert as_panel(history(1.0)).shape == (3 * 365 + 1, 1)
