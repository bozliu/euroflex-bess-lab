from __future__ import annotations

import pandas as pd
import pytest

from euroflex_bess_lab.backtesting.reasons import assign_reason_codes, assign_site_reason_codes
from euroflex_bess_lab.markets import DualPriceImbalanceSettlement, SinglePriceImbalanceSettlement
from euroflex_bess_lab.optimization.solver import (
    RiskPreference,
    solve_day_ahead_afrr_dispatch,
    solve_day_ahead_afrr_dispatch_scenario,
    solve_day_ahead_dispatch,
    solve_day_ahead_dispatch_scenario,
    solve_day_ahead_fcr_dispatch,
    solve_portfolio_day_ahead_afrr_dispatch,
    solve_portfolio_day_ahead_afrr_dispatch_scenario,
    solve_portfolio_day_ahead_dispatch,
    solve_portfolio_day_ahead_fcr_dispatch,
)
from euroflex_bess_lab.types import AssetSpec, AvailabilityMask, AvailabilityWindow, BatterySpec, SiteSpec


def make_price_frame(
    prices: list[float], *, timezone: str = "Europe/Brussels", market: str = "day_ahead"
) -> pd.DataFrame:
    index = pd.date_range("2025-01-01T00:00:00Z", periods=len(prices), freq="15min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp_utc": index,
            "timestamp_local": index.tz_convert(timezone),
            "market": market,
            "zone": "10YBE----------2",
            "resolution_minutes": 15,
            "resolution": "PT15M",
            "price_eur_per_mwh": prices,
            "currency": "EUR",
            "source": "unit_test",
            "value_kind": "actual",
            "is_actual": True,
            "is_forecast": False,
            "quality_status": "validated",
            "provenance": "source_pt15m",
        }
    )


def make_afrr_frames(
    *,
    capacity_up: list[float],
    capacity_down: list[float],
    activation_up: list[float],
    activation_down: list[float],
    ratio_up: list[float],
    ratio_down: list[float],
    timezone: str = "Europe/Brussels",
) -> dict[str, pd.DataFrame]:
    return {
        "capacity_up": make_price_frame(capacity_up, timezone=timezone, market="afrr_capacity_up"),
        "capacity_down": make_price_frame(capacity_down, timezone=timezone, market="afrr_capacity_down"),
        "activation_up": make_price_frame(activation_up, timezone=timezone, market="afrr_activation_price_up"),
        "activation_down": make_price_frame(activation_down, timezone=timezone, market="afrr_activation_price_down"),
        "ratio_up": make_price_frame(ratio_up, timezone=timezone, market="afrr_activation_ratio_up"),
        "ratio_down": make_price_frame(ratio_down, timezone=timezone, market="afrr_activation_ratio_down"),
    }


def make_scenario_snapshot(
    frame: pd.DataFrame,
    *,
    market: str,
    scenarios: dict[str, list[float]],
    weights: dict[str, float],
    decision_time: str = "2024-12-31T12:00:00Z",
) -> pd.DataFrame:
    resolution_minutes = int(frame["resolution_minutes"].iloc[0])
    rows: list[pd.DataFrame] = []
    for scenario_id, values in scenarios.items():
        rows.append(
            pd.DataFrame(
                {
                    "market": market,
                    "delivery_start_utc": frame["timestamp_utc"],
                    "delivery_end_utc": frame["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
                    "forecast_price_eur_per_mwh": values,
                    "issue_time_utc": pd.Timestamp(decision_time),
                    "available_from_utc": pd.Timestamp(decision_time),
                    "provider_name": "unit_test",
                    "scenario_id": scenario_id,
                    "scenario_weight": weights[scenario_id],
                }
            )
        )
    return pd.concat(rows, ignore_index=True)


def make_battery(**overrides: object) -> BatterySpec:
    payload = {
        "name": "test_battery",
        "power_mw": 1.0,
        "energy_mwh": 1.0,
        "initial_soc_mwh": 0.5,
        "terminal_soc_mwh": 0.5,
        "soc_min_mwh": 0.1,
        "soc_max_mwh": 0.9,
        "charge_efficiency": 0.95,
        "discharge_efficiency": 0.95,
        "connection_limit_mw": 1.0,
        "minimum_headroom_mwh": 0.0,
    }
    payload.update(overrides)
    return BatterySpec.model_validate(payload)


def make_asset(asset_id: str, **battery_overrides: object) -> AssetSpec:
    return AssetSpec.model_validate({"id": asset_id, "kind": "battery", "battery": make_battery(**battery_overrides)})


def test_battery_spec_rejects_infeasible_soc_after_headroom() -> None:
    with pytest.raises(ValueError):
        make_battery(initial_soc_mwh=0.15, minimum_headroom_mwh=0.1)


def test_day_ahead_solver_charges_on_negative_prices() -> None:
    frame = make_price_frame([-50.0, -20.0, 90.0, 120.0])
    result = solve_day_ahead_dispatch(frame, make_battery())
    assert result.dispatch.loc[0, "charge_mw"] > 0.0
    assert result.dispatch["discharge_mw"].max() > 0.0


def test_connection_limit_caps_dispatch() -> None:
    frame = make_price_frame([-40.0, -40.0, 120.0, 120.0])
    result = solve_day_ahead_dispatch(frame, make_battery(power_mw=2.0, connection_limit_mw=0.5))
    assert result.dispatch["charge_mw"].max() <= 0.500001
    assert result.dispatch["discharge_mw"].max() <= 0.500001


def test_availability_outage_zeroes_dispatch() -> None:
    outage = AvailabilityWindow(
        start=pd.Timestamp("2025-01-01T00:15:00Z"),
        end=pd.Timestamp("2025-01-01T00:45:00Z"),
        availability_factor=0.0,
    )
    frame = make_price_frame([-30.0, 110.0, 115.0, 120.0])
    result = solve_day_ahead_dispatch(frame, make_battery(availability=AvailabilityMask(outages=[outage])))
    blocked = result.dispatch[result.dispatch["timestamp_utc"].between(outage.start, outage.end, inclusive="left")]
    assert blocked["charge_mw"].eq(0.0).all()
    assert blocked["discharge_mw"].eq(0.0).all()


def test_imbalance_settlement_sign_is_positive_for_extra_export_at_positive_price() -> None:
    dispatch = pd.DataFrame(
        {
            "timestamp_utc": pd.date_range("2025-01-01T00:00:00Z", periods=2, freq="15min", tz="UTC"),
            "baseline_net_export_mw": [0.2, -0.1],
            "net_export_mw": [0.5, -0.1],
        }
    )
    prices = pd.DataFrame(
        {
            "timestamp_utc": dispatch["timestamp_utc"],
            "price_eur_per_mwh": [80.0, 50.0],
        }
    )
    aligned = dispatch.merge(
        prices[["timestamp_utc", "price_eur_per_mwh"]].rename(
            columns={"price_eur_per_mwh": "imbalance_actual_price_eur_per_mwh"}
        ),
        on="timestamp_utc",
        how="left",
    )
    aligned["imbalance_mw"] = aligned["net_export_mw"] - aligned["baseline_net_export_mw"]
    settlement = SinglePriceImbalanceSettlement().settle_imbalance(aligned, dt_hours=0.25)
    assert settlement.iloc[0] > 0.0
    assert settlement.iloc[1] == 0.0


def test_dual_price_settlement_uses_shortage_and_surplus_columns() -> None:
    dispatch = pd.DataFrame(
        {
            "imbalance_mw": [0.5, -0.5],
            "imbalance_surplus_price_eur_per_mwh": [60.0, 61.0],
            "imbalance_shortage_price_eur_per_mwh": [90.0, 91.0],
        }
    )
    settlement = DualPriceImbalanceSettlement().settle_imbalance(dispatch, dt_hours=0.25)
    assert settlement.iloc[0] == 0.5 * 60.0 * 0.25
    assert settlement.iloc[1] == -0.5 * 91.0 * 0.25


def test_reason_codes_cover_required_cases() -> None:
    battery = make_battery()
    dispatch = pd.DataFrame(
        {
            "timestamp_utc": pd.date_range("2025-01-01T00:00:00Z", periods=5, freq="15min", tz="UTC"),
            "timestamp_local": pd.date_range("2025-01-01T01:00:00+01:00", periods=5, freq="15min"),
            "charge_mw": [0.7, 0.0, 0.0, 0.0, 0.0],
            "discharge_mw": [0.0, 0.0, 0.7, 1.0, 0.0],
            "soc_mwh": [0.6, battery.effective_soc_min_mwh, 0.7, 0.7, 0.7],
            "net_export_mw": [-0.7, 0.0, 0.7, 1.0, 0.0],
            "baseline_net_export_mw": [0.0, 0.0, 0.0, 0.0, 0.0],
            "imbalance_mw": [0.0, 0.0, 0.7, 0.0, 0.0],
            "availability_factor": [1.0] * 5,
            "power_limit_mw": [1.0, 1.0, 1.0, 1.0, 1.0],
            "fcr_reserved_mw": [0.0] * 5,
        }
    )
    coded = assign_reason_codes(dispatch, battery, overlay=True)
    assert "charge_for_da_spread" in set(coded["reason_code"])
    assert "blocked_by_soc_limit" in set(coded["reason_code"])
    assert "discharge_for_imbalance_capture" in set(coded["reason_code"])
    assert "blocked_by_connection_limit" in set(coded["reason_code"])
    assert "idle_due_to_efficiency_or_degradation" in set(coded["reason_code"])


def test_fcr_reservation_reduces_charge_and_discharge_headroom() -> None:
    day_ahead = make_price_frame([30.0, 31.0, 32.0, 33.0])
    fcr = make_price_frame([100.0, 100.0, 100.0, 100.0], market="fcr_capacity")
    result = solve_day_ahead_fcr_dispatch(
        day_ahead_frame=day_ahead,
        fcr_capacity_frame=fcr,
        battery=make_battery(initial_soc_mwh=0.5, terminal_soc_mwh=0.5),
        reserve_sustain_duration_minutes=15,
    )
    dispatch = result.dispatch
    assert dispatch["fcr_reserved_mw"].gt(0.0).any()
    assert (dispatch["charge_mw"] + dispatch["fcr_reserved_mw"] <= dispatch["power_limit_mw"] + 1e-6).all()
    assert (dispatch["discharge_mw"] + dispatch["fcr_reserved_mw"] <= dispatch["power_limit_mw"] + 1e-6).all()


def test_fcr_soc_headroom_blocks_infeasible_reserve_commitment() -> None:
    day_ahead = make_price_frame([25.0, 25.0, 25.0, 25.0])
    fcr = make_price_frame([200.0, 200.0, 200.0, 200.0], market="fcr_capacity")
    low_soc_battery = make_battery(initial_soc_mwh=0.12, terminal_soc_mwh=0.12)
    result = solve_day_ahead_fcr_dispatch(
        day_ahead_frame=day_ahead,
        fcr_capacity_frame=fcr,
        battery=low_soc_battery,
        reserve_sustain_duration_minutes=15,
    )
    assert result.dispatch.loc[0, "fcr_reserved_mw"] <= 0.1


def test_high_fcr_prices_shift_flat_day_to_reserve() -> None:
    day_ahead = make_price_frame([40.0, 41.0, 40.5, 40.8])
    fcr = make_price_frame([120.0, 120.0, 120.0, 120.0], market="fcr_capacity")
    result = solve_day_ahead_fcr_dispatch(
        day_ahead_frame=day_ahead,
        fcr_capacity_frame=fcr,
        battery=make_battery(),
        reserve_sustain_duration_minutes=15,
    )
    dispatch = result.dispatch
    assert dispatch["fcr_reserved_mw"].mean() > dispatch["charge_mw"].mean()
    assert dispatch["fcr_reserved_mw"].mean() > dispatch["discharge_mw"].mean()


def test_low_fcr_prices_preserve_energy_arbitrage() -> None:
    day_ahead = make_price_frame([-40.0, -20.0, 100.0, 120.0])
    fcr = make_price_frame([1.0, 1.0, 1.0, 1.0], market="fcr_capacity")
    result = solve_day_ahead_fcr_dispatch(
        day_ahead_frame=day_ahead,
        fcr_capacity_frame=fcr,
        battery=make_battery(),
        reserve_sustain_duration_minutes=15,
    )
    dispatch = result.dispatch
    assert dispatch["charge_mw"].max() > 0.0
    assert dispatch["discharge_mw"].max() > 0.0
    assert dispatch["fcr_reserved_mw"].max() < 0.5


def test_degradation_still_suppresses_low_value_micro_cycling() -> None:
    day_ahead = make_price_frame([50.0, 51.0, 52.0, 53.0])
    fcr = make_price_frame([0.0, 0.0, 0.0, 0.0], market="fcr_capacity")
    result = solve_day_ahead_fcr_dispatch(
        day_ahead_frame=day_ahead,
        fcr_capacity_frame=fcr,
        battery=make_battery(),
        degradation_cost_eur_per_mwh=100.0,
        reserve_sustain_duration_minutes=15,
    )
    assert result.dispatch["charge_mw"].max() == pytest.approx(0.0)
    assert result.dispatch["discharge_mw"].max() == pytest.approx(0.0)


def test_scenario_downside_penalty_changes_energy_schedule() -> None:
    day_ahead = make_price_frame([-15.0, -10.0, 20.0, 25.0])
    snapshot = make_scenario_snapshot(
        day_ahead,
        market="day_ahead",
        scenarios={
            "upside": [-15.0, -10.0, 120.0, 140.0],
            "downside": [-15.0, -10.0, 8.0, 6.0],
        },
        weights={"upside": 0.55, "downside": 0.45},
    )
    expected_value = solve_day_ahead_dispatch_scenario(
        price_frame=day_ahead,
        price_snapshot=snapshot,
        battery=make_battery(),
        risk=RiskPreference(mode="expected_value", penalty_lambda=0.0, tail_alpha=None),
    )
    robust = solve_day_ahead_dispatch_scenario(
        price_frame=day_ahead,
        price_snapshot=snapshot,
        battery=make_battery(),
        risk=RiskPreference(mode="downside_penalty", penalty_lambda=4.0, tail_alpha=None),
    )
    expected_spread = max(expected_value.metadata["scenario_profit_eur"].values()) - min(
        expected_value.metadata["scenario_profit_eur"].values()
    )
    robust_spread = max(robust.metadata["scenario_profit_eur"].values()) - min(
        robust.metadata["scenario_profit_eur"].values()
    )
    assert robust_spread < expected_spread
    assert robust.metadata["expected_value_eur"] < expected_value.metadata["expected_value_eur"]
    assert (
        not robust.dispatch[["charge_mw", "discharge_mw"]]
        .round(6)
        .equals(expected_value.dispatch[["charge_mw", "discharge_mw"]].round(6))
    )


def test_scenario_afrr_up_prices_bias_upward_reserve() -> None:
    day_ahead = make_price_frame([40.0, 40.0, 40.0, 40.0])
    snapshots = {
        "capacity_up": make_scenario_snapshot(
            make_afrr_frames(
                capacity_up=[0.0] * 4,
                capacity_down=[0.0] * 4,
                activation_up=[0.0] * 4,
                activation_down=[0.0] * 4,
                ratio_up=[0.0] * 4,
                ratio_down=[0.0] * 4,
            )["capacity_up"],
            market="afrr_capacity_up",
            scenarios={"base": [110.0] * 4, "stress": [130.0] * 4},
            weights={"base": 0.6, "stress": 0.4},
        ),
        "capacity_down": make_scenario_snapshot(
            make_afrr_frames(
                capacity_up=[0.0] * 4,
                capacity_down=[0.0] * 4,
                activation_up=[0.0] * 4,
                activation_down=[0.0] * 4,
                ratio_up=[0.0] * 4,
                ratio_down=[0.0] * 4,
            )["capacity_down"],
            market="afrr_capacity_down",
            scenarios={"base": [0.0] * 4, "stress": [0.0] * 4},
            weights={"base": 0.6, "stress": 0.4},
        ),
        "activation_up": make_scenario_snapshot(
            make_afrr_frames(
                capacity_up=[0.0] * 4,
                capacity_down=[0.0] * 4,
                activation_up=[0.0] * 4,
                activation_down=[0.0] * 4,
                ratio_up=[0.0] * 4,
                ratio_down=[0.0] * 4,
            )["activation_up"],
            market="afrr_activation_price_up",
            scenarios={"base": [140.0] * 4, "stress": [180.0] * 4},
            weights={"base": 0.6, "stress": 0.4},
        ),
        "activation_down": make_scenario_snapshot(
            make_afrr_frames(
                capacity_up=[0.0] * 4,
                capacity_down=[0.0] * 4,
                activation_up=[0.0] * 4,
                activation_down=[0.0] * 4,
                ratio_up=[0.0] * 4,
                ratio_down=[0.0] * 4,
            )["activation_down"],
            market="afrr_activation_price_down",
            scenarios={"base": [-50.0] * 4, "stress": [-50.0] * 4},
            weights={"base": 0.6, "stress": 0.4},
        ),
        "ratio_up": make_scenario_snapshot(
            make_afrr_frames(
                capacity_up=[0.0] * 4,
                capacity_down=[0.0] * 4,
                activation_up=[0.0] * 4,
                activation_down=[0.0] * 4,
                ratio_up=[0.0] * 4,
                ratio_down=[0.0] * 4,
            )["ratio_up"],
            market="afrr_activation_ratio_up",
            scenarios={"base": [0.10] * 4, "stress": [0.10] * 4},
            weights={"base": 0.6, "stress": 0.4},
        ),
        "ratio_down": make_scenario_snapshot(
            make_afrr_frames(
                capacity_up=[0.0] * 4,
                capacity_down=[0.0] * 4,
                activation_up=[0.0] * 4,
                activation_down=[0.0] * 4,
                ratio_up=[0.0] * 4,
                ratio_down=[0.0] * 4,
            )["ratio_down"],
            market="afrr_activation_ratio_down",
            scenarios={"base": [0.0] * 4, "stress": [0.0] * 4},
            weights={"base": 0.6, "stress": 0.4},
        ),
    }
    result = solve_day_ahead_afrr_dispatch_scenario(
        day_ahead_frame=day_ahead,
        day_ahead_snapshot=make_scenario_snapshot(
            day_ahead,
            market="day_ahead",
            scenarios={"base": [40.0] * 4, "stress": [40.0] * 4},
            weights={"base": 0.6, "stress": 0.4},
        ),
        afrr_capacity_up_snapshot=snapshots["capacity_up"],
        afrr_capacity_down_snapshot=snapshots["capacity_down"],
        afrr_activation_price_up_snapshot=snapshots["activation_up"],
        afrr_activation_price_down_snapshot=snapshots["activation_down"],
        afrr_activation_ratio_up_snapshot=snapshots["ratio_up"],
        afrr_activation_ratio_down_snapshot=snapshots["ratio_down"],
        battery=make_battery(initial_soc_mwh=0.7, terminal_soc_mwh=0.5),
        risk=RiskPreference(mode="expected_value", penalty_lambda=0.0, tail_alpha=None),
        reserve_sustain_duration_minutes=15,
    )
    assert result.dispatch["afrr_up_reserved_mw"].mean() > result.dispatch["afrr_down_reserved_mw"].mean()


def test_portfolio_scenario_shared_poi_binds_for_afrr() -> None:
    day_ahead = make_price_frame([45.0, 45.0, 45.0, 45.0])
    site = SiteSpec.model_validate({"id": "site", "poi_import_limit_mw": 1.0, "poi_export_limit_mw": 1.0})
    assets = [make_asset("a1", power_mw=1.0), make_asset("a2", power_mw=1.0)]
    base_snapshot = make_scenario_snapshot(
        day_ahead,
        market="day_ahead",
        scenarios={"base": [45.0] * 4, "stress": [45.0] * 4},
        weights={"base": 0.5, "stress": 0.5},
    )
    high_up = {"base": [100.0] * 4, "stress": [120.0] * 4}
    tiny_down = {"base": [1.0] * 4, "stress": [1.0] * 4}
    result = solve_portfolio_day_ahead_afrr_dispatch_scenario(
        day_ahead_frame=day_ahead,
        day_ahead_snapshot=base_snapshot,
        afrr_capacity_up_snapshot=make_scenario_snapshot(
            make_afrr_frames(
                capacity_up=[0.0] * 4,
                capacity_down=[0.0] * 4,
                activation_up=[0.0] * 4,
                activation_down=[0.0] * 4,
                ratio_up=[0.0] * 4,
                ratio_down=[0.0] * 4,
            )["capacity_up"],
            market="afrr_capacity_up",
            scenarios=high_up,
            weights={"base": 0.5, "stress": 0.5},
        ),
        afrr_capacity_down_snapshot=make_scenario_snapshot(
            make_afrr_frames(
                capacity_up=[0.0] * 4,
                capacity_down=[0.0] * 4,
                activation_up=[0.0] * 4,
                activation_down=[0.0] * 4,
                ratio_up=[0.0] * 4,
                ratio_down=[0.0] * 4,
            )["capacity_down"],
            market="afrr_capacity_down",
            scenarios=tiny_down,
            weights={"base": 0.5, "stress": 0.5},
        ),
        afrr_activation_price_up_snapshot=make_scenario_snapshot(
            make_afrr_frames(
                capacity_up=[0.0] * 4,
                capacity_down=[0.0] * 4,
                activation_up=[0.0] * 4,
                activation_down=[0.0] * 4,
                ratio_up=[0.0] * 4,
                ratio_down=[0.0] * 4,
            )["activation_up"],
            market="afrr_activation_price_up",
            scenarios=high_up,
            weights={"base": 0.5, "stress": 0.5},
        ),
        afrr_activation_price_down_snapshot=make_scenario_snapshot(
            make_afrr_frames(
                capacity_up=[0.0] * 4,
                capacity_down=[0.0] * 4,
                activation_up=[0.0] * 4,
                activation_down=[0.0] * 4,
                ratio_up=[0.0] * 4,
                ratio_down=[0.0] * 4,
            )["activation_down"],
            market="afrr_activation_price_down",
            scenarios=tiny_down,
            weights={"base": 0.5, "stress": 0.5},
        ),
        afrr_activation_ratio_up_snapshot=make_scenario_snapshot(
            make_afrr_frames(
                capacity_up=[0.0] * 4,
                capacity_down=[0.0] * 4,
                activation_up=[0.0] * 4,
                activation_down=[0.0] * 4,
                ratio_up=[0.0] * 4,
                ratio_down=[0.0] * 4,
            )["ratio_up"],
            market="afrr_activation_ratio_up",
            scenarios={"base": [0.3] * 4, "stress": [0.4] * 4},
            weights={"base": 0.5, "stress": 0.5},
        ),
        afrr_activation_ratio_down_snapshot=make_scenario_snapshot(
            make_afrr_frames(
                capacity_up=[0.0] * 4,
                capacity_down=[0.0] * 4,
                activation_up=[0.0] * 4,
                activation_down=[0.0] * 4,
                ratio_up=[0.0] * 4,
                ratio_down=[0.0] * 4,
            )["ratio_down"],
            market="afrr_activation_ratio_down",
            scenarios={"base": [0.01] * 4, "stress": [0.01] * 4},
            weights={"base": 0.5, "stress": 0.5},
        ),
        site=site,
        assets=assets,
        risk=RiskPreference(mode="expected_value", penalty_lambda=0.0, tail_alpha=None),
        reserve_sustain_duration_minutes=15,
    )
    assert result.site_dispatch["reserved_capacity_mw"].max() <= 1.000001


def test_high_afrr_up_prices_shift_flat_day_to_upward_reserve() -> None:
    day_ahead = make_price_frame([40.0, 41.0, 40.5, 40.8])
    afrr = make_afrr_frames(
        capacity_up=[80.0, 80.0, 80.0, 80.0],
        capacity_down=[1.0, 1.0, 1.0, 1.0],
        activation_up=[160.0, 160.0, 160.0, 160.0],
        activation_down=[1.0, 1.0, 1.0, 1.0],
        ratio_up=[0.35, 0.35, 0.35, 0.35],
        ratio_down=[0.0, 0.0, 0.0, 0.0],
    )
    result = solve_day_ahead_afrr_dispatch(
        day_ahead_frame=day_ahead,
        afrr_capacity_up_frame=afrr["capacity_up"],
        afrr_capacity_down_frame=afrr["capacity_down"],
        afrr_activation_price_up_frame=afrr["activation_up"],
        afrr_activation_price_down_frame=afrr["activation_down"],
        afrr_activation_ratio_up_frame=afrr["ratio_up"],
        afrr_activation_ratio_down_frame=afrr["ratio_down"],
        battery=make_battery(initial_soc_mwh=0.7, terminal_soc_mwh=0.5),
        reserve_sustain_duration_minutes=15,
    )
    dispatch = result.dispatch
    assert dispatch["afrr_up_reserved_mw"].mean() > 0.2
    assert dispatch["afrr_up_reserved_mw"].mean() > dispatch["afrr_down_reserved_mw"].mean()
    assert (dispatch["discharge_mw"] + dispatch["afrr_up_reserved_mw"] <= dispatch["power_limit_mw"] + 1e-6).all()


def test_high_afrr_down_prices_shift_flat_day_to_downward_reserve() -> None:
    day_ahead = make_price_frame([40.0, 40.2, 40.1, 40.3])
    afrr = make_afrr_frames(
        capacity_up=[1.0, 1.0, 1.0, 1.0],
        capacity_down=[70.0, 70.0, 70.0, 70.0],
        activation_up=[1.0, 1.0, 1.0, 1.0],
        activation_down=[120.0, 120.0, 120.0, 120.0],
        ratio_up=[0.0, 0.0, 0.0, 0.0],
        ratio_down=[0.30, 0.30, 0.30, 0.30],
    )
    result = solve_day_ahead_afrr_dispatch(
        day_ahead_frame=day_ahead,
        afrr_capacity_up_frame=afrr["capacity_up"],
        afrr_capacity_down_frame=afrr["capacity_down"],
        afrr_activation_price_up_frame=afrr["activation_up"],
        afrr_activation_price_down_frame=afrr["activation_down"],
        afrr_activation_ratio_up_frame=afrr["ratio_up"],
        afrr_activation_ratio_down_frame=afrr["ratio_down"],
        battery=make_battery(initial_soc_mwh=0.4, terminal_soc_mwh=0.5),
        reserve_sustain_duration_minutes=15,
    )
    dispatch = result.dispatch
    assert dispatch["afrr_down_reserved_mw"].mean() > 0.2
    assert dispatch["afrr_down_reserved_mw"].mean() > dispatch["afrr_up_reserved_mw"].mean()
    assert (dispatch["charge_mw"] + dispatch["afrr_down_reserved_mw"] <= dispatch["power_limit_mw"] + 1e-6).all()


def test_afrr_expected_soc_evolution_changes_with_activation_ratio() -> None:
    day_ahead = make_price_frame([45.0, 45.0, 45.0, 45.0])
    base_kwargs = {
        "day_ahead_frame": day_ahead,
        "battery": make_battery(initial_soc_mwh=0.8, terminal_soc_mwh=None),
        "reserve_sustain_duration_minutes": 15,
    }
    low_ratio = make_afrr_frames(
        capacity_up=[50.0] * 4,
        capacity_down=[1.0] * 4,
        activation_up=[120.0] * 4,
        activation_down=[1.0] * 4,
        ratio_up=[0.05] * 4,
        ratio_down=[0.0] * 4,
    )
    high_ratio = make_afrr_frames(
        capacity_up=[50.0] * 4,
        capacity_down=[1.0] * 4,
        activation_up=[120.0] * 4,
        activation_down=[1.0] * 4,
        ratio_up=[0.40] * 4,
        ratio_down=[0.0] * 4,
    )
    low = solve_day_ahead_afrr_dispatch(
        afrr_capacity_up_frame=low_ratio["capacity_up"],
        afrr_capacity_down_frame=low_ratio["capacity_down"],
        afrr_activation_price_up_frame=low_ratio["activation_up"],
        afrr_activation_price_down_frame=low_ratio["activation_down"],
        afrr_activation_ratio_up_frame=low_ratio["ratio_up"],
        afrr_activation_ratio_down_frame=low_ratio["ratio_down"],
        **base_kwargs,
    )
    high = solve_day_ahead_afrr_dispatch(
        afrr_capacity_up_frame=high_ratio["capacity_up"],
        afrr_capacity_down_frame=high_ratio["capacity_down"],
        afrr_activation_price_up_frame=high_ratio["activation_up"],
        afrr_activation_price_down_frame=high_ratio["activation_down"],
        afrr_activation_ratio_up_frame=high_ratio["ratio_up"],
        afrr_activation_ratio_down_frame=high_ratio["ratio_down"],
        **base_kwargs,
    )
    assert high.dispatch["soc_mwh"].iloc[-1] < low.dispatch["soc_mwh"].iloc[-1]


def test_portfolio_afrr_allocation_respects_site_headroom() -> None:
    day_ahead = make_price_frame([42.0, 42.0, 42.0, 42.0])
    afrr = make_afrr_frames(
        capacity_up=[60.0] * 4,
        capacity_down=[30.0] * 4,
        activation_up=[140.0] * 4,
        activation_down=[60.0] * 4,
        ratio_up=[0.20] * 4,
        ratio_down=[0.08] * 4,
    )
    site = SiteSpec(id="site", poi_import_limit_mw=1.2, poi_export_limit_mw=1.2)
    result = solve_portfolio_day_ahead_afrr_dispatch(
        day_ahead_frame=day_ahead,
        afrr_capacity_up_frame=afrr["capacity_up"],
        afrr_capacity_down_frame=afrr["capacity_down"],
        afrr_activation_price_up_frame=afrr["activation_up"],
        afrr_activation_price_down_frame=afrr["activation_down"],
        afrr_activation_ratio_up_frame=afrr["ratio_up"],
        afrr_activation_ratio_down_frame=afrr["ratio_down"],
        site=site,
        assets=[make_asset("a1", power_mw=1.0), make_asset("a2", power_mw=1.0)],
        degradation_costs_eur_per_mwh={"a1": 0.0, "a2": 0.0},
        reserve_sustain_duration_minutes=15,
    )
    dispatch = result.site_dispatch
    assert dispatch["afrr_up_reserved_mw"].max() <= 1.200001
    assert dispatch["afrr_down_reserved_mw"].max() <= 1.200001
    assert (dispatch["charge_mw"] + dispatch["fcr_reserved_mw"] + dispatch["afrr_down_reserved_mw"] <= 1.200001).all()
    assert (dispatch["discharge_mw"] + dispatch["fcr_reserved_mw"] + dispatch["afrr_up_reserved_mw"] <= 1.200001).all()


def test_portfolio_solver_enforces_site_poi_limit() -> None:
    day_ahead = make_price_frame([-20.0, -20.0, 100.0, 100.0])
    site = SiteSpec(id="site", poi_import_limit_mw=1.0, poi_export_limit_mw=1.0)
    result = solve_portfolio_day_ahead_dispatch(
        day_ahead,
        site,
        [make_asset("a1", power_mw=1.0), make_asset("a2", power_mw=1.0)],
        degradation_costs_eur_per_mwh={"a1": 0.0, "a2": 0.0},
    )
    assert result.site_dispatch["charge_mw"].max() <= 1.000001
    assert result.site_dispatch["discharge_mw"].max() <= 1.000001
    coded = assign_site_reason_codes(result.site_dispatch, site)
    assert "blocked_by_site_poi_limit" in set(coded["reason_code"])


def test_portfolio_fcr_allocation_respects_site_headroom() -> None:
    day_ahead = make_price_frame([40.0, 40.0, 40.0, 40.0])
    fcr = make_price_frame([120.0, 120.0, 120.0, 120.0], market="fcr_capacity")
    site = SiteSpec(id="site", poi_import_limit_mw=1.2, poi_export_limit_mw=1.2)
    result = solve_portfolio_day_ahead_fcr_dispatch(
        day_ahead_frame=day_ahead,
        fcr_capacity_frame=fcr,
        site=site,
        assets=[make_asset("a1", power_mw=1.0), make_asset("a2", power_mw=1.0)],
        degradation_costs_eur_per_mwh={"a1": 0.0, "a2": 0.0},
        reserve_sustain_duration_minutes=15,
    )
    assert result.site_dispatch["fcr_reserved_mw"].max() <= 1.200001
    assert (result.site_dispatch["charge_mw"] + result.site_dispatch["fcr_reserved_mw"] <= 1.200001).all()
    assert (result.site_dispatch["discharge_mw"] + result.site_dispatch["fcr_reserved_mw"] <= 1.200001).all()


def test_portfolio_heterogeneous_assets_allocate_differently() -> None:
    day_ahead = make_price_frame([-30.0, -10.0, 120.0, 150.0])
    site = SiteSpec(id="site", poi_import_limit_mw=1.6, poi_export_limit_mw=1.6)
    result = solve_portfolio_day_ahead_dispatch(
        day_ahead,
        site,
        [
            make_asset("a1", power_mw=1.0, energy_mwh=2.0, soc_max_mwh=1.8, initial_soc_mwh=1.0, terminal_soc_mwh=1.0),
            make_asset("a2", power_mw=0.5, energy_mwh=0.8, soc_max_mwh=0.7, initial_soc_mwh=0.4, terminal_soc_mwh=0.4),
        ],
        degradation_costs_eur_per_mwh={"a1": 0.0, "a2": 0.0},
    )
    grouped = result.asset_dispatch.groupby("asset_id")["discharge_mw"].sum()
    assert grouped["a1"] != pytest.approx(grouped["a2"])
