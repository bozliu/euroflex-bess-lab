"""Nordic walk-forward backtest: DST-safe persistence, causality, oracle bound, data handling and the research
script end to end on the committed fixture tree (no network; outputs only under ``tmp_path``)."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.markets.nordic.backtest import (
    MAX_MISSING_DA_QH,
    NORDIC_BATTERY,
    ZoneMarket,
    annualise,
    applicable_workflows,
    local_day_bounds,
    persistence_forecast,
    run_zone,
    workflow_products,
)
from euroflex_bess_lab.valuation.revenue import LedgerMetadata, normalize_ledger

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "nordic" / "public"
TZ = "Europe/Copenhagen"


def _utc(day: str) -> pd.Timestamp:
    return pd.Timestamp(day).tz_localize(TZ).tz_convert("UTC")


def test_persistence_uses_the_same_local_time_on_the_previous_day_across_dst() -> None:
    # 2025-10-25 (24 h), 2025-10-26 (25 h, clocks go back), 2025-10-27 (24 h).
    qh = pd.date_range(_utc("2025-10-25"), _utc("2025-10-28"), freq="15min", inclusive="left")
    local = qh.tz_convert(TZ)
    values = (local.day * 100 + local.hour).to_numpy(dtype=float)
    forecast, lag = persistence_forecast(values, qh)
    day26 = np.asarray(local.normalize() == pd.Timestamp("2025-10-26", tz=TZ))
    assert day26.sum() == 100
    # Both 02:xx hours of the 25-hour day are forecast with the single 02:xx hour of the 25th.
    expected = 2500 + local[day26].hour.to_numpy()
    assert np.array_equal(forecast[day26], expected)
    day27 = np.asarray(local.normalize() == pd.Timestamp("2025-10-27", tz=TZ))
    assert np.array_equal(forecast[day27], 2600 + local[day27].hour.to_numpy())
    assert (lag[day26 | day27] == 1).all() and np.isnan(forecast[:96]).all()


def test_persistence_falls_back_to_older_days_and_fills_the_spring_forward_gap() -> None:
    qh = pd.date_range(_utc("2026-03-27"), _utc("2026-03-31"), freq="15min", inclusive="left")  # 29 Mar = 23 h
    local = qh.tz_convert(TZ)
    values = (local.day * 100 + local.hour).to_numpy(dtype=float)
    values[(local.normalize() == pd.Timestamp("2026-03-28", tz=TZ)) & (local.hour == 10)] = np.nan
    forecast, lag = persistence_forecast(values, qh)
    day29 = np.asarray(local.normalize() == pd.Timestamp("2026-03-29", tz=TZ))
    hour10 = day29 & (local.hour == 10)
    assert np.all(forecast[hour10] == 2710) and np.all(lag[hour10] == 2)  # D-1 missing -> D-2
    day30 = np.asarray(local.normalize() == pd.Timestamp("2026-03-30", tz=TZ))
    # 02:xx did not exist on the 29th: filled from the neighbouring slot of the same day.
    gap = day30 & (local.hour == 2)
    assert np.isfinite(forecast[gap]).all()


def _synthetic_market(zone: str = "SE3", days: int = 6, seed: int = 0) -> tuple[ZoneMarket, pd.Timestamp, pd.Timestamp]:
    rng = np.random.default_rng(seed)
    start, end = _utc("2025-06-01"), _utc("2025-06-01") + pd.Timedelta(days=days)
    qh = pd.date_range(start, end, freq="15min", inclusive="left")
    hours = pd.date_range(start, end, freq="1h", inclusive="left")
    shape = 60 + 50 * np.sin(np.arange(len(qh)) / 96 * 2 * np.pi)
    da = pd.DataFrame(
        {"timestamp_utc": qh, "resolution_minutes": 15, "price_eur_mwh": shape + rng.normal(0, 25, len(qh))}
    )
    reserves = {
        p.key: pd.DataFrame({"delivery_start_utc": hours, "price_eur_per_mw_h": rng.gamma(2.0, 6.0, len(hours))})
        for p in workflow_products(zone, "full_stack")
    }
    market = ZoneMarket.from_frames(zone, start, end, da, reserves)
    return market, start + pd.Timedelta(days=1), end


def test_oracle_bounds_persistence_every_day_and_schedules_are_feasible() -> None:
    market, start, end = _synthetic_market()
    result = run_zone(market, start, end)
    daily = result.daily
    assert set(daily["workflow"]) == set(applicable_workflows("SE3")) and not result.skipped_days
    pivot = daily.pivot_table(index=["day", "workflow"], columns="mode", values="net_eur")
    assert (pivot["oracle"] >= pivot["persistence"] - 1e-6).all()
    full = daily.loc[daily["workflow"] == "full_stack"]
    da_only = daily.loc[daily["workflow"] == "da_only"]
    assert (
        full.loc[full["mode"] == "oracle", "net_eur"].sum() >= da_only.loc[da_only["mode"] == "oracle", "net_eur"].sum()
    )
    assert result.ledger is not None and len(result.ledger) == 96 * 5
    ledger = normalize_ledger(result.ledger)
    pers = full.loc[full["mode"] == "persistence"]
    assert ledger["pnl_eur"].sum() == pytest.approx(pers["net_eur"].sum(), rel=1e-9)
    summary = annualise(pers)
    assert summary["days"] == 5 and summary["cycles_per_day"] >= 0


def test_future_prices_never_change_todays_decision() -> None:
    market, start, end = _synthetic_market(days=5, seed=2)
    day = pd.Timestamp("2025-06-03")
    lo = local_day_bounds(day)[0]
    base = run_zone(market, lo, local_day_bounds(day)[1], workflows=["full_stack"], modes=["persistence"]).ledger
    da = pd.DataFrame(
        {"timestamp_utc": market.qh_utc, "resolution_minutes": market.da_resolution, "price_eur_mwh": market.da_price}
    )
    later = da["timestamp_utc"] >= lo
    da.loc[later, "price_eur_mwh"] = -da.loc[later, "price_eur_mwh"] * 3
    reserves = {}
    for key, values in market.reserve.items():
        v = values.copy()
        v[market.hour_utc >= lo] *= 10
        reserves[key] = pd.DataFrame({"delivery_start_utc": market.hour_utc, "price_eur_per_mw_h": v})
    shocked = ZoneMarket.from_frames(
        "SE3", market.qh_utc[0], market.qh_utc[-1] + pd.Timedelta(minutes=15), da, reserves
    )
    again = run_zone(shocked, lo, local_day_bounds(day)[1], workflows=["full_stack"], modes=["persistence"]).ledger
    assert base is not None and again is not None
    columns = [
        "charged_mwh",
        "discharged_mwh",
        "soc_end_mwh",
        *[c for c in base.columns if c.endswith("_mw") and c != "fleet_mw"],
    ]
    pd.testing.assert_frame_equal(base[columns], again[columns])


def test_missing_day_ahead_quarter_hours_are_untradable_or_skip_the_day() -> None:
    market, start, end = _synthetic_market(days=4, seed=4)
    q0 = market.qh_utc.searchsorted(_utc("2025-06-03"))
    market.da_price[q0 + 40 : q0 + 44] = np.nan  # one hour missing on the 3rd
    q1 = market.qh_utc.searchsorted(_utc("2025-06-04"))
    market.da_price[q1 : q1 + MAX_MISSING_DA_QH + 1] = np.nan  # too much missing on the 4th
    result = run_zone(market, start, end, workflows=["da_only"], modes=["oracle"])
    assert result.skipped_days == ["2025-06-04"]
    third = result.daily.loc[result.daily["day"] == "2025-06-03"].iloc[0]
    assert third["untradable_qh"] == 4


def test_swedish_mfrr_missing_hours_mean_no_procurement() -> None:
    start, end = _utc("2025-06-01"), _utc("2025-06-03")
    qh = pd.date_range(start, end, freq="15min", inclusive="left")
    hours = pd.date_range(start, end, freq="1h", inclusive="left")
    da = pd.DataFrame({"timestamp_utc": qh, "resolution_minutes": 15, "price_eur_mwh": 50.0})
    mfrr = pd.DataFrame({"delivery_start_utc": hours[::2], "price_eur_per_mw_h": 30.0})
    market = ZoneMarket.from_frames("SE3", start, end, da, {"mfrr_up": mfrr, "afrr_up": mfrr.iloc[:0]})
    assert np.nanmin(market.reserve["mfrr_up"]) == 0.0 and np.isfinite(market.reserve["mfrr_up"]).all()
    assert np.isnan(market.reserve["afrr_up"]).all()  # an empty series stays "no market"


def test_fixture_market_loads_with_hourly_and_quarter_hour_days() -> None:
    market = ZoneMarket.from_public("DK1", _utc("2025-09-28"), _utc("2025-10-03"), root=FIXTURES)
    hourly = market.day_inputs(pd.Timestamp("2025-09-30"), forecast=False)
    quarter = market.day_inputs(pd.Timestamp("2025-10-01"), forecast=False)
    assert hourly is not None and hourly.hourly_da and quarter is not None and not quarter.hourly_da
    assert set(market.reserve) == {p.key for p in workflow_products("DK1", "full_stack")}
    fi = ZoneMarket.from_public("FI", _utc("2025-06-09"), _utc("2025-06-13"), root=FIXTURES)
    assert set(fi.reserve) == {"afrr_up", "afrr_down"}


@pytest.fixture(scope="module")
def research() -> ModuleType:
    path = REPO_ROOT / "scripts" / "research" / "nordic_backtest.py"
    spec = importlib.util.spec_from_file_location("nordic_backtest_under_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_zone_periods_latest_12_full_months_and_years(research: ModuleType) -> None:
    days = [d.date().isoformat() for d in pd.date_range("2024-02-01", "2026-10-25")]
    periods = research.zone_periods(days)
    assert periods["latest12"] == ("2025-10-01", "2026-09-30")
    assert periods["2024"] == ("2024-02-01", "2024-12-31") and periods["2026"] == ("2026-01-01", "2026-10-25")
    fi = research.zone_periods([d.date().isoformat() for d in pd.date_range("2024-02-01", "2026-07-01")])
    assert fi["latest12"] == ("2025-07-01", "2026-06-30")


def test_research_script_runs_end_to_end_on_fixtures(research: ModuleType, tmp_path: Path) -> None:
    code = research.main(
        [
            "--start",
            "2025-06-09",
            "--end",
            "2025-10-27",
            "--data-dir",
            str(FIXTURES),
            "--out-dir",
            str(tmp_path),
            "--processes",
            "1",
        ]
    )
    assert code == 0
    report = (tmp_path / "reports" / "nordics.md").read_text(encoding="utf-8")
    assert "## Latest 12 full months: revenue stack per zone" in report and "CC BY 4.0" in report
    assert "Not modelled as a strategy" in report and "/" + "Users" + "/" not in report
    for name in ("nordic_revenue_stack.png", "nordic_monthly_net_revenue.png", "nordic_capacity_prices.png"):
        assert (tmp_path / "reports" / "figures" / name).stat().st_size > 1000
    ledgers = tmp_path / "reports" / "ledgers"
    daily = pd.read_parquet(ledgers / "nordic_daily.parquet")
    assert set(daily["zone"]) == {"DK1", "DK2", "SE3", "SE4", "FI"}
    meta = LedgerMetadata.from_dict(json.loads((ledgers / "nordic_qh_SE3_full_stack.json").read_text()))
    assert meta.market == "SE3" and meta.power_mw == NORDIC_BATTERY.power_mw and meta.local_timezone == TZ
    summary = json.loads((ledgers / "nordic_summary.json").read_text())
    assert summary["checks"]["no_lookahead"]["forecasts_changed_by_future_prices"] == 0
    assert summary["checks"]["feasibility"]["violations"] == 0
