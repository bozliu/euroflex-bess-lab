from __future__ import annotations

import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.valuation import (
    LEDGER_COLUMNS,
    LedgerMetadata,
    annualise,
    complete_days,
    daily_pnl,
    ledger_from_simulation,
    load_ledger_metadata,
    market_timezone,
    metadata_from_simulation,
    normalize_ledger,
    read_ledger,
    seasonal_annual_value,
    summarize_ledger,
    write_ledger,
)
from euroflex_bess_lab.valuation.revenue import DAYS_PER_YEAR, infer_interval_minutes


def local_grid(first_day: str, last_day: str, timezone: str = "Europe/Brussels") -> pd.DatetimeIndex:
    """Quarter-hour UTC grid covering whole local days ``first_day .. last_day``."""
    start = pd.Timestamp(first_day).tz_localize(timezone).tz_convert("UTC")
    end = (pd.Timestamp(last_day) + pd.Timedelta(days=1)).tz_localize(timezone).tz_convert("UTC")
    return pd.date_range(start, end, freq="15min", inclusive="left")


def constant_ledger(grid: pd.DatetimeIndex, pnl: float = 1.0, degradation: float = 0.0) -> pd.DataFrame:
    return pd.DataFrame(
        {"timestamp_utc": grid, "revenue_eur": pnl + degradation, "degradation_eur": degradation, "pnl_eur": pnl}
    )


def test_daily_pnl_uses_local_delivery_days_across_both_dst_switches() -> None:
    spring = daily_pnl(constant_ledger(local_grid("2026-03-28", "2026-03-30")), timezone="Europe/Brussels")
    autumn = daily_pnl(constant_ledger(local_grid("2026-10-24", "2026-10-26")), timezone="Europe/Brussels")
    assert spring["n_intervals"].tolist() == [96, 92, 96]
    assert autumn["n_intervals"].tolist() == [96, 100, 96]
    assert spring["expected_intervals"].tolist() == [96.0, 92.0, 96.0]
    assert np.allclose(autumn["coverage"], 1.0)
    assert spring["pnl_eur"].tolist() == [96.0, 92.0, 96.0]
    assert [d.date().isoformat() for d in autumn["date"]] == ["2026-10-24", "2026-10-25", "2026-10-26"]


def test_daily_pnl_flags_partial_edge_days_and_complete_days_drops_them() -> None:
    grid = pd.date_range("2026-06-01", "2026-06-04", freq="15min", inclusive="left", tz="UTC")
    daily = daily_pnl(constant_ledger(grid), timezone="Europe/Brussels", power_mw=2.0)
    # UTC midnight is 02:00 in Brussels summer time: the first local day misses 8 QHs, the last has only 8.
    assert daily["n_intervals"].tolist() == [88, 96, 96, 8]
    kept = complete_days(daily, min_coverage=0.98)
    assert len(kept) == 2
    assert np.allclose(kept["pnl_eur_per_mw"], 48.0)


def test_normalize_ledger_accepts_backtest_aliases_and_derives_columns() -> None:
    grid = pd.date_range("2026-01-01", periods=4, freq="15min", tz="UTC")
    backtest = pd.DataFrame(
        {"delivery_start_utc": grid, "realized_pnl_eur": [1.0, 2.0, np.nan, 4.0], "degradation_cost_eur": 0.5}
    )
    out = normalize_ledger(backtest)
    assert list(out.columns) == list(LEDGER_COLUMNS)
    assert out["pnl_eur"].tolist() == [1.0, 2.0, 0.0, 4.0]
    assert out["pnl_was_nan"].tolist() == [False, False, True, False]
    assert out["revenue_eur"].tolist()[:2] == [1.5, 2.5]

    priced = pd.DataFrame({"timestamp_utc": grid, "energy_mwh": [0.25, -0.25, 0.0, 0.1], "price_eur_mwh": 100.0})
    out = normalize_ledger(priced)
    assert out["revenue_eur"].tolist() == [25.0, -25.0, 0.0, 10.0]
    assert out["pnl_eur"].tolist() == out["revenue_eur"].tolist()
    assert out["discharged_mwh"].tolist() == [0.25, 0.0, 0.0, 0.1]
    assert out["charged_mwh"].tolist() == [0.0, 0.25, 0.0, 0.0]


def test_normalize_ledger_rejects_duplicates_unless_summed() -> None:
    grid = pd.date_range("2026-01-01", periods=2, freq="15min", tz="UTC")
    sites = pd.DataFrame({"timestamp_utc": list(grid) * 2, "pnl_eur": [1.0, 2.0, 3.0, 4.0]})
    with pytest.raises(ValueError, match="duplicate"):
        normalize_ledger(sites)
    summed = normalize_ledger(sites, sum_duplicates=True)
    assert summed["pnl_eur"].tolist() == [4.0, 6.0]
    with pytest.raises(ValueError, match="timestamp_utc"):
        normalize_ledger(pd.DataFrame({"pnl_eur": [1.0]}))
    with pytest.raises(ValueError, match="pnl_eur"):
        normalize_ledger(pd.DataFrame({"timestamp_utc": grid, "energy_mwh": [1.0, 2.0]}))


def test_summary_annualises_per_mw_and_reports_distribution() -> None:
    grid = local_grid("2025-01-01", "2025-12-31")
    ledger = constant_ledger(grid, pnl=2.0, degradation=0.5)
    ledger["energy_mwh"] = 0.25
    metadata = LedgerMetadata(asset_id="flat", power_mw=2.0, energy_mwh=4.0)
    summary = summarize_ledger(ledger, metadata)
    assert summary.n_days == 365
    assert summary.pnl_eur_per_mw_year == pytest.approx(2.0 * 96 / 2.0 * DAYS_PER_YEAR, rel=1e-3)
    assert summary.revenue_eur_per_mw_year == pytest.approx(2.5 * 96 / 2.0 * DAYS_PER_YEAR, rel=1e-3)
    assert summary.seasonal_pnl_eur_per_mw_year == pytest.approx(96.0 * DAYS_PER_YEAR, rel=1e-3)
    assert summary.negative_day_share == 0.0
    assert summary.max_drawdown_eur_per_mw == 0.0
    assert summary.full_cycles_per_day == pytest.approx(96 * 0.25 / 4.0, rel=1e-3)
    # top 37 of 365 days: the 100-QH autumn DST day plus 36 regular days, out of 35,040 QHs in total
    assert summary.top_decile_day_share == pytest.approx((100 + 36 * 96) / 35040, rel=1e-9)
    payload = json.loads(json.dumps(summary.to_dict()))
    assert payload["asset_id"] == "flat"


def test_seasonal_annual_value_needs_all_months_and_reweights_partial_years() -> None:
    days = pd.date_range("2026-01-01", "2026-12-31", freq="D")
    daily = pd.DataFrame({"date": days, "pnl_eur_per_mw": np.where(days.month <= 6, 100.0, 300.0)})
    first_half = daily.loc[daily["date"].dt.month <= 6]
    assert seasonal_annual_value(first_half) is None
    value = seasonal_annual_value(daily)
    assert value == pytest.approx(100.0 * (31 + 28.25 + 31 + 30 + 31 + 30) + 300.0 * (31 + 31 + 30 + 31 + 30 + 31))
    assert annualise(1000.0, 10, power_mw=2.0) == pytest.approx(1000.0 / 2.0 / 10 * DAYS_PER_YEAR)


def test_metadata_parsing_is_tolerant_and_sidecars_round_trip(tmp_path) -> None:
    metadata = LedgerMetadata.from_dict(
        {
            "name": "be_strategy",
            "battery": {"power_mw": 10, "energy_mwh": 20, "degradation_eur_per_mwh": 8},
            "fleet_size_mw": 500,
            "mechanism": "new2026",
            "lag": "1 min",
        }
    )
    assert metadata.asset_id == "be_strategy"
    assert metadata.power_mw == 10.0 and metadata.energy_mwh == 20.0
    assert metadata.fleet_mw == 500.0
    assert metadata.extra == {"lag": "1 min"}
    assert metadata.local_timezone == "Europe/Brussels"
    assert market_timezone("DE-LU") == "Europe/Berlin"

    grid = local_grid("2026-03-02", "2026-03-03")
    for suffix in (".parquet", ".csv"):
        path = write_ledger(constant_ledger(grid), tmp_path / f"ledger{suffix}", metadata)
        assert load_ledger_metadata(path) == metadata
        ledger, loaded = read_ledger(path, power_mw=5.0)
        assert loaded.power_mw == 5.0
        assert ledger["timestamp_utc"].dt.tz is not None
        assert len(ledger) == len(grid)
    with pytest.raises(ValueError, match="power_mw"):
        LedgerMetadata(asset_id="x").require_power_mw()


def test_simulation_adapters_match_the_realtime_result_shape(tmp_path) -> None:
    grid = local_grid("2026-03-02", "2026-03-02")
    qh = constant_ledger(grid)
    # attribute names of euroflex_bess_lab.realtime.SimulationResult
    result = SimpleNamespace(
        strategy="threshold",
        mechanism="new2026",
        battery=SimpleNamespace(power_mw=2.0, energy_mwh=4.0, degradation_eur_per_mwh=8.0, availability=1.0),
        publication=SimpleNamespace(minute_lag=1, qh_final_lag_minutes=5, da_publication_hour_local=13),
        qh=qh,
        timezone="Europe/Brussels",
        meta={"fleet_mw": 750.0, "impact": "linear"},
    )
    assert len(ledger_from_simulation(result)) == 96
    metadata = metadata_from_simulation(result, asset_id="be")
    assert (metadata.power_mw, metadata.strategy, metadata.mechanism, metadata.asset_id) == (
        2.0,
        "threshold",
        "new2026",
        "be",
    )
    assert metadata.fleet_mw == 750.0 and metadata.extra == {"impact": "linear"}
    assert metadata.publication_lag == "minute_lag=1, qh_final_lag_minutes=5, da_publication_hour_local=13"
    assert metadata.period_start == grid[0].isoformat()
    assert metadata.period_end == (grid[-1] + pd.Timedelta(minutes=15)).isoformat()
    with pytest.raises(TypeError):
        ledger_from_simulation(object())

    # SimulationResult.to_parquet writes qh_ledger.parquet + summary.json (no .meta.json sidecar)
    run = tmp_path / "be_threshold_new2026"
    run.mkdir()
    qh.to_parquet(run / "qh_ledger.parquet")
    summary = {
        "strategy": "threshold",
        "mechanism": "new2026",
        "battery": {"power_mw": 2.0, "energy_mwh": 4.0, "degradation_eur_per_mwh": 8.0},
        "publication": {"minute_lag": 1, "qh_final_lag_minutes": 5},
        "start_utc": "2026-03-01 23:00:00+00:00",
        "eur_per_mw_year": 12.0,
    }
    (run / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    ledger, loaded = read_ledger(run / "qh_ledger.parquet")
    assert (loaded.asset_id, loaded.power_mw, loaded.mechanism) == ("be_threshold_new2026", 2.0, "new2026")
    assert loaded.publication_lag == "minute_lag=1, qh_final_lag_minutes=5"
    assert loaded.period_start == "2026-03-01 23:00:00+00:00"
    assert loaded.source == "summary.json" and loaded.extra["eur_per_mw_year"] == 12.0


def test_annualisation_uses_covered_time_for_partial_days_and_hourly_ledgers() -> None:
    grid = pd.date_range("2026-03-02 00:00", periods=36 * 4, freq="15min", tz="UTC")  # 36 hours
    metadata = LedgerMetadata(asset_id="short", power_mw=1.0)
    loose = summarize_ledger(constant_ledger(grid, pnl=1.0), metadata, min_coverage=0.0)
    assert loose.days_covered == pytest.approx(1.5)
    assert loose.pnl_eur_per_mw_year == pytest.approx(144.0 / 1.5 * DAYS_PER_YEAR)
    hourly = local_grid("2025-01-01", "2025-12-31")[::4]
    yearly = summarize_ledger(constant_ledger(hourly, pnl=4.0), metadata)
    assert yearly.interval_minutes == 60.0 and yearly.days_covered == pytest.approx(365.0)
    assert yearly.pnl_eur_per_mw_year == pytest.approx(4.0 * 24 * DAYS_PER_YEAR, rel=1e-9)


def test_interval_inference_is_resolution_agnostic() -> None:
    grid = pd.date_range("2026-01-01", periods=8, freq="15min", tz="UTC")
    assert infer_interval_minutes(grid) == 15.0
    assert infer_interval_minutes(pd.Series(grid.astype("datetime64[s, UTC]"))) == 15.0
    hourly = pd.date_range("2026-01-01", periods=8, freq="h", tz="UTC")
    daily = daily_pnl(constant_ledger(hourly), timezone="UTC")
    assert daily["expected_intervals"].iloc[0] == 24.0
