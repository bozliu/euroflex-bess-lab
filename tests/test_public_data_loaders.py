from __future__ import annotations

import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.public_data import (
    data_root,
    load_belgium_legacy,
    load_belgium_minute,
    load_belgium_new_formula_minute,
    load_belgium_new_formula_qh,
    load_belgium_qh,
    load_belgium_si_forecast,
    load_capacity_awards,
    load_day_ahead_prices,
    load_energy_bid_curves,
    load_germany_reserve,
    load_res_load,
    raw_root,
    read_manifest,
)
from euroflex_bess_lab.public_data.normalize import MINUTE_COLUMNS, QH_COLUMNS, SI_FORECAST_COLUMNS

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "public"


@pytest.fixture
def fixture_root(monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("EUROFLEX_DATA_DIR", str(FIXTURES))
    monkeypatch.delenv("EUROFLEX_RAW_DIR", raising=False)
    return FIXTURES


def _is_utc(series: pd.Series) -> bool:
    return str(series.dtype) == "datetime64[ns, UTC]"


def test_data_root_honours_env_and_walks_up(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("EUROFLEX_DATA_DIR", str(tmp_path / "custom"))
    assert data_root() == (tmp_path / "custom").resolve()
    monkeypatch.setenv("EUROFLEX_RAW_DIR", str(tmp_path / "rawcache"))
    assert raw_root() == (tmp_path / "rawcache").resolve()
    monkeypatch.delenv("EUROFLEX_DATA_DIR")
    monkeypatch.delenv("EUROFLEX_RAW_DIR")
    nested = tmp_path / "project" / "a" / "b"
    nested.mkdir(parents=True)
    (tmp_path / "project" / "data" / "public").mkdir(parents=True)
    monkeypatch.chdir(nested)
    assert data_root() == (tmp_path / "project" / "data" / "public").resolve()
    assert raw_root() == (tmp_path / "project" / "data" / "raw").resolve()


def test_minute_loader_half_open_utc_and_columns(fixture_root: Path) -> None:
    frame = load_belgium_minute("2025-06-10", "2025-06-10T01:00:00")
    assert list(frame.columns) == MINUTE_COLUMNS
    assert len(frame) == 60
    assert frame["timestamp_utc"].iloc[0] == pd.Timestamp("2025-06-10T00:00:00Z")
    assert frame["timestamp_utc"].iloc[-1] == pd.Timestamp("2025-06-10T00:59:00Z")
    assert _is_utc(frame["timestamp_utc"]) and _is_utc(frame["qh_start_utc"])
    assert frame["timestamp_utc"].is_monotonic_increasing
    subset = load_belgium_minute(
        pd.Timestamp("2025-06-10T02:00:00+02:00"), "2025-06-10T00:30:00Z", columns=["price_cum_eur_mwh"]
    )
    assert list(subset.columns) == ["timestamp_utc", "price_cum_eur_mwh"]
    assert len(subset) == 30 and subset["price_cum_eur_mwh"].dtype == np.float32


def test_minute_loader_spans_two_year_partitions(fixture_root: Path) -> None:
    frame = load_belgium_minute("2025-06-12T23:00:00Z", "2026-03-02T00:30:00Z", columns=["si_mw"])
    assert set(frame["timestamp_utc"].dt.year) == {2025, 2026}
    assert len(frame) == 60 + 30
    assert frame["timestamp_utc"].is_unique


def test_qh_and_new_formula_loaders(fixture_root: Path) -> None:
    qh = load_belgium_qh("2026-03-02", "2026-03-05")
    assert list(qh.columns) == QH_COLUMNS and len(qh) == 288
    assert qh["imbalance_price_eur_mwh"].notna().all()
    nf_qh = load_belgium_new_formula_qh("2026-03-02", "2026-03-05")
    nf_min = load_belgium_new_formula_minute(
        "2026-03-02", "2026-03-05", columns=["imbalance_price_eur_mwh", "si_rolling_mw"]
    )
    assert len(nf_qh) == 288 and len(nf_min) == 4320
    assert {"weight_afrr", "weight_mfrr", "incentive_eur_mwh", "frce_l2_target_mw"} <= set(nf_qh.columns)
    assert list(nf_min.columns) == ["timestamp_utc", "imbalance_price_eur_mwh", "si_rolling_mw"]


def test_si_forecast_loader_by_issue_time_and_target(fixture_root: Path) -> None:
    by_issue = load_belgium_si_forecast("current", "2026-03-03", "2026-03-03T01:00:00Z")
    assert list(by_issue.columns) == SI_FORECAST_COLUMNS
    assert (by_issue["prediction_time_utc"] >= pd.Timestamp("2026-03-03T00:00:00Z")).all()
    assert (by_issue["prediction_time_utc"] < pd.Timestamp("2026-03-03T01:00:00Z")).all()
    assert by_issue["prediction_quality"].between(0, 1).all()
    nxt = load_belgium_si_forecast("next", "2026-03-03T01:00:00Z", "2026-03-03T02:00:00Z", on="target_qh_utc")
    lead = nxt["target_qh_utc"] - nxt["prediction_time_utc"]
    assert (lead > pd.Timedelta(0)).all() and (lead <= pd.Timedelta(minutes=16)).all()
    assert nxt["target_qh_utc"].min() == pd.Timestamp("2026-03-03T01:00:00Z")
    with pytest.raises(ValueError):
        load_belgium_si_forecast("tomorrow", "2026-03-03", "2026-03-04")
    with pytest.raises(ValueError):
        load_belgium_si_forecast("current", "2026-03-03", "2026-03-04", on="nonsense")


def test_day_ahead_loader_resolution(fixture_root: Path) -> None:
    be_2025 = load_day_ahead_prices("BE", "2025-06-10", "2025-06-11")
    assert len(be_2025) == 24 and set(be_2025["resolution_minutes"]) == {60}
    de_2026 = load_day_ahead_prices("de-lu", "2026-03-02", "2026-03-03")
    assert len(de_2026) == 96 and set(de_2026["resolution_minutes"]) == {15}
    assert list(de_2026.columns) == ["timestamp_utc", "resolution_minutes", "price_eur_mwh"]
    with pytest.raises(ValueError):
        load_day_ahead_prices("FR", "2025-06-10", "2025-06-11")
    expanded = load_day_ahead_prices("BE", "2025-06-10T00:30:00Z", "2025-06-10T02:00:00Z", quarter_hourly=True)
    assert len(expanded) == 6 and set(expanded["resolution_minutes"]) == {60}
    assert expanded["timestamp_utc"].iloc[0] == pd.Timestamp("2025-06-10T00:30:00Z")
    assert (expanded["timestamp_utc"].diff().dropna() == pd.Timedelta(minutes=15)).all()
    hourly = be_2025.set_index("timestamp_utc")["price_eur_mwh"]
    assert expanded["price_eur_mwh"].iloc[0] == hourly.loc[pd.Timestamp("2025-06-10T00:00:00Z")]
    assert expanded["price_eur_mwh"].iloc[2] == hourly.loc[pd.Timestamp("2025-06-10T01:00:00Z")]
    qh_2026 = load_day_ahead_prices("DE-LU", "2026-03-02", "2026-03-03", quarter_hourly=True)
    pd.testing.assert_frame_equal(qh_2026, de_2026)


def test_capacity_res_and_curve_loaders(fixture_root: Path) -> None:
    afrr = load_capacity_awards("aFRR", "2026-03-02", "2026-03-05")
    assert set(afrr["direction"]) == {"up", "down"} and len(afrr) == 36
    assert (afrr["delivery_end_utc"] > afrr["delivery_start_utc"]).all()
    assert (afrr["awarded_mw"] > 0).all()
    mfrr = load_capacity_awards("mfrr", "2025-06-10", "2025-06-13")
    assert set(mfrr["direction"]) == {"up"}
    res = load_res_load("2025-06-10", "2025-06-11", columns=["pv_mw", "load_mw"])
    assert len(res) == 96 and res["pv_mw"].max() > 0
    curves = load_energy_bid_curves("2026-03-02", "2026-03-02T00:15:00Z", direction="up")
    assert len(curves) == 81 and set(curves["direction"]) == {"up"}
    prices = curves["price_eur_mwh"].dropna().to_numpy()
    assert np.all(np.diff(prices) >= 0)  # upward merit order is non-decreasing
    down = load_energy_bid_curves("2026-03-02", "2026-03-02T00:15:00Z", direction="down")["price_eur_mwh"].dropna()
    assert np.all(np.diff(down.to_numpy()) <= 0)
    with pytest.raises(ValueError):
        load_energy_bid_curves("2026-03-02", "2026-03-03", direction="sideways")


def test_germany_loader_uses_fixture_then_raw_fallback(
    fixture_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fcr = load_germany_reserve("fcr", "2025-06-10", "2025-06-13")
    assert set(fcr["direction"]) == {"symmetric"} and len(fcr) > 0
    # Non-redistributed tables fall back to the raw cache when the public tree only has a stub sidecar.
    public = tmp_path / "public"
    raw = tmp_path / "raw"
    (public / "germany").mkdir(parents=True)
    (raw / "germany").mkdir(parents=True)
    shutil.copy(FIXTURES / "germany" / "afrr_capacity.parquet", raw / "germany" / "afrr_capacity.parquet")
    monkeypatch.setenv("EUROFLEX_DATA_DIR", str(public))
    monkeypatch.setenv("EUROFLEX_RAW_DIR", str(raw))
    afrr = load_germany_reserve("afrr", "2026-03-02", "2026-03-05")
    assert set(afrr["direction"]) == {"up", "down"}
    (raw / "germany" / "afrr_capacity.parquet").unlink()
    with pytest.raises(FileNotFoundError, match="euroflex data fetch germany-reserve --product afrr"):
        load_germany_reserve("afrr", "2026-03-02", "2026-03-05")


def test_missing_files_name_the_fetch_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EUROFLEX_DATA_DIR", str(tmp_path))
    with pytest.raises(
        FileNotFoundError, match=r"euroflex data fetch belgium-minute --start 2025-01-01 --end 2026-01-01"
    ):
        load_belgium_minute("2025-06-10", "2025-06-11")
    with pytest.raises(FileNotFoundError, match="euroflex data fetch day-ahead --zone BE"):
        load_day_ahead_prices("BE", "2025-06-10", "2025-06-11")
    with pytest.raises(FileNotFoundError, match="euroflex data fetch belgium-new-formula"):
        load_belgium_new_formula_qh()
    with pytest.raises(FileNotFoundError, match="euroflex data manifest"):
        read_manifest()


def test_out_of_coverage_and_invalid_ranges(fixture_root: Path) -> None:
    empty = load_belgium_minute("2023-01-01", "2023-02-01")
    assert empty.empty and list(empty.columns) == MINUTE_COLUMNS
    future = load_belgium_qh("2031-01-01", "2031-01-02")
    assert future.empty and list(future.columns) == QH_COLUMNS
    with pytest.raises(ValueError, match="half-open"):
        load_belgium_qh("2026-03-03", "2026-03-02")
    with pytest.raises(KeyError, match="Unknown columns"):
        load_belgium_qh("2026-03-02", "2026-03-03", columns=["not_a_column"])


def test_legacy_loader(fixture_root: Path) -> None:
    qh = load_belgium_legacy("qh", "2024-05-20", "2024-06-30")
    assert len(qh) == 96 and qh["timestamp_utc"].max() < pd.Timestamp("2024-05-21T22:00:00Z")
    assert (qh["imbalance_price_eur_mwh"] == qh["imbalance_price_long_eur_mwh"]).all()
    minute = load_belgium_legacy("minute", "2024-05-20", "2024-05-20T00:15:00Z", columns=["price_cum_eur_mwh"])
    assert len(minute) == 15
    assert load_belgium_legacy("qh", "2025-01-01", "2025-02-01").empty
    with pytest.raises(ValueError):
        load_belgium_legacy("hourly", "2024-05-20", "2024-05-21")


def test_read_manifest_on_fixture_tree(fixture_root: Path) -> None:
    manifest = read_manifest()
    paths = {entry["path"] for entry in manifest["files"]}
    assert "belgium/minute_2025.parquet" in paths and "energy_charts/da_prices_BE.parquet" in paths
    assert manifest["n_files"] == len(paths)
    elia = [e for e in manifest["files"] if e["path"].startswith("belgium/")]
    assert all(e["licence"] == "CC BY 4.0" and e["attribution"] == "Elia Transmission Belgium SA" for e in elia)
