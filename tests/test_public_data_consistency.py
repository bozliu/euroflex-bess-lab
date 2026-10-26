from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.public_data import (
    last_minute_vs_qh,
    load_belgium_minute,
    load_belgium_new_formula_minute,
    load_belgium_new_formula_qh,
    load_belgium_qh,
)
from euroflex_bess_lab.public_data.validate import curve_vs_published_prices, minute_grid_report

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "public"
REPO_DATA = Path(__file__).resolve().parents[1] / "data" / "public"


@pytest.fixture
def fixture_root(monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("EUROFLEX_DATA_DIR", str(FIXTURES))
    return FIXTURES


def test_last_minute_vs_qh_exact_match_on_synthetic_data() -> None:
    minutes = pd.date_range("2026-03-02", periods=30, freq="1min", tz="UTC")
    minute = pd.DataFrame(
        {
            "timestamp_utc": minutes,
            "qh_start_utc": minutes.floor("15min"),
            "price_cum_eur_mwh": np.r_[np.full(14, 1.0), 100.0, np.full(14, 2.0), 200.0],
            "si_cum_mw": np.r_[np.zeros(14), -50.0, np.zeros(14), 30.0],
        }
    )
    qh = pd.DataFrame(
        {
            "timestamp_utc": pd.date_range("2026-03-02", periods=2, freq="15min", tz="UTC"),
            "imbalance_price_eur_mwh": [100.0, 200.5],
            "si_mw": [-50.0, 30.0],
        }
    )
    report = last_minute_vs_qh(minute, qh)
    assert report["qh_compared"] == 2
    price = report["price_cum_eur_mwh_vs_imbalance_price_eur_mwh"]
    assert price["match_rate"] == pytest.approx(0.5) and price["match_rate_within_1"] == pytest.approx(1.0)
    si = report["si_cum_mw_vs_si_mw"]
    assert si["match_rate"] == pytest.approx(1.0) and si["sign_match_rate"] == pytest.approx(1.0)


def test_ods133_minute14_tracks_ods134_on_fixtures(fixture_root: Path) -> None:
    for start, end in (("2025-06-10", "2025-06-13"), ("2026-03-02", "2026-03-05")):
        minute = load_belgium_minute(start, end)
        qh = load_belgium_qh(start, end)
        grid = minute_grid_report(minute["timestamp_utc"])
        assert grid["unique"] and grid["sorted"] and grid["missing_intervals"] == 0
        report = last_minute_vs_qh(minute, qh)
        assert report["qh_compared"] == 288
        price = report["price_cum_eur_mwh_vs_imbalance_price_eur_mwh"]
        # Minute 14 is an estimate of the validated QH price: often identical, nearly always close.
        assert 0.2 <= price["match_rate"] <= 1.0
        assert price["match_rate_within_1"] >= 0.5
        assert report["si_cum_mw_vs_si_mw"]["sign_match_rate"] >= 0.85


def test_new_formula_minute14_matches_quarter_hour_on_fixtures(fixture_root: Path) -> None:
    minute = load_belgium_new_formula_minute("2026-03-02", "2026-03-05")
    qh = load_belgium_new_formula_qh("2026-03-02", "2026-03-05")
    minute["qh_start_utc"] = minute["timestamp_utc"].dt.floor("15min")
    report = last_minute_vs_qh(
        minute, qh, pairs=(("imbalance_price_eur_mwh", "imbalance_price_eur_mwh"), ("frce_mw", "frce_mw"))
    )
    assert report["qh_compared"] == 288
    assert report["frce_mw"]["match_rate"] == pytest.approx(1.0)
    assert report["imbalance_price_eur_mwh"]["match_rate"] >= 0.8


def test_new_formula_price_decomposition_on_fixtures(fixture_root: Path) -> None:
    qh = load_belgium_new_formula_qh("2026-03-02", "2026-03-05")
    main = np.where(qh["si_mw"] < 0, qh["main_up_eur_mwh"], qh["main_down_eur_mwh"])
    implied = main + qh["incentive_eur_mwh"].fillna(0.0)
    close = np.isclose(implied, qh["imbalance_price_eur_mwh"], atol=0.05)
    assert close.mean() >= 0.95


def test_curve_vs_published_prices_on_synthetic_data() -> None:
    ts = pd.date_range("2026-03-02", periods=4, freq="15min", tz="UTC")
    grid = np.array([0.0, 25.0, 50.0])
    rows = []
    for i, t in enumerate(ts):
        for v, p in zip(grid, [50.0 + i, 100.0 + i, 150.0 + i], strict=True):
            rows.append({"timestamp_utc": t, "direction": "up", "cum_mw": v, "price_eur_mwh": p})
    curves = pd.DataFrame(rows)
    qh = pd.DataFrame(
        {
            "timestamp_utc": ts,
            "si_mw": [-200.0] * 4,
            "afrr_up_mw": [10.0, 20.0, 30.0, 40.0],
            "mfrr_sa_up_mw": 0.0,
            "mfrr_da_up_mw": 0.0,
            "mip_eur_mwh": [100.0, 101.0, 152.0, 153.0],
        }
    )
    report = curve_vs_published_prices(curves, qh)
    assert report["up"]["compared"] == 4
    assert report["up"]["median_abs_diff_eur_mwh"] == pytest.approx(0.0)


@pytest.mark.slow
def test_full_dataset_minute_grids_and_consistency(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EUROFLEX_DATA_DIR", str(REPO_DATA))
    minute = load_belgium_minute(
        "2024-05-21T22:00:00Z", "2026-10-24T22:00:00Z", columns=["price_cum_eur_mwh", "si_cum_mw", "qh_start_utc"]
    )
    grid = minute_grid_report(minute["timestamp_utc"])
    assert grid["unique"] and grid["sorted"] and grid["missing_intervals"] == 0
    qh = load_belgium_qh("2024-05-21T22:00:00Z", "2026-10-24T22:00:00Z", columns=["imbalance_price_eur_mwh", "si_mw"])
    report = last_minute_vs_qh(
        minute, qh, pairs=(("price_cum_eur_mwh", "imbalance_price_eur_mwh"), ("si_cum_mw", "si_mw"))
    )
    price = report["price_cum_eur_mwh_vs_imbalance_price_eur_mwh"]
    assert report["qh_compared"] > 80_000
    assert 0.3 <= price["match_rate"] <= 0.7 and price["match_rate_within_1"] >= 0.65
    assert report["si_cum_mw_vs_si_mw"]["sign_match_rate"] >= 0.9


@pytest.mark.slow
def test_full_dataset_new_formula_coverage(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EUROFLEX_DATA_DIR", str(REPO_DATA))
    qh = load_belgium_new_formula_qh()
    minute = load_belgium_new_formula_minute(columns=["imbalance_price_eur_mwh"])
    assert len(qh) == 20_352 and qh["timestamp_utc"].min() == pd.Timestamp("2026-01-01T00:00:00Z")
    assert len(minute) == 305_265 and minute["timestamp_utc"].max() == pd.Timestamp("2026-07-31T23:59:00Z")
