from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.valuation import mechanism_decomposition, reprice_ledger, value_waterfall
from euroflex_bess_lab.valuation.plots import plot_waterfall
from euroflex_bess_lab.valuation.report import render_waterfall
from euroflex_bess_lab.valuation.revenue import DAYS_PER_YEAR

GRID = pd.date_range("2026-03-01 23:00", "2026-03-11 23:00", freq="15min", inclusive="left", tz="UTC")
PER_YEAR = 96 * DAYS_PER_YEAR  # quarter-hours per year


def ledger(pnl: float, degradation: float = 0.0, grid: pd.DatetimeIndex = GRID) -> pd.DataFrame:
    return pd.DataFrame(
        {"timestamp_utc": grid, "revenue_eur": pnl + degradation, "degradation_eur": degradation, "pnl_eur": pnl}
    )


def test_waterfall_deltas_are_exact_and_sum_to_the_final_level() -> None:
    chain = {
        "oracle": ledger(10.0),
        "strategy": ledger(7.0),
        "mechanism": ledger(8.0),
        "impact": ledger(6.0),
        "availability": ledger(5.5),
        "degradation": ledger(5.0),
    }
    result = value_waterfall(chain, power_mw=2.0)
    table = result.table
    assert table["kind"].tolist() == ["anchor"] + ["effect"] * 5 + ["total"]
    expected = {"strategy": -3.0, "mechanism": 1.0, "impact": -2.0, "availability": -0.5, "degradation": -0.5}
    for key, delta in expected.items():
        assert result.effect(key) == pytest.approx(delta * PER_YEAR / 2.0, rel=1e-9)
    assert result.anchor_eur_per_mw_year == pytest.approx(10.0 * PER_YEAR / 2.0)
    effects = table.loc[table["kind"] == "effect", "delta_eur_per_mw_year"].sum()
    assert result.anchor_eur_per_mw_year + effects == pytest.approx(result.final_eur_per_mw_year)
    assert result.final_eur_per_mw_year == pytest.approx(5.0 * PER_YEAR / 2.0)
    assert result.years_equivalent == pytest.approx(10.0 / DAYS_PER_YEAR)
    assert set(result.daily["key"]) == set(chain)
    assert "Decision / forecast gap" in render_waterfall(result)


def test_missing_steps_are_skipped_and_ledgers_are_aligned_on_common_intervals() -> None:
    short = GRID[96:]
    result = value_waterfall(
        {"oracle": ledger(10.0), "strategy": ledger(7.0, grid=short), "impact": ledger(6.0)}, power_mw=1.0
    )
    assert result.table["key"].tolist() == ["oracle", "strategy", "impact", "total"]
    assert result.effect("impact") == pytest.approx(-1.0 * PER_YEAR)
    assert result.n_intervals == len(short)
    assert result.dropped_intervals == {"oracle": 96, "strategy": 0, "impact": 96}
    assert any("common intervals" in note for note in result.notes)
    without_oracle = value_waterfall({"strategy": ledger(7.0), "mechanism": ledger(8.0)}, power_mw=1.0)
    assert without_oracle.table["kind"].iloc[0] == "anchor" and "anchor is `strategy`" in without_oracle.notes[0]
    with pytest.raises(ValueError, match="not waterfall steps"):
        value_waterfall({"oracle": ledger(1.0), "other": ledger(1.0)}, power_mw=1.0)


def test_gross_basis_shows_degradation_last_and_analytic_availability_scales_later_steps() -> None:
    chain = {"oracle": ledger(10.0, degradation=2.0), "strategy": ledger(7.0, degradation=1.0)}
    gross = value_waterfall(chain, power_mw=1.0, basis="gross")
    assert gross.table["key"].tolist() == ["oracle", "strategy", "degradation_cost", "total"]
    assert gross.anchor_eur_per_mw_year == pytest.approx(12.0 * PER_YEAR)
    assert gross.effect("degradation_cost") == pytest.approx(-1.0 * PER_YEAR)
    assert gross.final_eur_per_mw_year == pytest.approx(7.0 * PER_YEAR)

    analytic = value_waterfall(
        {"oracle": ledger(10.0), "strategy": ledger(8.0), "degradation": ledger(6.0)},
        power_mw=1.0,
        availability_factor=0.9,
    )
    rows = analytic.table.set_index("key")
    assert rows.loc["availability", "source"] == "analytic"
    assert analytic.effect("availability") == pytest.approx(-0.1 * 8.0 * PER_YEAR)
    assert analytic.effect("degradation") == pytest.approx(0.9 * (6.0 - 8.0) * PER_YEAR)
    assert analytic.final_eur_per_mw_year == pytest.approx(0.9 * 6.0 * PER_YEAR)


def test_repricing_and_mechanism_decomposition() -> None:
    grid = GRID[:8]
    energy = np.array([0.25, -0.25, 0.0, 0.25, -0.25, 0.25, 0.0, -0.25])
    current = pd.DataFrame(
        {"timestamp_utc": grid, "energy_mwh": energy, "price_eur_mwh": 100.0, "degradation_eur": 0.0}
    )
    new_prices = pd.DataFrame({"timestamp_utc": grid[:-1], "imbalance_price_eur_mwh": 80.0})
    repriced = reprice_ledger(current, new_prices, price_column="imbalance_price_eur_mwh")
    assert repriced["revenue_eur"].tolist()[:7] == pytest.approx((energy[:7] * 80.0).tolist())
    assert bool(repriced["price_missing"].iloc[-1]) and repriced["revenue_eur"].iloc[-1] == 0.0
    series = pd.Series(80.0, index=grid)
    assert reprice_ledger(current, series)["revenue_eur"].tolist() == pytest.approx((energy * 80.0).tolist())

    new = current.assign(energy_mwh=energy * 1.5, price_eur_mwh=80.0)
    result = mechanism_decomposition(
        current,
        new,
        new_prices.assign(imbalance_price_eur_mwh=80.0),
        power_mw=1.0,
        price_column="imbalance_price_eur_mwh",
    )
    total_change = result.final_eur_per_mw_year - result.anchor_eur_per_mw_year
    assert result.effect("repriced") + result.effect("new") == pytest.approx(total_change)
    with pytest.raises(ValueError, match="energy_mwh"):
        reprice_ledger(ledger(1.0), series)


def test_waterfall_plot_is_written(tmp_path) -> None:
    result = value_waterfall({"oracle": ledger(10.0), "strategy": ledger(7.0), "mechanism": ledger(8.0)}, power_mw=1.0)
    path = plot_waterfall(result, tmp_path / "figures" / "waterfall.png", subtitle="test")
    assert path.exists() and path.stat().st_size > 10_000
    assert result.plot(tmp_path / "method.png").exists()
