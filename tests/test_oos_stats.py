"""Statistics of the out-of-sample evaluation: block bootstrap, paired comparisons, monthly tables, model ageing."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams
from euroflex_bess_lab.realtime.forecasting.holdout import (
    HoldoutSplit,
    ageing_rows,
    circular_block_bootstrap_indices,
    daily_pnl_series,
    evaluate_holdout,
    holdout_split,
    monthly_rows,
    paired_comparison,
    to_jsonable,
    trim_to_test,
    validate_evaluation,
)

BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)


def _days(n: int, start: str = "2026-01-01") -> pd.Index:
    return pd.Index([d.strftime("%Y-%m-%d") for d in pd.date_range(start, periods=n, freq="D")], name="date")


def test_circular_block_bootstrap_draws_wrapped_consecutive_blocks() -> None:
    rng = np.random.default_rng(0)
    idx = circular_block_bootstrap_indices(30, 7, 200, rng)
    assert idx.shape == (200, 30) and idx.min() >= 0 and idx.max() < 30
    steps = (np.diff(idx, axis=1) % 30)[:, :6]  # inside the first block every step is +1 (mod n)
    assert (steps == 1).all()
    # every observation is drawn with (nearly) equal frequency: the circular scheme has no end effects
    counts = np.bincount(circular_block_bootstrap_indices(30, 7, 4000, rng).ravel(), minlength=30)
    assert counts.min() / counts.max() > 0.9
    iid = circular_block_bootstrap_indices(5, 1, 10, rng)
    assert iid.shape == (10, 5)
    assert circular_block_bootstrap_indices(5, 50, 3, rng).shape == (3, 5)  # block longer than n is capped
    with pytest.raises(ValueError):
        circular_block_bootstrap_indices(0, 7, 10, rng)


def test_paired_comparison_of_a_constant_daily_edge() -> None:
    days = _days(70)
    reference = pd.Series(np.linspace(50.0, 150.0, 70), index=days)
    arm = reference + 10.0
    result = paired_comparison(arm, reference, arm="frozen", reference="rule", n_boot=500, seed=1)
    assert result.days == 70
    assert result.diff_eur_per_mw_year == pytest.approx(3652.5)
    assert result.diff_ci_eur_per_mw_year == pytest.approx((3652.5, 3652.5))
    assert result.ratio == pytest.approx(arm.sum() / reference.sum())
    assert result.ratio_ci[0] <= result.ratio <= result.ratio_ci[1]
    assert result.p_value == pytest.approx(1 / 501)
    assert result.months == 3 and result.months_won == 3 and result.share_months_won == 1.0
    out = result.to_dict()
    assert out["uplift_pct"] == pytest.approx(100 * (result.ratio - 1))
    assert out["block_days"] == 7 and out["n_boot"] == 500


def test_paired_comparison_without_an_edge_covers_zero() -> None:
    days = _days(84)
    reference = pd.Series(100.0 + 30.0 * np.sin(np.arange(84) / 5.0), index=days)
    arm = reference + np.where(np.arange(84) % 2 == 0, 8.0, -8.0)
    result = paired_comparison(arm, reference, n_boot=2000, seed=2)
    lo, hi = result.diff_ci_eur_per_mw_year
    assert lo < 0.0 < hi
    assert result.ratio_ci[0] < 1.0 < result.ratio_ci[1]
    assert 0.2 < result.p_value < 0.8


def test_block_bootstrap_widens_the_interval_for_serially_dependent_days() -> None:
    rng = np.random.default_rng(3)
    n = 360
    shocks = rng.normal(0.0, 10.0, n)
    persistent = np.zeros(n)
    for i in range(1, n):  # AR(1) daily differences, as for strategies that share regimes
        persistent[i] = 0.9 * persistent[i - 1] + shocks[i]
    days = _days(n, "2025-07-01")
    reference = pd.Series(100.0 + rng.normal(0.0, 5.0, n), index=days)
    arm = reference + 2.0 + persistent
    weekly = paired_comparison(arm, reference, block_days=7, n_boot=3000, seed=4)
    iid = paired_comparison(arm, reference, block_days=1, n_boot=3000, seed=4)
    width = np.diff(weekly.diff_ci_eur_per_mw_year)[0]
    assert width > 1.5 * np.diff(iid.diff_ci_eur_per_mw_year)[0]


def test_paired_comparison_aligns_on_common_finite_days() -> None:
    a = pd.Series([1.0, 2.0, np.nan, 4.0], index=_days(4))
    b = pd.Series([0.5, 1.0, 1.0], index=_days(3, "2026-01-02"))
    result = paired_comparison(a, b, n_boot=50)
    assert result.days == 2  # 01-02 and 01-04: 01-01 is missing in b, 01-03 is NaN in a
    with pytest.raises(ValueError):
        paired_comparison(a, pd.Series([1.0], index=_days(1, "2027-01-01")), n_boot=10)
    with pytest.raises(ValueError):
        paired_comparison(a, b, confidence=1.0)


def _ledger(times: pd.DatetimeIndex, pnl_per_qh: float) -> pd.DataFrame:
    n = len(times)
    discharged = np.full(n, 0.1)
    return pd.DataFrame(
        {
            "timestamp_utc": times,
            "energy_mwh": np.zeros(n),
            "discharged_mwh": discharged,
            "charged_mwh": discharged,
            "base_price_eur_mwh": np.full(n, 50.0),
            "price_eur_mwh": np.full(n, 50.0),
            "price_missing": np.zeros(n, dtype=bool),
            "revenue_eur": np.full(n, pnl_per_qh) + 0.8,
            "degradation_eur": np.full(n, 0.8),
            "pnl_eur": np.full(n, pnl_per_qh),
            "soc_end_mwh": np.ones(n),
        }
    )


@pytest.fixture
def synthetic_evaluation() -> tuple[dict[str, pd.DataFrame], HoldoutSplit]:
    split = holdout_split("2026-02-28", "2026-04-15")  # March (with the DST switch) and half of April
    times = pd.date_range(split.cutoff_utc - pd.Timedelta(days=3), split.test_end_utc, freq="15min", inclusive="left")
    pnl = {"oracle_qh": 3.0, "threshold_price": 1.0, "threshold_si": 0.5, "frozen": 1.5, "wf": 1.6}
    return {name: _ledger(times, value) for name, value in pnl.items()}, split


def test_monthly_rows_and_ageing(synthetic_evaluation: tuple[dict[str, pd.DataFrame], HoldoutSplit]) -> None:
    ledgers, split = synthetic_evaluation
    trimmed = {k: trim_to_test(v, split) for k, v in ledgers.items()}
    assert trimmed["frozen"]["timestamp_utc"].min() == split.cutoff_utc
    rows = monthly_rows(trimmed, split, power_mw=1.0)
    assert [r["month"] for r in rows] == ["2026-03", "2026-04"]
    march, april = rows
    assert march["days"] == 31 and not march["partial"] and march["months_since_cutoff"] == 1
    assert april["days"] == 15 and april["partial"] and april["months_since_cutoff"] == 2
    assert march["pnl_eur"]["frozen"] == pytest.approx(1.5 * (31 * 96 - 4))  # 23-hour day on 29 March
    per_year = 1.5 * 4 * 24 * 365.25
    assert march["eur_per_mw_year"]["frozen"] == pytest.approx(per_year)
    assert april["eur_per_mw_year"]["frozen"] == pytest.approx(per_year)
    assert march["capture_vs_oracle"]["frozen"] == pytest.approx(0.5)
    assert "oracle_qh" not in march["capture_vs_oracle"]
    ageing = ageing_rows(rows, {"frozen": "wf"})
    assert [r["ratio"] for r in ageing] == pytest.approx([1.5 / 1.6, 1.5 / 1.6])
    daily = daily_pnl_series(trimmed["frozen"])
    assert daily.index[0] == "2026-03-01" and len(daily) == 46


def test_evaluate_holdout_block(synthetic_evaluation: tuple[dict[str, pd.DataFrame], HoldoutSplit]) -> None:
    ledgers, split = synthetic_evaluation
    block = evaluate_holdout(
        ledgers,
        split,
        battery=BATTERY,
        mechanism="current",
        frozen_arms=["frozen"],
        walk_forward={"frozen": "wf"},
        thresholds=["threshold_price", "threshold_si"],
        n_boot=200,
    )
    assert validate_evaluation(block) == []
    assert block["best_threshold"] == "threshold_price"
    assert block["id"] == "current_2026-02-28"
    frozen = block["arms"]["frozen"]
    assert frozen["eur_per_mw_year"] == pytest.approx(1.5 * 4 * 24 * 365.25)
    assert frozen["capture_vs_oracle"] == pytest.approx(0.5)
    vs_rule = block["comparisons"]["frozen"]["vs_best_threshold"]
    assert vs_rule["uplift_pct"] == pytest.approx(50.0)
    assert vs_rule["months_won"] == 2 and vs_rule["months"] == 2
    vs_wf = block["comparisons"]["frozen"]["vs_walk_forward"]
    assert vs_wf["ratio"] == pytest.approx(1.5 / 1.6)
    assert set(block["comparisons"]["frozen"]["vs_thresholds"]) == {"threshold_price", "threshold_si"}
    assert "wf" in block["comparisons"]  # the walk-forward arm is compared with the rules too
    assert [r["months_since_cutoff"] for r in block["ageing"]] == [1, 2]
    with pytest.raises(ValueError):
        evaluate_holdout(
            ledgers,
            split,
            battery=BATTERY,
            mechanism="current",
            frozen_arms=["frozen"],
            walk_forward={},
            thresholds=[],
        )


def test_to_jsonable_handles_numpy_and_non_finite_values() -> None:
    value = {
        "a": np.float32(1.5),
        "b": np.int64(3),
        "c": [np.nan, math.inf, 2.0],
        "d": np.array([1, 2]),
        "e": np.bool_(True),
        "f": pd.Timestamp("2026-01-01", tz="UTC"),
        "g": (1, "x"),
    }
    out = to_jsonable(value)
    assert out == {
        "a": 1.5,
        "b": 3,
        "c": [None, None, 2.0],
        "d": [1, 2],
        "e": True,
        "f": "2026-01-01T00:00:00+00:00",
        "g": [1, "x"],
    }
