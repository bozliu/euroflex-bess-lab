"""Hold-out (frozen-model) evaluation: the split, the frozen fit and its invariance to post-cutoff data.

Fast tests use the synthetic market and the committed real-data fixture days (``tests/fixtures/public``); the slow
test uses the full ``data/public`` tree (``EUROFLEX_RUN_SLOW=1``).
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams, MarketData, PublicationModel, check_no_lookahead
from euroflex_bess_lab.realtime.forecasting import (
    GBMParams,
    PriceForecaster,
    PriceForecasterConfig,
    PrismForecaster,
    PrismForecasterConfig,
    monthly_folds,
    target_known_minute,
    training_rows,
)
from euroflex_bess_lab.realtime.forecasting.holdout import (
    FrozenHistoryForecaster,
    HoldoutSplit,
    assert_frozen,
    end_of_local_day,
    fit_frozen,
    frozen_training_audit,
    history_profile,
    holdout_split,
    minute_index,
    perturb_unpublished,
    truncate_at,
)
from euroflex_bess_lab.realtime.forecasting.synthetic import synthetic_market
from euroflex_bess_lab.realtime.strategies.forecast_mpc import CachedForecaster, ForecastMPCStrategy, MPCConfig
from euroflex_bess_lab.realtime.strategies.prism import PrismStrategy

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "public"
FAST = GBMParams(max_iter=12, max_leaf_nodes=15, min_samples_leaf=20)
BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)
PUB = PublicationModel(minute_lag=1, qh_final_lag_minutes=5)
UTC = "UTC"


def _ts(value: str) -> pd.Timestamp:
    return pd.Timestamp(value, tz=UTC)


def _synthetic_split(market: MarketData, train_days: int = 6) -> HoldoutSplit:
    """Cutoff at local midnight ``train_days`` after the synthetic market start (Europe/Brussels, CET)."""
    cutoff = market.start_utc + pd.Timedelta(days=train_days) - pd.Timedelta(hours=1)
    return HoldoutSplit("synthetic", cutoff, market.start_utc, market.end_utc)


TINY = GBMParams(max_iter=6, max_leaf_nodes=7, min_samples_leaf=20)


def _price(market: MarketData, params: GBMParams = FAST, stride: int = 4) -> PriceForecaster:
    return PriceForecaster(PriceForecasterConfig(params=params, stride=stride))


def _prism(
    mechanism: Literal["current", "new2026"] = "current",
    components: Literal["published", "reconstructed"] = "published",
    params: GBMParams = FAST,
    stride: int = 4,
) -> PrismForecaster:
    return PrismForecaster(
        PrismForecasterConfig(mechanism=mechanism, components=components, params=params, stride=stride)
    )


# --------------------------------------------------------------------------------------------------------------
# the split


def test_cutoff_is_the_end_of_the_local_day_across_dst() -> None:
    assert end_of_local_day("2025-06-30") == _ts("2025-06-30T22:00")  # CEST
    assert end_of_local_day("2025-12-31") == _ts("2025-12-31T23:00")  # CET
    assert end_of_local_day("2026-03-31") == _ts("2026-03-31T22:00")
    assert end_of_local_day("2026-03-28") == _ts("2026-03-28T23:00")  # last CET day
    assert end_of_local_day("2026-03-29") == _ts("2026-03-29T22:00")  # 23-hour DST day
    assert end_of_local_day("2026-10-24") == _ts("2026-10-24T22:00")
    assert end_of_local_day("2026-10-25") == _ts("2026-10-25T23:00")  # 25-hour day
    assert end_of_local_day(pd.Timestamp("2025-06-30T23:59", tz="Europe/Brussels")) == _ts("2025-06-30T22:00")


def test_holdout_split_matches_the_design() -> None:
    expected = {
        "2025-06-30": ("2024-12-31T23:00", "2025-06-30T22:00", 16, "2025-07"),
        "2025-12-31": ("2025-06-30T22:00", "2025-12-31T23:00", 10, "2026-01"),
        "2026-03-31": ("2025-09-30T22:00", "2026-03-31T22:00", 7, "2026-04"),
    }
    for day, (train_start, cutoff, months, first) in expected.items():
        split = holdout_split(day, "2026-10-24", train_months=6)
        assert split.label == day
        assert split.train_start_utc == _ts(train_start)
        assert split.cutoff_utc == _ts(cutoff)
        assert split.test_end_utc == _ts("2026-10-24T22:00")
        assert len(split.months()) == months and split.months()[0] == first and split.months()[-1] == "2026-10"
        assert split.months_since_cutoff(first) == 1
        fold = split.fold
        assert fold.train_end == fold.test_start == split.cutoff_utc
        assert split.to_dict()["months"] == split.months()
    # clipped to the first day of data, and invalid orderings are rejected
    clipped = holdout_split("2024-07-31", "2024-09-30", train_months=6, data_start="2024-05-21T22:00:00Z")
    assert clipped.train_start_utc == _ts("2024-05-21T22:00")
    with pytest.raises(ValueError):
        holdout_split("2026-10-24", "2026-10-01")
    with pytest.raises(ValueError):
        HoldoutSplit("bad", _ts("2026-01-01T00:07"), _ts("2025-12-01"), _ts("2026-02-01"))
    with pytest.raises(ValueError):
        holdout_split("2026-01-31", "2026-03-31", train_months=0)


@pytest.mark.parametrize("horizon", [0, 1])
def test_frozen_split_never_trains_on_data_after_the_cutoff(horizon: int) -> None:
    market = synthetic_market(days=9, seed=2)
    split = _synthetic_split(market)
    cut = minute_index(market, split.cutoff_utc)
    rows = training_rows(market, split.fold, horizon=horizon, publication=PUB, stride=1)
    assert len(rows) > 0
    assert rows.max() < cut
    assert (target_known_minute(market, horizon, PUB)[rows] <= cut).all()
    # the embargo is tight: the last usable decision row lies within two quarter-hours plus the lag of the cutoff
    assert cut - rows.max() <= 15 * (horizon + 2) + PUB.qh_final_lag_minutes


# --------------------------------------------------------------------------------------------------------------
# the frozen fit


def test_fit_frozen_truncates_at_the_cutoff_and_records_the_proof() -> None:
    market = synthetic_market(days=9, seed=4)
    split = _synthetic_split(market)
    forecaster = _price(market)
    audit = fit_frozen(forecaster, market, split, PUB)
    assert audit["passed"] and audit["n_folds"] == 1 and audit["fit_market_ends_by_cutoff"]
    assert pd.Timestamp(audit["fit_market_end_utc"]) == split.cutoff_utc
    for row in audit["training_rows"]:
        assert pd.Timestamp(row["last_row_utc"]) < split.cutoff_utc
        assert pd.Timestamp(row["last_target_published_utc"]) <= split.cutoff_utc
    assert_frozen(forecaster, split.cutoff_utc)
    assert_frozen(CachedForecaster(forecaster), split.cutoff_utc)  # wrappers are looked through
    with pytest.raises(ValueError):
        assert_frozen(forecaster, split.cutoff_utc - pd.Timedelta(hours=1))  # trained past that cutoff
    # predictions exist only from the cutoff on, i.e. the model never forecasts its own training window
    frame = forecaster.predict(market, PUB)
    cut = minute_index(market, split.cutoff_utc)
    assert np.isnan(frame.current.mean[:cut]).all()
    assert np.isfinite(frame.current.mean[cut:]).mean() > 0.99


def test_assert_frozen_rejects_walk_forward_models() -> None:
    market = synthetic_market(days=12, seed=6)
    folds = monthly_folds(market.start_utc + pd.Timedelta(days=8), market.end_utc, data_start=market.start_utc)
    folds = [*folds, *folds] if len(folds) == 1 else folds
    walk_forward = PriceForecaster(PriceForecasterConfig(params=FAST, stride=6))
    walk_forward.folds_ = folds  # structure only: assert_frozen inspects the folds
    with pytest.raises(ValueError, match="exactly one"):
        assert_frozen(walk_forward, folds[0].test_start)
    with pytest.raises(ValueError, match="no fitted"):
        assert_frozen(object(), folds[0].test_start)


def test_frozen_model_is_unchanged_when_post_cutoff_data_change() -> None:
    """Replacing everything not yet published at T by noise leaves the fitted model bit-identical.

    The fit loads nothing stamped at or after T. That is stricter than the publication model: without truncation
    the last training rows before T could also read day-ahead prices for delivery after T (published the day
    before), and that is the only difference - hiding those prices makes the untruncated fit identical again.
    """
    market = synthetic_market(days=9, seed=7)
    split = _synthetic_split(market)
    cut = minute_index(market, split.cutoff_utc)
    perturbed = perturb_unpublished(market, cut, PUB, seed=11)
    assert not np.allclose(perturbed.minute["si_mw"][cut:], market.minute["si_mw"][cut:])
    da = market.da_price.copy()
    da[cut // 15 :] = np.nan
    no_future_da = MarketData(**{**market.__dict__, "da_price": da})
    makers = (
        lambda m: _price(m, TINY, 6),
        lambda m: _prism(params=TINY, stride=6),
        lambda m: _prism("new2026", "reconstructed", TINY, 6),
    )
    for make in makers:
        reference = make(market)
        fit_frozen(reference, market, split, PUB)
        on_perturbed = make(market)
        fit_frozen(on_perturbed, perturbed, split, PUB)
        untruncated = make(market)  # whole market loaded: the embargo alone keeps post-T targets out
        untruncated.fit(no_future_da, [split.fold], PUB)
        a = reference.predict(market, PUB)
        for other in (on_perturbed, untruncated):
            b = other.predict(market, PUB)
            np.testing.assert_array_equal(a.current.mean, b.current.mean)
            np.testing.assert_array_equal(a.next.values, b.next.values)
            np.testing.assert_array_equal(a.current.p_short, b.current.p_short)


def test_perturb_unpublished_follows_the_publication_model() -> None:
    market = synthetic_market(days=3, seed=9)
    cut = 1440 + 37
    perturbed = perturb_unpublished(market, cut, PUB, seed=1)
    m_vis, q_vis = PUB.visible_minute_end(cut), PUB.visible_qh_end(cut)
    for name, values in market.minute.items():
        np.testing.assert_array_equal(perturbed.minute[name][:m_vis], values[:m_vis])
        assert not np.allclose(perturbed.minute[name][m_vis:], values[m_vis:], equal_nan=True)
    for name, values in market.qh.items():
        np.testing.assert_array_equal(perturbed.qh[name][:q_vis], values[:q_vis])
    for name, values in market.settlement_prices.items():
        np.testing.assert_array_equal(perturbed.settlement_prices[name][:q_vis], values[:q_vis])
    visible_da = market.da_visible_from_minute <= cut
    np.testing.assert_array_equal(perturbed.da_price[visible_da], market.da_price[visible_da])
    assert truncate_at(market, market.end_utc + pd.Timedelta(days=1)) is market
    assert truncate_at(market, market.start_utc + pd.Timedelta(days=1)).n_minutes == 1440


def test_frozen_history_forecaster_freezes_only_the_price_profile() -> None:
    market = synthetic_market(days=10, seed=12)
    split = _synthetic_split(market, train_days=7)
    forecaster = _price(market)
    fit_frozen(forecaster, market, split, PUB)
    history = forecaster.history_prices(market)
    profile = history_profile(history, market, split.cutoff_utc, days=5, gap_days=1)
    assert len(profile) == 5 * 96
    k_cut = minute_index(market, split.cutoff_utc) // 15
    np.testing.assert_array_equal(profile, history[k_cut - 6 * 96 : k_cut - 96])
    wrapped = FrozenHistoryForecaster(CachedForecaster(forecaster), split.cutoff_utc, profile)
    a, b = forecaster.predict(market, PUB), wrapped.predict(market, PUB)
    np.testing.assert_array_equal(a.current.mean, b.current.mean)  # forecasts untouched
    np.testing.assert_array_equal(b.history_prices[:k_cut], history[:k_cut])  # the published past is kept
    after = np.arange(market.n_qh - k_cut)
    np.testing.assert_array_equal(b.history_prices[k_cut:], profile[after % len(profile)])
    assert b.meta["frozen_history"]["profile_days"] == 5
    with pytest.raises(ValueError):
        history_profile(history, market, split.cutoff_utc, days=7, gap_days=1)  # not covered by the market
    with pytest.raises(ValueError):
        FrozenHistoryForecaster(forecaster, split.cutoff_utc, profile[:50])


def test_frozen_strategies_pass_the_lookahead_audit_after_the_cutoff() -> None:
    market = synthetic_market(days=9, seed=14)
    split = _synthetic_split(market)
    price, prism = _price(market), _prism()
    fit_frozen(price, market, split, PUB)
    fit_frozen(prism, market, split, PUB)
    profile = history_profile(price.history_prices(market), market, split.cutoff_utc, days=4, gap_days=1)
    window = market.slice(split.cutoff_utc - pd.Timedelta(hours=11), split.cutoff_utc + pd.Timedelta(days=1))
    risk = MPCConfig(risk="quantile")
    cases = [
        lambda: ForecastMPCStrategy(CachedForecaster(price), risk),
        lambda: PrismStrategy(CachedForecaster(prism), risk),
        lambda: ForecastMPCStrategy(FrozenHistoryForecaster(price, split.cutoff_utc, profile), risk),
    ]
    for make in cases:
        for cut in (700, 1100):  # both after the cutoff (minute 660 of the window)
            audit = check_no_lookahead(make, window, BATTERY, cut_minute=cut, publication=PUB)
            assert audit.passed, audit


# --------------------------------------------------------------------------------------------------------------
# real Elia data


@pytest.fixture
def fixture_market(monkeypatch: pytest.MonkeyPatch) -> MarketData:
    if not (FIXTURES / "belgium" / "minute_2026.parquet").exists():
        pytest.skip("public-data fixtures not available")
    monkeypatch.setenv("EUROFLEX_DATA_DIR", str(FIXTURES))
    return MarketData.from_public("2026-03-02T00:00:00Z", "2026-03-05T00:00:00Z")


def test_frozen_fit_on_real_fixture_days_is_invariant_and_causal(fixture_market: MarketData) -> None:
    market = fixture_market
    split = replace(
        holdout_split("2026-03-03", "2026-03-04"), train_start_utc=market.start_utc, test_end_utc=market.end_utc
    )
    assert split.cutoff_utc == _ts("2026-03-03T23:00")
    cut = minute_index(market, split.cutoff_utc)
    zero = _prism("new2026", "reconstructed")
    audit = fit_frozen(zero, market, split, PUB)
    assert audit["passed"]
    again = _prism("new2026", "reconstructed")
    fit_frozen(again, perturb_unpublished(market, cut, PUB, seed=5), split, PUB)
    a, b = zero.predict(market, PUB), again.predict(market, PUB)
    np.testing.assert_array_equal(a.current.mean, b.current.mean)
    # the zero-history fit never loads a new-formula price: the fit market has none of them before T either way
    assert frozen_training_audit(zero, truncate_at(market, split.cutoff_utc), split, PUB)["passed"]
    test = market.slice(split.cutoff_utc - pd.Timedelta(hours=1), market.end_utc)
    for cut_minute in (31, 900):
        result = check_no_lookahead(
            lambda: PrismStrategy(CachedForecaster(zero), MPCConfig(risk="quantile")),
            test,
            BATTERY,
            cut_minute=cut_minute,
            mechanism="new2026",
            publication=PUB,
        )
        assert result.passed, result


@pytest.mark.slow
def test_frozen_fit_on_full_data_is_invariant_and_causal() -> None:
    split = replace(holdout_split("2025-06-30", "2025-07-14"), train_start_utc=_ts("2025-06-09T22:00"))
    market = MarketData.from_public(split.train_start_utc - pd.Timedelta(days=2), split.test_end_utc)
    cut = minute_index(market, split.cutoff_utc)
    params = GBMParams(max_iter=40)
    first = PriceForecaster(PriceForecasterConfig(params=params, stride=3))
    assert fit_frozen(first, market, split, PUB)["passed"]
    second = PriceForecaster(PriceForecasterConfig(params=params, stride=3))
    fit_frozen(second, perturb_unpublished(market, cut, PUB, seed=3), split, PUB)
    a, b = first.predict(market, PUB), second.predict(market, PUB)
    np.testing.assert_array_equal(a.current.mean, b.current.mean)
    window = market.slice("2025-07-06T22:00:00Z", "2025-07-09T22:00:00Z")
    for cut_minute in (37, 1503, 2999):
        audit = check_no_lookahead(
            lambda: ForecastMPCStrategy(CachedForecaster(first), MPCConfig(risk="quantile")),
            window,
            BATTERY,
            cut_minute=cut_minute,
            publication=PUB,
        )
        assert audit.passed, audit
