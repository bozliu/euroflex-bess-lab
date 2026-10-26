"""Causal features and walk-forward forecasters for Belgian real-time imbalance trading.

- :mod:`.features`: per-decision-minute feature matrix from visible information only;
- :mod:`.splits`: monthly walk-forward folds with an embargo on target publication;
- :mod:`.price`: direct forecaster of the final settlement price (quantiles, mean, P(SI < 0));
- :mod:`.state`: PRISM state forecaster (P(SI < 0) x regime components priced by the mechanism operator);
- :mod:`.fusion`: precision-weighted fusion with the live running quarter-hour-to-date price;
- :mod:`.skill`: pinball loss, CRPS, Brier score per minute-in-quarter-hour;
- :mod:`.climatology`, :mod:`.distribution`: helpers used by the forecast-driven strategies;
- :mod:`.synthetic`: a synthetic market with the public-data field names (tests and examples).

scikit-learn (optional extra ``ml``) is imported only when a model is fitted.
"""

from __future__ import annotations

from .base import DiscreteForecast, ForecastFrame, SupportsForecast, WalkForwardForecaster, merge_forecasters
from .climatology import CLIMATOLOGY_LEVELS, slot_climatology, trailing_mean
from .distribution import crps_discrete, distribution_mean, tail_mean, weighted_quantile
from .features import (
    ALL_GROUPS,
    CURRENT_GROUPS,
    INVARIANT_GROUPS,
    NEW_FORMULA_GROUPS,
    FeatureConfig,
    FeatureMatrix,
    baseline_values,
    build_features,
    history_price_series,
    minute_target,
    target_known_minute,
)
from .fusion import RunningEstimateFusion, running_error_variance, running_estimate
from .models import DEFAULT_QUANTILES, GBMParams, MeanHead, QuantileHead, SignHead, quantile_weights, require_sklearn
from .price import CURRENT_PRICE_BASELINE, NEW_FORMULA_PRICE_BASELINE, PriceForecaster, PriceForecasterConfig
from .skill import forecast_skill, pinball_loss
from .splits import Fold, monthly_folds, single_fold, training_rows, window_rows
from .state import PrismForecaster, PrismForecasterConfig, regime_components

__all__ = [
    "ALL_GROUPS",
    "CLIMATOLOGY_LEVELS",
    "CURRENT_GROUPS",
    "CURRENT_PRICE_BASELINE",
    "DEFAULT_QUANTILES",
    "INVARIANT_GROUPS",
    "NEW_FORMULA_GROUPS",
    "NEW_FORMULA_PRICE_BASELINE",
    "DiscreteForecast",
    "FeatureConfig",
    "FeatureMatrix",
    "Fold",
    "ForecastFrame",
    "GBMParams",
    "MeanHead",
    "PriceForecaster",
    "PriceForecasterConfig",
    "PrismForecaster",
    "PrismForecasterConfig",
    "QuantileHead",
    "RunningEstimateFusion",
    "SignHead",
    "SupportsForecast",
    "WalkForwardForecaster",
    "baseline_values",
    "build_features",
    "crps_discrete",
    "distribution_mean",
    "forecast_skill",
    "merge_forecasters",
    "history_price_series",
    "minute_target",
    "monthly_folds",
    "pinball_loss",
    "quantile_weights",
    "regime_components",
    "running_error_variance",
    "running_estimate",
    "require_sklearn",
    "single_fold",
    "slot_climatology",
    "tail_mean",
    "target_known_minute",
    "trailing_mean",
    "training_rows",
    "weighted_quantile",
    "window_rows",
]
