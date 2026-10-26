"""Direct forecaster of the final settlement price of the current and the next quarter-hour."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
from numpy.typing import NDArray

from ..replay import MarketData
from ..types import PublicationModel
from .base import DiscreteForecast, WalkForwardForecaster
from .features import CURRENT_GROUPS, FeatureConfig, FeatureMatrix, baseline_values, minute_target
from .models import DEFAULT_QUANTILES, GBMParams, MeanHead, QuantileHead, SignHead, quantile_weights
from .splits import Fold, training_rows

FloatArray = NDArray[np.float64]

#: Residual baselines: the running quarter-hour-to-date price, then the previous quarter-hour, then the last
#: published final price.
CURRENT_PRICE_BASELINE: tuple[str, ...] = ("cur_price_cum", "cur_price_prev_qh", "hist_price_last")
NEW_FORMULA_PRICE_BASELINE: tuple[str, ...] = ("nf_price_cum", "nf_price_prev_qh", "hist_price_last")


@dataclass(frozen=True)
class PriceForecasterConfig:
    """``mechanism`` names the settlement series to predict (``market.settlement(mechanism)``); ``si_field`` the
    quarter-hour SI used for the P(SI < 0) head (``nf_si_mw`` for the new-formula tables). Heads learn the residual
    to ``baseline`` (feature names, first finite value wins)."""

    mechanism: str = "current"
    features: FeatureConfig = field(default_factory=lambda: FeatureConfig(groups=CURRENT_GROUPS))
    quantiles: tuple[float, ...] = DEFAULT_QUANTILES
    stride: int = 3
    params: GBMParams = field(default_factory=GBMParams)
    si_field: str = "si_mw"
    baseline: tuple[str, ...] = CURRENT_PRICE_BASELINE


@dataclass
class _HeadSet:
    quantile: QuantileHead
    mean: MeanHead
    sign: SignHead


class PriceForecaster(WalkForwardForecaster):
    """Walk-forward gradient boosting on the published final price (quantiles, mean, P(SI < 0))."""

    def __init__(self, config: PriceForecasterConfig | None = None, name: str | None = None) -> None:
        self.config = config or PriceForecasterConfig()
        super().__init__(self.config.features)
        self.name = name or f"price_{self.config.mechanism}"
        self._weights = quantile_weights(self.config.quantiles).astype(np.float32)

    def history_prices(self, market: MarketData) -> FloatArray:
        if self.config.mechanism in market.settlement_prices:
            return np.asarray(market.settlement(self.config.mechanism), dtype=float)
        return np.full(market.n_qh, np.nan)

    def _targets(self, market: MarketData, horizon: int) -> tuple[FloatArray, FloatArray]:
        price = minute_target(market, self.history_prices(market), horizon)
        si = market.qh.get(self.config.si_field)
        if si is None:
            sign = np.full(market.n_minutes, np.nan)
        else:
            si_t = minute_target(market, np.asarray(si, dtype=float), horizon)
            sign = np.where(np.isnan(si_t), np.nan, (si_t < 0.0).astype(float))
        return price, sign

    def _baseline(self, x: NDArray[np.float32]) -> FloatArray:
        return baseline_values(x, self.feature_names_, self.config.baseline)

    def _fit_fold(
        self, market: MarketData, features: FeatureMatrix, fold: Fold, publication: PublicationModel
    ) -> tuple[Any, dict[str, Any]]:
        heads: dict[int, _HeadSet] = {}
        log: dict[str, Any] = {}
        for horizon in (0, 1):
            rows = training_rows(market, fold, horizon=horizon, publication=publication, stride=self.config.stride)
            price, sign = self._targets(market, horizon)
            x = features.values[rows]
            residual = price[rows] - self._baseline(x)
            heads[horizon] = _HeadSet(
                quantile=QuantileHead(self.config.quantiles, self.config.params).fit(x, residual),
                mean=MeanHead(self.config.params).fit(x, residual),
                sign=SignHead(self.config.params).fit(x, sign[rows]),
            )
            log[f"train_rows_h{horizon}"] = int(np.isfinite(price[rows]).sum())
        return heads, log

    def _allocate(self, n: int) -> tuple[DiscreteForecast, DiscreteForecast]:
        k = len(self.config.quantiles)
        return DiscreteForecast.empty(n, k), DiscreteForecast.empty(n, k)

    def _predict_rows(
        self,
        models: Any,
        x: NDArray[np.float32],
        rows: NDArray[np.int64],
        current: DiscreteForecast,
        nxt: DiscreteForecast,
    ) -> None:
        base = self._baseline(x)
        for horizon, target in ((0, current), (1, nxt)):
            head: _HeadSet = models[horizon]
            target.values[rows] = (head.quantile.predict(x) + base[:, None]).astype(np.float32)
            target.weights[rows] = self._weights
            target.mean[rows] = head.mean.predict(x) + base
            if target.p_short is not None:
                target.p_short[rows] = head.sign.predict(x)
