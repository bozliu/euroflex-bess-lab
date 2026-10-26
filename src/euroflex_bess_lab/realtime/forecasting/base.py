"""Forecast containers and the walk-forward fit/predict loop shared by all forecasters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

import numpy as np
from numpy.typing import NDArray

from ..replay import MarketData
from ..types import PublicationModel
from .features import FeatureConfig, FeatureMatrix, build_features
from .splits import Fold, window_rows

FloatArray = NDArray[np.float64]


@dataclass
class DiscreteForecast:
    """A per-row discrete price distribution: ``values[t, s]`` with probability ``weights[t, s]``.

    ``mean`` is the model's conditional mean (it may differ from ``sum(weights * values)`` because quantile points
    truncate the tails). Rows without a forecast are NaN.
    """

    values: NDArray[np.float32]
    weights: NDArray[np.float32]
    mean: FloatArray
    p_short: FloatArray | None = None

    @classmethod
    def empty(cls, n: int, n_points: int) -> DiscreteForecast:
        return cls(
            values=np.full((n, n_points), np.nan, dtype=np.float32),
            weights=np.full((n, n_points), np.nan, dtype=np.float32),
            mean=np.full(n, np.nan),
            p_short=np.full(n, np.nan),
        )

    @property
    def n_points(self) -> int:
        return int(self.values.shape[1])

    def available(self) -> NDArray[np.bool_]:
        return np.isfinite(self.mean) & np.isfinite(self.values).all(axis=1)


@dataclass
class ForecastFrame:
    """Forecasts for every decision minute of a market (current quarter-hour and the next one).

    ``history_prices`` is the quarter-hour price series the forecaster treats as the published price history of the
    mechanism (strategies may read it only below ``visible_qh_end``, e.g. for climatology and terminal values).
    """

    current: DiscreteForecast
    next: DiscreteForecast
    history_prices: FloatArray
    meta: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class SupportsForecast(Protocol):
    """What the forecast-driven strategies need from a forecaster."""

    name: str

    def predict(
        self,
        market: MarketData,
        publication: PublicationModel | None = None,
        rows: NDArray[np.int64] | None = None,
    ) -> ForecastFrame:
        """Forecasts for every decision minute of ``market`` (or only for ``rows``; other rows are NaN)."""
        ...


class WalkForwardForecaster(ABC):
    """Fits one model set per walk-forward fold and predicts each fold's test window with its own models."""

    name: str = "forecaster"

    def __init__(self, feature_config: FeatureConfig) -> None:
        self.feature_config = feature_config
        self.folds_: list[Fold] = []
        self.fold_models_: list[Any] = []
        self.feature_names_: list[str] = []
        self.fit_publication_: PublicationModel | None = None
        self.fit_log_: list[dict[str, Any]] = []

    # subclass hooks -------------------------------------------------------------------------------------------
    @abstractmethod
    def _fit_fold(
        self, market: MarketData, features: FeatureMatrix, fold: Fold, publication: PublicationModel
    ) -> tuple[Any, dict[str, Any]]:
        """Return the fitted model set for ``fold`` and a small log dict."""

    @abstractmethod
    def _allocate(self, n: int) -> tuple[DiscreteForecast, DiscreteForecast]: ...

    @abstractmethod
    def _predict_rows(
        self,
        models: Any,
        x: NDArray[np.float32],
        rows: NDArray[np.int64],
        current: DiscreteForecast,
        nxt: DiscreteForecast,
    ) -> None: ...

    @abstractmethod
    def history_prices(self, market: MarketData) -> FloatArray: ...

    # public API -----------------------------------------------------------------------------------------------
    def features(self, market: MarketData, publication: PublicationModel) -> FeatureMatrix:
        return build_features(market, publication, self.feature_config)

    def fit(
        self,
        market: MarketData,
        folds: list[Fold],
        publication: PublicationModel | None = None,
        features: FeatureMatrix | None = None,
    ) -> WalkForwardForecaster:
        publication = publication or PublicationModel()
        features = features if features is not None else self.features(market, publication)
        self.feature_names_ = list(features.names)
        self.folds_, self.fold_models_, self.fit_log_ = [], [], []
        for fold in folds:
            models, log = self._fit_fold(market, features, fold, publication)
            self.folds_.append(fold)
            self.fold_models_.append(models)
            self.fit_log_.append({"fold": fold.label, **fold.to_dict(), **log})
        self.fit_publication_ = publication
        return self

    def predict(
        self,
        market: MarketData,
        publication: PublicationModel | None = None,
        rows: NDArray[np.int64] | None = None,
    ) -> ForecastFrame:
        """Predict each fold's test window with its own models; ``rows`` restricts the work to those decision rows
        (live re-planning: features are rebuilt on the whole window, models are evaluated on a few rows only)."""
        if not self.folds_:
            raise RuntimeError(f"{self.name} is not fitted")
        publication = publication or self.fit_publication_ or PublicationModel()
        features = self.features(market, publication)
        if features.names != self.feature_names_:
            raise ValueError("feature layout differs from the one used for fitting")
        current, nxt = self._allocate(market.n_minutes)
        wanted = None if rows is None else np.unique(np.asarray(rows, dtype=np.int64))
        for fold, models in zip(self.folds_, self.fold_models_, strict=True):
            fold_rows = window_rows(market, fold)
            if wanted is not None:
                fold_rows = np.intersect1d(fold_rows, wanted, assume_unique=True)
            if len(fold_rows) == 0:
                continue
            self._predict_rows(models, features.values[fold_rows], fold_rows, current, nxt)
        return ForecastFrame(
            current=current,
            next=nxt,
            history_prices=self.history_prices(market),
            meta={"forecaster": self.name, "folds": [f.label for f in self.folds_]},
        )


def merge_forecasters(parts: list[WalkForwardForecaster]) -> WalkForwardForecaster:
    """Combine forecasters fitted on disjoint folds (e.g. in parallel processes) into one, ordered by test start."""
    if not parts:
        raise ValueError("nothing to merge")
    first = parts[0]
    for other in parts[1:]:
        if type(other) is not type(first) or other.feature_names_ != first.feature_names_:
            raise ValueError("forecasters to merge must share class and feature layout")
    entries = sorted(
        (
            (fold, models, log)
            for part in parts
            for fold, models, log in zip(part.folds_, part.fold_models_, part.fit_log_, strict=True)
        ),
        key=lambda item: item[0].test_start,
    )
    for (a, _, _), (b, _, _) in zip(entries, entries[1:], strict=False):
        if b.test_start < a.test_end:
            raise ValueError("merged folds must have disjoint test windows")
    first.folds_ = [e[0] for e in entries]
    first.fold_models_ = [e[1] for e in entries]
    first.fit_log_ = [e[2] for e in entries]
    return first
