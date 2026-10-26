"""Gradient-boosting forecast heads: quantiles, conditional mean and the probability of a short system.

scikit-learn is an optional extra (``pip install 'euroflex-bess-lab[ml]'``); it is imported lazily so the core
package works without it. All heads degrade to empirical constants when the training set is too small or
degenerate (e.g. the first walk-forward month), so they never fail on thin data.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
from numpy.typing import NDArray

FloatArray = NDArray[np.float64]

DEFAULT_QUANTILES: tuple[float, ...] = (0.1, 0.25, 0.5, 0.75, 0.9)
MIN_ROWS = 200


def require_sklearn() -> Any:
    """Import :mod:`sklearn.ensemble` or raise an actionable error."""
    try:
        import sklearn.ensemble as ensemble
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise ImportError(
            "realtime forecasting models need scikit-learn; install the optional extra: "
            "pip install 'euroflex-bess-lab[ml]'"
        ) from exc
    return ensemble


def quantile_weights(levels: tuple[float, ...] | list[float] | FloatArray) -> FloatArray:
    """Probability mass represented by each quantile level (mid-point rule; sums to one)."""
    q = np.asarray(levels, dtype=float)
    if q.ndim != 1 or len(q) == 0 or np.any(np.diff(q) <= 0) or q[0] <= 0 or q[-1] >= 1:
        raise ValueError("quantile levels must be strictly increasing inside (0, 1)")
    edges = np.concatenate([[0.0], 0.5 * (q[1:] + q[:-1]), [1.0]])
    return np.diff(edges)


@dataclass(frozen=True)
class GBMParams:
    """Histogram gradient-boosting settings (fixed a priori; not tuned on any test window)."""

    max_iter: int = 200
    learning_rate: float = 0.08
    max_leaf_nodes: int = 31
    min_samples_leaf: int = 80
    l2_regularization: float = 1.0
    max_bins: int = 255
    random_state: int = 0

    def kwargs(self) -> dict[str, Any]:
        out = asdict(self)
        out["early_stopping"] = False
        return out


def _clean(x: NDArray[np.float32], y: FloatArray) -> tuple[NDArray[np.float32], FloatArray]:
    ok = np.isfinite(y)
    return x[ok], y[ok]


def _usable_columns(x: NDArray[np.float32]) -> NDArray[np.int64]:
    """Columns with at least one finite value (all-NaN columns break histogram binning)."""
    if len(x) == 0:
        return np.arange(x.shape[1], dtype=np.int64)
    return np.flatnonzero(np.isfinite(x).any(axis=0)).astype(np.int64)


class QuantileHead:
    """One quantile-loss gradient-boosting model per level; predictions are sorted (no crossing)."""

    def __init__(self, levels: tuple[float, ...] = DEFAULT_QUANTILES, params: GBMParams | None = None) -> None:
        self.levels = tuple(float(q) for q in levels)
        self.params = params or GBMParams()
        self.models: list[Any] = []
        self.constant: FloatArray | None = None
        self.columns: NDArray[np.int64] | None = None

    def fit(self, x: NDArray[np.float32], y: FloatArray) -> QuantileHead:
        x, y = _clean(x, y)
        self.models = []
        self.constant = None
        if len(y) < MIN_ROWS:
            self.constant = np.quantile(y, self.levels) if len(y) else np.zeros(len(self.levels))
            return self
        ensemble = require_sklearn()
        self.columns = _usable_columns(x)
        for q in self.levels:
            model = ensemble.HistGradientBoostingRegressor(loss="quantile", quantile=q, **self.params.kwargs())
            model.fit(x[:, self.columns], y)
            self.models.append(model)
        return self

    def predict(self, x: NDArray[np.float32]) -> FloatArray:
        if self.constant is not None:
            return np.tile(self.constant, (len(x), 1))
        if not self.models or self.columns is None:
            raise RuntimeError("QuantileHead is not fitted")
        xc = x[:, self.columns]
        out = np.column_stack([m.predict(xc) for m in self.models])
        out.sort(axis=1)
        return out


class MeanHead:
    """Squared-error gradient boosting for the conditional mean (the risk-neutral price)."""

    def __init__(self, params: GBMParams | None = None) -> None:
        self.params = params or GBMParams()
        self.model: Any = None
        self.constant: float | None = None
        self.columns: NDArray[np.int64] | None = None

    def fit(self, x: NDArray[np.float32], y: FloatArray) -> MeanHead:
        x, y = _clean(x, y)
        self.model, self.constant = None, None
        if len(y) < MIN_ROWS:
            self.constant = float(np.mean(y)) if len(y) else 0.0
            return self
        ensemble = require_sklearn()
        self.columns = _usable_columns(x)
        self.model = ensemble.HistGradientBoostingRegressor(loss="squared_error", **self.params.kwargs())
        self.model.fit(x[:, self.columns], y)
        return self

    def predict(self, x: NDArray[np.float32]) -> FloatArray:
        if self.constant is not None:
            return np.full(len(x), self.constant)
        if self.model is None or self.columns is None:
            raise RuntimeError("MeanHead is not fitted")
        return np.asarray(self.model.predict(x[:, self.columns]), dtype=float)


class SignHead:
    """Gradient-boosting classifier for a binary event (here: the final system imbalance is negative)."""

    def __init__(self, params: GBMParams | None = None) -> None:
        self.params = params or GBMParams()
        self.model: Any = None
        self.constant: float | None = None
        self.columns: NDArray[np.int64] | None = None

    def fit(self, x: NDArray[np.float32], y: FloatArray) -> SignHead:
        x, y = _clean(x, y)
        self.model, self.constant = None, None
        labels = y > 0.5
        if len(y) < MIN_ROWS or labels.all() or not labels.any():
            self.constant = float(labels.mean()) if len(y) else 0.5
            return self
        ensemble = require_sklearn()
        self.columns = _usable_columns(x)
        self.model = ensemble.HistGradientBoostingClassifier(loss="log_loss", **self.params.kwargs())
        self.model.fit(x[:, self.columns], labels.astype(int))
        return self

    def predict(self, x: NDArray[np.float32]) -> FloatArray:
        if self.constant is not None:
            return np.full(len(x), self.constant)
        if self.model is None or self.columns is None:
            raise RuntimeError("SignHead is not fitted")
        return np.asarray(self.model.predict_proba(x[:, self.columns])[:, 1], dtype=float)
