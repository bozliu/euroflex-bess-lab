"""SCALES-L state forecaster (earlier name PRISM): forecast the mechanism-invariant state, then price it through
the mechanism operator.

Both Elia formulas share one structure: the settlement price is an *upward* component when the final system
imbalance is negative and a *downward* component otherwise,

- ``current``: ``up = MIP + alpha``, ``down = MDP - alpha`` (:class:`CurrentAlphaMechanism`);
- ``new2026``: ``up = max(main_up, incentive)``, ``down = min(main_down, incentive)`` with ``main`` the
  volume-weighted aFRR/mFRR price (:class:`NewWeightedMechanism2026`).

The forecaster learns (1) ``P(SI < 0)`` - a pure state quantity that does not depend on the price formula - and
(2) the distribution of each component conditional on its regime. The price distribution is the mixture
``p * D_up + (1 - p) * D_down``, i.e. the operator applied to the forecast state.

Zero-history transfer: with ``components="reconstructed"`` the new-2026 components are rebuilt from current-design
public state (ODS166 product prices, ODS127 volumes) by
:func:`~euroflex_bess_lab.realtime.mechanisms.reconstruct_new_formula_history`, so a new-formula forecaster can be
trained before a single new-formula price has been published. This is a first-order counterfactual: it ignores how
market participants would have reacted to the new price, and the rare incentive component is not reconstructed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ..mechanisms import reconstruct_new_formula_history
from ..replay import MarketData
from ..types import PublicationModel
from .base import DiscreteForecast, WalkForwardForecaster
from .features import (
    CURRENT_GROUPS,
    INVARIANT_GROUPS,
    FeatureConfig,
    FeatureMatrix,
    baseline_values,
    history_price_series,
    minute_target,
)
from .models import DEFAULT_QUANTILES, GBMParams, MeanHead, QuantileHead, SignHead, quantile_weights
from .splits import Fold, training_rows

FloatArray = NDArray[np.float64]


def regime_components(market: MarketData, mechanism: str, components: str) -> tuple[FloatArray, FloatArray]:
    """Quarter-hour ``(up, down)`` component prices: the settlement price if the system were short / long."""
    qh = market.qh
    n = market.n_qh

    def col(name: str) -> FloatArray:
        values = qh.get(name)
        return np.full(n, np.nan) if values is None else np.asarray(values, dtype=float)

    if mechanism == "current":
        alpha = np.nan_to_num(col("alpha_eur_mwh"))
        return col("mip_eur_mwh") + alpha, col("mdp_eur_mwh") - alpha
    if mechanism == "new2026" and components == "published":
        inc = col("nf_incentive_eur_mwh")
        return np.fmax(col("nf_main_up_eur_mwh"), inc), np.fmin(col("nf_main_down_eur_mwh"), inc)
    if mechanism == "new2026" and components == "reconstructed":
        if "si_mw" not in qh:
            return np.full(n, np.nan), np.full(n, np.nan)
        rebuilt = reconstruct_new_formula_history(pd.DataFrame(qh))
        return (
            rebuilt["main_up_eur_mwh"].to_numpy(dtype=float),
            rebuilt["main_down_eur_mwh"].to_numpy(dtype=float),
        )
    raise ValueError(f"unsupported mechanism/components combination: {mechanism}/{components}")


def default_component_baselines(mechanism: str, components: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Residual baselines for the (up, down) heads: running component, previous quarter-hour, last final price.

    The zero-history variant only sees current-design state, so it anchors on the running (single) aFRR price.
    """
    if mechanism == "current":
        return (
            ("cur_up_cum", "cur_up_prev_qh", "hist_price_last"),
            ("cur_down_cum", "cur_down_prev_qh", "hist_price_last"),
        )
    if components == "published":
        return (
            ("nf_main_up_cum", "nf_main_up_prev_qh", "hist_price_last"),
            ("nf_main_down_cum", "nf_main_down_prev_qh", "hist_price_last"),
        )
    return (
        ("afrr_price_up_cum", "afrr_price_up_prev_qh", "hist_price_last"),
        ("afrr_price_down_cum", "afrr_price_down_prev_qh", "hist_price_last"),
    )


def default_prism_features(mechanism: str, components: str) -> FeatureConfig:
    """Invariant state features; the current-formula running fields only when pricing the current formula."""
    if mechanism == "current":
        return FeatureConfig(groups=CURRENT_GROUPS, history="settlement:current")
    if components == "reconstructed":
        return FeatureConfig(groups=INVARIANT_GROUPS, history="reconstructed:new2026")
    return FeatureConfig(groups=(*INVARIANT_GROUPS, "new_mech"), history="settlement:new2026")


@dataclass(frozen=True)
class PrismForecasterConfig:
    mechanism: Literal["current", "new2026"] = "current"
    components: Literal["published", "reconstructed"] = "published"
    features: FeatureConfig | None = None
    quantiles: tuple[float, ...] = DEFAULT_QUANTILES
    stride: int = 3
    params: GBMParams = field(default_factory=GBMParams)
    si_field: str = "si_mw"
    up_baseline: tuple[str, ...] | None = None
    down_baseline: tuple[str, ...] | None = None

    def feature_config(self) -> FeatureConfig:
        return self.features or default_prism_features(self.mechanism, self.components)

    def baselines(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        up, down = default_component_baselines(self.mechanism, self.components)
        return self.up_baseline or up, self.down_baseline or down


@dataclass
class _RegimeHeads:
    sign: SignHead
    up_quantile: QuantileHead
    up_mean: MeanHead
    down_quantile: QuantileHead
    down_mean: MeanHead


class PrismForecaster(WalkForwardForecaster):
    """Regime probability x regime-conditional component distributions -> mechanism price mixture."""

    def __init__(self, config: PrismForecasterConfig | None = None, name: str | None = None) -> None:
        self.config = config or PrismForecasterConfig()
        super().__init__(self.config.feature_config())
        self.name = name or f"prism_{self.config.mechanism}_{self.config.components}"
        self._weights = quantile_weights(self.config.quantiles).astype(np.float32)

    def history_prices(self, market: MarketData) -> FloatArray:
        history = self.feature_config.history
        if history == "none":
            history = f"settlement:{self.config.mechanism}"
        return history_price_series(market, history)

    def _fit_fold(
        self, market: MarketData, features: FeatureMatrix, fold: Fold, publication: PublicationModel
    ) -> tuple[Any, dict[str, Any]]:
        up_qh, down_qh = regime_components(market, self.config.mechanism, self.config.components)
        si_qh = market.qh.get(self.config.si_field)
        if si_qh is None:
            raise KeyError(f"quarter-hour field `{self.config.si_field}` is needed for the regime head")
        heads: dict[int, _RegimeHeads] = {}
        log: dict[str, Any] = {}
        q, p = self.config.quantiles, self.config.params
        up_chain, down_chain = self.config.baselines()
        for horizon in (0, 1):
            rows = training_rows(market, fold, horizon=horizon, publication=publication, stride=self.config.stride)
            si = minute_target(market, np.asarray(si_qh, dtype=float), horizon)[rows]
            x = features.values[rows]
            up = minute_target(market, up_qh, horizon)[rows] - baseline_values(x, self.feature_names_, up_chain)
            down = minute_target(market, down_qh, horizon)[rows] - baseline_values(x, self.feature_names_, down_chain)
            known = np.isfinite(si)
            short = known & (si < 0.0)
            long = known & (si >= 0.0)
            sign = np.where(known, (si < 0.0).astype(float), np.nan)
            heads[horizon] = _RegimeHeads(
                sign=SignHead(p).fit(x, sign),
                up_quantile=QuantileHead(q, p).fit(x[short], up[short]),
                up_mean=MeanHead(p).fit(x[short], up[short]),
                down_quantile=QuantileHead(q, p).fit(x[long], down[long]),
                down_mean=MeanHead(p).fit(x[long], down[long]),
            )
            log[f"train_rows_h{horizon}"] = int(known.sum())
            log[f"short_share_h{horizon}"] = float(short.sum() / max(known.sum(), 1))
        return heads, log

    def _allocate(self, n: int) -> tuple[DiscreteForecast, DiscreteForecast]:
        k = 2 * len(self.config.quantiles)
        return DiscreteForecast.empty(n, k), DiscreteForecast.empty(n, k)

    def _predict_rows(
        self,
        models: Any,
        x: NDArray[np.float32],
        rows: NDArray[np.int64],
        current: DiscreteForecast,
        nxt: DiscreteForecast,
    ) -> None:
        w = self._weights
        up_chain, down_chain = self.config.baselines()
        up_base = baseline_values(x, self.feature_names_, up_chain)
        down_base = baseline_values(x, self.feature_names_, down_chain)
        for horizon, target in ((0, current), (1, nxt)):
            head: _RegimeHeads = models[horizon]
            p_short = np.clip(head.sign.predict(x), 0.0, 1.0)
            up_q = head.up_quantile.predict(x) + up_base[:, None]
            down_q = head.down_quantile.predict(x) + down_base[:, None]
            up_mean = head.up_mean.predict(x) + up_base
            down_mean = head.down_mean.predict(x) + down_base
            target.values[rows] = np.hstack([up_q, down_q]).astype(np.float32)
            target.weights[rows] = np.hstack([p_short[:, None] * w, (1.0 - p_short)[:, None] * w]).astype(np.float32)
            target.mean[rows] = p_short * up_mean + (1.0 - p_short) * down_mean
            if target.p_short is not None:
                target.p_short[rows] = p_short
