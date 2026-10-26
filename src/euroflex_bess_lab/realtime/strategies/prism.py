"""SCALES-L: the mechanism-aware real-time strategy (module name kept from the earlier working name PRISM).

SCALES-L is the lag-aware control layer of the SCALES framework (State, Code/mechanism, Agent impact, Lag-aware
control, Equilibrium, Structured contracts).

The imbalance price is ``pi_k = M(X_k)``: a mechanism operator ``M`` applied to a mechanism-invariant state ``X_k``
(system imbalance, activated volumes, product prices). SCALES-L forecasts the state, not the price:

1. ``P(SI_k < 0)`` - the regime of the final system imbalance (the same quantity under every formula);
2. the regime-conditional components the operator needs (``current``: MIP + alpha / MDP - alpha; ``new2026``:
   volume-weighted aFRR/mFRR main components);
3. the price distribution is the operator applied to the forecast state, ``p * D_up + (1 - p) * D_down``, which
   the rolling MPC core of :mod:`.forecast_mpc` trades.

Because the operator is explicit, a SCALES-L forecaster for a *new* mechanism can be trained before that mechanism has
produced a single price: the components are recomputed from historical state with the new operator
(``components="reconstructed"``, zero-history transfer). See ``reports/realtime_belgium.md`` for the Jan-Jul 2026
natural experiment (same days, both formulas).
"""

from __future__ import annotations

from collections.abc import Mapping

from ..forecasting.base import SupportsForecast
from ..forecasting.models import GBMParams
from ..forecasting.splits import Fold
from ..forecasting.state import PrismForecaster, PrismForecasterConfig
from ..replay import DecisionContext, MarketData
from ..types import PublicationModel
from .base import BaseStrategy, Strategy
from .forecast_mpc import CachedForecaster, ForecastMPCStrategy, MPCConfig


def _mechanism_of(forecaster: SupportsForecast) -> str | None:
    """Operator of the SCALES-L (PRISM) forecaster inside any wrapper chain (cache, live fusion, ...)."""
    inner: object = forecaster
    for _ in range(8):
        if isinstance(inner, PrismForecaster):
            return inner.config.mechanism
        nested = getattr(inner, "forecaster", None)
        if nested is None:
            return None
        inner = nested
    return None


class PrismStrategy(ForecastMPCStrategy):
    """SCALES-L: rolling MPC on mechanism-priced state forecasts; refuses a mechanism it was not built for."""

    def __init__(
        self,
        forecaster: SupportsForecast,
        config: MPCConfig | None = None,
        name: str | None = None,
    ) -> None:
        mechanism = _mechanism_of(forecaster)
        if mechanism is None:
            raise TypeError("PrismStrategy needs a PrismForecaster (optionally wrapped, e.g. in CachedForecaster)")
        super().__init__(forecaster, config, name or f"prism_{mechanism}")
        self.mechanism = mechanism

    def reset(self, ctx: DecisionContext) -> None:
        if ctx.mechanism != self.mechanism:
            raise ValueError(
                f"SCALES-L forecaster prices the `{self.mechanism}` operator but the run settles under `{ctx.mechanism}`"
            )
        super().reset(ctx)


def fit_prism(
    market: MarketData,
    folds: list[Fold],
    *,
    mechanism: str = "current",
    components: str = "published",
    publication: PublicationModel | None = None,
    params: GBMParams | None = None,
    stride: int = 3,
) -> PrismForecaster:
    """Fit a :class:`PrismForecaster` on ``folds`` (convenience wrapper used by the backtest script)."""
    config = PrismForecasterConfig(
        mechanism=mechanism,  # type: ignore[arg-type]
        components=components,  # type: ignore[arg-type]
        params=params or GBMParams(),
        stride=stride,
    )
    forecaster = PrismForecaster(config)
    forecaster.fit(market, folds, publication)
    return forecaster


# --------------------------------------------------------------------------------------------------------------
# live (paper) trading


class MechanismSwitchStrategy(BaseStrategy):
    """Delegates to one strategy per settlement mechanism and follows ``ctx.mechanism`` (e.g. across the go-live).

    On a switch the newly active strategy is reset on the data visible at that minute; otherwise ``refresh`` is
    forwarded so the delegate re-plans on the growing live window.
    """

    def __init__(self, strategies: Mapping[str, Strategy], name: str = "prism") -> None:
        if not strategies:
            raise ValueError("at least one strategy is needed")
        self.strategies = dict(strategies)
        self.name = name
        self._active: str | None = None

    def _pick(self, ctx: DecisionContext) -> Strategy:
        try:
            return self.strategies[ctx.mechanism]
        except KeyError as exc:
            raise ValueError(f"no strategy for mechanism `{ctx.mechanism}`; have {sorted(self.strategies)}") from exc

    def reset(self, ctx: DecisionContext) -> None:
        self._active = ctx.mechanism
        self._pick(ctx).reset(ctx)

    def refresh(self, ctx: DecisionContext) -> None:
        strategy = self._pick(ctx)
        if ctx.mechanism != self._active:
            self.reset(ctx)
            return
        refresh = getattr(strategy, "refresh", None)
        if callable(refresh):
            refresh(ctx)
        else:
            strategy.reset(ctx)

    def decide(self, ctx: DecisionContext) -> float:
        if ctx.mechanism != self._active:
            self.reset(ctx)
        return self._pick(ctx).decide(ctx)


def live_prism(
    mechanism: str = "auto",
    risk: str = "quantile",
    models_dir: str | None = None,
    live_fusion: bool = True,
    running_field: str = "nf_imbalance_price_eur_mwh",
) -> Strategy:
    """Live factory for SCALES-L (module name ``prism``): deployment forecasters from the backtest's ``deploy`` stage.

    ``current`` prices the current operator; ``new2026`` uses the zero-history new-formula forecaster, fused with
    the live running new-formula price (``running_field``) when ``live_fusion``; ``auto`` holds both and switches
    with the live settlement rule.
    """
    from ..forecasting.deploy import load_forecaster, load_fusion_variance
    from ..forecasting.fusion import RunningEstimateFusion

    config = MPCConfig() if risk == "neutral" else MPCConfig(risk=risk)  # type: ignore[arg-type]

    def current() -> PrismStrategy:
        return PrismStrategy(CachedForecaster(load_forecaster("prism_current", models_dir)), config, name="prism")

    def new() -> PrismStrategy:
        base = load_forecaster("prism_new2026", models_dir)
        forecaster: SupportsForecast = base
        if live_fusion:
            forecaster = RunningEstimateFusion(base, running_field, load_fusion_variance(models_dir))
        return PrismStrategy(CachedForecaster(forecaster), config, name="prism")

    if mechanism == "current":
        return current()
    if mechanism == "new2026":
        return new()
    return MechanismSwitchStrategy({"current": current(), "new2026": new()}, name="prism")


#: Factories for ``euroflex live run --strategy <name>`` (discovered by ``realtime.live.strategies``).
LIVE_STRATEGIES = {"prism": live_prism, "scales-l": live_prism}
