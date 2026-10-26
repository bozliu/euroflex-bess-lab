"""Impact layer of the real-time lab: how the Belgian imbalance price responds when a fleet of size Q deviates.

- :mod:`.curves` / :mod:`.alpha`: data-driven volume -> price curves (fixed-effect monotone fits) and Elia's alpha rule;
- :mod:`.response`: counterfactual prices ``pi(q) = M(X(q))`` for the current and the 2026 formula;
- :mod:`.channels`: split of the own-price impact into merit-order, alpha, weight and branch (overshoot) channels;
- :mod:`.operators`: :class:`~euroflex_bess_lab.realtime.ImpactModel` implementations for ``simulate``;
- :mod:`.validation`: back-tests of the operators on history;
- :mod:`.policies`: an impact-aware threshold policy that internalises the fleet's own impact;
- :mod:`.fleet`: per-MW capacity-value curves versus fleet size;
- :mod:`.equilibrium`: multi-fleet herding (closed loop) versus a best-response (Nash) equilibrium.

See ``docs/impact_and_herding.md`` for methods and limitations.
"""

from __future__ import annotations

from .alpha import (
    ALPHA_FEATURES,
    ALPHA_THRESHOLD_MW,
    TARIFF_ALPHA,
    AlphaFit,
    AlphaModel,
    alpha_feature,
    fit_alpha_model,
    price_factor,
)
from .channels import channel_value, current_channels, new_channels
from .curves import (
    DEFAULT_REGIME_EDGES_HOURS,
    RegimeCurves,
    ResponseCurve,
    fit_regime_curves,
    fit_response_curve,
    hour_groups,
    local_hour_regime,
)
from .operators import (
    CurrentFormulaImpact,
    CurrentResponseFit,
    ImpactOperator,
    NewFormula2026Impact,
    NewFormulaResponseFit,
    fit_current_response,
    fit_new_formula_response,
    operator_for,
)
from .response import (
    NEUTRAL_BAND_MW,
    BidCurveDelta,
    CurveDelta,
    NewFormulaCurves,
    VolumeDelta,
    assemble_current_price,
    counterfactual_marginal_prices,
    current_price,
    displace,
    new_formula_main,
    new_formula_parts,
    new_formula_price,
)
from .state import (
    CurrentState,
    NewFormulaState,
    current_running_state,
    current_state_from_arrays,
    current_state_from_market,
    new_running_state,
    new_state_from_arrays,
    new_state_from_market,
    qh_to_date_mean,
    structural_marginal_prices,
)

__all__ = [
    "ALPHA_FEATURES",
    "ALPHA_THRESHOLD_MW",
    "DEFAULT_REGIME_EDGES_HOURS",
    "NEUTRAL_BAND_MW",
    "TARIFF_ALPHA",
    "AlphaFit",
    "AlphaModel",
    "BidCurveDelta",
    "CurrentFormulaImpact",
    "CurrentResponseFit",
    "CurrentState",
    "CurveDelta",
    "ImpactOperator",
    "NewFormula2026Impact",
    "NewFormulaCurves",
    "NewFormulaResponseFit",
    "NewFormulaState",
    "RegimeCurves",
    "ResponseCurve",
    "VolumeDelta",
    "alpha_feature",
    "assemble_current_price",
    "channel_value",
    "counterfactual_marginal_prices",
    "current_channels",
    "current_price",
    "current_running_state",
    "current_state_from_arrays",
    "current_state_from_market",
    "displace",
    "fit_alpha_model",
    "fit_current_response",
    "fit_new_formula_response",
    "fit_regime_curves",
    "fit_response_curve",
    "hour_groups",
    "local_hour_regime",
    "new_channels",
    "new_formula_main",
    "new_formula_parts",
    "new_formula_price",
    "new_running_state",
    "new_state_from_arrays",
    "new_state_from_market",
    "operator_for",
    "price_factor",
    "qh_to_date_mean",
    "structural_marginal_prices",
]
