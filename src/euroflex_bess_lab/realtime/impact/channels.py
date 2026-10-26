"""Decomposition of a fleet's own price impact into channels (Proposition 1 of the paper plan).

The total impact ``pi_k(q) - pi_k(0)`` of a deviation path ``q`` is split sequentially, per quarter-hour:

Current formula (``CurrentFormulaImpact``), evaluated in the quarter-hour's *original* branch first:

1. ``merit``: the marginal price moves along the merit order (MIP' - MIP or MDP' - MDP, FLOOR / CAP respected);
2. ``alpha_own``: alpha changes because this quarter-hour's imbalance moves (``x`` and ``|SI| > 150``; on the original
   side of zero, so a deviation that removes the whole imbalance removes the alpha premium) and because the price
   factor ``c`` follows MIP' / MDP';
3. ``alpha_carry``: alpha changes because the fleet's deviation in the *previous* quarter-hour moved ``|SI_{k-1}|``
   (alpha's driver averages two quarter-hours, so a deviation also prices the next quarter-hour);
4. ``branch``: the rest -- switching to the neutral band (``|SI'| < 25 MW``, midpoint price) or to the other branch
   (overshoot).

2026 formula (``NewFormula2026Impact``):

1. ``merit``: aFRR / mFRR product prices move along their curves with the weights held fixed;
2. ``weight``: re-weighting between aFRR and mFRR (non-zero only with ``displacement="afrr_first"``);
3. ``branch``: the rest (sign flip to the other main component; the incentive rule).

The channels sum to the total impact by construction. :func:`channel_value` turns them into EUR per MW per year for a
fleet whose quarter-hour energies are known.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import numpy as np
from numpy.typing import NDArray

from .alpha import price_factor
from .operators import CurrentFormulaImpact, NewFormula2026Impact
from .response import NEUTRAL_BAND_MW, counterfactual_marginal_prices, displace, new_formula_parts

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]

CURRENT_CHANNELS: tuple[str, ...] = ("merit", "alpha_own", "alpha_carry", "branch")
NEW_CHANNELS: tuple[str, ...] = ("merit", "weight", "branch")


def _previous(q: FloatArray) -> FloatArray:
    out: FloatArray = np.concatenate([[0.0], np.asarray(q, dtype=float)[:-1]])
    return out


def current_channels(
    op: CurrentFormulaImpact, q: FloatArray, q_prev: FloatArray | None = None
) -> dict[str, FloatArray]:
    """Channel split of the current-formula impact for a deviation path over all quarter-hours of ``op``.

    ``q_prev`` defaults to the path shifted by one quarter-hour (the fleet's own previous deviation).
    """
    st = op.state
    fit = op.fit
    up, down = op._deltas()
    qq = np.asarray(q, dtype=float)
    qp = _previous(qq) if q_prev is None else np.asarray(q_prev, dtype=float)
    total = op.price_at(qq, q_prev=qp) - op.price_at(np.zeros_like(qq), q_prev=np.zeros_like(qq))
    si = st.si_mw
    prev = np.where(np.isnan(st.si_prev_mw), si, st.si_prev_mw)
    u2, d2 = displace(st.up_mw, st.down_mw, qq, fit.pass_through)
    mip2, mdp2 = counterfactual_marginal_prices(st, u2, d2, up=up, down=down)
    alpha_model = fit.alpha
    short = si < 0.0
    neutral0 = np.abs(si) < NEUTRAL_BAND_MW
    neutral0 = neutral0 & np.isfinite(0.5 * (st.floor_eur_mwh + st.cap_eur_mwh))
    si2 = si + qq

    def alpha_in_original_branch(
        si_new: FloatArray, prev_new: FloatArray, mip: FloatArray, mdp: FloatArray
    ) -> FloatArray:
        core = alpha_model.predict(si_new, prev_new)  # tariff part, zero for |SI'| <= 150
        if alpha_model.use_price_factor:
            core = core * price_factor(si, mip, mdp)  # the factor of the *original* branch
        return core

    # the original branch's alpha is evaluated on its own side of zero: removing the imbalance removes the
    # premium (alpha channel); jumping to the other branch's price and alpha is the branch channel
    si2_side = np.where(short, np.minimum(si2, 0.0), np.maximum(si2, 0.0))
    a0 = alpha_in_original_branch(si, prev, st.mip_eur_mwh, st.mdp_eur_mwh)
    a_own = alpha_in_original_branch(si2_side, prev, mip2, mdp2)
    a_carry = alpha_in_original_branch(si2_side, prev + qp, mip2, mdp2)
    sign = np.where(short, 1.0, -1.0)
    merit = np.where(short, mip2 - st.mip_eur_mwh, mdp2 - st.mdp_eur_mwh)
    alpha_own = sign * (a_own - a0)
    alpha_carry = sign * (a_carry - a_own)
    merit = np.where(neutral0, 0.0, merit)
    alpha_own = np.where(neutral0, 0.0, alpha_own)
    alpha_carry = np.where(neutral0, 0.0, alpha_carry)
    branch = total - merit - alpha_own - alpha_carry
    return {
        "total": total,
        "merit": np.nan_to_num(merit),
        "alpha_own": np.nan_to_num(alpha_own),
        "alpha_carry": np.nan_to_num(alpha_carry),
        "branch": np.nan_to_num(branch),
    }


def new_channels(op: NewFormula2026Impact, q: FloatArray, *, displacement: str | None = None) -> dict[str, FloatArray]:
    """Channel split of the 2026-formula impact (``displacement`` overrides the operator's rule)."""
    fit = op.fit if displacement is None else replace(op.fit, displacement=displacement)
    op2 = replace(op, fit=fit)
    qq = np.asarray(q, dtype=float)
    total = op2.price_at(qq) - op2.price_at(np.zeros_like(qq))
    parts = new_formula_parts(
        op2.state, qq, curves=fit.curves(), pass_through=fit.pass_through, displacement=fit.displacement
    )
    short = op2.state.si_mw < 0.0
    merit = np.where(short, parts["merit_up"], parts["merit_down"])
    weight = np.where(short, parts["weight_up"], parts["weight_down"])
    branch = total - merit - weight
    return {
        "total": total,
        "merit": np.nan_to_num(merit),
        "weight": np.nan_to_num(weight),
        "branch": np.nan_to_num(branch),
    }


def channel_value(
    channels: dict[str, FloatArray],
    energy_mwh: FloatArray,
    *,
    unit_power_mw: float,
    years: float,
    active: NDArray[np.bool_] | None = None,
) -> dict[str, Any]:
    """Revenue effect of each channel (EUR per MW per year) and the mean price effect on active quarter-hours."""
    e = np.asarray(energy_mwh, dtype=float)
    act = np.abs(e) > 1e-9 if active is None else active
    out: dict[str, Any] = {}
    for name, delta in channels.items():
        d = np.nan_to_num(np.asarray(delta, dtype=float))
        out[f"{name}_eur_per_mw_year"] = float(np.sum(e * d) / unit_power_mw / years) if years > 0 else float("nan")
        out[f"{name}_mean_eur_mwh"] = float(np.mean(d[act])) if act.any() else 0.0
    return out
