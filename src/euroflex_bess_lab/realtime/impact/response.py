"""Counterfactual settlement prices when a fleet deviates by ``q`` MW (the PRISM impact layer).

``pi_k(q) = M(X_k(q))``: the fleet's quarter-hour average deviation ``q`` (MW, + = long / net injection) shifts the
system imbalance to ``SI + q`` and displaces Elia's balancing activations; the mechanism ``M`` then maps the
counterfactual state to the price.

Displacement rule (both mechanisms): Elia's controller keeps the area balanced, so the net aFRR + mFRR activation
moves by ``-kappa * q``, where the *pass-through* ``kappa`` is estimated from history (regression of the change in
net aFRR + mFRR activation on the change in SI between consecutive quarter-hours: ~0.65 in 2025-2026, the rest is
absorbed by imbalance netting / IGCC and the ACE). A long deviation first reduces the gross upward activation ``U`` and
only then adds downward activation: ``U' = max(U - kappa q, 0)``, ``D' = D + max(kappa q - U, 0)``; a short
deviation symmetrically reduces ``D`` first.
Product prices move along their fitted volume -> price curves (incremental form, so the observed price level is
kept). The price branch is chosen by the sign of the counterfactual ``SI + q``: a deviation larger than ``|SI|``
flips the sign and switches the branch (the overshoot mechanism).

- Current formula (Elia's rule, verified on ODS134): ``MIP' = max(MIP + f_up(U') - f_up(U), FLOOR)``,
  ``MDP' = min(MDP + f_down(D') - f_down(D), CAP)``; alpha by the exact rule at the counterfactual SI, previous-QH SI
  and MIP'/MDP' (zero for ``|SI'| <= 150 MW``); price ``(FLOOR + CAP) / 2`` if ``|SI'| < 25 MW``, else
  ``MIP' + alpha'`` if ``SI' < 0``, else ``MDP' - alpha'``.
- New 2026 formula: within the original branch the published aFRR / mFRR weights are kept (reductions are pro-rata
  across aFRR and mFRR, increases are aFRR) and the branch's main component moves by the weighted product-price
  changes; after a sign flip the new-direction regulation is all aFRR (weights 1 / 0) priced along the aFRR curve
  from the observed new-direction volume. The final mapping is
  :meth:`~euroflex_bess_lab.realtime.mechanisms.NewWeightedMechanism2026.price_given_si` (incentive rule included).

Both default operators are non-increasing in ``q`` within a branch (short / neutral / long for the current rule;
more long deviation never raises the price) and return the published price exactly at ``q = 0``. The optional
``afrr_first`` displacement of the 2026 operator re-weights aFRR / mFRR and deliberately breaks this property (the
weight channel).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

import numpy as np
from numpy.typing import ArrayLike, NDArray

from ..mechanisms import NewWeightedMechanism2026
from .alpha import ALPHA_THRESHOLD_MW, AlphaModel
from .curves import PRICE_CAP_EUR_MWH, PRICE_FLOOR_EUR_MWH, RegimeCurves
from .state import CurrentState, NewFormulaState

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]


@runtime_checkable
class VolumeDelta(Protocol):
    """Price change of one product when its activated volume moves from ``v_from`` to ``v_to`` (per state row)."""

    def delta(self, regime: IntArray, row: IntArray, v_from: FloatArray, v_to: FloatArray) -> FloatArray: ...


@dataclass(frozen=True)
class CurveDelta:
    """Adapter: fitted :class:`RegimeCurves` (state-dependent through the regime id)."""

    curves: RegimeCurves

    def delta(self, regime: IntArray, row: IntArray, v_from: FloatArray, v_to: FloatArray) -> FloatArray:
        return self.curves.delta(regime, v_from, v_to)


@dataclass(frozen=True)
class BidCurveDelta:
    """Quarter-hour specific merit-order curves (``prices[row, j]`` = price to activate ``grid_mw[j]``).

    Rows with ``row < 0`` or a missing curve segment fall back to ``fallback``. Prices beyond the available bid
    volume are held at the last available price (no scarcity extrapolation).
    """

    grid_mw: FloatArray
    prices: FloatArray
    fallback: VolumeDelta
    increasing: bool = True

    def value(self, rows: IntArray, volume: FloatArray) -> FloatArray:
        """Curve price at ``volume`` for the given table rows (``volume`` shape ``(n,)`` or ``(n, k)``)."""
        grid = self.grid_mw
        v = np.clip(volume, grid[0], grid[-1])
        pos = np.clip(np.searchsorted(grid, v, side="right") - 1, 0, len(grid) - 2)
        w = (v - grid[pos]) / (grid[pos + 1] - grid[pos])
        table = self.prices[rows]
        if v.ndim == 2:
            lo = np.take_along_axis(table, pos, axis=1)
            hi = np.take_along_axis(table, pos + 1, axis=1)
        else:
            lo = table[np.arange(len(rows)), pos]
            hi = table[np.arange(len(rows)), pos + 1]
        hi = np.where(np.isnan(hi), lo, hi)
        out: FloatArray = lo + w * (hi - lo)
        return out

    def delta(self, regime: IntArray, row: IntArray, v_from: FloatArray, v_to: FloatArray) -> FloatArray:
        a, b = np.broadcast_arrays(np.asarray(v_from, dtype=float), np.asarray(v_to, dtype=float))
        fallback = self.fallback.delta(regime, row, a, b)
        ok_row = row >= 0
        if not ok_row.any():
            return fallback
        safe_rows = np.where(ok_row, row, 0)
        diff = self.value(safe_rows, b) - self.value(safe_rows, a)
        mask = ok_row if a.ndim == 1 else ok_row[:, None]
        out: FloatArray = np.where(mask & np.isfinite(diff), diff, fallback)
        return out


def _col(x: FloatArray, like: FloatArray) -> FloatArray:
    """``x`` as a column when ``like`` is 2-D (state rows x deviation grid), else unchanged."""
    arr: FloatArray = np.asarray(x, dtype=float)
    if like.ndim == 2 and arr.ndim == 1:
        out: FloatArray = arr[:, None]
        return out
    return arr


def _as_q(q: ArrayLike, n: int) -> FloatArray:
    arr = np.asarray(q, dtype=float)
    if arr.ndim == 0:
        arr = np.full(n, float(arr))
    if arr.shape[0] != n:
        raise ValueError(f"deviation has {arr.shape[0]} rows, state has {n}")
    return arr


def displace(
    up: FloatArray, down: FloatArray, q: FloatArray, pass_through: float = 1.0
) -> tuple[FloatArray, FloatArray]:
    """Gross activated volumes after a deviation ``q`` (MW, + = long). Net activation moves by ``-pass_through * q``."""
    if not 0.0 <= pass_through <= 1.5:
        raise ValueError("pass_through must be in [0, 1.5]")
    qp = np.maximum(q, 0.0) * pass_through
    qn = np.maximum(-q, 0.0) * pass_through
    up2 = np.maximum(up - qp, 0.0) + np.maximum(qn - down, 0.0)
    down2 = np.maximum(down - qn, 0.0) + np.maximum(qp - up, 0.0)
    return up2, down2


NEUTRAL_BAND_MW = 25.0


def counterfactual_marginal_prices(
    state: CurrentState,
    u2: FloatArray,
    d2: FloatArray,
    *,
    up: VolumeDelta,
    down: VolumeDelta,
) -> tuple[FloatArray, FloatArray]:
    """MIP' / MDP' after the activated volumes move to ``(u2, d2)``.

    Prices move along the volume-response curves and respect Elia's structure ``MIP = max(product prices, FLOOR)``,
    ``MDP = min(product prices, CAP)``: MIP' never falls below FLOOR and MDP' never rises above CAP.
    """
    u = _col(state.up_mw, u2)
    d = _col(state.down_mw, d2)
    u_b, d_b = np.broadcast_arrays(u, u2)[0], np.broadcast_arrays(d, d2)[0]
    mip = _col(state.mip_eur_mwh, u2) + up.delta(state.regime, state.curve_row, u_b, u2)
    mdp = _col(state.mdp_eur_mwh, d2) + down.delta(state.regime, state.curve_row, d_b, d2)
    floor = _col(state.floor_eur_mwh, u2)
    cap = _col(state.cap_eur_mwh, d2)
    mip = np.where(np.isfinite(floor), np.maximum(mip, floor), mip)
    mdp = np.where(np.isfinite(cap), np.minimum(mdp, cap), mdp)
    return np.asarray(mip, dtype=float), np.asarray(mdp, dtype=float)


def assemble_current_price(
    si2: FloatArray,
    si_prev2: FloatArray,
    mip2: FloatArray,
    mdp2: FloatArray,
    floor: FloatArray,
    cap: FloatArray,
    alpha_model: AlphaModel | None,
    *,
    alpha_mode: str = "exact",
    alpha_obs: FloatArray | None = None,
    si: FloatArray | None = None,
    si_prev: FloatArray | None = None,
    neutral_band_mw: float = NEUTRAL_BAND_MW,
) -> FloatArray:
    """Elia's current rule at a counterfactual state.

    ``|SI'| < 25 MW``: ``(FLOOR + CAP) / 2``; short: ``MIP' + alpha'``; long: ``MDP' - alpha'`` with
    ``alpha' = 1{|SI'| > 150} * tariff(x') * c(MIP' or MDP')`` (mode ``exact``; see :mod:`.alpha`).
    """
    if alpha_model is None:
        base = np.zeros_like(si2) if alpha_obs is None else np.where(np.isnan(alpha_obs), 0.0, alpha_obs)
        alpha = np.where(np.abs(si2) <= ALPHA_THRESHOLD_MW, 0.0, base)
    else:
        obs = np.zeros_like(si2) if alpha_obs is None else alpha_obs
        alpha = alpha_model.counterfactual(
            obs,
            si2 if si is None else si,
            si_prev2 if si_prev is None else si_prev,
            si2,
            si_prev2,
            mode=alpha_mode,
            mip_new=mip2,
            mdp_new=mdp2,
        )
    mid = 0.5 * (floor + cap)
    neutral = (np.abs(si2) < neutral_band_mw) & np.isfinite(mid)
    price = np.where(neutral, mid, np.where(si2 < 0.0, mip2 + alpha, mdp2 - alpha))
    out: FloatArray = np.clip(price, PRICE_FLOOR_EUR_MWH, PRICE_CAP_EUR_MWH)
    return out


def current_price(
    state: CurrentState,
    q: ArrayLike,
    *,
    up: VolumeDelta,
    down: VolumeDelta,
    alpha_model: AlphaModel | None,
    q_prev: ArrayLike | None = None,
    alpha_mode: str = "exact",
    pass_through: float = 1.0,
) -> FloatArray:
    """Current-formula price for deviation ``q`` per state row (``q`` of shape ``(n,)`` or ``(n, k)``)."""
    n = len(state)
    qq = _as_q(q, n)
    si = _col(state.si_mw, qq)
    u = _col(state.up_mw, qq)
    d = _col(state.down_mw, qq)
    u2, d2 = displace(u, d, qq, pass_through)
    mip2, mdp2 = counterfactual_marginal_prices(state, u2, d2, up=up, down=down)
    si2 = si + qq
    qp = np.zeros(n) if q_prev is None else _as_q(q_prev, n)
    prev = np.where(np.isnan(state.si_prev_mw), state.si_mw, state.si_prev_mw)
    si_prev2 = np.broadcast_to(_col(prev + qp, qq), si2.shape)
    shape = si2.shape

    def bc(x: FloatArray) -> FloatArray:
        out_b: FloatArray = np.broadcast_to(_col(x, qq), shape)
        return out_b

    return assemble_current_price(
        si2,
        si_prev2,
        mip2,
        mdp2,
        bc(state.floor_eur_mwh),
        bc(state.cap_eur_mwh),
        alpha_model,
        alpha_mode=alpha_mode,
        alpha_obs=bc(state.alpha_eur_mwh),
        si=bc(state.si_mw),
        si_prev=bc(prev),
    )


def _nan0(x: FloatArray) -> FloatArray:
    out: FloatArray = np.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0)
    return out


def _split(gross_a: FloatArray, gross_m: FloatArray, total_new: FloatArray) -> tuple[FloatArray, FloatArray]:
    """Allocate a new gross total to aFRR / mFRR: reductions pro-rata, increases all aFRR."""
    total = gross_a + gross_m
    with np.errstate(invalid="ignore", divide="ignore"):
        ratio = np.where(total > 0.0, total_new / np.where(total > 0.0, total, 1.0), 0.0)
    shrink = total_new <= total
    a_new = np.where(shrink, np.where(total > 0.0, gross_a * ratio, 0.0), gross_a + (total_new - total))
    m_new = np.where(shrink, np.where(total > 0.0, gross_m * ratio, 0.0), gross_m)
    return a_new, m_new


@dataclass(frozen=True)
class NewFormulaCurves:
    """Fitted product-price curves of the 2026 formula."""

    afrr_up: VolumeDelta
    afrr_down: VolumeDelta
    mfrr_up: VolumeDelta
    mfrr_down: VolumeDelta


DISPLACEMENTS: tuple[str, ...] = ("pro_rata", "afrr_first")


def _net_weight(a_net: FloatArray, m_net: FloatArray) -> FloatArray:
    """aFRR weight of the 2026 formula from net volumes, ``|net aFRR| / (|net aFRR| + |net mFRR|)`` (1 if both 0)."""
    total = np.abs(a_net) + np.abs(m_net)
    with np.errstate(invalid="ignore", divide="ignore"):
        out: FloatArray = np.where(total > 0.0, np.abs(a_net) / np.where(total > 0.0, total, 1.0), 1.0)
    return out


def new_formula_parts(
    state: NewFormulaState,
    q: ArrayLike,
    *,
    curves: NewFormulaCurves,
    pass_through: float = 1.0,
    displacement: str = "pro_rata",
) -> dict[str, FloatArray]:
    """Counterfactual pieces of the 2026 formula for deviation ``q``.

    Returned arrays: ``si2``; ``merit_up`` / ``merit_down`` (product prices moved, weights fixed);
    ``weight_up`` / ``weight_down`` (re-weighting, zero for ``pro_rata``); ``flip_up`` / ``flip_down`` (aFRR-only main
    component after a sign flip); ``main_up`` / ``main_down`` (published) and ``main_up2`` / ``main_down2`` (final).

    ``pro_rata`` (default): reductions are shared by aFRR and mFRR in proportion to their gross volumes, increases go
    to aFRR, the published weights are kept within the branch (monotone). ``afrr_first``: only aFRR moves (mFRR is
    scheduled for the whole quarter-hour) and the weights follow the net volumes
    (``w' = w + w_net(q) - w_net(0)``) -- the *weight channel*, which raises the short-branch price when a long
    deviation displaces aFRR while dearer mFRR stays activated.
    """
    if displacement not in DISPLACEMENTS:
        raise ValueError(f"displacement must be one of {DISPLACEMENTS}")
    n = len(state)
    qq = _as_q(q, n)
    row = np.full(n, -1, dtype=np.int64)
    reg = state.regime
    si = _col(state.si_mw, qq)
    au, ad = _col(state.afrr_up_mw, qq), _col(state.afrr_down_mw, qq)
    mu, md = _col(state.mfrr_up_mw, qq), _col(state.mfrr_down_mw, qq)
    if displacement == "pro_rata":
        up2, down2 = displace(au + mu, ad + md, qq, pass_through)
        au2, mu2 = _split(au, mu, up2)
        ad2, md2 = _split(ad, md, down2)
    else:
        au2, ad2 = displace(au, ad, qq, pass_through)
        mu2, md2 = np.broadcast_to(mu, au2.shape), np.broadcast_to(md, ad2.shape)
    shape = au2.shape

    def bc(x: FloatArray) -> FloatArray:
        out_b: FloatArray = np.broadcast_to(x, shape)
        return out_b

    d_au = curves.afrr_up.delta(reg, row, bc(au), au2)
    d_ad = curves.afrr_down.delta(reg, row, bc(ad), ad2)
    d_mu = curves.mfrr_up.delta(reg, row, bc(mu), mu2)
    d_md = curves.mfrr_down.delta(reg, row, bc(md), md2)
    wa_raw = _col(state.weight_afrr, qq)
    wm_raw = _col(state.weight_mfrr, qq)
    no_w = np.isnan(wa_raw) & np.isnan(wm_raw)
    wa = np.where(no_w, 1.0, _nan0(wa_raw))
    wm = np.where(no_w, 0.0, _nan0(wm_raw))
    pm_u = _col(state.mfrr_price_up_eur_mwh, qq)
    pm_d = _col(state.mfrr_price_down_eur_mwh, qq)
    has_pm_u, has_pm_d = np.isfinite(pm_u), np.isfinite(pm_d)
    pa_u = _col(state.afrr_price_up_eur_mwh, qq)
    pa_d = _col(state.afrr_price_down_eur_mwh, qq)
    merit_up = wa * d_au + np.where(has_pm_u, wm * d_mu, 0.0)
    merit_down = wa * d_ad + np.where(has_pm_d, wm * d_md, 0.0)
    if displacement == "afrr_first":
        w_model_0 = _net_weight(au - ad, mu - md)
        w_model_q = _net_weight(au2 - ad2, mu2 - md2)
        wa2 = np.clip(wa + w_model_q - w_model_0, 0.0, 1.0)
        wm2 = 1.0 - wa2
        weight_up = (wa2 - wa) * (pa_u + d_au) + (wm2 - wm) * _nan0(pm_u + d_mu)
        weight_down = (wa2 - wa) * (pa_d + d_ad) + (wm2 - wm) * _nan0(pm_d + d_md)
    else:
        weight_up = np.zeros(shape)
        weight_down = np.zeros(shape)
    main_up = _col(state.main_up_eur_mwh, qq)
    main_down = _col(state.main_down_eur_mwh, qq)
    same_up = main_up + merit_up + weight_up
    same_down = main_down + merit_down + weight_down
    flip_up = pa_u + d_au
    flip_down = pa_d + d_ad
    si2 = si + qq
    short, short2 = si < 0.0, si2 < 0.0
    same = short == short2
    main_up2 = np.where(same | ~short2, same_up, flip_up)
    main_down2 = np.where(same | short2, same_down, flip_down)
    return {
        "si2": np.asarray(si2, dtype=float),
        "merit_up": bc(merit_up),
        "merit_down": bc(merit_down),
        "weight_up": bc(weight_up),
        "weight_down": bc(weight_down),
        "flip_up": bc(flip_up),
        "flip_down": bc(flip_down),
        "main_up": bc(main_up),
        "main_down": bc(main_down),
        "main_up2": np.clip(main_up2, PRICE_FLOOR_EUR_MWH, PRICE_CAP_EUR_MWH),
        "main_down2": np.clip(main_down2, PRICE_FLOOR_EUR_MWH, PRICE_CAP_EUR_MWH),
    }


def new_formula_main(
    state: NewFormulaState,
    q: ArrayLike,
    *,
    curves: NewFormulaCurves,
    pass_through: float = 1.0,
    displacement: str = "pro_rata",
) -> tuple[FloatArray, FloatArray, FloatArray]:
    """Counterfactual ``(si', main_up', main_down')`` for deviation ``q`` (see :func:`new_formula_parts`)."""
    parts = new_formula_parts(state, q, curves=curves, pass_through=pass_through, displacement=displacement)
    return parts["si2"], parts["main_up2"], parts["main_down2"]


def new_formula_price(
    state: NewFormulaState,
    q: ArrayLike,
    *,
    curves: NewFormulaCurves,
    pass_through: float = 1.0,
    displacement: str = "pro_rata",
) -> FloatArray:
    """2026-formula price for deviation ``q`` per state row via ``NewWeightedMechanism2026.price_given_si``."""
    si2, main_up2, main_down2 = new_formula_main(
        state, q, curves=curves, pass_through=pass_through, displacement=displacement
    )
    incentive = np.broadcast_to(_col(state.incentive_eur_mwh, si2), si2.shape)
    out: FloatArray = NewWeightedMechanism2026.price_given_si(
        si2, main_up=main_up2, main_down=main_down2, incentive=incentive
    )
    return out
