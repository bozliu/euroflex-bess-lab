"""Multi-fleet implicit balancing: closed-loop herding versus best-response (Nash) equilibria.

``N`` battery fleets react to the same Belgian running price signal. Every minute each fleet decides its power; the
quarter-hour is settled at the counterfactual price ``pi_k(q_tot)`` of the *total* fleet deviation (QH-level impact
operator). Policies (all use only information visible under the :class:`PublicationModel`):

- ``herding``: heterogeneous price-taking threshold rules (different observation lags and band quantiles). The
  signal they see is *closed loop*: the running QH-to-date price published by Elia already contains the fleets'
  own deviations so far in the quarter-hour, recomputed with the running-state impact operator. Large total
  capacity therefore makes fleets overshoot, see the flipped price and reverse (the herd behaviour of
  Bruninx, Karimi Madahi et al., arXiv 2602.17352).
- ``herding_homogeneous``: identical price takers (same lag and bands) -- isolates the effect of heterogeneity.
- ``naive``: every fleet internalises its *own* impact but ignores the others (each solves the single-fleet
  impact-aware problem; :mod:`.policies`).
- ``nash``: symmetric equilibrium -- each fleet best-responds to the others' quantities given the price-response
  function of the running state (damped best-response iteration on a quantity grid). Fleets can net their known
  deviations out of the published signal, so they decide on the exogenous state.
- ``central``: one optimiser controls the whole implicit capacity (the joint-profit / aggregator benchmark).

For every policy and total capacity the module reports the per-MW value, cycles, overshoot frequency (fleet
deviation opposite to SI and larger than ``|SI|``: the branch flips), the residual imbalance energy and a TSO
activation-cost proxy (``0.25 * (U' MIP' - D' MDP')`` from the current-formula state, the same physical activation
model for both imbalance-price formulas because the price formula does not change how BSPs are paid).

Approximations (see ``docs/impact_and_herding.md``): fleets' bands come from the exogenous published prices; the
equilibrium is a per-minute stage game (each fleet's opportunity cost of energy = its band), SOC limits only clip
the chosen power; the impact operators are the fitted models of :mod:`.operators`.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ..replay import MarketData
from ..types import MINUTES_PER_QH, BatteryParams, PublicationModel
from .curves import local_hour_regime
from .operators import (
    CurrentFormulaImpact,
    CurrentResponseFit,
    ImpactOperator,
    NewFormulaResponseFit,
)
from .policies import SIGNAL_FIELDS, VolumeFromSI, current_running_state_from_si, threshold_bands
from .response import CurveDelta, current_price, displace, new_formula_price
from .state import new_running_state

FloatArray = NDArray[np.float64]
BoolArray = NDArray[np.bool_]
PriceFn = Callable[[FloatArray], FloatArray]

FLEET_POLICIES: tuple[str, ...] = ("herding", "herding_homogeneous", "naive", "nash", "central")


# --------------------------------------------------------------------------------------------------------------
# best responses and equilibria (vectorised over independent rows, e.g. minutes or scenarios)
# --------------------------------------------------------------------------------------------------------------


def _cost(q: FloatArray, cost_up: FloatArray, cost_down: FloatArray) -> FloatArray:
    out: FloatArray = np.where(q > 0.0, cost_up, np.where(q < 0.0, cost_down, 0.0))
    return out


def best_response(
    price_of_own: PriceFn,
    capacity: FloatArray,
    cost_up: FloatArray,
    cost_down: FloatArray,
    grid_u: FloatArray,
    *,
    refine_steps: int = 18,
) -> FloatArray:
    """Quantity maximising ``q * (P(q) - c(q))`` over ``q in [-capacity, capacity]`` for every row.

    ``price_of_own(q)`` maps own quantities ``(R, G)`` to prices ``(R, G)`` (the other fleets' quantities are bound in
    the closure). A grid search is followed by a vectorised golden-section refinement inside the best grid cell's
    neighbourhood (the objective may be discontinuous at a branch switch; the refinement never returns a worse
    point than the grid optimum). Returns 0 where no quantity has a positive objective.
    """
    cap = np.asarray(capacity, dtype=float).reshape(-1)
    cu = np.broadcast_to(np.asarray(cost_up, dtype=float), cap.shape)
    cd = np.broadcast_to(np.asarray(cost_down, dtype=float), cap.shape)
    q = grid_u[None, :] * cap[:, None]
    obj = q * (price_of_own(q) - _cost(q, cu[:, None], cd[:, None]))
    obj = np.where(np.isfinite(obj), obj, -np.inf)
    best = np.argmax(obj, axis=1)
    rows = np.arange(len(cap))
    q_best = q[rows, best]
    obj_best = obj[rows, best]
    if refine_steps > 0 and len(grid_u) > 2:
        step = (grid_u[1] - grid_u[0]) * cap
        lo = np.clip(q_best - step, -cap, cap)
        hi = np.clip(q_best + step, -cap, cap)
        ratio = (math.sqrt(5.0) - 1.0) / 2.0

        def f(x: FloatArray) -> FloatArray:
            val = x * (price_of_own(x[:, None])[:, 0] - _cost(x, cu, cd))
            out: FloatArray = np.where(np.isfinite(val), val, -np.inf)
            return out

        a, b = lo.copy(), hi.copy()
        c = b - ratio * (b - a)
        d = a + ratio * (b - a)
        fc, fd = f(c), f(d)
        for _ in range(refine_steps):
            left = fc > fd
            b = np.where(left, d, b)
            a = np.where(left, a, c)
            d_new = np.where(left, c, a + ratio * (b - a))
            c_new = np.where(left, b - ratio * (b - a), d)
            fd_new = np.where(left, fc, np.nan)
            fc_new = np.where(left, np.nan, fd)
            need_c = left
            need_d = ~left
            fc = np.where(need_c, f(c_new), fc_new)
            fd = np.where(need_d, f(d_new), fd_new)
            c, d = c_new, d_new
        x = 0.5 * (a + b)
        fx = f(x)
        better = fx > obj_best
        q_best = np.where(better, x, q_best)
        obj_best = np.where(better, fx, obj_best)
    result: FloatArray = np.where(obj_best > 0.0, q_best, 0.0)
    return result


def symmetric_equilibrium(
    price_of_total: PriceFn,
    n_fleets: int,
    capacity: FloatArray,
    cost_up: FloatArray,
    cost_down: FloatArray,
    grid_u: FloatArray,
    *,
    max_iter: int = 60,
    tol_mw: float = 0.05,
    damping: float | None = None,
    refine_steps: int = 18,
    initial: FloatArray | None = None,
) -> tuple[FloatArray, BoolArray]:
    """Symmetric Nash equilibrium of ``n_fleets`` identical fleets (per row): ``q = BR((n - 1) q)``.

    Damped fixed-point iteration ``q <- (1 - w) q + w BR((n - 1) q)`` with ``w = 2 / (n + 1)`` by default, which
    converges in one step for a linear price response (Cournot). ``initial`` warm-starts the iteration (e.g. with
    the previous minute's solution). Returns ``(q_per_fleet, converged)``.
    """
    cap = np.asarray(capacity, dtype=float).reshape(-1)
    w = 2.0 / (n_fleets + 1.0) if damping is None else damping
    q = np.zeros_like(cap) if initial is None else np.clip(np.asarray(initial, dtype=float), -cap, cap)
    converged = np.zeros(len(cap), dtype=bool)
    for _ in range(max_iter):
        others = (n_fleets - 1) * q

        def own(x: FloatArray, others: FloatArray = others) -> FloatArray:
            return price_of_total(x + others[:, None])

        br = best_response(own, cap, cost_up, cost_down, grid_u, refine_steps=refine_steps)
        step = br - q
        converged = np.abs(step) <= tol_mw
        if converged.all():
            q = br
            break
        q = q + w * step
    return q, converged


def nash_equilibrium(
    price_of_total: PriceFn,
    capacities: FloatArray,
    cost_up: FloatArray,
    cost_down: FloatArray,
    grid_u: FloatArray,
    *,
    max_iter: int = 100,
    tol_mw: float = 0.05,
    damping: float = 1.0,
    refine_steps: int = 18,
) -> tuple[FloatArray, BoolArray]:
    """General ``N``-fleet Nash equilibrium by (damped) Gauss-Seidel best responses, per row.

    ``capacities``, ``cost_up``, ``cost_down``: arrays ``(R, N)``. Sequential best responses converge for a linear
    price response (Cournot is a potential game); for the piecewise impact operators convergence is reported.
    """
    caps = np.atleast_2d(np.asarray(capacities, dtype=float))
    cu = np.broadcast_to(np.asarray(cost_up, dtype=float), caps.shape)
    cd = np.broadcast_to(np.asarray(cost_down, dtype=float), caps.shape)
    r, n = caps.shape
    q = np.zeros((r, n))
    converged = np.zeros(r, dtype=bool)
    for _ in range(max_iter):
        biggest = np.zeros(r)
        for i in range(n):
            others = q.sum(axis=1) - q[:, i]

            def own(x: FloatArray, others: FloatArray = others) -> FloatArray:
                return price_of_total(x + others[:, None])

            br = best_response(own, caps[:, i], cu[:, i], cd[:, i], grid_u, refine_steps=refine_steps)
            new = q[:, i] + damping * (br - q[:, i])
            biggest = np.maximum(biggest, np.abs(new - q[:, i]))
            q[:, i] = new
        converged = biggest <= tol_mw
        if converged.all():
            break
    return q, converged


def linear_cournot_solution(a: float, b: float, s: float, cost: float, n_fleets: int) -> float:
    """Analytic symmetric equilibrium of ``P(Q) = a - b (s + Q)`` with marginal cost ``cost`` (unbounded capacity)."""
    return max((a - b * s - cost) / (b * (n_fleets + 1.0)), 0.0)


class TablePrice:
    """Price as a function of the total deviation, tabulated on a uniform grid per row (linear interpolation)."""

    def __init__(self, lo: FloatArray, step: FloatArray, values: FloatArray) -> None:
        self.lo = np.asarray(lo, dtype=float)
        self.step = np.where(np.asarray(step, dtype=float) > 0.0, step, 1.0)
        self.values = np.asarray(values, dtype=float)

    def __call__(self, q: FloatArray) -> FloatArray:
        qq = np.asarray(q, dtype=float)
        two_d = qq.ndim == 2
        qq2 = qq if two_d else qq[:, None]
        pos = (qq2 - self.lo[:, None]) / self.step[:, None]
        g = self.values.shape[1]
        i = np.clip(np.floor(pos).astype(np.int64), 0, g - 2)
        w = np.clip(pos - i, 0.0, 1.0)
        lo_v = np.take_along_axis(self.values, i, axis=1)
        hi_v = np.take_along_axis(self.values, i + 1, axis=1)
        out: FloatArray = lo_v + w * (hi_v - lo_v)
        return out if two_d else out[:, 0]


# --------------------------------------------------------------------------------------------------------------
# running price response for the minute loop
# --------------------------------------------------------------------------------------------------------------


class RunningResponse:
    """Counterfactual *running* price ``pi(X_run[r], q)`` for one visible minute row and many deviations.

    ``price(r, q, q_prev)`` accepts ``q`` of shape ``(S,)`` or ``(S, G)`` and the previous quarter-hour deviation
    ``q_prev`` ``(S,)``; ``base[r]`` is the model price at zero deviation, ``signal[r]`` the published running price.
    Uses the same displacement / curve / alpha logic as the quarter-hour operators (scalar fast path).
    """

    def __init__(
        self,
        mechanism: str,
        market: MarketData,
        fit: CurrentResponseFit | NewFormulaResponseFit,
        *,
        volume_map: VolumeFromSI | None = None,
    ) -> None:
        self.mechanism = mechanism
        self.fit = fit
        self.signal = np.asarray(market.require_minute(SIGNAL_FIELDS[mechanism]), dtype=float)
        n = market.n_minutes
        if mechanism == "current":
            if not isinstance(fit, CurrentResponseFit) or volume_map is None:
                raise TypeError("current mechanism needs a CurrentResponseFit and a VolumeFromSI map")
            self._cur_state = current_running_state_from_si(market, fit, volume_map)
            self._cur_kwargs: dict[str, Any] = {
                "up": CurveDelta(fit.mip),
                "down": CurveDelta(fit.mdp),
                "alpha_model": fit.alpha,
                "alpha_mode": fit.alpha_mode,
                "pass_through": fit.pass_through,
            }
            self.base = current_price(self._cur_state, np.zeros(n), **self._cur_kwargs)
        elif mechanism == "new2026":
            if not isinstance(fit, NewFormulaResponseFit):
                raise TypeError("new2026 mechanism needs a NewFormulaResponseFit")
            self._nf_state = new_running_state(market, regime_edges_hours=fit.regime_edges_hours)
            self._curves = fit.curves()
            self.base = new_formula_price(
                self._nf_state,
                np.zeros(n),
                curves=self._curves,
                pass_through=fit.pass_through,
                displacement=fit.displacement,
            )
        else:
            raise KeyError(f"unknown mechanism `{mechanism}`")
        self.n = n

    def price(self, r: int, q: FloatArray, q_prev: FloatArray) -> FloatArray:
        """Model running price at row ``r`` for deviations ``q`` ((S,) or (S, G)) and previous-QH deviations."""
        qq = np.asarray(q, dtype=float)
        rows = np.full(qq.shape[0], r, dtype=np.int64)
        if self.mechanism == "current":
            state = self._cur_state.take(rows)
            qp = np.broadcast_to(np.asarray(q_prev, dtype=float), (qq.shape[0],))
            out: FloatArray = current_price(state, qq, q_prev=qp, **self._cur_kwargs)
            return out
        fit_n = self.fit
        assert isinstance(fit_n, NewFormulaResponseFit)
        state_n = self._nf_state.take(rows)
        out_n: FloatArray = new_formula_price(
            state_n, qq, curves=self._curves, pass_through=fit_n.pass_through, displacement=fit_n.displacement
        )
        return out_n


# --------------------------------------------------------------------------------------------------------------
# fleets and the game loop
# --------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class FleetSpec:
    """One implicit-balancing fleet (aggregated batteries with the template's duration and efficiencies)."""

    name: str
    power_mw: float
    lag_minutes: int = 1
    upper_quantile: float = 0.75
    lower_quantile: float = 0.25


def make_fleets(total_mw: float, n_fleets: int, *, heterogeneous: bool) -> tuple[FleetSpec, ...]:
    """Split ``total_mw`` equally into ``n_fleets`` fleets; heterogeneous fleets differ in lag and bands."""
    lags = (1, 1, 2, 3, 2, 1, 3, 2)
    uppers = (0.75, 0.70, 0.80, 0.75, 0.70, 0.80, 0.75, 0.70)
    lowers = (0.25, 0.30, 0.20, 0.25, 0.30, 0.20, 0.25, 0.30)
    fleets = []
    for i in range(n_fleets):
        if heterogeneous:
            fleets.append(FleetSpec(f"fleet{i + 1}", total_mw / n_fleets, lags[i % 8], uppers[i % 8], lowers[i % 8]))
        else:
            fleets.append(FleetSpec(f"fleet{i + 1}", total_mw / n_fleets))
    return tuple(fleets)


@dataclass
class GameResult:
    """Outcome of one (policy, total capacity) scenario."""

    policy: str
    mechanism: str
    total_mw: float
    n_fleets: int
    summary: dict[str, Any]
    qh: pd.DataFrame = field(repr=False)
    fleets: pd.DataFrame = field(repr=False)


def overshoot_flags(si: FloatArray, q: FloatArray, *, tol_mw: float = 1e-6) -> dict[str, FloatArray]:
    """Per-QH flags: active, helping (deviation opposite to SI), overshoot (sign flip), harmful (|SI'| > |SI|)."""
    s = np.asarray(si, dtype=float)
    qq = np.asarray(q, dtype=float)
    active = np.abs(qq) > tol_mw
    helping = active & (qq * s < 0.0)
    overshoot = helping & (np.abs(qq) > np.abs(s))
    harmful = active & (np.abs(s + qq) > np.abs(s))
    return {"active": active, "helping": helping, "overshoot": overshoot, "harmful": harmful}


def _qh_mean(minute_values: FloatArray, n_qh: int) -> FloatArray:
    out: FloatArray = minute_values.reshape(minute_values.shape[:-1] + (n_qh, MINUTES_PER_QH)).mean(axis=-1)
    return out


def simulate_fleet_game(
    market: MarketData,
    mechanism: str,
    *,
    policy: str,
    scenarios: Sequence[tuple[FleetSpec, ...]],
    settle: ImpactOperator,
    running: RunningResponse,
    cost_operator: CurrentFormulaImpact | None = None,
    battery: BatteryParams | None = None,
    publication: PublicationModel | None = None,
    grid_points: int = 41,
    table_points: int = 121,
    equilibrium_lag: int = 1,
    lookback_qh: int = 96 * 14,
    min_spread_eur_mwh: float = 20.0,
    min_history_qh: int = 96,
    progress: Callable[[str], None] | None = None,
) -> list[GameResult]:
    """Run ``policy`` for several fleet configurations (vectorised; each scenario has the same number of fleets).

    ``battery`` is the per-MW template (duration, efficiencies, SOC band, degradation); fleet ``i`` is that template
    scaled to ``power_mw``. Returns one :class:`GameResult` per scenario.
    """
    if policy not in FLEET_POLICIES:
        raise KeyError(f"unknown fleet policy `{policy}`; known: {FLEET_POLICIES}")
    battery = battery or BatteryParams(power_mw=1.0, energy_mwh=2.0)
    publication = publication or PublicationModel()
    n_s = len(scenarios)
    n_f = len(scenarios[0])
    if any(len(s) != n_f for s in scenarios):
        raise ValueError("all scenarios must have the same number of fleets")
    T, n_qh = market.n_minutes, market.n_qh
    cap = np.array([[f.power_mw for f in s] for s in scenarios], dtype=float)  # (S, N)
    p_max = cap * battery.availability
    energy = cap * battery.energy_mwh / battery.power_mw
    soc_min, soc_max = energy * battery.soc_min_frac, energy * battery.soc_max_frac
    soc = energy * battery.initial_soc_frac
    eta_c, eta_d = battery.charge_efficiency, battery.discharge_efficiency
    dt = 1.0 / 60.0
    lags = np.array([[f.lag_minutes for f in s] for s in scenarios], dtype=np.int64)
    prices_pub = market.settlement(mechanism)
    band_cache: dict[tuple[float, float], tuple[FloatArray, FloatArray]] = {}
    band_lo = np.zeros((n_s, n_f, n_qh))
    band_up = np.zeros((n_s, n_f, n_qh))
    for si_, sc in enumerate(scenarios):
        for fi, f in enumerate(sc):
            key = (f.upper_quantile, f.lower_quantile)
            if key not in band_cache:
                band_cache[key] = threshold_bands(
                    prices_pub,
                    n_qh,
                    publication,
                    lookback_qh=lookback_qh,
                    upper_quantile=f.upper_quantile,
                    lower_quantile=f.lower_quantile,
                    min_spread_eur_mwh=min_spread_eur_mwh,
                    min_history_qh=min_history_qh,
                    default_upper_eur_mwh=150.0,
                    default_lower_eur_mwh=0.0,
                )
            band_lo[si_, fi], band_up[si_, fi] = band_cache[key]
    grid_u = np.linspace(-1.0, 1.0, grid_points)
    total_cap = cap.sum(axis=1)
    table_cap = total_cap * battery.availability
    table_u = np.linspace(-1.0, 1.0, table_points)
    q_table_grid = table_u[None, :] * table_cap[:, None]
    table_step = 2.0 * table_cap / (table_points - 1)
    fleet_energy = np.zeros((n_s, n_f, n_qh))
    fleet_dis = np.zeros((n_s, n_f, n_qh))
    fleet_chg = np.zeros((n_s, n_f, n_qh))
    total_power = np.zeros((n_s, T))
    sig_cf = np.full((n_s, T), np.nan)
    qh_sum = np.zeros(n_s)
    q_prev = np.zeros(n_s)
    converged_count = 0
    decisions = 0
    nash_prev = np.zeros(n_s)
    herding = policy.startswith("herding")
    report_every = max(T // 10, 1)
    for t in range(T):
        k, m = divmod(t, MINUTES_PER_QH)
        if m == 0:
            if t > 0:
                q_prev = qh_sum / MINUTES_PER_QH
            qh_sum = np.zeros(n_s)
        if herding:
            r = t - lags - 1  # (S, N)
            valid = r >= 0
            obs = np.where(valid, sig_cf[np.arange(n_s)[:, None], np.clip(r, 0, None)], np.nan)
            up_k, lo_k = band_up[:, :, k], band_lo[:, :, k]
            u = np.where(obs >= up_k, 1.0, np.where(obs <= lo_k, -1.0, 0.0))
            request = u * p_max
        else:
            r0 = t - equilibrium_lag - 1
            if r0 < 0 or not math.isfinite(running.signal[r0]):
                request = np.zeros((n_s, n_f))
            else:
                shift = running.signal[r0] - running.base[r0]
                table_vals = running.price(r0, q_table_grid, q_prev) + shift
                table_vals = np.where(np.isfinite(table_vals), table_vals, running.signal[r0])
                table = TablePrice(-table_cap, table_step, table_vals)
                c_up, c_lo = band_up[:, 0, k], band_lo[:, 0, k]
                if policy == "central":
                    q_tot = best_response(table, table_cap, c_up, c_lo, grid_u, refine_steps=0)
                    request = q_tot[:, None] * (cap / total_cap[:, None])
                elif policy == "naive":
                    request = np.zeros((n_s, n_f))
                    done: dict[tuple[float, float], int] = {}
                    for fi in range(n_f):
                        key = (scenarios[0][fi].upper_quantile, scenarios[0][fi].lower_quantile)
                        same_cap = fi > 0 and np.allclose(p_max[:, fi], p_max[:, done.get(key, 0)])
                        if key in done and same_cap:
                            request[:, fi] = request[:, done[key]]
                            continue
                        request[:, fi] = best_response(
                            table, p_max[:, fi], band_up[:, fi, k], band_lo[:, fi, k], grid_u, refine_steps=0
                        )
                        done.setdefault(key, fi)
                else:  # nash (symmetric fleets), warm-started from the previous minute
                    q_s, conv = symmetric_equilibrium(
                        table,
                        n_f,
                        p_max[:, 0],
                        c_up,
                        c_lo,
                        grid_u,
                        max_iter=20,
                        tol_mw=0.5,
                        refine_steps=0,
                        initial=nash_prev,
                    )
                    nash_prev = q_s
                    converged_count += int(conv.sum())
                    decisions += len(conv)
                    request = np.repeat(q_s[:, None], n_f, axis=1)
        # battery physics (vectorised over scenarios and fleets)
        p = np.clip(request, -p_max, p_max)
        room_dis = np.maximum((soc - soc_min) * eta_d / dt, 0.0)
        room_chg = np.maximum((soc_max - soc) / (eta_c * dt), 0.0)
        p = np.where(p > 0.0, np.minimum(p, room_dis), np.maximum(p, -room_chg))
        soc = np.where(p > 0.0, soc - p * dt / eta_d, soc - p * dt * eta_c)
        soc = np.clip(soc, soc_min, soc_max)
        fleet_energy[:, :, k] += p * dt
        fleet_dis[:, :, k] += np.clip(p, 0.0, None) * dt
        fleet_chg[:, :, k] += np.clip(-p, 0.0, None) * dt
        f_tot = p.sum(axis=1)
        total_power[:, t] = f_tot
        qh_sum = qh_sum + f_tot
        if herding:
            f_cum = qh_sum / (m + 1)
            if math.isfinite(running.signal[t]):
                sig_cf[:, t] = running.signal[t] + running.price(t, f_cum, q_prev) - running.base[t]
        if progress is not None and t % report_every == 0:
            progress(f"{policy} {mechanism}: minute {t}/{T}")
    # settlement and metrics
    q_tot_qh = _qh_mean(total_power, n_qh)  # (S, n_qh)
    base_price = np.asarray(market.settlement(mechanism), dtype=float)
    si = np.asarray(settle.state.si_mw, dtype=float)
    results: list[GameResult] = []
    years = market.days / 365.25
    idx = np.arange(n_qh, dtype=np.int64)
    if cost_operator is not None:
        cost_state = cost_operator.state
        kappa = cost_operator.fit.pass_through
        u0, d0 = cost_state.up_mw, cost_state.down_mw
    for si_, sc in enumerate(scenarios):
        q = q_tot_qh[si_]
        price = settle.adjust(idx, base_price, q)
        missing = np.isnan(price)
        revenue = np.where(missing[None, :], 0.0, fleet_energy[si_] * np.nan_to_num(price)[None, :])
        degradation = fleet_dis[si_] * battery.degradation_eur_per_mwh
        pnl = revenue - degradation
        flags = overshoot_flags(si, q)
        active_n = max(int(flags["active"].sum()), 1)
        resid = np.abs(si + q) * 0.25
        resid0 = np.abs(si) * 0.25
        summary: dict[str, Any] = {
            "policy": policy,
            "mechanism": mechanism,
            "total_mw": float(cap[si_].sum()),
            "n_fleets": n_f,
            "eur_per_mw_year": float(pnl.sum() / cap[si_].sum() / years) if years > 0 else float("nan"),
            "pnl_eur": float(pnl.sum()),
            "cycles_per_day": float(
                fleet_dis[si_].sum() / (energy[si_].sum() * (battery.soc_max_frac - battery.soc_min_frac)) / market.days
            ),
            "active_qh_share": float(flags["active"].mean()),
            "overshoot_share": float(flags["overshoot"].sum() / active_n),
            "overshoot_qh_share": float(flags["overshoot"].mean()),
            "harmful_share": float(flags["harmful"].sum() / active_n),
            "residual_imbalance_change_mwh_per_year": float((np.nansum(resid) - np.nansum(resid0)) / years),
            "residual_imbalance_change_pct": float(100.0 * (np.nansum(resid) / np.nansum(resid0) - 1.0)),
            "mean_abs_price_impact_eur_mwh": float(
                np.nanmean(np.abs(price - base_price)[flags["active"]]) if flags["active"].any() else 0.0
            ),
            "fleet_value_eur_per_mw_year": [float(pnl[fi].sum() / cap[si_, fi] / years) for fi in range(n_f)],
        }
        if decisions:
            summary["equilibrium_converged_share"] = converged_count / decisions
        if cost_operator is not None:
            u2, d2 = displace(u0, d0, q, kappa)
            mip2 = cost_state.mip_eur_mwh + cost_operator.fit.mip.delta(cost_state.regime, u0, u2)
            mdp2 = cost_state.mdp_eur_mwh + cost_operator.fit.mdp.delta(cost_state.regime, d0, d2)
            cost = 0.25 * (u2 * mip2 - d2 * mdp2)
            cost0 = 0.25 * (u0 * cost_state.mip_eur_mwh - d0 * cost_state.mdp_eur_mwh)
            summary["activation_cost_change_eur_per_year"] = float((np.nansum(cost) - np.nansum(cost0)) / years)
            summary["activation_cost_change_pct"] = float(100.0 * (np.nansum(cost) / np.nansum(cost0) - 1.0))
        qh = pd.DataFrame(
            {
                "timestamp_utc": market.qh_times,
                "si_mw": si,
                "fleet_delta_mw": q,
                "base_price_eur_mwh": base_price,
                "price_eur_mwh": price,
                "overshoot": flags["overshoot"],
                "pnl_eur": pnl.sum(axis=0),
            }
        )
        fleets_df = pd.DataFrame(
            {
                "fleet": [f.name for f in sc],
                "power_mw": [f.power_mw for f in sc],
                "lag_minutes": [f.lag_minutes for f in sc],
                "upper_quantile": [f.upper_quantile for f in sc],
                "lower_quantile": [f.lower_quantile for f in sc],
                "pnl_eur": pnl.sum(axis=1),
                "eur_per_mw_year": summary["fleet_value_eur_per_mw_year"],
            }
        )
        results.append(
            GameResult(
                policy=policy,
                mechanism=mechanism,
                total_mw=float(cap[si_].sum()),
                n_fleets=n_f,
                summary=summary,
                qh=qh,
                fleets=fleets_df,
            )
        )
    return results


def regime_ids(times: pd.DatetimeIndex, fit: CurrentResponseFit | NewFormulaResponseFit, timezone: str) -> np.ndarray:
    return local_hour_regime(times, timezone=timezone, edges_hours=fit.regime_edges_hours)
