"""Linear programs for the GB backtest (HiGHS).

* :class:`StorageLP` - a reusable H-period storage schedule (discharge/charge/SOC) with separate sell and buy
  prices, per-period net-power limits (headroom left by response contracts), a soft SOC band (response energy
  volumes) and an expected SOC drift (response delivery). The model is built once and only costs and bounds change
  between solves, so rolling re-planning is fast.
* :func:`solve_stack_lp` - joint choice of dynamic response MW per EFA block and product together with a baseline
  schedule (day-ahead allocation and perfect-foresight oracles).

Power sign convention as in :mod:`euroflex_bess_lab.realtime`: discharge (export) is positive.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import highspy
import numpy as np
from numpy.typing import NDArray
from scipy import sparse
from scipy.optimize import linprog

from ...realtime.types import BatteryParams
from .rules import (
    DELIVERY_DURATION_HOURS,
    HIGH_PRODUCTS,
    LOW_PRODUCTS,
    RESPONSE_PRODUCTS,
    SP_HOURS,
    StackingRules,
)

FloatArray = NDArray[np.float64]
INF = highspy.kHighsInf
SOC_PENALTY_GBP_PER_MWH = 5000.0
CHARGE_TIEBREAK_GBP_PER_MWH = 1e-3
SIMULTANEOUS_TOL = 1e-6
MAX_DEBURN_ITERATIONS = 12


@dataclass(frozen=True)
class StoragePlan:
    discharge_mw: FloatArray
    charge_mw: FloatArray
    soc_end_mwh: FloatArray
    objective: float
    soc_violation_mwh: float

    @property
    def net_mw(self) -> FloatArray:
        return self.discharge_mw - self.charge_mw


class StorageLP:
    """Storage LP over ``horizon`` periods of ``dt_hours`` with incremental re-solves.

    Columns: d (discharge MW), c (charge MW), s (SOC at period end, MWh), u / o (SOC band slack below / above).
    Rows: SOC dynamics, net power up / down limits, soft SOC lower / upper band.
    """

    def __init__(
        self,
        horizon: int,
        battery: BatteryParams,
        *,
        dt_hours: float = SP_HOURS,
        soc_penalty: float = SOC_PENALTY_GBP_PER_MWH,
        charge_tiebreak: float = CHARGE_TIEBREAK_GBP_PER_MWH,
    ) -> None:
        if horizon < 1:
            raise ValueError("horizon must be positive")
        self.h = int(horizon)
        self.battery = battery
        self.dt = float(dt_hours)
        self.penalty = float(soc_penalty)
        self.tiebreak = float(charge_tiebreak)
        n = self.h
        self._cols = 5 * n
        self._rows = 5 * n
        self._d = np.arange(n, dtype=np.int32)
        self._c = self._d + n
        self._s = self._d + 2 * n
        self._u = self._d + 3 * n
        self._o = self._d + 4 * n
        eta_c, eta_d = battery.charge_efficiency, battery.discharge_efficiency
        rows: list[NDArray[np.int64]] = []
        cols: list[NDArray[np.int64]] = []
        vals: list[FloatArray] = []

        def add(r: NDArray[np.int64], c: NDArray[np.int64], v: float | FloatArray) -> None:
            rows.append(r)
            cols.append(c)
            vals.append(np.broadcast_to(np.asarray(v, dtype=float), r.shape).astype(float))

        k = np.arange(n, dtype=np.int64)
        d_idx, c_idx, s_idx = k, k + n, k + 2 * n
        # dynamics rows 0..n-1: s_k - s_{k-1} + dt/eta_d d_k - dt*eta_c c_k = drift_k (+ soc0 for k = 0)
        add(k, s_idx, 1.0)
        add(k[1:], s_idx[:-1], -1.0)
        add(k, d_idx, self.dt / eta_d)
        add(k, c_idx, -self.dt * eta_c)
        # net up rows n..2n-1: d - c <= up ; net down rows 2n..3n-1: c - d <= down
        add(k + n, d_idx, 1.0)
        add(k + n, c_idx, -1.0)
        add(k + 2 * n, c_idx, 1.0)
        add(k + 2 * n, d_idx, -1.0)
        # soft lower rows 3n..4n-1: s + u >= lb ; soft upper rows 4n..5n-1: s - o <= ub
        add(k + 3 * n, s_idx, 1.0)
        add(k + 3 * n, k + 3 * n, 1.0)
        add(k + 4 * n, s_idx, 1.0)
        add(k + 4 * n, k + 4 * n, -1.0)
        matrix = sparse.csc_matrix(
            (np.concatenate(vals), (np.concatenate(rows), np.concatenate(cols))), shape=(self._rows, self._cols)
        )
        lp = highspy.HighsLp()
        lp.num_col_ = self._cols
        lp.num_row_ = self._rows
        lp.col_cost_ = np.zeros(self._cols)
        upper = np.full(self._cols, INF)
        upper[: 2 * n] = battery.max_power_mw
        lp.col_upper_ = upper
        lower = np.zeros(self._cols)
        lower[2 * n : 3 * n] = -INF  # SOC itself is free; the band is enforced through the soft rows
        lp.col_lower_ = lower
        lp.row_lower_ = np.concatenate([np.zeros(n), np.full(2 * n, -INF), np.zeros(n), np.full(n, -INF)])
        lp.row_upper_ = np.concatenate([np.zeros(n), np.zeros(2 * n), np.full(n, INF), np.zeros(n)])
        lp.a_matrix_.format_ = highspy.MatrixFormat.kColwise
        lp.a_matrix_.start_ = matrix.indptr.astype(np.int32)
        lp.a_matrix_.index_ = matrix.indices.astype(np.int32)
        lp.a_matrix_.value_ = matrix.data
        self._highs: Any = highspy.Highs()
        self._highs.setOptionValue("output_flag", False)
        self._highs.passModel(lp)
        self._all_cols = np.arange(self._cols, dtype=np.int32)
        self._all_rows = np.arange(self._rows, dtype=np.int32)

    def solve(
        self,
        price_sell: FloatArray,
        *,
        soc0: float,
        price_buy: FloatArray | None = None,
        drift: FloatArray | None = None,
        net_up: FloatArray | None = None,
        net_down: FloatArray | None = None,
        soc_lb: FloatArray | None = None,
        soc_ub: FloatArray | None = None,
        degradation: FloatArray | float | None = None,
        terminal_value: float = 0.0,
        discharge_ub: FloatArray | None = None,
        charge_ub: FloatArray | None = None,
        deburn: bool = False,
    ) -> StoragePlan:
        """Maximise ``sum dt*(p_sell d - p_buy c) - dt*deg*d + terminal_value*s_end`` for ``len(price_sell)``
        periods (``<= horizon``; remaining periods are inert).

        An LP may charge and discharge in the same period to dissipate energy (negative prices or a SOC band it
        cannot otherwise meet), which a battery cannot do. ``deburn=True`` forbids that by re-solving with the
        smaller leg of such periods fixed to zero (used by every GB strategy and oracle)."""

        n_used = len(price_sell)
        if n_used == 0 or n_used > self.h:
            raise ValueError(f"need 1..{self.h} periods, got {n_used}")
        b = self.battery
        n = self.h

        def pad(values: FloatArray | float | None, default: float) -> FloatArray:
            out = np.full(n, default, dtype=float)
            if values is None:
                return out
            arr = np.broadcast_to(np.asarray(values, dtype=float), (n_used,))
            out[:n_used] = np.nan_to_num(arr, nan=default)
            return out

        p_sell = pad(price_sell, 0.0)
        p_buy = pad(price_buy if price_buy is not None else price_sell, 0.0)
        deg = pad(degradation if degradation is not None else b.degradation_eur_per_mwh, 0.0)
        active = np.zeros(n, dtype=bool)
        active[:n_used] = True
        cost = np.zeros(self._cols)
        cost[self._d] = np.where(active, -self.dt * p_sell + self.dt * deg, 0.0)
        cost[self._c] = np.where(active, self.dt * p_buy + self.dt * self.tiebreak, 0.0)
        cost[self._s[n_used - 1]] = -float(terminal_value)
        cost[self._u] = self.penalty
        cost[self._o] = self.penalty
        col_low = np.zeros(self._cols)
        col_low[self._s] = -INF
        col_up = np.full(self._cols, INF)
        d_ub = pad(discharge_ub, b.max_power_mw)
        c_ub = pad(charge_ub, b.max_power_mw)
        col_up[self._d] = np.where(active, np.clip(d_ub, 0.0, b.max_power_mw), 0.0)
        col_up[self._c] = np.where(active, np.clip(c_ub, 0.0, b.max_power_mw), 0.0)
        dyn = pad(drift, 0.0)
        dyn[0] += float(soc0)
        up = pad(net_up, b.max_power_mw)
        down = pad(net_down, b.max_power_mw)
        lb = pad(soc_lb, b.soc_min_mwh)
        ub = pad(soc_ub, b.soc_max_mwh)
        lb[~active] = -INF
        ub[~active] = INF
        row_low = np.concatenate([dyn, np.full(2 * n, -INF), lb, np.full(n, -INF)])
        row_up = np.concatenate([dyn, up, down, np.full(n, INF), ub])
        h = self._highs
        h.changeColsCost(self._cols, self._all_cols, cost)
        h.changeRowsBounds(self._rows, self._all_rows, row_low, row_up)
        x = np.zeros(self._cols)
        for _ in range(MAX_DEBURN_ITERATIONS if deburn else 1):
            h.changeColsBounds(self._cols, self._all_cols, col_low, col_up)
            h.run()
            status = h.getModelStatus()
            if status != highspy.HighsModelStatus.kOptimal:
                raise RuntimeError(f"storage LP not optimal: {status}")
            x = np.asarray(h.getSolution().col_value)
            both = np.flatnonzero(np.minimum(x[self._d[:n_used]], x[self._c[:n_used]]) > SIMULTANEOUS_TOL)
            if not deburn or len(both) == 0:
                break
            keep_discharge = x[self._d[both]] >= x[self._c[both]]
            col_up[self._c[both[keep_discharge]]] = 0.0
            col_up[self._d[both[~keep_discharge]]] = 0.0
        violation = float(x[self._u[:n_used]].sum() + x[self._o[:n_used]].sum())
        return StoragePlan(
            discharge_mw=x[self._d[:n_used]].copy(),
            charge_mw=x[self._c[:n_used]].copy(),
            soc_end_mwh=x[self._s[:n_used]].copy(),
            objective=-float(h.getInfo().objective_function_value),
            soc_violation_mwh=violation,
        )


def rolling_oracle(
    price_sell: FloatArray,
    battery: BatteryParams,
    *,
    price_buy: FloatArray | None = None,
    degradation: FloatArray | None = None,
    window: int = 7 * 48,
    overlap: int = 48,
    soc0: float | None = None,
) -> StoragePlan:
    """Perfect-foresight schedule in rolling windows (commit ``window`` periods, look ``overlap`` further)."""

    n = len(price_sell)
    lp = StorageLP(window + overlap, battery)
    d = np.zeros(n)
    c = np.zeros(n)
    s = np.zeros(n)
    soc = battery.initial_soc_mwh if soc0 is None else float(soc0)
    start = 0
    while start < n:
        stop = min(n, start + window + overlap)
        plan = lp.solve(
            price_sell[start:stop],
            soc0=soc,
            price_buy=None if price_buy is None else price_buy[start:stop],
            degradation=None if degradation is None else degradation[start:stop],
            deburn=True,
        )
        commit = min(window, n - start)
        d[start : start + commit] = plan.discharge_mw[:commit]
        c[start : start + commit] = plan.charge_mw[:commit]
        s[start : start + commit] = plan.soc_end_mwh[:commit]
        soc = float(plan.soc_end_mwh[commit - 1])
        start += commit
    return StoragePlan(d, c, s, float("nan"), 0.0)


# ------------------------------------------------------------------------------------- response + baseline


@dataclass(frozen=True)
class StackProblem:
    """Inputs of :func:`solve_stack_lp` for ``n`` consecutive SPs.

    ``free_block[k]`` is the index (into ``block_price``/``block_hours``) of the EFA block whose response MW are
    decided by this LP, or -1 when SP ``k`` has fixed contracts ``fixed_mw[product][k]`` (zero by default).
    ``block_price[b][product]`` is the (forecast or realised) clearing price; ``energy[product][k]`` the (expected or
    realised) MWh delivered per MW in SP ``k``. ``price_sell``/``price_buy`` value baseline energy; ``None`` disables
    the baseline (response only, SOC must then stay feasible on its own).
    """

    price_sell: FloatArray | None
    price_buy: FloatArray | None
    free_block: NDArray[np.int64]
    block_price: Sequence[Mapping[str, float]]
    block_hours: FloatArray
    energy: Mapping[str, FloatArray]
    soc0: float
    degradation: FloatArray
    terminal_value: float = 0.0
    fixed_mw: Mapping[str, FloatArray] | None = None
    products: tuple[str, ...] = RESPONSE_PRODUCTS
    max_product_mw: float | None = None


@dataclass(frozen=True)
class StackSolution:
    block_mw: list[dict[str, float]]
    discharge_mw: FloatArray
    charge_mw: FloatArray
    soc_end_mwh: FloatArray
    objective: float
    soc_violation_mwh: float


def solve_stack_lp(
    problem: StackProblem, battery: BatteryParams, rules: StackingRules | None = None, *, dt: float = SP_HOURS
) -> StackSolution:
    """Joint response allocation (per free EFA block and product) and baseline schedule (one LP, HiGHS)."""

    rules = rules or StackingRules()
    n = len(problem.free_block)
    n_blocks = len(problem.block_hours)
    products = list(problem.products)
    n_prod = len(products)
    eta_c, eta_d = battery.charge_efficiency, battery.discharge_efficiency
    p_max = battery.max_power_mw
    use_baseline = problem.price_sell is not None
    # column layout
    v0 = 0
    nv = n_blocks * n_prod
    d0 = v0 + nv
    c0 = d0 + n
    s0 = c0 + n
    u0 = s0 + n
    o0 = u0 + n + 1  # +1 slack for the initial SOC requirement
    n_cols = o0 + n + 1

    def vcol(b: int, j: int) -> int:
        return v0 + b * n_prod + j

    cost = np.zeros(n_cols)
    lower = np.zeros(n_cols)
    upper = np.full(n_cols, np.inf)
    cap = p_max if problem.max_product_mw is None else min(p_max, problem.max_product_mw)
    upper[v0 : v0 + nv] = cap
    upper[d0 : d0 + n] = p_max if use_baseline else 0.0
    upper[c0 : c0 + n] = p_max if use_baseline else 0.0
    lower[s0 : s0 + n] = -np.inf
    upper[s0 : s0 + n] = np.inf
    if use_baseline:
        assert problem.price_sell is not None
        p_sell = np.nan_to_num(np.asarray(problem.price_sell, dtype=float))
        p_buy = np.nan_to_num(np.asarray(problem.price_buy if problem.price_buy is not None else p_sell, dtype=float))
        cost[d0 : d0 + n] = -dt * p_sell + dt * problem.degradation
        cost[c0 : c0 + n] = dt * p_buy + dt * CHARGE_TIEBREAK_GBP_PER_MWH
    cost[s0 + n - 1] = -problem.terminal_value
    cost[u0:n_cols] = SOC_PENALTY_GBP_PER_MWH
    for b in range(n_blocks):
        for j, product in enumerate(products):
            price = float(problem.block_price[b].get(product, np.nan))
            if not np.isfinite(price):
                upper[vcol(b, j)] = 0.0  # no clearing price (missing auction): nothing can be contracted
                continue
            cost[vcol(b, j)] -= price * float(problem.block_hours[b])
    # response export degradation (per MWh delivered by low products)
    fixed = {p: np.zeros(n) for p in products}
    if problem.fixed_mw is not None:
        for p in products:
            if p in problem.fixed_mw:
                fixed[p] = np.nan_to_num(np.asarray(problem.fixed_mw[p], dtype=float))
    energy = {p: np.nan_to_num(np.asarray(problem.energy[p], dtype=float)) for p in products}
    free = np.asarray(problem.free_block, dtype=np.int64)
    for j, product in enumerate(products):
        if product in LOW_PRODUCTS:
            for kk in np.flatnonzero(free >= 0):
                cost[vcol(int(free[kk]), j)] += problem.degradation[kk] * energy[product][kk]
    cost = np.nan_to_num(cost, nan=0.0)

    r_eq: list[int] = []
    c_eq: list[int] = []
    v_eq: list[float] = []
    b_eq = np.zeros(n)
    r_ub: list[int] = []
    c_ub: list[int] = []
    v_ub: list[float] = []
    b_ub: list[float] = []

    def ub_row(entries: list[tuple[int, float]], rhs: float) -> None:
        row = len(b_ub)
        for col, val in entries:
            r_ub.append(row)
            c_ub.append(col)
            v_ub.append(val)
        b_ub.append(rhs)

    low_idx = [j for j, p in enumerate(products) if p in LOW_PRODUCTS]
    high_idx = [j for j, p in enumerate(products) if p in HIGH_PRODUCTS]
    dur = np.array([DELIVERY_DURATION_HOURS[p] for p in products])
    e_scale_low = 1.0 / eta_d if rules.apply_efficiency_to_energy_volume else 1.0
    e_scale_high = eta_c if rules.apply_efficiency_to_energy_volume else 1.0

    for k in range(n):
        # dynamics: s_k - s_{k-1} + dt/eta_d d_k - dt eta_c c_k + sum_low e/eta_d v - sum_high e eta_c v = fixed drift
        r_eq += [k]
        c_eq += [s0 + k]
        v_eq += [1.0]
        if k > 0:
            r_eq += [k]
            c_eq += [s0 + k - 1]
            v_eq += [-1.0]
        r_eq += [k, k]
        c_eq += [d0 + k, c0 + k]
        v_eq += [dt / eta_d, -dt * eta_c]
        fixed_drift = 0.0
        for j, product in enumerate(products):
            coef = energy[product][k] / eta_d if j in low_idx else -energy[product][k] * eta_c
            if free[k] >= 0:
                r_eq.append(k)
                c_eq.append(vcol(int(free[k]), j))
                v_eq.append(coef)
            else:
                fixed_drift -= coef * fixed[product][k]
        b_eq[k] = fixed_drift + (problem.soc0 if k == 0 else 0.0)

        # power: d - c + L <= P ; c - d + H <= P
        if free[k] >= 0:
            ent_l = [(vcol(int(free[k]), j), 1.0) for j in low_idx]
            ent_h = [(vcol(int(free[k]), j), 1.0) for j in high_idx]
            fix_l = fix_h = 0.0
        else:
            ent_l, ent_h = [], []
            fix_l = float(sum(fixed[products[j]][k] for j in low_idx))
            fix_h = float(sum(fixed[products[j]][k] for j in high_idx))
        ub_row([(d0 + k, 1.0), (c0 + k, -1.0), *ent_l], p_max - fix_l)
        ub_row([(c0 + k, 1.0), (d0 + k, -1.0), *ent_h], p_max - fix_h)

    # SOC band at period ends; at block boundaries the next block's requirement applies as well
    def requirement(k: int) -> tuple[list[tuple[int, float]], float, list[tuple[int, float]], float]:
        if k < 0 or k >= n:
            return [], 0.0, [], 0.0
        if free[k] >= 0:
            b = int(free[k])
            low = [(vcol(b, j), dur[j] * e_scale_low) for j in low_idx]
            high = [(vcol(b, j), dur[j] * e_scale_high) for j in high_idx]
            return low, 0.0, high, 0.0
        low_fix = float(sum(fixed[products[j]][k] * dur[j] for j in low_idx)) * e_scale_low
        high_fix = float(sum(fixed[products[j]][k] * dur[j] for j in high_idx)) * e_scale_high
        return [], low_fix, [], high_fix

    for k in range(-1, n):
        reqs = [requirement(k + 1)]
        if k >= 0:
            reqs.append(requirement(k))
        for low, low_fix, high, high_fix in reqs:
            if k >= 0:
                # -(s_k + u_k) + low_req <= -soc_min ; s_k - o_k + high_req <= soc_max
                ub_row([(s0 + k, -1.0), (u0 + k, -1.0), *low], -battery.soc_min_mwh - low_fix)
                ub_row([(s0 + k, 1.0), (o0 + k, -1.0), *high], battery.soc_max_mwh - high_fix)
            else:
                ub_row([(u0 + n, -1.0), *low], problem.soc0 - battery.soc_min_mwh - low_fix)
                ub_row([(o0 + n, -1.0), *high], battery.soc_max_mwh - problem.soc0 - high_fix)

    # procurement: offered MW per direction within the power rating, offered + reserved capacity per StackingRules
    procurement = rules.procurement_rows(products, p_max, p_max, charge_efficiency=eta_c, discharge_efficiency=eta_d)
    for b in range(n_blocks):
        for coef, rhs in procurement:
            ub_row([(vcol(b, j), float(coef[j])) for j in range(n_prod) if coef[j] != 0.0], rhs)

    a_eq = sparse.csr_matrix((v_eq, (r_eq, c_eq)), shape=(n, n_cols))
    a_ub = sparse.csr_matrix((v_ub, (r_ub, c_ub)), shape=(len(b_ub), n_cols))
    x = np.zeros(n_cols)
    result = None
    for _ in range(MAX_DEBURN_ITERATIONS):
        result = linprog(
            cost,
            A_ub=a_ub,
            b_ub=np.asarray(b_ub),
            A_eq=a_eq,
            b_eq=b_eq,
            bounds=np.column_stack([lower, upper]),
            method="highs",
        )
        if not result.success:
            raise RuntimeError(f"stack LP failed: {result.message}")
        x = result.x
        both = np.flatnonzero(np.minimum(x[d0 : d0 + n], x[c0 : c0 + n]) > SIMULTANEOUS_TOL)
        if not use_baseline or len(both) == 0:
            break
        # a battery cannot charge and discharge at once: keep the larger leg in those periods and re-solve
        keep_discharge = x[d0 + both] >= x[c0 + both]
        upper[c0 + both[keep_discharge]] = 0.0
        upper[d0 + both[~keep_discharge]] = 0.0
    assert result is not None
    block_mw = [{p: float(max(x[vcol(b, j)], 0.0)) for j, p in enumerate(products)} for b in range(n_blocks)]
    return StackSolution(
        block_mw=block_mw,
        discharge_mw=x[d0 : d0 + n].copy(),
        charge_mw=x[c0 : c0 + n].copy(),
        soc_end_mwh=x[s0 : s0 + n].copy(),
        objective=-float(result.fun),
        soc_violation_mwh=float(x[u0:n_cols].sum()),
    )
