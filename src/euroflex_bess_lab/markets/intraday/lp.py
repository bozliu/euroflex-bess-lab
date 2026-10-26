"""Linear programs of the two-stage strategy (HiGHS through ``highspy``, warm-started for rolling re-solves).

Conventions: quarter-hour steps of ``h = 0.25`` h; ``d`` / ``c`` = discharge / charge power (MW, >= 0); SOC in MWh
at the end of each quarter-hour; ``x`` = day-ahead position (MW, + = sold); ``zs`` / ``zb`` = intraday sell / buy
(MW). Physical net injection ``d - c = x + zs - zb``. ``d + c <= P`` is the convex hull of "charge or discharge":
with settlement on quarter-hour energy a battery can realise any such pair by switching mode inside the
quarter-hour, so no binary variables are needed. Day-ahead positions on hourly products are constant over the
four quarter-hours of the hour (``blocks``).
"""

from __future__ import annotations

from dataclasses import dataclass

import highspy
import numpy as np
from numpy.typing import NDArray
from scipy import sparse

from ...realtime.types import BatteryParams
from .config import QH_HOURS, TradingCosts

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]
_INF = highspy.kHighsInf


class LpError(RuntimeError):
    """Raised when HiGHS does not return an optimal solution."""


def _new_highs() -> highspy.Highs:
    model = highspy.Highs()
    model.setOptionValue("output_flag", False)
    model.setOptionValue("presolve", "off")
    return model


def _pass_lp(
    model: highspy.Highs,
    cost: FloatArray,
    col_lower: FloatArray,
    col_upper: FloatArray,
    matrix: sparse.csc_matrix,
    row_lower: FloatArray,
    row_upper: FloatArray,
) -> None:
    lp = highspy.HighsLp()
    n_col, n_row = len(cost), len(row_lower)
    lp.num_col_ = n_col
    lp.num_row_ = n_row
    lp.col_cost_ = np.asarray(cost, dtype=float)
    lp.col_lower_ = np.asarray(col_lower, dtype=float)
    lp.col_upper_ = np.asarray(col_upper, dtype=float)
    lp.row_lower_ = np.where(np.isfinite(row_lower), row_lower, -_INF)
    lp.row_upper_ = np.where(np.isfinite(row_upper), row_upper, _INF)
    csc = matrix.tocsc()
    csc.sort_indices()
    lp.a_matrix_.format_ = highspy.MatrixFormat.kColwise
    lp.a_matrix_.start_ = csc.indptr.astype(np.int32)
    lp.a_matrix_.index_ = csc.indices.astype(np.int32)
    lp.a_matrix_.value_ = csc.data.astype(float)
    lp.a_matrix_.num_col_ = n_col
    lp.a_matrix_.num_row_ = n_row
    model.passModel(lp)


def _run(model: highspy.Highs, what: str) -> FloatArray:
    model.run()
    status = model.getModelStatus()
    if status != highspy.HighsModelStatus.kOptimal:
        raise LpError(f"{what}: HiGHS status {model.modelStatusToString(status)}")
    return np.asarray(model.getSolution().col_value, dtype=float)


def _soc_block(
    n: int, h: float, eta_c: float, eta_d: float, d_cols: IntArray, c_cols: IntArray, s_cols: IntArray
) -> tuple[list[int], list[int], list[float]]:
    """Triplets of ``s_j - s_{j-1} + h/eta_d d_j - h eta_c c_j = rhs_j`` (rows 0..n-1, relative)."""

    rows: list[int] = []
    cols: list[int] = []
    vals: list[float] = []
    for j in range(n):
        rows += [j, j, j]
        cols += [int(s_cols[j]), int(d_cols[j]), int(c_cols[j])]
        vals += [1.0, h / eta_d, -h * eta_c]
        if j > 0:
            rows.append(j)
            cols.append(int(s_cols[j - 1]))
            vals.append(-1.0)
    return rows, cols, vals


@dataclass(frozen=True)
class DaPlan:
    """Day-ahead schedule: position (= physical schedule when no intraday trading follows)."""

    position_mw: FloatArray
    discharge_mw: FloatArray
    charge_mw: FloatArray
    soc_end_mwh: FloatArray
    objective_eur: float


def solve_da_schedule(
    prices: FloatArray,
    blocks: IntArray,
    battery: BatteryParams,
    *,
    initial_soc_mwh: float,
    terminal_soc_mwh: float,
    fee_eur_mwh: float = 0.0,
    tiebreak_eur_mwh: float = 1e-3,
) -> DaPlan:
    """Price-taking day-ahead schedule maximising ``prices`` revenue minus fees and degradation.

    ``blocks[j]`` is the day-ahead product of quarter-hour j (contiguous ids); positions are constant per block.
    """

    prices = np.nan_to_num(np.asarray(prices, dtype=float))
    n = len(prices)
    n_blocks = int(blocks.max()) + 1 if n else 0
    h = QH_HOURS
    p_max = battery.max_power_mw
    d_cols = np.arange(n_blocks, dtype=np.int64)
    c_cols = n_blocks + np.arange(n_blocks, dtype=np.int64)
    s_cols = 2 * n_blocks + np.arange(n, dtype=np.int64)
    n_col = 2 * n_blocks + n
    rows, cols, vals = _soc_block(
        n, h, battery.charge_efficiency, battery.discharge_efficiency, d_cols[blocks], c_cols[blocks], s_cols
    )
    for b in range(n_blocks):
        rows += [n + b, n + b]
        cols += [int(d_cols[b]), int(c_cols[b])]
        vals += [1.0, 1.0]
    matrix = sparse.csc_matrix((vals, (rows, cols)), shape=(n + n_blocks, n_col))
    cost = np.zeros(n_col)
    np.add.at(cost, d_cols[blocks], (-prices + fee_eur_mwh + battery.degradation_eur_per_mwh) * h)
    np.add.at(cost, c_cols[blocks], (prices + fee_eur_mwh + tiebreak_eur_mwh) * h)
    col_lower = np.concatenate([np.zeros(2 * n_blocks), np.full(n, battery.soc_min_mwh)])
    col_upper = np.concatenate([np.full(2 * n_blocks, p_max), np.full(n, battery.soc_max_mwh)])
    col_lower[s_cols[-1]] = col_upper[s_cols[-1]] = terminal_soc_mwh
    row_lower = np.concatenate([np.zeros(n), np.full(n_blocks, -np.inf)])
    row_upper = np.concatenate([np.zeros(n), np.full(n_blocks, p_max)])
    row_lower[0] = row_upper[0] = initial_soc_mwh
    model = _new_highs()
    _pass_lp(model, cost, col_lower, col_upper, matrix, row_lower, row_upper)
    sol = _run(model, "day-ahead schedule")
    d_blk, c_blk = sol[d_cols], sol[c_cols]
    discharge, charge = d_blk[blocks], c_blk[blocks]
    return DaPlan(
        position_mw=discharge - charge,
        discharge_mw=discharge,
        charge_mw=charge,
        soc_end_mwh=sol[s_cols],
        objective_eur=-float(model.getInfo().objective_function_value),
    )


@dataclass(frozen=True)
class IntradayStep:
    discharge_mw: float
    charge_mw: float
    sell_mw: float
    buy_mw: float


class IntradayDayLP:
    """Intraday re-optimisation of one delivery day around a fixed day-ahead position ``x``.

    ``set_prices(k, forecast)`` updates the expected execution prices of products ``k..n-1``; ``fix(j, step)``
    freezes a committed product; ``solve()`` re-optimises (warm start). The terminal SOC is an equality so the
    next day's day-ahead schedule (which assumed it) stays feasible.
    """

    def __init__(
        self,
        position_mw: FloatArray,
        battery: BatteryParams,
        costs: TradingCosts,
        *,
        initial_soc_mwh: float,
        terminal_soc_mwh: float,
        tiebreak_eur_mwh: float = 1e-3,
    ) -> None:
        x = np.asarray(position_mw, dtype=float)
        n = len(x)
        self.n = n
        self.h = QH_HOURS
        self.cost_id = costs.id_cost_eur_mwh
        p_max = battery.max_power_mw
        self.d = np.arange(n, dtype=np.int64)
        self.c = n + self.d
        self.s = 2 * n + self.d
        self.zs = 3 * n + self.d
        self.zb = 4 * n + self.d
        rows: list[int] = []
        cols: list[int] = []
        vals: list[float] = []
        for j in range(n):  # balance rows 0..n-1
            rows += [j, j, j, j]
            cols += [int(self.d[j]), int(self.c[j]), int(self.zs[j]), int(self.zb[j])]
            vals += [1.0, -1.0, -1.0, 1.0]
        soc_rows, soc_cols, soc_vals = _soc_block(
            n, self.h, battery.charge_efficiency, battery.discharge_efficiency, self.d, self.c, self.s
        )
        rows += [n + r for r in soc_rows]
        cols += soc_cols
        vals += soc_vals
        for j in range(n):  # power rows 2n..3n-1
            rows += [2 * n + j, 2 * n + j]
            cols += [int(self.d[j]), int(self.c[j])]
            vals += [1.0, 1.0]
        matrix = sparse.csc_matrix((vals, (rows, cols)), shape=(3 * n, 5 * n))
        cost = np.zeros(5 * n)
        cost[self.d] = battery.degradation_eur_per_mwh * self.h
        cost[self.c] = tiebreak_eur_mwh * self.h
        col_lower = np.concatenate([np.zeros(2 * n), np.full(n, battery.soc_min_mwh), np.zeros(2 * n)])
        col_upper = np.concatenate(
            [np.full(2 * n, p_max), np.full(n, battery.soc_max_mwh), np.full(2 * n, 2.0 * p_max)]
        )
        col_lower[self.s[-1]] = col_upper[self.s[-1]] = terminal_soc_mwh
        row_lower = np.concatenate([x, np.zeros(n), np.full(n, -np.inf)])
        row_upper = np.concatenate([x, np.zeros(n), np.full(n, p_max)])
        row_lower[n] = row_upper[n] = initial_soc_mwh
        self.model = _new_highs()
        _pass_lp(self.model, cost, col_lower, col_upper, matrix, row_lower, row_upper)
        self._solution: FloatArray | None = None

    def set_prices(self, k: int, forecast: FloatArray) -> None:
        """Expected execution prices of products ``k..n-1`` (``len(forecast) == n - k``)."""

        f = np.nan_to_num(np.asarray(forecast, dtype=float))
        if len(f) != self.n - k:
            raise ValueError("forecast must cover products k..n-1")
        idx = np.concatenate([self.zs[k:], self.zb[k:]]).astype(np.int32)
        values = np.concatenate([-(f - self.cost_id) * self.h, (f + self.cost_id) * self.h])
        self.model.changeColsCost(len(idx), idx, values)

    @property
    def has_solution(self) -> bool:
        return self._solution is not None

    def solve(self) -> FloatArray:
        """Re-optimise (warm start); on a non-optimal status retry once from a cold start.

        If both attempts fail, :class:`LpError` is raised and the previous solution is kept, so ``step(j)`` still
        returns the last feasible plan (which already satisfied every remaining constraint).
        """

        try:
            solution = _run(self.model, "intraday re-optimisation")
        except LpError:
            self.model.clearSolver()
            solution = _run(self.model, "intraday re-optimisation (cold start)")
        self._solution = solution
        return solution

    def step(self, j: int) -> IntradayStep:
        if self._solution is None:
            raise RuntimeError("solve() first")
        sol = self._solution
        d, c = max(0.0, float(sol[self.d[j]])), max(0.0, float(sol[self.c[j]]))
        net = float(sol[self.zs[j]] - sol[self.zb[j]])
        return IntradayStep(discharge_mw=d, charge_mw=c, sell_mw=max(net, 0.0), buy_mw=max(-net, 0.0))

    def fix(self, j: int, step: IntradayStep) -> None:
        idx = np.array([self.d[j], self.c[j], self.zs[j], self.zb[j]], dtype=np.int32)
        values = np.array([step.discharge_mw, step.charge_mw, step.sell_mw, step.buy_mw], dtype=float)
        self.model.changeColsBounds(4, idx, values, values)

    def soc_end(self) -> FloatArray:
        if self._solution is None:
            raise RuntimeError("solve() first")
        return self._solution[self.s].copy()


@dataclass(frozen=True)
class JointPlan:
    position_mw: FloatArray
    discharge_mw: FloatArray
    charge_mw: FloatArray
    sell_mw: FloatArray
    buy_mw: FloatArray
    soc_end_mwh: FloatArray


def solve_oracle_da_id(
    da_prices: FloatArray,
    id_prices: FloatArray,
    blocks: IntArray,
    battery: BatteryParams,
    costs: TradingCosts,
    *,
    initial_soc_mwh: float,
    terminal_soc_mwh: float,
    tiebreak_eur_mwh: float = 1e-3,
) -> JointPlan:
    """Perfect-foresight day-ahead position + intraday trades (upper bound; not tradable).

    The day-ahead position must itself be a feasible battery schedule (own SOC path, terminal SOC), so the oracle
    cannot sell day-ahead volumes the battery could never deliver.
    """

    da = np.nan_to_num(np.asarray(da_prices, dtype=float))
    idp = np.nan_to_num(np.asarray(id_prices, dtype=float))
    n = len(da)
    nb = int(blocks.max()) + 1
    h = QH_HOURS
    p_max = battery.max_power_mw
    dx = np.arange(nb, dtype=np.int64)
    cx = nb + dx
    sx = 2 * nb + np.arange(n, dtype=np.int64)
    base = 2 * nb + n
    d = base + np.arange(n, dtype=np.int64)
    c = base + n + np.arange(n, dtype=np.int64)
    s = base + 2 * n + np.arange(n, dtype=np.int64)
    zs = base + 3 * n + np.arange(n, dtype=np.int64)
    zb = base + 4 * n + np.arange(n, dtype=np.int64)
    n_col = base + 5 * n
    eta_c, eta_d = battery.charge_efficiency, battery.discharge_efficiency
    rows: list[int] = []
    cols: list[int] = []
    vals: list[float] = []
    r0 = 0  # day-ahead position SOC rows
    sr, sc, sv = _soc_block(n, h, eta_c, eta_d, dx[blocks], cx[blocks], sx)
    rows += [r0 + r for r in sr]
    cols += sc
    vals += sv
    r1 = n  # day-ahead position power rows
    for b in range(nb):
        rows += [r1 + b, r1 + b]
        cols += [int(dx[b]), int(cx[b])]
        vals += [1.0, 1.0]
    r2 = n + nb  # balance rows: d - c - zs + zb - dx + cx = 0
    for j in range(n):
        rows += [r2 + j] * 6
        cols += [int(d[j]), int(c[j]), int(zs[j]), int(zb[j]), int(dx[blocks[j]]), int(cx[blocks[j]])]
        vals += [1.0, -1.0, -1.0, 1.0, -1.0, 1.0]
    r3 = 2 * n + nb  # physical SOC rows
    sr, sc, sv = _soc_block(n, h, eta_c, eta_d, d, c, s)
    rows += [r3 + r for r in sr]
    cols += sc
    vals += sv
    r4 = 3 * n + nb  # physical power rows
    for j in range(n):
        rows += [r4 + j, r4 + j]
        cols += [int(d[j]), int(c[j])]
        vals += [1.0, 1.0]
    n_row = 4 * n + nb
    matrix = sparse.csc_matrix((vals, (rows, cols)), shape=(n_row, n_col))
    cost = np.zeros(n_col)
    np.add.at(cost, dx[blocks], (-da + costs.da_fee_eur_mwh) * h)
    np.add.at(cost, cx[blocks], (da + costs.da_fee_eur_mwh + tiebreak_eur_mwh) * h)
    cost[d] = battery.degradation_eur_per_mwh * h
    cost[c] = tiebreak_eur_mwh * h
    cost[zs] = -(idp - costs.id_cost_eur_mwh) * h
    cost[zb] = (idp + costs.id_cost_eur_mwh) * h
    col_lower = np.zeros(n_col)
    col_upper = np.full(n_col, p_max)
    col_lower[sx] = battery.soc_min_mwh
    col_upper[sx] = battery.soc_max_mwh
    col_lower[s] = battery.soc_min_mwh
    col_upper[s] = battery.soc_max_mwh
    col_upper[zs] = col_upper[zb] = 2.0 * p_max
    col_lower[sx[-1]] = col_upper[sx[-1]] = terminal_soc_mwh
    col_lower[s[-1]] = col_upper[s[-1]] = terminal_soc_mwh
    row_lower = np.zeros(n_row)
    row_upper = np.zeros(n_row)
    row_lower[r1 : r1 + nb] = -np.inf
    row_upper[r1 : r1 + nb] = p_max
    row_lower[r4 : r4 + n] = -np.inf
    row_upper[r4 : r4 + n] = p_max
    row_lower[r0] = row_upper[r0] = initial_soc_mwh
    row_lower[r3] = row_upper[r3] = initial_soc_mwh
    model = _new_highs()
    _pass_lp(model, cost, col_lower, col_upper, matrix, row_lower, row_upper)
    sol = _run(model, "joint day-ahead + intraday oracle")
    net = sol[zs] - sol[zb]
    return JointPlan(
        position_mw=(sol[dx] - sol[cx])[blocks],
        discharge_mw=np.maximum(sol[d], 0.0),
        charge_mw=np.maximum(sol[c], 0.0),
        sell_mw=np.maximum(net, 0.0),
        buy_mw=np.maximum(-net, 0.0),
        soc_end_mwh=sol[s],
    )
