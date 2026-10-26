"""One-delivery-day LP: day-ahead arbitrage co-optimised with Nordic reserve capacity (HiGHS via SciPy).

Decision variables per quarter-hour ``t``: charge ``c_t`` and discharge ``d_t`` (MW, day-ahead schedule) and SOC
``s_t`` (MWh, end of the quarter-hour); per reserve product ``k`` and product block ``b``: capacity ``r_kb`` (MW).

    max  sum_t 0.25 * price_t * (d_t - c_t) - 0.25 * (deg * d_t + eps * c_t) + sum_k sum_b r_kb * sum_{h in b} pi_kh
    s.t. s_t = s_{t-1} + 0.25 * (eta_c * c_t - d_t / eta_d),  s_{-1} = s_start,  s_{n-1} = s_end
         d_t - c_t + sum_k up_k * r_k  <= P        (discharge headroom incl. the reserve's own NEM reservation)
         c_t - d_t + sum_k down_k * r_k <= P       (charge headroom)
         c_t + d_t <= P                            (converter limit; stops loss-burning at negative prices)
         s - sum_k E_up_k * r_k / eta_d >= s_min   and   s + sum_k E_down_k * r_k * eta_c <= s_max
                                                   (at the end of every quarter-hour and the start of every hour)
         d_t - c_t constant within an hour when day-ahead products are hourly (before 2025-10-01)

Reserve revenue is the capacity price per MW and hour of availability; activation energy is not simulated (its
expected value is taken as zero and the energy reservation covers the SOC excursion).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

import numpy as np
from numpy.typing import NDArray
from scipy import sparse
from scipy.optimize import linprog

from ...realtime.types import QH_HOURS, BatteryParams
from .rules import ReserveProduct

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]


@dataclass
class DayInputs:
    """Prices of one delivery day on its quarter-hour and hour grids (local day, DST days have 92/100 QHs)."""

    da_price: FloatArray  # (n,) EUR/MWh per quarter-hour
    qh_hour: IntArray  # (n,) hour index (0..m-1) of every quarter-hour
    reserve_price: Mapping[str, FloatArray] = field(default_factory=dict)  # key -> (m,) EUR/MW/h (NaN = no market)
    hour_block: Mapping[int, IntArray] = field(default_factory=dict)  # block_hours -> (m,) block index per hour
    hourly_da: bool = False
    da_tradable: NDArray[np.bool_] | None = None  # (n,) False = no day-ahead position (price unknown in the data)

    @property
    def n_qh(self) -> int:
        return int(len(self.da_price))

    @property
    def n_hours(self) -> int:
        return int(self.qh_hour.max()) + 1 if len(self.qh_hour) else 0


@dataclass
class DaySchedule:
    charge_mw: FloatArray
    discharge_mw: FloatArray
    soc_mwh: FloatArray
    reserve_mw: dict[str, FloatArray]  # key -> (m,) MW per hour (block values repeated)
    objective_eur: float

    @property
    def net_mw(self) -> FloatArray:
        return self.discharge_mw - self.charge_mw


def _blocks(inputs: DayInputs, product: ReserveProduct) -> IntArray:
    m = inputs.n_hours
    if product.block_hours <= 1:
        return np.arange(m, dtype=np.int64)
    blocks = inputs.hour_block.get(product.block_hours)
    if blocks is None:
        blocks = np.arange(m, dtype=np.int64) // product.block_hours
    return np.asarray(blocks, dtype=np.int64)


def solve_day(
    inputs: DayInputs,
    products: Sequence[ReserveProduct],
    battery: BatteryParams,
    *,
    start_soc_mwh: float | None = None,
    end_soc_mwh: float | None = None,
    free_end_soc: bool = False,
    converter_limit: bool = True,
    charge_cost_eur_per_mwh: float = 1e-3,
) -> DaySchedule:
    """Solve the day LP for ``inputs`` (forecast or realised prices). Missing reserve prices fix capacity to 0.

    ``free_end_soc`` drops the terminal SOC equality and ``converter_limit=False`` drops ``c + d <= P``; together
    they give the formulation of ``realtime.strategies.solve_qh_arbitrage`` (used as a consistency check).
    """

    n, m = inputs.n_qh, inputs.n_hours
    dt = QH_HOURS
    p_max = battery.max_power_mw
    eta_c, eta_d = battery.charge_efficiency, battery.discharge_efficiency
    s_start = battery.initial_soc_mwh if start_soc_mwh is None else float(start_soc_mwh)
    s_end = s_start if end_soc_mwh is None else float(end_soc_mwh)
    s_min, s_max = battery.soc_min_mwh, battery.soc_max_mwh
    price = np.nan_to_num(np.asarray(inputs.da_price, dtype=np.float64))
    qh_hour = np.asarray(inputs.qh_hour, dtype=np.int64)

    block_of_hour = [_blocks(inputs, p) for p in products]
    n_blocks = [int(b.max()) + 1 if len(b) else 0 for b in block_of_hour]
    offsets = np.cumsum([3 * n, *n_blocks]).astype(np.int64)
    n_var = int(offsets[-1])
    ic, id_, is_ = np.arange(n), np.arange(n, 2 * n), np.arange(2 * n, 3 * n)

    cost = np.zeros(n_var)
    cost[ic] = price * dt + charge_cost_eur_per_mwh * dt
    cost[id_] = -price * dt + battery.degradation_eur_per_mwh * dt
    upper = np.full(n_var, np.inf)
    tradable = np.ones(n, dtype=bool) if inputs.da_tradable is None else np.asarray(inputs.da_tradable, dtype=bool)
    upper[ic] = np.where(tradable, p_max, 0.0)
    upper[id_] = np.where(tradable, p_max, 0.0)
    lower = np.zeros(n_var)
    lower[is_] = s_min
    upper[is_] = s_max
    # QH t -> column of every product's capacity variable in force during t.
    reserve_cols: list[IntArray] = []
    for k, product in enumerate(products):
        blocks = block_of_hour[k]
        cols = offsets[k] + np.arange(n_blocks[k])
        hourly_price = np.asarray(inputs.reserve_price.get(product.key, np.full(m, np.nan)), dtype=np.float64)
        available = np.isfinite(hourly_price)
        cost[cols] = -np.bincount(blocks, weights=np.where(available, hourly_price, 0.0), minlength=n_blocks[k])
        block_ok = np.bincount(blocks, weights=available.astype(float), minlength=n_blocks[k]) == np.bincount(
            blocks, minlength=n_blocks[k]
        )
        cap = p_max / max(product.power_up, product.power_down, 1e-9)
        upper[cols] = np.where(block_ok, cap, 0.0)
        reserve_cols.append(cols[blocks[qh_hour]])

    rows: list[IntArray] = []
    cols_: list[IntArray] = []
    vals: list[FloatArray] = []
    b_ub: list[FloatArray] = []
    row = 0

    def add(r: IntArray, c: IntArray, v: FloatArray) -> None:
        rows.append(np.asarray(r, dtype=np.int64))
        cols_.append(np.asarray(c, dtype=np.int64))
        vals.append(np.asarray(v, dtype=np.float64))

    t = np.arange(n)
    # Power headroom (discharge and charge direction) and converter limit.
    for sign_d, attr in ((1.0, "power_up"), (-1.0, "power_down")):
        add(row + t, id_, np.full(n, sign_d))
        add(row + t, ic, np.full(n, -sign_d))
        for k, product in enumerate(products):
            coef = getattr(product, attr)
            if coef:
                add(row + t, reserve_cols[k], np.full(n, coef))
        b_ub.append(np.full(n, p_max))
        row += n
    if converter_limit:
        add(row + t, ic, np.ones(n))
        add(row + t, id_, np.ones(n))
        b_ub.append(np.full(n, p_max))
        row += n

    # Energy headroom at the end of every QH and at the start of every hour (reserves change only there).
    first_of_hour = np.flatnonzero(np.r_[True, qh_hour[1:] != qh_hour[:-1]])
    e_up = [p.energy_up_hours / eta_d for p in products]
    e_down = [p.energy_down_hours * eta_c for p in products]
    for sign, coefs, bound in ((-1.0, e_up, -s_min), (1.0, e_down, s_max)):
        # end of QH: sign * s_t + sum coef * r <= bound
        add(row + t, is_, np.full(n, sign))
        for k in range(len(products)):
            if coefs[k]:
                add(row + t, reserve_cols[k], np.full(n, coefs[k]))
        b_ub.append(np.full(n, bound))
        row += n
        # start of hour: sign * s_{t-1} + sum coef * r_t <= bound (s_{-1} = s_start is a constant)
        starts = first_of_hour
        nr = len(starts)
        inner = starts[starts > 0]
        add(row + np.flatnonzero(starts > 0), is_[inner - 1], np.full(len(inner), sign))
        for k in range(len(products)):
            if coefs[k]:
                add(row + np.arange(nr), reserve_cols[k][starts], np.full(nr, coefs[k]))
        rhs = np.full(nr, bound)
        rhs[starts == 0] -= sign * s_start
        b_ub.append(rhs)
        row += nr

    a_ub = sparse.csr_matrix((np.concatenate(vals), (np.concatenate(rows), np.concatenate(cols_))), shape=(row, n_var))

    # Equalities: SOC dynamics, terminal SOC, hourly day-ahead products.
    er: list[IntArray] = []
    ec: list[IntArray] = []
    ev: list[FloatArray] = []
    b_eq: list[float] = []
    er += [t, t, t]
    ec += [is_, ic, id_]
    ev += [np.ones(n), np.full(n, -dt * eta_c), np.full(n, dt / eta_d)]
    if n > 1:
        er.append(t[1:])
        ec.append(is_[:-1])
        ev.append(-np.ones(n - 1))
    b_eq = [s_start] + [0.0] * (n - 1)
    eq_row = n
    if not free_end_soc:
        er.append(np.array([eq_row]))
        ec.append(np.array([is_[-1]]))
        ev.append(np.ones(1))
        b_eq.append(s_end)
        eq_row += 1
    if inputs.hourly_da and n > 1:
        same = np.flatnonzero(qh_hour[1:] == qh_hour[:-1])
        k_rows = eq_row + np.arange(len(same))
        for col_offset, sign in ((id_, 1.0), (ic, -1.0)):
            er += [k_rows, k_rows]
            ec += [col_offset[same], col_offset[same + 1]]
            ev += [np.full(len(same), sign), np.full(len(same), -sign)]
        b_eq += [0.0] * len(same)
        eq_row += len(same)
    a_eq = sparse.csr_matrix((np.concatenate(ev), (np.concatenate(er), np.concatenate(ec))), shape=(eq_row, n_var))

    result = linprog(
        cost,
        A_ub=a_ub,
        b_ub=np.concatenate(b_ub),
        A_eq=a_eq,
        b_eq=np.asarray(b_eq),
        bounds=np.column_stack([lower, upper]),
        method="highs",
    )
    if not result.success:
        raise RuntimeError(f"Nordic day LP failed: {result.message}")
    x = result.x
    reserves = {}
    for k, product in enumerate(products):
        block_values = x[offsets[k] : offsets[k] + n_blocks[k]]
        reserves[product.key] = np.maximum(block_values[block_of_hour[k]], 0.0)
    return DaySchedule(
        charge_mw=np.maximum(x[ic], 0.0),
        discharge_mw=np.maximum(x[id_], 0.0),
        soc_mwh=x[is_],
        reserve_mw=reserves,
        objective_eur=float(-result.fun),
    )


@dataclass(frozen=True)
class DaySettlement:
    da_revenue_eur: float
    reserve_revenue_eur: dict[str, float]
    degradation_eur: float
    discharged_mwh: float
    charged_mwh: float

    @property
    def reserve_total_eur(self) -> float:
        return float(sum(self.reserve_revenue_eur.values()))

    @property
    def net_eur(self) -> float:
        return self.da_revenue_eur + self.reserve_total_eur - self.degradation_eur


def settle_day(schedule: DaySchedule, realised: DayInputs, battery: BatteryParams) -> DaySettlement:
    """Settle a schedule at realised prices (day-ahead per quarter-hour, capacity per MW and hour).

    A capacity price that is missing in the realised data (no procurement in that zone and hour) pays nothing.
    """

    dt = QH_HOURS
    price = np.nan_to_num(np.asarray(realised.da_price, dtype=np.float64))
    da = float(np.sum(price * (schedule.discharge_mw - schedule.charge_mw)) * dt)
    reserve = {}
    for key, mw in schedule.reserve_mw.items():
        pi = np.asarray(realised.reserve_price.get(key, np.full(len(mw), np.nan)), dtype=np.float64)
        reserve[key] = float(np.sum(np.nan_to_num(pi) * mw))
    discharged = float(np.sum(schedule.discharge_mw) * dt)
    charged = float(np.sum(schedule.charge_mw) * dt)
    return DaySettlement(
        da_revenue_eur=da,
        reserve_revenue_eur=reserve,
        degradation_eur=battery.degradation_eur_per_mwh * discharged,
        discharged_mwh=discharged,
        charged_mwh=charged,
    )


def check_schedule(
    schedule: DaySchedule,
    inputs: DayInputs,
    products: Sequence[ReserveProduct],
    battery: BatteryParams,
    *,
    tol: float = 1e-6,
) -> list[str]:
    """Independent feasibility check of a schedule against the product rules (used by tests and the script)."""

    problems: list[str] = []
    dt = QH_HOURS
    p_max = battery.max_power_mw
    net = schedule.discharge_mw - schedule.charge_mw
    hours = np.asarray(inputs.qh_hour, dtype=np.int64)
    up = net + sum((p.power_up * schedule.reserve_mw[p.key][hours] for p in products), np.zeros(len(net)))
    down = -net + sum((p.power_down * schedule.reserve_mw[p.key][hours] for p in products), np.zeros(len(net)))
    if (up > p_max + tol).any():
        problems.append(f"discharge headroom exceeded by {float((up - p_max).max()):.6f} MW")
    if (down > p_max + tol).any():
        problems.append(f"charge headroom exceeded by {float((down - p_max).max()):.6f} MW")
    soc_start = np.r_[battery.initial_soc_mwh, schedule.soc_mwh[:-1]]
    expected = soc_start + dt * (
        battery.charge_efficiency * schedule.charge_mw - schedule.discharge_mw / battery.discharge_efficiency
    )
    if np.abs(expected - schedule.soc_mwh).max() > 1e-5:
        problems.append("SOC dynamics violated")
    e_up = sum(
        (p.energy_up_hours / battery.discharge_efficiency * schedule.reserve_mw[p.key][hours] for p in products),
        np.zeros(len(net)),
    )
    e_down = sum(
        (p.energy_down_hours * battery.charge_efficiency * schedule.reserve_mw[p.key][hours] for p in products),
        np.zeros(len(net)),
    )
    for label, soc in (("end", schedule.soc_mwh), ("start", soc_start)):
        if (soc - e_up < battery.soc_min_mwh - 1e-5).any():
            problems.append(f"upward energy reservation violated at QH {label}")
        if (soc + e_down > battery.soc_max_mwh + 1e-5).any():
            problems.append(f"downward energy reservation violated at QH {label}")
    return problems
