"""Causal minute-level features for forecasting the final quarter-hour outcome.

Row ``t`` of a :class:`FeatureMatrix` describes what is known when the decision for minute ``t`` is taken under a
:class:`~euroflex_bess_lab.realtime.PublicationModel`:

- minute rows ``j <= t - minute_lag - 1`` (the latest one is ``r = t - minute_lag - 1``);
- final quarter-hour values of quarter-hours ``k`` with ``15 k + 15 + qh_final_lag_minutes <= t``;
- day-ahead prices whose publication minute is ``<= t``.

Minute semantics (checked on Elia data, see ``reports/realtime_belgium.md``): ODS133/ODS165 fields (``si_cum_mw``,
``price_cum_eur_mwh``, ``mip_eur_mwh``, ``alpha_eur_mwh``, ``afrr_price_up_eur_mwh``, ...) and the new-formula
``nf_*`` minute fields are cumulative quarter-hour-to-date values; ODS126 fields (``si_mw``, ``afrr_up_mw``, ...) are
instantaneous. Cumulative fields are only used inside the quarter-hour they belong to.

Feature names are fixed by the :class:`FeatureConfig`; a field missing from a market yields an all-NaN column, so a
model trained on one market can always be applied to another (gradient boosting handles NaN natively).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ..mechanisms import CurrentAlphaMechanism, NewWeightedMechanism2026, reconstruct_new_formula_history
from ..replay import MarketData
from ..types import MINUTES_PER_QH, PublicationModel

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]

#: Feature groups that describe the mechanism-invariant system state (usable before and after a formula change).
INVARIANT_GROUPS: tuple[str, ...] = (
    "calendar",
    "si",
    "volumes",
    "product_prices",
    "elia_forecast",
    "day_ahead",
    "history",
)
#: Default groups for a price forecaster under the current formula.
CURRENT_GROUPS: tuple[str, ...] = (*INVARIANT_GROUPS, "current_mech")
#: Default groups for a price forecaster trained on new-formula (ODS211/212) data.
NEW_FORMULA_GROUPS: tuple[str, ...] = (*INVARIANT_GROUPS, "new_mech")
ALL_GROUPS: tuple[str, ...] = (*INVARIANT_GROUPS, "current_mech", "new_mech")

_ELIA_BINS_SHORT = ("p_si_lt_m400", "p_si_m400_m200", "p_si_m200_0")


@dataclass(frozen=True)
class FeatureConfig:
    """Which feature groups to build and which published price series feeds the ``history`` group.

    ``history``: ``"settlement:<mechanism>"`` (published final prices, e.g. ``settlement:current``),
    ``"reconstructed:new2026"`` (new-design prices recomputed from current-design quarter-hour state with
    :func:`~euroflex_bess_lab.realtime.mechanisms.reconstruct_new_formula_history`; no new-formula price history), or
    ``"none"``.
    """

    groups: tuple[str, ...] = CURRENT_GROUPS
    history: str = "settlement:current"
    ffill_minutes: int = 2

    def __post_init__(self) -> None:
        unknown = set(self.groups) - set(ALL_GROUPS)
        if unknown:
            raise ValueError(f"unknown feature groups {sorted(unknown)}; known: {list(ALL_GROUPS)}")
        if self.history != "none" and not self.history.startswith(("settlement:", "reconstructed:")):
            raise ValueError("history must be 'none', 'settlement:<mechanism>' or 'reconstructed:new2026'")


@dataclass
class FeatureMatrix:
    """Feature values (float32, one row per decision minute) and their names."""

    names: list[str]
    values: NDArray[np.float32]
    meta: dict[str, object] = field(default_factory=dict)

    def column(self, name: str) -> NDArray[np.float32]:
        return self.values[:, self.names.index(name)]

    def select(self, names: Sequence[str]) -> NDArray[np.float32]:
        index = [self.names.index(n) for n in names]
        return self.values[:, index]


# --------------------------------------------------------------------------------------------------------------
# vectorised helpers


def _ffill(values: FloatArray, limit: int) -> FloatArray:
    if limit <= 0 or not np.isnan(values).any():
        return values
    return pd.Series(values).ffill(limit=limit).to_numpy(dtype=float)


def _ffill_within_qh(values: FloatArray, limit: int) -> FloatArray:
    """Forward-fill gaps without crossing a quarter-hour boundary (for quarter-hour-to-date fields)."""
    if limit <= 0 or not np.isnan(values).any():
        return values
    by_qh = pd.DataFrame(values.reshape(-1, MINUTES_PER_QH))
    return by_qh.ffill(axis=1, limit=limit).to_numpy(dtype=float).reshape(-1)


def _qh_running_mean(values: FloatArray) -> FloatArray:
    """Mean of the non-NaN values from the start of each quarter-hour up to and including each row."""
    by_qh = values.reshape(-1, MINUTES_PER_QH)
    present = ~np.isnan(by_qh)
    sums = np.cumsum(np.where(present, by_qh, 0.0), axis=1)
    counts = np.cumsum(present, axis=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        out = np.where(counts > 0, sums / np.maximum(counts, 1), np.nan)
    return out.reshape(-1)


def rolling_qh_stat(values: FloatArray, window: int, stat: str = "mean") -> FloatArray:
    """Trailing NaN-aware mean or std over ``values[k - window + 1 : k + 1]`` for every ``k``."""
    present = ~np.isnan(values)
    v = np.where(present, values, 0.0)
    cs = np.concatenate([[0.0], np.cumsum(v)])
    cs2 = np.concatenate([[0.0], np.cumsum(v * v)])
    cc = np.concatenate([[0], np.cumsum(present)])
    k = np.arange(len(values))
    lo = np.maximum(k - window + 1, 0)
    total = cs[k + 1] - cs[lo]
    count = cc[k + 1] - cc[lo]
    with np.errstate(invalid="ignore", divide="ignore"):
        mean = np.where(count > 0, total / np.maximum(count, 1), np.nan)
        if stat == "mean":
            return mean
        if stat == "std":
            second = (cs2[k + 1] - cs2[lo]) / np.maximum(count, 1)
            return np.where(count > 1, np.sqrt(np.maximum(second - mean * mean, 0.0)), np.nan)
    raise ValueError(f"unknown stat {stat!r}")


def _take(values: FloatArray, index: IntArray, valid: NDArray[np.bool_]) -> FloatArray:
    out = np.full(len(index), np.nan)
    if len(values):
        safe = np.clip(index, 0, len(values) - 1)
        out[valid] = values[safe[valid]]
    return out


def history_price_series(market: MarketData, history: str) -> FloatArray:
    """Quarter-hour price series used by the ``history`` group (indexable only below ``visible_qh_end``)."""
    if history == "none":
        return np.full(market.n_qh, np.nan)
    kind, _, name = history.partition(":")
    if kind == "settlement":
        if name in market.settlement_prices:
            return np.asarray(market.settlement_prices[name], dtype=float)
        return np.full(market.n_qh, np.nan)
    if kind == "reconstructed" and name == "new2026":
        if "si_mw" not in market.qh:
            return np.full(market.n_qh, np.nan)
        frame = pd.DataFrame(market.qh)
        rebuilt = reconstruct_new_formula_history(frame)
        return rebuilt["imbalance_price_eur_mwh"].to_numpy(dtype=float)
    raise ValueError(f"unknown history series {history!r}")


@dataclass
class _Index:
    """Index arithmetic shared by all feature groups."""

    n: int
    t: IntArray
    r: IntArray
    valid_r: NDArray[np.bool_]
    qh: IntArray
    qh_start: IntArray
    minute_in_qh: IntArray
    n_visible: IntArray
    in_qh: NDArray[np.bool_]
    prev_index: IntArray
    prev_valid: NDArray[np.bool_]
    k_last: IntArray
    k_last_valid: NDArray[np.bool_]

    @classmethod
    def build(cls, n: int, publication: PublicationModel) -> _Index:
        t = np.arange(n, dtype=np.int64)
        r = t - publication.minute_lag - 1
        qh = t // MINUTES_PER_QH
        qh_start = qh * MINUTES_PER_QH
        n_visible = np.clip(r - qh_start + 1, 0, MINUTES_PER_QH)
        prev_index = np.minimum(r, qh_start - 1)
        prev_valid = (prev_index >= qh_start - MINUTES_PER_QH) & (prev_index >= 0)
        k_last = np.maximum((t - publication.qh_final_lag_minutes) // MINUTES_PER_QH, 0) - 1
        return cls(
            n=n,
            t=t,
            r=r,
            valid_r=r >= 0,
            qh=qh,
            qh_start=qh_start,
            minute_in_qh=t - qh_start,
            n_visible=n_visible,
            in_qh=n_visible > 0,
            prev_index=prev_index,
            prev_valid=prev_valid,
            k_last=k_last,
            k_last_valid=k_last >= 0,
        )


class _Builder:
    def __init__(self, market: MarketData, publication: PublicationModel, config: FeatureConfig) -> None:
        self.market = market
        self.publication = publication
        self.config = config
        self.ix = _Index.build(market.n_minutes, publication)
        self.columns: dict[str, FloatArray] = {}
        self._nan = np.full(market.n_minutes, np.nan)

    # raw minute access ---------------------------------------------------------------------------------------
    def raw(self, name: str) -> FloatArray | None:
        values = self.market.minute.get(name)
        return None if values is None else np.asarray(values, dtype=float)

    def raw_sum(self, names: Sequence[str]) -> FloatArray | None:
        arrays = [a for a in (self.raw(n) for n in names) if a is not None]
        if not arrays:
            return None
        stacked = np.vstack(arrays)
        all_nan = np.isnan(stacked).all(axis=0)
        return np.where(all_nan, np.nan, np.nansum(stacked, axis=0))

    def latest(self, values: FloatArray | None) -> FloatArray:
        """Latest visible value (any quarter-hour), forward-filled over short gaps."""
        if values is None:
            return self._nan
        filled = _ffill(values, self.config.ffill_minutes)
        return _take(filled, self.ix.r, self.ix.valid_r)

    def latest_lagged(self, values: FloatArray | None, extra: int) -> FloatArray:
        if values is None:
            return self._nan
        filled = _ffill(values, self.config.ffill_minutes)
        index = self.ix.r - extra
        return _take(filled, index, index >= 0)

    def current_qh(self, values: FloatArray | None) -> FloatArray:
        """Latest visible value of a quarter-hour-to-date field inside the current quarter-hour, else NaN."""
        if values is None:
            return self._nan
        filled = _ffill_within_qh(values, self.config.ffill_minutes)
        return _take(filled, self.ix.r, self.ix.in_qh)

    def previous_qh(self, values: FloatArray | None) -> FloatArray:
        """Latest visible value belonging to the previous quarter-hour (its final value once fully visible)."""
        if values is None:
            return self._nan
        filled = _ffill_within_qh(values, self.config.ffill_minutes)
        return _take(filled, self.ix.prev_index, self.ix.prev_valid)

    def running_mean_current_qh(self, values: FloatArray | None) -> FloatArray:
        if values is None:
            return self._nan
        return _take(_qh_running_mean(values), self.ix.r, self.ix.in_qh)

    def qh_visible(self, values_qh: FloatArray | None, offset: int = 0) -> FloatArray:
        """Final quarter-hour values ``values_qh[k_last - offset]`` (published by decision time)."""
        if values_qh is None:
            return self._nan
        index = self.ix.k_last - offset
        return _take(np.asarray(values_qh, dtype=float), index, index >= 0)

    def add(self, name: str, values: FloatArray) -> None:
        self.columns[name] = values

    # groups ---------------------------------------------------------------------------------------------------
    def calendar(self) -> None:
        ix = self.ix
        local = self.market.minute_times.tz_convert(self.market.timezone)
        hour = local.hour.to_numpy(dtype=float) + local.minute.to_numpy(dtype=float) / 60.0
        self.add("minute_in_qh", ix.minute_in_qh.astype(float))
        self.add("n_visible_minutes", ix.n_visible.astype(float))
        self.add("hour_local", hour)
        self.add("hour_sin", np.sin(2 * np.pi * hour / 24.0))
        self.add("hour_cos", np.cos(2 * np.pi * hour / 24.0))
        self.add("day_of_week", local.day_of_week.to_numpy(dtype=float))
        self.add("month", local.month.to_numpy(dtype=float))

    def si(self) -> None:
        ix = self.ix
        si_inst = self.raw("si_mw")
        si_cum_raw = self.raw("si_cum_mw")
        si_cum = self.current_qh(si_cum_raw)
        inst = self.latest(si_inst)
        inst_mean = self.running_mean_current_qh(si_inst)
        self.add("si_cum", si_cum)
        self.add("si_inst", inst)
        self.add("si_inst_d1", inst - self.latest_lagged(si_inst, 1))
        self.add("si_inst_d3", inst - self.latest_lagged(si_inst, 3))
        self.add("si_inst_d5", inst - self.latest_lagged(si_inst, 5))
        self.add("si_inst_qh_mean", inst_mean)
        base = np.where(np.isnan(si_cum), inst_mean, si_cum)
        nv = ix.n_visible.astype(float)
        projected = np.where(nv > 0, (nv * base + (MINUTES_PER_QH - nv) * inst) / MINUTES_PER_QH, inst)
        projected = np.where(np.isnan(projected), base, projected)
        self.add("si_projected", projected)
        self.add("si_prev_qh", self.previous_qh(si_cum_raw))
        self.add("ace_inst", self.latest(self.raw("ace_mw")))
        self.add("ace_cum", self.current_qh(self.raw("ace_cum_mw")))

    def volumes(self) -> None:
        pairs = {
            "afrr_up": ("afrr_up_mw",),
            "afrr_down": ("afrr_down_mw",),
            "mfrr_up": ("mfrr_sa_up_mw", "mfrr_da_up_mw"),
            "mfrr_down": ("mfrr_sa_down_mw", "mfrr_da_down_mw"),
            "igcc_up": ("igcc_up_mw",),
            "igcc_down": ("igcc_down_mw",),
        }
        latest: dict[str, FloatArray] = {}
        for label, names in pairs.items():
            values = self.raw_sum(names)
            latest[label] = self.latest(values)
            self.add(f"{label}_inst", latest[label])
            self.add(f"{label}_qh_mean", self.running_mean_current_qh(values))
        self.add("afrr_net_inst", latest["afrr_up"] - latest["afrr_down"])

    def product_prices(self) -> None:
        for label, name in (("afrr_price_up", "afrr_price_up_eur_mwh"), ("afrr_price_down", "afrr_price_down_eur_mwh")):
            values = self.raw(name)
            self.add(f"{label}_cum", self.current_qh(values))
            self.add(f"{label}_prev_qh", self.previous_qh(values))
        self.add("mfrr_avail_price_up", self.latest(self.raw("mfrr_avail_price_up_eur_mwh")))
        self.add("mfrr_avail_price_down", self.latest(self.raw("mfrr_avail_price_down_eur_mwh")))

    def current_mech(self) -> None:
        price = self.raw("price_cum_eur_mwh")
        mip = self.current_qh(self.raw("mip_eur_mwh"))
        mdp = self.current_qh(self.raw("mdp_eur_mwh"))
        alpha = self.current_qh(self.raw("alpha_eur_mwh"))
        run_price = self.current_qh(price)
        self.add("cur_price_cum", run_price)
        self.add("cur_price_prev_qh", self.previous_qh(price))
        if price is None:
            self.add("cur_price_cum_d1", self._nan)
        else:
            before = _take(_ffill_within_qh(price, self.config.ffill_minutes), self.ix.r - 1, self.ix.n_visible > 1)
            self.add("cur_price_cum_d1", run_price - before)
        self.add("cur_mip_cum", mip)
        self.add("cur_mdp_cum", mdp)
        self.add("cur_alpha_cum", alpha)
        alpha0 = np.nan_to_num(alpha)
        self.add("cur_up_cum", mip + alpha0)
        self.add("cur_down_cum", mdp - alpha0)
        alpha_prev = np.nan_to_num(self.previous_qh(self.raw("alpha_eur_mwh")))
        self.add("cur_up_prev_qh", self.previous_qh(self.raw("mip_eur_mwh")) + alpha_prev)
        self.add("cur_down_prev_qh", self.previous_qh(self.raw("mdp_eur_mwh")) - alpha_prev)
        self.add("cur_floor", self.latest(self.raw("floor_price_eur_mwh")))
        self.add("cur_cap", self.latest(self.raw("cap_price_eur_mwh")))
        projected_si = self.columns.get("si_projected")
        if projected_si is None:
            projected_si = self.current_qh(self.raw("si_cum_mw"))
        with np.errstate(invalid="ignore"):
            projected = CurrentAlphaMechanism.price_given_si(
                np.nan_to_num(projected_si), mip=mip, mdp=mdp, alpha=np.nan_to_num(alpha)
            )
        self.add("cur_price_projected", np.where(np.isnan(projected_si), np.nan, projected))

    def new_mech(self) -> None:
        price = self.raw("nf_imbalance_price_eur_mwh")
        up = self.current_qh(self.raw("nf_main_up_eur_mwh"))
        down = self.current_qh(self.raw("nf_main_down_eur_mwh"))
        self.add("nf_price_cum", self.current_qh(price))
        self.add("nf_price_prev_qh", self.previous_qh(price))
        self.add("nf_main_up_cum", up)
        self.add("nf_main_down_cum", down)
        self.add("nf_main_up_prev_qh", self.previous_qh(self.raw("nf_main_up_eur_mwh")))
        self.add("nf_main_down_prev_qh", self.previous_qh(self.raw("nf_main_down_eur_mwh")))
        self.add("nf_si_cum", self.current_qh(self.raw("nf_si_mw")))
        self.add("nf_weight_afrr_cum", self.current_qh(self.raw("nf_weight_afrr")))
        self.add("nf_incentive_cum", self.current_qh(self.raw("nf_incentive_eur_mwh")))
        projected_si = self.columns.get("si_projected")
        if projected_si is None:
            projected_si = self.current_qh(self.raw("nf_si_mw"))
        projected = NewWeightedMechanism2026.price_given_si(np.nan_to_num(projected_si), main_up=up, main_down=down)
        self.add("nf_price_projected", np.where(np.isnan(projected_si), np.nan, projected))

    def elia_forecast(self) -> None:
        cur_prefix, next_prefix = "elia_fc_cur_", "elia_fc_next_"
        cur_si = self.current_qh(self.raw(f"{cur_prefix}si_forecast_mw"))
        prev_next_si = self.previous_qh(self.raw(f"{next_prefix}si_forecast_mw"))
        self.add("elia_si_fc", np.where(np.isnan(cur_si), prev_next_si, cur_si))
        self.add("elia_si_fc_is_current", (~np.isnan(cur_si)).astype(float))
        short_cur = self.raw_sum([f"{cur_prefix}{b}" for b in _ELIA_BINS_SHORT])
        short_next = self.raw_sum([f"{next_prefix}{b}" for b in _ELIA_BINS_SHORT])
        p_cur = self.current_qh(short_cur)
        p_prev_next = self.previous_qh(short_next)
        self.add("elia_p_short", np.where(np.isnan(p_cur), p_prev_next, p_cur))
        self.add("elia_p_lt_m400", self.current_qh(self.raw(f"{cur_prefix}p_si_lt_m400")))
        self.add("elia_p_gt_400", self.current_qh(self.raw(f"{cur_prefix}p_si_gt_400")))
        self.add("elia_next_si_fc", self.current_qh(self.raw(f"{next_prefix}si_forecast_mw")))
        self.add("elia_next_p_short", self.current_qh(short_next))

    def day_ahead(self) -> None:
        ix = self.ix
        market = self.market
        da = np.asarray(market.da_price, dtype=float)
        visible_from = market.da_visible_from_minute

        def da_at(offset: int) -> FloatArray:
            k = ix.qh + offset
            ok = (k >= 0) & (k < market.n_qh)
            ks = np.clip(k, 0, max(market.n_qh - 1, 0))
            ok &= visible_from[ks] <= ix.t
            return _take(da, ks, ok)

        cur = da_at(0)
        self.add("da_cur", cur)
        self.add("da_next", da_at(1))
        self.add("da_next4", da_at(4))
        past_day = rolling_qh_stat(da, 96, "mean")
        self.add("da_rel_past_day", cur - _take(past_day, ix.qh - 1, ix.qh - 1 >= 0))

    def history(self) -> None:
        ix = self.ix
        prices = history_price_series(self.market, self.config.history)
        si_final = self.market.qh.get("si_mw")
        self.add("hist_price_last", self.qh_visible(prices))
        self.add("hist_price_last2", self.qh_visible(prices, 1))
        self.add("hist_price_mean4", self.qh_visible(rolling_qh_stat(prices, 4)))
        self.add("hist_price_mean96", self.qh_visible(rolling_qh_stat(prices, 96)))
        self.add("hist_price_std16", self.qh_visible(rolling_qh_stat(prices, 16, "std")))
        slot = ix.qh - 96
        self.add("hist_price_slot_yday", _take(prices, slot, slot >= 0))
        self.add("hist_price_next_slot_yday", _take(prices, slot + 1, slot + 1 >= 0))
        self.add("hist_si_last", self.qh_visible(si_final))
        si_mean4 = None if si_final is None else rolling_qh_stat(np.asarray(si_final, dtype=float), 4)
        self.add("hist_si_mean4", self.qh_visible(si_mean4))


_GROUP_ORDER: tuple[str, ...] = (
    "calendar",
    "si",
    "volumes",
    "product_prices",
    "current_mech",
    "new_mech",
    "elia_forecast",
    "day_ahead",
    "history",
)


def build_features(
    market: MarketData,
    publication: PublicationModel | None = None,
    config: FeatureConfig | None = None,
) -> FeatureMatrix:
    """Build the causal feature matrix of ``market`` (one row per decision minute)."""
    publication = publication or PublicationModel()
    config = config or FeatureConfig()
    builder = _Builder(market, publication, config)
    methods: dict[str, Callable[[], None]] = {
        "calendar": builder.calendar,
        "si": builder.si,
        "volumes": builder.volumes,
        "product_prices": builder.product_prices,
        "current_mech": builder.current_mech,
        "new_mech": builder.new_mech,
        "elia_forecast": builder.elia_forecast,
        "day_ahead": builder.day_ahead,
        "history": builder.history,
    }
    for group in _GROUP_ORDER:
        if group in config.groups:
            methods[group]()
    names = list(builder.columns)
    values = np.empty((market.n_minutes, len(names)), dtype=np.float32)
    for j, name in enumerate(names):
        values[:, j] = builder.columns[name]
    return FeatureMatrix(
        names=names,
        values=values,
        meta={"groups": list(config.groups), "history": config.history, "minute_lag": publication.minute_lag},
    )


# --------------------------------------------------------------------------------------------------------------
# targets


def qh_of_minute(market: MarketData) -> IntArray:
    return np.arange(market.n_minutes, dtype=np.int64) // MINUTES_PER_QH


def minute_target(market: MarketData, values_qh: FloatArray, horizon: int = 0) -> FloatArray:
    """Per decision minute ``t``: ``values_qh[qh(t) + horizon]`` (NaN beyond the grid)."""
    k = qh_of_minute(market) + horizon
    return _take(np.asarray(values_qh, dtype=float), k, (k >= 0) & (k < market.n_qh))


def target_known_minute(market: MarketData, horizon: int, publication: PublicationModel) -> IntArray:
    """Minute index from which the final value targeted by row ``t`` (horizon ``h``) is published."""
    k = qh_of_minute(market) + horizon
    return (k + 1) * MINUTES_PER_QH + publication.qh_final_lag_minutes


def baseline_values(values: NDArray[np.float32], names: Sequence[str], chain: Sequence[str]) -> FloatArray:
    """Per row, the first finite value among the feature columns ``chain`` (missing names are skipped), else 0.

    Forecast heads learn the residual ``target - baseline``; a running quarter-hour-to-date estimate is a strong
    baseline that trees cannot reproduce exactly on heavy-tailed prices.
    """
    out = np.full(len(values), np.nan)
    for name in chain:
        if name not in names:
            continue
        column = values[:, names.index(name)].astype(float)
        out = np.where(np.isnan(out), column, out)
    return np.where(np.isfinite(out), out, 0.0)
