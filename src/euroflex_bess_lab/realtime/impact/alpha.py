"""Elia's alpha component (current imbalance-price formula) and its counterfactual.

Exact rule (verified on ODS134, 22 May 2024 - Jul 2026: 100 % of quarter-hours to 0.011 EUR/MWh; structure
communicated by the paper agent and checked here)::

    alpha = 1{|SI_k| > 150 MW} * 200 / (1 + exp((450 - x) / 65)) * c
    x     = (|SI_k| + |SI_{k-1}|) / 2                       (this and the previous quarter-hour)
    c     = clip((400 - MIP) / 200, 0, 1)   if SI_k < 0     (short)
            clip((MDP + 200) / 200, 0, 1)   otherwise       (long)

So alpha is not a function of |SI| alone: it depends on the previous quarter-hour's imbalance (through ``x``) and on
the marginal price of the branch (through the price factor ``c``, which fades alpha out when MIP approaches
400 EUR/MWh or MDP approaches -200 EUR/MWh). In a counterfactual the operators evaluate the rule at the fleet-shifted
imbalance (``SI_k + q_k`` and ``SI_{k-1} + q_{k-1}``) and the counterfactual MIP'/MDP' (mode ``exact``).

Fitted alternatives (least-squares and isotonic logistic curves on several drivers ``x``, without the price factor)
remain available as candidates of :func:`fit_alpha_model`, which ranks them on a hold-out period, and the legacy
``ratio`` mode (scale the published alpha by ``A(x') / A(x)``) is kept for comparison.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any

import numpy as np
from numpy.typing import ArrayLike, NDArray
from scipy.optimize import curve_fit, isotonic_regression

FloatArray = NDArray[np.float64]

ALPHA_THRESHOLD_MW = 150.0
ALPHA_FEATURES: tuple[str, ...] = ("abs_si", "mean_abs_si_prev", "abs_mean_si_prev", "max_abs_si_prev")
ALPHA_MODES: tuple[str, ...] = ("exact", "ratio", "incremental", "model")
RATIO_CAP = 1.3


def alpha_feature(name: str, si: ArrayLike, si_prev: ArrayLike) -> FloatArray:
    """Candidate alpha driver ``x`` (MW). A missing previous value falls back to the current one."""
    s = np.asarray(si, dtype=float)
    prev = np.asarray(si_prev, dtype=float)
    prev = np.where(np.isnan(prev), s, prev)
    if name == "abs_si":
        out = np.abs(s)
    elif name == "mean_abs_si_prev":
        out = 0.5 * (np.abs(s) + np.abs(prev))
    elif name == "abs_mean_si_prev":
        out = np.abs(0.5 * (s + prev))
    elif name == "max_abs_si_prev":
        out = np.maximum(np.abs(s), np.abs(prev))
    else:
        raise KeyError(f"unknown alpha feature `{name}`; known: {ALPHA_FEATURES}")
    result: FloatArray = np.asarray(out, dtype=float)
    return result


def price_factor(si: ArrayLike, mip: ArrayLike, mdp: ArrayLike) -> FloatArray:
    """Elia's alpha price factor ``c`` (short: ``(400 - MIP) / 200``, long: ``(MDP + 200) / 200``, clipped to [0, 1]).

    Missing MIP / MDP give ``c = 1`` (no information -> plain tariff curve).
    """
    s = np.asarray(si, dtype=float)
    up = np.clip((400.0 - np.asarray(mip, dtype=float)) / 200.0, 0.0, 1.0)
    down = np.clip((np.asarray(mdp, dtype=float) + 200.0) / 200.0, 0.0, 1.0)
    out = np.where(s < 0.0, up, down)
    result: FloatArray = np.where(np.isnan(out), 1.0, out)
    return result


def _logistic(x: FloatArray, a: float, b: float, c: float, d: float) -> FloatArray:
    z = np.clip((c - x) / max(d, 1e-6), -60.0, 60.0)
    out: FloatArray = a + b / (1.0 + np.exp(z))
    return out


@dataclass(frozen=True)
class AlphaModel:
    """Alpha as a function of the (counterfactual) imbalance of this and the previous quarter-hour.

    Default = Elia's rule: tariff logistic on ``x = (|SI_k| + |SI_{k-1}|) / 2`` times the price factor ``c(MIP, MDP)``
    (``use_price_factor``; applied when MIP / MDP are passed to :meth:`predict`).
    """

    feature: str = "mean_abs_si_prev"
    kind: str = "logistic"
    a: float = 0.0
    b: float = 200.0
    c: float = 450.0
    d: float = 65.0
    iso_x: tuple[float, ...] = ()
    iso_y: tuple[float, ...] = ()
    threshold_mw: float = ALPHA_THRESHOLD_MW
    use_price_factor: bool = True
    label: str = "tariff"

    @property
    def max_alpha(self) -> float:
        if self.kind == "isotonic":
            return float(max(self.iso_y)) if self.iso_y else 0.0
        return float(self.a + self.b)

    def core(self, x: ArrayLike) -> FloatArray:
        xv = np.asarray(x, dtype=float)
        if self.kind == "logistic":
            out = _logistic(xv, self.a, self.b, self.c, self.d)
        elif self.kind == "isotonic":
            if not self.iso_x:
                return np.zeros_like(xv)
            out = np.interp(xv, np.asarray(self.iso_x), np.asarray(self.iso_y))
        else:
            raise ValueError(f"unknown alpha model kind `{self.kind}`")
        result: FloatArray = np.clip(out, 0.0, None)
        return result

    def predict(
        self,
        si: ArrayLike,
        si_prev: ArrayLike | None = None,
        *,
        mip: ArrayLike | None = None,
        mdp: ArrayLike | None = None,
    ) -> FloatArray:
        s = np.asarray(si, dtype=float)
        prev = s if si_prev is None else np.asarray(si_prev, dtype=float)
        x = alpha_feature(self.feature, s, prev)
        out = np.where(np.abs(s) > self.threshold_mw, self.core(x), 0.0)
        if self.use_price_factor and mip is not None and mdp is not None:
            out = out * price_factor(s, mip, mdp)
        result: FloatArray = np.asarray(out, dtype=float)
        return result

    def counterfactual(
        self,
        alpha_obs: ArrayLike,
        si: ArrayLike,
        si_prev: ArrayLike,
        si_new: ArrayLike,
        si_prev_new: ArrayLike,
        *,
        mode: str = "exact",
        mip: ArrayLike | None = None,
        mdp: ArrayLike | None = None,
        mip_new: ArrayLike | None = None,
        mdp_new: ArrayLike | None = None,
    ) -> FloatArray:
        """Alpha after the imbalance moves from ``(si, si_prev)`` to ``(si_new, si_prev_new)``.

        - ``exact``: the rule at the counterfactual imbalance and the counterfactual MIP' / MDP' (``mip_new`` /
          ``mdp_new``; without them the price factor is dropped);
        - ``ratio``: ``alpha_obs * A(x') / A(x)`` (keeps the published-to-model factor; legacy), capped at
          ``1.3 * max(A)``;
        - ``incremental``: ``alpha_obs + A(x') - A(x)``;
        - ``model``: ``A(x')`` without the price factor.

        Always zero for ``|si_new| <= threshold`` and never negative.
        """
        s, sp = np.asarray(si, dtype=float), np.asarray(si_prev, dtype=float)
        s2, sp2 = np.asarray(si_new, dtype=float), np.asarray(si_prev_new, dtype=float)
        if mode == "exact":
            out = self.predict(s2, sp2, mip=mip_new, mdp=mdp_new)
        else:
            obs = np.asarray(alpha_obs, dtype=float)
            m_obs = self.predict(s, sp)
            m_new = self.predict(s2, sp2)
            above_obs = np.abs(s) > self.threshold_mw
            observed = np.where(np.isnan(obs), m_obs, obs)
            if mode == "model":
                out = m_new
            elif mode == "incremental":
                out = np.where(above_obs, np.maximum(observed + m_new - m_obs, 0.0), m_new)
            elif mode == "ratio":
                usable = above_obs & (m_obs > 1e-6)
                with np.errstate(invalid="ignore", divide="ignore"):
                    ratio = np.where(usable, observed / np.where(usable, m_obs, 1.0), 1.0)
                out = np.clip(ratio * m_new, 0.0, RATIO_CAP * max(self.max_alpha, 1e-9))
            else:
                raise ValueError(f"unknown alpha mode `{mode}`; known: {ALPHA_MODES}")
        result: FloatArray = np.where(np.abs(s2) <= self.threshold_mw, 0.0, np.clip(out, 0.0, None))
        return result

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["iso_x"] = list(self.iso_x)
        data["iso_y"] = list(self.iso_y)
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AlphaModel:
        values = dict(data)
        values["iso_x"] = tuple(values.get("iso_x", ()))
        values["iso_y"] = tuple(values.get("iso_y", ()))
        return cls(**values)


TARIFF_ALPHA = AlphaModel()


@dataclass
class AlphaFit:
    """Best alpha model plus the hold-out scores of every candidate."""

    model: AlphaModel
    mode: str = "exact"
    candidates: list[dict[str, Any]] = field(default_factory=list)
    n_train: int = 0
    n_test: int = 0
    zero_rule_share: float = float("nan")
    tariff_exact_share: float = float("nan")
    exact_rule_share: float = float("nan")

    def to_dict(self) -> dict[str, Any]:
        return {
            "model": self.model.to_dict(),
            "mode": self.mode,
            "candidates": self.candidates,
            "n_train": self.n_train,
            "n_test": self.n_test,
            "zero_rule_share": self.zero_rule_share,
            "tariff_exact_share": self.tariff_exact_share,
            "exact_rule_share": self.exact_rule_share,
        }


def _fit_logistic(x: FloatArray, y: FloatArray) -> tuple[float, float, float, float]:
    b0 = float(np.quantile(y, 0.99)) if len(y) else 100.0
    p0 = (0.0, max(b0, 1.0), float(np.median(x)) if len(x) else 450.0, 65.0)
    bounds = ([-50.0, 0.0, 0.0, 1.0], [200.0, 5_000.0, 5_000.0, 2_000.0])
    try:
        params, _ = curve_fit(_logistic, x, y, p0=p0, bounds=bounds, maxfev=20_000)
    except (RuntimeError, ValueError):
        params = np.asarray(p0, dtype=float)
    a, b, c, d = (float(v) for v in params)
    return a, b, c, d


def _fit_isotonic(x: FloatArray, y: FloatArray, n_points: int = 60) -> tuple[tuple[float, ...], tuple[float, ...]]:
    if len(x) == 0:
        return (), ()
    order = np.argsort(x, kind="stable")
    xs, ys = x[order], y[order]
    fitted = np.asarray(isotonic_regression(ys, increasing=True).x, dtype=float)
    grid = np.unique(np.quantile(xs, np.linspace(0.0, 1.0, n_points)))
    values = np.interp(grid, xs, fitted)
    return tuple(float(v) for v in grid), tuple(float(v) for v in np.maximum.accumulate(values))


def _candidate(kind: str, feature: str, x: FloatArray, y: FloatArray, threshold_mw: float) -> AlphaModel:
    if kind == "tariff":
        return AlphaModel(feature=feature, threshold_mw=threshold_mw, label=f"tariff:{feature}")
    if kind == "logistic":
        a, b, c, d = _fit_logistic(x, y)
        return AlphaModel(
            feature=feature,
            kind="logistic",
            a=a,
            b=b,
            c=c,
            d=d,
            threshold_mw=threshold_mw,
            use_price_factor=False,
            label=f"ls:{feature}",
        )
    if kind == "isotonic":
        gx, gy = _fit_isotonic(x, y)
        return AlphaModel(
            feature=feature,
            kind="isotonic",
            iso_x=gx,
            iso_y=gy,
            threshold_mw=threshold_mw,
            use_price_factor=False,
            label=f"iso:{feature}",
        )
    raise ValueError(f"unknown alpha model kind `{kind}`")


def alpha_change_errors(
    model: AlphaModel,
    si: FloatArray,
    si_prev: FloatArray,
    alpha: FloatArray,
    rows: NDArray[np.int64],
    mode: str,
    *,
    mip: FloatArray | None = None,
    mdp: FloatArray | None = None,
) -> FloatArray:
    """Errors when predicting ``alpha_k`` from quarter-hour ``k - 1`` with ``model.counterfactual`` (rows = k).

    ``exact`` mode uses the published MIP / MDP of quarter-hour ``k`` as the counterfactual marginal prices.
    """
    rows = rows[rows >= 2]
    prev_rows = rows - 1
    pred = model.counterfactual(
        alpha[prev_rows],
        si[prev_rows],
        si_prev[prev_rows],
        si[rows],
        si_prev[rows],
        mode=mode,
        mip_new=None if mip is None else mip[rows],
        mdp_new=None if mdp is None else mdp[rows],
    )
    out: FloatArray = pred - alpha[rows]
    return out[np.isfinite(out)]


def fit_alpha_model(
    si: ArrayLike,
    si_prev: ArrayLike,
    alpha: ArrayLike,
    *,
    mip: ArrayLike | None = None,
    mdp: ArrayLike | None = None,
    features: Sequence[str] = ALPHA_FEATURES,
    kinds: Sequence[str] = ("tariff", "logistic", "isotonic"),
    mode: str = "exact",
    test_fraction: float = 0.3,
    threshold_mw: float = ALPHA_THRESHOLD_MW,
) -> AlphaFit:
    """Score the exact rule and fitted alternatives on the last ``test_fraction`` of the sample.

    Rows must be consecutive quarter-hours in time order. Fitted candidates use rows with ``|SI| > threshold`` of the
    first part. Candidates are ranked by the mean absolute error of their ``mode`` counterfactual when predicting each
    hold-out quarter-hour's alpha from the previous quarter-hour (the use the operators make of the model; fitted
    candidates have no price factor, so they are scored in ``ratio`` mode); level errors are reported too. The
    returned model is the best candidate re-fitted on all rows. ``exact_rule_share`` / ``tariff_exact_share``: share
    of quarter-hours with ``|SI| > threshold`` matched to 0.011 / 0.05 EUR/MWh by the exact rule / the tariff curve
    without the price factor.
    """
    s = np.asarray(si, dtype=float)
    prev = np.asarray(si_prev, dtype=float)
    a_obs = np.asarray(alpha, dtype=float)
    mip_a = None if mip is None else np.asarray(mip, dtype=float)
    mdp_a = None if mdp is None else np.asarray(mdp, dtype=float)
    n = len(s)
    finite = np.isfinite(s) & np.isfinite(a_obs)
    below = finite & (np.abs(s) <= threshold_mw)
    zero_share = float(np.mean(np.abs(a_obs[below]) < 1e-9)) if below.any() else float("nan")
    active = finite & (np.abs(s) > threshold_mw)
    tariff_err = np.abs(AlphaModel(use_price_factor=False).predict(s, prev) - a_obs)[active]
    tariff_exact = float(np.mean(tariff_err < 0.05)) if active.any() else float("nan")
    exact_err = np.abs(TARIFF_ALPHA.predict(s, prev, mip=mip_a, mdp=mdp_a) - a_obs)[active]
    exact_share = float(np.mean(exact_err < 0.011)) if active.any() and mip_a is not None else float("nan")
    idx = np.flatnonzero(active)
    if len(idx) < 20:
        return AlphaFit(
            model=AlphaModel(threshold_mw=threshold_mw),
            mode=mode,
            zero_rule_share=zero_share,
            tariff_exact_share=tariff_exact,
            exact_rule_share=exact_share,
        )
    split_row = int(n * (1.0 - test_fraction))
    train = idx[idx < split_row]
    test_rows = np.arange(max(split_row, 2), n, dtype=np.int64)
    test_rows = test_rows[finite[test_rows] & finite[test_rows - 1]]
    test_active = idx[idx >= split_row]
    candidates: list[dict[str, Any]] = []
    best: tuple[float, str, str] | None = None
    for feature in features:
        x_all = alpha_feature(feature, s, prev)
        for kind in kinds:
            model = _candidate(kind, feature, x_all[train], a_obs[train], threshold_mw)
            m_mode = mode if model.use_price_factor else ("ratio" if mode == "exact" else mode)
            level_pred = model.predict(
                s[test_active],
                prev[test_active],
                mip=None if mip_a is None else mip_a[test_active],
                mdp=None if mdp_a is None else mdp_a[test_active],
            )
            level = level_pred - a_obs[test_active]
            change = alpha_change_errors(model, s, prev, a_obs, test_rows, m_mode, mip=mip_a, mdp=mdp_a)
            score = float(np.mean(np.abs(change))) if len(change) else float("nan")
            row: dict[str, Any] = {
                "feature": feature,
                "kind": kind if not (kind == "tariff" and mip_a is not None) else "tariff x price factor",
                "mode": m_mode,
                "change_mae": score,
                "level_mae": float(np.mean(np.abs(level))) if len(level) else float("nan"),
                "level_rmse": float(np.sqrt(np.mean(level**2))) if len(level) else float("nan"),
                "level_exact_share": float(np.mean(np.abs(level) < 0.05)) if len(level) else float("nan"),
            }
            candidates.append(row)
            if math.isfinite(score) and (best is None or score < best[0]):
                best = (score, feature, kind)
    if best is None:
        best = (float("nan"), "mean_abs_si_prev", "tariff")
    _, feature, kind = best
    x_all = alpha_feature(feature, s, prev)
    final = _candidate(kind, feature, x_all[idx], a_obs[idx], threshold_mw)
    persistence = a_obs[test_rows] - a_obs[test_rows - 1]
    candidates.append(
        {
            "feature": "-",
            "kind": "persistence",
            "mode": "-",
            "change_mae": float(np.nanmean(np.abs(persistence))) if len(persistence) else float("nan"),
            "level_mae": float("nan"),
            "level_rmse": float("nan"),
            "level_exact_share": float("nan"),
        }
    )
    return AlphaFit(
        model=final,
        mode=mode if final.use_price_factor else ("ratio" if mode == "exact" else mode),
        candidates=sorted(candidates, key=lambda r: np.inf if not np.isfinite(r["change_mae"]) else r["change_mae"]),
        n_train=int(len(train)),
        n_test=int(len(test_rows)),
        zero_rule_share=zero_share,
        tariff_exact_share=tariff_exact,
        exact_rule_share=exact_share,
    )
