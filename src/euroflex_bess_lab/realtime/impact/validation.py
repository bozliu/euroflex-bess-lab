"""Back-tests of the impact operators on history.

Two complementary checks, both reported on a hold-out period the curves were not fitted on:

1. **Volume-response curves** (``validate_curve_changes``): for consecutive quarter-hours of the same delivery hour,
   predict the price change from the change in activated volume, ``f(V_k) - f(V_{k-1})``, and compare with the
   realised change. Baseline: no change. Reported: MAE / RMSE of both, out-of-sample R^2 against the baseline, the
   correlation and the calibration slope (realised change regressed on the predicted change; 1 = well calibrated).

2. **Full operator, natural SI shifts** (``validate_operator_si_shift``): treat the change of the system imbalance
   between consecutive quarter-hours, ``q = SI_k - SI_{k-1}``, as if a fleet had deviated by ``q`` from the state of
   quarter-hour ``k - 1`` and predict the *published price* of quarter-hour ``k`` (including the branch switch when
   the SI changes sign). Baselines: persistence (``P_{k-1}``) and the same operator with flat volume curves (only
   the branch / alpha logic). This is an analogue test: real SI changes also move bid prices, the day-ahead level,
   etc., which the operator does not know, so the errors are an upper bound on the operator's error for a pure
   fleet deviation.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import Any

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from .curves import RegimeCurves, ResponseCurve, hour_groups
from .operators import CurrentFormulaImpact, CurrentResponseFit, NewFormula2026Impact, NewFormulaResponseFit

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]


def _flat_like(curves: RegimeCurves) -> RegimeCurves:
    return RegimeCurves(pooled=ResponseCurve.flat(label="flat", increasing=curves.pooled.increasing))


@dataclass
class ErrorStats:
    """Prediction errors of a model and a baseline on the same rows."""

    name: str
    n: int
    mae: float
    rmse: float
    baseline_mae: float
    baseline_rmse: float
    r2_vs_baseline: float
    corr: float
    calibration_slope: float
    sign_hit_rate: float
    extra: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        extra = out.pop("extra")
        out.update(extra)
        return out


def error_stats(
    name: str,
    actual: FloatArray,
    predicted: FloatArray,
    baseline: FloatArray,
    *,
    target_is_change: bool,
    extra: dict[str, Any] | None = None,
) -> ErrorStats:
    """Compare ``predicted`` and ``baseline`` against ``actual``.

    For change targets the correlation / calibration / sign statistics are computed on the changes themselves; for
    level targets they use the predicted change relative to the baseline (``predicted - baseline``) against the
    realised change (``actual - baseline``).
    """
    ok = np.isfinite(actual) & np.isfinite(predicted) & np.isfinite(baseline)
    a, p, b = actual[ok], predicted[ok], baseline[ok]
    n = int(len(a))
    if n == 0:
        nan = float("nan")
        return ErrorStats(name, 0, nan, nan, nan, nan, nan, nan, nan, nan, dict(extra or {}))
    err, err0 = p - a, b - a
    sse, sse0 = float(np.sum(err**2)), float(np.sum(err0**2))
    if target_is_change:
        real, pred = a, p
    else:
        real, pred = a - b, p - b
    moving = np.abs(pred) > 1e-9
    corr = float(np.corrcoef(pred, real)[0, 1]) if n > 2 and np.std(pred) > 0 and np.std(real) > 0 else float("nan")
    slope = float(np.dot(pred, real) / np.dot(pred, pred)) if np.dot(pred, pred) > 0 else float("nan")
    hits = float(np.mean(np.sign(pred[moving]) == np.sign(real[moving]))) if moving.any() else float("nan")
    return ErrorStats(
        name=name,
        n=n,
        mae=float(np.mean(np.abs(err))),
        rmse=float(np.sqrt(sse / n)),
        baseline_mae=float(np.mean(np.abs(err0))),
        baseline_rmse=float(np.sqrt(sse0 / n)),
        r2_vs_baseline=1.0 - sse / sse0 if sse0 > 0 else float("nan"),
        corr=corr,
        calibration_slope=slope,
        sign_hit_rate=hits,
        extra=dict(extra or {}),
    )


def consecutive_pairs(times: pd.DatetimeIndex, *, same_hour: bool = True) -> IntArray:
    """Row indices ``k`` (>= 1) whose predecessor ``k - 1`` is the previous quarter-hour (same delivery hour)."""
    idx = pd.DatetimeIndex(times)
    if len(idx) < 2:
        return np.zeros(0, dtype=np.int64)
    step_ok = (idx[1:] - idx[:-1]) == pd.Timedelta(minutes=15)
    rows = np.flatnonzero(np.asarray(step_ok)) + 1
    if same_hour:
        groups = hour_groups(idx)
        rows = rows[groups[rows] == groups[rows - 1]]
    return rows.astype(np.int64)


def validate_curve_changes(
    name: str,
    volume: FloatArray,
    price: FloatArray,
    times: pd.DatetimeIndex,
    curves: RegimeCurves,
    regime: IntArray,
    *,
    rows: IntArray | None = None,
    winsor_quantiles: tuple[float, float] = (0.005, 0.995),
) -> ErrorStats:
    """Predict within-hour price changes from volume changes with ``curves`` (see module docstring).

    ``rows`` restricts the evaluation to quarter-hours ``k`` in the hold-out period. Realised changes are winsorised
    at ``winsor_quantiles`` for the robust statistics in ``extra`` (raw statistics are the headline).
    """
    pairs = consecutive_pairs(times)
    if rows is not None:
        pairs = np.intersect1d(pairs, rows)
    v, p = np.asarray(volume, dtype=float), np.asarray(price, dtype=float)
    actual = p[pairs] - p[pairs - 1]
    predicted = curves.delta(regime[pairs], v[pairs - 1], v[pairs])
    zero = np.zeros_like(actual)
    finite = np.isfinite(actual)
    lo, hi = np.quantile(actual[finite], winsor_quantiles) if finite.any() else (0.0, 0.0)
    robust = error_stats(f"{name}:winsorised", np.clip(actual, lo, hi), predicted, zero, target_is_change=True)
    return error_stats(
        name,
        actual,
        predicted,
        zero,
        target_is_change=True,
        extra={
            "robust_mae": robust.mae,
            "robust_baseline_mae": robust.baseline_mae,
            "robust_r2_vs_baseline": robust.r2_vs_baseline,
            "robust_calibration_slope": robust.calibration_slope,
            "mean_abs_predicted_change": float(np.nanmean(np.abs(predicted))) if len(predicted) else float("nan"),
        },
    )


def _shift_predictions(operator: CurrentFormulaImpact | NewFormula2026Impact, pairs: IntArray) -> FloatArray:
    si = operator.state.si_mw
    q = si[pairs] - si[pairs - 1]
    if isinstance(operator, CurrentFormulaImpact):
        prev2 = np.where(pairs >= 2, pairs - 2, pairs - 1)
        q_prev = si[pairs - 1] - si[prev2]
        return operator.price_at(q, rows=pairs - 1, q_prev=q_prev)
    return operator.price_at(q, rows=pairs - 1)


def validate_operator_si_shift(
    name: str,
    operator: CurrentFormulaImpact | NewFormula2026Impact,
    published_price: FloatArray,
    *,
    rows: IntArray | None = None,
    same_hour: bool = False,
) -> dict[str, Any]:
    """Predict ``P_k`` from the state of ``k - 1`` and the SI change (see module docstring)."""
    pairs = consecutive_pairs(operator.times, same_hour=same_hour)
    if rows is not None:
        pairs = np.intersect1d(pairs, rows)
    price = np.asarray(published_price, dtype=float)
    actual, persistence = price[pairs], price[pairs - 1]
    predicted = _shift_predictions(operator, pairs)
    flat_op: CurrentFormulaImpact | NewFormula2026Impact
    if isinstance(operator, CurrentFormulaImpact):
        flat_c: CurrentResponseFit = replace(
            operator.fit, mip=_flat_like(operator.fit.mip), mdp=_flat_like(operator.fit.mdp)
        )
        flat_op = replace(operator, fit=flat_c, up_delta=None, down_delta=None)
    else:
        fit = operator.fit
        flat_n: NewFormulaResponseFit = replace(
            fit,
            afrr_up=_flat_like(fit.afrr_up),
            afrr_down=_flat_like(fit.afrr_down),
            mfrr_up=_flat_like(fit.mfrr_up),
            mfrr_down=_flat_like(fit.mfrr_down),
        )
        flat_op = replace(operator, fit=flat_n)
    flat = _shift_predictions(flat_op, pairs)
    si = operator.state.si_mw
    flip = np.sign(si[pairs]) != np.sign(si[pairs - 1])
    out: dict[str, Any] = {"name": name, "pairs": int(len(pairs)), "sign_flip_share": float(np.mean(flip))}
    for label, mask in (("all", np.ones(len(pairs), dtype=bool)), ("sign_flip", flip), ("same_sign", ~flip)):
        out[label] = {
            "vs_persistence": error_stats(
                f"{name}:{label}", actual[mask], predicted[mask], persistence[mask], target_is_change=False
            ).to_dict(),
            "flat_curves_vs_persistence": error_stats(
                f"{name}:{label}:flat", actual[mask], flat[mask], persistence[mask], target_is_change=False
            ).to_dict(),
        }
    return out


def holdout_split(times: pd.DatetimeIndex, split_utc: str | pd.Timestamp) -> tuple[IntArray, IntArray]:
    """Row indices before / at-or-after ``split_utc``."""
    ts = pd.Timestamp(split_utc)
    ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    idx = pd.DatetimeIndex(times)
    before = np.flatnonzero(np.asarray(idx < ts)).astype(np.int64)
    after = np.flatnonzero(np.asarray(idx >= ts)).astype(np.int64)
    return before, after
