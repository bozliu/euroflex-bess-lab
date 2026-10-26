"""Hold-out evaluation of frozen forecasters: train once on data up to a cutoff ``T``, freeze, trade after ``T``.

The walk-forward folds of :mod:`.splits` retrain every month. A *frozen* model answers a stricter question: does a
model that stopped learning at ``T`` keep making money afterwards, and how quickly does it age? This module holds the
pieces that make that evaluation checkable:

- :func:`holdout_split`: the cutoff (end of a local calendar day) and the single train/test fold around it, with the
  same trailing training window and target-publication embargo as the walk-forward folds;
- :func:`fit_frozen` / :func:`frozen_training_audit`: fit on a market truncated at ``T`` and record the proof (one
  fold, no training row at or after ``T``, every training target published by ``T``);
- :func:`perturb_unpublished`: replace everything not yet published at a minute by noise (a fitted model must not
  change when post-``T`` data change);
- :class:`FrozenHistoryForecaster`: an ablation that also freezes the MPC's price profile (stage 2..H climatology
  and terminal value) at ``T``;
- :func:`paired_comparison`: paired daily P&L differences with a circular block-bootstrap confidence interval;
- :func:`evaluate_holdout`: per-arm summaries, monthly tables and the model-ageing curve of one cutoff;
- :func:`validate_summary`: schema check of the machine-readable summary (``reports/ledgers/oos_summary.json``).

"Frozen" means that no learned parameter is re-estimated after ``T``. Inputs that a live system recomputes from data
published before each decision (running quarter-hour values, recent final prices, the MPC's rolling price profile)
keep updating, exactly as in live operation; :class:`FrozenHistoryForecaster` removes the last of those as well.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date
from typing import Any

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ..metrics import daily_pnl, summarize_ledger
from ..replay import MarketData
from ..types import MINUTES_PER_QH, BatteryParams, PublicationModel
from .base import ForecastFrame, SupportsForecast, WalkForwardForecaster
from .features import target_known_minute
from .splits import Fold, training_rows

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]

DEFAULT_TIMEZONE = "Europe/Brussels"
QH_PER_DAY = 96
DAYS_PER_YEAR = 365.25
SUMMARY_SCHEMA_VERSION = 1


def _utc(value: str | date | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _qh_aligned(ts: pd.Timestamp) -> bool:
    return not (ts.second or ts.microsecond or ts.nanosecond or ts.minute % MINUTES_PER_QH)


def end_of_local_day(day: str | date | pd.Timestamp, timezone: str = DEFAULT_TIMEZONE) -> pd.Timestamp:
    """UTC instant at which the local calendar ``day`` ends, i.e. the next local midnight (DST-safe)."""
    ts = pd.Timestamp(day)
    if ts.tzinfo is not None:
        ts = ts.tz_convert(timezone).tz_localize(None)
    next_midnight = ts.normalize() + pd.Timedelta(days=1)
    return next_midnight.tz_localize(timezone).tz_convert("UTC")


# --------------------------------------------------------------------------------------------------------------
# the split


@dataclass(frozen=True)
class HoldoutSplit:
    """Train on ``[train_start_utc, cutoff_utc)`` once, then test on ``[cutoff_utc, test_end_utc)`` unchanged.

    ``cutoff_utc`` is ``T``: the end of the local cutoff day ``label``. Training rows are further embargoed by
    :func:`~.splits.training_rows` (a row is used only if its target was published by ``T``).
    """

    label: str
    cutoff_utc: pd.Timestamp
    train_start_utc: pd.Timestamp
    test_end_utc: pd.Timestamp
    timezone: str = DEFAULT_TIMEZONE

    def __post_init__(self) -> None:
        if not self.train_start_utc < self.cutoff_utc < self.test_end_utc:
            raise ValueError(f"invalid hold-out ordering: {self}")
        for name in ("cutoff_utc", "train_start_utc", "test_end_utc"):
            if not _qh_aligned(getattr(self, name)):
                raise ValueError(f"{name} must be aligned to a quarter-hour boundary")

    @property
    def fold(self) -> Fold:
        """The single walk-forward fold of the frozen model (``train_end == test_start == T``)."""
        return Fold(self.train_start_utc, self.cutoff_utc, self.cutoff_utc, self.test_end_utc)

    @property
    def test_days(self) -> float:
        return float((self.test_end_utc - self.cutoff_utc) / pd.Timedelta(days=1))

    @property
    def train_days(self) -> float:
        return float((self.cutoff_utc - self.train_start_utc) / pd.Timedelta(days=1))

    def months(self) -> list[str]:
        """Local calendar months (``YYYY-MM``) overlapping the test window."""
        first = self.cutoff_utc.tz_convert(self.timezone)
        last = (self.test_end_utc - pd.Timedelta(minutes=1)).tz_convert(self.timezone)
        periods = pd.period_range(first.strftime("%Y-%m"), last.strftime("%Y-%m"), freq="M")
        return [str(p) for p in periods]

    def months_since_cutoff(self, month: str) -> int:
        """Month index relative to the cutoff month (the first full month after a month-end cutoff is 1)."""
        cutoff_month = (self.cutoff_utc - pd.Timedelta(minutes=1)).tz_convert(self.timezone)
        year, mon = (int(x) for x in month.split("-")[:2])
        return (year * 12 + mon) - (cutoff_month.year * 12 + cutoff_month.month)

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "cutoff_utc": self.cutoff_utc.isoformat(),
            "train_start_utc": self.train_start_utc.isoformat(),
            "test_start_utc": self.cutoff_utc.isoformat(),
            "test_end_utc": self.test_end_utc.isoformat(),
            "timezone": self.timezone,
            "train_days": self.train_days,
            "test_days": self.test_days,
            "months": self.months(),
        }


def holdout_split(
    cutoff_day: str | date | pd.Timestamp,
    end_day: str | date | pd.Timestamp,
    *,
    train_months: int = 6,
    timezone: str = DEFAULT_TIMEZONE,
    data_start: str | pd.Timestamp | None = None,
) -> HoldoutSplit:
    """Cutoff ``T`` = end of local ``cutoff_day``; train on the ``train_months`` local calendar months before ``T``
    (clipped to ``data_start``); test on ``[T, end of local end_day)``."""
    if train_months < 1:
        raise ValueError("train_months must be at least 1")
    cutoff = end_of_local_day(cutoff_day, timezone)
    local_midnight = cutoff.tz_convert(timezone).tz_localize(None)
    train_start = (local_midnight - pd.DateOffset(months=train_months)).tz_localize(timezone).tz_convert("UTC")
    if data_start is not None:
        train_start = max(train_start, _utc(data_start))
    label = (cutoff - pd.Timedelta(minutes=1)).tz_convert(timezone).strftime("%Y-%m-%d")
    return HoldoutSplit(
        label=label,
        cutoff_utc=cutoff,
        train_start_utc=pd.Timestamp(train_start),
        test_end_utc=end_of_local_day(end_day, timezone),
        timezone=timezone,
    )


# --------------------------------------------------------------------------------------------------------------
# fitting and the proof that the fit is frozen


def minute_index(market: MarketData, ts: pd.Timestamp) -> int:
    """Minute index of ``ts`` on the market grid (may lie outside ``[0, n_minutes]``)."""
    return int((ts - market.start_utc) / pd.Timedelta(minutes=1))


def truncate_at(market: MarketData, end: str | pd.Timestamp) -> MarketData:
    """The market restricted to minutes before ``end`` (nothing at or after ``end`` survives)."""
    end_ts = _utc(end)
    if end_ts >= market.end_utc:
        return market
    if end_ts <= market.start_utc:
        raise ValueError("truncation point lies before the market start")
    return market.slice(market.start_utc, end_ts)


def frozen_training_audit(
    forecaster: WalkForwardForecaster,
    market: MarketData,
    split: HoldoutSplit,
    publication: PublicationModel | None = None,
) -> dict[str, Any]:
    """Machine-checkable evidence that ``forecaster`` (fitted on ``market``) is frozen at ``split.cutoff_utc``.

    Recomputes the embargoed training rows of every fold and checks: exactly one fold; it ends at or before ``T``
    and is tested from ``T`` on; no training decision minute at or after ``T``; every training target published
    by ``T`` under ``publication``.
    """
    publication = publication or forecaster.fit_publication_ or PublicationModel()
    cut = minute_index(market, split.cutoff_utc)
    stride = int(getattr(getattr(forecaster, "config", None), "stride", 1))
    folds = list(forecaster.folds_)
    passed = len(folds) == 1
    rows_info: list[dict[str, Any]] = []
    for fold in folds:
        passed &= fold.train_end <= split.cutoff_utc <= fold.test_start
        for horizon in (0, 1):
            rows = training_rows(market, fold, horizon=horizon, publication=publication, stride=stride)
            if len(rows) == 0:
                passed = False
                rows_info.append({"fold": fold.label, "horizon": horizon, "n_rows": 0})
                continue
            known = target_known_minute(market, horizon, publication)[rows]
            last_row, last_known = int(rows.max()), int(known.max())
            passed &= last_row < cut and last_known <= cut
            rows_info.append(
                {
                    "fold": fold.label,
                    "horizon": horizon,
                    "n_rows": len(rows),
                    "first_row_utc": (market.start_utc + pd.Timedelta(minutes=int(rows.min()))).isoformat(),
                    "last_row_utc": (market.start_utc + pd.Timedelta(minutes=last_row)).isoformat(),
                    "last_target_published_utc": (market.start_utc + pd.Timedelta(minutes=last_known)).isoformat(),
                }
            )
    return {
        "passed": bool(passed),
        "cutoff_utc": split.cutoff_utc.isoformat(),
        "n_folds": len(folds),
        "folds": [f.to_dict() for f in folds],
        "fit_market_start_utc": market.start_utc.isoformat(),
        "fit_market_end_utc": market.end_utc.isoformat(),
        "fit_market_ends_by_cutoff": bool(market.end_utc <= split.cutoff_utc),
        "training_rows": rows_info,
        "publication": {"minute_lag": publication.minute_lag, "qh_final_lag_minutes": publication.qh_final_lag_minutes},
    }


def fit_frozen(
    forecaster: WalkForwardForecaster,
    market: MarketData,
    split: HoldoutSplit,
    publication: PublicationModel | None = None,
) -> dict[str, Any]:
    """Fit ``forecaster`` once on the hold-out fold with data before ``T`` only and return the training audit.

    The market is truncated at ``T`` first, so no minute or quarter-hour at or after ``T`` can reach the fit, not
    even through feature construction. Raises ``RuntimeError`` if the audit fails.
    """
    publication = publication or PublicationModel()
    fit_market = truncate_at(market, split.cutoff_utc)
    forecaster.fit(fit_market, [split.fold], publication)
    audit = frozen_training_audit(forecaster, fit_market, split, publication)
    if not audit["passed"] or not audit["fit_market_ends_by_cutoff"]:
        raise RuntimeError(f"frozen fit of {forecaster.name} failed its training audit: {audit}")
    return audit


def assert_frozen(forecaster: Any, cutoff_utc: str | pd.Timestamp) -> None:
    """Raise ``ValueError`` unless ``forecaster`` (or the forecaster it wraps) has one fold ending by ``T`` and tested
    from ``T`` on."""
    cutoff = _utc(cutoff_utc)
    inner: Any = forecaster
    for _ in range(8):
        if isinstance(inner, WalkForwardForecaster):
            break
        inner = getattr(inner, "forecaster", None)
    if not isinstance(inner, WalkForwardForecaster):
        raise ValueError("no fitted WalkForwardForecaster found")
    folds = list(inner.folds_)
    if len(folds) != 1:
        raise ValueError(f"{inner.name} has {len(folds)} folds; a frozen model has exactly one")
    fold = folds[0]
    if not fold.train_end <= cutoff <= fold.test_start:
        raise ValueError(f"{inner.name} trains until {fold.train_end} / tests from {fold.test_start}, cutoff {cutoff}")


def perturb_unpublished(
    market: MarketData,
    cut_minute: int,
    publication: PublicationModel | None = None,
    *,
    seed: int = 0,
    scale: float = 500.0,
) -> MarketData:
    """Copy of ``market`` in which every value not yet published at decision minute ``cut_minute`` is noise.

    Under ``publication`` that is: minute rows from ``visible_minute_end(cut)``, quarter-hour values and settlement
    prices from ``visible_qh_end(cut)``, and day-ahead prices published after ``cut``. A model fitted at ``cut``
    (training rows embargoed to targets published by ``cut``) must be identical on both markets.
    """
    publication = publication or PublicationModel()
    rng = np.random.default_rng(seed)
    m_cut = publication.visible_minute_end(cut_minute)
    q_cut = publication.visible_qh_end(cut_minute)

    def noise(values: FloatArray, start: int) -> FloatArray:
        out = np.array(values, dtype=float, copy=True)
        if start < len(out):
            out[max(start, 0) :] = rng.normal(0.0, scale, size=len(out) - max(start, 0))
        return out

    minute = {k: noise(v, m_cut) for k, v in market.minute.items()}
    qh = {k: noise(v, q_cut) for k, v in market.qh.items()}
    settlement = {k: noise(v, q_cut) for k, v in market.settlement_prices.items()}
    da = np.array(market.da_price, dtype=float, copy=True)
    hidden = market.da_visible_from_minute > cut_minute
    da[hidden] = rng.normal(0.0, scale, size=int(hidden.sum()))
    return MarketData(
        **{**market.__dict__, "minute": minute, "qh": qh, "settlement_prices": settlement, "da_price": da}
    )


# --------------------------------------------------------------------------------------------------------------
# ablation: freeze the MPC's price profile too


def history_profile(
    history_prices: FloatArray,
    market: MarketData,
    cutoff_utc: str | pd.Timestamp,
    *,
    days: int = 28,
    gap_days: int = 1,
) -> FloatArray:
    """The ``days`` x 96 published quarter-hour prices of ``[T - days - gap_days, T - gap_days)``.

    ``profile[0]`` has the quarter-hour-of-day of ``T``; every value was published at least ``gap_days`` before
    ``T``. ``market`` must cover the window (e.g. the training market of the frozen model).
    """
    if days < 1 or gap_days < 0:
        raise ValueError("days must be >= 1 and gap_days >= 0")
    k_cut = minute_index(market, _utc(cutoff_utc)) // MINUTES_PER_QH
    k0, k1 = k_cut - QH_PER_DAY * (days + gap_days), k_cut - QH_PER_DAY * gap_days
    if k0 < 0 or k1 > len(history_prices):
        raise ValueError("market does not cover the profile window before the cutoff")
    return np.array(history_prices[k0:k1], dtype=float, copy=True)


class FrozenHistoryForecaster:
    """Ablation wrapper: the published-price history handed to the MPC is frozen at the cutoff.

    Forecasts are those of the wrapped forecaster; only ``ForecastFrame.history_prices`` changes: quarter-hours from
    ``T`` on are replaced by ``profile`` repeated (see :func:`history_profile`). The MPC builds its stage 2..H
    climatology and terminal value from that series, so every input of its value function is then fixed by data
    published before ``T``.
    """

    def __init__(
        self,
        base: SupportsForecast,
        cutoff_utc: str | pd.Timestamp,
        profile: FloatArray,
        name: str | None = None,
    ) -> None:
        values = np.asarray(profile, dtype=float)
        if values.ndim != 1 or len(values) == 0 or len(values) % QH_PER_DAY:
            raise ValueError("profile must be a non-empty whole number of days of quarter-hour prices")
        self.forecaster = base
        self.cutoff_utc = _utc(cutoff_utc)
        self.profile = values
        self.name = name or f"{base.name}+frozen_history"

    def frozen_history(self, market: MarketData, history: FloatArray) -> FloatArray:
        out = np.array(history, dtype=float, copy=True)
        offset = np.asarray((market.qh_times - self.cutoff_utc) / pd.Timedelta(minutes=MINUTES_PER_QH), dtype=float)
        after = offset >= 0
        out[after] = self.profile[offset[after].astype(np.int64) % len(self.profile)]
        return out

    def predict(
        self,
        market: MarketData,
        publication: PublicationModel | None = None,
        rows: NDArray[np.int64] | None = None,
    ) -> ForecastFrame:
        frame = self.forecaster.predict(market, publication, rows=rows)
        meta = dict(frame.meta)
        meta["frozen_history"] = {"cutoff_utc": self.cutoff_utc.isoformat(), "profile_days": len(self.profile) // 96}
        return replace(frame, history_prices=self.frozen_history(market, frame.history_prices), meta=meta)


# --------------------------------------------------------------------------------------------------------------
# statistics


def daily_pnl_series(ledger: pd.DataFrame, timezone: str = DEFAULT_TIMEZONE) -> pd.Series:
    """Net P&L per local delivery day (index ``YYYY-MM-DD``)."""
    daily = daily_pnl(ledger, timezone=timezone)
    index = pd.Index([str(d) for d in daily["date"]], name="date")
    return pd.Series(daily["pnl_eur"].to_numpy(dtype=float), index=index, name="pnl_eur")


def circular_block_bootstrap_indices(n: int, block: int, n_boot: int, rng: np.random.Generator) -> IntArray:
    """``(n_boot, n)`` index matrix of the circular block bootstrap (Politis and Romano, 1992).

    Each replicate concatenates ``ceil(n / block)`` blocks of ``block`` consecutive observations with uniformly drawn
    starts, wrapping around the end, and keeps the first ``n``.
    """
    if n <= 0 or n_boot <= 0:
        raise ValueError("n and n_boot must be positive")
    block = max(1, min(int(block), n))
    n_blocks = -(-n // block)
    starts = rng.integers(0, n, size=(n_boot, n_blocks))
    idx = (starts[:, :, None] + np.arange(block)[None, None, :]).reshape(n_boot, n_blocks * block)[:, :n]
    out: IntArray = (idx % n).astype(np.int64)
    return out


@dataclass(frozen=True)
class PairedComparison:
    """Paired daily comparison of ``arm`` against ``reference`` on their common delivery days."""

    arm: str
    reference: str
    days: int
    total_eur: float
    reference_total_eur: float
    ratio: float
    ratio_ci: tuple[float, float]
    diff_eur_per_mw_year: float
    diff_ci_eur_per_mw_year: tuple[float, float]
    p_value: float
    months: int
    months_won: int
    block_days: int
    n_boot: int
    confidence: float

    @property
    def share_months_won(self) -> float:
        return self.months_won / self.months if self.months else math.nan

    def to_dict(self) -> dict[str, Any]:
        return {
            "arm": self.arm,
            "reference": self.reference,
            "days": self.days,
            "total_eur": self.total_eur,
            "reference_total_eur": self.reference_total_eur,
            "ratio": self.ratio,
            "ratio_ci": list(self.ratio_ci),
            "uplift_pct": 100.0 * (self.ratio - 1.0),
            "uplift_ci_pct": [100.0 * (self.ratio_ci[0] - 1.0), 100.0 * (self.ratio_ci[1] - 1.0)],
            "diff_eur_per_mw_year": self.diff_eur_per_mw_year,
            "diff_ci_eur_per_mw_year": list(self.diff_ci_eur_per_mw_year),
            "p_value": self.p_value,
            "months": self.months,
            "months_won": self.months_won,
            "share_months_won": self.share_months_won,
            "block_days": self.block_days,
            "n_boot": self.n_boot,
            "confidence": self.confidence,
        }


def paired_comparison(
    a: pd.Series,
    b: pd.Series,
    *,
    arm: str = "a",
    reference: str = "b",
    power_mw: float = 1.0,
    block_days: int = 7,
    n_boot: int = 10_000,
    seed: int = 0,
    confidence: float = 0.95,
) -> PairedComparison:
    """Compare two daily P&L series (index ``YYYY-MM-DD``) day by day.

    - ``ratio = sum(a) / sum(b)`` with a percentile CI from the circular block bootstrap (blocks of ``block_days``
      consecutive days, resampled jointly for both series so the pairing is kept);
    - ``diff_eur_per_mw_year``: mean daily difference ``a - b`` annualised per MW, with its bootstrap CI;
    - ``p_value``: one-sided bootstrap p-value of ``H0: mean(a - b) <= 0`` (replicates re-centred on the null);
    - ``months_won``: local calendar months (``YYYY-MM`` of the index) in which ``sum(a) > sum(b)``.
    """
    if not 0.0 < confidence < 1.0:
        raise ValueError("confidence must be in (0, 1)")
    common = a.index.intersection(b.index).sort_values()
    x = a.reindex(common).to_numpy(dtype=float)
    y = b.reindex(common).to_numpy(dtype=float)
    ok = np.isfinite(x) & np.isfinite(y)
    x, y, days = x[ok], y[ok], common[ok]
    n = len(x)
    if n == 0:
        raise ValueError(f"no common days between {arm} and {reference}")
    d = x - y
    mean_d = float(d.mean())
    idx = circular_block_bootstrap_indices(n, block_days, n_boot, np.random.default_rng(seed))
    boot_mean = d[idx].mean(axis=1)
    sx, sy = x[idx].sum(axis=1), y[idx].sum(axis=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        boot_ratio = np.where(sy > 0.0, sx / np.where(sy > 0.0, sy, 1.0), np.nan)
    lo, hi = (1.0 - confidence) / 2.0, 1.0 - (1.0 - confidence) / 2.0
    scale = DAYS_PER_YEAR / power_mw
    diff_ci = np.quantile(boot_mean, [lo, hi]) * scale
    finite_ratio = boot_ratio[np.isfinite(boot_ratio)]
    ratio_ci = np.quantile(finite_ratio, [lo, hi]) if len(finite_ratio) else np.array([np.nan, np.nan])
    p_value = float((1 + np.sum(boot_mean - mean_d >= mean_d)) / (n_boot + 1))
    total_x, total_y = float(x.sum()), float(y.sum())
    months = pd.Index([str(day)[:7] for day in days])
    monthly_x = pd.Series(x, index=months).groupby(level=0).sum()
    monthly_y = pd.Series(y, index=months).groupby(level=0).sum()
    return PairedComparison(
        arm=arm,
        reference=reference,
        days=n,
        total_eur=total_x,
        reference_total_eur=total_y,
        ratio=total_x / total_y if total_y > 0 else math.nan,
        ratio_ci=(float(ratio_ci[0]), float(ratio_ci[1])),
        diff_eur_per_mw_year=mean_d * scale,
        diff_ci_eur_per_mw_year=(float(diff_ci[0]), float(diff_ci[1])),
        p_value=p_value,
        months=len(monthly_x),
        months_won=int((monthly_x > monthly_y).sum()),
        block_days=int(block_days),
        n_boot=int(n_boot),
        confidence=float(confidence),
    )


# --------------------------------------------------------------------------------------------------------------
# evaluation of one cutoff


ARM_SUMMARY_KEYS: tuple[str, ...] = (
    "days",
    "pnl_eur",
    "revenue_eur",
    "degradation_eur",
    "eur_per_mw_year",
    "capture_vs_oracle",
    "cycles_per_day",
    "avg_sell_price_eur_mwh",
    "avg_buy_price_eur_mwh",
    "daily_pnl_cvar05_eur",
    "max_drawdown_eur",
    "negative_day_share",
    "price_missing_qh",
)


def trim_to_test(ledger: pd.DataFrame, split: HoldoutSplit) -> pd.DataFrame:
    """Quarter-hour ledger rows inside the test window ``[T, test_end)``."""
    ts = pd.to_datetime(ledger["timestamp_utc"], utc=True)
    keep = (ts >= split.cutoff_utc) & (ts < split.test_end_utc)
    return ledger.loc[keep.to_numpy()].reset_index(drop=True)


def _local_month(ledger: pd.DataFrame, timezone: str) -> pd.Series:
    return pd.to_datetime(ledger["timestamp_utc"], utc=True).dt.tz_convert(timezone).dt.strftime("%Y-%m")


def monthly_rows(
    ledgers: Mapping[str, pd.DataFrame],
    split: HoldoutSplit,
    *,
    power_mw: float = 1.0,
    oracle: str | None = "oracle_qh",
) -> list[dict[str, Any]]:
    """Per local month of the test window: P&L, EUR/MW/yr (annualised on the hours covered) and capture vs oracle."""
    if not ledgers:
        return []
    first = next(iter(ledgers.values()))
    months = _local_month(first, split.timezone)
    hours = (months.groupby(months).size() * 0.25).to_dict()
    local_days = pd.to_datetime(first["timestamp_utc"], utc=True).dt.tz_convert(split.timezone).dt.date
    days = local_days.groupby(months.to_numpy()).nunique().to_dict()
    pnl = {
        name: ledger.groupby(_local_month(ledger, split.timezone))["pnl_eur"].sum() for name, ledger in ledgers.items()
    }
    rows: list[dict[str, Any]] = []
    for month in split.months():
        if month not in hours:
            continue
        month_start = pd.Timestamp(f"{month}-01")
        n_days_month = int(month_start.days_in_month)
        years = hours[month] / (24.0 * DAYS_PER_YEAR)
        row: dict[str, Any] = {
            "month": month,
            "months_since_cutoff": split.months_since_cutoff(month),
            "days": int(days.get(month, 0)),
            "partial": bool(int(days.get(month, 0)) < n_days_month),
            "pnl_eur": {},
            "eur_per_mw_year": {},
            "capture_vs_oracle": {},
        }
        oracle_pnl = float(pnl[oracle].get(month, np.nan)) if oracle and oracle in pnl else math.nan
        for name, series in pnl.items():
            value = float(series.get(month, 0.0))
            row["pnl_eur"][name] = value
            row["eur_per_mw_year"][name] = value / power_mw / years if years > 0 else math.nan
            if oracle and name != oracle and np.isfinite(oracle_pnl) and oracle_pnl > 0:
                row["capture_vs_oracle"][name] = value / oracle_pnl
        rows.append(row)
    return rows


def ageing_rows(monthly: Sequence[Mapping[str, Any]], pairs: Mapping[str, str]) -> list[dict[str, Any]]:
    """Model-ageing curve: monthly P&L of each frozen arm divided by its walk-forward counterpart."""
    out: list[dict[str, Any]] = []
    for frozen, wf in pairs.items():
        for row in monthly:
            a, b = row["pnl_eur"].get(frozen), row["pnl_eur"].get(wf)
            if a is None or b is None:
                continue
            out.append(
                {
                    "arm": frozen,
                    "reference": wf,
                    "month": row["month"],
                    "months_since_cutoff": row["months_since_cutoff"],
                    "partial": row["partial"],
                    "frozen_eur_per_mw_year": row["eur_per_mw_year"][frozen],
                    "walk_forward_eur_per_mw_year": row["eur_per_mw_year"][wf],
                    "ratio": a / b if b > 0 else math.nan,
                }
            )
    return out


def evaluate_holdout(
    ledgers: Mapping[str, pd.DataFrame],
    split: HoldoutSplit,
    *,
    battery: BatteryParams,
    mechanism: str,
    frozen_arms: Sequence[str],
    walk_forward: Mapping[str, str],
    thresholds: Sequence[str],
    oracle: str | None = "oracle_qh",
    block_days: int = 7,
    n_boot: int = 10_000,
    seed: int = 0,
    confidence: float = 0.95,
) -> dict[str, Any]:
    """Everything the out-of-sample report needs for one cutoff (JSON-ready, see :func:`validate_summary`).

    ``walk_forward`` maps a frozen arm to its monthly-retrained counterpart; the best threshold is the threshold arm
    with the highest total P&L over the test window (chosen after the fact, which favours the benchmark).
    """
    if not thresholds:
        raise ValueError("at least one threshold arm is needed")
    trimmed = {name: trim_to_test(ledger, split) for name, ledger in ledgers.items()}
    oracle_pnl = float(trimmed[oracle]["pnl_eur"].sum()) if oracle and oracle in trimmed else None
    arms: dict[str, dict[str, Any]] = {}
    for name, qh in trimmed.items():
        summary = summarize_ledger(
            qh,
            battery,
            n_minutes=len(qh) * MINUTES_PER_QH,
            oracle_pnl_eur=oracle_pnl if name != oracle else None,
            timezone=split.timezone,
        )
        arms[name] = {k: summary.get(k) for k in ARM_SUMMARY_KEYS}
    best_threshold = max(thresholds, key=lambda name: float(arms[name]["pnl_eur"]))
    daily = {name: daily_pnl_series(qh, split.timezone) for name, qh in trimmed.items()}

    def compare(arm: str, reference: str) -> dict[str, Any]:
        result = paired_comparison(
            daily[arm],
            daily[reference],
            arm=arm,
            reference=reference,
            power_mw=battery.power_mw,
            block_days=block_days,
            n_boot=n_boot,
            seed=seed,
            confidence=confidence,
        )
        return result.to_dict()

    comparisons: dict[str, dict[str, Any]] = {}
    for arm in (*frozen_arms, *dict.fromkeys(walk_forward.values())):
        if arm not in daily:
            continue
        entry: dict[str, Any] = {
            "vs_best_threshold": compare(arm, best_threshold),
            "vs_thresholds": {t: compare(arm, t) for t in thresholds},
        }
        wf = walk_forward.get(arm)
        if wf is not None and wf in daily:
            entry["vs_walk_forward"] = compare(arm, wf)
        if oracle and oracle in daily:
            entry["vs_oracle"] = compare(arm, oracle)
        comparisons[arm] = entry
    monthly = monthly_rows(trimmed, split, power_mw=battery.power_mw, oracle=oracle)
    return {
        "id": f"{mechanism}_{split.label}",
        "mechanism": mechanism,
        "split": split.to_dict(),
        "arms": arms,
        "frozen_arms": list(frozen_arms),
        "walk_forward": dict(walk_forward),
        "thresholds": list(thresholds),
        "oracle": oracle,
        "best_threshold": best_threshold,
        "comparisons": comparisons,
        "monthly": monthly,
        "ageing": ageing_rows(monthly, {k: v for k, v in walk_forward.items() if k in trimmed and v in trimmed}),
        "statistics": {"block_days": block_days, "n_boot": n_boot, "seed": seed, "confidence": confidence},
    }


# --------------------------------------------------------------------------------------------------------------
# JSON and schema


def to_jsonable(value: Any) -> Any:
    """Recursively convert numpy scalars/arrays and timestamps; non-finite floats become ``None``."""
    if isinstance(value, Mapping):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [to_jsonable(v) for v in value]
    if isinstance(value, np.ndarray):
        return [to_jsonable(v) for v in value.tolist()]
    if isinstance(value, bool | np.bool_):
        return bool(value)
    if isinstance(value, int | np.integer):
        return int(value)
    if isinstance(value, float | np.floating):
        f = float(value)
        return f if math.isfinite(f) else None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


_TOP_LEVEL: dict[str, type | tuple[type, ...]] = {
    "schema_version": int,
    "generated_by": str,
    "data_source": str,
    "battery": dict,
    "publication": dict,
    "settings": dict,
    "primary_arms": dict,
    "evaluations": list,
    "frozen_training_audit": list,
    "walk_forward_models": dict,
    "lookahead_audit": dict,
    "headline": list,
    "runtime_seconds": dict,
}
_EVALUATION: dict[str, type | tuple[type, ...]] = {
    "id": str,
    "mechanism": str,
    "split": dict,
    "arms": dict,
    "frozen_arms": list,
    "walk_forward": dict,
    "thresholds": list,
    "best_threshold": str,
    "comparisons": dict,
    "monthly": list,
    "ageing": list,
    "statistics": dict,
}
_SPLIT_KEYS = ("label", "cutoff_utc", "train_start_utc", "test_start_utc", "test_end_utc", "months")
_COMPARISON_KEYS = (
    "arm",
    "reference",
    "days",
    "ratio",
    "ratio_ci",
    "uplift_pct",
    "uplift_ci_pct",
    "diff_eur_per_mw_year",
    "diff_ci_eur_per_mw_year",
    "p_value",
    "months",
    "months_won",
    "share_months_won",
    "block_days",
    "n_boot",
)
_MONTH_KEYS = ("month", "months_since_cutoff", "days", "partial", "pnl_eur", "eur_per_mw_year", "capture_vs_oracle")
_AUDIT_ROW_KEYS = ("slice", "mechanism", "arm", "cut_minute", "passed", "tradable")


def _check(obj: Mapping[str, Any], spec: Mapping[str, type | tuple[type, ...]], where: str) -> list[str]:
    errors = []
    for key, kind in spec.items():
        if key not in obj:
            errors.append(f"{where}: missing `{key}`")
        elif not isinstance(obj[key], kind):
            errors.append(f"{where}: `{key}` should be {kind}, got {type(obj[key]).__name__}")
    return errors


def validate_evaluation(block: Mapping[str, Any], where: str = "evaluation") -> list[str]:
    errors = _check(block, _EVALUATION, where)
    if errors:
        return errors
    errors += [f"{where}.split: missing `{k}`" for k in _SPLIT_KEYS if k not in block["split"]]
    for arm in block["frozen_arms"]:
        if arm not in block["arms"]:
            errors.append(f"{where}: frozen arm `{arm}` has no summary")
        for key in ("eur_per_mw_year", "pnl_eur", "cycles_per_day"):
            if arm in block["arms"] and key not in block["arms"][arm]:
                errors.append(f"{where}.arms.{arm}: missing `{key}`")
        comp = block["comparisons"].get(arm)
        if comp is None:
            errors.append(f"{where}: no comparisons for `{arm}`")
            continue
        for name in ("vs_best_threshold", "vs_walk_forward"):
            if name not in comp:
                errors.append(f"{where}.comparisons.{arm}: missing `{name}`")
                continue
            errors += [
                f"{where}.comparisons.{arm}.{name}: missing `{k}`" for k in _COMPARISON_KEYS if k not in comp[name]
            ]
    if block["best_threshold"] not in block["thresholds"]:
        errors.append(f"{where}: best_threshold is not one of the thresholds")
    for i, row in enumerate(block["monthly"]):
        errors += [f"{where}.monthly[{i}]: missing `{k}`" for k in _MONTH_KEYS if k not in row]
    for i, row in enumerate(block["ageing"]):
        for key in ("arm", "reference", "month", "months_since_cutoff", "ratio"):
            if key not in row:
                errors.append(f"{where}.ageing[{i}]: missing `{key}`")
    return errors


def validate_summary(summary: Mapping[str, Any]) -> list[str]:
    """Problems with an out-of-sample summary (empty list = valid). Checks keys and types, not numbers."""
    errors = _check(summary, _TOP_LEVEL, "summary")
    if errors:
        return errors
    if summary["schema_version"] != SUMMARY_SCHEMA_VERSION:
        errors.append(f"summary: schema_version {summary['schema_version']} != {SUMMARY_SCHEMA_VERSION}")
    ids = []
    for i, block in enumerate(summary["evaluations"]):
        if not isinstance(block, Mapping):
            errors.append(f"evaluations[{i}] is not an object")
            continue
        errors += validate_evaluation(block, f"evaluations[{i}]")
        ids.append(block.get("id"))
        primary = summary["primary_arms"].get(block.get("mechanism"))
        if primary is not None and primary not in block.get("frozen_arms", []):
            errors.append(f"evaluations[{i}]: primary arm `{primary}` is not evaluated")
    if len(set(ids)) != len(ids):
        errors.append("summary: evaluation ids are not unique")
    for i, audit in enumerate(summary["frozen_training_audit"]):
        for key in ("model", "passed", "cutoff_utc", "n_folds"):
            if key not in audit:
                errors.append(f"frozen_training_audit[{i}]: missing `{key}`")
    rows = summary["lookahead_audit"].get("rows")
    if not isinstance(rows, list):
        errors.append("lookahead_audit: missing `rows` list")
    else:
        for i, row in enumerate(rows):
            errors += [f"lookahead_audit.rows[{i}]: missing `{k}`" for k in _AUDIT_ROW_KEYS if k not in row]
    for key in ("all_tradable_passed", "tradable_arms_audited"):
        if key not in summary["lookahead_audit"]:
            errors.append(f"lookahead_audit: missing `{key}`")
    if not all(isinstance(line, str) for line in summary["headline"]):
        errors.append("headline: every entry must be a string")
    return errors
