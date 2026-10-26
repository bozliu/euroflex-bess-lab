"""Value attribution: a waterfall from the perfect-foresight oracle to the contractable P&L.

Inputs are several ledgers of the *same battery over the same period*, each differing from the previous one in a
single factor. The default chain (keys of the ``ledgers`` mapping, in order):

==================  ==================================================================================
key                 ledger
==================  ==================================================================================
``oracle``          perfect-foresight optimum on final prices (upper bound, not tradable)
``strategy``        the tradable strategy, same mechanism / price-taker / full availability
``mechanism``       the strategy re-run with prices of the other settlement mechanism (``new2026``)
``impact``          the same, impact-aware at fleet size ``Q`` (price moved by the fleet's own deviation)
``availability``    the same, with the contracted availability (outages)
``degradation``     the same, with the contractual cycling limit / wear cost
==================  ==================================================================================

Each effect is the difference between consecutive ledgers, so the waterfall is *sequential*: an effect measured
after another is conditional on it (the order matters when effects interact). Missing ledgers are skipped and the
next effect is measured against the last present ledger. All ledgers are aligned on their common intervals.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd

from .revenue import DAYS_PER_YEAR, _jsonable, infer_interval_minutes, normalize_ledger

DEFAULT_STEPS: tuple[tuple[str, str], ...] = (
    ("oracle", "Oracle (perfect foresight)"),
    ("strategy", "Decision / forecast gap"),
    ("mechanism", "Mechanism effect"),
    ("impact", "Price-impact effect"),
    ("availability", "Availability effect"),
    ("degradation", "Cycling / degradation effect"),
)

Basis = Literal["net", "gross"]


@dataclass(frozen=True)
class WaterfallResult:
    """Tidy waterfall table (+ daily detail) in EUR per MW per year over the common period."""

    table: pd.DataFrame
    daily: pd.DataFrame
    power_mw: float
    timezone: str
    basis: str
    period_start_utc: str
    period_end_utc: str
    first_local_day: str
    last_local_day: str
    n_intervals: int
    interval_minutes: float
    years_equivalent: float
    dropped_intervals: dict[str, int]
    notes: tuple[str, ...]

    def effect(self, key: str) -> float:
        rows = self.table.loc[self.table["key"] == key]
        if rows.empty:
            raise KeyError(f"no waterfall step `{key}`")
        return float(rows["delta_eur_per_mw_year"].iloc[0])

    def plot(self, path: str | Path, *, title: str | None = None, subtitle: str | None = None) -> Path:
        """Write the matplotlib waterfall chart (PNG) and return its path."""
        from .plots import plot_waterfall

        return plot_waterfall(self, path, title=title, subtitle=subtitle)

    @property
    def anchor_eur_per_mw_year(self) -> float:
        return float(self.table.loc[self.table["kind"] == "anchor", "level_eur_per_mw_year"].iloc[0])

    @property
    def final_eur_per_mw_year(self) -> float:
        return float(self.table.loc[self.table["kind"] == "total", "level_eur_per_mw_year"].iloc[0])

    def to_dict(self) -> dict[str, Any]:
        return _jsonable(
            {
                "units": "EUR per MW per year",
                "basis": self.basis,
                "power_mw": self.power_mw,
                "timezone": self.timezone,
                "period_start_utc": self.period_start_utc,
                "period_end_utc": self.period_end_utc,
                "first_local_day": self.first_local_day,
                "last_local_day": self.last_local_day,
                "n_intervals": self.n_intervals,
                "interval_minutes": self.interval_minutes,
                "years_equivalent": self.years_equivalent,
                "dropped_intervals": self.dropped_intervals,
                "anchor_eur_per_mw_year": self.anchor_eur_per_mw_year,
                "final_eur_per_mw_year": self.final_eur_per_mw_year,
                "steps": self.table.to_dict(orient="records"),
                "notes": list(self.notes),
            }
        )


def _local_dates(timestamps: pd.DatetimeIndex, timezone: str) -> pd.DatetimeIndex:
    return pd.DatetimeIndex(timestamps.tz_convert(timezone).tz_localize(None).normalize())


def value_waterfall(
    ledgers: Mapping[str, pd.DataFrame],
    *,
    power_mw: float,
    timezone: str = "Europe/Brussels",
    steps: Sequence[tuple[str, str]] = DEFAULT_STEPS,
    basis: Basis = "net",
    availability_factor: float | None = None,
    final_label: str = "Contractable P&L",
) -> WaterfallResult:
    """Sequential value waterfall over ledgers keyed by ``steps`` (first present step = anchor).

    ``basis="net"`` measures every level on ``pnl_eur`` (revenue minus wear); ``basis="gross"`` measures levels on
    ``revenue_eur`` and appends a final *degradation cost* bar so the total still equals net P&L. When no
    ``availability`` ledger is supplied, ``availability_factor`` adds an analytic pro-rata availability step that
    also scales every later level (an approximation: outages are assumed uncorrelated with value).
    """
    if power_mw <= 0.0:
        raise ValueError("power_mw must be positive")
    if basis not in ("net", "gross"):
        raise ValueError("basis must be 'net' or 'gross'")
    step_keys = [key for key, _ in steps]
    unknown = sorted(set(ledgers) - set(step_keys))
    if unknown:
        raise ValueError(f"ledgers {unknown} are not waterfall steps {step_keys}; pass custom `steps`")
    present = [(key, label) for key, label in steps if key in ledgers]
    if not present:
        raise ValueError("no ledgers given")
    if availability_factor is not None and not 0.0 < availability_factor <= 1.0:
        raise ValueError("availability_factor must be in (0, 1]")

    frames = {key: normalize_ledger(ledgers[key]).set_index("timestamp_utc") for key, _ in present}
    common = None
    for frame in frames.values():
        common = frame.index if common is None else common.intersection(frame.index)
    assert common is not None
    common = common.sort_values()
    if len(common) == 0:
        raise ValueError("ledgers have no common intervals")
    dropped = {key: int(len(frame) - len(common)) for key, frame in frames.items()}
    interval = infer_interval_minutes(pd.Series(common))
    years = len(common) * interval / (60.0 * 24.0 * DAYS_PER_YEAR)
    dates = _local_dates(pd.DatetimeIndex(common), timezone)
    per_year = 1.0 / (power_mw * years)

    rows: list[dict[str, Any]] = []
    daily_rows: list[pd.DataFrame] = []
    previous_level: float | None = None
    previous_daily: pd.Series | None = None
    anchor_level: float | None = None
    scale = 1.0
    last_degradation = 0.0
    notes: list[str] = []
    value_column = "pnl_eur" if basis == "net" else "revenue_eur"

    for key, label in steps:
        if key in frames:
            frame = frames[key].loc[common]
            pnl = float(frame["pnl_eur"].sum())
            revenue = float(frame["revenue_eur"].sum())
            degradation = float(frame["degradation_eur"].sum())
            level_total = (pnl if basis == "net" else revenue) * scale
            level = level_total * per_year
            daily = pd.Series(frame[value_column].to_numpy(dtype=float) * scale, index=dates).groupby(level=0).sum()
            daily = daily / power_mw
            source = "ledger"
            last_degradation = degradation * scale
        elif key == "availability" and availability_factor is not None and previous_level is not None:
            level = previous_level * availability_factor
            pnl = revenue = degradation = float("nan")
            assert previous_daily is not None
            daily = previous_daily * availability_factor
            last_degradation *= availability_factor
            scale *= availability_factor
            source = "analytic"
            label = f"{label} (analytic pro-rata, {availability_factor:.1%})"
            notes.append(
                f"availability effect is an analytic pro-rata approximation at {availability_factor:.1%}; "
                "it also scales later steps"
            )
        else:
            continue
        kind = "anchor" if previous_level is None else "effect"
        delta = level if previous_level is None else level - previous_level
        if anchor_level is None:
            anchor_level = level
        delta_daily = daily if previous_daily is None else daily - previous_daily
        rows.append(
            {
                "order": len(rows),
                "key": key,
                "label": label,
                "kind": kind,
                "source": source,
                "level_eur_per_mw_year": level,
                "delta_eur_per_mw_year": delta,
                "pnl_eur_per_mw_year": pnl * scale * per_year if source == "ledger" else float("nan"),
                "revenue_eur_per_mw_year": revenue * scale * per_year if source == "ledger" else float("nan"),
                "degradation_eur_per_mw_year": degradation * scale * per_year if source == "ledger" else float("nan"),
                "share_of_anchor": delta / anchor_level if anchor_level else float("nan"),
                "daily_delta_mean_eur_per_mw": float(delta_daily.mean()),
                "daily_delta_std_eur_per_mw": float(delta_daily.std(ddof=1)) if len(delta_daily) > 1 else 0.0,
                "share_days_delta_negative": float(np.mean(delta_daily.to_numpy() < 0.0)) if kind == "effect" else 0.0,
            }
        )
        daily_rows.append(
            pd.DataFrame(
                {
                    "date": np.asarray(daily.index),
                    "key": key,
                    "level_eur_per_mw": daily.to_numpy(dtype=float),
                    "delta_eur_per_mw": delta_daily.reindex(daily.index).to_numpy(dtype=float),
                }
            )
        )
        previous_level, previous_daily = level, daily

    assert previous_level is not None and anchor_level is not None
    if basis == "gross":
        degradation_level = last_degradation * per_year
        level = previous_level - degradation_level
        rows.append(
            {
                "order": len(rows),
                "key": "degradation_cost",
                "label": "Degradation cost",
                "kind": "effect",
                "source": "ledger",
                "level_eur_per_mw_year": level,
                "delta_eur_per_mw_year": -degradation_level,
                "pnl_eur_per_mw_year": float("nan"),
                "revenue_eur_per_mw_year": float("nan"),
                "degradation_eur_per_mw_year": degradation_level,
                "share_of_anchor": -degradation_level / anchor_level if anchor_level else float("nan"),
                "daily_delta_mean_eur_per_mw": float("nan"),
                "daily_delta_std_eur_per_mw": float("nan"),
                "share_days_delta_negative": float("nan"),
            }
        )
        previous_level = level
    rows.append(
        {
            "order": len(rows),
            "key": "total",
            "label": final_label,
            "kind": "total",
            "source": "derived",
            "level_eur_per_mw_year": previous_level,
            "delta_eur_per_mw_year": previous_level,
            "pnl_eur_per_mw_year": float("nan"),
            "revenue_eur_per_mw_year": float("nan"),
            "degradation_eur_per_mw_year": float("nan"),
            "share_of_anchor": previous_level / anchor_level if anchor_level else float("nan"),
            "daily_delta_mean_eur_per_mw": float("nan"),
            "daily_delta_std_eur_per_mw": float("nan"),
            "share_days_delta_negative": float("nan"),
        }
    )
    if present[0][0] != steps[0][0]:
        notes.append(f"anchor is `{present[0][0]}` because no `{steps[0][0]}` ledger was given")
    if any(dropped.values()):
        notes.append("ledgers were aligned on their common intervals; see dropped_intervals")
    return WaterfallResult(
        table=pd.DataFrame(rows),
        daily=pd.concat(daily_rows, ignore_index=True),
        power_mw=float(power_mw),
        timezone=timezone,
        basis=basis,
        period_start_utc=pd.Timestamp(common[0]).isoformat(),
        period_end_utc=(pd.Timestamp(common[-1]) + pd.Timedelta(minutes=interval)).isoformat(),
        first_local_day=dates[0].date().isoformat(),
        last_local_day=dates[-1].date().isoformat(),
        n_intervals=len(common),
        interval_minutes=interval,
        years_equivalent=years,
        dropped_intervals=dropped,
        notes=tuple(notes),
    )


def reprice_ledger(
    ledger: pd.DataFrame,
    prices: pd.DataFrame | pd.Series,
    *,
    price_column: str = "price_eur_mwh",
) -> pd.DataFrame:
    """Same dispatch, different settlement prices: ``revenue = energy_mwh * new price`` (degradation unchanged).

    ``prices`` is a frame with ``timestamp_utc`` and ``price_column`` or a series indexed by timestamp. Intervals
    without a new price earn nothing and are flagged in ``price_missing``.
    """
    data = normalize_ledger(ledger)
    if data["energy_mwh"].isna().all():
        raise ValueError("repricing needs `energy_mwh` in the ledger")
    if isinstance(prices, pd.Series):
        frame = pd.DataFrame({"timestamp_utc": prices.index, "new_price": prices.to_numpy(dtype=float)})
    else:
        if price_column not in prices.columns or "timestamp_utc" not in prices.columns:
            raise KeyError(f"price frame needs `timestamp_utc` and `{price_column}`")
        frame = prices.loc[:, ["timestamp_utc", price_column]].rename(columns={price_column: "new_price"})
    frame = frame.assign(timestamp_utc=pd.to_datetime(frame["timestamp_utc"], utc=True))
    frame = frame.drop_duplicates("timestamp_utc", keep="last")
    merged = data.merge(frame, on="timestamp_utc", how="left")
    new_price = merged["new_price"].to_numpy(dtype=float)
    missing = np.isnan(new_price)
    energy = merged["energy_mwh"].fillna(0.0).to_numpy(dtype=float)
    revenue = np.where(missing, 0.0, energy * np.nan_to_num(new_price))
    out = data.copy()
    out["price_eur_mwh"] = new_price
    out["price_missing"] = missing
    out["revenue_eur"] = revenue
    out["pnl_eur"] = revenue - out["degradation_eur"].to_numpy(dtype=float)
    return out


def mechanism_decomposition(
    current: pd.DataFrame,
    new: pd.DataFrame,
    new_prices: pd.DataFrame | pd.Series,
    *,
    power_mw: float,
    timezone: str = "Europe/Brussels",
    price_column: str = "price_eur_mwh",
) -> WaterfallResult:
    """Split the mechanism effect into *repricing* (same dispatch at new-formula prices) and *re-optimisation*."""
    repriced = reprice_ledger(current, new_prices, price_column=price_column)
    steps = (
        ("current", "Strategy under the current formula"),
        ("repriced", "Repricing effect (same dispatch, new-formula prices)"),
        ("new", "Re-optimisation effect (strategy run under the new formula)"),
    )
    return value_waterfall(
        {"current": current, "repriced": repriced, "new": new},
        power_mw=power_mw,
        timezone=timezone,
        steps=steps,
        final_label="Strategy under the new formula",
    )
