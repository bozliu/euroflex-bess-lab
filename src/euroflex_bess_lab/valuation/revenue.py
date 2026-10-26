"""Ledger adapters: interval P&L ledgers -> daily P&L tables, per-MW-year metrics and summary statistics.

Input contract (one row per settlement interval; the real-time simulator emits quarter-hours, the existing
backtest engine emits its native resolution):

========================  ========  ================================================================
column                    required  meaning
========================  ========  ================================================================
``timestamp_utc``         yes       interval start, tz-aware UTC (naive values are read as UTC)
``pnl_eur``               see note  net P&L of the interval = ``revenue_eur - degradation_eur``
``revenue_eur``           see note  market revenue (imbalance settlement: ``energy_mwh * price_eur_mwh``)
``degradation_eur``       no        wear charge (>= 0); 0 when absent
``energy_mwh``            no        net energy, ``+`` = sold / long (battery discharge)
``price_eur_mwh``         no        settlement price of the interval
``discharged_mwh``        no        gross discharge (defaults to ``max(energy_mwh, 0)``)
``charged_mwh``           no        gross charge (defaults to ``max(-energy_mwh, 0)``)
``price_missing``         no        ``True`` where the price was unavailable and the interval earned 0
========================  ========  ================================================================

Note: at least one of ``pnl_eur``, ``revenue_eur`` or (``energy_mwh`` and ``price_eur_mwh``) must be present. The
backtest-engine names ``realized_pnl_eur``/``total_pnl_eur``, ``degradation_cost_eur``, ``delivery_start_utc`` and
``price_eur_per_mwh`` are accepted as aliases.

Run metadata travels separately as :class:`LedgerMetadata` (a ``<ledger>.meta.json`` sidecar on disk).
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

DAYS_PER_YEAR = 365.25
"""Days per year used to annualise historical ledgers (matches the real-time simulator's summary)."""

MARKET_TIMEZONES: dict[str, str] = {
    "BE": "Europe/Brussels",
    "BELGIUM": "Europe/Brussels",
    "DE": "Europe/Berlin",
    "DE-LU": "Europe/Berlin",
    "DE_LU": "Europe/Berlin",
    "GERMANY": "Europe/Berlin",
    "NL": "Europe/Amsterdam",
    "NETHERLANDS": "Europe/Amsterdam",
    "FR": "Europe/Paris",
}

COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "timestamp_utc": ("delivery_start_utc", "interval_start_utc", "qh_start_utc"),
    "pnl_eur": ("realized_pnl_eur", "total_pnl_eur", "net_pnl_eur"),
    "revenue_eur": ("gross_revenue_eur", "market_revenue_eur"),
    "degradation_eur": ("degradation_cost_eur",),
    "price_eur_mwh": ("price_eur_per_mwh",),
    "energy_mwh": ("net_energy_mwh",),
}

LEDGER_COLUMNS: tuple[str, ...] = (
    "timestamp_utc",
    "energy_mwh",
    "discharged_mwh",
    "charged_mwh",
    "price_eur_mwh",
    "price_missing",
    "revenue_eur",
    "degradation_eur",
    "pnl_eur",
    "pnl_was_nan",
)

_MONTH_DAYS = np.array([31.0, 28.25, 31.0, 30.0, 31.0, 30.0, 31.0, 31.0, 30.0, 31.0, 30.0, 31.0])


def market_timezone(market: str | None, default: str = "Europe/Brussels") -> str:
    """Local delivery-day timezone for a market code such as ``BE``, ``DE-LU`` or ``NL``."""
    if not market:
        return default
    return MARKET_TIMEZONES.get(market.strip().upper(), default)


@dataclass(frozen=True)
class LedgerMetadata:
    """Run metadata that accompanies a ledger (battery spec, market, mechanism, strategy, fleet, period).

    ``fleet_mw`` is the implicit-reaction fleet size assumed when the ledger was simulated (0 = price-taker run on
    published prices). It must be on the same axis as any capacity-value curve used later.
    """

    asset_id: str = "asset"
    market: str = "BE"
    power_mw: float | None = None
    energy_mwh: float | None = None
    mechanism: str | None = None
    strategy: str | None = None
    fleet_mw: float = 0.0
    impact_aware: bool = False
    availability: float = 1.0
    degradation_eur_per_mwh: float | None = None
    period_start: str | None = None
    period_end: str | None = None
    publication_lag: str | None = None
    timezone: str | None = None
    source: str = "unspecified"
    synthetic: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def local_timezone(self) -> str:
        return self.timezone or market_timezone(self.market)

    def require_power_mw(self) -> float:
        if self.power_mw is None or not math.isfinite(self.power_mw) or self.power_mw <= 0.0:
            raise ValueError(
                f"ledger `{self.asset_id}` has no positive power_mw; pass it explicitly or add it to the .meta.json"
            )
        return float(self.power_mw)

    def with_overrides(self, **overrides: Any) -> LedgerMetadata:
        values = {f.name: getattr(self, f.name) for f in fields(self)}
        for key, value in overrides.items():
            if value is None:
                continue
            if key not in values:
                raise KeyError(f"unknown LedgerMetadata field `{key}`")
            values[key] = value
        return LedgerMetadata(**values)

    def to_dict(self) -> dict[str, Any]:
        return _jsonable(asdict(self))

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> LedgerMetadata:
        """Tolerant parser: accepts flat keys, a nested ``battery`` block and common aliases."""
        data = dict(payload)
        battery = data.pop("battery", None)
        if isinstance(battery, Mapping):
            for key in ("power_mw", "energy_mwh", "availability", "degradation_eur_per_mwh"):
                if battery.get(key) is not None:
                    data.setdefault(key, battery[key])
        publication = data.pop("publication", None)
        if isinstance(publication, Mapping) and "publication_lag" not in data:
            data["publication_lag"] = ", ".join(f"{key}={value}" for key, value in publication.items())
        aliases = {
            "battery_power_mw": "power_mw",
            "battery_energy_mwh": "energy_mwh",
            "fleet_size_mw": "fleet_mw",
            "strategy_name": "strategy",
            "mechanism_name": "mechanism",
            "zone": "market",
            "start": "period_start",
            "end": "period_end",
            "start_utc": "period_start",
            "end_utc": "period_end",
            "name": "asset_id",
        }
        for alias, canonical in aliases.items():
            if alias in data and canonical not in data:
                data[canonical] = data.pop(alias)
        known = {f.name for f in fields(cls)} - {"extra"}
        kwargs: dict[str, Any] = {key: data[key] for key in known if key in data and data[key] is not None}
        extra = dict(data.get("extra") or {}) if isinstance(data.get("extra"), Mapping) else {}
        extra.update({key: value for key, value in data.items() if key not in known and key != "extra"})
        for key in ("power_mw", "energy_mwh", "fleet_mw", "availability", "degradation_eur_per_mwh"):
            if key in kwargs:
                kwargs[key] = float(kwargs[key])
        for key in ("period_start", "period_end"):
            if key in kwargs:
                kwargs[key] = str(kwargs[key])
        return cls(**kwargs, extra=_jsonable(extra))


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def normalize_ledger(frame: pd.DataFrame, *, sum_duplicates: bool = False) -> pd.DataFrame:
    """Validate a ledger and return it in canonical form (columns :data:`LEDGER_COLUMNS`, sorted by time).

    Missing columns are derived (``pnl = revenue - degradation``, ``revenue = pnl + degradation`` or
    ``energy * price``). NaN P&L values are set to 0 and flagged in ``pnl_was_nan``. Duplicate timestamps raise
    unless ``sum_duplicates`` is set (e.g. site-level rows of a portfolio backtest).
    """
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("ledger must be a pandas DataFrame")
    data = frame.copy()
    for canonical, aliases in COLUMN_ALIASES.items():
        if canonical in data.columns:
            continue
        for alias in aliases:
            if alias in data.columns:
                data = data.rename(columns={alias: canonical})
                break
    if "timestamp_utc" not in data.columns:
        raise ValueError("ledger needs a `timestamp_utc` column (interval start, UTC)")
    if data.empty:
        raise ValueError("ledger is empty")

    out = pd.DataFrame({"timestamp_utc": pd.to_datetime(data["timestamp_utc"], utc=True)})
    for column in ("energy_mwh", "price_eur_mwh", "revenue_eur", "degradation_eur", "pnl_eur"):
        if column in data.columns:
            out[column] = pd.to_numeric(data[column], errors="coerce").astype(float).to_numpy()

    has_energy = "energy_mwh" in out.columns
    has_price = "price_eur_mwh" in out.columns
    if "degradation_eur" not in out.columns:
        out["degradation_eur"] = 0.0
    out["degradation_eur"] = out["degradation_eur"].fillna(0.0)
    if "revenue_eur" not in out.columns and "pnl_eur" not in out.columns:
        if not (has_energy and has_price):
            raise ValueError("ledger needs `pnl_eur`, `revenue_eur`, or both `energy_mwh` and `price_eur_mwh`")
        out["revenue_eur"] = (out["energy_mwh"] * out["price_eur_mwh"]).to_numpy()
    if "revenue_eur" not in out.columns:
        out["revenue_eur"] = out["pnl_eur"] + out["degradation_eur"]
    if "pnl_eur" not in out.columns:
        out["pnl_eur"] = out["revenue_eur"] - out["degradation_eur"]

    if not has_energy:
        out["energy_mwh"] = np.nan
    if not has_price:
        out["price_eur_mwh"] = np.nan
    for column, sign in (("discharged_mwh", 1.0), ("charged_mwh", -1.0)):
        if column in data.columns:
            out[column] = pd.to_numeric(data[column], errors="coerce").astype(float).to_numpy()
        else:
            out[column] = np.clip(sign * out["energy_mwh"].to_numpy(dtype=float), 0.0, None)
    if "price_missing" in data.columns:
        out["price_missing"] = data["price_missing"].fillna(False).astype(bool).to_numpy()
    else:
        out["price_missing"] = out["price_eur_mwh"].isna().to_numpy() & has_price

    out["pnl_was_nan"] = out["pnl_eur"].isna().to_numpy()
    for column in ("pnl_eur", "revenue_eur"):
        out[column] = out[column].fillna(0.0)

    out = out.sort_values("timestamp_utc", kind="stable").reset_index(drop=True)
    if out["timestamp_utc"].duplicated().any():
        if not sum_duplicates:
            n_dup = int(out["timestamp_utc"].duplicated().sum())
            raise ValueError(
                f"ledger has {n_dup} duplicate timestamps; pass sum_duplicates=True to aggregate site-level rows"
            )
        sums = out.groupby("timestamp_utc", sort=True)[
            ["energy_mwh", "discharged_mwh", "charged_mwh", "revenue_eur", "degradation_eur", "pnl_eur"]
        ].sum(min_count=1)
        flags = out.groupby("timestamp_utc", sort=True)[["price_missing", "pnl_was_nan"]].any()
        prices = out.groupby("timestamp_utc", sort=True)["price_eur_mwh"].mean()
        out = pd.concat([sums, flags, prices], axis=1).reset_index()
    return out.loc[:, list(LEDGER_COLUMNS)]


def infer_interval_minutes(timestamps: pd.Series | pd.DatetimeIndex) -> float:
    """Median spacing of the ledger timestamps in minutes (15 for quarter-hour ledgers)."""
    index = pd.DatetimeIndex(pd.to_datetime(timestamps, utc=True)).sort_values()
    if len(index) < 2:
        return 15.0
    # resolution-agnostic (pandas 3 defaults to microseconds): convert to float minutes explicitly
    diffs = np.diff(index.tz_convert(None).to_numpy().astype("datetime64[ns]").astype(np.int64)) / 60e9
    diffs = diffs[diffs > 0]
    return float(np.median(diffs)) if len(diffs) else 15.0


def _local_day_hours(dates: pd.DatetimeIndex, timezone: str) -> np.ndarray:
    """Length of each local day in hours (23 / 24 / 25 around DST switches)."""
    start = dates.tz_localize(timezone, nonexistent="shift_forward")
    end = (dates + pd.Timedelta(days=1)).tz_localize(timezone, nonexistent="shift_forward")
    return np.asarray((end - start) / pd.Timedelta(hours=1), dtype=float)


def daily_pnl(
    ledger: pd.DataFrame,
    *,
    timezone: str = "Europe/Brussels",
    power_mw: float | None = None,
    interval_minutes: float | None = None,
) -> pd.DataFrame:
    """Aggregate a ledger into local delivery days.

    Returns one row per local date with ``date`` (naive midnight of the local day), ``pnl_eur``, ``revenue_eur``,
    ``degradation_eur``, ``discharged_mwh``, ``charged_mwh``, ``n_intervals``, ``expected_intervals`` (DST-aware:
    92/96/100 quarter-hours), ``coverage``, ``hours_covered``, ``n_price_missing`` and, when ``power_mw`` is given,
    the per-MW columns ``pnl_eur_per_mw``, ``revenue_eur_per_mw`` and ``degradation_eur_per_mw``.
    Partial days are kept and flagged by ``coverage``; use :func:`complete_days` before computing statistics.
    """
    data = normalize_ledger(ledger)
    step = float(interval_minutes) if interval_minutes else infer_interval_minutes(data["timestamp_utc"])
    local_date = data["timestamp_utc"].dt.tz_convert(timezone).dt.tz_localize(None).dt.normalize()
    data = data.assign(date=local_date)
    grouped = data.groupby("date", sort=True)
    out = grouped[["pnl_eur", "revenue_eur", "degradation_eur"]].sum()
    out[["discharged_mwh", "charged_mwh"]] = grouped[["discharged_mwh", "charged_mwh"]].sum(min_count=1)
    out["n_intervals"] = grouped.size().astype(int)
    out["n_price_missing"] = grouped["price_missing"].sum().astype(int)
    dates = pd.DatetimeIndex(out.index)
    out["expected_intervals"] = _local_day_hours(dates, timezone) * 60.0 / step
    out["coverage"] = out["n_intervals"] / out["expected_intervals"]
    out["hours_covered"] = out["n_intervals"] * step / 60.0
    if power_mw is not None:
        if power_mw <= 0.0:
            raise ValueError("power_mw must be positive")
        for column in ("pnl_eur", "revenue_eur", "degradation_eur"):
            out[f"{column}_per_mw"] = out[column] / float(power_mw)
    out = out.reset_index()
    out["date"] = pd.to_datetime(out["date"])
    return out


def complete_days(daily: pd.DataFrame, min_coverage: float = 0.98) -> pd.DataFrame:
    """Days whose interval coverage is at least ``min_coverage`` (drops the partial first/last local day)."""
    if "coverage" not in daily.columns:
        return daily.copy()
    return daily.loc[daily["coverage"] >= min_coverage].reset_index(drop=True)


def annualise(total: float, n_days: float, *, power_mw: float = 1.0, days_per_year: float = DAYS_PER_YEAR) -> float:
    """Scale a period total to EUR per MW per year."""
    if n_days <= 0:
        return float("nan")
    return float(total) / float(power_mw) / float(n_days) * days_per_year


def seasonal_annual_value(daily: pd.DataFrame, column: str = "pnl_eur_per_mw") -> float | None:
    """Season-weighted annual value: sum over calendar months of (mean daily value x average days in month).

    Removes the bias of plain annualisation when a ledger covers part of a year (e.g. Jan-Jul 2026). Returns
    ``None`` unless all 12 calendar months are present.
    """
    if daily.empty:
        return None
    months = pd.DatetimeIndex(daily["date"]).month
    means = daily.groupby(months)[column].mean()
    if len(means) < 12:
        return None
    return float(np.sum(means.sort_index().to_numpy(dtype=float) * _MONTH_DAYS))


@dataclass(frozen=True)
class RevenueSummary:
    """Per-MW-year and daily statistics of one ledger over its complete local days."""

    asset_id: str
    market: str
    timezone: str
    mechanism: str | None
    strategy: str | None
    power_mw: float
    energy_mwh: float | None
    fleet_mw: float
    first_day: str | None
    last_day: str | None
    n_days: int
    n_days_dropped: int
    days_covered: float
    n_intervals: int
    interval_minutes: float
    n_price_missing: int
    total_pnl_eur: float
    total_revenue_eur: float
    total_degradation_eur: float
    pnl_eur_per_mw_year: float
    revenue_eur_per_mw_year: float
    degradation_eur_per_mw_year: float
    seasonal_pnl_eur_per_mw_year: float | None
    daily_mean_eur_per_mw: float
    daily_std_eur_per_mw: float
    daily_p05_eur_per_mw: float
    daily_p50_eur_per_mw: float
    daily_p95_eur_per_mw: float
    negative_day_share: float
    worst_day: str | None
    worst_day_eur_per_mw: float
    best_day: str | None
    best_day_eur_per_mw: float
    top_decile_day_share: float | None
    max_drawdown_eur_per_mw: float
    full_cycles_per_day: float | None
    synthetic: bool

    def to_dict(self) -> dict[str, Any]:
        return _jsonable(asdict(self))


def summarize_daily(
    daily: pd.DataFrame,
    metadata: LedgerMetadata,
    *,
    min_coverage: float = 0.98,
    interval_minutes: float = 15.0,
) -> RevenueSummary:
    """Summary statistics from a :func:`daily_pnl` table (complete days only)."""
    power = metadata.require_power_mw()
    kept = complete_days(daily, min_coverage)
    n_days = len(kept)
    # covered time in days (partial days count pro rata; 23 / 25-hour DST days by their intervals)
    if "hours_covered" in kept.columns:
        days_covered = float(kept["hours_covered"].sum()) / 24.0
    else:
        days_covered = float(kept["n_intervals"].sum()) * float(interval_minutes) / 1440.0
    if n_days == 0:
        raise ValueError(f"ledger `{metadata.asset_id}` has no complete local days (min_coverage={min_coverage})")
    per_mw = kept["pnl_eur"].to_numpy(dtype=float) / power
    total_pnl = float(kept["pnl_eur"].sum())
    total_rev = float(kept["revenue_eur"].sum())
    total_deg = float(kept["degradation_eur"].sum())
    order = np.argsort(per_mw)
    worst_i, best_i = int(order[0]), int(order[-1])
    descending = np.sort(per_mw)[::-1]
    n_top = max(1, int(math.ceil(0.1 * n_days)))
    top_share = float(descending[:n_top].sum() / per_mw.sum()) if per_mw.sum() > 0 else None
    cumulative = np.cumsum(per_mw)
    drawdown = float(np.max(np.maximum.accumulate(np.concatenate([[0.0], cumulative]))[1:] - cumulative))
    discharged = kept["discharged_mwh"].to_numpy(dtype=float)
    cycles: float | None = None
    if metadata.energy_mwh and np.isfinite(discharged).any():
        cycles = float(np.nansum(discharged) / float(metadata.energy_mwh) / days_covered)
    seasonal_frame = kept.assign(pnl_eur_per_mw=per_mw)
    dates = pd.DatetimeIndex(kept["date"])
    return RevenueSummary(
        asset_id=metadata.asset_id,
        market=metadata.market,
        timezone=metadata.local_timezone,
        mechanism=metadata.mechanism,
        strategy=metadata.strategy,
        power_mw=power,
        energy_mwh=metadata.energy_mwh,
        fleet_mw=metadata.fleet_mw,
        first_day=dates[0].date().isoformat(),
        last_day=dates[-1].date().isoformat(),
        n_days=n_days,
        n_days_dropped=len(daily) - n_days,
        days_covered=days_covered,
        n_intervals=int(kept["n_intervals"].sum()),
        interval_minutes=float(interval_minutes),
        n_price_missing=int(kept["n_price_missing"].sum()),
        total_pnl_eur=total_pnl,
        total_revenue_eur=total_rev,
        total_degradation_eur=total_deg,
        pnl_eur_per_mw_year=annualise(total_pnl, days_covered, power_mw=power),
        revenue_eur_per_mw_year=annualise(total_rev, days_covered, power_mw=power),
        degradation_eur_per_mw_year=annualise(total_deg, days_covered, power_mw=power),
        seasonal_pnl_eur_per_mw_year=seasonal_annual_value(seasonal_frame, "pnl_eur_per_mw"),
        daily_mean_eur_per_mw=float(per_mw.mean()),
        daily_std_eur_per_mw=float(per_mw.std(ddof=1)) if n_days > 1 else 0.0,
        daily_p05_eur_per_mw=float(np.quantile(per_mw, 0.05)),
        daily_p50_eur_per_mw=float(np.quantile(per_mw, 0.50)),
        daily_p95_eur_per_mw=float(np.quantile(per_mw, 0.95)),
        negative_day_share=float(np.mean(per_mw < 0.0)),
        worst_day=dates[worst_i].date().isoformat(),
        worst_day_eur_per_mw=float(per_mw[worst_i]),
        best_day=dates[best_i].date().isoformat(),
        best_day_eur_per_mw=float(per_mw[best_i]),
        top_decile_day_share=top_share,
        max_drawdown_eur_per_mw=drawdown,
        full_cycles_per_day=cycles,
        synthetic=metadata.synthetic,
    )


def summarize_ledger(
    ledger: pd.DataFrame,
    metadata: LedgerMetadata,
    *,
    min_coverage: float = 0.98,
) -> RevenueSummary:
    """:func:`daily_pnl` + :func:`summarize_daily` in one call."""
    normalized = normalize_ledger(ledger)
    step = infer_interval_minutes(normalized["timestamp_utc"])
    daily = daily_pnl(
        normalized, timezone=metadata.local_timezone, power_mw=metadata.require_power_mw(), interval_minutes=step
    )
    return summarize_daily(daily, metadata, min_coverage=min_coverage, interval_minutes=step)


def load_ledger(path: str | Path) -> pd.DataFrame:
    """Read a ledger from parquet or CSV (timestamps are parsed by :func:`normalize_ledger`)."""
    file = Path(path)
    if not file.exists():
        raise FileNotFoundError(f"ledger file not found: {file}")
    suffix = "".join(file.suffixes[-2:]).lower() if file.suffix.lower() == ".gz" else file.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(file)
    if suffix in {".csv", ".csv.gz"}:
        return pd.read_csv(file)
    raise ValueError(f"unsupported ledger format `{file.suffix}` (use .parquet or .csv)")


def metadata_sidecar_paths(path: str | Path) -> list[Path]:
    """Candidate sidecar locations: ``X.meta.json`` (data-contract style) and ``X.parquet.meta.json``."""
    file = Path(path)
    return [file.with_suffix(".meta.json"), file.with_name(f"{file.name}.meta.json")]


def load_ledger_metadata(path: str | Path) -> LedgerMetadata | None:
    """Metadata from the first existing sidecar of ``path`` (``None`` if there is none).

    Looks for ``X.meta.json`` / ``X.parquet.meta.json`` and then for the ``summary.json`` that
    ``SimulationResult.to_parquet`` writes next to ``qh_ledger.parquet`` (used when it has a ``battery`` block).
    """
    for candidate in metadata_sidecar_paths(path):
        if candidate.exists():
            payload = json.loads(candidate.read_text(encoding="utf-8"))
            if not isinstance(payload, Mapping):
                raise ValueError(f"metadata sidecar {candidate.name} must hold a JSON object")
            return LedgerMetadata.from_dict(payload)
    run_summary = Path(path).with_name("summary.json")
    if run_summary.exists():
        payload = json.loads(run_summary.read_text(encoding="utf-8"))
        if isinstance(payload, Mapping) and isinstance(payload.get("battery"), Mapping):
            asset = Path(path).parent.name or Path(path).stem
            return LedgerMetadata.from_dict({"asset_id": asset, "source": run_summary.name, **payload})
    return None


def read_ledger(path: str | Path, **overrides: Any) -> tuple[pd.DataFrame, LedgerMetadata]:
    """Ledger plus metadata (sidecar if present, then ``overrides``; ``asset_id`` defaults to the file stem)."""
    file = Path(path)
    metadata = load_ledger_metadata(file) or LedgerMetadata(asset_id=file.stem.split(".")[0], source=file.name)
    return normalize_ledger(load_ledger(file)), metadata.with_overrides(**overrides)


def write_ledger(ledger: pd.DataFrame, path: str | Path, metadata: LedgerMetadata | None = None) -> Path:
    """Write a canonical ledger (parquet or CSV by suffix) plus an optional ``X.meta.json`` sidecar."""
    file = Path(path)
    file.parent.mkdir(parents=True, exist_ok=True)
    frame = normalize_ledger(ledger)
    if file.suffix.lower() == ".csv":
        frame.to_csv(file, index=False)
    else:
        frame.to_parquet(file, index=False)
    if metadata is not None:
        file.with_suffix(".meta.json").write_text(json.dumps(metadata.to_dict(), indent=2) + "\n", encoding="utf-8")
    return file


def ledger_from_simulation(result: Any) -> pd.DataFrame:
    """Canonical ledger from a real-time ``SimulationResult`` (its ``qh`` frame) or from a ledger DataFrame."""
    if isinstance(result, pd.DataFrame):
        return normalize_ledger(result)
    qh = getattr(result, "qh", None)
    if not isinstance(qh, pd.DataFrame):
        raise TypeError("expected a DataFrame or an object with a `qh` DataFrame attribute")
    return normalize_ledger(qh)


def metadata_from_simulation(result: Any, **overrides: Any) -> LedgerMetadata:
    """Best-effort :class:`LedgerMetadata` from a simulation result (duck-typed; ``overrides`` win)."""
    payload: dict[str, Any] = {}
    for attribute in ("meta", "metadata"):
        meta = getattr(result, attribute, None)
        if isinstance(meta, Mapping):
            payload.update(meta)
    timezone = getattr(result, "timezone", None)
    if isinstance(timezone, str):
        payload.setdefault("timezone", timezone)
    publication = getattr(result, "publication", None)
    if publication is not None and not isinstance(publication, str):
        lags = {
            key: getattr(publication, key)
            for key in ("minute_lag", "qh_final_lag_minutes", "da_publication_hour_local")
            if hasattr(publication, key)
        }
        if lags:
            payload.setdefault("publication_lag", ", ".join(f"{key}={value}" for key, value in lags.items()))
    qh = getattr(result, "qh", None)
    if isinstance(qh, pd.DataFrame) and len(qh) and "timestamp_utc" in qh.columns:
        stamps = pd.to_datetime(qh["timestamp_utc"], utc=True)
        payload.setdefault("period_start", stamps.min().isoformat())
        payload.setdefault(
            "period_end", (stamps.max() + pd.Timedelta(minutes=infer_interval_minutes(stamps))).isoformat()
        )
    battery = getattr(result, "battery", None)
    if battery is not None:
        for key in ("power_mw", "energy_mwh", "availability", "degradation_eur_per_mwh"):
            value = getattr(battery, key, None)
            if value is not None:
                payload.setdefault(key, value)
    for attribute, key in (("strategy", "strategy"), ("mechanism", "mechanism")):
        value = getattr(result, attribute, None)
        if value is not None:
            payload.setdefault(key, getattr(value, "name", value) if not isinstance(value, str) else value)
    metadata = LedgerMetadata.from_dict(payload)
    return metadata.with_overrides(**overrides)
