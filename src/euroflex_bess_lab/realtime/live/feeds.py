"""Polling clients for Elia's near-real-time Open Data feeds and Energy-Charts day-ahead prices.

Every feed returns *contract-shaped* frames (``Plan.md`` section 5): snake_case column names, tz-aware UTC
timestamps (``timestamp_utc`` = interval start), float measurements and string quality flags. The upstream ->
contract column maps are imported from :mod:`euroflex_bess_lab.public_data.normalize`, so a row seen live and the
same row downloaded later from Elia's historical dataset land in the same columns with the same dtypes.

Time semantics (Elia Open Data field documentation):

- ``ods161`` / ``ods175`` (per minute, cumulative quarter-hour-to-date): ``datetime`` is the minute at the end of
  which the calculation ran; row ``11:49`` covers ``[11:45, 11:50)`` and is complete at 11:50:00.
- ``ods169`` (per minute, instantaneous): ``datetime`` is the interval start; complete one minute later.
- ``ods162`` / ``ods212`` (per quarter-hour): interval start; complete at the end of the quarter-hour.
- ``ods136`` / ``ods147`` (Elia SI forecast for the current / next quarter-hour): ``predictiontimeutc`` is the issue
  time of the forecast.

A row's *completion time* is ``timestamp + resolution`` (the issue time for forecasts). The first poll that sees a
row, minus its completion time, is the observed publication latency; it is what calibrates
:attr:`~euroflex_bess_lab.realtime.PublicationModel.minute_lag`.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import time
from collections import deque
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from email.utils import parsedate_to_datetime
from typing import Any, Literal, Protocol, runtime_checkable

import numpy as np
import pandas as pd
import requests

from ...public_data.energy_charts import infer_resolution_minutes, parse_price_payload
from ...public_data.normalize import (
    NEW_FORMULA_MAP,
    NEW_FORMULA_QH_COLUMNS,
    ODS126_MAP,
    ODS133_MAP,
    ODS134_MAP,
    ODS165_MAP,
    SI_FORECAST_MAP,
    STRING_COLUMNS,
    cast_contract_types,
)

LOGGER = logging.getLogger(__name__)

ELIA_BASE_URL = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets"
ENERGY_CHARTS_PRICE_URL = "https://api.energy-charts.info/price"
USER_AGENT = "euroflex-bess-lab-live/1 (+https://github.com/bozliu/euroflex-bess-lab)"
ELIA_ATTRIBUTION = "Elia Transmission Belgium SA, Elia Open Data (CC BY 4.0)"
ENERGY_CHARTS_ATTRIBUTION = "Bundesnetzagentur | SMARD.de via Energy-Charts (CC BY 4.0)"
LOCAL_TIMEZONE = "Europe/Brussels"
RETRY_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
MIN_POLL_BUDGET_S = 1.0
_NS_PER_S = 1_000_000_000

TableKind = Literal["minute", "qh", "nf_minute", "nf_qh", "si_forecast_current", "si_forecast_next", "day_ahead"]

#: Column prefix of each table inside :class:`~euroflex_bess_lab.realtime.MarketData` (see ``Plan.md`` section 7).
TABLE_PREFIX: dict[str, str] = {
    "minute": "",
    "qh": "",
    "nf_minute": "nf_",
    "nf_qh": "nf_",
    "si_forecast_current": "elia_fc_cur_",
    "si_forecast_next": "elia_fc_next_",
    "day_ahead": "",
}
MINUTE_TABLES = frozenset({"minute", "nf_minute", "si_forecast_current", "si_forecast_next"})
QH_TABLES = frozenset({"qh", "nf_qh"})


class FeedError(RuntimeError):
    """A poll failed (network, HTTP status or payload)."""


class FeedSchemaError(FeedError):
    """The upstream payload no longer matches the expected shape (renamed field, wrong type, ...)."""


class RetryLater(FeedError):
    """The upstream asked us to back off (HTTP 429 with ``Retry-After``) and retries were exhausted."""

    def __init__(self, message: str, retry_after_s: float | None) -> None:
        super().__init__(message)
        self.retry_after_s = retry_after_s


# --- feed specifications --------------------------------------------------------------------------------------

_QUALITY = {"qualitystatus": "quality_status"}
# ods175 (near real-time) names four fields differently from its historical twin ods165.
_ODS175_UPSTREAM = {
    "upwardavailablemfrrprice": "mfrrup",
    "downwardavailablemfrrprice": "mfrrdown",
    "intertsopriceimport": "price_intertso_i",
    "intertsopriceexport": "price_intertso_e",
}
ODS175_MAP: dict[str, str] = {_ODS175_UPSTREAM.get(k, k): v for k, v in ODS165_MAP.items()}


@dataclass(frozen=True)
class FeedSpec:
    """One upstream dataset and how it maps onto the data contract.

    ``columns`` maps upstream field -> contract column (excluding the time field). ``resolution_minutes`` is the
    interval length (0 for point-in-time forecasts) and defines the completion time used for latency and gaps.
    """

    name: str
    dataset_id: str
    table: TableKind
    columns: Mapping[str, str]
    time_field: str = "datetime"
    time_column: str = "timestamp_utc"
    extra_time_fields: Mapping[str, str] = field(default_factory=dict)
    required: tuple[str, ...] = ()
    resolution_minutes: int = 1
    poll_every_s: float = 15.0
    limit: int = 20
    source: Literal["elia", "energy_charts", "replay"] = "elia"
    historical_dataset_id: str | None = None
    historical_columns: Mapping[str, str] | None = None
    description: str = ""

    @property
    def prefix(self) -> str:
        return TABLE_PREFIX[self.table]

    @property
    def contract_columns(self) -> tuple[str, ...]:
        derived = ("qh_start_utc",) if self.table == "minute" else ()
        return (self.time_column, *self.extra_time_fields.values(), *derived, *dict.fromkeys(self.columns.values()))

    @property
    def numeric_columns(self) -> tuple[str, ...]:
        """Contract columns that become numeric arrays in :class:`MarketData` (no times, no flags)."""
        return tuple(
            c for c in dict.fromkeys(self.columns.values()) if c not in STRING_COLUMNS and not c.endswith("_utc")
        )

    @property
    def completion_offset(self) -> pd.Timedelta:
        return pd.Timedelta(minutes=self.resolution_minutes)

    @property
    def grid_step(self) -> pd.Timedelta:
        """Expected spacing of consecutive rows (one minute for per-minute forecasts)."""
        return pd.Timedelta(minutes=self.resolution_minutes or 1)

    def historical(self) -> FeedSpec | None:
        """Spec of the validated/historical twin dataset (``ods133`` for ``ods161``, ...), if any."""
        if not self.historical_dataset_id:
            return None
        return replace(
            self,
            name=f"{self.name}_historical",
            dataset_id=self.historical_dataset_id,
            columns=dict(self.historical_columns or self.columns),
            historical_dataset_id=None,
            historical_columns=None,
        )


IMBALANCE_PRICE_MINUTE = FeedSpec(
    name="imbalance_price_minute",
    dataset_id="ods161",
    table="minute",
    columns=ODS133_MAP,
    required=("datetime", "systemimbalance", "imbalanceprice"),
    poll_every_s=15.0,
    limit=20,
    historical_dataset_id="ods133",
    description="Imbalance price per minute, cumulative quarter-hour-to-date (near real-time)",
)
SYSTEM_IMBALANCE_MINUTE = FeedSpec(
    name="system_imbalance_minute",
    dataset_id="ods169",
    table="minute",
    columns={**ODS126_MAP, **_QUALITY},
    required=("datetime", "systemimbalance"),
    poll_every_s=15.0,
    limit=20,
    historical_dataset_id="ods126",
    description="Current system imbalance and activated volumes per minute, instantaneous (near real-time)",
)
PRICE_COMPONENTS_MINUTE = FeedSpec(
    name="price_components_minute",
    dataset_id="ods175",
    table="minute",
    columns={**ODS175_MAP, **_QUALITY},
    required=("datetime", "r2priceup", "r2pricedown"),
    poll_every_s=15.0,
    limit=20,
    historical_dataset_id="ods165",
    historical_columns={**ODS165_MAP, **_QUALITY},
    description="Balancing energy price components per minute (near real-time)",
)
IMBALANCE_PRICE_QH = FeedSpec(
    name="imbalance_price_qh",
    dataset_id="ods162",
    table="qh",
    columns=ODS134_MAP,
    required=("datetime", "systemimbalance", "imbalanceprice"),
    resolution_minutes=15,
    poll_every_s=30.0,
    limit=12,
    historical_dataset_id="ods134",
    description="Imbalance price per quarter-hour (near real-time, current day, not validated)",
)
SI_FORECAST_CURRENT_QH = FeedSpec(
    name="si_forecast_current_qh",
    dataset_id="ods136",
    table="si_forecast_current",
    columns=SI_FORECAST_MAP,
    time_field="predictiontimeutc",
    time_column="prediction_time_utc",
    extra_time_fields={"predictions_forecastedtimeutc": "target_qh_utc"},
    required=("predictiontimeutc", "predictions_forecastedtimeutc", "predictions_silinearregressionforecast"),
    resolution_minutes=0,
    poll_every_s=30.0,
    limit=5,
    description="Elia system-imbalance forecast for the current quarter-hour",
)
SI_FORECAST_NEXT_QH = replace(
    SI_FORECAST_CURRENT_QH,
    name="si_forecast_next_qh",
    dataset_id="ods147",
    table="si_forecast_next",
    description="Elia system-imbalance forecast for the next quarter-hour",
)
NEW_FORMULA_MINUTE = FeedSpec(
    name="new_formula_minute",
    dataset_id="ods211",
    table="nf_minute",
    columns=NEW_FORMULA_MAP,
    required=("datetime", "imbalanceprice"),
    poll_every_s=600.0,
    limit=20,
    description="New 2026 imbalance-price formula per minute (live from the go-live; simulation before)",
)
NEW_FORMULA_QH = FeedSpec(
    name="new_formula_qh",
    dataset_id="ods212",
    table="nf_qh",
    columns={k: v for k, v in NEW_FORMULA_MAP.items() if v in NEW_FORMULA_QH_COLUMNS},
    required=("datetime", "imbalanceprice"),
    resolution_minutes=15,
    poll_every_s=600.0,
    limit=8,
    description="New 2026 imbalance-price formula per quarter-hour (live from the go-live; simulation before)",
)
DAY_AHEAD_BE = FeedSpec(
    name="day_ahead_be",
    dataset_id="energy_charts_price_BE",
    table="day_ahead",
    columns={"price": "price_eur_mwh"},
    time_field="unix_seconds",
    resolution_minutes=15,
    poll_every_s=1800.0,
    source="energy_charts",
    description="Belgian day-ahead prices (Energy-Charts, SMARD/BNetzA)",
)

ELIA_FEEDS: tuple[FeedSpec, ...] = (
    IMBALANCE_PRICE_MINUTE,
    SYSTEM_IMBALANCE_MINUTE,
    PRICE_COMPONENTS_MINUTE,
    IMBALANCE_PRICE_QH,
    SI_FORECAST_CURRENT_QH,
    SI_FORECAST_NEXT_QH,
    NEW_FORMULA_MINUTE,
    NEW_FORMULA_QH,
)
DEFAULT_FEEDS: tuple[FeedSpec, ...] = (*ELIA_FEEDS, DAY_AHEAD_BE)


def feed_by_name(name: str, specs: Iterable[FeedSpec] = DEFAULT_FEEDS) -> FeedSpec:
    for spec in specs:
        if name in (spec.name, spec.dataset_id):
            return spec
    raise KeyError(f"unknown feed `{name}`")


# --- normalization --------------------------------------------------------------------------------------------


def empty_contract_frame(spec: FeedSpec) -> pd.DataFrame:
    data: dict[str, pd.Series] = {}
    for column in spec.contract_columns:
        if column.endswith("_utc"):
            data[column] = pd.Series(dtype="datetime64[ns, UTC]")
        elif column in STRING_COLUMNS:
            data[column] = pd.Series(dtype="string")
        else:
            data[column] = pd.Series(dtype="float32")
    return pd.DataFrame(data)


def records_from_payload(spec: FeedSpec, payload: Any) -> list[Mapping[str, Any]]:
    """Validate an Explore v2.1 ``/records`` (dict with ``results``) or ``/exports/json`` (list) payload."""
    if isinstance(payload, Mapping):
        if "error_code" in payload:
            raise FeedError(f"{spec.dataset_id}: API error {payload.get('error_code')}: {payload.get('message')}")
        results = payload.get("results")
    else:
        results = payload
    if not isinstance(results, list):
        raise FeedSchemaError(f"{spec.dataset_id}: expected a list of records, got {type(results).__name__}")
    for record in results:
        if not isinstance(record, Mapping):
            raise FeedSchemaError(f"{spec.dataset_id}: every record must be an object")
    return list(results)


def normalize_records(spec: FeedSpec, records: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    """Map raw Elia records onto contract columns (UTC times, float32 values, string flags), sorted and unique."""
    if not records:
        return empty_contract_frame(spec)
    raw = pd.DataFrame.from_records([dict(record) for record in records])
    missing = [name for name in spec.required if name not in raw.columns]
    if missing:
        raise FeedSchemaError(
            f"{spec.dataset_id}: upstream records lack required fields {missing}; present: {sorted(raw.columns)}"
        )
    frame = pd.DataFrame({spec.time_column: pd.to_datetime(raw[spec.time_field], utc=True, errors="coerce")})
    for upstream, contract in spec.extra_time_fields.items():
        values = raw[upstream] if upstream in raw.columns else pd.Series([None] * len(raw))
        frame[contract] = pd.to_datetime(values, utc=True, errors="coerce")
    if spec.table == "minute":
        frame["qh_start_utc"] = frame[spec.time_column].dt.floor("15min")
    for upstream, contract in spec.columns.items():
        frame[contract] = raw[upstream] if upstream in raw.columns else np.nan
    frame = frame.dropna(subset=[spec.time_column])
    frame = frame.drop_duplicates(subset=[spec.time_column], keep="last").sort_values(spec.time_column)
    return cast_contract_types(frame.reset_index(drop=True))


def quarterhour_mismatches(records: Sequence[Mapping[str, Any]]) -> int:
    """Rows whose upstream ``quarterhour`` differs from the floor of ``datetime`` (schema sanity check)."""
    count = 0
    for record in records:
        qh, ts = record.get("quarterhour"), record.get("datetime")
        if qh is None or ts is None:
            continue
        if pd.Timestamp(str(qh)).tz_convert("UTC") != pd.Timestamp(str(ts)).tz_convert("UTC").floor("15min"):
            count += 1
    return count


RawRecord = tuple[str, Mapping[str, Any]]


def raw_index(
    spec: FeedSpec, records: Sequence[Mapping[str, Any]], dataset_id: str | None = None
) -> dict[int, RawRecord]:
    """``(dataset_id, record as published)`` keyed by the UTC nanosecond of the time field (last occurrence wins)."""
    source = dataset_id or spec.dataset_id
    index: dict[int, RawRecord] = {}
    for record in records:
        value = record.get(spec.time_field)
        if value is None:
            continue
        try:
            stamp = pd.Timestamp(str(value))
        except (TypeError, ValueError):
            continue
        stamp = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
        index[int(stamp.value)] = (source, record)
    return index


def energy_charts_raw_index(payload: Any, dataset_id: str = "energy_charts_price_BE") -> dict[int, RawRecord]:
    """Energy-Charts products as published (``unix_seconds``, ``price``) keyed by UTC nanosecond."""
    if not isinstance(payload, Mapping):
        return {}
    seconds, prices = payload.get("unix_seconds") or [], payload.get("price") or []
    return {
        int(sec) * _NS_PER_S: (dataset_id, {"unix_seconds": sec, "price": price})
        for sec, price in zip(seconds, prices, strict=False)
    }


def normalize_energy_charts(payload: Any) -> pd.DataFrame:
    """Energy-Charts ``/price`` payload -> ``timestamp_utc``, ``resolution_minutes``, ``price_eur_mwh`` (float32)."""
    if not isinstance(payload, Mapping):
        raise FeedSchemaError("energy-charts: expected a JSON object")
    if "unix_seconds" not in payload or "price" not in payload:
        raise FeedSchemaError(f"energy-charts: payload lacks `unix_seconds`/`price`; keys: {sorted(payload)}")
    unit = payload.get("unit")
    if unit is not None and "EUR" not in str(unit):
        raise FeedSchemaError(f"energy-charts: unexpected price unit {unit!r}")
    try:
        frame = parse_price_payload(dict(payload))
    except ValueError as exc:
        raise FeedSchemaError(f"energy-charts: {exc}") from exc
    frame = frame.dropna(subset=["price_eur_mwh"])
    frame = frame.drop_duplicates("timestamp_utc", keep="last").sort_values("timestamp_utc").reset_index(drop=True)
    if frame.empty:
        return pd.DataFrame(
            {
                "timestamp_utc": pd.Series(dtype="datetime64[ns, UTC]"),
                "resolution_minutes": pd.Series(dtype="int16"),
                "price_eur_mwh": pd.Series(dtype="float32"),
            }
        )
    frame["timestamp_utc"] = frame["timestamp_utc"].astype("datetime64[ns, UTC]")
    frame["resolution_minutes"] = infer_resolution_minutes(frame["timestamp_utc"])
    frame["price_eur_mwh"] = frame["price_eur_mwh"].astype("float32")
    return frame[["timestamp_utc", "resolution_minutes", "price_eur_mwh"]]


def expected_local_day_rows(day: pd.Timestamp, timezone: str = LOCAL_TIMEZONE, resolution_minutes: int = 15) -> int:
    """Number of products in one local delivery day (92 / 96 / 100 quarter-hours around DST switches)."""
    start = pd.Timestamp(day.date()).tz_localize(timezone)
    end = (pd.Timestamp(day.date()) + pd.Timedelta(days=1)).tz_localize(timezone)
    return int((end - start) / pd.Timedelta(minutes=resolution_minutes))


# --- HTTP ------------------------------------------------------------------------------------------------------


def utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def iso_utc(ts: pd.Timestamp | None, *, fractional: bool = False) -> str | None:
    if ts is None or pd.isna(ts):
        return None
    stamp = pd.Timestamp(ts).tz_convert("UTC")
    if fractional:
        return stamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return stamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def _retry_after_seconds(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        try:
            when = parsedate_to_datetime(str(value))
        except (TypeError, ValueError):
            return None
        return max(0.0, (pd.Timestamp(when).tz_convert("UTC") - utc_now()).total_seconds())


def _server_date(headers: Any) -> pd.Timestamp | None:
    try:
        value = headers.get("Date") or headers.get("date")
    except AttributeError:
        return None
    if not value:
        return None
    try:
        return pd.Timestamp(parsedate_to_datetime(str(value))).tz_convert("UTC")
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class HttpPolicy:
    """Timeouts and retry/backoff for live polling (short: a slow poll must never block a decision)."""

    connect_timeout_s: float = 4.0
    read_timeout_s: float = 10.0
    max_retries: int = 2
    backoff_s: float = 1.0
    backoff_cap_s: float = 30.0


@dataclass(frozen=True)
class HttpResult:
    payload: Any
    status: int
    elapsed_s: float
    attempts: int
    received_at: pd.Timestamp
    server_date: pd.Timestamp | None = None


class JsonHttpClient:
    """GET + JSON with retries, exponential backoff and ``Retry-After``; inject ``session``/``sleep`` in tests."""

    def __init__(
        self,
        policy: HttpPolicy | None = None,
        *,
        session: Any = None,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], pd.Timestamp] = utc_now,
        user_agent: str = USER_AGENT,
    ) -> None:
        self.policy = policy or HttpPolicy()
        if session is None:
            session = requests.Session()
            session.headers.update({"User-Agent": user_agent, "Accept": "application/json"})
        self.session = session
        self.sleep = sleep
        self.now = now

    def _backoff(self, attempt: int) -> float:
        return float(min(self.policy.backoff_s * (2**attempt), self.policy.backoff_cap_s))

    def get_json(
        self, url: str, params: Mapping[str, Any] | None = None, *, budget_s: float | None = None
    ) -> HttpResult:
        """GET ``url``; ``budget_s`` caps timeouts and total backoff so the caller can keep a deadline."""
        started = time.monotonic()
        last_error: Exception | None = None
        retry_after: float | None = None
        attempts = 0
        for attempt in range(self.policy.max_retries + 1):
            remaining = None if budget_s is None else budget_s - (time.monotonic() - started)
            if remaining is not None and remaining <= 0.2:
                break
            connect = self.policy.connect_timeout_s
            read = self.policy.read_timeout_s
            if remaining is not None:
                connect, read = min(connect, remaining), min(read, remaining)
            attempts += 1
            wait = self._backoff(attempt)
            try:
                response = self.session.get(url, params=dict(params or {}), timeout=(connect, read))
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = exc
            else:
                status = int(response.status_code)
                headers = getattr(response, "headers", {}) or {}
                if status in RETRY_STATUS:
                    retry_after = _retry_after_seconds(headers.get("Retry-After"))
                    last_error = FeedError(f"HTTP {status}")
                    if retry_after is not None:
                        wait = min(retry_after, self.policy.backoff_cap_s)
                elif status >= 400:
                    body = str(getattr(response, "text", ""))[:200]
                    raise FeedError(f"HTTP {status} for {url}: {body}")
                else:
                    try:
                        payload = response.json()
                    except ValueError as exc:
                        last_error = FeedSchemaError(f"invalid JSON from {url}: {exc}")
                    else:
                        return HttpResult(
                            payload=payload,
                            status=status,
                            elapsed_s=time.monotonic() - started,
                            attempts=attempts,
                            received_at=self.now(),
                            server_date=_server_date(headers),
                        )
            if attempt >= self.policy.max_retries:
                break
            if budget_s is not None and (time.monotonic() - started) + wait > budget_s:
                break
            LOGGER.debug("retrying %s in %.1fs after %s", url, wait, last_error)
            self.sleep(wait)
        if isinstance(last_error, FeedError) and str(last_error) == "HTTP 429":
            raise RetryLater(f"rate limited by {url} (HTTP 429)", retry_after)
        if isinstance(last_error, FeedSchemaError):
            raise last_error
        raise FeedError(f"giving up on {url} after {attempts} attempt(s): {last_error}")


# --- feed state, statistics and updates -----------------------------------------------------------------------


def _quantiles(values: Sequence[float]) -> dict[str, float | int | None]:
    if not values:
        return {"n": 0, "min": None, "p50": None, "p90": None, "p99": None, "max": None, "mean": None}
    arr = np.asarray(values, dtype=float)
    return {
        "n": int(arr.size),
        "min": round(float(arr.min()), 2),
        "p50": round(float(np.quantile(arr, 0.5)), 2),
        "p90": round(float(np.quantile(arr, 0.9)), 2),
        "p99": round(float(np.quantile(arr, 0.99)), 2),
        "max": round(float(arr.max()), 2),
        "mean": round(float(arr.mean()), 2),
    }


@dataclass
class FeedStats:
    """Counters and latency samples for one feed (latency = first seen minus row completion, in seconds)."""

    polls: int = 0
    successes: int = 0
    errors: int = 0
    consecutive_errors: int = 0
    unchanged_polls: int = 0
    rows_new: int = 0
    rows_revised: int = 0
    schema_warnings: int = 0
    last_poll_utc: pd.Timestamp | None = None
    last_success_utc: pd.Timestamp | None = None
    last_error: str | None = None
    first_row_utc: pd.Timestamp | None = None
    latest_row_utc: pd.Timestamp | None = None
    latency_upper_s: deque[float] = field(default_factory=lambda: deque(maxlen=50_000))
    latency_mid_s: deque[float] = field(default_factory=lambda: deque(maxlen=50_000))
    http_elapsed_s: deque[float] = field(default_factory=lambda: deque(maxlen=5_000))
    clock_offset_s: deque[float] = field(default_factory=lambda: deque(maxlen=5_000))

    def summary(self) -> dict[str, Any]:
        return {
            "polls": self.polls,
            "successes": self.successes,
            "errors": self.errors,
            "consecutive_errors": self.consecutive_errors,
            "unchanged_polls": self.unchanged_polls,
            "rows_new": self.rows_new,
            "rows_revised": self.rows_revised,
            "schema_warnings": self.schema_warnings,
            "last_poll_utc": iso_utc(self.last_poll_utc),
            "last_success_utc": iso_utc(self.last_success_utc),
            "last_error": self.last_error,
            "first_row_utc": iso_utc(self.first_row_utc),
            "latest_row_utc": iso_utc(self.latest_row_utc),
            "latency_upper_s": _quantiles(list(self.latency_upper_s)),
            "latency_mid_s": _quantiles(list(self.latency_mid_s)),
            "http_elapsed_s": _quantiles(list(self.http_elapsed_s)),
            "clock_offset_s": _quantiles(list(self.clock_offset_s)),
        }


@dataclass(frozen=True)
class FeedUpdate:
    """Result of one poll of one feed: rows first seen now (``n_new``) or changed since last seen (``n_revised``)."""

    spec: FeedSpec
    received_at: pd.Timestamp
    rows: pd.DataFrame
    n_new: int = 0
    n_revised: int = 0
    latest_row_utc: pd.Timestamp | None = None
    error: str | None = None
    status: int | None = None
    elapsed_s: float = 0.0
    backfill: bool = False
    latency_s: tuple[float, ...] = ()
    #: per row of ``rows``: ``"new"`` or ``"revised"``, and the ``(dataset_id, upstream record as published)``
    kinds: tuple[str, ...] = ()
    raw: tuple[RawRecord | None, ...] = ()

    @property
    def ok(self) -> bool:
        return self.error is None

    def log_record(self) -> dict[str, Any]:
        return {
            "feed": self.spec.name,
            "dataset_id": self.spec.dataset_id,
            "received_at_utc": iso_utc(self.received_at, fractional=True),
            "status": self.status,
            "ok": self.ok,
            "error": self.error,
            "backfill": self.backfill,
            "n_rows": int(len(self.rows)),
            "n_new": self.n_new,
            "n_revised": self.n_revised,
            "latest_row_utc": iso_utc(self.latest_row_utc),
            "elapsed_s": round(self.elapsed_s, 3),
            "latency_s": [round(v, 1) for v in self.latency_s],
        }


class _FeedState:
    """What one feed has seen so far (bounded): values for revision detection, row times for gap detection."""

    def __init__(self, spec: FeedSpec, retention: pd.Timedelta) -> None:
        self.spec = spec
        self.retention = retention
        self.stats = FeedStats()
        self.values: dict[int, np.ndarray] = {}
        self.seen: set[int] = set()
        self.last_signature: str | None = None
        self.last_success_at: pd.Timestamp | None = None
        self.next_due: pd.Timestamp | None = None
        self.poll_every_s = spec.poll_every_s
        self.complete_days: set[str] = set()
        # Rows completed before we started watching cannot give a latency measurement.
        self.watch_start: pd.Timestamp | None = None

    def prune(self) -> None:
        latest = self.stats.latest_row_utc
        if latest is None:
            return
        cutoff = int((latest - self.retention).value)
        if len(self.values) > 4096 or len(self.seen) > 8192:
            self.values = {k: v for k, v in self.values.items() if k >= cutoff}
            self.seen = {k for k in self.seen if k >= cutoff}

    def gaps(self, since: pd.Timestamp | None = None) -> list[pd.Timestamp]:
        """Missing row times between the first (or ``since``) and the latest row seen."""
        if not self.seen or self.spec.table == "day_ahead":
            return []
        step = int(self.spec.grid_step.value)
        first = min(self.seen)
        if since is not None:
            first = max(first, int(pd.Timestamp(since).ceil(self.spec.grid_step).value))
        last = max(self.seen)
        if last <= first:
            return []
        expected = range(first, last + 1, step)
        return [pd.Timestamp(v, tz="UTC") for v in expected if v not in self.seen]


def _payload_signature(payload: Any) -> str:
    return hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


@runtime_checkable
class LiveSource(Protocol):
    """Where the live engine gets data from (real HTTP feeds or a replay of contract frames)."""

    @property
    def specs(self) -> Sequence[FeedSpec]: ...

    def next_due(self) -> pd.Timestamp | None: ...

    def poll(
        self, now: pd.Timestamp, *, deadline: pd.Timestamp | None = None, force: Collection[str] = ()
    ) -> list[FeedUpdate]: ...

    def backfill(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        *,
        deadline: pd.Timestamp | None = None,
        qh_history_start: pd.Timestamp | None = None,
    ) -> list[FeedUpdate]: ...

    def status(self, now: pd.Timestamp) -> dict[str, Any]: ...

    def set_poll_interval(self, name: str, seconds: float) -> None: ...


def aligned_after(t: pd.Timestamp, every_s: float, phase_s: float) -> pd.Timestamp:
    """First time strictly after ``t`` of the form ``k * every_s + phase_s`` (epoch seconds)."""
    every_ns = int(round(every_s * _NS_PER_S))
    phase_ns = int(round(phase_s * _NS_PER_S)) % every_ns
    k = (int(t.value) - phase_ns) // every_ns + 1
    return pd.Timestamp(k * every_ns + phase_ns, tz="UTC")


class _ScheduledFeeds:
    """Shared scheduling, diffing and statistics for live sources."""

    def __init__(
        self,
        specs: Sequence[FeedSpec],
        *,
        prefetch_s: float = 8.0,
        min_interval_s: float = 2.0,
        retention: pd.Timedelta = pd.Timedelta(hours=8),
        timezone: str = LOCAL_TIMEZONE,
    ) -> None:
        names = [s.name for s in specs]
        if len(set(names)) != len(names):
            raise ValueError(f"duplicate feed names: {names}")
        self._specs = tuple(specs)
        self.prefetch_s = prefetch_s
        self.min_interval_s = min_interval_s
        self.timezone = timezone
        self.state: dict[str, _FeedState] = {s.name: _FeedState(s, retention) for s in specs}

    @property
    def specs(self) -> Sequence[FeedSpec]:
        return self._specs

    def set_poll_interval(self, name: str, seconds: float) -> None:
        state = self.state[name]
        if seconds <= 0:
            raise ValueError("poll interval must be positive")
        if state.poll_every_s != seconds:
            state.poll_every_s = float(seconds)
            state.next_due = None

    def _phase(self, every_s: float) -> float:
        # Align sub-minute schedules so that one poll lands `prefetch_s` before each minute boundary; slower
        # schedules poll at half past the minute so they never compete with a decision for the network.
        if every_s <= 60 and abs(60 / every_s - round(60 / every_s)) < 1e-9:
            return (60.0 - self.prefetch_s) % every_s
        return 30.0

    def _schedule(self, state: _FeedState, now: pd.Timestamp, *, retry_after_s: float | None = None) -> None:
        if retry_after_s is not None:
            state.next_due = now + pd.Timedelta(seconds=max(retry_after_s, 1.0))
            return
        if state.spec.table == "day_ahead":
            state.next_due = self._next_day_ahead_due(state, now)
            return
        every = state.poll_every_s
        if state.stats.consecutive_errors:
            every = min(every * (2 ** min(state.stats.consecutive_errors, 4)), max(every, 300.0))
        state.next_due = aligned_after(now, every, self._phase(every))

    def _next_day_ahead_due(self, state: _FeedState, now: pd.Timestamp) -> pd.Timestamp:
        local = now.tz_convert(self.timezone)
        tomorrow = (local.normalize() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        if state.stats.consecutive_errors:
            regular = now + pd.Timedelta(seconds=min(state.poll_every_s, 600.0))
        elif tomorrow in state.complete_days:
            regular = now + pd.Timedelta(seconds=state.poll_every_s)
        else:
            # Day-ahead results are published around 12:45 local time: poll more often around then.
            publication = local.normalize() + pd.Timedelta(hours=12, minutes=50)
            if local < publication:
                regular = min(now + pd.Timedelta(seconds=state.poll_every_s), publication.tz_convert("UTC"))
            else:
                regular = now + pd.Timedelta(seconds=min(state.poll_every_s, 300.0))
        return pd.Timestamp(regular).tz_convert("UTC")

    def next_due(self) -> pd.Timestamp | None:
        """Earliest scheduled poll; a feed that was never polled is due immediately (the epoch)."""
        if not self.state:
            return None
        if any(s.next_due is None for s in self.state.values()):
            return pd.Timestamp(0, tz="UTC")
        return min(s.next_due for s in self.state.values() if s.next_due is not None)

    def _mark_complete_days(self, state: _FeedState, frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        local_days = frame["timestamp_utc"].dt.tz_convert(self.timezone).dt.strftime("%Y-%m-%d")
        for day, rows in frame.groupby(local_days):
            minutes = int(rows["resolution_minutes"].sum())
            if minutes >= 60 * 23:  # >= 23 h of products = complete (DST days have 23 or 25 hours)
                state.complete_days.add(str(day))

    def _diff(
        self,
        state: _FeedState,
        frame: pd.DataFrame,
        received_at: pd.Timestamp,
        *,
        backfill: bool,
    ) -> tuple[pd.DataFrame, int, int, list[float], list[str]]:
        """Rows first seen now or revised (with their kind); records latency of rows first seen by a regular poll."""
        spec = state.spec
        if frame.empty:
            return frame, 0, 0, [], []
        times = pd.DatetimeIndex(frame[spec.time_column])
        keys = times.asi8
        numeric = [c for c in spec.numeric_columns if c in frame.columns]
        if spec.table == "day_ahead":
            numeric = ["price_eur_mwh"]
        values = frame[numeric].to_numpy(dtype=float, na_value=np.nan) if numeric else np.zeros((len(frame), 0))
        keep = np.zeros(len(frame), dtype=bool)
        kinds: list[str] = []
        n_new = n_revised = 0
        latencies: list[float] = []
        previous_poll = state.last_success_at
        if state.watch_start is None:
            state.watch_start = received_at
        watch_start_ns = int(state.watch_start.value)
        completion_ns = int(spec.completion_offset.value)
        for i, key in enumerate(keys):
            row = values[i]
            old = state.values.get(int(key))
            if old is None:
                keep[i] = True
                kinds.append("new")
                n_new += 1
                state.seen.add(int(key))
                completed = int(key) + completion_ns
                if not backfill and previous_poll is not None and completed >= watch_start_ns:
                    upper = (received_at.value - completed) / _NS_PER_S
                    lower = (previous_poll.value - completed) / _NS_PER_S
                    latencies.append(upper)
                    state.stats.latency_upper_s.append(upper)
                    state.stats.latency_mid_s.append(upper if lower < 0 else 0.5 * (upper + lower))
            elif not np.array_equal(old, row, equal_nan=True):
                keep[i] = True
                kinds.append("revised")
                n_revised += 1
            state.values[int(key)] = row
        latest = pd.Timestamp(times.max())
        if state.stats.latest_row_utc is None or latest > state.stats.latest_row_utc:
            state.stats.latest_row_utc = latest
        earliest = pd.Timestamp(times.min())
        if state.stats.first_row_utc is None or earliest < state.stats.first_row_utc:
            state.stats.first_row_utc = earliest
        state.stats.rows_new += n_new
        state.stats.rows_revised += n_revised
        state.prune()
        return frame.loc[keep].reset_index(drop=True), n_new, n_revised, latencies, kinds

    def status(self, now: pd.Timestamp) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for name, state in self.state.items():
            summary = state.stats.summary()
            latest = state.stats.latest_row_utc
            if latest is not None and state.spec.table != "day_ahead":
                completed = latest + state.spec.completion_offset
                summary["staleness_s"] = round((now - completed).total_seconds(), 1)
                summary["live"] = bool(now - completed <= pd.Timedelta(hours=2))
            gaps = state.gaps()
            summary["gaps"] = {"count": len(gaps), "last": [iso_utc(g) for g in gaps[-10:]]}
            summary["dataset_id"] = state.spec.dataset_id
            summary["poll_every_s"] = state.poll_every_s
            summary["next_due_utc"] = iso_utc(state.next_due)
            out[name] = summary
        return out


class EliaLiveFeeds(_ScheduledFeeds):
    """Polls Elia near-real-time datasets (records API) and Energy-Charts day-ahead prices on per-feed schedules.

    Minute feeds are polled every ``poll_every_s`` seconds on a schedule aligned so that one poll lands
    ``prefetch_s`` seconds before each minute boundary: decisions never wait for the network. Every poll keeps a
    small cache (payload signature, values per row) to detect unchanged payloads, revisions and gaps.
    """

    def __init__(
        self,
        specs: Sequence[FeedSpec] = DEFAULT_FEEDS,
        *,
        client: JsonHttpClient | None = None,
        base_url: str = ELIA_BASE_URL,
        energy_charts_url: str = ENERGY_CHARTS_PRICE_URL,
        prefetch_s: float = 8.0,
        min_interval_s: float = 2.0,
        timezone: str = LOCAL_TIMEZONE,
        da_zone: str = "BE",
    ) -> None:
        super().__init__(specs, prefetch_s=prefetch_s, min_interval_s=min_interval_s, timezone=timezone)
        self.client = client or JsonHttpClient()
        self.base_url = base_url.rstrip("/")
        self.energy_charts_url = energy_charts_url
        self.da_zone = da_zone

    def records_url(self, dataset_id: str) -> str:
        return f"{self.base_url}/{dataset_id}/records"

    def export_url(self, dataset_id: str) -> str:
        return f"{self.base_url}/{dataset_id}/exports/json"

    # -- polling --

    def poll(
        self, now: pd.Timestamp, *, deadline: pd.Timestamp | None = None, force: Collection[str] = ()
    ) -> list[FeedUpdate]:
        """Poll every feed that is due at ``now`` (or listed in ``force``); never raises for upstream errors."""
        updates: list[FeedUpdate] = []
        for name, state in self.state.items():
            due = state.next_due is None or state.next_due <= now
            if not due and name not in force:
                continue
            if state.stats.last_poll_utc is not None:
                since = (now - state.stats.last_poll_utc).total_seconds()
                if not due and since < self.min_interval_s:
                    continue
            budget = None if deadline is None else (deadline - self.client.now()).total_seconds()
            if budget is not None and budget < MIN_POLL_BUDGET_S:
                break  # not enough time before the decision: the feed stays due and is polled right after it
            updates.append(self._poll_one(state, now, budget_s=budget))
        return updates

    def _request(
        self, state: _FeedState, now: pd.Timestamp, budget_s: float | None
    ) -> tuple[HttpResult, pd.DataFrame, dict[int, RawRecord]]:
        spec = state.spec
        if spec.source == "energy_charts":
            local = now.tz_convert(self.timezone)
            first = local.normalize() - pd.Timedelta(days=1 if state.stats.successes == 0 else 0)
            last = local.normalize() + pd.Timedelta(days=1)
            params = {"bzn": self.da_zone, "start": f"{first:%Y-%m-%d}", "end": f"{last:%Y-%m-%d}"}
            result = self.client.get_json(self.energy_charts_url, params, budget_s=budget_s)
            frame = normalize_energy_charts(result.payload)
            self._mark_complete_days(state, frame)
            return result, frame, energy_charts_raw_index(result.payload, spec.dataset_id)
        params = {"order_by": f"{spec.time_field} desc", "limit": str(spec.limit)}
        result = self.client.get_json(self.records_url(spec.dataset_id), params, budget_s=budget_s)
        records = records_from_payload(spec, result.payload)
        if spec.table == "minute" and quarterhour_mismatches(records):
            state.stats.schema_warnings += 1
        return result, normalize_records(spec, records), raw_index(spec, records)

    def _poll_one(self, state: _FeedState, now: pd.Timestamp, *, budget_s: float | None) -> FeedUpdate:
        spec = state.spec
        state.stats.polls += 1
        state.stats.last_poll_utc = now
        try:
            result, frame, raw_by_key = self._request(state, now, budget_s)
        except RetryLater as exc:
            return self._failed(state, now, str(exc), retry_after_s=exc.retry_after_s)
        except FeedError as exc:
            return self._failed(state, now, str(exc))
        except Exception as exc:  # noqa: BLE001 - a feed bug must not stop the loop
            return self._failed(state, now, f"{type(exc).__name__}: {exc}")
        stats = state.stats
        stats.successes += 1
        stats.consecutive_errors = 0
        stats.last_success_utc = result.received_at
        stats.http_elapsed_s.append(result.elapsed_s)
        if result.server_date is not None:
            stats.clock_offset_s.append((result.received_at - result.server_date).total_seconds())
        signature = _payload_signature(result.payload)
        if signature == state.last_signature:
            stats.unchanged_polls += 1
            state.last_success_at = result.received_at
            self._schedule(state, now)
            return FeedUpdate(
                spec=spec,
                received_at=result.received_at,
                rows=frame.iloc[0:0],
                latest_row_utc=stats.latest_row_utc,
                status=result.status,
                elapsed_s=result.elapsed_s,
            )
        state.last_signature = signature
        rows, n_new, n_revised, latencies, kinds = self._diff(state, frame, result.received_at, backfill=False)
        state.last_success_at = result.received_at
        self._schedule(state, now)
        return FeedUpdate(
            spec=spec,
            received_at=result.received_at,
            rows=rows,
            n_new=n_new,
            n_revised=n_revised,
            latest_row_utc=stats.latest_row_utc,
            status=result.status,
            elapsed_s=result.elapsed_s,
            latency_s=tuple(latencies),
            kinds=tuple(kinds),
            raw=_raw_for_rows(spec, rows, raw_by_key),
        )

    def _failed(
        self, state: _FeedState, now: pd.Timestamp, message: str, *, retry_after_s: float | None = None
    ) -> FeedUpdate:
        state.stats.errors += 1
        state.stats.consecutive_errors += 1
        state.stats.last_error = message[:300]
        LOGGER.warning("feed %s (%s) poll failed: %s", state.spec.name, state.spec.dataset_id, message)
        self._schedule(state, now, retry_after_s=retry_after_s)
        return FeedUpdate(
            spec=state.spec,
            received_at=now,
            rows=empty_contract_frame(state.spec) if state.spec.table != "day_ahead" else pd.DataFrame(),
            error=message[:300],
            latest_row_utc=state.stats.latest_row_utc,
        )

    # -- backfill --

    def fetch_export(
        self,
        spec: FeedSpec,
        start: pd.Timestamp,
        end: pd.Timestamp | None = None,
        *,
        budget_s: float | None = None,
    ) -> pd.DataFrame:
        """All rows of ``spec.dataset_id`` with ``start <= time < end`` via the JSON export endpoint."""
        return self.fetch_export_raw(spec, start, end, budget_s=budget_s)[0]

    def fetch_export_raw(
        self,
        spec: FeedSpec,
        start: pd.Timestamp,
        end: pd.Timestamp | None = None,
        *,
        budget_s: float | None = None,
    ) -> tuple[pd.DataFrame, dict[int, RawRecord]]:
        """Like :meth:`fetch_export`, plus the upstream records keyed by row time (for the raw archive)."""
        where = f'{spec.time_field} >= "{iso_utc(start)}"'
        if end is not None:
            where += f' and {spec.time_field} < "{iso_utc(end)}"'
        params = {"where": where, "order_by": spec.time_field}
        result = self.client.get_json(self.export_url(spec.dataset_id), params, budget_s=budget_s)
        records = records_from_payload(spec, result.payload)
        return normalize_records(spec, records), raw_index(spec, records)

    def fetch_day_ahead(
        self, first_day: pd.Timestamp, last_day: pd.Timestamp, *, budget_s: float | None = None
    ) -> pd.DataFrame:
        return self.fetch_day_ahead_raw(first_day, last_day, budget_s=budget_s)[0]

    def fetch_day_ahead_raw(
        self, first_day: pd.Timestamp, last_day: pd.Timestamp, *, budget_s: float | None = None
    ) -> tuple[pd.DataFrame, dict[int, RawRecord]]:
        params = {"bzn": self.da_zone, "start": f"{first_day:%Y-%m-%d}", "end": f"{last_day:%Y-%m-%d}"}
        result = self.client.get_json(self.energy_charts_url, params, budget_s=budget_s)
        return normalize_energy_charts(result.payload), energy_charts_raw_index(result.payload)

    def backfill(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        *,
        deadline: pd.Timestamp | None = None,
        qh_history_start: pd.Timestamp | None = None,
    ) -> list[FeedUpdate]:
        """Recent history for the rolling window: near-real-time content plus validated quarter-hour prices.

        Minute and forecast feeds are read from their near-real-time datasets for ``[start, end)``; the
        quarter-hour price feed is also read from its historical twin (``ods134``) from ``qh_history_start``.
        """
        updates: list[FeedUpdate] = []
        for state in self.state.values():
            spec = state.spec
            if deadline is not None and self.client.now() >= deadline:
                break
            received = self.client.now()
            raw_by_key: dict[int, RawRecord] = {}
            try:
                if spec.source == "energy_charts":
                    local_start = (qh_history_start or start).tz_convert(self.timezone).normalize()
                    local_end = end.tz_convert(self.timezone).normalize() + pd.Timedelta(days=1)
                    frame, raw_by_key = self.fetch_day_ahead_raw(local_start, local_end)
                    self._mark_complete_days(state, frame)
                else:
                    frames = []
                    historical = spec.historical()
                    first = start
                    if spec.table in QH_TABLES and qh_history_start is not None:
                        first = qh_history_start
                    if spec.table in QH_TABLES and historical is not None and qh_history_start is not None:
                        older, older_raw = self.fetch_export_raw(historical, qh_history_start, end)
                        frames.append(older)
                        raw_by_key.update(older_raw)
                    recent, recent_raw = self.fetch_export_raw(spec, first, end)
                    frames.append(recent)
                    raw_by_key.update(recent_raw)
                    frames = [f for f in frames if not f.empty]
                    frame = (
                        pd.concat(frames, ignore_index=True)
                        .drop_duplicates(subset=[spec.time_column], keep="last")
                        .sort_values(spec.time_column)
                        .reset_index(drop=True)
                        if frames
                        else empty_contract_frame(spec)
                    )
            except FeedError as exc:
                state.stats.last_error = f"backfill: {exc}"[:300]
                updates.append(
                    FeedUpdate(
                        spec=spec,
                        received_at=received,
                        rows=empty_contract_frame(spec),
                        error=str(exc)[:300],
                        backfill=True,
                    )
                )
                continue
            rows, n_new, n_revised, _, kinds = self._diff(state, frame, received, backfill=True)
            # The backfill counts as the reference poll: rows first seen later get a latency measurement, and
            # slow feeds (day-ahead) are not requested again right away.
            state.last_success_at = received
            if spec.source == "energy_charts":
                state.stats.successes += 1
            self._schedule(state, received)
            updates.append(
                FeedUpdate(
                    spec=spec,
                    received_at=received,
                    rows=rows,
                    n_new=n_new,
                    n_revised=n_revised,
                    latest_row_utc=state.stats.latest_row_utc,
                    backfill=True,
                    kinds=tuple(kinds),
                    raw=_raw_for_rows(spec, rows, raw_by_key),
                )
            )
        return updates


def _raw_for_rows(
    spec: FeedSpec, rows: pd.DataFrame, raw_by_key: Mapping[int, RawRecord]
) -> tuple[RawRecord | None, ...]:
    if rows.empty:
        return ()
    keys = pd.DatetimeIndex(rows[spec.time_column]).asi8
    return tuple(raw_by_key.get(int(k)) for k in keys)


def sanitize_message(text: str) -> str:
    """Strip local absolute paths from messages that end up in persisted files."""
    import os
    from pathlib import Path

    out = str(text)
    for prefix, replacement in ((str(Path.cwd()), "."), (str(Path.home()), "~"), (os.sep + "Users" + os.sep, "~/")):
        if prefix and prefix not in (".", os.sep):
            out = out.replace(prefix, replacement)
    return out


def finite_or_none(value: Any, digits: int = 4) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return round(number, digits)
