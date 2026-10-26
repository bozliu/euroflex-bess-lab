"""Test utilities: a fake Elia / Energy-Charts HTTP session and a synthetic Belgian market.

:class:`FakeEliaSession` stands in for ``requests.Session`` inside
:class:`~euroflex_bess_lab.realtime.live.feeds.JsonHttpClient`. It serves the Explore v2.1 ``/records`` and
``/exports/json`` endpoints and the Energy-Charts ``/price`` endpoint from contract-shaped frames, translated back to
the upstream field names, and only returns rows that are already "published" at ``clock.now()``. Together with a
:class:`~euroflex_bess_lab.realtime.live.clock.ManualClock` this exercises the complete live path (HTTP ->
normalization -> engine) without a network and without sleeping.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from email.utils import format_datetime
from typing import Any
from urllib.parse import urlsplit

import numpy as np
import pandas as pd

from ...public_data.normalize import cast_contract_types
from ..mechanisms import CurrentAlphaMechanism
from .feeds import DEFAULT_FEEDS, LOCAL_TIMEZONE, FeedSpec

_WHERE = re.compile(r'(\w+)\s*(>=|<)\s*"([^"]+)"')


class FakeResponse:
    def __init__(self, status_code: int, payload: Any, headers: Mapping[str, str] | None = None) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = dict(headers or {})
        self.text = json.dumps(payload) if not isinstance(payload, str) else payload

    def json(self) -> Any:
        if isinstance(self._payload, str):
            raise ValueError("not JSON")
        return self._payload

    def close(self) -> None:
        return None


def _iso(ts: pd.Timestamp) -> str:
    return pd.Timestamp(ts).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%S+00:00")


def _json_value(value: Any) -> Any:
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float | np.floating):
        number = float(value)
        return None if math.isnan(number) else number
    if isinstance(value, np.integer):
        return int(value)
    return value


class FakeEliaSession:
    """Serve contract frames as Elia / Energy-Charts API responses, as published at ``clock()``.

    ``tables`` maps a feed name (e.g. ``"imbalance_price_minute"``) or dataset id (``"ods161"``) to a contract frame
    holding that feed's columns. A row is published ``delays_s[feed]`` seconds after its completion time (defaults:
    20 s per minute row, 150 s per quarter-hour, 30 s per forecast); day-ahead prices on D-1 at 12:45 local time.
    ``failures`` maps a dataset id to HTTP status codes returned (in order) before serving normally.
    """

    def __init__(
        self,
        tables: Mapping[str, pd.DataFrame],
        *,
        clock: Callable[[], pd.Timestamp],
        specs: Sequence[FeedSpec] = DEFAULT_FEEDS,
        delays_s: Mapping[str, float] | None = None,
        failures: Mapping[str, Sequence[int]] | None = None,
        da_publication_local: tuple[int, int] = (12, 45),
        timezone: str = LOCAL_TIMEZONE,
    ) -> None:
        self.clock = clock
        self.timezone = timezone
        self.requests: list[dict[str, Any]] = []
        self.headers: dict[str, str] = {}
        self._failures = {k: list(v) for k, v in (failures or {}).items()}
        self._datasets: dict[str, tuple[FeedSpec, list[dict[str, Any]], np.ndarray, np.ndarray]] = {}
        delays = dict(delays_s or {})
        by_key = {s.name: s for s in specs} | {s.dataset_id: s for s in specs}
        for key, frame in tables.items():
            spec = by_key[key]
            default = 150.0 if spec.resolution_minutes == 15 else (30.0 if spec.resolution_minutes == 0 else 20.0)
            delay = float(delays.get(key, delays.get(spec.name, delays.get(spec.dataset_id, default))))
            self._datasets[spec.dataset_id] = self._prepare(spec, frame, delay, da_publication_local)

    def _prepare(
        self, spec: FeedSpec, frame: pd.DataFrame, delay_s: float, da_publication_local: tuple[int, int]
    ) -> tuple[FeedSpec, list[dict[str, Any]], np.ndarray, np.ndarray]:
        data = frame.copy()
        times = pd.DatetimeIndex(pd.to_datetime(data[spec.time_column], utc=True))
        if spec.table == "day_ahead":
            local_day = times.tz_convert(self.timezone).normalize()
            hour, minute = da_publication_local
            published = (local_day - pd.Timedelta(days=1) + pd.Timedelta(hours=hour, minutes=minute)).tz_convert("UTC")
            available = np.asarray(published.asi8, dtype=np.int64)
            records = [
                {"unix_seconds": int(ts.value // 1_000_000_000), "price": _json_value(price)}
                for ts, price in zip(times, data["price_eur_mwh"], strict=True)
            ]
            return spec, records, np.asarray(times.asi8, dtype=np.int64), available
        completion = times + spec.completion_offset + pd.Timedelta(seconds=delay_s)
        available = np.asarray(completion.asi8, dtype=np.int64)
        inverse = {contract: upstream for upstream, contract in spec.columns.items()}
        extra_inverse = {contract: upstream for upstream, contract in spec.extra_time_fields.items()}
        resolution = "PT15M" if spec.resolution_minutes == 15 else "PT1M"
        records = []
        for i, ts in enumerate(times):
            record: dict[str, Any] = {spec.time_field: _iso(ts), "resolutioncode": resolution}
            if spec.table == "minute":
                record["quarterhour"] = _iso(ts.floor("15min"))
            for contract, upstream in extra_inverse.items():
                if contract in data.columns:
                    record[upstream] = _iso(pd.Timestamp(data[contract].iloc[i]))
            for contract, upstream in inverse.items():
                if contract in data.columns:
                    record[upstream] = _json_value(data[contract].iloc[i])
                elif contract == "quality_status":
                    record[upstream] = "NotValidated"
                else:
                    record[upstream] = None
            records.append(record)
        return spec, records, np.asarray(times.asi8, dtype=np.int64), available

    def update_table(self, key: str, frame: pd.DataFrame, *, delay_s: float | None = None) -> None:
        """Replace what a dataset serves from now on (e.g. Elia revising rows that were already published)."""
        spec, _, _, _ = next(v for k, v in self._datasets.items() if key in (k, v[0].name))
        default = 150.0 if spec.resolution_minutes == 15 else (30.0 if spec.resolution_minutes == 0 else 20.0)
        self._datasets[spec.dataset_id] = self._prepare(spec, frame, default if delay_s is None else delay_s, (12, 45))

    # -- requests.Session API --

    def get(self, url: str, params: Mapping[str, Any] | None = None, timeout: Any = None, **_: Any) -> FakeResponse:
        now = self.clock()
        params = dict(params or {})
        self.requests.append({"url": url, "params": params, "at": now})
        headers = {"Date": format_datetime(now.to_pydatetime().replace(microsecond=0), usegmt=True)}
        path = urlsplit(url).path
        if path.endswith("/price"):
            return self._energy_charts(params, now, headers)
        match = re.search(r"/datasets/([^/]+)/(records|exports/json)$", path)
        if match is None:
            return FakeResponse(404, {"error_code": "NotFound", "message": url}, headers)
        dataset_id, endpoint = match.group(1), match.group(2)
        if self._failures.get(dataset_id):
            status = self._failures[dataset_id].pop(0)
            extra = {"Retry-After": "3"} if status == 429 else {}
            return FakeResponse(status, {"error_code": f"HTTP{status}"}, {**headers, **extra})
        if dataset_id not in self._datasets:
            return FakeResponse(200, {"total_count": 0, "results": []} if endpoint == "records" else [], headers)
        spec, records, times, available = self._datasets[dataset_id]
        visible = np.flatnonzero(available <= now.value)
        if endpoint == "records":
            limit = int(params.get("limit", 10))
            order = visible[np.argsort(-times[visible], kind="stable")][:limit]
            return FakeResponse(
                200, {"total_count": int(len(visible)), "results": [records[i] for i in order]}, headers
            )
        lo, hi = -np.inf, np.inf
        for _, op, value in _WHERE.findall(str(params.get("where", ""))):
            stamp = float(pd.Timestamp(value).tz_convert("UTC").value)
            if op == ">=":
                lo = stamp
            else:
                hi = stamp
        chosen = [i for i in visible[np.argsort(times[visible], kind="stable")] if lo <= times[i] < hi]
        return FakeResponse(200, [records[i] for i in chosen], headers)

    def _energy_charts(self, params: Mapping[str, Any], now: pd.Timestamp, headers: dict[str, str]) -> FakeResponse:
        entry = next((v for v in self._datasets.values() if v[0].table == "day_ahead"), None)
        if self._failures.get("energy_charts"):
            status = self._failures["energy_charts"].pop(0)
            return FakeResponse(status, "Too Many Requests", {**headers, "Retry-After": "5"})
        if entry is None:
            return FakeResponse(200, {"unix_seconds": [], "price": [], "unit": "EUR / MWh"}, headers)
        _, records, times, available = entry
        first = pd.Timestamp(str(params["start"])).tz_localize(self.timezone).tz_convert("UTC")
        last = (pd.Timestamp(str(params["end"])) + pd.Timedelta(days=1)).tz_localize(self.timezone).tz_convert("UTC")
        chosen = [i for i in range(len(records)) if available[i] <= now.value and first.value <= times[i] < last.value]
        payload = {
            "license_info": "CC BY 4.0 (creativecommons.org/licenses/by/4.0) from Bundesnetzagentur | SMARD.de",
            "unix_seconds": [records[i]["unix_seconds"] for i in chosen],
            "price": [records[i]["price"] for i in chosen],
            "unit": "EUR / MWh",
            "deprecated": False,
        }
        return FakeResponse(200, payload, headers)

    def post(self, *args: Any, **kwargs: Any) -> FakeResponse:  # pragma: no cover - feeds never POST
        raise AssertionError("live feeds must never POST")

    def close(self) -> None:
        return None


def synthetic_market_frames(
    start: str | pd.Timestamp = "2026-03-02T00:00:00Z",
    hours: int = 6,
    *,
    seed: int = 3,
) -> dict[str, pd.DataFrame]:
    """Synthetic contract frames (float32-exact) for one Belgian market period.

    Returns ``minute`` (ODS133-like cumulative price/SI plus instantaneous SI and aFRR price), ``qh`` (ODS134-like,
    priced with :class:`~euroflex_bess_lab.realtime.CurrentAlphaMechanism`), ``nf_qh`` (a new-formula price) and
    ``day_ahead`` (hourly), all cast with the data-contract dtypes.
    """
    start_ts = pd.Timestamp(start)
    start_ts = start_ts.tz_localize("UTC") if start_ts.tzinfo is None else start_ts.tz_convert("UTC")
    rng = np.random.default_rng(seed)
    n_min = hours * 60
    minutes = pd.date_range(start_ts, periods=n_min, freq="1min")
    qh_times = pd.date_range(start_ts, periods=n_min // 15, freq="15min")
    si_qh = rng.normal(0.0, 200.0, size=len(qh_times))
    mip = 80.0 + np.clip(-si_qh, 0, None) * 0.4
    mdp = 40.0 - np.clip(si_qh, 0, None) * 0.3
    alpha = np.where(np.abs(si_qh) > 150, 10.0, 0.0)
    price = CurrentAlphaMechanism.price_given_si(si_qh, mip=mip, mdp=mdp, alpha=alpha)
    qh = pd.DataFrame(
        {
            "timestamp_utc": qh_times,
            "si_mw": si_qh,
            "ace_mw": si_qh * 0.1,
            "alpha_eur_mwh": alpha,
            "alpha_prime_eur_mwh": 0.0,
            "mip_eur_mwh": mip,
            "mdp_eur_mwh": mdp,
            "imbalance_price_eur_mwh": price,
            "quality_status": "NotValidated",
        }
    )
    progress = (np.arange(n_min) % 15 + 1) / 15.0
    q = np.arange(n_min) // 15
    noise = rng.normal(0.0, 30.0, size=n_min) * (1 - progress)
    minute = pd.DataFrame(
        {
            "timestamp_utc": minutes,
            "si_cum_mw": si_qh[q] + 3 * noise,
            "ace_cum_mw": si_qh[q] * 0.1,
            "alpha_eur_mwh": alpha[q],
            "alpha_prime_eur_mwh": 0.0,
            "mip_eur_mwh": mip[q],
            "mdp_eur_mwh": mdp[q],
            "price_cum_eur_mwh": price[q] + noise,
            "si_mw": si_qh[q] + rng.normal(0.0, 50.0, size=n_min),
            "afrr_up_mw": np.clip(-si_qh[q], 0, None) * 0.5,
            "afrr_down_mw": np.clip(si_qh[q], 0, None) * 0.5,
            "afrr_price_up_eur_mwh": mip[q] - 5.0,
            "afrr_price_down_eur_mwh": mdp[q] + 5.0,
        }
    )
    nf_price = np.where(si_qh < 0, mip - 3.0, mdp + 3.0)
    nf_qh = pd.DataFrame(
        {
            "timestamp_utc": qh_times,
            "si_mw": si_qh,
            "imbalance_price_eur_mwh": nf_price,
            "main_up_eur_mwh": mip - 3.0,
            "main_down_eur_mwh": mdp + 3.0,
            "quality_status": "NotValidated",
        }
    )
    hours_da = hours + 48
    da_start = start_ts.floor("1D") - pd.Timedelta(days=1)
    day_ahead = pd.DataFrame(
        {
            "timestamp_utc": pd.date_range(da_start, periods=hours_da, freq="1h"),
            "resolution_minutes": 60,
            "price_eur_mwh": (60.0 + 20 * np.sin(np.arange(hours_da) / 24 * 2 * np.pi)).astype(np.float32),
        }
    )
    return {
        "minute": cast_contract_types(minute),
        "qh": cast_contract_types(qh),
        "nf_qh": cast_contract_types(nf_qh),
        "day_ahead": day_ahead,
    }


#: Which synthetic minute columns each real minute feed publishes (for :class:`FakeEliaSession`).
MINUTE_FEED_COLUMNS: dict[str, tuple[str, ...]] = {
    "imbalance_price_minute": (
        "si_cum_mw",
        "ace_cum_mw",
        "alpha_eur_mwh",
        "alpha_prime_eur_mwh",
        "mip_eur_mwh",
        "mdp_eur_mwh",
        "price_cum_eur_mwh",
    ),
    "system_imbalance_minute": ("si_mw", "afrr_up_mw", "afrr_down_mw"),
    "price_components_minute": ("afrr_price_up_eur_mwh", "afrr_price_down_eur_mwh"),
}


def split_minute_frame(minute: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Split a synthetic minute frame into the three near-real-time minute feeds."""
    return {
        feed: minute[["timestamp_utc", *[c for c in columns if c in minute.columns]]].copy()
        for feed, columns in MINUTE_FEED_COLUMNS.items()
    }
