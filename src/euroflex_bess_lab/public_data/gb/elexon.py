"""Elexon BMRS Insights Solution API: system prices (DISEBSP), Market Index Data (MID) and system frequency (FREQ).

Raw payloads are cached under ``<raw>/gb/elexon/...`` with ``.source.json`` provenance records (see
:mod:`euroflex_bess_lab.public_data.http`). No API key is needed for these endpoints.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ..http import FetchRecord, HttpClient, cached_download, validate_json_file
from .sources import ELEXON_FREQUENCY_STREAM_URL, ELEXON_MID_STREAM_URL, ELEXON_SYSTEM_PRICES_URL, MID_PROVIDERS
from .timeutils import SP_MINUTES, settlement_date_period, uk_midnight_utc


@dataclass(frozen=True)
class RawFile:
    """One cached upstream payload."""

    key: str
    path: Path
    record: FetchRecord


def _iso_minute(ts: pd.Timestamp) -> str:
    return ts.tz_convert("UTC").strftime("%Y-%m-%dT%H:%MZ")


# --------------------------------------------------------------------------------------------- system prices


def fetch_system_prices_day(
    client: HttpClient, raw_dir: Path, settlement_date: str, *, refresh: bool = False, offline: bool = False
) -> RawFile:
    """System prices for one settlement day (``/balancing/settlement/system-prices/{date}``)."""

    day = pd.Timestamp(settlement_date)
    dest = raw_dir / "gb" / "elexon" / "system_prices" / f"{day:%Y}" / f"system_prices_{day:%Y-%m-%d}.json"
    url = ELEXON_SYSTEM_PRICES_URL.format(settlement_date=f"{day:%Y-%m-%d}")
    period_end = uk_midnight_utc(day + pd.Timedelta(days=1))
    record = cached_download(
        client,
        url,
        dest,
        params={"format": "json"},
        validate=validate_json_file,
        refresh=refresh,
        period_end_utc=period_end,
        settle=pd.Timedelta(days=3),
        offline=offline,
    )
    return RawFile(f"{day:%Y-%m-%d}", dest, record)


SYSTEM_PRICE_FIELDS: dict[str, str] = {
    "systemSellPrice": "system_sell_price_gbp_mwh",
    "systemBuyPrice": "system_buy_price_gbp_mwh",
    "netImbalanceVolume": "niv_mwh",
    "reserveScarcityPrice": "reserve_scarcity_price_gbp_mwh",
    "replacementPrice": "replacement_price_gbp_mwh",
    "replacementPriceReferenceVolume": "replacement_price_reference_volume_mwh",
    "sellPriceAdjustment": "sell_price_adjustment_gbp_mwh",
    "buyPriceAdjustment": "buy_price_adjustment_gbp_mwh",
    "totalAcceptedOfferVolume": "accepted_offer_volume_mwh",
    "totalAcceptedBidVolume": "accepted_bid_volume_mwh",
    "totalAdjustmentSellVolume": "adjustment_sell_volume_mwh",
    "totalAdjustmentBuyVolume": "adjustment_buy_volume_mwh",
    "totalSystemTaggedAcceptedOfferVolume": "system_tagged_offer_volume_mwh",
    "totalSystemTaggedAcceptedBidVolume": "system_tagged_bid_volume_mwh",
}


def _payload_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        rows = payload.get("data", [])
    else:
        rows = payload
    if not isinstance(rows, list):
        raise ValueError("Elexon payload must be a list of rows or an object with a `data` list")
    return [r for r in rows if isinstance(r, dict)]


def parse_system_prices(payloads: Iterable[Any]) -> pd.DataFrame:
    """Normalize DISEBSP payloads to one row per settlement period (UTC start ``timestamp_utc``)."""

    rows: list[dict[str, Any]] = []
    for payload in payloads:
        rows.extend(_payload_rows(payload))
    columns = [
        "timestamp_utc",
        "settlement_date",
        "settlement_period",
        "system_price_gbp_mwh",
        *SYSTEM_PRICE_FIELDS.values(),
    ]
    if not rows:
        return pd.DataFrame({c: pd.Series(dtype="float32") for c in columns})
    raw = pd.DataFrame(rows)
    frame = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(raw["startTime"], utc=True),
            "settlement_date": raw["settlementDate"].astype(str),
            "settlement_period": pd.to_numeric(raw["settlementPeriod"]).astype("int16"),
        }
    )
    for source, target in SYSTEM_PRICE_FIELDS.items():
        values = raw[source] if source in raw.columns else pd.Series(np.nan, index=raw.index)
        frame[target] = pd.to_numeric(values, errors="coerce").astype("float64")
    # Single imbalance price since P305 (5 Nov 2015): SSP == SBP. Keep both and expose the single price.
    frame["system_price_gbp_mwh"] = frame["system_sell_price_gbp_mwh"]
    frame["price_derivation_code"] = raw.get("priceDerivationCode", pd.Series(None, index=raw.index)).astype("string")
    frame["bsad_defaulted"] = raw.get("bsadDefaulted", pd.Series(False, index=raw.index)).fillna(False).astype(bool)
    frame["published_utc"] = pd.to_datetime(raw["createdDateTime"], utc=True)
    frame = frame.sort_values(["timestamp_utc", "published_utc"]).drop_duplicates("timestamp_utc", keep="last")
    end = frame["timestamp_utc"] + pd.Timedelta(minutes=SP_MINUTES)
    frame["publication_lag_minutes"] = ((frame["published_utc"] - end).dt.total_seconds() / 60.0).astype("float32")
    for column in [*SYSTEM_PRICE_FIELDS.values(), "system_price_gbp_mwh"]:
        frame[column] = frame[column].astype("float32")
    ordered = [
        "timestamp_utc",
        "settlement_date",
        "settlement_period",
        "system_price_gbp_mwh",
        "niv_mwh",
        "system_sell_price_gbp_mwh",
        "system_buy_price_gbp_mwh",
        "price_derivation_code",
        "reserve_scarcity_price_gbp_mwh",
        "replacement_price_gbp_mwh",
        "replacement_price_reference_volume_mwh",
        "sell_price_adjustment_gbp_mwh",
        "buy_price_adjustment_gbp_mwh",
        "bsad_defaulted",
        "accepted_offer_volume_mwh",
        "accepted_bid_volume_mwh",
        "adjustment_sell_volume_mwh",
        "adjustment_buy_volume_mwh",
        "system_tagged_offer_volume_mwh",
        "system_tagged_bid_volume_mwh",
        "published_utc",
        "publication_lag_minutes",
    ]
    return frame[ordered].reset_index(drop=True)


# ------------------------------------------------------------------------------------------------------- MID


def month_starts_utc(start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    """First instants (UTC midnight) of every calendar month overlapping ``[start, end)``."""

    first = pd.Timestamp(year=start.year, month=start.month, day=1, tz="UTC")
    months = pd.date_range(first, end, freq="MS", tz="UTC", inclusive="left")
    return list(months)


def _fetch_stream_month(
    client: HttpClient,
    raw_dir: Path,
    url: str,
    folder: str,
    stem: str,
    month: pd.Timestamp,
    *,
    extra_params: dict[str, str] | None = None,
    refresh: bool,
    offline: bool,
) -> RawFile:
    nxt = month + pd.offsets.MonthBegin(1)
    params = {"from": _iso_minute(month), "to": _iso_minute(nxt), **(extra_params or {})}
    dest = raw_dir / "gb" / "elexon" / folder / f"{stem}_{month:%Y-%m}.json"
    record = cached_download(
        client,
        url,
        dest,
        params=params,
        validate=validate_json_file,
        refresh=refresh,
        period_end_utc=nxt,
        settle=pd.Timedelta(days=2),
        offline=offline,
    )
    return RawFile(f"{month:%Y-%m}", dest, record)


def fetch_mid_month(
    client: HttpClient, raw_dir: Path, month: pd.Timestamp, *, refresh: bool = False, offline: bool = False
) -> RawFile:
    """Market Index Data (both providers) for SPs starting within one calendar month (UTC)."""

    return _fetch_stream_month(
        client, raw_dir, ELEXON_MID_STREAM_URL, "mid", "mid", month, refresh=refresh, offline=offline
    )


def parse_market_index(payloads: Iterable[Any]) -> pd.DataFrame:
    """One row per SP: provider prices/volumes and the volume-weighted market price across providers.

    A provider whose traded volume is below its Individual Liquidity Threshold reports price 0 and volume 0; such
    rows carry no price information and are treated as missing (``mid_price_gbp_mwh`` is NaN when no provider
    traded).
    """

    rows: list[dict[str, Any]] = []
    for payload in payloads:
        rows.extend(_payload_rows(payload))
    if not rows:
        return pd.DataFrame(
            {
                "timestamp_utc": pd.Series(dtype="datetime64[ns, UTC]"),
                "mid_price_gbp_mwh": pd.Series(dtype="float32"),
            }
        )
    raw = pd.DataFrame(rows)
    raw["timestamp_utc"] = pd.to_datetime(raw["startTime"], utc=True)
    raw["price"] = pd.to_numeric(raw["price"], errors="coerce")
    raw["volume"] = pd.to_numeric(raw["volume"], errors="coerce").fillna(0.0)
    raw = raw.drop_duplicates(["timestamp_utc", "dataProvider"], keep="last")
    frame = pd.DataFrame({"timestamp_utc": sorted(raw["timestamp_utc"].unique())})
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True)
    dates, periods = settlement_date_period(frame["timestamp_utc"])
    frame["settlement_date"] = dates
    frame["settlement_period"] = periods
    numerator = np.zeros(len(frame))
    denominator = np.zeros(len(frame))
    for provider in MID_PROVIDERS:
        part = raw[raw["dataProvider"] == provider].set_index("timestamp_utc")
        price = part["price"].reindex(frame["timestamp_utc"]).to_numpy(dtype=float)
        volume = part["volume"].reindex(frame["timestamp_utc"]).fillna(0.0).to_numpy(dtype=float)
        short = provider.removesuffix("MIDP").lower()
        frame[f"{short}_price_gbp_mwh"] = np.where(volume > 0, price, np.nan).astype("float32")
        frame[f"{short}_volume_mwh"] = volume.astype("float32")
        traded = volume > 0
        numerator[traded] += price[traded] * volume[traded]
        denominator[traded] += volume[traded]
    with np.errstate(invalid="ignore", divide="ignore"):
        market = np.where(denominator > 0, numerator / np.where(denominator > 0, denominator, 1.0), np.nan)
    frame["mid_price_gbp_mwh"] = market.astype("float32")
    frame["mid_volume_mwh"] = denominator.astype("float32")
    ordered = [
        "timestamp_utc",
        "settlement_date",
        "settlement_period",
        "mid_price_gbp_mwh",
        "mid_volume_mwh",
        "apx_price_gbp_mwh",
        "apx_volume_mwh",
        "n2ex_price_gbp_mwh",
        "n2ex_volume_mwh",
    ]
    return frame[ordered]


# ------------------------------------------------------------------------------------------------- frequency


def fetch_frequency_month(
    client: HttpClient, raw_dir: Path, month: pd.Timestamp, *, refresh: bool = False, offline: bool = False
) -> RawFile:
    """15-second system frequency (FREQ) for one calendar month (UTC)."""

    return _fetch_stream_month(
        client, raw_dir, ELEXON_FREQUENCY_STREAM_URL, "frequency", "frequency", month, refresh=refresh, offline=offline
    )


FREQUENCY_VALID_RANGE_HZ = (48.0, 52.0)


def parse_frequency_with_stats(payloads: Iterable[Any]) -> tuple[pd.DataFrame, dict[str, int]]:
    """Like :func:`parse_frequency` and also return how many samples were dropped as invalid.

    Samples outside 48-52 Hz (e.g. zeros from telemetry gaps) cannot be real GB system frequency and are dropped.
    """

    rows: list[dict[str, Any]] = []
    for payload in payloads:
        rows.extend(_payload_rows(payload))
    if not rows:
        empty = pd.DataFrame(
            {"timestamp_utc": pd.Series(dtype="datetime64[ns, UTC]"), "frequency_dev_mhz": pd.Series(dtype="int16")}
        )
        return empty, {"raw_samples": 0, "invalid_samples_dropped": 0, "duplicates_dropped": 0}
    raw = pd.DataFrame(rows)
    frame = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(raw["measurementTime"], utc=True),
            "frequency_hz": pd.to_numeric(raw["frequency"], errors="coerce"),
        }
    )
    lo, hi = FREQUENCY_VALID_RANGE_HZ
    valid = frame["frequency_hz"].between(lo, hi)
    invalid = int((~valid).sum())
    frame = frame[valid]
    before = len(frame)
    frame = frame.drop_duplicates("timestamp_utc", keep="last").sort_values("timestamp_utc")
    deviation = np.rint((frame["frequency_hz"].to_numpy(dtype=float) - 50.0) * 1000.0)
    frame["frequency_dev_mhz"] = deviation.astype("int16")
    stats = {
        "raw_samples": int(len(raw)),
        "invalid_samples_dropped": invalid,
        "duplicates_dropped": before - len(frame),
    }
    return frame[["timestamp_utc", "frequency_dev_mhz"]].reset_index(drop=True), stats


def parse_frequency(payloads: Iterable[Any]) -> pd.DataFrame:
    """``timestamp_utc`` and ``frequency_dev_mhz`` (int16, ``frequency_hz = 50 + frequency_dev_mhz / 1000``)."""

    return parse_frequency_with_stats(payloads)[0]


def read_payloads(files: Sequence[RawFile]) -> list[Any]:
    return [json.loads(f.path.read_text(encoding="utf-8")) for f in files]
