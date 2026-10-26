"""NESO data portal (CKAN): Enduring Auction Capability (EAC) day-ahead auction results summaries.

The ``eac-auction-results`` package holds one live "Results Summary" resource, a "Daily Results Summary" for the
latest auction and yearly archives (NESO moves data older than the current financial year into archives every
April). Resources are discovered with ``package_show`` and downloaded as CSV through ``datastore/dump``.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from ..http import FetchError, FetchRecord, HttpClient, cached_download, validate_json_file
from .sources import NESO_DATASTORE_DUMP_URL, NESO_EAC_PACKAGE, NESO_PACKAGE_SHOW_URL
from .timeutils import efa_date_block

EAC_SUMMARY_COLUMNS = (
    "auctionID",
    "auctionProduct",
    "serviceType",
    "deliveryStart",
    "deliveryEnd",
    "clearedVolume",
    "clearingPrice",
)
RESPONSE_PRODUCTS: dict[str, tuple[str, str]] = {
    "DCL": ("DC", "low"),
    "DCH": ("DC", "high"),
    "DML": ("DM", "low"),
    "DMH": ("DM", "high"),
    "DRL": ("DR", "low"),
    "DRH": ("DR", "high"),
}
RESERVE_PRODUCTS: dict[str, tuple[str, str]] = {
    "PQR": ("QR", "up"),
    "NQR": ("QR", "down"),
    "PSR": ("SR", "up"),
    "NSR": ("SR", "down"),
    "PBR": ("BR", "up"),
    "NBR": ("BR", "down"),
}


@dataclass(frozen=True)
class EacResource:
    resource_id: str
    name: str
    path: Path
    record: FetchRecord
    archived: bool


def validate_csv_with_columns(columns: Sequence[str]) -> Any:
    def _validate(path: Path) -> None:
        with path.open("r", encoding="utf-8-sig", errors="replace") as handle:
            header = handle.readline().strip().split(",")
        missing = [c for c in columns if c not in header]
        if missing:
            raise FetchError(f"{path.name} is not an EAC results summary CSV (missing columns {missing})")

    return _validate


def fetch_eac_package(
    client: HttpClient, raw_dir: Path, *, now: pd.Timestamp, refresh: bool = False, offline: bool = False
) -> tuple[Path, FetchRecord]:
    dest = raw_dir / "gb" / "neso" / "eac" / "package_show_eac-auction-results.json"
    record = cached_download(
        client,
        NESO_PACKAGE_SHOW_URL,
        dest,
        params={"id": NESO_EAC_PACKAGE},
        validate=validate_json_file,
        refresh=refresh,
        period_end_utc=now,
        offline=offline,
    )
    return dest, record


def summary_resources(package: dict[str, Any]) -> list[dict[str, Any]]:
    """Results-summary resources of the EAC package (live, daily and archives), oldest archive first."""

    result = package.get("result", package)
    resources = [r for r in result.get("resources", []) if "results summary" in str(r.get("name", "")).lower()]
    return sorted(resources, key=lambda r: (0 if "archive" in str(r.get("name", "")).lower() else 1, str(r["name"])))


def fetch_eac_summaries(
    client: HttpClient, raw_dir: Path, *, now: pd.Timestamp, refresh: bool = False, offline: bool = False
) -> tuple[list[EacResource], FetchRecord]:
    package_path, package_record = fetch_eac_package(client, raw_dir, now=now, refresh=refresh, offline=offline)
    package = json.loads(package_path.read_text(encoding="utf-8"))
    out: list[EacResource] = []
    for resource in summary_resources(package):
        resource_id = str(resource["id"])
        archived = "archive" in str(resource.get("name", "")).lower()
        dest = raw_dir / "gb" / "neso" / "eac" / f"results_summary_{resource_id}.csv"
        record = cached_download(
            client,
            NESO_DATASTORE_DUMP_URL.format(resource_id=resource_id),
            dest,
            validate=validate_csv_with_columns(EAC_SUMMARY_COLUMNS),
            refresh=refresh,
            # Archives are closed; the live and daily summaries change every day.
            period_end_utc=None if archived else now,
            offline=offline,
            allow_shrink=not archived,
        )
        out.append(EacResource(resource_id, str(resource.get("name", "")), dest, record, archived))
    return out, package_record


def read_eac_csv(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype={"auctionProduct": str, "serviceType": str})
    missing = [c for c in EAC_SUMMARY_COLUMNS if c not in frame.columns]
    if missing:
        raise ValueError(f"{path.name}: missing EAC columns {missing}")
    return frame


def normalize_eac(frames: Sequence[pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Split EAC result summaries into response (DC/DM/DR per EFA block) and reserve (per SP) tables.

    ``deliveryStart``/``deliveryEnd`` are UTC (NESO dataset notes). Duplicates across live/daily/archive resources
    are dropped on ``(product, delivery_start_utc)`` keeping the highest ``auctionID``.
    """

    if not frames:
        raise ValueError("no EAC result summary frames")
    raw = pd.concat(frames, ignore_index=True)
    raw = raw.dropna(subset=["auctionProduct", "deliveryStart", "deliveryEnd"])
    frame = pd.DataFrame(
        {
            "delivery_start_utc": pd.to_datetime(raw["deliveryStart"], utc=True),
            "delivery_end_utc": pd.to_datetime(raw["deliveryEnd"], utc=True),
            "product": raw["auctionProduct"].astype(str).str.strip().str.upper(),
            "service_type": raw["serviceType"].astype(str).str.strip(),
            "clearing_price_gbp_per_mw_h": pd.to_numeric(raw["clearingPrice"], errors="coerce"),
            "cleared_volume_mw": pd.to_numeric(raw["clearedVolume"], errors="coerce"),
            "auction_id": pd.to_numeric(raw["auctionID"], errors="coerce").fillna(-1).astype("int64"),
        }
    )
    before = len(frame)
    frame = frame.sort_values(["product", "delivery_start_utc", "auction_id"])
    frame = frame.drop_duplicates(["product", "delivery_start_utc"], keep="last")
    duplicates = before - len(frame)
    unknown = sorted(set(frame["product"]) - set(RESPONSE_PRODUCTS) - set(RESERVE_PRODUCTS))

    response = frame[frame["product"].isin(list(RESPONSE_PRODUCTS))].copy()
    response["service"] = response["product"].map(lambda p: RESPONSE_PRODUCTS[p][0])
    response["direction"] = response["product"].map(lambda p: RESPONSE_PRODUCTS[p][1])
    efa_dates, efa_blocks = efa_date_block(response["delivery_start_utc"])
    response["efa_date"] = efa_dates
    response["efa_block"] = efa_blocks
    response["duration_hours"] = (
        (response["delivery_end_utc"] - response["delivery_start_utc"]).dt.total_seconds() / 3600.0
    ).astype("float32")
    response = response[
        [
            "delivery_start_utc",
            "delivery_end_utc",
            "efa_date",
            "efa_block",
            "duration_hours",
            "product",
            "service",
            "direction",
            "clearing_price_gbp_per_mw_h",
            "cleared_volume_mw",
            "auction_id",
        ]
    ].sort_values(["delivery_start_utc", "product"], kind="stable")

    reserve = frame[frame["product"].isin(list(RESERVE_PRODUCTS))].copy()
    reserve["service"] = reserve["product"].map(lambda p: RESERVE_PRODUCTS[p][0])
    reserve["direction"] = reserve["product"].map(lambda p: RESERVE_PRODUCTS[p][1])
    reserve = reserve[
        [
            "delivery_start_utc",
            "delivery_end_utc",
            "product",
            "service",
            "direction",
            "clearing_price_gbp_per_mw_h",
            "cleared_volume_mw",
            "auction_id",
        ]
    ].sort_values(["delivery_start_utc", "product"], kind="stable")
    for table in (response, reserve):
        table["clearing_price_gbp_per_mw_h"] = table["clearing_price_gbp_per_mw_h"].astype("float32")
        table["cleared_volume_mw"] = table["cleared_volume_mw"].astype("float32")
    stats: dict[str, Any] = {
        "raw_rows": int(before),
        "duplicates_dropped": int(duplicates),
        "unknown_products": unknown,
        "response_rows": int(len(response)),
        "reserve_rows": int(len(reserve)),
    }
    return response.reset_index(drop=True), reserve.reset_index(drop=True), stats


def response_block_completeness(response: pd.DataFrame) -> dict[str, Any]:
    """Count EFA days with all 6 blocks x 6 products present."""

    if response.empty:
        return {"efa_days": 0, "complete_efa_days": 0, "incomplete_efa_days": []}
    counts = response.groupby("efa_date")["product"].count()
    incomplete = counts[counts != 36]
    return {
        "efa_days": int(len(counts)),
        "complete_efa_days": int((counts == 36).sum()),
        "incomplete_efa_days": [str(d) for d in incomplete.index[:50]],
        "negative_price_rows": int((response["clearing_price_gbp_per_mw_h"] < 0).sum()),
        "duration_hours_counts": {
            str(k): int(v) for k, v in response["duration_hours"].round(2).value_counts().sort_index().items()
        },
    }
