"""SVK Data Service (Svenska kraftnät, CKAN datastore) client.

Each resource is fetched as one CSV dump per bidding zone (``/datastore/dump/<resource>?format=csv&filters=...``),
cached under ``data/raw/nordic/svk/<key>/<key>_<zone>.csv``. The dumps hold the full history, so a cached dump is
only re-downloaded when it was taken before the snapshot boundary settled (see ``cached_download``) or with
``refresh=True``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ..http import FetchError, FetchRecord, HttpClient, cached_download
from .sources import SVK_DUMP, SVK_PACKAGE_API, SVK_RESOURCES, SvkResource

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SvkDump:
    key: str
    zone: str
    path: Path
    record: FetchRecord


def resolve_resource_id(client: HttpClient, resource: SvkResource) -> str:
    """Current datastore resource id of ``resource.package`` (falls back to the known id on any error)."""

    try:
        response = client.session.get(SVK_PACKAGE_API, params={"id": resource.package}, timeout=(20, 60))
        if int(response.status_code) != 200:
            return resource.resource_id
        payload = response.json()
        for item in payload.get("result", {}).get("resources", []):
            if item.get("datastore_active") and item.get("id"):
                return str(item["id"])
    except Exception as exc:  # network errors, unexpected payloads: keep the pinned id
        LOGGER.warning("Could not resolve SVK resource for %s (%s); using the pinned id", resource.package, exc)
    return resource.resource_id


def make_csv_validator(required: tuple[str, ...]) -> Callable[[Path], None]:
    def _validate(path: Path) -> None:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            header = handle.readline().strip().lstrip("\ufeff")
        names = {h.strip().strip('"') for h in header.split(",")}
        missing = [c for c in required if c not in names]
        if missing:
            raise FetchError(f"SVK dump {path.name} lacks columns {missing}; header={header[:200]!r}")

    return _validate


def dump_params(resource: SvkResource, zone: str) -> dict[str, str]:
    return {
        "format": "csv",
        "filters": json.dumps({"bidding_zone": zone}, separators=(",", ":")),
        "sort": "start_time_utc",
        "fields": ",".join(resource.fields),
    }


def fetch_dump(
    client: HttpClient,
    raw_dir: Path,
    key: str,
    zone: str,
    *,
    snapshot_end_utc: pd.Timestamp,
    refresh: bool = False,
    offline: bool = False,
    resource_id: str | None = None,
) -> SvkDump:
    resource = SVK_RESOURCES[key]
    dest = raw_dir / "nordic" / "svk" / key / f"{key}_{zone}.csv"
    rid = resource_id or (resource.resource_id if offline or dest.exists() else resolve_resource_id(client, resource))
    record = cached_download(
        client,
        SVK_DUMP.format(resource_id=rid),
        dest,
        params=dump_params(resource, zone),
        validate=make_csv_validator(resource.fields),
        refresh=refresh,
        period_end_utc=snapshot_end_utc,
        offline=offline,
    )
    return SvkDump(key, zone, dest, record)


def read_dump(dump: SvkDump) -> pd.DataFrame:
    frame = pd.read_csv(dump.path, dtype={"bidding_zone": "string"})
    if "start_time_utc" in frame.columns:
        frame["start_time_utc"] = pd.to_datetime(frame["start_time_utc"], utc=True).astype("datetime64[ns, UTC]")
    return frame
