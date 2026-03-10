from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .common import ConnectorFetchMetadata, fetch_remote_payload


def _validate_elia_payload(payload: object) -> None:
    if not isinstance(payload, dict) or "results" not in payload:
        raise ValueError("Expected Elia payload with `results`")


class EliaImbalanceConnector:
    """Fetch Elia quarter-hour imbalance prices from the official Opendatasoft API."""

    def __init__(
        self,
        *,
        dataset_id: str = "ods162",
        base_url: str = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets",
        timeout_seconds: int = 30,
    ) -> None:
        self.dataset_id = dataset_id
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def fetch(
        self,
        *,
        start: datetime,
        end: datetime,
        limit: int = 10000,
        max_retries: int = 0,
        backoff_factor: float = 0.5,
        cache_dir: Path | None = None,
        cache_ttl_minutes: int | None = None,
        return_metadata: bool = False,
    ) -> dict[str, object] | tuple[dict[str, object], ConnectorFetchMetadata]:
        where = (
            "datetime >= date'"
            + start.astimezone().strftime("%Y-%m-%dT%H:%M:%SZ")
            + "' AND datetime < date'"
            + end.astimezone().strftime("%Y-%m-%dT%H:%M:%SZ")
            + "'"
        )
        url = f"{self.base_url}/{self.dataset_id}/records"
        payload, metadata = fetch_remote_payload(
            connector_id="elia_imbalance",
            url=url,
            request_start_utc=start,
            request_end_utc=end,
            params={"where": where, "limit": limit, "order_by": "datetime"},
            timeout_seconds=self.timeout_seconds,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            cache_dir=cache_dir,
            cache_ttl_minutes=cache_ttl_minutes,
            schema_validator=_validate_elia_payload,
        )
        payload["dataset_id"] = self.dataset_id
        if return_metadata:
            return payload, metadata
        return payload
