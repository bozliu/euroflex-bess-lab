from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from ...time_utils import UTC
from .common import ConnectorFetchMetadata, fetch_remote_payload


def _validate_entsoe_xml(payload: object) -> None:
    if not isinstance(payload, str) or "<" not in payload or "TimeSeries" not in payload:
        raise ValueError("Expected ENTSO-E XML payload containing TimeSeries")


class EntsoeDayAheadConnector:
    """Fetch ENTSO-E day-ahead prices via the Transparency Platform API."""

    def __init__(
        self,
        *,
        token: str | None = None,
        base_url: str = "https://web-api.tp.entsoe.eu/api",
        timeout_seconds: int = 30,
    ) -> None:
        self.token = token or os.getenv("ENTSOE_API_TOKEN")
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds

    def fetch(
        self,
        *,
        start: datetime,
        end: datetime,
        zone: str = "10YBE----------2",
        max_retries: int = 0,
        backoff_factor: float = 0.5,
        cache_dir: Path | None = None,
        cache_ttl_minutes: int | None = None,
        return_metadata: bool = False,
    ) -> str | tuple[str, ConnectorFetchMetadata]:
        if not self.token:
            raise RuntimeError("ENTSOE_API_TOKEN is required for live ENTSO-E ingestion")
        params = {
            "securityToken": self.token,
            "documentType": "A44",
            "in_Domain": zone,
            "out_Domain": zone,
            "periodStart": start.astimezone(UTC).strftime("%Y%m%d%H%M"),
            "periodEnd": end.astimezone(UTC).strftime("%Y%m%d%H%M"),
        }
        payload, metadata = fetch_remote_payload(
            connector_id="entsoe_day_ahead",
            url=self.base_url,
            request_start_utc=start,
            request_end_utc=end,
            params=params,
            timeout_seconds=self.timeout_seconds,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            cache_dir=cache_dir,
            cache_ttl_minutes=cache_ttl_minutes,
            parser=lambda response: response.text,
            schema_validator=_validate_entsoe_xml,
            payload_suffix=".xml",
        )
        if return_metadata:
            return payload, metadata
        return payload
