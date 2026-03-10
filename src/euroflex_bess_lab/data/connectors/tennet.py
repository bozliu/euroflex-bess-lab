from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from .common import ConnectorFetchMetadata, fetch_remote_payload


def _validate_tennet_payload(payload: object) -> None:
    if not isinstance(payload, dict) or "TimeSeries" not in payload:
        raise ValueError("Expected TenneT payload with `TimeSeries`")


class TenneTSettlementPricesConnector:
    """Fetch TenneT NL settlement prices from the official developer API."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = "https://api.tennet.eu",
        timeout_seconds: int = 30,
    ) -> None:
        self.api_key = api_key or os.getenv("TENNET_API_KEY")
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def fetch(
        self,
        *,
        start: datetime,
        end: datetime,
        max_retries: int = 0,
        backoff_factor: float = 0.5,
        cache_dir: Path | None = None,
        cache_ttl_minutes: int | None = None,
        return_metadata: bool = False,
    ) -> dict[str, object] | tuple[dict[str, object], ConnectorFetchMetadata]:
        if not self.api_key:
            raise RuntimeError("TENNET_API_KEY is required for live TenneT ingestion")
        url = f"{self.base_url}/publications/v1/settlement-prices"
        payload, metadata = fetch_remote_payload(
            connector_id="tennet_settlement_prices",
            url=url,
            request_start_utc=start,
            request_end_utc=end,
            params={
                "date_from": start.strftime("%d-%m-%Y %H:%M:%S"),
                "date_to": end.strftime("%d-%m-%Y %H:%M:%S"),
            },
            headers={"Accept": "application/json", "apikey": self.api_key},
            timeout_seconds=self.timeout_seconds,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            cache_dir=cache_dir,
            cache_ttl_minutes=cache_ttl_minutes,
            schema_validator=_validate_tennet_payload,
        )
        if return_metadata:
            return payload, metadata
        return payload
