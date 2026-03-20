from __future__ import annotations

import os
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .common import ConnectorFetchMetadata, fetch_remote_payload

TENNET_ENVIRONMENT_ALIASES = {
    "prd": "production",
    "prod": "production",
    "production": "production",
    "acc": "acceptance",
    "acceptance": "acceptance",
}

TENNET_BASE_URLS = {
    "production": "https://api.tennet.eu",
    "acceptance": "https://api.acc.tennet.eu",
}

TENNET_PUBLICATION_ENDPOINTS = {
    "settlement_prices": {
        "path": "/publications/v1/settlement-prices",
        "endpoint_id": "publications_v1_settlement_prices",
        "connector_id": "tennet_settlement_prices",
    },
    "merit_order_list": {
        "path": "/publications/v1/merit-order-list",
        "endpoint_id": "publications_v1_merit_order_list",
        "connector_id": "tennet_merit_order_list",
    },
    "frequency_restoration_reserve_activations": {
        "path": "/publications/v1/frequency-restoration-reserve-activations",
        "endpoint_id": "publications_v1_frequency_restoration_reserve_activations",
        "connector_id": "tennet_frequency_restoration_reserve_activations",
    },
}


def _normalize_environment_name(environment: str | None) -> str:
    raw_value = (environment or os.getenv("TENNET_API_ENV") or "production").strip().lower()
    normalized = TENNET_ENVIRONMENT_ALIASES.get(raw_value)
    if normalized is None:
        supported = ", ".join(sorted(TENNET_ENVIRONMENT_ALIASES))
        raise ValueError(f"Unsupported TenneT environment {environment!r}. Expected one of: {supported}")
    return normalized


def _resolve_api_key(environment: str, explicit_api_key: str | None) -> tuple[str | None, str]:
    if explicit_api_key:
        return explicit_api_key, "init_arg"
    specific_env_var = "TENNET_API_KEY_ACCEPTANCE" if environment == "acceptance" else "TENNET_API_KEY_PRODUCTION"
    return os.getenv(specific_env_var) or os.getenv("TENNET_API_KEY"), specific_env_var


def _resolve_base_url(environment: str, explicit_base_url: str | None) -> str:
    if explicit_base_url:
        return explicit_base_url
    specific_env_var = (
        "TENNET_API_BASE_URL_ACCEPTANCE" if environment == "acceptance" else "TENNET_API_BASE_URL_PRODUCTION"
    )
    return os.getenv(specific_env_var) or os.getenv("TENNET_API_BASE_URL") or TENNET_BASE_URLS[environment]


def _extract_time_series_rows(payload: object) -> list[dict[str, object]]:
    if not isinstance(payload, dict):
        raise ValueError("Expected TenneT payload to be a mapping")
    if isinstance(payload.get("TimeSeries"), list):
        return payload["TimeSeries"]
    response_wrapper = payload.get("Response")
    if isinstance(response_wrapper, dict) and isinstance(response_wrapper.get("TimeSeries"), list):
        return response_wrapper["TimeSeries"]
    raise ValueError("Expected TenneT payload with `TimeSeries`")


def _validate_time_series_points(payload: object, *, required_point_fields: set[str]) -> list[dict[str, Any]]:
    time_series_rows = _extract_time_series_rows(payload)
    if not isinstance(time_series_rows, Iterable) or isinstance(time_series_rows, (str, bytes)):
        raise ValueError("Expected TenneT `TimeSeries` to be an iterable collection")
    validated_points: list[dict[str, Any]] = []
    for time_series in time_series_rows:
        if not isinstance(time_series, dict):
            raise ValueError("Each TenneT time-series entry must be a mapping")
        period = time_series.get("Period")
        if not isinstance(period, dict):
            raise ValueError("Each TenneT time-series entry must contain a `Period` mapping")
        points = period.get("Points")
        if not isinstance(points, Iterable) or isinstance(points, (str, bytes)):
            raise ValueError("Each TenneT `Period` must contain an iterable `Points` collection")
        for point in points:
            if not isinstance(point, dict):
                raise ValueError("Each TenneT point must be a mapping")
            missing_fields = sorted(required_point_fields.difference(point))
            if missing_fields:
                raise ValueError("TenneT point is missing required fields: " + ", ".join(missing_fields))
            validated_points.append(point)
    if not validated_points:
        raise ValueError("TenneT payload did not contain any point rows")
    return validated_points


def _validate_tennet_payload(payload: object) -> None:
    _validate_time_series_points(
        payload,
        required_point_fields={
            "shortage",
            "surplus",
            "dispatch_up",
            "dispatch_down",
            "timeInterval_start",
            "timeInterval_end",
        },
    )


def _validate_tennet_merit_order_payload(payload: object) -> None:
    validated_points = _validate_time_series_points(
        payload,
        required_point_fields={"Thresholds", "isp", "timeInterval_start", "timeInterval_end"},
    )
    for point in validated_points:
        thresholds = point.get("Thresholds")
        if not isinstance(thresholds, Iterable) or isinstance(thresholds, (str, bytes)):
            raise ValueError("TenneT merit-order point must contain an iterable `Thresholds` collection")
        saw_threshold = False
        for threshold in thresholds:
            if not isinstance(threshold, dict):
                raise ValueError("Each TenneT merit-order threshold must be a mapping")
            missing_fields = sorted({"capacity_threshold", "price_up", "price_down"}.difference(threshold))
            if missing_fields:
                raise ValueError("TenneT merit-order threshold is missing fields: " + ", ".join(missing_fields))
            saw_threshold = True
        if not saw_threshold:
            raise ValueError("TenneT merit-order payload did not contain any thresholds")


def _validate_tennet_frr_activations_payload(payload: object) -> None:
    _validate_time_series_points(
        payload,
        required_point_fields={
            "aFRR_down",
            "aFRR_up",
            "absolute_total_volume",
            "isp",
            "mfrrda_volume_down",
            "mfrrda_volume_up",
            "timeInterval_start",
            "timeInterval_end",
            "total_volume",
        },
    )


class _TenneTPublicationsConnector:
    publication_key: str
    schema_validator: Any
    window_hours: int | None = None

    def __init__(
        self,
        *,
        api_key: str | None = None,
        environment: str | None = None,
        base_url: str | None = None,
        timeout_seconds: int = 30,
    ) -> None:
        self.environment = _normalize_environment_name(environment)
        resolved_key, key_source = _resolve_api_key(self.environment, api_key)
        self.api_key = resolved_key
        self.api_key_source = key_source
        resolved_base_url = _resolve_base_url(self.environment, base_url)
        self.base_url = resolved_base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    @property
    def publication_path(self) -> str:
        return str(TENNET_PUBLICATION_ENDPOINTS[self.publication_key]["path"])

    @property
    def endpoint_id(self) -> str:
        return str(TENNET_PUBLICATION_ENDPOINTS[self.publication_key]["endpoint_id"])

    @property
    def connector_id(self) -> str:
        return str(TENNET_PUBLICATION_ENDPOINTS[self.publication_key]["connector_id"])

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
            raise RuntimeError(
                "A TenneT API key is required for live TenneT ingestion; "
                "set TENNET_API_KEY, TENNET_API_KEY_ACCEPTANCE, or TENNET_API_KEY_PRODUCTION"
            )
        if self.window_hours is not None and end - start > timedelta(hours=self.window_hours):
            return self._fetch_windowed(
                start=start,
                end=end,
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                cache_dir=cache_dir,
                cache_ttl_minutes=cache_ttl_minutes,
                return_metadata=return_metadata,
            )
        url = f"{self.base_url}{self.publication_path}"
        payload, metadata = fetch_remote_payload(
            connector_id=self.connector_id,
            url=url,
            request_start_utc=start,
            request_end_utc=end,
            endpoint_id=self.endpoint_id,
            source_operator="TenneT",
            auth_mode="apikey_header_env_var",
            environment=self.environment,
            base_url=self.base_url,
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
            schema_validator=self.schema_validator,
        )
        if return_metadata:
            return payload, metadata
        return payload

    def _fetch_windowed(
        self,
        *,
        start: datetime,
        end: datetime,
        max_retries: int,
        backoff_factor: float,
        cache_dir: Path | None,
        cache_ttl_minutes: int | None,
        return_metadata: bool,
    ) -> dict[str, object] | tuple[dict[str, object], ConnectorFetchMetadata]:
        assert self.window_hours is not None
        assert self.api_key is not None
        current_start = start
        merged_time_series: list[dict[str, object]] = []
        chunk_metadata: list[ConnectorFetchMetadata] = []
        while current_start < end:
            current_end = min(current_start + timedelta(hours=self.window_hours), end)
            chunk_payload, metadata = fetch_remote_payload(
                connector_id=self.connector_id,
                url=f"{self.base_url}{self.publication_path}",
                request_start_utc=current_start,
                request_end_utc=current_end,
                endpoint_id=self.endpoint_id,
                source_operator="TenneT",
                auth_mode="apikey_header_env_var",
                environment=self.environment,
                base_url=self.base_url,
                params={
                    "date_from": current_start.strftime("%d-%m-%Y %H:%M:%S"),
                    "date_to": current_end.strftime("%d-%m-%Y %H:%M:%S"),
                },
                headers={"Accept": "application/json", "apikey": self.api_key},
                timeout_seconds=self.timeout_seconds,
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                cache_dir=cache_dir,
                cache_ttl_minutes=cache_ttl_minutes,
                schema_validator=self.schema_validator,
            )
            chunk_metadata.append(metadata)
            merged_time_series.extend(_extract_time_series_rows(chunk_payload))
            current_start = current_end
        merged_payload: dict[str, object] = {"Response": {"TimeSeries": merged_time_series}}
        merged_metadata = ConnectorFetchMetadata(
            connector_id=self.connector_id,
            endpoint_id=self.endpoint_id,
            source_operator="TenneT",
            auth_mode="apikey_header_env_var",
            environment=self.environment,
            base_url=self.base_url,
            fetched_at_utc=datetime.now(tz=UTC).isoformat(),
            request_start_utc=start.astimezone(UTC).isoformat(),
            request_end_utc=end.astimezone(UTC).isoformat(),
            cache_hit=all(item.cache_hit for item in chunk_metadata),
            cache_key=f"{self.connector_id}_windowed_{start.astimezone(UTC).isoformat()}_{end.astimezone(UTC).isoformat()}",
            cache_path=None,
            timeout_seconds=self.timeout_seconds,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            status_code=chunk_metadata[-1].status_code if chunk_metadata else None,
        )
        if return_metadata:
            return merged_payload, merged_metadata
        return merged_payload


class TenneTSettlementPricesConnector(_TenneTPublicationsConnector):
    """Fetch TenneT NL settlement prices from the official developer API."""

    publication_key = "settlement_prices"
    schema_validator = staticmethod(_validate_tennet_payload)


class TenneTMeritOrderListConnector(_TenneTPublicationsConnector):
    """Fetch TenneT NL merit-order ladders for balancing and reserve bids."""

    publication_key = "merit_order_list"
    schema_validator = staticmethod(_validate_tennet_merit_order_payload)
    window_hours = 6


class TenneTFrequencyRestorationReserveActivationsConnector(_TenneTPublicationsConnector):
    """Fetch TenneT NL aFRR activation volumes from the official developer API."""

    publication_key = "frequency_restoration_reserve_activations"
    schema_validator = staticmethod(_validate_tennet_frr_activations_payload)
    window_hours = 6
