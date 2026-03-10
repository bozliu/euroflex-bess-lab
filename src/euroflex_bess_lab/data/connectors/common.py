from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests


class ConnectorError(RuntimeError):
    """Base connector error."""


class ConnectorAuthError(ConnectorError):
    """Raised when remote credentials are missing or rejected."""


class ConnectorRateLimitError(ConnectorError):
    """Raised when the remote API responds with a rate-limit condition."""


class ConnectorSchemaError(ConnectorError):
    """Raised when the upstream payload no longer matches the expected shape."""


@dataclass(frozen=True)
class ConnectorFetchMetadata:
    connector_id: str
    fetched_at_utc: str
    request_start_utc: str
    request_end_utc: str
    cache_hit: bool
    cache_key: str
    cache_path: str | None
    timeout_seconds: int
    max_retries: int
    backoff_factor: float
    status_code: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "connector_id": self.connector_id,
            "fetched_at_utc": self.fetched_at_utc,
            "request_start_utc": self.request_start_utc,
            "request_end_utc": self.request_end_utc,
            "cache_hit": self.cache_hit,
            "cache_key": self.cache_key,
            "cache_path": self.cache_path,
            "timeout_seconds": self.timeout_seconds,
            "max_retries": self.max_retries,
            "backoff_factor": self.backoff_factor,
            "status_code": self.status_code,
        }


def _cache_key(*, connector_id: str, url: str, params: dict[str, Any] | None) -> str:
    payload = json.dumps(
        {"connector_id": connector_id, "url": url, "params": params or {}}, sort_keys=True, default=str
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _cache_paths(cache_dir: Path, cache_key: str, *, suffix: str) -> tuple[Path, Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    payload_path = cache_dir / f"{cache_key}{suffix}"
    meta_path = cache_dir / f"{cache_key}.meta.json"
    return payload_path, meta_path


def _load_cache(
    *,
    cache_dir: Path | None,
    cache_key: str,
    suffix: str,
    cache_ttl_minutes: int | None,
) -> tuple[Any, dict[str, Any]] | None:
    if cache_dir is None or cache_ttl_minutes is None:
        return None
    payload_path, meta_path = _cache_paths(cache_dir, cache_key, suffix=suffix)
    if not payload_path.exists() or not meta_path.exists():
        return None
    age_seconds = time.time() - payload_path.stat().st_mtime
    if age_seconds > cache_ttl_minutes * 60:
        return None
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    if suffix == ".json":
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
    else:
        payload = payload_path.read_text(encoding="utf-8")
    return payload, metadata


def _write_cache(
    *,
    cache_dir: Path | None,
    cache_key: str,
    suffix: str,
    payload: Any,
    metadata: dict[str, Any],
) -> None:
    if cache_dir is None:
        return
    payload_path, meta_path = _cache_paths(cache_dir, cache_key, suffix=suffix)
    if suffix == ".json":
        payload_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    else:
        payload_path.write_text(str(payload), encoding="utf-8")
    meta_path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")


def _raise_for_status(response: requests.Response, *, connector_id: str) -> None:
    if response.status_code in {401, 403}:
        raise ConnectorAuthError(f"{connector_id} authentication failed with HTTP {response.status_code}")
    if response.status_code == 429:
        raise ConnectorRateLimitError(f"{connector_id} rate limit exceeded (HTTP 429)")
    response.raise_for_status()


def _request_with_retries(
    *,
    connector_id: str,
    method: str,
    url: str,
    params: dict[str, Any] | None,
    headers: dict[str, str] | None,
    timeout_seconds: int,
    max_retries: int,
    backoff_factor: float,
) -> requests.Response:
    attempts = max(max_retries, 0) + 1
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                headers=headers,
                timeout=timeout_seconds,
            )
            _raise_for_status(response, connector_id=connector_id)
            return response
        except (requests.RequestException, ConnectorError) as exc:
            last_error = exc
            if attempt >= attempts:
                break
            time.sleep(backoff_factor * (2 ** (attempt - 1)))
    if isinstance(last_error, ConnectorError):
        raise last_error
    raise ConnectorError(f"{connector_id} request failed after {attempts} attempt(s): {last_error}") from last_error


def fetch_remote_payload(
    *,
    connector_id: str,
    url: str,
    request_start_utc: datetime,
    request_end_utc: datetime,
    method: str = "GET",
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout_seconds: int = 30,
    max_retries: int = 0,
    backoff_factor: float = 0.5,
    cache_dir: Path | None = None,
    cache_ttl_minutes: int | None = None,
    parser: Callable[[requests.Response], Any] | None = None,
    schema_validator: Callable[[Any], None] | None = None,
    payload_suffix: str = ".json",
) -> tuple[Any, ConnectorFetchMetadata]:
    cache_key = _cache_key(connector_id=connector_id, url=url, params=params)
    cached = _load_cache(
        cache_dir=cache_dir, cache_key=cache_key, suffix=payload_suffix, cache_ttl_minutes=cache_ttl_minutes
    )
    if cached is not None:
        payload, cached_metadata = cached
        return payload, ConnectorFetchMetadata(
            connector_id=connector_id,
            fetched_at_utc=datetime.now(tz=UTC).isoformat(),
            request_start_utc=request_start_utc.astimezone(UTC).isoformat(),
            request_end_utc=request_end_utc.astimezone(UTC).isoformat(),
            cache_hit=True,
            cache_key=cache_key,
            cache_path=str(_cache_paths(cache_dir, cache_key, suffix=payload_suffix)[0])
            if cache_dir is not None
            else None,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            status_code=cached_metadata.get("status_code"),
        )

    response = _request_with_retries(
        connector_id=connector_id,
        method=method,
        url=url,
        params=params,
        headers=headers,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_factor=backoff_factor,
    )
    parsed = parser(response) if parser is not None else response.json()
    if schema_validator is not None:
        try:
            schema_validator(parsed)
        except Exception as exc:
            raise ConnectorSchemaError(f"{connector_id} response schema validation failed: {exc}") from exc
    metadata = ConnectorFetchMetadata(
        connector_id=connector_id,
        fetched_at_utc=datetime.now(tz=UTC).isoformat(),
        request_start_utc=request_start_utc.astimezone(UTC).isoformat(),
        request_end_utc=request_end_utc.astimezone(UTC).isoformat(),
        cache_hit=False,
        cache_key=cache_key,
        cache_path=str(_cache_paths(cache_dir, cache_key, suffix=payload_suffix)[0]) if cache_dir is not None else None,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_factor=backoff_factor,
        status_code=response.status_code,
    )
    _write_cache(
        cache_dir=cache_dir, cache_key=cache_key, suffix=payload_suffix, payload=parsed, metadata=metadata.as_dict()
    )
    return parsed, metadata
