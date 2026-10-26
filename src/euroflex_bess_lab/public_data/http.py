"""HTTP fetching with retries, timeouts, exponential backoff and an on-disk raw cache.

Every cached download ``X`` gets a small provenance record ``X.source.json`` (URL, query parameters, retrieval
time, size, sha256) so the normalized tables can cite exactly what they were built from. Records only contain
URLs and relative names, never local absolute paths.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import pandas as pd
import requests

LOGGER = logging.getLogger(__name__)

USER_AGENT = "euroflex-bess-lab-public-data/1 (+https://github.com/bozliu/euroflex-bess-lab)"
RETRY_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class FetchError(RuntimeError):
    """Raised when a download fails after all retries or returns an invalid payload."""


@dataclass(frozen=True)
class HttpConfig:
    connect_timeout_s: float = 20.0
    read_timeout_s: float = 900.0
    max_retries: int = 5
    backoff_s: float = 2.0
    backoff_cap_s: float = 120.0
    min_interval_s: float = 0.0


@dataclass(frozen=True)
class FetchRecord:
    """Provenance of one cached download."""

    url: str
    params: dict[str, str]
    retrieved_at_utc: str
    size_bytes: int
    sha256: str
    from_cache: bool = False

    @property
    def full_url(self) -> str:
        return f"{self.url}?{urlencode(self.params)}" if self.params else self.url

    def to_json(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "params": dict(self.params),
            "full_url": self.full_url,
            "retrieved_at_utc": self.retrieved_at_utc,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
        }

    @classmethod
    def from_json(cls, payload: Mapping[str, Any], *, from_cache: bool = True) -> FetchRecord:
        return cls(
            url=str(payload["url"]),
            params={str(k): str(v) for k, v in dict(payload.get("params") or {}).items()},
            retrieved_at_utc=str(payload["retrieved_at_utc"]),
            size_bytes=int(payload.get("size_bytes", 0)),
            sha256=str(payload.get("sha256", "")),
            from_cache=from_cache,
        )

    @property
    def retrieved_at(self) -> pd.Timestamp:
        return pd.Timestamp(self.retrieved_at_utc).tz_convert("UTC")


def utc_now_iso() -> str:
    return pd.Timestamp.now(tz="UTC").floor("s").strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(chunk_size), b""):
            digest.update(block)
    return digest.hexdigest()


def source_record_path(path: Path) -> Path:
    return path.with_name(path.name + ".source.json")


def read_source_record(path: Path) -> FetchRecord | None:
    record_path = source_record_path(path)
    if not record_path.exists():
        return None
    return FetchRecord.from_json(json.loads(record_path.read_text(encoding="utf-8")))


def write_source_record(path: Path, record: FetchRecord) -> None:
    source_record_path(path).write_text(json.dumps(record.to_json(), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def validate_parquet_file(path: Path) -> None:
    """Cheap structural validation of a parquet download (magic bytes + readable footer)."""

    size = path.stat().st_size
    if size < 12:
        raise FetchError(f"Downloaded parquet is too small ({size} bytes): {path.name}")
    with path.open("rb") as handle:
        head = handle.read(4)
        handle.seek(-4, os.SEEK_END)
        tail = handle.read(4)
    if head != b"PAR1" or tail != b"PAR1":
        snippet = path.read_bytes()[:200].decode("utf-8", errors="replace")
        raise FetchError(f"Download is not a parquet file ({path.name}); first bytes: {snippet!r}")
    import pyarrow.parquet as pq

    pq.read_metadata(path)


def validate_json_file(path: Path) -> None:
    try:
        json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FetchError(f"Download is not valid JSON: {path.name}") from exc


def validate_xlsx_file(path: Path) -> None:
    with path.open("rb") as handle:
        if handle.read(2) != b"PK":
            raise FetchError(f"Download is not an xlsx (zip) file: {path.name}")


@dataclass
class HttpClient:
    """Thin wrapper around ``requests`` with retry/backoff; inject ``session`` to mock HTTP in tests."""

    config: HttpConfig = field(default_factory=HttpConfig)
    session: Any = None
    sleep: Callable[[float], None] = time.sleep
    _last_request_at: float = 0.0

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()
            self.session.headers.update({"User-Agent": USER_AGENT})

    def _throttle(self) -> None:
        if self.config.min_interval_s <= 0:
            return
        wait = self.config.min_interval_s - (time.monotonic() - self._last_request_at)
        if wait > 0:
            self.sleep(wait)

    def _backoff(self, attempt: int, retry_after: str | None = None) -> float:
        if retry_after:
            try:
                return min(float(retry_after), self.config.backoff_cap_s)
            except ValueError:
                pass
        return float(min(self.config.backoff_s * (2**attempt), self.config.backoff_cap_s))

    def download(
        self,
        url: str,
        dest: Path,
        *,
        params: Mapping[str, str] | None = None,
        validate: Callable[[Path], None] | None = None,
    ) -> FetchRecord:
        """Download ``url`` to ``dest`` atomically (``dest.part`` then rename) and write its source record."""

        params_dict = {str(k): str(v) for k, v in (params or {}).items()}
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")
        last_error: Exception | None = None
        for attempt in range(self.config.max_retries + 1):
            self._throttle()
            self._last_request_at = time.monotonic()
            try:
                response = self.session.get(
                    url,
                    params=params_dict or None,
                    timeout=(self.config.connect_timeout_s, self.config.read_timeout_s),
                    stream=True,
                )
                status = int(response.status_code)
                if status in RETRY_STATUS:
                    retry_after = response.headers.get("Retry-After") if hasattr(response, "headers") else None
                    response.close()
                    raise _RetryableStatus(status, retry_after)
                if status >= 400:
                    body = response.text[:300] if hasattr(response, "text") else ""
                    response.close()
                    raise FetchError(f"HTTP {status} for {url} params={params_dict}: {body}")
                with part.open("wb") as handle:
                    for block in response.iter_content(chunk_size=1 << 20):
                        if block:
                            handle.write(block)
                response.close()
                if validate is not None:
                    validate(part)
                os.replace(part, dest)
                record = FetchRecord(
                    url=url,
                    params=params_dict,
                    retrieved_at_utc=utc_now_iso(),
                    size_bytes=dest.stat().st_size,
                    sha256=sha256_file(dest),
                )
                write_source_record(dest, record)
                return record
            except _RetryableStatus as exc:
                last_error = exc
                wait = self._backoff(attempt, exc.retry_after)
            except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as exc:
                last_error = exc
                wait = self._backoff(attempt)
            except FetchError as exc:
                # Invalid payloads (e.g. an HTML error page instead of parquet) are retried too; HTTP 4xx are not.
                if str(exc).startswith("HTTP "):
                    part.unlink(missing_ok=True)
                    raise
                last_error = exc
                wait = self._backoff(attempt)
            part.unlink(missing_ok=True)
            if attempt < self.config.max_retries:
                LOGGER.warning("Retrying %s in %.1fs after: %s", url, wait, last_error)
                self.sleep(wait)
        raise FetchError(
            f"Giving up on {url} params={params_dict} after {self.config.max_retries + 1} attempts: {last_error}"
        )


class _RetryableStatus(Exception):
    def __init__(self, status: int, retry_after: str | None) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status
        self.retry_after = retry_after


def payload_size(path: Path, suffix: str | None = None) -> int | None:
    """Row count of a cached payload (parquet rows, Energy-Charts JSON points); ``None`` when unknown."""

    kind = suffix or path.suffix
    try:
        if kind == ".parquet":
            import pyarrow.parquet as pq

            return int(pq.read_metadata(path).num_rows)
        if kind == ".json":
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and isinstance(payload.get("unix_seconds"), list):
                return len(payload["unix_seconds"])
    except (OSError, ValueError):
        return None
    return None


def _move(src: Path, dst: Path) -> None:
    if src.exists():
        os.replace(src, dst)
    record_src, record_dst = source_record_path(src), source_record_path(dst)
    if record_src.exists():
        os.replace(record_src, record_dst)


def cached_download(
    client: HttpClient,
    url: str,
    dest: Path,
    *,
    params: Mapping[str, str] | None = None,
    validate: Callable[[Path], None] | None = None,
    refresh: bool = False,
    period_end_utc: pd.Timestamp | None = None,
    settle: pd.Timedelta = pd.Timedelta(days=3),
    max_open_age: pd.Timedelta = pd.Timedelta(hours=12),
    offline: bool = False,
    allow_shrink: bool = False,
) -> FetchRecord:
    """Return the cached file for ``dest`` or download it.

    A cached chunk is reused unless ``refresh`` is set or the chunk was still *open* when it was downloaded
    (retrieved before ``period_end_utc + settle``) and is older than ``max_open_age``. ``offline=True`` never
    touches the network and raises ``FileNotFoundError`` for missing chunks.

    Re-downloads never destroy data: if the new payload has fewer rows than the cached one (upstream deleted
    history, e.g. Elia's rolling bid windows or the ODS211/ODS212 removal) or the download fails, the cached copy
    is kept (``allow_shrink=True`` accepts a smaller payload).
    """

    if dest.exists() and not refresh:
        record = read_source_record(dest)
        if record is None:
            # File placed by hand (e.g. an emergency download); adopt it with its modification time.
            mtime = pd.Timestamp(dest.stat().st_mtime, unit="s", tz="UTC").floor("s")
            record = FetchRecord(
                url=url,
                params={str(k): str(v) for k, v in (params or {}).items()},
                retrieved_at_utc=mtime.strftime("%Y-%m-%dT%H:%M:%SZ"),
                size_bytes=dest.stat().st_size,
                sha256=sha256_file(dest),
            )
            write_source_record(dest, record)
        is_open = period_end_utc is not None and record.retrieved_at < period_end_utc + settle
        too_old = pd.Timestamp.now(tz="UTC") - record.retrieved_at > max_open_age
        if offline or not (is_open and too_old):
            return FetchRecord.from_json(record.to_json(), from_cache=True)
    if offline:
        if dest.exists():
            cached = read_source_record(dest)
            if cached is not None:
                return FetchRecord.from_json(cached.to_json(), from_cache=True)
        raise FileNotFoundError(f"Raw cache file missing and offline mode is on: {dest.name}")
    if not dest.exists():
        LOGGER.info("Downloading %s -> %s", url, dest.name)
        return client.download(url, dest, params=params, validate=validate)
    backup = dest.with_name(dest.name + ".prev")
    _move(dest, backup)
    try:
        LOGGER.info("Refreshing %s -> %s", url, dest.name)
        record = client.download(url, dest, params=params, validate=validate)
    except (FetchError, OSError) as exc:
        LOGGER.warning("Refresh of %s failed (%s); keeping the cached copy", dest.name, exc)
        _move(backup, dest)
        kept = read_source_record(dest)
        if kept is None:
            raise
        return FetchRecord.from_json(kept.to_json(), from_cache=True)
    old_rows, new_rows = payload_size(backup, dest.suffix), payload_size(dest)
    if not allow_shrink and old_rows is not None and new_rows is not None and new_rows < old_rows:
        LOGGER.warning("Refreshed %s has fewer rows (%d < %d); keeping the cached copy", dest.name, new_rows, old_rows)
        _move(backup, dest)
        kept = read_source_record(dest)
        return FetchRecord.from_json(kept.to_json() if kept else record.to_json(), from_cache=True)
    backup.unlink(missing_ok=True)
    source_record_path(backup).unlink(missing_ok=True)
    return record
