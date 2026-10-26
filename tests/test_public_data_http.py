from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from euroflex_bess_lab.public_data import energy_charts
from euroflex_bess_lab.public_data.elia import Chunk, export_url, fetch_dataset, plan_chunks, read_chunks, where_clause
from euroflex_bess_lab.public_data.http import (
    FetchError,
    HttpClient,
    HttpConfig,
    cached_download,
    read_source_record,
    validate_parquet_file,
)

LOCAL_PREFIX = "/" + "Users" + "/"  # never write absolute local paths into files


def _parquet_bytes(n: int = 3) -> bytes:
    table = pa.table(
        {
            "datetime": pa.array(pd.date_range("2026-03-02", periods=n, freq="15min", tz="UTC")),
            "value": pa.array([float(i) for i in range(n)]),
        }
    )
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


class FakeResponse:
    def __init__(self, status: int, body: bytes, headers: dict[str, str] | None = None) -> None:
        self.status_code = status
        self._body = body
        self.headers = headers or {}

    @property
    def text(self) -> str:
        return self._body.decode("utf-8", errors="replace")

    def iter_content(self, chunk_size: int = 1) -> Any:
        for i in range(0, len(self._body), max(1, chunk_size)):
            yield self._body[i : i + chunk_size]

    def close(self) -> None:
        return None


class FakeSession:
    """Replays a scripted list of responses (or exceptions) and records every request."""

    def __init__(self, script: list[Any]) -> None:
        self.script = list(script)
        self.calls: list[tuple[str, dict[str, str] | None]] = []

    def get(self, url: str, params: dict[str, str] | None = None, timeout: Any = None, stream: bool = False) -> Any:
        self.calls.append((url, params))
        item = self.script.pop(0) if len(self.script) > 1 else self.script[0]
        if isinstance(item, Exception):
            raise item
        return item


def _client(session: FakeSession, retries: int = 3) -> HttpClient:
    return HttpClient(config=HttpConfig(max_retries=retries, backoff_s=0.0), session=session, sleep=lambda _s: None)


def test_download_retries_transient_errors_then_writes_atomically(tmp_path: Path) -> None:
    body = _parquet_bytes()
    session = FakeSession(
        [
            FakeResponse(503, b"busy"),
            requests.ConnectionError("reset"),
            FakeResponse(200, b"<html>oops</html>"),  # invalid payload is retried
            FakeResponse(200, body),
        ]
    )
    dest = tmp_path / "raw" / "x.parquet"
    record = _client(session).download(
        "https://example.test/x", dest, params={"where": "a"}, validate=validate_parquet_file
    )
    assert dest.read_bytes() == body
    assert not dest.with_name("x.parquet.part").exists()
    assert len(session.calls) == 4
    stored = read_source_record(dest)
    assert stored is not None and stored.sha256 == record.sha256 and stored.params == {"where": "a"}
    assert "where=a" in stored.full_url
    assert LOCAL_PREFIX not in dest.with_name("x.parquet.source.json").read_text(encoding="utf-8")


def test_download_gives_up_and_does_not_retry_client_errors(tmp_path: Path) -> None:
    always_busy = FakeSession([FakeResponse(429, b"slow down", {"Retry-After": "0"})])
    with pytest.raises(FetchError, match="Giving up"):
        _client(always_busy, retries=2).download("https://example.test/x", tmp_path / "x.parquet")
    assert len(always_busy.calls) == 3
    not_found = FakeSession([FakeResponse(404, b"missing")])
    with pytest.raises(FetchError, match="HTTP 404"):
        _client(not_found).download("https://example.test/y", tmp_path / "y.parquet")
    assert len(not_found.calls) == 1


def test_cached_download_reuses_closed_chunks_and_refreshes_open_ones(tmp_path: Path) -> None:
    session = FakeSession([FakeResponse(200, _parquet_bytes())])
    client = _client(session)
    dest = tmp_path / "c.parquet"
    closed_end = pd.Timestamp("2020-01-01", tz="UTC")
    first = cached_download(
        client, "https://example.test/c", dest, validate=validate_parquet_file, period_end_utc=closed_end
    )
    assert not first.from_cache
    again = cached_download(
        client, "https://example.test/c", dest, validate=validate_parquet_file, period_end_utc=closed_end
    )
    assert again.from_cache and len(session.calls) == 1
    # A chunk retrieved before its period ended is re-fetched once it is older than max_open_age.
    open_end = pd.Timestamp.now(tz="UTC") + pd.Timedelta(days=10)
    cached_download(
        client, "https://example.test/c", dest, period_end_utc=open_end, max_open_age=pd.Timedelta(seconds=-1)
    )
    assert len(session.calls) == 2
    cached_download(client, "https://example.test/c", dest, refresh=True)
    assert len(session.calls) == 3
    with pytest.raises(FileNotFoundError):
        cached_download(client, "https://example.test/d", tmp_path / "missing.parquet", offline=True)


def test_cached_download_adopts_hand_placed_files(tmp_path: Path) -> None:
    dest = tmp_path / "manual.parquet"
    dest.write_bytes(_parquet_bytes())
    session = FakeSession([FakeResponse(500, b"should not be called")])
    record = cached_download(
        _client(session), "https://example.test/m", dest, period_end_utc=pd.Timestamp("2020-01-01", tz="UTC")
    )
    assert record.from_cache and session.calls == []
    assert read_source_record(dest) is not None


def test_plan_chunks_are_calendar_aligned() -> None:
    chunks = plan_chunks(pd.Timestamp("2024-05-21T22:00Z"), pd.Timestamp("2024-07-02T00:00Z"), "month")
    assert [c.label for c in chunks] == [
        "2024-05-01__2024-06-01",
        "2024-06-01__2024-07-01",
        "2024-07-01__2024-08-01",
    ]
    years = plan_chunks(pd.Timestamp("2024-05-21T22:00Z"), pd.Timestamp("2026-10-26T00:00Z"), "year")
    assert [c.label for c in years] == ["2024-01-01__2025-01-01", "2025-01-01__2026-01-01", "2026-01-01__2027-01-01"]
    assert plan_chunks(pd.Timestamp("2025-01-01", tz="UTC"), pd.Timestamp("2025-01-01", tz="UTC"), "month") == []
    assert [
        c.label for c in plan_chunks(pd.Timestamp("2025-01-01", tz="UTC"), pd.Timestamp("2025-02-01", tz="UTC"), "all")
    ] == ["all"]


def test_where_clause_uses_explicit_utc_literals() -> None:
    chunk = Chunk(pd.Timestamp("2026-03-01", tz="UTC"), pd.Timestamp("2026-04-01", tz="UTC"))
    assert where_clause("datetime", chunk) == 'datetime>="2026-03-01T00:00:00Z" and datetime<"2026-04-01T00:00:00Z"'
    assert where_clause("datetime", Chunk(None, None)) is None


def test_fetch_dataset_requests_each_chunk_once(tmp_path: Path) -> None:
    session = FakeSession([FakeResponse(200, _parquet_bytes())])
    client = _client(session)
    chunks = fetch_dataset(
        client,
        tmp_path,
        "ods134",
        "datetime",
        pd.Timestamp("2025-01-01", tz="UTC"),
        pd.Timestamp("2025-03-01", tz="UTC"),
        freq="month",
    )
    assert len(chunks) == 2 and len(session.calls) == 2
    url, params = session.calls[0]
    assert url == export_url("ods134")
    assert params == {"where": 'datetime>="2025-01-01T00:00:00Z" and datetime<"2025-02-01T00:00:00Z"'}
    assert chunks[0].path == tmp_path / "elia" / "ods134" / "ods134__2025-01-01__2025-02-01.parquet"
    frame = read_chunks(chunks)
    assert len(frame) == 6
    cached = fetch_dataset(
        client,
        tmp_path,
        "ods134",
        "datetime",
        pd.Timestamp("2025-01-01", tz="UTC"),
        pd.Timestamp("2025-03-01", tz="UTC"),
    )
    assert all(c.record.from_cache for c in cached) and len(session.calls) == 2


def test_energy_charts_fetch_month_builds_inclusive_local_day_query(tmp_path: Path) -> None:
    payload = {"unix_seconds": [1759262400], "price": [70.0], "license_info": "CC BY 4.0"}
    session = FakeSession([FakeResponse(200, json.dumps(payload).encode())])
    chunk = energy_charts.fetch_month(_client(session), tmp_path, "BE", pd.Timestamp("2025-02-01"))
    url, params = session.calls[0]
    assert url == energy_charts.ENERGY_CHARTS_PRICE_API
    assert params == {"bzn": "BE", "start": "2025-02-01", "end": "2025-02-28"}
    assert chunk.path.name == "price_BE_2025-02.json"
    with pytest.raises(ValueError):
        energy_charts.fetch_month(_client(session), tmp_path, "FR", pd.Timestamp("2025-02-01"))


def test_refresh_never_shrinks_or_loses_cached_data(tmp_path: Path) -> None:
    dest = tmp_path / "ods211__all.parquet"
    big = _parquet_bytes(10)
    session = FakeSession([FakeResponse(200, big)])
    client = _client(session)
    cached_download(client, "https://example.test/ods211", dest, validate=validate_parquet_file)
    # Upstream deleted the data: a refresh returns fewer rows -> the cached copy is kept.
    session.script = [FakeResponse(200, _parquet_bytes(0))]
    kept = cached_download(client, "https://example.test/ods211", dest, validate=validate_parquet_file, refresh=True)
    assert kept.from_cache and pq.read_metadata(dest).num_rows == 10
    assert not dest.with_name(dest.name + ".prev").exists()
    # A failing refresh also keeps the cached copy.
    session.script = [FakeResponse(503, b"down")]
    kept = cached_download(_client(session, retries=1), "https://example.test/ods211", dest, refresh=True)
    assert kept.from_cache and pq.read_metadata(dest).num_rows == 10
    # A larger payload replaces it; allow_shrink accepts smaller ones explicitly.
    session.script = [FakeResponse(200, _parquet_bytes(12))]
    cached_download(client, "https://example.test/ods211", dest, validate=validate_parquet_file, refresh=True)
    assert pq.read_metadata(dest).num_rows == 12
    session.script = [FakeResponse(200, _parquet_bytes(2))]
    cached_download(client, "https://example.test/ods211", dest, refresh=True, allow_shrink=True)
    assert pq.read_metadata(dest).num_rows == 2


def test_frozen_datasets_are_not_refreshed(tmp_path: Path) -> None:
    session = FakeSession([FakeResponse(200, _parquet_bytes())])
    client = _client(session)
    frozen = pd.Timestamp("2026-08-01", tz="UTC")
    fetch_dataset(client, tmp_path, "ods212", "datetime", None, None, freq="all", frozen_after=frozen)
    # Pretend the cache is old: an open "all" chunk would be re-fetched, a frozen one is not.
    record_path = tmp_path / "elia" / "ods212" / "ods212__all.parquet.source.json"
    payload = json.loads(record_path.read_text(encoding="utf-8"))
    payload["retrieved_at_utc"] = "2026-10-26T03:13:00Z"
    record_path.write_text(json.dumps(payload), encoding="utf-8")
    again = fetch_dataset(client, tmp_path, "ods212", "datetime", None, None, freq="all", frozen_after=frozen)
    assert again[0].record.from_cache and len(session.calls) == 1
