"""ID AEP (netztransparenz.de) download, parsing, local build and loader - synthetic values, no network."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.public_data.http import FetchError, HttpClient, HttpConfig
from euroflex_bess_lab.public_data.intraday import (
    FETCH_COMMAND,
    ID_AEP_COLUMN,
    ID_AEP_RELPATH,
    build_id_aep,
    encode_request,
    load_intraday_index,
    parse_id_aep_csv,
)
from euroflex_bess_lab.public_data.intraday.netztransparenz import (
    ID_AEP_CSV_HANDLER_URL,
    decode_request,
    month_starts,
    normalize_chunks,
    request_params,
    validate_id_aep_csv,
)

HEADER = "﻿Datum von;(Uhrzeit) von;Zeitzone;(Uhrzeit) bis;Zeitzone;ID AEP in EUR/MWh"
LOCAL_PREFIX = "/" + "Users" + "/"


def synthetic_csv(local_from: str, local_to: str, *, value: Any = None, missing: set[int] | None = None) -> str:
    """CSV in the handler's format (UTC rows) for local days ``[local_from, local_to)``; synthetic prices."""

    lo = pd.Timestamp(local_from).tz_localize("Europe/Berlin").tz_convert("UTC")
    hi = pd.Timestamp(local_to).tz_localize("Europe/Berlin").tz_convert("UTC")
    lines = [HEADER]
    for i, ts in enumerate(pd.date_range(lo, hi, freq="15min", inclusive="left")):
        if missing and i in missing:
            continue
        end = ts + pd.Timedelta(minutes=15)
        price = value if value is not None else f"{50 + (i % 7) * 1.25:.2f}".replace(".", ",")
        lines.append(f"{ts:%d.%m.%Y};{ts:%H:%M};UTC;{end:%H:%M};UTC;{price}")
    return "\n".join(lines) + "\n"


class FakeResponse:
    def __init__(self, status: int, body: bytes) -> None:
        self.status_code = status
        self._body = body
        self.headers: dict[str, str] = {}

    @property
    def text(self) -> str:
        return self._body.decode("utf-8", errors="replace")

    def iter_content(self, chunk_size: int = 1) -> Any:
        for i in range(0, len(self._body), max(1, chunk_size)):
            yield self._body[i : i + chunk_size]

    def close(self) -> None:
        return None


class MonthSession:
    """Answers handler requests with a synthetic month CSV; records the decoded requests."""

    def __init__(self, last_local_day: str | None = None, broken: bool = False) -> None:
        self.requests: list[dict[str, Any]] = []
        self.urls: list[str] = []
        self.last_local_day = last_local_day
        self.broken = broken

    def get(self, url: str, params: dict[str, str] | None = None, **_: Any) -> FakeResponse:
        self.urls.append(url)
        request = decode_request((params or {})["request"])
        self.requests.append(request)
        if self.broken:
            return FakeResponse(200, b"<html>maintenance</html>")
        local_to = request["LocalTo"]
        if self.last_local_day is not None:
            local_to = min(local_to, f"{pd.Timestamp(self.last_local_day) + pd.Timedelta(days=1):%Y-%m-%d}")
        return FakeResponse(200, synthetic_csv(request["LocalFrom"], local_to).encode("utf-8"))


def _client(session: MonthSession) -> HttpClient:
    return HttpClient(config=HttpConfig(max_retries=0, backoff_s=0.0), session=session, sleep=lambda _s: None)


def test_request_round_trip_matches_browser_settings() -> None:
    encoded = encode_request("2024-01-01", "2024-02-01")
    payload = decode_request(encoded)
    assert payload["LocalFrom"] == "2024-01-01" and payload["LocalTo"] == "2024-02-01"
    assert payload["ResultTimeZone"] == "utc"
    assert payload["Settings"]["DataType"] == 30 and payload["Settings"]["WebApiRoute"] == "IdAep"
    assert decode_request(request_params(pd.Timestamp("2024-12-15"))["request"])["LocalTo"] == "2025-01-01"
    assert [m.strftime("%Y-%m") for m in month_starts(pd.Timestamp("2024-11-03"), pd.Timestamp("2025-02-01"))] == [
        "2024-11",
        "2024-12",
        "2025-01",
    ]


def test_parse_handles_german_decimals_thousands_and_undefined_values() -> None:
    text = "\n".join(
        [
            HEADER,
            "01.01.2026;00:00;UTC;00:15;UTC;12,34",
            "01.01.2026;00:15;UTC;00:30;UTC;-5,19",
            "01.01.2026;00:30;UTC;00:45;UTC;1.234,56",
            "01.01.2026;00:45;UTC;01:00;UTC;",
            "01.01.2026;01:00;UTC;01:15;UTC;N.A.",
            "2026-01-01;01:15;UTC;01:30;UTC;7",
        ]
    )
    frame, stats = parse_id_aep_csv(text)
    assert stats == {"rows": 6, "undefined": 2, "unparsed": 0}
    values = frame[ID_AEP_COLUMN].to_numpy()
    assert values[:3].tolist() == [12.34, -5.19, 1234.56]
    assert np.isnan(values[3]) and np.isnan(values[4]) and values[5] == 7.0
    assert str(frame["timestamp_utc"].dt.tz) == "UTC"
    assert (frame["timestamp_utc"].diff().dropna() == pd.Timedelta(minutes=15)).all()


def test_parse_dst_fall_back_day_has_100_quarter_hours() -> None:
    frame, stats = parse_id_aep_csv(synthetic_csv("2025-10-26", "2025-10-27"))
    assert stats["rows"] == 100 and len(frame) == 100
    spring, _ = parse_id_aep_csv(synthetic_csv("2026-03-29", "2026-03-30"))
    assert len(spring) == 92


def test_parse_rejects_local_time_zone_html_and_bad_intervals(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="UTC"):
        parse_id_aep_csv(HEADER + "\n01.01.2026;00:00;CET;00:15;CET;1,0\n")
    with pytest.raises(ValueError, match="Datum von"):
        parse_id_aep_csv("<html>error</html>")
    with pytest.raises(ValueError, match="15-minute"):
        parse_id_aep_csv(HEADER + "\n01.01.2026;00:00;UTC;01:00;UTC;1,0\n")
    bad = tmp_path / "bad.csv"
    bad.write_text("<!DOCTYPE html><html></html>", encoding="utf-8")
    with pytest.raises(FetchError):
        validate_id_aep_csv(bad)
    empty, stats = parse_id_aep_csv(HEADER + "\n")
    assert empty.empty and stats["rows"] == 0


def test_normalize_reindexes_gaps_and_drops_duplicates(tmp_path: Path) -> None:
    from euroflex_bess_lab.public_data.http import FetchRecord
    from euroflex_bess_lab.public_data.intraday.netztransparenz import MonthChunk

    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    a.write_text(synthetic_csv("2026-01-01", "2026-01-02", missing={5, 6}), encoding="utf-8")
    b.write_text(synthetic_csv("2026-01-01", "2026-01-03"), encoding="utf-8")
    record = FetchRecord(url="u", params={}, retrieved_at_utc="2026-10-26T00:00:00Z", size_bytes=1, sha256="x")
    frame, stats = normalize_chunks([MonthChunk(pd.Timestamp("2026-01-01"), a, record)])
    assert len(frame) == 96 and stats["missing_quarter_hours"] == 2
    assert frame[ID_AEP_COLUMN].isna().sum() == 2
    both, stats_both = normalize_chunks(
        [MonthChunk(pd.Timestamp("2026-01-01"), a, record), MonthChunk(pd.Timestamp("2026-01-01"), b, record)]
    )
    assert len(both) == 192 and stats_both["duplicates_dropped"] == 94 and both[ID_AEP_COLUMN].notna().all()


def test_build_writes_local_table_and_public_stub_only(tmp_path: Path) -> None:
    public, raw = tmp_path / "public", tmp_path / "raw"
    session = MonthSession(last_local_day="2026-02-10")
    result = build_id_aep(
        start="2026-01-01", end="2026-02-12", client=_client(session), root=public, raw_dir=raw, refresh=True
    )
    assert session.urls == [ID_AEP_CSV_HANDLER_URL] * 2
    assert [r["LocalFrom"] for r in session.requests] == ["2026-01-01", "2026-02-01"]
    assert result.last_complete_day == "2026-02-10"
    local = raw / ID_AEP_RELPATH
    assert local.exists() and not (public / ID_AEP_RELPATH).exists(), "the index must never be written to public"
    stub = json.loads((public / "intraday" / "id_aep_DE-LU.meta.json").read_text(encoding="utf-8"))
    assert stub["redistributed"] is False
    assert stub["local_path"] == "data/raw/intraday/id_aep_DE-LU.parquet"
    assert stub["fetch_command"] == FETCH_COMMAND and "Impressum" in stub["licence_url"]
    assert stub["coverage_end_utc"] == "2026-02-10T23:00:00Z"
    text = (public / "intraday" / "id_aep_DE-LU.meta.json").read_text(encoding="utf-8")
    text += (raw / "intraday" / "id_aep_DE-LU.meta.json").read_text(encoding="utf-8")
    assert LOCAL_PREFIX not in text and str(tmp_path) not in text
    frame = load_intraday_index("2025-12-31T23:00Z", "2026-03-01", root=public, raw=raw)  # UTC bounds
    assert len(frame) == 41 * 96 and frame[ID_AEP_COLUMN].notna().all()
    assert list(frame.columns) == ["timestamp_utc", "resolution_minutes", ID_AEP_COLUMN]
    # offline rebuild uses the raw cache only
    offline = build_id_aep(
        start="2026-01-01",
        end="2026-02-12",
        client=_client(MonthSession(broken=True)),
        root=public,
        raw_dir=raw,
        offline=True,
    )
    assert offline.local.rows == result.local.rows


def test_build_rejects_html_payloads(tmp_path: Path) -> None:
    with pytest.raises(FetchError):
        build_id_aep(
            start="2026-01-01",
            end="2026-01-05",
            client=_client(MonthSession(broken=True)),
            root=tmp_path / "public",
            raw_dir=tmp_path / "raw",
        )


def test_loader_errors_name_the_fetch_command(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="euroflex_bess_lab.public_data.intraday fetch"):
        load_intraday_index("2026-01-01", "2026-01-02", root=tmp_path / "p", raw=tmp_path / "r")
    with pytest.raises(ValueError, match="DE-LU"):
        load_intraday_index("2026-01-01", "2026-01-02", zone="BE", root=tmp_path, raw=tmp_path)
    with pytest.raises(ValueError, match="after"):
        load_intraday_index("2026-01-02", "2026-01-01", root=tmp_path, raw=tmp_path)


def test_committed_stub_sidecar_is_consistent() -> None:
    root = Path(__file__).resolve().parents[1] / "data" / "public" / "intraday"
    stub_path = root / "id_aep_DE-LU.meta.json"
    if not stub_path.exists():
        pytest.skip("no committed ID AEP sidecar stub")
    stub = json.loads(stub_path.read_text(encoding="utf-8"))
    assert stub["redistributed"] is False and stub["path"] == ID_AEP_RELPATH
    assert not list(root.glob("*.parquet")), "the ID AEP is not redistributable; only the sidecar may be committed"
    assert LOCAL_PREFIX not in stub_path.read_text(encoding="utf-8")
