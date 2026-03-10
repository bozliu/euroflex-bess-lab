from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
import requests

from euroflex_bess_lab.data.connectors import ConnectorAuthError, ConnectorRateLimitError, ConnectorSchemaError
from euroflex_bess_lab.data.connectors.common import fetch_remote_payload
from euroflex_bess_lab.data.connectors.elia import EliaImbalanceConnector


class DummyResponse:
    def __init__(self, *, status_code: int = 200, json_payload: dict | None = None, text: str = "") -> None:
        self.status_code = status_code
        self._json_payload = json_payload or {}
        self.text = text

    def json(self):
        return self._json_payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def test_fetch_remote_payload_retries_on_transient_error(monkeypatch) -> None:
    attempts = {"count": 0}

    def fake_request(**kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise requests.Timeout("slow upstream")
        return DummyResponse(status_code=200, json_payload={"results": []})

    monkeypatch.setattr("euroflex_bess_lab.data.connectors.common.requests.request", fake_request)
    payload, metadata = fetch_remote_payload(
        connector_id="elia_imbalance",
        url="https://example.com",
        request_start_utc=datetime.now(tz=UTC),
        request_end_utc=datetime.now(tz=UTC),
        max_retries=1,
        schema_validator=lambda payload: None,
    )
    assert payload == {"results": []}
    assert metadata.cache_hit is False
    assert attempts["count"] == 2


def test_fetch_remote_payload_translates_auth_and_rate_limit_errors(monkeypatch) -> None:
    monkeypatch.setattr(
        "euroflex_bess_lab.data.connectors.common.requests.request",
        lambda **kwargs: DummyResponse(status_code=401),
    )
    with pytest.raises(ConnectorAuthError):
        fetch_remote_payload(
            connector_id="tennet_settlement_prices",
            url="https://example.com",
            request_start_utc=datetime.now(tz=UTC),
            request_end_utc=datetime.now(tz=UTC),
        )

    monkeypatch.setattr(
        "euroflex_bess_lab.data.connectors.common.requests.request",
        lambda **kwargs: DummyResponse(status_code=429),
    )
    with pytest.raises(ConnectorRateLimitError):
        fetch_remote_payload(
            connector_id="tennet_settlement_prices",
            url="https://example.com",
            request_start_utc=datetime.now(tz=UTC),
            request_end_utc=datetime.now(tz=UTC),
        )


def test_elia_connector_detects_schema_drift(monkeypatch) -> None:
    monkeypatch.setattr(
        "euroflex_bess_lab.data.connectors.common.requests.request",
        lambda **kwargs: DummyResponse(status_code=200, json_payload={"unexpected": []}),
    )
    connector = EliaImbalanceConnector()
    with pytest.raises(ConnectorSchemaError):
        connector.fetch(start=datetime.now(tz=UTC), end=datetime.now(tz=UTC))


def test_fetch_remote_payload_cache_hit_and_ttl(monkeypatch, tmp_path: Path) -> None:
    calls = {"count": 0}

    def fake_request(**kwargs):
        calls["count"] += 1
        return DummyResponse(status_code=200, json_payload={"results": [{"value": 1}]})

    monkeypatch.setattr("euroflex_bess_lab.data.connectors.common.requests.request", fake_request)
    kwargs = {
        "connector_id": "elia_imbalance",
        "url": "https://example.com",
        "request_start_utc": datetime.now(tz=UTC),
        "request_end_utc": datetime.now(tz=UTC),
        "cache_dir": tmp_path,
        "cache_ttl_minutes": 10,
        "schema_validator": lambda payload: None,
    }
    first_payload, first_meta = fetch_remote_payload(**kwargs)
    second_payload, second_meta = fetch_remote_payload(**kwargs)
    assert first_payload == second_payload
    assert first_meta.cache_hit is False
    assert second_meta.cache_hit is True
    assert calls["count"] == 1
    assert any(path.suffix == ".json" for path in tmp_path.iterdir())
