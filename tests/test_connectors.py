from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
import requests

from euroflex_bess_lab.data.connectors import ConnectorAuthError, ConnectorRateLimitError, ConnectorSchemaError
from euroflex_bess_lab.data.connectors.common import fetch_remote_payload
from euroflex_bess_lab.data.connectors.elia import EliaImbalanceConnector
from euroflex_bess_lab.data.connectors.tennet import (
    TenneTFrequencyRestorationReserveActivationsConnector,
    TenneTMeritOrderListConnector,
    TenneTSettlementPricesConnector,
)


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


def test_tennet_connector_requires_api_key(monkeypatch) -> None:
    monkeypatch.delenv("TENNET_API_KEY", raising=False)
    monkeypatch.delenv("TENNET_API_KEY_ACCEPTANCE", raising=False)
    monkeypatch.delenv("TENNET_API_KEY_PRODUCTION", raising=False)
    connector = TenneTSettlementPricesConnector(api_key=None)
    with pytest.raises(RuntimeError, match="A TenneT API key is required"):
        connector.fetch(start=datetime.now(tz=UTC), end=datetime.now(tz=UTC))


def test_tennet_connector_detects_schema_drift(monkeypatch) -> None:
    malformed = {
        "TimeSeries": [
            {
                "Period": {
                    "Points": [
                        {
                            "shortage": "82.00",
                            "dispatch_up": "82.00",
                            "dispatch_down": "60.00",
                            "timeInterval_start": "2025-06-20T00:00",
                            "timeInterval_end": "2025-06-20T00:15",
                        }
                    ]
                }
            }
        ]
    }
    monkeypatch.setattr(
        "euroflex_bess_lab.data.connectors.common.requests.request",
        lambda **kwargs: DummyResponse(status_code=200, json_payload=malformed),
    )
    connector = TenneTSettlementPricesConnector(api_key="dummy-token")
    with pytest.raises(ConnectorSchemaError, match="missing required fields"):
        connector.fetch(start=datetime.now(tz=UTC), end=datetime.now(tz=UTC))


def test_tennet_connector_acceptance_environment_uses_acceptance_base_url(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_request(**kwargs):
        calls.append(kwargs)
        return DummyResponse(
            status_code=200,
            json_payload={
                "TimeSeries": [
                    {
                        "Period": {
                            "Points": [
                                {
                                    "shortage": "82.00",
                                    "surplus": "60.00",
                                    "dispatch_up": "82.00",
                                    "dispatch_down": "60.00",
                                    "timeInterval_start": "2025-06-20T00:00",
                                    "timeInterval_end": "2025-06-20T00:15",
                                }
                            ]
                        }
                    }
                ]
            },
        )

    monkeypatch.setattr("euroflex_bess_lab.data.connectors.common.requests.request", fake_request)
    monkeypatch.setenv("TENNET_API_KEY_ACCEPTANCE", "acceptance-token")
    connector = TenneTSettlementPricesConnector(environment="acceptance")
    payload, metadata = connector.fetch(
        start=datetime.now(tz=UTC),
        end=datetime.now(tz=UTC),
        return_metadata=True,
    )
    assert payload["TimeSeries"]
    assert metadata.environment == "acceptance"
    assert metadata.base_url == "https://api.acc.tennet.eu"
    assert calls[0]["url"] == "https://api.acc.tennet.eu/publications/v1/settlement-prices"
    assert calls[0]["headers"]["apikey"] == "acceptance-token"


def test_tennet_connector_accepts_wrapped_response_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        "euroflex_bess_lab.data.connectors.common.requests.request",
        lambda **kwargs: DummyResponse(
            status_code=200,
            json_payload={
                "Response": {
                    "TimeSeries": [
                        {
                            "Period": {
                                "Points": [
                                    {
                                        "shortage": "82.00",
                                        "surplus": "60.00",
                                        "dispatch_up": None,
                                        "dispatch_down": "60.00",
                                        "timeInterval_start": "2025-06-20T00:00",
                                        "timeInterval_end": "2025-06-20T00:15",
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
        ),
    )
    connector = TenneTSettlementPricesConnector(api_key="dummy-token", environment="production")
    payload, metadata = connector.fetch(
        start=datetime.now(tz=UTC),
        end=datetime.now(tz=UTC),
        return_metadata=True,
    )
    assert payload["Response"]["TimeSeries"]
    assert metadata.environment == "production"
    assert metadata.base_url == "https://api.tennet.eu"


def test_tennet_connector_prefers_env_specific_base_url_override(monkeypatch) -> None:
    monkeypatch.setenv("TENNET_API_BASE_URL_ACCEPTANCE", "https://acceptance.example.test")
    connector = TenneTSettlementPricesConnector(api_key="dummy-token", environment="acceptance")
    assert connector.base_url == "https://acceptance.example.test"


def test_tennet_connector_rejects_unknown_environment() -> None:
    with pytest.raises(ValueError, match="Unsupported TenneT environment"):
        TenneTSettlementPricesConnector(api_key="dummy-token", environment="staging")


def test_tennet_merit_order_connector_detects_schema_drift(monkeypatch) -> None:
    malformed = {"Response": {"TimeSeries": [{"Period": {"Points": [{"isp": "1"}]}}]}}
    monkeypatch.setattr(
        "euroflex_bess_lab.data.connectors.common.requests.request",
        lambda **kwargs: DummyResponse(status_code=200, json_payload=malformed),
    )
    connector = TenneTMeritOrderListConnector(api_key="dummy-token")
    with pytest.raises(ConnectorSchemaError, match="missing required fields"):
        connector.fetch(start=datetime.now(tz=UTC), end=datetime.now(tz=UTC))


def test_tennet_afrr_activations_connector_detects_schema_drift(monkeypatch) -> None:
    malformed = {"Response": {"TimeSeries": [{"Period": {"Points": [{"aFRR_up": "1", "aFRR_down": "0"}]}}]}}
    monkeypatch.setattr(
        "euroflex_bess_lab.data.connectors.common.requests.request",
        lambda **kwargs: DummyResponse(status_code=200, json_payload=malformed),
    )
    connector = TenneTFrequencyRestorationReserveActivationsConnector(api_key="dummy-token")
    with pytest.raises(ConnectorSchemaError, match="missing required fields"):
        connector.fetch(start=datetime.now(tz=UTC), end=datetime.now(tz=UTC))


def test_tennet_merit_order_connector_acceptance_environment_uses_acceptance_base_url(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_request(**kwargs):
        calls.append(kwargs)
        return DummyResponse(
            status_code=200,
            json_payload={
                "Response": {
                    "TimeSeries": [
                        {
                            "Period": {
                                "Points": [
                                    {
                                        "isp": "1",
                                        "timeInterval_start": "2025-06-20T00:00",
                                        "timeInterval_end": "2025-06-20T00:15",
                                        "Thresholds": [
                                            {
                                                "capacity_threshold": "10",
                                                "price_up": "80.0",
                                                "price_down": "60.0",
                                            }
                                        ],
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
        )

    monkeypatch.setattr("euroflex_bess_lab.data.connectors.common.requests.request", fake_request)
    monkeypatch.setenv("TENNET_API_KEY_ACCEPTANCE", "acceptance-token")
    connector = TenneTMeritOrderListConnector(environment="acceptance")
    payload, metadata = connector.fetch(
        start=datetime.now(tz=UTC),
        end=datetime.now(tz=UTC),
        return_metadata=True,
    )
    assert payload["Response"]["TimeSeries"]
    assert metadata.environment == "acceptance"
    assert calls[0]["url"] == "https://api.acc.tennet.eu/publications/v1/merit-order-list"
    assert calls[0]["headers"]["apikey"] == "acceptance-token"


def test_tennet_merit_order_connector_windows_large_requests(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_request(**kwargs):
        calls.append(kwargs)
        return DummyResponse(
            status_code=200,
            json_payload={
                "Response": {
                    "TimeSeries": [
                        {
                            "Period": {
                                "Points": [
                                    {
                                        "isp": "1",
                                        "timeInterval_start": "2025-06-20T00:00",
                                        "timeInterval_end": "2025-06-20T00:15",
                                        "Thresholds": [
                                            {
                                                "capacity_threshold": "10",
                                                "price_up": "80.0",
                                                "price_down": "60.0",
                                            }
                                        ],
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
        )

    monkeypatch.setattr("euroflex_bess_lab.data.connectors.common.requests.request", fake_request)
    connector = TenneTMeritOrderListConnector(api_key="dummy-token", environment="production")
    connector.fetch(
        start=datetime(2025, 1, 13, 0, 0, tzinfo=UTC),
        end=datetime(2025, 1, 14, 0, 0, tzinfo=UTC),
    )
    assert len(calls) == 4
