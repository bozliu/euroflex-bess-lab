from __future__ import annotations

import os
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
import yaml

from euroflex_bess_lab.config import load_config
from euroflex_bess_lab.data.connectors.tennet import (
    TenneTFrequencyRestorationReserveActivationsConnector,
    TenneTMeritOrderListConnector,
    TenneTSettlementPricesConnector,
)
from euroflex_bess_lab.data.io import save_json, save_price_series
from euroflex_bess_lab.data.normalization import (
    derive_tennet_afrr_activation_series,
    normalize_tennet_frequency_restoration_reserve_activations_json,
    normalize_tennet_merit_order_list_json,
    normalize_tennet_settlement_prices_json,
)
from euroflex_bess_lab.validation import validate_data_file

REPO_ROOT = Path(__file__).resolve().parents[1]
HAS_TENNET_LIVE_KEY = any(
    os.getenv(name) for name in ("TENNET_API_KEY", "TENNET_API_KEY_ACCEPTANCE", "TENNET_API_KEY_PRODUCTION")
)


def _build_day_ahead_frame_from_imbalance(imbalance_frame: pd.DataFrame) -> pd.DataFrame:
    day_ahead = imbalance_frame[["timestamp_utc", "timestamp_local", "zone"]].copy()
    day_ahead["market"] = "day_ahead"
    day_ahead["resolution"] = "PT15M"
    day_ahead["price_eur_per_mwh"] = 51.0
    day_ahead["currency"] = "EUR"
    day_ahead["source"] = "tennet_live_smoke_fixture"
    day_ahead["is_actual"] = True
    day_ahead["is_forecast"] = False
    day_ahead["quality_status"] = "Validated"
    day_ahead["provenance"] = "day_ahead_live_smoke_fixture"
    return day_ahead[
        [
            "timestamp_utc",
            "timestamp_local",
            "market",
            "resolution",
            "price_eur_per_mwh",
            "currency",
            "zone",
            "source",
            "is_actual",
            "is_forecast",
            "quality_status",
            "provenance",
        ]
    ]


@pytest.mark.skipif(not HAS_TENNET_LIVE_KEY, reason="A TenneT live API key is required for live TenneT smoke")
def test_live_tennet_payload_normalizes_and_validates_with_dutch_live_example(tmp_path: Path) -> None:
    amsterdam = ZoneInfo("Europe/Amsterdam")
    delivery_date = datetime.now(tz=UTC).astimezone(amsterdam).date() - timedelta(days=1)
    start_local = datetime.combine(delivery_date, time(0, 0), tzinfo=amsterdam)
    end_local = start_local + timedelta(days=1)
    start = start_local.astimezone(UTC)
    end = end_local.astimezone(UTC)
    connector = TenneTSettlementPricesConnector(environment=os.getenv("TENNET_API_ENV"))
    payload, metadata = connector.fetch(start=start, end=end, max_retries=1, return_metadata=True)
    series = normalize_tennet_settlement_prices_json(payload, local_timezone="Europe/Amsterdam")

    live_dir = tmp_path / "live"
    live_dir.mkdir(parents=True, exist_ok=True)
    imbalance_path = live_dir / "netherlands_imbalance.parquet"
    save_price_series(series, imbalance_path)
    save_json(
        {
            **metadata.as_dict(),
            "normalization_name": "normalize_tennet_settlement_prices_json",
            "local_timezone": "Europe/Amsterdam",
            "series_name": series.name,
            "market": series.market,
            "zone": series.zone,
            "resolution_minutes": series.resolution_minutes,
            "value_kind": series.value_kind,
            "series_metadata": series.metadata,
        },
        imbalance_path.with_name(f"{imbalance_path.name}.meta.json"),
    )

    day_ahead = _build_day_ahead_frame_from_imbalance(series.data)
    day_ahead_path = live_dir / "netherlands_day_ahead.parquet"
    day_ahead.to_parquet(day_ahead_path, index=False)

    config = load_config(REPO_ROOT / "examples" / "configs" / "basic" / "netherlands_da_only_live_inputs.yaml")
    config.timing.delivery_start_date = delivery_date
    config.timing.delivery_end_date = delivery_date
    config.data.day_ahead.actual_path = day_ahead_path
    config.data.imbalance.actual_path = imbalance_path  # type: ignore[union-attr]
    config.artifacts.root_dir = tmp_path / "artifacts"
    config_path = tmp_path / "live_inputs.yaml"
    config_path.write_text(yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False), encoding="utf-8")

    report = validate_data_file(config_path)
    assert report.ok, report.as_dict()


@pytest.mark.skipif(not HAS_TENNET_LIVE_KEY, reason="A TenneT live API key is required for live TenneT smoke")
def test_live_tennet_reserve_payloads_derive_activation_series(tmp_path: Path) -> None:
    start = datetime(2025, 1, 13, 0, 0, tzinfo=UTC)
    end = datetime(2025, 1, 14, 0, 0, tzinfo=UTC)
    environment = os.getenv("TENNET_API_ENV")
    merit_connector = TenneTMeritOrderListConnector(environment=environment)
    activations_connector = TenneTFrequencyRestorationReserveActivationsConnector(environment=environment)

    merit_payload, merit_metadata = merit_connector.fetch(start=start, end=end, max_retries=1, return_metadata=True)
    activations_payload, activations_metadata = activations_connector.fetch(
        start=start, end=end, max_retries=1, return_metadata=True
    )
    merit_frame = normalize_tennet_merit_order_list_json(merit_payload, local_timezone="Europe/Amsterdam")
    activations_frame = normalize_tennet_frequency_restoration_reserve_activations_json(
        activations_payload, local_timezone="Europe/Amsterdam"
    )
    derived = derive_tennet_afrr_activation_series(
        merit_frame, activations_frame, zone="10YNL----------L", source="tennet_live_derived"
    )

    output_dir = tmp_path / "reserve_live"
    output_dir.mkdir(parents=True, exist_ok=True)
    merit_path = output_dir / "merit_order.parquet"
    activations_path = output_dir / "afrr_activations.parquet"
    merit_frame.to_parquet(merit_path, index=False)
    activations_frame.to_parquet(activations_path, index=False)
    save_json(
        {
            **merit_metadata.as_dict(),
            "normalization_name": "normalize_tennet_merit_order_list_json",
            "market": "afrr_merit_order",
        },
        merit_path.with_name(f"{merit_path.name}.meta.json"),
    )
    save_json(
        {
            **activations_metadata.as_dict(),
            "normalization_name": "normalize_tennet_frequency_restoration_reserve_activations_json",
            "market": "afrr_activation_volume",
        },
        activations_path.with_name(f"{activations_path.name}.meta.json"),
    )

    for market_name, series in derived.items():
        target_path = output_dir / f"{market_name}.parquet"
        save_price_series(series, target_path)
        assert target_path.exists()
        assert len(series.data) > 0
