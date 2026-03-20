from __future__ import annotations

import json
from pathlib import Path

from euroflex_bess_lab.data.normalization import (
    derive_tennet_afrr_activation_series,
    normalize_elia_imbalance_json,
    normalize_entsoe_day_ahead_xml,
    normalize_tennet_frequency_restoration_reserve_activations_json,
    normalize_tennet_merit_order_list_json,
    normalize_tennet_settlement_prices_json,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "raw"


def test_entsoe_normalization_expands_hourly_series_and_handles_dst_gap() -> None:
    payload = (FIXTURE_DIR / "entsoe" / "entsoe_day_ahead_belgium_dst.xml").read_text(encoding="utf-8")
    series = normalize_entsoe_day_ahead_xml(payload)
    local = series.data["timestamp_local"]
    dst_day = local.dt.date.astype(str) == "2024-03-31"
    assert len(series.data) == 92
    assert int(series.data["resolution_minutes"].iloc[0]) == 15
    assert not any((local[dst_day].dt.hour == 2).tolist())
    assert series.data["provenance"].eq("expanded_from_60m").all()


def test_elia_normalization_keeps_quarter_hour_fields() -> None:
    payload = json.loads((FIXTURE_DIR / "elia" / "elia_imbalance_belgium.json").read_text(encoding="utf-8"))
    series = normalize_elia_imbalance_json(payload)
    assert len(series.data) == 8
    assert "system_imbalance_mw" in series.data.columns
    assert "marginal_incremental_price_eur_per_mwh" in series.data.columns
    assert series.data["resolution_minutes"].eq(15).all()


def test_tennet_normalization_keeps_dual_price_fields() -> None:
    payload = json.loads((FIXTURE_DIR / "tennet" / "settlement_prices.json").read_text(encoding="utf-8"))
    series = normalize_tennet_settlement_prices_json(payload)
    assert len(series.data) == 8
    assert "imbalance_shortage_price_eur_per_mwh" in series.data.columns
    assert "imbalance_surplus_price_eur_per_mwh" in series.data.columns
    assert series.data["resolution_minutes"].eq(15).all()
    assert series.data["zone"].eq("10YNL----------L").all()


def test_tennet_normalization_accepts_wrapped_response_payload() -> None:
    payload = json.loads((FIXTURE_DIR / "tennet" / "settlement_prices.json").read_text(encoding="utf-8"))
    payload["TimeSeries"][0]["Period"]["Points"][0]["dispatch_up"] = None
    wrapped_payload = {"Response": payload}
    series = normalize_tennet_settlement_prices_json(wrapped_payload)
    assert len(series.data) == 8
    assert series.data["imbalance_shortage_price_eur_per_mwh"].notna().all()
    assert float(series.data["dispatch_up_price_eur_per_mwh"].iloc[0]) == float(
        series.data["imbalance_shortage_price_eur_per_mwh"].iloc[0]
    )


def test_tennet_merit_order_normalization_keeps_threshold_rows() -> None:
    payload = json.loads((FIXTURE_DIR / "tennet" / "merit_order_list.json").read_text(encoding="utf-8"))
    frame = normalize_tennet_merit_order_list_json(payload)
    assert len(frame) == 6
    assert "threshold_mw" in frame.columns
    assert "price_up_eur_per_mwh" in frame.columns
    assert frame["resolution_minutes"].eq(15).all()


def test_tennet_activations_normalization_keeps_afrr_volume_rows() -> None:
    payload = json.loads(
        (FIXTURE_DIR / "tennet" / "frequency_restoration_reserve_activations.json").read_text(encoding="utf-8")
    )
    frame = normalize_tennet_frequency_restoration_reserve_activations_json(payload)
    assert len(frame) == 2
    assert "afrr_up_mw" in frame.columns
    assert "afrr_down_mw" in frame.columns
    assert frame["resolution_minutes"].eq(15).all()


def test_tennet_derived_activation_series_maps_merit_order_to_prices_and_ratios() -> None:
    merit_payload = json.loads((FIXTURE_DIR / "tennet" / "merit_order_list.json").read_text(encoding="utf-8"))
    activations_payload = json.loads(
        (FIXTURE_DIR / "tennet" / "frequency_restoration_reserve_activations.json").read_text(encoding="utf-8")
    )
    merit_frame = normalize_tennet_merit_order_list_json(merit_payload)
    activations_frame = normalize_tennet_frequency_restoration_reserve_activations_json(activations_payload)
    derived = derive_tennet_afrr_activation_series(merit_frame, activations_frame)

    assert sorted(derived) == [
        "afrr_activation_price_down",
        "afrr_activation_price_up",
        "afrr_activation_ratio_down",
        "afrr_activation_ratio_up",
    ]
    assert float(derived["afrr_activation_price_up"].data["price_eur_per_mwh"].iloc[0]) == 100.0
    assert float(derived["afrr_activation_price_down"].data["price_eur_per_mwh"].iloc[1]) == 58.0
    assert float(derived["afrr_activation_ratio_up"].data["price_eur_per_mwh"].iloc[0]) == 0.4
    assert float(derived["afrr_activation_ratio_down"].data["price_eur_per_mwh"].iloc[1]) == 0.6
