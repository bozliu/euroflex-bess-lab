from __future__ import annotations

import json
from pathlib import Path

from euroflex_bess_lab.data.normalization import (
    normalize_elia_imbalance_json,
    normalize_entsoe_day_ahead_xml,
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
