"""Germany (DE-LU) adapter: registration, workflow rejection, day-ahead MTU transition, reserve blocks, parsers.

Day-ahead fixtures in tests/fixtures/germany are real Energy-Charts payloads (CC BY 4.0, Bundesnetzagentur |
SMARD.de). Reserve inputs are synthetic: regelleistung.net results are not redistributed.
"""

from __future__ import annotations

import json
import zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape

import numpy as np
import pandas as pd
import pytest
import yaml
from pydantic import ValidationError

from euroflex_bess_lab.benchmarks import BenchmarkRegistry
from euroflex_bess_lab.capabilities import experimental_workflow_capabilities, render_capability_matrix_markdown
from euroflex_bess_lab.config import BacktestConfig, load_config
from euroflex_bess_lab.markets import MarketRegistry
from euroflex_bess_lab.markets.adapters.germany import (
    DE_LU_BIDDING_ZONE,
    GERMANY_LFC_AREA,
    GermanyMarketAdapter,
    day_ahead_mtu_mismatch_days,
    energy_charts_payload_to_frame,
    expand_reserve_blocks,
    infer_source_resolution_minutes,
    normalize_day_ahead_frame,
    parse_regelleistung_afrr_results,
    parse_regelleistung_fcr_results,
    read_xlsx_rows,
    reserve_block_index,
)
from euroflex_bess_lab.validation import validate_config_file

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "germany"
EXAMPLES = REPO_ROOT / "examples" / "configs" / "basic"


def _payload(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / f"energy_charts_de_lu_{name}.json").read_text(encoding="utf-8"))


def _germany_config(workflow: str = "da_only", **extra: Any) -> dict[str, Any]:
    data_dir = REPO_ROOT / "examples" / "data"
    payload: dict[str, Any] = {
        "schema_version": 4,
        "run_name": f"germany-{workflow}",
        "market": {"id": "germany"},
        "workflow": workflow,
        "forecast_provider": {"name": "persistence"},
        "timing": {
            "timezone": "Europe/Berlin",
            "day_ahead_gate_closure_local": "12:00",
            "delivery_start_date": "2025-10-01",
            "delivery_end_date": "2025-10-01",
        },
        "site": {"id": "de-site", "poi_import_limit_mw": 1.0, "poi_export_limit_mw": 1.0},
        "assets": [
            {
                "id": "de-bess",
                "battery": {
                    "power_mw": 1.0,
                    "energy_mwh": 2.0,
                    "initial_soc_mwh": 1.0,
                    "terminal_soc_mwh": 1.0,
                    "soc_min_mwh": 0.1,
                    "soc_max_mwh": 1.9,
                    "charge_efficiency": 0.95,
                    "discharge_efficiency": 0.95,
                },
            }
        ],
        "data": {"day_ahead": {"actual_path": str(data_dir / "germany_day_ahead_prices.csv")}},
        "artifacts": {"root_dir": "artifacts/test", "save_inputs": False, "save_plots": False},
    }
    if workflow == "da_plus_fcr":
        payload["data"]["fcr_capacity"] = {"actual_path": str(data_dir / "germany_fcr_capacity_blocks.csv")}
        payload["fcr"] = {"sustain_duration_minutes": 30}
    if workflow == "da_plus_afrr":
        blocks = str(data_dir / "germany_afrr_capacity_blocks.csv")
        payload["data"]["afrr_capacity_up"] = {"actual_path": blocks}
        payload["data"]["afrr_capacity_down"] = {"actual_path": blocks}
        payload["afrr"] = {
            "sustain_duration_minutes": 60,
            "settlement_mode": "capacity_only",
            "activation_mode": "none",
        }
    for key, value in extra.items():
        payload[key] = value
    return payload


# --------------------------------------------------------------------------------------------- registration


def test_registry_exposes_germany_adapter() -> None:
    adapter = MarketRegistry.get("germany")
    assert isinstance(adapter, GermanyMarketAdapter)
    assert "germany" in MarketRegistry.supported_market_ids()
    assert adapter.timezone == "Europe/Berlin"
    assert adapter.day_ahead_zone == DE_LU_BIDDING_ZONE
    assert adapter.fcr_zone == adapter.afrr_zone == GERMANY_LFC_AREA
    assert adapter.supported_workflows == ("da_only", "da_plus_fcr", "da_plus_afrr")
    assert adapter.default_benchmarks() == ("perfect_foresight", "persistence", "csv")
    assert adapter.supported_reserve_products() == ("fcr_symmetric", "afrr_asymmetric")
    benchmark = BenchmarkRegistry.resolve("germany", "da_plus_fcr", "persistence", run_scope="single_asset")
    assert benchmark.benchmark_name == "germany.da_plus_fcr.persistence.single_asset"


def test_capability_matrix_declares_germany_surface() -> None:
    rows = {(row["workflow"], row["market"]): row for row in experimental_workflow_capabilities()}
    assert rows[("da_plus_fcr", "germany")]["tier"] == "experimental"
    assert rows[("da_plus_imbalance", "germany")]["tier"] == "unsupported"
    rendered = render_capability_matrix_markdown()
    assert "## Experimental market surface (Germany)" in rendered
    assert "| `da_plus_imbalance` | `germany` | No | No | unsupported |" in rendered


# --------------------------------------------------------------------------------------------- imbalance rejection


def test_config_rejects_da_plus_imbalance_for_germany() -> None:
    payload = _germany_config("da_plus_imbalance")
    payload["data"]["imbalance"] = {"actual_path": payload["data"]["day_ahead"]["actual_path"]}
    with pytest.raises(ValidationError, match="Bilanzkreistreue"):
        BacktestConfig.model_validate(payload)


def test_config_rejects_revision_with_imbalance_base_for_germany() -> None:
    payload = _germany_config(
        "schedule_revision",
        revision={"base_workflow": "da_plus_imbalance", "revision_checkpoints_local": ["10:00"]},
    )
    payload["data"]["imbalance"] = {"actual_path": payload["data"]["day_ahead"]["actual_path"]}
    with pytest.raises(ValidationError, match="does not support workflow `da_plus_imbalance`"):
        BacktestConfig.model_validate(payload)


def test_adapter_rejects_imbalance_workflow_directly() -> None:
    adapter = MarketRegistry.get("germany")
    with pytest.raises(ValueError, match="Bilanzkreistreue"):
        adapter.settlement_engine("da_plus_imbalance")
    with pytest.raises(ValueError, match="Unsupported Germany workflow"):
        adapter.settlement_engine("intraday_continuous")
    assert adapter.settlement_engine("da_only").settlement_basis == "no_imbalance_component"
    # Bypass config validation (model_copy does not re-validate) to prove the adapter rejects on its own.
    config = BacktestConfig.model_validate(_germany_config()).model_copy(update={"workflow": "da_plus_imbalance"})
    with pytest.raises(ValueError, match="Bilanzkreistreue"):
        adapter.validate_timing(config)
    with pytest.raises(ValueError, match="Bilanzkreistreue"):
        adapter.load_actuals(config)


def test_validate_config_reports_germany_imbalance_rejection(tmp_path: Path) -> None:
    payload = _germany_config("da_plus_imbalance")
    payload["data"]["imbalance"] = {"actual_path": payload["data"]["day_ahead"]["actual_path"]}
    path = tmp_path / "germany_imbalance.yaml"
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    report = validate_config_file(path)
    assert not report.ok
    assert "Bilanzkreistreue" in report.checks[0].detail


# --------------------------------------------------------------------------------------------- config rules


def test_capacity_only_afrr_rules() -> None:
    BacktestConfig.model_validate(_germany_config("da_plus_afrr"))

    default_mode = _germany_config("da_plus_afrr")
    default_mode["afrr"] = {"sustain_duration_minutes": 60}
    with pytest.raises(ValidationError, match="set afrr.settlement_mode=capacity_only"):
        BacktestConfig.model_validate(default_mode)

    with_activation = _germany_config("da_plus_afrr")
    with_activation["data"]["afrr_activation_ratio_up"] = {
        "actual_path": with_activation["data"]["day_ahead"]["actual_path"]
    }
    with pytest.raises(ValidationError, match="remove the activation inputs: afrr_activation_ratio_up"):
        BacktestConfig.model_validate(with_activation)

    mismatched = _germany_config("da_plus_afrr")
    mismatched["afrr"]["activation_mode"] = "expected_value"
    with pytest.raises(ValidationError, match="capacity_only requires afrr.activation_mode=none"):
        BacktestConfig.model_validate(mismatched)

    belgium = _germany_config("da_plus_afrr")
    belgium["market"] = {"id": "belgium"}
    belgium["timing"]["timezone"] = "Europe/Brussels"
    with pytest.raises(ValidationError, match="only supported for market germany"):
        BacktestConfig.model_validate(belgium)


def test_validate_timing_enforces_berlin_and_noon_gate_closure() -> None:
    adapter = MarketRegistry.get("germany")
    adapter.validate_timing(BacktestConfig.model_validate(_germany_config()))
    wrong_zone = _germany_config()
    wrong_zone["timing"]["timezone"] = "Europe/Brussels"
    with pytest.raises(ValueError, match="timezone=Europe/Berlin"):
        adapter.validate_timing(BacktestConfig.model_validate(wrong_zone))
    late = _germany_config()
    late["timing"]["day_ahead_gate_closure_local"] = "13:00"
    with pytest.raises(ValueError, match="<= 12:00"):
        adapter.validate_timing(BacktestConfig.model_validate(late))


@pytest.mark.parametrize(
    ("workflow", "clock", "basis"),
    [
        ("da_only", "12:00", "day_ahead_gate_closure"),
        ("da_plus_fcr", "08:00", "fcr_gate_closure"),
        ("da_plus_afrr", "09:00", "afrr_capacity_gate_closure"),
    ],
)
def test_decision_schedule_uses_reserve_gate_closures(workflow: str, clock: str, basis: str) -> None:
    config = BacktestConfig.model_validate(_germany_config(workflow))
    adapter = MarketRegistry.get("germany")
    schedule = adapter.decision_schedule(config)
    row = schedule.iloc[0]
    assert row["day_ahead_gate_closure_local"] == pd.Timestamp(f"2025-09-30 {clock}", tz="Europe/Berlin")
    assert row["day_ahead_market_gate_closure_local"] == pd.Timestamp("2025-09-30 12:00", tz="Europe/Berlin")
    assert row["decision_time_basis"] == basis
    assert adapter.settlement_metadata(config)["decision_time_basis"] == basis


# --------------------------------------------------------------------------------------------- day-ahead MTU transition


def test_hourly_day_ahead_is_expanded_to_flat_quarter_hours_with_provenance() -> None:
    raw = energy_charts_payload_to_frame(_payload("2025-09-15_2025-10-12"))
    assert len(raw) == 16 * 24 + 12 * 96
    frame = normalize_day_ahead_frame(raw, source="energy_charts")
    assert len(frame) == 28 * 96
    assert frame["timestamp_utc"].diff().dropna().eq(pd.Timedelta(minutes=15)).all()
    assert frame["resolution_minutes"].eq(15).all()
    local_day = frame["timestamp_local"].dt.date
    hourly = frame.loc[local_day < date(2025, 10, 1)]
    quarter_hourly = frame.loc[local_day >= date(2025, 10, 1)]
    assert hourly["source_resolution_minutes"].eq(60).all()
    assert hourly["provenance"].eq("expanded_from_60m").all()
    assert quarter_hourly["source_resolution_minutes"].eq(15).all()
    assert quarter_hourly["provenance"].eq("source_15m").all()
    per_hour = hourly.groupby(hourly["timestamp_utc"].dt.floor("h"))["price_eur_per_mwh"].agg(["min", "max"])
    assert (per_hour["min"] == per_hour["max"]).all()
    source_hourly = raw.loc[raw["timestamp_utc"] < pd.Timestamp("2025-09-30T22:00Z"), "price_eur_per_mwh"]
    np.testing.assert_allclose(per_hour["min"].to_numpy(), source_hourly.to_numpy())
    assert day_ahead_mtu_mismatch_days(frame) == []


def test_normalised_frames_can_be_fed_back_without_double_expansion(tmp_path: Path) -> None:
    frame = normalize_day_ahead_frame(energy_charts_payload_to_frame(_payload("2025-09-15_2025-10-12")))
    path = tmp_path / "normalized_day_ahead.parquet"
    frame.to_parquet(path, index=False)
    again = MarketRegistry.get("germany").load_market_series(
        path, name="day_ahead_actual", market="day_ahead", zone=DE_LU_BIDDING_ZONE
    )
    assert len(again.data) == len(frame)
    np.testing.assert_allclose(again.data["price_eur_per_mwh"], frame["price_eur_per_mwh"])
    assert again.data["provenance"].tolist() == frame["provenance"].tolist()
    assert again.data["source_resolution_minutes"].tolist() == frame["source_resolution_minutes"].tolist()
    assert again.metadata["expanded_interval_count"] == 16 * 96


def test_mtu_mismatch_days_are_reported() -> None:
    frame = normalize_day_ahead_frame(energy_charts_payload_to_frame(_payload("2025-10-25_2025-10-27")))
    frame.loc[frame["timestamp_local"].dt.date == date(2025, 10, 25), "source_resolution_minutes"] = 60
    assert day_ahead_mtu_mismatch_days(frame) == ["2025-10-25"]


@pytest.mark.parametrize(
    ("name", "day", "quarter_hours"),
    [("2025-10-25_2025-10-27", date(2025, 10, 26), 100), ("2026-03-28_2026-03-30", date(2026, 3, 29), 92)],
)
def test_dst_days_keep_local_day_length_and_block_structure(name: str, day: date, quarter_hours: int) -> None:
    frame = normalize_day_ahead_frame(energy_charts_payload_to_frame(_payload(name)))
    on_day = frame.loc[frame["timestamp_local"].dt.date == day]
    assert len(on_day) == quarter_hours
    blocks = reserve_block_index(on_day["timestamp_utc"])
    assert np.bincount(blocks).tolist() == [quarter_hours - 80, 16, 16, 16, 16, 16]


def test_resolution_inference_rejects_gaps_and_missing_prices() -> None:
    raw = energy_charts_payload_to_frame(_payload("2025-10-25_2025-10-27"))
    with pytest.raises(ValueError, match="gaps"):
        normalize_day_ahead_frame(raw.drop(index=150).reset_index(drop=True))
    payload = _payload("2025-10-25_2025-10-27")
    payload["price"][10] = None
    with pytest.raises(ValueError, match="missing prices"):
        normalize_day_ahead_frame(energy_charts_payload_to_frame(payload))
    with pytest.raises(ValueError, match="single row"):
        infer_source_resolution_minutes(raw["timestamp_utc"].iloc[[0, 200]])


def test_adapter_loads_energy_charts_json_directly() -> None:
    adapter = MarketRegistry.get("germany")
    series = adapter.load_market_series(
        FIXTURES / "energy_charts_de_lu_2025-09-15_2025-10-12.json",
        name="day_ahead_actual",
        market="day_ahead",
        zone=DE_LU_BIDDING_ZONE,
    )
    assert series.source == "energy_charts_api"
    assert series.metadata["expanded_interval_count"] == 16 * 96
    assert series.metadata["source_resolutions_minutes"] == [15, 60]
    assert "CC BY 4.0" in str(series.metadata["attribution"])


def test_public_data_layout_is_accepted(tmp_path: Path) -> None:
    raw = energy_charts_payload_to_frame(_payload("2025-09-15_2025-10-12"))
    resolution = infer_source_resolution_minutes(raw["timestamp_utc"])
    public = pd.DataFrame(
        {
            "timestamp_utc": raw["timestamp_utc"],
            "resolution_minutes": resolution,
            "price_eur_mwh": raw["price_eur_per_mwh"],
        }
    )
    path = tmp_path / "da_prices_DE-LU.parquet"
    public.to_parquet(path, index=False)
    series = MarketRegistry.get("germany").load_market_series(
        path, name="day_ahead_actual", market="day_ahead", zone=DE_LU_BIDDING_ZONE
    )
    assert len(series.data) == 28 * 96


# --------------------------------------------------------------------------------------------- reserve blocks


def _blocks(day: date, prices: list[float], *, direction: str, product: str = "aFRR") -> pd.DataFrame:
    rows = []
    for block, price in enumerate(prices):
        start = pd.Timestamp(f"{day} {block * 4:02d}:00", tz="Europe/Berlin")
        # Calendar midnight of the next local day (adding 24 h would land at 23:00 on a 25-hour day).
        end = (
            pd.Timestamp(f"{day} {block * 4 + 4:02d}:00", tz="Europe/Berlin")
            if block < 5
            else pd.Timestamp(f"{day + timedelta(days=1)} 00:00", tz="Europe/Berlin")
        )
        rows.append(
            {
                "delivery_start_utc": start.tz_convert("UTC"),
                "delivery_end_utc": pd.Timestamp(end).tz_convert("UTC"),
                "product": product,
                "direction": direction,
                "price_eur_per_mw_h": price,
                "marginal_price_eur_per_mw_h": price * 1.2,
            }
        )
    return pd.DataFrame(rows)


def test_reserve_blocks_expand_across_dst_and_filter_direction() -> None:
    day = date(2025, 10, 26)
    blocks = pd.concat(
        [
            _blocks(day, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0], direction="up"),
            _blocks(day, [7.0, 8.0, 9.0, 10.0, 11.0, 12.0], direction="NEG"),
        ],
        ignore_index=True,
    )
    up = expand_reserve_blocks(blocks, market="afrr_capacity_up", direction="POS")
    down = expand_reserve_blocks(blocks, market="afrr_capacity_down", direction="down")
    assert len(up) == len(down) == 100
    assert up["price_eur_per_mwh"].iloc[:20].eq(1.0).all() and up["price_eur_per_mwh"].iloc[20:36].eq(2.0).all()
    assert down["price_eur_per_mwh"].iloc[-16:].eq(12.0).all()
    assert up["provenance"].iloc[0] == "expanded_from_300m_block"
    assert up["provenance"].iloc[-1] == "expanded_from_240m_block"
    assert up["marginal_price_eur_per_mw_h"].iloc[0] == pytest.approx(1.2)
    with pytest.raises(ValueError, match="overlap"):
        expand_reserve_blocks(pd.concat([blocks, blocks]), market="afrr_capacity_up", direction="up")
    with pytest.raises(ValueError, match="no `symmetric` rows"):
        expand_reserve_blocks(blocks, market="fcr_capacity", direction="symmetric")


def test_adapter_load_actuals_for_capacity_only_afrr_example() -> None:
    config = load_config(EXAMPLES / "germany_da_plus_afrr_base.yaml")
    adapter = MarketRegistry.get("germany")
    actuals = adapter.load_actuals(config)
    assert len(actuals.day_ahead.data) == 192
    first_day = actuals.day_ahead.data.iloc[:96]
    assert first_day["provenance"].str.endswith("expanded_from_60m").all()
    assert actuals.afrr_capacity_up is not None and actuals.afrr_capacity_down is not None
    assert len(actuals.afrr_capacity_up.data) == len(actuals.afrr_capacity_down.data) == 192
    assert actuals.afrr_capacity_up.data["price_eur_per_mwh"].iloc[0] == pytest.approx(4.0)
    assert actuals.afrr_capacity_down.data["price_eur_per_mwh"].iloc[0] == pytest.approx(3.0)
    for series in (
        actuals.afrr_activation_price_up,
        actuals.afrr_activation_price_down,
        actuals.afrr_activation_ratio_up,
        actuals.afrr_activation_ratio_down,
    ):
        assert series is not None
        assert series.metadata["synthetic"] is True
        assert series.data["price_eur_per_mwh"].eq(0.0).all()
    product = adapter.build_reserve_product(config)
    assert product is not None
    assert product.settlement_assumption.settlement_mode == "capacity_only"
    assert product.activation_assumption.activation_mode == "none"
    assert product.metadata["product_block_minutes"] == 240
    assert product.metadata["pricing_rule"] == "pay_as_bid"


# --------------------------------------------------------------------------------------------- regelleistung.net parsers


def _column_letters(index: int) -> str:
    letters = ""
    index += 1
    while index:
        index, remainder = divmod(index - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _write_xlsx(path: Path, rows: list[list[object]]) -> Path:
    namespace = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    shared: list[str] = []
    positions: dict[str, int] = {}
    sheet_rows = []
    for row_number, row in enumerate(rows, start=1):
        cells = []
        for column, value in enumerate(row):
            reference = f"{_column_letters(column)}{row_number}"
            if isinstance(value, str):
                if value not in positions:
                    positions[value] = len(shared)
                    shared.append(value)
                cells.append(f'<c r="{reference}" t="s"><v>{positions[value]}</v></c>')
            elif value is not None:
                cells.append(f'<c r="{reference}"><v>{value}</v></c>')
        sheet_rows.append(f'<row r="{row_number}">{"".join(cells)}</row>')
    sheet = f'<?xml version="1.0" encoding="UTF-8"?><worksheet xmlns="{namespace}"><sheetData>{"".join(sheet_rows)}</sheetData></worksheet>'
    strings = "".join(f"<si><t>{escape(text)}</t></si>" for text in shared)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("xl/worksheets/sheet1.xml", sheet)
        archive.writestr(
            "xl/sharedStrings.xml", f'<?xml version="1.0" encoding="UTF-8"?><sst xmlns="{namespace}">{strings}</sst>'
        )
    return path


def _serial(day: date) -> float:
    return float((day - date(1899, 12, 30)).days)


def test_fcr_results_parser_converts_block_price_to_hourly(tmp_path: Path) -> None:
    day = date(2025, 10, 26)
    header = [
        "DATE_FROM",
        "DATE_TO",
        "PRODUCT_TYPE",
        "TENDER_NUMBER",
        "PRODUCTNAME",
        "GERMANY_DEMAND_[MW]",
        "GERMANY_SETTLEMENTCAPACITY_PRICE_[EUR/MW]",
    ]
    names = ["00_04", "04_08", "08_12", "12_16", "16_20", "20_24"]
    rows: list[list[object]] = [header]
    rows += [
        [_serial(day), _serial(day), "FCR", 1.0, f"NEGPOS_{name}", 500.0, 50.0 + index]
        for index, name in enumerate(names)
    ]
    rows += [[_serial(day), _serial(day), "FCR", 2.0, f"NEGPOS_{name}", "-", "-"] for name in names]
    parsed = parse_regelleistung_fcr_results(read_xlsx_rows(_write_xlsx(tmp_path / "fcr.xlsx", rows)))
    assert len(parsed) == 6
    assert parsed["block_hours"].tolist() == [5.0, 4.0, 4.0, 4.0, 4.0, 4.0]
    assert parsed["price_eur_per_mw_h"].iloc[0] == pytest.approx(50.0 / 5.0)
    assert parsed["price_eur_per_mw_h"].iloc[1] == pytest.approx(51.0 / 4.0)
    assert parsed["direction"].eq("symmetric").all()
    assert parsed["delivery_start_utc"].iloc[0] == pd.Timestamp("2025-10-25T22:00Z")
    assert parsed["delivery_end_utc"].iloc[-1] == pd.Timestamp("2025-10-26T23:00Z")


def test_afrr_results_parser_keeps_average_and_marginal(tmp_path: Path) -> None:
    day = date(2026, 3, 29)
    header = [
        "DATE_FROM",
        "DATE_TO",
        "TYPE_OF_RESERVES",
        "PRODUCT",
        "GERMANY_MIN_CAPACITY_PRICE_[(EUR/MW)/h]",
        "GERMANY_AVERAGE_CAPACITY_PRICE_[(EUR/MW)/h]",
        "GERMANY_MARGINAL_CAPACITY_PRICE_[(EUR/MW)/h]",
        "GERMANY_ALLOCATED_VOLUME_[MW]",
    ]
    rows: list[list[object]] = [header]
    for direction in ("POS", "NEG"):
        for index, name in enumerate(["00_04", "04_08", "08_12", "12_16", "16_20", "20_24"]):
            rows.append(
                [_serial(day), _serial(day), "aFRR", f"{direction}_{name}", 1.0, 2.0 + index, 3.0 + index, 1800.0]
            )
    parsed = parse_regelleistung_afrr_results(read_xlsx_rows(_write_xlsx(tmp_path / "afrr.xlsx", rows)))
    assert sorted(parsed["direction"].unique()) == ["down", "up"]
    up = parsed.loc[parsed["direction"] == "up"].reset_index(drop=True)
    assert up["block_hours"].tolist() == [3.0, 4.0, 4.0, 4.0, 4.0, 4.0]
    assert up["price_eur_per_mw_h"].tolist() == [2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
    assert up["marginal_price_eur_per_mw_h"].tolist() == [3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
    assert up["pricing_rule"].eq("pay_as_bid_average_benchmark").all()
    with pytest.raises(ValueError, match="missing column"):
        parse_regelleistung_afrr_results([["DATE_FROM", "PRODUCT"], [_serial(day), "POS_00_04"]])
