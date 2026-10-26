"""GB time conventions: settlement days/periods across clock changes, EFA days/blocks, EAC gate closure."""

from __future__ import annotations

import pandas as pd
import pytest

from euroflex_bess_lab.public_data.gb.timeutils import (
    eac_gate_closure_utc,
    efa_date_block,
    efa_day_start_utc,
    latest_uk_midnight_utc,
    settlement_date_period,
    settlement_grid,
    sp_grid,
    sp_start_from_date_period,
    uk_midnight_utc,
)


def test_settlement_days_have_46_48_50_periods_around_clock_changes() -> None:
    grid = settlement_grid("2026-03-28T00:00Z", "2026-03-31T00:00Z")
    counts = grid.groupby("settlement_date")["settlement_period"].agg(["min", "max", "count"])
    assert counts.loc["2026-03-28"].tolist() == [1, 48, 48]
    assert counts.loc["2026-03-29"].tolist() == [1, 46, 46]  # spring forward
    autumn = settlement_grid("2025-10-25T23:00Z", "2025-10-27T00:00Z")
    counts = autumn.groupby("settlement_date")["settlement_period"].agg(["min", "max", "count"])
    assert counts.loc["2025-10-26"].tolist() == [1, 50, 50]  # clocks go back


def test_period_start_round_trip_and_bst_offset() -> None:
    assert sp_start_from_date_period("2025-06-10", 1) == pd.Timestamp("2025-06-09T23:00Z")
    assert sp_start_from_date_period("2025-01-10", 1) == pd.Timestamp("2025-01-10T00:00Z")
    assert sp_start_from_date_period("2025-10-26", 50) == pd.Timestamp("2025-10-26T23:30Z")
    stamps = sp_grid("2025-06-09T23:00Z", "2025-06-10T23:00Z")
    dates, periods = settlement_date_period(stamps)
    assert set(dates) == {"2025-06-10"}
    assert periods.tolist() == list(range(1, 49))
    with pytest.raises(ValueError):
        sp_start_from_date_period("2025-06-10", 51)
    with pytest.raises(ValueError):
        sp_grid("2025-06-10T00:10Z", "2025-06-11T00:00Z")


def test_efa_blocks_follow_uk_local_time_including_3_and_5_hour_blocks() -> None:
    # spring: EFA day 2026-03-29 block 1 is 23:00 GMT -> 03:00 BST = 3 hours
    stamps = pd.DatetimeIndex(["2026-03-28T23:00Z", "2026-03-29T01:59Z", "2026-03-29T02:00Z", "2026-03-29T06:00Z"])
    dates, blocks = efa_date_block(stamps)
    assert dates.tolist() == ["2026-03-29"] * 4
    assert blocks.tolist() == [1, 1, 2, 3]
    # autumn: EFA day 2025-10-26 block 1 is 23:00 BST -> 03:00 GMT = 5 hours
    stamps = pd.DatetimeIndex(["2025-10-25T22:00Z", "2025-10-26T02:59Z", "2025-10-26T03:00Z", "2025-10-26T22:59Z"])
    dates, blocks = efa_date_block(stamps)
    assert dates.tolist() == ["2025-10-26"] * 4
    assert blocks.tolist() == [1, 1, 2, 6]
    assert efa_day_start_utc("2025-06-10") == pd.Timestamp("2025-06-09T22:00Z")
    assert efa_day_start_utc("2025-01-10") == pd.Timestamp("2025-01-09T23:00Z")


def test_eac_gate_closure_is_14_uk_on_the_previous_day() -> None:
    assert eac_gate_closure_utc("2025-06-10") == pd.Timestamp("2025-06-09T13:00Z")
    assert eac_gate_closure_utc("2025-01-10") == pd.Timestamp("2025-01-09T14:00Z")


def test_snapshot_end_is_latest_uk_midnight() -> None:
    assert latest_uk_midnight_utc(pd.Timestamp("2025-06-10T22:30Z")) == pd.Timestamp("2025-06-09T23:00Z")
    assert latest_uk_midnight_utc(pd.Timestamp("2025-06-10T23:30Z")) == pd.Timestamp("2025-06-10T23:00Z")
    assert uk_midnight_utc("2025-12-01") == pd.Timestamp("2025-12-01T00:00Z")
