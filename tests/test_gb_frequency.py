"""Dynamic response delivery curves (NESO Service Terms Schedule 2 Table 1) and per-SP response energy."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.public_data.gb.frequency import (
    EXPECTED_SAMPLES_PER_SP,
    delivery_fraction,
    product_fraction,
    response_energy_per_sp,
)
from euroflex_bess_lab.public_data.gb.timeutils import sp_grid

# Service Terms Schedule 2, Table 1 (frequency in Hz -> DC, DM, DR delivered fraction; + export, - import)
TABLE_1 = [
    (49.5, 1.0, 1.0, 1.0),
    (49.8, 0.05, 1.0, 1.0),
    (49.9, 0.023, 0.05, 0.4595),
    (49.985, 0.0, 0.0, 0.0),
    (50.015, 0.0, 0.0, 0.0),
    (50.1, -0.023, -0.05, -0.4595),
    (50.2, -0.05, -1.0, -1.0),
    (50.5, -1.0, -1.0, -1.0),
]


@pytest.mark.parametrize(("freq", "dc", "dm", "dr"), TABLE_1)
def test_delivery_curves_reproduce_service_terms_table(freq: float, dc: float, dm: float, dr: float) -> None:
    dev = freq - 50.0
    assert float(delivery_fraction("DC", dev)) == pytest.approx(dc, abs=6e-4)
    assert float(delivery_fraction("DM", dev)) == pytest.approx(dm, abs=6e-4)
    assert float(delivery_fraction("DR", dev)) == pytest.approx(dr, abs=6e-4)


def test_curves_saturate_and_products_are_one_sided() -> None:
    assert float(delivery_fraction("DC", -0.8)) == 1.0
    assert float(delivery_fraction("DR", 0.9)) == -1.0
    assert float(product_fraction("DRL", -0.1)) > 0 and float(product_fraction("DRH", -0.1)) == 0.0
    assert float(product_fraction("DRH", 0.1)) > 0 and float(product_fraction("DRL", 0.1)) == 0.0
    with pytest.raises(KeyError):
        product_fraction("FFR", 0.1)


def _frequency(starts: pd.DatetimeIndex, dev_mhz: np.ndarray) -> pd.DataFrame:
    stamps = pd.date_range(starts[0], periods=len(dev_mhz), freq="15s", tz="UTC")
    return pd.DataFrame({"timestamp_utc": stamps, "frequency_dev_mhz": dev_mhz.astype(np.int16)})


def test_constant_deviation_gives_closed_form_energy() -> None:
    starts = sp_grid("2025-06-10T00:00Z", "2025-06-10T01:00Z")
    dev = np.full(2 * EXPECTED_SAMPLES_PER_SP, -100)  # 49.9 Hz for one hour
    energy = response_energy_per_sp(_frequency(starts, dev), starts)
    assert energy["drl_mwh_per_mw"].tolist() == pytest.approx([0.5 * 0.4595] * 2, abs=1e-3)
    assert energy["dml_mwh_per_mw"].tolist() == pytest.approx([0.5 * 0.05] * 2, abs=1e-6)
    assert energy["drh_mwh_per_mw"].tolist() == [0.0, 0.0]
    assert energy["freq_mean_dev_mhz"].tolist() == pytest.approx([-100.0, -100.0])
    assert energy["n_samples"].tolist() == [120, 120]


def test_sparse_periods_are_nan_and_misaligned_grid_is_rejected() -> None:
    starts = sp_grid("2025-06-10T00:00Z", "2025-06-10T01:30Z")
    dev = np.full(2 * EXPECTED_SAMPLES_PER_SP + 30, 50)  # third SP has only 30 samples
    energy = response_energy_per_sp(_frequency(starts, dev), starts)
    assert np.isfinite(energy["dch_mwh_per_mw"].iloc[:2]).all()
    assert np.isnan(energy["dch_mwh_per_mw"].iloc[2])
    with pytest.raises(ValueError):
        response_energy_per_sp(_frequency(starts, dev), starts.delete(1))
