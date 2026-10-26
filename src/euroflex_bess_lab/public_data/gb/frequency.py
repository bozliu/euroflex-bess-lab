"""Dynamic response delivery curves (DC/DM/DR) and per-settlement-period response energy from system frequency.

Delivery curves follow the NESO Response Services Service Terms (Schedule 2, Table 1 and the service parameter
table): no delivery inside the +/-0.015 Hz deadband, linear interpolation between the points below and saturation
beyond the outermost point (para 6.7). Positive values are export (low-frequency response, battery discharges),
negative values import (high-frequency response, battery charges).

=====  =============================  ==============================  ======================
       deadband edge (0 %)            knee point                      full delivery (100 %)
=====  =============================  ==============================  ======================
DC     +/-0.015 Hz                    +/-0.2 Hz at 5 %                +/-0.5 Hz
DM     +/-0.015 Hz                    +/-0.1 Hz at 5 %                +/-0.2 Hz
DR     +/-0.015 Hz                    none (linear)                   +/-0.2 Hz
=====  =============================  ==============================  ======================

The per-SP energy table integrates these curves over BMRS 15-second frequency samples (each sample held for 15 s),
giving MWh delivered per MW of contracted service in each settlement period. This is an approximation of the
1-second performance-monitoring view: short excursions between samples are missed, which mostly affects DC.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .timeutils import SP_MINUTES

NOMINAL_HZ = 50.0
SAMPLE_SECONDS = 15.0
EXPECTED_SAMPLES_PER_SP = int(SP_MINUTES * 60 / SAMPLE_SECONDS)
MIN_SAMPLES_PER_SP = EXPECTED_SAMPLES_PER_SP // 2

# (frequency deviation in Hz, delivered fraction of contracted MW); + = export, - = import.
DELIVERY_CURVES: dict[str, tuple[tuple[float, float], ...]] = {
    "DC": ((-0.5, 1.0), (-0.2, 0.05), (-0.015, 0.0), (0.015, 0.0), (0.2, -0.05), (0.5, -1.0)),
    "DM": ((-0.2, 1.0), (-0.1, 0.05), (-0.015, 0.0), (0.015, 0.0), (0.1, -0.05), (0.2, -1.0)),
    "DR": ((-0.2, 1.0), (-0.015, 0.0), (0.015, 0.0), (0.2, -1.0)),
}
SERVICE_DURATION_HOURS: dict[str, float] = {"DC": 0.25, "DM": 0.5, "DR": 1.0}
PRODUCTS: tuple[str, ...] = ("DCL", "DCH", "DML", "DMH", "DRL", "DRH")


def product_service_direction(product: str) -> tuple[str, str]:
    if product not in PRODUCTS:
        raise KeyError(f"unknown dynamic response product {product!r}; expected one of {PRODUCTS}")
    return product[:2], "low" if product.endswith("L") else "high"


def delivery_fraction(service: str, deviation_hz: np.ndarray | float) -> np.ndarray:
    """Signed delivered fraction of contracted MW for a frequency deviation (Hz from 50 Hz)."""

    points = DELIVERY_CURVES[service]
    xp = np.array([p[0] for p in points])
    fp = np.array([p[1] for p in points])
    return np.interp(np.asarray(deviation_hz, dtype=float), xp, fp, left=fp[0], right=fp[-1])


def product_fraction(product: str, deviation_hz: np.ndarray | float) -> np.ndarray:
    """Unsigned delivered fraction for a directional product (DCL exports below 50 Hz, DCH imports above)."""

    service, direction = product_service_direction(product)
    signed = delivery_fraction(service, deviation_hz)
    return np.clip(signed, 0.0, None) if direction == "low" else np.clip(-signed, 0.0, None)


def response_energy_per_sp(frequency: pd.DataFrame, sp_starts: pd.DatetimeIndex) -> pd.DataFrame:
    """Per-SP response energy (MWh per contracted MW) for each product plus frequency statistics.

    ``frequency`` needs ``timestamp_utc`` and ``frequency_dev_mhz``. Each SP uses the mean delivered fraction over
    its samples times 0.5 h (missing samples are assumed to behave like the observed ones); SPs with fewer than
    half of the expected 120 samples are NaN.
    """

    starts = pd.DatetimeIndex(pd.to_datetime(sp_starts, utc=True)).as_unit("ns")
    out = pd.DataFrame({"timestamp_utc": starts})
    if len(starts) == 0:
        return out
    step_ns = pd.Timedelta(minutes=SP_MINUTES).value
    if len(starts) > 1 and not np.all(np.diff(starts.asi8) == step_ns):
        raise ValueError("sp_starts must be a regular 30-minute grid")
    n_sp = len(starts)
    ts_ns = pd.DatetimeIndex(pd.to_datetime(frequency["timestamp_utc"], utc=True)).as_unit("ns").asi8
    dev_hz = frequency["frequency_dev_mhz"].to_numpy(dtype=float) / 1000.0
    idx = np.floor_divide(ts_ns - starts.asi8[0], step_ns)
    valid = (idx >= 0) & (idx < n_sp)
    bins = idx[valid].astype(np.int64)
    dev = dev_hz[valid]
    counts = np.bincount(bins, minlength=n_sp).astype(float)
    enough = counts >= MIN_SAMPLES_PER_SP
    safe = np.where(counts > 0, counts, 1.0)
    hours = SP_MINUTES / 60.0
    for product in PRODUCTS:
        frac = product_fraction(product, dev)
        energy = np.bincount(bins, weights=frac, minlength=n_sp) / safe * hours
        out[f"{product.lower()}_mwh_per_mw"] = np.where(enough, energy, np.nan).astype("float32")
    sums = np.bincount(bins, weights=dev, minlength=n_sp)
    sq = np.bincount(bins, weights=dev * dev, minlength=n_sp)
    mean = sums / safe
    var = np.maximum(sq / safe - mean * mean, 0.0)
    out["freq_mean_dev_mhz"] = np.where(enough, mean * 1000.0, np.nan).astype("float32")
    out["freq_std_mhz"] = np.where(enough, np.sqrt(var) * 1000.0, np.nan).astype("float32")
    minimum = np.full(n_sp, np.inf)
    maximum = np.full(n_sp, -np.inf)
    np.minimum.at(minimum, bins, dev)
    np.maximum.at(maximum, bins, dev)
    out["freq_min_dev_mhz"] = np.where(enough, minimum * 1000.0, np.nan).astype("float32")
    out["freq_max_dev_mhz"] = np.where(enough, maximum * 1000.0, np.nan).astype("float32")
    outside = np.bincount(bins, weights=(np.abs(dev) > 0.2).astype(float), minlength=n_sp) / safe
    out["share_outside_200mhz"] = np.where(enough, outside, np.nan).astype("float32")
    out["n_samples"] = counts.astype("int16")
    return out
