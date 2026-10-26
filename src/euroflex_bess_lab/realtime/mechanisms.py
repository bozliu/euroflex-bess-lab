"""Explicit settlement-mechanism operators for the Belgian single imbalance price.

The real-time engine keeps the market *state* (system imbalance, activated volumes, product prices) separate from
the *mechanism* that maps that state to the settlement price. Two Elia mechanisms are implemented and validated
against Elia Open Data:

- :class:`CurrentAlphaMechanism`: the formula in force until the planned switch on 24 Nov 2026. If the system is
  short (SI < 0) the price is the marginal incremental price plus alpha, otherwise the marginal decremental price
  minus alpha. Reproduces 100 % of the 35,040 published 2025 quarter-hour prices (ODS134).
- :class:`NewWeightedMechanism2026`: the new design. The main component is the volume-weighted average of the aFRR
  and mFRR prices in the direction of the system imbalance; when Elia publishes an incentivising component the
  price is ``max(main_up, incentive)`` for a short system and ``min(main_down, incentive)`` for a long system.
  Reproduces 100 % of the 20,352 simulated Jan-Jul 2026 quarter-hour prices (ODS212).

All functions take column names from the public data contract (``Plan.md`` section 5).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

import numpy as np
import pandas as pd
from numpy.typing import NDArray

FloatArray = NDArray[np.float64]


def _col(frame: pd.DataFrame | Mapping[str, object], name: str) -> FloatArray:
    if name not in frame:
        raise KeyError(f"settlement mechanism needs column `{name}`")
    return np.asarray(frame[name], dtype=float)


def _optional_col(frame: pd.DataFrame | Mapping[str, object], name: str, length: int) -> FloatArray:
    if name not in frame:
        return np.full(length, np.nan)
    return np.asarray(frame[name], dtype=float)


@runtime_checkable
class SettlementMechanism(Protocol):
    """Maps a quarter-hour market state to the single imbalance price (EUR/MWh)."""

    @property
    def name(self) -> str: ...

    def price_from_components(self, frame: pd.DataFrame) -> FloatArray:
        """Settlement price per row of ``frame`` reconstructed from published components."""
        ...


@dataclass(frozen=True)
class CurrentAlphaMechanism:
    """Elia imbalance price until 24 Nov 2026: marginal price plus or minus alpha.

    Columns: ``si_mw``, ``mip_eur_mwh``, ``mdp_eur_mwh``, ``alpha_eur_mwh``.
    """

    name: str = "current"

    def price_from_components(self, frame: pd.DataFrame) -> FloatArray:
        si = _col(frame, "si_mw")
        return self.price_given_si(
            si,
            mip=_col(frame, "mip_eur_mwh"),
            mdp=_col(frame, "mdp_eur_mwh"),
            alpha=_col(frame, "alpha_eur_mwh"),
        )

    @staticmethod
    def price_given_si(si: FloatArray, *, mip: FloatArray, mdp: FloatArray, alpha: FloatArray) -> FloatArray:
        """Price for a (possibly counterfactual) system imbalance with given marginal prices and alpha."""
        si = np.asarray(si, dtype=float)
        result: FloatArray = np.where(
            si < 0.0, np.asarray(mip) + np.asarray(alpha), np.asarray(mdp) - np.asarray(alpha)
        )
        return result


@dataclass(frozen=True)
class NewWeightedMechanism2026:
    """Elia imbalance price design planned from 24 Nov 2026 (subject to CREG approval).

    Columns: ``si_mw`` and either the published ``main_up_eur_mwh`` / ``main_down_eur_mwh`` or the ingredients
    ``weight_afrr``, ``weight_mfrr``, ``afrr_price_{up,down}_eur_mwh``, ``mfrr_price_{up,down}_eur_mwh``;
    optional ``incentive_eur_mwh``.
    """

    name: str = "new2026"
    use_published_main: bool = True

    def main_components(self, frame: pd.DataFrame) -> tuple[FloatArray, FloatArray]:
        if self.use_published_main and "main_up_eur_mwh" in frame and "main_down_eur_mwh" in frame:
            return _col(frame, "main_up_eur_mwh"), _col(frame, "main_down_eur_mwh")
        return self.reconstruct_main(frame)

    @staticmethod
    def reconstruct_main(frame: pd.DataFrame) -> tuple[FloatArray, FloatArray]:
        """Volume-weighted main components from weights and product prices (missing product = zero weight)."""
        n = len(frame)
        w_a = np.nan_to_num(_optional_col(frame, "weight_afrr", n))
        w_m = np.nan_to_num(_optional_col(frame, "weight_mfrr", n))
        up = w_a * np.nan_to_num(_optional_col(frame, "afrr_price_up_eur_mwh", n)) + w_m * np.nan_to_num(
            _optional_col(frame, "mfrr_price_up_eur_mwh", n)
        )
        down = w_a * np.nan_to_num(_optional_col(frame, "afrr_price_down_eur_mwh", n)) + w_m * np.nan_to_num(
            _optional_col(frame, "mfrr_price_down_eur_mwh", n)
        )
        no_weights = (w_a + w_m) <= 0.0
        if no_weights.any():
            voaa_up = _optional_col(frame, "voaa_up_eur_mwh", n)
            voaa_down = _optional_col(frame, "voaa_down_eur_mwh", n)
            up = np.where(no_weights, voaa_up, up)
            down = np.where(no_weights, voaa_down, down)
        return up, down

    def price_from_components(self, frame: pd.DataFrame) -> FloatArray:
        main_up, main_down = self.main_components(frame)
        incentive = _optional_col(frame, "incentive_eur_mwh", len(frame))
        return self.price_given_si(_col(frame, "si_mw"), main_up=main_up, main_down=main_down, incentive=incentive)

    @staticmethod
    def price_given_si(
        si: FloatArray,
        *,
        main_up: FloatArray,
        main_down: FloatArray,
        incentive: FloatArray | None = None,
    ) -> FloatArray:
        si = np.asarray(si, dtype=float)
        short = si < 0.0
        base: FloatArray = np.where(short, np.asarray(main_up, dtype=float), np.asarray(main_down, dtype=float))
        if incentive is None:
            return base
        inc = np.asarray(incentive, dtype=float)
        has_inc = ~np.isnan(inc)
        adjusted = np.where(short, np.fmax(base, inc), np.fmin(base, inc))
        result: FloatArray = np.where(has_inc, adjusted, base)
        return result


@dataclass(frozen=True)
class PublishedPrice:
    """Uses a published price column as-is (e.g. the validated ODS134 price)."""

    column: str = "imbalance_price_eur_mwh"
    name: str = "published"

    def price_from_components(self, frame: pd.DataFrame) -> FloatArray:
        return _col(frame, self.column)


@dataclass
class MechanismValidation:
    mechanism: str
    rows: int
    compared: int
    match_rate: float
    max_abs_error: float
    mismatches: pd.DataFrame = field(repr=False)


def validate_mechanism(
    mechanism: SettlementMechanism,
    frame: pd.DataFrame,
    *,
    price_column: str = "imbalance_price_eur_mwh",
    atol: float = 0.011,
) -> MechanismValidation:
    """Compare a mechanism's reconstruction with a published price column."""
    reconstructed = mechanism.price_from_components(frame)
    published = _col(frame, price_column)
    comparable = ~(np.isnan(reconstructed) | np.isnan(published))
    close = np.isclose(reconstructed, published, atol=atol, rtol=0.0) & comparable
    compared = int(comparable.sum())
    errors = np.abs(reconstructed - published)
    mismatch_mask = comparable & ~close
    mismatches = frame.loc[mismatch_mask].copy()
    mismatches["reconstructed_eur_mwh"] = reconstructed[mismatch_mask]
    return MechanismValidation(
        mechanism=mechanism.name,
        rows=len(frame),
        compared=compared,
        match_rate=float(close.sum() / compared) if compared else float("nan"),
        max_abs_error=float(np.nanmax(errors[comparable])) if compared else float("nan"),
        mismatches=mismatches,
    )


MECHANISMS: dict[str, SettlementMechanism] = {
    "current": CurrentAlphaMechanism(),
    "new2026": NewWeightedMechanism2026(),
}


def mechanism_by_name(name: str) -> SettlementMechanism:
    try:
        return MECHANISMS[name]
    except KeyError as exc:
        raise KeyError(f"unknown settlement mechanism `{name}`; known: {sorted(MECHANISMS)}") from exc


def reconstruct_new_formula_history(qh: pd.DataFrame) -> pd.DataFrame:
    """Counterfactual new-2026 prices for periods priced under the current formula.

    Uses only current-design public components (ODS166 product prices, ODS127 activated volumes, ODS134 SI and
    marginal prices) to rebuild what the new design would have published:

    - direction from the SI sign (short -> upward products, long -> downward products);
    - weights from net activated volumes: ``w_afrr = |net aFRR| / (|net aFRR| + |net mFRR|)`` with net = up - down,
      one pair for both directions (matches 99.7 % of ODS212 weights; direction-specific volumes match ~94.5 %);
    - mFRR price = the most expensive activated mFRR price in the direction (max up / min down);
    - main component = weighted average; without activation in the direction, the current marginal price stands
      in for the value of avoided activation;
    - the rare incentivising component (needs FRCE vs the L2 target) is not reconstructed.

    First-order counterfactual: it reprices the historical system state and ignores how market parties would have
    reacted to the new price. Validate it with :func:`validate_mechanism` on the Jan-Jul 2026 overlap with ODS212.
    """
    n = len(qh)
    si = _col(qh, "si_mw")

    def opt(name: str) -> FloatArray:
        return _optional_col(qh, name, n)

    vol_afrr = np.abs(np.nan_to_num(opt("afrr_up_mw")) - np.nan_to_num(opt("afrr_down_mw")))
    vol_mfrr = np.abs(
        np.nan_to_num(opt("mfrr_sa_up_mw"))
        + np.nan_to_num(opt("mfrr_da_up_mw"))
        - np.nan_to_num(opt("mfrr_sa_down_mw"))
        - np.nan_to_num(opt("mfrr_da_down_mw"))
    )
    total = vol_afrr + vol_mfrr
    with np.errstate(invalid="ignore", divide="ignore"):
        w_afrr = np.where(total > 0.0, vol_afrr / total, np.nan)
    w_mfrr = np.where(total > 0.0, 1.0 - w_afrr, np.nan)
    with np.errstate(all="ignore"):
        mfrr_up = np.fmax(opt("mfrr_sa_price_up_eur_mwh"), opt("mfrr_da_price_up_eur_mwh"))
        mfrr_down = np.fmin(opt("mfrr_sa_price_down_eur_mwh"), opt("mfrr_da_price_down_eur_mwh"))
    frame = pd.DataFrame(
        {
            "si_mw": si,
            "weight_afrr": w_afrr,
            "weight_mfrr": w_mfrr,
            "afrr_price_up_eur_mwh": opt("afrr_price_up_eur_mwh"),
            "afrr_price_down_eur_mwh": opt("afrr_price_down_eur_mwh"),
            "mfrr_price_up_eur_mwh": mfrr_up,
            "mfrr_price_down_eur_mwh": mfrr_down,
            "voaa_up_eur_mwh": opt("mip_eur_mwh"),
            "voaa_down_eur_mwh": opt("mdp_eur_mwh"),
        },
        index=qh.index,
    )
    main_up, main_down = NewWeightedMechanism2026.reconstruct_main(frame)
    frame["main_up_eur_mwh"] = main_up
    frame["main_down_eur_mwh"] = main_down
    frame["imbalance_price_eur_mwh"] = NewWeightedMechanism2026.price_given_si(
        si, main_up=main_up, main_down=main_down, incentive=None
    )
    if "timestamp_utc" in qh:
        frame.insert(0, "timestamp_utc", qh["timestamp_utc"].to_numpy())
    return frame
