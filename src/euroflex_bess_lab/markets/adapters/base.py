from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd

from ...config import BacktestConfig
from ...data.io import load_price_series
from ...reserve import ReserveProduct
from ...types import PriceSeries


class BidConstraint(ABC):
    name: str

    @abstractmethod
    def validate(self, dispatch: pd.DataFrame) -> list[str]:
        """Return validation issues for a dispatch profile."""


class SettlementRule(ABC):
    name: str
    settlement_basis: str

    @abstractmethod
    def settle_imbalance(self, dispatch: pd.DataFrame, *, dt_hours: float) -> pd.Series:
        """Return imbalance settlement cashflows aligned to the dispatch frame."""


class PenaltyRule(ABC):
    name: str

    @abstractmethod
    def penalties(self, dispatch: pd.DataFrame) -> pd.Series:
        """Return per-interval penalties."""


class ActivationRule(ABC):
    name: str

    @abstractmethod
    def activation_volume(self, dispatch: pd.DataFrame) -> pd.Series:
        """Return activated volume under the market-specific rule."""


class NoImbalanceSettlement(SettlementRule):
    name = "NoImbalanceSettlement"
    settlement_basis = "no_imbalance_component"

    def settle_imbalance(self, dispatch: pd.DataFrame, *, dt_hours: float) -> pd.Series:
        return pd.Series(0.0, index=dispatch.index, dtype=float)


class SinglePriceImbalanceSettlement(SettlementRule):
    name = "SinglePriceImbalanceSettlement"
    settlement_basis = "single_price_eur_per_mwh"

    def __init__(self, *, price_column: str = "imbalance_actual_price_eur_per_mwh") -> None:
        self.price_column = price_column

    def settle_imbalance(self, dispatch: pd.DataFrame, *, dt_hours: float) -> pd.Series:
        if "imbalance_mw" not in dispatch.columns:
            raise ValueError("Dispatch frame must include imbalance_mw for imbalance settlement")
        if self.price_column not in dispatch.columns:
            raise ValueError(f"Dispatch frame must include {self.price_column} for imbalance settlement")
        return dispatch["imbalance_mw"] * dispatch[self.price_column] * dt_hours


class DualPriceImbalanceSettlement(SettlementRule):
    name = "DualPriceImbalanceSettlement"
    settlement_basis = "dual_price_shortage_surplus"

    def __init__(
        self,
        *,
        positive_column: str = "imbalance_surplus_price_eur_per_mwh",
        negative_column: str = "imbalance_shortage_price_eur_per_mwh",
    ) -> None:
        self.positive_column = positive_column
        self.negative_column = negative_column

    def settle_imbalance(self, dispatch: pd.DataFrame, *, dt_hours: float) -> pd.Series:
        if "imbalance_mw" not in dispatch.columns:
            raise ValueError("Dispatch frame must include imbalance_mw for imbalance settlement")
        if self.positive_column not in dispatch.columns or self.negative_column not in dispatch.columns:
            raise ValueError(
                "Dispatch frame must include both surplus and shortage imbalance price columns "
                f"({self.positive_column}, {self.negative_column})"
            )
        imbalance = dispatch["imbalance_mw"]
        prices = dispatch[self.positive_column].where(imbalance >= 0.0, dispatch[self.negative_column])
        return imbalance * prices * dt_hours


@dataclass(frozen=True)
class LoadedMarketData:
    day_ahead: PriceSeries
    imbalance: PriceSeries | None = None
    fcr_capacity: PriceSeries | None = None
    afrr_capacity_up: PriceSeries | None = None
    afrr_capacity_down: PriceSeries | None = None
    afrr_activation_price_up: PriceSeries | None = None
    afrr_activation_price_down: PriceSeries | None = None
    afrr_activation_ratio_up: PriceSeries | None = None
    afrr_activation_ratio_down: PriceSeries | None = None


class MarketAdapter(ABC):
    market_id: str
    timezone: str
    resolution_minutes: int = 15
    supported_workflows: tuple[str, ...]
    day_ahead_zone: str
    imbalance_zone: str
    fcr_zone: str | None = None
    afrr_zone: str | None = None

    @abstractmethod
    def load_actuals(self, config: BacktestConfig) -> LoadedMarketData:
        """Load normalized actual market data for the configured delivery window."""

    @abstractmethod
    def validate_timing(self, config: BacktestConfig) -> None:
        """Validate market-specific timing assumptions."""

    @abstractmethod
    def settlement_engine(self, workflow: str) -> SettlementRule:
        """Return the settlement engine for the selected workflow."""

    @abstractmethod
    def default_benchmarks(self) -> tuple[str, ...]:
        """Return the provider names exposed as market benchmarks."""

    @abstractmethod
    def supported_reserve_products(self) -> tuple[str, ...]:
        """Return reserve products available for benchmark workflows."""

    @abstractmethod
    def build_reserve_product(self, config: BacktestConfig) -> ReserveProduct | None:
        """Return a reserve product object for reserve-aware workflows."""

    def decision_schedule(self, config: BacktestConfig) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        for delivery_date in pd.date_range(
            config.timing.delivery_start_date, config.timing.delivery_end_date, freq="D"
        ):
            previous_day = (delivery_date - pd.Timedelta(days=1)).date()
            gate_closure_local = pd.Timestamp(
                f"{previous_day} {config.timing.day_ahead_gate_closure_local}",
                tz=self.timezone,
            )
            rows.append(
                {
                    "market_id": self.market_id,
                    "delivery_date_local": str(delivery_date.date()),
                    "day_ahead_gate_closure_local": gate_closure_local,
                    "day_ahead_gate_closure_utc": gate_closure_local.tz_convert("UTC"),
                }
            )
        return pd.DataFrame(rows)

    def gate_closure_definition(self, config: BacktestConfig) -> str:
        return (
            f"D-1 {config.timing.day_ahead_gate_closure_local} local gate closure "
            f"({self.timezone}, 15-minute evaluation grid)"
        )

    def load_input_series(
        self,
        *,
        path,
        name: str,
        market: str,
        zone: str,
        source: str = "configured_input",
    ) -> PriceSeries:
        return load_price_series(
            path,
            name=name,
            market=market,
            zone=zone,
            source=source,
            timezone=self.timezone,
        )

    def settlement_metadata(self, config: BacktestConfig) -> dict[str, object]:
        execution_workflow = config.execution_workflow
        metadata: dict[str, object] = {
            "market_id": self.market_id,
            "market_timezone": self.timezone,
            "gate_closure_definition": self.gate_closure_definition(config),
            "settlement_basis": self.settlement_engine(execution_workflow).settlement_basis,
        }
        reserve_product = self.build_reserve_product(config)
        if reserve_product is not None:
            metadata.update(
                {
                    "reserve_product_id": reserve_product.product_id,
                    "reserve_settlement_mode": reserve_product.settlement_assumption.settlement_mode,
                    "reserve_activation_mode": reserve_product.activation_assumption.activation_mode,
                    "reserve_sustain_duration_minutes": reserve_product.sustain_duration_minutes,
                    "simplified_product_logic": reserve_product.metadata.get("simplified_product_logic", True),
                }
            )
        else:
            metadata.update(
                {
                    "reserve_product_id": None,
                    "reserve_settlement_mode": None,
                    "reserve_activation_mode": None,
                    "reserve_sustain_duration_minutes": None,
                    "simplified_product_logic": None,
                }
            )
        if config.execution_workflow == "da_plus_afrr":
            metadata["afrr_supported"] = True
        return metadata
