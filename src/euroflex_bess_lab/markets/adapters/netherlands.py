from __future__ import annotations

from ...config import BacktestConfig
from ...reserve import (
    AfrrAsymmetricReserveProduct,
    CapacityOnlyReserveSettlement,
    CapacityPlusActivationReserveSettlement,
    ExpectedActivationAssumption,
    NoActivationAssumption,
    SymmetricCapacityReserveProduct,
)
from .base import DualPriceImbalanceSettlement, LoadedMarketData, MarketAdapter, NoImbalanceSettlement, SettlementRule


class NetherlandsMarketAdapter(MarketAdapter):
    market_id = "netherlands"
    timezone = "Europe/Amsterdam"
    resolution_minutes = 15
    supported_workflows = ("da_only", "da_plus_imbalance", "da_plus_fcr", "da_plus_afrr")
    day_ahead_zone = "10YNL----------L"
    imbalance_zone = "10YNL----------L"
    fcr_zone = "10YNL----------L"
    afrr_zone = "10YNL----------L"

    def load_actuals(self, config: BacktestConfig) -> LoadedMarketData:
        day_ahead = self.load_input_series(
            path=config.data.day_ahead.actual_path,
            name="day_ahead_actual",
            market="day_ahead",
            zone=self.day_ahead_zone,
        )
        imbalance = None
        if config.data.imbalance is not None:
            imbalance = self.load_input_series(
                path=config.data.imbalance.actual_path,
                name="imbalance_actual",
                market="imbalance",
                zone=self.imbalance_zone,
            )
        fcr_capacity = None
        if config.data.fcr_capacity is not None:
            fcr_capacity = self.load_input_series(
                path=config.data.fcr_capacity.actual_path,
                name="fcr_capacity_actual",
                market="fcr_capacity",
                zone=self.fcr_zone or self.day_ahead_zone,
            )
        afrr_capacity_up = None
        afrr_capacity_down = None
        afrr_activation_price_up = None
        afrr_activation_price_down = None
        afrr_activation_ratio_up = None
        afrr_activation_ratio_down = None
        if config.data.afrr_capacity_up is not None:
            afrr_capacity_up = self.load_input_series(
                path=config.data.afrr_capacity_up.actual_path,
                name="afrr_capacity_up_actual",
                market="afrr_capacity_up",
                zone=self.afrr_zone or self.day_ahead_zone,
            )
        if config.data.afrr_capacity_down is not None:
            afrr_capacity_down = self.load_input_series(
                path=config.data.afrr_capacity_down.actual_path,
                name="afrr_capacity_down_actual",
                market="afrr_capacity_down",
                zone=self.afrr_zone or self.day_ahead_zone,
            )
        if config.data.afrr_activation_price_up is not None:
            afrr_activation_price_up = self.load_input_series(
                path=config.data.afrr_activation_price_up.actual_path,
                name="afrr_activation_price_up_actual",
                market="afrr_activation_price_up",
                zone=self.afrr_zone or self.day_ahead_zone,
            )
        if config.data.afrr_activation_price_down is not None:
            afrr_activation_price_down = self.load_input_series(
                path=config.data.afrr_activation_price_down.actual_path,
                name="afrr_activation_price_down_actual",
                market="afrr_activation_price_down",
                zone=self.afrr_zone or self.day_ahead_zone,
            )
        if config.data.afrr_activation_ratio_up is not None:
            afrr_activation_ratio_up = self.load_input_series(
                path=config.data.afrr_activation_ratio_up.actual_path,
                name="afrr_activation_ratio_up_actual",
                market="afrr_activation_ratio_up",
                zone=self.afrr_zone or self.day_ahead_zone,
            )
        if config.data.afrr_activation_ratio_down is not None:
            afrr_activation_ratio_down = self.load_input_series(
                path=config.data.afrr_activation_ratio_down.actual_path,
                name="afrr_activation_ratio_down_actual",
                market="afrr_activation_ratio_down",
                zone=self.afrr_zone or self.day_ahead_zone,
            )
        return LoadedMarketData(
            day_ahead=day_ahead,
            imbalance=imbalance,
            fcr_capacity=fcr_capacity,
            afrr_capacity_up=afrr_capacity_up,
            afrr_capacity_down=afrr_capacity_down,
            afrr_activation_price_up=afrr_activation_price_up,
            afrr_activation_price_down=afrr_activation_price_down,
            afrr_activation_ratio_up=afrr_activation_ratio_up,
            afrr_activation_ratio_down=afrr_activation_ratio_down,
        )

    def validate_timing(self, config: BacktestConfig) -> None:
        if config.timing.timezone != self.timezone:
            raise ValueError(f"Netherlands adapter requires timezone={self.timezone}")
        if config.timing.resolution_minutes != self.resolution_minutes:
            raise ValueError("Netherlands adapter only supports 15-minute resolution")
        if config.execution_workflow == "da_plus_afrr":
            raise ValueError(
                "Netherlands adapter exposes an aFRR extension point, but da_plus_afrr is not yet supported"
            )

    def settlement_engine(self, workflow: str) -> SettlementRule:
        if workflow == "da_only":
            return NoImbalanceSettlement()
        if workflow == "da_plus_imbalance":
            return DualPriceImbalanceSettlement()
        if workflow in {"da_plus_fcr", "da_plus_afrr"}:
            return NoImbalanceSettlement()
        raise ValueError(f"Unsupported Netherlands workflow: {workflow}")

    def default_benchmarks(self) -> tuple[str, ...]:
        return ("perfect_foresight", "persistence", "csv")

    def supported_reserve_products(self) -> tuple[str, ...]:
        return ("fcr_symmetric", "afrr_asymmetric")

    def build_reserve_product(
        self, config: BacktestConfig
    ) -> SymmetricCapacityReserveProduct | AfrrAsymmetricReserveProduct | None:
        if config.execution_workflow == "da_plus_fcr" and config.fcr is not None:
            return SymmetricCapacityReserveProduct(
                market_id=self.market_id,
                sustain_duration_minutes=config.fcr.sustain_duration_minutes,
                settlement_assumption=CapacityOnlyReserveSettlement(),
                activation_assumption=NoActivationAssumption(),
                metadata={
                    "operator": "TenneT",
                    "zone": self.fcr_zone or self.day_ahead_zone,
                    "simplified_product_logic": config.fcr.simplified_product_logic,
                },
            )
        if config.execution_workflow == "da_plus_afrr" and config.afrr is not None:
            return AfrrAsymmetricReserveProduct(
                market_id=self.market_id,
                sustain_duration_minutes=config.afrr.sustain_duration_minutes,
                settlement_assumption=CapacityPlusActivationReserveSettlement(),
                activation_assumption=ExpectedActivationAssumption(),
                metadata={
                    "operator": "TenneT",
                    "zone": self.afrr_zone or self.day_ahead_zone,
                    "simplified_product_logic": config.afrr.simplified_product_logic,
                    "status": "not_yet_supported",
                },
            )
        return None
