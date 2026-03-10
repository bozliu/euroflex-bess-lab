from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ReserveSettlementAssumption(BaseModel):
    settlement_mode: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class CapacityOnlyReserveSettlement(ReserveSettlementAssumption):
    settlement_mode: Literal["capacity_only"] = "capacity_only"


class CapacityPlusActivationReserveSettlement(ReserveSettlementAssumption):
    settlement_mode: Literal["capacity_plus_activation_expected_value"] = "capacity_plus_activation_expected_value"


class NoActivationAssumption(BaseModel):
    activation_mode: Literal["none"] = "none"
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExpectedActivationAssumption(BaseModel):
    activation_mode: Literal["expected_value"] = "expected_value"
    metadata: dict[str, Any] = Field(default_factory=dict)


class ReserveProduct(BaseModel):
    product_id: str
    market_id: str
    price_unit: str = "EUR/MW/h"
    symmetry_required: bool = False
    sustain_duration_minutes: int
    settlement_assumption: ReserveSettlementAssumption
    activation_assumption: NoActivationAssumption | ExpectedActivationAssumption
    metadata: dict[str, Any] = Field(default_factory=dict)


class SymmetricCapacityReserveProduct(ReserveProduct):
    product_id: Literal["fcr_symmetric"] = "fcr_symmetric"
    symmetry_required: Literal[True] = True


class AfrrAsymmetricReserveProduct(ReserveProduct):
    product_id: Literal["afrr_asymmetric"] = "afrr_asymmetric"
    symmetry_required: Literal[False] = False
