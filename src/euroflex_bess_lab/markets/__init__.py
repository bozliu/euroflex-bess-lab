"""Public market-facing API.

Import market contracts through ``euroflex_bess_lab.markets`` or the package root.
Implementation details live under ``euroflex_bess_lab.markets.adapters``.
"""

from .adapters import (
    ActivationRule,
    BelgiumMarketAdapter,
    BidConstraint,
    DualPriceImbalanceSettlement,
    LoadedMarketData,
    MarketAdapter,
    MarketRegistry,
    NetherlandsMarketAdapter,
    NoImbalanceSettlement,
    PenaltyRule,
    SettlementRule,
    SinglePriceImbalanceSettlement,
)

__all__ = [
    "ActivationRule",
    "BelgiumMarketAdapter",
    "BidConstraint",
    "DualPriceImbalanceSettlement",
    "LoadedMarketData",
    "MarketAdapter",
    "MarketRegistry",
    "NetherlandsMarketAdapter",
    "NoImbalanceSettlement",
    "PenaltyRule",
    "SettlementRule",
    "SinglePriceImbalanceSettlement",
]
