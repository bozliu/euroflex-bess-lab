from __future__ import annotations

from .base import MarketAdapter
from .belgium import BelgiumMarketAdapter
from .germany import GermanyMarketAdapter
from .netherlands import NetherlandsMarketAdapter


class MarketRegistry:
    _ADAPTERS: dict[str, MarketAdapter] = {
        "belgium": BelgiumMarketAdapter(),
        "netherlands": NetherlandsMarketAdapter(),
        "germany": GermanyMarketAdapter(),
    }

    @classmethod
    def get(cls, market_id: str) -> MarketAdapter:
        try:
            return cls._ADAPTERS[market_id]
        except KeyError as exc:
            raise ValueError(f"Unsupported market adapter: {market_id}") from exc

    @classmethod
    def all(cls) -> list[MarketAdapter]:
        return list(cls._ADAPTERS.values())

    @classmethod
    def supported_market_ids(cls) -> tuple[str, ...]:
        return tuple(cls._ADAPTERS)
