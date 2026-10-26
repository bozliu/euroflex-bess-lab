"""Strategy protocol for the minute-level engine."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..replay import DecisionContext


@runtime_checkable
class Strategy(Protocol):
    """A real-time policy: returns the requested power (MW, + = discharge) for minute ``ctx.t``.

    The simulator clips the request to power and SOC limits. ``reset`` is called once before the first minute.
    Strategies must be deterministic given the visible data (seed any randomness in ``reset``).
    """

    name: str

    def reset(self, ctx: DecisionContext) -> None: ...

    def decide(self, ctx: DecisionContext) -> float: ...


class BaseStrategy:
    """Convenience base class with a no-op ``reset``."""

    name: str = "base"

    def reset(self, ctx: DecisionContext) -> None:  # noqa: B027 - intentional no-op hook
        return None

    def decide(self, ctx: DecisionContext) -> float:
        raise NotImplementedError


class IdleStrategy(BaseStrategy):
    """Never trades. Useful as a zero baseline and in tests."""

    name = "idle"

    def decide(self, ctx: DecisionContext) -> float:
        return 0.0
