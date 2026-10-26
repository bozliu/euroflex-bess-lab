"""Strategy registry for live paper trading: the *same* objects the backtests use.

Built-in names: ``threshold`` (price bands on the running quarter-hour price), ``threshold-si`` (system-imbalance
bands) and ``idle``. Advanced strategies from :mod:`euroflex_bess_lab.realtime.strategies` (``forecast_mpc``,
``prism``, ``rl``, ``*_extra``) are discovered lazily: a module that is missing, or whose optional dependency
(scikit-learn, torch) is not installed, is skipped with a clear reason instead of breaking the CLI. A module can
publish explicit factories in a ``LIVE_STRATEGIES`` (or ``STRATEGIES``) mapping; otherwise every strategy class
defined in it is registered under its ``name``. ``module.path:ClassName`` always works as an explicit spec.

The perfect-foresight oracle is not tradable and is refused.
"""

from __future__ import annotations

import importlib
import inspect
import json
import math
import pkgutil
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ..replay import DecisionContext
from ..strategies.base import BaseStrategy, IdleStrategy, Strategy
from ..strategies.threshold import ThresholdStrategy
from .feeds import finite_or_none

STRATEGY_PACKAGE = "euroflex_bess_lab.realtime.strategies"
OPTIONAL_MODULES: tuple[str, ...] = ("forecast_mpc", "prism", "rl")
NOT_TRADABLE = frozenset({"oracle", "oracle-qh", "oracle_qh", "oracleqh"})
SIGNAL_BY_MECHANISM = {"current": "price_cum_eur_mwh", "new2026": "nf_imbalance_price_eur_mwh"}


def strategy_diagnostics(strategy: Any) -> dict[str, Any]:
    """Small, JSON-friendly view of a strategy's internal state for the decision journal."""
    hook = getattr(strategy, "diagnostics", None)
    if callable(hook):
        try:
            payload = dict(hook())
        except Exception:  # noqa: BLE001 - diagnostics must never break a decision
            return {}
        return {
            str(k): finite_or_none(v) if isinstance(v, float | int) and not isinstance(v, bool) else v
            for k, v in payload.items()
        }
    if isinstance(strategy, ThresholdStrategy):
        return {
            "mode": strategy.mode,
            "signal_field": strategy.signal_field,
            "lower": finite_or_none(getattr(strategy, "_lower", math.nan)),
            "upper": finite_or_none(getattr(strategy, "_upper", math.nan)),
        }
    return {}


class MechanismSwitchingStrategy(BaseStrategy):
    """Delegates to one sub-strategy per settlement mechanism (``ctx.mechanism``).

    Used for ``threshold`` with ``--mechanism auto``: the rule reads the current-formula running price
    (``price_cum_eur_mwh``) before the go-live and the new-formula running price (``nf_imbalance_price_eur_mwh``)
    after it. Before the switch it is decision-for-decision identical to the plain current-formula strategy.
    Each sub-strategy is reset the first time it becomes active.
    """

    def __init__(
        self, strategies: Mapping[str, Strategy], *, name: str = "switching", default: str = "current"
    ) -> None:
        if default not in strategies:
            raise ValueError("default mechanism must be one of the sub-strategies")
        self.strategies = dict(strategies)
        self.name = name
        self.default = default
        self.active = default
        self._initialised: set[str] = set()

    def _pick(self, ctx: DecisionContext) -> tuple[str, Strategy]:
        key = ctx.mechanism if ctx.mechanism in self.strategies else self.default
        return key, self.strategies[key]

    def reset(self, ctx: DecisionContext) -> None:
        self._initialised = set()
        key, strategy = self._pick(ctx)
        strategy.reset(ctx)
        self._initialised.add(key)
        self.active = key

    def decide(self, ctx: DecisionContext) -> float:
        key, strategy = self._pick(ctx)
        if key not in self._initialised:
            strategy.reset(ctx)
            self._initialised.add(key)
        self.active = key
        return strategy.decide(ctx)

    @property
    def signal_field(self) -> str | None:
        value = getattr(self.strategies[self.active], "signal_field", None)
        return value if isinstance(value, str) else None

    def diagnostics(self) -> dict[str, Any]:
        return {"active_mechanism": self.active, **strategy_diagnostics(self.strategies[self.active])}


@dataclass(frozen=True)
class StrategyInfo:
    name: str
    description: str
    origin: str
    factory: Callable[..., Strategy] | None = None
    available: bool = True
    reason: str = ""


def _threshold_factory(mechanism: str = "auto", **params: Any) -> Strategy:
    if "signal_field" in params or mechanism in SIGNAL_BY_MECHANISM:
        params.setdefault("signal_field", SIGNAL_BY_MECHANISM.get(mechanism, "price_cum_eur_mwh"))
        params.setdefault("name", "threshold")
        return ThresholdStrategy(**params)
    subs: dict[str, Strategy] = {
        mech: ThresholdStrategy(**{**params, "signal_field": field, "name": "threshold"})
        for mech, field in SIGNAL_BY_MECHANISM.items()
    }
    return MechanismSwitchingStrategy(subs, name="threshold")


def _threshold_si_factory(mechanism: str = "auto", **params: Any) -> Strategy:
    params.setdefault("signal_field", "si_cum_mw")
    params.setdefault("name", "threshold_si")
    return ThresholdStrategy(mode="si", **params)


def _idle_factory(mechanism: str = "auto", **params: Any) -> Strategy:
    if params:
        raise ValueError("idle takes no parameters")
    return IdleStrategy()


BUILTIN: dict[str, StrategyInfo] = {
    "threshold": StrategyInfo(
        "threshold",
        "Price bands from recent final prices vs the running quarter-hour price (ThresholdStrategy)",
        "builtin",
        _threshold_factory,
    ),
    "threshold-si": StrategyInfo(
        "threshold-si",
        "System-imbalance bands on the running SI (ThresholdStrategy mode='si')",
        "builtin",
        _threshold_si_factory,
    ),
    "idle": StrategyInfo("idle", "Never trades (zero baseline)", "builtin", _idle_factory),
}


def normalize_name(name: str) -> str:
    return re.sub(r"[\s_]+", "-", name.strip().lower())


def _snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _declared_name(cls: Any) -> str | None:
    """The strategy's ``name``: a class attribute, else the default of the constructor's ``name`` parameter."""
    declared = getattr(cls, "name", None)
    if isinstance(declared, str) and declared not in ("", "base"):
        return declared
    try:
        default = inspect.signature(cls).parameters["name"].default
    except (KeyError, TypeError, ValueError):
        return None
    return default if isinstance(default, str) and default else None


def _is_strategy_class(obj: Any, module_name: str) -> bool:
    if not inspect.isclass(obj) or obj.__module__ != module_name or inspect.isabstract(obj):
        return False
    return callable(getattr(obj, "decide", None)) and callable(getattr(obj, "reset", None))


def _candidate_modules() -> list[str]:
    names = list(OPTIONAL_MODULES)
    try:
        package = importlib.import_module(STRATEGY_PACKAGE)
        for info in pkgutil.iter_modules(getattr(package, "__path__", [])):
            if info.name.endswith("_extra") and info.name not in names:
                names.append(info.name)
    except ImportError:  # pragma: no cover
        pass
    return names


def discover_strategies() -> tuple[dict[str, StrategyInfo], dict[str, str]]:
    """Strategies from optional modules, plus ``{module: reason}`` for the ones that could not be loaded."""
    found: dict[str, StrategyInfo] = {}
    unavailable: dict[str, str] = {}
    for module_name in _candidate_modules():
        full = f"{STRATEGY_PACKAGE}.{module_name}"
        try:
            module = importlib.import_module(full)
        except ModuleNotFoundError as exc:
            if exc.name in (full, module_name):
                unavailable[module_name] = "module not present in this checkout"
            else:
                unavailable[module_name] = f"optional dependency `{exc.name}` is not installed"
            continue
        except Exception as exc:  # noqa: BLE001 - a broken optional module must not break the CLI
            unavailable[module_name] = f"import failed: {type(exc).__name__}: {exc}"
            continue
        registry = getattr(module, "LIVE_STRATEGIES", None) or getattr(module, "STRATEGIES", None)
        if isinstance(registry, Mapping) and registry:
            for key, factory in registry.items():
                if callable(factory):
                    label = normalize_name(str(key))
                    found[label] = StrategyInfo(label, f"from {module_name}", full, factory)
            continue
        classes: list[Any] = [obj for _, obj in inspect.getmembers(module) if _is_strategy_class(obj, full)]
        for cls in classes:
            label = normalize_name(_declared_name(cls) or (module_name if len(classes) == 1 else _snake(cls.__name__)))
            doc = (inspect.getdoc(cls) or "").splitlines()
            found.setdefault(
                label, StrategyInfo(label, doc[0] if doc else f"{cls.__name__} from {module_name}", full, cls)
            )
        if not any(info.origin == full for info in found.values()):
            unavailable[module_name] = "no strategy class found in module"
    return found, unavailable


def available_strategies() -> tuple[dict[str, StrategyInfo], dict[str, str]]:
    discovered, unavailable = discover_strategies()
    merged = dict(BUILTIN)
    for name, info in discovered.items():
        merged.setdefault(name, info)
    return merged, unavailable


def parse_params(items: Sequence[str] | None) -> dict[str, Any]:
    """``["lookback_qh=672", "mode=price"]`` -> ``{"lookback_qh": 672, "mode": "price"}`` (JSON values)."""
    params: dict[str, Any] = {}
    for item in items or ():
        if "=" not in item:
            raise ValueError(f"strategy parameter `{item}` must look like key=value")
        key, raw = item.split("=", 1)
        try:
            params[key.strip()] = json.loads(raw)
        except json.JSONDecodeError:
            params[key.strip()] = raw
    return params


def _call_factory(factory: Callable[..., Strategy], mechanism: str, params: Mapping[str, Any]) -> Strategy:
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError):  # pragma: no cover - builtins without signatures
        return factory(**params)
    if "mechanism" in signature.parameters and not inspect.isclass(factory):
        return factory(mechanism=mechanism, **params)
    return factory(**params)


def make_strategy(
    spec: str, *, mechanism: str = "auto", params: Mapping[str, Any] | None = None
) -> tuple[Strategy, str]:
    """Instantiate a live strategy; returns ``(strategy, label)``. Raises ``ValueError`` with the available list."""
    options = dict(params or {})
    if ":" in spec:
        module_name, _, attr = spec.partition(":")
        try:
            target = getattr(importlib.import_module(module_name), attr)
        except (ImportError, AttributeError) as exc:
            raise ValueError(f"cannot load strategy `{spec}`: {exc}") from exc
        strategy = _call_factory(target, mechanism, options)
        return strategy, str(getattr(strategy, "name", attr))
    key = normalize_name(spec)
    if key.replace("-", "") in {n.replace("-", "").replace("_", "") for n in NOT_TRADABLE}:
        raise ValueError(
            "the oracle is a perfect-foresight upper bound (it reads final prices before they are published) "
            "and cannot be traded live"
        )
    registry: dict[str, StrategyInfo]
    unavailable: dict[str, str]
    if key in BUILTIN:
        registry, unavailable = dict(BUILTIN), {}
    else:
        registry, unavailable = available_strategies()
    info = registry.get(key)
    if info is None or info.factory is None:
        lines = [f"unknown or unavailable strategy `{spec}`.", f"available: {', '.join(sorted(registry))}"]
        if unavailable:
            lines.append("skipped: " + "; ".join(f"{m} ({why})" for m, why in sorted(unavailable.items())))
        raise ValueError(" ".join(lines))
    try:
        strategy = _call_factory(info.factory, mechanism, options)
    except TypeError as exc:
        raise ValueError(
            f"strategy `{key}` ({info.origin}) cannot be built from --param values alone: {exc}. "
            "Strategies that need a fitted model should expose a factory in the module's LIVE_STRATEGIES mapping "
            "(or pass `module.path:factory`)."
        ) from exc
    return strategy, key
