from __future__ import annotations

from dataclasses import dataclass

from .config import BacktestConfig
from .forecasts import (
    CsvForecastProvider,
    CustomPythonForecastProvider,
    ForecastProvider,
    PerfectForesightForecastProvider,
    PersistenceForecastProvider,
)
from .markets import MarketRegistry


@dataclass(frozen=True)
class BenchmarkDefinition:
    benchmark_name: str
    benchmark_family: str
    market_id: str
    workflow: str
    provider_name: str
    run_scope: str
    auditable: bool
    is_oracle: bool


class BenchmarkRegistry:
    @classmethod
    def resolve(
        cls,
        market_id: str,
        workflow: str,
        provider_name: str,
        *,
        run_scope: str,
        benchmark_suffix: str | None = None,
    ) -> BenchmarkDefinition:
        MarketRegistry.get(market_id)
        if provider_name not in {"perfect_foresight", "persistence", "csv", "custom_python"}:
            raise ValueError(f"Unsupported forecast provider: {provider_name}")
        benchmark_name = f"{market_id}.{workflow}.{provider_name}.{run_scope}"
        if benchmark_suffix is not None:
            benchmark_name = f"{benchmark_name}.{benchmark_suffix}"
        auditable = provider_name != "perfect_foresight"
        is_oracle = provider_name == "perfect_foresight"
        return BenchmarkDefinition(
            benchmark_name=benchmark_name,
            benchmark_family=workflow,
            market_id=market_id,
            workflow=workflow,
            provider_name=provider_name,
            run_scope=run_scope,
            auditable=auditable,
            is_oracle=is_oracle,
        )

    @classmethod
    def default_for_market(cls, market_id: str) -> tuple[str, ...]:
        return MarketRegistry.get(market_id).default_benchmarks()

    @classmethod
    def build_provider(cls, config: BacktestConfig) -> ForecastProvider:
        provider = config.forecast_provider
        if provider.name == "perfect_foresight":
            if provider.mode != "point":
                raise ValueError("perfect_foresight only supports forecast_provider.mode=point")
            return PerfectForesightForecastProvider()
        if provider.name == "persistence":
            if provider.mode != "point":
                raise ValueError("persistence only supports forecast_provider.mode=point")
            return PersistenceForecastProvider()
        if provider.name == "csv":
            return CsvForecastProvider(
                mode=provider.mode,
                day_ahead_path=provider.day_ahead_path,  # type: ignore[arg-type]
                imbalance_path=provider.imbalance_path,
                fcr_capacity_path=provider.fcr_capacity_path,
                afrr_capacity_up_path=provider.afrr_capacity_up_path,
                afrr_capacity_down_path=provider.afrr_capacity_down_path,
                afrr_activation_price_up_path=provider.afrr_activation_price_up_path,
                afrr_activation_price_down_path=provider.afrr_activation_price_down_path,
                afrr_activation_ratio_up_path=provider.afrr_activation_ratio_up_path,
                afrr_activation_ratio_down_path=provider.afrr_activation_ratio_down_path,
                scenario_id=provider.scenario_id,
            )
        if provider.name == "custom_python":
            return CustomPythonForecastProvider(
                mode=provider.mode,
                module_path=provider.module_path,  # type: ignore[arg-type]
                class_name=provider.class_name or "",
                init_kwargs=provider.init_kwargs,
                config=config,
            )
        raise ValueError(f"Unsupported forecast provider: {provider.name}")
