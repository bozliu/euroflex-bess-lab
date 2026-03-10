"""Public package interface for euroflex_bess_lab."""

from .backtesting.engine import WalkForwardEngine, run_backtest, run_walk_forward
from .batch import run_batch
from .benchmarks import BenchmarkRegistry
from .comparison import compare_runs
from .config import (
    BacktestConfig,
    BatchConfig,
    BatchJobConfig,
    SweepConfig,
    load_batch_config,
    load_config,
    load_sweep_config,
)
from .exports import export_bids, export_revision, export_schedule
from .forecasts import (
    CsvForecastProvider,
    CustomForecastModel,
    CustomPythonForecastProvider,
    ForecastProvider,
    PerfectForesightForecastProvider,
    PersistenceForecastProvider,
)
from .markets import MarketAdapter, MarketRegistry
from .reconciliation import reconcile_run
from .reserve import (
    AfrrAsymmetricReserveProduct,
    CapacityOnlyReserveSettlement,
    CapacityPlusActivationReserveSettlement,
    ExpectedActivationAssumption,
    NoActivationAssumption,
    ReserveProduct,
    ReserveSettlementAssumption,
    SymmetricCapacityReserveProduct,
)
from .types import AssetSpec, BatterySpec, MarketProduct, PnLAttribution, PriceSeries, RunResult, SiteSpec
from .validation import doctor, validate_config_file, validate_data_file

__all__ = [
    "BacktestConfig",
    "BatchConfig",
    "BatchJobConfig",
    "AssetSpec",
    "AfrrAsymmetricReserveProduct",
    "BenchmarkRegistry",
    "BatterySpec",
    "CsvForecastProvider",
    "CustomForecastModel",
    "CustomPythonForecastProvider",
    "CapacityPlusActivationReserveSettlement",
    "ForecastProvider",
    "MarketAdapter",
    "MarketProduct",
    "MarketRegistry",
    "NoActivationAssumption",
    "ExpectedActivationAssumption",
    "PerfectForesightForecastProvider",
    "PersistenceForecastProvider",
    "PnLAttribution",
    "PriceSeries",
    "ReserveProduct",
    "ReserveSettlementAssumption",
    "RunResult",
    "SiteSpec",
    "SweepConfig",
    "SymmetricCapacityReserveProduct",
    "CapacityOnlyReserveSettlement",
    "WalkForwardEngine",
    "compare_runs",
    "doctor",
    "export_bids",
    "export_revision",
    "export_schedule",
    "load_batch_config",
    "load_config",
    "load_sweep_config",
    "run_batch",
    "run_backtest",
    "run_walk_forward",
    "reconcile_run",
    "validate_config_file",
    "validate_data_file",
]

__version__ = "1.1.0"
