"""Belgian real-time implicit balancing on Elia Open Data: forecasting, MPC, PRISM, RL, audits, report.

Regenerates ``reports/realtime_belgium.md``, ``reports/figures/realtime_*.png`` and ``reports/ledgers/realtime_*``
from the committed public data set (``data/public``). Every stage caches its results (models, ledgers, skill tables)
outside the repository - ``$EUROFLEX_CACHE_DIR/realtime_backtest`` or ``~/.cache/euroflex_bess_lab/realtime_backtest``
(override with ``--cache``) - so the script can be resumed; ``--force`` recomputes.

Usage::

    python scripts/research/realtime_backtest.py                       # all stages
    python scripts/research/realtime_backtest.py --stages report       # rebuild the report from cached results
    python scripts/research/realtime_backtest.py --quick --out <scratch dir>   # smoke test (short periods)

Stages: ``semantics`` (minute-data checks), ``train`` (walk-forward forecasters), ``rl`` (DQN baseline),
``simulate`` (all strategy runs, forecast skill), ``audit`` (look-ahead audits on real data), ``report``, and
``deploy`` (fit live-trading forecasters on the latest six months; see ``realtime.forecasting.deploy``).
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import sys
import time
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field, replace
from multiprocessing import get_context
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
TZ = "Europe/Brussels"
DATA_START = pd.Timestamp("2024-05-21T22:00:00Z")
WARMUP_DAYS = 3
RISK_QUANTILE = 0.3
TRAIN_MONTHS = 6
FROZEN_TRAIN = ("2025-07-01T00:00:00Z", "2026-01-01T00:00:00Z")
NF_TRAIN = ("2026-01-01T00:00:00Z", "2026-04-01T00:00:00Z")
NF_TEST = ("2026-04-01T00:00:00Z", "2026-08-01T00:00:00Z")
NE_WINDOW = ("2026-01-01T00:00:00Z", "2026-08-01T00:00:00Z")
RL_TRAIN = ("2024-06-01T00:00:00Z", "2025-10-01T00:00:00Z")
RL_VALID = ("2025-10-01T00:00:00Z", "2026-01-01T00:00:00Z")
AUDIT_SLICES = {
    "2025-06-10": ("2025-06-10T00:00:00Z", "2025-06-13T00:00:00Z", ("current",)),
    "2026-03-02": ("2026-03-02T00:00:00Z", "2026-03-05T00:00:00Z", ("current", "new2026")),
    "2026-05-04": ("2026-05-04T00:00:00Z", "2026-05-07T00:00:00Z", ("new2026",)),
    "2026-09-10": ("2026-09-10T00:00:00Z", "2026-09-13T00:00:00Z", ("current",)),
}
AUDIT_CUTS = (37, 1503, 2999)
LAGS = (0, 1, 2, 3)
#: Ablation of SCALES-L's sign/branch decomposition: direct price models on rebuilt new-formula targets.
ABLATION_VARIANTS = ("direct_rebuilt", "direct_rebuilt_curfeat")
#: Natural-experiment groups whose ablation ledgers are exported (group key -> ledger period label).
ABLATION_EXPORT = {"NE_AprJul_new2026": "AprJul2026", "NE_JanJul_new2026": "JanJul2026"}
DEGRADATION = (0.0, 4.0, 8.0, 16.0)


def utc(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


# ----------------------------------------------------------------------------------------------------------------
# configuration


@dataclass
class Settings:
    out: Path
    cache: Path
    quick: bool = False
    workers: int = 3
    threads: int = 3
    rl_minutes: float = 25.0
    force: bool = False
    periods: dict[str, tuple[str, str, tuple[str, ...]]] = field(default_factory=dict)
    gbm: dict[str, Any] = field(default_factory=dict)

    @property
    def reports(self) -> Path:
        return self.out

    @property
    def ledgers(self) -> Path:
        return self.out / "ledgers"

    @property
    def figures(self) -> Path:
        return self.out / "figures"


def latest_data_end() -> pd.Timestamp:
    from euroflex_bess_lab import public_data

    qh = public_data.load_belgium_qh("2026-08-01T00:00:00Z", "2027-01-01T00:00:00Z")
    qh = qh.dropna(subset=["imbalance_price_eur_mwh"])
    last = pd.Timestamp(qh["timestamp_utc"].max()) + pd.Timedelta(minutes=15)
    local = last.tz_convert(TZ).normalize()  # last complete local delivery day
    return local.tz_convert("UTC")


def default_cache_root() -> Path:
    """Cache outside the repository tree (keeps the checkout clean; ``artifacts/`` must stay empty)."""
    env = os.environ.get("EUROFLEX_CACHE_DIR")
    return Path(env).expanduser() if env else Path.home() / ".cache" / "euroflex_bess_lab"


def make_settings(args: argparse.Namespace) -> Settings:
    out = Path(args.out) if args.out else REPO / "reports"
    cache = (
        Path(args.cache)
        if args.cache
        else default_cache_root() / ("realtime_quick" if args.quick else "realtime_backtest")
    )
    s = Settings(out=out, cache=cache, quick=args.quick, workers=args.workers, threads=args.threads, force=args.force)
    end_c = latest_data_end()
    if args.quick:
        s.periods = {
            "A": ("2025-06-01T00:00:00Z", "2025-06-15T00:00:00Z", ("current",)),
            "B": ("2026-04-01T00:00:00Z", "2026-04-15T00:00:00Z", ("current", "new2026")),
            "C": ("2026-09-01T00:00:00Z", "2026-09-08T00:00:00Z", ("current",)),
        }
        s.gbm = {"max_iter": 30, "max_leaf_nodes": 15}
        s.rl_minutes = 1.0
    else:
        s.periods = {
            "A": ("2024-06-01T00:00:00Z", "2026-01-01T00:00:00Z", ("current",)),
            "B": ("2026-01-01T00:00:00Z", "2026-08-01T00:00:00Z", ("current", "new2026")),
            "C": ("2026-08-01T00:00:00Z", end_c.isoformat(), ("current",)),
        }
        s.rl_minutes = args.rl_minutes
    return s


# ----------------------------------------------------------------------------------------------------------------
# forecaster specifications


@dataclass
class ForecasterSpec:
    name: str
    kind: str  # price | prism
    mechanism: str
    components: str = "published"
    features: str = "default"  # default | new_formula
    folds: list[Any] = field(default_factory=list)
    lag: int = 1
    description: str = ""


def forecaster_specs(s: Settings) -> dict[str, ForecasterSpec]:
    from euroflex_bess_lab.realtime.forecasting import monthly_folds, single_fold

    a0 = s.periods["A"][0]
    c1 = s.periods["C"][1]
    b0, b1 = s.periods["B"][0], s.periods["B"][1]
    windows = [(utc(p[0]), utc(p[1])) for p in s.periods.values()]

    def needed(folds: list[Any]) -> list[Any]:
        return [f for f in folds if any(f.test_start < w1 and f.test_end > w0 for w0, w1 in windows)]

    wf = needed(monthly_folds(a0, c1, data_start=DATA_START, train_months=TRAIN_MONTHS))
    wf_b = monthly_folds(b0, b1, data_start=DATA_START, train_months=TRAIN_MONTHS)
    nf_test = (max(utc(NF_TEST[0]), utc(b0)), min(utc(NF_TEST[1]), utc(b1)))
    nf_train = (utc(NF_TRAIN[0]), min(utc(NF_TRAIN[1]), nf_test[0]))
    if s.quick:
        nf_train = (utc(b0) - pd.Timedelta(days=21), utc(b0))
    frozen_train = (utc(FROZEN_TRAIN[0]), utc(FROZEN_TRAIN[1]))
    if s.quick:
        frozen_train = (utc("2025-12-01T00:00:00Z"), utc("2026-01-01T00:00:00Z"))
    ne_test = (utc(NE_WINDOW[0]) if not s.quick else utc(b0), utc(NE_WINDOW[1]) if not s.quick else utc(b1))
    specs = {
        "price_current": ForecasterSpec(
            "price_current",
            "price",
            "current",
            folds=wf,
            description="direct price model, current formula, monthly walk-forward",
        ),
        "prism_current": ForecasterSpec(
            "prism_current",
            "prism",
            "current",
            folds=wf,
            description="PRISM state model priced by the current operator",
        ),
        "prism_zero_wf": ForecasterSpec(
            "prism_zero_wf",
            "prism",
            "new2026",
            components="reconstructed",
            folds=wf_b,
            description="PRISM zero-history (new operator on reconstructed components), "
            "monthly walk-forward on current-design state only",
        ),
        "price_current_frozen": ForecasterSpec(
            "price_current_frozen",
            "price",
            "current",
            folds=single_fold(*frozen_train, *ne_test),
            description="current-formula price model frozen at 2026-01-01",
        ),
        "prism_zero_frozen": ForecasterSpec(
            "prism_zero_frozen",
            "prism",
            "new2026",
            components="reconstructed",
            folds=single_fold(*frozen_train, *ne_test),
            description="PRISM zero-history frozen at 2026-01-01",
        ),
        "price_new2026": ForecasterSpec(
            "price_new2026",
            "price",
            "new2026",
            features="new_formula",
            folds=single_fold(*nf_train, *nf_test),
            description="price model trained on new-formula prices Jan-Mar 2026",
        ),
        "prism_new_published": ForecasterSpec(
            "prism_new_published",
            "prism",
            "new2026",
            components="published",
            folds=single_fold(*nf_train, *nf_test),
            description="PRISM with published new-formula components Jan-Mar 2026",
        ),
        # ablation: the direct price model on new-formula prices rebuilt from current-design state
        "direct_rebuilt_frozen": ForecasterSpec(
            "direct_rebuilt_frozen",
            "price",
            "new2026",
            features="rebuilt_invariant",
            folds=single_fold(*frozen_train, *ne_test),
            description="direct price model, rebuilt new-formula targets, SCALES-L zero-history information set",
        ),
        "direct_rebuilt_curfeat_frozen": ForecasterSpec(
            "direct_rebuilt_curfeat_frozen",
            "price",
            "new2026",
            features="rebuilt_current",
            folds=single_fold(*frozen_train, *ne_test),
            description="direct price model, rebuilt new-formula targets, feature set of the naive arm (a)",
        ),
    }
    c0 = s.periods["C"][0]
    wf_c = monthly_folds(c0, c1, data_start=DATA_START, train_months=TRAIN_MONTHS)
    for lag in LAGS:
        if lag == 1:
            continue
        specs[f"price_current_lag{lag}"] = ForecasterSpec(
            f"price_current_lag{lag}", "price", "current", folds=wf_c, lag=lag, description=f"lag {lag} retrain"
        )
        specs[f"prism_current_lag{lag}"] = ForecasterSpec(
            f"prism_current_lag{lag}", "prism", "current", folds=wf_c, lag=lag, description=f"lag {lag} retrain"
        )
    return specs


def build_forecaster(spec: ForecasterSpec, gbm: dict[str, Any]) -> Any:
    from euroflex_bess_lab.realtime.forecasting import (
        CURRENT_GROUPS,
        CURRENT_PRICE_BASELINE,
        INVARIANT_GROUPS,
        NEW_FORMULA_GROUPS,
        NEW_FORMULA_PRICE_BASELINE,
        REBUILT_PRICE_BASELINE,
        FeatureConfig,
        GBMParams,
        PriceForecaster,
        PriceForecasterConfig,
        PrismForecaster,
        PrismForecasterConfig,
    )

    params = GBMParams(**gbm)
    if spec.kind == "price":
        if spec.features == "new_formula":
            config = PriceForecasterConfig(
                mechanism=spec.mechanism,
                features=FeatureConfig(groups=NEW_FORMULA_GROUPS, history="settlement:new2026"),
                si_field="nf_si_mw",
                baseline=NEW_FORMULA_PRICE_BASELINE,
                params=params,
            )
        elif spec.features == "rebuilt_invariant":
            config = PriceForecasterConfig(
                mechanism=spec.mechanism,
                features=FeatureConfig(groups=INVARIANT_GROUPS, history="reconstructed:new2026"),
                si_field="si_mw",
                baseline=REBUILT_PRICE_BASELINE,
                target="reconstructed:new2026",
                params=params,
            )
        elif spec.features == "rebuilt_current":
            config = PriceForecasterConfig(
                mechanism=spec.mechanism,
                features=FeatureConfig(groups=CURRENT_GROUPS, history="settlement:current"),
                si_field="si_mw",
                baseline=CURRENT_PRICE_BASELINE,
                target="reconstructed:new2026",
                params=params,
            )
        else:
            config = PriceForecasterConfig(mechanism=spec.mechanism, params=params)
        return PriceForecaster(config, name=spec.name)
    si_field = "nf_si_mw" if (spec.mechanism == "new2026" and spec.components == "published") else "si_mw"
    config_p = PrismForecasterConfig(
        mechanism=spec.mechanism,
        components=spec.components,
        params=params,
        si_field=si_field,
    )
    return PrismForecaster(config_p, name=spec.name)


def load_market(start: Any, end: Any, *, lag: int = 1) -> Any:
    from euroflex_bess_lab.realtime import MarketData, PublicationModel

    start_ts, end_ts = utc(start), utc(end)
    start_ts = max(start_ts, DATA_START)
    return MarketData.from_public(start_ts, end_ts, publication=PublicationModel(minute_lag=lag))


# ----------------------------------------------------------------------------------------------------------------
# workers


def _init_worker(threads: int) -> None:
    os.environ["OMP_NUM_THREADS"] = str(threads)
    os.environ.setdefault("PYTHONHASHSEED", "0")


def _fit_fold_task(spec: ForecasterSpec, fold_index: int, gbm: dict[str, Any], out_path: str) -> dict[str, Any]:
    from euroflex_bess_lab.realtime import PublicationModel

    t0 = time.perf_counter()
    fold = spec.folds[fold_index]
    market = load_market(fold.train_start - pd.Timedelta(days=2), fold.train_end, lag=spec.lag)
    forecaster = build_forecaster(spec, gbm)
    forecaster.fit(market, [fold], PublicationModel(minute_lag=spec.lag))
    with open(out_path, "wb") as fh:
        pickle.dump(forecaster, fh, protocol=pickle.HIGHEST_PROTOCOL)
    return {"spec": spec.name, "fold": fold.label, "seconds": time.perf_counter() - t0, **forecaster.fit_log_[0]}


def stage_train(s: Settings) -> None:
    from euroflex_bess_lab.realtime.forecasting import merge_forecasters

    specs = forecaster_specs(s)
    model_dir = s.cache / "models"
    part_dir = model_dir / "parts"
    part_dir.mkdir(parents=True, exist_ok=True)
    tasks = []
    for spec in specs.values():
        if (model_dir / f"{spec.name}.pkl").exists() and not s.force:
            continue
        for i, fold in enumerate(spec.folds):
            part = part_dir / f"{spec.name}__{fold.label}.pkl"
            if part.exists() and not s.force:
                continue
            tasks.append((spec, i, str(part)))
    print(f"[train] {len(tasks)} fold fits on {s.workers} workers", flush=True)
    logs: list[dict[str, Any]] = []
    if tasks:
        ctx = get_context("spawn")
        with ProcessPoolExecutor(s.workers, mp_context=ctx, initializer=_init_worker, initargs=(s.threads,)) as pool:
            futures = [pool.submit(_fit_fold_task, spec, i, s.gbm, part) for spec, i, part in tasks]
            for n, future in enumerate(as_completed(futures), 1):
                log = future.result()
                logs.append(log)
                print(f"[train] {n}/{len(tasks)} {log['spec']} {log['fold']} {log['seconds']:.0f}s", flush=True)
    for spec in specs.values():
        target = model_dir / f"{spec.name}.pkl"
        if target.exists() and not s.force:
            continue
        parts = []
        for fold in spec.folds:
            with open(part_dir / f"{spec.name}__{fold.label}.pkl", "rb") as fh:
                parts.append(pickle.load(fh))
        merged = merge_forecasters(parts)
        with open(target, "wb") as fh:
            pickle.dump(merged, fh, protocol=pickle.HIGHEST_PROTOCOL)
    log_path = s.cache / "train_log.json"
    previous = json.loads(log_path.read_text()) if log_path.exists() else []
    log_path.write_text(json.dumps(previous + logs, indent=1, default=str))


_FORECASTERS: dict[str, Any] = {}


def fusion_variance(s: Settings) -> np.ndarray:
    """Running-estimate error profile of the *current* formula on the frozen pre-2026 window (cached)."""
    from euroflex_bess_lab.realtime import PublicationModel
    from euroflex_bess_lab.realtime.forecasting.fusion import running_error_variance

    path = s.cache / "fusion_variance.json"
    if path.exists() and not s.force:
        return np.asarray(json.loads(path.read_text())["variance"], dtype=float)
    start, end = (utc(FROZEN_TRAIN[0]), utc(FROZEN_TRAIN[1]))
    if s.quick:
        start, end = utc("2025-12-01T00:00:00Z"), utc("2026-01-01T00:00:00Z")
    market = load_market(start - pd.Timedelta(days=1), end)
    variance = running_error_variance(
        market, "price_cum_eur_mwh", market.settlement("current"), PublicationModel(), start, end
    )
    path.write_text(json.dumps({"window": [start.isoformat(), end.isoformat()], "variance": list(variance)}))
    return variance


def get_forecaster(s: Settings, name: str) -> Any:
    from euroflex_bess_lab.realtime.forecasting.fusion import RunningEstimateFusion
    from euroflex_bess_lab.realtime.strategies.forecast_mpc import CachedForecaster

    if name not in _FORECASTERS:
        if name.endswith("_live"):
            base = name[: -len("_live")]
            with open(s.cache / "models" / f"{base}.pkl", "rb") as fh:
                fused = RunningEstimateFusion(pickle.load(fh), "nf_imbalance_price_eur_mwh", fusion_variance(s), name)
            _FORECASTERS[name] = CachedForecaster(fused)
        else:
            with open(s.cache / "models" / f"{name}.pkl", "rb") as fh:
                _FORECASTERS[name] = CachedForecaster(pickle.load(fh))
    return _FORECASTERS[name]


# ----------------------------------------------------------------------------------------------------------------
# RL


def stage_rl(s: Settings) -> None:
    target = s.cache / "rl_policy.npz"
    if target.exists() and not s.force:
        print("[rl] cached policy found", flush=True)
        return
    from euroflex_bess_lab.realtime import BatteryParams, PublicationModel
    from euroflex_bess_lab.realtime.strategies.rl import DQNConfig, train_dqn

    train = RL_TRAIN if not s.quick else ("2025-11-01T00:00:00Z", "2025-12-01T00:00:00Z")
    valid = RL_VALID if not s.quick else ("2025-12-01T00:00:00Z", "2025-12-10T00:00:00Z")
    market = load_market(utc(train[0]) - pd.Timedelta(days=1), valid[1])
    config = DQNConfig(max_minutes=s.rl_minutes)
    if s.quick:
        config = replace(config, n_envs=64, batch_size=256, learning_starts=2_000, validate_every=1_000)
    battery = BatteryParams(power_mw=1.0, energy_mwh=2.0)
    print(f"[rl] training DQN for up to {config.max_minutes} min", flush=True)
    policy = train_dqn(market, battery, train=train, validation=valid, publication=PublicationModel(), config=config)
    policy.save(target)
    print(f"[rl] done: {json.dumps({k: v for k, v in policy.log.items() if k != 'validation_history'}, default=str)}")


def _rl_process(settings: Settings) -> None:
    stage_rl(settings)


# ----------------------------------------------------------------------------------------------------------------
# strategies


BATTERIES = {
    "1mw_2mwh": dict(power_mw=1.0, energy_mwh=2.0),
    "1mw_1mwh": dict(power_mw=1.0, energy_mwh=1.0),
}


def make_strategy(s: Settings, name: str, mechanism: str, lag: int = 1) -> Any:
    from euroflex_bess_lab.realtime import IdleStrategy, OracleQHStrategy, ThresholdStrategy
    from euroflex_bess_lab.realtime.strategies.forecast_mpc import ForecastMPCStrategy, MPCConfig
    from euroflex_bess_lab.realtime.strategies.prism import PrismStrategy

    neutral, risk = MPCConfig(), MPCConfig(risk="quantile", risk_quantile=RISK_QUANTILE)
    suffix = "" if lag == 1 else f"_lag{lag}"
    if name == "idle":
        return IdleStrategy()
    if name == "oracle_qh":
        return OracleQHStrategy()
    if name == "threshold_price":
        field_name = "price_cum_eur_mwh" if mechanism == "current" else "nf_imbalance_price_eur_mwh"
        return ThresholdStrategy(mode="price", signal_field=field_name, name="threshold_price")
    if name == "threshold_si":
        return ThresholdStrategy(mode="si", signal_field="si_cum_mw", name="threshold_si")
    if name in ("forecast_mpc", "forecast_mpc_risk"):
        f = get_forecaster(s, f"price_current{suffix}")
        return ForecastMPCStrategy(f, risk if name.endswith("risk") else neutral, name=name)
    if name in ("prism", "prism_risk"):
        model = "prism_current" if mechanism == "current" else "prism_zero_wf"
        f = get_forecaster(s, f"{model}{suffix}")
        return PrismStrategy(f, risk if name.endswith("risk") else neutral, name=name)
    if name == "rl_dqn":
        from euroflex_bess_lab.realtime.strategies.rl import DQNPolicy, RLStrategy

        return RLStrategy(DQNPolicy.load(s.cache / "rl_policy.npz"), name="rl_dqn")
    # natural-experiment arms (new2026 settlement)
    arms: dict[str, tuple[str, Any, str]] = {
        "ne_naive_frozen": ("price_current_frozen", neutral, "mpc"),
        "ne_naive_frozen_risk": ("price_current_frozen", risk, "mpc"),
        "ne_naive_wf": ("price_current", neutral, "mpc"),
        "ne_price_new2026": ("price_new2026", neutral, "mpc"),
        "ne_price_new2026_risk": ("price_new2026", risk, "mpc"),
        "ne_prism_zero_frozen": ("prism_zero_frozen", neutral, "prism"),
        "ne_prism_zero_frozen_risk": ("prism_zero_frozen", risk, "prism"),
        "ne_prism_zero_wf": ("prism_zero_wf", neutral, "prism"),
        "ne_prism_new_published": ("prism_new_published", neutral, "prism"),
        "ne_prism_zero_live": ("prism_zero_frozen_live", neutral, "prism"),
        "direct_rebuilt": ("direct_rebuilt_frozen", neutral, "mpc"),
        "direct_rebuilt_risk": ("direct_rebuilt_frozen", risk, "mpc"),
        "direct_rebuilt_live": ("direct_rebuilt_frozen_live", neutral, "mpc"),
        "direct_rebuilt_live_risk": ("direct_rebuilt_frozen_live", risk, "mpc"),
        "direct_rebuilt_curfeat": ("direct_rebuilt_curfeat_frozen", neutral, "mpc"),
        "direct_rebuilt_curfeat_risk": ("direct_rebuilt_curfeat_frozen", risk, "mpc"),
        "direct_rebuilt_curfeat_live": ("direct_rebuilt_curfeat_frozen_live", neutral, "mpc"),
        "direct_rebuilt_curfeat_live_risk": ("direct_rebuilt_curfeat_frozen_live", risk, "mpc"),
        "ne_prism_zero_live_risk": ("prism_zero_frozen_live", risk, "prism"),
        "prism_live": ("prism_zero_wf_live", neutral, "prism"),
        "prism_live_risk": ("prism_zero_wf_live", risk, "prism"),
    }
    if name in arms:
        model, config, kind = arms[name]
        f = get_forecaster(s, model)
        if kind == "prism":
            return PrismStrategy(f, config, name=name)
        return ForecastMPCStrategy(f, config, name=name)
    raise KeyError(name)


MAIN_STRATEGIES = (
    "idle",
    "threshold_price",
    "threshold_si",
    "forecast_mpc",
    "forecast_mpc_risk",
    "prism",
    "prism_risk",
    "rl_dqn",
    "oracle_qh",
)


@dataclass
class SimGroup:
    key: str
    start: str
    end: str
    mechanism: str
    battery: str
    strategies: tuple[str, ...]
    lag: int = 1
    degradation: float = 8.0
    export: str | None = None  # period label for reports/ledgers
    skill: tuple[str, ...] = ()


def simulation_groups(s: Settings) -> list[SimGroup]:
    groups: list[SimGroup] = []
    for period, (start, end, mechanisms) in s.periods.items():
        for mechanism in mechanisms:
            for battery in BATTERIES:
                strategies = tuple(x for x in MAIN_STRATEGIES if not (x == "rl_dqn" and period == "A"))
                if battery != "1mw_2mwh":
                    strategies = tuple(x for x in strategies if x != "rl_dqn")
                if mechanism == "new2026":
                    strategies = (*strategies, "prism_live", "prism_live_risk")
                skill: tuple[str, ...] = ()
                if battery == "1mw_2mwh":
                    skill = ("price_current", "prism_current" if mechanism == "current" else "prism_zero_wf")
                groups.append(
                    SimGroup(
                        f"{period}_{mechanism}_{battery}",
                        start,
                        end,
                        mechanism,
                        battery,
                        strategies,
                        export=period,
                        skill=skill,
                    )
                )
    b0, b1 = s.periods["B"][0], s.periods["B"][1]
    nf0 = max(utc(NF_TEST[0]), utc(b0)) if not s.quick else utc(b0)
    nf1 = min(utc(NF_TEST[1]), utc(b1))
    ablation = tuple(f"{v}{suffix}" for v in ABLATION_VARIANTS for suffix in ("", "_risk", "_live", "_live_risk"))
    ablation_skill = tuple(f"{v}_frozen{live}" for v in ABLATION_VARIANTS for live in ("", "_live"))
    ne_arms = (
        "idle",
        "threshold_price",
        "threshold_si",
        "ne_naive_frozen",
        "ne_naive_frozen_risk",
        "ne_naive_wf",
        "ne_price_new2026",
        "ne_price_new2026_risk",
        "ne_prism_zero_frozen",
        "ne_prism_zero_frozen_risk",
        "ne_prism_zero_wf",
        "ne_prism_zero_live",
        "ne_prism_zero_live_risk",
        "ne_prism_new_published",
        *ablation,
        "oracle_qh",
    )
    groups.append(
        SimGroup(
            "NE_AprJul_new2026",
            nf0.isoformat(),
            nf1.isoformat(),
            "new2026",
            "1mw_2mwh",
            ne_arms,
            skill=(
                "price_current_frozen",
                "price_new2026",
                "prism_zero_frozen",
                "prism_zero_wf",
                "prism_new_published",
                "price_current",
                "prism_zero_frozen_live",
                *ablation_skill,
            ),
        )
    )
    groups.append(
        SimGroup(
            "NE_AprJul_current",
            nf0.isoformat(),
            nf1.isoformat(),
            "current",
            "1mw_2mwh",
            ("ne_naive_frozen", "forecast_mpc", "prism", "threshold_si", "oracle_qh"),
        )
    )
    groups.append(
        SimGroup(
            "NE_JanJul_new2026",
            b0,
            b1,
            "new2026",
            "1mw_2mwh",
            (
                "threshold_si",
                "ne_naive_frozen",
                "ne_naive_frozen_risk",
                "ne_prism_zero_frozen",
                "ne_prism_zero_frozen_risk",
                "ne_prism_zero_wf",
                "ne_prism_zero_live",
                "ne_prism_zero_live_risk",
                *ablation,
                "oracle_qh",
            ),
            skill=("price_current_frozen", "prism_zero_frozen", "prism_zero_frozen_live", *ablation_skill),
        )
    )
    c0, c1 = s.periods["C"][0], s.periods["C"][1]
    for lag in LAGS:
        strategies = ("threshold_price", "threshold_si", "forecast_mpc", "forecast_mpc_risk", "prism")
        if lag == 1:
            strategies = (*strategies, "oracle_qh")
        groups.append(SimGroup(f"LAG{lag}_C_current", c0, c1, "current", "1mw_2mwh", strategies, lag=lag))
    for deg in DEGRADATION:
        if deg == 8.0:
            continue
        groups.append(
            SimGroup(
                f"DEG{int(deg)}_B_current",
                b0,
                b1,
                "current",
                "1mw_2mwh",
                ("threshold_si", "forecast_mpc", "forecast_mpc_risk", "prism", "oracle_qh"),
                degradation=deg,
            )
        )
    return groups


def _trim(qh: pd.DataFrame, start: pd.Timestamp) -> pd.DataFrame:
    return qh[qh["timestamp_utc"] >= start].reset_index(drop=True)


def _run_group(s: Settings, group: SimGroup) -> dict[str, Any]:
    from euroflex_bess_lab.realtime import BatteryParams, PublicationModel, simulate, summarize_ledger
    from euroflex_bess_lab.realtime.forecasting import forecast_skill, minute_target  # noqa: F401

    out_dir = s.cache / "sim" / group.key
    out_dir.mkdir(parents=True, exist_ok=True)
    start, end = utc(group.start), utc(group.end)
    market_full = load_market(start - pd.Timedelta(days=WARMUP_DAYS), end, lag=group.lag)
    publication = PublicationModel(minute_lag=group.lag)
    battery = BatteryParams(**BATTERIES[group.battery], degradation_eur_per_mwh=group.degradation)
    results: dict[str, Any] = {}
    ledgers: dict[str, pd.DataFrame] = {}
    order = sorted(group.strategies, key=lambda x: x != "oracle_qh")  # oracle first for capture ratios
    oracle_pnl: float | None = None
    for name in order:
        path = out_dir / f"{name}.parquet"
        summary_path = out_dir / f"{name}.json"
        if path.exists() and summary_path.exists() and not s.force:
            ledgers[name] = pd.read_parquet(path)
            results[name] = json.loads(summary_path.read_text())
            if name == "oracle_qh":
                oracle_pnl = float(ledgers[name]["pnl_eur"].sum())
            continue
        t0 = time.perf_counter()
        strategy = make_strategy(s, name, group.mechanism, lag=group.lag)
        res = simulate(
            market_full, strategy, battery, mechanism=group.mechanism, publication=publication, record_minutes=False
        )
        qh = _trim(res.qh, start)
        summary = summarize_ledger(qh, battery, n_minutes=len(qh) * 15, oracle_pnl_eur=oracle_pnl, timezone=TZ)
        summary.update(
            {
                "strategy": name,
                "mechanism": group.mechanism,
                "group": group.key,
                "start_utc": start.isoformat(),
                "end_utc": end.isoformat(),
                "battery": asdict(battery),
                "publication": asdict(publication),
                "seconds": time.perf_counter() - t0,
            }
        )
        if name == "oracle_qh":
            oracle_pnl = float(qh["pnl_eur"].sum())
        qh.to_parquet(path, index=False)
        summary_path.write_text(json.dumps(summary, indent=1, default=str))
        ledgers[name], results[name] = qh, summary
        print(
            f"[sim] {group.key} {name}: {summary['eur_per_mw_year']:,.0f} EUR/MW/yr ({summary['seconds']:.0f}s)",
            flush=True,
        )
    # forecast skill on the evaluation window (current-QH and next-QH heads)
    for model in group.skill:
        skill_path = out_dir / f"skill_{model}.json"
        if skill_path.exists() and not s.force:
            continue
        forecaster = get_forecaster(s, model)
        frame = forecaster.predict(market_full, publication)
        m0 = int((start - market_full.start_utc) / pd.Timedelta(minutes=1))
        rows = np.arange(m0, market_full.n_minutes)
        prices = market_full.settlement(group.mechanism)
        si_field = "nf_si_mw" if group.mechanism == "new2026" else "si_mw"
        running = "price_cum_eur_mwh" if group.mechanism == "current" else "nf_imbalance_price_eur_mwh"
        tables = {}
        for horizon, dist in ((0, frame.current), (1, frame.next)):
            table = forecast_skill(
                dist,
                market_full,
                prices,
                si_qh=market_full.qh.get(si_field),
                horizon=horizon,
                rows=rows,
                publication=publication,
                running_field=running if horizon == 0 else None,
            )
            tables[f"h{horizon}"] = table.reset_index().to_dict(orient="list")
        skill_path.write_text(json.dumps(tables, default=float))
    _FORECASTERS.clear()
    return {"group": group.key, "strategies": list(results)}


def _run_group_task(s: Settings, group: SimGroup) -> dict[str, Any]:
    return _run_group(s, group)


def stage_simulate(s: Settings, only: str | None = None) -> None:
    groups = simulation_groups(s)
    if only:
        groups = [g for g in groups if g.key.startswith(only)]
    have_rl = (s.cache / "rl_policy.npz").exists()
    if not have_rl:
        groups = [replace(g, strategies=tuple(x for x in g.strategies if x != "rl_dqn")) for g in groups]
    print(f"[sim] {len(groups)} groups on {s.workers} workers (rl={'yes' if have_rl else 'no'})", flush=True)
    ctx = get_context("spawn")
    # large groups first
    groups.sort(key=lambda g: -(utc(g.end) - utc(g.start)).total_seconds() * len(g.strategies))
    with ProcessPoolExecutor(s.workers, mp_context=ctx, initializer=_init_worker, initargs=(s.threads,)) as pool:
        futures = {pool.submit(_run_group_task, s, g): g.key for g in groups}
        for future in as_completed(futures):
            print(f"[sim] finished {future.result()['group']}", flush=True)


# ----------------------------------------------------------------------------------------------------------------
# semantics


def stage_semantics(s: Settings) -> None:
    from euroflex_bess_lab.realtime import MarketData
    from euroflex_bess_lab.realtime.mechanisms import reconstruct_new_formula_history

    out: dict[str, Any] = {}
    years = [
        ("2024-05-22T00:00:00Z", "2025-01-01T00:00:00Z"),
        ("2025-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        ("2026-01-01T00:00:00Z", s.periods["C"][1]),
    ]
    if s.quick:
        years = [("2025-06-01T00:00:00Z", "2025-06-08T00:00:00Z")]
    rows = []
    running_rows = []
    for start, end in years:
        m = MarketData.from_public(
            start, end, include_new_formula=False, include_si_forecast=False, include_day_ahead=False
        )
        last = np.arange(m.n_qh) * 15 + 14
        price_last = m.minute["price_cum_eur_mwh"][last]
        final = m.qh["imbalance_price_eur_mwh"]
        si_last = m.minute["si_cum_mw"][last]
        si_final = m.qh["si_mw"]
        ok = np.isfinite(price_last) & np.isfinite(final)
        oks = np.isfinite(si_last) & np.isfinite(si_final)
        # instantaneous SI implied by the cumulative ODS133 field vs ODS126 at several offsets
        cum = m.minute["si_cum_mw"].reshape(-1, 15)
        mm = np.arange(15)[None, :]
        inst = cum.copy()
        inst[:, 1:] = (mm[:, 1:] + 1) * cum[:, 1:] - mm[:, 1:] * cum[:, :-1]
        inst = inst.reshape(-1)
        ods126 = m.minute["si_mw"]
        offsets = {}
        for off in (-1, 0, 1, 2):
            shifted = np.roll(ods126, -off)
            okk = np.isfinite(inst) & np.isfinite(shifted)
            okk[-3:] = False
            offsets[str(off)] = float(np.mean(np.abs(inst[okk] - shifted[okk])))
        rows.append(
            {
                "window": f"{start[:10]}..{end[:10]}",
                "qh": int(ok.sum()),
                "price_exact_share": float(np.mean(np.isclose(price_last[ok], final[ok], atol=0.01))),
                "price_mae_eur_mwh": float(np.mean(np.abs(price_last[ok] - final[ok]))),
                "price_p95_abs_err_eur_mwh": float(np.quantile(np.abs(price_last[ok] - final[ok]), 0.95)),
                "si_mae_mw": float(np.mean(np.abs(si_last[oks] - si_final[oks]))),
                "si_sign_agreement": float(np.mean(np.sign(si_last[oks]) == np.sign(si_final[oks]))),
                "ods126_vs_ods133_inst_mae_by_offset": offsets,
            }
        )
        cum_price = m.minute["price_cum_eur_mwh"].reshape(-1, 15)
        for minute in range(15):
            v = cum_price[:, minute]
            okm = np.isfinite(v) & np.isfinite(final)
            running_rows.append(
                {
                    "window": f"{start[:10]}..{end[:10]}",
                    "mechanism": "current",
                    "minute": minute,
                    "mae": float(np.mean(np.abs(v[okm] - final[okm]))),
                }
            )
    out["ods133_last_minute_vs_ods134"] = rows
    # new formula (ODS211/212)
    b0, b1 = NE_WINDOW if not s.quick else (s.periods["B"][0], s.periods["B"][1])
    m = MarketData.from_public(b0, b1, include_si_forecast=False, include_day_ahead=False)
    last = np.arange(m.n_qh) * 15 + 14
    nf_fields = [
        "imbalance_price_eur_mwh",
        "si_mw",
        "main_up_eur_mwh",
        "main_down_eur_mwh",
        "afrr_up_mw",
        "afrr_price_up_eur_mwh",
        "afrr_price_down_eur_mwh",
        "frce_mw",
        "weight_afrr",
    ]
    nf_rows = []
    for f in nf_fields:
        x, y = m.minute[f"nf_{f}"][last], m.qh[f"nf_{f}"]
        okf = np.isfinite(x) & np.isfinite(y)
        nf_rows.append(
            {
                "field": f,
                "qh": int(okf.sum()),
                "last_minute_exact_share": float(np.mean(np.isclose(x[okf], y[okf], atol=0.01))),
                "mae": float(np.mean(np.abs(x[okf] - y[okf]))),
            }
        )
    # cumulative signature: minute-to-minute change shrinks like 1/(m+1)
    sig = {}
    for f in ("nf_si_mw", "nf_afrr_up_mw", "si_cum_mw", "si_mw"):
        g = m.minute[f].reshape(-1, 15)
        sig[f] = [float(np.nanstd(g[:, k] - g[:, k - 1])) for k in range(1, 15)]
    out["ods211_last_minute_vs_ods212"] = nf_rows
    out["minute_change_std_by_minute"] = sig
    final_new = m.qh["nf_imbalance_price_eur_mwh"]
    cum_new = m.minute["nf_imbalance_price_eur_mwh"].reshape(-1, 15)
    final_cur = m.qh["imbalance_price_eur_mwh"]
    cum_cur = m.minute["price_cum_eur_mwh"].reshape(-1, 15)
    for minute in range(15):
        for label, cum_arr, fin in (("new2026", cum_new, final_new), ("current", cum_cur, final_cur)):
            v = cum_arr[:, minute]
            okm = np.isfinite(v) & np.isfinite(fin)
            running_rows.append(
                {
                    "window": "2026-01..2026-07",
                    "mechanism": label,
                    "minute": minute,
                    "mae": float(np.mean(np.abs(v[okm] - fin[okm]))),
                }
            )
    out["running_estimate_mae_by_minute"] = running_rows
    a, b = m.qh["si_mw"], m.qh["nf_si_mw"]
    okab = np.isfinite(a) & np.isfinite(b)
    out["si_ods134_vs_ods212"] = {
        "mae_mw": float(np.mean(np.abs(a[okab] - b[okab]))),
        "sign_agreement": float(np.mean(np.sign(a[okab]) == np.sign(b[okab]))),
    }
    rec = reconstruct_new_formula_history(pd.DataFrame(m.qh))
    r = rec["imbalance_price_eur_mwh"].to_numpy()
    pub = m.qh["nf_imbalance_price_eur_mwh"]
    cur = m.qh["imbalance_price_eur_mwh"]
    okr = np.isfinite(r) & np.isfinite(pub)
    short = m.qh["si_mw"] < 0
    regime = {}
    for label, mask in (("short", short), ("long", ~short)):
        regime[label] = {
            "share": float(np.mean(mask)),
            "current_mean": float(np.nanmean(cur[mask])),
            "new2026_published_mean": float(np.nanmean(pub[mask])),
            "new2026_reconstructed_mean": float(np.nanmean(r[mask])),
        }
    out["reconstruction_vs_ods212"] = {
        "qh": int(okr.sum()),
        "mae_eur_mwh": float(np.mean(np.abs(r[okr] - pub[okr]))),
        "corr": float(np.corrcoef(r[okr], pub[okr])[0, 1]),
        "corr_current_vs_new": float(np.corrcoef(cur[okr], pub[okr])[0, 1]),
        "regime_means": regime,
        "afrr_single_vs_directional_up_mae": float(
            np.nanmean(np.abs(m.qh["afrr_price_up_eur_mwh"] - m.qh["nf_afrr_price_up_eur_mwh"]))
        ),
        "afrr_single_vs_directional_down_mae": float(
            np.nanmean(np.abs(m.qh["afrr_price_down_eur_mwh"] - m.qh["nf_afrr_price_down_eur_mwh"]))
        ),
        "incentive_qh": int(np.isfinite(m.qh["nf_incentive_eur_mwh"]).sum()),
    }
    s.ledgers.mkdir(parents=True, exist_ok=True)
    (s.ledgers / "realtime_data_semantics.json").write_text(json.dumps(out, indent=1))
    print("[semantics] written", flush=True)


# ----------------------------------------------------------------------------------------------------------------
# audits


def _plan_white_box(s: Settings, name: str, mechanism: str, market: Any, battery: Any, cut: int) -> bool | None:
    """Stronger audit for forecast strategies: compare every precomputed plan array before the safe horizon."""
    from euroflex_bess_lab.realtime import MarketData, PublicationModel
    from euroflex_bess_lab.realtime.strategies.forecast_mpc import ForecastMPCStrategy, MPCPlan

    strategy = make_strategy(s, name, mechanism)
    if not isinstance(strategy, ForecastMPCStrategy):
        return None
    publication = PublicationModel()
    rng = np.random.default_rng(3)
    q_cut = cut // 15

    def noise(v: np.ndarray, start: int) -> np.ndarray:
        o = v.copy()
        o[start:] = rng.normal(0.0, 500.0, size=len(o) - start)
        return o

    da = market.da_price.copy()
    hidden = market.da_visible_from_minute >= cut
    da[hidden] = rng.normal(0.0, 500.0, size=int(hidden.sum()))
    perturbed = MarketData(
        **{
            **market.__dict__,
            "minute": {k: noise(v, cut) for k, v in market.minute.items()},
            "qh": {k: noise(v, q_cut) for k, v in market.qh.items()},
            "settlement_prices": {k: noise(v, q_cut) for k, v in market.settlement_prices.items()},
            "da_price": da,
        }
    )
    forecaster = strategy.forecaster.forecaster
    a = MPCPlan.build(forecaster.predict(market, publication), market, battery, publication, strategy.config)
    b = MPCPlan.build(forecaster.predict(perturbed, publication), perturbed, battery, publication, strategy.config)
    safe = min(cut + 2, q_cut * 15 + 20)
    used = np.unique(a.plan_row[:safe][a.plan_row[:safe] >= 0])
    return bool(
        np.allclose(a.price_discharge[:safe], b.price_discharge[:safe], equal_nan=True)
        and np.allclose(a.price_charge[:safe], b.price_charge[:safe], equal_nan=True)
        and np.array_equal(a.plan_row[:safe], b.plan_row[:safe])
        and np.allclose(a.slopes[used], b.slopes[used], equal_nan=True)
    )


def stage_audit(s: Settings) -> None:
    from euroflex_bess_lab.realtime import BatteryParams, check_no_lookahead

    battery = BatteryParams(power_mw=1.0, energy_mwh=2.0)
    have_rl = (s.cache / "rl_policy.npz").exists()
    rows = []
    slices = (
        AUDIT_SLICES
        if not s.quick
        else {
            "quick": (
                s.periods["B"][0],
                (utc(s.periods["B"][0]) + pd.Timedelta(days=3)).isoformat(),
                ("current", "new2026"),
            )
        }
    )
    for label, (start, end, mechanisms) in slices.items():
        market = load_market(start, end)
        for mechanism in mechanisms:
            names = [
                "threshold_price",
                "threshold_si",
                "forecast_mpc",
                "forecast_mpc_risk",
                "prism",
                "prism_risk",
                "oracle_qh",
            ]
            if have_rl and utc(start) >= utc("2026-01-01T00:00:00Z"):
                names.append("rl_dqn")
            if mechanism == "new2026":
                names += [
                    "ne_naive_frozen",
                    "ne_prism_zero_frozen",
                    "ne_prism_zero_wf",
                    "ne_prism_zero_live",
                    "direct_rebuilt",
                    "direct_rebuilt_live_risk",
                    "direct_rebuilt_curfeat",
                    "prism_live",
                ]
                if utc(start) >= utc(NF_TEST[0]):
                    names += ["ne_price_new2026", "ne_prism_new_published"]
            for name in names:
                try:
                    make_strategy(s, name, mechanism)
                except (FileNotFoundError, KeyError):
                    continue
                for cut in AUDIT_CUTS:
                    audit = check_no_lookahead(
                        lambda n=name, mech=mechanism: make_strategy(s, n, mech),
                        market,
                        battery,
                        cut_minute=cut,
                        mechanism=mechanism,
                    )
                    white = _plan_white_box(s, name, mechanism, market, battery, cut)
                    rows.append(
                        {
                            "slice": label,
                            "mechanism": mechanism,
                            "strategy": name,
                            "cut_minute": cut,
                            "compared_minutes": audit.compared_minutes,
                            "passed": audit.passed,
                            "first_divergence_minute": audit.first_divergence_minute,
                            "white_box_passed": white,
                        }
                    )
                print(
                    f"[audit] {label} {mechanism} {name}: "
                    f"{all(r['passed'] for r in rows if r['strategy'] == name and r['slice'] == label)}",
                    flush=True,
                )
        _FORECASTERS.clear()
    s.ledgers.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_json(s.ledgers / "realtime_lookahead_audit.json", orient="records", indent=1)


# ----------------------------------------------------------------------------------------------------------------
# report


def _load_group(s: Settings, key: str) -> tuple[dict[str, dict[str, Any]], dict[str, pd.DataFrame]]:
    out_dir = s.cache / "sim" / key
    summaries: dict[str, dict[str, Any]] = {}
    ledgers: dict[str, pd.DataFrame] = {}
    if not out_dir.exists():
        return summaries, ledgers
    for path in sorted(out_dir.glob("*.json")):
        if path.name.startswith("skill_"):
            continue
        summaries[path.stem] = json.loads(path.read_text())
        ledger = out_dir / f"{path.stem}.parquet"
        if ledger.exists():
            ledgers[path.stem] = pd.read_parquet(ledger)
    return summaries, ledgers


def _fmt(value: Any, digits: int = 0) -> str:
    if value is None or (isinstance(value, float) and not np.isfinite(value)):
        return "n/a"
    if isinstance(value, float | int | np.floating):
        return f"{value:,.{digits}f}"
    return str(value)


def _table(rows: list[dict[str, Any]], columns: list[tuple[str, str, int]]) -> str:
    head = "| " + " | ".join(c[1] for c in columns) + " |"
    sep = "| " + " | ".join("---" if i == 0 else "---:" for i in range(len(columns))) + " |"
    body = ["| " + " | ".join(_fmt(r.get(c[0]), c[2]) for c in columns) + " |" for r in rows]
    return "\n".join([head, sep, *body])


SUMMARY_COLUMNS = [
    ("strategy", "Strategy", 0),
    ("eur_per_mw_year", "EUR/MW/yr", 0),
    ("capture_vs_oracle", "Capture vs oracle", 3),
    ("cycles_per_day", "Cycles/day", 2),
    ("avg_sell_price_eur_mwh", "Avg sell EUR/MWh", 1),
    ("avg_buy_price_eur_mwh", "Avg buy EUR/MWh", 1),
    ("daily_pnl_cvar05_eur", "Daily CVaR5% EUR", 0),
    ("max_drawdown_eur", "Max drawdown EUR", 0),
    ("negative_day_share", "Loss days", 3),
]


def _ordered(summaries: dict[str, dict[str, Any]], order: tuple[str, ...]) -> list[dict[str, Any]]:
    return [summaries[n] for n in order if n in summaries]


def _export_ledgers(s: Settings) -> list[str]:
    s.ledgers.mkdir(parents=True, exist_ok=True)
    written = []
    for group in simulation_groups(s):
        if group.export is None:
            continue
        summaries, ledgers = _load_group(s, group.key)
        for name, ledger in ledgers.items():
            if name == "idle":
                continue
            suffix = "" if group.battery == "1mw_2mwh" else "_1mwh"
            if group.battery != "1mw_2mwh" and name not in ("forecast_mpc", "forecast_mpc_risk", "prism", "oracle_qh"):
                continue
            stem = f"realtime_{name}_{group.mechanism}_{group.export}{suffix}"
            frame = ledger.copy()
            for column in frame.columns:
                if frame[column].dtype == np.float64:
                    frame[column] = frame[column].astype(np.float32)
            frame["strategy"] = name
            frame["mechanism"] = group.mechanism
            frame["fleet_mw"] = np.float32(BATTERIES[group.battery]["power_mw"])
            frame.to_parquet(s.ledgers / f"{stem}.parquet", index=False, compression="zstd")
            summary = summaries[name]
            sidecar = {
                "strategy": name,
                "mechanism": group.mechanism,
                "period": group.export,
                "start_utc": summary["start_utc"],
                "end_utc": summary["end_utc"],
                "battery": summary["battery"],
                "publication": summary["publication"],
                "price_taker": True,
                "fleet_mw": frame["fleet_mw"].iloc[0].item(),
                "source": "scripts/research/realtime_backtest.py on data/public (Elia Open Data, CC BY 4.0)",
                "summary": {
                    k: summary[k]
                    for k in (
                        "eur_per_mw_year",
                        "cycles_per_day",
                        "pnl_eur",
                        "daily_pnl_cvar05_eur",
                        "capture_vs_oracle",
                    )
                    if k in summary
                },
            }
            (s.ledgers / f"{stem}.json").write_text(json.dumps(sidecar, indent=1, default=str))
            written.append(stem)
    written += _export_ablation_ledgers(s)
    return written


def _export_ablation_ledgers(s: Settings) -> list[str]:
    """Quarter-hour ledgers of the rebuilt-target ablation arms (``realtime_direct_rebuilt*``)."""
    written = []
    for key, label in ABLATION_EXPORT.items():
        summaries, ledgers = _load_group(s, key)
        for name, ledger in ledgers.items():
            if not name.startswith("direct_rebuilt"):
                continue
            stem = f"realtime_{name}_new2026_{label}"
            frame = ledger.copy()
            for column in frame.columns:
                if frame[column].dtype == np.float64:
                    frame[column] = frame[column].astype(np.float32)
            frame["strategy"] = name
            frame["mechanism"] = "new2026"
            frame["fleet_mw"] = np.float32(1.0)
            frame.to_parquet(s.ledgers / f"{stem}.parquet", index=False, compression="zstd")
            summary = summaries[name]
            sidecar = {
                "strategy": name,
                "mechanism": "new2026",
                "period": label,
                "start_utc": summary["start_utc"],
                "end_utc": summary["end_utc"],
                "battery": summary["battery"],
                "publication": summary["publication"],
                "price_taker": True,
                "fleet_mw": 1.0,
                "training": "frozen fold 2025-07-01..2026-01-01, targets rebuilt with reconstruct_new_formula_history",
                "source": "scripts/research/realtime_backtest.py on data/public (Elia Open Data, CC BY 4.0)",
                "summary": {k: summary[k] for k in ("eur_per_mw_year", "cycles_per_day", "pnl_eur") if k in summary},
            }
            (s.ledgers / f"{stem}.json").write_text(json.dumps(sidecar, indent=1, default=str))
            written.append(stem)
    return written


def _fit_deploy_task(name: str, spec: ForecasterSpec, gbm: dict[str, Any], out_path: str) -> dict[str, Any]:
    from euroflex_bess_lab.realtime import PublicationModel

    t0 = time.perf_counter()
    fold = spec.folds[0]
    market = load_market(fold.train_start - pd.Timedelta(days=2), fold.train_end)
    forecaster = build_forecaster(spec, gbm)
    forecaster.fit(market, [fold], PublicationModel())
    with open(out_path, "wb") as fh:
        pickle.dump(forecaster, fh, protocol=pickle.HIGHEST_PROTOCOL)
    return {"name": name, "seconds": time.perf_counter() - t0, **forecaster.fit_log_[0]}


def stage_deploy(s: Settings) -> None:
    """Fit the live-trading forecasters on the latest data (test window covers the live period)."""
    import shutil

    from euroflex_bess_lab.realtime import PublicationModel
    from euroflex_bess_lab.realtime.forecasting.deploy import (
        DEPLOY_FILES,
        FUSION_FILE,
        MODELS_ENV,
        RL_POLICY_FILE,
        deployment_fold,
    )
    from euroflex_bess_lab.realtime.forecasting.fusion import running_error_variance

    out = Path(os.environ[MODELS_ENV]).expanduser() if os.environ.get(MODELS_ENV) else s.cache / "models"
    out.mkdir(parents=True, exist_ok=True)
    fold = deployment_fold(s.periods["C"][1], data_start=DATA_START)
    specs = {
        "price_current": ForecasterSpec("deploy_price_current", "price", "current", folds=[fold]),
        "prism_current": ForecasterSpec("deploy_prism_current", "prism", "current", folds=[fold]),
        "prism_new2026": ForecasterSpec(
            "deploy_prism_zero", "prism", "new2026", components="reconstructed", folds=[fold]
        ),
    }
    logs = []
    ctx = get_context("spawn")
    with ProcessPoolExecutor(s.workers, mp_context=ctx, initializer=_init_worker, initargs=(s.threads,)) as pool:
        futures = [
            pool.submit(_fit_deploy_task, key, spec, s.gbm, str(out / DEPLOY_FILES[key])) for key, spec in specs.items()
        ]
        for future in as_completed(futures):
            log = future.result()
            logs.append(log)
            print(f"[deploy] {log['name']} fitted in {log['seconds']:.0f}s", flush=True)
    market = load_market(fold.train_start - pd.Timedelta(days=1), fold.train_end)
    variance = running_error_variance(
        market, "price_cum_eur_mwh", market.settlement("current"), PublicationModel(), fold.train_start, fold.train_end
    )
    (out / FUSION_FILE).write_text(
        json.dumps({"window": [fold.train_start.isoformat(), fold.train_end.isoformat()], "variance": list(variance)})
    )
    policy = s.cache / "rl_policy.npz"
    if policy.exists():
        s.ledgers.mkdir(parents=True, exist_ok=True)
        for target in (out / RL_POLICY_FILE, s.ledgers / "realtime_rl_policy.npz"):
            shutil.copyfile(policy, target)
            shutil.copyfile(policy.with_suffix(".json"), target.with_suffix(".json"))
    manifest = {
        "fold": fold.to_dict(),
        "forecasters": {key: DEPLOY_FILES[key] for key in specs},
        "fusion_variance": FUSION_FILE,
        "rl_policy": RL_POLICY_FILE if policy.exists() else None,
        "fit_logs": logs,
        "how_to_use": "euroflex live run --strategy prism | forecast-mpc | rl-dqn (models dir: $EUROFLEX_MODELS_DIR)",
    }
    (out / "deploy_manifest.json").write_text(json.dumps(manifest, indent=1, default=str))
    s.ledgers.mkdir(parents=True, exist_ok=True)
    (s.ledgers / "realtime_deploy_manifest.json").write_text(json.dumps(manifest, indent=1, default=str))
    print(f"[deploy] models written to the cache models directory ({len(specs)} forecasters)", flush=True)


def stage_report(s: Settings) -> None:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from realtime_report import write_report

    written = _export_ledgers(s)
    log_path = s.cache / "train_log.json"
    if log_path.exists():
        folds = json.loads(log_path.read_text())
        keep = ("spec", "fold", "train_start", "train_end", "test_start", "test_end", "train_rows_h0", "train_rows_h1")
        rows = sorted(({k: f.get(k) for k in keep} for f in folds), key=lambda r: (str(r["spec"]), str(r["fold"])))
        for row in rows:  # machine-checkable: no fold trains on or after its test window
            row["train_before_test"] = bool(utc(row["train_end"]) <= utc(row["test_start"]))
        (s.ledgers / "realtime_forecaster_folds.json").write_text(json.dumps(rows, indent=1))
    write_report(s, written)


# ----------------------------------------------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stages", default="semantics,train,rl,simulate,audit,report")
    parser.add_argument("--out", default=None, help="report root (default: reports)")
    parser.add_argument("--cache", default=None)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--threads", type=int, default=3)
    parser.add_argument("--rl-minutes", type=float, default=25.0)
    parser.add_argument("--rl-background", action="store_true", help="train RL in a parallel process during train")
    parser.add_argument("--only", default=None, help="simulate only groups whose key starts with this prefix")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    s = make_settings(args)
    s.cache.mkdir(parents=True, exist_ok=True)
    stages = [x.strip() for x in args.stages.split(",") if x.strip()]
    t0 = time.perf_counter()
    stage_fns: dict[str, Callable[[], None]] = {
        "semantics": lambda: stage_semantics(s),
        "train": lambda: stage_train(s),
        "rl": lambda: stage_rl(s),
        "simulate": lambda: stage_simulate(s, args.only),
        "audit": lambda: stage_audit(s),
        "report": lambda: stage_report(s),
        "deploy": lambda: stage_deploy(s),
    }
    rl_proc = None
    if args.rl_background and "rl" in stages:
        rl_proc = get_context("spawn").Process(target=_rl_process, args=(s,))
        rl_proc.start()
        stages = [x for x in stages if x != "rl"]
    for stage in stages:
        if stage == "simulate" and rl_proc is not None:
            rl_proc.join()
            rl_proc = None
        print(f"== stage {stage} ({time.perf_counter() - t0:.0f}s)", flush=True)
        stage_fns[stage]()
    if rl_proc is not None:
        rl_proc.join()
    print(f"== done in {time.perf_counter() - t0:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
