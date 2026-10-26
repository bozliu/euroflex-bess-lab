"""Out-of-sample evidence: forecasters trained only on data up to a cutoff T, frozen, and traded after T.

Regenerates ``reports/out_of_sample.md``, ``reports/figures/oos_*.png`` and ``reports/ledgers/oos_*`` (quarter-hour
ledgers of the frozen arms with JSON sidecars, daily P&L of every arm, and the machine-readable summary
``reports/ledgers/oos_summary.json``) from the committed public data set (``data/public``).

Design (fixed before the run): for each cutoff T (end of a local day in Europe/Brussels) the forecasters of the
real-time strategy stack (``realtime_backtest.py``: the direct price model behind the forecast MPC and the SCALES-L
state model) are fitted once on the six months before T, with the walk-forward embargo on target publication, and
never updated. They trade every minute of (T, end] next to the monthly-retrained (walk-forward) versions of the same
models, the reactive threshold rules and the perfect-foresight oracle, all on the same days. For the new 2026 formula
a zero-history SCALES-L model frozen at 2026-01-01 (it never sees a new-formula price) trades Jan-Jul 2026.

Stages (each caches its results outside the repository, ``$EUROFLEX_CACHE_DIR/out_of_sample`` or
``~/.cache/euroflex_bess_lab/out_of_sample``; ``--force`` recomputes):

- ``train``: fit the frozen forecasters (and any walk-forward model missing from the realtime-backtest cache);
- ``simulate``: run every arm on every evaluation window, plus monthly forecast skill;
- ``audit``: ``check_no_lookahead`` (and a white-box plan comparison) for every tradable arm on real-data slices;
- ``report``: statistics (paired daily differences, circular block bootstrap), ledgers, figures, report.

Usage::

    python scripts/research/out_of_sample.py                        # all stages (about 30-40 min on 3 workers)
    python scripts/research/out_of_sample.py --stages report        # rebuild the report from cached results
    python scripts/research/out_of_sample.py --quick --out <scratch dir>   # smoke test (short windows)
"""

from __future__ import annotations

import argparse
import json
import pickle
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field, replace
from multiprocessing import get_context
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import realtime_backtest as rb  # noqa: E402  (reused: forecaster specs, data loading, defaults, fold fitting)

from euroflex_bess_lab.realtime.forecasting import holdout  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
TZ = holdout.DEFAULT_TIMEZONE
CUTOFFS = ("2025-06-30", "2025-12-31", "2026-03-31")
END_DAY = "2026-10-24"
NF_CUTOFF = "2025-12-31"
NF_END_DAY = "2026-07-31"
BATTERY = {"power_mw": 1.0, "energy_mwh": 2.0, "charge_efficiency": 0.95, "discharge_efficiency": 0.95}
DEGRADATION_EUR_PER_MWH = 8.0
MINUTE_LAG, QH_FINAL_LAG = 1, 5
BLOCK_DAYS, N_BOOT, SEED, CONFIDENCE = 7, 10_000, 20261026, 0.95
AUDIT_CUTS = rb.AUDIT_CUTS
LATE_AUDIT = {"current": "2026-09-10T00:00:00Z", "new2026": "2026-05-04T00:00:00Z"}
AUDIT_DAYS = 3
THRESHOLDS = ("threshold_price", "threshold_si")
#: Primary arm of each mechanism, fixed before any result was computed (the headline reports it; others are context).
PRIMARY = {"current": "prism_risk_frozen", "new2026": "prism_zero_risk_frozen"}
ARMS = {
    "current": (
        "oracle_qh",
        "threshold_price",
        "threshold_si",
        "prism_risk_frozen",
        "forecast_mpc_risk_frozen",
        "prism_risk_wf",
        "forecast_mpc_risk_wf",
    ),
    "new2026": (
        "oracle_qh",
        "threshold_price",
        "threshold_si",
        "prism_zero_risk_frozen",
        "prism_zero_live_risk_frozen",
        "prism_zero_risk_wf",
        "prism_zero_live_risk_wf",
    ),
}
#: Optional ablation (``--ablation``): the MPC's price profile (stage 2..H climatology, terminal value) frozen at T as
#: well. Its results are written to the local cache only (``ablation_summary.json``), not to the public report.
ABLATION_ARMS = {
    "current": ("prism_risk_frozen_plan", "forecast_mpc_risk_frozen_plan"),
    "new2026": ("prism_zero_risk_frozen_plan",),
}
WALK_FORWARD = {
    "current": {
        "prism_risk_frozen": "prism_risk_wf",
        "forecast_mpc_risk_frozen": "forecast_mpc_risk_wf",
        "prism_risk_frozen_plan": "prism_risk_wf",
        "forecast_mpc_risk_frozen_plan": "forecast_mpc_risk_wf",
    },
    "new2026": {
        "prism_zero_risk_frozen": "prism_zero_risk_wf",
        "prism_zero_live_risk_frozen": "prism_zero_live_risk_wf",
        "prism_zero_risk_frozen_plan": "prism_zero_risk_wf",
    },
}
LABELS = {
    "oracle_qh": "Oracle (perfect foresight)",
    "threshold_price": "Threshold (running price)",
    "threshold_si": "Threshold (running SI)",
    "prism_risk_frozen": "SCALES-L, risk filter - frozen at T",
    "forecast_mpc_risk_frozen": "Forecast MPC, risk filter - frozen at T",
    "prism_risk_wf": "SCALES-L, risk filter - monthly retrain",
    "forecast_mpc_risk_wf": "Forecast MPC, risk filter - monthly retrain",
    "prism_risk_frozen_plan": "SCALES-L, risk filter - frozen at T, price profile frozen too",
    "forecast_mpc_risk_frozen_plan": "Forecast MPC, risk filter - frozen at T, price profile frozen too",
    "prism_zero_risk_frozen": "SCALES-L zero-history, risk filter - frozen 2026-01-01",
    "prism_zero_live_risk_frozen": "SCALES-L zero-history + live running price, risk filter - frozen 2026-01-01",
    "prism_zero_risk_wf": "SCALES-L zero-history, risk filter - monthly retrain",
    "prism_zero_live_risk_wf": "SCALES-L zero-history + live running price, risk filter - monthly retrain",
    "prism_zero_risk_frozen_plan": "SCALES-L zero-history, risk filter - frozen 2026-01-01, price profile frozen too",
}
#: frozen model kind -> (ForecasterSpec kind, mechanism, components)
FROZEN_KINDS = {
    "price_current": ("price", "current", "published"),
    "prism_current": ("prism", "current", "published"),
    "prism_zero": ("prism", "new2026", "reconstructed"),
}
#: walk-forward model -> (ForecasterSpec kind, mechanism, components); file names as in realtime_backtest.py
WF_KINDS = {
    "price_current": ("price", "current", "published"),
    "prism_current": ("prism", "current", "published"),
    "prism_zero_wf": ("prism", "new2026", "reconstructed"),
}


def utc(value: str | pd.Timestamp) -> pd.Timestamp:
    return rb.utc(value)


def publication() -> Any:
    from euroflex_bess_lab.realtime import PublicationModel

    return PublicationModel(minute_lag=MINUTE_LAG, qh_final_lag_minutes=QH_FINAL_LAG)


def battery_params() -> Any:
    from euroflex_bess_lab.realtime import BatteryParams

    return BatteryParams(**BATTERY, degradation_eur_per_mwh=DEGRADATION_EUR_PER_MWH)


# ----------------------------------------------------------------------------------------------------------------
# configuration


@dataclass
class Settings:
    out: Path
    cache: Path
    wf_dir: Path
    quick: bool = False
    workers: int = 3
    threads: int = 3
    force: bool = False
    gbm: dict[str, Any] = field(default_factory=dict)
    cutoffs: tuple[str, ...] = CUTOFFS
    end_day: str = END_DAY
    nf_cutoff: str = NF_CUTOFF
    nf_end_day: str = NF_END_DAY
    train_months: int = rb.TRAIN_MONTHS
    quick_train_days: int | None = None
    n_boot: int = N_BOOT
    audit_cuts: tuple[int, ...] = AUDIT_CUTS
    late_audit: bool = True
    profile_days: int = 28
    ablation: bool = False

    @property
    def ledgers(self) -> Path:
        return self.out / "ledgers"

    @property
    def figures(self) -> Path:
        return self.out / "figures"

    @property
    def models(self) -> Path:
        return self.cache / "models"


def make_settings(args: argparse.Namespace) -> Settings:
    out = Path(args.out) if args.out else REPO / "reports"
    root = rb.default_cache_root()
    cache = Path(args.cache) if args.cache else root / ("out_of_sample_quick" if args.quick else "out_of_sample")
    wf_dir = Path(args.wf_models) if args.wf_models else root / "realtime_backtest" / "models"
    s = Settings(out=out, cache=cache, wf_dir=wf_dir, quick=args.quick, workers=args.workers, threads=args.threads)
    s.force = args.force
    s.ablation = args.ablation
    if args.quick:
        s.cutoffs = ("2026-03-31",)
        s.end_day = "2026-04-10"
        s.nf_cutoff, s.nf_end_day = "2026-03-15", "2026-03-25"
        s.quick_train_days = 21
        s.gbm = {"max_iter": 30, "max_leaf_nodes": 15}
        s.n_boot = 500
        s.audit_cuts = (37, 1503)
        s.late_audit = False
        s.profile_days = 7
    return s


@dataclass(frozen=True)
class Evaluation:
    key: str
    mechanism: str
    split: holdout.HoldoutSplit
    arms: tuple[str, ...]

    @property
    def frozen_kinds(self) -> tuple[str, ...]:
        return ("price_current", "prism_current") if self.mechanism == "current" else ("prism_zero",)

    @property
    def wf_models(self) -> tuple[str, ...]:
        return ("price_current", "prism_current") if self.mechanism == "current" else ("prism_zero_wf",)


def make_split(s: Settings, cutoff: str, end_day: str) -> holdout.HoldoutSplit:
    split = holdout.holdout_split(cutoff, end_day, train_months=s.train_months, timezone=TZ, data_start=rb.DATA_START)
    if s.quick_train_days:
        split = replace(split, train_start_utc=split.cutoff_utc - pd.Timedelta(days=s.quick_train_days))
    return split


def arms_for(s: Settings, mechanism: str) -> tuple[str, ...]:
    return ARMS[mechanism] + (ABLATION_ARMS[mechanism] if s.ablation else ())


def walk_forward_pairs(mechanism: str, arms: tuple[str, ...]) -> dict[str, str]:
    """Frozen arm -> monthly-retrained counterpart, restricted to ``arms``."""
    return {k: v for k, v in WALK_FORWARD[mechanism].items() if k in arms and v in arms}


def evaluations(s: Settings) -> list[Evaluation]:
    out = [
        Evaluation(f"current_{c}", "current", make_split(s, c, s.end_day), arms_for(s, "current")) for c in s.cutoffs
    ]
    nf_split = make_split(s, s.nf_cutoff, s.nf_end_day)
    out.append(Evaluation(f"new2026_{s.nf_cutoff}", "new2026", nf_split, arms_for(s, "new2026")))
    return out


def evaluation_by_key(s: Settings, key: str) -> Evaluation:
    for ev in evaluations(s):
        if ev.key == key:
            return ev
    raise KeyError(key)


def frozen_model_path(s: Settings, kind: str, label: str) -> Path:
    return s.models / f"frozen_{kind}_{label}.pkl"


# ----------------------------------------------------------------------------------------------------------------
# walk-forward models: reuse the realtime-backtest cache when it covers the windows, else fit the missing folds


def needed_window(s: Settings, name: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    evs = [ev for ev in evaluations(s) if name in ev.wf_models]
    start = min(ev.split.cutoff_utc for ev in evs) - pd.Timedelta(days=rb.WARMUP_DAYS)
    return start, max(ev.split.test_end_utc for ev in evs)


def wf_folds_needed(s: Settings, name: str) -> list[Any]:
    from euroflex_bess_lab.realtime.forecasting import monthly_folds

    start, end = needed_window(s, name)
    if name == "prism_zero_wf":  # monthly retrain on current-design state only, as in realtime_backtest.py
        start = max(start, utc("2026-01-01T00:00:00Z"))
    return monthly_folds(start, end, data_start=rb.DATA_START, train_months=rb.TRAIN_MONTHS)


def fold_coverage(forecaster: Any, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, Any]:
    """Which part of ``[start, end)`` the walk-forward folds predict, and whether every fold trains before it tests."""
    folds = sorted(forecaster.folds_, key=lambda f: f.test_start)
    covered = pd.Timedelta(0)
    for f in folds:
        lo, hi = max(f.test_start, start), min(f.test_end, end)
        if hi > lo:
            covered += hi - lo
    gaps = [(a.test_end, b.test_start) for a, b in zip(folds, folds[1:], strict=False) if b.test_start > a.test_end]
    return {
        "n_folds": len(folds),
        "first_test_start_utc": folds[0].test_start.isoformat() if folds else None,
        "last_test_end_utc": folds[-1].test_end.isoformat() if folds else None,
        "coverage_share": float(covered / (end - start)) if end > start else 0.0,
        "gaps": [(a.isoformat(), b.isoformat()) for a, b in gaps],
        "all_train_before_test": bool(all(f.train_end <= f.test_start for f in folds)),
        "folds": [f.to_dict() for f in folds],
    }


def wf_model_path(s: Settings, name: str) -> Path:
    """The realtime-backtest model if present (same specification), else this script's own walk-forward fit."""
    shared = s.wf_dir / f"{name}.pkl"
    own = s.models / f"wf_{name}.pkl"
    if own.exists():
        return own
    return shared


def _wf_usable(s: Settings, name: str) -> bool:
    path = wf_model_path(s, name)
    if not path.exists():
        return False
    with open(path, "rb") as fh:
        forecaster = pickle.load(fh)
    start, end = needed_window(s, name)
    if name == "prism_zero_wf":
        start = max(start, utc("2026-01-01T00:00:00Z"))
    cov = fold_coverage(forecaster, start, end)
    return cov["coverage_share"] > 0.999 and cov["all_train_before_test"]


# ----------------------------------------------------------------------------------------------------------------
# train


def _fit_frozen_task(
    kind: str, split: holdout.HoldoutSplit, gbm: dict[str, Any], out_path: str, profile_days: int
) -> dict[str, Any]:
    from euroflex_bess_lab.realtime.forecasting.fusion import running_error_variance

    t0 = time.perf_counter()
    spec_kind, mechanism, components = FROZEN_KINDS[kind]
    name = f"frozen_{kind}_{split.label}"
    spec = rb.ForecasterSpec(name, spec_kind, mechanism, components=components, folds=[split.fold])
    # load nothing at or after T: the market ends at the cutoff
    market = rb.load_market(split.train_start_utc - pd.Timedelta(days=2), split.cutoff_utc)
    forecaster = rb.build_forecaster(spec, gbm)
    audit = holdout.fit_frozen(forecaster, market, split, publication())
    profile = holdout.history_profile(forecaster.history_prices(market), market, split.cutoff_utc, days=profile_days)
    variance = None
    if mechanism == "new2026":  # fusion with the live running new price: current-formula error profile before T
        variance = running_error_variance(
            market,
            "price_cum_eur_mwh",
            market.settlement("current"),
            publication(),
            split.train_start_utc,
            split.cutoff_utc,
        )
    bundle = {
        "kind": kind,
        "forecaster": forecaster,
        "split": split,
        "profile": profile,
        "fusion_variance": variance,
        "training_audit": audit,
        "fit_log": forecaster.fit_log_,
    }
    with open(out_path, "wb") as fh:
        pickle.dump(bundle, fh, protocol=pickle.HIGHEST_PROTOCOL)
    log = {
        "model": f"frozen_{kind}",
        "kind": kind,
        "label": split.label,
        "seconds": time.perf_counter() - t0,
        "gbm": gbm,
        "fit_log": forecaster.fit_log_,
        **{k: audit[k] for k in ("passed", "cutoff_utc", "n_folds", "fit_market_end_utc", "fit_market_ends_by_cutoff")},
        "training_rows": audit["training_rows"],
        "fusion_variance": None if variance is None else [None if not np.isfinite(v) else float(v) for v in variance],
    }
    Path(out_path).with_suffix(".json").write_text(json.dumps(holdout.to_jsonable(log), indent=1))
    return log


def stage_train(s: Settings) -> None:
    from euroflex_bess_lab.realtime.forecasting import merge_forecasters

    s.models.mkdir(parents=True, exist_ok=True)
    tasks: list[tuple[str, Any]] = []
    for ev in evaluations(s):
        for kind in ev.frozen_kinds:
            path = frozen_model_path(s, kind, ev.split.label)
            if path.exists() and not s.force:
                continue
            tasks.append(("frozen", (kind, ev.split, s.gbm, str(path), s.profile_days)))
    wf_specs: dict[str, Any] = {}
    for name, (spec_kind, mechanism, components) in WF_KINDS.items():
        if not any(name in ev.wf_models for ev in evaluations(s)):
            continue
        if _wf_usable(s, name) and not s.force:
            print(f"[train] walk-forward `{name}`: reusing {wf_model_path(s, name).name}", flush=True)
            continue
        spec = rb.ForecasterSpec(
            f"wf_{name}", spec_kind, mechanism, components=components, folds=wf_folds_needed(s, name)
        )
        wf_specs[name] = spec
        part_dir = s.models / "parts"
        part_dir.mkdir(parents=True, exist_ok=True)
        for i, fold in enumerate(spec.folds):
            part = part_dir / f"wf_{name}__{fold.label}.pkl"
            if part.exists() and not s.force:
                continue
            tasks.append(("wf", (spec, i, s.gbm, str(part))))
    print(f"[train] {len(tasks)} fits on {s.workers} workers", flush=True)
    if tasks:
        ctx = get_context("spawn")
        with ProcessPoolExecutor(s.workers, mp_context=ctx, initializer=rb._init_worker, initargs=(s.threads,)) as pool:
            futures = {}
            for kind, payload in tasks:
                fn = _fit_frozen_task if kind == "frozen" else rb._fit_fold_task
                futures[pool.submit(fn, *payload)] = kind
            for n, future in enumerate(as_completed(futures), 1):
                log = future.result()
                label = log.get("model", log.get("spec"))
                print(f"[train] {n}/{len(tasks)} {label} {log.get('label', log.get('fold'))} {log['seconds']:.0f}s")
    for name, spec in wf_specs.items():
        parts = []
        for fold in spec.folds:
            with open(s.models / "parts" / f"wf_{name}__{fold.label}.pkl", "rb") as fh:
                parts.append(pickle.load(fh))
        with open(s.models / f"wf_{name}.pkl", "wb") as fh:
            pickle.dump(merge_forecasters(parts), fh, protocol=pickle.HIGHEST_PROTOCOL)


# ----------------------------------------------------------------------------------------------------------------
# arms


class ModelSet:
    """Forecasters of one evaluation, each wrapped once in ``CachedForecaster`` so arms share predictions."""

    def __init__(self, s: Settings, ev: Evaluation) -> None:
        self.s, self.ev = s, ev
        self._bundles: dict[str, dict[str, Any]] = {}
        self._cached: dict[str, Any] = {}

    def bundle(self, kind: str) -> dict[str, Any]:
        if kind not in self._bundles:
            with open(frozen_model_path(self.s, kind, self.ev.split.label), "rb") as fh:
                bundle = pickle.load(fh)
            holdout.assert_frozen(bundle["forecaster"], self.ev.split.cutoff_utc)
            if bundle["split"] != self.ev.split:
                raise ValueError(f"frozen model {kind} was fitted for another split: {bundle['split']}")
            self._bundles[kind] = bundle
        return self._bundles[kind]

    def frozen(self, kind: str) -> Any:
        from euroflex_bess_lab.realtime.strategies.forecast_mpc import CachedForecaster

        key = f"frozen_{kind}"
        if key not in self._cached:
            self._cached[key] = CachedForecaster(self.bundle(kind)["forecaster"])
        return self._cached[key]

    def wf(self, name: str) -> Any:
        from euroflex_bess_lab.realtime.strategies.forecast_mpc import CachedForecaster

        key = f"wf_{name}"
        if key not in self._cached:
            with open(wf_model_path(self.s, name), "rb") as fh:
                forecaster = pickle.load(fh)
            # keep only the folds this evaluation can use (memory); predictions of kept folds are unchanged
            lo = self.ev.split.cutoff_utc - pd.Timedelta(days=rb.WARMUP_DAYS + 1)
            hi = self.ev.split.test_end_utc
            keep = [i for i, f in enumerate(forecaster.folds_) if f.test_end > lo and f.test_start < hi]
            forecaster.folds_ = [forecaster.folds_[i] for i in keep]
            forecaster.fold_models_ = [forecaster.fold_models_[i] for i in keep]
            forecaster.fit_log_ = [forecaster.fit_log_[i] for i in keep]
            self._cached[key] = CachedForecaster(forecaster)
        return self._cached[key]

    def live(self, base: Any, key: str) -> Any:
        from euroflex_bess_lab.realtime.forecasting.fusion import RunningEstimateFusion
        from euroflex_bess_lab.realtime.strategies.forecast_mpc import CachedForecaster

        if key not in self._cached:
            variance = self.bundle("prism_zero")["fusion_variance"]
            fused = RunningEstimateFusion(base, "nf_imbalance_price_eur_mwh", variance, name=key)
            self._cached[key] = CachedForecaster(fused)
        return self._cached[key]

    def frozen_plan(self, kind: str) -> Any:
        key = f"frozen_plan_{kind}"
        if key not in self._cached:
            bundle = self.bundle(kind)
            self._cached[key] = holdout.FrozenHistoryForecaster(
                self.frozen(kind), self.ev.split.cutoff_utc, bundle["profile"], name=key
            )
        return self._cached[key]


def make_arm(name: str, ev: Evaluation, models: ModelSet) -> Any:
    from euroflex_bess_lab.realtime import OracleQHStrategy, ThresholdStrategy
    from euroflex_bess_lab.realtime.strategies.forecast_mpc import ForecastMPCStrategy, MPCConfig
    from euroflex_bess_lab.realtime.strategies.prism import PrismStrategy

    risk = MPCConfig(risk="quantile", risk_quantile=rb.RISK_QUANTILE)
    if name == "oracle_qh":
        return OracleQHStrategy()
    if name == "threshold_price":
        signal = "price_cum_eur_mwh" if ev.mechanism == "current" else "nf_imbalance_price_eur_mwh"
        return ThresholdStrategy(mode="price", signal_field=signal, name=name)
    if name == "threshold_si":
        return ThresholdStrategy(mode="si", signal_field="si_cum_mw", name=name)
    builders: dict[str, Any] = {
        "prism_risk_frozen": lambda: PrismStrategy(models.frozen("prism_current"), risk, name=name),
        "forecast_mpc_risk_frozen": lambda: ForecastMPCStrategy(models.frozen("price_current"), risk, name=name),
        "prism_risk_wf": lambda: PrismStrategy(models.wf("prism_current"), risk, name=name),
        "forecast_mpc_risk_wf": lambda: ForecastMPCStrategy(models.wf("price_current"), risk, name=name),
        "prism_risk_frozen_plan": lambda: PrismStrategy(models.frozen_plan("prism_current"), risk, name=name),
        "forecast_mpc_risk_frozen_plan": lambda: ForecastMPCStrategy(
            models.frozen_plan("price_current"), risk, name=name
        ),
        "prism_zero_risk_frozen": lambda: PrismStrategy(models.frozen("prism_zero"), risk, name=name),
        "prism_zero_live_risk_frozen": lambda: PrismStrategy(
            models.live(models.frozen("prism_zero"), "frozen_prism_zero_live"), risk, name=name
        ),
        "prism_zero_risk_wf": lambda: PrismStrategy(models.wf("prism_zero_wf"), risk, name=name),
        "prism_zero_live_risk_wf": lambda: PrismStrategy(
            models.live(models.wf("prism_zero_wf"), "wf_prism_zero_live"), risk, name=name
        ),
        "prism_zero_risk_frozen_plan": lambda: PrismStrategy(models.frozen_plan("prism_zero"), risk, name=name),
    }
    if name not in builders:
        raise KeyError(name)
    return builders[name]()


#: arm -> forecaster used, for the monthly skill tables
SKILL_MODELS = {
    "current": {
        "frozen_price_current": ("frozen", "price_current"),
        "frozen_prism_current": ("frozen", "prism_current"),
        "wf_price_current": ("wf", "price_current"),
        "wf_prism_current": ("wf", "prism_current"),
    },
    "new2026": {
        "frozen_prism_zero": ("frozen", "prism_zero"),
        "frozen_prism_zero_live": ("live", "prism_zero"),
        "wf_prism_zero_wf": ("wf", "prism_zero_wf"),
    },
}


def _skill_forecaster(models: ModelSet, role: str, name: str) -> Any:
    if role == "frozen":
        return models.frozen(name)
    if role == "live":
        return models.live(models.frozen(name), "frozen_prism_zero_live")
    return models.wf(name)


# ----------------------------------------------------------------------------------------------------------------
# simulate


def _monthly_skill(forecaster: Any, market: Any, ev: Evaluation) -> list[dict[str, Any]]:
    from euroflex_bess_lab.realtime.forecasting import forecast_skill

    frame = forecaster.predict(market, publication())
    prices = market.settlement(ev.mechanism)
    si_field = "nf_si_mw" if ev.mechanism == "new2026" else "si_mw"
    month_of_minute = np.asarray(market.minute_times.tz_convert(TZ).strftime("%Y-%m"))
    m0 = holdout.minute_index(market, ev.split.cutoff_utc)
    after = np.arange(market.n_minutes) >= m0
    rows_out = []
    for month in ev.split.months():
        rows = np.flatnonzero((month_of_minute == month) & after)
        if len(rows) == 0:
            continue
        table = forecast_skill(
            frame.current,
            market,
            prices,
            si_qh=market.qh.get(si_field),
            horizon=0,
            rows=rows,
            publication=publication(),
        )
        allrow = table.loc["all"]
        rows_out.append(
            {
                "month": month,
                "months_since_cutoff": ev.split.months_since_cutoff(month),
                "n": int(allrow["n"]),
                **{
                    k: float(allrow[k])
                    for k in ("mae_mean", "pinball", "crps", "brier", "brier_climatology")
                    if k in allrow
                },
            }
        )
    return rows_out


def _simulate_task(s: Settings, key: str) -> dict[str, Any]:
    from euroflex_bess_lab.realtime import simulate

    ev = evaluation_by_key(s, key)
    out_dir = s.cache / "sim" / ev.key
    out_dir.mkdir(parents=True, exist_ok=True)
    t_load = time.perf_counter()
    market = rb.load_market(ev.split.cutoff_utc - pd.Timedelta(days=rb.WARMUP_DAYS), ev.split.test_end_utc)
    load_seconds = time.perf_counter() - t_load
    battery = battery_params()
    models = ModelSet(s, ev)
    timings: dict[str, float] = {"load_market": load_seconds}
    for name in ev.arms:
        path, meta = out_dir / f"{name}.parquet", out_dir / f"{name}.json"
        if path.exists() and meta.exists() and not s.force:
            continue
        t0 = time.perf_counter()
        strategy = make_arm(name, ev, models)
        result = simulate(
            market, strategy, battery, mechanism=ev.mechanism, publication=publication(), record_minutes=False
        )
        qh = holdout.trim_to_test(result.qh, ev.split)
        qh.to_parquet(path, index=False)
        seconds = time.perf_counter() - t0
        timings[name] = seconds
        pnl = float(qh["pnl_eur"].sum())
        years = len(qh) * 0.25 / (24 * 365.25)
        meta.write_text(json.dumps({"arm": name, "evaluation": ev.key, "pnl_eur": pnl, "seconds": seconds}, indent=1))
        print(f"[sim] {ev.key} {name}: {pnl / years:,.0f} EUR/MW/yr ({seconds:.0f}s)", flush=True)
    skill_path = out_dir / "skill.json"
    if not skill_path.exists() or s.force:
        t0 = time.perf_counter()
        skill = {
            label: _monthly_skill(_skill_forecaster(models, role, name), market, ev)
            for label, (role, name) in SKILL_MODELS[ev.mechanism].items()
        }
        skill_path.write_text(json.dumps(holdout.to_jsonable(skill), indent=1))
        timings["skill"] = time.perf_counter() - t0
    timing_path = out_dir / "timings.json"
    previous = json.loads(timing_path.read_text()) if timing_path.exists() else {}
    timing_path.write_text(json.dumps({**previous, **timings}, indent=1))
    return {"evaluation": ev.key, "seconds": sum(timings.values())}


def _run_pool(s: Settings, fn: Any, keys: list[Any]) -> list[Any]:
    results = []
    ctx = get_context("spawn")
    with ProcessPoolExecutor(s.workers, mp_context=ctx, initializer=rb._init_worker, initargs=(s.threads,)) as pool:
        futures = {pool.submit(fn, s, key): key for key in keys}
        for future in as_completed(futures):
            results.append(future.result())
    return results


def stage_simulate(s: Settings) -> None:
    evs = sorted(evaluations(s), key=lambda ev: -ev.split.test_days)  # largest first
    print(f"[sim] {len(evs)} evaluations on {s.workers} workers", flush=True)
    for result in _run_pool(s, _simulate_task, [ev.key for ev in evs]):
        print(f"[sim] finished {result['evaluation']} ({result['seconds']:.0f}s)", flush=True)


# ----------------------------------------------------------------------------------------------------------------
# audit


@dataclass(frozen=True)
class AuditSlice:
    label: str
    start: pd.Timestamp
    end: pd.Timestamp
    mechanism: str
    arms: tuple[tuple[str, str], ...]  # (evaluation key, arm)


def audit_slices(s: Settings) -> list[AuditSlice]:
    out: list[AuditSlice] = []
    days = pd.Timedelta(days=AUDIT_DAYS if not s.quick else 2)
    by_mech: dict[str, list[Evaluation]] = {}
    for ev in evaluations(s):
        by_mech.setdefault(ev.mechanism, []).append(ev)
    for mechanism, evs in by_mech.items():
        shared = ("oracle_qh", *THRESHOLDS, *dict.fromkeys(WALK_FORWARD[mechanism].values()))
        for ev in evs:  # early slice of every evaluation: its frozen arms plus the shared arms
            start = ev.split.cutoff_utc + pd.Timedelta(days=6 if not s.quick else 2)
            frozen = tuple((ev.key, a) for a in ev.arms if a not in shared)
            out.append(
                AuditSlice(
                    f"{mechanism}:{start.tz_convert(TZ):%Y-%m-%d}",
                    start,
                    start + days,
                    mechanism,
                    (*frozen, *((ev.key, a) for a in shared)),
                )
            )
        if s.late_audit:  # one late slice inside every window of this mechanism: all frozen arms, shared arms once
            start = utc(LATE_AUDIT[mechanism])
            inside = [ev for ev in evs if ev.split.cutoff_utc <= start and start + days <= ev.split.test_end_utc]
            if inside:
                arms = [(ev.key, a) for ev in inside for a in ev.arms if a not in shared]
                arms += [(inside[0].key, a) for a in shared]
                out.append(
                    AuditSlice(
                        f"{mechanism}:{start.tz_convert(TZ):%Y-%m-%d}", start, start + days, mechanism, tuple(arms)
                    )
                )
    return out


def _white_box(make: Any, market: Any, battery: Any, cut: int) -> bool | None:
    """Plan arrays (risk prices, value-function slopes) must be identical before the earliest legitimate use."""
    from euroflex_bess_lab.realtime.audit import _perturbed
    from euroflex_bess_lab.realtime.strategies.forecast_mpc import ForecastMPCStrategy, MPCPlan

    strategy = make()
    if not isinstance(strategy, ForecastMPCStrategy):
        return None
    pub = publication()
    q_cut = cut // 15
    safe = min(cut + pub.minute_lag + 1, q_cut * 15 + 15 + pub.qh_final_lag_minutes, market.n_minutes)
    perturbed = _perturbed(market, cut_minute=cut, safe_until=safe, seed=3)
    a = MPCPlan.build(strategy.forecaster.predict(market, pub), market, battery, pub, strategy.config)
    b = MPCPlan.build(strategy.forecaster.predict(perturbed, pub), perturbed, battery, pub, strategy.config)
    used = np.unique(a.plan_row[:safe][a.plan_row[:safe] >= 0])
    return bool(
        np.allclose(a.price_discharge[:safe], b.price_discharge[:safe], equal_nan=True)
        and np.allclose(a.price_charge[:safe], b.price_charge[:safe], equal_nan=True)
        and np.array_equal(a.plan_row[:safe], b.plan_row[:safe])
        and np.allclose(a.slopes[used], b.slopes[used], equal_nan=True)
    )


def _audit_task(s: Settings, index: int) -> list[dict[str, Any]]:
    from euroflex_bess_lab.realtime import check_no_lookahead

    sl = audit_slices(s)[index]
    market = rb.load_market(sl.start, sl.end)
    battery = battery_params()
    sets: dict[str, ModelSet] = {}
    rows = []
    for ev_key, arm in sl.arms:
        ev = evaluation_by_key(s, ev_key)
        models = sets.setdefault(ev_key, ModelSet(s, ev))

        def make(arm: str = arm, ev: Evaluation = ev, models: ModelSet = models) -> Any:
            return make_arm(arm, ev, models)

        for cut in s.audit_cuts:
            audit = check_no_lookahead(
                make, market, battery, cut_minute=cut, mechanism=sl.mechanism, publication=publication()
            )
            rows.append(
                {
                    "slice": sl.label,
                    "start_utc": sl.start.isoformat(),
                    "end_utc": sl.end.isoformat(),
                    "mechanism": sl.mechanism,
                    "evaluation": ev_key,
                    "arm": arm,
                    "tradable": arm != "oracle_qh",
                    "cut_minute": cut,
                    "compared_minutes": audit.compared_minutes,
                    "passed": audit.passed,
                    "first_divergence_minute": audit.first_divergence_minute,
                    "white_box_passed": _white_box(make, market, battery, cut),
                }
            )
        ok = all(r["passed"] for r in rows if r["arm"] == arm and r["evaluation"] == ev_key)
        print(f"[audit] {sl.label} {ev_key} {arm}: {'pass' if ok else 'FAIL'}", flush=True)
    return rows


def stage_audit(s: Settings) -> None:
    slices = audit_slices(s)
    print(f"[audit] {len(slices)} slices, {sum(len(x.arms) for x in slices)} arm audits x {len(s.audit_cuts)} cuts")
    rows: list[dict[str, Any]] = []
    for part in _run_pool(s, _audit_task, list(range(len(slices)))):
        rows.extend(part)
    rows.sort(key=lambda r: (r["slice"], r["evaluation"], r["arm"], r["cut_minute"]))
    (s.cache / "audit.json").write_text(json.dumps(holdout.to_jsonable(rows), indent=1))


# ----------------------------------------------------------------------------------------------------------------
# report


def load_ledgers(s: Settings, ev: Evaluation) -> dict[str, pd.DataFrame]:
    out_dir = s.cache / "sim" / ev.key
    return {
        name: pd.read_parquet(out_dir / f"{name}.parquet") for name in ev.arms if (out_dir / f"{name}.parquet").exists()
    }


def stage_report(s: Settings, timings: dict[str, float]) -> None:
    from out_of_sample_report import write_outputs  # same directory (on sys.path, see the imports above)

    write_outputs(s, timings, sys.modules[__name__])


# ----------------------------------------------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stages", default="train,simulate,audit,report")
    parser.add_argument("--out", default=None, help="report root (default: reports)")
    parser.add_argument("--cache", default=None)
    parser.add_argument("--wf-models", default=None, help="directory with walk-forward models (realtime_backtest)")
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--threads", type=int, default=3)
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--ablation", action="store_true", help="also run the frozen-price-profile ablation (local cache output only)"
    )
    args = parser.parse_args(argv)
    s = make_settings(args)
    s.cache.mkdir(parents=True, exist_ok=True)
    stages = [x.strip() for x in args.stages.split(",") if x.strip()]
    timing_path = s.cache / "stage_seconds.json"
    timings: dict[str, float] = json.loads(timing_path.read_text()) if timing_path.exists() else {}
    t0 = time.perf_counter()
    for stage in stages:
        print(f"== stage {stage} ({time.perf_counter() - t0:.0f}s)", flush=True)
        t_stage = time.perf_counter()
        if stage == "train":
            stage_train(s)
        elif stage == "simulate":
            stage_simulate(s)
        elif stage == "audit":
            stage_audit(s)
        elif stage == "report":
            stage_report(s, timings)
        else:
            raise SystemExit(f"unknown stage {stage}")
        if stage != "report":
            timings[stage] = time.perf_counter() - t_stage
            timing_path.write_text(json.dumps(timings, indent=1))
    print(f"== done in {time.perf_counter() - t0:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
