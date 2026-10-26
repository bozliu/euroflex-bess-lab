"""Impact study: how the Belgian imbalance price responds when a fleet of size Q deviates (both mechanisms).

Regenerates every number, table and figure of ``reports/impact.md`` from the committed public data (``data/public``):

1. fit the impact operators (volume -> price curves with first-difference fixed effects, alpha model, activation
   pass-through) and back-test them on hold-out periods;
2. capacity-value curve: per-MW value of a fleet of Q MW (price-taking ThresholdStrategy replicated across the fleet
   versus the impact-aware threshold policy) under both formulas on the Jan-Jul 2026 natural-experiment window and
   on 2025 (current formula only) -> ``reports/ledgers/impact_capacity_value_curve.parquet`` (+ json);
3. multi-fleet study: N = 4 fleets, closed-loop herding versus naive / Nash / central best responses as total
   implicit capacity grows -> ``reports/ledgers/impact_equilibrium.parquet`` (+ json);
4. figures ``reports/figures/impact_*.png`` and the report ``reports/impact.md``.

Usage::

    .venv/bin/python scripts/research/impact_study.py            # full study (~20-30 min on a laptop)
    .venv/bin/python scripts/research/impact_study.py --quick --out /tmp/impact_quick   # smoke run
    .venv/bin/python scripts/research/impact_study.py --report-only  # re-render figures + report from saved results

Battery: 1 MW / 2 MWh unit, 95 % / 95 % one-way efficiency, SOC 5-95 %, degradation 8 EUR/MWh discharged,
publication lags of the real-time core (minute rows usable one minute after they end, final QH prices five
minutes after the QH). No real orders; every policy is audited for look-ahead in the test-suite.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from euroflex_bess_lab import public_data as pdata
from euroflex_bess_lab.realtime import BatteryParams, MarketData, PublicationModel
from euroflex_bess_lab.realtime.impact import (
    CurrentFormulaImpact,
    NewFormula2026Impact,
    RegimeCurves,
    ResponseCurve,
    current_state_from_market,
    fit_alpha_model,
    fit_current_response,
    fit_new_formula_response,
    new_state_from_market,
    operator_for,
)
from euroflex_bess_lab.realtime.impact.channels import channel_value, current_channels, new_channels
from euroflex_bess_lab.realtime.impact.equilibrium import (
    FLEET_POLICIES,
    RunningResponse,
    make_fleets,
    simulate_fleet_game,
)
from euroflex_bess_lab.realtime.impact.fleet import (
    CURVE_COLUMNS,
    FLEET_SIZES_MW,
    PolicySpec,
    capacity_value_curve,
    curve_point,
    curve_sidecar,
    resettle,
    resettle_ledger,
)
from euroflex_bess_lab.realtime.impact.policies import (
    ImpactAwareThresholdStrategy,
    VolumeFromSI,
    price_taking_threshold,
)
from euroflex_bess_lab.realtime.impact.response import BidCurveDelta, CurveDelta
from euroflex_bess_lab.realtime.impact.validation import (
    consecutive_pairs,
    error_stats,
    validate_curve_changes,
    validate_operator_si_shift,
)
from euroflex_bess_lab.realtime.simulator import simulate

REPO = Path(__file__).resolve().parents[2]
BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)
PUBLICATION = PublicationModel()
N_FLEETS = 4
EQ_CAPACITIES_MW = (100.0, 200.0, 400.0, 800.0, 1600.0, 2400.0, 3200.0)
LEDGER_SIZES_MW = (1.0, 100.0, 400.0, 1600.0)
SENSITIVITY_SIZES_MW = (100.0, 400.0, 1600.0)

FULL = {
    "windows": {
        "2026H1": ("2026-01-01", "2026-08-01", ("current", "new2026")),
        "2025": ("2025-01-01", "2026-01-01", ("current",)),
    },
    "current_folds": [
        ("2024H2->2025H1", ("2024-06-01", "2025-01-01"), ("2025-01-01", "2025-07-01")),
        ("2025H1->2025H2", ("2025-01-01", "2025-07-01"), ("2025-07-01", "2026-01-01")),
        ("2025H2->2026H1", ("2025-07-01", "2026-01-01"), ("2026-01-01", "2026-08-01")),
    ],
    "new_folds": [
        ("2026Q1->2026AprJul", ("2026-01-01", "2026-04-01"), ("2026-04-01", "2026-08-01")),
        ("2026AprJul->2026Q1", ("2026-04-01", "2026-08-01"), ("2026-01-01", "2026-04-01")),
    ],
    "half_years": [
        ("2024H2", "2024-06-01", "2025-01-01"),
        ("2025H1", "2025-01-01", "2025-07-01"),
        ("2025H2", "2025-07-01", "2026-01-01"),
        ("2026H1", "2026-01-01", "2026-08-01"),
    ],
    "alpha_period": ("2024-06-01", "2026-08-01"),
    "base_current": ("2024-06-01", "2026-08-01"),
    "base_new": ("2026-01-01", "2026-08-01"),
    "fleet_sizes": FLEET_SIZES_MW,
    "eq_capacities": EQ_CAPACITIES_MW,
    # what an optimiser that only knows history would believe (impact-aware sensitivity)
    "prior_fits": {
        "2026H1:current": ("2025-07-01", "2026-01-01", "2025H2"),
        "2025:current": ("2024-06-01", "2025-01-01", "2024H2"),
        "2026H1:new2026": ("2026-01-01", "2026-04-01", "2026Q1 (inside the window)"),
    },
}
QUICK = {
    "windows": {
        "2026H1": ("2026-03-02", "2026-03-16", ("current", "new2026")),
        "2025": ("2025-06-02", "2025-06-16", ("current",)),
    },
    "current_folds": [("2026Mar1->2026Mar2", ("2026-03-02", "2026-03-09"), ("2026-03-09", "2026-03-16"))],
    "new_folds": [("2026Mar1->2026Mar2", ("2026-03-02", "2026-03-09"), ("2026-03-09", "2026-03-16"))],
    "half_years": [("2026Mar", "2026-03-02", "2026-03-16")],
    "alpha_period": ("2026-02-01", "2026-03-16"),
    "base_current": ("2025-06-01", "2026-03-16"),
    "base_new": ("2026-03-02", "2026-03-16"),
    "fleet_sizes": (1.0, 100.0, 800.0),
    "eq_capacities": (100.0, 800.0),
}


def log(msg: str) -> None:
    print(f"[impact {time.strftime('%H:%M:%S')}] {msg}", flush=True)


class Loader:
    """Loads one base :class:`MarketData` per flavour (with / without the 2026-formula tables) and slices it."""

    def __init__(self, base_current: tuple[str, str], base_new: tuple[str, str]) -> None:
        self._spans = {False: base_current, True: base_new}
        self._base: dict[bool, MarketData] = {}

    def __call__(self, start: str, end: str, *, new_formula: bool = False) -> MarketData:
        if new_formula not in self._base:
            a, b = self._spans[new_formula]
            log(f"load {a}..{b} (new formula: {new_formula})")
            self._base[new_formula] = MarketData.from_public(
                a, b, include_new_formula=new_formula, include_si_forecast=False, include_day_ahead=False
            )
        base = self._base[new_formula]
        if pd.Timestamp(start, tz="UTC") < base.start_utc or pd.Timestamp(end, tz="UTC") > base.end_utc:
            raise ValueError(f"{start}..{end} is outside the loaded span {base.start_utc}..{base.end_utc}")
        return base.slice(start, end)


def _jsonable(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating | float):
        value = float(obj)
        return None if math.isnan(value) or math.isinf(value) else value
    if isinstance(obj, np.bool_):
        return bool(obj)
    return obj


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_jsonable(data), indent=2, sort_keys=False) + "\n", encoding="utf-8")


def curve_at(curves: RegimeCurves, volumes: tuple[float, ...]) -> dict[str, float]:
    pooled = curves.pooled
    return {f"{int(v)}": float(pooled.value(v)) for v in volumes}


# --------------------------------------------------------------------------------------------------------------
# 1. fits and validation
# --------------------------------------------------------------------------------------------------------------


def bid_curve_arrays(market: MarketData) -> tuple[np.ndarray, np.ndarray, np.ndarray] | None:
    """Merit-order curves (ODS156/157 aggregated by Agent DATA) aligned to ``market.qh_times``; None if missing."""
    try:
        frame = pdata.load_energy_bid_curves(market.start_utc, market.end_utc)
    except (FileNotFoundError, KeyError, ValueError):
        return None
    if frame.empty:
        return None
    grid = np.sort(frame["cum_mw"].unique()).astype(float)

    def pivot(direction: str) -> np.ndarray:
        part = frame[frame["direction"] == direction]
        table = part.pivot_table(index="timestamp_utc", columns="cum_mw", values="price_eur_mwh", aggfunc="first")
        table = table.reindex(index=market.qh_times, columns=grid)
        return table.to_numpy(dtype=float)

    return grid, pivot("up"), pivot("down")


def validation_section(cfg: dict[str, Any], load: Loader) -> dict[str, Any]:
    out: dict[str, Any] = {"current": [], "new2026": [], "half_years": [], "bid_curves": {}}
    hours = (50.0, 100.0, 200.0, 400.0)
    for label, start, end in cfg["half_years"]:
        market = load(start, end)
        fit, _ = fit_current_response(market, use_regimes=False)
        out["half_years"].append(
            {
                "period": label,
                "start": start,
                "end": end,
                "pass_through": fit.pass_through,
                "mip_curve_eur_mwh_at_mw": curve_at(fit.mip, hours),
                "mdp_curve_eur_mwh_at_mw": curve_at(fit.mdp, hours),
                "mip_curve": fit.mip.pooled.to_dict(),
                "mdp_curve": fit.mdp.pooled.to_dict(),
            }
        )
    for label, (ta, tb), (va, vb) in cfg["current_folds"]:
        train, test = load(ta, tb), load(va, vb)
        fit, _ = fit_current_response(train)
        op = CurrentFormulaImpact.from_market(test, fit)
        st = op.state
        mip = validate_curve_changes("mip_vs_up", st.up_mw, st.mip_eur_mwh, test.qh_times, fit.mip, st.regime)
        mdp = validate_curve_changes("mdp_vs_down", st.down_mw, st.mdp_eur_mwh, test.qh_times, fit.mdp, st.regime)
        shift = validate_operator_si_shift("current", op, test.settlement("current"))
        row: dict[str, Any] = {
            "fold": label,
            "train": [ta, tb],
            "test": [va, vb],
            "pass_through": fit.pass_through,
            "curve_mip": mip.to_dict(),
            "curve_mdp": mdp.to_dict(),
            "si_shift": shift,
        }
        bids = bid_curve_arrays(test)
        if bids is not None and np.isfinite(bids[1]).any():
            grid, up, down = bids
            op_b = CurrentFormulaImpact.from_market(test, fit, bid_curves=(grid, up, down))
            row["si_shift_bid_curves"] = validate_operator_si_shift(
                "current_bid_curves", op_b, test.settlement("current")
            )
            flat = CurveDelta(RegimeCurves(pooled=ResponseCurve.flat()))
            bu, bd = BidCurveDelta(grid, up, fallback=flat), BidCurveDelta(grid, down, fallback=flat, increasing=False)
            pairs = consecutive_pairs(test.qh_times)
            for name, delta, vol, price in (
                ("mip", bu, st.up_mw, st.mip_eur_mwh),
                ("mdp", bd, st.down_mw, st.mdp_eur_mwh),
            ):
                v0, v1 = np.nan_to_num(vol[pairs - 1]), np.nan_to_num(vol[pairs])
                pred = delta.delta(np.zeros(len(pairs), dtype=np.int64), pairs, v0, v1)
                act = price[pairs] - price[pairs - 1]
                row[f"bid_curve_{name}"] = error_stats(
                    f"bid_curve_{name}", act, pred, np.zeros_like(act), target_is_change=True
                ).to_dict()
            rows_all = np.arange(test.n_qh)
            lvl = bu.value(rows_all, np.clip(np.nan_to_num(st.up_mw), 0.0, None))
            ok = np.isfinite(lvl) & np.isfinite(st.mip_eur_mwh)
            slope = (bu.value(rows_all, np.nan_to_num(st.up_mw) + 100.0) - lvl) / 100.0
            row["bid_curve_level"] = {
                "mip_corr": float(np.corrcoef(lvl[ok], st.mip_eur_mwh[ok])[0, 1]) if ok.sum() > 2 else None,
                "mip_mae": float(np.mean(np.abs(lvl[ok] - st.mip_eur_mwh[ok]))) if ok.any() else None,
                "median_local_slope_up_eur_mwh_per_mw": float(np.nanmedian(slope)),
                "coverage_share": float(np.mean(np.isfinite(up[:, 0]))),
            }
        out["current"].append(row)
        log(f"validation current {label}: SI-shift MAE {shift['all']['vs_persistence']['mae']:.2f}")
    for label, (ta, tb), (va, vb) in cfg["new_folds"]:
        train, test = load(ta, tb, new_formula=True), load(va, vb, new_formula=True)
        fit_n = fit_new_formula_response(train)
        op_n = NewFormula2026Impact.from_market(test, fit_n)
        st_n = op_n.state
        au = validate_curve_changes(
            "afrr_up", st_n.afrr_up_mw, st_n.afrr_price_up_eur_mwh, test.qh_times, fit_n.afrr_up, st_n.regime
        )
        ad = validate_curve_changes(
            "afrr_down", st_n.afrr_down_mw, st_n.afrr_price_down_eur_mwh, test.qh_times, fit_n.afrr_down, st_n.regime
        )
        shift_n = validate_operator_si_shift("new2026", op_n, test.settlement("new2026"))
        out["new2026"].append(
            {
                "fold": label,
                "train": [ta, tb],
                "test": [va, vb],
                "pass_through": fit_n.pass_through,
                "curve_afrr_up": au.to_dict(),
                "curve_afrr_down": ad.to_dict(),
                "si_shift": shift_n,
                "afrr_up_curve": fit_n.afrr_up.pooled.to_dict(),
                "afrr_down_curve": fit_n.afrr_down.pooled.to_dict(),
            }
        )
        log(f"validation new2026 {label}: SI-shift MAE {shift_n['all']['vs_persistence']['mae']:.2f}")
    a0, a1 = cfg["alpha_period"]
    market = load(a0, a1)
    st = current_state_from_market(market)
    alpha_fit = fit_alpha_model(st.si_mw, st.si_prev_mw, st.alpha_eur_mwh, mip=st.mip_eur_mwh, mdp=st.mdp_eur_mwh)
    months = pd.DatetimeIndex(market.qh_times).tz_convert("Europe/Brussels").strftime("%Y-%m")
    from euroflex_bess_lab.realtime.impact.alpha import TARIFF_ALPHA, AlphaModel

    tariff_only = AlphaModel(use_price_factor=False).predict(st.si_mw, st.si_prev_mw)
    exact_rule = TARIFF_ALPHA.predict(st.si_mw, st.si_prev_mw, mip=st.mip_eur_mwh, mdp=st.mdp_eur_mwh)
    active = np.abs(st.si_mw) > 150.0
    frame = pd.DataFrame(
        {
            "month": months[active],
            "tariff_only": (np.abs(tariff_only - st.alpha_eur_mwh) < 0.05)[active],
            "exact_rule": (np.abs(exact_rule - st.alpha_eur_mwh) < 0.011)[active],
        }
    )
    by_month = frame.groupby("month")[["tariff_only", "exact_rule"]].mean()
    # Elia's current rule end to end (neutral band, structural MIP/MDP, exact alpha) on the published price
    from euroflex_bess_lab.realtime.impact.response import assemble_current_price

    published = market.settlement("current")
    prev = np.where(np.isnan(st.si_prev_mw), st.si_mw, st.si_prev_mw)
    rule = assemble_current_price(
        st.si_mw, prev, st.mip_eur_mwh, st.mdp_eur_mwh, st.floor_eur_mwh, st.cap_eur_mwh, TARIFF_ALPHA
    )
    ok = np.isfinite(rule) & np.isfinite(published)
    out["alpha"] = {
        **alpha_fit.to_dict(),
        "tariff_only_exact_share_by_month": by_month["tariff_only"].to_dict(),
        "exact_rule_share_by_month": by_month["exact_rule"].to_dict(),
        "price_rule_match_share": float(np.mean(np.isclose(rule[ok], published[ok], atol=0.011))),
        "price_rule_compared": int(ok.sum()),
        "neutral_band_share": float(np.mean(np.abs(st.si_mw[np.isfinite(st.si_mw)]) < 25.0)),
        "period": [a0, a1],
    }
    return out


def branch_gap_section(market: MarketData) -> dict[str, Any]:
    qh = market.qh
    cur = (qh["mip_eur_mwh"] + qh["alpha_eur_mwh"]) - (qh["mdp_eur_mwh"] - qh["alpha_eur_mwh"])
    new = qh["nf_main_up_eur_mwh"] - qh["nf_main_down_eur_mwh"]
    qs = [0.05, 0.25, 0.5, 0.75, 0.95]
    return {
        "current_gap_quantiles": dict(zip([str(q) for q in qs], np.nanquantile(cur, qs).tolist(), strict=True)),
        "current_gap_nonpositive_share": float(np.nanmean(cur <= 0.0)),
        "new_gap_quantiles": dict(zip([str(q) for q in qs], np.nanquantile(new, qs).tolist(), strict=True)),
        "new_gap_nonpositive_share": float(np.nanmean(new <= 0.0)),
        "signal_alignment": signal_alignment(market),
    }


def signal_alignment(market: MarketData) -> dict[str, Any]:
    """How well the final price of each mechanism tells a band trader the direction of the system imbalance.

    Bands as in the price-taking policy (0.25 / 0.75 quantiles of the last 14 days of published prices). Reported:
    share of 'discharge' quarter-hours (price >= upper band) in which the system was short, and of 'charge'
    quarter-hours (price <= lower band) in which it was long -- i.e. how often following the price helps the system.
    """
    from euroflex_bess_lab.realtime.impact.policies import threshold_bands

    out: dict[str, Any] = {}
    for mech, si_name in (("current", "si_mw"), ("new2026", "nf_si_mw")):
        if mech not in market.settlement_prices or si_name not in market.qh:
            continue
        price = market.settlement(mech)
        lower, upper = threshold_bands(
            price,
            market.n_qh,
            PUBLICATION,
            lookback_qh=96 * 14,
            upper_quantile=0.75,
            lower_quantile=0.25,
            min_spread_eur_mwh=20.0,
            min_history_qh=96,
            default_upper_eur_mwh=150.0,
            default_lower_eur_mwh=0.0,
        )
        si = market.qh[si_name]
        dis, chg = price >= upper, price <= lower
        out[mech] = {
            "discharge_signal_share": float(np.mean(dis)),
            "charge_signal_share": float(np.mean(chg)),
            "short_given_discharge_signal": float(np.mean(si[dis] < 0.0)) if dis.any() else None,
            "long_given_charge_signal": float(np.mean(si[chg] > 0.0)) if chg.any() else None,
        }
    return out


# --------------------------------------------------------------------------------------------------------------
# 2. capacity-value curve
# --------------------------------------------------------------------------------------------------------------


_MODELS: dict[tuple[str, str, str], tuple[Any, Any, VolumeFromSI | None, np.ndarray]] = {}


def window_models(market: MarketData, mechanism: str) -> tuple[Any, Any, VolumeFromSI | None, np.ndarray]:
    """Fit on the window itself (in-sample 'truth' for the simulation) and build the operator (cached)."""
    key = (str(market.start_utc), str(market.end_utc), mechanism)
    if key not in _MODELS:
        _MODELS[key] = _window_models(market, mechanism)
    return _MODELS[key]


def _window_models(market: MarketData, mechanism: str) -> tuple[Any, Any, VolumeFromSI | None, np.ndarray]:
    fit: Any
    vmap: VolumeFromSI | None
    if mechanism == "current":
        fit, _ = fit_current_response(market)
        st = current_state_from_market(market)
        vmap = VolumeFromSI.fit(st.si_mw, st.up_mw, st.down_mw)
        si = st.si_mw
    else:
        fit = fit_new_formula_response(market)
        vmap = None
        si = new_state_from_market(market).si_mw
    return fit, operator_for(mechanism, market, fit), vmap, si


channel_rows: list[dict[str, Any]] = []


def capacity_section(cfg: dict[str, Any], load: Loader) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    curves: list[pd.DataFrame] = []
    ledgers: list[pd.DataFrame] = []
    sens_rows: list[dict[str, Any]] = []
    channel_rows.clear()
    fits: dict[str, Any] = {}
    for window, (start, end, mechanisms) in cfg["windows"].items():
        market = load(start, end, new_formula="new2026" in mechanisms)
        for mechanism in mechanisms:
            fit, op, vmap, si = window_models(market, mechanism)
            fits[f"{window}:{mechanism}"] = fit.to_dict()

            def make_aware(q: float, fit: Any = fit, vmap: VolumeFromSI | None = vmap, m: str = mechanism) -> Any:
                return ImpactAwareThresholdStrategy(m, fit, q, volume_map=vmap)

            def make_taker(q: float, m: str = mechanism) -> Any:
                del q  # a price taker ignores the fleet size
                return price_taking_threshold(m)

            policies = [
                PolicySpec("price_taking_threshold", make_taker, False),
                PolicySpec("impact_aware_threshold", make_aware, True),
            ]
            t0 = time.time()
            frame, led = capacity_value_curve(
                market,
                mechanism,
                op,
                policies,
                si_mw=si,
                battery=BATTERY,
                window=window,
                fleet_sizes=cfg["fleet_sizes"],
                publication=PUBLICATION,
                keep_ledgers_for=LEDGER_SIZES_MW,
                progress=log,
            )
            log(f"curve {window} {mechanism} done in {time.time() - t0:.0f}s")
            curves.append(frame)
            ledgers.extend(led)
            # sensitivities for the price taker (re-settle one trace) and a misspecified impact-aware policy
            base = simulate(market, price_taking_threshold(mechanism), BATTERY, mechanism=mechanism)
            assert base.minute is not None
            power = base.minute["power_mw"].to_numpy(dtype=float)
            soc = base.minute["soc_mwh"].to_numpy(dtype=float)
            energy = base.qh["energy_mwh"].to_numpy(dtype=float)
            years = market.days / 365.25
            for q_mw in SENSITIVITY_SIZES_MW:
                q_path = energy / 0.25 * (q_mw / BATTERY.power_mw)
                variants: list[tuple[str, dict[str, np.ndarray]]] = []
                if mechanism == "current":
                    variants.append(("default", current_channels(op, q_path)))
                else:
                    variants.append(("pro_rata", new_channels(op, q_path)))
                    variants.append(("afrr_first", new_channels(op, q_path, displacement="afrr_first")))
                for variant, ch in variants:
                    channel_rows.append(
                        {
                            "window": window,
                            "mechanism": mechanism,
                            "policy": "price_taking_threshold",
                            "displacement": variant,
                            "fleet_mw": q_mw,
                            **channel_value(ch, energy, unit_power_mw=BATTERY.power_mw, years=years),
                        }
                    )
            envs: dict[str, Any] = {"fitted_curves": op}
            if mechanism == "current":
                flat_fit = replace(
                    fit,
                    mip=RegimeCurves(pooled=ResponseCurve.flat(increasing=True)),
                    mdp=RegimeCurves(pooled=ResponseCurve.flat(increasing=False)),
                )
                envs["branch_alpha_only"] = CurrentFormulaImpact.from_market(market, flat_fit)
                bids = bid_curve_arrays(market)
                if bids is not None and np.isfinite(bids[1]).mean() > 0.5:
                    envs["local_bid_curves"] = CurrentFormulaImpact.from_market(market, fit, bid_curves=bids)
                envs["pass_through_1"] = CurrentFormulaImpact.from_market(market, replace(fit, pass_through=1.0))
            else:
                flat_n = replace(
                    fit,
                    afrr_up=RegimeCurves(pooled=ResponseCurve.flat(increasing=True)),
                    afrr_down=RegimeCurves(pooled=ResponseCurve.flat(increasing=False)),
                    mfrr_up=RegimeCurves(pooled=ResponseCurve.flat(increasing=True)),
                    mfrr_down=RegimeCurves(pooled=ResponseCurve.flat(increasing=False)),
                )
                envs["branch_only"] = NewFormula2026Impact.from_market(market, flat_n)
            for env_name, env in envs.items():
                for q in SENSITIVITY_SIZES_MW:
                    qh = resettle(market, BATTERY, power, soc, mechanism=mechanism, impact=env, fleet_mw=q)
                    point = curve_point(
                        qh, BATTERY, n_minutes=market.n_minutes, si_mw=si, fleet_mw=q, timezone=market.timezone
                    )
                    sens_rows.append(
                        {
                            "window": window,
                            "mechanism": mechanism,
                            "policy": "price_taking_threshold",
                            "environment": env_name,
                            "belief": "-",
                            "fleet_mw": q,
                            **point,
                        }
                    )
            q_sens = 400.0 if 400.0 in cfg["fleet_sizes"] else cfg["fleet_sizes"][-1]
            beliefs: list[tuple[str, float, Any, VolumeFromSI | None]] = [
                ("believes x0.5", 0.5, fit, vmap),
                ("believes x2", 2.0, fit, vmap),
            ]
            prior = cfg.get("prior_fits", {}).get(f"{window}:{mechanism}")
            if prior is not None:
                p_market = load(prior[0], prior[1], new_formula=mechanism == "new2026")
                p_fit, _, p_vmap, _ = window_models(p_market, mechanism)
                beliefs.append((f"fit {prior[2]}", 1.0, p_fit, p_vmap))
            for belief, scale, b_fit, b_vmap in beliefs:
                q = q_sens
                strat = ImpactAwareThresholdStrategy(mechanism, b_fit, q, volume_map=b_vmap, impact_scale=scale)
                res = simulate(
                    market,
                    strat,
                    BATTERY,
                    mechanism=mechanism,
                    publication=PUBLICATION,
                    impact=op,
                    fleet_mw=q,
                    record_minutes=False,
                )
                point = curve_point(
                    res.qh, BATTERY, n_minutes=market.n_minutes, si_mw=si, fleet_mw=q, timezone=market.timezone
                )
                sens_rows.append(
                    {
                        "window": window,
                        "mechanism": mechanism,
                        "policy": "impact_aware_threshold",
                        "environment": "fitted_curves",
                        "belief": belief,
                        "fleet_mw": q,
                        **point,
                    }
                )
    curve = pd.concat(curves, ignore_index=True)
    ledger = pd.concat(ledgers, ignore_index=True) if ledgers else pd.DataFrame()
    return curve, ledger, pd.DataFrame(sens_rows), fits


def strat_ledger_rows(cfg: dict[str, Any], load: Loader, ledgers_dir: Path) -> list[dict[str, Any]]:
    """Capacity-value rows for other agents' published price-taking QH ledgers (``realtime_*.parquet``).

    Their decisions do not depend on the fleet size, so the published quarter-hour energies are re-settled at the
    counterfactual prices of a fleet of Q MW. Only (strategy, mechanism) series that cover at least 90 % of one of
    the evaluation windows are used.
    """
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    need = {"timestamp_utc", "energy_mwh", "discharged_mwh", "charged_mwh", "strategy", "mechanism"}
    for path in sorted(ledgers_dir.glob("realtime_*.parquet")):
        try:
            frame = pd.read_parquet(path)
        except (OSError, ValueError):
            continue
        if not need <= set(frame.columns):
            continue
        battery = BATTERY
        sidecar = path.with_suffix(".json")
        if sidecar.exists():
            try:
                meta = json.loads(sidecar.read_text(encoding="utf-8"))
            except ValueError:
                meta = {}
            if meta.get("price_taker") is False:
                continue  # decisions depend on the fleet: cannot be re-settled
            spec = meta.get("battery", {})
            if isinstance(spec, dict) and {"power_mw", "energy_mwh"} <= set(spec):
                try:
                    battery = BatteryParams(**{k: v for k, v in spec.items() if k in asdict(BATTERY)})
                except (TypeError, ValueError):
                    battery = BATTERY
        duration = battery.energy_mwh / battery.power_mw
        suffix = "" if math.isclose(duration, 2.0) else f"_{duration:g}h"
        frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True)
        for (strategy, mechanism), part in frame.groupby(["strategy", "mechanism"]):
            if mechanism not in ("current", "new2026") or str(strategy).startswith("oracle"):
                continue
            if "fleet_mw" in part and part["fleet_mw"].nunique() > 1:
                part = part[np.isclose(part["fleet_mw"], part["fleet_mw"].min())]
            for window, (start, end, mechs) in cfg["windows"].items():
                if mechanism not in mechs:
                    continue
                inside = part[
                    (part["timestamp_utc"] >= pd.Timestamp(start, tz="UTC"))
                    & (part["timestamp_utc"] < pd.Timestamp(end, tz="UTC"))
                ]
                inside = inside.drop_duplicates("timestamp_utc").sort_values("timestamp_utc")
                market = load(start, end, new_formula="new2026" in mechs)
                key = (f"strat:{strategy}{suffix}", window, str(mechanism))
                if len(inside) < 0.9 * market.n_qh or key in seen:
                    continue  # not this window, or already covered by an earlier ledger (sorted file order)
                seen.add(key)
                fit, op, _, si = window_models(market, mechanism)
                pos = market.qh_times.get_indexer(pd.DatetimeIndex(inside["timestamp_utc"]))
                for q in cfg["fleet_sizes"]:
                    qh = resettle_ledger(inside, market, battery, mechanism=mechanism, impact=op, fleet_mw=q)
                    point = curve_point(
                        qh.reset_index(drop=True),
                        battery,
                        n_minutes=15 * len(qh),
                        si_mw=si[pos],
                        fleet_mw=q,
                        timezone=market.timezone,
                    )
                    rows.append(
                        {
                            "mechanism": mechanism,
                            "policy": f"strat:{strategy}{suffix}",
                            "fleet_mw": float(q),
                            **point,
                            "window": window,
                            "start_utc": str(market.start_utc),
                            "end_utc": str(market.end_utc),
                            "days": market.days,
                            "source_ledger": path.name,
                        }
                    )
                log(f"STRAT ledger {path.name}: {strategy} ({mechanism}, {window}) re-settled")
    return rows


# --------------------------------------------------------------------------------------------------------------
# 3. multi-fleet equilibrium versus herding
# --------------------------------------------------------------------------------------------------------------


def equilibrium_section(cfg: dict[str, Any], load: Loader) -> pd.DataFrame:
    start, end, mechanisms = cfg["windows"]["2026H1"]
    market = load(start, end, new_formula=True)
    fit_c, op_c, vmap, _ = window_models(market, "current")
    rows: list[dict[str, Any]] = []
    for mechanism in mechanisms:
        if mechanism == "current":
            fit, settle = fit_c, op_c
        else:
            fit, settle, _, _ = window_models(market, mechanism)
        running = RunningResponse(mechanism, market, fit, volume_map=vmap)
        for policy in FLEET_POLICIES:
            scenarios = [make_fleets(q, N_FLEETS, heterogeneous=(policy == "herding")) for q in cfg["eq_capacities"]]
            t0 = time.time()
            results = simulate_fleet_game(
                market,
                mechanism,
                policy=policy,
                scenarios=scenarios,
                settle=settle,
                running=running,
                cost_operator=op_c,
                battery=BATTERY,
                publication=PUBLICATION,
            )
            log(f"equilibrium {mechanism} {policy}: {time.time() - t0:.0f}s")
            for res in results:
                summary = dict(res.summary)
                fleet_values = summary.pop("fleet_value_eur_per_mw_year")
                summary["fleet_value_min_eur_per_mw_year"] = float(np.min(fleet_values))
                summary["fleet_value_max_eur_per_mw_year"] = float(np.max(fleet_values))
                summary["window"] = "2026H1"
                summary["start_utc"] = str(market.start_utc)
                summary["end_utc"] = str(market.end_utc)
                rows.append(summary)
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------------------------------------------


LEDGER_COLUMNS = (
    "timestamp_utc",
    "energy_mwh",
    "discharged_mwh",
    "base_price_eur_mwh",
    "price_eur_mwh",
    "pnl_eur",
    "strategy",
    "mechanism",
    "fleet_mw",
    "window",
)


def write_qh_ledgers(ledger: pd.DataFrame, ledgers_dir: Path, windows: dict[str, Any]) -> None:
    """One QH-ledger file per evaluation window (each <= 5 MB): ``impact_qh_ledgers_<window>.parquet`` + json."""
    for old in ledgers_dir.glob("impact_qh_ledgers*"):
        old.unlink()
    for window, part in ledger.groupby("window", sort=True):
        small = part[list(LEDGER_COLUMNS)].copy()
        for col in small.columns:
            if small[col].dtype == np.float64:
                small[col] = small[col].astype(np.float32)
        for col in ("strategy", "mechanism", "window"):
            small[col] = small[col].astype("category")
        name = f"impact_qh_ledgers_{window}"
        small.to_parquet(ledgers_dir / f"{name}.parquet", index=False, compression="zstd")
        write_json(
            ledgers_dir / f"{name}.json",
            {
                "table": name,
                "owner": "impact",
                "description": "Quarter-hour ledgers of the capacity-value-curve runs for selected fleet sizes. "
                "energy_mwh / discharged_mwh / pnl_eur are per unit battery (1 MW / 2 MWh): multiply by fleet_mw for "
                "the fleet; price_eur_mwh is the counterfactual price of the fleet's deviation, base_price_eur_mwh "
                "the published price; degradation = 8 EUR/MWh discharged (pnl = energy * price - degradation).",
                "window": window,
                "period": windows.get(str(window)),
                "fleet_sizes_mw": list(LEDGER_SIZES_MW),
                "strategies": sorted(small["strategy"].astype(str).unique()),
                "mechanisms": sorted(small["mechanism"].astype(str).unique()),
                "battery": asdict(BATTERY),
                "publication": asdict(PUBLICATION),
            },
        )


def save_curve(curve: pd.DataFrame, ledgers_dir: Path) -> pd.DataFrame:
    curve = curve.sort_values(["window", "mechanism", "policy", "fleet_mw"]).reset_index(drop=True)
    ordered = [*CURVE_COLUMNS, *[c for c in curve.columns if c not in CURVE_COLUMNS]]
    curve = curve[ordered]
    curve.to_parquet(ledgers_dir / "impact_capacity_value_curve.parquet", index=False)
    return curve


def curve_settings(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "fleet_sizes_mw": list(cfg["fleet_sizes"]),
        "windows": {k: list(v[:2]) + [list(v[2])] for k, v in cfg["windows"].items()},
        "price_taking_policy": "ThresholdStrategy(mode='price', signal=running QH price of the mechanism, bands = "
        "0.25 / 0.75 quantiles of the last 14 days of published prices, min spread 20 EUR/MWh)",
        "impact_aware_policy": "ImpactAwareThresholdStrategy (same signal and bands; maximises q (P(q) - band))",
        "strat_policies": "strat:<name> rows: Agent STRAT's published price-taking QH ledgers (reports/ledgers/"
        "realtime_*.parquet) re-settled at the counterfactual price of a fleet of Q MW (_1h = 1 MW / 1 MWh battery)",
        "impact_operator": "fitted on the evaluation window itself. current: Elia's exact rule (neutral band "
        "|SI| < 25 MW, MIP / MDP bounded by FLOOR / CAP, alpha = tariff x price factor at the counterfactual SI of "
        "this and the previous quarter-hour), MIP / MDP first-difference volume curves, activation pass-through; "
        "new2026: NewWeightedMechanism2026 with aFRR / mFRR price curves and published weights (pro-rata)",
    }


def write_curve_sidecar(curve: pd.DataFrame, ledgers_dir: Path, cfg: dict[str, Any]) -> None:
    write_json(
        ledgers_dir / "impact_capacity_value_curve.json",
        curve_sidecar(curve, battery=BATTERY, publication=PUBLICATION, settings=curve_settings(cfg)),
    )


def update_strat(cfg: dict[str, Any], out: Path) -> None:
    """Refresh only the STRAT-ledger rows of a saved capacity-value curve (other agents' ledgers changed)."""
    load = Loader(cfg["base_current"], cfg["base_new"])
    ledgers_dir = out / "ledgers"
    curve = pd.read_parquet(ledgers_dir / "impact_capacity_value_curve.parquet")
    curve = curve[~curve["policy"].astype(str).str.startswith("strat:")]
    strat = strat_ledger_rows(cfg, load, REPO / "reports" / "ledgers")
    if strat:
        curve = pd.concat([curve, pd.DataFrame(strat)], ignore_index=True)
    curve = save_curve(curve, ledgers_dir)
    write_curve_sidecar(curve, ledgers_dir, cfg)
    log(f"curve updated with {len(strat)} STRAT rows")


def run(cfg: dict[str, Any], out: Path, *, skip_equilibrium: bool = False) -> dict[str, Any]:
    load = Loader(cfg["base_current"], cfg["base_new"])
    ledgers_dir = out / "ledgers"
    ledgers_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    validation = validation_section(cfg, load)
    start, end, _ = cfg["windows"]["2026H1"]
    validation["branch_gaps"] = branch_gap_section(load(start, end, new_formula=True))
    write_json(ledgers_dir / "impact_validation.json", validation)
    log(f"validation written ({time.time() - t0:.0f}s)")
    curve, ledger, sens, fits = capacity_section(cfg, load)
    strat = strat_ledger_rows(cfg, load, REPO / "reports" / "ledgers")
    if strat:
        curve = pd.concat([curve, pd.DataFrame(strat)], ignore_index=True)
    curve = save_curve(curve, ledgers_dir)
    settings = curve_settings(cfg)
    write_curve_sidecar(curve, ledgers_dir, cfg)
    if len(ledger):
        write_qh_ledgers(ledger, ledgers_dir, settings["windows"])
    sens.to_parquet(ledgers_dir / "impact_capacity_value_sensitivity.parquet", index=False)
    pd.DataFrame(channel_rows).to_parquet(ledgers_dir / "impact_channels.parquet", index=False)
    write_json(ledgers_dir / "impact_response_fits.json", fits)
    log(f"capacity curves written ({time.time() - t0:.0f}s)")
    if not skip_equilibrium:
        eq = equilibrium_section(cfg, load)
        eq.to_parquet(ledgers_dir / "impact_equilibrium.parquet", index=False)
        write_json(
            ledgers_dir / "impact_equilibrium.json",
            {
                "table": "impact_equilibrium",
                "owner": "impact",
                "window": list(cfg["windows"]["2026H1"][:2]),
                "n_fleets": N_FLEETS,
                "total_capacities_mw": list(cfg["eq_capacities"]),
                "policies": {
                    "herding": "heterogeneous price-taking threshold fleets (lags 1/1/2/3 min, bands 0.75/0.25, "
                    "0.70/0.30, 0.80/0.20, 0.75/0.25), closed-loop running signal",
                    "herding_homogeneous": "identical price takers (lag 1, bands 0.75/0.25), closed loop",
                    "naive": "each fleet internalises its own impact only (ignores the others)",
                    "nash": "symmetric Nash equilibrium of identical fleets (damped best responses)",
                    "central": "one optimiser for the whole implicit capacity (joint profit)",
                },
                "battery_per_mw": asdict(BATTERY),
                "publication": asdict(PUBLICATION),
            },
        )
        log(f"equilibrium written ({time.time() - t0:.0f}s)")
    return {"elapsed_s": time.time() - t0}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--quick", action="store_true", help="two-week smoke run (needs --out)")
    parser.add_argument("--out", type=Path, default=None, help="output root (default: the repo's reports/)")
    parser.add_argument("--skip-equilibrium", action="store_true")
    parser.add_argument("--report-only", action="store_true", help="re-render figures and report from saved results")
    parser.add_argument(
        "--update-strat", action="store_true", help="re-settle other agents' realtime_* ledgers into the saved curve"
    )
    args = parser.parse_args(argv)
    out = args.out or (REPO / "reports")
    if args.quick and args.out is None:
        parser.error("--quick writes partial results; pass --out to a scratch directory")
    cfg = QUICK if args.quick else FULL
    if args.update_strat:
        update_strat(cfg, out)
    elif not args.report_only:
        run(cfg, out, skip_equilibrium=args.skip_equilibrium)
    from impact_report import render  # noqa: E402  (sibling module in scripts/research)

    render(out, quick=args.quick)
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    raise SystemExit(main())
