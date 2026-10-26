"""Paper-local numbers for the PES GM 2027 paper (outputs under artifacts/paper_pesgm2027).

Every number in ``main.tex`` that is not copied from a committed report is computed here from committed repository
inputs (``data/public`` via ``euroflex_bess_lab.public_data`` and ``reports/ledgers``). Output:
``artifacts/paper_pesgm2027/outputs/paper_numbers.json``. ``numbers.md`` maps each number in the paper to its source.

Run from the repository root:

    python scripts/research/paper_pesgm2027_numbers.py

Sections
--------
A. Current rule (ODS134 + ODS166/127 components, 2024-06-01..2026-08-01): exact operator checks.
B. New 2026 rule (Elia simulation ODS212, Jan-Jul 2026): exact operator checks (weights, incentive, magnitude).
C. Same-state price statistics (cross-check of reports/mechanism_natural_experiment.md).
D. Paired moving-block bootstrap of value differences from the period-B quarter-hour ledgers
   (reports/ledgers/realtime_*_B.parquet, Jan-Jul 2026, 1 MW / 2 MWh, lag 1 min).
E. Where the value is earned: share of P&L by |SI| regime (oracle and SCALES-L ledgers).
F. Robustness: the 6 April 2026 floor-price day, bootstrap block length, risk-filter effect.
G. Ablation (Agent STRAT): direct price model on operator-rebuilt targets vs naive transfer, SCALES-L and the
   in-mechanism model, read from reports/ledgers/realtime_direct_rebuilt_bootstrap.json.
H. Look-ahead audit counts from reports/ledgers/realtime_lookahead_audit.json and reports/ledgers/oos_summary.json.
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from euroflex_bess_lab import public_data

REPO = Path(__file__).resolve().parents[2]
HERE = REPO / "artifacts" / "paper_pesgm2027"
LEDGERS = REPO / "reports" / "ledgers"
OUT = HERE / "outputs"

CUR_START, CUR_END = "2024-06-01T00:00:00Z", "2026-08-01T00:00:00Z"
NE_START, NE_END = "2026-01-01T00:00:00Z", "2026-08-01T00:00:00Z"
TOL = 0.011  # EUR/MWh, the repository's reproduction tolerance (validate_mechanism)
W_TOL = 1e-3  # weight tolerance
DAYS_PER_YEAR = 365.25
BOOT_REPS = 2000
BOOT_BLOCK = 7
BOOT_SEED = 20270725


def share(mask: np.ndarray, valid: np.ndarray | None = None) -> float:
    m = np.asarray(mask, dtype=bool)
    if valid is not None:
        m = m[np.asarray(valid, dtype=bool)]
    return float(m.mean()) if m.size else float("nan")


# ----------------------------------------------------------------------------------------------------------------
# A. current rule


def current_rule_checks() -> dict[str, Any]:
    q = public_data.load_belgium_qh(CUR_START, CUR_END)

    def f(c: str) -> np.ndarray:
        return q[c].to_numpy(dtype=float)

    si, price = f("si_mw"), f("imbalance_price_eur_mwh")
    mip, mdp, alpha = f("mip_eur_mwh"), f("mdp_eur_mwh"), f("alpha_eur_mwh")
    floor, cap = f("floor_price_eur_mwh"), f("cap_price_eur_mwh")
    up = np.vstack([f("afrr_price_up_eur_mwh"), f("mfrr_sa_price_up_eur_mwh"), f("mfrr_da_price_up_eur_mwh"), floor])
    dn = np.vstack(
        [f("afrr_price_down_eur_mwh"), f("mfrr_sa_price_down_eur_mwh"), f("mfrr_da_price_down_eur_mwh"), cap]
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        mip_s, mdp_s = np.nanmax(up, axis=0), np.nanmin(dn, axis=0)
    short = si < 0.0
    dead = np.abs(si) < 25.0
    act = np.abs(si) > 150.0
    prev = np.concatenate([[np.nan], si[:-1]])
    prev = np.where(np.isnan(prev), si, prev)
    x = 0.5 * (np.abs(si) + np.abs(prev))
    tariff = 200.0 / (1.0 + np.exp((450.0 - x) / 65.0))
    c_pub = np.where(short, np.clip((400.0 - mip) / 200.0, 0, 1), np.clip((mdp + 200.0) / 200.0, 0, 1))
    c_str = np.where(short, np.clip((400.0 - mip_s) / 200.0, 0, 1), np.clip((mdp_s + 200.0) / 200.0, 0, 1))
    alpha_rule_pub = np.where(act, tariff * c_pub, 0.0)
    alpha_rule_str = np.where(act, tariff * c_str, 0.0)
    x_absmean = np.abs(0.5 * (si + prev))
    alpha_absmean = np.where(act, 200.0 / (1.0 + np.exp((450.0 - x_absmean) / 65.0)) * c_pub, 0.0)
    alpha_no_c = np.where(act, tariff, 0.0)
    ok_alpha = np.isfinite(alpha) & np.isfinite(si)
    pub_price = np.where(short, mip + alpha, mdp - alpha)
    ok_p = np.isfinite(pub_price) & np.isfinite(price)
    mid = 0.5 * (floor + cap)
    full = np.where(dead, mid, np.where(short, mip_s + alpha_rule_str, mdp_s - alpha_rule_str))
    ok_f = np.isfinite(full) & np.isfinite(price)
    full_match = np.isclose(full, price, atol=TOL)
    years = pd.DatetimeIndex(q["timestamp_utc"]).year
    by_year = {}
    for y in sorted(set(years)):
        s = (years == y) & ok_f
        by_year[str(y)] = {"qh": int(s.sum()), "match": share(full_match[s])}
    ok_ms = short & ~dead & np.isfinite(mip) & np.isfinite(mip_s)
    ok_md = ~short & ~dead & np.isfinite(mdp) & np.isfinite(mdp_s)
    ok_dead = dead & np.isfinite(mid) & np.isfinite(price)
    alpha_prime = f("alpha_prime_eur_mwh")
    return {
        "window_utc": [CUR_START, CUR_END],
        "qh": int(len(q)),
        "price_from_published_mip_mdp_alpha": {
            "qh": int(ok_p.sum()),
            "match": share(np.isclose(pub_price, price, atol=TOL)[ok_p]),
        },
        "mip_from_products_and_floor": {
            "qh": int(ok_ms.sum()),
            "match": share(np.isclose(mip_s, mip, atol=TOL)[ok_ms]),
        },
        "mdp_from_products_and_cap": {"qh": int(ok_md.sum()), "match": share(np.isclose(mdp_s, mdp, atol=TOL)[ok_md])},
        "dead_band": {
            "share_of_qh": share(dead[np.isfinite(si)]),
            "qh": int(ok_dead.sum()),
            "price_equals_midpoint": share(np.isclose(price, mid, atol=TOL)[ok_dead]),
        },
        "alpha": {
            "active_share_of_qh": share(act[np.isfinite(si)]),
            "qh_active": int((act & ok_alpha).sum()),
            "rule_match_published_mip_mdp": share(np.isclose(alpha_rule_pub, alpha, atol=TOL)[act & ok_alpha]),
            "rule_match_structural_mip_mdp": share(np.isclose(alpha_rule_str, alpha, atol=TOL)[act & ok_alpha]),
            "qh_inactive": int((~act & ok_alpha).sum()),
            "zero_when_inactive": share(np.isclose(alpha, 0.0, atol=TOL)[~act & ok_alpha]),
            "variant_abs_of_mean_si": share(np.isclose(alpha_absmean, alpha, atol=TOL)[act & ok_alpha]),
            "variant_no_price_factor": share(np.isclose(alpha_no_c, alpha, atol=TOL)[act & ok_alpha]),
        },
        "alpha_prime_all_zero": bool(np.nanmax(np.abs(alpha_prime)) == 0.0),
        "alpha_prime_nonzero_qh": int((np.abs(np.nan_to_num(alpha_prime)) > 0.0).sum()),
        "alpha_prime_missing_qh": int(np.isnan(alpha_prime).sum()),
        "full_rebuild": {
            "qh": int(ok_f.sum()),
            "match": share(full_match[ok_f]),
            "mismatch_qh": int((ok_f & ~full_match).sum()),
            "by_year": by_year,
        },
    }


# ----------------------------------------------------------------------------------------------------------------
# B. new rule


def new_rule_checks() -> dict[str, Any]:
    nf = public_data.load_belgium_new_formula_qh(NE_START, NE_END)

    def z(c: str) -> np.ndarray:
        return np.nan_to_num(nf[c].to_numpy(dtype=float))

    def f(c: str) -> np.ndarray:
        return nf[c].to_numpy(dtype=float)

    n = len(nf)
    si, price = f("si_mw"), f("imbalance_price_eur_mwh")
    short = si < 0.0
    a_up, a_dn = z("afrr_up_mw"), z("afrr_down_mw")
    m_up = z("mfrr_sa_up_mw") + z("mfrr_da_up_mw")
    m_dn = z("mfrr_sa_down_mw") + z("mfrr_da_down_mw")
    has_da = (z("mfrr_da_up_mw") + z("mfrr_da_down_mw")) > 0.0
    a_net, m_net = np.abs(a_up - a_dn), np.abs(m_up - m_dn)
    tot = a_net + m_net
    w_net = np.where(tot > 0.0, a_net / np.where(tot > 0.0, tot, 1.0), np.nan)
    w_pub = f("weight_afrr")
    ok_w = np.isfinite(w_pub) & np.isfinite(w_net)
    w_match = np.abs(w_net - w_pub) <= W_TOL
    # direction-specific alternative (volumes in the SI direction only)
    a_dir, m_dir = np.where(short, a_up, a_dn), np.where(short, m_up, m_dn)
    td = a_dir + m_dir
    w_dir = np.where(td > 0.0, a_dir / np.where(td > 0.0, td, 1.0), np.nan)
    ok_d = ok_w & np.isfinite(w_dir)
    # alternative that adds iGCC to the published aFRR volumes
    g_up, g_dn = z("igcc_up_mw"), z("igcc_down_mw")
    a_net_g = np.abs(a_up + g_up - a_dn - g_dn)
    tot_g = a_net_g + m_net
    w_g = np.where(tot_g > 0.0, a_net_g / np.where(tot_g > 0.0, tot_g, 1.0), np.nan)
    ok_g = ok_w & np.isfinite(w_g)
    # mFRR component
    sa_up, da_up, sa_dn, da_dn = (
        f(c)
        for c in (
            "mfrr_sa_price_up_eur_mwh",
            "mfrr_da_price_up_eur_mwh",
            "mfrr_sa_price_down_eur_mwh",
            "mfrr_da_price_down_eur_mwh",
        )
    )
    mfrr_up_pub, mfrr_dn_pub = f("mfrr_price_up_eur_mwh"), f("mfrr_price_down_eur_mwh")
    mx, mn = np.fmax(sa_up, da_up), np.fmin(sa_dn, da_dn)
    ok_mu, ok_md = np.isfinite(mfrr_up_pub), np.isfinite(mfrr_dn_pub)
    # main components from published weights and product prices
    wa, wm = np.nan_to_num(w_pub), np.nan_to_num(f("weight_mfrr"))
    au, ad = f("afrr_price_up_eur_mwh"), f("afrr_price_down_eur_mwh")
    main_up_pub, main_dn_pub = f("main_up_eur_mwh"), f("main_down_eur_mwh")
    main_up = wa * np.nan_to_num(au) + wm * np.nan_to_num(mfrr_up_pub)
    main_dn = wa * np.nan_to_num(ad) + wm * np.nan_to_num(mfrr_dn_pub)
    defined = (wa + wm) > 0.0
    voaa_up, voaa_dn = f("voaa_up_eur_mwh"), f("voaa_down_eur_mwh")
    voaa_mid = 0.5 * (voaa_up + voaa_dn)
    undefined = ~defined
    # incentive
    inc = f("incentive_eur_mwh")
    frce, l2 = f("frce_mw"), f("frce_l2_target_mw")
    has_inc = np.isfinite(inc)
    trig = (np.abs(frce) > l2) & (np.abs(si) > 250.0)
    maxmin = np.where(short, np.fmax(main_up_pub, inc), np.fmin(main_dn_pub, inc))
    additive = np.where(short, main_up_pub + inc, main_dn_pub + inc)
    ratio = np.abs(frce) / l2
    neg = frce < 0.0
    mag = np.minimum(1000.0, np.where(neg, 700.0 + 1128.0 * (ratio - 1.0), 500.0 + 801.0 * (ratio - 1.0)))
    inc_rule = -np.sign(frce) * mag
    capped = np.abs(inc) >= 1000.0 - 1e-6
    fits = {}
    for side, msk in (("frce_negative", has_inc & ~capped & neg), ("frce_positive", has_inc & ~capped & ~neg)):
        xx, yy = ratio[msk] - 1.0, np.abs(inc[msk])
        if msk.sum() >= 2:
            design = np.vstack([np.ones_like(xx), xx]).T
            coef, *_ = np.linalg.lstsq(design, yy, rcond=None)
            fits[side] = {
                "qh": int(msk.sum()),
                "c0": float(coef[0]),
                "c1": float(coef[1]),
                "max_abs_residual": float(np.max(np.abs(design @ coef - yy))),
            }
    # end-to-end rebuild: volumes, product prices, FRCE, L2 target only (no published weights/main/incentive)
    wa_r = np.where(tot > 0.0, w_net, 0.0)
    wm_r = np.where(tot > 0.0, 1.0 - wa_r, 0.0)
    up_r = np.where(tot > 0.0, wa_r * np.nan_to_num(au) + wm_r * np.nan_to_num(mx), voaa_mid)
    dn_r = np.where(tot > 0.0, wa_r * np.nan_to_num(ad) + wm_r * np.nan_to_num(mn), voaa_mid)
    i_r = np.where(trig, inc_rule, np.nan)
    p_r = np.where(short, up_r, dn_r)
    p_r = np.where(np.isfinite(i_r), np.where(short, np.fmax(up_r, i_r), np.fmin(dn_r, i_r)), p_r)
    full_match = np.isclose(p_r, price, atol=TOL)
    err = np.abs(p_r - price)
    # price from published components (cross-check of the mechanism report)
    p_pub = np.where(short, main_up_pub, main_dn_pub)
    p_pub = np.where(has_inc, maxmin, p_pub)
    return {
        "window_utc": [NE_START, NE_END],
        "qh": int(n),
        "price_from_published_main_and_incentive": share(np.isclose(p_pub, price, atol=TOL)),
        "main_components_from_published_weights": {
            "qh": int(defined.sum()),
            "up_match": share(np.isclose(main_up, main_up_pub, atol=TOL)[defined]),
            "down_match": share(np.isclose(main_dn, main_dn_pub, atol=TOL)[defined]),
        },
        "mfrr_component": {
            "up_is_max_sa_da": {"qh": int(ok_mu.sum()), "match": share(np.isclose(mx, mfrr_up_pub, atol=TOL)[ok_mu])},
            "down_is_min_sa_da": {"qh": int(ok_md.sum()), "match": share(np.isclose(mn, mfrr_dn_pub, atol=TOL)[ok_md])},
        },
        "weights_net_volume": {
            "qh": int(ok_w.sum()),
            "match": share(w_match[ok_w]),
            "qh_without_mfrr_da": int((ok_w & ~has_da).sum()),
            "match_without_mfrr_da": share(w_match[ok_w & ~has_da]),
            "qh_with_mfrr_da": int((ok_w & has_da).sum()),
            "mismatch_qh": int((ok_w & ~w_match).sum()),
            "mismatch_qh_with_mfrr_da": int((ok_w & ~w_match & has_da).sum()),
        },
        "weights_direction_specific": {"qh": int(ok_d.sum()), "match": share((np.abs(w_dir - w_pub) <= W_TOL)[ok_d])},
        "weights_net_volume_plus_igcc": {"qh": int(ok_g.sum()), "match": share((np.abs(w_g - w_pub) <= W_TOL)[ok_g])},
        "counter_direction_mfrr_enters_as_zero": {
            "qh_short": int((short & (wm > 0.0) & ~ok_mu).sum()),
            "qh_long": int((~short & (wm > 0.0) & ~ok_md).sum()),
            "note": "w_mfrr > 0 but no mFRR price in the SI direction: the published main component equals w_afrr * aFRR price",
        },
        "no_net_activation": {
            "qh": int(undefined.sum()),
            "main_equals_voaa_midpoint": int(
                (np.isclose(main_up_pub, voaa_mid, atol=TOL) & np.isclose(main_dn_pub, voaa_mid, atol=TOL))[
                    undefined
                ].sum()
            ),
        },
        "incentive": {
            "published_qh": int(has_inc.sum()),
            "published_share": share(has_inc),
            "trigger_rule_qh": int(trig.sum()),
            "trigger_and_published": int((trig & has_inc).sum()),
            "max_min_rule_fits": int(np.isclose(maxmin, price, atol=TOL)[has_inc].sum()),
            "additive_rule_fits": int(np.isclose(additive, price, atol=TOL)[has_inc].sum()),
            "price_equals_incentive": int(np.isclose(price, inc, atol=TOL)[has_inc].sum()),
            "short_qh": int((has_inc & short).sum()),
            "long_qh": int((has_inc & ~short).sum()),
            "magnitude_rule_fits": int(np.isclose(inc_rule, inc, atol=TOL)[has_inc].sum()),
            "magnitude_rule_max_abs_error": float(np.nanmax(np.abs(inc_rule - inc)[has_inc])),
            "capped_qh": int((has_inc & capped).sum()),
            "uncapped_qh": int((has_inc & ~capped).sum()),
            "sign_is_minus_sign_frce": bool(np.all(np.sign(inc[has_inc]) == -np.sign(frce[has_inc]))),
            "fits": fits,
        },
        "full_rebuild": {
            "qh": int(n),
            "match": share(full_match),
            "qh_without_mfrr_da": int((~has_da).sum()),
            "match_without_mfrr_da": share(full_match[~has_da]),
            "qh_with_mfrr_da": int(has_da.sum()),
            "match_with_mfrr_da": share(full_match[has_da]),
            "mae_eur_mwh": float(np.mean(err)),
        },
    }


# ----------------------------------------------------------------------------------------------------------------
# C. same-state statistics (cross-check)


def same_state_stats() -> dict[str, Any]:
    q = public_data.load_belgium_qh(NE_START, NE_END)[["timestamp_utc", "si_mw", "imbalance_price_eur_mwh"]]
    nf = public_data.load_belgium_new_formula_qh(NE_START, NE_END)[
        ["timestamp_utc", "si_mw", "imbalance_price_eur_mwh", "main_up_eur_mwh", "main_down_eur_mwh"]
    ]
    m = q.merge(nf, on="timestamp_utc", suffixes=("_cur", "_new"))
    out: dict[str, Any] = {"qh": int(len(m))}
    for tag in ("cur", "new"):
        p = m[f"imbalance_price_eur_mwh_{tag}"]
        s = m[f"si_mw_{tag}"] < 0.0
        out[tag] = {
            "std": float(p.std()),
            "mean_short": float(p[s].mean()),
            "mean_long": float(p[~s].mean()),
            "short_minus_long": float(p[s].mean() - p[~s].mean()),
            "short_share": float(s.mean()),
        }
    out["std_ratio_new_over_cur"] = out["new"]["std"] / out["cur"]["std"]
    # both prices classified by the same (ODS134) final SI: the same-state comparison
    s = m["si_mw_cur"] < 0.0
    for tag in ("cur", "new"):
        p = m[f"imbalance_price_eur_mwh_{tag}"]
        out[f"{tag}_by_ods134_si"] = {
            "mean_short": float(p[s].mean()),
            "mean_long": float(p[~s].mean()),
            "short_minus_long": float(p[s].mean() - p[~s].mean()),
        }
    return out


# ----------------------------------------------------------------------------------------------------------------
# D. paired block bootstrap on period-B ledgers


def ledger(name: str) -> pd.DataFrame:
    df = pd.read_parquet(LEDGERS / f"{name}.parquet")
    df["day"] = pd.DatetimeIndex(df["timestamp_utc"]).tz_convert("UTC").floor("D")
    return df


def daily(name: str) -> pd.Series:
    df = ledger(name)
    return df.groupby("day")["pnl_eur"].sum()


def sidecar(name: str) -> dict[str, Any]:
    return json.loads((LEDGERS / f"{name}.json").read_text())["summary"]


def block_indices(n: int, rng: np.random.Generator) -> np.ndarray:
    starts = rng.integers(0, n - BOOT_BLOCK + 1, size=int(np.ceil(n / BOOT_BLOCK)))
    idx = (starts[:, None] + np.arange(BOOT_BLOCK)[None, :]).ravel()[:n]
    return idx


def bootstrap(pairs: dict[str, tuple[str, str]], ratios: dict[str, tuple[str, str]]) -> dict[str, Any]:
    names = sorted({x for p in list(pairs.values()) + list(ratios.values()) for x in p})
    series = {k: daily(k) for k in names}
    days = sorted(set.intersection(*(set(s.index) for s in series.values())))
    mat = {k: s.reindex(days).to_numpy(dtype=float) for k, s in series.items()}
    n = len(days)
    rng = np.random.default_rng(BOOT_SEED)
    draws = [block_indices(n, rng) for _ in range(BOOT_REPS)]
    out: dict[str, Any] = {"days": n, "replicates": BOOT_REPS, "block_days": BOOT_BLOCK, "seed": BOOT_SEED}
    for label, (a, b) in pairs.items():
        d = mat[a] - mat[b]
        point = float(d.mean() * DAYS_PER_YEAR)
        boots = np.array([d[i].mean() * DAYS_PER_YEAR for i in draws])
        out[label] = {
            "a": a,
            "b": b,
            "diff_eur_per_mw_year": point,
            "ci95": [float(np.quantile(boots, 0.025)), float(np.quantile(boots, 0.975))],
            "a_eur_per_mw_year": float(mat[a].mean() * DAYS_PER_YEAR),
            "b_eur_per_mw_year": float(mat[b].mean() * DAYS_PER_YEAR),
            "share_of_boot_above_zero": float((boots > 0).mean()),
        }
    for label, (num, den) in ratios.items():
        point = float(mat[num].sum() / mat[den].sum())
        boots = np.array([mat[num][i].sum() / mat[den][i].sum() for i in draws])
        out[label] = {
            "num": num,
            "den": den,
            "ratio": point,
            "ci95": [float(np.quantile(boots, 0.025)), float(np.quantile(boots, 0.975))],
        }
    # capture difference (same draws)
    return out


def capture_difference() -> dict[str, Any]:
    names = [
        "realtime_prism_risk_current_B",
        "realtime_oracle_qh_current_B",
        "realtime_prism_risk_new2026_B",
        "realtime_oracle_qh_new2026_B",
        "realtime_prism_live_risk_new2026_B",
        "realtime_forecast_mpc_risk_current_B",
    ]
    series = {k: daily(k) for k in names}
    days = sorted(set.intersection(*(set(s.index) for s in series.values())))
    mat = {k: s.reindex(days).to_numpy(dtype=float) for k, s in series.items()}
    rng = np.random.default_rng(BOOT_SEED + 1)
    n = len(days)
    out = {}
    combos = {
        "scales_l_risk_new_minus_current": (
            "realtime_prism_risk_new2026_B",
            "realtime_oracle_qh_new2026_B",
            "realtime_prism_risk_current_B",
            "realtime_oracle_qh_current_B",
        ),
        "scales_l_live_risk_new_minus_mpc_risk_current": (
            "realtime_prism_live_risk_new2026_B",
            "realtime_oracle_qh_new2026_B",
            "realtime_forecast_mpc_risk_current_B",
            "realtime_oracle_qh_current_B",
        ),
    }
    for label, (a, oa, b, ob) in combos.items():
        point = mat[a].sum() / mat[oa].sum() - mat[b].sum() / mat[ob].sum()
        boots = []
        for _ in range(BOOT_REPS):
            i = block_indices(n, rng)
            boots.append(mat[a][i].sum() / mat[oa][i].sum() - mat[b][i].sum() / mat[ob][i].sum())
        boots_arr = np.array(boots)
        out[label] = {
            "diff": float(point),
            "ci95": [float(np.quantile(boots_arr, 0.025)), float(np.quantile(boots_arr, 0.975))],
        }
    return out


# ----------------------------------------------------------------------------------------------------------------
# E. value by |SI| regime


def value_by_regime() -> dict[str, Any]:
    q = public_data.load_belgium_qh(NE_START, NE_END)[["timestamp_utc", "si_mw"]]
    nf = public_data.load_belgium_new_formula_qh(NE_START, NE_END)[["timestamp_utc", "si_mw"]]
    out: dict[str, Any] = {}
    for name, si_frame in (
        ("realtime_oracle_qh_current_B", q),
        ("realtime_oracle_qh_new2026_B", nf),
        ("realtime_prism_risk_current_B", q),
        ("realtime_prism_risk_new2026_B", nf),
        ("realtime_prism_live_risk_new2026_B", nf),
        ("realtime_forecast_mpc_risk_current_B", q),
        ("realtime_forecast_mpc_risk_new2026_B", nf),
        ("realtime_threshold_si_current_B", q),
        ("realtime_threshold_si_new2026_B", nf),
    ):
        df = ledger(name).merge(si_frame, on="timestamp_utc", how="left")
        a = df["si_mw"].abs()
        regime = np.select([a < 25.0, a <= 150.0], ["lt25", "25to150"], default="gt150")
        total = df["pnl_eur"].sum()
        g = df.groupby(regime)["pnl_eur"].sum()
        qh_share = pd.Series(regime).value_counts(normalize=True)
        out[name] = {
            "pnl_share": {k: float(v / total) for k, v in g.items()},
            "qh_share": {k: float(v) for k, v in qh_share.items()},
            "eur_per_mw_year": float(total / (df["day"].nunique() / DAYS_PER_YEAR)),
        }
    return out


# ----------------------------------------------------------------------------------------------------------------
# F. robustness: the 6 April 2026 floor-price day, block length, risk filter


def boot_mean(d: np.ndarray, block: int, seed: int = BOOT_SEED) -> list[float]:
    rng = np.random.default_rng(seed)
    n = len(d)
    vals = []
    for _ in range(BOOT_REPS):
        starts = rng.integers(0, n - block + 1, size=int(np.ceil(n / block)))
        idx = (starts[:, None] + np.arange(block)[None, :]).ravel()[:n]
        vals.append(d[idx].mean() * DAYS_PER_YEAR)
    return [float(np.quantile(vals, 0.025)), float(np.quantile(vals, 0.975))]


def robustness() -> dict[str, Any]:
    floor_day = pd.Timestamp("2026-04-06", tz="UTC")
    oc, on = daily("realtime_oracle_qh_current_B"), daily("realtime_oracle_qh_new2026_B")
    days = oc.index.intersection(on.index)
    oc, on = oc.reindex(days), on.reindex(days)
    keep = days != floor_day
    d_all = (on - oc).to_numpy(dtype=float)
    d_ex = (on - oc)[keep].to_numpy(dtype=float)
    out: dict[str, Any] = {
        "floor_day_utc": str(floor_day.date()),
        "oracle_floor_day_pnl_eur": {"current": float(oc[floor_day]), "new2026": float(on[floor_day])},
        "oracle_eur_per_mw_year_excl_floor_day": {
            "current": float(oc[keep].mean() * DAYS_PER_YEAR),
            "new2026": float(on[keep].mean() * DAYS_PER_YEAR),
        },
        "oracle_diff_excl_floor_day": {"diff": float(d_ex.mean() * DAYS_PER_YEAR), "ci95": boot_mean(d_ex, BOOT_BLOCK)},
        "oracle_diff_all": {"diff": float(d_all.mean() * DAYS_PER_YEAR), "ci95": boot_mean(d_all, BOOT_BLOCK)},
    }
    out["oracle_ratio_excl_floor_day"] = (
        out["oracle_eur_per_mw_year_excl_floor_day"]["new2026"]
        / out["oracle_eur_per_mw_year_excl_floor_day"]["current"]
    )
    a, b = daily("realtime_prism_risk_new2026_B"), daily("realtime_forecast_mpc_risk_new2026_B")
    dd = (a - b).reindex(days).to_numpy(dtype=float)
    out["block_length_sensitivity_scales_l_minus_mpc_new"] = {str(blk): boot_mean(dd, blk) for blk in (1, 3, 7, 14, 28)}
    for tag, risk, neutral in (
        ("new2026", "realtime_prism_risk_new2026_B", "realtime_prism_new2026_B"),
        ("current", "realtime_prism_risk_current_B", "realtime_prism_current_B"),
    ):
        x = (daily(risk) - daily(neutral)).reindex(days).to_numpy(dtype=float)
        out[f"risk_filter_effect_scales_l_{tag}"] = {
            "diff": float(x.mean() * DAYS_PER_YEAR),
            "ci95": boot_mean(x, BOOT_BLOCK),
        }
    return out


# ----------------------------------------------------------------------------------------------------------------
# G. ablation: direct price model on operator-rebuilt targets (reports/ledgers/realtime_direct_rebuilt_bootstrap.json)

ABLATION_FAMILIES = {
    "e": ("direct_rebuilt", "direct_rebuilt_risk", "direct_rebuilt_live", "direct_rebuilt_live_risk"),
    "f": (
        "direct_rebuilt_curfeat",
        "direct_rebuilt_curfeat_risk",
        "direct_rebuilt_curfeat_live",
        "direct_rebuilt_curfeat_live_risk",
    ),
}


def ablation() -> dict[str, Any]:
    rows = json.loads((LEDGERS / "realtime_direct_rebuilt_bootstrap.json").read_text())
    out: dict[str, Any] = {"n_comparisons": len(rows), "comparisons": []}
    for r in rows:
        if r["reference"].startswith("ne_naive"):
            kind = "vs_naive"
        elif r["reference"].startswith("ne_prism_zero"):
            kind = "vs_scales_l"
        elif r["reference"].startswith("ne_price_new2026"):
            kind = "vs_in_mechanism"
        else:
            kind = "other"
        out["comparisons"].append(
            {
                "window": r["window"],
                "arm": r["arm"],
                "reference": r["reference"],
                "kind": kind,
                "arm_eur_per_mw_year": r["arm_eur_per_mw_year"],
                "reference_eur_per_mw_year": r["reference_eur_per_mw_year"],
                "arm_capture_vs_oracle": r["arm_capture_vs_oracle"],
                "diff_eur_per_mw_year": r["diff_eur_per_mw_year"],
                "ci95": r["diff_ci_eur_per_mw_year"],
                "uplift_pct": r["uplift_pct"],
                "uplift_ci_pct": r["uplift_ci_pct"],
                "days": r["days"],
                "n_boot": r["n_boot"],
                "block_days": r["block_days"],
            }
        )
    for fam, arms in ABLATION_FAMILIES.items():
        sel = [c for c in out["comparisons"] if c["arm"] in arms]
        vs_s = [c for c in sel if c["kind"] == "vs_scales_l"]
        vs_n = [c for c in sel if c["kind"] == "vs_naive"]
        vs_b = [c for c in sel if c["kind"] == "vs_in_mechanism"]
        out[f"family_{fam}"] = {
            "vs_scales_l_n": len(vs_s),
            "vs_scales_l_intervals_including_zero": int(sum(c["ci95"][0] <= 0.0 <= c["ci95"][1] for c in vs_s)),
            "vs_scales_l_max_abs_diff": float(max(abs(c["diff_eur_per_mw_year"]) for c in vs_s)),
            "vs_naive_n": len(vs_n),
            "vs_naive_min_diff": float(min(c["diff_eur_per_mw_year"] for c in vs_n)),
            "vs_naive_max_diff": float(max(c["diff_eur_per_mw_year"] for c in vs_n)),
            "vs_naive_min_lower_bound": float(min(c["ci95"][0] for c in vs_n)),
            "vs_in_mechanism": [{"arm": c["arm"], "diff": c["diff_eur_per_mw_year"], "ci95": c["ci95"]} for c in vs_b],
        }
    # gap closed on Apr-Jul (no risk filter): (arm - naive) / (in-mechanism price model - naive)
    ref = {c["arm"]: c for c in out["comparisons"] if c["window"] == "Apr-Jul 2026" and c["kind"] == "vs_naive"}
    naive = ref["direct_rebuilt"]["reference_eur_per_mw_year"]
    in_mech = next(
        c["reference_eur_per_mw_year"]
        for c in out["comparisons"]
        if c["window"] == "Apr-Jul 2026" and c["arm"] == "direct_rebuilt" and c["kind"] == "vs_in_mechanism"
    )
    out["gap_closed_apr_jul_neutral"] = {
        "naive": naive,
        "in_mechanism": in_mech,
        "direct_rebuilt": (ref["direct_rebuilt"]["arm_eur_per_mw_year"] - naive) / (in_mech - naive),
        "direct_rebuilt_live": (ref["direct_rebuilt_live"]["arm_eur_per_mw_year"] - naive) / (in_mech - naive),
        "scales_l_zero_frozen_73344": (73344.0 - naive) / (in_mech - naive),
        "scales_l_zero_live_frozen_81735": (81735.0 - naive) / (in_mech - naive),
        "note": "SCALES-L values from the reports/realtime_belgium.md Apr-Jul table (arms (c), (c''))",
    }
    # uplift of the frozen learners over the frozen naive model, Jan-Jul, risk filter
    jr = {c["arm"]: c for c in out["comparisons"] if c["window"] == "Jan-Jul 2026" and c["kind"] == "vs_naive"}
    js = {c["arm"]: c for c in out["comparisons"] if c["window"] == "Jan-Jul 2026" and c["kind"] == "vs_scales_l"}
    naive_risk = jr["direct_rebuilt_risk"]["reference_eur_per_mw_year"]
    scales_risk = js["direct_rebuilt_risk"]["reference_eur_per_mw_year"]
    out["jan_jul_risk_uplift_over_naive"] = {
        "direct_rebuilt_risk_pct": jr["direct_rebuilt_risk"]["uplift_pct"],
        "direct_rebuilt_risk_ci_pct": jr["direct_rebuilt_risk"]["uplift_ci_pct"],
        "scales_l_zero_frozen_risk_pct": 100.0 * (scales_risk / naive_risk - 1.0),
        "naive_frozen_risk_eur_per_mw_year": naive_risk,
        "scales_l_zero_frozen_risk_eur_per_mw_year": scales_risk,
        "scales_l_zero_live_frozen_risk_eur_per_mw_year": js["direct_rebuilt_live_risk"]["reference_eur_per_mw_year"],
    }
    return out


# ----------------------------------------------------------------------------------------------------------------
# H. look-ahead audit counts


def audits() -> dict[str, Any]:
    rt = json.loads((LEDGERS / "realtime_lookahead_audit.json").read_text())
    causal = [r for r in rt if "oracle" not in r["strategy"]]
    oracle = [r for r in rt if "oracle" in r["strategy"]]
    oos = json.loads((LEDGERS / "oos_summary.json").read_text())["lookahead_audit"]
    out = {
        "realtime_causal_audits": len(causal),
        "realtime_causal_passed": int(sum(r["passed"] for r in causal)),
        "realtime_causal_configs": len({(r["strategy"], r["mechanism"]) for r in causal}),
        "realtime_white_box": int(sum(r["white_box_passed"] is not None for r in causal)),
        "realtime_white_box_passed": int(sum(bool(r["white_box_passed"]) for r in causal)),
        "realtime_oracle_audits": len(oracle),
        "realtime_oracle_failed": int(sum(not r["passed"] for r in oracle)),
        "oos_tradable_audits": oos["tradable_audits"],
        "oos_tradable_passed": oos["tradable_passed"],
        "oos_white_box": oos["white_box_audits"],
        "oos_white_box_passed": oos["white_box_passed"],
        "oos_oracle_audits": oos["oracle_audits"],
        "oos_oracle_failed": oos["oracle_failed"],
    }
    out["total_causal_audits"] = out["realtime_causal_audits"] + out["oos_tradable_audits"]
    out["total_causal_passed"] = out["realtime_causal_passed"] + out["oos_tradable_passed"]
    out["total_white_box_passed"] = out["realtime_white_box_passed"] + out["oos_white_box_passed"]
    out["total_oracle_failed"] = out["realtime_oracle_failed"] + out["oos_oracle_failed"]
    out["total_oracle_audits"] = out["realtime_oracle_audits"] + out["oos_oracle_audits"]
    return out


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    pairs = {
        "rule_effect_oracle": ("realtime_oracle_qh_new2026_B", "realtime_oracle_qh_current_B"),
        "rule_effect_scales_l_risk": ("realtime_prism_risk_new2026_B", "realtime_prism_risk_current_B"),
        "rule_effect_forecast_mpc_risk": (
            "realtime_forecast_mpc_risk_new2026_B",
            "realtime_forecast_mpc_risk_current_B",
        ),
        "rule_effect_threshold_si": ("realtime_threshold_si_new2026_B", "realtime_threshold_si_current_B"),
        "rule_effect_threshold_price": ("realtime_threshold_price_new2026_B", "realtime_threshold_price_current_B"),
        "rule_effect_rl_dqn": ("realtime_rl_dqn_new2026_B", "realtime_rl_dqn_current_B"),
        "new_scales_l_risk_minus_mpc_risk": ("realtime_prism_risk_new2026_B", "realtime_forecast_mpc_risk_new2026_B"),
        "new_scales_l_live_risk_minus_mpc_risk": (
            "realtime_prism_live_risk_new2026_B",
            "realtime_forecast_mpc_risk_new2026_B",
        ),
        "new_scales_l_live_risk_minus_threshold_price": (
            "realtime_prism_live_risk_new2026_B",
            "realtime_threshold_price_new2026_B",
        ),
        "new_scales_l_risk_minus_threshold_price": (
            "realtime_prism_risk_new2026_B",
            "realtime_threshold_price_new2026_B",
        ),
        "new_scales_l_live_risk_minus_scales_l_risk": (
            "realtime_prism_live_risk_new2026_B",
            "realtime_prism_risk_new2026_B",
        ),
        "cur_scales_l_risk_minus_mpc_risk": ("realtime_prism_risk_current_B", "realtime_forecast_mpc_risk_current_B"),
    }
    ratios = {
        "capture_scales_l_risk_current": ("realtime_prism_risk_current_B", "realtime_oracle_qh_current_B"),
        "capture_scales_l_risk_new": ("realtime_prism_risk_new2026_B", "realtime_oracle_qh_new2026_B"),
        "capture_scales_l_live_risk_new": ("realtime_prism_live_risk_new2026_B", "realtime_oracle_qh_new2026_B"),
        "capture_mpc_risk_current": ("realtime_forecast_mpc_risk_current_B", "realtime_oracle_qh_current_B"),
        "capture_mpc_risk_new": ("realtime_forecast_mpc_risk_new2026_B", "realtime_oracle_qh_new2026_B"),
    }
    sidecars = {k: sidecar(k) for k in sorted({x for p in list(pairs.values()) + list(ratios.values()) for x in p})}
    result = {
        "A_current_rule": current_rule_checks(),
        "B_new_rule": new_rule_checks(),
        "C_same_state": same_state_stats(),
        "D_bootstrap": bootstrap(pairs, ratios),
        "D_capture_difference": capture_difference(),
        "D_sidecar_summaries": sidecars,
        "E_value_by_si_regime": value_by_regime(),
        "F_robustness": robustness(),
        "G_ablation_rebuilt_targets": ablation(),
        "H_audits": audits(),
    }
    path = OUT / "paper_numbers.json"
    path.write_text(json.dumps(result, indent=1, sort_keys=False))
    print(f"wrote {path.relative_to(REPO)}")


if __name__ == "__main__":
    main()
