"""GB simulator on the real-data fixture days (2025-06-03 .. 2025-06-13): accounting, physical and contract
rules, and no look-ahead for every causal strategy (perturbing data published after a cut never changes the
decisions taken before it)."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.markets.gb import GBMarket, GBSimulator, run_strategy
from euroflex_bess_lab.markets.gb.engine import GBResult
from euroflex_bess_lab.markets.gb.rules import HIGH_PRODUCTS, LOW_PRODUCTS, RESERVED_CAPACITY_SHARE, RESPONSE_PRODUCTS
from euroflex_bess_lab.markets.gb.strategies import DEFAULT_BATTERY, MAIN_CONFIGS
from euroflex_bess_lab.public_data.gb import (
    load_gb_fx,
    load_gb_market_index,
    load_gb_response_auctions,
    load_gb_response_energy,
    load_gb_system_prices,
)
from euroflex_bess_lab.public_data.gb.timeutils import UK_TZ

REPO_ROOT = Path(__file__).resolve().parents[1]
PUBLIC = REPO_ROOT / "tests" / "fixtures" / "gb" / "public"
START, END = "2025-06-03", "2025-06-13"


def _frames() -> dict[str, pd.DataFrame]:
    return {
        "system_prices": load_gb_system_prices(START, END, root=PUBLIC),
        "market_index": load_gb_market_index(START, END, root=PUBLIC),
        "response_auctions": load_gb_response_auctions("2025-06-01", "2025-06-15", root=PUBLIC),
        "response_energy": load_gb_response_energy(START, END, root=PUBLIC),
        "fx": load_gb_fx("2025-05-20", END, root=PUBLIC),
    }


def _market(frames: dict[str, pd.DataFrame]) -> GBMarket:
    return GBMarket.from_frames(**frames, start=START, end=END)


@pytest.fixture(scope="module")
def market() -> GBMarket:
    return _market(_frames())


@pytest.fixture(scope="module")
def results(market: GBMarket) -> dict[str, GBResult]:
    names = [*MAIN_CONFIGS, "wholesale_oracle", "imbalance_oracle", "fr_oracle", "combined_oracle"]
    return {name: run_strategy(name, market) for name in names}


def test_market_bundle_aligns_tables(market: GBMarket) -> None:
    assert market.n == 10 * 48
    assert np.isfinite(market.sip).all() and np.isfinite(market.mid_settle).all()
    assert set(market.efa_block.tolist()) == {1, 2, 3, 4, 5, 6}
    gates = market.eac_gate_sps()
    assert len(gates) == 10
    assert all(market.ts[g].tz_convert(UK_TZ).hour == 14 for g in gates)
    assert np.isfinite(market.eac_price["DCL"]).mean() > 0.95
    assert 0.8 < market.gbp_per_eur.min() <= market.gbp_per_eur.max() < 0.9


def test_ledger_accounting_identities(results: dict[str, GBResult]) -> None:
    for name, result in results.items():
        led = result.ledger
        total = led["revenue_wholesale_gbp"] + led["revenue_imbalance_gbp"] + led["revenue_response_gbp"]
        assert np.allclose(led["pnl_gbp"], total - led["degradation_gbp"]), name
        assert np.allclose(led["pnl_eur"], led["pnl_gbp"] / led["gbp_per_eur"]), name
        per_product = sum(led[f"revenue_{p.lower()}_gbp"] for p in RESPONSE_PRODUCTS)
        assert np.allclose(per_product, led["revenue_response_gbp"]), name
        assert np.allclose(led["imbalance_mw"], led["baseline_mw"] - led["wholesale_mw"]), name
        expected_dis = 0.5 * led["baseline_mw"].clip(lower=0) + led["response_export_mwh"]
        assert np.allclose(led["discharged_mwh"], expected_dis), name
        assert np.allclose(led["degradation_gbp"], led["discharged_mwh"] * 8.0 * led["gbp_per_eur"]), name


def test_physical_limits_hold(results: dict[str, GBResult]) -> None:
    for name, result in results.items():
        led = result.ledger
        low = sum(led[f"mw_{p.lower()}"] for p in LOW_PRODUCTS)
        high = sum(led[f"mw_{p.lower()}"] for p in HIGH_PRODUCTS)
        assert (led["baseline_mw"] + low <= 1.0 + 1e-6).all(), name
        assert (-led["baseline_mw"] + high <= 1.0 + 1e-6).all(), name
        assert led["soc_end_mwh"].between(-1e-9, 2.0 + 1e-9).all(), name
        start = led["soc_start_mwh"].to_numpy()
        assert np.allclose(start[1:], led["soc_end_mwh"].to_numpy()[:-1]), name


def test_workflows_only_use_their_markets(results: dict[str, GBResult]) -> None:
    assert results["wholesale_forecast"].ledger["revenue_imbalance_gbp"].abs().max() < 1e-6
    assert results["niv_chasing"].ledger["revenue_wholesale_gbp"].abs().max() < 1e-9
    assert results["wholesale_forecast"].ledger["revenue_response_gbp"].abs().max() == 0.0
    fr = results["fr_stack"].ledger
    assert fr["revenue_wholesale_gbp"].abs().max() == 0.0 and fr["revenue_response_gbp"].abs().sum() > 0


def test_response_contracts_follow_the_auction_rules(market: GBMarket) -> None:
    sim = GBSimulator(market, DEFAULT_BATTERY, MAIN_CONFIGS["fr_stack"])
    result = sim.run()
    led = result.ledger
    first_gate = int(market.eac_gate_sps()[0])
    first_auctioned = int(market.efa_day[first_gate + 18])
    before = market.efa_day < first_auctioned
    assert all((led.loc[before, f"mw_{p.lower()}"] == 0).all() for p in RESPONSE_PRODUCTS)
    for p in RESPONSE_PRODUCTS:
        offered, accepted = sim.offered[p], sim.accepted[p]
        clearing, offer = market.eac_price[p], sim.offer_price[p]
        won = accepted > 0
        assert (clearing[won] >= offer[won] - 1e-9).all()
        lost = (offered > 0) & ~won
        assert (clearing[lost] < offer[lost]).all() | (~np.isfinite(clearing[lost])).all()
    low = sum(led[f"mw_{p.lower()}"] * (1 + RESERVED_CAPACITY_SHARE[p]) for p in LOW_PRODUCTS)
    high = sum(led[f"mw_{p.lower()}"] * (1 + RESERVED_CAPACITY_SHARE[p]) for p in HIGH_PRODUCTS)
    assert (low <= 1.0 + 1e-6).all() and (high <= 1.0 + 1e-6).all()
    assert led.loc[low > 0, "available_low"].mean() > 0.9


def test_oracles_bound_their_causal_strategies(results: dict[str, GBResult]) -> None:
    def net(name: str) -> float:
        return float(results[name].ledger["pnl_gbp"].sum())

    assert net("wholesale_oracle") >= net("wholesale_forecast")
    assert net("imbalance_oracle") >= net("niv_chasing")
    assert net("combined_oracle") >= net("combined_stack")


# ---------------------------------------------------------------------------------------------- no look-ahead


def _perturbed(frames: dict[str, pd.DataFrame], cut: pd.Timestamp, seed: int = 7) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    out = {k: v.copy() for k, v in frames.items()}
    sp = out["system_prices"]
    after = sp["timestamp_utc"] >= cut
    sp.loc[after, "system_price_gbp_mwh"] = rng.normal(150, 120, after.sum()).astype("float32")
    sp.loc[after, "niv_mwh"] = rng.normal(0, 500, after.sum()).astype("float32")
    mid = out["market_index"]
    after = mid["timestamp_utc"] >= cut
    mid.loc[after, "mid_price_gbp_mwh"] = rng.normal(150, 80, after.sum()).astype("float32")
    energy = out["response_energy"]
    after = energy["timestamp_utc"] >= cut
    for p in RESPONSE_PRODUCTS:
        column = f"{p.lower()}_mwh_per_mw"
        energy.loc[after, column] = rng.uniform(0, 0.3, after.sum()).astype("float32")
    auctions = out["response_auctions"]
    # results of EFA day E are published at 15:00 UK on E-1
    published = pd.to_datetime(auctions["efa_date"]).dt.tz_localize(UK_TZ) - pd.Timedelta(hours=9)
    later = published.dt.tz_convert("UTC") >= cut
    auctions.loc[later, "clearing_price_gbp_per_mw_h"] = rng.normal(5, 15, later.sum()).astype("float32")
    return out


@pytest.mark.parametrize("name", list(MAIN_CONFIGS))
def test_causal_strategies_do_not_look_ahead(name: str) -> None:
    frames = _frames()
    cut = pd.Timestamp("2025-06-08T09:30Z")
    base_market = _market(frames)
    test_market = _market(_perturbed(frames, cut))
    config = replace(MAIN_CONFIGS[name])
    base = GBSimulator(base_market, DEFAULT_BATTERY, config)
    base.run()
    test = GBSimulator(test_market, DEFAULT_BATTERY, config)
    test.run()
    c = base_market.index_of(cut)
    if config.wholesale == "mpc":
        upto = c + config.wholesale_lead_sps + 1  # decided at or before the cut
        assert np.allclose(base.wholesale[:upto], test.wholesale[:upto])
        assert not np.allclose(base.wholesale, test.wholesale)  # the perturbation does matter later
    if config.imbalance != "none":
        upto = c + config.imbalance_lead_sps + 1
        assert np.allclose(base.baseline[:upto], test.baseline[:upto])
    if config.response != "none":
        gates = [int(g) for g in base_market.eac_gate_sps() if g <= c]
        for g in gates:
            sps = base_market.day_sps(int(base_market.efa_day[g + 18]))
            for p in RESPONSE_PRODUCTS:
                assert np.allclose(base.offered[p][sps], test.offered[p][sps]), (name, p)
                assert np.allclose(base.offer_price[p][sps], test.offer_price[p][sps], equal_nan=True), (name, p)


def test_oracle_uses_the_future_by_design() -> None:
    frames = _frames()
    cut = pd.Timestamp("2025-06-08T09:30Z")
    base = run_strategy("wholesale_oracle", _market(frames)).ledger["wholesale_mw"].to_numpy()
    test = run_strategy("wholesale_oracle", _market(_perturbed(frames, cut))).ledger["wholesale_mw"].to_numpy()
    c = _market(frames).index_of(cut)
    assert not np.allclose(base[:c], test[:c])
