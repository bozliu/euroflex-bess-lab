from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams, check_no_lookahead, simulate
from euroflex_bess_lab.realtime.forecasting.synthetic import synthetic_market

BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)


def test_rl_module_imports_torch_lazily() -> None:
    code = (
        "import sys\n"
        "import euroflex_bess_lab.realtime.strategies.rl as rl\n"
        "import euroflex_bess_lab.realtime.forecasting\n"
        "assert 'torch' not in sys.modules, 'torch imported at module import'\n"
        "assert 'sklearn' not in sys.modules, 'sklearn imported at module import'\n"
        "print(rl.RL_FEATURES[0])\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "minute_in_qh"


def test_tiny_dqn_training_on_cpu_and_audit(tmp_path: Path) -> None:
    pytest.importorskip("torch")
    from euroflex_bess_lab.realtime.strategies.rl import DQNConfig, DQNPolicy, RLStrategy, train_dqn

    market = synthetic_market(days=9, seed=3)
    start = market.start_utc
    config = DQNConfig(
        n_envs=32,
        batch_size=128,
        buffer_size=20_000,
        learning_starts=1_000,
        max_updates=600,
        validate_every=300,
        eps_decay_updates=500,
        max_minutes=1.0,
        device="cpu",
        hidden=(32, 32),
    )
    policy = train_dqn(
        market,
        BATTERY,
        train=(start, start + pd.Timedelta(days=6)),
        validation=(start + pd.Timedelta(days=6), start + pd.Timedelta(days=7)),
        config=config,
    )
    assert policy.log["device"] == "cpu"
    assert policy.log["updates"] == 600
    assert len(policy.log["validation_history"]) >= 2
    test = market.slice(start + pd.Timedelta(days=7), market.end_utc)
    result = simulate(test, RLStrategy(policy), BATTERY, record_minutes=True)
    assert result.minute is not None
    powers = result.minute["power_mw"].to_numpy()
    assert np.all(np.abs(powers) <= BATTERY.max_power_mw + 1e-9)
    on_grid = np.isin(np.round(powers, 9), [-1.0, -0.5, 0.0, 0.5, 1.0])
    assert on_grid.mean() > 0.5  # off-grid values only where the simulator clipped at an SOC limit
    saved = policy.save(tmp_path / "policy.npz")
    loaded = DQNPolicy.load(saved)
    again = simulate(test, RLStrategy(loaded), BATTERY, record_minutes=False)
    assert again.pnl_eur == pytest.approx(result.pnl_eur)
    audit = check_no_lookahead(lambda: RLStrategy(loaded), test, BATTERY, cut_minute=900)
    assert audit.passed, audit
