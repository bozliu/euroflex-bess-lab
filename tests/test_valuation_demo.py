from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from euroflex_bess_lab.valuation import write_ledger
from euroflex_bess_lab.valuation.synthetic import synthetic_ledger_set

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "research" / "valuation_demo.py"


def load_demo():
    spec = importlib.util.spec_from_file_location("valuation_demo_script", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_demo_runs_the_full_chain_on_synthetic_data(tmp_path: Path) -> None:
    demo = load_demo()
    report = tmp_path / "reports" / "valuation_demo.md"
    figures = tmp_path / "reports" / "figures"
    args = ["--report", str(report), "--figures-dir", str(figures), "--paths", "200", "--tenor-years", "3"]
    demo.main([*args, "--synthetic-start", "2025-10-01", "--synthetic-end", "2026-04-01"])
    text = report.read_text(encoding="utf-8")
    assert text.startswith("# Contract valuation demo - SYNTHETIC DATA")
    for heading in ("## Headline numbers", "## 2. Value attribution", "### 4.2 Revenue floor", "## 5. Portfolio"):
        assert heading in text
    assert "](figures/valuation_demo_waterfall.png)" in text
    for name in ("waterfall", "annual_distribution", "toll_sensitivity", "portfolio"):
        assert (figures / f"valuation_demo_{name}.png").exists()
    assert str(tmp_path) not in text and "/Users/" not in text


def test_demo_accepts_real_ledger_paths(tmp_path: Path) -> None:
    demo = load_demo()
    ledgers = synthetic_ledger_set("2025-10-01", "2026-04-01", seed=3)
    paths = {}
    for role, key in (("current", "strategy"), ("new2026", "mechanism"), ("oracle", "oracle"), ("germany", "germany")):
        ledger, metadata = ledgers[key]
        paths[role] = write_ledger(ledger, tmp_path / "runs" / f"{role}.parquet", metadata)
    report = tmp_path / "valuation_custom.md"
    args = [f"--{role}={path}" for role, path in paths.items()]
    args += ["--report", str(report), "--figures-dir", str(tmp_path / "fig"), "--figure-prefix", "valuation_custom"]
    demo.main([*args, "--paths", "200", "--tenor-years", "2", "--title", "Custom valuation"])
    text = report.read_text(encoding="utf-8")
    assert text.startswith("# Custom valuation")
    assert "| current | current.parquet |" in text  # files outside the repo are shown by name only
    assert "Synthetic inputs" in text  # the sidecars flag these ledgers as synthetic
    assert "| impact |" not in text  # roles without a ledger are skipped, never filled with synthetic data
    assert (tmp_path / "fig" / "valuation_custom_portfolio.png").exists()
    assert str(tmp_path) not in text
