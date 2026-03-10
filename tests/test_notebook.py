from __future__ import annotations

from pathlib import Path

import nbformat
from nbclient import NotebookClient


def _execute_notebook(source: Path, target: Path) -> None:
    notebook = nbformat.read(source, as_version=4)
    client = NotebookClient(notebook, timeout=600, kernel_name="python3")
    client.execute()
    nbformat.write(notebook, target)
    assert target.exists()


def test_cross_market_notebook_executes_top_to_bottom(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[1] / "notebooks" / "belgium_vs_netherlands_walk_forward.ipynb"
    _execute_notebook(source, tmp_path / "cross_market.ipynb")


def test_reserve_notebook_executes_top_to_bottom(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[1] / "notebooks" / "reserve_aware_value_stacking.ipynb"
    _execute_notebook(source, tmp_path / "reserve.ipynb")


def test_portfolio_notebook_executes_top_to_bottom(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[1] / "notebooks" / "portfolio_shared_constraints.ipynb"
    _execute_notebook(source, tmp_path / "portfolio.ipynb")


def test_revision_notebook_executes_top_to_bottom(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[1] / "notebooks" / "schedule_revision_reconciliation.ipynb"
    _execute_notebook(source, tmp_path / "revision.ipynb")
