"""``intraday_app``: fetch / inspect the local intraday index (register as ``euroflex data intraday ...``)."""

from __future__ import annotations

import logging
from pathlib import Path

import typer

from .build import ID_AEP_DEFAULT_START, build_id_aep
from .loaders import intraday_index_path

intraday_app = typer.Typer(
    help="German intraday price index ID AEP (netztransparenz.de; built locally under data/raw, not redistributed)."
)


@intraday_app.command("fetch")
def fetch(
    start: str = typer.Option(ID_AEP_DEFAULT_START, "--start", help="First local delivery day (YYYY-MM-DD)."),
    end: str | None = typer.Option(None, "--end", help="Exclusive last local delivery day (default: tomorrow)."),
    refresh: bool = typer.Option(False, "--refresh", help="Re-download cached monthly CSVs."),
    offline: bool = typer.Option(False, "--offline", help="Use the raw cache only (no network)."),
    data_dir: Path | None = typer.Option(None, "--data-dir", help="Public data root for the sidecar stub."),
    raw_dir: Path | None = typer.Option(None, "--raw-dir", help="Raw cache root (default: data/raw)."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Download monthly ID AEP CSVs (one request per month, polite pause) and build the local parquet table."""

    logging.basicConfig(level=logging.INFO if verbose else logging.WARNING, format="%(levelname)s %(message)s")
    result = build_id_aep(start=start, end=end, root=data_dir, raw_dir=raw_dir, refresh=refresh, offline=offline)
    validation = result.local.sidecar.get("validation", {})
    typer.echo(
        f"{result.local.relpath}\trows={result.local.rows}\tsize={result.local.size_bytes}\t"
        f"months={result.chunks}\tlast_complete_day={result.last_complete_day}\t"
        f"undefined={validation.get('undefined_values')}\tmissing_qh={validation.get('missing_quarter_hours')}"
    )
    typer.echo(f"stub sidecar: {result.stub_path.name} (redistributed: false)")


@intraday_app.command("path")
def path(source: str = typer.Option("id_aep", "--source")) -> None:
    """Print where the local intraday index file is expected."""

    typer.echo(str(intraday_index_path(source)))
