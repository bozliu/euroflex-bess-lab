"""``euroflex data gb ...`` commands (to be registered by the orchestrator as
``data_app.add_typer(gb_app, name="gb")``)."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import typer

from ..paths import data_root, raw_root
from .build import GBBuildContext, build_gb
from .tables import FETCH_TARGETS, GB_TABLES
from .timeutils import latest_uk_midnight_utc, to_utc

gb_app = typer.Typer(help="Great Britain public data: Elexon BMRS, NESO EAC auction results, ECB FX (data/public/gb).")


def make_context(
    data_dir: Path | None = None,
    raw_dir: Path | None = None,
    *,
    refresh: bool = False,
    offline: bool = False,
    as_of: str | None = None,
    start: str | None = None,
) -> GBBuildContext:
    root = (data_dir or data_root()).resolve()
    raw = (raw_dir or (root.parent / "raw" if data_dir else raw_root())).resolve()
    now = latest_uk_midnight_utc(to_utc(pd.Timestamp(as_of).tz_localize("Europe/London")) if as_of else None)
    ctx = GBBuildContext(root=root, raw_dir=raw, refresh=refresh, offline=offline, now=now)
    if start:
        ctx.start = to_utc(start)
    return ctx


@gb_app.command("fetch")
def fetch(
    target: str = typer.Argument("all", help="One of: " + ", ".join(FETCH_TARGETS) + ", all."),
    refresh: bool = typer.Option(False, "--refresh", help="Re-download cached raw chunks."),
    offline: bool = typer.Option(False, "--offline", help="Use the raw cache only (no network)."),
    data_dir: Path | None = typer.Option(None, "--data-dir", help="Output root (default: data/public)."),
    raw_dir: Path | None = typer.Option(None, "--raw-dir", help="Raw cache root (default: data/raw)."),
    as_of: str | None = typer.Option(
        None, "--as-of", help="Snapshot date YYYY-MM-DD: data end at Europe/London midnight starting that day."
    ),
    start: str | None = typer.Option(None, "--start", help="Coverage start (default 2023-12-01)."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Download (cached under data/raw/gb) and rebuild the normalized GB tables under data/public/gb."""

    logging.basicConfig(level=logging.INFO if verbose else logging.WARNING, format="%(levelname)s %(message)s")
    ctx = make_context(data_dir, raw_dir, refresh=refresh, offline=offline, as_of=as_of, start=start)
    for table in build_gb([target], ctx):
        typer.echo(f"{table.relpath}\t{table.rows} rows\t{table.size_bytes / 1e6:.2f} MB")


@gb_app.command("list")
def list_tables(data_dir: Path | None = typer.Option(None, "--data-dir")) -> None:
    """List the GB tables and whether they exist locally."""

    root = (data_dir or data_root()).resolve()
    for key, spec in GB_TABLES.items():
        paths = sorted(root.glob(spec.pattern.replace("{year}", "[0-9][0-9][0-9][0-9]")))
        size = sum(p.stat().st_size for p in paths)
        status = f"{len(paths)} file(s), {size / 1e6:.2f} MB" if paths else "missing"
        typer.echo(f"{key}\t{spec.pattern}\t{status}")
