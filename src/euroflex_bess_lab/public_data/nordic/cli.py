"""``nordic_data_app``: fetch and list the Nordic tables (to be registered by the orchestrator, e.g. as
``data_app.add_typer(nordic_data_app, name="nordic")`` -> ``euroflex data nordic fetch``)."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd
import typer

from ..http import HttpClient
from ..paths import data_root, raw_root
from ..writers import SIDECAR_SUFFIX
from . import energinet
from .build import TABLE_NAMES, NordicContext, as_of_snapshot, build_tables

nordic_data_app = typer.Typer(help="Nordic public data: Energinet (Energi Data Service) and Svenska kraftnät (SVK).")


@nordic_data_app.command("fetch")
def fetch(
    tables: str = typer.Option(
        ",".join(TABLE_NAMES), "--tables", help="Comma-separated subset of " + ", ".join(TABLE_NAMES)
    ),
    start: str = typer.Option("2023-12-01", "--start", help="First UTC instant to keep (default 2023-12-01)."),
    as_of: str | None = typer.Option(
        None, "--as-of", help="Snapshot date YYYY-MM-DD: data end at Copenhagen midnight of that day."
    ),
    refresh: bool = typer.Option(False, "--refresh", help="Re-download cached raw chunks."),
    offline: bool = typer.Option(False, "--offline", help="Use the raw cache only (no network)."),
    data_dir: Path | None = typer.Option(None, "--data-dir", help="Output root (default: data/public)."),
    raw_dir: Path | None = typer.Option(None, "--raw-dir", help="Raw cache root (default: data/raw)."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Download (or reuse cached) raw data and (re)build data/public/nordic/*.parquet with sidecars."""

    logging.basicConfig(level=logging.INFO if verbose else logging.WARNING, format="%(levelname)s %(message)s")
    root = (data_dir or data_root()).resolve()
    raw = (raw_dir or (root.parent / "raw" if data_dir else raw_root())).resolve()
    ctx = NordicContext(
        root=root,
        raw_dir=raw,
        client=HttpClient(config=energinet.ENERGINET_HTTP),
        refresh=refresh,
        offline=offline,
        start_utc=pd.Timestamp(start, tz="UTC")
        if pd.Timestamp(start).tzinfo is None
        else pd.Timestamp(start).tz_convert("UTC"),
        end_utc=as_of_snapshot(as_of),
    )
    names = [t.strip() for t in tables.split(",") if t.strip()]
    try:
        written = build_tables(ctx, names)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    for table in written:
        typer.echo(f"{table.relpath}\t{table.rows} rows\t{table.size_bytes / 1e6:.2f} MB")
    total = sum(t.size_bytes for t in written)
    typer.echo(f"{len(written)} files, {total / 1e6:.1f} MB (regenerate MANIFEST.json with `euroflex data manifest`)")


@nordic_data_app.command("list")
def list_tables(
    data_dir: Path | None = typer.Option(None, "--data-dir", help="Data root (default: data/public)."),
) -> None:
    """List the Nordic files described by sidecars."""

    root = (data_dir or data_root()).resolve() / "nordic"
    sidecars = sorted(root.glob(f"*{SIDECAR_SUFFIX}")) if root.exists() else []
    if not sidecars:
        typer.echo(f"No Nordic sidecars under {root}; run `python scripts/research/nordic_fetch.py`.")
        return
    for path in sidecars:
        meta = json.loads(path.read_text(encoding="utf-8"))
        typer.echo(
            f"{meta['path']}\t{meta.get('rows', '?')} rows\t{int(meta.get('size_bytes', 0)) / 1e6:.2f} MB\t"
            f"{meta.get('coverage_start_utc')} -> {meta.get('coverage_end_utc')}\t{meta.get('licence')}"
        )
