"""``euroflex data ...`` commands (registered by the orchestrator as ``app.add_typer(data_app, name="data")``)."""

from __future__ import annotations

import logging
from pathlib import Path

import typer

from .build import TARGETS, BuildContext, as_of_snapshot, build_target
from .elia import to_utc
from .http import HttpClient, HttpConfig
from .paths import data_root, raw_root
from .registry import ELIA_DATASETS, TABLES
from .writers import MANIFEST_NAME, build_manifest, collect_sidecars, verify_manifest

data_app = typer.Typer(help="Download, normalize, list and verify the public data tree (data/public).")


def _context(
    data_dir: Path | None, raw_dir: Path | None, refresh: bool, offline: bool, as_of: str | None = None
) -> BuildContext:
    root = (data_dir or data_root()).resolve()
    raw = (raw_dir or (root.parent / "raw" if data_dir else raw_root())).resolve()
    return BuildContext(
        root=root,
        raw_dir=raw,
        client=HttpClient(config=HttpConfig()),
        refresh=refresh,
        offline=offline,
        now=as_of_snapshot(as_of),
    )


@data_app.command("fetch")
def fetch(
    target: str = typer.Argument(
        ...,
        help="Table group: " + ", ".join(TARGETS) + "; 'all' for everything; 'raw' with --dataset to only cache an "
        "upstream Elia dataset.",
    ),
    start: str | None = typer.Option(
        None, "--start", help="Inclusive start (UTC); selects the year partitions to rebuild (always rebuilt whole)."
    ),
    end: str | None = typer.Option(None, "--end", help="Exclusive end (UTC); default: the snapshot boundary."),
    zone: str | None = typer.Option(None, "--zone", help="day-ahead: BE or DE-LU (default both)."),
    product: str | None = typer.Option(
        None, "--product", help="capacity awards: afrr|mfrr; germany-reserve: fcr|afrr."
    ),
    kind: str | None = typer.Option(None, "--kind", help="belgium-si-forecast: current|next (default both)."),
    dataset: str | None = typer.Option(None, "--dataset", help="target 'raw': Elia dataset id, e.g. ods133."),
    refresh: bool = typer.Option(False, "--refresh", help="Re-download cached raw chunks."),
    offline: bool = typer.Option(False, "--offline", help="Use the raw cache only (no network)."),
    data_dir: Path | None = typer.Option(None, "--data-dir", help="Output root (default: data/public)."),
    raw_dir: Path | None = typer.Option(None, "--raw-dir", help="Raw cache root (default: data/raw)."),
    as_of: str | None = typer.Option(
        None, "--as-of", help="Snapshot date YYYY-MM-DD: data end at Brussels midnight of that day (default today)."
    ),
    manifest: bool = typer.Option(True, "--manifest/--no-manifest", help="Regenerate MANIFEST.json afterwards."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Fetch upstream data (cached under data/raw) and (re)build normalized tables under data/public."""

    logging.basicConfig(level=logging.INFO if verbose else logging.WARNING, format="%(levelname)s %(message)s")
    ctx = _context(data_dir, raw_dir, refresh, offline, as_of)
    if target == "raw":
        if not dataset or dataset not in ELIA_DATASETS:
            raise typer.BadParameter(f"--dataset must be one of {sorted(ELIA_DATASETS)}")
        lo = to_utc(start or "2024-05-22")
        hi = to_utc(end) if end else ctx.now
        chunks = ctx.fetch(dataset, lo, hi)
        for chunk in chunks:
            typer.echo(
                f"{chunk.path.name}\t{chunk.record.size_bytes}\t{'cached' if chunk.record.from_cache else 'downloaded'}"
            )
        return
    targets = list(TARGETS) if target == "all" else [target]
    for name in targets:
        if name not in TARGETS:
            raise typer.BadParameter(f"Unknown target {name!r}; expected one of {', '.join(TARGETS)}, all, raw")
        written = build_target(ctx, name, start=start, end=end, zone=zone, product=product, kind=kind)
        for table in written:
            typer.echo(f"{table.relpath}\t{table.rows} rows\t{table.size_bytes / 1e6:.2f} MB")
    if manifest:
        result = build_manifest(ctx.root)
        typer.echo(f"{MANIFEST_NAME}: {result['n_files']} files, {result['total_size_bytes'] / 1e6:.1f} MB")


@data_app.command("list")
def list_tables(
    data_dir: Path | None = typer.Option(None, "--data-dir", help="Data root (default: data/public)."),
) -> None:
    """List the files described by the sidecars under the data root (or the registry when none exist)."""

    root = (data_dir or data_root()).resolve()
    sidecars = collect_sidecars(root) if root.exists() else []
    if not sidecars:
        typer.echo(f"No sidecars under {root}; registered tables:")
        for spec in TABLES.values():
            typer.echo(f"  {spec.pattern}\t{spec.fetch_command()}")
        return
    for entry in sidecars:
        flag = "" if entry.get("redistributed", True) else "\t[not redistributed]"
        size = int(entry.get("size_bytes", 0)) / 1e6
        typer.echo(
            f"{entry['path']}\t{entry.get('rows', '?')} rows\t{size:.2f} MB\t"
            f"{entry.get('coverage_start_utc')} -> {entry.get('coverage_end_utc')}\t{entry.get('licence')}{flag}"
        )


@data_app.command("verify")
def verify(
    data_dir: Path | None = typer.Option(None, "--data-dir", help="Data root (default: data/public)."),
) -> None:
    """Check every redistributed file against the sha256 recorded in MANIFEST.json."""

    root = (data_dir or data_root()).resolve()
    ok, issues = verify_manifest(root)
    for path in ok:
        typer.echo(f"OK\t{path}")
    for issue in issues:
        typer.echo(f"FAIL\t{issue.path}\t{issue.problem}", err=True)
    if issues:
        raise typer.Exit(code=1)
    typer.echo(f"{len(ok)} files verified")


@data_app.command("manifest")
def manifest_command(
    data_dir: Path | None = typer.Option(None, "--data-dir", help="Data root (default: data/public)."),
) -> None:
    """Regenerate MANIFEST.json from the ``*.meta.json`` sidecars."""

    root = (data_dir or data_root()).resolve()
    result = build_manifest(root)
    typer.echo(f"{root / MANIFEST_NAME}: {result['n_files']} files, {result['total_size_bytes'] / 1e6:.1f} MB")
