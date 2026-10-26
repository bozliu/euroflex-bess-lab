"""Reproduce the whole ``data/public`` tree from upstream sources.

Usage::

    python scripts/data/fetch_all_public.py                 # download (cached in data/raw/) and rebuild all
    python scripts/data/fetch_all_public.py --offline       # rebuild from the raw cache only
    python scripts/data/fetch_all_public.py --only belgium-minute day-ahead
    python scripts/data/fetch_all_public.py --offline --as-of 2026-10-26   # reproduce the committed snapshot

Steps: build every table group (see ``euroflex data fetch --help``), regenerate ``MANIFEST.json``, render
``data/public/README.md``, ``data/public/LICENSES.md`` and ``docs/public_datasets.md`` from the manifest, and
verify every sha256. Raw intermediates that are not needed after normalization can be removed with
``--prune-raw`` (keeps the small QH/day files, drops the large per-bid exports once the curves are built).
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from pathlib import Path

from euroflex_bess_lab.public_data.build import TARGETS, BuildContext, as_of_snapshot, build_target
from euroflex_bess_lab.public_data.docs import render_docs_page, render_licenses, render_readme
from euroflex_bess_lab.public_data.paths import data_root, raw_root
from euroflex_bess_lab.public_data.writers import build_manifest, verify_manifest

PRUNABLE_RAW = ("ods156", "ods157")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", type=Path, default=None, help="output root (default: data/public)")
    parser.add_argument("--raw-dir", type=Path, default=None, help="raw cache root (default: data/raw)")
    parser.add_argument("--only", nargs="*", default=None, help=f"subset of targets: {', '.join(TARGETS)}")
    parser.add_argument("--refresh", action="store_true", help="re-download cached raw chunks")
    parser.add_argument("--offline", action="store_true", help="never touch the network")
    parser.add_argument("--docs-dir", type=Path, default=None, help="where docs/public_datasets.md goes")
    parser.add_argument("--prune-raw", action="store_true", help="delete large per-bid raw exports afterwards")
    parser.add_argument(
        "--as-of",
        default=None,
        help="snapshot date YYYY-MM-DD (data end at Brussels midnight of that day); default today. Use the "
        "snapshot_end_utc recorded in the sidecars to reproduce a committed tree from the raw cache.",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(levelname)s %(message)s")

    root = (args.data_dir or data_root()).resolve()
    raw = (args.raw_dir or (root.parent / "raw" if args.data_dir else raw_root())).resolve()
    ctx = BuildContext(
        root=root, raw_dir=raw, refresh=args.refresh, offline=args.offline, now=as_of_snapshot(args.as_of)
    )
    targets = args.only or list(TARGETS)
    for target in targets:
        for table in build_target(ctx, target):
            print(f"{table.relpath:52s} {table.rows:>10,d} rows {table.size_bytes / 1e6:8.2f} MB")

    manifest = build_manifest(root)
    (root / "README.md").write_text(render_readme(manifest), encoding="utf-8")
    (root / "LICENSES.md").write_text(render_licenses(manifest), encoding="utf-8")
    docs_dir = args.docs_dir or (root.parent.parent / "docs")
    if docs_dir.is_dir():
        (docs_dir / "public_datasets.md").write_text(render_docs_page(manifest), encoding="utf-8")
    ok, issues = verify_manifest(root, manifest)
    print(
        f"MANIFEST.json: {manifest['n_files']} files, {manifest['total_size_bytes'] / 1e6:.1f} MB; verified {len(ok)}"
    )
    for issue in issues:
        print(f"FAIL {issue.path}: {issue.problem}", file=sys.stderr)
    if args.prune_raw:
        for dataset_id in PRUNABLE_RAW:
            target_dir = raw / "elia" / dataset_id
            if target_dir.exists():
                shutil.rmtree(target_dir)
                print(f"pruned raw cache {target_dir.name}")
    return 1 if issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
