"""Fetch and rebuild the Great Britain public data tables under ``data/public/gb``.

Usage::

    .venv/bin/python scripts/research/gb_fetch_data.py --target all          # everything (about 5 minutes)
    .venv/bin/python scripts/research/gb_fetch_data.py --target system-prices --offline

Targets: ``system-prices`` (Elexon BMRS DISEBSP, one request per settlement day), ``market-index`` (BMRS MID),
``eac`` (NESO EAC DC/DM/DR and reserve auction results), ``frequency`` (BMRS 15-second frequency and the derived
per-settlement-period response energy table), ``fx`` (ECB GBP per EUR). Raw downloads are cached under
``data/raw/gb`` (gitignored) with provenance records; normalized tables and ``.meta.json`` sidecars are written
under ``data/public/gb``. Regenerate ``data/public/MANIFEST.json`` afterwards with ``euroflex data manifest``.
"""

from __future__ import annotations

import argparse
import logging
import time

from euroflex_bess_lab.public_data.gb.build import build_gb
from euroflex_bess_lab.public_data.gb.cli import make_context
from euroflex_bess_lab.public_data.gb.tables import FETCH_TARGETS


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target", nargs="+", default=["all"], help=f"{', '.join(FETCH_TARGETS)} or all")
    parser.add_argument("--refresh", action="store_true", help="re-download cached raw chunks")
    parser.add_argument("--offline", action="store_true", help="use the raw cache only")
    parser.add_argument("--as-of", default=None, help="snapshot date YYYY-MM-DD (data end at UK midnight)")
    parser.add_argument("--start", default=None, help="coverage start (default 2023-12-01)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(levelname)s %(message)s")
    ctx = make_context(refresh=args.refresh, offline=args.offline, as_of=args.as_of, start=args.start)
    began = time.perf_counter()
    for table in build_gb(args.target, ctx):
        print(f"{table.relpath}\t{table.rows} rows\t{table.size_bytes / 1e6:.2f} MB")
    print(f"done in {time.perf_counter() - began:.0f} s (snapshot end {ctx.now})")


if __name__ == "__main__":
    main()
