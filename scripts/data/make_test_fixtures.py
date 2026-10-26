"""Cut tiny fixture slices of ``data/public`` into ``tests/fixtures/public`` (same layout, sidecars, manifest).

Windows (UTC, half-open): 2025-06-10..2025-06-13 and 2026-03-02..2026-03-05 for every table that covers them,
2024-05-20..2024-05-21 for the pre-MARI legacy tables. German reserve data are not redistributable, so the
fixture tree gets a small *synthetic* Germany table instead (clearly flagged in its sidecar).

Usage::

    python scripts/data/make_test_fixtures.py [--source data/public] [--dest tests/fixtures/public]
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

from euroflex_bess_lab.public_data.registry import TABLES
from euroflex_bess_lab.public_data.writers import SIDECAR_SUFFIX, build_manifest, write_table

WINDOWS = [
    (pd.Timestamp("2025-06-10", tz="UTC"), pd.Timestamp("2025-06-13", tz="UTC")),
    (pd.Timestamp("2026-03-02", tz="UTC"), pd.Timestamp("2026-03-05", tz="UTC")),
]
LEGACY_WINDOWS = [(pd.Timestamp("2024-05-20", tz="UTC"), pd.Timestamp("2024-05-21", tz="UTC"))]
DROP_META = {"schema_version", "path", "rows", "columns", "size_bytes", "sha256", "inputs", "validation"}


def _slice(frame: pd.DataFrame, column: str, windows: list[tuple[pd.Timestamp, pd.Timestamp]]) -> pd.DataFrame:
    mask = np.zeros(len(frame), dtype=bool)
    for lo, hi in windows:
        mask |= ((frame[column] >= lo) & (frame[column] < hi)).to_numpy()
    return frame.loc[mask].reset_index(drop=True)


def _synthetic_germany() -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(20261026)
    frames: dict[str, pd.DataFrame] = {}
    for product, directions in (("fcr", ["symmetric"]), ("afrr", ["up", "down"])):
        rows = []
        for lo, hi in WINDOWS:
            for day in pd.date_range(lo, hi, freq="D", inclusive="left"):
                local_day = day.tz_convert(None)
                for block in range(6):
                    start = (local_day + pd.Timedelta(hours=4 * block)).tz_localize("Europe/Berlin").tz_convert("UTC")
                    end = (local_day + pd.Timedelta(hours=4 * block + 4)).tz_localize("Europe/Berlin").tz_convert("UTC")
                    for direction in directions:
                        price = float(rng.uniform(2.0, 20.0))
                        rows.append(
                            {
                                "delivery_start_utc": start,
                                "delivery_end_utc": end,
                                "product": "FCR" if product == "fcr" else "aFRR",
                                "direction": direction,
                                "price_eur_per_mw_h": price,
                                "volume_mw": 580.0 if product == "fcr" else 1900.0,
                                "marginal_price_eur_per_mw_h": price * (1.0 if product == "fcr" else 1.15),
                            }
                        )
        frame = pd.DataFrame(rows)
        frame["delivery_start_utc"] = pd.to_datetime(frame["delivery_start_utc"], utc=True).astype(
            "datetime64[ns, UTC]"
        )
        frame["delivery_end_utc"] = pd.to_datetime(frame["delivery_end_utc"], utc=True).astype("datetime64[ns, UTC]")
        for column in ("price_eur_per_mw_h", "volume_mw", "marginal_price_eur_per_mw_h"):
            frame[column] = frame[column].astype("float32")
        for column in ("product", "direction"):
            frame[column] = frame[column].astype("string")
        frames[product] = frame
    return frames


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", type=Path, default=Path("data/public"))
    parser.add_argument("--dest", type=Path, default=Path("tests/fixtures/public"))
    args = parser.parse_args(argv)
    source: Path = args.source
    dest: Path = args.dest
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    by_path = {}
    for spec in TABLES.values():
        if spec.partition == "year":
            for year in range(spec.first_year or 2019, 2027):
                by_path[spec.relpath(year)] = spec
        else:
            by_path[spec.relpath()] = spec
    for parquet in sorted(source.rglob("*.parquet")):
        rel = parquet.relative_to(source).as_posix()
        spec = by_path.get(rel)
        if spec is None:
            continue
        frame = pd.read_parquet(parquet)
        windows = LEGACY_WINDOWS if spec.key.endswith("_legacy") else WINDOWS
        sliced = _slice(frame, spec.time_column, windows)
        if sliced.empty:
            continue
        meta_path = parquet.with_name(parquet.name.removesuffix(".parquet") + SIDECAR_SUFFIX)
        meta = {k: v for k, v in json.loads(meta_path.read_text(encoding="utf-8")).items() if k not in DROP_META}
        meta["source_urls"] = list(meta.get("source_urls", []))[:3]
        meta["fixture"] = {
            "sliced_from": f"data/public/{rel}",
            "windows_utc": [[f"{lo:%Y-%m-%dT%H:%M:%SZ}", f"{hi:%Y-%m-%dT%H:%M:%SZ}"] for lo, hi in windows],
        }
        meta["coverage_start_utc"] = f"{sliced[spec.time_column].min():%Y-%m-%dT%H:%M:%SZ}"
        meta["coverage_end_utc"] = f"{sliced[spec.time_column].max():%Y-%m-%dT%H:%M:%SZ}"
        written = write_table(sliced, dest, rel, meta)
        print(f"{rel:52s} {written.rows:>7d} rows {written.size_bytes / 1e3:8.1f} kB")
    for product, frame in _synthetic_germany().items():
        rel = TABLES[f"germany_{product}_capacity"].relpath()
        meta = {
            "table": f"germany_{product}_capacity",
            "title": "SYNTHETIC test fixture (random numbers in the contract layout; not regelleistung.net data)",
            "source": "synthetic (scripts/data/make_test_fixtures.py)",
            "dataset_ids": [],
            "licence": "Apache-2.0 (synthetic test data)",
            "attribution": "euroflex-bess-lab",
            "redistributed": True,
            "synthetic": True,
            "time_column": "delivery_start_utc",
            "coverage_start_utc": f"{frame['delivery_start_utc'].min():%Y-%m-%dT%H:%M:%SZ}",
            "coverage_end_utc": f"{frame['delivery_end_utc'].max():%Y-%m-%dT%H:%M:%SZ}",
        }
        written = write_table(frame, dest, rel, meta)
        print(f"{rel:52s} {written.rows:>7d} rows {written.size_bytes / 1e3:8.1f} kB (synthetic)")
    manifest = build_manifest(dest)
    print(f"fixture MANIFEST.json: {manifest['n_files']} files, {manifest['total_size_bytes'] / 1e6:.2f} MB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
