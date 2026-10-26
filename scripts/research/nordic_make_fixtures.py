"""Slice ``data/public/nordic`` into the tiny fixture tree ``tests/fixtures/nordic/public/nordic``.

    .venv/bin/python scripts/research/nordic_make_fixtures.py

Windows (UTC, half-open): 2025-06-07 22:00 to 2025-06-13 22:00 (hourly day-ahead era), 2025-09-25 22:00 to
2025-10-03 22:00 (switch to 15-minute day-ahead products on 2025-10-01) and 2025-10-22 22:00 to 2025-10-27 23:00
(25-hour day 2025-10-26). Sidecars keep the source metadata of the full tables and get fresh rows/sha256.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from euroflex_bess_lab.public_data.writers import write_table

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE = REPO_ROOT / "data" / "public" / "nordic"
TARGET_ROOT = REPO_ROOT / "tests" / "fixtures" / "nordic" / "public"
WINDOWS = (
    ("2025-06-07T22:00:00Z", "2025-06-13T22:00:00Z"),
    ("2025-09-25T22:00:00Z", "2025-10-03T22:00:00Z"),
    ("2025-10-22T22:00:00Z", "2025-10-27T23:00:00Z"),
)
KEEP_META = (
    "table",
    "title",
    "source",
    "dataset_ids",
    "dataset_pages",
    "licence",
    "licence_url",
    "terms_url",
    "attribution",
    "redistributed",
    "zone",
    "zones",
    "time_column",
    "transformation_notes",
    "fetch_command",
)


def main() -> None:
    for parquet in sorted(SOURCE.glob("*.parquet")):
        meta = json.loads(parquet.with_name(parquet.stem + ".meta.json").read_text(encoding="utf-8"))
        frame = pd.read_parquet(parquet)
        column = meta["time_column"]
        mask = pd.Series(False, index=frame.index)
        for lo, hi in WINDOWS:
            mask |= (frame[column] >= pd.Timestamp(lo)) & (frame[column] < pd.Timestamp(hi))
        part = frame.loc[mask].reset_index(drop=True)
        fixture_meta = {k: meta[k] for k in KEEP_META if k in meta}
        if "resolution_minutes" in part.columns:
            ends = part[column] + pd.to_timedelta(part["resolution_minutes"].astype("int64"), unit="min")
        elif "delivery_end_utc" in part.columns:
            ends = part["delivery_end_utc"]
        else:
            ends = part[column] + pd.Timedelta(hours=1)
        fixture_meta["coverage_start_utc"] = f"{part[column].min():%Y-%m-%dT%H:%M:%SZ}"
        fixture_meta["coverage_end_utc"] = f"{ends.max():%Y-%m-%dT%H:%M:%SZ}"
        fixture_meta["fixture_windows_utc"] = [list(w) for w in WINDOWS]
        fixture_meta["note"] = (
            "Test fixture sliced from data/public/" + meta["path"] + " by scripts/research/nordic_make_fixtures.py"
        )
        written = write_table(part, TARGET_ROOT, meta["path"], fixture_meta)
        print(f"{written.relpath}\t{written.rows} rows\t{written.size_bytes / 1e3:.1f} kB")


if __name__ == "__main__":
    main()
