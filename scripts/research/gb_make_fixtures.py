"""Write the tiny GB test fixtures under ``tests/fixtures/gb`` from the committed tables and the raw cache.

Usage (after ``scripts/research/gb_fetch_data.py``)::

    .venv/bin/python scripts/research/gb_make_fixtures.py

* ``tests/fixtures/gb/public/gb/*.parquet`` (+ ``.meta.json``): slices of the normalized tables for
  2025-06-03 .. 2025-06-13 (frequency only 2025-06-10), loadable with ``root=tests/fixtures/gb/public``.
* ``tests/fixtures/gb/raw/*``: small excerpts of upstream payloads (BMRS system prices / MID / frequency, NESO EAC
  results summary CSV, ECB CSV) for parser tests.

The excerpts are BMRS data (Elexon BMRS open data licence), NESO data (NESO Open Data Licence) and ECB data, all
redistributable with attribution (see ``data/public/gb/*.meta.json``).
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from euroflex_bess_lab.public_data.gb.tables import GB_TABLES

REPO_ROOT = Path(__file__).resolve().parents[2]
PUBLIC = REPO_ROOT / "data" / "public"
RAW = REPO_ROOT / "data" / "raw" / "gb"
OUT = REPO_ROOT / "tests" / "fixtures" / "gb"
START, END = pd.Timestamp("2025-06-03", tz="UTC"), pd.Timestamp("2025-06-14", tz="UTC")
FREQ_DAY = pd.Timestamp("2025-06-10", tz="UTC")
KEEP_META = (
    "table",
    "title",
    "source",
    "dataset_ids",
    "licence",
    "licence_url",
    "attribution",
    "time_column",
    "redistributed",
)


def _slice(key: str, frame: pd.DataFrame, lo: pd.Timestamp, hi: pd.Timestamp, relpath: str) -> None:
    spec = GB_TABLES[key]
    column = spec.time_column
    part = frame[(frame[column] >= lo) & (frame[column] < hi)].reset_index(drop=True)
    target = OUT / "public" / relpath
    target.parent.mkdir(parents=True, exist_ok=True)
    part.to_parquet(target, index=False, compression="zstd")
    meta_src = PUBLIC / relpath.replace(".parquet", ".meta.json")
    meta = json.loads(meta_src.read_text(encoding="utf-8"))
    sidecar = {k: meta[k] for k in KEEP_META if k in meta}
    sidecar.update(
        {
            "path": relpath,
            "fixture_of": relpath,
            "coverage_start_utc": f"{lo:%Y-%m-%dT%H:%M:%SZ}",
            "coverage_end_utc": f"{hi:%Y-%m-%dT%H:%M:%SZ}",
            "rows": int(len(part)),
            "generated_by": "scripts/research/gb_make_fixtures.py",
        }
    )
    target.with_name(target.name.replace(".parquet", ".meta.json")).write_text(
        json.dumps(sidecar, indent=2) + "\n", encoding="utf-8"
    )


def tables() -> None:
    for key in ("gb_system_prices", "gb_market_index", "gb_response_energy", "gb_fx"):
        relpath = GB_TABLES[key].relpath()
        frame = pd.read_parquet(PUBLIC / relpath)
        lo = START - pd.Timedelta(days=10) if key == "gb_fx" else START
        _slice(key, frame, lo, END, relpath)
    for key in ("gb_response_auctions", "gb_reserve_auctions"):
        relpath = GB_TABLES[key].relpath()
        frame = pd.read_parquet(PUBLIC / relpath)
        _slice(key, frame, START - pd.Timedelta(hours=1), END + pd.Timedelta(days=1), relpath)
    relpath = GB_TABLES["gb_frequency"].relpath(2025)
    frame = pd.read_parquet(PUBLIC / relpath)
    _slice("gb_frequency", frame, FREQ_DAY, FREQ_DAY + pd.Timedelta(days=1), relpath)


def raw_excerpts() -> None:
    out = OUT / "raw"
    out.mkdir(parents=True, exist_ok=True)
    sp = json.loads((RAW / "elexon" / "system_prices" / "2025" / "system_prices_2025-06-10.json").read_text())
    (out / "system_prices_2025-06-10.json").write_text(json.dumps(sp) + "\n", encoding="utf-8")
    sp_dst = json.loads((RAW / "elexon" / "system_prices" / "2025" / "system_prices_2025-10-26.json").read_text())
    (out / "system_prices_2025-10-26.json").write_text(json.dumps(sp_dst) + "\n", encoding="utf-8")
    mid = json.loads((RAW / "elexon" / "mid" / "mid_2025-06.json").read_text())
    day = [r for r in mid if str(r.get("settlementDate")) == "2025-06-10"]
    (out / "mid_2025-06-10.json").write_text(json.dumps(day) + "\n", encoding="utf-8")
    freq = json.loads((RAW / "elexon" / "frequency" / "frequency_2025-06.json").read_text())
    hour = [r for r in freq if str(r["measurementTime"]).startswith("2025-06-10T12")]
    (out / "frequency_2025-06-10T12.json").write_text(json.dumps(hour) + "\n", encoding="utf-8")
    eac_files = sorted((RAW / "neso" / "eac").glob("results_summary_*.csv"))
    frames = [pd.read_csv(p) for p in eac_files]
    eac = pd.concat(frames, ignore_index=True)
    keep = eac[eac["deliveryStart"].astype(str).str.startswith(("2025-06-09T22", "2025-06-10", "2025-06-11"))]
    keep.head(400).to_csv(out / "eac_results_summary_excerpt.csv", index=False)
    ecb = (RAW / "ecb" / "exr_d_gbp_eur.csv").read_text(encoding="utf-8").splitlines()
    header = ecb[0]
    rows = [line for line in ecb[1:] if ",2025-06-0" in line or ",2025-06-1" in line]
    (out / "ecb_gbp_eur_excerpt.csv").write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")


def main() -> None:
    tables()
    raw_excerpts()
    total = sum(p.stat().st_size for p in OUT.rglob("*") if p.is_file())
    print(f"wrote {OUT.relative_to(REPO_ROOT)} ({total / 1e3:.0f} kB)")


if __name__ == "__main__":
    main()
