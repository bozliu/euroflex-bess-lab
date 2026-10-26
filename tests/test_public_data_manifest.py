from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from euroflex_bess_lab.public_data.cli import data_app
from euroflex_bess_lab.public_data.docs import render_docs_page, render_licenses, render_readme
from euroflex_bess_lab.public_data.http import sha256_file
from euroflex_bess_lab.public_data.writers import (
    MANIFEST_NAME,
    build_manifest,
    sidecar_path,
    verify_manifest,
    write_stub_sidecar,
    write_table,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "public"
LOCAL_PREFIX = "/" + "Users" + "/"  # never write absolute local paths into files


def _frame(n: int = 96) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp_utc": pd.date_range("2026-03-02", periods=n, freq="15min", tz="UTC"),
            "price_eur_mwh": pd.Series(range(n), dtype="float32"),
            "quality_status": pd.Series(["Validated"] * n, dtype="string"),
        }
    )


def test_write_table_creates_zstd_parquet_and_sidecar(tmp_path: Path) -> None:
    meta = {
        "table": "demo",
        "licence": "CC BY 4.0",
        "attribution": "Elia Transmission Belgium SA",
        "redistributed": True,
    }
    written = write_table(_frame(), tmp_path, "belgium/demo.parquet", meta)
    assert written.path.exists() and written.rows == 96
    sidecar = json.loads(sidecar_path(written.path).read_text(encoding="utf-8"))
    assert sidecar["sha256"] == sha256_file(written.path)
    assert sidecar["path"] == "belgium/demo.parquet" and sidecar["rows"] == 96
    assert [c["name"] for c in sidecar["columns"]] == ["timestamp_utc", "price_eur_mwh", "quality_status"]
    metadata = pq.ParquetFile(written.path).metadata
    assert metadata.row_group(0).column(0).compression == "ZSTD"
    roundtrip = pd.read_parquet(written.path)
    assert str(roundtrip["timestamp_utc"].dtype) == "datetime64[ns, UTC]"
    pd.testing.assert_series_equal(roundtrip["price_eur_mwh"], _frame()["price_eur_mwh"])


def test_manifest_aggregates_sidecars_and_verify_detects_tampering(tmp_path: Path) -> None:
    write_table(
        _frame(), tmp_path, "belgium/a.parquet", {"licence": "CC BY 4.0", "retrieved_at_utc": "2026-10-26T00:00:00Z"}
    )
    write_table(_frame(10), tmp_path, "energy_charts/b.parquet", {"licence": "CC BY 4.0"})
    write_stub_sidecar(tmp_path, "germany/c.parquet", {"licence": "proprietary", "rows": 5, "sha256": "x"})
    manifest = build_manifest(tmp_path)
    assert manifest["n_files"] == 2 and manifest["n_not_redistributed"] == 1
    assert [f["path"] for f in manifest["files"]] == [
        "belgium/a.parquet",
        "energy_charts/b.parquet",
        "germany/c.parquet",
    ]
    assert manifest["latest_retrieval_utc"] == "2026-10-26T00:00:00Z"
    assert json.loads((tmp_path / MANIFEST_NAME).read_text(encoding="utf-8")) == manifest
    ok, issues = verify_manifest(tmp_path)
    assert ok == ["belgium/a.parquet", "energy_charts/b.parquet"] and issues == []
    # Rebuilding the manifest is deterministic.
    assert build_manifest(tmp_path) == manifest
    write_table(_frame(5), tmp_path, "belgium/a.parquet", {"licence": "CC BY 4.0"})  # changes the file...
    ok, issues = verify_manifest(tmp_path, manifest)  # ...but not the old manifest
    assert [i.path for i in issues] == ["belgium/a.parquet"] and "sha256 mismatch" in issues[0].problem
    (tmp_path / "energy_charts" / "b.parquet").unlink()
    _frame().to_parquet(tmp_path / "belgium" / "orphan.parquet")
    _, issues = verify_manifest(tmp_path, manifest)
    problems = {i.path: i.problem for i in issues}
    assert problems["energy_charts/b.parquet"] == "missing file"
    assert problems["belgium/orphan.parquet"] == "parquet file not listed in manifest"


def test_fixture_tree_manifest_verifies() -> None:
    ok, issues = verify_manifest(FIXTURES)
    assert issues == [], issues
    assert len(ok) == json.loads((FIXTURES / MANIFEST_NAME).read_text(encoding="utf-8"))["n_files"]


def test_cli_list_verify_manifest(tmp_path: Path) -> None:
    root = tmp_path / "public"
    shutil.copytree(FIXTURES, root)
    runner = CliRunner()
    listed = runner.invoke(data_app, ["list", "--data-dir", str(root)])
    assert listed.exit_code == 0, listed.output
    assert "belgium/minute_2025.parquet" in listed.output and "CC BY 4.0" in listed.output
    verified = runner.invoke(data_app, ["verify", "--data-dir", str(root)])
    assert verified.exit_code == 0, verified.output
    assert "files verified" in verified.output
    (root / "belgium" / "qh_2025.parquet").write_bytes(b"corrupted")
    failed = runner.invoke(data_app, ["verify", "--data-dir", str(root)])
    assert failed.exit_code == 1
    rebuilt = runner.invoke(data_app, ["manifest", "--data-dir", str(root)])
    assert rebuilt.exit_code == 0 and "MANIFEST.json" in rebuilt.output
    empty = runner.invoke(data_app, ["list", "--data-dir", str(tmp_path / "nothing")])
    assert empty.exit_code == 0 and "euroflex data fetch belgium-minute" in empty.output


def test_cli_fetch_offline_from_raw_cache(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    payload = {"unix_seconds": [1749506400 + 3600 * i for i in range(24)], "price": [50.0 + i for i in range(24)]}
    target = raw / "energy_charts" / "BE" / "price_BE_2025-06.json"
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps(payload), encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(
        data_app,
        [
            "fetch",
            "day-ahead",
            "--zone",
            "BE",
            "--start",
            "2025-06-01",
            "--end",
            "2025-07-01",
            "--offline",
            "--data-dir",
            str(tmp_path / "public"),
            "--raw-dir",
            str(raw),
        ],
    )
    assert result.exit_code != 0  # months other than 2025-06 are not cached and offline forbids downloads
    unknown = runner.invoke(data_app, ["fetch", "nonsense", "--offline", "--data-dir", str(tmp_path / "public")])
    assert unknown.exit_code != 0


def test_rendered_docs_mention_attribution_and_files() -> None:
    manifest = json.loads((FIXTURES / MANIFEST_NAME).read_text(encoding="utf-8"))
    readme = render_readme(manifest)
    licences = render_licenses(manifest)
    page = render_docs_page(manifest)
    assert "belgium/minute_2025.parquet" in readme and "fetch_all_public.py" in readme
    assert "Elia Transmission Belgium SA" in licences and "CC BY 4.0" in licences
    assert "SMARD.de" in licences and "not redistributed" in licences
    assert "Minute 14 vs. final quarter-hour" in page and "## Attribution" in page
    for text in (readme, licences, page):
        assert LOCAL_PREFIX not in text


def test_write_table_enforces_per_file_budget(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="per-file budget"):
        write_table(_frame(), tmp_path, "belgium/too_big.parquet", {}, max_file_bytes=10)
    assert not (tmp_path / "belgium" / "too_big.parquet").exists()
    assert not (tmp_path / "belgium" / "too_big.parquet.tmp").exists()
