"""scripts/ops/build_models.py and models/: manifest, sha256 verification, frozen reference fold (no fitting)."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

from euroflex_bess_lab.realtime.forecasting.deploy import DEPLOY_FILES, FUSION_FILE, RL_POLICY_FILE, deployment_fold

REPO_ROOT = Path(__file__).resolve().parents[1]
MODELS = REPO_ROOT / "models"


@pytest.fixture(scope="module")
def bm() -> ModuleType:
    path = REPO_ROOT / "scripts" / "ops" / "build_models.py"
    spec = importlib.util.spec_from_file_location("ops_build_models_under_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _fake_models(directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    for index, name in enumerate(("deploy_price_current.pkl", "deploy_prism_current.pkl", "deploy_prism_zero.pkl")):
        (directory / name).write_bytes(bytes([index]) * (100 + index))
    (directory / FUSION_FILE).write_text(json.dumps({"variance": [1.0, 2.0]}), encoding="utf-8")
    (directory / "deploy_manifest.json").write_text(
        json.dumps({"fold": {"train_end": "2026-10-24T22:00:00+00:00"}, "forecasters": DEPLOY_FILES}), encoding="utf-8"
    )
    return directory


def test_file_names_match_the_deploy_module(bm: ModuleType) -> None:
    assert set(DEPLOY_FILES.values()) | {FUSION_FILE} == set(bm.REQUIRED_FILES)
    assert RL_POLICY_FILE in bm.MODEL_FILES


def test_manifest_verify_and_tamper_detection(bm: ModuleType, tmp_path: Path) -> None:
    directory = _fake_models(tmp_path / "models")
    manifest = bm.build_manifest(directory)
    assert manifest["missing"] == [] and manifest["models_id"] and len(manifest["models_id"]) == 16
    assert manifest["fold"] == {"train_end": "2026-10-24T22:00:00+00:00"}
    assert bm.verify(directory, manifest) == []
    (directory / "deploy_prism_zero.pkl").write_bytes(b"\x07" * 102)  # same size, other bytes
    problems = bm.verify(directory, manifest)
    assert len(problems) == 1 and problems[0].startswith("deploy_prism_zero.pkl: sha256")
    (directory / "deploy_price_current.pkl").unlink()
    assert any("deploy_price_current.pkl: missing" in p for p in bm.verify(directory, manifest))
    assert bm.verify(directory, {"files": {}}) == ["manifest lists no files"]


def test_models_id_depends_only_on_required_files(bm: ModuleType, tmp_path: Path) -> None:
    directory = _fake_models(tmp_path / "models")
    first = bm.models_id(bm.describe_files(directory))
    (directory / "deploy_manifest.json").write_text("{}", encoding="utf-8")  # fit timings differ between builds
    (directory / RL_POLICY_FILE).write_bytes(b"policy")
    assert bm.models_id(bm.describe_files(directory)) == first
    (directory / FUSION_FILE).write_text(json.dumps({"variance": [1.0, 3.0]}), encoding="utf-8")
    assert bm.models_id(bm.describe_files(directory)) != first


def test_cli_manifest_then_verify(bm: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    directory = _fake_models(tmp_path / "models")
    assert bm.main(["verify", str(directory)]) == 2  # no manifest yet
    assert bm.main(["manifest", str(directory)]) == 0
    assert (directory / bm.MANIFEST_NAME).is_file()
    assert bm.main(["verify", str(directory)]) == 0
    (directory / "deploy_price_current.pkl").write_bytes(b"tampered")
    assert bm.main(["verify", str(directory)]) == 1
    capsys.readouterr()
    assert bm.main(["manifest", str(tmp_path / "absent")]) == 2


def test_build_refuses_without_scikit_learn(bm: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    real_import = bm.importlib.import_module

    def fake_import(name: str, *args: object) -> ModuleType:
        if name == "sklearn":
            raise ImportError("no sklearn")
        return real_import(name, *args)

    monkeypatch.setattr(bm.importlib, "import_module", fake_import)
    with pytest.raises(SystemExit, match="scikit-learn"):
        bm.build(tmp_path / "out", data_end="2026-10-24T22:00:00+00:00", workers=1, threads=1)


def test_reference_manifest_describes_the_frozen_models(bm: ModuleType) -> None:
    reference = json.loads((MODELS / "reference_manifest.json").read_text(encoding="utf-8"))
    assert reference["schema"] == bm.SCHEMA and reference["missing"] == []
    assert set(bm.REQUIRED_FILES) <= set(reference["files"])
    assert bm.models_id(reference["files"]) == reference["models_id"]
    assert reference["total_size_bytes"] == sum(v["size_bytes"] for v in reference["files"].values())
    assert reference["total_size_bytes"] > 20_000_000  # why the models are rebuilt locally, not committed
    fold = deployment_fold("2026-10-24T22:00:00Z", data_start="2024-05-21T22:00:00Z")
    assert pd.Timestamp(reference["fold"]["train_end"]) == fold.train_end
    assert pd.Timestamp(reference["fold"]["train_start"]) == fold.train_start
    assert pd.Timestamp(reference["fold"]["test_end"]) == fold.test_end
    assert bm.pinned_data_end(None) == "2026-10-24T22:00:00+00:00"
    assert "/" + "Users/" not in json.dumps(reference)


def test_models_directory_keeps_pickles_out_of_git() -> None:
    rules = [line.strip() for line in (MODELS / ".gitignore").read_text(encoding="utf-8").splitlines()]
    assert "*" in rules and "!reference_manifest.json" in rules and "!.gitignore" in rules
    committed = sorted(p.name for p in MODELS.iterdir() if p.name in {".gitignore", "reference_manifest.json"})
    assert committed == [".gitignore", "reference_manifest.json"]
