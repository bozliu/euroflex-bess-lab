#!/usr/bin/env python
"""Build, describe and verify the deployment models of the forecast-driven live strategies.

``forecast-mpc`` and SCALES-L (``prism``) trade on forecasters fitted by the ``deploy`` stage of
``scripts/research/realtime_backtest.py`` (see ``euroflex_bess_lab.realtime.forecasting.deploy``). The artefacts are
about 54 MB (three pickled gradient-boosting forecasters), too large to commit, so this script rebuilds them from
the committed ``data/public`` set into a local directory and writes a sha256 ``MANIFEST.json`` next to them.
Pickles are only ever loaded from that local directory (``$EUROFLEX_MODELS_DIR``), which you built yourself.

The deployment fold is pinned to the one in ``models/reference_manifest.json`` (training data up to
2026-10-24 22:00 UTC), so a rebuild reproduces the frozen models of the forward track record even after
``data/public`` grows. ``--data-end latest`` refits on everything that is committed instead.

Usage::

    python scripts/ops/build_models.py build --out models          # about 3 minutes on 3 cores; needs scikit-learn
    python scripts/ops/build_models.py verify models               # every file vs models/MANIFEST.json
    python scripts/ops/build_models.py verify models --reference models/reference_manifest.json
    python scripts/ops/build_models.py manifest <dir> --output <file>  # describe an existing models directory

Exit codes: 0 ok, 1 verification failed, 2 usage error or missing inputs.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import platform
import shutil
import sys
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[2]
MANIFEST_NAME = "MANIFEST.json"
REFERENCE_MANIFEST = REPO / "models" / "reference_manifest.json"
SCHEMA = "euroflex-models/1"
#: Files the deploy stage writes (names from ``realtime.forecasting.deploy``) plus its own manifest.
MODEL_FILES: tuple[str, ...] = (
    "deploy_price_current.pkl",
    "deploy_prism_current.pkl",
    "deploy_prism_zero.pkl",
    "deploy_fusion_variance.json",
    "rl_policy.npz",
    "rl_policy.json",
    "deploy_manifest.json",
)
REQUIRED_FILES: tuple[str, ...] = MODEL_FILES[:4]
COMMITTED_RL_POLICY = REPO / "reports" / "ledgers" / "realtime_rl_policy.npz"
VERSION_PACKAGES: tuple[str, ...] = ("numpy", "pandas", "scipy", "sklearn", "euroflex_bess_lab")


def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(chunk_size), b""):
            digest.update(block)
    return digest.hexdigest()


def library_versions() -> dict[str, str]:
    versions = {"python": platform.python_version()}
    for name in VERSION_PACKAGES:
        try:
            module = importlib.import_module(name)
        except ImportError:
            continue
        versions["scikit-learn" if name == "sklearn" else name] = str(getattr(module, "__version__", "unknown"))
    return versions


def describe_files(directory: Path, names: Sequence[str] = MODEL_FILES) -> dict[str, dict[str, Any]]:
    """``{file name: {sha256, size_bytes}}`` for the model files present in ``directory``."""
    out: dict[str, dict[str, Any]] = {}
    for name in names:
        path = directory / name
        if path.is_file():
            out[name] = {"sha256": sha256_file(path), "size_bytes": path.stat().st_size}
    return out


def models_id(files: Mapping[str, Mapping[str, Any]], names: Sequence[str] = REQUIRED_FILES) -> str:
    """Short identity of a model set: sha256 over the (name, sha256) pairs of the required files."""
    digest = hashlib.sha256()
    for name in sorted(names):
        entry = files.get(name)
        if entry is None:
            raise KeyError(f"model file {name} missing")
        digest.update(f"{name}:{entry['sha256']}\n".encode())
    return digest.hexdigest()[:16]


def build_manifest(directory: Path, *, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Manifest of a models directory (relative file names only, never local paths)."""
    files = describe_files(directory)
    missing = [name for name in REQUIRED_FILES if name not in files]
    manifest: dict[str, Any] = {
        "schema": SCHEMA,
        "files": files,
        "models_id": models_id(files) if not missing else None,
        "missing": missing,
        "total_size_bytes": int(sum(int(v["size_bytes"]) for v in files.values())),
        "versions": library_versions(),
    }
    deploy = directory / "deploy_manifest.json"
    if deploy.is_file():
        payload = json.loads(deploy.read_text(encoding="utf-8"))
        manifest["fold"] = payload.get("fold")
        manifest["forecasters"] = payload.get("forecasters")
    manifest.update(dict(extra or {}))
    return manifest


def verify(directory: Path, manifest: Mapping[str, Any]) -> list[str]:
    """Problems found when checking ``directory`` against ``manifest`` (empty list = verified)."""
    problems: list[str] = []
    expected = manifest.get("files") or {}
    if not isinstance(expected, Mapping) or not expected:
        return ["manifest lists no files"]
    for name in REQUIRED_FILES:
        if name not in expected:
            problems.append(f"{name}: not listed in the manifest")
    for name, entry in sorted(expected.items()):
        path = directory / name
        if not path.is_file():
            problems.append(f"{name}: missing")
            continue
        size = path.stat().st_size
        if int(entry.get("size_bytes", -1)) != size:
            problems.append(f"{name}: size {size} != {entry.get('size_bytes')}")
            continue
        digest = sha256_file(path)
        if digest != entry.get("sha256"):
            problems.append(f"{name}: sha256 {digest[:12]}... != {str(entry.get('sha256'))[:12]}...")
    return problems


def write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path.name} is not a JSON object")
    return payload


def pinned_data_end(value: str | None) -> str:
    """Exclusive end of the training data: explicit value, ``latest`` (committed data), or the reference fold."""
    import pandas as pd

    if value and value != "latest":
        ts = pd.Timestamp(value)
        ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
        return ts.isoformat()
    if value == "latest":
        backtest = _import_backtest()
        return str(backtest.latest_data_end().isoformat())
    if REFERENCE_MANIFEST.is_file():
        fold = read_json(REFERENCE_MANIFEST).get("fold") or {}
        if fold.get("train_end"):
            return str(pd.Timestamp(fold["train_end"]).tz_convert("UTC").isoformat())
    raise SystemExit("no --data-end given and models/reference_manifest.json has no fold; pass --data-end")


def _import_backtest() -> Any:
    """The deploy stage lives in ``scripts/research/realtime_backtest.py`` (importable by name for spawned workers)."""
    research = str(REPO / "scripts" / "research")
    if research not in sys.path:
        sys.path.insert(0, research)
    return importlib.import_module("realtime_backtest")


def build(out: Path, *, data_end: str, workers: int, threads: int) -> dict[str, Any]:
    """Fit the deployment forecasters into ``out`` (the research script writes nothing under ``reports/``)."""
    try:
        importlib.import_module("sklearn")
    except ImportError as exc:
        raise SystemExit("building models needs scikit-learn: pip install 'euroflex-bess-lab[ml]'") from exc
    backtest = _import_backtest()
    out = out.expanduser().resolve()
    out.mkdir(parents=True, exist_ok=True)
    previous = os.environ.get("EUROFLEX_MODELS_DIR")
    os.environ["EUROFLEX_MODELS_DIR"] = str(out)
    try:
        with tempfile.TemporaryDirectory(prefix="euroflex_models_") as scratch:
            root = Path(scratch)
            settings = backtest.Settings(out=root / "reports", cache=root / "cache", workers=workers, threads=threads)
            settings.periods = {"C": (data_end, data_end, ("current",))}
            settings.cache.mkdir(parents=True, exist_ok=True)
            backtest.stage_deploy(settings)
    finally:
        if previous is None:
            os.environ.pop("EUROFLEX_MODELS_DIR", None)
        else:
            os.environ["EUROFLEX_MODELS_DIR"] = previous
    if not (out / "rl_policy.npz").exists() and COMMITTED_RL_POLICY.is_file():
        shutil.copyfile(COMMITTED_RL_POLICY, out / "rl_policy.npz")
        sidecar = COMMITTED_RL_POLICY.with_suffix(".json")
        if sidecar.is_file():
            shutil.copyfile(sidecar, out / "rl_policy.json")
    manifest = build_manifest(out, extra={"data_end_utc": data_end, "built_by": "scripts/ops/build_models.py"})
    write_json(out / MANIFEST_NAME, manifest)
    return manifest


def compare_with_reference(manifest: Mapping[str, Any], reference: Mapping[str, Any]) -> dict[str, str]:
    """Per required file: ``identical`` or ``different`` (a rebuild with other library versions may differ)."""
    ours = manifest.get("files") or {}
    theirs = reference.get("files") or {}
    out: dict[str, str] = {}
    for name in REQUIRED_FILES:
        a, b = ours.get(name), theirs.get(name)
        if a is None or b is None:
            out[name] = "missing"
        else:
            out[name] = "identical" if a.get("sha256") == b.get("sha256") else "different"
    return out


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p_build = sub.add_parser("build", help="fit the deployment forecasters into --out")
    p_build.add_argument("--out", type=Path, default=Path(os.environ.get("EUROFLEX_MODELS_DIR") or "models"))
    p_build.add_argument("--data-end", default=None, help="UTC end of the training data, or 'latest'")
    p_build.add_argument("--workers", type=int, default=3)
    p_build.add_argument("--threads", type=int, default=3)
    p_build.add_argument("--force", action="store_true", help="rebuild even if a verified set is present")
    p_verify = sub.add_parser("verify", help="check a models directory against its MANIFEST.json")
    p_verify.add_argument("directory", type=Path)
    p_verify.add_argument("--manifest", type=Path, default=None, help="default: <directory>/MANIFEST.json")
    p_verify.add_argument("--reference", type=Path, default=None, help="also compare with this reference manifest")
    p_manifest = sub.add_parser("manifest", help="write a manifest describing an existing models directory")
    p_manifest.add_argument("directory", type=Path)
    p_manifest.add_argument("--output", type=Path, default=None, help="default: <directory>/MANIFEST.json")
    args = parser.parse_args(argv)

    if args.command == "build":
        out = Path(args.out)
        existing = out / MANIFEST_NAME
        if existing.is_file() and not args.force and not verify(out, read_json(existing)):
            print(f"models already built and verified in {out.name}/ (use --force to rebuild)")
            return 0
        manifest = build(out, data_end=pinned_data_end(args.data_end), workers=args.workers, threads=args.threads)
        print(json.dumps({k: manifest[k] for k in ("models_id", "total_size_bytes", "missing", "fold")}, indent=1))
        if REFERENCE_MANIFEST.is_file():
            print(json.dumps({"vs_reference": compare_with_reference(manifest, read_json(REFERENCE_MANIFEST))}))
        return 0 if not manifest["missing"] else 1
    if args.command == "verify":
        manifest_path = args.manifest or args.directory / MANIFEST_NAME
        if not manifest_path.is_file():
            print(f"no manifest {manifest_path.name}; run `build` or `manifest` first", file=sys.stderr)
            return 2
        problems = verify(args.directory, read_json(manifest_path))
        for problem in problems:
            print(f"FAIL {problem}", file=sys.stderr)
        if args.reference is not None:
            current = build_manifest(args.directory)
            print(json.dumps({"vs_reference": compare_with_reference(current, read_json(args.reference))}))
        if not problems:
            print(f"OK {args.directory.name}/: {len(read_json(manifest_path)['files'])} files verified")
        return 1 if problems else 0
    if args.command == "manifest":
        if not args.directory.is_dir():
            print(f"{args.directory} is not a directory", file=sys.stderr)
            return 2
        manifest = build_manifest(args.directory)
        write_json(args.output or args.directory / MANIFEST_NAME, manifest)
        print(json.dumps({k: manifest[k] for k in ("models_id", "total_size_bytes", "missing")}, indent=1))
        return 0 if not manifest["missing"] else 1
    return 2  # pragma: no cover


if __name__ == "__main__":
    raise SystemExit(main())
