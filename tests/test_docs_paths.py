from __future__ import annotations

import re
from pathlib import Path

from euroflex_bess_lab.capabilities import render_capability_matrix_markdown

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_readme_and_docs_reference_existing_example_and_notebook_paths() -> None:
    targets = [REPO_ROOT / "README.md", REPO_ROOT / "examples" / "README.md"]
    targets.extend(sorted((REPO_ROOT / "docs").glob("*.md")))

    referenced_paths: set[Path] = set()
    pattern = re.compile(r"(examples/[A-Za-z0-9_./-]+|notebooks/[A-Za-z0-9_./-]+)")
    for target in targets:
        content = target.read_text(encoding="utf-8")
        for match in pattern.findall(content):
            referenced_paths.add(REPO_ROOT / match)

    missing = sorted(str(path) for path in referenced_paths if not path.exists())
    assert not missing, f"Documentation references missing paths: {missing}"


def test_deleted_notebook_names_are_not_referenced_anywhere() -> None:
    pattern = re.compile(r"belgium-da-only-battery|belgium-da-imbalance-battery|belgium-battery-sensitivity")
    files = [REPO_ROOT / "README.md", REPO_ROOT / "examples" / "README.md"]
    files.extend(sorted((REPO_ROOT / "docs").glob("*.md")))

    offenders = []
    for target in files:
        if pattern.search(target.read_text(encoding="utf-8")):
            offenders.append(str(target))
    assert not offenders, f"Deleted notebook names still referenced in: {offenders}"


def test_capability_matrix_doc_matches_code_declared_capabilities() -> None:
    doc = (REPO_ROOT / "docs" / "capability_matrix.md").read_text(encoding="utf-8").strip()
    assert doc == render_capability_matrix_markdown().strip()
