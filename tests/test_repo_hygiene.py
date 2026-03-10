from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_generated_runtime_directories_are_not_present_in_repo_tree() -> None:
    assert not (REPO_ROOT / "tmp").exists()
    assert not (REPO_ROOT / "src" / "euroflex_bess_lab" / "market_rules").exists()
    assert not (REPO_ROOT / "src" / "euroflex_bess_lab" / "reporting").exists()
    assert sorted(path.name for path in (REPO_ROOT / "artifacts").iterdir()) == [".gitkeep"]


def test_repo_tree_has_no_finder_metadata_files() -> None:
    finder_files = sorted(str(path.relative_to(REPO_ROOT)) for path in REPO_ROOT.rglob(".DS_Store"))
    assert not finder_files, f"Unexpected .DS_Store files found: {finder_files}"


def test_gitignore_blocks_generated_outputs() -> None:
    gitignore = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8")
    assert "src/*.egg-info/" in gitignore
    assert "artifacts/**" in gitignore
    assert "tmp/**" in gitignore


def test_examples_use_bucketed_config_layout() -> None:
    assert (REPO_ROOT / "examples" / "configs" / "basic").exists()
    assert (REPO_ROOT / "examples" / "configs" / "reserve").exists()
    assert (REPO_ROOT / "examples" / "configs" / "canonical").exists()
    assert (REPO_ROOT / "examples" / "configs" / "custom").exists()


def test_public_examples_surface_is_curated() -> None:
    public_configs = sorted(
        str(path.relative_to(REPO_ROOT / "examples")) for path in (REPO_ROOT / "examples" / "configs").rglob("*.yaml")
    )
    assert public_configs == [
        "configs/basic/netherlands_da_only_base.yaml",
        "configs/canonical/belgium_full_stack.yaml",
        "configs/custom/belgium_full_stack_custom_python.yaml",
        "configs/reserve/belgium_da_plus_afrr_base.yaml",
    ]


def test_public_release_shell_files_exist() -> None:
    for path in (
        REPO_ROOT / "mkdocs.yml",
        REPO_ROOT / "Dockerfile",
        REPO_ROOT / "docker-compose.yml",
        REPO_ROOT / "schemas",
        REPO_ROOT / "CODEOWNERS",
        REPO_ROOT / ".github" / "workflows" / "release.yml",
        REPO_ROOT / ".github" / "workflows" / "docs.yml",
        REPO_ROOT / ".github" / "ISSUE_TEMPLATE" / "bug_report.yml",
        REPO_ROOT / ".github" / "PULL_REQUEST_TEMPLATE.md",
    ):
        assert path.exists(), f"Expected public release shell file to exist: {path}"


def test_readme_references_demo_gif_and_positioning_sections() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "docs/assets/canonical-belgium-demo.gif" in readme
    assert "## Install" in readme
    assert "python -m pip install euroflex-bess-lab" in readme
    assert "## Data Provenance & Sample Datasets" in readme
    assert "[Data provenance](docs/data_provenance.md)" in readme
    assert "## Who This Is For" in readme
    assert "## Why It Matters" in readme


def test_repo_tree_has_no_local_user_absolute_paths_in_text_files() -> None:
    offenders: list[str] = []
    local_user_prefix = "/Users/" + "bozliu/"
    ignored_dirs = {
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".ipynb_checkpoints",
        "build",
        "dist",
        "site",
        ".venv",
        "venv",
    }
    for path in REPO_ROOT.rglob("*"):
        if not path.is_file() or any(part in ignored_dirs for part in path.parts):
            continue
        if path.suffix in {".png", ".jpg", ".jpeg", ".parquet", ".sqlite3", ".pyc"}:
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if local_user_prefix in content:
            offenders.append(str(path.relative_to(REPO_ROOT)))
    assert not offenders, f"Local-user absolute paths found in text files: {offenders}"
