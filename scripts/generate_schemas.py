from __future__ import annotations

from pathlib import Path

from euroflex_bess_lab.contracts import write_json_schemas


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    write_json_schemas(repo_root / "schemas")


if __name__ == "__main__":
    main()
