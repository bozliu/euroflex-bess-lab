"""Filesystem layout of the public-data layer.

``data_root()`` is the committed, redistributable tree (``data/public``); ``raw_root()`` is the gitignored raw
download cache (``data/raw``). Both can be overridden with environment variables so tests and notebooks can point
the loaders at fixture trees.
"""

from __future__ import annotations

import os
from pathlib import Path

ENV_DATA_DIR = "EUROFLEX_DATA_DIR"
ENV_RAW_DIR = "EUROFLEX_RAW_DIR"


def _search_upwards(start: Path, relative: str) -> Path | None:
    for parent in (start, *start.parents):
        candidate = parent / relative
        if candidate.is_dir():
            return candidate
    return None


def data_root() -> Path:
    """Return the root of the public data tree.

    Resolution order: ``$EUROFLEX_DATA_DIR``; the first ``data/public`` directory found walking up from the current
    working directory; the ``data/public`` directory of the source checkout this package was imported from;
    finally ``./data/public`` (which may not exist yet).
    """

    env = os.environ.get(ENV_DATA_DIR)
    if env:
        return Path(env).expanduser().resolve()
    found = _search_upwards(Path.cwd().resolve(), "data/public")
    if found is not None:
        return found
    source_checkout = _search_upwards(Path(__file__).resolve().parent, "data/public")
    if source_checkout is not None:
        return source_checkout
    return (Path.cwd() / "data" / "public").resolve()


def raw_root() -> Path:
    """Return the root of the raw download cache (``$EUROFLEX_RAW_DIR`` or the sibling ``raw`` of ``data_root()``)."""

    env = os.environ.get(ENV_RAW_DIR)
    if env:
        return Path(env).expanduser().resolve()
    return data_root().parent / "raw"


def relative_to_root(path: Path, root: Path) -> str:
    """Return ``path`` relative to ``root`` as a POSIX string (never an absolute local path)."""

    return path.resolve().relative_to(root.resolve()).as_posix()
