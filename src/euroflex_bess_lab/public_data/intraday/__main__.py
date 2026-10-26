"""``python -m euroflex_bess_lab.public_data.intraday fetch --start 2024-01-01``."""

from __future__ import annotations

from .cli import intraday_app

if __name__ == "__main__":
    intraday_app()
