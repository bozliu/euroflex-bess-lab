"""Download (or reuse the raw cache) and build the Nordic public tables under ``data/public/nordic``.

    .venv/bin/python scripts/research/nordic_fetch.py                    # all tables, snapshot = today
    .venv/bin/python scripts/research/nordic_fetch.py --as-of 2026-10-26 # data end at Copenhagen midnight
    .venv/bin/python scripts/research/nordic_fetch.py --tables afrr,mfrr --offline

Sources (CC BY 4.0, no account or API key): Energinet's Energi Data Service (day-ahead DK1/DK2/SE3/SE4, FCR-N and
FCR-D of the joint SE-DK2 auctions, DK1 FCR, Nordic aFRR capacity, Danish mFRR capacity, Danish imbalance prices)
and Svenska kraftnät's SVK Data Service (Swedish mFRR capacity, Finnish day-ahead). Raw downloads are cached under
``data/raw/nordic`` (gitignored). Energi Data Service rate-limits per dataset (one request every ~2 minutes for
some datasets), so a cold run takes about 6-8 minutes; cached runs take seconds. Afterwards regenerate the
manifest with ``euroflex data manifest``.
"""

from __future__ import annotations

import sys

from euroflex_bess_lab.public_data.nordic.cli import nordic_data_app


def main() -> None:
    nordic_data_app(["fetch", *sys.argv[1:]], standalone_mode=True)


if __name__ == "__main__":
    main()
