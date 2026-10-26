"""Quarter-hourly intraday price index for DE-LU: the German TSOs' ID AEP from netztransparenz.de.

The index is downloaded through the page's public CSV download (no account) and kept local: the site terms
require prior written consent for reproduction, so ``data/public/intraday/`` holds only a sidecar stub with
``redistributed: false``. Build the local table with::

    python -m euroflex_bess_lab.public_data.intraday fetch --start 2024-01-01

then load it with :func:`load_intraday_index`. See ``docs/intraday.md``.
"""

from __future__ import annotations

from .build import FETCH_COMMAND, ID_AEP_RELPATH, ID_AEP_TABLE, build_id_aep
from .loaders import intraday_index_path, load_intraday_index
from .netztransparenz import (
    ID_AEP_ATTRIBUTION,
    ID_AEP_COLUMN,
    ID_AEP_LICENCE,
    ID_AEP_PAGE_URL,
    ID_AEP_TERMS_URL,
    encode_request,
    parse_id_aep_csv,
)

__all__ = [
    "FETCH_COMMAND",
    "ID_AEP_ATTRIBUTION",
    "ID_AEP_COLUMN",
    "ID_AEP_LICENCE",
    "ID_AEP_PAGE_URL",
    "ID_AEP_RELPATH",
    "ID_AEP_TABLE",
    "ID_AEP_TERMS_URL",
    "build_id_aep",
    "encode_request",
    "intraday_index_path",
    "load_intraday_index",
    "parse_id_aep_csv",
]
