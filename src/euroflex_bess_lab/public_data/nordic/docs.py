"""Licence statements for the Nordic files, for ``data/public/LICENSES.md`` and ``docs/public_datasets.md``.

``render_nordic_licence_section(manifest)`` returns a markdown section listing the Nordic files per publisher; the
generic renderer in ``public_data.docs`` can append it (``render_licenses`` / ``render_docs_page``).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .sources import (
    ENERGINET_ATTRIBUTION,
    ENERGINET_LICENCE_URL,
    ENERGINET_TERMS_URL,
    SVK_ATTRIBUTION,
    SVK_LICENCE_URL,
)

ENERGINET_STATEMENT = (
    "Contains data from Energinet's Energi Data Service (https://www.energidataservice.dk), licensed under the "
    "Creative Commons Attribution 4.0 International licence (CC BY 4.0; terms: " + ENERGINET_TERMS_URL + "). "
    "The data were modified: filtered to the modelled bidding zones, renamed, type-cast, reshaped to one row per "
    "product and direction, and a documented DST labelling error was repaired, as recorded in each file's "
    "`.meta.json` sidecar."
)
SVK_STATEMENT = (
    "Contains data from Svenska kraftnät's SVK Data Service (https://data.svk.se), licensed under CC BY 4.0 "
    "(" + SVK_LICENCE_URL + "). The data were modified: filtered to the modelled bidding zones, renamed, "
    "type-cast and merged with the Danish tables as recorded in each file's `.meta.json` sidecar."
)


def _bullets(paths: list[str]) -> str:
    return "\n".join(f"- `{p}`" for p in sorted(paths)) if paths else "- (none)"


def render_nordic_licence_section(manifest: Mapping[str, Any], heading: str = "##") -> str:
    """Markdown section with the Energinet and SVK statements and the Nordic files each one covers."""

    files = [e for e in manifest.get("files", []) if str(e.get("path", "")).startswith("nordic/")]
    energinet = [str(e["path"]) for e in files if "Energinet" in str(e.get("attribution", ""))]
    svk = [str(e["path"]) for e in files if "Svenska" in str(e.get("attribution", ""))]
    return (
        f"{heading} Energinet (Energi Data Service), Denmark\n\n{ENERGINET_STATEMENT} Attribution: "
        f"**{ENERGINET_ATTRIBUTION}**. Licence: {ENERGINET_LICENCE_URL}. Files:\n\n{_bullets(energinet)}\n\n"
        f"{heading} Svenska kraftnät (SVK Data Service), Sweden\n\n{SVK_STATEMENT} Attribution: **{SVK_ATTRIBUTION}**. "
        f"Files:\n\n{_bullets(svk)}\n"
    )
