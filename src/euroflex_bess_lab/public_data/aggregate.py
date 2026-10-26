"""Aggregations of bid-level Elia datasets: capacity-auction awards and energy-bid merit-order curves."""

from __future__ import annotations

import re

import numpy as np
import pandas as pd

LOCAL_TZ = "Europe/Brussels"
BLOCK_HOURS = 4
_PERIOD_RE = re.compile(r"^\s*(\d{1,2})\s*-\s*(\d{1,2})\s*(?:-?\s*(G2))?\s*$", re.IGNORECASE)

CAPACITY_COLUMNS = [
    "delivery_start_utc",
    "delivery_end_utc",
    "direction",
    "awarded_mw",
    "vwap_eur_per_mw_h",
    "marginal_eur_per_mw_h",
    "n_bids",
    "awarded_mw_multi_block",
    "offered_mw",
    "n_offered_bids",
]

_COMMON_CAPACITY_NOTES = [
    "One row per delivery block (4 hours, Europe/Brussels local time, converted to UTC; 3/5 hours on DST-change "
    "days) and direction (up/down). Prices are capacity prices in EUR/MW/h (pay-as-bid).",
    "Bids whose delivery period spans several 4-hour blocks (e.g. '0 - 24' daily bids) deliver in every block "
    "they cover, so they are counted in each covered block; awarded_mw_multi_block shows that share.",
    "awarded_mw = sum of awarded MW; vwap_eur_per_mw_h = awarded-volume-weighted average bid price; "
    "marginal_eur_per_mw_h = highest awarded bid price; n_bids = number of bids with awarded MW > 0; "
    "offered_mw / n_offered_bids describe all offered bids (selected or not) delivering in the block; they are "
    "null where the source lists awarded bids only (ODS055, i.e. mFRR before 2026-05-20).",
    "Gate-2 bids ('G2' suffix in capacitybiddeliveryperiod) are treated like other bids of their period.",
]
CAPACITY_NOTES = {
    "afrr": [
        "Sources: ODS125 (individual aFRR capacity bids up to 19/05/2026, one row per bid with up and down "
        "columns) and ODS207 (as of 20/05/2026, one row per bid and regulation direction). Where both cover a "
        "delivery day, ODS207 wins.",
        *_COMMON_CAPACITY_NOTES,
    ],
    "mfrr": [
        "Sources: ODS055 (individual mFRR capacity bids up to 19/05/2026) and ODS206 (as of 20/05/2026). Only the "
        "standard mFRR product is used (mFRR Flex was discontinued on 22/05/2024; its rows are ignored). Elia "
        "procures mFRR capacity in the upward direction only. Where both cover a delivery day, ODS206 wins.",
        *_COMMON_CAPACITY_NOTES,
    ],
}


def parse_period(value: object) -> tuple[int, int] | None:
    """``"0 - 4"``, ``"20-24"``, ``"0 - 24 - G2"`` -> ``(start_hour, end_hour)``."""

    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    match = _PERIOD_RE.match(str(value))
    if match is None:
        return None
    h0, h1 = int(match.group(1)), int(match.group(2))
    if not 0 <= h0 < h1 <= 24:
        return None
    return h0, h1


def _local_dates(values: pd.Series, *, is_utc_midnight: bool) -> pd.Series:
    if is_utc_midnight:
        ts = pd.to_datetime(values, utc=True).dt.tz_convert(LOCAL_TZ)
        return pd.Series(pd.to_datetime(ts.dt.strftime("%Y-%m-%d")), index=values.index)
    return pd.Series(pd.to_datetime(values.astype(str).str[:10]), index=values.index)


def _bids_frame(
    local_date: pd.Series,
    period: pd.Series,
    gate2: pd.Series,
    direction: pd.Series,
    offered: pd.Series,
    awarded: pd.Series,
    price: pd.Series,
    source: str,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "local_date": local_date.to_numpy(),
            "period": period.astype("string").to_numpy(),
            "gate2": gate2.to_numpy(),
            "direction": direction.to_numpy(),
            "offered_mw": pd.to_numeric(offered, errors="coerce").to_numpy(),
            "awarded_mw": pd.to_numeric(awarded, errors="coerce").fillna(0.0).to_numpy(),
            "price": pd.to_numeric(price, errors="coerce").to_numpy(),
            "source": source,
        }
    )


def _pick_period(primary: pd.Series, fallback: pd.Series | None) -> tuple[pd.Series, pd.Series]:
    text = primary.astype("string")
    if fallback is not None:
        text = text.fillna(fallback.astype("string"))
    gate2 = text.str.contains("G2", case=False, na=False)
    return text.fillna(""), gate2


def afrr_bids_long(legacy: pd.DataFrame, current: pd.DataFrame) -> pd.DataFrame:
    """Bid-level aFRR capacity offers in a long layout (one row per bid and direction)."""

    frames: list[pd.DataFrame] = []
    if not legacy.empty:
        date = _local_dates(legacy["delivery_date"], is_utc_midnight=False)
        period, gate2 = _pick_period(legacy["capacitybiddeliveryperiod"], legacy.get("delivery_period"))
        for direction, suffix in (("up", "up"), ("down", "down")):
            offered = legacy[f"afrr_offered_volume_{suffix}"]
            mask = offered.notna()
            frames.append(
                _bids_frame(
                    date[mask],
                    period[mask],
                    gate2[mask],
                    pd.Series(direction, index=legacy.index[mask]),
                    offered[mask],
                    legacy.loc[mask, f"afrr_awarded_volume_{suffix}"],
                    legacy.loc[mask, f"price_{suffix}"],
                    "ods125",
                )
            )
    if not current.empty:
        date = _local_dates(current["deliverydate"], is_utc_midnight=True)
        period, gate2 = _pick_period(current["capacitybiddeliveryperiod"], None)
        direction = current["regulationdirection"].astype(str).str.lower().map({"up": "up", "down": "down"})
        frames.append(
            _bids_frame(
                date,
                period,
                gate2,
                direction,
                current["afrrofferedvolume"],
                current["afrrawardedvolumemw"],
                current["price"],
                "ods207",
            )
        )
    return _prefer_current(frames, "ods207")


def mfrr_bids_long(legacy: pd.DataFrame, current: pd.DataFrame) -> pd.DataFrame:
    """Bid-level mFRR (standard product) capacity offers in a long layout."""

    frames: list[pd.DataFrame] = []
    if not legacy.empty:
        date = _local_dates(legacy["delivery_date"], is_utc_midnight=False)
        period, gate2 = _pick_period(legacy["capacitybiddeliveryperiod"], None)
        direction = legacy["regulation_direction"].astype(str).str.lower().map({"up": "up", "down": "down"})
        frames.append(
            _bids_frame(
                date,
                period,
                gate2,
                direction,
                legacy["mfrr_offered_volume"],
                legacy["mfrr_std_awarded_volume"],
                legacy["mfrr_std_price"],
                "ods055",
            )
        )
    if not current.empty:
        date = _local_dates(current["deliverydate"], is_utc_midnight=True)
        period, gate2 = _pick_period(current["capacitybiddeliveryperiod"], None)
        direction = current["regulationdirection"].astype(str).str.lower().map({"up": "up", "down": "down"})
        frames.append(
            _bids_frame(
                date,
                period,
                gate2,
                direction,
                current["mfrrofferedvolume"],
                current["mfrrstdawardedvolume"],
                current["mfrrstdcapbidprice"],
                "ods206",
            )
        )
    return _prefer_current(frames, "ods206")


def _prefer_current(frames: list[pd.DataFrame], current_source: str) -> pd.DataFrame:
    usable = [f for f in frames if not f.empty]
    if not usable:
        return pd.DataFrame(
            columns=["local_date", "period", "gate2", "direction", "offered_mw", "awarded_mw", "price", "source"]
        )
    bids = pd.concat(usable, ignore_index=True)
    bids = bids.dropna(subset=["local_date", "direction"])
    current_days = set(bids.loc[bids["source"] == current_source, "local_date"])
    if current_days:
        drop = (bids["source"] != current_source) & bids["local_date"].isin(current_days)
        bids = bids.loc[~drop]
    return bids.reset_index(drop=True)


def expand_to_blocks(bids: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object]]:
    """Map every bid to the 4-hour blocks its delivery period covers (vectorised)."""

    periods = bids["period"].astype(str)
    unique = {p: parse_period(p) or (-1, -1) for p in periods.unique()}
    unparsed = sorted(p for p, v in unique.items() if v[0] < 0)
    h0 = periods.map(lambda p: unique[p][0]).astype(int)
    h1 = periods.map(lambda p: unique[p][1]).astype(int)
    valid = h0 >= 0
    work = bids.loc[valid].copy()
    work["h0"] = h0[valid].to_numpy()
    work["h1"] = h1[valid].to_numpy()
    work["n_blocks"] = np.maximum(1, (work["h1"] - work["h0"]) // BLOCK_HOURS)
    work["multi_block"] = work["n_blocks"] > 1
    repeated = work.loc[work.index.repeat(work["n_blocks"])].copy()
    offset = repeated.groupby(level=0).cumcount().to_numpy()
    block_len = np.where(repeated["n_blocks"].to_numpy() > 1, BLOCK_HOURS, repeated["h1"] - repeated["h0"])
    repeated["block_h0"] = repeated["h0"].to_numpy() + offset * BLOCK_HOURS
    repeated["block_h1"] = repeated["block_h0"].to_numpy() + block_len
    stats: dict[str, object] = {
        "bids_with_unparsed_period": int((~valid).sum()),
        "unparsed_period_values": len(unparsed),
        "multi_block_bids": int(work["multi_block"].sum()),
    }
    return repeated.reset_index(drop=True), stats


AWARDED_ONLY_SOURCES = frozenset({"ods055"})  # ODS055 lists awarded mFRR bids only (no unselected offers)


def aggregate_capacity_awards(bids: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object]]:
    """Aggregate long-format bids to one row per (delivery block, direction)."""

    if bids.empty:
        return pd.DataFrame(columns=CAPACITY_COLUMNS), {"bids": 0}
    blocks, stats = expand_to_blocks(bids)
    blocks["awarded"] = blocks["awarded_mw"] > 0
    blocks["awarded_value"] = blocks["awarded_mw"] * blocks["price"]
    blocks["awarded_multi"] = np.where(blocks["multi_block"], blocks["awarded_mw"], 0.0)
    blocks["awarded_price"] = blocks["price"].where(blocks["awarded"])
    blocks["awarded_only_source"] = blocks["source"].isin(AWARDED_ONLY_SOURCES)
    keys = ["local_date", "block_h0", "block_h1", "direction"]
    grouped = blocks.groupby(keys, sort=True)
    out = pd.DataFrame(
        {
            "awarded_mw": grouped["awarded_mw"].sum(),
            "awarded_value": grouped["awarded_value"].sum(),
            "marginal_eur_per_mw_h": grouped["awarded_price"].max(),
            "n_bids": grouped["awarded"].sum(),
            "awarded_mw_multi_block": grouped["awarded_multi"].sum(),
            "offered_mw": grouped["offered_mw"].sum(min_count=1),
            "n_offered_bids": grouped.size(),
            "awarded_only_source": grouped["awarded_only_source"].any(),
        }
    ).reset_index()
    # Offer statistics are unknown where the source lists awarded bids only.
    out.loc[out["awarded_only_source"], "offered_mw"] = np.nan
    out["vwap_eur_per_mw_h"] = np.where(
        out["awarded_mw"] > 0, out["awarded_value"] / out["awarded_mw"].where(out["awarded_mw"] > 0), np.nan
    )
    day = pd.to_datetime(out["local_date"])
    start_local = (day + pd.to_timedelta(out["block_h0"], unit="h")).dt.tz_localize(
        LOCAL_TZ, ambiguous="NaT", nonexistent="shift_forward"
    )
    end_local = (day + pd.to_timedelta(out["block_h1"], unit="h")).dt.tz_localize(
        LOCAL_TZ, ambiguous="NaT", nonexistent="shift_forward"
    )
    out["delivery_start_utc"] = start_local.dt.tz_convert("UTC").astype("datetime64[ns, UTC]")
    out["delivery_end_utc"] = end_local.dt.tz_convert("UTC").astype("datetime64[ns, UTC]")
    out = out.dropna(subset=["delivery_start_utc", "delivery_end_utc"])
    for column in ("awarded_mw", "vwap_eur_per_mw_h", "marginal_eur_per_mw_h", "awarded_mw_multi_block", "offered_mw"):
        out[column] = out[column].astype("float32")
    out["n_bids"] = out["n_bids"].astype("int32")
    out["n_offered_bids"] = out["n_offered_bids"].astype("Int32").mask(out["awarded_only_source"])
    out["direction"] = out["direction"].astype("string")
    out = out.sort_values(["delivery_start_utc", "direction"]).reset_index(drop=True)[CAPACITY_COLUMNS]
    stats.update(
        {
            "bids": int(len(bids)),
            "bid_sources": {str(k): int(v) for k, v in bids["source"].value_counts().items()},
            "delivery_days": int(bids["local_date"].nunique()),
            "gate2_bids": int(bids["gate2"].sum()),
            "blocks_without_awards": int((out["awarded_mw"] <= 0).sum()),
            "period_values": sorted({str(p) for p in bids["period"].unique()})[:40],
        }
    )
    return out, stats


# --- energy bid merit-order curves -------------------------------------------------------------------------------

CURVE_STEP_MW = 25
CURVE_MAX_MW = 2000
CURVE_GRID = np.arange(0, CURVE_MAX_MW + CURVE_STEP_MW, CURVE_STEP_MW, dtype=np.float64)
CURVE_COLUMNS = ["timestamp_utc", "direction", "cum_mw", "price_eur_mwh"]
CURVE_NOTES = [
    "Built from ODS156 (incremental = direction up) and ODS157 (decremental = direction down) individual energy "
    "bids; only bids with availability == 'Available' are used. Raw bid rows are not redistributed.",
    "Bid volumes are the increments of Elia's published cumulativevolume in its merit-order rank, so bids that add "
    "no volume (mutually exclusive alternatives, unavailable bids) are excluded.",
    "Per quarter-hour and direction the available aFRR + mFRR (+ reserve-sharing) bids are sorted by price into an "
    "economic merit order: cheapest first for up (incremental), highest price first for down (decremental). "
    "price_eur_mwh at cum_mw = V is the price of the bid that covers the V-th MW, i.e. the marginal price needed "
    "to activate V MW. Null where the available volume is below V. Grid: 0..2000 MW in 25 MW steps.",
    "Elia's own rank (aFRR first, then mFRR) was tested as an alternative ordering and tracks the published "
    "MIP/MDP much worse (see validation.curve_vs_published_prices for the price-sorted curve).",
    "Approximation: real aFRR activations are continuous and priced via PICASSO cross-border marginal prices, mFRR "
    "via MARI, and Elia may skip bids (e.g. for congestion); treat the curve as a stylised local merit order for "
    "price-impact studies, not as the settlement price.",
]


def bid_curves(raw: pd.DataFrame, direction: str, grid: np.ndarray = CURVE_GRID) -> tuple[pd.DataFrame, dict[str, int]]:
    """Price-sorted merit-order curves on ``grid`` for every quarter-hour in ``raw`` (one ODS156/ODS157 chunk)."""

    stats = {"rows": int(len(raw))}
    if raw.empty:
        return pd.DataFrame(columns=CURVE_COLUMNS), stats
    available = raw["availability"].astype(str).str.lower() == "available" if "availability" in raw else True
    work = raw.loc[available, ["datetime", "rank", "cumulativevolume", "energybidmarginalprice"]].copy()
    stats["rows_not_available"] = int(len(raw) - len(work))
    work["ts"] = pd.to_datetime(work["datetime"], utc=True)
    work["rank_i"] = pd.to_numeric(work["rank"], errors="coerce")
    work["cum"] = pd.to_numeric(work["cumulativevolume"], errors="coerce")
    work["price"] = pd.to_numeric(work["energybidmarginalprice"], errors="coerce")
    work = work.dropna(subset=["ts", "rank_i", "cum", "price"])
    work = work.sort_values(["ts", "rank_i"], kind="stable")
    work = work.drop_duplicates(["ts", "rank_i"], keep="first")
    if direction not in {"up", "down"}:
        raise ValueError("direction must be 'up' or 'down'")
    work["cum"] = work.groupby("ts", sort=True)["cum"].cummax()
    work["volume"] = work.groupby("ts", sort=True)["cum"].diff().fillna(work["cum"]).clip(lower=0.0)
    stats["rows_without_volume"] = int((work["volume"] <= 0).sum())
    work = work[work["volume"] > 0]
    work = work.sort_values(["ts", "price", "rank_i"], ascending=[True, direction == "up", True], kind="stable")
    work["cum"] = work.groupby("ts", sort=True)["volume"].cumsum()
    work["envelope"] = work["price"]
    ts_values = work["ts"].to_numpy()
    cum = work["cum"].to_numpy(dtype=np.float64)
    env = work["envelope"].to_numpy(dtype=np.float64)
    boundaries = np.flatnonzero(np.r_[True, ts_values[1:] != ts_values[:-1], True])
    n_qh = len(boundaries) - 1
    prices = np.full((n_qh, len(grid)), np.nan, dtype=np.float64)
    for i in range(n_qh):
        lo, hi = boundaries[i], boundaries[i + 1]
        idx = np.searchsorted(cum[lo:hi], grid, side="left")
        ok = idx < (hi - lo)
        prices[i, ok] = env[lo:hi][idx[ok]]
    stamps = ts_values[boundaries[:-1]]
    frame = pd.DataFrame(
        {
            "timestamp_utc": np.repeat(stamps, len(grid)),
            "direction": direction,
            "cum_mw": np.tile(grid, n_qh).astype("float32"),
            "price_eur_mwh": prices.reshape(-1).astype("float32"),
        }
    )
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True).astype("datetime64[ns, UTC]")
    frame["direction"] = frame["direction"].astype("string")
    stats["quarter_hours"] = int(n_qh)
    return frame, stats
