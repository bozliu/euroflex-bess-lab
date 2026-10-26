# Nordic markets: Denmark (DK1, DK2), Sweden (SE3, SE4) and Finland (FI)

The Nordic module adds the battery markets of Denmark, Sweden and Finland on real public data: day-ahead
arbitrage co-optimised with FCR-N, FCR-D up/down, DK1 FCR, aFRR and mFRR capacity, plus the Danish imbalance
price for context. It has three parts:

- `euroflex_bess_lab.public_data.nordic`: fetch, normalise and load the public tables in `data/public/nordic`.
- `euroflex_bess_lab.markets.nordic`: the market rules (`rules.py`), a one-day LP (`lp.py`) and a walk-forward
  backtest (`backtest.py`).
- `scripts/research/nordic_fetch.py` and `scripts/research/nordic_backtest.py`, which regenerate the data and
  [reports/nordics.md](https://github.com/bozliu/euroflex-bess-lab/blob/main/reports/nordics.md) (revenue stacks
  per zone, capture vs a perfect-foresight oracle, cycles per day, per year and for the latest 12 months).

```bash
.venv/bin/python scripts/research/nordic_fetch.py --as-of 2026-10-26   # data/public/nordic (6.6 MB)
.venv/bin/python scripts/research/nordic_backtest.py                   # reports/nordics.md, about 1-4 minutes
```

## Data sources and licences

Every file is downloaded without an account or API key and is published under Creative Commons Attribution 4.0,
so the normalised tables are committed (`redistributed: true` in every sidecar).

| Source | Licence and attribution | Used for |
| --- | --- | --- |
| Energinet, [Energi Data Service](https://www.energidataservice.dk/) | CC BY 4.0 per the [terms and conditions](https://www.energidataservice.dk/terms-and-conditions) (copying, changing and redistributing allowed, also commercially); credit "Source: Energinet (www.energidataservice.dk)" | day-ahead DK1, DK2, SE3, SE4; FCR-N/FCR-D (joint SE-DK2 auctions, all Swedish zones included); DK1 FCR; Nordic aFRR capacity (incl. FI); Danish mFRR capacity; Danish imbalance prices |
| Svenska kraftnät, [SVK Data Service](https://data.svk.se/) | CC BY 4.0 (CKAN `license_id`); credit "Svenska kraftnät (SVK Data Service)" | Swedish mFRR capacity; Finnish day-ahead prices (and an independent SE3/SE4 cross-check) |

Checked and not used:

- **Fingrid open data** (data.fingrid.fi, CC BY 4.0) needs an API key or a signed-in session for every download
  (the dataset pages send a bearer token; without it the server answers `noToken`). Finnish FCR-N, FCR-D and mFRR
  capacity prices are therefore not in the committed data, and the Finnish stack is day-ahead plus aFRR.
- **Energy-Charts** day-ahead prices for SE3 and FI carry a "private and internal use only" notice (DK1, DK2 and
  SE4 are CC BY 4.0 via SMARD.de); the Energinet copies are used instead.

Rate limits: Energi Data Service limits requests per dataset (HTTP 429 with a `Retry-After` of a few minutes;
repeated excess can block the IP). The fetcher makes one request per calendar year for active datasets and one
for discontinued ones, filters zones and columns server-side and honours `Retry-After`. A cold fetch takes
about 6-8 minutes; cached rebuilds take seconds. Raw downloads are cached under `data/raw/nordic` (gitignored) with
a `.source.json` provenance record each.

### Tables (`data/public/nordic`)

| File | Content | Coverage |
| --- | --- | --- |
| `da_prices_<ZONE>.parquet` (DK1, DK2, SE3, SE4) | `timestamp_utc`, `resolution_minutes` (60 until 2025-09-30, then 15), `price_eur_mwh` | 2023-12-01 to the snapshot, complete grids |
| `da_prices_FI.parquet` | same columns, from SVK Data Service | to 2026-07-01 (the dataset stopped updating); about 2 % of hours and 3 % of quarter-hours missing |
| `fcr_nordic.parquet` | FCR-N, FCR-D up, FCR-D down per hour, zone (DK2, SE3, SE4) and auction (`early` = D-1 early, `late`, `total`); `price_eur_per_mw_h`, `volume_local_mw`, `volume_total_mw` | marginal prices from 2024-02-01 (average prices before) |
| `fcr_dk1.parquet` | DK1 FCR (Continental Europe cooperation): `price_eur_per_mw_h` (Danish price), `cross_border_price_eur_per_mw_h`, volumes | hourly rows, constant within 4-hour products |
| `afrr_capacity.parquet` | aFRR up/down marginal prices, procured MW and demand per zone and hour | DK2, FI, SE3, SE4 from 2023-12; DK1 from 2024-10-02 |
| `mfrr_capacity.parquet` | mFRR up/down marginal prices and procured MW; DK1/DK2 (Energinet daily auctions) and SE3/SE4 (SVK; only hours with Swedish procurement) | 2023-12 to the snapshot |
| `dk_imbalance.parquet` | DK1/DK2 imbalance price, day-ahead price, dominating direction, aFRR VWA and mFRR marginal prices | hourly before 2025-03-04, 15-minute afterwards |

Validation recorded in the sidecars: complete hourly and 15-minute day-ahead grids for DK1/DK2/SE3/SE4;
Energinet and SVK agree on SE3/SE4 prices to 0.01 EUR/MWh except on 2024-10-15, where SVK repeats rows (Energinet
matches Energy-Charts/SMARD; the Finnish day is dropped because it comes from the same SVK file); FCR prices are
identical in DK2, SE3 and SE4 (one joint price); the documented FcrNdDK2 timestamp error at the end of DST 2024 is
repaired; the Danish imbalance price follows the published max/min rule in 99.998 % of quarter-hours from
2025-03-20.

### Loaders

```python
from euroflex_bess_lab.public_data.nordic import load_dk_imbalance, load_nordic_day_ahead, load_nordic_reserve

da = load_nordic_day_ahead("SE3", "2025-06-10", "2025-06-12", quarter_hourly=True)
fcr_d_up = load_nordic_reserve("fcr_d_up", "SE3", "2025-06-10", "2025-06-12")  # auction="early"
afrr_down = load_nordic_reserve("afrr", "FI", "2025-06-10", "2025-06-12", direction="down")
fcr_dk1 = load_nordic_reserve("fcr", "DK1", "2025-06-10", "2025-06-12")
imbalance = load_dk_imbalance("DK2", "2025-06-10", "2025-06-12")
```

Bounds are half-open `[start, end)`, naive values are UTC, timestamps come back tz-aware UTC, and a missing file
raises `FileNotFoundError` with the fetch command that builds it. The root follows `EUROFLEX_DATA_DIR` like the
other public tables.

## Market rules encoded

### Products, power and energy reservations

Per MW of capacity sold (`C`), measured at the grid connection; when several products are sold in the same hour
the reservations add up, as the Nordic FCR requirements prescribe for FCR products.

| Product | Zones | Discharge power | Charge power | Stored energy kept | Product length | Source |
| --- | --- | --- | --- | --- | --- | --- |
| FCR-N (symmetric) | DK2, SE3, SE4 | 1.34 C | 1.34 C | 1 h x C up and down | 1 h | Nordic FCR technical requirements, section 3.5, Table 10 (LER) |
| FCR-D up | DK2, SE3, SE4 | 1.00 C | 0.20 C | 1/3 h x C up | 1 h | same; SvK: endurance at least 20 minutes |
| FCR-D down | DK2, SE3, SE4 | 0.20 C | 1.00 C | 1/3 h x C down | 1 h | same |
| FCR (Continental Europe) | DK1 | 1.25 C | 1.25 C | 0.5 h x C up and down | 4 h | Energinet prequalification (Nov 2025), section 10.1.1, Table 36 |
| aFRR up / down | DK1, DK2, SE3, SE4, FI | 1.00 C in its direction | (DK: +1.00 C opposite) | 1 h x C in its direction | 1 h | SvK aFRR endurance 1 h; Energinet LER rules, section 10.1.4 |
| mFRR up / down | DK1, DK2, SE3, SE4 | 1.00 C in its direction | (DK: +1.00 C opposite) | 1 h x C in its direction | 1 h | SvK: an award obliges energy bids in all four quarters of the hour |

Notes. The 0.34 x C (FCR-N) and 0.20 x C (FCR-D) extra power is the normal-state energy management (NEM) recharge
headroom. Energy reservations are converted to stored energy with the efficiencies (delivering `E` needs `E / 0.95`
in the battery, absorbing `E` adds `0.95 E`). Energinet's LER rules for aFRR/mFRR capacity require the sold
capacity to be deliverable in capacity and energy, forbid energy management through imbalances and describe an
intraday-based energy management that keeps the sold MW available in the opposite direction; the backtest
applies that reservation in DK1 and DK2 and shows a sensitivity without it (Energinet notes these rules are still
being refined). The TSOs plan to re-evaluate the FCR-N energy requirement within 1-1.5 h per direction (also a
sensitivity).

Sources:
[Nordic FCR technical requirements (14.5.2025)](https://www.fingrid.fi/globalassets/dokumentit/fi/sahkomarkkinat/reservit/technical-requirements-for-frequency-containment-reserve-provision-in-the-nordic-synchronous-area_14.5.2025.pdf),
[Energinet prequalification (November 2025)](https://en.energinet.dk/media/fm1pgsez/prequalification-of-units-and-agggegated-portfolios-november-2025.pdf),
[Energinet product overview DK1/DK2](https://en.energinet.dk/media/z4ilcceo/product-overview-dk1-and-dk2.pdf),
SvK product pages for [FCR-N](https://www.svk.se/aktorsportalen/bidra-med-reserver/om-olika-reserver/fcr-n/),
[FCR-D up](https://www.svk.se/aktorsportalen/bidra-med-reserver/om-olika-reserver/fcr-d-upp/),
[aFRR](https://www.svk.se/aktorsportalen/bidra-med-reserver/om-olika-reserver/afrr/) and
[mFRR](https://www.svk.se/aktorsportalen/bidra-med-reserver/om-olika-reserver/mfrr/).

### Auction timing and pricing

| Market | Zones | Gate closure | Pricing |
| --- | --- | --- | --- |
| FCR-N, FCR-D up/down, auction 1 ("D-1 early", former D-2) | SE1-SE4 and DK2 (joint) | D-1 00:30 | marginal (pay-as-cleared) per auction since 2024-02-01 |
| FCR-N, FCR-D, auction 2 ("D-1 late") | same | D-1 18:00 | marginal (not used; auction 1 procures most volume) |
| aFRR capacity (Nordic aFRR CM; DK1 local market from 2024-10-02) | DK1, DK2, SE, FI | D-1 07:30 CET | marginal |
| mFRR capacity (trilateral DK-SE-FI market from 2024-11-19) | DK1, DK2, SE, FI | D-1 07:30 | marginal |
| FCR (Continental Europe cooperation, 4-hour products) | DK1 | D-1 08:00 | marginal |
| Day-ahead (SDAC) | all | D-1 12:00 CET | zonal marginal price; 15-minute products from delivery day 2025-10-01 |

Sources: SvK product pages above,
[Nordic aFRR capacity market handbook](https://nordicbalancingmodel.net/wp-content/uploads/2022/08/Nordic-Handbook-aFRR-Capacity-Market.pdf),
[Energinet aFRR capacity market](https://en.energinet.dk/electricity/balancing-and-ancillary-services/market-overview-and-tender/afrr-capacity-market/),
Energinet dataset descriptions on Energi Data Service.

### Danish imbalance settlement and why implicit balancing is not modelled

Denmark settles imbalances at a single price per bidding zone and 15-minute period. Since March 2025 the price is
the higher of the mFRR marginal price and the aFRR volume-weighted average price when the dominating direction is
up, the lower of the two when it is down, and the day-ahead price without a dominating direction
([Energinet, imbalance price design](https://en.energinet.dk/electricity/balancing-and-ancillary-services/imbalance-price-design/)).

Deliberate implicit (passive) balancing, a core strategy in the Belgian module, is not modelled for Denmark:

- the Nordic TSOs expect balance responsible parties to balance their portfolios before the operating hour and to
  follow their submitted plans, monitor large or systematic deviations, and do not publish intra-period prices in
  order to avoid self-balancing
  ([Nordic Balancing Philosophy, May 2024](https://www.fingrid.fi/globalassets/dokumentit/fi/tiedotteet/ajankohtaista/nordic-balancing-philosophy-may24.pdf));
- Danish BRPs submit 5-minute operational schedules and are financially responsible for being in balance
  ([Energinet regulation C1, in Danish](https://energinet.dk/media/d45j2s2m/forskrift-c1.pdf));
- Energinet does not accept battery energy management through imbalances for aFRR/mFRR providers
  ([prequalification, section 10.1.4](https://en.energinet.dk/media/fm1pgsez/prequalification-of-units-and-agggegated-portfolios-november-2025.pdf)).

The imbalance table is used for descriptive statistics only (`reports/nordics.md`).

## Backtest model

- **Battery**: 1 MW / 2 MWh, SOC 5-95 %, start and end each delivery day (CET/CEST) at 50 %, efficiencies
  0.95 / 0.95, degradation 8 EUR per MWh discharged (`markets.nordic.backtest.NORDIC_BATTERY`, a
  `realtime.BatteryParams`).
- **LP** (`markets.nordic.lp.solve_day`, HiGHS through SciPy): quarter-hour charge/discharge/SOC plus hourly (DK1
  FCR: 4-hour block) capacity variables; power headroom in both directions, converter limit `c + d <= P`, stored
  energy above the SOC floor and below the ceiling at the end of every quarter-hour and the start of every hour,
  hourly-constant day-ahead position before 2025-10-01, terminal SOC. With the terminal SOC and converter limit
  dropped it reproduces `realtime.strategies.solve_qh_arbitrage` exactly (checked in the report and the tests).
- **Decision timing and forecast**: each day is decided once at the earliest gate closure of its products
  (D-1 00:30 or 07:30). At every gate the newest prices are those of D-1 (D-1 day-ahead prices appear on D-2 around
  13:00, D-1 capacity results on D-2), so the persistence forecast (D-1 at the same local clock time, D-2..D-7 for
  missing values) is the same at all gates and later re-optimisation would not change the schedule. A
  perturbation test confirms that prices from day D onwards never change D's forecast.
- **Settlement**: pay-as-cleared capacity, so offers are zero-price bids paid the realised marginal price;
  day-ahead schedules are price-independent quantity bids settled at realised prices. The oracle solves the same
  LP on realised prices.
- **Workflows**: `da_only`, `da_fcr`, `da_afrr`, `da_mfrr`, `full_stack`; modes `persistence` and `oracle`.
- **Outputs**: `reports/ledgers/nordic_daily.parquet` (all zones, workflows and modes per day),
  `reports/ledgers/nordic_qh_<ZONE>_full_stack.parquet` (quarter-hour ledgers in the valuation contract, with a
  `LedgerMetadata`-compatible `.json` sidecar) and `reports/ledgers/nordic_summary.json`.

```python
import pandas as pd
from euroflex_bess_lab.markets.nordic.backtest import ZoneMarket, annualise, run_zone

start = pd.Timestamp("2025-06-01", tz="Europe/Copenhagen").tz_convert("UTC")
end = pd.Timestamp("2025-07-01", tz="Europe/Copenhagen").tz_convert("UTC")
market = ZoneMarket.from_public("SE3", start, end)
result = run_zone(market, start, end, workflows=["da_only", "full_stack"])
full = result.daily.query("workflow == 'full_stack' and mode == 'persistence'")
print(annualise(full)["net_eur"])  # EUR per MW and year
```

## Scope and limitations

- Simulated revenue on public prices: a price taker whose zero-price capacity offers are always accepted, results
  per MW of a pooled fleet (minimum bids of 0.1 MW for FCR and 1 MW for aFRR/mFRR met by pooling). Several of these
  markets procure only tens of MW per zone and hour (median depth is listed in the report), so the per-MW results
  apply to a small participant.
- No intraday trading, no activation energy (aFRR/mFRR energy markets, FCR-N energy remuneration), no FFR; the
  energy reservations stand in for the state-of-charge effect of activations.
- SE/DK2 FCR is offered in auction 1 only; Finland has no FCR or mFRR (no keyless source) and day-ahead prices end
  on 2026-07-01 with gaps (untradable quarter-hours, days with more than two missing hours skipped).
- Linear battery model with a daily 50 % boundary; no grid tariffs, fees or taxes.
