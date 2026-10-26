# Licences and attribution for `data/public`

## Elia Open Data — CC BY 4.0

Contains data from Elia Open Data (https://opendata.elia.be), (c) Elia Transmission Belgium SA, licensed under the Creative Commons Attribution 4.0 International licence (CC BY 4.0). The data were modified: fields were renamed, merged, type-cast, re-gridded and/or aggregated as documented in each file's `.meta.json` sidecar.

Attribution: **Elia Transmission Belgium SA**. Licence: https://creativecommons.org/licenses/by/4.0/ (Elia's licence page:
https://opendata.elia.be/pages/licence/). Files:

- `belgium/afrr_capacity_awards.parquet`
- `belgium/energy_bid_curves_qh_2025.parquet`
- `belgium/energy_bid_curves_qh_2026.parquet`
- `belgium/mfrr_capacity_awards.parquet`
- `belgium/minute_2023_legacy.parquet`
- `belgium/minute_2024.parquet`
- `belgium/minute_2024_legacy.parquet`
- `belgium/minute_2025.parquet`
- `belgium/minute_2026.parquet`
- `belgium/new_formula_minute_2026.parquet`
- `belgium/new_formula_qh_2026.parquet`
- `belgium/qh_2019_legacy.parquet`
- `belgium/qh_2020_legacy.parquet`
- `belgium/qh_2021_legacy.parquet`
- `belgium/qh_2022_legacy.parquet`
- `belgium/qh_2023_legacy.parquet`
- `belgium/qh_2024.parquet`
- `belgium/qh_2024_legacy.parquet`
- `belgium/qh_2025.parquet`
- `belgium/qh_2026.parquet`
- `belgium/res_load_qh_2023.parquet`
- `belgium/res_load_qh_2024.parquet`
- `belgium/res_load_qh_2025.parquet`
- `belgium/res_load_qh_2026.parquet`
- `belgium/si_forecast_current_qh_2024.parquet`
- `belgium/si_forecast_current_qh_2025.parquet`
- `belgium/si_forecast_current_qh_2026.parquet`
- `belgium/si_forecast_next_qh_2024.parquet`
- `belgium/si_forecast_next_qh_2025.parquet`
- `belgium/si_forecast_next_qh_2026.parquet`

## Energy-Charts day-ahead prices — CC BY 4.0

Day-ahead prices: Bundesnetzagentur | SMARD.de, retrieved via the Energy-Charts API of Fraunhofer ISE (https://api.energy-charts.info), licensed under CC BY 4.0; de-duplicated and annotated with the product resolution. Attribution: **Bundesnetzagentur | SMARD.de, via Energy-Charts (Fraunhofer ISE)**. Files:

- `energy_charts/da_prices_BE.parquet`
- `energy_charts/da_prices_DE-LU.parquet`

## Data that is not redistributed (regelleistung.net, netztransparenz.de)

German reserve auction results from regelleistung.net (50Hertz, Amprion, TenneT TSO, TransnetBW) are NOT redistributed: the site's terms (https://www.regelleistung.net/de-de/Impressum, 'Nutzung der Internet-Seiten') require the TSOs' prior written consent for reproducing its content. Only the fetch code and a manifest stub (`redistributed: false`) are committed; build the files locally with `euroflex data fetch germany-reserve`.

The German intraday index ID AEP (netztransparenz.de, published by the four German TSOs) is NOT redistributed: the site's imprint requires the TSOs' prior written consent for reproducing its content. Only the fetch code, a manifest stub (`redistributed: false`) and derived daily backtest results are committed; build the index locally with `euroflex data intraday fetch`.

Manifest stubs:

- `germany/afrr_capacity.parquet`
- `germany/fcr_capacity.parquet`
- `intraday/id_aep_DE-LU.parquet`

## Energinet (Energi Data Service), Denmark

Contains data from Energinet's Energi Data Service (https://www.energidataservice.dk), licensed under the Creative Commons Attribution 4.0 International licence (CC BY 4.0; terms: https://www.energidataservice.dk/terms-and-conditions). The data were modified: filtered to the modelled bidding zones, renamed, type-cast, reshaped to one row per product and direction, and a documented DST labelling error was repaired, as recorded in each file's `.meta.json` sidecar. Attribution: **Source: Energinet (www.energidataservice.dk)**. Licence: https://creativecommons.org/licenses/by/4.0/. Files:

- `nordic/afrr_capacity.parquet`
- `nordic/da_prices_DK1.parquet`
- `nordic/da_prices_DK2.parquet`
- `nordic/da_prices_SE3.parquet`
- `nordic/da_prices_SE4.parquet`
- `nordic/dk_imbalance.parquet`
- `nordic/fcr_dk1.parquet`
- `nordic/fcr_nordic.parquet`
- `nordic/mfrr_capacity.parquet`

## Svenska kraftnät (SVK Data Service), Sweden

Contains data from Svenska kraftnät's SVK Data Service (https://data.svk.se), licensed under CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). The data were modified: filtered to the modelled bidding zones, renamed, type-cast and merged with the Danish tables as recorded in each file's `.meta.json` sidecar. Attribution: **Svenska kraftnät (SVK Data Service, data.svk.se), produced by Svk business units**. Files:

- `nordic/da_prices_FI.parquet`
- `nordic/mfrr_capacity.parquet`


## Great Britain (Elexon BMRS, NESO, ECB)

Great Britain: Contains BMRS data © Elexon Limited copyright and database right 2023-2026 (Elexon BMRS open data licence: https://www.elexon.co.uk/bsc/data/balancing-mechanism-reporting-agent/copyright-licence-bmrs-data/). Supported by National Energy SO Open Data (NESO Open Data Licence v1.0: https://www.neso.energy/data-portal/neso-open-licence). FX rates: Source: European Central Bank (euro foreign exchange reference rates) (https://www.ecb.europa.eu/services/disclaimer/html/index.en.html); daily rates are joined onto delivery dates. Files:

- `gb/ecb_gbp_per_eur.parquet`
- `gb/frequency_15s_2023.parquet`
- `gb/frequency_15s_2024.parquet`
- `gb/frequency_15s_2025.parquet`
- `gb/frequency_15s_2026.parquet`
- `gb/market_index.parquet`
- `gb/reserve_auctions.parquet`
- `gb/response_auctions.parquet`
- `gb/response_energy.parquet`
- `gb/system_prices.parquet`

## Code

The code that downloads and transforms these data is part of euroflex-bess-lab (Apache-2.0). The licences above
apply to the data only; no endorsement by the data providers is implied.
