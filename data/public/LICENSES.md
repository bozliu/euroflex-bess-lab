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

## regelleistung.net — not redistributed

German reserve auction results from regelleistung.net (50Hertz, Amprion, TenneT TSO, TransnetBW) are NOT redistributed: the site's terms (https://www.regelleistung.net/de-de/Impressum, 'Nutzung der Internet-Seiten') require the TSOs' prior written consent for reproducing its content. Only the fetch code and a manifest stub (`redistributed: false`) are committed; build the files locally with `euroflex data fetch germany-reserve`. Manifest stubs:

- `germany/afrr_capacity.parquet`
- `germany/fcr_capacity.parquet`

## Code

The code that downloads and transforms these data is part of euroflex-bess-lab (Apache-2.0). The licences above
apply to the data only; no endorsement by the data providers is implied.
