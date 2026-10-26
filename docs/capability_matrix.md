# Capability Matrix

This page is code-derived and kept under test so the published matrix stays aligned with the actual support declarations.

## Narrow GA promise

| Market | Scope | Workflow | Base workflow | Forecast paths | Operator path | Tier | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `belgium` | portfolio / shared POI | `schedule_revision` | `da_plus_afrr` | `persistence, csv` | `validate-config -> validate-data -> backtest -> reconcile -> export-schedule --profile operator -> export-bids --profile bid_planning` | ga | The first explicitly strong GA promise for euroflex_bess_lab. |

## Stable secondary workflow surface

| Workflow | Market | Single asset | Portfolio | Tier | Notes |
| --- | --- | --- | --- | --- | --- |
| `da_only` | `belgium` | Yes | Yes | stable | Secondary stable surface for energy-only planning. |
| `da_only` | `netherlands` | Yes | Yes | stable | Supported secondary surface; not part of the narrow GA promise. |
| `da_plus_fcr` | `belgium` | Yes | Yes | stable | Capacity-first symmetric reserve benchmark. |
| `da_plus_fcr` | `netherlands` | Yes | Yes | stable | Supported secondary reserve surface; not GA-promised. |
| `da_plus_afrr` | `belgium` | Yes | Yes | stable | Expected-value asymmetric aFRR benchmark. The GA promise is the revision-wrapped portfolio path. |
| `da_plus_afrr` | `netherlands` | Yes | Yes | stable | Promoted Dutch full-stack asymmetric aFRR surface. Belgium remains the narrow GA promise. |
| `schedule_revision` | `belgium` | Yes | Yes | stable | Publicly supported wrapper. Only the portfolio + da_plus_afrr base workflow is GA-promised. |
| `schedule_revision` | `netherlands` | Yes | Yes | stable | Supports Dutch full-stack revision, including da_plus_afrr with locked reserve commitments. |

## Experimental market surface (Germany)

| Workflow | Market | Single asset | Portfolio | Tier | Notes |
| --- | --- | --- | --- | --- | --- |
| `da_only` | `germany` | Yes | Yes | experimental | DE-LU day-ahead on the 15-minute grid; hourly MTU before 2025-10-01 is expanded with provenance. |
| `da_plus_fcr` | `germany` | Yes | Yes | experimental | Symmetric FCR capacity (4-hour products, pay-as-cleared, D-1 08:00). The engine lets reserve vary per quarter-hour; the block-constant research LP lives in scripts/research/germany_backtest.py. |
| `da_plus_afrr` | `germany` | Yes | Yes | experimental | Capacity-only asymmetric aFRR (4-hour products, pay-as-bid, benchmark = published German average awarded price); PICASSO activation energy is not modelled. |
| `schedule_revision` | `germany` | Yes | Yes | experimental | Wrapper over da_only / da_plus_fcr / da_plus_afrr; revised energy is valued at day-ahead prices. |
| `da_plus_imbalance` | `germany` | No | No | unsupported | Rejected by design: German balance groups must be balanced every quarter-hour (Bilanzkreistreue); reBAP is documented, not traded. |

## Forecast provider tiers

| Provider | Auditable | Tier | Notes |
| --- | --- | --- | --- |
| `persistence` | Yes | ga_forecast_path | Built-in deterministic operational forecast path for the narrow GA promise. |
| `csv` | Yes | ga_forecast_path | File-backed deterministic operator path for the narrow GA promise. |
| `custom_python` | Yes | integration_point | Trusted local integration point for private forecast logic; outside the deterministic GA promise. |
| `perfect_foresight` | No | oracle_only | Oracle benchmark only; not part of the operational support promise. |
