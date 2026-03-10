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
| `da_plus_afrr` | `netherlands` | No | No | unsupported | Extension point only; explicit runtime rejection. |
| `schedule_revision` | `belgium` | Yes | Yes | stable | Publicly supported wrapper. Only the portfolio + da_plus_afrr base workflow is GA-promised. |
| `schedule_revision` | `netherlands` | Yes | Yes | stable_partial | Supports da_only and da_plus_fcr base workflows only. |

## Forecast provider tiers

| Provider | Auditable | Tier | Notes |
| --- | --- | --- | --- |
| `persistence` | Yes | ga_forecast_path | Built-in deterministic operational forecast path for the narrow GA promise. |
| `csv` | Yes | ga_forecast_path | File-backed deterministic operator path for the narrow GA promise. |
| `custom_python` | Yes | integration_point | Trusted local integration point for private forecast logic; outside the deterministic GA promise. |
| `perfect_foresight` | No | oracle_only | Oracle benchmark only; not part of the operational support promise. |
