# Changelog

## v1.1.0

- adds the first operational integration layer with a local FastAPI service wrapper
- introduces the lightweight run registry, approval-state transitions, and structured operational logging
- adds `submission_candidate` export profiles for downstream human-in-the-loop handoff

## v1.0.0

- defines the narrow GA promise around the Belgium canonical path
- freezes the current machine-readable contracts under semver discipline
- keeps `perfect_foresight` oracle-only and `custom_python` as a stable integration point outside the deterministic GA promise

## v0.9.0

- extends the Belgium canonical path with scenario-aware planning and risk-aware reporting
- keeps `schema_version: 4` and treats scenario mode as an additive extension of the forecast layer
- adds scenario-bundle support through `csv` and `custom_python`
- keeps Belgium as the only GA scenario market for `da_plus_afrr`

## v0.8.1

- hardens the Belgium canonical path into the first explicit strong-GA workflow promise
- freezes the stable JSON contracts and documents compatibility / deprecation policy
- promotes wheel, Docker, Docker Compose, and canonical-config smoke paths into release gates
- tightens release/docs/export wording around benchmark vs operator-facing usage

## v0.8.0

- adds operational integration and BYO-ML forecast hooks
- adds batch-first CLI support
- adds machine-readable schemas for the stable artifact and config contracts
- adds connector hardening for retries, timeouts, caching, and schema checks

## v0.7.0

- adds Belgium-first expected-value `da_plus_afrr`
- supports single-asset, portfolio/shared-POI, and `schedule_revision` on top of `da_plus_afrr`

## v0.6.0

- adds schedule revision and reconciliation as first-class runtime capabilities

## v0.5.0

- promotes the project to a public beta surface with packaging, docs, Docker, and release workflows
- upgrades configs and artifact metadata to `schema_version: 4`
- introduces first-class site and asset modeling
- adds portfolio and shared-constraint optimization for `da_only` and `da_plus_fcr`
- keeps `da_plus_imbalance` single-asset only
- adds stable site/asset dispatch artifacts and portfolio-aware exports

## v0.4.1

- hardens the public release surface without changing market math
- adds validation, environment doctoring, and export commands
- cleans generated outputs from the repository
- reorganizes examples and clarifies trust/documentation boundaries

## v0.4.0

- adds reserve-aware `da_plus_fcr` value-stacking benchmarks
- extends Belgium and Netherlands adapters with reserve-lite support

## v0.3.0

- generalizes the framework from Belgium-only workflows to pluggable market adapters
- adds Netherlands / TenneT support

## v0.2.0

- adds walk-forward, forecast-aware benchmarking

## v0.1.0

- initial rule-aware benchmark framework for Belgium BESS workflows
