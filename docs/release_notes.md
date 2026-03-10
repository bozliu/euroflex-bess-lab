# Release Notes

## `v1.1.0`

- adds the first operational integration layer
- introduces the local FastAPI service wrapper and lightweight run registry
- adds approval-state transitions, structured operational logging, and `submission_candidate` export profiles

## `v1.0.0`

- defines the narrow GA promise around the Belgium canonical path
- keeps `schema_version: 4` and freezes the current stable machine-readable contracts
- treats `perfect_foresight` as oracle-only and `custom_python` as a stable integration point outside the deterministic GA promise

## `v0.9.0`

- extends the Belgium canonical path with scenario-aware planning and risk-aware reporting
- keeps `schema_version: 4`
- adds scenario-bundle support through `csv` and `custom_python`
- keeps Belgium as the only GA scenario market for `da_plus_afrr`
- preserves the canonical path at `examples/configs/canonical/belgium_full_stack.yaml`

## `v0.8.1`

- hardens the Belgium canonical path into the first strong-GA workflow promise
- freezes the stable JSON contracts and documents semver/deprecation rules
- promotes wheel, Docker, Docker Compose, and canonical-config smoke paths into release gates
- clarifies export profiles, support boundaries, and operator-facing docs

## `v0.8.0`

- adds operational integration and BYO-ML forecast hooks
- adds batch-first CLI support
- adds machine-readable schemas for the stable artifact and config contracts
- adds connector hardening for retries, timeouts, caching, and schema checks

## `v0.7.0`

- adds Belgium-first expected-value `da_plus_afrr`
- supports single-asset, portfolio/shared-POI, and `schedule_revision` on top of `da_plus_afrr`

## `v0.6.0`

- adds schedule revision and reconciliation as first-class runtime capabilities

## `v0.5.0`

- promotes the project to a public beta surface with packaging, docs, Docker, and release workflows
- upgrades configs and artifact metadata to `schema_version: 4`
- introduces first-class site and asset modeling
- adds portfolio and shared-constraint optimization for `da_only` and `da_plus_fcr`

## `v0.4.1`

- hardens the public release surface without changing market math
- adds validation, environment doctoring, and export commands

## `v0.4.0`

- adds reserve-aware `da_plus_fcr` value-stacking benchmarks

## `v0.3.0`

- generalizes the framework from Belgium-only workflows to pluggable market adapters
- adds Netherlands / TenneT support

## `v0.2.0`

- adds walk-forward, forecast-aware benchmarking

## `v0.1.0`

- initial rule-aware benchmark framework for Belgium BESS workflows
