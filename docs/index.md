# euroflex_bess_lab

`euroflex_bess_lab` is a public-core decision-support and operator-handoff framework for European BESS workflows. The current GA promise is intentionally narrow and centered on one canonical Belgium path, while the broader public surface is meant for enterprise evaluation, benchmarking, integration, and operator-facing support.

`euroflex_bess_lab` is especially relevant for:

- BESS owners, operators, and optimizers
- flexibility aggregators and VPP teams
- trading, scheduling, and dispatch support desks
- diligence, benchmarking, and revenue-modeling teams
- teams connecting private forecast models to a deterministic market-rule layer

The repository is now centered on one narrow GA promise:

- Belgium
- portfolio / shared POI
- `workflow: schedule_revision`
- `revision.base_workflow: da_plus_afrr`
- forecast paths: `persistence`, `csv`

Start with:

- [Quickstart](quickstart.md)
- [Commercial positioning](commercial_positioning.md)
- [Capability matrix](capability_matrix.md)
- [Operator runbook](operator_runbook.md)
- [Execution handoff](execution_handoff.md)
- [Export profiles](export_profiles.md)
- [Known limitations](known_limitations.md)
- [Compatibility and deprecation policy](compatibility_and_deprecation_policy.md)

Canonical config:

- `examples/configs/canonical/belgium_full_stack.yaml`

The repository also keeps:

- stable secondary surfaces for Belgium/Netherlands energy and FCR workflows
- Belgium aFRR benchmarking outside the narrow GA promise
- `custom_python` as a stable integration point
- oracle-only benchmarking through `perfect_foresight`

What it does **not** promise:

- live market submission
- EMS / SCADA control
- autonomous trading
