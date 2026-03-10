# Support Tiers

## GA

- Belgium
- portfolio / shared POI
- `workflow: schedule_revision`
- `revision.base_workflow: da_plus_afrr`
- forecast paths: `persistence`, `csv`

## Stable secondary surfaces

- Belgium `da_only`
- Belgium `da_plus_fcr`
- Belgium `da_plus_afrr`
- Netherlands `da_only`
- Netherlands `da_plus_fcr`
- Belgium and Netherlands `schedule_revision` where the underlying base workflow is honestly supported by the adapter

## Integration points

- `custom_python`

## Oracle only

- `perfect_foresight`

## Unsupported

- Netherlands `da_plus_afrr`
- live submission / EMS control
- continuous intraday execution

For the code-derived matrix that is kept under test, see [Capability matrix](capability_matrix.md).
