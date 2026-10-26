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
- Netherlands `da_plus_afrr`
- Belgium and Netherlands `schedule_revision` where the underlying base workflow is honestly supported by the adapter

## Experimental surfaces

- Germany `da_only`, `da_plus_fcr`, `da_plus_afrr` (capacity-only aFRR, average-awarded-price benchmark) and `schedule_revision`; `da_plus_imbalance` is rejected because German balance groups must stay balanced. See [Germany](germany.md).
- Belgium minute-level real-time engine (`euroflex rt ...`): implicit balancing under the current and the new 2026 imbalance-price formulas. See [Real-time engine](realtime_engine.md).

## Integration points

- `custom_python`

## Oracle only

- `perfect_foresight`

## Unsupported

- live submission / EMS control
- continuous intraday execution

For the code-derived matrix that is kept under test, see [Capability matrix](capability_matrix.md).
