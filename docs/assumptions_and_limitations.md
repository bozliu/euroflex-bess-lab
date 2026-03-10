# Assumptions And Limitations

`euroflex_bess_lab` is a scheduling, revision, and audit framework. It is not a live trading engine, a live reserve-submission stack, or an EMS / SCADA controller.

## Narrow GA scope

The only strong GA promise is:

- Belgium
- portfolio / shared POI
- `workflow: schedule_revision`
- `revision.base_workflow: da_plus_afrr`
- forecast paths: `persistence`, `csv`

Other surfaces may remain stable, oracle-only, or integration-only, but they are outside the narrow GA promise.

## Important simplifying assumptions

- Reserve support is benchmark-grade rather than live submission-grade.
- `da_plus_fcr` is capacity-first and does not model full activation dispatch.
- `da_plus_afrr` is expected-value only.
- Expected-value aFRR activation can diverge from realized discrete activation, so simulated SoC may diverge from real physical SoC.
- Schedule revision is checkpoint-based, not continuous intraday order-book trading.
- Export profiles are human-in-the-loop handoff formats, not live exchange payloads.
- Reconciliation uses benchmark-grade settlement logic and public/frozen realized inputs.

## Explicitly out of scope

- Netherlands `da_plus_afrr`
- live bid submission
- EMS / SCADA control
- continuous intraday execution and order-book simulation
- stochastic aFRR activation physics
- automated capital allocation or investment advice
