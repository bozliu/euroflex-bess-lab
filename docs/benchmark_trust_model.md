# Benchmark Trust Model

`euroflex_bess_lab` separates benchmark modes deliberately.

## Oracle benchmarks

Oracle benchmarks use perfect foresight. They are useful as:

- an upper bound
- a regression target
- a way to measure the cost of imperfect information

They are not auditable trading workflows.

Examples:

- `belgium.da_only.perfect_foresight.single_asset`
- `netherlands.da_plus_fcr.perfect_foresight.portfolio`
- `belgium.da_only.perfect_foresight.single_asset.baseline`

## As-of-safe workflows

As-of-safe workflows only use information visible at the decision timestamp.

Built-in auditable providers:

- `persistence`
- `csv`

These runs are the right baseline for forecast-aware public benchmarking.

## Revision-aware workflows

`schedule_revision` stays auditable only when:

- the base workflow is itself auditable under the chosen provider
- checkpoints use only data visible at that checkpoint
- locked commitments remain locked

Revision benchmarks produce paired benchmark names:

- `{market_id}.{base_workflow}.{provider_name}.{run_scope}.baseline`
- `{market_id}.{base_workflow}.{provider_name}.{run_scope}.revision`

This separates:

- the original D-1 plan
- the revised expected plan
- the final realized outcome

## Reserve-lite assumptions

Reserve-aware benchmarking in the current release line is intentionally simplified.

- symmetric FCR-style capacity reservation
- asymmetric Belgium-first aFRR with expected-value activation
- capacity-only settlement by default
- expected activation settlement for aFRR only
- optional non-delivery penalty proxy
- no qualification or bid-size granularity logic

## Benchmark-grade vs market-grade

Benchmark-grade:

- due diligence
- sensitivity analysis
- cross-market comparison
- portfolio screening
- plug-in testing for private forecasts
- checkpoint revision and expected-vs-realized audit

Not yet market-grade:

- live bid submission
- qualification-aware reserve operations
- continuous intraday re-trading
- operational reconciliation against real settlement statements
- EMS-integrated dispatch control
