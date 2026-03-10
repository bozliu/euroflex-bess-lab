# Architecture

`euroflex_bess_lab` is organized around six layers:

1. `config.py`
   - validates `schema_version: 4`
   - resolves runtime paths
   - treats the primary runtime object as a `site` with one or more `assets`

2. `markets/`
   - `euroflex_bess_lab.markets` is the public import surface
   - `markets/adapters/` contains Belgium and Netherlands implementations
   - adapters own timing, settlement semantics, actual-data loading, and reserve-product availability

3. `forecasts/`
   - built-in operational forecast paths are `persistence` and `csv`
   - `custom_python` is a stable integration point for trusted local model code
   - `perfect_foresight` stays oracle-only
   - walk-forward execution never lets the solver see data that was not visible at the decision timestamp

4. `optimization/`
   - Pyomo + HiGHS kernels for:
     - `da_only`
     - `da_plus_fcr`
     - Belgium-first `da_plus_afrr`
   - portfolio kernels enforce shared POI constraints at the site level

5. `backtesting/`
   - walk-forward engine
   - checkpoint-based schedule revision
   - oracle reference path
   - stable artifact writing
   - reason-code assignment for site and asset outputs

6. `analytics/`, `validation.py`, `exports.py`, and `reconciliation.py`
   - reporting and rainflow diagnostics
   - config/data/doctor checks
   - downstream handoff payloads for site schedules and asset allocations
   - baseline vs revision vs realized reconciliation

## Portfolio mental model

`v1.0.0` and `v1.1.0` keep the first-class site/portfolio model and add Belgium-first aFRR plus schedule lineage on top:

- a **site** owns shared POI limits
- **assets** are currently battery-only
- market data is site-level
- optimization decisions are made per asset but constrained by site import/export limits
- outputs are written at both the site and asset level
- revision runs preserve a baseline plan, checkpoint revisions, and a final locked-realized trace

## Revision mental model

`schedule_revision` is an execution wrapper:

- `workflow: schedule_revision` turns on checkpoint re-optimization
- `revision.base_workflow` selects the underlying market workflow
- past intervals are locked
- FCR and Belgium aFRR commitments remain locked in the current release line
- only future unlocked energy intervals can change

## Public import rule

New code should import through:

- `euroflex_bess_lab`
- `euroflex_bess_lab.markets`
