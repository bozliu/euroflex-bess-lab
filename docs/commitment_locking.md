# Commitment Locking

`euroflex_bess_lab` uses explicit lock states so revision runs remain auditable.

## Schedule states

- `baseline_committed`: the original D-1 plan
- `revised_plan`: a checkpoint-adjusted future plan
- `locked_realized`: an interval that can no longer be revised

These states appear through `schedule_version`, `lock_state`, and the schedule-lineage artifacts.

## Locking rules in the current release line

- once a delivery interval starts, it is locked
- past intervals are never revised
- day-ahead commitments are not re-nominated
- `da_plus_fcr` keeps FCR commitments locked once awarded
- Belgium `da_plus_afrr` keeps aFRR up/down commitments locked once awarded
- only future unlocked energy intervals may change

## Why this matters

Without locking, a revision backtest can quietly drift into hindsight. Locking is the line between:

- a truthful operating-plan experiment
- a nicer-looking but less credible re-optimization toy

## Portfolio implication

For portfolio runs, locking applies at both levels:

- per-asset schedules
- site-level POI usage

So a checkpoint revision must still respect:

- locked asset positions
- locked reserve headroom
- shared import/export limits
