# Revision Workflow

`schedule_revision` is an execution wrapper around an existing market workflow.

Use it when you want to:

- create a normal D-1 plan
- revisit the remaining horizon at explicit public-data checkpoints
- keep already committed intervals fixed
- compare the baseline plan with a revised expected plan

## Required shape

Set:

- `workflow: schedule_revision`
- `revision.base_workflow`

Supported public base workflows in the current release line:

- `da_only`
- `da_plus_fcr`
- Belgium `da_plus_afrr`

## What changes at a checkpoint

At each `revision_checkpoints_local` time, the engine:

1. reads the realized current state
2. locks all current and past intervals
3. re-optimizes only the future unlocked horizon
4. writes a new `schedule_version`

The result is a schedule lineage:

- `baseline`
- `revision_01`
- `revision_02`
- ...
- `final_realized`

## Base-workflow semantics

`base_workflow` controls:

- settlement logic
- reserve constraints
- forecast requirements
- auditable-vs-oracle interpretation

Examples:

- `schedule_revision` + `base_workflow=da_only` revises future energy positions
- `schedule_revision` + `base_workflow=da_plus_fcr` revises future energy positions while keeping awarded FCR headroom locked
- `schedule_revision` + `base_workflow=da_plus_afrr` revises future energy positions while keeping awarded Belgium aFRR up/down commitments locked

## What it is not

Revision in the current release line is not:

- continuous intraday trading
- order-book simulation
- day-ahead re-nomination
- reserve re-award logic
