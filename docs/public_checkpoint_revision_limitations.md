# Public Checkpoint Revision Limitations

`schedule_revision` in the current release line is designed to be truthful and auditable with public-style data, not to simulate a continuous intraday desk.

## What it does model

- a baseline D-1 schedule
- explicit local revision checkpoints
- locked commitments for current and past intervals
- forecast-aware re-optimization of the future unlocked horizon
- reserve headroom preservation when the base workflow is `da_plus_fcr`
- locked Belgium aFRR commitments with energy-only reshaping around them
- reconciliation against frozen or public realized inputs

## What it does not model

- continuous limit-order-book trading
- queue position, partial fills, or execution slippage
- day-ahead re-nomination after gate closure
- reserve re-award, FCR re-nomination, or aFRR re-award
- operator qualification logic
- production settlement statements

## Why this matters

Checkpoint revision is intentionally narrower than a live trading system. The goal is to answer:

- did later public information justify changing the future plan?
- how much value was blocked by locked commitments?
- how different was realized PnL from the baseline and revised expectations?

That makes `schedule_revision` useful for:

- operational decision-support experiments
- revision-policy benchmarking
- forecast-value studies
- schedule audit trails

It does **not** make the repo a live intraday execution engine.
