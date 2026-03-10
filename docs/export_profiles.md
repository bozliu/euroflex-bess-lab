# Export Profiles

`euroflex_bess_lab` keeps exports profile-aware so the repo is explicit about who a payload is for and what the payload is not.

## Schedule exports

Command:

```bash
euroflex export-schedule <run_dir> --profile benchmark
euroflex export-schedule <run_dir> --profile operator
euroflex export-schedule <run_dir> --profile submission_candidate
```

Profiles:

- `benchmark`
  - analytics-friendly
  - fuller field set
  - explicitly benchmark-grade only
- `operator`
  - slimmer schedule handoff
  - intended for human-in-the-loop scheduling workflows
  - still not live submission ready
- `submission_candidate`
  - closer to downstream scheduler or execution-router handoff
  - includes stricter schedule-lineage metadata
  - still not live submission ready

## Bid exports

Command:

```bash
euroflex export-bids <run_dir> --profile benchmark
euroflex export-bids <run_dir> --profile bid_planning
euroflex export-bids <run_dir> --profile submission_candidate
```

Profiles:

- `benchmark`
  - full benchmark context
  - reserve assumption tags
- `bid_planning`
  - human-facing bid-planning payload
  - still not a live market submission format
- `submission_candidate`
  - closer to downstream execution or routing handoff
  - keeps benchmark assumption tags and manifest metadata
  - still not a live market submission format

## Interpret the profiles literally

- `benchmark` is the analytics-first export surface
- `operator` is a human-in-the-loop schedule handoff format
- `bid_planning` is a human-in-the-loop planning format for bids/reserve allocations
- `submission_candidate` is a downstream handoff candidate for human review

None of them should be interpreted as:

- a live market submission payload
- an EMS / SCADA control payload
- a regulator-facing settlement file

## Manifest metadata

Every export manifest now includes:

- `profile`
- `intended_consumer`
- `benchmark_grade_only`
- `live_submission_ready`
- `run_id`
- `market_id`
- `workflow`
- `run_scope`
- generation timestamp and checksums
