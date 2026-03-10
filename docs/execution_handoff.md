# Execution Handoff

`euroflex_bess_lab` produces handoff artifacts for people and downstream systems. It does **not** produce live submission payloads for EPEX, Elia, or EMS/SCADA systems.

## Export profiles

### `benchmark`

- analytics-first payloads
- richest field surface
- intended for notebooks, reviews, and offline analysis

### `operator`

- human-in-the-loop schedule handoff
- cleaner site/asset schedule fields
- intended for schedulers and operations analysts

### `bid_planning`

- human-in-the-loop bid planning payloads
- site-level reserve/energy nomination plus asset allocation annex
- intended for traders or scheduling desks preparing bids

### `submission_candidate`

- closest handoff format to an execution router or internal scheduling service
- still **not** live-submission-ready
- explicit manifest metadata marks it as non-live

## Typical handoff chain

1. Build or revise the schedule.
2. Reconcile expected vs realized outputs.
3. Export `operator` schedule for review.
4. Export `bid_planning` or `submission_candidate` payloads for downstream ingestion.
5. Perform any market-specific XML/FIX/EMS transformation outside this repository.

## What downstream teams usually map from

- `site_schedule.json` or `site_bids.json`
- `asset_allocation.json` or `asset_reserve_allocation.json`
- `manifest.json` for checksums, profile, run id, schedule version, and provenance

## What a company still has to add

- market-specific submission adapters
- entitlement checks and approval workflow
- trader/ops review UI
- EMS or scheduling-system mapping
- live connector SLAs and operational monitoring
