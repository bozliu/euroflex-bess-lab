# Compatibility and Deprecation Policy

`euroflex_bess_lab` now treats the Belgium canonical path as the first explicitly strong-GA surface:

- market: Belgium
- scope: portfolio/shared POI
- workflow: `schedule_revision`
- `revision.base_workflow: da_plus_afrr`
- canonical config: `examples/configs/canonical/belgium_full_stack.yaml`

## Stable contracts

The following machine-readable contracts are frozen under semver discipline:

- `schemas/config.v4.json`
- `schemas/summary.schema.json`
- `schemas/export_manifest.schema.json`
- `schemas/reconciliation_summary.schema.json`

## Semver rules

- Patch releases may fix bugs, improve diagnostics, and tighten validation without changing the schema contract.
- Minor releases may add optional fields, optional config keys, or new export-profile metadata.
- Major releases are required for removals, renames, or meaning changes in frozen schema fields.

## CLI compatibility

The `euroflex` CLI is treated as stable for:

- `validate-config`
- `validate-data`
- `doctor`
- `backtest`
- `reconcile`
- `compare`
- `sweep`
- `batch`
- `export-schedule`
- `export-bids`
- `export-revision`

Additive flags may land in minor releases. Breaking command or flag changes require a major version.

## Export-profile stability

These profile names are part of the public contract:

- schedule exports: `benchmark`, `operator`, `submission_candidate`
- bid exports: `benchmark`, `bid_planning`, `submission_candidate`

Profiles may gain additive fields in minor releases, but their intent should stay stable:

- `benchmark`: analytics-first, benchmark-grade only
- `operator`: human-in-the-loop schedule handoff
- `bid_planning`: human-in-the-loop bid planning handoff
- `submission_candidate`: downstream execution-router or scheduler handoff candidate, still not live submission ready

None of these profiles are live-submission-ready in `v1.1.0`.

## Deprecation policy

- Deprecations are announced in the changelog and release notes before removal.
- Deprecated fields or surfaces should continue to validate for at least one minor release unless there is a correctness or safety issue.
- If a deprecation affects canonical examples, the replacement path will be documented in the same release.

## Scope policy

The canonical Belgium full-stack path is the reference support target. Other workflows and markets remain supported according to the published capability and stable/experimental matrices:

- [Capability matrix](capability_matrix.md)
- [Stable vs experimental workflows](stable_experimental_matrix.md)
