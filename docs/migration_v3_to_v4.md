# Migration v3 To v4

`v0.5.0` moved the public config shape from a single-battery model to a site/asset model. The `v1.x` line keeps `schema_version: 4`, keeps the optional `revision` block for checkpoint-based schedule revision, and treats Belgium-first `da_plus_afrr` as the canonical reserve-aware path.

## Before

`v3` centered configs on one implicit battery and single-asset artifacts.

## After

`v4` requires:

- `site`
- `assets`
- `run_scope` metadata in artifacts
- site and asset dispatch outputs

Optional in the `v1.x` line:

- `revision`
- baseline/revision/reconciliation artifacts for revision runs
- `afrr` plus the six Belgium aFRR benchmark input series when you opt into `da_plus_afrr`

## Mechanical migration

1. Replace the old top-level `battery` block with:
   - `site`
   - `assets`
2. Wrap the old battery spec under:
   - `assets[0].battery`
3. Add a stable asset id:
   - `assets[0].id`
4. Keep the rest of the workflow and data blocks largely the same.

## Artifact changes

New stable files:

- `site_dispatch.parquet`
- `asset_dispatch.parquet`
- `asset_pnl_attribution.parquet`

Use `site_dispatch.parquet` where older tooling expected the previous site-level dispatch output.

If you are already on a `v4` config, no schema migration is required for `v1.0.0` or `v1.1.0`; add `afrr` and the six aFRR input series only when you opt into Belgium `da_plus_afrr`.
