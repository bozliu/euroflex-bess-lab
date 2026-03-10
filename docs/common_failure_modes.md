# Common Failure Modes

## Config fails with `schema_version`

Use `schema_version: 4`. The current release line expects `site` and `assets`, not the old top-level `battery`.

## `schedule_revision` fails without a `revision` block

`schedule_revision` is a wrapper workflow. Add a `revision` block with:

- `base_workflow`
- ordered `revision_checkpoints_local`
- `lock_policy`
- `max_revision_horizon_intervals`

## Netherlands `da_plus_afrr` is rejected

This is intentional. Belgium is the only GA aFRR market in the current release line. The Netherlands adapter exposes the extension point, but it fails fast on runnable `da_plus_afrr` configs.

## Revision checkpoints are rejected

Check that `revision_checkpoints_local`:

- are unique
- are strictly ordered
- fall inside the delivery day in the configured local timezone

## CSV forecast validation fails for lookahead

Every non-oracle CSV forecast row must include valid `issue_time_utc` and `available_from_utc` values that are not later than the decision time.

## Data validation fails on timezone or DST alignment

Run:

```bash
euroflex validate-data <config>
```

and check that:

- `timestamp_utc` is unique and sorted
- `timestamp_local` matches the configured market timezone
- the delivery window fully covers the expected 15-minute grid

## Reserve results look too optimistic

That usually means you are interpreting reserve outputs as a live reserve engine. `da_plus_fcr` is a capacity-first benchmark and Belgium `da_plus_afrr` is an expected-value benchmark with explicit simplifications.

## `reconcile` does not match operator settlement

`euroflex reconcile` compares baseline, revised, and realized outcomes using the repository's benchmark-grade settlement logic plus frozen/public realized inputs. It is an audit aid, not a production settlement engine.
