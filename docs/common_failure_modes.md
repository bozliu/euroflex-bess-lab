# Common Failure Modes

## Config fails with `schema_version`

Use `schema_version: 4`. The current release line expects `site` and `assets`, not the old top-level `battery`.

## `schedule_revision` fails without a `revision` block

`schedule_revision` is a wrapper workflow. Add a `revision` block with:

- `base_workflow`
- ordered `revision_checkpoints_local`
- `lock_policy`
- `max_revision_horizon_intervals`

## TenneT live ingest fails before normalization

Check:

- you passed `--env acceptance` or `--env production` intentionally
- `TENNET_API_KEY` is present in the environment, or the matching `TENNET_API_KEY_ACCEPTANCE` / `TENNET_API_KEY_PRODUCTION`
- if TenneT gave you an environment-specific hostname, set `TENNET_API_BASE_URL_ACCEPTANCE` or `TENNET_API_BASE_URL_PRODUCTION`
- the requested window is within TenneT historical coverage
- the payload still contains native 15-minute settlement rows

The connector now fails early on missing credentials, rate limits, timeouts after the configured retries, and schema drift before normalization.

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

That usually means you are interpreting reserve outputs as a live reserve engine. `da_plus_fcr` is a capacity-first benchmark and Belgium/Netherlands `da_plus_afrr` are expected-value benchmarks with explicit simplifications.

## `reconcile` does not match operator settlement

`euroflex reconcile` compares baseline, revised, and realized outcomes using the repository's benchmark-grade settlement logic plus frozen/public realized inputs. It is an audit aid, not a production settlement engine.
