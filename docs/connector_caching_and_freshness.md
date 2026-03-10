# Connector Caching And Freshness

Live connectors now share a common request layer for ENTSO-E, Elia, and TenneT ingestion.

## Supported controls

CLI ingest commands support:

- `--timeout-seconds`
- `--max-retries`
- `--backoff-factor`
- `--cache-dir`
- `--cache-ttl-minutes`

Caching is off by default unless both `--cache-dir` and `--cache-ttl-minutes` are supplied.

## Freshness metadata

Raw payload saves write a sidecar metadata file next to the raw payload with:

- connector id
- fetched timestamp
- request window
- cache-hit status
- timeout / retry settings
- cache key

## Failure behavior

The shared connector layer now fails early for:

- missing or rejected credentials (`401` / `403`)
- rate limits (`429`)
- upstream schema drift before normalization

These are translated into clearer connector errors instead of raw `requests` exceptions where possible.
