# Service API

`v1.1.0` adds a lightweight local FastAPI wrapper around the CLI/runtime surfaces.

Start it locally:

```bash
euroflex serve --host 127.0.0.1 --port 8000
```

Available endpoints:

- `POST /validate`
- `POST /backtest`
- `POST /reconcile`
- `POST /export`
- `POST /batch/run`
- `POST /runs/{run_id}/transition`

This service is intentionally local-first:

- no auth layer
- no multi-tenant state
- no live submission
- no remote code execution

Use it when an operator tool or scheduler wants a machine-friendly wrapper around the canonical CLI flow.
