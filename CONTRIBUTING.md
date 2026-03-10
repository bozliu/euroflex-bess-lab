# Contributing

## Development flow

1. Create or update the `dl` conda environment.
2. Install the project in editable mode with `pip install -e .[dev]`.
3. Run:
   - `make lint`
   - `make typecheck`
   - `make test`
   - `make notebooks`
   - `make docs`

## Ground rules

- Treat `euroflex_bess_lab` and `euroflex_bess_lab.markets` as the public import surfaces.
- Keep `schema_version: 4` stable unless a planned breaking change is documented.
- Prefer frozen fixtures and deterministic examples over live-API-only tests.
- Keep new workflows assumption-explicit and document market simplifications.
- Avoid committing generated outputs under `artifacts/`, `tmp/`, or `*.egg-info/`.

## Pull requests

- Update docs when public behavior changes.
- Add tests for new CLI commands, artifact contracts, export outputs, or portfolio behaviors.
- Update the stable/experimental workflow matrix when workflow scope changes.
- If you touch public artifacts or configs, add a migration note.
