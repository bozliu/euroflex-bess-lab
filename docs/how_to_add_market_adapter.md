# How To Add A Market Adapter

`euroflex_bess_lab` treats the market layer as a plugin-style contract. A new adapter should let the rest of the stack keep the same shape:

- configs stay `schema_version: 4`
- walk-forward still runs through `run_walk_forward()`
- compare/sweep still consume the stable artifact contract
- reserve-aware workflows can query the adapter for supported reserve products
- portfolio runs continue to use site-level market data and asset-level dispatch outputs
- `schedule_revision` continues to route settlement and timing through `revision.base_workflow`

Public imports should go through `euroflex_bess_lab.markets` or the package root. `markets/adapters/` is the implementation layer, not the preferred import path for user code.

## Adapter checklist

Create a new file under `src/euroflex_bess_lab/markets/adapters/` and implement `MarketAdapter`:

```python
class MarketAdapter:
    market_id: str
    timezone: str
    resolution_minutes: int
    supported_workflows: tuple[str, ...]

    def load_actuals(self, config): ...
    def validate_timing(self, config): ...
    def decision_schedule(self, config): ...
    def settlement_engine(self, workflow): ...
    def default_benchmarks(self): ...
    def supported_reserve_products(self): ...
    def build_reserve_product(self, config): ...
```

## Practical steps

1. Add the adapter implementation.
   - Reuse `load_input_series()` from the base adapter.
   - Enforce market timezone and resolution in `validate_timing()`.
   - Return the right settlement engine for each execution workflow.
   - If the market supports reserve-lite benchmarks, expose them through `supported_reserve_products()` and `build_reserve_product()`.
   - Make sure revision runs behave correctly when `config.execution_workflow` differs from `config.workflow`.

2. Register the adapter.
   - Add it to `src/euroflex_bess_lab/markets/adapters/registry.py`.
   - Users select `--market` and `--workflow` explicitly.

3. Add normalized fixtures.
   - Put frozen sample CSVs in `examples/data/`.
   - If live ingestion needs auth, document the env vars in the example config.
   - Prefer raw fixture samples under `tests/fixtures/raw/` for normalization tests.

4. Add example configs.
   - Create `basic/<market>_da_only_base.yaml`.
   - If publicly supported, add a curated example only when it belongs in the promoted example surface.
   - If reserve-lite is supported, add `reserve/<market>_da_plus_fcr_base.yaml`.
   - If expected-value aFRR is supported, add `reserve/<market>_da_plus_afrr_base.yaml`.
   - If portfolio workflows are supported, add `basic/<market>_portfolio_da_only_base.yaml`, `reserve/<market>_portfolio_da_plus_fcr_base.yaml`, and any honest `da_plus_afrr` portfolio examples.
   - If revision is supported, add `basic/<market>_schedule_revision_da_only_base.yaml` and any reserve-aware revision examples you can support honestly.

5. Add tests.
   - Adapter contract test
   - Walk-forward smoke test
   - Artifact contract test
   - CLI smoke test if you add ingestion or a new workflow
   - Notebook or docs smoke test if the new adapter ships with a tutorial
