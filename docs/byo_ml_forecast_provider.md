# BYO-ML Forecast Provider

`custom_python` is the stable trusted-local forecast integration point. It stays outside the narrow deterministic GA promise, but it is a supported extension surface.

Use it when you want the walk-forward engine to call your Python forecaster directly at each decision checkpoint, instead of writing intermediate CSV snapshots by hand.

## Config shape

```yaml
forecast_provider:
  name: custom_python
  mode: point
  module_path: examples/custom_models/deterministic_visible_mean.py
  class_name: DeterministicVisibleMeanForecaster
  init_kwargs:
    blend_weight: 0.65
```

## Provider contract

Your class must define:

```python
class MyForecaster:
    def initialize(self, *, config, run_context) -> None:
        ...

    def generate_forecast(
        self,
        *,
        market,
        decision_time_utc,
        delivery_frame,
        visible_inputs,
    ):
        ...
```

Notes:

- `visible_inputs` is clipped by the framework to contain only data visible as of the current decision timestamp.
- `forecast_provider.mode` can stay `point` or move to `scenario_bundle` for Belgium-first uncertainty workflows.
- The returned frame must satisfy the normal forecast snapshot contract:
  - `market`
  - `delivery_start_utc`
  - `delivery_end_utc`
  - `forecast_price_eur_per_mwh`
  - `issue_time_utc`
  - `available_from_utc`
  - `provider_name`
- The framework still enforces no-lookahead and delivery-horizon coverage checks on the returned snapshot.

For scenario mode, add:

- `scenario_id`
- `scenario_weight`

## Trusted-local-code boundary

`custom_python` is for trusted local modules only. `euroflex_bess_lab` does not fetch remote code, sandbox user modules, or execute untrusted model plugins.

## Example

See:

- `examples/custom_models/deterministic_visible_mean.py`
- `examples/configs/custom/belgium_full_stack_custom_python.yaml`
