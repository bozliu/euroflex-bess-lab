# Quickstart

## Canonical path

The strongest supported path is:

- market: Belgium
- scope: portfolio / shared POI
- workflow: `schedule_revision`
- `revision.base_workflow: da_plus_afrr`
- forecast paths: `persistence`, `csv`
- config: `examples/configs/canonical/belgium_full_stack.yaml`

The canonical public flow assumes a clone of this repository. The promoted configs under `examples/` and notebook assets are not bundled into the PyPI wheel.

## Package-only smoke path

```bash
python -m pip install euroflex-bess-lab
euroflex --version
```

Use this to verify the public CLI/library install. For the canonical Belgium walkthrough below, clone the repository and run from the repo root.

## Repo checkout + local `dl` environment

```bash
git clone https://github.com/bozliu/euroflex-bess-lab.git
cd euroflex-bess-lab
conda env update -f environment.yml
conda activate dl
euroflex validate-config examples/configs/canonical/belgium_full_stack.yaml
euroflex validate-data examples/configs/canonical/belgium_full_stack.yaml
euroflex backtest examples/configs/canonical/belgium_full_stack.yaml --market belgium --workflow schedule_revision
euroflex reconcile artifacts/examples/<run_id> examples/configs/canonical/belgium_full_stack.yaml
euroflex export-schedule artifacts/examples/<run_id> --profile operator
euroflex export-bids artifacts/examples/<run_id> --profile bid_planning
```

## Docker

```bash
docker build -t euroflex-bess-lab .
docker run --rm -v "$PWD/artifacts:/app/artifacts" euroflex-bess-lab \
  euroflex backtest examples/configs/canonical/belgium_full_stack.yaml \
  --market belgium \
  --workflow schedule_revision
```

## Notebook-first

```bash
docker compose up notebooks
euroflex batch examples/batches/canonical_belgium_full_stack.yaml
```

Treat the Compose notebook path as local-only convenience for a trusted workstation. It is not an authenticated multi-user notebook service or a replacement for production access controls.

## Other public entry points

- Belgium single-asset aFRR baseline:
  `examples/configs/reserve/belgium_da_plus_afrr_base.yaml`
- Trusted local forecast integration:
  `examples/configs/custom/belgium_full_stack_custom_python.yaml`
- Secondary Netherlands energy-only surface:
  `examples/configs/basic/netherlands_da_only_base.yaml`
