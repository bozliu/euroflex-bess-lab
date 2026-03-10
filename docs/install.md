# Install Matrix

## Recommended: `dl` conda environment

```bash
conda env update -f environment.yml
conda activate dl
euroflex --version
```

This is the reference environment for local development, tests, notebooks, and canonical smoke runs.

## Package install

```bash
python -m pip install euroflex-bess-lab
euroflex --version
```

The release workflow smoke-tests a clean wheel install against the canonical Belgium full-stack path.

## Docker

```bash
docker build -t euroflex-bess-lab .
docker run --rm euroflex-bess-lab euroflex --version
```

The CI pipeline treats Docker build/run and Docker Compose notebook startup as release gates.

## Compose notebooks

```bash
docker compose up notebooks
```

This starts JupyterLab in the project container with the repository mounted for notebook-first evaluation.

Treat this Compose surface as local-development-only. It is a tokenless notebook convenience for a trusted workstation or private network, not an authenticated shared service.
