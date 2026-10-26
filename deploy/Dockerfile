# syntax=docker/dockerfile:1
# Live paper-trading engine image (shadow mode). Paper only: the engine never submits an order, bid or
# nomination to any market; setpoints are logged (default) or written to a CSV file in the run directory.
#
# Build from the repository root (BuildKit picks up deploy/Dockerfile.dockerignore):
#   docker build -f deploy/Dockerfile -t euroflex-bess-lab:live .
# Optional private image with the fitted forecasters baked in (build them first into models/ with
# `python scripts/ops/build_models.py build --out models`; about 54 MB of pickles you built yourself):
#   docker build -f deploy/Dockerfile --target with-models -t euroflex-bess-lab:live-models .
# Run (state in /data, fitted models in /models):
#   docker run -d --name euroflex-live -v euroflex-run:/data euroflex-bess-lab:live

ARG PYTHON_VERSION=3.12

FROM python:${PYTHON_VERSION}-slim AS build
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1
RUN python -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH
WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
# `ml` = scikit-learn for the forecast strategies (forecast-mpc, SCALES-L); wheels only, no compiler needed.
RUN pip install --upgrade pip \
    && pip install ".[ml]" \
    && pip check

FROM python:${PYTHON_VERSION}-slim AS app
ARG APP_UID=10001
LABEL org.opencontainers.image.title="euroflex-bess-lab live paper trading" \
      org.opencontainers.image.description="Belgian implicit-balancing strategies paper-traded on Elia Open Data; never submits orders" \
      org.opencontainers.image.source="https://github.com/bozliu/euroflex-bess-lab" \
      org.opencontainers.image.licenses="Apache-2.0"
RUN groupadd --system --gid "${APP_UID}" euroflex \
    && useradd --system --uid "${APP_UID}" --gid euroflex --home-dir /app --no-create-home \
       --shell /usr/sbin/nologin euroflex
COPY --from=build /opt/venv /opt/venv
WORKDIR /app
# Ops scripts (entrypoint, health check, model build) and the research script whose deploy stage fits the models.
COPY --chown=root:root --chmod=0755 scripts/ops ./scripts/ops
COPY --chown=root:root --chmod=0644 scripts/research/realtime_backtest.py ./scripts/research/realtime_backtest.py
COPY --chown=root:root --chmod=0644 reports/ledgers/realtime_rl_policy.npz reports/ledgers/realtime_rl_policy.json ./reports/ledgers/
COPY --chown=root:root --chmod=0644 models/reference_manifest.json ./models/reference_manifest.json
RUN mkdir -p /data /models \
    && chown euroflex:euroflex /data /models
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MPLCONFIGDIR=/tmp/matplotlib \
    EUROFLEX_PYTHON=/opt/venv/bin/python \
    EUROFLEX_RUN_DIR=/data/run \
    EUROFLEX_MODELS_DIR=/models \
    EUROFLEX_STRATEGY=threshold \
    EUROFLEX_MECHANISM=auto \
    EUROFLEX_EXECUTION=paper
USER euroflex
VOLUME ["/data"]
# Heartbeat check on <run>/status.json (rewritten every decision minute). The start period covers the API
# backfill of 14 days of quarter-hour prices before the first decision.
HEALTHCHECK --interval=60s --timeout=10s --start-period=5m --retries=3 \
    CMD ["/opt/venv/bin/python", "/app/scripts/ops/healthcheck.py", "--quiet"]
STOPSIGNAL SIGTERM
ENTRYPOINT ["/app/scripts/ops/run_live.sh"]

FROM app AS with-models
COPY --chown=root:root --chmod=0644 models/ /models/

# Default target (last stage): the image without models; mount a models volume at /models or use with-models.
FROM app AS runtime
