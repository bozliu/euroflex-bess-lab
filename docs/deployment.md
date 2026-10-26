# Deployment (live paper trading)

`deploy/` holds templates for running the live paper-trading engine (`euroflex live run`, see
`docs/live_paper_trading.md`) as a long-running service: a container image, Docker Compose, Kubernetes, AWS ECS
Fargate (with a CloudFormation stack), systemd and launchd. Every template starts the same entrypoint,
`scripts/ops/run_live.sh`, and is checked by the same heartbeat probe, `scripts/ops/healthcheck.py`.

> **Paper only.** The engine never submits an order, bid or nomination to any market, and the repository contains
> no market-submission adapter. The templates go one step further: `run_live.sh` accepts only the `paper`
> (log the setpoint) and `csv` (append it to a file inside the run directory) execution adapters, and every
> heartbeat must report `"mode": "paper"` and `"market_submission": false` or the health check fails. The only
> network traffic is read-only HTTPS GETs to `opendata.elia.be` and `api.energy-charts.info`.

## What is in `deploy/`

| File | Purpose |
| --- | --- |
| `deploy/Dockerfile` | multi-stage image: wheels built in a `build` stage, slim runtime as non-root user `euroflex` (uid 10001), `.[ml]` installed (scikit-learn for the forecast strategies), `HEALTHCHECK` on the heartbeat; optional `with-models` target |
| `deploy/Dockerfile.dockerignore` | build-context allowlist (no `data/`, `artifacts/`, run directories or virtualenvs in the context) |
| `deploy/docker-compose.live.yml` | `live` service with a persistent run-directory volume, `restart: unless-stopped`, read-only root filesystem, heartbeat health check; `build-models` and `reconcile` tools |
| `deploy/k8s/` | namespace, ConfigMap, PVCs, Deployment (one replica, `Recreate`, startup/liveness/readiness probes, resource limits), NetworkPolicy, `kustomization.yaml`, optional reconcile CronJob |
| `deploy/aws/ecs-task-definition.json` | ECS Fargate task definition with the run directory on EFS (placeholders, no account ids) |
| `deploy/aws/cloudformation.yaml` | minimal parameterised stack: ECS cluster and service (one task), encrypted EFS with an access point, IAM roles, log group, security groups |
| `deploy/systemd/` | `euroflex-live.service`, `live.env.example`, optional heartbeat watchdog (`euroflex-live-healthcheck.service` + `.timer`) |
| `deploy/launchd/io.github.euroflex-bess-lab.live.plist` | macOS LaunchAgent (placeholders for the checkout and state directories) |

## Configuration

`scripts/ops/run_live.sh` turns environment variables into a `euroflex live run` command (`EUROFLEX_DRY_RUN=1`
prints the command instead of running it). Extra arguments are passed through unchanged.

| Variable | Default | Meaning |
| --- | --- | --- |
| `EUROFLEX_RUN_DIR` | `artifacts/live/default` (`/data/run` in the image) | run directory; an existing one is resumed |
| `EUROFLEX_STRATEGY` | `threshold` | `threshold`, `threshold-si`, `idle`, `forecast-mpc`, `prism` / `scales-l` (SCALES-L), `rl-dqn` |
| `EUROFLEX_POWER_MW`, `EUROFLEX_ENERGY_MWH` | `1`, `2` | battery size |
| `EUROFLEX_MECHANISM` | `auto` | `auto` switches from the current to the new imbalance-price formula at the go-live; `current`; `new2026` |
| `EUROFLEX_EXECUTION` | `paper` | `paper` or `csv` (webhooks are refused by the wrapper) |
| `EUROFLEX_POLL_SECONDS` | `15` | polling interval of the minute feeds |
| `EUROFLEX_HISTORY_DAYS` | `14` | quarter-hour price history in the rolling window |
| `EUROFLEX_MINUTE_LAG`, `EUROFLEX_QH_FINAL_LAG` | `1`, `5` | publication model (minutes) |
| `EUROFLEX_DEGRADATION_EUR_PER_MWH` | `8` | degradation cost per MWh discharged |
| `EUROFLEX_REPLAN_EVERY` | `never` | `minute` re-runs `reset()` before each decision (the forecast strategies re-plan through `refresh()` anyway) |
| `EUROFLEX_PUBLIC_HISTORY` | `1` (`0` in the container templates) | seed the window from `data/public` when it covers it |
| `EUROFLEX_STRATEGY_PARAMS` | empty | space-separated `key=value` strategy parameters |
| `EUROFLEX_MODELS_DIR` | none (`/models` in the image) | fitted forecasters for `forecast-mpc` / `prism` |
| `EUROFLEX_VERIFY_MODELS` | `1` | check `<models>/MANIFEST.json` (sha256) before any pickle is loaded |
| `EUROFLEX_VERBOSE` | `0` | log every paper setpoint |
| `EUROFLEX_PYTHON` | `python` | interpreter used for the checks and as a fallback launcher |

A run directory holds one run: changing the strategy or the battery needs a new `EUROFLEX_RUN_DIR`.

## Health check

`python scripts/ops/healthcheck.py [--run-dir DIR]` (default `$EUROFLEX_RUN_DIR`) reads `status.json`, which the
engine rewrites atomically after every decision minute. Standard library only.

| Exit code | Meaning |
| --- | --- |
| `0` | healthy: heartbeat younger than `--max-age-s` (default 180 s, or `$EUROFLEX_HEALTH_MAX_AGE_S`), state `running` or `draining`, paper mode |
| `1` | unhealthy: stale heartbeat, engine stopped (`completed`, `stopped`, `interrupted`), or a `--min-decisions` / `--max-strategy-errors` limit not met |
| `3` | not started: no `status.json` yet (the engine first backfills 14 days of prices from the API) |
| `4` | invalid: unreadable or foreign `status.json`, or safety flags other than paper-only |

`--json` prints the evaluation (state, heartbeat age, decisions, settled quarter-hours, paper P&L); `--quiet` only
sets the exit code. The probes in the templates use a start period of 5 minutes (Docker, ECS) or a startup probe
of up to 10 minutes (Kubernetes); readiness additionally requires one decision (`--min-decisions 1`).

## Docker and Compose

```bash
docker build -f deploy/Dockerfile -t euroflex-bess-lab:live .
docker compose -f deploy/docker-compose.live.yml up -d --build     # threshold strategy, 1 MW / 2 MWh
docker compose -f deploy/docker-compose.live.yml ps                # health from the heartbeat
docker compose -f deploy/docker-compose.live.yml exec live euroflex live status /data/run
docker compose -f deploy/docker-compose.live.yml --profile tools run --rm reconcile
```

The image runs as uid 10001 with a read-only root filesystem (only `/data`, `/models` and a `/tmp` tmpfs are
writable), drops all capabilities and stops cleanly on `SIGTERM` (the engine persists its state and resumes on the
next start). State lives in the `live-run` volume; the image itself contains no data and no models.

## Forecast strategies: models

`forecast-mpc` and SCALES-L (`prism`) need the deployment forecasters fitted by the `deploy` stage of
`scripts/research/realtime_backtest.py` (training data 2026-04-24 to 2026-10-24, test window open until
2027-11-28). The artefacts are about 54 MB (three pickled gradient-boosting forecasters of 11.5, 21.3 and 20.7 MB,
plus a fusion-variance file and the RL policy), so they are not committed. They are rebuilt locally from the
committed `data/public` set (three parallel fits; about a minute on an 8-core laptop, longer on small machines):

```bash
pip install -e '.[ml]'
python scripts/ops/build_models.py build --out models     # writes models/*.pkl and models/MANIFEST.json
python scripts/ops/build_models.py verify models --reference models/reference_manifest.json
export EUROFLEX_MODELS_DIR=models
```

- The fold is pinned to `models/reference_manifest.json`, so a rebuild reproduces the frozen models even after
  `data/public` grows (`--data-end latest` refits on everything committed instead). A rebuild with the library
  versions recorded in the reference manifest reproduced every forecaster byte for byte (same sha256); other
  library versions can produce different bytes, which the track record treats as different models.
- `models/` is gitignored except for the reference manifest. Pickles are only loaded from a directory you built,
  and `run_live.sh` verifies `MANIFEST.json` before starting a forecast strategy.
- Compose: `docker compose -f deploy/docker-compose.live.yml --profile tools run --rm build-models` builds into the
  `live-models` volume (it mounts the checkout's `data/public` read-only).
- Kubernetes: copy a locally built directory into the `euroflex-live-models` PVC (for example with `kubectl cp`
  through a helper pod), or use the image target below.
- ECS or any registry-based platform: `docker build -f deploy/Dockerfile --target with-models -t <your registry>/euroflex-bess-lab:live-models .`
  bakes your locally built `models/` into a private image.

## Kubernetes

```bash
kubectl apply -k deploy/k8s                           # namespace euroflex, config, PVCs, deployment, network policy
kubectl -n euroflex logs deploy/euroflex-live -f
kubectl -n euroflex exec deploy/euroflex-live -- euroflex live status /data/run
kubectl apply -f deploy/k8s/cronjob-reconcile.yaml     # optional daily reconcile
```

Point `images` in `deploy/k8s/kustomization.yaml` at your registry. The Deployment has exactly one replica and the
`Recreate` strategy so that one run directory never has two writers; the pod runs non-root with a read-only root
filesystem under the `restricted` Pod Security level. Requests 250m CPU / 512 Mi, limits 1 CPU / 2 Gi. The run PVC
(20 Gi) holds the journal, ledger and the archive of every published row seen live (a few GB per year). The
NetworkPolicy denies all ingress and allows DNS and HTTPS egress. The reconcile CronJob mounts the same
ReadWriteOnce volume and is therefore scheduled onto the live pod's node.

## AWS ECS Fargate

```bash
aws cloudformation deploy --template-file deploy/aws/cloudformation.yaml --stack-name euroflex-live \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides ImageUri=<your image> VpcId=<vpc> SubnetA=<subnet> SubnetB=<subnet>
```

The stack creates an ECS cluster and a service with one Fargate task (`MaximumPercent` 100, `MinimumHealthyPercent`
0: never two tasks on the same state), encrypted EFS with an access point owned by uid 10001 for the run directory
(TLS and IAM authorisation), a task role limited to that access point, a log group and security groups (no inbound
rules). Parameters cover the strategy, battery, formula setting, CPU/memory, architecture and log retention.
`deploy/aws/ecs-task-definition.json` is the same task as a stand-alone template for existing clusters (replace
the `REPLACE_WITH_*` placeholders). Both are templates: bring your own image, VPC and subnets with outbound HTTPS.

## systemd

```bash
sudo useradd --system --home /var/lib/euroflex --shell /usr/sbin/nologin euroflex
sudo cp deploy/systemd/euroflex-live.service /etc/systemd/system/
sudo install -D -m 0640 deploy/systemd/live.env.example /etc/euroflex/live.env
sudo systemctl daemon-reload && sudo systemctl enable --now euroflex-live.service
journalctl -u euroflex-live -f
```

The unit expects a checkout in `/opt/euroflex-bess-lab` with a virtualenv in `.venv`, keeps state in
`/var/lib/euroflex` (`StateDirectory`), restarts on failure and runs with a hardened sandbox
(`ProtectSystem=strict`, `NoNewPrivileges`, private `/tmp`). The optional `euroflex-live-healthcheck.timer` restarts
the service when the heartbeat goes stale while the unit is active.

## launchd (macOS)

```bash
state="$HOME/Library/Application Support/euroflex-bess-lab"; mkdir -p "$state"
sed -e "s|__REPO_DIR__|$PWD|g" -e "s|__STATE_DIR__|$state|g" \
  deploy/launchd/io.github.euroflex-bess-lab.live.plist > ~/Library/LaunchAgents/io.github.euroflex-bess-lab.live.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/io.github.euroflex-bess-lab.live.plist
```

The agent keeps the engine alive (`KeepAlive`), logs to the state directory and keeps the run directory outside
the checkout. Stop it with `launchctl bootout gui/$(id -u)/io.github.euroflex-bess-lab.live`.

## Operating notes

- Restarts resume the run directory: SOC, open quarter-hours and unsettled prices are restored and minutes while
  the engine was down are journaled as missed (idle). An upgrade is stop, replace the image or checkout, start.
- `--mechanism auto` follows the new imbalance-price formula at the go-live (planned 24 Nov 2026 10:00 Brussels
  time) once Elia's feeds carry it; see `docs/live_paper_trading.md` for the switching rules.
- `euroflex live reconcile <run>` compares the provisional paper ledger with Elia's validated prices once they are
  published; the Compose `reconcile` tool and the Kubernetes CronJob run it daily.
- Keep the host clock NTP-synchronised: decisions are taken 0.5 s after each minute boundary.
- `tests/test_deploy_templates.py` checks that every template parses, references files that exist in the image or
  the checkout, stays paper-only, carries no account ids or credentials, and only sets variables that
  `run_live.sh` understands; `tests/test_deploy_healthcheck.py` runs the health check against a `status.json`
  written by the live engine.

The forward paper track record of the same strategies is described in `docs/track_record.md`.
