#!/usr/bin/env bash
# Start the live paper-trading engine (shadow mode) with settings taken from environment variables.
#
# PAPER ONLY. `euroflex live run` never submits an order, bid or nomination to any market; this wrapper
# additionally allows only the `paper` (log) and `csv` (setpoint file inside the run directory) execution
# adapters. The run directory is resumed after a restart (SOC, open quarter-hours, unsettled prices).
#
# Environment (default in brackets):
#   EUROFLEX_RUN_DIR                  run directory, resumed on restart [artifacts/live/default]
#   EUROFLEX_STRATEGY                 threshold | threshold-si | idle | forecast-mpc | prism | scales-l | rl-dqn [threshold]
#   EUROFLEX_POWER_MW                 battery power in MW [1]
#   EUROFLEX_ENERGY_MWH               battery energy in MWh [2]
#   EUROFLEX_MECHANISM                auto | current | new2026 [auto]
#   EUROFLEX_EXECUTION                paper | csv [paper]
#   EUROFLEX_POLL_SECONDS             polling interval of the minute feeds in seconds [15]
#   EUROFLEX_HISTORY_DAYS             days of quarter-hour price history in the rolling window [14]
#   EUROFLEX_MINUTE_LAG               publication lag of minute rows in minutes [1]
#   EUROFLEX_QH_FINAL_LAG             publication lag of final quarter-hour prices in minutes [5]
#   EUROFLEX_DEGRADATION_EUR_PER_MWH  degradation cost per MWh discharged [8]
#   EUROFLEX_REPLAN_EVERY             never | minute [never] (forecast strategies re-plan via refresh() anyway)
#   EUROFLEX_PUBLIC_HISTORY           1 = seed the window from data/public when it covers it [1]
#   EUROFLEX_STRATEGY_PARAMS          space-separated key=value strategy parameters []
#   EUROFLEX_MODELS_DIR               fitted models for forecast-mpc / prism / scales-l (rl-dqn: optional)
#   EUROFLEX_VERIFY_MODELS            1 = check <models>/MANIFEST.json (sha256) before loading pickles [1]
#   EUROFLEX_VERBOSE                  1 = log every paper setpoint [0]
#   EUROFLEX_PYTHON                   Python interpreter [python, else python3]
#   EUROFLEX_DRY_RUN                  1 = print the command instead of running it [0]
# Extra command-line arguments are appended to `euroflex live run` unchanged.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python_bin="${EUROFLEX_PYTHON:-$(command -v python || command -v python3 || echo python3)}"

run_dir="${EUROFLEX_RUN_DIR:-artifacts/live/default}"
strategy="${EUROFLEX_STRATEGY:-threshold}"
execution="${EUROFLEX_EXECUTION:-paper}"
models_dir="${EUROFLEX_MODELS_DIR:-}"

die() {
  echo "run_live.sh: $*" >&2
  exit 2
}

case "${execution}" in
  paper | csv) ;;
  *) die "EUROFLEX_EXECUTION=${execution} is not allowed here (paper or csv); see docs/deployment.md" ;;
esac

required_models=()
case "${strategy}" in
  forecast-mpc | forecast_mpc) required_models=(deploy_price_current.pkl) ;;
  prism | scales-l | scales_l)
    required_models=(deploy_prism_current.pkl deploy_prism_zero.pkl deploy_fusion_variance.json)
    ;;
esac

if ((${#required_models[@]} > 0)); then
  [[ -n "${models_dir}" ]] || die "strategy ${strategy} needs fitted models: set EUROFLEX_MODELS_DIR (build them with: python scripts/ops/build_models.py build --out <dir>)"
  for name in "${required_models[@]}"; do
    [[ -f "${models_dir}/${name}" ]] || die "${name} not found in EUROFLEX_MODELS_DIR (build with: python scripts/ops/build_models.py build --out <dir>)"
  done
fi
if [[ -n "${models_dir}" && -d "${models_dir}" && "${EUROFLEX_VERIFY_MODELS:-1}" == "1" ]]; then
  if [[ -f "${models_dir}/MANIFEST.json" ]]; then
    "${python_bin}" "${here}/build_models.py" verify "${models_dir}" >&2 || die "model files do not match ${models_dir}/MANIFEST.json"
  elif ((${#required_models[@]} > 0)); then
    echo "run_live.sh: warning: no MANIFEST.json in the models directory; pickles are not verified" >&2
  fi
fi

if command -v euroflex >/dev/null 2>&1; then
  cmd=(euroflex)
else
  cmd=("${python_bin}" -m euroflex_bess_lab.cli)
fi
cmd+=(
  live run
  --strategy "${strategy}"
  --power-mw "${EUROFLEX_POWER_MW:-1}"
  --energy-mwh "${EUROFLEX_ENERGY_MWH:-2}"
  --mechanism "${EUROFLEX_MECHANISM:-auto}"
  --run-dir "${run_dir}"
  --execution "${execution}"
  --poll-seconds "${EUROFLEX_POLL_SECONDS:-15}"
  --history-days "${EUROFLEX_HISTORY_DAYS:-14}"
  --minute-lag "${EUROFLEX_MINUTE_LAG:-1}"
  --qh-final-lag "${EUROFLEX_QH_FINAL_LAG:-5}"
  --degradation-eur-per-mwh "${EUROFLEX_DEGRADATION_EUR_PER_MWH:-8}"
  --replan-every "${EUROFLEX_REPLAN_EVERY:-never}"
)
if [[ "${EUROFLEX_PUBLIC_HISTORY:-1}" == "1" ]]; then
  cmd+=(--public-history)
else
  cmd+=(--no-public-history)
fi
if [[ "${EUROFLEX_VERBOSE:-0}" == "1" ]]; then
  cmd+=(--verbose)
fi
if [[ -n "${EUROFLEX_STRATEGY_PARAMS:-}" ]]; then
  read -r -a params <<<"${EUROFLEX_STRATEGY_PARAMS}"
  for param in "${params[@]}"; do
    cmd+=(--param "${param}")
  done
fi
cmd+=("$@")

if [[ "${EUROFLEX_DRY_RUN:-0}" == "1" ]]; then
  printf '%q ' "${cmd[@]}"
  printf '\n'
  exit 0
fi
mkdir -p "$(dirname "${run_dir}")"
echo "run_live.sh: paper trading only (no market submission); run directory ${run_dir}" >&2
exec "${cmd[@]}"
