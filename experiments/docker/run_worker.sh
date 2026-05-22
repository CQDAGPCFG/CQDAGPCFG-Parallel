#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKSPACE_ROOT="$(cd "${REPO_ROOT}/.." && pwd)"
CQDAGPCFG_ROOT="${CQDAGPCFG_ROOT:-${WORKSPACE_ROOT}/CQDAGPCFG}"

IMAGE="${CQPCFG_WORKER_IMAGE:-cqdagpcfg-worker:local}"
TRACKER_HOST="${TRACKER_HOST:-host.docker.internal}"
CQPCFG_PORT="${CQPCFG_PORT:-5555}"
ARTIFACT_VOLUME="${CQPCFG_WORKER_VOLUME:-docker_artifacts}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-docker}"
WORKER_NETWORK="${CQPCFG_WORKER_NETWORK:-${COMPOSE_PROJECT_NAME}_default}"
CONTAINER_NAME="${CQPCFG_WORKER_NAME:-cqpcfg-worker-$(date +%Y%m%d%H%M%S)-${RANDOM}}"

JOB_BOOTSTRAP_TIMEOUT_SECONDS="${JOB_BOOTSTRAP_TIMEOUT_SECONDS:-60}"
ROLE_REPLY_TIMEOUT_MS="${ROLE_REPLY_TIMEOUT_MS:-1000}"
METRICS_FLUSH_INTERVAL_SECONDS="${METRICS_FLUSH_INTERVAL_SECONDS:-0.25}"
CQPCFG_MODEL_JSON_PAGE_CACHE="${CQPCFG_MODEL_JSON_PAGE_CACHE:-128}"
CQPCFG_HASH_DELAY_SECONDS="${CQPCFG_HASH_DELAY_SECONDS:-0}"
CQPCFG_WORK_DELAY_SECONDS="${CQPCFG_WORK_DELAY_SECONDS:-0}"
CQPCFG_MIN_PASSWORD_LENGTH="${CQPCFG_MIN_PASSWORD_LENGTH:-0}"
CQPCFG_MAX_PASSWORD_LENGTH="${CQPCFG_MAX_PASSWORD_LENGTH:-0}"
CQPCFG_NODE_ID="${CQPCFG_NODE_ID:-${CONTAINER_NAME}}"

CQPCFG_WORKER_CPUS="${CQPCFG_WORKER_CPUS:-}"
CQPCFG_WORKER_CPUSET_CPUS="${CQPCFG_WORKER_CPUSET_CPUS:-}"
CQPCFG_WORKER_MEMORY="${CQPCFG_WORKER_MEMORY:-}"
CQPCFG_WORKER_MEMORY_RESERVATION="${CQPCFG_WORKER_MEMORY_RESERVATION:-}"
CQPCFG_WORKER_MEMORY_SWAP="${CQPCFG_WORKER_MEMORY_SWAP:-}"
CQPCFG_WORKER_SHM_SIZE="${CQPCFG_WORKER_SHM_SIZE:-}"
CQPCFG_WORKER_PIDS_LIMIT="${CQPCFG_WORKER_PIDS_LIMIT:-}"
CQPCFG_WORKER_GPUS="${CQPCFG_WORKER_GPUS:-}"
CQPCFG_RESOURCE_CPUS="${CQPCFG_RESOURCE_CPUS:-${CQPCFG_WORKER_CPUS}}"
CQPCFG_RESOURCE_MEMORY="${CQPCFG_RESOURCE_MEMORY:-${CQPCFG_WORKER_MEMORY}}"
CQPCFG_RESOURCE_GPUS="${CQPCFG_RESOURCE_GPUS:-}"
CQPCFG_RESOURCE_GPU_MEMORY="${CQPCFG_RESOURCE_GPU_MEMORY:-}"

if [[ ! -d "${CQDAGPCFG_ROOT}" ]]; then
  echo "CQDAGPCFG root not found: ${CQDAGPCFG_ROOT}" >&2
  echo "Set CQDAGPCFG_ROOT=/path/to/CQDAGPCFG and retry." >&2
  exit 1
fi

if [[ "${CQPCFG_FORCE_BUILD:-0}" == "1" ]] || ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
  docker build \
    -f "${SCRIPT_DIR}/Dockerfile" \
    -t "${IMAGE}" \
    "${REPO_ROOT}" >/dev/null
fi

docker volume create "${ARTIFACT_VOLUME}" >/dev/null

docker_args=(
  run
  -d
  --rm
  --name "${CONTAINER_NAME}"
  --hostname "${CONTAINER_NAME}"
  --label cqdagpcfg.project=parallel
  --label cqdagpcfg.role=worker
  --add-host host.docker.internal:host-gateway
)

if [[ -n "${WORKER_NETWORK}" ]]; then
  if docker network inspect "${WORKER_NETWORK}" >/dev/null 2>&1; then
    docker_args+=(--network "${WORKER_NETWORK}")
  else
    echo "worker network not found, falling back to Docker default bridge: ${WORKER_NETWORK}" >&2
  fi
fi

if [[ -n "${CQPCFG_WORKER_CPUS}" ]]; then
  docker_args+=(--cpus "${CQPCFG_WORKER_CPUS}")
fi
if [[ -n "${CQPCFG_WORKER_CPUSET_CPUS}" ]]; then
  docker_args+=(--cpuset-cpus "${CQPCFG_WORKER_CPUSET_CPUS}")
fi
if [[ -n "${CQPCFG_WORKER_MEMORY}" ]]; then
  docker_args+=(--memory "${CQPCFG_WORKER_MEMORY}")
fi
if [[ -n "${CQPCFG_WORKER_MEMORY_RESERVATION}" ]]; then
  docker_args+=(--memory-reservation "${CQPCFG_WORKER_MEMORY_RESERVATION}")
fi
if [[ -n "${CQPCFG_WORKER_MEMORY_SWAP}" ]]; then
  docker_args+=(--memory-swap "${CQPCFG_WORKER_MEMORY_SWAP}")
fi
if [[ -n "${CQPCFG_WORKER_SHM_SIZE}" ]]; then
  docker_args+=(--shm-size "${CQPCFG_WORKER_SHM_SIZE}")
fi
if [[ -n "${CQPCFG_WORKER_PIDS_LIMIT}" ]]; then
  docker_args+=(--pids-limit "${CQPCFG_WORKER_PIDS_LIMIT}")
fi
if [[ -n "${CQPCFG_WORKER_GPUS}" ]]; then
  docker_args+=(--gpus "${CQPCFG_WORKER_GPUS}")
fi

docker_args+=(
  -v "${REPO_ROOT}:/workspace/CQDAGPCFG-Parallel:ro"
  -v "${CQDAGPCFG_ROOT}:/workspace/CQDAGPCFG:ro"
  -v "${ARTIFACT_VOLUME}:/artifacts"
  -e "CQPCFG_NODE_ID=${CQPCFG_NODE_ID}"
  -e "CQPCFG_CONNECT=cqpcfg://${TRACKER_HOST}:${CQPCFG_PORT}"
  -e "CQPCFG_MODEL_CACHE_DIR=/artifacts/model-cache"
  -e "CQPCFG_METRICS_DIR=/artifacts/metrics"
  -e "CQPCFG_OUTPUTS_DIR=/artifacts/hits"
  -e "CQPCFG_JOB_BOOTSTRAP_TIMEOUT_SECONDS=${JOB_BOOTSTRAP_TIMEOUT_SECONDS}"
  -e "CQPCFG_ROLE_REPLY_TIMEOUT_MS=${ROLE_REPLY_TIMEOUT_MS}"
  -e "CQPCFG_METRICS_FLUSH_INTERVAL_SECONDS=${METRICS_FLUSH_INTERVAL_SECONDS}"
  -e "CQPCFG_MODEL_JSON_PAGE_CACHE=${CQPCFG_MODEL_JSON_PAGE_CACHE}"
  -e "CQPCFG_HASH_DELAY_SECONDS=${CQPCFG_HASH_DELAY_SECONDS}"
  -e "CQPCFG_WORK_DELAY_SECONDS=${CQPCFG_WORK_DELAY_SECONDS}"
  -e "CQPCFG_MIN_PASSWORD_LENGTH=${CQPCFG_MIN_PASSWORD_LENGTH}"
  -e "CQPCFG_MAX_PASSWORD_LENGTH=${CQPCFG_MAX_PASSWORD_LENGTH}"
)

if [[ -n "${CQPCFG_RESOURCE_CPUS}" ]]; then
  docker_args+=(-e "CQPCFG_RESOURCE_CPUS=${CQPCFG_RESOURCE_CPUS}")
fi
if [[ -n "${CQPCFG_RESOURCE_MEMORY}" ]]; then
  docker_args+=(-e "CQPCFG_RESOURCE_MEMORY=${CQPCFG_RESOURCE_MEMORY}")
fi
if [[ -n "${CQPCFG_RESOURCE_GPUS}" ]]; then
  docker_args+=(-e "CQPCFG_RESOURCE_GPUS=${CQPCFG_RESOURCE_GPUS}")
fi
if [[ -n "${CQPCFG_RESOURCE_GPU_MEMORY}" ]]; then
  docker_args+=(-e "CQPCFG_RESOURCE_GPU_MEMORY=${CQPCFG_RESOURCE_GPU_MEMORY}")
fi

docker_args+=(
  "${IMAGE}"
  python experiments/cqpcfg_experiment.py worker
)

container_id="$(docker "${docker_args[@]}")"

echo "started worker container"
echo "  name: ${CONTAINER_NAME}"
echo "  id  : ${container_id}"
echo "  cpu : ${CQPCFG_WORKER_CPUS:-unlimited}"
echo "  mem : ${CQPCFG_WORKER_MEMORY:-unlimited}"
echo "  gpu : ${CQPCFG_WORKER_GPUS:-none}"
echo "  net : ${WORKER_NETWORK:-bridge}"
echo "  logs: docker logs -f ${CONTAINER_NAME}"
