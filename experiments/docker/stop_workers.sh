#!/usr/bin/env bash
set -euo pipefail

mapfile -t CONTAINERS < <(
  docker ps -q \
    --filter label=cqdagpcfg.project=parallel \
    --filter label=cqdagpcfg.role=worker
)

if [[ "${#CONTAINERS[@]}" -eq 0 ]]; then
  echo "no CQDAGPCFG worker containers are running"
  exit 0
fi

docker stop "${CONTAINERS[@]}"
