#!/usr/bin/env bash
set -euo pipefail

current_dir="$(pwd)"
dir_name="$(basename "$current_dir")"
image_name="${CODEX_IMAGE:-heerozh_codex}"
dind_image="${DIND_IMAGE:-docker:27-dind}"
dind_name="${DIND_NAME:-codex-dind}"
dind_run_volume="${DIND_RUN_VOLUME:-${dind_name}-run}"
editor="${EDITOR:-${VISUAL:-vim}}"

# 如果命令是./xxx.sh build，才执行
if [[ "${1:-}" == "build" ]]; then
  docker build -f codex_docker -t "${image_name}" .
fi

cleanup() {
  docker rm -f "${dind_name}" >/dev/null 2>&1 || true
  docker volume rm "${dind_run_volume}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

docker rm -f "${dind_name}" >/dev/null 2>&1 || true
docker volume rm "${dind_run_volume}" >/dev/null 2>&1 || true
docker volume create "${dind_run_volume}" >/dev/null

docker run -d --rm --privileged \
  --name "${dind_name}" \
  -e DOCKER_TLS_CERTDIR= \
  -e DOCKER_DRIVER=overlay2 \
  -v "${dind_run_volume}:/var/run" \
  "${dind_image}" \
  --host=tcp://0.0.0.0:2375 \
  --host=unix:///var/run/docker.sock >/dev/null

printf "Starting DinD container '%s', about 15s...\n" "${dind_name}"
ready=0
for _ in $(seq 1 60); do
  if docker exec "${dind_name}" docker version >/dev/null 2>&1; then
    ready=1
    break
  fi
  printf "Waiting for DinD daemon to be ready...\n"
  sleep 1
done

if [[ "${ready}" -ne 1 ]]; then
  docker logs "${dind_name}" >&2 || true
  echo "DinD daemon did not become ready within 60s" >&2
  exit 1
fi

printf "DinD daemon is ready. Starting Main container...\n"
docker run --rm -it \
  --network "container:${dind_name}" \
  -e UV_PROJECT_ENVIRONMENT="/workspace/${dir_name}/.venv-docker" \
  -e UV_CACHE_DIR=/tmp/uv-cache \
  -e EDITOR="${editor}" \
  -e VISUAL="${editor}" \
  -e editor="${editor}" \
  -e LANG=C.UTF-8 \
  -e LC_ALL=C.UTF-8 \
  -e DOCKER_TLS_CERTDIR= \
  -v "${dind_run_volume}:/var/run" \
  -v "$(pwd):/workspace/${dir_name}" \
  -w "/workspace/${dir_name}" \
  -v "${HOME}/.codex:/root/.codex" \
  "${image_name}"
