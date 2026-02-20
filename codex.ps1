$currentDir = Get-Location  # 或使用 $PWD
$dirName = Split-Path $currentDir -Leaf

docker build -f codex_docker -t heerozh_codex .
docker run --rm -it `
    -e UV_PROJECT_ENVIRONMENT="/workspace/${dirName}/.venv-docker" `
    -e UV_CACHE_DIR=/tmp/uv-cache `
    -v ${PWD}:/workspace/${dirName} `
    -w /workspace/${dirName} `
    -v ${HOME}/.codex:/root/.codex `
    heerozh_codex
