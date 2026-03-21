$ErrorActionPreference = "Stop"

$currentDir = (Get-Location).Path
$dirName = Split-Path -Path $currentDir -Leaf
$imageName = if ($env:AGENT_IMAGE)
{
    $env:AGENT_IMAGE
}
else
{
    "heerozh_agent"
}
$dindImage = if ($env:DIND_IMAGE)
{
    $env:DIND_IMAGE
}
else
{
    "docker:27-dind"
}
$dindName = if ($env:DIND_NAME)
{
    $env:DIND_NAME
}
else
{
    "agent-dind"
}
$dindRunVolume = if ($env:DIND_RUN_VOLUME)
{
    $env:DIND_RUN_VOLUME
}
else
{
    "$dindName-run"
}
$editor = if ($env:EDITOR)
{
    $env:EDITOR
}
elseif ($env:VISUAL)
{
    $env:VISUAL
}
else
{
    "vim"
}
$agentHome = if ($env:HOME)
{
    $env:HOME
}
else
{
    $env:USERPROFILE
}

function Remove-DindResources
{
    docker rm -f $dindName *> $null
    docker volume rm $dindRunVolume *> $null
}

# 如果命令是./xxx.sh build，才执行
if ($args[0] -eq "build")
{
    docker build -f agent_docker -t $imageName .
}

try
{
    Remove-DindResources
    docker volume create $dindRunVolume *> $null

    docker run -d --rm --privileged `
        --name $dindName `
        -e DOCKER_TLS_CERTDIR= `
        -e DOCKER_DRIVER=overlay2 `
        -v "${dindRunVolume}:/var/run" `
        $dindImage `
        --host=tcp://0.0.0.0:2375 `
        --host=unix:///var/run/docker.sock *> $null

    $ready = $false
    for ($i = 0; $i -lt 60; $i++) {
        docker exec $dindName docker version *> $null
        if ($LASTEXITCODE -eq 0)
        {
            $ready = $true
            break
        }
        Write-Host "Waiting for DinD daemon to become ready..."
        Start-Sleep -Seconds 1
    }

    if (-not $ready)
    {
        docker logs $dindName
        throw "DinD daemon did not become ready within 60s"
    }

    docker run --rm -it `
        --security-opt seccomp=unconfined `
        --network "container:$dindName" `
        -e UV_PROJECT_ENVIRONMENT="/workspace/${dirName}/.venv-docker" `
        -e UV_CACHE_DIR=/tmp/uv-cache `
        -e EDITOR="$editor" `
        -e VISUAL="$editor" `
        -e editor="$editor" `
        -e LANG=C.UTF-8 `
        -e LC_ALL=C.UTF-8 `
        -e DOCKER_TLS_CERTDIR= `
        -v "${dindRunVolume}:/var/run" `
        -v "${currentDir}:/workspace/${dirName}" `
        -w "/workspace/${dirName}" `
        -v "${agentHome}/.codex:/root/.codex" `
        -v "${agentHome}/.gemini:/root/.gemini" `
        -v "${agentHome}/.claude:/root/.claude" `
        $imageName
}
finally
{
    Remove-DindResources
}
