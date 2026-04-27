#!/usr/bin/env bash
# HeTu sandbox bootstrap — install apt packages and set persistent env vars.
#
# This script must run as root inside the sandbox. The sandbox blocks sudo
# and "no new privileges", so run it from your HOST shell, in the project
# directory, piping the script through stdin (avoids any cross-platform
# path mapping):
#
#   PowerShell（用 cmd /c 避免 PS 把 LF 转 CRLF）:
#       cmd /c "type agent_install.sh | sbx exec -u root -i claude-HeTu bash"
#   cmd.exe:
#       type agent_install.sh | sbx exec -u root -i claude-HeTu bash
#   Bash / zsh / Git Bash:
#       cat agent_install.sh | sbx exec -u root -i claude-HeTu bash
#
# Replace "claude-HeTu" with your sandbox name if different (see `sbx ls`).
# Re-running is safe and cheap — package install is gated by a marker file,
# env vars are rewritten every time so they stay in sync with this script.

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    cat >&2 <<'EOF'
[agent_install] must run as root. From your host:
    sbx exec -u root <sandbox-name> bash /c/xsoft/HeTu/agent_install.sh
EOF
    exit 1
fi

PERSIST=/etc/sandbox-persistent.sh
MARKER=/var/lib/hetu-agent-install.done
BEGIN='# >>> HeTu agent_install (managed) >>>'
END='# <<< HeTu agent_install (managed) <<<'

# --- 1. Persistent env vars (always refreshed) ---
touch "$PERSIST"
if grep -qF "$BEGIN" "$PERSIST"; then
    sed -i "\|$BEGIN|,\|$END|d" "$PERSIST"
fi
{
    echo "$BEGIN"
    echo 'export UV_PROJECT_ENVIRONMENT=/tmp/hetu-venv-sandbox'
    echo 'export LANG=en_US.UTF-8'
    echo 'export LC_ALL=en_US.UTF-8'
    echo 'export LANGUAGE=en_US:en'
    echo "$END"
} >> "$PERSIST"
chmod 644 "$PERSIST"
echo "[agent_install] env vars written to $PERSIST"

# --- 2. Disable Claude Code's per-Bash sandbox (always refreshed) ---
# We're already inside an isolated sbx VM, so Claude's inner sandbox
# (NoNewPrivs + userns remap) is redundant and breaks sudo / apt.
SETTINGS=/home/agent/.claude/settings.json
mkdir -p "$(dirname "$SETTINGS")"
python3 - "$SETTINGS" <<'PY'
import json, os, sys
p = sys.argv[1]
data = {}
if os.path.exists(p) and os.path.getsize(p) > 0:
    with open(p) as f:
        data = json.load(f)
data.setdefault("sandbox", {})["enabled"] = False
with open(p, "w") as f:
    json.dump(data, f, indent=2)
PY
chown agent:agent "$(dirname "$SETTINGS")" "$SETTINGS"
echo "[agent_install] Claude sandbox disabled in $SETTINGS"

# --- 3. apt packages (skipped if marker exists) ---
if [[ -f "$MARKER" ]]; then
    echo "[agent_install] packages already installed (marker: $MARKER), skipping."
    exit 0
fi

export DEBIAN_FRONTEND=noninteractive

# .NET SDK 10 ships from packages.microsoft.com, not Debian/Ubuntu defaults.
if [[ ! -f /etc/apt/sources.list.d/microsoft-prod.list ]] && \
   [[ ! -f /etc/apt/sources.list.d/microsoft-prod.sources ]]; then
    apt-get update
    apt-get install -y --no-install-recommends ca-certificates curl gnupg
    . /etc/os-release
    curl -fsSL "https://packages.microsoft.com/config/${ID}/${VERSION_ID}/packages-microsoft-prod.deb" \
        -o /tmp/ms-prod.deb
    dpkg -i /tmp/ms-prod.deb
    rm /tmp/ms-prod.deb
fi

apt-get update
apt-get install -y --no-install-recommends \
    locales \
    bzr \
    dotnet-sdk-10.0 \
    fd-find \
    file \
    gettext \
    git-lfs \
    vim \
    inotify-tools \
    iputils-ping \
    moreutils \
    netcat-openbsd \
    openssh-client \
    sqlite3 \
    tzdata \
    uuid-dev \
    wget \
    xz-utils \
    zip
rm -rf /var/lib/apt/lists/*

# install utf8
sudo locale-gen en_US.UTF-8
sudo update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8

# update uv
curl -LsSf https://astral.sh/uv/install.sh | sh

mkdir -p "$(dirname "$MARKER")"
touch "$MARKER"
echo "[agent_install] done."
