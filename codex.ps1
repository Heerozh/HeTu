
.\.venv\Scripts\activate.ps1
chcp 65001

# set CODEX_HOME to current project directory .codex
$env:CODEX_HOME = Join-Path -Path (Get-Location) -ChildPath ".codex"

codex
