param(
    [string]$Name = "scientist-literature"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Compose = Join-Path $Root "docker-compose.yml"

if (-not (Get-Command codex -ErrorAction SilentlyContinue)) {
    throw "codex is not available on PATH"
}
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "docker is not available on PATH"
}

codex mcp add $Name -- docker compose -f $Compose run --rm --no-deps -T mcp
Write-Host "Registered MCP server '$Name'. Restart Codex, then inspect it with /mcp."
Write-Host "For long paper reads, apply the timeout settings from codex.config.example.toml."
