param(
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Compose = Join-Path $Root "docker-compose.yml"

Push-Location $Root
try {
    docker compose -f $Compose config --quiet
    if (-not $SkipBuild) {
        docker compose -f $Compose build mcp browser
    }
    docker compose -f $Compose run --rm --no-deps mcp `
        python3 -m scientist_literature_mcp.publisher_policy `
        --config /config/publishers.json
    docker compose -f $Compose up -d flaresolverr browser monitor
    docker compose -f $Compose ps
} finally {
    Pop-Location
}
