param(
    [Parameter(Mandatory = $true)]
    [string]$ReviewedBy
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Compose = Join-Path $Root "docker-compose.yml"

docker compose -f $Compose build mcp
docker compose -f $Compose run --rm --no-deps policy-admin `
    python3 -m scientist_literature_mcp.publisher_policy `
    --config /config/publishers.json --approve --reviewed-by $ReviewedBy
