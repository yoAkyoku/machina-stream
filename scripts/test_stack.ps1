[CmdletBinding()]
param(
    [switch]$SkipRecoveryTests,
    [switch]$SkipBrowserTests,
    [string]$ProjectName
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $false)][string[]]$ArgumentList = @()
    )

    & $FilePath @ArgumentList
    if ($LASTEXITCODE -ne 0) {
        throw "$FilePath $($ArgumentList -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Invoke-Compose {
    param([Parameter(Mandatory = $true)][string[]]$ArgumentList)

    $fileArguments = @()
    foreach ($composeFile in $script:composeFiles) {
        $fileArguments += @("-f", $composeFile)
    }
    Invoke-Checked -FilePath "docker" -ArgumentList (@("compose") + $fileArguments + @("-p", $script:composeProject) + $ArgumentList)
}

function Get-FreeLoopbackPort {
    $listener = [System.Net.Sockets.TcpListener]::new(
        [System.Net.IPAddress]::Loopback,
        0
    )
    try {
        $listener.Start()
        return ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port
    }
    finally {
        $listener.Stop()
    }
}

function Set-ProcessEnvironment {
    param([Parameter(Mandatory = $true)][string]$Name, [Parameter(Mandatory = $true)][string]$Value)

    $script:previousEnvironment[$Name] = [Environment]::GetEnvironmentVariable($Name, "Process")
    [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
}

$script:previousEnvironment = @{}
$script:composeProject = if ($ProjectName) {
    $ProjectName
}
else {
    "machina-stream-e2e-$([DateTime]::UtcNow.ToString('yyyyMMddHHmmss'))-$([Guid]::NewGuid().ToString('N').Substring(0, 6))"
}
$script:composeFiles = @("compose.yaml")
if ($env:MACHINA_REGISTRY_CA_FILE) {
    $script:composeFiles += "compose.registry-ca.yaml"
}
$stackStarted = $false
$artifactDir = Join-Path $repoRoot "artifacts\compose\$script:composeProject"

try {
    foreach ($command in @("docker", "node", "pnpm", "uv")) {
        if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
            throw "Required command '$command' was not found on PATH."
        }
    }

    Invoke-Checked -FilePath "docker" -ArgumentList @("compose", "version")
    $dockerOs = (& docker info --format "{{.OSType}}")
    if ($LASTEXITCODE -ne 0 -or $dockerOs.Trim() -ne "linux") {
        throw "Docker Desktop Linux engine is required; docker info reported '$dockerOs'."
    }

    Invoke-Checked -FilePath "uv" -ArgumentList @("sync", "--locked", "--extra", "dev")
    Invoke-Checked -FilePath "uv" -ArgumentList @("run", "python", "--version")
    Invoke-Checked -FilePath "uv" -ArgumentList @("run", "python", "scripts/check_compose_security.py")
    Invoke-Checked -FilePath "uv" -ArgumentList @("run", "ruff", "check", "backend", "tests")
    Invoke-Checked -FilePath "uv" -ArgumentList @("run", "ruff", "format", "--check", "backend", "tests")
    Invoke-Checked -FilePath "uv" -ArgumentList @("run", "pytest", "tests/unit", "--strict-markers", "-q")
    Invoke-Checked -FilePath "pnpm" -ArgumentList @("--dir", "web", "install", "--frozen-lockfile")
    Invoke-Checked -FilePath "pnpm" -ArgumentList @("--dir", "web", "lint")
    Invoke-Checked -FilePath "pnpm" -ArgumentList @("--dir", "web", "typecheck")

    Set-ProcessEnvironment -Name "MACHINA_E2E_PROJECT" -Value $script:composeProject
    Set-ProcessEnvironment -Name "API_HOST_PORT" -Value ([string](Get-FreeLoopbackPort))
    Set-ProcessEnvironment -Name "POSTGRES_HOST_PORT" -Value ([string](Get-FreeLoopbackPort))
    Set-ProcessEnvironment -Name "MACHINA_API_URL" -Value "http://127.0.0.1:$env:API_HOST_PORT"
    Set-ProcessEnvironment -Name "MACHINA_WEB_URL" -Value "http://127.0.0.1:3001"
    Set-ProcessEnvironment -Name "SIMULATOR_MACHINE_COUNT" -Value "1"
    Set-ProcessEnvironment -Name "SIMULATOR_EVENT_INTERVAL_SECONDS" -Value "1"

    Write-Host "Starting isolated Compose project $script:composeProject (API $env:API_HOST_PORT, PostgreSQL $env:POSTGRES_HOST_PORT)."
    $stackStarted = $true
    Invoke-Compose -ArgumentList @(
        "up", "-d", "--build", "--wait", "--wait-timeout", "240",
        "postgres", "kafka", "kafka-init", "api", "processor", "web", "prometheus", "grafana"
    )
    Invoke-Checked -FilePath "uv" -ArgumentList @(
        "run", "pytest", "tests/unit", "tests/integration", "--strict-markers",
        "-m", "integration and not recovery", "-q"
    )

    if (-not $SkipBrowserTests) {
        $playwrightCli = Join-Path $repoRoot "web\node_modules\.bin\playwright.cmd"
        Invoke-Checked -FilePath $playwrightCli -ArgumentList @("install", "chromium")
        Invoke-Checked -FilePath "pnpm" -ArgumentList @("--dir", "web", "test:e2e")
    }

    if (-not $SkipRecoveryTests) {
        Invoke-Checked -FilePath "uv" -ArgumentList @(
            "run", "pytest", "tests/integration", "--strict-markers", "-m", "recovery", "-q"
        )
    }

    Write-Host "Compose, integration, browser, and requested recovery checks passed."
}
finally {
    if ($stackStarted) {
        New-Item -ItemType Directory -Force -Path $artifactDir | Out-Null
        try { Invoke-Compose -ArgumentList @("ps") *>&1 | Set-Content (Join-Path $artifactDir "ps.txt") } catch { $_ | Set-Content (Join-Path $artifactDir "ps-error.txt") }
        try { Invoke-Compose -ArgumentList @("config", "--no-interpolate") *>&1 | Set-Content (Join-Path $artifactDir "config.yaml") } catch { $_ | Set-Content (Join-Path $artifactDir "config-error.txt") }
        try { Invoke-Compose -ArgumentList @("logs", "--no-color") *>&1 | Set-Content (Join-Path $artifactDir "logs.txt") } catch { $_ | Set-Content (Join-Path $artifactDir "logs-error.txt") }
        try { Invoke-Compose -ArgumentList @("down", "--volumes", "--remove-orphans") } catch { Write-Warning "Compose cleanup failed: $($_.Exception.Message)" }
    }

    foreach ($entry in $script:previousEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, "Process")
    }
}
