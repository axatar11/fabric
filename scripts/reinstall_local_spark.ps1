# Delete .venv and recreate it, then install PySpark (cleanest fix for broken packages).
# Usage: .\reinstall_local_spark.ps1
#        .\reinstall_local_spark.ps1 -KeepVenv    # only pip reinstall, no venv delete

param(
    [switch]$KeepVenv
)

$ErrorActionPreference = "Continue"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$Requirements = Join-Path $RepoRoot "requirements-local-spark.txt"

function Find-VenvRoot {
    $dir = $RepoRoot
    while ($dir) {
        if (Test-Path (Join-Path $dir ".venv")) {
            return $dir
        }
        $parent = Split-Path -Parent $dir
        if (-not $parent -or $parent -eq $dir) { break }
        $dir = $parent
    }
    # Default: C:\spark-dev when repo is C:\spark-dev\fabric\fabric
    $d = Split-Path -Parent $RepoRoot
    if ($d) { return (Split-Path -Parent $d) }
    return $RepoRoot
}

function Get-PyVersionArg {
    foreach ($ver in @("3.12", "3.11")) {
        & py "-$ver" -c "import sys" 2>$null
        if ($LASTEXITCODE -eq 0) { return "-$ver" }
    }
    Write-Host "ERROR: Install Python 3.11 or 3.12 (py launcher)." -ForegroundColor Red
    exit 1
}

$venvRoot = Find-VenvRoot
$venvPath = Join-Path $venvRoot ".venv"
$py = Join-Path $venvPath "Scripts\python.exe"
$pyArg = Get-PyVersionArg

Write-Host "Venv root:" $venvRoot
Write-Host "Requirements:" $Requirements

if (-not $KeepVenv) {
    if (Test-Path $venvPath) {
        Write-Host "Removing existing .venv..."
        Remove-Item -Recurse -Force $venvPath
    }
    Write-Host "Creating new venv (py $pyArg)..."
    & py $pyArg -m venv $venvPath
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} elseif (-not (Test-Path $py)) {
    Write-Host "ERROR: .venv not found. Run without -KeepVenv." -ForegroundColor Red
    exit 1
}

Write-Host "Using Python:" $py
& $py -m pip install --upgrade pip
if (-not (Test-Path $Requirements)) {
    Write-Host "ERROR: Missing $Requirements" -ForegroundColor Red
    exit 1
}
Write-Host "Installing from requirements-local-spark.txt..."
& $py -m pip install -r $Requirements
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Remove-Item Env:SPARK_HOME -ErrorAction SilentlyContinue

$verify = Join-Path $PSScriptRoot "verify_local_spark.py"
Write-Host "Running verify_local_spark.py..."
& $py $verify
exit $LASTEXITCODE
