# Recreates .venv with Python 3.12 (PySpark 3.5 does not support 3.14).
# Run from repo root, e.g. C:\spark-dev\CoE_transformation_framework:
#   .\scripts\reinstall_local_spark.ps1

param(
    [switch]$KeepVenv,
    [switch]$SkipAutoInstall
)

$ErrorActionPreference = "Continue"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$Requirements = Join-Path $RepoRoot "requirements-local-spark.txt"

function Refresh-PathEnv {
    $machine = [System.Environment]::GetEnvironmentVariable("Path", "Machine")
    $user = [System.Environment]::GetEnvironmentVariable("Path", "User")
    $env:Path = "$machine;$user"
}

function Test-Python312Or311 {
    param([string]$Exe)
    if (-not (Test-Path $Exe)) { return $false }
    $minor = & $Exe -c "import sys; print(sys.version_info.minor)" 2>$null
    if ($LASTEXITCODE -ne 0) { return $false }
    $major = & $Exe -c "import sys; print(sys.version_info.major)" 2>$null
    return ($major -eq "3" -and ($minor -eq "11" -or $minor -eq "12"))
}

function Find-BasePython {
    if ($env:MEDALLION_PYTHON -and (Test-Path $env:MEDALLION_PYTHON)) {
        if (Test-Python312Or311 $env:MEDALLION_PYTHON) { return $env:MEDALLION_PYTHON }
        Write-Host "WARNING: MEDALLION_PYTHON is not 3.11/3.12; using anyway." -ForegroundColor Yellow
        return $env:MEDALLION_PYTHON
    }

    foreach ($ver in @("3.12", "3.11")) {
        & py "-$ver" -c "import sys" 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            return (& py "-$ver" -c "import sys; print(sys.executable)").Trim()
        }
    }

    foreach ($name in @("python3.12", "python3.11")) {
        $cmd = Get-Command $name -ErrorAction SilentlyContinue
        if ($cmd -and (Test-Python312Or311 $cmd.Source)) { return $cmd.Source }
    }

    $paths = @(
        "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
        "$env:ProgramFiles\Python312\python.exe",
        "$env:ProgramFiles\Python311\python.exe"
    )
    foreach ($p in $paths) {
        if (Test-Python312Or311 $p) { return $p }
    }

    return $null
}

function Install-Python312 {
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
        Write-Host "winget not found. Run: winget install -e --id Python.Python.3.12" -ForegroundColor Yellow
        return $false
    }
    Write-Host "Installing Python 3.12 via winget (alongside your 3.14)..."
    & winget install -e --id Python.Python.3.12 --accept-package-agreements --accept-source-agreements
    if ($LASTEXITCODE -ne 0) { return $false }
    Refresh-PathEnv
    Start-Sleep -Seconds 3
    return $true
}

function Find-VenvRoot {
    $dir = $RepoRoot
    while ($dir) {
        if (Test-Path (Join-Path $dir ".venv")) { return $dir }
        $parent = Split-Path -Parent $dir
        if (-not $parent -or $parent -eq $dir) { break }
        $dir = $parent
    }
    $d = Split-Path -Parent $RepoRoot
    if ($d) { return (Split-Path -Parent $d) }
    return $RepoRoot
}

$basePython = Find-BasePython
if (-not $basePython -and -not $SkipAutoInstall) {
    Install-Python312 | Out-Null
    $basePython = Find-BasePython
}

if (-not $basePython) {
    Write-Host "ERROR: No Python 3.11 or 3.12 found." -ForegroundColor Red
    Write-Host ""
    Write-Host "You only have Python 3.14. PySpark 3.5 needs 3.12 for this venv."
    Write-Host ""
    Write-Host "  winget install -e --id Python.Python.3.12"
    Write-Host '  Close and reopen PowerShell, then: .\reinstall_local_spark.ps1'
    Write-Host '  Or: $env:MEDALLION_PYTHON = "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe"'
    & py --list 2>$null
    exit 1
}

$venvRoot = Find-VenvRoot
$venvPath = Join-Path $venvRoot ".venv"
$py = Join-Path $venvPath "Scripts\python.exe"

Write-Host "Base Python:" $basePython
Write-Host "Venv root:" $venvRoot

if (-not $KeepVenv) {
    if (Test-Path $venvPath) {
        Write-Host "Removing existing .venv..."
        Remove-Item -Recurse -Force $venvPath
    }
    Write-Host "Creating new .venv..."
    & $basePython -m venv $venvPath
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} elseif (-not (Test-Path $py)) {
    Write-Host "ERROR: .venv not found. Run without -KeepVenv." -ForegroundColor Red
    exit 1
}

& $py -m pip install --upgrade pip
& $py -m pip install -r $Requirements
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Remove-Item Env:SPARK_HOME -ErrorAction SilentlyContinue
& $py (Join-Path $PSScriptRoot "verify_local_spark.py")
exit $LASTEXITCODE
