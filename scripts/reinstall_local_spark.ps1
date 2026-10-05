# Delete .venv and recreate it, then install PySpark (cleanest fix for broken packages).
# Usage: .\reinstall_local_spark.ps1
#        $env:MEDALLION_PYTHON = "C:\path\to\python312\python.exe"; .\reinstall_local_spark.ps1
#        .\reinstall_local_spark.ps1 -KeepVenv

param(
    [switch]$KeepVenv
)

$ErrorActionPreference = "Continue"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$Requirements = Join-Path $RepoRoot "requirements-local-spark.txt"

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

    $programFiles = @(
        "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
        "$env:ProgramFiles\Python312\python.exe",
        "$env:ProgramFiles\Python311\python.exe"
    )
    foreach ($p in $programFiles) {
        if (Test-Python312Or311 $p) { return $p }
    }

    if (Test-Path "$env:LOCALAPPDATA\Python") {
        foreach ($dir in Get-ChildItem "$env:LOCALAPPDATA\Python" -Directory -ErrorAction SilentlyContinue) {
            $exe = Join-Path $dir.FullName "python.exe"
            if (Test-Python312Or311 $exe) { return $exe }
        }
    }

    return $null
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
if (-not $basePython) {
    Write-Host "ERROR: No Python 3.11 or 3.12 found." -ForegroundColor Red
    Write-Host ""
    Write-Host "Install from https://www.python.org/downloads/ (3.12.x), enable 'py launcher', OR set:"
    Write-Host '  $env:MEDALLION_PYTHON = "C:\full\path\to\python.exe"'
    Write-Host "Then run this script again."
    Write-Host ""
    Write-Host "If you only have Python 3.14, PySpark 3.5 is not supported — install 3.12 alongside it."
    if (Get-Command py -ErrorAction SilentlyContinue) {
        Write-Host "Installed py versions:"
        & py --list 2>$null
    }
    exit 1
}

$venvRoot = Find-VenvRoot
$venvPath = Join-Path $venvRoot ".venv"
$py = Join-Path $venvPath "Scripts\python.exe"

Write-Host "Base Python:" $basePython
Write-Host "Venv root:" $venvRoot
Write-Host "Requirements:" $Requirements

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

Write-Host "Venv Python:" $py
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
