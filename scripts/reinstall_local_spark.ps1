# Reinstall PySpark in YOUR venv (not Windows Store python). Run from anywhere.
# Usage: .\reinstall_local_spark.ps1
#    or: C:\spark-dev\.venv\Scripts\Activate.ps1; .\reinstall_local_spark.ps1

$ErrorActionPreference = "Continue"

function Find-VenvPython {
    if ($env:VIRTUAL_ENV) {
        $p = Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"
        if (Test-Path $p) { return $p }
    }
    $dir = Split-Path -Parent $PSScriptRoot
    while ($dir) {
        $p = Join-Path $dir ".venv\Scripts\python.exe"
        if (Test-Path $p) { return $p }
        $parent = Split-Path -Parent $dir
        if (-not $parent -or $parent -eq $dir) { break }
        $dir = $parent
    }
    return $null
}

$py = Find-VenvPython
if (-not $py) {
    Write-Host "ERROR: No .venv found. Create one first, e.g.:" -ForegroundColor Red
    Write-Host "  cd C:\spark-dev"
    Write-Host "  py -3.12 -m venv .venv"
    Write-Host "  .\.venv\Scripts\Activate.ps1"
    Write-Host "  pip install -r fabric\fabric\requirements-local-spark.txt"
    exit 1
}

Write-Host "Using Python:" $py
& $py -c "import sys; print('  version:', sys.version.split()[0])"

Write-Host "Removing old PySpark packages..."
& $py -m pip uninstall -y pyspark delta-spark py4j | Out-Null

Write-Host "Purging pip cache..."
& $py -m pip cache purge | Out-Null

Write-Host "Installing pyspark==3.5.4 delta-spark==3.2.0..."
& $py -m pip install pyspark==3.5.4 delta-spark==3.2.0 py4j==0.10.9.7
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Remove-Item Env:SPARK_HOME -ErrorAction SilentlyContinue

$verify = Join-Path $PSScriptRoot "verify_local_spark.py"
Write-Host "Running verify_local_spark.py..."
& $py $verify
exit $LASTEXITCODE
