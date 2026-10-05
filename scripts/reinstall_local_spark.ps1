# Clean reinstall PySpark + Delta in the active venv (fixes mixed-version ImportError).
$ErrorActionPreference = "Stop"
Write-Host "Python:" (Get-Command python).Source
pip uninstall -y pyspark delta-spark py4j 2>$null
pip cache purge
pip install pyspark==3.5.4 delta-spark==3.2.0
Remove-Item Env:SPARK_HOME -ErrorAction SilentlyContinue
Write-Host "Done. Run: python fabric\fabric\scripts\verify_local_spark.py"
