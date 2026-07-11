# Python gate: ruff + pytest for python/kdp-data. Mirrors scripts/check.ps1's
# role for the Rust workspace; run before every commit touching python/.
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..\python")
uv run ruff check .
if ($LASTEXITCODE -ne 0) { exit 1 }
uv run pytest -q
exit $LASTEXITCODE
