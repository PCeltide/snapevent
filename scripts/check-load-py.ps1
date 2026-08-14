# Gate for the kdp-load-py bindings (crates/kdp-load-py — OUTSIDE the cargo
# workspace, so scripts/check.ps1 does not cover it). Builds the extension
# into python/'s venv via maturin, then runs its pytest smoke.
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
Set-Location (Join-Path $repo 'python')

uv sync | Out-Null
uv pip install --quiet 'maturin>=1.7,<2'
if ($LASTEXITCODE -ne 0) { Write-Error 'maturin install failed'; exit 1 }

uv run maturin develop --uv -m ../crates/kdp-load-py/Cargo.toml
if ($LASTEXITCODE -ne 0) { Write-Error 'maturin develop failed'; exit 1 }

uv run pytest ../crates/kdp-load-py/tests -q
if ($LASTEXITCODE -ne 0) { Write-Error 'kdp-load-py tests failed'; exit 1 }

Write-Host 'kdp-load-py checks passed.'
