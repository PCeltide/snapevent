#requires -Version 5
<#
.SYNOPSIS
    Pre-commit / CI gate for snapevent.
.DESCRIPTION
    Runs the same three checks that define the Phase-1 "done" criteria:
      1. cargo fmt  --check         (formatting)
      2. cargo clippy -D warnings   (lint; warnings are errors)
      3. cargo test --all           (tests)
    Exits non-zero on the first failure. Resolves paths relative to the repo
    root, so it can be run from any directory.
.EXAMPLE
    powershell -File scripts/check.ps1
#>
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host '==> cargo fmt --all -- --check'
cargo fmt --all -- --check
if ($LASTEXITCODE -ne 0) { Write-Error 'rustfmt check failed'; exit 1 }

Write-Host '==> cargo clippy --all-targets -- -D warnings'
cargo clippy --all-targets -- -D warnings
if ($LASTEXITCODE -ne 0) { Write-Error 'clippy failed'; exit 1 }

Write-Host '==> cargo test --all'
cargo test --all
if ($LASTEXITCODE -ne 0) { Write-Error 'tests failed'; exit 1 }

Write-Host ''
Write-Host 'All checks passed.' -ForegroundColor Green
