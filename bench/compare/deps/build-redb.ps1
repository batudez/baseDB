# Builds the redb wrapper (deps/redb, a Rust cdylib) and installs
# baseredb.dll into deps/install/redb, where the benchmark loads it.
# Needs the Rust toolchain with the GNU target:
#   rustup-init.exe --default-host x86_64-pc-windows-gnu
#
#   powershell -ExecutionPolicy Bypass -File bench\compare\deps\build-redb.ps1
# Native tools (tar, cmake, cargo) print progress on stderr, which PowerShell 5.1
# would turn into errors under "Stop": check their exit codes instead.
$ErrorActionPreference = "Continue"
$deps = $PSScriptRoot
$cargo = Join-Path $env:USERPROFILE ".cargo\bin\cargo.exe"
$target = Join-Path $env:TEMP "basedb-redb-target"
$install = Join-Path $deps "install\redb"

& $cargo build --release --manifest-path (Join-Path $deps "redb\Cargo.toml") --target-dir $target
if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }
New-Item -ItemType Directory -Force $install | Out-Null
Copy-Item (Join-Path $target "release\baseredb.dll") $install -Force -ErrorAction Stop
Write-Host "installed baseredb.dll into $install"
