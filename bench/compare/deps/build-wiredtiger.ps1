# Builds WiredTiger 11.3.1 with MinGW gcc (MSYS2 UCRT64) and installs it into
# deps/install/wiredtiger (include/ and lib/), where the benchmark's cgo code
# looks for it. Needs: MSYS2 with
#   pacman -S mingw-w64-ucrt-x86_64-gcc mingw-w64-ucrt-x86_64-cmake mingw-w64-ucrt-x86_64-ninja mingw-w64-ucrt-x86_64-python
#
#   powershell -ExecutionPolicy Bypass -File bench\compare\deps\build-wiredtiger.ps1
# Native tools (tar, cmake, cargo) print progress on stderr, which PowerShell 5.1
# would turn into errors under "Stop": check their exit codes instead.
$ErrorActionPreference = "Continue"
$version = "11.3.1"
$deps = $PSScriptRoot
$work = Join-Path $env:TEMP "basedb-wiredtiger-build"
$install = Join-Path $deps "install\wiredtiger"
$env:PATH = "C:\msys64\ucrt64\bin;$env:PATH"

if (Test-Path $work) { Remove-Item -Recurse -Force $work }
New-Item -ItemType Directory -Force $work | Out-Null
Invoke-WebRequest "https://github.com/wiredtiger/wiredtiger/archive/refs/tags/$version.tar.gz" -OutFile "$work\wt.tar.gz" -ErrorAction Stop
# A few files in the archive are symlinks that Windows cannot create; they are documentation only.
tar -xzf "$work\wt.tar.gz" -C $work 2>$null
$src = Join-Path $work "wiredtiger-$version"
if (-not (Test-Path "$src\CMakeLists.txt")) { throw "extracting WiredTiger failed" }

python (Join-Path $deps "patch_wiredtiger.py") $src
if ($LASTEXITCODE -ne 0) { throw "patching failed" }

cmake -S $src -B "$work\build" -G Ninja -DCMAKE_C_COMPILER=gcc -DCMAKE_BUILD_TYPE=Release `
    -DENABLE_PYTHON=OFF -DHAVE_UNITTEST=OFF -DENABLE_STRICT=OFF `
    -DENABLE_LZ4=OFF -DENABLE_SNAPPY=OFF -DENABLE_ZLIB=OFF -DENABLE_ZSTD=OFF `
    "-DCMAKE_C_FLAGS=-Wno-incompatible-pointer-types"
if ($LASTEXITCODE -ne 0) { throw "cmake configure failed" }
ninja -C "$work\build" wiredtiger_static
if ($LASTEXITCODE -ne 0) { throw "build failed" }

New-Item -ItemType Directory -Force "$install\include", "$install\lib" | Out-Null
Copy-Item "$work\build\include\wiredtiger.h" "$install\include\" -Force -ErrorAction Stop
Copy-Item "$work\build\libwiredtiger.a" "$install\lib\" -Force -ErrorAction Stop
Write-Host "installed WiredTiger $version into $install"
