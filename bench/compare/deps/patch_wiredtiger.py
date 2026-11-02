# Patches the WiredTiger 11.3.1 source so it builds with MinGW-w64 gcc
# (MSYS2 UCRT64). WiredTiger supports Windows only with MSVC. The changes
# keep its Windows code (src/os_win: files, threads, fsync) and only stop
# MSVC-specific switches and intrinsics from reaching gcc.
#
#   python patch_wiredtiger.py <wiredtiger source directory>
import sys

root = sys.argv[1]


def patch(rel, pairs):
    p = f"{root}/{rel}"
    s = open(p, encoding="utf-8").read()
    for old, new, count in pairs:
        n = s.count(old)
        if n != count:
            sys.exit(f"{rel}: expected {count} of {old!r}, found {n} (wrong WiredTiger version?)")
        s = s.replace(old, new)
    open(p, "w", encoding="utf-8").write(s)
    print("patched", rel)


# 1. The Windows platform config is all MSVC switches (/Ob1, /GF, ...) and an
#    MSVC spinlock. For gcc: gcc atomics, a static library, no switches.
open(f"{root}/cmake/configs/x86/windows/config.cmake", "w", encoding="utf-8").write(
    """set(WT_ARCH "x86" CACHE STRING "")
set(WT_OS "windows" CACHE STRING "")
set(WT_POSIX OFF CACHE BOOL "")
# MinGW gcc (baseDB benchmark): gcc atomics instead of MSVC intrinsics.
set(SPINLOCK_TYPE "gcc" CACHE STRING "" FORCE)
set(ENABLE_STATIC ON CACHE BOOL "" FORCE)
set(ENABLE_SHARED OFF CACHE BOOL "" FORCE)
set(WITH_PIC ON CACHE BOOL "" FORCE)
""")
print("patched cmake/configs/x86/windows/config.cmake")

# 2. /O2, /Od and /MT, /MD are MSVC switches: only for MSVC.
patch("cmake/configs/base.cmake", [
    ('    if(WT_WIN)\n        set(default_optimize_level "/O2")',
     '    if(MSVC)\n        set(default_optimize_level "/O2")', 1),
    ('    if(WT_WIN)\n        set(default_optimize_level "/Od")',
     '    if(MSVC)\n        set(default_optimize_level "/Od")', 1),
])
p = f"{root}/cmake/configs/base.cmake"
s = open(p, encoding="utf-8").read()
i = s.index("add_compile_options(/MT)")
j = s.rindex("if(WT_WIN)", 0, i)
open(p, "w", encoding="utf-8").write(s[:j] + "if(MSVC)" + s[j + len("if(WT_WIN)"):])
print("patched cmake/configs/base.cmake (CRT switches)")

# 3. MinGW already has ssize_t and struct timespec.
patch("src/include/os_windows.h", [
    ("#if _MSC_VER < 1900\n", "#if defined(_MSC_VER) && _MSC_VER < 1900\n", 1),
    ("#ifndef HAVE_SSIZE_T\n", "#if !defined(HAVE_SSIZE_T) && !defined(__MINGW32__)\n", 1),
])

# 4. MinGW gcc defines _M_AMD64 too, so both the gcc (inline asm) and the
#    MSVC (intrinsics) CRC32C code were compiled. Keep the gcc one.
patch("src/checksum/x86/crc32-x86.c", [
    ("#if defined(_M_AMD64)\n", "#if defined(_M_AMD64) && !defined(__GNUC__)\n", 2),
])

# 5. The "alternate checksum" only accepts pages written by an old MSVC build
#    with a CRC bug. A database made by this build never has one: no match.
patch("src/checksum/x86/crc32-x86-alt.c", [
    ("#if defined(_M_AMD64) && !defined(HAVE_NO_CRC32_HARDWARE)",
     "#if defined(_M_AMD64) && !defined(__GNUC__) && !defined(HAVE_NO_CRC32_HARDWARE)", 1),
])
p = f"{root}/src/checksum/x86/crc32-x86-alt.c"
s = open(p, encoding="utf-8").read()
k = s.rindex("#endif")
open(p, "w", encoding="utf-8").write(s[:k] + """#else
bool
__wt_checksum_alt_match(const void *chunk, size_t len, uint32_t v)
{
    (void)chunk;
    (void)len;
    (void)v;
    return (false);
}
#endif""" + s[k + len("#endif"):])
print("patched src/checksum/x86/crc32-x86-alt.c (gcc fallback)")

# 6. The file system layer depends on the OS, not on the compiler.
patch("src/conn/conn_api.c", [
    ("#if defined(_MSC_VER)\n            WT_ERR(__wt_os_win(session));",
     "#if defined(_WIN32)\n            WT_ERR(__wt_os_win(session));", 1),
])
