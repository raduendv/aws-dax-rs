# FFI package assembly

`scripts/assemble_ffi_packages.py` consumes only the pinned generators,
generated binding sources, and one built `aws-dax-ffi` shared library. It
creates host artifacts in `target/ffi-packages/<platform>`.

- `go/aws-dax-ffi-go-0.1.0.zip` — a Go module with cgo linkage and an
  included native archive/library and package-directory rpath where needed;
- `dotnet/Aws.Dax.Ffi.0.1.0.nupkg` — `net8.0` and
  `runtimes/<RID>/native`;
- `java/aws-dax-ffi-0.1.0.jar` and `-sources.jar` — Java 21 classes, sources,
  and package-relative JNA assets;
- `kotlin/aws-dax-ffi-kotlin-0.1.0.jar` — Kotlin/JVM classes and
  package-relative JNA assets;
- a platform-tagged `python/aws_dax_ffi-0.1.0-*.whl`; and
- `javascript/aws-dax-ffi-0.1.0.tgz` — an npm package using the selected
  `@ubjs/node` N-API backend.

The implemented package mappings are exact:

| Assembly platform | Native asset | Go | .NET RID | Python wheel tag | npm |
| --- | --- | --- | --- | --- | --- |
| `linux-x86_64` | `libaws_dax_ffi.so` | `linux && amd64`, shared library with `${SRCDIR}/native/linux-x86_64` rpath | `linux-x64` | `manylinux_2_28_x86_64` | `linux` / `x64` |
| `linux-aarch64` | `libaws_dax_ffi.so` | `linux && arm64`, shared library with `${SRCDIR}/native/linux-aarch64` rpath | `linux-arm64` | `manylinux_2_28_aarch64` | `linux` / `arm64` |
| `osx-x86_64` | `libaws_dax_ffi.dylib` | `darwin && amd64`, static `libaws_dax_ffi.a` | `osx-x64` | `macosx_10_12_x86_64` | `darwin` / `x64` |
| `osx-arm64` | `libaws_dax_ffi.dylib` | `darwin && arm64`, static `libaws_dax_ffi.a` | `osx-arm64` | `macosx_11_0_arm64` | `darwin` / `arm64` |
| `windows-x86_64` | `aws_dax_ffi.dll` | `windows && amd64`, GNU import library `libaws_dax_ffi.dll.a` | `win-x64` | `win_amd64` | `win32` / `x64` |

Java and Kotlin/JVM use `/native/<assembly-platform>/<native-asset>` resources
for every mapping. Their JNA shim extracts the package-owned resource before
loading it. Python and Node/N-API resolve the same package-owned path directly.
The assembler validates the native container and architecture before packaging:
64-bit little-endian ELF for both Linux targets, 64-bit little-endian Mach-O
for both macOS targets, and x86-64 PE for Windows. Relabelled or
wrong-architecture binaries are rejected. macOS package assembly requires the
matching Rust static library for Go, and Windows requires a matching GNU import
library; pass either explicitly with `--go-native-library` when it is not in
the host debug output.

All archives are written with fixed ordering, timestamps, modes, ownership,
and gzip headers. Each includes `aws_dax_ffi_package.json`, which records the
semantic package version, native SHA-256, installed native platform, and the
Phase 8 target matrix.

The JVM and Python packages carry an adjacent generated loader shim, rather
than relying on `java.library.path`, JNA search paths, `PYTHONPATH`, or dynamic
library environment variables.  The npm wrapper resolves its included native
library with `@ubjs/node`; it does not claim browser or WASM support.  Kotlin
is a JVM artifact only, not Kotlin/Native or KMP.

## Package validation and opt-in live examples

Install the pinned JavaScript packaging tools once with
`npm ci --ignore-scripts --prefix src/aws-dax-ffi/packaging/javascript-tools`.
Then run `scripts/validate_ffi_packages.sh` for the current-host clean-install
gate on macOS arm64 or Linux x86-64.
It assembles packages, compares two fixed-input assemblies byte-for-byte, and
compiles/runs all six clean consumers. The compiled languages cover the entire
live branch; Python `py_compile` and Node `--check` provide explicit syntax
coverage. The validator defaults `DAX_FFI_LIVE` and `DAX_FFI_CREATE_CLIENT` to `0`; an
explicit live opt-in is preserved. Package/tool dependency resolution can
still need registry access. Offline syntax/compile coverage is not evidence of
live parity.

The [clean package examples](../examples/packages/README.md) remain offline by
default. Run their installed entry points separately with `DAX_FFI_LIVE=1`,
`DAX_ENDPOINT` set, an AWS region/credential chain, and optionally
`DAX_TABLE_NAME` (default `DaxParityTable`). The table must have string `PK` and
`SK` keys. Live mode uses unique keys for PutItem, polls GetItem until the item
is observed (at most 20 attempts), and always attempts DeleteItem with
`ALL_OLD` after an attempted write, including possibly partial failures.
Missing/mismatched items, operation errors, and cleanup errors fail the run.
Shutdown and foreign-handle disposal/release precede the exact
`<Language> package live operations passed` marker.

`DAX_FFI_CREATE_CLIENT=1` with live mode off remains a construction/shutdown
check without item operations; live mode takes precedence. See the examples
README for the exact six marker strings, cleanup limitations, and
[remote Linux artifact-only installation/run commands](../examples/packages/README.md#remote-linux-artifact-only-invocation).
Those commands intentionally consume only packages and copied example source,
not generated binding directories, Rust build libraries, or loader-path
overrides. Local static layout checks and the macOS offline gate do not claim a
Linux runtime pass.

## Remote Linux x86-64 assembly and validation

Build and assemble on a glibc 2.28-compatible (or older) x86-64 Linux builder
so the `manylinux_2_28_x86_64` wheel tag is truthful. With the pinned language
toolchains and generator dependencies installed, run from the repository root:

```bash
set -euo pipefail
unset DAX_FFI_LIVE DAX_FFI_CREATE_CLIENT
cargo build -p aws-dax-ffi
file target/debug/libaws_dax_ffi.so
scripts/assemble_ffi_packages.py \
  --platform linux-x86_64 \
  --native-library target/debug/libaws_dax_ffi.so
python3 scripts/tests/test_assemble_ffi_packages.py \
  target/ffi-packages/linux-x86_64 \
  target/debug/libaws_dax_ffi.so
scripts/validate_ffi_packages.sh
```

The final command repeats deterministic assembly/diff checks and performs clean
offline installs and native loads for all six ecosystems on Linux. Only that
remote Linux run is runtime evidence. For live DAX validation, assemble first,
then run the artifact-only commands in the examples README with
`DAX_FFI_LIVE=1`; do not add `LD_LIBRARY_PATH`, `PYTHONPATH`, `NODE_PATH`, or
source-tree native assets.
