#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
first="$(mktemp -d "${TMPDIR:-/tmp}/aws-dax-ffi-bindings-a.XXXXXX")"
second="$(mktemp -d "${TMPDIR:-/tmp}/aws-dax-ffi-bindings-b.XXXXXX")"
snapshots="$(mktemp -d "${TMPDIR:-/tmp}/aws-dax-ffi-snapshots.XXXXXX")"

cleanup() {
  rm -rf "$first" "$second" "$snapshots"
}
trap cleanup EXIT

python3 "$root_dir/scripts/tests/test_optimize_ffi_python.py"
DAX_FFI_OUT_DIR="$first" "$root_dir/scripts/generate_ffi_bindings.sh" all
DAX_FFI_OUT_DIR="$second" "$root_dir/scripts/generate_ffi_bindings.sh" all

if ! diff -ru "$first" "$second"; then
  echo "generated FFI bindings are not deterministic" >&2
  exit 1
fi

case "$(uname -s)" in
  Darwin) python_native="$root_dir/target/debug/libaws_dax_ffi.dylib" ;;
  MINGW*|MSYS*|CYGWIN*) python_native="$root_dir/target/debug/aws_dax_ffi.dll" ;;
  *) python_native="$root_dir/target/debug/libaws_dax_ffi.so" ;;
esac
python3 "$root_dir/scripts/tests/test_python_buffer_bindings.py" \
  --binding "$first/python/aws_dax_ffi.py" --native-library "$python_native"

grep -q '^public record DaxClientConfig' "$first/dotnet/aws_dax_ffi.cs"
grep -q '^public static class AwsDax' "$first/dotnet/aws_dax_ffi.cs"

"$root_dir/scripts/snapshot_ffi_api.sh" "$first" "$snapshots"
diff -ru "$root_dir/src/aws-dax-ffi/api-snapshots" "$snapshots"
