#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
generated="$(mktemp -d "${TMPDIR:-/tmp}/aws-dax-ffi-bindings.XXXXXX")"

cleanup() {
  rm -rf "$generated"
}
trap cleanup EXIT

DAX_FFI_OUT_DIR="$generated" "$root_dir/scripts/generate_ffi_bindings.sh" all
rm -rf "$root_dir/src/aws-dax-ffi/api-snapshots"
"$root_dir/scripts/snapshot_ffi_api.sh" \
  "$generated" "$root_dir/src/aws-dax-ffi/api-snapshots"
