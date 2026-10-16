#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ffi_manifest="$root_dir/src/aws-dax-ffi/Cargo.toml"
ffi_config="$root_dir/src/aws-dax-ffi/uniffi.toml"
output_root="${DAX_FFI_OUT_DIR:-$root_dir/target/ffi-bindings}"
language="${1:-all}"
bin_suffix=""

case "$(uname -s)" in
  Darwin) library="$root_dir/target/debug/libaws_dax_ffi.dylib" ;;
  Linux) library="$root_dir/target/debug/libaws_dax_ffi.so" ;;
  MINGW*|MSYS*|CYGWIN*)
    library="$root_dir/target/debug/aws_dax_ffi.dll"
    bin_suffix=".exe"
    ;;
  *)
    echo "unsupported host for UniFFI generation: $(uname -s)" >&2
    exit 1
    ;;
esac

cargo build --manifest-path "$root_dir/Cargo.toml" -p aws-dax-ffi
cargo build --manifest-path "$root_dir/Cargo.toml" -p aws-dax-ffi \
  --all-features --bins
mkdir -p "$output_root"

generate_kotlin() {
  "$root_dir/target/debug/uniffi-bindgen-kotlin$bin_suffix" \
    generate "$library" --language kotlin --config "$ffi_config" \
    --out-dir "$output_root/kotlin" --no-format
}

generate_python() {
  "$root_dir/target/debug/uniffi-bindgen-python$bin_suffix" \
    generate "$library" --language python --config "$ffi_config" \
    --out-dir "$output_root/python"
  "$root_dir/scripts/fix_ffi_binding_integrity.sh" \
    python "$output_root/python"
  python3 "$root_dir/scripts/optimize_ffi_python.py" \
    "$output_root/python/aws_dax_ffi.py"
}

generate_go() {
  "$root_dir/target/debug/uniffi-bindgen-go$bin_suffix" \
    "$library" --library --config "$ffi_config" --out-dir "$output_root/go"
}

generate_dotnet() {
  "$root_dir/target/debug/uniffi-bindgen-cs$bin_suffix" \
    "$library" --library --config "$ffi_config" --out-dir "$output_root/dotnet" \
    --no-format
}

generate_java() {
  "$root_dir/target/debug/uniffi-bindgen-java$bin_suffix" \
    generate "$library" --config "$ffi_config" --out-dir "$output_root/java"
  "$root_dir/scripts/fix_ffi_binding_integrity.sh" \
    java "$output_root/java"
}

generate_javascript_koffi() {
  "$root_dir/target/debug/uniffi-bindgen-js-koffi$bin_suffix" \
    generate "$library" --crate-name aws-dax-ffi --manifest-path "$ffi_manifest" \
    --out-dir "$output_root/javascript-koffi"
}

generate_javascript_napi() {
  "$root_dir/target/debug/uniffi-bindgen-js-napi$bin_suffix" \
    generate napi bindings --library "$library" \
    --ts-dir "$output_root/javascript-napi" --lib-colocated --no-format
}

case "$language" in
  all)
    generate_go
    generate_dotnet
    generate_java
    generate_kotlin
    generate_python
    generate_javascript_napi
    ;;
  go) generate_go ;;
  dotnet) generate_dotnet ;;
  java) generate_java ;;
  kotlin) generate_kotlin ;;
  python) generate_python ;;
  javascript-koffi) generate_javascript_koffi ;;
  javascript-napi) generate_javascript_napi ;;
  *)
    echo "unknown language '$language'" >&2
    echo "expected: all, go, dotnet, java, kotlin, python, javascript-koffi, javascript-napi" >&2
    exit 2
    ;;
esac
