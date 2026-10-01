#!/usr/bin/env bash
set -euo pipefail

language="${1:?expected language}"
output_dir="${2:?expected generated language directory}"

case "$language" in
  java)
    integrity="$output_dir/software/amazon/dax/IntegrityCheckingUniffiLib.java"
    library="$output_dir/software/amazon/dax/UniffiLib.java"
    perl -0pi -e \
      's/final class IntegrityCheckingUniffiLib \{\n/final class IntegrityCheckingUniffiLib {\n    static void ensureInitialized() {}\n/' \
      "$integrity"
    perl -0pi -e \
      's/        \/\/ Force IntegrityCheckingUniffiLib to load first to run integrity checks\n        Class<\?> ignored = IntegrityCheckingUniffiLib\.class;/        \/\/ Force class initialization so native ABI checks run before the first call.\n        IntegrityCheckingUniffiLib.ensureInitialized();/' \
      "$library"
    grep -Fq 'static void ensureInitialized() {}' "$integrity"
    grep -Fq 'IntegrityCheckingUniffiLib.ensureInitialized();' "$library"
    ;;
  python)
    binding="$output_dir/aws_dax_ffi.py"
    perl -0pi -e \
      's/# _uniffi_check_api_checksums\(_UniffiLib\)/_uniffi_check_api_checksums(_UniffiLib)/' \
      "$binding"
    grep -Fq '_uniffi_check_api_checksums(_UniffiLib)' "$binding"
    if grep -Fq '# _uniffi_check_api_checksums(_UniffiLib)' "$binding"; then
      echo "Python checksum validation remains disabled" >&2
      exit 1
    fi
    ;;
  *)
    echo "no integrity fixup exists for '$language'" >&2
    exit 2
    ;;
esac
