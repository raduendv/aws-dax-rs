#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 GENERATED_BINDINGS_DIR SNAPSHOT_DIR" >&2
  exit 2
fi

generated="$1"
snapshots="$2"
mkdir -p "$snapshots"

awk '
  /^type (Dax[A-Za-z0-9_]*|(BatchGetItem|Query|Scan)Paginator[A-Za-z0-9_]*) (struct|interface|uint)/ { print $1, $2, $3 }
  /^func Ffi(Async)?ContractVersion/ {
    line = $0
    sub(/[[:space:]]*\{.*/, "", line)
    print line
  }
' "$generated/go/aws_dax_ffi/aws_dax_ffi.go" | LC_ALL=C sort -u >"$snapshots/go.txt"

awk '
  /^public (record|enum|abstract class) Dax/ {
    line = $0
    sub(/[[:space:]]*\(.*/, "", line)
    print line
  }
  /^public static class AwsDax/ { print }
  /^public (class|interface) I?(BatchGetItem|Query|Scan)Paginator / {
    print $1, $2, $3
  }
  /^[[:space:]]+public static (async Task<string>|string) Ffi(Async)?ContractVersion/ {
    line = $0
    sub(/[[:space:]]*\{.*/, "", line)
    print line
  }
' "$generated/dotnet/aws_dax_ffi.cs" | LC_ALL=C sort -u >"$snapshots/dotnet.txt"

find "$generated/java/software/amazon/dax" -maxdepth 1 -type f \
  \( -name 'Dax*.java' -o -name 'AwsDaxFfi.java' \
     -o -name 'BatchGetItemPaginator*.java' -o -name 'QueryPaginator*.java' \
     -o -name 'ScanPaginator*.java' \) \
  -exec basename {} .java \; |
  LC_ALL=C sort -u >"$snapshots/java.txt"

awk '
  /^(open class|public interface) (BatchGetItem|Query|Scan)Paginator/ {
    name = $3
    sub(/:.*/, "", name)
    print $1, $2, name
  }
  /^(data class|enum class|sealed class) Dax/ {
    line = $0
    sub(/[[:space:]]*\(.*/, "", line)
    sub(/[[:space:]]*\{.*/, "", line)
    print line
  }
  {
    if (match($0, /(suspend )?fun `ffi[^`]+`/)) {
      print substr($0, RSTART, RLENGTH)
    }
  }
' "$generated/kotlin/software/amazon/dax/aws_dax_ffi.kt" |
  LC_ALL=C sort -u >"$snapshots/kotlin.txt"

awk '
  /^class (Dax|(BatchGetItem|Query|Scan)Paginator)/ {
    line = $0
    sub(/[:(].*/, "", line)
    print line
  }
  /^(async )?def ffi_/ {
    line = $0
    sub(/:.*/, "", line)
    print line
  }
' "$generated/python/aws_dax_ffi.py" | LC_ALL=C sort -u >"$snapshots/python.txt"

awk '
  /^export (class|interface|type) (BatchGetItem|Query|Scan)Paginator/ {
    print $1, $2, $3
  }
  /^export (async )?function ffi/ {
    line = $0
    sub(/[[:space:]]*\{.*/, "", line)
    print line
  }
  /^export (type|enum) Dax/ {
    line = $0
    sub(/[[:space:]]*=.*/, "", line)
    sub(/[[:space:]]*\{.*/, "", line)
    print line
  }
' "$generated/javascript-napi/aws_dax_ffi.ts" |
  LC_ALL=C sort -u >"$snapshots/javascript.txt"
