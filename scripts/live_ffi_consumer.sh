#!/usr/bin/env bash
set -Eeuo pipefail

# Build without AWS access, or exec one previously built persistent consumer.
# DAX_FFI_TABLE_CLEANUP defaults to true; set it to false to retain test items.
action="${1:-}"
language="${2:-}"
if [[ $# != 2 || ! "$action" =~ ^(build|run)$ ]]; then
  echo "usage: $0 build|run go|dotnet|java|kotlin|python|javascript" >&2
  exit 2
fi
case "$language" in
  go|dotnet|java|kotlin|python|javascript) ;;
  *) echo "unknown consumer: $language" >&2; exit 2 ;;
esac

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_dir="$root/src/aws-dax-ffi/tests/consumers/$language"
bindings="$root/target/ffi-bindings"
native="$root/target/debug"
stage="$root/target/ffi-live/$language"
case "$(uname -s)" in
  Darwin) library=libaws_dax_ffi.dylib ;;
  Linux) library=libaws_dax_ffi.so ;;
  *) echo "unsupported consumer host" >&2; exit 2 ;;
esac
export LD_LIBRARY_PATH="$native${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export DYLD_LIBRARY_PATH="$native${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
if [[ "$language" == java || "$language" == kotlin ]]; then
  if [[ -z "${JAVA_HOME:-}" && -d /usr/lib/jvm/java-21-amazon-corretto.x86_64 ]]; then
    export JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto.x86_64
  fi
  if [[ -n "${JAVA_HOME:-}" ]]; then
    export PATH="$JAVA_HOME/bin:$PATH"
  fi
fi

if [[ "$action" == build ]]; then
  mkdir -p "$stage"
  case "$language" in
    go)
      cp "$source_dir/main.go" "$source_dir/go.mod" "$source_dir/go.sum" "$stage/"
      mkdir -p "$stage/aws_dax_ffi"
      cp "$bindings/go/aws_dax_ffi/"*.go "$bindings/go/aws_dax_ffi/"*.h "$stage/aws_dax_ffi/"
      cd "$stage"
      CGO_LDFLAGS="-L$native -laws_dax_ffi${CGO_LDFLAGS:+ $CGO_LDFLAGS}" go build -o live .
      ;;
    dotnet)
      cp "$source_dir/Program.cs" "$source_dir/Live.csproj" "$bindings/dotnet/aws_dax_ffi.cs" "$stage/"
      dotnet build "$stage/Live.csproj" --nologo
      ;;
    java|kotlin)
      cp "$source_dir/pom.xml" "$stage/"
      mkdir -p "$stage/src/main/$language"
      cp -R "$source_dir/src/main/$language/live" "$stage/src/main/$language/"
      cp -R "$bindings/$language/software" "$stage/src/main/$language/"
      mvn -q -f "$stage/pom.xml" compile dependency:build-classpath -Dmdep.outputFile=cp.txt
      ;;
    python)
      cp "$source_dir/live_direct_operations.py" "$source_dir/requirements.txt" \
        "$bindings/python/aws_dax_ffi.py" "$native/$library" "$stage/"
      python3 -m venv "$stage/venv"
      "$stage/venv/bin/pip" install --disable-pip-version-check -r "$stage/requirements.txt"
      "$stage/venv/bin/python" -m py_compile "$stage/live_direct_operations.py" "$stage/aws_dax_ffi.py"
      ;;
    javascript)
      mkdir -p "$stage/generated"
      cp "$source_dir/live.ts" "$source_dir/package.json" "$stage/"
      cp "$bindings/javascript-napi/"*.ts "$native/$library" "$stage/generated/"
      cd "$stage"
      npm install --no-audit --no-fund
      ./node_modules/.bin/tsc --noEmit --target ES2022 --module ESNext \
        --moduleResolution bundler --allowImportingTsExtensions --strict --skipLibCheck live.ts
      ;;
  esac
  echo "DAX_CONSUMER_BUILD language=$language status=ok"
  exit
fi

cd "$stage"
case "$language" in
  go) exec ./live ;;
  dotnet) exec dotnet bin/Debug/net8.0/Live.dll ;;
  java) exec java "-Djna.library.path=$native" -cp "target/classes:$(cat cp.txt)" live.Main ;;
  kotlin) exec java "-Djna.library.path=$native" -cp "target/classes:$(cat cp.txt)" live.MainKt ;;
  python) exec "$stage/venv/bin/python" live_direct_operations.py ;;
  javascript) exec node --import tsx live.ts ;;
esac
