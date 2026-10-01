#!/usr/bin/env bash
set -Eeuo pipefail

# Offline is the default; callers must explicitly opt into live package checks.
: "${DAX_FFI_LIVE:=0}"
: "${DAX_FFI_CREATE_CLIENT:=0}"
export DAX_FFI_LIVE DAX_FFI_CREATE_CLIENT

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -z "${JAVA_HOME:-}" && -d /usr/lib/jvm/java-21-amazon-corretto.x86_64 ]]; then
  export JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto.x86_64
fi
if [[ -n "${JAVA_HOME:-}" ]]; then
  export PATH="$JAVA_HOME/bin:$PATH"
fi
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) platform="osx-arm64" ;;
  Linux-x86_64) platform="linux-x86_64" ;;
  *)
    echo "unsupported package-validation host: $(uname -s)/$(uname -m)" >&2
    exit 1
    ;;
esac
artifacts="$root/target/ffi-packages/$platform"

"$root/scripts/assemble_ffi_packages.py"
first="$(mktemp -d)"
second="$(mktemp -d)"
clean="$(mktemp -d)"
linux_mock="$(mktemp -d)"
trap 'rm -rf "$first" "$second" "$clean" "$linux_mock"' EXIT
first_output="$root/target/ffi-package-repro-first"
second_output="$root/target/ffi-package-repro-second"
linux_first_output="$root/target/ffi-package-linux-repro-first"
linux_second_output="$root/target/ffi-package-linux-repro-second"
rm -rf "$first_output" "$second_output" "$linux_first_output" "$linux_second_output"
"$root/scripts/assemble_ffi_packages.py" --skip-generate --output "$first_output"
"$root/scripts/assemble_ffi_packages.py" --skip-generate --output "$second_output"
cp -R "$first_output/$platform/." "$first/"
cp -R "$second_output/$platform/." "$second/"
diff -ru "$first" "$second"

# A minimal x86-64 ELF header is sufficient because this gate inspects Linux
# package layout and loader metadata without executing foreign code on macOS.
linux_native="$linux_mock/libaws_dax_ffi.so"
printf '\177ELF\002\001\001\000\000\000\000\000\000\000\000\000\003\000\076\000mock-aws-dax-ffi\n' > "$linux_native"
"$root/scripts/assemble_ffi_packages.py" --platform linux-x86_64 \
  --native-library "$linux_native" --skip-generate --output "$linux_first_output"
"$root/scripts/assemble_ffi_packages.py" --platform linux-x86_64 \
  --native-library "$linux_native" --skip-generate --output "$linux_second_output"
diff -ru "$linux_first_output/linux-x86_64" "$linux_second_output/linux-x86_64"
python3 "$root/scripts/tests/test_assemble_ffi_packages.py" \
  "$linux_first_output/linux-x86_64" "$linux_native"
rm -rf "$first_output" "$second_output" "$linux_first_output" "$linux_second_output"

# Clean projects are deliberately artifact-only: no generated source or native
# library from target is placed on a loader path.
mkdir -p "$clean/go" "$clean/dotnet" "$clean/java" "$clean/kotlin" "$clean/python" "$clean/javascript"
unzip -q "$artifacts/go/"*.zip -d "$clean/go/module"
cp "$root/src/aws-dax-ffi/examples/packages/go/main.go" "$clean/go/main.go"
cp "$clean/go/main.go" "$clean/go/module/"
(cd "$clean/go/module" && go run .)

cp "$artifacts/dotnet/"*.nupkg "$clean/dotnet/"
cp "$root/src/aws-dax-ffi/examples/packages/dotnet/"{Check.csproj,Program.cs} "$clean/dotnet/"
(cd "$clean/dotnet" && dotnet run --nologo)

jna="$HOME/.m2/repository/net/java/dev/jna/jna/5.17.0/jna-5.17.0.jar"
cp "$root/src/aws-dax-ffi/examples/packages/java/Check.java" "$clean/java/"
javac --release 21 -cp "$artifacts/java/aws-dax-ffi-0.1.0.jar:$jna" -d "$clean/java" "$clean/java/Check.java"
java -cp "$clean/java:$artifacts/java/aws-dax-ffi-0.1.0.jar:$jna" Check

if ! cp -al "$HOME/.m2/repository" "$clean/kotlin/m2" 2>/dev/null; then
  rm -rf "$clean/kotlin/m2"
  cp -a "$HOME/.m2/repository" "$clean/kotlin/m2"
fi
kotlin_package="$clean/kotlin/m2/software/amazon/dax/aws-dax-ffi-kotlin/0.1.0"
mkdir -p "$kotlin_package" "$clean/kotlin/src/main/kotlin"
cp "$artifacts/kotlin/aws-dax-ffi-kotlin-0.1.0.jar" "$kotlin_package/aws-dax-ffi-kotlin-0.1.0.jar"
cp "$artifacts/kotlin/aws-dax-ffi-kotlin-0.1.0.pom" "$kotlin_package/aws-dax-ffi-kotlin-0.1.0.pom"
printf 'aws-dax-ffi-kotlin-0.1.0.jar>central=\naws-dax-ffi-kotlin-0.1.0.pom>central=\n' > "$kotlin_package/_remote.repositories"
cp "$root/src/aws-dax-ffi/examples/packages/kotlin/Check.kt" "$clean/kotlin/src/main/kotlin/"
cat > "$clean/kotlin/pom.xml" <<'EOF'
<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
<groupId>check</groupId><artifactId>check</artifactId><version>1</version>
<properties><kotlin.version>2.1.20</kotlin.version></properties>
<dependencies><dependency><groupId>software.amazon.dax</groupId><artifactId>aws-dax-ffi-kotlin</artifactId><version>0.1.0</version></dependency>
<dependency><groupId>org.jetbrains.kotlin</groupId><artifactId>kotlin-stdlib</artifactId><version>${kotlin.version}</version></dependency>
<dependency><groupId>org.jetbrains.kotlinx</groupId><artifactId>kotlinx-coroutines-core-jvm</artifactId><version>1.10.1</version></dependency></dependencies>
<build><plugins><plugin><groupId>org.jetbrains.kotlin</groupId><artifactId>kotlin-maven-plugin</artifactId><version>${kotlin.version}</version>
<executions><execution><id>compile</id><goals><goal>compile</goal></goals></execution></executions>
<configuration><sourceDirs><source>${project.basedir}/src/main/kotlin</source></sourceDirs></configuration>
</plugin></plugins></build></project>
EOF
(cd "$clean/kotlin" && mvn -q -o -Dmaven.repo.local="$clean/kotlin/m2" compile)
java -cp "$clean/kotlin/target/classes:$kotlin_package/aws-dax-ffi-kotlin-0.1.0.jar:$clean/kotlin/m2/net/java/dev/jna/jna/5.17.0/jna-5.17.0.jar:$clean/kotlin/m2/org/jetbrains/kotlin/kotlin-stdlib/2.1.20/kotlin-stdlib-2.1.20.jar:$clean/kotlin/m2/org/jetbrains/kotlinx/kotlinx-coroutines-core-jvm/1.10.1/kotlinx-coroutines-core-jvm-1.10.1.jar" CheckKt

python3 -m venv "$clean/python/venv"
"$clean/python/venv/bin/pip" install --no-index "$artifacts/python/"*.whl
PYTHONPYCACHEPREFIX="$clean/python/pycache" "$clean/python/venv/bin/python" -m py_compile "$root/src/aws-dax-ffi/examples/packages/python/check.py"
"$clean/python/venv/bin/python" "$root/src/aws-dax-ffi/examples/packages/python/check.py"

cp "$artifacts/javascript/"*.tgz "$clean/javascript/"
cp "$root/src/aws-dax-ffi/examples/packages/javascript/check.mjs" "$clean/javascript/"
(cd "$clean/javascript" && npm install --no-audit --no-fund --ignore-scripts ./aws-dax-ffi-*.tgz && node --check check.mjs && node check.mjs)

echo "DAX_FFI_PACKAGE_VALIDATION platform=$platform status=ok"
