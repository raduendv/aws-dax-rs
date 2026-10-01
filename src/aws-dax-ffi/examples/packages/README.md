# Clean packaged-binding examples

These programs are intentionally distinct from `tests/consumers`, which are
persistent live-DAX parity consumers.  They are installed only from a Phase 7
assembled artifact and run offline by default: each proves package-relative
native loading, calls the checksum-protected contract-version API, and creates
the language-native `DaxClientConfig`. No client or AWS operation is started by
default. Set `DAX_FFI_CREATE_CLIENT=1` and `DAX_ENDPOINT=host:port` to opt into
client construction and immediate shutdown, without item operations.

## Explicit live mode

Set **`DAX_FFI_LIVE=1`** to run real operations through the installed package.
This takes precedence over `DAX_FFI_CREATE_CLIENT`; it does not need both flags.
All six examples use:

- `DAX_ENDPOINT` (required, nonempty): a reachable plaintext or TLS DAX endpoint.
- `DAX_TABLE_NAME`: defaults to `DaxParityTable` when unset or empty. The table
  must already exist with string partition key `PK` and string sort key `SK`.
- `AWS_REGION`: optional explicit region; otherwise the native AWS configuration
  chain resolves the region. Credentials come from the native AWS credential
  chain, for example the remote host's IAM role. Grant the required DAX and
  backing-table access for PutItem, GetItem, and DeleteItem. Do not put
  credentials in example source or command history.

Each run generates unique `PK` and `SK` values and writes one item with
`value = "created"`. It polls GetItem at most 20 times, with 100 ms between
missing-item responses, and checks the returned keys and value. The overall
duration also includes per-request timeouts (60 seconds) and native retries;
the polling loop is not a two-second wall-clock timeout. Operation errors or
unexpected item contents fail immediately rather than being retried by the
example.

Cleanup is registered before PutItem, so even a failed/possibly partial write
gets a DeleteItem attempt with `ALL_OLD`. After an acknowledged write, the
returned old attributes must match the inserted item. Cleanup failures are
errors, never success. Clients are shut down in all exit paths after creation
and their foreign handles are disposed/released (Python uses its generated
finalizer after shutdown). As with any live client, a killed process or
unreachable cluster can prevent cleanup from completing.

Only after operations, cleanup, and shutdown succeed is exactly one of these
lines emitted:

```text
Go package live operations passed
.NET package live operations passed
Java package live operations passed
Kotlin/JVM package live operations passed
Python package live operations passed
Node/N-API package live operations passed
```

The Java example requires JDK 21 or newer.  The Kotlin example is Kotlin/JVM
with `kotlinx-coroutines`; it is neither Kotlin/Native nor KMP.  The
JavaScript example runs on Node using the selected N-API backend and is not a
browser/WASM example.

## Remote Linux artifact-only invocation

Run this on a Linux host that can reach DAX, **not** as part of the local
validator. Prerequisites: matching Linux/architecture package artifacts, Go
1.24+, .NET SDK 8+, JDK 21+, Kotlin compiler 2.1.20, Python 3 with venv/pip,
Node/npm compatible with `@ubjs/node` 0.31.0-6, and unzip. Provide the pinned
JNA 5.17.0, Kotlin stdlib 2.1.20, and coroutines 1.10.1 jars. npm may fetch the
package's declared `@ubjs/core`/`@ubjs/node` dependencies; prepopulate its cache
if registry access is unavailable.

**Platform prerequisite:** assemble `linux-x86_64` artifacts from an x86-64 ELF
`libaws_dax_ffi.so` on a glibc 2.28-compatible (or older) Linux builder. The
exact build, assembly, static inspection, and clean offline validation commands
are in the [packaging README](../../packaging/README.md#remote-linux-x86-64-assembly-and-validation).
Copying or renaming macOS artifacts is rejected and is not valid runtime proof.
The local macOS gate and mock-ELF archive checks do not claim Linux execution;
run the clean validator and the commands below on Linux.

Copy only the six artifact directories and this examples directory to the
remote host, then set absolute paths below. Neither a repository checkout,
generated binding sources outside the packages, nor Rust build output is
needed. Do not set native loader paths to a source/build tree.

```bash
set -euo pipefail
export ARTIFACTS=/opt/dax-packages/linux-x86_64
export EXAMPLES=/opt/dax-package-examples       # copy of examples/packages
export JNA_JAR=/opt/dax-deps/jna-5.17.0.jar
export KOTLIN_STDLIB_JAR=/opt/dax-deps/kotlin-stdlib-2.1.20.jar
export COROUTINES_JAR=/opt/dax-deps/kotlinx-coroutines-core-jvm-1.10.1.jar
export AWS_REGION=eu-west-1                    # use the cluster's region
export DAX_ENDPOINT='dax://your-cluster-endpoint:8111'
export DAX_TABLE_NAME=DaxParityTable
export DAX_FFI_LIVE=1
unset DAX_FFI_CREATE_CLIENT
unset LD_LIBRARY_PATH DYLD_LIBRARY_PATH DYLD_FALLBACK_LIBRARY_PATH PYTHONPATH
unset JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS
unset CGO_LDFLAGS LIBRARY_PATH NODE_PATH

work="$(mktemp -d)"
# The directory is retained for inspection/retry; it contains no repository build output.
printf 'Clean package workspace: %s\n' "$work"
mkdir -p "$work"/{go,dotnet,java,kotlin,python,javascript}

unzip -q "$ARTIFACTS/go/aws-dax-ffi-go-0.1.0.zip" -d "$work/go"
cp "$EXAMPLES/go/main.go" "$work/go/"
(cd "$work/go" && GOWORK=off go run .)

cp "$ARTIFACTS/dotnet/Aws.Dax.Ffi.0.1.0.nupkg" "$work/dotnet/"
cp "$EXAMPLES/dotnet/"{Check.csproj,Program.cs} "$work/dotnet/"
(cd "$work/dotnet" && dotnet restore --packages "$work/nuget" && dotnet run --no-restore --nologo)

cp "$EXAMPLES/java/Check.java" "$work/java/"
javac --release 21 -cp "$ARTIFACTS/java/aws-dax-ffi-0.1.0.jar:$JNA_JAR" \
  -d "$work/java" "$work/java/Check.java"
java -cp "$work/java:$ARTIFACTS/java/aws-dax-ffi-0.1.0.jar:$JNA_JAR" Check

cp "$EXAMPLES/kotlin/Check.kt" "$work/kotlin/"
kotlinc -jvm-target 21 \
  -cp "$ARTIFACTS/kotlin/aws-dax-ffi-kotlin-0.1.0.jar:$JNA_JAR:$COROUTINES_JAR" \
  -d "$work/kotlin/classes" "$work/kotlin/Check.kt"
java -cp "$work/kotlin/classes:$ARTIFACTS/kotlin/aws-dax-ffi-kotlin-0.1.0.jar:$JNA_JAR:$KOTLIN_STDLIB_JAR:$COROUTINES_JAR" CheckKt

python3 -m venv "$work/python/venv"
"$work/python/venv/bin/pip" install --no-index "$ARTIFACTS/python/"*.whl
cp "$EXAMPLES/python/check.py" "$work/python/"
"$work/python/venv/bin/python" "$work/python/check.py"

cp "$ARTIFACTS/javascript/aws-dax-ffi-0.1.0.tgz" "$work/javascript/"
cp "$EXAMPLES/javascript/check.mjs" "$work/javascript/"
(cd "$work/javascript" && \
  npm install --no-audit --no-fund --ignore-scripts ./aws-dax-ffi-0.1.0.tgz && \
  node check.mjs)
```

Use the TLS endpoint and its port instead when validating an encrypted cluster.
Re-running any example allocates a new key pair. To run the same installed
programs offline, set `DAX_FFI_LIVE=0` and leave `DAX_FFI_CREATE_CLIENT` unset;
to check only construction, set `DAX_FFI_CREATE_CLIENT=1` with live mode off.
