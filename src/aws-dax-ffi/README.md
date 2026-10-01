# AWS DAX generated bindings

`aws-dax-ffi` is the Rust-owned ABI facade for the `aws-dax` SDK. It generates
Go, .NET/C#, Java, Kotlin/JVM, Python, and Node/N-API APIs from one UniFFI
contract. Routing, retries, credentials, TLS, DAX protocol encoding, async
execution, pagination, cancellation, and shutdown remain implemented in Rust;
the generated packages are not independent protocol clients.

## Supported environments

| Ecosystem | Baseline | Async shape | Native package |
| --- | --- | --- | --- |
| Go | Go 1.24 | generated future callbacks | cgo module ZIP |
| .NET | .NET 8 | `Task` | NuGet with RID asset |
| Java | JDK 21 | `CompletableFuture` | classes/sources JAR |
| Kotlin | JVM 21 | suspending functions | Maven-style JAR/POM |
| Python | Python 3 | `asyncio` awaitables | platform wheel |
| JavaScript | Node 22 | `Promise`, `AbortSignal` | N-API npm tarball |

Native package mappings are Linux x86_64/aarch64, macOS x86_64/arm64, and
Windows x86_64. Kotlin/Native, Kotlin Multiplatform, browser JavaScript, and
WebAssembly are not supported claims.

## API reference

The language-neutral field and behavior reference is
[`CONTRACT.md`](./CONTRACT.md). Generated naming snapshots are checked in under
[`api-snapshots/`](./api-snapshots/) for:

- Go: `go.txt`
- .NET/C#: `dotnet.txt`
- Java: `java.txt`
- Kotlin/JVM: `kotlin.txt`
- Python: `python.txt`
- JavaScript/TypeScript: `javascript.txt`

The exported client covers:

- GetItem, PutItem, DeleteItem, UpdateItem, Query, and Scan;
- BatchGetItem and BatchWriteItem;
- TransactGetItems and TransactWriteItems;
- Query, Scan, and BatchGet paginators;
- endpoint discovery, refresh, active endpoints, and route snapshots;
- structured validation, transport, protocol, service, cancellation, timeout,
  and closed-client errors.

Every generated binding validates the UniFFI contract and callable checksums
before use. All DynamoDB values cross the ABI as FFI-owned tagged values;
AWS SDK for Rust types never cross the boundary.

## Credentials, TLS, and runtime ownership

Client creation uses the default AWS region and credential-provider chain
owned by Rust. Credentials, signing material, and arbitrary foreign credential
callbacks are never exported. Configure plaintext with a `dax://` endpoint and
TLS with a `daxs://` endpoint. Hostname verification is enabled by default;
the compatibility override must be restricted to controlled environments.

The library owns one process-wide multi-thread Tokio runtime. Foreign callers
must not provide a Tokio reactor. Cancelling a generated future aborts its Rust
task but does not close the client. Call `shutdown` first for deterministic SDK
cleanup, then invoke the language generator's disposal/release mechanism.
Shutdown is thread-safe and idempotent, and subsequent operations fail with a
structured closed-client error.

## Packages and validation

Install JavaScript packaging tools and assemble the current host packages:

```sh
npm ci --ignore-scripts --prefix src/aws-dax-ffi/packaging/javascript-tools
scripts/assemble_ffi_packages.py
scripts/validate_ffi_packages.sh
```

The validator installs only assembled artifacts into clean projects and does
not need loader-path overrides. It runs offline by default. Explicit
`DAX_FFI_LIVE=1` runs cleanup-safe Put/Get/Delete examples against `DAX_ENDPOINT`.
See [`examples/packages/README.md`](./examples/packages/README.md) for exact
commands and success markers.

Package archives are deterministic for fixed inputs and carry native
checksums/platform metadata. CI adds SHA-256 manifests, CycloneDX SBOMs, secret
scanning, and build-provenance attestations. None of the automation publishes
to an ecosystem registry.
