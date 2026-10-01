# AWS DAX Workspace and Multi-Language FFI Plan

## Objective

Convert the repository into the single source of truth for:

- the publishable Rust DAX SDK;
- protocol fuzzing;
- a Rust-owned UniFFI facade;
- generated Go, .NET/C#, Java, Kotlin, Python, and JavaScript bindings;
- native artifacts, package metadata, examples, conformance tests, and release
  automation.

The FFI API implementation will be written in Rust. The first release will
use generated UniFFI bindings without handwritten language-specific API
facades. Optional idiomatic wrappers may be added later without changing the
native contract.

This file is the implementation and release plan for the workspace and
multi-language FFI work.

## Approved decisions

- Use one Git repository and one Cargo workspace.
- Use three workspace members:
  - `src/aws-dax-sdk`
  - `src/aws-dax-fuzz`
  - `src/aws-dax-ffi`
- Move the current `aws-dax` package into `src/aws-dax-sdk` without changing
  its published crate name.
- Move the current fuzz package into `src/aws-dax-fuzz`.
- Build the FFI crate from the proof of concept at
  `/Users/radugribincea/projects/aws/uniffi-dax`, replacing its demonstration
  API with a complete Rust-owned DAX contract.
- Export generated bindings for Go, .NET/C#, Java, Kotlin, Python, and
  JavaScript.
- Require parity with every currently supported public Rust SDK operation,
  paginator, configuration behavior, lifecycle operation, and structured
  error before the first FFI release.
- Target Linux x86_64/aarch64, macOS x86_64/aarch64, and Windows x86_64.
- Keep DAX behavior, validation, conversion, retries, pagination,
  cancellation, and runtime ownership in Rust.
- Defer handwritten language facades.
- Start with one compatible UniFFI dependency train. The initial candidate is
  UniFFI 0.31 because the validated Go, C#, Java, and JavaScript generators
  are version-coupled to it.
- Compare the proof-of-concept JavaScript backend with the newer N-API path in
  Phase 0. Prefer N-API if it passes all contract and packaging gates.

## Target layout

```text
aws-dax-rs/
├── Cargo.toml
├── Cargo.lock
├── PLAN_FFI.md
├── README.md
├── .github/
├── scripts/
├── src/
│   ├── aws-dax-sdk/
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   ├── tests/
│   │   ├── examples/
│   │   └── README.md
│   ├── aws-dax-fuzz/
│   │   ├── Cargo.toml
│   │   ├── fuzz_targets/
│   │   └── corpus/
│   └── aws-dax-ffi/
│       ├── Cargo.toml
│       ├── uniffi.toml
│       ├── src/
│       ├── bindgen/
│       ├── tests/
│       ├── generated/
│       │   ├── go/
│       │   ├── dotnet/
│       │   ├── java/
│       │   ├── kotlin/
│       │   ├── python/
│       │   └── javascript/
│       ├── packages/
│       └── examples/
└── target/
```

Package-ready generated sources will remain build output until the FFI
contract and package layouts stabilize. Build-intermediate output remains
ignored. Generation must be byte-for-byte deterministic in CI. A later
packaging phase may check in generated source snapshots, in which case CI
must also fail when regeneration changes them.

## Architectural boundary

### `aws-dax-sdk`

The current Rust SDK remains the only implementation of:

- DAX protocol and wire codecs;
- transport, TLS, authorization, pooling, discovery, routing, health, and
  retries;
- validation and expressions;
- operations, transactions, pagination, and errors;
- the public Rust developer API.

The workspace move must not introduce FFI concerns into this crate.

### `aws-dax-ffi`

The FFI crate depends on `aws-dax-sdk` and owns:

- stable FFI-safe request and response records;
- enums and tagged unions;
- conversion to/from AWS SDK and `aws-dax` types;
- the exported client;
- Rust-owned Tokio behavior;
- async calls, cancellation, close, and object lifetime;
- FFI errors;
- paginator objects;
- bindgen configuration and binaries;
- cross-language fixtures and package assembly.

It must not reimplement DAX protocol behavior.

### Generated bindings

Generated bindings call the Rust FFI crate directly. Initial releases contain
no handwritten language SDK facade. Minimal `go.mod`, project files, Maven or
Gradle metadata, `pyproject.toml`, and `package.json` files are packaging
configuration rather than duplicated SDK behavior.

## FFI contract rules

1. Only FFI-owned types cross the ABI.
2. No AWS SDK generated Rust type crosses the ABI.
3. No Rust fluent builder crosses the ABI.
4. All records, enums, errors, and object methods have explicit compatibility
   guarantees.
5. Additive fields use optionals or versioned request records.
6. Breaking native changes require an ABI major-version change.
7. Native library and generated binding checksums must match.
8. Collections and byte payloads have documented size limits.
9. Credentials and secrets are never returned or logged.
10. Foreign-language code contains no DAX behavior.

## Proposed Rust-owned FFI model

### Values

Define `DaxAttributeValue` as a tagged union covering:

- string;
- number string;
- binary;
- bool;
- null;
- string/number/binary sets;
- list;
- map.

Define an item as a map from string to `DaxAttributeValue`. Preserve DynamoDB
numbers as strings.

### Client and configuration

Export Rust-owned equivalents for:

- endpoint list and TLS scheme;
- region;
- route manager;
- timeout and retry settings;
- IP discovery;
- logging;
- credential strategy;
- create, close, endpoint discovery, refresh, and route snapshots.

The first release should support the default AWS credential chain inside
Rust. Static credentials, callbacks, and foreign refresh providers require a
separate security and async-lifetime review.

### Operations

Export full async request/response records for:

- GetItem
- PutItem
- DeleteItem
- UpdateItem
- Query
- Scan
- BatchGetItem
- BatchWriteItem
- TransactGetItems
- TransactWriteItems

### Paginators

Export stateful paginator objects for Query, Scan, and BatchGetItem with:

- `has_more_pages`;
- async `next_page`;
- route affinity and failover;
- duplicate-token protection;
- cancellation/close semantics.

### Errors

Preserve configuration, validation, closed-client, transport, DAX, service,
and transaction cancellation data, including operation, kind, request ID,
service code, status, code sequence, and cancellation reasons.

## Phase 0 — Workspace and backend feasibility

### Workspace migration

- Replace the root package manifest with a virtual workspace manifest.
- Use resolver version 3.
- Move the SDK and fuzz packages to their target paths.
- Fix path dependencies, package include/readme paths, examples, tests,
  scripts, CI, documentation, and release commands.
- Preserve the published crate name `aws-dax`.
- Decide `default-members` behavior so normal workspace checks do not
  accidentally run fuzz targets.
- Preserve the ignored Go reference checkout.
- Compare pre/post move tests, Clippy, docs, package contents, and live smoke.

### UniFFI train

- Port only reusable proof-of-concept generator infrastructure.
- Pin every generator to a release or immutable revision.
- Prove one UniFFI train across Go, C#, Java, Kotlin, Python, and both JS
  candidates.
- Record license and source-policy implications.
- Add a compatibility check preventing independent UniFFI upgrades.

### JavaScript gate

Run the same async proof slice through the current proof-of-concept backend
and the newer N-API backend. Cover nested records/unions, bytes/maps, async
success, structured errors, cancellation, concurrency, and close/drop.

Prefer N-API only if it passes Node compatibility, Promise behavior,
cancellation, native loading, all target packaging designs, deterministic
generation, maintenance, and license gates.

Selected backend: `jhugman/uniffi-bindgen-react-native` 0.31.0-6 with its
`@ubjs/node` N-API runtime, pinned at commit
`3dd1cd1353750dc3bfbbf4bb4d4ccde26b0fbd2e`. It shares the UniFFI 0.31 train,
generates checksum-protected TypeScript, and successfully loaded the real
`aws-dax-ffi` library and called the proof contract. The Koffi generator
remains available as an explicit comparison target, but is not selected:
its generated proof function referenced an undefined `uniffiRustCaller` and
failed before entering Rust. The selected Go, C#, Java, Kotlin, Python, and
N-API bindings all executed the same Rust-owned async proof successfully.
Cancellation, concurrency, and lifecycle behavior remain release gates as
those surfaces are added to the FFI contract.

### Exit criteria

- Workspace migration is proven behavior-neutral.
- All six generators build from one dependency train.
- JavaScript backend is selected with evidence.
- One async proof slice runs in all languages on one host.
- Native artifact names/loading rules are fixed.

## Phase 1 — Contract inventory and fixtures

- The reviewed language-neutral inventory is maintained in
  `src/aws-dax-ffi/CONTRACT.md`.
- Inventory every public SDK operation, field, paginator, config option,
  lifecycle method, and error field.
- Map each to an FFI-owned equivalent.
- Define language-neutral fixtures for all AttributeValue variants, all
  requests/responses, transactions, errors, pagination, and routing.
- Define ABI evolution and package version mapping.
- Generate API snapshots for each backend.

### Exit criteria

- Every current Rust public operation has a reviewed mapping.
- Fixtures and six API snapshots are accepted.
- No exported type contains an SDK-generated type.

## Phase 2 — FFI foundations

Status: complete.

- Create `aws-dax-ffi` as `lib` and `cdylib`.
- Add scaffolding and one generator command per language.
- Implement Rust-owned config, credential loading, Tokio strategy, client
  create/shutdown, logging policy, ABI/checksum inspection, panic containment,
  and error conversion.
- Define deterministic shutdown and cancellation.
- Stress object lifetime, concurrent calls, repeated create/drop, and shutdown.

The generated Java and Kotlin object APIs reserve `close` for releasing the
UniFFI handle, so deterministic SDK cleanup is exported as `shutdown`.
Generated disposal remains separate. Go, .NET, Java, Kotlin, Python, and
N-API JavaScript all passed native create/error/concurrency/shutdown proofs;
N-API JavaScript additionally passed `AbortSignal` cancellation. Rust stress
tests cover repeated create/drop, independent concurrent clients, concurrent
idempotent shutdown, and handle release. Deterministic generation enables
callable checksum validation in every selected backend, including fail-closed
fixups for defects in the pinned Java and Python generators. Deliberately
mismatched Java and Python bindings were rejected before a contract call.

### Exit criteria

- All languages create and shut down the same Rust client.
- Async success/error/cancellation/concurrency pass.
- No task or native-handle leaks.
- Mismatched native libraries are rejected.

## Phase 3 — Shared values and conversions

Status: complete.

- Implement FFI AttributeValue and recursive collections.
- Enforce recursion, collection, string, and byte limits.
- Implement complete FFI ↔ SDK round trips.
- Add cross-language fixtures, property tests, and fuzzing.
- Reject malformed, oversized, deeply nested, or invalid-number values
  deterministically.

All ten value variants and complete items now round-trip through bounded
FFI-to-AWS-SDK conversion. One aggregate budget enforces depth, node,
collection-entry, per-value byte, and total-byte limits. Sets reject empty and
duplicate values; number strings enforce the pinned Go decimal grammar,
DynamoDB precision, exponent, and magnitude constraints. Unit, property, and
10,000-run nightly fuzz tests pass. The generated contract proof passed nested
all-variant round trips and structured invalid-number errors in Go, .NET,
Java, Kotlin, Python, and N-API JavaScript.

## Phase 4 — Direct operations

Status: complete.

Export GetItem, PutItem, DeleteItem, UpdateItem, Query, and Scan with complete
supported fields and outputs.

For every operation:

- add Rust conversion/unit tests;
- regenerate all bindings;
- run fixture tests in all languages;
- run plaintext and TLS live DAX tests from all languages.

All six direct operations are exported through FFI-owned request and response
records with bounded AttributeValue conversion before dispatch. Unit tests
cover every request field, closed-client behavior, and invalid values.

Generated foreign calls exposed that UniFFI futures can be polled without a
Tokio reactor. The FFI now owns one process-wide multi-thread Tokio runtime;
client construction and all SDK calls execute on it, and dropping a foreign
future aborts the corresponding Rust task. Regression tests exercise Tokio
timers from a non-Tokio executor and cancellation cleanup.

Persistent Go, .NET, Java, Kotlin, Python, and N-API JavaScript consumers each
passed PutItem, GetItem, UpdateItem, Query, Scan, and cleanup-safe DeleteItem
against both plaintext and TLS DAX endpoints.

## Phase 5 — Batch and transactions

Status: complete.

Export BatchGetItem, BatchWriteItem, TransactGetItems, and TransactWriteItems,
including action unions, unprocessed data, tokens, consumed capacity, item
collection metrics, and cancellation reasons/items.

The Rust FFI slice is implemented and locally validated. All four methods use
the existing FFI-owned DTOs, bounded AttributeValue conversion, structured
errors, and the explicit Rust-owned `runtime::run` executor boundary. Unit and
contract-fixture tests cover every request action, unprocessed data, response
metrics/capacity, transaction tokens, invalid nested values, closed-client
dispatch, and cancellation reasons/items.

Live validation exposed and fixed four shared SDK protocol defects: BatchWrite
and BatchGet now drain every top-level response section before a tube is
reused; standard consumed-capacity payloads are decoded from their packed CBOR
byte strings while transaction capacity uses the extended ordinal-map format;
and transaction key payloads preserve multi-byte CBOR string-length headers.
Regression tests cover each framing boundary and long composite transaction
keys.

Persistent Go, .NET, Java, Kotlin, Python, and N-API JavaScript consumers now
exercise BatchWrite put/delete, BatchGet, TransactWrite put/delete,
TransactGet, structured transaction cancellation reasons, and unconditional
cleanup. Every consumer passed against both plaintext and TLS DAX endpoints.

The final gate passed SDK and FFI all-target tests, fuzz-target compilation,
workspace all-target/all-feature Clippy with warnings denied, warnings-denied
documentation, dependency policy, deterministic binding generation and API
snapshots, and diff checks. Fuzz targets are compiled rather than invoked by
the finite test gate because libFuzzer executables intentionally run without a
natural termination condition.

## Phase 6 — Paginators, routing, and lifecycle

Status: complete.

- Export Query, Scan, and BatchGet paginators.
- Export discovery, refresh, and route snapshots.
- Preserve route affinity/failover, retry behavior, background tasks, close,
  and cancellation.
- Test duplicate tokens, cancellation between pages, failover, close during
  calls, and controlled one-node failure from every language.

Stateful Query, Scan, and BatchGet paginators now expose synchronous
`has_more_pages` and asynchronous optional `next_page` results while
serializing cursor mutation in Rust. BatchGet preserves the duplicate-token
stop policy. Discovery, explicit refresh, active endpoints, and route snapshots
are exported as FFI-owned records, and SDK shutdown/cancellation semantics are
preserved without inventing foreign-runtime behavior.

Persistent Go, .NET, Java, Kotlin, Python, and N-API JavaScript consumers passed
multi-page Query and Scan, BatchGet paginator completion, discovery, refresh,
route snapshots, shutdown, and rejected post-shutdown calls against both
plaintext and TLS DAX. All six also passed a prewarmed TLS run with exactly one
discovered node blocked. The shared opt-in harness verifies the rule before and
after each workload and removes it through an EXIT trap; every run reported
successful cleanup.

The final finite gate passed SDK and FFI all-target tests, fuzz-target
compilation, warnings-denied workspace Clippy and documentation, dependency
policy, deterministic bindings and snapshots, offline fault-harness tests, and
diff checks.

## Phase 7 — Generated packages and examples

Status: complete.

`scripts/assemble_ffi_packages.py` assembles versioned artifacts using the
pinned generators and records native checksums and platform metadata.
`scripts/validate_ffi_packages.sh` verifies fixed-input reproducibility, archive
layout, clean artifact-only installation, package-relative native loading, and
the generated examples. The macOS arm64 offline gate passed for all six
ecosystems without loader-path overrides. Genuine Linux x86_64 artifacts also
passed deterministic reproduction, clean installation, and native loading.
From those Linux packages alone, Go, .NET, Java, Kotlin/JVM, Python, and
Node/N-API each passed cleanup-safe PutItem, polling GetItem, and DeleteItem
examples against both plaintext and TLS DAX. The remaining native targets are
part of the Phase 8 cross-platform build matrix rather than Phase 7 package
design.

### Go

- Generate Go/cgo sources and module metadata.
- Package native assets for supported targets.
- Compile and run a clean consumer example.

### .NET

- Generate C# and build one NuGet package with RID-specific assets.
- Use .NET 8 as the initial baseline.
- Test Task behavior, errors, disposal, and native loading.

### Java

- Generate Java with the selected compatible backend.
- Record the backend-imposed JDK baseline.
- Package Java classes/sources and native assets.
- Test CompletableFuture, executor behavior, native access, and close.

### Kotlin

- Generate Kotlin/JVM.
- Test suspend functions and coroutine cancellation.
- Do not claim Kotlin/Native/KMP without separate validation.

### Python

- Generate Python and build platform wheels.
- Test asyncio, exceptions, bytes, imports, and native loading.

### JavaScript

- Generate TypeScript/JavaScript through the selected backend.
- Build npm packages with deterministic native resolution.
- Test Promises, errors, concurrency, cancellation, package installation, and
  TypeScript declarations.
- Do not claim browser/WASM support.

### Exit criteria

- Every package installs into a clean project.
- Every generated example runs against live DAX.
- Native loading needs no ad hoc path manipulation.

## Phase 8 — CI and supply chain

Status: complete.

CI now uses immutable action revisions and finite Rust, SDK, FFI, binding,
fuzz-compilation, package, and policy gates. A five-target native matrix builds
Linux x86_64/aarch64, macOS x86_64/arm64, and Windows x86_64, then assembles
each target twice and rejects any archive difference. Every target package is
statically checked for the correct Go linkage, .NET RID, JVM resource,
platform wheel, npm platform, native checksum, and binary architecture.
Package jobs emit SHA-256 manifests, CycloneDX SBOMs, and main-branch
provenance attestations; source/license policy and secret scanning are
separate required gates. Bounded fuzzing runs on a schedule, while live DAX
package parity is manual, serialized, and restricted to a VPC-reachable
self-hosted runner.

The full matrix was also proven with real local artifacts: native builds and
six-package assembly passed for both macOS architectures, both Linux
architectures, and Windows x86_64. Linux aarch64 and Windows were cross-built
with Zig; their outputs were verified as aarch64 ELF and x86-64 PE rather than
relabeled host binaries.

Add CI for:

- Rust workspace format/test/Clippy/docs/deny/package checks;
- deterministic generation and API snapshots;
- all six language compile tests;
- all five native target builds;
- clean package installation;
- controlled live DAX parity;
- scheduled stress and fuzz tests.

Require immutable pins, source/license checks, reproducible build metadata,
SBOMs, checksums, artifact signing/provenance, and secret-free artifacts.

## Phase 9 — Release readiness

Status: complete; publication is intentionally not performed.

Compatibility, generated API entry points, credentials, TLS, Rust-owned async
runtime behavior, cancellation, shutdown, native loading, target mappings, and
unsupported claims are documented for all six ecosystems. The release
checklist now requires per-ecosystem clean installation, full operation parity,
plaintext/TLS package validation, one-node fault recovery, deterministic
archives, checksums, SBOMs, provenance, and release-candidate inspection from
downloaded packages only.

Artifact-only Linux x86_64 release candidates were installed into clean Go,
.NET, Java, Kotlin/JVM, Python, and Node projects without loader-path
overrides. All six passed cleanup-safe packaged PutItem/GetItem/DeleteItem over
both plaintext and TLS DAX. Persistent generated consumers separately passed
direct, batch, transaction, paginator, routing, lifecycle, structured-error,
and one-node TLS failover coverage. Real native artifacts and six-package
layouts now pass for all five release targets. No package was uploaded to an
ecosystem registry, no tag was created, and future publication still requires
explicit user approval.

- Document SDK/FFI compatibility.
- Generate language API references.
- Document loading, credentials, TLS, async runtime, cancellation, and close.
- Add a six-ecosystem release checklist.
- Validate release candidates only from packaged artifacts.
- Publish FFI packages only after full parity and platform gates pass.

## First FFI release gates

- Every current Rust operation is exported.
- All languages pass the same semantic fixture suite.
- Plaintext/TLS live DAX passes in all languages.
- Paginators, transactions, discovery, routing, retries, cancellation, and
  close pass.
- All five native targets build/package.
- ABI/checksum mismatch is detected.
- No release-blocking memory, lifecycle, concurrency, or security issue
  remains.
- Handwritten language facades remain out of scope.

## Agent strategy

The existing DAX port agents are not specialized for ABI and packaging work.
Add three repository agents:

### `dax-ffi-contract-architect`

For API inventory, FFI type design, compatibility/versioning, runtime,
lifecycle, cancellation, and credential boundaries. It must not change DAX
protocol behavior.

### `dax-ffi-implementer`

For one approved vertical slice at a time: Rust records/conversions, exports,
generator integration, fixtures, generated tests, and bounded package
assembly. It must not add handwritten language SDK behavior.

### `dax-ffi-parity-reviewer`

For read-only review of SDK/FFI parity, generated API consistency, ABI
compatibility, async/error/lifecycle semantics, and native packaging.

### Delegation rules

- Keep workspace moves and continuous debugging traces with one owner.
- Delegate independent package lanes only after the Rust contract is frozen.
- Never let separate agents invent per-language APIs.
- Use one contract and one fixture corpus for all backends.
- Require parity review after each operation family.

## Principal risks

| Risk | Mitigation |
|---|---|
| Third-party generators lag UniFFI | Pin one train and gate upgrades across all languages |
| Rust SDK types leak into ABI | Export only FFI-owned types |
| Recursive values exhaust resources | Enforce depth/length/byte limits |
| Foreign event loops cannot run Tokio | Own Tokio execution in Rust |
| Cancellation differs by language | Export explicit cancellation/close |
| Paginator pages cross routes | Paginator-local route affinity with failover |
| Native/generated mismatch | ABI version and checksum verification |
| Packaging multiplies maintenance | Generate APIs and template metadata |
| Java/JS backends change | Isolate generators behind common fixtures |
| Workspace move breaks publication | Compare package and validation outputs |
| Credentials cross unsafe boundary | Start with Rust-owned default chain |

## Execution order

1. Approve Phase 0 workspace/generator decisions.
2. Freeze the Phase 1 contract.
3. Build foundations and value conversions.
4. Export direct operations, then batch/transactions, then paginators.
5. Add package automation after generated APIs stabilize.
6. Require the complete six-language/five-target matrix for release.
