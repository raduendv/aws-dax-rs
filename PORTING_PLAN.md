# AWS DAX Go v2 to Rust porting plan

## Objective

Build a production-quality Rust DAX client that gives Rust developers the same
capabilities and operational behavior as `aws-dax-go-v2`, while feeling native
alongside the AWS SDK for Rust.

The reference implementation is the local `aws-dax-go-v2/` clone at:

- repository: `git@github.com:aws/aws-dax-go-v2.git`
- revision: `287eea5d36462faac54175d1e89878804b524928`
- tag: `v1.0.3`

The reference revision must remain pinned while a phase is in progress. Upstream
changes are incorporated deliberately, with a compatibility review and new
fixtures.

Current progress: the protocol-only `BatchWriteItem` encoder is implemented and
covered by an exact CBOR vector, including put/delete entries, batch limits,
duplicate-key rejection, multi-table schema lookup, and optional-operation
framing. Public BatchWrite transport wiring and bounded `BatchGetItem`
transport/response handling are now implemented; BatchGet projection responses
decode ordinal response maps and item-collection metrics.

The protocol-only `TransactGetItems` encoder is also implemented and covered by
an exact request vector. Public transaction transport integration and
projection/no-projection positional response handling are now implemented. The
protocol-only TransactWrite encoder now
covers Put, Delete, ConditionCheck, and bounded Update parallel-array requests
with exact Put and Update vectors. Public TransactWrite transport wiring now
decodes null return-value entries and consumed capacity; returned attributes
and item-collection metrics; returned attributes remain an explicit
compatibility boundary.

## Definition of one-to-one developer experience

One-to-one means behavioral and capability parity, not a line-by-line rewrite
or an imitation of Go syntax.

The Rust client should:

1. Use AWS SDK for Rust DynamoDB model types wherever the SDK exposes a suitable
   public type.
2. Follow the AWS SDK for Rust's async, fluent operation style where practical:

   ```rust,no_run
   let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
   let client = aws_dax::Client::from_sdk_config(
       &sdk_config,
       "dax://example-cluster:8111",
   )?;

   let output = client
       .get_item()
       .table_name("example")
       .key("id", AttributeValue::S("42".into()))
       .send()
       .await?;
   ```

3. Support the same DAX data-plane operations as the Go client:
   `PutItem`, `DeleteItem`, `UpdateItem`, `GetItem`, `Scan`, `Query`,
   `BatchWriteItem`, `BatchGetItem`, `TransactWriteItems`, and
   `TransactGetItems`.
4. Preserve the Go client's defaults, validation, retry classification,
   timeout behavior, endpoint discovery, routing, TLS behavior, error details,
   and pagination semantics unless a documented Rust SDK convention requires a
   deliberate difference.
5. Clearly reject DynamoDB operations that DAX does not implement. Unsupported
   behavior must be stable and tested rather than omitted accidentally.
6. Be safe for concurrent use, support cancellation, and shut down background
   work and pooled connections deterministically.
7. Produce wire-compatible requests and consume wire-compatible responses.

## Scope

### In scope

- Public client, configuration, operation builders, paginators, logging, and
  IPv4/IPv6 discovery controls.
- DAX CBOR protocol and DynamoDB attribute value conversion.
- DAX request/response encoding, validation, and error mapping.
- DynamoDB expression parsing and projection handling.
- DAX-specific SigV4 connection authorization.
- TCP/TLS transport, connection pooling, retries, discovery, health checks,
  routing, metrics, and graceful close.
- Unit, property, golden-vector, cross-language conformance, and opt-in live
  integration tests.
- User documentation, examples, changelog, license, notice, and third-party
  attribution appropriate for the Rust distribution.

### Out of scope until parity is complete

- New DAX features that are absent from the pinned Go implementation.
- A synchronous client.
- Reimplementing the complete AWS SDK DynamoDB model set.
- Optimizations that make protocol behavior harder to compare with the
  reference implementation.
- Default tests that require an AWS account or a live DAX cluster.

## Architectural target

The crate should be a library. The current binary-only skeleton will be replaced
after this plan is approved.

```text
public Client / Config / operation builders / paginators
  -> operation orchestration and model conversion
    -> validation, request encoding, response decoding, error mapping
      -> DAX CBOR, expression parser, projection logic, bounded caches
    -> cluster discovery and routing
      -> per-node client
        -> async tube pool
          -> TCP/TLS tube, handshake, and SigV4 authorization
```

Proposed internal module boundaries:

```text
src/
  lib.rs
  client.rs
  config.rs
  error.rs
  operation/
  paginator/
  protocol/
    cbor/
    request/
    response/
    expression/
    sigv4.rs
  transport/
    tube.rs
    pool.rs
    single.rs
  cluster/
    discovery.rs
    health.rs
    routing.rs
  util/
    lru.rs
    metrics.rs
```

Internal modules remain private unless a public type is needed for configuration,
operation input/output, or a stable error contract.

## Decisions and implementation gates

### Phase 0 recorded decisions

- **Crate identity:** package `aws-dax`, Rust crate path `aws_dax`.
- **MSRV:** Rust 1.85, the first stable compiler supporting edition 2024.
- **Public model boundary:** later operation builders and outputs will use AWS
  SDK for Rust DynamoDB public model types directly where they are suitable;
  conversions remain internal.
- **Async runtime:** Tokio 1.x is required for networked phases. The crate will
  not create a runtime.
- **Fixture container:** JSON manifests with schema `dax-fixture/v1`; binary
  data is lowercase hexadecimal, and error comparisons use stable categories.
- **Fixture freshness:** the `reference-fixture-export` tool refuses a reference
  revision mismatch and regenerates deterministic metadata fixtures; its opt-in
  stale-fixture test runs when the pinned local reference checkout is available.
- **Continuous integration:** GitHub Actions validates Rust 1.85 and stable,
  formatting, tests, Clippy, documentation, package contents, and
  `cargo-deny` dependency policy.
- **Phase 0 network policy:** unit and documentation tests are offline. Live
  DAX tests remain opt-in and are not part of the default suite.

These choices are intentionally narrow. User-agent naming, parser generation,
synthetic SigV4 fixture publication, and live-cluster CI policy require their
respective implementation phases and must not be implied by the Phase 0 scaffold.

### Remaining gates

1. **Public API shape.** Recommended: AWS SDK for Rust-style fluent builders,
   plus `send_with_input`-style internal helpers for testing and paginator reuse.
2. **AWS SDK compatibility floor.** Pin a tested compatible release range for
   `aws-config`, `aws-sdk-dynamodb`, credential types, and Smithy runtime crates.
   Avoid exposing Smithy internals unnecessarily because their APIs evolve.
3. **Error contract.** Use a crate-owned non-exhaustive error type that retains
   DAX code, HTTP-like status, request ID, retryability, fault classification,
   transaction cancellation reasons, and source errors.
4. **Expression parser strategy.** First recover the authoritative DynamoDB
   grammar and its license. Prefer reproducible generation when a mature Rust
   target exists; otherwise implement a purpose-built parser against imported
   conformance cases. Do not translate generated Go parser files manually.
5. **Live-test strategy.** Decide which AWS account/cluster tests CI may run and
   keep them opt-in via features and environment configuration.

## Delivery phases

Each phase ends with passing tests, formatted code, clean lint output, updated
documentation, and a parity report against the pinned Go source. A later phase
must not compensate for an unverified earlier protocol layer.

### Phase 0: Contract and fixture harness

Deliver:

- Convert the skeleton into a library with agreed crate metadata and MSRV.
- Record the public API decision and dependency compatibility policy.
- Add test support that can consume language-neutral fixtures.
- Add a small Go fixture exporter or checked-in fixtures for deterministic
  reference cases. Generated fixture files must include provenance: Go test or
  symbol, source revision, generator version, and schema version.
- Establish CI for formatting, linting, unit tests, documentation, MSRV, and
  dependency/license checks.

Exit criteria:

- A minimal Rust usage example compiles.
- Fixture generation is reproducible and detects stale fixtures.
- No runtime networking or protocol behavior is claimed yet.

### Phase 1: Public compatibility model

Deliver:

- `Client`, `Config`, constructors, endpoint parsing, discovery policy, logging
  hooks, deterministic `close`, and drop behavior.
- Fluent builders and input/output conversion boundaries for all supported
  data-plane operations.
- Stable errors for unsupported operations and invalid configuration.
- Compile-time API tests and documentation examples.

Exit criteria:

- The public API covers every supported Go operation and every agreed
  configuration control.
- Defaults are listed in a Go-to-Rust compatibility table and tested.
- No public protocol or transport implementation types leak from the crate.

### Phase 2: CBOR and DynamoDB values

**Status: accepted.** The initial protocol slice passed full local validation
and a final parity review against the pinned Go reference. It implements
bounds-checked definite-length CBOR AttributeValues, bignums/decimals/sets,
lexdecimal range keys, item-key frames, and schema-compressed non-key values.
The protocol helpers remain explicitly unstable while Phase 3 integrates them
with request/response framing and client-owned schema caches.

Deliver:

- Bounds-checked CBOR reader/writer with explicit malformed-input errors.
- Exact encoding and decoding for all DynamoDB `AttributeValue` variants.
- Arbitrary-size integers, decimals, lexicographically sortable decimals, key
  and non-key item forms, attribute-list compression, and schema references.
- Property tests and byte-level golden fixtures imported from Go tests.

Exit criteria:

- Every Go CBOR fixture has an equivalent passing Rust case.
- Integer boundaries, empty/null distinctions, sets, nesting, decimal ordering,
  truncation, malformed lengths, and EOF behavior are covered.
- Fuzz targets exist for untrusted decoders.

### Phase 3: Request, response, validation, and errors

**Status: substantially implemented with bounded compatibility edges.** The
request/response codecs, validation, expression handling, DAX error mapping,
schema caches, connection pooling, TCP/TLS transport, SigV4 authorization,
retries, supported operations, discovery, routing, and lifecycle behavior are
covered by exact vectors and local fixtures. Unsupported fields and deliberately
bounded expression forms remain explicit errors. The hostname-verification
compatibility override is implemented as an explicit dangerous rustls verifier,
matching the Go client's insecure TLS compatibility behavior.

Deliver:

- DAX service/method identifiers and exact parameter ordinals.
- Encoders/decoders for all supported operations and DAX control calls.
- Client-side validators and bounded caches for attribute lists/key schemas.
- DAX, throttling, transaction cancellation, request ID, status, legacy, and
  I/O error translation.

Exit criteria:

- Representative CRUD, query, scan, batch, transaction, discovery, and error
  messages match golden bytes or normalized values.
- Omitted, defaulted, duplicate, and invalid fields match Go behavior.
- No unknown error is silently converted into a success-shaped result.

### Phase 4: Expressions and projections

Deliver:

- Projection path/ordinal processing.
- Projection, condition, key-condition, filter, and update expression parsing.
- Expression attribute name/value substitution and expression-specific
  validation.
- Reproducible parser generation or a documented handwritten implementation.

Current implementation note: the handwritten UpdateItem parser now supports
bounded section-aware action lists (`SET`, `REMOVE`, `ADD`, and `DELETE`) with
Go-compatible action and variable ordering. The full DynamoDB expression
grammar and complete Go corpus remain Phase 4 exit work.

Exit criteria:

- The full Go expression/projection corpus passes in Rust.
- Validity, error category, and token/position details match the compatibility
  contract.
- Differential tests cover both accepted and rejected expressions.

### Phase 5: SigV4 and connection handshake

Deliver:

- DAX-specific SigV4 canonical request, credential scope, signing key, session
  token handling, and authorization payload.
- Tube session handshake with magic `J7yne5G`, layering, session ID, user-agent
  header, client mode, and exact flush ordering.
- Clock and credential-provider abstractions for deterministic tests.

Exit criteria:

- All Go SigV4 vectors match.
- Handshake and authorization bytes match the reference fixture.
- Host, port, IPv6, UTC, credential refresh, and session-token cases pass.

### Phase 6: Single-node async transport, pooling, and retries

Deliver:

- Tokio TCP/TLS dialing with certificate verification and explicit hostname
  verification opt-out compatibility.
- Tube lifecycle, buffered I/O, auth refresh, deadlines, and cancellation.
- Bounded async pool with allocation limit, timeout, waiters, priority path,
  idle reuse, gauges, close behavior, and failed-tube disposal.
- Read/write retry policies, equal-jitter delay, injectable clock/sleep/random
  sources, and terminal error preservation.

Exit criteria:

- Deterministic concurrency tests cover allocation races, cancellation, close,
  waiter wakeup, dial failure, partial I/O, retry exhaustion, and tube reuse.
- No task, connection, permit, or waiter leaks under stress tests.
- Retry attempt counts and seeded delay vectors match Go.

### Phase 7: Cluster discovery, routing, and health

Deliver:

- Seed parsing, endpoint discovery, refresh, IPv4/IPv6 filtering, and per-node
  lifecycle.
- Health state machine, periodic checks, route listeners, random selection,
  route removal, fail-open thresholds, and temporary route-manager disablement.
- Internal metrics and structured logging that avoid credentials and item data.

Progress:

- Added the transport-independent `Endpoints` request codec and exact method
  vector.
- Added strict endpoint-roster response decoding, including node identity,
  hostname, raw address, port, role, availability zone, and leader session.
- Added `Client::discover_endpoints` so applications can explicitly inspect the
  configured seed's current roster. Multi-node routing and refresh remain
  intentionally pending until the endpoint-roster abstraction is in place.
- Routed endpoint discovery through the shared `ControlResolver`, preserving
  the same envelope/error handling boundary as the other DAX control calls.
- Added an internal endpoint-roster normalization boundary that converts raw
  IPv4/IPv6 bytes to socket addresses, applies `IpDiscovery`, preserves node
  metadata, and rejects malformed or empty usable rosters before routing.
- Added deterministic roster replacement semantics: duplicate node IDs are
  rejected, and validated refreshes produce added, removed, retained, and
  changed node sets before the active roster is replaced.
- Wired the validated roster into client state with atomic snapshot replacement:
  successful discovery commits a complete roster, while validation or family
  selection failures preserve the previous snapshot.
- Added a transport-independent route-table foundation with deterministic
  round-robin node selection and cursor reset on a complete roster replacement.
  Request execution remains on the single-endpoint executor until multi-node
  connection pooling is implemented.
- Added a non-networking `Client::active_endpoints` accessor for the last
  successfully validated roster; failed discovery cannot replace this snapshot.
- Consolidated the client’s committed roster and future route-selection state
  behind one `RouteTable`, preventing discovery snapshots and routing metadata
  from diverging during refresh.
- Added explicit `Client::refresh_endpoints` lifecycle semantics with a public
  `EndpointRefresh` diff; failed discovery or validation leaves the last valid
  route table unchanged.
- Serialized route-table commits so concurrent discovery calls cannot
  interleave replacement and cursor-reset operations; each refresh publishes one
  complete roster transition.
- Added route health state: unhealthy nodes are skipped, unknown nodes cannot be
  marked, recovered nodes rejoin selection, and a roster replacement removes
  stale health entries.
- Added regression coverage proving that a node removed by discovery and later
  re-added starts healthy rather than inheriting stale suppression state.
- Added explicit internal route-selection reasons for an undiscovered roster
  versus a roster where every node is currently unhealthy.
- Added Go-compatible bounded failure accounting: a node is excluded after
  three consecutive recorded failures and re-enters selection after a success.
- Added previous-node avoidance for retry selection, allowing failover to a
  different healthy route while preserving fallback behavior when no alternate
  route exists.
- Centralized resolved-node dial-address formatting, including bracketed IPv6
  literals, as the next transport integration boundary.
- Added transport-ready `RouteTarget` metadata containing node ID, dial address,
  and TLS server name.
- Factored transport dialing through a route-target-aware helper that reuses
  TCP/TLS setup and preserves the route’s TLS SNI name.
- Added a node-keyed routed transport-pool boundary with independent executor
  lookup/removal, lazy construction, route-key retention, and close-all
  semantics.
- Added route synchronization that closes and removes pools for departed nodes
  while preserving executors for retained routes.
- Added deterministic route-target snapshots derived directly from the
  committed roster, ensuring transport pool synchronization uses validated
  dial/SNI metadata rather than independently reconstructed addresses.
- Added health-filtered route-target snapshots so pool synchronization can
  exclude nodes past the Go-compatible failure threshold while retaining the
  full roster for diagnostics and refresh diffing.
- Wired discovery and refresh commits to prune departed or currently
  unhealthy node pools, and close all routed pools during client shutdown;
  existing retained pools remain reusable.
- Pool synchronization keys include stable node identity and dial address, so
  an address change for an existing node closes the old executor instead of
  reusing a stale connection pool.
- Wired data-plane request dispatch through discovered route selection with
  seed fallback before discovery, route success recovery, and three-failure
  pool eviction. Schema/control calls continue to use the configured seed.
- Added ordered multi-seed transport fallback for unencrypted configurations,
  preserving independent authenticated pools and trying later seeds after a
  failed seed request.
- Added per-request failover across healthy discovered routes after transport
  failures, while preserving direct propagation of DAX service responses.
- Restricted failover to transport/I/O failures so protocol and validation
  errors are not retried on unrelated nodes.
- Gated persistent route-health suppression and pool eviction behind the
  `route_manager_enabled` configuration, while retaining per-request
  alternate-route failover.
- Added Go-compatible two-thirds route-manager fail-open recovery that restores
  the complete validated roster before suppression leaves too few active
  routes.
- Added temporary route-manager disablement after three fail-open events
  within two health-check intervals, with automatic ten-minute recovery.
- Added lazy background endpoint refresh driven by `cluster_update_interval`,
  with immediate initial discovery, validated atomic commits, and deterministic
  cancellation on close.
- Added per-route `Endpoints` health probes driven by
  `client_health_check_interval`, with one-second deadlines and integration
  with transport-error classification and per-node pool replacement.
- Separated the Go-compatible five-consecutive-health-failure threshold from
  the three-failure request route-suppression threshold.
- Matched Go health probes' explicit three-retry control request budget before
  counting a transport failure.
- Matched Go health-probe replacement timing by removing a failed route pool
  immediately; consecutive read-failure thresholding remains separate.
- Added recovery coverage proving a successful health probe clears both
  failure domains.
- Added explicit seed hostname resolution with configured IPv4/IPv6 filtering
  while preserving discovered-route dialing and TLS SNI behavior.
- Added focused seed-resolution coverage for family ordering, per-address
  ports, and unsupported-family failures.
- Matched Go seed fallback ordering by attempting each selected resolved
  address sequentially before moving to the next configured seed.
- Added lifecycle regression coverage proving `Client::close` cancels and
  removes all started background task handles.
- Added paused-Tokio timing coverage for immediate scheduler startup and
  configured periodic ticks.
- Added paused-Tokio coverage for the health task's delayed first probe cycle.
- Added paused-Tokio coverage for the idle-reaper task's delayed first cycle.
- Added paused-Tokio coverage for the one-millisecond zero-duration safeguard.
- Kept Tokio's paused-time `test-util` feature in dev dependencies only and
  verified the crate packages successfully.
- Closed a scheduler-start race by rechecking the shared closed state while
  holding each task mutex, with regression coverage for closed-client startup.
- Made task-slot removal and abort occur under the same scheduler mutex to
  close the complementary shutdown/start interleaving.
- Added finished-task slot recovery and regression coverage so unexpectedly
  exited schedulers can be recreated.
- Explicit successful discovery and refresh now clear stale background-refresh
  diagnostics.
- Fixed final-drop lifecycle cleanup with an explicit client-owner counter so
  scheduler-held shared state cannot prevent task cancellation.
- Added clone/drop lifecycle coverage proving non-final clone drops preserve
  shared background tasks.
- Added close-across-clones coverage proving explicit close remains client-wide
  and cancels shared schedulers immediately.
- Added repeated clone/drop-cycle coverage for owner-counter stability.
- Added periodic idle-tube reaping across all transport pools driven by
  `idle_connection_reap_delay`, with deterministic shutdown.
- Hardened refresh, health-check, and reaping schedulers against zero-valued
  durations with a one-millisecond minimum tick.
- Added a thread-safe last-background-refresh-error diagnostic with warning
  logging and successful-refresh clearing.
- Added read-only `Client::route_snapshot()` observability for discovered node
  targets, TLS names, request-suppression health, and health-probe status.
- Added structured decoding of Go-compatible transaction cancellation reason
  codes and messages in DAX errors.
- Added public API coverage for retaining structured transaction cancellation
  reasons.
- Applied the shared CBOR container limit to cancellation-reason arrays before
  allocation.
- Preserved optional raw CBOR item payloads attached to cancellation reasons
  for later schema-aware reconstruction.
- Added context-aware `TransactionCancellationReason::decode_item`, restoring
  request key attributes and decoding compressed non-key attributes through the
  caller-provided attribute-name-list cache.
- Added public edge-case coverage for absent payloads and unknown attribute-list
  IDs.
- Added an exact non-null cancellation-item wire vector without exposing a
  misleading schema-free decoder.
- Rejected malformed cancellation-item CBOR before exposing raw payload bytes.
- Added Go-compatible normalized categories for transaction conflict,
  transaction-in-progress, and idempotent-parameter-mismatch failures.
- Added Go-compatible normalized categories for resource-in-use,
  item-collection-size-limit, and limit-exceeded failures.
- Added a compile-checked public API example for SDK configuration and
  expression-free `GetItem`.
- Added a live Scan paginator fixture covering continuation propagation,
  persistent transport reuse, cached attribute names, and terminal pages.
- Added a live Query paginator fixture covering continuation propagation,
  persistent transport reuse, cached attribute names, and terminal pages.
- Added direct BatchGet response-codec coverage for unprocessed-key decoding
  and preservation of consistency/projection request options.
- Added direct BatchGet codec coverage for normal response item decoding,
  key restoration, projected response reconstruction, expression-name alias
  preservation, and trailing-data rejection.
- Added public BatchGet validation parity for empty request maps and empty
  per-table key lists before any network I/O.
- Added public BatchWrite validation coverage for empty request maps before
  any network I/O.
- Added BatchGet paginator state coverage for empty unprocessed-key maps,
  continuation retries, and opt-in duplicate-token stopping.
- Added expression regression coverage for missing filter placeholders and
  unsupported nested functions.
- Added public transaction-cancellation coverage for composite-key
  reconstruction and incomplete-key rejection.
- Corrected README capability statements to reflect implemented discovery,
  routing, batch/transaction operations, and projected response decoding.
- Removed additional stale README claims that supported operations were
  transport-unavailable or projected responses were not yet decoded.
- Completed typed service-error category parity for all known Go error-code
  branches represented by the Rust public error model.
- Explicitly ran the pinned Go-reference handshake fixture test successfully.
- Re-ran the checked-in Go-reference handshake metadata fixture through the
  ignored-test path successfully.
- Added a paused-Tokio controlled-transport refresh fixture covering the
  initial and interval-driven endpoint refresh requests.
- Added a paused-Tokio controlled-route health fixture covering a real
  interval-driven `Endpoints` probe.
- Refresh and health scheduler paths now have real controlled-transport
  fixtures; idle-reaper behavior is covered by the direct pooled-transport
  eviction fixture and paused-time scheduler timing tests.
- Ran `cargo deny check`; license and source policies pass, while the
  transitive AWS SDK legacy hyper/rustls advisory exceptions are documented in
  `deny.toml`.
- Replaced brittle transport-message matching with structured transport error
  categories shared by routed failover and health probes.
- Added regression coverage preventing protocol-message wording from being
  misclassified as a transport failure.

Exit criteria:

- Go cluster, route manager, and health state-machine cases are represented and
  pass.
- Tokio time is paused in timer-sensitive tests.
- Discovery refresh, shutdown races, IPv6/SNI formatting, and degraded-cluster
  behavior are covered.

### Phase 8: Paginators, integration, and release hardening

Deliver:

- Query, Scan, and BatchGet paginators with AWS SDK-style fluent ergonomics.
- Duplicate-token stopping, empty pages, option application, error propagation,
  and unprocessed-key behavior.
- Cross-language conformance runner, opt-in live DAX tests, examples, README,
  migration guide, changelog, license/notice files, and release checklist.

Query and Scan now expose lazy public paginators. They preserve the original
operation inputs, apply continuation keys page by page, stop on empty or
duplicate tokens, and propagate transport/validation errors.

BatchGetItem now exposes a lazy paginator that retries `UnprocessedKeys` while
preserving the original request options and supports duplicate-token stopping.

Exit criteria:

- All unit, property, differential, stress, documentation, and approved live
  tests pass.
- Public API documentation contains complete examples.
- Compatibility exceptions are explicit, justified, and approved.
- The crate can be packaged without the local Go clone or generated junk.

## Validation strategy

### Compatibility matrix

Maintain a checked-in matrix during implementation with one row per:

- public constructor, configuration field, operation, and paginator;
- validator and error category;
- protocol control call and data-plane call;
- discovery, routing, health, retry, timeout, TLS, and close behavior.

Each row identifies the Go source symbol/test, Rust symbol/test, parity status,
and any approved deviation.

### Test layers

1. **Compile/API tests:** Rust developer ergonomics and type compatibility.
2. **Unit tests:** pure state machines, validation, parsers, caches, and builders.
3. **Golden tests:** CBOR, requests, responses, handshake, SigV4, and errors.
4. **Property tests:** value round trips, decimal ordering, and parser/decoder
   invariants.
5. **Differential tests:** run equivalent vectors through Go and Rust and compare
   normalized results.
6. **Concurrency tests:** paused time, deterministic randomness, stress, and
   Loom-style modeling where practical.
7. **Fuzzing:** all untrusted byte decoders and expression parsing.
8. **Live tests:** opt-in execution against a disposable DAX cluster.

Golden fixtures are necessary but not sufficient: tests must verify meaningful
fields and required thresholds rather than only snapshotting large structures.

## Major risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Rust SDK models/builders differ from Go SDK types | Decide the public boundary in Phase 0; isolate conversions and pin a supported SDK range. |
| Generated grammar source is absent | Recover and license the grammar before Phase 4; use the Go test corpus as the acceptance contract. |
| A generic CBOR crate changes wire representation | Keep DAX-specific encoding under crate control and prove it with byte fixtures. |
| Async pooling introduces deadlocks or leaks | Use bounded primitives, explicit ownership, deterministic cancellation tests, stress tests, and model checking where useful. |
| Error details are lost behind generic Rust errors | Define a crate-owned structured error and preserve source/DAX metadata. |
| SigV4 helper APIs do not support DAX's connection flow | Implement the small DAX-specific canonicalization layer using audited crypto primitives and Go golden vectors. |
| Go tests rely on timing or randomness | Inject clocks, sleepers, and seeded RNGs; use paused Tokio time. |
| Local reference clone is accidentally published | Keep `aws-dax-go-v2/` ignored and validate `cargo package --list`. |
| Sensitive values appear in diagnostics | Treat credentials, signatures, keys, and item data as secrets; test redacted `Debug` and logs. |

## Agent workflow

Repository-local agents live in `.github/agents/`:

- `dax-reference-analyst`: extracts a bounded contract or fixture set from the
  pinned Go implementation without editing Rust code.
- `dax-port-implementer`: implements one approved phase or vertical slice,
  including tests and documentation.
- `dax-parity-reviewer`: performs a read-only comparison of a completed slice
  against its Go contract and reports actionable gaps.

For each implementation slice:

1. The reference analyst records the exact Go symbols, behavior, tests, and
   fixture provenance.
2. The implementer changes only the approved slice and runs targeted validation.
3. The parity reviewer compares public behavior and protocol output, then reports
   blockers before the slice advances.
4. The implementer resolves confirmed findings and reruns validation.

Agents must not work on overlapping writable scopes concurrently.

## First review checklist

Before coding, approve or change:

- crate name and minimum supported Rust version;
- AWS SDK dependency/version policy;
- fluent-builder public API direction;
- Tokio runtime requirement;
- structured error contract;
- parser generation strategy;
- live integration-test policy;
- eight-phase ordering and exit criteria;
- the three agent roles and their boundaries.
