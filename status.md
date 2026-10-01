# AWS DAX Rust Port — Status Update

**Date:** 2026-10-03

## Overall progress

The Go-to-Rust port of `aws-dax-go-v2` is progressing incrementally, with a focus on preserving AWS SDK-style developer ergonomics and DAX wire/protocol compatibility.

## Completed functionality

- Core client lifecycle, configuration, validation, error handling, transport, pooling, retries, TLS/SigV4 authorization, and response drainage.
- Direct operations:
  - `GetItem`
  - `PutItem`
  - `DeleteItem`
  - `Query`
  - `Scan`
  - `UpdateItem`
- Batch operations:
  - `BatchWriteItem`
  - `BatchGetItem`
- Transactions:
  - `TransactGetItems`
  - `TransactWriteItems`
- DAX CBOR request and response codecs.
- Compressed attribute encoding and decoding.
- Schema and attribute-list resolution.
- Projections, aliases, nested document paths, and list indexes.
- Consumed-capacity and item-collection-metrics decoding.
- Bounded expression support for:
  - Filters and key conditions.
  - Update actions.
  - Predicates.
  - Arithmetic.
  - `if_not_exists`.
  - `list_append`.
- AWS SDK-style paginators for Query, Scan, and BatchGet, including continuation-token handling and duplicate-token protection.

## Latest work

- Added Go-compatible `Endpoints` discovery request encoding.
- Added endpoint-roster response decoding for:
  - Node ID.
  - Hostname.
  - Address.
  - Port.
  - Role.
  - Availability zone.
  - Leader session ID.
- Added public `Client::discover_endpoints`.
- Added the public `DiscoveredEndpoint` type.
- Added valid and invalid endpoint codec tests.
- Added validated, atomic endpoint-roster replacement with IPv4/IPv6 policy
  selection, route diffs, round-robin selection, health thresholds, and
  previous-node avoidance.
- Added node-keyed routed transport pools with address-change invalidation,
  lazy per-route executor construction, and deterministic shutdown.
- Wired data-plane operations through discovered route selection with seed
  fallback before discovery, route success recovery, and pool eviction after
  three consecutive failures. Schema/control calls remain on the configured
  seed.
- Added ordered multi-seed transport fallback for unencrypted configurations;
  each seed retains its own authenticated connection pool and is tried after
  the previous seed fails.
- Added per-request routed failover: transport failures mark the selected
  node and retry the request on another healthy discovered node when
  available, while DAX service responses remain unchanged. Persistent route
  suppression is now gated by `route_manager_enabled`, matching Go behavior.
- Added Go-compatible two-thirds fail-open recovery: when enabled route
  suppression would reduce active routes below two-thirds of the validated
  roster, the complete roster is restored for another attempt.
- Added temporary route-manager disablement after three fail-open events
  within two health-check intervals, with ten-minute automatic recovery.
- Added lazy background endpoint refresh using `cluster_update_interval`, with
  an immediate first discovery, atomic roster commits, and deterministic
  cancellation during client close.
- Added periodic per-route `Endpoints` health probes gated by
  `route_manager_enabled`, with one-second deadlines, route recovery, failure
  transport-error classification, an independent five-failure health
  threshold, three internal retries, pool replacement, and deterministic
  cancellation.
- Corrected periodic health-probe parity so transport or timeout failure
  immediately replaces the affected pool; the five-failure threshold remains
  separate from probe replacement.
- Added regression coverage proving a removed health-check pool is closed and
  recreated rather than reused.
- Added route-table coverage proving a successful health probe clears
  three-failure suppression and makes the node selectable again.
- Added coverage proving a successful health probe clears both the independent
  five-failure health counter and any request-suppression state.
- Added explicit seed hostname resolution with `ip_discovery` family
  filtering, ordered address attempts, and preserved TLS SNI; routed addresses
  remain direct validated socket targets.
- Added seed address-selection tests covering family preference, per-address
  ports, and unmatched-family rejection.
- Changed selected seed-address dialing to ordered sequential attempts before
  advancing to the next configured seed, matching the Go fallback lifecycle.
- Added deterministic lifecycle cancellation coverage for refresh, health, and
  idle-reaping tasks.
- Added paused-Tokio timing coverage proving scheduler intervals tick
  immediately and then honor their configured period.
- Added paused-Tokio coverage proving health probes wait one configured
  interval after task startup before their first probe cycle.
- Added paused-Tokio coverage proving idle reaping waits one configured
  interval before its first reap cycle.
- Added paused-Tokio coverage proving zero-valued scheduler durations use the
  one-millisecond safety tick.
- Added a paused-Tokio controlled-transport fixture proving the refresh task
  performs its initial endpoint discovery and a subsequent scheduled refresh.
- Added a paused-Tokio controlled-route fixture proving the health scheduler
  executes an `Endpoints` probe against a discovered route.
- Kept Tokio's `test-util` feature test-only while validating the publishable
  crate package.
- Closed a scheduler-start race by rechecking the shared closed state while
  holding each task mutex, so `close()` cannot miss a task spawned concurrently.
- Added coverage proving a closed client cannot start lifecycle tasks again.
- Made scheduler shutdown atomic with task-slot removal, preventing a
  concurrent starter from filling a slot after shutdown has taken it.
- Added finished-task slot recovery so an unexpectedly exited scheduler can be
  recreated on the next lazy lifecycle trigger.
- Added coverage for replacement of finished refresh, health, and reap tasks.
- Explicit successful discovery and refresh also clear stale background-refresh
  diagnostics, with direct regression coverage for the clearing behavior.
- Fixed final-drop cleanup by tracking explicit `Client` owner count instead
  of relying on the shared closed-flag `Arc` count, which background tasks
  retain.
- Added clone-lifecycle coverage proving dropping one client clone leaves
  shared schedulers alive until the final owner closes or drops.
- Added explicit close-across-clones coverage proving one close cancels shared
  schedulers and marks every clone closed.
- Added repeated clone/drop-cycle coverage for the explicit client-owner
  counter.
- Hardened all lifecycle schedulers against zero-valued durations by clamping
  them to a one-millisecond tick.
- Added last-background-refresh-error diagnostics with warning logging and
  clearing after a successful background refresh, while preserving the last
  valid roster.
- Added read-only `Client::route_snapshot()` observability for discovered node
  targets, TLS names, request-suppression health, and health-probe status.
- Added transaction-cancellation envelope decoding with structured per-item
  reason codes and messages.
- Added public-model coverage proving callers can retain and compare structured
  transaction cancellation reasons.
- Bounded cancellation-reason arrays before allocation and covered oversized
  wire inputs.
- Preserved optional raw CBOR item payloads attached to transaction
  cancellation reasons.
- Added distinct error categories for transaction conflict, transaction in
  progress, and idempotent parameter mismatch responses.
- Added Go-compatible categories for resource-in-use, item-collection-size-limit,
  and limit-exceeded responses.
- Added a compile-checked `examples/get_item.rs` entry point for the public
  SDK-style configuration and operation flow.
- Completed the typed Go DAX service-error category mapping without collapsing
  known transaction, limit, or not-implemented failures into `Unknown`.
- Added exact wire coverage for non-null compressed cancellation item payloads;
  schema-aware reconstruction remains operation-context dependent.
- Replaced routed failover's transport-message substring matching with
  structured `TransportErrorKind` classification for I/O, timeout, and
  unexpected-EOF failures.
- Added regression coverage ensuring protocol messages containing connection
  wording cannot trigger routed failover.
- Restricted routed request failover to transport/I/O failures; protocol,
  validation, and DAX service errors now return directly, with explicit
  classifier coverage for generic protocol failures.
- Added periodic idle-tube reaping across seed and routed pools using
  `idle_connection_reap_delay`, with deterministic cancellation.
- Matched the Go expression lexer’s ASCII whitespace behavior and rejected
  unsupported Unicode whitespace across projection, key-condition, filter, and
  update expressions.
- Matched Go DAX service-error and retry classification when the wire code
  sequence includes the optional intermediate code before the operation
  category (for example, `[4,37,38,39,50]`), with matrix coverage for all
  mapped categories, including the Go-specific `[4,37,38,44]` not-implemented
  shape. Transport-level regressions also prove that validation and
  not-implemented responses remain non-retryable, while unknown intermediate
  code shapes and truncated service sequences remain unmapped and
  non-retryable.
- Confirmed schema cache misses are transport-backed in the client path:
  `DefineKeySchema`, `DefineAttributeListId`, and `DefineAttributeList` are
  single-flight control loads rather than deferred request-codec failures.
- Added cross-surface regression coverage proving unsupported Unicode
  whitespace is rejected consistently in projection, key-condition, filter,
  and update expressions.
- Added transport-level coverage proving independent read and write retry
  budgets are applied by operation type, not only preserved in configuration.
- Added explicit retry-budget routing coverage for every write operation and
  representative read/control operation, including schema and endpoint loads.
- Added end-to-end transport coverage proving a non-retryable DAX response is
  returned immediately without consuming the remaining retry budget.
- Added deterministic Tokio-time coverage proving retryable responses wait for
  the configured fixed delay before the next attempt.
- Added Go-compatible equal-jitter throttling backoff with injectable test
  randomness, including direct and intermediate-code throttling envelopes.
- Added deterministic coverage proving the fixed delay is not applied after
  the final permitted attempt.
- Added BatchGet CBOR coverage for pages that return decoded items alongside
  unprocessed keys, preserving request metadata for continuation.
- Added BatchGet paginator coverage proving duplicate-token detection compares
  the full `KeysAndAttributes` request metadata, matching Go deep-equality
  behavior.
- Added BatchGet paginator coverage proving distinct unprocessed-key pages
  advance until the response is exhausted.
- Added BatchGet paginator coverage proving an explicit empty
  `UnprocessedKeys` map terminates pagination after a prior continuation.
- Added a Go golden-vector regression for nested aliased update paths with
  list indexes, including exact DAX document-path encoding.
- Added the adjacent Go golden-vector regression for `REMOVE` actions on
  indexed document paths.
- Added exact Go golden vectors for numeric `if_not_exists` and `list_append`
  update expressions.
- Added the Go numeric subtraction golden vector for `SET Price = Price - :p`.
- Added Go numeric `ADD` and `DELETE` golden-vector coverage for update
  actions.
- Added a Go case-insensitive `begins_With` filter golden-vector regression.
- Added a Go case-insensitive `attribute_type` filter golden-vector regression.
- Added a Go case-insensitive `contains` filter golden-vector regression.
- Added a Go case-insensitive `attribute_exists` filter golden-vector regression.
- Added a Go case-insensitive `attribute_not_exists` filter golden-vector regression.
- Added a Go case-insensitive `size` filter golden-vector regression.
- Added the Go aliased `attribute_not_exists(#a.k1)` filter golden vector.
- Added Go literal-operand `begins_With(a, substr)` filter encoding.
- Added the Go multi-action numeric `DELETE Color :p, Color_2 :p` vector.
- Added update placeholder ordering coverage for repeated and distinct values.
- Added cross-section update placeholder reuse coverage for SET and ADD.
- Added Go attribute-to-attribute comparison encoding for negated filters.
- Added the direct Go `a1 = a2` attribute-comparison vector.
- Added the Go numeric placeholder `a1 <> :v1` comparison vector.
- Added the Go compound numeric comparison vector for `AND` filters.
- Added the paired Go compound numeric comparison vector for `OR` filters.
- Added Go literal-operand `IN (b,c,d)` filter encoding.
- Added mixed literal/placeholder `IN` coverage, preserving operand IDs and
  value-table ordering.
- Added exact Go projection vectors for simple, nested, indexed, and aliased
  document paths.
- Added Go expression-error regressions for invalid projections, missing
  placeholders, unused condition values, and unsupported update functions.
- Added the Go unused projection-alias regression across a query request.
- Added condition-expression nested-function regressions and corrected their
  surface-specific error label.
- Added the Go malformed signed-index key-condition regression with the
  document-path error classification.
- Added the Go nested `if_not_exists(list_append(...))` update-error case.
- Added the exact Go two-condition key-condition benchmark vector.
- Added the exact Go standalone `attribute_exists(a)` function vector.
- Added the exact Go standalone `CONTAINS(a, :v)` function vector.
- Added the exact Go standalone `attribute_type(a, S)` function vector.
- Added the exact Go standalone literal `begins_With(a, substr)` function
  vector.
- Added the exact Go standalone `a > size(c)` function vector.
- Added a dedicated exact Go aliased `attribute_not_exists(#a.k1)` vector.
- Added a direct exact Go nested aliased/indexed update payload vector.
- Added a direct exact Go indexed `REMOVE` update payload vector.
- Added a direct exact Go numeric `ADD` update payload vector.
- Added a direct exact Go numeric `DELETE` update payload vector.
- Added a direct exact Go numeric subtraction update payload vector.
- Added a direct exact Go numeric `list_append` update payload vector.
- Added a direct exact Go numeric `if_not_exists` update payload vector.
- Added a direct exact Go `BETWEEN` filter payload vector.
- Added a direct exact Go repeated-placeholder numeric `DELETE` payload vector.
- Added Go-compatible equal-jitter throttling backoff with configurable base
  and maximum delays, zero-value default restoration, cap-before-jitter
  behavior, and deterministic transport coverage.
- Added BatchGet CBOR coverage for pages containing both decoded responses and
  unprocessed keys, plus multi-page paginator continuation coverage.
- Added cargo-fuzz targets for item-key and transaction-cancellation decoding.
- Added an ignored, environment-gated live smoke harness for plaintext and TLS
  DAX endpoints covering PutItem, GetItem, Query, and cleanup.
- Added Query and Scan `IndexName` request encoding for GSI and LSI access.
- Verified live GSI Query, LSI Query, filtered Scan, and UpdateItem
  round-trips on both plaintext and TLS DAX endpoints.
- Added live BatchGet coverage and paginator execution for response pages
  containing DAX's raw compressed-attribute payload form.
- Accepted DAX's empty-array response for mutation operations with no returned
  attributes, matching live DeleteItem behavior.
- Added bounded indefinite-length CBOR array and map decoding for DAX response
  containers, compressed attribute values, and consumed-capacity index maps.
- Verified the live smoke harness against the provisioned `DaxParityTable`:
  PutItem, GetItem, Query, and cleanup pass on both plaintext and TLS DAX
  endpoints.
- Stabilized the deterministic retry-delay fixture against concurrent paused
  Tokio-clock tests by keeping its request timeout outside the test clock
  window.

## Validation

- `cargo test --all-targets`: **342 tests passed** (285 library tests, 12
  CBOR integration tests, 3 fixture tests, and 42 public-model/live-harness
  tests), with two intentionally ignored live/reference tests.
- `cargo clippy --all-targets --all-features -- -D warnings`: passed.
- `cargo fmt --check`: passed.
- `cargo doc --no-deps`: passed.
- `git diff --check`: passed.
- Live EC2 smoke test: passed on both configured plaintext and TLS endpoints
  with IAM-role credentials.
- Live BatchGet/paginator test: passed on both configured plaintext and TLS
  endpoints.
- Live BatchWriteItem test: passed with a minimal PutRequest and cleanup delete
  on both configured plaintext and TLS endpoints.
- Live Query and Scan paginator tests: passed across multiple pages with a
  one-item page limit on both configured endpoints.
- Live TransactWriteItems test: passed with two unique Put actions on both
  configured endpoints.
- `cargo package --allow-dirty`: passed, including package verification.
- `cargo test --all-targets -- --ignored`: passed for the checked-in
  Go-reference handshake metadata fixture.
- `cargo deny check`: passed with documented exceptions for advisories confined
  to the AWS SDK's transitive legacy hyper/rustls compatibility graph;
  duplicate-version findings remain warnings.
- `cargo-fuzz` targets cover standalone AttributeValue, schema-compressed
  non-key item decoding, item-key decoding, and transaction-cancellation item
  reconstruction; corpus expansion remains optional follow-up work.

## Current status

The implementation is stable for the supported operation, discovery, routing,
transport, and lifecycle slices. Exact protocol vectors, local transport
fixtures, and clone/close/drop scheduler coverage cover the implemented
behavior.

## Completed status

The following implementation and validation work is complete:

- Direct, batch, transaction, pagination, routing, discovery, health, retry,
  transport, lifecycle, expression, CBOR, compressed-attribute, and
  transaction-cancellation compatibility slices listed above.
- TransactGetItems live validation after correcting the key-frame
  double-wrapping divergence.
- Endpoint discovery and refresh on both live plaintext and TLS clusters,
  including IPv4-selected route snapshots.
- Route-health suppression and recovery at the Go-compatible thresholds in
  deterministic tests.
- Release-readiness checks: `cargo deny check`, `cargo doc --no-deps`,
  `cargo package --allow-dirty --list`, formatting, diff checks, Clippy, and
  the complete local test suite.

## Deferred validation

These are not known implementation defects; they require infrastructure or
optional additional confidence work:

- Live node-failure, route manipulation, failover, and recovery scenarios.
  The current DAX cluster setup has no safe failure-injection control plane.
- Additional expression differential vectors remain optional follow-up work.

The initial fuzz-corpus expansion is now complete: all four decoder targets
have seeded empty, valid, truncated, and/or indefinite-length CBOR inputs and
each completed a bounded 100-run nightly `cargo fuzz` smoke session without
crashes.

## Release-gated actions

These actions remain intentionally pending release approval:

- Choose and approve the crate version and release date.
- Update `Cargo.toml`, `Cargo.lock`, `CHANGELOG.md`, and compatibility
  documentation for the approved release.
- Run the release checklist on the approved Rust toolchain matrix.
- Commit, tag, publish, and announce the release.

No current item represents a known local implementation blocker.

## Tracking files

- [PORTING_PLAN.md](./PORTING_PLAN.md)
- [README.md](./README.md)
