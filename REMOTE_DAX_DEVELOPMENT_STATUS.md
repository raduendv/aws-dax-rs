# Remote DAX Development Status

**Last updated:** 2026-10-03

This file tracks execution of
[REMOTE_DAX_DEVELOPMENT_PLAN.md](./REMOTE_DAX_DEVELOPMENT_PLAN.md). It is
separate from the crate implementation status so infrastructure results,
environment blockers, and live coverage can evolve independently.

## Fresh operation audit — 2026-10-03

| Operation or surface | Status | Result |
|---|---:|---|
| PutItem, GetItem, DeleteItem | ✅ | Reached and verified by the live smoke |
| Query, Scan, GSI Query, LSI Query | ✅ | Direct live calls completed |
| BatchWriteItem | ✅ | Live write completed |
| TransactWriteItems | ✅ | Remote transaction example passed |
| TransactGetItems | ✅ | Two responses returned |
| Endpoint discovery/refresh | ✅ | Live discovery test passed |
| Query paginator | ✅ | Six expected items returned exactly once on a pinned route |
| Scan paginator | ✅ | Six expected filtered items returned exactly once |
| UpdateItem verification | ✅ | Bounded GetItem polling observed the updated value |
| BatchGetItem | ✅ | Live response and paginator completed |

The full plaintext and TLS live matrices pass. Continuation requests use
paginator-local route affinity, routed Scan connections are quarantined after
their page response, and endpoint-specific test keys avoid cross-cluster cache
propagation interference.

## Environment status

| Resource | Status | Notes |
|---|---|---|
| EC2 validation host | Ready | `ec2-user@18.201.46.195` |
| IAM role credentials | Ready | No static credentials copied |
| `DaxParityTable` | Ready | `eu-west-1`, PK/SK table keys |
| Plaintext DAX endpoint | Passing | `dax://...` live smoke path |
| TLS DAX endpoint | Passing | `daxs://...` live smoke path |
| Local live harness | Ready | Ignored and gated by `DAX_LIVE_TESTS=1` |

## Completed live coverage

- PutItem, GetItem, base-table Query, and DeleteItem cleanup.
- GSI Query using `IndexName = GSI1`.
- LSI Query using `IndexName = LSI1`.
- Filtered Scan.
- UpdateItem followed by GetItem verification.
- BatchGetItem response decoding.
- BatchGet paginator execution with no remaining unprocessed keys.
- Plaintext and TLS endpoint execution for the above operations.
- Real DAX indefinite-length response containers.
- Real DAX raw compressed-attribute BatchGet payloads.
- BatchGet response decoding with live schema attribute-list resolution.
- BatchWriteItem with a live PutRequest and subsequent BatchGet verification.
- Multi-page Query and Scan paginator execution with a one-item page limit.
- TransactGetItems with two records returned from a live transaction.
- Runtime-backed cleanup of every run-scoped item, including cleanup after
  assertion or request failures.
- UpdateItem live verification now uses bounded polling to account for the
  cluster's observed post-write cache propagation delay.
- Endpoint discovery and refresh now pass against both plaintext and TLS
  clusters, including route snapshots for the address-family-selected roster.
- TransactGetItems now passes against the live TLS endpoint after correcting
  the key-frame double-wrapping divergence.
- TransactWriteItems now passes remotely after matching Go's complete
  parallel-array framing. The fix removed duplicate CBOR array headers around
  condition and update arrays; compressed value framing, transaction key
  frames, and generated client request tokens also match Go.
- Request-failure suppression and health-probe recovery are covered by
  deterministic route-table tests at the Go parity thresholds (three request
  failures and five probe failures).

## Protocol findings resolved

- Real Query responses used indefinite-length CBOR containers.
- Real BatchGet responses used raw compressed-attribute payloads inside the
  response item array rather than the locally assumed byte wrapper.
- BatchGet may omit the consumed-capacity array when no capacity was requested.
- DeleteItem with no returned attributes may return an empty CBOR array rather
  than a response map.
- Query and Scan index names needed to be encoded as request parameter `11`.
- Endpoint discovery accepts omitted hostname fields used by the Go client and
  falls back to the configured seed hostname for TLS routing.
- Stream framing accepts large integer values without weakening string and
  container bounds.

Each finding now has deterministic local coverage in the Rust test suite where
the wire shape is reproducible.

## Deferred validation

- Broader degraded-cluster scenarios, especially two-node blocking, remain
  deferred. The opt-in one-node route harness has been exercised safely on the
  EC2 validation host: reads recovered and BatchWriteItem succeeded, while
  TransactWriteItems returned an opaque status 500 before the final wire
  framing fix. The normal transaction path now passes; transaction failover
  should be rerun with the fixed encoder before being marked as passing.

## Next execution order

### Phase 1: Confidence validation

1. Run the ignored live smoke, batch, transaction, pagination, discovery, and
   refresh tests on both plaintext and TLS endpoints.
2. Record route-health suppression and recovery against deterministic
   threshold tests.
3. Attempt controlled degraded-cluster validation only when a safe node or
   route failure control is available; never induce failure against the
   shared cluster without an explicit rollback procedure.

Phase 1 result: smoke, batch, transaction, pagination, discovery, and refresh
paths have passed on the available plaintext/TLS clusters. Controlled
multi-node failure injection remains deferred because no safe failure control
plane is available; one-node outbound-route validation is covered by the
explicit harness below.

An explicit opt-in fault-injection harness is now available at
`scripts/live_fault_injection.sh`. It discovers route IPs through the Rust
client, blocks one or two outbound IPv4 routes with `iptables`, runs the live
workload, and removes every rule through an exit trap. It must only be run on
an isolated validation host with an approved rollback window:

```sh
DAX_FAULT_INJECTION=1 \
DAX_ENDPOINT=daxs://radu-rust-tls.cykcls.dax-clusters.eu-west-1.amazonaws.com \
DAX_BLOCK_COUNT=1 \
./scripts/live_fault_injection.sh
```

The harness has been run against the TLS cluster with one blocked route.
Read and write workloads passed, while the transaction workload produced the
expected structured failure result. Firewall cleanup was verified by the exit
trap. Cleanup requests are bounded so a blocked route cannot hang the
harness. The initial transaction result exposed Rust implementation gaps: routed node
executors inherited node-local retry budgets instead of matching Go's
cluster-level retry boundary, and operation control resolution used the seed
transport instead of the selected routed client. Rust now disables retries in
routed executors and resolves key schemas and attribute-list IDs through the
routed protocol path. The original opaque transaction 500 is therefore
treated as a Rust parity defect under active revalidation, not an accepted
server limitation.

The harness exits unsuccessfully if read or write workloads do not report
success; transaction outcomes are reported through the structured
fault-result line. A live run must prewarm discovery on the client before
applying firewall rules to model an established DAX client; starting a fresh
client after blocking the seed can only validate seed-bootstrap behavior.

A controlled two-node TLS run was also completed with `DAX_BLOCK_COUNT=2`.
Both firewall rules were active during the workload and the exit trap
reported successful cleanup. With two of three IPv4 routes blocked, the
read and write workloads timed out during schema/bootstrap, while the
transaction workload reached a successful `TransactGetItems` result through
the remaining route. This is recorded as expected degraded-cluster
exhaustion, not as a general failover pass.

### Phase 2: Fuzzing hardening

1. Maintain dedicated fuzz targets for AttributeValue, compressed attributes,
   item keys, and transaction-cancellation payloads.
2. Seed each target with minimal valid, empty, truncated, indefinite-length,
   and malformed CBOR inputs.
3. Run bounded local fuzz smoke sessions and preserve any minimized regression
   inputs under the corresponding `src/aws-dax-fuzz/corpus` directory.
4. Re-run the full Rust validation matrix after corpus or decoder changes.

Phase 2 result: all four fuzz targets completed bounded 100-run smoke
sessions with seeded corpora and no crashes. Local fuzzing requires a nightly
Rust toolchain because `cargo-fuzz` enables sanitizer instrumentation.

The phase was expanded with an expression-encoding fuzz target and seeded
corpus. All five targets completed extended 1,000-run nightly smoke campaigns
without crashes:

- `decode_attribute_value`
- `decode_compressed_attributes`
- `decode_item_key`
- `decode_transaction_cancellation_item`
- `encode_filter_expression`

The campaigns retained minimized coverage-increasing inputs in the tracked
corpora. No fuzz artifact or crash input was produced.

The confidence campaign was extended to 2,000 runs per target on nightly
Rust. All five targets completed successfully without crashes or sanitizer
artifacts:

- `decode_attribute_value`
- `decode_compressed_attributes`
- `decode_item_key`
- `decode_transaction_cancellation_item`
- `encode_filter_expression`

The pinned Go expression corpus was reviewed against the Rust request tests.
All distinct reference vectors are represented, including projections,
comparisons, logical operators, negation, `IN`, `BETWEEN`, supported
expression functions, aliases, document paths, update actions, and malformed
or unsupported-expression cases. No additional differential vector was
needed without duplicating existing coverage.

### Phase 3: Release gate

1. Obtain release approval, choose the release version/date, and execute the
   release checklist.

The local release-readiness checks currently pass for dependency policy,
documentation generation, and package-content inspection. Publication,
versioning, and tagging remain intentionally outside this development pass.

## Commands

Local gate:

```sh
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
git diff --check
```

Remote live gate:

```sh
AWS_REGION=eu-west-1 \
DAX_LIVE_TESTS=1 \
DAX_TABLE_NAME=DaxParityTable \
cargo test -p aws-dax --test live_dax -- --ignored --nocapture
```

The remote command is run from `/home/ec2-user/aws-dax-rs` on the EC2 host.
