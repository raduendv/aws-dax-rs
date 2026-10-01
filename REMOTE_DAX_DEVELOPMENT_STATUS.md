# Remote DAX Development Status

**Last updated:** 2026-10-03

This file tracks execution of
[REMOTE_DAX_DEVELOPMENT_PLAN.md](./REMOTE_DAX_DEVELOPMENT_PLAN.md). It is
separate from the crate implementation status so infrastructure results,
environment blockers, and live coverage can evolve independently.

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
- TransactWriteItems with two unique Put actions and run-scoped cleanup.
- Runtime-backed cleanup of every run-scoped item, including cleanup after
  assertion or request failures.
- UpdateItem live verification now uses bounded polling to account for the
  cluster's observed post-write cache propagation delay.
- Endpoint discovery and refresh now pass against both plaintext and TLS
  clusters, including route snapshots for the address-family-selected roster.
- TransactGetItems now passes against the live TLS endpoint after correcting
  the key-frame double-wrapping divergence.
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

- Broader degraded-cluster scenarios require controlled node failure or route
  manipulation and are not covered by the current single-cluster harness.
  The live environment does not provide a safe node-failure control plane, so
  those scenarios remain local deterministic coverage only.

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
node-failure injection remains deferred because no safe failure control plane
is available.

### Phase 2: Fuzzing hardening

1. Maintain dedicated fuzz targets for AttributeValue, compressed attributes,
   item keys, and transaction-cancellation payloads.
2. Seed each target with minimal valid, empty, truncated, indefinite-length,
   and malformed CBOR inputs.
3. Run bounded local fuzz smoke sessions and preserve any minimized regression
   inputs under the corresponding `fuzz/corpus` directory.
4. Re-run the full Rust validation matrix after corpus or decoder changes.

Phase 2 result: all four fuzz targets completed bounded 100-run smoke
sessions with seeded corpora and no crashes. Local fuzzing requires a nightly
Rust toolchain because `cargo-fuzz` enables sanitizer instrumentation.

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
cargo test --test live_dax -- --ignored --nocapture
```

The remote command is run from `/home/ec2-user/aws-dax-rs` on the EC2 host.
