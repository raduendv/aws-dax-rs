# Remote DAX Development and Live-Testing Plan

This document defines the repeatable workflow for validating the Rust port
against real AWS DAX infrastructure without making live AWS access part of
normal local or CI test runs.

## Objectives

- Verify wire compatibility against real plaintext and TLS DAX clusters.
- Exercise the same public AWS SDK-style operations used by local fixtures.
- Keep live mutations isolated to run-scoped keys and clean them up reliably.
- Separate infrastructure findings from local protocol and API regressions.
- Preserve a fast, deterministic offline test suite for every code change.

## Environment

### Local development checkout

- Repository: `aws-dax-rs`
- Go reference checkout: adjacent `aws-dax-go-v2/`
- Live harness: `tests/live_dax.rs`
- Live opt-in: `DAX_LIVE_TESTS=1`

### AWS resources

- Region: `eu-west-1`
- DynamoDB table: `DaxParityTable`
- Table schema:
  - Hash key: `PK` (`S`)
  - Range key: `SK` (`S`)
  - LSI: `LSI1`, using `LSI1SK` (`N`)
  - GSI: `GSI1`, using `GSI1PK` (`S`) and `GSI1SK` (`S`)
- Plaintext endpoint:
  `dax://radu-rust.cykcls.dax-clusters.eu-west-1.amazonaws.com`
- TLS endpoint:
  `daxs://radu-rust-tls.cykcls.dax-clusters.eu-west-1.amazonaws.com`
- EC2 validation host: `ec2-user@18.201.46.195`
- Remote checkout: `/home/ec2-user/aws-dax-rs`
- Authentication: EC2 instance IAM role; do not copy static credentials.

## Synchronization workflow

Run from the repository root. Exclude build output and fuzz targets so the
remote host builds with its own toolchain:

```sh
rsync -avx --delete \
  --exclude target \
  --exclude fuzz/target \
  -e "ssh -o StrictHostKeyChecking=no -i ~/.ssh/radu-dax.pem" \
  . ec2-user@18.201.46.195:/home/ec2-user/aws-dax-rs
```

For focused iterations, sync only the changed source and test files:

```sh
rsync -avx --checksum \
  -e "ssh -o StrictHostKeyChecking=no -i ~/.ssh/radu-dax.pem" \
  src/ tests/live_dax.rs \
  ec2-user@18.201.46.195:/home/ec2-user/aws-dax-rs/
```

The remote checkout is a validation workspace, not a source of truth. All
changes must be made and reviewed in the local repository first.

## Validation command

```sh
ssh -o StrictHostKeyChecking=no -i ~/.ssh/radu-dax.pem \
  ec2-user@18.201.46.195 \
  'cd /home/ec2-user/aws-dax-rs &&
   AWS_REGION=eu-west-1
   DAX_LIVE_TESTS=1
   DAX_TABLE_NAME=DaxParityTable
   cargo test --test live_dax -- --ignored --nocapture'
```

`DAX_ENDPOINTS` may override the default endpoint pair with a comma-separated
list. The harness must remain ignored and environment-gated so normal
`cargo test` never mutates AWS resources.

## Test design rules

1. Generate a unique partition key per run, such as `LIVE#<epoch-millis>`.
2. Use only that run's keys for writes, reads, queries, and cleanup.
3. Exercise plaintext and TLS endpoints independently.
4. Attempt cleanup after every successful write, including when a later
   assertion fails. If the harness grows beyond a single test body, use a
   cleanup guard or an explicit error-collection path.
5. Never use production tables or credentials in the live harness.
6. Prefer assertions on returned values and continuation state, not only
   successful request completion.
7. Record protocol discrepancies in the live status file before changing
   codecs.

## Phased execution

### Phase 1: Basic data-plane smoke

- PutItem
- GetItem
- base-table Query
- DeleteItem cleanup
- plaintext and TLS endpoints

### Phase 2: Indexes and expressions

- GSI Query
- LSI Query
- filtered Scan
- projections and aliases
- UpdateItem with expression values
- conditional success and expected conditional failure

### Phase 3: Batch and pagination

- BatchGetItem responses
- BatchGet unprocessed-key continuation
- Query paginator with multiple pages
- Scan paginator with multiple pages
- BatchWriteItem put/delete requests
- retry behavior for transient or throttled responses

### Phase 4: Transactions and failure behavior

- TransactGetItems
- TransactWriteItems
- cancellation reasons and item payload reconstruction
- idempotency and conditional failures
- malformed or unavailable route behavior

### Phase 5: Cluster lifecycle and networking

- Endpoint discovery
- IPv4-only and IPv6-only selection
- route suppression and recovery
- node failure and failover
- TLS SNI and certificate validation
- client close/drop while background tasks are active

## Local validation gate

Before every remote run:

```sh
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
git diff --check
```

After a live discrepancy is fixed, add a deterministic local fixture whenever
the wire shape can be reproduced without AWS. Update this plan only when the
workflow or phase gates change; record execution results in
`REMOTE_DAX_DEVELOPMENT_STATUS.md`.

## Safety and release boundaries

- Live tests are confidence checks, not a substitute for unit or integration
  tests.
- No live endpoint or AWS resource should be required to build, lint, package,
  or publish the crate.
- Do not commit private keys, credentials, captured authorization headers, or
  unredacted sensitive response data.
- Release approval remains separate from live-test success.
