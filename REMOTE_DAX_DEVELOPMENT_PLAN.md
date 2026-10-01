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
  --exclude src/aws-dax-fuzz/target \
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
   cargo test -p aws-dax --test live_dax -- --ignored --nocapture'
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

### FFI Phase 6: Controlled one-node failover

The six persistent consumers in `src/aws-dax-ffi/tests/consumers/` support an
opt-in prewarm barrier. When **both** `DAX_FAULT_READY_FILE` and
`DAX_FAULT_RELEASE_FILE` are nonempty, the consumer creates its client, discovers
endpoints, refreshes routes, checks that routes exist, and exclusively creates
an empty ready file. It then waits up to **120 seconds**, using a monotonic
clock, for the release file. Timeout and filesystem errors fail explicitly and
shut down the client before any mutation or mutation-cleanup handler runs.
Neither or only one variable set leaves normal runs unchanged. Standalone
barrier users must provide distinct, fresh paths in a private directory and
remove those files afterward; an existing ready file is an error.

`scripts/live_fault_injection.sh --consumer NAME` coordinates this barrier for
exactly one of `go`, `dotnet`, `java`, `kotlin`, `python`, or `javascript`. It
uses the existing Rust route-discovery test and one shared firewall
insertion/verification/cleanup implementation. It builds the selected consumer
before launching it, waits for ready, inserts exactly `DAX_BLOCK_COUNT`
distinct IPv4 DROP rules, verifies them, and releases the consumer. A pass
requires exit zero, the exact language-specific `... operations passed` line,
and rules still active after the workload (including its item cleanup).
`DAX_FAULT_SUMMARY status=ok` must be followed by
`DAX_FAULT_CLEANUP status=ok` and an overall zero exit.

Safety requirements:

- Run only on the dedicated **Linux EC2 validation host**, never a production
  host/table. DROP rules affect all outbound traffic to the selected node IPs.
  Run one fault harness at a time.
- Require `DAX_FAULT_INJECTION=1`, `DAX_ENDPOINT`, Linux `setsid` (util-linux),
  and noninteractive `sudo -n iptables` permission. Credentials remain the EC2
  IAM role; do not copy credentials.
- Rules have a per-process comment; cleanup deletes only this run's rules.
  The EXIT trap stops the owned child process group, removes rules, verifies
  removal, and removes its private ready/release/log directory on success,
  failure, SIGINT, or SIGTERM. Failed rule removal is an explicit nonzero error.
  SIGKILL, host failure, or loss of sudo/firewall access cannot guarantee
  cleanup; inspect/remove rules tagged `aws-dax-fault-<pid>` manually then.
- Ready waiting defaults to 120 seconds; post-release execution defaults to
  1800 seconds because a blocked route can consume multiple bounded operation
  timeout/retry budgets before route health converges. Override with
  `DAX_FAULT_READY_TIMEOUT_SECONDS` and
  `DAX_FAULT_WORKLOAD_TIMEOUT_SECONDS` (integers 1..9999). These runner limits
  do not extend the consumer's fixed 120-second release wait.
- `DAX_BLOCK_COUNT` defaults to 1; 2 remains available for the existing
  two-route fault mode. Phase 6 one-node validation uses **1**.

After synchronizing the checkout, prepare matching native/generated bindings
on the remote host. The installed Go/cgo compiler, .NET 8, Java 21, Maven,
Python 3, Node.js/npm, and Rust tools must be on `PATH`; Kotlin is built with
the consumer's pinned Maven plugin. Use the established Java installation:

```sh
cd /home/ec2-user/aws-dax-rs
export JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto.x86_64
export PATH="$JAVA_HOME/bin:$PATH"
bash scripts/generate_ffi_bindings.sh all

# Explicitly opt in to ONE consumer and ONE blocked route.
DAX_FAULT_INJECTION=1 \
AWS_REGION=eu-west-1 \
DAX_TABLE_NAME=DaxParityTable \
DAX_ENDPOINT=dax://radu-rust.cykcls.dax-clusters.eu-west-1.amazonaws.com \
DAX_BLOCK_COUNT=1 \
bash scripts/live_fault_injection.sh --consumer go
```

Replace `go` with `dotnet`, `java`, `kotlin`, `python`, or `javascript` for the
other consumers. Repeat separately with
`DAX_ENDPOINT=daxs://radu-rust-tls.cykcls.dax-clusters.eu-west-1.amazonaws.com`
for TLS. Do not set the barrier file variables yourself when using the runner.
The language selector is validated; commands are passed as argument arrays
and executed directly, never through `eval` or a shell command string.

`scripts/live_ffi_consumer.sh build NAME` stages the persistent consumer and
generated sources under `target/ffi-live/NAME`, then compiles/type-checks it
without creating a DAX client. Sources come from `target/ffi-bindings/` and
the matching native library from `target/debug/`. Go links through cgo; .NET
uses its net8.0 DLL; Java/Kotlin use Maven's classpath and JNA library path;
Python and N-API JavaScript receive a colocated native library. JavaScript
uses the pinned npm manifest, strict TypeScript checking, and Node's `tsx`
loader. `run NAME` execs the staged consumer; the fault harness invokes it in
a dedicated process group. This staging is validation build output, not a
package/release artifact.

Without `--consumer`, the original Rust read/write/transaction mode remains
available; `DAX_FAULT_PREWARM=1` still selects the prewarmed Rust transaction
workload. Rust transaction fault-result classification is unchanged and is
not substituted for a generated-language success marker.

Offline checks (no AWS, sudo, or real firewall calls):

```sh
for language in go dotnet java kotlin python javascript; do
  bash scripts/live_ffi_consumer.sh build "$language" || exit
done
bash -n scripts/live_fault_injection.sh scripts/live_ffi_consumer.sh
python3 -B -m unittest discover -s scripts/tests -p 'test_live_fault_injection.py' -v
```

The guard tests replace route discovery, language execution, and iptables in
a private fixture checkout. They cover dispatch, exact rule counts, ordering,
failure/timeout/signal cleanup, success-marker enforcement, and both Rust
modes. Python barrier tests cover opt-in behavior, prewarm ordering, release,
timeout, empty routes, exclusive ready-file creation, and filesystem errors.
Local validation does not establish live failover parity: the six-language
one-node fault matrix remains an explicit remote execution gate.

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
