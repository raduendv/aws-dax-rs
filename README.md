# AWS DAX client for Rust

`aws-dax` is a Rust port of the AWS DAX Go v2 client with documented bounded
compatibility edges.

The project aims for capability and behavioral parity with the pinned
`aws-dax-go-v2` `v1.0.3` reference implementation while using idiomatic Rust and
the AWS SDK for Rust's asynchronous developer experience.

Construct a client from AWS SDK for Rust shared configuration and a DAX
discovery endpoint:

```rust,no_run
use aws_dax::Client;
use aws_sdk_dynamodb::types::AttributeValue;

# async fn example() -> Result<(), aws_dax::Error> {
let sdk_config = aws_config::load_from_env().await;
let client = Client::from_sdk_config(&sdk_config, "dax://example-cluster:8111")?;
let output = client
    .get_item()
    .table_name("example")
    .key("id", AttributeValue::S("42".into()))
    .send()
    .await?;
# Ok(())
# }
```

The same flow is available as a compile-checked example:

```sh
cargo run --example get_item
```

The example reads AWS credentials and region from the standard environment
chain and expects a reachable DAX discovery endpoint at
`dax://example-cluster:8111`; it does not contain credentials or make network
requests during compilation.

## Current status

The crate has completed its public-model phase and implements the supported DAX
protocol slices described below. It opens direct configured DAX endpoints for
`GetItem`, `PutItem`, `DeleteItem`, `Scan`, `Query`, batch operations, and
transaction operations, with endpoint discovery, routed pools, retries, and
lifecycle schedulers enabled for the implemented paths. Unsupported fields and
operations remain explicit errors rather than silent fallbacks.

It does provide typed DAX configuration validation, endpoint normalization,
IPv4/IPv6 discovery selection, and an idempotent client lifecycle. Data-plane
operation builders use AWS SDK for Rust DynamoDB input/output types. Unsupported
operation fields return explicit validation or protocol errors before network
I/O. `GetItem`, `PutItem`, `DeleteItem`, and table-key-paginated `Scan` perform
internal schema control loading and DAX request/response execution. `PutItem`
and `DeleteItem` support `ReturnValues::AllOld`; `Scan` returns `Items`,
`Count`, `ScannedCount`, and `LastEvaluatedKey`, and supports `ConsistentRead`,
`Limit`, table-key `ExclusiveStartKey`, and `ReturnConsumedCapacity` (`TOTAL`
and `INDEXES`), along with `Select::Count`. `Query` returns the corresponding
item, count, pagination, and consumed-capacity fields for a partition-key
equality condition in the exact
form `attribute = :placeholder`, optionally joined with a sort-key comparison:
`AND attribute <|<=|=|>=|> :placeholder`, or `AND begins_with(attribute,
:placeholder)`, or `AND attribute BETWEEN :lower AND :upper`. Aliases,
through `ExpressionAttributeNames`, may be shared across the key condition and
filter, and projection. All supplied aliases and expression values must be
referenced by one of those bounded expressions. `Query` also supports
`Select::Count`, bounded projection expressions, and comparison filters in the
form `attribute <>|<|<=|=|>=|> :placeholder`.
It also supports `begins_with(attribute, :placeholder)` and
`attribute BETWEEN :lower AND :upper`; up to two supported conditions may be
joined with `AND` or `OR`. Query and Scan support DynamoDB `IndexName` values
through the same request input types. Other compound filters or expressions
remain explicit errors. A single supported condition may also
be negated as `NOT (condition)` or `NOT condition`. `attribute_exists(attribute)` and
`attribute_not_exists(attribute)` may be used alone or as one of two joined
conditions. `attribute_type(attribute, TYPE)` is supported for DynamoDB type
literals `S`, `N`, `B`, `BOOL`, `NULL`, `L`, `M`, `SS`, `NS`, and `BS`.
It also accepts an expression-attribute-value placeholder as its second
argument. `contains(attribute, :placeholder)` is supported for a single
attribute-value placeholder.
`attribute IN (:first, :second, ...)` supports 2 through 100
attribute-value placeholders. Comparisons against `size(attribute)` are also
supported, for example `payload_size > size(payload)`.
`Scan` supports the same bounded filter-expression subset as `Query`, including
aliases and expression attribute values; projection expressions remain
limited to comma-separated attribute document paths such as
`profile.email` and non-negative list indexes such as `items[0].status`, with
aliases supported. The same bounded projection-expression subset is available
for `GetItem`, `Scan`, and `Query`.
Filter and condition expressions also support bounded dotted document paths and
non-negative list indexes using the same DAX path encoding.
Expression whitespace follows the Go lexer’s ASCII set (space, tab, carriage
return, and line feed); other Unicode whitespace characters are rejected.
`DeleteItem` supports the same bounded expression subset as a
`ConditionExpression`, including aliases and expression attribute values.
`PutItem` supports the same bounded `ConditionExpression` subset.
`UpdateItem` directly supports bounded `UpdateExpression` action lists with
`SET attribute = :placeholder`,
`SET attribute = attribute +/- :placeholder`,
`SET attribute = if_not_exists(attribute, :placeholder)`,
`SET attribute = list_append(attribute, :placeholder)`, or
`ADD attribute :placeholder`, `DELETE attribute :placeholder`, or
`REMOVE attribute` clauses, including bounded dotted document paths and
non-negative list indexes, with bounded `ConditionExpression` support and
optional `ReturnValues::AllOld`; other update actions and condition forms
remain explicit errors. Commas inside supported nested functions remain valid,
and action/value variable ordering follows the action-list order.

The internal protocol codecs currently support bounded, definite-length DAX
CBOR AttributeValue encoding/decoding for scalars, collections, numbers
(including bignums and decimal fractions), and DynamoDB sets. They also support
DAX item-key frames, including lexdecimal numeric range keys, and
schema-compressed non-key attributes using caller-provided attribute-list IDs.
This unstable implementation detail has no public compatibility guarantees.
Internal request codecs now cover the control and expression-free `GetItem`,
`PutItem`, `DeleteItem`, and `Scan` request streams. They also decode the
mandatory DAX success/error envelope and compressed-attribute response bodies
when supplied with schema/attribute-list state, and normalize verified DAX
error sequences into crate-owned error kinds. Internal cache-backed control
resolution now shares concurrent same-key loads, does not cache failures, and
preserves the reserved empty attribute-list fast path. The internal transport
protocol also has Go-compatible tube preamble and DAX SigV4 authorization
framing, including the 225-second authorization refresh window. Control
resolution is asynchronous and cancellation-safe before socket I/O is
introduced. Internal control tubes now also write and flush the
preamble/request and drain exactly the DAX envelope plus one control body
before reuse. A private TCP control executor
validates the wire lifecycle against local fixtures. It retains clean tubes,
reuses authorization for the reference 225-second window, and expires
authorization after a drained authentication-required response. Discovery and
routing are enabled for the implemented operations. Unsupported request and
response fields are rejected before network I/O. Its internal pool
supports verified `daxs://` connections with native trust roots and discovery
hostname SNI. The `skip_hostname_verification` compatibility override is
available for controlled environments and intentionally disables certificate
and hostname verification, matching the Go client's `InsecureSkipVerify`
behavior; it should not be used without an explicit compensating control. The
pool keeps clean tubes in LIFO order, reaps expired tubes when checking out a
connection,
bounds concurrent connection attempts using `max_pending_connections_per_host`,
and lets DAX control calls bypass a saturated normal connection-attempt gate.
It applies the configured read/write retry budgets (additional attempts after
the initial request) and fixed retry delay for non-throttle failures under one
request timeout, retrying Go-compatible transient DAX responses. Throttled
responses use the Go retryer's equal-jitter backoff. Client closure
now closes idle internal transport tubes, prevents further connection attempts,
and ensures a tube returned after shutdown is discarded.
The throttle base delay and maximum backoff are configurable through
`ConfigBuilder::throttle_base_delay` and `ConfigBuilder::throttle_max_backoff`;
their defaults match the Go client (`70ms` and `20s`).

The internal request codec also now emits the Go-compatible `BatchWriteItem`
request stream for single- and multi-table inputs. It validates the DynamoDB
batch limits, write-request shape, duplicate keys, per-table key schemas, and
compressed non-key attribute-list IDs, while preserving deterministic table
ordering for reproducible vectors. The public `batch_write_item()` builder now loads all referenced table schemas,
resolves compressed attribute lists, executes through the pooled transport, and
decodes unprocessed requests and consumed capacity. Item-collection metrics are
rejected explicitly until their response model is ported.

The public `batch_get_item()` builder likewise loads per-table key schemas,
encodes consistency and key batches, executes through the pooled transport, and
decodes returned items, unprocessed keys, and consumed capacity. Projection responses decode DAX's ordinal projected-attribute response map,
including expression-name aliases and nested document paths.

The protocol codec now also emits the Go-compatible `TransactGetItems`
parallel-array request layout, including per-table key schemas, duplicate-key
validation, bounded projection ASTs, and consumed-capacity options. Its public
transport path now supports no-projection positional responses, lazy
schema-compressed attribute-list loading, and consumed-capacity decoding.
Projection responses now decode ordinal response maps, including aliased
nested document paths and list indexes.

The protocol codec also emits bounded `TransactWriteItems` parallel arrays for
Put, Delete, ConditionCheck, and Update actions. It validates duplicate
table/key pairs, encodes compressed Put attributes and condition expressions,
preserves return-values-on-condition-failure flags, idempotency tokens, and
capacity options. The public transport path decodes null return-value entries,
consumed capacity, and item-collection metrics. Returned attributes remain an
explicit compatibility boundary. Unsupported transaction expression forms
remain explicit errors.

`Client::query_paginator` and `Client::scan_paginator` provide lazy,
AWS-SDK-style page iteration. They preserve the operation input, apply each
`LastEvaluatedKey`, stop on an empty or duplicate continuation token, and
propagate the underlying operation error.

`Client::batch_get_item_paginator` retries `UnprocessedKeys` with the original
request options and can optionally stop on a repeated unprocessed-key token.

`Client::discover_endpoints` explicitly retrieves the current DAX cluster
roster from the configured seed and decodes it into `DiscoveredEndpoint`
values. After discovery, data-plane requests select discovered routes
round-robin, use node-keyed pooled executors, and record consecutive route
failures. Requests continue to use the configured seed until discovery has
committed a roster. Periodic refresh and active health-check tasks remain
bounded compatibility work.

If a routed transport/I/O attempt fails, the client records the node failure
and tries another healthy discovered node when one is available. Protocol,
validation, and DAX service responses are returned directly and are not treated
as transport failures.
Longer-lived route suppression after repeated failures is enabled only when
`route_manager_enabled(true)` is configured, matching the Go client.
When enabled suppression would reduce the active set below two-thirds of the
validated roster, the complete roster is restored (fail-open), matching the Go
route manager and preventing a transient outage from becoming permanent.
After three fail-open events within two health-check intervals, suppression is
temporarily disabled for ten minutes, then automatically re-enabled.

Multiple unencrypted discovery endpoints are retained as ordered seeds. Control
and pre-discovery data-plane requests try each seed in order when an earlier
seed cannot complete the request.
Seed hostnames are resolved explicitly and filtered through `ip_discovery`
before dialing; discovered route addresses continue to use their validated
address family directly. TLS seed connections retain the configured hostname
for SNI. Selected seed addresses are attempted in order before the next
configured seed is tried.

Once an async operation starts, the client lazily starts a background endpoint
refresh task using `cluster_update_interval`. It attempts an initial discovery
immediately, then validates and commits complete rosters on each interval.
The task is aborted by `Client::close`; construction itself remains free of
runtime and network requirements.

With `route_manager_enabled(true)`, a second background task probes each
discovered route with the `Endpoints` control request every
`client_health_check_interval`. Successful probes recover routes; failed probes
feed route suppression and fail-open handling. Health probes use a one-second
deadline and are canceled on close. Only transport/I/O failures suppress a
route; DAX service or protocol responses are left visible for diagnostics.

An `Endpoints` health-probe transport failure immediately closes and removes
that node’s routed pool; the next probe recreates it while leaving the
discovered route active. Health probes use three internal retries before
counting a transport failure. The separate five-consecutive-read-failure
threshold is reserved for request-health replacement parity. A regression test
covers this replacement boundary. A third lifecycle task reaps
idle tubes from seed and routed pools at
`idle_connection_reap_delay`, matching the Go pool lifecycle and stopping with
the client. Zero-valued scheduler durations are clamped to a one-millisecond
tick rather than causing a runtime panic.

`Client::active_endpoints` returns the last successfully validated roster
without performing network I/O, or `None` before the first successful
discovery.

`Client::refresh_endpoints` performs an explicit validated refresh and returns
an `EndpointRefresh` summary of added, removed, retained, and changed nodes.
Failed refreshes leave the previous active roster unchanged. The
`Client::last_refresh_error` accessor exposes the most recent background
refresh failure for diagnostics and clears after a successful refresh; failures
are also emitted through the configured warning logger. A successful explicit
discovery or refresh clears any stale background-refresh diagnostic as well.
`Client::route_snapshot` exposes validated node dial targets, TLS server names,
request-suppression health, and periodic health-probe status without performing
network I/O.
Transaction-canceled DAX envelopes preserve per-item cancellation codes,
messages, and raw CBOR item payloads through `Error::Dax`. Call
`TransactionCancellationReason::decode_item` with the transaction request key,
table key schema, and attribute-name-list cache to reconstruct a complete
`HashMap<String, AttributeValue>` with key attributes restored.
Transaction conflict, in-progress, and idempotent-parameter-mismatch responses
are exposed as distinct `DaxErrorKind` values.
Unsupported DAX operations are normalized as `DaxErrorKind::NotImplemented`.
Resource-in-use, item-collection-size-limit, and limit-exceeded responses are
also normalized into distinct `DaxErrorKind` values.

`Client` is cloneable. Clones share transport pools, endpoint state, and
background tasks; dropping a non-final clone leaves the shared client running.
Calling `close` from any clone closes the shared client and cancels all
background tasks, while dropping the final clone performs the same cleanup.

Connection failures use the public `TransportErrorKind` classification and are
the only failures eligible for routed failover. Protocol decoding, validation,
and DAX service errors remain distinct and are returned directly.

The currently available `fixture` module validates versioned, language-neutral
conformance fixtures that later phases use to compare the Rust implementation
with the Go reference.

## Development

```sh
cargo fmt --check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

The untrusted CBOR decoders have persistent `cargo-fuzz` targets:

```sh
cargo fuzz run decode_attribute_value
cargo fuzz run decode_compressed_attributes
cargo fuzz run decode_transaction_cancellation_item
cargo fuzz run decode_item_key
```

The live DAX smoke test is intentionally ignored and requires explicit
environment opt-in:

```sh
export DAX_LIVE_TESTS=1
export DAX_TABLE_NAME=DaxParityTable
export DAX_ENDPOINTS='dax://cluster.example.com,daxs://cluster-tls.example.com'
cargo test --test live_dax -- --ignored --nocapture
```

The remote EC2 synchronization workflow, safety rules, validation phases, and
current infrastructure results are documented in
[REMOTE_DAX_DEVELOPMENT_PLAN.md](./REMOTE_DAX_DEVELOPMENT_PLAN.md) and
[REMOTE_DAX_DEVELOPMENT_STATUS.md](./REMOTE_DAX_DEVELOPMENT_STATUS.md).

Pull requests and pushes to `main` run the same checks on Rust 1.85 and stable,
plus `cargo-deny` dependency, advisory, source, and license policy checks.

With the pinned Go reference clone available at `aws-dax-go-v2/`, verify that
checked-in reference fixtures are current:

```sh
cargo test --test reference_fixture_export -- --ignored
```

See [the porting plan](PORTING_PLAN.md) for the approved architecture, phase
gates, compatibility strategy, and remaining decisions.
