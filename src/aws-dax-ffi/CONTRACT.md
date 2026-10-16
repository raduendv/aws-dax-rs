# AWS DAX FFI Contract

This document defines the language-neutral contract generated from
`aws-dax-ffi`. The native implementation delegates DAX behavior to `aws-dax`;
foreign bindings do not reimplement routing, retries, encoding, credentials,
or operation behavior.

## Compatibility rules

- Only types owned by `aws-dax-ffi` cross the ABI.
- AWS SDK for Rust models and fluent builders never cross the ABI.
- Exported requests are immutable records passed to asynchronous client
  methods.
- DynamoDB numbers remain strings and binary values remain bytes.
- Required-field and service validation errors are returned, never replaced
  with defaults.
- Adding an optional record field is compatible. Removing or changing a field,
  enum variant, or method requires a new contract major version.
- Generated bindings verify the UniFFI contract version and per-function
  checksums before use.
- Rust owns one process-wide multi-thread Tokio runtime needed by the SDK.
  Client creation and every SDK operation are spawned onto that executor;
  foreign runtimes receive their normal generated async abstraction and never
  need to provide a Tokio reactor.

Package versions use the same semantic version as `aws-dax-ffi`. Generated
packages declare that native version exactly; the generated checksum gate
rejects an incompatible library even when package managers resolve the wrong
artifact.

## Packaged binding distribution

Phase 7 assembly consumes the pinned generators and one native artifact and
emits host packages with a native SHA-256, semantic version, installed-platform
label, and the Phase 8 target matrix. Supported package mappings are Linux
x86_64/aarch64, macOS x86_64/arm64, and Windows x86_64. The assembler rejects
native files whose container or architecture does not match the requested
mapping. .NET targets net8.0 with RID-native assets;
Java classes/sources target JDK 21 and include package-relative native assets;
Kotlin is JVM-only and preserves generated coroutine/suspend behavior; Python
is a platform wheel; and JavaScript is the selected Node N-API backend with
declarations.  No Kotlin/Native/KMP, browser JavaScript, or WASM support is
claimed.  Package-native loading must not require loader environment variables,
JNA paths, or `PYTHONPATH`.

Release artifacts include deterministic SHA-256 manifests and CycloneDX SBOMs.
CI uses immutable action revisions, scans the source tree for secrets, and
creates GitHub build-provenance attestations for main-branch package artifacts.
These controls do not publish packages; publication requires explicit release
approval.

The checked API-name snapshots in `api-snapshots/` make generator naming and
visibility changes explicit. Run `scripts/update_ffi_api_snapshots.sh` only
when an intentional contract change has been reviewed.

All selected bindings validate UniFFI contract and callable checksums before
use. The pinned Python generator emits its callable checksum invocation
commented out, and the pinned Java generator loads its integrity class without
initializing it. Generation applies deterministic, fail-closed fixups for
those two backend defects; the fixup script fails if the expected generated
patterns change.

Python generation also replaces the pinned backend's per-byte ctypes buffer
writer with deterministic bytearray staging and one Rust-owned allocation/copy.
The transform rejects unexpected templates or dependencies on removed builder
internals. Scalar encodings, generated validation and ownership transfer stay
unchanged. Discard before finalization allocates no Rust memory; failed copies
free their allocation. This is a generation-time implementation detail, not an
API change or runtime monkey patch. Tests compare baseline/optimized bytes,
exercise real Rust conversions and verify allocation/failure cleanup.

The first contract uses Rust's default AWS credential chain. Rust
`SdkConfig`, `SharedCredentialsProvider`, `Logger`, and arbitrary foreign
credential callbacks are not exported.

## Shared values

`DaxAttributeValue` is a tagged union with these variants:

- `string(String)`
- `number(String)`
- `binary(bytes)`
- `boolean(bool)`
- `null`
- `string_set(sequence<String>)`
- `number_set(sequence<String>)`
- `binary_set(sequence<bytes>)`
- `list_value(sequence<DaxAttributeValue>)`
- `map_value(map<String, DaxAttributeValue>)`

The list and map variants use the explicit `list_value` and `map_value` names
because bare `List` and `Map` variants shadow Kotlin collection types in the
generated sealed class.

The AWS SDK's forward-compatibility `Unknown` variant is rejected. Conversion
uses one budget for an entire value or item and enforces:

- at most 32 nested value levels;
- at most 100,000 aggregate value nodes;
- at most 100,000 aggregate collection entries;
- at most 1 MiB for any string, map key, number string, or binary value;
- at most 16 MiB of aggregate string and binary data.

String, number, and binary sets must be non-empty and contain no duplicate
members. Number strings use the Go reference decimal grammar, its signed
32-bit exponent bound, DynamoDB's 38-digit precision, and adjusted exponent
range from -130 through 125. Invalid values return a structured validation
error before transport work starts.

`ffi_round_trip_attribute_value` is the generated-binding contract proof for
this conversion. It lowers one foreign value, converts it to the AWS SDK
representation under the same bounds used by operations, converts it back,
and returns the language-native generated value.

Common aliases used below:

- `Item`: `map<String, DaxAttributeValue>`
- `Key`: `map<String, DaxAttributeValue>`
- `ExpressionNames`: `map<String, String>`
- `ExpressionValues`: `map<String, DaxAttributeValue>`

Shared enums:

- `ReturnValue`: `none`, `all_old`
- `ReturnConsumedCapacity`: `none`, `total`, `indexes`
- `ReturnItemCollectionMetrics`: `none`, `size`
- `Select`: `all_attributes`, `all_projected_attributes`,
  `specific_attributes`, `count`
- `IpDiscovery`: `default`, `ipv4`, `ipv6`
- `LogLevel`: `off`, `error`, `warn`, `info`, `debug`

The direct-operation `ReturnValue` enum intentionally contains only values
implemented by the current SDK. Additional update return modes require SDK
support before they can be added here.

## Client configuration and lifecycle

`DaxClientConfig` fields:

| Field | Type | Default |
|---|---|---|
| `endpoints` | `sequence<String>` | required, non-empty |
| `region` | `String?` | default AWS region chain |
| `request_timeout_millis` | `u64` | `60000` |
| `write_retries` | `u32` | `2` |
| `read_retries` | `u32` | `2` |
| `retry_delay_millis` | `u64` | `0` |
| `throttle_base_delay_millis` | `u64` | `70` |
| `throttle_max_backoff_millis` | `u64` | `20000` |
| `max_pending_connections_per_host` | `i32` | `10` |
| `cluster_update_interval_millis` | `u64` | `4000` |
| `cluster_update_threshold_millis` | `u64` | `125` |
| `idle_connection_reap_delay_millis` | `u64` | `30000` |
| `client_health_check_interval_millis` | `u64` | `5000` |
| `skip_hostname_verification` | `bool` | `false` |
| `route_manager_enabled` | `bool` | `false` |
| `ip_discovery` | `IpDiscovery` | `default` |
| `log_level` | `LogLevel` | `off` |

`DaxClient` methods:

- asynchronous direct operations listed below;
- `query_paginator`, `scan_paginator`, and `batch_get_item_paginator`;
- asynchronous `discover_endpoints` and `refresh_endpoints`;
- synchronous `active_endpoints`, `route_snapshot`, `last_refresh_error`,
  `is_closed`, and idempotent `shutdown`.

Client creation is asynchronous but performs no DAX network I/O. It resolves
the Rust-owned AWS region and credential-provider chain, validates
configuration, and creates an independent native client. Generated objects may
be shared across concurrent foreign calls. `shutdown` is thread-safe and
idempotent; shutting down one client does not affect another client. Dropping a
generated object without calling `close` releases its native UniFFI handle and
the underlying SDK client. Explicit `shutdown` remains recommended so
transport pools and background work are released deterministically.

The SDK lifecycle method is named `shutdown`, rather than `close`, at the
language-neutral boundary because the Java and Kotlin generators reserve
`close` for releasing the UniFFI object handle. Generated object disposal and
SDK shutdown are intentionally separate: call `shutdown` first for
deterministic SDK cleanup, then use the language's generated disposal method.

Cancellation follows each generator's native async mechanism. Cancelling an
exported future frees the UniFFI future handle and aborts its task on the
Rust-owned runtime; it does not implicitly close an already returned client.
JavaScript's selected N-API binding accepts an `AbortSignal`; other generators
expose their normal future, task, coroutine, or event-loop cancellation
behavior.

`DiscoveredEndpoint` contains `node_id`, `hostname`, `address`, `port`, `role`,
optional `availability_zone`, and optional `leader_session_id`.

`EndpointRefresh` contains node ID sequences named `added`, `removed`,
`retained`, and `changed`.

`RouteSnapshot` contains `node_id`, `dial_address`, `tls_server_name`,
`healthy`, and `health_probe_healthy`.

The exported records are named `DaxDiscoveredEndpoint`, `DaxEndpointRefresh`,
and `DaxRouteSnapshot`. Node IDs, ports, roles and leader session IDs are signed
64-bit integers; endpoint addresses are raw byte sequences (IPv4 or IPv6), not
formatted strings. Conversion preserves all SDK metadata without reinterpreting
roles or health flags.

`discover_endpoints` returns a sequence of discovered endpoints and commits the
validated roster. `refresh_endpoints` returns the SDK's node-level diff. Both
execute through `runtime::run`, including the SDK's background-task startup.
Failed discovery/refresh leaves the last valid roster intact.
`active_endpoints` returns an optional sequence (`None` before a roster is
committed); `route_snapshot` returns a sequence (empty before discovery).
These synchronous accessors perform no network I/O and retain SDK semantics
after shutdown. `last_refresh_error` returns the SDK's optional background
refresh diagnostic; it is not a history of foreground-call errors.

## Direct operations

Every operation is an asynchronous method on `DaxClient` accepting one request
record and returning one response record. Direct, batch, and transaction
methods lower their FFI-owned records and execute the SDK call inside the
Rust-owned `runtime::run` executor boundary.

### GetItem

Request:

- required `table_name`
- required `key`
- optional `consistent_read`
- optional `projection_expression`
- `expression_attribute_names`

Response:

- optional `item`

### PutItem

Request:

- required `table_name`
- required `item`
- optional `return_values`
- optional `condition_expression`
- `expression_attribute_names`
- `expression_attribute_values`

Response:

- optional old `attributes`

### DeleteItem

Request:

- required `table_name`
- required `key`
- optional `return_values`
- optional `condition_expression`
- `expression_attribute_names`
- `expression_attribute_values`

Response:

- optional old `attributes`

### UpdateItem

Request:

- required `table_name`
- required `key`
- required bounded `update_expression`
- optional `condition_expression`
- `expression_attribute_names`
- `expression_attribute_values`
- optional `return_values`

Response:

- optional old `attributes`

### Query

Request:

- required `table_name`
- optional `index_name`
- required `key_condition_expression`
- optional `filter_expression`
- optional `projection_expression`
- `expression_attribute_names`
- `expression_attribute_values`
- optional `consistent_read`
- optional positive `limit`
- optional `scan_index_forward`
- optional `select`
- optional `return_consumed_capacity`
- optional `exclusive_start_key`

Response:

- `items`
- `count` (zero when absent from the native SDK output)
- `scanned_count` (zero when absent from the native SDK output)
- optional `last_evaluated_key`
- optional `consumed_capacity`

### Scan

Request:

- required `table_name`
- optional `index_name`
- optional `filter_expression`
- optional `projection_expression`
- `expression_attribute_names`
- `expression_attribute_values`
- optional `consistent_read`
- optional positive `limit`
- optional `select`
- optional `return_consumed_capacity`
- optional `exclusive_start_key`

Response:

- `items`
- `count` (zero when absent from the native SDK output)
- `scanned_count` (zero when absent from the native SDK output)
- optional `last_evaluated_key`
- optional `consumed_capacity`

Legacy DynamoDB condition maps, `attributes_to_get`, segmented parallel Scan,
and Query/Scan fields not encoded by the current SDK are not exported.

## Batch operations

### BatchGetItem

`KeysAndAttributes` contains:

- non-empty `keys`
- optional `consistent_read`
- optional `projection_expression`
- `expression_attribute_names`

Request:

- non-empty `request_items: map<table_name, KeysAndAttributes>`
- optional `return_consumed_capacity`
- at most 100 keys in total

Response:

- `responses: map<table_name, sequence<Item>>`
- `unprocessed_keys: map<table_name, KeysAndAttributes>`
- `consumed_capacity`

All request keys and returned items use the shared bounded AttributeValue
conversion. Absent optional SDK collection fields become empty FFI
collections.

The paginator retries `unprocessed_keys`. Its request option
`stop_on_duplicate_token` defaults to `false`.

### BatchWriteItem

`BatchWriteAction` is a tagged union:

- `put { item }`
- `delete { key }`

Request:

- non-empty `request_items: map<table_name, sequence<BatchWriteAction>>`
- optional `return_consumed_capacity`
- optional `return_item_collection_metrics`
- at most 25 actions in total, with no duplicate item keys

Response:

- `unprocessed_items: map<table_name, sequence<BatchWriteAction>>`
- `consumed_capacity`
- `item_collection_metrics`

Unprocessed write actions are raised back into the same tagged union accepted
by the request.

## Transaction operations

### TransactGetItems

`TransactGetAction` contains:

- required `table_name`
- required `key`
- optional `projection_expression`
- `expression_attribute_names`

Request:

- non-empty `transact_items`
- optional `return_consumed_capacity`

Response:

- ordered `responses`, each containing an optional `item`
- `consumed_capacity`

### TransactWriteItems

`TransactWriteAction` is a tagged union:

- `condition_check`: table, key, required condition expression, expression
  names/values, optional return-values-on-condition-check-failure;
- `put`: table, item, optional condition expression, expression names/values,
  optional return-values-on-condition-check-failure;
- `delete`: table, key, optional condition expression, expression
  names/values, optional return-values-on-condition-check-failure;
- `update`: table, key, required bounded update expression, optional condition
  expression, expression names/values, optional
  return-values-on-condition-check-failure.

Request:

- non-empty `transact_items`
- optional `client_request_token`
- optional `return_consumed_capacity`
- optional `return_item_collection_metrics`
- at most 100 actions, with no duplicate item keys

Response:

- `consumed_capacity`
- `item_collection_metrics`

Transaction cancellation codes and raw cancellation items remain carried by
the structured error contract described below.

## Shared response records

`ConsumedCapacity` preserves:

- optional `table_name`
- optional `capacity_units`
- optional `read_capacity_units`
- optional `write_capacity_units`
- optional table capacity
- local-secondary-index capacities
- global-secondary-index capacities

`Capacity` preserves optional capacity, read-capacity, and write-capacity units.

`ItemCollectionMetrics` preserves the item-collection key and the optional
two-value size-estimate range in gigabytes. Batch and transaction outputs group
these records by table name. Because the frozen FFI record requires an
item-collection key, an SDK response that omits that required key is rejected
as a structured validation error rather than silently replaced with an empty
item.

## Paginators

`QueryPaginator` and `ScanPaginator` expose:

- synchronous `has_more_pages`;
- asynchronous `next_page`, returning an optional normal operation response.

`BatchGetItemPaginator` additionally accepts `stop_on_duplicate_token` at
construction. Paginators are stateful objects, are not clonable across the
ABI, and retain the SDK's paginator-local route affinity and failover behavior.

The client factories are synchronous, fallible, and perform no network I/O:

- `query_paginator(DaxQueryRequest) -> QueryPaginator`;
- `scan_paginator(DaxScanRequest) -> ScanPaginator`;
- `batch_get_item_paginator(DaxBatchGetItemRequest, bool) -> BatchGetItemPaginator`.

The boolean is the required `stop_on_duplicate_token` argument, fixed for that
BatchGet traversal. Factories use the same bounded request conversion as direct
operations. SDK/service validation remains lazy; construction on a closed client
can succeed, but fetching a non-exhausted page returns the SDK's closed error.

Each object owns exactly one SDK paginator. There is no cursor-clone operation:
duplicating a generated handle aliases the same cursor, not an independent
traversal. Concurrent `next_page` calls are serialized by an asynchronous mutex.
`has_more_pages` is a nonblocking snapshot of the last completed SDK transition
(initially true), including while another call is in flight. It is advisory, not
a reservation of a page. `next_page` returns an optional `DaxQueryResponse`,
`DaxScanResponse`, or `DaxBatchGetItemResponse`, respectively, using exactly the
direct-operation response converters. Exhausted cursors return `None` without
network I/O, even after client shutdown.

Query and Scan stop on an absent or repeated continuation key. BatchGet retries
unprocessed keys, stops when none remain, and stops on identical keys and request
metadata only when its duplicate-token option is true. The terminating page is
still returned. SDK errors leave the continuation state retryable.

All asynchronous page work, including waiting for the cursor lock, executes
through `runtime::run`. Cancelling/dropping a page future aborts its runtime task
and releases the cursor lock and checked-out transport; it does not close the
client. Cancellation before an SDK page completes leaves the continuation state
unchanged. A completed SDK transition is not rolled back if cancellation races
with delivery or FFI response conversion fails. The snapshot is updated before
response conversion.

A paginator retains its SDK client independently of the original FFI client
handle. Dropping the last paginator handle releases its ownership once any
in-flight call releases its references. Explicit client shutdown affects all its
paginators and prevents later network operations. In-flight operations retain
the SDK's existing completion/timeout behavior; shutdown is not equivalent to
cancelling each outstanding future and does not force every such result to
`Closed`.

## Errors

All exported failures use one structured `DaxException` payload:

- `category`: configuration, not-implemented, transport-unavailable,
  validation, protocol, transport, service, or closed;
- stable `message`;
- optional `operation`;
- optional configuration, transport, or service kind;
- optional `request_id`;
- optional `error_code`;
- optional `status_code`;
- `code_sequence`;
- transaction `cancellation_reasons`.

Each cancellation reason preserves optional `code`, optional `message`, and
optional raw item CBOR bytes. Decoding raw cancellation items remains an
advanced Rust SDK facility until the FFI contract has a bounded,
context-owning representation for key schema and attribute-list state.

Errors never include credentials, credential-provider diagnostics, or
unredacted request values.

## Deliberately non-exported Rust surfaces

- AWS SDK generated types and `input(...)` escape hatches;
- Rust fluent builders;
- `Client::from_sdk_config` and `Client::config`;
- `Logger` trait objects;
- hidden CBOR helpers and fixture parser types;
- `unsupported_operation`, which is an SDK compatibility helper rather than a
  DAX operation.
