# Changelog

All notable changes to `aws-dax` are documented here.

## Unreleased

The Rust port currently tracks the pinned `aws-dax-go-v2` `v1.0.3`
implementation for the supported bounded compatibility surface:

- AWS SDK-style configuration, fluent operation builders, typed errors, and
  paginators.
- DAX CBOR request/response codecs, SigV4 authorization, TCP/TLS transport,
  pooling, retries, discovery, routing, health checks, and lifecycle tasks.
- GetItem, PutItem, DeleteItem, UpdateItem, Query, Scan, BatchGetItem,
  BatchWriteItem, TransactGetItems, and TransactWriteItems.
- Go-compatible throttling equal-jitter backoff with configurable base and
  maximum delays, including zero-value default restoration.
- BatchGet response and paginator continuation coverage for unprocessed keys,
  including duplicate-token protection.
- Fuzz targets for AttributeValue, compressed attributes, item keys, and
  transaction-cancellation item decoding.

Known bounded compatibility edges are listed in the
[README](README.md) and tracked in [status.md](status.md).
