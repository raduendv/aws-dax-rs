//! AWS DAX client for Rust.
//!
//! This crate is a Go-to-Rust compatibility port. It validates DAX
//! configuration and endpoint rules, and its direct transport implements the
//! supported bounded operation, discovery, routing, and lifecycle slices.
//! The [`fixture`] module validates language-neutral conformance fixtures that
//! later porting phases consume.
//!
//! The public API follows the asynchronous, fluent style of the AWS SDK for
//! Rust. See the repository's `PORTING_PLAN.md` for compatibility boundaries,
//! delivery phases, and validation requirements.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod client;
mod config;
mod error;
mod logging;
mod operation;
mod paginator;
mod protocol;

pub use client::{Client, RouteSnapshot};
pub use config::{Config, ConfigBuilder, Endpoint, EndpointScheme, IpDiscovery};
pub use error::{
    ConfigError, DaxErrorKind, Error, TransactionCancellationReason, TransportErrorKind,
};
pub use logging::{LogLevel, Logger};
pub use operation::{
    BatchGetItemFluentBuilder, BatchWriteItemFluentBuilder, DeleteItemFluentBuilder,
    GetItemFluentBuilder, PutItemFluentBuilder, QueryFluentBuilder, ScanFluentBuilder,
    TransactGetItemsFluentBuilder, TransactWriteItemsFluentBuilder, UpdateItemFluentBuilder,
};
pub use paginator::{BatchGetItemPaginator, QueryPaginator, ScanPaginator};
pub use protocol::{cbor::DiscoveredEndpoint, cluster::EndpointRefresh};

/// Unstable internal DAX CBOR protocol support.
///
/// This module exists for protocol conformance testing during the port and has
/// no public compatibility guarantees. Application code must use [`Client`].
#[doc(hidden)]
pub mod cbor {
    pub use crate::protocol::cbor::{
        CborError, decode_attribute_value, decode_item_key, decode_item_non_key_attributes,
        decode_transaction_cancellation_item, encode_attribute_value, encode_item_key,
        encode_item_non_key_attributes,
    };
}

/// Language-neutral conformance fixture support.
pub mod fixture;
