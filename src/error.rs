use std::{error::Error as StdError, fmt};

use aws_sdk_dynamodb::types::{AttributeDefinition, AttributeValue};

use crate::protocol::cbor::DaxResponseError;

/// A normalized DAX server failure category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DaxErrorKind {
    /// The requested resource does not exist.
    ResourceNotFound,
    /// The requested resource is already in use.
    ResourceInUse,
    /// A condition expression was not satisfied.
    ConditionalCheckFailed,
    /// The request exceeded available provisioned throughput.
    ProvisionedThroughputExceeded,
    /// The request was throttled.
    Throttling,
    /// The request failed DAX/DynamoDB validation.
    Validation,
    /// The service reported an internal failure.
    InternalServer,
    /// The item collection exceeded its size limit.
    ItemCollectionSizeLimitExceeded,
    /// The request exceeded a service-side limit.
    LimitExceeded,
    /// A transaction was canceled and may include per-item reasons.
    TransactionCanceled,
    /// A transaction conflicted with another transaction.
    TransactionConflict,
    /// A transaction is still in progress.
    TransactionInProgress,
    /// A transaction token was reused with different parameters.
    IdempotentParameterMismatch,
    /// The requested operation is not implemented by DAX.
    NotImplemented,
    /// The DAX code sequence is not recognized yet.
    Unknown,
}

/// Configuration validation or endpoint parsing failure.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigError {
    /// No cluster discovery endpoint was configured.
    MissingEndpoint,
    /// No AWS region was configured.
    MissingRegion,
    /// No AWS credential provider was configured.
    MissingCredentials,
    /// The configured maximum pending connection count is invalid.
    NegativeMaxPendingConnections,
    /// The requested IP discovery preference is invalid.
    InvalidIpDiscovery(String),
    /// An endpoint could not be parsed.
    InvalidEndpoint,
    /// An endpoint scheme is unsupported.
    UnsupportedEndpointScheme,
    /// Encrypted and unencrypted endpoints were mixed.
    InconsistentEndpointSchemes,
    /// More than one encrypted discovery endpoint was configured.
    MultipleEncryptedEndpoints,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEndpoint => write!(formatter, "missing required parameter: Endpoint"),
            Self::MissingRegion => write!(formatter, "missing required parameter: config.Region"),
            Self::MissingCredentials => {
                write!(formatter, "missing required parameter: config.Credentials")
            }
            Self::NegativeMaxPendingConnections => {
                write!(formatter, "MaxPendingConnectionsPerHost cannot be negative")
            }
            Self::InvalidIpDiscovery(value) => write!(
                formatter,
                "config.IpDiscovery must be 'ipv4' or 'ipv6', found `{value}`"
            ),
            Self::InvalidEndpoint => write!(formatter, "invalid DAX endpoint"),
            Self::UnsupportedEndpointScheme => {
                write!(formatter, "URL scheme must be one of dax,daxs")
            }
            Self::InconsistentEndpointSchemes => {
                write!(
                    formatter,
                    "inconsistency between the schemes of provided endpoints"
                )
            }
            Self::MultipleEncryptedEndpoints => write!(
                formatter,
                "only one cluster discovery endpoint may be provided for encrypted cluster"
            ),
        }
    }
}

impl StdError for ConfigError {}

/// Classifies failures caused by the connection rather than by DAX.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransportErrorKind {
    /// The request exceeded its configured deadline.
    Timeout,
    /// Establishing or using the socket failed.
    Io,
    /// The peer closed the stream before a complete response arrived.
    UnexpectedEof,
}

/// A reason attached to a canceled transaction item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionCancellationReason {
    /// DAX cancellation code, when supplied.
    pub code: Option<String>,
    /// DAX cancellation message, when supplied.
    pub message: Option<String>,
    /// Raw CBOR item payload supplied by DAX, when present.
    pub item_cbor: Option<Box<[u8]>>,
}

impl TransactionCancellationReason {
    /// Decodes the compressed item payload with transaction request context.
    ///
    /// DAX cancellation payloads omit key attributes and refer to a cached
    /// attribute-name list, so decoding requires the original request key,
    /// table key schema, and cache contents.
    pub fn decode_item(
        &self,
        key: &std::collections::HashMap<String, AttributeValue>,
        key_definition: &[AttributeDefinition],
        attribute_lists: &std::collections::HashMap<i64, Vec<String>>,
    ) -> Result<std::collections::HashMap<String, AttributeValue>, crate::cbor::CborError> {
        crate::cbor::decode_transaction_cancellation_item(
            self.item_cbor.as_deref(),
            key,
            key_definition,
            attribute_lists,
        )
    }
}

/// A DAX client failure.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// Client construction rejected its configuration.
    Configuration(ConfigError),
    /// The requested DynamoDB operation is unsupported by DAX.
    NotImplemented {
        /// DynamoDB operation name.
        operation: &'static str,
    },
    /// A DAX-supported operation whose transport has not been ported yet.
    TransportUnavailable {
        /// DynamoDB operation name.
        operation: &'static str,
    },
    /// A request or discovered endpoint failed DAX validation.
    Validation {
        /// Stable DAX validation message.
        message: String,
    },
    /// DAX protocol framing or decoding failed for an operation.
    Protocol {
        /// DAX operation name.
        operation: &'static str,
        /// Stable protocol failure description.
        message: String,
    },
    /// A connection-level failure eligible for routed failover.
    Transport {
        /// DAX operation name.
        operation: &'static str,
        /// Connection failure category.
        kind: TransportErrorKind,
        /// Stable diagnostic description.
        message: String,
    },
    /// A normalized DAX server response failure.
    Dax {
        /// Stable failure category derived from the DAX code sequence.
        kind: DaxErrorKind,
        /// DAX diagnostic message.
        message: String,
        /// DAX request ID, when supplied.
        request_id: Option<String>,
        /// DAX service error code, when supplied.
        error_code: Option<String>,
        /// DAX status code, explicit or inferred.
        status_code: i64,
        /// Raw DAX error-code sequence for retry and diagnostics.
        code_sequence: Vec<i64>,
        /// Per-item cancellation reasons for transaction cancellation failures.
        cancellation_reasons: Option<Box<[TransactionCancellationReason]>>,
    },
    /// The client has already been closed.
    Closed,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(error) => error.fmt(formatter),
            Self::NotImplemented { operation } => {
                write!(
                    formatter,
                    "NotImplemented: `{operation}` is not supported by DAX"
                )
            }
            Self::TransportUnavailable { operation } => write!(
                formatter,
                "DAX transport is not available yet for supported operation `{operation}`"
            ),
            Self::Validation { message } => write!(formatter, "ValidationException: {message}"),
            Self::Protocol { operation, message } => {
                write!(
                    formatter,
                    "DAX protocol failure for `{operation}`: {message}"
                )
            }
            Self::Transport {
                operation, message, ..
            } => {
                write!(
                    formatter,
                    "DAX transport failure for `{operation}`: {message}"
                )
            }
            Self::Dax {
                kind,
                message,
                request_id,
                status_code,
                ..
            } => {
                write!(
                    formatter,
                    "DAX {kind:?} failure (status {status_code}, request ID {}): {message}",
                    request_id.as_deref().unwrap_or("unavailable")
                )
            }
            Self::Closed => write!(formatter, "DAX client is closed"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Configuration(error) => Some(error),
            Self::NotImplemented { .. }
            | Self::TransportUnavailable { .. }
            | Self::Validation { .. }
            | Self::Protocol { .. }
            | Self::Transport { .. }
            | Self::Dax { .. }
            | Self::Closed => None,
        }
    }
}

impl From<ConfigError> for Error {
    fn from(value: ConfigError) -> Self {
        Self::Configuration(value)
    }
}

impl From<DaxResponseError> for Error {
    fn from(value: DaxResponseError) -> Self {
        Self::Dax {
            kind: dax_error_kind(&value.code_sequence),
            message: value.message,
            request_id: value.request_id,
            error_code: value.error_code,
            status_code: value.status_code,
            code_sequence: value.code_sequence,
            cancellation_reasons: value.cancellation_reasons.and_then(|reasons| {
                if reasons.is_empty() {
                    None
                } else {
                    Some(
                        reasons
                            .into_iter()
                            .map(|reason| TransactionCancellationReason {
                                code: reason.code,
                                message: reason.message,
                                item_cbor: reason.item_cbor,
                            })
                            .collect::<Vec<_>>()
                            .into_boxed_slice(),
                    )
                }
            }),
        }
    }
}

fn dax_error_kind(codes: &[i64]) -> DaxErrorKind {
    match codes {
        [_, 23, 24, ..] => DaxErrorKind::ResourceNotFound,
        [_, 23, 35, ..] => DaxErrorKind::ResourceInUse,
        _ if dax_error_code(codes) == Some(41) => DaxErrorKind::ResourceNotFound,
        _ if dax_error_code(codes) == Some(45) => DaxErrorKind::ResourceInUse,
        _ if dax_error_code(codes) == Some(40) => DaxErrorKind::ProvisionedThroughputExceeded,
        _ if dax_error_code(codes) == Some(43) => DaxErrorKind::ConditionalCheckFailed,
        _ if dax_error_code(codes) == Some(46) => DaxErrorKind::Validation,
        _ if dax_error_code(codes) == Some(47) => DaxErrorKind::InternalServer,
        _ if dax_error_code(codes) == Some(48) => DaxErrorKind::ItemCollectionSizeLimitExceeded,
        _ if dax_error_code(codes) == Some(49) => DaxErrorKind::LimitExceeded,
        _ if dax_error_code(codes) == Some(50) => DaxErrorKind::Throttling,
        _ if dax_error_code(codes) == Some(58) => DaxErrorKind::TransactionCanceled,
        _ if dax_error_code(codes) == Some(57) => DaxErrorKind::TransactionConflict,
        _ if dax_error_code(codes) == Some(59) => DaxErrorKind::TransactionInProgress,
        _ if dax_error_code(codes) == Some(60) => DaxErrorKind::IdempotentParameterMismatch,
        _ if dax_not_implemented(codes) => DaxErrorKind::NotImplemented,
        _ => DaxErrorKind::Unknown,
    }
}

pub(crate) fn dax_error_kind_for_retry(codes: &[i64]) -> Option<DaxErrorKind> {
    let kind = dax_error_kind(codes);
    matches!(
        kind,
        DaxErrorKind::ProvisionedThroughputExceeded | DaxErrorKind::Throttling
    )
    .then_some(kind)
}

fn dax_error_code(codes: &[i64]) -> Option<i64> {
    match codes {
        [_, 37, 39, code, ..] | [_, 37, 38, 39, code, ..] => Some(*code),
        _ => None,
    }
}

fn dax_not_implemented(codes: &[i64]) -> bool {
    matches!(codes, [_, 37, 39, 44, ..] | [_, 37, 38, 44, ..])
}

#[cfg(test)]
mod tests {
    use super::{DaxErrorKind, Error, dax_error_kind};
    use crate::protocol::cbor::DaxResponseError;

    #[test]
    fn maps_pinned_go_dax_code_sequences() {
        assert_eq!(dax_error_kind(&[4, 23, 24]), DaxErrorKind::ResourceNotFound);
        assert_eq!(dax_error_kind(&[4, 23, 35]), DaxErrorKind::ResourceInUse);
        assert_eq!(
            dax_error_kind(&[4, 37, 39, 43]),
            DaxErrorKind::ConditionalCheckFailed
        );
        assert_eq!(
            dax_error_kind(&[4, 37, 39, 40]),
            DaxErrorKind::ProvisionedThroughputExceeded
        );
        assert_eq!(dax_error_kind(&[4, 37, 39, 50]), DaxErrorKind::Throttling);
        assert_eq!(
            dax_error_kind(&[4, 37, 38, 39, 50]),
            DaxErrorKind::Throttling
        );
        let intermediate_code_cases = [
            (40, DaxErrorKind::ProvisionedThroughputExceeded),
            (41, DaxErrorKind::ResourceNotFound),
            (43, DaxErrorKind::ConditionalCheckFailed),
            (45, DaxErrorKind::ResourceInUse),
            (46, DaxErrorKind::Validation),
            (47, DaxErrorKind::InternalServer),
            (48, DaxErrorKind::ItemCollectionSizeLimitExceeded),
            (49, DaxErrorKind::LimitExceeded),
            (57, DaxErrorKind::TransactionConflict),
            (58, DaxErrorKind::TransactionCanceled),
            (59, DaxErrorKind::TransactionInProgress),
            (60, DaxErrorKind::IdempotentParameterMismatch),
        ];
        for (code, expected) in intermediate_code_cases {
            assert_eq!(dax_error_kind(&[4, 37, 38, 39, code]), expected);
        }
        assert_eq!(
            dax_error_kind(&[4, 37, 39, 45]),
            DaxErrorKind::ResourceInUse
        );
        assert_eq!(
            dax_error_kind(&[4, 37, 39, 41]),
            DaxErrorKind::ResourceNotFound
        );
        assert_eq!(dax_error_kind(&[4, 37, 39, 46]), DaxErrorKind::Validation);
        assert_eq!(
            dax_error_kind(&[4, 37, 39, 47]),
            DaxErrorKind::InternalServer
        );
        assert_eq!(
            dax_error_kind(&[4, 37, 39, 48]),
            DaxErrorKind::ItemCollectionSizeLimitExceeded
        );
        assert_eq!(
            dax_error_kind(&[4, 37, 39, 49]),
            DaxErrorKind::LimitExceeded
        );
        assert_eq!(
            dax_error_kind(&[4, 37, 39, 58]),
            DaxErrorKind::TransactionCanceled
        );
        assert_eq!(
            dax_error_kind(&[4, 37, 39, 57]),
            DaxErrorKind::TransactionConflict
        );
        assert_eq!(
            dax_error_kind(&[4, 37, 39, 59]),
            DaxErrorKind::TransactionInProgress
        );
        assert_eq!(
            dax_error_kind(&[4, 37, 39, 60]),
            DaxErrorKind::IdempotentParameterMismatch
        );
        assert_eq!(
            dax_error_kind(&[4, 37, 39, 44]),
            DaxErrorKind::NotImplemented
        );
        assert_eq!(
            dax_error_kind(&[4, 37, 38, 44]),
            DaxErrorKind::NotImplemented
        );
        assert_eq!(dax_error_kind(&[4, 37, 40, 50]), DaxErrorKind::Unknown);
        assert_eq!(dax_error_kind(&[4, 37, 40, 44]), DaxErrorKind::Unknown);
        assert_eq!(dax_error_kind(&[4, 37, 44]), DaxErrorKind::Unknown);
        assert_eq!(dax_error_kind(&[3, 1]), DaxErrorKind::Unknown);
        assert_eq!(dax_error_kind(&[4, 58]), DaxErrorKind::Unknown);
        assert_eq!(dax_error_kind(&[58]), DaxErrorKind::Unknown);
    }

    #[test]
    fn formats_dax_failures_without_omitting_metadata() {
        let error = Error::Dax {
            kind: DaxErrorKind::Throttling,
            message: "busy".into(),
            request_id: Some("request-1".into()),
            error_code: Some("ThrottlingException".into()),
            status_code: 400,
            code_sequence: vec![4, 37, 39, 50],
            cancellation_reasons: None,
        };
        assert_eq!(
            error.to_string(),
            "DAX Throttling failure (status 400, request ID request-1): busy"
        );
    }

    #[test]
    fn converts_go_transaction_cancellation_metadata_to_public_error() {
        let error = Error::from(DaxResponseError {
            code_sequence: vec![4, 37, 39, 58],
            message: "transaction canceled".into(),
            request_id: Some("request-1".into()),
            error_code: Some("TransactionCanceledException".into()),
            status_code: 400,
            cancellation_reasons: Some(
                vec![crate::protocol::cbor::CancellationReasonMetadata {
                    code: Some("ConditionalCheckFailed".into()),
                    message: Some("condition failed".into()),
                    item_cbor: Some(vec![0x81, 0xf6].into_boxed_slice()),
                }]
                .into_boxed_slice(),
            ),
        });

        let Error::Dax {
            kind,
            cancellation_reasons,
            ..
        } = error
        else {
            panic!("expected DAX error");
        };
        assert_eq!(kind, DaxErrorKind::TransactionCanceled);
        let reasons = cancellation_reasons.expect("cancellation reasons");
        assert_eq!(reasons.len(), 1);
        assert_eq!(reasons[0].code.as_deref(), Some("ConditionalCheckFailed"));
        assert_eq!(reasons[0].message.as_deref(), Some("condition failed"));
        assert_eq!(
            reasons[0].item_cbor.as_deref(),
            Some([0x81, 0xf6].as_slice())
        );
    }

    #[test]
    fn omits_empty_go_transaction_cancellation_metadata() {
        let error = Error::from(DaxResponseError {
            code_sequence: vec![4, 37, 39, 58],
            message: "transaction canceled".into(),
            request_id: None,
            error_code: None,
            status_code: 400,
            cancellation_reasons: Some(Vec::new().into_boxed_slice()),
        });

        let Error::Dax {
            kind,
            cancellation_reasons,
            ..
        } = error
        else {
            panic!("expected DAX error");
        };
        assert_eq!(kind, DaxErrorKind::TransactionCanceled);
        assert_eq!(cancellation_reasons, None);
    }

    #[test]
    fn preserves_unknown_dax_error_metadata() {
        let error = Error::from(DaxResponseError {
            code_sequence: vec![9, 1, 2],
            message: "future service failure".into(),
            request_id: Some("request-unknown".into()),
            error_code: Some("FutureException".into()),
            status_code: 503,
            cancellation_reasons: None,
        });

        assert_eq!(
            error,
            Error::Dax {
                kind: DaxErrorKind::Unknown,
                message: "future service failure".into(),
                request_id: Some("request-unknown".into()),
                error_code: Some("FutureException".into()),
                status_code: 503,
                code_sequence: vec![9, 1, 2],
                cancellation_reasons: None,
            }
        );
    }
}
