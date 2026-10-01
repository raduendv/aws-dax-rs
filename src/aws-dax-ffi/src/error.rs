use crate::{
    DaxConfigurationErrorKind, DaxErrorCategory, DaxException, DaxServiceErrorKind,
    DaxTransactionCancellationReason, DaxTransportErrorKind,
};

/// Structured failure returned by exported FFI operations.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum DaxFfiError {
    /// An operation failed with stable structured details.
    #[error("{summary}")]
    Failure {
        /// Stable human-readable summary.
        summary: String,
        /// Language-neutral failure details.
        details: DaxException,
    },
}

impl DaxFfiError {
    pub(crate) fn internal(message: impl Into<String>) -> Self {
        let message = message.into();
        Self::Failure {
            summary: message.clone(),
            details: DaxException {
                category: DaxErrorCategory::Unknown,
                message,
                operation: None,
                configuration_kind: None,
                transport_kind: None,
                service_kind: None,
                request_id: None,
                error_code: None,
                status_code: None,
                code_sequence: Vec::new(),
                cancellation_reasons: Vec::new(),
            },
        }
    }

    pub(crate) fn validation(message: impl Into<String>) -> Self {
        let message = message.into();
        Self::Failure {
            summary: message.clone(),
            details: DaxException {
                category: DaxErrorCategory::Validation,
                message,
                operation: None,
                configuration_kind: None,
                transport_kind: None,
                service_kind: None,
                request_id: None,
                error_code: None,
                status_code: None,
                code_sequence: Vec::new(),
                cancellation_reasons: Vec::new(),
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn details(&self) -> &DaxException {
        match self {
            Self::Failure { details, .. } => details,
        }
    }
}

impl From<aws_dax::Error> for DaxFfiError {
    fn from(error: aws_dax::Error) -> Self {
        let message = error.to_string();
        let details = match error {
            aws_dax::Error::Configuration(error) => DaxException {
                category: DaxErrorCategory::Configuration,
                message: message.clone(),
                operation: None,
                configuration_kind: Some(configuration_error_kind(&error)),
                transport_kind: None,
                service_kind: None,
                request_id: None,
                error_code: None,
                status_code: None,
                code_sequence: Vec::new(),
                cancellation_reasons: Vec::new(),
            },
            aws_dax::Error::NotImplemented { operation } => DaxException {
                category: DaxErrorCategory::NotImplemented,
                message: message.clone(),
                operation: Some(operation.to_owned()),
                configuration_kind: None,
                transport_kind: None,
                service_kind: None,
                request_id: None,
                error_code: None,
                status_code: None,
                code_sequence: Vec::new(),
                cancellation_reasons: Vec::new(),
            },
            aws_dax::Error::TransportUnavailable { operation } => DaxException {
                category: DaxErrorCategory::TransportUnavailable,
                message: message.clone(),
                operation: Some(operation.to_owned()),
                configuration_kind: None,
                transport_kind: None,
                service_kind: None,
                request_id: None,
                error_code: None,
                status_code: None,
                code_sequence: Vec::new(),
                cancellation_reasons: Vec::new(),
            },
            aws_dax::Error::Validation { message: detail } => DaxException {
                category: DaxErrorCategory::Validation,
                message: detail,
                operation: None,
                configuration_kind: None,
                transport_kind: None,
                service_kind: None,
                request_id: None,
                error_code: None,
                status_code: None,
                code_sequence: Vec::new(),
                cancellation_reasons: Vec::new(),
            },
            aws_dax::Error::Protocol {
                operation,
                message: detail,
            } => DaxException {
                category: DaxErrorCategory::Protocol,
                message: detail,
                operation: Some(operation.to_owned()),
                configuration_kind: None,
                transport_kind: None,
                service_kind: None,
                request_id: None,
                error_code: None,
                status_code: None,
                code_sequence: Vec::new(),
                cancellation_reasons: Vec::new(),
            },
            aws_dax::Error::Transport {
                operation,
                kind,
                message: detail,
            } => DaxException {
                category: DaxErrorCategory::Transport,
                message: detail,
                operation: Some(operation.to_owned()),
                configuration_kind: None,
                transport_kind: Some(transport_error_kind(kind)),
                service_kind: None,
                request_id: None,
                error_code: None,
                status_code: None,
                code_sequence: Vec::new(),
                cancellation_reasons: Vec::new(),
            },
            aws_dax::Error::Dax {
                kind,
                message: detail,
                request_id,
                error_code,
                status_code,
                code_sequence,
                cancellation_reasons,
            } => DaxException {
                category: DaxErrorCategory::Service,
                message: detail,
                operation: None,
                configuration_kind: None,
                transport_kind: None,
                service_kind: Some(service_error_kind(kind)),
                request_id,
                error_code,
                status_code: Some(status_code),
                code_sequence,
                cancellation_reasons: cancellation_reasons
                    .map(Vec::from)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|reason| DaxTransactionCancellationReason {
                        code: reason.code,
                        message: reason.message,
                        item_cbor: reason.item_cbor.map(Vec::from),
                    })
                    .collect(),
            },
            aws_dax::Error::Closed => DaxException {
                category: DaxErrorCategory::Closed,
                message: message.clone(),
                operation: None,
                configuration_kind: None,
                transport_kind: None,
                service_kind: None,
                request_id: None,
                error_code: None,
                status_code: None,
                code_sequence: Vec::new(),
                cancellation_reasons: Vec::new(),
            },
            _ => DaxException {
                category: DaxErrorCategory::Unknown,
                message: message.clone(),
                operation: None,
                configuration_kind: None,
                transport_kind: None,
                service_kind: None,
                request_id: None,
                error_code: None,
                status_code: None,
                code_sequence: Vec::new(),
                cancellation_reasons: Vec::new(),
            },
        };
        Self::Failure {
            summary: message,
            details,
        }
    }
}

fn configuration_error_kind(error: &aws_dax::ConfigError) -> DaxConfigurationErrorKind {
    match error {
        aws_dax::ConfigError::MissingEndpoint => DaxConfigurationErrorKind::MissingEndpoint,
        aws_dax::ConfigError::MissingRegion => DaxConfigurationErrorKind::MissingRegion,
        aws_dax::ConfigError::MissingCredentials => DaxConfigurationErrorKind::MissingCredentials,
        aws_dax::ConfigError::NegativeMaxPendingConnections => {
            DaxConfigurationErrorKind::NegativeMaxPendingConnections
        }
        aws_dax::ConfigError::InvalidIpDiscovery(_) => {
            DaxConfigurationErrorKind::InvalidIpDiscovery
        }
        aws_dax::ConfigError::InvalidEndpoint => DaxConfigurationErrorKind::InvalidEndpoint,
        aws_dax::ConfigError::UnsupportedEndpointScheme => {
            DaxConfigurationErrorKind::UnsupportedEndpointScheme
        }
        aws_dax::ConfigError::InconsistentEndpointSchemes => {
            DaxConfigurationErrorKind::InconsistentEndpointSchemes
        }
        aws_dax::ConfigError::MultipleEncryptedEndpoints => {
            DaxConfigurationErrorKind::MultipleEncryptedEndpoints
        }
        _ => DaxConfigurationErrorKind::Unknown,
    }
}

fn transport_error_kind(kind: aws_dax::TransportErrorKind) -> DaxTransportErrorKind {
    match kind {
        aws_dax::TransportErrorKind::Timeout => DaxTransportErrorKind::Timeout,
        aws_dax::TransportErrorKind::Io => DaxTransportErrorKind::Io,
        aws_dax::TransportErrorKind::UnexpectedEof => DaxTransportErrorKind::UnexpectedEof,
        _ => DaxTransportErrorKind::Unknown,
    }
}

fn service_error_kind(kind: aws_dax::DaxErrorKind) -> DaxServiceErrorKind {
    match kind {
        aws_dax::DaxErrorKind::ResourceNotFound => DaxServiceErrorKind::ResourceNotFound,
        aws_dax::DaxErrorKind::ResourceInUse => DaxServiceErrorKind::ResourceInUse,
        aws_dax::DaxErrorKind::ConditionalCheckFailed => {
            DaxServiceErrorKind::ConditionalCheckFailed
        }
        aws_dax::DaxErrorKind::ProvisionedThroughputExceeded => {
            DaxServiceErrorKind::ProvisionedThroughputExceeded
        }
        aws_dax::DaxErrorKind::Throttling => DaxServiceErrorKind::Throttling,
        aws_dax::DaxErrorKind::Validation => DaxServiceErrorKind::Validation,
        aws_dax::DaxErrorKind::InternalServer => DaxServiceErrorKind::InternalServer,
        aws_dax::DaxErrorKind::ItemCollectionSizeLimitExceeded => {
            DaxServiceErrorKind::ItemCollectionSizeLimitExceeded
        }
        aws_dax::DaxErrorKind::LimitExceeded => DaxServiceErrorKind::LimitExceeded,
        aws_dax::DaxErrorKind::TransactionCanceled => DaxServiceErrorKind::TransactionCanceled,
        aws_dax::DaxErrorKind::TransactionConflict => DaxServiceErrorKind::TransactionConflict,
        aws_dax::DaxErrorKind::TransactionInProgress => DaxServiceErrorKind::TransactionInProgress,
        aws_dax::DaxErrorKind::IdempotentParameterMismatch => {
            DaxServiceErrorKind::IdempotentParameterMismatch
        }
        aws_dax::DaxErrorKind::NotImplemented => DaxServiceErrorKind::NotImplemented,
        aws_dax::DaxErrorKind::Unknown => DaxServiceErrorKind::Unknown,
        _ => DaxServiceErrorKind::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_error_keeps_structured_category() {
        let error = DaxFfiError::from(aws_dax::Error::Closed);
        assert_eq!(error.details().category, DaxErrorCategory::Closed);
        assert_eq!(error.to_string(), "DAX client is closed");
    }

    #[test]
    fn configuration_error_keeps_specific_kind() {
        let error = DaxFfiError::from(aws_dax::Error::Configuration(
            aws_dax::ConfigError::MissingEndpoint,
        ));
        assert_eq!(
            error.details().configuration_kind,
            Some(DaxConfigurationErrorKind::MissingEndpoint)
        );
    }

    #[test]
    fn transaction_error_keeps_cancellation_reasons_and_items() {
        let error = DaxFfiError::from(aws_dax::Error::Dax {
            kind: aws_dax::DaxErrorKind::TransactionCanceled,
            message: "transaction canceled".into(),
            request_id: Some("request-1".into()),
            error_code: Some("TransactionCanceledException".into()),
            status_code: 400,
            code_sequence: vec![4, 23, 31],
            cancellation_reasons: Some(
                vec![aws_dax::TransactionCancellationReason {
                    code: Some("ConditionalCheckFailed".into()),
                    message: Some("condition failed".into()),
                    item_cbor: Some(vec![0x81, 0xf6].into_boxed_slice()),
                }]
                .into_boxed_slice(),
            ),
        });
        let details = error.details();
        assert_eq!(
            details.service_kind,
            Some(DaxServiceErrorKind::TransactionCanceled)
        );
        assert_eq!(details.request_id.as_deref(), Some("request-1"));
        assert_eq!(details.code_sequence, vec![4, 23, 31]);
        assert_eq!(
            details.cancellation_reasons,
            vec![DaxTransactionCancellationReason {
                code: Some("ConditionalCheckFailed".into()),
                message: Some("condition failed".into()),
                item_cbor: Some(vec![0x81, 0xf6]),
            }]
        );
    }
}
