#![allow(
    missing_docs,
    reason = "generated contract fields are documented together in CONTRACT.md"
)]

use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum DaxAttributeValue {
    String {
        string: String,
    },
    Number {
        number: String,
    },
    Binary {
        binary: Vec<u8>,
    },
    Boolean {
        boolean: bool,
    },
    Null,
    StringSet {
        values: Vec<String>,
    },
    NumberSet {
        values: Vec<String>,
    },
    BinarySet {
        values: Vec<Vec<u8>>,
    },
    ListValue {
        values: Vec<DaxAttributeValue>,
    },
    MapValue {
        values: HashMap<String, DaxAttributeValue>,
    },
}

pub type DaxItem = HashMap<String, DaxAttributeValue>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum DaxReturnValue {
    None,
    AllOld,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum DaxReturnConsumedCapacity {
    None,
    Total,
    Indexes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum DaxReturnItemCollectionMetrics {
    None,
    Size,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum DaxReturnValuesOnConditionCheckFailure {
    None,
    AllOld,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum DaxSelect {
    AllAttributes,
    AllProjectedAttributes,
    SpecificAttributes,
    Count,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum DaxIpDiscovery {
    Default,
    Ipv4,
    Ipv6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum DaxLogLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct DaxClientConfig {
    pub endpoints: Vec<String>,
    pub region: Option<String>,
    pub request_timeout_millis: u64,
    pub write_retries: u32,
    pub read_retries: u32,
    pub retry_delay_millis: u64,
    pub throttle_base_delay_millis: u64,
    pub throttle_max_backoff_millis: u64,
    pub max_pending_connections_per_host: i32,
    pub cluster_update_interval_millis: u64,
    pub cluster_update_threshold_millis: u64,
    pub idle_connection_reap_delay_millis: u64,
    pub client_health_check_interval_millis: u64,
    pub skip_hostname_verification: bool,
    pub route_manager_enabled: bool,
    pub ip_discovery: DaxIpDiscovery,
    pub log_level: DaxLogLevel,
}

impl Default for DaxClientConfig {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            region: None,
            request_timeout_millis: 60_000,
            write_retries: 2,
            read_retries: 2,
            retry_delay_millis: 0,
            throttle_base_delay_millis: 70,
            throttle_max_backoff_millis: 20_000,
            max_pending_connections_per_host: 10,
            cluster_update_interval_millis: 4_000,
            cluster_update_threshold_millis: 125,
            idle_connection_reap_delay_millis: 30_000,
            client_health_check_interval_millis: 5_000,
            skip_hostname_verification: false,
            route_manager_enabled: false,
            ip_discovery: DaxIpDiscovery::Default,
            log_level: DaxLogLevel::Off,
        }
    }
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxGetItemRequest {
    pub table_name: String,
    pub key: DaxItem,
    pub consistent_read: Option<bool>,
    pub projection_expression: Option<String>,
    pub expression_attribute_names: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxGetItemResponse {
    pub item: Option<DaxItem>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxPutItemRequest {
    pub table_name: String,
    pub item: DaxItem,
    pub return_values: Option<DaxReturnValue>,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: HashMap<String, String>,
    pub expression_attribute_values: DaxItem,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxPutItemResponse {
    pub attributes: Option<DaxItem>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxDeleteItemRequest {
    pub table_name: String,
    pub key: DaxItem,
    pub return_values: Option<DaxReturnValue>,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: HashMap<String, String>,
    pub expression_attribute_values: DaxItem,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxDeleteItemResponse {
    pub attributes: Option<DaxItem>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxUpdateItemRequest {
    pub table_name: String,
    pub key: DaxItem,
    pub update_expression: String,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: HashMap<String, String>,
    pub expression_attribute_values: DaxItem,
    pub return_values: Option<DaxReturnValue>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxUpdateItemResponse {
    pub attributes: Option<DaxItem>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxQueryRequest {
    pub table_name: String,
    pub index_name: Option<String>,
    pub key_condition_expression: String,
    pub filter_expression: Option<String>,
    pub projection_expression: Option<String>,
    pub expression_attribute_names: HashMap<String, String>,
    pub expression_attribute_values: DaxItem,
    pub consistent_read: Option<bool>,
    pub limit: Option<i32>,
    pub scan_index_forward: Option<bool>,
    pub select: Option<DaxSelect>,
    pub return_consumed_capacity: Option<DaxReturnConsumedCapacity>,
    pub exclusive_start_key: Option<DaxItem>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxScanRequest {
    pub table_name: String,
    pub index_name: Option<String>,
    pub filter_expression: Option<String>,
    pub projection_expression: Option<String>,
    pub expression_attribute_names: HashMap<String, String>,
    pub expression_attribute_values: DaxItem,
    pub consistent_read: Option<bool>,
    pub limit: Option<i32>,
    pub select: Option<DaxSelect>,
    pub return_consumed_capacity: Option<DaxReturnConsumedCapacity>,
    pub exclusive_start_key: Option<DaxItem>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxCapacity {
    pub capacity_units: Option<f64>,
    pub read_capacity_units: Option<f64>,
    pub write_capacity_units: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxConsumedCapacity {
    pub table_name: Option<String>,
    pub capacity_units: Option<f64>,
    pub read_capacity_units: Option<f64>,
    pub write_capacity_units: Option<f64>,
    pub table: Option<DaxCapacity>,
    pub local_secondary_indexes: HashMap<String, DaxCapacity>,
    pub global_secondary_indexes: HashMap<String, DaxCapacity>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxQueryResponse {
    pub items: Vec<DaxItem>,
    pub count: i32,
    pub scanned_count: i32,
    pub last_evaluated_key: Option<DaxItem>,
    pub consumed_capacity: Option<DaxConsumedCapacity>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxScanResponse {
    pub items: Vec<DaxItem>,
    pub count: i32,
    pub scanned_count: i32,
    pub last_evaluated_key: Option<DaxItem>,
    pub consumed_capacity: Option<DaxConsumedCapacity>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxKeysAndAttributes {
    pub keys: Vec<DaxItem>,
    pub consistent_read: Option<bool>,
    pub projection_expression: Option<String>,
    pub expression_attribute_names: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxBatchGetItemRequest {
    pub request_items: HashMap<String, DaxKeysAndAttributes>,
    pub return_consumed_capacity: Option<DaxReturnConsumedCapacity>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxBatchGetItemResponse {
    pub responses: HashMap<String, Vec<DaxItem>>,
    pub unprocessed_keys: HashMap<String, DaxKeysAndAttributes>,
    pub consumed_capacity: Vec<DaxConsumedCapacity>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum DaxBatchWriteAction {
    Put { item: DaxItem },
    Delete { key: DaxItem },
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxBatchWriteItemRequest {
    pub request_items: HashMap<String, Vec<DaxBatchWriteAction>>,
    pub return_consumed_capacity: Option<DaxReturnConsumedCapacity>,
    pub return_item_collection_metrics: Option<DaxReturnItemCollectionMetrics>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxItemCollectionMetrics {
    pub item_collection_key: DaxItem,
    pub size_estimate_range_gb: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxBatchWriteItemResponse {
    pub unprocessed_items: HashMap<String, Vec<DaxBatchWriteAction>>,
    pub consumed_capacity: Vec<DaxConsumedCapacity>,
    pub item_collection_metrics: HashMap<String, Vec<DaxItemCollectionMetrics>>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxTransactGetAction {
    pub table_name: String,
    pub key: DaxItem,
    pub projection_expression: Option<String>,
    pub expression_attribute_names: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxTransactGetItemsRequest {
    pub transact_items: Vec<DaxTransactGetAction>,
    pub return_consumed_capacity: Option<DaxReturnConsumedCapacity>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxTransactGetResponse {
    pub item: Option<DaxItem>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxTransactGetItemsResponse {
    pub responses: Vec<DaxTransactGetResponse>,
    pub consumed_capacity: Vec<DaxConsumedCapacity>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum DaxTransactWriteAction {
    ConditionCheck {
        table_name: String,
        key: DaxItem,
        condition_expression: String,
        expression_attribute_names: HashMap<String, String>,
        expression_attribute_values: DaxItem,
        return_values_on_condition_check_failure: Option<DaxReturnValuesOnConditionCheckFailure>,
    },
    Put {
        table_name: String,
        item: DaxItem,
        condition_expression: Option<String>,
        expression_attribute_names: HashMap<String, String>,
        expression_attribute_values: DaxItem,
        return_values_on_condition_check_failure: Option<DaxReturnValuesOnConditionCheckFailure>,
    },
    Delete {
        table_name: String,
        key: DaxItem,
        condition_expression: Option<String>,
        expression_attribute_names: HashMap<String, String>,
        expression_attribute_values: DaxItem,
        return_values_on_condition_check_failure: Option<DaxReturnValuesOnConditionCheckFailure>,
    },
    Update {
        table_name: String,
        key: DaxItem,
        update_expression: String,
        condition_expression: Option<String>,
        expression_attribute_names: HashMap<String, String>,
        expression_attribute_values: DaxItem,
        return_values_on_condition_check_failure: Option<DaxReturnValuesOnConditionCheckFailure>,
    },
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxTransactWriteItemsRequest {
    pub transact_items: Vec<DaxTransactWriteAction>,
    pub client_request_token: Option<String>,
    pub return_consumed_capacity: Option<DaxReturnConsumedCapacity>,
    pub return_item_collection_metrics: Option<DaxReturnItemCollectionMetrics>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct DaxTransactWriteItemsResponse {
    pub consumed_capacity: Vec<DaxConsumedCapacity>,
    pub item_collection_metrics: HashMap<String, Vec<DaxItemCollectionMetrics>>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct DaxDiscoveredEndpoint {
    pub node_id: i64,
    pub hostname: String,
    pub address: Vec<u8>,
    pub port: i64,
    pub role: i64,
    pub availability_zone: Option<String>,
    pub leader_session_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct DaxEndpointRefresh {
    pub added: Vec<i64>,
    pub removed: Vec<i64>,
    pub retained: Vec<i64>,
    pub changed: Vec<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct DaxRouteSnapshot {
    pub node_id: i64,
    pub dial_address: String,
    pub tls_server_name: String,
    pub healthy: bool,
    pub health_probe_healthy: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum DaxErrorCategory {
    Configuration,
    NotImplemented,
    TransportUnavailable,
    Validation,
    Protocol,
    Transport,
    Service,
    Closed,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum DaxConfigurationErrorKind {
    MissingEndpoint,
    MissingRegion,
    MissingCredentials,
    NegativeMaxPendingConnections,
    InvalidIpDiscovery,
    InvalidEndpoint,
    UnsupportedEndpointScheme,
    InconsistentEndpointSchemes,
    MultipleEncryptedEndpoints,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum DaxServiceErrorKind {
    ResourceNotFound,
    ResourceInUse,
    ConditionalCheckFailed,
    ProvisionedThroughputExceeded,
    Throttling,
    Validation,
    InternalServer,
    ItemCollectionSizeLimitExceeded,
    LimitExceeded,
    TransactionCanceled,
    TransactionConflict,
    TransactionInProgress,
    IdempotentParameterMismatch,
    NotImplemented,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum DaxTransportErrorKind {
    Timeout,
    Io,
    UnexpectedEof,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct DaxTransactionCancellationReason {
    pub code: Option<String>,
    pub message: Option<String>,
    pub item_cbor: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct DaxException {
    pub category: DaxErrorCategory,
    pub message: String,
    pub operation: Option<String>,
    pub configuration_kind: Option<DaxConfigurationErrorKind>,
    pub transport_kind: Option<DaxTransportErrorKind>,
    pub service_kind: Option<DaxServiceErrorKind>,
    pub request_id: Option<String>,
    pub error_code: Option<String>,
    pub status_code: Option<i64>,
    pub code_sequence: Vec<i64>,
    pub cancellation_reasons: Vec<DaxTransactionCancellationReason>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_defaults_match_sdk_defaults() {
        let config = DaxClientConfig::default();
        assert_eq!(config.request_timeout_millis, 60_000);
        assert_eq!(config.write_retries, 2);
        assert_eq!(config.read_retries, 2);
        assert_eq!(config.max_pending_connections_per_host, 10);
        assert_eq!(config.ip_discovery, DaxIpDiscovery::Default);
    }

    #[test]
    fn attribute_values_are_recursive() {
        let value = DaxAttributeValue::MapValue {
            values: HashMap::from([(
                "nested".to_owned(),
                DaxAttributeValue::ListValue {
                    values: vec![DaxAttributeValue::Number {
                        number: "1.25".to_owned(),
                    }],
                },
            )]),
        };
        assert!(matches!(value, DaxAttributeValue::MapValue { .. }));
    }
}
