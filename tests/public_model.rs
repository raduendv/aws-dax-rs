use std::{
    collections::HashMap,
    future::Future,
    net::IpAddr,
    pin::pin,
    str::FromStr,
    sync::{Arc, Barrier, Mutex},
    task::{Context, Poll, Waker},
    thread,
    time::Duration,
};

use aws_config::{Region, SdkConfig};
use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_dax::{
    BatchGetItemPaginator, Client, Config, ConfigError, DaxErrorKind, Endpoint, EndpointScheme,
    Error, IpDiscovery, LogLevel, Logger, QueryPaginator, ScanPaginator,
    TransactionCancellationReason,
};
use aws_sdk_dynamodb::{
    operation::batch_get_item::BatchGetItemInput,
    operation::batch_write_item::BatchWriteItemInput,
    operation::delete_item::DeleteItemInput,
    operation::get_item::GetItemInput,
    operation::put_item::PutItemInput,
    operation::query::QueryInput,
    operation::scan::ScanInput,
    operation::transact_get_items::TransactGetItemsInput,
    operation::transact_write_items::TransactWriteItemsInput,
    operation::update_item::UpdateItemInput,
    types::{
        AttributeDefinition, AttributeValue, Put, ScalarAttributeType, TransactGetItem,
        TransactWriteItem, WriteRequest,
    },
};

#[derive(Default)]
struct CapturingLogger(Mutex<Vec<(LogLevel, String)>>);

impl Logger for CapturingLogger {
    fn log(&self, level: LogLevel, message: &str) {
        self.0.lock().unwrap().push((level, message.into()));
    }
}

#[test]
fn transact_get_over_limit_reports_before_schema_lookup() {
    let client = Client::new(valid_config()).unwrap();
    let items = (0..101)
        .map(|_| TransactGetItem::builder().build())
        .collect();
    let input = TransactGetItemsInput::builder()
        .set_transact_items(Some(items))
        .build()
        .unwrap();
    let mut future = pin!(client.transact_get_items().input(input).send());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "TransactGetItems request count"
    ));
}

#[test]
fn batch_get_over_limit_reports_before_schema_lookup() {
    let client = Client::new(valid_config()).unwrap();
    let keys = (0..101)
        .map(|id| {
            let mut key = HashMap::new();
            key.insert("id".to_owned(), AttributeValue::N(id.to_string()));
            key
        })
        .collect();
    let mut request_items = HashMap::new();
    request_items.insert(
        "TestTable".to_owned(),
        aws_sdk_dynamodb::types::KeysAndAttributes::builder()
            .set_keys(Some(keys))
            .build()
            .unwrap(),
    );
    let input = BatchGetItemInput::builder()
        .set_request_items(Some(request_items))
        .build()
        .unwrap();
    let mut future = pin!(client.batch_get_item().input(input).send());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "BatchGetItem request count"
    ));
}

#[test]
fn transact_write_over_limit_reports_before_schema_lookup() {
    let client = Client::new(valid_config()).unwrap();
    let items = (0..101)
        .map(|_| TransactWriteItem::builder().build())
        .collect();
    let input = TransactWriteItemsInput::builder()
        .set_transact_items(Some(items))
        .build()
        .unwrap();
    let mut future = pin!(client.transact_write_items().input(input).send());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "TransactWriteItems request count"
    ));
}

fn credentials_provider() -> SharedCredentialsProvider {
    SharedCredentialsProvider::new(Credentials::new(
        "access-key",
        "secret-key",
        None,
        None,
        "test",
    ))
}

#[test]
fn batch_write_over_limit_reports_before_schema_lookup() {
    let client = Client::new(valid_config()).unwrap();
    let requests = (0..26)
        .map(|id| {
            let mut item = HashMap::new();
            item.insert("id".to_owned(), AttributeValue::N(id.to_string()));
            let put = aws_sdk_dynamodb::types::PutRequest::builder()
                .set_item(Some(item))
                .build()
                .unwrap();
            WriteRequest::builder().put_request(put).build()
        })
        .collect();
    let mut request_items = HashMap::new();
    request_items.insert("TestTable".to_owned(), requests);
    let input = BatchWriteItemInput::builder()
        .set_request_items(Some(request_items))
        .build()
        .unwrap();
    let mut future = pin!(client.batch_write_item().input(input).send());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "BatchWriteItem request count"
    ));
}

fn valid_config() -> Config {
    Config::builder()
        .endpoint("dax://cluster.example.com")
        .region("us-west-2")
        .credentials_provider(credentials_provider())
        .build()
        .expect("config is valid")
}

#[test]
fn creates_a_client_from_shared_aws_sdk_configuration() {
    let sdk_config = SdkConfig::builder()
        .region(Region::new("us-west-2"))
        .credentials_provider(credentials_provider())
        .build();

    let client = Client::from_sdk_config(&sdk_config, "dax://cluster.example.com:8111").unwrap();

    assert_eq!(
        client.config().endpoints().unwrap(),
        vec![Endpoint::parse("dax://cluster.example.com:8111").unwrap()]
    );
}

#[test]
fn defaults_match_the_reference_configuration() {
    let config = Config::default();

    assert_eq!(config.request_timeout(), Duration::from_secs(60));
    assert_eq!(config.write_retries(), 2);
    assert_eq!(config.read_retries(), 2);
    assert_eq!(config.retry_delay(), Duration::ZERO);
    assert_eq!(config.max_pending_connections_per_host(), 10);
    assert_eq!(config.cluster_update_interval(), Duration::from_secs(4));
    assert_eq!(
        config.cluster_update_threshold(),
        Duration::from_millis(125)
    );
    assert_eq!(config.idle_connection_reap_delay(), Duration::from_secs(30));
    assert_eq!(
        config.client_health_check_interval(),
        Duration::from_secs(5)
    );
    assert!(!config.skip_hostname_verification());
    assert!(!config.route_manager_enabled());
    assert_eq!(config.ip_discovery(), IpDiscovery::Default);
    assert_eq!(config.log_level(), LogLevel::Off);
}

#[test]
fn validates_required_configuration_fields() {
    assert_eq!(
        Client::new(Config::default()).unwrap_err(),
        Error::Configuration(ConfigError::MissingEndpoint)
    );

    let missing_region = Config::builder()
        .endpoint("dax://cluster.example.com")
        .credentials_provider(credentials_provider())
        .build()
        .unwrap_err();
    assert_eq!(missing_region, ConfigError::MissingRegion);

    let missing_credentials = Config::builder()
        .endpoint("dax://cluster.example.com")
        .region("us-west-2")
        .build()
        .unwrap_err();
    assert_eq!(missing_credentials, ConfigError::MissingCredentials);
}

#[test]
fn preserves_public_transaction_cancellation_reasons() {
    let error = Error::Dax {
        kind: DaxErrorKind::TransactionCanceled,
        message: "transaction canceled".into(),
        request_id: Some("request-1".into()),
        error_code: Some("TransactionCanceledException".into()),
        status_code: 400,
        code_sequence: vec![4, 37, 39, 58],
        cancellation_reasons: Some(
            vec![TransactionCancellationReason {
                code: Some("ConditionalCheckFailed".into()),
                message: Some("condition failed".into()),
                item_cbor: Some(vec![0x81, 0xf6].into_boxed_slice()),
            }]
            .into_boxed_slice(),
        ),
    };

    let Error::Dax {
        kind,
        cancellation_reasons,
        ..
    } = error
    else {
        panic!("expected DAX error");
    };
    assert_eq!(kind, DaxErrorKind::TransactionCanceled);
    assert_eq!(
        cancellation_reasons.as_deref(),
        Some(
            [TransactionCancellationReason {
                code: Some("ConditionalCheckFailed".into()),
                message: Some("condition failed".into()),
                item_cbor: Some(vec![0x81, 0xf6].into_boxed_slice()),
            }]
            .as_slice()
        )
    );
}

#[test]
fn decodes_contextual_transaction_cancellation_item() {
    let reason = TransactionCancellationReason {
        code: Some("ConditionalCheckFailed".into()),
        message: Some("condition failed".into()),
        item_cbor: Some(vec![0x01, 0x63, b'o', b'l', b'd'].into_boxed_slice()),
    };
    let key = HashMap::from([("pk".to_owned(), AttributeValue::S("v".into()))]);
    let schema = vec![
        AttributeDefinition::builder()
            .attribute_name("pk")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .unwrap(),
    ];
    let attribute_lists = HashMap::from([(1_i64, vec!["status".to_owned()])]);

    let item = reason.decode_item(&key, &schema, &attribute_lists).unwrap();

    assert_eq!(item.get("pk"), Some(&AttributeValue::S("v".into())));
    assert_eq!(item.get("status"), Some(&AttributeValue::S("old".into())));
}

#[test]
fn decodes_contextual_transaction_cancellation_composite_key() {
    let reason = TransactionCancellationReason {
        code: Some("ConditionalCheckFailed".into()),
        message: None,
        item_cbor: Some(vec![0x02, 0x63, b'o', b'l', b'd', 0x63, b'n', b'e', b'w'].into()),
    };
    let key = HashMap::from([
        ("pk".to_owned(), AttributeValue::S("partition".into())),
        ("sk".to_owned(), AttributeValue::N("7".into())),
    ]);
    let schema = vec![
        AttributeDefinition::builder()
            .attribute_name("pk")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .unwrap(),
        AttributeDefinition::builder()
            .attribute_name("sk")
            .attribute_type(ScalarAttributeType::N)
            .build()
            .unwrap(),
    ];
    let attribute_lists = HashMap::from([(2_i64, vec!["status".to_owned(), "note".to_owned()])]);

    let item = reason.decode_item(&key, &schema, &attribute_lists).unwrap();

    assert_eq!(item.len(), 4);
    assert_eq!(item.get("pk"), Some(&AttributeValue::S("partition".into())));
    assert_eq!(item.get("sk"), Some(&AttributeValue::N("7".into())));
    assert_eq!(item.get("status"), Some(&AttributeValue::S("old".into())));
    assert_eq!(item.get("note"), Some(&AttributeValue::S("new".into())));
}

#[test]
fn decodes_mixed_transaction_cancellation_items_with_request_keys() {
    let schema = vec![
        AttributeDefinition::builder()
            .attribute_name("hk")
            .attribute_type(ScalarAttributeType::N)
            .build()
            .unwrap(),
    ];
    let keys = [
        HashMap::from([("hk".to_owned(), AttributeValue::N("0".into()))]),
        HashMap::from([("hk".to_owned(), AttributeValue::N("1".into()))]),
        HashMap::from([("hk".to_owned(), AttributeValue::N("2".into()))]),
    ];
    let attribute_lists = HashMap::from([(12345_i64, vec!["attr".to_owned()])]);
    let reasons = [
        TransactionCancellationReason {
            code: Some("NONE".into()),
            message: None,
            item_cbor: None,
        },
        TransactionCancellationReason {
            code: Some("ConditionalCheckFailed".into()),
            message: Some("first reason".into()),
            item_cbor: Some(vec![0x19, 0x30, 0x39, 0x61, 0x30].into_boxed_slice()),
        },
        TransactionCancellationReason {
            code: Some("TransactionInProgress".into()),
            message: Some("second reason".into()),
            item_cbor: None,
        },
    ];

    let item = reasons[1]
        .decode_item(&keys[1], &schema, &attribute_lists)
        .unwrap();
    assert_eq!(reasons[0].code.as_deref(), Some("NONE"));
    assert_eq!(reasons[2].message.as_deref(), Some("second reason"));
    assert_eq!(item.get("hk"), Some(&AttributeValue::N("1".into())));
    assert_eq!(item.get("attr"), Some(&AttributeValue::S("0".into())));
}

#[test]
fn rejects_contextual_transaction_cancellation_item_with_incomplete_key() {
    let reason = TransactionCancellationReason {
        code: None,
        message: None,
        item_cbor: Some(vec![0x01, 0x63, b'o', b'l', b'd'].into_boxed_slice()),
    };
    let key = HashMap::from([("pk".to_owned(), AttributeValue::S("partition".into()))]);
    let schema = vec![
        AttributeDefinition::builder()
            .attribute_name("pk")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .unwrap(),
        AttributeDefinition::builder()
            .attribute_name("sk")
            .attribute_type(ScalarAttributeType::N)
            .build()
            .unwrap(),
    ];
    let attribute_lists = HashMap::from([(1_i64, vec!["status".to_owned()])]);

    assert_eq!(
        reason.decode_item(&key, &schema, &attribute_lists),
        Err(aws_dax::cbor::CborError::InvalidItemKey(
            "a required key is missing",
        ))
    );
}

#[test]
fn rejects_cancellation_item_without_payload() {
    let reason = TransactionCancellationReason {
        code: None,
        message: None,
        item_cbor: None,
    };
    let key = HashMap::from([("pk".to_owned(), AttributeValue::S("v".into()))]);
    let schema = vec![
        AttributeDefinition::builder()
            .attribute_name("pk")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .unwrap(),
    ];

    assert_eq!(
        reason.decode_item(&key, &schema, &HashMap::new()),
        Err(aws_dax::cbor::CborError::MissingItemPayload)
    );
}

#[test]
fn rejects_cancellation_item_with_unknown_attribute_list() {
    let reason = TransactionCancellationReason {
        code: None,
        message: None,
        item_cbor: Some(vec![0x01, 0x63, b'o', b'l', b'd'].into_boxed_slice()),
    };
    let key = HashMap::from([("pk".to_owned(), AttributeValue::S("v".into()))]);
    let schema = vec![
        AttributeDefinition::builder()
            .attribute_name("pk")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .unwrap(),
    ];

    assert_eq!(
        reason.decode_item(&key, &schema, &HashMap::new()),
        Err(aws_dax::cbor::CborError::UnknownAttributeListId(1))
    );
}

#[test]
fn exposes_distinct_go_service_error_categories() {
    let categories = [
        DaxErrorKind::ResourceInUse,
        DaxErrorKind::ItemCollectionSizeLimitExceeded,
        DaxErrorKind::LimitExceeded,
        DaxErrorKind::TransactionConflict,
        DaxErrorKind::TransactionInProgress,
        DaxErrorKind::IdempotentParameterMismatch,
    ];

    assert_eq!(categories.len(), 6);
    assert_ne!(categories[0], DaxErrorKind::Unknown);
    assert_ne!(categories[1], categories[2]);
    assert_ne!(categories[3], categories[4]);
    assert_ne!(categories[4], categories[5]);
}

#[test]
fn normalizes_dax_endpoints() {
    let plain = Endpoint::parse("cluster.example.com:8123").unwrap();
    assert_eq!(plain.scheme(), EndpointScheme::Dax);
    assert_eq!(plain.host(), "cluster.example.com");
    assert_eq!(plain.port(), 8123);

    let tls = Endpoint::parse("daxs://cluster.example.com").unwrap();
    assert_eq!(tls.scheme(), EndpointScheme::Daxs);
    assert_eq!(tls.port(), 9111);

    let ipv6 = Endpoint::parse("dax://[2001:db8::1]:8111").unwrap();
    assert_eq!(ipv6.host(), "2001:db8::1");
    assert_eq!(ipv6.port(), 8111);
}

#[test]
fn preserves_observable_invalid_port_fallback() {
    let endpoint = Endpoint::parse("daxs://cluster.example.com:not-a-port").unwrap();
    assert_eq!(endpoint.port(), 9111);
}

#[test]
fn redacts_unsupported_endpoint_scheme_diagnostics() {
    let error = Endpoint::parse("access-key-secret://cluster.example.com:8111").unwrap_err();
    assert_eq!(error, ConfigError::UnsupportedEndpointScheme);
    assert!(!error.to_string().contains("access-key-secret"));
    assert!(!format!("{error:?}").contains("access-key-secret"));
}

#[test]
fn rejects_invalid_endpoint_scheme_combinations() {
    let mixed = Config::builder()
        .endpoint("dax://first.example.com")
        .endpoint("daxs://second.example.com")
        .region("us-west-2")
        .credentials_provider(credentials_provider())
        .build()
        .unwrap_err();
    assert_eq!(mixed, ConfigError::InconsistentEndpointSchemes);

    let multiple_tls = Config::builder()
        .endpoint("daxs://first.example.com")
        .endpoint("daxs://second.example.com")
        .region("us-west-2")
        .credentials_provider(credentials_provider())
        .build()
        .unwrap_err();
    assert_eq!(multiple_tls, ConfigError::MultipleEncryptedEndpoints);
}

#[test]
fn rejects_endpoint_user_information_and_redacts_configuration_debug_output() {
    let error = Endpoint::parse("dax://access-key:secret@cluster.example.com:8111").unwrap_err();
    assert_eq!(error, ConfigError::InvalidEndpoint);
    assert!(!error.to_string().contains("access-key"));
    assert!(!error.to_string().contains("secret"));

    let config = Config::builder()
        .endpoint("dax://cluster.example.com:8111")
        .region("us-west-2")
        .credentials_provider(credentials_provider())
        .build()
        .unwrap();
    let debug = format!("{config:?}");
    assert!(!debug.contains("cluster.example.com"));
    assert!(!debug.contains("access-key"));
    assert!(!debug.contains("secret-key"));
}

#[test]
fn preserves_all_exposed_configuration_controls() {
    let logger = Arc::new(CapturingLogger::default());
    let config = Config::builder()
        .endpoint("dax://cluster.example.com:8111")
        .region("us-west-2")
        .credentials_provider(credentials_provider())
        .request_timeout(Duration::from_secs(45))
        .write_retries(3)
        .read_retries(4)
        .retry_delay(Duration::from_millis(10))
        .max_pending_connections_per_host(3)
        .cluster_update_interval(Duration::from_secs(6))
        .cluster_update_threshold(Duration::from_millis(250))
        .idle_connection_reap_delay(Duration::from_secs(40))
        .client_health_check_interval(Duration::from_secs(7))
        .skip_hostname_verification(true)
        .route_manager_enabled(true)
        .ip_discovery(IpDiscovery::Ipv6)
        .logger(logger)
        .log_level(LogLevel::Debug)
        .build()
        .unwrap();

    assert_eq!(config.request_timeout(), Duration::from_secs(45));
    assert_eq!(config.read_retries(), 4);
    assert_eq!(config.write_retries(), 3);
    assert_eq!(config.retry_delay(), Duration::from_millis(10));
    assert_eq!(config.max_pending_connections_per_host(), 3);
    assert_eq!(config.cluster_update_interval(), Duration::from_secs(6));
    assert_eq!(
        config.cluster_update_threshold(),
        Duration::from_millis(250)
    );
    assert_eq!(config.idle_connection_reap_delay(), Duration::from_secs(40));
    assert_eq!(
        config.client_health_check_interval(),
        Duration::from_secs(7)
    );
    assert!(config.skip_hostname_verification());
    assert!(config.route_manager_enabled());
    assert_eq!(config.ip_discovery(), IpDiscovery::Ipv6);
    assert_eq!(config.log_level(), LogLevel::Debug);

    let error = Config::builder()
        .endpoint("dax://cluster.example.com:8111")
        .region("us-west-2")
        .credentials_provider(credentials_provider())
        .max_pending_connections_per_host(-1)
        .build()
        .unwrap_err();
    assert_eq!(error, ConfigError::NegativeMaxPendingConnections);
}

#[test]
fn selects_addresses_using_the_go_discovery_policy() {
    let addresses = [
        IpAddr::from_str("2001:db8::1").unwrap(),
        IpAddr::from_str("192.0.2.1").unwrap(),
        IpAddr::from_str("2001:db8::2").unwrap(),
    ];

    assert_eq!(
        IpDiscovery::Default.select_addresses(&addresses).unwrap(),
        vec![IpAddr::from_str("192.0.2.1").unwrap()]
    );
    assert_eq!(
        IpDiscovery::Ipv6.select_addresses(&addresses).unwrap(),
        vec![
            IpAddr::from_str("2001:db8::1").unwrap(),
            IpAddr::from_str("2001:db8::2").unwrap(),
        ]
    );
    assert_eq!(
        IpDiscovery::Ipv4
            .select_addresses(&[IpAddr::from_str("2001:db8::1").unwrap()])
            .unwrap_err(),
        Error::Validation {
            message: "ipDiscovery ipv4 does not match the SupportedNetworkType ipv6.".into()
        }
    );
}

#[test]
fn client_close_is_idempotent_and_unsupported_operations_are_explicit() {
    let client = Client::new(valid_config()).unwrap();
    assert_eq!(
        client.unsupported_operation("CreateBackup").unwrap_err(),
        Error::NotImplemented {
            operation: "CreateBackup"
        }
    );

    client.close().unwrap();
    client.close().unwrap();
    assert!(client.is_closed());
    assert_eq!(
        client.unsupported_operation("CreateBackup").unwrap_err(),
        Error::Closed
    );
}

#[test]
fn exposes_typed_builders_for_all_go_supported_operations() {
    let client = Client::new(valid_config()).unwrap();

    let _ = client.put_item();
    let _ = client.delete_item();
    let _ = client.update_item();
    let _ = client.get_item();
    let _ = client.scan();
    let _ = client.query();
    let _ = client.batch_write_item();
    let _ = client.batch_get_item();
    let _ = client.transact_write_items();
    let _ = client.transact_get_items();
}

#[test]
fn get_item_without_input_reports_explicit_validation() {
    let client = Client::new(valid_config()).unwrap();
    let mut future = pin!(client.get_item().send());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required GetItem input"
    ));
}

#[test]
fn operations_without_input_report_explicit_validation() {
    let client = Client::new(valid_config()).unwrap();
    let mut context = Context::from_waker(Waker::noop());

    let mut put = pin!(client.put_item().send());
    assert!(matches!(
        put.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required PutItem input"
    ));
    let mut delete = pin!(client.delete_item().send());
    assert!(matches!(
        delete.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required DeleteItem input"
    ));
    let mut update = pin!(client.update_item().send());
    assert!(matches!(
        update.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required UpdateItem input"
    ));
    let mut query = pin!(client.query().send());
    assert!(matches!(
        query.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required Query input"
    ));
    let mut scan = pin!(client.scan().send());
    assert!(matches!(
        scan.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required Scan input"
    ));
    let mut batch_write = pin!(client.batch_write_item().send());
    assert!(matches!(
        batch_write.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required BatchWriteItem input"
    ));
    let mut batch_get = pin!(client.batch_get_item().send());
    assert!(matches!(
        batch_get.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required BatchGetItem input"
    ));
    let mut transact_write = pin!(client.transact_write_items().send());
    assert!(matches!(
        transact_write.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required TransactWriteItems input"
    ));
    let mut transact_get = pin!(client.transact_get_items().send());
    assert!(matches!(
        transact_get.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required TransactGetItems input"
    ));
}

#[test]
fn item_operations_validate_required_fields_before_schema_lookup() {
    let client = Client::new(valid_config()).unwrap();
    let mut context = Context::from_waker(Waker::noop());

    let put = PutItemInput::builder().build().unwrap();
    let mut put_future = pin!(client.put_item().input(put).send());
    assert!(matches!(
        put_future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required parameter: TableName"
    ));

    let delete = DeleteItemInput::builder()
        .table_name("TestTable")
        .build()
        .unwrap();
    let mut delete_future = pin!(client.delete_item().input(delete).send());
    assert!(matches!(
        delete_future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required parameter: Key"
    ));

    let get = GetItemInput::builder()
        .key("id", AttributeValue::S("1".into()))
        .build()
        .unwrap();
    let mut get_future = pin!(client.get_item().input(get).send());
    assert!(matches!(
        get_future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required parameter: TableName"
    ));

    let update = UpdateItemInput::builder()
        .table_name("TestTable")
        .update_expression("SET #value = :value")
        .build()
        .unwrap();
    let mut update_future = pin!(client.update_item().input(update).send());
    assert!(matches!(
        update_future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required parameter: Key"
    ));
}

#[test]
fn query_and_scan_validate_table_name_before_schema_lookup() {
    let client = Client::new(valid_config()).unwrap();
    let mut context = Context::from_waker(Waker::noop());

    let query = QueryInput::builder().build().unwrap();
    let mut query_future = pin!(client.query().input(query).send());
    assert!(matches!(
        query_future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required parameter: TableName"
    ));

    let scan = ScanInput::builder().build().unwrap();
    let mut scan_future = pin!(client.scan().input(scan).send());
    assert!(matches!(
        scan_future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required parameter: TableName"
    ));
}

#[tokio::test]
async fn paginators_defer_input_validation_until_first_page() {
    let client = Client::new(valid_config()).unwrap();

    let query = QueryInput::builder().build().unwrap();
    let mut query_paginator: QueryPaginator = client.query_paginator(query);
    assert!(query_paginator.has_more_pages());
    assert!(matches!(
        query_paginator.next_page().await,
        Err(Error::Validation { message })
            if message == "missing required parameter: TableName"
    ));

    let scan = ScanInput::builder().build().unwrap();
    let mut scan_paginator: ScanPaginator = client.scan_paginator(scan);
    assert!(scan_paginator.has_more_pages());
    assert!(matches!(
        scan_paginator.next_page().await,
        Err(Error::Validation { message })
            if message == "missing required parameter: TableName"
    ));

    let batch_get = BatchGetItemInput::builder().build().unwrap();
    let mut batch_get_paginator: BatchGetItemPaginator = client.batch_get_item_paginator(batch_get);
    assert!(batch_get_paginator.has_more_pages());
    assert!(matches!(
        batch_get_paginator.next_page().await,
        Err(Error::Validation { message })
            if message == "missing required parameter: RequestItems"
    ));
}

#[test]
fn update_without_expression_reports_before_schema_lookup() {
    let client = Client::new(valid_config()).unwrap();
    let input = UpdateItemInput::builder()
        .table_name("TestTable")
        .key("id", AttributeValue::S("1".into()))
        .build()
        .unwrap();
    let mut future = pin!(client.update_item().input(input).send());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required parameter: UpdateExpression"
    ));
}

#[test]
fn transact_write_without_action_reports_before_schema_lookup() {
    let client = Client::new(valid_config()).unwrap();
    let input = TransactWriteItemsInput::builder()
        .transact_items(TransactWriteItem::builder().build())
        .build()
        .unwrap();
    let mut future = pin!(client.transact_write_items().input(input).send());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "TransactWriteItems item must contain one action"
    ));
}

#[test]
fn transact_write_with_multiple_actions_reports_before_schema_lookup() {
    let client = Client::new(valid_config()).unwrap();
    let mut item = HashMap::new();
    item.insert("id".to_owned(), AttributeValue::S("1".into()));
    let put = Put::builder()
        .table_name("TestTable")
        .set_item(Some(item))
        .build()
        .unwrap();
    let transaction_item = TransactWriteItem::builder()
        .put(put)
        .delete(
            aws_sdk_dynamodb::types::Delete::builder()
                .table_name("TestTable")
                .set_key(Some(HashMap::from([(
                    "id".to_owned(),
                    AttributeValue::S("1".into()),
                )])))
                .build()
                .unwrap(),
        )
        .build();
    let input = TransactWriteItemsInput::builder()
        .transact_items(transaction_item)
        .build()
        .unwrap();
    let mut future = pin!(client.transact_write_items().input(input).send());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "TransactWriteItems item must contain one action"
    ));
}

#[test]
fn transact_get_without_get_action_reports_explicit_validation() {
    let client = Client::new(valid_config()).unwrap();
    let input = TransactGetItemsInput::builder()
        .transact_items(TransactGetItem::builder().build())
        .build()
        .unwrap();
    let mut future = pin!(client.transact_get_items().input(input).send());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "TransactGetItems item must contain Get"
    ));
}

#[test]
fn batch_write_without_action_reports_before_schema_lookup() {
    let client = Client::new(valid_config()).unwrap();
    let mut request_items = HashMap::new();
    request_items.insert(
        "TestTable".to_owned(),
        vec![WriteRequest::builder().build()],
    );
    let input = BatchWriteItemInput::builder()
        .set_request_items(Some(request_items))
        .build()
        .unwrap();
    let mut future = pin!(client.batch_write_item().input(input).send());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "BatchWriteItem write request shape"
    ));
}

#[test]
fn batch_get_with_empty_request_items_reports_explicit_validation() {
    let client = Client::new(valid_config()).unwrap();
    let input = BatchGetItemInput::builder()
        .set_request_items(Some(HashMap::new()))
        .build()
        .unwrap();
    let mut future = pin!(client.batch_get_item().input(input).send());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "BatchGetItem request items"
    ));
}

#[test]
fn batch_get_with_empty_table_keys_reports_explicit_validation() {
    let client = Client::new(valid_config()).unwrap();
    let mut request_items = HashMap::new();
    request_items.insert(
        "TestTable".to_owned(),
        aws_sdk_dynamodb::types::KeysAndAttributes::builder()
            .set_keys(Some(Vec::new()))
            .build()
            .unwrap(),
    );
    let input = BatchGetItemInput::builder()
        .set_request_items(Some(request_items))
        .build()
        .unwrap();
    let mut future = pin!(client.batch_get_item().input(input).send());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "BatchGetItem request items"
    ));
}

#[test]
fn batch_write_with_empty_request_items_reports_explicit_validation() {
    let client = Client::new(valid_config()).unwrap();
    let input = BatchWriteItemInput::builder()
        .set_request_items(Some(HashMap::new()))
        .build()
        .unwrap();
    let mut future = pin!(client.batch_write_item().input(input).send());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { .. }))
    ));
}

#[test]
fn batch_write_with_empty_table_requests_reports_explicit_validation() {
    let client = Client::new(valid_config()).unwrap();
    let mut request_items = HashMap::new();
    request_items.insert("TestTable".to_owned(), Vec::new());
    let input = BatchWriteItemInput::builder()
        .set_request_items(Some(request_items))
        .build()
        .unwrap();
    let mut future = pin!(client.batch_write_item().input(input).send());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "BatchWriteItem request count"
    ));
}

#[test]
fn transact_write_with_empty_items_reports_explicit_validation() {
    let client = Client::new(valid_config()).unwrap();
    let input = TransactWriteItemsInput::builder()
        .set_transact_items(Some(Vec::new()))
        .build()
        .unwrap();
    let mut future = pin!(client.transact_write_items().input(input).send());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required parameter: TransactItems"
    ));
}

#[test]
fn transact_get_with_empty_items_reports_explicit_validation() {
    let client = Client::new(valid_config()).unwrap();
    let input = TransactGetItemsInput::builder()
        .set_transact_items(Some(Vec::new()))
        .build()
        .unwrap();
    let mut future = pin!(client.transact_get_items().input(input).send());
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Validation { message }))
            if message == "missing required parameter: TransactItems"
    ));
}

#[test]
fn logs_construction_and_keeps_shared_client_open_until_the_last_clone_drops() {
    let logger = Arc::new(CapturingLogger::default());
    let client = Client::new(
        Config::builder()
            .endpoint("dax://cluster.example.com:8111")
            .region("us-west-2")
            .credentials_provider(credentials_provider())
            .logger(logger.clone())
            .log_level(LogLevel::Info)
            .build()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        logger.0.lock().unwrap().as_slice(),
        &[(
            LogLevel::Info,
            "DAX client created without starting network transport".into()
        )]
    );

    let clone = client.clone();
    drop(client);
    assert!(
        !clone.is_closed(),
        "dropping a clone must not close the client"
    );
    drop(clone);
}

#[test]
#[allow(clippy::result_large_err)]
fn concurrent_close_and_builder_send_observe_a_stable_closed_state() {
    let client = Arc::new(Client::new(valid_config()).unwrap());
    let barrier = Arc::new(Barrier::new(3));

    let closer = {
        let client = client.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            barrier.wait();
            client.close()
        })
    };
    let sender = {
        let client = client.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            barrier.wait();
            let builder = client.get_item();
            client.close().unwrap();
            let mut future = pin!(builder.send());
            let mut context = Context::from_waker(Waker::noop());
            future.as_mut().poll(&mut context)
        })
    };

    barrier.wait();
    closer.join().unwrap().unwrap();
    assert_eq!(sender.join().unwrap(), Poll::Ready(Err(Error::Closed)));
    assert!(client.is_closed());
}
