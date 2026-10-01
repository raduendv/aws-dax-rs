//! Loopback fixtures use the pinned SDK protocol vectors, not a live cluster.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use aws_sdk_dynamodb::{
    config::{Credentials, SharedCredentialsProvider},
    operation::{query::QueryOutput, scan::ScanOutput},
    types::{AttributeValue, Capacity, ConsumedCapacity},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::{JoinHandle, JoinSet},
};

use crate::{operation::*, *};

const WAIT: Duration = Duration::from_secs(5);
const ENDPOINTS: &[u8] = &[0x01, 0x1a, 0x1b, 0x2b, 0xcf, 0x02];

fn item(value: &str) -> DaxItem {
    HashMap::from([(
        "pk".into(),
        DaxAttributeValue::String {
            string: value.into(),
        },
    )])
}

fn query() -> DaxQueryRequest {
    DaxQueryRequest {
        table_name: "Table".into(),
        index_name: None,
        key_condition_expression: "pk = :value".into(),
        filter_expression: None,
        projection_expression: None,
        expression_attribute_names: HashMap::new(),
        expression_attribute_values: HashMap::from([(
            ":value".into(),
            DaxAttributeValue::String { string: "v".into() },
        )]),
        consistent_read: None,
        limit: None,
        scan_index_forward: None,
        select: None,
        return_consumed_capacity: None,
        exclusive_start_key: None,
    }
}

fn scan() -> DaxScanRequest {
    DaxScanRequest {
        table_name: "Table".into(),
        index_name: None,
        filter_expression: None,
        projection_expression: None,
        expression_attribute_names: HashMap::new(),
        expression_attribute_values: HashMap::new(),
        consistent_read: Some(true),
        limit: Some(1),
        select: None,
        return_consumed_capacity: Some(DaxReturnConsumedCapacity::Indexes),
        exclusive_start_key: Some(item("v")),
    }
}

fn batch() -> DaxBatchGetItemRequest {
    DaxBatchGetItemRequest {
        request_items: HashMap::from([(
            "Table".into(),
            DaxKeysAndAttributes {
                keys: vec![item("v")],
                consistent_read: Some(true),
                projection_expression: None,
                expression_attribute_names: HashMap::new(),
            },
        )]),
        return_consumed_capacity: None,
    }
}

fn client(endpoint: String) -> Arc<DaxClient> {
    Arc::new(DaxClient {
        inner: aws_dax::Client::new(
            aws_dax::Config::builder()
                .endpoint(endpoint)
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                    "key", "secret", None, None, "fixture",
                )))
                .read_retries(0)
                .request_timeout(Duration::from_secs(2))
                .cluster_update_interval(Duration::from_secs(3600))
                .idle_connection_reap_delay(Duration::from_secs(3600))
                .build()
                .unwrap(),
        )
        .unwrap(),
    })
}

fn hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    (0..value.len())
        .step_by(2)
        .map(|offset| u8::from_str_radix(&value[offset..offset + 2], 16).unwrap())
        .collect()
}

#[derive(Default)]
struct FixtureState {
    pages: Mutex<VecDeque<Vec<u8>>>,
    roster: Mutex<Vec<u8>>,
}

struct Fixture {
    client: Arc<DaxClient>,
    port: u16,
    state: Arc<FixtureState>,
    requests: mpsc::UnboundedReceiver<Vec<u8>>,
    disconnects: mpsc::UnboundedReceiver<()>,
    server: JoinHandle<()>,
}

impl Fixture {
    async fn new(pages: Vec<Vec<u8>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let port = address.port();
        let client = client(format!("dax://{address}"));
        let mut roster = hex("8081a8000101636e6f6402447f0000010319");
        roster.extend(port.to_be_bytes());
        roster.extend(hex("040105616106070780"));
        let state = Arc::new(FixtureState {
            pages: Mutex::new(pages.into()),
            roster: Mutex::new(roster),
        });
        let (requests_tx, requests) = mpsc::unbounded_channel();
        let (disconnects_tx, disconnects) = mpsc::unbounded_channel();
        let server_state = Arc::clone(&state);
        let server = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        connections.spawn(serve(
                            stream,
                            Arc::clone(&server_state),
                            requests_tx.clone(),
                            disconnects_tx.clone(),
                        ));
                    }
                    result = connections.join_next(), if !connections.is_empty() => {
                        result.unwrap().unwrap();
                    }
                }
            }
        });
        Self {
            client,
            port,
            state,
            requests,
            disconnects,
            server,
        }
    }

    async fn request(&mut self) -> Vec<u8> {
        tokio::time::timeout(WAIT, self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }

    async fn disconnected(&mut self) {
        tokio::time::timeout(WAIT, self.disconnects.recv())
            .await
            .unwrap()
            .unwrap();
    }

    async fn finish(&mut self) {
        self.client.shutdown().unwrap();
        assert!(!self.server.is_finished(), "fixture server failed");
        self.server.abort();
        assert!((&mut self.server).await.unwrap_err().is_cancelled());
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.client.shutdown().unwrap();
        self.server.abort();
    }
}

async fn serve(
    mut stream: TcpStream,
    state: Arc<FixtureState>,
    requests: mpsc::UnboundedSender<Vec<u8>>,
    disconnects: mpsc::UnboundedSender<()>,
) {
    let preamble = b"\x67J7yne5G\x00\x610\xa1\x69UserAgent\x73DaxGoV2Client-1.0.3\x00";
    let mut received = vec![0; preamble.len()];
    if let Err(error) = stream.read_exact(&mut received).await {
        assert_peer_closed(error);
        return;
    }
    assert_eq!(received, preamble);
    // Fixed credentials, region and timestamp width make authorization length
    // deterministic. No signature generation or production parser is duplicated.
    let string_to_sign_len =
        b"AWS4-HMAC-SHA256\n20000101T000000Z\n20000101/us-east-1/dax/aws4_request\n".len() + 64;
    let auth_len = 6 + 4 + 66 + 2 + string_to_sign_len + 1 + b"\x71DaxGoClient-1.0.0".len();
    let mut authorization = vec![0; auth_len];
    if let Err(error) = stream.read_exact(&mut authorization).await {
        assert_peer_closed(error);
        return;
    }

    loop {
        let mut header = [0; 6];
        match stream.read_exact(&mut header).await {
            Ok(_) => {}
            Err(error) => {
                assert_peer_closed(error);
                return;
            }
        }
        let response = if header == ENDPOINTS {
            let roster = state.roster.lock().unwrap().clone();
            if roster.is_empty() {
                requests.send(header.to_vec()).unwrap();
            }
            roster
        } else if header[..2] == [1, 0x3a] && header[2..] == 742_646_398_u32.to_be_bytes() {
            let mut table = [0; 6];
            stream.read_exact(&mut table).await.unwrap();
            assert_eq!(&table, b"\x45Table");
            hex("80a162706b6153")
        } else if header == [1, 0x1a, 0x27, 0xf9, 0xbd, 0x71] {
            assert_eq!(stream.read_u8().await.unwrap(), 9);
            hex("80816161")
        } else {
            assert_eq!(header[0], 1);
            assert_eq!(header[1], 0x3a);
            let mut request = header.to_vec();
            // All scripted data requests end in an indefinite kwargs map. The
            // fixed fixture inputs contain no other 0xff byte.
            loop {
                let byte = stream.read_u8().await.unwrap();
                request.push(byte);
                assert!(request.len() < 256, "unexpected fixture request");
                if byte == 0xff {
                    break;
                }
            }
            requests.send(request).unwrap();
            state.pages.lock().unwrap().pop_front().unwrap_or_default()
        };
        // An empty fixture response deliberately stalls until cancellation or
        // timeout drops the checked-out transport.
        if response.is_empty() {
            match stream.read(&mut [0; 1]).await {
                Ok(count) => assert_eq!(count, 0),
                Err(error) => assert_peer_closed(error),
            }
            let _ = disconnects.send(());
            return;
        }
        if let Err(error) = stream.write_all(&response).await {
            assert_peer_closed(error);
            return;
        }
    }
}

fn assert_peer_closed(error: std::io::Error) {
    assert!(
        matches!(
            error.kind(),
            std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::BrokenPipe
        ),
        "unexpected fixture I/O error: {error}"
    );
}

#[test]
fn paginator_requests_preserve_every_query_and_scan_field() {
    let mut request = query();
    request.index_name = Some("index".into());
    request.filter_expression = Some("#pk = :value".into());
    request.projection_expression = Some("#pk".into());
    request.expression_attribute_names = HashMap::from([("#pk".into(), "pk".into())]);
    request.consistent_read = Some(false);
    request.limit = Some(7);
    request.scan_index_forward = Some(false);
    request.select = Some(DaxSelect::SpecificAttributes);
    request.return_consumed_capacity = Some(DaxReturnConsumedCapacity::Total);
    request.exclusive_start_key = Some(item("start"));
    let sdk = query_input(request.clone()).unwrap();
    assert_eq!(sdk.table_name(), Some("Table"));
    assert_eq!(sdk.index_name(), request.index_name.as_deref());
    assert_eq!(
        sdk.key_condition_expression(),
        Some(request.key_condition_expression.as_str())
    );
    assert_eq!(
        sdk.filter_expression(),
        request.filter_expression.as_deref()
    );
    assert_eq!(
        sdk.projection_expression(),
        request.projection_expression.as_deref()
    );
    assert_eq!(
        sdk.expression_attribute_names(),
        Some(&request.expression_attribute_names)
    );
    assert_eq!(
        sdk.expression_attribute_values().unwrap()[":value"],
        AttributeValue::S("v".into())
    );
    assert_eq!(sdk.consistent_read(), Some(false));
    assert_eq!(sdk.limit(), Some(7));
    assert_eq!(sdk.scan_index_forward(), Some(false));
    assert_eq!(sdk.select().unwrap().as_str(), "SPECIFIC_ATTRIBUTES");
    assert_eq!(sdk.return_consumed_capacity().unwrap().as_str(), "TOTAL");
    assert_eq!(
        sdk.exclusive_start_key().unwrap()["pk"],
        AttributeValue::S("start".into())
    );

    let request = DaxScanRequest {
        table_name: request.table_name,
        index_name: request.index_name,
        filter_expression: request.filter_expression,
        projection_expression: request.projection_expression,
        expression_attribute_names: request.expression_attribute_names,
        expression_attribute_values: request.expression_attribute_values,
        consistent_read: request.consistent_read,
        limit: request.limit,
        select: request.select,
        return_consumed_capacity: request.return_consumed_capacity,
        exclusive_start_key: request.exclusive_start_key,
    };
    let sdk_scan = scan_input(request).unwrap();
    assert_eq!(sdk_scan.table_name, sdk.table_name);
    assert_eq!(sdk_scan.index_name, sdk.index_name);
    assert_eq!(sdk_scan.filter_expression, sdk.filter_expression);
    assert_eq!(sdk_scan.projection_expression, sdk.projection_expression);
    assert_eq!(
        sdk_scan.expression_attribute_names,
        sdk.expression_attribute_names
    );
    assert_eq!(
        sdk_scan.expression_attribute_values,
        sdk.expression_attribute_values
    );
    assert_eq!(sdk_scan.consistent_read, sdk.consistent_read);
    assert_eq!(sdk_scan.limit, sdk.limit);
    assert_eq!(sdk_scan.select, sdk.select);
    assert_eq!(
        sdk_scan.return_consumed_capacity,
        sdk.return_consumed_capacity
    );
    assert_eq!(sdk_scan.exclusive_start_key, sdk.exclusive_start_key);
    assert!(
        query_input(query())
            .unwrap()
            .expression_attribute_names
            .is_none()
    );
    let mut empty = scan();
    empty.exclusive_start_key = None;
    let sdk = scan_input(empty).unwrap();
    assert!(sdk.expression_attribute_values.is_none());
    assert!(sdk.exclusive_start_key.is_none());
}

#[test]
fn page_converters_preserve_items_tokens_counts_and_nested_capacity() {
    let values = HashMap::from([("pk".into(), AttributeValue::S("v".into()))]);
    let capacity = ConsumedCapacity::builder()
        .table_name("Table")
        .capacity_units(1.5)
        .read_capacity_units(1.0)
        .write_capacity_units(0.5)
        .table(Capacity::builder().capacity_units(0.5).build())
        .local_secondary_indexes(
            "local",
            Capacity::builder().read_capacity_units(0.25).build(),
        )
        .global_secondary_indexes(
            "global",
            Capacity::builder().write_capacity_units(0.75).build(),
        )
        .build();
    let query = query_response(
        QueryOutput::builder()
            .items(values.clone())
            .count(1)
            .scanned_count(7)
            .set_last_evaluated_key(Some(values.clone()))
            .consumed_capacity(capacity.clone())
            .build(),
    )
    .unwrap();
    let scan = scan_response(
        ScanOutput::builder()
            .items(values.clone())
            .count(1)
            .scanned_count(7)
            .set_last_evaluated_key(Some(values))
            .consumed_capacity(capacity)
            .build(),
    )
    .unwrap();
    assert_eq!(query.items, vec![item("v")]);
    assert_eq!(query.last_evaluated_key, Some(item("v")));
    assert_eq!((query.count, query.scanned_count), (1, 7));
    let capacity = query.consumed_capacity.as_ref().unwrap();
    assert_eq!(capacity.table_name.as_deref(), Some("Table"));
    assert_eq!(capacity.capacity_units, Some(1.5));
    assert_eq!(capacity.read_capacity_units, Some(1.0));
    assert_eq!(capacity.write_capacity_units, Some(0.5));
    assert_eq!(capacity.table.as_ref().unwrap().capacity_units, Some(0.5));
    assert_eq!(
        capacity.local_secondary_indexes["local"].read_capacity_units,
        Some(0.25)
    );
    assert_eq!(
        capacity.global_secondary_indexes["global"].write_capacity_units,
        Some(0.75)
    );
    assert_eq!(scan.items, query.items);
    assert_eq!(scan.last_evaluated_key, query.last_evaluated_key);
    assert_eq!(scan.consumed_capacity, query.consumed_capacity);
    assert_eq!((scan.count, scan.scanned_count), (1, 7));
    assert!(
        query_response(QueryOutput::builder().build())
            .unwrap()
            .items
            .is_empty()
    );
    assert!(
        scan_response(ScanOutput::builder().build())
            .unwrap()
            .last_evaluated_key
            .is_none()
    );
    let invalid = HashMap::from([("pk".into(), AttributeValue::N("invalid".into()))]);
    assert!(query_response(QueryOutput::builder().items(invalid.clone()).build()).is_err());
    assert!(
        scan_response(
            ScanOutput::builder()
                .set_last_evaluated_key(Some(invalid))
                .build()
        )
        .is_err()
    );
}

#[tokio::test]
async fn query_pages_advance_and_stop_on_absent_or_duplicate_token() {
    for duplicate in [false, true] {
        let terminal = if duplicate { "4177" } else { "f6" };
        let mut fixture = Fixture::new(vec![
            hex("80a407818241764509636f6c6408010941770a01"),
            hex(&format!("80a307818241774509636f6c64080109{terminal}")),
        ])
        .await;
        let paginator = fixture.client.query_paginator(query()).unwrap();
        assert!(paginator.has_more_pages());
        let first = paginator.next_page().await.unwrap().unwrap();
        assert_eq!(first.items[0]["pk"], item("v")["pk"]);
        assert_eq!(
            first.items[0]["a"],
            DaxAttributeValue::String {
                string: "old".into()
            }
        );
        assert_eq!(first.last_evaluated_key, Some(item("w")));
        assert_eq!(first.scanned_count, 1);
        assert!(paginator.has_more_pages());
        let second = paginator.next_page().await.unwrap().unwrap();
        assert_eq!(second.items[0]["pk"], item("w")["pk"]);
        assert!(!paginator.has_more_pages());
        fixture.client.shutdown().unwrap();
        assert!(paginator.next_page().await.unwrap().is_none());
        assert_eq!(fixture.request().await.len(), 30);
        let next = fixture.request().await;
        assert_eq!(next.len(), 33);
        assert!(next.windows(3).any(|value| value == b"\x09\x41w"));
        assert!(fixture.requests.try_recv().is_err());
        fixture.finish().await;
    }
}

#[tokio::test]
async fn scan_pages_are_serialized_and_share_one_cursor_across_handles() {
    let mut fixture = Fixture::new(vec![
        hex("80a40152655461626c65fb3ff8000000000000f6f6f607818241764509636f6c640801094177"),
        hex("80a40152655461626c65fb3ff8000000000000f6f6f607818241774509636f6c64080109f6"),
    ])
    .await;
    let paginator = fixture.client.scan_paginator(scan()).unwrap();
    let alias = Arc::clone(&paginator);
    let (first, second, third) = tokio::join!(
        paginator.next_page(),
        alias.next_page(),
        paginator.next_page()
    );
    let pages = [first.unwrap(), second.unwrap(), third.unwrap()];
    assert_eq!(pages.iter().filter(|page| page.is_none()).count(), 1);
    let pages = pages.into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(pages.len(), 2);
    for page in &pages {
        assert_eq!(page.count, 1);
        assert_eq!(
            page.consumed_capacity.as_ref().unwrap().capacity_units,
            Some(1.5)
        );
    }
    assert!(
        pages
            .iter()
            .any(|page| page.items[0]["pk"] == item("v")["pk"])
    );
    assert!(
        pages
            .iter()
            .any(|page| page.items[0]["pk"] == item("w")["pk"])
    );
    assert!(!paginator.has_more_pages());
    assert!(!alias.has_more_pages());
    assert_eq!(fixture.request().await[19], b'v');
    assert_eq!(fixture.request().await[19], b'w');
    assert!(fixture.requests.try_recv().is_err());
    fixture.finish().await;
}

#[tokio::test]
async fn scan_stops_on_initial_duplicate_token() {
    let mut fixture = Fixture::new(vec![hex("80a307800800094176")]).await;
    let paginator = fixture.client.scan_paginator(scan()).unwrap();
    assert_eq!(
        paginator
            .next_page()
            .await
            .unwrap()
            .unwrap()
            .last_evaluated_key,
        Some(item("v"))
    );
    assert!(!paginator.has_more_pages());
    assert!(paginator.next_page().await.unwrap().is_none());
    fixture.finish().await;
}

#[tokio::test]
async fn batch_duplicate_policy_and_unprocessed_key_advancement() {
    for stop in [false, true] {
        let mut fixture = Fixture::new(vec![
            hex("8082a0a1655461626c6581417680"),
            hex("8082a0a1655461626c6581417780"),
            hex("8082a0a080"),
        ])
        .await;
        let paginator = fixture
            .client
            .batch_get_item_paginator(batch(), stop)
            .unwrap();
        let first = paginator.next_page().await.unwrap().unwrap();
        assert_eq!(first.unprocessed_keys, batch().request_items);
        assert_eq!(paginator.has_more_pages(), !stop);
        assert_eq!(
            fixture.request().await,
            hex("013a29985cdba1655461626c6583f5f6814176bfff")
        );
        if !stop {
            let second = paginator.next_page().await.unwrap().unwrap();
            assert_eq!(second.unprocessed_keys["Table"].keys, vec![item("w")]);
            assert!(paginator.has_more_pages());
            let third = paginator.next_page().await.unwrap().unwrap();
            assert!(third.unprocessed_keys.is_empty());
            assert!(!paginator.has_more_pages());
            assert_eq!(
                fixture.request().await,
                hex("013a29985cdba1655461626c6583f5f6814176bfff")
            );
            assert_eq!(
                fixture.request().await,
                hex("013a29985cdba1655461626c6583f5f6814177bfff")
            );
        }
        assert!(paginator.next_page().await.unwrap().is_none());
        assert!(fixture.requests.try_recv().is_err());
        fixture.finish().await;
    }
}

#[tokio::test]
async fn constructors_validate_values_and_closed_errors_do_not_exhaust_paginators() {
    let client = client("dax://127.0.0.1:1".into());
    client.shutdown().unwrap();
    let invalid = HashMap::from([(
        "pk".into(),
        DaxAttributeValue::Number {
            number: "NaN".into(),
        },
    )]);
    let mut bad_query = query();
    bad_query.exclusive_start_key = Some(invalid.clone());
    let mut bad_scan = scan();
    bad_scan.expression_attribute_values = invalid.clone();
    let mut bad_batch = batch();
    bad_batch.request_items.get_mut("Table").unwrap().keys = vec![invalid];
    for error in [
        client.query_paginator(bad_query).unwrap_err(),
        client.scan_paginator(bad_scan).unwrap_err(),
        client
            .batch_get_item_paginator(bad_batch, true)
            .unwrap_err(),
    ] {
        assert_eq!(error.details().category, DaxErrorCategory::Validation);
    }
    let query = client.query_paginator(query()).unwrap();
    let scan = client.scan_paginator(scan()).unwrap();
    let batch = client.batch_get_item_paginator(batch(), false).unwrap();
    for _ in 0..2 {
        for error in [
            query.next_page().await.unwrap_err(),
            scan.next_page().await.unwrap_err(),
            batch.next_page().await.unwrap_err(),
        ] {
            assert_eq!(error.details().category, DaxErrorCategory::Closed);
        }
        assert!(query.has_more_pages());
        assert!(scan.has_more_pages());
        assert!(batch.has_more_pages());
    }
}

#[tokio::test]
async fn cancellation_releases_transport_and_cursor_lock_without_advancing() {
    let mut fixture = Fixture::new(vec![]).await;
    let paginator = fixture.client.query_paginator(query()).unwrap();
    let active = Arc::clone(&paginator);
    let call = tokio::spawn(async move { active.next_page().await });
    assert_eq!(fixture.request().await.len(), 30);
    assert!(paginator.has_more_pages());

    let queued = Arc::clone(&paginator);
    let mut waiting = Box::pin(queued.next_page());
    assert!(futures::poll!(&mut waiting).is_pending());
    drop(waiting);
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    fixture.disconnected().await;
    assert!(paginator.has_more_pages());
    fixture
        .state
        .pages
        .lock()
        .unwrap()
        .push_back(hex("80a30780080009f6"));
    assert!(paginator.next_page().await.unwrap().is_some());
    assert!(!paginator.has_more_pages());
    assert_eq!(fixture.request().await.len(), 30);
    assert!(fixture.requests.try_recv().is_err());
    let weak = Arc::downgrade(&paginator);
    drop(queued);
    drop(paginator);
    assert!(weak.upgrade().is_none());
    fixture.finish().await;
}

#[tokio::test]
async fn shutdown_preserves_inflight_timeout_and_refuses_subsequent_pages() {
    let mut fixture = Fixture::new(vec![]).await;
    let other = client("dax://127.0.0.1:1".into());
    let paginator = fixture.client.scan_paginator(scan()).unwrap();
    let active = Arc::clone(&paginator);
    let call = tokio::spawn(async move { active.next_page().await });
    fixture.request().await;
    fixture.client.shutdown().unwrap();
    let error = tokio::time::timeout(WAIT, call)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.details().category, DaxErrorCategory::Transport);
    assert_eq!(
        error.details().transport_kind,
        Some(DaxTransportErrorKind::Timeout)
    );
    assert_eq!(error.details().operation.as_deref(), Some("Scan"));
    assert_eq!(
        paginator.next_page().await.unwrap_err().details().category,
        DaxErrorCategory::Closed
    );
    assert!(paginator.has_more_pages());
    assert!(!other.is_closed());
    fixture.finish().await;
}

#[tokio::test]
async fn batch_cancellation_between_pages_keeps_the_last_unprocessed_keys() {
    let mut fixture = Fixture::new(vec![hex("8082a0a1655461626c6581417780")]).await;
    let paginator = fixture
        .client
        .batch_get_item_paginator(batch(), true)
        .unwrap();
    paginator.next_page().await.unwrap().unwrap();
    fixture.request().await;
    let active = Arc::clone(&paginator);
    let call = tokio::spawn(async move { active.next_page().await });
    let cancelled_request = fixture.request().await;
    assert!(cancelled_request.windows(2).any(|bytes| bytes == b"\x41w"));
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    fixture.disconnected().await;
    assert!(paginator.has_more_pages());
    fixture
        .state
        .pages
        .lock()
        .unwrap()
        .push_back(hex("8082a0a080"));
    let page = paginator.next_page().await.unwrap().unwrap();
    assert!(page.unprocessed_keys.is_empty());
    assert_eq!(fixture.request().await, cancelled_request);
    assert!(!paginator.has_more_pages());
    fixture.finish().await;
}

#[tokio::test]
async fn discovery_and_refresh_cancel_without_committing_or_closing_client() {
    for refresh in [false, true] {
        let mut fixture = Fixture::new(vec![]).await;
        let roster = std::mem::take(&mut *fixture.state.roster.lock().unwrap());
        let client = Arc::clone(&fixture.client);
        let call = tokio::spawn(async move {
            if refresh {
                client.refresh_endpoints().await.map(|_| ())
            } else {
                client.discover_endpoints().await.map(|_| ())
            }
        });
        // Both the foreground operation and the SDK's initial background
        // refresh have reached their stalled control requests.
        assert_eq!(fixture.request().await, ENDPOINTS);
        assert_eq!(fixture.request().await, ENDPOINTS);
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        fixture.disconnected().await;
        assert!(!fixture.client.is_closed());
        assert_eq!(fixture.client.active_endpoints().unwrap(), None);
        assert!(fixture.client.route_snapshot().unwrap().is_empty());
        *fixture.state.roster.lock().unwrap() = roster;
        assert_eq!(fixture.client.discover_endpoints().await.unwrap().len(), 1);
        fixture.finish().await;
    }
}

#[tokio::test]
async fn paginator_retains_sdk_client_after_original_ffi_handle_is_dropped() {
    let mut fixture = Fixture::new(vec![hex("80a307800800094177"), hex("80a30780080009f6")]).await;
    let paginator = fixture.client.query_paginator(query()).unwrap();
    let original = std::mem::replace(&mut fixture.client, client("dax://127.0.0.1:1".into()));
    let weak = Arc::downgrade(&original);
    drop(original);
    assert!(weak.upgrade().is_none());
    assert!(paginator.next_page().await.unwrap().is_some());
    assert!(paginator.has_more_pages());
    assert!(paginator.next_page().await.unwrap().is_some());
    assert!(!paginator.has_more_pages());
    let weak = Arc::downgrade(&paginator);
    drop(paginator);
    assert!(weak.upgrade().is_none());
    fixture.finish().await;
}

#[test]
fn endpoint_dtos_preserve_all_fields_without_reinterpreting_metadata() {
    let endpoint = DaxDiscoveredEndpoint::from(aws_dax::DiscoveredEndpoint {
        node_id: i64::MAX,
        hostname: "node.example".into(),
        address: vec![0; 16],
        port: 8111,
        role: 2,
        availability_zone: None,
        leader_session_id: Some(i64::MIN),
    });
    assert_eq!(
        endpoint,
        DaxDiscoveredEndpoint {
            node_id: i64::MAX,
            hostname: "node.example".into(),
            address: vec![0; 16],
            port: 8111,
            role: 2,
            availability_zone: None,
            leader_session_id: Some(i64::MIN),
        }
    );
    assert_eq!(
        DaxEndpointRefresh::from(aws_dax::EndpointRefresh {
            added: vec![3],
            removed: vec![1],
            retained: vec![2, 4],
            changed: vec![4],
        }),
        DaxEndpointRefresh {
            added: vec![3],
            removed: vec![1],
            retained: vec![2, 4],
            changed: vec![4],
        }
    );
    for (healthy, health_probe_healthy) in [(false, true), (true, false)] {
        assert_eq!(
            DaxRouteSnapshot::from(aws_dax::RouteSnapshot {
                node_id: 4,
                dial_address: "[::1]:8111".into(),
                tls_server_name: "node.example".into(),
                healthy,
                health_probe_healthy,
            }),
            DaxRouteSnapshot {
                node_id: 4,
                dial_address: "[::1]:8111".into(),
                tls_server_name: "node.example".into(),
                healthy,
                health_probe_healthy,
            }
        );
    }
}

#[tokio::test]
async fn discovery_refresh_and_snapshots_preserve_last_valid_roster_on_error() {
    let mut fixture = Fixture::new(vec![]).await;
    assert_eq!(fixture.client.active_endpoints().unwrap(), None);
    assert!(fixture.client.route_snapshot().unwrap().is_empty());
    assert_eq!(fixture.client.last_refresh_error(), None);
    let discovered = fixture.client.discover_endpoints().await.unwrap();
    assert_eq!(
        discovered,
        vec![DaxDiscoveredEndpoint {
            node_id: 1,
            hostname: "nod".into(),
            address: vec![127, 0, 0, 1],
            port: i64::from(fixture.port),
            role: 1,
            availability_zone: Some("a".into()),
            leader_session_id: Some(7),
        }]
    );
    assert_eq!(
        fixture.client.active_endpoints().unwrap(),
        Some(discovered.clone())
    );
    let routes = fixture.client.route_snapshot().unwrap();
    assert_eq!(
        routes,
        vec![DaxRouteSnapshot {
            node_id: 1,
            dial_address: format!("127.0.0.1:{}", fixture.port),
            tls_server_name: "nod".into(),
            healthy: true,
            health_probe_healthy: true,
        }]
    );
    assert_eq!(
        fixture.client.refresh_endpoints().await.unwrap(),
        DaxEndpointRefresh {
            added: vec![],
            removed: vec![],
            retained: vec![1],
            changed: vec![],
        }
    );
    *fixture.state.roster.lock().unwrap() = hex("80f6");
    let error = fixture.client.refresh_endpoints().await.unwrap_err();
    assert_eq!(error.details().category, DaxErrorCategory::Protocol);
    assert_eq!(fixture.client.active_endpoints().unwrap(), Some(discovered));
    assert_eq!(fixture.client.route_snapshot().unwrap(), routes);
    fixture.client.shutdown().unwrap();
    for error in [
        fixture.client.discover_endpoints().await.unwrap_err(),
        fixture.client.refresh_endpoints().await.unwrap_err(),
    ] {
        assert_eq!(error.details().category, DaxErrorCategory::Closed);
    }
    // SDK metadata accessors remain readable after shutdown.
    assert_eq!(fixture.client.route_snapshot().unwrap(), routes);
    fixture.finish().await;
}

#[test]
fn exports_execute_without_a_foreign_tokio_runtime_and_drop_lazily() {
    let client = client("dax://127.0.0.1:1".into());
    let query = client.query_paginator(query()).unwrap();
    let scan = client.scan_paginator(scan()).unwrap();
    let batch = client.batch_get_item_paginator(batch(), true).unwrap();
    drop(query.next_page());
    drop(scan.next_page());
    drop(batch.next_page());
    assert!(query.has_more_pages() && scan.has_more_pages() && batch.has_more_pages());
    client.shutdown().unwrap();
    futures::executor::block_on(async {
        assert_eq!(
            query.next_page().await.unwrap_err().details().category,
            DaxErrorCategory::Closed
        );
        assert_eq!(
            scan.next_page().await.unwrap_err().details().category,
            DaxErrorCategory::Closed
        );
        assert_eq!(
            batch.next_page().await.unwrap_err().details().category,
            DaxErrorCategory::Closed
        );
        assert_eq!(
            client
                .discover_endpoints()
                .await
                .unwrap_err()
                .details()
                .category,
            DaxErrorCategory::Closed
        );
        assert_eq!(
            client
                .refresh_endpoints()
                .await
                .unwrap_err()
                .details()
                .category,
            DaxErrorCategory::Closed
        );
    });
}
