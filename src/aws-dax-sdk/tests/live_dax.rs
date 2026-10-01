use std::{
    collections::{HashMap, HashSet},
    env, fs,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_config::BehaviorVersion;
use aws_dax::Client;
use aws_sdk_dynamodb::{
    operation::{
        batch_get_item::BatchGetItemInput, batch_write_item::BatchWriteItemInput,
        query::QueryInput, scan::ScanInput, transact_get_items::TransactGetItemsInput,
        transact_write_items::TransactWriteItemsInput,
    },
    types::{
        AttributeValue, Get, KeysAndAttributes, Put, PutRequest, TransactGetItem,
        TransactWriteItem, WriteRequest,
    },
};

const DEFAULT_TABLE: &str = "DaxParityTable";
const DEFAULT_ENDPOINTS: &[&str] = &[
    "dax://radu-rust.cykcls.dax-clusters.eu-west-1.amazonaws.com",
    "daxs://radu-rust-tls.cykcls.dax-clusters.eu-west-1.amazonaws.com",
];

struct LiveCleanup {
    client: Client,
    table: String,
    keys: Vec<(AttributeValue, AttributeValue)>,
    armed: bool,
}

impl LiveCleanup {
    fn new(client: Client, table: &str) -> Self {
        Self {
            client,
            table: table.to_owned(),
            keys: Vec::new(),
            armed: true,
        }
    }

    fn track(&mut self, partition_key: AttributeValue, sort_key: AttributeValue) {
        self.keys.push((partition_key, sort_key));
    }

    async fn run(&mut self) -> Result<(), String> {
        while let Some((partition_key, sort_key)) = self.keys.pop() {
            tokio::time::timeout(
                Duration::from_secs(15),
                self.client
                    .delete_item()
                    .table_name(&self.table)
                    .key("PK", partition_key)
                    .key("SK", sort_key)
                    .send(),
            )
            .await
            .map_err(|_| "cleanup DeleteItem timed out".to_owned())?
            .map_err(|error| format!("cleanup DeleteItem failed: {error}"))?;
        }
        self.armed = false;
        Ok(())
    }
}

impl Drop for LiveCleanup {
    fn drop(&mut self) {
        if !self.armed || self.keys.is_empty() {
            return;
        }
        let client = self.client.clone();
        let table = self.table.clone();
        let keys = std::mem::take(&mut self.keys);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                for (partition_key, sort_key) in keys {
                    if let Err(error) = client
                        .delete_item()
                        .table_name(&table)
                        .key("PK", partition_key)
                        .key("SK", sort_key)
                        .send()
                        .await
                    {
                        eprintln!("live cleanup DeleteItem failed for {table}: {error}");
                    }
                }
            });
        } else {
            eprintln!(
                "live cleanup skipped because no Tokio runtime is available for {} item(s)",
                keys.len()
            );
        }
    }
}

#[tokio::test]
#[ignore = "requires DAX_LIVE_TESTS=1 and the configured live clusters"]
async fn exercises_live_plaintext_and_tls_smoke_operations() {
    if env::var("DAX_LIVE_TESTS").as_deref() != Ok("1") {
        return;
    }

    let table = env::var("DAX_TABLE_NAME").unwrap_or_else(|_| DEFAULT_TABLE.into());
    let endpoints = env::var("DAX_ENDPOINTS")
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|endpoint| !endpoint.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|_| {
            DEFAULT_ENDPOINTS
                .iter()
                .map(|endpoint| (*endpoint).into())
                .collect()
        });
    assert!(
        !endpoints.is_empty(),
        "DAX_ENDPOINTS must contain at least one endpoint"
    );

    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region("eu-west-1")
        .load()
        .await;
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_millis();
    let sort_key = "SMOKE".to_owned();

    for (endpoint_index, endpoint) in endpoints.into_iter().enumerate() {
        let partition_key = format!("LIVE#{suffix}-{endpoint_index}");
        let client = Client::from_sdk_config(&sdk_config, endpoint.clone())
            .unwrap_or_else(|error| panic!("failed to configure {endpoint}: {error}"));
        let mut cleanup = LiveCleanup::new(client.clone(), &table);
        let item = HashMap::from([
            ("PK".to_owned(), AttributeValue::S(partition_key.clone())),
            ("SK".to_owned(), AttributeValue::S(sort_key.clone())),
            ("LSI1SK".to_owned(), AttributeValue::N("1".into())),
            (
                "GSI1PK".to_owned(),
                AttributeValue::S(format!("LIVE#{suffix}")),
            ),
            ("GSI1SK".to_owned(), AttributeValue::S("SMOKE".into())),
            ("Name".to_owned(), AttributeValue::S("live-smoke".into())),
        ]);

        client
            .put_item()
            .table_name(&table)
            .item("PK", item["PK"].clone())
            .item("SK", item["SK"].clone())
            .item("LSI1SK", item["LSI1SK"].clone())
            .item("GSI1PK", item["GSI1PK"].clone())
            .item("GSI1SK", item["GSI1SK"].clone())
            .item("Name", item["Name"].clone())
            .send()
            .await
            .unwrap_or_else(|error| panic!("PutItem failed for {endpoint}: {error}"));
        cleanup.track(item["PK"].clone(), item["SK"].clone());

        let fetched = client
            .get_item()
            .table_name(&table)
            .key("PK", item["PK"].clone())
            .key("SK", item["SK"].clone())
            .send()
            .await
            .unwrap_or_else(|error| panic!("GetItem failed for {endpoint}: {error}"));
        assert_eq!(
            fetched.item().and_then(|attributes| attributes.get("Name")),
            Some(&AttributeValue::S("live-smoke".into()))
        );
        if env::var("DAX_FAULT_PREWARM").as_deref() == Ok("1") {
            client
                .discover_endpoints()
                .await
                .unwrap_or_else(|error| panic!("fault prewarm discovery failed: {error}"));
            for route in client.route_snapshot().unwrap_or_default() {
                eprintln!(
                    "DAX_PREWARM_ROUTE node={} address={} healthy={} probe_healthy={}",
                    route.node_id, route.dial_address, route.healthy, route.health_probe_healthy
                );
            }
            let ready_file = env::var("DAX_FAULT_READY_FILE")
                .expect("DAX_FAULT_READY_FILE is required for fault prewarm");
            let release_file = env::var("DAX_FAULT_RELEASE_FILE")
                .expect("DAX_FAULT_RELEASE_FILE is required for fault prewarm");
            fs::write(&ready_file, b"ready").expect("fault prewarm ready file is writable");
            while !std::path::Path::new(&release_file).exists() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        if env::var("DAX_FAULT_WORKLOAD").as_deref() == Ok("read") {
            cleanup
                .run()
                .await
                .unwrap_or_else(|error| panic!("read fault workload cleanup failed: {error}"));
            eprintln!("DAX_FAULT_RESULT workload=read operation=GetItem success=1");
            continue;
        }

        let query_input = QueryInput::builder()
            .table_name(&table)
            .key_condition_expression("PK = :pk")
            .expression_attribute_values(":pk", item["PK"].clone())
            .build()
            .expect("live query input is valid");
        let queried = client
            .query()
            .input(query_input)
            .send()
            .await
            .unwrap_or_else(|error| panic!("Query failed for {endpoint}: {error}"));
        assert!(
            queried
                .items()
                .iter()
                .any(|candidate| candidate.get("SK") == Some(&AttributeValue::S(sort_key.clone())))
        );

        let gsi_query = QueryInput::builder()
            .table_name(&table)
            .index_name("GSI1")
            .key_condition_expression("GSI1PK = :pk")
            .expression_attribute_values(":pk", item["GSI1PK"].clone())
            .build()
            .expect("live GSI query input is valid");
        let gsi_queried = client
            .query()
            .input(gsi_query)
            .send()
            .await
            .unwrap_or_else(|error| panic!("GSI Query failed for {endpoint}: {error}"));
        assert!(gsi_queried.items().iter().any(|candidate| {
            candidate.get("SK") == Some(&AttributeValue::S(sort_key.clone()))
        }));

        let lsi_query = QueryInput::builder()
            .table_name(&table)
            .index_name("LSI1")
            .key_condition_expression("PK = :pk AND LSI1SK = :sk")
            .expression_attribute_values(":pk", item["PK"].clone())
            .expression_attribute_values(":sk", item["LSI1SK"].clone())
            .build()
            .expect("live LSI query input is valid");
        let lsi_queried = client
            .query()
            .input(lsi_query)
            .send()
            .await
            .unwrap_or_else(|error| panic!("LSI Query failed for {endpoint}: {error}"));
        assert!(lsi_queried.items().iter().any(|candidate| {
            candidate.get("SK") == Some(&AttributeValue::S(sort_key.clone()))
        }));

        let scanned = client
            .scan()
            .table_name(&table)
            .filter_expression("PK = :pk")
            .expression_attribute_value(":pk", item["PK"].clone())
            .send()
            .await
            .unwrap_or_else(|error| panic!("Scan failed for {endpoint}: {error}"));
        assert!(scanned.items().iter().any(|candidate| {
            candidate.get("SK") == Some(&AttributeValue::S(sort_key.clone()))
        }));

        let batch_key = HashMap::from([
            ("PK".to_owned(), AttributeValue::S(partition_key.clone())),
            ("SK".to_owned(), AttributeValue::S("BATCH".into())),
            ("Name".to_owned(), AttributeValue::S("batch-live".into())),
        ]);
        let batch_write = BatchWriteItemInput::builder()
            .request_items(
                table.clone(),
                vec![
                    WriteRequest::builder()
                        .put_request(
                            PutRequest::builder()
                                .set_item(Some(batch_key.clone()))
                                .build()
                                .expect("batch put request is valid"),
                        )
                        .build(),
                ],
            )
            .build()
            .expect("batch write input is valid");
        let batch_write_output = client
            .batch_write_item()
            .input(batch_write)
            .send()
            .await
            .unwrap_or_else(|error| panic!("BatchWriteItem failed for {endpoint}: {error}"));
        assert!(
            batch_write_output
                .unprocessed_items()
                .is_none_or(HashMap::is_empty),
            "DAX returned unprocessed live batch write"
        );
        cleanup.track(batch_key["PK"].clone(), batch_key["SK"].clone());
        if env::var("DAX_FAULT_WORKLOAD").as_deref() == Ok("write") {
            cleanup
                .run()
                .await
                .unwrap_or_else(|error| panic!("write fault workload cleanup failed: {error}"));
            eprintln!("DAX_FAULT_RESULT workload=write operation=BatchWriteItem success=1");
            continue;
        }

        let transaction_items = ["TX-1", "TX-2"]
            .into_iter()
            .map(|sort_key| {
                HashMap::from([
                    ("PK".to_owned(), AttributeValue::S(partition_key.clone())),
                    ("SK".to_owned(), AttributeValue::S(sort_key.into())),
                    (
                        "Name".to_owned(),
                        AttributeValue::S("transaction-live".into()),
                    ),
                ])
            })
            .collect::<Vec<_>>();
        let transaction_write = TransactWriteItemsInput::builder()
            .transact_items(
                TransactWriteItem::builder()
                    .put(
                        Put::builder()
                            .table_name(&table)
                            .set_item(Some(transaction_items[0].clone()))
                            .build()
                            .expect("transaction put is valid"),
                    )
                    .build(),
            )
            .transact_items(
                TransactWriteItem::builder()
                    .put(
                        Put::builder()
                            .table_name(&table)
                            .set_item(Some(transaction_items[1].clone()))
                            .build()
                            .expect("transaction put is valid"),
                    )
                    .build(),
            )
            .build()
            .expect("transaction write input is valid");
        let transaction_workload = env::var("DAX_FAULT_WORKLOAD").as_deref() == Ok("transaction");
        let transaction_write_result = client
            .transact_write_items()
            .input(transaction_write)
            .send()
            .await;
        if let Err(error) = transaction_write_result {
            if transaction_workload {
                eprintln!(
                    "DAX_FAULT_RESULT workload=transaction operation=TransactWriteItems \
                     success=0 error={error}"
                );
                cleanup.run().await.unwrap_or_else(|cleanup_error| {
                    panic!("transaction fault workload cleanup failed: {cleanup_error}")
                });
                continue;
            }
            panic!("TransactWriteItems failed for {endpoint}: {error}");
        }
        for item in &transaction_items {
            cleanup.track(item["PK"].clone(), item["SK"].clone());
        }

        let transaction_read = TransactGetItemsInput::builder()
            .transact_items(
                TransactGetItem::builder()
                    .get(
                        Get::builder()
                            .table_name(&table)
                            .set_key(Some(HashMap::from([
                                ("PK".to_owned(), transaction_items[0]["PK"].clone()),
                                ("SK".to_owned(), transaction_items[0]["SK"].clone()),
                            ])))
                            .build()
                            .expect("transaction get is valid"),
                    )
                    .build(),
            )
            .transact_items(
                TransactGetItem::builder()
                    .get(
                        Get::builder()
                            .table_name(&table)
                            .set_key(Some(HashMap::from([
                                ("PK".to_owned(), transaction_items[1]["PK"].clone()),
                                ("SK".to_owned(), transaction_items[1]["SK"].clone()),
                            ])))
                            .build()
                            .expect("transaction get is valid"),
                    )
                    .build(),
            )
            .build()
            .expect("transaction get input is valid");
        let transaction_read_result = client
            .transact_get_items()
            .input(transaction_read)
            .send()
            .await;
        if let Err(error) = transaction_read_result {
            if transaction_workload {
                eprintln!(
                    "DAX_FAULT_RESULT workload=transaction operation=TransactGetItems \
                     success=0 error={error}"
                );
                cleanup.run().await.unwrap_or_else(|cleanup_error| {
                    panic!("transaction fault workload cleanup failed: {cleanup_error}")
                });
                continue;
            }
            panic!("TransactGetItems failed for {endpoint}: {error}");
        }
        if transaction_workload {
            cleanup.run().await.unwrap_or_else(|error| {
                panic!("transaction fault workload cleanup failed: {error}")
            });
            eprintln!("DAX_FAULT_RESULT workload=transaction operation=TransactGetItems success=1");
            continue;
        }

        for sort_key in ["PAGE-1", "PAGE-2"] {
            let paged_item = HashMap::from([
                ("PK", AttributeValue::S(partition_key.clone())),
                ("SK", AttributeValue::S(sort_key.into())),
                ("Name", AttributeValue::S("live-paged".into())),
            ]);
            client
                .put_item()
                .table_name(&table)
                .item("PK", paged_item["PK"].clone())
                .item("SK", paged_item["SK"].clone())
                .item("Name", paged_item["Name"].clone())
                .send()
                .await
                .unwrap_or_else(|error| panic!("paged PutItem failed for {endpoint}: {error}"));
            cleanup.track(paged_item["PK"].clone(), paged_item["SK"].clone());
        }

        let paged_query = QueryInput::builder()
            .table_name(&table)
            .key_condition_expression("PK = :pk")
            .expression_attribute_values(":pk", AttributeValue::S(partition_key.clone()))
            .limit(1)
            .build()
            .expect("paged query input is valid");
        let mut query_result = None;
        for _ in 0..30 {
            let mut query_paginator = client.query_paginator(paged_query.clone());
            let mut pages = 0;
            let mut item_count = 0;
            let mut sort_keys = HashSet::new();
            while let Some(page) = query_paginator
                .next_page()
                .await
                .unwrap_or_else(|error| panic!("paged Query failed for {endpoint}: {error}"))
            {
                pages += 1;
                item_count += page.items().len();
                sort_keys.extend(page.items().iter().filter_map(|item| match item.get("SK") {
                    Some(AttributeValue::S(value)) => Some(value.clone()),
                    _ => None,
                }));
            }
            if sort_keys.len() == 6 && item_count == 6 {
                query_result = Some((pages, item_count, sort_keys));
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let (query_pages, query_item_count, query_sort_keys) =
            query_result.expect("Query paginator should return all six unique items");
        assert!(query_pages >= 6, "expected multiple Query pages");
        assert_eq!(query_item_count, 6, "expected each Query item exactly once");
        assert_eq!(query_sort_keys.len(), 6);

        let paged_scan = ScanInput::builder()
            .table_name(&table)
            .filter_expression("PK = :pk")
            .expression_attribute_values(":pk", AttributeValue::S(partition_key.clone()))
            .limit(1)
            .build()
            .expect("paged scan input is valid");
        let mut scan_result = None;
        for _ in 0..30 {
            let mut scan_paginator = client.scan_paginator(paged_scan.clone());
            let mut pages = 0;
            let mut item_count = 0;
            let mut sort_keys = HashSet::new();
            while let Some(page) = scan_paginator
                .next_page()
                .await
                .unwrap_or_else(|error| panic!("paged Scan failed for {endpoint}: {error}"))
            {
                pages += 1;
                item_count += page.items().len();
                sort_keys.extend(page.items().iter().filter_map(|item| match item.get("SK") {
                    Some(AttributeValue::S(value)) => Some(value.clone()),
                    _ => None,
                }));
            }
            if sort_keys.len() == 6 && item_count == 6 {
                scan_result = Some((pages, item_count, sort_keys));
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let (scan_pages, scan_item_count, scan_sort_keys) =
            scan_result.expect("Scan paginator should return all six unique items");
        assert!(scan_pages >= 1);
        assert_eq!(scan_item_count, 6, "expected each Scan item exactly once");
        assert_eq!(scan_sort_keys.len(), 6);

        client
            .update_item()
            .table_name(&table)
            .key("PK", item["PK"].clone())
            .key("SK", item["SK"].clone())
            .update_expression("SET #name = :updated")
            .expression_attribute_name("#name", "Name")
            .expression_attribute_value(":updated", AttributeValue::S("live-updated".into()))
            .send()
            .await
            .unwrap_or_else(|error| panic!("UpdateItem failed for {endpoint}: {error}"));
        let mut updated = None;
        for _ in 0..10 {
            let fetched = client
                .get_item()
                .table_name(&table)
                .key("PK", item["PK"].clone())
                .key("SK", item["SK"].clone())
                .send()
                .await
                .unwrap_or_else(|error| {
                    panic!("GetItem after update failed for {endpoint}: {error}")
                });
            if fetched.item().and_then(|attributes| attributes.get("Name"))
                == Some(&AttributeValue::S("live-updated".into()))
            {
                updated = Some(fetched);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            updated
                .as_ref()
                .and_then(|output| output.item())
                .and_then(|attributes| attributes.get("Name")),
            Some(&AttributeValue::S("live-updated".into()))
        );

        let batch_keys = vec![
            HashMap::from([
                ("PK".to_owned(), item["PK"].clone()),
                ("SK".to_owned(), item["SK"].clone()),
            ]),
            HashMap::from([
                ("PK".to_owned(), batch_key["PK"].clone()),
                ("SK".to_owned(), batch_key["SK"].clone()),
            ]),
        ];
        let batch_keys_and_attributes = KeysAndAttributes::builder()
            .set_keys(Some(batch_keys))
            .build()
            .expect("batch get keys are valid");
        let batch_request = BatchGetItemInput::builder()
            .request_items(table.clone(), batch_keys_and_attributes)
            .build()
            .expect("batch get input is valid");
        let mut batch_paginator = client.batch_get_item_paginator(batch_request);
        let batch_page = batch_paginator
            .next_page()
            .await
            .unwrap_or_else(|error| panic!("BatchGetItem failed for {endpoint}: {error}"))
            .expect("batch get returns a page");
        assert_eq!(
            batch_page
                .responses()
                .and_then(|responses| responses.get(&table))
                .map(Vec::len),
            Some(2)
        );
        assert!(!batch_paginator.has_more_pages());

        cleanup
            .run()
            .await
            .unwrap_or_else(|error| panic!("live cleanup failed for {endpoint}: {error}"));
    }
}

#[tokio::test]
#[ignore = "requires DAX_LIVE_TESTS=1 and the configured live clusters"]
async fn exercises_live_endpoint_discovery_and_route_health() {
    if env::var("DAX_LIVE_TESTS").as_deref() != Ok("1") {
        return;
    }

    let endpoint = env::var("DAX_ENDPOINT").unwrap_or_else(|_| DEFAULT_ENDPOINTS[0].to_owned());
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region("eu-west-1")
        .load()
        .await;
    let client = Client::from_sdk_config(&sdk_config, endpoint.clone())
        .unwrap_or_else(|error| panic!("failed to configure {endpoint}: {error}"));

    let discovered = client
        .discover_endpoints()
        .await
        .unwrap_or_else(|error| panic!("endpoint discovery failed for {endpoint}: {error}"));
    assert!(
        !discovered.is_empty(),
        "DAX discovery returned no endpoints for {endpoint}"
    );

    let snapshot = client
        .route_snapshot()
        .expect("route snapshot is available after discovery");
    assert!(snapshot.len() <= discovered.len());
    assert!(
        snapshot.iter().all(|route| route.healthy),
        "all discovered routes should initially be request-healthy: {snapshot:?}"
    );
    assert!(
        snapshot.iter().all(|route| route.health_probe_healthy),
        "all discovered routes should initially be probe-healthy: {snapshot:?}"
    );
    if env::var_os("DAX_PRINT_ROUTES").is_some() {
        for route in &snapshot {
            println!(
                "DAX_ROUTE node={} address={} healthy={} probe_healthy={}",
                route.node_id, route.dial_address, route.healthy, route.health_probe_healthy
            );
        }
    }

    let refresh = client
        .refresh_endpoints()
        .await
        .unwrap_or_else(|error| panic!("endpoint refresh failed for {endpoint}: {error}"));
    assert!(
        refresh.added.is_empty() && refresh.removed.is_empty(),
        "refresh should preserve the discovered roster: {refresh:?}"
    );
}
